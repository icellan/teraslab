//! Wire-format types for operation-based replication.
//!
//! Compact binary serialization — a Spend op is under 90 bytes on the wire.
//!
//! # Wire layout
//!
//! Each `ReplicaBatch` frame begins with a 1-byte protocol version tag.
//!
//! ## V2 (current — produced by [`ReplicaBatch::serialize`])
//!
//! `[version=2:1][first_seq:8][count:4][trace_id:16][span_id:8][source_node_id:8][cluster_key:8][op0_len:4][op0]…`
//!
//! Header is 53 bytes ([`ReplicaBatch::HEADER_SIZE`]). The 8-byte
//! `cluster_key` field is inserted immediately after `source_node_id`
//! and BEFORE `op0_len`; receivers compare it against their current
//! cluster epoch and reject mismatches with `ERR_STALE_EPOCH`. A value
//! of 0 means "unknown / not set" (e.g. non-clustered tests).
//!
//! ## V1 (legacy — decoded for one-version compat, never produced)
//!
//! V1 (legacy, lacking `cluster_key`) decoded with a 45-byte header.
//! F-G7-012 removed the V1 decoder: senders never produced V1 in this
//! repository's history, and the V1 wildcard cluster_key was a
//! defense-in-depth hole against the Phase B2 stale-epoch gate. The
//! receiver now rejects any leading version byte other than V2 with
//! [`ProtocolError::UnknownVersion`].
//!
//! ## Common fields
//!
//! The 24-byte trace-context region (see [`crate::observability::WireTraceContext`])
//! is zero when the sender has no active/sampled span; receivers treat
//! all-zero as "absent". The 8-byte `source_node_id` is zero when no
//! stable sender id is available; receivers treat zero as "unknown" and
//! fall back to TCP peer keying.
//!
//! Decoders reject any other version byte with [`ProtocolError::UnknownVersion`].

use crate::index::TxKey;
use crate::observability::WireTraceContext;
use thiserror::Error;

/// Current batch wire layout. F-G7-012 removed the legacy V1 decoder;
/// receivers reject any other version byte with
/// [`ProtocolError::UnknownVersion`]. Senders always produce V2.
///
/// Full wire layout:
/// `[version=2:1][first_seq:8][count:4][trace_id:16][span_id:8][source_node_id:8][cluster_key:8][op0_len:4][op0]…`
pub const BATCH_PROTOCOL_V2: u8 = 2;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("buffer too short: need {need}, have {have}")]
    BufferTooShort { need: usize, have: usize },
    #[error("unknown op type: {0}")]
    UnknownOp(u8),
    #[error("unknown batch protocol version: {0}")]
    UnknownVersion(u8),
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
const OP_MARK_LONGEST_CHAIN: u8 = 14;
const OP_PRUNE_SLOT_IF_SPENT_BY: u8 = 15;
/// Deletion-tombstone §6: a delete that carries the tombstone fields so the
/// replica records the tombstone alongside the record removal. The legacy
/// [`OP_DELETE`] (tag 12) stays decodable for back-compat (an old peer or an
/// old redo/receive replay) and is emitted unchanged when tombstones are
/// disabled. Mirrors the `CreateV2`/`SpendV2` versioning precedent: new tag,
/// old tag retained.
const OP_DELETE_V2: u8 = 16;

/// Fixed wire payload of a [`ReplicaOp::DeleteV2`] op, AFTER the 1-byte
/// [`OP_DELETE_V2`] tag. `#[repr(C, packed)]` pins the on-wire byte order to
/// declaration order with no compiler padding, matching the manual
/// little-endian encode/decode below. All multi-byte fields are stored
/// little-endian; the compile-time assertion guards the 41-byte size.
#[repr(C, packed)]
struct DeleteV2Wire {
    txid: [u8; 32],
    deletion_height: u32,
    generation: u32,
    cause: u8,
}

const _: () = assert!(core::mem::size_of::<DeleteV2Wire>() == 41);

/// A single replication operation sent from master to replica.
/// A mutation operation to be replicated from master to replica.
///
/// Every record-retaining mutation variant carries or embeds
/// `master_generation` — the record's generation counter on the master AFTER
/// the mutation was applied.
/// The replica uses this to:
/// - Set the generation to the master's value instead of auto-incrementing
/// - Detect stale/out-of-order ops with wrapping generation ordering
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicaOp {
    Spend {
        tx_key: TxKey,
        offset: u32,
        spending_data: [u8; 36],
        current_block_height: u32,
        block_height_retention: u32,
        master_generation: u32,
    },
    Unspend {
        tx_key: TxKey,
        offset: u32,
        /// Expected spending data before clearing.
        spending_data: [u8; 36],
        current_block_height: u32,
        block_height_retention: u32,
        master_generation: u32,
    },
    SetMined {
        tx_key: TxKey,
        block_id: u32,
        block_height: u32,
        subtree_idx: u32,
        on_longest_chain: bool,
        current_block_height: u32,
        block_height_retention: u32,
        master_generation: u32,
    },
    UnsetMined {
        tx_key: TxKey,
        block_id: u32,
        current_block_height: u32,
        block_height_retention: u32,
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
    /// Delete carrying the tombstone fields (deletion-tombstone §6).
    ///
    /// Applied by the receiver as: remove the record (same as [`Self::Delete`])
    /// AND write a tombstone via the engine's existing tombstone-write path,
    /// so the replica's own restart self-purges (§5.2) and its redb tombstone
    /// index is updated. The master emits this (instead of [`Self::Delete`])
    /// when `tombstones_enabled`, carrying the same `deletion_height` /
    /// `generation` / `cause` the master's own tombstone recorded. `cause` is
    /// the [`crate::tombstone::TombstoneCause`] discriminant byte.
    ///
    /// Like [`Self::Delete`], this carries no `master_generation` idempotency
    /// token — it is an idempotent remove keyed on `tx_key`. The `generation`
    /// here is the *tombstone's* generation (the record's generation at
    /// deletion), a first-class tombstone field, not the replication ordering
    /// token. `master_generation()` therefore returns `None` for this op,
    /// exactly as for [`Self::Delete`].
    DeleteV2 {
        tx_key: TxKey,
        deletion_height: u32,
        generation: u32,
        cause: u8,
    },
    PruneSlot {
        tx_key: TxKey,
        offset: u32,
    },
    PruneSlotIfSpentBy {
        tx_key: TxKey,
        offset: u32,
        child_txid: [u8; 32],
    },
    /// Mark a transaction as on or off the longest chain.
    ///
    /// Mutates `unmined_since`, `delete_at_height`, and `generation` on
    /// the master and must therefore be replicated. Recovery replay and
    /// receiver apply paths use `master_generation` as the idempotency
    /// token (R-053): a replica that is already at-or-ahead of
    /// `master_generation` under wrapping generation ordering treats this op
    /// as already applied.
    MarkLongestChain {
        tx_key: TxKey,
        on_longest_chain: bool,
        current_block_height: u32,
        block_height_retention: u32,
        master_generation: u32,
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
            | Self::DeleteV2 { tx_key, .. }
            | Self::PruneSlot { tx_key, .. }
            | Self::PruneSlotIfSpentBy { tx_key, .. }
            | Self::MarkLongestChain { tx_key, .. } => *tx_key,
        }
    }

    /// Extract the master generation from a mutation op, if present.
    ///
    /// Create embeds generation in its extended metadata bytes at offsets
    /// 46..50. Delete and PruneSlot don't carry generation because they remove
    /// data or mutate only a terminal slot status.
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
            }
            | Self::MarkLongestChain {
                master_generation, ..
            } => Some(*master_generation),
            Self::Create { metadata_bytes, .. } => create_embedded_generation(metadata_bytes),
            // DeleteV2's `generation` is the tombstone's own field, NOT the
            // replication ordering token — it is an idempotent remove keyed on
            // `tx_key`, exactly like `Delete`. So it carries no
            // `master_generation`.
            Self::Delete { .. }
            | Self::DeleteV2 { .. }
            | Self::PruneSlot { .. }
            | Self::PruneSlotIfSpentBy { .. } => None,
        }
    }
}

fn create_embedded_generation(metadata_bytes: &[u8]) -> Option<u32> {
    if metadata_bytes.len() < 50 {
        return None;
    }
    Some(u32::from_le_bytes(metadata_bytes[46..50].try_into().ok()?))
}

impl ReplicaOp {
    /// Serialize this op to bytes. Returns the serialized form.
    ///
    /// Most mutation ops append `master_generation` (4 bytes LE) after all
    /// other fields. Create embeds it in the extended metadata bytes.
    /// Delete/PruneSlot do not carry generation.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(80);
        match self {
            ReplicaOp::Spend {
                tx_key,
                offset,
                spending_data,
                current_block_height,
                block_height_retention,
                master_generation,
            } => {
                buf.push(OP_SPEND);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(spending_data);
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::Unspend {
                tx_key,
                offset,
                spending_data,
                current_block_height,
                block_height_retention,
                master_generation,
            } => {
                buf.push(OP_UNSPEND);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(spending_data);
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::SetMined {
                tx_key,
                block_id,
                block_height,
                subtree_idx,
                on_longest_chain,
                current_block_height,
                block_height_retention,
                master_generation,
            } => {
                buf.push(OP_SET_MINED);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&block_id.to_le_bytes());
                buf.extend_from_slice(&block_height.to_le_bytes());
                buf.extend_from_slice(&subtree_idx.to_le_bytes());
                buf.push(u8::from(*on_longest_chain));
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
            }
            ReplicaOp::UnsetMined {
                tx_key,
                block_id,
                current_block_height,
                block_height_retention,
                master_generation,
            } => {
                buf.push(OP_UNSET_MINED);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&block_id.to_le_bytes());
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
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
            ReplicaOp::DeleteV2 {
                tx_key,
                deletion_height,
                generation,
                cause,
            } => {
                // Layout mirrors `DeleteV2Wire` (txid | dh | gen | cause), all
                // little-endian, 41 payload bytes after the tag.
                buf.push(OP_DELETE_V2);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&deletion_height.to_le_bytes());
                buf.extend_from_slice(&generation.to_le_bytes());
                buf.push(*cause);
            }
            ReplicaOp::PruneSlot { tx_key, offset } => {
                buf.push(OP_PRUNE_SLOT);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
            }
            ReplicaOp::PruneSlotIfSpentBy {
                tx_key,
                offset,
                child_txid,
            } => {
                buf.push(OP_PRUNE_SLOT_IF_SPENT_BY);
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(child_txid);
            }
            ReplicaOp::MarkLongestChain {
                tx_key,
                on_longest_chain,
                current_block_height,
                block_height_retention,
                master_generation,
            } => {
                buf.push(OP_MARK_LONGEST_CHAIN);
                buf.extend_from_slice(&tx_key.txid);
                buf.push(u8::from(*on_longest_chain));
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
                buf.extend_from_slice(&master_generation.to_le_bytes());
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
                need(rest, 84)?; // 32 + 4 + 36 + 4(current) + 4(retention) + 4(gen)
                let key = read_key(rest);
                let offset = u32::from_le_bytes(rest[32..36].try_into().unwrap());
                let mut sd = [0u8; 36];
                sd.copy_from_slice(&rest[36..72]);
                let current_block_height = r_u32(rest, 72);
                let block_height_retention = r_u32(rest, 76);
                let master_generation = r_u32(rest, 80);
                Ok((
                    ReplicaOp::Spend {
                        tx_key: key,
                        offset,
                        spending_data: sd,
                        current_block_height,
                        block_height_retention,
                        master_generation,
                    },
                    85,
                ))
            }
            OP_UNSPEND => {
                need(rest, 84)?; // 32 + 4 + 36 + 4(current) + 4(retention) + 4(gen)
                let mut sd = [0u8; 36];
                sd.copy_from_slice(&rest[36..72]);
                Ok((
                    ReplicaOp::Unspend {
                        tx_key: read_key(rest),
                        offset: r_u32(rest, 32),
                        spending_data: sd,
                        current_block_height: r_u32(rest, 72),
                        block_height_retention: r_u32(rest, 76),
                        master_generation: r_u32(rest, 80),
                    },
                    85,
                ))
            }
            OP_SET_MINED => {
                need(rest, 57)?; // 32 + 4 + 4 + 4 + 1 + 4(current) + 4(retention) + 4(gen)
                Ok((
                    ReplicaOp::SetMined {
                        tx_key: read_key(rest),
                        block_id: r_u32(rest, 32),
                        block_height: r_u32(rest, 36),
                        subtree_idx: r_u32(rest, 40),
                        on_longest_chain: rest[44] != 0,
                        current_block_height: r_u32(rest, 45),
                        block_height_retention: r_u32(rest, 49),
                        master_generation: r_u32(rest, 53),
                    },
                    58,
                ))
            }
            OP_UNSET_MINED => {
                need(rest, 48)?; // 32 + 4 + 4(current) + 4(retention) + 4(gen)
                Ok((
                    ReplicaOp::UnsetMined {
                        tx_key: read_key(rest),
                        block_id: r_u32(rest, 32),
                        current_block_height: r_u32(rest, 36),
                        block_height_retention: r_u32(rest, 40),
                        master_generation: r_u32(rest, 44),
                    },
                    49,
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
                // Use checked_mul for consistency with the sibling create-path
                // decoders (codec.rs `validate_batch_count`, dispatch.rs
                // migration entry_count). An attacker-controlled hash_count
                // cannot overflow `usize` here on 64-bit, but an overflowed
                // `need` would underflow the bound check; treat overflow as an
                // unsatisfiable length (BufferTooShort).
                let hashes_bytes =
                    hash_count
                        .checked_mul(32)
                        .ok_or(ProtocolError::BufferTooShort {
                            need: usize::MAX,
                            have: rest.len(),
                        })?;
                need(rest, pos + hashes_bytes)?;
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
                need(rest, pos + 1)?;
                let is_external = rest[pos] != 0;
                pos += 1;
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
            OP_DELETE_V2 => {
                // 32(txid) + 4(deletion_height) + 4(generation) + 1(cause) = 41.
                need(rest, 41)?;
                Ok((
                    ReplicaOp::DeleteV2 {
                        tx_key: read_key(rest),
                        deletion_height: r_u32(rest, 32),
                        generation: r_u32(rest, 36),
                        cause: rest[40],
                    },
                    42,
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
            OP_PRUNE_SLOT_IF_SPENT_BY => {
                need(rest, 68)?;
                let mut child_txid = [0u8; 32];
                child_txid.copy_from_slice(&rest[36..68]);
                Ok((
                    ReplicaOp::PruneSlotIfSpentBy {
                        tx_key: read_key(rest),
                        offset: r_u32(rest, 32),
                        child_txid,
                    },
                    69,
                ))
            }
            OP_MARK_LONGEST_CHAIN => {
                // 32(tx_key) + 1(on_longest_chain) + 4(current_block_height)
                // + 4(block_height_retention) + 4(master_generation) = 45.
                need(rest, 45)?;
                Ok((
                    ReplicaOp::MarkLongestChain {
                        tx_key: read_key(rest),
                        on_longest_chain: rest[32] != 0,
                        current_block_height: r_u32(rest, 33),
                        block_height_retention: r_u32(rest, 37),
                        master_generation: r_u32(rest, 41),
                    },
                    46,
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
/// Each batch carries an optional W3C trace context (`trace_id`, `span_id`)
/// so replicas can stitch their `handle_replica_batch` span into the
/// sender's trace. When `trace_ctx` is `None` the on-wire bytes are zero
/// and receivers treat the absence as "start a new root span."
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
    /// `None` for non-clustered tests; receivers fall back to TCP peer
    /// keying when absent.
    pub source_node_id: Option<u64>,
    /// Cluster epoch identifier carried with the batch so the receiver
    /// can detect stale-epoch traffic and reject it with
    /// `ERR_STALE_EPOCH` (see `crate::protocol::opcodes::ERR_STALE_EPOCH`).
    ///
    /// A value of `0` means "unknown / not set": non-clustered tests
    /// and legacy V1 frames (which lack the field entirely) decode with
    /// `cluster_key == 0`. Receivers treating `0` as a wildcard SHOULD
    /// only do so when they themselves have no current cluster epoch.
    pub cluster_key: u64,
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
    /// Sequence-gap NAK (R-D1/D-3): the batch's `first_sequence` is ahead
    /// of the receiver's next-expected per-stream sequence. Nothing was
    /// applied and the receiver's watermark did NOT advance. The sender
    /// must re-send relabeled at `expected_sequence` (benign hole left by
    /// a failed/compensated earlier batch) or run catch-up.
    ///
    /// Wire note: this is an additive ack tag (2) introduced together
    /// with the dense per-replica sequence space. A pre-upgrade master
    /// decodes it as [`ProtocolError::UnknownOp`] and treats the batch
    /// as failed — safe (no false ACK), mirroring how the V2 batch
    /// version byte was introduced (one-version compat, fail closed).
    Gap {
        /// The receiver's next-expected sequence (applied watermark + 1).
        expected_sequence: u64,
        /// The `first_sequence` the offending batch carried.
        received_first_sequence: u64,
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
    /// Serialize to bytes using the V2 wire format.
    ///
    /// Layout:
    /// `[version=2:1][first_seq:8][count:4][trace_id:16][span_id:8][source_node_id:8][cluster_key:8][op0_len:4][op0]…`
    ///
    /// When `trace_ctx` is `None`, the 24 trace-context bytes are zero.
    /// When `source_node_id` is `None`, the source field is zero.
    /// `cluster_key` is encoded verbatim (0 means "unknown / not set").
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::HEADER_SIZE + self.ops.len() * 64);
        buf.push(BATCH_PROTOCOL_V2);
        buf.extend_from_slice(&self.first_sequence.to_le_bytes());
        buf.extend_from_slice(&(self.ops.len() as u32).to_le_bytes());
        let mut tc = [0u8; WireTraceContext::SIZE];
        if let Some(ctx) = self.trace_ctx {
            ctx.write_to(&mut tc);
        }
        buf.extend_from_slice(&tc);
        buf.extend_from_slice(&self.source_node_id.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&self.cluster_key.to_le_bytes());
        for op in &self.ops {
            let ob = op.serialize();
            buf.extend_from_slice(&(ob.len() as u32).to_le_bytes());
            buf.extend_from_slice(&ob);
        }
        buf
    }

    /// Deserialize from bytes.
    ///
    /// * Leading byte == [`BATCH_PROTOCOL_V2`]: parse V2 layout
    ///   (53-byte header including `cluster_key`).
    /// * Anything else: [`ProtocolError::UnknownVersion`].
    ///
    /// F-G7-012 removed the legacy V1 decoder: V1 frames decoded with
    /// `cluster_key = 0`, which the Phase B2 stale-epoch gate treats
    /// as a wildcard. Accepting V1 in clustered mode therefore
    /// silently bypassed the epoch invariant. Senders have always
    /// produced V2, so removing the decoder is the cheapest fix.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        need(data, 1)?;
        match data[0] {
            BATCH_PROTOCOL_V2 => Self::decode_v2(data),
            other => Err(ProtocolError::UnknownVersion(other)),
        }
    }

    /// Decode a V2 frame (53-byte header, includes `cluster_key`).
    fn decode_v2(data: &[u8]) -> Result<Self> {
        need(data, Self::HEADER_SIZE)?;
        let first_sequence = u64::from_le_bytes(data[1..9].try_into().unwrap());
        let count = u32::from_le_bytes(data[9..13].try_into().unwrap()) as usize;
        let trace_ctx = WireTraceContext::read_from(&data[13..13 + WireTraceContext::SIZE]);
        let source_off = 13 + WireTraceContext::SIZE;
        let source_node_id = decode_source_node_id(&data[source_off..source_off + 8]);
        let cluster_off = source_off + 8;
        let cluster_key =
            u64::from_le_bytes(data[cluster_off..cluster_off + 8].try_into().unwrap());
        let ops = Self::decode_ops(data, Self::HEADER_SIZE, count)?;
        Ok(ReplicaBatch {
            first_sequence,
            ops,
            trace_ctx,
            source_node_id,
            cluster_key,
        })
    }

    /// Op-stream decoder. `header_size` is the byte offset at which
    /// the length-prefixed op stream begins. Kept as a separate helper
    /// in case a future version of the wire format reuses the same
    /// length-prefixed op layout with a different header size.
    fn decode_ops(data: &[u8], header_size: usize, count: usize) -> Result<Vec<ReplicaOp>> {
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
        Ok(ops)
    }

    /// The last sequence number in this batch.
    pub fn last_sequence(&self) -> u64 {
        self.first_sequence + self.ops.len().saturating_sub(1) as u64
    }

    /// Current (V2) batch header overhead in bytes.
    ///
    /// Layout: `version(1) + first_sequence(8) + count(4) + trace_ctx(24) + source_node_id(8) + cluster_key(8) = 53`.
    pub const HEADER_SIZE: usize = 1 + 8 + 4 + WireTraceContext::SIZE + 8 + 8;

    /// Byte offset of the `trace_id` field in the serialized frame.
    /// Exposed for the test suite that inspects exact byte layout.
    pub const TRACE_ID_OFFSET: usize = 1 + 8 + 4;

    /// Byte offset of the `span_id` field in the serialized frame.
    pub const SPAN_ID_OFFSET: usize = Self::TRACE_ID_OFFSET + 16;

    /// Byte offset of the `cluster_key` field in a V2 serialized frame.
    /// `source_node_id` immediately precedes it (8 bytes wide).
    pub const CLUSTER_KEY_OFFSET: usize = Self::SPAN_ID_OFFSET + 8 + 8;
}

/// Map a raw 8-byte source-node id field to the optional in-memory
/// representation: zero on the wire is `None`, anything else is `Some`.
fn decode_source_node_id(bytes: &[u8]) -> Option<u64> {
    let raw = u64::from_le_bytes(bytes.try_into().unwrap());
    if raw == 0 { None } else { Some(raw) }
}

/// F-G7-017: cap diagnostic strings in `ReplicaAck::Error` so a
/// long-tail message (e.g. a `format!` that embeds a full filesystem
/// path) cannot overflow `MAX_ACK_FRAME_SIZE` on the master side
/// and lose the entire ACK. The cap matches the master's frame budget
/// minus the fixed ReplicaAck::Error header bytes and HMAC suffix.
pub const MAX_ACK_ERROR_MESSAGE_LEN: usize = 2048;

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
                // F-G7-017: truncate at a char boundary so the
                // resulting String stays valid UTF-8. Use a slice of
                // the original; clients on the read side decode with
                // from_utf8_lossy so even a mid-codepoint cut would
                // be tolerated, but we keep the wire encoding clean.
                let message_bytes = if message.len() > MAX_ACK_ERROR_MESSAGE_LEN {
                    let mut cut = MAX_ACK_ERROR_MESSAGE_LEN;
                    while cut > 0 && !message.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    &message.as_bytes()[..cut]
                } else {
                    message.as_bytes()
                };
                buf.push(1);
                buf.extend_from_slice(&failed_sequence.to_le_bytes());
                buf.extend_from_slice(&(message_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(message_bytes);
            }
            ReplicaAck::Gap {
                expected_sequence,
                received_first_sequence,
            } => {
                buf.push(2);
                buf.extend_from_slice(&expected_sequence.to_le_bytes());
                buf.extend_from_slice(&received_first_sequence.to_le_bytes());
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
            2 => {
                need(data, 17)?;
                Ok(ReplicaAck::Gap {
                    expected_sequence: u64::from_le_bytes(data[1..9].try_into().unwrap()),
                    received_first_sequence: u64::from_le_bytes(data[9..17].try_into().unwrap()),
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
            current_block_height: 700_000,
            block_height_retention: 144,
            master_generation: 0,
        };
        let bytes = op.serialize();
        assert!(
            bytes.len() < 90,
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
    fn prune_slot_if_spent_by_round_trip() {
        let op = ReplicaOp::PruneSlotIfSpentBy {
            tx_key: key(2),
            offset: 42,
            child_txid: [0xC1; 32],
        };
        let bytes = op.serialize();
        let (decoded, consumed) = ReplicaOp::deserialize(&bytes).unwrap();
        assert_eq!(decoded, op);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn delete_v2_round_trip_all_fields() {
        // Deletion-tombstone §6: DeleteV2 carries the tombstone fields. Cover
        // a non-zero deletion_height + generation + each cause discriminant.
        for cause in [
            crate::tombstone::TombstoneCause::SpentDah,
            crate::tombstone::TombstoneCause::Admin,
            crate::tombstone::TombstoneCause::MigrationPrune,
        ] {
            let op = ReplicaOp::DeleteV2 {
                tx_key: key(7),
                deletion_height: 0x0123_4567,
                generation: 0x89AB_CDEF,
                cause: cause.as_u8(),
            };
            let bytes = op.serialize();
            // 1 (tag) + 32 (txid) + 4 (dh) + 4 (gen) + 1 (cause) = 42 bytes.
            assert_eq!(bytes.len(), 42, "DeleteV2 wire size");
            assert_eq!(bytes[0], OP_DELETE_V2, "DeleteV2 op tag is 16");
            let (decoded, consumed) = ReplicaOp::deserialize(&bytes).unwrap();
            assert_eq!(decoded, op, "DeleteV2 round-trip failed for {cause:?}");
            assert_eq!(consumed, bytes.len());
            // Verify the decoded fields explicitly (packed → local copies).
            match decoded {
                ReplicaOp::DeleteV2 {
                    tx_key,
                    deletion_height,
                    generation,
                    cause: got_cause,
                } => {
                    assert_eq!(tx_key, key(7));
                    assert_eq!(deletion_height, 0x0123_4567);
                    assert_eq!(generation, 0x89AB_CDEF);
                    assert_eq!(got_cause, cause.as_u8());
                }
                other => panic!("expected DeleteV2, got {other:?}"),
            }
        }
    }

    #[test]
    fn delete_v2_wire_struct_is_41_bytes() {
        // Guards the fixed payload size the manual encode/decode mirrors.
        assert_eq!(core::mem::size_of::<DeleteV2Wire>(), 41);
    }

    #[test]
    fn delete_v2_does_not_carry_master_generation() {
        // Like V1 Delete, DeleteV2 is an idempotent remove keyed on tx_key —
        // its `generation` is the tombstone field, NOT the replication
        // ordering token, so `master_generation()` must be None.
        let op = ReplicaOp::DeleteV2 {
            tx_key: key(3),
            deletion_height: 10,
            generation: 99,
            cause: crate::tombstone::TombstoneCause::Admin.as_u8(),
        };
        assert_eq!(op.master_generation(), None);
        assert_eq!(op.tx_key(), key(3));
    }

    #[test]
    fn v1_delete_still_decodes_unchanged() {
        // Back-compat: the V1 Delete op (tag 12, 33 bytes) must still encode
        // and decode exactly as before DeleteV2 was added.
        let op = ReplicaOp::Delete { tx_key: key(5) };
        let bytes = op.serialize();
        assert_eq!(bytes.len(), 33, "V1 Delete wire size unchanged");
        assert_eq!(bytes[0], OP_DELETE, "V1 Delete tag is 12");
        let (decoded, consumed) = ReplicaOp::deserialize(&bytes).unwrap();
        assert_eq!(decoded, op);
        assert_eq!(consumed, 33);
        assert_eq!(decoded.master_generation(), None);
    }

    #[test]
    fn all_variants_round_trip() {
        let ops = vec![
            ReplicaOp::Spend {
                tx_key: key(1),
                offset: 0,
                spending_data: [0x11; 36],
                current_block_height: 700_000,
                block_height_retention: 144,
                master_generation: 0,
            },
            ReplicaOp::Unspend {
                tx_key: key(2),
                offset: 1,
                spending_data: [0x22; 36],
                current_block_height: 700_001,
                block_height_retention: 145,
                master_generation: 0,
            },
            ReplicaOp::SetMined {
                tx_key: key(3),
                block_id: 100,
                block_height: 800000,
                subtree_idx: 7,
                on_longest_chain: true,
                current_block_height: 800010,
                block_height_retention: 288,
                master_generation: 0,
            },
            ReplicaOp::UnsetMined {
                tx_key: key(4),
                block_id: 200,
                current_block_height: 800020,
                block_height_retention: 288,
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
            ReplicaOp::DeleteV2 {
                tx_key: key(16),
                deletion_height: 812_345,
                generation: 9,
                cause: crate::tombstone::TombstoneCause::SpentDah.as_u8(),
            },
            ReplicaOp::PruneSlot {
                tx_key: key(13),
                offset: 99,
            },
            ReplicaOp::PruneSlotIfSpentBy {
                tx_key: key(15),
                offset: 100,
                child_txid: [0xC1; 32],
            },
            ReplicaOp::MarkLongestChain {
                tx_key: key(14),
                on_longest_chain: true,
                current_block_height: 800_000,
                block_height_retention: 288,
                master_generation: 7,
            },
        ];

        for op in &ops {
            let bytes = op.serialize();
            let (decoded, consumed) = ReplicaOp::deserialize(&bytes).unwrap();
            assert_eq!(&decoded, op, "round-trip failed for {op:?}");
            assert_eq!(consumed, bytes.len());
        }
    }

    /// R-052: explicit byte-layout round-trip for `MarkLongestChain`.
    /// Verifies opcode 14, the exact 46-byte serialized length, and that
    /// `on_longest_chain=false`, large `current_block_height`, and a
    /// non-zero `master_generation` all survive a serialize/deserialize
    /// round-trip — the decoder MUST observe the same bytes the encoder
    /// emits.
    #[test]
    fn replica_op_mark_longest_chain_round_trip() {
        // on_longest_chain = true case
        let op_true = ReplicaOp::MarkLongestChain {
            tx_key: key(0xAA),
            on_longest_chain: true,
            current_block_height: 0xDEAD_BEEF,
            block_height_retention: 288,
            master_generation: 42,
        };
        let bytes = op_true.serialize();
        // 1 (opcode) + 32 (tx_key) + 1 (bool) + 4 + 4 + 4 = 46 bytes.
        assert_eq!(bytes.len(), 46, "MarkLongestChain wire size must be 46");
        // First byte must be opcode 14.
        assert_eq!(bytes[0], 14, "MarkLongestChain opcode must be 14");
        let (decoded, consumed) = ReplicaOp::deserialize(&bytes).unwrap();
        assert_eq!(decoded, op_true);
        assert_eq!(consumed, 46);

        // on_longest_chain = false case (the reorg-rollback flavor) —
        // unmined_since gets set to current_block_height.
        let op_false = ReplicaOp::MarkLongestChain {
            tx_key: key(0xBB),
            on_longest_chain: false,
            current_block_height: 750_000,
            block_height_retention: 0,
            master_generation: u32::MAX,
        };
        let bytes2 = op_false.serialize();
        assert_eq!(bytes2.len(), 46);
        let (decoded2, consumed2) = ReplicaOp::deserialize(&bytes2).unwrap();
        assert_eq!(decoded2, op_false);
        assert_eq!(consumed2, 46);

        // Sanity: tx_key + master_generation are exposed via accessors.
        assert_eq!(decoded.tx_key(), key(0xAA));
        assert_eq!(decoded.master_generation(), Some(42));
        assert_eq!(decoded2.master_generation(), Some(u32::MAX));
    }

    /// Future-proofing guard: an unknown opcode (e.g. a value the
    /// receiver doesn't yet recognise) MUST surface
    /// [`ProtocolError::UnknownOp`] rather than panic, silently advance,
    /// or misinterpret subsequent bytes. R-052 introduces opcode 14;
    /// this test asserts the rejection path stays intact for opcode 99.
    #[test]
    fn unknown_op_byte_rejected_explicitly() {
        let mut bad = vec![0u8; 64];
        bad[0] = 99; // unassigned opcode
        let err = ReplicaOp::deserialize(&bad).expect_err("unknown opcode must error");
        match err {
            ProtocolError::UnknownOp(v) => assert_eq!(v, 99),
            other => panic!("expected UnknownOp(99), got {other:?}"),
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
    fn create_embedded_generation_is_exposed_for_stale_guard() {
        let mut metadata_bytes = vec![0u8; 70];
        metadata_bytes[46..50].copy_from_slice(&42u32.to_le_bytes());
        let op = ReplicaOp::Create {
            tx_key: key(3),
            metadata_bytes,
            utxo_hashes: vec![[0xAB; 32]],
            cold_data: None,
            is_external: false,
        };
        assert_eq!(op.master_generation(), Some(42));
    }

    #[test]
    fn legacy_create_without_embedded_generation_has_no_generation_token() {
        let op = ReplicaOp::Create {
            tx_key: key(4),
            metadata_bytes: vec![0u8; 46],
            utxo_hashes: vec![[0xAB; 32]],
            cold_data: None,
            is_external: false,
        };
        assert_eq!(op.master_generation(), None);
    }

    #[test]
    fn create_missing_is_external_byte_rejected() {
        let op = ReplicaOp::Create {
            tx_key: key(2),
            metadata_bytes: vec![0; 32],
            utxo_hashes: vec![[0xAB; 32]],
            cold_data: None,
            is_external: false,
        };
        let mut bytes = op.serialize();
        assert_eq!(bytes.pop(), Some(0), "last byte is the is_external flag");
        let err = ReplicaOp::deserialize(&bytes)
            .expect_err("truncated Create must not default is_external to false");
        match err {
            ProtocolError::BufferTooShort { .. } => {}
            other => panic!("expected BufferTooShort, got {other:?}"),
        }
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
                    current_block_height: 700_000,
                    block_height_retention: 288,
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
            cluster_key: 0,
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
                current_block_height: 700_000 + i as u32,
                block_height_retention: 288,
                master_generation: 0,
            })
            .collect();
        let batch = ReplicaBatch {
            first_sequence: 1000,
            ops,
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };
        let bytes = batch.serialize();
        let decoded = ReplicaBatch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.ops.len(), 100);
        assert_eq!(decoded.first_sequence, 1000);
        assert_eq!(decoded.last_sequence(), 1099);
    }

    #[test]
    fn batch_header_overhead() {
        // V2: version(1) + first_sequence(8) + count(4) + trace_ctx(24) + source_node_id(8) + cluster_key(8) = 53.
        assert_eq!(ReplicaBatch::HEADER_SIZE, 53);
        // Legacy V1 header (compat decode path): same minus cluster_key.
        // HEADER_SIZE_V1 (45) was the legacy V1 layout — removed in F-G7-012.
        assert_eq!(ReplicaBatch::TRACE_ID_OFFSET, 13);
        assert_eq!(ReplicaBatch::SPAN_ID_OFFSET, 29);
        assert_eq!(ReplicaBatch::CLUSTER_KEY_OFFSET, 45);
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
            cluster_key: 0,
        };
        let bytes = batch.serialize();
        // Version byte (current = V2).
        assert_eq!(bytes[0], BATCH_PROTOCOL_V2);
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
            cluster_key: 0,
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
            cluster_key: 0,
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
    fn replication_batch_rejects_unknown_version_byte() {
        // Any leading byte other than BATCH_PROTOCOL_V2 must error. We
        // construct a frame whose body would otherwise be valid for the
        // current layout.
        let op = ReplicaOp::Delete { tx_key: key(4) };
        let ob = op.serialize();
        let mut frame = Vec::new();
        frame.push(0xFE); // not V2
        frame.extend_from_slice(&7u64.to_le_bytes());
        frame.extend_from_slice(&1u32.to_le_bytes());
        frame.extend_from_slice(&[0u8; WireTraceContext::SIZE]);
        frame.extend_from_slice(&0u64.to_le_bytes());
        frame.extend_from_slice(&(ob.len() as u32).to_le_bytes());
        frame.extend_from_slice(&ob);

        let err = ReplicaBatch::deserialize(&frame).expect_err("must reject unknown version");
        match err {
            ProtocolError::UnknownVersion(v) => assert_eq!(v, 0xFE),
            other => panic!("expected UnknownVersion, got {other:?}"),
        }
    }

    /// F-G7-012: the legacy V1 decoder is removed because V1 frames
    /// stamped `cluster_key = 0` which the stale-epoch gate accepts
    /// as a wildcard, silently bypassing the Phase B2 epoch invariant.
    /// Senders never produced V1 in this repository's history. The
    /// decoder must now reject the V1 version byte as unknown.
    #[test]
    fn replication_batch_rejects_v1_version_byte() {
        let op = ReplicaOp::Delete { tx_key: key(9) };
        let ob = op.serialize();
        let mut frame = Vec::new();
        // Reconstruct what a V1 frame looked like — leading byte = 1.
        frame.push(1u8);
        frame.extend_from_slice(&777u64.to_le_bytes());
        frame.extend_from_slice(&1u32.to_le_bytes());
        frame.extend_from_slice(&[0u8; WireTraceContext::SIZE]);
        frame.extend_from_slice(&55u64.to_le_bytes());
        frame.extend_from_slice(&(ob.len() as u32).to_le_bytes());
        frame.extend_from_slice(&ob);

        let err = ReplicaBatch::deserialize(&frame)
            .expect_err("V1 frames must be rejected as unknown version");
        match err {
            ProtocolError::UnknownVersion(v) => assert_eq!(v, 1),
            other => panic!("expected UnknownVersion(1), got {other:?}"),
        }
    }

    #[test]
    fn replica_batch_v2_round_trip_carries_cluster_key() {
        let batch = ReplicaBatch {
            first_sequence: 4242,
            ops: vec![
                ReplicaOp::Spend {
                    tx_key: key(1),
                    offset: 7,
                    spending_data: [0x55; 36],
                    current_block_height: 700_000,
                    block_height_retention: 288,
                    master_generation: 11,
                },
                ReplicaOp::Delete { tx_key: key(2) },
            ],
            trace_ctx: None,
            source_node_id: Some(123_456_789),
            cluster_key: 0xDEAD_BEEF_CAFE_BABE,
        };
        let bytes = batch.serialize();
        // Default-encode is V2; leading byte must be V2.
        assert_eq!(bytes[0], BATCH_PROTOCOL_V2);
        let decoded = ReplicaBatch::deserialize(&bytes).unwrap();
        assert_eq!(decoded.cluster_key, 0xDEAD_BEEF_CAFE_BABE);
        assert_eq!(decoded, batch);
    }

    #[test]
    fn replica_batch_v2_header_size_is_53() {
        assert_eq!(ReplicaBatch::HEADER_SIZE, 53);
    }

    #[test]
    fn replica_batch_v2_short_payload_rejected() {
        // A V2 prefix but fewer than HEADER_SIZE (53) bytes — must surface
        // the truncation error variant.
        let mut frame = Vec::new();
        frame.push(BATCH_PROTOCOL_V2);
        // Only 10 bytes of payload — well below the 53-byte V2 header.
        frame.extend_from_slice(&[0u8; 10]);
        let err = ReplicaBatch::deserialize(&frame).expect_err("must reject short V2 frame");
        match err {
            ProtocolError::BufferTooShort { need, have } => {
                assert_eq!(need, ReplicaBatch::HEADER_SIZE);
                assert_eq!(have, frame.len());
            }
            other => panic!("expected BufferTooShort, got {other:?}"),
        }
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

    /// F-G7-017: messages longer than MAX_ACK_ERROR_MESSAGE_LEN must
    /// be truncated at serialize time so a bug in the replica's error
    /// formatter cannot overflow the master's MAX_ACK_FRAME_SIZE
    /// budget and lose the diagnostic entirely.
    #[test]
    fn ack_error_message_truncated_above_cap() {
        let oversized = "x".repeat(MAX_ACK_ERROR_MESSAGE_LEN + 1024);
        let ack = ReplicaAck::Error {
            failed_sequence: 11,
            message: oversized,
        };
        let bytes = ack.serialize();
        let decoded = ReplicaAck::deserialize(&bytes).unwrap();
        match decoded {
            ReplicaAck::Error {
                failed_sequence,
                message,
            } => {
                assert_eq!(failed_sequence, 11);
                assert_eq!(
                    message.len(),
                    MAX_ACK_ERROR_MESSAGE_LEN,
                    "message must be truncated to MAX_ACK_ERROR_MESSAGE_LEN bytes",
                );
                assert!(
                    message.bytes().all(|b| b == b'x'),
                    "truncation must preserve the prefix",
                );
            }
            other => panic!("expected Error variant, got {other:?}"),
        }
    }

    /// R-D1: the sequence-gap NAK round-trips on the wire. Tag 2 is
    /// additive — pre-upgrade decoders reject it as `UnknownOp` and
    /// treat the batch as failed (no false ACK), the same fail-closed
    /// posture as the V2 batch version byte.
    #[test]
    fn ack_gap_round_trip() {
        let ack = ReplicaAck::Gap {
            expected_sequence: 42,
            received_first_sequence: 99,
        };
        let bytes = ack.serialize();
        assert_eq!(bytes.len(), 17, "tag(1) + expected(8) + received(8)");
        assert_eq!(bytes[0], 2, "Gap NAK uses ack tag 2");
        let decoded = ReplicaAck::deserialize(&bytes).unwrap();
        assert_eq!(decoded, ack);

        // Truncated Gap frames must error, not mis-decode.
        let err = ReplicaAck::deserialize(&bytes[..16]).unwrap_err();
        match err {
            ProtocolError::BufferTooShort { need, have } => {
                assert_eq!(need, 17);
                assert_eq!(have, 16);
            }
            other => panic!("expected BufferTooShort, got {other:?}"),
        }
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
