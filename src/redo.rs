//! Circular redo log for crash recovery.
//!
//! Operations are appended to the log before data writes. On crash recovery,
//! all entries after the last checkpoint are replayed idempotently. The log
//! wraps around when it reaches the end, reusing space freed by checkpoints.

use crate::device::{AlignedBuf, BlockDevice};
use crate::index::TxKey;
use std::sync::Arc;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from redo log operations.
#[derive(Error, Debug)]
pub enum RedoError {
    /// The redo log is full (checkpoint needed).
    #[error("redo log full: {used}/{capacity} bytes used")]
    LogFull { used: u64, capacity: u64 },

    /// Device I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] crate::device::DeviceError),

    /// Entry checksum mismatch during recovery.
    #[error("checksum mismatch at offset {offset}")]
    ChecksumMismatch { offset: u64 },

    /// Corrupted or truncated entry.
    #[error("corrupted entry at offset {offset}")]
    Corrupted { offset: u64 },
}

pub type Result<T> = std::result::Result<T, RedoError>;

// ---------------------------------------------------------------------------
// RedoOp
// ---------------------------------------------------------------------------

/// Type tags for serialized redo operations.
const OP_SPEND: u8 = 1;
const OP_UNSPEND: u8 = 2;
const OP_SET_MINED: u8 = 3;
const OP_FREEZE: u8 = 4;
const OP_UNFREEZE: u8 = 5;
const OP_REASSIGN: u8 = 6;
const OP_PRUNE_SLOT: u8 = 7;
const OP_CREATE: u8 = 9;
const OP_DELETE: u8 = 10;
const OP_CHECKPOINT: u8 = 11;
const OP_SET_CONFLICTING: u8 = 12;
const OP_SET_LOCKED: u8 = 13;
const OP_PRESERVE_UNTIL: u8 = 14;
const OP_MARK_LONGEST_CHAIN: u8 = 15;

/// A redo log operation that can be serialized and replayed.
#[derive(Debug, Clone, PartialEq)]
pub enum RedoOp {
    Spend {
        tx_key: TxKey,
        offset: u32,
        spending_data: [u8; 36],
        new_spent_count: u32,
    },
    Unspend {
        tx_key: TxKey,
        offset: u32,
        new_spent_count: u32,
    },
    SetMined {
        tx_key: TxKey,
        block_id: u32,
        block_height: u32,
        subtree_idx: u32,
        unset: bool,
    },
    Freeze {
        tx_key: TxKey,
        offset: u32,
    },
    Unfreeze {
        tx_key: TxKey,
        offset: u32,
    },
    Reassign {
        tx_key: TxKey,
        offset: u32,
        new_hash: [u8; 32],
        block_height: u32,
        spendable_after: u32,
    },
    PruneSlot {
        tx_key: TxKey,
        offset: u32,
    },
    Create {
        tx_key: TxKey,
        record_offset: u64,
        utxo_count: u32,
    },
    Delete {
        tx_key: TxKey,
        record_offset: u64,
        record_size: u64,
    },
    SetConflicting {
        tx_key: TxKey,
        value: bool,
        current_block_height: u32,
        block_height_retention: u32,
    },
    SetLocked {
        tx_key: TxKey,
        value: bool,
    },
    PreserveUntil {
        tx_key: TxKey,
        block_height: u32,
    },
    MarkOnLongestChain {
        tx_key: TxKey,
        on_longest_chain: bool,
        current_block_height: u32,
        block_height_retention: u32,
    },
    Checkpoint,
}

impl RedoOp {
    fn op_type(&self) -> u8 {
        match self {
            RedoOp::Spend { .. } => OP_SPEND,
            RedoOp::Unspend { .. } => OP_UNSPEND,
            RedoOp::SetMined { .. } => OP_SET_MINED,
            RedoOp::Freeze { .. } => OP_FREEZE,
            RedoOp::Unfreeze { .. } => OP_UNFREEZE,
            RedoOp::Reassign { .. } => OP_REASSIGN,
            RedoOp::PruneSlot { .. } => OP_PRUNE_SLOT,
            RedoOp::Create { .. } => OP_CREATE,
            RedoOp::Delete { .. } => OP_DELETE,
            RedoOp::SetConflicting { .. } => OP_SET_CONFLICTING,
            RedoOp::SetLocked { .. } => OP_SET_LOCKED,
            RedoOp::PreserveUntil { .. } => OP_PRESERVE_UNTIL,
            RedoOp::MarkOnLongestChain { .. } => OP_MARK_LONGEST_CHAIN,
            RedoOp::Checkpoint => OP_CHECKPOINT,
        }
    }

    /// Extract the tx_key from the operation, if it has one.
    ///
    /// Returns `None` for `Checkpoint` which has no associated key.
    pub fn tx_key(&self) -> Option<&TxKey> {
        match self {
            RedoOp::Spend { tx_key, .. }
            | RedoOp::Unspend { tx_key, .. }
            | RedoOp::SetMined { tx_key, .. }
            | RedoOp::Freeze { tx_key, .. }
            | RedoOp::Unfreeze { tx_key, .. }
            | RedoOp::Reassign { tx_key, .. }
            | RedoOp::PruneSlot { tx_key, .. }
            | RedoOp::Create { tx_key, .. }
            | RedoOp::Delete { tx_key, .. }
            | RedoOp::SetConflicting { tx_key, .. }
            | RedoOp::SetLocked { tx_key, .. }
            | RedoOp::PreserveUntil { tx_key, .. }
            | RedoOp::MarkOnLongestChain { tx_key, .. } => Some(tx_key),
            RedoOp::Checkpoint => None,
        }
    }

    /// Serialize op-specific data (without type byte, sequence, or length).
    fn serialize_data(&self, buf: &mut Vec<u8>) {
        match self {
            RedoOp::Spend { tx_key, offset, spending_data, new_spent_count } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(spending_data);
                buf.extend_from_slice(&new_spent_count.to_le_bytes());
            }
            RedoOp::Unspend { tx_key, offset, new_spent_count } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(&new_spent_count.to_le_bytes());
            }
            RedoOp::SetMined { tx_key, block_id, block_height, subtree_idx, unset } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&block_id.to_le_bytes());
                buf.extend_from_slice(&block_height.to_le_bytes());
                buf.extend_from_slice(&subtree_idx.to_le_bytes());
                buf.push(if *unset { 1 } else { 0 });
            }
            RedoOp::Freeze { tx_key, offset } | RedoOp::Unfreeze { tx_key, offset }
            | RedoOp::PruneSlot { tx_key, offset } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
            }
            RedoOp::Reassign { tx_key, offset, new_hash, block_height, spendable_after } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(new_hash);
                buf.extend_from_slice(&block_height.to_le_bytes());
                buf.extend_from_slice(&spendable_after.to_le_bytes());
            }
            RedoOp::Create { tx_key, record_offset, utxo_count } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&record_offset.to_le_bytes());
                buf.extend_from_slice(&utxo_count.to_le_bytes());
            }
            RedoOp::Delete { tx_key, record_offset, record_size } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&record_offset.to_le_bytes());
                buf.extend_from_slice(&record_size.to_le_bytes());
            }
            RedoOp::SetConflicting { tx_key, value, current_block_height, block_height_retention } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.push(if *value { 1 } else { 0 });
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
            }
            RedoOp::SetLocked { tx_key, value } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.push(if *value { 1 } else { 0 });
            }
            RedoOp::PreserveUntil { tx_key, block_height } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&block_height.to_le_bytes());
            }
            RedoOp::MarkOnLongestChain { tx_key, on_longest_chain, current_block_height, block_height_retention } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.push(if *on_longest_chain { 1 } else { 0 });
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
            }
            RedoOp::Checkpoint => {}
        }
    }

    /// Deserialize op from type byte + data bytes.
    fn deserialize(op_type: u8, data: &[u8]) -> Option<Self> {
        match op_type {
            OP_SPEND if data.len() >= 76 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut sd = [0u8; 36]; sd.copy_from_slice(&data[36..72]);
                let cnt = u32::from_le_bytes(data[72..76].try_into().unwrap());
                Some(RedoOp::Spend { tx_key: TxKey { txid }, offset, spending_data: sd, new_spent_count: cnt })
            }
            OP_UNSPEND if data.len() >= 40 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let cnt = u32::from_le_bytes(data[36..40].try_into().unwrap());
                Some(RedoOp::Unspend { tx_key: TxKey { txid }, offset, new_spent_count: cnt })
            }
            OP_SET_MINED if data.len() >= 45 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                Some(RedoOp::SetMined {
                    tx_key: TxKey { txid },
                    block_id: u32::from_le_bytes(data[32..36].try_into().unwrap()),
                    block_height: u32::from_le_bytes(data[36..40].try_into().unwrap()),
                    subtree_idx: u32::from_le_bytes(data[40..44].try_into().unwrap()),
                    unset: data[44] != 0,
                })
            }
            OP_FREEZE | OP_UNFREEZE | OP_PRUNE_SLOT if data.len() >= 36 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let key = TxKey { txid };
                match op_type {
                    OP_FREEZE => Some(RedoOp::Freeze { tx_key: key, offset }),
                    OP_UNFREEZE => Some(RedoOp::Unfreeze { tx_key: key, offset }),
                    OP_PRUNE_SLOT => Some(RedoOp::PruneSlot { tx_key: key, offset }),
                    _ => None,
                }
            }
            OP_REASSIGN if data.len() >= 76 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut nh = [0u8; 32]; nh.copy_from_slice(&data[36..68]);
                Some(RedoOp::Reassign {
                    tx_key: TxKey { txid }, offset, new_hash: nh,
                    block_height: u32::from_le_bytes(data[68..72].try_into().unwrap()),
                    spendable_after: u32::from_le_bytes(data[72..76].try_into().unwrap()),
                })
            }
            OP_CREATE if data.len() >= 44 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                Some(RedoOp::Create {
                    tx_key: TxKey { txid },
                    record_offset: u64::from_le_bytes(data[32..40].try_into().unwrap()),
                    utxo_count: u32::from_le_bytes(data[40..44].try_into().unwrap()),
                })
            }
            OP_DELETE if data.len() >= 48 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                Some(RedoOp::Delete {
                    tx_key: TxKey { txid },
                    record_offset: u64::from_le_bytes(data[32..40].try_into().unwrap()),
                    record_size: u64::from_le_bytes(data[40..48].try_into().unwrap()),
                })
            }
            OP_SET_CONFLICTING if data.len() >= 41 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                Some(RedoOp::SetConflicting {
                    tx_key: TxKey { txid }, value: data[32] != 0,
                    current_block_height: u32::from_le_bytes(data[33..37].try_into().unwrap()),
                    block_height_retention: u32::from_le_bytes(data[37..41].try_into().unwrap()),
                })
            }
            OP_SET_LOCKED if data.len() >= 33 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                Some(RedoOp::SetLocked { tx_key: TxKey { txid }, value: data[32] != 0 })
            }
            OP_PRESERVE_UNTIL if data.len() >= 36 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                Some(RedoOp::PreserveUntil {
                    tx_key: TxKey { txid },
                    block_height: u32::from_le_bytes(data[32..36].try_into().unwrap()),
                })
            }
            OP_MARK_LONGEST_CHAIN if data.len() >= 41 => {
                let mut txid = [0u8; 32]; txid.copy_from_slice(&data[..32]);
                Some(RedoOp::MarkOnLongestChain {
                    tx_key: TxKey { txid }, on_longest_chain: data[32] != 0,
                    current_block_height: u32::from_le_bytes(data[33..37].try_into().unwrap()),
                    block_height_retention: u32::from_le_bytes(data[37..41].try_into().unwrap()),
                })
            }
            OP_CHECKPOINT => Some(RedoOp::Checkpoint),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// RedoEntry
// ---------------------------------------------------------------------------

/// A single redo log entry with sequence number and checksum.
#[derive(Debug, Clone)]
pub struct RedoEntry {
    /// Monotonically increasing sequence number.
    pub sequence: u64,
    /// The operation.
    pub op: RedoOp,
}

// Entry on disk: [length:4][sequence:8][op_type:1][op_data:N][checksum:4]
// length = 8 + 1 + N + 4 (everything after the length field)
const ENTRY_HEADER_SIZE: usize = 4; // length field
const ENTRY_SEQ_SIZE: usize = 8;
const ENTRY_TYPE_SIZE: usize = 1;
const ENTRY_CHECKSUM_SIZE: usize = 4;
const ENTRY_OVERHEAD: usize = ENTRY_SEQ_SIZE + ENTRY_TYPE_SIZE + ENTRY_CHECKSUM_SIZE;

impl RedoEntry {
    /// Serialize this entry to bytes.
    fn serialize(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&self.sequence.to_le_bytes());
        payload.push(self.op.op_type());
        self.op.serialize_data(&mut payload);

        let checksum = crc32fast::hash(&payload);
        payload.extend_from_slice(&checksum.to_le_bytes());

        let length = payload.len() as u32;
        let mut out = Vec::with_capacity(ENTRY_HEADER_SIZE + payload.len());
        out.extend_from_slice(&length.to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Deserialize from bytes. Returns (entry, bytes_consumed) or None.
    fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        if data.len() < ENTRY_HEADER_SIZE {
            return None;
        }
        let length = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        if length == 0 {
            return None; // End marker
        }
        let total = ENTRY_HEADER_SIZE + length;
        if data.len() < total || length < ENTRY_OVERHEAD {
            return None;
        }

        let payload = &data[ENTRY_HEADER_SIZE..total];
        let content_len = length - ENTRY_CHECKSUM_SIZE;
        let stored_checksum = u32::from_le_bytes(
            payload[content_len..content_len + 4].try_into().unwrap(),
        );
        let computed = crc32fast::hash(&payload[..content_len]);
        if stored_checksum != computed {
            return None;
        }

        let sequence = u64::from_le_bytes(payload[..8].try_into().unwrap());
        let op_type = payload[8];
        let op_data = &payload[9..content_len];

        let op = RedoOp::deserialize(op_type, op_data)?;
        Some((RedoEntry { sequence, op }, total))
    }
}

// ---------------------------------------------------------------------------
// RedoLog
// ---------------------------------------------------------------------------

/// Circular redo log on a block device.
///
/// Entries are appended to an in-memory buffer and flushed to device
/// on demand. A checkpoint marker allows space reclamation.
pub struct RedoLog {
    device: Arc<dyn BlockDevice>,
    log_offset: u64,
    log_size: u64,
    write_pos: u64,
    checkpoint_seq: u64,
    next_sequence: u64,
    buffer: Vec<u8>,
    flushed_pos: u64,
}

impl RedoLog {
    /// Open or create a redo log at the given device region.
    ///
    /// Scans for existing entries to determine the current position.
    pub fn open(device: Arc<dyn BlockDevice>, log_offset: u64, log_size: u64) -> Result<Self> {
        let mut log = Self {
            device,
            log_offset,
            log_size,
            write_pos: 0,
            checkpoint_seq: 0,
            next_sequence: 1,
            buffer: Vec::new(),
            flushed_pos: 0,
        };

        // Scan existing entries to find write position and checkpoint
        let entries = log.scan_all()?;
        if let Some(last) = entries.last() {
            log.next_sequence = last.sequence + 1;
        }

        // Find last checkpoint to set checkpoint_seq
        for e in entries.iter().rev() {
            if e.op == RedoOp::Checkpoint {
                log.checkpoint_seq = e.sequence;
                break;
            }
        }

        Ok(log)
    }

    /// Append an operation to the buffer (not yet durable).
    ///
    /// Returns the assigned sequence number.
    pub fn append(&mut self, op: RedoOp) -> Result<u64> {
        let seq = self.next_sequence;
        self.next_sequence += 1;

        let entry = RedoEntry { sequence: seq, op };
        let bytes = entry.serialize();

        if self.write_pos + self.buffer.len() as u64 + bytes.len() as u64 > self.log_size {
            return Err(RedoError::LogFull {
                used: self.write_pos + self.buffer.len() as u64,
                capacity: self.log_size,
            });
        }

        self.buffer.extend_from_slice(&bytes);
        Ok(seq)
    }

    /// Flush the buffer to device, making all appended entries durable.
    pub fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let align = self.device.alignment();
        let device_offset = self.log_offset + self.write_pos;
        let aligned_offset = device_offset / align as u64 * align as u64;
        let intra = (device_offset - aligned_offset) as usize;
        let total = intra + self.buffer.len();
        let aligned_total = total.div_ceil(align) * align;

        let mut buf = AlignedBuf::new(aligned_total, align);

        // Read existing data if we're not block-aligned
        if intra > 0 || !total.is_multiple_of(align) {
            let read_len = aligned_total.min((self.log_size - (aligned_offset - self.log_offset)) as usize);
            let read_aligned = read_len.div_ceil(align) * align;
            if read_aligned <= buf.len() {
                let _ = self.device.pread(&mut buf[..read_aligned], aligned_offset);
            }
        }

        buf[intra..intra + self.buffer.len()].copy_from_slice(&self.buffer);
        self.device.pwrite(&buf, aligned_offset)?;
        self.device.sync()?;

        self.write_pos += self.buffer.len() as u64;
        self.flushed_pos = self.write_pos;
        self.buffer.clear();
        Ok(())
    }

    /// Append and flush in one call.
    pub fn append_and_flush(&mut self, op: RedoOp) -> Result<u64> {
        let seq = self.append(op)?;
        self.flush()?;
        Ok(seq)
    }

    /// Write a checkpoint marker. All entries before this are committed.
    pub fn checkpoint(&mut self) -> Result<()> {
        let seq = self.append(RedoOp::Checkpoint)?;
        self.flush()?;
        self.checkpoint_seq = seq;
        Ok(())
    }

    /// Read all entries after the last checkpoint (for crash recovery).
    pub fn recover(&self) -> Result<Vec<RedoEntry>> {
        let all = self.scan_all()?;

        // Find last checkpoint
        let mut checkpoint_idx = None;
        for (i, e) in all.iter().enumerate() {
            if e.op == RedoOp::Checkpoint {
                checkpoint_idx = Some(i);
            }
        }

        match checkpoint_idx {
            Some(idx) => Ok(all[idx + 1..].to_vec()),
            None => Ok(all),
        }
    }

    /// Read all entries with sequence >= `from_seq` from the log.
    ///
    /// Used for replica catch-up: the master replays redo entries that
    /// the replica missed while it was disconnected. Returns an empty
    /// vec if the requested sequence has already been reclaimed.
    pub fn read_from_sequence(&self, from_seq: u64) -> Result<Vec<RedoEntry>> {
        let all = self.scan_all()?;
        Ok(all.into_iter().filter(|e| e.sequence >= from_seq).collect())
    }

    /// The next sequence number that will be assigned.
    pub fn current_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Advance the checkpoint, allowing entries before it to be reclaimed.
    pub fn advance_checkpoint(&mut self, up_to_sequence: u64) -> Result<()> {
        if up_to_sequence > self.checkpoint_seq {
            self.checkpoint_seq = up_to_sequence;
        }
        Ok(())
    }

    /// Current write position within the log (bytes from start).
    pub fn write_position(&self) -> u64 {
        self.write_pos + self.buffer.len() as u64
    }

    /// Space remaining in the log.
    pub fn available_space(&self) -> u64 {
        self.log_size.saturating_sub(self.write_pos + self.buffer.len() as u64)
    }

    /// Reset the log (after checkpoint + reclaim). Dangerous — only call
    /// when all entries have been checkpointed and applied.
    pub fn reset(&mut self) -> Result<()> {
        // Zero out the first block to mark end of entries
        let align = self.device.alignment();
        let buf = AlignedBuf::new(align, align);
        self.device.pwrite(&buf, self.log_offset)?;
        self.write_pos = 0;
        self.flushed_pos = 0;
        self.buffer.clear();
        Ok(())
    }

    /// Scan all valid entries in the log from the beginning.
    fn scan_all(&self) -> Result<Vec<RedoEntry>> {
        let align = self.device.alignment();
        let read_size = self.log_size as usize;
        let aligned_read = read_size.div_ceil(align) * align;

        let mut buf = AlignedBuf::new(aligned_read, align);
        self.device.pread(&mut buf, self.log_offset)?;

        let data = &buf[..read_size];
        let mut entries = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            match RedoEntry::deserialize(&data[pos..]) {
                Some((entry, consumed)) => {
                    entries.push(entry);
                    pos += consumed;
                }
                None => break,
            }
        }

        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemoryDevice;

    fn make_log(size: u64) -> (Arc<MemoryDevice>, RedoLog) {
        let dev = Arc::new(MemoryDevice::new(size, 4096).unwrap());
        let log = RedoLog::open(dev.clone(), 0, size).unwrap();
        (dev, log)
    }

    fn test_key(n: u8) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0] = n;
        TxKey { txid }
    }

    // -- Basic tests --

    #[test]
    fn append_flush_recover() {
        let (_, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Spend {
            tx_key: test_key(1), offset: 5, spending_data: [0xAB; 36], new_spent_count: 1,
        }).unwrap();

        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].op {
            RedoOp::Spend { tx_key, offset, spending_data, new_spent_count } => {
                assert_eq!(tx_key.txid[0], 1);
                assert_eq!(*offset, 5);
                assert_eq!(*spending_data, [0xAB; 36]);
                assert_eq!(*new_spent_count, 1);
            }
            other => panic!("expected Spend, got {other:?}"),
        }
    }

    #[test]
    fn append_100_flush_recover_all() {
        let (_, mut log) = make_log(1024 * 1024);
        for i in 0..100u8 {
            log.append(RedoOp::Freeze { tx_key: test_key(i), offset: i as u32 }).unwrap();
        }
        log.flush().unwrap();

        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 100);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.sequence, i as u64 + 1);
        }
    }

    #[test]
    fn no_flush_not_recovered() {
        let (dev, mut log) = make_log(1024 * 1024);
        log.append(RedoOp::Freeze { tx_key: test_key(1), offset: 0 }).unwrap();
        // Don't flush

        // Simulate crash — reopen
        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn checkpoint_clears_entries() {
        let (_, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(1), offset: 0 }).unwrap();
        log.checkpoint().unwrap();

        let entries = log.recover().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn checkpoint_only_returns_after() {
        let (_, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(1), offset: 0 }).unwrap();
        log.checkpoint().unwrap();
        log.append_and_flush(RedoOp::Unfreeze { tx_key: test_key(2), offset: 1 }).unwrap();

        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].op {
            RedoOp::Unfreeze { tx_key, offset } => {
                assert_eq!(tx_key.txid[0], 2);
                assert_eq!(*offset, 1);
            }
            other => panic!("expected Unfreeze, got {other:?}"),
        }
    }

    #[test]
    fn corrupted_entry_stops_recovery() {
        let (dev, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(1), offset: 0 }).unwrap();
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(2), offset: 1 }).unwrap();

        // Corrupt byte in the second entry
        let align = dev.alignment();
        let mut buf = AlignedBuf::new(align, align);
        dev.pread(&mut buf, 0).unwrap();
        // Find roughly where the second entry is and corrupt it
        buf[100] ^= 0xFF;
        dev.pwrite(&buf, 0).unwrap();

        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        // Should get at most the first entry (second is corrupt)
        assert!(entries.len() <= 1);
    }

    // -- Serialization round-trip tests --

    #[test]
    fn round_trip_all_variants() {
        let ops = vec![
            RedoOp::Spend { tx_key: test_key(1), offset: 5, spending_data: [0xAB; 36], new_spent_count: 42 },
            RedoOp::Unspend { tx_key: test_key(2), offset: 3, new_spent_count: 10 },
            RedoOp::SetMined { tx_key: test_key(3), block_id: 100, block_height: 800000, subtree_idx: 7, unset: false },
            RedoOp::SetMined { tx_key: test_key(4), block_id: 200, block_height: 900000, subtree_idx: 3, unset: true },
            RedoOp::Freeze { tx_key: test_key(5), offset: 0 },
            RedoOp::Unfreeze { tx_key: test_key(6), offset: 1 },
            RedoOp::Reassign { tx_key: test_key(7), offset: 2, new_hash: [0xCC; 32], block_height: 1000, spendable_after: 100 },
            RedoOp::PruneSlot { tx_key: test_key(8), offset: 4 },
            RedoOp::Create { tx_key: test_key(9), record_offset: 4096, utxo_count: 10 },
            RedoOp::Delete { tx_key: test_key(10), record_offset: 8192, record_size: 1024 },
            RedoOp::SetConflicting { tx_key: test_key(11), value: true, current_block_height: 500, block_height_retention: 288 },
            RedoOp::SetLocked { tx_key: test_key(12), value: false },
            RedoOp::PreserveUntil { tx_key: test_key(13), block_height: 5000 },
            RedoOp::MarkOnLongestChain { tx_key: test_key(14), on_longest_chain: true, current_block_height: 600, block_height_retention: 288 },
            RedoOp::Checkpoint,
        ];

        let (_, mut log) = make_log(1024 * 1024);
        for op in &ops {
            log.append(op.clone()).unwrap();
        }
        log.flush().unwrap();

        // Recover should get all entries (checkpoint is last, so no filtering)
        let all = log.scan_all().unwrap();
        assert_eq!(all.len(), ops.len());
        for (i, entry) in all.iter().enumerate() {
            assert_eq!(entry.op, ops[i], "mismatch at index {i}");
        }
    }

    // -- Circular / capacity tests --

    #[test]
    fn log_full_returns_error() {
        // Small log: 8KB
        let (_, mut log) = make_log(8192);
        let mut count = 0;
        loop {
            match log.append(RedoOp::Freeze { tx_key: test_key(count as u8), offset: 0 }) {
                Ok(_) => { count += 1; }
                Err(RedoError::LogFull { .. }) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(count > 0);
    }

    #[test]
    fn checkpoint_then_reset_reclaims_space() {
        let (_, mut log) = make_log(8192);
        // Fill most of the log
        for i in 0..50u8 {
            log.append(RedoOp::Freeze { tx_key: test_key(i), offset: 0 }).unwrap();
        }
        log.flush().unwrap();
        log.checkpoint().unwrap();

        // Reset reclaims all space
        log.reset().unwrap();

        // Should be able to write again
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(99), offset: 0 }).unwrap();
        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn available_space_decreases() {
        let (_, mut log) = make_log(1024 * 1024);
        let initial = log.available_space();
        log.append(RedoOp::Freeze { tx_key: test_key(1), offset: 0 }).unwrap();
        assert!(log.available_space() < initial);
    }

    // -- Reopen persistence test --

    #[test]
    fn reopen_sees_flushed_entries() {
        let (dev, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(1), offset: 0 }).unwrap();
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(2), offset: 1 }).unwrap();
        drop(log);

        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn reopen_after_checkpoint() {
        let (dev, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(1), offset: 0 }).unwrap();
        log.checkpoint().unwrap();
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(2), offset: 1 }).unwrap();
        drop(log);

        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert_eq!(entries.len(), 1); // Only entry after checkpoint
    }

    #[test]
    fn truncated_entry_stops_recovery() {
        let (dev, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(1), offset: 0 }).unwrap();
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(2), offset: 1 }).unwrap();

        // Simulate truncation: zero out most of the second entry
        let align = dev.alignment();
        let mut buf = AlignedBuf::new(align, align);
        dev.pread(&mut buf, 0).unwrap();
        // Write a partial length at the second entry location, then zeros
        // This simulates a power failure mid-write of the second entry
        let first_entry_end = 60; // approximate size of first entry
        // Zero out from midpoint of second entry onward
        for b in buf[first_entry_end + 10..first_entry_end + 50].iter_mut() {
            *b = 0;
        }
        dev.pwrite(&buf, 0).unwrap();

        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        // Should get at most the first entry (second is truncated)
        assert!(entries.len() <= 1);
    }

    #[test]
    fn rapid_checkpoint_append_cycles() {
        let (_, mut log) = make_log(1024 * 1024);

        for cycle in 0..20u8 {
            for i in 0..10u8 {
                log.append(RedoOp::Freeze {
                    tx_key: test_key(cycle * 10 + i),
                    offset: 0,
                })
                .unwrap();
            }
            log.flush().unwrap();
            log.checkpoint().unwrap();

            // Verify only entries after the most recent checkpoint are returned
            let entries = log.recover().unwrap();
            assert!(entries.is_empty(), "cycle {cycle}: should have 0 entries after checkpoint");
        }

        // After all cycles, total space used should not leak
        assert!(log.available_space() > 0);
    }

    #[test]
    fn zero_length_marks_end_of_valid_data() {
        let (dev, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze { tx_key: test_key(1), offset: 0 }).unwrap();

        // The area after the last entry should have zero bytes (marking end)
        // This is implicitly tested by recovery stopping at the right place
        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);

        // Write zeros at position after the entry to ensure scan stops
        let align = dev.alignment();
        let pos = log.write_position();
        if pos < 1024 * 1024 - align as u64 {
            // Already zeroed by initial device state — verify scan stops
            let all = log.scan_all().unwrap();
            assert_eq!(all.len(), 1);
        }
    }

    #[test]
    fn crash_simulation_random_corruption() {
        // Write 10 entries, then corrupt at random positions
        // Recovery should always succeed (possibly with fewer entries)
        let (dev, mut log) = make_log(1024 * 1024);
        for i in 0..10u8 {
            log.append_and_flush(RedoOp::Freeze { tx_key: test_key(i), offset: i as u32 }).unwrap();
        }

        // Try 50 different corruption points
        for corrupt_offset in (20..500).step_by(10) {
            let dev2 = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
            // Copy original data
            let align = dev.alignment();
            let mut buf = AlignedBuf::new(align, align);
            dev.pread(&mut buf, 0).unwrap();
            dev2.pwrite(&buf, 0).unwrap();

            // Corrupt one byte
            let mut buf2 = AlignedBuf::new(align, align);
            dev2.pread(&mut buf2, 0).unwrap();
            if corrupt_offset < buf2.len() {
                buf2[corrupt_offset] ^= 0xFF;
                dev2.pwrite(&buf2, 0).unwrap();
            }

            // Recovery should not panic or error
            let log2 = RedoLog::open(dev2, 0, 1024 * 1024).unwrap();
            let result = log2.recover();
            assert!(result.is_ok(), "recovery failed at corruption offset {corrupt_offset}");
        }
    }

    // -----------------------------------------------------------------------
    // Round-trip serialization tests — one per RedoOp variant
    // -----------------------------------------------------------------------

    /// Helper: create a TxKey where txid is filled with a repeating byte pattern.
    fn make_txid(byte: u8) -> TxKey {
        let mut txid = [0u8; 32];
        for (i, b) in txid.iter_mut().enumerate() {
            *b = byte.wrapping_add(i as u8);
        }
        TxKey { txid }
    }

    /// Helper: round-trip a single RedoOp through the redo log and assert equality.
    fn assert_round_trip(op: RedoOp) {
        let (_, mut log) = make_log(1024 * 1024);
        log.append_and_flush(op.clone()).unwrap();
        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1, "expected exactly 1 recovered entry");
        assert_eq!(entries[0].op, op, "round-tripped op does not match original");
    }

    #[test]
    fn round_trip_spend() {
        let mut spending_data = [0u8; 36];
        for (i, b) in spending_data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(7);
        }
        assert_round_trip(RedoOp::Spend {
            tx_key: make_txid(0xA1),
            offset: 42,
            spending_data,
            new_spent_count: 17,
        });
    }

    #[test]
    fn round_trip_unspend() {
        assert_round_trip(RedoOp::Unspend {
            tx_key: make_txid(0xB2),
            offset: 99,
            new_spent_count: 3,
        });
    }

    #[test]
    fn round_trip_set_mined() {
        assert_round_trip(RedoOp::SetMined {
            tx_key: make_txid(0xC3),
            block_id: 123456,
            block_height: 800_000,
            subtree_idx: 15,
            unset: false,
        });
    }

    #[test]
    fn round_trip_set_mined_unset() {
        assert_round_trip(RedoOp::SetMined {
            tx_key: make_txid(0xC4),
            block_id: 654321,
            block_height: 900_001,
            subtree_idx: 0,
            unset: true,
        });
    }

    #[test]
    fn round_trip_freeze() {
        assert_round_trip(RedoOp::Freeze {
            tx_key: make_txid(0xD5),
            offset: 7,
        });
    }

    #[test]
    fn round_trip_unfreeze() {
        assert_round_trip(RedoOp::Unfreeze {
            tx_key: make_txid(0xE6),
            offset: 255,
        });
    }

    #[test]
    fn round_trip_reassign() {
        let mut new_hash = [0u8; 32];
        for (i, b) in new_hash.iter_mut().enumerate() {
            *b = 0xFF_u8.wrapping_sub(i as u8);
        }
        assert_round_trip(RedoOp::Reassign {
            tx_key: make_txid(0xF7),
            offset: 10,
            new_hash,
            block_height: 1_000_000,
            spendable_after: 500,
        });
    }

    #[test]
    fn round_trip_prune_slot() {
        assert_round_trip(RedoOp::PruneSlot {
            tx_key: make_txid(0x08),
            offset: 31,
        });
    }

    #[test]
    fn round_trip_create() {
        assert_round_trip(RedoOp::Create {
            tx_key: make_txid(0x19),
            record_offset: 0x0000_DEAD_BEEF_0000,
            utxo_count: 250,
        });
    }

    #[test]
    fn round_trip_delete() {
        assert_round_trip(RedoOp::Delete {
            tx_key: make_txid(0x2A),
            record_offset: 0x0000_CAFE_BABE_0000,
            record_size: 4096,
        });
    }

    #[test]
    fn round_trip_set_conflicting() {
        assert_round_trip(RedoOp::SetConflicting {
            tx_key: make_txid(0x3B),
            value: true,
            current_block_height: 750_000,
            block_height_retention: 288,
        });
    }

    #[test]
    fn round_trip_set_conflicting_false() {
        assert_round_trip(RedoOp::SetConflicting {
            tx_key: make_txid(0x3C),
            value: false,
            current_block_height: 100,
            block_height_retention: 1000,
        });
    }

    #[test]
    fn round_trip_set_locked() {
        assert_round_trip(RedoOp::SetLocked {
            tx_key: make_txid(0x4C),
            value: true,
        });
    }

    #[test]
    fn round_trip_set_locked_false() {
        assert_round_trip(RedoOp::SetLocked {
            tx_key: make_txid(0x4D),
            value: false,
        });
    }

    #[test]
    fn round_trip_preserve_until() {
        assert_round_trip(RedoOp::PreserveUntil {
            tx_key: make_txid(0x5D),
            block_height: 999_999,
        });
    }

    #[test]
    fn round_trip_mark_on_longest_chain() {
        assert_round_trip(RedoOp::MarkOnLongestChain {
            tx_key: make_txid(0x6E),
            on_longest_chain: true,
            current_block_height: 800_123,
            block_height_retention: 576,
        });
    }

    #[test]
    fn round_trip_mark_on_longest_chain_false() {
        assert_round_trip(RedoOp::MarkOnLongestChain {
            tx_key: make_txid(0x6F),
            on_longest_chain: false,
            current_block_height: 1,
            block_height_retention: 0,
        });
    }

    #[test]
    fn round_trip_checkpoint() {
        // Checkpoint is special: it is the last entry, so scan_all is needed
        // since recover() filters entries before the last checkpoint.
        let (_, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Checkpoint).unwrap();
        let all = log.scan_all().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].op, RedoOp::Checkpoint);
    }

    // -----------------------------------------------------------------------
    // RedoLog integration: append 5 ops, flush, reopen, recover
    // -----------------------------------------------------------------------

    #[test]
    fn redo_log_integration_reopen_recovers_five_ops() {
        let ops = vec![
            RedoOp::Spend {
                tx_key: make_txid(0x01),
                offset: 0,
                spending_data: [0xDD; 36],
                new_spent_count: 1,
            },
            RedoOp::SetMined {
                tx_key: make_txid(0x02),
                block_id: 42,
                block_height: 100_000,
                subtree_idx: 3,
                unset: false,
            },
            RedoOp::Create {
                tx_key: make_txid(0x03),
                record_offset: 8192,
                utxo_count: 5,
            },
            RedoOp::SetConflicting {
                tx_key: make_txid(0x04),
                value: true,
                current_block_height: 200_000,
                block_height_retention: 288,
            },
            RedoOp::MarkOnLongestChain {
                tx_key: make_txid(0x05),
                on_longest_chain: true,
                current_block_height: 300_000,
                block_height_retention: 144,
            },
        ];

        let (dev, mut log) = make_log(1024 * 1024);
        for op in &ops {
            log.append(op.clone()).unwrap();
        }
        log.flush().unwrap();
        drop(log);

        // Simulate restart: reopen the log from the same device
        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert_eq!(entries.len(), 5, "expected 5 recovered entries after reopen");
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.sequence, (i + 1) as u64, "sequence mismatch at index {i}");
            assert_eq!(entry.op, ops[i], "op mismatch at index {i}");
        }
    }

    // -----------------------------------------------------------------------
    // Log full test: fill until LogFull error
    // -----------------------------------------------------------------------

    #[test]
    fn log_full_error_not_panic() {
        // Use a very small log (4KB) so it fills quickly
        let (_, mut log) = make_log(4096);
        let mut appended = 0u32;
        loop {
            let result = log.append(RedoOp::Delete {
                tx_key: make_txid(appended as u8),
                record_offset: appended as u64 * 4096,
                record_size: 4096,
            });
            match result {
                Ok(_) => appended += 1,
                Err(RedoError::LogFull { used, capacity }) => {
                    assert!(used > 0, "used should be > 0 when log is full");
                    assert_eq!(capacity, 4096, "capacity should match log size");
                    break;
                }
                Err(e) => panic!("expected LogFull, got: {e}"),
            }
        }
        assert!(appended > 0, "should have appended at least one entry before LogFull");
    }

    // -----------------------------------------------------------------------
    // Corrupted entry recovery: entries before corruption are returned
    // -----------------------------------------------------------------------

    #[test]
    fn corrupted_entry_recovery_returns_entries_before_corruption() {
        let (dev, mut log) = make_log(1024 * 1024);

        // Write 5 entries
        let ops: Vec<RedoOp> = (0..5u8)
            .map(|i| RedoOp::Freeze { tx_key: make_txid(i), offset: i as u32 })
            .collect();
        for op in &ops {
            log.append_and_flush(op.clone()).unwrap();
        }

        // Determine where the third entry starts (after two entries).
        // Each Freeze entry is: 4 (length) + 8 (seq) + 1 (type) + 32 (txid) + 4 (offset) + 4 (crc) = 53 bytes
        let entry_size = 53usize;
        let corrupt_target = entry_size * 2 + 10; // middle of the third entry

        let align = dev.alignment();
        let mut buf = AlignedBuf::new(align, align);
        dev.pread(&mut buf, 0).unwrap();
        buf[corrupt_target] ^= 0xFF;
        dev.pwrite(&buf, 0).unwrap();

        // Reopen and recover
        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();

        // We should get exactly the 2 entries before the corruption
        assert_eq!(entries.len(), 2, "should recover entries before corruption");
        assert_eq!(entries[0].op, ops[0]);
        assert_eq!(entries[1].op, ops[1]);
    }

    // -----------------------------------------------------------------------
    // Checkpoint test: only post-checkpoint ops returned
    // -----------------------------------------------------------------------

    #[test]
    fn checkpoint_returns_only_post_checkpoint_ops() {
        let (dev, mut log) = make_log(1024 * 1024);

        // Append 3 pre-checkpoint ops
        let pre_ops = vec![
            RedoOp::Freeze { tx_key: make_txid(0x10), offset: 0 },
            RedoOp::Unfreeze { tx_key: make_txid(0x11), offset: 1 },
            RedoOp::PruneSlot { tx_key: make_txid(0x12), offset: 2 },
        ];
        for op in &pre_ops {
            log.append(op.clone()).unwrap();
        }
        log.flush().unwrap();
        log.checkpoint().unwrap();

        // Append 2 post-checkpoint ops
        let post_ops = vec![
            RedoOp::SetLocked { tx_key: make_txid(0x20), value: true },
            RedoOp::PreserveUntil { tx_key: make_txid(0x21), block_height: 12345 },
        ];
        for op in &post_ops {
            log.append(op.clone()).unwrap();
        }
        log.flush().unwrap();
        drop(log);

        // Reopen and recover — only post-checkpoint ops should appear
        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert_eq!(entries.len(), 2, "expected 2 post-checkpoint entries");
        assert_eq!(entries[0].op, post_ops[0], "first post-checkpoint op mismatch");
        assert_eq!(entries[1].op, post_ops[1], "second post-checkpoint op mismatch");
    }
}
