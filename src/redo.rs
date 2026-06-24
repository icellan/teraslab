//! Linear-with-reset redo log for crash recovery.
//!
//! Operations are appended to the log before data writes. On crash recovery,
//! all entries after the last checkpoint are replayed idempotently.
//!
//! R-027 (BC-13): the on-disk layout is **linear**, not circular. `write_pos`
//! advances monotonically and is only ever rewound by reclaiming an already
//! durable prefix; there is no in-place wrap. A full log returns
//! [`RedoError::LogFull`] until the checkpoint task frees space. The prior
//! module documentation called this "circular" and described "wrapping
//! around", which set false expectations. The naming has been corrected here;
//! the public type names retain `RedoLog` for back-compat.
//!
//! ## Production checkpoint / reclamation flow
//!
//! The periodic checkpoint task (see [`crate::checkpoint`]) reclaims log
//! space without writing a `Checkpoint` marker:
//!
//! 1. It snapshots the engine state to a durable index snapshot and runs the
//!    data/index barrier (fsync).
//! 2. It writes a **recovery-progress fence** via
//!    [`RedoLog::mark_recovery_progress`] at the sequence the snapshot covers.
//!    This is *not* a whole-engine checkpoint: recovery must still replay any
//!    post-fence entries, because non-dispatch producers can append while the
//!    snapshot is being written.
//! 3. It reclaims only the covered prefix with
//!    [`RedoLog::compact_prefix_through`], which rewinds `write_pos` past the
//!    fenced entries while leaving every entry after the fence intact.
//!    Sequence numbers continue monotonically across the reclaim.
//!
//! The legacy [`RedoLog::mark_checkpoint`] (which appends a
//! [`RedoOp::Checkpoint`] marker) and the wholesale [`RedoLog::reset`] (which
//! rewinds `write_pos` to zero) are **test-only**: production never calls
//! them. [`RedoOp::Checkpoint`] is still recognised on replay (skipped, see
//! [`crate::recovery`]) so logs written by older builds remain readable.
//!
//! ## On-disk layout (F-G4-001)
//!
//! The redo region starts with a fixed-size `RedoHeader` occupying the
//! first aligned block (`HEADER_BLOCK_SIZE` bytes, equal to the device's
//! alignment, typically 4 KiB). The header carries:
//!
//! * a magic + format version so older logs are rejected with a clear
//!   "version not supported" error rather than silently misparsing;
//! * the high-water `next_sequence`, persisted on every flush /
//!   compaction / reset so a restart after `compact_prefix_through` reduced
//!   the log to empty does NOT reseed sequence numbers from 1 (which
//!   prior to F-G4-001 silently corrupted replication watermarks);
//! * the last `checkpoint_seq`, in case the on-disk entries no longer
//!   include a `Checkpoint` marker after compaction.
//! * a CRC-32 over the rest of the header; an invalid CRC fails `open()`
//!   with a versioning error rather than silently falling back to "scan
//!   from offset 0 with `next_sequence = 1`".
//!
//! Entries are appended starting at byte `HEADER_BLOCK_SIZE` of the redo
//! region; `write_pos` is offset RELATIVE to the entries region. Capacity
//! reported via [`RedoLog::capacity`] is the entries-only size
//! (`log_size - HEADER_BLOCK_SIZE`).

use crate::device::{AlignedBuf, BlockDevice};
use crate::index::TxKey;
use crate::metrics::redo_metrics;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Instant;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Stable prefix of the [`RedoError::LogFull`] `Display` string.
///
/// Some call paths flatten `RedoError` into a `String` (e.g. the
/// replication intent-recovery path returns `Result<(), String>`).
/// Callers that must discriminate transient redo backpressure from
/// terminal device faults match against this prefix instead of
/// hard-coding the literal, so the `Display` format and the discriminator
/// cannot drift apart. The full `Display` is
/// `"redo log full: {used}/{capacity} bytes used"`.
pub const LOG_FULL_MESSAGE_PREFIX: &str = "redo log full";

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

    /// Redo entry sequence numbers must be contiguous within a log epoch.
    #[error(
        "redo sequence out of order at offset {offset}: previous={previous}, current={current}"
    )]
    SequenceOutOfOrder {
        offset: u64,
        previous: u64,
        current: u64,
    },

    /// The requested log region (`log_offset + log_size`) does not fit
    /// within the backing device. Rejected at open-time so callers never
    /// perform an I/O past the end of the device.
    #[error(
        "redo log region out of bounds: offset {log_offset} + size {log_size} > device size {device_size}"
    )]
    OutOfBounds {
        log_offset: u64,
        log_size: u64,
        device_size: u64,
    },

    /// F-G4-001: the header block carries a magic byte string that does not
    /// match the current redo-log format. Either a foreign region was
    /// pointed at, or the log was written by a version of the binary that
    /// used a different (older or newer) magic. Recovery refuses to open
    /// rather than silently misparsing as the current format.
    #[error(
        "redo header magic mismatch: expected {expected:#x?}, found {found:#x?} — log was written by an incompatible version"
    )]
    HeaderMagicMismatch { expected: [u8; 8], found: [u8; 8] },

    /// F-G4-001: the header block CRC does not match the rest of the
    /// header bytes. The header was corrupted (torn write, device
    /// corruption, etc.). Recovery refuses to open rather than silently
    /// falling back to "scan + seed `next_sequence = 1`", which would
    /// reuse sequence numbers and silently break replication watermarks.
    #[error("redo header CRC mismatch: stored {stored:#x}, computed {computed:#x}")]
    HeaderCrcMismatch { stored: u32, computed: u32 },

    /// F-G4-001: the header carries a format version this binary does not
    /// know how to interpret. Reject at open rather than guessing.
    #[error("redo header version {found} not supported (expected {expected})")]
    UnsupportedHeaderVersion { expected: u16, found: u16 },

    /// F-G4-001: the redo region is too small to hold even the fixed-size
    /// header block plus a single aligned entry block.
    #[error(
        "redo log region too small: {log_size} bytes (header block requires {required_for_header})"
    )]
    LogRegionTooSmall {
        log_size: u64,
        required_for_header: u64,
    },

    /// F-G4-002: a prior `flush()` returned an I/O error and the in-memory
    /// state is no longer trustworthy. The log is poisoned; subsequent
    /// `append`/`flush` calls fail until the process restarts and recovers
    /// from the on-disk state.
    #[error("redo log poisoned by earlier flush failure; restart required")]
    Poisoned,
}

pub type Result<T> = std::result::Result<T, RedoError>;

// ---------------------------------------------------------------------------
// RedoHeader (F-G4-001)
// ---------------------------------------------------------------------------

/// Magic bytes identifying a TeraSlab redo log written in the
/// header-bearing format introduced by F-G4-001. Any prior on-disk
/// representation (entries written directly at `log_offset` with no
/// header) is rejected at open with [`RedoError::HeaderMagicMismatch`].
const REDO_HEADER_MAGIC: [u8; 8] = *b"TSLREDO1";

/// Current redo-log format version. Bumping this rejects older logs at
/// open with [`RedoError::UnsupportedHeaderVersion`] rather than silently
/// misparsing them.
///
/// Version 2 (B-3) appends a `logical_start` field after `checkpoint_seq`
/// recording the byte offset (relative to the entries region) at which
/// the first live entry begins. Version-1 logs are decoded with
/// `logical_start = 0` (the historical implicit start), so existing
/// on-disk logs upgrade transparently on the next compaction.
const REDO_HEADER_VERSION: u16 = 2;

/// On-disk layout of the redo-region header block (F-G4-001, B-3).
///
/// Layout: `magic(8) | version(2) | reserved(2) | next_sequence(8) |
/// checkpoint_seq(8) | logical_start(8) | crc32(4)` = 40 bytes; written
/// into the first `HEADER_BLOCK_SIZE` bytes of the redo region with the
/// remaining bytes zeroed. The CRC covers every byte before it.
///
/// A version-1 header (B-3 predecessor) is 32 bytes and omits the
/// `logical_start` field; [`RedoHeader::deserialize`] decodes both.
const HEADER_FIXED_LEN: usize = 8 + 2 + 2 + 8 + 8 + 8 + 4;

/// Byte length of the legacy version-1 header (no `logical_start`).
const HEADER_FIXED_LEN_V1: usize = 8 + 2 + 2 + 8 + 8 + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RedoHeader {
    next_sequence: u64,
    checkpoint_seq: u64,
    /// B-3: byte offset (relative to the entries region) of the first
    /// live entry. Compaction advances this instead of physically
    /// rewriting retained entries to the front of the region, so a torn
    /// compaction write can never destroy a durable (possibly acked)
    /// retained entry. Always `0` immediately after [`RedoLog::reset`].
    logical_start: u64,
}

impl RedoHeader {
    /// Serialize the header into a fresh `Vec<u8>` of length
    /// [`HEADER_FIXED_LEN`]. Callers should pad to the chosen block size
    /// (typically the device alignment) before writing to the device.
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_FIXED_LEN);
        buf.extend_from_slice(&REDO_HEADER_MAGIC);
        buf.extend_from_slice(&REDO_HEADER_VERSION.to_le_bytes());
        buf.extend_from_slice(&[0u8; 2]); // reserved
        buf.extend_from_slice(&self.next_sequence.to_le_bytes());
        buf.extend_from_slice(&self.checkpoint_seq.to_le_bytes());
        buf.extend_from_slice(&self.logical_start.to_le_bytes());
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        debug_assert_eq!(buf.len(), HEADER_FIXED_LEN);
        buf
    }

    /// Parse a header from the prefix of `data`.
    ///
    /// Returns [`RedoError::HeaderMagicMismatch`] if the magic byte string
    /// does not match the current format (rejecting older logs and foreign
    /// regions), [`RedoError::UnsupportedHeaderVersion`] for a known-magic
    /// but unknown-version header, [`RedoError::HeaderCrcMismatch`] if the
    /// CRC is corrupt.
    fn deserialize(data: &[u8]) -> Result<Self> {
        // The version-1 length is the smallest header we can parse; the
        // version field then selects how many further bytes are present.
        if data.len() < HEADER_FIXED_LEN_V1 {
            // Treat too-short region as magic mismatch so the error is
            // self-describing: callers see "expected vs found magic" with
            // the bytes that were actually present.
            let mut found = [0u8; 8];
            let copy_len = data.len().min(8);
            found[..copy_len].copy_from_slice(&data[..copy_len]);
            return Err(RedoError::HeaderMagicMismatch {
                expected: REDO_HEADER_MAGIC,
                found,
            });
        }
        let mut found_magic = [0u8; 8];
        found_magic.copy_from_slice(&data[..8]);
        if found_magic != REDO_HEADER_MAGIC {
            return Err(RedoError::HeaderMagicMismatch {
                expected: REDO_HEADER_MAGIC,
                found: found_magic,
            });
        }
        let version = u16::from_le_bytes(data[8..10].try_into().unwrap());
        if version > REDO_HEADER_VERSION {
            return Err(RedoError::UnsupportedHeaderVersion {
                expected: REDO_HEADER_VERSION,
                found: version,
            });
        }
        // skip reserved 2 bytes
        let next_sequence = u64::from_le_bytes(data[12..20].try_into().unwrap());
        let checkpoint_seq = u64::from_le_bytes(data[20..28].try_into().unwrap());
        // B-3: version 2 appends `logical_start(8)` before the CRC.
        // Version 1 has the CRC immediately at byte 28 and no
        // `logical_start` (implicit 0).
        let (logical_start, crc_off) = if version >= 2 {
            if data.len() < HEADER_FIXED_LEN {
                let mut found = [0u8; 8];
                found.copy_from_slice(&data[..8]);
                return Err(RedoError::HeaderMagicMismatch {
                    expected: REDO_HEADER_MAGIC,
                    found,
                });
            }
            (u64::from_le_bytes(data[28..36].try_into().unwrap()), 36)
        } else {
            (0u64, 28)
        };
        let stored_crc = u32::from_le_bytes(data[crc_off..crc_off + 4].try_into().unwrap());
        let computed = crc32fast::hash(&data[..crc_off]);
        if stored_crc != computed {
            return Err(RedoError::HeaderCrcMismatch {
                stored: stored_crc,
                computed,
            });
        }
        Ok(RedoHeader {
            next_sequence,
            checkpoint_seq,
            logical_start,
        })
    }
}

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
const OP_SECONDARY_UNMINED_UPDATE: u8 = 16;
const OP_SECONDARY_DAH_UPDATE: u8 = 17;
const OP_ALLOCATE_REGION: u8 = 18;
const OP_FREE_REGION: u8 = 19;
const OP_HASHTABLE_RESIZE_BEGIN: u8 = 20;
const OP_HASHTABLE_RESIZE_COMMIT: u8 = 21;
/// Gap #2: full-payload Create entry. Carries every byte the engine
/// wrote at `record_offset` (metadata + UTXO slots + cold data) plus
/// the parent_txids needed to rebuild conflicting-child links. Recovery
/// can reconstruct the on-device record bit-for-bit identical to a
/// successful create. The legacy [`OP_CREATE`] tag is retained for
/// back-compat decoding of redo logs written before this change.
const OP_CREATE_V2: u8 = 22;
/// Gap #8: compensation intent for unset-mined. Carries the prior
/// `block_height` + `subtree_idx` captured BEFORE the engine applied the
/// unset, so a crash mid-rollback can restore the block entry exactly.
const OP_COMPENSATE_UNSET_MINED: u8 = 23;
/// Gap #8: compensation intent for reassign. Carries the prior
/// `utxo_hash` of the slot before reassign so rollback restores the
/// original hash instead of writing zeros.
const OP_COMPENSATE_REASSIGN: u8 = 24;
/// Gap #8: compensation intent for prune. Carries the prior status byte
/// of the slot so rollback restores `UTXO_SPENT` / `UTXO_FROZEN` /
/// `UTXO_UNSPENT` exactly instead of unconditionally restoring UNSPENT.
const OP_COMPENSATE_PRUNE: u8 = 25;
/// Gap #8: compensation intent for set-locked. Carries the prior locked
/// flag and `delete_at_height` so rollback restores pruning state exactly.
const OP_COMPENSATE_SET_LOCKED: u8 = 26;
/// Conditional parent-prune entry used when deleting a child transaction.
/// Replays only if the parent slot is still spent by the deleted child txid.
const OP_PRUNE_SLOT_IF_SPENT_BY: u8 = 27;
/// Durable intent to append a child txid to a parent's conflicting-child list.
const OP_APPEND_CONFLICTING_CHILD: u8 = 28;
/// Recovery progress marker written after replay has safely processed all
/// entries through `through_sequence`.
const OP_RECOVERY_PROGRESS: u8 = 29;
/// Spend redo entry with derived metadata context.
const OP_SPEND_V2: u8 = 30;
/// Unspend redo entry with derived metadata context.
const OP_UNSPEND_V2: u8 = 31;
/// F-G4-008: explicit V2 tag for freeze entries that carry `utxo_hash`.
/// Disambiguates from legacy [`OP_FREEZE`] entries by type byte rather
/// than data length, so a future entry shape with 68+ bytes cannot be
/// silently routed to the V2 decoder.
const OP_FREEZE_V2: u8 = 32;
/// F-G4-008: explicit V2 tag for unfreeze entries that carry `utxo_hash`.
const OP_UNFREEZE_V2: u8 = 33;
/// F-X-022: durable intent to append a child txid to a parent's
/// deleted-children list. Mirrors [`OP_APPEND_CONFLICTING_CHILD`] —
/// the engine writes and fsyncs this before allocating/writing the
/// replacement deleted-child-list block. Full startup recovery
/// collects these entries and drains them after constructing the
/// engine.
const OP_APPEND_DELETED_CHILD: u8 = 34;
/// B-5: Spend redo entry that additionally carries the slot's
/// `utxo_hash`. A `SpendV2`/`UnspendV2` (opcodes 30/31) carries only the
/// spending data, so a CRC-failing spent slot in the WAL window cannot be
/// rebuilt the way `CreateV2` rebuilds a whole record — recovery
/// fail-closed-bricks the node. The V3 entries embed the 32-byte
/// `utxo_hash` so replay can reconstruct a torn slot from the durable
/// redo intent. Legacy V2 entries (no hash) remain decodable with
/// `utxo_hash = None`.
const OP_SPEND_V3: u8 = 35;
/// B-5: Unspend redo entry carrying the slot's `utxo_hash`. See
/// `OP_SPEND_V3`.
const OP_UNSPEND_V3: u8 = 36;
/// Remove a child txid from a parent's conflicting-children list. The exact
/// inverse of [`OP_APPEND_CONFLICTING_CHILD`]; same deferred-drain recovery
/// model (the operation needs the engine allocator + stripe locks, so low-level
/// replay collects it and the engine drains it after construction). Idempotent.
const OP_REMOVE_CONFLICTING_CHILD: u8 = 37;
/// F-A1/reassign: reassign redo entry that additionally carries the slot's
/// `prior_utxo_hash` (the hash the live `engine.reassign` validated against
/// before mutating). Legacy [`OP_REASSIGN`] entries carry only the new hash,
/// so a reassign the engine *rejected* (wrong prior hash) could become a
/// durable mutation on crash-replay — recovery had no way to re-validate
/// identity. V2 entries let `replay_metadata_op` skip a reassign whose prior
/// hash no longer matches the on-disk slot, exactly as the live path rejects
/// it. Legacy V1 entries remain decodable.
const OP_REASSIGN_V2: u8 = 38;

/// F-G4-006: hard cap on the number of parent_txids decoded from a single
/// `CreateV2` redo entry. Bitcoin transactions in practice rarely have
/// more than a handful of conflicting parents; a wire-controlled
/// `u16::MAX` would let a corrupt-but-CRC-valid entry pin ~2 MiB at
/// startup per offending entry, multiplied by however many such entries
/// the redo region contains.
const MAX_CREATE_V2_PARENTS: usize = 64;
/// F-G4-006: hard cap on the `record_bytes` slab decoded from a single
/// `CreateV2` redo entry. The TeraSlab record size is bounded by
/// `TxMetadata::record_size_for(utxo_count)` plus cold data, which in
/// the worst case the engine emits is well under 1 MiB.
const MAX_CREATE_V2_RECORD_BYTES: usize = 1024 * 1024;

/// A redo log operation that can be serialized and replayed.
#[derive(Debug, Clone, PartialEq)]
pub enum RedoOp {
    Spend {
        tx_key: TxKey,
        offset: u32,
        spending_data: [u8; 36],
        new_spent_count: u32,
    },
    SpendV2 {
        tx_key: TxKey,
        offset: u32,
        spending_data: [u8; 36],
        new_spent_count: u32,
        current_block_height: u32,
        block_height_retention: u32,
        target_generation: u32,
        updated_at: u64,
        /// B-5: the spent slot's `utxo_hash`. `Some` for entries written
        /// in the V3 format (carries the hash, serialized under
        /// `OP_SPEND_V3`); `None` for legacy V2 entries that predate
        /// the hash. When present, recovery can rebuild a CRC-failing
        /// slot from this intent instead of fail-closed-bricking.
        utxo_hash: Option<[u8; 32]>,
    },
    Unspend {
        tx_key: TxKey,
        offset: u32,
        /// Expected spending data before clearing. `None` is used only for
        /// legacy redo entries written before unspend carried this field.
        spending_data: Option<[u8; 36]>,
        new_spent_count: u32,
    },
    UnspendV2 {
        tx_key: TxKey,
        offset: u32,
        spending_data: [u8; 36],
        new_spent_count: u32,
        current_block_height: u32,
        block_height_retention: u32,
        target_generation: u32,
        updated_at: u64,
        /// B-5: the slot's `utxo_hash`. `Some` for V3 entries
        /// (serialized under `OP_UNSPEND_V3`), `None` for legacy V2.
        /// Lets recovery rebuild a CRC-failing slot to UNSPENT with the
        /// correct hash instead of fail-closed-bricking.
        utxo_hash: Option<[u8; 32]>,
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
    /// Freeze entry written by dispatch paths that validated a specific
    /// UTXO hash. Legacy [`RedoOp::Freeze`] entries remain decodable.
    FreezeV2 {
        tx_key: TxKey,
        offset: u32,
        utxo_hash: [u8; 32],
    },
    Unfreeze {
        tx_key: TxKey,
        offset: u32,
    },
    /// Unfreeze entry written by dispatch paths that validated a specific
    /// UTXO hash. Legacy [`RedoOp::Unfreeze`] entries remain decodable.
    UnfreezeV2 {
        tx_key: TxKey,
        offset: u32,
        utxo_hash: [u8; 32],
    },
    Reassign {
        tx_key: TxKey,
        offset: u32,
        new_hash: [u8; 32],
        block_height: u32,
        spendable_after: u32,
    },
    /// Reassign entry written by the dispatch path that additionally carries
    /// the `prior_utxo_hash` the request validated against. Recovery uses it
    /// to skip a reassign whose prior hash no longer matches the on-disk slot
    /// (i.e. an operation the live engine would have rejected), preventing a
    /// rejected reassign from becoming a durable mutation after crash-replay.
    /// Legacy [`RedoOp::Reassign`] entries remain decodable.
    ReassignV2 {
        tx_key: TxKey,
        offset: u32,
        new_hash: [u8; 32],
        block_height: u32,
        spendable_after: u32,
        prior_utxo_hash: [u8; 32],
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
    /// Legacy create entry: only enough payload to register the index
    /// entry on replay. Kept so logs written before gap #2 can still be
    /// replayed (back-compat). New writes use [`RedoOp::CreateV2`] which
    /// carries enough state to rebuild the full on-device record.
    Create {
        tx_key: TxKey,
        record_offset: u64,
        utxo_count: u32,
    },
    /// Gap #2 full-payload create entry.
    ///
    /// Captures every authoritative byte the engine writes at
    /// `record_offset` plus the conflicting-child link inputs the
    /// post-write step needs. On replay the recovery path reconstructs
    /// the on-device record bit-for-bit identical to a successful
    /// create — there is no separate "register index, hope the device
    /// bytes are intact" window.
    ///
    /// Wire layout (after the type byte):
    /// ```text
    /// [tx_key:32]
    /// [record_offset:8 LE]
    /// [utxo_count:4 LE]
    /// [is_conflicting:1]
    /// [record_len:4 LE]
    /// [record_bytes:record_len]
    /// [parent_txids_count:2 LE]
    /// [parent_txids:32 * parent_txids_count]
    /// ```
    /// `record_bytes` is the same buffer `Engine::write_full_record_with_cold`
    /// pwrites at `record_offset` (metadata header + UTXO slots + cold
    /// data, no device-alignment padding). `parent_txids` is empty
    /// when `is_conflicting` is false.
    CreateV2 {
        /// Primary key of the new transaction.
        tx_key: TxKey,
        /// Device byte offset where the record starts.
        record_offset: u64,
        /// Number of UTXO slots written immediately after the metadata.
        utxo_count: u32,
        /// Whether this create marks the tx as CONFLICTING — controls
        /// whether replay walks `parent_txids` and re-establishes the
        /// `append_conflicting_child` links.
        is_conflicting: bool,
        /// The exact bytes the engine wrote at `record_offset` (metadata
        /// header + UTXO slots + cold data). Recovery `pwrite`s these
        /// directly so the post-replay record is byte-identical.
        record_bytes: Vec<u8>,
        /// Parent transaction IDs whose conflicting-child lists must
        /// receive `tx_key.txid`. Empty when `is_conflicting` is false.
        parent_txids: Vec<[u8; 32]>,
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
    /// R-221: intent to append `child_txid` to `parent_key`'s
    /// conflicting-children list.
    ///
    /// The engine writes and fsyncs this before allocating/writing the
    /// replacement child-list block. Low-level recovery cannot safely replay
    /// it because the append path needs the engine allocator and stripe locks,
    /// so full startup recovery collects these entries and drains them after
    /// constructing the engine.
    ///
    /// Wire layout (after the type byte):
    /// ```text
    /// [parent_key:32]
    /// [child_txid:32]
    /// ```
    AppendConflictingChild {
        parent_key: TxKey,
        child_txid: [u8; 32],
    },
    /// Durable intent to REMOVE a child txid from a parent's
    /// conflicting-children list — the exact inverse of
    /// [`Self::AppendConflictingChild`]. Same deferred-drain recovery model:
    /// low-level replay collects it and the engine drains it via
    /// `Engine::remove_conflicting_child` (idempotent) after construction.
    ///
    /// Wire layout (after the type byte):
    /// ```text
    /// [parent_key:32]
    /// [child_txid:32]
    /// ```
    RemoveConflictingChild {
        parent_key: TxKey,
        child_txid: [u8; 32],
    },
    /// F-X-022: durable intent to append a child txid to a parent's
    /// deleted-children list. Aerospike `addDeletedChildren` parity.
    ///
    /// Emitted from `Engine::append_deleted_child` after the prune-slot
    /// mutation, so the chain of redo entries is:
    /// `PruneSlotIfSpentBy` (logically primary) →
    /// `AppendDeletedChild` (audit/diagnostic + defense-in-depth at
    /// idempotent-respend). The replay handler defers the append to a
    /// post-engine-construction draining pass for the same reason as
    /// [`Self::AppendConflictingChild`] — the operation needs the engine
    /// allocator and stripe locks.
    ///
    /// Wire layout (after the type byte):
    /// ```text
    /// [parent_key:32]
    /// [child_txid:32]
    /// ```
    AppendDeletedChild {
        parent_key: TxKey,
        child_txid: [u8; 32],
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
        /// Target record generation after this op is applied. Used by the
        /// replay handler as the idempotency token (H7): replay skips when
        /// the on-device `meta.generation` is already at-or-ahead under the
        /// wrapping generation order, and on apply writes
        /// `meta.generation = generation` so subsequent replays of the same
        /// entry are correctly observed as idempotent.
        generation: u32,
    },
    /// Two-phase durability intent record for the unmined secondary index.
    ///
    /// Appended + fsynced BEFORE the redb secondary index transaction is
    /// committed. On crash recovery, the replay path checks the primary
    /// index's current `unmined_since` and only reapplies the secondary
    /// update if it is stale, ensuring idempotency.
    SecondaryUnminedUpdate {
        tx_key: TxKey,
        old_height: u32,
        new_height: u32,
    },
    /// Two-phase durability intent record for the DAH secondary index.
    ///
    /// Appended + fsynced BEFORE the redb secondary index transaction is
    /// committed. On crash recovery, the replay path checks the primary
    /// index's current `delete_at_height` and only reapplies the secondary
    /// update if it is stale, ensuring idempotency.
    SecondaryDahUpdate {
        tx_key: TxKey,
        old_height: u32,
        new_height: u32,
    },
    /// Durability record for a device-space reservation made by the allocator.
    ///
    /// Appended + fsynced BEFORE the offset is returned to any caller that
    /// might issue a data write. On crash recovery, the replay handler
    /// marks the region as allocated in the rebuilt in-memory allocator:
    /// it removes the region from the freelist (if present) and bumps
    /// `next_offset` if the allocation extended the high-water mark.
    /// Replaying the same record twice is a no-op (idempotent).
    AllocateRegion {
        /// Device byte offset of the allocated region.
        offset: u64,
        /// Size of the allocation in bytes (already aligned).
        size: u64,
        /// Logical device identifier — currently always 0 (single device).
        /// Reserved for future multi-device deployments.
        device_id: u8,
    },
    /// Durability record for a device-space release by the allocator.
    ///
    /// Appended + fsynced BEFORE the freelist is mutated. On crash
    /// recovery, the replay handler inserts the region into the rebuilt
    /// in-memory allocator's freelist (with coalescing) so the region is
    /// available for reuse. Replaying the same record twice is a no-op.
    FreeRegion {
        /// Device byte offset of the freed region.
        offset: u64,
        /// Size of the freed region in bytes (already aligned).
        size: u64,
        /// Logical device identifier — matches the paired `AllocateRegion`.
        device_id: u8,
    },
    /// Durability record for the start of a file-backed hash table resize.
    ///
    /// Appended + fsynced BEFORE any tmp file is written. Captures the tmp
    /// file path (as raw bytes, because filesystem paths are not guaranteed
    /// to be UTF-8) and the target capacity. On crash recovery, a
    /// `HashtableResizeBegin` without a matching `HashtableResizeCommit`
    /// indicates a partially-written tmp file that must be removed: the
    /// primary index file is untouched until the rename + commit, so the
    /// server can safely retry the resize on the next load-factor trigger.
    HashtableResizeBegin {
        /// Raw bytes of the tmp file path. Stored as bytes (not `String`)
        /// because POSIX paths can be any sequence of non-NUL bytes and are
        /// not guaranteed to be valid UTF-8.
        tmp_path_bytes: Vec<u8>,
        /// Target capacity in buckets (power of two).
        new_capacity: u64,
    },
    /// Durability record for a successfully completed hash table resize.
    ///
    /// Appended + fsynced AFTER the tmp file has been written, fsynced,
    /// renamed over the original, and the parent directory fsynced. On
    /// crash recovery, pairing a `HashtableResizeCommit` with its matching
    /// `HashtableResizeBegin` (same `new_capacity`) indicates the resize
    /// completed atomically — nothing to roll back.
    HashtableResizeCommit {
        /// Target capacity in buckets — matches the paired `Begin`.
        new_capacity: u64,
    },
    /// Gap #8 (TERANODE_PRODUCTION_READINESS_GAPS.md): compensation intent
    /// for an unset-mined operation that needs to be rolled back after a
    /// replication failure.
    ///
    /// Captures the `block_height` + `subtree_idx` that were paired with
    /// `block_id` BEFORE the engine cleared the block entry. Recovery
    /// replays this entry by re-adding the block entry with these exact
    /// values, so a crash mid-rollback cannot produce a record whose
    /// block entry has zeroed height/subtree fields.
    ///
    /// Wire layout (after the type byte):
    /// ```text
    /// [tx_key:32]
    /// [block_id:4 LE]
    /// [block_height:4 LE]
    /// [subtree_idx:4 LE]
    /// ```
    CompensateUnsetMined {
        /// Primary key of the affected transaction.
        tx_key: TxKey,
        /// Block id whose entry is being restored.
        block_id: u32,
        /// Original block height captured pre-apply.
        block_height: u32,
        /// Original subtree index captured pre-apply.
        subtree_idx: u32,
    },
    /// Gap #8 (TERANODE_PRODUCTION_READINESS_GAPS.md): compensation intent
    /// for a reassign operation that needs to be rolled back after a
    /// replication failure.
    ///
    /// Captures the slot's prior `utxo_hash` BEFORE the reassign overwrote
    /// it. Recovery replays this by restoring the slot to UNSPENT with the
    /// original hash instead of zeros.
    ///
    /// Wire layout (after the type byte):
    /// ```text
    /// [tx_key:32]
    /// [offset:4 LE]
    /// [prior_utxo_hash:32]
    /// ```
    CompensateReassign {
        /// Primary key of the affected transaction.
        tx_key: TxKey,
        /// Slot offset that was reassigned.
        offset: u32,
        /// The slot's `utxo_hash` before the reassign was applied.
        prior_utxo_hash: [u8; 32],
    },
    /// Gap #8 (TERANODE_PRODUCTION_READINESS_GAPS.md): compensation intent
    /// for a prune-slot operation that needs to be rolled back after a
    /// replication failure.
    ///
    /// Captures the slot's prior status byte (UNSPENT / SPENT / FROZEN /
    /// etc.) BEFORE the prune set it to PRUNED. Recovery replays this by
    /// writing back the original status byte exactly.
    ///
    /// Wire layout (after the type byte):
    /// ```text
    /// [tx_key:32]
    /// [offset:4 LE]
    /// [prior_status:1]
    /// ```
    CompensatePrune {
        /// Primary key of the affected transaction.
        tx_key: TxKey,
        /// Slot offset that was pruned.
        offset: u32,
        /// The slot's `status` byte before the prune was applied.
        prior_status: u8,
    },
    /// Gap #8 compensation intent for set-locked rollback.
    ///
    /// Captures the pre-apply locked flag and `delete_at_height` so recovery
    /// can restore pruning state exactly after a crash mid-compensation.
    ///
    /// Wire layout (after the type byte):
    /// ```text
    /// [tx_key:32]
    /// [prior_locked:1]
    /// [prior_delete_at_height:4 LE]
    /// ```
    CompensateSetLocked {
        /// Primary key of the affected transaction.
        tx_key: TxKey,
        /// Whether the record was locked before SetLocked was applied.
        prior_locked: bool,
        /// The record's `delete_at_height` before SetLocked was applied.
        prior_delete_at_height: u32,
    },
    /// Startup replay progress marker.
    ///
    /// Unlike [`RedoOp::Checkpoint`], this does not prove the whole engine
    /// snapshot is durable and does not reclaim log space. It only lets a
    /// subsequent crash during recovery skip redo entries that were already
    /// replayed safely by an earlier recovery attempt.
    RecoveryProgress {
        through_sequence: u64,
    },
    Checkpoint,
}

impl RedoOp {
    fn op_type(&self) -> u8 {
        match self {
            RedoOp::Spend { .. } => OP_SPEND,
            RedoOp::SpendV2 {
                utxo_hash: Some(_), ..
            } => OP_SPEND_V3,
            RedoOp::SpendV2 { .. } => OP_SPEND_V2,
            RedoOp::Unspend { .. } => OP_UNSPEND,
            RedoOp::UnspendV2 {
                utxo_hash: Some(_), ..
            } => OP_UNSPEND_V3,
            RedoOp::UnspendV2 { .. } => OP_UNSPEND_V2,
            RedoOp::SetMined { .. } => OP_SET_MINED,
            RedoOp::Freeze { .. } => OP_FREEZE,
            RedoOp::FreezeV2 { .. } => OP_FREEZE_V2,
            RedoOp::Unfreeze { .. } => OP_UNFREEZE,
            RedoOp::UnfreezeV2 { .. } => OP_UNFREEZE_V2,
            RedoOp::Reassign { .. } => OP_REASSIGN,
            RedoOp::ReassignV2 { .. } => OP_REASSIGN_V2,
            RedoOp::PruneSlot { .. } => OP_PRUNE_SLOT,
            RedoOp::PruneSlotIfSpentBy { .. } => OP_PRUNE_SLOT_IF_SPENT_BY,
            RedoOp::Create { .. } => OP_CREATE,
            RedoOp::CreateV2 { .. } => OP_CREATE_V2,
            RedoOp::Delete { .. } => OP_DELETE,
            RedoOp::SetConflicting { .. } => OP_SET_CONFLICTING,
            RedoOp::AppendConflictingChild { .. } => OP_APPEND_CONFLICTING_CHILD,
            RedoOp::RemoveConflictingChild { .. } => OP_REMOVE_CONFLICTING_CHILD,
            RedoOp::AppendDeletedChild { .. } => OP_APPEND_DELETED_CHILD,
            RedoOp::SetLocked { .. } => OP_SET_LOCKED,
            RedoOp::PreserveUntil { .. } => OP_PRESERVE_UNTIL,
            RedoOp::MarkOnLongestChain { .. } => OP_MARK_LONGEST_CHAIN,
            RedoOp::SecondaryUnminedUpdate { .. } => OP_SECONDARY_UNMINED_UPDATE,
            RedoOp::SecondaryDahUpdate { .. } => OP_SECONDARY_DAH_UPDATE,
            RedoOp::AllocateRegion { .. } => OP_ALLOCATE_REGION,
            RedoOp::FreeRegion { .. } => OP_FREE_REGION,
            RedoOp::HashtableResizeBegin { .. } => OP_HASHTABLE_RESIZE_BEGIN,
            RedoOp::HashtableResizeCommit { .. } => OP_HASHTABLE_RESIZE_COMMIT,
            RedoOp::CompensateUnsetMined { .. } => OP_COMPENSATE_UNSET_MINED,
            RedoOp::CompensateReassign { .. } => OP_COMPENSATE_REASSIGN,
            RedoOp::CompensatePrune { .. } => OP_COMPENSATE_PRUNE,
            RedoOp::CompensateSetLocked { .. } => OP_COMPENSATE_SET_LOCKED,
            RedoOp::RecoveryProgress { .. } => OP_RECOVERY_PROGRESS,
            RedoOp::Checkpoint => OP_CHECKPOINT,
        }
    }

    /// Extract the tx_key from the operation, if it has one.
    ///
    /// Returns `None` for `Checkpoint` which has no associated key.
    pub fn tx_key(&self) -> Option<&TxKey> {
        match self {
            RedoOp::Spend { tx_key, .. }
            | RedoOp::SpendV2 { tx_key, .. }
            | RedoOp::Unspend { tx_key, .. }
            | RedoOp::UnspendV2 { tx_key, .. }
            | RedoOp::SetMined { tx_key, .. }
            | RedoOp::Freeze { tx_key, .. }
            | RedoOp::FreezeV2 { tx_key, .. }
            | RedoOp::Unfreeze { tx_key, .. }
            | RedoOp::UnfreezeV2 { tx_key, .. }
            | RedoOp::Reassign { tx_key, .. }
            | RedoOp::ReassignV2 { tx_key, .. }
            | RedoOp::PruneSlot { tx_key, .. }
            | RedoOp::PruneSlotIfSpentBy { tx_key, .. }
            | RedoOp::Create { tx_key, .. }
            | RedoOp::CreateV2 { tx_key, .. }
            | RedoOp::Delete { tx_key, .. }
            | RedoOp::SetConflicting { tx_key, .. }
            | RedoOp::SetLocked { tx_key, .. }
            | RedoOp::PreserveUntil { tx_key, .. }
            | RedoOp::MarkOnLongestChain { tx_key, .. }
            | RedoOp::SecondaryUnminedUpdate { tx_key, .. }
            | RedoOp::SecondaryDahUpdate { tx_key, .. }
            | RedoOp::CompensateUnsetMined { tx_key, .. }
            | RedoOp::CompensateReassign { tx_key, .. }
            | RedoOp::CompensatePrune { tx_key, .. }
            | RedoOp::CompensateSetLocked { tx_key, .. } => Some(tx_key),
            RedoOp::AppendConflictingChild { parent_key, .. }
            | RedoOp::RemoveConflictingChild { parent_key, .. }
            | RedoOp::AppendDeletedChild { parent_key, .. } => Some(parent_key),
            RedoOp::AllocateRegion { .. }
            | RedoOp::FreeRegion { .. }
            | RedoOp::HashtableResizeBegin { .. }
            | RedoOp::HashtableResizeCommit { .. }
            | RedoOp::RecoveryProgress { .. }
            | RedoOp::Checkpoint => None,
        }
    }

    /// The block height this op observed when it was applied, if any (height
    /// subsystem, deletion-tombstone design §4).
    ///
    /// Returns `Some(h)` for the height-bearing ops that carry the chain height
    /// (or, for [`RedoOp::SetMined`], the mined block height) at the time the
    /// engine applied them: spend / unspend (V2 — V1 predates the field),
    /// set-mined, reassign, set-conflicting, preserve-until,
    /// mark-on-longest-chain, and the unset-mined compensation. Returns `None`
    /// for ops that carry no height (freeze, prune, raw create/delete, region
    /// allocation, control entries).
    ///
    /// Folded by recovery into [`crate::recovery::RecoveryStats`] so a node
    /// whose durable `.height` file was lost or corrupted still floors its
    /// `last_durable_height` at the max height its own replayed records prove
    /// it has seen — independent of whether deletion tombstones are enabled.
    /// This keeps the GC horizon / rejoin gate from regressing to 0 (design
    /// §4 height subsystem; BUG3).
    pub fn observed_block_height(&self) -> Option<u32> {
        match self {
            RedoOp::SpendV2 {
                current_block_height,
                ..
            }
            | RedoOp::UnspendV2 {
                current_block_height,
                ..
            }
            | RedoOp::SetConflicting {
                current_block_height,
                ..
            }
            | RedoOp::MarkOnLongestChain {
                current_block_height,
                ..
            } => Some(*current_block_height),
            RedoOp::SetMined { block_height, .. }
            | RedoOp::Reassign { block_height, .. }
            | RedoOp::ReassignV2 { block_height, .. }
            | RedoOp::PreserveUntil { block_height, .. }
            | RedoOp::CompensateUnsetMined { block_height, .. } => Some(*block_height),
            // No height carried (or V1 ops predating the height field) — these
            // contribute nothing to the floor.
            RedoOp::Spend { .. }
            | RedoOp::Unspend { .. }
            | RedoOp::Freeze { .. }
            | RedoOp::FreezeV2 { .. }
            | RedoOp::Unfreeze { .. }
            | RedoOp::UnfreezeV2 { .. }
            | RedoOp::PruneSlot { .. }
            | RedoOp::PruneSlotIfSpentBy { .. }
            | RedoOp::Create { .. }
            | RedoOp::CreateV2 { .. }
            | RedoOp::Delete { .. }
            | RedoOp::AppendConflictingChild { .. }
            | RedoOp::RemoveConflictingChild { .. }
            | RedoOp::AppendDeletedChild { .. }
            | RedoOp::SetLocked { .. }
            | RedoOp::SecondaryUnminedUpdate { .. }
            | RedoOp::SecondaryDahUpdate { .. }
            | RedoOp::CompensateReassign { .. }
            | RedoOp::CompensatePrune { .. }
            | RedoOp::CompensateSetLocked { .. }
            | RedoOp::AllocateRegion { .. }
            | RedoOp::FreeRegion { .. }
            | RedoOp::HashtableResizeBegin { .. }
            | RedoOp::HashtableResizeCommit { .. }
            | RedoOp::RecoveryProgress { .. }
            | RedoOp::Checkpoint => None,
        }
    }

    /// Serialize op-specific data (without type byte, sequence, or length).
    fn serialize_data(&self, buf: &mut Vec<u8>) {
        match self {
            RedoOp::Spend {
                tx_key,
                offset,
                spending_data,
                new_spent_count,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(spending_data);
                buf.extend_from_slice(&new_spent_count.to_le_bytes());
            }
            RedoOp::SpendV2 {
                tx_key,
                offset,
                spending_data,
                new_spent_count,
                current_block_height,
                block_height_retention,
                target_generation,
                updated_at,
                utxo_hash,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(spending_data);
                buf.extend_from_slice(&new_spent_count.to_le_bytes());
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
                buf.extend_from_slice(&target_generation.to_le_bytes());
                buf.extend_from_slice(&updated_at.to_le_bytes());
                // B-5: V3 entries append the slot hash (32 bytes). The
                // type byte (OP_SPEND_V3) selects this branch on decode.
                if let Some(hash) = utxo_hash {
                    buf.extend_from_slice(hash);
                }
            }
            RedoOp::Unspend {
                tx_key,
                offset,
                spending_data,
                new_spent_count,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                if let Some(spending_data) = spending_data {
                    buf.extend_from_slice(spending_data);
                    buf.extend_from_slice(&new_spent_count.to_le_bytes());
                } else {
                    buf.extend_from_slice(&new_spent_count.to_le_bytes());
                }
            }
            RedoOp::UnspendV2 {
                tx_key,
                offset,
                spending_data,
                new_spent_count,
                current_block_height,
                block_height_retention,
                target_generation,
                updated_at,
                utxo_hash,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(spending_data);
                buf.extend_from_slice(&new_spent_count.to_le_bytes());
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
                buf.extend_from_slice(&target_generation.to_le_bytes());
                buf.extend_from_slice(&updated_at.to_le_bytes());
                // B-5: V3 entries append the slot hash (32 bytes).
                if let Some(hash) = utxo_hash {
                    buf.extend_from_slice(hash);
                }
            }
            RedoOp::SetMined {
                tx_key,
                block_id,
                block_height,
                subtree_idx,
                unset,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&block_id.to_le_bytes());
                buf.extend_from_slice(&block_height.to_le_bytes());
                buf.extend_from_slice(&subtree_idx.to_le_bytes());
                buf.push(if *unset { 1 } else { 0 });
            }
            RedoOp::Freeze { tx_key, offset }
            | RedoOp::Unfreeze { tx_key, offset }
            | RedoOp::PruneSlot { tx_key, offset } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
            }
            RedoOp::PruneSlotIfSpentBy {
                tx_key,
                offset,
                child_txid,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(child_txid);
            }
            RedoOp::FreezeV2 {
                tx_key,
                offset,
                utxo_hash,
            }
            | RedoOp::UnfreezeV2 {
                tx_key,
                offset,
                utxo_hash,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(utxo_hash);
            }
            RedoOp::Reassign {
                tx_key,
                offset,
                new_hash,
                block_height,
                spendable_after,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(new_hash);
                buf.extend_from_slice(&block_height.to_le_bytes());
                buf.extend_from_slice(&spendable_after.to_le_bytes());
            }
            RedoOp::ReassignV2 {
                tx_key,
                offset,
                new_hash,
                block_height,
                spendable_after,
                prior_utxo_hash,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(new_hash);
                buf.extend_from_slice(&block_height.to_le_bytes());
                buf.extend_from_slice(&spendable_after.to_le_bytes());
                buf.extend_from_slice(prior_utxo_hash);
            }
            RedoOp::Create {
                tx_key,
                record_offset,
                utxo_count,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&record_offset.to_le_bytes());
                buf.extend_from_slice(&utxo_count.to_le_bytes());
            }
            RedoOp::CreateV2 {
                tx_key,
                record_offset,
                utxo_count,
                is_conflicting,
                record_bytes,
                parent_txids,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&record_offset.to_le_bytes());
                buf.extend_from_slice(&utxo_count.to_le_bytes());
                buf.push(if *is_conflicting { 1 } else { 0 });
                let record_len = record_bytes.len() as u32;
                buf.extend_from_slice(&record_len.to_le_bytes());
                buf.extend_from_slice(record_bytes);
                let parents = parent_txids.len() as u16;
                buf.extend_from_slice(&parents.to_le_bytes());
                for ptx in parent_txids {
                    buf.extend_from_slice(ptx);
                }
            }
            RedoOp::Delete {
                tx_key,
                record_offset,
                record_size,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&record_offset.to_le_bytes());
                buf.extend_from_slice(&record_size.to_le_bytes());
            }
            RedoOp::SetConflicting {
                tx_key,
                value,
                current_block_height,
                block_height_retention,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.push(if *value { 1 } else { 0 });
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
            }
            RedoOp::AppendConflictingChild {
                parent_key,
                child_txid,
            }
            | RedoOp::RemoveConflictingChild {
                parent_key,
                child_txid,
            }
            | RedoOp::AppendDeletedChild {
                parent_key,
                child_txid,
            } => {
                buf.extend_from_slice(&parent_key.txid);
                buf.extend_from_slice(child_txid);
            }
            RedoOp::SetLocked { tx_key, value } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.push(if *value { 1 } else { 0 });
            }
            RedoOp::PreserveUntil {
                tx_key,
                block_height,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&block_height.to_le_bytes());
            }
            RedoOp::MarkOnLongestChain {
                tx_key,
                on_longest_chain,
                current_block_height,
                block_height_retention,
                generation,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.push(if *on_longest_chain { 1 } else { 0 });
                buf.extend_from_slice(&current_block_height.to_le_bytes());
                buf.extend_from_slice(&block_height_retention.to_le_bytes());
                buf.extend_from_slice(&generation.to_le_bytes());
            }
            RedoOp::SecondaryUnminedUpdate {
                tx_key,
                old_height,
                new_height,
            }
            | RedoOp::SecondaryDahUpdate {
                tx_key,
                old_height,
                new_height,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&old_height.to_le_bytes());
                buf.extend_from_slice(&new_height.to_le_bytes());
            }
            RedoOp::AllocateRegion {
                offset,
                size,
                device_id,
            }
            | RedoOp::FreeRegion {
                offset,
                size,
                device_id,
            } => {
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(&size.to_le_bytes());
                buf.push(*device_id);
            }
            RedoOp::HashtableResizeBegin {
                tmp_path_bytes,
                new_capacity,
            } => {
                // [new_capacity:8][path_len:4][path_bytes:N]
                buf.extend_from_slice(&new_capacity.to_le_bytes());
                let path_len = tmp_path_bytes.len() as u32;
                buf.extend_from_slice(&path_len.to_le_bytes());
                buf.extend_from_slice(tmp_path_bytes);
            }
            RedoOp::HashtableResizeCommit { new_capacity } => {
                buf.extend_from_slice(&new_capacity.to_le_bytes());
            }
            RedoOp::CompensateUnsetMined {
                tx_key,
                block_id,
                block_height,
                subtree_idx,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&block_id.to_le_bytes());
                buf.extend_from_slice(&block_height.to_le_bytes());
                buf.extend_from_slice(&subtree_idx.to_le_bytes());
            }
            RedoOp::CompensateReassign {
                tx_key,
                offset,
                prior_utxo_hash,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(prior_utxo_hash);
            }
            RedoOp::CompensatePrune {
                tx_key,
                offset,
                prior_status,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.push(*prior_status);
            }
            RedoOp::CompensateSetLocked {
                tx_key,
                prior_locked,
                prior_delete_at_height,
            } => {
                buf.extend_from_slice(&tx_key.txid);
                buf.push(if *prior_locked { 1 } else { 0 });
                buf.extend_from_slice(&prior_delete_at_height.to_le_bytes());
            }
            RedoOp::RecoveryProgress { through_sequence } => {
                buf.extend_from_slice(&through_sequence.to_le_bytes());
            }
            RedoOp::Checkpoint => {}
        }
    }

    /// Deserialize op from type byte + data bytes.
    fn deserialize(op_type: u8, data: &[u8]) -> Option<Self> {
        match op_type {
            OP_SPEND if data.len() >= 76 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut sd = [0u8; 36];
                sd.copy_from_slice(&data[36..72]);
                let cnt = u32::from_le_bytes(data[72..76].try_into().unwrap());
                Some(RedoOp::Spend {
                    tx_key: TxKey { txid },
                    offset,
                    spending_data: sd,
                    new_spent_count: cnt,
                })
            }
            OP_SPEND_V3 if data.len() >= 128 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut sd = [0u8; 36];
                sd.copy_from_slice(&data[36..72]);
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&data[96..128]);
                Some(RedoOp::SpendV2 {
                    tx_key: TxKey { txid },
                    offset,
                    spending_data: sd,
                    new_spent_count: u32::from_le_bytes(data[72..76].try_into().unwrap()),
                    current_block_height: u32::from_le_bytes(data[76..80].try_into().unwrap()),
                    block_height_retention: u32::from_le_bytes(data[80..84].try_into().unwrap()),
                    target_generation: u32::from_le_bytes(data[84..88].try_into().unwrap()),
                    updated_at: u64::from_le_bytes(data[88..96].try_into().unwrap()),
                    utxo_hash: Some(hash),
                })
            }
            OP_SPEND_V2 if data.len() >= 96 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut sd = [0u8; 36];
                sd.copy_from_slice(&data[36..72]);
                Some(RedoOp::SpendV2 {
                    tx_key: TxKey { txid },
                    offset,
                    spending_data: sd,
                    new_spent_count: u32::from_le_bytes(data[72..76].try_into().unwrap()),
                    current_block_height: u32::from_le_bytes(data[76..80].try_into().unwrap()),
                    block_height_retention: u32::from_le_bytes(data[80..84].try_into().unwrap()),
                    target_generation: u32::from_le_bytes(data[84..88].try_into().unwrap()),
                    updated_at: u64::from_le_bytes(data[88..96].try_into().unwrap()),
                    utxo_hash: None,
                })
            }
            OP_UNSPEND if data.len() >= 76 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut sd = [0u8; 36];
                sd.copy_from_slice(&data[36..72]);
                let cnt = u32::from_le_bytes(data[72..76].try_into().unwrap());
                Some(RedoOp::Unspend {
                    tx_key: TxKey { txid },
                    offset,
                    spending_data: Some(sd),
                    new_spent_count: cnt,
                })
            }
            OP_UNSPEND_V3 if data.len() >= 128 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut sd = [0u8; 36];
                sd.copy_from_slice(&data[36..72]);
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&data[96..128]);
                Some(RedoOp::UnspendV2 {
                    tx_key: TxKey { txid },
                    offset,
                    spending_data: sd,
                    new_spent_count: u32::from_le_bytes(data[72..76].try_into().unwrap()),
                    current_block_height: u32::from_le_bytes(data[76..80].try_into().unwrap()),
                    block_height_retention: u32::from_le_bytes(data[80..84].try_into().unwrap()),
                    target_generation: u32::from_le_bytes(data[84..88].try_into().unwrap()),
                    updated_at: u64::from_le_bytes(data[88..96].try_into().unwrap()),
                    utxo_hash: Some(hash),
                })
            }
            OP_UNSPEND_V2 if data.len() >= 96 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut sd = [0u8; 36];
                sd.copy_from_slice(&data[36..72]);
                Some(RedoOp::UnspendV2 {
                    tx_key: TxKey { txid },
                    offset,
                    spending_data: sd,
                    new_spent_count: u32::from_le_bytes(data[72..76].try_into().unwrap()),
                    current_block_height: u32::from_le_bytes(data[76..80].try_into().unwrap()),
                    block_height_retention: u32::from_le_bytes(data[80..84].try_into().unwrap()),
                    target_generation: u32::from_le_bytes(data[84..88].try_into().unwrap()),
                    updated_at: u64::from_le_bytes(data[88..96].try_into().unwrap()),
                    utxo_hash: None,
                })
            }
            OP_UNSPEND if data.len() >= 40 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let cnt = u32::from_le_bytes(data[36..40].try_into().unwrap());
                Some(RedoOp::Unspend {
                    tx_key: TxKey { txid },
                    offset,
                    spending_data: None,
                    new_spent_count: cnt,
                })
            }
            OP_SET_MINED if data.len() >= 45 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::SetMined {
                    tx_key: TxKey { txid },
                    block_id: u32::from_le_bytes(data[32..36].try_into().unwrap()),
                    block_height: u32::from_le_bytes(data[36..40].try_into().unwrap()),
                    subtree_idx: u32::from_le_bytes(data[40..44].try_into().unwrap()),
                    unset: data[44] != 0,
                })
            }
            // F-G4-008: distinct opcodes for V2 freeze/unfreeze. The old
            // overlap with [`OP_FREEZE`] / [`OP_UNFREEZE`] (disambiguating
            // by `data.len() >= 68`) was fragile against future entry
            // shapes; routing by op_type byte is unambiguous.
            OP_FREEZE_V2 if data.len() >= 68 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut utxo_hash = [0u8; 32];
                utxo_hash.copy_from_slice(&data[36..68]);
                Some(RedoOp::FreezeV2 {
                    tx_key: TxKey { txid },
                    offset,
                    utxo_hash,
                })
            }
            OP_UNFREEZE_V2 if data.len() >= 68 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut utxo_hash = [0u8; 32];
                utxo_hash.copy_from_slice(&data[36..68]);
                Some(RedoOp::UnfreezeV2 {
                    tx_key: TxKey { txid },
                    offset,
                    utxo_hash,
                })
            }
            OP_FREEZE | OP_UNFREEZE | OP_PRUNE_SLOT if data.len() >= 36 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let key = TxKey { txid };
                match op_type {
                    OP_FREEZE => Some(RedoOp::Freeze {
                        tx_key: key,
                        offset,
                    }),
                    OP_UNFREEZE => Some(RedoOp::Unfreeze {
                        tx_key: key,
                        offset,
                    }),
                    OP_PRUNE_SLOT => Some(RedoOp::PruneSlot {
                        tx_key: key,
                        offset,
                    }),
                    _ => None,
                }
            }
            OP_PRUNE_SLOT_IF_SPENT_BY if data.len() >= 68 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut child_txid = [0u8; 32];
                child_txid.copy_from_slice(&data[36..68]);
                Some(RedoOp::PruneSlotIfSpentBy {
                    tx_key: TxKey { txid },
                    offset,
                    child_txid,
                })
            }
            OP_REASSIGN if data.len() >= 76 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut nh = [0u8; 32];
                nh.copy_from_slice(&data[36..68]);
                Some(RedoOp::Reassign {
                    tx_key: TxKey { txid },
                    offset,
                    new_hash: nh,
                    block_height: u32::from_le_bytes(data[68..72].try_into().unwrap()),
                    spendable_after: u32::from_le_bytes(data[72..76].try_into().unwrap()),
                })
            }
            OP_REASSIGN_V2 if data.len() >= 108 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut nh = [0u8; 32];
                nh.copy_from_slice(&data[36..68]);
                let mut prior = [0u8; 32];
                prior.copy_from_slice(&data[76..108]);
                Some(RedoOp::ReassignV2 {
                    tx_key: TxKey { txid },
                    offset,
                    new_hash: nh,
                    block_height: u32::from_le_bytes(data[68..72].try_into().unwrap()),
                    spendable_after: u32::from_le_bytes(data[72..76].try_into().unwrap()),
                    prior_utxo_hash: prior,
                })
            }
            OP_CREATE if data.len() >= 44 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::Create {
                    tx_key: TxKey { txid },
                    record_offset: u64::from_le_bytes(data[32..40].try_into().unwrap()),
                    utxo_count: u32::from_le_bytes(data[40..44].try_into().unwrap()),
                })
            }
            OP_CREATE_V2 if data.len() >= 51 => {
                // Layout: tx_key(32) + record_offset(8) + utxo_count(4)
                //       + is_conflicting(1) + record_len(4) + record_bytes(N)
                //       + parents_count(2) + parents(32*M)
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let record_offset = u64::from_le_bytes(data[32..40].try_into().unwrap());
                let utxo_count = u32::from_le_bytes(data[40..44].try_into().unwrap());
                let is_conflicting = data[44] != 0;
                let record_len = u32::from_le_bytes(data[45..49].try_into().unwrap()) as usize;
                // F-G4-006: cap `record_len` so a corrupt-but-CRC-valid
                // entry cannot inflate startup memory by a fabricated value
                // larger than any legitimate record. Real records are
                // bounded by `TxMetadata::record_size_for(utxo_count)`
                // plus cold data; 1 MiB is a comfortable upper bound that
                // exceeds anything the engine emits.
                if record_len > MAX_CREATE_V2_RECORD_BYTES {
                    return None;
                }
                let record_end = 49usize.checked_add(record_len)?;
                if data.len() < record_end + 2 {
                    return None;
                }
                let record_bytes = data[49..record_end].to_vec();
                let parents_count_raw =
                    u16::from_le_bytes(data[record_end..record_end + 2].try_into().unwrap())
                        as usize;
                let parents_start = record_end + 2;
                let parents_end = parents_start.checked_add(parents_count_raw.checked_mul(32)?)?;
                if data.len() < parents_end {
                    return None;
                }
                // F-G4-006: cap `parents_count` so a corrupt entry
                // cannot pre-allocate ~2 MiB of `[u8; 32]` slots. Real
                // transactions rarely have more than a few conflicting
                // parents; cap at 64 (still well above any observed
                // legitimate value).
                if parents_count_raw > MAX_CREATE_V2_PARENTS {
                    return None;
                }
                // Cap is enforced above; allocation is now bounded.
                let mut parent_txids: Vec<[u8; 32]> = Vec::with_capacity(parents_count_raw);
                for i in 0..parents_count_raw {
                    let off = parents_start + i * 32;
                    let mut ptx = [0u8; 32];
                    ptx.copy_from_slice(&data[off..off + 32]);
                    parent_txids.push(ptx);
                }
                Some(RedoOp::CreateV2 {
                    tx_key: TxKey { txid },
                    record_offset,
                    utxo_count,
                    is_conflicting,
                    record_bytes,
                    parent_txids,
                })
            }
            OP_DELETE if data.len() >= 48 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::Delete {
                    tx_key: TxKey { txid },
                    record_offset: u64::from_le_bytes(data[32..40].try_into().unwrap()),
                    record_size: u64::from_le_bytes(data[40..48].try_into().unwrap()),
                })
            }
            OP_SET_CONFLICTING if data.len() >= 41 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::SetConflicting {
                    tx_key: TxKey { txid },
                    value: data[32] != 0,
                    current_block_height: u32::from_le_bytes(data[33..37].try_into().unwrap()),
                    block_height_retention: u32::from_le_bytes(data[37..41].try_into().unwrap()),
                })
            }
            OP_APPEND_CONFLICTING_CHILD if data.len() >= 64 => {
                let mut parent_txid = [0u8; 32];
                parent_txid.copy_from_slice(&data[..32]);
                let mut child_txid = [0u8; 32];
                child_txid.copy_from_slice(&data[32..64]);
                Some(RedoOp::AppendConflictingChild {
                    parent_key: TxKey { txid: parent_txid },
                    child_txid,
                })
            }
            OP_REMOVE_CONFLICTING_CHILD if data.len() >= 64 => {
                let mut parent_txid = [0u8; 32];
                parent_txid.copy_from_slice(&data[..32]);
                let mut child_txid = [0u8; 32];
                child_txid.copy_from_slice(&data[32..64]);
                Some(RedoOp::RemoveConflictingChild {
                    parent_key: TxKey { txid: parent_txid },
                    child_txid,
                })
            }
            OP_APPEND_DELETED_CHILD if data.len() >= 64 => {
                let mut parent_txid = [0u8; 32];
                parent_txid.copy_from_slice(&data[..32]);
                let mut child_txid = [0u8; 32];
                child_txid.copy_from_slice(&data[32..64]);
                Some(RedoOp::AppendDeletedChild {
                    parent_key: TxKey { txid: parent_txid },
                    child_txid,
                })
            }
            OP_SET_LOCKED if data.len() >= 33 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::SetLocked {
                    tx_key: TxKey { txid },
                    value: data[32] != 0,
                })
            }
            OP_PRESERVE_UNTIL if data.len() >= 36 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::PreserveUntil {
                    tx_key: TxKey { txid },
                    block_height: u32::from_le_bytes(data[32..36].try_into().unwrap()),
                })
            }
            OP_MARK_LONGEST_CHAIN if data.len() >= 45 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::MarkOnLongestChain {
                    tx_key: TxKey { txid },
                    on_longest_chain: data[32] != 0,
                    current_block_height: u32::from_le_bytes(data[33..37].try_into().unwrap()),
                    block_height_retention: u32::from_le_bytes(data[37..41].try_into().unwrap()),
                    generation: u32::from_le_bytes(data[41..45].try_into().unwrap()),
                })
            }
            OP_SECONDARY_UNMINED_UPDATE if data.len() >= 40 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::SecondaryUnminedUpdate {
                    tx_key: TxKey { txid },
                    old_height: u32::from_le_bytes(data[32..36].try_into().unwrap()),
                    new_height: u32::from_le_bytes(data[36..40].try_into().unwrap()),
                })
            }
            OP_SECONDARY_DAH_UPDATE if data.len() >= 40 => {
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::SecondaryDahUpdate {
                    tx_key: TxKey { txid },
                    old_height: u32::from_le_bytes(data[32..36].try_into().unwrap()),
                    new_height: u32::from_le_bytes(data[36..40].try_into().unwrap()),
                })
            }
            OP_ALLOCATE_REGION if data.len() >= 17 => {
                let offset = u64::from_le_bytes(data[0..8].try_into().unwrap());
                let size = u64::from_le_bytes(data[8..16].try_into().unwrap());
                let device_id = data[16];
                Some(RedoOp::AllocateRegion {
                    offset,
                    size,
                    device_id,
                })
            }
            OP_FREE_REGION if data.len() >= 17 => {
                let offset = u64::from_le_bytes(data[0..8].try_into().unwrap());
                let size = u64::from_le_bytes(data[8..16].try_into().unwrap());
                let device_id = data[16];
                Some(RedoOp::FreeRegion {
                    offset,
                    size,
                    device_id,
                })
            }
            OP_HASHTABLE_RESIZE_BEGIN if data.len() >= 12 => {
                let new_capacity = u64::from_le_bytes(data[0..8].try_into().unwrap());
                let path_len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
                if data.len() < 12 + path_len {
                    return None;
                }
                let tmp_path_bytes = data[12..12 + path_len].to_vec();
                Some(RedoOp::HashtableResizeBegin {
                    tmp_path_bytes,
                    new_capacity,
                })
            }
            OP_HASHTABLE_RESIZE_COMMIT if data.len() >= 8 => {
                let new_capacity = u64::from_le_bytes(data[0..8].try_into().unwrap());
                Some(RedoOp::HashtableResizeCommit { new_capacity })
            }
            OP_COMPENSATE_UNSET_MINED if data.len() >= 44 => {
                // [tx_key:32][block_id:4][block_height:4][subtree_idx:4]
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::CompensateUnsetMined {
                    tx_key: TxKey { txid },
                    block_id: u32::from_le_bytes(data[32..36].try_into().unwrap()),
                    block_height: u32::from_le_bytes(data[36..40].try_into().unwrap()),
                    subtree_idx: u32::from_le_bytes(data[40..44].try_into().unwrap()),
                })
            }
            OP_COMPENSATE_REASSIGN if data.len() >= 68 => {
                // [tx_key:32][offset:4][prior_utxo_hash:32]
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let mut prior = [0u8; 32];
                prior.copy_from_slice(&data[36..68]);
                Some(RedoOp::CompensateReassign {
                    tx_key: TxKey { txid },
                    offset,
                    prior_utxo_hash: prior,
                })
            }
            OP_COMPENSATE_PRUNE if data.len() >= 37 => {
                // [tx_key:32][offset:4][prior_status:1]
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                let offset = u32::from_le_bytes(data[32..36].try_into().unwrap());
                let prior_status = data[36];
                Some(RedoOp::CompensatePrune {
                    tx_key: TxKey { txid },
                    offset,
                    prior_status,
                })
            }
            OP_COMPENSATE_SET_LOCKED if data.len() >= 37 => {
                // [tx_key:32][prior_locked:1][prior_delete_at_height:4]
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&data[..32]);
                Some(RedoOp::CompensateSetLocked {
                    tx_key: TxKey { txid },
                    prior_locked: data[32] != 0,
                    prior_delete_at_height: u32::from_le_bytes(data[33..37].try_into().unwrap()),
                })
            }
            OP_RECOVERY_PROGRESS if data.len() >= 8 => Some(RedoOp::RecoveryProgress {
                through_sequence: u64::from_le_bytes(data[..8].try_into().unwrap()),
            }),
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
        let stored_checksum =
            u32::from_le_bytes(payload[content_len..content_len + 4].try_into().unwrap());
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

/// Lock-free view of the redo log's space accounting. Updated under the
/// `RedoLog` lock on every state change; read without locking by observers
/// such as `/admin/top` so the observability read never contends with the
/// write path.
#[derive(Debug)]
pub struct RedoAtomics {
    write_pos: AtomicU64,
    logical_start: AtomicU64,
    entries_region_size: u64, // immutable after open
}

impl RedoAtomics {
    /// Bytes written into the entries region (mirrors `RedoLog::write_position`).
    pub fn write_position(&self) -> u64 {
        self.write_pos.load(AtomicOrdering::Relaxed)
    }

    /// Bytes available for new entries (mirrors `RedoLog::available_space`).
    pub fn available_space(&self) -> u64 {
        let used = self.write_pos.load(AtomicOrdering::Relaxed)
            - self.logical_start.load(AtomicOrdering::Relaxed);
        self.entries_region_size.saturating_sub(used)
    }
}

/// Linear-with-reset redo log on a block device.
///
/// Entries are appended to an in-memory buffer and flushed to device
/// on demand. `write_pos` advances monotonically; a successful
/// [`RedoLog::mark_checkpoint`] + [`RedoLog::reset`] pair (driven by
/// [`crate::checkpoint`]) returns `write_pos` to zero so future
/// appends start at the beginning of the log region. There is no
/// in-place wrap — see the module-level documentation for the full
/// rationale (R-027 / BC-13).
pub struct RedoLog {
    device: Arc<dyn BlockDevice>,
    /// Device byte offset of the redo region's first byte (the header).
    log_offset: u64,
    /// Total bytes of the redo region, header + entries.
    log_size: u64,
    /// F-G4-001: bytes reserved at the start of the redo region for the
    /// fixed-size header block; equal to the device's alignment, captured
    /// at `open` so it is stable for the life of this `RedoLog`.
    header_block_size: u64,
    /// Bytes written into the entries region (relative to the entries
    /// region start, not the device). Advances monotonically until
    /// `reset` / `compact_prefix_through` rewinds it.
    write_pos: u64,
    /// B-3: byte offset (relative to the entries region) of the first
    /// live entry. The scan begins here; bytes before it are stale
    /// post-compaction garbage that is logically reclaimed. Compaction
    /// that retains entries advances this rather than physically moving
    /// the retained bytes, so a torn compaction write cannot lose a
    /// durable retained entry. Reset to 0 by [`Self::reset`].
    logical_start: u64,
    checkpoint_seq: u64,
    next_sequence: u64,
    buffer: Vec<u8>,
    /// Durable entries discovered at open plus entries from successful
    /// flushes. Recovery and replica catch-up use this cache instead of
    /// rescanning the full redo region on every call.
    entries_cache: Vec<RedoEntry>,
    /// Entries appended to `buffer` but not yet fsynced. Moved into
    /// `entries_cache` only after `flush()` succeeds.
    pending_entries: Vec<RedoEntry>,
    /// Entry count for metrics: number of `append()` calls currently sitting
    /// in `buffer`. Reset to 0 after a successful `flush()`. Zero-cost when
    /// metrics are not initialized — the counter is still updated but never
    /// read by the hot path.
    buffered_entries: u64,
    /// F-G4-002: once a `flush()` returns an I/O error, the in-memory
    /// buffer state is no longer trustworthy — another thread's appends
    /// may sit alongside the failed batch and a subsequent successful
    /// flush would silently persist ops the originating client was told
    /// failed. Poisoning the log here forces operators to restart, at
    /// which point recovery reconstructs from the on-disk state.
    poisoned: bool,
    /// Lock-free mirror of `write_pos`/`logical_start` for observers.
    atomics: Arc<RedoAtomics>,
}

impl RedoLog {
    /// Open or create a redo log at the given device region.
    ///
    /// Reads the on-disk header (F-G4-001) to recover `next_sequence` and
    /// `checkpoint_seq`; if the header magic is missing the region is
    /// freshly initialised. If the magic matches a foreign / older format
    /// the open fails with [`RedoError::HeaderMagicMismatch`] (or
    /// [`RedoError::HeaderCrcMismatch`] / [`RedoError::UnsupportedHeaderVersion`])
    /// rather than silently falling back to "scan from offset 0 and seed
    /// `next_sequence = 1`", which prior to F-G4-001 reused sequence
    /// numbers across restarts whenever compaction emptied the entries
    /// region.
    ///
    /// # Errors
    ///
    /// * [`RedoError::OutOfBounds`] if `log_offset + log_size` would
    ///   overflow `u64` or extend past the device's reported size.
    /// * [`RedoError::LogRegionTooSmall`] if the region cannot hold the
    ///   fixed-size header block plus a single aligned entry block.
    /// * [`RedoError::HeaderMagicMismatch`] / [`RedoError::HeaderCrcMismatch`] /
    ///   [`RedoError::UnsupportedHeaderVersion`] when the existing header
    ///   does not match this binary's expected format.
    pub fn open(device: Arc<dyn BlockDevice>, log_offset: u64, log_size: u64) -> Result<Self> {
        let device_size = device.size();
        let end = log_offset
            .checked_add(log_size)
            .ok_or(RedoError::OutOfBounds {
                log_offset,
                log_size,
                device_size,
            })?;
        if end > device_size {
            return Err(RedoError::OutOfBounds {
                log_offset,
                log_size,
                device_size,
            });
        }

        let align = device.alignment() as u64;
        if align == 0 {
            return Err(RedoError::LogRegionTooSmall {
                log_size,
                required_for_header: 1,
            });
        }
        // F-G4-001: reserve at least one aligned block for the header,
        // and at least one more for entries.
        let header_block_size = align;
        if log_size < header_block_size.saturating_mul(2) {
            return Err(RedoError::LogRegionTooSmall {
                log_size,
                required_for_header: header_block_size.saturating_mul(2),
            });
        }

        let mut log = Self {
            device,
            log_offset,
            log_size,
            header_block_size,
            write_pos: 0,
            logical_start: 0,
            checkpoint_seq: 0,
            next_sequence: 1,
            buffer: Vec::new(),
            entries_cache: Vec::new(),
            pending_entries: Vec::new(),
            buffered_entries: 0,
            poisoned: false,
            atomics: Arc::new(RedoAtomics {
                write_pos: AtomicU64::new(0),
                logical_start: AtomicU64::new(0),
                entries_region_size: log_size - header_block_size,
            }),
        };

        // Try to read the on-disk header. If the magic is absent
        // (region is freshly zeroed), initialise a fresh header below.
        // If the magic is present but invalid, propagate the error so
        // the operator sees a clear "version not supported" rather than
        // silently misparsing.
        let header_present = log.read_header_or_init()?;

        // Scan existing entries from the entries region to find the
        // write tail and entries cache. A checksum/truncation failure at
        // the final entry is treated as the end of valid log data, so
        // appends after restart resume at the last fully-valid entry
        // instead of overwriting from offset zero.
        let (entries, tail_pos) = log.scan_entries_region_with_tail()?;
        log.write_pos = tail_pos;
        log.entries_cache = entries.clone();

        // F-G4-001: the on-disk header is the authoritative source for
        // `next_sequence`. Only fall back to scan-derived value if the
        // region was freshly initialised (no header was written yet) or
        // the scan observed a strictly higher sequence than the header
        // recorded.
        if let Some(last) = entries.last() {
            let scan_next = last.sequence + 1;
            if !header_present || scan_next > log.next_sequence {
                log.next_sequence = scan_next;
            }
        }

        // Find last checkpoint to set checkpoint_seq (only if not
        // already set authoritatively by the header).
        if !header_present {
            for e in entries.iter().rev() {
                if e.op == RedoOp::Checkpoint {
                    log.checkpoint_seq = e.sequence;
                    break;
                }
            }
        }

        // If the region looked fresh, write an initial header so the
        // next open sees the same authoritative state.
        if !header_present {
            log.write_header()?;
        }

        // Sync recovered write_pos/logical_start into the lock-free atomics.
        log.atomics
            .write_pos
            .store(log.write_pos, AtomicOrdering::Relaxed);
        log.atomics
            .logical_start
            .store(log.logical_start, AtomicOrdering::Relaxed);
        Ok(log)
    }

    /// Device byte offset of the first entry byte (header block end).
    fn entries_region_offset(&self) -> u64 {
        self.log_offset + self.header_block_size
    }

    /// Number of bytes available for entries (region minus header block).
    fn entries_region_size(&self) -> u64 {
        self.log_size - self.header_block_size
    }

    /// Read the on-disk header (F-G4-001). Returns `Ok(true)` when a
    /// valid header was decoded; `Ok(false)` when the header block is
    /// freshly zeroed (magic bytes all zero) and the caller should
    /// initialise a new header. Returns a typed error for non-zero /
    /// mismatched magic, bad CRC, or unsupported version.
    fn read_header_or_init(&mut self) -> Result<bool> {
        let align = self.device.alignment();
        let mut buf = AlignedBuf::new(self.header_block_size as usize, align);
        self.device.pread_exact_at(&mut buf, self.log_offset)?;
        if buf[..HEADER_FIXED_LEN].iter().all(|b| *b == 0) {
            return Ok(false);
        }
        let header = RedoHeader::deserialize(&buf[..HEADER_FIXED_LEN])?;
        self.next_sequence = header.next_sequence.max(1);
        self.checkpoint_seq = header.checkpoint_seq;
        self.logical_start = header.logical_start;
        Ok(true)
    }

    /// Serialize and durably write the header. Pads to `header_block_size`
    /// with zeros so the write covers the full reserved block atomically
    /// at the device's alignment.
    fn write_header(&self) -> Result<()> {
        self.write_header_bytes()?;
        self.device.sync()?;
        Ok(())
    }

    /// Serialize and pwrite the header WITHOUT an fsync. The caller is
    /// responsible for the device sync that makes it durable.
    ///
    /// The hot-path `flush` uses this to fold the header pwrite into the SAME
    /// fsync as the entries write (one `device.sync()` flushes both the
    /// entries block and the header block — non-overlapping regions of the
    /// same device), halving the per-flush fsync count. `reset` /
    /// `mark_checkpoint` / `compact_prefix_through` use [`Self::write_header`]
    /// (pwrite + its own fsync) because they make the header durable on its
    /// own, when the entries region is empty and cannot reconstruct
    /// `next_sequence` on reopen.
    fn write_header_bytes(&self) -> Result<()> {
        let header = RedoHeader {
            next_sequence: self.next_sequence,
            checkpoint_seq: self.checkpoint_seq,
            logical_start: self.logical_start,
        };
        let bytes = header.serialize();
        let align = self.device.alignment();
        let mut buf = AlignedBuf::new(self.header_block_size as usize, align);
        buf[..bytes.len()].copy_from_slice(&bytes);
        // Trailing bytes are already zeroed by AlignedBuf::new.
        self.device.pwrite_all_at(&buf, self.log_offset)?;
        Ok(())
    }

    /// Append an operation to the buffer (not yet durable).
    ///
    /// Returns the assigned sequence number.
    pub fn append(&mut self, op: RedoOp) -> Result<u64> {
        // F-G4-002: refuse to append on a poisoned log.
        if self.poisoned {
            return Err(RedoError::Poisoned);
        }
        let seq = self.next_sequence;
        let entry = RedoEntry { sequence: seq, op };
        let bytes = entry.serialize();

        let entries_capacity = self.entries_region_size();
        if self.write_pos + self.buffer.len() as u64 + bytes.len() as u64 > entries_capacity {
            return Err(RedoError::LogFull {
                used: self.write_pos + self.buffer.len() as u64,
                capacity: entries_capacity,
            });
        }

        self.buffer.extend_from_slice(&bytes);
        self.pending_entries.push(entry);
        self.next_sequence += 1;
        if let Some(m) = redo_metrics() {
            m.redo_append_total.inc();
            self.buffered_entries += 1;
        }
        Ok(seq)
    }

    /// Flush the buffer to device, making all appended entries durable.
    ///
    /// F-G4-004: writes are append-only at aligned offsets — the buffer
    /// is padded to the next alignment boundary in-memory, so there is
    /// no read-modify-write of the trailing partial block. After the
    /// entries pwrite the header is rewritten with the new
    /// `next_sequence` so the value survives a restart even when the
    /// entries region is later compacted to empty (F-G4-001).
    ///
    /// F-G4-002: any I/O error here poisons the log; the in-memory
    /// buffer is dropped before returning so a retry by a different
    /// thread cannot accidentally re-flush the same bytes.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn flush(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(RedoError::Poisoned);
        }
        if self.buffer.is_empty() {
            return Ok(());
        }

        let align = self.device.alignment();
        let align_u64 = align as u64;
        let entries_region_off = self.entries_region_offset();
        let device_offset = entries_region_off + self.write_pos;
        // Append-only: writes start at an aligned offset. New flushes
        // always keep `write_pos` block-aligned (see end of function),
        // so `intra` is normally 0; the partial-block branch is taken
        // only if a previous run left a non-aligned tail.
        let aligned_offset = device_offset / align_u64 * align_u64;
        let intra = (device_offset - aligned_offset) as usize;
        let total = intra + self.buffer.len();
        // Round up to alignment, padding with zeros — the
        // sequence-monotonicity scan stops at the first length=0 word,
        // so trailing zero bytes are the natural end-of-data sentinel.
        let aligned_total = total.div_ceil(align) * align;

        let mut buf = AlignedBuf::new(aligned_total, align);
        if intra > 0 {
            // F-G4-004: read back only the leading partial block — not
            // the entire aligned_total. Trailing bytes past our buffer
            // remain zero (AlignedBuf::new), so there is no
            // read-then-rewrite of the tail.
            self.device
                .pread_exact_at(&mut buf[..align], aligned_offset)?;
            // Defensive zero of anything past `intra` that the read
            // pulled in, so the post-buffer area is a clean tail-zero
            // sentinel.
            buf[intra..align].fill(0);
        }

        buf[intra..intra + self.buffer.len()].copy_from_slice(&self.buffer);
        if let Err(e) = self.device.pwrite_all_at(&buf, aligned_offset) {
            if let Some(m) = redo_metrics() {
                m.redo_flush_errors_total.inc();
            }
            self.poison_drop_buffer();
            return Err(e.into());
        }

        // F-G4-001 + PERF #5: pwrite the header (new `next_sequence` high-water
        // + checkpoint_seq + logical_start) and let the SINGLE entries fsync
        // below make BOTH durable. A `device.sync()` flushes every dirty block
        // of the device, so one sync covers the entries block and the
        // (non-overlapping) header block. This halves the per-flush fsync count
        // versus a second standalone header fsync, while still persisting
        // `next_sequence` on every flush — the high-water mark a corrupt-tail
        // reopen relies on to avoid reusing a sequence number.
        if let Err(e) = self.write_header_bytes() {
            if let Some(m) = redo_metrics() {
                m.redo_flush_errors_total.inc();
            }
            self.poison_drop_buffer();
            return Err(e);
        }

        crate::fault_injection::check(crate::fault_injection::SyncPoint::BeforeRedoFsync);
        // Scope the sync call tightly so the latency histogram reflects only
        // the fsync wall time, not the buffer-assembly / pwrite preamble.
        //
        // PERF #6: `sync_data` (fdatasync on Linux) — the redo log is a fixed
        // length region (never resized), so the inode-metadata flush a full
        // fsync would do is unnecessary; skipping it cuts redo flush cost on
        // Linux. This one fsync covers both the entries pwrite and the folded
        // header pwrite (PERF #5). reset/checkpoint/compaction keep the full
        // `sync` (rare, and they rewrite the header on its own).
        let sync_start = Instant::now();
        let sync_res = self.device.sync_data();
        if let Some(m) = redo_metrics() {
            m.redo_flush_latency_ns.record_since(sync_start);
        }
        if let Err(e) = sync_res {
            if let Some(m) = redo_metrics() {
                m.redo_flush_errors_total.inc();
            }
            self.poison_drop_buffer();
            return Err(e.into());
        }
        crate::fault_injection::check(crate::fault_injection::SyncPoint::AfterRedoFsync);

        let flushed_bytes = self.buffer.len() as u64;
        let flushed_entries = self.buffered_entries;
        // F-G4-004: bump write_pos by the aligned amount so subsequent
        // flushes always start at the next aligned offset.
        self.write_pos = (aligned_offset + aligned_total as u64) - entries_region_off;
        self.publish_atomics();
        self.buffer.clear();
        self.entries_cache.append(&mut self.pending_entries);
        self.buffered_entries = 0;

        if let Some(m) = redo_metrics() {
            m.redo_bytes_per_flush.record_ns(flushed_bytes);
            m.redo_entries_per_flush.record_ns(flushed_entries);
        }
        Ok(())
    }

    /// F-G4-002: drop all in-flight buffer + pending state on flush
    /// failure. Other threads that contributed to `buffer` will see
    /// `Poisoned` on their next call.
    fn poison_drop_buffer(&mut self) {
        self.poisoned = true;
        self.buffer.clear();
        self.pending_entries.clear();
        self.buffered_entries = 0;
    }

    /// Append and flush in one call.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn append_and_flush(&mut self, op: RedoOp) -> Result<u64> {
        let seq = self.append(op)?;
        self.flush()?;
        Ok(seq)
    }

    /// Append a batch of operations and flush with a single fsync.
    ///
    /// All `ops` are appended to the in-memory buffer in order, then the
    /// buffer is flushed to device. The returned `(first_seq, last_seq)`
    /// covers the assigned sequence range. Empty `ops` returns `(0, 0)`
    /// without flushing.
    ///
    /// Used by two-phase durability for secondary indexes: multiple
    /// secondary-intent entries (e.g. one DAH + one unmined) are grouped
    /// into a single fsync before the redb transactions are committed.
    pub fn append_batch_and_flush(&mut self, ops: &[RedoOp]) -> Result<(u64, u64)> {
        if ops.is_empty() {
            return Ok((0, 0));
        }
        let first_seq = self.next_sequence;
        let mut last_seq = first_seq;
        for op in ops {
            last_seq = self.append(op.clone())?;
        }
        self.flush()?;
        Ok((first_seq, last_seq))
    }

    /// Write and flush a checkpoint marker.
    ///
    /// This only records the recovery boundary. It does not reclaim redo-log
    /// space; callers that have durably snapshotted all state must call
    /// [`RedoLog::reset`] after this marker to make earlier bytes reusable.
    pub fn mark_checkpoint(&mut self) -> Result<()> {
        let seq = self.append(RedoOp::Checkpoint)?;
        self.flush()?;
        self.checkpoint_seq = seq;
        // F-G4-001: persist the updated checkpoint_seq in the header.
        self.write_header()
    }

    /// Write and flush a recovery-progress marker.
    ///
    /// This marker means startup replay safely processed every redo entry
    /// through `through_sequence`. It does not replace checkpoints and does
    /// not reclaim bytes; it only bounds repeated recovery work if the
    /// process crashes again before the next checkpoint can reset the log.
    pub fn mark_recovery_progress(&mut self, through_sequence: u64) -> Result<()> {
        self.append(RedoOp::RecoveryProgress { through_sequence })?;
        self.flush()
    }

    /// Read all entries after the last checkpoint (for crash recovery).
    pub fn recover(&self) -> Result<Vec<RedoEntry>> {
        let all = self.scan_all()?;

        // F-G4-010: bound `progress_through` against the highest entry
        // sequence we have actually seen, so a corrupt-but-CRC-valid
        // `RecoveryProgress` with `through_sequence = u64::MAX` cannot
        // mask all post-marker entries from replay.
        let max_seq = all.iter().map(|e| e.sequence).max().unwrap_or(0);

        // Find last checkpoint and any recovery-progress marker after it.
        let mut start_idx = 0usize;
        let mut progress_through = 0u64;
        for (i, e) in all.iter().enumerate() {
            if e.op == RedoOp::Checkpoint {
                start_idx = i + 1;
                progress_through = 0;
            } else if let RedoOp::RecoveryProgress { through_sequence } = e.op
                && i >= start_idx
                && through_sequence > progress_through
                && through_sequence <= max_seq
            {
                progress_through = through_sequence;
            }
        }

        Ok(all[start_idx..]
            .iter()
            .filter(|entry| {
                !matches!(entry.op, RedoOp::RecoveryProgress { .. })
                    && entry.sequence > progress_through
            })
            .cloned()
            .collect())
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

    /// The sequence number of the earliest available entry in the log.
    ///
    /// Returns `Ok(Some(seq))` if the log contains at least one entry,
    /// `Ok(None)` if the log is empty. Used by replication catch-up to
    /// detect redo log truncation: if the earliest entry is beyond a
    /// replica's last-acked position, the log has wrapped and the
    /// replica needs a full resync instead of incremental catch-up.
    pub fn earliest_sequence(&self) -> Result<Option<u64>> {
        let all = self.scan_all()?;
        Ok(all.first().map(|e| e.sequence))
    }

    // F-G4-003: `advance_checkpoint` was dead code — only mutated an
    // in-memory `checkpoint_seq` and reclaimed nothing. The live
    // reclamation path is `compact_prefix_through`. The dead method has
    // been removed entirely.

    /// Current write position within the entries region (bytes from
    /// start of entries region; does NOT include the header block).
    pub fn write_position(&self) -> u64 {
        self.write_pos + self.buffer.len() as u64
    }

    /// Space remaining in the entries region.
    pub fn available_space(&self) -> u64 {
        self.entries_region_size()
            .saturating_sub(self.write_pos + self.buffer.len() as u64)
    }

    /// Fraction of the entries region's capacity currently used (0.0 .. 1.0).
    ///
    /// Used by the background checkpoint task to decide when to roll the
    /// log: when this exceeds the configured threshold the task takes a
    /// snapshot of the rest of the durability state, writes a checkpoint
    /// marker, and `reset()`s the log so future appends write from offset
    /// 0 again.
    pub fn usage_fraction(&self) -> f64 {
        let cap = self.entries_region_size();
        if cap == 0 {
            return 1.0;
        }
        let used = self.write_pos + self.buffer.len() as u64;
        used as f64 / cap as f64
    }

    /// Total capacity of the entries region in bytes.
    pub fn capacity(&self) -> u64 {
        self.entries_region_size()
    }

    /// Mirror the current `write_pos`/`logical_start` into the lock-free atomics.
    fn publish_atomics(&self) {
        self.atomics
            .write_pos
            .store(self.write_pos, AtomicOrdering::Relaxed);
        self.atomics
            .logical_start
            .store(self.logical_start, AtomicOrdering::Relaxed);
    }

    /// A cheap clonable handle to the lock-free space accounting.
    pub fn atomics(&self) -> Arc<RedoAtomics> {
        Arc::clone(&self.atomics)
    }

    /// Reset the log (after checkpoint + reclaim). Dangerous — only call
    /// when all entries have been checkpointed and applied.
    ///
    /// F-G4-013: zero the entire entries region (not just the first
    /// aligned block) so a stale entry left past the first block from a
    /// previous run cannot be re-discovered by
    /// `scan_entries_region_with_tail`. After zeroing, the header is
    /// rewritten so the persisted `next_sequence` does NOT roll back
    /// (F-G4-001).
    pub fn reset(&mut self) -> Result<()> {
        let align = self.device.alignment();
        let entries_off = self.entries_region_offset();
        let entries_size = self.entries_region_size() as usize;
        // Zero the entire entries region in alignment-sized chunks. For
        // the typical 64 MiB log this is a one-time cost at checkpoint
        // cadence; not in the hot append path.
        const ZERO_CHUNK: usize = 1024 * 1024; // 1 MiB at a time
        let chunk = ZERO_CHUNK.next_multiple_of(align);
        let buf = AlignedBuf::new(chunk, align);
        let mut written = 0usize;
        while written < entries_size {
            let remaining = entries_size - written;
            let this = remaining.min(chunk);
            let this_aligned = this.div_ceil(align) * align;
            self.device
                .pwrite_all_at(&buf[..this_aligned], entries_off + written as u64)?;
            written += this_aligned;
        }
        self.device.sync()?;
        self.write_pos = 0;
        // B-3: a full reset reclaims the entire region; the live window
        // starts at offset 0 again.
        self.logical_start = 0;
        self.publish_atomics();
        self.buffer.clear();
        self.entries_cache.clear();
        self.pending_entries.clear();
        self.buffered_entries = 0;
        // F-G4-001: persist the new write_pos / next_sequence in the
        // header — the entries region is empty but `next_sequence` must
        // not roll back across restarts.
        self.write_header()
    }

    /// Reclaim entries whose effects are covered by a durable snapshot.
    ///
    /// Unlike [`Self::reset`], this preserves any entries with sequence
    /// numbers greater than `through_sequence`. That lets checkpointing
    /// snapshot at a stable fence while concurrent or later writers keep
    /// their redo ranges available for recovery and replica catch-up.
    ///
    /// # Crash safety (B-3)
    ///
    /// The previous implementation rewrote the retained post-fence
    /// entries **in place** at the start of the entries region with a
    /// single multi-block `pwrite`. A torn power-loss write there
    /// destroyed those retained entries — which can include already-acked
    /// mutations from non-dispatch producers (allocator
    /// `AllocateRegion`/`FreeRegion`, secondary-intent records,
    /// engine-internal `AppendConflictingChild`). Compaction overwrote
    /// the only durable copy.
    ///
    /// This implementation never overwrites a live entry:
    ///
    /// * **Empty retained set** (the common clean-checkpoint case) →
    ///   delegate to [`Self::reset`], which zeroes the whole region and
    ///   resets `logical_start` to 0. Nothing live exists to lose.
    /// * **Non-empty retained set** → write a fresh copy of the retained
    ///   entries (plus a zero sentinel block) into a region that holds NO
    ///   live bytes — either past the current tail, or, if the tail has no
    ///   room, into the stale front gap `[0, logical_start)` left by a
    ///   previous compaction — and `fsync`. The original retained copy
    ///   stays intact and is still the one the header points at. Only then
    ///   is the CRC-protected header atomically flipped to point
    ///   `logical_start` at the new copy. A crash *before* the header flip
    ///   recovers the pre-compaction log (old copy, old `logical_start`);
    ///   a crash *after* it recovers the post-compaction log (new copy,
    ///   fully fsynced before the flip). A torn write of the new copy is
    ///   harmless because the header was not yet flipped to it.
    ///
    /// The on-disk header preserves the high-water `next_sequence` so even
    /// an empty retained set does not roll back the sequence counter
    /// (F-G4-001).
    pub fn compact_prefix_through(&mut self, through_sequence: u64) -> Result<()> {
        if self.poisoned {
            return Err(RedoError::Poisoned);
        }
        self.flush()?;

        let mut retained: Vec<RedoEntry> = self
            .entries_cache
            .iter()
            .filter(|entry| entry.sequence > through_sequence)
            .cloned()
            .collect();
        if retained.iter().all(|entry| {
            matches!(
                &entry.op,
                RedoOp::RecoveryProgress {
                    through_sequence: marker_through,
                } if *marker_through <= through_sequence
            )
        }) {
            retained.clear();
        }

        // Empty retained set: nothing live to preserve — zero the whole
        // region and reset `logical_start` to 0. This is the only path
        // that physically reclaims the prefix, and it is crash-safe
        // because there is no live entry to overwrite.
        if retained.is_empty() {
            return self.reset();
        }

        let mut bytes = Vec::new();
        for entry in &retained {
            bytes.extend_from_slice(&entry.serialize());
        }

        let align = self.device.alignment();
        let entries_capacity = self.entries_region_size();
        let content_aligned = bytes
            .len()
            .checked_add(ENTRY_HEADER_SIZE)
            .map(|n| n.div_ceil(align) * align)
            .ok_or(RedoError::LogFull {
                used: u64::MAX,
                capacity: entries_capacity,
            })?;
        // Reserve one extra aligned block for the zero sentinel so the
        // scan stops cleanly past the new copy (F-G4-012).
        let total_with_tail = content_aligned.saturating_add(align);
        let total_u64 = total_with_tail as u64;

        // Pick a staging region that holds NO live bytes. Live bytes
        // occupy `[logical_start, write_pos)`. Prefer past the tail; fall
        // back to the stale front gap `[0, logical_start)`.
        let new_start = if self
            .write_pos
            .checked_add(total_u64)
            .is_some_and(|end| end <= entries_capacity)
        {
            self.write_pos
        } else if total_u64 <= self.logical_start {
            // The front gap left by a previous compaction is large enough.
            // Writing here cannot touch the live `[logical_start, ..)`
            // range, so it is just as crash-safe as the past-tail path.
            0
        } else {
            // Neither stale region can hold the relocated copy without
            // overwriting live bytes. Surface LogFull so the caller defers
            // reclamation to the next clean checkpoint (empty-retained
            // reset path frees the whole region).
            return Err(RedoError::LogFull {
                used: self.write_pos.saturating_add(total_u64),
                capacity: entries_capacity,
            });
        };

        let mut buf = AlignedBuf::new(total_with_tail, align);
        buf[..bytes.len()].copy_from_slice(&bytes);
        // Phase 1: durably stage the relocated copy in the chosen stale
        // region. The header still points `logical_start` at the original
        // copy, so a crash anywhere up to and including this fsync
        // recovers the pre-compaction log intact.
        self.device
            .pwrite_all_at(&buf, self.entries_region_offset() + new_start)?;
        self.device.sync()?;

        // Phase 2: atomically flip the CRC-protected header to the new
        // copy. After this fsync the relocated copy is authoritative.
        self.logical_start = new_start;
        self.write_pos = new_start + content_aligned as u64;
        self.publish_atomics();
        self.buffer.clear();
        self.entries_cache = retained;
        self.pending_entries.clear();
        self.buffered_entries = 0;
        self.write_header()?;
        Ok(())
    }

    /// Scan all valid entries in the log from the entries cache.
    fn scan_all(&self) -> Result<Vec<RedoEntry>> {
        Ok(self.entries_cache.clone())
    }

    /// Scan the entries region from disk in aligned chunks (F-G4-009).
    ///
    /// Prior to this fix the scan allocated `log_size` bytes up-front
    /// (default 64 MiB; configurable to GiB). Now we read in
    /// `SCAN_CHUNK_BYTES` slices, carrying over any trailing partial
    /// entry between chunks. Memory footprint at startup is bounded at
    /// `SCAN_CHUNK_BYTES + entries.size_of` instead of `log_size +
    /// entries.size_of`.
    ///
    /// **F-G4-004 pad-gap handling.** `flush()` rounds `write_pos` up to
    /// the device alignment after every flush, so the bytes between the
    /// last entry of a flush and the next alignment boundary are zero
    /// padding. A naive "stop at the first `length=0` word" scan would
    /// treat that pad as end-of-log and lose every entry written in
    /// subsequent flushes within the same checkpoint epoch.
    ///
    /// Rule: a `length=0` word that sits at the **start of an alignment
    /// unit** is a true end-of-log sentinel. A `length=0` word mid-block
    /// is interpreted as flush pad: validate that all bytes from there to
    /// the next aligned boundary are zero, advance `local_pos` past the
    /// pad, and retry the deserialize at the aligned position. If the
    /// next entry's sequence is non-consecutive (e.g. stale data left
    /// behind beyond a `compact_prefix_through` sentinel block), treat
    /// that as end-of-log rather than raising `SequenceOutOfOrder`.
    fn scan_entries_region_with_tail(&self) -> Result<(Vec<RedoEntry>, u64)> {
        // 4 MiB working buffer (alignment-rounded). Tunable.
        const SCAN_CHUNK_BYTES: usize = 4 * 1024 * 1024;

        let align = self.device.alignment();
        let align_u64 = align as u64;
        let entries_off = self.entries_region_offset();
        let entries_size = self.entries_region_size();
        let total_to_read = entries_size as usize;
        let chunk = SCAN_CHUNK_BYTES.next_multiple_of(align).max(align);

        let mut entries = Vec::new();
        let mut prev_seq: Option<u64> = None;
        // Bytes pending from the previous chunk (partial trailing entry).
        let mut carry: Vec<u8> = Vec::new();
        // Total bytes consumed within the entries region (relative to
        // the entries region start). Includes any pad bytes skipped via
        // the F-G4-004 alignment-pad rule so the resulting tail is
        // aligned for subsequent appends.
        //
        // B-3: the scan starts at `logical_start`, not at offset 0. Bytes
        // before `logical_start` are stale entries that a prior
        // compaction logically reclaimed by advancing the header pointer
        // instead of physically overwriting them — so they are never read
        // and a torn compaction write could never have destroyed a live
        // retained entry. `logical_start` is always alignment-aligned
        // (compaction snaps it to a block boundary), so the F-G4-004
        // pad-gap arithmetic below stays valid.
        let mut consumed_in_region: u64 = self.logical_start;
        let mut scan_start: u64 = self.logical_start;
        // Set when we have just skipped over flush pad zeros to the next
        // alignment boundary. Used to soften the strict sequence-order
        // check at the first post-skip entry: a stale or unrelated entry
        // beyond the skipped gap is treated as end-of-log instead of a
        // hard `SequenceOutOfOrder` error.
        let mut just_skipped_pad = false;

        while scan_start < total_to_read as u64 {
            let remaining = total_to_read as u64 - scan_start;
            let this_read = remaining.min(chunk as u64) as usize;
            let aligned_read = this_read.div_ceil(align) * align;
            let mut buf = AlignedBuf::new(aligned_read, align);
            self.device
                .pread_exact_at(&mut buf, entries_off + scan_start)?;
            let chunk_slice = &buf[..this_read];

            // Concatenate any partial-entry carry from the previous
            // chunk with this chunk's bytes. The carry is bounded by
            // the max entry size, not by total scan size.
            let combined: Vec<u8> = if carry.is_empty() {
                chunk_slice.to_vec()
            } else {
                let mut v = std::mem::take(&mut carry);
                v.extend_from_slice(chunk_slice);
                v
            };

            // Drain whole entries from `combined`.
            let mut local_pos = 0usize;
            let mut stop_scan = false;
            loop {
                match RedoEntry::deserialize(&combined[local_pos..]) {
                    Some((entry, consumed_entry)) => {
                        if let Some(prev) = prev_seq
                            && prev.checked_add(1) != Some(entry.sequence)
                        {
                            if just_skipped_pad {
                                // Non-consecutive sequence past a pad
                                // skip: this is stale data beyond a
                                // legitimate end-of-flush boundary
                                // (e.g. left over from before
                                // `compact_prefix_through` zeroed the
                                // sentinel block). Stop here; the tail
                                // position already points at this stale
                                // block so future appends overwrite it.
                                stop_scan = true;
                                break;
                            }
                            return Err(RedoError::SequenceOutOfOrder {
                                offset: consumed_in_region + local_pos as u64,
                                previous: prev,
                                current: entry.sequence,
                            });
                        }
                        prev_seq = Some(entry.sequence);
                        entries.push(entry);
                        local_pos += consumed_entry;
                        just_skipped_pad = false;
                    }
                    None => {
                        // Distinguish "end-of-data marker" (length=0)
                        // from "partial entry split across chunks".
                        if combined[local_pos..].len() < ENTRY_HEADER_SIZE {
                            // Partial entry: defer to next chunk.
                            carry = combined[local_pos..].to_vec();
                            break;
                        }
                        let lw = u32::from_le_bytes(
                            combined[local_pos..local_pos + 4].try_into().unwrap(),
                        );
                        if lw != 0 {
                            // Truncated or CRC-failing entry: end of log.
                            carry = combined[local_pos..].to_vec();
                            break;
                        }
                        // length=0. Apply F-G4-004 pad-gap rule.
                        let abs_pos = consumed_in_region + local_pos as u64;
                        if abs_pos.is_multiple_of(align_u64) {
                            // Aligned boundary + length=0 → true end of log.
                            stop_scan = true;
                            break;
                        }
                        // Mid-block length=0: treat as flush pad. The
                        // padding region MUST be entirely zero — a
                        // non-zero byte here is corruption, not legit
                        // pad, so stop scanning rather than skipping
                        // into garbage.
                        let pad_end_abs = abs_pos.next_multiple_of(align_u64);
                        let pad_skip = (pad_end_abs - abs_pos) as usize;
                        if combined[local_pos..].len() < pad_skip {
                            // Not enough bytes in this combined buffer
                            // to verify the full pad. Defer to next
                            // chunk; `prev_seq` and entry list carry
                            // forward unchanged.
                            carry = combined[local_pos..].to_vec();
                            break;
                        }
                        if !combined[local_pos..local_pos + pad_skip]
                            .iter()
                            .all(|b| *b == 0)
                        {
                            // Non-zero byte within the pad region: not
                            // legitimate flush pad. Stop here.
                            stop_scan = true;
                            break;
                        }
                        local_pos += pad_skip;
                        just_skipped_pad = true;
                        // Re-enter loop; deserialize from the new
                        // aligned position.
                    }
                }
            }
            consumed_in_region += local_pos as u64;
            if stop_scan {
                return Ok((entries, consumed_in_region));
            }
            scan_start += this_read as u64;
        }
        Ok((entries, consumed_in_region))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::{DeviceError, MemoryDevice};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// `LOG_FULL_MESSAGE_PREFIX` must remain a prefix of the actual
    /// `RedoError::LogFull` `Display` string. Callers that flatten the
    /// error into a `String` (e.g. the replication intent-recovery startup
    /// barrier) discriminate transient redo backpressure from terminal
    /// device faults by matching this prefix; if the `#[error(...)]`
    /// format and the prefix ever drift apart, that discrimination breaks
    /// silently and rejoin-after-quiesce regresses to a terminal abort.
    #[test]
    fn log_full_message_prefix_matches_display() {
        let rendered = RedoError::LogFull {
            used: 10,
            capacity: 20,
        }
        .to_string();
        assert!(
            rendered.starts_with(LOG_FULL_MESSAGE_PREFIX),
            "LOG_FULL_MESSAGE_PREFIX {LOG_FULL_MESSAGE_PREFIX:?} is not a prefix of {rendered:?}",
        );
    }

    struct ReadFailingDevice {
        inner: Arc<MemoryDevice>,
        fail_reads: AtomicBool,
    }

    impl ReadFailingDevice {
        fn new(size: u64) -> Self {
            Self {
                inner: Arc::new(MemoryDevice::new(size, 4096).unwrap()),
                fail_reads: AtomicBool::new(false),
            }
        }

        fn fail_reads(&self) {
            self.fail_reads.store(true, Ordering::SeqCst);
        }
    }

    impl BlockDevice for ReadFailingDevice {
        fn pread(&self, buf: &mut [u8], offset: u64) -> crate::device::Result<usize> {
            if self.fail_reads.load(Ordering::SeqCst) {
                return Err(DeviceError::Io(std::io::Error::other(
                    "simulated redo pread failure",
                )));
            }
            self.inner.pread(buf, offset)
        }

        fn pwrite(&self, buf: &[u8], offset: u64) -> crate::device::Result<usize> {
            self.inner.pwrite(buf, offset)
        }

        fn alignment(&self) -> usize {
            self.inner.alignment()
        }

        fn size(&self) -> u64 {
            self.inner.size()
        }

        fn sync(&self) -> crate::device::Result<()> {
            self.inner.sync()
        }
    }

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
    fn open_with_out_of_bounds_log_region_fails() {
        // Device is 64 KiB; attempt to open a log that extends past it.
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024, 4096).unwrap());
        let log_offset = 32 * 1024;
        let log_size = 64 * 1024; // end = 96 KiB > device size (64 KiB)
        match RedoLog::open(dev.clone(), log_offset, log_size) {
            Ok(_) => panic!("expected OutOfBounds, got Ok"),
            Err(RedoError::OutOfBounds {
                log_offset: lo,
                log_size: ls,
                device_size,
            }) => {
                assert_eq!(lo, log_offset);
                assert_eq!(ls, log_size);
                assert_eq!(device_size, 64 * 1024);
            }
            Err(other) => panic!("expected OutOfBounds, got {other:?}"),
        }

        // Overflow case: u64::MAX offset.
        match RedoLog::open(dev, u64::MAX, 1) {
            Ok(_) => panic!("expected OutOfBounds on overflow, got Ok"),
            Err(RedoError::OutOfBounds { .. }) => {}
            Err(other) => panic!("expected OutOfBounds, got {other:?}"),
        }
    }

    #[test]
    fn append_flush_recover() {
        let (_, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Spend {
            tx_key: test_key(1),
            offset: 5,
            spending_data: [0xAB; 36],
            new_spent_count: 1,
        })
        .unwrap();

        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].op {
            RedoOp::Spend {
                tx_key,
                offset,
                spending_data,
                new_spent_count,
            } => {
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
            log.append(RedoOp::Freeze {
                tx_key: test_key(i),
                offset: i as u32,
            })
            .unwrap();
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
        log.append(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        // Don't flush

        // Simulate crash — reopen
        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn mark_checkpoint_clears_recovery_entries() {
        let (_, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        log.mark_checkpoint().unwrap();

        let entries = log.recover().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn mark_checkpoint_only_returns_after() {
        let (_, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        log.mark_checkpoint().unwrap();
        log.append_and_flush(RedoOp::Unfreeze {
            tx_key: test_key(2),
            offset: 1,
        })
        .unwrap();

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
        // F-G4-004: append both entries before a single flush so they
        // sit contiguously on disk; otherwise the trailing alignment
        // pad between separate flushes is interpreted (post-fix) as a
        // pad gap, and the second entry would still be reached past the
        // corruption-free pad. We want the corruption itself to be the
        // thing that stops the scan.
        log.append(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        log.append(RedoOp::Freeze {
            tx_key: test_key(2),
            offset: 1,
        })
        .unwrap();
        log.flush().unwrap();

        // Corrupt a byte well inside the second entry. The entries
        // region starts at offset = device alignment (the F-G4-001
        // header block claims the first alignment unit).
        let align = dev.alignment();
        let entries_region_offset = align as u64;
        let mut buf = AlignedBuf::new(align, align);
        dev.pread(&mut buf, entries_region_offset).unwrap();
        // First Freeze entry is 53 bytes (4 length + 8 seq + 1 type +
        // 32 txid + 4 offset + 4 crc). Corrupt the payload of the
        // second entry at offset 53 + ~20 from the start of the entries
        // block.
        buf[73] ^= 0xFF;
        dev.pwrite(&buf, entries_region_offset).unwrap();

        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        // Should get at most the first entry (second is corrupt).
        assert!(entries.len() <= 1);
    }

    #[test]
    fn redo_sequence_monotonicity_validation() {
        let (dev, mut log) = make_log(1024 * 1024);
        let first_op = RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        };
        let second_op = RedoOp::Freeze {
            tx_key: test_key(2),
            offset: 1,
        };
        // F-G4-004: append both in one flush so the entries are
        // contiguous on disk; otherwise each flush would block-align
        // write_pos and the trailing zero-padding would stop the scan
        // before reaching the second entry.
        log.append(first_op.clone()).unwrap();
        log.append(second_op.clone()).unwrap();
        log.flush().unwrap();

        // Rewrite the second entry's sequence number to 99 so the
        // monotonicity scan rejects it. The first alignment unit of
        // the redo region is the F-G4-001 header block; the entries
        // region starts at offset = device alignment.
        let first_entry = RedoEntry {
            sequence: 1,
            op: first_op,
        }
        .serialize();
        let rewritten_second = RedoEntry {
            sequence: 99,
            op: second_op,
        }
        .serialize();
        let align = dev.alignment();
        let entries_region_offset = align as u64;
        let mut buf = AlignedBuf::new(align, align);
        dev.pread(&mut buf, entries_region_offset).unwrap();
        let second_offset = first_entry.len();
        buf[second_offset..second_offset + rewritten_second.len()]
            .copy_from_slice(&rewritten_second);
        dev.pwrite(&buf, entries_region_offset).unwrap();

        match RedoLog::open(dev, 0, 1024 * 1024) {
            Err(RedoError::SequenceOutOfOrder {
                previous, current, ..
            }) => {
                assert_eq!(previous, 1);
                assert_eq!(current, 99);
            }
            Ok(_) => panic!("expected SequenceOutOfOrder, got Ok"),
            Err(other) => panic!("expected SequenceOutOfOrder, got {other:?}"),
        }
    }

    // -- Serialization round-trip tests --

    #[test]
    fn round_trip_all_variants() {
        let ops = vec![
            RedoOp::Spend {
                tx_key: test_key(1),
                offset: 5,
                spending_data: [0xAB; 36],
                new_spent_count: 42,
            },
            RedoOp::Unspend {
                tx_key: test_key(2),
                offset: 3,
                spending_data: Some([0xCD; 36]),
                new_spent_count: 10,
            },
            RedoOp::SetMined {
                tx_key: test_key(3),
                block_id: 100,
                block_height: 800000,
                subtree_idx: 7,
                unset: false,
            },
            RedoOp::SetMined {
                tx_key: test_key(4),
                block_id: 200,
                block_height: 900000,
                subtree_idx: 3,
                unset: true,
            },
            RedoOp::Freeze {
                tx_key: test_key(5),
                offset: 0,
            },
            RedoOp::Unfreeze {
                tx_key: test_key(6),
                offset: 1,
            },
            RedoOp::Reassign {
                tx_key: test_key(7),
                offset: 2,
                new_hash: [0xCC; 32],
                block_height: 1000,
                spendable_after: 100,
            },
            RedoOp::PruneSlot {
                tx_key: test_key(8),
                offset: 4,
            },
            RedoOp::Create {
                tx_key: test_key(9),
                record_offset: 4096,
                utxo_count: 10,
            },
            RedoOp::Delete {
                tx_key: test_key(10),
                record_offset: 8192,
                record_size: 1024,
            },
            RedoOp::SetConflicting {
                tx_key: test_key(11),
                value: true,
                current_block_height: 500,
                block_height_retention: 288,
            },
            RedoOp::AppendConflictingChild {
                parent_key: test_key(111),
                child_txid: [0xDD; 32],
            },
            RedoOp::RemoveConflictingChild {
                parent_key: test_key(111),
                child_txid: [0xDD; 32],
            },
            RedoOp::AppendDeletedChild {
                parent_key: test_key(112),
                child_txid: [0xDE; 32],
            },
            RedoOp::SetLocked {
                tx_key: test_key(12),
                value: false,
            },
            RedoOp::PreserveUntil {
                tx_key: test_key(13),
                block_height: 5000,
            },
            RedoOp::MarkOnLongestChain {
                tx_key: test_key(14),
                on_longest_chain: true,
                current_block_height: 600,
                block_height_retention: 288,
                generation: 1,
            },
            RedoOp::SecondaryUnminedUpdate {
                tx_key: test_key(15),
                old_height: 0,
                new_height: 500,
            },
            RedoOp::SecondaryDahUpdate {
                tx_key: test_key(16),
                old_height: 100,
                new_height: 600,
            },
            RedoOp::RecoveryProgress {
                through_sequence: 16,
            },
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
            match log.append(RedoOp::Freeze {
                tx_key: test_key(count as u8),
                offset: 0,
            }) {
                Ok(_) => {
                    count += 1;
                }
                Err(RedoError::LogFull { .. }) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(count > 0);
    }

    #[test]
    fn mark_checkpoint_then_reset_reclaims_space() {
        // F-G4-001 reserves one alignment unit at the start of the
        // region for the header; the entries region needs another
        // alignment unit for at least one flush. 50 Freeze entries × 53
        // bytes ≈ 2650 bytes, so a 16 KiB log gives ample room.
        let (_, mut log) = make_log(16 * 1024);
        // Fill most of the log
        for i in 0..50u8 {
            log.append(RedoOp::Freeze {
                tx_key: test_key(i),
                offset: 0,
            })
            .unwrap();
        }
        log.flush().unwrap();
        log.mark_checkpoint().unwrap();

        // Reset reclaims all space
        log.reset().unwrap();

        // Should be able to write again
        log.append_and_flush(RedoOp::Freeze {
            tx_key: test_key(99),
            offset: 0,
        })
        .unwrap();
        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn mark_checkpoint_does_not_reclaim_space() {
        let (_, mut log) = make_log(1024 * 1024);
        let initial = log.available_space();
        log.append_and_flush(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        let after_append = log.available_space();

        log.mark_checkpoint().unwrap();

        assert!(
            log.available_space() < after_append,
            "mark_checkpoint only writes a marker; reset performs reclamation"
        );
        assert!(after_append < initial);
    }

    #[test]
    fn available_space_decreases() {
        let (_, mut log) = make_log(1024 * 1024);
        let initial = log.available_space();
        log.append(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        assert!(log.available_space() < initial);
    }

    // -- Reopen persistence test --

    #[test]
    fn reopen_sees_flushed_entries() {
        // F-G4-004: append both entries before a single flush so they
        // sit contiguously in one block on disk. Two separate flushes
        // would block-align write_pos between them and the scan would
        // stop at the trailing zero padding after the first entry.
        let (dev, mut log) = make_log(1024 * 1024);
        log.append(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        log.append(RedoOp::Freeze {
            tx_key: test_key(2),
            offset: 1,
        })
        .unwrap();
        log.flush().unwrap();
        drop(log);

        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn reopen_recovers_entries_across_multiple_flushes_with_alignment_pad() {
        // Regression test for the F-G4-004 scan/pad interaction.
        //
        // F-G4-004 rounds `write_pos` up to the device alignment after
        // every flush so subsequent flushes are pure aligned appends.
        // Each flush therefore writes its entries followed by zero
        // padding up to the next alignment boundary. If the post-restart
        // scan stops at the first `length=0` word it treats the pad of
        // the first flush as end-of-log and loses every entry written in
        // later flushes within the same checkpoint epoch.
        //
        // Two separate flushes, no checkpoint in between, then reopen:
        // both entries MUST survive.
        let (dev, mut log) = make_log(1024 * 1024);

        log.append(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        log.flush().unwrap();

        log.append(RedoOp::Freeze {
            tx_key: test_key(2),
            offset: 1,
        })
        .unwrap();
        log.flush().unwrap();

        drop(log);

        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert_eq!(
            entries.len(),
            2,
            "both entries must survive the alignment-pad gap between flushes",
        );
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
    }

    #[test]
    fn pad_gap_followed_by_adjacent_sequence_does_not_lose_entries() {
        // External-review P0 boundary case for the F-G4-004 pad-gap rule.
        //
        // Hand-craft the entries region so the byte layout is exactly:
        //
        //   [entry seq=1 (Freeze, 53 bytes)][zeros to next align boundary]
        //   [entry seq=2 (Freeze, 53 bytes)][trailing zeros]
        //
        // The pre-gap entry has sequence N and the post-gap entry has the
        // genuinely-adjacent sequence N+1. The QA hazard: a scanner that
        // either (a) stops at the first length=0 pad word, or (b) over-
        // eagerly classifies the N+1 entry as "ambiguous stale data past
        // the pad" and drops it, would silently lose the second entry —
        // exactly the data-loss path flagged by review.
        //
        // Contract: scanner returns BOTH entries. `prev=1`, `entry.seq=2`,
        // `prev.checked_add(1) == Some(2)` so the `just_skipped_pad`
        // softening branch is NOT taken; the entry is accepted normally.
        let (dev, log) = make_log(1024 * 1024);

        let first_op = RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        };
        let second_op = RedoOp::Freeze {
            tx_key: test_key(2),
            offset: 1,
        };
        // Use the real serializer so the on-disk bytes (CRC, length,
        // sequence, op type, payload) match exactly what the scanner
        // expects to deserialize.
        let first_bytes = RedoEntry {
            sequence: 1,
            op: first_op.clone(),
        }
        .serialize();
        let second_bytes = RedoEntry {
            sequence: 2,
            op: second_op.clone(),
        }
        .serialize();

        // Drop the runtime log so its in-memory entries_cache cannot mask
        // a scan regression — recovery must come exclusively from the
        // device.
        drop(log);

        // The entries region starts at offset = device alignment
        // (the F-G4-001 header block claims the first alignment unit).
        let align = dev.alignment();
        let entries_region_offset = align as u64;

        // Layout the two entries across two alignment units so the
        // pad gap between them is the structure under test.
        let mut block0 = AlignedBuf::new(align, align);
        block0[..first_bytes.len()].copy_from_slice(&first_bytes);
        // Bytes from first_bytes.len()..align stay zero — this is the
        // pad gap.
        dev.pwrite(&block0, entries_region_offset).unwrap();

        let mut block1 = AlignedBuf::new(align, align);
        block1[..second_bytes.len()].copy_from_slice(&second_bytes);
        // Trailing bytes after second_bytes stay zero — that pad ends at
        // an aligned boundary and acts as the true end-of-log sentinel.
        dev.pwrite(&block1, entries_region_offset + align as u64)
            .unwrap();

        // Re-open against the modified device and recover.
        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();

        assert_eq!(
            entries.len(),
            2,
            "pad-gap scanner must return BOTH entries when the post-gap \
             sequence is genuinely adjacent (N+1); dropping the second \
             entry is silent data loss",
        );
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[0].op, first_op);
        assert_eq!(entries[1].op, second_op);
    }

    #[test]
    fn pad_gap_followed_by_truncated_entry_terminates_cleanly() {
        // External-review P0 boundary case for the F-G4-004 pad-gap rule.
        //
        // Layout:
        //
        //   [entry seq=1 (Freeze, 53 bytes)][zeros to next align boundary]
        //   [4-byte length header claiming a huge body][no body bytes]
        //
        // The post-gap "entry" has a valid-looking nonzero length word but
        // its declared body extends past every following byte in the log
        // region — every subsequent chunk read keeps producing the same
        // partial-entry state. The QA hazard: a scanner that loops trying
        // to assemble the entry could hang indefinitely (each iteration
        // makes no forward progress past the truncated header).
        //
        // Contract: scanner returns the pre-gap entry only, and the call
        // returns synchronously (well under 100 ms on any reasonable host)
        // — no infinite loop. We enforce the timing in-process via a
        // watchdog thread so a regression cannot hang CI.
        let (dev, log) = make_log(1024 * 1024);

        let first_op = RedoOp::Freeze {
            tx_key: test_key(7),
            offset: 0,
        };
        let first_bytes = RedoEntry {
            sequence: 1,
            op: first_op.clone(),
        }
        .serialize();

        drop(log);

        let align = dev.alignment();
        let entries_region_offset = align as u64;

        // Block 0: pre-gap entry + zero pad to next align boundary.
        let mut block0 = AlignedBuf::new(align, align);
        block0[..first_bytes.len()].copy_from_slice(&first_bytes);
        dev.pwrite(&block0, entries_region_offset).unwrap();

        // Block 1: a 4-byte length header whose value is far larger than
        // anything that follows. The deserializer returns None on the
        // "data.len() < total" check; the scanner then sees `lw != 0`
        // and stashes the bytes into `carry`. As `scan_start` advances
        // through the rest of the (zero-filled) log region, carry keeps
        // accumulating zeros that never satisfy the CRC, until the loop
        // exits cleanly at `scan_start >= total_to_read`.
        //
        // Pick a length that is plausibly an entry (passes the
        // `length >= ENTRY_OVERHEAD` sanity check inside `deserialize`)
        // so the truncation path — not the structural-rejection path —
        // is exercised. ENTRY_OVERHEAD = 13; we use a value well above
        // that.
        let mut block1 = AlignedBuf::new(align, align);
        let truncated_len: u32 = 1_000_000; // far larger than remaining bytes
        block1[..4].copy_from_slice(&truncated_len.to_le_bytes());
        // Leave bytes 4..align zero so they cannot accidentally form a
        // valid entry under any interpretation.
        dev.pwrite(&block1, entries_region_offset + align as u64)
            .unwrap();

        // Wrap the scan in a watchdog: spawn a background thread that
        // panics the test if the scan does not complete within 100 ms.
        // The scan itself is single-threaded and synchronous, so a true
        // infinite-loop regression would deadlock without this guard.
        let dev_for_scan: Arc<dyn BlockDevice> = dev;
        let (tx, rx) = std::sync::mpsc::channel::<Vec<RedoEntry>>();
        let handle = std::thread::spawn(move || {
            let log2 = RedoLog::open(dev_for_scan, 0, 1024 * 1024).unwrap();
            let entries = log2.recover().unwrap();
            // If the channel send fails the parent already gave up on us;
            // that is the only acceptable "ignore" because the test has
            // already declared failure.
            let _ = tx.send(entries);
        });

        let entries = match rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(entries) => entries,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!(
                    "pad-gap scanner did not terminate within 100 ms on a \
                     truncated post-gap entry — likely an infinite loop in \
                     the scan/carry path",
                );
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!(
                    "scan worker disconnected without producing a result \
                     — recover() likely panicked",
                );
            }
        };
        // Join the worker so a successful scan still cleans up the
        // thread before the test exits. The thread has already sent, so
        // join completes immediately.
        handle.join().expect("scan worker panicked");

        assert_eq!(
            entries.len(),
            1,
            "pad-gap scanner must return the pre-gap entry and stop when \
             the post-gap entry is truncated (cannot satisfy its declared \
             length / CRC)",
        );
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[0].op, first_op);
    }

    #[test]
    fn redo_repeated_reads_use_entry_cache_after_open() {
        let dev = Arc::new(ReadFailingDevice::new(1024 * 1024));
        let dev_trait: Arc<dyn BlockDevice> = dev.clone();
        let mut log = RedoLog::open(dev_trait, 0, 1024 * 1024).unwrap();
        let op = RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        };
        log.append_and_flush(op.clone()).unwrap();

        // If recover/read_from_sequence/earliest_sequence rescan the device,
        // this flag makes them fail. They should use the in-memory entry
        // cache populated by open() plus successful flushes.
        dev.fail_reads();

        assert_eq!(log.earliest_sequence().unwrap(), Some(1));
        assert_eq!(log.read_from_sequence(1).unwrap()[0].op, op);
        assert_eq!(log.recover().unwrap().len(), 1);
    }

    #[test]
    fn reopen_after_checkpoint() {
        let (dev, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        log.mark_checkpoint().unwrap();
        log.append_and_flush(RedoOp::Freeze {
            tx_key: test_key(2),
            offset: 1,
        })
        .unwrap();
        drop(log);

        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert_eq!(entries.len(), 1); // Only entry after checkpoint
    }

    #[test]
    fn truncated_entry_stops_recovery() {
        let (dev, mut log) = make_log(1024 * 1024);
        // F-G4-004: single flush so the entries are contiguous on disk;
        // the corruption we add below is what should stop the scan, not
        // an alignment pad.
        log.append(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        log.append(RedoOp::Freeze {
            tx_key: test_key(2),
            offset: 1,
        })
        .unwrap();
        log.flush().unwrap();

        // Simulate truncation: rewrite the second entry's payload area
        // to a non-zero garbage byte so its CRC fails. Writing zeros
        // here would (correctly, under the pad-gap rule) be treated as
        // pad — we want the test to exercise the CRC-failure stop, so
        // use a non-zero pattern.
        let align = dev.alignment();
        let entries_region_offset = align as u64;
        let mut buf = AlignedBuf::new(align, align);
        dev.pread(&mut buf, entries_region_offset).unwrap();
        // First Freeze entry is 53 bytes; corrupt the second entry's
        // payload bytes 70..100 with a non-zero pattern.
        for b in buf[70..100].iter_mut() {
            *b = 0xAA;
        }
        dev.pwrite(&buf, entries_region_offset).unwrap();

        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        // Should get at most the first entry (second is truncated /
        // corrupt).
        assert!(entries.len() <= 1);
    }

    #[test]
    fn open_resumes_append_after_last_valid_entry_when_final_entry_is_partial() {
        let (dev, mut log) = make_log(1024 * 1024);
        let first = RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        };
        let second = RedoOp::Freeze {
            tx_key: test_key(2),
            offset: 1,
        };
        let third = RedoOp::Freeze {
            tx_key: test_key(3),
            offset: 2,
        };

        // F-G4-004: append both entries before a single flush so they
        // sit contiguously on disk. Otherwise each flush would block-
        // align write_pos and the trailing zero-padding would stop the
        // scan before reaching the second entry.
        log.append(first.clone()).unwrap();
        let first_tail = log.write_position();
        log.append(second).unwrap();
        log.flush().unwrap();
        drop(log);

        // The entries region starts at offset = device alignment
        // (F-G4-001). Corrupt the second entry by flipping a byte well
        // past the first entry's tail.
        let align = dev.alignment();
        let entries_region_offset = align as u64;
        let mut buf = AlignedBuf::new(align, align);
        dev.pread(&mut buf, entries_region_offset).unwrap();
        buf[first_tail as usize + 20] ^= 0xFF;
        dev.pwrite(&buf, entries_region_offset).unwrap();

        let mut reopened = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
        assert_eq!(
            reopened.write_position(),
            first_tail,
            "open must resume after the last fully valid entry",
        );
        // F-G4-001 persists `next_sequence` in the header on every
        // flush, so after a corrupt-tail recovery the next sequence
        // continues from the high-water mark (3) rather than reusing
        // the corrupted entry's sequence (2). The corrupted slot is
        // effectively burned to keep replication watermarks monotonic.
        assert_eq!(reopened.current_sequence(), 3);

        reopened.append_and_flush(third.clone()).unwrap();
        let entries = reopened.recover().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].op, first);
        assert_eq!(entries[1].op, third);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 3);
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
            log.mark_checkpoint().unwrap();

            // Verify only entries after the most recent checkpoint are returned
            let entries = log.recover().unwrap();
            assert!(
                entries.is_empty(),
                "cycle {cycle}: should have 0 entries after checkpoint"
            );
        }

        // After all cycles, total space used should not leak
        assert!(log.available_space() > 0);
    }

    #[test]
    fn zero_length_marks_end_of_valid_data() {
        let (dev, mut log) = make_log(1024 * 1024);
        log.append_and_flush(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();

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
        // Write 10 entries in a single flush so they sit contiguously
        // in the entries region (F-G4-004 block-aligns each flush).
        // Then for each corruption point we copy the first two blocks
        // (header + first entries block) to a fresh device and flip one
        // byte inside the entries block. Recovery must never panic or
        // error — it returns whatever prefix scanned cleanly.
        let (dev, mut log) = make_log(1024 * 1024);
        for i in 0..10u8 {
            log.append(RedoOp::Freeze {
                tx_key: test_key(i),
                offset: i as u32,
            })
            .unwrap();
        }
        log.flush().unwrap();

        let align = dev.alignment();
        // 10 Freeze entries × 53 bytes = 530 bytes, well within one
        // 4 KiB block. Corrupt offsets cover that range (relative to
        // the entries region, i.e. starting after the header block).
        for entries_offset in (10..500).step_by(10) {
            let dev2 = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
            // Copy the header block + first entries block from dev.
            let mut header_buf = AlignedBuf::new(align, align);
            dev.pread(&mut header_buf, 0).unwrap();
            dev2.pwrite(&header_buf, 0).unwrap();
            let mut entries_buf = AlignedBuf::new(align, align);
            dev.pread(&mut entries_buf, align as u64).unwrap();
            dev2.pwrite(&entries_buf, align as u64).unwrap();

            // Flip a byte inside the entries block.
            let mut buf2 = AlignedBuf::new(align, align);
            dev2.pread(&mut buf2, align as u64).unwrap();
            if entries_offset < buf2.len() {
                buf2[entries_offset] ^= 0xFF;
                dev2.pwrite(&buf2, align as u64).unwrap();
            }

            // Recovery should not panic or error.
            let log2 = RedoLog::open(dev2, 0, 1024 * 1024).unwrap();
            let result = log2.recover();
            assert!(
                result.is_ok(),
                "recovery failed at entries-region corruption offset {entries_offset}"
            );
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
        assert_eq!(
            entries[0].op, op,
            "round-tripped op does not match original"
        );
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
    fn round_trip_spend_v2() {
        // V2 (no hash) round-trips with utxo_hash = None.
        assert_round_trip(RedoOp::SpendV2 {
            tx_key: make_txid(0xA2),
            offset: 42,
            spending_data: [0x5A; 36],
            new_spent_count: 17,
            current_block_height: 800_000,
            block_height_retention: 288,
            target_generation: 12,
            updated_at: 123_456_789,
            utxo_hash: None,
        });
        // B-5: V3 (with hash) round-trips preserving the 32-byte hash.
        assert_round_trip(RedoOp::SpendV2 {
            tx_key: make_txid(0xA2),
            offset: 42,
            spending_data: [0x5A; 36],
            new_spent_count: 17,
            current_block_height: 800_000,
            block_height_retention: 288,
            target_generation: 12,
            updated_at: 123_456_789,
            utxo_hash: Some([0x7C; 32]),
        });
    }

    #[test]
    fn round_trip_unspend() {
        assert_round_trip(RedoOp::Unspend {
            tx_key: make_txid(0xB2),
            offset: 99,
            spending_data: Some([0xBC; 36]),
            new_spent_count: 3,
        });
    }

    #[test]
    fn round_trip_unspend_v2() {
        assert_round_trip(RedoOp::UnspendV2 {
            tx_key: make_txid(0xB3),
            offset: 99,
            spending_data: [0xBC; 36],
            new_spent_count: 3,
            current_block_height: 800_100,
            block_height_retention: 144,
            target_generation: 13,
            updated_at: 987_654_321,
            utxo_hash: None,
        });
        // B-5: V3 unspend round-trips with the hash.
        assert_round_trip(RedoOp::UnspendV2 {
            tx_key: make_txid(0xB3),
            offset: 99,
            spending_data: [0xBC; 36],
            new_spent_count: 3,
            current_block_height: 800_100,
            block_height_retention: 144,
            target_generation: 13,
            updated_at: 987_654_321,
            utxo_hash: Some([0x3D; 32]),
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
    fn round_trip_freeze_v2() {
        assert_round_trip(RedoOp::FreezeV2 {
            tx_key: make_txid(0xD6),
            offset: 8,
            utxo_hash: [0xAB; 32],
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
    fn round_trip_unfreeze_v2() {
        assert_round_trip(RedoOp::UnfreezeV2 {
            tx_key: make_txid(0xE7),
            offset: 256,
            utxo_hash: [0xCD; 32],
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
    fn round_trip_reassign_v2() {
        let mut new_hash = [0u8; 32];
        for (i, b) in new_hash.iter_mut().enumerate() {
            *b = 0xFF_u8.wrapping_sub(i as u8);
        }
        let mut prior_utxo_hash = [0u8; 32];
        for (i, b) in prior_utxo_hash.iter_mut().enumerate() {
            *b = 0x10_u8.wrapping_add(i as u8);
        }
        assert_round_trip(RedoOp::ReassignV2 {
            tx_key: make_txid(0xF8),
            offset: 11,
            new_hash,
            block_height: 2_000_000,
            spendable_after: 750,
            prior_utxo_hash,
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

    /// Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): the new
    /// full-payload create entry must round-trip through serialize +
    /// deserialize with all fields intact, including variable-length
    /// `record_bytes` and `parent_txids`. Tests three scenarios:
    /// non-conflicting + no parents, conflicting with parents, and a
    /// large record (10k bytes) to exercise the length encoding.
    #[test]
    fn round_trip_create_v2_minimal() {
        assert_round_trip(RedoOp::CreateV2 {
            tx_key: make_txid(0x90),
            record_offset: 0x1000,
            utxo_count: 1,
            is_conflicting: false,
            record_bytes: vec![0xAB; 200],
            parent_txids: Vec::new(),
        });
    }

    #[test]
    fn round_trip_create_v2_with_conflicting_parents() {
        let parents: Vec<[u8; 32]> = (0..5u8).map(make_txid).map(|k| k.txid).collect();
        assert_round_trip(RedoOp::CreateV2 {
            tx_key: make_txid(0x91),
            record_offset: 0x2000,
            utxo_count: 4,
            is_conflicting: true,
            record_bytes: vec![0xCD; 512],
            parent_txids: parents,
        });
    }

    #[test]
    fn round_trip_create_v2_large_record() {
        // 10 kB record bytes — exercises the 4-byte record_len field.
        let big = (0..10_000u32).map(|i| i as u8).collect::<Vec<u8>>();
        assert_round_trip(RedoOp::CreateV2 {
            tx_key: make_txid(0x92),
            record_offset: 0x3000_0000,
            utxo_count: 1024,
            is_conflicting: true,
            record_bytes: big,
            parent_txids: vec![[0xEE; 32]; 3],
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
    fn append_conflicting_child_redo_round_trip() {
        assert_round_trip(RedoOp::AppendConflictingChild {
            parent_key: make_txid(0x3D),
            child_txid: make_txid(0x3E).txid,
        });
    }

    #[test]
    fn remove_conflicting_child_redo_round_trip() {
        assert_round_trip(RedoOp::RemoveConflictingChild {
            parent_key: make_txid(0x5D),
            child_txid: make_txid(0x5E).txid,
        });
    }

    #[test]
    fn append_deleted_child_redo_round_trip() {
        assert_round_trip(RedoOp::AppendDeletedChild {
            parent_key: make_txid(0x4D),
            child_txid: make_txid(0x4E).txid,
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
            generation: 42,
        });
    }

    #[test]
    fn round_trip_mark_on_longest_chain_false() {
        assert_round_trip(RedoOp::MarkOnLongestChain {
            tx_key: make_txid(0x6F),
            on_longest_chain: false,
            current_block_height: 1,
            block_height_retention: 0,
            generation: 0,
        });
    }

    #[test]
    fn round_trip_secondary_unmined_update() {
        assert_round_trip(RedoOp::SecondaryUnminedUpdate {
            tx_key: make_txid(0x70),
            old_height: 0,
            new_height: 500,
        });
        assert_round_trip(RedoOp::SecondaryUnminedUpdate {
            tx_key: make_txid(0x71),
            old_height: 500,
            new_height: 0,
        });
        assert_round_trip(RedoOp::SecondaryUnminedUpdate {
            tx_key: make_txid(0x72),
            old_height: 100,
            new_height: 200,
        });
    }

    #[test]
    fn round_trip_secondary_dah_update() {
        assert_round_trip(RedoOp::SecondaryDahUpdate {
            tx_key: make_txid(0x73),
            old_height: 0,
            new_height: 900,
        });
        assert_round_trip(RedoOp::SecondaryDahUpdate {
            tx_key: make_txid(0x74),
            old_height: 900,
            new_height: 0,
        });
    }

    /// Gap #8 (TERANODE_PRODUCTION_READINESS_GAPS.md): compensation
    /// intent variants must round-trip every captured before-image bit
    /// so a crash-mid-rollback recovery can restore the original state
    /// exactly. Cover edge cases: max-value u32 fields, non-zero block
    /// height/subtree, all status-byte values that PruneSlot may have
    /// overwritten.
    #[test]
    fn round_trip_compensate_unset_mined() {
        assert_round_trip(RedoOp::CompensateUnsetMined {
            tx_key: make_txid(0x80),
            block_id: 12345,
            block_height: 800_000,
            subtree_idx: 3,
        });
        // Boundary: u32 max for each numeric field.
        assert_round_trip(RedoOp::CompensateUnsetMined {
            tx_key: make_txid(0x81),
            block_id: u32::MAX,
            block_height: u32::MAX,
            subtree_idx: u32::MAX,
        });
        // Zero values are valid in their own right (low block heights).
        assert_round_trip(RedoOp::CompensateUnsetMined {
            tx_key: make_txid(0x82),
            block_id: 0,
            block_height: 0,
            subtree_idx: 0,
        });
    }

    #[test]
    fn round_trip_compensate_reassign() {
        let mut prior = [0u8; 32];
        for (i, b) in prior.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(11);
        }
        assert_round_trip(RedoOp::CompensateReassign {
            tx_key: make_txid(0x83),
            offset: 7,
            prior_utxo_hash: prior,
        });
        // Distinct hash, different offset.
        assert_round_trip(RedoOp::CompensateReassign {
            tx_key: make_txid(0x84),
            offset: 0,
            prior_utxo_hash: [0xAA; 32],
        });
        // All-zero prior hash is a legitimate edge case (the slot was
        // historically zero-hashed before reassign).
        assert_round_trip(RedoOp::CompensateReassign {
            tx_key: make_txid(0x85),
            offset: u32::MAX,
            prior_utxo_hash: [0u8; 32],
        });
    }

    #[test]
    fn round_trip_compensate_prune() {
        // Cover every status-byte value the prune path could be reversing.
        for status in &[
            crate::record::UTXO_UNSPENT,
            crate::record::UTXO_SPENT,
            crate::record::UTXO_FROZEN,
            crate::record::UTXO_PRUNED,
            0xFFu8, // sentinel: unknown/future status — must round-trip too.
        ] {
            assert_round_trip(RedoOp::CompensatePrune {
                tx_key: make_txid(*status),
                offset: 13,
                prior_status: *status,
            });
        }
    }

    #[test]
    fn round_trip_compensate_set_locked() {
        assert_round_trip(RedoOp::CompensateSetLocked {
            tx_key: make_txid(0x86),
            prior_locked: false,
            prior_delete_at_height: 1288,
        });
        assert_round_trip(RedoOp::CompensateSetLocked {
            tx_key: make_txid(0x87),
            prior_locked: true,
            prior_delete_at_height: 0,
        });
        assert_round_trip(RedoOp::CompensateSetLocked {
            tx_key: make_txid(0x88),
            prior_locked: false,
            prior_delete_at_height: u32::MAX,
        });
    }

    #[test]
    fn append_batch_and_flush_assigns_contiguous_sequences() {
        let (_, mut log) = make_log(1024 * 1024);
        let ops = vec![
            RedoOp::SecondaryDahUpdate {
                tx_key: test_key(1),
                old_height: 0,
                new_height: 100,
            },
            RedoOp::SecondaryUnminedUpdate {
                tx_key: test_key(1),
                old_height: 0,
                new_height: 500,
            },
        ];
        let (first, last) = log.append_batch_and_flush(&ops).unwrap();
        assert_eq!(first, 1);
        assert_eq!(last, 2);

        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[0].op, ops[0]);
        assert_eq!(entries[1].op, ops[1]);
    }

    #[test]
    fn append_batch_and_flush_empty_no_flush() {
        let (_, mut log) = make_log(1024 * 1024);
        let (first, last) = log.append_batch_and_flush(&[]).unwrap();
        assert_eq!((first, last), (0, 0));
        let entries = log.recover().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn round_trip_allocate_region() {
        assert_round_trip(RedoOp::AllocateRegion {
            offset: 0x0000_1234_5678_9ABC,
            size: 4096,
            device_id: 0,
        });
        assert_round_trip(RedoOp::AllocateRegion {
            offset: 1024 * 1024,
            size: 16 * 1024,
            device_id: 7,
        });
    }

    #[test]
    fn round_trip_free_region() {
        assert_round_trip(RedoOp::FreeRegion {
            offset: 0x0000_DEAD_BEEF_0000,
            size: 8192,
            device_id: 0,
        });
        assert_round_trip(RedoOp::FreeRegion {
            offset: 2 * 1024 * 1024,
            size: 32 * 1024,
            device_id: 3,
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

    #[test]
    fn recovery_progress_tracking() {
        let (_, mut log) = make_log(1024 * 1024);
        let first = log
            .append_and_flush(RedoOp::Freeze {
                tx_key: test_key(1),
                offset: 0,
            })
            .unwrap();
        log.mark_recovery_progress(first).unwrap();
        let second = log
            .append_and_flush(RedoOp::Unfreeze {
                tx_key: test_key(1),
                offset: 0,
            })
            .unwrap();

        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, second);
        assert!(matches!(entries[0].op, RedoOp::Unfreeze { .. }));
    }

    #[test]
    fn compact_prefix_through_preserves_post_fence_entries() {
        let (dev, mut log) = make_log(1024 * 1024);
        let first = log
            .append_and_flush(RedoOp::Freeze {
                tx_key: test_key(1),
                offset: 0,
            })
            .unwrap();
        let second = log
            .append_and_flush(RedoOp::Unfreeze {
                tx_key: test_key(1),
                offset: 0,
            })
            .unwrap();
        let third = log
            .append_and_flush(RedoOp::Freeze {
                tx_key: test_key(2),
                offset: 0,
            })
            .unwrap();

        log.mark_recovery_progress(second).unwrap();
        log.compact_prefix_through(second).unwrap();

        let entries = log.recover().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, third);
        assert!(matches!(entries[0].op, RedoOp::Freeze { .. }));
        // B-3: compaction with a non-empty retained set no longer
        // rewrites the retained payload to the front (that in-place
        // overwrite was torn-write-unsafe). Instead it relocates a fresh
        // copy past the live tail and advances `logical_start`. The live
        // window therefore begins at a non-zero `logical_start`, and the
        // single retained entry plus its zero sentinel occupy at most one
        // aligned block beyond it. Recovery sees exactly the retained
        // entry regardless.
        assert!(log.logical_start > 0, "retained set relocated past tail");
        assert!(
            log.write_position() - log.logical_start <= 4096,
            "relocated retained payload fits one aligned block",
        );
        assert!(first < second && second < third);

        drop(log);
        let reopened = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = reopened.recover().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, third);
        assert!(matches!(entries[0].op, RedoOp::Freeze { .. }));
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
                generation: 7,
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
        assert_eq!(
            entries.len(),
            5,
            "expected 5 recovered entries after reopen"
        );
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(
                entry.sequence,
                (i + 1) as u64,
                "sequence mismatch at index {i}"
            );
            assert_eq!(entry.op, ops[i], "op mismatch at index {i}");
        }
    }

    // -----------------------------------------------------------------------
    // Log full test: fill until LogFull error
    // -----------------------------------------------------------------------

    #[test]
    fn log_full_error_not_panic() {
        // F-G4-001 reserves one alignment block for the header, so the
        // entries-region capacity is `log_size - alignment`. Use an
        // 8 KiB log: header takes 4 KiB, entries capacity is 4 KiB.
        let (_, mut log) = make_log(8192);
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
                    assert_eq!(
                        capacity, 4096,
                        "capacity should match entries-region size (log - header)"
                    );
                    break;
                }
                Err(e) => panic!("expected LogFull, got: {e}"),
            }
        }
        assert!(
            appended > 0,
            "should have appended at least one entry before LogFull"
        );
    }

    #[test]
    fn redo_append_failure_sequence_gap() {
        // F-G4-001: the header block claims the first alignment unit, so
        // a 4 KiB log holds no entries. Use 8 KiB (4 KiB header + 4 KiB
        // entries) — still small enough to fill quickly.
        let (_, mut log) = make_log(8192);
        loop {
            match log.append(RedoOp::Freeze {
                tx_key: test_key(1),
                offset: 1,
            }) {
                Ok(_) => {}
                Err(RedoError::LogFull { .. }) => break,
                Err(e) => panic!("unexpected error: {e:?}"),
            }
        }

        let next_after_first_failure = log.next_sequence;
        let second = log.append(RedoOp::Freeze {
            tx_key: test_key(2),
            offset: 2,
        });
        assert!(matches!(second, Err(RedoError::LogFull { .. })));
        assert_eq!(
            log.next_sequence, next_after_first_failure,
            "failed append must not consume a sequence number"
        );
    }

    // -----------------------------------------------------------------------
    // Corrupted entry recovery: entries before corruption are returned
    // -----------------------------------------------------------------------

    #[test]
    fn corrupted_entry_recovery_returns_entries_before_corruption() {
        let (dev, mut log) = make_log(1024 * 1024);

        // F-G4-004: append all entries before a single flush so they
        // sit contiguously in one entries block. Otherwise the scan
        // would stop at the zero-padded gap after the first entry.
        let ops: Vec<RedoOp> = (0..5u8)
            .map(|i| RedoOp::Freeze {
                tx_key: make_txid(i),
                offset: i as u32,
            })
            .collect();
        for op in &ops {
            log.append(op.clone()).unwrap();
        }
        log.flush().unwrap();

        // Each Freeze entry is: 4 (length) + 8 (seq) + 1 (type) + 32
        // (txid) + 4 (offset) + 4 (crc) = 53 bytes. Corrupt a byte in
        // the middle of the third entry.
        let entry_size = 53usize;
        let corrupt_target = entry_size * 2 + 10; // middle of third entry

        // The entries region starts at offset = device alignment
        // (F-G4-001 header block claims the first alignment unit).
        let align = dev.alignment();
        let entries_region_offset = align as u64;
        let mut buf = AlignedBuf::new(align, align);
        dev.pread(&mut buf, entries_region_offset).unwrap();
        buf[corrupt_target] ^= 0xFF;
        dev.pwrite(&buf, entries_region_offset).unwrap();

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

        // F-G4-004: append everything (pre-ops + Checkpoint + post-ops)
        // before a single flush so the entries sit contiguously on disk
        // and the post-restart scan can read past the Checkpoint marker.
        // (`mark_checkpoint` would call `flush` after appending the
        // Checkpoint, which under F-G4-004 block-aligns write_pos and
        // leaves a zero-padded gap that stops a later scan early.)
        let pre_ops = vec![
            RedoOp::Freeze {
                tx_key: make_txid(0x10),
                offset: 0,
            },
            RedoOp::Unfreeze {
                tx_key: make_txid(0x11),
                offset: 1,
            },
            RedoOp::PruneSlot {
                tx_key: make_txid(0x12),
                offset: 2,
            },
        ];
        let post_ops = vec![
            RedoOp::SetLocked {
                tx_key: make_txid(0x20),
                value: true,
            },
            RedoOp::PreserveUntil {
                tx_key: make_txid(0x21),
                block_height: 12345,
            },
        ];
        for op in &pre_ops {
            log.append(op.clone()).unwrap();
        }
        log.append(RedoOp::Checkpoint).unwrap();
        for op in &post_ops {
            log.append(op.clone()).unwrap();
        }
        log.flush().unwrap();
        drop(log);

        // Reopen and recover — only post-checkpoint ops should appear.
        // recover() walks the scanned entries, sets start_idx to the
        // position after the last Checkpoint, and returns the tail.
        let log2 = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
        let entries = log2.recover().unwrap();
        assert_eq!(entries.len(), 2, "expected 2 post-checkpoint entries");
        assert_eq!(
            entries[0].op, post_ops[0],
            "first post-checkpoint op mismatch"
        );
        assert_eq!(
            entries[1].op, post_ops[1],
            "second post-checkpoint op mismatch"
        );
    }

    /// Phase 5: appending 100 entries and flushing records non-zero
    /// flush latency, bumps the append counter, and sets the bytes/entries
    /// distribution buckets.
    #[test]
    fn redo_flush_records_latency_and_bytes() {
        use crate::metrics::{RedoMetrics, init_redo_metrics, redo_metrics};
        use std::sync::OnceLock;

        static TEST_METRICS: OnceLock<RedoMetrics> = OnceLock::new();
        let m_ref: &'static RedoMetrics = TEST_METRICS.get_or_init(RedoMetrics::new);
        init_redo_metrics(m_ref);
        let metrics = redo_metrics().expect("metrics installed");
        let before_flush_count = metrics.redo_flush_latency_ns.count();
        let before_append = metrics.redo_append_total.get();
        let before_bytes_count = metrics.redo_bytes_per_flush.count();
        let before_entries_count = metrics.redo_entries_per_flush.count();

        let (_, mut log) = make_log(1024 * 1024);
        for i in 0..100u8 {
            log.append(RedoOp::Freeze {
                tx_key: make_txid(i),
                offset: i as u32,
            })
            .unwrap();
        }
        log.flush().unwrap();

        // Exactly one flush call. flush_latency count must advance by at
        // least 1 (≥ to be robust to other parallel flushes in the process).
        assert!(
            metrics.redo_flush_latency_ns.count() > before_flush_count,
            "redo_flush_latency_ns.count() should advance",
        );
        // 100 appends -> append counter advances by at least 100.
        assert!(
            metrics.redo_append_total.get() - before_append >= 100,
            "redo_append_total should advance by at least 100",
        );
        // bytes/entries histograms should have recorded exactly 1 datum
        // per flush call — assert that this flush contributed.
        assert!(
            metrics.redo_bytes_per_flush.count() > before_bytes_count,
            "redo_bytes_per_flush.count() should advance",
        );
        assert!(
            metrics.redo_entries_per_flush.count() > before_entries_count,
            "redo_entries_per_flush.count() should advance",
        );
    }

    // -- B-3: crash-safe compaction --------------------------------------

    /// Block device with a durable shadow and a "crash on the Nth sync"
    /// trigger. `sync()` copies live → shadow (durable). `crash()` (and
    /// the auto-crash on the configured sync index) copies shadow → live,
    /// modeling a power loss that drops every write issued since the last
    /// durable sync. Unlike `MemoryDevice::new_volatile` this lets a test
    /// pinpoint the crash to a specific sync boundary *inside*
    /// `compact_prefix_through` (between the relocated-copy fsync and the
    /// header-flip fsync).
    struct CrashCowDevice {
        live: parking_lot::Mutex<Vec<u8>>,
        shadow: parking_lot::Mutex<Vec<u8>>,
        alignment: usize,
        sync_count: std::sync::atomic::AtomicU64,
        /// Crash automatically when the sync counter reaches this value
        /// (1-based). 0 disables auto-crash.
        crash_at_sync: std::sync::atomic::AtomicU64,
        crashed: AtomicBool,
    }

    impl CrashCowDevice {
        fn new(size: usize, alignment: usize) -> Self {
            Self {
                live: parking_lot::Mutex::new(vec![0u8; size]),
                shadow: parking_lot::Mutex::new(vec![0u8; size]),
                alignment,
                sync_count: std::sync::atomic::AtomicU64::new(0),
                crash_at_sync: std::sync::atomic::AtomicU64::new(0),
                crashed: AtomicBool::new(false),
            }
        }

        fn arm_crash_at_sync(&self, n: u64) {
            self.crash_at_sync.store(n, Ordering::SeqCst);
        }

        fn crash(&self) {
            let shadow = self.shadow.lock();
            let mut live = self.live.lock();
            live.copy_from_slice(&shadow);
            self.crashed.store(true, Ordering::SeqCst);
        }

        fn crashed(&self) -> bool {
            self.crashed.load(Ordering::SeqCst)
        }
    }

    impl BlockDevice for CrashCowDevice {
        fn pread(&self, buf: &mut [u8], offset: u64) -> crate::device::Result<usize> {
            let live = self.live.lock();
            let start = offset as usize;
            let end = start + buf.len();
            if end > live.len() {
                return Err(DeviceError::Io(std::io::Error::other("oob pread")));
            }
            buf.copy_from_slice(&live[start..end]);
            Ok(buf.len())
        }

        fn pwrite(&self, buf: &[u8], offset: u64) -> crate::device::Result<usize> {
            if self.crashed.load(Ordering::SeqCst) {
                return Err(DeviceError::Io(std::io::Error::other("post-crash write")));
            }
            let mut live = self.live.lock();
            let start = offset as usize;
            let end = start + buf.len();
            if end > live.len() {
                return Err(DeviceError::Io(std::io::Error::other("oob pwrite")));
            }
            live[start..end].copy_from_slice(buf);
            Ok(buf.len())
        }

        fn alignment(&self) -> usize {
            self.alignment
        }

        fn size(&self) -> u64 {
            self.live.lock().len() as u64
        }

        fn sync(&self) -> crate::device::Result<()> {
            let n = self.sync_count.fetch_add(1, Ordering::SeqCst) + 1;
            let target = self.crash_at_sync.load(Ordering::SeqCst);
            if target != 0 && n == target {
                // Power loss strikes exactly at this sync: writes issued
                // since the previous durable sync are dropped. The sync
                // itself does NOT make them durable.
                self.crash();
                return Err(DeviceError::Io(std::io::Error::other("power loss at sync")));
            }
            // Normal durable sync: live becomes the durable shadow.
            let live = self.live.lock();
            let mut shadow = self.shadow.lock();
            shadow.copy_from_slice(&live);
            Ok(())
        }

        // Force the redo log onto the pread/pwrite path (no zero-copy
        // alias) so the crash/shadow model governs every byte.
        fn as_raw_ptr(&self) -> Option<*mut u8> {
            None
        }
    }

    /// B-3: a torn (partial) write of the retained set during the OLD
    /// in-place compaction strategy silently loses retained entries. This
    /// test reproduces the historical hazard directly on the device to
    /// document why the in-place rewrite was unsafe.
    #[test]
    fn old_inplace_compaction_torn_write_loses_retained_entries() {
        let align = 4096usize;
        let size = 256 * 1024usize;
        let dev = Arc::new(CrashCowDevice::new(size, align));
        let dyn_dev: Arc<dyn BlockDevice> = dev.clone();

        // Lay down 200 entries in a single flush so the retained set
        // spans several aligned blocks without per-entry block padding.
        let mut log = RedoLog::open(dyn_dev.clone(), 0, size as u64).unwrap();
        for i in 0..200u32 {
            log.append(RedoOp::Freeze {
                tx_key: test_key((i & 0xff) as u8),
                offset: i,
            })
            .unwrap();
        }
        log.flush().unwrap();
        drop(log);

        // Simulate the OLD strategy: serialize the retained set
        // (seq 51..=200) and rewrite it IN PLACE at the start of the
        // entries region, but tear the write after only the first aligned
        // block reaches the platter (the rest is lost to power failure).
        let reopened = RedoLog::open(dyn_dev.clone(), 0, size as u64).unwrap();
        let retained: Vec<RedoEntry> = reopened
            .read_from_sequence(51)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(retained.len(), 150, "precondition: 150 retained entries");
        let mut bytes = Vec::new();
        for e in &retained {
            bytes.extend_from_slice(&e.serialize());
        }
        assert!(bytes.len() > align, "retained set must span >1 block");
        let entries_off = align as u64; // header is one aligned block

        // Torn write: only the first `align` bytes land durably.
        let mut first_block = vec![0u8; align];
        first_block.copy_from_slice(&bytes[..align]);
        dev.pwrite(&first_block, entries_off).unwrap();
        dev.sync().unwrap(); // first block durable
        dev.crash(); // remainder of the in-place rewrite lost

        // Reopen and scan from offset 0 (old behavior had no
        // logical_start). The torn front block parses only a prefix of
        // the retained entries — the rest are gone.
        let recovered = {
            let mut buf = vec![0u8; align];
            dev.pread(&mut buf, entries_off).unwrap();
            // Count how many whole entries survive in the first block.
            let mut count = 0;
            let mut pos = 0;
            while let Some((_, consumed)) = RedoEntry::deserialize(&buf[pos..]) {
                count += 1;
                pos += consumed;
                if pos >= buf.len() {
                    break;
                }
            }
            count
        };
        assert!(
            recovered < 150,
            "OLD in-place torn write must lose retained entries (survived {recovered}/150)",
        );
    }

    /// B-3: the NEW compaction strategy survives a crash at the worst
    /// point — after the relocated copy is fsynced but before the header
    /// flip is durable. Recovery must reproduce ALL retained (acked)
    /// entries from the pre-compaction copy with zero loss.
    #[test]
    fn new_compaction_crash_before_header_flip_preserves_retained() {
        let align = 4096usize;
        let size = 256 * 1024usize;
        let dev = Arc::new(CrashCowDevice::new(size, align));
        let dyn_dev: Arc<dyn BlockDevice> = dev.clone();

        let mut log = RedoLog::open(dyn_dev.clone(), 0, size as u64).unwrap();
        for i in 1..=60u8 {
            log.append_and_flush(RedoOp::Freeze {
                tx_key: test_key(i),
                offset: i as u32,
            })
            .unwrap();
        }
        let high_seq = log.current_sequence();

        // Count syncs already issued, then arm the crash for the SECOND
        // sync that occurs during compaction. compact does:
        //   sync #A = relocated-copy fsync (durable)
        //   sync #B = header-flip fsync     <- crash here
        let base = dev.sync_count.load(Ordering::SeqCst);
        dev.arm_crash_at_sync(base + 2);

        let err = log.compact_prefix_through(30);
        assert!(err.is_err(), "compaction must observe the simulated crash");
        assert!(dev.crashed(), "device must have crashed at the header flip");
        drop(log);

        // Reopen: the header was never flipped, so logical_start still
        // points at the ORIGINAL retained copy. All 60 entries (we
        // compacted through 30 but the prefix bytes are still physically
        // present and pointed at) replay — crucially seq 31..=60 (the
        // retained, possibly-acked set) are intact.
        let reopened = RedoLog::open(dyn_dev, 0, size as u64).unwrap();
        let entries = reopened.read_from_sequence(31).unwrap();
        let seqs: Vec<u64> = entries.iter().map(|e| e.sequence).collect();
        assert_eq!(
            seqs,
            (31..=60).collect::<Vec<u64>>(),
            "retained entries must survive a crash before the header flip",
        );
        assert_eq!(
            reopened.current_sequence(),
            high_seq,
            "next_sequence must not roll back",
        );
    }

    /// PERF #5: a hot-path flush issues exactly ONE device sync. The header
    /// (next_sequence high-water) is folded into the SAME fsync as the entries
    /// instead of a second standalone fsync — halving per-flush fsyncs while
    /// keeping next_sequence durable on every flush (the corrupt-tail recovery
    /// test above proves that durability is load-bearing: it is what stops a
    /// truncated-tail reopen from reusing a sequence number).
    #[test]
    fn flush_folds_header_into_single_device_sync() {
        let dev = Arc::new(CrashCowDevice::new(256 * 1024, 4096));
        let dyn_dev: Arc<dyn BlockDevice> = dev.clone();
        let mut log = RedoLog::open(dyn_dev, 0, 256 * 1024).unwrap();
        let base = dev.sync_count.load(Ordering::SeqCst);
        log.append(RedoOp::Freeze {
            tx_key: test_key(1),
            offset: 0,
        })
        .unwrap();
        log.flush().unwrap();
        assert_eq!(
            dev.sync_count.load(Ordering::SeqCst) - base,
            1,
            "flush must issue exactly one device sync (entries + header folded into one), not two",
        );
    }

    /// PERF #6: the redo hot-path flush issues `sync_data` (fdatasync), not the
    /// full `sync` (fsync) — the fixed-length redo region never resizes, so the
    /// inode-metadata flush a full fsync performs is unnecessary.
    #[test]
    fn flush_uses_sync_data_not_full_sync() {
        struct SyncKindDevice {
            inner: crate::device::MemoryDevice,
            full: std::sync::atomic::AtomicU64,
            data: std::sync::atomic::AtomicU64,
        }
        impl BlockDevice for SyncKindDevice {
            fn pread(&self, b: &mut [u8], o: u64) -> crate::device::Result<usize> {
                self.inner.pread(b, o)
            }
            fn pwrite(&self, b: &[u8], o: u64) -> crate::device::Result<usize> {
                self.inner.pwrite(b, o)
            }
            fn alignment(&self) -> usize {
                self.inner.alignment()
            }
            fn size(&self) -> u64 {
                self.inner.size()
            }
            fn sync(&self) -> crate::device::Result<()> {
                self.full.fetch_add(1, Ordering::SeqCst);
                self.inner.sync()
            }
            fn sync_data(&self) -> crate::device::Result<()> {
                self.data.fetch_add(1, Ordering::SeqCst);
                self.inner.sync()
            }
        }

        let dev = Arc::new(SyncKindDevice {
            inner: crate::device::MemoryDevice::new(256 * 1024, 4096).unwrap(),
            full: std::sync::atomic::AtomicU64::new(0),
            data: std::sync::atomic::AtomicU64::new(0),
        });
        let dyn_dev: Arc<dyn BlockDevice> = dev.clone();
        let mut log = RedoLog::open(dyn_dev, 0, 256 * 1024).unwrap();
        let full0 = dev.full.load(Ordering::SeqCst);
        let data0 = dev.data.load(Ordering::SeqCst);
        log.append(RedoOp::Freeze {
            tx_key: test_key(2),
            offset: 0,
        })
        .unwrap();
        log.flush().unwrap();
        assert_eq!(
            dev.data.load(Ordering::SeqCst) - data0,
            1,
            "redo flush must use sync_data (fdatasync) on the hot path",
        );
        assert_eq!(
            dev.full.load(Ordering::SeqCst) - full0,
            0,
            "redo flush must NOT issue a full fsync on the hot path",
        );
    }

    /// B-3: a clean (uninterrupted) compaction with a non-empty retained
    /// set relocates the live window via `logical_start` and reopens with
    /// exactly the retained entries — and the relocated copy survives a
    /// reopen even though the original prefix bytes are still on disk.
    #[test]
    fn new_compaction_clean_relocates_via_logical_start() {
        let align = 4096usize;
        let size = 256 * 1024usize;
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(size as u64, align).unwrap());

        let mut log = RedoLog::open(dev.clone(), 0, size as u64).unwrap();
        for i in 1..=40u8 {
            log.append_and_flush(RedoOp::Freeze {
                tx_key: test_key(i),
                offset: i as u32,
            })
            .unwrap();
        }
        let high_seq = log.current_sequence();
        log.compact_prefix_through(25).unwrap();
        // logical_start advanced past offset 0.
        assert!(log.logical_start > 0, "logical_start must advance");
        drop(log);

        let reopened = RedoLog::open(dev, 0, size as u64).unwrap();
        let seqs: Vec<u64> = reopened
            .read_from_sequence(1)
            .unwrap()
            .iter()
            .map(|e| e.sequence)
            .collect();
        assert_eq!(
            seqs,
            (26..=40).collect::<Vec<u64>>(),
            "only retained entries 26..=40 must be visible after compaction",
        );
        assert_eq!(reopened.current_sequence(), high_seq);
    }

    /// B-3: when the tail has no room, compaction relocates the retained
    /// copy into the stale front gap left by a previous compaction rather
    /// than failing — and recovery still reproduces exactly the retained
    /// entries.
    #[test]
    fn new_compaction_reuses_front_gap_when_tail_full() {
        let align = 4096usize;
        // Small region: header(1 block) + ~7 entry blocks.
        let size = 8 * align;
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(size as u64, align).unwrap());

        let mut log = RedoLog::open(dev.clone(), 0, size as u64).unwrap();
        // Each append_and_flush consumes one aligned block. Fill several.
        for i in 1..=5u8 {
            log.append_and_flush(RedoOp::Freeze {
                tx_key: test_key(i),
                offset: i as u32,
            })
            .unwrap();
        }
        // First compaction: retain 4..=5 → relocates past the tail and
        // advances logical_start, opening a front gap.
        log.compact_prefix_through(3).unwrap();
        let first_start = log.logical_start;
        assert!(first_start > 0, "first compaction opens a front gap");

        // Append more until the tail is near the end so the next
        // compaction cannot fit past the tail and must reuse the front gap.
        let mut seq = log.current_sequence();
        loop {
            match log.append_and_flush(RedoOp::Freeze {
                tx_key: test_key(9),
                offset: 0,
            }) {
                Ok(s) => seq = s,
                Err(RedoError::LogFull { .. }) => break,
                Err(e) => panic!("unexpected: {e:?}"),
            }
        }
        // Retain only the final entry → small copy that fits the front gap.
        log.compact_prefix_through(seq - 1).unwrap();
        assert_eq!(
            log.logical_start, 0,
            "second compaction reused the front gap"
        );
        let high = log.current_sequence();
        drop(log);

        let reopened = RedoLog::open(dev, 0, size as u64).unwrap();
        let seqs: Vec<u64> = reopened
            .read_from_sequence(1)
            .unwrap()
            .iter()
            .map(|e| e.sequence)
            .collect();
        assert_eq!(seqs, vec![seq], "only the retained entry survives");
        assert_eq!(reopened.current_sequence(), high);
    }

    /// B-3: a version-1 header (no `logical_start`) decodes with
    /// `logical_start = 0` and is upgraded transparently.
    #[test]
    fn v1_header_decodes_with_zero_logical_start() {
        // Build a synthetic v1 header: magic|ver=1|reserved|next|ckpt|crc.
        let mut buf = Vec::new();
        buf.extend_from_slice(&REDO_HEADER_MAGIC);
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&[0u8; 2]);
        buf.extend_from_slice(&42u64.to_le_bytes());
        buf.extend_from_slice(&7u64.to_le_bytes());
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(buf.len(), HEADER_FIXED_LEN_V1);

        let header = RedoHeader::deserialize(&buf).unwrap();
        assert_eq!(header.next_sequence, 42);
        assert_eq!(header.checkpoint_seq, 7);
        assert_eq!(header.logical_start, 0, "v1 logical_start defaults to 0");
    }

    #[test]
    fn atomic_snapshot_tracks_append_without_lock() {
        let (_dev, mut log) = make_log(1 << 20);
        let atomics = log.atomics();
        let before = atomics.write_position();
        log.append(RedoOp::Checkpoint).unwrap();
        log.flush().unwrap();
        let after = atomics.write_position();
        assert!(
            after > before,
            "atomic write_position must advance after a flushed append"
        );
        // The atomic snapshot must agree with the locked accessors.
        assert_eq!(atomics.write_position(), log.write_position());
        assert_eq!(atomics.available_space(), log.available_space());
    }
}
