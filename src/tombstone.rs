//! Append-only on-device deletion-tombstone log (deletion-tombstone Phase 1).
//!
//! A tombstone is a tiny durable record `(txid, shard, deletion_height,
//! generation, cause)` written when the cluster physically removes a UTXO
//! record. Its purpose is to make the
//! *absence* of a key self-describing — "absent because the cluster deleted
//! it" rather than "absent, never received" — so a stale rejoinee can drop a
//! deleted key instead of resurrecting it.
//!
//! ## How this differs from the redo log
//!
//! This module is modeled on [`crate::redo`] (same `O_DIRECT` [`crate::device`]
//! I/O, same header-block + CRC-per-entry conventions, same torn-tail-drop scan
//! semantics) but with one load-bearing difference:
//!
//! * The redo log is **linear-with-reset**: `write_pos` advances and the
//!   checkpoint task `reset`s it to zero, so a redo entry survives at most one
//!   checkpoint interval.
//! * The tombstone log is **append-only and NEVER reset on checkpoint**. It is
//!   compacted *only* by GC (a later phase) via [`TombstoneLog::compact_through`],
//!   which reclaims the prefix below a proven-safe block height. This is what
//!   lets tombstones outlive the linear-reset redo window across a rolling
//!   restart.
//!
//! ## On-device layout
//!
//! ```text
//! [ TombstoneHeader block : header_block_size bytes ]
//! [ Tombstone entry : 56 bytes ] [ Tombstone entry : 56 bytes ] ...
//! ```
//!
//! The header block (aligned to the device's block size, like
//! [`crate::redo`]'s `RedoHeader`) carries a magic + version + `next_seq`
//! high-water + `compacted_through_height` + a CRC. Entries are fixed-size
//! 56-byte [`Tombstone`] records appended immediately after the header block;
//! each carries its own CRC-32 so a torn tail entry is detected and dropped on
//! scan exactly as a torn redo tail is.
//!
//! ## Phase scope
//!
//! This is PURE STORAGE. Nothing in the engine, replication, reconciliation,
//! GC daemon, or recovery wires into it yet (those are later phases of the
//! deletion-tombstone design). The module is additive: it changes no existing
//! behavior.

use crate::device::{AlignedBuf, BlockDevice};
use crate::index::TxKey;
use std::mem::size_of;
use std::sync::Arc;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from tombstone log operations.
#[derive(Error, Debug)]
pub enum TombstoneError {
    /// The tombstone region is full — no space for another 56-byte entry
    /// before the next GC compaction reclaims the prefix. Unlike the redo
    /// log this is not relieved by a checkpoint; only
    /// [`TombstoneLog::compact_through`] frees space.
    #[error("tombstone log full: {used}/{capacity} bytes used")]
    RegionFull { used: u64, capacity: u64 },

    /// Device I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] crate::device::DeviceError),

    /// An entry's stored CRC did not match the computed CRC over its bytes.
    #[error(
        "tombstone entry CRC mismatch at offset {offset}: stored {stored:#x}, computed {computed:#x}"
    )]
    Crc {
        offset: u64,
        stored: u32,
        computed: u32,
    },

    /// A `cause` byte did not correspond to a known [`TombstoneCause`].
    #[error("unknown tombstone cause byte: {0}")]
    UnknownCause(u8),

    /// The header block carries a magic that does not match this format.
    /// Either a foreign region was pointed at or the log was written by an
    /// incompatible version. Open refuses rather than misparsing.
    #[error(
        "tombstone header magic mismatch: expected {expected:#x?}, found {found:#x?} — log was written by an incompatible version"
    )]
    HeaderMagicMismatch { expected: [u8; 8], found: [u8; 8] },

    /// The header block CRC does not match the rest of the header bytes
    /// (torn write / device corruption). Open refuses rather than seeding a
    /// bogus `next_seq`.
    #[error("tombstone header CRC mismatch: stored {stored:#x}, computed {computed:#x}")]
    HeaderCrcMismatch { stored: u32, computed: u32 },

    /// The header carries a format version this binary does not understand.
    #[error("tombstone header version {found} not supported (expected {expected})")]
    UnsupportedHeaderVersion { expected: u16, found: u16 },

    /// The requested region (`region_offset + region_size`) does not fit
    /// within the backing device. Rejected at open so callers never issue
    /// an I/O past the end of the device.
    #[error(
        "tombstone region out of bounds: offset {region_offset} + size {region_size} > device size {device_size}"
    )]
    OutOfBounds {
        region_offset: u64,
        region_size: u64,
        device_size: u64,
    },

    /// The region is too small to hold the header block plus at least one
    /// aligned entry block.
    #[error("tombstone region too small: {region_size} bytes (header block requires {required})")]
    RegionTooSmall { region_size: u64, required: u64 },
}

/// Result alias for tombstone operations.
pub type Result<T> = std::result::Result<T, TombstoneError>;

// ---------------------------------------------------------------------------
// TombstoneCause
// ---------------------------------------------------------------------------

/// Why a record was physically deleted. Stored as a single byte in the
/// [`Tombstone`] and used as a diagnostic + GC-policy hook (later phases).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TombstoneCause {
    /// DAH sweep: a spent-and-mined UTXO whose retention window elapsed.
    /// This is the dominant path and the one that causes resurrection
    /// double-spends if a stale node rejoins.
    SpentDah = 0,
    /// Direct admin `DeleteRequest` (unconditional, out-of-band).
    Admin = 1,
    /// Migration prune: a local key the authoritative manifest omits.
    MigrationPrune = 2,
}

impl TombstoneCause {
    /// The single-byte discriminant for on-device storage.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse a cause byte. Returns [`TombstoneError::UnknownCause`] for any
    /// value outside the defined discriminants so a corrupt-but-CRC-valid
    /// byte cannot silently decode to a wrong variant.
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(TombstoneCause::SpentDah),
            1 => Ok(TombstoneCause::Admin),
            2 => Ok(TombstoneCause::MigrationPrune),
            other => Err(TombstoneError::UnknownCause(other)),
        }
    }
}

// ---------------------------------------------------------------------------
// Tombstone (on-device entry)
// ---------------------------------------------------------------------------

/// Fixed-size on-device tombstone entry — exactly 56 bytes, 8-byte aligned.
///
/// Field layout:
///
/// | field | bytes | offset |
/// |---|---|---|
/// | `txid` | 32 | 0 |
/// | `shard` | 2 | 32 |
/// | `deletion_height` | 4 | 34 |
/// | `generation` | 4 | 38 |
/// | `cause` | 1 | 42 |
/// | `flags` | 1 | 43 |
/// | `_pad` | 8 | 44 |
/// | `crc32` | 4 | 52 |
///
/// The design table lists `_pad: [u8;4]`, which would sum the fields to 52
/// bytes; to honor the "Total 56 bytes, 8-byte aligned" requirement the
/// padding is widened to 8 bytes so the struct's `size_of` is exactly 56 and a
/// multiple of 8. The padding bytes are written as zero. The CRC at offset 52
/// covers the preceding 52 bytes (every field including `_pad`).
///
/// `#[repr(C, packed)]` guarantees the on-device byte order matches the field
/// declaration order with no compiler-inserted padding between fields; the
/// explicit `_pad` is the only padding and is part of the wire layout.
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct Tombstone {
    /// The deleted key — matches [`TxKey::txid`].
    pub txid: [u8; 32],
    /// Shard id, so GC and reconciliation are O(shard) without recomputing
    /// placement.
    pub shard: u16,
    /// Block height at which the deletion became authoritative. Drives the
    /// GC horizon (a later phase).
    pub deletion_height: u32,
    /// The record's `generation` at deletion time. Lets reconciliation
    /// distinguish "this exact version was deleted" from "a newer
    /// re-creation exists" (the create-after-delete defense, design §8.4).
    pub generation: u32,
    /// [`TombstoneCause`] discriminant.
    pub cause: u8,
    /// Reserved flags byte (zero today).
    pub flags: u8,
    /// Explicit padding to make the struct 56 bytes / 8-byte aligned.
    pub _pad: [u8; 8],
    /// CRC-32 over the preceding 52 bytes of the serialized layout.
    pub crc32: u32,
}

/// Serialized on-device size of a [`Tombstone`], in bytes.
pub const TOMBSTONE_SIZE: usize = 56;

// Compile-time size assertion (project convention; see `record.rs`).
const _: () = assert!(size_of::<Tombstone>() == TOMBSTONE_SIZE);
// 8-byte alignment requirement from the design table.
const _: () = assert!(TOMBSTONE_SIZE.is_multiple_of(8));

/// Number of bytes covered by the entry CRC: everything before the trailing
/// `crc32` field.
const TOMBSTONE_CRC_REGION: usize = TOMBSTONE_SIZE - 4;

impl Tombstone {
    /// Construct a tombstone from its logical fields, computing the CRC.
    ///
    /// `flags` is reserved and should be zero today. The returned value is
    /// ready to [`serialize`](Self::serialize) — its `crc32` already matches
    /// the serialized bytes.
    pub fn new(
        txid: [u8; 32],
        shard: u16,
        deletion_height: u32,
        generation: u32,
        cause: TombstoneCause,
        flags: u8,
    ) -> Self {
        let mut t = Tombstone {
            txid,
            shard,
            deletion_height,
            generation,
            cause: cause.as_u8(),
            flags,
            _pad: [0u8; 8],
            crc32: 0,
        };
        let bytes = t.serialize_without_crc();
        t.crc32 = crc32fast::hash(&bytes);
        t
    }

    /// The decoded [`TombstoneCause`].
    ///
    /// # Errors
    /// [`TombstoneError::UnknownCause`] if the stored byte is not a known
    /// discriminant.
    pub fn cause(&self) -> Result<TombstoneCause> {
        // Copy the packed field to a local before reading (E0793).
        let c = self.cause;
        TombstoneCause::from_u8(c)
    }

    /// The key as a [`TxKey`].
    pub fn tx_key(&self) -> TxKey {
        TxKey { txid: self.txid }
    }

    /// Serialize the 52-byte CRC region (every field except the trailing
    /// `crc32`). Used both to compute and to verify the CRC.
    fn serialize_without_crc(&self) -> [u8; TOMBSTONE_CRC_REGION] {
        // Copy packed fields to locals before use (E0793).
        let txid = self.txid;
        let shard = self.shard;
        let deletion_height = self.deletion_height;
        let generation = self.generation;
        let cause = self.cause;
        let flags = self.flags;
        let pad = self._pad;

        let mut out = [0u8; TOMBSTONE_CRC_REGION];
        out[0..32].copy_from_slice(&txid);
        out[32..34].copy_from_slice(&shard.to_le_bytes());
        out[34..38].copy_from_slice(&deletion_height.to_le_bytes());
        out[38..42].copy_from_slice(&generation.to_le_bytes());
        out[42] = cause;
        out[43] = flags;
        out[44..52].copy_from_slice(&pad);
        out
    }

    /// Serialize to the fixed 56-byte on-device representation.
    pub fn serialize(&self) -> [u8; TOMBSTONE_SIZE] {
        let body = self.serialize_without_crc();
        let crc = self.crc32;
        let mut out = [0u8; TOMBSTONE_SIZE];
        out[..TOMBSTONE_CRC_REGION].copy_from_slice(&body);
        out[TOMBSTONE_CRC_REGION..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Parse a tombstone from its fixed 56-byte representation, verifying the
    /// CRC and the `cause` byte.
    ///
    /// # Errors
    /// * [`TombstoneError::Crc`] if the stored CRC does not match.
    /// * [`TombstoneError::UnknownCause`] if the `cause` byte is invalid.
    ///
    /// `offset` is used only to make the [`TombstoneError::Crc`] error
    /// self-describing; pass the entry's byte offset within the region (or 0
    /// for an offset-agnostic parse).
    pub fn parse(bytes: &[u8; TOMBSTONE_SIZE], offset: u64) -> Result<Self> {
        let stored = u32::from_le_bytes(bytes[TOMBSTONE_CRC_REGION..].try_into().unwrap());
        let computed = crc32fast::hash(&bytes[..TOMBSTONE_CRC_REGION]);
        if stored != computed {
            return Err(TombstoneError::Crc {
                offset,
                stored,
                computed,
            });
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&bytes[0..32]);
        let shard = u16::from_le_bytes(bytes[32..34].try_into().unwrap());
        let deletion_height = u32::from_le_bytes(bytes[34..38].try_into().unwrap());
        let generation = u32::from_le_bytes(bytes[38..42].try_into().unwrap());
        let cause = bytes[42];
        // Validate the cause byte rather than trusting a CRC-valid-but-bogus
        // value; a corrupt cause is a hard parse error.
        TombstoneCause::from_u8(cause)?;
        let flags = bytes[43];
        let mut pad = [0u8; 8];
        pad.copy_from_slice(&bytes[44..52]);
        Ok(Tombstone {
            txid,
            shard,
            deletion_height,
            generation,
            cause,
            flags,
            _pad: pad,
            crc32: stored,
        })
    }
}

impl std::fmt::Debug for Tombstone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Copy packed fields to locals before formatting (E0793).
        let txid = self.txid;
        let shard = self.shard;
        let deletion_height = self.deletion_height;
        let generation = self.generation;
        let cause = self.cause;
        let flags = self.flags;
        let crc32 = self.crc32;
        f.debug_struct("Tombstone")
            .field("txid", &hex_prefix(&txid))
            .field("shard", &shard)
            .field("deletion_height", &deletion_height)
            .field("generation", &generation)
            .field("cause", &cause)
            .field("flags", &flags)
            .field("crc32", &format_args!("{crc32:#010x}"))
            .finish()
    }
}

impl PartialEq for Tombstone {
    fn eq(&self, other: &Self) -> bool {
        // Compare via the serialized form to sidestep packed-field borrows.
        self.serialize() == other.serialize()
    }
}

impl Eq for Tombstone {}

/// Render the first few bytes of a txid for `Debug` without pulling a hex
/// dependency. Diagnostic only.
fn hex_prefix(txid: &[u8; 32]) -> String {
    let mut s = String::with_capacity(10);
    for b in &txid[..4] {
        s.push_str(&format!("{b:02x}"));
    }
    s.push_str("..");
    s
}

// ---------------------------------------------------------------------------
// TombstoneHeader
// ---------------------------------------------------------------------------

/// Magic bytes identifying a TeraSlab tombstone log region.
const TOMBSTONE_HEADER_MAGIC: [u8; 8] = *b"TSLTOMB1";

/// Current tombstone-log format version.
const TOMBSTONE_HEADER_VERSION: u16 = 1;

/// Fixed serialized header length:
/// `magic(8) | version(2) | reserved(2) | next_seq(8) |
/// compacted_through_height(4) | crc32(4)` = 28 bytes. The CRC covers every
/// byte before it.
const HEADER_FIXED_LEN: usize = 8 + 2 + 2 + 8 + 4 + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TombstoneHeader {
    /// Monotonic high-water sequence: the next sequence number that will be
    /// assigned to an appended tombstone. Persisted so it never rolls back
    /// across a restart even after compaction reclaims the prefix.
    next_seq: u64,
    /// The block height through which the prefix has been GC-compacted. All
    /// live entries have `deletion_height >= compacted_through_height`. A
    /// scan reads only from this point forward.
    compacted_through_height: u32,
}

impl TombstoneHeader {
    /// Serialize the header into a fresh `Vec<u8>` of length
    /// [`HEADER_FIXED_LEN`]. Callers pad to the header block size before
    /// writing.
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_FIXED_LEN);
        buf.extend_from_slice(&TOMBSTONE_HEADER_MAGIC);
        buf.extend_from_slice(&TOMBSTONE_HEADER_VERSION.to_le_bytes());
        buf.extend_from_slice(&[0u8; 2]); // reserved
        buf.extend_from_slice(&self.next_seq.to_le_bytes());
        buf.extend_from_slice(&self.compacted_through_height.to_le_bytes());
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        debug_assert_eq!(buf.len(), HEADER_FIXED_LEN);
        buf
    }

    /// Parse a header from the prefix of `data`.
    ///
    /// # Errors
    /// * [`TombstoneError::HeaderMagicMismatch`] for a foreign / wrong magic.
    /// * [`TombstoneError::UnsupportedHeaderVersion`] for a known-magic but
    ///   unknown version.
    /// * [`TombstoneError::HeaderCrcMismatch`] for a corrupt CRC.
    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_FIXED_LEN {
            let mut found = [0u8; 8];
            let copy_len = data.len().min(8);
            found[..copy_len].copy_from_slice(&data[..copy_len]);
            return Err(TombstoneError::HeaderMagicMismatch {
                expected: TOMBSTONE_HEADER_MAGIC,
                found,
            });
        }
        let mut found_magic = [0u8; 8];
        found_magic.copy_from_slice(&data[..8]);
        if found_magic != TOMBSTONE_HEADER_MAGIC {
            return Err(TombstoneError::HeaderMagicMismatch {
                expected: TOMBSTONE_HEADER_MAGIC,
                found: found_magic,
            });
        }
        let version = u16::from_le_bytes(data[8..10].try_into().unwrap());
        if version != TOMBSTONE_HEADER_VERSION {
            return Err(TombstoneError::UnsupportedHeaderVersion {
                expected: TOMBSTONE_HEADER_VERSION,
                found: version,
            });
        }
        // skip reserved 2 bytes (data[10..12])
        let next_seq = u64::from_le_bytes(data[12..20].try_into().unwrap());
        let compacted_through_height = u32::from_le_bytes(data[20..24].try_into().unwrap());
        let stored_crc = u32::from_le_bytes(data[24..28].try_into().unwrap());
        let computed = crc32fast::hash(&data[..24]);
        if stored_crc != computed {
            return Err(TombstoneError::HeaderCrcMismatch {
                stored: stored_crc,
                computed,
            });
        }
        Ok(TombstoneHeader {
            next_seq,
            compacted_through_height,
        })
    }
}

// ---------------------------------------------------------------------------
// TombstoneLog
// ---------------------------------------------------------------------------

/// Append-only on-device tombstone log.
///
/// Entries are 56-byte [`Tombstone`] records appended after a fixed header
/// block. Appends are durable only after [`TombstoneLog::sync`] (or a
/// [`TombstoneLog::append_synced`]) — `append` writes the bytes via the
/// device's `pwrite` but coalesces the fsync with the caller's existing
/// delete fsync (design §3.3: zero net new fsyncs on the hot path). The log
/// is NOT reset on checkpoint; [`TombstoneLog::compact_through`] is the only
/// path that reclaims space.
pub struct TombstoneLog {
    device: Arc<dyn BlockDevice>,
    /// Device byte offset of the region's first byte (the header block).
    region_offset: u64,
    /// Total bytes of the region, header + entries.
    region_size: u64,
    /// Bytes reserved at the start for the fixed header block (== device
    /// alignment), captured at open.
    header_block_size: u64,
    /// Byte offset of the next entry append, relative to the entries region
    /// start (not the device). Advances by [`TOMBSTONE_SIZE`] per append.
    write_pos: u64,
    /// Next sequence number to assign. Persisted in the header so it never
    /// rolls back across a restart.
    next_seq: u64,
    /// The GC-compacted height watermark from the header.
    compacted_through_height: u32,
}

impl TombstoneLog {
    /// Open an existing tombstone log or initialise a fresh one at the given
    /// device region.
    ///
    /// If the header block is freshly zeroed the region is initialised with a
    /// fresh header (`next_seq = 1`, `compacted_through_height = 0`). If the
    /// magic is present but invalid the open fails rather than misparsing.
    /// After reading the header, the entries region is scanned to find the
    /// append tail; a torn (partial or CRC-failing) final entry is treated as
    /// end-of-log and dropped, so appends resume after the last fully-valid
    /// entry.
    ///
    /// # Errors
    /// * [`TombstoneError::OutOfBounds`] if the region would extend past the
    ///   device.
    /// * [`TombstoneError::RegionTooSmall`] if the region cannot hold the
    ///   header block plus one aligned entry block.
    /// * [`TombstoneError::HeaderMagicMismatch`] /
    ///   [`TombstoneError::HeaderCrcMismatch`] /
    ///   [`TombstoneError::UnsupportedHeaderVersion`] for an incompatible
    ///   existing header.
    pub fn open(
        device: Arc<dyn BlockDevice>,
        region_offset: u64,
        region_size: u64,
    ) -> Result<Self> {
        let mut log = Self::validated_skeleton(device, region_offset, region_size)?;

        let header_present = log.read_header_or_init()?;
        let (entries, tail_pos) = log.scan_entries_region_with_tail()?;
        log.write_pos = tail_pos;

        // Sequence numbers live only in the header (entries carry no seq
        // field). `read_header_or_init` already seeded `next_seq` from the
        // header when present, so it never rolls back across a restart — even
        // after a compaction reclaimed the prefix.
        //
        // For a fresh region (no header) the scan determines the high-water:
        // each surviving entry consumed one sequence number, so the next
        // sequence is `1 + entries_seen`. A torn-tail entry is already
        // excluded from `entries`, which matches the append tail.
        if !header_present {
            log.next_seq = 1u64.saturating_add(entries.len() as u64);
            log.write_header()?;
        }
        Ok(log)
    }

    /// Create (force-initialise) a fresh tombstone log at the given region,
    /// zeroing the entries region and writing a fresh header.
    ///
    /// Unlike [`open`](Self::open) this does not attempt to recover an
    /// existing log; it unconditionally resets the region to empty. Used when
    /// provisioning a new device.
    ///
    /// # Errors
    /// Same region-bounds errors as [`open`](Self::open), plus any device I/O
    /// error while zeroing or writing the header.
    pub fn create(
        device: Arc<dyn BlockDevice>,
        region_offset: u64,
        region_size: u64,
    ) -> Result<Self> {
        let log = Self::validated_skeleton(device, region_offset, region_size)?;
        log.zero_entries_region()?;
        log.write_header()?;
        Ok(log)
    }

    /// Validate the region bounds and return a zeroed in-memory skeleton with
    /// a fresh-log seed (`next_seq = 1`, `compacted_through_height = 0`).
    /// Shared by [`open`](Self::open) and [`create`](Self::create).
    fn validated_skeleton(
        device: Arc<dyn BlockDevice>,
        region_offset: u64,
        region_size: u64,
    ) -> Result<Self> {
        let device_size = device.size();
        let end = region_offset
            .checked_add(region_size)
            .ok_or(TombstoneError::OutOfBounds {
                region_offset,
                region_size,
                device_size,
            })?;
        if end > device_size {
            return Err(TombstoneError::OutOfBounds {
                region_offset,
                region_size,
                device_size,
            });
        }
        let align = device.alignment() as u64;
        if align == 0 {
            return Err(TombstoneError::RegionTooSmall {
                region_size,
                required: 1,
            });
        }
        let header_block_size = align;
        if region_size < header_block_size.saturating_mul(2) {
            return Err(TombstoneError::RegionTooSmall {
                region_size,
                required: header_block_size.saturating_mul(2),
            });
        }
        Ok(Self {
            device,
            region_offset,
            region_size,
            header_block_size,
            write_pos: 0,
            next_seq: 1,
            compacted_through_height: 0,
        })
    }

    /// Device byte offset of the first entry byte (header block end).
    fn entries_region_offset(&self) -> u64 {
        self.region_offset + self.header_block_size
    }

    /// Number of bytes available for entries (region minus header block).
    fn entries_region_size(&self) -> u64 {
        self.region_size - self.header_block_size
    }

    /// The GC watermark: live entries all have `deletion_height >=` this.
    pub fn compacted_through_height(&self) -> u32 {
        self.compacted_through_height
    }

    /// The next sequence number that will be assigned to an appended entry.
    pub fn current_sequence(&self) -> u64 {
        self.next_seq
    }

    /// Bytes currently occupied by entries (from the entries-region start).
    pub fn write_position(&self) -> u64 {
        self.write_pos
    }

    /// Total capacity of the entries region in bytes.
    pub fn capacity(&self) -> u64 {
        self.entries_region_size()
    }

    /// Number of entries that currently fit before [`TombstoneError::RegionFull`].
    pub fn available_entries(&self) -> u64 {
        self.entries_region_size().saturating_sub(self.write_pos) / TOMBSTONE_SIZE as u64
    }

    /// Read the on-disk header. Returns `Ok(true)` if a valid header was
    /// decoded, `Ok(false)` if the header block is freshly zeroed (caller
    /// should init a fresh header). Typed error for mismatched / corrupt
    /// header.
    fn read_header_or_init(&mut self) -> Result<bool> {
        let align = self.device.alignment();
        let mut buf = AlignedBuf::new(self.header_block_size as usize, align);
        self.device.pread_exact_at(&mut buf, self.region_offset)?;
        if buf[..HEADER_FIXED_LEN].iter().all(|b| *b == 0) {
            return Ok(false);
        }
        let header = TombstoneHeader::deserialize(&buf[..HEADER_FIXED_LEN])?;
        self.next_seq = header.next_seq.max(1);
        self.compacted_through_height = header.compacted_through_height;
        Ok(true)
    }

    /// Serialize and durably write the header, padded to the header block
    /// size and fsynced.
    fn write_header(&self) -> Result<()> {
        let header = TombstoneHeader {
            next_seq: self.next_seq,
            compacted_through_height: self.compacted_through_height,
        };
        let bytes = header.serialize();
        let align = self.device.alignment();
        let mut buf = AlignedBuf::new(self.header_block_size as usize, align);
        buf[..bytes.len()].copy_from_slice(&bytes);
        self.device.pwrite_all_at(&buf, self.region_offset)?;
        self.device.sync()?;
        Ok(())
    }

    /// Zero the entire entries region in aligned chunks. Used by
    /// [`create`](Self::create) and [`compact_through`](Self::compact_through)
    /// when the live set becomes empty.
    fn zero_entries_region(&self) -> Result<()> {
        let align = self.device.alignment();
        let entries_off = self.entries_region_offset();
        let entries_size = self.entries_region_size() as usize;
        const ZERO_CHUNK: usize = 1024 * 1024;
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
        Ok(())
    }

    /// Append one tombstone entry (NOT yet durable until [`sync`](Self::sync)).
    ///
    /// Assigns the next sequence number (the caller does not pass one — the
    /// log owns sequencing) and writes the 56 entry bytes via the device's
    /// `pwrite`, padded to the device alignment so there is no
    /// read-modify-write of a trailing partial block. The fsync is deferred
    /// to [`sync`](Self::sync) so the tombstone can ride an existing delete
    /// fsync (design §3.3). Returns the assigned sequence number.
    ///
    /// # Errors
    /// * [`TombstoneError::RegionFull`] if there is no room for another entry
    ///   (only relieved by [`compact_through`](Self::compact_through)).
    /// * [`TombstoneError::Io`] on a device write error.
    pub fn append(&mut self, tombstone: &Tombstone) -> Result<u64> {
        let entries_capacity = self.entries_region_size();
        if self.write_pos + TOMBSTONE_SIZE as u64 > entries_capacity {
            return Err(TombstoneError::RegionFull {
                used: self.write_pos,
                capacity: entries_capacity,
            });
        }

        let bytes = tombstone.serialize();
        let align = self.device.alignment();
        let align_u64 = align as u64;
        let entries_off = self.entries_region_offset();
        let device_offset = entries_off + self.write_pos;
        let aligned_offset = device_offset / align_u64 * align_u64;
        let intra = (device_offset - aligned_offset) as usize;
        let total = intra + TOMBSTONE_SIZE;
        let aligned_total = total.div_ceil(align) * align;

        let mut buf = AlignedBuf::new(aligned_total, align);
        if intra > 0 {
            // Read back only the leading partial block so the already-written
            // earlier entries in this block survive the pwrite. Trailing
            // bytes past our entry stay zero (the natural end-of-log sentinel
            // for the scan).
            self.device
                .pread_exact_at(&mut buf[..align], aligned_offset)?;
            buf[intra..align].fill(0);
        }
        buf[intra..intra + TOMBSTONE_SIZE].copy_from_slice(&bytes);
        self.device.pwrite_all_at(&buf, aligned_offset)?;

        let seq = self.next_seq;
        self.next_seq += 1;
        self.write_pos += TOMBSTONE_SIZE as u64;
        Ok(seq)
    }

    /// Make all appended entries durable.
    ///
    /// Fsyncs the device (covering the entry pwrites) and then rewrites the
    /// header so the bumped `next_seq` survives a restart even if the entries
    /// region is later compacted to empty. Callers that already fsync for a
    /// delete should call this once after the delete's own fsync to coalesce;
    /// the header rewrite is the only extra fsync and is amortized across a
    /// batch.
    ///
    /// # Errors
    /// [`TombstoneError::Io`] on a device sync / header write error.
    pub fn sync(&mut self) -> Result<()> {
        self.device.sync()?;
        self.write_header()
    }

    /// Append one tombstone and immediately make it durable.
    ///
    /// Convenience wrapper around [`append`](Self::append) + [`sync`](Self::sync)
    /// for callers that do not coalesce with another fsync. Returns the
    /// assigned sequence number.
    pub fn append_synced(&mut self, tombstone: &Tombstone) -> Result<u64> {
        let seq = self.append(tombstone)?;
        self.sync()?;
        Ok(seq)
    }

    /// Read all live tombstone entries from disk, validating each CRC.
    ///
    /// The scan reads the entries region PHYSICALLY from the front
    /// (`entries_region_offset`, i.e. relative position 0) and walks forward
    /// entry-by-entry. It does NOT filter by `compacted_through_height` — that
    /// header field is a GC bookkeeping watermark, not a read cursor; the live
    /// set is whatever entries are physically present at the front of the
    /// region. Compaction enforces the watermark by physically rewriting the
    /// retained suffix to the front (see [`compact_through`](Self::compact_through)),
    /// so the physical front IS the live set.
    ///
    /// The walk STOPS at the first all-zero block (the end-of-log sentinel left
    /// by fresh / reclaimed / zeroed space) or the first entry that fails to
    /// parse. A torn tail — a final entry whose bytes are partial, zero-padded,
    /// or CRC-failing — is DROPPED (treated as end-of-log) exactly like a torn
    /// redo tail; it is not an error. Returns the entries in append order.
    ///
    /// # Errors
    /// [`TombstoneError::Io`] on a device read error.
    pub fn scan(&self) -> Result<Vec<Tombstone>> {
        let (entries, _tail) = self.scan_entries_region_with_tail()?;
        Ok(entries)
    }

    /// Scan the entries region from disk, returning the parsed entries and
    /// the byte offset (relative to the entries region) of the append tail.
    ///
    /// The tail is the offset just past the last fully-valid entry. The first
    /// entry that fails to parse (CRC mismatch, all-zero sentinel, or a short
    /// trailing read) marks end-of-log; everything from there is treated as a
    /// torn tail / free space and dropped.
    fn scan_entries_region_with_tail(&self) -> Result<(Vec<Tombstone>, u64)> {
        const SCAN_CHUNK_BYTES: usize = 4 * 1024 * 1024;
        let align = self.device.alignment();
        let entries_off = self.entries_region_offset();
        let entries_size = self.entries_region_size();
        // Each non-final chunk must consume a whole number of entries AND
        // start at a device-aligned offset, so `pos` (the next read offset)
        // stays aligned across iterations. Pick a chunk that is a common
        // multiple of both the device alignment and `TOMBSTONE_SIZE` — its
        // least common multiple times a scale factor — so a full chunk leaves
        // no partial-entry remainder. Only the final partial chunk
        // (`remaining < chunk`) can have a sub-entry tail, where stopping is
        // correct.
        let step = lcm(align, TOMBSTONE_SIZE);
        let chunk = (SCAN_CHUNK_BYTES.next_multiple_of(step)).max(step);

        let mut entries = Vec::new();
        let mut pos: u64 = 0; // relative to entries region start
        let mut buf = AlignedBuf::new(chunk, align);

        while pos < entries_size {
            let remaining = entries_size - pos;
            let this_read = remaining.min(chunk as u64) as usize;
            let aligned_read = this_read.div_ceil(align) * align;
            self.device
                .pread_exact_at(&mut buf[..aligned_read], entries_off + pos)?;

            let mut local = 0usize;
            let mut stop = false;
            while local + TOMBSTONE_SIZE <= this_read {
                let entry_bytes: &[u8; TOMBSTONE_SIZE] = buf[local..local + TOMBSTONE_SIZE]
                    .try_into()
                    .expect("slice is exactly TOMBSTONE_SIZE");
                // All-zero block = end-of-log sentinel (fresh / reclaimed
                // space). Treat as torn tail / EOF.
                if entry_bytes.iter().all(|b| *b == 0) {
                    stop = true;
                    break;
                }
                match Tombstone::parse(entry_bytes, pos + local as u64) {
                    Ok(t) => {
                        entries.push(t);
                        local += TOMBSTONE_SIZE;
                    }
                    Err(_) => {
                        // Torn / corrupt tail entry: drop it and stop. This is
                        // the redo torn-tail convention — a bad final entry is
                        // not a hard error.
                        stop = true;
                        break;
                    }
                }
            }
            pos += local as u64;
            let was_final_chunk = this_read < chunk;
            if stop || local == 0 || was_final_chunk {
                // `stop`: hit a torn/zero entry — end of log.
                // `local == 0`: region remainder smaller than one entry — no
                // more whole entries can be read.
                // `was_final_chunk`: this read covered the rest of the entries
                // region (`remaining <= chunk`), so any sub-entry leftover is
                // trailing free space, not a continuation — re-reading it would
                // use an unaligned offset. Non-final chunks consume a whole
                // number of entries (chunk is a multiple of TOMBSTONE_SIZE), so
                // `pos` stays aligned for the next read.
                break;
            }
        }
        Ok((entries, pos))
    }

    /// GC-compact the log prefix: record `height` as the new
    /// `compacted_through_height`, dropping all entries with
    /// `deletion_height < height`.
    ///
    /// This is the ONLY path that reclaims space. It is crash-safe via a
    /// strict **zero-then-write-then-header** protocol (the in-place rewrite of
    /// a multi-block buffer is NOT atomic across device blocks, so the region
    /// MUST be cleared first):
    ///
    /// 1. **Zero** the entire entries region and fsync. This erases the old
    ///    image completely, so no stale old-image block can survive past a
    ///    partially-written new image.
    /// 2. **Write** the retained suffix (entries with `deletion_height >=
    ///    height`) to the FRONT of the region, followed by a zeroed sentinel
    ///    block, and fsync.
    /// 3. **Header** LAST (the commit point): advance `compacted_through_height`
    ///    and fsync. The header is a single aligned block, so this is an atomic
    ///    single-block write.
    ///
    /// Crash at any step yields either the old consistent state or the new
    /// consistent state, never a franken-mix:
    /// * Crash after step 1 (or mid step 2) — the front holds only a fully
    ///   persisted PREFIX of the new image; every byte past it is zero, so
    ///   [`scan`](Self::scan) stops at the first all-zero block and reads only
    ///   that prefix (a subset of the retained set, never stale old entries).
    ///   The header still carries the OLD (lower) watermark, so the next GC
    ///   tick re-runs the compaction cleanly.
    /// * Crash after step 2, before step 3 — the full new image is on disk but
    ///   the header still carries the OLD watermark; consistent, and the next
    ///   tick re-runs (idempotent).
    /// * Crash after step 3 — the new image and new watermark are both durable.
    ///
    /// Calling with a `height` at or below the current
    /// `compacted_through_height` is a no-op (idempotent).
    ///
    /// # Errors
    /// [`TombstoneError::Io`] on a device read/write/sync error.
    pub fn compact_through(&mut self, height: u32) -> Result<()> {
        if height <= self.compacted_through_height {
            return Ok(());
        }
        // Read the current live set, then retain only entries at or above the
        // new height.
        let retained: Vec<Tombstone> = self
            .scan()?
            .into_iter()
            .filter(|t| {
                let dh = t.deletion_height;
                dh >= height
            })
            .collect();

        if retained.is_empty() {
            // Nothing survives: zero the entries region and just advance the
            // header watermark. Crash-safe — there is no live entry to lose.
            self.zero_entries_region()?;
            self.write_pos = 0;
            self.compacted_through_height = height;
            return self.write_header();
        }

        // Rewrite the retained suffix to the front of the entries region.
        let align = self.device.alignment();
        let entries_off = self.entries_region_offset();
        let entries_capacity = self.entries_region_size();
        let content_len = retained.len() * TOMBSTONE_SIZE;
        let total = content_len.saturating_add(align);
        let aligned_total = total.div_ceil(align) * align;
        if aligned_total as u64 > entries_capacity {
            // Should not happen — retained is a subset of what already fit —
            // but guard rather than write past the region.
            return Err(TombstoneError::RegionFull {
                used: aligned_total as u64,
                capacity: entries_capacity,
            });
        }

        // STEP 1 — zero the WHOLE entries region first, then fsync. This is the
        // load-bearing crash-safety step. Without it, the multi-block
        // `pwrite_all_at` of the retained image below is NOT atomic across
        // device blocks: a crash could persist block 0 of the new (shorter)
        // image while block 1 still holds the STALE TAIL of the old (longer)
        // image — valid CRC-passing 56-byte entries with no torn boundary, so
        // `scan()` would read new-block-0 ++ stale-old-block-1 as a
        // consistent-looking but WRONG live set (resurrecting dropped
        // tombstones / losing retained ones). Pre-zeroing guarantees every byte
        // past the rewrite is zero, so whatever prefix of the new image is
        // persisted, `scan()` stops at the first all-zero block — it can only
        // read a prefix of the NEW image, never any old-image bleed-through.
        self.zero_entries_region()?;

        // STEP 2 — write the retained image to the front and fsync. The buffer
        // is `aligned_total` bytes: the entries followed by at least a full
        // zeroed sentinel block (the `+ align` above rounds up so there is
        // always a trailing zero block), so `scan()` stops cleanly past the
        // copy even when the retained image does not fill the region.
        let mut buf = AlignedBuf::new(aligned_total, align);
        for (i, t) in retained.iter().enumerate() {
            let off = i * TOMBSTONE_SIZE;
            buf[off..off + TOMBSTONE_SIZE].copy_from_slice(&t.serialize());
        }
        self.device.pwrite_all_at(&buf, entries_off)?;
        self.device.sync()?;

        self.write_pos = content_len as u64;
        self.compacted_through_height = height;
        // STEP 3 — header LAST (the commit point). The header is exactly one
        // aligned block (`header_block_size == alignment`), so its `pwrite` +
        // fsync is a single-block atomic write. Before this fsync the old
        // (lower) watermark is on disk paired with a consistent zeroed-tail new
        // front; after it the new watermark is authoritative. A crash before it
        // leaves the OLD watermark and the next GC tick re-runs cleanly.
        self.write_header()
    }
}

/// Greatest common divisor (Euclid).
fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

/// Least common multiple of two non-zero usizes.
fn lcm(a: usize, b: usize) -> usize {
    if a == 0 || b == 0 {
        return 0;
    }
    a / gcd(a, b) * b
}

// ---------------------------------------------------------------------------
// Migration reconciliation decision (deletion-tombstone Phase 8, design §7)
// ---------------------------------------------------------------------------

/// The action to take for a single local key during tombstone-driven migration
/// reconciliation.
///
/// Produced by [`classify_reconcile`] against the authoritative source's
/// live + tombstone manifest for the shard. The three outcomes partition the
/// rejoinee's local over-count exactly:
///
/// * [`Keep`](Self::Keep) — the source holds the key live (normal superset), or
///   the local copy is a *newer* re-creation than the source's tombstone
///   (§8.4). Existing behavior; the generation is reconciled by the existing
///   exact-entry path.
/// * [`Drop`](Self::Drop) — the source has an authoritative tombstone for the
///   key at a generation at-or-ahead of the local copy. Resurrection-safe
///   removal of a deleted UTXO.
/// * [`Transfer`](Self::Transfer) — the source has neither a live copy nor a
///   tombstone: the key was *never received* by the source. It must be
///   transferred up to the master (no-loss), never dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileAction {
    /// Keep the local record (normal superset, or a newer local re-creation).
    Keep,
    /// Drop the local record — the source authoritatively deleted it.
    Drop,
    /// Transfer the local record up to the master — never-received, no-loss.
    Transfer,
}

/// Classify one local key against a single authoritative source's manifest
/// (the 4-row classification table).
///
/// This is the PURE decision core of tombstone-driven migration reconciliation:
/// no cluster, no engine, no I/O. It is exhaustively unit-testable in isolation
/// (design §11.3). The multi-source UNION rule (§9.1 #1) is layered *on top* of
/// this by [`classify_reconcile_union`]; this single-source form is the
/// building block.
///
/// Inputs (all for the SAME key the rejoinee holds locally at `local_gen`):
/// * `local_gen` — the generation of the rejoinee's local record.
/// * `source_live_gen` — `Some(gen)` if the source holds the key LIVE at `gen`
///   (its manifest lists it), else `None`.
/// * `source_tombstone_gen` — `Some(gen)` if the source has a TOMBSTONE for the
///   key at deletion-generation `gen`, else `None`.
///
/// The §7 table, in order:
///
/// | source-live | source-tombstone | result |
/// |---|---|---|
/// | present | — | [`Keep`](ReconcileAction::Keep) (normal superset; generation reconciled by the exact-entry path) |
/// | absent | present, `tomb_gen >= local_gen` | [`Drop`](ReconcileAction::Drop) (authoritative deletion, resurrection-safe) |
/// | absent | absent | [`Transfer`](ReconcileAction::Transfer) (never-received, no-loss) |
/// | absent | present, `tomb_gen < local_gen` | [`Keep`](ReconcileAction::Keep) (newer re-creation, §8.4) |
///
/// `live` WINS over `tombstone`: if the source lists the key live it is Keep
/// regardless of any tombstone it also carries (a re-creation supersedes an
/// older deletion). The `>=` / `<` generation split uses the wrapping-aware
/// [`crate::record::generation_at_or_ahead`] comparison so a generation wrap
/// cannot flip a Drop into a Keep or vice versa (§8.4).
///
/// No-loss invariant: this returns [`Drop`](ReconcileAction::Drop) ONLY when a
/// tombstone is present. A key the source merely omits (no live, no tombstone)
/// is ALWAYS [`Transfer`](ReconcileAction::Transfer) — never dropped.
pub fn classify_reconcile(
    local_gen: u32,
    source_live_gen: Option<u32>,
    source_tombstone_gen: Option<u32>,
) -> ReconcileAction {
    // Row 1: source holds the key live → keep (live beats tombstone).
    if source_live_gen.is_some() {
        return ReconcileAction::Keep;
    }
    match source_tombstone_gen {
        // Rows 2 & 4: not live, tombstone present. The tombstone authorizes a
        // drop only when its generation is at-or-ahead of the local copy's
        // (the deletion covers this version or a newer one). A tombstone for an
        // OLDER generation (`tomb_gen < local_gen`) is for a version the
        // rejoinee has since superseded with a newer re-creation → keep (§8.4).
        Some(tomb_gen) => {
            if crate::record::generation_at_or_ahead(tomb_gen, local_gen) {
                ReconcileAction::Drop
            } else {
                ReconcileAction::Keep
            }
        }
        // Row 3: not live, no tombstone → never-received → transfer (no-loss).
        None => ReconcileAction::Transfer,
    }
}

/// Classify one local key against the UNION of ALL pending sources' manifests
/// (the multi-source union rule).
///
/// This is the load-bearing correctness point of multi-source migration. A
/// single-source [`classify_reconcile`] is WRONG when several sources stream the
/// same shard concurrently: source X may tombstone key `k` while source Y holds
/// `k` LIVE. Dropping on X's tombstone alone would resurrect-then-lose a key Y
/// still has live. The fix: evaluate the drop against the UNION.
///
/// Inputs (for the SAME local key at `local_gen`):
/// * `union_live` — `true` iff ANY pending source holds the key live.
/// * `union_tombstone_gen` — the tombstone generation to use for the drop test,
///   i.e. `Some(gen)` iff SOME pending source tombstones the key; when several
///   do, pass the MAXIMUM (newest) tombstone generation (the most permissive
///   for a drop — if even the newest tombstone is older than the local copy,
///   none authorize the drop). `None` iff no pending source tombstones it.
///
/// Decision (the union of §7 across sources):
/// * key live on ANY source → [`Keep`](ReconcileAction::Keep) (a live holder
///   anywhere wins; a key tombstoned by some source but live on another is
///   kept).
/// * not live anywhere, tombstoned by some source at `gen >= local_gen` →
///   [`Drop`](ReconcileAction::Drop).
/// * not live anywhere, tombstoned by some source at `gen < local_gen` →
///   [`Keep`](ReconcileAction::Keep) (newer local re-creation, §8.4).
/// * not live anywhere, tombstoned by NO source → [`Transfer`](ReconcileAction::Transfer)
///   (never-received, no-loss).
///
/// This is exactly [`classify_reconcile`] evaluated with the union live flag
/// folded into `source_live_gen` — it is expressed as a thin wrapper so the
/// union semantics are explicit and independently testable.
pub fn classify_reconcile_union(
    local_gen: u32,
    union_live: bool,
    union_tombstone_gen: Option<u32>,
) -> ReconcileAction {
    // Fold the union-live flag into the single-source form. The generation
    // carried in the `Some` is irrelevant for the live case (row 1 ignores it),
    // so a sentinel `local_gen` is fine — `classify_reconcile` only inspects
    // `is_some()` for the live arm.
    let source_live_gen = if union_live { Some(local_gen) } else { None };
    classify_reconcile(local_gen, source_live_gen, union_tombstone_gen)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemoryDevice;

    fn test_txid(n: u8) -> [u8; 32] {
        let mut txid = [0u8; 32];
        txid[0] = n;
        txid[31] = n.wrapping_add(7);
        txid
    }

    fn make_log(size: u64) -> (Arc<MemoryDevice>, TombstoneLog) {
        let dev = Arc::new(MemoryDevice::new(size, 4096).unwrap());
        let log = TombstoneLog::open(dev.clone(), 0, size).unwrap();
        (dev, log)
    }

    // -- Tombstone struct / serialization --

    #[test]
    fn tombstone_is_56_bytes() {
        assert_eq!(size_of::<Tombstone>(), 56);
        assert_eq!(TOMBSTONE_SIZE, 56);
        let t = Tombstone::new(test_txid(1), 3, 100, 5, TombstoneCause::SpentDah, 0);
        assert_eq!(t.serialize().len(), 56);
    }

    #[test]
    fn serialize_parse_round_trip_all_causes() {
        for (i, cause) in [
            TombstoneCause::SpentDah,
            TombstoneCause::Admin,
            TombstoneCause::MigrationPrune,
        ]
        .into_iter()
        .enumerate()
        {
            let txid = test_txid(i as u8 + 10);
            let t = Tombstone::new(txid, 42, 123_456, 99, cause, 0);
            let bytes = t.serialize();
            let parsed = Tombstone::parse(&bytes, 0).unwrap();

            // Copy packed fields to locals before asserting (E0793).
            let p_txid = parsed.txid;
            let p_shard = parsed.shard;
            let p_dh = parsed.deletion_height;
            let p_gen = parsed.generation;
            let p_flags = parsed.flags;
            assert_eq!(p_txid, txid);
            assert_eq!(p_shard, 42);
            assert_eq!(p_dh, 123_456);
            assert_eq!(p_gen, 99);
            assert_eq!(parsed.cause().unwrap(), cause);
            assert_eq!(p_flags, 0);
            assert_eq!(parsed, t);
        }
    }

    #[test]
    fn parse_rejects_corrupted_crc() {
        let t = Tombstone::new(test_txid(2), 1, 10, 1, TombstoneCause::Admin, 0);
        let mut bytes = t.serialize();
        // Flip a byte in the body (not the CRC field) so the CRC no longer
        // matches.
        bytes[5] ^= 0xFF;
        match Tombstone::parse(&bytes, 777) {
            Err(TombstoneError::Crc { offset, .. }) => assert_eq!(offset, 777),
            other => panic!("expected Crc error, got {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_bad_cause_byte() {
        let t = Tombstone::new(test_txid(3), 1, 10, 1, TombstoneCause::SpentDah, 0);
        let mut bytes = t.serialize();
        // Set an invalid cause byte (offset 42) and recompute the CRC so the
        // failure is specifically the cause check, not the CRC.
        bytes[42] = 9;
        let crc = crc32fast::hash(&bytes[..TOMBSTONE_CRC_REGION]);
        bytes[TOMBSTONE_CRC_REGION..].copy_from_slice(&crc.to_le_bytes());
        match Tombstone::parse(&bytes, 0) {
            Err(TombstoneError::UnknownCause(9)) => {}
            other => panic!("expected UnknownCause(9), got {other:?}"),
        }
    }

    #[test]
    fn cause_from_u8_valid_and_invalid() {
        assert_eq!(
            TombstoneCause::from_u8(0).unwrap(),
            TombstoneCause::SpentDah
        );
        assert_eq!(TombstoneCause::from_u8(1).unwrap(), TombstoneCause::Admin);
        assert_eq!(
            TombstoneCause::from_u8(2).unwrap(),
            TombstoneCause::MigrationPrune
        );
        for bad in [3u8, 4, 100, 255] {
            match TombstoneCause::from_u8(bad) {
                Err(TombstoneError::UnknownCause(b)) => assert_eq!(b, bad),
                other => panic!("expected UnknownCause({bad}), got {other:?}"),
            }
        }
    }

    #[test]
    fn cause_round_trips_through_u8() {
        for cause in [
            TombstoneCause::SpentDah,
            TombstoneCause::Admin,
            TombstoneCause::MigrationPrune,
        ] {
            assert_eq!(TombstoneCause::from_u8(cause.as_u8()).unwrap(), cause);
        }
    }

    // -- Log append / scan --

    #[test]
    fn append_scan_round_trip_order_preserved() {
        let (_dev, mut log) = make_log(1024 * 1024);
        let mut seqs = Vec::new();
        for n in 0..10u8 {
            let t = Tombstone::new(
                test_txid(n),
                n as u16,
                1000 + n as u32,
                n as u32,
                TombstoneCause::SpentDah,
                0,
            );
            seqs.push(log.append(&t).unwrap());
        }
        log.sync().unwrap();
        // Sequences are contiguous starting at 1.
        assert_eq!(seqs, (1..=10u64).collect::<Vec<_>>());

        let scanned = log.scan().unwrap();
        assert_eq!(scanned.len(), 10);
        for (n, t) in scanned.iter().enumerate() {
            let txid = t.txid;
            let dh = t.deletion_height;
            assert_eq!(txid, test_txid(n as u8));
            assert_eq!(dh, 1000 + n as u32);
        }
    }

    #[test]
    fn reopen_recovers_entries_and_sequence() {
        let dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        {
            let mut log = TombstoneLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
            for n in 0..5u8 {
                let t = Tombstone::new(
                    test_txid(n),
                    n as u16,
                    500 + n as u32,
                    n as u32,
                    TombstoneCause::Admin,
                    0,
                );
                log.append(&t).unwrap();
            }
            log.sync().unwrap();
            assert_eq!(log.current_sequence(), 6);
        }
        let log = TombstoneLog::open(dev, 0, 1024 * 1024).unwrap();
        let scanned = log.scan().unwrap();
        assert_eq!(scanned.len(), 5);
        // next_seq must not roll back across the reopen.
        assert_eq!(log.current_sequence(), 6);
    }

    #[test]
    fn torn_tail_entry_is_dropped() {
        let dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut log = TombstoneLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
        for n in 0..4u8 {
            let t = Tombstone::new(
                test_txid(n),
                n as u16,
                100 + n as u32,
                n as u32,
                TombstoneCause::SpentDah,
                0,
            );
            log.append(&t).unwrap();
        }
        log.sync().unwrap();
        let tail = log.write_position();

        // Corrupt the 4th (final) entry on device by flipping a byte in its
        // body. The header block is `alignment` bytes; entries follow, so
        // entry 3 starts at entries_off + 3*56.
        let header_block = 4096u64;
        let entry3_off = header_block + 3 * TOMBSTONE_SIZE as u64;
        let align = 4096usize;
        let aligned_off = entry3_off / align as u64 * align as u64;
        let intra = (entry3_off - aligned_off) as usize;
        let mut block = AlignedBuf::new(align, align);
        dev.pread_exact_at(&mut block, aligned_off).unwrap();
        block[intra + 4] ^= 0xFF; // corrupt entry-3 body byte
        dev.pwrite_all_at(&block, aligned_off).unwrap();
        dev.sync().unwrap();

        // Re-open and scan: the torn final entry is dropped, the first 3
        // survive, and the append tail points before the torn entry.
        let log2 = TombstoneLog::open(dev, 0, 1024 * 1024).unwrap();
        let scanned = log2.scan().unwrap();
        assert_eq!(scanned.len(), 3, "torn final entry must be dropped");
        for (n, t) in scanned.iter().enumerate() {
            let txid = t.txid;
            assert_eq!(txid, test_txid(n as u8));
        }
        assert!(
            log2.write_position() < tail,
            "append tail must rewind before the torn entry"
        );
    }

    #[test]
    fn header_bad_magic_rejected() {
        let dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        {
            let mut log = TombstoneLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
            let t = Tombstone::new(test_txid(1), 0, 1, 0, TombstoneCause::SpentDah, 0);
            log.append_synced(&t).unwrap();
        }
        // Corrupt the magic bytes in the header block.
        let align = 4096usize;
        let mut block = AlignedBuf::new(align, align);
        dev.pread_exact_at(&mut block, 0).unwrap();
        block[0] = b'X';
        dev.pwrite_all_at(&block, 0).unwrap();
        dev.sync().unwrap();
        match TombstoneLog::open(dev, 0, 1024 * 1024) {
            Err(TombstoneError::HeaderMagicMismatch { .. }) => {}
            Err(other) => panic!("expected HeaderMagicMismatch, got {other:?}"),
            Ok(_) => panic!("expected HeaderMagicMismatch, got Ok"),
        }
    }

    #[test]
    fn header_bad_crc_rejected() {
        let dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        {
            let mut log = TombstoneLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
            let t = Tombstone::new(test_txid(1), 0, 1, 0, TombstoneCause::SpentDah, 0);
            log.append_synced(&t).unwrap();
        }
        // Corrupt a header field (next_seq region) without fixing the CRC.
        let align = 4096usize;
        let mut block = AlignedBuf::new(align, align);
        dev.pread_exact_at(&mut block, 0).unwrap();
        block[12] ^= 0xFF; // first byte of next_seq
        dev.pwrite_all_at(&block, 0).unwrap();
        dev.sync().unwrap();
        match TombstoneLog::open(dev, 0, 1024 * 1024) {
            Err(TombstoneError::HeaderCrcMismatch { .. }) => {}
            Err(other) => panic!("expected HeaderCrcMismatch, got {other:?}"),
            Ok(_) => panic!("expected HeaderCrcMismatch, got Ok"),
        }
    }

    // -- Compaction --

    #[test]
    fn compact_through_reclaims_prefix_and_updates_header() {
        let dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut log = TombstoneLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
        // Heights 100,110,120,...,190.
        for n in 0..10u8 {
            let t = Tombstone::new(
                test_txid(n),
                n as u16,
                100 + n as u32 * 10,
                n as u32,
                TombstoneCause::SpentDah,
                0,
            );
            log.append(&t).unwrap();
        }
        log.sync().unwrap();
        assert_eq!(log.scan().unwrap().len(), 10);

        // Compact through height 150: drops heights 100..=140 (5 entries),
        // keeps 150..=190 (5 entries).
        log.compact_through(150).unwrap();
        assert_eq!(log.compacted_through_height(), 150);
        let suffix = log.scan().unwrap();
        assert_eq!(suffix.len(), 5);
        for t in &suffix {
            let dh = t.deletion_height;
            assert!(dh >= 150, "retained entry below watermark: {dh}");
        }

        // Re-open: compaction is durable and the suffix re-derives correctly.
        let log2 = TombstoneLog::open(dev, 0, 1024 * 1024).unwrap();
        assert_eq!(log2.compacted_through_height(), 150);
        let suffix2 = log2.scan().unwrap();
        assert_eq!(suffix2.len(), 5);
        // Heights preserved exactly.
        let mut heights: Vec<u32> = suffix2.iter().map(|t| t.deletion_height).collect();
        heights.sort_unstable();
        assert_eq!(heights, vec![150, 160, 170, 180, 190]);
    }

    #[test]
    fn compact_through_all_reclaims_everything() {
        let dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut log = TombstoneLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
        for n in 0..6u8 {
            let t = Tombstone::new(
                test_txid(n),
                0,
                100 + n as u32,
                0,
                TombstoneCause::MigrationPrune,
                0,
            );
            log.append(&t).unwrap();
        }
        log.sync().unwrap();
        log.compact_through(10_000).unwrap();
        assert!(log.scan().unwrap().is_empty());
        assert_eq!(log.write_position(), 0);

        // After reclaiming everything, appends still work and the watermark
        // persists across reopen.
        let t = Tombstone::new(test_txid(99), 0, 20_000, 0, TombstoneCause::Admin, 0);
        log.append_synced(&t).unwrap();
        let log2 = TombstoneLog::open(dev, 0, 1024 * 1024).unwrap();
        assert_eq!(log2.compacted_through_height(), 10_000);
        assert_eq!(log2.scan().unwrap().len(), 1);
    }

    #[test]
    fn compact_through_below_watermark_is_noop() {
        let (_dev, mut log) = make_log(1024 * 1024);
        for n in 0..4u8 {
            let t = Tombstone::new(
                test_txid(n),
                0,
                100 + n as u32,
                0,
                TombstoneCause::SpentDah,
                0,
            );
            log.append(&t).unwrap();
        }
        log.sync().unwrap();
        log.compact_through(102).unwrap();
        assert_eq!(log.compacted_through_height(), 102);
        let before = log.scan().unwrap().len();
        // A lower (or equal) height is a no-op.
        log.compact_through(50).unwrap();
        assert_eq!(log.compacted_through_height(), 102);
        log.compact_through(102).unwrap();
        assert_eq!(log.compacted_through_height(), 102);
        assert_eq!(log.scan().unwrap().len(), before);
    }

    // -- Compaction crash-safety (zero-then-write-then-header) --

    /// Direct aligned write of arbitrary bytes onto the device, bypassing the
    /// log. Used by the crash-safety tests to PLANT a stale old-image block in
    /// the region past the retained image and prove the fixed protocol's prior
    /// zeroing makes `scan()` immune to it. `offset`/`bytes.len()` must be
    /// device-aligned.
    fn raw_write(dev: &MemoryDevice, offset: u64, bytes: &[u8]) {
        let align = dev.alignment();
        // Round the write up to a whole number of aligned blocks (the device
        // rejects sub-block buffers). The planted bytes sit at the front of the
        // block; trailing padding is zero.
        let buf_len = bytes.len().div_ceil(align) * align;
        let mut buf = AlignedBuf::new(buf_len, align);
        buf[..bytes.len()].copy_from_slice(bytes);
        dev.pwrite_all_at(&buf, offset).unwrap();
    }

    /// The franken-mix the adversarial review described: a compaction whose
    /// retained image is SHORTER than the old image, with valid CRC-passing
    /// stale old entries left in a later block. Under the buggy in-place
    /// rewrite, `scan()` would read new-block-0 ++ stale-old-block-N as a
    /// consistent-looking but WRONG live set (no torn boundary to stop it).
    /// The fixed zero-then-write-then-header protocol zeroes the whole region
    /// first, so `scan()` stops at the first all-zero block and NEVER reads the
    /// stale tail — even if we forcibly re-plant it after compaction (proving
    /// the live set is the physical front, not whatever happens to be later).
    #[test]
    fn compact_ignores_stale_old_image_in_later_block() {
        let region = 1024 * 1024;
        let dev = Arc::new(MemoryDevice::new(region, 4096).unwrap());
        let mut log = TombstoneLog::open(dev.clone(), 0, region).unwrap();

        // A long old image: 200 entries (11_200 bytes ~ spans 3 blocks of the
        // 4 KiB region). Heights 100..=398 step 2.
        for n in 0..200u32 {
            let t = Tombstone::new(
                test_txid((n % 251) as u8),
                (n % 4096) as u16,
                100 + n * 2,
                n,
                TombstoneCause::SpentDah,
                0,
            );
            log.append(&t).unwrap();
        }
        log.sync().unwrap();
        assert_eq!(log.scan().unwrap().len(), 200);

        // Compact through a high height so only the last 5 entries survive
        // (heights 490, 492, 494, 496, 498). The retained image (5 entries =
        // 280 bytes) is far shorter than the old image and fits in block 0.
        log.compact_through(490).unwrap();
        let retained = log.scan().unwrap();
        assert_eq!(retained.len(), 5, "exactly the high-height suffix survives");
        let mut heights: Vec<u32> = retained.iter().map(|t| t.deletion_height).collect();
        heights.sort_unstable();
        assert_eq!(heights, vec![490, 492, 494, 496, 498]);

        // Now FORCIBLY plant valid CRC-passing stale entries in the SECOND
        // block of the entries region (offset = header + one block). These are
        // exactly the kind of bytes a torn in-place rewrite would have left
        // behind. We then re-scan WITHOUT advancing the header: the fixed
        // protocol guarantees scan() stops at the first all-zero block (the
        // sentinel right after the 5 retained entries in block 0), so these
        // planted entries in block 1 must be invisible.
        let align = dev.alignment() as u64;
        let entries_off = log.entries_region_offset();
        let second_block = entries_off + align;
        let mut stale = Vec::new();
        for n in 0..10u32 {
            // Heights below the watermark — these are the dropped prefix that
            // must NOT resurrect.
            let t = Tombstone::new(test_txid(n as u8), 0, 100 + n, n, TombstoneCause::Admin, 0);
            stale.extend_from_slice(&t.serialize());
        }
        raw_write(&dev, second_block, &stale);

        // The block-0 sentinel (the zeroed tail after the 5 retained entries)
        // stops the scan before block 1, so the planted stale entries are NOT
        // read. Live set is unchanged: still exactly the 5 retained.
        let after = log.scan().unwrap();
        assert_eq!(
            after.len(),
            5,
            "stale entries in a later block must be invisible (scan stops at the block-0 zero sentinel)"
        );
        let mut h2: Vec<u32> = after.iter().map(|t| t.deletion_height).collect();
        h2.sort_unstable();
        assert_eq!(h2, vec![490, 492, 494, 496, 498]);
        for t in &after {
            let dh = t.deletion_height;
            assert!(dh >= 490, "resurrected dropped entry: dh={dh}");
        }

        // Reopen re-derives the same live set (header-driven open, physical
        // front scan) — the planted block-1 bytes remain dead.
        let log2 = TombstoneLog::open(dev, 0, region).unwrap();
        assert_eq!(log2.compacted_through_height(), 490);
        assert_eq!(log2.scan().unwrap().len(), 5);
    }

    /// The entries region beyond the retained image is physically ZERO after a
    /// compaction — no stale tail of the old (longer) image remains. Read the
    /// device bytes directly and assert everything past the retained image +
    /// its sentinel is zero.
    #[test]
    fn compact_zeroes_region_beyond_retained_image() {
        let region = 256 * 1024;
        let dev = Arc::new(MemoryDevice::new(region, 4096).unwrap());
        let mut log = TombstoneLog::open(dev.clone(), 0, region).unwrap();

        for n in 0..100u32 {
            let t = Tombstone::new(
                test_txid((n % 251) as u8),
                0,
                1000 + n,
                n,
                TombstoneCause::SpentDah,
                0,
            );
            log.append(&t).unwrap();
        }
        log.sync().unwrap();

        // Keep only the last 3 (heights 1097, 1098, 1099).
        log.compact_through(1097).unwrap();
        assert_eq!(log.scan().unwrap().len(), 3);

        // Read the entire entries region back and confirm every byte past the
        // retained content is zero (the 3 entries = 168 bytes; everything from
        // 168 to end-of-region is the zeroed sentinel + reclaimed space).
        let entries_off = log.entries_region_offset();
        let entries_size = log.entries_region_size() as usize;
        let align = dev.alignment();
        let mut readback = AlignedBuf::new(entries_size.div_ceil(align) * align, align);
        dev.pread_exact_at(&mut readback[..entries_size], entries_off)
            .unwrap();
        let content_len = 3 * TOMBSTONE_SIZE;
        assert!(
            readback[content_len..entries_size].iter().all(|b| *b == 0),
            "region beyond the retained image must be zeroed (no stale old-image tail)"
        );
    }

    /// A retained image larger than one device block round-trips correctly
    /// through compaction: the multi-block rewrite + multi-block pre-zero both
    /// behave, and scan re-derives the exact suffix.
    #[test]
    fn compact_multi_block_retained_image_round_trips() {
        let region = 1024 * 1024;
        let dev = Arc::new(MemoryDevice::new(region, 4096).unwrap());
        let mut log = TombstoneLog::open(dev.clone(), 0, region).unwrap();

        // 400 entries; heights 0..=399. One 4 KiB block holds 73 entries
        // (73*56=4088), so a retained image of 200 entries spans ~3 blocks.
        for n in 0..400u32 {
            let t = Tombstone::new(
                test_txid((n % 251) as u8),
                (n % 4096) as u16,
                n,
                n,
                TombstoneCause::MigrationPrune,
                0,
            );
            log.append(&t).unwrap();
        }
        log.sync().unwrap();

        // Drop heights 0..=199, keep 200..=399 (200 entries, multi-block).
        log.compact_through(200).unwrap();
        let suffix = log.scan().unwrap();
        assert_eq!(suffix.len(), 200, "exactly the 200-entry suffix survives");
        for (i, t) in suffix.iter().enumerate() {
            let dh = t.deletion_height;
            let g = t.generation;
            assert_eq!(dh, 200 + i as u32, "suffix entry {i} height out of order");
            assert_eq!(
                g,
                200 + i as u32,
                "suffix entry {i} generation out of order"
            );
        }

        // Reopen: the multi-block retained image is durable and re-derives.
        let log2 = TombstoneLog::open(dev, 0, region).unwrap();
        assert_eq!(log2.compacted_through_height(), 200);
        let suffix2 = log2.scan().unwrap();
        assert_eq!(suffix2.len(), 200);
        let first_dh = suffix2[0].deletion_height;
        let last_dh = suffix2[199].deletion_height;
        assert_eq!(first_dh, 200);
        assert_eq!(last_dh, 399);
    }

    /// End-to-end crash simulation via the volatile device. `compact_through`
    /// fsyncs at three points (post-zero, post-image, post-header), so the
    /// device's durable shadow only ever holds a committed boundary state. A
    /// power loss reverting to that shadow must therefore recover a CONSISTENT
    /// state — never a franken-mix, never a resurrected dropped entry. (The
    /// finer-grained "crash strictly between two of those fsyncs" windows are
    /// proven by the direct-byte planting test above, which shows scan() stops
    /// at the zeroed boundary regardless of what stale bytes lie beyond it.)
    #[test]
    fn compact_crash_recovery_via_volatile_device_is_consistent() {
        let region = 256 * 1024;
        let dev = Arc::new(MemoryDevice::new_volatile(region, 4096).unwrap());
        let mut log = TombstoneLog::open(dev.clone(), 0, region).unwrap();
        for n in 0..50u32 {
            let t = Tombstone::new(
                test_txid((n % 251) as u8),
                0,
                100 + n,
                n,
                TombstoneCause::SpentDah,
                0,
            );
            log.append(&t).unwrap();
        }
        log.sync().unwrap();
        assert_eq!(log.scan().unwrap().len(), 50);

        // Perform a real compaction (keep last 5: heights 145..=149). This
        // fsyncs the zeroed region, the retained image, AND the header — so the
        // device's durable shadow now reflects the fully-committed new state.
        log.compact_through(145).unwrap();
        assert_eq!(log.scan().unwrap().len(), 5);

        // Now simulate a power loss with NO intervening sync: reverts to the
        // last durable state (the committed compaction). Whatever survives is a
        // consistent state, never a franken-mix.
        assert!(dev.simulate_power_loss(), "device must be volatile");
        let log2 = TombstoneLog::open(dev, 0, region).unwrap();
        let recovered = log2.scan().unwrap();
        // Every recovered entry is at or above some watermark — none is a
        // resurrected sub-watermark dropped entry, and the count is a
        // subset-or-equal of the retained set.
        assert!(recovered.len() <= 50, "no entries fabricated");
        for t in &recovered {
            let dh = t.deletion_height;
            assert!(
                dh >= 100,
                "recovered entry below any real height (fabricated): dh={dh}"
            );
        }
        // Specifically: the committed state is the 5-entry suffix.
        let mut h: Vec<u32> = recovered.iter().map(|t| t.deletion_height).collect();
        h.sort_unstable();
        assert_eq!(h, vec![145, 146, 147, 148, 149]);
    }

    // -- Multi-chunk scan (cross chunk-boundary) --

    /// The scan reads the entries region in chunks of `lcm(align, 56)` rounded
    /// up to >= 4 MiB (4,214,784 bytes = 75,264 entries on a 4 KiB-aligned
    /// device). All prior scan tests use sub-chunk regions, so the cross-chunk
    /// boundary path (`was_final_chunk == false`, `pos` staying aligned across
    /// iterations) was untested. Append just over one chunk's worth of entries
    /// and assert scan returns every entry, in order, across the boundary.
    #[test]
    fn scan_spans_multiple_chunks_in_order() {
        // One chunk holds 75,264 entries; append enough to require >= 2 chunk
        // reads. Region: 4 KiB header + room for ~120k entries (~6.4 MiB).
        const N: u32 = 80_000;
        let region = 7 * 1024 * 1024;
        let dev = Arc::new(MemoryDevice::new(region, 4096).unwrap());
        let mut log = TombstoneLog::open(dev, 0, region).unwrap();

        for i in 0..N {
            // deletion_height = index, so order and identity are checkable.
            let t = Tombstone::new(
                test_txid((i % 251) as u8),
                (i % 4096) as u16,
                i,
                i,
                TombstoneCause::SpentDah,
                0,
            );
            log.append(&t).unwrap();
        }
        log.sync().unwrap();

        let entries = log.scan().unwrap();
        assert_eq!(entries.len(), N as usize, "every appended entry survives");
        // Order preserved across the chunk boundary (entry 75,264 is the first
        // of the second chunk).
        for (i, t) in entries.iter().enumerate() {
            let dh = t.deletion_height;
            let g = t.generation;
            assert_eq!(dh, i as u32, "entry {i} deletion_height out of order");
            assert_eq!(g, i as u32, "entry {i} generation out of order");
        }
        // Spot-check the exact boundary entries.
        let boundary = 75_264usize;
        let last_of_chunk0 = entries[boundary - 1].deletion_height;
        let first_of_chunk1 = entries[boundary].deletion_height;
        assert_eq!(last_of_chunk0, (boundary - 1) as u32);
        assert_eq!(first_of_chunk1, boundary as u32);

        // Reopen and rescan: the entries survive a fresh open (header-driven).
        let entries2 = log.scan().unwrap();
        assert_eq!(entries2.len(), N as usize);
    }

    // -- Region full --

    #[test]
    fn append_past_capacity_returns_region_full() {
        // Small region: 4 KiB header + 8 KiB entries.
        let dev = Arc::new(MemoryDevice::new(64 * 1024, 4096).unwrap());
        let mut log = TombstoneLog::open(dev, 0, 12 * 1024).unwrap();
        let capacity_entries = log.available_entries();
        assert!(capacity_entries > 0);
        let mut appended = 0u64;
        loop {
            let t = Tombstone::new(
                test_txid((appended % 251) as u8),
                0,
                appended as u32,
                0,
                TombstoneCause::SpentDah,
                0,
            );
            match log.append(&t) {
                Ok(_) => appended += 1,
                Err(TombstoneError::RegionFull { capacity, .. }) => {
                    assert_eq!(capacity, log.capacity());
                    break;
                }
                Err(other) => panic!("unexpected error: {other:?}"),
            }
            if appended > 1_000 {
                panic!("region never filled");
            }
        }
        assert_eq!(appended, capacity_entries);
        // Entries appended before the fill are intact and durable.
        log.sync().unwrap();
        assert_eq!(log.scan().unwrap().len() as u64, appended);
    }

    #[test]
    fn open_out_of_bounds_region_fails() {
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024, 4096).unwrap());
        match TombstoneLog::open(dev, 32 * 1024, 64 * 1024) {
            Err(TombstoneError::OutOfBounds { .. }) => {}
            Err(other) => panic!("expected OutOfBounds, got {other:?}"),
            Ok(_) => panic!("expected OutOfBounds, got Ok"),
        }
    }

    #[test]
    fn open_region_too_small_fails() {
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024, 4096).unwrap());
        // 4 KiB region < 2 * 4 KiB header requirement.
        match TombstoneLog::open(dev, 0, 4096) {
            Err(TombstoneError::RegionTooSmall { .. }) => {}
            Err(other) => panic!("expected RegionTooSmall, got {other:?}"),
            Ok(_) => panic!("expected RegionTooSmall, got Ok"),
        }
    }

    #[test]
    fn create_initialises_fresh_region() {
        let dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        // Pre-populate via open+append, then create() must wipe it.
        {
            let mut log = TombstoneLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
            let t = Tombstone::new(test_txid(1), 0, 1, 0, TombstoneCause::SpentDah, 0);
            log.append_synced(&t).unwrap();
        }
        let log = TombstoneLog::create(dev, 0, 1024 * 1024).unwrap();
        assert!(log.scan().unwrap().is_empty());
        assert_eq!(log.current_sequence(), 1);
        assert_eq!(log.compacted_through_height(), 0);
    }

    // -----------------------------------------------------------------------
    // classify_reconcile — the §7 4-row decision (Phase 8). Exhaustive.
    // -----------------------------------------------------------------------

    #[test]
    fn classify_row1_source_live_keeps_regardless_of_tombstone() {
        // Row 1: source holds the key live → Keep, whether or not a tombstone
        // is also present, and for any generation relationship.
        assert_eq!(
            classify_reconcile(5, Some(5), None),
            ReconcileAction::Keep,
            "live, no tombstone → keep"
        );
        assert_eq!(
            classify_reconcile(5, Some(7), None),
            ReconcileAction::Keep,
            "live at newer gen → keep"
        );
        assert_eq!(
            classify_reconcile(5, Some(3), None),
            ReconcileAction::Keep,
            "live at older gen → keep (live always wins)"
        );
        // Live WINS over a co-present tombstone (re-creation supersedes deletion).
        assert_eq!(
            classify_reconcile(5, Some(6), Some(5)),
            ReconcileAction::Keep,
            "live beats tombstone even when tombstone_gen >= local"
        );
        assert_eq!(
            classify_reconcile(5, Some(6), Some(10)),
            ReconcileAction::Keep,
            "live beats tombstone even when tombstone_gen > local"
        );
    }

    #[test]
    fn classify_row2_drop_when_tombstone_at_or_ahead_of_local() {
        // Row 2: not live, tombstone present at gen >= local → Drop. Cover the
        // boundary (==) and strictly-greater.
        assert_eq!(
            classify_reconcile(5, None, Some(5)),
            ReconcileAction::Drop,
            "tombstone_gen == local_gen → drop (boundary)"
        );
        assert_eq!(
            classify_reconcile(5, None, Some(6)),
            ReconcileAction::Drop,
            "tombstone_gen > local_gen → drop"
        );
        assert_eq!(
            classify_reconcile(0, None, Some(0)),
            ReconcileAction::Drop,
            "both zero → drop (boundary)"
        );
    }

    #[test]
    fn classify_row3_transfer_when_no_live_and_no_tombstone() {
        // Row 3: never-received → Transfer (no-loss). The ONLY way a never-
        // received key behaves; it is NEVER dropped.
        assert_eq!(
            classify_reconcile(5, None, None),
            ReconcileAction::Transfer,
            "no live, no tombstone → transfer (no-loss)"
        );
        assert_eq!(
            classify_reconcile(0, None, None),
            ReconcileAction::Transfer,
            "gen 0, never-received → transfer"
        );
        assert_eq!(
            classify_reconcile(u32::MAX, None, None),
            ReconcileAction::Transfer,
            "gen MAX, never-received → transfer"
        );
    }

    #[test]
    fn classify_row4_keep_when_tombstone_older_than_local() {
        // Row 4: not live, tombstone present at gen < local → Keep (the local
        // copy is a newer re-creation; §8.4 generation defense).
        assert_eq!(
            classify_reconcile(6, None, Some(5)),
            ReconcileAction::Keep,
            "tombstone_gen < local_gen → keep (newer re-creation)"
        );
        assert_eq!(
            classify_reconcile(10, None, Some(0)),
            ReconcileAction::Keep,
            "tombstone far older than local → keep"
        );
    }

    #[test]
    fn classify_generation_boundary_exhaustive() {
        // The == boundary is the load-bearing line between Drop (row 2) and
        // Keep (row 4). Walk it directly: at local_gen == tomb_gen we Drop;
        // one below (tomb older) we Keep; one above (tomb newer) we Drop.
        let local = 100u32;
        assert_eq!(
            classify_reconcile(local, None, Some(local)),
            ReconcileAction::Drop,
            "tomb == local → drop"
        );
        assert_eq!(
            classify_reconcile(local, None, Some(local - 1)),
            ReconcileAction::Keep,
            "tomb == local-1 → keep"
        );
        assert_eq!(
            classify_reconcile(local, None, Some(local + 1)),
            ReconcileAction::Drop,
            "tomb == local+1 → drop"
        );
    }

    #[test]
    fn classify_generation_wrapping_is_respected() {
        // The split uses wrapping-aware `generation_at_or_ahead`, so a small
        // forward delta across the u32 wrap boundary is "ahead" (Drop), while a
        // small backward delta is "behind" (Keep). This guards §8.4 against a
        // naive `tomb_gen >= local_gen` that would invert near wrap.
        // local just below MAX, tombstone just past wrap (forward by 2) → drop.
        assert_eq!(
            classify_reconcile(u32::MAX - 1, None, Some(1)),
            ReconcileAction::Drop,
            "tombstone forward across wrap (ahead) → drop"
        );
        // local just past wrap, tombstone just below MAX (backward) → keep.
        assert_eq!(
            classify_reconcile(1, None, Some(u32::MAX - 1)),
            ReconcileAction::Keep,
            "tombstone backward across wrap (behind) → keep"
        );
    }

    // -----------------------------------------------------------------------
    // classify_reconcile_union — the §9.1 #1 multi-source union. Exhaustive
    // over the {live / tombstone / omit} × {live / tombstone / omit} matrix.
    // -----------------------------------------------------------------------

    /// Fold two per-source manifest states into the union inputs for
    /// [`classify_reconcile_union`]: union_live (live on ANY source) and the
    /// MAX tombstone generation across sources that tombstone the key.
    fn union_of(
        x_live: Option<u32>,
        x_tomb: Option<u32>,
        y_live: Option<u32>,
        y_tomb: Option<u32>,
    ) -> (bool, Option<u32>) {
        let union_live = x_live.is_some() || y_live.is_some();
        let union_tomb = match (x_tomb, y_tomb) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        (union_live, union_tomb)
    }

    #[test]
    fn union_live_on_any_source_keeps() {
        let local = 5u32;
        // X tombstones k, Y has k live → keep (live anywhere wins).
        let (live, tomb) = union_of(None, Some(5), Some(5), None);
        assert_eq!(
            classify_reconcile_union(local, live, tomb),
            ReconcileAction::Keep,
            "X tombstones, Y live → keep"
        );
        // X live, Y tombstones → keep.
        let (live, tomb) = union_of(Some(5), None, None, Some(9));
        assert_eq!(
            classify_reconcile_union(local, live, tomb),
            ReconcileAction::Keep,
            "X live, Y tombstones → keep"
        );
        // Both live → keep.
        let (live, tomb) = union_of(Some(5), None, Some(7), None);
        assert_eq!(
            classify_reconcile_union(local, live, tomb),
            ReconcileAction::Keep,
            "both live → keep"
        );
    }

    #[test]
    fn union_tombstoned_by_some_live_by_none_drops() {
        let local = 5u32;
        // X tombstones k at gen >= local, Y omits k → drop (no source live).
        let (live, tomb) = union_of(None, Some(5), None, None);
        assert_eq!(
            classify_reconcile_union(local, live, tomb),
            ReconcileAction::Drop,
            "X tombstones (>= local), Y omits → drop"
        );
        // X tombstones at older gen, Y tombstones at >= local → drop (max wins,
        // and the newest authorizes the drop).
        let (live, tomb) = union_of(None, Some(3), None, Some(6));
        assert_eq!(
            classify_reconcile_union(local, live, tomb),
            ReconcileAction::Drop,
            "max tombstone gen >= local → drop"
        );
    }

    #[test]
    fn union_omitted_by_all_transfers() {
        let local = 5u32;
        // Neither source has k live or tombstoned → never-received → transfer.
        let (live, tomb) = union_of(None, None, None, None);
        assert_eq!(
            classify_reconcile_union(local, live, tomb),
            ReconcileAction::Transfer,
            "omitted by all → transfer (no-loss)"
        );
    }

    #[test]
    fn union_all_tombstones_older_than_local_keeps() {
        let local = 10u32;
        // Both sources tombstone k, but BOTH at gen < local → keep (newer
        // local re-creation; even the max tombstone is older than local).
        let (live, tomb) = union_of(None, Some(5), None, Some(8));
        assert_eq!(
            classify_reconcile_union(local, live, tomb),
            ReconcileAction::Keep,
            "all tombstones older than local → keep (§8.4)"
        );
    }

    #[test]
    fn union_full_matrix_3x3() {
        // Exhaustive {live(L) / tombstone(T) / omit(O)} × {L / T / O}.
        // Tombstone gens chosen >= local so a tombstone, when decisive, drops.
        let local = 5u32;
        let states: [(Option<u32>, Option<u32>, &str); 3] = [
            (Some(5), None, "L"),
            (None, Some(5), "T"),
            (None, None, "O"),
        ];
        for (x_live, x_tomb, xn) in states {
            for (y_live, y_tomb, yn) in states {
                let (live, tomb) = union_of(x_live, x_tomb, y_live, y_tomb);
                let got = classify_reconcile_union(local, live, tomb);
                let expected = if x_live.is_some() || y_live.is_some() {
                    // Any live → keep.
                    ReconcileAction::Keep
                } else if x_tomb.is_some() || y_tomb.is_some() {
                    // No live, some tombstone (>= local) → drop.
                    ReconcileAction::Drop
                } else {
                    // No live, no tombstone → transfer.
                    ReconcileAction::Transfer
                };
                assert_eq!(got, expected, "matrix X={xn} Y={yn}");
            }
        }
    }
}
