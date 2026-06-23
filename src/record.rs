//! On-disk record layout types for TeraSlab.
//!
//! All structures are `#[repr(C, packed)]` with compile-time size assertions
//! to guarantee a stable, known byte layout on NVMe devices.
//!
//! # Record integrity
//!
//! [`TxMetadata`] embeds a CRC32 checksum computed over the entire 320-byte
//! header (with the checksum slot zeroed during computation). Each
//! [`UtxoSlot`] also stores a CRC32 over its logical 69-byte payload. Every
//! write recomputes the relevant CRC and every read validates it, returning
//! [`RecordError::CrcMismatch`] on disagreement. This guards against torn
//! writes, bit-rot, and partial-sector updates that would otherwise silently
//! corrupt fields such as `utxo_count`, `spent_utxos`, slot status, or
//! slot `spending_data`.

use bitflags::bitflags;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Logical payload size of a single UTXO slot before the checksum footer.
pub const UTXO_SLOT_PAYLOAD_SIZE: usize = 69;

/// Byte offset of the UTXO slot CRC32 footer within the on-disk slot.
pub const UTXO_SLOT_CRC32_OFFSET: usize = UTXO_SLOT_PAYLOAD_SIZE;

/// Size of a single UTXO slot on disk, including the CRC32 footer.
pub const UTXO_SLOT_SIZE: usize = UTXO_SLOT_PAYLOAD_SIZE + 4;

/// Size of a single block entry in bytes.
pub const BLOCK_ENTRY_SIZE: usize = 12;

/// Number of block entries stored inline in metadata.
pub const INLINE_BLOCK_ENTRIES: usize = 3;

/// Magic number identifying a valid TeraSlab record ("SLAB" in ASCII).
pub const METADATA_MAGIC: u32 = 0x534C_4142;

/// Current schema version.
pub const METADATA_VERSION: u32 = 2;

/// UTXO status: the output is unspent and available.
pub const UTXO_UNSPENT: u8 = 0x00;

/// UTXO status: the output has been spent.
pub const UTXO_SPENT: u8 = 0x01;

/// UTXO status: the child transaction was pruned/deleted (terminal).
pub const UTXO_PRUNED: u8 = 0x02;

/// UTXO status: the output is frozen (all spending_data bytes are 0xFF).
pub const UTXO_FROZEN: u8 = 0xFF;

/// Byte value used to fill spending_data when a UTXO is frozen.
pub const FROZEN_BYTE: u8 = 0xFF;

// Compute padding so TxMetadata is a multiple of 64 bytes.
const RAW_METADATA_SIZE: usize = std::mem::size_of::<TxMetadataRaw>();
const METADATA_PADDING_AMOUNT: usize = if RAW_METADATA_SIZE.is_multiple_of(64) {
    0
} else {
    64 - (RAW_METADATA_SIZE % 64)
};

/// Total size of the metadata header (padded to a 64-byte boundary).
pub const METADATA_SIZE: usize = RAW_METADATA_SIZE + METADATA_PADDING_AMOUNT;

/// Amount of padding bytes appended to reach the 64-byte boundary.
pub const METADATA_PADDING: usize = METADATA_PADDING_AMOUNT;

// ---------------------------------------------------------------------------
// RecordError
// ---------------------------------------------------------------------------

/// Errors produced when parsing on-disk record structures.
///
/// CRC32 checksum mismatches on [`TxMetadata`] or [`UtxoSlot`]
/// deserialization indicate disk corruption, a torn write, or a
/// partial-sector update. Callers must propagate this error — silent
/// acceptance of corrupted record bytes is unsafe for a UTXO store.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum RecordError {
    /// The CRC32 checksum stored in the record header did not match the
    /// checksum computed over the header bytes (with the checksum slot
    /// zeroed during computation).
    #[error("CRC mismatch: expected 0x{expected:08X}, actual 0x{actual:08X}")]
    CrcMismatch {
        /// CRC value read from the on-disk header.
        expected: u32,
        /// CRC value computed over the header bytes on read.
        actual: u32,
    },
    /// The source slice was shorter than [`METADATA_SIZE`], so a full header
    /// could not be read. Returned by [`TxMetadata::from_bytes`] instead of
    /// risking an out-of-bounds slice panic on a truncated/torn read.
    #[error("metadata slice too short: have {actual} bytes, need {required}")]
    TooShort {
        /// Number of bytes actually available in the source slice.
        actual: usize,
        /// Minimum number of bytes required (`METADATA_SIZE`).
        required: usize,
    },
}

// ---------------------------------------------------------------------------
// UtxoSlot
// ---------------------------------------------------------------------------

/// A single UTXO output slot on disk.
///
/// Fixed at 73 bytes on disk: 69 logical bytes plus a 4-byte CRC32 footer.
/// Always pre-allocated at full size from creation, even when unspent, so
/// the record never grows on spend (eliminating copy-on-write).
///
/// # spending_data interpretation by status
///
/// | Status   | spending_data content                              |
/// |----------|----------------------------------------------------|
/// | Unspent  | `[spendable_height:4 LE][zeros:32]` (0 = immediate)|
/// | Spent    | `[txid:32][vin:4 LE]`                              |
/// | Pruned   | Preserved from last spend (audit trail)             |
/// | Frozen   | All 0xFF (36 bytes)                                |
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct UtxoSlot {
    /// UTXO hash (always present, set at creation).
    pub hash: [u8; 32],
    /// Status byte — see `UTXO_UNSPENT`, `UTXO_SPENT`, `UTXO_PRUNED`, `UTXO_FROZEN`.
    pub status: u8,
    /// Multi-purpose 36-byte field; interpretation depends on `status`.
    pub spending_data: [u8; 36],
}

impl UtxoSlot {
    /// Create a new unspent UTXO slot with the given hash.
    pub fn new_unspent(hash: [u8; 32]) -> Self {
        Self {
            hash,
            status: UTXO_UNSPENT,
            spending_data: [0u8; 36],
        }
    }

    /// Create a new frozen UTXO slot with the given hash.
    pub fn new_frozen(hash: [u8; 32]) -> Self {
        Self {
            hash,
            status: UTXO_FROZEN,
            spending_data: [FROZEN_BYTE; 36],
        }
    }

    /// Create a frozen UTXO slot that preserves a reassignment cooldown (LP-4).
    ///
    /// The frozen state is carried by the `status` byte (`UTXO_FROZEN`), which
    /// is the authoritative frozen signal everywhere in the engine
    /// ([`Self::is_frozen`] checks `status`, and the spend path matches the
    /// `UTXO_FROZEN` status arm). The reassignment cooldown
    /// (`block_height + spendable_after`, spec §2.4) normally lives in
    /// `spending_data[0..4]` of an *unspent* slot; a plain
    /// [`Self::new_frozen`] overwrites those 4 bytes with the all-`0xFF`
    /// marker, silently erasing the cooldown so a later `unfreeze` makes the
    /// court-ordered reassigned output immediately spendable — bypassing the
    /// safety window.
    ///
    /// This constructor keeps the 4-byte cooldown in `spending_data[0..4]` and
    /// fills the remaining 32 bytes with the frozen marker, so the cooldown
    /// survives a freeze/unfreeze round-trip. When `cooldown == 0` the result
    /// is byte-identical to [`Self::new_frozen`].
    pub fn new_frozen_with_cooldown(hash: [u8; 32], cooldown: u32) -> Self {
        let mut spending_data = [FROZEN_BYTE; 36];
        spending_data[0..4].copy_from_slice(&cooldown.to_le_bytes());
        Self {
            hash,
            status: UTXO_FROZEN,
            spending_data,
        }
    }

    /// Create an unspent UTXO slot carrying a reassignment cooldown (LP-4).
    ///
    /// `spending_data[0..4]` encodes the cooldown height (0 = immediately
    /// spendable); the remaining 32 bytes are zeroed. This is the inverse of
    /// [`Self::new_frozen_with_cooldown`] used by `unfreeze` to restore the
    /// cooldown that a freeze cycle would otherwise have wiped. When
    /// `cooldown == 0` the result is byte-identical to [`Self::new_unspent`].
    pub fn new_unspent_with_cooldown(hash: [u8; 32], cooldown: u32) -> Self {
        let mut spending_data = [0u8; 36];
        spending_data[0..4].copy_from_slice(&cooldown.to_le_bytes());
        Self {
            hash,
            status: UTXO_UNSPENT,
            spending_data,
        }
    }

    /// Extract the reassignment cooldown height from a slot's `spending_data`.
    ///
    /// Reads `spending_data[0..4]` as a little-endian `u32`. For an unspent
    /// slot this is the spendable-at height (0 = immediately spendable); for a
    /// frozen slot written by [`Self::new_frozen_with_cooldown`] it is the
    /// preserved cooldown (and `0xFFFF_FFFF` for a legacy all-`0xFF` frozen
    /// slot that carried no cooldown — treated as "no cooldown to restore" by
    /// `unfreeze`).
    pub fn reassignment_cooldown(&self) -> u32 {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&self.spending_data[0..4]);
        u32::from_le_bytes(buf)
    }

    /// Create a new spent UTXO slot with the given hash and spending data.
    ///
    /// `spending_data` must be exactly 36 bytes: txid(32) + vin(4 LE).
    pub fn new_spent(hash: [u8; 32], spending_data: [u8; 36]) -> Self {
        Self {
            hash,
            status: UTXO_SPENT,
            spending_data,
        }
    }

    /// Returns `true` if this slot is in the frozen state.
    pub fn is_frozen(&self) -> bool {
        self.status == UTXO_FROZEN
    }

    /// Returns `true` if this slot is unspent.
    pub fn is_unspent(&self) -> bool {
        self.status == UTXO_UNSPENT
    }

    /// Returns `true` if this slot is spent.
    pub fn is_spent(&self) -> bool {
        self.status == UTXO_SPENT
    }

    /// Returns `true` if this slot is pruned (terminal state).
    pub fn is_pruned(&self) -> bool {
        self.status == UTXO_PRUNED
    }

    /// Serialize this slot to a byte slice.
    ///
    /// The destination must be at least `UTXO_SLOT_SIZE` bytes.
    pub fn to_bytes(&self, dst: &mut [u8]) {
        debug_assert!(dst.len() >= UTXO_SLOT_SIZE);
        dst[..32].copy_from_slice(&self.hash);
        dst[32] = self.status;
        dst[33..69].copy_from_slice(&self.spending_data);
        let crc = crc32fast::hash(&dst[..UTXO_SLOT_PAYLOAD_SIZE]);
        dst[UTXO_SLOT_CRC32_OFFSET..UTXO_SLOT_CRC32_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
    }

    /// Deserialize a slot from a byte slice.
    ///
    /// The source must be at least `UTXO_SLOT_SIZE` bytes.
    pub fn from_bytes(src: &[u8]) -> Result<Self, RecordError> {
        debug_assert!(src.len() >= UTXO_SLOT_SIZE);
        let mut expected = [0u8; 4];
        expected.copy_from_slice(&src[UTXO_SLOT_CRC32_OFFSET..UTXO_SLOT_CRC32_OFFSET + 4]);
        let expected = u32::from_le_bytes(expected);
        let actual = crc32fast::hash(&src[..UTXO_SLOT_PAYLOAD_SIZE]);
        if actual != expected {
            return Err(RecordError::CrcMismatch { expected, actual });
        }

        let mut hash = [0u8; 32];
        hash.copy_from_slice(&src[..32]);
        let status = src[32];
        let mut spending_data = [0u8; 36];
        spending_data.copy_from_slice(&src[33..69]);
        Ok(Self {
            hash,
            status,
            spending_data,
        })
    }
}

impl std::fmt::Debug for UtxoSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtxoSlot")
            .field("hash", &hex_short(&self.hash))
            .field("status", &self.status)
            .finish()
    }
}

impl PartialEq for UtxoSlot {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
            && self.status == other.status
            && self.spending_data == other.spending_data
    }
}

impl Eq for UtxoSlot {}

// ---------------------------------------------------------------------------
// BlockEntry
// ---------------------------------------------------------------------------

/// A single block entry combining block_id, block_height, and subtree_idx.
///
/// Replaces the three parallel lists from the original Lua design.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockEntry {
    /// Block ID where this transaction was mined.
    pub block_id: u32,
    /// Block height.
    pub block_height: u32,
    /// Subtree index within the block.
    pub subtree_idx: u32,
}

impl BlockEntry {
    /// Serialize this entry to a byte slice (must be >= 12 bytes).
    pub fn to_bytes(&self, dst: &mut [u8]) {
        debug_assert!(dst.len() >= BLOCK_ENTRY_SIZE);
        dst[0..4].copy_from_slice(&self.block_id.to_le_bytes());
        dst[4..8].copy_from_slice(&self.block_height.to_le_bytes());
        dst[8..12].copy_from_slice(&self.subtree_idx.to_le_bytes());
    }

    /// Deserialize an entry from a byte slice (must be >= 12 bytes).
    pub fn from_bytes(src: &[u8]) -> Self {
        debug_assert!(src.len() >= BLOCK_ENTRY_SIZE);
        Self {
            block_id: u32::from_le_bytes(src[0..4].try_into().unwrap()),
            block_height: u32::from_le_bytes(src[4..8].try_into().unwrap()),
            subtree_idx: u32::from_le_bytes(src[8..12].try_into().unwrap()),
        }
    }
}

// ---------------------------------------------------------------------------
// ExternalRef
// ---------------------------------------------------------------------------

/// Reference to externally stored transaction data (large transactions).
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct ExternalRef {
    /// Storage backend type: 0=inline, 1=local_file, 2=object_store.
    pub store_type: u8,
    /// Content hash (txID used as blob key).
    pub content_hash: [u8; 32],
    /// Original blob size in bytes.
    pub total_size: u64,
    /// Number of inputs in the blob.
    pub input_count: u32,
    /// Number of outputs in the blob.
    pub output_count: u32,
    /// Byte offset within blob for inputs section.
    pub inputs_offset: u64,
    /// Byte offset within blob for outputs section.
    pub outputs_offset: u64,
}

impl ExternalRef {
    /// Create a zeroed (empty) external reference.
    pub fn zeroed() -> Self {
        Self {
            store_type: 0,
            content_hash: [0u8; 32],
            total_size: 0,
            input_count: 0,
            output_count: 0,
            inputs_offset: 0,
            outputs_offset: 0,
        }
    }
}

impl std::fmt::Debug for ExternalRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalRef")
            .field("store_type", &self.store_type)
            .field("total_size", &{ self.total_size })
            .finish()
    }
}

impl PartialEq for ExternalRef {
    fn eq(&self, other: &Self) -> bool {
        self.store_type == other.store_type
            && self.content_hash == other.content_hash
            && { self.total_size } == { other.total_size }
            && { self.input_count } == { other.input_count }
            && { self.output_count } == { other.output_count }
            && { self.inputs_offset } == { other.inputs_offset }
            && { self.outputs_offset } == { other.outputs_offset }
    }
}

impl Eq for ExternalRef {}

// ---------------------------------------------------------------------------
// TxFlags
// ---------------------------------------------------------------------------

bitflags! {
    /// Packed bitfield for transaction boolean flags (1 byte).
    ///
    /// Replaces 5 separate fields from the original design.
    /// The `CREATING` flag is eliminated — it only existed for multi-record
    /// 2-phase commit which is unnecessary with single-record atomic writes.
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TxFlags: u8 {
        /// Bit 0 — write-once, set at Create for coinbase transactions.
        const IS_COINBASE    = 0b0000_0001;
        /// Bit 1 — mutable, toggled by SetConflicting.
        const CONFLICTING    = 0b0000_0010;
        /// Bit 2 — mutable, toggled by SetLocked / cleared by SetMined.
        const LOCKED         = 0b0000_0100;
        /// Bit 3 — write-once, set at Create for large transactions.
        const EXTERNAL       = 0b0000_1000;
        /// Bit 4 — mutable, tracks last all-spent signaling state.
        const LAST_SPENT_ALL = 0b0001_0000;
        /// Bit 5 — index-only flag (not persisted to device metadata).
        /// When set in `TxIndexEntry.tx_flags`, the `dah_or_preserve` field
        /// holds `preserve_until` instead of `delete_at_height`.
        const HAS_PRESERVE_UNTIL = 0b0010_0000;
        /// Bit 6 — write-once-ish, set by `reassign` on first reassignment
        /// (LP-3). Persisted to device metadata.
        ///
        /// Mirrors the Aerospike Lua `reassign` which inflates
        /// `recordUtxos` by 1 (`teranode.lua:945`) so the all-spent check
        /// (`spent_utxos == utxo_count`) can never become true on a
        /// reassigned record — keeping the court-ordered reassignment's
        /// audit trail (old hash → new hash) on the store permanently.
        /// TeraSlab cannot fabricate a phantom UTXO slot, so it carries the
        /// "this record has been reassigned" fact in this flag instead and
        /// excludes such records from the all-spent DAH path in
        /// `evaluate_delete_at_height`. The CONFLICTING DAH branch is
        /// unaffected (the Lua `+1` only touches the all-spent computation),
        /// so a reassigned record that is later marked conflicting still
        /// gets DAH'd, matching the reference.
        const REASSIGNED = 0b0100_0000;
    }
}

// ---------------------------------------------------------------------------
// TxMetadata (the raw inner struct, before padding)
// ---------------------------------------------------------------------------

/// Internal struct to compute the unpadded metadata size.
/// Not used directly; `TxMetadata` adds padding.
#[repr(C, packed)]
struct TxMetadataRaw {
    _magic: u32,
    _schema_version: u32,
    _record_size: u32,
    _utxo_count: u32,
    _tx_id: [u8; 32],
    _tx_version: u32,
    _locktime: u32,
    _identity_crc: u32,
    _fee: u64,
    _size_in_bytes: u64,
    _extended_size: u64,
    _flags: u8,
    _spending_height: u32,
    _created_at: u64,
    _spent_utxos: u32,
    _pruned_utxos: u32,
    _generation: u32,
    _updated_at: u64,
    _block_entry_count: u8,
    _block_entries_inline: [BlockEntry; INLINE_BLOCK_ENTRIES],
    _block_overflow_offset: u64,
    _reassignment_offset: u64,
    _reassignment_count: u8,
    _unmined_since: u32,
    _delete_at_height: u32,
    _preserve_until: u32,
    _external_ref: ExternalRef,
    _conflicting_children_count: u8,
    _conflicting_children_offset: u64,
    _deleted_children_count: u8,
    _deleted_children_offset: u64,
    _crc32: u32,
}

// ---------------------------------------------------------------------------
// TxMetadata
// ---------------------------------------------------------------------------

/// Full metadata header stored at the beginning of every record on device.
///
/// Fixed size (`METADATA_SIZE` bytes), padded to a 64-byte boundary.
/// Metadata is placed first in the record so that UTXO slot offsets are
/// deterministic: `METADATA_SIZE + vout * 69`.
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct TxMetadata {
    /// Magic number for record validation (must be `METADATA_MAGIC`).
    pub magic: u32,
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// Total record size in bytes (metadata + slots + cold data).
    pub record_size: u32,
    /// Number of UTXO slots allocated in this record.
    pub utxo_count: u32,

    /// Transaction hash (32 bytes, write-once).
    pub tx_id: [u8; 32],
    /// Bitcoin transaction version field.
    pub tx_version: u32,
    /// Transaction locktime.
    pub locktime: u32,

    /// CRC32 over the immutable identity prefix `[0..IDENTITY_PREFIX_LEN)` —
    /// `magic`, `schema_version`, `record_size`, `utxo_count`, `tx_id`,
    /// `tx_version`, `locktime`. All of these are write-once at create and
    /// never mutated, so this CRC is computed once by [`TxMetadata::to_bytes`]
    /// and is stable for the life of the record.
    ///
    /// It exists so the `get_spend` hot path can read+validate just the first
    /// cache line of the header (60 bytes via
    /// [`TxMetadata::read_identity_from`]) instead of the full
    /// `METADATA_SIZE`-byte header guarded by [`Self::crc32`]. Reading one
    /// cache line instead of five roughly halves read latency on cold
    /// records. Callers must never write this field directly.
    pub identity_crc: u32,

    /// Transaction fee in satoshis (signed to match Go's int64).
    pub fee: u64,
    /// Serialized transaction size in bytes.
    pub size_in_bytes: u64,
    /// Extended metadata size.
    pub extended_size: u64,
    /// Packed boolean flags — see [`TxFlags`].
    pub flags: TxFlags,
    /// Coinbase maturity height (blockHeight + 100 for coinbase txs).
    pub spending_height: u32,
    /// Record creation timestamp (milliseconds since epoch).
    pub created_at: u64,

    /// Number of UTXO slots with status == SPENT.
    pub spent_utxos: u32,
    /// Number of UTXO slots with status == PRUNED.
    pub pruned_utxos: u32,
    /// Monotonically increasing mutation counter.
    pub generation: u32,
    /// Timestamp of last mutation (milliseconds since epoch).
    pub updated_at: u64,

    /// Total block entries (inline + overflow).
    pub block_entry_count: u8,
    /// First 3 block entries stored inline.
    pub block_entries_inline: [BlockEntry; INLINE_BLOCK_ENTRIES],
    /// Device offset to overflow block (0 = no overflow).
    pub block_overflow_offset: u64,
    /// Device offset to reassignment extension block (0 = none).
    pub reassignment_offset: u64,
    /// Number of reassignments recorded.
    pub reassignment_count: u8,

    /// Block height when transaction became unmined (0 = on longest chain).
    pub unmined_since: u32,
    /// Block height at which this record should be deleted (0 = not set).
    pub delete_at_height: u32,
    /// Block height until which this record is preserved (0 = not set).
    pub preserve_until: u32,

    /// External blob reference (for large transactions).
    pub external_ref: ExternalRef,

    /// Number of conflicting children txids stored for this transaction.
    pub conflicting_children_count: u8,
    /// Device offset to a separately-allocated block of txids (0 = none).
    pub conflicting_children_offset: u64,

    /// Number of deleted children txids stored for this transaction.
    ///
    /// F-X-022: parity with Aerospike Lua `addDeletedChildren`. Populated
    /// whenever a child tx is pruned against this parent — the child's
    /// txid is appended to the per-record list referenced by
    /// `deleted_children_offset`. Consulted by the idempotent-respend
    /// short-circuit as defense-in-depth against the
    /// resurrected-then-pruned re-spend pattern (the primary spend
    /// rejection still flows through `UTXO_PRUNED` on the slot).
    pub deleted_children_count: u8,
    /// Device offset to a separately-allocated block of deleted-child
    /// txids (0 = none). See [`Self::deleted_children_count`].
    pub deleted_children_offset: u64,

    /// CRC32 checksum over the entire `METADATA_SIZE`-byte header,
    /// computed with this field zeroed. Guards against torn writes and
    /// on-disk corruption.
    ///
    /// Populated by [`TxMetadata::to_bytes`]; validated by
    /// [`TxMetadata::from_bytes`]. Callers should never write this field
    /// directly.
    pub crc32: u32,

    /// Padding to reach `METADATA_SIZE` (64-byte aligned).
    pub _padding: [u8; METADATA_PADDING],
}

impl TxMetadata {
    /// Create a new metadata header with default/zero values and the magic number set.
    pub fn new(utxo_count: u32) -> Self {
        let record_size = METADATA_SIZE as u32 + utxo_count * UTXO_SLOT_SIZE as u32;
        Self {
            magic: METADATA_MAGIC,
            schema_version: METADATA_VERSION,
            record_size,
            utxo_count,
            tx_id: [0u8; 32],
            tx_version: 0,
            locktime: 0,
            identity_crc: 0,
            fee: 0,
            size_in_bytes: 0,
            extended_size: 0,
            flags: TxFlags::empty(),
            spending_height: 0,
            created_at: 0,
            spent_utxos: 0,
            pruned_utxos: 0,
            generation: 0,
            updated_at: 0,
            block_entry_count: 0,
            block_entries_inline: [BlockEntry {
                block_id: 0,
                block_height: 0,
                subtree_idx: 0,
            }; INLINE_BLOCK_ENTRIES],
            block_overflow_offset: 0,
            reassignment_offset: 0,
            reassignment_count: 0,
            unmined_since: 0,
            delete_at_height: 0,
            preserve_until: 0,
            external_ref: ExternalRef::zeroed(),
            conflicting_children_count: 0,
            conflicting_children_offset: 0,
            deleted_children_count: 0,
            deleted_children_offset: 0,
            crc32: 0,
            _padding: [0u8; METADATA_PADDING],
        }
    }

    /// Byte offset from record start to UTXO slot `slot_index`.
    pub fn utxo_slot_offset(slot_index: u32) -> u64 {
        METADATA_SIZE as u64 + (slot_index as u64) * UTXO_SLOT_SIZE as u64
    }

    /// Total byte size of a record with `utxo_count` slots (metadata + slots only,
    /// not including cold data).
    pub fn record_size_for(utxo_count: u32) -> u64 {
        METADATA_SIZE as u64 + (utxo_count as u64) * UTXO_SLOT_SIZE as u64
    }

    /// Serialize the entire metadata struct to a byte slice, computing and
    /// stamping the CRC32 checksum over the serialized bytes (with the CRC
    /// slot zeroed during computation).
    ///
    /// The destination must be at least `METADATA_SIZE` bytes. After this
    /// call, `dst[..METADATA_SIZE]` contains a self-describing header whose
    /// CRC can be validated by [`TxMetadata::from_bytes`].
    pub fn to_bytes(&self, dst: &mut [u8]) {
        debug_assert!(dst.len() >= METADATA_SIZE);
        // Safety: TxMetadata is repr(C, packed), so we can transmute it to bytes.
        let src = unsafe {
            std::slice::from_raw_parts((self as *const Self).cast::<u8>(), METADATA_SIZE)
        };
        dst[..METADATA_SIZE].copy_from_slice(src);

        // Stamp the immutable identity-prefix CRC first, so it is part of the
        // bytes covered by the full-header CRC below. The prefix CRC covers
        // `[0..IDENTITY_PREFIX_LEN)` (which does NOT include the prefix-CRC
        // slot itself at `IDENTITY_CRC_OFFSET`), so no zeroing is required.
        let id_crc = crc32fast::hash(&dst[..IDENTITY_PREFIX_LEN]);
        dst[IDENTITY_CRC_OFFSET..IDENTITY_CRC_OFFSET + 4].copy_from_slice(&id_crc.to_le_bytes());

        // Zero the CRC slot, compute CRC over the full METADATA_SIZE bytes,
        // then stamp the result into the CRC slot.
        let crc_off = CRC32_OFFSET;
        dst[crc_off..crc_off + 4].copy_from_slice(&[0u8; 4]);
        let crc = crc32fast::hash(&dst[..METADATA_SIZE]);
        dst[crc_off..crc_off + 4].copy_from_slice(&crc.to_le_bytes());
    }

    /// Deserialize metadata from a byte slice, validating the CRC32 checksum.
    ///
    /// Returns [`RecordError::CrcMismatch`] if the stored CRC disagrees with
    /// a freshly-computed CRC over the header bytes (with the CRC slot
    /// zeroed). This is the only gate against torn writes and on-disk bit
    /// rot; callers must propagate the error — silent acceptance of
    /// corrupted metadata can break UTXO correctness.
    ///
    /// Returns [`RecordError::TooShort`] if `src` is shorter than
    /// [`METADATA_SIZE`]. This is a real runtime check (not just a
    /// `debug_assert!`) so that release builds fail closed on a truncated or
    /// torn read rather than risking an out-of-bounds slice panic.
    ///
    /// The source must be at least `METADATA_SIZE` bytes.
    pub fn from_bytes(src: &[u8]) -> Result<Self, RecordError> {
        if src.len() < METADATA_SIZE {
            return Err(RecordError::TooShort {
                actual: src.len(),
                required: METADATA_SIZE,
            });
        }
        let crc_off = CRC32_OFFSET;
        let mut expected = [0u8; 4];
        expected.copy_from_slice(&src[crc_off..crc_off + 4]);
        let expected = u32::from_le_bytes(expected);

        // Compute CRC over the header bytes with the CRC slot zeroed.
        // We use a small on-stack buffer rather than allocating.
        let mut buf = [0u8; METADATA_SIZE];
        buf.copy_from_slice(&src[..METADATA_SIZE]);
        buf[crc_off..crc_off + 4].copy_from_slice(&[0u8; 4]);
        let actual = crc32fast::hash(&buf);

        if actual != expected {
            return Err(RecordError::CrcMismatch { expected, actual });
        }

        let mut meta = std::mem::MaybeUninit::<Self>::uninit();
        // Safety: TxMetadata is repr(C, packed) and Copy. We copy exactly
        // METADATA_SIZE bytes into the struct.
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                meta.as_mut_ptr().cast::<u8>(),
                METADATA_SIZE,
            );
            Ok(meta.assume_init())
        }
    }

    /// Deserialize metadata from a byte slice without validating the CRC.
    ///
    /// **Crate-internal diagnostics helper.** Intended for recovery /
    /// debugging tooling that needs to inspect a known-corrupt header.
    /// F-G1-006: the visibility was tightened from `pub` to `pub(crate)`
    /// so external callers cannot bypass the CRC integrity story by
    /// grepping for "fast metadata read". Library code on the hot path
    /// must use [`TxMetadata::from_bytes`] which validates the CRC.
    ///
    /// `#[allow(dead_code)]` because the helper is kept for future
    /// diagnostics tooling; the `pub(crate)` visibility documents the
    /// intent but means no crate-external caller can reach it, and
    /// today no in-crate caller exists.
    #[allow(dead_code)]
    pub(crate) fn from_bytes_unchecked(src: &[u8]) -> Self {
        debug_assert!(src.len() >= METADATA_SIZE);
        let mut meta = std::mem::MaybeUninit::<Self>::uninit();
        // Safety: TxMetadata is repr(C, packed) and Copy.
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                meta.as_mut_ptr().cast::<u8>(),
                METADATA_SIZE,
            );
            meta.assume_init()
        }
    }
}

/// Byte offset of the CRC32 field within a serialized [`TxMetadata`] header.
/// Used by [`TxMetadata::to_bytes`] and [`TxMetadata::from_bytes`] to locate
/// the four CRC bytes for zeroing/stamping during checksum computation.
pub const CRC32_OFFSET: usize = std::mem::offset_of!(TxMetadata, crc32);

/// Byte offset of the identity-prefix CRC32 slot within a serialized header.
pub const IDENTITY_CRC_OFFSET: usize = std::mem::offset_of!(TxMetadata, identity_crc);

/// Number of leading header bytes covered by [`TxMetadata::identity_crc`].
///
/// These bytes are all write-once at create (`magic`, `schema_version`,
/// `record_size`, `utxo_count`, `tx_id`, `tx_version`, `locktime`) and end
/// exactly where the identity-CRC slot begins.
pub const IDENTITY_PREFIX_LEN: usize = IDENTITY_CRC_OFFSET;

/// Number of leading header bytes a reader must copy to validate and parse
/// the identity prefix: the covered region plus the 4-byte CRC slot.
pub const IDENTITY_READ_LEN: usize = IDENTITY_CRC_OFFSET + 4;

// The identity prefix (read + its CRC) must fit within a single 64-byte
// cache line — that is the entire point of the layout (one cache-line read
// on the get_spend hot path instead of the full five-line header).
const _: () = assert!(IDENTITY_READ_LEN <= 64);
// `locktime` is the last covered field and must butt up against the CRC slot.
const _: () = assert!(std::mem::offset_of!(TxMetadata, locktime) + 4 == IDENTITY_CRC_OFFSET);
// The identity fields must sit inside the covered prefix.
const _: () = assert!(std::mem::offset_of!(TxMetadata, utxo_count) + 4 <= IDENTITY_PREFIX_LEN);
const _: () = assert!(std::mem::offset_of!(TxMetadata, tx_id) + 32 <= IDENTITY_PREFIX_LEN);

/// The immutable identity of a record, extractable from just its first
/// cache line via [`TxMetadata::read_identity_from`].
///
/// Used by the `get_spend` hot path: every field here is write-once at
/// create, so reading the cache-line-sized prefix is sufficient to (a)
/// confirm the record at an offset still belongs to the requested
/// `tx_id` (F-G2-001 aliasing defense), (b) re-derive the authoritative
/// `utxo_count` bound, and (c) obtain `locktime` — without paying for the
/// full `METADATA_SIZE`-byte header read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TxIdentity {
    /// Transaction hash (write-once).
    pub tx_id: [u8; 32],
    /// Number of UTXO slots allocated in this record (write-once).
    pub utxo_count: u32,
    /// Transaction locktime (write-once).
    pub locktime: u32,
}

impl TxMetadata {
    /// Parse and validate the immutable identity prefix from the first
    /// [`IDENTITY_READ_LEN`] bytes of a serialized header.
    ///
    /// `src` must be at least [`IDENTITY_READ_LEN`] bytes. Returns
    /// [`RecordError::CrcMismatch`] if the stored identity CRC disagrees with
    /// a freshly-computed CRC over `[0..IDENTITY_PREFIX_LEN)`.
    ///
    /// A `CrcMismatch` here means one of: (a) on-disk corruption of the
    /// identity bytes, or (b) the header has been overwritten by a
    /// `DeletedRecordMarker` or another record's bytes (offset reuse) — i.e.
    /// the record no longer carries a valid live identity. Callers on the
    /// lock-free read path treat this the same way they treat a `tx_id`
    /// mismatch: the requested transaction is not present at this offset.
    ///
    /// Note: this validates *only* the identity prefix, not the full header.
    /// It is exclusively for hot read paths that need `tx_id` / `utxo_count`
    /// / `locktime`; any path needing other fields must use
    /// [`TxMetadata::from_bytes`].
    #[inline]
    pub fn read_identity_from(src: &[u8]) -> Result<TxIdentity, RecordError> {
        debug_assert!(src.len() >= IDENTITY_READ_LEN);
        let mut stored = [0u8; 4];
        stored.copy_from_slice(&src[IDENTITY_CRC_OFFSET..IDENTITY_CRC_OFFSET + 4]);
        let expected = u32::from_le_bytes(stored);
        let actual = crc32fast::hash(&src[..IDENTITY_PREFIX_LEN]);
        if actual != expected {
            return Err(RecordError::CrcMismatch { expected, actual });
        }

        let uc_off = std::mem::offset_of!(TxMetadata, utxo_count);
        let id_off = std::mem::offset_of!(TxMetadata, tx_id);
        let lt_off = std::mem::offset_of!(TxMetadata, locktime);
        let mut utxo_count = [0u8; 4];
        utxo_count.copy_from_slice(&src[uc_off..uc_off + 4]);
        let mut locktime = [0u8; 4];
        locktime.copy_from_slice(&src[lt_off..lt_off + 4]);
        let mut tx_id = [0u8; 32];
        tx_id.copy_from_slice(&src[id_off..id_off + 32]);

        Ok(TxIdentity {
            tx_id,
            utxo_count: u32::from_le_bytes(utxo_count),
            locktime: u32::from_le_bytes(locktime),
        })
    }

    /// Compute the CRC32 over this metadata header with the CRC slot zeroed.
    ///
    /// Used by the targeted-write helpers in [`crate::io`] that mutate only
    /// a handful of fields on the device — they restamp the CRC based on
    /// the full in-memory struct (which carries the updated fields) so the
    /// header remains verifiable on read.
    pub fn compute_crc(&self) -> u32 {
        let mut buf = [0u8; METADATA_SIZE];
        // Safety: TxMetadata is repr(C, packed).
        let src = unsafe {
            std::slice::from_raw_parts((self as *const Self).cast::<u8>(), METADATA_SIZE)
        };
        buf.copy_from_slice(src);
        // Re-derive the identity-prefix CRC from the (immutable) prefix fields,
        // exactly as `to_bytes` does, so the full CRC this returns matches the
        // on-disk bytes regardless of the in-memory `identity_crc` field. The
        // targeted-write restamp path never rewrites the identity slot on
        // device, so this keeps `compute_crc` consistent with `to_bytes`
        // without depending on the caller having round-tripped through
        // `from_bytes`.
        let id_crc = crc32fast::hash(&buf[..IDENTITY_PREFIX_LEN]);
        buf[IDENTITY_CRC_OFFSET..IDENTITY_CRC_OFFSET + 4].copy_from_slice(&id_crc.to_le_bytes());
        buf[CRC32_OFFSET..CRC32_OFFSET + 4].copy_from_slice(&[0u8; 4]);
        crc32fast::hash(&buf)
    }
}

impl std::fmt::Debug for TxMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxMetadata")
            .field("magic", &format_args!("0x{:08X}", { self.magic }))
            .field("schema_version", &{ self.schema_version })
            .field("record_size", &{ self.record_size })
            .field("utxo_count", &{ self.utxo_count })
            .field("tx_id", &hex_short(&self.tx_id))
            .field("flags", &self.flags)
            .field("spent_utxos", &{ self.spent_utxos })
            .field("generation", &{ self.generation })
            .field("block_entry_count", &self.block_entry_count)
            .finish()
    }
}

impl PartialEq for TxMetadata {
    fn eq(&self, other: &Self) -> bool {
        let mut a = [0u8; METADATA_SIZE];
        let mut b = [0u8; METADATA_SIZE];
        self.to_bytes(&mut a);
        other.to_bytes(&mut b);
        a == b
    }
}

impl Eq for TxMetadata {}

// ---------------------------------------------------------------------------
// Compile-time assertions
// ---------------------------------------------------------------------------

const _: () = assert!(std::mem::size_of::<UtxoSlot>() == UTXO_SLOT_PAYLOAD_SIZE);
const _: () = assert!(std::mem::size_of::<BlockEntry>() == BLOCK_ENTRY_SIZE);
const _: () = assert!(BLOCK_ENTRY_SIZE == 12);
const _: () = assert!(UTXO_SLOT_PAYLOAD_SIZE == 69);
const _: () = assert!(UTXO_SLOT_SIZE == 73);
const _: () = assert!(std::mem::size_of::<TxFlags>() == 1);
const _: () = assert!(METADATA_SIZE.is_multiple_of(64));
// METADATA_SIZE is 320 bytes (grew from 256 to accommodate the trailing
// `crc32` field — see task C7). The header is cache-line aligned and UTXO
// slots live at a deterministic offset `METADATA_SIZE + vout * UTXO_SLOT_SIZE`.
const _: () = assert!(METADATA_SIZE == 320);
const _: () = assert!(std::mem::size_of::<TxMetadata>() == METADATA_SIZE);
// The CRC slot must sit inside the header (before the padding tail) so it
// is covered by the checksum computation.
const _: () = assert!(CRC32_OFFSET + 4 <= METADATA_SIZE);

// ---------------------------------------------------------------------------
// DeletedRecordMarker
// ---------------------------------------------------------------------------

/// Magic number identifying a deleted-record tombstone marker ("DELD" in
/// ASCII). Distinct from [`METADATA_MAGIC`] ("SLAB") so a device scan can
/// tell a deleted record apart from a live one, and non-zero so it is also
/// distinct from the legacy all-zero deleted-header convention.
pub const DELETED_RECORD_MAGIC: u32 = 0x444C_4544;

/// Length-bearing on-device marker written in place of a record's metadata
/// header when the record is deleted (`Engine::delete`).
///
/// # Why this exists
///
/// The delete path zeroes the record's metadata header as a crash-recovery
/// skip-guard, but leaves the record body (UTXO slots / cold data) intact.
/// A device-scan index rebuild that runs after a delete-then-crash (before
/// the next allocator checkpoint frees the region in the persisted freelist)
/// must skip the *whole* deleted record. With a bare all-zero header it can
/// only advance one alignment block — for any record larger than one block
/// the next read lands on the deleted record's still-non-zero body, fails
/// the magic/CRC check, and aborts the rebuild → boot loop.
///
/// This marker is written into the first [`METADATA_SIZE`] bytes of the
/// record (the rest of the header window is zeroed, so old transaction bytes
/// do not remain readable). It carries the freed `record_size`, letting the
/// rebuild skip `align_up(record_size)` — exactly as it does for a live
/// record — instead of a single block.
///
/// It is CRC-protected so a torn write of the marker cannot be mistaken for a
/// valid skip instruction (a torn marker fails the CRC check and is rejected
/// as corruption, never silently advancing the scan by a garbage length).
///
/// Crash-safety: the marker is written and fsynced in the *same* delete
/// fsync that the old zeroing used — it adds no extra device writes.
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct DeletedRecordMarker {
    /// Magic number — must be [`DELETED_RECORD_MAGIC`]. Sits at byte offset 0,
    /// the same position as [`TxMetadata::magic`], so a scan can read four
    /// bytes at the record start and classify the header in one read.
    pub magic: u32,
    /// Total size in bytes of the deleted record (metadata + slots + cold
    /// data), i.e. the value of [`TxMetadata::record_size`] at delete time.
    /// The rebuild advances `align_up(record_size)` past this marker.
    pub record_size: u64,
    /// CRC32 over these marker bytes with the `crc32` slot zeroed. Guards
    /// against a torn write being read back as a valid length-skip.
    pub crc32: u32,
}

/// Number of leading bytes of a record header occupied by a serialized
/// [`DeletedRecordMarker`]. The remaining header bytes up to [`METADATA_SIZE`]
/// are zeroed on delete.
pub const DELETED_RECORD_MARKER_SIZE: usize = std::mem::size_of::<DeletedRecordMarker>();

/// Byte offset of the CRC32 field within a serialized [`DeletedRecordMarker`].
pub const DELETED_RECORD_MARKER_CRC_OFFSET: usize =
    std::mem::offset_of!(DeletedRecordMarker, crc32);

impl DeletedRecordMarker {
    /// Construct a marker for a deleted record of `record_size` bytes.
    pub fn new(record_size: u64) -> Self {
        Self {
            magic: DELETED_RECORD_MAGIC,
            record_size,
            crc32: 0,
        }
    }

    /// Serialize the marker into the first [`DELETED_RECORD_MARKER_SIZE`]
    /// bytes of `dst`, stamping the CRC32 over the marker bytes (with the CRC
    /// slot zeroed during computation). `dst` must be at least
    /// [`DELETED_RECORD_MARKER_SIZE`] bytes.
    pub fn to_bytes(&self, dst: &mut [u8]) {
        debug_assert!(dst.len() >= DELETED_RECORD_MARKER_SIZE);
        // Safety: DeletedRecordMarker is repr(C, packed); transmute to bytes.
        let src = unsafe {
            std::slice::from_raw_parts(
                (self as *const Self).cast::<u8>(),
                DELETED_RECORD_MARKER_SIZE,
            )
        };
        dst[..DELETED_RECORD_MARKER_SIZE].copy_from_slice(src);
        let crc_off = DELETED_RECORD_MARKER_CRC_OFFSET;
        dst[crc_off..crc_off + 4].copy_from_slice(&[0u8; 4]);
        let crc = crc32fast::hash(&dst[..DELETED_RECORD_MARKER_SIZE]);
        dst[crc_off..crc_off + 4].copy_from_slice(&crc.to_le_bytes());
    }

    /// Parse a deleted-record marker from the start of `src`, returning it
    /// only if the magic matches [`DELETED_RECORD_MAGIC`] AND the CRC32
    /// validates.
    ///
    /// Returns `None` when the bytes are not a (valid) marker — i.e. a live
    /// record header, an all-zero legacy/reservation header, or a torn marker
    /// write. `None` deliberately does NOT distinguish these: the caller has
    /// already separated the all-zero and live-magic cases, so any remaining
    /// `None` from a non-zero, non-live header is genuine corruption.
    pub fn try_parse(src: &[u8]) -> Option<Self> {
        if src.len() < DELETED_RECORD_MARKER_SIZE {
            return None;
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&src[0..4]);
        if u32::from_le_bytes(magic) != DELETED_RECORD_MAGIC {
            return None;
        }
        let crc_off = DELETED_RECORD_MARKER_CRC_OFFSET;
        let mut expected = [0u8; 4];
        expected.copy_from_slice(&src[crc_off..crc_off + 4]);
        let expected = u32::from_le_bytes(expected);

        let mut buf = [0u8; DELETED_RECORD_MARKER_SIZE];
        buf.copy_from_slice(&src[..DELETED_RECORD_MARKER_SIZE]);
        buf[crc_off..crc_off + 4].copy_from_slice(&[0u8; 4]);
        let actual = crc32fast::hash(&buf);
        if actual != expected {
            return None;
        }

        let mut marker = std::mem::MaybeUninit::<Self>::uninit();
        // Safety: DeletedRecordMarker is repr(C, packed) and Copy; copy
        // exactly DELETED_RECORD_MARKER_SIZE bytes in.
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                marker.as_mut_ptr().cast::<u8>(),
                DELETED_RECORD_MARKER_SIZE,
            );
            Some(marker.assume_init())
        }
    }
}

// The marker must fit inside the header window it overwrites, and its magic
// must share byte offset 0 with `TxMetadata::magic` so a scan can classify a
// header by reading four bytes at the record start.
const _: () = assert!(DELETED_RECORD_MARKER_SIZE <= METADATA_SIZE);
const _: () = assert!(std::mem::offset_of!(DeletedRecordMarker, magic) == 0);
const _: () = assert!(std::mem::offset_of!(TxMetadata, magic) == 0);
const _: () = assert!(DELETED_RECORD_MARKER_CRC_OFFSET + 4 <= DELETED_RECORD_MARKER_SIZE);
// Distinct from the live-record magic so the two never collide.
const _: () = assert!(DELETED_RECORD_MAGIC != METADATA_MAGIC);
const _: () = assert!(DELETED_RECORD_MAGIC != 0);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Half of the `u32` generation number space.
///
/// Record generations are compared as wrapping serial numbers. A target
/// generation is considered ahead of a local generation only when it is in the
/// next half of the `u32` space: `0 < target.wrapping_sub(local) < 2^31`.
/// The exact half-range distance is ambiguous, so it is classified as
/// not-ahead; redo and replication callers treat that as already applied or
/// stale and must not rely on retaining more than `2^31 - 1` outstanding
/// mutations for one record.
pub const GENERATION_ORDER_WINDOW: u32 = 1u32 << 31;

/// Early-warning threshold for the wrapping generation ordering.
///
/// When the forward delta `target.wrapping_sub(local)` exceeds this value
/// (half of [`GENERATION_ORDER_WINDOW`]) the record is within `2^30`
/// mutations of the ambiguity boundary at `2^31`. F-G1-019: emit a
/// `warn`-level log and bump
/// [`crate::metrics::AllocatorMetrics::generation_wrap_warn_total`] so
/// operators can see the approach long before the comparison flips.
pub const GENERATION_WRAP_WARN_DELTA: u32 = 1u32 << 30;

/// Return true when `target` is newer than `local` under wrapping generation
/// ordering.
///
/// F-G1-019: when the forward delta exceeds [`GENERATION_WRAP_WARN_DELTA`]
/// the function emits a `warn`-level log and bumps the
/// `generation_wrap_warn_total` counter on `AllocatorMetrics`. The
/// classification result is unchanged — only telemetry.
pub fn generation_target_ahead(local: u32, target: u32) -> bool {
    let delta = target.wrapping_sub(local);
    if delta != 0 && delta < GENERATION_ORDER_WINDOW && delta > GENERATION_WRAP_WARN_DELTA {
        tracing::warn!(
            target = "teraslab::record",
            local,
            target,
            delta,
            threshold = GENERATION_WRAP_WARN_DELTA,
            window = GENERATION_ORDER_WINDOW,
            "generation_target_ahead: forward delta within 2^30 of wrap-ambiguity window",
        );
        if let Some(m) = crate::metrics::allocator_metrics() {
            m.generation_wrap_warn_total.inc();
        }
    }
    delta != 0 && delta < GENERATION_ORDER_WINDOW
}

/// Return true when `local` is at or ahead of `target` under wrapping
/// generation ordering.
///
/// This is the replacement for plain `local >= target` when deciding whether
/// redo replay or replica apply has already observed a record generation.
pub fn generation_at_or_ahead(local: u32, target: u32) -> bool {
    !generation_target_ahead(local, target)
}

fn hex_short(data: &[u8]) -> String {
    if data.len() <= 4 {
        data.iter().map(|b| format!("{b:02x}")).collect()
    } else {
        let head: String = data[..4].iter().map(|b| format!("{b:02x}")).collect();
        format!("{head}...")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Layout size tests --

    #[test]
    fn utxo_slot_size() {
        assert_eq!(std::mem::size_of::<UtxoSlot>(), UTXO_SLOT_PAYLOAD_SIZE);
        assert_eq!(UTXO_SLOT_PAYLOAD_SIZE, 69);
        assert_eq!(UTXO_SLOT_CRC32_OFFSET, 69);
        assert_eq!(UTXO_SLOT_SIZE, 73);
    }

    #[test]
    fn deleted_marker_roundtrip() {
        let record_size = TxMetadata::record_size_for(80);
        let mut buf = [0u8; METADATA_SIZE];
        DeletedRecordMarker::new(record_size).to_bytes(&mut buf);

        let parsed = DeletedRecordMarker::try_parse(&buf).expect("must parse");
        let parsed_size = { parsed.record_size };
        let parsed_magic = { parsed.magic };
        assert_eq!(parsed_size, record_size);
        assert_eq!(parsed_magic, DELETED_RECORD_MAGIC);
    }

    #[test]
    fn deleted_marker_magic_distinct_from_live() {
        // A live record header must NOT be parsed as a deleted marker.
        let meta = TxMetadata::new(5);
        let mut buf = [0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        assert!(
            DeletedRecordMarker::try_parse(&buf).is_none(),
            "live header must not parse as a deleted marker"
        );
        assert_ne!(DELETED_RECORD_MAGIC, METADATA_MAGIC);
    }

    #[test]
    fn deleted_marker_rejects_torn_write() {
        let mut buf = [0u8; METADATA_SIZE];
        DeletedRecordMarker::new(4096).to_bytes(&mut buf);
        // Flip a byte inside the marker payload (not the magic): CRC must
        // reject it so a torn write is never read back as a valid skip.
        buf[4] ^= 0xFF;
        assert!(
            DeletedRecordMarker::try_parse(&buf).is_none(),
            "torn marker must fail CRC and parse as None"
        );
    }

    #[test]
    fn deleted_marker_rejects_all_zero() {
        // An all-zero header (legacy delete / reservation) is NOT a marker.
        let buf = [0u8; METADATA_SIZE];
        assert!(DeletedRecordMarker::try_parse(&buf).is_none());
    }

    // NOTE: the marker-fits-header and magic-at-offset-0 invariants are
    // enforced at COMPILE time by `const _: () = assert!(...)` next to the
    // `DeletedRecordMarker` definition, so there is no runtime test for them
    // (a runtime assert over two consts is vacuous and clippy-rejected).

    #[test]
    fn block_entry_size() {
        assert_eq!(std::mem::size_of::<BlockEntry>(), BLOCK_ENTRY_SIZE);
        assert_eq!(BLOCK_ENTRY_SIZE, 12);
    }

    #[test]
    fn metadata_size_aligned() {
        assert_eq!(METADATA_SIZE % 64, 0);
    }

    #[test]
    fn tx_flags_size() {
        assert_eq!(std::mem::size_of::<TxFlags>(), 1);
    }

    #[test]
    fn generation_order_handles_wraparound() {
        assert!(generation_at_or_ahead(7, 7));
        assert!(generation_target_ahead(7, 8));
        assert!(generation_at_or_ahead(8, 7));
        assert!(generation_target_ahead(u32::MAX, 0));
        assert!(generation_at_or_ahead(0, u32::MAX));
        assert!(!generation_target_ahead(0, 1u32 << 31));
        assert!(generation_at_or_ahead(0, 1u32 << 31));
    }

    // -- Layout field offset tests --

    #[test]
    fn utxo_slot_field_offsets() {
        assert_eq!(std::mem::offset_of!(UtxoSlot, hash), 0);
        assert_eq!(std::mem::offset_of!(UtxoSlot, status), 32);
        assert_eq!(std::mem::offset_of!(UtxoSlot, spending_data), 33);
    }

    #[test]
    fn block_entry_field_offsets() {
        assert_eq!(std::mem::offset_of!(BlockEntry, block_id), 0);
        assert_eq!(std::mem::offset_of!(BlockEntry, block_height), 4);
        assert_eq!(std::mem::offset_of!(BlockEntry, subtree_idx), 8);
    }

    // -- Round-trip serialization tests --

    #[test]
    fn utxo_slot_round_trip_known_data() {
        let mut hash = [0u8; 32];
        hash[0] = 0xAA;
        hash[31] = 0xBB;
        let mut sd = [0u8; 36];
        sd[0] = 0x01;
        sd[35] = 0x02;
        let slot = UtxoSlot {
            hash,
            status: UTXO_SPENT,
            spending_data: sd,
        };

        let mut buf = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut buf);
        let restored = UtxoSlot::from_bytes(&buf).expect("slot CRC should verify");

        assert_eq!(restored.hash, hash);
        assert_eq!(restored.status, UTXO_SPENT);
        assert_eq!(restored.spending_data, sd);
        assert_eq!(slot, restored);
    }

    #[test]
    fn utxo_slot_crc_rejects_torn_payload() {
        let slot = UtxoSlot::new_spent([0x44; 32], [0x55; 36]);
        let mut buf = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut buf);

        buf[33] ^= 0x01;

        let err = UtxoSlot::from_bytes(&buf).expect_err("torn slot payload must fail CRC");
        assert!(
            matches!(err, RecordError::CrcMismatch { .. }),
            "expected CrcMismatch, got {err:?}",
        );
    }

    #[test]
    fn utxo_slot_unspent() {
        let hash = [0x42u8; 32];
        let slot = UtxoSlot::new_unspent(hash);
        assert_eq!(slot.status, UTXO_UNSPENT);
        assert_eq!(slot.spending_data, [0u8; 36]);
        assert!(slot.is_unspent());
        assert!(!slot.is_spent());
        assert!(!slot.is_frozen());
        assert!(!slot.is_pruned());

        let mut buf = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut buf);
        let restored = UtxoSlot::from_bytes(&buf).expect("slot CRC should verify");
        assert_eq!(slot, restored);
    }

    #[test]
    fn utxo_slot_spent() {
        let hash = [0x11u8; 32];
        let mut sd = [0u8; 36];
        sd[..32].copy_from_slice(&[0xABu8; 32]); // txid
        sd[32..36].copy_from_slice(&42u32.to_le_bytes()); // vin
        let slot = UtxoSlot::new_spent(hash, sd);
        assert_eq!(slot.status, UTXO_SPENT);
        assert_eq!(slot.spending_data, sd);
        assert!(slot.is_spent());

        let mut buf = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut buf);
        let restored = UtxoSlot::from_bytes(&buf).expect("slot CRC should verify");
        assert_eq!(slot, restored);
    }

    #[test]
    fn utxo_slot_pruned() {
        let hash = [0x22u8; 32];
        let mut sd = [0u8; 36];
        sd[..32].copy_from_slice(&[0xCDu8; 32]);
        sd[32..36].copy_from_slice(&7u32.to_le_bytes());
        let slot = UtxoSlot {
            hash,
            status: UTXO_PRUNED,
            spending_data: sd,
        };
        assert!(slot.is_pruned());
        assert_eq!(slot.status, UTXO_PRUNED);
        // spending_data preserved for audit
        assert_eq!(slot.spending_data, sd);

        let mut buf = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut buf);
        let restored = UtxoSlot::from_bytes(&buf).expect("slot CRC should verify");
        assert_eq!(slot, restored);
    }

    #[test]
    fn utxo_slot_frozen() {
        let hash = [0x33u8; 32];
        let slot = UtxoSlot::new_frozen(hash);
        assert_eq!(slot.status, UTXO_FROZEN);
        assert_eq!(slot.spending_data, [FROZEN_BYTE; 36]);
        assert!(slot.is_frozen());

        let mut buf = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut buf);
        let restored = UtxoSlot::from_bytes(&buf).expect("slot CRC should verify");
        assert_eq!(slot, restored);
    }

    #[test]
    fn block_entry_round_trip() {
        let entry = BlockEntry {
            block_id: 12345,
            block_height: 800_000,
            subtree_idx: 42,
        };
        let mut buf = [0u8; BLOCK_ENTRY_SIZE];
        entry.to_bytes(&mut buf);
        let restored = BlockEntry::from_bytes(&buf);
        assert_eq!(entry, restored);
    }

    #[test]
    fn metadata_round_trip_all_fields() {
        let mut meta = TxMetadata::new(100);
        meta.tx_id = [0xABu8; 32];
        meta.tx_version = 2;
        meta.locktime = 500_000;
        meta.fee = 1000;
        meta.size_in_bytes = 250;
        meta.extended_size = 300;
        meta.flags = TxFlags::IS_COINBASE | TxFlags::LOCKED;
        meta.spending_height = 800_100;
        meta.created_at = 1710000000000;
        meta.spent_utxos = 50;
        meta.pruned_utxos = 3;
        meta.generation = 42;
        meta.updated_at = 1710000001000;
        meta.block_entry_count = 2;
        meta.block_entries_inline[0] = BlockEntry {
            block_id: 1,
            block_height: 800_000,
            subtree_idx: 10,
        };
        meta.block_entries_inline[1] = BlockEntry {
            block_id: 2,
            block_height: 800_001,
            subtree_idx: 20,
        };
        meta.unmined_since = 799_000;
        meta.delete_at_height = 801_000;
        meta.preserve_until = 802_000;
        meta.external_ref.store_type = 2;
        meta.external_ref.total_size = 1_000_000;

        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        let restored = TxMetadata::from_bytes(&buf).expect("valid CRC");
        assert_eq!(meta, restored);
    }

    #[test]
    fn metadata_magic_correct() {
        let meta = TxMetadata::new(10);
        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        let restored = TxMetadata::from_bytes(&buf).expect("valid CRC");
        assert_eq!({ restored.magic }, METADATA_MAGIC);
    }

    #[test]
    fn metadata_zero_block_entries() {
        let meta = TxMetadata::new(5);
        assert_eq!(meta.block_entry_count, 0);
        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        let restored = TxMetadata::from_bytes(&buf).expect("valid CRC");
        assert_eq!(restored.block_entry_count, 0);
    }

    #[test]
    fn metadata_three_block_entries() {
        let mut meta = TxMetadata::new(10);
        meta.block_entry_count = 3;
        meta.block_entries_inline[0] = BlockEntry {
            block_id: 100,
            block_height: 1,
            subtree_idx: 0,
        };
        meta.block_entries_inline[1] = BlockEntry {
            block_id: 200,
            block_height: 2,
            subtree_idx: 1,
        };
        meta.block_entries_inline[2] = BlockEntry {
            block_id: 300,
            block_height: 3,
            subtree_idx: 2,
        };

        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        let restored = TxMetadata::from_bytes(&buf).expect("valid CRC");

        assert_eq!(restored.block_entry_count, 3);
        assert_eq!({ restored.block_entries_inline[0].block_id }, 100);
        assert_eq!({ restored.block_entries_inline[1].block_id }, 200);
        assert_eq!({ restored.block_entries_inline[2].block_id }, 300);
    }

    #[test]
    fn tx_flags_coinbase_locked() {
        let flags = TxFlags::IS_COINBASE | TxFlags::LOCKED;
        assert_eq!(flags.bits(), 0b0000_0101);
        assert!(flags.contains(TxFlags::IS_COINBASE));
        assert!(flags.contains(TxFlags::LOCKED));
        assert!(!flags.contains(TxFlags::CONFLICTING));
        assert!(!flags.contains(TxFlags::EXTERNAL));
        assert!(!flags.contains(TxFlags::LAST_SPENT_ALL));
    }

    #[test]
    fn tx_flags_all() {
        let flags = TxFlags::IS_COINBASE
            | TxFlags::CONFLICTING
            | TxFlags::LOCKED
            | TxFlags::EXTERNAL
            | TxFlags::LAST_SPENT_ALL;
        assert_eq!(flags.bits(), 0b0001_1111);
    }

    // -- Offset calculation tests --

    #[test]
    fn utxo_slot_offset_calculation() {
        assert_eq!(TxMetadata::utxo_slot_offset(0), METADATA_SIZE as u64);
        assert_eq!(
            TxMetadata::utxo_slot_offset(1),
            METADATA_SIZE as u64 + UTXO_SLOT_SIZE as u64
        );
        assert_eq!(
            TxMetadata::utxo_slot_offset(100),
            METADATA_SIZE as u64 + 100 * UTXO_SLOT_SIZE as u64
        );
    }

    #[test]
    fn record_size_calculation() {
        assert_eq!(TxMetadata::record_size_for(0), METADATA_SIZE as u64);
        assert_eq!(
            TxMetadata::record_size_for(1),
            METADATA_SIZE as u64 + UTXO_SLOT_SIZE as u64
        );
        assert_eq!(
            TxMetadata::record_size_for(1000),
            METADATA_SIZE as u64 + 1000 * UTXO_SLOT_SIZE as u64
        );
    }

    // -- Edge case and boundary condition tests --

    #[test]
    fn utxo_slot_all_zero_hash_round_trip() {
        let hash = [0u8; 32];
        let slot = UtxoSlot::new_unspent(hash);

        let mut buf = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut buf);
        let restored = UtxoSlot::from_bytes(&buf).expect("slot CRC should verify");

        assert_eq!(restored.hash, [0u8; 32]);
        assert_eq!(restored.status, UTXO_UNSPENT);
        assert_eq!(restored.spending_data, [0u8; 36]);
        assert_eq!(slot, restored);
    }

    #[test]
    fn utxo_slot_all_ff_hash_round_trip() {
        let hash = [0xFFu8; 32];
        let slot = UtxoSlot::new_unspent(hash);

        let mut buf = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut buf);
        let restored = UtxoSlot::from_bytes(&buf).expect("slot CRC should verify");

        assert_eq!(restored.hash, [0xFFu8; 32]);
        assert_eq!(restored.status, UTXO_UNSPENT);
        assert_eq!(restored.spending_data, [0u8; 36]);
        assert_eq!(slot, restored);
    }

    #[test]
    fn block_entry_u32_max_round_trip() {
        let entry = BlockEntry {
            block_id: u32::MAX,
            block_height: u32::MAX,
            subtree_idx: u32::MAX,
        };
        let mut buf = [0u8; BLOCK_ENTRY_SIZE];
        entry.to_bytes(&mut buf);
        let restored = BlockEntry::from_bytes(&buf);

        assert_eq!({ restored.block_id }, u32::MAX);
        assert_eq!({ restored.block_height }, u32::MAX);
        assert_eq!({ restored.subtree_idx }, u32::MAX);
        assert_eq!(entry, restored);
    }

    #[test]
    fn metadata_block_entry_count_zero_ignores_inline() {
        let mut meta = TxMetadata::new(5);
        meta.block_entry_count = 0;
        // Write garbage into inline entries — should be irrelevant when count is 0
        meta.block_entries_inline[0] = BlockEntry {
            block_id: 999,
            block_height: 888,
            subtree_idx: 777,
        };
        meta.block_entries_inline[1] = BlockEntry {
            block_id: 111,
            block_height: 222,
            subtree_idx: 333,
        };

        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        let restored = TxMetadata::from_bytes(&buf).expect("valid CRC");

        // The count must be 0, meaning a consumer should not read any inline entries.
        assert_eq!(restored.block_entry_count, 0);
        // The inline bytes still survive the round-trip (raw memcpy), but they
        // are logically meaningless because count is 0.
        assert_eq!(meta, restored);
    }

    #[test]
    fn identity_prefix_roundtrips() {
        let mut m = TxMetadata::new(7);
        m.tx_id = [0xAB; 32];
        m.locktime = 0xCAFE;
        let mut buf = vec![0u8; METADATA_SIZE];
        m.to_bytes(&mut buf);

        let id = TxMetadata::read_identity_from(&buf).expect("valid identity CRC");
        assert_eq!(id.tx_id, [0xAB; 32]);
        assert_eq!(id.utxo_count, 7);
        assert_eq!(id.locktime, 0xCAFE);
    }

    #[test]
    fn identity_prefix_detects_corruption_in_covered_bytes() {
        let mut m = TxMetadata::new(3);
        m.tx_id = [0x11; 32];
        m.locktime = 5;
        let mut buf = vec![0u8; METADATA_SIZE];
        m.to_bytes(&mut buf);

        // Flip a byte inside the covered prefix (within tx_id at offset 16).
        buf[16] ^= 0xFF;
        assert!(matches!(
            TxMetadata::read_identity_from(&buf),
            Err(RecordError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn identity_prefix_rejects_deleted_marker_header() {
        // A deleted record's header is a DeletedRecordMarker, not a live
        // identity prefix — read_identity_from must reject it so the
        // get_spend hot path treats a tombstoned offset as "not present".
        let mut buf = vec![0u8; METADATA_SIZE];
        DeletedRecordMarker::new(TxMetadata::record_size_for(5)).to_bytes(&mut buf);
        assert!(TxMetadata::read_identity_from(&buf).is_err());
    }

    #[test]
    fn identity_crc_is_independent_of_mutable_fields() {
        // The identity prefix CRC must depend ONLY on write-once fields. A
        // mutation to a field outside the prefix (generation, spent_utxos)
        // followed by a full re-serialize must leave the identity bytes — and
        // their CRC — byte-for-byte unchanged. This is what lets mutation
        // restamps skip the identity slot entirely.
        let mut m = TxMetadata::new(4);
        m.tx_id = [0x22; 32];
        m.locktime = 9;
        let mut a = vec![0u8; METADATA_SIZE];
        m.to_bytes(&mut a);
        let id_a = TxMetadata::read_identity_from(&a).expect("valid");

        m.generation = 999;
        m.spent_utxos = 2;
        m.updated_at = 123_456;
        let mut b = vec![0u8; METADATA_SIZE];
        m.to_bytes(&mut b);
        let id_b = TxMetadata::read_identity_from(&b).expect("valid");

        assert_eq!(id_a, id_b);
        assert_eq!(
            a[IDENTITY_CRC_OFFSET..IDENTITY_CRC_OFFSET + 4],
            b[IDENTITY_CRC_OFFSET..IDENTITY_CRC_OFFSET + 4],
            "identity CRC must not change when mutable fields change"
        );
    }

    #[test]
    fn metadata_block_entry_count_3_inline_round_trip() {
        let mut meta = TxMetadata::new(10);
        meta.block_entry_count = INLINE_BLOCK_ENTRIES as u8;
        meta.block_entries_inline[0] = BlockEntry {
            block_id: 1000,
            block_height: 500_000,
            subtree_idx: 7,
        };
        meta.block_entries_inline[1] = BlockEntry {
            block_id: 2000,
            block_height: 500_001,
            subtree_idx: 14,
        };
        meta.block_entries_inline[2] = BlockEntry {
            block_id: 3000,
            block_height: 500_002,
            subtree_idx: 21,
        };

        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        let restored = TxMetadata::from_bytes(&buf).expect("valid CRC");

        assert_eq!(restored.block_entry_count, 3);
        for i in 0..INLINE_BLOCK_ENTRIES {
            assert_eq!({ restored.block_entries_inline[i].block_id }, {
                meta.block_entries_inline[i].block_id
            });
            assert_eq!({ restored.block_entries_inline[i].block_height }, {
                meta.block_entries_inline[i].block_height
            });
            assert_eq!({ restored.block_entries_inline[i].subtree_idx }, {
                meta.block_entries_inline[i].subtree_idx
            });
        }
        assert_eq!(meta, restored);
    }

    #[test]
    fn metadata_magic_validation_corrupted() {
        let meta = TxMetadata::new(10);
        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);

        // Corrupt the first 4 bytes (magic field).
        buf[0] = 0x00;
        buf[1] = 0x00;
        buf[2] = 0x00;
        buf[3] = 0x00;

        // Checksum-validating read must reject the corrupted header.
        match TxMetadata::from_bytes(&buf) {
            Err(RecordError::CrcMismatch { .. }) => {}
            other => panic!("expected CrcMismatch, got {other:?}"),
        }

        // The unchecked path still exposes the raw bytes for diagnostics,
        // and shows the magic field now reads as zero.
        let raw = TxMetadata::from_bytes_unchecked(&buf);
        assert_ne!({ raw.magic }, METADATA_MAGIC);
        assert_eq!({ raw.magic }, 0x0000_0000);
    }

    #[test]
    fn metadata_from_bytes_detects_single_bit_flip_anywhere() {
        let mut meta = TxMetadata::new(7);
        meta.tx_id = [0xAB; 32];
        meta.generation = 42;
        meta.updated_at = 1_700_000_000_000;
        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);

        // Flip one bit far from the magic — CRC must still catch it.
        buf[200] ^= 0x01;
        let err = TxMetadata::from_bytes(&buf).unwrap_err();
        match err {
            RecordError::CrcMismatch { expected, actual } => {
                assert_ne!(expected, actual, "CRC must differ after bit flip");
            }
            other => panic!("expected CrcMismatch, got {other:?}"),
        }
    }

    #[test]
    fn metadata_from_bytes_rejects_short_slice() {
        // REL-129: a slice shorter than METADATA_SIZE must fail closed with
        // a descriptive error in release builds too, not slice-panic. Cover
        // an empty slice, a one-byte-short slice, and an arbitrary truncation.
        for short_len in [0usize, METADATA_SIZE - 1, METADATA_SIZE / 2] {
            let buf = vec![0u8; short_len];
            let err = TxMetadata::from_bytes(&buf).unwrap_err();
            match err {
                RecordError::TooShort { actual, required } => {
                    assert_eq!(actual, short_len, "reported length must match slice");
                    assert_eq!(
                        required, METADATA_SIZE,
                        "required length must be METADATA_SIZE"
                    );
                }
                other => panic!("len={short_len}: expected TooShort, got {other:?}"),
            }
        }
    }

    #[test]
    fn metadata_crc_is_deterministic_across_round_trips() {
        let meta = TxMetadata::new(12);
        let mut a = vec![0u8; METADATA_SIZE];
        let mut b = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut a);
        meta.to_bytes(&mut b);
        assert_eq!(
            a, b,
            "two serializations of the same metadata must match byte-for-byte"
        );
    }

    #[test]
    fn metadata_crc_zeroed_field_is_rejected() {
        let meta = TxMetadata::new(3);
        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        // Zero out the CRC slot.
        buf[CRC32_OFFSET..CRC32_OFFSET + 4].copy_from_slice(&[0u8; 4]);
        let err = TxMetadata::from_bytes(&buf).unwrap_err();
        let RecordError::CrcMismatch { expected, actual } = err else {
            panic!("expected CrcMismatch, got {err:?}");
        };
        assert_eq!(expected, 0, "stored CRC was zeroed");
        assert_ne!(actual, 0, "computed CRC over populated header is non-zero");
    }

    #[test]
    fn metadata_all_fields_max_values_round_trip() {
        // Build manually to avoid overflow in TxMetadata::new() when utxo_count is u32::MAX
        let mut meta = TxMetadata::new(0);
        meta.magic = METADATA_MAGIC;
        meta.schema_version = u32::MAX;
        meta.record_size = u32::MAX;
        meta.utxo_count = u32::MAX;
        meta.tx_id = [0xFFu8; 32];
        meta.tx_version = u32::MAX;
        meta.locktime = u32::MAX;
        meta.fee = u64::MAX;
        meta.size_in_bytes = u64::MAX;
        meta.extended_size = u64::MAX;
        meta.flags = TxFlags::from_bits_truncate(0xFF);
        meta.spending_height = u32::MAX;
        meta.created_at = u64::MAX;
        meta.spent_utxos = u32::MAX;
        meta.pruned_utxos = u32::MAX;
        meta.generation = u32::MAX;
        meta.updated_at = u64::MAX;
        meta.block_entry_count = u8::MAX;
        meta.block_entries_inline = [BlockEntry {
            block_id: u32::MAX,
            block_height: u32::MAX,
            subtree_idx: u32::MAX,
        }; INLINE_BLOCK_ENTRIES];
        meta.block_overflow_offset = u64::MAX;
        meta.reassignment_offset = u64::MAX;
        meta.reassignment_count = u8::MAX;
        meta.unmined_since = u32::MAX;
        meta.delete_at_height = u32::MAX;
        meta.preserve_until = u32::MAX;
        meta.external_ref = ExternalRef {
            store_type: u8::MAX,
            content_hash: [0xFFu8; 32],
            total_size: u64::MAX,
            input_count: u32::MAX,
            output_count: u32::MAX,
            inputs_offset: u64::MAX,
            outputs_offset: u64::MAX,
        };
        meta.conflicting_children_count = u8::MAX;
        meta.conflicting_children_offset = u64::MAX;

        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        let restored = TxMetadata::from_bytes(&buf).expect("valid CRC");

        assert_eq!({ restored.magic }, METADATA_MAGIC);
        assert_eq!({ restored.schema_version }, u32::MAX);
        assert_eq!({ restored.record_size }, u32::MAX);
        assert_eq!({ restored.utxo_count }, u32::MAX);
        assert_eq!(restored.tx_id, [0xFFu8; 32]);
        assert_eq!({ restored.tx_version }, u32::MAX);
        assert_eq!({ restored.locktime }, u32::MAX);
        assert_eq!({ restored.fee }, u64::MAX);
        assert_eq!({ restored.size_in_bytes }, u64::MAX);
        assert_eq!({ restored.extended_size }, u64::MAX);
        assert_eq!({ restored.spending_height }, u32::MAX);
        assert_eq!({ restored.created_at }, u64::MAX);
        assert_eq!({ restored.spent_utxos }, u32::MAX);
        assert_eq!({ restored.pruned_utxos }, u32::MAX);
        assert_eq!({ restored.generation }, u32::MAX);
        assert_eq!({ restored.updated_at }, u64::MAX);
        assert_eq!(restored.block_entry_count, u8::MAX);
        assert_eq!({ restored.block_overflow_offset }, u64::MAX);
        assert_eq!({ restored.reassignment_offset }, u64::MAX);
        assert_eq!(restored.reassignment_count, u8::MAX);
        assert_eq!({ restored.unmined_since }, u32::MAX);
        assert_eq!({ restored.delete_at_height }, u32::MAX);
        assert_eq!({ restored.preserve_until }, u32::MAX);
        assert_eq!(restored.conflicting_children_count, u8::MAX);
        assert_eq!({ restored.conflicting_children_offset }, u64::MAX);
        assert_eq!(meta, restored);
    }

    #[test]
    fn external_ref_round_trip_through_metadata() {
        let ext = ExternalRef {
            store_type: 1,
            content_hash: {
                let mut h = [0u8; 32];
                h[0] = 0xDE;
                h[15] = 0xAD;
                h[31] = 0xBE;
                h
            },
            total_size: 10_000_000,
            input_count: 500,
            output_count: 1200,
            inputs_offset: 64,
            outputs_offset: 32768,
        };

        let mut meta = TxMetadata::new(5);
        meta.flags = TxFlags::EXTERNAL;
        meta.external_ref = ext;

        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        let restored = TxMetadata::from_bytes(&buf).expect("valid CRC");

        let rext = restored.external_ref;
        assert_eq!(rext.store_type, 1);
        assert_eq!(rext.content_hash[0], 0xDE);
        assert_eq!(rext.content_hash[15], 0xAD);
        assert_eq!(rext.content_hash[31], 0xBE);
        assert_eq!({ rext.total_size }, 10_000_000);
        assert_eq!({ rext.input_count }, 500);
        assert_eq!({ rext.output_count }, 1200);
        assert_eq!({ rext.inputs_offset }, 64);
        assert_eq!({ rext.outputs_offset }, 32768);
        assert_eq!(ext, rext);
    }

    #[test]
    fn external_ref_zeroed_all_bytes() {
        let ext = ExternalRef::zeroed();
        assert_eq!(ext.store_type, 0);
        assert_eq!(ext.content_hash, [0u8; 32]);
        assert_eq!({ ext.total_size }, 0);
        assert_eq!({ ext.input_count }, 0);
        assert_eq!({ ext.output_count }, 0);
        assert_eq!({ ext.inputs_offset }, 0);
        assert_eq!({ ext.outputs_offset }, 0);

        // Verify the raw byte representation is all zeros
        let size = std::mem::size_of::<ExternalRef>();
        let bytes =
            unsafe { std::slice::from_raw_parts((&ext as *const ExternalRef).cast::<u8>(), size) };
        assert!(bytes.iter().all(|&b| b == 0));
    }

    #[test]
    fn tx_flags_all_combinations_round_trip() {
        let all_flags = [
            TxFlags::IS_COINBASE,
            TxFlags::CONFLICTING,
            TxFlags::LOCKED,
            TxFlags::EXTERNAL,
            TxFlags::LAST_SPENT_ALL,
        ];

        // Iterate every combination of 5 flags (2^5 = 32 combinations)
        for mask in 0u8..32 {
            let mut flags = TxFlags::empty();
            for (i, &flag) in all_flags.iter().enumerate() {
                if mask & (1 << i) != 0 {
                    flags |= flag;
                }
            }

            let mut meta = TxMetadata::new(1);
            meta.flags = flags;

            let mut buf = vec![0u8; METADATA_SIZE];
            meta.to_bytes(&mut buf);
            let restored = TxMetadata::from_bytes(&buf).expect("valid CRC");

            assert_eq!(
                restored.flags, flags,
                "Flag combination mask={mask:#010b} did not survive round-trip"
            );
        }
    }

    #[test]
    fn utxo_slot_frozen_has_all_ff_spending_data() {
        let hash = [0x77u8; 32];
        let slot = UtxoSlot::new_frozen(hash);

        assert_eq!(slot.status, UTXO_FROZEN);
        for (i, &byte) in slot.spending_data.iter().enumerate() {
            assert_eq!(
                byte, 0xFF,
                "spending_data[{i}] should be 0xFF for frozen slot, got {byte:#04x}"
            );
        }
    }

    #[test]
    fn utxo_slot_spent_round_trip_preserves_spending_data() {
        let hash = [0x55u8; 32];
        // Build a specific 36-byte spending_data: txid(32) + vin(4)
        let mut sd = [0u8; 36];
        for (i, byte) in sd.iter_mut().enumerate().take(32) {
            *byte = (i as u8).wrapping_mul(7).wrapping_add(0x13);
        }
        sd[32..36].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());

        let slot = UtxoSlot::new_spent(hash, sd);

        let mut buf = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut buf);
        let restored = UtxoSlot::from_bytes(&buf).expect("slot CRC should verify");

        assert_eq!(restored.status, UTXO_SPENT);
        assert_eq!(restored.hash, hash);
        for (i, (&got, &want)) in restored.spending_data.iter().zip(sd.iter()).enumerate() {
            assert_eq!(
                got, want,
                "spending_data[{i}] mismatch: expected {want:#04x}, got {got:#04x}",
            );
        }
        assert_eq!(slot, restored);
    }
}
