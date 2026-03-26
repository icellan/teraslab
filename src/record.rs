//! On-disk record layout types for TeraSlab.
//!
//! All structures are `#[repr(C, packed)]` with compile-time size assertions
//! to guarantee a stable, known byte layout on NVMe devices.

use bitflags::bitflags;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Size of a single UTXO slot in bytes.
pub const UTXO_SLOT_SIZE: usize = 69;

/// Size of a single block entry in bytes.
pub const BLOCK_ENTRY_SIZE: usize = 12;

/// Number of block entries stored inline in metadata.
pub const INLINE_BLOCK_ENTRIES: usize = 3;

/// Magic number identifying a valid TeraSlab record ("SLAB" in ASCII).
pub const METADATA_MAGIC: u32 = 0x534C_4142;

/// Current schema version.
pub const METADATA_VERSION: u32 = 1;

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
// UtxoSlot
// ---------------------------------------------------------------------------

/// A single UTXO output slot on disk.
///
/// Fixed at 69 bytes. Always pre-allocated at full size from creation, even
/// when unspent, so the record never grows on spend (eliminating copy-on-write).
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
    }

    /// Deserialize a slot from a byte slice.
    ///
    /// The source must be at least `UTXO_SLOT_SIZE` bytes.
    pub fn from_bytes(src: &[u8]) -> Self {
        debug_assert!(src.len() >= UTXO_SLOT_SIZE);
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&src[..32]);
        let status = src[32];
        let mut spending_data = [0u8; 36];
        spending_data.copy_from_slice(&src[33..69]);
        Self {
            hash,
            status,
            spending_data,
        }
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

    /// Padding to reach `METADATA_SIZE` (64-byte aligned).
    pub _padding: [u8; METADATA_PADDING],
}

impl TxMetadata {
    /// Create a new metadata header with default/zero values and the magic number set.
    pub fn new(utxo_count: u32) -> Self {
        let record_size =
            METADATA_SIZE as u32 + utxo_count * UTXO_SLOT_SIZE as u32;
        Self {
            magic: METADATA_MAGIC,
            schema_version: METADATA_VERSION,
            record_size,
            utxo_count,
            tx_id: [0u8; 32],
            tx_version: 0,
            locktime: 0,
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

    /// Serialize the entire metadata struct to a byte slice.
    ///
    /// The destination must be at least `METADATA_SIZE` bytes.
    pub fn to_bytes(&self, dst: &mut [u8]) {
        debug_assert!(dst.len() >= METADATA_SIZE);
        // Safety: TxMetadata is repr(C, packed), so we can transmute it to bytes.
        let src = unsafe {
            std::slice::from_raw_parts(
                (self as *const Self).cast::<u8>(),
                METADATA_SIZE,
            )
        };
        dst[..METADATA_SIZE].copy_from_slice(src);
    }

    /// Deserialize metadata from a byte slice.
    ///
    /// The source must be at least `METADATA_SIZE` bytes.
    pub fn from_bytes(src: &[u8]) -> Self {
        debug_assert!(src.len() >= METADATA_SIZE);
        let mut meta = std::mem::MaybeUninit::<Self>::uninit();
        // Safety: TxMetadata is repr(C, packed) and Copy. We copy exactly
        // METADATA_SIZE bytes into the struct.
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

const _: () = assert!(std::mem::size_of::<UtxoSlot>() == UTXO_SLOT_SIZE);
const _: () = assert!(std::mem::size_of::<BlockEntry>() == BLOCK_ENTRY_SIZE);
const _: () = assert!(BLOCK_ENTRY_SIZE == 12);
const _: () = assert!(UTXO_SLOT_SIZE == 69);
const _: () = assert!(std::mem::size_of::<TxFlags>() == 1);
const _: () = assert!(METADATA_SIZE.is_multiple_of(64));
const _: () = assert!(METADATA_SIZE == 256); // must not grow — conflicting_children fits in padding
const _: () = assert!(std::mem::size_of::<TxMetadata>() == METADATA_SIZE);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
        assert_eq!(std::mem::size_of::<UtxoSlot>(), UTXO_SLOT_SIZE);
        assert_eq!(UTXO_SLOT_SIZE, 69);
    }

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
        let restored = UtxoSlot::from_bytes(&buf);

        assert_eq!(restored.hash, hash);
        assert_eq!(restored.status, UTXO_SPENT);
        assert_eq!(restored.spending_data, sd);
        assert_eq!(slot, restored);
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
        let restored = UtxoSlot::from_bytes(&buf);
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
        let restored = UtxoSlot::from_bytes(&buf);
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
        let restored = UtxoSlot::from_bytes(&buf);
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
        let restored = UtxoSlot::from_bytes(&buf);
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
        let restored = TxMetadata::from_bytes(&buf);
        assert_eq!(meta, restored);
    }

    #[test]
    fn metadata_magic_correct() {
        let meta = TxMetadata::new(10);
        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        let restored = TxMetadata::from_bytes(&buf);
        assert_eq!({ restored.magic }, METADATA_MAGIC);
    }

    #[test]
    fn metadata_zero_block_entries() {
        let meta = TxMetadata::new(5);
        assert_eq!(meta.block_entry_count, 0);
        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);
        let restored = TxMetadata::from_bytes(&buf);
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
        let restored = TxMetadata::from_bytes(&buf);

        assert_eq!(restored.block_entry_count, 3);
        assert_eq!(
            { restored.block_entries_inline[0].block_id },
            100
        );
        assert_eq!(
            { restored.block_entries_inline[1].block_id },
            200
        );
        assert_eq!(
            { restored.block_entries_inline[2].block_id },
            300
        );
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
        assert_eq!(
            TxMetadata::record_size_for(0),
            METADATA_SIZE as u64
        );
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
        let restored = UtxoSlot::from_bytes(&buf);

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
        let restored = UtxoSlot::from_bytes(&buf);

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
        let restored = TxMetadata::from_bytes(&buf);

        // The count must be 0, meaning a consumer should not read any inline entries.
        assert_eq!(restored.block_entry_count, 0);
        // The inline bytes still survive the round-trip (raw memcpy), but they
        // are logically meaningless because count is 0.
        assert_eq!(meta, restored);
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
        let restored = TxMetadata::from_bytes(&buf);

        assert_eq!(restored.block_entry_count, 3);
        for i in 0..INLINE_BLOCK_ENTRIES {
            assert_eq!(
                { restored.block_entries_inline[i].block_id },
                { meta.block_entries_inline[i].block_id }
            );
            assert_eq!(
                { restored.block_entries_inline[i].block_height },
                { meta.block_entries_inline[i].block_height }
            );
            assert_eq!(
                { restored.block_entries_inline[i].subtree_idx },
                { meta.block_entries_inline[i].subtree_idx }
            );
        }
        assert_eq!(meta, restored);
    }

    #[test]
    fn metadata_magic_validation_corrupted() {
        let meta = TxMetadata::new(10);
        let mut buf = vec![0u8; METADATA_SIZE];
        meta.to_bytes(&mut buf);

        // Corrupt the first 4 bytes (magic field)
        buf[0] = 0x00;
        buf[1] = 0x00;
        buf[2] = 0x00;
        buf[3] = 0x00;

        let restored = TxMetadata::from_bytes(&buf);
        assert_ne!({ restored.magic }, METADATA_MAGIC);
        assert_eq!({ restored.magic }, 0x0000_0000);
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
        let restored = TxMetadata::from_bytes(&buf);

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
        let restored = TxMetadata::from_bytes(&buf);

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
        let bytes = unsafe {
            std::slice::from_raw_parts(
                (&ext as *const ExternalRef).cast::<u8>(),
                size,
            )
        };
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
            let restored = TxMetadata::from_bytes(&buf);

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
        let restored = UtxoSlot::from_bytes(&buf);

        assert_eq!(restored.status, UTXO_SPENT);
        assert_eq!(restored.hash, hash);
        for i in 0..36 {
            assert_eq!(
                restored.spending_data[i], sd[i],
                "spending_data[{i}] mismatch: expected {:#04x}, got {:#04x}",
                sd[i], restored.spending_data[i]
            );
        }
        assert_eq!(slot, restored);
    }
}
