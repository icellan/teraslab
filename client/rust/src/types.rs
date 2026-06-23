//! Domain types for the TeraSlab client.
//!
//! Provides Rust equivalents of all Go client types. Where the server's `teraslab`
//! crate already has the type, we re-export it. Where the Go client defines its own
//! response/parsed types, we define them here.

use crate::errors::ClientError;

// ---------------------------------------------------------------------------
// Fundamental types
// ---------------------------------------------------------------------------

/// A 32-byte transaction identifier (double-SHA256 hash).
pub type TxID = [u8; 32];

/// A 32-byte UTXO hash (SHA256).
pub type UtxoHash = [u8; 32];

/// 36 bytes: spending txid (32) + vin index (4 LE).
pub type SpendingData = [u8; 36];

// ---------------------------------------------------------------------------
// Request parameter types
// ---------------------------------------------------------------------------

/// A single item in a [`SpendBatch`](crate::Client::spend_batch) request.
#[derive(Debug, Clone)]
pub struct SpendItem {
    /// Transaction ID containing the UTXO.
    pub txid: TxID,
    /// Output index within the transaction.
    pub vout: u32,
    /// Expected UTXO hash for verification.
    pub utxo_hash: UtxoHash,
    /// The spending transaction ID (32 bytes) + vin index (4 bytes LE).
    pub spending_data: SpendingData,
}

/// Shared parameters for a [`SpendBatch`](crate::Client::spend_batch) request.
#[derive(Debug, Clone)]
pub struct SpendBatchParams {
    /// If true, skip the conflicting-flag check.
    pub ignore_conflicting: bool,
    /// If true, skip the locked-flag check.
    pub ignore_locked: bool,
    /// Current block height for retention logic.
    pub current_block_height: u32,
    /// Number of blocks to retain before pruning.
    pub block_height_retention: u32,
}

/// A single item in an [`UnspendBatch`](crate::Client::unspend_batch) request.
#[derive(Debug, Clone)]
pub struct UnspendItem {
    /// Transaction ID containing the UTXO.
    pub txid: TxID,
    /// Output index within the transaction.
    pub vout: u32,
    /// Expected UTXO hash for verification.
    pub utxo_hash: UtxoHash,
    /// Expected current spending data. The server only clears the slot if
    /// this matches the marker recorded by the original spend.
    pub spending_data: SpendingData,
}

/// Shared parameters for an [`UnspendBatch`](crate::Client::unspend_batch) request.
#[derive(Debug, Clone)]
pub struct UnspendBatchParams {
    /// Current block height for retention logic.
    pub current_block_height: u32,
    /// Number of blocks to retain before pruning.
    pub block_height_retention: u32,
}

/// Shared parameters for a [`SetMinedBatch`](crate::Client::set_mined_batch) request.
#[derive(Debug, Clone)]
pub struct SetMinedBatchParams {
    /// Block ID to associate with the transactions.
    pub block_id: u32,
    /// Block height of the mined block.
    pub block_height: u32,
    /// Subtree index within the block.
    pub subtree_idx: u32,
    /// Whether this block is on the longest chain.
    pub on_longest_chain: bool,
    /// If true, unset the mined status instead of setting it.
    pub unset_mined: bool,
    /// Current block height for retention logic.
    pub current_block_height: u32,
    /// Number of blocks to retain before pruning.
    pub block_height_retention: u32,
}

/// A single item in a [`CreateBatch`](crate::Client::create_batch) request.
#[derive(Debug, Clone)]
pub struct CreateItem {
    /// Transaction ID.
    pub txid: TxID,
    /// Transaction version.
    pub tx_version: u32,
    /// Transaction locktime.
    pub locktime: u32,
    /// Transaction fee in satoshis.
    pub fee: u64,
    /// Transaction size in bytes.
    pub size_in_bytes: u64,
    /// Extended transaction size.
    pub extended_size: u64,
    /// Whether this is a coinbase transaction.
    pub is_coinbase: bool,
    /// Spending height for coinbase maturity.
    pub spending_height: u32,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Flags byte.
    pub flags: u8,
    /// UTXO hashes for each output.
    pub utxo_hashes: Vec<UtxoHash>,
    /// Cold data (serialized inputs/outputs/inpoints).
    pub cold_data: Vec<u8>,
    /// Optional mined block ID.
    pub mined_block_id: Option<u32>,
    /// Optional mined block height.
    pub mined_block_height: Option<u32>,
    /// Optional mined subtree index.
    pub mined_subtree_idx: Option<u32>,
    /// Parent txids for conflicting-children updates when conflicting=true.
    pub parent_txids: Vec<TxID>,
}

/// A single item in a [`FreezeBatch`](crate::Client::freeze_batch) or
/// [`UnfreezeBatch`](crate::Client::unfreeze_batch) request.
#[derive(Debug, Clone)]
pub struct FreezeItem {
    /// Transaction ID containing the UTXO.
    pub txid: TxID,
    /// Output index within the transaction.
    pub vout: u32,
    /// Expected UTXO hash for verification.
    pub utxo_hash: UtxoHash,
}

/// A single item in a [`ReassignBatch`](crate::Client::reassign_batch) request.
#[derive(Debug, Clone)]
pub struct ReassignItem {
    /// Transaction ID containing the UTXO.
    pub txid: TxID,
    /// Output index within the transaction.
    pub vout: u32,
    /// Current UTXO hash to match.
    pub utxo_hash: UtxoHash,
    /// New UTXO hash to assign.
    pub new_utxo_hash: UtxoHash,
}

/// Shared parameters for a [`ReassignBatch`](crate::Client::reassign_batch) request.
#[derive(Debug, Clone)]
pub struct ReassignBatchParams {
    /// Block height at which the reassignment is effective.
    pub block_height: u32,
    /// Number of blocks after which the UTXO becomes spendable.
    pub spendable_after: u32,
}

/// Shared parameters for a [`SetConflictingBatch`](crate::Client::set_conflicting_batch) request.
#[derive(Debug, Clone)]
pub struct SetConflictingParams {
    /// Whether to set (true) or clear (false) the conflicting flag.
    pub value: bool,
    /// Current block height for retention logic.
    pub current_block_height: u32,
    /// Number of blocks to retain before pruning.
    pub block_height_retention: u32,
}

/// Shared parameters for a [`MarkLongestChainBatch`](crate::Client::mark_longest_chain_batch) request.
#[derive(Debug, Clone)]
pub struct MarkLongestChainParams {
    /// Whether to mark as on longest chain (true) or not (false).
    pub on_longest_chain: bool,
    /// Current block height for retention logic.
    pub current_block_height: u32,
    /// Number of blocks to retain before pruning.
    pub block_height_retention: u32,
}

/// A single item in a [`GetSpendBatch`](crate::Client::get_spend_batch) request.
#[derive(Debug, Clone)]
pub struct GetSpendItem {
    /// Transaction ID containing the UTXO.
    pub txid: TxID,
    /// Output index within the transaction.
    pub vout: u32,
    /// Expected UTXO hash for this output.
    pub utxo_hash: UtxoHash,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// A per-item error from a batch response.
#[derive(Debug, Clone)]
pub struct BatchItemError {
    /// 0-based index into the original request batch.
    pub item_index: u32,
    /// Error code from the server.
    pub code: u16,
    /// Additional error data (e.g., 36 bytes of spending_data for ALREADY_SPENT,
    /// or 4 bytes of required height for COINBASE_IMMATURE).
    pub data: Vec<u8>,
}

/// A per-item success result with signal and block IDs.
///
/// Returned by spend and set-mined operations.
#[derive(Debug, Clone)]
pub struct BatchItemSuccess {
    /// 0-based index into the original request batch.
    pub item_index: u32,
    /// Signal value (see `SIGNAL_*` constants).
    pub signal: u8,
    /// Block IDs associated with the transaction.
    pub block_ids: Vec<u32>,
}

/// Response from a [`SpendBatch`](crate::Client::spend_batch) or
/// [`SetMinedBatch`](crate::Client::set_mined_batch) operation.
///
/// Contains both per-item success signals and per-item errors.
#[derive(Debug, Clone)]
pub struct SpendBatchResponse {
    /// Per-item success results with signals and block IDs.
    pub successes: Vec<BatchItemSuccess>,
    /// Per-item errors (empty when all items succeeded).
    pub errors: Vec<BatchItemError>,
}

/// Generic response for mutation batch operations.
///
/// `errors` is empty when all items succeed.
#[derive(Debug, Clone)]
pub struct BatchResult {
    /// Per-item errors (empty on full success).
    pub errors: Vec<BatchItemError>,
}

/// A single item in a [`GetBatch`](crate::Client::get_batch) response.
#[derive(Debug, Clone)]
pub struct GetResult {
    /// 0 for success, 1 for error (e.g., not found).
    pub status: u8,
    /// Serialized record data selected by the field mask.
    pub data: Vec<u8>,
}

/// Byte size of each per-field metadata bit (bits 0-18).
/// Index by bit number. Bits 19+ are variable-size and not in this table.
const FIELD_SIZES: [usize; 19] = [
    4,  // 0: tx_version
    4,  // 1: locktime
    8,  // 2: fee
    8,  // 3: size_in_bytes
    8,  // 4: extended_size
    1,  // 5: flags
    4,  // 6: spending_height
    8,  // 7: created_at
    4,  // 8: spent_utxos
    4,  // 9: pruned_utxos
    4,  // 10: utxo_count
    4,  // 11: generation
    8,  // 12: updated_at
    4,  // 13: unmined_since
    4,  // 14: delete_at_height
    4,  // 15: preserve_until
    65, // 16: external_ref
    1,  // 17: reassignment_count
    1,  // 18: block_entry_count
];

/// Compute the byte offset of `target_bit` within response data encoded
/// with `field_mask`. Returns `None` if `target_bit` is not set in the mask
/// or if `target_bit` >= 19 (variable-size fields).
#[inline]
fn field_offset(field_mask: u32, target_bit: u32) -> Option<usize> {
    if target_bit >= 19 || field_mask & (1 << target_bit) == 0 {
        return None;
    }
    let mut offset = 0usize;
    for bit in 0..target_bit {
        if field_mask & (1 << bit) != 0 {
            offset += FIELD_SIZES[bit as usize];
        }
    }
    Some(offset)
}

/// Result of a [`get_batch`](crate::Client::get_batch) call.
///
/// Bundles the field mask with the per-item results so that field accessors
/// don't need the mask passed in on every call.
///
/// # Zero-alloc field access
///
/// For hot paths that process millions of records, use the typed accessors
/// (e.g. [`spent_utxos`](Self::spent_utxos), [`is_mined`](Self::is_mined))
/// which read directly from the wire bytes without allocating a `TxMetadata`
/// struct:
///
/// ```no_run
/// # use teraslab_client::*;
/// # async fn example(client: &Client) -> Result<(), ClientError> {
/// let txids: Vec<[u8; 32]> = vec![];
/// let mask = FIELD_SPENT_UTXOS | FIELD_BLOCK_ENTRY_COUNT;
/// let batch = client.get_batch(mask, &txids).await?;
/// for i in 0..batch.len() {
///     if let Some(spent) = batch.spent_utxos(i) {
///         println!("spent: {spent}");
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Full decode
///
/// When you need all fields as a struct, use [`decode_metadata`](Self::decode_metadata):
///
/// ```no_run
/// # use teraslab_client::*;
/// # async fn example(client: &Client) -> Result<(), ClientError> {
/// let txids: Vec<[u8; 32]> = vec![];
/// let batch = client.get_batch(FIELD_ALL_METADATA, &txids).await?;
/// for i in 0..batch.len() {
///     if let Some((meta, _)) = batch.decode_metadata(i)? {
///         println!("fee: {}", meta.fee);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct GetBatchResult {
    /// The field mask used in the request.
    pub field_mask: u32,
    /// Per-item results, positionally aligned with the request txids.
    pub items: Vec<GetResult>,
}

impl GetBatchResult {
    /// Number of items in the batch.
    #[inline]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the batch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns `true` if item `i` was found (status == 0).
    #[inline]
    pub fn found(&self, i: usize) -> bool {
        self.items[i].status == 0
    }

    /// Access the raw [`GetResult`] for item `i`.
    #[inline]
    pub fn item(&self, i: usize) -> &GetResult {
        &self.items[i]
    }

    /// Iterator over items with field accessors.
    ///
    /// Each yielded [`GetResultRef`] carries the field mask so you can call
    /// typed accessors without passing the mask:
    ///
    /// ```no_run
    /// # use teraslab_client::*;
    /// # async fn example(client: &Client) -> Result<(), ClientError> {
    /// # let txids = vec![];
    /// let batch = client.get_batch(FIELD_SPENT_UTXOS, &txids).await?;
    /// for record in batch.iter() {
    ///     if let Some(spent) = record.spent_utxos() {
    ///         println!("spent: {spent}");
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn iter(&self) -> GetBatchIter<'_> {
        GetBatchIter {
            field_mask: self.field_mask,
            inner: self.items.iter(),
        }
    }

    /// Decode full [`TxMetadata`] for item `i`. Allocates.
    ///
    /// Returns `Ok(None)` if the record was not found.
    /// Returns `Ok(Some((metadata, bytes_consumed)))` on success.
    pub fn decode_metadata(&self, i: usize) -> Result<Option<(TxMetadata, usize)>, ClientError> {
        if self.items[i].status != 0 {
            return Ok(None);
        }
        let (meta, n) = TxMetadata::decode(self.field_mask, &self.items[i].data)?;
        Ok(Some((meta, n)))
    }

    // -- Zero-alloc field accessors (read directly from wire bytes) --

    /// Read a `u32` field from item `i`. Zero-alloc.
    #[inline]
    fn read_u32(&self, i: usize, field_bit: u32) -> Option<u32> {
        let item = &self.items[i];
        if item.status != 0 {
            return None;
        }
        let off = field_offset(self.field_mask, field_bit)?;
        if off + 4 > item.data.len() {
            return None;
        }
        Some(u32::from_le_bytes(
            item.data[off..off + 4].try_into().unwrap(),
        ))
    }

    /// Read a `u64` field from item `i`. Zero-alloc.
    #[inline]
    fn read_u64(&self, i: usize, field_bit: u32) -> Option<u64> {
        let item = &self.items[i];
        if item.status != 0 {
            return None;
        }
        let off = field_offset(self.field_mask, field_bit)?;
        if off + 8 > item.data.len() {
            return None;
        }
        Some(u64::from_le_bytes(
            item.data[off..off + 8].try_into().unwrap(),
        ))
    }

    /// Read a `u8` field from item `i`. Zero-alloc.
    #[inline]
    fn read_u8(&self, i: usize, field_bit: u32) -> Option<u8> {
        let item = &self.items[i];
        if item.status != 0 {
            return None;
        }
        let off = field_offset(self.field_mask, field_bit)?;
        if off >= item.data.len() {
            return None;
        }
        Some(item.data[off])
    }

    // -- Typed convenience accessors --

    /// Read `tx_version` for item `i` (bit 0, u32). Zero-alloc.
    #[inline]
    pub fn tx_version(&self, i: usize) -> Option<u32> {
        self.read_u32(i, 0)
    }
    /// Read `locktime` for item `i` (bit 1, u32). Zero-alloc.
    #[inline]
    pub fn locktime(&self, i: usize) -> Option<u32> {
        self.read_u32(i, 1)
    }
    /// Read `fee` for item `i` (bit 2, u64). Zero-alloc.
    #[inline]
    pub fn fee(&self, i: usize) -> Option<u64> {
        self.read_u64(i, 2)
    }
    /// Read `size_in_bytes` for item `i` (bit 3, u64). Zero-alloc.
    #[inline]
    pub fn size_in_bytes(&self, i: usize) -> Option<u64> {
        self.read_u64(i, 3)
    }
    /// Read `extended_size` for item `i` (bit 4, u64). Zero-alloc.
    #[inline]
    pub fn extended_size(&self, i: usize) -> Option<u64> {
        self.read_u64(i, 4)
    }
    /// Read `flags` byte for item `i` (bit 5, u8). Zero-alloc.
    #[inline]
    pub fn flags(&self, i: usize) -> Option<u8> {
        self.read_u8(i, 5)
    }
    /// Read `spending_height` for item `i` (bit 6, u32). Zero-alloc.
    #[inline]
    pub fn spending_height(&self, i: usize) -> Option<u32> {
        self.read_u32(i, 6)
    }
    /// Read `created_at` for item `i` (bit 7, u64). Zero-alloc.
    #[inline]
    pub fn created_at(&self, i: usize) -> Option<u64> {
        self.read_u64(i, 7)
    }
    /// Read `spent_utxos` for item `i` (bit 8, u32). Zero-alloc.
    #[inline]
    pub fn spent_utxos(&self, i: usize) -> Option<u32> {
        self.read_u32(i, 8)
    }
    /// Read `pruned_utxos` for item `i` (bit 9, u32). Zero-alloc.
    #[inline]
    pub fn pruned_utxos(&self, i: usize) -> Option<u32> {
        self.read_u32(i, 9)
    }
    /// Read `utxo_count` for item `i` (bit 10, u32). Zero-alloc.
    #[inline]
    pub fn utxo_count(&self, i: usize) -> Option<u32> {
        self.read_u32(i, 10)
    }
    /// Read `generation` for item `i` (bit 11, u32). Zero-alloc.
    #[inline]
    pub fn generation(&self, i: usize) -> Option<u32> {
        self.read_u32(i, 11)
    }
    /// Read `updated_at` for item `i` (bit 12, u64). Zero-alloc.
    #[inline]
    pub fn updated_at(&self, i: usize) -> Option<u64> {
        self.read_u64(i, 12)
    }
    /// Read `unmined_since` for item `i` (bit 13, u32). Zero-alloc.
    #[inline]
    pub fn unmined_since(&self, i: usize) -> Option<u32> {
        self.read_u32(i, 13)
    }
    /// Read `delete_at_height` for item `i` (bit 14, u32). Zero-alloc.
    #[inline]
    pub fn delete_at_height(&self, i: usize) -> Option<u32> {
        self.read_u32(i, 14)
    }
    /// Read `preserve_until` for item `i` (bit 15, u32). Zero-alloc.
    #[inline]
    pub fn preserve_until(&self, i: usize) -> Option<u32> {
        self.read_u32(i, 15)
    }
    /// Read `reassignment_count` for item `i` (bit 17, u8). Zero-alloc.
    #[inline]
    pub fn reassignment_count(&self, i: usize) -> Option<u8> {
        self.read_u8(i, 17)
    }
    /// Read `block_entry_count` for item `i` (bit 18, u8). Zero-alloc.
    #[inline]
    pub fn block_entry_count(&self, i: usize) -> Option<u8> {
        self.read_u8(i, 18)
    }

    // -- Derived convenience accessors --

    /// Check if item `i` is conflicting (flags bit 1). Zero-alloc.
    #[inline]
    pub fn is_conflicting(&self, i: usize) -> Option<bool> {
        self.flags(i).map(|f| f & 0b0000_0010 != 0)
    }

    /// Check if item `i` is locked (flags bit 2). Zero-alloc.
    #[inline]
    pub fn is_locked(&self, i: usize) -> Option<bool> {
        self.flags(i).map(|f| f & 0b0000_0100 != 0)
    }

    /// Check if item `i` is coinbase (flags bit 0). Zero-alloc.
    #[inline]
    pub fn is_coinbase(&self, i: usize) -> Option<bool> {
        self.flags(i).map(|f| f & 0b0000_0001 != 0)
    }

    /// Check if item `i` is mined (block_entry_count > 0). Zero-alloc.
    #[inline]
    pub fn is_mined(&self, i: usize) -> Option<bool> {
        self.block_entry_count(i).map(|c| c > 0)
    }

    /// Get a [`GetResultRef`] for item `i` (zero-alloc view with field accessors).
    #[inline]
    pub fn get(&self, i: usize) -> GetResultRef<'_> {
        GetResultRef {
            field_mask: self.field_mask,
            item: &self.items[i],
        }
    }
}

impl<'a> IntoIterator for &'a GetBatchResult {
    type Item = GetResultRef<'a>;
    type IntoIter = GetBatchIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator over [`GetBatchResult`] yielding [`GetResultRef`] items.
pub struct GetBatchIter<'a> {
    field_mask: u32,
    inner: std::slice::Iter<'a, GetResult>,
}

impl<'a> Iterator for GetBatchIter<'a> {
    type Item = GetResultRef<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| GetResultRef {
            field_mask: self.field_mask,
            item,
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for GetBatchIter<'_> {}

/// Zero-alloc reference to a single item within a [`GetBatchResult`].
///
/// Carries the field mask so typed accessors don't need it passed in:
///
/// ```no_run
/// # use teraslab_client::*;
/// # async fn example(client: &Client) -> Result<(), ClientError> {
/// # let txids = vec![];
/// let batch = client.get_batch(FIELD_SPENT_UTXOS | FIELD_FLAGS, &txids).await?;
/// for record in &batch {
///     if record.found() {
///         println!("spent={:?} conflicting={:?}", record.spent_utxos(), record.is_conflicting());
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct GetResultRef<'a> {
    field_mask: u32,
    item: &'a GetResult,
}

impl<'a> GetResultRef<'a> {
    /// Returns `true` if the record was found (status == 0).
    #[inline]
    pub fn found(&self) -> bool {
        self.item.status == 0
    }
    /// Returns the raw status byte.
    #[inline]
    pub fn status(&self) -> u8 {
        self.item.status
    }
    /// Returns the raw data bytes.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.item.data
    }
    /// Returns the field mask.
    #[inline]
    pub fn field_mask(&self) -> u32 {
        self.field_mask
    }

    /// Decode full [`TxMetadata`]. Allocates.
    pub fn decode_metadata(&self) -> Result<Option<(TxMetadata, usize)>, ClientError> {
        if self.item.status != 0 {
            return Ok(None);
        }
        let (meta, n) = TxMetadata::decode(self.field_mask, &self.item.data)?;
        Ok(Some((meta, n)))
    }

    // -- Zero-alloc scalar reads --

    #[inline]
    fn read_u32(&self, field_bit: u32) -> Option<u32> {
        if self.item.status != 0 {
            return None;
        }
        let off = field_offset(self.field_mask, field_bit)?;
        if off + 4 > self.item.data.len() {
            return None;
        }
        Some(u32::from_le_bytes(
            self.item.data[off..off + 4].try_into().unwrap(),
        ))
    }

    #[inline]
    fn read_u64(&self, field_bit: u32) -> Option<u64> {
        if self.item.status != 0 {
            return None;
        }
        let off = field_offset(self.field_mask, field_bit)?;
        if off + 8 > self.item.data.len() {
            return None;
        }
        Some(u64::from_le_bytes(
            self.item.data[off..off + 8].try_into().unwrap(),
        ))
    }

    #[inline]
    fn read_u8(&self, field_bit: u32) -> Option<u8> {
        if self.item.status != 0 {
            return None;
        }
        let off = field_offset(self.field_mask, field_bit)?;
        if off >= self.item.data.len() {
            return None;
        }
        Some(self.item.data[off])
    }

    // -- Typed field accessors --

    /// Read `tx_version` (bit 0, u32). Zero-alloc.
    #[inline]
    pub fn tx_version(&self) -> Option<u32> {
        self.read_u32(0)
    }
    /// Read `locktime` (bit 1, u32). Zero-alloc.
    #[inline]
    pub fn locktime(&self) -> Option<u32> {
        self.read_u32(1)
    }
    /// Read `fee` (bit 2, u64). Zero-alloc.
    #[inline]
    pub fn fee(&self) -> Option<u64> {
        self.read_u64(2)
    }
    /// Read `size_in_bytes` (bit 3, u64). Zero-alloc.
    #[inline]
    pub fn size_in_bytes(&self) -> Option<u64> {
        self.read_u64(3)
    }
    /// Read `extended_size` (bit 4, u64). Zero-alloc.
    #[inline]
    pub fn extended_size(&self) -> Option<u64> {
        self.read_u64(4)
    }
    /// Read `flags` byte (bit 5, u8). Zero-alloc.
    #[inline]
    pub fn flags(&self) -> Option<u8> {
        self.read_u8(5)
    }
    /// Read `spending_height` (bit 6, u32). Zero-alloc.
    #[inline]
    pub fn spending_height(&self) -> Option<u32> {
        self.read_u32(6)
    }
    /// Read `created_at` (bit 7, u64). Zero-alloc.
    #[inline]
    pub fn created_at(&self) -> Option<u64> {
        self.read_u64(7)
    }
    /// Read `spent_utxos` (bit 8, u32). Zero-alloc.
    #[inline]
    pub fn spent_utxos(&self) -> Option<u32> {
        self.read_u32(8)
    }
    /// Read `pruned_utxos` (bit 9, u32). Zero-alloc.
    #[inline]
    pub fn pruned_utxos(&self) -> Option<u32> {
        self.read_u32(9)
    }
    /// Read `utxo_count` (bit 10, u32). Zero-alloc.
    #[inline]
    pub fn utxo_count(&self) -> Option<u32> {
        self.read_u32(10)
    }
    /// Read `generation` (bit 11, u32). Zero-alloc.
    #[inline]
    pub fn generation(&self) -> Option<u32> {
        self.read_u32(11)
    }
    /// Read `updated_at` (bit 12, u64). Zero-alloc.
    #[inline]
    pub fn updated_at(&self) -> Option<u64> {
        self.read_u64(12)
    }
    /// Read `unmined_since` (bit 13, u32). Zero-alloc.
    #[inline]
    pub fn unmined_since(&self) -> Option<u32> {
        self.read_u32(13)
    }
    /// Read `delete_at_height` (bit 14, u32). Zero-alloc.
    #[inline]
    pub fn delete_at_height(&self) -> Option<u32> {
        self.read_u32(14)
    }
    /// Read `preserve_until` (bit 15, u32). Zero-alloc.
    #[inline]
    pub fn preserve_until(&self) -> Option<u32> {
        self.read_u32(15)
    }
    /// Read `reassignment_count` (bit 17, u8). Zero-alloc.
    #[inline]
    pub fn reassignment_count(&self) -> Option<u8> {
        self.read_u8(17)
    }
    /// Read `block_entry_count` (bit 18, u8). Zero-alloc.
    #[inline]
    pub fn block_entry_count(&self) -> Option<u8> {
        self.read_u8(18)
    }

    // -- Derived accessors --

    /// Check if conflicting (flags bit 1). Zero-alloc.
    #[inline]
    pub fn is_conflicting(&self) -> Option<bool> {
        self.flags().map(|f| f & 0b0000_0010 != 0)
    }
    /// Check if locked (flags bit 2). Zero-alloc.
    #[inline]
    pub fn is_locked(&self) -> Option<bool> {
        self.flags().map(|f| f & 0b0000_0100 != 0)
    }
    /// Check if coinbase (flags bit 0). Zero-alloc.
    #[inline]
    pub fn is_coinbase(&self) -> Option<bool> {
        self.flags().map(|f| f & 0b0000_0001 != 0)
    }
    /// Check if mined (block_entry_count > 0). Zero-alloc.
    #[inline]
    pub fn is_mined(&self) -> Option<bool> {
        self.block_entry_count().map(|c| c > 0)
    }
}

/// A single item in a [`GetSpendBatch`](crate::Client::get_spend_batch) response.
#[derive(Debug, Clone)]
pub struct GetSpendResult {
    /// 0 for success, 1 for error.
    pub status: u8,
    /// Error code (0 on success).
    pub error_code: u16,
    /// UTXO slot status (see `SLOT_*` constants).
    pub slot_status: u8,
    /// Spending data (36 bytes: spending txid + vin index).
    pub spending_data: SpendingData,
}

/// Response from [`process_expired_preservations`](crate::Client::process_expired_preservations).
#[derive(Debug, Clone)]
pub struct ProcessExpiredResult {
    /// Number of transactions successfully deleted.
    pub deleted: u32,
    /// Number of transactions that failed to delete.
    pub failed: u32,
}

/// Cluster partition map describing the topology for client-side routing.
#[derive(Debug, Clone)]
pub struct PartitionMap {
    /// Map version (incremented on topology changes).
    pub version: u64,
    /// List of nodes in the cluster.
    pub nodes: Vec<NodeInfo>,
    /// Shard-to-node assignment table (4096 entries).
    pub assignments: Vec<u64>,
}

/// Describes a single node in the cluster.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Unique node identifier.
    pub id: u64,
    /// Network address (host:port).
    pub addr: String,
}

// ---------------------------------------------------------------------------
// Parsed data types (for interpreting GetBatch response data)
// ---------------------------------------------------------------------------

/// Parsed transaction metadata from a [`GetBatch`](crate::Client::get_batch) response.
///
/// Decode with [`TxMetadata::decode`].
#[derive(Debug, Clone, Default)]
pub struct TxMetadata {
    /// Transaction version.
    pub tx_version: u32,
    /// Transaction locktime.
    pub locktime: u32,
    /// Transaction fee in satoshis.
    pub fee: u64,
    /// Transaction size in bytes.
    pub size_in_bytes: u64,
    /// Extended transaction size.
    pub extended_size: u64,
    /// Flags byte.
    pub flags: u8,
    /// Spending height for coinbase maturity.
    pub spending_height: u32,
    /// Creation timestamp (Unix millis).
    pub created_at: u64,
    /// Number of spent UTXOs.
    pub spent_utxos: u32,
    /// Number of pruned UTXOs.
    pub pruned_utxos: u32,
    /// Total UTXO count.
    pub utxo_count: u32,
    /// Record generation counter.
    pub generation: u32,
    /// Last update timestamp (Unix millis).
    pub updated_at: u64,
    /// Block height since the transaction was unmined.
    pub unmined_since: u32,
    /// Block height at which the record should be deleted.
    pub delete_at_height: u32,
    /// Block height until which the record is preserved.
    pub preserve_until: u32,
    /// External blob reference.
    pub external_ref: ExternalRef,
    /// Number of reassignments recorded.
    pub reassignment_count: u8,
    /// Number of block entries (> 0 means mined).
    pub block_entry_count: u8,
}

/// External blob reference for large transactions.
#[derive(Debug, Clone, Default)]
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

/// The byte size of the wire metadata section when ALL_METADATA fields are requested.
/// 81 (original) + 65 (external_ref) + 1 (reassignment_count) + 1 (block_entry_count) = 148.
/// Only valid when all per-field metadata bits (0-18) are set.
pub const ALL_METADATA_SIZE: usize = 148;

impl TxMetadata {
    /// Decode transaction metadata from the raw bytes of a [`GetResult`] data field.
    ///
    /// The `field_mask` must match what was requested in the [`get_batch`](crate::Client::get_batch)
    /// call. Only the fields whose bits are set in `field_mask` are present in `data`,
    /// in the canonical bit order (bit 0 first). Fields not present get their default
    /// value (0 / false / zeroed).
    ///
    /// Returns the decoded struct and the number of bytes consumed from `data`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Protocol`] if the data is too short for the requested fields.
    pub fn decode(field_mask: u32, data: &[u8]) -> Result<(Self, usize), ClientError> {
        let mut meta = Self::default();
        let mut pos: usize = 0;

        macro_rules! need {
            ($n:expr) => {
                if pos + $n > data.len() {
                    return Err(ClientError::Protocol(format!(
                        "tx metadata: need {} more bytes at offset {}, have {}",
                        $n,
                        pos,
                        data.len()
                    )));
                }
            };
        }

        macro_rules! read_u32 {
            () => {{
                need!(4);
                let v = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                v
            }};
        }

        macro_rules! read_u64 {
            () => {{
                need!(8);
                let v = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                v
            }};
        }

        if field_mask & FIELD_TX_VERSION != 0 {
            meta.tx_version = read_u32!();
        }
        if field_mask & FIELD_LOCKTIME != 0 {
            meta.locktime = read_u32!();
        }
        if field_mask & FIELD_FEE != 0 {
            meta.fee = read_u64!();
        }
        if field_mask & FIELD_SIZE_IN_BYTES != 0 {
            meta.size_in_bytes = read_u64!();
        }
        if field_mask & FIELD_EXTENDED_SIZE != 0 {
            meta.extended_size = read_u64!();
        }
        if field_mask & FIELD_FLAGS != 0 {
            need!(1);
            meta.flags = data[pos];
            pos += 1;
        }
        if field_mask & FIELD_SPENDING_HEIGHT != 0 {
            meta.spending_height = read_u32!();
        }
        if field_mask & FIELD_CREATED_AT != 0 {
            meta.created_at = read_u64!();
        }
        if field_mask & FIELD_SPENT_UTXOS != 0 {
            meta.spent_utxos = read_u32!();
        }
        if field_mask & FIELD_PRUNED_UTXOS != 0 {
            meta.pruned_utxos = read_u32!();
        }
        if field_mask & FIELD_UTXO_COUNT != 0 {
            meta.utxo_count = read_u32!();
        }
        if field_mask & FIELD_GENERATION != 0 {
            meta.generation = read_u32!();
        }
        if field_mask & FIELD_UPDATED_AT != 0 {
            meta.updated_at = read_u64!();
        }
        if field_mask & FIELD_UNMINED_SINCE != 0 {
            meta.unmined_since = read_u32!();
        }
        if field_mask & FIELD_DELETE_AT_HEIGHT != 0 {
            meta.delete_at_height = read_u32!();
        }
        if field_mask & FIELD_PRESERVE_UNTIL != 0 {
            meta.preserve_until = read_u32!();
        }
        if field_mask & FIELD_EXTERNAL_REF != 0 {
            need!(65);
            let store_type = data[pos];
            pos += 1;
            let mut content_hash = [0u8; 32];
            content_hash.copy_from_slice(&data[pos..pos + 32]);
            pos += 32;
            let total_size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let input_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let output_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let inputs_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let outputs_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            meta.external_ref = ExternalRef {
                store_type,
                content_hash,
                total_size,
                input_count,
                output_count,
                inputs_offset,
                outputs_offset,
            };
        }
        if field_mask & FIELD_REASSIGNMENT_COUNT != 0 {
            need!(1);
            meta.reassignment_count = data[pos];
            pos += 1;
        }
        if field_mask & FIELD_BLOCK_ENTRY_COUNT != 0 {
            need!(1);
            meta.block_entry_count = data[pos];
            pos += 1;
        }

        Ok((meta, pos))
    }
}

/// The byte size of the raw on-disk metadata struct (FIELD_RAW_METADATA).
pub const RAW_METADATA_SIZE: usize = 256;

/// Full on-disk metadata struct returned by [`FIELD_RAW_METADATA`].
///
/// Contains every field including internal storage details (magic, schema
/// version, device offsets, padding). Intended for debugging and diagnostics.
///
/// Decode with [`TxMetadataRaw::decode`].
#[derive(Debug, Clone)]
pub struct TxMetadataRaw {
    /// Raw 256-byte on-disk representation.
    pub bytes: [u8; RAW_METADATA_SIZE],
    // Parsed convenience accessors for the most commonly inspected fields:
    /// Magic number (should be 0x534C4142 = "SLAB").
    pub magic: u32,
    /// Schema version.
    pub schema_version: u32,
    /// Total record size on disk.
    pub record_size: u32,
    /// UTXO count.
    pub utxo_count: u32,
    /// Transaction ID (32 bytes).
    pub tx_id: [u8; 32],
    /// Flags byte.
    pub flags: u8,
    /// Spent UTXO count.
    pub spent_utxos: u32,
    /// Block entry count.
    pub block_entry_count: u8,
    /// Block overflow device offset (0 = no overflow).
    pub block_overflow_offset: u64,
    /// Reassignment device offset (0 = none).
    pub reassignment_offset: u64,
    /// Reassignment count.
    pub reassignment_count: u8,
    /// Conflicting children count.
    pub conflicting_children_count: u8,
    /// Conflicting children device offset (0 = none).
    pub conflicting_children_offset: u64,
}

impl TxMetadataRaw {
    /// Decode the full on-disk metadata struct from a [`FIELD_RAW_METADATA`]
    /// response.
    ///
    /// The data must be at least 256 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Protocol`] if the data is too short.
    pub fn decode(data: &[u8]) -> Result<Self, ClientError> {
        if data.len() < RAW_METADATA_SIZE {
            return Err(ClientError::Protocol(format!(
                "raw metadata: need {} bytes, have {}",
                RAW_METADATA_SIZE,
                data.len()
            )));
        }
        let mut bytes = [0u8; RAW_METADATA_SIZE];
        bytes.copy_from_slice(&data[..RAW_METADATA_SIZE]);

        let mut tx_id = [0u8; 32];
        tx_id.copy_from_slice(&data[16..48]);

        // The on-disk layout is repr(C, packed) TxMetadataRaw from record.rs.
        // We parse the key fields by their known offsets in that struct.
        //
        // Offsets (from record.rs TxMetadataRaw):
        //   0: magic (u32)
        //   4: schema_version (u32)
        //   8: record_size (u32)
        //  12: utxo_count (u32)
        //  16: tx_id ([u8; 32])
        //  48: tx_version (u32)
        //  52: locktime (u32)
        //  56: fee (u64)
        //  64: size_in_bytes (u64)
        //  72: extended_size (u64)
        //  80: flags (u8)
        //  81: spending_height (u32)
        //  85: created_at (u64)
        //  93: spent_utxos (u32)
        //  97: pruned_utxos (u32)
        // 101: generation (u32)
        // 105: updated_at (u64)
        // 113: block_entry_count (u8)
        // 114: block_entries_inline (3 × BlockEntry(12) = 36)
        // 150: block_overflow_offset (u64)
        // 158: reassignment_offset (u64)
        // 166: reassignment_count (u8)
        // 167: unmined_since (u32)
        // 171: delete_at_height (u32)
        // 175: preserve_until (u32)
        // 179: external_ref (ExternalRef = 65 bytes)
        // 244: conflicting_children_count (u8)
        // 245: conflicting_children_offset (u64)
        // 253: _padding (3 bytes to reach 256)

        Ok(Self {
            bytes,
            magic: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            schema_version: u32::from_le_bytes(data[4..8].try_into().unwrap()),
            record_size: u32::from_le_bytes(data[8..12].try_into().unwrap()),
            utxo_count: u32::from_le_bytes(data[12..16].try_into().unwrap()),
            tx_id,
            flags: data[80],
            spent_utxos: u32::from_le_bytes(data[93..97].try_into().unwrap()),
            block_entry_count: data[113],
            block_overflow_offset: u64::from_le_bytes(data[150..158].try_into().unwrap()),
            reassignment_offset: u64::from_le_bytes(data[158..166].try_into().unwrap()),
            reassignment_count: data[166],
            conflicting_children_count: data[244],
            conflicting_children_offset: u64::from_le_bytes(data[245..253].try_into().unwrap()),
        })
    }
}

/// Represents a single UTXO slot from a [`GetBatch`](crate::Client::get_batch) response.
#[derive(Debug, Clone)]
pub struct UtxoSlot {
    /// UTXO hash.
    pub hash: UtxoHash,
    /// Slot status (see `SLOT_*` constants).
    pub status: u8,
    /// Spending data (36 bytes: spending txid + vin index). Zeroed if unspent.
    pub spending_data: SpendingData,
}

/// Size of a single serialized UTXO slot: hash(32) + status(1) + spending_data(36) = 69 bytes.
const UTXO_SLOT_SIZE: usize = 69;

impl UtxoSlot {
    /// Decode UTXO slots from the raw bytes.
    ///
    /// The data starts with a 4-byte LE count, followed by `count` slot entries
    /// of 69 bytes each: hash(32) + status(1) + spending_data(36).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Protocol`] if the data is truncated.
    pub fn decode_slots(data: &[u8]) -> Result<Vec<Self>, ClientError> {
        if data.len() < 4 {
            return Err(ClientError::Protocol(format!(
                "utxo slots: need 4 bytes, have {}",
                data.len()
            )));
        }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let needed = 4 + count * UTXO_SLOT_SIZE;
        if data.len() < needed {
            return Err(ClientError::Protocol(format!(
                "utxo slots: need {} bytes, have {}",
                needed,
                data.len()
            )));
        }
        let mut slots = Vec::with_capacity(count);
        let mut pos = 4;
        for _ in 0..count {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&data[pos..pos + 32]);
            let status = data[pos + 32];
            let mut spending_data = [0u8; 36];
            spending_data.copy_from_slice(&data[pos + 33..pos + 69]);
            slots.push(Self {
                hash,
                status,
                spending_data,
            });
            pos += UTXO_SLOT_SIZE;
        }
        Ok(slots)
    }
}

/// Represents a single block entry from a [`GetBatch`](crate::Client::get_batch) response.
#[derive(Debug, Clone)]
pub struct BlockEntry {
    /// Block ID.
    pub block_id: u32,
    /// Block height.
    pub block_height: u32,
    /// Subtree index within the block.
    pub subtree_idx: u32,
}

/// Size of a single serialized block entry: block_id(4) + block_height(4) + subtree_idx(4) = 12.
const BLOCK_ENTRY_SIZE: usize = 12;

impl BlockEntry {
    /// Decode block entries from the raw bytes.
    ///
    /// The data starts with a 1-byte count. Only up to 3 inline entries are sent.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Protocol`] if the data is truncated.
    pub fn decode_entries(data: &[u8]) -> Result<Vec<Self>, ClientError> {
        if data.is_empty() {
            return Err(ClientError::Protocol(
                "block entries: need 1 byte, have 0".to_string(),
            ));
        }
        let count = data[0] as usize;
        // Only up to 3 inline entries are sent on the wire.
        let inline_count = count.min(3);
        let needed = 1 + inline_count * BLOCK_ENTRY_SIZE;
        if data.len() < needed {
            return Err(ClientError::Protocol(
                "block entries: truncated".to_string(),
            ));
        }
        let mut entries = Vec::with_capacity(inline_count);
        let mut pos = 1;
        for _ in 0..inline_count {
            entries.push(Self {
                block_id: u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()),
                block_height: u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap()),
                subtree_idx: u32::from_le_bytes(data[pos + 8..pos + 12].try_into().unwrap()),
            });
            pos += BLOCK_ENTRY_SIZE;
        }
        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Field mask constants
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Per-field mask constants (must match server FieldMask bits)
// ---------------------------------------------------------------------------

/// Include transaction version (4 bytes).
pub const FIELD_TX_VERSION: u32 = 1 << 0;
/// Include transaction locktime (4 bytes).
pub const FIELD_LOCKTIME: u32 = 1 << 1;
/// Include transaction fee (8 bytes).
pub const FIELD_FEE: u32 = 1 << 2;
/// Include transaction size in bytes (8 bytes).
pub const FIELD_SIZE_IN_BYTES: u32 = 1 << 3;
/// Include extended transaction size (8 bytes).
pub const FIELD_EXTENDED_SIZE: u32 = 1 << 4;
/// Include flags byte (1 byte).
pub const FIELD_FLAGS: u32 = 1 << 5;
/// Include spending height (4 bytes).
pub const FIELD_SPENDING_HEIGHT: u32 = 1 << 6;
/// Include creation timestamp (8 bytes).
pub const FIELD_CREATED_AT: u32 = 1 << 7;
/// Include spent UTXO count (4 bytes).
pub const FIELD_SPENT_UTXOS: u32 = 1 << 8;
/// Include pruned UTXO count (4 bytes).
pub const FIELD_PRUNED_UTXOS: u32 = 1 << 9;
/// Include total UTXO count (4 bytes).
pub const FIELD_UTXO_COUNT: u32 = 1 << 10;
/// Include record generation counter (4 bytes).
pub const FIELD_GENERATION: u32 = 1 << 11;
/// Include last update timestamp (8 bytes).
pub const FIELD_UPDATED_AT: u32 = 1 << 12;
/// Include unmined_since block height (4 bytes).
pub const FIELD_UNMINED_SINCE: u32 = 1 << 13;
/// Include delete_at_height (4 bytes).
pub const FIELD_DELETE_AT_HEIGHT: u32 = 1 << 14;
/// Include preserve_until block height (4 bytes).
pub const FIELD_PRESERVE_UNTIL: u32 = 1 << 15;
/// Include external blob reference (65 bytes).
pub const FIELD_EXTERNAL_REF: u32 = 1 << 16;
/// Include reassignment count (1 byte).
pub const FIELD_REASSIGNMENT_COUNT: u32 = 1 << 17;
/// Include block entry count (1 byte).
pub const FIELD_BLOCK_ENTRY_COUNT: u32 = 1 << 18;
/// Include UTXO slot data in GetBatch response (variable).
pub const FIELD_UTXO_SLOTS: u32 = 1 << 19;
/// Include cold data in GetBatch response (variable).
pub const FIELD_COLD_DATA: u32 = 1 << 20;
/// Include block entries in GetBatch response (variable).
pub const FIELD_BLOCK_ENTRIES: u32 = 1 << 21;
/// Include conflicting children txids in GetBatch response (variable).
pub const FIELD_CONFLICTING_CHILDREN: u32 = 1 << 22;
/// Include the raw on-disk metadata struct (256 bytes, for debugging).
/// When set, the full struct is returned as-is including internal fields.
/// Takes precedence over per-field metadata bits if both are set.
pub const FIELD_RAW_METADATA: u32 = 1 << 23;

/// Convenience alias: all per-field metadata bits (bits 0-18).
pub const FIELD_ALL_METADATA: u32 = 0x0007_FFFF;
/// Include all client-facing fields in GetBatch response (bits 0-22, excludes RAW_METADATA).
pub const FIELD_ALL: u32 = 0x007F_FFFF;

// ---------------------------------------------------------------------------
// Slot status constants
// ---------------------------------------------------------------------------

/// UTXO slot is unspent and available.
pub const SLOT_UNSPENT: u8 = 0x00;
/// UTXO slot has been spent.
pub const SLOT_SPENT: u8 = 0x01;
/// UTXO slot has been pruned.
pub const SLOT_PRUNED: u8 = 0x02;
/// UTXO slot is frozen (locked for reassignment).
pub const SLOT_FROZEN: u8 = 0xFF;

// ---------------------------------------------------------------------------
// Signal constants
// ---------------------------------------------------------------------------

/// No signal.
pub const SIGNAL_NONE: u8 = 0;
/// All UTXOs in the transaction are now spent.
pub const SIGNAL_ALL_SPENT: u8 = 1;
/// Not all UTXOs are spent yet.
pub const SIGNAL_NOT_ALL_SPENT: u8 = 2;
/// The delete_at_height field was set on this transaction.
pub const SIGNAL_DELETE_AT_HEIGHT_SET: u8 = 3;
/// The delete_at_height field was unset on this transaction.
pub const SIGNAL_DELETE_AT_HEIGHT_UNSET: u8 = 4;
/// The transaction was preserved.
pub const SIGNAL_PRESERVE: u8 = 5;

// ---------------------------------------------------------------------------
// Cluster constants
// ---------------------------------------------------------------------------

/// Number of shards in the cluster hash table.
pub const NUM_SHARDS: usize = 4096;
