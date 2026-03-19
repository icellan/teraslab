//! Record creation operation.
//!
//! Allocates space, initializes metadata and UTXO slots, writes the complete
//! record in one I/O operation, and registers it in the index.

use crate::index::TxKey;
use crate::record::BlockEntry;
use thiserror::Error;

/// Errors from record creation.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum CreateError {
    /// Device has no space for the requested allocation.
    #[error("device full")]
    DeviceFull,

    /// A record with this txid already exists in the index.
    #[error("duplicate txid")]
    DuplicateTxId,

    /// Zero UTXOs is not valid.
    #[error("invalid utxo count: 0")]
    InvalidUtxoCount,

    /// Device I/O or allocator error.
    #[error("storage error: {detail}")]
    StorageError { detail: String },
}

/// Block info for transactions that are already mined at creation time.
#[derive(Debug, Clone)]
pub struct MinedBlockInfo {
    /// Block ID.
    pub block_id: u32,
    /// Block height.
    pub block_height: u32,
    /// Subtree index.
    pub subtree_idx: u32,
}

/// Request to create a new transaction record.
#[derive(Debug, Clone)]
pub struct CreateRequest {
    /// Transaction hash (32 bytes).
    pub tx_id: [u8; 32],
    /// Bitcoin transaction version.
    pub tx_version: u32,
    /// Transaction locktime.
    pub locktime: u32,
    /// Transaction fee in satoshis.
    pub fee: u64,
    /// Serialized transaction size.
    pub size_in_bytes: u64,
    /// Extended metadata size.
    pub extended_size: u64,
    /// Whether this is a coinbase transaction.
    pub is_coinbase: bool,
    /// Coinbase maturity height (blockHeight + 100), 0 if not coinbase.
    pub spending_height: u32,
    /// UTXO hashes — one per output.
    pub utxo_hashes: Vec<[u8; 32]>,
    /// Raw input data (None if external or not available).
    pub inputs: Option<Vec<u8>>,
    /// Raw output data (None if external or not available).
    pub outputs: Option<Vec<u8>>,
    /// Raw inpoints data (None if not available).
    pub inpoints: Option<Vec<u8>>,
    /// Whether inputs/outputs are stored externally (blob store).
    pub is_external: bool,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: u64,
    /// Current block height (for unmined_since).
    pub block_height: u32,
    /// Pre-mined block info (empty = unmined).
    pub mined_block_infos: Vec<MinedBlockInfo>,
    /// Create all UTXOs in frozen state.
    pub frozen: bool,
    /// Create as conflicting.
    pub conflicting: bool,
    /// Create as locked.
    pub locked: bool,
    /// Parent txids for conflicting-children updates when conflicting=true.
    pub parent_txids: Vec<[u8; 32]>,
}

impl CreateRequest {
    /// Build a [`TxKey`] from this request's tx_id.
    pub fn tx_key(&self) -> TxKey {
        TxKey { txid: self.tx_id }
    }

    /// Compute block entries from mined_block_infos (up to inline limit).
    pub fn block_entries(&self) -> Vec<BlockEntry> {
        self.mined_block_infos
            .iter()
            .map(|info| BlockEntry {
                block_id: info.block_id,
                block_height: info.block_height,
                subtree_idx: info.subtree_idx,
            })
            .collect()
    }
}

/// Response from a successful record creation.
#[derive(Debug, Clone)]
pub struct CreateResponse {
    /// Device offset where the record was written.
    pub record_offset: u64,
    /// Number of UTXO slots in the record.
    pub utxo_count: u32,
}

/// Request for a batch of record creations.
#[derive(Debug, Clone)]
pub struct BatchCreateRequest {
    /// Individual creation requests.
    pub transactions: Vec<CreateRequest>,
}

/// Response from a batch creation.
#[derive(Debug, Clone)]
pub struct BatchCreateResponse {
    /// Per-transaction results. Index corresponds to the input order.
    pub results: Vec<Result<CreateResponse, CreateError>>,
}
