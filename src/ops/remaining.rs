//! Types for remaining operations: freeze, unfreeze, reassign,
//! setConflicting, setLocked, preserveUntil, delete, getSpend.

use crate::index::TxKey;
use crate::ops::signal::Signal;

// -- Freeze --

/// Request to freeze a UTXO (set status to FROZEN, spending_data all 0xFF).
#[derive(Debug, Clone)]
pub struct FreezeRequest {
    pub tx_key: TxKey,
    pub offset: u32,
    pub utxo_hash: [u8; 32],
}

// -- Unfreeze --

/// Request to unfreeze a UTXO (set status to UNSPENT, spending_data zeroed).
#[derive(Debug, Clone)]
pub struct UnfreezeRequest {
    pub tx_key: TxKey,
    pub offset: u32,
    pub utxo_hash: [u8; 32],
}

// -- Reassign --

/// Request to reassign a frozen UTXO to a new hash.
#[derive(Debug, Clone)]
pub struct ReassignRequest {
    pub tx_key: TxKey,
    pub offset: u32,
    /// Current (old) UTXO hash — must match the stored hash.
    pub utxo_hash: [u8; 32],
    /// Replacement hash.
    pub new_utxo_hash: [u8; 32],
    /// Block height when reassignment occurs.
    pub block_height: u32,
    /// Blocks after block_height before this UTXO becomes spendable.
    pub spendable_after: u32,
}

// -- SetConflicting --

/// Request to set or clear the conflicting flag.
#[derive(Debug, Clone)]
pub struct SetConflictingRequest {
    pub tx_key: TxKey,
    pub value: bool,
    pub current_block_height: u32,
    pub block_height_retention: u32,
}

/// Response from setConflicting.
#[derive(Debug, Clone)]
pub struct SetConflictingResponse {
    pub signal: Signal,
}

// -- SetLocked --

/// Request to set or clear the locked flag.
#[derive(Debug, Clone)]
pub struct SetLockedRequest {
    pub tx_key: TxKey,
    pub value: bool,
}

// -- PreserveUntil --

/// Request to preserve a record until a specific block height.
#[derive(Debug, Clone)]
pub struct PreserveUntilRequest {
    pub tx_key: TxKey,
    pub block_height: u32,
}

/// Response from preserveUntil.
#[derive(Debug, Clone)]
pub struct PreserveUntilResponse {
    pub signal: Signal,
}

// -- Delete --

/// Request to delete a transaction record.
#[derive(Debug, Clone)]
pub struct DeleteRequest {
    pub tx_key: TxKey,
}

// -- GetSpend --

/// Request to read spending data for a specific UTXO.
#[derive(Debug, Clone)]
pub struct GetSpendRequest {
    pub tx_key: TxKey,
    pub offset: u32,
    pub utxo_hash: [u8; 32],
}

/// Response with UTXO status and spending data.
#[derive(Debug, Clone)]
pub struct GetSpendResponse {
    /// UTXO status byte (0x00=unspent, 0x01=spent, 0x02=pruned, 0xFF=frozen).
    pub status: u8,
    /// Spending data (36 bytes) — present when spent or frozen.
    pub spending_data: Option<[u8; 36]>,
    /// Transaction locktime from record metadata.
    pub locktime: u32,
}
