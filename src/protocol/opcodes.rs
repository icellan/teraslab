//! Operation codes for the TeraSlab binary wire protocol.
//!
//! Every operation has a batch variant. Single-item ops are batches of size 1.

// Mutations
pub const OP_SPEND_BATCH: u16 = 1;
pub const OP_UNSPEND_BATCH: u16 = 2;
pub const OP_SET_MINED_BATCH: u16 = 3;
pub const OP_CREATE_BATCH: u16 = 4;
pub const OP_FREEZE_BATCH: u16 = 5;
pub const OP_UNFREEZE_BATCH: u16 = 6;
pub const OP_REASSIGN_BATCH: u16 = 7;
pub const OP_SET_CONFLICTING_BATCH: u16 = 8;
pub const OP_SET_LOCKED_BATCH: u16 = 9;
pub const OP_PRESERVE_UNTIL_BATCH: u16 = 10;
pub const OP_DELETE_BATCH: u16 = 11;
pub const OP_MARK_LONGEST_CHAIN_BATCH: u16 = 12;

// Reads
pub const OP_GET_BATCH: u16 = 20;
pub const OP_GET_SPEND_BATCH: u16 = 21;

// Pruner
pub const OP_QUERY_OLD_UNMINED: u16 = 30;
pub const OP_PRESERVE_TRANSACTIONS: u16 = 31;
pub const OP_PROCESS_EXPIRED_PRESERVATIONS: u16 = 32;

// Cluster / admin
pub const OP_GET_PARTITION_MAP: u16 = 100;
pub const OP_HEALTH: u16 = 101;
pub const OP_PING: u16 = 102;

// Streaming
pub const OP_STREAM_CHUNK: u16 = 200;
pub const OP_STREAM_END: u16 = 201;

// Replication (inter-node)
pub const OP_REPLICA_BATCH: u16 = 240;
pub const OP_REPLICA_ACK: u16 = 241;

// Cluster (inter-node)
pub const OP_HEARTBEAT: u16 = 250;

// Compatibility
pub const OP_INCREMENT_SPENT_EXTRA_RECS: u16 = 255;

/// Error codes shared across all batch operations.
pub const ERR_OK: u16 = 0;
pub const ERR_TX_NOT_FOUND: u16 = 1;
pub const ERR_UTXO_HASH_MISMATCH: u16 = 2;
pub const ERR_ALREADY_SPENT: u16 = 3;
pub const ERR_ALREADY_FROZEN: u16 = 4;
pub const ERR_UTXO_NOT_FROZEN: u16 = 5;
pub const ERR_INVALID_SPEND: u16 = 6;
pub const ERR_FROZEN: u16 = 7;
pub const ERR_CONFLICTING: u16 = 8;
pub const ERR_LOCKED: u16 = 9;
pub const ERR_COINBASE_IMMATURE: u16 = 10;
pub const ERR_VOUT_OUT_OF_RANGE: u16 = 11;
pub const ERR_ALREADY_EXISTS: u16 = 12;
pub const ERR_FROZEN_UNTIL: u16 = 13;
pub const ERR_REDIRECT: u16 = 14;
pub const ERR_INTERNAL: u16 = 255;

/// Response status codes.
pub const STATUS_OK: u8 = 0;
pub const STATUS_ERROR: u8 = 1;
pub const STATUS_NOT_FOUND: u8 = 2;
pub const STATUS_REDIRECT: u8 = 3;
pub const STATUS_PARTIAL_ERROR: u8 = 4;

/// Maximum frame payload size (16 MiB).
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;
