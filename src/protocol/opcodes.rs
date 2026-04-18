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
pub const OP_GET_COMMITTED_TOPOLOGY: u16 = 103;

// Streaming
pub const OP_STREAM_CHUNK: u16 = 200;
pub const OP_STREAM_END: u16 = 201;

// Replication (inter-node)
pub const OP_REPLICA_BATCH: u16 = 240;
pub const OP_REPLICA_ACK: u16 = 241;
/// Sent after all migration batches for a shard to confirm the target
/// has durably received the data. The target verifies and responds OK.
pub const OP_MIGRATION_COMPLETE: u16 = 242;
/// Batched variant of `OP_MIGRATION_COMPLETE`: marks multiple shards
/// as migration-complete in a single TCP frame. Eliminates the per-shard
/// round-trip overhead that made empty-shard completions take seconds
/// instead of milliseconds.
pub const OP_MIGRATION_BATCH_COMPLETE: u16 = 243;

// Cluster (inter-node)
pub const OP_HEARTBEAT: u16 = 250;

// Topology authority (inter-node)
/// Propose a new topology term to peers.
pub const OP_TOPOLOGY_PROPOSE: u16 = 251;
/// Vote on a proposed topology term.
pub const OP_TOPOLOGY_VOTE: u16 = 252;
/// Commit a quorum-achieved topology term.
pub const OP_TOPOLOGY_COMMIT: u16 = 253;

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
pub const ERR_NO_QUORUM: u16 = 15;

/// Shard data is being migrated; client should retry after a brief delay.
pub const ERR_MIGRATION_IN_PROGRESS: u16 = 19;

/// Required replication ACKs were not received within the timeout.
/// The mutation was applied locally and recorded in the redo log, but
/// the durability contract (RF copies) was not satisfied.
pub const ERR_REPLICATION_FAILED: u16 = 20;

/// OP_MIGRATION_COMPLETE arrived with `record_count > 0` but no manifest
/// hash / exact-manifest entries. Safety: without the hash, we cannot
/// verify every received record matches the source's contents, so a
/// malformed or stale frame could mark a non-empty shard migrated
/// prematurely. Sources must include a manifest when the shard has data.
pub const ERR_MIGRATION_MANIFEST_REQUIRED: u16 = 21;

/// OP_MIGRATION_COMPLETE carried a manifest hash that did not match the
/// receiver's locally computed manifest (content differs even if record
/// count matches). Distinct from `ERR_MIGRATION_IN_PROGRESS` so callers
/// can distinguish "still streaming" from "data corruption".
pub const ERR_MIGRATION_MANIFEST_MISMATCH: u16 = 22;

/// A topology vote was recorded in memory but the subsequent on-disk
/// persist (voted_term fsync) failed. Returned BEFORE the reply frame is
/// built — the proposer treats this as "no vote" and will retry. This
/// preserves the safety property: a voter never advertises a vote it
/// could lose across a crash.
pub const ERR_TOPOLOGY_PERSIST_FAILED: u16 = 23;

// Streaming errors
/// Blob stream not found for the given txid on this connection.
pub const ERR_STREAM_NOT_FOUND: u16 = 16;
/// Blob not found in blobstore (EXTERNAL_BLOB flag set but no pre-uploaded blob).
pub const ERR_BLOB_NOT_FOUND: u16 = 17;
/// Chunk offset does not match expected position in stream.
pub const ERR_STREAM_OFFSET_MISMATCH: u16 = 18;

pub const ERR_INTERNAL: u16 = 255;

/// Response status codes.
pub const STATUS_OK: u8 = 0;
pub const STATUS_ERROR: u8 = 1;
pub const STATUS_NOT_FOUND: u8 = 2;
pub const STATUS_REDIRECT: u8 = 3;
pub const STATUS_PARTIAL_ERROR: u8 = 4;
/// The mutation was applied and redo-durable locally, but the configured
/// replication ACK policy could not be satisfied AND the server is running
/// in best-effort replication mode (so the request is not rejected).
///
/// Semantics: the client received an acknowledgment that is weaker than
/// `STATUS_OK` — durability is single-node only, so if the master crashes
/// before catch-up streaming propagates this write to replicas, it may be
/// lost. Clients that require quorum durability must treat this as a
/// failure; clients that prefer availability may treat it as best-effort
/// success.
///
/// This status is only emitted when `replication_degraded_mode = "best_effort"`
/// is configured AND the actual number of replica ACKs is below the
/// configured ACK policy threshold (for the current implementation:
/// zero replica ACKs out of one or more targets — see `replicate_all_ops`
/// in `server/dispatch.rs`).
pub const STATUS_DEGRADED_DURABILITY: u8 = 5;

/// Wire flags bit indicating cold_data was pre-uploaded to blobstore.
/// Set on CreateItem.flags byte (bit 3) when the client has already
/// uploaded the blob via OP_STREAM_CHUNK/OP_STREAM_END.
pub const FLAG_EXTERNAL_BLOB: u8 = 0x08;

/// Request flag: bypass shard ownership check and read locally.
///
/// Used by test clients for replication verification — reading the same
/// record from both master and replica for byte-for-byte comparison.
pub const FLAG_LOCAL_READ: u16 = 0x0001;

/// Request flag on `OP_REPLICA_BATCH`: indicates this batch is part of a
/// shard migration (not normal replication). When set, `request_id`
/// carries the shard number and the receiver registers the shard as
/// actively receiving inbound migration data.
pub const FLAG_MIGRATION_BATCH: u16 = 0x0002;

/// Maximum frame payload size (512 MiB).
///
/// BSV mainnet already has transactions exceeding 300 MB. The wire format
/// uses a `u32` length prefix (max ~4 GB) so the encoding can handle any
/// size up to the BSV block limit. We cap at 512 MiB to provide basic DoS
/// protection while comfortably supporting the largest known transactions.
pub const MAX_FRAME_SIZE: u32 = 512 * 1024 * 1024;
