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
/// Per-record migration / routing diagnosis (Phase A diagnostic foundation).
///
/// Wire layout:
///
/// Request payload (from client):
/// ```text
///   count: u32 LE                        // number of txids, 1..=64
///   txid:  [u8; 32]  *  count            // raw txid bytes, no hex
/// ```
///
/// Response payload (STATUS_OK):
/// ```text
///   count: u32 LE                        // echoes request count
///   entry: [u8; KEY_DIAGNOSIS_ENCODED_SIZE] * count
///
///   each entry, in declaration order:
///     shard:                          u16 LE   (offset  0,  2 bytes)
///     this_node_id:                   u64 LE   (offset  2,  8 bytes)
///     local_view_canonical_master_id: u64 LE   (offset 10,  8 bytes)
///     has_local_data:                 u8       (offset 18,  1 byte; 0|1)
///     is_local_master_of_shard:       u8       (offset 19,  1 byte; 0|1)
///     has_pending_inbound:            u8       (offset 20,  1 byte; 0|1)
///     is_shard_fenced:                u8       (offset 21,  1 byte; 0|1)
///     is_migrating_shard:             u8       (offset 22,  1 byte; 0|1)
///     topology_epoch:                 u64 LE   (offset 23,  8 bytes)
/// ```
///
/// Total entry width is `KEY_DIAGNOSIS_ENCODED_SIZE = 31` bytes. All
/// widths are fixed (no varints) so callers can index entries by stride.
///
/// Malformed requests (count > 64, or insufficient trailing bytes) are
/// rejected with `STATUS_ERROR` + `ERR_INTERNAL`.
pub const OP_ADMIN_DIAGNOSE_KEY: u16 = 104;

/// Maximum number of txids accepted in a single `OP_ADMIN_DIAGNOSE_KEY`
/// request. The barrier in integration tests only ever inspects the
/// first ~32 failing records, so 64 is comfortably above expected use
/// while bounding worst-case CPU and response size.
pub const ADMIN_DIAGNOSE_KEY_MAX_TXIDS: u32 = 64;

/// Encoded width of a single `KeyDiagnosis` entry in the response payload
/// of `OP_ADMIN_DIAGNOSE_KEY`. See the opcode's doc comment for the
/// per-field layout.
pub const KEY_DIAGNOSIS_ENCODED_SIZE: usize = 2 + 8 + 8 + 1 + 1 + 1 + 1 + 1 + 8;

/// Per-shard partition version report exchanged during the post-commit
/// exchange phase before a migration plan is built.
///
/// After every topology commit, the coordinator collects these reports from
/// every alive peer to discover which nodes already hold which shards' data.
/// The migration plan is then computed against this *actual* distribution
/// instead of a derived-from-topology guess, eliminating master overallocation
/// (`total_masters > 4096`) and stuck migrations.
///
/// Request payload (coordinator → peer):
/// ```text
///   cluster_key: u64 LE   (8 bytes)
/// ```
///
/// Response payload (peer → coordinator):
/// ```text
///   node_id:     u64 LE   (8 bytes)
///   cluster_key: u64 LE   (8 bytes)
///   entry_count: u32 LE   (4 bytes)
///   entries: PartitionVersionEntry * count    // entry_count entries, each 12 bytes
/// ```
///
/// Each entry layout (12 bytes):
/// ```text
///   shard:            u16 LE   (2 bytes)
///   flags:            u8       (1 byte; bit0=is_master, bit1=is_subset)
///   replica_count:    u8       (1 byte)
///   last_applied_seq: u64 LE   (8 bytes)
/// ```
pub const OP_PARTITION_VERSION_REPORT: u16 = 105;

/// Encoded width of a single `PartitionVersionEntry` on the wire.
pub const PARTITION_VERSION_ENTRY_SIZE: usize = 2 + 1 + 1 + 8;

/// Phase I — admin opcode returning a snapshot of this node's cluster
/// readiness. Designed for client / test pre-flight checks so callers
/// only seed records once the cluster has settled.
///
/// Request payload: empty.
///
/// Response payload (`STATUS_OK`, fixed 17 bytes):
/// ```text
///   swim_state:                u8       (1 byte; 0=Joining, 1=Alive,
///                                              2=Suspect, 3=Dead)
///   last_committed_term:       u64 LE   (8 bytes)
///   last_topology_commit_age:  u64 LE   (8 bytes; milliseconds since
///                                       the most recent committed
///                                       topology was applied locally,
///                                       or `u64::MAX` when no commit
///                                       has been observed yet)
/// ```
pub const OP_ADMIN_CLUSTER_HEALTH: u16 = 106;

/// Encoded width of the `OP_ADMIN_CLUSTER_HEALTH` response payload.
pub const ADMIN_CLUSTER_HEALTH_PAYLOAD_SIZE: usize = 1 + 8 + 8;

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

/// Returned by a replica when an incoming `ReplicaBatch`'s `cluster_key`
/// does not match the receiver's current cluster epoch (e.g. the sender
/// is operating against a stale topology view after a master change or
/// migration boundary). The caller should refresh its routing — query
/// `OP_GET_COMMITTED_TOPOLOGY` / `OP_GET_PARTITION_MAP` — and retry the
/// request against the new master. Distinct from
/// `ERR_MIGRATION_IN_PROGRESS` (transient, same epoch) and
/// `ERR_REDIRECT` (per-key routing miss): a stale-epoch error means the
/// sender's whole view of cluster ownership is out of date.
pub const ERR_STALE_EPOCH: u16 = 24;

/// Phase I — this node is part of the cluster member set but has not yet
/// observed its first quorum-committed topology. Writes and reads
/// against a `Joining` node are rejected with this code so a client
/// that seeds against a freshly-spawned peer cannot accidentally drive
/// data into a half-formed cluster. Retryable: re-issue once the node
/// has been promoted to `Alive` (signalled by `OP_ADMIN_CLUSTER_HEALTH`).
pub const ERR_CLUSTER_NOT_READY: u16 = 25;

/// Gap #5 — a secondary index (DAH or unmined) failed to rebuild during
/// startup and the node is running with that index unavailable. Endpoints
/// that depend on the missing index reject requests with this code; the
/// regular spend / get / create / mutate paths still work because the
/// primary index is intact. Recovery requires the operator to investigate
/// the underlying I/O / device error and restart the node so the secondary
/// rebuild can be re-attempted.
pub const ERR_INDEX_DEGRADED: u16 = 26;

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

/// Request flag on `OP_MIGRATION_COMPLETE`: verify the shard manifest but
/// leave inbound migration state pending. Sources use this before a batched
/// `OP_MIGRATION_BATCH_COMPLETE` so data-bearing migrations still get exact
/// verification without forcing one durable inbound-state write per shard.
pub const FLAG_MIGRATION_VERIFY_ONLY: u16 = 0x0004;

/// Maximum frame payload size (16 MiB) for normal client/server traffic.
///
/// BSV mainnet has transactions exceeding 300 MB, but those payloads are
/// uploaded via the dedicated streaming path (`OP_STREAM_CHUNK` /
/// `OP_STREAM_END`) which fragments large blobs into many small frames.
/// All non-streaming operations — batched mutations, reads, admin queries,
/// inter-node replication, and cluster control — fit comfortably below this
/// cap: a fully-loaded `OP_GET_BATCH` of `max_batch_size = 8192` 32-byte
/// txids is < 256 KiB, and an `OP_REPLICA_BATCH` carrying thousands of
/// fixed-size ops is similarly bounded.
///
/// Lowering the cap from a permissive 512 MiB serves two purposes:
///
/// 1. **Memory-pressure DoS**. The TCP handler resizes a per-connection
///    read buffer up to the advertised frame length (see
///    `src/server/mod.rs`). With many concurrent connections, a high cap
///    lets a small number of malicious clients reserve gigabytes of RAM by
///    advertising large frames.
/// 2. **Pre-allocation amplification**. Batch decoders read a `count`
///    field and then call `Vec::with_capacity(count)`. A 16 MiB ceiling
///    bounds the worst-case pre-allocation count to roughly
///    `MAX_FRAME_SIZE / per_item_min_size` even before the per-decoder
///    `max_batch_size` guard runs, capping any residual allocation
///    amplification at hundreds of MB instead of tens of GB.
///
/// Streaming chunk frames are still bounded by this constant, which is
/// fine: the streaming path is designed around small per-chunk payloads
/// (typically 1–4 MiB) so the client can pipeline many chunks while the
/// server maintains a stable memory budget.
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;
