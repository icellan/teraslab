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
/// Remove children from parents' conflicting-children lists. Request payload:
/// `[count:u32 LE]` then `count` × `[parent_txid:32][child_txid:32]`. Routed by
/// the parent txid. Backs Teranode's `RemoveFromConflictingChildren`.
pub const OP_REMOVE_CONFLICTING_CHILD_BATCH: u16 = 13;

// Reads
pub const OP_GET_BATCH: u16 = 20;
pub const OP_GET_SPEND_BATCH: u16 = 21;

// Pruner
pub const OP_QUERY_OLD_UNMINED: u16 = 30;
pub const OP_PRESERVE_TRANSACTIONS: u16 = 31;
pub const OP_PROCESS_EXPIRED_PRESERVATIONS: u16 = 32;
/// Return all txids currently carrying the CONFLICTING flag (bit 0x02).
///
/// Request payload: empty. Response (`STATUS_OK`): `[count:u32 LE][txid:32]*count`.
/// Backs Teranode's `GetConflictingTxIterator`.
pub const OP_QUERY_CONFLICTING: u16 = 33;

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
/// rejected with `STATUS_ERROR` + `ERR_PAYLOAD_MALFORMED` (post-P3.10;
/// pre-P3.10 servers returned `ERR_INTERNAL`).
pub const OP_ADMIN_DIAGNOSE_KEY: u16 = 104;

/// On-wire protocol-version handshake.
///
/// Request payload: empty.
///
/// Response payload (STATUS_OK, 2 bytes):
/// ```text
///   protocol_version: u16 LE   // server's PROTOCOL_VERSION
/// ```
///
/// Clients SHOULD call this once at connection setup before issuing any
/// mutation opcode. The reported version determines:
///
/// - Whether the typed error codes added in v2 (`ERR_PAYLOAD_MALFORMED`,
///   `ERR_STORAGE_IO`, `ERR_OPCODE_UNSUPPORTED`, `ERR_RATE_LIMITED`,
///   `ERR_NOT_CLUSTERED`, `ERR_INVARIANT_VIOLATION`, `ERR_STREAM_INVARIANT`)
///   are available. Pre-v2 servers return `ERR_INTERNAL` for those
///   cases; v2+ servers return the typed code.
/// - Whether new opcodes added after the client was built are present.
///
/// Compatibility contract:
///
/// - Old clients (v1) issuing `OP_HELLO` against a v1 server receive
///   `ERR_OPCODE_UNSUPPORTED` (or `ERR_INTERNAL` on pre-P3.10 servers);
///   either way the response is parseable as "no v2 features".
/// - New clients (v2+) issuing `OP_HELLO` against a v2 server receive
///   `STATUS_OK` with the 2-byte version payload.
/// - If a client receives a version HIGHER than its compiled-in
///   `PROTOCOL_VERSION`, it MUST cap its expectations at its own
///   version — servers preserve backward compatibility for v1 wire
///   formats.
///
/// Lightweight by design: no authentication, no allocation, no
/// dependency on cluster state. Safe to spam on every reconnect.
pub const OP_HELLO: u16 = 107;

/// Inter-node query returning the responding node's `last_durable_height`
/// (deletion-tombstone design §4, height subsystem).
///
/// Pull-based by design: this is NOT a SWIM/gossip wire change. A node that
/// needs the cluster's finalized-height view queries each committed member
/// with this op on demand (the GC horizon and the rejoin-eligibility gate),
/// rather than piggybacking height on the membership payload.
///
/// Request payload: empty.
///
/// Response payload (`STATUS_OK`, fixed 4 bytes):
/// ```text
///   last_durable_height: u32 LE   (4 bytes)
/// ```
///
/// HMAC-gated as an inter-node opcode (see [`is_inter_node_auth_opcode`]): the
/// height exposes cluster-internal progress, mirroring `OP_GET_PARTITION_MAP`.
pub const OP_GET_NODE_HEIGHT: u16 = 108;

/// Encoded width of the `OP_GET_NODE_HEIGHT` response payload.
pub const NODE_HEIGHT_PAYLOAD_SIZE: usize = 4;

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
/// Pull-based migration repair (W1.1 FIX B): a node whose committed
/// topology says it should be receiving shard data — but which has
/// pending inbound migrations and no incoming pushes (e.g. it activated
/// the topology term after the sources already ran their plans) — sends
/// this to each source node. Payload:
/// `[topology_epoch:8][requester_node:8][shard_count:4][shard_id:2 × count]`.
/// The source validates the epoch against its own activated shard-table
/// version and re-runs the normal outbound migration (or re-sends the
/// completion handshake) for the listed shards. Fully idempotent.
pub const OP_MIGRATION_TRANSFER_REQUEST: u16 = 244;

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
/// The receiver does not own the requested key's shard. The accompanying
/// payload tells the client where to retry.
///
/// Two on-wire shapes carry REDIRECT data, both extended in R-041 to
/// include the source node's `shard_table_version` so the client can
/// detect a stale-route loop (server's view <= client's view → stop
/// following):
///
///   - **Top-level `STATUS_REDIRECT` payload**: encoded by
///     [`crate::protocol::codec::encode_redirect_with_version`] as
///     `[addr_len:2][addr][shard_table_version:8 (le)]`.
///     Back-compat: legacy [`crate::protocol::codec::decode_redirect`]
///     parses only the address half and ignores the trailing 8 version
///     bytes — old clients still work, just without loop detection.
///
///   - **Per-item `BatchItemError.error_data`** (`PartialError` responses
///     from `OP_SPEND_BATCH` / `OP_CREATE_BATCH` / `OP_SET_MINED_BATCH`
///     etc.): same format as above, length-prefixed addr + 8-byte
///     trailing version. Pre-R-041 this field carried raw `addr_bytes`
///     with no length prefix; the new decoder
///     [`crate::protocol::codec::decode_redirect_with_version`]
///     accepts both shapes (versioned, length-prefixed-no-version, and
///     legacy raw-addr).
///
///   - **Per-item `WireGetResult.data`** (`OP_GET_BATCH` redirect
///     responses): wire format is `[ERR_REDIRECT_byte:1]` followed by
///     the same `[addr_len:2][addr][shard_table_version:8]` payload.
///     The leading status byte preserves the legacy framing convention
///     for that path.
///
/// Clients use [`crate::protocol::codec::classify_redirect`] to decide
/// whether to follow the REDIRECT (server strictly ahead) or stop
/// (server equal or behind → loop).
pub const ERR_REDIRECT: u16 = 14;
pub const ERR_NO_QUORUM: u16 = 15;

/// Shard data is being migrated; client should retry after a brief delay.
pub const ERR_MIGRATION_IN_PROGRESS: u16 = 19;

/// Required replication ACKs were not received within the timeout.
/// The mutation was applied locally and recorded in the redo log, but
/// the durability contract (RF copies) was not satisfied.
pub const ERR_REPLICATION_FAILED: u16 = 20;

/// OP_MIGRATION_COMPLETE arrived without a manifest hash / exact-manifest
/// entries. Safety: without the hash, we cannot verify every received
/// record matches the source's contents, so a malformed or stale frame
/// could mark a shard migrated prematurely. Sources must include a
/// manifest even for empty shards.
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

/// Inter-node request or response frame failed HMAC authentication.
///
/// Returned only on cluster-control/replication sockets when a node is
/// configured with `cluster_secret` and the peer sends an unsigned,
/// malformed, stale, or wrongly-signed frame.
pub const ERR_CLUSTER_AUTH_FAILED: u16 = 27;

/// P3.10 / F-G5-017 — typed wire error codes.
///
/// The request payload failed wire-decode (truncated header, malformed
/// count prefix, oversize batch, invalid UTF-8 in addr, etc.). The client
/// must not retry blindly: the frame's bytes are wrong.
///
/// Pre-P3.10 these failures were reported as `ERR_INTERNAL` with a
/// free-text message; classifiers now read the typed code directly.
pub const ERR_PAYLOAD_MALFORMED: u16 = 28;

/// P3.10 / F-G5-017 — the dispatcher does not recognise `op_code`. May
/// indicate a client built against a newer protocol than the server, or
/// a corrupted frame. Distinct from `ERR_PAYLOAD_MALFORMED` because the
/// frame *was* decodable — only the opcode is unknown.
pub const ERR_OPCODE_UNSUPPORTED: u16 = 29;

/// P3.10 / F-G5-017 — a device read/write failure surfaced from the
/// engine or blobstore. The mutation was rejected; the client may retry,
/// but the same I/O failure is likely to recur until the operator
/// resolves the underlying issue. Distinct from `ERR_INTERNAL` (truly
/// unknown) and `ERR_REPLICATION_FAILED` (replication ack policy).
pub const ERR_STORAGE_IO: u16 = 30;

/// P3.10 / F-G5-017 — the listener's aggregate in-flight request memory
/// limit is exhausted. Retry after backoff: the limit is per-connection
/// concurrent reservation, not a per-account quota.
pub const ERR_RATE_LIMITED: u16 = 31;

/// P3.10 / F-G5-017 — a cluster control opcode (topology propose / vote
/// / commit, partition map, etc.) arrived on a server that has no
/// `RunningCluster` attached (single-node mode). The client should not
/// retry against this server — it is structurally incapable of serving
/// the request.
pub const ERR_NOT_CLUSTERED: u16 = 32;

/// P3.10 / F-G5-017 — a wire-protocol invariant was violated by the
/// caller. Currently used by inter-node opcodes that overload `request_id`
/// to carry a shard number: setting the upper 48 bits is forbidden so a
/// typo cannot silently target an unintended shard.
pub const ERR_INVARIANT_VIOLATION: u16 = 33;

// Streaming errors
/// Blob stream not found for the given txid on this connection.
pub const ERR_STREAM_NOT_FOUND: u16 = 16;
/// Blob not found in blobstore (EXTERNAL_BLOB flag set but no pre-uploaded blob).
pub const ERR_BLOB_NOT_FOUND: u16 = 17;
/// Chunk offset does not match expected position in stream.
pub const ERR_STREAM_OFFSET_MISMATCH: u16 = 18;

/// P3.10 / F-G5-017 — a stream-protocol invariant was violated (offset
/// mismatch on chunk arrival, byte counter overflow, total stream bytes
/// exceeded the configured maximum). Distinct from `ERR_PAYLOAD_MALFORMED`
/// because the frame parses correctly — the stream state machine
/// rejected it.
pub const ERR_STREAM_INVARIANT: u16 = 34;

/// F-X-022 — the reference UDF `addDeletedChildren` parity. The idempotent
/// re-spend short-circuit consulted the parent record's
/// `deleted_children` list and found the requested child txid present
/// (the spending child was pruned after originally consuming this
/// output — "resurrected-then-pruned"). Distinct from
/// `ERR_INVALID_SPEND` (which reuses the slot's `UTXO_PRUNED` payload):
/// `ERR_DELETED_CHILDREN` fires at the idempotent-respend path where
/// the slot itself still reads `UTXO_SPENT` by the requesting child,
/// but the deleted-children audit list contradicts the slot. Wire
/// payload is the single-byte child_count (count of entries in the
/// deleted-children list at the time of rejection — useful for client
/// observability and reorg-storm detection).
pub const ERR_DELETED_CHILDREN: u16 = 35;

/// KO-3 — a guarded DAH-sweep delete re-validated the record under the
/// per-tx stripe lock and found it no longer due (a concurrent
/// `PreserveUntilBatch` set/extended `preserve_until`, or the record's
/// spent/longest-chain state regressed since the sweep's lock-free
/// re-validation). The record is intentionally KEPT, not deleted; the
/// pruner counts it as a skipped candidate rather than a deletion. Only
/// ever produced by the internal sweep path
/// (`OP_PROCESS_EXPIRED_PRESERVATIONS`); a direct client `OP_DELETE_BATCH`
/// is unconditional and never returns this code.
pub const ERR_NOT_DUE: u16 = 36;

/// W1.1 FIX A — a migration completion handshake (`OP_MIGRATION_COMPLETE`
/// or `OP_MIGRATION_BATCH_COMPLETE`) or a transfer request
/// (`OP_MIGRATION_TRANSFER_REQUEST`) arrived stamped with a topology
/// epoch the receiver has NOT yet activated (its shard-table version is
/// behind the frame's epoch). The receiver cannot meaningfully
/// acknowledge a handoff for a topology it has not installed: doing so
/// previously let a source commit a master move against a CPU-starved
/// target that never registered the inbound shards, leaving them
/// permanently masterless. Retryable: the sender must treat this as
/// pending (the target will activate the term shortly) — never as a
/// completed handoff.
pub const ERR_MIGRATION_TARGET_NOT_READY: u16 = 37;

/// P3.10 / F-G5-017 — wire protocol revision.
///
/// `1` is the historical implicit version: legacy clients and servers do
/// not exchange a version handshake, so the constant exists for
/// documentation only. It is bumped to `2` in this revision because the
/// dispatcher now emits typed `ERR_PAYLOAD_MALFORMED` / `ERR_STORAGE_IO`
/// / `ERR_OPCODE_UNSUPPORTED` / `ERR_RATE_LIMITED` / `ERR_NOT_CLUSTERED`
/// / `ERR_INVARIANT_VIOLATION` / `ERR_STREAM_INVARIANT` codes in places
/// where v1 always returned `ERR_INTERNAL`. Old clients that match on
/// `ERR_INTERNAL` for specific failures (malformed frames, storage I/O,
/// unknown opcodes) must be updated to match on the new codes; new
/// clients must continue to accept `ERR_INTERNAL` as a fallback for
/// genuinely unclassified failures.
pub const PROTOCOL_VERSION: u16 = 2;

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

// ---------------------------------------------------------------------------
// CREATE-wire flag bits (the `flags` byte on a `CreateItem` / OP_CREATE_BATCH)
// ---------------------------------------------------------------------------
//
// IMPORTANT — wire vs persisted numbering are DIFFERENT namespaces.
//
// These constants describe the bit layout of the `flags` byte carried on the
// CREATE wire (decoded in `server::dispatch` create handler and in
// `replication::receiver`). The engine maps these wire bits onto the persisted
// [`crate::record::TxFlags`], which use a DIFFERENT numbering:
//
//   wire (here):       LOCKED=0x01, CONFLICTING=0x02, FROZEN=0x04, EXTERNAL_BLOB=0x08
//   persisted TxFlags: IS_COINBASE=0x01, CONFLICTING=0x02, LOCKED=0x04, EXTERNAL=0x08
//
// The footgun these named constants exist to prevent: a caller that reaches for
// the *persisted* LOCKED bit (0x04) and puts it on the CREATE wire silently
// creates a FROZEN UTXO (wire 0x04). Always build the CREATE `flags` byte from
// the `CREATE_FLAG_*` constants below, never from raw bit literals or the
// `TxFlags` constants.

/// CREATE-wire bit: create this transaction LOCKED (spends rejected until an
/// explicit `OP_SET_LOCKED_BATCH(false)` clears it). Wire 0x01 — NOT the
/// persisted `TxFlags::LOCKED` (0x04).
pub const CREATE_FLAG_LOCKED: u8 = 0x01;

/// CREATE-wire bit: create this transaction marked CONFLICTING. Wire 0x02
/// (coincides with `TxFlags::CONFLICTING`, but treat as a separate namespace).
pub const CREATE_FLAG_CONFLICTING: u8 = 0x02;

/// CREATE-wire bit: create this transaction FROZEN. Wire 0x04 — NOT the
/// persisted `TxFlags::LOCKED` (0x04); persisted FROZEN is tracked separately.
pub const CREATE_FLAG_FROZEN: u8 = 0x04;

/// CREATE-wire bit (alias of [`FLAG_EXTERNAL_BLOB`]): cold_data was pre-uploaded
/// to the blobstore via OP_STREAM_CHUNK/OP_STREAM_END. Wire 0x08.
pub const CREATE_FLAG_EXTERNAL_BLOB: u8 = 0x08;

/// Wire flags bit indicating cold_data was pre-uploaded to blobstore.
/// Set on CreateItem.flags byte (bit 3) when the client has already
/// uploaded the blob via OP_STREAM_CHUNK/OP_STREAM_END.
///
/// Retained name for existing call sites; identical to
/// [`CREATE_FLAG_EXTERNAL_BLOB`].
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

/// Request flag on `OP_MIGRATION_COMPLETE`: this is an ABORT signal, not a
/// real completion. The source could not finish streaming the shard's data
/// (baseline/late-key/delta/manifest/commit-handshake failure) and is telling
/// the target to abandon the in-flight inbound transfer: clear the
/// inbound-pending fence so the shard is not stranded forever, WITHOUT a
/// record-count or manifest check (an abort is precisely the case where the
/// target's partial copy will not match any count) and WITHOUT pruning. The
/// source remains the authoritative master+holder of the shard — the handoff
/// did NOT complete, so the target must not commit the shard to itself and the
/// source must not record a committed handoff. Idempotent: a target with no
/// matching inbound entry treats it as a no-op.
pub const FLAG_MIGRATION_ABORT: u16 = 0x0008;

/// Request flag on `OP_MIGRATION_COMPLETE` (only meaningful together with
/// `FLAG_MIGRATION_VERIFY_ONLY` and an exact-entry manifest): a SUPERSET
/// verification probe, not an exact-count completion.
///
/// # Why it exists (sc09/sc05 drain convergence)
///
/// A gracefully-draining node that re-asserted stale mastership of a NON-empty
/// shard a live peer already took over streams its (older) copy to the rightful
/// master `R`, then sends a normal completion carrying its own record count
/// `K`. But `R` already holds the shard and has accepted writes the draining
/// node never saw, so `R`'s actual count is `> K`: the exact-count completion is
/// rejected as `record count mismatch: expected K, got actual`. The draining
/// node then rolls the shard back to itself (its copy is non-empty, so the
/// no-loss guard forbids dropping it on a failed handoff) — and the handoff
/// NEVER completes, leaving ~5 phantom master shards that stall the drain.
///
/// This flag breaks that deadlock the no-loss way. After streaming, the source
/// probes `R`: "do you hold EVERY (txid, generation) in my exact manifest?".
/// The target verifies superset containment (every source entry present locally
/// with the matching generation) and ignores the exact-count equality — `R`
/// legitimately holds MORE. A `STATUS_OK` proves the source's data is safe in
/// `R`, so the source may relinquish the phantom mastership (transfer-then-
/// relinquish). The probe NEVER mutates target state (it is verify-only: no
/// prune, no commit, no inbound clear), so a mismatch is harmless.
pub const FLAG_MIGRATION_SUPERSET_OK: u16 = 0x0010;

/// Request flag on `OP_MIGRATION_COMPLETE`: the frame carries an appended
/// TOMBSTONE section for tombstone-driven migration reconciliation
/// (deletion-tombstone Phase 8, design §7). Set ONLY by a source whose
/// `tombstone_reconciliation_enabled` config is true.
///
/// # Wire layout (appended AFTER the `from_node:u64`)
///
/// ```text
///   [tombstone_section_version:1]   = TOMBSTONE_SECTION_VERSION (1)
///   [tombstone_count (M):4]         u32 LE
///   [M × 36 bytes]                  per entry: txid:[u8;32] || generation:u32 LE
/// ```
///
/// # Back-compat (load-bearing)
///
/// A receiver that does not understand this flag — or whose own
/// `tombstone_reconciliation_enabled` is false — IGNORES the flag and the
/// appended bytes entirely: every existing decode (`record_count`,
/// `manifest_hash`, exact entries, `from_node`) reads from FIXED offsets that
/// the tombstone section follows, so a flagless/older receiver parses the frame
/// EXACTLY as today and the trailing tombstone bytes are simply not read. The
/// section is version-prefixed so a future layout change is detectable; an
/// unknown version is treated as "no tombstone section" (degrades to the
/// never-received TRANSFER decision, which is no-loss).
pub const FLAG_MIGRATION_TOMBSTONES: u16 = 0x0020;

/// On-wire version byte for the `OP_MIGRATION_COMPLETE` tombstone section
/// (deletion-tombstone Phase 8). Bumped if the per-entry layout changes; a
/// receiver that sees an unrecognized version treats the section as absent.
pub const TOMBSTONE_SECTION_VERSION: u8 = 1;

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

/// Opcodes that carry cluster authority and therefore require HMAC framing
/// whenever a `cluster_secret` is configured.
///
/// F-G5-005: `OP_ADMIN_DIAGNOSE_KEY` and `OP_ADMIN_CLUSTER_HEALTH` are
/// included because they expose cluster-internal routing state
/// (per-key master id, topology epoch, pending-inbound flags, SWIM
/// state). Treating them as inter-node opcodes closes the
/// reconnaissance surface that previously let any TCP client on the
/// public port enumerate the cluster's routing tables.
///
/// E-02 (2026-05-29 audit): two opcodes are *intentionally* excluded:
///
/// - `OP_HEARTBEAT`(250) — a client-facing liveness shim kept for
///   legacy clients that have not moved to the gossip UDP transport
///   (F-G5-006). Its handler returns `STATUS_OK` with an empty payload,
///   mutates nothing, and feeds no membership state (SWIM liveness is
///   UDP and HMAC-authenticated separately in `cluster::swim`). Gating
///   it would break exactly the legacy probes it exists for while
///   protecting nothing — it discloses as little as the equally
///   unauthenticated `OP_PING`/`OP_HEALTH`.
/// - `OP_REPLICA_ACK`(241) — never arrives as a request frame: the ack
///   is the *response payload* of `OP_REPLICA_BATCH`, and response
///   frames are HMAC-verified by the sender when a `cluster_secret` is
///   configured (see `verify_frame` in
///   `replication::tcp_transport::recv_ack`). Listing it here would be
///   dead code on the request path.
///
/// `tests/g5_protocol_auth.rs` pins both exclusions so a future edit
/// that adds them fails a test instead of silently breaking legacy
/// probes or implying request-path coverage that does not exist.
pub fn is_inter_node_auth_opcode(op_code: u16) -> bool {
    matches!(
        op_code,
        OP_GET_PARTITION_MAP
            | OP_GET_COMMITTED_TOPOLOGY
            | OP_REPLICA_BATCH
            | OP_MIGRATION_COMPLETE
            | OP_MIGRATION_BATCH_COMPLETE
            | OP_MIGRATION_TRANSFER_REQUEST
            | OP_TOPOLOGY_PROPOSE
            | OP_TOPOLOGY_VOTE
            | OP_TOPOLOGY_COMMIT
            | OP_PARTITION_VERSION_REPORT
            | OP_ADMIN_DIAGNOSE_KEY
            | OP_ADMIN_CLUSTER_HEALTH
            | OP_GET_NODE_HEIGHT
    )
}

/// R-089 (GH-13): per-item upper bound on the `cold_data` payload inside
/// a `OP_CREATE_BATCH` frame.
///
/// Each create item carries a `cold_data` blob whose length is encoded
/// as a `u32`. Without a per-item cap, an attacker who fits within the
/// outer [`MAX_FRAME_SIZE`] can still concentrate the entire 16 MiB
/// budget into a single item — and the engine then allocates a `Vec`
/// of that size in `to_vec()` plus another aligned write buffer of the
/// same size. 4 MiB per item is well above any realistic transaction
/// (BSV transactions are typically a few KB; the single-tx limit in
/// the network is 10 MiB raw and most fall under 1 MiB). 4 MiB caps
/// the per-item allocation at a predictable headroom while still
/// permitting the largest legitimate transactions.
pub const MAX_COLD_DATA_PER_ITEM: u32 = 4 * 1024 * 1024;

/// R-090: maximum UTXO hashes accepted in one `OP_CREATE_BATCH` item.
///
/// The outer frame cap already prevents truly unbounded input, but without
/// a named per-item ceiling one create item can still reserve a multi-megabyte
/// `Vec<[u8; 32]>` before the engine applies any semantic validation. 131,072
/// outputs consumes 4 MiB of hash storage and is far above normal transaction
/// fanout while keeping per-item memory predictable.
pub const MAX_UTXO_HASHES_PER_CREATE_ITEM: u32 = 131_072;

/// R-090: maximum conflicting-parent txids accepted in one create item.
///
/// Parent lists are only used for conflicting-child bookkeeping. A 65,536 item
/// cap still permits pathological conflict fanout while bounding the decoder's
/// pre-allocation to 2 MiB per create item.
pub const MAX_PARENT_TXIDS_PER_CREATE_ITEM: u32 = 65_536;

#[cfg(test)]
mod create_flag_tests {
    use super::*;

    /// Mirrors the CREATE-wire decode in `server::dispatch` and
    /// `replication::receiver`: a `flags` byte → (frozen, conflicting, locked,
    /// external) using the named constants. Kept in lockstep with those call
    /// sites so the constant numbering can never silently drift.
    fn decode_create_flags(flags: u8) -> (bool, bool, bool, bool) {
        (
            flags & CREATE_FLAG_FROZEN != 0,
            flags & CREATE_FLAG_CONFLICTING != 0,
            flags & CREATE_FLAG_LOCKED != 0,
            flags & CREATE_FLAG_EXTERNAL_BLOB != 0,
        )
    }

    #[test]
    fn create_flag_constants_have_locked_layout_not_persisted_layout() {
        // The footgun guard: wire LOCKED is bit 0x01, NOT the persisted
        // TxFlags::LOCKED (0x04). Wire 0x04 is FROZEN.
        assert_eq!(CREATE_FLAG_LOCKED, 0x01);
        assert_eq!(CREATE_FLAG_CONFLICTING, 0x02);
        assert_eq!(CREATE_FLAG_FROZEN, 0x04);
        assert_eq!(CREATE_FLAG_EXTERNAL_BLOB, 0x08);
        // EXTERNAL_BLOB alias is byte-identical to the legacy name.
        assert_eq!(CREATE_FLAG_EXTERNAL_BLOB, FLAG_EXTERNAL_BLOB);
    }

    #[test]
    fn create_flag_constants_are_distinct_single_bits() {
        let bits = [
            CREATE_FLAG_LOCKED,
            CREATE_FLAG_CONFLICTING,
            CREATE_FLAG_FROZEN,
            CREATE_FLAG_EXTERNAL_BLOB,
        ];
        // Each is exactly one bit set.
        for b in bits {
            assert_eq!(b.count_ones(), 1, "flag {b:#04x} is not a single bit");
        }
        // No two share a bit (OR of all == sum of all == no overlap).
        let or_all = bits.iter().fold(0u8, |acc, b| acc | b);
        let sum_all: u8 = bits.iter().copied().sum();
        assert_eq!(or_all, sum_all, "flag bits overlap");
        assert_eq!(or_all, 0x0F);
    }

    #[test]
    fn each_named_bit_decodes_to_its_field_only() {
        assert_eq!(
            decode_create_flags(CREATE_FLAG_LOCKED),
            (false, false, true, false),
            "LOCKED must set only `locked`"
        );
        assert_eq!(
            decode_create_flags(CREATE_FLAG_CONFLICTING),
            (false, true, false, false),
            "CONFLICTING must set only `conflicting`"
        );
        assert_eq!(
            decode_create_flags(CREATE_FLAG_FROZEN),
            (true, false, false, false),
            "FROZEN must set only `frozen`"
        );
        assert_eq!(
            decode_create_flags(CREATE_FLAG_EXTERNAL_BLOB),
            (false, false, false, true),
            "EXTERNAL_BLOB must set only `external`"
        );
    }

    #[test]
    fn combined_flags_decode_independently() {
        let combined = CREATE_FLAG_LOCKED | CREATE_FLAG_FROZEN;
        assert_eq!(decode_create_flags(combined), (true, false, true, false));
        assert_eq!(decode_create_flags(0), (false, false, false, false));
    }
}
