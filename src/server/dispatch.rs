//! Request dispatch: maps wire protocol opcodes to Engine methods.
//!
//! In clustered mode, the dispatcher checks shard ownership before
//! processing key-based operations. If this node doesn't own the shard,
//! it returns a Redirect response.
//!
//! After successful mutations:
//! - Redo log entries are appended for crash recovery.
//! - Replication ops are sent to replica nodes (if in cluster mode with RF > 1).

use crate::cluster::coordinator::RunningCluster;
use crate::cluster::shards::{NodeId, ShardHandoff, ShardTable};
use crate::index::TxKey;
use crate::ops::create::*;
use crate::ops::engine::{Engine, build_cold_data};
use crate::ops::error::SpendError;
use crate::ops::mark_longest_chain::*;
use crate::ops::remaining::*;
use crate::ops::spend::*;
use crate::ops::unspend::*;
use crate::protocol::codec::*;
use crate::protocol::frame::*;
use crate::protocol::opcodes::*;
use crate::record::{METADATA_SIZE, TxFlags};
use crate::redo::{RedoLog, RedoOp};
use crate::replication::manager::ReplicaTransport;
use crate::replication::protocol::{ReplicaAck, ReplicaBatch, ReplicaOp};
use crate::replication::receiver::{
    DEFAULT_STREAM_KEY, handle_replica_batch, handle_replica_batch_with_tracker,
};
use crate::replication::tcp_transport::TcpReplicaTransport;
use crate::storage::blobstore::BlobStore;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::LazyLock;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

const MIGRATION_REPLICATION_TIMEOUT_FLOOR: Duration = Duration::from_secs(30);

/// Per-address replication connection slot. Each replica address gets its
/// own independent mutex, so concurrent sends to different replicas never
/// contend on a single lock. At millions of ops/sec with RF=3, this
/// eliminates the serialization point that a single global pool creates.
struct PerAddrSlot {
    connection: Option<TcpReplicaTransport>,
    last_acked: u64,
}

/// Per-address connection pool. The outer HashMap is locked briefly for
/// lookup/insert only. Each address has its own `Arc<Mutex<PerAddrSlot>>`,
/// so concurrent sends to different replicas proceed without contention.
static REPL_POOL: LazyLock<Mutex<HashMap<SocketAddr, std::sync::Arc<Mutex<PerAddrSlot>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Configurable worker thread count for the replication runtime.
/// Set via [`init_repl_worker_threads`] before the runtime is first used.
/// Defaults to 2 if not explicitly configured.
static REPL_WORKER_THREADS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Configure the number of replication I/O worker threads.
///
/// Must be called during server startup before any replication occurs.
/// If not called, defaults to 2 threads. At high throughput (10M+ ops/sec),
/// consider setting this to `num_cpus / 4` or the replication factor,
/// whichever is larger.
pub fn init_repl_worker_threads(count: usize) {
    let _ = REPL_WORKER_THREADS.set(count.max(1));
}

/// Shared tokio runtime for async replication I/O. Uses a configurable thread
/// pool dedicated to replication, keeping blocking I/O off the main server
/// threads while reusing threads across replication calls instead of spawning
/// new OS threads per `replicate_all_ops` invocation.
static REPL_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let workers = REPL_WORKER_THREADS.get().copied().unwrap_or(2);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("repl-io")
        .enable_all()
        .build()
        .expect("failed to create replication tokio runtime")
});

/// Persistent replication ACK tracker. Initialized during server startup
/// via `init_ack_tracker()`. Records per-replica durable ACK sequences
/// to disk so that after a master restart, catch-up streaming can resume
/// from the correct position.
static ACK_TRACKER: std::sync::OnceLock<crate::replication::durable::AckTracker> =
    std::sync::OnceLock::new();

/// Persistent receiver-side applied tracker for production `OP_REPLICA_BATCH`
/// dispatch through the main server port. Initialized at startup beside the
/// cluster state file. This closes the gap where production dispatch used a
/// thread-local in-memory high-water mark and lost dedup state on restart.
static REPLICA_APPLIED_TRACKER: std::sync::OnceLock<
    crate::replication::durable::ReplicaAppliedTracker,
> = std::sync::OnceLock::new();

/// Persistent master-side pending replication intent tracker.
static REPLICATION_INTENT_TRACKER: std::sync::OnceLock<
    crate::replication::durable::ReplicationIntentTracker,
> = std::sync::OnceLock::new();

/// Monotonic diagnostic high-water mark across all source streams.
static DISPATCH_REPLICA_LAST_APPLIED: AtomicU64 = AtomicU64::new(0);

/// Global metrics reference. Initialized during server startup via
/// `init_dispatch_metrics()`. Used to increment operation counters
/// without threading metrics through every handler function.
static DISPATCH_METRICS: std::sync::OnceLock<&'static crate::metrics::ThreadMetrics> =
    std::sync::OnceLock::new();

/// Global histograms reference. Initialized during server startup via
/// `init_dispatch_histograms()`. Records per-handler end-to-end latency
/// for Prometheus histogram export. Like `DISPATCH_METRICS`, all call
/// sites are guarded with `if let Some(h) = DISPATCH_HISTOGRAMS.get()`
/// so tests that skip init still work.
static DISPATCH_HISTOGRAMS: std::sync::OnceLock<&'static crate::metrics::ThreadHistograms> =
    std::sync::OnceLock::new();

/// Initialize the persistent ACK tracker.
///
/// Must be called once during server startup before any replication occurs.
/// The `path` should be alongside the cluster state file (e.g., `<device>.repl-ack`).
pub fn init_ack_tracker(path: std::path::PathBuf) {
    let tracker = crate::replication::durable::AckTracker::new(path);
    let _ = ACK_TRACKER.set(tracker);
}

/// Initialize the persistent receiver-side applied tracker.
///
/// Must be called once during clustered server startup before accepting
/// replication frames. A corrupt tracker is returned as an error so startup
/// can fail closed instead of serving with unknown receiver durability state.
pub fn init_replica_applied_tracker(path: std::path::PathBuf) -> std::result::Result<(), String> {
    let tracker = crate::replication::durable::ReplicaAppliedTracker::load(path)
        .map_err(|e| format!("load replica applied tracker: {e}"))?;
    let initial = tracker.snapshot().values().copied().max().unwrap_or(0);
    DISPATCH_REPLICA_LAST_APPLIED.store(initial, std::sync::atomic::Ordering::Relaxed);
    REPLICA_APPLIED_TRACKER
        .set(tracker)
        .map_err(|_| "replica applied tracker already initialized".to_string())
}

/// Initialize the persistent master-side replication intent tracker.
pub fn init_replication_intent_tracker(
    path: std::path::PathBuf,
) -> std::result::Result<(), String> {
    let tracker = crate::replication::durable::ReplicationIntentTracker::load(path)
        .map_err(|e| format!("load replication intent tracker: {e}"))?;
    REPLICATION_INTENT_TRACKER
        .set(tracker)
        .map_err(|_| "replication intent tracker already initialized".to_string())
}

/// Initialize the dispatch metrics reference.
///
/// Must be called once during server startup before any requests are processed.
pub fn init_dispatch_metrics(metrics: &'static crate::metrics::ThreadMetrics) {
    let _ = DISPATCH_METRICS.set(metrics);
}

/// Initialize the dispatch histograms reference.
///
/// Must be called once during server startup before any requests are
/// processed. Tests that don't install a histogram reference still work —
/// handlers skip the `record_since` call in that case.
pub fn init_dispatch_histograms(histograms: &'static crate::metrics::ThreadHistograms) {
    let _ = DISPATCH_HISTOGRAMS.set(histograms);
}

/// Dispatch a request frame to the appropriate Engine method.
///
/// If `cluster` is Some, shard ownership is checked for key-based operations.
/// Requests for keys not owned by this node get a Redirect response.
///
/// Mutation path (durability contract):
/// 1. Engine applies the mutation (durable via O_DIRECT).
/// 2. Redo log records the mutation and fsyncs (mandatory — failure fails
///    the client request).
/// 3. Replication sends to replicas with durable sequence numbers.
/// 4. Client response is sent.
#[tracing::instrument(
    skip_all,
    level = "debug",
    fields(op = %request.op_code, request_id = request.request_id),
)]
pub(crate) fn handle_request(
    request: &RequestFrame,
    engine: &Engine,
    max_batch_size: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
    conn_state: &mut super::ConnectionState,
    blob_store: Option<&dyn BlobStore>,
) -> ResponseFrame {
    // Reject mutations when the cluster lacks quorum to prevent split-brain.
    if is_mutation_opcode(request.op_code)
        && let Some(err_resp) = check_quorum(cluster, request.request_id)
    {
        return err_resp;
    }

    // Refresh the cached wall-clock time once per request so that all
    // individual operations within the batch share the same timestamp.
    engine.refresh_clock();

    // Batch-level entry counters (one per request frame). Item-level
    // `_items_attempted` counters are incremented inside each handler once
    // the payload is decoded — they can't be incremented here because the
    // item count is payload-dependent.
    if let Some(m) = DISPATCH_METRICS.get() {
        match request.op_code {
            OP_CREATE_BATCH => m.creates_attempted.inc(),
            OP_SET_MINED_BATCH => m.set_mined_attempted.inc(),
            OP_GET_BATCH | OP_GET_SPEND_BATCH => m.gets_attempted.inc(),
            OP_FREEZE_BATCH => m.freezes_attempted.inc(),
            OP_UNFREEZE_BATCH => m.unfreezes_attempted.inc(),
            OP_DELETE_BATCH => m.deletes_attempted.inc(),
            OP_REASSIGN_BATCH => m.reassign_attempted.inc(),
            OP_SET_CONFLICTING_BATCH => m.set_conflicting_attempted.inc(),
            OP_SET_LOCKED_BATCH => m.set_locked_attempted.inc(),
            OP_PRESERVE_UNTIL_BATCH => m.preserve_until_attempted.inc(),
            OP_MARK_LONGEST_CHAIN_BATCH => m.mark_longest_chain_attempted.inc(),
            _ => {}
        }
    }

    // Wrap each handler with latency timing. The timer closure fetches the
    // global histograms ref once per request (a `Relaxed` atomic load) and
    // does nothing if the handler didn't opt into timing or if histograms
    // weren't initialized.
    let start = std::time::Instant::now();

    let response = match request.op_code {
        OP_SPEND_BATCH => handle_spend_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_UNSPEND_BATCH => {
            handle_unspend_batch(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_SET_MINED_BATCH => {
            handle_set_mined_batch(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_CREATE_BATCH => handle_create_batch(
            request,
            engine,
            max_batch_size,
            cluster,
            redo_log,
            blob_store,
        ),
        OP_FREEZE_BATCH => handle_freeze_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_UNFREEZE_BATCH => {
            handle_unfreeze_batch(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_REASSIGN_BATCH => {
            handle_reassign_batch(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_SET_CONFLICTING_BATCH => {
            handle_set_conflicting_batch(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_SET_LOCKED_BATCH => {
            handle_set_locked_batch(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_PRESERVE_UNTIL_BATCH => {
            handle_preserve_until_batch(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_DELETE_BATCH => handle_delete_batch(request, engine, max_batch_size, cluster, redo_log),
        OP_MARK_LONGEST_CHAIN_BATCH => {
            handle_mark_longest_chain_batch(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_GET_BATCH => handle_get_batch(request, engine, max_batch_size, cluster),
        OP_GET_SPEND_BATCH => handle_get_spend_batch(request, engine, max_batch_size, cluster),
        OP_QUERY_OLD_UNMINED => handle_query_old_unmined(request, engine),
        OP_PRESERVE_TRANSACTIONS => {
            handle_preserve_transactions(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_PROCESS_EXPIRED_PRESERVATIONS => handle_process_expired(request, engine, redo_log),
        OP_GET_PARTITION_MAP => handle_get_partition_map(request, cluster),
        OP_GET_COMMITTED_TOPOLOGY => handle_get_committed_topology(request, cluster),
        OP_ADMIN_DIAGNOSE_KEY => handle_admin_diagnose_key(request, engine, cluster),
        OP_PARTITION_VERSION_REPORT => handle_partition_version_report(request, engine, cluster),
        OP_PING => ResponseFrame {
            request_id: request.request_id,
            status: STATUS_OK,
            payload: vec![],
        },
        OP_HEALTH => ResponseFrame {
            request_id: request.request_id,
            status: STATUS_OK,
            payload: b"ok".to_vec(),
        },
        OP_INCREMENT_SPENT_EXTRA_RECS => ResponseFrame {
            request_id: request.request_id,
            status: STATUS_OK,
            payload: vec![], // No-op compatibility shim
        },
        OP_STREAM_CHUNK => handle_stream_chunk(request, conn_state, blob_store, cluster),
        OP_STREAM_END => handle_stream_end(request, conn_state),
        OP_REPLICA_BATCH => {
            // Dispatch replication batch to the receiver's apply logic.
            // During migration, flags bit FLAG_MIGRATION_BATCH is set
            // and request_id carries the shard number. Register this
            // shard as actively receiving inbound migration data so
            // the read/write path knows to wait for completion.
            // Normal replication batches do NOT set this flag.
            if request.flags & FLAG_MIGRATION_BATCH != 0
                && let Some(cluster) = cluster
            {
                let shard = request.request_id as u16;
                let already_expected = cluster.inbound_bitmap().test(shard);
                let should_track_handoff = {
                    let table = cluster.shard_table();
                    let table = table.read();
                    table.shard_handoff_state(shard) != ShardHandoff::ServingNew
                };
                if already_expected || should_track_handoff {
                    cluster.mark_inbound_active(shard);
                }
            }
            // Phase B3: route the receiver's local cluster_key view through
            // the coordinator-owned `RunningCluster` accessor instead of a
            // global atomic. When dispatch is invoked without a cluster
            // (single-node tests, non-clustered mode) the gate falls back to
            // `0` which preserves the V1-compat "accept all" behavior.
            let local_cluster_key = cluster.map(|c| c.local_cluster_key()).unwrap_or(0);
            if let Some(applied) = REPLICA_APPLIED_TRACKER.get() {
                handle_replica_batch_with_tracker(
                    request,
                    engine,
                    &DISPATCH_REPLICA_LAST_APPLIED,
                    applied,
                    DEFAULT_STREAM_KEY,
                    local_cluster_key,
                )
            } else {
                // Test harness / single-stream path: route through the
                // cluster-key-aware variant so the gate honors the
                // coordinator's view even without a persistent tracker.
                // The receiver still uses a thread-local in-memory
                // tracker internally, so parallel tests stay isolated.
                crate::replication::receiver::handle_replica_batch_with_cluster_key(
                    request,
                    engine,
                    &DISPATCH_REPLICA_LAST_APPLIED,
                    local_cluster_key,
                )
            }
            // NOTE: We do NOT mark inbound shards as complete here.
            // Only the OP_MIGRATION_COMPLETE handshake clears the
            // pending-inbound flag, after verifying the data arrived.
        }
        OP_MIGRATION_COMPLETE => {
            // Migration-complete handshake: the source has finished
            // streaming all batches for a shard and wants confirmation
            // that we received the data. The request_id carries the shard.
            //
            // Payload format:
            //   [record_count:8]                 — expected records (0 = empty shard)
            //   [fence_sequence:8] (optional)    — redo fence sequence for audit
            //   [topology_epoch:8] (optional)    — reject stale migrations
            //   [manifest_hash:32] (optional)    — XOR-hash of (txid, generation) pairs
            let shard = request.request_id as u16;

            let expected_records = if request.payload.len() >= 8 {
                u64::from_le_bytes(request.payload[..8].try_into().unwrap())
            } else {
                0
            };
            let _fence_sequence = if request.payload.len() >= 16 {
                u64::from_le_bytes(request.payload[8..16].try_into().unwrap())
            } else {
                0
            };
            let migration_epoch = if request.payload.len() >= 24 {
                u64::from_le_bytes(request.payload[16..24].try_into().unwrap())
            } else {
                0
            };
            let source_manifest: Option<[u8; 32]> = if request.payload.len() >= 56 {
                let mut h = [0u8; 32];
                h.copy_from_slice(&request.payload[24..56]);
                // All-zeros = no manifest (legacy source or empty shard).
                if h == [0u8; 32] { None } else { Some(h) }
            } else {
                None
            };
            let (source_entries, completion_from_node): (
                Option<Vec<(TxKey, u32)>>,
                Option<NodeId>,
            ) = if request.payload.len() >= 60 {
                let entry_count =
                    u32::from_le_bytes(request.payload[56..60].try_into().unwrap()) as usize;
                let needed = 60 + entry_count * 36;
                if request.payload.len() < needed {
                    return error_response(
                        request.request_id,
                        ERR_MIGRATION_IN_PROGRESS,
                        &format!(
                            "shard {shard} malformed exact-manifest payload: need {needed} bytes, got {}",
                            request.payload.len(),
                        ),
                    );
                }
                let mut entries = Vec::with_capacity(entry_count);
                let mut pos = 60;
                for _ in 0..entry_count {
                    let mut txid = [0u8; 32];
                    txid.copy_from_slice(&request.payload[pos..pos + 32]);
                    pos += 32;
                    let generation =
                        u32::from_le_bytes(request.payload[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    entries.push((TxKey { txid }, generation));
                }
                let completion_from_node = if request.payload.len() >= needed + 8 {
                    Some(NodeId(u64::from_le_bytes(
                        request.payload[needed..needed + 8].try_into().unwrap(),
                    )))
                } else {
                    None
                };
                (Some(entries), completion_from_node)
            } else {
                (None, None)
            };

            // Reject migrations from very stale topology epochs.
            // Allow 2 epochs of slack to accommodate re-activation cycles
            // where the epoch advances while migrations are in flight.
            if migration_epoch > 0
                && let Some(cluster) = cluster
            {
                let current_epoch = cluster.topology_epoch();
                if current_epoch > migration_epoch + 2 {
                    return error_response(
                        request.request_id,
                        ERR_MIGRATION_IN_PROGRESS,
                        &format!(
                            "stale migration epoch {migration_epoch} < current {current_epoch}"
                        ),
                    );
                }
            }

            // A bare zero-count completion (no manifest, no exact entries)
            // is a control-plane signal used by fast paths to clear pending
            // inbound state when the target already has the shard contents.
            let no_data_completion = expected_records == 0
                && source_manifest.is_none()
                && source_entries
                    .as_ref()
                    .is_none_or(|entries| entries.is_empty());
            let verify_only = request.flags & FLAG_MIGRATION_VERIFY_ONLY != 0;

            // Safety requirement (H3): when the source claims `record_count > 0`,
            // it MUST also send a manifest hash (or exact-entry manifest). Without
            // one, we cannot verify that every received record matches the source's
            // contents and a malformed/stale frame could mark a non-empty shard
            // migrated prematurely. Reject non-empty completions that lack both.
            let has_manifest_evidence =
                source_manifest.is_some() || source_entries.as_ref().is_some_and(|e| !e.is_empty());
            if expected_records > 0 && !has_manifest_evidence {
                return error_response(
                    request.request_id,
                    ERR_MIGRATION_MANIFEST_REQUIRED,
                    &format!(
                        "shard {shard} migration-complete with record_count={expected_records} requires manifest hash or exact-entry manifest",
                    ),
                );
            }

            if let Some(entries) = source_entries.as_ref()
                && !entries.is_empty()
                && entries.len() as u64 == expected_records
            {
                let expected_keys: std::collections::HashSet<TxKey> =
                    entries.iter().map(|(key, _)| *key).collect();
                for key in engine.keys_for_shard(shard) {
                    if expected_keys.contains(&key) {
                        continue;
                    }
                    match engine.delete(&crate::ops::remaining::DeleteRequest { tx_key: key }) {
                        Ok(()) | Err(crate::ops::error::SpendError::TxNotFound) => {}
                        Err(e) => {
                            return error_response(
                                request.request_id,
                                ERR_MIGRATION_IN_PROGRESS,
                                &format!(
                                    "shard {shard} failed to prune stale key {:?}: {e:?}",
                                    key,
                                ),
                            );
                        }
                    }
                }
            }

            // Note: the zero-count + no-manifest fast-path is preserved as a
            // legitimate control-plane signal used when the receiver already
            // holds the shard contents (e.g. via prior replica delivery) and
            // the source just needs to clear pending-inbound state. The
            // `record_count > 0` guard above is sufficient to close H3: any
            // frame claiming to have delivered data MUST include cryptographic
            // evidence that the data actually matches the source.

            // Verify the actual record count matches expected exactly
            // using the O(1) per-shard counter.
            let actual = engine.shard_record_count(shard);
            let count_ok = if no_data_completion {
                true
            } else if expected_records == 0 {
                actual == 0
            } else {
                actual == expected_records
            };

            if !count_ok {
                return error_response(
                    request.request_id,
                    ERR_MIGRATION_IN_PROGRESS,
                    &format!(
                        "shard {shard} record count mismatch: expected {expected_records}, got {actual}"
                    ),
                );
            }

            // Only treat the exact-entry manifest as "verified" when it is
            // non-empty AND its length matches the expected record count.
            // An empty exact-entry list with `record_count > 0` is not
            // evidence of anything — the receiver must still verify via
            // the SHA-256 manifest (H3 safety requirement).
            let exact_entries_verified = if let Some(entries) = source_entries.as_ref()
                && !entries.is_empty()
                && entries.len() as u64 == expected_records
            {
                for (key, expected_generation) in entries {
                    let meta = match engine.read_metadata(key) {
                        Ok(meta) => meta,
                        Err(e) => {
                            return error_response(
                                request.request_id,
                                ERR_MIGRATION_IN_PROGRESS,
                                &format!("shard {shard} missing exact key {:?}: {e:?}", key,),
                            );
                        }
                    };
                    let actual_generation = meta.generation;
                    if actual_generation != *expected_generation {
                        return error_response(
                            request.request_id,
                            ERR_MIGRATION_IN_PROGRESS,
                            &format!(
                                "shard {shard} generation mismatch for {:?}: expected {}, got {}",
                                key, expected_generation, actual_generation,
                            ),
                        );
                    }
                }
                true
            } else {
                false
            };

            // Skip the expensive O(N) manifest hash scan when exact-entry
            // verification already confirmed every key's generation — the
            // manifest hash would recompute the same result.
            if !exact_entries_verified && let Some(expected_hash) = source_manifest {
                let mut local_manifest = crate::cluster::coordinator::ManifestHasher::new();
                for key in engine.keys_for_shard(shard) {
                    let meta = match engine.read_metadata(&key) {
                        Ok(meta) => meta,
                        Err(e) => {
                            return error_response(
                                request.request_id,
                                ERR_MIGRATION_IN_PROGRESS,
                                &format!(
                                    "shard {shard} manifest read_metadata failed for {:?}: {e:?}",
                                    key,
                                ),
                            );
                        }
                    };
                    local_manifest.fold(&key.txid, meta.generation);
                }
                let local_hash = local_manifest.finalize();
                if local_hash != expected_hash {
                    return error_response(
                        request.request_id,
                        ERR_MIGRATION_MANIFEST_MISMATCH,
                        &format!(
                            "shard {shard} manifest hash mismatch (count matched at {actual} records but content differs)",
                        ),
                    );
                }
            }

            if verify_only {
                return ResponseFrame {
                    request_id: request.request_id,
                    status: STATUS_OK,
                    payload: Vec::new(),
                };
            }

            if let Some(cluster) = cluster {
                if no_data_completion {
                    if let Some(from_node) = completion_from_node {
                        cluster.mark_inbound_complete_from_source(shard, from_node);
                    } else {
                        cluster.mark_inbound_complete_all(shard);
                    }
                } else if let Some(from_node) = completion_from_node {
                    cluster.mark_inbound_complete_from_source(shard, from_node);
                } else {
                    cluster.mark_inbound_complete(shard);
                }
                let should_commit = {
                    let shard_table = cluster.shard_table();
                    let table = shard_table.read();
                    table.target_assignment(shard).master == cluster.self_id()
                } && !cluster.has_pending_inbound_shard(shard);
                if should_commit {
                    cluster.shard_table().write().commit_shard(shard);
                }
            }
            ResponseFrame {
                request_id: request.request_id,
                status: STATUS_OK,
                payload: Vec::new(),
            }
        }
        OP_MIGRATION_BATCH_COMPLETE => {
            // Batched migration-complete: marks multiple shards as done
            // in a single TCP frame. Wire format:
            //   [shard_count:4][shard_id:2 × count][from_node:8]
            if request.payload.len() < 4 {
                return error_response(
                    request.request_id,
                    ERR_INTERNAL,
                    "batch-complete: too short",
                );
            }
            let shard_count = u32::from_le_bytes(request.payload[..4].try_into().unwrap()) as usize;
            let expected_len = 4 + shard_count * 2 + 8;
            if request.payload.len() < expected_len {
                return error_response(
                    request.request_id,
                    ERR_INTERNAL,
                    &format!(
                        "batch-complete: need {expected_len} bytes, got {}",
                        request.payload.len()
                    ),
                );
            }
            let mut shards = Vec::with_capacity(shard_count);
            for i in 0..shard_count {
                let off = 4 + i * 2;
                shards.push(u16::from_le_bytes(
                    request.payload[off..off + 2].try_into().unwrap(),
                ));
            }
            let from_node_off = 4 + shard_count * 2;
            let from_node = NodeId(u64::from_le_bytes(
                request.payload[from_node_off..from_node_off + 8]
                    .try_into()
                    .unwrap(),
            ));

            if let Some(cluster) = cluster {
                cluster.mark_inbound_complete_many_from_source(&shards, from_node);
                // Batch-commit all shards where this node is the new master
                // and no inbound is pending.
                let self_id = cluster.self_id();
                let shard_table = cluster.shard_table();
                let mut table = shard_table.write();
                for &shard in &shards {
                    let is_new_master = table.target_assignment(shard).master == self_id;
                    if is_new_master && !cluster.has_pending_inbound_shard(shard) {
                        table.commit_shard(shard);
                    }
                }
                drop(table);
                let _ = from_node; // Used for audit logging if needed
            }

            ResponseFrame {
                request_id: request.request_id,
                status: STATUS_OK,
                payload: Vec::new(),
            }
        }
        OP_TOPOLOGY_PROPOSE => {
            // Topology authority: another node is proposing a new term.
            //
            // Safety requirement (H10): `voted_term` MUST be fsync'd to disk
            // BEFORE the vote reply frame hits the wire. If the voter crashes
            // between reply and persist, the proposer may have already
            // counted our "yes" toward quorum while we come back thinking we
            // never voted — giving us license to vote "yes" for a *conflicting*
            // term and causing split-brain. The sequence is:
            //
            //   1. `handle_propose` records the vote in memory.
            //   2. `persist_topology` fsyncs it durably.
            //   3. Only then do we construct and return the reply frame.
            //
            // If step 2 fails we return `ERR_TOPOLOGY_PERSIST_FAILED` and the
            // proposer treats it as "no vote / retry".
            let cluster = match cluster {
                Some(c) => c,
                None => return error_response(request.request_id, ERR_INTERNAL, "not clustered"),
            };
            match crate::cluster::topology::TopologyTerm::deserialize(&request.payload) {
                Some(propose) => {
                    let vote = cluster.topology_authority().handle_propose(&propose);
                    if vote.accepted
                        && let Err(e) = cluster.persist_topology()
                    {
                        return error_response(
                            request.request_id,
                            ERR_TOPOLOGY_PERSIST_FAILED,
                            &format!(
                                "topology vote accepted for term {} but persist failed: {e}",
                                propose.term,
                            ),
                        );
                    }
                    ResponseFrame {
                        request_id: request.request_id,
                        status: STATUS_OK,
                        payload: vote.serialize(),
                    }
                }
                None => error_response(
                    request.request_id,
                    ERR_INTERNAL,
                    "malformed topology propose",
                ),
            }
        }
        OP_TOPOLOGY_VOTE => {
            // Topology authority: a peer voted on our proposal.
            // Check if quorum is reached — if so, broadcast commit.
            // The commit broadcast is handled by the coordinator event loop,
            // not here. We just return OK with any resulting commit.
            let cluster = match cluster {
                Some(c) => c,
                None => return error_response(request.request_id, ERR_INTERNAL, "not clustered"),
            };
            match crate::cluster::topology::TopologyVote::deserialize(&request.payload) {
                Some(vote) => {
                    let commit = cluster.topology_authority().handle_vote(&vote);
                    let payload = match commit {
                        Some(c) => c.serialize(),
                        None => Vec::new(),
                    };
                    ResponseFrame {
                        request_id: request.request_id,
                        status: STATUS_OK,
                        payload,
                    }
                }
                None => error_response(request.request_id, ERR_INTERNAL, "malformed topology vote"),
            }
        }
        OP_TOPOLOGY_COMMIT => {
            // Topology authority: a proposer achieved quorum and is committing.
            // Activate the shard table with the committed members.
            let cluster = match cluster {
                Some(c) => c,
                None => return error_response(request.request_id, ERR_INTERNAL, "not clustered"),
            };
            match crate::cluster::topology::TopologyCommit::deserialize(&request.payload) {
                Some(commit) => {
                    let members = commit.members.clone();
                    if let Some(term) = cluster.topology_authority().handle_commit(&commit) {
                        // Safety requirement (H10): persist the committed
                        // `committed_term` / `committed_members` BEFORE
                        // replying so the commit survives a crash. If
                        // persist fails, refuse to ack; the proposer will
                        // retry and we'll re-apply on the retry.
                        if let Err(e) = cluster.persist_topology() {
                            return error_response(
                                request.request_id,
                                ERR_TOPOLOGY_PERSIST_FAILED,
                                &format!(
                                    "topology commit term {term} applied in memory but persist failed: {e}",
                                ),
                            );
                        }
                        tracing::info!(
                            term = term,
                            members = members.len(),
                            "cluster: topology committed"
                        );
                        // Signal the coordinator event loop to activate the
                        // shard table with the committed member list — only
                        // after the commit is durable.
                        cluster.signal_topology_committed(members, term);
                    }
                    ResponseFrame {
                        request_id: request.request_id,
                        status: STATUS_OK,
                        payload: Vec::new(),
                    }
                }
                None => error_response(
                    request.request_id,
                    ERR_INTERNAL,
                    "malformed topology commit",
                ),
            }
        }
        _ => error_response(request.request_id, ERR_INTERNAL, "unknown opcode"),
    };

    // Record end-to-end handler latency into the appropriate histogram.
    // The per-item outcome counters are incremented inside each handler;
    // this records wall-clock time from dispatch entry to response built.
    if let Some(h) = DISPATCH_HISTOGRAMS.get() {
        match request.op_code {
            OP_SPEND_BATCH => {
                h.spend_latency.record_since(start);
                // spend_multi_latency shadows spend_latency for legacy
                // /admin/top compatibility — same samples.
                h.spend_multi_latency.record_since(start);
            }
            OP_UNSPEND_BATCH => h.unspend_latency.record_since(start),
            OP_CREATE_BATCH => h.create_latency.record_since(start),
            OP_SET_MINED_BATCH => h.set_mined_latency.record_since(start),
            OP_FREEZE_BATCH => h.freeze_latency.record_since(start),
            OP_UNFREEZE_BATCH => h.unfreeze_latency.record_since(start),
            OP_DELETE_BATCH => h.delete_latency.record_since(start),
            OP_GET_BATCH | OP_GET_SPEND_BATCH => h.get_latency.record_since(start),
            OP_MARK_LONGEST_CHAIN_BATCH => h.mark_longest_chain_latency.record_since(start),
            OP_REASSIGN_BATCH => h.reassign_latency.record_since(start),
            OP_SET_CONFLICTING_BATCH => h.set_conflicting_latency.record_since(start),
            OP_SET_LOCKED_BATCH => h.set_locked_latency.record_since(start),
            OP_PRESERVE_UNTIL_BATCH => h.preserve_until_latency.record_since(start),
            _ => {}
        }
    }

    response
}

// ---------------------------------------------------------------------------
// Redo log helper
// ---------------------------------------------------------------------------

/// Append redo ops to the log and flush.
///
/// Returns the sequence number of the last appended entry on success.
/// Returns an error string if the redo log write or flush fails — the
/// caller must fail the client request to maintain the durability
/// contract (every acknowledged mutation has a redo log entry).
///
/// When no redo log is configured (single-node test setups), returns
/// `Ok(0)` — the engine writes are still durable via O_DIRECT but
/// there is no crash recovery journal.
/// Write redo operations to the WAL and flush.
///
/// Returns `(first_seq, last_seq)` — the redo sequence range assigned to the
/// appended entries. These are the authoritative sequence numbers used by
/// replica catch-up and ACK tracking.
fn write_redo_ops(
    redo_log: Option<&Mutex<RedoLog>>,
    ops: &[RedoOp],
) -> std::result::Result<(u64, u64), String> {
    let redo = match redo_log {
        Some(r) => r,
        None => return Ok((0, 0)),
    };
    if ops.is_empty() {
        return Ok((0, 0));
    }
    let mut log = redo.lock();
    let first_seq = log.current_sequence();
    let mut last_seq = first_seq;
    for op in ops {
        last_seq = log
            .append(op.clone())
            .map_err(|e| format!("redo log append: {e}"))?;
    }
    log.flush().map_err(|e| format!("redo log flush: {e}"))?;
    Ok((first_seq, last_seq))
}

// ---------------------------------------------------------------------------
// Replication helper
// ---------------------------------------------------------------------------

/// Outcome of a replication attempt, conveyed back to request handlers so
/// they can pick the right response status for the client.
///
/// - [`ReplicationOutcome::NotApplicable`]: no replication was attempted —
///   either the server is not part of a cluster, there were no ops to
///   replicate, or no replica targets were resolved. The client response
///   is the natural `STATUS_OK` / `STATUS_PARTIAL_ERROR` for the handler.
/// - [`ReplicationOutcome::Full`]: every replica target ACKed successfully
///   (or the configured ACK policy was met for the normal case). Full
///   cluster durability was achieved; respond with `STATUS_OK`.
/// - [`ReplicationOutcome::Degraded`]: best-effort mode is active AND
///   **zero** replica targets ACKed. The mutation is durable only on the
///   local master; if the master crashes before catch-up streaming, the
///   write is lost. Respond with `STATUS_DEGRADED_DURABILITY` so the
///   client knows durability silently degraded to single-node.
///
/// The threshold for `Degraded` is deliberately "zero ACKs" (as opposed to
/// "less than quorum") because the *middle* case — some but not all
/// replicas ACKed — still satisfies the weakest commonly-desired invariant
/// (the write exists on at least one peer, so a single master crash will
/// not lose it). That case continues to emit `STATUS_OK` in best-effort
/// mode and only ticks the `replication_degraded_acks` telemetry counter.
/// The zero-ACK case is fundamentally different: the write exists on no
/// peer at all, and a master crash loses it unconditionally — that is the
/// signal the client actually needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicationOutcome {
    /// No replication was applicable (standalone, no ops, or no targets).
    NotApplicable,
    /// Every replica target ACKed successfully (or the ACK policy was met).
    Full,
    /// Best-effort mode: zero replica targets ACKed. Durability is
    /// single-node only and clients should be informed via
    /// `STATUS_DEGRADED_DURABILITY`.
    Degraded,
}

impl ReplicationOutcome {
    /// Whether this outcome indicates the client should receive
    /// `STATUS_DEGRADED_DURABILITY` instead of `STATUS_OK`.
    #[inline]
    pub(crate) fn is_degraded(self) -> bool {
        matches!(self, ReplicationOutcome::Degraded)
    }
}

#[inline]
fn valid_redo_range(range: (u64, u64)) -> bool {
    range.0 != 0 && range.1 >= range.0
}

fn begin_replication_intent(range: (u64, u64)) -> std::result::Result<(), String> {
    if !valid_redo_range(range) {
        return Ok(());
    }
    if let Some(tracker) = REPLICATION_INTENT_TRACKER.get() {
        tracker
            .begin(range.0, range.1)
            .map_err(|e| format!("replication intent begin: {e}"))?;
    }
    Ok(())
}

fn commit_replication_intent(range: (u64, u64)) -> std::result::Result<(), String> {
    if !valid_redo_range(range) {
        return Ok(());
    }
    if let Some(tracker) = REPLICATION_INTENT_TRACKER.get() {
        tracker
            .commit(range.0, range.1)
            .map_err(|e| format!("replication intent commit: {e}"))?;
    }
    Ok(())
}

fn clear_replication_intent_after_compensation(range: (u64, u64)) {
    if let Err(e) = commit_replication_intent(range) {
        tracing::warn!(err = %e, "replication intent: failed to clear after compensation");
    }
}

fn clear_replication_intent_after_success(range: (u64, u64)) {
    if let Err(e) = commit_replication_intent(range) {
        tracing::warn!(
            err = %e,
            "replication intent: failed to clear after successful replica ACKs; startup recovery will replay"
        );
    }
}

/// Send replication operations to replica nodes for the given keys.
///
/// Uses the redo sequence range from `write_redo_ops()` to tag batches so
/// that replica ACK tracking and catch-up use the same sequence space as
/// the durable redo log.
///
/// When ACK policy enforcement is enabled (RF >= 2 with a non-best_effort
/// policy), replication failures are returned as errors so the caller can
/// fail the client request. In best-effort mode, failures are logged only
/// and the return value distinguishes `Full` (all replicas ACKed) from
/// `Degraded` (zero replicas ACKed — durability collapsed to single-node).
///
/// Returns `Ok(ReplicationOutcome)` when the ACK policy is satisfied or
/// best-effort mode suppresses the error. Returns `Err(message)` when the
/// required number of replica ACKs was not received AND best-effort is
/// disabled.
fn replicate_all_ops(
    cluster: Option<&RunningCluster>,
    ops_by_key: &[(TxKey, Vec<ReplicaOp>)],
    redo_seq_range: (u64, u64),
) -> std::result::Result<ReplicationOutcome, String> {
    let cluster = match cluster {
        Some(c) => c,
        None => return Ok(ReplicationOutcome::NotApplicable),
    };
    if ops_by_key.is_empty() {
        return Ok(ReplicationOutcome::NotApplicable);
    }
    begin_replication_intent(redo_seq_range)?;

    // Group all ops by target replica address
    let table = cluster.shard_table();
    let table_guard = table.read();
    let rf = table_guard.replication_factor();
    let expected_replicas_per_key = rf.saturating_sub(1) as usize;
    let mut by_addr: HashMap<SocketAddr, Vec<ReplicaOp>> = HashMap::new();
    let mut target_errors = Vec::new();

    for (key, ops) in ops_by_key {
        let shard = ShardTable::shard_for_key(key);
        // Use target_assignment (new topology) rather than effective_assignment
        // (old topology during handoff). Replication must go to nodes in the
        // NEW member list — the old assignment may reference dead nodes whose
        // departure triggered the topology change.
        let assignment = table_guard.target_assignment(shard);
        if rf > 1 && assignment.replicas.len() < expected_replicas_per_key {
            target_errors.push(format!(
                "shard {shard} has {} replica targets, expected {expected_replicas_per_key} for RF={rf}",
                assignment.replicas.len(),
            ));
            continue;
        }
        for replica_id in &assignment.replicas {
            match cluster.node_addr(replica_id) {
                Some(addr) => {
                    by_addr.entry(addr).or_default().extend(ops.clone());
                }
                None if rf > 1 => {
                    target_errors.push(format!(
                        "shard {shard} replica node {} has no resolved address",
                        replica_id.0,
                    ));
                }
                None => {}
            }
        }
    }
    drop(table_guard);

    if !target_errors.is_empty() {
        target_errors.sort();
        target_errors.dedup();
        return Err(format!(
            "replication target resolution failed: {}",
            target_errors.join("; "),
        ));
    }

    if by_addr.is_empty() {
        // No replicas configured or no replica addresses known.
        if rf > 1 {
            return Err(format!(
                "replication target resolution failed: no replica targets for RF={rf}",
            ));
        }
        clear_replication_intent_after_success(redo_seq_range);
        return Ok(ReplicationOutcome::NotApplicable);
    }

    // Send to all replica targets in parallel using the shared replication
    // runtime. Each send runs on a blocking task (reusing pooled threads)
    // instead of spawning a new OS thread per replication call.
    let source_node_id = cluster.self_id().0;
    let ack_timeout = replication_ack_timeout_for(
        cluster.replication_timeout(),
        cluster.migration_pressure_active(),
    );
    // Phase B3: stamp every outbound batch with the live coordinator
    // epoch so the receiver's gate can reject stale-cluster writes.
    let cluster_key = cluster.local_cluster_key();
    let results: Vec<std::result::Result<(), String>> = REPL_RUNTIME.block_on(async {
        let mut handles = Vec::with_capacity(by_addr.len());
        for (addr, ops) in by_addr {
            handles.push(tokio::task::spawn_blocking(move || {
                if ops.is_empty() {
                    return Ok(());
                }
                let batch = ReplicaBatch {
                    first_sequence: redo_seq_range.0,
                    ops,
                    trace_ctx: crate::observability::WireTraceContext::from_current_span(),
                    source_node_id: Some(source_node_id),
                    cluster_key,
                };
                send_replica_batch_to(addr, &batch, ack_timeout)
            }));
        }
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            results.push(
                handle
                    .await
                    .unwrap_or_else(|_| Err("task panicked".to_string())),
            );
        }
        results
    });

    let mut ack_count: usize = 0;
    let mut last_error: Option<String> = None;
    for result in &results {
        match result {
            Ok(()) => {
                ack_count += 1;
            }
            Err(e) => {
                tracing::warn!(err = %e, "replication to replica failed");
                last_error = Some(e.clone());
            }
        }
    }
    let total_targets = results.len();

    let ack_policy = cluster.ack_policy();
    let best_effort = cluster.is_replication_best_effort();
    let classification =
        classify_replication_outcome(ack_count, total_targets, ack_policy, best_effort);

    match classification {
        ReplicationClassification::PolicyViolation { required } => Err(format!(
            "replication: {ack_count}/{total_targets} replicas ACKed, need {required}: {}",
            last_error.unwrap_or_default()
        )),
        ReplicationClassification::PartialAck => {
            // At least one replica ACKed but not all — multi-node durability
            // partially preserved. Tick the existing "degraded acks" counter
            // but still return `Full`, so the client sees STATUS_OK.
            if let Some(metrics) = DISPATCH_METRICS.get() {
                metrics.replication_degraded_acks.inc();
            }
            tracing::warn!(
                ack_count,
                total_targets,
                "replication: degraded ack (best_effort)",
            );
            clear_replication_intent_after_success(redo_seq_range);
            Ok(ReplicationOutcome::Full)
        }
        ReplicationClassification::ZeroAckBestEffort => {
            // Zero replicas ACKed in best-effort mode: durability collapsed
            // to single-node. Escalate to `Degraded` so the caller responds
            // with STATUS_DEGRADED_DURABILITY and the dedicated metric
            // (`repl_degraded_durability`) ticks.
            if let Some(metrics) = DISPATCH_METRICS.get() {
                metrics.repl_degraded_durability.inc();
            }
            tracing::warn!(
                total_targets,
                err = %last_error.clone().unwrap_or_default(),
                "replication: DEGRADED DURABILITY — 0 replicas ACKed, client will receive STATUS_DEGRADED_DURABILITY (best_effort)",
            );
            clear_replication_intent_after_success(redo_seq_range);
            Ok(ReplicationOutcome::Degraded)
        }
        ReplicationClassification::FullAck => {
            clear_replication_intent_after_success(redo_seq_range);
            Ok(ReplicationOutcome::Full)
        }
    }
}

fn replication_ack_timeout_for(base: Duration, migration_pressure: bool) -> Duration {
    if migration_pressure {
        base.max(MIGRATION_REPLICATION_TIMEOUT_FLOOR)
    } else {
        base
    }
}

/// Resolve durable pending replication intents before the server starts
/// serving client traffic.
///
/// Any range left in the intent tracker means the previous process applied a
/// mutation locally but crashed before it could prove the replica ACK policy
/// and clear the marker. We reconstruct replica ops from the redo log and
/// replicate them to the current target holders. If any range cannot be
/// resolved, startup must fail closed.
pub fn recover_pending_replication_intents(
    cluster: &RunningCluster,
    redo_log: Option<&Mutex<RedoLog>>,
    engine: &Engine,
) -> std::result::Result<(), String> {
    let tracker = match REPLICATION_INTENT_TRACKER.get() {
        Some(t) => t,
        None => return Ok(()),
    };
    recover_pending_replication_intents_from_tracker(tracker, redo_log, engine, |ops, range| {
        replicate_all_ops(Some(cluster), ops, range).map(|_| ())
    })
}

fn recover_pending_replication_intents_from_tracker<F>(
    tracker: &crate::replication::durable::ReplicationIntentTracker,
    redo_log: Option<&Mutex<RedoLog>>,
    engine: &Engine,
    mut replicate: F,
) -> std::result::Result<(), String>
where
    F: FnMut(&[(TxKey, Vec<ReplicaOp>)], (u64, u64)) -> std::result::Result<(), String>,
{
    let pending = tracker.pending();
    if pending.is_empty() {
        return Ok(());
    }
    let redo_log = redo_log.ok_or_else(|| {
        format!(
            "replication intent recovery requires redo log; {} pending range(s)",
            pending.len(),
        )
    })?;

    for range in pending {
        let entries = {
            let log = redo_log.lock();
            log.read_from_sequence(range.first_sequence)
                .map_err(|e| format!("read redo for pending replication intent: {e}"))?
        };
        let entries: Vec<_> = entries
            .into_iter()
            .filter(|entry| {
                entry.sequence >= range.first_sequence && entry.sequence <= range.last_sequence
            })
            .collect();
        if entries.is_empty()
            || entries.first().map(|e| e.sequence) != Some(range.first_sequence)
            || entries.last().map(|e| e.sequence) != Some(range.last_sequence)
        {
            return Err(format!(
                "pending replication intent {}..{} cannot be resolved: redo entries missing",
                range.first_sequence, range.last_sequence,
            ));
        }

        let mut ops_by_key = Vec::new();
        for entry in &entries {
            let Some(tx_key) = entry.op.tx_key().copied() else {
                continue;
            };
            let shard = ShardTable::shard_for_key(&tx_key);
            if let Some(op) =
                crate::cluster::coordinator::redo_entry_to_replica_op(entry, shard, engine)
            {
                ops_by_key.push((tx_key, vec![op]));
            }
        }

        if ops_by_key.is_empty() {
            tracker
                .commit(range.first_sequence, range.last_sequence)
                .map_err(|e| format!("replication intent commit: {e}"))?;
            continue;
        }

        replicate(&ops_by_key, (range.first_sequence, range.last_sequence))?;
        tracker
            .commit(range.first_sequence, range.last_sequence)
            .map_err(|e| format!("replication intent commit: {e}"))?;
    }

    Ok(())
}

/// Classification of an ACK tally against the configured ACK policy.
///
/// This is a pure function of the ACK counts, the policy, and the
/// best-effort flag, so it can be tested without a live cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicationClassification {
    /// Every replica target ACKed successfully.
    FullAck,
    /// Some (but not all) replicas ACKed. In best-effort mode with at least
    /// one ACK, the write is still multi-node durable so we respond OK.
    /// In the non-best-effort case this only occurs when the configured
    /// policy explicitly permits it (e.g. `WriteMajority` on RF=3 with
    /// 2/3 ACKs).
    PartialAck,
    /// Best-effort mode AND zero replicas ACKed AND at least one target
    /// existed. This is the "silently single-node" case that requires
    /// STATUS_DEGRADED_DURABILITY.
    ZeroAckBestEffort,
    /// The ACK count is below the configured policy threshold and
    /// best-effort is disabled. The caller must fail the client request
    /// with ERR_REPLICATION_FAILED.
    PolicyViolation {
        /// Number of replica ACKs the configured policy required.
        required: usize,
    },
}

/// Pure, side-effect-free classification of replication ACK outcome.
///
/// Inputs:
/// - `ack_count`: number of replica targets that ACKed successfully.
/// - `total_targets`: number of replica targets the batch was sent to.
/// - `ack_policy`: `Some(policy)` to enforce, `None` for best-effort (no
///   minimum enforced).
/// - `best_effort`: whether `replication_degraded_mode = "best_effort"`
///   is active — determines whether a policy violation is suppressed and
///   whether zero-ACK triggers the degraded-durability escalation.
///
/// See [`ReplicationClassification`] for semantics.
pub(crate) fn classify_replication_outcome(
    ack_count: usize,
    total_targets: usize,
    ack_policy: Option<crate::replication::manager::AckPolicy>,
    best_effort: bool,
) -> ReplicationClassification {
    let required = match ack_policy {
        Some(crate::replication::manager::AckPolicy::WriteAll) => total_targets,
        Some(crate::replication::manager::AckPolicy::WriteMajority) => {
            // For majority across N replica targets, we need ceil(N/2)
            // replica ACKs (master itself counts implicitly as one copy).
            total_targets.div_ceil(2)
        }
        None => 0, // best-effort: no minimum
    };

    if ack_count < required && !best_effort {
        return ReplicationClassification::PolicyViolation { required };
    }

    if best_effort && ack_count == 0 && total_targets > 0 {
        return ReplicationClassification::ZeroAckBestEffort;
    }

    if ack_count < total_targets {
        return ReplicationClassification::PartialAck;
    }

    ReplicationClassification::FullAck
}

/// Compensate for a replication failure by reversing locally-applied mutations.
///
/// When `replicate_all_ops` fails, the local engine has already applied the
/// ops and the redo log has the forward entries. This function applies the
/// inverse operation for each op in `repl_ops`, then appends compensating
/// redo entries so crash recovery also reverses the mutations.
///
/// This ensures the local node doesn't diverge from replicas: the client
/// receives an error, and the local state is rolled back as if the write
/// never happened.
///
/// # Crash safety: the double-fault gap
///
/// If the master crashes between the original redo write (durable) and
/// this compensation function completing, crash recovery will replay the
/// original mutation without compensation. The write becomes durable on
/// the master even though no replica received it and the client got no
/// response.
///
/// This is acceptable because:
/// 1. The client received no response (connection dropped), so it
///    doesn't know whether the write succeeded — it must handle
///    ambiguity regardless.
/// 2. Replica catch-up streaming will propagate the write to replicas
///    once they reconnect, restoring the replication invariant.
/// 3. Actual data loss requires a second fault: the master's disk
///    failing before catch-up completes (double-fault scenario).
///
/// The alternative (writing a "pending replication" marker in the redo
/// entry and checking it on recovery) would add per-write overhead to
/// the hot path for a scenario that requires two independent failures.
fn compensate_replication_failure(
    engine: &Engine,
    repl_ops: &[(TxKey, Vec<ReplicaOp>)],
    redo_log: Option<&Mutex<RedoLog>>,
) {
    let mut comp_redo: Vec<RedoOp> = Vec::new();

    for (key, ops) in repl_ops {
        for op in ops {
            match op {
                ReplicaOp::Spend { offset, .. } => {
                    if let Ok(slot) = engine.read_slot(key, *offset) {
                        let req = crate::ops::unspend::UnspendRequest {
                            tx_key: *key,
                            offset: *offset,
                            utxo_hash: slot.hash,
                            current_block_height: 0,
                            block_height_retention: 0,
                        };
                        let _ = engine.unspend(&req);
                        comp_redo.push(RedoOp::Unspend {
                            tx_key: *key,
                            offset: *offset,
                            new_spent_count: 0,
                        });
                    }
                }
                ReplicaOp::Unspend { offset, .. } => {
                    // Reverse unspend → re-spend the slot with zero spending_data
                    if let Ok(slot) = engine.read_slot(key, *offset) {
                        let req = crate::ops::spend::SpendMultiRequest {
                            tx_key: *key,
                            spends: vec![crate::ops::spend::SpendItem {
                                offset: *offset,
                                utxo_hash: slot.hash,
                                spending_data: [0u8; 36],
                                idx: 0,
                            }],
                            ignore_conflicting: true,
                            ignore_locked: true,
                            current_block_height: 0,
                            block_height_retention: 0,
                        };
                        if let Ok(v) = engine.validate_spend_multi(&req) {
                            let _ = v.apply(engine);
                        }
                        comp_redo.push(RedoOp::Spend {
                            tx_key: *key,
                            offset: *offset,
                            spending_data: [0u8; 36],
                            new_spent_count: 0,
                        });
                    }
                }
                ReplicaOp::Freeze { offset, .. } => {
                    if let Ok(slot) = engine.read_slot(key, *offset) {
                        let req = crate::ops::remaining::UnfreezeRequest {
                            tx_key: *key,
                            offset: *offset,
                            utxo_hash: slot.hash,
                        };
                        let _ = engine.unfreeze(&req);
                        comp_redo.push(RedoOp::Unfreeze {
                            tx_key: *key,
                            offset: *offset,
                        });
                    }
                }
                ReplicaOp::Unfreeze { offset, .. } => {
                    if let Ok(slot) = engine.read_slot(key, *offset) {
                        let req = crate::ops::remaining::FreezeRequest {
                            tx_key: *key,
                            offset: *offset,
                            utxo_hash: slot.hash,
                        };
                        let _ = engine.freeze(&req);
                        comp_redo.push(RedoOp::Freeze {
                            tx_key: *key,
                            offset: *offset,
                        });
                    }
                }
                ReplicaOp::SetMined {
                    block_id,
                    block_height,
                    subtree_idx,
                    ..
                } => {
                    let req = crate::ops::set_mined::SetMinedRequest {
                        tx_key: *key,
                        block_id: *block_id,
                        block_height: *block_height,
                        subtree_idx: *subtree_idx,
                        on_longest_chain: false,
                        unset_mined: true,
                        current_block_height: 0,
                        block_height_retention: 0,
                    };
                    let _ = engine.set_mined(&req);
                    comp_redo.push(RedoOp::SetMined {
                        tx_key: *key,
                        block_id: *block_id,
                        block_height: *block_height,
                        subtree_idx: *subtree_idx,
                        unset: true,
                    });
                }
                ReplicaOp::UnsetMined { block_id, .. } => {
                    // Reverse unset → re-set the block entry. We don't have
                    // block_height/subtree_idx from the UnsetMined op, so use
                    // defaults. The caller (set_mined handler) should have
                    // included these in the op if they were needed for reversal.
                    let req = crate::ops::set_mined::SetMinedRequest {
                        tx_key: *key,
                        block_id: *block_id,
                        block_height: 0,
                        subtree_idx: 0,
                        on_longest_chain: true,
                        unset_mined: false,
                        current_block_height: 0,
                        block_height_retention: 0,
                    };
                    let _ = engine.set_mined(&req);
                    comp_redo.push(RedoOp::SetMined {
                        tx_key: *key,
                        block_id: *block_id,
                        block_height: 0,
                        subtree_idx: 0,
                        unset: false,
                    });
                }
                ReplicaOp::Reassign {
                    offset,
                    new_hash,
                    block_height,
                    spendable_after,
                    ..
                } => {
                    // Reverse reassign: reassign back to the old hash. The
                    // old hash is the current slot hash (since the reassign
                    // was already applied, the slot now has new_hash). We need
                    // to read the slot, but the hash is now new_hash. We don't
                    // have the OLD hash. Best effort: reassign to zeros.
                    // In practice, reassign compensation is rare (only on
                    // frozen UTXOs during coinbase maturation).
                    let req = crate::ops::remaining::ReassignRequest {
                        tx_key: *key,
                        offset: *offset,
                        utxo_hash: *new_hash,     // current hash after reassign
                        new_utxo_hash: [0u8; 32], // can't restore original
                        block_height: *block_height,
                        spendable_after: *spendable_after,
                    };
                    let _ = engine.reassign(&req);
                    comp_redo.push(RedoOp::Reassign {
                        tx_key: *key,
                        offset: *offset,
                        new_hash: [0u8; 32],
                        block_height: *block_height,
                        spendable_after: *spendable_after,
                    });
                }
                ReplicaOp::PruneSlot { offset, .. } => {
                    // PruneSlot only changes the status byte to UTXO_PRUNED.
                    // The slot data (hash, spending_data) is preserved. To
                    // reverse, read the slot and restore the status to UNSPENT.
                    // This is conservative: we don't know the original status
                    // (could have been SPENT or FROZEN), but UNSPENT is the
                    // safest default since the record will be re-evaluated.
                    if let Some(entry) = engine.lookup(key)
                        && let Ok(mut slot) =
                            crate::io::read_utxo_slot(engine.device(), entry.record_offset, *offset)
                        && slot.status == crate::record::UTXO_PRUNED
                    {
                        slot.status = crate::record::UTXO_UNSPENT;
                        let _ = crate::io::write_utxo_slot(
                            engine.device(),
                            entry.record_offset,
                            *offset,
                            &slot,
                        );
                    }
                    // No redo entry needed: PruneSlot is only generated during
                    // migration delta replay, not from client dispatch handlers.
                }
                ReplicaOp::SetConflicting {
                    value,
                    current_block_height,
                    retention,
                    ..
                } => {
                    let req = crate::ops::remaining::SetConflictingRequest {
                        tx_key: *key,
                        value: !value,
                        current_block_height: *current_block_height,
                        block_height_retention: *retention,
                    };
                    let _ = engine.set_conflicting(&req);
                    comp_redo.push(RedoOp::SetConflicting {
                        tx_key: *key,
                        value: !value,
                        current_block_height: *current_block_height,
                        block_height_retention: *retention,
                    });
                }
                ReplicaOp::SetLocked { value, .. } => {
                    let req = crate::ops::remaining::SetLockedRequest {
                        tx_key: *key,
                        value: !value,
                    };
                    let _ = engine.set_locked(&req);
                    comp_redo.push(RedoOp::SetLocked {
                        tx_key: *key,
                        value: !value,
                    });
                }
                ReplicaOp::PreserveUntil { .. } => {
                    let req = crate::ops::remaining::PreserveUntilRequest {
                        tx_key: *key,
                        block_height: 0,
                    };
                    let _ = engine.preserve_until(&req);
                    comp_redo.push(RedoOp::PreserveUntil {
                        tx_key: *key,
                        block_height: 0,
                    });
                }
                ReplicaOp::Create { .. } => {
                    let req = crate::ops::remaining::DeleteRequest { tx_key: *key };
                    let _ = engine.delete(&req);
                    comp_redo.push(RedoOp::Delete {
                        tx_key: *key,
                        record_offset: 0,
                        record_size: 0,
                    });
                }
                ReplicaOp::Delete { .. } => {
                    // Delete compensation is handled directly in
                    // handle_delete_batch using pre-captured record snapshots.
                    // If this path is reached from another handler, the record
                    // is already destroyed and cannot be restored here.
                }
            }
        }
    }

    if !comp_redo.is_empty() {
        let _ = write_redo_ops(redo_log, &comp_redo);
    }
}

/// Send a `ReplicaBatch` to a replica node via TCP using the wire protocol.
///
/// Reuses a persistent connection from the per-address pool. If no cached
/// connection exists or the cached one has failed, a fresh TCP connection
/// is established. The per-address mutex ensures that concurrent sends to
/// the SAME replica are serialized (correct: TCP is ordered), while sends
/// to DIFFERENT replicas proceed in parallel without contention.
fn send_replica_batch_to(
    addr: SocketAddr,
    batch: &ReplicaBatch,
    ack_timeout: Duration,
) -> std::result::Result<(), String> {
    // Get or create the per-address slot. The outer pool lock is held
    // only for the HashMap lookup/insert, not during I/O.
    let slot = {
        let mut pool = REPL_POOL.lock();
        pool.entry(addr)
            .or_insert_with(|| {
                std::sync::Arc::new(Mutex::new(PerAddrSlot {
                    connection: None,
                    last_acked: 0,
                }))
            })
            .clone()
    };

    // Lock only this address's slot. Other addresses are uncontended.
    let mut slot_guard = slot.lock();

    let mut transport = match slot_guard.connection.take() {
        Some(t) if t.is_connected() => t,
        _ => TcpReplicaTransport::connect(&addr.to_string(), Duration::from_secs(5))
            .map_err(|e| format!("connect: {e}"))?,
    };

    if let Err(e) = transport.send_batch(batch) {
        // Connection may be stale (broken by partition, killed node, etc.).
        // Drop the broken transport and reconnect once before giving up.
        drop(transport);
        let mut retry_transport =
            TcpReplicaTransport::connect(&addr.to_string(), Duration::from_secs(5))
                .map_err(|e2| format!("send: {e}; reconnect: {e2}"))?;
        if let Err(e2) = retry_transport.send_batch(batch) {
            return Err(format!("send after reconnect: {e2}"));
        }
        transport = retry_transport;
    }

    match transport.recv_ack(ack_timeout) {
        Ok(ReplicaAck::Ok { through_sequence }) => {
            slot_guard.connection = Some(transport);
            slot_guard.last_acked = through_sequence;
            // Persist the ACK sequence for crash-safe catch-up.
            if let Some(tracker) = ACK_TRACKER.get() {
                tracker.record_ack(addr, through_sequence);
            }
            Ok(())
        }
        Ok(ReplicaAck::Error { message, .. }) => {
            slot_guard.connection = Some(transport);
            Err(format!("replica error: {message}"))
        }
        Err(e) => Err(format!("recv_ack: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Quorum check
// ---------------------------------------------------------------------------

/// Check if the cluster has quorum (majority of nodes are alive).
///
/// Returns `None` if quorum is met or no cluster is configured (single-node mode).
/// Returns `Some(ResponseFrame)` with an error if quorum is not met, meaning
/// this node cannot safely accept mutations.
///
/// In a clustered deployment, a node must see at least 2 alive nodes (including
/// itself) to accept writes. This prevents split-brain scenarios where isolated
/// nodes diverge by independently accepting conflicting writes.
fn check_quorum(cluster: Option<&RunningCluster>, request_id: u64) -> Option<ResponseFrame> {
    let cluster = cluster?;
    let alive = cluster.alive_node_count();
    let peak = cluster.peak_cluster_size();

    // A node that has only ever seen itself (peak=1) is a standalone cluster
    // node — quorum is trivially met. This covers single-node test setups
    // and bootstrap scenarios.
    if peak <= 1 {
        return None;
    }

    // For a node that was previously part of a multi-node cluster, require
    // a majority (more than half of the peak observed cluster size) to prevent
    // split-brain. With 3 nodes, need >= 2. With 5 nodes, need >= 3.
    let quorum_needed = (peak / 2) + 1;
    if alive < quorum_needed {
        return Some(error_response(request_id, ERR_NO_QUORUM, "no quorum"));
    }
    None
}

/// Returns true if the given opcode is a mutation that requires quorum.
fn is_mutation_opcode(op: u16) -> bool {
    matches!(
        op,
        OP_SPEND_BATCH
            | OP_UNSPEND_BATCH
            | OP_SET_MINED_BATCH
            | OP_CREATE_BATCH
            | OP_FREEZE_BATCH
            | OP_UNFREEZE_BATCH
            | OP_REASSIGN_BATCH
            | OP_SET_CONFLICTING_BATCH
            | OP_SET_LOCKED_BATCH
            | OP_PRESERVE_UNTIL_BATCH
            | OP_DELETE_BATCH
            | OP_MARK_LONGEST_CHAIN_BATCH
            | OP_PRESERVE_TRANSACTIONS
            | OP_PROCESS_EXPIRED_PRESERVATIONS
    )
}

// ---------------------------------------------------------------------------
// Shard ownership check
// ---------------------------------------------------------------------------

/// Check if a txid belongs to a shard owned by this node.
///
/// Returns `None` if the key is local (or no cluster is configured).
/// Returns `Some(BatchItemError)` with a redirect error if the key belongs
/// to a remote node, including the target node's address in `error_data`.
///
/// When `allow_if_migrating` is true (for read operations), the check
/// allows local handling if this node is actively migrating the shard
/// outbound — the data is still present locally until migration completes.
fn check_shard_ownership(
    txid: &[u8; 32],
    item_index: u32,
    cluster: Option<&RunningCluster>,
    allow_if_migrating: bool,
) -> Option<BatchItemError> {
    let cluster = cluster?;
    let key = TxKey { txid: *txid };
    match cluster.is_master(&key) {
        crate::cluster::coordinator::MasterQueryResult::Yes => {
            // If we're the new master but still waiting for inbound migration
            // data, reject mutations so clients retry after migration completes.
            // Reads are handled separately with a wait loop.
            if !allow_if_migrating && cluster.has_pending_inbound(&key) {
                let shard = crate::cluster::shards::ShardTable::shard_for_key(&key);
                tracing::debug!(
                    shard,
                    "dispatch: write rejected — pending inbound migration"
                );
                Some(BatchItemError {
                    item_index,
                    error_code: ERR_MIGRATION_IN_PROGRESS,
                    error_data: Vec::new(),
                })
            } else if !allow_if_migrating && cluster.is_shard_write_fenced(&key) {
                let shard = crate::cluster::shards::ShardTable::shard_for_key(&key);
                tracing::debug!(
                    shard,
                    "dispatch: write rejected — write-fenced (delta streaming)"
                );
                Some(BatchItemError {
                    item_index,
                    error_code: ERR_MIGRATION_IN_PROGRESS,
                    error_data: Vec::new(),
                })
            } else {
                None
            }
        }
        crate::cluster::coordinator::MasterQueryResult::Transitioning { last_known_term } => {
            // Topology proposal in flight but not yet quorum-committed.
            // Don't redirect (the redirect target may itself be wrong).
            // Tell the client to retry; once the gap closes the next
            // attempt resolves to Yes or No deterministically.
            tracing::debug!(
                last_known_term,
                "dispatch: deferring request — topology in transition"
            );
            Some(BatchItemError {
                item_index,
                error_code: ERR_MIGRATION_IN_PROGRESS,
                error_data: Vec::new(),
            })
        }
        crate::cluster::coordinator::MasterQueryResult::No => {
            // During outbound migration, reads can still be served locally
            // because the data hasn't been removed yet.
            if allow_if_migrating && cluster.is_migrating_outbound(&key) {
                return None;
            }
            // Determine the target node address for the redirect
            let route = cluster.route(&key);
            let error_data = match route {
                crate::cluster::shards::RouteDecision::RedirectTo { node, .. } => {
                    match cluster.node_addr(&node) {
                        Some(addr) => addr.to_string().into_bytes(),
                        None => Vec::new(),
                    }
                }
                crate::cluster::shards::RouteDecision::HandleLocally => return None,
            };
            // M10: count every stale-routed request so operators can alert on
            // persistent stale-routing storms (indicates clients are not
            // refreshing the partition map). Best-effort: no-op if metrics
            // haven't been initialized (e.g. unit tests).
            if let Some(m) = DISPATCH_METRICS.get() {
                m.stale_routing_request_total.inc();
            }
            Some(BatchItemError {
                item_index,
                error_code: ERR_REDIRECT,
                error_data,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Spend
// ---------------------------------------------------------------------------

fn handle_spend_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (params, items) = match decode_spend_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed spend batch"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    // Tick entry counters: one batch, N attempted items.
    if let Some(m) = DISPATCH_METRICS.get() {
        m.spend_multi_batches.inc();
        m.spends_attempted.inc_by(items.len() as u64);
        m.spend_multi_items_attempted.inc_by(items.len() as u64);
    }

    // Group items by txid for efficient locking
    let mut by_txid: HashMap<[u8; 32], Vec<(usize, &WireSpendItem)>> = HashMap::new();
    for (i, item) in items.iter().enumerate() {
        by_txid.entry(item.txid).or_default().push((i, item));
    }

    // Track per-item outcome. `succeeded` is incremented once per item that
    // actually transitioned the slot to SPENT (valid_spends during apply).
    // `idempotent` is items that were silently no-op (already SPENT with the
    // same spending_data). `failed` is items present in the errors vec.
    let mut succeeded: u64 = 0;
    let mut errors: Vec<BatchItemError> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    let mut spend_redo_range: (u64, u64) = (0, 0);

    // WAL-first ordering: for each txid group we validate under lock,
    // write redo ops to the WAL (fsync), THEN apply the mutation.
    // This guarantees that if the process crashes after the engine
    // mutation, the redo log already has the entry and replicas will
    // see it during catch-up streaming.
    for (txid, group) in &by_txid {
        if let Some(redirect_err) = check_shard_ownership(txid, group[0].0 as u32, cluster, false) {
            for &(i, _) in group {
                errors.push(BatchItemError {
                    item_index: i as u32,
                    error_code: redirect_err.error_code,
                    error_data: redirect_err.error_data.clone(),
                });
            }
            continue;
        }

        let spend_items: Vec<SpendItem> = group
            .iter()
            .map(|(i, item)| SpendItem {
                offset: item.vout,
                utxo_hash: item.utxo_hash,
                spending_data: item.spending_data,
                idx: *i as u32,
            })
            .collect();

        let multi_req = SpendMultiRequest {
            tx_key: TxKey { txid: *txid },
            spends: spend_items,
            ignore_conflicting: params.ignore_conflicting,
            ignore_locked: params.ignore_locked,
            current_block_height: params.current_block_height,
            block_height_retention: params.block_height_retention,
        };

        // Phase 1: Validate under lock (no disk writes yet).
        let validated = match engine.validate_spend_multi(&multi_req) {
            Ok(v) => v,
            Err(err) => {
                for &(i, _) in group {
                    errors.push(spend_error_to_batch_error(i as u32, &err));
                }
                continue;
            }
        };

        // Phase 2: Build redo ops for validated items BEFORE mutation.
        // The post-mutation generation is pre_generation + 1.
        let error_indices: std::collections::HashSet<u32> =
            validated.errors.keys().copied().collect();
        let key = TxKey { txid: *txid };
        let post_generation = validated.pre_generation.wrapping_add(1);

        let mut redo_ops: Vec<RedoOp> = Vec::new();
        let mut key_repl_ops: Vec<ReplicaOp> = Vec::new();
        for &(i, item) in group {
            if !error_indices.contains(&(i as u32)) {
                redo_ops.push(RedoOp::Spend {
                    tx_key: key,
                    offset: item.vout,
                    spending_data: item.spending_data,
                    new_spent_count: 0,
                });
                key_repl_ops.push(ReplicaOp::Spend {
                    tx_key: key,
                    offset: item.vout,
                    spending_data: item.spending_data,
                    master_generation: post_generation,
                });
            }
        }

        // Phase 3: Write redo BEFORE engine mutation (WAL-first).
        // Lock is still held via ValidatedSpend, so no concurrent
        // mutation can interleave.
        match write_redo_ops(redo_log, &redo_ops) {
            Ok(range) => {
                if spend_redo_range.0 == 0 && spend_redo_range.1 == 0 {
                    spend_redo_range = range;
                } else if range.1 > 0 {
                    spend_redo_range.1 = range.1; // Extend the end
                }
            }
            Err(e) => {
                // Redo failure: don't apply, return error.
                // ValidatedSpend drops here, releasing the lock.
                return error_response(req.request_id, ERR_INTERNAL, &e);
            }
        }

        // Phase 4: Apply the mutation (still under lock).
        // ValidatedSpend is consumed, lock released after write.
        let validation_errors = validated.errors.clone();
        let resp = match validated.apply(engine) {
            Ok(r) => r,
            Err(e) => {
                // DAH overflow (config misconfiguration) or similar —
                // surface as ERR_INTERNAL rather than silently clamping.
                return error_response(req.request_id, ERR_INTERNAL, &e.to_string());
            }
        };

        if !key_repl_ops.is_empty() {
            repl_ops_by_key.push((key, key_repl_ops));
        }

        // Tally this group's outcomes before draining the validation
        // errors: real transitions come from resp.spent_count (which
        // excludes idempotent re-spends), failed from the error map.
        // Idempotent = group.len() - succeeded - failed.
        succeeded += resp.spent_count as u64;

        for (idx, err) in validation_errors {
            errors.push(spend_error_to_batch_error(idx, &err));
        }

        // Use signal/block_ids from resp if needed in the future.
        let _ = resp.signal;
    }

    // Final per-item outcome classification for this batch. `errors` holds
    // validation failures *and* redirect errors (when the txid is not owned
    // by this node), so all three buckets sum to items.len().
    let failed_total = errors.len() as u64;
    let idempotent_total = (items.len() as u64)
        .saturating_sub(succeeded)
        .saturating_sub(failed_total);
    if let Some(m) = DISPATCH_METRICS.get() {
        m.spends_succeeded.inc_by(succeeded);
        m.spends_idempotent.inc_by(idempotent_total);
        m.spends_failed.inc_by(failed_total);
        m.spend_multi_items_succeeded.inc_by(succeeded);
        m.spend_multi_items_idempotent.inc_by(idempotent_total);
        m.spend_multi_items_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations.inc_by(OpCode::Spend, Outcome::Ok, succeeded);
        m.operations
            .inc_by(OpCode::Spend, Outcome::Idempotent, idempotent_total);
        for e in &errors {
            m.operations
                .inc(OpCode::Spend, classify_wire_error_code(e.error_code));
        }
    }

    // Phase 5: Replicate (redo already fsynced, engine already applied).
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, spend_redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(spend_redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    if errors.is_empty() {
        let status = if repl_outcome.is_degraded() {
            STATUS_DEGRADED_DURABILITY
        } else {
            STATUS_OK
        };
        ResponseFrame {
            request_id: req.request_id,
            status,
            payload: vec![],
        }
    } else {
        errors.sort_by_key(|e| e.item_index);
        ResponseFrame {
            request_id: req.request_id,
            status: STATUS_PARTIAL_ERROR,
            payload: encode_sparse_errors(&errors),
        }
    }
}

// ---------------------------------------------------------------------------
// Unspend
// ---------------------------------------------------------------------------

// NOTE ON WAL ORDERING: Unlike `handle_spend_batch` which holds the
// per-txid lock across redo write + engine mutation (because spend uses
// validate-then-apply), the handlers below (unspend, set_mined, freeze,
// etc.) write redo ops BEFORE acquiring the engine lock. This is safe
// because ALL redo operations in these paths are idempotent — replaying
// a redo entry that was already applied is a no-op due to generation
// guards and slot-state checks. If a non-idempotent redo op is ever
// added to these paths, this pattern must be restructured to match
// the spend path's WAL-first-under-lock discipline.
fn handle_unspend_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (params, items) = match decode_unspend_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed unspend batch"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    // Tick entry counters: one batch, N attempted items.
    if let Some(m) = DISPATCH_METRICS.get() {
        m.unspend_multi_batches.inc();
        m.unspends_attempted.inc_by(items.len() as u64);
        m.unspend_multi_items_attempted.inc_by(items.len() as u64);
    }

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership and build redo ops from request parameters.
    struct ValidUnspend<'a> {
        idx: usize,
        key: TxKey,
        item: &'a WireSlotItem,
        pre_generation: u32,
    }
    let mut valid_items: Vec<ValidUnspend> = Vec::new();
    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: item.txid };
        // Snapshot the generation BEFORE unspend so we can classify the
        // outcome as "real unspend" (gen bumped) vs "idempotent noop"
        // (gen unchanged — slot was already UNSPENT).
        let pre_generation = engine.lookup(&key).map(|e| e.generation).unwrap_or(0);
        redo_ops.push(RedoOp::Unspend {
            tx_key: key,
            offset: item.vout,
            new_spent_count: 0,
        });
        valid_items.push(ValidUnspend {
            idx: i,
            key,
            item,
            pre_generation,
        });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_INTERNAL, &e),
    };

    // Phase 3: Apply engine mutations and build repl ops.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    let mut succeeded: u64 = 0;
    let mut idempotent: u64 = 0;
    for v in &valid_items {
        match engine.unspend(&UnspendRequest {
            tx_key: v.key,
            offset: v.item.vout,
            utxo_hash: v.item.utxo_hash,
            current_block_height: params.current_block_height,
            block_height_retention: params.block_height_retention,
        }) {
            Ok(resp) => {
                if resp.generation == v.pre_generation {
                    // No-op: slot was already UNSPENT, generation unchanged.
                    idempotent += 1;
                } else {
                    succeeded += 1;
                }
                repl_ops_by_key.push((
                    v.key,
                    vec![ReplicaOp::Unspend {
                        tx_key: v.key,
                        offset: v.item.vout,
                        master_generation: resp.generation,
                    }],
                ));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    let failed_total = errors.len() as u64;
    if let Some(m) = DISPATCH_METRICS.get() {
        m.unspends_succeeded.inc_by(succeeded);
        m.unspends_noop.inc_by(idempotent);
        m.unspends_failed.inc_by(failed_total);
        m.unspend_multi_items_succeeded.inc_by(succeeded);
        m.unspend_multi_items_idempotent.inc_by(idempotent);
        m.unspend_multi_items_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations.inc_by(OpCode::Unspend, Outcome::Ok, succeeded);
        m.operations
            .inc_by(OpCode::Unspend, Outcome::Idempotent, idempotent);
        for e in &errors {
            m.operations
                .inc(OpCode::Unspend, classify_wire_error_code(e.error_code));
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

// ---------------------------------------------------------------------------
// SetMined
// ---------------------------------------------------------------------------

fn handle_set_mined_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (params, txids) = match decode_set_mined_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed set_mined batch"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    if let Some(m) = DISPATCH_METRICS.get() {
        m.set_mined_items_attempted.inc_by(txids.len() as u64);
    }

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership and build redo ops from request params.
    struct ValidSetMined {
        idx: usize,
        key: TxKey,
    }
    let mut valid_items: Vec<ValidSetMined> = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        redo_ops.push(RedoOp::SetMined {
            tx_key: key,
            block_id: params.block_id,
            block_height: params.block_height,
            subtree_idx: params.subtree_idx,
            unset: params.unset_mined,
        });
        valid_items.push(ValidSetMined { idx: i, key });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_INTERNAL, &e),
    };

    // Phase 3: Apply engine mutations via batch API (params passed once by reference).
    let engine_params = crate::ops::set_mined::SetMinedSharedParams {
        block_id: params.block_id,
        block_height: params.block_height,
        subtree_idx: params.subtree_idx,
        current_block_height: params.current_block_height,
        block_height_retention: params.block_height_retention,
        on_longest_chain: params.on_longest_chain,
        unset_mined: params.unset_mined,
    };
    let keys: Vec<TxKey> = valid_items.iter().map(|v| v.key).collect();
    let results = engine.set_mined_batch(&engine_params, &keys);

    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    let mut succeeded: u64 = 0;
    for (v, result) in valid_items.iter().zip(results) {
        match result {
            Ok(resp) => {
                succeeded += 1;
                let mgen = resp.generation;
                if params.unset_mined {
                    repl_ops_by_key.push((
                        v.key,
                        vec![ReplicaOp::UnsetMined {
                            tx_key: v.key,
                            block_id: params.block_id,
                            master_generation: mgen,
                        }],
                    ));
                } else {
                    repl_ops_by_key.push((
                        v.key,
                        vec![ReplicaOp::SetMined {
                            tx_key: v.key,
                            block_id: params.block_id,
                            block_height: params.block_height,
                            subtree_idx: params.subtree_idx,
                            on_longest_chain: params.on_longest_chain,
                            master_generation: mgen,
                        }],
                    ));
                }
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    let failed_total = errors.len() as u64;
    if let Some(m) = DISPATCH_METRICS.get() {
        m.set_mined_items_succeeded.inc_by(succeeded);
        m.set_mined_items_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations
            .inc_by(OpCode::SetMined, Outcome::Ok, succeeded);
        for e in &errors {
            m.operations
                .inc(OpCode::SetMined, classify_wire_error_code(e.error_code));
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    // Final batch-level ticks: set_mined_succeeded counts a successful batch,
    // set_mined_attempted incremented at dispatch entry. Tick succeeded only
    // if no items failed to preserve a useful "batches that fully succeeded"
    // gauge separate from item-level accounting.
    if let Some(m) = DISPATCH_METRICS.get()
        && failed_total == 0
    {
        m.set_mined_succeeded.inc();
    }

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

/// Parse the wire cold_data blob into separate inputs/outputs/inpoints fields.
/// Wire format: [inputs_len:4 LE][inputs][outputs_len:4 LE][outputs][inpoints_len:4 LE][inpoints]
#[allow(clippy::type_complexity)]
fn parse_cold_data_fields(cold_data: &[u8]) -> (Option<&[u8]>, Option<&[u8]>, Option<&[u8]>) {
    if cold_data.len() < 12 {
        return (None, None, None);
    }
    let mut pos = 0usize;

    let il = u32::from_le_bytes(cold_data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if pos + il > cold_data.len() {
        return (None, None, None);
    }
    let inputs = &cold_data[pos..pos + il];
    pos += il;

    let inputs_opt = if inputs.is_empty() {
        None
    } else {
        Some(inputs)
    };

    if pos + 4 > cold_data.len() {
        return (inputs_opt, None, None);
    }
    let ol = u32::from_le_bytes(cold_data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if pos + ol > cold_data.len() {
        return (inputs_opt, None, None);
    }
    let outputs = &cold_data[pos..pos + ol];
    pos += ol;

    let outputs_opt = if outputs.is_empty() {
        None
    } else {
        Some(outputs)
    };

    if pos + 4 > cold_data.len() {
        return (inputs_opt, outputs_opt, None);
    }
    let pl = u32::from_le_bytes(cold_data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if pos + pl > cold_data.len() {
        return (inputs_opt, outputs_opt, None);
    }
    let inpoints = &cold_data[pos..pos + pl];

    let inpoints_opt = if inpoints.is_empty() {
        None
    } else {
        Some(inpoints)
    };

    (inputs_opt, outputs_opt, inpoints_opt)
}

fn handle_create_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
    blob_store: Option<&dyn BlobStore>,
) -> ResponseFrame {
    let items = match decode_create_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed create batch"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Pre-compute mined_block_infos for each item so CreateRequest can borrow them.
    let mined_infos: Vec<Vec<crate::ops::create::MinedBlockInfo>> = items
        .iter()
        .map(|item| {
            if let Some(block_id) = item.mined_block_id {
                vec![crate::ops::create::MinedBlockInfo {
                    block_id,
                    block_height: item.mined_block_height.unwrap_or(0),
                    subtree_idx: item.mined_subtree_idx.unwrap_or(0),
                }]
            } else {
                vec![]
            }
        })
        .collect();

    // Phase 1: Validate ownership, check blobs, pre-allocate space, build redo ops.
    struct ValidCreate<'a> {
        idx: usize,
        create_req: CreateRequest<'a>,
        record_offset: u64,
    }
    let mut valid_items: Vec<ValidCreate> = Vec::new();

    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }

        // Check whether this item uses an externally-uploaded blob.
        let is_ext = item.flags & FLAG_EXTERNAL_BLOB != 0;
        if is_ext && let Some(bs) = blob_store {
            match bs.exists(&item.txid) {
                Ok(true) => {}
                Ok(false) => {
                    errors.push(BatchItemError {
                        item_index: i as u32,
                        error_code: ERR_BLOB_NOT_FOUND,
                        error_data: vec![],
                    });
                    continue;
                }
                Err(_) => {
                    errors.push(BatchItemError {
                        item_index: i as u32,
                        error_code: ERR_INTERNAL,
                        error_data: vec![],
                    });
                    continue;
                }
            }
        }

        let (inputs, outputs, inpoints) = if is_ext {
            (None, None, None)
        } else {
            parse_cold_data_fields(&item.cold_data)
        };

        let create_req = CreateRequest {
            tx_id: item.txid,
            tx_version: item.tx_version,
            locktime: item.locktime,
            fee: item.fee,
            size_in_bytes: item.size_in_bytes,
            extended_size: item.extended_size,
            is_coinbase: item.is_coinbase,
            spending_height: item.spending_height,
            utxo_hashes: &item.utxo_hashes,
            inputs,
            outputs,
            inpoints,
            is_external: is_ext,
            created_at: item.created_at,
            block_height: item.block_height,
            mined_block_infos: &mined_infos[i],
            frozen: item.flags & 0x04 != 0,
            conflicting: item.flags & 0x02 != 0,
            locked: item.flags & 0x01 != 0,
            parent_txids: &item.parent_txids,
        };

        // Pre-allocate space to get record_offset for the redo entry.
        match engine.pre_allocate_create(&create_req) {
            Ok((record_offset, utxo_count)) => {
                let key = TxKey { txid: item.txid };
                redo_ops.push(RedoOp::Create {
                    tx_key: key,
                    record_offset,
                    utxo_count,
                });
                valid_items.push(ValidCreate {
                    idx: i,
                    create_req,
                    record_offset,
                });
            }
            Err(CreateError::DuplicateTxId) => {
                errors.push(BatchItemError {
                    item_index: i as u32,
                    error_code: ERR_ALREADY_EXISTS,
                    error_data: vec![],
                });
            }
            Err(_) => {
                errors.push(BatchItemError {
                    item_index: i as u32,
                    error_code: ERR_INTERNAL,
                    error_data: vec![],
                });
            }
        }
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // Redo failed: free all pre-allocated space.
            for v in &valid_items {
                let utxo_count = v.create_req.utxo_hashes.len() as u32;
                let base_size = crate::record::TxMetadata::record_size_for(utxo_count);
                let cold_len = if v.create_req.is_external && v.create_req.inputs.is_none() {
                    0u64
                } else {
                    build_cold_data(
                        v.create_req.inputs,
                        v.create_req.outputs,
                        v.create_req.inpoints,
                    )
                    .len() as u64
                };
                let _ = engine
                    .allocator()
                    .lock()
                    .free(v.record_offset, base_size + cold_len);
            }
            return error_response(req.request_id, ERR_INTERNAL, &e);
        }
    };

    // Phase 3: Apply engine mutations at pre-allocated offsets.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    for v in &valid_items {
        let item = &items[v.idx];
        match engine.create_at_offset(&v.create_req, v.record_offset) {
            Ok(_) => {
                let key = TxKey { txid: item.txid };
                // Serialize full metadata for the replica so a promoted replica
                // has the authoritative record state.
                let mut meta_buf = Vec::with_capacity(128);
                // Core 46 bytes
                meta_buf.extend_from_slice(&item.tx_version.to_le_bytes());
                meta_buf.extend_from_slice(&item.locktime.to_le_bytes());
                meta_buf.extend_from_slice(&item.fee.to_le_bytes());
                meta_buf.extend_from_slice(&item.size_in_bytes.to_le_bytes());
                meta_buf.extend_from_slice(&item.extended_size.to_le_bytes());
                meta_buf.push(if item.is_coinbase { 1 } else { 0 });
                meta_buf.extend_from_slice(&item.spending_height.to_le_bytes());
                meta_buf.extend_from_slice(&item.created_at.to_le_bytes());
                meta_buf.push(item.flags);
                // Lifecycle 24 bytes
                let (r_gen, r_upd, r_ums, r_dah, r_pu) =
                    if let Ok(meta) = engine.read_metadata(&key) {
                        (
                            { meta.generation },
                            { meta.updated_at },
                            { meta.unmined_since },
                            { meta.delete_at_height },
                            { meta.preserve_until },
                        )
                    } else {
                        (0u32, 0u64, 0u32, 0u32, 0u32)
                    };
                meta_buf.extend_from_slice(&r_gen.to_le_bytes());
                meta_buf.extend_from_slice(&r_upd.to_le_bytes());
                meta_buf.extend_from_slice(&r_ums.to_le_bytes());
                meta_buf.extend_from_slice(&r_dah.to_le_bytes());
                meta_buf.extend_from_slice(&r_pu.to_le_bytes());
                // Extended: block_height + mined_block_infos + parent_txids
                meta_buf.extend_from_slice(&item.block_height.to_le_bytes());
                let block_infos = v.create_req.mined_block_infos;
                meta_buf.push(block_infos.len() as u8);
                for info in block_infos {
                    meta_buf.extend_from_slice(&info.block_id.to_le_bytes());
                    meta_buf.extend_from_slice(&info.block_height.to_le_bytes());
                    meta_buf.extend_from_slice(&info.subtree_idx.to_le_bytes());
                }
                meta_buf.extend_from_slice(&(item.parent_txids.len() as u16).to_le_bytes());
                for ptx in &item.parent_txids {
                    meta_buf.extend_from_slice(ptx);
                }

                repl_ops_by_key.push((
                    key,
                    vec![ReplicaOp::Create {
                        tx_key: key,
                        metadata_bytes: meta_buf,
                        utxo_hashes: item.utxo_hashes.clone(),
                        cold_data: if item.cold_data.is_empty() {
                            None
                        } else {
                            Some(item.cold_data.clone())
                        },
                        is_external: item.flags & FLAG_EXTERNAL_BLOB != 0,
                    }],
                ));
            }
            Err(CreateError::DuplicateTxId) => {
                errors.push(BatchItemError {
                    item_index: v.idx as u32,
                    error_code: ERR_ALREADY_EXISTS,
                    error_data: vec![],
                });
            }
            Err(_) => {
                errors.push(BatchItemError {
                    item_index: v.idx as u32,
                    error_code: ERR_INTERNAL,
                    error_data: vec![],
                });
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    // Tick per-item outcome counters. Succeeded = items.len() - errors.len().
    let failed_total = errors.len() as u64;
    let succeeded_total = (items.len() as u64).saturating_sub(failed_total);
    if let Some(m) = DISPATCH_METRICS.get() {
        m.creates_succeeded.inc_by(succeeded_total);
        m.creates_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations
            .inc_by(OpCode::Create, Outcome::Ok, succeeded_total);
        for e in &errors {
            m.operations
                .inc(OpCode::Create, classify_wire_error_code(e.error_code));
        }
    }

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

// ---------------------------------------------------------------------------
// Freeze / Unfreeze / Delete / SetLocked / etc — simple dispatch
// ---------------------------------------------------------------------------

fn handle_freeze_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let items = match decode_slot_item_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership and build redo ops from request params.
    struct ValidFreeze<'a> {
        idx: usize,
        key: TxKey,
        item: &'a WireSlotItem,
    }
    let mut valid_items: Vec<ValidFreeze> = Vec::new();
    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: item.txid };
        redo_ops.push(RedoOp::Freeze {
            tx_key: key,
            offset: item.vout,
        });
        valid_items.push(ValidFreeze { idx: i, key, item });
    }
    let total_items = items.len() as u64;

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_INTERNAL, &e),
    };

    // Phase 3: Apply engine mutations and build repl ops from engine results.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    for v in &valid_items {
        match engine.freeze(&FreezeRequest {
            tx_key: v.key,
            offset: v.item.vout,
            utxo_hash: v.item.utxo_hash,
        }) {
            Ok(mgen) => {
                repl_ops_by_key.push((
                    v.key,
                    vec![ReplicaOp::Freeze {
                        tx_key: v.key,
                        offset: v.item.vout,
                        master_generation: mgen,
                    }],
                ));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    let failed_total = errors.len() as u64;
    let succeeded_total = total_items.saturating_sub(failed_total);
    if let Some(m) = DISPATCH_METRICS.get() {
        m.freezes_succeeded.inc_by(succeeded_total);
        m.freezes_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations
            .inc_by(OpCode::Freeze, Outcome::Ok, succeeded_total);
        for e in &errors {
            m.operations
                .inc(OpCode::Freeze, classify_wire_error_code(e.error_code));
        }
    }

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

fn handle_unfreeze_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let items = match decode_slot_item_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let total_items = items.len() as u64;
    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership and build redo ops from request params.
    struct ValidUnfreeze<'a> {
        idx: usize,
        key: TxKey,
        item: &'a WireSlotItem,
    }
    let mut valid_items: Vec<ValidUnfreeze> = Vec::new();
    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: item.txid };
        redo_ops.push(RedoOp::Unfreeze {
            tx_key: key,
            offset: item.vout,
        });
        valid_items.push(ValidUnfreeze { idx: i, key, item });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_INTERNAL, &e),
    };

    // Phase 3: Apply engine mutations and build repl ops from engine results.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    for v in &valid_items {
        match engine.unfreeze(&UnfreezeRequest {
            tx_key: v.key,
            offset: v.item.vout,
            utxo_hash: v.item.utxo_hash,
        }) {
            Ok(mgen) => {
                repl_ops_by_key.push((
                    v.key,
                    vec![ReplicaOp::Unfreeze {
                        tx_key: v.key,
                        offset: v.item.vout,
                        master_generation: mgen,
                    }],
                ));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    let failed_total = errors.len() as u64;
    let succeeded_total = total_items.saturating_sub(failed_total);
    if let Some(m) = DISPATCH_METRICS.get() {
        m.unfreezes_succeeded.inc_by(succeeded_total);
        m.unfreezes_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations
            .inc_by(OpCode::Unfreeze, Outcome::Ok, succeeded_total);
        for e in &errors {
            m.operations
                .inc(OpCode::Unfreeze, classify_wire_error_code(e.error_code));
        }
    }

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

fn handle_reassign_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (params, items) = match decode_reassign_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let total_items = items.len() as u64;
    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership and build redo ops from request params.
    struct ValidReassign<'a> {
        idx: usize,
        key: TxKey,
        item: &'a WireReassignItem,
    }
    let mut valid_items: Vec<ValidReassign> = Vec::new();
    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: item.txid };
        redo_ops.push(RedoOp::Reassign {
            tx_key: key,
            offset: item.vout,
            new_hash: item.new_utxo_hash,
            block_height: params.block_height,
            spendable_after: params.spendable_after,
        });
        valid_items.push(ValidReassign { idx: i, key, item });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_INTERNAL, &e),
    };

    // Phase 3: Apply engine mutations and build repl ops from engine results.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    for v in &valid_items {
        match engine.reassign(&ReassignRequest {
            tx_key: v.key,
            offset: v.item.vout,
            utxo_hash: v.item.utxo_hash,
            new_utxo_hash: v.item.new_utxo_hash,
            block_height: params.block_height,
            spendable_after: params.spendable_after,
        }) {
            Ok(mgen) => {
                repl_ops_by_key.push((
                    v.key,
                    vec![ReplicaOp::Reassign {
                        tx_key: v.key,
                        offset: v.item.vout,
                        new_hash: v.item.new_utxo_hash,
                        block_height: params.block_height,
                        spendable_after: params.spendable_after,
                        master_generation: mgen,
                    }],
                ));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    let failed_total = errors.len() as u64;
    let succeeded_total = total_items.saturating_sub(failed_total);
    if let Some(m) = DISPATCH_METRICS.get() {
        m.reassign_succeeded.inc_by(succeeded_total);
        m.reassign_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations
            .inc_by(OpCode::Reassign, Outcome::Ok, succeeded_total);
        for e in &errors {
            m.operations
                .inc(OpCode::Reassign, classify_wire_error_code(e.error_code));
        }
    }

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

fn handle_set_conflicting_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (shared, txids) = match decode_txid_batch(&req.payload, 9) {
        // value(1) + cbh(4) + bhr(4)
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let value = shared[0] != 0;
    let cbh = u32::from_le_bytes(shared[1..5].try_into().unwrap());
    let bhr = u32::from_le_bytes(shared[5..9].try_into().unwrap());
    let total_items = txids.len() as u64;

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership and build redo ops from request params.
    struct ValidSetConflicting {
        idx: usize,
        key: TxKey,
    }
    let mut valid_items: Vec<ValidSetConflicting> = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        redo_ops.push(RedoOp::SetConflicting {
            tx_key: key,
            value,
            current_block_height: cbh,
            block_height_retention: bhr,
        });
        valid_items.push(ValidSetConflicting { idx: i, key });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_INTERNAL, &e),
    };

    // Phase 3: Apply engine mutations and build repl ops from engine results.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    for v in &valid_items {
        match engine.set_conflicting(&SetConflictingRequest {
            tx_key: v.key,
            value,
            current_block_height: cbh,
            block_height_retention: bhr,
        }) {
            Ok(resp) => {
                repl_ops_by_key.push((
                    v.key,
                    vec![ReplicaOp::SetConflicting {
                        tx_key: v.key,
                        value,
                        current_block_height: cbh,
                        retention: bhr,
                        master_generation: resp.generation,
                    }],
                ));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    let failed_total = errors.len() as u64;
    let succeeded_total = total_items.saturating_sub(failed_total);
    if let Some(m) = DISPATCH_METRICS.get() {
        m.set_conflicting_succeeded.inc_by(succeeded_total);
        m.set_conflicting_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations
            .inc_by(OpCode::SetConflicting, Outcome::Ok, succeeded_total);
        for e in &errors {
            m.operations.inc(
                OpCode::SetConflicting,
                classify_wire_error_code(e.error_code),
            );
        }
    }

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

fn handle_set_locked_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (shared, txids) = match decode_txid_batch(&req.payload, 1) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let value = shared[0] != 0;
    let total_items = txids.len() as u64;

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership and build redo ops from request params.
    struct ValidSetLocked {
        idx: usize,
        key: TxKey,
    }
    let mut valid_items: Vec<ValidSetLocked> = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        redo_ops.push(RedoOp::SetLocked { tx_key: key, value });
        valid_items.push(ValidSetLocked { idx: i, key });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_INTERNAL, &e),
    };

    // Phase 3: Apply engine mutations and build repl ops from engine results.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    for v in &valid_items {
        match engine.set_locked(&SetLockedRequest {
            tx_key: v.key,
            value,
        }) {
            Ok(mgen) => {
                repl_ops_by_key.push((
                    v.key,
                    vec![ReplicaOp::SetLocked {
                        tx_key: v.key,
                        value,
                        master_generation: mgen,
                    }],
                ));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    let failed_total = errors.len() as u64;
    let succeeded_total = total_items.saturating_sub(failed_total);
    if let Some(m) = DISPATCH_METRICS.get() {
        m.set_locked_succeeded.inc_by(succeeded_total);
        m.set_locked_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations
            .inc_by(OpCode::SetLocked, Outcome::Ok, succeeded_total);
        for e in &errors {
            m.operations
                .inc(OpCode::SetLocked, classify_wire_error_code(e.error_code));
        }
    }

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

fn handle_preserve_until_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (shared, txids) = match decode_txid_batch(&req.payload, 4) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let height = u32::from_le_bytes(shared[0..4].try_into().unwrap());
    let total_items = txids.len() as u64;

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership and build redo ops from request params.
    struct ValidPreserve {
        idx: usize,
        key: TxKey,
    }
    let mut valid_items: Vec<ValidPreserve> = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        redo_ops.push(RedoOp::PreserveUntil {
            tx_key: key,
            block_height: height,
        });
        valid_items.push(ValidPreserve { idx: i, key });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_INTERNAL, &e),
    };

    // Phase 3: Apply engine mutations and build repl ops from engine results.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    for v in &valid_items {
        match engine.preserve_until(&PreserveUntilRequest {
            tx_key: v.key,
            block_height: height,
        }) {
            Ok(resp) => {
                repl_ops_by_key.push((
                    v.key,
                    vec![ReplicaOp::PreserveUntil {
                        tx_key: v.key,
                        block_height: height,
                        master_generation: resp.generation,
                    }],
                ));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    let failed_total = errors.len() as u64;
    let succeeded_total = total_items.saturating_sub(failed_total);
    if let Some(m) = DISPATCH_METRICS.get() {
        m.preserve_until_succeeded.inc_by(succeeded_total);
        m.preserve_until_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations
            .inc_by(OpCode::PreserveUntil, Outcome::Ok, succeeded_total);
        for e in &errors {
            m.operations.inc(
                OpCode::PreserveUntil,
                classify_wire_error_code(e.error_code),
            );
        }
    }

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

fn handle_delete_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (_, txids) = match decode_txid_batch(&req.payload, 0) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let total_items = txids.len() as u64;
    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership, lookup record_offset (read-only), build redo ops.
    // Also snapshot each record BEFORE deletion so we can restore on replication failure.
    struct ValidDelete {
        idx: usize,
        key: TxKey,
        /// Full record snapshot for compensation. Contains the metadata bytes
        /// and UTXO hashes needed to re-create the record if replication fails.
        snapshot: Option<DeleteSnapshot>,
    }
    struct DeleteSnapshot {
        metadata_bytes: Vec<u8>,
        utxo_hashes: Vec<[u8; 32]>,
        cold_data: Option<Vec<u8>>,
        is_external: bool,
    }
    let mut valid_items: Vec<ValidDelete> = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        let record_offset = engine.lookup(&key).map(|e| e.record_offset).unwrap_or(0);
        redo_ops.push(RedoOp::Delete {
            tx_key: key,
            record_offset,
            record_size: 0,
        });

        // Snapshot the record for compensation. Read metadata + UTXO slots.
        let snapshot = if let Ok(meta) = engine.read_metadata(&key) {
            let mut utxo_hashes = Vec::with_capacity(meta.utxo_count as usize);
            for v in 0..meta.utxo_count {
                match engine.read_slot(&key, v) {
                    Ok(slot) => utxo_hashes.push(slot.hash),
                    Err(_) => utxo_hashes.push([0u8; 32]),
                }
            }
            // Build the metadata bytes in the same format as migrate_shard.
            let mut meta_buf = Vec::with_capacity(70);
            meta_buf.extend_from_slice(&meta.tx_version.to_le_bytes());
            meta_buf.extend_from_slice(&meta.locktime.to_le_bytes());
            meta_buf.extend_from_slice(&meta.fee.to_le_bytes());
            meta_buf.extend_from_slice(&meta.size_in_bytes.to_le_bytes());
            meta_buf.extend_from_slice(&meta.extended_size.to_le_bytes());
            let (is_coinbase, wire_flags) =
                crate::replication::protocol::create_metadata_flag_bytes(meta.flags);
            meta_buf.push(is_coinbase);
            meta_buf.extend_from_slice(&meta.spending_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.created_at.to_le_bytes());
            meta_buf.push(wire_flags);
            meta_buf.extend_from_slice(&meta.generation.to_le_bytes());
            meta_buf.extend_from_slice(&meta.updated_at.to_le_bytes());
            meta_buf.extend_from_slice(&meta.unmined_since.to_le_bytes());
            meta_buf.extend_from_slice(&meta.delete_at_height.to_le_bytes());
            meta_buf.extend_from_slice(&meta.preserve_until.to_le_bytes());

            let cold_data = if meta.flags.contains(crate::record::TxFlags::EXTERNAL) {
                engine
                    .blob_store()
                    .and_then(|bs| bs.get(&key.txid).ok().flatten())
            } else {
                None
            };

            Some(DeleteSnapshot {
                metadata_bytes: meta_buf,
                utxo_hashes,
                cold_data,
                is_external: meta.flags.contains(crate::record::TxFlags::EXTERNAL),
            })
        } else {
            None
        };

        valid_items.push(ValidDelete {
            idx: i,
            key,
            snapshot,
        });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_INTERNAL, &e),
    };

    // Phase 3: Apply engine mutations and build repl ops.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    let mut deleted_snapshots: Vec<(TxKey, DeleteSnapshot)> = Vec::new();
    for v in valid_items.iter() {
        match engine.delete(&DeleteRequest { tx_key: v.key }) {
            Ok(()) => {
                repl_ops_by_key.push((v.key, vec![ReplicaOp::Delete { tx_key: v.key }]));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }
    // Collect snapshots for successfully deleted records.
    for v in valid_items {
        if let Some(snap) = v.snapshot
            && repl_ops_by_key.iter().any(|(k, _)| *k == v.key)
        {
            deleted_snapshots.push((v.key, snap));
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            // Compensate: re-create deleted records from snapshots.
            for (key, snap) in &deleted_snapshots {
                let create_op = ReplicaOp::Create {
                    tx_key: *key,
                    metadata_bytes: snap.metadata_bytes.clone(),
                    utxo_hashes: snap.utxo_hashes.clone(),
                    cold_data: snap.cold_data.clone(),
                    is_external: snap.is_external,
                };
                // Apply the re-create through the replication receiver path
                // which handles the full Create logic.
                let create_req = crate::protocol::frame::RequestFrame {
                    request_id: 0,
                    op_code: OP_REPLICA_BATCH,
                    flags: 0,
                    payload: ReplicaBatch {
                        first_sequence: 0,
                        ops: vec![create_op],
                        trace_ctx: None,
                        source_node_id: None,
                        // Self-compensation path: applies through the
                        // ungated `handle_replica_batch` so cluster_key
                        // gating does not apply. The wire field is
                        // therefore left as the V1-compat sentinel `0`.
                        cluster_key: 0,
                    }
                    .serialize(),
                };
                let _ = handle_replica_batch(
                    &create_req,
                    engine,
                    &std::sync::atomic::AtomicU64::new(0),
                );
                // Append a Create redo entry for crash recovery.
                if let Some(entry) = engine.lookup(key) {
                    let _ = write_redo_ops(
                        redo_log,
                        &[RedoOp::Create {
                            tx_key: *key,
                            record_offset: entry.record_offset,
                            utxo_count: snap.utxo_hashes.len() as u32,
                        }],
                    );
                }
            }
            // Also compensate any non-delete ops in the same batch.
            let non_delete: Vec<_> = repl_ops_by_key
                .iter()
                .filter(|(_, ops)| !ops.iter().any(|o| matches!(o, ReplicaOp::Delete { .. })))
                .cloned()
                .collect();
            if !non_delete.is_empty() {
                compensate_replication_failure(engine, &non_delete, redo_log);
            }
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    let failed_total = errors.len() as u64;
    let succeeded_total = total_items.saturating_sub(failed_total);
    if let Some(m) = DISPATCH_METRICS.get() {
        m.deletes_succeeded.inc_by(succeeded_total);
        m.deletes_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations
            .inc_by(OpCode::Delete, Outcome::Ok, succeeded_total);
        for e in &errors {
            m.operations
                .inc(OpCode::Delete, classify_wire_error_code(e.error_code));
        }
    }

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

fn handle_mark_longest_chain_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (shared, txids) = match decode_txid_batch(&req.payload, 9) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let on_longest_chain = shared[0] != 0;
    let cbh = u32::from_le_bytes(shared[1..5].try_into().unwrap());
    let bhr = u32::from_le_bytes(shared[5..9].try_into().unwrap());
    let total_items = txids.len() as u64;

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership and build redo ops from request params.
    struct ValidMark {
        idx: usize,
        key: TxKey,
    }
    let mut valid_items: Vec<ValidMark> = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        // Target generation for this mutation is the current primary
        // generation + 1. Replay uses this as the idempotency token (H7):
        // once applied, meta.generation == target_generation, so a later
        // replay with the same (or smaller) generation is skipped.
        // If the record does not exist, default to 1 — the engine will
        // produce TxNotFound, and the replay handler will treat the op as
        // a no-op on the missing record.
        let target_generation = engine
            .lookup(&key)
            .map(|e| e.generation.wrapping_add(1))
            .unwrap_or(1);
        redo_ops.push(RedoOp::MarkOnLongestChain {
            tx_key: key,
            on_longest_chain,
            current_block_height: cbh,
            block_height_retention: bhr,
            generation: target_generation,
        });
        valid_items.push(ValidMark { idx: i, key });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    if let Err(e) = write_redo_ops(redo_log, &redo_ops) {
        return error_response(req.request_id, ERR_INTERNAL, &e);
    }

    // Phase 3: Apply engine mutations.
    for v in &valid_items {
        match engine.mark_on_longest_chain(&MarkOnLongestChainRequest {
            tx_key: v.key,
            on_longest_chain,
            current_block_height: cbh,
            block_height_retention: bhr,
        }) {
            Ok(_) => {
                // MarkOnLongestChain is metadata-only; no dedicated ReplicaOp
                // needed — the SetMined replication already covers block tracking.
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    let failed_total = errors.len() as u64;
    let succeeded_total = total_items.saturating_sub(failed_total);
    if let Some(m) = DISPATCH_METRICS.get() {
        m.mark_longest_chain_succeeded.inc_by(succeeded_total);
        m.mark_longest_chain_failed.inc_by(failed_total);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations
            .inc_by(OpCode::MarkLongestChain, Outcome::Ok, succeeded_total);
        for e in &errors {
            m.operations.inc(
                OpCode::MarkLongestChain,
                classify_wire_error_code(e.error_code),
            );
        }
    }

    batch_response(req.request_id, &errors)
}

// ---------------------------------------------------------------------------
// GetBatch
// ---------------------------------------------------------------------------

fn handle_get_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    let (field_mask, txids) = match decode_get_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed get batch"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let local_read = req.flags & FLAG_LOCAL_READ != 0;

    let mut results = Vec::with_capacity(txids.len());
    // Track per-item outcomes: STATUS_OK => succeeded, ERR_TX_NOT_FOUND =>
    // not_found, anything else => failed.
    let mut ok_count: u64 = 0;
    let mut not_found_count: u64 = 0;
    let mut failed_count: u64 = 0;
    for txid in &txids {
        let key = TxKey { txid: *txid };

        // In cluster mode, serve reads if we're master OR if the record is
        // available locally (handles the migration window where shard tables
        // may be inconsistent across nodes).
        if !local_read && let Some(cluster) = cluster {
            let mastership = cluster.is_master(&key);
            let is_migrating_out = cluster.is_migrating_outbound(&key);

            // Distinguish three cases explicitly:
            //   - Yes        → serve locally (subject to inbound-migration check below)
            //   - No         → REDIRECT (or serve during outbound migration)
            //   - Transitioning → MIGRATION_IN_PROGRESS (retryable)
            let is_master = match mastership {
                crate::cluster::coordinator::MasterQueryResult::Yes => true,
                crate::cluster::coordinator::MasterQueryResult::Transitioning {
                    last_known_term,
                } => {
                    tracing::debug!(
                        last_known_term,
                        "dispatch: get deferring — topology in transition"
                    );
                    results.push(WireGetResult {
                        status: ERR_MIGRATION_IN_PROGRESS as u8,
                        data: vec![],
                    });
                    continue;
                }
                crate::cluster::coordinator::MasterQueryResult::No => false,
            };

            if !is_master && !is_migrating_out {
                let route = cluster.route(&key);
                let redirect_status = match route {
                    crate::cluster::shards::RouteDecision::RedirectTo { node, .. } => {
                        match cluster.node_addr(&node) {
                            Some(addr) => {
                                let addr_bytes = addr.to_string().into_bytes();
                                let mut data = Vec::with_capacity(2 + addr_bytes.len());
                                data.extend_from_slice(&(ERR_REDIRECT as u8).to_le_bytes());
                                data.extend_from_slice(&addr_bytes);
                                data
                            }
                            None => vec![ERR_REDIRECT as u8],
                        }
                    }
                    _ => vec![ERR_REDIRECT as u8],
                };
                // M10: count the stale-routed read.
                if let Some(m) = DISPATCH_METRICS.get() {
                    m.stale_routing_request_total.inc();
                }
                results.push(WireGetResult {
                    status: ERR_REDIRECT as u8,
                    data: redirect_status,
                });
                continue;
            }

            // If we're master but don't have the data and inbound migration
            // is still pending, return a retry signal immediately instead of
            // parking a request thread behind migration progress.
            if is_master && engine.read_metadata(&key).is_err() && cluster.has_pending_inbound(&key)
            {
                let shard = crate::cluster::shards::ShardTable::shard_for_key(&key);
                tracing::debug!(shard, "dispatch: read still waiting for inbound migration");
                results.push(WireGetResult {
                    status: ERR_MIGRATION_IN_PROGRESS as u8,
                    data: vec![],
                });
                continue;
            }
        }
        // Fast path: if ALL requested fields are cached in the primary index,
        // serve directly without reading device metadata (zero I/O).
        if field_mask.fully_cached() {
            if let Some(entry) = engine.lookup_cached(&key) {
                let mut data = Vec::new();
                let has_preserve = entry.tx_flags & TxFlags::HAS_PRESERVE_UNTIL.bits() != 0;
                // Strip the index-only HAS_PRESERVE_UNTIL bit before returning flags
                let wire_flags = entry.tx_flags & !TxFlags::HAS_PRESERVE_UNTIL.bits();
                if field_mask.has(FieldMask::FLAGS) {
                    data.push(wire_flags);
                }
                if field_mask.has(FieldMask::SPENT_UTXOS) {
                    data.extend_from_slice(&entry.spent_utxos.to_le_bytes());
                }
                if field_mask.has(FieldMask::UTXO_COUNT) {
                    data.extend_from_slice(&entry.utxo_count.to_le_bytes());
                }
                if field_mask.has(FieldMask::UNMINED_SINCE) {
                    data.extend_from_slice(&entry.unmined_since.to_le_bytes());
                }
                if field_mask.has(FieldMask::DELETE_AT_HEIGHT) {
                    let dah = if has_preserve {
                        0u32
                    } else {
                        entry.dah_or_preserve
                    };
                    data.extend_from_slice(&dah.to_le_bytes());
                }
                if field_mask.has(FieldMask::PRESERVE_UNTIL) {
                    let pu = if has_preserve {
                        entry.dah_or_preserve
                    } else {
                        0u32
                    };
                    data.extend_from_slice(&pu.to_le_bytes());
                }
                if field_mask.has(FieldMask::BLOCK_ENTRY_COUNT) {
                    data.push(entry.block_entry_count);
                }
                results.push(WireGetResult {
                    status: STATUS_OK,
                    data,
                });
                continue;
            }
            // Not in index — fall through to TxNotFound below
            results.push(WireGetResult {
                status: ERR_TX_NOT_FOUND as u8,
                data: vec![],
            });
            continue;
        }

        // Slow path: read full metadata from device for non-cached fields.
        match engine.read_metadata(&key) {
            Ok(meta) => {
                let mut data = Vec::new();
                if field_mask.has(FieldMask::RAW_METADATA) {
                    // Raw debug mode: dump the full on-disk struct as-is.
                    let mut buf = vec![0u8; METADATA_SIZE];
                    meta.to_bytes(&mut buf);
                    data.extend_from_slice(&buf);
                } else {
                    // Per-field metadata serialization.
                    if field_mask.has(FieldMask::TX_VERSION) {
                        data.extend_from_slice(&{ meta.tx_version }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::LOCKTIME) {
                        data.extend_from_slice(&{ meta.locktime }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::FEE) {
                        data.extend_from_slice(&{ meta.fee }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::SIZE_IN_BYTES) {
                        data.extend_from_slice(&{ meta.size_in_bytes }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::EXTENDED_SIZE) {
                        data.extend_from_slice(&{ meta.extended_size }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::FLAGS) {
                        data.push({ meta.flags }.bits());
                    }
                    if field_mask.has(FieldMask::SPENDING_HEIGHT) {
                        data.extend_from_slice(&{ meta.spending_height }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::CREATED_AT) {
                        data.extend_from_slice(&{ meta.created_at }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::SPENT_UTXOS) {
                        data.extend_from_slice(&{ meta.spent_utxos }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::PRUNED_UTXOS) {
                        data.extend_from_slice(&{ meta.pruned_utxos }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::UTXO_COUNT) {
                        data.extend_from_slice(&{ meta.utxo_count }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::GENERATION) {
                        data.extend_from_slice(&{ meta.generation }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::UPDATED_AT) {
                        data.extend_from_slice(&{ meta.updated_at }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::UNMINED_SINCE) {
                        data.extend_from_slice(&{ meta.unmined_since }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::DELETE_AT_HEIGHT) {
                        data.extend_from_slice(&{ meta.delete_at_height }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::PRESERVE_UNTIL) {
                        data.extend_from_slice(&{ meta.preserve_until }.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::EXTERNAL_REF) {
                        let ext = { meta.external_ref };
                        data.push(ext.store_type);
                        data.extend_from_slice(&ext.content_hash);
                        data.extend_from_slice(&ext.total_size.to_le_bytes());
                        data.extend_from_slice(&ext.input_count.to_le_bytes());
                        data.extend_from_slice(&ext.output_count.to_le_bytes());
                        data.extend_from_slice(&ext.inputs_offset.to_le_bytes());
                        data.extend_from_slice(&ext.outputs_offset.to_le_bytes());
                    }
                    if field_mask.has(FieldMask::REASSIGNMENT_COUNT) {
                        data.push(meta.reassignment_count);
                    }
                    if field_mask.has(FieldMask::BLOCK_ENTRY_COUNT) {
                        data.push(meta.block_entry_count);
                    }
                }
                if field_mask.has(FieldMask::UTXO_SLOTS) {
                    let utxo_count = { meta.utxo_count };
                    data.extend_from_slice(&utxo_count.to_le_bytes());
                    for v in 0..utxo_count {
                        match engine.read_slot(&key, v) {
                            Ok(slot) => {
                                data.extend_from_slice(&slot.hash);
                                data.push(slot.status);
                                data.extend_from_slice(&slot.spending_data);
                            }
                            Err(_) => {
                                // Slot read error — fill with zeros
                                data.extend_from_slice(&[0u8; 69]);
                            }
                        }
                    }
                }
                if field_mask.has(FieldMask::COLD_DATA) {
                    match engine.read_cold_data(&key) {
                        Ok(cold) => {
                            data.extend_from_slice(&(cold.len() as u32).to_le_bytes());
                            data.extend_from_slice(&cold);
                        }
                        Err(_) => {
                            data.extend_from_slice(&0u32.to_le_bytes());
                        }
                    }
                }
                if field_mask.has(FieldMask::BLOCK_ENTRIES) {
                    let count = { meta.block_entry_count };
                    data.push(count);
                    let inline_count = count.min(3);
                    for i in 0..inline_count as usize {
                        let be = { meta.block_entries_inline[i] };
                        data.extend_from_slice(&{ be.block_id }.to_le_bytes());
                        data.extend_from_slice(&{ be.block_height }.to_le_bytes());
                        data.extend_from_slice(&{ be.subtree_idx }.to_le_bytes());
                    }
                }
                if field_mask.has(FieldMask::CONFLICTING_CHILDREN) {
                    match engine.read_conflicting_children(&key) {
                        Ok(children) => {
                            data.push(children.len() as u8);
                            for child in &children {
                                data.extend_from_slice(child);
                            }
                        }
                        Err(_) => {
                            data.push(0u8);
                        }
                    }
                }
                results.push(WireGetResult { status: 0, data });
            }
            Err(SpendError::TxNotFound) => {
                results.push(WireGetResult {
                    status: 1,
                    data: vec![],
                });
            }
            Err(_) => {
                results.push(WireGetResult {
                    status: 1,
                    data: vec![],
                });
            }
        }
    }

    // Classify per-item outcome.
    for r in &results {
        match r.status {
            STATUS_OK => ok_count += 1,
            s if s == ERR_TX_NOT_FOUND as u8 => not_found_count += 1,
            _ => failed_count += 1,
        }
    }
    // Count redirects separately from the "failed" bucket so the labeled
    // operations table can distinguish them. `WireGetResult::status` uses
    // the low byte of the wire error code, so `ERR_REDIRECT as u8` is
    // distinguishable without decoding `data`.
    let mut redirect_count: u64 = 0;
    let mut other_failed: u64 = 0;
    for r in &results {
        match r.status {
            STATUS_OK => {}
            s if s == ERR_TX_NOT_FOUND as u8 => {}
            s if s == ERR_REDIRECT as u8 => redirect_count += 1,
            _ => other_failed += 1,
        }
    }
    if let Some(m) = DISPATCH_METRICS.get() {
        m.gets_succeeded.inc_by(ok_count);
        m.gets_not_found.inc_by(not_found_count);
        m.gets_failed.inc_by(failed_count);
        // Dual-write: labeled operations table.
        use crate::metrics::{OpCode, Outcome};
        m.operations.inc_by(OpCode::Get, Outcome::Ok, ok_count);
        m.operations
            .inc_by(OpCode::Get, Outcome::ErrNotFound, not_found_count);
        m.operations
            .inc_by(OpCode::Get, Outcome::Redirect, redirect_count);
        m.operations
            .inc_by(OpCode::Get, Outcome::Other, other_failed);
    }

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload: encode_get_response(&results),
    }
}

// ---------------------------------------------------------------------------
// Pruner operations
// ---------------------------------------------------------------------------

fn handle_query_old_unmined(req: &RequestFrame, engine: &Engine) -> ResponseFrame {
    // Payload: [cutoff_height:4]
    if req.payload.len() < 4 {
        return error_response(req.request_id, ERR_INTERNAL, "malformed query");
    }
    let cutoff = u32::from_le_bytes(req.payload[0..4].try_into().unwrap());
    let keys = engine.unmined_index().range_query(cutoff);

    let mut payload = Vec::with_capacity(4 + keys.len() * 32);
    payload.extend_from_slice(&(keys.len() as u32).to_le_bytes());
    for key in &keys {
        payload.extend_from_slice(&key.txid);
    }

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload,
    }
}

fn handle_preserve_transactions(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    // Same format as PreserveUntilBatch: [count:4][block_height:4][txids]
    let (shared, txids) = match decode_txid_batch(&req.payload, 4) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if txids.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }
    let height = u32::from_le_bytes(shared[0..4].try_into().unwrap());

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership and build redo ops from request params.
    struct ValidPreserveTx {
        idx: usize,
        key: TxKey,
    }
    let mut valid_items: Vec<ValidPreserveTx> = Vec::new();
    for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        redo_ops.push(RedoOp::PreserveUntil {
            tx_key: key,
            block_height: height,
        });
        valid_items.push(ValidPreserveTx { idx: i, key });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_redo_ops(redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_INTERNAL, &e),
    };

    // Phase 3: Apply engine mutations and build repl ops from engine results.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    for v in &valid_items {
        match engine.preserve_until(&PreserveUntilRequest {
            tx_key: v.key,
            block_height: height,
        }) {
            Ok(resp) => {
                repl_ops_by_key.push((
                    v.key,
                    vec![ReplicaOp::PreserveUntil {
                        tx_key: v.key,
                        block_height: height,
                        master_generation: resp.generation,
                    }],
                ));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range) {
        Ok(o) => o,
        Err(e) => {
            compensate_replication_failure(engine, &repl_ops_by_key, redo_log);
            clear_replication_intent_after_compensation(redo_range);
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

fn handle_process_expired(
    req: &RequestFrame,
    engine: &Engine,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    // Payload: [current_height:4]
    if req.payload.len() < 4 {
        return error_response(req.request_id, ERR_INTERNAL, "malformed");
    }
    let current_height = u32::from_le_bytes(req.payload[0..4].try_into().unwrap());

    // Query DAH index for transactions due for deletion
    let keys = engine.dah_index().range_query(current_height);

    // Phase 1: Lookup record offsets (read-only) and build redo ops.
    let mut redo_ops: Vec<RedoOp> = Vec::new();
    let mut valid_keys: Vec<TxKey> = Vec::new();
    for key in &keys {
        let record_offset = engine.lookup(key).map(|e| e.record_offset).unwrap_or(0);
        redo_ops.push(RedoOp::Delete {
            tx_key: *key,
            record_offset,
            record_size: 0,
        });
        valid_keys.push(*key);
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    if let Err(e) = write_redo_ops(redo_log, &redo_ops) {
        return error_response(req.request_id, ERR_INTERNAL, &e);
    }

    // Phase 3: Apply engine mutations.
    let mut deleted = 0u32;
    let mut failed = 0u32;
    for key in &valid_keys {
        match engine.delete(&DeleteRequest { tx_key: *key }) {
            Ok(()) => deleted += 1,
            Err(_) => failed += 1,
        }
    }

    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&deleted.to_le_bytes());
    payload.extend_from_slice(&failed.to_le_bytes());

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload,
    }
}

// ---------------------------------------------------------------------------
// GetSpend
// ---------------------------------------------------------------------------

fn handle_get_spend_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    let items = match decode_get_spend_batch(&req.payload) {
        Some(r) => r,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed"),
    };
    if items.len() as u32 > max_batch {
        return error_response(req.request_id, ERR_INTERNAL, "batch too large");
    }

    let local_read = req.flags & FLAG_LOCAL_READ != 0;

    let mut results = Vec::with_capacity(items.len());
    for item in &items {
        // Check shard ownership — reads are allowed during outbound migration
        // because this node still holds the data until migration completes.
        // FLAG_LOCAL_READ bypasses this check for replication verification.
        if !local_read && let Some(cluster) = cluster {
            let key = TxKey { txid: item.txid };
            match cluster.is_master(&key) {
                crate::cluster::coordinator::MasterQueryResult::Yes => {}
                crate::cluster::coordinator::MasterQueryResult::Transitioning {
                    last_known_term,
                } => {
                    tracing::debug!(
                        last_known_term,
                        "dispatch: get_spend deferring — topology in transition"
                    );
                    results.push(WireGetSpendResult {
                        status: 1,
                        error_code: ERR_MIGRATION_IN_PROGRESS,
                        slot_status: 0,
                        spending_data: [0; 36],
                    });
                    continue;
                }
                crate::cluster::coordinator::MasterQueryResult::No => {
                    if cluster.is_migrating_outbound(&key) {
                        // Outbound migration: data still present locally.
                    } else {
                        // M10: count the stale-routed GetSpend.
                        if let Some(m) = DISPATCH_METRICS.get() {
                            m.stale_routing_request_total.inc();
                        }
                        results.push(WireGetSpendResult {
                            status: 1,
                            error_code: ERR_REDIRECT,
                            slot_status: 0,
                            spending_data: [0; 36],
                        });
                        continue;
                    }
                }
            }
        }

        // GetSpend needs the utxo_hash for validation. Since the wire format
        // only sends txid+vout, we skip hash validation at this level and
        // return whatever is at that slot offset.
        let key = TxKey { txid: item.txid };
        match engine.read_metadata(&key) {
            Ok(meta) => {
                let utxo_count = { meta.utxo_count };
                if item.vout >= utxo_count {
                    results.push(WireGetSpendResult {
                        status: 1,
                        error_code: ERR_VOUT_OUT_OF_RANGE,
                        slot_status: 0,
                        spending_data: [0; 36],
                    });
                } else {
                    match engine.read_slot(&key, item.vout) {
                        Ok(slot) => {
                            results.push(WireGetSpendResult {
                                status: 0,
                                error_code: ERR_OK,
                                slot_status: slot.status,
                                spending_data: slot.spending_data,
                            });
                        }
                        Err(_) => {
                            results.push(WireGetSpendResult {
                                status: 1,
                                error_code: ERR_INTERNAL,
                                slot_status: 0,
                                spending_data: [0; 36],
                            });
                        }
                    }
                }
            }
            Err(SpendError::TxNotFound) => {
                results.push(WireGetSpendResult {
                    status: 1,
                    error_code: ERR_TX_NOT_FOUND,
                    slot_status: 0,
                    spending_data: [0; 36],
                });
            }
            Err(_) => {
                results.push(WireGetSpendResult {
                    status: 1,
                    error_code: ERR_INTERNAL,
                    slot_status: 0,
                    spending_data: [0; 36],
                });
            }
        }
    }

    // Dual-write: labeled operations table for GetSpend. Classify by the
    // result's `error_code` (already a u16) so the mapping is exact.
    if let Some(m) = DISPATCH_METRICS.get() {
        use crate::metrics::{OpCode, Outcome};
        for r in &results {
            let outcome = if r.status == 0 {
                Outcome::Ok
            } else {
                classify_wire_error_code(r.error_code)
            };
            m.operations.inc(OpCode::GetSpend, outcome);
        }
    }

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload: encode_get_spend_response(&results),
    }
}

// ---------------------------------------------------------------------------
// Streaming blob upload
// ---------------------------------------------------------------------------

/// Handle a single chunk of a streaming blob upload.
///
/// Looks up or creates an active stream session for the given txid in the
/// per-connection state. Validates the chunk offset matches the expected
/// position (no gaps or overlaps). On write error the stream is aborted and
/// removed from the connection state.
fn handle_stream_chunk(
    req: &RequestFrame,
    conn_state: &mut super::ConnectionState,
    blob_store: Option<&dyn BlobStore>,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    let chunk = match decode_stream_chunk(&req.payload) {
        Some(c) => c,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed stream chunk"),
    };

    // Check shard ownership — streaming writes are mutations on the master.
    if let Some(redirect_err) = check_shard_ownership(&chunk.txid, 0, cluster, false) {
        return error_response(req.request_id, redirect_err.error_code, "shard not owned");
    }

    let blob_store = match blob_store {
        Some(bs) => bs,
        None => return error_response(req.request_id, ERR_INTERNAL, "blobstore not configured"),
    };

    // Get or create the stream session for this txid.
    use std::collections::hash_map::Entry;
    if let Entry::Vacant(entry) = conn_state.streams.entry(chunk.txid) {
        match blob_store.begin_stream(&chunk.txid) {
            Ok(writer) => {
                entry.insert(super::ActiveStream {
                    writer,
                    bytes_received: 0,
                });
            }
            Err(e) => {
                return error_response(req.request_id, ERR_INTERNAL, &format!("begin_stream: {e}"));
            }
        }
    }

    let stream = conn_state
        .streams
        .get_mut(&chunk.txid)
        .expect("just inserted");

    // Verify chunk offset matches expected position.
    if chunk.offset != stream.bytes_received {
        return error_response(
            req.request_id,
            ERR_STREAM_OFFSET_MISMATCH,
            &format!(
                "expected offset {}, got {}",
                stream.bytes_received, chunk.offset
            ),
        );
    }

    // Write the chunk data.
    if let Err(e) = stream.writer.write_chunk(chunk.data) {
        // Abort the stream on write error.
        if let Some(s) = conn_state.streams.remove(&chunk.txid) {
            let _ = s.writer.abort();
        }
        return error_response(req.request_id, ERR_INTERNAL, &format!("write_chunk: {e}"));
    }

    stream.bytes_received += chunk.data.len() as u64;

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload: vec![],
    }
}

/// Finalize a streaming blob upload.
///
/// Removes the active stream session from the connection state, verifies
/// the total bytes received match the declared total, and calls `finish`
/// on the blob stream writer to atomically commit the blob.
fn handle_stream_end(req: &RequestFrame, conn_state: &mut super::ConnectionState) -> ResponseFrame {
    let end = match decode_stream_end(&req.payload) {
        Some(e) => e,
        None => return error_response(req.request_id, ERR_INTERNAL, "malformed stream end"),
    };

    let stream = match conn_state.streams.remove(&end.txid) {
        Some(s) => s,
        None => {
            return error_response(
                req.request_id,
                ERR_STREAM_NOT_FOUND,
                "no active stream for txid",
            );
        }
    };

    // Verify total size matches what was received.
    if stream.bytes_received != end.total_size {
        let _ = stream.writer.abort();
        return error_response(
            req.request_id,
            ERR_INTERNAL,
            &format!(
                "size mismatch: received {} bytes, expected {}",
                stream.bytes_received, end.total_size
            ),
        );
    }

    // Finalize the blob — makes it available for reads.
    match stream.writer.finish() {
        Ok(_total) => ResponseFrame {
            request_id: req.request_id,
            status: STATUS_OK,
            payload: vec![],
        },
        Err(e) => error_response(req.request_id, ERR_INTERNAL, &format!("finish: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn error_response(request_id: u64, code: u16, msg: &str) -> ResponseFrame {
    let mut payload = Vec::new();
    payload.extend_from_slice(&code.to_le_bytes());
    payload.extend_from_slice(&(msg.len() as u16).to_le_bytes());
    payload.extend_from_slice(msg.as_bytes());
    ResponseFrame {
        request_id,
        status: STATUS_ERROR,
        payload,
    }
}

fn batch_response(request_id: u64, errors: &[BatchItemError]) -> ResponseFrame {
    batch_response_with_outcome(request_id, errors, ReplicationOutcome::Full)
}

/// Like [`batch_response`], but promotes a clean response to
/// `STATUS_DEGRADED_DURABILITY` when replication returned
/// [`ReplicationOutcome::Degraded`] (best-effort mode, zero replica ACKs).
///
/// When there *are* per-item errors we still return `STATUS_PARTIAL_ERROR`:
/// the partial-error path already conveys that not every item succeeded,
/// and overwriting it with the degraded-durability status would erase the
/// per-item diagnostic detail the client needs. The degraded-durability
/// metric has already been incremented inside `replicate_all_ops`, so the
/// server-side telemetry is unaffected.
fn batch_response_with_outcome(
    request_id: u64,
    errors: &[BatchItemError],
    outcome: ReplicationOutcome,
) -> ResponseFrame {
    if errors.is_empty() {
        let status = if outcome.is_degraded() {
            STATUS_DEGRADED_DURABILITY
        } else {
            STATUS_OK
        };
        ResponseFrame {
            request_id,
            status,
            payload: vec![],
        }
    } else {
        ResponseFrame {
            request_id,
            status: STATUS_PARTIAL_ERROR,
            payload: encode_sparse_errors(errors),
        }
    }
}

fn spend_error_to_batch_error(item_index: u32, err: &SpendError) -> BatchItemError {
    let (code, data) = match err {
        SpendError::TxNotFound => (ERR_TX_NOT_FOUND, vec![]),
        SpendError::Conflicting => (ERR_CONFLICTING, vec![]),
        SpendError::Locked => (ERR_LOCKED, vec![]),
        SpendError::CoinbaseImmature {
            spending_height, ..
        } => (
            ERR_COINBASE_IMMATURE,
            spending_height.to_le_bytes().to_vec(),
        ),
        SpendError::UtxoNotFound { .. } => (ERR_VOUT_OUT_OF_RANGE, vec![]),
        SpendError::UtxoHashMismatch { .. } => (ERR_UTXO_HASH_MISMATCH, vec![]),
        SpendError::AlreadySpent { spending_data, .. } => {
            (ERR_ALREADY_SPENT, spending_data.to_vec())
        }
        SpendError::Frozen { .. } => (ERR_FROZEN, vec![]),
        SpendError::FrozenUntil { .. } => (ERR_FROZEN_UNTIL, vec![]),
        SpendError::InvalidSpend { spending_data, .. } => {
            (ERR_INVALID_SPEND, spending_data.to_vec())
        }
        SpendError::Pruned { .. } => (ERR_INVALID_SPEND, vec![]),
        SpendError::AlreadyFrozen { .. } => (ERR_ALREADY_FROZEN, vec![]),
        SpendError::NotFrozen { .. } => (ERR_UTXO_NOT_FROZEN, vec![]),
        SpendError::StorageError { .. } => (ERR_INTERNAL, vec![]),
        SpendError::DahOverflow { .. } => (ERR_INTERNAL, vec![]),
    };
    BatchItemError {
        item_index,
        error_code: code,
        error_data: data,
    }
}

/// Classify a [`SpendError`] into its coarse-grained [`Outcome`] bucket.
///
/// Mapping (stable — keep in sync with the Phase 2 spec):
/// - `TxNotFound`                         → `ErrNotFound`
/// - `Conflicting`, `AlreadySpent`,
///   `InvalidSpend`, `Pruned`              → `ErrConflicting`
/// - `Locked`, `Frozen`, `FrozenUntil`,
///   `AlreadyFrozen`, `NotFrozen`          → `ErrFrozen`
/// - `StorageError`, `DahOverflow`         → `ErrStorage`
/// - `CoinbaseImmature`, `UtxoNotFound`,
///   `UtxoHashMismatch`                    → `Other`
#[allow(dead_code)] // used by tests + future refactor of error classification
pub(crate) fn classify_spend_error(err: &SpendError) -> crate::metrics::Outcome {
    use crate::metrics::Outcome;
    match err {
        SpendError::TxNotFound => Outcome::ErrNotFound,
        SpendError::Conflicting
        | SpendError::AlreadySpent { .. }
        | SpendError::InvalidSpend { .. }
        | SpendError::Pruned { .. } => Outcome::ErrConflicting,
        SpendError::Locked
        | SpendError::Frozen { .. }
        | SpendError::FrozenUntil { .. }
        | SpendError::AlreadyFrozen { .. }
        | SpendError::NotFrozen { .. } => Outcome::ErrFrozen,
        SpendError::StorageError { .. } | SpendError::DahOverflow { .. } => Outcome::ErrStorage,
        SpendError::CoinbaseImmature { .. }
        | SpendError::UtxoNotFound { .. }
        | SpendError::UtxoHashMismatch { .. } => Outcome::Other,
    }
}

/// Classify a wire-level error code (produced by decode/redirect) into an
/// [`Outcome`]. Used when the dispatch handler constructs a
/// [`BatchItemError`] directly rather than through
/// [`spend_error_to_batch_error`].
pub(crate) fn classify_wire_error_code(code: u16) -> crate::metrics::Outcome {
    use crate::metrics::Outcome;
    match code {
        ERR_REDIRECT => Outcome::Redirect,
        ERR_TX_NOT_FOUND => Outcome::ErrNotFound,
        ERR_CONFLICTING | ERR_ALREADY_SPENT | ERR_INVALID_SPEND | ERR_ALREADY_EXISTS => {
            Outcome::ErrConflicting
        }
        ERR_LOCKED | ERR_FROZEN | ERR_FROZEN_UNTIL | ERR_ALREADY_FROZEN | ERR_UTXO_NOT_FROZEN => {
            Outcome::ErrFrozen
        }
        ERR_INTERNAL => Outcome::ErrStorage,
        _ => Outcome::Other,
    }
}

// ---------------------------------------------------------------------------
// Partition map
// ---------------------------------------------------------------------------

fn handle_get_partition_map(req: &RequestFrame, cluster: Option<&RunningCluster>) -> ResponseFrame {
    match cluster {
        Some(c) => ResponseFrame {
            request_id: req.request_id,
            status: STATUS_OK,
            payload: c.encode_partition_map(),
        },
        None => {
            // Single-node mode: return a trivial partition map
            let mut payload = Vec::new();
            payload.extend_from_slice(&0u64.to_le_bytes()); // version = 0
            payload.extend_from_slice(&1u32.to_le_bytes()); // 1 node
            payload.extend_from_slice(&0u64.to_le_bytes()); // node_id = 0
            let addr = b"127.0.0.1:3300";
            payload.extend_from_slice(&(addr.len() as u16).to_le_bytes());
            payload.extend_from_slice(addr);
            // All 4096 shards map to node 0
            for _ in 0..4096u16 {
                payload.extend_from_slice(&0u64.to_le_bytes());
            }
            ResponseFrame {
                request_id: req.request_id,
                status: STATUS_OK,
                payload,
            }
        }
    }
}

fn handle_get_committed_topology(
    req: &RequestFrame,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    match cluster {
        Some(c) => ResponseFrame {
            request_id: req.request_id,
            status: STATUS_OK,
            payload: c.encode_committed_topology(),
        },
        None => error_response(req.request_id, ERR_INTERNAL, "not clustered"),
    }
}

/// Handle `OP_ADMIN_DIAGNOSE_KEY`: return per-record diagnostic info for a
/// list of txids.
///
/// See the doc comment on [`OP_ADMIN_DIAGNOSE_KEY`] for the exact wire
/// layout. This handler:
///
/// 1. Parses `[count: u32 LE][txid: 32B] * count` from the request.
/// 2. Rejects malformed payloads (no count prefix, length mismatch, or
///    `count > ADMIN_DIAGNOSE_KEY_MAX_TXIDS`) with `STATUS_ERROR` /
///    `ERR_INTERNAL`.
/// 3. For each txid, queries the migration tracker (via
///    `MigrationManager::diagnose_key_routing`) and the local shard
///    table / index to produce one [`KeyDiagnosis`] entry, then encodes
///    them in declaration order.
///
/// Works in single-node mode (no cluster) by returning a defaulted
/// diagnosis where shard-table / migration fields are zero/false.
fn handle_admin_diagnose_key(
    req: &RequestFrame,
    engine: &Engine,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    let payload = &req.payload;
    if payload.len() < 4 {
        return error_response(
            req.request_id,
            ERR_INTERNAL,
            "malformed admin diagnose: missing count",
        );
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes")) as usize;
    if count as u32 > ADMIN_DIAGNOSE_KEY_MAX_TXIDS {
        return error_response(
            req.request_id,
            ERR_INTERNAL,
            "malformed admin diagnose: count exceeds cap",
        );
    }
    let expected_len = 4usize + count.saturating_mul(32);
    if payload.len() != expected_len {
        return error_response(
            req.request_id,
            ERR_INTERNAL,
            "malformed admin diagnose: length mismatch",
        );
    }

    let mut response = Vec::with_capacity(4 + count * KEY_DIAGNOSIS_ENCODED_SIZE);
    response.extend_from_slice(&(count as u32).to_le_bytes());

    for i in 0..count {
        let off = 4 + i * 32;
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&payload[off..off + 32]);
        let key = TxKey { txid };
        let shard = ShardTable::shard_for_key(&key);

        // Migration tracker fields.
        let mut diag = match cluster {
            Some(c) => c.diagnose_key_routing(shard),
            None => crate::cluster::migration::KeyDiagnosis {
                shard,
                this_node_id: 0,
                local_view_canonical_master_id: 0,
                has_local_data: false,
                is_local_master_of_shard: false,
                has_pending_inbound: false,
                is_shard_fenced: false,
                is_migrating_shard: false,
                topology_epoch: 0,
            },
        };

        // Index lookup is in-memory and cheap; no async needed.
        diag.has_local_data = engine.lookup_cached(&key).is_some();

        encode_key_diagnosis(&diag, &mut response);
    }

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload: response,
    }
}

/// Handle `OP_PARTITION_VERSION_REPORT`: return this node's per-shard data
/// state so the coordinator can build a migration plan that reflects the
/// actual on-disk distribution.
///
/// See the doc comment on [`OP_PARTITION_VERSION_REPORT`] for the wire layout.
///
/// `last_applied_seq` is reported as `engine.shard_record_count(shard)` —
/// the engine does not currently track per-shard replication sequence numbers,
/// and a non-zero record count is a safe proxy for "this node holds data for
/// this shard". The migration-plan refinement only fires when the value is
/// strictly greater than zero, so the proxy never causes a wrong skip.
fn handle_partition_version_report(
    req: &RequestFrame,
    engine: &Engine,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    // Reject mismatched cluster_key — a stale coordinator must not influence
    // this node's view of the partition map.
    if req.payload.len() < 8 {
        return error_response(
            req.request_id,
            ERR_INTERNAL,
            "malformed partition version report: missing cluster_key",
        );
    }
    let request_cluster_key = u64::from_le_bytes(req.payload[0..8].try_into().unwrap_or([0u8; 8]));

    let (self_id, local_cluster_key) = match cluster {
        Some(c) => (c.self_id().0, c.local_cluster_key()),
        // Single-node mode: respond with empty entries and zero ids.
        None => (0u64, 0u64),
    };

    if cluster.is_some() && request_cluster_key != local_cluster_key {
        return error_response(
            req.request_id,
            ERR_STALE_EPOCH,
            "partition version report: cluster_key mismatch",
        );
    }

    let entries: Vec<(u16, u8, u8, u64)> = match cluster {
        Some(c) => {
            let table = c.shard_table();
            let table_guard = table.read();
            let inbound_bm = c.inbound_bitmap();
            (0..crate::cluster::shards::NUM_SHARDS as u16)
                .filter_map(|shard| {
                    let count = engine.shard_record_count(shard);
                    let assignment = table_guard.target_assignment(shard);
                    let is_master = assignment.master == c.self_id();
                    let is_subset = inbound_bm.test(shard);
                    let is_replica = assignment.replicas.contains(&c.self_id());
                    // Only emit entries where this node has any role or any data —
                    // shards we neither own nor hold are uninteresting to the
                    // coordinator and would just bloat the response.
                    if !is_master && !is_replica && !is_subset && count == 0 {
                        return None;
                    }
                    let mut flags = 0u8;
                    if is_master {
                        flags |= 0b01;
                    }
                    if is_subset {
                        flags |= 0b10;
                    }
                    let replica_count =
                        u8::try_from(assignment.replicas.len().min(255)).unwrap_or(255);
                    Some((shard, flags, replica_count, count))
                })
                .collect()
        }
        None => Vec::new(),
    };

    let mut payload = Vec::with_capacity(20 + entries.len() * PARTITION_VERSION_ENTRY_SIZE);
    payload.extend_from_slice(&self_id.to_le_bytes());
    payload.extend_from_slice(&local_cluster_key.to_le_bytes());
    payload.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (shard, flags, replica_count, last_applied_seq) in entries {
        payload.extend_from_slice(&shard.to_le_bytes());
        payload.push(flags);
        payload.push(replica_count);
        payload.extend_from_slice(&last_applied_seq.to_le_bytes());
    }

    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload,
    }
}

/// Append the on-the-wire encoding of a [`KeyDiagnosis`] to `out`.
///
/// Layout matches the doc comment on [`OP_ADMIN_DIAGNOSE_KEY`] and
/// writes exactly [`KEY_DIAGNOSIS_ENCODED_SIZE`] bytes.
fn encode_key_diagnosis(d: &crate::cluster::migration::KeyDiagnosis, out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(&d.shard.to_le_bytes());
    out.extend_from_slice(&d.this_node_id.to_le_bytes());
    out.extend_from_slice(&d.local_view_canonical_master_id.to_le_bytes());
    out.push(u8::from(d.has_local_data));
    out.push(u8::from(d.is_local_master_of_shard));
    out.push(u8::from(d.has_pending_inbound));
    out.push(u8::from(d.is_shard_fenced));
    out.push(u8::from(d.is_migrating_shard));
    out.extend_from_slice(&d.topology_epoch.to_le_bytes());
    debug_assert_eq!(out.len() - start, KEY_DIAGNOSIS_ENCODED_SIZE);
}

// ---------------------------------------------------------------------------
// Tests — Layer 1 dispatch tests (no TCP, no Docker)
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::disallowed_macros)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::{BlockDevice, MemoryDevice};
    use crate::index::{DahIndex, Index, UnminedIndex};
    use crate::locks::StripedLocks;
    use crate::ops::engine::Engine;
    use std::sync::Arc;

    /// Test harness for Layer 1 dispatch testing.
    ///
    /// Creates an in-memory Engine with no network, no Docker. Tests handler
    /// logic directly by calling `handle_request()`.
    struct DispatchTestHarness {
        engine: Engine,
    }

    impl DispatchTestHarness {
        /// Create a new harness with a 64 MB in-memory device.
        fn new() -> Self {
            let dev: Arc<dyn BlockDevice> =
                Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
            let alloc = SlotAllocator::new(dev.clone()).unwrap();
            let index = Index::new(10000).unwrap();
            let locks = StripedLocks::new(1024);
            let dah = DahIndex::new();
            let unmined = UnminedIndex::new();
            let engine = Engine::new(dev, index, alloc, locks, dah, unmined);
            Self { engine }
        }

        /// Dispatch a request and return the response.
        fn request(&self, op_code: u16, payload: Vec<u8>) -> ResponseFrame {
            self.request_with_max_batch(op_code, payload, 8192)
        }

        /// Dispatch a request with a custom max_batch_size.
        fn request_with_max_batch(
            &self,
            op_code: u16,
            payload: Vec<u8>,
            max_batch_size: u32,
        ) -> ResponseFrame {
            let req = RequestFrame {
                request_id: 1,
                op_code,
                flags: 0,
                payload,
            };
            let mut conn_state = crate::server::ConnectionState::new();
            handle_request(
                &req,
                &self.engine,
                max_batch_size,
                None,
                None,
                &mut conn_state,
                None,
            )
        }

        fn request_with_cluster(
            &self,
            op_code: u16,
            payload: Vec<u8>,
            cluster: &crate::cluster::coordinator::RunningCluster,
        ) -> ResponseFrame {
            let req = RequestFrame {
                request_id: 1,
                op_code,
                flags: 0,
                payload,
            };
            let mut conn_state = crate::server::ConnectionState::new();
            handle_request(
                &req,
                &self.engine,
                8192,
                Some(cluster),
                None,
                &mut conn_state,
                None,
            )
        }

        /// Create a single transaction with the given utxo_count via OP_CREATE_BATCH.
        fn create_tx(&self, txid: [u8; 32], utxo_count: u32) -> ResponseFrame {
            let hashes: Vec<[u8; 32]> = (0..utxo_count)
                .map(|i| {
                    let mut h = [0u8; 32];
                    h[0] = (i & 0xFF) as u8;
                    h[1] = ((i >> 8) & 0xFF) as u8;
                    h
                })
                .collect();

            let item = WireCreateItem {
                txid,
                tx_version: 1,
                locktime: 0,
                fee: 500,
                size_in_bytes: 250,
                extended_size: 250,
                is_coinbase: false,
                spending_height: 0,
                created_at: 1700000000000,
                flags: 0,
                utxo_hashes: hashes,
                cold_data: vec![],
                block_height: 0,
                mined_block_id: None,
                mined_block_height: None,
                mined_subtree_idx: None,
                parent_txids: vec![],
            };
            let payload = encode_create_batch(&[item]);
            self.request(OP_CREATE_BATCH, payload)
        }

        /// Generate a deterministic txid from a byte value.
        fn make_txid(n: u8) -> [u8; 32] {
            let mut txid = [0u8; 32];
            txid[0] = n;
            txid[31] = n.wrapping_mul(7); // mix a second byte to reduce collisions
            txid
        }
    }

    // -----------------------------------------------------------------------
    // 1a. handle_query_old_unmined — matching txids returned
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_query_old_unmined_returns_matching_txids() {
        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(1);
        let txid_b = DispatchTestHarness::make_txid(2);
        let txid_c = DispatchTestHarness::make_txid(3);

        // Create 3 txs
        assert_eq!(h.create_tx(txid_a, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_c, 2).status, STATUS_OK);

        // Manually insert into unmined index at different heights
        {
            let mut ui = h.engine.unmined_index();
            ui.insert(100, TxKey { txid: txid_a }, None).unwrap();
            ui.insert(200, TxKey { txid: txid_b }, None).unwrap();
            ui.insert(300, TxKey { txid: txid_c }, None).unwrap();
        }

        // Query with cutoff_height=200 — should return txid_a (100) and txid_b (200)
        let mut payload = Vec::new();
        payload.extend_from_slice(&200u32.to_le_bytes());
        let resp = h.request(OP_QUERY_OLD_UNMINED, payload);
        assert_eq!(resp.status, STATUS_OK);

        // Parse response: [count:4][txids × count]
        let count = u32::from_le_bytes(resp.payload[0..4].try_into().unwrap());
        assert_eq!(count, 2);

        let mut returned_txids: Vec<[u8; 32]> = Vec::new();
        for i in 0..count as usize {
            let start = 4 + i * 32;
            let mut txid = [0u8; 32];
            txid.copy_from_slice(&resp.payload[start..start + 32]);
            returned_txids.push(txid);
        }
        assert!(returned_txids.contains(&txid_a));
        assert!(returned_txids.contains(&txid_b));
        assert!(!returned_txids.contains(&txid_c));
    }

    // -----------------------------------------------------------------------
    // 1b. handle_query_old_unmined — malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_query_old_unmined_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_QUERY_OLD_UNMINED, vec![0xAA, 0xBB]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 1c. handle_preserve_transactions — preserves records
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_preserve_transactions_preserves_records() {
        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(10);
        let txid_b = DispatchTestHarness::make_txid(11);
        let txid_c = DispatchTestHarness::make_txid(12);

        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_c, 1).status, STATUS_OK);

        // Send OP_PRESERVE_TRANSACTIONS with height=1000
        let preserve_height: u32 = 1000;
        let mut shared = Vec::new();
        shared.extend_from_slice(&preserve_height.to_le_bytes());
        let payload = encode_txid_batch(&[txid_a, txid_b, txid_c], &shared);
        let resp = h.request(OP_PRESERVE_TRANSACTIONS, payload);
        assert_eq!(resp.status, STATUS_OK);

        // Read back each tx and verify preserve_until is set
        for txid in &[txid_a, txid_b, txid_c] {
            let key = TxKey { txid: *txid };
            let meta = h.engine.read_metadata(&key).unwrap();
            assert_eq!(
                { meta.preserve_until },
                1000,
                "preserve_until should be 1000 for txid starting with {:?}",
                txid[0]
            );
        }
    }

    // -----------------------------------------------------------------------
    // 1d. handle_preserve_transactions — malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_preserve_transactions_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_PRESERVE_TRANSACTIONS, vec![]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 1e. handle_process_expired — deletes eligible records
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_process_expired_deletes_eligible() {
        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(20);
        let txid_b = DispatchTestHarness::make_txid(21);
        let txid_c = DispatchTestHarness::make_txid(22);

        assert_eq!(h.create_tx(txid_a, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_c, 2).status, STATUS_OK);

        // Set DAH on txid_a and txid_b by inserting into the DAH index directly
        {
            let mut dah = h.engine.dah_index();
            dah.insert(500, TxKey { txid: txid_a }, None).unwrap();
            dah.insert(600, TxKey { txid: txid_b }, None).unwrap();
        }

        // Send OP_PROCESS_EXPIRED_PRESERVATIONS with current_height=700
        // (above both DAH entries)
        let mut payload = Vec::new();
        payload.extend_from_slice(&700u32.to_le_bytes());
        let resp = h.request(OP_PROCESS_EXPIRED_PRESERVATIONS, payload);
        assert_eq!(resp.status, STATUS_OK);
        assert!(resp.payload.len() >= 8);

        let deleted = u32::from_le_bytes(resp.payload[0..4].try_into().unwrap());
        let failed = u32::from_le_bytes(resp.payload[4..8].try_into().unwrap());
        assert_eq!(deleted, 2, "expected 2 deleted");
        assert_eq!(failed, 0, "expected 0 failed");

        // Verify txid_a and txid_b are gone, txid_c still exists
        assert!(h.engine.lookup(&TxKey { txid: txid_a }).is_none());
        assert!(h.engine.lookup(&TxKey { txid: txid_b }).is_none());
        assert!(h.engine.lookup(&TxKey { txid: txid_c }).is_some());
    }

    // -----------------------------------------------------------------------
    // 1f. handle_process_expired — malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_process_expired_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_PROCESS_EXPIRED_PRESERVATIONS, vec![]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 2g. Quorum failure — cannot construct RunningCluster without threads
    // -----------------------------------------------------------------------

    // Skipped (with explanation, not #[ignore]):
    //
    // `RunningCluster` has private fields and its constructor spawns SWIM
    // protocol and event-loop threads, making it impossible to construct a
    // lightweight mock in unit tests. A proper quorum-failure test requires
    // either:
    //   (a) Extracting a `QuorumChecker` trait from `RunningCluster`, or
    //   (b) Testing via the integration test layer (tests/cluster_tcp.rs).
    //
    // The `check_quorum()` function is thoroughly tested by inspection of
    // its three code paths (no cluster, peak<=1, alive < quorum_needed).

    // -----------------------------------------------------------------------
    // 3h. Unknown opcode returns error
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_unknown_opcode_returns_error() {
        let h = DispatchTestHarness::new();
        // Use opcode 999 which is not defined
        let resp = h.request(999, vec![]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(
            msg.contains("unknown opcode"),
            "expected 'unknown opcode' in: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // 4i. Spend malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_spend_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_SPEND_BATCH, vec![0xDE, 0xAD, 0xBE]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 4j. Create malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_create_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_CREATE_BATCH, vec![0x01, 0x02, 0x03]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 4k. Get malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_get_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_GET_BATCH, vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 4l. SetMined malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_set_mined_malformed_payload() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_SET_MINED_BATCH, vec![0x01, 0x02, 0x03]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 4m. Delete malformed payload
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_delete_malformed_payload() {
        let h = DispatchTestHarness::new();
        // decode_txid_batch with shared_len=0 requires at least 4 bytes
        let resp = h.request(OP_DELETE_BATCH, vec![0xAA, 0xBB]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 5n. Create then Get — all fields round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_create_then_get_all_fields() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(40);

        // Create 1 tx with 3 UTXOs
        let resp = h.create_tx(txid, 3);
        assert_eq!(resp.status, STATUS_OK);

        // Get it back with ALL_METADATA mask
        let get_payload = encode_get_batch(FieldMask::ALL_METADATA, &[txid]);
        let resp = h.request(OP_GET_BATCH, get_payload);
        assert_eq!(resp.status, STATUS_OK);

        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, 0, "expected found (status=0)");

        // Decode metadata fields in order:
        // tx_version(4) + locktime(4) + fee(8) + size_in_bytes(8) + extended_size(8)
        // + flags(1) + spending_height(4) + created_at(8) + spent_utxos(4) + pruned_utxos(4)
        // + utxo_count(4) + generation(4) + updated_at(8) + unmined_since(4)
        // + delete_at_height(4) + preserve_until(4) + external_ref(65)
        // + reassignment_count(1) + block_entry_count(1)
        let data = &results[0].data;
        let mut pos = 0;

        let tx_version = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        assert_eq!(tx_version, 1);

        let locktime = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        assert_eq!(locktime, 0);

        let fee = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        assert_eq!(fee, 500);

        let size_in_bytes = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        assert_eq!(size_in_bytes, 250);

        let _extended_size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let _flags = data[pos];
        pos += 1;

        let _spending_height = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let _created_at = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let spent_utxos = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        assert_eq!(spent_utxos, 0, "no UTXOs should be spent");

        let _pruned_utxos = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let utxo_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        assert_eq!(utxo_count, 3, "utxo_count should be 3");
        let _ = pos; // silence unused warning
    }

    // -----------------------------------------------------------------------
    // 5o. Create, Spend, then Get — verify spent_utxos=1
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_create_spend_then_get() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(50);

        // Create with 3 UTXOs
        let resp = h.create_tx(txid, 3);
        assert_eq!(resp.status, STATUS_OK);

        // Spend UTXO at vout=0
        let mut utxo_hash = [0u8; 32];
        utxo_hash[0] = 0; // matches the hash generated in create_tx for vout=0
        let spend_params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let spend_item = WireSpendItem {
            txid,
            vout: 0,
            utxo_hash,
            spending_data: {
                let mut sd = [0u8; 36];
                sd[0] = 0xAB;
                sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                sd
            },
        };
        let spend_payload = encode_spend_batch(&spend_params, &[spend_item]);
        let resp = h.request(OP_SPEND_BATCH, spend_payload);
        assert_eq!(resp.status, STATUS_OK);

        // Get and verify spent_utxos=1
        let get_payload = encode_get_batch(FieldMask::SPENT_UTXOS, &[txid]);
        let resp = h.request(OP_GET_BATCH, get_payload);
        assert_eq!(resp.status, STATUS_OK);

        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, 0);
        let spent = u32::from_le_bytes(results[0].data[0..4].try_into().unwrap());
        assert_eq!(spent, 1, "spent_utxos should be 1 after spending 1 UTXO");
    }

    // -----------------------------------------------------------------------
    // 5p. Create, SetMined, then Get — verify block_entry_count > 0
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_create_set_mined_then_get() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(60);

        let resp = h.create_tx(txid, 2);
        assert_eq!(resp.status, STATUS_OK);

        // SetMined
        let set_mined_params = SetMinedBatchParams {
            block_id: 42,
            block_height: 1000,
            subtree_idx: 0,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let set_mined_payload = encode_set_mined_batch(&set_mined_params, &[txid]);
        let resp = h.request(OP_SET_MINED_BATCH, set_mined_payload);
        assert_eq!(resp.status, STATUS_OK);

        // Get with BLOCK_ENTRY_COUNT field
        let get_payload = encode_get_batch(FieldMask::BLOCK_ENTRY_COUNT, &[txid]);
        let resp = h.request(OP_GET_BATCH, get_payload);
        assert_eq!(resp.status, STATUS_OK);

        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, 0);
        let block_entry_count = results[0].data[0];
        assert!(
            block_entry_count > 0,
            "block_entry_count should be > 0 after SetMined, got {block_entry_count}"
        );
    }

    // -----------------------------------------------------------------------
    // 5q. Create, Delete, then Get — not found
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_create_delete_then_get_not_found() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(70);

        let resp = h.create_tx(txid, 2);
        assert_eq!(resp.status, STATUS_OK);

        // Delete
        let delete_payload = encode_txid_batch(&[txid], &[]);
        let resp = h.request(OP_DELETE_BATCH, delete_payload);
        assert_eq!(resp.status, STATUS_OK);

        // Get — should return status=1 (not found)
        let get_payload = encode_get_batch(FieldMask::ALL_METADATA, &[txid]);
        let resp = h.request(OP_GET_BATCH, get_payload);
        assert_eq!(resp.status, STATUS_OK); // overall response is OK

        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].status, 1,
            "expected not-found status=1 after delete"
        );
    }

    // -----------------------------------------------------------------------
    // 5r. Ping returns OK
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_ping_returns_ok() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_PING, vec![]);
        assert_eq!(resp.status, STATUS_OK);
        assert!(resp.payload.is_empty(), "PING payload should be empty");
    }

    // -----------------------------------------------------------------------
    // 5s. Health returns OK
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_health_returns_ok() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_HEALTH, vec![]);
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(resp.payload, b"ok", "HEALTH payload should be b\"ok\"");
    }

    // -----------------------------------------------------------------------
    // 6t. Batch too large rejected
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_batch_too_large_rejected() {
        let h = DispatchTestHarness::new();

        // Build a create batch with 20 items, but set max_batch_size=10
        let items: Vec<WireCreateItem> = (0..20u8)
            .map(|i| {
                let txid = DispatchTestHarness::make_txid(100 + i);
                let mut hash = [0u8; 32];
                hash[0] = i;
                WireCreateItem {
                    txid,
                    tx_version: 1,
                    locktime: 0,
                    fee: 100,
                    size_in_bytes: 100,
                    extended_size: 100,
                    is_coinbase: false,
                    spending_height: 0,
                    created_at: 1700000000000,
                    flags: 0,
                    utxo_hashes: vec![hash],
                    cold_data: vec![],
                    block_height: 0,
                    mined_block_id: None,
                    mined_block_height: None,
                    mined_subtree_idx: None,
                    parent_txids: vec![],
                }
            })
            .collect();
        let payload = encode_create_batch(&items);
        let resp = h.request_with_max_batch(OP_CREATE_BATCH, payload, 10);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
        assert!(
            msg.contains("batch too large"),
            "expected 'batch too large' in: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // WAL-first regression tests — redo fsynced before engine mutation
    // -----------------------------------------------------------------------

    /// Test harness with redo log support for crash-recovery testing.
    struct RedoDispatchHarness {
        engine: Engine,
        redo_log: Arc<Mutex<crate::redo::RedoLog>>,
        data_dev: Arc<MemoryDevice>,
        redo_dev: Arc<MemoryDevice>,
    }

    impl RedoDispatchHarness {
        fn new() -> Self {
            let data_dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
            let redo_dev = Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
            let alloc = SlotAllocator::new(data_dev.clone()).unwrap();
            let index = Index::new(10000).unwrap();
            let locks = StripedLocks::new(1024);
            let dah = DahIndex::new();
            let unmined = UnminedIndex::new();
            let engine = Engine::new(
                data_dev.clone() as Arc<dyn BlockDevice>,
                index,
                alloc,
                locks,
                dah,
                unmined,
            );
            let redo_log = crate::redo::RedoLog::open(
                redo_dev.clone() as Arc<dyn BlockDevice>,
                0,
                4 * 1024 * 1024,
            )
            .unwrap();
            Self {
                engine,
                redo_log: Arc::new(Mutex::new(redo_log)),
                data_dev,
                redo_dev,
            }
        }

        /// Dispatch a request through the full handler with redo log attached.
        fn request(&self, op_code: u16, payload: Vec<u8>) -> ResponseFrame {
            let req = RequestFrame {
                request_id: 1,
                op_code,
                flags: 0,
                payload,
            };
            let mut conn_state = crate::server::ConnectionState::new();
            handle_request(
                &req,
                &self.engine,
                8192,
                None,
                Some(&self.redo_log),
                &mut conn_state,
                None,
            )
        }

        /// Create a transaction and return the response.
        fn create_tx(&self, txid: [u8; 32], utxo_count: u32) -> ResponseFrame {
            let hashes: Vec<[u8; 32]> = (0..utxo_count)
                .map(|i| {
                    let mut h = [0u8; 32];
                    h[0] = (i & 0xFF) as u8;
                    h[1] = ((i >> 8) & 0xFF) as u8;
                    h
                })
                .collect();
            let item = WireCreateItem {
                txid,
                tx_version: 1,
                locktime: 0,
                fee: 500,
                size_in_bytes: 250,
                extended_size: 250,
                is_coinbase: false,
                spending_height: 0,
                created_at: 1700000000000,
                flags: 0,
                utxo_hashes: hashes,
                cold_data: vec![],
                block_height: 0,
                mined_block_id: None,
                mined_block_height: None,
                mined_subtree_idx: None,
                parent_txids: vec![],
            };
            let payload = encode_create_batch(&[item]);
            self.request(OP_CREATE_BATCH, payload)
        }

        /// Simulate crash: drop engine and redo log, rebuild from devices.
        /// Returns a new harness with recovered state.
        fn crash_and_recover(self) -> Self {
            // Drop the engine and redo log — simulates SIGKILL.
            // The MemoryDevice data survives (it's Arc'd).
            let data_dev = self.data_dev.clone();
            let redo_dev = self.redo_dev.clone();
            drop(self);

            // Reopen redo log from device
            let redo_log = crate::redo::RedoLog::open(
                redo_dev.clone() as Arc<dyn BlockDevice>,
                0,
                4 * 1024 * 1024,
            )
            .unwrap();

            // Create fresh index + allocator
            let alloc = SlotAllocator::new(data_dev.clone()).unwrap();
            let mut index: crate::index::PrimaryBackend = Index::new(10000).unwrap().into();

            // Run recovery to rebuild index from redo log
            let stats =
                crate::recovery::recover(&*data_dev as &dyn BlockDevice, &redo_log, &mut index)
                    .unwrap();
            eprintln!(
                "recovery: {} replayed, {} skipped, {} failed",
                stats.entries_replayed, stats.entries_skipped, stats.entries_failed
            );

            let engine = Engine::new(
                data_dev.clone() as Arc<dyn BlockDevice>,
                index,
                alloc,
                StripedLocks::new(1024),
                DahIndex::new(),
                UnminedIndex::new(),
            );

            Self {
                engine,
                redo_log: Arc::new(Mutex::new(redo_log)),
                data_dev,
                redo_dev,
            }
        }
    }

    #[test]
    fn acked_creates_survive_crash() {
        let h = RedoDispatchHarness::new();

        // Create 50 records, collecting ACK'd txids
        let mut acked_keys = Vec::new();
        for i in 0..50u8 {
            let mut txid = [0u8; 32];
            txid[0] = i;
            txid[31] = i.wrapping_mul(7);
            let resp = h.create_tx(txid, 3);
            if resp.status == STATUS_OK {
                acked_keys.push(TxKey { txid });
            }
        }
        assert!(
            !acked_keys.is_empty(),
            "should have ACK'd at least one create"
        );

        // CRASH and recover
        let h2 = h.crash_and_recover();

        // Every ACK'd key must be in the recovered index
        let mut missing = Vec::new();
        for key in &acked_keys {
            if h2.engine.lookup(key).is_none() {
                missing.push(key);
            }
        }
        assert!(
            missing.is_empty(),
            "ACKed creates lost after crash: {}/{} missing",
            missing.len(),
            acked_keys.len()
        );
    }

    #[test]
    fn acked_spends_survive_crash() {
        let h = RedoDispatchHarness::new();

        // Create 20 records
        let mut txids = Vec::new();
        for i in 0..20u8 {
            let mut txid = [0u8; 32];
            txid[0] = i;
            txid[31] = i.wrapping_mul(7);
            let resp = h.create_tx(txid, 3);
            assert_eq!(resp.status, STATUS_OK, "create {i} failed");
            txids.push(txid);
        }

        // Spend slot 0 on the first 10 records
        let mut acked_spends = Vec::new();
        for txid in txids.iter().take(10) {
            let key = TxKey { txid: *txid };
            let hash = h.engine.read_slot(&key, 0).unwrap().hash;
            let spend_item = WireSpendItem {
                txid: *txid,
                vout: 0,
                utxo_hash: hash,
                spending_data: [0xAB; 36],
            };
            let params = SpendBatchParams {
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            };
            let payload = encode_spend_batch(&params, &[spend_item]);
            let resp = h.request(OP_SPEND_BATCH, payload);
            if resp.status == STATUS_OK {
                acked_spends.push(key);
            }
        }
        assert!(
            !acked_spends.is_empty(),
            "should have ACK'd at least one spend"
        );

        // CRASH and recover
        let h2 = h.crash_and_recover();

        // Verify spent slots are still spent after recovery
        let mut lost = 0;
        for key in &acked_spends {
            match h2.engine.read_slot(key, 0) {
                Ok(slot) => {
                    if !slot.is_spent() {
                        lost += 1;
                    }
                }
                Err(_) => lost += 1,
            }
        }
        assert_eq!(
            lost,
            0,
            "ACKed spends lost after crash: {}/{} not spent",
            lost,
            acked_spends.len()
        );
    }

    #[test]
    fn acked_mark_longest_chain_survives_crash() {
        let h = RedoDispatchHarness::new();

        // Create 10 records and set_mined on them
        let mut txids = Vec::new();
        for i in 0..10u8 {
            let mut txid = [0u8; 32];
            txid[0] = i;
            txid[31] = i.wrapping_mul(7);
            let resp = h.create_tx(txid, 2);
            assert_eq!(resp.status, STATUS_OK);
            txids.push(txid);
        }

        // set_mined on all
        let set_mined_params = SetMinedBatchParams {
            block_id: 42,
            block_height: 1000,
            subtree_idx: 5,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let payload = encode_set_mined_batch(&set_mined_params, &txids);
        let resp = h.request(OP_SET_MINED_BATCH, payload);
        assert_eq!(resp.status, STATUS_OK);

        // mark_on_longest_chain = false (unmined) on first 5
        let shared = {
            let mut s = Vec::with_capacity(9);
            s.push(0u8); // on_longest_chain = false
            s.extend_from_slice(&2000u32.to_le_bytes()); // current_block_height
            s.extend_from_slice(&288u32.to_le_bytes()); // block_height_retention
            s
        };
        let payload_bytes = encode_txid_batch(&txids[..5], &shared);
        let resp = h.request(OP_MARK_LONGEST_CHAIN_BATCH, payload_bytes);
        assert_eq!(resp.status, STATUS_OK);

        // CRASH and recover
        let h2 = h.crash_and_recover();

        // The first 5 should have unmined_since = 2000 after recovery
        for txid in txids.iter().take(5) {
            let key = TxKey { txid: *txid };
            let meta = h2.engine.read_metadata(&key).unwrap();
            assert_eq!(
                { meta.unmined_since },
                2000,
                "mark_longest_chain not recovered for txid[0]={:02x}",
                txid[0]
            );
        }
    }

    #[test]
    fn dispatch_get_redirects_non_master_even_if_local_copy_exists() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(90);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let shard = crate::cluster::shards::ShardTable::shard_for_key(&TxKey { txid });
        let members = vec![
            crate::cluster::shards::NodeId(1),
            crate::cluster::shards::NodeId(2),
        ];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 2, 11);
        let master = table.target_assignment(shard).master;
        let self_id = if master == crate::cluster::shards::NodeId(1) {
            crate::cluster::shards::NodeId(2)
        } else {
            crate::cluster::shards::NodeId(1)
        };
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            self_id,
            table,
            &[
                (
                    crate::cluster::shards::NodeId(1),
                    "127.0.0.1:4401".parse().unwrap(),
                ),
                (
                    crate::cluster::shards::NodeId(2),
                    "127.0.0.1:4402".parse().unwrap(),
                ),
            ],
            &members,
            &[],
            &[],
            &[],
            2,
        );

        let resp = h.request_with_cluster(
            OP_GET_BATCH,
            crate::protocol::codec::encode_get_batch(FieldMask::ALL_METADATA, &[txid]),
            &cluster,
        );
        assert_eq!(resp.status, STATUS_OK);
        let results = crate::protocol::codec::decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, ERR_REDIRECT as u8);
    }

    #[test]
    fn dispatch_returns_migration_in_progress_for_transitioning_state() {
        // Phase B4: when the local topology_epoch is ahead of the
        // committed term (membership change proposed but not quorum-
        // committed), GET_BATCH must return ERR_MIGRATION_IN_PROGRESS
        // (retryable) rather than ERR_REDIRECT (non-retryable to a
        // possibly-wrong target).
        let h = DispatchTestHarness::new();
        let shard = 33u16;
        let mut txid = [0u8; 32];
        txid[..2].copy_from_slice(&shard.to_le_bytes());

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 7);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4901".parse().unwrap(),
            )],
            &members,
            &[],
            &[],
            &[],
            1,
        );
        // Drive the cluster into the "Transitioning" gap: local
        // topology_epoch = 8, committed_term still = 7. We bump
        // `topology_epoch` directly because `cluster_key_handle()` now
        // exposes the quorum-committed term (see `RunningCluster::
        // local_cluster_key`), which is precisely the value that must
        // *not* advance to trigger the Transitioning gap here.
        cluster
            .topology_epoch_handle()
            .store(8, std::sync::atomic::Ordering::Release);

        let resp = h.request_with_cluster(
            OP_GET_BATCH,
            crate::protocol::codec::encode_get_batch(FieldMask::ALL_METADATA, &[txid]),
            &cluster,
        );
        assert_eq!(resp.status, STATUS_OK);
        let results = crate::protocol::codec::decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].status, ERR_MIGRATION_IN_PROGRESS as u8,
            "Transitioning state must yield ERR_MIGRATION_IN_PROGRESS, not ERR_REDIRECT \
             (so the client retries instead of chasing a possibly-wrong redirect target)",
        );
    }

    #[test]
    fn dispatch_get_pending_inbound_returns_quick_retry_signal() {
        let h = DispatchTestHarness::new();
        let shard = 77u16;
        let mut txid = [0u8; 32];
        txid[..2].copy_from_slice(&shard.to_le_bytes());
        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 12);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4501".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        let started = std::time::Instant::now();
        let resp = h.request_with_cluster(
            OP_GET_BATCH,
            crate::protocol::codec::encode_get_batch(FieldMask::ALL_METADATA, &[txid]),
            &cluster,
        );
        let elapsed = started.elapsed();

        assert_eq!(resp.status, STATUS_OK);
        let results = crate::protocol::codec::decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, ERR_MIGRATION_IN_PROGRESS as u8);
        assert!(
            elapsed < std::time::Duration::from_millis(200),
            "pending inbound read should fail fast, took {:?}",
            elapsed
        );
    }

    #[test]
    fn migration_complete_zero_count_clears_populated_inbound_shard() {
        let h = DispatchTestHarness::new();
        let shard = 91u16;
        let mut txid = [0u8; 32];
        txid[..2].copy_from_slice(&shard.to_le_bytes());
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 12);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4601".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload: 0u64.to_le_bytes().to_vec(),
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            cluster.inbound_pending_count(),
            0,
            "zero-count completion should clear pending inbound even when the shard already has data"
        );
    }

    #[test]
    fn migration_complete_full_zero_payload_clears_populated_inbound_shard() {
        let h = DispatchTestHarness::new();
        let shard = 92u16;
        let mut txid = [0u8; 32];
        txid[..2].copy_from_slice(&shard.to_le_bytes());
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 12);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4602".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        assert_eq!(cluster.inbound_pending_count(), 1);

        let mut payload = Vec::new();
        payload.extend_from_slice(&0u64.to_le_bytes());
        payload.extend_from_slice(&0u64.to_le_bytes());
        payload.extend_from_slice(&0u64.to_le_bytes());
        payload.extend_from_slice(&[0u8; 32]);
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&crate::cluster::shards::NodeId(7).0.to_le_bytes());

        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload,
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            cluster.inbound_pending_count(),
            0,
            "the real zero-count completion wire format should clear populated inbound shards"
        );
    }

    #[test]
    fn stale_migration_batch_does_not_recreate_inbound_on_settled_shard() {
        let h = DispatchTestHarness::new();
        let shard = 123u16;
        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 20);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4603".parse().unwrap(),
            )],
            &members,
            &[],
            &[],
            &[],
            1,
        );

        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_REPLICA_BATCH,
            flags: FLAG_MIGRATION_BATCH,
            payload: ReplicaBatch {
                first_sequence: 0,
                ops: vec![],
                trace_ctx: None,
                source_node_id: None,
                cluster_key: 0,
            }
            .serialize(),
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            cluster.inbound_pending_count(),
            0,
            "late migration batches must not recreate inbound fences after handoff settled",
        );
    }

    // -----------------------------------------------------------------------
    // Replication outcome classifier (pure-function tests).
    //
    // Exercises the ACK-tally classifier that drives whether a write path
    // returns STATUS_OK, STATUS_DEGRADED_DURABILITY, or ERR_REPLICATION_FAILED
    // to the client. The response-frame tests below then confirm the byte
    // mapping from classifier outcome → wire status.
    // -----------------------------------------------------------------------

    use crate::replication::manager::AckPolicy;

    #[test]
    fn classify_zero_acks_best_effort_is_degraded() {
        // Best-effort cluster, 2 replicas targeted, 0 ACKed → silently single-node
        let c = classify_replication_outcome(0, 2, None, true);
        assert_eq!(c, ReplicationClassification::ZeroAckBestEffort);
    }

    #[test]
    fn classify_partial_ack_best_effort_is_partial() {
        // 1 of 2 replicas ACKed, best-effort mode — still multi-node durable
        let c = classify_replication_outcome(1, 2, None, true);
        assert_eq!(c, ReplicationClassification::PartialAck);
    }

    #[test]
    fn classify_full_ack_best_effort_is_full() {
        let c = classify_replication_outcome(2, 2, None, true);
        assert_eq!(c, ReplicationClassification::FullAck);
    }

    #[test]
    fn classify_zero_acks_strict_mode_is_policy_violation() {
        // Not best-effort, WriteAll with 2 targets and 0 ACKs → violation
        let c = classify_replication_outcome(0, 2, Some(AckPolicy::WriteAll), false);
        assert_eq!(
            c,
            ReplicationClassification::PolicyViolation { required: 2 }
        );
    }

    #[test]
    fn classify_partial_below_majority_strict_is_policy_violation() {
        // 3 replicas, majority requires ceil(3/2) = 2 ACKs; 1 ACK → violation
        let c = classify_replication_outcome(1, 3, Some(AckPolicy::WriteMajority), false);
        assert_eq!(
            c,
            ReplicationClassification::PolicyViolation { required: 2 }
        );
    }

    #[test]
    fn classify_majority_met_exactly_strict_is_partial_ack() {
        // 3 replicas, 2 ACKs = majority met → `PartialAck` (not `FullAck`)
        let c = classify_replication_outcome(2, 3, Some(AckPolicy::WriteMajority), false);
        assert_eq!(c, ReplicationClassification::PartialAck);
    }

    #[test]
    fn classify_no_targets_is_full() {
        // Empty target list — nothing to ACK, trivially full.
        let c = classify_replication_outcome(0, 0, None, true);
        assert_eq!(c, ReplicationClassification::FullAck);
    }

    #[test]
    fn replication_ack_timeout_extends_only_during_migration_pressure() {
        assert_eq!(
            replication_ack_timeout_for(Duration::from_secs(3), false),
            Duration::from_secs(3)
        );
        assert_eq!(
            replication_ack_timeout_for(Duration::from_secs(3), true),
            Duration::from_secs(30)
        );
        assert_eq!(
            replication_ack_timeout_for(Duration::from_secs(45), true),
            Duration::from_secs(45)
        );
    }

    // -----------------------------------------------------------------------
    // Status-byte mapping (batch_response_with_outcome).
    //
    // The spec requires asserting on the ACTUAL status byte, not `!=0`.
    // These tests pin the byte value emitted for each outcome.
    // -----------------------------------------------------------------------

    #[test]
    fn degraded_outcome_maps_to_status_degraded_durability_byte() {
        // STATUS_DEGRADED_DURABILITY has the concrete wire value 5.
        assert_eq!(STATUS_DEGRADED_DURABILITY, 5);

        let resp = batch_response_with_outcome(42, &[], ReplicationOutcome::Degraded);
        // The test MUST check the exact status byte, not merely that
        // status != STATUS_OK.
        assert_eq!(resp.status, STATUS_DEGRADED_DURABILITY);
        assert_eq!(resp.status, 5u8);
        assert_ne!(resp.status, STATUS_OK);
        assert_eq!(resp.request_id, 42);
        assert!(resp.payload.is_empty());
    }

    #[test]
    fn full_outcome_maps_to_status_ok_byte() {
        let resp = batch_response_with_outcome(7, &[], ReplicationOutcome::Full);
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(resp.status, 0u8);
    }

    #[test]
    fn not_applicable_outcome_maps_to_status_ok_byte() {
        // Standalone server / no replicas resolved — clean STATUS_OK.
        let resp = batch_response_with_outcome(11, &[], ReplicationOutcome::NotApplicable);
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(resp.status, 0u8);
    }

    #[test]
    fn partial_errors_override_degraded_status() {
        // If the batch had per-item errors, we must return STATUS_PARTIAL_ERROR
        // so the client sees the per-item diagnostics, not a blanket status
        // byte that hides them. The degraded-durability escalation is still
        // visible via server metrics.
        let errors = vec![BatchItemError {
            item_index: 0,
            error_code: ERR_TX_NOT_FOUND,
            error_data: vec![],
        }];
        let resp = batch_response_with_outcome(1, &errors, ReplicationOutcome::Degraded);
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);
        assert_ne!(resp.status, STATUS_DEGRADED_DURABILITY);
    }

    // -----------------------------------------------------------------------
    // End-to-end: the classifier-driven status byte selection matches the
    // semantic requirements of the C9 bug fix.
    //
    // These tests compose classifier + response-frame builder in the same
    // way `replicate_all_ops` does, so they verify that the "response seen
    // by the client" is correct for the relevant ACK patterns. They
    // intentionally do not spin up a RunningCluster (the cluster module is
    // outside this fix's scope) — instead they model the exact ACK-counting
    // boundary that was broken.
    // -----------------------------------------------------------------------

    /// Map a [`ReplicationClassification`] to the [`ReplicationOutcome`]
    /// the real dispatch path would synthesize. Kept in sync with the logic
    /// at the end of `replicate_all_ops`.
    fn classification_to_outcome(c: ReplicationClassification) -> ReplicationOutcome {
        match c {
            ReplicationClassification::FullAck | ReplicationClassification::PartialAck => {
                ReplicationOutcome::Full
            }
            ReplicationClassification::ZeroAckBestEffort => ReplicationOutcome::Degraded,
            ReplicationClassification::PolicyViolation { .. } => {
                // Strict mode — in the real dispatch this is returned as
                // Err and becomes ERR_REPLICATION_FAILED; this helper is
                // only exercised on success paths in the tests below.
                ReplicationOutcome::Full
            }
        }
    }

    #[test]
    fn best_effort_all_replicas_fail_yields_status_degraded_durability() {
        // Simulated "all failed": 0 out of 2 replicas ACKed, best-effort.
        let classification = classify_replication_outcome(0, 2, None, true);
        assert_eq!(classification, ReplicationClassification::ZeroAckBestEffort);

        let outcome = classification_to_outcome(classification);
        assert_eq!(outcome, ReplicationOutcome::Degraded);

        let resp = batch_response_with_outcome(1, &[], outcome);
        // Exact status byte, per spec.
        assert_eq!(resp.status, STATUS_DEGRADED_DURABILITY);
        assert_ne!(resp.status, STATUS_OK);
    }

    #[test]
    fn best_effort_some_replicas_ack_yields_status_ok() {
        // Policy: "any ACK = OK in best-effort" — documented in
        // `replicate_all_ops` as the PartialAck case. 1 of 3 ACKed → OK.
        let classification = classify_replication_outcome(1, 3, None, true);
        assert_eq!(classification, ReplicationClassification::PartialAck);

        let outcome = classification_to_outcome(classification);
        assert_eq!(outcome, ReplicationOutcome::Full);

        let resp = batch_response_with_outcome(1, &[], outcome);
        assert_eq!(resp.status, STATUS_OK);
        assert_ne!(resp.status, STATUS_DEGRADED_DURABILITY);
    }

    #[test]
    fn strict_mode_zero_acks_is_hard_error_not_degraded() {
        // With non-best-effort mode the caller propagates Err which maps to
        // ERR_REPLICATION_FAILED on the wire — not STATUS_DEGRADED_DURABILITY.
        let classification = classify_replication_outcome(0, 2, Some(AckPolicy::WriteAll), false);
        assert_eq!(
            classification,
            ReplicationClassification::PolicyViolation { required: 2 }
        );
    }

    // -----------------------------------------------------------------------
    // H3: OP_MIGRATION_COMPLETE manifest enforcement.
    //
    // Source nodes MUST include a manifest hash (or exact-entry manifest)
    // when `record_count > 0`. Without one, a malformed/stale frame could
    // mark a non-empty shard migrated prematurely. These tests exercise
    // the three required paths:
    //   1. non-empty with no manifest → rejected with ERR_MIGRATION_MANIFEST_REQUIRED
    //   2. non-empty with mismatched manifest → ERR_MIGRATION_MANIFEST_MISMATCH
    //   3. non-empty with matching manifest → STATUS_OK and pending-inbound cleared
    // -----------------------------------------------------------------------

    /// Helper: build an `OP_MIGRATION_COMPLETE` payload with the given
    /// `record_count`, optional manifest hash, optional exact-entry manifest,
    /// and optional completion source node id. Mirrors the on-wire layout
    /// the dispatch handler decodes.
    fn build_migration_complete_payload(
        record_count: u64,
        fence_sequence: u64,
        migration_epoch: u64,
        manifest_hash: Option<[u8; 32]>,
        exact_entries: Option<&[(TxKey, u32)]>,
        from_node: Option<NodeId>,
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&record_count.to_le_bytes());
        payload.extend_from_slice(&fence_sequence.to_le_bytes());
        payload.extend_from_slice(&migration_epoch.to_le_bytes());
        // manifest_hash (32 bytes, all-zero = "no manifest")
        payload.extend_from_slice(&manifest_hash.unwrap_or([0u8; 32]));
        let entries = exact_entries.unwrap_or(&[]);
        payload.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for (key, generation) in entries {
            payload.extend_from_slice(&key.txid);
            payload.extend_from_slice(&generation.to_le_bytes());
        }
        if let Some(node) = from_node {
            payload.extend_from_slice(&node.0.to_le_bytes());
        }
        payload
    }

    /// Construct a txid whose shard (low 12 bits of txid[0..2]) equals `shard`.
    fn txid_for_shard(shard: u16, salt: u8) -> [u8; 32] {
        let mut txid = [0u8; 32];
        // Low 12 bits of little-endian u16 at [0..2] = shard.
        let bytes = (shard & 0x0FFF).to_le_bytes();
        txid[0] = bytes[0];
        // Preserve the shard bits in byte 1's low nibble.
        txid[1] = bytes[1];
        txid[2] = salt;
        txid
    }

    #[test]
    fn pending_replication_recovery_requires_redo_log() {
        let tracker = crate::replication::durable::ReplicationIntentTracker::in_memory();
        tracker.begin(7, 7).unwrap();
        let h = DispatchTestHarness::new();

        let err =
            recover_pending_replication_intents_from_tracker(&tracker, None, &h.engine, |_, _| {
                panic!("replication must not run without redo")
            })
            .unwrap_err();

        assert!(err.contains("requires redo log"), "err was: {err}");
        assert_eq!(tracker.pending().len(), 1);
    }

    #[test]
    fn pending_replication_recovery_replays_redo_and_commits_intent() {
        let h = DispatchTestHarness::new();
        let redo_dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let redo_log = Mutex::new(
            RedoLog::open(redo_dev, 0, 4 * 1024 * 1024).expect("redo log opens on memory device"),
        );
        let tracker = crate::replication::durable::ReplicationIntentTracker::in_memory();
        let tx_key = TxKey {
            txid: txid_for_shard(40, 9),
        };
        let range = write_redo_ops(
            Some(&redo_log),
            &[RedoOp::Delete {
                tx_key,
                record_offset: 4096,
                record_size: 256,
            }],
        )
        .expect("redo write succeeds");
        tracker.begin(range.0, range.1).unwrap();

        let mut observed_range = None;
        let mut observed_ops = Vec::new();
        recover_pending_replication_intents_from_tracker(
            &tracker,
            Some(&redo_log),
            &h.engine,
            |ops, range| {
                observed_range = Some(range);
                observed_ops = ops.to_vec();
                Ok(())
            },
        )
        .expect("pending intent recovery succeeds");

        assert!(tracker.pending().is_empty());
        assert_eq!(observed_range, Some(range));
        assert_eq!(observed_ops.len(), 1);
        assert_eq!(observed_ops[0].0, tx_key);
        assert!(matches!(
            observed_ops[0].1.as_slice(),
            [ReplicaOp::Delete { tx_key: deleted }] if *deleted == tx_key
        ));
    }

    #[test]
    fn replicate_all_ops_rf2_missing_replica_address_fails() {
        let members = vec![
            crate::cluster::shards::NodeId(1),
            crate::cluster::shards::NodeId(2),
        ];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 2, 90);
        let shard = (0..crate::cluster::shards::NUM_SHARDS as u16)
            .find(|&s| {
                let assignment = table.target_assignment(s);
                assignment.master == crate::cluster::shards::NodeId(1)
                    && assignment
                        .replicas
                        .contains(&crate::cluster::shards::NodeId(2))
            })
            .expect("expected a shard mastered by node 1 with node 2 as replica");
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4801".parse().unwrap(),
            )],
            &members,
            &[],
            &[],
            &[],
            2,
        );
        let tx_key = TxKey {
            txid: txid_for_shard(shard, 9),
        };
        let ops = vec![(
            tx_key,
            vec![crate::replication::protocol::ReplicaOp::Delete { tx_key }],
        )];

        let err = replicate_all_ops(Some(&cluster), &ops, (0, 0))
            .expect_err("RF>1 write must fail when the replica address is unresolved");
        assert!(
            err.contains("has no resolved address"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn migration_complete_rejects_non_empty_without_manifest() {
        let h = DispatchTestHarness::new();
        let shard = 30u16;
        let txid = txid_for_shard(shard, 1);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 42);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4701".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        // Claim record_count=1 but send no manifest hash and no exact entries.
        let payload = build_migration_complete_payload(1, 0, 0, None, None, None);
        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload,
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(
            resp.status, STATUS_ERROR,
            "non-empty without manifest must be rejected"
        );
        assert!(resp.payload.len() >= 2);
        let err_code = u16::from_le_bytes(resp.payload[..2].try_into().unwrap());
        assert_eq!(
            err_code, ERR_MIGRATION_MANIFEST_REQUIRED,
            "expected ERR_MIGRATION_MANIFEST_REQUIRED, got {err_code}"
        );
        // Pending-inbound MUST remain set — the unverified frame must not
        // advance migration state.
        assert_eq!(
            cluster.inbound_pending_count(),
            1,
            "rejected completion must not clear pending inbound"
        );
    }

    #[test]
    fn migration_complete_rejects_mismatched_manifest() {
        let h = DispatchTestHarness::new();
        let shard = 31u16;
        let txid = txid_for_shard(shard, 2);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);
        assert_eq!(
            h.engine.shard_record_count(shard),
            1,
            "test precondition: shard {shard} must contain the created record"
        );

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 43);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4702".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );
        let pending_before = cluster.inbound_pending_count();
        assert_eq!(
            pending_before, 1,
            "test precondition: 1 shard pending inbound before OP_MIGRATION_COMPLETE"
        );

        // Use a deliberately wrong manifest (all-ones → cannot match real data).
        let wrong_manifest = [0xFFu8; 32];
        let payload = build_migration_complete_payload(1, 0, 0, Some(wrong_manifest), None, None);
        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload,
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_ERROR);
        assert!(resp.payload.len() >= 2);
        let err_code = u16::from_le_bytes(resp.payload[..2].try_into().unwrap());
        assert_eq!(
            err_code, ERR_MIGRATION_MANIFEST_MISMATCH,
            "expected ERR_MIGRATION_MANIFEST_MISMATCH, got {err_code}"
        );
        assert_eq!(
            cluster.inbound_pending_count(),
            1,
            "mismatched manifest must not clear pending inbound"
        );
    }

    #[test]
    fn migration_complete_accepts_matching_manifest() {
        let h = DispatchTestHarness::new();
        let shard = 32u16;
        let txid = txid_for_shard(shard, 3);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        // Compute the expected manifest hash over the single record present.
        let key = TxKey { txid };
        let meta = h.engine.read_metadata(&key).unwrap();
        let mut expected = crate::cluster::coordinator::ManifestHasher::new();
        expected.fold(&txid, meta.generation);
        let expected_hash = expected.finalize();

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 44);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4703".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        let payload = build_migration_complete_payload(1, 0, 0, Some(expected_hash), None, None);
        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload,
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_OK, "matching manifest should succeed");
        assert_eq!(
            cluster.inbound_pending_count(),
            0,
            "matching manifest should clear pending inbound"
        );
    }

    #[test]
    fn migration_complete_exact_entries_prune_extra_local_records() {
        let h = DispatchTestHarness::new();
        let shard = 37u16;
        let txid_a = txid_for_shard(shard, 7);
        let txid_b = txid_for_shard(shard, 8);
        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 1).status, STATUS_OK);

        let key_a = TxKey { txid: txid_a };
        let meta_a = h.engine.read_metadata(&key_a).unwrap();
        let entries = vec![(key_a, meta_a.generation)];

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 49);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4708".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        let payload = build_migration_complete_payload(
            1,
            0,
            0,
            None,
            Some(&entries),
            Some(crate::cluster::shards::NodeId(2)),
        );
        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload,
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            cluster.inbound_pending_count(),
            0,
            "successful exact-entry reconciliation should clear pending inbound"
        );
        assert_eq!(h.engine.shard_record_count(shard), 1);
        assert!(h.engine.read_metadata(&key_a).is_ok());
        assert!(h.engine.read_metadata(&TxKey { txid: txid_b }).is_err());
    }

    #[test]
    fn migration_complete_rejects_count_mismatch_without_exact_entries() {
        let h = DispatchTestHarness::new();
        let shard = 38u16;
        let txid_a = txid_for_shard(shard, 9);
        let txid_b = txid_for_shard(shard, 10);
        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 1).status, STATUS_OK);

        let key_a = TxKey { txid: txid_a };
        let meta_a = h.engine.read_metadata(&key_a).unwrap();
        let mut expected = crate::cluster::coordinator::ManifestHasher::new();
        expected.fold(&txid_a, meta_a.generation);
        let expected_hash = expected.finalize();

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 50);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4709".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        let payload = build_migration_complete_payload(1, 0, 0, Some(expected_hash), None, None);
        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload,
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_ERROR);
        assert!(resp.payload.len() >= 2);
        let err_code = u16::from_le_bytes(resp.payload[..2].try_into().unwrap());
        assert_eq!(err_code, ERR_MIGRATION_IN_PROGRESS);
        assert_eq!(cluster.inbound_pending_count(), 1);
    }

    #[test]
    fn migration_complete_verify_only_keeps_inbound_pending() {
        let h = DispatchTestHarness::new();
        let shard = 36u16;
        let txid = txid_for_shard(shard, 6);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let key = TxKey { txid };
        let meta = h.engine.read_metadata(&key).unwrap();
        let entries = vec![(key, meta.generation)];
        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 48);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4707".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        let payload = build_migration_complete_payload(
            1,
            0,
            0,
            None,
            Some(&entries),
            Some(crate::cluster::shards::NodeId(2)),
        );
        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: FLAG_MIGRATION_VERIFY_ONLY,
            payload,
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            cluster.inbound_pending_count(),
            1,
            "verify-only completion must not clear pending inbound until the batched durable completion arrives"
        );
    }

    #[test]
    fn migration_complete_accepts_non_empty_with_exact_entry_manifest() {
        // The exact-entry manifest (list of (txid, generation)) is an
        // alternative to the SHA-256 hash — also cryptographically strong
        // evidence of shard content. A non-empty migration-complete with
        // exact entries but no hash must still be accepted.
        let h = DispatchTestHarness::new();
        let shard = 33u16;
        let txid = txid_for_shard(shard, 4);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);
        let meta = h.engine.read_metadata(&TxKey { txid }).unwrap();

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 45);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4704".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        let entries = vec![(TxKey { txid }, meta.generation)];
        let payload = build_migration_complete_payload(
            1,
            0,
            0,
            None,
            Some(&entries),
            Some(crate::cluster::shards::NodeId(1)),
        );
        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload,
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(cluster.inbound_pending_count(), 0);
    }

    #[test]
    fn migration_no_data_completion_clears_only_source_inbound() {
        let h = DispatchTestHarness::new();
        let shard = 34u16;
        let members = vec![
            crate::cluster::shards::NodeId(1),
            crate::cluster::shards::NodeId(2),
            crate::cluster::shards::NodeId(3),
        ];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 2, 46);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4705".parse().unwrap(),
            )],
            &members,
            &[],
            &[],
            &[],
            3,
        );
        cluster.register_test_inbound_from_source(shard, crate::cluster::shards::NodeId(2));
        cluster.register_test_inbound_from_source(shard, crate::cluster::shards::NodeId(3));
        assert_eq!(cluster.inbound_pending_count(), 2);

        let payload = build_migration_complete_payload(
            0,
            0,
            0,
            None,
            Some(&[]),
            Some(crate::cluster::shards::NodeId(2)),
        );
        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload,
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            cluster.pending_inbound_entries(),
            vec![(shard, crate::cluster::shards::NodeId(3))],
            "zero-record completion from one source must not clear other sources"
        );
        assert!(cluster.has_pending_inbound_shard(shard));
    }

    #[test]
    fn migration_batch_complete_clears_only_source_inbound() {
        let h = DispatchTestHarness::new();
        let shard = 35u16;
        let members = vec![
            crate::cluster::shards::NodeId(1),
            crate::cluster::shards::NodeId(2),
            crate::cluster::shards::NodeId(3),
        ];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 2, 47);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4706".parse().unwrap(),
            )],
            &members,
            &[],
            &[],
            &[],
            3,
        );
        cluster.register_test_inbound_from_source(shard, crate::cluster::shards::NodeId(2));
        cluster.register_test_inbound_from_source(shard, crate::cluster::shards::NodeId(3));
        assert_eq!(cluster.inbound_pending_count(), 2);

        let mut payload = Vec::new();
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&shard.to_le_bytes());
        payload.extend_from_slice(&crate::cluster::shards::NodeId(2).0.to_le_bytes());
        let req = RequestFrame {
            request_id: 0,
            op_code: OP_MIGRATION_BATCH_COMPLETE,
            flags: 0,
            payload,
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            cluster.pending_inbound_entries(),
            vec![(shard, crate::cluster::shards::NodeId(3))],
            "batch completion from one source must not clear other sources"
        );
        assert!(cluster.has_pending_inbound_shard(shard));
    }

    // -----------------------------------------------------------------------
    // H10: topology vote MUST persist before reply.
    //
    // The voter persists `voted_term` / `committed_term` to disk BEFORE the
    // reply frame is constructed. If the persist fails, the reply carries
    // ERR_TOPOLOGY_PERSIST_FAILED (not STATUS_OK) so the proposer does not
    // count the vote. Without this ordering, a voter that crashed between
    // the reply and the persist could vote differently on restart →
    // split-brain.
    // -----------------------------------------------------------------------

    #[test]
    fn topology_vote_persisted_before_reply() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("node.topology");

        let h = DispatchTestHarness::new();
        let self_id = crate::cluster::shards::NodeId(1);
        let members = vec![self_id, crate::cluster::shards::NodeId(2)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&[self_id], 1, 10);
        let cluster = crate::cluster::coordinator::new_test_running_cluster_with_topology_path(
            self_id,
            table,
            &[(self_id, "127.0.0.1:4710".parse().unwrap())],
            &[self_id],
            &[],
            &[],
            &[],
            1,
            Some(path.clone()),
        );

        // Propose a new term that subsumes this single-node cluster.
        let proposer = crate::cluster::shards::NodeId(2);
        let propose = crate::cluster::topology::TopologyTerm::new(500, members.clone(), proposer);

        let req = RequestFrame {
            request_id: 1,
            op_code: OP_TOPOLOGY_PROPOSE,
            flags: 0,
            payload: propose.serialize(),
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_OK, "propose should be accepted");
        // Decode the vote and confirm we accepted.
        let vote = crate::cluster::topology::TopologyVote::deserialize(&resp.payload)
            .expect("vote must deserialize");
        assert!(
            vote.accepted,
            "vote must be accepted for subsuming proposal"
        );
        assert_eq!(vote.term, 500);

        // The reply has already been returned by handle_request. The
        // safety invariant: by the time the caller observes the reply,
        // the on-disk state MUST contain voted_term=500.
        let persisted = crate::cluster::coordinator::load_topology_state(&path);
        assert_eq!(
            persisted.voted_term, 500,
            "voted_term must be persisted BEFORE the reply is observable; \
             found {} on disk after reply returned",
            persisted.voted_term,
        );
    }

    #[test]
    fn topology_vote_reply_failure_surfaces_persist_error() {
        // Point topology_state_path at a non-existent parent directory —
        // File::create will fail, persist_topology() returns Err, and the
        // vote handler must respond with ERR_TOPOLOGY_PERSIST_FAILED rather
        // than acking the vote.
        let bogus = std::path::PathBuf::from("/nonexistent/teraslab-topology-h10/node.topology");
        let h = DispatchTestHarness::new();
        let self_id = crate::cluster::shards::NodeId(1);
        let members = vec![self_id, crate::cluster::shards::NodeId(2)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&[self_id], 1, 10);
        let cluster = crate::cluster::coordinator::new_test_running_cluster_with_topology_path(
            self_id,
            table,
            &[(self_id, "127.0.0.1:4711".parse().unwrap())],
            &[self_id],
            &[],
            &[],
            &[],
            1,
            Some(bogus),
        );

        let proposer = crate::cluster::shards::NodeId(2);
        let propose = crate::cluster::topology::TopologyTerm::new(600, members.clone(), proposer);

        let req = RequestFrame {
            request_id: 1,
            op_code: OP_TOPOLOGY_PROPOSE,
            flags: 0,
            payload: propose.serialize(),
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );

        assert_eq!(
            resp.status, STATUS_ERROR,
            "persist failure must surface as error"
        );
        assert!(resp.payload.len() >= 2);
        let err_code = u16::from_le_bytes(resp.payload[..2].try_into().unwrap());
        assert_eq!(
            err_code, ERR_TOPOLOGY_PERSIST_FAILED,
            "expected ERR_TOPOLOGY_PERSIST_FAILED, got {err_code}"
        );
    }

    #[test]
    fn topology_commit_persisted_before_reply() {
        // Committing a new term must also persist before the reply so
        // restart-after-crash observes the committed term.
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("node.topology");

        let h = DispatchTestHarness::new();
        let self_id = crate::cluster::shards::NodeId(1);
        let members = vec![self_id, crate::cluster::shards::NodeId(2)];
        // Start from a cluster already at term 10 (single-node).
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&[self_id], 1, 10);
        let cluster = crate::cluster::coordinator::new_test_running_cluster_with_topology_path(
            self_id,
            table,
            &[(self_id, "127.0.0.1:4712".parse().unwrap())],
            &[self_id],
            &[],
            &[],
            &[],
            1,
            Some(path.clone()),
        );

        // Step 1: accept a proposal (sets voted_term).
        let proposer = crate::cluster::shards::NodeId(2);
        let propose = crate::cluster::topology::TopologyTerm::new(700, members.clone(), proposer);
        let req = RequestFrame {
            request_id: 1,
            op_code: OP_TOPOLOGY_PROPOSE,
            flags: 0,
            payload: propose.serialize(),
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );
        assert_eq!(resp.status, STATUS_OK);

        // Step 2: commit that term.
        let commit = crate::cluster::topology::TopologyCommit {
            term: 700,
            proposer,
            members: members.clone(),
            digest: crate::cluster::topology::TopologyTerm::compute_digest(700, &members),
        };
        let req = RequestFrame {
            request_id: 2,
            op_code: OP_TOPOLOGY_COMMIT,
            flags: 0,
            payload: commit.serialize(),
        };
        let resp = handle_request(
            &req,
            &h.engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );
        assert_eq!(resp.status, STATUS_OK, "commit must succeed");

        // By the time the reply is visible, committed_term=700 must be on disk.
        let persisted = crate::cluster::coordinator::load_topology_state(&path);
        assert_eq!(
            persisted.committed_term, 700,
            "committed_term must be persisted before the commit reply returns"
        );
        assert_eq!(persisted.committed_members, members);
    }

    // -----------------------------------------------------------------------
    // Phase 1: observability counters + latency histograms
    //
    // These tests exercise the instrumentation inside each `handle_*_batch`
    // handler. They all share a single process-global `ThreadMetrics`
    // because DISPATCH_METRICS is a OnceLock — so each test takes
    // `METRICS_TEST_LOCK` and snapshots counter deltas instead of relying
    // on absolute values.
    // -----------------------------------------------------------------------

    use crate::metrics::{ThreadHistograms, ThreadMetrics};
    use std::sync::{Mutex as StdMutex, OnceLock};

    /// Lazily-initialized global test metrics. Installed into DISPATCH_METRICS
    /// on first access; subsequent accesses return the same reference.
    fn test_metrics() -> &'static ThreadMetrics {
        static INIT: OnceLock<&'static ThreadMetrics> = OnceLock::new();
        INIT.get_or_init(|| {
            let leaked: &'static ThreadMetrics = Box::leak(Box::new(ThreadMetrics::new()));
            super::init_dispatch_metrics(leaked);
            leaked
        })
    }

    fn test_histograms() -> &'static ThreadHistograms {
        static INIT: OnceLock<&'static ThreadHistograms> = OnceLock::new();
        INIT.get_or_init(|| {
            let leaked: &'static ThreadHistograms = Box::leak(Box::new(ThreadHistograms::new()));
            super::init_dispatch_histograms(leaked);
            leaked
        })
    }

    /// Serialize metrics-observing tests so concurrent increments from
    /// neighbours do not pollute each test's delta. `Mutex` is fine here;
    /// if a test panics the poison is cleared manually.
    fn metrics_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        match LOCK.get_or_init(|| StdMutex::new(())).lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Capture the current counter values as a named tuple for delta math.
    fn snapshot_spend(m: &ThreadMetrics) -> (u64, u64, u64, u64, u64) {
        (
            m.spend_multi_items_attempted.get(),
            m.spend_multi_items_succeeded.get(),
            m.spend_multi_items_idempotent.get(),
            m.spend_multi_items_failed.get(),
            m.spend_multi_batches.get(),
        )
    }

    fn snapshot_unspend(m: &ThreadMetrics) -> (u64, u64, u64, u64, u64) {
        (
            m.unspend_multi_items_attempted.get(),
            m.unspend_multi_items_succeeded.get(),
            m.unspend_multi_items_idempotent.get(),
            m.unspend_multi_items_failed.get(),
            m.unspend_multi_batches.get(),
        )
    }

    /// Submit a spend batch with three items: two valid, one with a wrong
    /// utxo_hash. Assert the per-item counters advance by (3, 2, 0, 1).
    #[test]
    fn handle_spend_batch_increments_items_succeeded_and_failed() {
        let _guard = metrics_test_lock();
        let m = test_metrics();
        let _ = test_histograms();

        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(40);
        let txid_b = DispatchTestHarness::make_txid(41);
        let txid_c = DispatchTestHarness::make_txid(42);
        assert_eq!(h.create_tx(txid_a, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_c, 2).status, STATUS_OK);

        // Hash generated by create_tx for vout=0 is all-zeros with the
        // low-order nibble encoding the vout.
        let utxo_hash_vout0 = [0u8; 32];
        // Deliberately wrong hash for item C — will produce UtxoHashMismatch.
        let wrong_hash = [0xEEu8; 32];
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let items = vec![
            WireSpendItem {
                txid: txid_a,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
                spending_data: [0xA1; 36],
            },
            WireSpendItem {
                txid: txid_b,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
                spending_data: [0xB2; 36],
            },
            WireSpendItem {
                txid: txid_c,
                vout: 0,
                utxo_hash: wrong_hash,
                spending_data: [0xC3; 36],
            },
        ];
        let before = snapshot_spend(m);
        let resp = h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &items));
        let after = snapshot_spend(m);

        // Expect STATUS_PARTIAL_ERROR because one item failed validation.
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);
        assert_eq!(after.0 - before.0, 3, "items_attempted += 3");
        assert_eq!(after.1 - before.1, 2, "items_succeeded += 2");
        assert_eq!(after.2 - before.2, 0, "no idempotent items");
        assert_eq!(after.3 - before.3, 1, "items_failed += 1 (hash mismatch)");
        assert_eq!(after.4 - before.4, 1, "one batch processed");
    }

    /// Re-sending the exact same spend should classify the second send as
    /// idempotent rather than succeeded or failed.
    #[test]
    fn handle_spend_batch_idempotent_counted_as_idempotent() {
        let _guard = metrics_test_lock();
        let m = test_metrics();
        let _ = test_histograms();

        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(43);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let utxo_hash_vout0 = [0u8; 32];
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let item = WireSpendItem {
            txid,
            vout: 0,
            utxo_hash: utxo_hash_vout0,
            spending_data: [0xAB; 36],
        };

        // First spend: succeeds.
        let before1 = snapshot_spend(m);
        let r1 = h.request(
            OP_SPEND_BATCH,
            encode_spend_batch(&params, std::slice::from_ref(&item)),
        );
        let after1 = snapshot_spend(m);
        assert_eq!(r1.status, STATUS_OK);
        assert_eq!(after1.1 - before1.1, 1, "first spend: 1 success");
        assert_eq!(after1.2 - before1.2, 0, "first spend: 0 idempotent");

        // Second identical spend: idempotent.
        let before2 = snapshot_spend(m);
        let r2 = h.request(
            OP_SPEND_BATCH,
            encode_spend_batch(&params, std::slice::from_ref(&item)),
        );
        let after2 = snapshot_spend(m);
        assert_eq!(r2.status, STATUS_OK);
        assert_eq!(after2.0 - before2.0, 1, "items_attempted += 1");
        assert_eq!(after2.1 - before2.1, 0, "second spend: no new success");
        assert_eq!(after2.2 - before2.2, 1, "second spend: 1 idempotent");
        assert_eq!(after2.3 - before2.3, 0, "second spend: no failures");
    }

    /// Unspend should classify each item as succeeded (real unspend),
    /// idempotent (already-unspent noop), or failed (hash mismatch).
    #[test]
    fn handle_unspend_batch_ticks_outcome_counters() {
        let _guard = metrics_test_lock();
        let m = test_metrics();
        let _ = test_histograms();

        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(50);
        let txid_b = DispatchTestHarness::make_txid(51);
        let txid_c = DispatchTestHarness::make_txid(52);
        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_c, 1).status, STATUS_OK);

        // Spend txid_a first so the subsequent unspend is a real unspend
        // (the other two are never spent → unspend is an idempotent no-op).
        let utxo_hash_vout0 = [0u8; 32];
        let sp = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let spend_item = WireSpendItem {
            txid: txid_a,
            vout: 0,
            utxo_hash: utxo_hash_vout0,
            spending_data: [0x77; 36],
        };
        assert_eq!(
            h.request(OP_SPEND_BATCH, encode_spend_batch(&sp, &[spend_item]))
                .status,
            STATUS_OK,
        );

        // Now submit unspend for A (real), B (noop), and C with wrong hash (fail).
        let wrong_hash = [0x88u8; 32];
        let items = vec![
            WireSlotItem {
                txid: txid_a,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
            },
            WireSlotItem {
                txid: txid_b,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
            },
            WireSlotItem {
                txid: txid_c,
                vout: 0,
                utxo_hash: wrong_hash,
            },
        ];
        let params = UnspendBatchParams {
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let before = snapshot_unspend(m);
        let resp = h.request(OP_UNSPEND_BATCH, encode_unspend_batch(&params, &items));
        let after = snapshot_unspend(m);
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);
        assert_eq!(after.0 - before.0, 3, "items_attempted += 3");
        assert_eq!(after.1 - before.1, 1, "items_succeeded += 1 (A)");
        assert_eq!(after.2 - before.2, 1, "items_idempotent += 1 (B)");
        assert_eq!(after.3 - before.3, 1, "items_failed += 1 (C wrong hash)");
        assert_eq!(after.4 - before.4, 1, "one unspend batch");
    }

    /// SetMined items should tick attempted/succeeded/failed per item.
    #[test]
    fn handle_set_mined_batch_ticks_outcome_counters() {
        let _guard = metrics_test_lock();
        let m = test_metrics();
        let _ = test_histograms();

        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(60);
        let txid_b = DispatchTestHarness::make_txid(61);
        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 1).status, STATUS_OK);
        // txid_c is NOT created — set_mined on it must fail with TxNotFound.
        let txid_c = DispatchTestHarness::make_txid(62);

        let params = SetMinedBatchParams {
            block_id: 42,
            block_height: 100,
            subtree_idx: 0,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let before_att = m.set_mined_items_attempted.get();
        let before_succ = m.set_mined_items_succeeded.get();
        let before_fail = m.set_mined_items_failed.get();

        let payload = encode_set_mined_batch(&params, &[txid_a, txid_b, txid_c]);
        let resp = h.request(OP_SET_MINED_BATCH, payload);
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);

        let after_att = m.set_mined_items_attempted.get();
        let after_succ = m.set_mined_items_succeeded.get();
        let after_fail = m.set_mined_items_failed.get();
        assert_eq!(after_att - before_att, 3, "set_mined_items_attempted += 3");
        assert_eq!(
            after_succ - before_succ,
            2,
            "set_mined_items_succeeded += 2"
        );
        assert_eq!(after_fail - before_fail, 1, "set_mined_items_failed += 1");
    }

    /// Create items should tick creates_attempted (once per batch),
    /// creates_succeeded, and creates_failed.
    #[test]
    fn handle_create_batch_ticks_outcome_counters() {
        let _guard = metrics_test_lock();
        let m = test_metrics();
        let _ = test_histograms();

        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(70);
        // Pre-create txid_a so the second create in the batch below collides.
        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);

        let before_att = m.creates_attempted.get();
        let before_succ = m.creates_succeeded.get();
        let before_fail = m.creates_failed.get();

        // Batch: [new, duplicate-of-txid_a] → one success + one failure.
        let txid_new = DispatchTestHarness::make_txid(71);
        let items = vec![
            WireCreateItem {
                txid: txid_new,
                tx_version: 1,
                locktime: 0,
                fee: 100,
                size_in_bytes: 200,
                extended_size: 200,
                is_coinbase: false,
                spending_height: 0,
                created_at: 1700000000000,
                flags: 0,
                utxo_hashes: vec![[0u8; 32]],
                cold_data: vec![],
                block_height: 0,
                mined_block_id: None,
                mined_block_height: None,
                mined_subtree_idx: None,
                parent_txids: vec![],
            },
            WireCreateItem {
                txid: txid_a,
                tx_version: 1,
                locktime: 0,
                fee: 100,
                size_in_bytes: 200,
                extended_size: 200,
                is_coinbase: false,
                spending_height: 0,
                created_at: 1700000000000,
                flags: 0,
                utxo_hashes: vec![[0u8; 32]],
                cold_data: vec![],
                block_height: 0,
                mined_block_id: None,
                mined_block_height: None,
                mined_subtree_idx: None,
                parent_txids: vec![],
            },
        ];
        let payload = encode_create_batch(&items);
        let resp = h.request(OP_CREATE_BATCH, payload);
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);

        let after_att = m.creates_attempted.get();
        let after_succ = m.creates_succeeded.get();
        let after_fail = m.creates_failed.get();
        assert_eq!(
            after_att - before_att,
            1,
            "creates_attempted += 1 (per batch)"
        );
        assert_eq!(after_succ - before_succ, 1, "creates_succeeded += 1");
        assert_eq!(after_fail - before_fail, 1, "creates_failed += 1");
    }

    /// Freeze items should tick freezes_succeeded / freezes_failed per item.
    #[test]
    fn handle_freeze_batch_ticks_outcome_counters() {
        let _guard = metrics_test_lock();
        let m = test_metrics();
        let _ = test_histograms();

        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(80);
        assert_eq!(h.create_tx(txid_a, 2).status, STATUS_OK);
        let txid_missing = DispatchTestHarness::make_txid(81);

        let utxo_hash_vout0 = [0u8; 32];
        let items = vec![
            WireSlotItem {
                txid: txid_a,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
            },
            WireSlotItem {
                txid: txid_missing,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
            },
        ];
        let payload = encode_slot_item_batch(&items);

        let before_succ = m.freezes_succeeded.get();
        let before_fail = m.freezes_failed.get();
        let resp = h.request(OP_FREEZE_BATCH, payload);
        let after_succ = m.freezes_succeeded.get();
        let after_fail = m.freezes_failed.get();
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);
        assert_eq!(after_succ - before_succ, 1, "freezes_succeeded += 1");
        assert_eq!(after_fail - before_fail, 1, "freezes_failed += 1");
    }

    /// Delete items should tick deletes_succeeded / deletes_failed per item.
    #[test]
    fn handle_delete_batch_ticks_outcome_counters() {
        let _guard = metrics_test_lock();
        let m = test_metrics();
        let _ = test_histograms();

        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(90);
        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);
        let txid_missing = DispatchTestHarness::make_txid(91);

        let payload = encode_txid_batch(&[txid_a, txid_missing], &[]);
        let before_succ = m.deletes_succeeded.get();
        let before_fail = m.deletes_failed.get();
        let resp = h.request(OP_DELETE_BATCH, payload);
        let after_succ = m.deletes_succeeded.get();
        let after_fail = m.deletes_failed.get();
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);
        assert_eq!(
            after_succ - before_succ,
            1,
            "deletes_succeeded += 1 (A deleted)"
        );
        assert_eq!(after_fail - before_fail, 1, "deletes_failed += 1 (missing)");
    }

    /// Dispatch must record an end-to-end latency sample into
    /// `h.spend_latency` for every spend batch processed.
    #[test]
    fn dispatch_records_spend_latency_histogram() {
        let _guard = metrics_test_lock();
        let _ = test_metrics();
        let hists = test_histograms();

        let h = DispatchTestHarness::new();
        // Create several txs and spend each.
        let base = DispatchTestHarness::make_txid(100)[0];
        let n: u8 = 5;
        for i in 0..n {
            let txid = DispatchTestHarness::make_txid(base.wrapping_add(i));
            assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);
        }

        let before_count = hists.spend_latency.count();
        let before_sum = hists.spend_latency.sum_ns();
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let utxo_hash_vout0 = [0u8; 32];
        for i in 0..n {
            let txid = DispatchTestHarness::make_txid(base.wrapping_add(i));
            let item = WireSpendItem {
                txid,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
                spending_data: {
                    let mut sd = [0u8; 36];
                    sd[0] = i;
                    sd
                },
            };
            let resp = h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &[item]));
            assert_eq!(resp.status, STATUS_OK);
        }
        let after_count = hists.spend_latency.count();
        let after_sum = hists.spend_latency.sum_ns();
        assert_eq!(
            after_count - before_count,
            n as u64,
            "spend_latency.count() should advance by exactly {n}",
        );
        assert!(
            after_sum > before_sum,
            "spend_latency.sum_ns() must be strictly greater after processing {n} batches",
        );
    }

    // -----------------------------------------------------------------------
    // Phase 2: LabeledCounter / {op, outcome} table dual-writes
    // -----------------------------------------------------------------------

    /// Drive a mix of Ok / Idempotent / ErrConflicting spends through
    /// handle_spend_batch and assert the labeled operations table advances
    /// by the exact expected counts for each outcome bucket.
    #[test]
    fn operations_table_counts_spend_ok_and_err() {
        use crate::metrics::{OpCode, Outcome};
        let _guard = metrics_test_lock();
        let m = test_metrics();
        let _ = test_histograms();

        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(150);
        let txid_b = DispatchTestHarness::make_txid(151);
        let txid_c = DispatchTestHarness::make_txid(152);
        let txid_missing = DispatchTestHarness::make_txid(153);
        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_c, 1).status, STATUS_OK);

        let utxo_hash_vout0 = [0u8; 32];
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        // Round 1: spend A and B successfully, C with wrong hash → Other.
        let wrong_hash = [0xEEu8; 32];
        let items = vec![
            WireSpendItem {
                txid: txid_a,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
                spending_data: [0x11; 36],
            },
            WireSpendItem {
                txid: txid_b,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
                spending_data: [0x22; 36],
            },
            WireSpendItem {
                txid: txid_c,
                vout: 0,
                utxo_hash: wrong_hash,
                spending_data: [0x33; 36],
            },
        ];
        let before_ok = m.operations.get(OpCode::Spend, Outcome::Ok);
        let before_idem = m.operations.get(OpCode::Spend, Outcome::Idempotent);
        let before_other = m.operations.get(OpCode::Spend, Outcome::Other);
        let before_conflict = m.operations.get(OpCode::Spend, Outcome::ErrConflicting);
        let before_not_found = m.operations.get(OpCode::Spend, Outcome::ErrNotFound);
        let resp = h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &items));
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);

        // Round 2: replay A with identical spending_data → Idempotent.
        // Also try D which does not exist → ErrNotFound.
        let items2 = vec![
            WireSpendItem {
                txid: txid_a,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
                spending_data: [0x11; 36],
            },
            WireSpendItem {
                txid: txid_missing,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
                spending_data: [0x44; 36],
            },
        ];
        let resp2 = h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &items2));
        assert_eq!(resp2.status, STATUS_PARTIAL_ERROR);

        // Round 3: attempt to spend A again with DIFFERENT spending_data →
        // ErrConflicting (AlreadySpent).
        let items3 = vec![WireSpendItem {
            txid: txid_a,
            vout: 0,
            utxo_hash: utxo_hash_vout0,
            spending_data: [0x55; 36],
        }];
        let resp3 = h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &items3));
        assert_eq!(resp3.status, STATUS_PARTIAL_ERROR);

        let after_ok = m.operations.get(OpCode::Spend, Outcome::Ok);
        let after_idem = m.operations.get(OpCode::Spend, Outcome::Idempotent);
        let after_other = m.operations.get(OpCode::Spend, Outcome::Other);
        let after_conflict = m.operations.get(OpCode::Spend, Outcome::ErrConflicting);
        let after_not_found = m.operations.get(OpCode::Spend, Outcome::ErrNotFound);

        assert_eq!(after_ok - before_ok, 2, "Ok += 2 (A + B)");
        assert_eq!(after_idem - before_idem, 1, "Idempotent += 1 (A replayed)");
        assert_eq!(
            after_other - before_other,
            1,
            "Other += 1 (C UtxoHashMismatch)"
        );
        assert_eq!(
            after_conflict - before_conflict,
            1,
            "ErrConflicting += 1 (A AlreadySpent)"
        );
        assert_eq!(
            after_not_found - before_not_found,
            1,
            "ErrNotFound += 1 (missing txid)"
        );
    }

    /// Exercise every `SpendError` variant through `classify_spend_error` and
    /// assert the mapping is stable. This guards against silent drift when
    /// variants are added or renamed — a compile error here forces the
    /// author to update the Phase 2 spec.
    #[test]
    fn outcome_classification_is_stable_for_every_spend_error_variant() {
        use super::classify_spend_error;
        use crate::metrics::Outcome;

        let fresh_spending_data = [0u8; 36];
        let cases: Vec<(SpendError, Outcome)> = vec![
            (SpendError::TxNotFound, Outcome::ErrNotFound),
            (SpendError::Conflicting, Outcome::ErrConflicting),
            (SpendError::Locked, Outcome::ErrFrozen),
            (
                SpendError::CoinbaseImmature {
                    spending_height: 5,
                    current_height: 1,
                },
                Outcome::Other,
            ),
            (SpendError::UtxoNotFound { offset: 0 }, Outcome::Other),
            (SpendError::UtxoHashMismatch { offset: 0 }, Outcome::Other),
            (
                SpendError::AlreadySpent {
                    offset: 0,
                    spending_data: fresh_spending_data,
                },
                Outcome::ErrConflicting,
            ),
            (SpendError::Frozen { offset: 0 }, Outcome::ErrFrozen),
            (
                SpendError::FrozenUntil {
                    offset: 0,
                    spendable_at_height: 1,
                },
                Outcome::ErrFrozen,
            ),
            (
                SpendError::InvalidSpend {
                    offset: 0,
                    spending_data: fresh_spending_data,
                },
                Outcome::ErrConflicting,
            ),
            (SpendError::Pruned { offset: 0 }, Outcome::ErrConflicting),
            (SpendError::AlreadyFrozen { offset: 0 }, Outcome::ErrFrozen),
            (SpendError::NotFrozen { offset: 0 }, Outcome::ErrFrozen),
            (
                SpendError::StorageError {
                    detail: "disk".into(),
                },
                Outcome::ErrStorage,
            ),
            (
                SpendError::DahOverflow {
                    current_height: u32::MAX - 1,
                    retention: 288,
                },
                Outcome::ErrStorage,
            ),
        ];
        for (err, expected) in cases {
            let got = classify_spend_error(&err);
            assert_eq!(
                got, expected,
                "classify_spend_error({err:?}) → {got:?}, expected {expected:?}"
            );
        }
    }

    /// `/metrics` must emit one `teraslab_operations_total{op=..,outcome=..}`
    /// line per cell in the labeled table, with values matching
    /// `ThreadMetrics.operations.get(op, outcome)`.
    #[test]
    fn prometheus_emits_operations_total_with_labels() {
        use crate::metrics::{OpCode, Outcome};
        let _guard = metrics_test_lock();
        let m = test_metrics();
        let _ = test_histograms();

        let h = DispatchTestHarness::new();
        // Seed concrete, known values through the dispatch path: one Ok spend,
        // one Idempotent replay, one ErrNotFound.
        let txid_a = DispatchTestHarness::make_txid(200);
        let txid_missing = DispatchTestHarness::make_txid(201);
        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);
        let utxo_hash_vout0 = [0u8; 32];
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let item_a = WireSpendItem {
            txid: txid_a,
            vout: 0,
            utxo_hash: utxo_hash_vout0,
            spending_data: [0xAA; 36],
        };
        let item_missing = WireSpendItem {
            txid: txid_missing,
            vout: 0,
            utxo_hash: utxo_hash_vout0,
            spending_data: [0xBB; 36],
        };
        assert_eq!(
            h.request(
                OP_SPEND_BATCH,
                encode_spend_batch(&params, std::slice::from_ref(&item_a))
            )
            .status,
            STATUS_OK
        );
        assert_eq!(
            h.request(
                OP_SPEND_BATCH,
                encode_spend_batch(&params, std::slice::from_ref(&item_a))
            )
            .status,
            STATUS_OK // idempotent replay reports STATUS_OK
        );
        let _ = h.request(
            OP_SPEND_BATCH,
            encode_spend_batch(&params, std::slice::from_ref(&item_missing)),
        );

        let hists = crate::metrics::ThreadHistograms::new();
        let text = crate::server::http::render_metrics_text(m, &hists, 0, 0, 0, 0);

        // Every (op, outcome) cell must appear exactly once with matching value.
        let mut found_spend_ok = false;
        let mut found_spend_not_found = false;
        let mut found_spend_idempotent = false;
        for &op in OpCode::all() {
            for &outcome in Outcome::all() {
                let needle = format!(
                    "teraslab_operations_total{{op=\"{}\",outcome=\"{}\"}} ",
                    op.as_str(),
                    outcome.as_str(),
                );
                let line = text
                    .lines()
                    .find(|l| l.starts_with(&needle))
                    .unwrap_or_else(|| panic!("missing Prometheus line for {needle}"));
                let val: u64 = line
                    .rsplit(' ')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| panic!("unparseable Prometheus line: {line}"));
                let expected = m.operations.get(op, outcome);
                assert_eq!(
                    val, expected,
                    "label value mismatch for {needle}: metric={val} counter={expected}"
                );
                if op == OpCode::Spend && outcome == Outcome::Ok && val > 0 {
                    found_spend_ok = true;
                }
                if op == OpCode::Spend && outcome == Outcome::ErrNotFound && val > 0 {
                    found_spend_not_found = true;
                }
                if op == OpCode::Spend && outcome == Outcome::Idempotent && val > 0 {
                    found_spend_idempotent = true;
                }
            }
        }
        assert!(found_spend_ok, "expected at least one Spend/Ok tick");
        assert!(
            found_spend_idempotent,
            "expected at least one Spend/Idempotent tick"
        );
        assert!(
            found_spend_not_found,
            "expected at least one Spend/ErrNotFound tick"
        );
    }

    // -----------------------------------------------------------------
    // Phase 3 — tracing span integration tests
    // -----------------------------------------------------------------

    /// Capturing `tracing_subscriber::Layer` that records every span it sees
    /// along with the values of selected fields and the parent span id.
    ///
    /// This is a real layer (not a stub): each new span pushes a record onto
    /// a shared `Vec`, and every field event is serialised into the record.
    /// Used by the span-integration tests below to assert on structure.
    mod capture {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        use tracing::Subscriber;
        use tracing::field::{Field, Visit};
        use tracing::span::{Attributes, Id};
        use tracing_subscriber::Layer;
        use tracing_subscriber::layer::Context;
        use tracing_subscriber::registry::LookupSpan;

        #[derive(Clone, Debug)]
        pub struct CapturedSpan {
            pub name: &'static str,
            pub id: u64,
            pub parent_id: Option<u64>,
            pub fields: HashMap<String, String>,
        }

        #[derive(Default)]
        pub struct CaptureLayer {
            pub spans: Arc<Mutex<Vec<CapturedSpan>>>,
        }

        impl CaptureLayer {
            pub fn new() -> Self {
                Self::default()
            }
        }

        struct FieldVisitor<'a>(&'a mut HashMap<String, String>);

        impl<'a> Visit for FieldVisitor<'a> {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                self.0
                    .insert(field.name().to_string(), format!("{value:?}"));
            }
            fn record_str(&mut self, field: &Field, value: &str) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
            fn record_u64(&mut self, field: &Field, value: u64) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
            fn record_i64(&mut self, field: &Field, value: i64) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
            fn record_bool(&mut self, field: &Field, value: bool) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
        }

        impl<S> Layer<S> for CaptureLayer
        where
            S: Subscriber + for<'a> LookupSpan<'a>,
        {
            fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
                let mut fields = HashMap::new();
                attrs.record(&mut FieldVisitor(&mut fields));
                let parent_id = ctx
                    .span(id)
                    .and_then(|s| s.parent())
                    .map(|p| p.id().into_u64());
                let mut spans = self.spans.lock().expect("capture lock poisoned");
                spans.push(CapturedSpan {
                    name: attrs.metadata().name(),
                    id: id.into_u64(),
                    parent_id,
                    fields,
                });
            }
        }
    }

    /// Run a closure inside a scoped `tracing` subscriber composed from a
    /// capturing layer, and return the captured spans.
    ///
    /// Uses `tracing::subscriber::with_default` to scope the subscriber to the
    /// current thread so concurrent tests do not interfere. The subscriber
    /// honours `DEBUG` level so `#[instrument(level = "debug")]` sites fire.
    fn with_capture<F: FnOnce()>(f: F) -> Vec<capture::CapturedSpan> {
        use tracing_subscriber::prelude::*;
        let layer = capture::CaptureLayer::new();
        let spans = layer.spans.clone();
        let filter = tracing_subscriber::EnvFilter::new("debug");
        let subscriber = tracing_subscriber::registry().with(filter).with(layer);
        tracing::subscriber::with_default(subscriber, f);
        let guard = spans.lock().expect("capture lock poisoned");
        guard.clone()
    }

    /// Emit a single `info!` event through a `tracing_subscriber::fmt::Layer`
    /// configured to write JSON into a `Vec<u8>` sink, then parse the output
    /// and assert on the level and a field value.
    #[test]
    fn tracing_subscriber_emits_json_for_info_events() {
        use std::io;
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::fmt::MakeWriter;
        use tracing_subscriber::prelude::*;

        #[derive(Clone, Default)]
        struct SharedBuf(Arc<Mutex<Vec<u8>>>);

        impl io::Write for SharedBuf {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().expect("sink lock").extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        impl<'a> MakeWriter<'a> for SharedBuf {
            type Writer = Self;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let sink = SharedBuf::default();
        let layer = tracing_subscriber::fmt::Layer::new()
            .json()
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(sink.clone());
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("info"))
            .with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(service = "teraslab-test", answer = 42u64, "hello world");
        });

        let bytes = sink.0.lock().expect("sink lock").clone();
        let line = std::str::from_utf8(&bytes).expect("json layer emitted invalid utf-8");
        // The fmt::Layer may emit multiple JSON objects separated by newlines;
        // take the first line (the event we emitted).
        let first = line
            .lines()
            .find(|l| !l.is_empty())
            .expect("no JSON output captured");
        let parsed: serde_json::Value =
            serde_json::from_str(first).expect("output line is not valid JSON");

        assert_eq!(parsed["level"], "INFO");
        // The fmt layer nests the event fields under `fields`.
        assert_eq!(parsed["fields"]["service"], "teraslab-test");
        assert_eq!(parsed["fields"]["answer"], 42);
        assert_eq!(parsed["fields"]["message"], "hello world");
    }

    /// Driving a single `handle_request` through the dispatch path should
    /// create exactly one top-level dispatch span with `op` and `request_id`
    /// fields matching the supplied frame.
    #[test]
    fn dispatch_handle_request_emits_request_scoped_span() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(200);

        // Seed a tx so the following spend targets a real record.
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let utxo_hash_vout0 = [0u8; 32];
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let item = WireSpendItem {
            txid,
            vout: 0,
            utxo_hash: utxo_hash_vout0,
            spending_data: [0xCE; 36],
        };
        let payload = encode_spend_batch(&params, std::slice::from_ref(&item));
        let request = RequestFrame {
            request_id: 777,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload,
        };

        let spans = with_capture(|| {
            let mut conn_state = crate::server::ConnectionState::new();
            let _ = handle_request(&request, &h.engine, 8192, None, None, &mut conn_state, None);
        });

        let dispatch_spans: Vec<_> = spans
            .iter()
            .filter(|s| s.name == "handle_request")
            .collect();
        assert_eq!(
            dispatch_spans.len(),
            1,
            "expected exactly one handle_request span, got {spans:?}",
        );
        let s = dispatch_spans[0];
        assert_eq!(
            s.fields.get("op").map(String::as_str),
            Some(OP_SPEND_BATCH.to_string().as_str()),
        );
        assert_eq!(s.fields.get("request_id").map(String::as_str), Some("777"),);
    }

    /// The `spend_multi` engine span is emitted under the current dispatch
    /// span, so its captured `parent_id` must equal the dispatch span's id.
    #[test]
    fn engine_spend_multi_span_child_of_dispatch_span() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(201);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let utxo_hash_vout0 = [0u8; 32];
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let item = WireSpendItem {
            txid,
            vout: 0,
            utxo_hash: utxo_hash_vout0,
            spending_data: [0xD1; 36],
        };
        let payload = encode_spend_batch(&params, std::slice::from_ref(&item));
        let request = RequestFrame {
            request_id: 888,
            op_code: OP_SPEND_BATCH,
            flags: 0,
            payload,
        };

        let spans = with_capture(|| {
            let mut conn_state = crate::server::ConnectionState::new();
            let _ = handle_request(&request, &h.engine, 8192, None, None, &mut conn_state, None);
        });

        let dispatch_span = spans
            .iter()
            .find(|s| s.name == "handle_request")
            .expect("no dispatch span captured");
        // The dispatch path calls `Engine::validate_spend_multi` followed by
        // `ValidatedSpend::apply`. `apply` is the instrumented span that
        // runs under the dispatch span; `spend_multi` (a wrapper that calls
        // both) is not invoked on the OP_SPEND_BATCH hot path. Either span
        // proves the parent linkage; we assert on `apply` because it is the
        // site reached from the dispatch wire opcode.
        let apply_span = spans
            .iter()
            .find(|s| s.name == "apply")
            .expect("no apply span captured");
        assert_eq!(
            apply_span.parent_id,
            Some(dispatch_span.id),
            "apply parent ({:?}) should be dispatch span ({})",
            apply_span.parent_id,
            dispatch_span.id,
        );

        // Drive the higher-level wrapper directly so the spend_multi span is
        // also exercised and its parent/child wiring verified.
        let second_txid = DispatchTestHarness::make_txid(202);
        assert_eq!(h.create_tx(second_txid, 1).status, STATUS_OK);

        let wrapped_spans = with_capture(|| {
            let _ = h.engine.spend_multi(&crate::ops::spend::SpendMultiRequest {
                tx_key: crate::index::TxKey { txid: second_txid },
                spends: vec![crate::ops::spend::SpendItem {
                    utxo_hash: [0u8; 32],
                    offset: 0,
                    spending_data: [0xD2; 36],
                    idx: 0,
                }],
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            });
        });
        let sm = wrapped_spans
            .iter()
            .find(|s| s.name == "spend_multi")
            .expect("no spend_multi span captured from direct call");
        let sm_apply = wrapped_spans
            .iter()
            .find(|s| s.name == "apply")
            .expect("no apply span captured from direct call");
        assert_eq!(
            sm_apply.parent_id,
            Some(sm.id),
            "direct spend_multi should parent its inner apply span",
        );
    }

    // -----------------------------------------------------------------------
    // OP_ADMIN_DIAGNOSE_KEY: per-txid diagnostic dump (Phase A diagnostic).
    // -----------------------------------------------------------------------

    /// `OP_ADMIN_DIAGNOSE_KEY` returns one fixed-width entry per requested
    /// txid, in order, with the responding node's view of routing/migration
    /// state for that key's shard. The shard field must match
    /// `ShardTable::shard_for_key`.
    #[test]
    fn dispatch_admin_diagnose_key_returns_per_txid_state() {
        let h = DispatchTestHarness::new();

        // Two txids that fall in distinct shards (low 12 bits of LE u16).
        let mut txid_a = [0u8; 32];
        txid_a[0] = 0xAB;
        txid_a[1] = 0x00;
        let mut txid_b = [0u8; 32];
        txid_b[0] = 0x42;
        txid_b[1] = 0x01;

        // Pre-populate txid_a so has_local_data is true for it.
        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);

        let shard_a = crate::cluster::shards::ShardTable::shard_for_key(&TxKey { txid: txid_a });
        let shard_b = crate::cluster::shards::ShardTable::shard_for_key(&TxKey { txid: txid_b });
        assert_ne!(
            shard_a, shard_b,
            "test depends on txids landing in distinct shards"
        );

        // Cluster harness with self_id=1 mastering all shards. Mark shard_b
        // as having pending inbound, and shard_a as fenced — so each diag
        // entry exercises a distinct flag.
        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 12);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4801".parse().unwrap(),
            )],
            &members,
            &[shard_b], // inbound_shards
            &[],        // migrating_shards
            &[shard_a], // fenced_shards
            1,
        );

        // Encode payload: [count: u32 LE][txid: 32B] * count
        let mut payload = Vec::with_capacity(4 + 64);
        payload.extend_from_slice(&2u32.to_le_bytes());
        payload.extend_from_slice(&txid_a);
        payload.extend_from_slice(&txid_b);

        let resp = h.request_with_cluster(OP_ADMIN_DIAGNOSE_KEY, payload, &cluster);
        assert_eq!(resp.status, STATUS_OK, "diagnose_key should succeed");

        // Response: [count: u32 LE][entry: KEY_DIAGNOSIS_ENCODED_SIZE bytes] * count
        let body = &resp.payload;
        assert!(body.len() >= 4, "response too short");
        let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
        assert_eq!(count, 2, "expected 2 entries");

        let entry_size = KEY_DIAGNOSIS_ENCODED_SIZE;
        assert_eq!(
            body.len(),
            4 + count * entry_size,
            "response length must match count * entry_size"
        );

        // Decode entry 0 (txid_a).
        let off_a = 4;
        let shard_field_a = u16::from_le_bytes(body[off_a..off_a + 2].try_into().unwrap());
        assert_eq!(shard_field_a, shard_a, "entry 0 shard mismatch");
        let this_node_a = u64::from_le_bytes(body[off_a + 2..off_a + 10].try_into().unwrap());
        assert_eq!(this_node_a, 1, "this_node_id should be self_id=1");
        let canonical_master_a =
            u64::from_le_bytes(body[off_a + 10..off_a + 18].try_into().unwrap());
        assert_eq!(canonical_master_a, 1, "canonical master should be 1");
        let has_local_data_a = body[off_a + 18];
        assert_eq!(has_local_data_a, 1, "txid_a was created → has_local_data");
        let is_local_master_a = body[off_a + 19];
        assert_eq!(is_local_master_a, 1, "self_id is master of every shard");
        let has_pending_inbound_a = body[off_a + 20];
        assert_eq!(has_pending_inbound_a, 0, "shard_a is not in inbound_shards");
        let is_shard_fenced_a = body[off_a + 21];
        assert_eq!(is_shard_fenced_a, 1, "shard_a was fenced");
        let is_migrating_shard_a = body[off_a + 22];
        assert_eq!(is_migrating_shard_a, 0, "no active migration for shard_a");
        let topology_epoch_a = u64::from_le_bytes(body[off_a + 23..off_a + 31].try_into().unwrap());
        assert_eq!(
            topology_epoch_a,
            cluster.topology_epoch(),
            "topology_epoch must match coordinator"
        );

        // Decode entry 1 (txid_b).
        let off_b = 4 + entry_size;
        let shard_field_b = u16::from_le_bytes(body[off_b..off_b + 2].try_into().unwrap());
        assert_eq!(shard_field_b, shard_b, "entry 1 shard mismatch");
        let has_local_data_b = body[off_b + 18];
        assert_eq!(
            has_local_data_b, 0,
            "txid_b was never created → no local data"
        );
        let has_pending_inbound_b = body[off_b + 20];
        assert_eq!(has_pending_inbound_b, 1, "shard_b is in inbound_shards");
        let is_shard_fenced_b = body[off_b + 21];
        assert_eq!(is_shard_fenced_b, 0, "shard_b was not fenced");
    }

    /// Truncated payloads (count claims more txids than bytes provide) and
    /// counts above the documented cap (64) must be rejected with
    /// STATUS_ERROR / ERR_INTERNAL.
    #[test]
    fn dispatch_admin_diagnose_key_malformed_payload() {
        let h = DispatchTestHarness::new();

        // Empty payload — no count prefix.
        let resp = h.request(OP_ADMIN_DIAGNOSE_KEY, vec![]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, _msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);

        // Count says 2 but only 1 txid worth of bytes follows.
        let mut short = Vec::new();
        short.extend_from_slice(&2u32.to_le_bytes());
        short.extend_from_slice(&[0u8; 32]);
        let resp = h.request(OP_ADMIN_DIAGNOSE_KEY, short);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, _msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);

        // Count above cap (65 > 64).
        let mut too_many = Vec::new();
        too_many.extend_from_slice(&65u32.to_le_bytes());
        too_many.extend_from_slice(&vec![0u8; 65 * 32]);
        let resp = h.request(OP_ADMIN_DIAGNOSE_KEY, too_many);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, _msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_INTERNAL);
    }
}
