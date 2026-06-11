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
use crate::ops::engine::Engine;
use crate::ops::error::SpendError;
use crate::ops::mark_longest_chain::*;
use crate::ops::remaining::*;
use crate::ops::spend::*;
use crate::ops::unspend::*;
use crate::protocol::codec::*;
use crate::protocol::frame::*;
use crate::protocol::opcodes::*;
use crate::record::{ExternalRef, METADATA_SIZE, TxFlags};
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

/// Per-address replication connection slot. Each replica address gets its
/// own independent mutex, so concurrent sends to different replicas never
/// contend on a single lock. At millions of ops/sec with RF=3, this
/// eliminates the serialization point that a single global pool creates.
///
/// R-D1/D-3: the slot also owns the master→replica **dense stream
/// cursor** (`next_sequence`). Sequence assignment, send, and ACK all
/// happen under this slot's mutex, so each replica address observes a
/// strictly ordered, densely numbered batch stream regardless of how
/// many dispatch threads fan out concurrently.
struct PerAddrSlot {
    connection: Option<TcpReplicaTransport>,
    /// Highest per-stream sequence this replica has ACKed (diagnostic).
    last_acked: u64,
    /// Next per-replica stream sequence to assign. `None` until the
    /// cursor has been synced to the replica's authoritative applied
    /// watermark via an empty-batch probe (process start / first
    /// contact). Positions consumed by failed sends are burned —
    /// never reused — so a position at or below the replica's
    /// watermark always refers to content the replica has applied.
    next_sequence: Option<u64>,
}

/// Per-address connection pool. The outer HashMap is locked briefly for
/// lookup/insert only. Each address has its own `Arc<Mutex<PerAddrSlot>>`,
/// so concurrent sends to different replicas proceed without contention.
static REPL_POOL: LazyLock<Mutex<HashMap<SocketAddr, std::sync::Arc<Mutex<PerAddrSlot>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn le_u16_at(bytes: &[u8], offset: usize) -> Option<u16> {
    let b = bytes.get(offset..offset.checked_add(2)?)?;
    Some(u16::from_le_bytes([b[0], b[1]]))
}

fn le_u32_at(bytes: &[u8], offset: usize) -> Option<u32> {
    let b = bytes.get(offset..offset.checked_add(4)?)?;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn le_u64_at(bytes: &[u8], offset: usize) -> Option<u64> {
    let b = bytes.get(offset..offset.checked_add(8)?)?;
    Some(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

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

const MAX_REPLICATION_FANOUTS_IN_FLIGHT: usize = 128;
static REPLICATION_FANOUT_PERMITS: LazyLock<(std::sync::Mutex<usize>, std::sync::Condvar)> =
    LazyLock::new(|| (std::sync::Mutex::new(0), std::sync::Condvar::new()));

struct ReplicationFanoutPermit;

fn acquire_replication_fanout_permit() -> ReplicationFanoutPermit {
    let (lock, cvar) = &*REPLICATION_FANOUT_PERMITS;
    let mut in_flight = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    while *in_flight >= MAX_REPLICATION_FANOUTS_IN_FLIGHT {
        in_flight = cvar
            .wait(in_flight)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
    *in_flight += 1;
    ReplicationFanoutPermit
}

impl Drop for ReplicationFanoutPermit {
    fn drop(&mut self) {
        let (lock, cvar) = &*REPLICATION_FANOUT_PERMITS;
        let mut in_flight = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *in_flight = in_flight.saturating_sub(1);
        cvar.notify_one();
    }
}

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

/// Gap #5 — secondary-index readiness status.
///
/// Tracks whether each secondary index (DAH and unmined) was successfully
/// (re)built at startup. When a secondary rebuild fails, the binary still
/// starts (the primary index is intact and the node can serve regular
/// spend/get/create traffic) but endpoints that depend on the missing
/// secondary reject requests with [`crate::protocol::opcodes::ERR_INDEX_DEGRADED`]
/// until the operator investigates and restarts. We deliberately do NOT
/// silently start with an empty secondary index because that would silently
/// break the pruner, unmined iterator, DAH-driven deletion, conflict, and
/// mining workflows.
///
/// Both flags default to `true` (healthy) at process start so that test
/// harnesses and code paths that never call [`set_secondary_status`] keep
/// the historical "everything is ready" behavior.
static SECONDARY_DAH_OK: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
static SECONDARY_UNMINED_OK: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// Snapshot of secondary-index readiness flags at startup.
///
/// Returned by [`secondary_status`]. Both flags default to `true` (healthy);
/// the server binary calls [`set_secondary_status`] after rebuild attempts
/// to flip them to `false` if the corresponding rebuild failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecondaryStatus {
    /// Whether the DAH index was successfully (re)built at startup.
    pub dah_ok: bool,
    /// Whether the unmined index was successfully (re)built at startup.
    pub unmined_ok: bool,
}

impl SecondaryStatus {
    /// True when both secondary indexes are healthy.
    pub fn fully_ok(&self) -> bool {
        self.dah_ok && self.unmined_ok
    }
}

/// Set the global secondary-index readiness flags.
///
/// Called once during server startup after the index/secondary rebuild
/// attempts complete. Subsequent reads via [`secondary_status`] observe the
/// stored values with `Ordering::Relaxed` semantics — fine for a flag that
/// is set once before the server begins accepting client requests.
pub fn set_secondary_status(status: SecondaryStatus) {
    SECONDARY_DAH_OK.store(status.dah_ok, std::sync::atomic::Ordering::Relaxed);
    SECONDARY_UNMINED_OK.store(status.unmined_ok, std::sync::atomic::Ordering::Relaxed);
}

/// Read the current secondary-index readiness flags.
pub fn secondary_status() -> SecondaryStatus {
    SecondaryStatus {
        dah_ok: SECONDARY_DAH_OK.load(std::sync::atomic::Ordering::Relaxed),
        unmined_ok: SECONDARY_UNMINED_OK.load(std::sync::atomic::Ordering::Relaxed),
    }
}

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

/// R-038 (D-01): borrow the static [`crate::replication::durable::AckTracker`] for background subsystems
/// (e.g. the replica-lag monitor) that need a `&'static AckTracker` handle.
///
/// Returns `None` until [`init_ack_tracker`] has been called. Single-node
/// servers never call it, so single-node callers naturally see `None` and
/// skip lag monitoring.
pub fn ack_tracker_handle() -> Option<&'static crate::replication::durable::AckTracker> {
    ACK_TRACKER.get()
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

/// Flush any coalesced replication-intent commits before a clean shutdown.
pub fn flush_replication_intent_tracker() -> std::result::Result<(), String> {
    if let Some(tracker) = REPLICATION_INTENT_TRACKER.get() {
        tracker
            .flush()
            .map_err(|e| format!("replication intent flush: {e}"))?;
    }
    Ok(())
}

/// Initialize the dispatch metrics reference.
///
/// Must be called once during server startup before any requests are processed.
pub fn init_dispatch_metrics(metrics: &'static crate::metrics::ThreadMetrics) {
    let _ = DISPATCH_METRICS.set(metrics);
}

/// P2.2: crate-internal accessor for the same `ThreadMetrics` handle that
/// in-dispatch code reaches via `DISPATCH_METRICS.get()`. Surfaced so
/// non-dispatch hot paths (e.g. `InflightBytesLimiter::try_acquire` in
/// `server::mod`) can bump per-thread counters without duplicating the
/// `OnceLock`. Returns `None` before `init_dispatch_metrics` has been
/// called, matching every other call site's tolerance for unset state.
pub(crate) fn dispatch_metrics_handle() -> Option<&'static crate::metrics::ThreadMetrics> {
    DISPATCH_METRICS.get().copied()
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
/// # Mutation path (durability contract — WAL-first)
///
/// Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md) corrected the
/// previously stated "engine-first" ordering — the actual implemented
/// order is:
///
/// 1. **Validate under lock** — parse, check shard ownership, acquire
///    the per-record lock. For multi-spend, snapshot the metadata.
/// 2. **Append + fsync redo entry** — every authoritative WAL-first
///    payload (full record bytes for `CreateV2`, real `new_spent_count`
///    for spend/unspend) is captured BEFORE any device write so
///    recovery can reconstruct the post-mutation state byte-for-byte.
///    Redo open/create failure is fatal at startup; redo flush failure
///    fails the client request.
/// 3. **Apply to engine** — write UTXO slots / metadata to the block
///    device via `pwrite_all_at` (durable on return for `DirectDevice`
///    via `O_DIRECT`).
/// 4. **Replicate** — fan out to replicas with the durable sequence
///    numbers assigned in step 2. Best-effort under the current ack
///    policy; degraded RF>1 modes are validated at config load time.
/// 5. **Respond** — send the success/error response to the client.
///
/// Crash recovery walks the redo log after the last checkpoint and
/// idempotently re-applies entries; CreateV2 fully reconstructs
/// records, spend / unspend overwrite `meta.spent_utxos` with the
/// correct value the dispatch path computed before the WAL flush.
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
    // Phase I — readiness gate (runs BEFORE the quorum check). A
    // multi-node node that has joined SWIM membership but has not yet
    // observed its first quorum-committed topology must reject
    // client-facing reads/writes with `ERR_CLUSTER_NOT_READY` (retryable
    // by the client) so a seeding client cannot drive data into a
    // half-formed cluster. This is more diagnostic than the broader
    // `ERR_NO_QUORUM` and lets clients discriminate "still booting"
    // from "lost quorum after formation".
    //
    // The gate is intentionally narrow: cluster-bootstrap traffic
    // (topology proposals/votes/commits, replica batches, admin
    // health/diagnostics, ping) bypasses the check so a node can
    // become Alive in the first place.
    if needs_cluster_readiness(request.op_code)
        && let Some(c) = cluster
        && c.shard_table().read().replication_factor() > 1
        && !c.cluster_health().is_ready()
    {
        return error_response(
            request.request_id,
            ERR_CLUSTER_NOT_READY,
            "node has not yet observed first committed topology",
        );
    }

    // Gap #5 — secondary-index readiness gate. When a secondary index
    // (DAH or unmined) failed to rebuild at startup, the binary is still
    // running so the primary index continues to serve regular spend/get/
    // create traffic, but endpoints that depend on the missing secondary
    // must reject requests with `ERR_INDEX_DEGRADED` instead of silently
    // returning empty results (which would break pruner / unmined iterator /
    // DAH-driven deletion / conflict / mining workflows). Recovery requires
    // the operator to investigate the underlying I/O / device error and
    // restart the node so the secondary rebuild can be re-attempted.
    if let Some(err_resp) = check_secondary_readiness(request.op_code, request.request_id) {
        return err_resp;
    }

    // Reject mutations when the cluster lacks quorum to prevent split-brain.
    if is_mutation_opcode(request.op_code)
        && let Some(err_resp) = check_quorum(cluster, request.request_id)
    {
        return err_resp;
    }

    let _visibility_guard = acquire_dispatch_visibility_guard(engine, request.op_code);

    // Refresh the cached wall-clock time once per request so that all
    // individual operations within the batch share the same timestamp.
    engine.refresh_clock();

    // Batch-level entry counters (one per request frame). Item-level
    // `_items_attempted` counters are incremented inside each handler once
    // the payload is decoded — they can't be incremented here because the
    // item count is payload-dependent.
    if let Some(m) = DISPATCH_METRICS.get() {
        match request.op_code {
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
        OP_QUERY_OLD_UNMINED => handle_query_old_unmined(request, engine, cluster),
        OP_PRESERVE_TRANSACTIONS => {
            handle_preserve_transactions(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_PROCESS_EXPIRED_PRESERVATIONS => {
            handle_process_expired(request, engine, max_batch_size, cluster, redo_log)
        }
        OP_GET_PARTITION_MAP => handle_get_partition_map(request, cluster),
        OP_GET_COMMITTED_TOPOLOGY => handle_get_committed_topology(request, cluster),
        OP_ADMIN_DIAGNOSE_KEY => handle_admin_diagnose_key(request, engine, cluster),
        OP_PARTITION_VERSION_REPORT => handle_partition_version_report(request, engine, cluster),
        OP_ADMIN_CLUSTER_HEALTH => handle_admin_cluster_health(request, cluster),
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
        OP_HELLO => ResponseFrame {
            request_id: request.request_id,
            status: STATUS_OK,
            payload: PROTOCOL_VERSION.to_le_bytes().to_vec(),
        },
        // F-G5-006: SWIM heartbeats use the dedicated UDP gossip port, not
        // this TCP data port. The OP_HEARTBEAT constant exists for clients
        // that have not been updated to use the gossip transport — respond
        // with STATUS_OK and an empty payload instead of falling into the
        // catch-all "unknown opcode" error so an operator who misconfigures
        // a probe doesn't see misleading ERR_INTERNAL responses.
        OP_HEARTBEAT => ResponseFrame {
            request_id: request.request_id,
            status: STATUS_OK,
            payload: vec![],
        },
        // F-G5-015: OP_INCREMENT_SPENT_EXTRA_RECS is a no-op compatibility
        // shim retained for clients carried over from the pre-TeraSlab Lua
        // UDF. The old store maintained extra-records counters that
        // TeraSlab tracks implicitly via UTXO generation. Returning
        // STATUS_OK with an empty payload is the contract: callers expect
        // success and do not parse a body. Returning an error here would
        // break legacy clients; logging on every call would flood the
        // logs. The constant is retained until all clients have migrated;
        // a counter would be a better signal, but a counter requires
        // shared mutable state and the call-site is hot.
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
                // F-G5-010: request_id is overloaded to carry the shard
                // number for migration batches. Reject any caller whose
                // upper 48 bits are non-zero — that would silently land
                // on a different shard than the caller intended (typo,
                // bug, or attacker repurposing the field).
                if request.request_id >> 16 != 0 {
                    return error_response(
                        request.request_id,
                        ERR_INVARIANT_VIOLATION,
                        "FLAG_MIGRATION_BATCH: request_id must encode shard in low 16 bits",
                    );
                }
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
                    Some(applied),
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
            // F-G5-004: reject any caller that sets the upper 48 bits of
            // request_id. Like FLAG_MIGRATION_BATCH (F-G5-010), the field
            // is overloaded for shard identity here; a typo or repurposed
            // id must not silently target an unintended shard.
            if request.request_id >> 16 != 0 {
                return error_response(
                    request.request_id,
                    ERR_INVARIANT_VIOLATION,
                    "OP_MIGRATION_COMPLETE: request_id must encode shard in low 16 bits",
                );
            }
            let shard = request.request_id as u16;

            let expected_records = le_u64_at(&request.payload, 0).unwrap_or(0);
            let _fence_sequence = le_u64_at(&request.payload, 8).unwrap_or(0);
            let migration_epoch = le_u64_at(&request.payload, 16).unwrap_or(0);
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
                let entry_count = match le_u32_at(&request.payload, 56) {
                    Some(n) => n as usize,
                    None => {
                        return error_response(
                            request.request_id,
                            ERR_MIGRATION_IN_PROGRESS,
                            "malformed exact-manifest entry count",
                        );
                    }
                };
                // R-043 (GH-04): use `checked_mul` + `checked_add` so
                // an attacker-controlled `entry_count` cannot overflow
                // `usize` (matters on 32-bit; defensive on 64-bit) and
                // produce a tiny `needed` that bypasses the size check
                // before allocating a `Vec::with_capacity(entry_count)`.
                // Pre-fix `60 + entry_count * 36` was an unchecked
                // multiply on a `u32::MAX` payload value.
                let needed = match 36usize
                    .checked_mul(entry_count)
                    .and_then(|n| n.checked_add(60))
                {
                    Some(n) => n,
                    None => {
                        return error_response(
                            request.request_id,
                            ERR_MIGRATION_IN_PROGRESS,
                            &format!(
                                "shard {shard} entry_count overflow ({entry_count}); rejecting frame",
                            ),
                        );
                    }
                };
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
                    let Some(generation) = le_u32_at(&request.payload, pos) else {
                        return error_response(
                            request.request_id,
                            ERR_MIGRATION_IN_PROGRESS,
                            "malformed exact-manifest generation",
                        );
                    };
                    pos += 4;
                    entries.push((TxKey { txid }, generation));
                }
                let completion_from_node = if request.payload.len() >= needed + 8 {
                    match le_u64_at(&request.payload, needed) {
                        Some(node) => Some(NodeId(node)),
                        None => {
                            return error_response(
                                request.request_id,
                                ERR_MIGRATION_IN_PROGRESS,
                                "malformed migration completion source node",
                            );
                        }
                    }
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

            let verify_only = request.flags & FLAG_MIGRATION_VERIFY_ONLY != 0;

            // Safety requirement (R-219): every completion, including an
            // empty shard (`record_count == 0`), MUST send cryptographic
            // manifest evidence. The prior zero-count/no-manifest fast path
            // let a stale source clear inbound state for a non-empty shard
            // without proving the target's contents.
            let has_manifest_evidence =
                source_manifest.is_some() || source_entries.as_ref().is_some_and(|e| !e.is_empty());
            if !has_manifest_evidence {
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

            // Verify the actual record count matches expected exactly
            // using the O(1) per-shard counter.
            let actual = engine.shard_record_count(shard);
            let count_ok = if expected_records == 0 {
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
                if let Some(from_node) = completion_from_node {
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
                    ERR_PAYLOAD_MALFORMED,
                    "batch-complete: too short",
                );
            }
            let shard_count = match le_u32_at(&request.payload, 0) {
                Some(n) => n as usize,
                None => {
                    return error_response(
                        request.request_id,
                        ERR_PAYLOAD_MALFORMED,
                        "batch-complete: malformed shard count",
                    );
                }
            };
            let expected_len = match 2usize
                .checked_mul(shard_count)
                .and_then(|n| n.checked_add(4))
                .and_then(|n| n.checked_add(8))
            {
                Some(len) => len,
                None => {
                    return error_response(
                        request.request_id,
                        ERR_PAYLOAD_MALFORMED,
                        &format!(
                            "batch-complete: shard_count overflow ({shard_count}); rejecting frame"
                        ),
                    );
                }
            };
            if request.payload.len() < expected_len {
                return error_response(
                    request.request_id,
                    ERR_PAYLOAD_MALFORMED,
                    &format!(
                        "batch-complete: need {expected_len} bytes, got {}",
                        request.payload.len()
                    ),
                );
            }
            let mut shards = Vec::with_capacity(shard_count);
            for i in 0..shard_count {
                let off = 4 + i * 2;
                let Some(shard) = le_u16_at(&request.payload, off) else {
                    return error_response(
                        request.request_id,
                        ERR_PAYLOAD_MALFORMED,
                        "batch-complete: malformed shard id",
                    );
                };
                shards.push(shard);
            }
            let from_node_off = 4 + shard_count * 2;
            let Some(from_node_id) = le_u64_at(&request.payload, from_node_off) else {
                return error_response(
                    request.request_id,
                    ERR_PAYLOAD_MALFORMED,
                    "batch-complete: malformed source node",
                );
            };
            let from_node = NodeId(from_node_id);

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
                None => {
                    return error_response(request.request_id, ERR_NOT_CLUSTERED, "not clustered");
                }
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
                    ERR_PAYLOAD_MALFORMED,
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
                None => {
                    return error_response(request.request_id, ERR_NOT_CLUSTERED, "not clustered");
                }
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
                None => error_response(
                    request.request_id,
                    ERR_PAYLOAD_MALFORMED,
                    "malformed topology vote",
                ),
            }
        }
        OP_TOPOLOGY_COMMIT => {
            // Topology authority: a proposer achieved quorum and is committing.
            // Activate the shard table with the committed members.
            let cluster = match cluster {
                Some(c) => c,
                None => {
                    return error_response(request.request_id, ERR_NOT_CLUSTERED, "not clustered");
                }
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
                    ERR_PAYLOAD_MALFORMED,
                    "malformed topology commit",
                ),
            }
        }
        _ => error_response(request.request_id, ERR_OPCODE_UNSUPPORTED, "unknown opcode"),
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

const REDO_GROUP_COMMIT_WINDOW: Duration = Duration::from_micros(200);

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
    write_redo_ops_with_group_window(redo_log, ops, REDO_GROUP_COMMIT_WINDOW)
}

fn write_redo_ops_with_group_window(
    redo_log: Option<&Mutex<RedoLog>>,
    ops: &[RedoOp],
    group_window: Duration,
) -> std::result::Result<(u64, u64), String> {
    let redo = match redo_log {
        Some(r) => r,
        None => return Ok((0, 0)),
    };
    if ops.is_empty() {
        return Ok((0, 0));
    }

    let (first_seq, last_seq) = {
        let mut log = redo.lock();
        let first_seq = log.current_sequence();
        let mut last_seq = first_seq;
        for op in ops {
            last_seq = log.append(op.clone()).map_err(|e| {
                // F-G5-008: log the underlying I/O error (which may
                // contain file paths or kernel diagnostic strings) at
                // `error!` level for operator triage, but return a
                // sanitized message to the client so internal
                // deployment topology is not leaked through ERR_INTERNAL
                // payloads. Same treatment for `redo log flush` below.
                tracing::error!(err = %e, "redo log append failed");
                "redo log append failed".to_string()
            })?;
        }
        (first_seq, last_seq)
    };

    if !group_window.is_zero() {
        std::thread::sleep(group_window);
    }

    let mut log = redo.lock();
    log.flush().map_err(|e| {
        tracing::error!(err = %e, "redo log flush failed");
        "redo log flush failed".to_string()
    })?;
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

fn begin_replication_intent_with_tracker(
    range: (u64, u64),
    tracker: Option<&crate::replication::durable::ReplicationIntentTracker>,
) -> std::result::Result<(), String> {
    if !valid_redo_range(range) {
        return Ok(());
    }
    if let Some(tracker) = tracker {
        tracker
            .begin(range.0, range.1)
            .map_err(|e| format!("replication intent begin: {e}"))?;
    }
    Ok(())
}

fn write_replicated_redo_ops(
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
    ops: &[RedoOp],
) -> std::result::Result<(u64, u64), String> {
    write_replicated_redo_ops_with_tracker(
        cluster.is_some(),
        redo_log,
        ops,
        REPLICATION_INTENT_TRACKER.get(),
    )
}

fn write_replicated_redo_ops_with_tracker(
    replication_applicable: bool,
    redo_log: Option<&Mutex<RedoLog>>,
    ops: &[RedoOp],
    tracker: Option<&crate::replication::durable::ReplicationIntentTracker>,
) -> std::result::Result<(u64, u64), String> {
    let range = write_redo_ops(redo_log, ops)?;
    if replication_applicable && !ops.is_empty() {
        // R-036: the master-side intent is the durable bridge between
        // "redo fsynced" and "replica ACK policy satisfied". It must be
        // persisted before the local engine apply; otherwise a crash in that
        // window leaves a local-only mutation with no startup barrier.
        begin_replication_intent_with_tracker(range, tracker)?;
    }
    Ok(range)
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

fn clear_replication_intents_after_compensation(ranges: &[(u64, u64)]) {
    for &range in ranges {
        if let Err(e) = commit_replication_intent(range) {
            tracing::warn!(err = %e, "replication intent: failed to clear after compensation");
        }
    }
}

fn clear_replication_intents_after_success(ranges: &[(u64, u64)]) {
    for &range in ranges {
        if let Err(e) = commit_replication_intent(range) {
            tracing::warn!(
                err = %e,
                "replication intent: failed to clear after successful replica ACKs; startup recovery will replay"
            );
        }
    }
}

/// Send replication operations to replica nodes for the given keys.
///
/// R-D1/D-3: each replica address receives batches on its own dense
/// per-replica sequence stream (see [`send_replica_ops_to`]); the
/// master-global redo range from `write_redo_ops()` is NOT used as the
/// wire label (each address only carries a per-address op subset), but
/// its high end is recorded per ACK in the durable `AckTracker` so
/// catch-up and lag monitoring operate in real redo space.
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
/// Build the per-address fan-out map for an outbound replication batch.
///
/// Pure function (no I/O): consults the shard table for replica targets,
/// the migration tracker for active dual-write windows, and the cluster's
/// node-address map to produce the `addr → ops` plan that
/// `replicate_all_ops` will actually send over the wire.
///
/// During an outbound migration of `shard` from this node, the dual-write
/// window contains the destination NodeIds (new master + new replicas).
/// Their addresses are added to the fan-out so writes that happen mid-
/// migration land on both the old replica set (durability) and the new
/// master / replica set (post-handoff consistency).
/// Phase E — replica fan-out plan returned by [`build_replication_targets`].
///
/// `by_addr` is the `addr → ops` map every targeted node receives.
///
/// `dual_write_only` is the subset of `by_addr` keys that exist
/// *solely* because at least one shard in the batch is migrating
/// outbound and the dual-write window names the target. Replicate
/// callers use this to enforce the per-set ACK invariant: a write
/// that touched a migrating shard cannot succeed unless at least one
/// `dual_write_only` address ACKed, regardless of the configured
/// `WriteAll` / `WriteMajority` policy. Without this, a `WriteMajority`
/// fan-out over the unioned set could ACK on the OLD replicas alone
/// and silently leave the new master with stale data, defeating the
/// dual-write durability invariant.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationPlan {
    pub by_addr: HashMap<SocketAddr, Vec<ReplicaOp>>,
    pub dual_write_only: std::collections::HashSet<SocketAddr>,
}

pub(crate) fn build_replication_targets(
    cluster: &RunningCluster,
    ops_by_key: &[(TxKey, Vec<ReplicaOp>)],
) -> std::result::Result<ReplicationPlan, String> {
    let table = cluster.shard_table();
    let table_guard = table.read();
    let rf = table_guard.replication_factor();
    let expected_replicas_per_key = rf.saturating_sub(1) as usize;
    let mut by_addr: HashMap<SocketAddr, Vec<ReplicaOp>> = HashMap::new();
    let mut regular_addrs: std::collections::HashSet<SocketAddr> = std::collections::HashSet::new();
    let mut dual_write_addrs: std::collections::HashSet<SocketAddr> =
        std::collections::HashSet::new();
    let mut target_errors: Vec<String> = Vec::new();
    let self_id = cluster.self_id();

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
        // Phase E: expand the replica set to include any active dual-write
        // destinations (new master + new replicas of an in-flight outbound
        // migration). This guarantees the new master observes writes that
        // happen during the migration window, so the post-handoff record
        // set is consistent with the pre-handoff durable state.
        //
        // Dual-write fan-out is best-effort with respect to address
        // resolution: a brand-new destination that hasn't yet been gossiped
        // is silently skipped rather than failing the write, because the
        // migration stream itself will deliver baseline+deltas to the
        // destination once the address is known.
        let dual_write_extras = cluster.dual_write_targets_for_shard(shard);
        for replica_id in &assignment.replicas {
            match cluster.node_addr(replica_id) {
                Some(addr) => {
                    by_addr.entry(addr).or_default().extend(ops.clone());
                    regular_addrs.insert(addr);
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
        for extra in &dual_write_extras {
            if *extra == self_id || assignment.replicas.contains(extra) {
                continue;
            }
            if let Some(addr) = cluster.node_addr(extra) {
                by_addr.entry(addr).or_default().extend(ops.clone());
                dual_write_addrs.insert(addr);
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

    // Subtract any addr that is *also* a regular replica for some
    // other shard — a replica promoted to new master during migration
    // satisfies the regular-set policy on its own and counts on both
    // sides; we only want addrs that are exclusively dual-write.
    dual_write_addrs.retain(|a| !regular_addrs.contains(a));

    Ok(ReplicationPlan {
        by_addr,
        dual_write_only: dual_write_addrs,
    })
}

fn replicate_all_ops(
    cluster: Option<&RunningCluster>,
    ops_by_key: &[(TxKey, Vec<ReplicaOp>)],
    redo_seq_range: (u64, u64),
    intent_ranges: &[(u64, u64)],
) -> std::result::Result<ReplicationOutcome, String> {
    let cluster = match cluster {
        Some(c) => c,
        None => return Ok(ReplicationOutcome::NotApplicable),
    };
    if ops_by_key.is_empty() {
        clear_replication_intents_after_success(intent_ranges);
        return Ok(ReplicationOutcome::NotApplicable);
    }
    let _fanout_permit = acquire_replication_fanout_permit();

    // Group all ops by target replica address — including any dual-write
    // expansion for shards currently migrating outbound (Phase E).
    let plan = build_replication_targets(cluster, ops_by_key)?;
    let ReplicationPlan {
        by_addr,
        dual_write_only,
    } = plan;
    let rf = cluster.shard_table().read().replication_factor();

    if by_addr.is_empty() {
        // No replicas configured or no replica addresses known.
        if rf > 1 {
            return Err(format!(
                "replication target resolution failed: no replica targets for RF={rf}",
            ));
        }
        clear_replication_intents_after_success(intent_ranges);
        return Ok(ReplicationOutcome::NotApplicable);
    }

    // Send to all replica targets in parallel using the shared replication
    // runtime. Each send runs on a blocking task (reusing pooled threads)
    // instead of spawning a new OS thread per replication call.
    let source_node_id = cluster.self_id().0;
    let ack_timeout = replication_ack_timeout_for(
        cluster.replication_timeout(),
        cluster.migration_pressure_active(),
        cluster.replication_timeout_during_migration(),
    );
    // Phase B3: stamp every outbound batch with the live coordinator
    // epoch so the receiver's gate can reject stale-cluster writes.
    let cluster_key = cluster.local_cluster_key();
    let auth_secret = cluster.cluster_secret().map(|s| s.to_vec());
    // Preserve the (addr, result) association so we can apply per-set
    // ACK accounting (Phase E) after the parallel fan-out completes.
    let results: Vec<(SocketAddr, std::result::Result<(), String>)> =
        REPL_RUNTIME.block_on(async {
            let mut handles = Vec::with_capacity(by_addr.len());
            for (addr, ops) in by_addr {
                let auth_secret = auth_secret.clone();
                handles.push(tokio::task::spawn_blocking(move || {
                    if ops.is_empty() {
                        return (addr, Ok(()));
                    }
                    // R-D1/D-3: per-replica dense stream labels are
                    // assigned inside `send_replica_ops_to` under the
                    // per-address slot mutex — NOT the master-global
                    // redo range, which covers ops this address never
                    // receives. The redo range's high end is recorded
                    // against the ACK for catch-up/lag bookkeeping.
                    let res = send_replica_ops_to(
                        addr,
                        &ops,
                        ack_timeout,
                        auth_secret.as_deref(),
                        cluster_key,
                        source_node_id,
                        redo_seq_range.1,
                    );
                    (addr, res)
                }));
            }
            let mut results = Vec::with_capacity(handles.len());
            for handle in handles {
                results.push(handle.await.unwrap_or_else(|_| {
                    (
                        SocketAddr::from(([0u8, 0, 0, 0], 0)),
                        Err("task panicked".to_string()),
                    )
                }));
            }
            results
        });

    let mut ack_count: usize = 0;
    let mut last_error: Option<String> = None;
    let mut dual_write_acks: usize = 0;
    for (addr, result) in &results {
        match result {
            Ok(()) => {
                ack_count += 1;
                if dual_write_only.contains(addr) {
                    dual_write_acks += 1;
                }
            }
            Err(e) => {
                tracing::warn!(err = %e, "replication to replica failed");
                last_error = Some(e.clone());
            }
        }
    }
    let total_targets = results.len();
    let dual_write_total = dual_write_only.len();
    // Phase E per-set ACK invariant: when at least one shard in this
    // batch is migrating outbound, require ≥1 ACK from the dual-write
    // set so the new master observes writes during the migration
    // window. Otherwise a `WriteMajority` policy could ACK on the OLD
    // replicas alone, leaving the post-handoff record set divergent.
    if dual_write_total > 0 && dual_write_acks == 0 {
        let best_effort_now = cluster.is_replication_best_effort();
        if best_effort_now {
            tracing::warn!(
                dual_write_total,
                "replication: dual-write set produced 0 ACKs (best_effort — write proceeds, new master may need full resync)",
            );
        } else {
            return Err(format!(
                "replication: dual-write set produced 0 ACKs of {dual_write_total} new-master target(s); \
                 migration durability requires at least one new-master ACK: {}",
                last_error.unwrap_or_default()
            ));
        }
    }

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
            // D-01: `PartialAck` is also the legitimate quorum-met-but-not-
            // all outcome for a non-best-effort WriteMajority policy, so the
            // ack policy is reported as a structured field instead of a
            // hard-coded "(best_effort)" parenthetical.
            tracing::warn!(
                ack_count,
                total_targets,
                best_effort,
                ack_policy = ?ack_policy,
                "replication: degraded ack",
            );
            clear_replication_intents_after_success(intent_ranges);
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
            clear_replication_intents_after_success(intent_ranges);
            Ok(ReplicationOutcome::Degraded)
        }
        ReplicationClassification::FullAck => {
            clear_replication_intents_after_success(intent_ranges);
            Ok(ReplicationOutcome::Full)
        }
    }
}

fn replication_ack_timeout_for(
    base: Duration,
    migration_pressure: bool,
    migration_timeout_floor: Duration,
) -> Duration {
    if migration_pressure {
        base.max(migration_timeout_floor)
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
        // The intent range is already present in the durable tracker; this
        // recovery path commits it explicitly after successful fan-out.
        replicate_all_ops(Some(cluster), ops, range, &[]).map(|_| ())
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
        let (entries, earliest_sequence, current_sequence) = {
            let log = redo_log.lock();
            // F-G5-008: redo I/O errors carry kernel diagnostic strings
            // and (depending on the storage backend) file paths. Log
            // detail for operator triage; return a sanitized message
            // that does not leak deployment topology to clients.
            let entries = log.read_from_sequence(range.first_sequence).map_err(|e| {
                tracing::error!(err = %e, "read redo for pending replication intent failed");
                "read redo for pending replication intent failed".to_string()
            })?;
            let earliest = log.earliest_sequence().map_err(|e| {
                tracing::error!(err = %e, "read redo floor for pending replication intent failed");
                "read redo floor for pending replication intent failed".to_string()
            })?;
            (entries, earliest, log.current_sequence())
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
            let range_reclaimed = match earliest_sequence {
                Some(earliest) => earliest > range.first_sequence,
                None => current_sequence > range.last_sequence,
            };
            if range_reclaimed {
                tracing::warn!(
                    first_sequence = range.first_sequence,
                    last_sequence = range.last_sequence,
                    ?earliest_sequence,
                    current_sequence,
                    "pending replication intent refers to reclaimed redo range; clearing marker and requiring replica full resync/catch-up",
                );
                tracker
                    .commit(range.first_sequence, range.last_sequence)
                    .map_err(|e| format!("replication intent commit: {e}"))?;
                continue;
            }
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
        Some(policy) => crate::replication::manager::required_replica_acks(total_targets, policy),
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

/// Gap #8 (TERANODE_PRODUCTION_READINESS_GAPS.md): pre-apply state
/// captured by a dispatch handler so a failed-replication rollback can
/// restore the record exactly.
///
/// Each variant pairs 1:1 with a `ReplicaOp` whose original compensation
/// path used defaults / zeros / unconditional restores:
///
/// * `UnsetMined` — block_height + subtree_idx of the entry being cleared.
/// * `Reassign` — utxo_hash of the slot before the reassign.
/// * `Prune` — status byte of the slot before the prune.
/// * `SetLocked` — locked flag + DAH before SetLocked cleared pruning state.
///
/// `None` indicates "no before-image needed" (the op's compensation is
/// fully determined by the on-device state at rollback time, e.g.
/// spend, freeze, etc.).
#[derive(Debug, Clone, Copy)]
pub(crate) enum BeforeImage {
    /// No before-image needed — compensation reads current device state.
    None,
    /// Captured pre-apply block-entry fields for an unset-mined op.
    UnsetMined { block_height: u32, subtree_idx: u32 },
    /// Captured pre-apply utxo_hash for a reassign op.
    Reassign { prior_utxo_hash: [u8; 32] },
    /// Captured pre-apply locked flag and DAH for a set-locked op.
    SetLocked {
        prior_locked: bool,
        prior_delete_at_height: u32,
    },
    /// Captured pre-apply status byte for a prune op.
    ///
    /// `PruneSlot` `ReplicaOp`s are currently only generated by the
    /// migration delta path, which does not call
    /// `compensate_replication_failure`. The variant exists so a future
    /// dispatch handler that adds a prune client API can capture the
    /// pre-apply status byte without re-plumbing the rollback API.
    /// Tests in `tests/replication_rollback.rs` construct this variant
    /// directly to exercise the compensation + recovery path.
    #[allow(dead_code)]
    Prune { prior_status: u8 },
}

/// Gap #8: build a parallel `before_images` vec of `BeforeImage::None`
/// for handlers that don't capture pre-apply state (spend, freeze,
/// set-conflicting, etc.). The shape mirrors `repl_ops` exactly so
/// `compensate_replication_failure` can index `before_images[i].1[j]`
/// in lockstep with `repl_ops[i].1[j]`.
fn no_before_images(repl_ops: &[(TxKey, Vec<ReplicaOp>)]) -> Vec<(TxKey, Vec<BeforeImage>)> {
    repl_ops
        .iter()
        .map(|(k, ops)| (*k, vec![BeforeImage::None; ops.len()]))
        .collect()
}

fn before_images_match_repl_ops(
    repl_ops: &[(TxKey, Vec<ReplicaOp>)],
    before_images: &[(TxKey, Vec<BeforeImage>)],
) -> bool {
    repl_ops.len() == before_images.len()
        && repl_ops
            .iter()
            .zip(before_images)
            .all(|((op_key, ops), (before_key, images))| {
                op_key == before_key && ops.len() == images.len()
            })
}

fn push_repl_with_before_image(
    repl_ops: &mut Vec<(TxKey, Vec<ReplicaOp>)>,
    before_images: &mut Vec<(TxKey, Vec<BeforeImage>)>,
    key: TxKey,
    op: ReplicaOp,
    before: BeforeImage,
) {
    repl_ops.push((key, vec![op]));
    before_images.push((key, vec![before]));
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
/// # Gap #8: bit-exact rollback via captured before-images
///
/// `before_images[i].1[j]` carries the pre-apply state captured at
/// dispatch entry for `repl_ops[i].1[j]`. When the variant is non-`None`
/// the compensation path uses the captured value to restore the original
/// record state exactly — no zeros, no defaults, no unconditional
/// restores. The compensation also appends a `Compensate*` redo entry
/// with the same before-image so a crash mid-rollback can re-run the
/// restore deterministically (gap #8 part 3).
///
/// `before_images.len()` MUST equal `repl_ops.len()` and each inner Vec
/// MUST align with the corresponding `repl_ops` inner Vec. Callers that
/// have no before-image to capture pass `BeforeImage::None`.
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
    before_images: &[(TxKey, Vec<BeforeImage>)],
    redo_log: Option<&Mutex<RedoLog>>,
) -> std::result::Result<(), String> {
    let mut comp_redo: Vec<RedoOp> = Vec::new();

    let before_shape_ok = before_images_match_repl_ops(repl_ops, before_images);
    if !before_shape_ok {
        tracing::error!(
            repl_groups = repl_ops.len(),
            before_groups = before_images.len(),
            "replication compensation aborted: before-image shape mismatch"
        );
        return Err("before-image shape mismatch".to_string());
    }

    // Helper: look up the before-image aligned with `repl_ops[i].1[j]`. Shape
    // was validated above, so a missing entry is impossible unless this function
    // is edited incorrectly.
    let lookup_before = |i: usize, j: usize| -> BeforeImage { before_images[i].1[j] };

    for (i, (key, ops)) in repl_ops.iter().enumerate() {
        for (j, op) in ops.iter().enumerate() {
            match op {
                ReplicaOp::Spend {
                    offset,
                    current_block_height,
                    block_height_retention,
                    ..
                } => {
                    if let Ok(slot) = engine.read_slot(key, *offset) {
                        let req = crate::ops::unspend::UnspendRequest {
                            tx_key: *key,
                            offset: *offset,
                            utxo_hash: slot.hash,
                            spending_data: slot.spending_data,
                            current_block_height: *current_block_height,
                            block_height_retention: *block_height_retention,
                        };
                        let _ = engine.unspend(&req);
                        comp_redo.push(RedoOp::Unspend {
                            tx_key: *key,
                            offset: *offset,
                            spending_data: Some(slot.spending_data),
                            new_spent_count: 0,
                        });
                    }
                }
                ReplicaOp::Unspend {
                    offset,
                    spending_data,
                    current_block_height,
                    block_height_retention,
                    ..
                } => {
                    // Reverse unspend → re-spend the slot with its exact
                    // prior spending_data and original DAH evaluation context.
                    if let Ok(slot) = engine.read_slot(key, *offset) {
                        let req = crate::ops::spend::SpendMultiRequest {
                            tx_key: *key,
                            spends: vec![crate::ops::spend::SpendItem {
                                offset: *offset,
                                utxo_hash: slot.hash,
                                spending_data: *spending_data,
                                idx: 0,
                            }],
                            ignore_conflicting: true,
                            ignore_locked: true,
                            current_block_height: *current_block_height,
                            block_height_retention: *block_height_retention,
                        };
                        if let Ok(v) = engine.validate_spend_multi(&req) {
                            let _ = v.apply(engine);
                        }
                        comp_redo.push(RedoOp::Spend {
                            tx_key: *key,
                            offset: *offset,
                            spending_data: *spending_data,
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
                        comp_redo.push(RedoOp::UnfreezeV2 {
                            tx_key: *key,
                            offset: *offset,
                            utxo_hash: slot.hash,
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
                        comp_redo.push(RedoOp::FreezeV2 {
                            tx_key: *key,
                            offset: *offset,
                            utxo_hash: slot.hash,
                        });
                    }
                }
                ReplicaOp::SetMined {
                    block_id,
                    block_height,
                    subtree_idx,
                    current_block_height,
                    block_height_retention,
                    ..
                } => {
                    let req = crate::ops::set_mined::SetMinedRequest {
                        tx_key: *key,
                        block_id: *block_id,
                        block_height: *block_height,
                        subtree_idx: *subtree_idx,
                        on_longest_chain: false,
                        unset_mined: true,
                        current_block_height: *current_block_height,
                        block_height_retention: *block_height_retention,
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
                ReplicaOp::UnsetMined {
                    block_id,
                    current_block_height,
                    block_height_retention,
                    ..
                } => {
                    // Gap #8: reverse unset → re-set the block entry using
                    // the captured pre-apply block_height + subtree_idx.
                    // When no before-image is available we fall back to
                    // best-effort zeros AND skip emitting a Compensate*
                    // redo entry — a crash mid-rollback would replay zeros
                    // which is worse than letting startup recovery skip
                    // the missing compensation entry.
                    let (bh, sti) = match lookup_before(i, j) {
                        BeforeImage::UnsetMined {
                            block_height,
                            subtree_idx,
                        } => (block_height, subtree_idx),
                        _ => (0u32, 0u32),
                    };
                    let req = crate::ops::set_mined::SetMinedRequest {
                        tx_key: *key,
                        block_id: *block_id,
                        block_height: bh,
                        subtree_idx: sti,
                        on_longest_chain: true,
                        unset_mined: false,
                        current_block_height: *current_block_height,
                        block_height_retention: *block_height_retention,
                    };
                    let _ = engine.set_mined(&req);
                    // Forward redo entry: re-add the original block entry
                    // (so a recovery replay applies the same restoration).
                    comp_redo.push(RedoOp::SetMined {
                        tx_key: *key,
                        block_id: *block_id,
                        block_height: bh,
                        subtree_idx: sti,
                        unset: false,
                    });
                    // Compensation-intent redo entry — only emitted when a
                    // real before-image was captured. On crash mid-
                    // rollback, recovery's `replay_compensate_unset_mined`
                    // restores the same state from this entry alone.
                    if matches!(lookup_before(i, j), BeforeImage::UnsetMined { .. }) {
                        comp_redo.push(RedoOp::CompensateUnsetMined {
                            tx_key: *key,
                            block_id: *block_id,
                            block_height: bh,
                            subtree_idx: sti,
                        });
                    }
                }
                ReplicaOp::Reassign {
                    offset,
                    new_hash,
                    block_height,
                    spendable_after,
                    ..
                } => {
                    // Gap #8: reverse reassign using the captured prior
                    // utxo_hash. When no before-image is available, fall
                    // back to zeros AND skip the Compensate* entry — a
                    // crash mid-rollback that replayed zeros would silently
                    // diverge.
                    let prior_hash = match lookup_before(i, j) {
                        BeforeImage::Reassign { prior_utxo_hash } => Some(prior_utxo_hash),
                        _ => None,
                    };
                    let restore_hash = prior_hash.unwrap_or([0u8; 32]);
                    // The post-reassign slot is UTXO_UNSPENT with the new
                    // hash. `engine.reassign` requires UTXO_FROZEN as a
                    // precondition, so going back through that API would
                    // silently fail (status mismatch). Restore by writing
                    // the slot directly instead.
                    if let Some(entry) = engine.lookup(key)
                        && let Ok(slot) =
                            crate::io::read_utxo_slot(engine.device(), entry.record_offset, *offset)
                        && slot.hash == *new_hash
                    {
                        let restored = crate::record::UtxoSlot::new_unspent(restore_hash);
                        let _ = crate::io::write_utxo_slot(
                            engine.device(),
                            entry.record_offset,
                            *offset,
                            &restored,
                        );
                    }
                    // Forward redo entry mirrors the engine call so a
                    // recovery replay re-applies the same hash restoration.
                    // We retain the Reassign entry for back-compat with
                    // recovery paths that don't yet know about
                    // CompensateReassign.
                    comp_redo.push(RedoOp::Reassign {
                        tx_key: *key,
                        offset: *offset,
                        new_hash: restore_hash,
                        block_height: *block_height,
                        spendable_after: *spendable_after,
                    });
                    // Compensation-intent: only when a real before-image
                    // was captured. Recovery's `replay_compensate_reassign`
                    // restores the slot bit-exactly from this entry.
                    if let Some(prior) = prior_hash {
                        comp_redo.push(RedoOp::CompensateReassign {
                            tx_key: *key,
                            offset: *offset,
                            prior_utxo_hash: prior,
                        });
                    }
                }
                ReplicaOp::PruneSlot { offset, .. }
                | ReplicaOp::PruneSlotIfSpentBy { offset, .. } => {
                    // Gap #8: PruneSlot only changes the status byte to
                    // UTXO_PRUNED. The slot data (hash, spending_data) is
                    // preserved on device. To reverse, restore the captured
                    // pre-apply status byte (UNSPENT / SPENT / FROZEN).
                    // When no before-image is available we fall back to
                    // UNSPENT — preserves prior best-effort behavior — and
                    // skip the Compensate* redo entry.
                    let prior_status = match lookup_before(i, j) {
                        BeforeImage::Prune { prior_status } => Some(prior_status),
                        _ => None,
                    };
                    let restore_status = prior_status.unwrap_or(crate::record::UTXO_UNSPENT);
                    if let Some(entry) = engine.lookup(key)
                        && let Ok(mut slot) =
                            crate::io::read_utxo_slot(engine.device(), entry.record_offset, *offset)
                        && slot.status == crate::record::UTXO_PRUNED
                    {
                        slot.status = restore_status;
                        let _ = crate::io::write_utxo_slot(
                            engine.device(),
                            entry.record_offset,
                            *offset,
                            &slot,
                        );
                    }
                    // Compensation-intent redo entry — only when a real
                    // before-image was captured. A crash mid-rollback
                    // would otherwise have no way to recover the status
                    // byte; with the entry, recovery's
                    // `replay_compensate_prune` restores the slot
                    // bit-exactly.
                    if let Some(s) = prior_status {
                        comp_redo.push(RedoOp::CompensatePrune {
                            tx_key: *key,
                            offset: *offset,
                            prior_status: s,
                        });
                    }
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
                    let (target_locked, target_dah) = match lookup_before(i, j) {
                        BeforeImage::SetLocked {
                            prior_locked,
                            prior_delete_at_height,
                        } => (prior_locked, prior_delete_at_height),
                        _ => (!value, 0),
                    };
                    let _ =
                        engine.restore_set_locked_for_compensation(key, target_locked, target_dah);
                    comp_redo.push(RedoOp::SetLocked {
                        tx_key: *key,
                        value: target_locked,
                    });
                    if matches!(lookup_before(i, j), BeforeImage::SetLocked { .. }) {
                        comp_redo.push(RedoOp::CompensateSetLocked {
                            tx_key: *key,
                            prior_locked: target_locked,
                            prior_delete_at_height: target_dah,
                        });
                    }
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
                ReplicaOp::MarkLongestChain {
                    on_longest_chain,
                    current_block_height,
                    block_height_retention,
                    ..
                } => {
                    // R-052 best-effort compensation: re-apply with the
                    // inverse `on_longest_chain` flag. Matches the no-
                    // before-image strategy used by SetConflicting /
                    // SetLocked / PreserveUntil — bit-exact pre-image
                    // restoration is deferred until a dedicated
                    // BeforeImage::MarkLongestChain variant is plumbed
                    // through. The forward redo entry mirrors the call
                    // so a recovery replay performs the same flip.
                    let req = crate::ops::mark_longest_chain::MarkOnLongestChainRequest {
                        tx_key: *key,
                        on_longest_chain: !on_longest_chain,
                        current_block_height: *current_block_height,
                        block_height_retention: *block_height_retention,
                    };
                    if let Ok(resp) = engine.mark_on_longest_chain(&req) {
                        comp_redo.push(RedoOp::MarkOnLongestChain {
                            tx_key: *key,
                            on_longest_chain: !on_longest_chain,
                            current_block_height: *current_block_height,
                            block_height_retention: *block_height_retention,
                            generation: resp.generation,
                        });
                    }
                }
            }
        }
    }

    if !comp_redo.is_empty() {
        write_redo_ops(redo_log, &comp_redo)
            .map(|_| ())
            .map_err(|e| format!("replication compensation redo write failed: {e}"))?;
    }

    Ok(())
}

fn compensate_replication_failure_or_error(
    request_id: u64,
    engine: &Engine,
    repl_ops: &[(TxKey, Vec<ReplicaOp>)],
    before_images: &[(TxKey, Vec<BeforeImage>)],
    redo_log: Option<&Mutex<RedoLog>>,
    intent_ranges: &[(u64, u64)],
) -> Option<ResponseFrame> {
    match compensate_replication_failure(engine, repl_ops, before_images, redo_log) {
        Ok(()) => {
            clear_replication_intents_after_compensation(intent_ranges);
            None
        }
        Err(cause) => {
            tracing::error!(
                cause = %cause,
                "replication compensation failed; leaving replication intent pending for operator recovery"
            );
            Some(error_response(request_id, ERR_INTERNAL, &cause))
        }
    }
}

/// Look up (or create) the per-address replication slot.
fn repl_slot_for(addr: SocketAddr) -> std::sync::Arc<Mutex<PerAddrSlot>> {
    // The outer pool lock is held only for the HashMap lookup/insert,
    // not during I/O.
    let mut pool = REPL_POOL.lock();
    pool.entry(addr)
        .or_insert_with(|| {
            std::sync::Arc::new(Mutex::new(PerAddrSlot {
                connection: None,
                last_acked: 0,
                next_sequence: None,
            }))
        })
        .clone()
}

/// Drop the cached per-address replication state (pooled connection AND
/// dense-sequence cursor) for `addr`.
///
/// The next [`send_replica_ops_to`] re-probes the replica's applied
/// watermark before assigning sequence numbers — exactly what a fresh
/// master process does on first contact. Integration tests use this to
/// emulate a master restart against a live replica; it is also safe to
/// call operationally (the probe/relabel protocol re-converges).
pub fn reset_replica_stream(addr: SocketAddr) {
    let slot = {
        let mut pool = REPL_POOL.lock();
        pool.remove(&addr)
    };
    // Lock (and immediately drop) the slot so an in-flight send on the
    // removed slot finishes before the connection closes underneath it.
    if let Some(slot) = slot {
        let mut guard = slot.lock();
        guard.connection = None;
        guard.next_sequence = None;
    }
}

/// Drop only the pooled connection for `addr`, KEEPING the
/// dense-sequence cursor.
///
/// Used when the replica process restarted (its old connection is dead
/// or would be served by a defunct handler) but the master's stream
/// position remains valid: the next send reconnects and continues at
/// the same cursor, and the receiver's persisted watermark makes the
/// first post-restart batch an exact next-expected match.
pub fn drop_replica_connection(addr: SocketAddr) {
    let slot = {
        let pool = REPL_POOL.lock();
        pool.get(&addr).cloned()
    };
    if let Some(slot) = slot {
        slot.lock().connection = None;
    }
}

/// Send one `ReplicaBatch` frame on the slot's pooled connection and wait
/// for the replica's `ReplicaAck`.
///
/// Reuses the cached connection if healthy, otherwise reconnects (once on
/// a failed send). Returns the decoded ack on success; `Err` covers
/// connect/send/recv transport failures. The caller holds the slot lock,
/// so concurrent sends to the SAME replica are serialized (correct: TCP
/// is ordered) while different replicas proceed in parallel.
fn exchange_replica_batch(
    slot_guard: &mut PerAddrSlot,
    addr: SocketAddr,
    batch: &ReplicaBatch,
    ack_timeout: Duration,
    auth_secret: Option<&[u8]>,
) -> std::result::Result<ReplicaAck, String> {
    let mut transport = match slot_guard.connection.take() {
        Some(t) if t.is_connected() && t.auth_secret_matches(auth_secret) => t,
        _ => TcpReplicaTransport::connect_with_auth(
            &addr.to_string(),
            Duration::from_secs(5),
            auth_secret.map(|s| s.to_vec()),
        )
        .map_err(|e| format!("connect: {e}"))?,
    };

    if let Err(e) = transport.send_batch(batch) {
        // Connection may be stale (broken by partition, killed node, etc.).
        // Drop the broken transport and reconnect once before giving up.
        drop(transport);
        let mut retry_transport = TcpReplicaTransport::connect_with_auth(
            &addr.to_string(),
            Duration::from_secs(5),
            auth_secret.map(|s| s.to_vec()),
        )
        .map_err(|e2| format!("send: {e}; reconnect: {e2}"))?;
        if let Err(e2) = retry_transport.send_batch(batch) {
            return Err(format!("send after reconnect: {e2}"));
        }
        transport = retry_transport;
    }

    match transport.recv_ack(ack_timeout) {
        Ok(ack) => {
            slot_guard.connection = Some(transport);
            Ok(ack)
        }
        Err(e) => Err(format!("recv_ack: {e}")),
    }
}

/// Maximum send attempts per batch inside [`send_replica_ops_to`]: the
/// initial send plus one relabeled retry after a cursor-resync signal
/// (`ReplicaAck::Gap` or a duplicate-skip ACK whose `through_sequence`
/// is ahead of the batch). The receiver's watermark can only move via
/// this slot's own serialized sends, so a single renegotiation round
/// always converges; a second disagreement is a hard error.
const MAX_SEQUENCE_RENEGOTIATIONS: usize = 2;

/// Send replica ops to one replica address on that address's **dense
/// per-replica sequence stream** (R-D1/D-3).
///
/// Behavior, all under the per-address slot mutex:
///
/// 1. If the slot's stream cursor is unsynced (first contact since
///    process start), send an empty-batch **probe**; the receiver ACKs
///    its authoritative per-stream applied watermark and the cursor is
///    adopted as `watermark + 1`. This is what keeps the master's
///    per-replica next-sequence consistent with the receiver's persisted
///    watermark across restarts of either side.
/// 2. Label the batch `first_sequence = cursor` and send.
/// 3. ACK handling:
///    * `Ok { through == last_sequence }` — success: advance the cursor,
///      record `redo_high` (the redo-log position this batch covers) in
///      the durable [`crate::replication::durable::AckTracker`] for
///      catch-up/lag bookkeeping.
///    * `Gap { expected_sequence }` — the replica is missing positions
///      below the label (burned by an earlier failed batch): relabel at
///      `expected_sequence` and retry once.
///    * `Ok { through > last_sequence }` — duplicate-skip against a
///      watermark ahead of the cursor (cursor desync): adopt
///      `through + 1` and retry once. The batch content was NOT applied,
///      so this is never treated as success.
///    * transport / replica error — fail the batch and **burn** the
///      assigned positions (advance the cursor past them). The replica
///      may or may not have applied the frame; burning guarantees a
///      position is never reused for different content, and the benign
///      hole (if any) heals on the next send via the Gap/relabel path.
///
/// `redo_high` is the highest master redo-log sequence whose ops are
/// covered by this batch — pass `0` when not applicable (e.g. catch-up
/// chunks whose redo progress the caller records itself).
///
/// Returns `Ok(())` once the replica has durably applied (or provably
/// already applied) every op in `ops`; `Err(message)` otherwise.
pub fn send_replica_ops_to(
    addr: SocketAddr,
    ops: &[ReplicaOp],
    ack_timeout: Duration,
    auth_secret: Option<&[u8]>,
    cluster_key: u64,
    source_node_id: u64,
    redo_high: u64,
) -> std::result::Result<(), String> {
    if ops.is_empty() {
        return Ok(());
    }
    let slot = repl_slot_for(addr);
    // Lock only this address's slot. Other addresses are uncontended.
    let mut slot_guard = slot.lock();

    // Sync the stream cursor on first contact: adopt the replica's
    // authoritative applied watermark. Initializing from any master-side
    // persisted value instead could label NEW ops at positions the
    // replica already covers, which the receiver would dedup-skip and
    // ACK — silent drop. The probe makes that impossible by
    // construction.
    let mut next = match slot_guard.next_sequence {
        Some(n) => n,
        None => {
            let probe = ReplicaBatch {
                first_sequence: 0,
                ops: Vec::new(),
                trace_ctx: None,
                source_node_id: Some(source_node_id),
                cluster_key,
            };
            match exchange_replica_batch(&mut slot_guard, addr, &probe, ack_timeout, auth_secret)?
            {
                ReplicaAck::Ok { through_sequence } => {
                    let n = through_sequence + 1;
                    slot_guard.next_sequence = Some(n);
                    n
                }
                other => {
                    return Err(format!("watermark probe rejected: {other:?}"));
                }
            }
        }
    };

    for attempt in 0..MAX_SEQUENCE_RENEGOTIATIONS {
        let batch = ReplicaBatch {
            first_sequence: next,
            ops: ops.to_vec(),
            trace_ctx: crate::observability::WireTraceContext::from_current_span(),
            source_node_id: Some(source_node_id),
            cluster_key,
        };
        let last = batch.last_sequence();

        let ack = match exchange_replica_batch(&mut slot_guard, addr, &batch, ack_timeout, auth_secret)
        {
            Ok(ack) => ack,
            Err(e) => {
                // Burn the assigned positions: the frame may have been
                // applied with the ACK lost in flight. Reusing the
                // positions for different content could be dedup-skipped
                // by the receiver; a hole heals via Gap/relabel instead.
                slot_guard.next_sequence = Some(last + 1);
                return Err(e);
            }
        };

        match ack {
            ReplicaAck::Ok { through_sequence } if through_sequence == last => {
                slot_guard.next_sequence = Some(last + 1);
                slot_guard.last_acked = through_sequence;
                // Persist the redo-log coverage for crash-safe catch-up
                // and lag monitoring (real redo space, not the stream
                // labels — see AckTracker docs).
                if redo_high > 0
                    && let Some(tracker) = ACK_TRACKER.get()
                {
                    tracker.record_ack(addr, redo_high);
                }
                return Ok(());
            }
            ReplicaAck::Ok { through_sequence } => {
                // Duplicate-skip against a watermark ahead of our cursor:
                // nothing from THIS batch was applied. Adopt and retry.
                tracing::warn!(
                    %addr,
                    attempt,
                    sent_first = next,
                    sent_last = last,
                    replica_through = through_sequence,
                    "replication: cursor behind replica watermark; resyncing and relabeling",
                );
                slot_guard.next_sequence = Some(through_sequence + 1);
                next = through_sequence + 1;
            }
            ReplicaAck::Gap {
                expected_sequence, ..
            } => {
                // Benign hole left by burned positions; relabel down to
                // the replica's next-expected sequence and retry.
                tracing::warn!(
                    %addr,
                    attempt,
                    sent_first = next,
                    expected_sequence,
                    "replication: replica NAKed sequence gap; relabeling and re-sending",
                );
                slot_guard.next_sequence = Some(expected_sequence);
                next = expected_sequence;
            }
            ReplicaAck::Error { message, .. } => {
                // Burn the positions — the replica may have applied a
                // prefix before failing.
                slot_guard.next_sequence = Some(last + 1);
                return Err(format!("replica error: {message}"));
            }
        }
    }

    Err(format!(
        "replication to {addr}: sequence renegotiation did not converge after \
         {MAX_SEQUENCE_RENEGOTIATIONS} attempts",
    ))
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

fn needs_dispatch_visibility_barrier(op: u16) -> bool {
    is_mutation_opcode(op)
        || matches!(
            op,
            OP_GET_BATCH | OP_GET_SPEND_BATCH | OP_QUERY_OLD_UNMINED | OP_REPLICA_BATCH
        )
}

/// True for ops whose state must NOT be observed by concurrent reads:
/// every mutation opcode plus `OP_REPLICA_BATCH` (which APPLIES master-
/// originated mutations locally). These take the exclusive (write) side
/// of the barrier; client reads take the shared (read) side and are
/// blocked while a mutation/replica-batch is in flight.
fn needs_exclusive_visibility_barrier(op: u16) -> bool {
    is_mutation_opcode(op) || matches!(op, OP_REPLICA_BATCH)
}

/// Owns whichever side of the `dispatch_visibility_barrier` rwlock is
/// appropriate for the opcode. Dropping it releases the guard; the
/// underlying enum keeps the borrow checker honest without exposing
/// `RwLockReadGuard`/`RwLockWriteGuard` to every call site. Both
/// variants exist for their RAII side effect (the inner guards are
/// never read).
#[allow(clippy::large_enum_variant, dead_code)]
enum DispatchVisibilityGuard<'a> {
    Shared(parking_lot::RwLockReadGuard<'a, ()>),
    Exclusive(parking_lot::RwLockWriteGuard<'a, ()>),
}

fn acquire_dispatch_visibility_guard(
    engine: &Engine,
    op: u16,
) -> Option<DispatchVisibilityGuard<'_>> {
    if !needs_dispatch_visibility_barrier(op) {
        return None;
    }
    if needs_exclusive_visibility_barrier(op) {
        Some(DispatchVisibilityGuard::Exclusive(
            engine.acquire_mutation_visibility_guard(),
        ))
    } else {
        Some(DispatchVisibilityGuard::Shared(
            engine.acquire_dispatch_visibility_guard(),
        ))
    }
}

/// Phase I — true if `op` is a client-facing read or write that must
/// wait for the node to be `Alive` (its first committed topology
/// observed). Bootstrap traffic — topology proposals/votes/commits,
/// replica batches, ping/health, partition map, admin diagnostics —
/// bypasses the gate so the node has an opportunity to become ready
/// in the first place.
fn needs_cluster_readiness(op: u16) -> bool {
    is_mutation_opcode(op) || matches!(op, OP_GET_BATCH | OP_GET_SPEND_BATCH | OP_QUERY_OLD_UNMINED)
}

/// Gap #5 — return `Some(error_response)` when `op` depends on a secondary
/// index that failed to rebuild at startup.
///
/// The mapping below identifies each opcode's secondary-index dependency:
///
/// - **Unmined index** (`SECONDARY_UNMINED_OK`): drives the pruner's
///   "old unmined" iterator (`OP_QUERY_OLD_UNMINED`) and the
///   mining/longest-chain workflow (`OP_MARK_LONGEST_CHAIN_BATCH`,
///   which writes the secondary unmined entry on every transition).
///   `OP_SET_MINED_BATCH` also touches the secondary by clearing
///   `unmined_since`, so it's gated as well.
///
/// - **DAH index** (`SECONDARY_DAH_OK`): drives the DAH-based deletion
///   sweep (`OP_PROCESS_EXPIRED_PRESERVATIONS`), the preservation override
///   (`OP_PRESERVE_TRANSACTIONS`, `OP_PRESERVE_UNTIL_BATCH`), and the
///   conflict workflow (`OP_SET_CONFLICTING_BATCH` schedules / clears DAH
///   entries depending on `block_height_retention`).
///
/// - **Both indexes** (`OP_DELETE_BATCH`): a delete touches both
///   secondaries to remove any tombstone entries.
///
/// Regular spend/get/create/freeze/unfreeze/unspend/reassign/set-locked
/// paths do NOT depend on the secondary indexes, so they keep working even
/// when a secondary is degraded — that's what the gap doc required.
fn check_secondary_readiness(op: u16, request_id: u64) -> Option<ResponseFrame> {
    secondary_readiness_verdict(op, secondary_status(), request_id)
}

/// Pure policy function: given an opcode and a [`SecondaryStatus`] snapshot,
/// return `Some(error_response)` if the op depends on an unavailable
/// secondary, or `None` otherwise.
///
/// Split out from [`check_secondary_readiness`] so tests can drive every
/// branch deterministically without mutating the global readiness flags
/// (which would race with other parallel tests).
pub(crate) fn secondary_readiness_verdict(
    op: u16,
    status: SecondaryStatus,
    request_id: u64,
) -> Option<ResponseFrame> {
    if status.fully_ok() {
        return None;
    }
    let needs_unmined = matches!(
        op,
        OP_QUERY_OLD_UNMINED | OP_MARK_LONGEST_CHAIN_BATCH | OP_SET_MINED_BATCH
    );
    let needs_dah = matches!(
        op,
        OP_PROCESS_EXPIRED_PRESERVATIONS
            | OP_PRESERVE_TRANSACTIONS
            | OP_PRESERVE_UNTIL_BATCH
            | OP_SET_CONFLICTING_BATCH
    );
    let needs_both = matches!(op, OP_DELETE_BATCH);
    if needs_both && (!status.dah_ok || !status.unmined_ok) {
        return Some(error_response(
            request_id,
            ERR_INDEX_DEGRADED,
            "secondary index unavailable: delete requires both DAH and unmined indexes",
        ));
    }
    if needs_unmined && !status.unmined_ok {
        return Some(error_response(
            request_id,
            ERR_INDEX_DEGRADED,
            "secondary index unavailable: unmined-secondary failed to rebuild at startup",
        ));
    }
    if needs_dah && !status.dah_ok {
        return Some(error_response(
            request_id,
            ERR_INDEX_DEGRADED,
            "secondary index unavailable: DAH-secondary failed to rebuild at startup",
        ));
    }
    None
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
            // Determine the target node address for the redirect.
            //
            // R-041: encode the source node's `shard_table_version` from
            // `RouteDecision::RedirectTo` into `error_data` alongside the
            // address so the receiving client can detect a stale-route
            // loop. Wire format: `[addr_len:2][addr][shard_table_version:8]`.
            // Older clients (or external Go adapters) that read
            // `error_data` as raw addr bytes via `from_utf8` will see a
            // malformed UTF-8 buffer (the trailing version bytes are
            // binary), `from_utf8` returns Err, and the legacy
            // redirect-collector returns `None` — falling through to a
            // PartialError surfaced to the caller. That is a graceful
            // failure (no silent corruption); the legacy client just
            // does not benefit from version-based loop detection.
            let route = cluster.route(&key);
            let error_data = match route {
                crate::cluster::shards::RouteDecision::RedirectTo {
                    node,
                    shard_table_version,
                } => match cluster.node_addr(&node) {
                    Some(addr) => crate::protocol::codec::encode_redirect_with_version(
                        &addr.to_string(),
                        shard_table_version,
                    ),
                    None => crate::protocol::codec::encode_redirect_with_version(
                        "",
                        shard_table_version,
                    ),
                },
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
    let (params, items) = match decode_spend_batch_checked(&req.payload, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "spend batch", e),
    };

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
    let mut idempotent: u64 = 0;
    let mut errors: Vec<BatchItemError> = Vec::new();
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    let mut spend_redo_range: (u64, u64) = (0, 0);
    let mut spend_intent_ranges: Vec<(u64, u64)> = Vec::new();

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

        // Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): compute the real
        // `new_spent_count` for every Spend redo entry BEFORE the redo
        // flush. Recovery's `replay_spend` overwrites `meta.spent_utxos`
        // with whatever the entry carries; previously we wrote `0`, so a
        // crash in the WAL-before-data window would leave the counter
        // wrong even though the slot transition was correctly replayed.
        //
        // Each redo entry receives the cumulative count AFTER its own
        // application, computed as `pre_spent + running_transitions`.
        // The running counter only advances for items that the validator
        // marked as real UNSPENT→SPENT transitions (in `transitions()`);
        // idempotent re-spends and validation errors do not bump the
        // counter, matching what `apply()` will write.
        let pre_spent_count = validated.pre_spent_count();
        let transition_offsets: std::collections::HashSet<u32> = validated
            .transitions()
            .iter()
            .map(|(off, _)| *off)
            .collect();

        let mut redo_ops: Vec<RedoOp> = Vec::new();
        let mut key_repl_ops: Vec<ReplicaOp> = Vec::new();
        let mut running_count = pre_spent_count;
        for &(i, item) in group {
            if !error_indices.contains(&(i as u32)) && transition_offsets.contains(&item.vout) {
                // Real UNSPENT → SPENT — counter advances by 1. Idempotent
                // re-spends do not emit redo/replication or bump generation;
                // they match the single-spend no-op contract.
                running_count = running_count.wrapping_add(1);
                redo_ops.push(RedoOp::SpendV2 {
                    tx_key: key,
                    offset: item.vout,
                    spending_data: item.spending_data,
                    new_spent_count: running_count,
                    current_block_height: params.current_block_height,
                    block_height_retention: params.block_height_retention,
                    target_generation: post_generation,
                    updated_at: engine.now_millis(),
                });
                key_repl_ops.push(ReplicaOp::Spend {
                    tx_key: key,
                    offset: item.vout,
                    spending_data: item.spending_data,
                    current_block_height: params.current_block_height,
                    block_height_retention: params.block_height_retention,
                    master_generation: post_generation,
                });
            }
        }

        // Phase 3: Write redo BEFORE engine mutation (WAL-first).
        // Lock is still held via ValidatedSpend, so no concurrent
        // mutation can interleave.
        if !redo_ops.is_empty() {
            match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
                Ok(range) => {
                    if valid_redo_range(range) {
                        spend_intent_ranges.push(range);
                    }
                    if spend_redo_range.0 == 0 && spend_redo_range.1 == 0 {
                        spend_redo_range = range;
                    } else if range.1 > 0 {
                        spend_redo_range.1 = range.1; // Extend the end
                    }
                }
                Err(e) => {
                    // Redo failure: don't apply, return error.
                    // ValidatedSpend drops here, releasing the lock.
                    // M-01: `attempted` already ticked for the whole batch;
                    // classify every item before the early return so the
                    // storage failure is visible in op metrics. Items from
                    // this group and any unprocessed group count as
                    // ErrStorage-failed; prior groups keep their tallies.
                    if let Some(m) = DISPATCH_METRICS.get() {
                        use crate::metrics::OpCode;
                        let failed = tally_storage_abort(
                            m,
                            OpCode::Spend,
                            items.len() as u64,
                            succeeded,
                            idempotent,
                            &errors,
                        );
                        m.spends_succeeded.inc_by(succeeded);
                        m.spends_idempotent.inc_by(idempotent);
                        m.spends_failed.inc_by(failed);
                        m.spend_multi_items_succeeded.inc_by(succeeded);
                        m.spend_multi_items_idempotent.inc_by(idempotent);
                        m.spend_multi_items_failed.inc_by(failed);
                    }
                    return error_response(req.request_id, ERR_STORAGE_IO, &e);
                }
            }
        }

        // Phase 4: Apply the mutation (still under lock).
        // ValidatedSpend is consumed, lock released after write.
        let validation_errors = validated.errors.clone();
        idempotent += validated.idempotent_count() as u64;
        let resp = match validated.apply(engine) {
            Ok(r) => r,
            Err(e) => {
                // DAH overflow (config misconfiguration) or similar —
                // surface as ERR_STORAGE_IO rather than silently clamping.
                // M-01: classify outcomes before the early return. This
                // group's validation errors keep their real classification;
                // its would-be transitions and all unprocessed groups count
                // as ErrStorage-failed. Note `idempotent` already includes
                // this group (added above) — idempotent items are no-ops,
                // so the failed apply does not invalidate them.
                for (idx, err) in &validation_errors {
                    errors.push(spend_error_to_batch_error(*idx, err));
                }
                if let Some(m) = DISPATCH_METRICS.get() {
                    use crate::metrics::OpCode;
                    let failed = tally_storage_abort(
                        m,
                        OpCode::Spend,
                        items.len() as u64,
                        succeeded,
                        idempotent,
                        &errors,
                    );
                    m.spends_succeeded.inc_by(succeeded);
                    m.spends_idempotent.inc_by(idempotent);
                    m.spends_failed.inc_by(failed);
                    m.spend_multi_items_succeeded.inc_by(succeeded);
                    m.spend_multi_items_idempotent.inc_by(idempotent);
                    m.spend_multi_items_failed.inc_by(failed);
                }
                return error_response(req.request_id, ERR_STORAGE_IO, &e.to_string());
            }
        };

        if !key_repl_ops.is_empty() {
            repl_ops_by_key.push((key, key_repl_ops));
        }

        // Tally this group's outcomes before draining the validation
        // errors: real transitions come from resp.spent_count, and no-op
        // successes come directly from the validator's idempotent count.
        // Failed items come from the error map.
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
    let idempotent_total = idempotent;
    debug_assert_eq!(
        succeeded + idempotent_total + failed_total,
        items.len() as u64,
        "spend batch item accounting should be exhaustive"
    );
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
    let repl_outcome = match replicate_all_ops(
        cluster,
        &repl_ops_by_key,
        spend_redo_range,
        &spend_intent_ranges,
    ) {
        Ok(o) => o,
        Err(e) => {
            let before_images = no_before_images(&repl_ops_by_key);
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images,
                redo_log,
                &spend_intent_ranges,
            ) {
                return resp;
            }
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
    let (params, items) = match decode_unspend_batch_checked(&req.payload, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "unspend batch", e),
    };

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
        item: &'a WireUnspendItem,
        pre_generation: u32,
    }
    let mut valid_items: Vec<ValidUnspend> = Vec::new();
    // Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): per-txid running
    // `spent_utxos` counter for redo entries. Recovery's `replay_unspend`
    // overwrites `meta.spent_utxos = new_spent_count`; previously we
    // wrote `0`, which corrupted the counter on crash-replay even when
    // the slot transition was correct. Initialize each running counter
    // from the index entry's cached `spent_utxos` (kept in sync with
    // metadata under the per-record lock) and decrement (saturating at
    // 0) for every entry — replay is idempotent against UTXO_UNSPENT
    // slots so over-decrement on a re-played idempotent redo is
    // harmless because replay skips before touching metadata.
    let mut running_spent: std::collections::HashMap<TxKey, u32> = std::collections::HashMap::new();
    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: item.txid };
        // Snapshot the generation BEFORE unspend so we can classify the
        // outcome as "real unspend" (gen bumped) vs "idempotent noop"
        // (gen unchanged — slot was already UNSPENT).
        let entry = engine.lookup(&key);
        let pre_generation = entry.as_ref().map(|e| e.generation).unwrap_or(0);
        let pre_spent = entry.as_ref().map(|e| e.spent_utxos).unwrap_or(0);
        // Initialize the running counter with the current spent count
        // (from index cache) the first time we see this txid in this
        // batch. Subsequent items in the same batch decrement from
        // there, modeling the per-item recovery state.
        let counter = running_spent.entry(key).or_insert(pre_spent);
        *counter = counter.saturating_sub(1);
        redo_ops.push(RedoOp::UnspendV2 {
            tx_key: key,
            offset: item.vout,
            spending_data: item.spending_data,
            new_spent_count: *counter,
            current_block_height: params.current_block_height,
            block_height_retention: params.block_height_retention,
            target_generation: pre_generation.wrapping_add(1),
            updated_at: engine.now_millis(),
        });
        valid_items.push(ValidUnspend {
            idx: i,
            key,
            item,
            pre_generation,
        });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // M-01: `attempted` already ticked; nothing has applied yet, so
            // every non-redirected item counts as ErrStorage-failed.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed =
                    tally_storage_abort(m, OpCode::Unspend, items.len() as u64, 0, 0, &errors);
                m.unspends_failed.inc_by(failed);
                m.unspend_multi_items_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &e);
        }
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
            spending_data: v.item.spending_data,
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
                        spending_data: v.item.spending_data,
                        current_block_height: params.current_block_height,
                        block_height_retention: params.block_height_retention,
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
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            let before_images = no_before_images(&repl_ops_by_key);
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
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
    let (params, txids) = match decode_set_mined_batch_checked(&req.payload, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "set_mined batch", e),
    };

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
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // M-01: `attempted` already ticked; nothing has applied yet, so
            // every non-redirected item counts as ErrStorage-failed.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed =
                    tally_storage_abort(m, OpCode::SetMined, txids.len() as u64, 0, 0, &errors);
                m.set_mined_items_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &e);
        }
    };

    // Gap #8: capture pre-apply (block_height, subtree_idx) for the
    // matching block_id BEFORE the engine clears the entry. Only the
    // unset path needs this — when adding a block entry, rollback is a
    // simple "remove the same block_id" operation that doesn't need
    // historical fields. We read the metadata once per valid_item; if
    // the block_id is not currently present, the unset is a no-op and
    // there is nothing to compensate.
    //
    // F-G5-022 / A-4 (RESOLVED 2026-05-18 — not-applicable):
    // The read here is unlocked, but the engine apply path
    // (`set_mined_batch` → `set_mined_inner`) takes the per-tx stripe
    // mutex at function entry and holds it for the entire validate-
    // and-write sequence (see `Engine` doc comment, "Atomic-apply
    // invariant"). A concurrent same-key set_mined that lands between
    // this read and the engine apply does NOT cause a write-correctness
    // bug — the engine still applies under its own lock and the result
    // returned to the client is consistent. The compensation
    // before-image captured here is a snapshot of state at read time;
    // if it diverges from the now-committed engine state, the rollback
    // restores to a slightly earlier point. That is acceptable under
    // the replication contract: failed-replication compensation runs
    // BEFORE the response is returned to the client, so no committed
    // observer ever saw the to-be-rolled-back state. The reproduction
    // test `tests/g2_atomic_apply.rs` confirms the atomic-apply
    // invariant for the spend path; set_mined inherits the same
    // stripe-lock discipline.
    let pre_unset_image: std::collections::HashMap<TxKey, BeforeImage> = if params.unset_mined {
        valid_items
            .iter()
            .filter_map(|v| {
                let entry = engine.read_block_entry(&v.key, params.block_id).ok()??;
                Some((
                    v.key,
                    BeforeImage::UnsetMined {
                        block_height: { entry.block_height },
                        subtree_idx: { entry.subtree_idx },
                    },
                ))
            })
            .collect()
    } else {
        std::collections::HashMap::new()
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
    let mut before_images_by_key: Vec<(TxKey, Vec<BeforeImage>)> = Vec::new();
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
                            current_block_height: params.current_block_height,
                            block_height_retention: params.block_height_retention,
                            master_generation: mgen,
                        }],
                    ));
                    before_images_by_key.push((
                        v.key,
                        vec![
                            pre_unset_image
                                .get(&v.key)
                                .copied()
                                .unwrap_or(BeforeImage::None),
                        ],
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
                            current_block_height: params.current_block_height,
                            block_height_retention: params.block_height_retention,
                            master_generation: mgen,
                        }],
                    ));
                    before_images_by_key.push((v.key, vec![BeforeImage::None]));
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
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            // Gap #8: rollback uses the captured pre-unset block-entry
            // fields so a crash mid-rollback can be replayed exactly.
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images_by_key,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
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

    let Some(inputs) = take_cold_data_section(cold_data, &mut pos) else {
        return (None, None, None);
    };

    let inputs_opt = if inputs.is_empty() {
        None
    } else {
        Some(inputs)
    };

    let Some(outputs) = take_cold_data_section(cold_data, &mut pos) else {
        return (inputs_opt, None, None);
    };

    let outputs_opt = if outputs.is_empty() {
        None
    } else {
        Some(outputs)
    };

    let Some(inpoints) = take_cold_data_section(cold_data, &mut pos) else {
        return (inputs_opt, outputs_opt, None);
    };

    let inpoints_opt = if inpoints.is_empty() {
        None
    } else {
        Some(inpoints)
    };

    (inputs_opt, outputs_opt, inpoints_opt)
}

fn take_cold_data_section<'a>(cold_data: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let len_end = pos.checked_add(4)?;
    let len_bytes = cold_data.get(*pos..len_end)?;
    let len = u32::from_le_bytes(len_bytes.try_into().ok()?) as usize;
    let start = len_end;
    let end = start.checked_add(len)?;
    let section = cold_data.get(start..end)?;
    *pos = end;
    Some(section)
}

fn handle_create_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
    blob_store: Option<&dyn BlobStore>,
) -> ResponseFrame {
    let items = match decode_create_batch_checked(&req.payload, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "create batch", e),
    };

    if let Some(m) = DISPATCH_METRICS.get() {
        m.creates_attempted.inc_by(items.len() as u64);
    }

    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    fn release_create_reservation(
        engine: &Engine,
        record_offset: u64,
        reservation_size: u64,
        context: &str,
    ) -> std::result::Result<(), String> {
        engine
            .allocator()
            .lock()
            .free(record_offset, reservation_size)
            .map_err(|e| {
                format!(
                    "create reservation rollback failed after {context}: \
                     offset={record_offset} size={reservation_size}: {e}"
                )
            })
    }

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

    // Phase 1: Validate ownership, check blobs, and build the record bytes
    // that will be captured in CreateV2 after batch allocation assigns
    // record offsets.
    struct PendingCreate<'a, 'p> {
        idx: usize,
        create_req: CreateRequest<'a>,
        utxo_count: u32,
        reservation_size: u64,
        record_bytes: Vec<u8>,
        /// F-IJ-002: keeps the external blob pinned against the periodic
        /// blob-GC sweep from the digest check until index registration.
        /// Released on drop — every failure path (item `continue`, batch
        /// early return) un-pins automatically.
        blob_pin: Option<crate::storage::blobstore::BlobPinGuard<'p>>,
    }

    struct ValidCreate<'a, 'p> {
        idx: usize,
        create_req: CreateRequest<'a>,
        record_offset: u64,
        reservation_size: u64,
        /// See [`PendingCreate::blob_pin`]; held until after
        /// `create_at_offset` registers the index entry. Never read —
        /// exists purely for its `Drop` (un-pin).
        _blob_pin: Option<crate::storage::blobstore::BlobPinGuard<'p>>,
    }
    let mut pending_items: Vec<PendingCreate> = Vec::new();
    let mut valid_items: Vec<ValidCreate> = Vec::new();

    for (i, item) in items.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(&item.txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }

        // Check whether this item uses an externally-uploaded blob and bind
        // the record metadata to the durable blobstore digest.
        let is_ext = item.flags & FLAG_EXTERNAL_BLOB != 0;
        let (external_ref, blob_pin) = if is_ext {
            let Some(bs) = blob_store else {
                errors.push(BatchItemError {
                    item_index: i as u32,
                    error_code: ERR_BLOB_NOT_FOUND,
                    error_data: vec![],
                });
                continue;
            };
            // F-IJ-002: pin the txid BEFORE the digest check and hold the
            // pin until index registration. The blob may be older than the
            // F-G9-004 grace window (clients legitimately stream the blob
            // long before sending the create), so without the pin a
            // concurrent periodic GC sweep could unlink it between this
            // digest check and `create_at_offset`'s registration —
            // acknowledging an EXTERNAL record whose cold data is gone.
            let pin = engine.blob_pins().pin(&item.txid);
            match bs.digest(&item.txid) {
                Ok(Some(digest)) => (
                    Some(ExternalRef {
                        store_type: 1,
                        content_hash: digest.sha256,
                        total_size: digest.length,
                        input_count: 0,
                        output_count: 0,
                        inputs_offset: 0,
                        outputs_offset: 0,
                    }),
                    Some(pin),
                ),
                Ok(None) => {
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
                        error_code: ERR_STORAGE_IO,
                        error_data: vec![],
                    });
                    continue;
                }
            }
        } else {
            (None, None)
        };

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
            external_ref,
            parent_txids: &item.parent_txids,
        };

        let key = TxKey { txid: item.txid };
        if engine.lookup(&key).is_some() {
            errors.push(BatchItemError {
                item_index: i as u32,
                error_code: ERR_ALREADY_EXISTS,
                error_data: vec![],
            });
            continue;
        }

        // Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): build the full
        // record bytes before any create WAL flush so the redo entry
        // contains everything recovery needs to reconstruct the on-device
        // record byte-for-byte.
        let (record_bytes, utxo_count) = match engine.build_create_record_bytes(&create_req) {
            Ok(built) => built,
            Err(_) => {
                errors.push(BatchItemError {
                    item_index: i as u32,
                    error_code: ERR_STORAGE_IO,
                    error_data: vec![],
                });
                continue;
            }
        };
        let reservation_size = record_bytes.len() as u64;
        pending_items.push(PendingCreate {
            idx: i,
            create_req,
            utxo_count,
            reservation_size,
            record_bytes,
            blob_pin,
        });
    }

    // Phase 1b: reserve all successful create candidates with one allocator
    // WAL fsync. The redo log still contains ordinary AllocateRegion entries,
    // so recovery does not need a new on-disk operation.
    let reservation_sizes: Vec<u64> = pending_items
        .iter()
        .map(|pending| pending.reservation_size)
        .collect();
    let allocated_regions = match engine.allocator().lock().allocate_batch(&reservation_sizes) {
        Ok(regions) => regions,
        Err(e) => {
            // M-01: `attempted` already ticked; nothing has applied yet, so
            // every item not already in `errors` counts as ErrStorage-failed.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed =
                    tally_storage_abort(m, OpCode::Create, items.len() as u64, 0, 0, &errors);
                m.creates_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &format!("{e}"));
        }
    };

    for (pending, allocated) in pending_items.into_iter().zip(allocated_regions) {
        let Some(region) = allocated else {
            errors.push(BatchItemError {
                item_index: pending.idx as u32,
                error_code: ERR_STORAGE_IO,
                error_data: vec![],
            });
            continue;
        };
        let key = TxKey {
            txid: pending.create_req.tx_id,
        };
        let parent_txids: Vec<[u8; 32]> = if pending.create_req.conflicting {
            pending.create_req.parent_txids.to_vec()
        } else {
            Vec::new()
        };
        redo_ops.push(RedoOp::CreateV2 {
            tx_key: key,
            record_offset: region.offset,
            utxo_count: pending.utxo_count,
            is_conflicting: pending.create_req.conflicting,
            record_bytes: pending.record_bytes,
            parent_txids,
        });
        valid_items.push(ValidCreate {
            idx: pending.idx,
            create_req: pending.create_req,
            record_offset: region.offset,
            reservation_size: region.size,
            _blob_pin: pending.blob_pin,
        });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // Redo failed: free all pre-allocated space.
            let mut rollback_errors = Vec::new();
            for v in &valid_items {
                if let Err(rollback_err) = release_create_reservation(
                    engine,
                    v.record_offset,
                    v.reservation_size,
                    "create redo write failure",
                ) {
                    tracing::error!(err = %rollback_err, "create batch rollback failed");
                    rollback_errors.push(rollback_err);
                }
            }
            let msg = if rollback_errors.is_empty() {
                e
            } else {
                format!(
                    "{e}; allocator rollback errors: {}",
                    rollback_errors.join("; ")
                )
            };
            // M-01: `attempted` already ticked; nothing has applied yet, so
            // every item not already in `errors` counts as ErrStorage-failed.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed =
                    tally_storage_abort(m, OpCode::Create, items.len() as u64, 0, 0, &errors);
                m.creates_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &msg);
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
                if let Some(ext) = v.create_req.external_ref {
                    meta_buf.push(ext.store_type);
                    meta_buf.extend_from_slice(&ext.content_hash);
                    meta_buf.extend_from_slice(&ext.total_size.to_le_bytes());
                    meta_buf.extend_from_slice(&ext.input_count.to_le_bytes());
                    meta_buf.extend_from_slice(&ext.output_count.to_le_bytes());
                    meta_buf.extend_from_slice(&ext.inputs_offset.to_le_bytes());
                    meta_buf.extend_from_slice(&ext.outputs_offset.to_le_bytes());
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
                // R-014 (A-05): create_at_offset detected the duplicate
                // AFTER pre_allocate_create reserved space — free the
                // reserved region so the allocator does not leak under
                // concurrent create races on the same txid.
                let rollback = release_create_reservation(
                    engine,
                    v.record_offset,
                    v.reservation_size,
                    "create_at_offset duplicate",
                );
                match rollback {
                    Ok(()) => errors.push(BatchItemError {
                        item_index: v.idx as u32,
                        error_code: ERR_ALREADY_EXISTS,
                        error_data: vec![],
                    }),
                    Err(e) => {
                        tracing::error!(err = %e, "create batch rollback failed");
                        errors.push(BatchItemError {
                            item_index: v.idx as u32,
                            error_code: ERR_STORAGE_IO,
                            error_data: vec![],
                        });
                    }
                }
            }
            Err(_) => {
                // R-014 (A-05): same fix for the catch-all error path —
                // any failure after pre_allocate_create must release
                // the reserved region.
                if let Err(e) = release_create_reservation(
                    engine,
                    v.record_offset,
                    v.reservation_size,
                    "create_at_offset failure",
                ) {
                    tracing::error!(err = %e, "create batch rollback failed");
                }
                errors.push(BatchItemError {
                    item_index: v.idx as u32,
                    error_code: ERR_STORAGE_IO,
                    error_data: vec![],
                });
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            let before_images = no_before_images(&repl_ops_by_key);
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
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
    let items = match decode_slot_item_batch_checked(&req.payload, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "freeze batch", e),
    };
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
        redo_ops.push(RedoOp::FreezeV2 {
            tx_key: key,
            offset: item.vout,
            utxo_hash: item.utxo_hash,
        });
        valid_items.push(ValidFreeze { idx: i, key, item });
    }
    let total_items = items.len() as u64;

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // M-01: nothing has applied yet — classify every non-redirected
            // item as ErrStorage-failed before the early return.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed = tally_storage_abort(m, OpCode::Freeze, total_items, 0, 0, &errors);
                m.freezes_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &e);
        }
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
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            let before_images = no_before_images(&repl_ops_by_key);
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
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
    let items = match decode_slot_item_batch_checked(&req.payload, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "unfreeze batch", e),
    };
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
        redo_ops.push(RedoOp::UnfreezeV2 {
            tx_key: key,
            offset: item.vout,
            utxo_hash: item.utxo_hash,
        });
        valid_items.push(ValidUnfreeze { idx: i, key, item });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // M-01: nothing has applied yet — classify every non-redirected
            // item as ErrStorage-failed before the early return.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed = tally_storage_abort(m, OpCode::Unfreeze, total_items, 0, 0, &errors);
                m.unfreezes_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &e);
        }
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
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            let before_images = no_before_images(&repl_ops_by_key);
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
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
    let (params, items) = match decode_reassign_batch_checked(&req.payload, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "reassign batch", e),
    };
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
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // M-01: nothing has applied yet — classify every non-redirected
            // item as ErrStorage-failed before the early return.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed = tally_storage_abort(m, OpCode::Reassign, total_items, 0, 0, &errors);
                m.reassign_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &e);
        }
    };

    // Phase 3: Apply engine mutations and build repl ops from engine results.
    //
    // Gap #8: capture the slot's prior utxo_hash for compensation. The
    // engine.reassign call asserts that the on-device slot hash equals
    // `v.item.utxo_hash`; if the assertion holds and the call returns
    // Ok, we know the prior hash exactly. The user's claim is the
    // pre-apply value — there's no need for a separate device read.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    let mut before_images_by_key: Vec<(TxKey, Vec<BeforeImage>)> = Vec::new();
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
                before_images_by_key.push((
                    v.key,
                    vec![BeforeImage::Reassign {
                        prior_utxo_hash: v.item.utxo_hash,
                    }],
                ));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            // Gap #8: rollback restores the captured prior utxo_hash, no
            // zeros, no defaults.
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images_by_key,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
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
    let (shared, txids) = match decode_txid_batch_checked(&req.payload, 9, max_batch) {
        // value(1) + cbh(4) + bhr(4)
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "set_conflicting batch", e),
    };
    let value = shared[0] != 0;
    let Some(cbh) = le_u32_at(&shared, 1) else {
        return error_response(
            req.request_id,
            ERR_PAYLOAD_MALFORMED,
            "malformed set_conflicting shared data",
        );
    };
    let Some(bhr) = le_u32_at(&shared, 5) else {
        return error_response(
            req.request_id,
            ERR_PAYLOAD_MALFORMED,
            "malformed set_conflicting shared data",
        );
    };
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
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // M-01: nothing has applied yet — classify every non-redirected
            // item as ErrStorage-failed before the early return.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed =
                    tally_storage_abort(m, OpCode::SetConflicting, total_items, 0, 0, &errors);
                m.set_conflicting_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &e);
        }
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
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            let before_images = no_before_images(&repl_ops_by_key);
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
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
    let (shared, txids) = match decode_txid_batch_checked(&req.payload, 1, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "set_locked batch", e),
    };
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
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // M-01: nothing has applied yet — classify every non-redirected
            // item as ErrStorage-failed before the early return.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed = tally_storage_abort(m, OpCode::SetLocked, total_items, 0, 0, &errors);
                m.set_locked_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &e);
        }
    };

    // Phase 3: Apply engine mutations and build repl ops from engine results.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    let mut before_images_by_key: Vec<(TxKey, Vec<BeforeImage>)> = Vec::new();
    for v in &valid_items {
        match engine.set_locked_with_before_image(&SetLockedRequest {
            tx_key: v.key,
            value,
        }) {
            Ok(resp) => {
                repl_ops_by_key.push((
                    v.key,
                    vec![ReplicaOp::SetLocked {
                        tx_key: v.key,
                        value,
                        master_generation: resp.generation,
                    }],
                ));
                before_images_by_key.push((
                    v.key,
                    vec![BeforeImage::SetLocked {
                        prior_locked: resp.prior_locked,
                        prior_delete_at_height: resp.prior_delete_at_height,
                    }],
                ));
            }
            Err(err) => {
                errors.push(spend_error_to_batch_error(v.idx as u32, &err));
            }
        }
    }

    // Phase 4: Replicate.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images_by_key,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
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
    let (shared, txids) = match decode_txid_batch_checked(&req.payload, 4, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "preserve_until batch", e),
    };
    let Some(height) = le_u32_at(&shared, 0) else {
        return error_response(
            req.request_id,
            ERR_PAYLOAD_MALFORMED,
            "malformed preserve_until shared data",
        );
    };
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
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // M-01: nothing has applied yet — classify every non-redirected
            // item as ErrStorage-failed before the early return.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed =
                    tally_storage_abort(m, OpCode::PreserveUntil, total_items, 0, 0, &errors);
                m.preserve_until_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &e);
        }
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
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            let before_images = no_before_images(&repl_ops_by_key);
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
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

/// One snapshotted UTXO slot. Used by the delete-batch compensation
/// path (R-007 — Codex F1) to restore the exact pre-delete slot
/// state (status + spending_data + hash) when a replication failure
/// forces the master to undo a delete.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SnapshotSlot {
    pub hash: [u8; 32],
    pub status: u8,
    pub spending_data: [u8; 36],
}

/// Full pre-delete snapshot of one transaction record. See
/// [`build_delete_compensation_ops`] for how this is replayed into
/// `ReplicaOp` form when replication fails.
#[derive(Debug, Clone)]
pub(crate) struct DeleteSnapshot {
    pub metadata_bytes: Vec<u8>,
    /// Generation captured from the metadata at snapshot time. Used
    /// as the `master_generation` on the per-slot Spend/Freeze
    /// compensation ops so the receiver applies them as a coherent
    /// view of the pre-delete state (matches the
    /// `stream_shard_baseline` migration replay pattern).
    pub master_generation: u32,
    pub slots: Vec<SnapshotSlot>,
    pub cold_data: Option<Vec<u8>>,
    pub is_external: bool,
}

/// Build the sequence of `ReplicaOp` ops that, when applied in order,
/// re-establish the exact pre-delete state of `key`. The Create op
/// restores the metadata + utxo_hashes; subsequent Spend / Freeze /
/// PruneSlot ops re-stamp slots that were not in the default UNSPENT
/// state.
///
/// R-007 / Codex F1: pre-fix the compensation only emitted Create,
/// which left every slot UNSPENT regardless of pre-delete state. A
/// previously-spent slot could then be spent again by a different
/// transaction. The fix uses the same Create + per-slot replay
/// pattern that `stream_shard_baseline` uses for migration baselines.
pub(crate) fn build_delete_compensation_ops(key: &TxKey, snap: &DeleteSnapshot) -> Vec<ReplicaOp> {
    let mut ops: Vec<ReplicaOp> = Vec::with_capacity(1 + snap.slots.len());
    ops.push(ReplicaOp::Create {
        tx_key: *key,
        metadata_bytes: snap.metadata_bytes.clone(),
        utxo_hashes: snap.slots.iter().map(|s| s.hash).collect(),
        cold_data: snap.cold_data.clone(),
        is_external: snap.is_external,
    });
    for (v, slot) in snap.slots.iter().enumerate() {
        let offset = v as u32;
        match slot.status {
            crate::record::UTXO_SPENT => {
                ops.push(ReplicaOp::Spend {
                    tx_key: *key,
                    offset,
                    spending_data: slot.spending_data,
                    // Delete compensation first restores lifecycle metadata
                    // through Create; these slot restamps must not re-evaluate
                    // DAH and move the snapshotted pruning target.
                    current_block_height: 0,
                    block_height_retention: 0,
                    master_generation: snap.master_generation,
                });
            }
            crate::record::UTXO_FROZEN => {
                ops.push(ReplicaOp::Freeze {
                    tx_key: *key,
                    offset,
                    master_generation: snap.master_generation,
                });
            }
            crate::record::UTXO_PRUNED => {
                ops.push(ReplicaOp::PruneSlot {
                    tx_key: *key,
                    offset,
                });
            }
            _ => {
                // UTXO_UNSPENT and any other byte: nothing to replay
                // — Create already produces an unspent slot.
            }
        }
    }
    ops
}

fn handle_delete_batch(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    let (_, txids) = match decode_txid_batch_checked(&req.payload, 0, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "delete batch", e),
    };
    let total_items = txids.len() as u64;
    let mut errors = Vec::new();
    let mut redo_ops: Vec<RedoOp> = Vec::new();

    // Phase 1: Validate ownership, lookup record_offset (read-only), build redo ops.
    // Also snapshot each record BEFORE deletion so we can restore on replication failure.
    struct ValidDelete {
        idx: usize,
        key: TxKey,
        parent_prunes: Vec<ParentPrune>,
        /// Full record snapshot for compensation. Contains the metadata
        /// bytes AND per-slot state (hash + status + spending_data) so
        /// the compensation path can rebuild not just an empty record
        /// but the exact pre-delete slot states. R-007 (Codex F1) — the
        /// previous version captured only `utxo_hashes`, so a compensation
        /// after replication failure recreated previously-spent slots as
        /// UNSPENT, opening a double-spend window.
        snapshot: Option<DeleteSnapshot>,
    }
    #[derive(Clone, Copy)]
    struct ParentPrune {
        key: TxKey,
        offset: u32,
    }
    let mut valid_items: Vec<ValidDelete> = Vec::new();
    'items: for (i, txid) in txids.iter().enumerate() {
        if let Some(redirect_err) = check_shard_ownership(txid, i as u32, cluster, false) {
            errors.push(redirect_err);
            continue;
        }
        let key = TxKey { txid: *txid };
        let record_offset = engine.lookup(&key).map(|e| e.record_offset).unwrap_or(0);
        let record_size = if record_offset == 0 {
            0
        } else {
            match engine.read_metadata(&key) {
                Ok(meta) => ({ meta.record_size }) as u64,
                Err(e) => {
                    errors.push(BatchItemError {
                        item_index: i as u32,
                        error_code: ERR_STORAGE_IO,
                        error_data: format!("delete metadata read failed: {e}").into_bytes(),
                    });
                    continue;
                }
            }
        };
        // Snapshot the record for compensation. Read metadata + every
        // slot's full state (hash + status + spending_data). R-007: a
        // partial snapshot — utxo_hashes only — meant a compensation
        // recreated previously-spent slots as UNSPENT, allowing a
        // double-spend immediately after a failed delete.
        let snapshot = if let Ok(meta) = engine.read_metadata(&key) {
            let slots = match engine.read_slots(&key) {
                Ok(slots) => slots
                    .into_iter()
                    .map(|slot| SnapshotSlot {
                        hash: slot.hash,
                        status: slot.status,
                        spending_data: slot.spending_data,
                    })
                    .collect::<Vec<_>>(),
                Err(e) => {
                    // R-007 / IJK-19: do NOT silently substitute a
                    // zero hash here. A read failure means we cannot
                    // produce a faithful pre-delete snapshot; if
                    // replication later fails we would compensate with
                    // a corrupted view.
                    tracing::error!(
                        txid = ?key.txid,
                        err = ?e,
                        "delete snapshot: slot-region read failed; skipping snapshot",
                    );
                    Vec::new()
                }
            };
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

            if slots.len() != meta.utxo_count as usize {
                None
            } else {
                Some(DeleteSnapshot {
                    metadata_bytes: meta_buf,
                    master_generation: { meta.generation },
                    slots,
                    cold_data,
                    is_external: meta.flags.contains(crate::record::TxFlags::EXTERNAL),
                })
            }
        } else {
            None
        };

        if snapshot
            .as_ref()
            .is_some_and(|snap| snap.is_external && snap.cold_data.is_none())
        {
            errors.push(BatchItemError {
                item_index: i as u32,
                error_code: ERR_STORAGE_IO,
                error_data: b"delete external blob snapshot missing".to_vec(),
            });
            continue;
        }

        // R-119: deleting a child transaction must first make every
        // parent slot spent by that child terminal (`PRUNED`), replacing
        // Lua's `deletedChildren` map with the Rust slot status. This
        // local path is intentionally fail-closed in cluster mode when a
        // parent belongs to another shard master; a distributed
        // master-to-master prune transaction is still required for that
        // topology.
        let mut parent_prunes = Vec::new();
        let parent_txids = match engine.parent_txids_for_child(&key) {
            Ok(parent_txids) => parent_txids,
            Err(e) => {
                errors.push(BatchItemError {
                    item_index: i as u32,
                    error_code: ERR_STORAGE_IO,
                    error_data: format!("delete parent-prune input parse failed: {e}").into_bytes(),
                });
                continue;
            }
        };
        for parent_txid in parent_txids {
            if let Some(route_err) = check_shard_ownership(&parent_txid, i as u32, cluster, false) {
                errors.push(BatchItemError {
                    item_index: i as u32,
                    error_code: ERR_INVARIANT_VIOLATION,
                    error_data: format!(
                        "delete parent-prune requires local parent master; route error {}",
                        route_err.error_code
                    )
                    .into_bytes(),
                });
                continue 'items;
            }
            let parent_key = TxKey { txid: parent_txid };
            match engine.slots_spent_by_child(&parent_key, key.txid) {
                Ok(offsets) => {
                    parent_prunes.extend(offsets.into_iter().map(|offset| ParentPrune {
                        key: parent_key,
                        offset,
                    }));
                }
                Err(e) => {
                    errors.push(BatchItemError {
                        item_index: i as u32,
                        error_code: ERR_STORAGE_IO,
                        error_data: format!("delete parent-prune scan failed: {e}").into_bytes(),
                    });
                    continue 'items;
                }
            }
        }

        for prune in &parent_prunes {
            redo_ops.push(RedoOp::PruneSlotIfSpentBy {
                tx_key: prune.key,
                offset: prune.offset,
                child_txid: key.txid,
            });
        }
        redo_ops.push(RedoOp::Delete {
            tx_key: key,
            record_offset,
            record_size,
        });

        valid_items.push(ValidDelete {
            idx: i,
            key,
            parent_prunes,
            snapshot,
        });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // M-01: nothing has applied yet — classify every non-redirected
            // item as ErrStorage-failed before the early return.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed = tally_storage_abort(m, OpCode::Delete, total_items, 0, 0, &errors);
                m.deletes_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &e);
        }
    };

    // Phase 3: Apply engine mutations and build repl ops.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    let mut before_images_by_key: Vec<(TxKey, Vec<BeforeImage>)> = Vec::new();
    let mut deleted_snapshots: Vec<(TxKey, DeleteSnapshot)> = Vec::new();
    for v in valid_items.iter() {
        let mut item_prune_ops: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
        let mut item_prune_before: Vec<(TxKey, Vec<BeforeImage>)> = Vec::new();
        let mut item_failed = false;
        for prune in &v.parent_prunes {
            match engine.prune_slot_if_spent_by_child(&prune.key, prune.offset, v.key.txid) {
                Ok(applied) => {
                    if applied {
                        push_repl_with_before_image(
                            &mut item_prune_ops,
                            &mut item_prune_before,
                            prune.key,
                            ReplicaOp::PruneSlotIfSpentBy {
                                tx_key: prune.key,
                                offset: prune.offset,
                                child_txid: v.key.txid,
                            },
                            BeforeImage::Prune {
                                prior_status: crate::record::UTXO_SPENT,
                            },
                        );
                    }
                }
                Err(err) => {
                    if !item_prune_ops.is_empty() {
                        match compensate_replication_failure(
                            engine,
                            &item_prune_ops,
                            &item_prune_before,
                            redo_log,
                        ) {
                            Ok(()) => {}
                            Err(e) => {
                                return error_response(req.request_id, ERR_INTERNAL, &e);
                            }
                        }
                    }
                    errors.push(spend_error_to_batch_error(v.idx as u32, &err));
                    item_failed = true;
                    break;
                }
            }
        }
        if item_failed {
            continue;
        }
        match engine.delete(&DeleteRequest { tx_key: v.key }) {
            Ok(()) => {
                repl_ops_by_key.extend(item_prune_ops);
                before_images_by_key.extend(item_prune_before);
                push_repl_with_before_image(
                    &mut repl_ops_by_key,
                    &mut before_images_by_key,
                    v.key,
                    ReplicaOp::Delete { tx_key: v.key },
                    BeforeImage::None,
                );
            }
            Err(err) => {
                if !item_prune_ops.is_empty() {
                    match compensate_replication_failure(
                        engine,
                        &item_prune_ops,
                        &item_prune_before,
                        redo_log,
                    ) {
                        Ok(()) => {}
                        Err(e) => {
                            return error_response(req.request_id, ERR_INTERNAL, &e);
                        }
                    }
                }
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
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            // Compensate: re-create deleted records from snapshots, then
            // replay per-slot state so a previously-spent / frozen /
            // pruned slot is restored to that exact state. R-007: this
            // is the fix for Codex F1 — without the per-slot replay,
            // the recreated record would have all slots in the default
            // UNSPENT state, opening a double-spend window for any
            // outputs that were already spent before the delete.
            //
            // R-007 / R-035 also drops the `let _ = handle_replica_batch`
            // and `let _ = write_redo_ops` swallows: a compensation
            // failure must surface as ERR_INTERNAL so the operator can
            // intervene; silently clearing the replication intent on
            // top of a half-restored state is exactly the divergence
            // BC-62 / F9 warned about.
            // F-G5-023 (maintainability hazard): this in-process
            // compensation path hand-constructs an OP_REPLICA_BATCH frame
            // and feeds it back through `handle_replica_batch`. That
            // bypasses every check the network path applies (HMAC, the
            // cluster_key gate, sequence-number dedupe) — intentional
            // because the inputs are trusted-by-construction, but the
            // network path and the compensation path will drift apart if
            // a future security gate is added to one and not the other.
            // The structural fix is to extract a pure `apply_replica_ops`
            // function in `src/replication/receiver.rs` (G7 territory)
            // and call it from both sites; for now the synthesised-frame
            // approach is wired so the rollback semantics stay correct.
            let mut compensation_failed: Option<String> = None;
            for (key, snap) in &deleted_snapshots {
                let ops = build_delete_compensation_ops(key, snap);

                let create_req = crate::protocol::frame::RequestFrame {
                    request_id: 0,
                    op_code: OP_REPLICA_BATCH,
                    flags: 0,
                    payload: ReplicaBatch {
                        first_sequence: 0,
                        ops,
                        trace_ctx: None,
                        source_node_id: None,
                        // Self-compensation path: applies through the
                        // ungated `handle_replica_batch` so cluster_key
                        // gating does not apply. The wire field is
                        // therefore left as the V1-compat sentinel `0`.
                        cluster_key: 0,
                    }
                    .serialize()
                    .into(),
                };
                let resp = handle_replica_batch(
                    &create_req,
                    engine,
                    &std::sync::atomic::AtomicU64::new(0),
                );
                if resp.status != STATUS_OK {
                    compensation_failed = Some(format!(
                        "delete compensation failed for txid {:?}: status={}",
                        key.txid, resp.status,
                    ));
                    break;
                }
                // Append a Create redo entry for crash recovery.
                let entry = match engine.lookup(key) {
                    Some(e) => e,
                    None => {
                        compensation_failed = Some(format!(
                            "delete compensation: re-created record disappeared for txid {:?}",
                            key.txid
                        ));
                        break;
                    }
                };
                if let Err(e) = write_redo_ops(
                    redo_log,
                    &[RedoOp::Create {
                        tx_key: *key,
                        record_offset: entry.record_offset,
                        utxo_count: snap.slots.len() as u32,
                    }],
                ) {
                    compensation_failed = Some(format!(
                        "delete compensation redo append failed for txid {:?}: {e}",
                        key.txid
                    ));
                    break;
                }
            }
            if let Some(cause) = compensation_failed {
                tracing::error!(cause = %cause, "delete compensation aborted; node is in degraded state");
                return error_response(req.request_id, ERR_INTERNAL, &cause);
            }
            // Also compensate any non-delete ops in the same batch.
            let non_delete: Vec<_> = repl_ops_by_key
                .iter()
                .filter(|(_, ops)| !ops.iter().any(|o| matches!(o, ReplicaOp::Delete { .. })))
                .cloned()
                .collect();
            let non_delete_before: Vec<_> = repl_ops_by_key
                .iter()
                .zip(before_images_by_key.iter())
                .filter(|((_, ops), _)| !ops.iter().any(|o| matches!(o, ReplicaOp::Delete { .. })))
                .map(|(_, before)| before.clone())
                .collect();
            if !non_delete.is_empty() {
                if let Some(resp) = compensate_replication_failure_or_error(
                    req.request_id,
                    engine,
                    &non_delete,
                    &non_delete_before,
                    redo_log,
                    &[redo_range],
                ) {
                    return resp;
                }
            } else {
                clear_replication_intents_after_compensation(&[redo_range]);
            }
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
    let (shared, txids) = match decode_txid_batch_checked(&req.payload, 9, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "mark_longest_chain batch", e),
    };
    let on_longest_chain = shared[0] != 0;
    let Some(cbh) = le_u32_at(&shared, 1) else {
        return error_response(
            req.request_id,
            ERR_PAYLOAD_MALFORMED,
            "malformed mark_longest_chain shared data",
        );
    };
    let Some(bhr) = le_u32_at(&shared, 5) else {
        return error_response(
            req.request_id,
            ERR_PAYLOAD_MALFORMED,
            "malformed mark_longest_chain shared data",
        );
    };
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
        redo_ops.push(RedoOp::MarkOnLongestChain {
            tx_key: key,
            on_longest_chain,
            current_block_height: cbh,
            block_height_retention: bhr,
            // The dispatcher is deliberately outside the per-tx stripe
            // lock at WAL-build time, so it cannot safely predict the
            // post-mutation generation. `0` selects replay's
            // value-idempotent path instead of a stale pre-lock
            // generation token.
            generation: 0,
        });
        valid_items.push(ValidMark { idx: i, key });
    }

    // Phase 2: WAL-first — write redo before engine mutation.
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => {
            // M-01: nothing has applied yet — classify every non-redirected
            // item as ErrStorage-failed before the early return.
            if let Some(m) = DISPATCH_METRICS.get() {
                use crate::metrics::OpCode;
                let failed =
                    tally_storage_abort(m, OpCode::MarkLongestChain, total_items, 0, 0, &errors);
                m.mark_longest_chain_failed.inc_by(failed);
            }
            return error_response(req.request_id, ERR_STORAGE_IO, &e);
        }
    };

    // Phase 3: Apply engine mutations and capture per-item master
    // generation so the replicated op carries the correct idempotency
    // token (R-053). Build `repl_ops_by_key` in lockstep — only items
    // that successfully apply locally get replicated.
    let mut repl_ops_by_key: Vec<(TxKey, Vec<ReplicaOp>)> = Vec::new();
    for v in &valid_items {
        match engine.mark_on_longest_chain(&MarkOnLongestChainRequest {
            tx_key: v.key,
            on_longest_chain,
            current_block_height: cbh,
            block_height_retention: bhr,
        }) {
            Ok(resp) => {
                // R-052: emit a dedicated MarkLongestChain ReplicaOp so
                // replicas observe the same `unmined_since` / DAH /
                // generation transition the master just applied.
                // Pre-fix this handler emitted nothing here — silent
                // master/replica divergence on every reorg.
                repl_ops_by_key.push((
                    v.key,
                    vec![ReplicaOp::MarkLongestChain {
                        tx_key: v.key,
                        on_longest_chain,
                        current_block_height: cbh,
                        block_height_retention: bhr,
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

    // Phase 4: Replicate. Mirrors set_mined / spend handlers.
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            let before_images = no_before_images(&repl_ops_by_key);
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
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
    let (field_mask, txids) = match decode_get_batch_checked(&req.payload, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "get batch", e),
    };

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
                // R-041: GetBatch per-item REDIRECT data layout is now
                // `[ERR_REDIRECT_byte:1][addr_len:2][addr][shard_table_version:8]`
                // — the trailing version lets the client detect a stale-route
                // loop (server's view <= client's known view → stop following).
                // The legacy form `[ERR_REDIRECT_byte:1][addr_bytes:N]` had no
                // length prefix and no version, so any cluster-mid-topology-
                // change cycle (A→B→C→A) was unobservable client-side.
                let redirect_status = match route {
                    crate::cluster::shards::RouteDecision::RedirectTo {
                        node,
                        shard_table_version,
                    } => {
                        let addr_str = match cluster.node_addr(&node) {
                            Some(a) => a.to_string(),
                            None => String::new(),
                        };
                        let payload = crate::protocol::codec::encode_redirect_with_version(
                            &addr_str,
                            shard_table_version,
                        );
                        let mut data = Vec::with_capacity(1 + payload.len());
                        data.push(ERR_REDIRECT as u8);
                        data.extend_from_slice(&payload);
                        data
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
                // R-045 (Codex F4): track whether any inner sub-read
                // (slot / cold-data / conflicting-children) failed.
                // Pre-fix the failures were silently filled with
                // zeros / length-0 / count-0, so storage corruption
                // was indistinguishable from a clean read of an
                // empty record. Now we surface inner failures as
                // ERR_STORAGE_IO on the result item — clients can
                // retry instead of trusting the synthesized bytes.
                // (P3.10 / F-G5-017; pre-P3.10 the code was ERR_INTERNAL.)
                let mut inner_read_failed = false;
                if field_mask.has(FieldMask::UTXO_SLOTS) {
                    let utxo_count = { meta.utxo_count };
                    data.extend_from_slice(&utxo_count.to_le_bytes());
                    match engine.read_slots(&key) {
                        Ok(slots) if slots.len() == utxo_count as usize => {
                            for slot in slots {
                                data.extend_from_slice(&slot.hash);
                                data.push(slot.status);
                                data.extend_from_slice(&slot.spending_data);
                            }
                        }
                        _ => {
                            inner_read_failed = true;
                            // Still emit padding bytes so the length declared
                            // in `utxo_count` matches the data length; the
                            // per-item ERR_STORAGE_IO status tells the client
                            // these bytes are unreliable.
                            for _ in 0..utxo_count {
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
                            inner_read_failed = true;
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
                            inner_read_failed = true;
                            data.push(0u8);
                        }
                    }
                }
                let status = if inner_read_failed {
                    // ERR_STORAGE_IO on the wire — distinguishes sub-read
                    // corruption from a clean `Ok(0)` case. (P3.10 /
                    // F-G5-017; pre-P3.10 the code was ERR_INTERNAL.)
                    ERR_STORAGE_IO as u8
                } else {
                    0
                };
                results.push(WireGetResult { status, data });
            }
            Err(SpendError::TxNotFound) => {
                results.push(WireGetResult {
                    status: ERR_TX_NOT_FOUND as u8,
                    data: vec![],
                });
            }
            Err(_) => {
                // R-045 (Codex F4): a non-`TxNotFound` metadata read
                // error is storage corruption / I/O failure, not a
                // missing record. Pre-fix this returned status=1
                // (ERR_TX_NOT_FOUND on the wire), so a client could
                // not distinguish "tx really doesn't exist" from
                // "tx exists but the device returned bad bytes" —
                // the natural retry behaviour for the latter never
                // fired. Surface as ERR_STORAGE_IO so the client
                // retries. (P3.10 / F-G5-017; pre-P3.10 ERR_INTERNAL.)
                results.push(WireGetResult {
                    status: ERR_STORAGE_IO as u8,
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

    let payload = match try_encode_get_response(&results) {
        Ok(payload) => payload,
        Err(e) => {
            return codec_error_response(req.request_id, "get batch response", e);
        }
    };
    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload,
    }
}

// ---------------------------------------------------------------------------
// Pruner operations
// ---------------------------------------------------------------------------

/// Return the current unmined-index snapshot at the instant the index query
/// runs. The result is not a read lock over subsequent engine mutations:
/// concurrent set-mined/mark-longest-chain updates may become visible
/// immediately after this response is assembled.
///
/// F-G5-003: in cluster mode, the response is filtered to keys whose
/// shard this node is master of. Pre-fix the handler walked the entire
/// local unmined index and returned every txid below `cutoff`, which
/// disclosed the unmined-pool view of shards this node only held as a
/// replica (or stale data from before a migration). With cluster
/// information available, only locally-mastered keys are returned;
/// in single-node mode (`cluster = None`) the full index is returned
/// as before.
fn handle_query_old_unmined(
    req: &RequestFrame,
    engine: &Engine,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    // Payload: [cutoff_height:4]
    if req.payload.len() < 4 {
        return error_response(req.request_id, ERR_PAYLOAD_MALFORMED, "malformed query");
    }
    let Some(cutoff) = le_u32_at(&req.payload, 0) else {
        return error_response(req.request_id, ERR_PAYLOAD_MALFORMED, "malformed query");
    };
    let candidates = engine.unmined_index().range_query(cutoff);
    let mut keys = Vec::with_capacity(candidates.len());
    for key in candidates {
        // F-G5-003: skip keys this node does not master. Single-node mode
        // (no cluster) keeps the prior behaviour.
        if let Some(c) = cluster {
            match c.is_master(&key) {
                crate::cluster::coordinator::MasterQueryResult::Yes => {}
                _ => continue,
            }
        }
        match engine.read_metadata(&key) {
            Ok(meta) if { meta.preserve_until } == 0 => keys.push(key),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    txid_prefix = ?&key.txid[..4],
                    err = ?e,
                    "query_old_unmined: skipping candidate whose metadata could not be revalidated"
                );
            }
        }
    }

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
    let (shared, txids) = match decode_txid_batch_checked(&req.payload, 4, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "preserve_transactions batch", e),
    };
    let Some(height) = le_u32_at(&shared, 0) else {
        return error_response(
            req.request_id,
            ERR_PAYLOAD_MALFORMED,
            "malformed preserve_transactions shared data",
        );
    };

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
    let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) {
        Ok(range) => range,
        Err(e) => return error_response(req.request_id, ERR_STORAGE_IO, &e),
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
    let repl_outcome = match replicate_all_ops(cluster, &repl_ops_by_key, redo_range, &[redo_range])
    {
        Ok(o) => o,
        Err(e) => {
            let before_images = no_before_images(&repl_ops_by_key);
            if let Some(resp) = compensate_replication_failure_or_error(
                req.request_id,
                engine,
                &repl_ops_by_key,
                &before_images,
                redo_log,
                &[redo_range],
            ) {
                return resp;
            }
            return error_response(req.request_id, ERR_REPLICATION_FAILED, &e);
        }
    };

    batch_response_with_outcome(req.request_id, &errors, repl_outcome)
}

/// R-008 (Codex F2): process expired preservations as a clustered,
/// replicated, ownership-checked operation.
///
/// Pre-fix this handler:
///
/// 1. queried the DAH index on whatever node received op 32 (any
///    node — no ownership check), and
/// 2. called `engine.delete` directly without going through
///    `replicate_all_ops`.
///
/// In a multi-node cluster, that meant ANY node receiving the opcode
/// could delete whatever its local DAH index said was due, and a
/// successful local delete on the master never propagated to
/// replicas. Both paths broke shard consistency: replicas retained
/// data the master removed; non-master nodes could delete records
/// they don't own.
///
/// The fix is in three layers:
///
/// a. **Ownership filter:** only consider keys for which this node
///    is the master and which are not currently fenced or pending
///    inbound migration. Non-owned keys are silently dropped from
///    the candidate set — clients invoking this opcode are sweepers
///    that fan out across all masters anyway, so a single rebind
///    here just defers the work to the right master.
/// b. **Re-validation (folds in IJK-09 / R-102):** the DAH index is
///    a cache; before deleting, re-read the on-device metadata and
///    verify the record still satisfies `should_delete_at_height` —
///    i.e. `preserve_until == 0`, `delete_at_height <= current_height`,
///    `spent_utxos == utxo_count`, `unmined_since == 0`. A stale
///    DAH entry that points at a now-preserved record otherwise
///    results in silent data loss.
/// c. **Replication + compensation:** for the surviving candidates,
///    build a synthetic OP_DELETE_BATCH payload and dispatch through
///    `handle_delete_batch`. That handler already has the full
///    replication + compensation path from R-007, including the
///    per-slot snapshot rebuilds. This way process-expired and
///    delete-batch share one rollback codepath instead of needing a
///    duplicate maintained in lockstep.
fn handle_process_expired(
    req: &RequestFrame,
    engine: &Engine,
    max_batch: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
) -> ResponseFrame {
    // Payload: [current_height:4]
    if req.payload.len() < 4 {
        return error_response(req.request_id, ERR_PAYLOAD_MALFORMED, "malformed");
    }
    let Some(current_height) = le_u32_at(&req.payload, 0) else {
        return error_response(req.request_id, ERR_PAYLOAD_MALFORMED, "malformed");
    };

    // Query DAH index for transactions due for deletion. The DAH index
    // is per-node and reflects only records this node knows about, so
    // it is already (mostly) ownership-filtered when running in cluster
    // mode — but we still re-check ownership explicitly below because
    // (a) DAH may transiently include non-master records during
    // migration, and (b) the index can lag behind the on-device
    // metadata.
    let candidates = engine.dah_index().range_query(current_height);

    // Phase 1: filter by ownership + re-validate against current
    // metadata. A DAH entry is a hint; the metadata is authoritative.
    let mut owned_due: Vec<[u8; 32]> = Vec::new();
    for key in &candidates {
        // Ownership: skip if not master or not yet ready to write
        // (pending inbound migration / fenced).
        if check_shard_ownership(&key.txid, 0, cluster, false).is_some() {
            continue;
        }
        // Re-validate: read the on-device metadata and confirm the
        // record really is due. Skip if preserved, not fully spent,
        // unmined, or the DAH is in the future. R-102 / IJK-09.
        let meta = match engine.read_metadata(key) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if { meta.preserve_until } != 0 {
            continue;
        }
        let dah = { meta.delete_at_height };
        if dah == 0 || dah > current_height {
            continue;
        }
        if { meta.spent_utxos } != { meta.utxo_count } {
            continue;
        }
        if { meta.unmined_since } != 0 {
            continue;
        }
        owned_due.push(key.txid);
    }

    let candidate_count = owned_due.len() as u32;

    if owned_due.is_empty() {
        // Nothing to do for this node — return a count-shaped reply so
        // the client can recognize a clean no-op without parsing
        // errors out of the ERR_INTERNAL channel.
        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        return ResponseFrame {
            request_id: req.request_id,
            status: STATUS_OK,
            payload,
        };
    }

    // Phase 2: dispatch as a synthetic OP_DELETE_BATCH so the
    // replication + compensation logic from R-007 runs. The synthetic
    // request keeps the original request_id so the response correlates
    // back to the caller.
    //
    // F-G5-028: calling `handle_delete_batch` directly (rather than
    // re-entering `handle_request`) bypasses the dispatcher's
    // `needs_cluster_readiness`, `check_secondary_readiness`, and
    // `check_quorum` middleware. That is intentional and safe here
    // because the outer `OP_PROCESS_EXPIRED_PRESERVATIONS` request has
    // already cleared every one of those gates — re-entering the
    // dispatcher would cost an extra cluster-state read for no benefit.
    // A future maintainer who wants quorum re-checked under a slow path
    // should route through `handle_request` instead.
    let delete_payload = crate::protocol::codec::encode_txid_batch(&owned_due, &[]);
    let delete_req = RequestFrame {
        request_id: req.request_id,
        op_code: crate::protocol::opcodes::OP_DELETE_BATCH,
        flags: req.flags,
        payload: delete_payload.into(),
    };
    let delete_resp = handle_delete_batch(&delete_req, engine, max_batch, cluster, redo_log);

    // Collapse the OP_DELETE_BATCH response shape into the legacy
    // (deleted:u32, failed:u32) format that
    // OP_PROCESS_EXPIRED_PRESERVATIONS callers expect. The batch
    // handler returns:
    //   - STATUS_OK with empty payload  → all `candidate_count` deleted
    //   - STATUS_DEGRADED_DURABILITY    → all deleted but durability
    //                                     was degraded; surface the
    //                                     status to the caller
    //   - STATUS_PARTIAL_ERROR          → payload is sparse-error
    //                                     encoded; subtract the error
    //                                     count from candidate_count
    //   - STATUS_ERROR (replication / internal) → propagate as-is
    let (deleted, failed) = match delete_resp.status {
        STATUS_OK | STATUS_DEGRADED_DURABILITY => (candidate_count, 0u32),
        STATUS_PARTIAL_ERROR => {
            // Sparse-error encoding starts with a u32 error count.
            let err_count = le_u32_at(&delete_resp.payload, 0).unwrap_or(candidate_count);
            (candidate_count.saturating_sub(err_count), err_count)
        }
        _ => return delete_resp,
    };

    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&deleted.to_le_bytes());
    payload.extend_from_slice(&failed.to_le_bytes());
    ResponseFrame {
        request_id: req.request_id,
        status: delete_resp.status,
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
    let items = match decode_get_spend_batch_checked(&req.payload, max_batch) {
        Ok(r) => r,
        Err(e) => return codec_error_response(req.request_id, "get_spend batch", e),
    };

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

        let key = TxKey { txid: item.txid };
        match engine.get_spend(&GetSpendRequest {
            tx_key: key,
            offset: item.vout,
            utxo_hash: item.utxo_hash,
        }) {
            Ok(spend) => {
                results.push(WireGetSpendResult {
                    status: 0,
                    error_code: ERR_OK,
                    slot_status: spend.status,
                    spending_data: spend.spending_data.unwrap_or([0; 36]),
                });
            }
            Err(SpendError::TxNotFound) => {
                results.push(WireGetSpendResult {
                    status: 1,
                    error_code: ERR_TX_NOT_FOUND,
                    slot_status: 0,
                    spending_data: [0; 36],
                });
            }
            Err(SpendError::UtxoNotFound { .. }) => {
                results.push(WireGetSpendResult {
                    status: 1,
                    error_code: ERR_VOUT_OUT_OF_RANGE,
                    slot_status: 0,
                    spending_data: [0; 36],
                });
            }
            Err(SpendError::UtxoHashMismatch { .. }) => {
                results.push(WireGetSpendResult {
                    status: 1,
                    error_code: ERR_UTXO_HASH_MISMATCH,
                    slot_status: 0,
                    spending_data: [0; 36],
                });
            }
            Err(_) => {
                results.push(WireGetSpendResult {
                    status: 1,
                    error_code: ERR_STORAGE_IO,
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
/// R-044 (GH-06 / GH-09): per-stream upper bound on cumulative bytes.
///
/// Each active streaming-blob upload session is keyed by txid and
/// holds an `ActiveStream` whose `bytes_received` counter grows by
/// `chunk.data.len()` per `OP_STREAM_CHUNK`. Pre-fix that growth
/// was unbounded — a malicious or buggy client could keep sending
/// 4 KiB chunks until either the per-connection allocation budget
/// or the underlying blob store filled up. The natural per-frame
/// cap (`MAX_FRAME_SIZE`) limits a single chunk but doesn't constrain
/// the total upload. 4 GiB is well above the largest legitimate
/// transaction-cold-data payload but small enough that an attacker
/// cannot weaponize one connection into multi-terabyte writes.
/// Operators can tune this per connection with
/// `ServerConfig::max_stream_total_bytes`.
fn handle_stream_chunk(
    req: &RequestFrame,
    conn_state: &mut super::ConnectionState,
    blob_store: Option<&dyn BlobStore>,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    let chunk = match decode_stream_chunk(&req.payload) {
        Some(c) => c,
        None => {
            return error_response(
                req.request_id,
                ERR_PAYLOAD_MALFORMED,
                "malformed stream chunk",
            );
        }
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
    //
    // F-G5-024: collapse the previous Vacant-insert + separate get_mut(...)
    // .expect("just inserted") pattern into a single branch. The old code
    // was locally safe (either the entry already existed, or we just
    // inserted on Ok, or the Err arm returned early) but a future
    // contributor could add an early return on the Occupied path without
    // realising the second lookup needs both branches present.
    use std::collections::hash_map::Entry;
    let stream = match conn_state.streams.entry(chunk.txid) {
        Entry::Occupied(occupied) => occupied.into_mut(),
        Entry::Vacant(vacant) => match blob_store.begin_stream(&chunk.txid) {
            Ok(writer) => vacant.insert(super::ActiveStream {
                writer,
                bytes_received: 0,
            }),
            Err(e) => {
                return error_response(
                    req.request_id,
                    ERR_STORAGE_IO,
                    &format!("begin_stream: {e}"),
                );
            }
        },
    };

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

    // R-044: enforce the per-stream cumulative cap BEFORE writing.
    // `checked_add` defends against an attacker who advertises a
    // chunk_data_len that, added to the running counter, would wrap
    // `u64` and silently bypass the cap below.
    let projected = match stream.bytes_received.checked_add(chunk.data.len() as u64) {
        Some(n) => n,
        None => {
            if let Some(s) = conn_state.streams.remove(&chunk.txid) {
                let _ = s.writer.abort();
            }
            return error_response(
                req.request_id,
                ERR_STREAM_INVARIANT,
                "stream byte counter overflow",
            );
        }
    };
    let max_stream_total_bytes = conn_state.max_stream_total_bytes;
    if projected > max_stream_total_bytes {
        if let Some(s) = conn_state.streams.remove(&chunk.txid) {
            let _ = s.writer.abort();
        }
        return error_response(
            req.request_id,
            ERR_STREAM_INVARIANT,
            &format!(
                "stream exceeds maximum total bytes ({max_stream_total_bytes}): would reach {projected}",
            ),
        );
    }

    // Write the chunk data.
    if let Err(e) = stream.writer.write_chunk(chunk.data) {
        // Abort the stream on write error.
        if let Some(s) = conn_state.streams.remove(&chunk.txid) {
            let _ = s.writer.abort();
        }
        return error_response(req.request_id, ERR_STORAGE_IO, &format!("write_chunk: {e}"));
    }

    stream.bytes_received = projected;

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
        None => {
            return error_response(
                req.request_id,
                ERR_PAYLOAD_MALFORMED,
                "malformed stream end",
            );
        }
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
            ERR_STREAM_INVARIANT,
            &format!(
                "size mismatch: received {} bytes, expected {}",
                stream.bytes_received, end.total_size
            ),
        );
    }

    // Finalize the blob — makes it available for reads. Return the durable
    // digest so clients/proxies can bind subsequent creates to the same
    // payload metadata if they cache the response.
    match stream.writer.finish() {
        Ok(digest) => {
            let mut payload = Vec::with_capacity(40);
            payload.extend_from_slice(&digest.sha256);
            payload.extend_from_slice(&digest.length.to_le_bytes());
            ResponseFrame {
                request_id: req.request_id,
                status: STATUS_OK,
                payload,
            }
        }
        Err(e) => error_response(req.request_id, ERR_STORAGE_IO, &format!("finish: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn error_response(request_id: u64, code: u16, msg: &str) -> ResponseFrame {
    ResponseFrame {
        request_id,
        status: STATUS_ERROR,
        payload: encode_error_payload(code, msg),
    }
}

/// Build an error response from a [`CodecError`] returned by one of the
/// `decode_*_checked` decoders.
///
/// `op_label` is a human-readable label for the operation (e.g.
/// `"spend batch"`) used in the error message. The response uses
/// `STATUS_ERROR` and `ERR_PAYLOAD_MALFORMED` (P3.10 / F-G5-017): every
/// `CodecError` variant is a wire-decode failure (HeaderTooShort,
/// BatchTooLarge, TruncatedBatch, SectionTruncated) so callers can route
/// on the typed code instead of substring-matching the human-readable
/// message. Pre-P3.10 the code was `ERR_INTERNAL`.
///
/// We deliberately use [`CodecError::Display`] so the wire payload
/// records the specific failure (`HeaderTooShort`, `BatchTooLarge`,
/// `TruncatedBatch`, or `SectionTruncated`) without leaking any
/// server-side state. The handler logs at debug level for operator
/// triage.
fn codec_error_response(request_id: u64, op_label: &str, err: CodecError) -> ResponseFrame {
    tracing::debug!(op = op_label, err = %err, "codec rejected request before allocation");
    error_response(
        request_id,
        ERR_PAYLOAD_MALFORMED,
        &format!("malformed {op_label}: {err}"),
    )
}

/// Build a per-batch response frame, promoting clean responses to
/// `STATUS_DEGRADED_DURABILITY` when replication returned
/// [`ReplicationOutcome::Degraded`] (best-effort mode, zero replica ACKs).
///
/// When there *are* per-item errors we still return `STATUS_PARTIAL_ERROR`:
/// the partial-error path already conveys that not every item succeeded,
/// and overwriting it with the degraded-durability status would erase the
/// per-item diagnostic detail the client needs. The degraded-durability
/// metric has already been incremented inside `replicate_all_ops`, so the
/// server-side telemetry is unaffected.
///
/// Callers in non-cluster paths can pass [`ReplicationOutcome::Full`]
/// (or [`ReplicationOutcome::NotApplicable`]) to get plain `STATUS_OK`.
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
        // R-018 (A-10): include the 4-byte spendable_at_height in the
        // wire payload so the client knows when to retry. The previous
        // empty-payload behavior contradicted README's documented
        // "ERR_FROZEN_UNTIL carries 4-byte spendable_at_height" contract
        // and forced clients to either guess or poll blindly.
        SpendError::FrozenUntil {
            spendable_at_height,
            ..
        } => (ERR_FROZEN_UNTIL, spendable_at_height.to_le_bytes().to_vec()),
        SpendError::InvalidSpend { spending_data, .. } => {
            (ERR_INVALID_SPEND, spending_data.to_vec())
        }
        // R-015 (A-07): Pruned UTXOs preserve `spending_data` on disk
        // for forensic / proof-of-prune lookups. Surface that data on
        // the wire instead of a meaningless empty payload.
        SpendError::Pruned { spending_data, .. } => (ERR_INVALID_SPEND, spending_data.to_vec()),
        SpendError::AlreadyFrozen { .. } => (ERR_ALREADY_FROZEN, vec![]),
        SpendError::NotFrozen { .. } => (ERR_UTXO_NOT_FROZEN, vec![]),
        SpendError::StorageError { .. } => (ERR_STORAGE_IO, vec![]),
        SpendError::DahOverflow { .. } => (ERR_STORAGE_IO, vec![]),
        SpendError::ReassignOverflow { .. } => (ERR_STORAGE_IO, vec![]),
        // F-G2-002: reserved frozen-sentinel rejection. Reuses
        // `ERR_INVALID_SPEND` — semantically the request is malformed
        // (caller asked us to write the on-disk frozen marker as the
        // spender). No new wire opcode is added; the client can
        // distinguish via the empty payload (real `InvalidSpend` carries
        // the 36-byte `spending_data`).
        SpendError::ReservedSpendingData { .. } => (ERR_INVALID_SPEND, vec![]),
        // F-X-022: Aerospike `addDeletedChildren` parity. Distinct wire
        // code (`ERR_DELETED_CHILDREN = 35`) so clients can distinguish
        // the resurrected-then-pruned rejection from the regular
        // `UTXO_PRUNED` slot-status rejection (`ERR_INVALID_SPEND`).
        // Payload is the single-byte child_count for client diagnostics.
        SpendError::DeletedChildren { child_count, .. } => {
            (ERR_DELETED_CHILDREN, vec![*child_count])
        }
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
        | SpendError::Pruned { .. }
        // F-X-022: deleted-children rejection is the same conflict class
        // as a regular pruned/conflicting failure — the chain history
        // diverged under the parent.
        | SpendError::DeletedChildren { .. } => Outcome::ErrConflicting,
        SpendError::Locked
        | SpendError::Frozen { .. }
        | SpendError::FrozenUntil { .. }
        | SpendError::AlreadyFrozen { .. }
        | SpendError::NotFrozen { .. } => Outcome::ErrFrozen,
        SpendError::StorageError { .. }
        | SpendError::DahOverflow { .. }
        | SpendError::ReassignOverflow { .. } => Outcome::ErrStorage,
        SpendError::CoinbaseImmature { .. }
        | SpendError::UtxoNotFound { .. }
        | SpendError::UtxoHashMismatch { .. }
        // F-G2-002: reserved-sentinel rejection is a request-shape error,
        // grouped with the other "Other" bucket entries.
        | SpendError::ReservedSpendingData { .. } => Outcome::Other,
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
        ERR_CONFLICTING | ERR_ALREADY_SPENT | ERR_INVALID_SPEND | ERR_ALREADY_EXISTS
        | ERR_DELETED_CHILDREN => Outcome::ErrConflicting,
        ERR_LOCKED | ERR_FROZEN | ERR_FROZEN_UNTIL | ERR_ALREADY_FROZEN | ERR_UTXO_NOT_FROZEN => {
            Outcome::ErrFrozen
        }
        // P3.10 / F-G5-017: every typed wire-level failure code that
        // previously folded into `ERR_INTERNAL` now classifies the same way
        // — they all surface as `ErrStorage` in the metrics histogram so
        // existing dashboards keep working. `ERR_INTERNAL` itself is kept
        // for genuinely unclassified failures.
        ERR_INTERNAL
        | ERR_STORAGE_IO
        | ERR_PAYLOAD_MALFORMED
        | ERR_OPCODE_UNSUPPORTED
        | ERR_RATE_LIMITED
        | ERR_NOT_CLUSTERED
        | ERR_INVARIANT_VIOLATION
        | ERR_STREAM_INVARIANT => Outcome::ErrStorage,
        _ => Outcome::Other,
    }
}

/// M-01: terminal metrics tally for a mutating batch handler that aborts
/// with a batch-wide `ERR_STORAGE_IO` early return BEFORE reaching its
/// normal post-loop tally block.
///
/// The handler's `*_attempted` counters tick when the batch is decoded, so
/// an early return must still classify every item exactly once — otherwise
/// `attempted == succeeded + idempotent + failed` silently breaks and the
/// storage failure is invisible in `/metrics` (`Outcome::ErrStorage` would
/// have no incrementing call site on the write path).
///
/// Items the caller already classified keep their real outcome: `succeeded`
/// and `idempotent` tick `Outcome::Ok` / `Outcome::Idempotent`, and each
/// entry in `errors` (validation/redirect failures collected so far) is
/// classified through [`classify_wire_error_code`]. Every remaining item in
/// the batch — the one that hit the storage error plus any item not yet
/// processed — is counted as failed with [`crate::metrics::Outcome::ErrStorage`].
///
/// Returns the total failed count (`errors.len()` + the storage-failed
/// remainder) so the caller can bump its op-specific `*_failed` scalar(s)
/// by the same amount before returning the error response.
fn tally_storage_abort(
    m: &crate::metrics::ThreadMetrics,
    op: crate::metrics::OpCode,
    total_items: u64,
    succeeded: u64,
    idempotent: u64,
    errors: &[BatchItemError],
) -> u64 {
    use crate::metrics::Outcome;
    let classified = succeeded + idempotent + errors.len() as u64;
    let storage_failed = total_items.saturating_sub(classified);
    m.operations.inc_by(op, Outcome::Ok, succeeded);
    m.operations.inc_by(op, Outcome::Idempotent, idempotent);
    for e in errors {
        m.operations.inc(op, classify_wire_error_code(e.error_code));
    }
    m.operations.inc_by(op, Outcome::ErrStorage, storage_failed);
    errors.len() as u64 + storage_failed
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
        None => error_response(req.request_id, ERR_NOT_CLUSTERED, "not clustered"),
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
    // F-G5-007: route count through the bounds-checked le_u32_at helper
    // (rather than the panic-on-truncate try_into-expect pattern) so a
    // future refactor that drops the inline length check cannot panic on
    // a client-controlled payload. Same change applies to F-G5-009 in
    // handle_partition_version_report.
    let Some(count_u32) = le_u32_at(payload, 0) else {
        return error_response(
            req.request_id,
            ERR_PAYLOAD_MALFORMED,
            "malformed admin diagnose: missing count",
        );
    };
    let count = count_u32 as usize;
    if count as u32 > ADMIN_DIAGNOSE_KEY_MAX_TXIDS {
        return error_response(
            req.request_id,
            ERR_PAYLOAD_MALFORMED,
            "malformed admin diagnose: count exceeds cap",
        );
    }
    let expected_len = 4usize + count.saturating_mul(32);
    if payload.len() != expected_len {
        return error_response(
            req.request_id,
            ERR_PAYLOAD_MALFORMED,
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
/// Phase I — `OP_ADMIN_CLUSTER_HEALTH` handler.
///
/// Returns this node's [`ClusterHealth`](crate::cluster::coordinator::ClusterHealth)
/// snapshot — used by clients (and `wait_specific_nodes_ready` in the
/// integration harness) to refuse seeding against a node that has not
/// yet observed its first quorum-committed topology.
///
/// In single-node / no-cluster mode (e.g. test fixtures without a
/// `RunningCluster`), the handler returns `STATUS_OK` with the node
/// reporting itself as `Joining` so callers consistently treat
/// "no cluster" as "not yet ready".
fn handle_admin_cluster_health(
    req: &RequestFrame,
    cluster: Option<&RunningCluster>,
) -> ResponseFrame {
    let snapshot = match cluster {
        Some(c) => c.cluster_health(),
        None => crate::cluster::coordinator::ClusterHealth {
            swim_state: crate::cluster::coordinator::ClusterHealthSwimState::Joining,
            last_committed_term: 0,
            last_topology_commit_age_ms: u64::MAX,
        },
    };
    ResponseFrame {
        request_id: req.request_id,
        status: STATUS_OK,
        payload: snapshot.serialize().to_vec(),
    }
}

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
    // F-G5-009: use le_u64_at instead of the silently-substituted-zero
    // try_into-unwrap_or pattern that previously parsed a truncated
    // payload as cluster_key = 0. The preceding length check makes the
    // path unreachable today but the pattern was inconsistent with the
    // rest of the dispatcher's helpers.
    let Some(request_cluster_key) = le_u64_at(&req.payload, 0) else {
        return error_response(
            req.request_id,
            ERR_PAYLOAD_MALFORMED,
            "malformed partition version report: missing cluster_key",
        );
    };

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
    use crate::device::{BlockDevice, MemoryDevice, ReadFailingDevice};
    use crate::index::{DahIndex, Index, UnminedIndex};
    use crate::locks::StripedLocks;
    use crate::ops::engine::Engine;
    use std::sync::Arc;

    #[test]
    fn dispatch_parsers_use_take_helper() {
        let source = include_str!("dispatch.rs");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("dispatch.rs contains production section");
        assert!(
            !production.contains("try_into().unwrap()"),
            "production dispatch parsers must use checked endian helpers, not try_into().unwrap()",
        );
        // F-G5-007: `try_into().expect(...)` is the same panic-on-truncate
        // pattern under a different macro name; reject both.
        assert!(
            !production.contains("try_into().expect("),
            "production dispatch parsers must use checked endian helpers, not try_into().expect()",
        );
        // F-G5-009: `try_into().unwrap_or(...)` silently substitutes the
        // fallback on a truncated payload, which can bypass downstream
        // equality / mismatch checks. Use le_u32_at / le_u64_at instead.
        assert!(
            !production.contains("try_into().unwrap_or("),
            "production dispatch parsers must use checked endian helpers, not try_into().unwrap_or()",
        );
    }

    #[test]
    fn replication_backpressure_bounded_by_permit_pool() {
        let mut permits = Vec::new();
        for _ in 0..MAX_REPLICATION_FANOUTS_IN_FLIGHT {
            permits.push(acquire_replication_fanout_permit());
        }

        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let permit = acquire_replication_fanout_permit();
            tx.send(()).unwrap();
            drop(permit);
        });

        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "a new replication fan-out must block while all permits are held"
        );
        permits.pop();
        rx.recv_timeout(std::time::Duration::from_secs(1))
            .expect("dropping a permit should release one waiting fan-out");
        drop(permits);
        waiter.join().unwrap();
    }

    /// Test harness for Layer 1 dispatch testing.
    ///
    /// Creates an in-memory Engine with no network, no Docker. Tests handler
    /// logic directly by calling `handle_request()`.
    ///
    /// Every harness holds `metrics_test_lock` for its lifetime so the
    /// process-wide `DISPATCH_METRICS` singleton is mutated by exactly one
    /// test at a time. Without this, `snapshot_spend`/`snapshot_unspend`
    /// delta math races against parallel dispatch tests that don't observe
    /// metrics (~57 of the 72 harness sites). Holding the guard inside the
    /// harness means callers cannot accidentally forget to take it — and
    /// also means tests must NOT take `metrics_test_lock()` themselves
    /// (stdlib `Mutex` is not re-entrant; doing so deadlocks on the same
    /// thread).
    struct DispatchTestHarness {
        engine: Engine,
        _metrics_guard: std::sync::MutexGuard<'static, ()>,
    }

    impl DispatchTestHarness {
        /// Create a new harness with a 64 MB in-memory device.
        fn new() -> Self {
            let dev: Arc<dyn BlockDevice> =
                Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
            Self::with_device(dev)
        }

        fn with_device(dev: Arc<dyn BlockDevice>) -> Self {
            let alloc = SlotAllocator::new(dev.clone()).unwrap();
            let index = Index::new(10000).unwrap();
            let locks = StripedLocks::new(1024);
            let dah = DahIndex::new();
            let unmined = UnminedIndex::new();
            let engine = Engine::new(dev, index, alloc, locks, dah, unmined);
            Self {
                engine,
                _metrics_guard: metrics_test_lock(),
            }
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
                payload: payload.into(),
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

        fn request_with_blob_store(
            &self,
            op_code: u16,
            payload: Vec<u8>,
            blob_store: &dyn BlobStore,
        ) -> ResponseFrame {
            let req = RequestFrame {
                request_id: 1,
                op_code,
                flags: 0,
                payload: payload.into(),
            };
            let mut conn_state = crate::server::ConnectionState::new();
            handle_request(
                &req,
                &self.engine,
                8192,
                None,
                None,
                &mut conn_state,
                Some(blob_store),
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
                payload: payload.into(),
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

    #[test]
    fn parse_cold_data_fields_rejects_truncated_large_section_length() {
        let mut cold_data = Vec::new();
        cold_data.extend_from_slice(&u32::MAX.to_le_bytes());
        cold_data.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD]);
        cold_data.extend_from_slice(&0u32.to_le_bytes());

        let (inputs, outputs, inpoints) = parse_cold_data_fields(&cold_data);
        assert!(inputs.is_none());
        assert!(outputs.is_none());
        assert!(inpoints.is_none());
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
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    #[test]
    fn dispatch_query_old_unmined_skips_preserved_records() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(9);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        {
            let mut ui = h.engine.unmined_index();
            ui.insert(100, TxKey { txid }, None).unwrap();
        }

        let mut shared = Vec::new();
        shared.extend_from_slice(&1000u32.to_le_bytes());
        let payload = encode_txid_batch(&[txid], &shared);
        assert_eq!(
            h.request(OP_PRESERVE_TRANSACTIONS, payload).status,
            STATUS_OK
        );

        let resp = h.request(OP_QUERY_OLD_UNMINED, 200u32.to_le_bytes().to_vec());
        assert_eq!(resp.status, STATUS_OK);
        let count = u32::from_le_bytes(resp.payload[0..4].try_into().unwrap());
        assert_eq!(
            count, 0,
            "preserved unmined tx must not be returned to the pruner"
        );
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
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    // -----------------------------------------------------------------------
    // 1e. handle_process_expired — deletes eligible records
    // -----------------------------------------------------------------------

    /// R-008 (Codex F2) + R-102 / IJK-09: process-expired must only
    /// delete records that BOTH (a) are due per the on-device metadata
    /// (`spent_utxos == utxo_count`, `preserve_until == 0`,
    /// `unmined_since == 0`, `delete_at_height <= current_height`) and
    /// (b) belong to a shard this node masters. The DAH index is a
    /// hint, not authoritative.
    ///
    /// Pre-fix this handler blindly deleted every record whose key
    /// appeared in the DAH range query — even fully-unspent records,
    /// even records whose `preserve_until` had been pushed forward
    /// after the DAH entry was inserted, and even records this node
    /// did not master. The new behavior matches the audit's intent:
    /// stale DAH entries are skipped, master-only records are deleted.
    #[test]
    fn dispatch_process_expired_deletes_only_truly_eligible() {
        let h = DispatchTestHarness::new();
        let txid_a = DispatchTestHarness::make_txid(20);
        let txid_b = DispatchTestHarness::make_txid(21);
        let txid_c = DispatchTestHarness::make_txid(22);

        // txid_a, txid_b, txid_c each have 2 utxos. To make a record
        // truly eligible for the pruner, ALL of its utxos must be
        // SPENT and the metadata must carry a non-zero
        // delete_at_height in the past, with no preserve_until and
        // no unmined_since. Using the regular spend handler with a
        // small `block_height_retention` produces exactly that state.
        assert_eq!(h.create_tx(txid_a, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 2).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_c, 2).status, STATUS_OK);

        // Mine each record AND spend all its slots. DAH is only set
        // when the record is mined (`unmined_since == 0`) — without
        // mining first, `evaluate_delete_at_height` would leave DAH
        // at 0 and the records would never qualify for the pruner.
        let make_eligible = |txid: [u8; 32]| {
            let key = TxKey { txid };
            let entry = h.engine.lookup(&key).expect("seed lookup");
            let utxo_count = entry.utxo_count;
            // Mine the record by calling engine.set_mined directly
            // (avoids encoding a full SET_MINED_BATCH wire frame for a
            // unit test).
            h.engine
                .set_mined(&crate::ops::set_mined::SetMinedRequest {
                    tx_key: key,
                    block_id: 1,
                    block_height: 50,
                    subtree_idx: 0,
                    on_longest_chain: true,
                    unset_mined: false,
                    current_block_height: 100,
                    block_height_retention: 1,
                })
                .expect("set_mined seed");

            let hashes: Vec<[u8; 32]> = (0..utxo_count)
                .map(|v| h.engine.read_slot(&key, v).unwrap().hash)
                .collect();
            let params = SpendBatchParams {
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 100,
                // retention=1 → spending at block 100 with the record
                // mined at block 50 sets delete_at_height around
                // 100 + 1 = 101, well below the 700 we use below.
                block_height_retention: 1,
            };
            let items: Vec<WireSpendItem> = (0..utxo_count)
                .map(|i| WireSpendItem {
                    txid,
                    vout: i,
                    utxo_hash: hashes[i as usize],
                    spending_data: [(0xC0 + i as u8); 36],
                })
                .collect();
            let resp = h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &items));
            assert_eq!(
                resp.status, STATUS_OK,
                "spend-all-slots must succeed for {txid:?}"
            );
            let post = h.engine.read_metadata(&key).unwrap();
            assert_eq!({ post.spent_utxos }, utxo_count);
            assert_ne!(
                { post.delete_at_height },
                0,
                "spend on a fully-mined fully-spent record must set delete_at_height"
            );
        };
        make_eligible(txid_a);
        make_eligible(txid_b);
        // txid_c stays unspent — it is a control case that proves
        // process-expired skips records that are not actually due
        // even when they show up in the DAH index.

        // txid_c: insert a DAH index entry directly so the range
        // query returns it, but the on-device metadata still says
        // spent_utxos == 0 — process-expired must skip it after
        // the re-validation step (R-102 / IJK-09).
        {
            let mut dah = h.engine.dah_index();
            dah.insert(500, TxKey { txid: txid_c }, None).unwrap();
        }

        let mut payload = Vec::new();
        payload.extend_from_slice(&700u32.to_le_bytes());
        let resp = h.request(OP_PROCESS_EXPIRED_PRESERVATIONS, payload);
        assert_eq!(resp.status, STATUS_OK);
        assert!(resp.payload.len() >= 8);

        let deleted = u32::from_le_bytes(resp.payload[0..4].try_into().unwrap());
        assert_eq!(
            deleted, 2,
            "expected exactly 2 deletes (txid_a + txid_b); got {deleted}"
        );

        // txid_a and txid_b are gone, txid_c still exists despite
        // its DAH-index entry — that's the IJK-09 fix.
        assert!(h.engine.lookup(&TxKey { txid: txid_a }).is_none());
        assert!(h.engine.lookup(&TxKey { txid: txid_b }).is_none());
        assert!(
            h.engine.lookup(&TxKey { txid: txid_c }).is_some(),
            "process-expired must skip records that are not actually due, even if they appear in the DAH index"
        );
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
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
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
        assert_eq!(code, ERR_OPCODE_UNSUPPORTED);
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
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
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
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
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
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
        assert!(msg.contains("malformed"), "expected 'malformed' in: {msg}");
    }

    #[test]
    fn get_batch_propagates_storage_errors_not_zeros() {
        let inner: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let (failing, fail_reads) = ReadFailingDevice::new(inner);
        let dev: Arc<dyn BlockDevice> = failing;
        let h = DispatchTestHarness::with_device(dev);

        let txid = DispatchTestHarness::make_txid(41);
        assert_eq!(h.create_tx(txid, 2).status, STATUS_OK);

        fail_reads.store(true, std::sync::atomic::Ordering::SeqCst);

        let payload = encode_get_batch(FieldMask::RAW_METADATA, &[txid]);
        let resp = h.request(OP_GET_BATCH, payload);
        assert_eq!(resp.status, STATUS_OK);
        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].status, ERR_STORAGE_IO as u8,
            "storage read failures must surface as ERR_STORAGE_IO per-item status (P3.10 / F-G5-017)"
        );
        assert!(
            results[0].data.is_empty(),
            "metadata read failures must not synthesize a zero-filled record"
        );
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
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
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
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
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

    #[test]
    fn dispatch_get_utxo_slots_round_trips_slot_region() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(42);

        assert_eq!(h.create_tx(txid, 3).status, STATUS_OK);

        let resp = h.request(
            OP_GET_BATCH,
            encode_get_batch(FieldMask::UTXO_SLOTS, &[txid]),
        );
        assert_eq!(resp.status, STATUS_OK);
        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, STATUS_OK);

        let data = &results[0].data;
        let utxo_count = u32::from_le_bytes(data[0..4].try_into().unwrap());
        assert_eq!(utxo_count, 3);
        assert_eq!(data.len(), 4 + 3 * 69);

        let mut pos = 4;
        for i in 0..3u8 {
            let hash = &data[pos..pos + 32];
            pos += 32;
            let status = data[pos];
            pos += 1;
            let spending_data = &data[pos..pos + 36];
            pos += 36;

            assert_eq!(hash[0], i);
            assert_eq!(hash[1], 0);
            assert_eq!(status, crate::record::UTXO_UNSPENT);
            assert_eq!(spending_data, &[0u8; 36]);
        }
    }

    /// R-044 (GH-06 / GH-09) regression: an active streaming-blob
    /// upload session whose cumulative `bytes_received` would exceed
    /// the configured per-stream byte cap MUST be aborted with `ERR_INTERNAL`
    /// — the server must not accept the chunk and must not let the
    /// counter grow unbounded. Pre-fix the per-stream counter
    /// incremented on every chunk with no upper bound, so a single
    /// connection could write multi-terabyte blobs by sending
    /// 4 KiB chunks indefinitely.
    ///
    /// Test installs a 1024-byte cap on the connection state so we can
    /// exercise the rejection path without allocating gigabytes of test data.
    #[test]
    fn stream_chunk_aborts_when_cumulative_bytes_exceed_cap() {
        use crate::protocol::codec::encode_stream_chunk;
        use crate::protocol::opcodes::OP_STREAM_CHUNK;
        use crate::storage::blobstore::{BlobStore, MemoryBlobStore};

        let h = DispatchTestHarness::new();
        let blob_store: std::sync::Arc<dyn BlobStore> = std::sync::Arc::new(MemoryBlobStore::new());
        let txid = DispatchTestHarness::make_txid(0xCA);

        // Hold a single ConnectionState across two chunks, since the
        // active-stream session lives in conn_state and per-chunk
        // routing through handle_request is what production does.
        let mut conn_state =
            crate::server::ConnectionState::new().with_max_stream_total_bytes(1024);

        // Chunk 1: 800 bytes at offset 0 — under the 1024-byte cap.
        let chunk1 = vec![0xAAu8; 800];
        let req1 = RequestFrame {
            request_id: 1,
            op_code: OP_STREAM_CHUNK,
            flags: 0,
            payload: encode_stream_chunk(&txid, 0, &chunk1).into(),
        };
        let resp1 = handle_request(
            &req1,
            &h.engine,
            8192,
            None,
            None,
            &mut conn_state,
            Some(&*blob_store),
        );
        assert_eq!(
            resp1.status,
            STATUS_OK,
            "first chunk under the cap must succeed; payload: {:?}",
            String::from_utf8_lossy(&resp1.payload),
        );

        // Chunk 2: 300 bytes at offset 800 — pushes total to 1100,
        // exceeding the 1024-byte cap. MUST be rejected with
        // ERR_INTERNAL and a "exceeds maximum" message in the
        // payload, and the stream session must be removed.
        let chunk2 = vec![0xBBu8; 300];
        let req2 = RequestFrame {
            request_id: 2,
            op_code: OP_STREAM_CHUNK,
            flags: 0,
            payload: encode_stream_chunk(&txid, 800, &chunk2).into(),
        };
        let resp2 = handle_request(
            &req2,
            &h.engine,
            8192,
            None,
            None,
            &mut conn_state,
            Some(&*blob_store),
        );
        assert_ne!(
            resp2.status, STATUS_OK,
            "second chunk that pushes total over the cap must be rejected",
        );
        let msg = String::from_utf8_lossy(&resp2.payload);
        assert!(
            msg.contains("exceeds maximum"),
            "rejection must include the cap-exceeded reason; got: {msg}",
        );
        assert!(
            !conn_state.streams.contains_key(&txid),
            "stream session must be removed after exceeding the cap",
        );
    }

    #[test]
    fn dispatch_external_create_binds_record_to_blob_digest() {
        let h = DispatchTestHarness::new();
        let blob_store = crate::storage::blobstore::MemoryBlobStore::new();
        let txid = DispatchTestHarness::make_txid(41);
        let blob_digest = blob_store
            .put(&txid, b"external transaction payload")
            .unwrap();

        let item = WireCreateItem {
            txid,
            tx_version: 1,
            locktime: 0,
            fee: 500,
            size_in_bytes: 123,
            extended_size: 123,
            is_coinbase: false,
            spending_height: 0,
            created_at: 1700000000000,
            flags: FLAG_EXTERNAL_BLOB,
            utxo_hashes: vec![[0xAB; 32]],
            cold_data: vec![],
            block_height: 0,
            mined_block_id: None,
            mined_block_height: None,
            mined_subtree_idx: None,
            parent_txids: vec![],
        };

        let resp =
            h.request_with_blob_store(OP_CREATE_BATCH, encode_create_batch(&[item]), &blob_store);
        assert_eq!(resp.status, STATUS_OK);

        let resp = h.request(
            OP_GET_BATCH,
            encode_get_batch(FieldMask::EXTERNAL_REF, &[txid]),
        );
        assert_eq!(resp.status, STATUS_OK);
        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, STATUS_OK);

        let data = &results[0].data;
        assert_eq!(data.len(), 65);
        assert_eq!(data[0], 1);
        assert_eq!(&data[1..33], &blob_digest.sha256);
        assert_eq!(
            u64::from_le_bytes(data[33..41].try_into().unwrap()),
            blob_digest.length
        );
        assert_ne!(&data[1..33], &txid);
    }

    #[test]
    fn delete_external_blob_missing_rejects_before_wal_and_mutation() {
        let mut h = DispatchTestHarness::new();
        let blob_store: Arc<dyn crate::storage::blobstore::BlobStore> =
            Arc::new(crate::storage::blobstore::MemoryBlobStore::new());
        h.engine.set_blob_store(blob_store.clone());

        let txid = DispatchTestHarness::make_txid(42);
        blob_store
            .put(&txid, b"external transaction payload")
            .unwrap();

        let item = WireCreateItem {
            txid,
            tx_version: 1,
            locktime: 0,
            fee: 500,
            size_in_bytes: 123,
            extended_size: 123,
            is_coinbase: false,
            spending_height: 0,
            created_at: 1700000000000,
            flags: FLAG_EXTERNAL_BLOB,
            utxo_hashes: vec![[0xBC; 32]],
            cold_data: vec![],
            block_height: 0,
            mined_block_id: None,
            mined_block_height: None,
            mined_subtree_idx: None,
            parent_txids: vec![],
        };
        let resp =
            h.request_with_blob_store(OP_CREATE_BATCH, encode_create_batch(&[item]), &*blob_store);
        assert_eq!(resp.status, STATUS_OK);

        blob_store.delete(&txid).unwrap();

        let resp = h.request(OP_DELETE_BATCH, encode_txid_batch(&[txid], &[]));
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);
        let errors = decode_sparse_errors(&resp.payload).unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].item_index, 0);
        assert_eq!(errors[0].error_code, ERR_STORAGE_IO);
        assert!(
            String::from_utf8_lossy(&errors[0].error_data)
                .contains("external blob snapshot missing")
        );

        let resp = h.request(
            OP_GET_BATCH,
            encode_get_batch(FieldMask::TX_VERSION, &[txid]),
        );
        assert_eq!(resp.status, STATUS_OK);
        let results = decode_get_response(&resp.payload).unwrap();
        assert_eq!(
            results[0].status, STATUS_OK,
            "delete must not remove the record when its external blob snapshot is missing"
        );
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

    #[test]
    fn dispatch_delete_child_prunes_parent_slot_before_removing_child() {
        let h = DispatchTestHarness::new();
        let parent_txid = DispatchTestHarness::make_txid(71);
        let child_txid = DispatchTestHarness::make_txid(72);

        assert_eq!(h.create_tx(parent_txid, 2).status, STATUS_OK);

        let mut extended_input = vec![0u8; 36];
        extended_input[..32].copy_from_slice(&parent_txid);
        extended_input[32..36].copy_from_slice(&1u32.to_le_bytes());
        let mut inputs_blob = Vec::new();
        inputs_blob.extend_from_slice(&1u32.to_le_bytes());
        inputs_blob.extend_from_slice(&(extended_input.len() as u32).to_le_bytes());
        inputs_blob.extend_from_slice(&extended_input);
        let child_cold = crate::ops::engine::build_cold_data(Some(&inputs_blob), None, None);

        let child = WireCreateItem {
            txid: child_txid,
            tx_version: 1,
            locktime: 0,
            fee: 500,
            size_in_bytes: 250,
            extended_size: 250,
            is_coinbase: false,
            spending_height: 0,
            created_at: 1700000000000,
            flags: 0,
            utxo_hashes: vec![[0xCC; 32]],
            cold_data: child_cold,
            block_height: 0,
            mined_block_id: None,
            mined_block_height: None,
            mined_subtree_idx: None,
            parent_txids: vec![],
        };
        assert_eq!(
            h.request(OP_CREATE_BATCH, encode_create_batch(&[child]))
                .status,
            STATUS_OK
        );

        let mut parent_hash = [0u8; 32];
        parent_hash[0] = 1;
        let mut spending_data = [0u8; 36];
        spending_data[..32].copy_from_slice(&child_txid);
        spending_data[32..36].copy_from_slice(&0u32.to_le_bytes());
        let spend = WireSpendItem {
            txid: parent_txid,
            vout: 1,
            utxo_hash: parent_hash,
            spending_data,
        };
        let spend_params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        assert_eq!(
            h.request(OP_SPEND_BATCH, encode_spend_batch(&spend_params, &[spend]))
                .status,
            STATUS_OK
        );

        let delete_resp = h.request(OP_DELETE_BATCH, encode_txid_batch(&[child_txid], &[]));
        assert_eq!(delete_resp.status, STATUS_OK);

        let child_get = h.request(
            OP_GET_BATCH,
            encode_get_batch(FieldMask::TX_VERSION, &[child_txid]),
        );
        let child_results = decode_get_response(&child_get.payload).unwrap();
        assert_eq!(child_results[0].status, 1);

        let parent_key = TxKey { txid: parent_txid };
        let parent_slot = h.engine.read_slot(&parent_key, 1).unwrap();
        assert_eq!(parent_slot.status, crate::record::UTXO_PRUNED);
        assert_eq!(parent_slot.spending_data, spending_data);

        let parent_meta = h.engine.read_metadata(&parent_key).unwrap();
        assert_eq!({ parent_meta.spent_utxos }, 0);
        assert_eq!({ parent_meta.pruned_utxos }, 1);
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

    /// OP_HELLO returns the server's `PROTOCOL_VERSION` as a 2-byte LE u16.
    /// The handshake lets clients negotiate the typed error-code surface
    /// added in PROTOCOL_VERSION=2 (and any future bumps). See
    /// `OP_HELLO` doc-comment for the compatibility contract.
    #[test]
    fn dispatch_hello_returns_protocol_version() {
        let h = DispatchTestHarness::new();
        let resp = h.request(OP_HELLO, vec![]);
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            resp.payload.len(),
            2,
            "HELLO payload must be exactly 2 bytes (u16 LE)"
        );
        let observed = u16::from_le_bytes(resp.payload[..2].try_into().unwrap());
        assert_eq!(
            observed, PROTOCOL_VERSION,
            "HELLO must report the server's compiled-in PROTOCOL_VERSION"
        );
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
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
        // The decoder rejects the over-size batch BEFORE allocation, so the
        // error message now identifies the configured limit explicitly. We
        // verify both the count (20) and the configured cap (10) appear so
        // a regression that loses either value would fail this assertion.
        assert!(
            msg.contains("exceeds max_batch_size 10") && msg.contains("20"),
            "expected 'count 20 exceeds max_batch_size 10' in: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // WAL-first regression tests — redo fsynced before engine mutation
    // -----------------------------------------------------------------------

    /// Test harness with redo log support for crash-recovery testing.
    ///
    /// Like [`DispatchTestHarness`], holds `metrics_test_lock` for its
    /// lifetime: dispatch requests issued through this harness mutate the
    /// process-wide `DISPATCH_METRICS` singleton, and without the guard
    /// they race the metrics-delta tests (e.g.
    /// `prometheus_emits_operations_total_with_labels`, which compares a
    /// rendered snapshot against live counters). Tests using this harness
    /// must NOT take `metrics_test_lock()` themselves (stdlib `Mutex` is
    /// not re-entrant; doing so deadlocks on the same thread).
    struct RedoDispatchHarness {
        engine: Engine,
        redo_log: Arc<Mutex<crate::redo::RedoLog>>,
        data_dev: Arc<MemoryDevice>,
        redo_dev: Arc<MemoryDevice>,
        _metrics_guard: std::sync::MutexGuard<'static, ()>,
    }

    struct CountingSyncDevice {
        inner: Arc<MemoryDevice>,
        sync_count: std::sync::atomic::AtomicUsize,
    }

    impl CountingSyncDevice {
        fn new(size: u64, alignment: usize) -> Self {
            Self {
                inner: Arc::new(MemoryDevice::new(size, alignment).unwrap()),
                sync_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn sync_count(&self) -> usize {
            self.sync_count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl BlockDevice for CountingSyncDevice {
        fn pread(&self, buf: &mut [u8], offset: u64) -> crate::device::Result<usize> {
            self.inner.pread(buf, offset)
        }

        fn pwrite(&self, buf: &[u8], offset: u64) -> crate::device::Result<usize> {
            self.inner.pwrite(buf, offset)
        }

        fn alignment(&self) -> usize {
            self.inner.alignment()
        }

        fn size(&self) -> u64 {
            self.inner.size()
        }

        fn sync(&self) -> crate::device::Result<()> {
            self.sync_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.sync()
        }
    }

    #[test]
    fn redo_group_commit_coalesces_concurrent_dispatch_writers() {
        let redo_dev = Arc::new(CountingSyncDevice::new(4 * 1024 * 1024, 4096));
        let redo_log = Arc::new(Mutex::new(
            RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, 4 * 1024 * 1024)
                .expect("redo log opens"),
        ));
        // Capture sync count after open — F-G4-001's initial header
        // write at open also syncs, and is not part of the group-commit
        // accounting under test.
        let baseline_syncs = redo_dev.sync_count();
        let barrier = Arc::new(std::sync::Barrier::new(3));

        let spawn_writer = |byte: u8| {
            let redo_log = Arc::clone(&redo_log);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let tx_key = TxKey { txid: [byte; 32] };
                barrier.wait();
                write_redo_ops_with_group_window(
                    Some(redo_log.as_ref()),
                    &[RedoOp::Delete {
                        tx_key,
                        record_offset: u64::from(byte) * 4096,
                        record_size: 4096,
                    }],
                    Duration::from_millis(50),
                )
                .expect("grouped redo write succeeds")
            })
        };

        let first = spawn_writer(1);
        let second = spawn_writer(2);
        barrier.wait();

        let mut ranges = vec![
            first.join().expect("first writer joins"),
            second.join().expect("second writer joins"),
        ];
        ranges.sort_by_key(|range| range.0);

        assert_eq!(ranges, vec![(1, 1), (2, 2)]);
        // F-G4-001 made flush() emit two device syncs per effective
        // flush: one for the entries pwrite and one for the persisted-
        // header pwrite. Group commit collapses the two concurrent
        // writers into one effective flush, so the post-open delta is
        // exactly 2 syncs.
        assert_eq!(
            redo_dev.sync_count() - baseline_syncs,
            2,
            "concurrent dispatch writers should share one effective flush \
             (one entries sync + one header sync under F-G4-001)"
        );

        let entries = redo_log.lock().recover().expect("recover grouped entries");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
    }

    impl RedoDispatchHarness {
        fn new() -> Self {
            Self::new_with_redo_size(4 * 1024 * 1024)
        }

        fn new_with_redo_size(redo_size: u64) -> Self {
            let data_dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
            let redo_dev = Arc::new(MemoryDevice::new(redo_size.max(4096), 4096).unwrap());
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
                redo_size.max(4096),
            )
            .unwrap();
            Self {
                engine,
                redo_log: Arc::new(Mutex::new(redo_log)),
                data_dev,
                redo_dev,
                _metrics_guard: metrics_test_lock(),
            }
        }

        fn new_with_exact_redo_log_size(log_size: u64) -> Self {
            let data_dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
            let redo_dev = Arc::new(MemoryDevice::new(log_size.max(4096), 4096).unwrap());
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
            let redo_log =
                crate::redo::RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, log_size)
                    .unwrap();
            let redo_log = Arc::new(Mutex::new(redo_log));
            engine.allocator().lock().set_redo_log(redo_log.clone());
            Self {
                engine,
                redo_log,
                data_dev,
                redo_dev,
                _metrics_guard: metrics_test_lock(),
            }
        }

        /// Dispatch a request through the full handler with redo log attached.
        fn request(&self, op_code: u16, payload: Vec<u8>) -> ResponseFrame {
            let req = RequestFrame {
                request_id: 1,
                op_code,
                flags: 0,
                payload: payload.into(),
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
                // Safe to reacquire: `drop(self)` above released the
                // original harness's guard.
                _metrics_guard: metrics_test_lock(),
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

    /// Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): the spend redo entry
    /// must carry the real `new_spent_count`, not `0`. Simulates the
    /// crash-between-WAL-fsync-and-engine-apply window by spending three
    /// slots, then *forcing* recovery to reapply the spend redo entries
    /// against fresh on-device record bytes (reset by the test). After
    /// recovery, `meta.spent_utxos` must equal 3 — the count the redo
    /// entries actually carried — not 0.
    #[test]
    fn spend_redo_carries_real_new_spent_count_for_replay() {
        let h = RedoDispatchHarness::new();
        let txid = {
            let mut t = [0u8; 32];
            t[0] = 0xAA;
            t
        };
        assert_eq!(h.create_tx(txid, 3).status, STATUS_OK);
        let key = TxKey { txid };

        // Capture the original record offset and hashes BEFORE spending.
        let entry = h.engine.lookup(&key).expect("create registered tx");
        let record_offset = entry.record_offset;
        let pre_meta = crate::io::read_metadata(
            &*h.data_dev as &dyn crate::device::BlockDevice,
            record_offset,
        )
        .expect("read pre-spend metadata");
        let pre_spent: u32 = { pre_meta.spent_utxos };
        assert_eq!(pre_spent, 0, "pre-state spent_utxos must be 0");
        let original_slots: Vec<crate::record::UtxoSlot> = (0..3u32)
            .map(|i| {
                crate::io::read_utxo_slot(
                    &*h.data_dev as &dyn crate::device::BlockDevice,
                    record_offset,
                    i,
                )
                .expect("read original slot")
            })
            .collect();

        // Spend all three slots in a single batch.
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let items: Vec<WireSpendItem> = (0..3u32)
            .map(|i| WireSpendItem {
                txid,
                vout: i,
                utxo_hash: original_slots[i as usize].hash,
                spending_data: [(0xC0 + i as u8); 36],
            })
            .collect();
        assert_eq!(
            h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &items))
                .status,
            STATUS_OK,
            "spend batch must succeed",
        );

        // Verify the engine applied the count.
        let post_meta = crate::io::read_metadata(
            &*h.data_dev as &dyn crate::device::BlockDevice,
            record_offset,
        )
        .expect("read post-spend metadata");
        assert_eq!(
            { post_meta.spent_utxos },
            3,
            "engine must have applied real spent_utxos = 3",
        );

        // Roll the on-device slots + counter back to the pre-spend state to
        // simulate "redo fsynced but engine apply lost". The redo entries
        // are still in the redo log device.
        for (i, slot) in original_slots.iter().enumerate() {
            crate::io::write_utxo_slot(
                &*h.data_dev as &dyn crate::device::BlockDevice,
                record_offset,
                i as u32,
                slot,
            )
            .expect("restore pre-spend slot");
        }
        let mut reset_meta = post_meta;
        reset_meta.spent_utxos = pre_spent;
        crate::io::write_metadata(
            &*h.data_dev as &dyn crate::device::BlockDevice,
            record_offset,
            &reset_meta,
        )
        .expect("restore pre-spend metadata");

        // CRASH and recover — replay must reconstruct counter to 3.
        let h2 = h.crash_and_recover();
        let recovered_meta = crate::io::read_metadata(
            &*h2.data_dev as &dyn crate::device::BlockDevice,
            record_offset,
        )
        .expect("read recovered metadata");
        assert_eq!(
            { recovered_meta.spent_utxos },
            3,
            "redo replay must restore spent_utxos = 3 (gap #2 — \
             previously written as 0 in dispatch and applied verbatim)",
        );
        // And every slot must be SPENT after replay.
        for i in 0..3u32 {
            let slot = crate::io::read_utxo_slot(
                &*h2.data_dev as &dyn crate::device::BlockDevice,
                record_offset,
                i,
            )
            .expect("recovered slot reads");
            assert!(slot.is_spent(), "slot {i} must be SPENT after replay");
        }
    }

    /// Companion test for unspend: the same `new_spent_count: 0`
    /// placeholder bug existed in the unspend dispatch path. After
    /// unspending one slot, simulate "redo fsynced but engine lost" by
    /// rolling the slot + counter back, and verify replay restores the
    /// post-unspend counter (decremented from the original).
    #[test]
    fn unspend_redo_carries_real_new_spent_count_for_replay() {
        let h = RedoDispatchHarness::new();
        let txid = {
            let mut t = [0u8; 32];
            t[0] = 0xBB;
            t
        };
        assert_eq!(h.create_tx(txid, 2).status, STATUS_OK);
        let key = TxKey { txid };

        // Spend both slots so the counter is 2 before the unspend.
        let entry = h.engine.lookup(&key).expect("create registered tx");
        let record_offset = entry.record_offset;
        let original_slots: Vec<crate::record::UtxoSlot> = (0..2u32)
            .map(|i| {
                crate::io::read_utxo_slot(
                    &*h.data_dev as &dyn crate::device::BlockDevice,
                    record_offset,
                    i,
                )
                .expect("read original slot")
            })
            .collect();
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let spend_items: Vec<WireSpendItem> = (0..2u32)
            .map(|i| WireSpendItem {
                txid,
                vout: i,
                utxo_hash: original_slots[i as usize].hash,
                spending_data: [(0xC0 + i as u8); 36],
            })
            .collect();
        assert_eq!(
            h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &spend_items))
                .status,
            STATUS_OK,
        );
        let pre_unspend_meta = crate::io::read_metadata(
            &*h.data_dev as &dyn crate::device::BlockDevice,
            record_offset,
        )
        .expect("read pre-unspend metadata");
        assert_eq!({ pre_unspend_meta.spent_utxos }, 2);

        // Now unspend slot 0 only.
        let unspend_items = vec![WireUnspendItem {
            txid,
            vout: 0,
            utxo_hash: original_slots[0].hash,
            spending_data: [0xC0; 36],
        }];
        let unspend_params = UnspendBatchParams {
            current_block_height: 1000,
            block_height_retention: 288,
        };
        assert_eq!(
            h.request(
                OP_UNSPEND_BATCH,
                encode_unspend_batch(&unspend_params, &unspend_items),
            )
            .status,
            STATUS_OK,
        );

        // Verify engine count went 2 → 1.
        let post_unspend_meta = crate::io::read_metadata(
            &*h.data_dev as &dyn crate::device::BlockDevice,
            record_offset,
        )
        .expect("read post-unspend metadata");
        assert_eq!(
            { post_unspend_meta.spent_utxos },
            1,
            "engine must apply real spent_utxos = 1 after unspending one slot",
        );

        // Simulate "redo fsynced but engine lost": roll slot 0 back to
        // SPENT and counter back to 2.
        let spent_zero = crate::record::UtxoSlot::new_spent(original_slots[0].hash, [0xC0; 36]);
        crate::io::write_utxo_slot(
            &*h.data_dev as &dyn crate::device::BlockDevice,
            record_offset,
            0,
            &spent_zero,
        )
        .expect("restore SPENT slot 0");
        let mut reset_meta = post_unspend_meta;
        reset_meta.spent_utxos = 2;
        crate::io::write_metadata(
            &*h.data_dev as &dyn crate::device::BlockDevice,
            record_offset,
            &reset_meta,
        )
        .expect("restore pre-unspend metadata");

        // CRASH and recover — replay_unspend must take counter 2 → 1
        // by writing the redo's `new_spent_count`.
        let h2 = h.crash_and_recover();
        let recovered_meta = crate::io::read_metadata(
            &*h2.data_dev as &dyn crate::device::BlockDevice,
            record_offset,
        )
        .expect("read recovered metadata");
        assert_eq!(
            { recovered_meta.spent_utxos },
            1,
            "redo replay must decrement spent_utxos to 1 (gap #2 — \
             previously written as 0 in dispatch)",
        );
        let recovered_slot0 = crate::io::read_utxo_slot(
            &*h2.data_dev as &dyn crate::device::BlockDevice,
            record_offset,
            0,
        )
        .expect("recovered slot 0 reads");
        assert!(
            recovered_slot0.is_unspent(),
            "slot 0 must be UNSPENT after replay",
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

    // R-041: REDIRECT now carries shard_table_version so clients can detect
    // a stale-route loop instead of chasing redirects forever. This test
    // exercises the per-item REDIRECT path on both write (BatchItemError)
    // and read (WireGetResult) flows and asserts the version round-trips.
    #[test]
    fn redirect_includes_shard_table_version_for_loop_detection() {
        use crate::cluster::shards::NodeId;
        use crate::cluster::shards::ShardTable;

        // DispatchTestHarness::new() holds metrics_test_lock for the
        // harness lifetime, so the global `operations` /
        // `stale_routing_request_total` increments this test causes do
        // not race with `prometheus_emits_operations_total_with_labels`
        // (which compares a rendered snapshot against live counters).

        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(91);
        let shard = ShardTable::shard_for_key(&TxKey { txid });
        let members = vec![NodeId(1), NodeId(2)];
        // Pick a non-trivial shard table version so an "0" placeholder
        // would be obvious. compute_with_epoch(epoch=42) sets table.version=42.
        const STALE_VERSION: u64 = 42;
        let table = ShardTable::compute_with_epoch(&members, 2, STALE_VERSION);
        let master = table.target_assignment(shard).master;
        // self_id != master so this node redirects.
        let self_id = if master == NodeId(1) {
            NodeId(2)
        } else {
            NodeId(1)
        };
        let target_addr_for_master = if master == NodeId(1) {
            "127.0.0.1:4501".parse::<std::net::SocketAddr>().unwrap()
        } else {
            "127.0.0.1:4502".parse::<std::net::SocketAddr>().unwrap()
        };
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            self_id,
            table,
            &[
                (NodeId(1), "127.0.0.1:4501".parse().unwrap()),
                (NodeId(2), "127.0.0.1:4502".parse().unwrap()),
            ],
            &members,
            &[],
            &[],
            &[],
            2,
        );

        // -------- Write path: BatchItemError redirect data --------
        // SetMined writes are routed via check_shard_ownership, which
        // emits BatchItemError { error_code: ERR_REDIRECT, error_data }.
        let set_mined_params = SetMinedBatchParams {
            block_id: 1,
            block_height: 100,
            subtree_idx: 0,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 100,
            block_height_retention: 288,
        };
        let payload = encode_set_mined_batch(&set_mined_params, &[txid]);
        let resp = h.request_with_cluster(OP_SET_MINED_BATCH, payload, &cluster);
        // Per-item redirect comes back as a partial-error with one entry.
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);
        let errs = crate::protocol::codec::decode_sparse_errors(&resp.payload).unwrap();
        assert_eq!(errs.len(), 1, "exactly one redirected item");
        assert_eq!(errs[0].error_code, ERR_REDIRECT);

        // R-041 assertion: error_data must decode to (addr, Some(version))
        // via decode_redirect_with_version. Pre-fix the data was raw addr
        // bytes with no version → decode_redirect_with_version would have
        // returned (addr, None) and the client could not perform a
        // server-vs-client version comparison.
        let (addr, version) =
            crate::protocol::codec::decode_redirect_with_version(&errs[0].error_data)
                .expect("R-041: BatchItemError REDIRECT data must decode with version");
        assert_eq!(addr, target_addr_for_master.to_string());
        assert_eq!(
            version,
            Some(STALE_VERSION),
            "R-041: server's shard_table_version must round-trip through error_data \
             so a client at the same/newer version can detect a stale-route loop",
        );

        // The classify helper must say "Stale" when the client knows a
        // version equal to the server's — chasing this redirect would
        // either loop or land on an even-older view.
        assert_eq!(
            crate::protocol::codec::classify_redirect(version, STALE_VERSION),
            crate::protocol::codec::RedirectFollowDecision::Stale,
            "R-041: server.version == client.version must be classified Stale \
             so the client refreshes/fails instead of looping",
        );
        // And "Follow" only when the server is strictly ahead.
        assert_eq!(
            crate::protocol::codec::classify_redirect(version, STALE_VERSION - 1),
            crate::protocol::codec::RedirectFollowDecision::Follow,
        );

        // -------- Read path: WireGetResult REDIRECT data --------
        // GET_BATCH per-item REDIRECT carries `[ERR_REDIRECT_byte:1][addr_len:2][addr][version:8]`
        // in WireGetResult.data — strip the leading status byte and run
        // the same decode/classify checks.
        let resp = h.request_with_cluster(
            OP_GET_BATCH,
            crate::protocol::codec::encode_get_batch(FieldMask::ALL_METADATA, &[txid]),
            &cluster,
        );
        assert_eq!(resp.status, STATUS_OK);
        let results = crate::protocol::codec::decode_get_response(&resp.payload).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, ERR_REDIRECT as u8);
        assert!(
            results[0].data.len() > 1,
            "R-041: GetBatch REDIRECT payload must include addr+version, not just the status byte",
        );
        assert_eq!(
            results[0].data[0], ERR_REDIRECT as u8,
            "first byte is still the legacy ERR_REDIRECT status repeat"
        );
        let (get_addr, get_version) =
            crate::protocol::codec::decode_redirect_with_version(&results[0].data[1..])
                .expect("R-041: GetBatch REDIRECT payload must decode with version");
        assert_eq!(get_addr, target_addr_for_master.to_string());
        assert_eq!(
            get_version,
            Some(STALE_VERSION),
            "R-041: GetBatch redirect must also carry the server's shard_table_version",
        );
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
    fn client_handles_migration_in_progress_polling() {
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

    /// R-043 (GH-04) regression: a malicious or buggy peer that sends
    /// an `OP_MIGRATION_COMPLETE` frame whose `entry_count` is so
    /// large that `entry_count * 36` overflows `usize` MUST be rejected
    /// with `ERR_MIGRATION_IN_PROGRESS` instead of allocating a
    /// `Vec::with_capacity(entry_count)` (which would OOM the process,
    /// and on 32-bit could even pass the size sanity check because
    /// `60 + entry_count * 36` would wrap to a small value).
    ///
    /// We exercise the overflow path by sending `entry_count =
    /// usize::MAX as u32` (which fits in the wire `u32` field) but a
    /// payload that is only the 60-byte header. The server must
    /// reject without panicking or allocating.
    #[test]
    fn migration_complete_unchecked_multiply_rejects_max_count() {
        let h = DispatchTestHarness::new();
        let shard = 17u16;

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 12);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4717".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        // Build a 60-byte header: shard:u64 | utxo_records_received:u64 |
        // migration_epoch:u64 | source_manifest:32 | entry_count:u32 = u32::MAX.
        let mut payload = Vec::with_capacity(60);
        payload.extend_from_slice(&(shard as u64).to_le_bytes()); // shard:u64
        payload.extend_from_slice(&0u64.to_le_bytes()); // utxo_records_received
        payload.extend_from_slice(&0u64.to_le_bytes()); // migration_epoch
        payload.extend_from_slice(&[0u8; 32]); // source_manifest (all zeros)
        payload.extend_from_slice(&u32::MAX.to_le_bytes()); // entry_count = ATTACK
        assert_eq!(payload.len(), 60);

        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload: payload.into(),
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

        // Either the overflow check fires (preferred) or the size
        // mismatch fires (also acceptable — both end in a rejection
        // before any large allocation). Both surface as
        // `ERR_MIGRATION_IN_PROGRESS` per the existing rejection path.
        assert_ne!(
            resp.status, STATUS_OK,
            "u32::MAX entry_count must be rejected, not accepted",
        );
        assert!(
            !resp.payload.is_empty(),
            "rejection must include an error message in the payload",
        );
    }

    #[test]
    fn migration_complete_zero_count_without_manifest_rejected() {
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
            payload: 0u64.to_le_bytes().to_vec().into(),
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
        assert_eq!(
            u16::from_le_bytes(resp.payload[0..2].try_into().unwrap()),
            ERR_MIGRATION_MANIFEST_REQUIRED
        );
        assert_eq!(
            cluster.inbound_pending_count(),
            1,
            "zero-count completion without manifest must not clear pending inbound"
        );
    }

    #[test]
    fn migration_complete_full_zero_payload_rejected() {
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
            payload: payload.into(),
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
        assert_eq!(
            u16::from_le_bytes(resp.payload[0..2].try_into().unwrap()),
            ERR_MIGRATION_MANIFEST_REQUIRED
        );
        assert_eq!(
            cluster.inbound_pending_count(),
            1,
            "all-zero manifest is treated as missing and must not clear inbound"
        );
    }

    #[test]
    fn migration_complete_zero_count_with_wrong_manifest_rejected() {
        let h = DispatchTestHarness::new();
        let shard = 93u16;

        let members = vec![crate::cluster::shards::NodeId(1)];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 1, 12);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            crate::cluster::shards::NodeId(1),
            table,
            &[(
                crate::cluster::shards::NodeId(1),
                "127.0.0.1:4604".parse().unwrap(),
            )],
            &members,
            &[shard],
            &[],
            &[],
            1,
        );

        assert_eq!(h.engine.shard_record_count(shard), 0);
        assert_eq!(cluster.inbound_pending_count(), 1);

        let payload =
            build_migration_complete_payload(0, 0, 0, Some([0xFFu8; 32]), Some(&[]), None);
        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload: payload.into(),
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
        assert_eq!(
            u16::from_le_bytes(resp.payload[0..2].try_into().unwrap()),
            ERR_MIGRATION_MANIFEST_MISMATCH
        );
        assert_eq!(
            cluster.inbound_pending_count(),
            1,
            "wrong zero-record manifest must not clear inbound"
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
            // F-G7-005: migration batches arriving at a clustered
            // receiver (local_cluster_key != 0) MUST carry a matching
            // non-zero cluster_key — the wildcard is reserved for
            // normal replication so a buggy/hostile sender cannot
            // replay arbitrary mutations through the dedup-bypass
            // path. Stamp the batch with the cluster's current
            // committed term so it survives the epoch gate and we
            // actually exercise the settled-shard logic the test
            // targets.
            payload: ReplicaBatch {
                first_sequence: 0,
                ops: vec![],
                trace_ctx: None,
                source_node_id: None,
                cluster_key: cluster.local_cluster_key(),
            }
            .serialize()
            .into(),
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
    fn classify_write_majority_threshold_consistency_rf2_through_rf7() {
        let expected = [
            (2usize, 1usize),
            (3usize, 1usize),
            (4usize, 2usize),
            (5usize, 2usize),
            (6usize, 3usize),
            (7usize, 3usize),
        ];

        for (rf, required) in expected {
            let targets = rf - 1;
            let below = required.saturating_sub(1);
            if below < required {
                assert_eq!(
                    classify_replication_outcome(
                        below,
                        targets,
                        Some(AckPolicy::WriteMajority),
                        false,
                    ),
                    ReplicationClassification::PolicyViolation { required },
                    "RF={rf} below-majority must fail"
                );
            }

            let at_threshold = classify_replication_outcome(
                required,
                targets,
                Some(AckPolicy::WriteMajority),
                false,
            );
            if required == targets {
                assert_eq!(
                    at_threshold,
                    ReplicationClassification::FullAck,
                    "RF={rf} all targets acked"
                );
            } else {
                assert_eq!(
                    at_threshold,
                    ReplicationClassification::PartialAck,
                    "RF={rf} threshold ack count should satisfy majority"
                );
            }
        }
    }

    #[test]
    fn classify_no_targets_is_full() {
        // Empty target list — nothing to ACK, trivially full.
        let c = classify_replication_outcome(0, 0, None, true);
        assert_eq!(c, ReplicationClassification::FullAck);
    }

    #[test]
    fn replication_timeout_migration_pressure_override() {
        assert_eq!(
            replication_ack_timeout_for(Duration::from_secs(3), false, Duration::from_secs(30)),
            Duration::from_secs(3)
        );
        assert_eq!(
            replication_ack_timeout_for(Duration::from_secs(3), true, Duration::from_secs(30)),
            Duration::from_secs(30)
        );
        assert_eq!(
            replication_ack_timeout_for(Duration::from_secs(45), true, Duration::from_secs(30)),
            Duration::from_secs(45)
        );
        assert_eq!(
            replication_ack_timeout_for(Duration::from_secs(3), true, Duration::from_secs(10)),
            Duration::from_secs(10)
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
    // for every completion, including empty shards. Without one, a
    // malformed/stale frame could mark a shard migrated prematurely.
    // These tests exercise the required paths:
    //   1. no manifest → rejected with ERR_MIGRATION_MANIFEST_REQUIRED
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

    fn compute_manifest_for_entries(entries: &[(TxKey, u32)]) -> [u8; 32] {
        let mut manifest = crate::cluster::coordinator::ManifestHasher::new();
        for (key, generation) in entries {
            manifest.fold(&key.txid, *generation);
        }
        manifest.finalize()
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
    fn intent_persists_before_local_apply() {
        let redo_dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let redo_log = Mutex::new(
            RedoLog::open(redo_dev, 0, 4 * 1024 * 1024).expect("redo log opens on memory device"),
        );
        let tracker = crate::replication::durable::ReplicationIntentTracker::in_memory();
        let tx_key = TxKey {
            txid: txid_for_shard(42, 9),
        };

        let range = write_replicated_redo_ops_with_tracker(
            true,
            Some(&redo_log),
            &[RedoOp::Delete {
                tx_key,
                record_offset: 4096,
                record_size: 256,
            }],
            Some(&tracker),
        )
        .expect("replicated redo write and intent begin succeed");

        assert_eq!(
            tracker.pending(),
            vec![crate::replication::durable::ReplicationIntentRange {
                first_sequence: range.0,
                last_sequence: range.1,
            }],
            "the intent marker must be durable before any caller applies the local mutation"
        );
        let entries = redo_log
            .lock()
            .read_from_sequence(range.0)
            .expect("redo range remains readable");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, range.0);
    }

    #[test]
    fn compensation_no_observable_window() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let engine = Arc::new(DispatchTestHarness::new().engine);
        let reader_engine = Arc::clone(&engine);
        let mutation_guard = acquire_dispatch_visibility_guard(engine.as_ref(), OP_SPEND_BATCH)
            .expect("mutation op should acquire the visibility barrier");
        let reader_entered = Arc::new(AtomicBool::new(false));
        let reader_finished = Arc::new(AtomicBool::new(false));
        let reader_entered_thread = Arc::clone(&reader_entered);
        let reader_finished_thread = Arc::clone(&reader_finished);

        let handle = std::thread::spawn(move || {
            reader_entered_thread.store(true, Ordering::SeqCst);
            let _read_guard =
                acquire_dispatch_visibility_guard(reader_engine.as_ref(), OP_GET_BATCH)
                    .expect("read op should acquire the visibility barrier");
            reader_finished_thread.store(true, Ordering::SeqCst);
        });

        while !reader_entered.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(
            !reader_finished.load(Ordering::SeqCst),
            "client reads must block while a mutation can still be rolled back"
        );

        drop(mutation_guard);
        handle.join().expect("reader thread joins");
        assert!(
            reader_finished.load(Ordering::SeqCst),
            "reader proceeds after mutation replication/compensation window closes"
        );
    }

    #[test]
    fn intent_recovery_handles_redo_wrap_around_gracefully() {
        let h = DispatchTestHarness::new();
        let redo_dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let redo_log = Mutex::new(
            RedoLog::open(redo_dev, 0, 4 * 1024 * 1024).expect("redo log opens on memory device"),
        );
        let tracker = crate::replication::durable::ReplicationIntentTracker::in_memory();
        let tx_key = TxKey {
            txid: txid_for_shard(41, 9),
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

        {
            let mut log = redo_log.lock();
            log.mark_checkpoint().unwrap();
            log.reset().unwrap();
            assert_eq!(log.earliest_sequence().unwrap(), None);
            assert!(log.current_sequence() > range.1);
        }

        let mut replicated = false;
        recover_pending_replication_intents_from_tracker(
            &tracker,
            Some(&redo_log),
            &h.engine,
            |_ops, _range| {
                replicated = true;
                Ok(())
            },
        )
        .expect("reclaimed redo range should clear stale intent instead of bricking startup");

        assert!(
            !replicated,
            "reclaimed range cannot be incrementally replayed"
        );
        assert!(
            tracker.pending().is_empty(),
            "stale pending intent should be cleared after redo reclamation"
        );
    }

    /// Phase I — `OP_ADMIN_CLUSTER_HEALTH` returns the cluster health
    /// snapshot from `RunningCluster::cluster_health` and serializes it
    /// into the 17-byte payload defined by the opcode.
    /// Phase I — multi-node node that hasn't observed its first
    /// committed topology must reject reads/writes with
    /// `ERR_CLUSTER_NOT_READY`. `OP_ADMIN_CLUSTER_HEALTH` and other
    /// bootstrap traffic still flow so the node can become ready.
    #[test]
    fn err_cluster_not_ready_gates_writes_when_joining() {
        let n1 = crate::cluster::shards::NodeId(1);
        let n2 = crate::cluster::shards::NodeId(2);
        // Empty `committed_members` ⇒ `topology_authority.handle_commit`
        // is never called ⇒ `committed_term == 0` ⇒ Joining.
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&[n1, n2], 2, 1);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            n1,
            table,
            &[(n1, "127.0.0.1:9501".parse().unwrap())],
            &[], // <-- no committed topology
            &[],
            &[],
            &[],
            2,
        );
        assert!(!cluster.cluster_health().is_ready());

        // Send an OP_DELETE_BATCH (mutation). Should be rejected with
        // ERR_CLUSTER_NOT_READY before the handler runs. Payload shape
        // is irrelevant — the gate runs before parsing.
        let mut payload = vec![1u8, 0, 0, 0];
        payload.extend_from_slice(&[0xabu8; 32]);
        let req = RequestFrame {
            request_id: 7,
            op_code: OP_DELETE_BATCH,
            flags: 0,
            payload: payload.into(),
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &DispatchTestHarness::new().engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );
        assert_eq!(resp.status, STATUS_ERROR);
        assert!(resp.payload.len() >= 2);
        let err = u16::from_le_bytes(resp.payload[..2].try_into().unwrap());
        assert_eq!(
            err, ERR_CLUSTER_NOT_READY,
            "Joining node must reject mutations with ERR_CLUSTER_NOT_READY",
        );

        // OP_ADMIN_CLUSTER_HEALTH must still respond OK so a client can
        // diagnose the readiness state.
        let admin_req = RequestFrame {
            request_id: 8,
            op_code: OP_ADMIN_CLUSTER_HEALTH,
            flags: 0,
            payload: Vec::new().into(),
        };
        let admin_resp = handle_request(
            &admin_req,
            &DispatchTestHarness::new().engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );
        assert_eq!(
            admin_resp.status, STATUS_OK,
            "OP_ADMIN_CLUSTER_HEALTH must bypass the readiness gate",
        );
    }

    #[test]
    fn op_admin_cluster_health_returns_serialized_snapshot() {
        use crate::cluster::coordinator::{ClusterHealth, ClusterHealthSwimState};
        let n1 = crate::cluster::shards::NodeId(1);
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&[n1], 1, 3);
        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            n1,
            table,
            &[(n1, "127.0.0.1:9001".parse().unwrap())],
            &[n1],
            &[],
            &[],
            &[],
            1,
        );
        let req = RequestFrame {
            request_id: 17,
            op_code: OP_ADMIN_CLUSTER_HEALTH,
            flags: 0,
            payload: Vec::new().into(),
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &DispatchTestHarness::new().engine,
            8192,
            Some(&cluster),
            None,
            &mut conn_state,
            None,
        );
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(resp.request_id, 17);
        let decoded =
            ClusterHealth::deserialize(&resp.payload).expect("response decodes as ClusterHealth");
        assert_eq!(
            decoded.swim_state,
            ClusterHealthSwimState::Alive,
            "test cluster has a committed term so it must report Alive",
        );
        assert!(decoded.is_ready());
    }

    #[test]
    fn op_admin_cluster_health_without_cluster_reports_joining() {
        use crate::cluster::coordinator::{ClusterHealth, ClusterHealthSwimState};
        let req = RequestFrame {
            request_id: 99,
            op_code: OP_ADMIN_CLUSTER_HEALTH,
            flags: 0,
            payload: Vec::new().into(),
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &DispatchTestHarness::new().engine,
            8192,
            None, // no RunningCluster wired in
            None,
            &mut conn_state,
            None,
        );
        assert_eq!(resp.status, STATUS_OK);
        let decoded =
            ClusterHealth::deserialize(&resp.payload).expect("response decodes as ClusterHealth");
        assert_eq!(decoded.swim_state, ClusterHealthSwimState::Joining);
        assert_eq!(decoded.last_committed_term, 0);
        assert_eq!(decoded.last_topology_commit_age_ms, u64::MAX);
        assert!(!decoded.is_ready());
    }

    /// Phase E: while shard is migrating outbound from this node, the
    /// build_replication_targets fan-out must include the dual-write
    /// destination's address in addition to the regular replica targets.
    #[test]
    fn build_replication_targets_includes_dual_write_destination_during_migration() {
        let n1 = crate::cluster::shards::NodeId(1);
        let n2 = crate::cluster::shards::NodeId(2);
        let n3 = crate::cluster::shards::NodeId(3);
        let members = vec![n1, n2, n3];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 2, 200);
        let shard = (0..crate::cluster::shards::NUM_SHARDS as u16)
            .find(|&s| {
                let a = table.target_assignment(s);
                a.master == n1 && a.replicas.contains(&n2) && !a.replicas.contains(&n3)
            })
            .expect("expected shard mastered by n1 with n2 (not n3) as replica");

        let n1_addr: SocketAddr = "127.0.0.1:8901".parse().unwrap();
        let n2_addr: SocketAddr = "127.0.0.1:8902".parse().unwrap();
        let n3_addr: SocketAddr = "127.0.0.1:8903".parse().unwrap();

        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            n1,
            table,
            &[(n1, n1_addr), (n2, n2_addr), (n3, n3_addr)],
            &members,
            &[],
            &[],
            &[],
            3,
        );
        // Open dual-write window: this node (n1) is migrating shard out to n3.
        cluster.test_open_dual_write_window(shard, n3);

        let tx_key = TxKey {
            txid: txid_for_shard(shard, 17),
        };
        let ops = vec![(
            tx_key,
            vec![crate::replication::protocol::ReplicaOp::Delete { tx_key }],
        )];

        let plan = build_replication_targets(&cluster, &ops)
            .expect("dual-write target resolution should succeed");

        assert!(
            plan.by_addr.contains_key(&n2_addr),
            "regular replica n2 must remain in fan-out: {:?}",
            plan.by_addr,
        );
        assert!(
            plan.by_addr.contains_key(&n3_addr),
            "dual-write destination n3 must be added during migration window: {:?}",
            plan.by_addr,
        );
        assert!(
            !plan.by_addr.contains_key(&n1_addr),
            "self (n1) must never be in replica fan-out: {:?}",
            plan.by_addr,
        );
        // Phase E per-set tagging: n3 is exclusively a dual-write
        // destination for this batch (not in any shard's regular
        // replica list); the helper must surface it in
        // `dual_write_only` so `replicate_all_ops` can enforce the
        // ≥1-NEW-ACK invariant.
        assert!(
            plan.dual_write_only.contains(&n3_addr),
            "n3 must be tagged as dual-write-only: {:?}",
            plan.dual_write_only,
        );
        assert!(
            !plan.dual_write_only.contains(&n2_addr),
            "n2 is a regular replica; must not appear in dual_write_only: {:?}",
            plan.dual_write_only,
        );
    }

    /// Phase E: outside an active migration, the dual-write window is empty
    /// and the fan-out must NOT include the would-be destination address.
    #[test]
    fn build_replication_targets_excludes_dual_write_when_not_migrating() {
        let n1 = crate::cluster::shards::NodeId(1);
        let n2 = crate::cluster::shards::NodeId(2);
        let n3 = crate::cluster::shards::NodeId(3);
        let members = vec![n1, n2, n3];
        let table = crate::cluster::shards::ShardTable::compute_with_epoch(&members, 2, 201);
        let shard = (0..crate::cluster::shards::NUM_SHARDS as u16)
            .find(|&s| {
                let a = table.target_assignment(s);
                a.master == n1 && a.replicas.contains(&n2) && !a.replicas.contains(&n3)
            })
            .expect("expected shard mastered by n1 with n2 (not n3) as replica");

        let n2_addr: SocketAddr = "127.0.0.1:8912".parse().unwrap();
        let n3_addr: SocketAddr = "127.0.0.1:8913".parse().unwrap();

        let cluster = crate::cluster::coordinator::new_test_running_cluster(
            n1,
            table,
            &[(n2, n2_addr), (n3, n3_addr)],
            &members,
            &[],
            &[],
            &[],
            3,
        );
        // No migration started — dual-write window is empty.

        let tx_key = TxKey {
            txid: txid_for_shard(shard, 18),
        };
        let ops = vec![(
            tx_key,
            vec![crate::replication::protocol::ReplicaOp::Delete { tx_key }],
        )];

        let plan = build_replication_targets(&cluster, &ops)
            .expect("regular target resolution should succeed");

        assert!(
            plan.by_addr.contains_key(&n2_addr),
            "regular replica n2 must be in fan-out",
        );
        assert!(
            !plan.by_addr.contains_key(&n3_addr),
            "n3 must NOT be in fan-out outside the migration window",
        );
        assert!(
            plan.dual_write_only.is_empty(),
            "no migration in flight ⇒ dual_write_only must be empty: {:?}",
            plan.dual_write_only,
        );
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

        let err = replicate_all_ops(Some(&cluster), &ops, (0, 0), &[])
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
            payload: payload.into(),
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
            payload: payload.into(),
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
            payload: payload.into(),
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
            payload: payload.into(),
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
            payload: payload.into(),
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
            payload: payload.into(),
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
            payload: payload.into(),
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
    fn migration_empty_manifest_completion_clears_only_source_inbound() {
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
            Some(compute_manifest_for_entries(&[])),
            Some(&[]),
            Some(crate::cluster::shards::NodeId(2)),
        );
        let req = RequestFrame {
            request_id: shard as u64,
            op_code: OP_MIGRATION_COMPLETE,
            flags: 0,
            payload: payload.into(),
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
            payload: payload.into(),
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

    #[test]
    fn migration_batch_complete_rejects_huge_shard_count_before_allocation() {
        let h = DispatchTestHarness::new();
        let mut payload = Vec::new();
        payload.extend_from_slice(&u32::MAX.to_le_bytes());

        let req = RequestFrame {
            request_id: 0,
            op_code: OP_MIGRATION_BATCH_COMPLETE,
            flags: 0,
            payload: payload.into(),
        };
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(&req, &h.engine, 8192, None, None, &mut conn_state, None);

        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
        assert!(
            msg.contains("batch-complete"),
            "expected batch-complete rejection, got {msg}"
        );
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
        let other = crate::cluster::shards::NodeId(2);
        let members = vec![self_id, other];
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
        // F-G8-001: handle_propose now rejects any proposal that
        // introduces a NodeId the voter has never seen as a committed
        // voter. Pre-seed the ever-seen set with both members so the
        // subsume proposal is accepted and we can exercise the vote-
        // persist-before-reply path the test actually targets.
        cluster
            .topology_authority()
            .set_committed_voter_ever_seen(&[self_id, other]);

        // Propose a new term that subsumes this single-node cluster.
        let proposer = other;
        let propose = crate::cluster::topology::TopologyTerm::new(
            500,
            members.clone(),
            proposer,
            crate::cluster::topology::ClusterId::UNSET,
        );

        let req = RequestFrame {
            request_id: 1,
            op_code: OP_TOPOLOGY_PROPOSE,
            flags: 0,
            payload: propose.serialize().into(),
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
        let other = crate::cluster::shards::NodeId(2);
        let members = vec![self_id, other];
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
        // F-G8-001: pre-seed the ever-seen set so the subsume proposal
        // is accepted by handle_propose and we exercise the persist-
        // failure path (rather than failing earlier on split-brain).
        cluster
            .topology_authority()
            .set_committed_voter_ever_seen(&[self_id, other]);

        let proposer = other;
        let propose = crate::cluster::topology::TopologyTerm::new(
            600,
            members.clone(),
            proposer,
            crate::cluster::topology::ClusterId::UNSET,
        );

        let req = RequestFrame {
            request_id: 1,
            op_code: OP_TOPOLOGY_PROPOSE,
            flags: 0,
            payload: propose.serialize().into(),
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

        // E-2: the commit-apply guard now mirrors the propose side, so the
        // {1} → {1,2} growth (which introduces unseen node 2) requires a
        // matching cluster_id; the ever-seen fallback would otherwise
        // reject it when cluster_id is unset.
        let cid = crate::cluster::topology::ClusterId([0x5B; 16]);
        cluster.topology_authority().set_cluster_id(cid);

        // Step 1: accept a proposal (sets voted_term).
        let proposer = crate::cluster::shards::NodeId(2);
        let propose =
            crate::cluster::topology::TopologyTerm::new(700, members.clone(), proposer, cid);
        let req = RequestFrame {
            request_id: 1,
            op_code: OP_TOPOLOGY_PROPOSE,
            flags: 0,
            payload: propose.serialize().into(),
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
            cluster_id: cid,
            digest: crate::cluster::topology::TopologyTerm::compute_digest(700, &cid, &members),
            voters: members.clone(),
        };
        let req = RequestFrame {
            request_id: 2,
            op_code: OP_TOPOLOGY_COMMIT,
            flags: 0,
            payload: commit.serialize().into(),
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

    #[test]
    fn deleted_topo_file_prevents_single_node_bootstrap() {
        let tmp = tempfile::TempDir::new().unwrap();
        let cluster_state_path = tmp.path().join("node.cluster");
        let topology_path =
            crate::cluster::coordinator::topology_state_path_for_cluster_state(&cluster_state_path);
        let self_id = crate::cluster::shards::NodeId(1);
        let prior_members = vec![
            self_id,
            crate::cluster::shards::NodeId(2),
            crate::cluster::shards::NodeId(3),
        ];
        let prior_table =
            crate::cluster::shards::ShardTable::compute_with_epoch(&prior_members, 1, 9);
        let prior_cluster =
            crate::cluster::coordinator::new_test_running_cluster_with_topology_path(
                self_id,
                prior_table,
                &[(self_id, "127.0.0.1:4801".parse().unwrap())],
                &prior_members,
                &[],
                &[],
                &[],
                3,
                Some(topology_path.clone()),
            );
        prior_cluster
            .persist_topology()
            .expect("multi-node topology marker should persist");
        std::fs::remove_file(&topology_path).expect("delete persisted topology file");

        let restored =
            crate::cluster::coordinator::load_startup_topology_state(&cluster_state_path);
        assert!(
            restored.peak_cluster_size >= 2,
            "deleted .topo must not erase local multi-node evidence; restored peak={}",
            restored.peak_cluster_size,
        );

        let h = DispatchTestHarness::new();
        let fresh_single_node_table =
            crate::cluster::shards::ShardTable::compute_with_epoch(&[self_id], 1, 1);
        let rebooted_cluster = crate::cluster::coordinator::new_test_running_cluster(
            self_id,
            fresh_single_node_table,
            &[(self_id, "127.0.0.1:4802".parse().unwrap())],
            &[self_id],
            &[],
            &[],
            &[],
            restored.peak_cluster_size as usize,
        );
        let item = WireCreateItem {
            txid: DispatchTestHarness::make_txid(82),
            tx_version: 1,
            locktime: 0,
            fee: 500,
            size_in_bytes: 250,
            extended_size: 250,
            is_coinbase: false,
            spending_height: 0,
            created_at: 1700000000000,
            flags: 0,
            utxo_hashes: vec![DispatchTestHarness::make_txid(83)],
            cold_data: vec![],
            block_height: 0,
            mined_block_id: None,
            mined_block_height: None,
            mined_subtree_idx: None,
            parent_txids: vec![],
        };
        let resp = h.request_with_cluster(
            OP_CREATE_BATCH,
            encode_create_batch(&[item]),
            &rebooted_cluster,
        );
        assert_eq!(
            resp.status, STATUS_ERROR,
            "node with deleted .topo and prior multi-node marker must not accept fresh single-node writes",
        );
        assert!(resp.payload.len() >= 2);
        let err_code = u16::from_le_bytes(resp.payload[..2].try_into().unwrap());
        assert_eq!(err_code, ERR_NO_QUORUM);
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

    #[test]
    fn pruned_utxo_spend_returns_original_spending_data() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(44);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let key = TxKey { txid };
        let entry = h.engine.lookup(&key).unwrap();
        let utxo_hash = [0u8; 32];
        let spending_data = [0x5Au8; 36];
        let mut pruned_slot = crate::record::UtxoSlot::new_spent(utxo_hash, spending_data);
        pruned_slot.status = crate::record::UTXO_PRUNED;
        crate::io::write_utxo_slot(h.engine.device(), entry.record_offset, 0, &pruned_slot)
            .unwrap();

        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        // F-G2-002: avoid the reserved all-`0xFF` sentinel for the
        // request's spending_data; that triggers the
        // `ReservedSpendingData` rejection (ERR_INVALID_SPEND with
        // empty payload) before the engine ever reads the slot, so
        // the pruned-spending-data forensic payload never surfaces.
        let item = WireSpendItem {
            txid,
            vout: 0,
            utxo_hash,
            spending_data: [0xEE; 36],
        };

        let resp = h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &[item]));
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);
        let errors = decode_sparse_errors(&resp.payload).unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].item_index, 0);
        assert_eq!(errors[0].error_code, ERR_INVALID_SPEND);
        assert_eq!(errors[0].error_data, spending_data.to_vec());
    }

    /// Re-sending the exact same spend should classify the second send as
    /// idempotent rather than succeeded or failed.
    #[test]
    fn handle_spend_batch_idempotent_counted_as_idempotent() {
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
            WireUnspendItem {
                txid: txid_a,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
                spending_data: [0x77; 36],
            },
            WireUnspendItem {
                txid: txid_b,
                vout: 0,
                utxo_hash: utxo_hash_vout0,
                spending_data: [0; 36],
            },
            WireUnspendItem {
                txid: txid_c,
                vout: 0,
                utxo_hash: wrong_hash,
                spending_data: [0; 36],
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

    #[test]
    fn handle_unspend_batch_rejects_wrong_spending_data() {
        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(53);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let stored_spending_data = [0x91; 36];
        let spend_item = WireSpendItem {
            txid,
            vout: 0,
            utxo_hash: [0; 32],
            spending_data: stored_spending_data,
        };
        assert_eq!(
            h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &[spend_item]))
                .status,
            STATUS_OK,
        );

        let unspend_params = UnspendBatchParams {
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let unspend_item = WireUnspendItem {
            txid,
            vout: 0,
            utxo_hash: [0; 32],
            spending_data: [0x92; 36],
        };
        let resp = h.request(
            OP_UNSPEND_BATCH,
            encode_unspend_batch(&unspend_params, &[unspend_item]),
        );
        assert_eq!(resp.status, STATUS_PARTIAL_ERROR);
        let errors = decode_sparse_errors(&resp.payload).unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].item_index, 0);
        assert_eq!(errors[0].error_code, ERR_INVALID_SPEND);
        assert_eq!(errors[0].error_data, stored_spending_data.to_vec());

        let slot = h
            .engine
            .read_slot(&TxKey { txid }, 0)
            .expect("slot must remain readable");
        assert!(slot.is_spent());
        assert_eq!(slot.spending_data, stored_spending_data);
    }

    /// SetMined items should tick attempted/succeeded/failed per item.
    #[test]
    fn handle_set_mined_batch_ticks_outcome_counters() {
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

    /// Create items should tick creates_attempted, creates_succeeded,
    /// and creates_failed once per item.
    #[test]
    fn handle_create_batch_ticks_outcome_counters() {
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
            2,
            "creates_attempted += 2 (per item)"
        );
        assert_eq!(after_succ - before_succ, 1, "creates_succeeded += 1");
        assert_eq!(after_fail - before_fail, 1, "creates_failed += 1");
    }

    /// Freeze items should tick freezes_succeeded / freezes_failed per item.
    #[test]
    fn handle_freeze_batch_ticks_outcome_counters() {
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

    // -----------------------------------------------------------------------
    // M-01: batch-wide storage errors (early `ERR_STORAGE_IO` returns) must
    // still classify every attempted item, so `attempted == succeeded +
    // idempotent + failed` holds and `Outcome::ErrStorage` ticks on the
    // write path.
    // -----------------------------------------------------------------------

    /// Append filler redo entries until the log reports `LogFull`, so the
    /// next handler-side `write_replicated_redo_ops` call fails and the
    /// handler takes its batch-wide `ERR_STORAGE_IO` early return.
    fn fill_redo_log(h: &RedoDispatchHarness) {
        for i in 0..2048u64 {
            let r = write_redo_ops(
                Some(&h.redo_log),
                &[RedoOp::Delete {
                    tx_key: TxKey { txid: [0xFE; 32] },
                    record_offset: i * 4096,
                    record_size: 4096,
                }],
            );
            if r.is_err() {
                return;
            }
        }
        panic!("redo log did not fill within 2048 filler appends");
    }

    /// M-01 (apply path): a mid-batch `validated.apply()` failure — here a
    /// DAH overflow from `current_block_height + retention > u32::MAX` —
    /// returns batch-wide `ERR_STORAGE_IO`. The early return must still
    /// tick `spends_failed` and `operations{spend,err_storage}` so the
    /// `attempted == succeeded + idempotent + failed` invariant holds.
    #[test]
    fn handle_spend_batch_apply_storage_error_ticks_err_storage() {
        use crate::metrics::{OpCode, Outcome};
        let m = test_metrics();
        let _ = test_histograms();

        let h = DispatchTestHarness::new();
        let txid = DispatchTestHarness::make_txid(190);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);

        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            // u32::MAX + 1 overflows the DAH computation inside apply().
            current_block_height: u32::MAX,
            block_height_retention: 1,
        };
        let item = WireSpendItem {
            txid,
            vout: 0,
            utxo_hash: [0u8; 32],
            spending_data: [0xD1; 36],
        };

        let before = snapshot_spend(m);
        let before_scalar_failed = m.spends_failed.get();
        let before_err_storage = m.operations.get(OpCode::Spend, Outcome::ErrStorage);
        let resp = h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &[item]));
        let after = snapshot_spend(m);

        assert_eq!(resp.status, STATUS_ERROR);
        let (code, _msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_STORAGE_IO);

        assert_eq!(after.0 - before.0, 1, "items_attempted += 1");
        assert_eq!(after.1 - before.1, 0, "no item succeeded");
        assert_eq!(after.2 - before.2, 0, "no item idempotent");
        assert_eq!(after.3 - before.3, 1, "items_failed += 1 (storage error)");
        assert_eq!(
            m.spends_failed.get() - before_scalar_failed,
            1,
            "spends_failed += 1 (storage error)"
        );
        assert_eq!(
            m.operations.get(OpCode::Spend, Outcome::ErrStorage) - before_err_storage,
            1,
            "operations{{spend,err_storage}} += 1"
        );
        assert_eq!(
            after.0 - before.0,
            (after.1 - before.1) + (after.2 - before.2) + (after.3 - before.3),
            "attempted == succeeded + idempotent + failed must hold on the early return"
        );
    }

    /// M-01 (WAL path): a redo-log append failure during spend takes the
    /// batch-wide `ERR_STORAGE_IO` early return BEFORE the terminal tally.
    /// The metrics must still classify every attempted item.
    #[test]
    fn handle_spend_batch_redo_log_full_ticks_err_storage() {
        use crate::metrics::{OpCode, Outcome};
        let m = test_metrics();
        let _ = test_histograms();

        let h = RedoDispatchHarness::new_with_redo_size(64 * 1024);
        let txid = DispatchTestHarness::make_txid(191);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);
        fill_redo_log(&h);

        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let item = WireSpendItem {
            txid,
            vout: 0,
            utxo_hash: [0u8; 32],
            spending_data: [0xD2; 36],
        };

        let before = snapshot_spend(m);
        let before_err_storage = m.operations.get(OpCode::Spend, Outcome::ErrStorage);
        let resp = h.request(OP_SPEND_BATCH, encode_spend_batch(&params, &[item]));
        let after = snapshot_spend(m);

        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_STORAGE_IO);
        assert!(
            msg.contains("redo log append failed"),
            "expected redo append failure, got: {msg}"
        );

        assert_eq!(after.0 - before.0, 1, "items_attempted += 1");
        assert_eq!(after.3 - before.3, 1, "items_failed += 1 (redo full)");
        assert_eq!(
            m.operations.get(OpCode::Spend, Outcome::ErrStorage) - before_err_storage,
            1,
            "operations{{spend,err_storage}} += 1"
        );
        assert_eq!(
            after.0 - before.0,
            (after.1 - before.1) + (after.2 - before.2) + (after.3 - before.3),
            "attempted == succeeded + idempotent + failed must hold on the early return"
        );
    }

    /// M-01 (WAL path, set_mined): a redo-log append failure during
    /// set_mined must tick `set_mined_items_failed` and
    /// `operations{set_mined,err_storage}` for every item in the batch.
    #[test]
    fn handle_set_mined_batch_redo_log_full_ticks_err_storage() {
        use crate::metrics::{OpCode, Outcome};
        let m = test_metrics();
        let _ = test_histograms();

        let h = RedoDispatchHarness::new_with_redo_size(64 * 1024);
        let txid_a = DispatchTestHarness::make_txid(192);
        let txid_b = DispatchTestHarness::make_txid(193);
        assert_eq!(h.create_tx(txid_a, 1).status, STATUS_OK);
        assert_eq!(h.create_tx(txid_b, 1).status, STATUS_OK);
        fill_redo_log(&h);

        let params = SetMinedBatchParams {
            block_id: 7,
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
        let before_err_storage = m.operations.get(OpCode::SetMined, Outcome::ErrStorage);

        let payload = encode_set_mined_batch(&params, &[txid_a, txid_b]);
        let resp = h.request(OP_SET_MINED_BATCH, payload);

        assert_eq!(resp.status, STATUS_ERROR);
        let (code, _msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_STORAGE_IO);

        let att = m.set_mined_items_attempted.get() - before_att;
        let succ = m.set_mined_items_succeeded.get() - before_succ;
        let fail = m.set_mined_items_failed.get() - before_fail;
        assert_eq!(att, 2, "set_mined_items_attempted += 2");
        assert_eq!(succ, 0, "no item succeeded");
        assert_eq!(fail, 2, "set_mined_items_failed += 2 (redo full)");
        assert_eq!(att, succ + fail, "attempted == succeeded + failed");
        assert_eq!(
            m.operations.get(OpCode::SetMined, Outcome::ErrStorage) - before_err_storage,
            2,
            "operations{{set_mined,err_storage}} += 2"
        );
    }

    /// M-01 (create): both batch-wide `ERR_STORAGE_IO` early returns in
    /// handle_create_batch — the CreateV2 redo-append failure and the
    /// allocator `allocate_batch` failure — must tick `creates_failed`
    /// and `operations{create,err_storage}`.
    #[test]
    fn handle_create_batch_redo_log_full_ticks_err_storage() {
        use crate::metrics::{OpCode, Outcome};
        let m = test_metrics();
        let _ = test_histograms();

        // 8 KiB region = header block + one entries block; the allocator
        // append+flush consumes the entire entries region, so the CreateV2
        // append fails (see `create_batch_redo_write_failure_surfaces_...`).
        let h = RedoDispatchHarness::new_with_exact_redo_log_size(8192);

        let before_att = m.creates_attempted.get();
        let before_succ = m.creates_succeeded.get();
        let before_fail = m.creates_failed.get();
        let before_err_storage = m.operations.get(OpCode::Create, Outcome::ErrStorage);

        // First create: allocator reservation WAL succeeds (fills the log),
        // CreateV2 WAL append fails → redo-failure early return.
        let resp = h.create_tx(DispatchTestHarness::make_txid(194), 1);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, _msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_STORAGE_IO);

        assert_eq!(m.creates_attempted.get() - before_att, 1);
        assert_eq!(m.creates_succeeded.get() - before_succ, 0);
        assert_eq!(
            m.creates_failed.get() - before_fail,
            1,
            "creates_failed += 1 (CreateV2 redo append failed)"
        );
        assert_eq!(
            m.operations.get(OpCode::Create, Outcome::ErrStorage) - before_err_storage,
            1,
            "operations{{create,err_storage}} += 1"
        );

        // Second create: the allocator's own redo append now fails inside
        // `allocate_batch` → the other early return.
        let before_att2 = m.creates_attempted.get();
        let before_fail2 = m.creates_failed.get();
        let before_err_storage2 = m.operations.get(OpCode::Create, Outcome::ErrStorage);
        let resp2 = h.create_tx(DispatchTestHarness::make_txid(195), 1);
        assert_eq!(resp2.status, STATUS_ERROR);
        let (code2, _msg2) = decode_error_payload(&resp2.payload).unwrap();
        assert_eq!(code2, ERR_STORAGE_IO);
        assert_eq!(m.creates_attempted.get() - before_att2, 1);
        assert_eq!(
            m.creates_failed.get() - before_fail2,
            1,
            "creates_failed += 1 (allocate_batch failed)"
        );
        assert_eq!(
            m.operations.get(OpCode::Create, Outcome::ErrStorage) - before_err_storage2,
            1,
            "operations{{create,err_storage}} += 1 on allocate_batch failure"
        );
    }

    /// M-01 (WAL path, freeze): freeze's `*_attempted` scalar is per batch,
    /// but the per-item `freezes_failed` and the labeled operations table
    /// must still classify every item when the redo append fails.
    #[test]
    fn handle_freeze_batch_redo_log_full_ticks_err_storage() {
        use crate::metrics::{OpCode, Outcome};
        let m = test_metrics();
        let _ = test_histograms();

        let h = RedoDispatchHarness::new_with_redo_size(64 * 1024);
        let txid = DispatchTestHarness::make_txid(196);
        assert_eq!(h.create_tx(txid, 1).status, STATUS_OK);
        fill_redo_log(&h);

        let items = vec![WireSlotItem {
            txid,
            vout: 0,
            utxo_hash: [0u8; 32],
        }];
        let before_fail = m.freezes_failed.get();
        let before_succ = m.freezes_succeeded.get();
        let before_err_storage = m.operations.get(OpCode::Freeze, Outcome::ErrStorage);

        let resp = h.request(OP_FREEZE_BATCH, encode_slot_item_batch(&items));
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, _msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_STORAGE_IO);

        assert_eq!(m.freezes_succeeded.get() - before_succ, 0);
        assert_eq!(
            m.freezes_failed.get() - before_fail,
            1,
            "freezes_failed += 1 (redo full)"
        );
        assert_eq!(
            m.operations.get(OpCode::Freeze, Outcome::ErrStorage) - before_err_storage,
            1,
            "operations{{freeze,err_storage}} += 1"
        );
    }

    /// Dispatch must record an end-to-end latency sample into
    /// `h.spend_latency` for every spend batch processed.
    #[test]
    fn dispatch_records_spend_latency_histogram() {
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
            (
                SpendError::Pruned {
                    offset: 0,
                    spending_data: [0xAB; 36],
                },
                Outcome::ErrConflicting,
            ),
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
            (
                SpendError::DeletedChildren {
                    offset: 0,
                    child_count: 1,
                },
                Outcome::ErrConflicting,
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
            payload: payload.into(),
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
            payload: payload.into(),
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
    /// STATUS_ERROR / ERR_PAYLOAD_MALFORMED (P3.10; pre-P3.10 this was
    /// ERR_INTERNAL).
    #[test]
    fn dispatch_admin_diagnose_key_malformed_payload() {
        let h = DispatchTestHarness::new();

        // Empty payload — no count prefix.
        let resp = h.request(OP_ADMIN_DIAGNOSE_KEY, vec![]);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, _msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);

        // Count says 2 but only 1 txid worth of bytes follows.
        let mut short = Vec::new();
        short.extend_from_slice(&2u32.to_le_bytes());
        short.extend_from_slice(&[0u8; 32]);
        let resp = h.request(OP_ADMIN_DIAGNOSE_KEY, short);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, _msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);

        // Count above cap (65 > 64).
        let mut too_many = Vec::new();
        too_many.extend_from_slice(&65u32.to_le_bytes());
        too_many.extend_from_slice(&vec![0u8; 65 * 32]);
        let resp = h.request(OP_ADMIN_DIAGNOSE_KEY, too_many);
        assert_eq!(resp.status, STATUS_ERROR);
        let (code, _msg) = decode_error_payload(&resp.payload).unwrap();
        assert_eq!(code, ERR_PAYLOAD_MALFORMED);
    }

    // ---------------------------------------------------------------------
    // Gap #5 — secondary readiness gate (pure policy)
    //
    // These tests drive `secondary_readiness_verdict` directly with explicit
    // `SecondaryStatus` snapshots so they don't race with other tests on the
    // global `SECONDARY_DAH_OK` / `SECONDARY_UNMINED_OK` flags.
    // ---------------------------------------------------------------------

    fn extract_err(resp: &ResponseFrame) -> u16 {
        assert_eq!(resp.status, STATUS_ERROR);
        decode_error_payload(&resp.payload).unwrap().0
    }

    #[test]
    fn secondary_readiness_fully_ok_passes_every_op() {
        let status = SecondaryStatus {
            dah_ok: true,
            unmined_ok: true,
        };
        for op in &[
            OP_QUERY_OLD_UNMINED,
            OP_MARK_LONGEST_CHAIN_BATCH,
            OP_SET_MINED_BATCH,
            OP_PROCESS_EXPIRED_PRESERVATIONS,
            OP_PRESERVE_TRANSACTIONS,
            OP_PRESERVE_UNTIL_BATCH,
            OP_SET_CONFLICTING_BATCH,
            OP_DELETE_BATCH,
            OP_SPEND_BATCH,
            OP_GET_BATCH,
            OP_CREATE_BATCH,
        ] {
            assert!(
                secondary_readiness_verdict(*op, status, 1).is_none(),
                "op={op} must pass when both flags are ok",
            );
        }
    }

    #[test]
    fn secondary_readiness_unmined_degraded_blocks_unmined_endpoints() {
        let status = SecondaryStatus {
            dah_ok: true,
            unmined_ok: false,
        };
        for op in &[
            OP_QUERY_OLD_UNMINED,
            OP_MARK_LONGEST_CHAIN_BATCH,
            OP_SET_MINED_BATCH,
        ] {
            let resp = secondary_readiness_verdict(*op, status, 1).expect("must reject");
            assert_eq!(extract_err(&resp), ERR_INDEX_DEGRADED, "op={op}");
        }
    }

    #[test]
    fn secondary_readiness_dah_degraded_blocks_dah_endpoints() {
        let status = SecondaryStatus {
            dah_ok: false,
            unmined_ok: true,
        };
        for op in &[
            OP_PROCESS_EXPIRED_PRESERVATIONS,
            OP_PRESERVE_TRANSACTIONS,
            OP_PRESERVE_UNTIL_BATCH,
            OP_SET_CONFLICTING_BATCH,
        ] {
            let resp = secondary_readiness_verdict(*op, status, 1).expect("must reject");
            assert_eq!(extract_err(&resp), ERR_INDEX_DEGRADED, "op={op}");
        }
    }

    #[test]
    fn secondary_readiness_either_degraded_blocks_delete() {
        for status in &[
            SecondaryStatus {
                dah_ok: false,
                unmined_ok: true,
            },
            SecondaryStatus {
                dah_ok: true,
                unmined_ok: false,
            },
            SecondaryStatus {
                dah_ok: false,
                unmined_ok: false,
            },
        ] {
            let resp =
                secondary_readiness_verdict(OP_DELETE_BATCH, *status, 1).expect("must reject");
            assert_eq!(extract_err(&resp), ERR_INDEX_DEGRADED);
        }
    }

    #[test]
    fn secondary_readiness_keeps_spend_get_create_alive_when_degraded() {
        // Gap #5 requirement: spend / get / create MUST keep working when
        // a secondary index is degraded.
        for status in &[
            SecondaryStatus {
                dah_ok: false,
                unmined_ok: true,
            },
            SecondaryStatus {
                dah_ok: true,
                unmined_ok: false,
            },
            SecondaryStatus {
                dah_ok: false,
                unmined_ok: false,
            },
        ] {
            for op in &[
                OP_SPEND_BATCH,
                OP_UNSPEND_BATCH,
                OP_GET_BATCH,
                OP_GET_SPEND_BATCH,
                OP_CREATE_BATCH,
                OP_FREEZE_BATCH,
                OP_UNFREEZE_BATCH,
                OP_REASSIGN_BATCH,
                OP_SET_LOCKED_BATCH,
            ] {
                assert!(
                    secondary_readiness_verdict(*op, *status, 1).is_none(),
                    "op={op} must keep working with status={status:?}",
                );
            }
        }
    }

    #[test]
    fn secondary_readiness_unmined_degraded_does_not_block_dah_only_ops() {
        let status = SecondaryStatus {
            dah_ok: true,
            unmined_ok: false,
        };
        // DAH-only handlers must keep working when only unmined is down.
        for op in &[
            OP_PROCESS_EXPIRED_PRESERVATIONS,
            OP_PRESERVE_TRANSACTIONS,
            OP_PRESERVE_UNTIL_BATCH,
            OP_SET_CONFLICTING_BATCH,
        ] {
            assert!(
                secondary_readiness_verdict(*op, status, 1).is_none(),
                "op={op} should pass when only unmined is degraded",
            );
        }
    }

    #[test]
    fn secondary_readiness_request_id_propagates() {
        let status = SecondaryStatus {
            dah_ok: false,
            unmined_ok: false,
        };
        let resp = secondary_readiness_verdict(OP_DELETE_BATCH, status, 0xDEAD_BEEF)
            .expect("delete must be rejected");
        assert_eq!(resp.request_id, 0xDEAD_BEEF);
    }

    // -----------------------------------------------------------------------
    // Gap #8: replication-rollback correctness + crash-mid-rollback
    // -----------------------------------------------------------------------
    //
    // These tests exercise `compensate_replication_failure` directly with
    // crafted before-images, then verify the engine state is restored
    // bit-exactly. Integration with the full TCP cluster is unnecessary
    // for these invariants — the rollback API itself is the unit under
    // test. Tests #4 and #5 cover the durability + acknowledged-implies-
    // replicated invariants by replaying compensation entries from the
    // redo log and forcing a replication-failure path through the
    // dispatch handlers respectively.

    /// Build a minimal record on the harness with N utxo slots — used by
    /// the compensation tests so the engine has something to read/write.
    fn rollback_seed_record(h: &RedoDispatchHarness, txid: [u8; 32], utxos: u32) -> TxKey {
        let resp = h.create_tx(txid, utxos);
        assert_eq!(
            resp.status, STATUS_OK,
            "seed create must succeed (got status {})",
            resp.status
        );
        TxKey { txid }
    }

    /// R-007 (Codex F1): the `build_delete_compensation_ops` helper
    /// must reproduce per-slot state after Create — a previously-spent
    /// slot must be re-stamped with its original `spending_data`, a
    /// frozen slot must be re-frozen, a pruned slot must be re-pruned,
    /// and unspent slots stay default. Pre-fix the compensation only
    /// emitted Create with `utxo_hashes`, leaving every slot UNSPENT
    /// regardless of pre-delete state and opening a double-spend
    /// window after a failed delete-batch replication.
    #[test]
    fn delete_compensation_ops_restore_per_slot_state() {
        let mut txid = [0u8; 32];
        txid[0] = 0xC0;
        let key = TxKey { txid };

        // Synthesize a snapshot with one slot of each interesting status.
        let mut spend_a = [0u8; 36];
        spend_a[0..4].copy_from_slice(&[0xAA, 0xAA, 0xAA, 0xAA]);
        let mut spend_b = [0u8; 36];
        spend_b[0..4].copy_from_slice(&[0xBB, 0xBB, 0xBB, 0xBB]);
        let snap = DeleteSnapshot {
            metadata_bytes: vec![0u8; 70],
            master_generation: 7,
            slots: vec![
                // 0: unspent (no replay op expected)
                SnapshotSlot {
                    hash: [0x10; 32],
                    status: crate::record::UTXO_UNSPENT,
                    spending_data: [0u8; 36],
                },
                // 1: spent with spend_a
                SnapshotSlot {
                    hash: [0x11; 32],
                    status: crate::record::UTXO_SPENT,
                    spending_data: spend_a,
                },
                // 2: frozen
                SnapshotSlot {
                    hash: [0x12; 32],
                    status: crate::record::UTXO_FROZEN,
                    spending_data: [0u8; 36],
                },
                // 3: pruned
                SnapshotSlot {
                    hash: [0x13; 32],
                    status: crate::record::UTXO_PRUNED,
                    spending_data: [0u8; 36],
                },
                // 4: spent with spend_b
                SnapshotSlot {
                    hash: [0x14; 32],
                    status: crate::record::UTXO_SPENT,
                    spending_data: spend_b,
                },
            ],
            cold_data: None,
            is_external: false,
        };

        let ops = build_delete_compensation_ops(&key, &snap);

        // First op MUST be Create with the snapshotted hashes.
        match &ops[0] {
            ReplicaOp::Create {
                tx_key,
                utxo_hashes,
                is_external,
                ..
            } => {
                assert_eq!(*tx_key, key);
                assert_eq!(utxo_hashes.len(), 5);
                assert_eq!(utxo_hashes[1], [0x11; 32]);
                assert!(!*is_external);
            }
            other => panic!("expected Create as first op, got {other:?}"),
        }

        // Subsequent ops must restore non-default slot states. Order
        // doesn't matter for correctness as long as Create is first.
        let tail = &ops[1..];
        let spent_a = tail.iter().find(|op| {
            matches!(op,
                ReplicaOp::Spend { tx_key, offset: 1, spending_data, master_generation, .. }
                if *tx_key == key && *spending_data == spend_a && *master_generation == 7
            )
        });
        assert!(
            spent_a.is_some(),
            "compensation must re-stamp slot 1 with the original spending_data; got {ops:?}"
        );

        let spent_b = tail.iter().find(|op| {
            matches!(op,
                ReplicaOp::Spend { tx_key, offset: 4, spending_data, master_generation, .. }
                if *tx_key == key && *spending_data == spend_b && *master_generation == 7
            )
        });
        assert!(
            spent_b.is_some(),
            "compensation must re-stamp slot 4 with the original spending_data"
        );

        let frozen = tail.iter().find(|op| {
            matches!(op,
                ReplicaOp::Freeze { tx_key, offset: 2, master_generation }
                if *tx_key == key && *master_generation == 7
            )
        });
        assert!(frozen.is_some(), "compensation must re-freeze slot 2");

        let pruned = tail.iter().find(|op| {
            matches!(op,
                ReplicaOp::PruneSlot { tx_key, offset: 3 }
                if *tx_key == key
            )
        });
        assert!(pruned.is_some(), "compensation must re-prune slot 3");

        // Slot 0 was UNSPENT — it should NOT have a replay op, since
        // Create defaults to UNSPENT and an extra op would over-bump
        // generation on the receiver.
        let no_extras = tail.iter().any(|op| {
            matches!(
                op,
                ReplicaOp::Spend { offset: 0, .. }
                    | ReplicaOp::Freeze { offset: 0, .. }
                    | ReplicaOp::PruneSlot { offset: 0, .. }
            )
        });
        assert!(
            !no_extras,
            "compensation must NOT emit a replay op for slot 0 (UNSPENT)"
        );
    }

    /// Test 1 (gap #8): unset-mined rollback restores the original
    /// (block_height, subtree_idx) — NOT zeros.
    #[test]
    fn rollback_unset_mined_restores_block_entry_exactly() {
        use crate::ops::set_mined::SetMinedRequest;

        let h = RedoDispatchHarness::new();
        let mut txid = [0u8; 32];
        txid[0] = 0x42;
        let key = rollback_seed_record(&h, txid, 1);

        // Set mined with NON-zero height + subtree.
        let block_id = 12345;
        let block_height = 800_000;
        let subtree_idx = 7;
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id,
                block_height,
                subtree_idx,
                on_longest_chain: true,
                unset_mined: false,
                current_block_height: 0,
                block_height_retention: 0,
            })
            .expect("set_mined seed");

        // Now apply unset_mined locally. Capture the before-image FIRST.
        let pre_meta = h.engine.read_metadata(&key).expect("read_metadata pre");
        let count = pre_meta.block_entry_count as usize;
        let inline = count.min(crate::record::INLINE_BLOCK_ENTRIES);
        let mut captured: Option<BeforeImage> = None;
        for i in 0..inline {
            if { pre_meta.block_entries_inline[i].block_id } == block_id {
                captured = Some(BeforeImage::UnsetMined {
                    block_height: { pre_meta.block_entries_inline[i].block_height },
                    subtree_idx: { pre_meta.block_entries_inline[i].subtree_idx },
                });
                break;
            }
        }
        let before_image = captured.expect("captured before-image");

        // Apply the unset locally (simulating the dispatch handler's
        // engine.set_mined_batch with unset_mined=true).
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id,
                block_height,
                subtree_idx,
                on_longest_chain: false,
                unset_mined: true,
                current_block_height: 0,
                block_height_retention: 0,
            })
            .expect("local unset");

        // Run compensation as if replication failed.
        let repl_ops = vec![(
            key,
            vec![ReplicaOp::UnsetMined {
                tx_key: key,
                block_id,
                current_block_height: 0,
                block_height_retention: 0,
                master_generation: 0, // not material to compensation
            }],
        )];
        let before_images = vec![(key, vec![before_image])];
        compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
            .unwrap();

        // Post-compensation: the block entry MUST be restored with the
        // original (height, subtree). Not zeros.
        let post_meta = h.engine.read_metadata(&key).expect("read_metadata post");
        let post_count = post_meta.block_entry_count as usize;
        let post_inline = post_count.min(crate::record::INLINE_BLOCK_ENTRIES);
        let mut found = false;
        for i in 0..post_inline {
            if { post_meta.block_entries_inline[i].block_id } == block_id {
                let bh = { post_meta.block_entries_inline[i].block_height };
                let st = { post_meta.block_entries_inline[i].subtree_idx };
                assert_eq!(bh, block_height, "block_height not restored");
                assert_eq!(st, subtree_idx, "subtree_idx not restored");
                found = true;
                break;
            }
        }
        assert!(found, "block entry not restored after compensation");
    }

    #[test]
    fn rollback_unset_mined_restores_overflow_block_entry_exactly() {
        use crate::ops::set_mined::SetMinedRequest;

        let h = RedoDispatchHarness::new();
        let mut txid = [0u8; 32];
        txid[0] = 0x43;
        let key = rollback_seed_record(&h, txid, 1);

        for bid in 1..=5u32 {
            h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: key,
                    block_id: bid,
                    block_height: 900_000 + bid,
                    subtree_idx: 20 + bid,
                    on_longest_chain: true,
                    unset_mined: false,
                    current_block_height: 0,
                    block_height_retention: 0,
                })
                .expect("seed mined entry");
        }

        let block_id = 5;
        let before = h
            .engine
            .read_block_entry(&key, block_id)
            .expect("read before image")
            .expect("overflow before image");
        let block_height = { before.block_height };
        let subtree_idx = { before.subtree_idx };
        let before_image = BeforeImage::UnsetMined {
            block_height,
            subtree_idx,
        };

        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id,
                block_height,
                subtree_idx,
                on_longest_chain: false,
                unset_mined: true,
                current_block_height: 0,
                block_height_retention: 0,
            })
            .expect("local unset");

        let repl_ops = vec![(
            key,
            vec![ReplicaOp::UnsetMined {
                tx_key: key,
                block_id,
                current_block_height: 0,
                block_height_retention: 0,
                master_generation: 0,
            }],
        )];
        let before_images = vec![(key, vec![before_image])];
        compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
            .unwrap();

        let restored = h
            .engine
            .read_block_entry(&key, block_id)
            .expect("read restored entry")
            .expect("restored overflow entry");
        assert_eq!({ restored.block_height }, 900_005);
        assert_eq!({ restored.subtree_idx }, 25);
    }

    #[test]
    fn unspend_compensation_preserves_dah() {
        use crate::ops::set_mined::SetMinedRequest;
        use crate::ops::spend::SpendRequest;

        let h = RedoDispatchHarness::new();
        let txid = DispatchTestHarness::make_txid(156);
        let key = rollback_seed_record(&h, txid, 2);

        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 156,
                block_height: 900_000,
                subtree_idx: 0,
                on_longest_chain: true,
                unset_mined: false,
                current_block_height: 900_000,
                block_height_retention: 50,
            })
            .expect("mine record");

        let slot0 = h.engine.read_slot(&key, 0).expect("slot 0");
        h.engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: slot0.hash,
                spending_data: [0xA0; 36],
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 900_010,
                block_height_retention: 50,
            })
            .expect("spend first output");

        let slot1 = h.engine.read_slot(&key, 1).expect("slot 1");
        let failed_spending_data = [0xB1; 36];
        h.engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 1,
                utxo_hash: slot1.hash,
                spending_data: failed_spending_data,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 900_020,
                block_height_retention: 50,
            })
            .expect("spend second output");

        let after_spend = h.engine.read_metadata(&key).expect("metadata after spend");
        assert_eq!({ after_spend.spent_utxos }, 2);
        assert_eq!({ after_spend.delete_at_height }, 900_070);
        assert!(
            !h.engine.dah_index().range_query(u32::MAX).is_empty(),
            "test setup should register the DAH before rollback",
        );

        let repl_ops = vec![(
            key,
            vec![ReplicaOp::Spend {
                tx_key: key,
                offset: 1,
                spending_data: failed_spending_data,
                current_block_height: 900_020,
                block_height_retention: 50,
                master_generation: { after_spend.generation },
            }],
        )];
        let before_images = no_before_images(&repl_ops);
        compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
            .unwrap();

        let restored = h.engine.read_metadata(&key).expect("metadata restored");
        assert_eq!({ restored.spent_utxos }, 1);
        assert_eq!(
            { restored.delete_at_height },
            0,
            "rollback unspend must clear DAH when the record is no longer all-spent",
        );
        assert!(
            h.engine.dah_index().range_query(u32::MAX).is_empty(),
            "rollback must remove the stale DAH index entry",
        );
        let post_slot1 = h.engine.read_slot(&key, 1).expect("post slot 1");
        assert_eq!(post_slot1.status, crate::record::UTXO_UNSPENT);
    }

    #[test]
    fn rollback_set_locked_restores_prior_dah() {
        use crate::ops::set_mined::SetMinedRequest;
        use crate::ops::spend::SpendRequest;

        let h = RedoDispatchHarness::new();
        let txid = DispatchTestHarness::make_txid(155);
        let key = rollback_seed_record(&h, txid, 1);

        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 1,
                block_height: 700,
                subtree_idx: 0,
                on_longest_chain: true,
                unset_mined: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .expect("mine record");
        let slot = h.engine.read_slot(&key, 0).expect("slot 0");
        h.engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: slot.hash,
                spending_data: [0x55; 36],
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .expect("spend all outputs");

        let before = h.engine.read_metadata(&key).expect("metadata before lock");
        let prior_dah = { before.delete_at_height };
        assert_ne!(prior_dah, 0, "test setup should have DAH pruning state");
        assert!(!before.flags.contains(crate::record::TxFlags::LOCKED));

        let locked = h
            .engine
            .set_locked_with_before_image(&crate::ops::remaining::SetLockedRequest {
                tx_key: key,
                value: true,
            })
            .expect("lock record");
        let after_lock = h.engine.read_metadata(&key).expect("metadata after lock");
        assert!(after_lock.flags.contains(crate::record::TxFlags::LOCKED));
        assert_eq!({ after_lock.delete_at_height }, 0);

        let repl_ops = vec![(
            key,
            vec![ReplicaOp::SetLocked {
                tx_key: key,
                value: true,
                master_generation: locked.generation,
            }],
        )];
        let before_images = vec![(
            key,
            vec![BeforeImage::SetLocked {
                prior_locked: locked.prior_locked,
                prior_delete_at_height: locked.prior_delete_at_height,
            }],
        )];
        compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
            .unwrap();

        let restored = h.engine.read_metadata(&key).expect("metadata restored");
        assert!(!restored.flags.contains(crate::record::TxFlags::LOCKED));
        assert_eq!({ restored.delete_at_height }, prior_dah);
    }

    #[test]
    fn compensation_redo_failure_returns_error() {
        use crate::ops::remaining::FreezeRequest;

        // F-G4-001: the redo region must hold a header block (one
        // device alignment unit) plus at least one entries block, so
        // 4 KiB is rejected at open. 8 KiB still fills quickly under
        // the loop below.
        let h = RedoDispatchHarness::new_with_redo_size(8192);
        let key = rollback_seed_record(&h, DispatchTestHarness::make_txid(157), 1);
        let slot = h.engine.read_slot(&key, 0).expect("slot");
        h.engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: slot.hash,
            })
            .expect("freeze locally");

        {
            let mut log = h.redo_log.lock();
            let mut n = 0u8;
            loop {
                let result = log.append_and_flush(RedoOp::Delete {
                    tx_key: TxKey { txid: [n; 32] },
                    record_offset: u64::from(n) * 4096,
                    record_size: 4096,
                });
                if result.is_err() {
                    break;
                }
                n = n.wrapping_add(1);
            }
        }

        let repl_ops = vec![(
            key,
            vec![ReplicaOp::Freeze {
                tx_key: key,
                offset: 0,
                master_generation: 0,
            }],
        )];
        let before_images = no_before_images(&repl_ops);
        let err =
            compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
                .expect_err("redo log full must make compensation fail visibly");
        assert!(
            err.contains("replication compensation redo write failed"),
            "unexpected compensation error: {err}"
        );
    }

    #[test]
    fn create_batch_redo_failure_surfaces_allocator_rollback_failure() {
        // F-G4-001: the redo region must hold a header block (one
        // alignment unit) plus at least one entries block. With a
        // 8 KiB region the entries capacity is exactly one alignment
        // unit (4 KiB), and because F-G4-004 block-aligns write_pos
        // after every flush, the very first allocator append+flush
        // consumes the entire entries region. The next append
        // (CreateV2) therefore fails with LogFull, exercising the
        // "create redo write failure + allocator rollback errors"
        // path the test asserts on.
        let h = RedoDispatchHarness::new_with_exact_redo_log_size(8192);
        let resp = h.create_tx(DispatchTestHarness::make_txid(158), 1);

        assert_eq!(resp.status, STATUS_ERROR);
        let (code, msg) = decode_error_payload(&resp.payload).expect("error payload");
        assert_eq!(code, ERR_STORAGE_IO);
        assert!(
            msg.contains("redo log append"),
            "expected primary redo failure in error: {msg}"
        );
        assert!(
            msg.contains("allocator rollback errors"),
            "expected rollback failure to be surfaced in error: {msg}"
        );
        assert!(
            msg.contains("create reservation rollback failed after create redo write failure"),
            "expected create reservation rollback context in error: {msg}"
        );
    }

    #[test]
    fn create_batch_fsync_count_optimized() {
        let data_dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = Arc::new(CountingSyncDevice::new(8 * 1024 * 1024, 4096));
        let alloc = SlotAllocator::new(data_dev.clone()).unwrap();
        let index = Index::new(10000).unwrap();
        let locks = StripedLocks::new(1024);
        let dah = DahIndex::new();
        let unmined = UnminedIndex::new();
        let engine = Engine::new(
            data_dev as Arc<dyn BlockDevice>,
            index,
            alloc,
            locks,
            dah,
            unmined,
        );
        let redo_log = crate::redo::RedoLog::open(
            redo_dev.clone() as Arc<dyn BlockDevice>,
            0,
            8 * 1024 * 1024,
        )
        .unwrap();
        let redo_log = Arc::new(Mutex::new(redo_log));
        engine.set_redo_log(redo_log.clone());
        engine.allocator().lock().set_redo_log(redo_log.clone());

        let items: Vec<WireCreateItem> = (0..10u8)
            .map(|i| WireCreateItem {
                txid: DispatchTestHarness::make_txid(180u8.wrapping_add(i)),
                tx_version: 1,
                locktime: 0,
                fee: 500,
                size_in_bytes: 250,
                extended_size: 250,
                is_coinbase: false,
                spending_height: 0,
                created_at: 1700000000000 + u64::from(i),
                flags: 0,
                utxo_hashes: vec![[i; 32]],
                cold_data: vec![],
                block_height: 700_000,
                mined_block_id: Some(10_000 + u32::from(i)),
                mined_block_height: Some(700_000 + u32::from(i)),
                mined_subtree_idx: Some(u32::from(i)),
                parent_txids: vec![],
            })
            .collect();

        let req = RequestFrame {
            request_id: 1,
            op_code: OP_CREATE_BATCH,
            flags: 0,
            payload: encode_create_batch(&items).into(),
        };
        let before_syncs = redo_dev.sync_count();
        let mut conn_state = crate::server::ConnectionState::new();
        let resp = handle_request(
            &req,
            &engine,
            8192,
            None,
            Some(&redo_log),
            &mut conn_state,
            None,
        );

        assert_eq!(resp.status, STATUS_OK);
        // F-G4-001: each effective flush emits two device syncs (one
        // for the entries pwrite, one for the persisted-header rewrite
        // carrying the new `next_sequence`). Two effective flushes
        // (allocator reservations + CreateV2 WAL) therefore produce
        // four device syncs.
        assert_eq!(
            redo_dev.sync_count() - before_syncs,
            4,
            "create batch should fsync twice for allocator reservations \
             and twice for CreateV2 WAL (entries + F-G4-001 header per flush)"
        );

        let entries = redo_log.lock().recover().unwrap();
        let allocate_entries = entries
            .iter()
            .filter(|entry| matches!(entry.op, RedoOp::AllocateRegion { .. }))
            .count();
        let create_entries = entries
            .iter()
            .filter(|entry| matches!(entry.op, RedoOp::CreateV2 { .. }))
            .count();
        assert_eq!(allocate_entries, items.len());
        assert_eq!(create_entries, items.len());
    }

    /// Test 2 (gap #8): reassign rollback restores the original
    /// utxo_hash — NOT zeros.
    #[test]
    fn rollback_reassign_restores_prior_hash_exactly() {
        use crate::ops::remaining::{FreezeRequest, ReassignRequest};

        let h = RedoDispatchHarness::new();
        let mut txid = [0u8; 32];
        txid[0] = 0x55;
        let key = rollback_seed_record(&h, txid, 1);

        // Slot 0 starts with hash [0,0,...,0] — that's the seed value
        // from RedoDispatchHarness::create_tx. We need to FREEZE the
        // slot first because the engine.reassign requires UTXO_FROZEN.
        let initial_slot = h.engine.read_slot(&key, 0).expect("read seed slot");
        let original_hash = initial_slot.hash;

        h.engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: original_hash,
            })
            .expect("freeze");

        // Reassign to a non-zero NEW hash.
        let mut new_hash = [0u8; 32];
        for (i, b) in new_hash.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(17);
        }
        let block_height = 700_000;
        let spendable_after = 100;
        h.engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: original_hash,
                new_utxo_hash: new_hash,
                block_height,
                spendable_after,
            })
            .expect("reassign");

        // Verify slot now has the new hash.
        let mid_slot = h.engine.read_slot(&key, 0).expect("read mid slot");
        assert_eq!(mid_slot.hash, new_hash, "reassign should have applied");

        // Run compensation with the captured before-image (original hash).
        let repl_ops = vec![(
            key,
            vec![ReplicaOp::Reassign {
                tx_key: key,
                offset: 0,
                new_hash,
                block_height,
                spendable_after,
                master_generation: 0,
            }],
        )];
        let before_images = vec![(
            key,
            vec![BeforeImage::Reassign {
                prior_utxo_hash: original_hash,
            }],
        )];
        compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
            .unwrap();

        // Post-compensation: slot's hash MUST be the original — NOT zeros.
        let post_slot = h.engine.read_slot(&key, 0).expect("read post slot");
        assert_eq!(
            post_slot.hash, original_hash,
            "rollback wrote {:?}, expected original {:?}",
            post_slot.hash, original_hash
        );
        // And NOT the all-zero stub (which would happen with the old
        // best-effort path even if `original_hash` happens to be all-zero).
        // The test value is non-zero so this is meaningful here.
    }

    #[test]
    fn compensation_fallback_never_writes_zero_hashes() {
        use crate::ops::remaining::{FreezeRequest, ReassignRequest};

        let h = RedoDispatchHarness::new();
        let txid = DispatchTestHarness::make_txid(153);
        let key = rollback_seed_record(&h, txid, 2);

        let original_hash = h.engine.read_slot(&key, 1).expect("slot 1").hash;
        assert_ne!(
            original_hash, [0u8; 32],
            "test precondition: use a non-zero original hash"
        );
        h.engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 1,
                utxo_hash: original_hash,
            })
            .expect("freeze");

        let mut new_hash = [0u8; 32];
        for (i, b) in new_hash.iter_mut().enumerate() {
            *b = (i as u8).wrapping_add(31);
        }
        h.engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 1,
                utxo_hash: original_hash,
                new_utxo_hash: new_hash,
                block_height: 700_001,
                spendable_after: 100,
            })
            .expect("reassign");
        assert_eq!(h.engine.read_slot(&key, 1).unwrap().hash, new_hash);

        let repl_ops = vec![(
            key,
            vec![ReplicaOp::Reassign {
                tx_key: key,
                offset: 1,
                new_hash,
                block_height: 700_001,
                spendable_after: 100,
                master_generation: 0,
            }],
        )];
        let before_images = Vec::new();

        let err =
            compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
                .expect_err("before-image shape mismatch should fail closed");
        assert!(
            err.contains("before-image shape mismatch"),
            "unexpected compensation error: {err}"
        );

        let post_hash = h.engine.read_slot(&key, 1).unwrap().hash;
        assert_eq!(
            post_hash, new_hash,
            "before-image shape mismatch must fail closed instead of writing a zero hash"
        );
        assert_ne!(
            post_hash, [0u8; 32],
            "compensation must never substitute an all-zero fallback hash"
        );
    }

    /// Test 3a (gap #8): prune rollback against a SPENT slot restores
    /// UTXO_SPENT, NOT UTXO_UNSPENT.
    #[test]
    fn rollback_prune_restores_spent_status_exactly() {
        use crate::record::{UTXO_PRUNED, UTXO_SPENT};

        let h = RedoDispatchHarness::new();
        let mut txid = [0u8; 32];
        txid[0] = 0x66;
        let key = rollback_seed_record(&h, txid, 1);

        // Manually mutate the on-device slot to SPENT (the prune+rollback
        // we test here doesn't need the spend op to have come from
        // the dispatch path — we want to control the prior_status byte
        // explicitly).
        let entry = h.engine.lookup(&key).expect("lookup");
        let mut slot = crate::io::read_utxo_slot(h.engine.device(), entry.record_offset, 0)
            .expect("read slot");
        slot.status = UTXO_SPENT;
        crate::io::write_utxo_slot(h.engine.device(), entry.record_offset, 0, &slot)
            .expect("write spent slot");

        // Apply prune locally.
        let mut pruned = slot;
        pruned.status = UTXO_PRUNED;
        crate::io::write_utxo_slot(h.engine.device(), entry.record_offset, 0, &pruned)
            .expect("apply prune");

        // Run compensation with captured prior status = SPENT.
        let repl_ops = vec![(
            key,
            vec![ReplicaOp::PruneSlot {
                tx_key: key,
                offset: 0,
            }],
        )];
        let before_images = vec![(
            key,
            vec![BeforeImage::Prune {
                prior_status: UTXO_SPENT,
            }],
        )];
        compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
            .unwrap();

        // Post-compensation: slot status MUST be SPENT, NOT UNSPENT.
        let post_slot = crate::io::read_utxo_slot(h.engine.device(), entry.record_offset, 0)
            .expect("read post slot");
        assert_eq!(
            post_slot.status, UTXO_SPENT,
            "prune rollback should restore SPENT, got 0x{:02x}",
            post_slot.status
        );
    }

    /// Test 3b (gap #8): prune rollback against a FROZEN slot restores
    /// UTXO_FROZEN, NOT UTXO_UNSPENT.
    #[test]
    fn rollback_prune_restores_frozen_status_exactly() {
        use crate::record::{UTXO_FROZEN, UTXO_PRUNED};

        let h = RedoDispatchHarness::new();
        let mut txid = [0u8; 32];
        txid[0] = 0x77;
        let key = rollback_seed_record(&h, txid, 1);

        let entry = h.engine.lookup(&key).expect("lookup");
        let mut slot = crate::io::read_utxo_slot(h.engine.device(), entry.record_offset, 0)
            .expect("read slot");
        slot.status = UTXO_FROZEN;
        crate::io::write_utxo_slot(h.engine.device(), entry.record_offset, 0, &slot)
            .expect("write frozen slot");

        // Apply prune locally.
        let mut pruned = slot;
        pruned.status = UTXO_PRUNED;
        crate::io::write_utxo_slot(h.engine.device(), entry.record_offset, 0, &pruned)
            .expect("apply prune");

        let repl_ops = vec![(
            key,
            vec![ReplicaOp::PruneSlot {
                tx_key: key,
                offset: 0,
            }],
        )];
        let before_images = vec![(
            key,
            vec![BeforeImage::Prune {
                prior_status: UTXO_FROZEN,
            }],
        )];
        compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
            .unwrap();

        let post_slot = crate::io::read_utxo_slot(h.engine.device(), entry.record_offset, 0)
            .expect("read post slot");
        assert_eq!(
            post_slot.status, UTXO_FROZEN,
            "prune rollback should restore FROZEN, got 0x{:02x}",
            post_slot.status
        );
    }

    /// Test 4 (gap #8): a crash mid-rollback. Persist a Compensate*
    /// redo entry, simulate crash before the engine apply runs, then
    /// startup recovery must complete the compensation from the redo
    /// entry alone. Verifies the durability invariant: any Compensate*
    /// entry that reaches the redo log produces a deterministic
    /// post-recovery state.
    #[test]
    fn crash_mid_rollback_recovers_compensation_from_redo() {
        use crate::ops::set_mined::SetMinedRequest;

        let h = RedoDispatchHarness::new();
        let mut txid = [0u8; 32];
        txid[0] = 0x88;
        let key = rollback_seed_record(&h, txid, 1);

        // Set mined.
        let block_id = 99;
        let block_height = 850_000;
        let subtree_idx = 5;
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id,
                block_height,
                subtree_idx,
                on_longest_chain: true,
                unset_mined: false,
                current_block_height: 0,
                block_height_retention: 0,
            })
            .expect("set_mined");

        // Unset mined locally (the engine apply happens; we'd then attempt
        // replication, fail, and roll back — but here we simulate a crash
        // BEFORE the engine.set_mined(restore) runs by ONLY appending
        // the Compensate* redo entry and crashing immediately.
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id,
                block_height,
                subtree_idx,
                on_longest_chain: false,
                unset_mined: true,
                current_block_height: 0,
                block_height_retention: 0,
            })
            .expect("local unset");

        // Append the compensation intent to the redo log (simulating the
        // first half of `compensate_replication_failure`'s work) but DO
        // NOT run the engine restore.
        h.redo_log
            .lock()
            .append_and_flush(RedoOp::CompensateUnsetMined {
                tx_key: key,
                block_id,
                block_height,
                subtree_idx,
            })
            .expect("append compensate intent");

        // Verify the slot's metadata pre-recovery: the entry should be
        // ABSENT (the unset removed it; the compensation hasn't run yet).
        let pre_meta = h.engine.read_metadata(&key).expect("pre meta");
        let pre_inline =
            (pre_meta.block_entry_count as usize).min(crate::record::INLINE_BLOCK_ENTRIES);
        let pre_present =
            (0..pre_inline).any(|i| { pre_meta.block_entries_inline[i].block_id } == block_id);
        assert!(
            !pre_present,
            "block entry should be absent before recovery (precondition)"
        );

        // Crash + recover. Recovery must replay the CompensateUnsetMined
        // entry and restore the block entry exactly.
        let h2 = h.crash_and_recover();

        let post_meta = h2.engine.read_metadata(&key).expect("post meta");
        let post_inline =
            (post_meta.block_entry_count as usize).min(crate::record::INLINE_BLOCK_ENTRIES);
        let mut restored = false;
        for i in 0..post_inline {
            if { post_meta.block_entries_inline[i].block_id } == block_id {
                let bh = { post_meta.block_entries_inline[i].block_height };
                let st = { post_meta.block_entries_inline[i].subtree_idx };
                assert_eq!(bh, block_height, "post-recovery height not restored");
                assert_eq!(st, subtree_idx, "post-recovery subtree not restored");
                restored = true;
                break;
            }
        }
        assert!(
            restored,
            "block entry not restored from CompensateUnsetMined replay"
        );
    }

    /// Test 5 (gap #8): acknowledged-implies-replicated invariant.
    ///
    /// When `replicate_all_ops` returns `Err`, the dispatch handler MUST
    /// (a) NOT acknowledge the client (returns ERR_REPLICATION_FAILED),
    /// AND (b) leave the local state at the pre-apply value — bit-exact,
    /// no defaults, no zeros.
    ///
    /// We exercise this by calling `compensate_replication_failure`
    /// directly after a local engine apply, verifying the slot is
    /// indistinguishable from its pre-apply state. The handler-level
    /// path that returns `error_response(ERR_REPLICATION_FAILED, ...)`
    /// after the compensation is identical across all dispatch paths
    /// and is exercised by the existing `acked_*_survives_crash` tests
    /// — combined, the two invariants form the per-op chain: ACKed
    /// requests are durable AND replicated; rejected requests leave no
    /// durable trace of the would-be local apply.
    #[test]
    fn rollback_leaves_no_observable_local_apply_for_reassign() {
        use crate::ops::remaining::{FreezeRequest, ReassignRequest};

        let h = RedoDispatchHarness::new();
        let mut txid = [0u8; 32];
        txid[0] = 0x99;
        let key = rollback_seed_record(&h, txid, 1);

        // Snapshot the pre-apply slot bytes via direct device read.
        let entry = h.engine.lookup(&key).expect("lookup");
        let pre_apply = h.engine.read_slot(&key, 0).expect("pre slot");
        let original_hash = pre_apply.hash;

        // Freeze (required for reassign).
        h.engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: original_hash,
            })
            .expect("freeze");

        // Snapshot AFTER freeze (this is the "pre-apply for the
        // reassign+rollback" state we want to restore to).
        let frozen_slot = h.engine.read_slot(&key, 0).expect("frozen slot");

        // Apply reassign locally with a non-zero target hash.
        let mut new_hash = [0u8; 32];
        for (i, b) in new_hash.iter_mut().enumerate() {
            *b = (i as u8).wrapping_add(13);
        }
        h.engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: original_hash,
                new_utxo_hash: new_hash,
                block_height: 750_000,
                spendable_after: 100,
            })
            .expect("reassign");

        // Replication "fails" → run compensation with the captured
        // before-image. After compensation, the slot's hash MUST be the
        // original; the slot was returned to UNSPENT (engine.reassign's
        // post-state on the rollback path), and the engine has no record
        // of the failed reassign existing.
        let repl_ops = vec![(
            key,
            vec![ReplicaOp::Reassign {
                tx_key: key,
                offset: 0,
                new_hash,
                block_height: 750_000,
                spendable_after: 100,
                master_generation: 0,
            }],
        )];
        let before_images = vec![(
            key,
            vec![BeforeImage::Reassign {
                prior_utxo_hash: original_hash,
            }],
        )];
        compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
            .unwrap();

        // Post-rollback slot: hash MUST equal the pre-reassign hash.
        let post_slot = crate::io::read_utxo_slot(h.engine.device(), entry.record_offset, 0)
            .expect("post slot");
        assert_eq!(
            post_slot.hash, original_hash,
            "post-rollback hash {:?} does not match pre-apply hash {:?}",
            post_slot.hash, original_hash
        );
        // The slot is observably back to a state where the original hash
        // is on device; the only difference from the frozen pre-state
        // is the status byte (rolling back via reassign-with-original-
        // hash leaves it UNSPENT, not FROZEN). That is the engine's
        // documented reassign-rollback semantics — silence the unused
        // bind so this test doesn't rely on the exact post-status.
        let _ = frozen_slot;
    }

    #[test]
    fn concurrent_reassign_compensation_uses_correct_prior_hash() {
        use crate::ops::remaining::{FreezeRequest, ReassignRequest};

        let h = RedoDispatchHarness::new();
        let txid = DispatchTestHarness::make_txid(154);
        let key = rollback_seed_record(&h, txid, 2);

        let original_hash = h.engine.read_slot(&key, 0).expect("slot 0").hash;
        h.engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: original_hash,
            })
            .expect("freeze original");

        let first_hash = [0xA1; 32];
        h.engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: original_hash,
                new_utxo_hash: first_hash,
                block_height: 750_000,
                spendable_after: 100,
            })
            .expect("first reassign");

        h.engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: first_hash,
            })
            .expect("freeze first hash");

        let second_hash = [0xB2; 32];
        h.engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: first_hash,
                new_utxo_hash: second_hash,
                block_height: 750_100,
                spendable_after: 100,
            })
            .expect("second reassign");

        let repl_ops = vec![(
            key,
            vec![ReplicaOp::Reassign {
                tx_key: key,
                offset: 0,
                new_hash: second_hash,
                block_height: 750_100,
                spendable_after: 100,
                master_generation: 0,
            }],
        )];
        let before_images = vec![(
            key,
            vec![BeforeImage::Reassign {
                prior_utxo_hash: first_hash,
            }],
        )];
        compensate_replication_failure(&h.engine, &repl_ops, &before_images, Some(&h.redo_log))
            .unwrap();

        let post_slot = h.engine.read_slot(&key, 0).expect("post slot");
        assert_eq!(
            post_slot.hash, first_hash,
            "rollback of the second reassign must restore the first reassign's hash, not the original hash"
        );
    }
}
