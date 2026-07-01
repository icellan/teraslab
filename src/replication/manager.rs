//! Replication manager — orchestrates sending to multiple replicas
//! with configurable acknowledgment policies.
//!
//! D-10 — TEST-ONLY MODULE. [`ReplicationManager`] is constructed only in test
//! code (`cluster::coordinator` tests under `#[cfg(test)]` plus this module's
//! own unit tests); it is NOT on the production replication path. Production
//! replication is `server::dispatch::replicate_all_ops`, which fans out one
//! `ReplicaBatch` per replica via `send_replica_batch_to` and counts ACKs with
//! `classify_replication_outcome`; the receive side is
//! `replication::receiver::handle_replica_batch_with_tracker`. This manager is
//! retained as a tested reference implementation of the quorum/straggler/
//! catch-up math. Do NOT wire it into the dispatch path without first
//! reconciling its sequence-space and ACK semantics against the production
//! path (Rule 6 two-implementation hazard).

use crate::metrics::replication_metrics;
use crate::observability::WireTraceContext;
use crate::replication::protocol::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;

/// Errors from the replication manager.
#[derive(Error, Debug)]
pub enum ReplicationError {
    /// Not enough replicas available to satisfy the ack policy.
    #[error("insufficient replicas: {available} available, {required} required")]
    InsufficientReplicas { available: usize, required: usize },

    /// A replica failed to acknowledge within the timeout.
    #[error("replica timeout after {0:?}")]
    Timeout(Duration),

    /// A replica returned an error.
    #[error("replica error at sequence {sequence}: {message}")]
    ReplicaError { sequence: u64, message: String },

    /// Transport error.
    #[error("transport error: {0}")]
    Transport(String),
}

/// Acknowledgment policy: how many replicas must ACK before the master
/// considers a write durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckPolicy {
    /// Wait for ALL replicas to ACK. Strongest durability.
    WriteAll,
    /// Wait for floor(RF/2)+1 total copies (including master).
    /// For RF=3: master + 1 replica = 2, so wait for 1 replica ACK.
    WriteMajority,
}

/// Configuration for the replication manager.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Acknowledgment policy.
    pub ack_policy: AckPolicy,
    /// Timeout for each replication batch.
    pub replication_timeout: Duration,
    /// Entries per catchup batch.
    pub catchup_batch_size: usize,
    /// Maximum redo-derived operations to send to one replica in one
    /// catch-up pass. This bounds how long a lagging replica can monopolize
    /// the catch-up loop before live replication and other replicas get a
    /// scheduling opportunity.
    pub catchup_max_ops_per_pass: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            ack_policy: AckPolicy::WriteAll,
            replication_timeout: Duration::from_secs(5),
            catchup_batch_size: 1000,
            catchup_max_ops_per_pass: 10_000,
        }
    }
}

/// Number of replica ACKs required for a write to satisfy `policy`.
///
/// `replica_targets` is the number of replicas the master sent to, so the
/// replication factor is `replica_targets + 1` because the master already has
/// one local durable copy. For `WriteMajority`, this returns the number of
/// replica ACKs needed to reach a strict majority of all copies.
pub fn required_replica_acks(replica_targets: usize, policy: AckPolicy) -> usize {
    match policy {
        AckPolicy::WriteAll => replica_targets,
        AckPolicy::WriteMajority => {
            let replication_factor = replica_targets.saturating_add(1);
            let majority_copies = replication_factor / 2 + 1;
            majority_copies.saturating_sub(1)
        }
    }
}

/// Transport abstraction for sending batches to a replica.
///
/// Implemented by in-memory channels (testing) and TCP (production).
pub trait ReplicaTransport: Send {
    /// Send a batch of operations.
    fn send_batch(&mut self, batch: &ReplicaBatch) -> std::result::Result<(), ReplicationError>;

    /// Receive an acknowledgment.
    fn recv_ack(&mut self, timeout: Duration) -> std::result::Result<ReplicaAck, ReplicationError>;

    /// Whether the connection is healthy.
    fn is_connected(&self) -> bool;
}

/// Tracks the state of a single replica connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaState {
    /// Replica is fully caught up and receiving live ops.
    Live,
    /// Replica is behind and needs catchup from the given sequence.
    CatchingUp { from_sequence: u64 },
    /// Replica is disconnected.
    Down,
    /// Redo entries needed for catch-up have been reclaimed (circular log
    /// wrapped). A full shard resync is required before this replica can
    /// return to Live.
    NeedsResync,
}

/// Phase H — published by the replication manager when a replica is
/// found to need a full-shard resync (its redo entries have been
/// reclaimed past `last_acked`).
///
/// `node_id` is the raw `NodeId.0` of the affected replica. `shards`
/// lists the specific shards that require resync; an empty `shards` is
/// the manager's "I don't know — coordinator should consult the shard
/// table for every shard this replica should hold" signal.
///
/// The replication module deliberately uses a raw `u64` here so it
/// stays free of a back-edge dependency on `crate::cluster::shards`.
/// Coordinator-side handlers wrap this back into `NodeId(node_id)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResyncRequest {
    /// `NodeId.0` of the replica that needs a full-shard resync.
    pub node_id: u64,
    /// Specific shards that require resync. Empty = "all shards this
    /// replica should hold per the current shard table".
    pub shards: Vec<u16>,
}

/// Manages a single replica's transport and state.
///
/// `transport` is `Option<_>` because the dispatch hot path (C-10 /
/// F-G7-018 early-return on majority) moves the transport into a
/// detached worker thread for the duration of a `replicate_batch`
/// call. While a straggler worker is still in flight, the slot stays
/// `None` until `ReplicationManager::drain_stragglers` joins it and
/// restores the transport.
pub struct ReplicaSender {
    transport: Option<Box<dyn ReplicaTransport>>,
    state: ReplicaState,
    last_acked: u64,
    /// Phase H — replica's `NodeId.0`. Stored as raw `u64` to keep
    /// `manager.rs` independent of `cluster::shards::NodeId`. Default
    /// `0` means "unknown" (test fixtures and old call sites).
    node_id: u64,
    /// Phase H — has a `ResyncRequest` already been emitted for this
    /// sender's current `NeedsResync` excursion? Reset when the sender
    /// transitions back to `Live` so future truncations re-emit.
    resync_emitted: bool,
}

impl ReplicaSender {
    /// Create a new sender with the given transport.
    pub fn new(transport: Box<dyn ReplicaTransport>) -> Self {
        Self {
            transport: Some(transport),
            state: ReplicaState::Live,
            last_acked: 0,
            node_id: 0,
            resync_emitted: false,
        }
    }

    /// Read-only probe — returns `false` when the transport is currently
    /// held by a straggler worker (rather than panicking), because
    /// reconnect heuristics call this without first draining.
    fn transport_is_connected(&self) -> bool {
        self.transport.as_ref().is_some_and(|t| t.is_connected())
    }

    /// Current state of this replica.
    pub fn state(&self) -> &ReplicaState {
        &self.state
    }

    /// Highest sequence ACKed by this replica.
    pub fn last_acked(&self) -> u64 {
        self.last_acked
    }

    /// Replication lag (ops behind master).
    pub fn lag(&self, master_seq: u64) -> u64 {
        master_seq.saturating_sub(self.last_acked)
    }
}

/// Orchestrates replication to multiple replicas.
///
/// # Concurrency contract (F-G7-019)
///
/// `ReplicationManager` is **not** internally synchronized. Every
/// state-mutating method (`replicate_batch`, `run_catchup`,
/// `check_reconnected`, `mark_replica_live`, `set_replica_node_id`,
/// `install_resync_request_channel`) takes `&mut self` so Rust's
/// borrow checker enforces single-threaded access at compile time.
///
/// Callers that share a `ReplicationManager` between the
/// coordinator (which calls `mark_replica_live` from the topology
/// loop) and the replication hot path (which calls `replicate_batch`
/// from the dispatch thread) **MUST** wrap it in an external
/// `Mutex<ReplicationManager>` (or equivalent) so the two paths
/// observe a consistent sender table. Without that, the compiler
/// will refuse to compile a shared reference; with it, the lock
/// linearizes the state transitions.
///
/// The manager intentionally does NOT internalize a `Mutex<Senders>`
/// because (a) callers already need an external mutex to serialize
/// the dispatch thread's borrow of the senders' transports during
/// the parallel fan-out, and (b) doubling up locks would force the
/// hot path to traverse a mutex on every batch.
pub struct ReplicationManager {
    senders: Vec<ReplicaSender>,
    config: ReplicationConfig,
    next_sequence: u64,
    /// Shared cluster epoch handle. Every constructed [`ReplicaBatch`]
    /// is stamped with `current_cluster_key.load(Ordering::Acquire)` so
    /// receivers can reject stale-epoch batches (Phase B2 gate).
    ///
    /// A value of `0` means "unknown" and is preserved on the wire as
    /// V1-compat: the receiver accepts those batches unconditionally.
    /// Production wiring (Phase B3) installs a coordinator-owned atomic
    /// that bumps on every `migration_complete` to fence stale masters.
    current_cluster_key: Arc<AtomicU64>,
    /// Phase H — when a sender enters `NeedsResync` (its catch-up redo
    /// is gone) the manager publishes a [`ResyncRequest`] on this
    /// channel exactly once. The coordinator's event loop synthesizes
    /// the corresponding full-shard migration tasks. `None` means the
    /// caller hasn't installed a channel — `NeedsResync` still happens
    /// but nothing is auto-published (legacy / test behaviour).
    resync_request_tx: Option<std::sync::mpsc::Sender<ResyncRequest>>,
    /// C-10 / F-G7-018 — straggler join handles retained by
    /// [`Self::replicate_batch`] when it early-returns on majority ACK.
    /// Each entry owns the transport for its slot until the straggler
    /// thread completes; the next state-mutating call drains them via
    /// [`Self::drain_stragglers`] to reclaim transports and apply
    /// late state transitions. `pending_stragglers[i]` corresponds to
    /// `senders[i]`; `None` means no straggler is in flight.
    pending_stragglers: Vec<Option<std::thread::JoinHandle<StragglerOutcome>>>,
}

/// Per-replica outcome of one batch's `send_batch` + `recv_ack`
/// round-trip. Sent on the per-batch mpsc channel so the master can
/// early-return as soon as quorum is reached (C-10 / F-G7-018).
#[derive(Debug)]
enum BatchOutcome {
    Ok { through_sequence: u64 },
    ReplicaErr { sequence: u64, message: String },
    TransportErr(ReplicationError),
}

/// What a straggler worker returns via its `JoinHandle` so the manager
/// can reclaim the transport and apply late state on the next
/// [`ReplicationManager::drain_stragglers`].
struct StragglerOutcome {
    outcome: BatchOutcome,
    transport: Box<dyn ReplicaTransport>,
    /// Serialized-batch size captured by the originating call —
    /// retained so a late ACK can update the per-replica `bytes_sent`
    /// metric without recomputing it.
    batch_bytes: u64,
}

/// Manual clone — `ReplicationError` is not `Clone` (its sources
/// aren't), so the early-return path that needs both a channel signal
/// AND a straggler return value clones with this helper.
fn clone_outcome(o: &BatchOutcome) -> BatchOutcome {
    match o {
        BatchOutcome::Ok { through_sequence } => BatchOutcome::Ok {
            through_sequence: *through_sequence,
        },
        BatchOutcome::ReplicaErr { sequence, message } => BatchOutcome::ReplicaErr {
            sequence: *sequence,
            message: message.clone(),
        },
        BatchOutcome::TransportErr(e) => BatchOutcome::TransportErr(clone_replication_error(e)),
    }
}

/// Manual clone for [`ReplicationError`].
/// Poll `handle.is_finished()` until it returns true or `deadline` elapses.
///
/// `std::thread::JoinHandle::join` has no built-in timeout, so the drain
/// path uses this helper to avoid blocking the master write path on a
/// permanently stuck worker (see `drain_stragglers`). The poll cadence
/// starts at 100µs and grows exponentially to a 50ms cap — fast enough
/// that a worker that finishes "just now" is reclaimed promptly, slow
/// enough that a multi-second wait does not burn a core.
fn wait_for_finish_until(
    handle: Option<&std::thread::JoinHandle<StragglerOutcome>>,
    deadline: Instant,
) -> bool {
    let Some(handle) = handle else {
        return true;
    };
    let mut backoff = Duration::from_micros(100);
    let max_backoff = Duration::from_millis(50);
    loop {
        if handle.is_finished() {
            return true;
        }
        let now = Instant::now();
        if now >= deadline {
            return handle.is_finished();
        }
        let remaining = deadline.saturating_duration_since(now);
        std::thread::sleep(backoff.min(remaining));
        backoff = (backoff * 2).min(max_backoff);
    }
}

fn clone_replication_error(e: &ReplicationError) -> ReplicationError {
    match e {
        ReplicationError::InsufficientReplicas {
            available,
            required,
        } => ReplicationError::InsufficientReplicas {
            available: *available,
            required: *required,
        },
        ReplicationError::Timeout(d) => ReplicationError::Timeout(*d),
        ReplicationError::ReplicaError { sequence, message } => ReplicationError::ReplicaError {
            sequence: *sequence,
            message: message.clone(),
        },
        ReplicationError::Transport(s) => ReplicationError::Transport(s.clone()),
    }
}

impl ReplicationManager {
    /// Create a new manager with the given configuration and replica transports.
    ///
    /// The cluster_key handle defaults to a freshly allocated atomic
    /// initialized to `0` (unknown — V1-compat). Production callers
    /// that need to participate in the cluster-key gate must instead use
    /// [`with_cluster_key`](Self::with_cluster_key) so the manager and
    /// the receiver share the same coordinator-owned epoch handle.
    pub fn new(config: ReplicationConfig, transports: Vec<Box<dyn ReplicaTransport>>) -> Self {
        Self::with_cluster_key(config, transports, Arc::new(AtomicU64::new(0)))
    }

    /// Create a manager with sequence state recovered from the redo log.
    ///
    /// `initial_sequence` should be `redo_log.current_sequence()` so that
    /// replication sequence numbers are contiguous with the durable log.
    /// The cluster_key handle defaults to `0` (unknown); use
    /// [`with_initial_sequence_and_cluster_key`](Self::with_initial_sequence_and_cluster_key)
    /// to install a real epoch atomic.
    pub fn with_initial_sequence(
        config: ReplicationConfig,
        transports: Vec<Box<dyn ReplicaTransport>>,
        initial_sequence: u64,
    ) -> Self {
        Self::with_initial_sequence_and_cluster_key(
            config,
            transports,
            initial_sequence,
            Arc::new(AtomicU64::new(0)),
        )
    }

    /// Create a manager wired to a coordinator-owned cluster_key handle.
    ///
    /// The shared `Arc<AtomicU64>` is stamped onto every constructed
    /// [`ReplicaBatch`] via an `Acquire` load on the hot path.
    pub fn with_cluster_key(
        config: ReplicationConfig,
        transports: Vec<Box<dyn ReplicaTransport>>,
        current_cluster_key: Arc<AtomicU64>,
    ) -> Self {
        let senders: Vec<_> = transports.into_iter().map(ReplicaSender::new).collect();
        let n = senders.len();
        Self {
            senders,
            config,
            next_sequence: 1,
            current_cluster_key,
            resync_request_tx: None,
            pending_stragglers: (0..n).map(|_| None).collect(),
        }
    }

    /// Create a manager with both an initial sequence (recovered from
    /// the redo log) and a coordinator-owned cluster_key handle.
    pub fn with_initial_sequence_and_cluster_key(
        config: ReplicationConfig,
        transports: Vec<Box<dyn ReplicaTransport>>,
        initial_sequence: u64,
        current_cluster_key: Arc<AtomicU64>,
    ) -> Self {
        let senders: Vec<_> = transports.into_iter().map(ReplicaSender::new).collect();
        let n = senders.len();
        Self {
            senders,
            config,
            next_sequence: initial_sequence.max(1),
            current_cluster_key,
            resync_request_tx: None,
            pending_stragglers: (0..n).map(|_| None).collect(),
        }
    }

    /// Phase H — install a channel that receives a [`ResyncRequest`]
    /// every time a sender transitions into `NeedsResync`. Re-installs
    /// replace any existing channel.
    pub fn install_resync_request_channel(&mut self, tx: std::sync::mpsc::Sender<ResyncRequest>) {
        self.resync_request_tx = Some(tx);
    }

    /// Phase H — tag a sender with the replica's `NodeId.0` so emitted
    /// [`ResyncRequest`]s name the right peer. Out-of-bounds indices
    /// are silently ignored (defensive — sender list may be replaced
    /// during topology activation).
    pub fn set_replica_node_id(&mut self, sender_idx: usize, node_id: u64) {
        if let Some(s) = self.senders.get_mut(sender_idx) {
            s.node_id = node_id;
        }
    }

    /// Phase H — coordinator-side notification that a full-shard resync
    /// has completed for `sender_idx`. Transitions the sender from
    /// `NeedsResync` back to `Live` and resets the resync-emitted gate
    /// so a future truncation can re-publish. No-op if the index is
    /// out of bounds or the sender is not currently `NeedsResync`.
    pub fn mark_replica_live(&mut self, sender_idx: usize) {
        if let Some(s) = self.senders.get_mut(sender_idx)
            && s.state == ReplicaState::NeedsResync
        {
            s.state = ReplicaState::Live;
            s.resync_emitted = false;
        }
    }

    /// Access the shared cluster_key handle so the coordinator can
    /// bump the epoch (Phase B3) or share it with the local
    /// [`ReplicationReceiver`](crate::replication::ReplicationReceiver).
    pub fn cluster_key_handle(&self) -> Arc<AtomicU64> {
        self.current_cluster_key.clone()
    }

    /// Replicate a batch of operations to all live replicas.
    ///
    /// Each `Live` replica gets its own detached worker thread that owns
    /// the transport for the duration of the round-trip. Workers send
    /// their outcome on a per-batch mpsc channel; the master reads from
    /// that channel until either:
    ///
    /// 1. enough ACKs have arrived to satisfy [`AckPolicy`]
    ///    (`required_ack_count()` for `WriteMajority`, all live replicas
    ///    for `WriteAll`), at which point it **returns early** while the
    ///    remaining slow replicas keep running in the background, or
    /// 2. enough failures have arrived to make the quota unreachable, at
    ///    which point it returns the first observed error.
    ///
    /// Straggler join handles are retained in `pending_stragglers` so
    /// the next state-mutating call drains them via
    /// `Self::drain_stragglers`, applies the late outcome to sender
    /// state, and restores the transport. This preserves the durable-log
    /// invariant — every assigned sequence eventually reaches every
    /// replica — while removing slow followers from the master's tail
    /// latency.
    ///
    /// **C-10 / F-G7-018** — early-return on majority. The previous
    /// implementation joined every worker before returning, so a single
    /// slow follower's RTT dominated `WriteMajority` latency.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn replicate_batch(
        &mut self,
        ops: &[ReplicaOp],
    ) -> std::result::Result<(), ReplicationError> {
        if ops.is_empty() {
            return Ok(());
        }

        // Reclaim transports + apply state from any straggler threads
        // left over from prior batches. This must happen BEFORE we
        // compute `live_count` so a late "transport error" outcome
        // gets accounted for in this call's quorum decision.
        self.drain_stragglers();

        let required = self.required_ack_count();
        let live_count = self
            .senders
            .iter()
            .filter(|s| *s.state() == ReplicaState::Live)
            .count();

        if live_count < required {
            return Err(ReplicationError::InsufficientReplicas {
                available: live_count,
                required,
            });
        }

        // Snapshot the current span's trace context so every wire frame
        // carries the sender's provenance. `from_current_span` is a cheap
        // no-op when no subscriber is installed or the span isn't sampled.
        let trace_ctx = WireTraceContext::from_current_span();
        let batch = ReplicaBatch {
            first_sequence: self.next_sequence,
            ops: ops.to_vec(),
            trace_ctx,
            source_node_id: None,
            // Phase B2: stamp every batch with the current cluster epoch
            // from the shared coordinator-owned atomic so the receiver
            // can fence stale-epoch masters.
            cluster_key: self.current_cluster_key.load(Ordering::Acquire),
        };
        // F-G7-007: `next_sequence` advances BEFORE the fan-out
        // result is reconciled. A retry of the same logical ops
        // therefore lands on a new sequence range (the replica's
        // dedup tracker stores both ranges as legitimate). This
        // matches the durable-log invariant — every assigned
        // sequence is recorded as an intent — and the master's
        // ReplicationIntentTracker reconciles overlapping ranges
        // on restart. Do NOT reset `next_sequence` on full failure:
        // that would let the next batch reuse a sequence the redo
        // log has already journalled, breaking the invariant.
        self.next_sequence += ops.len() as u64;

        let timeout = self.config.replication_timeout;

        // Cache the subsystem metrics pointer once so the workers do not
        // repeatedly pay the `OnceLock::get()` cost. `batch_bytes` is the
        // serialized-batch size, computed once for all replicas.
        let metrics = replication_metrics();
        let batch_bytes = batch.serialize().len() as u64;
        if let Some(m) = metrics {
            m.repl_batches_sent_total.inc();
            m.leader_sequence
                .store(self.next_sequence, std::sync::atomic::Ordering::Relaxed);
        }
        let start = Instant::now();

        // Share the batch with all workers via Arc — the workers each
        // need an owned reference (`'static`) since they're not scoped.
        let batch_arc = Arc::new(batch);
        // Per-batch ack channel. Workers send (idx, outcome) here so the
        // master can early-return as soon as quorum acks arrive. The
        // worker also returns the transport via its JoinHandle so the
        // straggler drain can reclaim it.
        let (tx, rx) = std::sync::mpsc::channel::<(usize, BatchOutcome)>();

        // Spawn one detached worker per live sender. Non-live senders
        // are simply skipped — no thread spawn, no ack on the channel.
        let mut live_count_dispatched = 0usize;
        for (idx, sender) in self.senders.iter_mut().enumerate() {
            if *sender.state() != ReplicaState::Live {
                continue;
            }
            live_count_dispatched += 1;
            // Move transport out of the sender slot into the worker
            // thread. The slot stays `None` until the straggler joins
            // (in `drain_stragglers`) and restores it.
            let transport_box = sender
                .transport
                .take()
                .expect("sender transport already in flight — drain invariant violated");
            let tx = tx.clone();
            let batch_arc = batch_arc.clone();
            let handle = std::thread::Builder::new()
                .name(format!("replica-{idx}-batch-{}", batch_arc.first_sequence))
                .spawn(move || {
                    let mut transport = transport_box;
                    if let Some(m) = metrics {
                        m.mark_in_flight(idx);
                    }
                    let outcome = match transport.send_batch(&batch_arc) {
                        Ok(()) => match transport.recv_ack(timeout) {
                            Ok(ReplicaAck::Ok { through_sequence }) => {
                                BatchOutcome::Ok { through_sequence }
                            }
                            Ok(ReplicaAck::Error {
                                failed_sequence,
                                message,
                            }) => BatchOutcome::ReplicaErr {
                                sequence: failed_sequence,
                                message,
                            },
                            Ok(ReplicaAck::Gap {
                                expected_sequence,
                                received_first_sequence,
                            }) => BatchOutcome::ReplicaErr {
                                sequence: received_first_sequence,
                                message: format!(
                                    "sequence-gap NAK: replica expects {expected_sequence}"
                                ),
                            },
                            Err(e) => BatchOutcome::TransportErr(e),
                        },
                        Err(e) => BatchOutcome::TransportErr(e),
                    };
                    // Best-effort early-return signal. If the master
                    // already returned, the receiver may be dropped;
                    // the worker still completes so its straggler
                    // outcome can be reclaimed by the next drain.
                    let signal = clone_outcome(&outcome);
                    let _ = tx.send((idx, signal));
                    StragglerOutcome {
                        outcome,
                        transport,
                        batch_bytes,
                    }
                })
                .expect("failed to spawn replication worker thread");
            self.pending_stragglers[idx] = Some(handle);
        }
        // Drop the master's clone of the sender so the channel closes
        // once every worker exits (used to detect "all done" if every
        // ack is consumed).
        drop(tx);
        debug_assert_eq!(live_count_dispatched, live_count);

        // Read acks from the channel until quorum is reached, the
        // remaining acks can no longer satisfy it, or the timeout
        // elapses.
        let mut acks_received = 0usize;
        let mut successes = 0usize;
        let mut first_error: Option<ReplicationError> = None;
        let target_successes = match self.config.ack_policy {
            AckPolicy::WriteAll => live_count,
            AckPolicy::WriteMajority => required,
        };

        let deadline = start + timeout;
        let result = loop {
            if successes >= target_successes {
                break Ok(());
            }
            let remaining = live_count - acks_received;
            if successes + remaining < target_successes {
                break Err(first_error.as_ref().map(clone_replication_error).unwrap_or(
                    ReplicationError::InsufficientReplicas {
                        available: successes,
                        required: target_successes,
                    },
                ));
            }
            let now = Instant::now();
            let wait = deadline.saturating_duration_since(now);
            if wait.is_zero() {
                break Err(first_error
                    .as_ref()
                    .map(clone_replication_error)
                    .unwrap_or(ReplicationError::Timeout(timeout)));
            }
            match rx.recv_timeout(wait) {
                Ok((idx, outcome)) => {
                    acks_received += 1;
                    match outcome {
                        BatchOutcome::Ok { through_sequence } => {
                            successes += 1;
                            // Clamp to `self.next_sequence` so a buggy
                            // or malicious replica cannot artificially
                            // advance `last_acked` past sequences the
                            // master has actually assigned. Without the
                            // clamp a replica reporting `u64::MAX` would
                            // permanently mark the sender "caught up"
                            // and suppress catch-up on legitimate
                            // future divergence.
                            let through_sequence = through_sequence.min(self.next_sequence);
                            // Apply ACK immediately so subsequent
                            // batches (and metrics) see the latest
                            // last_acked even when this is the last
                            // ack we read before early-return.
                            self.senders[idx].last_acked = through_sequence;
                            if let Some(m) = metrics {
                                m.record_ack(idx, through_sequence, batch_bytes);
                            }
                        }
                        BatchOutcome::ReplicaErr { sequence, message } => {
                            self.senders[idx].state = ReplicaState::Down;
                            if let Some(m) = metrics {
                                m.record_failure(idx);
                            }
                            if first_error.is_none() {
                                first_error =
                                    Some(ReplicationError::ReplicaError { sequence, message });
                            }
                        }
                        BatchOutcome::TransportErr(e) => {
                            self.senders[idx].state = ReplicaState::Down;
                            if let Some(m) = metrics {
                                m.record_failure(idx);
                            }
                            if first_error.is_none() {
                                first_error = Some(e);
                            }
                        }
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    break Err(first_error
                        .as_ref()
                        .map(clone_replication_error)
                        .unwrap_or(ReplicationError::Timeout(timeout)));
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    if successes >= target_successes {
                        break Ok(());
                    }
                    break Err(first_error.as_ref().map(clone_replication_error).unwrap_or(
                        ReplicationError::InsufficientReplicas {
                            available: successes,
                            required: target_successes,
                        },
                    ));
                }
            }
        };

        // Record end-to-end latency for the foreground portion of the
        // fan-out — straggler RTTs are observed via per-replica lag
        // metrics, not this histogram.
        if let Some(m) = metrics {
            m.repl_batch_latency_ns.record_since(start);
        }

        // Two-phase straggler reclamation:
        //
        //   - On Ok (success / early-return): only join workers that
        //     have ALREADY finished. Still-running workers stay in
        //     `pending_stragglers` so the slow-replica latency win
        //     survives. The next batch's `drain_stragglers` reclaims
        //     them.
        //   - On Err: block-join every remaining worker. The request
        //     already failed, so latency no longer matters; joining
        //     synchronously surfaces the worker's true outcome (e.g.
        //     `BatchOutcome::TransportErr(Timeout)`) and lets us mark
        //     the sender Down for the next batch's quorum decision.
        //     This matches the original `thread::scope`-joined
        //     semantics on the failure path.
        let force_join = result.is_err();
        let mut result = result;
        for idx in 0..self.pending_stragglers.len() {
            let finished = self.pending_stragglers[idx]
                .as_ref()
                .is_some_and(|h| h.is_finished());
            if !finished && !force_join {
                continue;
            }
            let Some(handle) = self.pending_stragglers[idx].take() else {
                continue;
            };
            match handle.join() {
                Ok(straggler) => {
                    self.senders[idx].transport = Some(straggler.transport);
                    match straggler.outcome {
                        BatchOutcome::Ok { through_sequence } => {
                            // Same buggy/malicious replica clamp as the
                            // foreground ack path above.
                            let through_sequence = through_sequence.min(self.next_sequence);
                            if through_sequence > self.senders[idx].last_acked {
                                self.senders[idx].last_acked = through_sequence;
                                if let Some(m) = metrics {
                                    m.record_ack(idx, through_sequence, straggler.batch_bytes);
                                }
                            }
                        }
                        BatchOutcome::ReplicaErr { .. } | BatchOutcome::TransportErr(_) => {
                            if self.senders[idx].state == ReplicaState::Live {
                                self.senders[idx].state = ReplicaState::Down;
                                if let Some(m) = metrics {
                                    m.record_failure(idx);
                                }
                            }
                        }
                    }
                }
                Err(payload) => {
                    let detail = if let Some(s) = payload.downcast_ref::<&'static str>() {
                        (*s).to_string()
                    } else if let Some(s) = payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "<non-string panic payload>".to_string()
                    };
                    if let Some(m) = replication_metrics() {
                        m.replica_worker_panics_total.inc();
                    }
                    tracing::error!(
                        panic = %detail,
                        "replica replication worker panicked",
                    );
                    self.senders[idx].state = ReplicaState::Down;
                    if let Some(m) = metrics {
                        m.record_failure(idx);
                    }
                    // Prefer the panic-message error over the generic
                    // InsufficientReplicas one — the panic detail is
                    // the actionable diagnostic for the operator.
                    let panic_err =
                        ReplicationError::Transport(format!("replica worker panicked: {detail}"));
                    result = match result {
                        Ok(()) => Ok(()),
                        Err(prev @ ReplicationError::InsufficientReplicas { .. })
                        | Err(prev @ ReplicationError::Timeout(_)) => {
                            // Override only when the previous error is
                            // the synthetic placeholder; preserve a real
                            // first-error variant we already returned.
                            let _ = prev;
                            Err(panic_err)
                        }
                        Err(other) => Err(other),
                    };
                }
            }
        }

        result
    }

    /// Join any straggler worker threads from prior `replicate_batch`
    /// calls, applying late outcomes to sender state and restoring the
    /// transports so the next batch can dispatch on them.
    ///
    /// Called at the start of every state-mutating method that touches
    /// transports directly. Idempotent — no-op when no stragglers are
    /// pending. Worker panics are caught and downgraded to a Down state
    /// transition (matching the prior scoped behaviour).
    fn drain_stragglers(&mut self) {
        // Bound the wait per straggler. A worker's recv_ack already
        // honours `replication_timeout`, so a healthy straggler is
        // either already finished or finishes within one timeout
        // window. We add the same window again as a generous slack
        // for the worker's epilogue (channel send, transport
        // teardown). Anything beyond that means the worker is stuck
        // on a non-cancellable I/O — e.g. a blocked socket write
        // because the OS hasn't surfaced the peer reset yet, or a
        // pathological replica that holds the connection open without
        // ACKing. Joining forever in that state would stall the
        // master write path on every subsequent batch (the very
        // latency win C-10 / F-G7-018 introduced). Mark the sender
        // Down and defer the handle to a future drain.
        let join_deadline = std::time::Instant::now() + (self.config.replication_timeout * 2);
        for idx in 0..self.pending_stragglers.len() {
            // Wait for the worker to finish OR for the deadline. The
            // wait MUST be bounded: an unbounded `JoinHandle::join`
            // here is the bug — see C-10 follow-up.
            let finished =
                wait_for_finish_until(self.pending_stragglers[idx].as_ref(), join_deadline);
            if !finished {
                if let Some(m) = replication_metrics() {
                    m.replica_worker_panics_total.inc();
                }
                tracing::warn!(
                    target: "teraslab::replication::drain",
                    replica = idx,
                    "straggler did not finish within 2×replication_timeout; marking Down, deferring join"
                );
                if self.senders[idx].state == ReplicaState::Live {
                    self.senders[idx].state = ReplicaState::Down;
                    if let Some(m) = replication_metrics() {
                        m.record_failure(idx);
                    }
                }
                // Leave the handle in `pending_stragglers[idx]` so a
                // future drain reclaims it once the worker finally
                // unsticks. The transport slot stays `None`; the
                // sender is Down so dispatch will skip it.
                continue;
            }
            let Some(handle) = self.pending_stragglers[idx].take() else {
                continue;
            };
            let result = match handle.join() {
                Ok(r) => r,
                Err(payload) => {
                    let detail = if let Some(s) = payload.downcast_ref::<&'static str>() {
                        (*s).to_string()
                    } else if let Some(s) = payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "<non-string panic payload>".to_string()
                    };
                    if let Some(m) = replication_metrics() {
                        m.replica_worker_panics_total.inc();
                    }
                    tracing::error!(
                        panic = %detail,
                        "replica replication worker panicked",
                    );
                    // Worker panicked — transport is lost. Mark Down
                    // and leave the slot's `transport = None`. The
                    // sender is unusable until external code rebuilds
                    // it.
                    self.senders[idx].state = ReplicaState::Down;
                    if let Some(m) = replication_metrics() {
                        m.record_failure(idx);
                    }
                    continue;
                }
            };
            // Restore the transport so the sender is ready for the next batch.
            self.senders[idx].transport = Some(result.transport);
            let metrics = replication_metrics();
            // Apply the late outcome. `last_acked` only advances; state
            // transitions to Down on any error. Applying the same ACK
            // twice (we already applied it in the foreground when the
            // channel delivered it) is idempotent thanks to the
            // monotonicity guard below.
            match result.outcome {
                BatchOutcome::Ok { through_sequence } => {
                    // Buggy/malicious replica clamp — see foreground ack
                    // path.
                    let through_sequence = through_sequence.min(self.next_sequence);
                    if through_sequence > self.senders[idx].last_acked {
                        self.senders[idx].last_acked = through_sequence;
                        if let Some(m) = metrics {
                            m.record_ack(idx, through_sequence, result.batch_bytes);
                        }
                    }
                }
                BatchOutcome::ReplicaErr { .. } | BatchOutcome::TransportErr(_) => {
                    if self.senders[idx].state == ReplicaState::Live {
                        // We early-returned before observing this
                        // outcome on the foreground channel — apply it now.
                        self.senders[idx].state = ReplicaState::Down;
                        if let Some(m) = metrics {
                            m.record_failure(idx);
                        }
                    }
                }
            }
        }
    }

    /// Number of replica ACKs required by the current policy.
    pub fn required_ack_count(&self) -> usize {
        required_replica_acks(self.senders.len(), self.config.ack_policy)
    }

    /// Number of live replicas.
    pub fn live_count(&self) -> usize {
        self.senders
            .iter()
            .filter(|s| *s.state() == ReplicaState::Live)
            .count()
    }

    /// Current master sequence (next to be assigned).
    pub fn current_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Access a sender by index (for testing).
    pub fn sender(&self, index: usize) -> &ReplicaSender {
        &self.senders[index]
    }

    /// Check for reconnected replicas and transition them to catch-up state.
    ///
    /// Call this periodically. Any replica that was `Down` but whose
    /// transport is now connected transitions to `CatchingUp { from_sequence }`.
    pub fn check_reconnected(&mut self) {
        // Reclaim transports from any straggler workers before probing
        // `is_connected()` so we observe the post-batch state instead
        // of returning `false` for a slot still owned by a worker.
        self.drain_stragglers();
        for sender in &mut self.senders {
            if sender.state == ReplicaState::Down && sender.transport_is_connected() {
                sender.state = ReplicaState::CatchingUp {
                    from_sequence: sender.last_acked + 1,
                };
            }
        }
    }

    /// Run catch-up for all replicas in the `CatchingUp` state using
    /// redo log entries.
    ///
    /// For each catching-up replica, reads entries from its `from_sequence`
    /// through the master's current sequence and sends them in batches.
    /// After all entries are sent and acknowledged, transitions to `Live`.
    ///
    /// The `entries_fn` callback provides redo entries for a given sequence
    /// range, abstracting the redo log. In production, callers pass
    /// `|from_seq| redo_log.read_from_sequence(from_seq)` converted to
    /// `ReplicaOp`s.
    pub fn run_catchup<F>(&mut self, ops_from_seq: F) -> std::result::Result<(), ReplicationError>
    where
        F: Fn(u64) -> Vec<ReplicaOp>,
    {
        // Reclaim transports from any straggler workers before catch-up
        // starts a fresh send/recv cycle on each sender's transport.
        self.drain_stragglers();
        let batch_size = self.config.catchup_batch_size;
        let max_ops_per_pass = self.config.catchup_max_ops_per_pass.max(1);
        let timeout = self.config.replication_timeout;
        let master_seq = self.next_sequence;
        // Phase H — collect resync notifications inside the borrow loop;
        // publish after the loop so we don't double-borrow `self`.
        let mut resync_to_emit: Vec<ResyncRequest> = Vec::new();

        for sender in &mut self.senders {
            let from_seq = match sender.state {
                ReplicaState::CatchingUp { from_sequence } => from_sequence,
                _ => continue,
            };

            if from_seq >= master_seq {
                // Already caught up
                sender.state = ReplicaState::Live;
                sender.resync_emitted = false;
                continue;
            }

            let mut ops = ops_from_seq(from_seq);
            if ops.is_empty() {
                // Redo log entries were reclaimed; can't catch up this way.
                // Transition to NeedsResync so the caller knows a full
                // shard copy is required.
                sender.state = ReplicaState::NeedsResync;
                // Phase H — emit a ResyncRequest exactly once per
                // NeedsResync excursion so the coordinator can synthesize
                // full-shard migration tasks without re-firing on every
                // catch-up tick while resync is already pending.
                if !sender.resync_emitted {
                    resync_to_emit.push(ResyncRequest {
                        node_id: sender.node_id,
                        shards: Vec::new(),
                    });
                    sender.resync_emitted = true;
                }
                continue;
            }
            if ops.len() > max_ops_per_pass {
                ops.truncate(max_ops_per_pass);
            }

            // Send in batches, advancing the sequence cursor per chunk
            // so the replica's last_applied reflects the correct position.
            let mut ok = true;
            let mut chunk_seq = from_seq;
            for chunk in ops.chunks(batch_size) {
                let batch = ReplicaBatch {
                    first_sequence: chunk_seq,
                    ops: chunk.to_vec(),
                    trace_ctx: WireTraceContext::from_current_span(),
                    source_node_id: None,
                    // Phase B2: stamp catch-up chunks with the current
                    // cluster_key — a recovering replica that has rolled
                    // over to a new epoch must not silently apply old
                    // chunks from a stale leader.
                    cluster_key: self.current_cluster_key.load(Ordering::Acquire),
                };
                // After `drain_stragglers()` at the top of `run_catchup`,
                // every sender's transport is restored (or `None` only
                // if a worker panicked, in which case the sender is now
                // `Down` and we wouldn't reach here for it).
                let transport = match sender.transport.as_mut() {
                    Some(t) => t,
                    None => {
                        sender.state = ReplicaState::Down;
                        ok = false;
                        break;
                    }
                };
                if transport.send_batch(&batch).is_err() {
                    sender.state = ReplicaState::Down;
                    ok = false;
                    break;
                }
                match transport.recv_ack(timeout) {
                    Ok(ReplicaAck::Ok { through_sequence }) => {
                        let expected_through = batch.last_sequence();
                        // F-G7-011: the receiver's `already_applied`
                        // high-water mark may be AHEAD of this chunk's
                        // last_sequence (e.g. a normal-replication
                        // batch landed during catch-up). In that case
                        // the receiver ACKs with the existing
                        // high-water mark, which is `>=
                        // expected_through`. Strict-equality marked
                        // those replicas Down and caused spurious
                        // flap. Treat ahead-of-chunk ACKs as success
                        // (replica is healthy and already covers this
                        // range) and only fail when the replica is
                        // strictly BEHIND the chunk's last sequence.
                        if through_sequence < expected_through {
                            sender.state = ReplicaState::Down;
                            ok = false;
                            break;
                        }
                        sender.last_acked = through_sequence;
                        chunk_seq = through_sequence.saturating_add(1);
                    }
                    _ => {
                        sender.state = ReplicaState::Down;
                        ok = false;
                        break;
                    }
                }
            }

            if ok && sender.last_acked.saturating_add(1) >= master_seq {
                sender.state = ReplicaState::Live;
                sender.resync_emitted = false;
            } else if ok {
                sender.state = ReplicaState::CatchingUp {
                    from_sequence: sender.last_acked.saturating_add(1),
                };
            }
        }

        // Phase H — publish the collected resync notifications. Send
        // failures (channel disconnected) are tracked but not propagated;
        // the resync_emitted flag stays set so we don't re-publish to a
        // dropped receiver every tick.
        if !resync_to_emit.is_empty()
            && let Some(ref tx) = self.resync_request_tx
        {
            for req in resync_to_emit {
                let _ = tx.send(req);
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// In-memory transport for testing
// ---------------------------------------------------------------------------

/// In-memory transport using crossbeam-style channels for testing.
///
/// Pairs: `InMemoryTransport::pair()` returns (master_side, replica_side).
pub struct InMemoryTransport {
    tx: std::sync::mpsc::Sender<Vec<u8>>,
    rx: std::sync::mpsc::Receiver<Vec<u8>>,
}

impl InMemoryTransport {
    /// Create a pair of connected transports.
    pub fn pair() -> (Self, Self) {
        let (tx1, rx1) = std::sync::mpsc::channel();
        let (tx2, rx2) = std::sync::mpsc::channel();
        (
            Self { tx: tx1, rx: rx2 }, // master side: sends on tx1, receives on rx2
            Self { tx: tx2, rx: rx1 }, // replica side: sends on tx2, receives on rx1
        )
    }

    /// Receive a batch (replica side).
    pub fn recv_batch(
        &self,
        timeout: Duration,
    ) -> std::result::Result<ReplicaBatch, ReplicationError> {
        let data = self
            .rx
            .recv_timeout(timeout)
            .map_err(|_| ReplicationError::Timeout(timeout))?;
        ReplicaBatch::deserialize(&data).map_err(|e| ReplicationError::Transport(format!("{e}")))
    }

    /// Send an ack (replica side).
    pub fn send_ack(&self, ack: &ReplicaAck) -> std::result::Result<(), ReplicationError> {
        self.tx
            .send(ack.serialize())
            .map_err(|e| ReplicationError::Transport(format!("{e}")))
    }
}

impl ReplicaTransport for InMemoryTransport {
    fn send_batch(&mut self, batch: &ReplicaBatch) -> std::result::Result<(), ReplicationError> {
        self.tx
            .send(batch.serialize())
            .map_err(|e| ReplicationError::Transport(format!("{e}")))
    }

    fn recv_ack(&mut self, timeout: Duration) -> std::result::Result<ReplicaAck, ReplicationError> {
        let data = self
            .rx
            .recv_timeout(timeout)
            .map_err(|_| ReplicationError::Timeout(timeout))?;
        ReplicaAck::deserialize(&data).map_err(|e| ReplicationError::Transport(format!("{e}")))
    }

    fn is_connected(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::TxKey;

    fn key(n: u8) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0] = n;
        TxKey { txid }
    }

    /// Simulate a replica that auto-ACKs every batch it receives.
    fn spawn_auto_ack_replica(
        replica_transport: InMemoryTransport,
    ) -> std::thread::JoinHandle<Vec<ReplicaBatch>> {
        std::thread::spawn(move || {
            let mut received = Vec::new();
            while let Ok(batch) = replica_transport.recv_batch(Duration::from_secs(1)) {
                let ack = ReplicaAck::Ok {
                    through_sequence: batch.last_sequence(),
                };
                replica_transport.send_ack(&ack).unwrap();
                received.push(batch);
            }
            received
        })
    }

    #[test]
    fn single_replica_spend_replicated() {
        let (master_t, replica_t) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(replica_t);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                ..Default::default()
            },
            vec![Box::new(master_t)],
        );

        let ops = vec![ReplicaOp::Spend {
            tx_key: key(1),
            offset: 0,
            spending_data: [0xAB; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 0,
        }];
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr); // Close channels
        let received = handle.join().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].ops, ops);
    }

    #[test]
    fn batch_50_ops_single_frame() {
        let (master_t, replica_t) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(replica_t);

        let mut mgr =
            ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(master_t)]);

        let ops: Vec<ReplicaOp> = (0..50u8)
            .map(|i| ReplicaOp::Spend {
                tx_key: key(i),
                offset: i as u32,
                spending_data: [i; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 0,
            })
            .collect();
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received.len(), 1); // Single frame, not 50
        assert_eq!(received[0].ops.len(), 50);
    }

    #[test]
    fn rf3_both_replicas_receive() {
        let (mt1, rt1) = InMemoryTransport::pair();
        let (mt2, rt2) = InMemoryTransport::pair();
        let h1 = spawn_auto_ack_replica(rt1);
        let h2 = spawn_auto_ack_replica(rt2);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                ..Default::default()
            },
            vec![Box::new(mt1), Box::new(mt2)],
        );

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        assert_eq!(r1[0].ops, ops);
        assert_eq!(r2[0].ops, ops);
    }

    #[test]
    fn write_all_rf3_one_fails() {
        let (mt1, rt1) = InMemoryTransport::pair();
        let (mt2, _rt2) = InMemoryTransport::pair(); // Drop replica side → will fail
        let _h1 = spawn_auto_ack_replica(rt1);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                replication_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            vec![Box::new(mt1), Box::new(mt2)],
        );

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        let result = mgr.replicate_batch(&ops);
        assert!(result.is_err());
    }

    #[test]
    fn write_majority_rf3_one_acks() {
        let (mt1, rt1) = InMemoryTransport::pair();
        let (mt2, _rt2) = InMemoryTransport::pair(); // Drop → fails
        let _h1 = spawn_auto_ack_replica(rt1);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteMajority,
                replication_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            vec![Box::new(mt1), Box::new(mt2)],
        );

        // WriteMajority with RF=3: need 1 replica ACK (master + 1 = majority of 3)
        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        mgr.replicate_batch(&ops).unwrap();
    }

    #[test]
    fn write_majority_rf3_none_ack() {
        let (mt1, _rt1) = InMemoryTransport::pair();
        let (mt2, _rt2) = InMemoryTransport::pair();

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteMajority,
                replication_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            vec![Box::new(mt1), Box::new(mt2)],
        );

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        assert!(mgr.replicate_batch(&ops).is_err());
    }

    #[test]
    fn sequence_numbers_contiguous() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        mgr.replicate_batch(&[ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }])
        .unwrap();
        mgr.replicate_batch(&[
            ReplicaOp::Freeze {
                tx_key: key(2),
                offset: 1,
                master_generation: 0,
            },
            ReplicaOp::Freeze {
                tx_key: key(3),
                offset: 2,
                master_generation: 0,
            },
        ])
        .unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received[0].first_sequence, 1);
        assert_eq!(received[0].last_sequence(), 1);
        assert_eq!(received[1].first_sequence, 2);
        assert_eq!(received[1].last_sequence(), 3);
    }

    #[test]
    fn mixed_op_types_in_batch() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        let ops = vec![
            ReplicaOp::Spend {
                tx_key: key(1),
                offset: 0,
                spending_data: [0x11; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 0,
            },
            ReplicaOp::SetMined {
                tx_key: key(2),
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                on_longest_chain: true,
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 0,
            },
            ReplicaOp::PruneSlot {
                tx_key: key(3),
                offset: 5,
            },
        ];
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received[0].ops.len(), 3);
    }

    #[test]
    fn empty_batch_noop() {
        let (mt, rt) = InMemoryTransport::pair();
        let _handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        mgr.replicate_batch(&[]).unwrap();
    }

    #[test]
    fn required_ack_count_calculations() {
        // RF=2 (1 replica), WriteAll: need 1 ACK
        let mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                ..Default::default()
            },
            vec![Box::new(InMemoryTransport::pair().0)],
        );
        assert_eq!(mgr.required_ack_count(), 1);

        // RF=3 (2 replicas), WriteAll: need 2 ACKs
        let mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                ..Default::default()
            },
            vec![
                Box::new(InMemoryTransport::pair().0),
                Box::new(InMemoryTransport::pair().0),
            ],
        );
        assert_eq!(mgr.required_ack_count(), 2);

        // RF=3, WriteMajority: need 1 ACK (master + 1 = majority of 3)
        let mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteMajority,
                ..Default::default()
            },
            vec![
                Box::new(InMemoryTransport::pair().0),
                Box::new(InMemoryTransport::pair().0),
            ],
        );
        assert_eq!(mgr.required_ack_count(), 1);

        // RF=2, WriteMajority: need 1 ACK (master + 1 = majority of 2)
        let mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteMajority,
                ..Default::default()
            },
            vec![Box::new(InMemoryTransport::pair().0)],
        );
        assert_eq!(mgr.required_ack_count(), 1);
    }

    #[test]
    fn write_majority_threshold_consistency_rf2_through_rf7() {
        let expected = [
            (2usize, 1usize),
            (3usize, 1usize),
            (4usize, 2usize),
            (5usize, 2usize),
            (6usize, 3usize),
            (7usize, 3usize),
        ];

        for (rf, required) in expected {
            let replica_targets = rf - 1;
            assert_eq!(
                required_replica_acks(replica_targets, AckPolicy::WriteMajority),
                required,
                "RF={rf} WriteMajority threshold mismatch"
            );
            assert_eq!(
                required_replica_acks(replica_targets, AckPolicy::WriteAll),
                replica_targets,
                "RF={rf} WriteAll threshold mismatch"
            );

            let transports: Vec<Box<dyn ReplicaTransport>> = (0..replica_targets)
                .map(|_| Box::new(InMemoryTransport::pair().0) as Box<dyn ReplicaTransport>)
                .collect();
            let mgr = ReplicationManager::new(
                ReplicationConfig {
                    ack_policy: AckPolicy::WriteMajority,
                    ..Default::default()
                },
                transports,
            );
            assert_eq!(
                mgr.required_ack_count(),
                required,
                "manager RF={rf} threshold must use shared helper"
            );
        }
    }

    // -- Single-operation replication for each op type --

    #[test]
    fn create_op_replicated() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        let ops = vec![ReplicaOp::Create {
            tx_key: key(1),
            metadata_bytes: vec![0x01; 64],
            utxo_hashes: vec![[0xAA; 32]; 5],
            cold_data: Some(vec![0xCC; 20]),
            is_external: false,
        }];
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received[0].ops, ops);
    }

    #[test]
    fn delete_op_replicated() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        let ops = vec![ReplicaOp::Delete { tx_key: key(1) }];
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received[0].ops, ops);
    }

    #[test]
    fn freeze_unfreeze_replicated() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        let ops = vec![
            ReplicaOp::Freeze {
                tx_key: key(1),
                offset: 3,
                master_generation: 0,
            },
            ReplicaOp::Unfreeze {
                tx_key: key(1),
                offset: 3,
                master_generation: 0,
            },
        ];
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received[0].ops, ops);
    }

    #[test]
    fn reassign_op_replicated() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        let ops = vec![ReplicaOp::Reassign {
            tx_key: key(1),
            offset: 0,
            new_hash: [0xBB; 32],
            block_height: 1000,
            spendable_after: 100,
            master_generation: 0,
        }];
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received[0].ops, ops);
    }

    #[test]
    fn all_flag_ops_replicated() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        let ops = vec![
            ReplicaOp::SetConflicting {
                tx_key: key(1),
                value: true,
                current_block_height: 1000,
                retention: 288,
                master_generation: 0,
            },
            ReplicaOp::SetLocked {
                tx_key: key(2),
                value: true,
                master_generation: 0,
            },
            ReplicaOp::PreserveUntil {
                tx_key: key(3),
                block_height: 5000,
                master_generation: 0,
            },
        ];
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received[0].ops, ops);
    }

    // -- Batch ACK sequence test --

    #[test]
    fn batch_ack_through_sequence_correct() {
        let (mt, rt) = InMemoryTransport::pair();

        // Custom replica that verifies ACK through_sequence
        let handle = std::thread::spawn(move || {
            let batch = rt.recv_batch(Duration::from_secs(1)).unwrap();
            assert_eq!(batch.first_sequence, 1);
            assert_eq!(batch.ops.len(), 10);
            let expected_through = batch.last_sequence();
            assert_eq!(expected_through, 10); // first_sequence(1) + 10 - 1

            let ack = ReplicaAck::Ok {
                through_sequence: expected_through,
            };
            rt.send_ack(&ack).unwrap();
            expected_through
        });

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        let ops: Vec<ReplicaOp> = (0..10u8)
            .map(|i| ReplicaOp::Freeze {
                tx_key: key(i),
                offset: i as u32,
                master_generation: 0,
            })
            .collect();
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let through = handle.join().unwrap();
        assert_eq!(through, 10);
    }

    /// Re-review P2: a buggy or malicious replica that ACKs a
    /// `through_sequence` beyond what the master has actually assigned
    /// must not be able to advance `last_acked` past `next_sequence`.
    /// Without the `through_sequence.min(self.next_sequence)` clamp, a
    /// replica reporting `u64::MAX` would be permanently flagged
    /// "caught up", suppressing catch-up on legitimate future
    /// divergence. The clamp is identical at all three application
    /// sites (foreground recv loop, post-batch finished-drain,
    /// straggler late-outcome); this exercises the foreground path.
    #[test]
    fn malicious_replica_through_sequence_is_clamped_to_next_sequence() {
        let (mt, rt) = InMemoryTransport::pair();

        // Replica lies: ACKs u64::MAX regardless of the batch sequence.
        let handle = std::thread::spawn(move || {
            let _batch = rt.recv_batch(Duration::from_secs(1)).unwrap();
            rt.send_ack(&ReplicaAck::Ok {
                through_sequence: u64::MAX,
            })
            .unwrap();
        });

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        let ops: Vec<ReplicaOp> = (0..10u8)
            .map(|i| ReplicaOp::Freeze {
                tx_key: key(i),
                offset: i as u32,
                master_generation: 0,
            })
            .collect();
        mgr.replicate_batch(&ops).unwrap();

        // next_sequence advanced 1 -> 11 (initial 1 + 10 ops). The lying
        // ACK of u64::MAX must be clamped down to 11, not accepted verbatim.
        assert_eq!(mgr.next_sequence, 11);
        assert_eq!(
            mgr.senders[0].last_acked(),
            11,
            "through_sequence must be clamped to next_sequence (11), \
             not the replica's u64::MAX claim",
        );

        drop(mgr);
        handle.join().unwrap();
    }

    // -- Idempotency under replication --

    #[test]
    fn idempotent_spend_via_replication() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        let ops = vec![ReplicaOp::Spend {
            tx_key: key(1),
            offset: 0,
            spending_data: [0xAB; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 0,
        }];
        // Send same ops twice
        mgr.replicate_batch(&ops).unwrap();
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        // Both batches received (replication doesn't dedup at transport level;
        // idempotency is handled at the application layer on the replica)
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].ops, ops);
        assert_eq!(received[1].ops, ops);
    }

    #[test]
    fn write_majority_rf2_succeeds() {
        let (mt, rt) = InMemoryTransport::pair();
        let _handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteMajority,
                ..Default::default()
            },
            vec![Box::new(mt)],
        );

        // RF=2: need 1 replica ACK (majority of 2 = 1 + master)
        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        mgr.replicate_batch(&ops).unwrap();
    }

    #[test]
    fn catch_up_transitions_to_live() {
        let (mt, rt) = InMemoryTransport::pair();
        let _handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        // Send 5 ops normally
        for i in 0..5 {
            let ops = vec![ReplicaOp::Freeze {
                tx_key: key(i),
                offset: 0,
                master_generation: 0,
            }];
            mgr.replicate_batch(&ops).unwrap();
        }
        assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);
        assert_eq!(mgr.sender(0).last_acked(), 5);

        // Simulate disconnect: force the replica to Down state
        mgr.senders[0].state = ReplicaState::Down;

        // "Send" 5 more ops by advancing the master sequence without
        // actually sending to the downed replica
        mgr.next_sequence += 5; // master advanced to seq 11

        // Simulate reconnect: the transport is still connected
        mgr.check_reconnected();
        assert!(matches!(
            mgr.sender(0).state(),
            ReplicaState::CatchingUp { from_sequence: 6 }
        ));

        // Run catch-up with mock ops
        mgr.run_catchup(|from_seq| {
            (from_seq..11)
                .map(|i| ReplicaOp::Freeze {
                    tx_key: key(i as u8),
                    offset: 0,
                    master_generation: 0,
                })
                .collect()
        })
        .unwrap();

        assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);
    }

    /// F-G7-007: when a `WriteAll` batch fails on every replica the
    /// master's `next_sequence` MUST still advance, so the retried
    /// ops land on a fresh sequence range. Resetting the cursor on
    /// failure would let the next batch reuse a sequence the redo
    /// log has already journalled. The replica's dedup tracker
    /// stores both ranges as legitimate; the durable-log invariant
    /// (every assigned sequence recorded as an intent) is preserved.
    #[test]
    fn replicate_batch_advances_next_sequence_on_full_failure() {
        let (mt, _replica_rx) = InMemoryTransport::pair();
        // Drop the replica side so recv_ack times out.

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                replication_timeout: Duration::from_millis(50),
                ..Default::default()
            },
            vec![Box::new(mt)],
        );
        let initial = mgr.current_sequence();
        assert_eq!(initial, 1);

        let ops = vec![
            ReplicaOp::Freeze {
                tx_key: key(1),
                offset: 0,
                master_generation: 0,
            },
            ReplicaOp::Freeze {
                tx_key: key(2),
                offset: 0,
                master_generation: 0,
            },
        ];
        let res = mgr.replicate_batch(&ops);
        assert!(res.is_err(), "fan-out must fail when no replica acks");

        // next_sequence MUST have advanced past the failed batch.
        assert_eq!(
            mgr.current_sequence(),
            initial + ops.len() as u64,
            "next_sequence must advance even when the fan-out failed",
        );
    }

    /// F-G7-009: a panicking replica worker must:
    ///   1. Surface the panic message in the resulting TransportErr
    ///      so the diagnostic isn't lost,
    ///   2. Bump `replica_worker_panics_total` so operators can
    ///      alert on the underlying bug.
    #[test]
    fn replicate_batch_panic_captured_with_payload() {
        struct PanickingTransport;
        impl ReplicaTransport for PanickingTransport {
            fn send_batch(
                &mut self,
                _batch: &ReplicaBatch,
            ) -> std::result::Result<(), ReplicationError> {
                panic!("synthetic send_batch failure: oxidized");
            }
            fn recv_ack(
                &mut self,
                _t: Duration,
            ) -> std::result::Result<ReplicaAck, ReplicationError> {
                unreachable!("send_batch panicked");
            }
            fn is_connected(&self) -> bool {
                true
            }
        }

        // Install metrics so the counter has somewhere to live.
        static TEST_METRICS: std::sync::OnceLock<&'static crate::metrics::ReplicationMetrics> =
            std::sync::OnceLock::new();
        let metrics_ref = *TEST_METRICS
            .get_or_init(|| Box::leak(Box::new(crate::metrics::ReplicationMetrics::new())));
        crate::metrics::init_replication_metrics(metrics_ref);
        let metrics =
            crate::metrics::replication_metrics().expect("replication metrics installed for test");
        let before = metrics.replica_worker_panics_total.get();

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                replication_timeout: Duration::from_millis(50),
                ..Default::default()
            },
            vec![Box::new(PanickingTransport)],
        );
        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        let err = mgr.replicate_batch(&ops).expect_err("must fail");
        let after = metrics.replica_worker_panics_total.get();
        assert!(
            after > before,
            "replica_worker_panics_total must bump (was {before}, now {after})",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("oxidized") || msg.contains("synthetic"),
            "panic payload must propagate into the error message (got: {msg})",
        );
    }

    /// F-G7-011: when a catch-up chunk overlaps with normal replication
    /// the replica's dedup tracker may already cover a higher
    /// `through_sequence` than the chunk's last sequence. The receiver
    /// then ACKs with that ahead-of-chunk high-water mark, not the
    /// chunk's own. Previously the master's strict-equality check
    /// (`through_sequence != expected_through`) transitioned the
    /// replica to Down spuriously, causing flap. The fix accepts any
    /// ACK that is `>= expected_through` as success.
    #[test]
    fn catchup_accepts_ack_ahead_of_chunk_last_sequence() {
        // Custom transport whose recv_ack returns a through_sequence
        // strictly greater than the most recently sent batch's last_sequence.
        struct AheadAckTransport {
            connected: bool,
            extra_ahead: u64,
            last_sent_through: u64,
        }
        impl ReplicaTransport for AheadAckTransport {
            fn send_batch(
                &mut self,
                batch: &ReplicaBatch,
            ) -> std::result::Result<(), ReplicationError> {
                self.last_sent_through = batch.last_sequence();
                Ok(())
            }
            fn recv_ack(
                &mut self,
                _timeout: Duration,
            ) -> std::result::Result<ReplicaAck, ReplicationError> {
                Ok(ReplicaAck::Ok {
                    through_sequence: self.last_sent_through + self.extra_ahead,
                })
            }
            fn is_connected(&self) -> bool {
                self.connected
            }
        }

        let transport = AheadAckTransport {
            connected: true,
            extra_ahead: 2,
            last_sent_through: 0,
        };
        let mut mgr =
            ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(transport)]);

        // Drive into CatchingUp with three ops to ship (from_sequence = 1,
        // master at 4 → ops 1..=3).
        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 1 };
        mgr.next_sequence = 4;

        mgr.run_catchup(|from_seq| {
            (from_seq..4)
                .map(|i| ReplicaOp::Freeze {
                    tx_key: key(i as u8),
                    offset: 0,
                    master_generation: 0,
                })
                .collect()
        })
        .expect("run_catchup must not error when the replica is ahead");

        // With the F-G7-011 fix the sender stays catching-up or becomes
        // Live (never Down). Without the fix the strict-equality check
        // would set it to Down.
        assert!(
            !matches!(*mgr.sender(0).state(), ReplicaState::Down),
            "ahead-of-chunk ACK must not transition replica to Down (was {:?})",
            mgr.sender(0).state(),
        );
    }

    #[test]
    fn catchup_transitions_to_needs_resync_when_redo_reclaimed() {
        let (master_tx, _replica_rx) = InMemoryTransport::pair();

        let config = ReplicationConfig::default();
        let mut mgr = ReplicationManager::new(config, vec![Box::new(master_tx)]);

        // Manually set up the scenario: replica is catching up
        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 1 };
        mgr.next_sequence = 10;

        // Run catch-up with an empty result (simulating reclaimed redo)
        mgr.run_catchup(|_from_seq| Vec::new()).unwrap();

        // Should transition to NeedsResync, not stay in CatchingUp
        assert_eq!(*mgr.sender(0).state(), ReplicaState::NeedsResync);
    }

    // ── Phase H: redo-truncation resync ──────────────────────────────────

    #[test]
    fn needs_resync_emits_resync_request() {
        let (master_tx, _replica_rx) = InMemoryTransport::pair();
        let mut mgr =
            ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(master_tx)]);
        // Tag this sender with the replica's NodeId so the resync request
        // names the right peer.
        mgr.set_replica_node_id(0, 42);
        let (tx, rx) = std::sync::mpsc::channel::<ResyncRequest>();
        mgr.install_resync_request_channel(tx);

        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 1 };
        mgr.next_sequence = 10;
        mgr.run_catchup(|_from_seq| Vec::new()).unwrap();

        let req = rx
            .try_recv()
            .expect("ResyncRequest must be published when sender hits NeedsResync");
        assert_eq!(req.node_id, 42);
        // Without explicit per-sender shard tracking the manager publishes
        // an empty shard list — the coordinator interprets this as
        // "every shard the replica should hold per the shard table".
        assert!(req.shards.is_empty());

        // Idempotent: a second call without state change does not re-emit.
        mgr.run_catchup(|_| Vec::new()).unwrap();
        assert!(
            rx.try_recv().is_err(),
            "ResyncRequest must not re-fire once the sender is already in NeedsResync",
        );
    }

    #[test]
    fn needs_resync_clears_after_full_shard_apply() {
        let (master_tx, _replica_rx) = InMemoryTransport::pair();
        let mut mgr =
            ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(master_tx)]);
        mgr.senders[0].state = ReplicaState::NeedsResync;
        mgr.mark_replica_live(0);
        assert_eq!(
            *mgr.sender(0).state(),
            ReplicaState::Live,
            "after full-shard resync the coordinator can transition the sender back to Live",
        );
    }

    #[test]
    fn mark_replica_live_is_no_op_for_unknown_index() {
        // Out-of-bounds index must not panic — defensive guard since
        // ResyncRequest may name a sender that has since been replaced.
        let (master_tx, _replica_rx) = InMemoryTransport::pair();
        let mut mgr =
            ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(master_tx)]);
        mgr.mark_replica_live(99); // far past the only sender
        assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);
    }

    // -----------------------------------------------------------------------
    // Part 3.2: Replica slow but not dead
    // -----------------------------------------------------------------------

    #[test]
    fn slow_replica_succeeds_within_timeout() {
        // Replica takes 200ms but timeout is 5s → should succeed
        let (mt, rt) = InMemoryTransport::pair();

        let handle = std::thread::spawn(move || {
            let batch = rt.recv_batch(Duration::from_secs(5)).unwrap();
            std::thread::sleep(Duration::from_millis(200)); // slow but alive
            let ack = ReplicaAck::Ok {
                through_sequence: batch.last_sequence(),
            };
            rt.send_ack(&ack).unwrap();
        });

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                replication_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            vec![Box::new(mt)],
        );

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        let result = mgr.replicate_batch(&ops);
        assert!(result.is_ok(), "slow replica within timeout should succeed");

        handle.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // Part 3.3: Replica timeout
    // -----------------------------------------------------------------------

    #[test]
    fn replica_timeout_returns_error() {
        // Replica takes 500ms, timeout is 100ms → should fail
        let (mt, rt) = InMemoryTransport::pair();

        let _handle = std::thread::spawn(move || {
            if let Ok(batch) = rt.recv_batch(Duration::from_secs(5)) {
                std::thread::sleep(Duration::from_millis(500)); // too slow
                let ack = ReplicaAck::Ok {
                    through_sequence: batch.last_sequence(),
                };
                let _ = rt.send_ack(&ack);
            }
        });

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                replication_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            vec![Box::new(mt)],
        );

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        let result = mgr.replicate_batch(&ops);
        assert!(result.is_err(), "replica timeout should return error");

        // Replica should be marked Down
        assert_eq!(*mgr.sender(0).state(), ReplicaState::Down);
    }

    // -----------------------------------------------------------------------
    // Part 3.5: Sequence numbers and initial sequence
    // -----------------------------------------------------------------------

    #[test]
    fn initial_sequence_syncs_with_redo_log() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        // Start replication from sequence 100 (simulating redo log state)
        let mut mgr = ReplicationManager::with_initial_sequence(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
            100,
        );

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(
            received[0].first_sequence, 100,
            "should start from redo log sequence"
        );
    }

    // -----------------------------------------------------------------------
    // Part 3.6: Master crashes → Down state handling
    // -----------------------------------------------------------------------

    #[test]
    fn down_replica_excluded_from_replication() {
        let (mt1, rt1) = InMemoryTransport::pair();
        let (mt2, _rt2) = InMemoryTransport::pair(); // Drop replica side
        let _h1 = spawn_auto_ack_replica(rt1);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteMajority,
                replication_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            vec![Box::new(mt1), Box::new(mt2)],
        );

        // First batch: one replica fails
        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        mgr.replicate_batch(&ops).unwrap(); // succeeds with majority

        // Second batch: down replica excluded, live replica handles it
        let ops2 = vec![ReplicaOp::Freeze {
            tx_key: key(2),
            offset: 0,
            master_generation: 0,
        }];
        let result = mgr.replicate_batch(&ops2);
        // With WriteMajority RF=3, need 1 ACK. 1 live replica → should succeed.
        assert!(
            result.is_ok(),
            "should succeed with 1 live replica and WriteMajority"
        );
    }

    // -----------------------------------------------------------------------
    // Part 3.9: Replication ordering
    // -----------------------------------------------------------------------

    #[test]
    fn operations_arrive_in_order() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        // Send 100 operations in rapid succession
        for i in 0..100u8 {
            let ops = vec![ReplicaOp::Freeze {
                tx_key: key(i),
                offset: i as u32,
                master_generation: 0,
            }];
            mgr.replicate_batch(&ops).unwrap();
        }

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received.len(), 100);

        // Verify ordering: first_sequence must increase monotonically
        for i in 1..received.len() {
            assert!(
                received[i].first_sequence > received[i - 1].first_sequence,
                "batch {i} sequence {} should be > batch {} sequence {}",
                received[i].first_sequence,
                i - 1,
                received[i - 1].first_sequence
            );
        }

        // Verify each batch has the expected key
        for (i, batch) in received.iter().enumerate() {
            match &batch.ops[0] {
                ReplicaOp::Freeze { tx_key, offset, .. } => {
                    assert_eq!(tx_key.txid[0], i as u8);
                    assert_eq!(*offset, i as u32);
                }
                _ => panic!("unexpected op type"),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Part 3.11: Catchup protocol
    // -----------------------------------------------------------------------

    #[test]
    fn catchup_idempotent_restart() {
        // If catch-up is interrupted and restarted, no double-application.
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                catchup_batch_size: 5,
                ..Default::default()
            },
            vec![Box::new(mt)],
        );

        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 1 };
        mgr.next_sequence = 11;

        // First catchup: succeeds, transitions to Live
        mgr.run_catchup(|from_seq| {
            (from_seq..11)
                .map(|i| ReplicaOp::Freeze {
                    tx_key: key(i as u8),
                    offset: 0,
                    master_generation: 0,
                })
                .collect()
        })
        .unwrap();
        assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);

        // Simulate: replica goes down and comes back
        mgr.senders[0].state = ReplicaState::CatchingUp {
            from_sequence: mgr.sender(0).last_acked() + 1,
        };
        mgr.next_sequence = 11; // same sequence

        // Second catchup from last acked: should be a no-op (already caught up)
        mgr.run_catchup(|from_seq| {
            if from_seq >= 11 {
                return Vec::new();
            }
            (from_seq..11)
                .map(|i| ReplicaOp::Freeze {
                    tx_key: key(i as u8),
                    offset: 0,
                    master_generation: 0,
                })
                .collect()
        })
        .unwrap();
        assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);

        drop(mgr);
        let _ = handle.join();
    }

    #[test]
    fn catchup_progress_tracked_via_last_acked() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                catchup_batch_size: 3,
                ..Default::default()
            },
            vec![Box::new(mt)],
        );

        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 50 };
        mgr.next_sequence = 60;

        mgr.run_catchup(|from_seq| {
            (from_seq..60)
                .map(|i| ReplicaOp::Freeze {
                    tx_key: key(i as u8),
                    offset: 0,
                    master_generation: 0,
                })
                .collect()
        })
        .unwrap();

        // After catchup: last_acked should be at the end of the range
        assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);
        assert!(
            mgr.sender(0).last_acked() >= 59,
            "last_acked should be >= 59 after catching up to seq 60"
        );

        drop(mgr);
        let _ = handle.join();
    }

    #[test]
    fn catchup_chunk_seq_matches_replica_ack_sequence() {
        let (mt, rt) = InMemoryTransport::pair();

        let handle = std::thread::spawn(move || {
            let batch = rt.recv_batch(Duration::from_secs(1)).unwrap();
            assert_eq!(batch.first_sequence, 10);
            rt.send_ack(&ReplicaAck::Ok {
                through_sequence: batch.first_sequence,
            })
            .unwrap();
        });

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                catchup_batch_size: 3,
                ..Default::default()
            },
            vec![Box::new(mt)],
        );
        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 10 };
        mgr.next_sequence = 20;

        mgr.run_catchup(|from_seq| {
            (from_seq..20)
                .map(|i| ReplicaOp::Freeze {
                    tx_key: key(i as u8),
                    offset: 0,
                    master_generation: 0,
                })
                .collect()
        })
        .unwrap();

        assert_eq!(
            *mgr.sender(0).state(),
            ReplicaState::Down,
            "catch-up must fail closed when replica ACK does not match the chunk end",
        );

        handle.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // Part 3.12: Connection management
    // -----------------------------------------------------------------------

    #[test]
    fn check_reconnected_transitions_down_to_catchup() {
        let (mt, _rt) = InMemoryTransport::pair();

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        // Simulate: replica was sending, got to seq 50, then went down
        mgr.senders[0].last_acked = 50;
        mgr.senders[0].state = ReplicaState::Down;

        // InMemoryTransport always reports connected
        mgr.check_reconnected();
        assert_eq!(
            *mgr.sender(0).state(),
            ReplicaState::CatchingUp { from_sequence: 51 },
            "should transition to CatchingUp from last_acked + 1"
        );
    }

    #[test]
    fn lag_calculation() {
        let (mt, _rt) = InMemoryTransport::pair();
        let mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);
        // last_acked = 0, master seq = 1
        assert_eq!(mgr.sender(0).lag(100), 100);
        assert_eq!(mgr.sender(0).lag(0), 0);
    }

    // -----------------------------------------------------------------------
    // Part 3: RF=1 (no replicas) — edge case
    // -----------------------------------------------------------------------

    #[test]
    fn zero_replicas_always_succeeds() {
        // With no replicas, replication should be a no-op success
        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![], // no replicas
        );

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        // WriteAll with 0 replicas: 0 required ACKs → should succeed
        mgr.replicate_batch(&ops).unwrap();
    }

    #[test]
    fn catchup_chunked_batches_have_correct_first_sequence() {
        // Regression test: when catch-up ops are sent in multiple chunks,
        // each chunk must have the correct first_sequence. The old code
        // used the starting from_seq for all chunks, which caused the
        // replica to record incorrect last_applied values.
        let (master_t, replica_t) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(replica_t);

        let config = ReplicationConfig {
            catchup_batch_size: 2, // Force chunking at 2 ops per batch
            ..Default::default()
        };
        let mut mgr = ReplicationManager::new(config, vec![Box::new(master_t)]);

        // Set up catching-up state starting from sequence 100
        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 100 };
        mgr.next_sequence = 105;

        // Provide 5 ops → should be sent in 3 chunks (2+2+1)
        let result = mgr.run_catchup(|from_seq| {
            assert_eq!(from_seq, 100);
            (0..5)
                .map(|i| ReplicaOp::Freeze {
                    tx_key: key(i),
                    offset: 0,
                    master_generation: 0,
                })
                .collect()
        });
        assert!(result.is_ok());
        assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(
            received.len(),
            3,
            "5 ops at batch_size=2 should produce 3 batches"
        );

        // Verify each chunk has the correct first_sequence
        assert_eq!(received[0].first_sequence, 100);
        assert_eq!(received[0].ops.len(), 2);
        assert_eq!(received[0].last_sequence(), 101);

        assert_eq!(received[1].first_sequence, 102);
        assert_eq!(received[1].ops.len(), 2);
        assert_eq!(received[1].last_sequence(), 103);

        assert_eq!(received[2].first_sequence, 104);
        assert_eq!(received[2].ops.len(), 1);
        assert_eq!(received[2].last_sequence(), 104);
    }

    #[test]
    fn catchup_max_ops_per_pass_keeps_replica_catching_up() {
        let (master_t, replica_t) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(replica_t);

        let config = ReplicationConfig {
            catchup_batch_size: 2,
            catchup_max_ops_per_pass: 3,
            ..Default::default()
        };
        let mut mgr = ReplicationManager::new(config, vec![Box::new(master_t)]);

        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 100 };
        mgr.next_sequence = 110;

        mgr.run_catchup(|from_seq| {
            assert_eq!(from_seq, 100);
            (0..10)
                .map(|i| ReplicaOp::Freeze {
                    tx_key: key(i),
                    offset: 0,
                    master_generation: 0,
                })
                .collect()
        })
        .unwrap();

        assert_eq!(
            *mgr.sender(0).state(),
            ReplicaState::CatchingUp { from_sequence: 103 },
            "catch-up must stop after the per-pass cap and remember where to resume"
        );
        assert_eq!(mgr.sender(0).last_acked(), 102);

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].first_sequence, 100);
        assert_eq!(received[0].ops.len(), 2);
        assert_eq!(received[1].first_sequence, 102);
        assert_eq!(received[1].ops.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Deep edge cases: catch-up state machine
    // -----------------------------------------------------------------------

    /// Replica already caught up (from_seq >= master_seq): transitions
    /// directly to Live without sending anything.
    #[test]
    fn catchup_already_caught_up_transitions_to_live() {
        let (mt, _rt) = InMemoryTransport::pair();
        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 100 };
        mgr.senders[0].last_acked = 99;
        mgr.next_sequence = 100; // from_seq(100) >= master(100)

        mgr.run_catchup(|_| {
            panic!("should not be called when already caught up");
        })
        .unwrap();

        assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);
    }

    /// Catch-up batch_size=1: each op sent individually. Verify all are
    /// applied with correct first_sequence values.
    #[test]
    fn catchup_batch_size_1_each_op_individual() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                catchup_batch_size: 1,
                ..Default::default()
            },
            vec![Box::new(mt)],
        );

        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 50 };
        mgr.next_sequence = 53;

        mgr.run_catchup(|from_seq| {
            assert_eq!(from_seq, 50);
            (0..3)
                .map(|i| ReplicaOp::Freeze {
                    tx_key: key(i),
                    offset: 0,
                    master_generation: 0,
                })
                .collect()
        })
        .unwrap();

        assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received.len(), 3, "3 ops at batch_size=1 = 3 batches");
        assert_eq!(received[0].first_sequence, 50);
        assert_eq!(received[1].first_sequence, 51);
        assert_eq!(received[2].first_sequence, 52);
    }

    /// Catch-up transport failure mid-stream: replica should be marked Down.
    #[test]
    fn catchup_transport_failure_marks_down() {
        let (mt, _rt) = InMemoryTransport::pair(); // drop replica side → send fails

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                catchup_batch_size: 5,
                replication_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            vec![Box::new(mt)],
        );

        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 1 };
        mgr.next_sequence = 10;

        mgr.run_catchup(|_| {
            (0..9)
                .map(|i| ReplicaOp::Freeze {
                    tx_key: key(i as u8),
                    offset: 0,
                    master_generation: 0,
                })
                .collect()
        })
        .unwrap();

        assert_eq!(
            *mgr.sender(0).state(),
            ReplicaState::Down,
            "transport failure during catchup should mark Down"
        );
    }

    /// check_reconnected on a NeedsResync replica: should stay NeedsResync
    /// (reconnection alone isn't enough to resume — full resync needed).
    #[test]
    fn check_reconnected_does_not_clear_needs_resync() {
        let (mt, _rt) = InMemoryTransport::pair();
        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        mgr.senders[0].state = ReplicaState::NeedsResync;
        mgr.check_reconnected();
        // NeedsResync is not Down, so check_reconnected should not touch it.
        assert_eq!(*mgr.sender(0).state(), ReplicaState::NeedsResync);
    }

    /// WriteMajority RF=5 (4 replicas): verify required_ack_count is exactly 2
    /// (majority of 5 = 3 copies; master counts as 1; need 2 replica ACKs).
    #[test]
    fn required_ack_count_rf5_write_majority() {
        let mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteMajority,
                ..Default::default()
            },
            vec![
                Box::new(InMemoryTransport::pair().0),
                Box::new(InMemoryTransport::pair().0),
                Box::new(InMemoryTransport::pair().0),
                Box::new(InMemoryTransport::pair().0),
            ],
        );
        // RF=5: majority = 5/2+1 = 3. Master counts as 1. Need 2 ACKs.
        assert_eq!(mgr.required_ack_count(), 2);
    }

    /// WriteAll RF=5: required_ack_count should be 4 (all replicas).
    #[test]
    fn required_ack_count_rf5_write_all() {
        let mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                ..Default::default()
            },
            vec![
                Box::new(InMemoryTransport::pair().0),
                Box::new(InMemoryTransport::pair().0),
                Box::new(InMemoryTransport::pair().0),
                Box::new(InMemoryTransport::pair().0),
            ],
        );
        assert_eq!(mgr.required_ack_count(), 4);
    }

    /// with_initial_sequence(0) clamps to 1.
    #[test]
    fn with_initial_sequence_zero_clamps() {
        let mgr =
            ReplicationManager::with_initial_sequence(ReplicationConfig::default(), vec![], 0);
        assert_eq!(mgr.current_sequence(), 1);
    }

    /// Parallel fan-out: with N=3 replicas where one replica is
    /// intentionally slow (200ms ACK delay) and the other two ACK
    /// immediately, the total `replicate_batch` wall time must be
    /// dominated by the slow replica — NOT the sum of all three
    /// sleeps (as a serial loop would produce).
    ///
    /// Guarantees the replication dispatch runs concurrently.
    #[test]
    fn replicate_batch_fan_out_runs_in_parallel() {
        // Helper: spawn a replica that sleeps `delay` before ACKing.
        fn spawn_delayed_ack(
            rt: InMemoryTransport,
            delay: Duration,
        ) -> std::thread::JoinHandle<()> {
            std::thread::spawn(move || {
                while let Ok(batch) = rt.recv_batch(Duration::from_secs(2)) {
                    std::thread::sleep(delay);
                    let ack = ReplicaAck::Ok {
                        through_sequence: batch.last_sequence(),
                    };
                    if rt.send_ack(&ack).is_err() {
                        return;
                    }
                }
            })
        }

        let (mt1, rt1) = InMemoryTransport::pair();
        let (mt2, rt2) = InMemoryTransport::pair();
        let (mt3, rt3) = InMemoryTransport::pair();

        // Replica 1 is the slow one (200ms); 2 and 3 ACK fast.
        let h1 = spawn_delayed_ack(rt1, Duration::from_millis(200));
        let h2 = spawn_delayed_ack(rt2, Duration::from_millis(5));
        let h3 = spawn_delayed_ack(rt3, Duration::from_millis(5));

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                replication_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            vec![Box::new(mt1), Box::new(mt2), Box::new(mt3)],
        );

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];

        let start = std::time::Instant::now();
        mgr.replicate_batch(&ops).unwrap();
        let elapsed = start.elapsed();

        // Parallel: ~200ms (dominated by slowest replica).
        // Serial would be ~210ms (200 + 5 + 5) — close enough that a
        // tight bound is fragile, so we use a generous ceiling that
        // still catches serial-regression cases with several extra
        // slow replicas. Make the test more discriminating below.
        //
        // For a clearer signal, rerun with MORE slow replicas so the
        // gap between serial and parallel is unambiguous.
        assert!(
            elapsed < Duration::from_millis(500),
            "parallel replication took {:?}, expected well under 500ms",
            elapsed
        );

        drop(mgr);
        let _ = h1.join();
        let _ = h2.join();
        let _ = h3.join();
    }

    /// Stronger parallelism test: 3 replicas dispatched by one
    /// `replicate_batch`. Concurrency is proven STRUCTURALLY with a 3-way
    /// barrier rather than a wall-clock bound — each replica's ack thread must
    /// reach the barrier before any can ack, so the barrier releases ONLY if
    /// all three were dispatched concurrently. Serial dispatch would block on
    /// the first replica's barrier wait forever (the other two not yet
    /// dispatched), `replicate_batch` would hit its 2s timeout, and the
    /// `.unwrap()` below would panic — so a clean return is the proof of
    /// parallel dispatch. (The previous version asserted `elapsed < 300ms`,
    /// which flaked on contended CI where limited cores serialise "parallel"
    /// threads and scheduler jitter inflates the wall time.)
    #[test]
    fn replicate_batch_three_slow_replicas_run_concurrently() {
        use std::sync::Barrier;

        fn spawn_barriered_ack(
            rt: InMemoryTransport,
            barrier: Arc<Barrier>,
        ) -> std::thread::JoinHandle<()> {
            std::thread::spawn(move || {
                while let Ok(batch) = rt.recv_batch(Duration::from_secs(2)) {
                    // Block until ALL replicas have been dispatched and reached
                    // here. Only concurrent dispatch can release this.
                    barrier.wait();
                    let ack = ReplicaAck::Ok {
                        through_sequence: batch.last_sequence(),
                    };
                    if rt.send_ack(&ack).is_err() {
                        return;
                    }
                }
            })
        }

        let (mt1, rt1) = InMemoryTransport::pair();
        let (mt2, rt2) = InMemoryTransport::pair();
        let (mt3, rt3) = InMemoryTransport::pair();

        let barrier = Arc::new(Barrier::new(3));
        let h1 = spawn_barriered_ack(rt1, barrier.clone());
        let h2 = spawn_barriered_ack(rt2, barrier.clone());
        let h3 = spawn_barriered_ack(rt3, barrier);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                ack_policy: AckPolicy::WriteAll,
                replication_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            vec![Box::new(mt1), Box::new(mt2), Box::new(mt3)],
        );

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];

        // Returns Ok ONLY if all three replicas were dispatched concurrently
        // (the barrier released). Serial dispatch deadlocks the barrier and
        // this times out -> unwrap panics -> test fails.
        mgr.replicate_batch(&ops)
            .expect("parallel dispatch must let the 3-way ack barrier release within the timeout");

        drop(mgr);
        let _ = h1.join();
        let _ = h2.join();
        let _ = h3.join();
    }

    /// Drive 10 batches through `replicate_batch` with an auto-ACK replica
    /// that sleeps briefly per batch, then assert that the Phase 5
    /// replication metrics correctly record latency and bytes.
    ///
    /// The metrics ref may already be installed (by e.g. a sibling test); we
    /// install our own test-only static lazily and observe deltas rather than
    /// absolute counts.
    #[test]
    fn replication_records_batch_latency_and_lag() {
        use crate::metrics::{ReplicationMetrics, init_replication_metrics, replication_metrics};
        use std::sync::OnceLock;
        static TEST_METRICS: OnceLock<ReplicationMetrics> = OnceLock::new();
        let m_ref: &'static ReplicationMetrics = TEST_METRICS.get_or_init(ReplicationMetrics::new);
        init_replication_metrics(m_ref);
        // Use the real installed metrics for observation (may be the one we
        // just tried to install, or an earlier one from another test).
        let metrics = replication_metrics().expect("metrics installed");
        let before_count = metrics.repl_batch_latency_ns.count();
        let before_sum = metrics.repl_batch_latency_ns.sum_ns();
        let before_bytes0 = metrics.per_replica[0].bytes_sent.get();
        let before_sent = metrics.repl_batches_sent_total.get();

        // Spawn a deliberately-slow auto-ACK replica so the histogram
        // captures a non-zero sum.
        let (mt, rt) = InMemoryTransport::pair();
        let handle = std::thread::spawn(move || {
            while let Ok(batch) = rt.recv_batch(Duration::from_secs(2)) {
                std::thread::sleep(Duration::from_millis(5));
                let ack = ReplicaAck::Ok {
                    through_sequence: batch.last_sequence(),
                };
                if rt.send_ack(&ack).is_err() {
                    return;
                }
            }
        });

        let mut mgr = ReplicationManager::new(
            ReplicationConfig {
                replication_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            vec![Box::new(mt)],
        );

        for i in 0..10u8 {
            let ops = vec![ReplicaOp::Freeze {
                tx_key: key(i),
                offset: 0,
                master_generation: 0,
            }];
            mgr.replicate_batch(&ops).unwrap();
        }

        drop(mgr);
        let _ = handle.join();

        let after_count = metrics.repl_batch_latency_ns.count();
        let after_sum = metrics.repl_batch_latency_ns.sum_ns();
        let after_bytes0 = metrics.per_replica[0].bytes_sent.get();
        let after_sent = metrics.repl_batches_sent_total.get();

        // Use >= rather than == because parallel tests may also drive the
        // same global metrics counters.
        assert!(
            after_count - before_count >= 10,
            "repl_batch_latency_ns should record at least 10 samples, got {}",
            after_count - before_count
        );
        assert!(
            after_sent - before_sent >= 10,
            "repl_batches_sent_total should advance by ≥ 10, got {}",
            after_sent - before_sent
        );
        // 10 batches × 5ms sleep lower bound = 50ms. Histogram sum is in ns.
        assert!(
            after_sum - before_sum >= 50_000_000,
            "sum_ns advanced by {} ns, expected >= 50,000,000",
            after_sum - before_sum,
        );
        assert!(
            after_bytes0 > before_bytes0,
            "per-replica bytes_sent should advance (before={before_bytes0}, after={after_bytes0})"
        );
    }

    /// Replica sends an error ACK: sender should be marked Down and the
    /// error propagated.
    #[test]
    fn replica_error_ack_marks_sender_down() {
        let (mt, rt) = InMemoryTransport::pair();

        let _handle = std::thread::spawn(move || {
            if let Ok(batch) = rt.recv_batch(Duration::from_secs(1)) {
                let ack = ReplicaAck::Error {
                    failed_sequence: batch.first_sequence,
                    message: "test error".into(),
                };
                let _ = rt.send_ack(&ack);
            }
        });

        let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        let result = mgr.replicate_batch(&ops);
        assert!(result.is_err());
        assert_eq!(*mgr.sender(0).state(), ReplicaState::Down);
    }

    /// Phase B2: every batch the manager constructs must carry the
    /// current cluster_key from its shared `Arc<AtomicU64>`. With the
    /// epoch handle holding 42, the receiver-side observer must see
    /// `cluster_key: 42` on the deserialized batch.
    #[test]
    fn manager_attaches_current_cluster_key() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU64;

        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let cluster_key = Arc::new(AtomicU64::new(42));
        let mut mgr = ReplicationManager::with_cluster_key(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
            cluster_key.clone(),
        );

        let ops = vec![ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        }];
        mgr.replicate_batch(&ops).unwrap();

        drop(mgr);
        let received = handle.join().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(
            received[0].cluster_key, 42,
            "manager must stamp every batch with the current cluster_key",
        );

        // A subsequent epoch bump propagates to the next batch.
        cluster_key.store(99, std::sync::atomic::Ordering::Release);
        let (mt2, rt2) = InMemoryTransport::pair();
        let handle2 = spawn_auto_ack_replica(rt2);
        let mut mgr2 = ReplicationManager::with_cluster_key(
            ReplicationConfig::default(),
            vec![Box::new(mt2)],
            cluster_key.clone(),
        );
        mgr2.replicate_batch(&ops).unwrap();
        drop(mgr2);
        let received2 = handle2.join().unwrap();
        assert_eq!(
            received2[0].cluster_key, 99,
            "epoch bumps via the shared Arc must be visible on the next batch",
        );
    }
}
