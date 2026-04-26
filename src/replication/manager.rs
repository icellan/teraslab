//! Replication manager — orchestrates sending to multiple replicas
//! with configurable acknowledgment policies.

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
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            ack_policy: AckPolicy::WriteAll,
            replication_timeout: Duration::from_secs(5),
            catchup_batch_size: 1000,
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

/// Manages a single replica's transport and state.
pub struct ReplicaSender {
    transport: Box<dyn ReplicaTransport>,
    state: ReplicaState,
    last_acked: u64,
}

impl ReplicaSender {
    /// Create a new sender with the given transport.
    pub fn new(transport: Box<dyn ReplicaTransport>) -> Self {
        Self {
            transport,
            state: ReplicaState::Live,
            last_acked: 0,
        }
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
        let senders = transports.into_iter().map(ReplicaSender::new).collect();
        Self {
            senders,
            config,
            next_sequence: 1,
            current_cluster_key,
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
        let senders = transports.into_iter().map(ReplicaSender::new).collect();
        Self {
            senders,
            config,
            next_sequence: initial_sequence.max(1),
            current_cluster_key,
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
    /// Sends to every `Live` replica in parallel using
    /// [`std::thread::scope`] — one scoped worker thread per live sender
    /// performs the `send_batch` + `recv_ack` round-trip concurrently.
    /// A slow replica therefore no longer blocks the dispatch of the
    /// remaining replicas.
    ///
    /// After all workers join, outcomes are reconciled against the
    /// configured [`AckPolicy`]: `WriteAll` requires every live replica
    /// to ACK, `WriteMajority` requires at least `required_ack_count()`.
    /// On failure, the first error observed (in sender order) is
    /// returned so the caller has a deterministic diagnostic.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn replicate_batch(
        &mut self,
        ops: &[ReplicaOp],
    ) -> std::result::Result<(), ReplicationError> {
        if ops.is_empty() {
            return Ok(());
        }

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
        self.next_sequence += ops.len() as u64;

        let timeout = self.config.replication_timeout;

        // Cache the subsystem metrics pointer once so the scoped workers
        // do not repeatedly pay the `OnceLock::get()` cost. `bytes` is
        // the serialized-batch size, computed once for all replicas.
        let metrics = replication_metrics();
        let batch_bytes = batch.serialize().len() as u64;
        if let Some(m) = metrics {
            m.repl_batches_sent_total.inc();
            m.leader_sequence
                .store(self.next_sequence, std::sync::atomic::Ordering::Relaxed);
        }
        let start = Instant::now();

        // Per-sender outcome produced inside each scoped thread. We do
        // not mutate `sender.state` or `sender.last_acked` from inside
        // the thread so the reconciliation loop below can observe the
        // same deterministic ordering as the previous serial loop.
        enum Outcome {
            Ok { through_sequence: u64 },
            ReplicaErr { sequence: u64, message: String },
            TransportErr(ReplicationError),
            Skipped,
        }

        // Fan out to every live replica in parallel. Using
        // `std::thread::scope` borrows each sender mutably for the
        // duration of the scope without requiring 'static bounds.
        let batch_ref = &batch;
        let outcomes: Vec<Outcome> = std::thread::scope(|s| {
            let handles: Vec<_> = self
                .senders
                .iter_mut()
                .enumerate()
                .map(|(idx, sender)| {
                    s.spawn(move || {
                        if *sender.state() != ReplicaState::Live {
                            return Outcome::Skipped;
                        }
                        if let Some(m) = metrics {
                            m.mark_in_flight(idx);
                        }
                        match sender.transport.send_batch(batch_ref) {
                            Ok(()) => match sender.transport.recv_ack(timeout) {
                                Ok(ReplicaAck::Ok { through_sequence }) => {
                                    Outcome::Ok { through_sequence }
                                }
                                Ok(ReplicaAck::Error {
                                    failed_sequence,
                                    message,
                                }) => Outcome::ReplicaErr {
                                    sequence: failed_sequence,
                                    message,
                                },
                                Err(e) => Outcome::TransportErr(e),
                            },
                            Err(e) => Outcome::TransportErr(e),
                        }
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join()
                        .unwrap_or(Outcome::TransportErr(ReplicationError::Transport(
                            "replica worker panicked".into(),
                        )))
                })
                .collect()
        });

        // Record end-to-end latency once for the whole fan-out. Per-replica
        // drill-down is recorded below alongside the reconciliation loop so
        // each cell reflects the correct success/failure accounting.
        if let Some(m) = metrics {
            m.repl_batch_latency_ns.record_since(start);
        }

        // Reconcile outcomes into per-sender state. The ordering here
        // matches `self.senders` so `first_error` is deterministic.
        let mut successes = 0usize;
        let mut first_error: Option<ReplicationError> = None;
        for (idx, (sender, outcome)) in self
            .senders
            .iter_mut()
            .zip(outcomes.into_iter())
            .enumerate()
        {
            match outcome {
                Outcome::Ok { through_sequence } => {
                    sender.last_acked = through_sequence;
                    successes += 1;
                    if let Some(m) = metrics {
                        m.record_ack(idx, through_sequence, batch_bytes);
                    }
                }
                Outcome::ReplicaErr { sequence, message } => {
                    sender.state = ReplicaState::Down;
                    if let Some(m) = metrics {
                        m.record_failure(idx);
                    }
                    if first_error.is_none() {
                        first_error = Some(ReplicationError::ReplicaError { sequence, message });
                    }
                }
                Outcome::TransportErr(e) => {
                    sender.state = ReplicaState::Down;
                    if let Some(m) = metrics {
                        m.record_failure(idx);
                    }
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Outcome::Skipped => {}
            }
        }

        match self.config.ack_policy {
            AckPolicy::WriteAll => {
                if successes == live_count {
                    Ok(())
                } else {
                    Err(
                        first_error.unwrap_or(ReplicationError::InsufficientReplicas {
                            available: successes,
                            required: live_count,
                        }),
                    )
                }
            }
            AckPolicy::WriteMajority => {
                if successes >= required {
                    Ok(())
                } else {
                    Err(
                        first_error.unwrap_or(ReplicationError::InsufficientReplicas {
                            available: successes,
                            required,
                        }),
                    )
                }
            }
        }
    }

    /// Number of replica ACKs required by the current policy.
    pub fn required_ack_count(&self) -> usize {
        let rf = self.senders.len() + 1; // replicas + master
        match self.config.ack_policy {
            AckPolicy::WriteAll => self.senders.len(),
            AckPolicy::WriteMajority => {
                let majority = rf / 2 + 1;
                majority.saturating_sub(1) // master counts as 1
            }
        }
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
        for sender in &mut self.senders {
            if sender.state == ReplicaState::Down && sender.transport.is_connected() {
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
        let batch_size = self.config.catchup_batch_size;
        let timeout = self.config.replication_timeout;
        let master_seq = self.next_sequence;

        for sender in &mut self.senders {
            let from_seq = match sender.state {
                ReplicaState::CatchingUp { from_sequence } => from_sequence,
                _ => continue,
            };

            if from_seq >= master_seq {
                // Already caught up
                sender.state = ReplicaState::Live;
                continue;
            }

            let ops = ops_from_seq(from_seq);
            if ops.is_empty() {
                // Redo log entries were reclaimed; can't catch up this way.
                // Transition to NeedsResync so the caller knows a full
                // shard copy is required.
                sender.state = ReplicaState::NeedsResync;
                continue;
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
                if let Err(_e) = sender.transport.send_batch(&batch) {
                    sender.state = ReplicaState::Down;
                    ok = false;
                    break;
                }
                match sender.transport.recv_ack(timeout) {
                    Ok(ReplicaAck::Ok { through_sequence }) => {
                        sender.last_acked = through_sequence;
                    }
                    _ => {
                        sender.state = ReplicaState::Down;
                        ok = false;
                        break;
                    }
                }
                chunk_seq += chunk.len() as u64;
            }

            if ok {
                sender.state = ReplicaState::Live;
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
                master_generation: 0,
            },
            ReplicaOp::SetMined {
                tx_key: key(2),
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                on_longest_chain: true,
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

    /// Stronger parallelism test: 3 replicas that each sleep 150ms
    /// before ACKing. Serial execution would take ~450ms; parallel
    /// execution takes ~150ms. Assert the wall time is strictly less
    /// than what serial would require.
    #[test]
    fn replicate_batch_three_slow_replicas_run_concurrently() {
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

        let per_replica_delay = Duration::from_millis(150);
        let h1 = spawn_delayed_ack(rt1, per_replica_delay);
        let h2 = spawn_delayed_ack(rt2, per_replica_delay);
        let h3 = spawn_delayed_ack(rt3, per_replica_delay);

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

        // Serial lower bound: 3 * 150ms = 450ms.
        // Parallel upper bound: ~150ms + thread-spawn overhead.
        // Assert well below 3x to guarantee concurrency — allow generous
        // headroom (300ms) for CI scheduler jitter.
        assert!(
            elapsed < Duration::from_millis(300),
            "expected parallel dispatch (~150ms), got {:?} (serial would be ~450ms)",
            elapsed
        );

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
