//! Replication manager — orchestrates sending to multiple replicas
//! with configurable acknowledgment policies.

use crate::replication::protocol::*;
use std::time::Duration;
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
}

impl ReplicationManager {
    /// Create a new manager with the given configuration and replica transports.
    pub fn new(
        config: ReplicationConfig,
        transports: Vec<Box<dyn ReplicaTransport>>,
    ) -> Self {
        let senders = transports
            .into_iter()
            .map(ReplicaSender::new)
            .collect();
        Self {
            senders,
            config,
            next_sequence: 1,
        }
    }

    /// Create a manager with sequence state recovered from the redo log.
    ///
    /// `initial_sequence` should be `redo_log.current_sequence()` so that
    /// replication sequence numbers are contiguous with the durable log.
    pub fn with_initial_sequence(
        config: ReplicationConfig,
        transports: Vec<Box<dyn ReplicaTransport>>,
        initial_sequence: u64,
    ) -> Self {
        let senders = transports
            .into_iter()
            .map(ReplicaSender::new)
            .collect();
        Self {
            senders,
            config,
            next_sequence: initial_sequence.max(1),
        }
    }

    /// Replicate a batch of operations to all live replicas.
    ///
    /// Sends to all live replicas in parallel (via threads), then waits
    /// for ACKs according to the configured ack policy.
    pub fn replicate_batch(
        &mut self,
        ops: &[ReplicaOp],
    ) -> std::result::Result<(), ReplicationError> {
        if ops.is_empty() {
            return Ok(());
        }

        let required = self.required_ack_count();
        let live_count = self.senders.iter().filter(|s| *s.state() == ReplicaState::Live).count();

        if live_count < required {
            return Err(ReplicationError::InsufficientReplicas {
                available: live_count,
                required,
            });
        }

        let batch = ReplicaBatch {
            first_sequence: self.next_sequence,
            ops: ops.to_vec(),
        };
        self.next_sequence += ops.len() as u64;

        let timeout = self.config.replication_timeout;
        let mut successes = 0;
        let mut first_error: Option<ReplicationError> = None;

        for sender in &mut self.senders {
            if *sender.state() != ReplicaState::Live {
                continue;
            }

            match sender.transport.send_batch(&batch) {
                Ok(()) => {
                    match sender.transport.recv_ack(timeout) {
                        Ok(ReplicaAck::Ok { through_sequence }) => {
                            sender.last_acked = through_sequence;
                            successes += 1;
                        }
                        Ok(ReplicaAck::Error { failed_sequence, message }) => {
                            sender.state = ReplicaState::Down;
                            if first_error.is_none() {
                                first_error = Some(ReplicationError::ReplicaError {
                                    sequence: failed_sequence,
                                    message,
                                });
                            }
                        }
                        Err(e) => {
                            sender.state = ReplicaState::Down;
                            if first_error.is_none() {
                                first_error = Some(e);
                            }
                        }
                    }
                }
                Err(e) => {
                    sender.state = ReplicaState::Down;
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        match self.config.ack_policy {
            AckPolicy::WriteAll => {
                if successes == live_count {
                    Ok(())
                } else {
                    Err(first_error.unwrap_or(ReplicationError::InsufficientReplicas {
                        available: successes,
                        required: live_count,
                    }))
                }
            }
            AckPolicy::WriteMajority => {
                if successes >= required {
                    Ok(())
                } else {
                    Err(first_error.unwrap_or(ReplicationError::InsufficientReplicas {
                        available: successes,
                        required,
                    }))
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
        self.senders.iter().filter(|s| *s.state() == ReplicaState::Live).count()
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
    pub fn recv_batch(&self, timeout: Duration) -> std::result::Result<ReplicaBatch, ReplicationError> {
        let data = self.rx.recv_timeout(timeout)
            .map_err(|_| ReplicationError::Timeout(timeout))?;
        ReplicaBatch::deserialize(&data)
            .map_err(|e| ReplicationError::Transport(format!("{e}")))
    }

    /// Send an ack (replica side).
    pub fn send_ack(&self, ack: &ReplicaAck) -> std::result::Result<(), ReplicationError> {
        self.tx.send(ack.serialize())
            .map_err(|e| ReplicationError::Transport(format!("{e}")))
    }
}

impl ReplicaTransport for InMemoryTransport {
    fn send_batch(&mut self, batch: &ReplicaBatch) -> std::result::Result<(), ReplicationError> {
        self.tx.send(batch.serialize())
            .map_err(|e| ReplicationError::Transport(format!("{e}")))
    }

    fn recv_ack(&mut self, timeout: Duration) -> std::result::Result<ReplicaAck, ReplicationError> {
        let data = self.rx.recv_timeout(timeout)
            .map_err(|_| ReplicationError::Timeout(timeout))?;
        ReplicaAck::deserialize(&data)
            .map_err(|e| ReplicationError::Transport(format!("{e}")))
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
        let mut txid = [0u8; 32]; txid[0] = n; TxKey { txid }
    }

    /// Simulate a replica that auto-ACKs every batch it receives.
    fn spawn_auto_ack_replica(
        replica_transport: InMemoryTransport,
    ) -> std::thread::JoinHandle<Vec<ReplicaBatch>> {
        std::thread::spawn(move || {
            let mut received = Vec::new();
            while let Ok(batch) = replica_transport.recv_batch(Duration::from_secs(1)) {
                let ack = ReplicaAck::Ok { through_sequence: batch.last_sequence() };
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
            ReplicationConfig { ack_policy: AckPolicy::WriteAll, ..Default::default() },
            vec![Box::new(master_t)],
        );

        let ops = vec![ReplicaOp::Spend { tx_key: key(1), offset: 0, spending_data: [0xAB; 36], master_generation: 0 }];
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

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(master_t)],
        );

        let ops: Vec<ReplicaOp> = (0..50u8)
            .map(|i| ReplicaOp::Spend { tx_key: key(i), offset: i as u32, spending_data: [i; 36], master_generation: 0 })
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
            ReplicationConfig { ack_policy: AckPolicy::WriteAll, ..Default::default() },
            vec![Box::new(mt1), Box::new(mt2)],
        );

        let ops = vec![ReplicaOp::Freeze { tx_key: key(1), offset: 0, master_generation: 0 }];
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

        let ops = vec![ReplicaOp::Freeze { tx_key: key(1), offset: 0, master_generation: 0 }];
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
        let ops = vec![ReplicaOp::Freeze { tx_key: key(1), offset: 0, master_generation: 0 }];
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

        let ops = vec![ReplicaOp::Freeze { tx_key: key(1), offset: 0, master_generation: 0 }];
        assert!(mgr.replicate_batch(&ops).is_err());
    }

    #[test]
    fn sequence_numbers_contiguous() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

        mgr.replicate_batch(&[ReplicaOp::Freeze { tx_key: key(1), offset: 0, master_generation: 0 }]).unwrap();
        mgr.replicate_batch(&[
            ReplicaOp::Freeze { tx_key: key(2), offset: 1, master_generation: 0 },
            ReplicaOp::Freeze { tx_key: key(3), offset: 2, master_generation: 0 },
        ]).unwrap();

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

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

        let ops = vec![
            ReplicaOp::Spend { tx_key: key(1), offset: 0, spending_data: [0x11; 36], master_generation: 0 },
            ReplicaOp::SetMined { tx_key: key(2), block_id: 1, block_height: 100, subtree_idx: 0, on_longest_chain: true, master_generation: 0 },
            ReplicaOp::PruneSlot { tx_key: key(3), offset: 5 },
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

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

        mgr.replicate_batch(&[]).unwrap();
    }

    #[test]
    fn required_ack_count_calculations() {
        // RF=2 (1 replica), WriteAll: need 1 ACK
        let mgr = ReplicationManager::new(
            ReplicationConfig { ack_policy: AckPolicy::WriteAll, ..Default::default() },
            vec![Box::new(InMemoryTransport::pair().0)],
        );
        assert_eq!(mgr.required_ack_count(), 1);

        // RF=3 (2 replicas), WriteAll: need 2 ACKs
        let mgr = ReplicationManager::new(
            ReplicationConfig { ack_policy: AckPolicy::WriteAll, ..Default::default() },
            vec![Box::new(InMemoryTransport::pair().0), Box::new(InMemoryTransport::pair().0)],
        );
        assert_eq!(mgr.required_ack_count(), 2);

        // RF=3, WriteMajority: need 1 ACK (master + 1 = majority of 3)
        let mgr = ReplicationManager::new(
            ReplicationConfig { ack_policy: AckPolicy::WriteMajority, ..Default::default() },
            vec![Box::new(InMemoryTransport::pair().0), Box::new(InMemoryTransport::pair().0)],
        );
        assert_eq!(mgr.required_ack_count(), 1);

        // RF=2, WriteMajority: need 1 ACK (master + 1 = majority of 2)
        let mgr = ReplicationManager::new(
            ReplicationConfig { ack_policy: AckPolicy::WriteMajority, ..Default::default() },
            vec![Box::new(InMemoryTransport::pair().0)],
        );
        assert_eq!(mgr.required_ack_count(), 1);
    }

    // -- Single-operation replication for each op type --

    #[test]
    fn create_op_replicated() {
        let (mt, rt) = InMemoryTransport::pair();
        let handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

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

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

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

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

        let ops = vec![
            ReplicaOp::Freeze { tx_key: key(1), offset: 3, master_generation: 0 },
            ReplicaOp::Unfreeze { tx_key: key(1), offset: 3, master_generation: 0 },
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

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

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

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

        let ops = vec![
            ReplicaOp::SetConflicting { tx_key: key(1), value: true, current_block_height: 1000, retention: 288, master_generation: 0 },
            ReplicaOp::SetLocked { tx_key: key(2), value: true, master_generation: 0 },
            ReplicaOp::PreserveUntil { tx_key: key(3), block_height: 5000, master_generation: 0 },
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

            let ack = ReplicaAck::Ok { through_sequence: expected_through };
            rt.send_ack(&ack).unwrap();
            expected_through
        });

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

        let ops: Vec<ReplicaOp> = (0..10u8)
            .map(|i| ReplicaOp::Freeze { tx_key: key(i), offset: i as u32, master_generation: 0 })
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

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

        let ops = vec![ReplicaOp::Spend {
            tx_key: key(1), offset: 0, spending_data: [0xAB; 36], master_generation: 0,
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
        let ops = vec![ReplicaOp::Freeze { tx_key: key(1), offset: 0, master_generation: 0 }];
        mgr.replicate_batch(&ops).unwrap();
    }

    #[test]
    fn catch_up_transitions_to_live() {
        let (mt, rt) = InMemoryTransport::pair();
        let _handle = spawn_auto_ack_replica(rt);

        let mut mgr = ReplicationManager::new(
            ReplicationConfig::default(),
            vec![Box::new(mt)],
        );

        // Send 5 ops normally
        for i in 0..5 {
            let ops = vec![ReplicaOp::Freeze { tx_key: key(i), offset: 0, master_generation: 0 }];
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
        assert!(matches!(mgr.sender(0).state(), ReplicaState::CatchingUp { from_sequence: 6 }));

        // Run catch-up with mock ops
        mgr.run_catchup(|from_seq| {
            (from_seq..11)
                .map(|i| ReplicaOp::Freeze { tx_key: key(i as u8), offset: 0, master_generation: 0 })
                .collect()
        }).unwrap();

        assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);
    }

    #[test]
    fn catchup_transitions_to_needs_resync_when_redo_reclaimed() {
        let (master_tx, _replica_rx) = InMemoryTransport::pair();
        let (_tx2, replica_side) = InMemoryTransport::pair();

        let config = ReplicationConfig::default();
        let mut mgr = ReplicationManager::new(config, vec![Box::new(master_tx)]);

        // Replicate some ops to advance the sequence
        let ops: Vec<ReplicaOp> = (0..5)
            .map(|i| ReplicaOp::Freeze { tx_key: key(i), offset: 0, master_generation: 0 })
            .collect();

        // Manually set up the scenario: replica is catching up
        mgr.senders[0].state = ReplicaState::CatchingUp { from_sequence: 1 };
        mgr.next_sequence = 10;

        // Run catch-up with an empty result (simulating reclaimed redo)
        mgr.run_catchup(|_from_seq| Vec::new()).unwrap();

        // Should transition to NeedsResync, not stay in CatchingUp
        assert_eq!(*mgr.sender(0).state(), ReplicaState::NeedsResync);
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
        assert_eq!(received.len(), 3, "5 ops at batch_size=2 should produce 3 batches");

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
}
