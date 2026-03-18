//! Batch accumulator for replication operations.
//!
//! Collects ReplicaOps during a batch of client mutations, then flushes
//! them to all replicas as a single frame.

use crate::replication::protocol::ReplicaOp;

/// Accumulates ReplicaOps for batch replication.
///
/// Operations are added one at a time during a client mutation batch.
/// Once the batch is complete, `drain()` returns all accumulated ops
/// for sending as a single `ReplicaBatch` frame.
pub struct ReplicaBatchAccumulator {
    ops: Vec<ReplicaOp>,
    max_batch_size: usize,
}

impl ReplicaBatchAccumulator {
    /// Create a new accumulator with the given flush threshold.
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            ops: Vec::with_capacity(max_batch_size),
            max_batch_size,
        }
    }

    /// Add an op to the current batch.
    pub fn push(&mut self, op: ReplicaOp) {
        self.ops.push(op);
    }

    /// Take all accumulated ops, clearing the accumulator.
    pub fn drain(&mut self) -> Vec<ReplicaOp> {
        std::mem::take(&mut self.ops)
    }

    /// Number of accumulated ops.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether the accumulator is empty.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Whether the batch has reached the flush threshold.
    pub fn should_flush(&self) -> bool {
        self.ops.len() >= self.max_batch_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::TxKey;

    fn key(n: u8) -> TxKey {
        let mut txid = [0u8; 32]; txid[0] = n; TxKey { txid }
    }

    #[test]
    fn push_and_drain() {
        let mut acc = ReplicaBatchAccumulator::new(100);
        assert!(acc.is_empty());

        acc.push(ReplicaOp::Freeze { tx_key: key(1), offset: 0 });
        acc.push(ReplicaOp::Freeze { tx_key: key(2), offset: 1 });
        assert_eq!(acc.len(), 2);
        assert!(!acc.is_empty());

        let ops = acc.drain();
        assert_eq!(ops.len(), 2);
        assert!(acc.is_empty());
        assert_eq!(acc.len(), 0);
    }

    #[test]
    fn should_flush_at_threshold() {
        let mut acc = ReplicaBatchAccumulator::new(3);
        acc.push(ReplicaOp::Freeze { tx_key: key(1), offset: 0 });
        acc.push(ReplicaOp::Freeze { tx_key: key(2), offset: 1 });
        assert!(!acc.should_flush());

        acc.push(ReplicaOp::Freeze { tx_key: key(3), offset: 2 });
        assert!(acc.should_flush());
    }

    #[test]
    fn drain_clears_for_reuse() {
        let mut acc = ReplicaBatchAccumulator::new(100);
        acc.push(ReplicaOp::Delete { tx_key: key(1) });
        acc.drain();
        acc.push(ReplicaOp::Delete { tx_key: key(2) });
        let ops = acc.drain();
        assert_eq!(ops.len(), 1);
    }
}
