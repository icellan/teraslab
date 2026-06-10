//! Batch accumulator for replication operations.
//!
//! Collects ReplicaOps during a batch of client mutations, then flushes
//! them to all replicas as a single frame.

use crate::replication::protocol::ReplicaOp;

/// Error returned by [`ReplicaBatchAccumulator::push`] when the
/// caller has accumulated more than 2× the soft `max_batch_size`
/// without draining. The accumulator is in a consistent state on
/// return (the op was NOT added).
#[derive(thiserror::Error, Debug)]
#[error("replica batch accumulator full: {len} ops accumulated, hard cap is {hard_cap}")]
pub struct AccumulatorOverflow {
    pub len: usize,
    pub hard_cap: usize,
}

/// Accumulates ReplicaOps for batch replication.
///
/// Operations are added one at a time during a client mutation batch.
/// Once the batch is complete, `drain()` returns all accumulated ops
/// for sending as a single `ReplicaBatch` frame.
///
/// F-G7-010: `push` returns `Ok(true)` when the accumulator has hit
/// the soft `max_batch_size` flush hint and the caller SHOULD drain
/// now. Pushes past `2 * max_batch_size` return `AccumulatorOverflow`
/// without modifying state — the hard cap protects against caller
/// bugs that forget to drain and would otherwise let the underlying
/// `Vec` grow unbounded.
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
    ///
    /// Returns `Ok(true)` if the new length crossed the soft
    /// `max_batch_size` threshold (caller should drain). Returns
    /// `Ok(false)` otherwise. Returns `Err(AccumulatorOverflow)` and
    /// leaves the accumulator unchanged when the hard cap
    /// (`2 * max_batch_size`) would be exceeded.
    pub fn push(&mut self, op: ReplicaOp) -> Result<bool, AccumulatorOverflow> {
        let hard_cap = self.max_batch_size.saturating_mul(2);
        if self.ops.len() >= hard_cap {
            return Err(AccumulatorOverflow {
                len: self.ops.len(),
                hard_cap,
            });
        }
        self.ops.push(op);
        Ok(self.ops.len() >= self.max_batch_size)
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
        let mut txid = [0u8; 32];
        txid[0] = n;
        TxKey { txid }
    }

    #[test]
    fn push_and_drain() {
        let mut acc = ReplicaBatchAccumulator::new(100);
        assert!(acc.is_empty());

        let _ = acc.push(ReplicaOp::Freeze {
            tx_key: key(1),
            offset: 0,
            master_generation: 0,
        });
        let _ = acc.push(ReplicaOp::Freeze {
            tx_key: key(2),
            offset: 1,
            master_generation: 0,
        });
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
        // First two pushes stay below the soft threshold.
        assert!(
            !acc.push(ReplicaOp::Freeze {
                tx_key: key(1),
                offset: 0,
                master_generation: 0,
            })
            .unwrap()
        );
        assert!(
            !acc.push(ReplicaOp::Freeze {
                tx_key: key(2),
                offset: 1,
                master_generation: 0,
            })
            .unwrap()
        );
        assert!(!acc.should_flush());

        // The push that reaches the soft threshold returns true.
        assert!(
            acc.push(ReplicaOp::Freeze {
                tx_key: key(3),
                offset: 2,
                master_generation: 0,
            })
            .unwrap()
        );
        assert!(acc.should_flush());
    }

    #[test]
    fn drain_clears_for_reuse() {
        let mut acc = ReplicaBatchAccumulator::new(100);
        let _ = acc.push(ReplicaOp::Delete { tx_key: key(1) });
        acc.drain();
        let _ = acc.push(ReplicaOp::Delete { tx_key: key(2) });
        let ops = acc.drain();
        assert_eq!(ops.len(), 1);
    }

    /// F-G7-010: pushing past `2 * max_batch_size` without draining
    /// must fail with `AccumulatorOverflow` and leave the accumulator
    /// unchanged.
    #[test]
    fn push_past_hard_cap_returns_overflow() {
        let mut acc = ReplicaBatchAccumulator::new(2);
        // Soft threshold = 2, hard cap = 4. Push 4 successfully.
        for i in 0..4u8 {
            acc.push(ReplicaOp::Delete { tx_key: key(i) }).unwrap();
        }
        assert_eq!(acc.len(), 4);

        let err = acc
            .push(ReplicaOp::Delete { tx_key: key(5) })
            .expect_err("must reject pushes past 2x threshold");
        assert_eq!(err.len, 4);
        assert_eq!(err.hard_cap, 4);
        // State unchanged after the rejected push.
        assert_eq!(acc.len(), 4);
    }
}
