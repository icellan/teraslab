//! Signal codes returned by mutation operations.
//!
//! Signals drive follow-up actions (pruning, external blob management).

/// Signal returned by spend/unspend/setMined operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Signal {
    /// No signal.
    None,
    /// All UTXOs in this record are now spent.
    AllSpent,
    /// Not all UTXOs are spent (state transition from all-spent).
    NotAllSpent,
    /// `delete_at_height` was set on this record.
    DeleteAtHeightSet,
    /// `delete_at_height` was cleared on this record.
    DeleteAtHeightUnset,
    /// `preserve_until` was set on an external record.
    Preserve,
}

impl Signal {
    /// Encode this signal as a single wire byte for batch mutation responses
    /// (e.g. `OP_SET_MINED_BATCH`).
    ///
    /// The byte values form a stable wire contract consumed by the
    /// `BatchItemSuccess.signal` field (see
    /// [`crate::protocol::codec::encode_partial_with_signals`]). `0` is
    /// reserved for [`Signal::None`] (no signal), matching the codec
    /// convention that a zero byte means "no follow-up signal". The remaining
    /// values mirror the Lua UDF's signal strings (`ALLSPENT`, `NOTALLSPENT`,
    /// `DAHSET`, `DAHUNSET`, `PRESERVE`) in declaration order.
    ///
    /// These values must never be renumbered without a wire-protocol version
    /// bump: clients decode them positionally.
    pub fn to_wire(&self) -> u8 {
        match self {
            Signal::None => 0,
            Signal::AllSpent => 1,
            Signal::NotAllSpent => 2,
            Signal::DeleteAtHeightSet => 3,
            Signal::DeleteAtHeightUnset => 4,
            Signal::Preserve => 5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Signal;

    #[test]
    fn to_wire_is_stable_and_distinct() {
        // Pin the exact byte values — they are a wire contract.
        assert_eq!(Signal::None.to_wire(), 0);
        assert_eq!(Signal::AllSpent.to_wire(), 1);
        assert_eq!(Signal::NotAllSpent.to_wire(), 2);
        assert_eq!(Signal::DeleteAtHeightSet.to_wire(), 3);
        assert_eq!(Signal::DeleteAtHeightUnset.to_wire(), 4);
        assert_eq!(Signal::Preserve.to_wire(), 5);

        // All distinct — no two variants collide on the wire.
        let all = [
            Signal::None.to_wire(),
            Signal::AllSpent.to_wire(),
            Signal::NotAllSpent.to_wire(),
            Signal::DeleteAtHeightSet.to_wire(),
            Signal::DeleteAtHeightUnset.to_wire(),
            Signal::Preserve.to_wire(),
        ];
        let mut sorted = all.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), all.len(), "wire signal bytes must be distinct");
    }
}
