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
