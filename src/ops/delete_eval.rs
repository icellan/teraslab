//! Event-driven `deleteAtHeight` evaluation.
//!
//! Ported from `teranode.lua` lines 927–1008. Called at the end of spend,
//! unspend, setMined, and setConflicting. Pure function: reads metadata
//! and returns what to change without performing I/O.

use crate::ops::signal::Signal;
use crate::record::{TxFlags, TxMetadata};

/// Patch to apply to metadata after deleteAtHeight evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DahPatch {
    /// New value for `delete_at_height` (0 = clear).
    pub new_delete_at_height: u32,
    /// Whether the `LAST_SPENT_ALL` flag should be set or cleared.
    pub last_spent_all: bool,
}

/// Evaluate whether `delete_at_height` should be set, cleared, or left alone.
///
/// Returns `(signal, optional_patch)`. The caller applies the patch to metadata
/// and updates the DAH secondary index.
///
/// # Logic (from spec §3.13 / Lua lines 927–1008)
///
/// 1. If `block_height_retention == 0` → no signal, no patch
/// 2. If `preserve_until != 0` → no signal, no patch
/// 3. If CONFLICTING → set DAH if not already set
/// 4. All-spent check: `spent_utxos == utxo_count`
/// 5. State transition tracking via LAST_SPENT_ALL flag
/// 6. If all-spent AND has blocks AND on longest chain → set/update DAH
/// 7. If conditions not met AND DAH is set → clear DAH
pub fn evaluate_delete_at_height(
    metadata: &TxMetadata,
    current_block_height: u32,
    block_height_retention: u32,
) -> (Signal, Option<DahPatch>) {
    if block_height_retention == 0 {
        return (Signal::None, None);
    }

    if { metadata.preserve_until } != 0 {
        return (Signal::None, None);
    }

    let existing_dah = { metadata.delete_at_height };
    let new_dah = current_block_height.saturating_add(block_height_retention);
    let is_external = metadata.flags.contains(TxFlags::EXTERNAL);

    // Handle conflicting transactions
    if metadata.flags.contains(TxFlags::CONFLICTING) {
        if existing_dah == 0 {
            let signal = if is_external {
                Signal::DeleteAtHeightSet
            } else {
                Signal::None
            };
            return (
                signal,
                Some(DahPatch {
                    new_delete_at_height: new_dah,
                    last_spent_all: metadata.flags.contains(TxFlags::LAST_SPENT_ALL),
                }),
            );
        }
        return (Signal::None, None);
    }

    let spent_utxos = { metadata.spent_utxos };
    let utxo_count = { metadata.utxo_count };
    let all_spent = spent_utxos == utxo_count;
    let has_blocks = metadata.block_entry_count > 0;
    let on_longest_chain = { metadata.unmined_since } == 0;
    let was_all_spent = metadata.flags.contains(TxFlags::LAST_SPENT_ALL);

    // State transition signaling (non-master records without totalExtraRecs)
    // In TeraSlab there's no pagination, so we always do the master-record
    // logic with the all-spent check.

    if all_spent && has_blocks && on_longest_chain {
        // Set or update DAH
        if existing_dah == 0 || existing_dah < new_dah {
            let signal = if is_external {
                Signal::DeleteAtHeightSet
            } else {
                Signal::None
            };
            let last_all = if !was_all_spent {
                // State transition: not-all-spent → all-spent
                true
            } else {
                was_all_spent
            };
            return (
                signal,
                Some(DahPatch {
                    new_delete_at_height: new_dah,
                    last_spent_all: last_all,
                }),
            );
        }
        // DAH already set and >= new value — check for state transition signal
        if !was_all_spent {
            return (
                Signal::None,
                Some(DahPatch {
                    new_delete_at_height: existing_dah,
                    last_spent_all: true,
                }),
            );
        }
        return (Signal::None, None);
    }

    // Conditions not met — clear DAH if it was set
    if existing_dah != 0 {
        let signal = if is_external {
            Signal::DeleteAtHeightUnset
        } else {
            Signal::None
        };
        return (
            signal,
            Some(DahPatch {
                new_delete_at_height: 0,
                last_spent_all: false,
            }),
        );
    }

    // Check for all-spent state transition (signal only, no DAH change)
    if all_spent != was_all_spent {
        let signal = if all_spent {
            Signal::AllSpent
        } else {
            Signal::NotAllSpent
        };
        return (
            signal,
            Some(DahPatch {
                new_delete_at_height: 0,
                last_spent_all: all_spent,
            }),
        );
    }

    (Signal::None, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{BlockEntry, TxMetadata};

    fn make_meta(utxo_count: u32, spent: u32, flags: TxFlags) -> TxMetadata {
        let mut m = TxMetadata::new(utxo_count);
        m.spent_utxos = spent;
        m.flags = flags;
        m
    }

    fn with_blocks(mut m: TxMetadata) -> TxMetadata {
        m.block_entry_count = 1;
        m.block_entries_inline[0] = BlockEntry {
            block_id: 1,
            block_height: 100,
            subtree_idx: 0,
        };
        m
    }

    #[test]
    fn zero_retention_no_signal() {
        let m = make_meta(10, 10, TxFlags::empty());
        let (sig, patch) = evaluate_delete_at_height(&m, 100, 0);
        assert_eq!(sig, Signal::None);
        assert!(patch.is_none());
    }

    #[test]
    fn preserve_until_blocks_eval() {
        let mut m = make_meta(10, 10, TxFlags::empty());
        m.preserve_until = 500;
        let (sig, patch) = evaluate_delete_at_height(&m, 100, 288);
        assert_eq!(sig, Signal::None);
        assert!(patch.is_none());
    }

    #[test]
    fn conflicting_sets_dah() {
        let m = make_meta(10, 5, TxFlags::CONFLICTING);
        let (sig, patch) = evaluate_delete_at_height(&m, 100, 288);
        assert_eq!(sig, Signal::None); // Not external
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 388);
    }

    #[test]
    fn conflicting_existing_dah_no_change() {
        let mut m = make_meta(10, 5, TxFlags::CONFLICTING);
        m.delete_at_height = 500;
        let (sig, patch) = evaluate_delete_at_height(&m, 100, 288);
        assert_eq!(sig, Signal::None);
        assert!(patch.is_none());
    }

    #[test]
    fn all_spent_with_blocks_on_chain_sets_dah() {
        let m = with_blocks(make_meta(10, 10, TxFlags::empty()));
        // unmined_since = 0 means on longest chain
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288);
        assert_eq!(sig, Signal::None); // Not external
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 1288);
        assert!(p.last_spent_all);
    }

    #[test]
    fn all_spent_no_blocks_no_dah() {
        let m = make_meta(10, 10, TxFlags::empty());
        // No blocks, but all spent
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288);
        // Should signal all-spent transition
        assert_eq!(sig, Signal::AllSpent);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 0);
        assert!(p.last_spent_all);
    }

    #[test]
    fn not_all_spent_no_signal() {
        let m = with_blocks(make_meta(10, 5, TxFlags::empty()));
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288);
        assert_eq!(sig, Signal::None);
        assert!(patch.is_none());
    }

    #[test]
    fn transition_all_to_not_all_spent() {
        let m = with_blocks(make_meta(10, 5, TxFlags::LAST_SPENT_ALL));
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288);
        // Was all-spent, now not → signal + clear DAH
        assert_eq!(sig, Signal::NotAllSpent);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 0);
        assert!(!p.last_spent_all);
    }

    #[test]
    fn external_tx_signals_dah_set() {
        let m = with_blocks(make_meta(10, 10, TxFlags::EXTERNAL));
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288);
        assert_eq!(sig, Signal::DeleteAtHeightSet);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 1288);
    }

    #[test]
    fn external_conflicting_signals_dah_set() {
        let m = make_meta(10, 5, TxFlags::CONFLICTING | TxFlags::EXTERNAL);
        let (sig, _) = evaluate_delete_at_height(&m, 100, 288);
        assert_eq!(sig, Signal::DeleteAtHeightSet);
    }

    #[test]
    fn clear_dah_when_conditions_unmet() {
        let mut m = with_blocks(make_meta(10, 5, TxFlags::LAST_SPENT_ALL));
        m.delete_at_height = 500;
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288);
        // Non-external tx: clearing DAH returns Signal::None (not DAHUNSET)
        // The LAST_SPENT_ALL → not-all-spent transition is captured in the patch
        assert_eq!(sig, Signal::None);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 0);
        assert!(!p.last_spent_all);
    }

    #[test]
    fn external_clear_dah_signals_unset() {
        let mut m = with_blocks(make_meta(10, 5, TxFlags::EXTERNAL | TxFlags::LAST_SPENT_ALL));
        m.delete_at_height = 500;
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288);
        assert_eq!(sig, Signal::DeleteAtHeightUnset);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 0);
    }

    #[test]
    fn unmined_tx_no_dah() {
        let mut m = with_blocks(make_meta(10, 10, TxFlags::empty()));
        m.unmined_since = 500; // Not on longest chain
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288);
        // All spent but not on longest chain → signal all-spent transition only
        assert_eq!(sig, Signal::AllSpent);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 0);
    }
}
