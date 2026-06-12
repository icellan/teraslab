//! Event-driven `deleteAtHeight` evaluation.
//!
//! Ported from `teranode.lua` lines 927–1008. Called at the end of spend,
//! unspend, setMined, and setConflicting. Pure function: reads metadata
//! and returns what to change without performing I/O.

use crate::ops::error::SpendError;
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

/// Result of a `deleteAtHeight` evaluation: the signal to emit (if any)
/// and an optional metadata patch to apply.
pub type DahEvalResult = Result<(Signal, Option<DahPatch>), SpendError>;

/// Compute `current_block_height + block_height_retention` with overflow
/// detection.
///
/// Returns [`SpendError::DahOverflow`] if the addition would wrap past
/// `u32::MAX`. Using `saturating_add` here would silently pin UTXOs as
/// unprunable (because the clamped `u32::MAX` value is far above any real
/// future block height), causing permanent store bloat with no error signal.
fn checked_new_dah(
    current_block_height: u32,
    block_height_retention: u32,
) -> Result<u32, SpendError> {
    current_block_height
        .checked_add(block_height_retention)
        .ok_or(SpendError::DahOverflow {
            current_height: current_block_height,
            retention: block_height_retention,
        })
}

/// Evaluate whether `delete_at_height` should be set, cleared, or left alone.
///
/// Returns `Ok((signal, optional_patch))`. The caller applies the patch to
/// metadata and updates the DAH secondary index.
///
/// # Errors
///
/// Returns [`SpendError::DahOverflow`] if `current_block_height +
/// block_height_retention` would overflow `u32`. Never silently clamps,
/// because a saturating-clamped DAH pins UTXOs as unprunable and causes
/// permanent store bloat. Config validation bounds `block_height_retention`
/// well below the overflow threshold, so this only fires on misconfiguration.
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
///
/// Unmined transactions intentionally do not get `delete_at_height` here:
/// `metadata.unmined_since != 0` means the transaction is not on the longest
/// chain, so pruning is driven by the unmined secondary index rather than the
/// DAH index. This preserves data needed for reorg handling until a separate
/// unmined-retention policy decides it is safe to delete.
pub fn evaluate_delete_at_height(
    metadata: &TxMetadata,
    current_block_height: u32,
    block_height_retention: u32,
) -> DahEvalResult {
    if block_height_retention == 0 {
        return Ok((Signal::None, None));
    }

    if { metadata.preserve_until } != 0 {
        return Ok((Signal::None, None));
    }

    let existing_dah = { metadata.delete_at_height };
    let new_dah = checked_new_dah(current_block_height, block_height_retention)?;
    let is_external = metadata.flags.contains(TxFlags::EXTERNAL);

    // Handle conflicting transactions
    if metadata.flags.contains(TxFlags::CONFLICTING) {
        if existing_dah == 0 {
            let signal = if is_external {
                Signal::DeleteAtHeightSet
            } else {
                Signal::None
            };
            return Ok((
                signal,
                Some(DahPatch {
                    new_delete_at_height: new_dah,
                    last_spent_all: metadata.flags.contains(TxFlags::LAST_SPENT_ALL),
                }),
            ));
        }
        return Ok((Signal::None, None));
    }

    let spent_utxos = { metadata.spent_utxos };
    let utxo_count = { metadata.utxo_count };
    // LP-3: a reassigned record is never treated as all-spent, mirroring the
    // Lua reference's `recordUtxos + 1` (teranode.lua:945) which permanently
    // keeps the all-spent check false so the court-ordered reassignment audit
    // trail is retained forever. The CONFLICTING branch above is unaffected
    // (the Lua `+1` only touches the all-spent computation), so a reassigned
    // record later marked conflicting still gets DAH'd.
    let all_spent = spent_utxos == utxo_count && !metadata.flags.contains(TxFlags::REASSIGNED);
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
            return Ok((
                signal,
                Some(DahPatch {
                    new_delete_at_height: new_dah,
                    last_spent_all: last_all,
                }),
            ));
        }
        // DAH already set and >= new value — check for state transition signal
        if !was_all_spent {
            return Ok((
                Signal::None,
                Some(DahPatch {
                    new_delete_at_height: existing_dah,
                    last_spent_all: true,
                }),
            ));
        }
        return Ok((Signal::None, None));
    }

    // Conditions not met — clear DAH if it was set
    if existing_dah != 0 {
        let signal = if is_external {
            Signal::DeleteAtHeightUnset
        } else {
            Signal::None
        };
        return Ok((
            signal,
            Some(DahPatch {
                new_delete_at_height: 0,
                last_spent_all: false,
            }),
        ));
    }

    // Check for all-spent state transition (signal only, no DAH change)
    if all_spent != was_all_spent {
        let signal = if all_spent {
            Signal::AllSpent
        } else {
            Signal::NotAllSpent
        };
        return Ok((
            signal,
            Some(DahPatch {
                new_delete_at_height: 0,
                last_spent_all: all_spent,
            }),
        ));
    }

    Ok((Signal::None, None))
}

/// Evaluate `deleteAtHeight` from cached index fields — no metadata read needed.
///
/// Same logic as [`evaluate_delete_at_height`] but takes individual cached values
/// from `TxIndexEntry` instead of a `&TxMetadata` reference.
///
/// The `has_preserve_until` flag indicates whether `dah_or_preserve` holds
/// `preserve_until` (true) or `delete_at_height` (false).
///
/// # Errors
///
/// Returns [`SpendError::DahOverflow`] if `current_block_height +
/// block_height_retention` would overflow `u32`. See
/// [`evaluate_delete_at_height`] for rationale.
#[allow(clippy::too_many_arguments)]
pub fn evaluate_dah_cached(
    tx_flags: TxFlags,
    spent_utxos: u32,
    utxo_count: u32,
    block_entry_count: u8,
    unmined_since: u32,
    has_preserve_until: bool,
    dah_or_preserve: u32,
    current_block_height: u32,
    block_height_retention: u32,
) -> DahEvalResult {
    if block_height_retention == 0 {
        return Ok((Signal::None, None));
    }

    if has_preserve_until {
        return Ok((Signal::None, None));
    }

    let existing_dah = dah_or_preserve; // it's delete_at_height when !has_preserve_until
    let new_dah = checked_new_dah(current_block_height, block_height_retention)?;
    let is_external = tx_flags.contains(TxFlags::EXTERNAL);

    // Handle conflicting transactions
    if tx_flags.contains(TxFlags::CONFLICTING) {
        if existing_dah == 0 {
            let signal = if is_external {
                Signal::DeleteAtHeightSet
            } else {
                Signal::None
            };
            return Ok((
                signal,
                Some(DahPatch {
                    new_delete_at_height: new_dah,
                    last_spent_all: tx_flags.contains(TxFlags::LAST_SPENT_ALL),
                }),
            ));
        }
        return Ok((Signal::None, None));
    }

    // LP-3: reassigned records are never all-spent (see
    // `evaluate_delete_at_height`). The discriminant rides in `tx_flags`,
    // synced from metadata by `sync_index_cache`.
    let all_spent = spent_utxos == utxo_count && !tx_flags.contains(TxFlags::REASSIGNED);
    let has_blocks = block_entry_count > 0;
    let on_longest_chain = unmined_since == 0;
    let was_all_spent = tx_flags.contains(TxFlags::LAST_SPENT_ALL);

    if all_spent && has_blocks && on_longest_chain {
        if existing_dah == 0 || existing_dah < new_dah {
            let signal = if is_external {
                Signal::DeleteAtHeightSet
            } else {
                Signal::None
            };
            let last_all = if !was_all_spent { true } else { was_all_spent };
            return Ok((
                signal,
                Some(DahPatch {
                    new_delete_at_height: new_dah,
                    last_spent_all: last_all,
                }),
            ));
        }
        if !was_all_spent {
            return Ok((
                Signal::None,
                Some(DahPatch {
                    new_delete_at_height: existing_dah,
                    last_spent_all: true,
                }),
            ));
        }
        return Ok((Signal::None, None));
    }

    if existing_dah != 0 {
        let signal = if is_external {
            Signal::DeleteAtHeightUnset
        } else {
            Signal::None
        };
        return Ok((
            signal,
            Some(DahPatch {
                new_delete_at_height: 0,
                last_spent_all: false,
            }),
        ));
    }

    if all_spent != was_all_spent {
        let signal = if all_spent {
            Signal::AllSpent
        } else {
            Signal::NotAllSpent
        };
        return Ok((
            signal,
            Some(DahPatch {
                new_delete_at_height: 0,
                last_spent_all: all_spent,
            }),
        ));
    }

    Ok((Signal::None, None))
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
        let (sig, patch) = evaluate_delete_at_height(&m, 100, 0).expect("no overflow");
        assert_eq!(sig, Signal::None);
        assert!(patch.is_none());
    }

    #[test]
    fn preserve_until_blocks_eval() {
        let mut m = make_meta(10, 10, TxFlags::empty());
        m.preserve_until = 500;
        let (sig, patch) = evaluate_delete_at_height(&m, 100, 288).expect("no overflow");
        assert_eq!(sig, Signal::None);
        assert!(patch.is_none());
    }

    #[test]
    fn conflicting_sets_dah() {
        let m = make_meta(10, 5, TxFlags::CONFLICTING);
        let (sig, patch) = evaluate_delete_at_height(&m, 100, 288).expect("no overflow");
        assert_eq!(sig, Signal::None); // Not external
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 388);
    }

    #[test]
    fn conflicting_existing_dah_no_change() {
        let mut m = make_meta(10, 5, TxFlags::CONFLICTING);
        m.delete_at_height = 500;
        let (sig, patch) = evaluate_delete_at_height(&m, 100, 288).expect("no overflow");
        assert_eq!(sig, Signal::None);
        assert!(patch.is_none());
    }

    #[test]
    fn all_spent_with_blocks_on_chain_sets_dah() {
        let m = with_blocks(make_meta(10, 10, TxFlags::empty()));
        // unmined_since = 0 means on longest chain
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288).expect("no overflow");
        assert_eq!(sig, Signal::None); // Not external
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 1288);
        assert!(p.last_spent_all);
    }

    #[test]
    fn all_spent_no_blocks_no_dah() {
        let m = make_meta(10, 10, TxFlags::empty());
        // No blocks, but all spent
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288).expect("no overflow");
        // Should signal all-spent transition
        assert_eq!(sig, Signal::AllSpent);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 0);
        assert!(p.last_spent_all);
    }

    #[test]
    fn not_all_spent_no_signal() {
        let m = with_blocks(make_meta(10, 5, TxFlags::empty()));
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288).expect("no overflow");
        assert_eq!(sig, Signal::None);
        assert!(patch.is_none());
    }

    #[test]
    fn transition_all_to_not_all_spent() {
        let m = with_blocks(make_meta(10, 5, TxFlags::LAST_SPENT_ALL));
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288).expect("no overflow");
        // Was all-spent, now not -> signal + clear DAH
        assert_eq!(sig, Signal::NotAllSpent);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 0);
        assert!(!p.last_spent_all);
    }

    #[test]
    fn external_tx_signals_dah_set() {
        let m = with_blocks(make_meta(10, 10, TxFlags::EXTERNAL));
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288).expect("no overflow");
        assert_eq!(sig, Signal::DeleteAtHeightSet);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 1288);
    }

    #[test]
    fn external_conflicting_signals_dah_set() {
        let m = make_meta(10, 5, TxFlags::CONFLICTING | TxFlags::EXTERNAL);
        let (sig, _) = evaluate_delete_at_height(&m, 100, 288).expect("no overflow");
        assert_eq!(sig, Signal::DeleteAtHeightSet);
    }

    #[test]
    fn clear_dah_when_conditions_unmet() {
        let mut m = with_blocks(make_meta(10, 5, TxFlags::LAST_SPENT_ALL));
        m.delete_at_height = 500;
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288).expect("no overflow");
        // Non-external tx: clearing DAH returns Signal::None (not DAHUNSET)
        // The LAST_SPENT_ALL -> not-all-spent transition is captured in the patch
        assert_eq!(sig, Signal::None);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 0);
        assert!(!p.last_spent_all);
    }

    #[test]
    fn external_clear_dah_signals_unset() {
        let mut m = with_blocks(make_meta(
            10,
            5,
            TxFlags::EXTERNAL | TxFlags::LAST_SPENT_ALL,
        ));
        m.delete_at_height = 500;
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288).expect("no overflow");
        assert_eq!(sig, Signal::DeleteAtHeightUnset);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 0);
    }

    #[test]
    fn lp3_reassigned_record_not_all_spent_no_dah() {
        // All slots SPENT, mined, on longest chain — but REASSIGNED set, so
        // the all-spent check is forced false and no DAH is set (LP-3 /
        // Lua recordUtxos+1).
        let m = with_blocks(make_meta(10, 10, TxFlags::REASSIGNED));
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288).expect("no overflow");
        // Not all-spent (reassigned) and no DAH was set → no signal, no patch.
        assert_eq!(sig, Signal::None);
        assert!(
            patch.is_none(),
            "reassigned record must not be DAH'd (LP-3); got {patch:?}"
        );
    }

    #[test]
    fn lp3_reassigned_conflicting_still_dahd() {
        // The Lua `recordUtxos + 1` only affects the all-spent computation;
        // a reassigned record later marked conflicting still gets DAH'd.
        let m = make_meta(10, 5, TxFlags::REASSIGNED | TxFlags::CONFLICTING);
        let (_sig, patch) = evaluate_delete_at_height(&m, 100, 288).expect("no overflow");
        let p = patch.expect("conflicting reassigned record must still be DAH'd");
        assert_eq!(p.new_delete_at_height, 388);
    }

    #[test]
    fn lp3_reassigned_cached_path_not_all_spent() {
        // Same exclusion on the cached-fields fast path.
        let (sig, patch) =
            evaluate_dah_cached(TxFlags::REASSIGNED, 10, 10, 1, 0, false, 0, 1000, 288)
                .expect("no overflow");
        assert_eq!(sig, Signal::None);
        assert!(patch.is_none(), "cached path must also exclude reassigned");
    }

    #[test]
    fn unmined_tx_no_dah() {
        let mut m = with_blocks(make_meta(10, 10, TxFlags::empty()));
        m.unmined_since = 500; // Not on longest chain
        let (sig, patch) = evaluate_delete_at_height(&m, 1000, 288).expect("no overflow");
        // All spent but not on longest chain -> signal all-spent transition only
        assert_eq!(sig, Signal::AllSpent);
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 0);
    }

    // ----- Overflow tests (C8 regression guard) -----

    #[test]
    fn dah_overflow_errors_instead_of_saturating() {
        // u32::MAX - 5 + 10 would wrap to 4. saturating_add would clamp to
        // u32::MAX and pin the UTXO as unprunable. checked_add must error.
        let m = with_blocks(make_meta(10, 10, TxFlags::empty()));
        let err = evaluate_delete_at_height(&m, u32::MAX - 5, 10).unwrap_err();
        match err {
            SpendError::DahOverflow {
                current_height,
                retention,
            } => {
                assert_eq!(current_height, u32::MAX - 5);
                assert_eq!(retention, 10);
            }
            other => panic!("expected DahOverflow, got {other:?}"),
        }
    }

    #[test]
    fn dah_normal_height_returns_correct_sum() {
        // Sanity guard: for realistic BSV heights, checked_add matches
        // the documented behavior exactly.
        let m = with_blocks(make_meta(10, 10, TxFlags::empty()));
        let (_sig, patch) =
            evaluate_delete_at_height(&m, 800_000, 1000).expect("no overflow at normal heights");
        let p = patch.expect("all-spent on-chain tx with blocks produces patch");
        assert_eq!(p.new_delete_at_height, 801_000);
    }

    #[test]
    fn dah_overflow_boundary_exact_u32_max() {
        // current + retention == u32::MAX exactly is the last legal value.
        let m = with_blocks(make_meta(10, 10, TxFlags::empty()));
        let (_sig, patch) =
            evaluate_delete_at_height(&m, u32::MAX - 1000, 1000).expect("equal to u32::MAX is OK");
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, u32::MAX);
    }

    #[test]
    fn dah_overflow_one_past_boundary_errors() {
        // current + retention == u32::MAX + 1 must error.
        let m = with_blocks(make_meta(10, 10, TxFlags::empty()));
        let err = evaluate_delete_at_height(&m, u32::MAX - 1000, 1001).unwrap_err();
        assert!(matches!(err, SpendError::DahOverflow { .. }));
    }

    #[test]
    fn dah_cached_overflow_errors() {
        // Cached-fields path must also enforce overflow detection.
        let err = evaluate_dah_cached(TxFlags::empty(), 10, 10, 1, 0, false, 0, u32::MAX - 5, 10)
            .unwrap_err();
        assert!(matches!(err, SpendError::DahOverflow { .. }));
    }

    #[test]
    fn dah_cached_normal_height_ok() {
        let (_sig, patch) =
            evaluate_dah_cached(TxFlags::empty(), 10, 10, 1, 0, false, 0, 800_000, 1000)
                .expect("no overflow at normal heights");
        let p = patch.unwrap();
        assert_eq!(p.new_delete_at_height, 801_000);
    }
}
