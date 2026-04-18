//! Test-only fault-injection harness for crash-recovery validation.
//!
//! This module exposes a named set of sync-points in the write path
//! (redo flush, data pwrite, redb commit, hashtable-resize rename,
//! allocator redo, secondary redb commit) and a thread-local
//! [`FaultMode`] that lets tests arm a panic or a silent no-op at a
//! specific point.
//!
//! ## Design
//!
//! The public API is [`check`], which is invoked at each named boundary
//! in the production write path. When the feature flag
//! `fault-injection` is NOT enabled, [`check`] is an empty inline
//! function — the compiler drops the call entirely, so there is ZERO
//! runtime cost in release builds.
//!
//! When the feature is enabled, [`check`] consults the current
//! thread's [`FaultMode`]:
//!
//! - [`FaultMode::None`]: no-op (fast-path, no allocation).
//! - [`FaultMode::PanicAt`]: if the supplied `point` matches, panic with
//!   a distinctive message that tests catch via
//!   [`std::panic::catch_unwind`]. The panic happens BEFORE the
//!   corresponding side-effect has been observed by the caller, so
//!   placing the check immediately before an fsync simulates a crash
//!   "between previous-step-durable and this-fsync".
//! - [`FaultMode::NoOpAt`]: if the supplied `point` matches, silently
//!   skip (do nothing). Used to simulate "the durability call succeeded
//!   in returning but the write never actually hit the platter" — rare
//!   but useful for cross-checking replay idempotency.
//!
//! ## Crash semantics
//!
//! A panic here is NOT a real SIGKILL, but it exercises the same code
//! paths for recovery purposes: the durability contract is
//! "everything before the last successful fsync is persisted," and an
//! in-process panic at the instant of the fsync preserves exactly that
//! invariant (nothing after the panic runs, including the fsync
//! itself).
//!
//! Tests must tear down the `RedoLog` and backing device handles
//! cleanly and re-open them before asserting post-recovery state, so
//! that no in-memory buffers (which would survive a real process crash
//! as lost data) leak into the recovery run.
//!
//! ## Scope
//!
//! This module is compiled ONLY when `cfg(any(test, feature = "fault-injection"))`
//! is active. When the feature flag is disabled and we are not in
//! test mode, callers still reference [`check`] via an inlined no-op
//! shim defined at the crate root so the production paths remain
//! trivially zero-cost.

/// Named boundaries at which fault injection may fire.
///
/// The variants correspond to specific crash windows in the write
/// path. Each documents the invariant that should hold post-recovery
/// if a panic fires at that point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncPoint {
    /// Just before the redo log's fsync (`RedoLog::flush`) finishes.
    /// Panicking here simulates a crash with redo-buffer bytes written
    /// to the page cache but not yet stable on the platter. Post-
    /// recovery: entries from this batch MUST NOT be replayed.
    BeforeRedoFsync,

    /// Immediately after the redo log's fsync returns successfully.
    /// Panicking here simulates a crash with redo entries durable but
    /// the subsequent data pwrite / redb commit skipped. Post-
    /// recovery: entries MUST be replayed (idempotent).
    AfterRedoFsync,

    /// Just before the data-region pwrite (UTXO slot or metadata).
    /// Panicking here simulates a crash AFTER redo fsync but BEFORE
    /// the actual on-device bytes are written. Post-recovery: replay
    /// MUST produce the final bytes.
    BeforeDataPwrite,

    /// Just after the data-region pwrite returns. Simulates a crash
    /// after data write but before any downstream secondary index
    /// update. Post-recovery: primary bytes are correct, secondary
    /// indexes must be reconciled from the redo log.
    AfterDataPwrite,

    /// Just before the redb secondary-index transaction commit (DAH
    /// or unmined). Panicking here simulates the C4 bug window: redo
    /// durable, secondary redb NOT committed. Post-recovery: the
    /// secondary index MUST be reconciled from the durable redo
    /// intent via generation-aware replay.
    BeforeIndexCommit,

    /// Just after the redb secondary-index commit returns. Simulates
    /// crash with secondary redb durable but the caller's return-
    /// path incomplete. Post-recovery: state is already consistent
    /// (redo replay is a no-op via idempotency check).
    AfterIndexCommit,

    /// Between the tmp-file rename and the parent-directory fsync
    /// during a crash-atomic hashtable resize (C5). Simulates a crash
    /// where the rename hit the kernel but the directory entry is
    /// not yet fsync-durable. Post-recovery: recovery must detect
    /// the pending-resize intent (via `HashtableResizeBegin` without
    /// a matching `Commit`) and remove the orphan tmp file (if the
    /// rename didn't survive) or accept the new table.
    MidHashtableResize,

    /// Between the allocator redo append-and-flush and the in-memory
    /// freelist mutation (C6). Simulates a crash with the allocator
    /// intent durable but the caller-visible freelist still showing
    /// the region as free. Post-recovery: replay of the
    /// `AllocateRegion` / `FreeRegion` redo entry must rebuild the
    /// freelist.
    MidAllocatorPersist,

    /// Just before the secondary-index redb transaction commits in
    /// [`crate::index::redb_dah`] or [`crate::index::redb_unmined`].
    /// Alias of [`SyncPoint::BeforeIndexCommit`] for readability when
    /// the test specifically targets the secondary (redb) path.
    BeforeSecondaryRedbCommit,

    /// Just after the redb secondary-index commit returns.
    /// See [`SyncPoint::BeforeSecondaryRedbCommit`].
    AfterSecondaryRedbCommit,
}

/// Thread-local fault configuration.
///
/// Default is [`FaultMode::None`]. Tests call [`arm`] / [`disarm`] to
/// toggle the mode before invoking the target code path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    /// No fault injection — [`check`] is a no-op.
    None,
    /// Panic when the armed [`SyncPoint`] is reached.
    PanicAt(SyncPoint),
    /// Silently skip past the armed [`SyncPoint`] — the caller that
    /// invoked [`check`] proceeds as if nothing happened, but any
    /// action guarded by the check site (e.g. an fsync wrapped by the
    /// harness) is NOT executed.
    ///
    /// Currently [`check`] does not gate any real work itself — the
    /// production side-effect (fsync / pwrite / redb commit) is still
    /// performed by the caller after [`check`] returns. `NoOpAt` is
    /// therefore functionally equivalent to `None` unless a future
    /// extension adds guarded actions. It is retained here so tests
    /// can express intent without a breaking API change later.
    NoOpAt(SyncPoint),
}

#[cfg(any(test, feature = "fault-injection"))]
mod inner {
    use super::{FaultMode, SyncPoint};
    use std::cell::Cell;

    thread_local! {
        static FAULT_MODE: Cell<FaultMode> = const { Cell::new(FaultMode::None) };
    }

    /// Set the current thread's fault mode.
    ///
    /// Returns the previous mode so tests can restore it deterministically.
    pub fn arm(mode: FaultMode) -> FaultMode {
        FAULT_MODE.with(|slot| slot.replace(mode))
    }

    /// Clear the current thread's fault mode (equivalent to
    /// `arm(FaultMode::None)` but reads more clearly at call sites).
    pub fn disarm() -> FaultMode {
        FAULT_MODE.with(|slot| slot.replace(FaultMode::None))
    }

    /// Return the current thread's fault mode.
    pub fn current() -> FaultMode {
        FAULT_MODE.with(|slot| slot.get())
    }

    /// Check whether the supplied [`SyncPoint`] matches the armed
    /// fault mode. Panics with a distinctive message when armed as
    /// [`FaultMode::PanicAt`] for this point.
    ///
    /// The panic message format is
    /// `"teraslab fault-injection: panic at {point:?}"` — tests
    /// pattern-match the prefix via
    /// [`std::panic::catch_unwind`].
    #[inline]
    pub fn check(point: SyncPoint) {
        let mode = current();
        match mode {
            FaultMode::PanicAt(p) if p == point => {
                panic!("teraslab fault-injection: panic at {point:?}");
            }
            _ => {}
        }
    }

    /// The distinctive panic-message prefix for fault-injection
    /// panics. Exposed so tests can assert on the unwind payload.
    pub const PANIC_PREFIX: &str = "teraslab fault-injection: panic at ";
}

#[cfg(not(any(test, feature = "fault-injection")))]
mod inner {
    use super::{FaultMode, SyncPoint};

    /// No-op `arm` for non-instrumented builds.
    #[inline]
    pub fn arm(_mode: FaultMode) -> FaultMode {
        FaultMode::None
    }

    /// No-op `disarm` for non-instrumented builds.
    #[inline]
    pub fn disarm() -> FaultMode {
        FaultMode::None
    }

    /// Always returns [`FaultMode::None`] in non-instrumented builds.
    #[inline]
    pub fn current() -> FaultMode {
        FaultMode::None
    }

    /// Zero-cost no-op in non-instrumented builds — compiler drops
    /// the call.
    #[inline]
    pub fn check(_point: SyncPoint) {}

    /// Unused in non-instrumented builds; kept for API symmetry.
    pub const PANIC_PREFIX: &str = "teraslab fault-injection: panic at ";
}

pub use inner::{PANIC_PREFIX, arm, check, current, disarm};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disarmed_check_is_noop() {
        let _prev = disarm();
        // No panic expected — this call is a hot no-op.
        check(SyncPoint::BeforeRedoFsync);
        check(SyncPoint::AfterRedoFsync);
        check(SyncPoint::BeforeDataPwrite);
        check(SyncPoint::AfterDataPwrite);
        check(SyncPoint::BeforeIndexCommit);
        check(SyncPoint::AfterIndexCommit);
        check(SyncPoint::MidHashtableResize);
        check(SyncPoint::MidAllocatorPersist);
        check(SyncPoint::BeforeSecondaryRedbCommit);
        check(SyncPoint::AfterSecondaryRedbCommit);
        assert_eq!(current(), FaultMode::None);
    }

    #[test]
    fn armed_panic_at_matching_point_fires_with_prefix() {
        let prev = arm(FaultMode::PanicAt(SyncPoint::BeforeRedoFsync));
        assert_eq!(prev, FaultMode::None);
        let result = std::panic::catch_unwind(|| {
            check(SyncPoint::BeforeRedoFsync);
        });
        // Restore state BEFORE any early-return path can leak the arm.
        let cleared = disarm();
        assert!(matches!(cleared, FaultMode::PanicAt(SyncPoint::BeforeRedoFsync)));
        let err = result.expect_err("expected panic at BeforeRedoFsync");
        let msg = panic_message(&err);
        assert!(
            msg.starts_with(PANIC_PREFIX),
            "expected prefix {PANIC_PREFIX:?}, got {msg:?}"
        );
    }

    #[test]
    fn armed_panic_at_other_point_does_not_fire() {
        let prev = arm(FaultMode::PanicAt(SyncPoint::BeforeRedoFsync));
        assert_eq!(prev, FaultMode::None);
        // Armed at BeforeRedoFsync, but we check a different point → no panic.
        check(SyncPoint::AfterRedoFsync);
        check(SyncPoint::BeforeDataPwrite);
        let cleared = disarm();
        assert!(matches!(cleared, FaultMode::PanicAt(SyncPoint::BeforeRedoFsync)));
    }

    #[test]
    fn noop_at_does_not_panic() {
        let _prev = arm(FaultMode::NoOpAt(SyncPoint::BeforeIndexCommit));
        check(SyncPoint::BeforeIndexCommit);
        check(SyncPoint::AfterIndexCommit);
        let _ = disarm();
    }

    /// Extract the `&str` message from a panic payload. Covers the two
    /// common payload types (`&'static str` and `String`) that
    /// `panic!("...")` produces.
    fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
        if let Some(s) = payload.downcast_ref::<&'static str>() {
            return (*s).to_string();
        }
        if let Some(s) = payload.downcast_ref::<String>() {
            return s.clone();
        }
        format!("<non-string panic payload: {:?}>", payload.type_id())
    }
}
