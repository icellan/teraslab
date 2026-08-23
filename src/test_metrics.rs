//! Test-only access to the PROCESS-GLOBAL migration metrics, shared by every
//! module whose tests assert on them.
//!
//! [`crate::metrics::init_migration_metrics`] installs into a `OnceLock`, so the
//! first installer wins for the whole test binary. That makes "are migration
//! metrics installed?" depend on which test ran first — a cross-module ordering
//! hazard. Both halves of the answer live here so any module can install
//! deliberately rather than inherit another module's side effect, and so the
//! serializing guard is the SAME mutex everywhere (it was previously private to
//! the coordinator's test module, which is why `server::dispatch` had no way to
//! assert on a migration counter at all).

use crate::metrics::MigrationMetrics;
use std::sync::OnceLock;

/// Install the shared test `MigrationMetrics` (idempotent) and return it.
///
/// Safe to call from any module: the underlying `OnceLock` makes repeat
/// installs a no-op and every caller observes the same instance, so counter
/// deltas taken under [`migration_metrics_test_guard`] are consistent.
pub(crate) fn install_test_migration_metrics() -> &'static MigrationMetrics {
    use crate::metrics::{init_migration_metrics, migration_metrics};
    static TEST_METRICS: OnceLock<MigrationMetrics> = OnceLock::new();
    let m_ref: &'static MigrationMetrics = TEST_METRICS.get_or_init(MigrationMetrics::new);
    init_migration_metrics(m_ref);
    migration_metrics().expect("metrics installed")
}

/// Serializes tests that assert on the process-global migration counters.
///
/// Required for ABSOLUTE assertions and for any `before`/`after` delta whose
/// counter a concurrent test could also move.
pub(crate) fn migration_metrics_test_guard() -> parking_lot::MutexGuard<'static, ()> {
    static GUARD: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| parking_lot::Mutex::new(())).lock()
}
