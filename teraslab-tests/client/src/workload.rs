//! Background workload generator that drives operations against the cluster.
//!
//! The [`WorkloadRunner`] tracks configuration, metrics, and run-state but does
//! **not** hold a reference to a network client. The actual execution loop lives
//! in the test scenarios — they create the runner, then in a `tokio` task call
//! methods on it and the client. This keeps the module simple and avoids
//! circular dependency issues.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::Rng;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Rate targets for each operation type (operations per second).
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    /// Target number of CREATE operations per second.
    pub creates_per_sec: u32,
    /// Target number of SPEND operations per second.
    pub spends_per_sec: u32,
    /// Target number of SET_MINED operations per second.
    pub set_mined_per_sec: u32,
    /// Target number of READ operations per second.
    pub reads_per_sec: u32,
    /// Target number of DELETE operations per second.
    pub deletes_per_sec: u32,
    /// Target number of FREEZE operations per second.
    pub freeze_per_sec: u32,
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Atomic counters for every operation type, split into success and error.
///
/// All fields use relaxed ordering for maximum throughput — callers that need
/// a consistent snapshot should use [`WorkloadRunner::snapshot`].
pub struct WorkloadMetrics {
    /// Successful CREATE operations.
    pub creates_ok: AtomicU64,
    /// Failed CREATE operations.
    pub creates_err: AtomicU64,
    /// Successful SPEND operations.
    pub spends_ok: AtomicU64,
    /// Failed SPEND operations.
    pub spends_err: AtomicU64,
    /// Successful READ operations.
    pub reads_ok: AtomicU64,
    /// Failed READ operations.
    pub reads_err: AtomicU64,
    /// Successful SET_MINED operations.
    pub set_mined_ok: AtomicU64,
    /// Failed SET_MINED operations.
    pub set_mined_err: AtomicU64,
    /// Successful DELETE operations.
    pub deletes_ok: AtomicU64,
    /// Failed DELETE operations.
    pub deletes_err: AtomicU64,
    /// Total operations attempted (success + error, all types).
    pub total_ops: AtomicU64,
    /// Total errors across all operation types.
    pub total_errors: AtomicU64,
}

impl Default for WorkloadMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkloadMetrics {
    /// Create a new metrics instance with every counter initialised to zero.
    pub fn new() -> Self {
        Self {
            creates_ok: AtomicU64::new(0),
            creates_err: AtomicU64::new(0),
            spends_ok: AtomicU64::new(0),
            spends_err: AtomicU64::new(0),
            reads_ok: AtomicU64::new(0),
            reads_err: AtomicU64::new(0),
            set_mined_ok: AtomicU64::new(0),
            set_mined_err: AtomicU64::new(0),
            deletes_ok: AtomicU64::new(0),
            deletes_err: AtomicU64::new(0),
            total_ops: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
        }
    }

    /// Read all counters into a plain-old-data [`WorkloadSnapshot`].
    ///
    /// Uses `Ordering::Relaxed` for each individual load. The snapshot is
    /// therefore *approximately* consistent — individual counters may reflect
    /// slightly different points in time — but this is acceptable for progress
    /// reporting and test assertions.
    pub fn snapshot(&self, elapsed: Duration) -> WorkloadSnapshot {
        WorkloadSnapshot {
            creates_ok: self.creates_ok.load(Ordering::Relaxed),
            creates_err: self.creates_err.load(Ordering::Relaxed),
            spends_ok: self.spends_ok.load(Ordering::Relaxed),
            spends_err: self.spends_err.load(Ordering::Relaxed),
            reads_ok: self.reads_ok.load(Ordering::Relaxed),
            reads_err: self.reads_err.load(Ordering::Relaxed),
            total_ops: self.total_ops.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            elapsed,
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot (plain-old-data copy of the metrics at a point in time)
// ---------------------------------------------------------------------------

/// A point-in-time copy of all workload counters plus the wall-clock elapsed
/// time since the workload started.
#[derive(Debug, Clone)]
pub struct WorkloadSnapshot {
    /// Successful CREATE operations.
    pub creates_ok: u64,
    /// Failed CREATE operations.
    pub creates_err: u64,
    /// Successful SPEND operations.
    pub spends_ok: u64,
    /// Failed SPEND operations.
    pub spends_err: u64,
    /// Successful READ operations.
    pub reads_ok: u64,
    /// Failed READ operations.
    pub reads_err: u64,
    /// Total operations attempted.
    pub total_ops: u64,
    /// Total errors across all types.
    pub total_errors: u64,
    /// Wall-clock time since the workload was created.
    pub elapsed: Duration,
}

// ---------------------------------------------------------------------------
// Run-state constants
// ---------------------------------------------------------------------------

/// The workload is actively generating operations.
const STATE_RUNNING: u8 = 0;
/// The workload is paused — no new operations are dispatched.
const STATE_PAUSED: u8 = 1;
/// The workload has been permanently stopped.
const STATE_STOPPED: u8 = 2;

// ---------------------------------------------------------------------------
// WorkloadRunner
// ---------------------------------------------------------------------------

/// Controls workload generation state and collects metrics.
///
/// The runner itself does **not** perform network I/O — it tracks
/// configuration, shared metrics, and a run-state flag that the test
/// scenario's `tokio` task checks between operation batches.
pub struct WorkloadRunner {
    /// Rate configuration for each operation type.
    config: WorkloadConfig,
    /// Shared metrics counters, wrapped in `Arc` so the driving task and the
    /// test harness can both access them concurrently.
    metrics: Arc<WorkloadMetrics>,
    /// Atomic run-state: 0 = running, 1 = paused, 2 = stopped.
    state: Arc<AtomicU8>,
    /// Wall-clock instant when the runner was created.
    start_time: Instant,
}

impl WorkloadRunner {
    /// Create a new workload runner with the given rate configuration.
    ///
    /// Metrics are initialised to zero and the state is set to *running*.
    ///
    /// # Parameters
    /// - `config`: Target operation rates per second for each type.
    pub fn new(config: WorkloadConfig) -> Self {
        Self {
            config,
            metrics: Arc::new(WorkloadMetrics::new()),
            state: Arc::new(AtomicU8::new(STATE_RUNNING)),
            start_time: Instant::now(),
        }
    }

    /// Returns a shared reference to the metrics counters.
    ///
    /// The returned `Arc` can be passed to background tasks that increment
    /// the counters while the test harness reads them for progress reporting.
    pub fn metrics(&self) -> Arc<WorkloadMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Returns a reference to the workload configuration.
    pub fn config(&self) -> &WorkloadConfig {
        &self.config
    }

    /// Returns a shared reference to the run-state flag.
    ///
    /// Background tasks should check this between operation batches to honour
    /// pause/stop requests from the test harness.
    pub fn state(&self) -> Arc<AtomicU8> {
        Arc::clone(&self.state)
    }

    /// Take a point-in-time snapshot of all metrics plus elapsed wall-clock time.
    pub fn snapshot(&self) -> WorkloadSnapshot {
        let elapsed = self.start_time.elapsed();
        self.metrics.snapshot(elapsed)
    }

    /// Pause the workload. Background tasks should stop dispatching operations
    /// until [`resume`](Self::resume) is called.
    pub fn pause(&self) {
        self.state.store(STATE_PAUSED, Ordering::Release);
    }

    /// Resume a paused workload.
    pub fn resume(&self) {
        self.state.store(STATE_RUNNING, Ordering::Release);
    }

    /// Permanently stop the workload. This cannot be undone.
    pub fn stop(&self) {
        self.state.store(STATE_STOPPED, Ordering::Release);
    }

    /// Returns `true` if the workload is currently paused.
    pub fn is_paused(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_PAUSED
    }

    /// Returns `true` if the workload has been permanently stopped.
    pub fn is_stopped(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_STOPPED
    }

    /// Returns `true` if the workload is actively running (neither paused nor stopped).
    pub fn is_running(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_RUNNING
    }

    /// Generate a random 32-byte txid.
    ///
    /// Uses the provided RNG to fill all 32 bytes with random data.
    ///
    /// # Parameters
    /// - `rng`: Any type implementing `rand::Rng`.
    pub fn generate_txid(rng: &mut impl Rng) -> [u8; 32] {
        let mut txid = [0u8; 32];
        rng.fill(&mut txid);
        txid
    }

    /// Generate a random 32-byte UTXO hash.
    ///
    /// Uses the provided RNG to fill all 32 bytes with random data.
    ///
    /// # Parameters
    /// - `rng`: Any type implementing `rand::Rng`.
    pub fn generate_utxo_hash(rng: &mut impl Rng) -> [u8; 32] {
        let mut hash = [0u8; 32];
        rng.fill(&mut hash);
        hash
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    fn default_config() -> WorkloadConfig {
        WorkloadConfig {
            creates_per_sec: 1000,
            spends_per_sec: 500,
            set_mined_per_sec: 200,
            reads_per_sec: 300,
            deletes_per_sec: 50,
            freeze_per_sec: 10,
        }
    }

    #[test]
    fn metrics_initialised_to_zero() {
        let m = WorkloadMetrics::new();
        assert_eq!(m.creates_ok.load(Ordering::Relaxed), 0);
        assert_eq!(m.creates_err.load(Ordering::Relaxed), 0);
        assert_eq!(m.spends_ok.load(Ordering::Relaxed), 0);
        assert_eq!(m.spends_err.load(Ordering::Relaxed), 0);
        assert_eq!(m.reads_ok.load(Ordering::Relaxed), 0);
        assert_eq!(m.reads_err.load(Ordering::Relaxed), 0);
        assert_eq!(m.set_mined_ok.load(Ordering::Relaxed), 0);
        assert_eq!(m.set_mined_err.load(Ordering::Relaxed), 0);
        assert_eq!(m.deletes_ok.load(Ordering::Relaxed), 0);
        assert_eq!(m.deletes_err.load(Ordering::Relaxed), 0);
        assert_eq!(m.total_ops.load(Ordering::Relaxed), 0);
        assert_eq!(m.total_errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn metrics_snapshot_reflects_increments() {
        let m = WorkloadMetrics::new();
        m.creates_ok.fetch_add(10, Ordering::Relaxed);
        m.spends_err.fetch_add(3, Ordering::Relaxed);
        m.total_ops.fetch_add(13, Ordering::Relaxed);
        m.total_errors.fetch_add(3, Ordering::Relaxed);

        let snap = m.snapshot(Duration::from_secs(5));
        assert_eq!(snap.creates_ok, 10);
        assert_eq!(snap.spends_err, 3);
        assert_eq!(snap.total_ops, 13);
        assert_eq!(snap.total_errors, 3);
        assert_eq!(snap.elapsed, Duration::from_secs(5));
    }

    #[test]
    fn runner_starts_in_running_state() {
        let runner = WorkloadRunner::new(default_config());
        assert!(runner.is_running());
        assert!(!runner.is_paused());
        assert!(!runner.is_stopped());
    }

    #[test]
    fn runner_pause_resume_stop() {
        let runner = WorkloadRunner::new(default_config());

        runner.pause();
        assert!(runner.is_paused());
        assert!(!runner.is_running());
        assert!(!runner.is_stopped());

        runner.resume();
        assert!(runner.is_running());
        assert!(!runner.is_paused());

        runner.stop();
        assert!(runner.is_stopped());
        assert!(!runner.is_running());
        assert!(!runner.is_paused());
    }

    #[test]
    fn runner_snapshot_includes_elapsed() {
        let runner = WorkloadRunner::new(default_config());
        // Just confirm the snapshot is obtainable and elapsed is non-negative.
        let snap = runner.snapshot();
        assert_eq!(snap.total_ops, 0);
        // elapsed should be very small but non-negative
        assert!(snap.elapsed.as_nanos() < 1_000_000_000);
    }

    #[test]
    fn runner_shared_metrics() {
        let runner = WorkloadRunner::new(default_config());
        let m = runner.metrics();
        m.creates_ok.fetch_add(7, Ordering::Relaxed);
        m.total_ops.fetch_add(7, Ordering::Relaxed);

        let snap = runner.snapshot();
        assert_eq!(snap.creates_ok, 7);
        assert_eq!(snap.total_ops, 7);
    }

    #[test]
    fn generate_txid_produces_32_bytes() {
        let mut rng = StdRng::seed_from_u64(42);
        let txid = WorkloadRunner::generate_txid(&mut rng);
        assert_eq!(txid.len(), 32);
        // With a seeded RNG the result should be deterministic and non-zero.
        assert_ne!(txid, [0u8; 32]);
    }

    #[test]
    fn generate_utxo_hash_produces_32_bytes() {
        let mut rng = StdRng::seed_from_u64(99);
        let hash = WorkloadRunner::generate_utxo_hash(&mut rng);
        assert_eq!(hash.len(), 32);
        assert_ne!(hash, [0u8; 32]);
    }

    #[test]
    fn generate_txid_deterministic_with_same_seed() {
        let mut rng1 = StdRng::seed_from_u64(123);
        let mut rng2 = StdRng::seed_from_u64(123);
        let a = WorkloadRunner::generate_txid(&mut rng1);
        let b = WorkloadRunner::generate_txid(&mut rng2);
        assert_eq!(a, b);
    }

    #[test]
    fn generate_txid_different_with_different_seeds() {
        let mut rng1 = StdRng::seed_from_u64(1);
        let mut rng2 = StdRng::seed_from_u64(2);
        let a = WorkloadRunner::generate_txid(&mut rng1);
        let b = WorkloadRunner::generate_txid(&mut rng2);
        assert_ne!(a, b);
    }

    #[test]
    fn config_accessible_from_runner() {
        let config = default_config();
        let runner = WorkloadRunner::new(config);
        assert_eq!(runner.config().creates_per_sec, 1000);
        assert_eq!(runner.config().spends_per_sec, 500);
        assert_eq!(runner.config().set_mined_per_sec, 200);
        assert_eq!(runner.config().reads_per_sec, 300);
        assert_eq!(runner.config().deletes_per_sec, 50);
        assert_eq!(runner.config().freeze_per_sec, 10);
    }

    #[test]
    fn state_arc_shared_correctly() {
        let runner = WorkloadRunner::new(default_config());
        let state = runner.state();
        // Simulate a background task stopping the workload via the shared Arc.
        state.store(STATE_STOPPED, Ordering::Release);
        assert!(runner.is_stopped());
    }
}
