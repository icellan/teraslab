//! Task #75 — coordinator event-loop stall watchdog.
//!
//! Diagnostic instrument for the recurring CI node-wedge where the
//! coordinator event loop AND the `/status` HTTP server go silent for
//! 120 s+ while SWIM probing and the periodic checkpoint task stay alive.
//! No stack evidence exists for the wedge, so this module captures the
//! next occurrence in three cheap layers:
//!
//! 1. **Heartbeat** ([`LoopHeartbeat`]): the event loop stamps a
//!    monotonic-millis timestamp plus a [`LoopPhase`] tag (which loop
//!    section it is about to run) — two relaxed atomic stores per stamp.
//!    When the loop wedges, the LAST tag names the section it entered and
//!    never left.
//! 2. **Watchdog thread** ([`spawn_watchdog`]): checks the heartbeat every
//!    [`WATCHDOG_CHECK_INTERVAL`]; a gap over [`STALL_THRESHOLD`] logs an
//!    ERROR with the stalled duration and last phase tag, rate-limited to
//!    once per [`STALL_RELOG_INTERVAL`] while the stall persists, plus one
//!    "recovered after Xs" line when the loop stamps again. The watchdog
//!    itself NEVER takes a coordinator lock — it reads atomics and uses
//!    zero-wait `try_*` probes only, so it cannot join the deadlock it is
//!    reporting on.
//! 3. **Lock fingerprint** ([`probe_coordinator_locks`]): zero-wait
//!    `try_read`/`try_lock` probes of the three contended coordinator locks
//!    (`shard_table`, `migration`, `node_addrs`). Which of them are
//!    unacquirable distinguishes "shard_table write-hold" from "migration
//!    mutex held" from "loop blocked on recv with all locks free".
//!
//! parking_lot's `deadlock_detection` cargo feature was considered and
//! deliberately NOT enabled: it hooks EVERY parking_lot acquire/release
//! process-wide (thread-local bookkeeping per lock op), and the hot path
//! takes multiple parking_lot locks per operation (record stripes, tx
//! locks, mined-index shards, redo). At the 10M+ ops/sec design target
//! that is a measurable tax on every request AND it perturbs the very
//! timing the instrument exists to observe. The try-lock fingerprint
//! answers the actionable question (WHICH lock is held) without either
//! cost; a full lock-cycle proof can be added later behind a
//! diagnostics-only build if the fingerprint proves insufficient.

use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Heartbeat silence beyond this duration is reported as a stall.
///
/// The event loop's slowest legitimate quiet path is a 100 ms
/// `recv_timeout` tick, so 10 s of silence is two orders of magnitude
/// beyond normal. Long activations/migration planning CAN legitimately
/// exceed this under CI load — the ERROR line's phase tag makes that case
/// readable (see module docs).
pub const STALL_THRESHOLD: Duration = Duration::from_secs(10);

/// How often the watchdog thread evaluates the heartbeat.
pub const WATCHDOG_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// While a stall persists, re-log at most once per this interval.
pub const STALL_RELOG_INTERVAL: Duration = Duration::from_secs(30);

/// Sleep slice inside the watchdog thread, kept short so shutdown is
/// observed promptly without busy-waiting.
const WATCHDOG_POLL_SLICE: Duration = Duration::from_millis(250);

/// Which section of the coordinator event loop last stamped the heartbeat.
///
/// The tags mark the loop SECTIONS in execution order, stamped at each
/// section's entry. A wedged loop therefore reports the section it entered
/// and never left; a healthy loop reports whatever section ran last before
/// the current `recv` wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum LoopPhase {
    /// Before the event loop's first iteration (or an unknown tag byte).
    Startup = 0,
    /// About to block on `event_rx.recv_timeout` (top of every iteration).
    Recv = 1,
    /// Handling a received `ClusterEvent` (may include a topology
    /// activation for `TopologyStale`).
    HandleEvent = 2,
    /// The recv-timeout branch: fallback proposer, re-election tick.
    TimeoutTick = 3,
    /// Periodic under-replication sweep + placement-upgrade settle gate.
    Sweep = 4,
    /// Debounced membership-change topology proposal firing.
    DebouncePropose = 5,
    /// Event-driven under-replication repair trigger evaluation/fire.
    RepairFire = 6,
    /// Inbound-migration prune + stranded-Fenced-task reap (locks the
    /// migration mutex every iteration).
    InboundPrune = 7,
    /// W1.5 prompt committed-term catch-up check (reads the shard table
    /// every iteration).
    PromptCatchUp = 8,
    /// Reactivation gates + reactivation work (startup / normal / drain).
    Reactivation = 9,
    /// Draining `topology_commit_rx` (activations / exchange spawns).
    CommitDrain = 10,
    /// Draining `exchange_complete_rx` (view-refined activations,
    /// re-heal, re-election).
    ExchangeDrain = 11,
    /// Draining `resync_request_rx` (spawning full-shard backfills).
    ResyncDrain = 12,
    /// Draining `transfer_request_rx` (source-side re-migrations).
    TransferDrain = 13,
    /// Requester-side stalled-inbound transfer-request pass.
    TransferRequest = 14,
}

impl LoopPhase {
    /// Human-readable tag name for logs and `/status`.
    pub fn as_str(self) -> &'static str {
        match self {
            LoopPhase::Startup => "startup",
            LoopPhase::Recv => "recv",
            LoopPhase::HandleEvent => "handle_event",
            LoopPhase::TimeoutTick => "timeout_tick",
            LoopPhase::Sweep => "sweep",
            LoopPhase::DebouncePropose => "debounce_propose",
            LoopPhase::RepairFire => "repair_fire",
            LoopPhase::InboundPrune => "inbound_prune",
            LoopPhase::PromptCatchUp => "prompt_catch_up",
            LoopPhase::Reactivation => "reactivation",
            LoopPhase::CommitDrain => "commit_drain",
            LoopPhase::ExchangeDrain => "exchange_drain",
            LoopPhase::ResyncDrain => "resync_drain",
            LoopPhase::TransferDrain => "transfer_drain",
            LoopPhase::TransferRequest => "transfer_request",
        }
    }

    /// Decode a stored tag byte. Unknown values decode to
    /// [`LoopPhase::Startup`] rather than failing — the byte only ever
    /// comes from [`LoopHeartbeat::stamp`], so an unknown value means a
    /// torn/uninitialized read and the diagnostic must still render.
    pub fn from_u8(v: u8) -> LoopPhase {
        match v {
            1 => LoopPhase::Recv,
            2 => LoopPhase::HandleEvent,
            3 => LoopPhase::TimeoutTick,
            4 => LoopPhase::Sweep,
            5 => LoopPhase::DebouncePropose,
            6 => LoopPhase::RepairFire,
            7 => LoopPhase::InboundPrune,
            8 => LoopPhase::PromptCatchUp,
            9 => LoopPhase::Reactivation,
            10 => LoopPhase::CommitDrain,
            11 => LoopPhase::ExchangeDrain,
            12 => LoopPhase::ResyncDrain,
            13 => LoopPhase::TransferDrain,
            14 => LoopPhase::TransferRequest,
            _ => LoopPhase::Startup,
        }
    }
}

/// Shared event-loop liveness stamp.
///
/// Time is measured as milliseconds since this struct's construction
/// (a monotonic [`Instant`] epoch), so wall-clock jumps (NTP) can never
/// fabricate or hide a stall.
pub struct LoopHeartbeat {
    /// Monotonic epoch all stored timestamps are relative to.
    epoch: Instant,
    /// Milliseconds since `epoch` at the last [`LoopHeartbeat::stamp`].
    beat_ms: AtomicU64,
    /// The [`LoopPhase`] tag byte of the last stamp.
    phase: AtomicU8,
}

impl LoopHeartbeat {
    /// Create a heartbeat whose first beat is "now" with
    /// [`LoopPhase::Startup`], so the pre-loop window never reads as an
    /// infinite stall.
    pub fn new() -> Self {
        LoopHeartbeat {
            epoch: Instant::now(),
            beat_ms: AtomicU64::new(0),
            phase: AtomicU8::new(LoopPhase::Startup as u8),
        }
    }

    /// Milliseconds elapsed since this heartbeat was created (monotonic).
    pub fn now_ms(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Record that the event loop is alive and about to run `phase`.
    /// Two relaxed stores — cheap enough for every loop section entry.
    pub fn stamp(&self, phase: LoopPhase) {
        self.beat_ms.store(self.now_ms(), Ordering::Relaxed);
        self.phase.store(phase as u8, Ordering::Relaxed);
    }

    /// The last stamp: (millis-since-epoch, phase tag).
    pub fn beat(&self) -> (u64, LoopPhase) {
        (
            self.beat_ms.load(Ordering::Relaxed),
            LoopPhase::from_u8(self.phase.load(Ordering::Relaxed)),
        )
    }

    /// Milliseconds of heartbeat silence plus the last phase tag —
    /// the `/status` diagnostic readout.
    pub fn age_and_phase(&self) -> (u64, LoopPhase) {
        let (beat, phase) = self.beat();
        (self.now_ms().saturating_sub(beat), phase)
    }
}

impl Default for LoopHeartbeat {
    fn default() -> Self {
        Self::new()
    }
}

/// What the watchdog should do after one heartbeat observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StallVerdict {
    /// Heartbeat fresh, no episode active.
    Healthy,
    /// A new stall episode began — log ERROR and capture the lock
    /// fingerprint (once per episode).
    StallStarted {
        /// Milliseconds of heartbeat silence observed.
        stalled_ms: u64,
    },
    /// Episode continues but the re-log interval has not elapsed — silent.
    StallOngoingQuiet {
        /// Milliseconds of heartbeat silence observed.
        stalled_ms: u64,
    },
    /// Episode continues and the re-log interval elapsed — log ERROR again.
    StallOngoingRelog {
        /// Milliseconds of heartbeat silence observed.
        stalled_ms: u64,
    },
    /// The loop stamped again after an episode — log recovery.
    Recovered {
        /// Full silent gap: last stamp before the stall to the first
        /// stamp after it.
        stalled_ms: u64,
    },
}

/// Pure stall-episode state machine (unit-testable without threads).
///
/// Feed it `(now_ms, beat_ms)` observations; it tracks episode boundaries
/// and rate-limits repeat logging. All timestamps must come from the same
/// monotonic clock ([`LoopHeartbeat::now_ms`]).
pub struct StallTracker {
    stall_threshold_ms: u64,
    relog_interval_ms: u64,
    /// `Some(beat)` while an episode is active — the beat value observed
    /// when the episode started, i.e. the LAST stamp before the silence.
    episode_beat_ms: Option<u64>,
    /// `now_ms` of the last emitted stall log (started or re-log).
    last_log_at_ms: u64,
}

impl StallTracker {
    /// Build a tracker with the given stall threshold and re-log interval.
    pub fn new(stall_threshold: Duration, relog_interval: Duration) -> Self {
        StallTracker {
            stall_threshold_ms: u64::try_from(stall_threshold.as_millis()).unwrap_or(u64::MAX),
            relog_interval_ms: u64::try_from(relog_interval.as_millis()).unwrap_or(u64::MAX),
            episode_beat_ms: None,
            last_log_at_ms: 0,
        }
    }

    /// Observe the heartbeat and decide what (if anything) to report.
    ///
    /// `now_ms` is the current monotonic time, `beat_ms` the heartbeat's
    /// last stamp; both from [`LoopHeartbeat`]'s clock. Returns the
    /// [`StallVerdict`] the caller should act on.
    pub fn observe(&mut self, now_ms: u64, beat_ms: u64) -> StallVerdict {
        let age_ms = now_ms.saturating_sub(beat_ms);
        let stalled = age_ms > self.stall_threshold_ms;
        match self.episode_beat_ms {
            None => {
                if stalled {
                    self.episode_beat_ms = Some(beat_ms);
                    self.last_log_at_ms = now_ms;
                    StallVerdict::StallStarted { stalled_ms: age_ms }
                } else {
                    StallVerdict::Healthy
                }
            }
            Some(episode_beat) => {
                // Any beat advance since the episode began means the loop
                // ran again: the ORIGINAL stall is over even if a NEW gap
                // has already exceeded the threshold (recovery + re-stall
                // inside one check interval). Report the recovery; the
                // next observation opens the new episode.
                if beat_ms != episode_beat {
                    self.episode_beat_ms = None;
                    return StallVerdict::Recovered {
                        stalled_ms: beat_ms.saturating_sub(episode_beat),
                    };
                }
                if !stalled {
                    // Unreachable with a monotonic clock (the gap can only
                    // shrink via a beat advance), kept total for safety.
                    self.episode_beat_ms = None;
                    return StallVerdict::Recovered { stalled_ms: age_ms };
                }
                if now_ms.saturating_sub(self.last_log_at_ms) >= self.relog_interval_ms {
                    self.last_log_at_ms = now_ms;
                    StallVerdict::StallOngoingRelog { stalled_ms: age_ms }
                } else {
                    StallVerdict::StallOngoingQuiet { stalled_ms: age_ms }
                }
            }
        }
    }
}

/// Zero-wait acquirability snapshot of the three contended coordinator
/// locks. `true` = a `try_*` acquisition SUCCEEDED (lock available).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockFingerprint {
    /// `shard_table` (`RwLock`) read-acquirable — `false` fingers a
    /// write-hold (or a writer-queue pileup) on the shard table.
    pub shard_table_readable: bool,
    /// `migration` (`Mutex`) acquirable — `false` fingers a held
    /// migration-manager mutex.
    pub migration_lockable: bool,
    /// `node_addrs` (`RwLock`) read-acquirable.
    pub node_addrs_readable: bool,
}

impl LockFingerprint {
    /// Compact log form, e.g. `shard_table=HELD migration=free
    /// node_addrs=free`. `HELD` marks the unacquirable locks — the
    /// fingerprint that distinguishes a shard-table write-hold from a
    /// migration-mutex hold from an all-free block (loop stuck elsewhere).
    pub fn describe(&self) -> String {
        let mark = |free: bool| if free { "free" } else { "HELD" };
        format!(
            "shard_table={} migration={} node_addrs={}",
            mark(self.shard_table_readable),
            mark(self.migration_lockable),
            mark(self.node_addrs_readable),
        )
    }
}

/// Probe the three coordinator locks with zero-wait `try_*` calls and
/// report which are acquirable. Never blocks and never holds any guard
/// beyond the probe expression, so it is safe to call from the watchdog
/// while the coordinator is wedged. Generic over the payload types so the
/// state machine is testable without coordinator fixtures.
pub fn probe_coordinator_locks<T, M, A>(
    shard_table: &RwLock<T>,
    migration: &Mutex<M>,
    node_addrs: &RwLock<A>,
) -> LockFingerprint {
    LockFingerprint {
        shard_table_readable: shard_table.try_read().is_some(),
        migration_lockable: migration.try_lock().is_some(),
        node_addrs_readable: node_addrs.try_read().is_some(),
    }
}

/// Spawn the watchdog thread. Checks `heartbeat` every
/// [`WATCHDOG_CHECK_INTERVAL`]; on a stall (silence >
/// [`STALL_THRESHOLD`]) logs ERROR with the stalled duration, the last
/// [`LoopPhase`] tag, and the [`LockFingerprint`] — once at episode start
/// and then at most once per [`STALL_RELOG_INTERVAL`] — plus one recovery
/// line when the loop stamps again. Exits when `shutdown` becomes `true`
/// (observed within [`WATCHDOG_POLL_SLICE`]).
///
/// The thread only ever reads atomics and zero-wait `try_*` probes; it
/// can never contribute to the contention it reports.
pub fn spawn_watchdog<T, M, A>(
    heartbeat: Arc<LoopHeartbeat>,
    shutdown: Arc<AtomicBool>,
    shard_table: Arc<RwLock<T>>,
    migration: Arc<Mutex<M>>,
    node_addrs: Arc<RwLock<A>>,
) -> std::thread::JoinHandle<()>
where
    T: Send + Sync + 'static,
    M: Send + 'static,
    A: Send + Sync + 'static,
{
    std::thread::spawn(move || {
        let mut tracker = StallTracker::new(STALL_THRESHOLD, STALL_RELOG_INTERVAL);
        let mut last_check = Instant::now();
        while !shutdown.load(Ordering::Relaxed) {
            std::thread::sleep(WATCHDOG_POLL_SLICE);
            if last_check.elapsed() < WATCHDOG_CHECK_INTERVAL {
                continue;
            }
            last_check = Instant::now();
            let (beat_ms, phase) = heartbeat.beat();
            match tracker.observe(heartbeat.now_ms(), beat_ms) {
                StallVerdict::Healthy | StallVerdict::StallOngoingQuiet { .. } => {}
                StallVerdict::StallStarted { stalled_ms } => {
                    let fp = probe_coordinator_locks(&shard_table, &migration, &node_addrs);
                    tracing::error!(
                        stalled_ms,
                        last_phase = phase.as_str(),
                        locks = %fp.describe(),
                        "cluster: coordinator event loop STALLED — no heartbeat; last \
                         phase tag names the loop section it never left",
                    );
                }
                StallVerdict::StallOngoingRelog { stalled_ms } => {
                    let fp = probe_coordinator_locks(&shard_table, &migration, &node_addrs);
                    tracing::error!(
                        stalled_ms,
                        last_phase = phase.as_str(),
                        locks = %fp.describe(),
                        "cluster: coordinator event loop STILL stalled",
                    );
                }
                StallVerdict::Recovered { stalled_ms } => {
                    tracing::error!(
                        stalled_ms,
                        "cluster: coordinator event loop recovered after {:.1}s stall",
                        stalled_ms as f64 / 1000.0,
                    );
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: u64 = 10_000; // stall threshold in ms, matches STALL_THRESHOLD
    const R: u64 = 30_000; // relog interval in ms, matches STALL_RELOG_INTERVAL

    fn tracker() -> StallTracker {
        StallTracker::new(Duration::from_millis(T), Duration::from_millis(R))
    }

    #[test]
    fn phase_tag_roundtrips_through_u8() {
        let all = [
            LoopPhase::Startup,
            LoopPhase::Recv,
            LoopPhase::HandleEvent,
            LoopPhase::TimeoutTick,
            LoopPhase::Sweep,
            LoopPhase::DebouncePropose,
            LoopPhase::RepairFire,
            LoopPhase::InboundPrune,
            LoopPhase::PromptCatchUp,
            LoopPhase::Reactivation,
            LoopPhase::CommitDrain,
            LoopPhase::ExchangeDrain,
            LoopPhase::ResyncDrain,
            LoopPhase::TransferDrain,
            LoopPhase::TransferRequest,
        ];
        for phase in all {
            assert_eq!(LoopPhase::from_u8(phase as u8), phase);
            assert!(!phase.as_str().is_empty());
        }
        // Unknown bytes decode to Startup, never panic.
        assert_eq!(LoopPhase::from_u8(200), LoopPhase::Startup);
    }

    #[test]
    fn heartbeat_stamp_updates_beat_and_phase() {
        let hb = LoopHeartbeat::new();
        let (beat0, phase0) = hb.beat();
        assert_eq!(beat0, 0);
        assert_eq!(phase0, LoopPhase::Startup);

        hb.stamp(LoopPhase::CommitDrain);
        let (beat, phase) = hb.beat();
        assert_eq!(phase, LoopPhase::CommitDrain);
        assert!(beat <= hb.now_ms());

        let (age, phase) = hb.age_and_phase();
        assert_eq!(phase, LoopPhase::CommitDrain);
        // Freshly stamped: silence is (far) below a second.
        assert!(
            age < 1_000,
            "fresh stamp must have near-zero age, got {age}"
        );
    }

    #[test]
    fn tracker_stays_healthy_below_threshold() {
        let mut t = tracker();
        assert_eq!(t.observe(0, 0), StallVerdict::Healthy);
        assert_eq!(t.observe(T, 0), StallVerdict::Healthy); // age == threshold: not yet a stall
        assert_eq!(t.observe(T + 5_000, T), StallVerdict::Healthy); // beat advanced
    }

    #[test]
    fn tracker_fires_once_then_rate_limits_then_relogs() {
        let mut t = tracker();
        // 11s of silence: episode starts.
        assert_eq!(
            t.observe(T + 1_000, 0),
            StallVerdict::StallStarted {
                stalled_ms: T + 1_000
            }
        );
        // 5s later, still silent, inside the 30s relog window: quiet.
        assert_eq!(
            t.observe(T + 6_000, 0),
            StallVerdict::StallOngoingQuiet {
                stalled_ms: T + 6_000
            }
        );
        // 30s after the first log: re-log exactly once...
        assert_eq!(
            t.observe(T + 31_000, 0),
            StallVerdict::StallOngoingRelog {
                stalled_ms: T + 31_000
            }
        );
        // ...then quiet again until the next 30s window elapses.
        assert_eq!(
            t.observe(T + 36_000, 0),
            StallVerdict::StallOngoingQuiet {
                stalled_ms: T + 36_000
            }
        );
        assert_eq!(
            t.observe(T + 61_000, 0),
            StallVerdict::StallOngoingRelog {
                stalled_ms: T + 61_000
            }
        );
    }

    #[test]
    fn tracker_reports_recovery_with_full_silent_gap() {
        let mut t = tracker();
        assert_eq!(
            t.observe(T + 1_000, 0),
            StallVerdict::StallStarted {
                stalled_ms: T + 1_000
            }
        );
        // The loop stamps again at 49_900: the recovery duration is the
        // whole silent gap (last stamp before the stall -> first after).
        assert_eq!(
            t.observe(50_000, 49_900),
            StallVerdict::Recovered { stalled_ms: 49_900 }
        );
        // Healthy afterwards while beats stay fresh.
        assert_eq!(t.observe(51_000, 50_900), StallVerdict::Healthy);
    }

    #[test]
    fn tracker_opens_new_episode_after_recovery() {
        let mut t = tracker();
        assert_eq!(
            t.observe(T + 1_000, 0),
            StallVerdict::StallStarted {
                stalled_ms: T + 1_000
            }
        );
        assert_eq!(
            t.observe(30_000, 29_000),
            StallVerdict::Recovered { stalled_ms: 29_000 }
        );
        // A second stall is a NEW episode: it must log (and dump) again.
        assert_eq!(
            t.observe(29_000 + T + 2_000, 29_000),
            StallVerdict::StallStarted {
                stalled_ms: T + 2_000
            }
        );
    }

    #[test]
    fn tracker_recovery_then_restall_between_checks() {
        let mut t = tracker();
        assert_eq!(
            t.observe(T + 1_000, 0),
            StallVerdict::StallStarted {
                stalled_ms: T + 1_000
            }
        );
        // Next check: the beat ADVANCED to 80_000 but is itself already
        // 20s stale — the loop recovered and wedged again between checks.
        // The old episode must close (with its true gap) before the new
        // one opens, so episode accounting never merges distinct stalls.
        assert_eq!(
            t.observe(100_000, 80_000),
            StallVerdict::Recovered { stalled_ms: 80_000 }
        );
        assert_eq!(
            t.observe(100_100, 80_000),
            StallVerdict::StallStarted { stalled_ms: 20_100 }
        );
    }

    #[test]
    fn probe_reports_all_free_locks() {
        let table = RwLock::new(1u32);
        let migration = Mutex::new(2u32);
        let addrs = RwLock::new(3u32);
        let fp = probe_coordinator_locks(&table, &migration, &addrs);
        assert_eq!(
            fp,
            LockFingerprint {
                shard_table_readable: true,
                migration_lockable: true,
                node_addrs_readable: true,
            }
        );
        assert_eq!(
            fp.describe(),
            "shard_table=free migration=free node_addrs=free"
        );
    }

    #[test]
    fn probe_fingers_held_locks_without_blocking() {
        let table = RwLock::new(1u32);
        let migration = Mutex::new(2u32);
        let addrs = RwLock::new(3u32);
        // Hold the shard table WRITE lock and the migration mutex; the
        // zero-wait probes must fail on those and succeed on node_addrs —
        // this is the "shard_table write-hold + migration held" fingerprint.
        let _table_guard = table.write();
        let _mig_guard = migration.lock();
        let started = Instant::now();
        let fp = probe_coordinator_locks(&table, &migration, &addrs);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "probe must be zero-wait even with locks held"
        );
        assert_eq!(
            fp,
            LockFingerprint {
                shard_table_readable: false,
                migration_lockable: false,
                node_addrs_readable: true,
            }
        );
        assert_eq!(
            fp.describe(),
            "shard_table=HELD migration=HELD node_addrs=free"
        );
    }

    #[test]
    fn probe_read_hold_does_not_finger_shard_table() {
        // A concurrent READER (e.g. /status) is not a write-hold: the
        // fingerprint must stay clean so only writer-side holds are blamed.
        let table = RwLock::new(1u32);
        let migration = Mutex::new(2u32);
        let addrs = RwLock::new(3u32);
        let _read_guard = table.read();
        let fp = probe_coordinator_locks(&table, &migration, &addrs);
        assert!(fp.shard_table_readable);
    }

    #[test]
    fn watchdog_thread_exits_on_shutdown() {
        let hb = Arc::new(LoopHeartbeat::new());
        let shutdown = Arc::new(AtomicBool::new(true));
        let handle = spawn_watchdog(
            hb,
            shutdown,
            Arc::new(RwLock::new(0u32)),
            Arc::new(Mutex::new(0u32)),
            Arc::new(RwLock::new(0u32)),
        );
        // Pre-set shutdown: the thread must exit after at most one poll
        // slice instead of running its 5s check cadence forever.
        let started = Instant::now();
        assert!(handle.join().is_ok());
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "watchdog must exit promptly on shutdown"
        );
    }
}
