//! Leader/follower group commit for the redo log.
//!
//! The redo log lives behind a `Mutex<RedoLog>`; appends are cheap (an in-memory
//! buffer extend plus a sequence draw) but the flush is an fsync. Without
//! coordination every concurrent batch RPC locks the log, appends, and fsyncs
//! one-at-a-time — so N concurrent writers cost N serial fsyncs, and write
//! throughput collapses to `1 / fsync_latency` regardless of concurrency.
//!
//! [`GroupCommit`] coalesces them. Concurrent committers stage their ops in a
//! shared pending queue (under a *fast* lock, never the RedoLog mutex) and wait.
//! Exactly one of them — the **leader** — drains the queue, appends every
//! staged submission to the log, and performs a **single** flush covering them
//! all, then hands each follower back its own sequence range. Followers that
//! arrive while the leader is mid-fsync simply land in the queue and are
//! absorbed into the leader's next round — so the fsync of round K covers every
//! writer that arrived during round K-1. Throughput becomes
//! `(concurrent writers × batch) / fsync_latency`, while durability is
//! unchanged: every committer still returns only after its entries are fsynced
//! (fsync-before-ack preserved).
//!
//! This is the coordinator the `#21` group-commit window note deferred: it
//! replaces the fixed `thread::sleep` window with condvar-driven leader/follower
//! coalescing, so there is no artificial latency floor.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::redo::{RedoLog, RedoOp};

/// Result of committing one submission: the `(first, last)` redo sequence range
/// it contributed, `None` if it had no sequence-bearing ops, or an error string
/// (the log was poisoned or the flush failed) — mirroring the routed
/// append/flush path's return type so callers are unchanged.
pub type CommitOutcome = Result<Option<(u64, u64)>, String>;

/// One queued commit request.
struct Submission {
    id: u64,
    ops: Vec<RedoOp>,
}

struct Inner {
    /// Submissions appended-but-not-yet-flushed, in submission order.
    pending: VecDeque<Submission>,
    /// Completed outcomes keyed by submission id, awaiting their follower to
    /// pick them up. Bounded by the number of concurrent committers.
    results: HashMap<u64, CommitOutcome>,
    /// True while a leader is draining + flushing. New committers see this and
    /// become followers instead of flushing themselves.
    flushing: bool,
    next_id: u64,
}

/// Leader/follower group-commit coordinator for one redo log.
pub struct GroupCommit {
    log: Arc<Mutex<RedoLog>>,
    inner: Mutex<Inner>,
    cv: Condvar,
    /// Buffered (relaxed) durability: when `true`, `commit` appends and returns
    /// WITHOUT fsync — durability is provided by a background flusher (and the
    /// checkpoint barrier) instead of per-commit. Trades a bounded crash-loss
    /// window (the un-flushed tail) for removing the fsync from the ack path.
    buffered: std::sync::atomic::AtomicBool,
}

impl GroupCommit {
    /// Wrap a redo log with a group-commit coordinator (strict durability).
    pub fn new(log: Arc<Mutex<RedoLog>>) -> Arc<Self> {
        Arc::new(Self {
            log,
            inner: Mutex::new(Inner {
                pending: VecDeque::new(),
                results: HashMap::new(),
                flushing: false,
                next_id: 0,
            }),
            cv: Condvar::new(),
            buffered: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Enable/disable buffered durability. Set once at startup from config.
    pub fn set_buffered(&self, buffered: bool) {
        self.buffered
            .store(buffered, std::sync::atomic::Ordering::Release);
    }

    /// Force the wrapped log durable (fsync). Used by the background flusher and
    /// the checkpoint barrier so buffered appends become durable. Idempotent and
    /// cheap when nothing is dirty.
    pub fn flush(&self) -> Result<(), String> {
        self.log
            .lock()
            .flush()
            .map_err(|e| format!("redo flush failed: {e}"))
    }

    /// The wrapped log, for paths that must lock it directly (checkpoint,
    /// secondary-index two-phase flush, recovery). They serialize with the
    /// coordinator on the same mutex; they simply do not coalesce with it.
    pub fn log(&self) -> &Arc<Mutex<RedoLog>> {
        &self.log
    }

    /// Durably append `ops` to the log, coalescing the fsync with any concurrent
    /// commits. Returns this submission's own `(first, last)` sequence range
    /// (or `Ok(None)` for empty `ops`). Returns `Err` if the log was poisoned,
    /// the flush failed, or the log is transiently full (`LogFull`). A `LogFull`
    /// error is retryable backpressure and does **not** poison the log — the
    /// submission is rejected atomically (nothing buffered) and succeeds on a
    /// retry once the checkpoint reclaims space.
    pub fn commit(&self, ops: Vec<RedoOp>) -> CommitOutcome {
        if ops.is_empty() {
            return Ok(None);
        }

        // Buffered durability: append under the log lock and return WITHOUT
        // fsync. Appends are cheap (in-memory buffer + sequence draw), so the
        // log mutex is held only briefly; the background flusher and checkpoint
        // make the appended entries durable. A bounded tail may be lost on crash
        // — the relaxed-durability contract.
        if self.buffered.load(std::sync::atomic::Ordering::Acquire) {
            let mut log = self.log.lock();
            return match log.append_atomic(&ops) {
                Ok(range) => Ok(range),
                Err(e @ crate::redo::RedoError::LogFull { .. }) => {
                    // Transient backpressure: the checkpoint reclaims space.
                    // `append_atomic` rejected the whole batch (nothing
                    // buffered), so the log is NOT poisoned — the caller retries
                    // once space frees. Poisoning here would turn a momentary
                    // full log into a permanent "restart required" wedge.
                    Err(format!("{e}"))
                }
                Err(e) => {
                    // A genuine fault (already-poisoned log): fail closed.
                    log.poison();
                    Err(format!("redo log append failed: {e}"))
                }
            };
        }

        let my_id = {
            let mut inner = self.inner.lock();
            let my_id = inner.next_id;
            inner.next_id += 1;
            inner.pending.push_back(Submission { id: my_id, ops });

            if inner.flushing {
                // Follower: a leader is already flushing and will absorb this
                // submission into its next round. Wait for the result.
                loop {
                    if let Some(outcome) = inner.results.remove(&my_id) {
                        return outcome;
                    }
                    self.cv.wait(&mut inner);
                }
            }

            // No leader running — become the leader.
            inner.flushing = true;
            my_id
        };

        // Leader loop: drain pending, do one append+flush per round, repeat
        // until nothing new arrived during the round. The fsync runs WITHOUT the
        // `inner` lock held, so followers keep staging into `pending` and are
        // picked up by the next round.
        let mut my_outcome: Option<CommitOutcome> = None;
        loop {
            let batch: Vec<Submission> = { self.inner.lock().pending.drain(..).collect() };

            if !batch.is_empty() {
                let outcomes = self.flush_batch(&batch);
                let mut inner = self.inner.lock();
                for (id, outcome) in outcomes {
                    if id == my_id {
                        my_outcome = Some(outcome);
                    } else {
                        inner.results.insert(id, outcome);
                    }
                }
                // Wake every follower so each can claim its result.
                self.cv.notify_all();
            }

            let mut inner = self.inner.lock();
            if inner.pending.is_empty() {
                // Nothing new arrived — release leadership and return. A
                // committer that locks `inner` after this sees `flushing == false`
                // and becomes the next leader itself.
                inner.flushing = false;
                drop(inner);
                return my_outcome.expect("leader always flushes its own submission");
            }
            // More work arrived during the flush — stay leader, next round.
        }
    }

    /// Append every submission's ops to the log and flush once. Returns the
    /// per-submission outcome.
    ///
    /// Each submission is appended **atomically** ([`RedoLog::append_atomic`]):
    /// it either fits entirely or is rejected whole, leaving nothing buffered.
    /// A rejected submission gets a transient `LogFull` error and is skipped —
    /// the log is **not** poisoned, because `LogFull` is reclaimed by the
    /// checkpoint and poisoning would wedge the node ("restart required") on a
    /// momentarily full log. Submissions that fit are made durable by the single
    /// flush; a small submission can still make progress in the same round that
    /// a large one is deferred. The only fatal append error is an
    /// already-poisoned log, which fails that submission. A flush I/O fault
    /// poisons (inside `flush`) and fails every appended submission.
    fn flush_batch(&self, batch: &[Submission]) -> Vec<(u64, CommitOutcome)> {
        let mut guard = self.log.lock();

        let mut outcomes: Vec<(u64, CommitOutcome)> = Vec::with_capacity(batch.len());
        let mut any_appended = false;
        for sub in batch {
            match guard.append_atomic(&sub.ops) {
                Ok(range) => {
                    if range.is_some() {
                        any_appended = true;
                    }
                    outcomes.push((sub.id, Ok(range)));
                }
                Err(e @ crate::redo::RedoError::LogFull { .. }) => {
                    // Transient: retryable backpressure, no poison. `append_atomic`
                    // buffered nothing for this submission.
                    outcomes.push((sub.id, Err(format!("{e}"))));
                }
                Err(e) => {
                    // Already-poisoned log: fail this submission, fail closed.
                    outcomes.push((sub.id, Err(format!("redo log append failed: {e}"))));
                }
            }
        }

        if any_appended && let Err(e) = guard.flush() {
            // `flush` poisoned + dropped the buffer, so every submission that
            // WAS appended loses durability and must fail. Submissions that
            // already hold a (non-durable) LogFull/poison error keep it.
            let msg = format!("redo log flush failed: {e}");
            return outcomes
                .into_iter()
                .map(|(id, oc)| (id, Err(oc.err().unwrap_or_else(|| msg.clone()))))
                .collect();
        }
        outcomes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::{BlockDevice, MemoryDevice, Result as DevResult};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// MemoryDevice wrapper that counts `sync_data`/`sync` calls so tests can
    /// assert how many fsyncs N concurrent commits actually cost.
    struct CountingDev {
        inner: MemoryDevice,
        syncs: AtomicUsize,
    }
    impl CountingDev {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap(),
                syncs: AtomicUsize::new(0),
            })
        }
    }
    impl BlockDevice for CountingDev {
        fn pread(&self, buf: &mut [u8], off: u64) -> DevResult<usize> {
            self.inner.pread(buf, off)
        }
        fn pwrite(&self, buf: &[u8], off: u64) -> DevResult<usize> {
            self.inner.pwrite(buf, off)
        }
        fn alignment(&self) -> usize {
            self.inner.alignment()
        }
        fn size(&self) -> u64 {
            self.inner.size()
        }
        fn sync(&self) -> DevResult<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            self.inner.sync()
        }
        fn sync_data(&self) -> DevResult<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            self.inner.sync_data()
        }
    }

    fn delete_op(byte: u8) -> RedoOp {
        RedoOp::Delete {
            tx_key: crate::index::TxKey { txid: [byte; 32] },
            record_offset: u64::from(byte) * 4096,
            record_size: 4096,
        }
    }

    fn open_log(dev: Arc<CountingDev>) -> Arc<Mutex<RedoLog>> {
        Arc::new(Mutex::new(
            RedoLog::open(dev as Arc<dyn BlockDevice>, 0, 4 * 1024 * 1024).unwrap(),
        ))
    }

    #[test]
    fn single_commit_returns_range_and_is_durable() {
        let dev = CountingDev::new();
        let gc = GroupCommit::new(open_log(dev.clone()));
        let before = dev.syncs.load(Ordering::SeqCst);

        let range = gc.commit(vec![delete_op(1)]).unwrap().expect("range");
        assert_eq!(range.0, range.1, "single op -> single sequence");
        assert_eq!(
            dev.syncs.load(Ordering::SeqCst) - before,
            1,
            "one commit -> exactly one fsync"
        );

        // Durable: the entry is recoverable.
        let entries = gc.log().lock().recover().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, range.0);
    }

    #[test]
    fn empty_commit_is_noop() {
        let dev = CountingDev::new();
        let gc = GroupCommit::new(open_log(dev.clone()));
        let before = dev.syncs.load(Ordering::SeqCst);
        assert!(gc.commit(vec![]).unwrap().is_none());
        assert_eq!(
            dev.syncs.load(Ordering::SeqCst),
            before,
            "empty commit must not flush"
        );
    }

    #[test]
    fn concurrent_commits_coalesce_and_get_distinct_ranges() {
        const N: usize = 16;
        let dev = CountingDev::new();
        let gc = GroupCommit::new(open_log(dev.clone()));
        let before = dev.syncs.load(Ordering::SeqCst);
        let barrier = Arc::new(std::sync::Barrier::new(N));

        let handles: Vec<_> = (0..N)
            .map(|i| {
                let gc = gc.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    gc.commit(vec![delete_op(i as u8)])
                        .expect("commit ok")
                        .expect("range")
                })
            })
            .collect();
        let mut ranges: Vec<(u64, u64)> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let fsyncs = dev.syncs.load(Ordering::SeqCst) - before;
        assert!(fsyncs >= 1, "must flush at least once");
        assert!(
            fsyncs < N,
            "group commit must coalesce: {N} concurrent commits used {fsyncs} fsyncs (expected < {N})"
        );

        // Every committer got a distinct single-sequence range, and together they
        // cover a contiguous block with no gaps or dups.
        ranges.sort();
        for (i, r) in ranges.iter().enumerate() {
            assert_eq!(r.0, r.1, "each commit had one op");
            if i > 0 {
                assert_eq!(r.0, ranges[i - 1].0 + 1, "ranges contiguous, no dup/gap");
            }
        }
        assert_eq!(ranges.len(), N);

        // All N entries are durable.
        let entries = gc.log().lock().recover().unwrap();
        assert_eq!(entries.len(), N);
    }

    #[test]
    fn multi_op_submission_range_spans_its_ops() {
        let dev = CountingDev::new();
        let gc = GroupCommit::new(open_log(dev.clone()));
        let range = gc
            .commit(vec![delete_op(1), delete_op(2), delete_op(3)])
            .unwrap()
            .expect("range");
        assert_eq!(range.1 - range.0, 2, "3 ops -> span of 3 sequences");
        assert_eq!(gc.log().lock().recover().unwrap().len(), 3);
    }

    #[test]
    fn buffered_commit_does_not_fsync_until_flush() {
        // Buffered durability: commit appends + returns its range WITHOUT an
        // fsync, so the per-commit sync count stays flat. An explicit flush()
        // (what the background flusher / checkpoint call) then fsyncs once and
        // the entries become recoverable.
        let dev = CountingDev::new();
        let gc = GroupCommit::new(open_log(dev.clone()));
        gc.set_buffered(true);
        let before = dev.syncs.load(Ordering::SeqCst);

        let r1 = gc.commit(vec![delete_op(1)]).unwrap().expect("range");
        let r2 = gc.commit(vec![delete_op(2)]).unwrap().expect("range");
        assert_eq!(
            dev.syncs.load(Ordering::SeqCst) - before,
            0,
            "buffered commits must NOT fsync"
        );
        assert_eq!(r2.0, r1.0 + 1, "sequences still assigned, contiguous");

        // Explicit flush -> exactly one fsync, both entries durable.
        gc.flush().expect("flush ok");
        assert_eq!(
            dev.syncs.load(Ordering::SeqCst) - before,
            1,
            "flush fsyncs once for the buffered batch"
        );
        let entries = gc.log().lock().recover().unwrap();
        assert_eq!(
            entries.len(),
            2,
            "both buffered entries recoverable after flush"
        );
    }

    /// Build a GroupCommit over a small log so a fill loop reaches `LogFull`
    /// quickly. Returns both the coordinator and the shared log handle so a
    /// test can reclaim space directly.
    fn small_log_gc() -> (Arc<GroupCommit>, Arc<Mutex<RedoLog>>) {
        let dev = Arc::new(MemoryDevice::new(16 * 1024, 4096).unwrap());
        let log = Arc::new(Mutex::new(
            RedoLog::open(dev as Arc<dyn BlockDevice>, 0, 16 * 1024).unwrap(),
        ));
        (GroupCommit::new(log.clone()), log)
    }

    /// Fill the log via single-op commits until one reports `LogFull`. Returns
    /// the error so the caller can assert it is the transient full-log error.
    fn fill_until_full(gc: &GroupCommit) -> String {
        for i in 0..8192u32 {
            if let Err(e) = gc.commit(vec![delete_op((i % 251) as u8)]) {
                return e;
            }
        }
        panic!("log did not fill within the loop");
    }

    #[test]
    fn commit_logfull_does_not_poison_log() {
        // Strict (group) path: a full log returns a transient error, but the
        // log must NOT be poisoned. After reclaiming space, a commit succeeds —
        // a poisoned log would keep failing forever regardless of free space.
        let (gc, log) = small_log_gc();
        let err = fill_until_full(&gc);
        assert!(
            err.contains("redo log full"),
            "fill should end on a transient LogFull, got: {err}"
        );

        log.lock().reset().expect("reclaim space");
        let range = gc
            .commit(vec![delete_op(7)])
            .expect("log must accept writes again after reclaim — not poisoned")
            .expect("range");
        assert_eq!(range.0, range.1, "single op -> single sequence");
        assert_eq!(
            gc.log().lock().recover().unwrap().len(),
            1,
            "the post-reclaim commit is durable"
        );
    }

    #[test]
    fn buffered_commit_logfull_does_not_poison_log() {
        // Buffered path: same invariant. Buffered commits append without fsync,
        // so the buffer fills; a full log returns a transient error and the log
        // stays live — after reclaim a buffered commit + flush succeeds.
        let (gc, log) = small_log_gc();
        gc.set_buffered(true);
        let err = fill_until_full(&gc);
        assert!(
            err.contains("redo log full"),
            "buffered fill should end on a transient LogFull, got: {err}"
        );

        log.lock().reset().expect("reclaim space");
        gc.commit(vec![delete_op(7)])
            .expect("buffered log must accept writes again after reclaim");
        gc.flush().expect("flush ok");
        assert_eq!(
            gc.log().lock().recover().unwrap().len(),
            1,
            "the post-reclaim buffered commit is durable after flush"
        );
    }

    #[test]
    fn flush_failure_propagates_error() {
        // A device whose sync fails once armed -> flush fails -> commit errors
        // and the log is poisoned (next commit also errors). The failure is
        // armed only AFTER open (open writes+syncs the initial header).
        struct SyncFailDev {
            inner: MemoryDevice,
            armed: AtomicUsize, // 0 = ok, 1 = fail syncs
        }
        impl SyncFailDev {
            fn fail(&self) -> DevResult<()> {
                if self.armed.load(Ordering::SeqCst) == 1 {
                    Err(crate::device::DeviceError::Io(std::io::Error::other(
                        "boom",
                    )))
                } else {
                    Ok(())
                }
            }
        }
        impl BlockDevice for SyncFailDev {
            fn pread(&self, b: &mut [u8], o: u64) -> DevResult<usize> {
                self.inner.pread(b, o)
            }
            fn pwrite(&self, b: &[u8], o: u64) -> DevResult<usize> {
                self.inner.pwrite(b, o)
            }
            fn alignment(&self) -> usize {
                self.inner.alignment()
            }
            fn size(&self) -> u64 {
                self.inner.size()
            }
            fn sync(&self) -> DevResult<()> {
                self.fail()?;
                self.inner.sync()
            }
            fn sync_data(&self) -> DevResult<()> {
                self.fail()?;
                self.inner.sync_data()
            }
        }
        let dev = Arc::new(SyncFailDev {
            inner: MemoryDevice::new(1024 * 1024, 4096).unwrap(),
            armed: AtomicUsize::new(0),
        });
        let log = Arc::new(Mutex::new(
            RedoLog::open(dev.clone() as Arc<dyn BlockDevice>, 0, 1024 * 1024).unwrap(),
        ));
        dev.armed.store(1, Ordering::SeqCst); // now make every sync fail
        let gc = GroupCommit::new(log);
        assert!(
            gc.commit(vec![delete_op(1)]).is_err(),
            "flush failure -> Err"
        );
        // Poisoned now: subsequent commit also fails.
        assert!(
            gc.commit(vec![delete_op(2)]).is_err(),
            "log poisoned after flush failure"
        );
    }
}
