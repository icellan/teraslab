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
    /// Serializes flushers (the background flusher + the checkpoint barrier) so
    /// the prepare→pwrite→fsync sequence runs in order — header blocks (which
    /// carry the sequence high-water) are never written out of order. Committers
    /// do NOT take this lock; they only ever contend on `log` for the O(1)
    /// in-memory append, never the pwrite/fsync (which run under `flush_guard`
    /// with `log` released).
    flush_guard: Mutex<()>,
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
            flush_guard: Mutex::new(()),
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
    ///
    /// Lever 6(b): the (slow, ~ms `F_FULLFSYNC`) device barrier runs WITHOUT the
    /// log mutex held. The buffered entries + header are pwritten under the lock
    /// ([`RedoLog::flush_pwrite_no_sync`], advancing `write_pos` so concurrent
    /// appenders resume at the new position), the lock is released, and only then
    /// is the fsync issued on a captured device handle. Without this, every
    /// committer serialized behind whoever was mid-fsync, capping write
    /// throughput at `1 / fsync_latency`.
    pub fn flush(&self) -> Result<(), String> {
        // Serialize flushers so prepared chunks are pwritten in cursor order and
        // header blocks never regress. Committers never take this lock.
        let _fg = self.flush_guard.lock();

        // Phase 1 (under the log lock, O(1)): drain the buffer into device-ready
        // blocks and advance the cursor. Release the lock immediately after.
        let (prepared, dev) = {
            let mut log = self.log.lock();
            let prepared = log
                .prepare_flush()
                .map_err(|e| format!("redo flush failed: {e}"))?;
            (prepared, log.device_handle())
        };
        let Some(prepared) = prepared else {
            return Ok(()); // nothing buffered
        };

        // Phase 2 (WITHOUT the log lock): the slow O_DIRECT pwrite of the entries
        // + header. Concurrent committers keep appending under `log` meanwhile.
        if let Err(e) = RedoLog::commit_flush(&dev, &prepared) {
            if let Some(m) = crate::metrics::redo_metrics() {
                m.redo_flush_errors_total.inc();
            }
            // The buffer was already drained in Phase 1; poison so the node fails
            // closed (recovery replays the durable prefix on restart).
            self.log.lock().poison();
            return Err(format!("redo flush failed: {e}"));
        }

        crate::fault_injection::check(crate::fault_injection::SyncPoint::BeforeRedoFsync);
        let sync_start = std::time::Instant::now();
        let sync_res = dev.sync_data();
        if let Some(m) = crate::metrics::redo_metrics() {
            m.redo_flush_latency_ns.record_since(sync_start);
        }
        if let Err(e) = sync_res {
            if let Some(m) = crate::metrics::redo_metrics() {
                m.redo_flush_errors_total.inc();
            }
            // The pwritten tail may not be durable — poison so the node fails
            // closed (recovery replays the durable prefix on restart).
            self.log.lock().poison();
            return Err(format!("redo flush failed: {e}"));
        }
        crate::fault_injection::check(crate::fault_injection::SyncPoint::AfterRedoFsync);
        Ok(())
    }

    /// Pwrite the buffered entries + header to the device but do NOT issue the
    /// durability fsync.
    ///
    /// Used by the background flusher ONLY under the relaxed `redo_buffered_io`
    /// mode. The bytes are pushed into the OS page cache (the redo device is
    /// opened buffered, i.e. without `O_DIRECT`/`F_NOCACHE`, in that mode), and
    /// durability is provided by (a) the kernel's page-cache writeback and (b)
    /// the checkpoint barrier's explicit redo fsync via [`Self::flush`]
    /// ([`crate::ops::engine::Engine::flush_all_redo`]) BEFORE it fences and
    /// reclaims the log. Removing the per-flush fsync here is therefore safe for
    /// reclamation: the prefix is reclaimed only after the barrier has fsynced
    /// the redo, exactly as under strict durability.
    ///
    /// This advances `write_pos` (so a concurrent appender resumes at the new
    /// position) and moves the pending entries into the read cache, identically
    /// to [`Self::flush`] — only the trailing `sync_data()` is skipped. On a
    /// pwrite error the log is poisoned (fail-closed), matching [`Self::flush`].
    pub fn flush_no_sync(&self) -> Result<(), String> {
        // Serialize flushers so prepared chunks are pwritten in cursor order and
        // header blocks never regress — same guard as `flush`.
        let _fg = self.flush_guard.lock();

        let (prepared, dev) = {
            let mut log = self.log.lock();
            let prepared = log
                .prepare_flush()
                .map_err(|e| format!("redo flush failed: {e}"))?;
            (prepared, log.device_handle())
        };
        let Some(prepared) = prepared else {
            return Ok(()); // nothing buffered
        };

        if let Err(e) = RedoLog::commit_flush(&dev, &prepared) {
            if let Some(m) = crate::metrics::redo_metrics() {
                m.redo_flush_errors_total.inc();
            }
            // The buffer was already drained in `prepare_flush`; poison so the
            // node fails closed (recovery replays the durable prefix on restart).
            self.log.lock().poison();
            return Err(format!("redo flush failed: {e}"));
        }
        // Intentionally NO `dev.sync_data()` here — see the doc comment.
        Ok(())
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
        // fsync. The background flusher and checkpoint make the appended entries
        // durable. A bounded tail may be lost on crash — the relaxed-durability
        // contract.
        //
        // E7: the op encode + heap allocation (the expensive part — ~167µs in
        // lock under contention) is done OUTSIDE the lock via `pre_encode`; under
        // the lock only the O(1) finalize (sequence patch + CRC + memcpy) runs,
        // so the per-store redo mutex stops being the write-concurrency cap.
        if self.buffered.load(std::sync::atomic::Ordering::Acquire) {
            let pre: Vec<_> = ops
                .into_iter()
                .map(crate::redo::RedoEntry::pre_encode)
                .collect();
            let wait_start = std::time::Instant::now();
            let mut log = self.log.lock();
            if let Some(m) = crate::metrics::redo_metrics() {
                m.redo_commit_lock_wait_ns.record_since(wait_start);
            }
            return match log.append_preencoded_atomic(pre) {
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    #[test]
    fn flush_no_sync_pwrites_without_fsync_but_flush_does_fsync() {
        // `redo_buffered_io`: the background flusher's no-sync flush pwrites the
        // buffered entries (so they are in-process readable / page-cache visible)
        // but issues ZERO device fsyncs. A real `flush()` (the checkpoint barrier
        // / shutdown path) DOES fsync. Together these are the durability gate the
        // no-sync periodic flush relies on.
        let dev = CountingDev::new();
        let gc = GroupCommit::new(open_log(dev.clone()));
        gc.set_buffered(true);

        // Batch 1: buffered append, then a NO-SYNC flush.
        gc.commit(vec![delete_op(1)]).unwrap().expect("range");
        gc.commit(vec![delete_op(2)]).unwrap().expect("range");
        let before = dev.syncs.load(Ordering::SeqCst);
        gc.flush_no_sync().expect("no-sync flush ok");
        assert_eq!(
            dev.syncs.load(Ordering::SeqCst) - before,
            0,
            "flush_no_sync must NOT issue any device fsync"
        );
        // The pwritten entries are readable (the bytes are on the device, just
        // not yet fsynced) — recovery would replay them if they survive.
        assert_eq!(
            gc.log().lock().recover().unwrap().len(),
            2,
            "flush_no_sync still pwrites the entries"
        );

        // A second no-sync flush with nothing buffered is a clean no-op (0 sync).
        gc.flush_no_sync().expect("empty no-sync flush ok");
        assert_eq!(
            dev.syncs.load(Ordering::SeqCst) - before,
            0,
            "an empty no-sync flush touches the device zero times"
        );

        // Batch 2: a real flush DOES fsync (exactly once for the buffered batch).
        gc.commit(vec![delete_op(3)]).unwrap().expect("range");
        let before2 = dev.syncs.load(Ordering::SeqCst);
        gc.flush().expect("flush ok");
        assert_eq!(
            dev.syncs.load(Ordering::SeqCst) - before2,
            1,
            "flush() fsyncs once even when flush_no_sync was used earlier"
        );
        assert_eq!(
            gc.log().lock().recover().unwrap().len(),
            3,
            "all entries recoverable after the fsyncing flush"
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

    /// Lever 6(b): `GroupCommit::flush` must release the log mutex BEFORE the
    /// device fsync so a concurrent buffered commit can append while a slow
    /// fsync is in flight. A device whose `sync_data` blocks until signaled lets
    /// us prove it: one thread starts a flush (entering the blocked sync holding
    /// NO lock), and a commit on another thread must complete before the sync is
    /// released. Pre-fix (fsync under the lock) the commit would block on the
    /// log mutex until the sync returned → this test would time out.
    #[test]
    fn buffered_flush_releases_lock_before_fsync() {
        use std::sync::mpsc;
        use std::time::Duration;

        struct BlockingSyncDev {
            inner: MemoryDevice,
            armed: AtomicBool,   // false during open() so the header sync passes
            in_sync: AtomicBool, // true while a sync is parked
            release: AtomicBool, // set true to let a parked sync return
        }
        impl BlockingSyncDev {
            fn park(&self) {
                if self.armed.load(Ordering::SeqCst) {
                    self.in_sync.store(true, Ordering::SeqCst);
                    while !self.release.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    self.in_sync.store(false, Ordering::SeqCst);
                }
            }
        }
        impl BlockDevice for BlockingSyncDev {
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
                self.park();
                self.inner.sync()
            }
            fn sync_data(&self) -> DevResult<()> {
                self.park();
                self.inner.sync_data()
            }
        }

        let dev = Arc::new(BlockingSyncDev {
            inner: MemoryDevice::new(1024 * 1024, 4096).unwrap(),
            armed: AtomicBool::new(false),
            in_sync: AtomicBool::new(false),
            release: AtomicBool::new(false),
        });
        let log = Arc::new(Mutex::new(
            RedoLog::open(dev.clone() as Arc<dyn BlockDevice>, 0, 1024 * 1024).unwrap(),
        ));
        let gc = GroupCommit::new(log);
        gc.set_buffered(true);
        gc.commit(vec![delete_op(1)]).expect("buffered append");
        // Arm the blocking sync now that open()'s header sync is done.
        dev.armed.store(true, Ordering::SeqCst);

        // T1: flush enters the parked fsync — holding NO log lock if the fix works.
        let gc1 = gc.clone();
        let t1 = std::thread::spawn(move || gc1.flush());
        // Wait until the fsync is parked.
        let mut waited = 0;
        while !dev.in_sync.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(1));
            waited += 1;
            assert!(waited < 5000, "flush never reached the device fsync");
        }

        // T2: a buffered commit must complete while the fsync is still parked.
        let (tx, rx) = mpsc::channel();
        let gc2 = gc.clone();
        let t2 = std::thread::spawn(move || {
            let r = gc2.commit(vec![delete_op(2)]);
            let _ = tx.send(r);
        });
        let outcome = rx.recv_timeout(Duration::from_secs(5));
        // Release the fsync regardless so the threads can finish.
        dev.release.store(true, Ordering::SeqCst);
        t1.join().unwrap().expect("flush ok");
        t2.join().unwrap();

        let committed = outcome.expect(
            "a buffered commit blocked behind an in-flight fsync — flush held the log lock \
             across the device barrier",
        );
        committed.expect("the concurrent commit itself succeeded");

        // Both entries are durable after a final flush.
        gc.flush().expect("final flush");
        assert_eq!(gc.log().lock().recover().unwrap().len(), 2);
    }

    #[test]
    fn buffered_flush_releases_lock_before_pwrite() {
        // Lever (write-concurrency): the slow O_DIRECT *pwrite* of the buffered
        // entries must run WITHOUT the log mutex held, so a concurrent buffered
        // commit (which only needs the lock for an in-memory append) completes
        // while a flush is mid-pwrite. Profiling showed ~24ms `create_redo`
        // under 60 concurrent writers came from committers blocking behind the
        // pwrite-under-lock. Mirrors `buffered_flush_releases_lock_before_fsync`
        // but parks the pwrite instead of the fsync.
        use std::sync::mpsc;
        use std::time::Duration;

        struct BlockingPwriteDev {
            inner: MemoryDevice,
            armed: AtomicBool,     // false during open() so header pwrites pass
            in_pwrite: AtomicBool, // true while a pwrite is parked
            release: AtomicBool,   // set true to let a parked pwrite return
        }
        impl BlockingPwriteDev {
            fn park(&self) {
                if self.armed.load(Ordering::SeqCst) {
                    self.in_pwrite.store(true, Ordering::SeqCst);
                    while !self.release.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    self.in_pwrite.store(false, Ordering::SeqCst);
                }
            }
        }
        impl BlockDevice for BlockingPwriteDev {
            fn pread(&self, b: &mut [u8], o: u64) -> DevResult<usize> {
                self.inner.pread(b, o)
            }
            fn pwrite(&self, b: &[u8], o: u64) -> DevResult<usize> {
                self.park();
                self.inner.pwrite(b, o)
            }
            fn alignment(&self) -> usize {
                self.inner.alignment()
            }
            fn size(&self) -> u64 {
                self.inner.size()
            }
            fn sync(&self) -> DevResult<()> {
                self.inner.sync()
            }
            fn sync_data(&self) -> DevResult<()> {
                self.inner.sync_data()
            }
        }

        let dev = Arc::new(BlockingPwriteDev {
            inner: MemoryDevice::new(1024 * 1024, 4096).unwrap(),
            armed: AtomicBool::new(false),
            in_pwrite: AtomicBool::new(false),
            release: AtomicBool::new(false),
        });
        let log = Arc::new(Mutex::new(
            RedoLog::open(dev.clone() as Arc<dyn BlockDevice>, 0, 1024 * 1024).unwrap(),
        ));
        let gc = GroupCommit::new(log);
        gc.set_buffered(true);
        gc.commit(vec![delete_op(1)]).expect("buffered append");
        // Arm the blocking pwrite now that open()'s header pwrites are done.
        dev.armed.store(true, Ordering::SeqCst);

        // T1: flush enters the parked pwrite — holding NO log lock if the fix works.
        let gc1 = gc.clone();
        let t1 = std::thread::spawn(move || gc1.flush());
        let mut waited = 0;
        while !dev.in_pwrite.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(1));
            waited += 1;
            assert!(waited < 5000, "flush never reached the device pwrite");
        }

        // T2: a buffered commit must complete while the pwrite is still parked.
        let (tx, rx) = mpsc::channel();
        let gc2 = gc.clone();
        let t2 = std::thread::spawn(move || {
            let r = gc2.commit(vec![delete_op(2)]);
            let _ = tx.send(r);
        });
        let outcome = rx.recv_timeout(Duration::from_secs(5));
        dev.release.store(true, Ordering::SeqCst);
        t1.join().unwrap().expect("flush ok");
        t2.join().unwrap();

        let committed = outcome.expect(
            "a buffered commit blocked behind an in-flight pwrite — flush held the log lock \
             across the device pwrite",
        );
        committed.expect("the concurrent commit itself succeeded");

        // Both entries are durable after a final flush.
        gc.flush().expect("final flush");
        assert_eq!(gc.log().lock().recover().unwrap().len(), 2);
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
