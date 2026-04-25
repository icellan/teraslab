//! io_uring backend for Linux >= 5.6.
//!
//! Uses the `io-uring` crate (0.7.x) for direct control over the kernel's
//! submission and completion queues. Each submitted op carries a 64-bit
//! `user_data` tag so the caller can correlate completions with requests.
//!
//! This module is compiled only on Linux — the non-Linux case is handled
//! by `create_device_io` in the parent module, which simply bypasses the
//! ring-init attempt and constructs a `SyncFallback` directly. io_uring
//! is a Linux-kernel feature with no portable equivalent on macOS/BSD, so
//! this is a documented platform limitation rather than a stub.
//!
//! # Buffer lifetime invariant
//!
//! `submit_read`/`submit_write` borrow the `AlignedBuf` only for the duration
//! of the call, but the kernel reads/writes the raw buffer pointer at any
//! time between submission and completion. **The caller MUST keep the
//! `AlignedBuf` alive (i.e. not drop it, not reuse it, not let it move) until
//! the corresponding `user_data` is returned in a `Completion`.** The backend
//! does not retain any owning reference to the buffer; tracking outstanding
//! buffers is the caller's responsibility and is typically done by storing
//! them in a slab keyed by `user_data`.
//!
//! # Back-pressure
//!
//! When the submission queue is full, `submit_read`/`submit_write` return
//! [`std::io::ErrorKind::WouldBlock`]. The caller should then call
//! [`submit`](DeviceIo::submit) or [`submit_and_wait`](DeviceIo::submit_and_wait)
//! to drain completions and free SQ slots before retrying.

use super::{Completion, DeviceIo};
use crate::device::AlignedBuf;
use crate::metrics::io_uring_metrics;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use io_uring::{IoUring, opcode, squeue, types};

/// Human-readable identifier used by metrics to report the selected backend.
pub const BACKEND_ID: &str = "io_uring";

/// Fixed-size timestamp ring used to correlate SQE submissions with CQE
/// completions for latency measurement. Indexed by `user_data & MASK`.
///
/// A power-of-two size is required so the mask is cheap. Collisions (two
/// outstanding ops sharing the low `RING_BITS` bits of user_data) are
/// possible but have no correctness impact — the latency for the colliding
/// CQE is measured from the more recent SQE's timestamp, which is within
/// a single batch's lifetime.
const RING_BITS: u32 = 10;
const RING_SIZE: usize = 1 << RING_BITS; // 1024
const RING_MASK: u64 = (RING_SIZE as u64) - 1;

/// io_uring-based I/O backend for Linux >= 5.6.
///
/// Provides true asynchronous batched I/O via the kernel's io_uring
/// interface. Instantiated through `IoUringBackend::new(queue_depth)`.
pub struct IoUringBackend {
    ring: IoUring,
    /// Operations submitted but whose completions have not yet been drained.
    pending: usize,
    /// Configured queue depth (power of two, rounded up by the kernel).
    queue_depth: u32,
    /// Timestamp ring indexed by `user_data & RING_MASK` — each SQE push
    /// stores `Instant::now()` (encoded as nanoseconds since a fixed base)
    /// so the completion path can compute submit→complete latency.
    ///
    /// Zeros mean "no outstanding timestamp" which — because `now_ns()` is
    /// never exactly 0 — is an acceptable empty sentinel.
    ts_ring: Box<[AtomicU64; RING_SIZE]>,
    /// Common base `Instant` used to encode timestamps as compact `u64`
    /// nanoseconds. Recorded once at ring construction so subtraction is
    /// always monotonic.
    ts_base: Instant,
}

impl IoUringBackend {
    /// Create a new io_uring ring with the given queue depth.
    ///
    /// The kernel rounds the requested depth up to the next power of two.
    /// Returns an error (usually `ErrorKind::Unsupported`, `ErrorKind::PermissionDenied`,
    /// or ENOMEM) if io_uring cannot be set up on this kernel / ulimit.
    pub fn new(queue_depth: u32) -> Result<Self, std::io::Error> {
        // Clamp to at least 1 — IoUring::new rejects 0.
        let entries = queue_depth.max(1);
        let ring = IoUring::new(entries)?;
        // Allocate the timestamp ring on the heap so the `IoUringBackend`
        // stays cheap to move. Each cell is 8 bytes → 8 KiB total.
        let ts_ring: Box<[AtomicU64; RING_SIZE]> = {
            let v: Vec<AtomicU64> = (0..RING_SIZE).map(|_| AtomicU64::new(0)).collect();
            let boxed_slice: Box<[AtomicU64]> = v.into_boxed_slice();
            // Safety: boxed_slice has exactly RING_SIZE elements; converting
            // to a fixed-size array box is safe when the length matches.
            let ptr = Box::into_raw(boxed_slice) as *mut [AtomicU64; RING_SIZE];
            unsafe { Box::from_raw(ptr) }
        };
        Ok(Self {
            ring,
            pending: 0,
            queue_depth: entries,
            ts_ring,
            ts_base: Instant::now(),
        })
    }

    /// Encode `now` as nanoseconds since `self.ts_base`.
    #[inline(always)]
    fn now_ns(&self) -> u64 {
        self.ts_base.elapsed().as_nanos() as u64
    }

    /// Remember the submission time for `user_data`.
    #[inline(always)]
    fn record_submit_ts(&self, user_data: u64) {
        let idx = (user_data & RING_MASK) as usize;
        self.ts_ring[idx].store(self.now_ns(), Ordering::Relaxed);
    }

    /// Read and clear the submission time for `user_data`, returning the
    /// elapsed nanoseconds since submission. Returns `None` if the slot is
    /// empty (e.g. because a collision overwrote it) so the caller can
    /// skip recording a nonsensical latency.
    #[inline(always)]
    fn consume_submit_ts_from(
        ts_ring: &[AtomicU64; RING_SIZE],
        ts_base: Instant,
        user_data: u64,
    ) -> Option<u64> {
        let idx = (user_data & RING_MASK) as usize;
        let stored = ts_ring[idx].swap(0, Ordering::Relaxed);
        if stored == 0 {
            return None;
        }
        let now = ts_base.elapsed().as_nanos() as u64;
        Some(now.saturating_sub(stored))
    }

    /// Drain the completion queue into `out`, returning the number of completions harvested.
    ///
    /// Internal helper used by both `completions` (non-blocking) and
    /// `submit_and_wait` (after the kernel has guaranteed `min_complete`
    /// entries are ready).
    fn drain_completions(&mut self, out: &mut Vec<Completion>) -> usize {
        let metrics = io_uring_metrics();
        let ts_ring = &self.ts_ring;
        let ts_base = self.ts_base;
        let mut cq = self.ring.completion();
        cq.sync();
        let mut drained = 0usize;
        for cqe in &mut cq {
            let user_data = cqe.user_data();
            let result = cqe.result();
            if let Some(m) = metrics {
                if let Some(elapsed_ns) = Self::consume_submit_ts_from(ts_ring, ts_base, user_data)
                {
                    m.uring_completion_latency_ns.record_ns(elapsed_ns);
                }
                if result < 0 {
                    m.record_completion_error(result);
                }
            } else {
                // Drain the timestamp slot even when metrics aren't installed
                // so stale timestamps don't linger across resets.
                let _ = Self::consume_submit_ts_from(ts_ring, ts_base, user_data);
            }
            out.push(Completion { user_data, result });
            drained += 1;
        }
        // When `cq` is dropped, the updated head is stored back to the ring,
        // releasing the slots for the kernel to fill again.
        self.pending = self.pending.saturating_sub(drained);
        if let Some(m) = metrics {
            m.uring_pending
                .store(self.pending as u32, Ordering::Relaxed);
        }
        drained
    }

    fn push_sqe(&mut self, entry: &squeue::Entry, user_data: u64) -> Result<(), std::io::Error> {
        // Record the submission timestamp BEFORE pushing so a concurrent
        // completion on another op cannot race us to an empty slot. If the
        // push fails the slot is stale but will be overwritten by the next
        // successful submission on the same ring index.
        if io_uring_metrics().is_some() {
            self.record_submit_ts(user_data);
        }
        // SAFETY: The SQE's underlying buffer/fd are owned by the caller and the caller
        // has accepted the buffer-lifetime invariant documented at module level. The
        // entry itself is a local value owned by this stack frame; `push` copies it.
        let result = unsafe { self.ring.submission().push(entry) };
        match result {
            Ok(()) => {
                self.pending += 1;
                if let Some(m) = io_uring_metrics() {
                    m.uring_pending
                        .store(self.pending as u32, Ordering::Relaxed);
                }
                Ok(())
            }
            Err(_push_err) => Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "io_uring submission queue full — call submit/submit_and_wait to drain",
            )),
        }
    }
}

impl DeviceIo for IoUringBackend {
    fn submit_read(
        &mut self,
        fd: RawFd,
        buf: &mut AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<(), std::io::Error> {
        let len = buf.len();
        if len > u32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "buffer length exceeds u32::MAX",
            ));
        }
        let ptr = buf.as_mut_ptr();
        let entry = opcode::Read::new(types::Fd(fd), ptr, len as u32)
            .offset(offset)
            .build()
            .user_data(user_data);
        self.push_sqe(&entry, user_data)
    }

    fn submit_write(
        &mut self,
        fd: RawFd,
        buf: &AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<(), std::io::Error> {
        let len = buf.len();
        if len > u32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "buffer length exceeds u32::MAX",
            ));
        }
        let ptr = buf.as_ptr();
        let entry = opcode::Write::new(types::Fd(fd), ptr, len as u32)
            .offset(offset)
            .build()
            .user_data(user_data);
        self.push_sqe(&entry, user_data)
    }

    fn submit_and_wait(&mut self, min_complete: usize) -> Result<Vec<Completion>, std::io::Error> {
        // `submitter().submit_and_wait` both submits outstanding SQEs and
        // blocks until at least `min_complete` CQEs are available.
        let submit_start = Instant::now();
        let res = self.ring.submitter().submit_and_wait(min_complete);
        if let Some(m) = io_uring_metrics() {
            m.uring_submit_latency_ns.record_since(submit_start);
            if res.is_err() {
                m.uring_submit_errors_total.inc();
            }
        }
        res?;
        let mut out = Vec::with_capacity(min_complete.max(1));
        self.drain_completions(&mut out);
        Ok(out)
    }

    fn submit(&mut self) -> Result<(), std::io::Error> {
        let submit_start = Instant::now();
        let res = self.ring.submitter().submit();
        if let Some(m) = io_uring_metrics() {
            m.uring_submit_latency_ns.record_since(submit_start);
            if res.is_err() {
                m.uring_submit_errors_total.inc();
            }
        }
        res?;
        Ok(())
    }

    fn completions(&mut self) -> Vec<Completion> {
        let mut out = Vec::new();
        self.drain_completions(&mut out);
        out
    }

    fn pending(&self) -> usize {
        self.pending
    }

    fn backend_name(&self) -> &'static str {
        BACKEND_ID
    }
}

impl IoUringBackend {
    /// Configured queue depth (exposed for diagnostics / tests).
    pub fn queue_depth(&self) -> u32 {
        self.queue_depth
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_macros)]
mod tests {
    use super::*;
    use crate::device::AlignedBuf;
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use tempfile::NamedTempFile;

    const ALIGNMENT: usize = 4096;

    /// Attempt to construct an `IoUringBackend`. If io_uring is unavailable
    /// on this test host (old kernel, sandbox, ulimit), log and return `None`
    /// so the test finishes cleanly — a disabled environment is NOT a failed
    /// test. Returning the constructed backend otherwise.
    fn try_backend(depth: u32) -> Option<IoUringBackend> {
        match IoUringBackend::new(depth) {
            Ok(b) => Some(b),
            Err(e) => {
                eprintln!("io_uring unavailable on this host ({e}) — skipping test body");
                None
            }
        }
    }

    fn create_test_file(size: usize) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("tempfile");
        let data = vec![0u8; size];
        f.write_all(&data).expect("write zero fill");
        f.flush().expect("flush");
        f
    }

    #[test]
    fn submit_timestamp_consume_is_one_shot() {
        let ts_ring: Box<[AtomicU64; RING_SIZE]> = {
            let v: Vec<AtomicU64> = (0..RING_SIZE).map(|_| AtomicU64::new(0)).collect();
            let boxed_slice: Box<[AtomicU64]> = v.into_boxed_slice();
            let ptr = Box::into_raw(boxed_slice) as *mut [AtomicU64; RING_SIZE];
            unsafe { Box::from_raw(ptr) }
        };
        let ts_base = Instant::now();
        let user_data = 0x1ff;
        ts_ring[(user_data & RING_MASK) as usize].store(1, Ordering::Relaxed);

        assert!(IoUringBackend::consume_submit_ts_from(&ts_ring, ts_base, user_data).is_some());
        assert_eq!(
            IoUringBackend::consume_submit_ts_from(&ts_ring, ts_base, user_data),
            None,
            "completion timestamp slots must be cleared after the first consume",
        );
    }

    #[test]
    fn uring_write_then_read_roundtrip() {
        let Some(mut io) = try_backend(32) else {
            return;
        };
        let f = create_test_file(ALIGNMENT * 4);
        let fd = f.as_raw_fd();

        // Fill a buffer with a distinct pattern and write it.
        let mut wbuf = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        for i in 0..ALIGNMENT {
            wbuf[i] = (i % 251) as u8;
        }
        io.submit_write(fd, &wbuf, 0, 0xA5A5).expect("submit write");
        assert_eq!(io.pending(), 1);
        let completions = io.submit_and_wait(1).expect("submit_and_wait");
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].user_data, 0xA5A5);
        assert_eq!(
            completions[0].result, ALIGNMENT as i32,
            "full buffer should have been written"
        );
        assert_eq!(io.pending(), 0);
        // wbuf lifetime ends here — safe because we waited for completion.
        drop(wbuf);

        // Read it back and compare.
        let mut rbuf = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        io.submit_read(fd, &mut rbuf, 0, 0x5A5A)
            .expect("submit read");
        let completions = io.submit_and_wait(1).expect("submit_and_wait read");
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].user_data, 0x5A5A);
        assert_eq!(completions[0].result, ALIGNMENT as i32);
        for i in 0..ALIGNMENT {
            assert_eq!(rbuf[i], (i % 251) as u8, "byte {i} round-trip mismatch");
        }
    }

    #[test]
    fn uring_batch_submit_and_wait() {
        let Some(mut io) = try_backend(16) else {
            return;
        };
        let f = create_test_file(ALIGNMENT * 8);
        let fd = f.as_raw_fd();

        // Submit 8 writes in a single batch.
        let bufs: Vec<AlignedBuf> = (0..8u8)
            .map(|i| {
                let mut b = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
                for byte in b.iter_mut() {
                    *byte = i;
                }
                b
            })
            .collect();

        for (i, buf) in bufs.iter().enumerate() {
            io.submit_write(fd, buf, (i * ALIGNMENT) as u64, 100 + i as u64)
                .expect("submit write");
        }
        assert_eq!(io.pending(), 8);
        let comps = io.submit_and_wait(8).expect("submit_and_wait");
        assert_eq!(comps.len(), 8);
        for c in &comps {
            assert_eq!(c.result, ALIGNMENT as i32);
            assert!((100..108).contains(&c.user_data));
        }
        assert_eq!(io.pending(), 0);
        drop(bufs);

        // Read all 8 blocks back and verify each has the expected fill.
        let mut rbufs: Vec<AlignedBuf> = (0..8)
            .map(|_| AlignedBuf::new(ALIGNMENT, ALIGNMENT))
            .collect();
        for (i, buf) in rbufs.iter_mut().enumerate() {
            io.submit_read(fd, buf, (i * ALIGNMENT) as u64, 200 + i as u64)
                .expect("submit read");
        }
        let _ = io.submit_and_wait(8).expect("submit_and_wait read");
        for (i, buf) in rbufs.iter().enumerate() {
            assert!(
                buf.iter().all(|&b| b == i as u8),
                "block {i} mismatch: first byte = {}",
                buf[0]
            );
        }
    }

    #[test]
    fn uring_queue_full_returns_wouldblock() {
        // Small depth so we can saturate the SQ without an impractical loop.
        // The kernel may round up, so we keep pushing until we get WouldBlock.
        let Some(mut io) = try_backend(2) else {
            return;
        };
        let f = create_test_file(ALIGNMENT * 512);
        let fd = f.as_raw_fd();

        // Keep buffers alive for the whole test so kernel pointers stay valid
        // even though we never drain completions. Upper bound is generous.
        let mut bufs: Vec<AlignedBuf> = (0..4096)
            .map(|_| AlignedBuf::new(ALIGNMENT, ALIGNMENT))
            .collect();

        // Push reads until the SQ reports full.
        let mut saw_wouldblock = false;
        for (i, buf) in bufs.iter_mut().enumerate() {
            let offset = ((i % 512) * ALIGNMENT) as u64;
            match io.submit_read(fd, buf, offset, i as u64) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    saw_wouldblock = true;
                    break;
                }
                Err(e) => panic!("unexpected error kind {:?}: {e}", e.kind()),
            }
        }

        assert!(
            saw_wouldblock,
            "expected WouldBlock once the submission queue filled up"
        );

        // Drain via submit_and_wait so the buffers can be dropped safely.
        let pending_before = io.pending();
        assert!(pending_before > 0);
        let comps = io.submit_and_wait(pending_before).expect("drain");
        assert_eq!(comps.len(), pending_before);
        assert_eq!(io.pending(), 0);

        // After draining, we should be able to submit again.
        io.submit_read(fd, &mut bufs[0], 0, 9999)
            .expect("submit after drain");
        let _ = io.submit_and_wait(1).expect("final drain");
    }

    #[test]
    fn uring_completions_empty_when_idle() {
        let Some(mut io) = try_backend(8) else {
            return;
        };
        assert!(io.completions().is_empty());
        assert_eq!(io.pending(), 0);
    }

    #[test]
    fn uring_backend_name_is_io_uring() {
        let Some(io) = try_backend(4) else {
            return;
        };
        assert_eq!(io.backend_name(), "io_uring");
        assert_eq!(BACKEND_ID, "io_uring");
    }

    #[test]
    fn uring_nonblocking_completions_after_submit_and_wait() {
        // After submit_and_wait drains, completions() should return empty
        // even though we previously had pending ops.
        let Some(mut io) = try_backend(8) else {
            return;
        };
        let f = create_test_file(ALIGNMENT * 2);
        let fd = f.as_raw_fd();

        let wbuf = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        io.submit_write(fd, &wbuf, 0, 1).expect("submit");
        let comps = io.submit_and_wait(1).expect("wait");
        assert_eq!(comps.len(), 1);
        // CQ should now be empty.
        assert!(io.completions().is_empty());
        assert_eq!(io.pending(), 0);
    }

    /// Phase 5: after submitting a read and draining the completion, both
    /// submit-latency and completion-latency histograms must have at least
    /// one sample recorded.
    #[test]
    fn uring_submit_latency_recorded_on_linux() {
        use crate::metrics::{IoUringMetrics, init_io_uring_metrics, io_uring_metrics};
        use std::sync::OnceLock;

        static TEST_METRICS: OnceLock<IoUringMetrics> = OnceLock::new();
        let m_ref: &'static IoUringMetrics = TEST_METRICS.get_or_init(IoUringMetrics::new);
        init_io_uring_metrics(m_ref);
        let metrics = io_uring_metrics().expect("metrics installed");

        let Some(mut io) = try_backend(16) else {
            tracing::warn!("io_uring backend unavailable; skipping test body");
            return;
        };
        let f = create_test_file(ALIGNMENT * 2);
        let fd = f.as_raw_fd();

        let before_submit = metrics.uring_submit_latency_ns.count();
        let before_complete = metrics.uring_completion_latency_ns.count();

        let mut rbuf = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        io.submit_read(fd, &mut rbuf, 0, 0xBEEF)
            .expect("submit read");
        let comps = io.submit_and_wait(1).expect("wait");
        assert_eq!(comps.len(), 1);

        let after_submit = metrics.uring_submit_latency_ns.count();
        let after_complete = metrics.uring_completion_latency_ns.count();
        assert!(
            after_submit > before_submit,
            "uring_submit_latency_ns should record a sample",
        );
        assert!(
            after_complete > before_complete,
            "uring_completion_latency_ns should record a sample",
        );
    }
}
