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
use std::os::fd::RawFd;

use io_uring::{IoUring, opcode, squeue, types};

/// Human-readable identifier used by metrics to report the selected backend.
pub const BACKEND_ID: &str = "io_uring";

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
        Ok(Self {
            ring,
            pending: 0,
            queue_depth: entries,
        })
    }

    /// Drain the completion queue into `out`, returning the number of completions harvested.
    ///
    /// Internal helper used by both `completions` (non-blocking) and
    /// `submit_and_wait` (after the kernel has guaranteed `min_complete`
    /// entries are ready).
    fn drain_completions(&mut self, out: &mut Vec<Completion>) -> usize {
        let mut cq = self.ring.completion();
        cq.sync();
        let mut drained = 0usize;
        for cqe in &mut cq {
            out.push(Completion {
                user_data: cqe.user_data(),
                result: cqe.result(),
            });
            drained += 1;
        }
        // When `cq` is dropped, the updated head is stored back to the ring,
        // releasing the slots for the kernel to fill again.
        self.pending = self.pending.saturating_sub(drained);
        drained
    }

    fn push_sqe(&mut self, entry: &squeue::Entry) -> Result<(), std::io::Error> {
        // SAFETY: The SQE's underlying buffer/fd are owned by the caller and the caller
        // has accepted the buffer-lifetime invariant documented at module level. The
        // entry itself is a local value owned by this stack frame; `push` copies it.
        let result = unsafe { self.ring.submission().push(entry) };
        match result {
            Ok(()) => {
                self.pending += 1;
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
        self.push_sqe(&entry)
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
        self.push_sqe(&entry)
    }

    fn submit_and_wait(&mut self, min_complete: usize) -> Result<Vec<Completion>, std::io::Error> {
        // `submitter().submit_and_wait` both submits outstanding SQEs and
        // blocks until at least `min_complete` CQEs are available.
        self.ring.submitter().submit_and_wait(min_complete)?;
        let mut out = Vec::with_capacity(min_complete.max(1));
        self.drain_completions(&mut out);
        Ok(out)
    }

    fn submit(&mut self) -> Result<(), std::io::Error> {
        self.ring.submitter().submit()?;
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
                eprintln!(
                    "io_uring unavailable on this host ({e}) — skipping test body"
                );
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
            completions[0].result,
            ALIGNMENT as i32,
            "full buffer should have been written"
        );
        assert_eq!(io.pending(), 0);
        // wbuf lifetime ends here — safe because we waited for completion.
        drop(wbuf);

        // Read it back and compare.
        let mut rbuf = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        io.submit_read(fd, &mut rbuf, 0, 0x5A5A).expect("submit read");
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
}
