//! Synchronous fallback for systems without io_uring.
//!
//! Provides the exact same `DeviceIo` trait surface but executes each
//! operation synchronously with `libc::pread` / `libc::pwrite`.

use super::{Completion, DeviceIo};
use crate::device::AlignedBuf;
use std::os::fd::RawFd;

/// Stable identifier reported by [`DeviceIo::backend_name`] for this backend.
pub const BACKEND_ID: &str = "sync";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpKind {
    Read,
    Write,
}

struct PendingOp {
    kind: OpKind,
    fd: RawFd,
    buf_ptr: *mut u8,
    len: usize,
    offset: u64,
    user_data: u64,
}

// SAFETY: PendingOp contains raw pointers that are valid for the duration
// of the submit_and_wait call. The caller guarantees buffer lifetime.
unsafe impl Send for PendingOp {}
unsafe impl Sync for PendingOp {}

/// Synchronous I/O backend using `libc::pread` / `libc::pwrite`.
///
/// Operations are recorded in `submit_read`/`submit_write` and executed
/// sequentially in `submit_and_wait`, matching the batching semantics
/// of `IoUringBackend`.
pub struct SyncFallback {
    pending: Vec<PendingOp>,
}

impl SyncFallback {
    /// Create a new synchronous fallback.
    ///
    /// `queue_depth` pre-sizes the internal pending-op buffer so the first
    /// batch up to the configured depth does not reallocate. Clamped at
    /// 4096 to bound worst-case memory if a caller passes an absurd value
    /// — matches the practical ceiling on the io_uring backend's SQ.
    ///
    /// F-G1-011: previously the parameter was ignored entirely
    /// (`_queue_depth: u32`), so the contract of `create_device_io` —
    /// where the caller passes a queue depth expecting it to size the
    /// backend — silently did nothing on the sync path. The cost was a
    /// few extra `Vec` reallocations on the first batch; the contract
    /// gap was the bigger concern.
    pub fn new(queue_depth: u32) -> Result<Self, std::io::Error> {
        let cap = (queue_depth as usize).min(4096);
        Ok(Self {
            pending: Vec::with_capacity(cap),
        })
    }
}

impl DeviceIo for SyncFallback {
    fn submit_read(
        &mut self,
        fd: RawFd,
        buf: &mut AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<(), std::io::Error> {
        // F-G1-010: reject zero-length buffers symmetrically with
        // `IoUringBackend::submit_read`. The pointer would be dangling
        // (`NonNull::dangling()`) and libc::pread with len=0 is a
        // documented no-op, but the contract on `submit_read` says the
        // caller-supplied buffer is the I/O target — passing a
        // dangling pointer through is a footgun that should fail
        // loudly.
        if buf.len() == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "submit_read: zero-length buffer (dangling pointer)",
            ));
        }
        self.pending.push(PendingOp {
            kind: OpKind::Read,
            fd,
            buf_ptr: buf.as_mut_ptr(),
            len: buf.len(),
            offset,
            user_data,
        });
        Ok(())
    }

    fn submit_write(
        &mut self,
        fd: RawFd,
        buf: &AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<(), std::io::Error> {
        // F-G1-010: see submit_read above.
        if buf.len() == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "submit_write: zero-length buffer (dangling pointer)",
            ));
        }
        self.pending.push(PendingOp {
            kind: OpKind::Write,
            fd,
            buf_ptr: buf.as_ptr() as *mut u8,
            len: buf.len(),
            offset,
            user_data,
        });
        Ok(())
    }

    fn submit_and_wait(&mut self, _min_complete: usize) -> Result<Vec<Completion>, std::io::Error> {
        let mut completions = Vec::with_capacity(self.pending.len());
        for op in self.pending.drain(..) {
            let raw_result = match op.kind {
                OpKind::Read => unsafe {
                    libc::pread(
                        op.fd,
                        op.buf_ptr as *mut libc::c_void,
                        op.len,
                        op.offset as libc::off_t,
                    )
                },
                OpKind::Write => unsafe {
                    libc::pwrite(
                        op.fd,
                        op.buf_ptr as *const libc::c_void,
                        op.len,
                        op.offset as libc::off_t,
                    )
                },
            };
            // F-G1-001: On error pread/pwrite returns -1 and sets errno.
            // The `DeviceIo::Completion::result` contract (see
            // `device_io/mod.rs::Completion::result`) is "bytes
            // transferred, or **negative errno** on failure" — matching
            // the io_uring CQE encoding so downstream telemetry
            // (`record_completion_error` in the io_uring backend) and
            // any caller that translates `result` to
            // `io::Error::from_raw_os_error(-result)` produce the same
            // value across both backends. Before this fix every distinct
            // I/O failure (EBADF / EIO / ENOSPC / EAGAIN) collapsed to
            // a single `-1`. Now we read errno and stamp `-errno` so
            // callers can distinguish them.
            let result = if raw_result < 0 {
                let errno = std::io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO);
                -errno
            } else {
                raw_result as i32
            };
            completions.push(Completion {
                user_data: op.user_data,
                result,
            });
        }
        Ok(completions)
    }

    fn submit(&mut self) -> Result<(), std::io::Error> {
        // No-op for sync backend; ops execute in submit_and_wait
        Ok(())
    }

    fn completions(&mut self) -> Vec<Completion> {
        // Always empty — sync ops complete immediately in submit_and_wait
        Vec::new()
    }

    fn pending(&self) -> usize {
        self.pending.len()
    }

    fn backend_name(&self) -> &'static str {
        BACKEND_ID
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

    fn create_test_file(size: usize) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![0u8; size];
        f.write_all(&data).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn sync_single_write_then_read() {
        let f = create_test_file(ALIGNMENT * 4);
        let fd = f.as_raw_fd();
        let mut io = SyncFallback::new(32).unwrap();

        // Write known pattern
        let mut wbuf = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        for i in 0..ALIGNMENT {
            wbuf[i] = (i % 251) as u8;
        }
        io.submit_write(fd, &wbuf, 0, 100).unwrap();
        assert_eq!(io.pending(), 1);
        let completions = io.submit_and_wait(1).unwrap();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].user_data, 100);
        assert_eq!(completions[0].result, ALIGNMENT as i32);

        // Read it back
        let mut rbuf = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        io.submit_read(fd, &mut rbuf, 0, 200).unwrap();
        let completions = io.submit_and_wait(1).unwrap();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].user_data, 200);
        assert_eq!(completions[0].result, ALIGNMENT as i32);

        for i in 0..ALIGNMENT {
            assert_eq!(rbuf[i], (i % 251) as u8, "mismatch at byte {i}");
        }
    }

    #[test]
    fn sync_batch_50_writes_then_reads() {
        let f = create_test_file(ALIGNMENT * 50);
        let fd = f.as_raw_fd();
        let mut io = SyncFallback::new(64).unwrap();

        // Write 50 blocks with distinct patterns
        let write_bufs: Vec<AlignedBuf> = (0..50)
            .map(|i| {
                let mut buf = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
                for j in 0..ALIGNMENT {
                    buf[j] = ((i * 7 + j) % 256) as u8;
                }
                buf
            })
            .collect();

        for (i, buf) in write_bufs.iter().enumerate() {
            io.submit_write(fd, buf, (i * ALIGNMENT) as u64, i as u64)
                .unwrap();
        }
        assert_eq!(io.pending(), 50);
        let completions = io.submit_and_wait(50).unwrap();
        assert_eq!(completions.len(), 50);
        for (i, c) in completions.iter().enumerate() {
            assert_eq!(c.user_data, i as u64);
            assert_eq!(c.result, ALIGNMENT as i32);
        }

        // Read all 50 back
        let mut read_bufs: Vec<AlignedBuf> = (0..50)
            .map(|_| AlignedBuf::new(ALIGNMENT, ALIGNMENT))
            .collect();
        for (i, buf) in read_bufs.iter_mut().enumerate() {
            io.submit_read(fd, buf, (i * ALIGNMENT) as u64, 1000 + i as u64)
                .unwrap();
        }
        let completions = io.submit_and_wait(50).unwrap();
        assert_eq!(completions.len(), 50);

        for (i, buf) in read_bufs.iter().enumerate() {
            for j in 0..ALIGNMENT {
                assert_eq!(
                    buf[j],
                    ((i * 7 + j) % 256) as u8,
                    "mismatch at block {i} byte {j}"
                );
            }
        }
    }

    #[test]
    fn sync_interleaved_reads_and_writes() {
        let f = create_test_file(ALIGNMENT * 4);
        let fd = f.as_raw_fd();
        let mut io = SyncFallback::new(32).unwrap();

        // Write block 0
        let mut wbuf0 = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        wbuf0[0] = 0xAA;
        io.submit_write(fd, &wbuf0, 0, 1).unwrap();
        io.submit_and_wait(1).unwrap();

        // Write block 1
        let mut wbuf1 = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        wbuf1[0] = 0xBB;
        io.submit_write(fd, &wbuf1, ALIGNMENT as u64, 2).unwrap();
        io.submit_and_wait(1).unwrap();

        // Interleave: read block 0, write block 2, read block 1
        let mut rbuf0 = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        let mut wbuf2 = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        wbuf2[0] = 0xCC;
        let mut rbuf1 = AlignedBuf::new(ALIGNMENT, ALIGNMENT);

        io.submit_read(fd, &mut rbuf0, 0, 10).unwrap();
        io.submit_write(fd, &wbuf2, (ALIGNMENT * 2) as u64, 11)
            .unwrap();
        io.submit_read(fd, &mut rbuf1, ALIGNMENT as u64, 12)
            .unwrap();
        assert_eq!(io.pending(), 3);

        let completions = io.submit_and_wait(3).unwrap();
        assert_eq!(completions.len(), 3);
        assert_eq!(completions[0].user_data, 10);
        assert_eq!(completions[1].user_data, 11);
        assert_eq!(completions[2].user_data, 12);

        assert_eq!(rbuf0[0], 0xAA);
        assert_eq!(rbuf1[0], 0xBB);

        // Verify block 2 was written
        let mut rbuf2 = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        io.submit_read(fd, &mut rbuf2, (ALIGNMENT * 2) as u64, 20)
            .unwrap();
        io.submit_and_wait(1).unwrap();
        assert_eq!(rbuf2[0], 0xCC);
    }

    #[test]
    fn sync_user_data_matches() {
        let f = create_test_file(ALIGNMENT * 10);
        let fd = f.as_raw_fd();
        let mut io = SyncFallback::new(32).unwrap();

        let tags: Vec<u64> = vec![42, 1000, 0xDEADBEEF, 7, u64::MAX];
        let mut bufs: Vec<AlignedBuf> = tags
            .iter()
            .map(|_| AlignedBuf::new(ALIGNMENT, ALIGNMENT))
            .collect();

        for (i, (buf, &tag)) in bufs.iter_mut().zip(tags.iter()).enumerate() {
            io.submit_read(fd, buf, (i * ALIGNMENT) as u64, tag)
                .unwrap();
        }

        let completions = io.submit_and_wait(tags.len()).unwrap();
        for (c, &expected_tag) in completions.iter().zip(tags.iter()) {
            assert_eq!(c.user_data, expected_tag);
        }
    }

    #[test]
    fn sync_completions_empty_before_submit() {
        let mut io = SyncFallback::new(32).unwrap();
        assert!(io.completions().is_empty());
    }

    #[test]
    fn sync_submit_noop() {
        let mut io = SyncFallback::new(32).unwrap();
        io.submit().unwrap(); // Should not panic or error
    }

    /// F-G1-011 regression: `SyncFallback::new(queue_depth)` must
    /// pre-size the pending-op buffer to honour the documented
    /// contract that `create_device_io(queue_depth)` sizes the
    /// backend. Before the fix the parameter was `_queue_depth: u32`
    /// and the buffer started with capacity zero — the first
    /// `submit_*` call triggered a `Vec` reallocation that the
    /// io_uring backend never paid.
    #[test]
    fn sync_new_pre_sizes_pending_buffer_to_queue_depth() {
        let io = SyncFallback::new(128).unwrap();
        assert!(
            io.pending.capacity() >= 128,
            "queue_depth=128 must pre-size pending to >= 128 (got capacity={})",
            io.pending.capacity()
        );
    }

    /// F-G1-011 regression: capacity is clamped at 4096 so a caller
    /// that passes `queue_depth = u32::MAX` does not try to allocate
    /// gigabytes of `PendingOp` slots up front.
    #[test]
    fn sync_new_clamps_excessive_queue_depth() {
        let io = SyncFallback::new(u32::MAX).unwrap();
        assert!(
            io.pending.capacity() <= 4096,
            "queue_depth=u32::MAX must clamp to <= 4096 (got capacity={})",
            io.pending.capacity()
        );
    }

    /// F-G1-010 regression: `submit_read` must reject a zero-length
    /// buffer instead of passing the `AlignedBuf::new(0, …)` dangling
    /// pointer through to the syscall layer.
    #[test]
    fn sync_submit_read_rejects_zero_length_buffer() {
        let mut io = SyncFallback::new(8).unwrap();
        let mut buf = AlignedBuf::new(0, ALIGNMENT);
        let err = io
            .submit_read(0, &mut buf, 0, 1)
            .expect_err("zero-length submit_read must error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    /// F-G1-010 regression: same contract for `submit_write`.
    #[test]
    fn sync_submit_write_rejects_zero_length_buffer() {
        let mut io = SyncFallback::new(8).unwrap();
        let buf = AlignedBuf::new(0, ALIGNMENT);
        let err = io
            .submit_write(0, &buf, 0, 1)
            .expect_err("zero-length submit_write must error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn create_device_io_returns_sync_on_macos() {
        // On macOS (or any non-Linux), create_device_io should return SyncFallback
        let io = super::super::create_device_io(32).unwrap();
        assert_eq!(io.pending(), 0);
    }

    /// F-G1-001 regression: a failed read on a bad fd must surface the
    /// real errno (negated, per the `DeviceIo::Completion::result`
    /// contract) — `-libc::EBADF` here — not a bare `-1`. Before the
    /// fix every libc error collapsed to `-1` and downstream telemetry
    /// (`io_uring_backend::record_completion_error`) recorded the
    /// wrong error class for sync-fallback failures.
    #[test]
    fn sync_pread_on_bad_fd_returns_neg_ebadf() {
        let mut io = SyncFallback::new(8).unwrap();
        // -1 is never a valid open fd; libc::pread(-1, ...) returns -1
        // and sets errno = EBADF.
        let bad_fd: std::os::fd::RawFd = -1;
        let mut rbuf = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        io.submit_read(bad_fd, &mut rbuf, 0, 0xCAFE).unwrap();
        let completions = io.submit_and_wait(1).unwrap();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].user_data, 0xCAFE);
        assert_eq!(
            completions[0].result,
            -libc::EBADF,
            "bad fd must surface negative EBADF, not bare -1",
        );
    }

    /// F-G1-001 regression: a failed pwrite to a bad fd must surface
    /// `-libc::EBADF` as well — same contract as the read case.
    #[test]
    fn sync_pwrite_on_bad_fd_returns_neg_ebadf() {
        let mut io = SyncFallback::new(8).unwrap();
        let bad_fd: std::os::fd::RawFd = -1;
        let wbuf = AlignedBuf::new(ALIGNMENT, ALIGNMENT);
        io.submit_write(bad_fd, &wbuf, 0, 0xBEEF).unwrap();
        let completions = io.submit_and_wait(1).unwrap();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].user_data, 0xBEEF);
        assert_eq!(
            completions[0].result,
            -libc::EBADF,
            "bad fd must surface negative EBADF, not bare -1",
        );
    }
}
