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
    /// Create a new synchronous fallback. The `_queue_depth` parameter is
    /// accepted for API compatibility but ignored (no ring to size).
    pub fn new(_queue_depth: u32) -> Result<Self, std::io::Error> {
        Ok(Self {
            pending: Vec::new(),
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
            let result = match op.kind {
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
            completions.push(Completion {
                user_data: op.user_data,
                result: result as i32,
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

    #[test]
    fn create_device_io_returns_sync_on_macos() {
        // On macOS (or any non-Linux), create_device_io should return SyncFallback
        let io = super::super::create_device_io(32);
        assert_eq!(io.pending(), 0);
    }
}
