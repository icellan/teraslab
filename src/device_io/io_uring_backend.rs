//! io_uring backend for Linux >= 5.6.
//!
//! Uses the `io-uring` crate for direct control over submission and
//! completion queues.

use super::{Completion, DeviceIo};
use crate::device::AlignedBuf;
use std::os::fd::RawFd;

/// io_uring-based I/O backend for Linux >= 5.6.
///
/// Provides true asynchronous batched I/O via the kernel's io_uring
/// interface. Falls back to `SyncFallback` on older kernels.
pub struct IoUringBackend {
    // io_uring ring would go here when the io-uring crate is added
    _pending: usize,
}

impl IoUringBackend {
    /// Create a new io_uring ring with the given queue depth.
    ///
    /// Returns an error if io_uring is not supported on this kernel.
    pub fn new(_queue_depth: u32) -> Result<Self, std::io::Error> {
        // io_uring requires Linux >= 5.6 and the io-uring crate dependency.
        // Until the crate is added, always return an error to trigger SyncFallback.
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "io_uring support requires the io-uring crate (Linux >= 5.6 only)",
        ))
    }
}

impl DeviceIo for IoUringBackend {
    fn submit_read(
        &mut self,
        _fd: RawFd,
        _buf: &mut AlignedBuf,
        _offset: u64,
        _user_data: u64,
    ) -> Result<(), std::io::Error> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "io_uring not initialized",
        ))
    }

    fn submit_write(
        &mut self,
        _fd: RawFd,
        _buf: &AlignedBuf,
        _offset: u64,
        _user_data: u64,
    ) -> Result<(), std::io::Error> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "io_uring not initialized",
        ))
    }

    fn submit_and_wait(&mut self, _min_complete: usize) -> Result<Vec<Completion>, std::io::Error> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "io_uring not initialized",
        ))
    }

    fn submit(&mut self) -> Result<(), std::io::Error> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "io_uring not initialized",
        ))
    }

    fn completions(&mut self) -> Vec<Completion> {
        Vec::new()
    }

    fn pending(&self) -> usize {
        self._pending
    }
}
