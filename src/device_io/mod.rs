//! Batched device I/O abstraction.
//!
//! Both `IoUringBackend` and `SyncFallback` implement the `DeviceIo` trait,
//! providing a single interface for the spend path regardless of kernel support.

mod sync_fallback;

#[cfg(target_os = "linux")]
mod io_uring_backend;

pub use sync_fallback::SyncFallback;

#[cfg(target_os = "linux")]
pub use io_uring_backend::IoUringBackend;

use crate::device::AlignedBuf;
use std::os::fd::RawFd;

/// Result of a completed I/O operation.
#[derive(Debug, Clone)]
pub struct Completion {
    /// Caller-supplied tag identifying this operation.
    pub user_data: u64,
    /// Bytes transferred, or negative errno on failure.
    pub result: i32,
}

/// Trait abstracting batched device I/O.
///
/// Both `IoUringBackend` and `SyncFallback` implement this identically.
/// Operations are queued with `submit_read`/`submit_write` and executed
/// on `submit_and_wait` or `submit`.
pub trait DeviceIo: Send + Sync {
    /// Queue a pread operation. I/O is deferred until `submit_and_wait` or `submit`.
    fn submit_read(
        &mut self,
        fd: RawFd,
        buf: &mut AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<(), std::io::Error>;

    /// Queue a pwrite operation. Same deferred semantics.
    fn submit_write(
        &mut self,
        fd: RawFd,
        buf: &AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<(), std::io::Error>;

    /// Submit all pending operations and block until at least `min_complete` finish.
    fn submit_and_wait(&mut self, min_complete: usize) -> Result<Vec<Completion>, std::io::Error>;

    /// Submit all pending operations without waiting.
    fn submit(&mut self) -> Result<(), std::io::Error>;

    /// Harvest completed operations (non-blocking).
    fn completions(&mut self) -> Vec<Completion>;

    /// Number of operations currently pending (submitted but not completed).
    fn pending(&self) -> usize;
}

/// Create the best available `DeviceIo` backend.
///
/// On Linux >= 5.6, attempts `IoUringBackend`. Falls back to `SyncFallback`
/// on unsupported kernels or non-Linux platforms (macOS, test environments).
pub fn create_device_io(queue_depth: u32) -> Box<dyn DeviceIo> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(backend) = IoUringBackend::new(queue_depth) {
            return Box::new(backend);
        }
    }
    let _ = queue_depth;
    Box::new(SyncFallback::new(queue_depth).expect("SyncFallback cannot fail"))
}
