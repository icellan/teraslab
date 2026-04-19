//! Batched device I/O abstraction.
//!
//! Both `IoUringBackend` and `SyncFallback` implement the `DeviceIo` trait,
//! providing a single interface for the spend path regardless of kernel support.
//!
//! On Linux, [`create_device_io`] prefers `IoUringBackend` (kernel async I/O,
//! Linux >= 5.6) and falls back to `SyncFallback` (sequential libc
//! pread/pwrite) only if ring setup fails — e.g. kernel too old, `ulimit
//! -l` too small, seccomp-filtered sandbox. On non-Linux platforms
//! `SyncFallback` is the only available backend; the io_uring module
//! returns an `Unsupported` error at `new()` time and this is a documented
//! platform limitation, not a stub.
//!
//! Callers can query which backend is actually in use via
//! [`DeviceIo::backend_name`] — useful for metrics / observability so the
//! operator can see whether the server is running on the fast path.

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

    /// Stable identifier for the concrete backend ("io_uring" or "sync").
    ///
    /// Used by metrics / `/metrics` endpoints so operators can confirm
    /// which I/O backend is actually serving the spend path. The default
    /// implementation returns `"unknown"` — every concrete backend in this
    /// module overrides it.
    fn backend_name(&self) -> &'static str {
        "unknown"
    }
}

/// Create the best available `DeviceIo` backend.
///
/// On Linux >= 5.6, attempts `IoUringBackend`. Falls back to `SyncFallback`
/// on unsupported kernels or non-Linux platforms (macOS, test environments).
/// When a fallback is taken on Linux, the reason is emitted to stderr so
/// operators can diagnose why the faster path was not selected.
pub fn create_device_io(queue_depth: u32) -> Box<dyn DeviceIo> {
    #[cfg(target_os = "linux")]
    {
        match IoUringBackend::new(queue_depth) {
            Ok(backend) => return Box::new(backend),
            Err(e) => {
                // Operators should see this: it means the server is running
                // on the slower sync path. Common causes: kernel < 5.6,
                // ulimit -l too small, seccomp filter blocking io_uring_setup.
                tracing::warn!(
                    err = %e,
                    "device_io: io_uring init failed; falling back to SyncFallback",
                );
            }
        }
    }
    let _ = queue_depth;
    match SyncFallback::new(queue_depth) {
        Ok(backend) => Box::new(backend),
        Err(e) => {
            // SyncFallback::new only returns Ok, so this is genuinely unreachable —
            // but avoid unwrap/expect per CLAUDE.md and panic with a clear message
            // instead of silently hiding the bug if the signature ever changes.
            panic!("SyncFallback::new returned an error it documents as impossible: {e}");
        }
    }
}
