//! Whole-frame assembly deadline shared by the public server accept path
//! and the replication receiver (L-01 / follow-up E-1).
//!
//! A socket's `set_read_timeout` is a *per-syscall* timeout — it resets on
//! every successful read, so it cannot bound the *total* time a multi-read
//! frame assembly takes. A slow-drip peer delivering one byte just inside
//! the per-read timeout keeps every individual read "succeeding" forever
//! and can pin a handler thread (and whatever per-connection resources it
//! holds) indefinitely at negligible bandwidth. [`DeadlineReader`] wraps a
//! [`TcpStream`] and enforces an absolute deadline across all the reads
//! that assemble a single frame.

use std::io::Read;
use std::net::TcpStream;
use std::time::{Duration, Instant};

/// Whole-frame assembly deadline, measured from the moment the 4-byte
/// length prefix has been read off the wire.
///
/// The per-syscall read timeout (30 s on both the server accept path and
/// the replication receiver) resets on every successful read, so a
/// slow-drip peer delivering one byte every ~29 s keeps each individual
/// read "succeeding" and could pin a handler thread — and whatever
/// per-connection resources it holds — indefinitely at negligible
/// bandwidth. This deadline bounds total frame-assembly time regardless of
/// per-read progress.
///
/// Value: 2 × the 30 s per-read timeout. Generous for legitimate peers — a
/// worst-case `MAX_FRAME_SIZE` (16 MiB) frame completes in time at
/// ~280 KiB/s — while capping how long a drip-feed can hold a connection
/// slot.
pub(crate) const FRAME_ASSEMBLY_TIMEOUT: Duration = Duration::from_secs(60);

/// `Read` adapter that enforces a whole-frame assembly deadline on top of
/// the socket's per-syscall read timeout.
///
/// `set_read_timeout` resets on every successful read, so it cannot bound
/// the *total* time a multi-read frame assembly takes — a slow-drip peer
/// defeats it byte by byte. This adapter checks the deadline before every
/// read (returning an [`std::io::ErrorKind::TimedOut`] error once it has
/// passed) and, when less than `base_timeout` remains, shrinks the socket
/// read timeout to the remainder so a single blocking read cannot overshoot
/// the deadline either.
///
/// If [`timeout_shrunk`](DeadlineReader::timeout_shrunk) is `true` after
/// use, the caller MUST restore the socket's read timeout to `base_timeout`
/// before the next frame's length-prefix read — the idle-peer drop path
/// relies on it.
pub(crate) struct DeadlineReader<'a> {
    stream: &'a TcpStream,
    deadline: Instant,
    base_timeout: Duration,
    /// Set once a read shrinks the socket's read timeout below
    /// `base_timeout`; the caller must restore the base timeout afterward.
    pub(crate) timeout_shrunk: bool,
}

impl<'a> DeadlineReader<'a> {
    /// Wrap `stream` with an absolute frame-assembly `deadline`.
    ///
    /// `base_timeout` is the socket's nominal per-read timeout; reads near
    /// the deadline temporarily shrink the socket timeout below it so a
    /// single blocking read cannot overshoot the deadline.
    pub(crate) fn new(stream: &'a TcpStream, deadline: Instant, base_timeout: Duration) -> Self {
        Self {
            stream,
            deadline,
            base_timeout,
            timeout_shrunk: false,
        }
    }
}

impl Read for DeadlineReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "frame assembly deadline exceeded",
            ));
        }
        if remaining < self.base_timeout {
            // `remaining` is non-zero here, so this never trips the
            // `set_read_timeout(Some(ZERO)) == Err` contract.
            self.stream.set_read_timeout(Some(remaining))?;
            self.timeout_shrunk = true;
        }
        let mut inner = self.stream;
        inner.read(buf)
    }
}
