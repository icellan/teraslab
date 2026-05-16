//! TCP-based transport for replication.
//!
//! Wraps a `TcpStream` and uses the TeraSlab wire protocol frames:
//! - Master to Replica: `RequestFrame` with `op_code=OP_REPLICA_BATCH`, payload = batch bytes
//! - Replica to Master: `ResponseFrame` with `status=STATUS_OK` for
//!   `ReplicaAck::Ok` and `status=STATUS_ERROR` for `ReplicaAck::Error`,
//!   payload = ack bytes

use crate::protocol::frame::{RequestFrame, ResponseFrame};
use crate::protocol::opcodes::*;
use crate::replication::manager::ReplicationError;
use crate::replication::protocol::{ReplicaAck, ReplicaBatch};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use super::manager::ReplicaTransport;

/// ACK response frames are mostly tiny (`ResponseFrame` header +
/// `ReplicaAck::Ok` is ~22 bytes), but `ReplicaAck::Error` carries a
/// variable-length diagnostic message. F-G7-017 raised the cap from
/// 1 KiB to 4 KiB after a `format!("flush applied tracker: {e}")`
/// containing a long path overflowed the previous budget and lost
/// the diagnostic. The sender additionally truncates messages above
/// `MAX_ACK_ERROR_MESSAGE_LEN` so a buggy replica cannot push the
/// master past this cap.
const MAX_ACK_FRAME_SIZE: usize = 4096;
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(3);

/// Configure TCP keepalive on a stream for fast broken-connection detection.
///
/// After a container SIGKILL the peer's TCP connection is silently broken.
/// Without keepalive the surviving node doesn't know until the next read
/// timeout fires (seconds to minutes). Keepalive probes detect the dead
/// connection within a few seconds.
///
/// Settings: idle=5s, interval=1s, count=3 → dead peer detected in ~8s.
pub fn configure_tcp_keepalive(stream: &TcpStream) {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let fd = stream.as_raw_fd();
        unsafe {
            // SAFETY: `fd` is borrowed from a live `TcpStream`, and every
            // optval pointer below points to an initialized `libc::c_int`
            // that remains valid for the duration of the `setsockopt` call.
            // The level/option pairs are OS constants for TCP keepalive
            // tuning. Errors are intentionally ignored because keepalive is
            // a best-effort latency optimization; the socket remains usable.
            // Enable keepalive
            let enable: libc::c_int = 1;
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &enable as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );

            // Time before first keepalive probe (seconds)
            let idle: libc::c_int = 5;
            #[cfg(target_os = "macos")]
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPALIVE,
                &idle as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            #[cfg(target_os = "linux")]
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPIDLE,
                &idle as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );

            // Interval between keepalive probes (seconds)
            let interval: libc::c_int = 1;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPINTVL,
                &interval as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );

            // Number of failed probes before declaring dead
            let count: libc::c_int = 3;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPCNT,
                &count as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

/// TCP-based transport for replication.
///
/// Sends `ReplicaBatch` as wire-protocol `RequestFrame`s with opcode
/// `OP_REPLICA_BATCH`, and reads `ReplicaAck` from the corresponding
/// `ResponseFrame`. Designed for blocking (std::net) I/O.
pub struct TcpReplicaTransport {
    stream: TcpStream,
    request_id: u64,
    write_timeout: Duration,
    auth_secret: Option<Vec<u8>>,
}

impl TcpReplicaTransport {
    /// Connect to a replica at the given address with a timeout.
    ///
    /// Sets read/write timeouts and enables TCP keepalive so broken
    /// connections (e.g. after container SIGKILL) are detected quickly
    /// instead of waiting for the OS default keepalive timeout (minutes).
    pub fn connect(addr: &str, timeout: Duration) -> Result<Self, ReplicationError> {
        Self::connect_with_auth(addr, timeout, None)
    }

    /// Connect to a replica and authenticate every request/response frame
    /// with `auth_secret` when configured.
    pub fn connect_with_auth(
        addr: &str,
        timeout: Duration,
        auth_secret: Option<Vec<u8>>,
    ) -> Result<Self, ReplicationError> {
        let sock_addr: std::net::SocketAddr = addr
            .parse()
            .map_err(|e| ReplicationError::Transport(format!("invalid address '{addr}': {e}")))?;
        let stream = TcpStream::connect_timeout(&sock_addr, timeout)
            .map_err(|e| ReplicationError::Transport(format!("connect to {addr}: {e}")))?;
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|e| ReplicationError::Transport(format!("set write timeout: {e}")))?;
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|e| ReplicationError::Transport(format!("set read timeout: {e}")))?;
        // Disable Nagle's algorithm: replication payloads are
        // latency-sensitive and already batched at the application
        // layer. Letting the TCP stack coalesce small ACK frames
        // adds tens of milliseconds of tail latency per round-trip.
        stream
            .set_nodelay(true)
            .map_err(|e| ReplicationError::Transport(format!("set nodelay: {e}")))?;
        configure_tcp_keepalive(&stream);
        Ok(Self {
            stream,
            request_id: 0,
            write_timeout: timeout,
            auth_secret,
        })
    }

    /// Wrap an existing `TcpStream` (for testing or when the connection is
    /// already established, e.g. from an accepted socket).
    ///
    /// Best-effort enables `TCP_NODELAY` so streams wrapped here receive
    /// the same low-latency behavior as `connect()`. Any OS error is
    /// ignored because the stream is already usable without the option.
    pub fn from_stream(stream: TcpStream) -> Self {
        Self::from_stream_with_auth(stream, None)
    }

    /// Wrap an existing stream with optional frame authentication.
    pub fn from_stream_with_auth(stream: TcpStream, auth_secret: Option<Vec<u8>>) -> Self {
        let _ = stream.set_nodelay(true);
        Self {
            stream,
            request_id: 0,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            auth_secret,
        }
    }

    /// Override the timeout applied immediately before every replication
    /// write. `connect()` initializes this from the replication timeout;
    /// tests and pre-accepted streams can set it explicitly.
    pub fn set_write_timeout(&mut self, timeout: Duration) {
        self.write_timeout = timeout;
    }

    /// Whether this pooled connection was created for the same auth mode.
    pub fn auth_secret_matches(&self, auth_secret: Option<&[u8]>) -> bool {
        match (&self.auth_secret, auth_secret) {
            (None, None) => true,
            (Some(a), Some(b)) => a.as_slice() == b,
            _ => false,
        }
    }
}

impl ReplicaTransport for TcpReplicaTransport {
    /// Send a `ReplicaBatch` to the replica as a wire-protocol request frame.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            replica_addr = %self.stream.peer_addr()
                .map(|a| a.to_string())
                .unwrap_or_else(|_| "<unknown>".to_string()),
            request_id = self.request_id + 1,
            batch_len = batch.ops.len(),
        ),
    )]
    fn send_batch(&mut self, batch: &ReplicaBatch) -> std::result::Result<(), ReplicationError> {
        self.request_id += 1;
        let frame = RequestFrame {
            request_id: self.request_id,
            op_code: OP_REPLICA_BATCH,
            flags: 0,
            payload: batch.serialize(),
        };
        let encoded = frame.encode();
        let bytes = if let Some(secret) = self.auth_secret.as_deref() {
            crate::cluster::auth::sign_frame(secret, &encoded)
                .map_err(|e| ReplicationError::Transport(format!("sign request frame: {e}")))?
        } else {
            encoded
        };
        self.stream
            .set_write_timeout(Some(self.write_timeout))
            .map_err(|e| ReplicationError::Transport(format!("set write timeout: {e}")))?;
        self.stream
            .write_all(&bytes)
            .map_err(|e| ReplicationError::Transport(format!("send_batch write: {e}")))?;
        Ok(())
    }

    /// Read a `ReplicaAck` from the replica's response frame.
    ///
    /// Applies the given timeout to the read. Returns `ReplicationError::Timeout`
    /// on `TimedOut` / `WouldBlock` I/O errors.
    fn recv_ack(&mut self, timeout: Duration) -> std::result::Result<ReplicaAck, ReplicationError> {
        self.stream
            .set_read_timeout(Some(timeout))
            .map_err(|e| ReplicationError::Transport(format!("set read timeout: {e}")))?;

        // Read the 4-byte length prefix
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).map_err(|e| {
            if e.kind() == std::io::ErrorKind::TimedOut
                || e.kind() == std::io::ErrorKind::WouldBlock
            {
                ReplicationError::Timeout(timeout)
            } else {
                ReplicationError::Transport(format!("recv_ack read length: {e}"))
            }
        })?;
        let total_len = u32::from_le_bytes(len_buf) as usize;

        let max_ack_frame_size = MAX_ACK_FRAME_SIZE
            + self
                .auth_secret
                .as_ref()
                .map(|_| crate::cluster::auth::SIGNED_SUFFIX_LEN)
                .unwrap_or(0);
        if total_len > max_ack_frame_size {
            return Err(ReplicationError::Transport(format!(
                "response ACK frame too large: {total_len} > {max_ack_frame_size}"
            )));
        }

        // Read the body
        let mut body = vec![0u8; total_len];
        self.stream.read_exact(&mut body).map_err(|e| {
            if e.kind() == std::io::ErrorKind::TimedOut
                || e.kind() == std::io::ErrorKind::WouldBlock
            {
                ReplicationError::Timeout(timeout)
            } else {
                ReplicationError::Transport(format!("recv_ack read body: {e}"))
            }
        })?;

        // Reconstruct the full frame for decoding
        let mut full = Vec::with_capacity(4 + total_len);
        full.extend_from_slice(&len_buf);
        full.extend_from_slice(&body);

        let frame = if let Some(secret) = self.auth_secret.as_deref() {
            crate::cluster::auth::verify_frame(secret, &full).map_err(|e| {
                ReplicationError::Transport(format!("authenticate response frame: {e}"))
            })?
        } else {
            full
        };

        let (resp, _) = ResponseFrame::decode(&frame)
            .map_err(|e| ReplicationError::Transport(format!("decode response frame: {e}")))?;

        // F-G7-002: The response's request_id MUST match the outgoing
        // request_id we incremented in `send_batch`. If a future code
        // path ever re-caches a transport after a partial-ACK timeout
        // a stale buffered ACK could otherwise be silently attributed
        // to the next request. Reject mismatches as a hard transport
        // error so the caller drops the connection and reconnects.
        if resp.request_id != self.request_id {
            return Err(ReplicationError::Transport(format!(
                "ack request_id mismatch: expected {}, got {}",
                self.request_id, resp.request_id
            )));
        }

        if resp.status != STATUS_OK {
            // Try to deserialize as an error ack; otherwise wrap the status
            if let Ok(ack) = ReplicaAck::deserialize(&resp.payload) {
                return Ok(ack);
            }
            return Err(ReplicationError::Transport(format!(
                "replica returned status {}",
                resp.status
            )));
        }

        ReplicaAck::deserialize(&resp.payload)
            .map_err(|e| ReplicationError::Transport(format!("deserialize ack: {e}")))
    }

    /// Check whether the underlying TCP connection appears healthy.
    ///
    /// F-G7-014: this is a BEST-EFFORT probe, NOT a true liveness
    /// check. It calls `TcpStream::take_error` which consumes the
    /// asynchronous error flag (SO_ERROR). On macOS the flag only
    /// surfaces ECONNRESET-class events, not graceful peer FIN: a
    /// peer that has closed its half of the socket still reports
    /// `is_connected == true` until the next read returns EOF.
    /// Concurrent callers also race because `take_error` is consuming.
    ///
    /// `check_reconnected` in
    /// [`crate::replication::manager::ReplicationManager`] uses this
    /// as a hint to move a sender from Down → CatchingUp; the next
    /// `send_batch` / `recv_ack` is the authoritative liveness check
    /// and re-marks the sender Down on hard failure. Do not rely on
    /// this method as a positive correctness signal.
    fn is_connected(&self) -> bool {
        self.stream
            .take_error()
            .map(|e| e.is_none())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::TxKey;
    use crate::replication::protocol::ReplicaOp;
    use std::net::TcpListener;

    fn key(n: u8) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0] = n;
        TxKey { txid }
    }

    #[test]
    fn send_batch_and_recv_ack_over_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let batch = ReplicaBatch {
            first_sequence: 1,
            ops: vec![ReplicaOp::Freeze {
                tx_key: key(1),
                offset: 0,
                master_generation: 0,
            }],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };

        let batch_clone = batch.clone();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();

            // Read the request frame
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).unwrap();
            let total_len = u32::from_le_bytes(len_buf) as usize;
            let mut body = vec![0u8; total_len];
            stream.read_exact(&mut body).unwrap();

            let mut full = Vec::with_capacity(4 + total_len);
            full.extend_from_slice(&len_buf);
            full.extend_from_slice(&body);

            let (req, _) = RequestFrame::decode(&full).unwrap();
            assert_eq!(req.op_code, OP_REPLICA_BATCH);

            let received = ReplicaBatch::deserialize(&req.payload).unwrap();
            assert_eq!(received, batch_clone);

            // Send ack response
            let ack = ReplicaAck::Ok {
                through_sequence: received.last_sequence(),
            };
            let resp = ResponseFrame {
                request_id: req.request_id,
                status: STATUS_OK,
                payload: ack.serialize(),
            };
            stream.write_all(&resp.encode()).unwrap();
        });

        let mut transport =
            TcpReplicaTransport::connect(&addr.to_string(), Duration::from_secs(5)).unwrap();
        transport.send_batch(&batch).unwrap();
        let ack = transport.recv_ack(Duration::from_secs(5)).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Ok {
                through_sequence: 1
            }
        );

        handle.join().unwrap();
    }

    #[test]
    fn recv_ack_accepts_status_error_with_replica_ack_error() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let ack = ReplicaAck::Error {
                failed_sequence: 7,
                message: "apply failed".to_string(),
            };
            let resp = ResponseFrame {
                // Match the transport's pre-incremented request_id (0)
                // since this test invokes recv_ack without a prior
                // send_batch (which would have bumped the counter).
                request_id: 0,
                status: STATUS_ERROR,
                payload: ack.serialize(),
            };
            stream.write_all(&resp.encode()).unwrap();
        });

        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        let mut transport = TcpReplicaTransport::from_stream(stream);
        let ack = transport.recv_ack(Duration::from_secs(5)).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Error {
                failed_sequence: 7,
                message: "apply failed".to_string(),
            },
        );

        handle.join().unwrap();
    }

    /// F-G7-002: a response whose `request_id` doesn't match the
    /// transport's outgoing `request_id` must be rejected as a hard
    /// transport error, even if the body is a well-formed `ReplicaAck`.
    /// Without this, a stale buffered ACK from a previous request
    /// could be silently attributed to the next call.
    #[test]
    fn recv_ack_rejects_request_id_mismatch() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            // Drain the client's request frame so the round-trip
            // completes (we ignore its contents).
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).unwrap();
            let total_len = u32::from_le_bytes(len_buf) as usize;
            let mut body = vec![0u8; total_len];
            stream.read_exact(&mut body).unwrap();

            // Reply with a wildly mismatched request_id.
            let ack = ReplicaAck::Ok {
                through_sequence: 1,
            };
            let resp = ResponseFrame {
                request_id: 999_999,
                status: STATUS_OK,
                payload: ack.serialize(),
            };
            stream.write_all(&resp.encode()).unwrap();
        });

        let mut transport =
            TcpReplicaTransport::connect(&addr.to_string(), Duration::from_secs(5)).unwrap();
        let batch = ReplicaBatch {
            first_sequence: 1,
            ops: vec![ReplicaOp::Freeze {
                tx_key: key(1),
                offset: 0,
                master_generation: 0,
            }],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };
        transport.send_batch(&batch).unwrap();
        let err = transport
            .recv_ack(Duration::from_secs(5))
            .expect_err("must reject ack with mismatched request_id");
        match err {
            ReplicationError::Transport(msg) => {
                assert!(
                    msg.contains("request_id mismatch"),
                    "expected request_id mismatch error, got: {msg}",
                );
            }
            other => panic!("expected Transport error, got {other:?}"),
        }
        handle.join().unwrap();
    }

    #[test]
    fn recv_ack_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept but never respond
        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            // Hold the connection open
            std::thread::sleep(Duration::from_secs(2));
            drop(stream);
        });

        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        let mut transport = TcpReplicaTransport::from_stream(stream);

        let batch = ReplicaBatch {
            first_sequence: 1,
            ops: vec![ReplicaOp::Freeze {
                tx_key: key(1),
                offset: 0,
                master_generation: 0,
            }],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };
        transport.send_batch(&batch).unwrap();

        let result = transport.recv_ack(Duration::from_millis(100));
        assert!(matches!(result, Err(ReplicationError::Timeout(_))));

        handle.join().unwrap();
    }

    /// F-G7-017: the master rejects ACK frames larger than
    /// `MAX_ACK_FRAME_SIZE` (4 KiB after F-G7-017). The test asserts
    /// the cap is enforced; the constant name stays generic so the
    /// test survives further raises.
    #[test]
    fn recv_ack_max_allocation_capped_for_error_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .write_all(&(MAX_ACK_FRAME_SIZE as u32 + 1).to_le_bytes())
                .unwrap();
        });

        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        let mut transport = TcpReplicaTransport::from_stream(stream);

        let result = transport.recv_ack(Duration::from_secs(5));
        assert!(matches!(result, Err(ReplicationError::Transport(_))));
        let err = result.unwrap_err().to_string();
        assert!(err.contains("ACK frame too large"), "error was: {err}");

        handle.join().unwrap();
    }

    #[test]
    fn is_connected_on_live_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_secs(1));
            drop(stream);
        });

        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        let transport = TcpReplicaTransport::from_stream(stream);
        assert!(transport.is_connected());

        handle.join().unwrap();
    }

    #[test]
    fn is_connected_preserves_read_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_millis(200));
        });

        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        let timeout = Some(Duration::from_secs(7));
        stream.set_read_timeout(timeout).unwrap();
        let transport = TcpReplicaTransport::from_stream(stream);

        assert!(transport.is_connected());
        assert_eq!(
            transport.stream.read_timeout().unwrap(),
            timeout,
            "is_connected must not mutate socket read timeout",
        );

        handle.join().unwrap();
    }

    #[test]
    fn connect_to_invalid_addr_returns_error() {
        let result = TcpReplicaTransport::connect("not_a_valid_address", Duration::from_secs(1));
        assert!(matches!(result, Err(ReplicationError::Transport(_))));
    }

    /// TCP_NODELAY must be enabled on every replication connection:
    /// replication payloads are already batched at the application
    /// layer and ACK frames are small, so Nagle's algorithm only
    /// adds tail latency.
    #[test]
    fn tcp_nodelay_enabled_on_connect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            // Keep the server side alive long enough for the client
            // to inspect its socket options.
            std::thread::sleep(Duration::from_millis(200));
            drop(stream);
        });

        let transport =
            TcpReplicaTransport::connect(&addr.to_string(), Duration::from_secs(5)).unwrap();
        assert!(
            transport.stream.nodelay().expect("read nodelay"),
            "TCP_NODELAY must be enabled on the replication socket",
        );

        drop(transport);
        handle.join().unwrap();
    }

    /// `from_stream` wraps an already-connected socket and also makes
    /// a best-effort attempt to enable TCP_NODELAY. Verify the option
    /// is set after wrapping.
    #[test]
    fn tcp_nodelay_enabled_via_from_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_millis(200));
            drop(stream);
        });

        let client = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        // Confirm nodelay is NOT enabled by default (sanity: the test
        // then asserts from_stream flips it on).
        let default_nodelay = client.nodelay().unwrap_or(false);
        let transport = TcpReplicaTransport::from_stream(client);
        assert!(
            transport.stream.nodelay().expect("read nodelay"),
            "from_stream must enable TCP_NODELAY (was default={})",
            default_nodelay,
        );

        drop(transport);
        handle.join().unwrap();
    }

    #[test]
    fn tcp_keepalive_configured_on_connect() {
        // Verify that connect() configures TCP keepalive without error.
        // We can't easily inspect the socket options portably, but we can
        // verify the connection succeeds and the keepalive function doesn't
        // panic or error.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_millis(100));
        });

        // connect() now calls configure_tcp_keepalive internally
        let transport =
            TcpReplicaTransport::connect(&addr.to_string(), Duration::from_secs(5)).unwrap();
        assert!(transport.is_connected());

        handle.join().unwrap();
    }

    #[test]
    fn write_timeout_independent_of_connect_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).unwrap();
            let total_len = u32::from_le_bytes(len_buf) as usize;
            let mut body = vec![0u8; total_len];
            stream.read_exact(&mut body).unwrap();
        });

        let client = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        assert_eq!(client.write_timeout().unwrap(), None);
        let mut transport = TcpReplicaTransport::from_stream(client);
        let send_timeout = Duration::from_millis(250);
        transport.set_write_timeout(send_timeout);

        let batch = ReplicaBatch {
            first_sequence: 1,
            ops: vec![ReplicaOp::Freeze {
                tx_key: key(1),
                offset: 0,
                master_generation: 1,
            }],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };
        transport.send_batch(&batch).unwrap();

        assert_eq!(
            transport.stream.write_timeout().unwrap(),
            Some(send_timeout)
        );
        handle.join().unwrap();
    }

    #[test]
    fn configure_keepalive_on_raw_stream() {
        // Verify configure_tcp_keepalive works on a plain TcpStream.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_millis(100));
        });

        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        // Must not panic
        super::configure_tcp_keepalive(&stream);

        handle.join().unwrap();
    }

    // -----------------------------------------------------------------
    // Phase 3 — tracing span integration tests
    // -----------------------------------------------------------------

    /// Driving a `TcpReplicaTransport::send_batch` call should emit a
    /// `send_batch` span with a `replica_addr` field equal to the peer's
    /// socket address.
    #[test]
    fn replication_send_batch_span_has_replica_addr_field() {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        use tracing::field::{Field, Visit};
        use tracing::span::{Attributes, Id};
        use tracing_subscriber::Layer;
        use tracing_subscriber::layer::Context;
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::registry::LookupSpan;

        #[derive(Clone, Debug)]
        struct Captured {
            name: &'static str,
            fields: HashMap<String, String>,
        }

        #[derive(Default)]
        struct CaptureLayer {
            spans: Arc<Mutex<Vec<Captured>>>,
        }

        struct FieldVisitor<'a>(&'a mut HashMap<String, String>);

        impl<'a> Visit for FieldVisitor<'a> {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                self.0
                    .insert(field.name().to_string(), format!("{value:?}"));
            }
            fn record_str(&mut self, field: &Field, value: &str) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
            fn record_u64(&mut self, field: &Field, value: u64) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
            fn record_i64(&mut self, field: &Field, value: i64) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
            fn record_bool(&mut self, field: &Field, value: bool) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
        }

        impl<S> Layer<S> for CaptureLayer
        where
            S: tracing::Subscriber + for<'a> LookupSpan<'a>,
        {
            fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
                let mut fields = HashMap::new();
                attrs.record(&mut FieldVisitor(&mut fields));
                self.spans.lock().expect("capture lock").push(Captured {
                    name: attrs.metadata().name(),
                    fields,
                });
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = addr.to_string();

        // Accept and drain one send so the write doesn't block.
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut len_buf = [0u8; 4];
            let _ = stream.read_exact(&mut len_buf);
            let total_len = u32::from_le_bytes(len_buf) as usize;
            let mut body = vec![0u8; total_len];
            let _ = stream.read_exact(&mut body);
        });

        let spans_captured = Arc::new(Mutex::new(Vec::<Captured>::new()));
        let layer = CaptureLayer {
            spans: spans_captured.clone(),
        };
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("debug"))
            .with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let mut transport =
                TcpReplicaTransport::connect(&addr_str, Duration::from_secs(5)).unwrap();
            let batch = ReplicaBatch {
                first_sequence: 7,
                ops: vec![ReplicaOp::Freeze {
                    tx_key: key(7),
                    offset: 0,
                    master_generation: 0,
                }],
                trace_ctx: None,
                source_node_id: None,
                cluster_key: 0,
            };
            transport.send_batch(&batch).unwrap();
        });

        handle.join().unwrap();

        let captured = spans_captured.lock().expect("capture lock").clone();
        let send_span = captured
            .iter()
            .find(|s| s.name == "send_batch")
            .expect("no send_batch span captured");

        let field = send_span
            .fields
            .get("replica_addr")
            .expect("send_batch span missing replica_addr field");
        assert_eq!(
            field, &addr_str,
            "replica_addr field should match the peer socket address",
        );
        // And the batch length field is wired for free — validate it too so
        // the assertion isn't a single-signal check.
        assert_eq!(
            send_span.fields.get("batch_len").map(String::as_str),
            Some("1"),
        );
    }
}
