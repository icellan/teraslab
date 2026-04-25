//! TCP-based transport for replication.
//!
//! Wraps a `TcpStream` and uses the TeraSlab wire protocol frames:
//! - Master to Replica: `RequestFrame` with `op_code=OP_REPLICA_BATCH`, payload = batch bytes
//! - Replica to Master: `ResponseFrame` with `status=STATUS_OK`, payload = ack bytes

use crate::protocol::frame::{RequestFrame, ResponseFrame};
use crate::protocol::opcodes::*;
use crate::replication::manager::ReplicationError;
use crate::replication::protocol::{ReplicaAck, ReplicaBatch};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use super::manager::ReplicaTransport;

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
}

impl TcpReplicaTransport {
    /// Connect to a replica at the given address with a timeout.
    ///
    /// Sets read/write timeouts and enables TCP keepalive so broken
    /// connections (e.g. after container SIGKILL) are detected quickly
    /// instead of waiting for the OS default keepalive timeout (minutes).
    pub fn connect(addr: &str, timeout: Duration) -> Result<Self, ReplicationError> {
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
        })
    }

    /// Wrap an existing `TcpStream` (for testing or when the connection is
    /// already established, e.g. from an accepted socket).
    ///
    /// Best-effort enables `TCP_NODELAY` so streams wrapped here receive
    /// the same low-latency behavior as `connect()`. Any OS error is
    /// ignored because the stream is already usable without the option.
    pub fn from_stream(stream: TcpStream) -> Self {
        let _ = stream.set_nodelay(true);
        Self {
            stream,
            request_id: 0,
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
        let bytes = frame.encode();
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

        if total_len as u32 > MAX_FRAME_SIZE {
            return Err(ReplicationError::Transport(format!(
                "response frame too large: {total_len}"
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

        let (resp, _) = ResponseFrame::decode(&full)
            .map_err(|e| ReplicationError::Transport(format!("decode response frame: {e}")))?;

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
    /// TCP does not provide a clean "is alive" test without writing data.
    /// This performs a non-blocking peek attempt; a clean connection that
    /// has no pending data will return `WouldBlock`, which we treat as
    /// connected.
    fn is_connected(&self) -> bool {
        // Save current timeout, set to very short for the probe
        let orig = self.stream.read_timeout().ok().flatten();
        let _ = self.stream.set_read_timeout(Some(Duration::from_millis(1)));
        let mut probe = [0u8; 1];
        let connected = match self.stream.peek(&mut probe) {
            Ok(0) => false, // EOF — peer closed
            Ok(_) => true,  // Data available
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => true,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => true,
            Err(_) => false,
        };
        let _ = self.stream.set_read_timeout(orig);
        connected
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
        };
        transport.send_batch(&batch).unwrap();

        let result = transport.recv_ack(Duration::from_millis(100));
        assert!(matches!(result, Err(ReplicationError::Timeout(_))));

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
