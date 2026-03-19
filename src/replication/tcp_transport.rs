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
    /// Sets both read and write timeouts on the resulting stream.
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
        Ok(Self {
            stream,
            request_id: 0,
        })
    }

    /// Wrap an existing `TcpStream` (for testing or when the connection is
    /// already established, e.g. from an accepted socket).
    pub fn from_stream(stream: TcpStream) -> Self {
        Self {
            stream,
            request_id: 0,
        }
    }
}

impl ReplicaTransport for TcpReplicaTransport {
    /// Send a `ReplicaBatch` to the replica as a wire-protocol request frame.
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
        let _ = self
            .stream
            .set_read_timeout(Some(Duration::from_millis(1)));
        let mut probe = [0u8; 1];
        let connected = match self.stream.peek(&mut probe) {
            Ok(0) => false,             // EOF — peer closed
            Ok(_) => true,              // Data available
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
            }],
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
        assert_eq!(ack, ReplicaAck::Ok { through_sequence: 1 });

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

        let stream =
            TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        let mut transport = TcpReplicaTransport::from_stream(stream);

        let batch = ReplicaBatch {
            first_sequence: 1,
            ops: vec![ReplicaOp::Freeze {
                tx_key: key(1),
                offset: 0,
            }],
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

        let stream =
            TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
        let transport = TcpReplicaTransport::from_stream(stream);
        assert!(transport.is_connected());

        handle.join().unwrap();
    }

    #[test]
    fn connect_to_invalid_addr_returns_error() {
        let result = TcpReplicaTransport::connect("not_a_valid_address", Duration::from_secs(1));
        assert!(matches!(result, Err(ReplicationError::Transport(_))));
    }
}
