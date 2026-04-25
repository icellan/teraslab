//! Pipelined TCP connection for the TeraSlab wire protocol.
//!
//! Each [`PipeConn`] multiplexes many in-flight requests over a single TCP
//! connection, matching responses to callers by `request_id`. This mirrors the
//! Go client's `pipeConn`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use teraslab::protocol::frame::{RequestFrame, ResponseFrame};
use teraslab::protocol::opcodes::MAX_FRAME_SIZE;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::errors::ClientError;

/// A pipelined TCP connection that supports multiple concurrent in-flight
/// requests matched by `request_id`.
///
/// The connection runs a background read loop (spawned Tokio task) that
/// dispatches incoming response frames to the callers waiting on oneshot
/// channels.
pub(crate) struct PipeConn {
    /// The write half, protected by a mutex to serialize writes.
    writer: tokio::sync::Mutex<tokio::io::WriteHalf<TcpStream>>,
    /// Map of pending request_id -> oneshot sender for response delivery.
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<ResponseFrame>>>>,
    /// Atomically incrementing request ID counter.
    next_id: AtomicU64,
    /// Whether this connection is still alive, shared with the read loop.
    alive: Arc<AtomicBool>,
    /// Handle to the background read loop task. Kept alive until dropped.
    _read_task: JoinHandle<()>,
}

impl PipeConn {
    /// Establish a new pipelined connection to the given address.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connection`] if the TCP connection fails or times out.
    pub async fn dial(addr: &str, timeout: Duration) -> Result<Self, ClientError> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ClientError::Connection(format!("dial timeout: {}", addr)))?
            .map_err(|e| ClientError::Connection(format!("dial {}: {}", addr, e)))?;

        // Disable Nagle's algorithm for lower latency.
        stream
            .set_nodelay(true)
            .map_err(|e| ClientError::Connection(format!("set_nodelay: {}", e)))?;

        let (reader, writer) = tokio::io::split(stream);

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<ResponseFrame>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let alive = Arc::new(AtomicBool::new(true));

        let read_pending = Arc::clone(&pending);
        let read_alive = Arc::clone(&alive);

        let read_task = tokio::spawn(async move {
            read_loop(reader, read_pending, read_alive).await;
        });

        Ok(Self {
            writer: tokio::sync::Mutex::new(writer),
            pending,
            next_id: AtomicU64::new(0),
            alive,
            _read_task: read_task,
        })
    }

    /// Send a request and wait for its response.
    ///
    /// The `request_id` is assigned automatically. The method serializes the
    /// request frame, writes it under the write lock, and awaits the response
    /// on a oneshot channel.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connection`] if the connection is dead or the
    /// write fails, or [`ClientError::Timeout`] if the default timeout elapses.
    pub async fn round_trip(
        &self,
        op_code: u16,
        flags: u16,
        payload: Vec<u8>,
    ) -> Result<ResponseFrame, ClientError> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(ClientError::Connection("connection closed".to_string()));
        }

        let req_id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = oneshot::channel();

        // Register the pending request.
        {
            let mut pending = self.pending.lock();
            pending.insert(req_id, tx);
        }

        // Encode and write the request frame.
        let frame = RequestFrame {
            request_id: req_id,
            op_code,
            flags,
            payload,
        };
        let encoded = frame.encode();

        {
            let mut writer = self.writer.lock().await;
            if let Err(e) = writer.write_all(&encoded).await {
                self.pending.lock().remove(&req_id);
                return Err(ClientError::Connection(format!("write: {}", e)));
            }
        }

        // Wait for the response with a generous timeout.
        match tokio::time::timeout(Duration::from_secs(30), rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => {
                // Sender was dropped (connection closed).
                Err(ClientError::Connection("connection closed".to_string()))
            }
            Err(_) => {
                self.pending.lock().remove(&req_id);
                Err(ClientError::Timeout)
            }
        }
    }

    /// Returns true if the connection is still alive.
    pub fn alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// Close this connection, waking all pending callers.
    pub async fn close(&self) {
        self.alive.store(false, Ordering::Release);
        // Drop all pending senders to wake waiters.
        let mut pending = self.pending.lock();
        pending.clear();
    }
}

impl Drop for PipeConn {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        self._read_task.abort();
    }
}

/// Background read loop that reads response frames from the TCP stream
/// and dispatches them to the corresponding pending oneshot senders.
async fn read_loop(
    mut reader: tokio::io::ReadHalf<TcpStream>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<ResponseFrame>>>>,
    alive: Arc<AtomicBool>,
) {
    let mut len_buf = [0u8; 4];
    loop {
        // Read 4-byte length prefix.
        if reader.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let total_length = u32::from_le_bytes(len_buf);
        if !(9..=MAX_FRAME_SIZE).contains(&total_length) {
            break;
        }

        // Read the body.
        let body_len = total_length as usize;
        let mut body = vec![0u8; body_len];
        if reader.read_exact(&mut body).await.is_err() {
            break;
        }

        // Decode: request_id(8) + status(1) + payload
        if body.len() < 9 {
            break;
        }
        let request_id = u64::from_le_bytes(body[0..8].try_into().unwrap());
        let status = body[8];
        let payload = body[9..].to_vec();

        let resp = ResponseFrame {
            request_id,
            status,
            payload,
        };

        // Look up and dispatch.
        let sender = {
            let mut map = pending.lock();
            map.remove(&request_id)
        };
        if let Some(tx) = sender {
            // Ignore send errors (caller may have timed out and dropped the receiver).
            let _ = tx.send(resp);
        }
    }

    // Connection is dead. Mark alive as false and wake all pending callers.
    alive.store(false, Ordering::Release);

    // Drop all pending senders so callers get an error.
    let mut map = pending.lock();
    map.clear();
}
