//! TCP server for the TeraSlab binary wire protocol.
//!
//! Accepts client connections, reads request frames, dispatches to the
//! Engine, and writes response frames. One thread per connection.

pub mod dispatch;
pub mod http;

use crate::cluster::coordinator::RunningCluster;
use crate::config::ServerConfig;
use crate::ops::engine::Engine;
use crate::protocol::frame::{RequestFrame, ResponseFrame};
use crate::protocol::opcodes::*;
use crate::redo::RedoLog;
use crate::storage::blobstore::{BlobStore, BlobStreamWriter};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Per-connection state for streaming blob uploads.
///
/// Each active upload session is keyed by txid. Sessions are cleaned up
/// (aborted) when the connection closes.
pub(crate) struct ConnectionState {
    pub(crate) streams: HashMap<[u8; 32], ActiveStream>,
}

/// An in-progress streaming blob upload for a single txid.
pub(crate) struct ActiveStream {
    pub(crate) writer: Box<dyn BlobStreamWriter>,
    pub(crate) bytes_received: u64,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            streams: HashMap::new(),
        }
    }
}

impl Drop for ConnectionState {
    fn drop(&mut self) {
        // Abort any in-progress streams on connection close.
        for (_txid, stream) in self.streams.drain() {
            let _ = stream.writer.abort();
        }
    }
}

/// Running TeraSlab server instance.
pub struct Server {
    engine: Arc<Engine>,
    config: ServerConfig,
    cluster: Option<Arc<RunningCluster>>,
    redo_log: Option<Arc<Mutex<RedoLog>>>,
    blob_store: Option<Arc<dyn BlobStore>>,
    shutdown: Arc<AtomicBool>,
    active_connections: Arc<AtomicUsize>,
}

impl Server {
    /// Create a new server with the given engine and configuration.
    pub fn new(engine: Arc<Engine>, config: ServerConfig) -> Self {
        Self {
            engine,
            config,
            cluster: None,
            redo_log: None,
            blob_store: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Set the cluster coordinator for distributed mode.
    pub fn with_cluster(mut self, cluster: Arc<RunningCluster>) -> Self {
        self.cluster = Some(cluster);
        self
    }

    /// Set the redo log for crash recovery durability.
    pub fn with_redo_log(mut self, redo_log: Arc<Mutex<RedoLog>>) -> Self {
        self.redo_log = Some(redo_log);
        self
    }

    /// Share an external active connection counter with the server.
    ///
    /// The counter is incremented on accept and decremented on disconnect.
    /// This allows other subsystems (like the HTTP server) to observe the
    /// current connection count.
    pub fn with_active_connections(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.active_connections = counter;
        self
    }

    /// Set the blob store for external cold data storage.
    pub fn with_blob_store(mut self, store: Arc<dyn BlobStore>) -> Self {
        self.blob_store = Some(store);
        self
    }

    /// Start listening for client connections. Blocks until shutdown.
    pub fn run(&self) -> Result<(), String> {
        let listener = TcpListener::bind(&self.config.listen_addr)
            .map_err(|e| format!("failed to bind {}: {e}", self.config.listen_addr))?;

        // Set non-blocking so we can check shutdown flag
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("failed to set non-blocking: {e}"))?;

        tracing::info!(listen_addr = %self.config.listen_addr, "TeraSlab server listening");

        while !self.shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, addr)) => {
                    let active = self.active_connections.load(Ordering::Relaxed);
                    if active >= self.config.max_connections {
                        tracing::warn!(peer_addr = %addr, active, "rejecting connection: max connections reached");
                        drop(stream);
                        continue;
                    }

                    self.active_connections.fetch_add(1, Ordering::Relaxed);

                    let engine = self.engine.clone();
                    let shutdown = self.shutdown.clone();
                    let active_conns = self.active_connections.clone();
                    let max_batch = self.config.max_batch_size;
                    let cluster = self.cluster.clone();
                    let redo_log = self.redo_log.clone();
                    let blob_store = self.blob_store.clone();

                    std::thread::spawn(move || {
                        if let Err(e) = handle_connection(
                            stream,
                            &engine,
                            &shutdown,
                            max_batch,
                            cluster.as_deref(),
                            redo_log.as_deref(),
                            blob_store.as_deref(),
                        ) {
                            tracing::warn!(peer_addr = %addr, err = %e, "connection error");
                        }
                        active_conns.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No pending connection — sleep briefly and retry
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => {
                    tracing::warn!(err = %e, "accept error");
                }
            }
        }

        tracing::info!(
            active = self.active_connections.load(Ordering::Relaxed),
            "server shutting down, waiting for active connections to drain",
        );

        // Wait for active connections to drain
        while self.active_connections.load(Ordering::Relaxed) > 0 {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        Ok(())
    }

    /// Signal the server to shut down gracefully.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Whether the server is shutting down.
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Number of active client connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }
}

/// Handle a single client connection: read frames, dispatch, respond.
///
/// Creates a [`ConnectionState`] that tracks in-progress streaming blob
/// uploads. When the connection closes (normally or on error), the
/// `ConnectionState` `Drop` impl aborts any incomplete streams.
fn handle_connection(
    mut stream: TcpStream,
    engine: &Engine,
    shutdown: &AtomicBool,
    max_batch_size: u32,
    cluster: Option<&RunningCluster>,
    redo_log: Option<&Mutex<RedoLog>>,
    blob_store: Option<&dyn BlobStore>,
) -> Result<(), String> {
    stream
        .set_nonblocking(false)
        .map_err(|e| format!("set_nonblocking: {e}"))?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .map_err(|e| format!("set_read_timeout: {e}"))?;

    let mut read_buf = vec![0u8; 256 * 1024]; // 256 KB read buffer
    let mut conn_state = ConnectionState::new();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Read the 4-byte length prefix
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()), // Client disconnected
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(e) => return Err(format!("read length: {e}")),
        }

        let total_length = u32::from_le_bytes(len_buf);
        if total_length > MAX_FRAME_SIZE {
            // Reject oversized frame
            let resp = ResponseFrame {
                request_id: 0,
                status: STATUS_ERROR,
                payload: b"frame too large".to_vec(),
            };
            let _ = stream.write_all(&resp.encode());
            return Err(format!("frame too large: {total_length}"));
        }

        // Read the full frame
        let frame_len = total_length as usize;
        if read_buf.len() < frame_len {
            read_buf.resize(frame_len, 0);
        }
        stream
            .read_exact(&mut read_buf[..frame_len])
            .map_err(|e| format!("read frame body: {e}"))?;

        // Reconstruct the full frame bytes (length prefix + body)
        let mut frame_bytes = Vec::with_capacity(4 + frame_len);
        frame_bytes.extend_from_slice(&len_buf);
        frame_bytes.extend_from_slice(&read_buf[..frame_len]);

        let (request, _) = RequestFrame::decode(&frame_bytes)
            .map_err(|e| format!("decode frame: {e}"))?;

        // Dispatch to handler
        let response = dispatch::handle_request(
            &request,
            engine,
            max_batch_size,
            cluster,
            redo_log,
            &mut conn_state,
            blob_store,
        );

        // Write response
        let response_bytes = response.encode();
        stream
            .write_all(&response_bytes)
            .map_err(|e| format!("write response: {e}"))?;
    }
}
