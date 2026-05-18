//! TCP server for the TeraSlab binary wire protocol.
//!
//! Accepts client connections, reads request frames, dispatches to the
//! Engine, and writes response frames. One thread per connection.

pub mod dispatch;
pub mod http;
pub mod startup;

use crate::cluster::coordinator::RunningCluster;
use crate::config::ServerConfig;
use crate::ops::engine::Engine;
use crate::protocol::codec::encode_error_payload;
use crate::protocol::frame::{RequestFrame, ResponseFrame};
use crate::protocol::opcodes::*;
use crate::redo::RedoLog;
use crate::storage::blobstore::{BlobStore, BlobStreamWriter};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// F-G5-001 (CRITICAL): emit a single `tracing::warn!` the first time an
/// inter-node opcode is received with no `cluster_secret` configured.
/// The default (trusted-overlay) behaviour is fail-open per
/// `_review/FIX_POLICY.md` §2; the warning surfaces the situation so
/// an operator who forgot to wire a secret notices in production logs.
///
/// One-shot — additional unsigned inter-node frames after the first are
/// silently accepted (still subject to the per-frame size / rate caps).
static UNAUTHENTICATED_INTER_NODE_WARNED: AtomicBool = AtomicBool::new(false);

const READ_BUF_RETAINED_SIZE: usize = 256 * 1024;
const CONNECTION_READ_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECTION_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared aggregate cap for request-frame memory across all connection
/// threads. The single-frame `MAX_FRAME_SIZE` guard bounds one allocation;
/// this limiter bounds the sum of frames being read/processed concurrently.
#[derive(Debug)]
pub(crate) struct InflightBytesLimiter {
    limit: usize,
    used: AtomicUsize,
}

#[derive(Debug)]
pub(crate) struct InflightBytesPermit {
    limiter: Arc<InflightBytesLimiter>,
    bytes: usize,
}

impl InflightBytesLimiter {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    pub(crate) fn try_acquire(self: &Arc<Self>, bytes: usize) -> Option<InflightBytesPermit> {
        if self.limit == 0 {
            return Some(InflightBytesPermit {
                limiter: self.clone(),
                bytes: 0,
            });
        }
        if bytes > self.limit {
            return None;
        }

        let mut observed = self.used.load(Ordering::Relaxed);
        loop {
            let next = observed.checked_add(bytes)?;
            if next > self.limit {
                return None;
            }
            match self.used.compare_exchange_weak(
                observed,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(InflightBytesPermit {
                        limiter: self.clone(),
                        bytes,
                    });
                }
                Err(actual) => observed = actual,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

impl Drop for InflightBytesPermit {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.limiter.used.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

/// Per-connection state for streaming blob uploads.
///
/// Each active upload session is keyed by txid. Sessions are cleaned up
/// (aborted) when the connection closes.
pub(crate) struct ConnectionState {
    pub(crate) streams: HashMap<[u8; 32], ActiveStream>,
    pub(crate) max_stream_total_bytes: u64,
}

/// An in-progress streaming blob upload for a single txid.
pub(crate) struct ActiveStream {
    pub(crate) writer: Box<dyn BlobStreamWriter>,
    pub(crate) bytes_received: u64,
}

impl ConnectionState {
    pub(crate) fn new() -> Self {
        Self {
            streams: HashMap::new(),
            max_stream_total_bytes: ServerConfig::DEFAULT_MAX_STREAM_TOTAL_BYTES,
        }
    }

    pub(crate) fn with_max_stream_total_bytes(mut self, max_stream_total_bytes: u64) -> Self {
        self.max_stream_total_bytes = max_stream_total_bytes;
        self
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
    inflight_request_bytes: Arc<InflightBytesLimiter>,
    /// P1.2: mio `Waker` that, when triggered, wakes the accept-loop
    /// poller (kqueue on macOS / epoll on Linux). Populated by
    /// [`Server::run`] just before entering the loop and consumed by
    /// [`Server::shutdown`]. Pre-fix the loop relied on
    /// `thread::sleep(10ms)` between `accept()` retries, burning CPU on
    /// idle listeners and slowing shutdown to one sleep-tick.
    shutdown_waker: Mutex<Option<Arc<mio::Waker>>>,
}

impl Server {
    /// Create a new server with the given engine and configuration.
    pub fn new(engine: Arc<Engine>, config: ServerConfig) -> Self {
        let inflight_request_bytes =
            Arc::new(InflightBytesLimiter::new(config.max_inflight_request_bytes));
        Self {
            engine,
            config,
            cluster: None,
            redo_log: None,
            blob_store: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            active_connections: Arc::new(AtomicUsize::new(0)),
            inflight_request_bytes,
            shutdown_waker: Mutex::new(None),
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
        // P1.2: bind once with the std listener (it owns the SO_REUSEADDR /
        // bind-error reporting we already surface in tests), then convert
        // to a mio source. mio requires the FD to be non-blocking, which
        // `TcpListener::bind` defaults to *blocking*; flip it before
        // registering.
        let std_listener = TcpListener::bind(&self.config.listen_addr)
            .map_err(|e| format!("failed to bind {}: {e}", self.config.listen_addr))?;
        std_listener
            .set_nonblocking(true)
            .map_err(|e| format!("failed to set non-blocking: {e}"))?;
        let mut mio_listener = mio::net::TcpListener::from_std(std_listener);

        // P1.2: build the poller and register both the listener and a
        // `Waker` (self-pipe abstraction on Linux/macOS — eventfd or
        // pipe2). `Server::shutdown` calls `Waker::wake`, which posts a
        // readiness event; the loop exits within microseconds rather than
        // waiting for the next 10 ms `thread::sleep` tick.
        const LISTENER_TOKEN: mio::Token = mio::Token(0);
        const SHUTDOWN_TOKEN: mio::Token = mio::Token(1);
        let mut poll = mio::Poll::new().map_err(|e| format!("mio::Poll::new: {e}"))?;
        poll.registry()
            .register(
                &mut mio_listener,
                LISTENER_TOKEN,
                mio::Interest::READABLE,
            )
            .map_err(|e| format!("register listener: {e}"))?;
        let waker = Arc::new(
            mio::Waker::new(poll.registry(), SHUTDOWN_TOKEN)
                .map_err(|e| format!("mio::Waker::new: {e}"))?,
        );
        // Publish the waker so `Server::shutdown` can wake the loop.
        // Stored before the loop starts so a fast `shutdown()` immediately
        // after `run()` enters cannot race past an empty handle. If
        // `shutdown()` ran *before* publish, the `shutdown` flag is
        // already true and the initial `if shutdown.load(...)` check
        // below short-circuits.
        *self.shutdown_waker.lock() = Some(waker.clone());
        // Pre-allocate the event buffer outside the loop.
        let mut events = mio::Events::with_capacity(16);

        tracing::info!(listen_addr = %self.config.listen_addr, "TeraSlab server listening");

        'accept_loop: while !self.shutdown.load(Ordering::Relaxed) {
            // Block until either the listener becomes readable or the
            // waker fires. `None` timeout means "wait forever"; the
            // waker guarantees we'll wake on shutdown, so there is no
            // CPU spin on an idle listener.
            if let Err(e) = poll.poll(&mut events, None) {
                if e.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(format!("mio poll: {e}"));
            }
            let mut listener_ready = false;
            for event in events.iter() {
                match event.token() {
                    LISTENER_TOKEN => listener_ready = true,
                    SHUTDOWN_TOKEN => {
                        // Waker fired — re-check the flag at the top of
                        // the loop. Spurious wakeups are harmless.
                    }
                    _ => {}
                }
            }
            if !listener_ready {
                continue 'accept_loop;
            }
            // Drain all pending accepts so a single readiness edge
            // doesn't leak connections (mio reports edge-triggered on
            // some platforms).
            loop {
                match mio_listener.accept() {
                    Ok((stream, addr)) => {
                        // Convert mio stream back to std::net::TcpStream so
                        // the rest of the connection handler (which uses
                        // blocking read_exact / write_all) is unchanged.
                        let std_stream: TcpStream = {
                            #[cfg(unix)]
                            {
                                use std::os::unix::io::{FromRawFd, IntoRawFd};
                                // SAFETY: mio::net::TcpStream is a thin
                                // wrapper around the OS FD; `into_raw_fd`
                                // transfers ownership of the FD out of mio
                                // and `from_raw_fd` takes it into std. No
                                // double-free.
                                unsafe { TcpStream::from_raw_fd(stream.into_raw_fd()) }
                            }
                            #[cfg(not(unix))]
                            {
                                // No non-Unix targets currently supported.
                                compile_error!("server accept loop requires a Unix target");
                            }
                        };
                        // Hand off to the existing accept-handler. Move
                        // the stream into a local variable named
                        // `mut stream` so the rest of the body (taken
                        // verbatim from pre-fix) compiles unchanged.
                        let mut stream = std_stream;
                        // Restore blocking mode for the per-connection
                        // thread; `handle_connection_inner` also flips
                        // it but flipping back here lets the timeout
                        // settings in the `max_connections` reject path
                        // work the same as pre-fix.
                        let _ = stream.set_nonblocking(false);
                        let active = self.active_connections.load(Ordering::Relaxed);
                        if active >= self.config.max_connections {
                            tracing::warn!(peer_addr = %addr, active, "rejecting connection: max connections reached");
                            let _ = stream.set_write_timeout(Some(CONNECTION_WRITE_TIMEOUT));
                            let response = ResponseFrame {
                                request_id: 0,
                                status: STATUS_ERROR,
                                payload: encode_error_payload(ERR_INTERNAL, "max connections reached"),
                            };
                            let _ = stream.write_all(&response.encode());
                            drop(stream);
                            continue;
                        }

                        self.active_connections.fetch_add(1, Ordering::Relaxed);

                        let engine = self.engine.clone();
                        let shutdown = self.shutdown.clone();
                        let active_conns = self.active_connections.clone();
                        let max_batch = self.config.max_batch_size;
                        let max_stream_total_bytes = self.config.max_stream_total_bytes;
                        let cluster = self.cluster.clone();
                        let redo_log = self.redo_log.clone();
                        let blob_store = self.blob_store.clone();
                        let inflight_request_bytes = self.inflight_request_bytes.clone();
                        let cluster_secret = self
                            .config
                            .cluster_secret
                            .as_ref()
                            .map(|s| Arc::new(s.as_bytes().to_vec()));
                        // F-G5-001 (CRITICAL): wire G10's `ServerConfig::strict_auth`
                        // into the connection-handler options. When `true`, any
                        // inter-node opcode that arrives without a `cluster_secret`
                        // is rejected with `ERR_CLUSTER_AUTH_FAILED`. Default
                        // remains `false` (trusted-overlay) per FIX_POLICY §2.
                        let strict_auth = self.config.strict_auth;

                        std::thread::spawn(move || {
                            if let Err(e) = handle_connection_inner(
                                stream,
                                &engine,
                                &shutdown,
                                ConnectionOptions {
                                    max_batch_size: max_batch,
                                    max_stream_total_bytes,
                                    cluster: cluster.as_deref(),
                                    redo_log: redo_log.as_deref(),
                                    blob_store: blob_store.as_deref(),
                                    inflight_request_bytes,
                                    cluster_secret,
                                    strict_auth,
                                    read_timeout: CONNECTION_READ_TIMEOUT,
                                    write_timeout: CONNECTION_WRITE_TIMEOUT,
                                },
                            ) {
                                tracing::warn!(peer_addr = %addr, err = %e, "connection error");
                            }
                            active_conns.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No more pending connections — go back to poll.
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(err = %e, "accept error");
                        break;
                    }
                }
            }
        }
        // Drop the published waker so a late `shutdown()` after the loop
        // exits is a harmless no-op (the AtomicBool is already true).
        *self.shutdown_waker.lock() = None;

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
    ///
    /// P1.2: flips the shutdown flag *and* wakes the mio poller (via the
    /// registered [`mio::Waker`]). The accept loop observes the flag on
    /// the next iteration and exits, typically within microseconds rather
    /// than the worst-case 10 ms it used to spin-wait for.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(waker) = self.shutdown_waker.lock().as_ref() {
            // `Waker::wake` is best-effort: a failure here means the FD
            // backing it has already been closed (e.g. `run` already
            // returned), which is the expected race when shutdown is
            // called after a clean exit.
            if let Err(e) = waker.wake() {
                tracing::debug!(err = %e, "Server::shutdown: Waker::wake failed (already shut down?)");
            }
        }
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

struct ConnectionOptions<'a> {
    max_batch_size: u32,
    max_stream_total_bytes: u64,
    cluster: Option<&'a RunningCluster>,
    redo_log: Option<&'a Mutex<RedoLog>>,
    blob_store: Option<&'a dyn BlobStore>,
    inflight_request_bytes: Arc<InflightBytesLimiter>,
    cluster_secret: Option<Arc<Vec<u8>>>,
    /// F-G5-001 (CRITICAL): when `true`, an inter-node opcode that arrives
    /// without a `cluster_secret` configured is rejected with
    /// `ERR_CLUSTER_AUTH_FAILED`. When `false` (the default trusted-overlay
    /// behaviour), the frame is accepted unauthenticated — a one-shot
    /// warning is emitted via [`UNAUTHENTICATED_INTER_NODE_WARNED`].
    ///
    /// Orchestrator-wired: G10 owns `ServerConfig::strict_auth` and the
    /// CLI `--strict-auth` flag; this field is the local read-site.
    strict_auth: bool,
    read_timeout: Duration,
    write_timeout: Duration,
}

/// Handle a single client connection: read frames, dispatch, respond.
///
/// Creates a [`ConnectionState`] that tracks in-progress streaming blob
/// uploads. When the connection closes (normally or on error), the
/// `ConnectionState` `Drop` impl aborts any incomplete streams.
fn handle_connection_inner(
    mut stream: TcpStream,
    engine: &Engine,
    shutdown: &AtomicBool,
    opts: ConnectionOptions<'_>,
) -> Result<(), String> {
    stream
        .set_nonblocking(false)
        .map_err(|e| format!("set_nonblocking: {e}"))?;
    stream
        .set_read_timeout(Some(opts.read_timeout))
        .map_err(|e| format!("set_read_timeout: {e}"))?;
    // R-054 (LMNH-01 / Codex F13): cap write time so a slow-reader
    // client cannot pin a server thread indefinitely. Pre-fix
    // `write_all` blocked forever waiting for the client to drain its
    // recv buffer; ~`max_connections` slow readers exhausted the
    // connection thread budget and DoS'd the master. 30 s matches the
    // read timeout above; both should be longer than typical
    // client-side handler latency but short enough that operators
    // notice stuck connections promptly.
    stream
        .set_write_timeout(Some(opts.write_timeout))
        .map_err(|e| format!("set_write_timeout: {e}"))?;

    let mut read_buf = vec![0u8; READ_BUF_RETAINED_SIZE];
    let mut conn_state =
        ConnectionState::new().with_max_stream_total_bytes(opts.max_stream_total_bytes);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Read the 4-byte length prefix
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()), // Client disconnected
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => return Ok(()),
            Err(e) => return Err(format!("read length: {e}")),
        }

        // Reject oversized frames BEFORE any per-connection buffer
        // allocation. The advertised `total_length` is attacker-controlled
        // up to 4 GiB; without this guard, a single hostile client could
        // drive the per-connection `read_buf.resize(frame_len, ..)` up to
        // multi-gigabyte allocations before any decoding occurs (gap #10
        // in TERANODE_PRODUCTION_READINESS_GAPS.md).
        let total_length = u32::from_le_bytes(len_buf);
        let max_wire_frame_size = MAX_FRAME_SIZE
            + opts
                .cluster_secret
                .as_ref()
                .map(|_| crate::cluster::auth::SIGNED_SUFFIX_LEN as u32)
                .unwrap_or(0);
        if total_length > max_wire_frame_size {
            let resp = ResponseFrame {
                request_id: 0,
                status: STATUS_ERROR,
                payload: b"frame too large".to_vec(),
            };
            let _ = stream.write_all(&resp.encode());
            return Err(format!(
                "frame too large: {total_length} > MAX_FRAME_SIZE {max_wire_frame_size}"
            ));
        }

        // Read the full frame. The `frame_len` is now guaranteed to be
        // <= `MAX_FRAME_SIZE`, so the buffer growth is bounded regardless
        // of how many concurrent connections advertise large frames.
        let frame_len = total_length as usize;
        let _inflight_permit = match opts.inflight_request_bytes.try_acquire(frame_len) {
            Some(permit) => permit,
            None => {
                let resp = ResponseFrame {
                    request_id: 0,
                    status: STATUS_ERROR,
                    payload: encode_error_payload(
                        ERR_INTERNAL,
                        "aggregate in-flight request memory limit exceeded",
                    ),
                };
                let _ = stream.write_all(&resp.encode());
                return Err(format!(
                    "aggregate in-flight request memory limit exceeded: requested {frame_len} bytes"
                ));
            }
        };
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

        let request_id = peek_request_id(&frame_bytes).unwrap_or(0);
        let peeked_op = peek_request_op_code(&frame_bytes);
        let is_inter_node_op = peeked_op.map(is_inter_node_auth_opcode).unwrap_or(false);
        let auth_required = is_inter_node_op && opts.cluster_secret.is_some();
        // F-G5-001 (CRITICAL): inter-node opcode arrived with no
        // `cluster_secret`. Default behaviour is fail-open (trusted
        // overlay, FIX_POLICY §2); opt-in `strict_auth` rejects.
        // Either way, surface the first unauthenticated event in logs.
        if is_inter_node_op && opts.cluster_secret.is_none() {
            if opts.strict_auth {
                let op_code = peeked_op.unwrap_or(0);
                let resp = ResponseFrame {
                    request_id,
                    status: STATUS_ERROR,
                    payload: encode_error_payload(
                        ERR_CLUSTER_AUTH_FAILED,
                        "strict_auth: cluster_secret required for inter-node opcode",
                    ),
                };
                let _ = stream.write_all(&resp.encode());
                return Err(format!(
                    "strict_auth: rejecting unsigned inter-node op_code={op_code}"
                ));
            }
            if !UNAUTHENTICATED_INTER_NODE_WARNED.swap(true, Ordering::AcqRel) {
                let op_code = peeked_op.unwrap_or(0);
                tracing::warn!(
                    target: "teraslab::security",
                    op_code,
                    "inter-node opcode received without cluster_secret configured — \
                     accepting unauthenticated frame (trusted-overlay default). \
                     Configure `cluster_secret` or pass `--strict-auth` to enforce.",
                );
            }
        }
        let request_frame_bytes = if auth_required {
            match crate::cluster::auth::verify_frame(
                opts.cluster_secret
                    .as_ref()
                    .expect("checked above")
                    .as_slice(),
                &frame_bytes,
            ) {
                Ok(verified) => verified,
                Err(e) => {
                    let resp = ResponseFrame {
                        request_id,
                        status: STATUS_ERROR,
                        payload: encode_error_payload(
                            ERR_CLUSTER_AUTH_FAILED,
                            &format!("cluster frame authentication failed: {e}"),
                        ),
                    };
                    let _ = stream.write_all(&resp.encode());
                    return Err(format!("cluster frame authentication failed: {e}"));
                }
            }
        } else {
            frame_bytes
        };

        let (request, _) =
            RequestFrame::decode(&request_frame_bytes).map_err(|e| format!("decode frame: {e}"))?;

        // Dispatch to handler
        let response = dispatch::handle_request(
            &request,
            engine,
            opts.max_batch_size,
            opts.cluster,
            opts.redo_log,
            &mut conn_state,
            opts.blob_store,
        );

        // Write response
        let encoded_response = response.encode();
        let response_bytes = if auth_required {
            crate::cluster::auth::sign_frame(
                opts.cluster_secret
                    .as_ref()
                    .expect("checked above")
                    .as_slice(),
                &encoded_response,
            )
            .map_err(|e| format!("sign response frame: {e}"))?
        } else {
            encoded_response
        };
        stream
            .write_all(&response_bytes)
            .map_err(|e| format!("write response: {e}"))?;
        reset_read_buf_if_oversized(&mut read_buf);
    }
}

fn peek_request_id(frame_bytes: &[u8]) -> Option<u64> {
    if frame_bytes.len() < 12 {
        return None;
    }
    Some(u64::from_le_bytes(frame_bytes[4..12].try_into().ok()?))
}

fn peek_request_op_code(frame_bytes: &[u8]) -> Option<u16> {
    if frame_bytes.len() < 14 {
        return None;
    }
    Some(u16::from_le_bytes(frame_bytes[12..14].try_into().ok()?))
}

fn reset_read_buf_if_oversized(read_buf: &mut Vec<u8>) {
    if read_buf.capacity() > READ_BUF_RETAINED_SIZE {
        *read_buf = vec![0u8; READ_BUF_RETAINED_SIZE];
    } else if read_buf.len() != READ_BUF_RETAINED_SIZE {
        read_buf.resize(READ_BUF_RETAINED_SIZE, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::{BlockDevice, MemoryDevice};
    use crate::index::{DahIndex, Index, UnminedIndex};
    use crate::locks::StripedLocks;

    fn test_engine() -> Engine {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        Engine::new(
            dev,
            Index::new(1024).unwrap(),
            alloc,
            StripedLocks::new(64),
            DahIndex::new(),
            UnminedIndex::new(),
        )
    }

    #[test]
    fn read_buf_shrinks_after_small_frame() {
        let mut read_buf = vec![0u8; READ_BUF_RETAINED_SIZE * 4];
        assert!(read_buf.capacity() > READ_BUF_RETAINED_SIZE);

        reset_read_buf_if_oversized(&mut read_buf);

        assert_eq!(read_buf.len(), READ_BUF_RETAINED_SIZE);
        assert_eq!(read_buf.capacity(), READ_BUF_RETAINED_SIZE);
    }

    #[test]
    fn silent_client_dropped_after_idle_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(test_engine());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel();

        let server_engine = engine.clone();
        let server_shutdown = shutdown.clone();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let result = handle_connection_inner(
                stream,
                &server_engine,
                &server_shutdown,
                ConnectionOptions {
                    max_batch_size: 1024,
                    max_stream_total_bytes: ServerConfig::DEFAULT_MAX_STREAM_TOTAL_BYTES,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: None,
                    strict_auth: false,
                    read_timeout: Duration::from_millis(50),
                    write_timeout: Duration::from_secs(1),
                },
            );
            tx.send(result).unwrap();
        });

        let _client = TcpStream::connect(addr).unwrap();
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("silent client should be dropped after read timeout");
        assert!(result.is_ok(), "connection result was {result:?}");
    }

    #[test]
    fn unsigned_inter_node_frame_rejected_when_cluster_secret_configured() {
        assert_unsigned_protected_opcode_rejected(OP_REPLICA_BATCH);
    }

    #[test]
    fn unsigned_topology_frame_rejected_when_cluster_secret_configured() {
        assert_unsigned_protected_opcode_rejected(OP_TOPOLOGY_COMMIT);
    }

    #[test]
    fn unsigned_migration_frame_rejected_when_cluster_secret_configured() {
        assert_unsigned_protected_opcode_rejected(OP_MIGRATION_COMPLETE);
    }

    fn assert_unsigned_protected_opcode_rejected(op_code: u16) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(test_engine());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel();

        let server_engine = engine.clone();
        let server_shutdown = shutdown.clone();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let result = handle_connection_inner(
                stream,
                &server_engine,
                &server_shutdown,
                ConnectionOptions {
                    max_batch_size: 1024,
                    max_stream_total_bytes: ServerConfig::DEFAULT_MAX_STREAM_TOTAL_BYTES,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: Some(Arc::new(b"cluster-secret".to_vec())),
                    strict_auth: false,
                    read_timeout: Duration::from_secs(1),
                    write_timeout: Duration::from_secs(1),
                },
            );
            tx.send(result).unwrap();
        });

        let mut client = TcpStream::connect(addr).unwrap();
        let request = RequestFrame {
            request_id: 7,
            op_code,
            flags: 0,
            payload: Vec::new(),
        };
        client.write_all(&request.encode()).unwrap();

        let response = read_response_frame_for_test(&mut client);
        assert_eq!(response.request_id, 7);
        assert_eq!(response.status, STATUS_ERROR);
        assert_eq!(
            u16::from_le_bytes(response.payload[0..2].try_into().unwrap()),
            ERR_CLUSTER_AUTH_FAILED
        );
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("server should return after auth failure");
        assert!(result.is_err(), "auth failure should close connection");
    }

    #[test]
    fn signed_inter_node_frame_receives_signed_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(test_engine());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel();

        let server_engine = engine.clone();
        let server_shutdown = shutdown.clone();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let result = handle_connection_inner(
                stream,
                &server_engine,
                &server_shutdown,
                ConnectionOptions {
                    max_batch_size: 1024,
                    max_stream_total_bytes: ServerConfig::DEFAULT_MAX_STREAM_TOTAL_BYTES,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: Some(Arc::new(b"cluster-secret".to_vec())),
                    strict_auth: false,
                    read_timeout: Duration::from_secs(1),
                    write_timeout: Duration::from_secs(1),
                },
            );
            tx.send(result).unwrap();
        });

        let mut client = TcpStream::connect(addr).unwrap();
        let request = RequestFrame {
            request_id: 8,
            op_code: OP_GET_PARTITION_MAP,
            flags: 0,
            payload: Vec::new(),
        };
        let signed = crate::cluster::auth::sign_frame(b"cluster-secret", &request.encode())
            .expect("request signing");
        client.write_all(&signed).unwrap();

        let signed_response = read_raw_frame_for_test(&mut client);
        assert!(
            signed_response.len() >= crate::cluster::auth::SIGNED_SUFFIX_LEN + 4,
            "signed response should carry the auth suffix"
        );
        let verified =
            crate::cluster::auth::verify_frame(b"cluster-secret", &signed_response).unwrap();
        let (response, consumed) = ResponseFrame::decode(&verified).unwrap();
        assert_eq!(consumed, verified.len());
        assert_eq!(response.request_id, 8);
        assert_eq!(response.status, STATUS_OK);

        drop(client);
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("server should exit after client disconnect");
        assert!(result.is_ok(), "connection result was {result:?}");
    }

    #[test]
    fn inflight_request_limiter_caps_aggregate_bytes() {
        let limiter = Arc::new(InflightBytesLimiter::new(16));
        let first = limiter.try_acquire(10).expect("first permit");
        assert_eq!(limiter.used(), 10);
        assert!(
            limiter.try_acquire(7).is_none(),
            "second permit should exceed aggregate cap"
        );
        drop(first);
        assert_eq!(limiter.used(), 0);
        let second = limiter.try_acquire(16).expect("full-cap permit");
        assert_eq!(limiter.used(), 16);
        drop(second);
        assert_eq!(limiter.used(), 0);
    }

    fn read_raw_frame_for_test(stream: &mut TcpStream) -> Vec<u8> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let total_len = u32::from_le_bytes(len_buf) as usize;
        let mut body = vec![0u8; total_len];
        stream.read_exact(&mut body).unwrap();
        let mut full = Vec::with_capacity(4 + total_len);
        full.extend_from_slice(&len_buf);
        full.extend_from_slice(&body);
        full
    }

    fn read_response_frame_for_test(stream: &mut TcpStream) -> ResponseFrame {
        let full = read_raw_frame_for_test(stream);
        let (response, consumed) = ResponseFrame::decode(&full).unwrap();
        assert_eq!(consumed, full.len());
        response
    }

    /// F-G5-001 (CRITICAL): with `strict_auth = true` AND `cluster_secret =
    /// None`, an inter-node opcode must be rejected with
    /// `ERR_CLUSTER_AUTH_FAILED` rather than silently accepted.
    #[test]
    fn strict_auth_rejects_inter_node_op_when_secret_missing() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(test_engine());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel();

        let server_engine = engine.clone();
        let server_shutdown = shutdown.clone();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let result = handle_connection_inner(
                stream,
                &server_engine,
                &server_shutdown,
                ConnectionOptions {
                    max_batch_size: 1024,
                    max_stream_total_bytes: ServerConfig::DEFAULT_MAX_STREAM_TOTAL_BYTES,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: None,
                    strict_auth: true,
                    read_timeout: Duration::from_secs(1),
                    write_timeout: Duration::from_secs(1),
                },
            );
            tx.send(result).unwrap();
        });

        let mut client = TcpStream::connect(addr).unwrap();
        let request = RequestFrame {
            request_id: 42,
            op_code: OP_TOPOLOGY_COMMIT,
            flags: 0,
            payload: Vec::new(),
        };
        client.write_all(&request.encode()).unwrap();

        let response = read_response_frame_for_test(&mut client);
        assert_eq!(response.request_id, 42);
        assert_eq!(response.status, STATUS_ERROR);
        assert_eq!(
            u16::from_le_bytes(response.payload[0..2].try_into().unwrap()),
            ERR_CLUSTER_AUTH_FAILED
        );
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("server should return after strict-auth rejection");
        assert!(
            result.is_err(),
            "strict_auth rejection should close connection: {result:?}",
        );
    }

    /// F-G5-001 (CRITICAL): with `strict_auth = false` (default trusted-
    /// overlay), an inter-node opcode without `cluster_secret` is accepted
    /// — the warning is emitted once and dispatch proceeds.
    #[test]
    fn fail_open_accepts_inter_node_op_when_secret_missing() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(test_engine());
        let shutdown = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel();

        let server_engine = engine.clone();
        let server_shutdown = shutdown.clone();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let result = handle_connection_inner(
                stream,
                &server_engine,
                &server_shutdown,
                ConnectionOptions {
                    max_batch_size: 1024,
                    max_stream_total_bytes: ServerConfig::DEFAULT_MAX_STREAM_TOTAL_BYTES,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: None,
                    strict_auth: false,
                    read_timeout: Duration::from_secs(1),
                    write_timeout: Duration::from_secs(1),
                },
            );
            tx.send(result).unwrap();
        });

        let mut client = TcpStream::connect(addr).unwrap();
        // GET_PARTITION_MAP is an inter-node opcode but the single-node
        // dispatch path returns STATUS_OK with a 1-node trivial partition
        // map. Asserting STATUS_OK confirms the frame reached dispatch
        // (i.e. was not rejected by the auth gate).
        let request = RequestFrame {
            request_id: 9,
            op_code: OP_GET_PARTITION_MAP,
            flags: 0,
            payload: Vec::new(),
        };
        client.write_all(&request.encode()).unwrap();

        let response = read_response_frame_for_test(&mut client);
        assert_eq!(response.request_id, 9);
        assert_eq!(response.status, STATUS_OK);

        drop(client);
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("server should return after client disconnect");
        assert!(result.is_ok(), "fail-open accepted result was {result:?}");
    }
}
