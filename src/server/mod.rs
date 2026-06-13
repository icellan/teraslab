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
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// F-G5-001 (CRITICAL): emit a single `tracing::warn!` the first time an
/// inter-node opcode is received with no `cluster_secret` configured.
/// The default (trusted-overlay) behaviour is fail-open by design;
/// the warning surfaces the situation so
/// an operator who forgot to wire a secret notices in production logs.
///
/// One-shot — additional unsigned inter-node frames after the first are
/// silently accepted (still subject to the per-frame size / rate caps).
static UNAUTHENTICATED_INTER_NODE_WARNED: AtomicBool = AtomicBool::new(false);

/// Minimum interval between `teraslab::security` warnings about
/// unauthenticated inter-node frames *from the same peer*. The warning is
/// per-peer rate-limited rather than per-frame: in trusted-overlay mode
/// (no `cluster_secret`) EVERY inter-node frame — every replica batch
/// (op 240), migration frame, SWIM/topology frame — is unauthenticated, so
/// a per-frame `warn!` produces millions of identical lines that drown all
/// real diagnostic signal. The per-frame *rate* is still observable via the
/// `replica_unauthenticated_accept_total` counter; the log only needs to
/// surface WHICH peers are sending unsigned frames, periodically.
const UNAUTH_WARN_PER_PEER_INTERVAL: Duration = Duration::from_secs(300);

/// Last time an unauthenticated-accept warning was emitted for each peer IP,
/// keyed by source IP (not `SocketAddr`: source ports churn every
/// reconnect, and the meaningful identity is the source node). Bounded by
/// the cluster's node count.
static UNAUTH_WARN_LAST_BY_PEER: Mutex<Option<HashMap<std::net::IpAddr, Instant>>> =
    Mutex::new(None);

/// Returns `true` if a warning should be emitted now for `peer` (first time
/// seen, or the per-peer interval has elapsed), updating the bookkeeping.
fn should_warn_unauthenticated(peer: Option<std::net::IpAddr>) -> bool {
    let Some(ip) = peer else {
        // Unknown peer address — fall back to the legacy one-shot so we
        // still surface the condition exactly once without spamming.
        return !UNAUTHENTICATED_INTER_NODE_WARNED.swap(true, Ordering::AcqRel);
    };
    let now = Instant::now();
    let mut guard = UNAUTH_WARN_LAST_BY_PEER.lock();
    let map = guard.get_or_insert_with(HashMap::new);
    match map.get(&ip) {
        Some(last) if now.duration_since(*last) < UNAUTH_WARN_PER_PEER_INTERVAL => false,
        _ => {
            map.insert(ip, now);
            true
        }
    }
}

const READ_BUF_RETAINED_SIZE: usize = 256 * 1024;

/// Bytes peeked off the wire AFTER the 4-byte length prefix but BEFORE
/// committing to a buffered vs. streaming read of the body: 8-byte
/// `request_id` + 2-byte `op_code`. The peeked head is used to compute
/// `is_inter_node_op` so that signed inter-node frames can route into
/// `verify_signed_body_streaming` (slow-loris-resistant) instead of
/// materialising the entire body in `read_buf` before HMAC verify.
const HEAD_PEEK_LEN: usize = 10;
const CONNECTION_READ_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECTION_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// L-01: whole-frame assembly deadline, measured from the moment the
/// 4-byte length prefix has been read off the wire. Shared with the
/// replication receiver (follow-up E-1) — see
/// [`crate::protocol::deadline`] for the rationale and the
/// [`DeadlineReader`] adapter that enforces it.
use crate::protocol::deadline::{DeadlineReader, FRAME_ASSEMBLY_TIMEOUT};

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
            // P2.2: per-request size exceeds the entire aggregate cap — a
            // single frame can never be admitted. Counted as a rejection.
            Self::record_rejection();
            return None;
        }

        let mut observed = self.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = observed.checked_add(bytes) else {
                // Arithmetic overflow on the cumulative byte count. Tens
                // of millions of in-flight frames before this can happen;
                // still classify as a rejection rather than silently
                // dropping the signal.
                Self::record_rejection();
                return None;
            };
            if next > self.limit {
                // The aggregate cap would be exceeded; reject + record.
                Self::record_rejection();
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

    /// P2.2: bump `ThreadMetrics::inflight_bytes_rejected_total` on the
    /// rejection path. Uses the same `DISPATCH_METRICS` handle every
    /// other dispatch counter site uses (see `dispatch_metrics_handle`),
    /// so the counter ticks at the same per-thread cost (~1 ns
    /// `fetch_add`) as the existing operation counters.
    #[inline]
    fn record_rejection() {
        if let Some(m) = crate::server::dispatch::dispatch_metrics_handle() {
            m.inflight_bytes_rejected_total.inc();
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
/// (aborted) when the connection closes, when their cumulative byte cap is
/// exceeded, or when they go idle past [`Self::stream_idle_timeout`] (the
/// idle-stream reaper — see [`Self::reap_idle_streams`]).
pub(crate) struct ConnectionState {
    pub(crate) streams: HashMap<[u8; 32], ActiveStream>,
    pub(crate) max_stream_total_bytes: u64,
    /// H-1/LM-1: cap on the number of concurrent in-progress streams. A new
    /// stream open past this count is rejected with `ERR_RATE_LIMITED`
    /// *before* a file descriptor / tmp file is allocated. `0` disables the
    /// cap. See [`ServerConfig::max_active_streams_per_connection`].
    pub(crate) max_active_streams: usize,
    /// H-2: idle timeout after which a stream that has received no further
    /// chunk is reaped (its fd, tmp file, hasher, and map entry freed),
    /// independently of connection close. `None` disables the reaper. See
    /// [`ServerConfig::stream_idle_timeout_secs`].
    pub(crate) stream_idle_timeout: Option<Duration>,
}

/// An in-progress streaming blob upload for a single txid.
pub(crate) struct ActiveStream {
    pub(crate) writer: Box<dyn BlobStreamWriter>,
    pub(crate) bytes_received: u64,
    /// H-2: wall-clock instant of the most recent chunk for this stream,
    /// refreshed on every accepted `OP_STREAM_CHUNK`. The idle reaper aborts
    /// streams whose `last_activity` is older than
    /// [`ConnectionState::stream_idle_timeout`].
    pub(crate) last_activity: Instant,
}

impl ConnectionState {
    pub(crate) fn new() -> Self {
        Self {
            streams: HashMap::new(),
            max_stream_total_bytes: ServerConfig::DEFAULT_MAX_STREAM_TOTAL_BYTES,
            max_active_streams: ServerConfig::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION,
            stream_idle_timeout: Some(Duration::from_secs(
                ServerConfig::DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
            )),
        }
    }

    pub(crate) fn with_max_stream_total_bytes(mut self, max_stream_total_bytes: u64) -> Self {
        self.max_stream_total_bytes = max_stream_total_bytes;
        self
    }

    /// H-1/LM-1: override the concurrent-stream cap. `0` disables it.
    pub(crate) fn with_max_active_streams(mut self, max_active_streams: usize) -> Self {
        self.max_active_streams = max_active_streams;
        self
    }

    /// H-2: override the idle-stream timeout. `None` disables the reaper.
    pub(crate) fn with_stream_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.stream_idle_timeout = timeout;
        self
    }

    /// H-1/LM-1: whether a *new* stream may be opened right now.
    ///
    /// Returns `true` when the concurrent-stream cap is disabled (`0`) or the
    /// current open-stream count is strictly below the cap. Callers must
    /// check this on the vacant-entry path of `OP_STREAM_CHUNK` *before*
    /// calling `begin_stream`, so a rejected open never allocates a file
    /// descriptor or tmp file.
    pub(crate) fn can_open_new_stream(&self) -> bool {
        self.max_active_streams == 0 || self.streams.len() < self.max_active_streams
    }

    /// H-2: reap in-progress streams idle longer than
    /// [`Self::stream_idle_timeout`], measured against `now`.
    ///
    /// Each reaped stream's writer is `abort`ed (removing its tmp file) and
    /// its map entry dropped. Returns the number of streams reaped. A no-op
    /// when the timeout is `None` (reaper disabled) or no stream is stale.
    ///
    /// `now` is passed in (rather than read internally) so the reaper is
    /// deterministically testable without sleeping; the connection loop
    /// passes `Instant::now()`.
    pub(crate) fn reap_idle_streams(&mut self, now: Instant) -> usize {
        let Some(timeout) = self.stream_idle_timeout else {
            return 0;
        };
        // Collect stale keys first to avoid mutating the map while iterating.
        let stale: Vec<[u8; 32]> = self
            .streams
            .iter()
            .filter(|(_, s)| now.duration_since(s.last_activity) >= timeout)
            .map(|(txid, _)| *txid)
            .collect();
        for txid in &stale {
            if let Some(stream) = self.streams.remove(txid) {
                let _ = stream.writer.abort();
            }
        }
        stale.len()
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

/// Per-source-IP connection counter shared with the accept loop.
///
/// Maps the peer's [`std::net::IpAddr`] (NOT `SocketAddr` — the source port
/// changes on every connection, the IP does not) to the number of
/// currently-active connections from that IP. Used to enforce
/// [`ServerConfig::max_connections_per_ip`] before a per-connection thread
/// is spawned.
pub(crate) type PerIpCounter = Arc<Mutex<HashMap<std::net::IpAddr, usize>>>;

/// RAII guard that decrements the per-IP connection counter exactly once
/// when the connection thread exits.
///
/// The accept loop increments the counter when a connection is admitted
/// and constructs this guard, which it moves into the spawned
/// per-connection thread. Whether the thread exits normally, returns
/// `Err`, or panics, the guard's `Drop` impl removes the connection from
/// the per-IP tally — preventing the count from leaking over the
/// lifetime of the process.
pub(crate) struct PerIpGuard {
    counter: PerIpCounter,
    ip: std::net::IpAddr,
}

impl Drop for PerIpGuard {
    fn drop(&mut self) {
        let mut map = self.counter.lock();
        if let Some(count) = map.get_mut(&self.ip) {
            *count = count.saturating_sub(1);
            // GC empty entries so a never-returning attacker can't grow
            // the map without bound.
            if *count == 0 {
                map.remove(&self.ip);
            }
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
    /// Per-source-IP connection counter for DoS-resistance against a
    /// single hostile peer pinning all `max_connections` slots with
    /// slow-loris reads. See [`PerIpCounter`].
    connections_per_ip: PerIpCounter,
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
            connections_per_ip: Arc::new(Mutex::new(HashMap::new())),
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

    /// E-2: verify the `cluster_secret` the server signs with (from
    /// [`ServerConfig`]) agrees with the one the attached cluster coordinator
    /// uses for inter-node HMAC.
    ///
    /// These are two independently-populated copies. If they diverge — easy to
    /// do in programmatic construction by setting only one — the server signs
    /// responses with one secret while topology / replication proposals expect
    /// the other, so every HMAC verification fails forever with no surfaced
    /// error (a silent cluster-formation hang). Failing closed here turns that
    /// silent hang into a loud typed error at startup. Returns the stringified
    /// [`ConfigError::ClusterSecretMismatch`] when the secrets disagree and
    /// clustering is active.
    fn validate_cluster_secret_agreement(&self) -> Result<(), String> {
        let server_secret = self.config.cluster_secret.as_ref().map(|s| s.as_bytes());
        let cluster_secret = self.cluster.as_ref().and_then(|c| c.cluster_secret());
        let multi_node = self.config.is_clustered() || self.config.replication_factor > 1;
        crate::config::check_cluster_secret_agreement(
            server_secret,
            cluster_secret,
            self.cluster.is_some(),
            multi_node,
        )
        .map_err(|e| e.to_string())
    }

    /// Start listening for client connections. Blocks until shutdown.
    pub fn run(&self) -> Result<(), String> {
        // E-2: fail closed before binding if the server's cluster_secret and
        // the attached coordinator's secret disagree — otherwise inter-node
        // HMAC silently fails forever and cluster formation hangs.
        self.validate_cluster_secret_agreement()?;

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
            .register(&mut mio_listener, LISTENER_TOKEN, mio::Interest::READABLE)
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
                        // Disable Nagle. Without this, the server's small
                        // response frames (e.g. ReplicaAck::Ok at 9 bytes)
                        // are held in the kernel TCP send buffer waiting
                        // for more data or a peer ACK — interacting with
                        // delayed-ACK on the master side to add 40 ms-3 s
                        // of latency per RPC. The master side already sets
                        // TCP_NODELAY on TcpReplicaTransport (see
                        // src/replication/tcp_transport.rs); without the
                        // server-side mirror, every OP_REPLICA_BATCH ACK
                        // sat in Nagle's buffer long enough that the
                        // master's 3 s recv_ack timeout fired before the
                        // response arrived. Per-RPC latency drops from
                        // seconds back to single-digit milliseconds with
                        // this on.
                        let _ = stream.set_nodelay(true);

                        // Per-source-IP cap (DoS hardening). Enforced
                        // BEFORE the global cap, BEFORE any frame
                        // parsing, and BEFORE any bytes are written to
                        // the socket. A single hostile peer that drains
                        // its quota gets a silent close — no per-reject
                        // frame is sent because writing one would let
                        // the attacker measure the cap and slow-loris
                        // around it.
                        //
                        // `max_connections_per_ip == 0` disables the
                        // cap (operators behind a single egress NAT may
                        // legitimately need this).
                        let per_ip_guard = if self.config.max_connections_per_ip > 0 {
                            let peer_ip = addr.ip();
                            let mut map = self.connections_per_ip.lock();
                            let count = map.entry(peer_ip).or_insert(0);
                            if *count >= self.config.max_connections_per_ip {
                                let observed = *count;
                                drop(map);
                                tracing::info!(
                                    peer_addr = %addr,
                                    peer_ip = %peer_ip,
                                    count = observed,
                                    limit = self.config.max_connections_per_ip,
                                    "rejecting connection: per-IP cap reached",
                                );
                                drop(stream);
                                continue;
                            }
                            *count += 1;
                            drop(map);
                            Some(PerIpGuard {
                                counter: self.connections_per_ip.clone(),
                                ip: peer_ip,
                            })
                        } else {
                            None
                        };

                        let active = self.active_connections.load(Ordering::Relaxed);
                        if active >= self.config.max_connections {
                            tracing::warn!(peer_addr = %addr, active, "rejecting connection: max connections reached");
                            let _ = stream.set_write_timeout(Some(CONNECTION_WRITE_TIMEOUT));
                            let response = ResponseFrame {
                                request_id: 0,
                                status: STATUS_ERROR,
                                payload: encode_error_payload(
                                    ERR_RATE_LIMITED,
                                    "max connections reached",
                                ),
                            };
                            let _ = stream.write_all(&response.encode());
                            drop(stream);
                            // `per_ip_guard` drops here, releasing the
                            // slot we reserved a moment ago — the
                            // global-cap reject must not leak per-IP
                            // count.
                            drop(per_ip_guard);
                            continue;
                        }

                        self.active_connections.fetch_add(1, Ordering::Relaxed);

                        let engine = self.engine.clone();
                        let shutdown = self.shutdown.clone();
                        let active_conns = self.active_connections.clone();
                        let max_batch = self.config.max_batch_size;
                        let max_stream_total_bytes = self.config.max_stream_total_bytes;
                        let max_active_streams = self.config.max_active_streams_per_connection;
                        let stream_idle_timeout_secs = self.config.stream_idle_timeout_secs;
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

                        // Move the per-IP guard into the spawned
                        // thread so its `Drop` runs exactly once when
                        // the connection thread exits (normal return,
                        // error, or panic). This is the RAII half of
                        // the per-IP accounting — the increment
                        // happened in the accept loop above; the
                        // decrement happens here.
                        let per_ip_guard_moved = per_ip_guard;
                        std::thread::spawn(move || {
                            let _per_ip_guard = per_ip_guard_moved;
                            if let Err(e) = handle_connection_inner(
                                stream,
                                &engine,
                                &shutdown,
                                ConnectionOptions {
                                    max_batch_size: max_batch,
                                    max_stream_total_bytes,
                                    max_active_streams,
                                    stream_idle_timeout_secs,
                                    cluster: cluster.as_deref(),
                                    redo_log: redo_log.as_deref(),
                                    blob_store: blob_store.as_deref(),
                                    inflight_request_bytes,
                                    cluster_secret,
                                    strict_auth,
                                    read_timeout: CONNECTION_READ_TIMEOUT,
                                    frame_deadline: FRAME_ASSEMBLY_TIMEOUT,
                                    write_timeout: CONNECTION_WRITE_TIMEOUT,
                                },
                            ) {
                                tracing::warn!(peer_addr = %addr, err = %e, "connection error");
                            }
                            active_conns.fetch_sub(1, Ordering::Relaxed);
                            // `_per_ip_guard` drops here.
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
    /// H-1/LM-1: per-connection concurrent-stream cap (`0` disables). See
    /// [`ServerConfig::max_active_streams_per_connection`].
    max_active_streams: usize,
    /// H-2: per-stream idle timeout in seconds (`0` disables the reaper). See
    /// [`ServerConfig::stream_idle_timeout_secs`].
    stream_idle_timeout_secs: u64,
    cluster: Option<&'a RunningCluster>,
    redo_log: Option<&'a Mutex<RedoLog>>,
    blob_store: Option<&'a dyn BlobStore>,
    inflight_request_bytes: Arc<InflightBytesLimiter>,
    cluster_secret: Option<Arc<Vec<u8>>>,
    /// F-G5-001 (CRITICAL): when `true`, an inter-node opcode that arrives
    /// without a `cluster_secret` configured is rejected with
    /// `ERR_CLUSTER_AUTH_FAILED`. When `false` (the default trusted-overlay
    /// behaviour), the frame is accepted unauthenticated — a per-peer
    /// rate-limited warning is emitted (see `should_warn_unauthenticated`)
    /// and the `replica_unauthenticated_accept_total` counter ticks.
    ///
    /// Orchestrator-wired: G10 owns `ServerConfig::strict_auth` and the
    /// CLI `--strict-auth` flag; this field is the local read-site.
    strict_auth: bool,
    read_timeout: Duration,
    /// L-01: whole-frame assembly deadline (see [`FRAME_ASSEMBLY_TIMEOUT`]).
    /// Injectable per-connection so tests can exercise the deadline
    /// without 60-second sleeps; production wiring always passes the
    /// constant.
    frame_deadline: Duration,
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

    // C-6 / F-G5-011 (P3.4): the per-connection read buffer is a
    // `BytesMut` rather than a `Vec<u8>` so that each completed frame
    // can be split off and frozen into a zero-copy `Bytes` slice. The
    // resulting `Bytes` is passed to `RequestFrame::decode_bytes`, which
    // produces a payload `Bytes` that shares the underlying allocation
    // — no `to_vec()` copy on the request hot path.
    let mut read_buf = BytesMut::with_capacity(READ_BUF_RETAINED_SIZE);
    read_buf.resize(READ_BUF_RETAINED_SIZE, 0);
    // H-2: `0` disables the idle reaper; any positive value installs a
    // per-stream idle deadline enforced on each request (see the
    // `reap_idle_streams` call in the loop below).
    let stream_idle_timeout = if opts.stream_idle_timeout_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(opts.stream_idle_timeout_secs))
    };
    let mut conn_state = ConnectionState::new()
        .with_max_stream_total_bytes(opts.max_stream_total_bytes)
        .with_max_active_streams(opts.max_active_streams)
        .with_stream_idle_timeout(stream_idle_timeout);

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
                        ERR_RATE_LIMITED,
                        "aggregate in-flight request memory limit exceeded",
                    ),
                };
                let _ = stream.write_all(&resp.encode());
                return Err(format!(
                    "aggregate in-flight request memory limit exceeded: requested {frame_len} bytes"
                ));
            }
        };
        // Peek the request_id (8 bytes) and op_code (2 bytes) directly off
        // the wire WITHOUT first buffering the entire frame body. This is
        // the slow-loris fix (F-G5-016 / re-review P2): for inter-node
        // signed frames we MUST be able to start streaming the body
        // through `verify_frame_streaming_*` instead of materialising
        // `frame_len` bytes in the connection buffer before HMAC verify.
        // Without it, the per-IP connection cap (`max_connections_per_ip`
        // = 64 by default) lets a malicious peer keep 64 × peak-frame
        // bytes pinned per IP just by sending wrong-tag garbage.
        // L-01: the rest of the frame (head peek + body) must be fully
        // assembled within `opts.frame_deadline` of the length prefix
        // arriving. The per-syscall `read_timeout` alone cannot enforce
        // this — it resets on every successful read, so a slow-drip
        // client (one byte every ~29 s) would otherwise pin this thread,
        // its inflight permit, and a connection slot indefinitely. All
        // post-prefix reads below go through `deadline_stream`.
        let mut deadline_stream = DeadlineReader::new(
            &stream,
            Instant::now() + opts.frame_deadline,
            opts.read_timeout,
        );
        let mut head_buf = [0u8; HEAD_PEEK_LEN];
        let head_to_read = HEAD_PEEK_LEN.min(frame_len);
        deadline_stream
            .read_exact(&mut head_buf[..head_to_read])
            .map_err(|e| format!("read frame head: {e}"))?;

        let request_id = if head_to_read >= 8 {
            u64::from_le_bytes(head_buf[..8].try_into().unwrap_or([0; 8]))
        } else {
            0
        };
        let peeked_op = if head_to_read >= 10 {
            Some(u16::from_le_bytes(
                head_buf[8..10].try_into().unwrap_or([0; 2]),
            ))
        } else {
            None
        };
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
            // P2.1 (F-G7-001): bump the receiver-side counter every time
            // we accept an inter-node opcode without an HMAC layer. Unlike
            // the one-shot `warn!` below, the counter must tick on every
            // such frame so dashboards can compute a *rate* — a slow drip
            // of unauthenticated frames is the signal pattern this metric
            // is designed to expose. The counter field is owned by G7's
            // `ReplicationMetrics` schema (see `metrics.rs`); the bump
            // site lives here in the G5 auth gate per the cross-cutting
            // ownership note attached to the field.
            if let Some(repl) = crate::metrics::replication_metrics() {
                repl.replica_unauthenticated_accept_total.inc();
            }
            let op_code = peeked_op.unwrap_or(0);
            // Per-PEER rate-limited `warn` so operators see which peers send
            // unsigned frames, without the per-frame flood (every inter-node
            // frame is unauthenticated in trusted-overlay mode). The counter
            // above already exposes the per-frame rate for dashboards; the
            // log only needs the distinct offenders, re-surfaced every
            // `UNAUTH_WARN_PER_PEER_INTERVAL`. The legacy one-shot flag is
            // still flipped so first-occurrence log scrapers keep working.
            let peer_ip = stream.peer_addr().ok().map(|a| a.ip());
            if should_warn_unauthenticated(peer_ip) {
                tracing::warn!(
                    target: "teraslab::security",
                    op_code,
                    peer = ?peer_ip,
                    "unauthenticated replica accept: inter-node opcode \
                     received without cluster_secret configured — \
                     accepting frame (trusted-overlay default). Configure \
                     `cluster_secret` or pass `--strict-auth` to enforce. \
                     (further frames from this peer suppressed for 5m)",
                );
            }
            let _ = UNAUTHENTICATED_INTER_NODE_WARNED.swap(true, Ordering::AcqRel);
        }
        // Two body-read paths now diverge based on whether the frame
        // must be HMAC-verified:
        //
        // - `auth_required` → streaming verify. The remainder of the
        //   body is read by `verify_signed_body_streaming` in 8 KiB
        //   chunks, never materialising the full `frame_len` bytes.
        //   The verified payload is written into a fresh, disposable
        //   `Vec<u8>` sink which is `drop()`ped on
        //   `Err(PermissionDenied)` — unauthenticated partial-write
        //   bytes NEVER leak into the persistent `read_buf` or to
        //   dispatch. This is the slow-loris fix (F-G5-016): a 16 MiB
        //   wrong-tag frame now rejects with ~48 KiB of total
        //   verifier-side allocation (8 KiB chunk + 40 B tail + sink
        //   that never exceeds ~32 KiB before HMAC reject) instead of
        //   the previous 16 MiB connection-buffer materialisation.
        //
        // - non-auth (the common client-traffic case) → assemble the
        //   full frame in the persistent `read_buf` and freeze a zero-
        //   copy `Bytes`. The 4-byte length-prefix + 10-byte head we
        //   already peeked off the wire are spliced back into the
        //   buffer before reading the remainder.
        let request_frame_bytes: Bytes = if auth_required {
            let key = opts.cluster_secret.as_ref().expect("checked above");
            let head_slice = &head_buf[..head_to_read];
            // L-01: chunked verify reads also run through the deadline
            // reader so a drip-fed signed body cannot outlive the
            // frame-assembly deadline.
            let mut chained = std::io::Cursor::new(head_slice).chain(&mut deadline_stream);
            // Disposable sink: pre-seed a 4-byte length-prefix slot
            // (overwritten with `payload_len` on success) so the
            // returned `Bytes` matches the `[length:4][payload]` shape
            // that `RequestFrame::decode_bytes` expects.
            let mut sink: Vec<u8> = Vec::with_capacity(4 + frame_len);
            sink.extend_from_slice(&[0u8; 4]);
            let payload_len = match crate::cluster::auth::verify_signed_body_streaming(
                key.as_slice(),
                frame_len,
                &mut chained,
                &mut sink,
            ) {
                Ok(n) => n,
                Err(e) => {
                    // SECURITY: drop the sink before responding so
                    // the partially-written unauthenticated bytes
                    // never escape this scope.
                    drop(sink);
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
            };
            sink[0..4].copy_from_slice(&(payload_len as u32).to_le_bytes());
            sink.truncate(4 + payload_len);
            Bytes::from(sink)
        } else {
            // Assemble the full frame (length prefix + body) into the
            // persistent `read_buf`. The 4-byte length prefix and the
            // `head_to_read` peeked bytes are spliced in first; the
            // remainder is read from the stream.
            if read_buf.len() < 4 + frame_len {
                read_buf.resize(4 + frame_len, 0);
            }
            read_buf[..4].copy_from_slice(&len_buf);
            read_buf[4..4 + head_to_read].copy_from_slice(&head_buf[..head_to_read]);
            if frame_len > head_to_read {
                deadline_stream
                    .read_exact(&mut read_buf[4 + head_to_read..4 + frame_len])
                    .map_err(|e| format!("read frame body: {e}"))?;
            }
            let frame_bytes_mut = read_buf.split_to(4 + frame_len);
            // Shrink read_buf IMMEDIATELY after split_to so a giant
            // frame does not pin peak-frame capacity on the connection
            // during dispatch + response write. Under the per-IP
            // connection cap (`max_connections_per_ip = 64` by default)
            // a 16 MiB peak frame would otherwise hold 64 × 16 MiB
            // = 1 GiB pinned across concurrent slow-loris-ish clients
            // until each connection's iteration completed.
            reset_read_buf_if_oversized(&mut read_buf);
            if read_buf.capacity() < READ_BUF_RETAINED_SIZE {
                read_buf.reserve(READ_BUF_RETAINED_SIZE - read_buf.capacity());
            }
            if read_buf.len() < READ_BUF_RETAINED_SIZE {
                read_buf.resize(READ_BUF_RETAINED_SIZE, 0);
            }
            frame_bytes_mut.freeze()
        };

        // L-01: a deadline-capped read may have shrunk the socket read
        // timeout below the base value. Restore it so the next
        // iteration's length-prefix read keeps the original idle-client
        // drop semantics (`read_timeout`, treated as a clean close).
        if deadline_stream.timeout_shrunk {
            stream
                .set_read_timeout(Some(opts.read_timeout))
                .map_err(|e| format!("restore read_timeout: {e}"))?;
        }

        let (request, _) = RequestFrame::decode_bytes(request_frame_bytes)
            .map_err(|e| format!("decode frame: {e}"))?;

        // H-2: reap idle streams on every request before dispatch. The
        // server is thread-per-connection and synchronous, so this is the
        // natural tick — a client that keeps the connection cheaply alive
        // with periodic pings (or any other op) drives a sweep here, freeing
        // the fd / tmp file / hasher of any stream that has received no chunk
        // within `stream_idle_timeout`. No background thread is required and
        // the map is per-connection, so this holds no shared lock.
        let reaped = conn_state.reap_idle_streams(Instant::now());
        if reaped > 0 {
            tracing::debug!(
                reaped,
                remaining = conn_state.streams.len(),
                "reaped idle blob-stream sessions",
            );
        }

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
        // (The per-iteration read_buf reset has moved up to right
        // after `split_to` so a giant frame does not pin per-IP
        // capacity through the dispatch + response window. Reset
        // again here is redundant.)
    }
}

fn reset_read_buf_if_oversized(read_buf: &mut BytesMut) {
    if read_buf.capacity() > READ_BUF_RETAINED_SIZE {
        *read_buf = BytesMut::with_capacity(READ_BUF_RETAINED_SIZE);
        read_buf.resize(READ_BUF_RETAINED_SIZE, 0);
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

    #[test]
    fn unauthenticated_warn_is_rate_limited_per_peer() {
        use std::net::{IpAddr, Ipv4Addr};
        // Unique IPs so this test is independent of any other test that
        // might touch the shared rate-limit map.
        let a = Some(IpAddr::V4(Ipv4Addr::new(10, 99, 0, 1)));
        let b = Some(IpAddr::V4(Ipv4Addr::new(10, 99, 0, 2)));

        // First sighting of each peer warns; immediate repeats are
        // suppressed (the per-peer interval has not elapsed).
        assert!(should_warn_unauthenticated(a), "first frame from A must warn");
        assert!(
            !should_warn_unauthenticated(a),
            "second frame from A within the interval must be suppressed"
        );
        assert!(
            !should_warn_unauthenticated(a),
            "third frame from A within the interval must be suppressed"
        );
        // A distinct peer warns independently — one stuck peer does not mask
        // a different offender.
        assert!(should_warn_unauthenticated(b), "first frame from B must warn");
        assert!(!should_warn_unauthenticated(b), "B repeat suppressed");

        // A peer whose last-warn instant is older than the interval warns
        // again (forces re-surfacing of a persistent offender).
        let c = IpAddr::V4(Ipv4Addr::new(10, 99, 0, 3));
        {
            let mut guard = UNAUTH_WARN_LAST_BY_PEER.lock();
            let map = guard.get_or_insert_with(HashMap::new);
            map.insert(c, Instant::now() - UNAUTH_WARN_PER_PEER_INTERVAL - Duration::from_secs(1));
        }
        assert!(
            should_warn_unauthenticated(Some(c)),
            "peer past the interval must warn again"
        );
        assert!(
            !should_warn_unauthenticated(Some(c)),
            "and then be suppressed again"
        );
    }

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
        let mut read_buf = BytesMut::with_capacity(READ_BUF_RETAINED_SIZE * 4);
        read_buf.resize(READ_BUF_RETAINED_SIZE * 4, 0);
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
                    max_active_streams: ServerConfig::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION,
                    stream_idle_timeout_secs: ServerConfig::DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: None,
                    strict_auth: false,
                    read_timeout: Duration::from_millis(50),
                    frame_deadline: FRAME_ASSEMBLY_TIMEOUT,
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

    /// L-01: `CONNECTION_READ_TIMEOUT` is per-syscall — it resets on
    /// every successful read, so a client dripping one byte per interval
    /// keeps each individual read "succeeding" forever. The whole-frame
    /// assembly deadline must abort the connection once it expires,
    /// independent of per-read progress.
    #[test]
    fn dripping_client_disconnected_at_frame_assembly_deadline() {
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
                    max_active_streams: ServerConfig::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION,
                    stream_idle_timeout_secs: ServerConfig::DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: None,
                    strict_auth: false,
                    // Per-read timeout deliberately much longer than the
                    // drip interval below: every individual read makes
                    // "progress", so only the frame-assembly deadline can
                    // end this connection.
                    read_timeout: Duration::from_secs(5),
                    frame_deadline: Duration::from_millis(400),
                    write_timeout: Duration::from_secs(1),
                },
            );
            tx.send(result).unwrap();
        });

        let mut client = TcpStream::connect(addr).unwrap();
        // Declare a 16-byte frame body, then drip it one byte per 100 ms
        // — far slower than the 400 ms assembly deadline allows, yet each
        // drip lands well inside the 5 s per-read timeout.
        client.write_all(&16u32.to_le_bytes()).unwrap();
        let drip = std::thread::spawn(move || {
            for _ in 0..16 {
                if client.write_all(&[0u8]).is_err() {
                    return; // server closed the connection — expected
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        });

        let result = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("dripping client should be aborted at the frame-assembly deadline");
        let err = result.expect_err("frame-assembly deadline should abort the connection");
        assert!(
            err.contains("read frame"),
            "deadline abort should surface as a frame-read error, got: {err}"
        );
        drip.join().unwrap();
    }

    /// L-01 counterpart: a client that sends a frame in several chunks
    /// well within the deadline must still be served. The second request
    /// below arrives after an idle gap longer than the frame deadline —
    /// it verifies the handler restores the base per-read timeout after
    /// a deadline-capped frame read (otherwise the next length-prefix
    /// read would inherit the shrunken timeout and drop the connection).
    #[test]
    fn chunked_frame_within_deadline_succeeds() {
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
                    max_active_streams: ServerConfig::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION,
                    stream_idle_timeout_secs: ServerConfig::DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: None,
                    strict_auth: false,
                    read_timeout: Duration::from_secs(5),
                    frame_deadline: Duration::from_secs(1),
                    write_timeout: Duration::from_secs(1),
                },
            );
            tx.send(result).unwrap();
        });

        let mut client = TcpStream::connect(addr).unwrap();
        let request = RequestFrame {
            request_id: 21,
            op_code: OP_PING,
            flags: 0,
            payload: Bytes::new(),
        };
        // Send the 16-byte wire frame in three chunks 100 ms apart
        // (~300 ms total, well inside the 1 s deadline). The chunk
        // boundaries straddle the prefix, head-peek, and body reads.
        for chunk in request.encode().chunks(6) {
            client.write_all(chunk).unwrap();
            std::thread::sleep(Duration::from_millis(100));
        }
        let response = read_response_frame_for_test(&mut client);
        assert_eq!(response.request_id, 21);
        assert_eq!(response.status, STATUS_OK);

        // Idle for longer than the frame deadline, then send a second
        // request in one piece. A handler that failed to restore the
        // base read timeout would time out this length-prefix read and
        // close the connection before responding.
        std::thread::sleep(Duration::from_millis(1200));
        let request2 = RequestFrame {
            request_id: 22,
            op_code: OP_PING,
            flags: 0,
            payload: Bytes::new(),
        };
        client.write_all(&request2.encode()).unwrap();
        let response2 = read_response_frame_for_test(&mut client);
        assert_eq!(response2.request_id, 22);
        assert_eq!(response2.status, STATUS_OK);

        drop(client);
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("server should exit after client disconnect");
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
                    max_active_streams: ServerConfig::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION,
                    stream_idle_timeout_secs: ServerConfig::DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: Some(Arc::new(b"cluster-secret".to_vec())),
                    strict_auth: false,
                    read_timeout: Duration::from_secs(1),
                    frame_deadline: FRAME_ASSEMBLY_TIMEOUT,
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
            payload: Vec::new().into(),
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
                    max_active_streams: ServerConfig::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION,
                    stream_idle_timeout_secs: ServerConfig::DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: Some(Arc::new(b"cluster-secret".to_vec())),
                    strict_auth: false,
                    read_timeout: Duration::from_secs(1),
                    frame_deadline: FRAME_ASSEMBLY_TIMEOUT,
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
            payload: Vec::new().into(),
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
                    max_active_streams: ServerConfig::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION,
                    stream_idle_timeout_secs: ServerConfig::DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: None,
                    strict_auth: true,
                    read_timeout: Duration::from_secs(1),
                    frame_deadline: FRAME_ASSEMBLY_TIMEOUT,
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
            payload: Vec::new().into(),
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
                    max_active_streams: ServerConfig::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION,
                    stream_idle_timeout_secs: ServerConfig::DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: None,
                    strict_auth: false,
                    read_timeout: Duration::from_secs(1),
                    frame_deadline: FRAME_ASSEMBLY_TIMEOUT,
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
            payload: Vec::new().into(),
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

    /// P2.1 (F-G7-001): when the auth gate accepts an inter-node frame
    /// without a configured `cluster_secret` (the trusted-overlay
    /// fail-open default), it must bump
    /// `ReplicationMetrics::replica_unauthenticated_accept_total` so
    /// dashboards can alert on any non-zero rate.
    ///
    /// The metric is registered through a process-wide `OnceLock`. We
    /// install a `&'static` instance here (idempotent for parallel
    /// tests; the leak is `'static`-scoped and bounded to the process).
    /// Reading the counter both before and after the connection isolates
    /// this test from any other test that may have already bumped it.
    #[test]
    fn unauthenticated_inter_node_accept_increments_metric() {
        use crate::metrics::{ReplicationMetrics, init_replication_metrics, replication_metrics};

        // Install a process-wide ReplicationMetrics. `OnceLock` semantics
        // mean later test threads racing with us share the same handle —
        // we still observe the delta via before/after snapshots below.
        static METRICS_CELL: std::sync::OnceLock<ReplicationMetrics> = std::sync::OnceLock::new();
        let leaked: &'static ReplicationMetrics = METRICS_CELL.get_or_init(ReplicationMetrics::new);
        init_replication_metrics(leaked);
        let metrics = replication_metrics().expect("replication metrics installed by init_ above");
        let before = metrics.replica_unauthenticated_accept_total.get();

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
                    max_active_streams: ServerConfig::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION,
                    stream_idle_timeout_secs: ServerConfig::DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
                    cluster: None,
                    redo_log: None,
                    blob_store: None,
                    inflight_request_bytes: Arc::new(InflightBytesLimiter::new(0)),
                    cluster_secret: None,
                    strict_auth: false,
                    read_timeout: Duration::from_secs(1),
                    frame_deadline: FRAME_ASSEMBLY_TIMEOUT,
                    write_timeout: Duration::from_secs(1),
                },
            );
            tx.send(result).unwrap();
        });

        let mut client = TcpStream::connect(addr).unwrap();
        let request = RequestFrame {
            request_id: 11,
            // Any inter-node opcode reaches the auth gate; OP_REPLICA_BATCH
            // is the canonical example.
            op_code: OP_REPLICA_BATCH,
            flags: 0,
            payload: bytes::Bytes::new(),
        };
        client.write_all(&request.encode()).unwrap();

        // Read the response to ensure the gate has actually executed
        // before we sample the counter.
        let response = read_response_frame_for_test(&mut client);
        assert_eq!(response.request_id, 11);
        drop(client);
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("server should return after client disconnect");
        assert!(result.is_ok(), "fail-open accepted result was {result:?}");

        let after = metrics.replica_unauthenticated_accept_total.get();
        assert!(
            after > before,
            "expected replica_unauthenticated_accept_total to advance by \
             at least 1 (before={before}, after={after})",
        );
    }

    /// P2.2: every `InflightBytesLimiter::try_acquire` rejection — whether
    /// from the single-frame oversize guard, the per-thread arithmetic
    /// overflow guard, or the aggregate-cap guard — must bump
    /// `ThreadMetrics::inflight_bytes_rejected_total`. Pre-fix all three
    /// paths returned `None` silently; operators could not alert on
    /// backpressure-induced frame rejections.
    #[test]
    fn inflight_bytes_rejected_metric_increments_on_overflow() {
        use crate::metrics::ThreadMetrics;
        use crate::server::dispatch::init_dispatch_metrics;

        // Install a process-wide ThreadMetrics handle. `OnceLock`
        // semantics: parallel tests share the same handle — we capture
        // the before/after delta so concurrent bumps don't false-fail.
        static METRICS_CELL: std::sync::OnceLock<ThreadMetrics> = std::sync::OnceLock::new();
        let leaked: &'static ThreadMetrics = METRICS_CELL.get_or_init(ThreadMetrics::new);
        init_dispatch_metrics(leaked);
        let metrics = crate::server::dispatch::dispatch_metrics_handle()
            .expect("dispatch metrics installed above");

        let limiter = Arc::new(InflightBytesLimiter::new(16));

        // Single-frame oversize rejection: 17 > limit 16.
        let before_oversize = metrics.inflight_bytes_rejected_total.get();
        assert!(limiter.try_acquire(17).is_none());
        let after_oversize = metrics.inflight_bytes_rejected_total.get();
        assert!(
            after_oversize > before_oversize,
            "oversize rejection should advance counter \
             (before={before_oversize}, after={after_oversize})",
        );

        // Aggregate-cap rejection: hold 10 bytes, then try 7 more (=17 > 16).
        let _permit = limiter.try_acquire(10).expect("first permit");
        let before_aggregate = metrics.inflight_bytes_rejected_total.get();
        assert!(limiter.try_acquire(7).is_none());
        let after_aggregate = metrics.inflight_bytes_rejected_total.get();
        assert!(
            after_aggregate > before_aggregate,
            "aggregate-cap rejection should advance counter \
             (before={before_aggregate}, after={after_aggregate})",
        );

        // Negative control: a successful acquire must NOT bump the
        // counter. With the 10-byte permit held, asking for 6 fits
        // exactly under the cap.
        let before_ok = metrics.inflight_bytes_rejected_total.get();
        let _ok_permit = limiter.try_acquire(6).expect("permit under cap");
        let after_ok = metrics.inflight_bytes_rejected_total.get();
        assert_eq!(
            before_ok, after_ok,
            "successful acquire must not advance rejection counter",
        );
    }
}
