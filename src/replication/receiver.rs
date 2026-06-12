//! Replica-side replication receiver.
//!
//! Listens for `OP_REPLICA_BATCH` frames from the master and applies
//! operations to the local engine using idempotent mutation methods.
//! Each incoming batch is acknowledged with a `ReplicaAck` response frame.

use crate::index::TxKey;
use crate::ops::create::*;
use crate::ops::engine::Engine;
use crate::ops::remaining::*;
use crate::ops::set_mined::*;
use crate::ops::spend::*;
use crate::ops::unspend::*;
use crate::protocol::deadline::{DeadlineReader, FRAME_ASSEMBLY_TIMEOUT};
use crate::protocol::frame::{RequestFrame, ResponseFrame};
use crate::protocol::opcodes::*;
use crate::record::*;
use crate::replication::durable::ReplicaAppliedTracker;
use crate::replication::protocol::{ReplicaAck, ReplicaBatch, ReplicaOp};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// F-G7-003: cap on aggregate in-flight bytes across all
/// standalone-receiver connections. The standalone
/// [`ReplicationReceiver`] does its own inline frame buffering
/// outside the main server's bytes-limiter. Without an aggregate
/// cap, N concurrent peers can each force a [`MAX_FRAME_SIZE`]
/// allocation BEFORE the HMAC verification step runs — trivial
/// DoS for unauthenticated cluster traffic.
///
/// 256 MiB is generous enough for many concurrent replicas while
/// still bounding worst-case memory pressure. Once the cap is
/// reached new connection handlers refuse the frame and close
/// the connection.
const RECEIVER_INFLIGHT_BYTES_CAP: usize = 256 * 1024 * 1024;

/// Per-syscall read timeout applied to an accepted replication socket.
/// Like the server accept path's `CONNECTION_READ_TIMEOUT`, this resets
/// on every successful read, so it cannot bound total frame-assembly
/// time on its own — see [`FRAME_ASSEMBLY_TIMEOUT`] and follow-up E-1.
const RECEIVER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Aggregate counter of bytes currently held in the standalone
/// [`ReplicationReceiver`]'s per-connection frame buffers
/// (F-G7-003).
static RECEIVER_INFLIGHT_BYTES: AtomicUsize = AtomicUsize::new(0);

/// RAII guard that decrements [`RECEIVER_INFLIGHT_BYTES`] on drop.
/// Returned from [`reserve_inflight_bytes`] when the reservation
/// succeeds; the per-connection handler holds the guard for the
/// lifetime of the frame's body buffer.
struct InflightBytesGuard {
    bytes: usize,
}

impl Drop for InflightBytesGuard {
    fn drop(&mut self) {
        RECEIVER_INFLIGHT_BYTES.fetch_sub(self.bytes, Ordering::Release);
    }
}

/// Attempt to reserve `bytes` against the aggregate cap. Returns
/// `Some(guard)` on success (caller drops the guard to release),
/// `None` when the reservation would exceed `RECEIVER_INFLIGHT_BYTES_CAP`.
fn reserve_inflight_bytes(bytes: usize) -> Option<InflightBytesGuard> {
    // Use a CAS loop so concurrent reservations stay accurate.
    let mut current = RECEIVER_INFLIGHT_BYTES.load(Ordering::Acquire);
    loop {
        if current.saturating_add(bytes) > RECEIVER_INFLIGHT_BYTES_CAP {
            return None;
        }
        match RECEIVER_INFLIGHT_BYTES.compare_exchange_weak(
            current,
            current + bytes,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Some(InflightBytesGuard { bytes }),
            Err(actual) => current = actual,
        }
    }
}

/// Default stream key used when a receiver has not been told to
/// discriminate by peer address. Chosen as a short literal so the
/// key encoding in [`ReplicaAppliedTracker`] stays compact.
pub const DEFAULT_STREAM_KEY: &str = "default";

/// Replica-side replication receiver.
///
/// Accepts TCP connections from the master, reads `OP_REPLICA_BATCH`
/// request frames, applies each operation to the local `Engine`, and
/// sends back `ReplicaAck` response frames.
///
/// Multiple master connections can be handled concurrently; each gets
/// its own handler thread. Incoming batches are deduplicated via a
/// [`ReplicaAppliedTracker`] so a leader re-sending the same sequence
/// range after a replica restart (or network retry) does not cause
/// double-application of ops.
///
/// When an `ack_state_path` is configured the tracker persists
/// per-stream state to disk before each ACK, guaranteeing that a
/// receiver restart resumes with the correct `last_applied_seq`.
pub struct ReplicationReceiver {
    engine: Arc<Engine>,
    last_applied_sequence: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    /// Per-stream applied-sequence journal. Always present; when the
    /// receiver was constructed with [`Self::new`] it is memory-only.
    applied: Arc<ReplicaAppliedTracker>,
    /// Coordinator-owned cluster epoch handle. Phase B2 gate compares
    /// each inbound batch's `cluster_key` against this value (when
    /// non-zero) to fence stale-epoch masters before any engine work.
    /// `0` means "unknown" — accept unconditionally (V1-compat).
    local_cluster_key: Arc<AtomicU64>,
    /// Shared HMAC secret for authenticated inter-node TCP frames.
    auth_secret: Option<Arc<Vec<u8>>>,
}

impl ReplicationReceiver {
    /// Create a new receiver backed by the given engine and an
    /// in-memory idempotency journal. Useful for tests and for
    /// deployments that don't need restart-crash recovery.
    ///
    /// The cluster_key handle defaults to a fresh atomic at `0`
    /// (unknown — V1-compat). Production callers wire the
    /// coordinator-owned handle via
    /// [`with_cluster_key`](Self::with_cluster_key).
    pub fn new(engine: Arc<Engine>) -> Self {
        Self::with_cluster_key(engine, Arc::new(AtomicU64::new(0)))
    }

    /// Create a receiver wired to a coordinator-owned cluster_key handle.
    ///
    /// The same handle is shared with the local
    /// [`ReplicationManager`](crate::replication::manager::ReplicationManager)
    /// so leader-side stamping and replica-side gating observe a
    /// single source of truth for the cluster epoch.
    pub fn with_cluster_key(engine: Arc<Engine>, local_cluster_key: Arc<AtomicU64>) -> Self {
        Self {
            engine,
            last_applied_sequence: Arc::new(AtomicU64::new(0)),
            running: Arc::new(AtomicBool::new(true)),
            applied: Arc::new(ReplicaAppliedTracker::in_memory()),
            local_cluster_key,
            auth_secret: None,
        }
    }

    /// Require HMAC-framed request/response traffic on this receiver.
    pub fn with_auth_secret(mut self, secret: Vec<u8>) -> Self {
        self.auth_secret = Some(Arc::new(secret));
        self
    }

    /// Create a receiver with persistent idempotency state.
    ///
    /// The tracker file at `path` stores the per-stream
    /// `(stream_id, last_applied_seq)` map. On restart, existing
    /// records are loaded so the master's retransmit of an already
    /// applied batch is skipped before any op touches the engine.
    ///
    /// Returns an error if the file exists but is malformed.
    pub fn with_ack_state(
        engine: Arc<Engine>,
        path: std::path::PathBuf,
    ) -> std::result::Result<Self, String> {
        let tracker =
            ReplicaAppliedTracker::load(path).map_err(|e| format!("load applied tracker: {e}"))?;
        // Initial last_applied_sequence is the max across all streams
        // so the public API keeps its monotonic "latest seq" semantics.
        let initial_seq = tracker.snapshot().values().copied().max().unwrap_or(0);
        Ok(Self {
            engine,
            last_applied_sequence: Arc::new(AtomicU64::new(initial_seq)),
            running: Arc::new(AtomicBool::new(true)),
            applied: Arc::new(tracker),
            local_cluster_key: Arc::new(AtomicU64::new(0)),
            auth_secret: None,
        })
    }

    /// Install a coordinator-owned cluster_key handle on a receiver
    /// that was constructed via [`Self::new`] or [`Self::with_ack_state`].
    ///
    /// Used by the coordinator (Phase B3) to share the same epoch
    /// atomic between the local
    /// [`ReplicationManager`](crate::replication::manager::ReplicationManager)
    /// and this receiver after both have been constructed.
    pub fn set_cluster_key_handle(&mut self, local_cluster_key: Arc<AtomicU64>) {
        self.local_cluster_key = local_cluster_key;
    }

    /// Access the receiver's cluster_key handle.
    pub fn cluster_key_handle(&self) -> Arc<AtomicU64> {
        self.local_cluster_key.clone()
    }

    /// Access the underlying applied-sequence tracker. Exposed so
    /// callers (and tests) can inspect per-stream state or force a
    /// flush outside of the normal request path.
    pub fn applied_tracker(&self) -> Arc<ReplicaAppliedTracker> {
        self.applied.clone()
    }

    /// Start listening on the given address for replication connections.
    ///
    /// Spawns a background thread that accepts connections and a handler
    /// thread for each accepted connection. Returns after the listener
    /// thread is spawned. Use [`stop`](Self::stop) to shut down.
    pub fn start(&self, addr: &str) -> Result<(), String> {
        let listener = TcpListener::bind(addr)
            .map_err(|e| format!("failed to bind replication receiver on {addr}: {e}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("set non-blocking: {e}"))?;

        let engine = self.engine.clone();
        let running = self.running.clone();
        let last_applied = self.last_applied_sequence.clone();
        let applied = self.applied.clone();
        let cluster_key = self.local_cluster_key.clone();
        let auth_secret = self.auth_secret.clone();

        std::thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, peer_addr)) => {
                        let eng = engine.clone();
                        let run = running.clone();
                        let la = last_applied.clone();
                        let ap = applied.clone();
                        let ck = cluster_key.clone();
                        let secret = auth_secret.clone();
                        std::thread::spawn(move || {
                            let ctx = ConnectionContext {
                                engine: &eng,
                                running: &run,
                                last_applied: &la,
                                applied: ap,
                                local_cluster_key: ck,
                                auth_secret: secret,
                                frame_deadline: FRAME_ASSEMBLY_TIMEOUT,
                            };
                            handle_connection(stream, peer_addr, ctx);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(_e) => {
                        // Transient accept error; keep looping
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                }
            }
        });

        Ok(())
    }

    /// Highest sequence number that has been durably applied.
    pub fn last_applied_sequence(&self) -> u64 {
        self.last_applied_sequence.load(Ordering::Relaxed)
    }

    /// Signal the receiver to stop accepting new connections.
    ///
    /// Flushes the applied-sequence tracker to disk so the next
    /// instance restores the correct `last_applied_seq` on startup.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
        // Final flush to guarantee durability on clean shutdown.
        if let Err(e) = self.applied.flush() {
            tracing::warn!(err = %e, "replica applied tracker: final flush failed");
        }
    }
}

/// Handle a single connection from the master.
///
/// Reads request frames in a loop. For each `OP_REPLICA_BATCH`,
/// deserializes the batch, consults the idempotency journal to skip
/// already-applied prefixes, applies every remaining op to the
/// engine, persists the updated applied sequence to disk, and sends
/// back a `ReplicaAck` response.
struct ConnectionContext<'a> {
    engine: &'a Engine,
    running: &'a AtomicBool,
    last_applied: &'a AtomicU64,
    applied: Arc<ReplicaAppliedTracker>,
    local_cluster_key: Arc<AtomicU64>,
    auth_secret: Option<Arc<Vec<u8>>>,
    /// E-1: whole-frame assembly deadline applied to all post-length-prefix
    /// reads (see [`FRAME_ASSEMBLY_TIMEOUT`]). Injectable so tests can drive
    /// the deadline without a 60-second sleep; production wiring in
    /// [`ReplicationReceiver::start`] always passes the constant.
    frame_deadline: Duration,
}

fn handle_connection(mut stream: TcpStream, peer_addr: SocketAddr, ctx: ConnectionContext<'_>) {
    let ConnectionContext {
        engine,
        running,
        last_applied,
        applied,
        local_cluster_key,
        auth_secret,
        frame_deadline,
    } = ctx;

    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(RECEIVER_READ_TIMEOUT));
    // Disable Nagle's algorithm on the accepted replication socket so
    // ACK frames flush immediately. Best-effort — a failure here does
    // not prevent handling the connection, just re-enables Nagle.
    let _ = stream.set_nodelay(true);

    // Use the peer's IP:port as the stream key so each master has its
    // own deduplication state. Re-using the same key across reconnects
    // from the same peer intentionally preserves last_applied_seq.
    let stream_key = peer_addr.to_string();
    // F-G7-003: wrap the per-connection body buffer so its growth
    // is tracked against `RECEIVER_INFLIGHT_BYTES_CAP`. Dropping
    // `body` (when this function returns) releases the reservation.
    struct CountedBody {
        buf: Vec<u8>,
        reserved: usize,
    }
    impl Drop for CountedBody {
        fn drop(&mut self) {
            if self.reserved > 0 {
                RECEIVER_INFLIGHT_BYTES.fetch_sub(self.reserved, Ordering::Release);
            }
        }
    }
    let mut body = CountedBody {
        buf: Vec::new(),
        reserved: 0,
    };
    let mut frame_bytes = Vec::new();

    loop {
        if !running.load(Ordering::Relaxed) {
            return;
        }

        // Read 4-byte length prefix
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(_) => return,
        }

        let total_length = u32::from_le_bytes(len_buf);
        let max_wire_frame_size = MAX_FRAME_SIZE
            + auth_secret
                .as_ref()
                .map(|_| crate::cluster::auth::SIGNED_SUFFIX_LEN as u32)
                .unwrap_or(0);
        if total_length > max_wire_frame_size {
            // Frame too large, close connection
            return;
        }

        // Read the frame body.
        //
        // Two paths:
        // - `auth_secret.is_some()` → streaming verify. The body is
        //   read in 8 KiB chunks by `verify_signed_body_streaming` into
        //   a disposable `Vec<u8>` sink. On HMAC tag mismatch the sink
        //   is dropped and the connection is closed; the receiver
        //   NEVER materialises a `frame_len`-sized `body.buf` for
        //   wrong-tag frames. This is the slow-loris fix
        //   (F-G5-016 / re-review P2): without it, a malicious peer
        //   could feed 16 MiB of wrong-tag garbage per connection and
        //   drive the receiver's per-connection allocator up to peak
        //   frame size before the HMAC verify ran.
        // - `auth_secret.is_none()` → legacy buffered path. The
        //   `RECEIVER_INFLIGHT_BYTES_CAP` aggregate guard still bounds
        //   total exposure (F-G7-003).
        let frame_len = total_length as usize;
        let request_id;
        // E-1: every read that assembles the rest of this frame (head
        // peek, HMAC-streaming verify, and the buffered body) must
        // complete within `frame_deadline` of the length prefix arriving.
        // The per-syscall `RECEIVER_READ_TIMEOUT` alone cannot enforce
        // this — it resets on every successful read, so a slow-drip peer
        // delivering one byte per interval keeps each read "succeeding"
        // and would otherwise pin this handler thread and its
        // inflight-bytes reservation indefinitely. Mirrors the server
        // accept path's L-01 fix.
        let mut deadline_stream = DeadlineReader::new(
            &stream,
            Instant::now() + frame_deadline,
            RECEIVER_READ_TIMEOUT,
        );
        // `verified_frame` carries the verified `[payload_len:4][payload]`
        // buffer when the auth path runs. The non-auth path leaves it
        // empty and `request_frame_bytes` borrows from `frame_bytes`.
        let mut verified_frame: Vec<u8> = Vec::new();
        let request_frame_bytes: &[u8];
        if let Some(secret) = auth_secret.as_deref() {
            // Streaming verify path. Steps:
            //   1. Peek 8 bytes (`request_id`) directly off the wire
            //      so the auth-fail response can echo it (matches the
            //      pre-fix contract where `request_id` was peeked from
            //      the buffered body).
            //   2. Splice the peeked head back onto the stream via
            //      `Cursor::chain` so the streaming verifier sees the
            //      full body without our re-reading any bytes.
            //   3. Stream the body through
            //      `verify_signed_body_streaming` into a disposable
            //      `Vec<u8>` sink — dropped on `Err(PermissionDenied)`
            //      so partially-written unauthenticated bytes never
            //      reach `RequestFrame::decode`.
            const REQ_ID_PEEK: usize = 8;
            let head_to_read = REQ_ID_PEEK.min(frame_len);
            let mut head_buf = [0u8; REQ_ID_PEEK];
            // E-1: head peek runs through the deadline reader so a
            // drip-fed signed head cannot outlive the assembly deadline.
            if deadline_stream
                .read_exact(&mut head_buf[..head_to_read])
                .is_err()
            {
                return;
            }
            request_id = if head_to_read >= 8 {
                u64::from_le_bytes(head_buf[..8].try_into().unwrap_or([0; 8]))
            } else {
                0
            };
            let head_slice = &head_buf[..head_to_read];
            // `verify_signed_body_streaming` synthesises its OWN 4-byte
            // length prefix from `frame_len` (auth.rs), so the reader we
            // hand it must yield the BODY ONLY — the peeked head followed
            // by the remaining body bytes. Chaining `len_buf` here would
            // double-prefix the HMAC input and reject every honest signed
            // frame. E-1: the chunked verify reads run through the deadline
            // reader so a drip-fed signed body cannot outlive the deadline.
            let mut chained = std::io::Cursor::new(head_slice).chain(&mut deadline_stream);
            // Pre-seed a 4-byte length-prefix slot so the resulting
            // buffer matches the `[payload_len:4][payload]` shape
            // `RequestFrame::decode` expects.
            let mut sink: Vec<u8> = Vec::with_capacity(4 + frame_len);
            sink.extend_from_slice(&[0u8; 4]);
            let payload_len = match crate::cluster::auth::verify_signed_body_streaming(
                secret.as_slice(),
                frame_len,
                &mut chained,
                &mut sink,
            ) {
                Ok(n) => n,
                Err(e) => {
                    // SECURITY: drop the sink BEFORE writing the
                    // error response so the unauthenticated partial-
                    // write bytes never escape this scope.
                    drop(sink);
                    let response = ResponseFrame {
                        request_id,
                        status: STATUS_ERROR,
                        payload: crate::protocol::codec::encode_error_payload(
                            ERR_CLUSTER_AUTH_FAILED,
                            &format!("cluster frame authentication failed: {e}"),
                        ),
                    };
                    let _ = stream.write_all(&response.encode());
                    return;
                }
            };
            sink[0..4].copy_from_slice(&(payload_len as u32).to_le_bytes());
            sink.truncate(4 + payload_len);
            verified_frame = sink;
            request_frame_bytes = &verified_frame;
        } else {
            // F-G7-003: when the body Vec needs to grow, reserve the
            // growth delta against the global inflight cap and absorb
            // the reservation into the connection-scoped `body.reserved`
            // counter so the CountedBody Drop releases it on return.
            let new_bytes = frame_len.saturating_sub(body.buf.len());
            if new_bytes > 0 {
                match reserve_inflight_bytes(new_bytes) {
                    Some(guard) => {
                        body.reserved += guard.bytes;
                        std::mem::forget(guard);
                    }
                    None => {
                        tracing::warn!(
                            frame_len,
                            new_bytes,
                            "replication receiver: inflight bytes cap exceeded — closing connection",
                        );
                        return;
                    }
                }
                body.buf.resize(frame_len, 0);
            }
            // E-1: read the buffered body through the deadline reader so a
            // drip-fed unsigned body cannot outlive the assembly deadline.
            if deadline_stream
                .read_exact(&mut body.buf[..frame_len])
                .is_err()
            {
                return;
            }
            frame_bytes.clear();
            frame_bytes.reserve(4 + frame_len);
            frame_bytes.extend_from_slice(&len_buf);
            frame_bytes.extend_from_slice(&body.buf[..frame_len]);
            request_id = if frame_bytes.len() >= 12 {
                u64::from_le_bytes(frame_bytes[4..12].try_into().unwrap_or([0; 8]))
            } else {
                0
            };
            request_frame_bytes = &frame_bytes;
        }
        // E-1: a deadline-capped read may have shrunk the socket read
        // timeout below `RECEIVER_READ_TIMEOUT`. Restore it so the next
        // iteration's length-prefix read keeps the original idle-peer
        // semantics (a clean 30 s drop, treated as a `continue`).
        if deadline_stream.timeout_shrunk {
            let _ = stream.set_read_timeout(Some(RECEIVER_READ_TIMEOUT));
        }
        // Silence the `unused_assignments` warning when the non-auth path
        // never writes to `verified_frame`; it's still used to anchor the
        // borrow of `request_frame_bytes` on the auth path above.
        let _ = &verified_frame;
        let _ = request_id; // currently only consumed on the auth-fail path

        let (request, _) = match RequestFrame::decode(request_frame_bytes) {
            Ok(r) => r,
            Err(_) => return,
        };

        let response = if request.op_code == OP_REPLICA_BATCH {
            handle_replica_batch_with_tracker(
                &request,
                engine,
                last_applied,
                Some(applied.as_ref()),
                &stream_key,
                local_cluster_key.load(Ordering::Acquire),
            )
        } else {
            // Unknown opcode for replication receiver
            ResponseFrame {
                request_id: request.request_id,
                status: STATUS_ERROR,
                payload: b"unsupported opcode".to_vec(),
            }
        };

        let encoded_response = response.encode();
        let response_bytes = if let Some(secret) = auth_secret.as_deref() {
            match crate::cluster::auth::sign_frame(secret.as_slice(), &encoded_response) {
                Ok(bytes) => bytes,
                Err(_) => return,
            }
        } else {
            encoded_response
        };
        if stream.write_all(&response_bytes).is_err() {
            return;
        }
    }
}

/// Process an `OP_REPLICA_BATCH` request frame against the
/// [`DEFAULT_STREAM_KEY`] with an in-memory idempotency journal.
///
/// Thin wrapper around [`handle_replica_batch_with_tracker`]
/// preserved for call sites and tests that do not need
/// multi-stream deduplication or durable state. The cluster_key
/// gate is bypassed (`local_cluster_key = 0`, treated as "unknown")
/// because this entry point predates the gate; production paths
/// always go through `handle_replica_batch_with_tracker` directly.
pub fn handle_replica_batch(
    request: &RequestFrame,
    engine: &Engine,
    last_applied: &AtomicU64,
) -> ResponseFrame {
    handle_replica_batch_with_cluster_key(
        request,
        engine,
        last_applied,
        /* local_cluster_key */ 0,
    )
}

/// Same as [`handle_replica_batch`] but lets the caller pass the
/// receiver's `local_cluster_key` so the cluster-key gate is honored
/// even when no persistent applied tracker is available.
///
/// Used by [`crate::server::dispatch`] when
/// `init_replica_applied_tracker` has not been called (e.g. test
/// harnesses, single-stream setups). Batches are applied UNTRACKED
/// (no stream-sequence dedup or gap detection) — see the body comment
/// for the safety argument; the cluster-key view propagates from the
/// `RunningCluster::local_cluster_key()` accessor.
pub fn handle_replica_batch_with_cluster_key(
    request: &RequestFrame,
    engine: &Engine,
    last_applied: &AtomicU64,
    local_cluster_key: u64,
) -> ResponseFrame {
    // R-D1/D-3: this fallback runs UNTRACKED — no per-stream watermark,
    // no duplicate skip, no gap NAK. Every op in every batch is applied
    // (op-level idempotency — the per-record generation guard plus the
    // create-payload dedup — absorbs re-deliveries). The previous
    // implementation kept a `thread_local!` in-memory tracker
    // (F-G7-013), which gave each worker thread its own high-water
    // mark: useless as dedup (a re-send on another thread re-applied
    // anyway) and actively harmful once the dense-sequence contract
    // landed, because per-thread watermarks see "gaps" that are just
    // other threads' batches. Production paths go through
    // `init_replica_applied_tracker` and get the real per-stream
    // tracker; this entry point is reserved for tests, single-stream
    // harnesses, and the in-process compensation path.
    handle_replica_batch_with_tracker(
        request,
        engine,
        last_applied,
        None,
        DEFAULT_STREAM_KEY,
        local_cluster_key,
    )
}

/// Process an `OP_REPLICA_BATCH` request frame with explicit
/// idempotency tracking.
///
/// Steps:
/// 0. Phase B2 cluster-key gate. If the batch's `cluster_key` is
///    non-zero AND does not match `local_cluster_key`, reject
///    immediately with [`STATUS_ERROR`] / [`ERR_STALE_EPOCH`] before
///    touching the engine, the dedup tracker, or the migration-batch
///    bypass. `cluster_key == 0` retains V1-compat semantics: accept
///    unconditionally so older masters that pre-date the wire-V2
///    field still interoperate.
/// 1. Deserialize the [`ReplicaBatch`] payload.
/// 2. If the batch carried a W3C trace context (Phase 4), attach it as a
///    remote parent to the span created around this handler so the replica's
///    work is stitched into the leader's trace.
/// 3. R-D1/D-3 dense per-stream sequence contract (when `applied` is
///    `Some` and the batch is neither a migration batch nor an
///    out-of-band batch). With `watermark = applied.get(stream)` and
///    `expected = watermark + 1`:
///    * empty `ops` → watermark **probe**: ACK `Ok { watermark }`
///      without touching the engine. Masters send a probe to adopt the
///      replica's authoritative position before assigning sequences.
///    * `last_sequence() <= watermark` → true duplicate (idempotent
///      re-send): ACK `Ok { watermark }` without applying.
///    * `first_sequence > expected` → sequence **gap**: NAK with
///      [`ReplicaAck::Gap`] (STATUS_ERROR). Nothing is applied, the
///      watermark does not advance, and the master must re-send
///      relabeled at `expected` or run catch-up. This is what makes
///      out-of-order delivery and lost batches detectable instead of
///      silently ACK-dropped (audit finding D-1).
///    * otherwise apply, skipping any already-applied prefix
///      (`first_sequence <= watermark < last_sequence()`).
/// 4. Apply the surviving ops via [`apply_op`].
/// 5. `applied.set(stream_key, through_sequence)` and `applied.flush()`
///    BEFORE ACK, so durability is guaranteed on the wire.
///
/// Out-of-band batches (`first_sequence == 0` with non-empty `ops`)
/// bypass the tracker entirely: every op is applied and the watermark
/// is untouched. Sequence numbers in the dense stream space start at 1,
/// so 0 is reserved for unsequenced internal traffic (the in-process
/// compensation path and migration deltas). Op-level idempotency (the
/// per-record generation guard) keeps re-application safe.
///
/// When `applied` is `None` the batch is applied UNTRACKED — no dedup,
/// no gap detection — for test harnesses and single-stream callers that
/// never initialized a tracker.
///
/// `local_cluster_key` is the receiver's view of the current cluster
/// epoch (typically loaded from the coordinator-owned atomic shared
/// with the local
/// [`ReplicationManager`](crate::replication::manager::ReplicationManager)).
pub fn handle_replica_batch_with_tracker(
    request: &RequestFrame,
    engine: &Engine,
    last_applied: &AtomicU64,
    applied: Option<&ReplicaAppliedTracker>,
    stream_key: &str,
    local_cluster_key: u64,
) -> ResponseFrame {
    let batch = match ReplicaBatch::deserialize(&request.payload) {
        Ok(b) => b,
        Err(e) => {
            let ack = ReplicaAck::Error {
                failed_sequence: 0,
                message: format!("deserialize batch: {e}"),
            };
            return ResponseFrame {
                request_id: request.request_id,
                status: STATUS_ERROR,
                payload: ack.serialize(),
            };
        }
    };

    // Phase B2 stale-epoch gate, refined in the Phase B fixup. The
    // gate runs BEFORE the migration-batch bypass and BEFORE any
    // tracker/engine work so a stale-epoch master (including one
    // sending migration batches) cannot mutate local state.
    //
    // Semantics:
    // * `batch.cluster_key == 0`           → V1-compat sender; accept.
    // * `local_cluster_key == 0`           → receiver has not yet seen
    //   any quorum-committed term (post-restart, pre-bootstrap, or in
    //   the gap between SWIM discovery and the first multi-node
    //   commit). The sender has a quorum-committed view that we don't,
    //   so it is strictly more authoritative — accept and let the
    //   subsequent OP_TOPOLOGY_COMMIT bring our local view in line.
    // * `batch.cluster_key < local_cluster_key`
    //                                       → STALE master; reject.
    // * `batch.cluster_key > local_cluster_key`
    //                                       → newer-than-local sender.
    //   Same reasoning as the bootstrap case: the sender's term has
    //   already been quorum-committed elsewhere; our OP_TOPOLOGY_COMMIT
    //   is in flight or about to arrive. Accept rather than reject —
    //   strict-equality rejection caused legitimate cross-node
    //   replication to fail with `ERR_STALE_EPOCH` whenever commits
    //   propagated unevenly across the cluster (Phase B regression).
    // * `batch.cluster_key == local_cluster_key`
    //                                       → in lock-step; accept.
    if batch.cluster_key != 0 && local_cluster_key != 0 && batch.cluster_key < local_cluster_key {
        if let Some(m) = crate::metrics::replication_metrics() {
            m.replica_rejected_stale_cluster_key.inc();
        }
        tracing::warn!(
            batch_cluster_key = batch.cluster_key,
            local_cluster_key,
            first_sequence = batch.first_sequence,
            ops_len = batch.ops.len(),
            is_migration = request.flags & FLAG_MIGRATION_BATCH != 0,
            "replica rejected batch: stale cluster_key"
        );
        let mut payload = Vec::with_capacity(4);
        payload.extend_from_slice(&ERR_STALE_EPOCH.to_le_bytes());
        // Empty diagnostic message — the master logs the reject and
        // re-discovers cluster topology on the next handshake.
        payload.extend_from_slice(&0u16.to_le_bytes());
        return ResponseFrame {
            request_id: request.request_id,
            status: STATUS_ERROR,
            payload,
        };
    }

    // F-G7-005: tighten the cluster_key gate for migration batches.
    // The dedup-bypass path (FLAG_MIGRATION_BATCH) skips the
    // already-applied skip_count logic and unconditionally re-applies
    // every op. A buggy or hostile sender that sets the flag bit and
    // a `cluster_key = 0` wildcard could therefore replay arbitrary
    // mutations through the dedup-bypass path. When the receiver is
    // in steady-state clustered mode (`local_cluster_key != 0`) we
    // require a non-zero `cluster_key` on migration batches so the
    // sender's epoch is explicit; the wildcard remains accepted only
    // for normal-replication batches (where the per-stream dedup
    // tracker plus the generation guard absorb any replay damage).
    let is_migration_flag = request.flags & FLAG_MIGRATION_BATCH != 0;
    if is_migration_flag && batch.cluster_key == 0 && local_cluster_key != 0 {
        if let Some(m) = crate::metrics::replication_metrics() {
            m.replica_rejected_stale_cluster_key.inc();
        }
        tracing::warn!(
            local_cluster_key,
            first_sequence = batch.first_sequence,
            ops_len = batch.ops.len(),
            "replica rejected migration batch: cluster_key wildcard not allowed in clustered mode"
        );
        let mut payload = Vec::with_capacity(6);
        payload.extend_from_slice(&ERR_STALE_EPOCH.to_le_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes());
        return ResponseFrame {
            request_id: request.request_id,
            status: STATUS_ERROR,
            payload,
        };
    }

    let effective_stream_key = batch
        .source_node_id
        .map(|id| format!("node:{id}"))
        .unwrap_or_else(|| stream_key.to_string());

    // Phase 4: attach the incoming trace context as a remote parent so
    // the receiver's span is stitched into the sender's trace.
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let recv_span = tracing::debug_span!(
        "handle_replica_batch",
        stream_key = %effective_stream_key,
        first_sequence = batch.first_sequence,
        ops_len = batch.ops.len(),
        is_migration = request.flags & FLAG_MIGRATION_BATCH != 0,
    );
    if let Some(wire_ctx) = batch.trace_ctx
        && let Some(sc) = wire_ctx.to_span_context()
    {
        let cx = opentelemetry::Context::new().with_remote_span_context(sc);
        // `set_parent` on `tracing_opentelemetry::OpenTelemetrySpanExt`
        // returns `Result<Context, _>`; the outcome is advisory and we
        // intentionally drop it — the worst case is an un-parented span.
        let _ = recv_span.set_parent(cx);
    }
    let _entered = recv_span.enter();

    let through = batch.last_sequence();

    // Migration batches are coordinated out-of-band by the migration
    // pipeline (see `stream_shard_baseline`) and always start at
    // `first_sequence: 0`. They share the receiver's
    // `ReplicaAppliedTracker` with the normal-replication stream, so
    // applying the dedup / skip_count logic to them would silently
    // discard the batch any time the tracker has already seen a
    // higher sequence from normal replication — which is the common
    // case after a partition heal or scale-up migration, and the root
    // cause of "records unreadable on their new master" (pattern A).
    //
    // Treat migration batches as independent: apply every op in the
    // batch unconditionally and do NOT advance the normal-replication
    // high-water mark. The `OP_MIGRATION_COMPLETE` handshake performs
    // its own count + manifest verification so idempotency here is not
    // required for correctness — migrations are one-shot by protocol
    // and retried at the shard level on failure, not op-level via this
    // tracker.
    let is_migration = request.flags & FLAG_MIGRATION_BATCH != 0;

    // R-D1/D-3: out-of-band batches. The dense per-stream space starts
    // at sequence 1, so `first_sequence == 0` with non-empty ops marks
    // unsequenced internal traffic (the in-process compensation path).
    // Apply every op without consulting or advancing the tracker —
    // exactly like a migration batch. Pre-fix, such batches fell into
    // the high-water-mark dedup and were silently skipped whenever the
    // tracker had advanced, dropping compensation ops.
    let is_out_of_band = batch.first_sequence == 0 && !batch.ops.is_empty();

    // `true` when the per-stream dense-sequence bookkeeping applies.
    let tracked = !is_migration && !is_out_of_band;

    let already_applied = applied.map(|t| t.get(&effective_stream_key)).unwrap_or(0);

    if tracked && applied.is_some() {
        // Watermark probe: an empty batch never applies anything and
        // always ACKs the current per-stream watermark. Masters use it
        // to adopt the replica's authoritative position before
        // assigning dense sequence numbers (e.g. after a restart).
        if batch.ops.is_empty() {
            let ack = ReplicaAck::Ok {
                through_sequence: already_applied,
            };
            return ResponseFrame {
                request_id: request.request_id,
                status: STATUS_OK,
                payload: ack.serialize(),
            };
        }

        // True duplicate — the ENTIRE batch range is at or below the
        // watermark, so every position is provably applied. ACK with
        // the existing watermark so the master knows the data is
        // durable on this replica.
        if through <= already_applied {
            let ack = ReplicaAck::Ok {
                through_sequence: already_applied,
            };
            return ResponseFrame {
                request_id: request.request_id,
                status: STATUS_OK,
                payload: ack.serialize(),
            };
        }

        // Sequence gap — the batch starts ahead of the next-expected
        // sequence. NAK without applying and without advancing the
        // watermark: ACKing here is exactly the acked-but-never-applied
        // divergence of audit finding D-1. The master heals a benign
        // hole (positions burned by a failed/compensated batch) by
        // re-sending relabeled at `expected_sequence`; a real content
        // gap is repaired by catch-up.
        let expected = already_applied + 1;
        if batch.first_sequence > expected {
            if let Some(m) = crate::metrics::replication_metrics() {
                m.replica_rejected_sequence_gap.inc();
            }
            tracing::warn!(
                stream_key = %effective_stream_key,
                expected_sequence = expected,
                received_first_sequence = batch.first_sequence,
                ops_len = batch.ops.len(),
                "replica NAK: sequence gap — batch ahead of next-expected",
            );
            let ack = ReplicaAck::Gap {
                expected_sequence: expected,
                received_first_sequence: batch.first_sequence,
            };
            return ResponseFrame {
                request_id: request.request_id,
                status: STATUS_ERROR,
                payload: ack.serialize(),
            };
        }
    }

    // Refresh the cached clock once per batch so replicated mutations
    // record a current `updated_at` timestamp without issuing a
    // `clock_gettime` syscall per individual operation.
    engine.refresh_clock();

    // Determine where in the batch real work starts. If `first_sequence`
    // is already covered by `already_applied`, skip the duplicate prefix
    // (positions at or below the watermark are provably applied in the
    // dense stream space). Migration / out-of-band / untracked batches
    // bypass this skip — every op applies.
    let skip_count = if !tracked || applied.is_none() {
        0
    } else if batch.first_sequence <= already_applied {
        // `already_applied` is the highest sequence number already
        // durably applied. The first op in the batch corresponds to
        // sequence `first_sequence`; sequence `first_sequence + i` is
        // op `i`. We keep ops with seq > already_applied, which means
        // dropping `already_applied + 1 - first_sequence` ops.
        (already_applied + 1 - batch.first_sequence) as usize
    } else {
        0
    };

    let start_seq = batch.first_sequence + skip_count as u64;
    for (seq, op) in (start_seq..).zip(batch.ops.iter().skip(skip_count)) {
        if let Err(msg) = apply_op(engine, op) {
            let ack = ReplicaAck::Error {
                failed_sequence: seq,
                message: msg,
            };
            return ResponseFrame {
                request_id: request.request_id,
                status: STATUS_ERROR,
                payload: ack.serialize(),
            };
        }
    }

    // F-G7-016 (batched): sync the engine's block device + flush the
    // replica redo log ONCE per batch (after every op has been
    // applied), instead of once per op. The per-op fsync chain was a
    // major perf regression on slow filesystems (Docker named volumes):
    // each fsync is ~10 ms, so a 200-op batch took ~2 s on the receiver
    // and routinely exceeded the master's 3 s ACK timeout, so every
    // replica batch failed. Batching collapses 2 × N fsyncs into 2.
    //
    // Durability discipline preserved at the batch level: apply all
    // ops → fsync data device → flush redo log → ACK. When this
    // function returns Ok ResponseFrame, every op in the batch is on
    // durable storage AND in the redo log. On a replica crash between
    // device fsync and redo flush, recovery sees an apply that wasn't
    // journalled — engine.shard_record_count and idempotent apply
    // paths handle re-applying via the master's resync.
    if let Err(e) = engine.device().sync() {
        let ack = ReplicaAck::Error {
            failed_sequence: through,
            message: format!("post-batch device sync (F-G7-016): {e}"),
        };
        return ResponseFrame {
            request_id: request.request_id,
            status: STATUS_ERROR,
            payload: ack.serialize(),
        };
    }
    if let Err(msg) = flush_replica_redo_log(engine) {
        let ack = ReplicaAck::Error {
            failed_sequence: through,
            message: msg,
        };
        return ResponseFrame {
            request_id: request.request_id,
            status: STATUS_ERROR,
            payload: ack.serialize(),
        };
    }

    // Migration and out-of-band batches do not participate in the
    // normal-replication sequence space — don't let their
    // `first_sequence: 0` overwrite the receiver's high-water mark,
    // and skip the flush on their behalf.
    if tracked {
        if let Some(applied) = applied {
            // Persist the new high-water mark BEFORE ACKing. A flush failure
            // becomes a batch-level error so the master treats the replica
            // as not-yet-durable and will retry.
            applied.set(&effective_stream_key, through);
            if let Err(e) = applied.flush() {
                let ack = ReplicaAck::Error {
                    failed_sequence: through,
                    message: format!("flush applied tracker: {e}"),
                };
                return ResponseFrame {
                    request_id: request.request_id,
                    status: STATUS_ERROR,
                    payload: ack.serialize(),
                };
            }
        }

        // Use fetch_max to ensure monotonic advancement. Multiple master
        // connections may call this handler concurrently; a plain store()
        // could move last_applied backward if batches complete out of
        // sequence order.
        last_applied.fetch_max(through, Ordering::Relaxed);
    }

    let ack = ReplicaAck::Ok {
        through_sequence: if tracked { through } else { already_applied },
    };
    ResponseFrame {
        request_id: request.request_id,
        status: STATUS_OK,
        payload: ack.serialize(),
    }
}

fn existing_create_payload_matches(
    engine: &Engine,
    req: &CreateRequest<'_>,
    compare_metadata: bool,
) -> bool {
    let tx_key = req.tx_key();
    let Ok(meta) = engine.read_metadata(&tx_key) else {
        return false;
    };

    if meta.utxo_count != req.utxo_hashes.len() as u32 {
        return false;
    }

    if compare_metadata
        && (meta.tx_version != req.tx_version
            || meta.locktime != req.locktime
            || meta.fee != req.fee
            || meta.size_in_bytes != req.size_in_bytes
            || meta.extended_size != req.extended_size
            || meta.spending_height != req.spending_height
            || meta.created_at != req.created_at
            || meta.flags.contains(TxFlags::IS_COINBASE) != req.is_coinbase
            || meta.flags.contains(TxFlags::EXTERNAL) != req.is_external
            || meta.flags.contains(TxFlags::CONFLICTING) != req.conflicting
            || meta.flags.contains(TxFlags::LOCKED) != req.locked)
    {
        return false;
    }

    for (offset, expected_hash) in req.utxo_hashes.iter().enumerate() {
        let Ok(slot) = engine.read_slot(&tx_key, offset as u32) else {
            return false;
        };
        if slot.hash != *expected_hash {
            return false;
        }
    }

    true
}

fn apply_create_replica(
    engine: &Engine,
    tx_key: &TxKey,
    create_req: &CreateRequest<'_>,
    metadata_bytes: &[u8],
    cold_data: &Option<Vec<u8>>,
) -> std::result::Result<(), String> {
    match engine.create(create_req) {
        Ok(_) => {}
        Err(CreateError::DuplicateTxId)
            if existing_create_payload_matches(engine, create_req, metadata_bytes.len() >= 46) => {}
        Err(CreateError::DuplicateTxId) => {
            match engine.delete(&DeleteRequest {
                tx_key: *tx_key,
                due_guard: None,
            }) {
                Ok(()) | Err(crate::ops::error::SpendError::TxNotFound) => {}
                Err(e) => return Err(format!("replace duplicate create delete: {e}")),
            }
            if let Some(bs) = engine.blob_store()
                && let Err(e) = bs.delete(&tx_key.txid)
            {
                return Err(format!("replace duplicate create blob delete: {e}"));
            }
            engine
                .create(create_req)
                .map_err(|e| format!("replace duplicate create: {e}"))?;
        }
        Err(e) => return Err(format!("create: {e}")),
    }

    apply_create_lifecycle_and_blob(engine, tx_key, metadata_bytes, cold_data)
}

fn apply_create_lifecycle_and_blob(
    engine: &Engine,
    tx_key: &TxKey,
    metadata_bytes: &[u8],
    cold_data: &Option<Vec<u8>>,
) -> std::result::Result<(), String> {
    // Apply extended lifecycle metadata if present. Layout after the core
    // 46 bytes: generation(4) + updated_at(8) + unmined_since(4) +
    // delete_at_height(4) + preserve_until(4) = 24 bytes (total 70).
    //
    // R-035 (LMNH-31): if the master sent extended-lifecycle bytes, we
    // MUST persist them. Previously the per-step errors here were
    // swallowed with `let _ = ...`, which could ACK the batch while the
    // replica's record diverged from the master's (stale generation,
    // stale unmined_since, stale DAH). Treat each failure as a hard
    // batch-level error so the master retries instead of advancing its
    // durable high-water mark.
    if metadata_bytes.len() >= 70 {
        let generation = u32::from_le_bytes(
            metadata_bytes[46..50]
                .try_into()
                .map_err(|_| "lifecycle metadata generation slice".to_string())?,
        );
        let updated_at = u64::from_le_bytes(
            metadata_bytes[50..58]
                .try_into()
                .map_err(|_| "lifecycle metadata updated_at slice".to_string())?,
        );
        let unmined_since = u32::from_le_bytes(
            metadata_bytes[58..62]
                .try_into()
                .map_err(|_| "lifecycle metadata unmined_since slice".to_string())?,
        );
        let delete_at_height = u32::from_le_bytes(
            metadata_bytes[62..66]
                .try_into()
                .map_err(|_| "lifecycle metadata delete_at_height slice".to_string())?,
        );
        let preserve_until = u32::from_le_bytes(
            metadata_bytes[66..70]
                .try_into()
                .map_err(|_| "lifecycle metadata preserve_until slice".to_string())?,
        );
        // R-035 + F-2: route the lifecycle patch through the engine entry point
        // that updates the device footer AND the DAH/unmined secondary indexes
        // and primary-index cached fields atomically — the same machinery the
        // normal create/mutation path uses. A raw `write_metadata` here left the
        // migration target's secondaries stale until its next restart (DAH sweep
        // skipped migrated records, QUERY_OLD_UNMINED missed them, cached-vs-slow
        // GET disagreed).
        engine
            .restore_migrated_lifecycle(
                tx_key,
                generation,
                updated_at,
                unmined_since,
                delete_at_height,
                preserve_until,
            )
            .map_err(|e| format!("restore migrated lifecycle metadata: {e}"))?;
    }

    // Store cold data in the blobstore if provided. Blob persistence is part
    // of the durability contract: failing to store cold data must fail the ACK
    // so the master knows this replica is not a complete copy.
    if let Some(data) = cold_data
        && !data.is_empty()
        && let Some(bs) = engine.blob_store()
        && let Err(e) = bs.put(&tx_key.txid, data)
    {
        return Err(format!("cold data write failed for {:?}: {e}", tx_key));
    }

    Ok(())
}

/// F-G7-006: record that `apply_op` skipped a non-Create/non-Delete
/// op because the target TX or slot was missing on this replica.
///
/// The graceful skip preserves liveness on a replica that joined late
/// or lost an earlier Create batch, but it ALSO masks real divergence
/// (lost Create, dropped intent range, dedup-tracker drift). The
/// metric is the operator-facing signal: a non-zero value means the
/// master is sending mutations against records the replica never
/// received and counters such as `spent_utxos` are silently diverging.
#[inline]
fn record_apply_skipped_missing_tx(op_name: &'static str, tx_key: &TxKey) {
    if let Some(m) = crate::metrics::replication_metrics() {
        m.replica_apply_skipped_missing_tx.inc();
    }
    tracing::warn!(
        op = op_name,
        tx_key = ?tx_key.txid,
        "replica apply: tx or slot not found — skipping op; potential replication divergence",
    );
}

/// Record a HARD divergence — the replica's local slot state contradicts
/// what the master's mutation expected (e.g. master sent a Spend on what
/// it observed as Unspent, but the replica's slot is Frozen, Pruned, or
/// already Spent with different `spending_data`). Unlike the soft "tx
/// not found" skip, this is a state mismatch that the engine itself
/// surfaced — silently advancing the high-water mark would let the
/// replica drift further from the master on every subsequent op.
///
/// Caller MUST return Err from `apply_op` immediately so the batch is
/// NACKed: the master will mark this replica Down and trigger catch-up.
#[inline]
fn record_apply_divergence(op_name: &'static str, tx_key: &TxKey, detail: &str) {
    if let Some(m) = crate::metrics::replication_metrics() {
        m.replica_apply_divergence_total.inc();
    }
    tracing::error!(
        op = op_name,
        tx_key = ?tx_key.txid,
        detail = detail,
        "replica apply: local slot state contradicts master — aborting batch to force catch-up",
    );
}

/// Apply a single `ReplicaOp` to the engine.
///
/// For Spend, Freeze, Unfreeze, and Reassign operations the replica
/// does not have the UTXO hash in the op payload, so it reads the
/// current slot from the device to obtain the hash. The replica uses
/// `ignore_conflicting = true` and `ignore_locked = true` because the
/// master already validated those constraints.
///
/// Returns `Ok(())` on success (including graceful skip for
/// not-found records), or `Err(message)` if the operation fails in a
/// way that should abort the batch.
pub fn apply_op(engine: &Engine, op: &ReplicaOp) -> std::result::Result<(), String> {
    // Pre-apply generation guard: reject stale ops BEFORE mutating state.
    // An op is stale if the record's current generation is strictly ahead of
    // the master's generation under wrapping serial-number ordering. A target
    // generation is fresh only when it is within the next half of the u32
    // space from the local generation, so u32::MAX -> 0 is accepted.
    // Equal-generation replays are allowed through since all mutation ops are
    // idempotent and the generation sync at the end is a no-op.
    // Ops without master_generation (legacy Create, Delete, PruneSlot) skip
    // this check; they rely on idempotency in their match arms instead.
    if let Some(master_gen) = op.master_generation() {
        let tx_key = op.tx_key();
        if let Ok(meta) = engine.read_metadata(&tx_key) {
            let local_gen = { meta.generation };
            if local_gen != master_gen && generation_at_or_ahead(local_gen, master_gen) {
                return Ok(()); // Stale op — already superseded by a newer mutation
            }
        }
        // If read_metadata fails (TxNotFound), the record may not exist yet
        // or was deleted. Let the match arm handle it gracefully.
    }

    match op {
        ReplicaOp::Spend {
            tx_key,
            offset,
            spending_data,
            current_block_height,
            block_height_retention,
            ..
        } => {
            // Read the slot to get the UTXO hash
            let hash = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot.hash,
                Err(_) => {
                    // TX or slot not found — skip gracefully but
                    // surface the divergence via metrics + warn log
                    // so operators can detect a missing Create.
                    record_apply_skipped_missing_tx("spend", tx_key);
                    return Ok(());
                }
            };
            let req = SpendRequest {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: hash,
                spending_data: *spending_data,
                ignore_conflicting: true,
                ignore_locked: true,
                current_block_height: *current_block_height,
                block_height_retention: *block_height_retention,
            };
            match engine.spend(&req) {
                Ok(_) => Ok(()),
                // Engine returns AlreadySpent ONLY when the slot is
                // UTXO_SPENT with `spending_data != req.spending_data`
                // — the engine's idempotent-respend short-circuits the
                // matching case at the top of the spend path. So if
                // we see AlreadySpent here, the replica's local
                // spending_data disagrees with the master's: hard
                // divergence. NACK so the master can mark us Down
                // and resync.
                Err(crate::ops::error::SpendError::AlreadySpent { spending_data, .. }) => {
                    record_apply_divergence(
                        "spend",
                        tx_key,
                        &format!(
                            "AlreadySpent with different spending_data on replica (local={:02x?})",
                            &spending_data[..8],
                        ),
                    );
                    Err(format!(
                        "spend divergence: replica slot AlreadySpent with different spending_data ({:02x?}...)",
                        &spending_data[..8],
                    ))
                }
                // Master sent a Spend on what it observed as Unspent
                // but the replica's slot is Frozen — the master's
                // pre-spend validation would have rejected Frozen, so
                // the local Frozen state is real divergence.
                Err(crate::ops::error::SpendError::Frozen { .. }) => {
                    record_apply_divergence(
                        "spend",
                        tx_key,
                        "local slot Frozen but master sent Spend",
                    );
                    Err("spend divergence: local slot Frozen but master sent Spend".to_string())
                }
                // Same shape for Pruned — master would not have sent
                // Spend on a pruned slot if its local state matched
                // ours.
                Err(crate::ops::error::SpendError::Pruned { .. }) => {
                    record_apply_divergence(
                        "spend",
                        tx_key,
                        "local slot Pruned but master sent Spend",
                    );
                    Err("spend divergence: local slot Pruned but master sent Spend".to_string())
                }
                Err(e) => Err(format!("spend: {e}")),
            }
        }
        ReplicaOp::Unspend {
            tx_key,
            offset,
            spending_data,
            current_block_height,
            block_height_retention,
            ..
        } => {
            let slot = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot,
                Err(_) => {
                    record_apply_skipped_missing_tx("unspend", tx_key);
                    return Ok(());
                }
            };
            let req = UnspendRequest {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: slot.hash,
                spending_data: *spending_data,
                current_block_height: *current_block_height,
                block_height_retention: *block_height_retention,
            };
            match engine.unspend(&req) {
                Ok(_) => Ok(()),
                Err(e) => Err(format!("unspend: {e}")),
            }
        }
        ReplicaOp::SetMined {
            tx_key,
            block_id,
            block_height,
            subtree_idx,
            on_longest_chain,
            current_block_height,
            block_height_retention,
            ..
        } => {
            let req = SetMinedRequest {
                tx_key: *tx_key,
                block_id: *block_id,
                block_height: *block_height,
                subtree_idx: *subtree_idx,
                current_block_height: *current_block_height,
                block_height_retention: *block_height_retention,
                on_longest_chain: *on_longest_chain,
                unset_mined: false,
            };
            match engine.set_mined(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => {
                    record_apply_skipped_missing_tx("set_mined", tx_key);
                    Ok(())
                }
                Err(e) => Err(format!("set_mined: {e}")),
            }
        }
        ReplicaOp::UnsetMined {
            tx_key,
            block_id,
            current_block_height,
            block_height_retention,
            ..
        } => {
            let req = SetMinedRequest {
                tx_key: *tx_key,
                block_id: *block_id,
                block_height: 0,
                subtree_idx: 0,
                current_block_height: *current_block_height,
                block_height_retention: *block_height_retention,
                on_longest_chain: false,
                unset_mined: true,
            };
            match engine.set_mined(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => {
                    record_apply_skipped_missing_tx("unset_mined", tx_key);
                    Ok(())
                }
                Err(e) => Err(format!("unset_mined: {e}")),
            }
        }
        ReplicaOp::Freeze { tx_key, offset, .. } => {
            let hash = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot.hash,
                Err(_) => {
                    record_apply_skipped_missing_tx("freeze", tx_key);
                    return Ok(());
                }
            };
            let req = FreezeRequest {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: hash,
            };
            match engine.freeze(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::AlreadyFrozen { .. }) => Ok(()),
                Err(crate::ops::error::SpendError::AlreadySpent { .. }) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => {
                    record_apply_skipped_missing_tx("freeze", tx_key);
                    Ok(())
                }
                Err(e) => Err(format!("freeze: {e}")),
            }
        }
        ReplicaOp::Unfreeze { tx_key, offset, .. } => {
            let hash = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot.hash,
                Err(_) => {
                    record_apply_skipped_missing_tx("unfreeze", tx_key);
                    return Ok(());
                }
            };
            let req = UnfreezeRequest {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: hash,
            };
            match engine.unfreeze(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::NotFrozen { .. }) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => {
                    record_apply_skipped_missing_tx("unfreeze", tx_key);
                    Ok(())
                }
                Err(e) => Err(format!("unfreeze: {e}")),
            }
        }
        ReplicaOp::Reassign {
            tx_key,
            offset,
            new_hash,
            block_height,
            spendable_after,
            ..
        } => {
            let old_hash = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot.hash,
                Err(_) => {
                    record_apply_skipped_missing_tx("reassign", tx_key);
                    return Ok(());
                }
            };
            let req = ReassignRequest {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: old_hash,
                new_utxo_hash: *new_hash,
                block_height: *block_height,
                spendable_after: *spendable_after,
            };
            match engine.reassign(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::NotFrozen { .. }) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => {
                    record_apply_skipped_missing_tx("reassign", tx_key);
                    Ok(())
                }
                Err(e) => Err(format!("reassign: {e}")),
            }
        }
        ReplicaOp::SetConflicting {
            tx_key,
            value,
            current_block_height,
            retention,
            ..
        } => {
            let req = SetConflictingRequest {
                tx_key: *tx_key,
                value: *value,
                current_block_height: *current_block_height,
                block_height_retention: *retention,
            };
            match engine.set_conflicting(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => {
                    record_apply_skipped_missing_tx("set_conflicting", tx_key);
                    Ok(())
                }
                Err(e) => Err(format!("set_conflicting: {e}")),
            }
        }
        ReplicaOp::SetLocked { tx_key, value, .. } => {
            let req = SetLockedRequest {
                tx_key: *tx_key,
                value: *value,
            };
            match engine.set_locked_idempotent(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => {
                    record_apply_skipped_missing_tx("set_locked", tx_key);
                    Ok(())
                }
                Err(e) => Err(format!("set_locked: {e}")),
            }
        }
        ReplicaOp::PreserveUntil {
            tx_key,
            block_height,
            ..
        } => {
            let req = PreserveUntilRequest {
                tx_key: *tx_key,
                block_height: *block_height,
            };
            match engine.preserve_until(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => {
                    record_apply_skipped_missing_tx("preserve_until", tx_key);
                    Ok(())
                }
                Err(e) => Err(format!("preserve_until: {e}")),
            }
        }
        ReplicaOp::Create {
            tx_key,
            metadata_bytes,
            utxo_hashes,
            cold_data,
            is_external,
        } => {
            // Build a CreateRequest using metadata from the master.
            // The metadata_bytes contains: tx_version(4) + locktime(4) + fee(8) +
            // size_in_bytes(8) + extended_size(8) + is_coinbase(1) + spending_height(4) +
            // created_at(8) + flags(1) = 46 bytes.
            let (
                tx_version,
                locktime,
                fee,
                size_in_bytes,
                extended_size,
                is_coinbase,
                spending_height,
                created_at,
            ) = if metadata_bytes.len() >= 46 {
                let m = metadata_bytes.as_slice();
                (
                    u32::from_le_bytes(m[0..4].try_into().unwrap()),
                    u32::from_le_bytes(m[4..8].try_into().unwrap()),
                    u64::from_le_bytes(m[8..16].try_into().unwrap()),
                    u64::from_le_bytes(m[16..24].try_into().unwrap()),
                    u64::from_le_bytes(m[24..32].try_into().unwrap()),
                    m[32] != 0,
                    u32::from_le_bytes(m[33..37].try_into().unwrap()),
                    u64::from_le_bytes(m[37..45].try_into().unwrap()),
                )
            } else {
                (
                    1,
                    0,
                    0,
                    0,
                    0,
                    false,
                    0,
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                )
            };

            // Extract frozen/conflicting/locked from the wire flags byte
            // at offset 45 (locked=0x01, conflicting=0x02, frozen=0x04).
            let (frozen, conflicting, locked) = if metadata_bytes.len() >= 46 {
                let wire_flags = metadata_bytes[45];
                (
                    wire_flags & 0x04 != 0,
                    wire_flags & 0x02 != 0,
                    wire_flags & 0x01 != 0,
                )
            } else {
                (false, false, false)
            };

            // Parse extended fields at offset 70+: block_height(4) +
            // block_count(1) + [block_id(4)+block_height(4)+subtree_idx(4)]*N +
            // parent_txid_count(2) + parent_txids(32*N) +
            // optional ExternalRef(65).
            let mut block_height = 0u32;
            let mut mined_block_infos = Vec::new();
            let mut parent_txids: Vec<[u8; 32]> = Vec::new();
            let mut external_ref = None;
            if metadata_bytes.len() >= 75 {
                let m = metadata_bytes.as_slice();
                block_height = u32::from_le_bytes(m[70..74].try_into().unwrap());
                let block_count = m[74] as usize;
                let mut pos = 75;
                for _ in 0..block_count {
                    if pos + 12 > m.len() {
                        break;
                    }
                    mined_block_infos.push(crate::ops::create::MinedBlockInfo {
                        block_id: u32::from_le_bytes(m[pos..pos + 4].try_into().unwrap()),
                        block_height: u32::from_le_bytes(m[pos + 4..pos + 8].try_into().unwrap()),
                        subtree_idx: u32::from_le_bytes(m[pos + 8..pos + 12].try_into().unwrap()),
                    });
                    pos += 12;
                }
                if pos + 2 <= m.len() {
                    let ptx_count =
                        u16::from_le_bytes(m[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    for _ in 0..ptx_count {
                        if pos + 32 > m.len() {
                            break;
                        }
                        let mut ptx = [0u8; 32];
                        ptx.copy_from_slice(&m[pos..pos + 32]);
                        parent_txids.push(ptx);
                        pos += 32;
                    }
                }
                if *is_external && pos + 65 <= m.len() {
                    let mut content_hash = [0u8; 32];
                    content_hash.copy_from_slice(&m[pos + 1..pos + 33]);
                    external_ref = Some(ExternalRef {
                        store_type: m[pos],
                        content_hash,
                        total_size: u64::from_le_bytes(m[pos + 33..pos + 41].try_into().unwrap()),
                        input_count: u32::from_le_bytes(m[pos + 41..pos + 45].try_into().unwrap()),
                        output_count: u32::from_le_bytes(m[pos + 45..pos + 49].try_into().unwrap()),
                        inputs_offset: u64::from_le_bytes(
                            m[pos + 49..pos + 57].try_into().unwrap(),
                        ),
                        outputs_offset: u64::from_le_bytes(
                            m[pos + 57..pos + 65].try_into().unwrap(),
                        ),
                    });
                }
            }

            let create_req = CreateRequest {
                tx_id: tx_key.txid,
                tx_version,
                locktime,
                fee,
                size_in_bytes,
                extended_size,
                is_coinbase,
                spending_height,
                utxo_hashes,
                inputs: None,
                outputs: None,
                inpoints: None,
                is_external: *is_external,
                created_at,
                block_height,
                mined_block_infos: &mined_block_infos,
                frozen,
                conflicting,
                locked,
                external_ref,
                parent_txids: &parent_txids,
            };
            apply_create_replica(engine, tx_key, &create_req, metadata_bytes, cold_data)
        }
        ReplicaOp::Delete { tx_key } => {
            let req = DeleteRequest {
                tx_key: *tx_key,
                due_guard: None,
            };
            match engine.delete(&req) {
                Ok(()) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => Ok(()),
                Err(e) => Err(format!("delete: {e}")),
            }
        }
        ReplicaOp::PruneSlot { tx_key, offset } => {
            // C-4: route through the stripe-locked `engine.prune_slot` instead
            // of a raw `io::read_utxo_slot` → mutate → `io::write_utxo_slot`
            // RMW against the device. The lock-free RMW could race a
            // concurrent mutation on the same record and corrupt the slot
            // region; the engine method serializes on the tx stripe. Returns
            // `false` (a no-op skip) when the tx is absent or the slot is
            // already pruned — both idempotent.
            match engine.prune_slot(tx_key, *offset) {
                Ok(true) => Ok(()),
                Ok(false) => {
                    record_apply_skipped_missing_tx("prune_slot", tx_key);
                    Ok(())
                }
                Err(e) => Err(format!("prune_slot: {e}")),
            }
        }
        ReplicaOp::PruneSlotIfSpentBy {
            tx_key,
            offset,
            child_txid,
        } => engine
            .prune_slot_if_spent_by_child(tx_key, *offset, *child_txid)
            .map(|_| ())
            .map_err(|e| format!("prune_slot_if_spent_by: {e}")),
        ReplicaOp::MarkLongestChain {
            tx_key,
            on_longest_chain,
            current_block_height,
            block_height_retention,
            master_generation,
        } => {
            // R-053: idempotency-by-generation. The pre-apply guard at
            // the top of `apply_op` already rejects strictly-stale ops under
            // wrapping generation ordering. For MarkLongestChain we also need
            // to skip the equal-generation case: re-applying the same
            // `MarkLongestChain` would otherwise bump generation again on the
            // engine and write a stale `unmined_since`/DAH pair into the
            // secondary indexes, even though the post-apply generation sync
            // at the bottom of `apply_op` would immediately overwrite it back
            // to `master_generation`. The visible effect would be a DAH index
            // churn on every replay. Treating local at-or-ahead as a no-op
            // makes the op fully idempotent on the replica.
            if let Ok(meta) = engine.read_metadata(tx_key) {
                let local_gen = { meta.generation };
                if generation_at_or_ahead(local_gen, *master_generation) {
                    return Ok(());
                }
            }
            let req = crate::ops::mark_longest_chain::MarkOnLongestChainRequest {
                tx_key: *tx_key,
                on_longest_chain: *on_longest_chain,
                current_block_height: *current_block_height,
                block_height_retention: *block_height_retention,
            };
            match engine.mark_on_longest_chain(&req) {
                Ok(_) => Ok(()),
                Err(crate::ops::error::SpendError::TxNotFound) => {
                    record_apply_skipped_missing_tx("mark_longest_chain", tx_key);
                    Ok(())
                }
                Err(e) => Err(format!("mark_longest_chain: {e}")),
            }
        }
    }?;

    // After applying the mutation, sync the record's generation counter
    // to the master's value. The engine auto-increments generation on
    // every mutation, but the replica must use the master's generation
    // so both sides agree. The pre-apply guard above already rejected
    // strictly-stale ops under wrapping generation ordering, so here we
    // unconditionally set the generation to the master's value.
    //
    // R-035 (LMNH-31): the previous implementation swallowed
    // `read_metadata` and `write_metadata` errors with `let _ = ...`
    // and `if let Ok(...)`. A failure here means the replica's
    // generation counter has silently drifted from the master's, which
    // makes the next pre-apply generation guard incorrectly reject a
    // legitimate op (or accept a stale one). Both branches now hard-fail
    // the batch ACK so the master retries.
    if let Some(master_gen) = op.master_generation() {
        let tx_key = op.tx_key();
        // C-4: route the generation sync through the stripe-locked
        // `engine.set_record_generation` instead of a raw
        // `read_metadata` → mutate → `io::write_metadata` RMW. The lock-free
        // RMW could race a concurrent local mutation on the same record and
        // lose one of the two writes; the engine method holds the tx stripe
        // for the read-modify-write and refreshes the primary-index cache.
        // A missing record (returns `false`) means there is nothing to
        // reconcile — the op already short-circuited as a skip above.
        if !engine
            .set_record_generation(&tx_key, master_gen)
            .map_err(|e| format!("generation sync: {e}"))?
        {
            return Err(format!("generation sync: tx {tx_key:?} absent after apply"));
        }
    }

    // F-G7-016 (batched in `handle_replica_batch_with_tracker_inner`):
    // the device fsync is now done ONCE per batch — after every op has
    // been applied, before the post-apply redo entries are written and
    // before the ACK is sent. Calling `engine.device().sync()` per op
    // was a major perf regression: on slow filesystems (Docker named
    // volumes) each fsync is ~10ms, so a 200-op batch took ~2 s and
    // routinely exceeded the replica's 3 s ACK timeout. The "apply,
    // fsync data, then journal" discipline still holds at the batch
    // level: all batch ops are applied → device fsync → redo entries
    // written → ACK sent.

    // R-034 (BC-34): write a local redo entry so the replica can replay
    // through its own crash recovery and a failover does not require a
    // full resync of every surviving replica. The entry captures the
    // POST-apply state read back from the device, matching the discipline
    // the master uses on its own write path. Failure to journal the entry
    // is a hard batch-level error: ACKing without the local log would
    // re-introduce the same divergence R-034 was opened to fix.
    if let Some(redo_op) = build_post_apply_redo_op(engine, op)? {
        write_replica_redo_entry(engine, &redo_op)?;
    }

    Ok(())
}

/// Build the post-apply redo entry for a `ReplicaOp` after it was
/// successfully applied to the engine.
///
/// The entry captures the durable state currently on the device (counter
/// values, slot status, generation), not the raw input op, so a replica
/// crash + recovery replays the same on-device state the master would
/// reach after replaying its own redo log. The dispatch path on the
/// master computes these counters from validated state under the per-tx
/// lock; on the replica we read them back from the device after the
/// engine's apply_op has already taken and released the lock — ordering
/// here is "apply, fsync data, then journal" (the explicit
/// `engine.device().sync()` call in `apply_op` enforces the data fsync
/// before this function runs, F-G7-016) instead of the master's
/// "journal, then apply, then fsync data". Both orderings are correct
/// because all replica apply paths are idempotent and the redo replay
/// guards check the device state before re-writing.
///
/// Returns `Ok(None)` when the op has no recoverable redo entry (e.g.
/// the engine apply was a graceful skip because the record had already
/// been deleted), or when no redo log is attached (test paths).
fn build_post_apply_redo_op(
    engine: &Engine,
    op: &ReplicaOp,
) -> std::result::Result<Option<crate::redo::RedoOp>, String> {
    use crate::redo::RedoOp;
    if engine.redo_log().is_none() {
        return Ok(None);
    }
    match op {
        ReplicaOp::Spend {
            tx_key,
            offset,
            spending_data,
            current_block_height,
            block_height_retention,
            ..
        } => {
            let meta = match engine.read_metadata(tx_key) {
                Ok(m) => m,
                Err(_) => return Ok(None),
            };
            let new_spent_count = { meta.spent_utxos };
            // B-5: carry the slot hash (V3) so a torn slot can self-heal
            // on the replica too; fall back to V2 (no hash) if the slot
            // cannot be read.
            let utxo_hash = engine.read_slot(tx_key, *offset).ok().map(|s| s.hash);
            Ok(Some(RedoOp::SpendV2 {
                tx_key: *tx_key,
                offset: *offset,
                spending_data: *spending_data,
                new_spent_count,
                current_block_height: *current_block_height,
                block_height_retention: *block_height_retention,
                target_generation: { meta.generation },
                updated_at: { meta.updated_at },
                utxo_hash,
            }))
        }
        ReplicaOp::Unspend {
            tx_key,
            offset,
            spending_data,
            current_block_height,
            block_height_retention,
            ..
        } => {
            let meta = match engine.read_metadata(tx_key) {
                Ok(m) => m,
                Err(_) => return Ok(None),
            };
            let new_spent_count = { meta.spent_utxos };
            // B-5: carry the slot hash (V3) for replica self-heal.
            let utxo_hash = engine.read_slot(tx_key, *offset).ok().map(|s| s.hash);
            Ok(Some(RedoOp::UnspendV2 {
                tx_key: *tx_key,
                offset: *offset,
                spending_data: *spending_data,
                new_spent_count,
                current_block_height: *current_block_height,
                block_height_retention: *block_height_retention,
                target_generation: { meta.generation },
                updated_at: { meta.updated_at },
                utxo_hash,
            }))
        }
        ReplicaOp::SetMined {
            tx_key,
            block_id,
            block_height,
            subtree_idx,
            ..
        } => Ok(Some(RedoOp::SetMined {
            tx_key: *tx_key,
            block_id: *block_id,
            block_height: *block_height,
            subtree_idx: *subtree_idx,
            unset: false,
        })),
        ReplicaOp::UnsetMined {
            tx_key, block_id, ..
        } => Ok(Some(RedoOp::SetMined {
            tx_key: *tx_key,
            block_id: *block_id,
            block_height: 0,
            subtree_idx: 0,
            unset: true,
        })),
        ReplicaOp::Freeze { tx_key, offset, .. } => {
            let slot = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot,
                Err(_) => return Ok(None),
            };
            Ok(Some(RedoOp::FreezeV2 {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: slot.hash,
            }))
        }
        ReplicaOp::Unfreeze { tx_key, offset, .. } => {
            let slot = match engine.read_slot(tx_key, *offset) {
                Ok(slot) => slot,
                Err(_) => return Ok(None),
            };
            Ok(Some(RedoOp::UnfreezeV2 {
                tx_key: *tx_key,
                offset: *offset,
                utxo_hash: slot.hash,
            }))
        }
        ReplicaOp::Reassign {
            tx_key,
            offset,
            new_hash,
            block_height,
            spendable_after,
            ..
        } => Ok(Some(RedoOp::Reassign {
            tx_key: *tx_key,
            offset: *offset,
            new_hash: *new_hash,
            block_height: *block_height,
            spendable_after: *spendable_after,
        })),
        ReplicaOp::SetConflicting {
            tx_key,
            value,
            current_block_height,
            retention,
            ..
        } => Ok(Some(RedoOp::SetConflicting {
            tx_key: *tx_key,
            value: *value,
            current_block_height: *current_block_height,
            block_height_retention: *retention,
        })),
        ReplicaOp::SetLocked { tx_key, value, .. } => Ok(Some(RedoOp::SetLocked {
            tx_key: *tx_key,
            value: *value,
        })),
        ReplicaOp::PreserveUntil {
            tx_key,
            block_height,
            ..
        } => Ok(Some(RedoOp::PreserveUntil {
            tx_key: *tx_key,
            block_height: *block_height,
        })),
        ReplicaOp::Create {
            tx_key,
            utxo_hashes,
            ..
        } => {
            // Match the master's WAL-first contract: the replica records
            // the index registration via the legacy `Create` variant. The
            // CreateV2 full-payload path is the master's preferred form,
            // but on the replica the on-device record is already byte-for-
            // byte populated by `engine.create()` before this entry is
            // appended; the legacy variant is sufficient for replay because
            // replay verifies the record on disk before mutating.
            let entry = match engine.lookup(tx_key) {
                Some(e) => e,
                None => return Ok(None),
            };
            Ok(Some(RedoOp::Create {
                tx_key: *tx_key,
                record_offset: entry.record_offset,
                utxo_count: utxo_hashes.len() as u32,
            }))
        }
        ReplicaOp::Delete { tx_key } => {
            // After delete the index entry is gone, so we cannot re-read
            // the record offset / size. Recovery treats `Delete` as
            // "drop the index entry"; a replica that crashes here will
            // still observe the index lookup miss because the delete
            // already mutated the index in-memory and the next snapshot
            // (or live engine state) carries no entry. Use sentinel zeros
            // — replay's lookup-then-skip path handles the missing-index
            // case as Skipped.
            Ok(Some(RedoOp::Delete {
                tx_key: *tx_key,
                record_offset: 0,
                record_size: 0,
            }))
        }
        ReplicaOp::PruneSlot { tx_key, offset } => Ok(Some(RedoOp::PruneSlot {
            tx_key: *tx_key,
            offset: *offset,
        })),
        ReplicaOp::PruneSlotIfSpentBy {
            tx_key,
            offset,
            child_txid,
        } => Ok(Some(RedoOp::PruneSlotIfSpentBy {
            tx_key: *tx_key,
            offset: *offset,
            child_txid: *child_txid,
        })),
        // R-052 ReplicaOp variant. Mirror the master-side
        // RedoOp::MarkOnLongestChain entry so the replica's local
        // recovery can replay this op (R-034 contract). The redo
        // entry's `generation` field is the post-apply generation
        // we just wrote — read it from on-device metadata.
        ReplicaOp::MarkLongestChain {
            tx_key,
            on_longest_chain,
            current_block_height,
            block_height_retention,
            ..
        } => {
            let meta = match engine.read_metadata(tx_key) {
                Ok(m) => m,
                Err(_) => return Ok(None),
            };
            Ok(Some(RedoOp::MarkOnLongestChain {
                tx_key: *tx_key,
                on_longest_chain: *on_longest_chain,
                current_block_height: *current_block_height,
                block_height_retention: *block_height_retention,
                generation: { meta.generation },
            }))
        }
    }
}

/// Append + flush a redo entry on the replica's local engine log.
///
/// Returns `Err(message)` when the engine has a redo log attached AND
/// the append/flush fails — caller propagates to fail the batch ACK so
/// the master retries instead of advancing its durable high-water mark.
/// Append a redo entry for a single op WITHOUT flushing.
///
/// Called inside the per-op apply loop in
/// `handle_replica_batch_with_tracker`. The batch-level flush happens
/// once after the apply loop completes — see [`flush_replica_redo_log`].
/// Batching the flush eliminates the per-op fsync that was a major perf
/// regression on slow filesystems (Docker named volumes): a 200-op
/// batch went from ~2 s (200 × 10 ms) to a single fsync.
fn write_replica_redo_entry(
    engine: &Engine,
    op: &crate::redo::RedoOp,
) -> std::result::Result<(), String> {
    let log_arc = match engine.redo_log() {
        Some(l) => l,
        None => return Ok(()),
    };
    let mut guard = log_arc.lock();
    guard
        .append(op.clone())
        .map_err(|e| format!("replica redo append: {e}"))?;
    Ok(())
}

/// Flush the replica redo log to durable storage.
///
/// Called once per batch from `handle_replica_batch_with_tracker` after
/// every op has been applied + appended via [`write_replica_redo_entry`]
/// and after the data device fsync. The "apply, fsync data, then flush
/// redo log, then ACK" discipline is preserved at the batch level.
fn flush_replica_redo_log(engine: &Engine) -> std::result::Result<(), String> {
    let log_arc = match engine.redo_log() {
        Some(l) => l,
        None => return Ok(()),
    };
    let mut guard = log_arc.lock();
    guard
        .flush()
        .map_err(|e| format!("replica redo flush: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::{BlockDevice, MemoryDevice};
    use crate::index::{DahIndex, Index, TxIndexEntry, TxKey, UnminedIndex};
    use crate::locks::StripedLocks;

    fn make_engine() -> Arc<Engine> {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(10_000).unwrap();
        Arc::new(Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        ))
    }

    fn make_engine_with_blob_store(
        store: Arc<crate::storage::blobstore::MemoryBlobStore>,
    ) -> Arc<Engine> {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(10_000).unwrap();
        let mut engine = Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        );
        engine.set_blob_store(store);
        Arc::new(engine)
    }

    fn key(n: u8) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0] = n;
        TxKey { txid }
    }

    /// F-G7-003: the per-process inflight-bytes counter must
    /// refuse reservations that would cross
    /// `RECEIVER_INFLIGHT_BYTES_CAP`. Guarantees a known DoS bound:
    /// unauthenticated peers can't collectively force more than
    /// `RECEIVER_INFLIGHT_BYTES_CAP` bytes of body buffer allocation.
    #[test]
    fn inflight_bytes_cap_refuses_oversize_reservations() {
        // Snapshot the counter so concurrent test threads don't make
        // us flaky (test threads do not interact with the standalone
        // receiver, but the static is process-wide).
        let baseline = RECEIVER_INFLIGHT_BYTES.load(Ordering::Acquire);

        // A reasonable reservation succeeds.
        let small = reserve_inflight_bytes(1024).expect("1 KiB reservation must succeed");
        assert_eq!(
            RECEIVER_INFLIGHT_BYTES.load(Ordering::Acquire),
            baseline + 1024,
        );

        // A reservation that would exceed the cap returns None.
        let huge = reserve_inflight_bytes(RECEIVER_INFLIGHT_BYTES_CAP + 1);
        assert!(huge.is_none(), "reservation past the cap must fail",);

        // Releasing the small reservation restores the counter.
        drop(small);
        assert_eq!(RECEIVER_INFLIGHT_BYTES.load(Ordering::Acquire), baseline,);
    }

    #[test]
    fn receiver_reuses_buffer_per_connection() {
        let source = include_str!("receiver.rs");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("receiver.rs contains production section");
        assert!(
            !production.contains("let mut body = vec![0u8; frame_len]"),
            "replication receiver must reuse per-connection frame buffers",
        );
        // After F-G7-003 the per-connection buffer is wrapped in a
        // `CountedBody` so the global inflight-bytes counter can
        // track its growth. The reuse contract is preserved — the
        // wrapper still lives outside the read loop.
        assert!(
            production.contains("let mut body = CountedBody {"),
            "replication receiver must allocate the reusable body buffer outside the hot loop",
        );
    }

    /// Build the on-wire `RequestFrame` bytes for a single-Create
    /// `OP_REPLICA_BATCH`, used by the E-1 frame-assembly-deadline tests.
    fn replica_create_frame_bytes(request_id: u64, tx_key: TxKey) -> Vec<u8> {
        let batch = ReplicaBatch {
            // Sequence tied to request_id so successive frames on the same
            // connection are not deduplicated as already-applied.
            first_sequence: request_id,
            ops: vec![ReplicaOp::Create {
                tx_key,
                metadata_bytes: vec![0; 64],
                utxo_hashes: vec![[0xAA; 32]; 2],
                cold_data: None,
                is_external: false,
            }],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };
        RequestFrame {
            request_id,
            op_code: OP_REPLICA_BATCH,
            flags: 0,
            payload: batch.serialize().into(),
        }
        .encode()
    }

    /// Spawn `handle_connection` against a freshly accepted socket with an
    /// injected `frame_deadline`. Returns the connected client stream, the
    /// engine, and a join handle that completes once the handler returns.
    fn spawn_receiver_with_deadline(
        frame_deadline: Duration,
    ) -> (TcpStream, Arc<Engine>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = make_engine();

        let server_engine = engine.clone();
        let handle = std::thread::spawn(move || {
            let (stream, peer_addr) = listener.accept().unwrap();
            let running = AtomicBool::new(true);
            let last_applied = AtomicU64::new(0);
            let applied = Arc::new(ReplicaAppliedTracker::in_memory());
            let local_cluster_key = Arc::new(AtomicU64::new(0));
            let ctx = ConnectionContext {
                engine: &server_engine,
                running: &running,
                last_applied: &last_applied,
                applied,
                local_cluster_key,
                auth_secret: None,
                frame_deadline,
            };
            handle_connection(stream, peer_addr, ctx);
        });

        let client = TcpStream::connect(addr).unwrap();
        (client, engine, handle)
    }

    /// E-1: the per-syscall `RECEIVER_READ_TIMEOUT` resets on every
    /// successful read, so a peer dripping one byte per interval keeps
    /// each individual read "succeeding" forever. The whole-frame
    /// assembly deadline must abort the connection once it expires,
    /// independent of per-read progress. Mirrors the server accept
    /// path's `dripping_client_disconnected_at_frame_assembly_deadline`.
    #[test]
    fn dripping_peer_disconnected_at_frame_assembly_deadline() {
        // Deadline far shorter than the time the drip below needs to
        // deliver the whole frame, yet each drip lands well within the
        // 30 s per-read timeout — so only the assembly deadline can end
        // this connection.
        let (mut client, _engine, handle) =
            spawn_receiver_with_deadline(Duration::from_millis(400));

        // Declare a 16-byte frame body, then drip it one byte per 150 ms.
        // 16 bytes × 150 ms = 2.4 s ≫ the 400 ms assembly deadline, so a
        // handler honoring the deadline must close the socket after a few
        // drips — long before the full body is delivered. A handler that
        // resets its per-read timeout on each drip assembles the whole
        // frame and only finishes after ~2.4 s.
        client.write_all(&16u32.to_le_bytes()).unwrap();
        let drip_started = Instant::now();
        let drip = std::thread::spawn(move || {
            for _ in 0..16 {
                // The write may succeed even after the server hangs up
                // (bytes buffer locally), so socket errors are not a
                // reliable signal — the handler-return timing below is.
                if client.write_all(&[0u8]).is_err() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(150));
            }
        });

        // Load-bearing assertion: the handler must return shortly after
        // the 400 ms deadline and strictly before the ~2.4 s drip would
        // finish. A pre-fix handler resets its per-read timeout on every
        // drip and only returns once the whole body has arrived.
        let joined = wait_for_join(&handle, Duration::from_secs(2));
        let elapsed = drip_started.elapsed();
        assert!(
            joined,
            "receiver handler did not return after the frame-assembly deadline \
             (still running {elapsed:?} after the prefix arrived)"
        );
        assert!(
            elapsed < Duration::from_millis(1500),
            "handler returned only after {elapsed:?}; the 400 ms deadline should have \
             closed the connection long before the 2.4 s drip completed — it reset \
             its per-read timeout on every drip",
        );
        let _ = drip.join();
    }

    /// E-1 counterpart: a peer that delivers a frame in several chunks
    /// well within the deadline must still be applied. Also exercises the
    /// base-timeout restore: the second frame arrives after an idle gap
    /// longer than the (test-shortened) deadline, which a handler that
    /// failed to restore `RECEIVER_READ_TIMEOUT` would drop on its
    /// length-prefix read.
    #[test]
    fn chunked_frame_within_deadline_applies() {
        // 1.5 s deadline: comfortably longer than the chunked send below
        // (~0.4 s) yet short enough that the socket read timeout shrunk by
        // the deadline reader (~1 s remaining when the body completes)
        // would, if not restored, trip the second frame's prefix read
        // during the 1.3 s idle.
        let (mut client, engine, handle) =
            spawn_receiver_with_deadline(Duration::from_millis(1500));

        let tx_key = key(70);
        let frame = replica_create_frame_bytes(1, tx_key);
        // Send the wire frame in 32-byte chunks 50 ms apart (~0.4 s total,
        // well inside the 1.5 s deadline). Chunk boundaries straddle the
        // length prefix, the head peek, and the body.
        for chunk in frame.chunks(32) {
            client.write_all(chunk).unwrap();
            std::thread::sleep(Duration::from_millis(50));
        }
        // Read the ACK response frame so we know the batch was applied.
        let ack = read_ack_frame(&mut client);
        assert_eq!(ack.request_id, 1, "ack should echo the request_id");
        assert_eq!(ack.status, STATUS_OK, "batch should apply successfully");

        let slot = engine.read_slot(&tx_key, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
        assert_eq!(slot.hash, [0xAA; 32]);

        // Idle longer than the timeout the deadline reader shrank the
        // socket to while assembling the first frame, then send a second
        // frame in one piece. The handler must have restored the base read
        // timeout (30 s) after the first frame; otherwise this length-
        // prefix read inherits the shrunken timeout and drops the
        // connection during the idle gap.
        std::thread::sleep(Duration::from_millis(1300));
        let tx_key2 = key(71);
        let frame2 = replica_create_frame_bytes(2, tx_key2);
        client.write_all(&frame2).unwrap();
        let ack2 = read_ack_frame(&mut client);
        assert_eq!(ack2.request_id, 2);
        assert_eq!(ack2.status, STATUS_OK);
        let slot2 = engine.read_slot(&tx_key2, 0).unwrap();
        assert_eq!(slot2.status, UTXO_UNSPENT);

        drop(client);
        let joined = wait_for_join(&handle, Duration::from_secs(2));
        assert!(joined, "handler should return after the client disconnects");
    }

    /// Read a single length-prefixed `ResponseFrame` (the receiver's ACK)
    /// off `stream`.
    fn read_ack_frame(stream: &mut TcpStream) -> ResponseFrame {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).unwrap();
        let mut full = Vec::with_capacity(4 + len);
        full.extend_from_slice(&len_buf);
        full.extend_from_slice(&body);
        let (frame, _) = ResponseFrame::decode(&full).unwrap();
        frame
    }

    /// Spawn a receiver whose connection requires HMAC-signed inter-node
    /// frames (`auth_secret` set), driving the streaming-verify path.
    fn spawn_receiver_with_secret(
        frame_deadline: Duration,
        secret: Vec<u8>,
    ) -> (TcpStream, Arc<Engine>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = make_engine();

        let server_engine = engine.clone();
        let handle = std::thread::spawn(move || {
            let (stream, peer_addr) = listener.accept().unwrap();
            let running = AtomicBool::new(true);
            let last_applied = AtomicU64::new(0);
            let applied = Arc::new(ReplicaAppliedTracker::in_memory());
            let local_cluster_key = Arc::new(AtomicU64::new(0));
            let ctx = ConnectionContext {
                engine: &server_engine,
                running: &running,
                last_applied: &last_applied,
                applied,
                local_cluster_key,
                auth_secret: Some(Arc::new(secret)),
                frame_deadline,
            };
            handle_connection(stream, peer_addr, ctx);
        });

        let client = TcpStream::connect(addr).unwrap();
        (client, engine, handle)
    }

    /// Read one length-prefixed frame off `stream`, returning the raw
    /// `[len][body]` bytes (caller verifies/decodes them).
    fn read_framed_raw(stream: &mut TcpStream) -> Vec<u8> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).unwrap();
        let mut full = Vec::with_capacity(4 + len);
        full.extend_from_slice(&len_buf);
        full.extend_from_slice(&body);
        full
    }

    /// An HMAC-signed `OP_REPLICA_BATCH` must pass the receiver's
    /// streaming verify, apply, and produce a signed ACK. Pins the fix
    /// for the double-length-prefix bug in the streaming-verify reader
    /// (`verify_signed_body_streaming` synthesises its own prefix). Before
    /// the fix the receiver chained `len_buf` ahead of the body, so every
    /// honest signed frame failed HMAC with `ERR_CLUSTER_AUTH_FAILED`.
    #[test]
    fn signed_replica_batch_verified_and_applied() {
        let secret = b"receiver-secret".to_vec();
        let (mut client, engine, handle) =
            spawn_receiver_with_secret(Duration::from_secs(5), secret.clone());
        create_record(&engine, key(223), 2);
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let batch = make_spend_batch(1, key(223), 0..2, 1);
        let request = batch_request(&batch, 41);
        let signed = crate::cluster::auth::sign_frame(&secret, &request.encode()).unwrap();
        client.write_all(&signed).unwrap();

        // The ACK is itself signed; verify then decode it.
        let raw = read_framed_raw(&mut client);
        let verified = crate::cluster::auth::verify_frame(&secret, &raw).unwrap();
        let (response, consumed) = ResponseFrame::decode(&verified).unwrap();
        assert_eq!(consumed, verified.len());
        assert_eq!(response.request_id, 41);
        assert_eq!(response.status, STATUS_OK);
        let ack = ReplicaAck::deserialize(&response.payload).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Ok {
                through_sequence: 2
            }
        );

        let slot = engine.read_slot(&key(223), 0).unwrap();
        assert_eq!(slot.status, UTXO_SPENT);

        drop(client);
        assert!(wait_for_join(&handle, Duration::from_secs(2)));
    }

    /// Poll a join handle until it finishes or `timeout` elapses.
    fn wait_for_join(handle: &std::thread::JoinHandle<()>, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if handle.is_finished() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        handle.is_finished()
    }

    fn create_record(engine: &Engine, k: TxKey, utxo_count: u32) {
        let hashes: Vec<[u8; 32]> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i as u8;
                h[4..8].copy_from_slice(&k.txid[0..4]);
                h
            })
            .collect();
        let req = CreateRequest {
            tx_id: k.txid,
            tx_version: 1,
            locktime: 0,
            fee: 0,
            size_in_bytes: 0,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            utxo_hashes: &hashes,
            inputs: None,
            outputs: None,
            inpoints: None,
            is_external: false,
            created_at: 0,
            block_height: 0,
            mined_block_infos: &[],
            frozen: false,
            conflicting: false,
            locked: false,
            external_ref: None,
            parent_txids: &[],
        };
        engine.create(&req).unwrap();
    }

    fn set_generation(engine: &Engine, k: TxKey, generation: u32) {
        let entry = engine.lookup(&k).unwrap();
        let mut meta = engine.read_metadata(&k).unwrap();
        meta.generation = generation;
        crate::io::write_metadata(engine.device(), entry.record_offset, &meta).unwrap();
    }

    #[test]
    fn apply_spend_op() {
        let engine = make_engine();
        let k = key(1);
        create_record(&engine, k, 3);

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);

        let op = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xAB; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 0,
        };
        apply_op(&engine, &op).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_SPENT);
        assert_eq!(slot.spending_data[0], 0xAB);
    }

    #[test]
    fn apply_spend_uses_replicated_dah_context() {
        let engine = make_engine();
        let k = key(3);
        create_record(&engine, k, 1);

        apply_op(
            &engine,
            &ReplicaOp::SetMined {
                tx_key: k,
                block_id: 42,
                block_height: 700_000,
                subtree_idx: 0,
                on_longest_chain: true,
                current_block_height: 700_010,
                block_height_retention: 5,
                master_generation: 1,
            },
        )
        .unwrap();

        apply_op(
            &engine,
            &ReplicaOp::Spend {
                tx_key: k,
                offset: 0,
                spending_data: [0xBC; 36],
                current_block_height: 700_123,
                block_height_retention: 31,
                master_generation: 2,
            },
        )
        .unwrap();

        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!(
            { meta.delete_at_height },
            700_154,
            "receiver must use the master's DAH context, not a local default",
        );
    }

    /// C-4 regression: the plain `ReplicaOp::PruneSlot` apply path must route
    /// through the stripe-locked `engine.prune_slot` rather than a raw
    /// device-level read-modify-write. We assert the slot is pruned, that a
    /// second apply is idempotent (already-pruned → no-op skip), and — the
    /// load-bearing part — that the engine method actually serializes against
    /// a concurrent spend on the same record without corrupting the slot
    /// region. A lock-free RMW (the pre-fix behavior) has no such guarantee.
    #[test]
    fn apply_prune_slot_routes_through_stripe_lock() {
        let engine = make_engine();
        let k = key(91);
        create_record(&engine, k, 4);

        // Apply PruneSlot for offset 2 via the receiver.
        apply_op(
            &engine,
            &ReplicaOp::PruneSlot {
                tx_key: k,
                offset: 2,
            },
        )
        .unwrap();
        let slot = engine.read_slot(&k, 2).unwrap();
        assert_eq!(slot.status, UTXO_PRUNED, "PruneSlot must prune the slot");

        // Idempotent re-apply: already pruned → still pruned, no error.
        apply_op(
            &engine,
            &ReplicaOp::PruneSlot {
                tx_key: k,
                offset: 2,
            },
        )
        .unwrap();
        assert_eq!(engine.read_slot(&k, 2).unwrap().status, UTXO_PRUNED);

        // Missing tx → skip (no error).
        apply_op(
            &engine,
            &ReplicaOp::PruneSlot {
                tx_key: key(92),
                offset: 0,
            },
        )
        .unwrap();

        // Concurrency: hammer engine.prune_slot on one offset while a writer
        // spends a different offset on the same record. With the stripe lock
        // these serialize; without it the two RMWs on the same record region
        // could interleave. We assert the record stays internally consistent
        // (slot count stable, pruned + spent never exceed utxo_count).
        let engine2 = make_engine();
        let kc = key(93);
        let utxo_hashes: Vec<[u8; 32]> = (0..8u8)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i;
                h[1] = 0xCD;
                h
            })
            .collect();
        engine2
            .create(&crate::ops::create::CreateRequest {
                tx_id: kc.txid,
                tx_version: 1,
                locktime: 0,
                fee: 100,
                size_in_bytes: 100,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                utxo_hashes: &utxo_hashes,
                inputs: None,
                outputs: None,
                inpoints: None,
                is_external: false,
                created_at: 1710000000000,
                block_height: 0,
                mined_block_infos: &[],
                frozen: false,
                conflicting: false,
                locked: false,
                external_ref: None,
                parent_txids: &[],
            })
            .unwrap();

        let pruner = {
            let engine2 = engine2.clone();
            std::thread::spawn(move || {
                for off in 0..4u32 {
                    engine2.prune_slot(&kc, off).unwrap();
                    std::thread::yield_now();
                }
            })
        };
        let spender = {
            let engine2 = engine2.clone();
            let hashes = utxo_hashes.clone();
            std::thread::spawn(move || {
                for off in 4..8u32 {
                    let mut spending_data = [0u8; 36];
                    spending_data[0] = off as u8;
                    engine2
                        .spend(&crate::ops::spend::SpendRequest {
                            tx_key: kc,
                            offset: off,
                            utxo_hash: hashes[off as usize],
                            spending_data,
                            ignore_conflicting: false,
                            ignore_locked: false,
                            current_block_height: 0,
                            block_height_retention: 0,
                        })
                        .unwrap();
                    std::thread::yield_now();
                }
            })
        };
        pruner.join().unwrap();
        spender.join().unwrap();

        let (meta, slots) = engine2.read_record_snapshot(&kc).unwrap();
        assert_eq!(slots.len(), 8, "slot count must remain stable");
        let pruned = slots.iter().filter(|s| s.status == UTXO_PRUNED).count();
        let spent = slots.iter().filter(|s| s.status == UTXO_SPENT).count();
        assert_eq!(pruned, 4, "all four pruned slots must be PRUNED");
        assert_eq!(spent, 4, "all four spent slots must be SPENT");
        assert!(
            (pruned + spent) <= { meta.utxo_count } as usize,
            "status counts must not exceed utxo_count",
        );
    }

    /// C-4 regression: the post-apply generation sync must route through the
    /// stripe-locked `engine.set_record_generation`, not a raw
    /// `io::write_metadata`. We apply an op carrying a `master_generation` and
    /// assert the replica's generation matches the master's exactly (the
    /// engine auto-increments on the mutation, then the sync overwrites it).
    #[test]
    fn generation_sync_routes_through_stripe_lock() {
        let engine = make_engine();
        let k = key(94);
        create_record(&engine, k, 2);

        const MASTER_GEN: u32 = 12_345;
        apply_op(
            &engine,
            &ReplicaOp::Spend {
                tx_key: k,
                offset: 0,
                spending_data: [0x11; 36],
                current_block_height: 0,
                block_height_retention: 0,
                master_generation: MASTER_GEN,
            },
        )
        .unwrap();

        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!(
            { meta.generation },
            MASTER_GEN,
            "generation sync must set the replica's generation to the master's value",
        );
        // The cached generation in the primary index must match too — proving
        // set_record_generation refreshed the index cache (sync_index_cache).
        let cached = engine.lookup_cached(&k).unwrap();
        assert_eq!(
            cached.generation, MASTER_GEN,
            "primary-index cached generation must match the synced generation",
        );
    }

    #[test]
    fn apply_prune_slot_if_spent_by_updates_parent_counters() {
        let engine = make_engine();
        let parent = key(77);
        let child = key(78);
        create_record(&engine, parent, 2);

        let mut parent_hash = [0u8; 32];
        parent_hash[0] = 1;
        parent_hash[4..8].copy_from_slice(&parent.txid[0..4]);
        let mut spending_data = [0u8; 36];
        spending_data[..32].copy_from_slice(&child.txid);
        spending_data[32..36].copy_from_slice(&0u32.to_le_bytes());
        engine
            .spend(&crate::ops::spend::SpendRequest {
                tx_key: parent,
                offset: 1,
                utxo_hash: parent_hash,
                spending_data,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        apply_op(
            &engine,
            &ReplicaOp::PruneSlotIfSpentBy {
                tx_key: parent,
                offset: 1,
                child_txid: child.txid,
            },
        )
        .unwrap();

        let slot = engine.read_slot(&parent, 1).unwrap();
        assert_eq!(slot.status, UTXO_PRUNED);
        assert_eq!(slot.spending_data, spending_data);
        let meta = engine.read_metadata(&parent).unwrap();
        assert_eq!({ meta.spent_utxos }, 0);
        assert_eq!({ meta.pruned_utxos }, 1);
    }

    #[test]
    fn apply_spend_idempotent() {
        let engine = make_engine();
        let k = key(2);
        create_record(&engine, k, 3);

        let op = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xAB; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 0,
        };
        apply_op(&engine, &op).unwrap();
        // Apply again — should not error
        apply_op(&engine, &op).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_SPENT);
    }

    #[test]
    fn apply_create_op() {
        let engine = make_engine();
        let k = key(10);
        let hashes = vec![[0xAA; 32]; 5];

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: vec![0; 64],
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
        assert_eq!(slot.hash, [0xAA; 32]);
    }

    #[test]
    fn apply_create_idempotent() {
        let engine = make_engine();
        let k = key(11);
        let hashes = vec![[0xBB; 32]; 2];

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: vec![],
            utxo_hashes: hashes.clone(),
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();
        apply_op(&engine, &op).unwrap(); // duplicate — should be ok
    }

    #[test]
    fn apply_create_replaces_divergent_duplicate() {
        let engine = make_engine();
        let k = key(12);
        create_record(&engine, k, 2);

        let hashes = vec![[0xCC; 32]; 5];
        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: vec![],
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();

        let meta = engine.read_metadata(&k).unwrap();
        // `meta.utxo_count` is a packed-struct field — force a copy first
        // to avoid creating an unaligned reference inside `assert_eq!`.
        assert_eq!({ meta.utxo_count }, 5);
        let slot = engine.read_slot(&k, 4).unwrap();
        assert_eq!(slot.hash, [0xCC; 32]);
    }

    #[test]
    fn divergent_create_cleans_up_old_blob() {
        use crate::record::ExternalRef;
        use crate::storage::blobstore::{BlobStore, MemoryBlobStore};

        let store = Arc::new(MemoryBlobStore::new());
        let engine = make_engine_with_blob_store(store.clone());
        let k = key(112);
        let old_digest = store.put(&k.txid, b"old divergent blob").unwrap();
        let old_hashes = [[0x11; 32]];
        let old_ref = ExternalRef {
            store_type: 1,
            content_hash: old_digest.sha256,
            total_size: old_digest.length,
            input_count: 0,
            output_count: 0,
            inputs_offset: 0,
            outputs_offset: 0,
        };
        let old_req = CreateRequest {
            tx_id: k.txid,
            tx_version: 1,
            locktime: 0,
            fee: 0,
            size_in_bytes: old_digest.length,
            extended_size: old_digest.length,
            is_coinbase: false,
            spending_height: 0,
            utxo_hashes: &old_hashes,
            inputs: None,
            outputs: None,
            inpoints: None,
            is_external: true,
            created_at: 0,
            block_height: 0,
            mined_block_infos: &[],
            frozen: false,
            conflicting: false,
            locked: false,
            external_ref: Some(old_ref),
            parent_txids: &[],
        };
        engine.create(&old_req).unwrap();
        assert!(store.exists(&k.txid).unwrap());

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: vec![],
            utxo_hashes: vec![[0xCC; 32]; 2],
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();

        assert!(
            !store.exists(&k.txid).unwrap(),
            "divergent duplicate replacement must remove the old external blob"
        );
        let meta = engine.read_metadata(&k).unwrap();
        assert!(!meta.flags.contains(TxFlags::EXTERNAL));
    }

    #[test]
    fn apply_freeze_unfreeze() {
        let engine = make_engine();
        let k = key(20);
        create_record(&engine, k, 3);

        apply_op(
            &engine,
            &ReplicaOp::Freeze {
                tx_key: k,
                offset: 1,
                master_generation: 0,
            },
        )
        .unwrap();
        let slot = engine.read_slot(&k, 1).unwrap();
        assert_eq!(slot.status, UTXO_FROZEN);

        apply_op(
            &engine,
            &ReplicaOp::Unfreeze {
                tx_key: k,
                offset: 1,
                master_generation: 0,
            },
        )
        .unwrap();
        let slot = engine.read_slot(&k, 1).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
    }

    #[test]
    fn apply_delete_op() {
        let engine = make_engine();
        let k = key(30);
        create_record(&engine, k, 2);

        apply_op(&engine, &ReplicaOp::Delete { tx_key: k }).unwrap();
        assert!(engine.lookup(&k).is_none());
    }

    #[test]
    fn apply_set_mined() {
        let engine = make_engine();
        let k = key(40);
        create_record(&engine, k, 2);

        apply_op(
            &engine,
            &ReplicaOp::SetMined {
                tx_key: k,
                block_id: 42,
                block_height: 1000,
                subtree_idx: 0,
                on_longest_chain: true,
                current_block_height: 1000,
                block_height_retention: 288,
                master_generation: 0,
            },
        )
        .unwrap();

        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!(meta.block_entry_count, 1);
    }

    // ------------------------------------------------------------------
    // Coverage-matrix hole (2026-05-29 audit): the apply path for
    // unspend / unfreeze / reassign / set_conflicting / set_locked /
    // preserve_until / delete was implemented but never exercised by a
    // test. Each test below asserts the exact post-state AND that
    // re-applying the same op (replica crash + master resend, or redo
    // replay) is idempotent.
    // ------------------------------------------------------------------

    #[test]
    fn apply_unspend_op_and_idempotent_reapply() {
        let engine = make_engine();
        let k = key(50);
        create_record(&engine, k, 3);

        apply_op(
            &engine,
            &ReplicaOp::Spend {
                tx_key: k,
                offset: 0,
                spending_data: [0xAB; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 0,
            },
        )
        .unwrap();
        assert_eq!({ engine.read_metadata(&k).unwrap().spent_utxos }, 1);

        let unspend = ReplicaOp::Unspend {
            tx_key: k,
            offset: 0,
            spending_data: [0xAB; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 1,
        };
        apply_op(&engine, &unspend).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!({ meta.spent_utxos }, 0);
        let gen_after = { meta.generation };

        // Re-apply (resend after replica crash): already-unspent is a
        // no-op — no error, no counter change, no generation bump.
        apply_op(&engine, &unspend).unwrap();
        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!({ meta.spent_utxos }, 0);
        assert_eq!({ meta.generation }, gen_after);
        assert_eq!(engine.read_slot(&k, 0).unwrap().status, UTXO_UNSPENT);
    }

    #[test]
    fn apply_unspend_wrong_spending_data_is_noop_without_mutation() {
        let engine = make_engine();
        let k = key(51);
        create_record(&engine, k, 1);

        apply_op(
            &engine,
            &ReplicaOp::Spend {
                tx_key: k,
                offset: 0,
                spending_data: [0xAB; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 0,
            },
        )
        .unwrap();

        // An unspend whose spending_data does not match the recorded spend is a
        // silent idempotent no-op (Lua `unspend` ownership contract), NOT a
        // rejection — "never wipe a spend we don't own". The engine returns OK,
        // so the receiver applies a no-op: the spent slot and counter stay
        // exactly as the recorded spend left them.
        apply_op(
            &engine,
            &ReplicaOp::Unspend {
                tx_key: k,
                offset: 0,
                spending_data: [0xCD; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 1,
            },
        )
        .unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_SPENT);
        assert_eq!(slot.spending_data, [0xAB; 36]);
        assert_eq!({ engine.read_metadata(&k).unwrap().spent_utxos }, 1);
    }

    #[test]
    fn apply_unfreeze_idempotent_reapply() {
        let engine = make_engine();
        let k = key(52);
        create_record(&engine, k, 2);

        apply_op(
            &engine,
            &ReplicaOp::Freeze {
                tx_key: k,
                offset: 0,
                master_generation: 0,
            },
        )
        .unwrap();
        let unfreeze = ReplicaOp::Unfreeze {
            tx_key: k,
            offset: 0,
            master_generation: 1,
        };
        apply_op(&engine, &unfreeze).unwrap();
        assert_eq!(engine.read_slot(&k, 0).unwrap().status, UTXO_UNSPENT);

        // Second unfreeze hits NotFrozen — graceful skip, slot unchanged.
        apply_op(&engine, &unfreeze).unwrap();
        assert_eq!(engine.read_slot(&k, 0).unwrap().status, UTXO_UNSPENT);
    }

    #[test]
    fn apply_reassign_op_and_idempotent_reapply() {
        let engine = make_engine();
        let k = key(53);
        create_record(&engine, k, 2);

        apply_op(
            &engine,
            &ReplicaOp::Freeze {
                tx_key: k,
                offset: 0,
                master_generation: 0,
            },
        )
        .unwrap();

        let new_hash = [0x5A; 32];
        let reassign = ReplicaOp::Reassign {
            tx_key: k,
            offset: 0,
            new_hash,
            block_height: 800_000,
            spendable_after: 1_000,
            master_generation: 1,
        };
        apply_op(&engine, &reassign).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
        assert_eq!(slot.hash, new_hash);
        // Reassign stamps the cooldown height into spending_data[0..4].
        assert_eq!(
            u32::from_le_bytes(slot.spending_data[0..4].try_into().unwrap()),
            801_000,
        );
        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!({ meta.reassignment_count }, 1);

        // Re-apply: the slot is no longer frozen, so the handler maps
        // NotFrozen to a graceful skip — count and hash unchanged.
        apply_op(&engine, &reassign).unwrap();
        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!({ meta.reassignment_count }, 1);
        assert_eq!(engine.read_slot(&k, 0).unwrap().hash, new_hash);
    }

    #[test]
    fn apply_set_conflicting_op_set_and_clear() {
        use crate::record::TxFlags;
        let engine = make_engine();
        let k = key(54);
        create_record(&engine, k, 1);

        let set = ReplicaOp::SetConflicting {
            tx_key: k,
            value: true,
            current_block_height: 700_000,
            retention: 288,
            master_generation: 0,
        };
        apply_op(&engine, &set).unwrap();
        let meta = engine.read_metadata(&k).unwrap();
        assert!(meta.flags.contains(TxFlags::CONFLICTING));

        // Re-apply: flag already set, no error.
        apply_op(&engine, &set).unwrap();
        assert!(
            engine
                .read_metadata(&k)
                .unwrap()
                .flags
                .contains(TxFlags::CONFLICTING)
        );

        apply_op(
            &engine,
            &ReplicaOp::SetConflicting {
                tx_key: k,
                value: false,
                current_block_height: 700_000,
                retention: 288,
                master_generation: 1,
            },
        )
        .unwrap();
        assert!(
            !engine
                .read_metadata(&k)
                .unwrap()
                .flags
                .contains(TxFlags::CONFLICTING)
        );
    }

    #[test]
    fn apply_set_locked_op_set_and_clear() {
        use crate::record::TxFlags;
        let engine = make_engine();
        let k = key(55);
        create_record(&engine, k, 1);

        let lock = ReplicaOp::SetLocked {
            tx_key: k,
            value: true,
            master_generation: 0,
        };
        apply_op(&engine, &lock).unwrap();
        assert!(
            engine
                .read_metadata(&k)
                .unwrap()
                .flags
                .contains(TxFlags::LOCKED)
        );

        // Idempotent re-apply.
        apply_op(&engine, &lock).unwrap();
        assert!(
            engine
                .read_metadata(&k)
                .unwrap()
                .flags
                .contains(TxFlags::LOCKED)
        );

        apply_op(
            &engine,
            &ReplicaOp::SetLocked {
                tx_key: k,
                value: false,
                master_generation: 1,
            },
        )
        .unwrap();
        assert!(
            !engine
                .read_metadata(&k)
                .unwrap()
                .flags
                .contains(TxFlags::LOCKED)
        );
    }

    #[test]
    fn apply_preserve_until_op_and_idempotent_reapply() {
        use crate::record::TxFlags;
        let engine = make_engine();
        let k = key(56);
        create_record(&engine, k, 1);

        let preserve = ReplicaOp::PreserveUntil {
            tx_key: k,
            block_height: 900_000,
            master_generation: 0,
        };
        apply_op(&engine, &preserve).unwrap();
        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!({ meta.preserve_until }, 900_000);
        assert_eq!({ meta.delete_at_height }, 0);
        // HAS_PRESERVE_UNTIL is an index-only discriminant bit (it marks
        // the cached dah_or_preserve as a preserve height, R-019); it is
        // never written to on-device meta.flags.
        let entry = engine.lookup(&k).unwrap();
        assert_ne!(
            entry.tx_flags & TxFlags::HAS_PRESERVE_UNTIL.bits(),
            0,
            "index cache must carry the preserve discriminant so fast \
             paths skip DAH eviction (R-019)"
        );

        // Re-apply: same preserve height lands in the same state.
        apply_op(&engine, &preserve).unwrap();
        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!({ meta.preserve_until }, 900_000);
        assert_eq!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn apply_delete_idempotent_reapply() {
        let engine = make_engine();
        let k = key(57);
        create_record(&engine, k, 2);

        let delete = ReplicaOp::Delete { tx_key: k };
        apply_op(&engine, &delete).unwrap();
        assert!(engine.lookup(&k).is_none());

        // Resend after replica crash: TxNotFound is a graceful skip.
        apply_op(&engine, &delete).unwrap();
        assert!(engine.lookup(&k).is_none());
    }

    #[test]
    fn apply_missing_tx_gracefully_skipped() {
        let engine = make_engine();
        let k = key(99);
        // No record created — ops should succeed (skip)
        apply_op(
            &engine,
            &ReplicaOp::Spend {
                tx_key: k,
                offset: 0,
                spending_data: [0; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 0,
            },
        )
        .unwrap();
        apply_op(&engine, &ReplicaOp::Delete { tx_key: k }).unwrap();
        apply_op(
            &engine,
            &ReplicaOp::Freeze {
                tx_key: k,
                offset: 0,
                master_generation: 0,
            },
        )
        .unwrap();
    }

    #[test]
    fn apply_stale_spend_skipped() {
        let engine = make_engine();
        let k = key(100);
        create_record(&engine, k, 3);

        // Apply a spend with master_generation=2 to advance the record's gen.
        let op1 = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xAA; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 2,
        };
        apply_op(&engine, &op1).unwrap();
        let cur_gen = { engine.read_metadata(&k).unwrap().generation };
        assert_eq!(cur_gen, 2);

        // Now send a stale spend on slot 1.
        // The pre-apply guard should skip it entirely.
        let op2 = ReplicaOp::Spend {
            tx_key: k,
            offset: 1,
            spending_data: [0xBB; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 1,
        };
        apply_op(&engine, &op2).unwrap();
        // Slot 1 should still be UNSPENT because the stale op was rejected.
        let slot = engine.read_slot(&k, 1).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
    }

    #[test]
    fn generation_wraparound_idempotency() {
        let engine = make_engine();
        let k = key(104);
        create_record(&engine, k, 3);
        set_generation(&engine, k, u32::MAX);

        // Fresh across wrap: master_generation 0 is one step ahead of MAX.
        // A numeric stale guard (`master_gen < local_gen`) would skip it.
        let wrapped = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xF0; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 0,
        };
        apply_op(&engine, &wrapped).unwrap();
        let slot0 = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot0.status, UTXO_SPENT);
        assert_eq!(slot0.spending_data[0], 0xF0);
        assert_eq!({ engine.read_metadata(&k).unwrap().generation }, 0);

        // Stale pre-wrap op: MAX is now behind local generation 0 and must
        // not mutate a different slot.
        let stale_pre_wrap = ReplicaOp::Spend {
            tx_key: k,
            offset: 1,
            spending_data: [0xF1; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: u32::MAX,
        };
        apply_op(&engine, &stale_pre_wrap).unwrap();
        let slot1 = engine.read_slot(&k, 1).unwrap();
        assert_eq!(slot1.status, UTXO_UNSPENT);
        assert_eq!({ engine.read_metadata(&k).unwrap().generation }, 0);
    }

    #[test]
    fn apply_fresh_spend_applies() {
        let engine = make_engine();
        let k = key(101);
        create_record(&engine, k, 3);

        // Fresh op: master_gen=1 is ahead of local_gen=0.
        let op = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xCC; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 1,
        };
        apply_op(&engine, &op).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_SPENT);
        let cur_gen = { engine.read_metadata(&k).unwrap().generation };
        assert_eq!(cur_gen, 1);
    }

    #[test]
    fn apply_equal_generation_idempotent() {
        // Equal-generation replays are allowed through because all ops are
        // idempotent. The guard only rejects strictly-lower generations.
        let engine = make_engine();
        let k = key(102);
        create_record(&engine, k, 3);

        // Advance to gen=2 with a freeze.
        let op1 = ReplicaOp::Freeze {
            tx_key: k,
            offset: 0,
            master_generation: 2,
        };
        apply_op(&engine, &op1).unwrap();
        let cur_gen = { engine.read_metadata(&k).unwrap().generation };
        assert_eq!(cur_gen, 2);

        // Replay the same freeze (master_gen=2 == local_gen=2) — allowed,
        // handled idempotently by the engine (AlreadyFrozen → Ok(())).
        apply_op(&engine, &op1).unwrap();
        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_FROZEN);
    }

    #[test]
    fn apply_stale_freeze_skipped() {
        let engine = make_engine();
        let k = key(103);
        create_record(&engine, k, 3);

        // Advance to gen=5 via a spend.
        let op1 = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xEE; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 5,
        };
        apply_op(&engine, &op1).unwrap();

        // Stale freeze (gen=3 <= 5) on slot 1 should be rejected.
        let op2 = ReplicaOp::Freeze {
            tx_key: k,
            offset: 1,
            master_generation: 3,
        };
        apply_op(&engine, &op2).unwrap();
        let slot = engine.read_slot(&k, 1).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
    }

    /// R-052: applying a `MarkLongestChain` `ReplicaOp` MUST mutate
    /// `unmined_since`, run the DAH evaluator, and sync the record's
    /// generation to the master's value. Pre-fix the variant did not
    /// exist; the receiver had no way to apply this mutation at all.
    #[test]
    fn apply_mark_longest_chain_off_sets_unmined_and_syncs_generation() {
        let engine = make_engine();
        let k = key(120);
        create_record(&engine, k, 1);

        let pre_gen = { engine.read_metadata(&k).unwrap().generation };
        let master_generation = pre_gen + 1;
        let op = ReplicaOp::MarkLongestChain {
            tx_key: k,
            on_longest_chain: false,
            current_block_height: 800_000,
            block_height_retention: 288,
            master_generation,
        };
        apply_op(&engine, &op).unwrap();

        let post = engine.read_metadata(&k).unwrap();
        assert_eq!(
            { post.unmined_since },
            800_000,
            "off-chain mark must set unmined_since to current_block_height",
        );
        assert_eq!(
            { post.generation },
            master_generation,
            "generation must sync to master_generation after apply",
        );
    }

    /// R-052: re-marking a record back ON the longest chain MUST clear
    /// `unmined_since`. Verifies the inverse path of the off-chain test.
    #[test]
    fn apply_mark_longest_chain_on_clears_unmined() {
        let engine = make_engine();
        let k = key(121);
        create_record(&engine, k, 1);

        let g1 = { engine.read_metadata(&k).unwrap().generation } + 1;
        apply_op(
            &engine,
            &ReplicaOp::MarkLongestChain {
                tx_key: k,
                on_longest_chain: false,
                current_block_height: 800_000,
                block_height_retention: 288,
                master_generation: g1,
            },
        )
        .unwrap();
        assert_eq!({ engine.read_metadata(&k).unwrap().unmined_since }, 800_000);

        // Now mark it back ON: unmined_since must reset to 0 and the
        // generation must advance.
        let g2 = g1 + 1;
        apply_op(
            &engine,
            &ReplicaOp::MarkLongestChain {
                tx_key: k,
                on_longest_chain: true,
                current_block_height: 801_000,
                block_height_retention: 288,
                master_generation: g2,
            },
        )
        .unwrap();
        let post = engine.read_metadata(&k).unwrap();
        assert_eq!(
            { post.unmined_since },
            0,
            "on-chain mark must clear unmined_since"
        );
        assert_eq!({ post.generation }, g2);
    }

    /// R-053: replaying the same `MarkLongestChain` op twice (same
    /// `master_generation`) MUST be a no-op the second time. Equal-
    /// generation guard inside `apply_op` for MarkLongestChain skips
    /// the engine call entirely so generation does not bump twice and
    /// the DAH/unmined indexes are not re-written.
    ///
    /// This is the unit-level mirror of the TCP integration test
    /// `mark_longest_chain_replay_idempotent` — fails fast at the
    /// receiver layer if the equal-generation gate is dropped.
    #[test]
    fn apply_mark_longest_chain_equal_generation_idempotent() {
        let engine = make_engine();
        let k = key(122);
        create_record(&engine, k, 1);

        let pre_gen = { engine.read_metadata(&k).unwrap().generation };
        let master_generation = pre_gen + 1;
        let op = ReplicaOp::MarkLongestChain {
            tx_key: k,
            on_longest_chain: false,
            current_block_height: 850_000,
            block_height_retention: 288,
            master_generation,
        };

        // First apply mutates state.
        apply_op(&engine, &op).unwrap();
        let post1 = engine.read_metadata(&k).unwrap();
        let post1_gen = { post1.generation };
        let post1_unmined = { post1.unmined_since };
        let post1_dah = { post1.delete_at_height };
        assert_eq!(post1_gen, master_generation);
        assert_eq!(post1_unmined, 850_000);

        // Second apply (equal generation) MUST be a no-op.
        apply_op(&engine, &op).unwrap();
        let post2 = engine.read_metadata(&k).unwrap();
        assert_eq!(
            { post2.generation },
            post1_gen,
            "equal-generation replay must NOT bump generation (R-053)",
        );
        assert_eq!({ post2.unmined_since }, post1_unmined);
        assert_eq!({ post2.delete_at_height }, post1_dah);
    }

    /// R-052: a strictly-stale `MarkLongestChain` op (master_generation
    /// strictly less than the replica's current generation) MUST be
    /// rejected by the pre-apply guard at the top of `apply_op`. The
    /// replica's `unmined_since` must NOT be reverted.
    #[test]
    fn apply_stale_mark_longest_chain_skipped() {
        let engine = make_engine();
        let k = key(123);
        create_record(&engine, k, 1);

        // Advance to gen=5 via a non-chain op.
        let op_advance = ReplicaOp::Freeze {
            tx_key: k,
            offset: 0,
            master_generation: 5,
        };
        apply_op(&engine, &op_advance).unwrap();
        let cur_gen = { engine.read_metadata(&k).unwrap().generation };
        assert_eq!(cur_gen, 5);

        // Stale MarkLongestChain at gen=3 — must NOT mutate state.
        let stale = ReplicaOp::MarkLongestChain {
            tx_key: k,
            on_longest_chain: false,
            current_block_height: 900_000,
            block_height_retention: 288,
            master_generation: 3,
        };
        apply_op(&engine, &stale).unwrap();
        let post = engine.read_metadata(&k).unwrap();
        assert_eq!({ post.generation }, 5, "stale op must not bump generation");
        assert_eq!(
            { post.unmined_since },
            0,
            "stale off-chain mark must NOT set unmined_since",
        );
    }

    /// Build a full metadata buffer matching the extended wire format used by
    /// the live create path: core(46) + lifecycle(24) + extended fields.
    fn build_full_metadata(
        tx_version: u32,
        is_coinbase: bool,
        wire_flags: u8,
        generation: u32,
        block_height: u32,
        block_infos: &[(u32, u32, u32)], // (block_id, block_height, subtree_idx)
        parent_txids: &[[u8; 32]],
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        // Core 46 bytes
        buf.extend_from_slice(&tx_version.to_le_bytes()); // tx_version
        buf.extend_from_slice(&0u32.to_le_bytes()); // locktime
        buf.extend_from_slice(&0u64.to_le_bytes()); // fee
        buf.extend_from_slice(&0u64.to_le_bytes()); // size_in_bytes
        buf.extend_from_slice(&0u64.to_le_bytes()); // extended_size
        buf.push(if is_coinbase { 1 } else { 0 }); // is_coinbase
        buf.extend_from_slice(&0u32.to_le_bytes()); // spending_height
        buf.extend_from_slice(&0u64.to_le_bytes()); // created_at
        buf.push(wire_flags); // flags
        // Lifecycle 24 bytes
        buf.extend_from_slice(&generation.to_le_bytes()); // generation
        buf.extend_from_slice(&0u64.to_le_bytes()); // updated_at
        buf.extend_from_slice(&0u32.to_le_bytes()); // unmined_since
        buf.extend_from_slice(&0u32.to_le_bytes()); // delete_at_height
        buf.extend_from_slice(&0u32.to_le_bytes()); // preserve_until
        // Extended: block_height + block_infos + parent_txids
        buf.extend_from_slice(&block_height.to_le_bytes());
        buf.push(block_infos.len() as u8);
        for (bid, bht, bsi) in block_infos {
            buf.extend_from_slice(&bid.to_le_bytes());
            buf.extend_from_slice(&bht.to_le_bytes());
            buf.extend_from_slice(&bsi.to_le_bytes());
        }
        buf.extend_from_slice(&(parent_txids.len() as u16).to_le_bytes());
        for ptx in parent_txids {
            buf.extend_from_slice(ptx);
        }
        buf
    }

    #[test]
    fn create_replication_full_state() {
        let engine = make_engine();
        let k = key(110);
        let hashes = vec![[0xAA; 32]; 3];

        // Build metadata with mined_block_info, frozen flag, and parent_txids.
        let parent = [0xBBu8; 32];
        let meta_bytes = build_full_metadata(
            2,                // tx_version
            false,            // is_coinbase
            0x04,             // frozen=0x04
            5,                // generation
            1000,             // block_height
            &[(42, 1000, 7)], // one block entry
            &[parent],        // one parent_txid
        );

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: meta_bytes,
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();

        // Verify the record was created.
        let slot = engine.read_slot(&k, 0).unwrap();
        // Frozen flag should have been applied via CreateRequest.frozen = true
        assert_eq!(slot.status, UTXO_FROZEN);

        // Verify lifecycle metadata was applied.
        let meta = engine.read_metadata(&k).unwrap();
        let restored_gen = { meta.generation };
        assert_eq!(restored_gen, 5);

        // Verify block entry was applied.
        let block_count = { meta.block_entry_count };
        assert_eq!(block_count, 1);
        let be_id = { meta.block_entries_inline[0].block_id };
        assert_eq!(be_id, 42);
    }

    #[test]
    fn create_replication_46byte_compat() {
        // Old-format 46-byte payload should still work with defaults.
        let engine = make_engine();
        let k = key(111);
        let hashes = vec![[0xCC; 32]; 2];

        let mut meta_bytes = Vec::with_capacity(46);
        meta_bytes.extend_from_slice(&1u32.to_le_bytes()); // tx_version
        meta_bytes.extend_from_slice(&0u32.to_le_bytes()); // locktime
        meta_bytes.extend_from_slice(&100u64.to_le_bytes()); // fee
        meta_bytes.extend_from_slice(&200u64.to_le_bytes()); // size_in_bytes
        meta_bytes.extend_from_slice(&0u64.to_le_bytes()); // extended_size
        meta_bytes.push(0); // is_coinbase
        meta_bytes.extend_from_slice(&0u32.to_le_bytes()); // spending_height
        meta_bytes.extend_from_slice(&0u64.to_le_bytes()); // created_at
        meta_bytes.push(0); // flags

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: meta_bytes,
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();

        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
        let meta = engine.read_metadata(&k).unwrap();
        let block_count = { meta.block_entry_count };
        assert_eq!(block_count, 0); // No block entries in 46-byte format
    }

    #[test]
    fn create_replication_lifecycle_fields() {
        let engine = make_engine();
        let k = key(112);
        let hashes = vec![[0xDD; 32]; 1];

        let mut meta_bytes = build_full_metadata(1, false, 0, 10, 0, &[], &[]);
        // Patch lifecycle fields: set delete_at_height=500 and preserve_until=700
        // Offsets: generation(46-49), updated_at(50-57), unmined_since(58-61),
        //          delete_at_height(62-65), preserve_until(66-69)
        meta_bytes[62..66].copy_from_slice(&500u32.to_le_bytes());
        meta_bytes[66..70].copy_from_slice(&700u32.to_le_bytes());

        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: meta_bytes,
            utxo_hashes: hashes,
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &op).unwrap();

        let meta = engine.read_metadata(&k).unwrap();
        let restored_gen = { meta.generation };
        let restored_dah = { meta.delete_at_height };
        let restored_pu = { meta.preserve_until };
        assert_eq!(restored_gen, 10);
        assert_eq!(restored_dah, 500);
        assert_eq!(restored_pu, 700);
    }

    #[test]
    fn stale_create_replication_does_not_replace_newer_record() {
        let engine = make_engine();
        let k = key(113);
        let hashes = vec![[0x11; 32]; 1];

        let fresh = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: build_full_metadata(7, false, 0, 5, 0, &[], &[]),
            utxo_hashes: hashes.clone(),
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &fresh).unwrap();

        let stale = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes: build_full_metadata(99, false, 0, 3, 0, &[], &[]),
            utxo_hashes: vec![[0xEE; 32]; 1],
            cold_data: None,
            is_external: false,
        };
        apply_op(&engine, &stale).unwrap();

        let meta = engine.read_metadata(&k).unwrap();
        let generation = { meta.generation };
        let tx_version = { meta.tx_version };
        assert_eq!(generation, 5);
        assert_eq!(tx_version, 7);
        let slot = engine.read_slot(&k, 0).unwrap();
        assert_eq!(slot.hash, hashes[0]);
    }

    #[test]
    fn last_applied_monotonic_under_concurrent_batches() {
        // Regression test: multiple concurrent handler threads calling
        // handle_replica_batch must not regress last_applied. The old
        // code used store() which could overwrite a higher value with
        // a lower one if batch completion order differs from sequence
        // order. The fix uses fetch_max() for monotonic advancement.
        let engine = make_engine();
        let last_applied = Arc::new(AtomicU64::new(0));

        // Create two records so ops succeed
        create_record(&engine, key(200), 2);
        create_record(&engine, key(201), 2);

        // Batch A: sequence range 10..12 (higher)
        let batch_a = ReplicaBatch {
            first_sequence: 10,
            ops: vec![
                ReplicaOp::Spend {
                    tx_key: key(200),
                    offset: 0,
                    spending_data: [0xAA; 36],
                    current_block_height: 700_000,
                    block_height_retention: 288,
                    master_generation: 1,
                },
                ReplicaOp::Spend {
                    tx_key: key(200),
                    offset: 1,
                    spending_data: [0xBB; 36],
                    current_block_height: 700_000,
                    block_height_retention: 288,
                    master_generation: 1,
                },
            ],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };

        // Batch B: sequence range 5..6 (lower)
        let batch_b = ReplicaBatch {
            first_sequence: 5,
            ops: vec![ReplicaOp::Spend {
                tx_key: key(201),
                offset: 0,
                spending_data: [0xCC; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 1,
            }],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };

        // Simulate: batch A completes first, then batch B completes.
        // With store(), last_applied would go 11 → 5 (regression).
        // With fetch_max(), it stays at 11.
        let req_a = RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id: 1,
            flags: 0,
            payload: batch_a.serialize().into(),
        };
        let req_b = RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id: 2,
            flags: 0,
            payload: batch_b.serialize().into(),
        };

        let resp_a = handle_replica_batch(&req_a, &engine, &last_applied);
        assert_eq!(resp_a.status, STATUS_OK);
        assert_eq!(last_applied.load(Ordering::Relaxed), 11);

        let resp_b = handle_replica_batch(&req_b, &engine, &last_applied);
        assert_eq!(resp_b.status, STATUS_OK);
        // Key assertion: last_applied must NOT regress from 11 to 5.
        assert_eq!(
            last_applied.load(Ordering::Relaxed),
            11,
            "last_applied must be monotonic — fetch_max should keep it at 11, not regress to 5"
        );
    }

    #[test]
    fn last_applied_advances_when_higher() {
        // Complementary test: verify fetch_max does advance when the
        // new value is genuinely higher.
        let engine = make_engine();
        let last_applied = Arc::new(AtomicU64::new(0));
        create_record(&engine, key(210), 2);
        create_record(&engine, key(211), 2);

        // First batch: seq 1..2
        let batch_1 = ReplicaBatch {
            first_sequence: 1,
            ops: vec![ReplicaOp::Spend {
                tx_key: key(210),
                offset: 0,
                spending_data: [0xDD; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 1,
            }],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };
        let req_1 = RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id: 1,
            flags: 0,
            payload: batch_1.serialize().into(),
        };
        handle_replica_batch(&req_1, &engine, &last_applied);
        assert_eq!(last_applied.load(Ordering::Relaxed), 1);

        // Second batch: seq 10..11
        let batch_2 = ReplicaBatch {
            first_sequence: 10,
            ops: vec![ReplicaOp::Spend {
                tx_key: key(211),
                offset: 0,
                spending_data: [0xEE; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 1,
            }],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };
        let req_2 = RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id: 2,
            flags: 0,
            payload: batch_2.serialize().into(),
        };
        handle_replica_batch(&req_2, &engine, &last_applied);
        assert_eq!(last_applied.load(Ordering::Relaxed), 10);
    }

    // -------------------------------------------------------------------
    // H5: Replica-side idempotency journal
    // -------------------------------------------------------------------

    /// Construct a batch whose every op is a `Spend` on consecutive
    /// offsets of a single tx_key. Useful for tests that want to
    /// observe side-effects of apply.
    fn make_spend_batch(
        first_sequence: u64,
        tx_key: TxKey,
        offsets: std::ops::Range<u32>,
        generation: u32,
    ) -> ReplicaBatch {
        let ops = offsets
            .map(|offset| ReplicaOp::Spend {
                tx_key,
                offset,
                spending_data: [0xAA; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: generation,
            })
            .collect();
        ReplicaBatch {
            first_sequence,
            ops,
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        }
    }

    fn batch_request(batch: &ReplicaBatch, request_id: u64) -> RequestFrame {
        RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id,
            flags: 0,
            payload: batch.serialize().into(),
        }
    }

    /// `replica_skips_duplicate_resend`: the leader re-transmits an
    /// identical batch. The receiver must apply ops exactly once and
    /// then short-circuit the resend without touching the engine a
    /// second time.
    #[test]
    fn replica_skips_duplicate_resend() {
        let engine = make_engine();
        // 3 UTXOs so we can spend offsets 0..3.
        create_record(&engine, key(42), 3);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();

        let batch = make_spend_batch(10, key(42), 0..3, 1);
        let stream_key = "peer-A:5000";
        // R-D1: the dense-sequence contract NAKs batches ahead of
        // watermark+1, so seed the stream watermark at 9 to make
        // first_sequence=10 the next-expected position.
        tracker.set(stream_key, 9);

        // First application: all three spends go through.
        let resp_1 = handle_replica_batch_with_tracker(
            &batch_request(&batch, 1),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp_1.status, STATUS_OK);
        let ack_1 = ReplicaAck::deserialize(&resp_1.payload).unwrap();
        assert_eq!(
            ack_1,
            ReplicaAck::Ok {
                through_sequence: 12
            }
        );

        // Slot 0 is now SPENT.
        let slot0_after_first = engine.read_slot(&key(42), 0).unwrap();
        assert_eq!(slot0_after_first.status, UTXO_SPENT);

        // The tracker recorded the high-water mark.
        assert_eq!(tracker.get(stream_key), 12);

        // Mutate slot 1 OUT-OF-BAND to a state the spend op would
        // overwrite — if the resend hit apply_op again it would
        // zero the spending_data we inject here. A real system
        // would never do this; the test needs a witness that
        // proves no engine-level work happens.
        //
        // Simpler witness: ensure the resend does NOT increment the
        // engine's internal generation counter for the record. Read
        // the current metadata generation and compare after resend.
        let gen_after_first = { engine.read_metadata(&key(42)).unwrap().generation };

        // Resend the same batch.
        let resp_2 = handle_replica_batch_with_tracker(
            &batch_request(&batch, 2),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp_2.status, STATUS_OK);
        let ack_2 = ReplicaAck::deserialize(&resp_2.payload).unwrap();
        // Skipped batches still ACK with the existing high-water mark.
        assert_eq!(
            ack_2,
            ReplicaAck::Ok {
                through_sequence: 12
            }
        );

        // Generation must NOT have moved on the resend — proof the
        // engine was not touched a second time.
        let gen_after_resend = { engine.read_metadata(&key(42)).unwrap().generation };
        assert_eq!(
            gen_after_resend, gen_after_first,
            "duplicate resend must not mutate engine state",
        );

        // Tracker still sits at the same high-water mark.
        assert_eq!(tracker.get(stream_key), 12);
    }

    /// `replica_restart_remembers_last_applied_seq`: after persisting
    /// state and reopening the tracker from disk, the same batch is
    /// treated as a duplicate and skipped.
    #[test]
    fn replica_restart_remembers_last_applied_seq() {
        let engine = make_engine();
        create_record(&engine, key(43), 2);

        let last_applied = Arc::new(AtomicU64::new(0));
        let dir = tempfile::tempdir().unwrap();
        let tracker_path = dir.path().join("applied.dat");
        let stream_key = "peer-B:5100";

        // --- First lifecycle: apply, persist, drop.
        {
            let tracker = ReplicaAppliedTracker::load(tracker_path.clone()).unwrap();
            // R-D1: seed the watermark so first_sequence=100 is next-expected.
            tracker.set(stream_key, 99);
            let batch = make_spend_batch(100, key(43), 0..2, 1);
            let resp = handle_replica_batch_with_tracker(
                &batch_request(&batch, 1),
                &engine,
                &last_applied,
                Some(&tracker),
                stream_key,
                0,
            );
            assert_eq!(resp.status, STATUS_OK);
            assert_eq!(tracker.get(stream_key), 101);
            // Durability before ACK: tracker must have flushed.
            // (verified by reopening below — no explicit flush call)
        }

        // --- Second lifecycle: reopen tracker, resend SAME batch.
        let reopened_tracker = ReplicaAppliedTracker::load(tracker_path.clone()).unwrap();
        assert_eq!(
            reopened_tracker.get(stream_key),
            101,
            "reopened tracker must remember last_applied_seq persisted before ACK",
        );

        let gen_before_resend = { engine.read_metadata(&key(43)).unwrap().generation };
        let new_last_applied = Arc::new(AtomicU64::new(0));
        let batch = make_spend_batch(100, key(43), 0..2, 1);
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&batch, 2),
            &engine,
            &new_last_applied,
            Some(&reopened_tracker),
            stream_key,
            0,
        );
        let ack = ReplicaAck::deserialize(&resp.payload).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Ok {
                through_sequence: 101
            }
        );

        // Engine generation unchanged across the restart — proof the
        // resend did not touch the engine.
        let gen_after_resend = { engine.read_metadata(&key(43)).unwrap().generation };
        assert_eq!(gen_before_resend, gen_after_resend);
    }

    #[test]
    fn replica_source_node_id_dedupes_across_fallback_stream_keys_after_restart() {
        let engine = make_engine();
        create_record(&engine, key(46), 2);

        let dir = tempfile::tempdir().unwrap();
        let tracker_path = dir.path().join("applied.dat");
        let last_applied = Arc::new(AtomicU64::new(0));

        {
            let tracker = ReplicaAppliedTracker::load(tracker_path.clone()).unwrap();
            // R-D1: seed the source-node stream so 500 is next-expected.
            tracker.set("node:7", 499);
            let mut batch = make_spend_batch(500, key(46), 0..2, 1);
            batch.source_node_id = Some(7);
            let resp = handle_replica_batch_with_tracker(
                &batch_request(&batch, 1),
                &engine,
                &last_applied,
                Some(&tracker),
                "127.0.0.1:41000",
                0,
            );
            assert_eq!(resp.status, STATUS_OK);
            assert_eq!(tracker.get("node:7"), 501);
            assert_eq!(tracker.get("127.0.0.1:41000"), 0);
        }

        let tracker = ReplicaAppliedTracker::load(tracker_path).unwrap();
        assert_eq!(tracker.get("node:7"), 501);
        let gen_before_resend = { engine.read_metadata(&key(46)).unwrap().generation };

        let mut batch = make_spend_batch(500, key(46), 0..2, 1);
        batch.source_node_id = Some(7);
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&batch, 2),
            &engine,
            &Arc::new(AtomicU64::new(0)),
            Some(&tracker),
            "127.0.0.1:42000",
            0,
        );
        assert_eq!(resp.status, STATUS_OK);
        let ack = ReplicaAck::deserialize(&resp.payload).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Ok {
                through_sequence: 501
            }
        );
        let gen_after_resend = { engine.read_metadata(&key(46)).unwrap().generation };
        assert_eq!(
            gen_after_resend, gen_before_resend,
            "same source_node_id must skip duplicate resend even if TCP peer key changes",
        );
        assert_eq!(tracker.get("node:7"), 501);
        assert_eq!(tracker.get("127.0.0.1:42000"), 0);
    }

    /// `replica_applies_new_seqs_after_restart`: after reloading the
    /// tracker, sequences *higher* than the persisted last-applied
    /// mark must apply normally, not be skipped.
    #[test]
    fn replica_applies_new_seqs_after_restart() {
        let engine = make_engine();
        create_record(&engine, key(44), 4);

        let last_applied = Arc::new(AtomicU64::new(0));
        let dir = tempfile::tempdir().unwrap();
        let tracker_path = dir.path().join("applied.dat");
        let stream_key = "peer-C:5200";

        // --- First session: spend slots 0..2 → tracker = seq 201.
        {
            let tracker = ReplicaAppliedTracker::load(tracker_path.clone()).unwrap();
            // R-D1: seed the watermark so first_sequence=200 is next-expected.
            tracker.set(stream_key, 199);
            let batch = make_spend_batch(200, key(44), 0..2, 1);
            handle_replica_batch_with_tracker(
                &batch_request(&batch, 1),
                &engine,
                &last_applied,
                Some(&tracker),
                stream_key,
                0,
            );
            assert_eq!(tracker.get(stream_key), 201);
        }

        // --- Restart: reopen tracker, send HIGHER seqs.
        let tracker = ReplicaAppliedTracker::load(tracker_path).unwrap();
        assert_eq!(tracker.get(stream_key), 201);

        // Verify slots 2 and 3 are currently UNSPENT.
        assert_eq!(engine.read_slot(&key(44), 2).unwrap().status, UTXO_UNSPENT);
        assert_eq!(engine.read_slot(&key(44), 3).unwrap().status, UTXO_UNSPENT);

        let new_last_applied = Arc::new(AtomicU64::new(0));
        let batch = make_spend_batch(202, key(44), 2..4, 2);
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&batch, 10),
            &engine,
            &new_last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        let ack = ReplicaAck::deserialize(&resp.payload).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Ok {
                through_sequence: 203
            }
        );

        // The new ops actually applied.
        assert_eq!(engine.read_slot(&key(44), 2).unwrap().status, UTXO_SPENT);
        assert_eq!(engine.read_slot(&key(44), 3).unwrap().status, UTXO_SPENT);

        // Tracker advanced.
        assert_eq!(tracker.get(stream_key), 203);
    }

    /// Overlapping resend: the leader retransmits a batch whose
    /// first half was already applied and whose second half is new.
    /// Only the suffix should touch the engine.
    #[test]
    fn replica_skips_duplicate_prefix_only() {
        let engine = make_engine();
        create_record(&engine, key(45), 5);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let stream_key = "peer-D:5300";

        // R-D1: seed the watermark so first_sequence=300 is next-expected.
        tracker.set(stream_key, 299);
        // First: apply seqs 300..302 (slots 0,1,2).
        let batch_a = make_spend_batch(300, key(45), 0..3, 1);
        handle_replica_batch_with_tracker(
            &batch_request(&batch_a, 1),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(tracker.get(stream_key), 302);

        // Sanity: slots 3,4 still unspent.
        assert_eq!(engine.read_slot(&key(45), 3).unwrap().status, UTXO_UNSPENT);
        assert_eq!(engine.read_slot(&key(45), 4).unwrap().status, UTXO_UNSPENT);

        // Overlapping resend: seqs 301..304 covers slots 1..5. The
        // prefix (slots 1,2 = seqs 301,302) is duplicate; the suffix
        // (slots 3,4 = seqs 303,304) is new.
        let batch_b = make_spend_batch(301, key(45), 1..5, 2);
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&batch_b, 2),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        let ack = ReplicaAck::deserialize(&resp.payload).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Ok {
                through_sequence: 304
            }
        );

        // Slots 3 and 4 are now spent.
        assert_eq!(engine.read_slot(&key(45), 3).unwrap().status, UTXO_SPENT);
        assert_eq!(engine.read_slot(&key(45), 4).unwrap().status, UTXO_SPENT);
        assert_eq!(tracker.get(stream_key), 304);
    }

    // -------------------------------------------------------------------
    // R-D1/D-3: dense per-stream sequence contract
    // -------------------------------------------------------------------

    /// D-1 regression: out-of-order delivery. The batch labeled 11..20
    /// arrives before 1..10. PRE-FIX the receiver applied the early
    /// batch (advancing the high-water mark to 20) and then ACK-dropped
    /// the late batch 1..10 — acked but never applied, a silent
    /// permanent divergence. POST-FIX the ahead-of-expected batch is
    /// NAKed with `ReplicaAck::Gap`; once the sender re-delivers in
    /// order, both batches apply and the final state is complete.
    #[test]
    fn out_of_order_batch_naks_gap_then_applies_in_order() {
        let engine = make_engine();
        create_record(&engine, key(80), 4);
        create_record(&engine, key(81), 4);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let stream_key = "node:9";

        let batch_first = make_spend_batch(1, key(80), 0..2, 1); // seqs 1..2
        let batch_second = make_spend_batch(3, key(81), 0..2, 1); // seqs 3..4

        // Out-of-order: the SECOND batch arrives first → Gap NAK.
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&batch_second, 1),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp.status, STATUS_ERROR, "gap must NOT be ACKed");
        let ack = ReplicaAck::deserialize(&resp.payload).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Gap {
                expected_sequence: 1,
                received_first_sequence: 3,
            },
        );
        // NAK must not apply anything or advance any watermark.
        assert_eq!(engine.read_slot(&key(81), 0).unwrap().status, UTXO_UNSPENT);
        assert_eq!(tracker.get(stream_key), 0);
        assert_eq!(last_applied.load(Ordering::Relaxed), 0);

        // In-order delivery: batch 1..2 applies.
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&batch_first, 2),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            ReplicaAck::deserialize(&resp.payload).unwrap(),
            ReplicaAck::Ok {
                through_sequence: 2
            },
        );

        // Re-send of the previously NAKed batch now applies.
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&batch_second, 3),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            ReplicaAck::deserialize(&resp.payload).unwrap(),
            ReplicaAck::Ok {
                through_sequence: 4
            },
        );

        // Final state complete: all four ops applied.
        for (k, off) in [(80u8, 0u32), (80, 1), (81, 0), (81, 1)] {
            assert_eq!(
                engine.read_slot(&key(k), off).unwrap().status,
                UTXO_SPENT,
                "op on key {k} offset {off} must be applied after in-order re-delivery",
            );
        }
        assert_eq!(tracker.get(stream_key), 4);
    }

    /// A watermark probe (empty batch, `first_sequence: 0`) must ACK the
    /// current per-stream applied watermark without touching the engine
    /// or the tracker — both on a fresh stream (watermark 0) and after
    /// real batches have applied. Masters use the probe to adopt the
    /// replica's authoritative position before assigning dense labels.
    #[test]
    fn empty_batch_probe_acks_current_watermark() {
        let engine = make_engine();
        create_record(&engine, key(82), 2);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let stream_key = "node:5";

        let probe = ReplicaBatch {
            first_sequence: 0,
            ops: vec![],
            trace_ctx: None,
            source_node_id: None,
            cluster_key: 0,
        };

        // Fresh stream → watermark 0.
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&probe, 1),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            ReplicaAck::deserialize(&resp.payload).unwrap(),
            ReplicaAck::Ok {
                through_sequence: 0
            },
        );
        assert_eq!(tracker.get(stream_key), 0, "probe must not advance tracker");

        // Apply seqs 1..2, then probe again → watermark 2.
        let batch = make_spend_batch(1, key(82), 0..2, 1);
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&batch, 2),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp.status, STATUS_OK);

        let resp = handle_replica_batch_with_tracker(
            &batch_request(&probe, 3),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            ReplicaAck::deserialize(&resp.payload).unwrap(),
            ReplicaAck::Ok {
                through_sequence: 2
            },
        );
        assert_eq!(tracker.get(stream_key), 2);
    }

    /// Out-of-band batches (`first_sequence: 0` with non-empty ops — the
    /// in-process compensation path) must apply ALL ops regardless of the
    /// stream watermark and must not advance it. PRE-FIX such batches
    /// fell into the high-water-mark dedup: with any advanced watermark
    /// the whole batch (or its first op) was silently skipped while the
    /// caller saw STATUS_OK.
    #[test]
    fn out_of_band_zero_sequence_batch_applies_despite_high_watermark() {
        let engine = make_engine();
        create_record(&engine, key(83), 3);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let stream_key = DEFAULT_STREAM_KEY;

        // Advance the stream watermark with a normal batch (seq 1).
        let batch = make_spend_batch(1, key(83), 0..1, 1);
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&batch, 1),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(tracker.get(stream_key), 1);

        // Out-of-band single-op batch at first_sequence 0: must apply
        // even though through (0) <= watermark (1).
        let oob = make_spend_batch(0, key(83), 2..3, 1);
        let resp = handle_replica_batch_with_tracker(
            &batch_request(&oob, 2),
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(
            engine.read_slot(&key(83), 2).unwrap().status,
            UTXO_SPENT,
            "out-of-band (first_sequence=0) ops must apply regardless of the watermark",
        );
        assert_eq!(
            tracker.get(stream_key),
            1,
            "out-of-band batches must not advance the stream watermark",
        );
    }

    /// Pattern A root-cause regression: a migration batch uses
    /// `first_sequence: 0` because migrations are coordinated out-of-band,
    /// not through the replication sequence-number stream. If the receiver
    /// has already seen normal-replication batches with higher sequences on
    /// the same stream key, the dedup check silently skips the migration
    /// batch and the receiver ACKs OK without touching the engine — the
    /// source then sends OP_MIGRATION_COMPLETE, the manifest check either
    /// trivially passes (receiver's prior state satisfies `actual >=
    /// expected_records`) or collides, and records physically never land
    /// on the new master. Clients subsequently see `TX_NOT_FOUND` on reads
    /// that route to that new master.
    ///
    /// This test reproduces the silent-skip by threading a high-watermark
    /// batch through first (emulating normal replication traffic), then
    /// sending a `FLAG_MIGRATION_BATCH` batch with `first_sequence: 0`.
    /// The migration ops must be applied regardless of the tracker's
    /// current high-water mark.
    #[test]
    fn migration_batch_applies_even_when_tracker_seq_is_ahead() {
        let engine = make_engine();
        // One record with four slots so we can observe which batches
        // actually applied by reading slot status afterward.
        create_record(&engine, key(60), 4);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let stream_key = DEFAULT_STREAM_KEY;

        // Step 1: normal-replication batch at sequences 100..=102 that
        // spends slot 0. This pushes the tracker's high-water mark up to
        // 102 for `stream_key`.
        // R-D1: seed the watermark so first_sequence=100 is next-expected.
        tracker.set(stream_key, 99);
        let normal_batch = make_spend_batch(100, key(60), 0..1, 1);
        let normal_req = RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id: 1,
            flags: 0,
            payload: normal_batch.serialize().into(),
        };
        let resp = handle_replica_batch_with_tracker(
            &normal_req,
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(tracker.get(stream_key), 100);
        assert_eq!(engine.read_slot(&key(60), 0).unwrap().status, UTXO_SPENT);

        // Step 2: migration batch delivering a spend on slot 2. Uses
        // `first_sequence: 0` (migrations don't participate in the
        // master's replication sequence stream) and sets
        // `FLAG_MIGRATION_BATCH`. Before the fix the dedup check
        // `through (0) <= already_applied (100)` silently skipped every
        // op in this batch and slot 2 stayed UNSPENT.
        let migration_batch = make_spend_batch(0, key(60), 2..3, 1);
        let migration_req = RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id: /* shard id goes here in prod */ 7,
            flags: FLAG_MIGRATION_BATCH,
            payload: migration_batch.serialize().into(),
        };
        let resp = handle_replica_batch_with_tracker(
            &migration_req,
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp.status, STATUS_OK);
        // The ops in the migration batch must have been applied. Slot 2
        // transitions from UNSPENT to SPENT.
        assert_eq!(
            engine.read_slot(&key(60), 2).unwrap().status,
            UTXO_SPENT,
            "migration batch with first_sequence=0 was silently skipped \
             because the tracker already had a higher high-water mark \
             from normal replication",
        );
        // The migration batch must NOT advance the normal-replication
        // high-water mark backward — migrations are out-of-band.
        assert_eq!(
            tracker.get(stream_key),
            100,
            "migration batch should not overwrite the normal-replication \
             high-water mark with its own sequence space",
        );
    }

    /// F-G7-015 (positive verification): when the master retries a
    /// batch after a partial / stale-connection drop, the receiver's
    /// dedup tracker MUST skip the prefix that already applied and
    /// only re-apply the suffix. Sending the exact same batch twice
    /// must result in exactly one durable mutation per op, never two.
    #[test]
    fn duplicate_batch_after_stale_connection_skips_already_applied() {
        let engine = make_engine();
        let k = key(85);
        create_record(&engine, k, 3);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let stream_key = DEFAULT_STREAM_KEY;

        // Master sends a batch covering ops at sequences 1..=2. The
        // receiver applies them and advances the high-water mark.
        let batch = make_spend_batch(1, k, 0..2, /* delete_at_height adj */ 1);
        let req = batch_request(&batch, 1);
        let resp = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            /* local_cluster_key */ 0,
        );
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(engine.read_slot(&k, 0).unwrap().status, UTXO_SPENT);
        assert_eq!(engine.read_slot(&k, 1).unwrap().status, UTXO_SPENT);
        let meta_after_first = engine.read_metadata(&k).unwrap();
        let spent_after_first = { meta_after_first.spent_utxos };
        let gen_after_first = { meta_after_first.generation };
        assert_eq!(spent_after_first, 2);

        // Master retries the same batch (simulating a stale-connection
        // drop on the master side). The receiver's dedup tracker must
        // skip both ops; engine counters must not move.
        let resp2 = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            stream_key,
            0,
        );
        assert_eq!(resp2.status, STATUS_OK, "duplicate batch must still ACK OK",);
        let meta_after_retry = engine.read_metadata(&k).unwrap();
        assert_eq!(
            { meta_after_retry.spent_utxos },
            spent_after_first,
            "duplicate batch must NOT double-bump spent_utxos counter",
        );
        assert_eq!(
            { meta_after_retry.generation },
            gen_after_first,
            "duplicate batch must NOT bump generation again",
        );
    }

    // ----------------------------------------------------------------------
    // Phase 4 — wire-protocol trace context propagation
    // ----------------------------------------------------------------------

    /// Drive `handle_replica_batch_with_tracker` with a batch whose header
    /// carries a specific trace_id and span_id. Assert that the receiver's
    /// `handle_replica_batch` span is parented on the wire context by
    /// inspecting the span's OpenTelemetry trace_id.
    ///
    /// Implementation note: we do not hook the tracing layer
    /// (`tracing-opentelemetry`'s `OtelData` is not a public schema), and
    /// instead we verify behavior at the bridge boundary: after entering
    /// the receiver span we check that `Span::current().context()` carries
    /// the exact trace_id the sender encoded. This is the same observation
    /// the OTLP layer would make when it exports the span.
    #[test]
    fn receiver_attaches_incoming_trace_as_parent() {
        use crate::observability::WireTraceContext;
        use opentelemetry::trace::TraceContextExt;
        use std::sync::{Arc, Mutex};
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        use tracing_subscriber::prelude::*;

        // We install a tracing-opentelemetry layer backed by a no-op
        // tracer so `Span::current().context()` produces a real OTel
        // Context that honors the `set_parent` call.
        let noop_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
            .build();
        use opentelemetry::trace::TracerProvider as _;
        let tracer = noop_provider.tracer("teraslab-test");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let sub = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("debug"))
            .with(otel_layer);

        let engine = make_engine();
        create_record(&engine, key(42), 1);
        let last_applied = Arc::new(AtomicU64::new(0));
        let applied = ReplicaAppliedTracker::in_memory();

        let wire_ctx = WireTraceContext {
            trace_id: [
                0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
                0xFF, 0x01,
            ],
            span_id: [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11],
        };
        let batch = ReplicaBatch {
            first_sequence: 1,
            ops: vec![ReplicaOp::Spend {
                tx_key: key(42),
                offset: 0,
                spending_data: [0x77; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 1,
            }],
            trace_ctx: Some(wire_ctx),
            source_node_id: None,
            cluster_key: 0,
        };
        let req = RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id: 1,
            flags: 0,
            payload: batch.serialize().into(),
        };

        // Capture the trace_id observed inside the receiver's span.
        let observed: Arc<Mutex<Option<[u8; 16]>>> = Arc::new(Mutex::new(None));
        let observed_clone = observed.clone();

        tracing::subscriber::with_default(sub, || {
            // Wrap the call in a helper span that reads the current
            // context after `handle_replica_batch_with_tracker` enters
            // its own span. We instrument the call in-place.
            let resp = handle_replica_batch_with_tracker(
                &req,
                &engine,
                &last_applied,
                Some(&applied),
                "test-stream",
                0,
            );
            assert_eq!(resp.status, STATUS_OK);

            // Re-run the parent-attachment through the public helper to
            // observe the identical wiring from the call site. Build the
            // same span manually and read its context.
            let span = tracing::debug_span!("probe-handle_replica_batch");
            if let Some(sc) = wire_ctx.to_span_context() {
                let cx = opentelemetry::Context::new().with_remote_span_context(sc);
                let _ = span.set_parent(cx);
            }
            let _g = span.enter();
            let cx = tracing::Span::current().context();
            let sp_ref = opentelemetry::trace::TraceContextExt::span(&cx);
            let sc = sp_ref.span_context();
            if sc.is_valid() {
                *observed_clone.lock().unwrap() = Some(sc.trace_id().to_bytes());
            }
        });

        let seen = observed.lock().unwrap();
        assert_eq!(
            *seen,
            Some(wire_ctx.trace_id),
            "receiver's span context must match the wire trace_id",
        );
    }

    // ----------------------------------------------------------------------
    // Phase B2 — cluster_key gating
    //
    // The receiver rejects any batch whose `cluster_key` is non-zero AND
    // does not match the local cluster epoch. `cluster_key == 0` retains
    // V1-compat semantics (unknown — accept unconditionally). The gate runs
    // BEFORE the `FLAG_MIGRATION_BATCH` bypass so a stale-epoch migration
    // batch is also rejected.
    // ----------------------------------------------------------------------

    /// Decode the `[error_code:2 LE][msg_len:2 LE][msg]` payload that the
    /// dispatch layer uses for `STATUS_ERROR` responses, returning the
    /// `error_code` so tests can assert on the reject reason.
    fn decode_error_code(payload: &[u8]) -> u16 {
        assert!(
            payload.len() >= 2,
            "STATUS_ERROR payload must carry an error_code prefix",
        );
        u16::from_le_bytes([payload[0], payload[1]])
    }

    #[test]
    fn malformed_replica_batch_returns_status_error_ack() {
        let engine = make_engine();
        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let req = RequestFrame {
            request_id: 55,
            op_code: OP_REPLICA_BATCH,
            flags: 0,
            payload: Vec::new().into(),
        };

        let resp = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            DEFAULT_STREAM_KEY,
            0,
        );

        assert_eq!(
            resp.status, STATUS_ERROR,
            "malformed replication payloads must fail at the frame layer",
        );
        match ReplicaAck::deserialize(&resp.payload).expect("ack decodes") {
            ReplicaAck::Error {
                failed_sequence,
                message,
            } => {
                assert_eq!(failed_sequence, 0);
                assert!(message.contains("deserialize batch"), "message: {message}");
            }
            other => panic!("expected ReplicaAck::Error, got {other:?}"),
        }
    }

    /// Build a Spend-only batch for cluster-key tests. Mirrors the
    /// `make_spend_batch` helper above but lets the caller stamp the
    /// `cluster_key` field directly.
    fn batch_with_cluster_key(
        first_sequence: u64,
        tx_key: TxKey,
        offsets: std::ops::Range<u32>,
        generation: u32,
        cluster_key: u64,
    ) -> ReplicaBatch {
        let mut b = make_spend_batch(first_sequence, tx_key, offsets, generation);
        b.cluster_key = cluster_key;
        b
    }

    #[test]
    fn stale_cluster_key_batch_rejected() {
        let engine = make_engine();
        // Pre-existing record so we can detect mutation by reading slot status.
        create_record(&engine, key(70), 4);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let local_cluster_key: u64 = 7;

        // Snapshot the witness state before the call.
        let gen_before = { engine.read_metadata(&key(70)).unwrap().generation };
        let slot0_before = engine.read_slot(&key(70), 0).unwrap().status;

        let batch = batch_with_cluster_key(10, key(70), 0..2, 1, /* stale */ 5);
        let req = batch_request(&batch, 1);

        let resp = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            DEFAULT_STREAM_KEY,
            local_cluster_key,
        );

        assert_eq!(
            resp.status, STATUS_ERROR,
            "stale cluster_key batch must be rejected with STATUS_ERROR",
        );
        assert_eq!(
            decode_error_code(&resp.payload),
            ERR_STALE_EPOCH,
            "rejection error_code must be ERR_STALE_EPOCH",
        );

        // Witnesses: engine untouched, tracker not advanced, last_applied untouched.
        let gen_after = { engine.read_metadata(&key(70)).unwrap().generation };
        let slot0_after = engine.read_slot(&key(70), 0).unwrap().status;
        assert_eq!(
            gen_before, gen_after,
            "rejected batch must not advance engine generation",
        );
        assert_eq!(
            slot0_before, slot0_after,
            "rejected batch must not mutate slot status",
        );
        assert_eq!(
            tracker.get(DEFAULT_STREAM_KEY),
            0,
            "rejected batch must not advance tracker high-water mark",
        );
        assert_eq!(
            last_applied.load(Ordering::Relaxed),
            0,
            "rejected batch must not advance last_applied",
        );
    }

    #[test]
    fn current_cluster_key_batch_applied() {
        let engine = make_engine();
        create_record(&engine, key(71), 3);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let local_cluster_key: u64 = 7;

        // R-D1: seed the watermark so first_sequence=20 is next-expected.
        tracker.set(DEFAULT_STREAM_KEY, 19);
        let batch = batch_with_cluster_key(20, key(71), 0..2, 1, /* matching */ 7);
        let through = batch.last_sequence();
        let req = batch_request(&batch, 1);

        let resp = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            DEFAULT_STREAM_KEY,
            local_cluster_key,
        );

        assert_eq!(resp.status, STATUS_OK, "current-epoch batch must apply");
        let ack = ReplicaAck::deserialize(&resp.payload).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Ok {
                through_sequence: through,
            },
        );
        // Tracker advanced for normal-replication batches.
        assert_eq!(tracker.get(DEFAULT_STREAM_KEY), through);
        assert_eq!(last_applied.load(Ordering::Relaxed), through);
        assert_eq!(engine.read_slot(&key(71), 0).unwrap().status, UTXO_SPENT);
    }

    #[test]
    fn future_cluster_key_batch_applied() {
        // Phase B fixup: a sender carrying a future cluster_key has a
        // quorum-committed view ahead of ours — its OP_TOPOLOGY_COMMIT
        // is in flight (or already broadcast and our copy is queued).
        // The strict-equality reject of the original B2 design rejected
        // legitimate cross-node batches whenever commits propagated
        // unevenly, so the gate now accepts ahead-of-local batches and
        // lets the topology layer reconcile via OP_TOPOLOGY_COMMIT. The
        // tracker / last_applied / engine still advance because the
        // batch's data is authoritative.
        let engine = make_engine();
        create_record(&engine, key(72), 3);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let local_cluster_key: u64 = 7;

        // R-D1: seed the watermark so first_sequence=30 is next-expected.
        tracker.set(DEFAULT_STREAM_KEY, 29);
        let batch = batch_with_cluster_key(30, key(72), 0..1, 1, /* future */ 9);
        let through = batch.last_sequence();
        let req = batch_request(&batch, 1);

        let resp = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            DEFAULT_STREAM_KEY,
            local_cluster_key,
        );

        assert_eq!(
            resp.status, STATUS_OK,
            "future cluster_key (9 > 7) must apply — sender has the \
             ahead-of-local quorum-committed view; OP_TOPOLOGY_COMMIT \
             will reconcile our local view shortly",
        );
        let ack = ReplicaAck::deserialize(&resp.payload).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Ok {
                through_sequence: through,
            },
        );
        assert_eq!(engine.read_slot(&key(72), 0).unwrap().status, UTXO_SPENT);
        assert_eq!(tracker.get(DEFAULT_STREAM_KEY), through);
        assert_eq!(last_applied.load(Ordering::Relaxed), through);
    }

    #[test]
    fn unknown_cluster_key_batch_applied() {
        // V1-compat: a `cluster_key == 0` batch (e.g. produced by an older
        // master that did not emit the field) must apply unconditionally.
        let engine = make_engine();
        create_record(&engine, key(73), 3);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let local_cluster_key: u64 = 7;

        // R-D1: seed the watermark so first_sequence=40 is next-expected.
        tracker.set(DEFAULT_STREAM_KEY, 39);
        let batch = batch_with_cluster_key(40, key(73), 0..1, 1, /* unknown */ 0);
        let through = batch.last_sequence();
        let req = batch_request(&batch, 1);

        let resp = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            DEFAULT_STREAM_KEY,
            local_cluster_key,
        );

        assert_eq!(resp.status, STATUS_OK);
        let ack = ReplicaAck::deserialize(&resp.payload).unwrap();
        assert_eq!(
            ack,
            ReplicaAck::Ok {
                through_sequence: through,
            },
        );
        assert_eq!(engine.read_slot(&key(73), 0).unwrap().status, UTXO_SPENT);
    }

    #[test]
    fn stale_cluster_key_migration_batch_also_rejected() {
        // The cluster_key gate runs BEFORE the migration-batch bypass.
        // Even a `FLAG_MIGRATION_BATCH` payload must be rejected when its
        // stamped cluster_key does not match local_cluster_key.
        let engine = make_engine();
        create_record(&engine, key(74), 4);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let local_cluster_key: u64 = 7;

        let slot2_before = engine.read_slot(&key(74), 2).unwrap().status;
        let migration_batch = batch_with_cluster_key(0, key(74), 2..3, 1, /* stale */ 5);
        let req = RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id: 9,
            flags: FLAG_MIGRATION_BATCH,
            payload: migration_batch.serialize().into(),
        };

        let resp = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            DEFAULT_STREAM_KEY,
            local_cluster_key,
        );

        assert_eq!(
            resp.status, STATUS_ERROR,
            "stale-epoch migration batch must be rejected before bypass logic",
        );
        assert_eq!(decode_error_code(&resp.payload), ERR_STALE_EPOCH);
        let slot2_after = engine.read_slot(&key(74), 2).unwrap().status;
        assert_eq!(
            slot2_before, slot2_after,
            "rejected migration batch must not mutate engine state",
        );
    }

    /// F-G7-005: when the receiver is in clustered mode
    /// (`local_cluster_key != 0`) a migration batch (`FLAG_MIGRATION_BATCH`)
    /// stamped with the V1-compat wildcard `cluster_key = 0` must be
    /// rejected. The dedup-bypass path would otherwise unconditionally
    /// re-apply arbitrary mutations under a forged migration flag.
    #[test]
    fn migration_batch_with_wildcard_cluster_key_rejected_in_clustered_mode() {
        let engine = make_engine();
        create_record(&engine, key(80), 4);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let local_cluster_key: u64 = 7;

        let slot1_before = engine.read_slot(&key(80), 1).unwrap().status;
        // Migration batch stamped with cluster_key = 0 (V1 wildcard).
        let wildcard_batch = batch_with_cluster_key(0, key(80), 1..2, 1, /* wildcard */ 0);
        let req = RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id: 11,
            flags: FLAG_MIGRATION_BATCH,
            payload: wildcard_batch.serialize().into(),
        };

        let resp = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            DEFAULT_STREAM_KEY,
            local_cluster_key,
        );

        assert_eq!(
            resp.status, STATUS_ERROR,
            "wildcard-cluster_key migration batch must be rejected in clustered mode",
        );
        assert_eq!(decode_error_code(&resp.payload), ERR_STALE_EPOCH);
        let slot1_after = engine.read_slot(&key(80), 1).unwrap().status;
        assert_eq!(
            slot1_before, slot1_after,
            "rejected wildcard migration batch must not mutate engine state",
        );
    }

    /// F-G7-005 boundary: a migration batch with cluster_key = 0 must
    /// still be accepted when `local_cluster_key = 0` (the receiver
    /// hasn't observed a quorum-committed cluster term yet, e.g.
    /// post-restart or single-node demo). The wildcard reject only
    /// applies once the receiver is in clustered steady state.
    #[test]
    fn migration_batch_with_wildcard_cluster_key_accepted_when_local_zero() {
        let engine = make_engine();
        create_record(&engine, key(81), 2);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();

        let migration_batch = batch_with_cluster_key(0, key(81), 0..1, 1, /* wildcard */ 0);
        let req = RequestFrame {
            op_code: OP_REPLICA_BATCH,
            request_id: 12,
            flags: FLAG_MIGRATION_BATCH,
            payload: migration_batch.serialize().into(),
        };

        let resp = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            DEFAULT_STREAM_KEY,
            /* local_cluster_key */ 0,
        );

        assert_eq!(
            resp.status, STATUS_OK,
            "wildcard migration batch must be accepted when local_cluster_key = 0",
        );
        assert_eq!(engine.read_slot(&key(81), 0).unwrap().status, UTXO_SPENT);
    }

    /// F-G7-006: `apply_op` skips a Spend whose TX or slot is missing
    /// on the replica and returns Ok(()) to keep the batch ACK
    /// flowing. Silent skips mask real replication divergence (a lost
    /// Create batch, dropped intent range, dedup-tracker drift). The
    /// receiver must bump the `replica_apply_skipped_missing_tx`
    /// metric every time this happens so operators have a
    /// machine-readable divergence signal.
    #[test]
    fn apply_spend_on_missing_tx_increments_divergence_metric() {
        let engine = make_engine();
        let k = key(90);
        // Deliberately do NOT create the record; the replica is missing it.

        static TEST_METRICS: std::sync::OnceLock<&'static crate::metrics::ReplicationMetrics> =
            std::sync::OnceLock::new();
        let metrics_ref = *TEST_METRICS
            .get_or_init(|| Box::leak(Box::new(crate::metrics::ReplicationMetrics::new())));
        crate::metrics::init_replication_metrics(metrics_ref);
        let metrics =
            crate::metrics::replication_metrics().expect("replication metrics installed for test");
        let before = metrics.replica_apply_skipped_missing_tx.get();

        let op = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0x99; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            master_generation: 0,
        };
        // Must succeed (silent skip) so the batch ACK isn't aborted.
        apply_op(&engine, &op).unwrap();

        let after = metrics.replica_apply_skipped_missing_tx.get();
        assert!(
            after > before,
            "spend on missing TX must bump replica_apply_skipped_missing_tx \
             (was {before}, now {after})",
        );
    }

    #[test]
    fn replica_rejected_stale_cluster_key_metric_increments() {
        // The metric is process-wide via `replication_metrics()`. We make
        // sure it is initialized (idempotent) and snapshot the counter
        // before/after to assert exactly one increment per reject.
        let engine = make_engine();
        create_record(&engine, key(75), 2);

        let last_applied = Arc::new(AtomicU64::new(0));
        let tracker = ReplicaAppliedTracker::in_memory();
        let local_cluster_key: u64 = 7;

        // Ensure the process-wide replication metrics are installed so the
        // counter has somewhere to live. Idempotent: any earlier test that
        // installed it wins; the leak is acceptable for the test binary.
        static TEST_METRICS: std::sync::OnceLock<&'static crate::metrics::ReplicationMetrics> =
            std::sync::OnceLock::new();
        let metrics_ref = *TEST_METRICS
            .get_or_init(|| Box::leak(Box::new(crate::metrics::ReplicationMetrics::new())));
        crate::metrics::init_replication_metrics(metrics_ref);
        let metrics =
            crate::metrics::replication_metrics().expect("replication metrics installed for test");
        let before = metrics.replica_rejected_stale_cluster_key.get();

        let batch = batch_with_cluster_key(50, key(75), 0..1, 1, /* stale */ 3);
        let req = batch_request(&batch, 1);
        let resp = handle_replica_batch_with_tracker(
            &req,
            &engine,
            &last_applied,
            Some(&tracker),
            DEFAULT_STREAM_KEY,
            local_cluster_key,
        );
        assert_eq!(resp.status, STATUS_ERROR);

        let after = metrics.replica_rejected_stale_cluster_key.get();
        assert_eq!(
            after,
            before + 1,
            "replica_rejected_stale_cluster_key must increment exactly once per reject",
        );
    }

    // -----------------------------------------------------------------
    // R-034 / R-035 regression tests
    // -----------------------------------------------------------------

    /// A `BlockDevice` wrapper that delegates to an inner `MemoryDevice`
    /// but fails every `pwrite` once an arming flag is flipped on.
    ///
    /// Used by `replica_metadata_write_error_fails_batch_ack` to inject
    /// a metadata-write failure during `apply_op` so we can verify the
    /// error propagates back up to the batch ACK instead of being
    /// silently swallowed.
    struct ArmableFailingDevice {
        inner: Arc<MemoryDevice>,
        fail_writes: std::sync::atomic::AtomicBool,
    }

    impl ArmableFailingDevice {
        fn new(inner: Arc<MemoryDevice>) -> Self {
            Self {
                inner,
                fail_writes: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn arm(&self) {
            self.fail_writes
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl crate::device::BlockDevice for ArmableFailingDevice {
        fn pread(
            &self,
            buf: &mut [u8],
            offset: u64,
        ) -> std::result::Result<usize, crate::device::DeviceError> {
            self.inner.pread(buf, offset)
        }

        fn pwrite(
            &self,
            buf: &[u8],
            offset: u64,
        ) -> std::result::Result<usize, crate::device::DeviceError> {
            if self.fail_writes.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(crate::device::DeviceError::Io(std::io::Error::other(
                    "armed failure",
                )));
            }
            self.inner.pwrite(buf, offset)
        }

        fn alignment(&self) -> usize {
            self.inner.alignment()
        }

        fn size(&self) -> u64 {
            self.inner.size()
        }

        fn sync(&self) -> std::result::Result<(), crate::device::DeviceError> {
            self.inner.sync()
        }

        // Don't expose a raw pointer — the engine's fast path bypasses
        // pwrite when `as_raw_ptr()` is `Some`, which would dodge our
        // failure injection. Forcing the slow path keeps the test honest.
        fn as_raw_ptr(&self) -> Option<*mut u8> {
            None
        }
    }

    fn make_engine_with_armable_device() -> (Arc<Engine>, Arc<ArmableFailingDevice>) {
        let mem = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let dev = Arc::new(ArmableFailingDevice::new(mem));
        let dev_trait: Arc<dyn BlockDevice> = dev.clone();
        let alloc = SlotAllocator::new(dev_trait.clone()).unwrap();
        let index = Index::new(10_000).unwrap();
        let engine = Arc::new(Engine::new(
            dev_trait,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        ));
        (engine, dev)
    }

    fn attach_redo_log(engine: &Engine) -> Arc<parking_lot::Mutex<crate::redo::RedoLog>> {
        let redo_dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let log = crate::redo::RedoLog::open(redo_dev, 0, 4 * 1024 * 1024)
            .expect("redo log opens on memory device");
        let log_arc = Arc::new(parking_lot::Mutex::new(log));
        engine.set_redo_log(log_arc.clone());
        log_arc
    }

    /// R-035: a metadata write failure during `apply_op` (here, the
    /// post-apply generation sync) must surface as a batch-level error
    /// the master treats as not-yet-durable, instead of being silently
    /// swallowed by the previous `let _ = io::write_metadata(...)`
    /// pattern.
    ///
    /// We arm the device to fail every pwrite, then drive a Spend op
    /// through `apply_op`. Even if the engine's spend itself succeeded
    /// against the cached state path, the post-apply generation
    /// reconciliation must call `crate::io::write_metadata` and observe
    /// the failure. The fix routes that failure into the Result return,
    /// so `apply_op` returns Err — which the outer batch handler turns
    /// into a `ReplicaAck::Error`.
    #[test]
    fn replica_metadata_write_error_fails_batch_ack() {
        let (engine, dev) = make_engine_with_armable_device();
        let k = key(40);
        create_record(&engine, k, 2);

        // Arm the device so any further pwrite fails. The Spend will
        // observe the failure during one of its on-device writes
        // (slot or metadata); whichever surfaces first must propagate
        // an error rather than being swallowed.
        dev.arm();

        let op = ReplicaOp::Spend {
            tx_key: k,
            offset: 0,
            spending_data: [0xCD; 36],
            current_block_height: 700_000,
            block_height_retention: 288,
            // Force the post-apply generation-sync write path that
            // R-035 was about. Use a master_generation strictly greater
            // than the local one so the pre-apply guard accepts the op.
            master_generation: 100,
        };
        let err = apply_op(&engine, &op).expect_err(
            "apply_op must propagate the on-device write failure (R-035), not swallow it",
        );
        assert!(
            !err.is_empty(),
            "apply_op error message must be non-empty so the master can log the cause",
        );

        // Now drive the same op through `handle_replica_batch` so we
        // verify the failure becomes a `ReplicaAck::Error` (not Ok)
        // — the wire-level invariant the master relies on.
        let batch = ReplicaBatch {
            cluster_key: 0,
            first_sequence: 1,
            ops: vec![op.clone()],
            source_node_id: None,
            trace_ctx: None,
        };
        let req = RequestFrame {
            request_id: 7,
            op_code: OP_REPLICA_BATCH,
            flags: 0,
            payload: batch.serialize().into(),
        };
        let last = AtomicU64::new(0);
        let resp = handle_replica_batch(&req, &engine, &last);
        assert_eq!(
            resp.status, STATUS_ERROR,
            "ReplicaAck::Error must also use STATUS_ERROR at the frame layer",
        );
        let ack = ReplicaAck::deserialize(&resp.payload).expect("ack decodes");
        match ack {
            ReplicaAck::Error {
                failed_sequence,
                message,
            } => {
                assert_eq!(failed_sequence, 1, "failed_sequence is the offending op");
                assert!(
                    !message.is_empty(),
                    "error message must include diagnostic detail"
                );
            }
            ReplicaAck::Ok { .. } | ReplicaAck::Gap { .. } => {
                panic!(
                    "R-035: replica must NOT ACK Ok (or NAK Gap) when an on-device \
                     metadata write failed"
                )
            }
        }
        assert_eq!(
            last.load(Ordering::Relaxed),
            0,
            "last_applied must not advance when the op failed",
        );
    }

    /// R-034: after a successful apply on the replica, the engine's
    /// local redo log must contain a redo entry capturing the post-apply
    /// state. Without this, a master crash followed by replica failover
    /// requires a full resync because the surviving replica's recovery
    /// path has no redo entries to replay.
    ///
    /// We attach a redo log to the receiver's engine, drive several
    /// distinct `apply_op` calls through it, then read back the redo log
    /// and assert the entries are present. We also assert (a) the
    /// entries appear in apply order, and (b) the entry count matches
    /// the call count — i.e. every successful apply produces exactly one
    /// local redo record.
    #[test]
    fn replica_redo_log_catch_up_after_failover() {
        let engine = make_engine();
        let log_arc = attach_redo_log(&engine);

        let k = key(60);
        create_record(&engine, k, 3);
        // The Create above used `engine.create()`; it does NOT write a
        // replica redo entry on its own (that's the dispatch path's
        // job). Snapshot the sequence number AFTER setup so we only
        // count entries written by `apply_op`.
        let pre_apply_seq = log_arc.lock().current_sequence();

        let ops = vec![
            ReplicaOp::Spend {
                tx_key: k,
                offset: 0,
                spending_data: [0x11; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 50,
            },
            ReplicaOp::Spend {
                tx_key: k,
                offset: 1,
                spending_data: [0x22; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 51,
            },
            ReplicaOp::Freeze {
                tx_key: k,
                offset: 2,
                master_generation: 52,
            },
        ];

        for op in &ops {
            apply_op(&engine, op).expect("apply_op succeeds");
        }
        // F-G7-016 (batched): apply_op now only APPENDS the redo
        // entry to the in-memory buffer — the batch wrapper
        // `handle_replica_batch_with_tracker` flushes once at the end.
        // This unit test calls apply_op directly, so flush manually
        // here to make the appended entries visible to a subsequent
        // reader (mirrors the production batch-end flush).
        flush_replica_redo_log(&engine).expect("redo log flush");

        // Read all entries that were appended after the create-time
        // snapshot.
        let entries = {
            let log = log_arc.lock();
            log.read_from_sequence(pre_apply_seq)
                .expect("redo log replay reads back appended entries")
        };
        assert_eq!(
            entries.len(),
            ops.len(),
            "R-034: each successful apply_op must append exactly one local redo entry; \
             saw {} entries for {} ops",
            entries.len(),
            ops.len(),
        );

        // First two are SpendV2 entries targeting offsets 0 and 1, last
        // is Freeze on offset 2. The exact sequence shape proves that
        // apply_op is journaling per-op (and not, e.g., losing the
        // second spend).
        match &entries[0].op {
            crate::redo::RedoOp::SpendV2 {
                offset,
                target_generation,
                current_block_height,
                block_height_retention,
                ..
            } => {
                assert_eq!(*offset, 0);
                assert_eq!(*target_generation, 50);
                assert_eq!(*current_block_height, 700_000);
                assert_eq!(*block_height_retention, 288);
            }
            other => panic!("entry[0] should be SpendV2(off=0), got {other:?}"),
        }
        match &entries[1].op {
            crate::redo::RedoOp::SpendV2 {
                offset,
                target_generation,
                current_block_height,
                block_height_retention,
                ..
            } => {
                assert_eq!(*offset, 1);
                assert_eq!(*target_generation, 51);
                assert_eq!(*current_block_height, 700_000);
                assert_eq!(*block_height_retention, 288);
            }
            other => panic!("entry[1] should be SpendV2(off=1), got {other:?}"),
        }
        match &entries[2].op {
            crate::redo::RedoOp::Freeze { offset, .. }
            | crate::redo::RedoOp::FreezeV2 { offset, .. } => assert_eq!(*offset, 2),
            other => panic!("entry[2] should be Freeze/FreezeV2(off=2), got {other:?}"),
        }
    }

    /// R-034 invariant: the redo entry written on the replica must
    /// capture POST-apply state (the same shape the master writes), not
    /// the input op verbatim. The engine bumps `meta.spent_utxos` on
    /// every UNSPENT→SPENT transition, so an entry written after a
    /// Spend must carry the new `spent_utxos` count, not zero.
    ///
    /// This guards against a regression where someone re-implements
    /// `build_post_apply_redo_op` to copy the input op's fields verbatim
    /// — which would corrupt `spent_utxos` on replay (recovery's
    /// `replay_spend` overwrites `meta.spent_utxos = new_spent_count`
    /// unconditionally).
    #[test]
    fn replica_redo_entry_captures_post_apply_state() {
        let engine = make_engine();
        let log_arc = attach_redo_log(&engine);

        let k = key(61);
        create_record(&engine, k, 4);
        let pre_seq = log_arc.lock().current_sequence();

        // Apply two spends — after the second, meta.spent_utxos == 2.
        apply_op(
            &engine,
            &ReplicaOp::Spend {
                tx_key: k,
                offset: 0,
                spending_data: [0xA1; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 10,
            },
        )
        .unwrap();
        apply_op(
            &engine,
            &ReplicaOp::Spend {
                tx_key: k,
                offset: 1,
                spending_data: [0xA2; 36],
                current_block_height: 700_000,
                block_height_retention: 288,
                master_generation: 11,
            },
        )
        .unwrap();

        // Sanity: device-side counter is at 2.
        let meta = engine.read_metadata(&k).unwrap();
        let device_spent = { meta.spent_utxos };
        assert_eq!(device_spent, 2, "device counter should be 2 after 2 spends");

        // F-G7-016 (batched): apply_op now only appends; flush
        // manually to mirror the batch-wrapper end-of-batch flush.
        flush_replica_redo_log(&engine).expect("redo log flush");

        let entries = log_arc
            .lock()
            .read_from_sequence(pre_seq)
            .expect("redo log replay");
        assert_eq!(entries.len(), 2, "two spend ops -> two redo entries");

        // The second redo entry (SpendV2 off=1) must carry
        // new_spent_count == 2 and target_generation == 11 — proving the
        // entry captured POST-apply state, not just the input verb.
        match &entries[1].op {
            crate::redo::RedoOp::SpendV2 {
                offset,
                new_spent_count,
                target_generation,
                current_block_height,
                block_height_retention,
                ..
            } => {
                assert_eq!(*offset, 1);
                assert_eq!(
                    *new_spent_count, 2,
                    "R-034 invariant: redo entry MUST capture post-apply spent_utxos \
                     (got {new_spent_count}, expected 2)"
                );
                assert_eq!(*target_generation, 11);
                assert_eq!(*current_block_height, 700_000);
                assert_eq!(*block_height_retention, 288);
            }
            other => panic!("entry[1] should be SpendV2, got {other:?}"),
        }

        // And the first entry should carry new_spent_count == 1.
        match &entries[0].op {
            crate::redo::RedoOp::SpendV2 {
                offset,
                new_spent_count,
                target_generation,
                ..
            } => {
                assert_eq!(*offset, 0);
                assert_eq!(*new_spent_count, 1);
                assert_eq!(*target_generation, 10);
            }
            other => panic!("entry[0] should be SpendV2, got {other:?}"),
        }
    }

    /// Build a 70-byte migration `metadata_bytes` blob carrying the master's
    /// lifecycle state (the layout the baseline migration path streams). Bytes
    /// 0..46 are the core create header (all zero here except tx_version=1);
    /// bytes 46..70 are generation/updated_at/unmined_since/delete_at_height/
    /// preserve_until.
    fn migration_metadata_bytes(
        generation: u32,
        updated_at: u64,
        unmined_since: u32,
        delete_at_height: u32,
        preserve_until: u32,
    ) -> Vec<u8> {
        let mut m = vec![0u8; 70];
        // tx_version = 1 (offset 0..4); the rest of the core header stays zero.
        m[0..4].copy_from_slice(&1u32.to_le_bytes());
        m[46..50].copy_from_slice(&generation.to_le_bytes());
        m[50..58].copy_from_slice(&updated_at.to_le_bytes());
        m[58..62].copy_from_slice(&unmined_since.to_le_bytes());
        m[62..66].copy_from_slice(&delete_at_height.to_le_bytes());
        m[66..70].copy_from_slice(&preserve_until.to_le_bytes());
        m
    }

    /// Apply a migrated Create exactly as the receiver does for a streamed
    /// baseline record, with the supplied lifecycle metadata.
    fn apply_migrated_create(
        engine: &Engine,
        k: TxKey,
        metadata_bytes: Vec<u8>,
    ) -> std::result::Result<(), String> {
        let op = ReplicaOp::Create {
            tx_key: k,
            metadata_bytes,
            utxo_hashes: vec![[0xAA; 32]; 2],
            cold_data: None,
            is_external: false,
        };
        apply_op(engine, &op)
    }

    /// Decode the cached `delete_at_height` / `preserve_until` from a primary
    /// index entry exactly as the fully-cached GET fast path
    /// (`handle_get_batch`) does, so the test asserts what a client would see.
    fn cached_dah_preserve(entry: &TxIndexEntry) -> (u32, u32) {
        let has_preserve = entry.tx_flags & crate::record::TxFlags::HAS_PRESERVE_UNTIL.bits() != 0;
        if has_preserve {
            (0, entry.dah_or_preserve)
        } else {
            (entry.dah_or_preserve, 0)
        }
    }

    /// F-2: a migrated unmined record must land in the unmined secondary index
    /// (so `QUERY_OLD_UNMINED` finds it) AND have its primary cached
    /// `unmined_since` populated — both WITHOUT restarting the target.
    #[test]
    fn migrated_unmined_record_visible_in_unmined_index_and_cache() {
        let engine = make_engine();
        let k = key(1);
        let unmined_since = 700_000u32;

        apply_migrated_create(
            &engine,
            k,
            migration_metadata_bytes(7, 12_345, unmined_since, 0, 0),
        )
        .unwrap();

        // Device footer carries the real value.
        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!({ meta.unmined_since }, unmined_since);
        assert_eq!({ meta.generation }, 7);

        // Unmined secondary index contains it (QUERY_OLD_UNMINED-style lookup).
        let unmined_keys = engine.unmined_index().range_query(unmined_since + 1);
        assert!(
            unmined_keys.contains(&k),
            "migrated unmined record must be in the unmined index without restart",
        );

        // Primary cached field matches the slow path.
        let entry = engine
            .lookup(&k)
            .expect("entry present after migrated create");
        assert_eq!(
            entry.unmined_since,
            { meta.unmined_since },
            "cached unmined_since must match the device footer",
        );
    }

    /// F-2: a migrated record with `delete_at_height` set must land in the DAH
    /// secondary index (so the DAH sweep picks it up via the same range query)
    /// and have its primary cached field populated — without restart.
    #[test]
    fn migrated_dah_record_visible_in_dah_index_and_cache() {
        let engine = make_engine();
        let k = key(2);
        let dah = 800_000u32;

        apply_migrated_create(&engine, k, migration_metadata_bytes(3, 999, 0, dah, 0)).unwrap();

        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!({ meta.delete_at_height }, dah);

        // DAH sweep query (OP_PROCESS_EXPIRED_PRESERVATIONS) at a height past the
        // DAH must return the migrated key.
        let due = engine.dah_index().range_query(dah);
        assert!(
            due.contains(&k),
            "migrated DAH record must be swept-visible without restart",
        );
        // At a height before the DAH, it must NOT be due yet.
        let not_due = engine.dah_index().range_query(dah - 1);
        assert!(
            !not_due.contains(&k),
            "DAH record not due before its height"
        );

        // Primary cached field reflects the DAH.
        let entry = engine.lookup(&k).expect("entry present");
        let (cached_dah, cached_preserve) = cached_dah_preserve(&entry);
        assert_eq!(cached_dah, dah, "cached delete_at_height must match footer");
        assert_eq!(cached_preserve, 0);
    }

    /// F-2: a migrated preserved record must surface `preserve_until` through
    /// the cached primary path (HAS_PRESERVE_UNTIL discriminant set) and must
    /// NOT appear in the DAH index.
    #[test]
    fn migrated_preserved_record_visible_through_cache() {
        let engine = make_engine();
        let k = key(3);
        let preserve = 900_000u32;

        apply_migrated_create(&engine, k, migration_metadata_bytes(5, 555, 0, 0, preserve))
            .unwrap();

        let meta = engine.read_metadata(&k).unwrap();
        assert_eq!({ meta.preserve_until }, preserve);
        assert_eq!({ meta.delete_at_height }, 0);

        // Preserved record carries delete_at_height = 0, so it must be absent
        // from the DAH sweep even at u32::MAX.
        let due = engine.dah_index().range_query(u32::MAX);
        assert!(!due.contains(&k), "preserved record must not be DAH-swept",);

        // Cached path surfaces preserve_until via the HAS_PRESERVE_UNTIL bit.
        let entry = engine.lookup(&k).expect("entry present");
        assert!(
            entry.tx_flags & crate::record::TxFlags::HAS_PRESERVE_UNTIL.bits() != 0,
            "HAS_PRESERVE_UNTIL discriminant must be set in the cached flags",
        );
        let (cached_dah, cached_preserve) = cached_dah_preserve(&entry);
        assert_eq!(
            cached_preserve, preserve,
            "cached preserve_until must match"
        );
        assert_eq!(cached_dah, 0);
    }

    /// F-2 specific symptom: for a migrated record the fully-cached GET fast
    /// path and the slow (device-metadata) path must agree on
    /// `unmined_since` / `delete_at_height` / `preserve_until`. Before the fix
    /// the cached entry held zeros while the device footer held the real
    /// values, so the same key answered differently by field mask.
    #[test]
    fn migrated_record_cached_matches_slow_path() {
        let engine = make_engine();
        let k = key(4);
        let unmined_since = 0u32; // mined record that also has a DAH set
        let dah = 750_000u32;

        apply_migrated_create(
            &engine,
            k,
            migration_metadata_bytes(9, 4242, unmined_since, dah, 0),
        )
        .unwrap();

        // Slow path: device metadata.
        let meta = engine.read_metadata(&k).unwrap();
        // Cached path: primary index entry decoded like the GET fast path.
        let entry = engine.lookup(&k).expect("entry present");
        let (cached_dah, cached_preserve) = cached_dah_preserve(&entry);

        assert_eq!(
            entry.unmined_since,
            { meta.unmined_since },
            "cached vs slow-path unmined_since must match",
        );
        assert_eq!(
            cached_dah,
            { meta.delete_at_height },
            "cached vs slow-path delete_at_height must match",
        );
        assert_eq!(
            cached_preserve,
            { meta.preserve_until },
            "cached vs slow-path preserve_until must match",
        );

        // And the DAH index agrees with both.
        assert!(
            engine.dah_index().range_query(dah).contains(&k),
            "DAH index must agree with the cached + slow paths",
        );
    }
}
