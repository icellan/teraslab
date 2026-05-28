//! SWIM-style UDP membership protocol.
//!
//! Each node periodically probes a random peer. Membership updates are
//! piggybacked on probe/ack messages. Failure detection uses direct
//! probes with a suspicion timeout.

use crate::cluster::membership::{ClusterEvent, Membership};
use crate::cluster::shards::NodeId;
use crate::metrics::swim_metrics;
use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// SWIM protocol message types.
const MSG_PING: u8 = 1;
const MSG_ACK: u8 = 2;
const MSG_JOIN: u8 = 3;
/// Indirect probe request: "please probe this node for me".
const MSG_PING_REQ: u8 = 4;
/// Relayed result of an indirect probe: ACK from the probed target, forwarded by the relay.
/// Wire layout matches [`SwimRunner::encode_message`] piggyback, then `[probed_target_id:8]`,
/// then committed term (see [`SwimRunner::handle_message`]).
const MSG_INDIRECT_ACK: u8 = 5;

/// On-wire message format:
/// [msg_type:1][sender_id:8][sender_incarnation:8][sender_seq:8][sender_addr_len:2][sender_addr:N]
/// [update_count:2][ [node_id:8][state:1][incarnation:8][addr_len:2][addr:N] × count ]
///
/// `sender_seq` is a per-sender monotonic counter that the recipient
/// uses for replay defense (F-G8-003). The HMAC envelope covers the
/// entire payload — including the seq — so an attacker cannot alter the
/// seq without invalidating the tag. The recipient tracks per-peer
/// `(highest_seq, window_bitmap)` in [`SwimRunner::seen_seq`] and rejects
/// any (peer, seq) pair that has been seen before or that falls below
/// the window's left edge.
const MAX_MSG_SIZE: usize = 4096;

/// Maximum number of in-flight PING_REQ forwarding entries kept in
/// [`SwimRunner::ping_req_forwarding`]. Each entry is small (NodeId +
/// SocketAddr + Instant ≈ 32 bytes) but unbounded growth lets a peer
/// that floods PING_REQs for non-existent ids drive a slow memory
/// leak (F-G8-004). At 4096 entries the cap is ~128 KiB, easily two
/// orders of magnitude beyond a healthy steady-state.
const PING_REQ_FORWARDING_MAX: usize = 4096;

/// Number of PING_REQ forwarding entries evicted due to the cap in
/// `PING_REQ_FORWARDING_MAX`. Monotonic counter, process-lifetime.
///
/// Thin backward-compat wrapper around
/// [`crate::metrics::SwimMetrics::swim_ping_req_dropped_total`] (the
/// canonical home — P2.4). Returns 0 if `init_swim_metrics` has not
/// been called yet (pre-boot tests, etc.). Existing callers — chiefly
/// `tests/g8_ping_req_cap.rs` — keep working without import churn.
pub fn ping_req_dropped_total() -> u64 {
    crate::metrics::swim_metrics()
        .map(|m| m.swim_ping_req_dropped_total.get())
        .unwrap_or(0)
}
const MSG_SIZE_WARN_THRESHOLD: usize = MAX_MSG_SIZE * 4 / 5;
const DEAD_MEMBER_FORGET_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EncodedMessageSize {
    Normal,
    NearLimit,
    Oversize,
}

fn classify_encoded_message_size(len: usize) -> EncodedMessageSize {
    if len > MAX_MSG_SIZE {
        EncodedMessageSize::Oversize
    } else if len >= MSG_SIZE_WARN_THRESHOLD {
        EncodedMessageSize::NearLimit
    } else {
        EncodedMessageSize::Normal
    }
}

fn observe_encoded_message_size(msg_type: u8, len: usize) {
    match classify_encoded_message_size(len) {
        EncodedMessageSize::Normal => {}
        EncodedMessageSize::NearLimit => {
            tracing::warn!(
                msg_type,
                len,
                max = MAX_MSG_SIZE,
                "swim: encoded message is near UDP frame cap",
            );
        }
        EncodedMessageSize::Oversize => {
            tracing::warn!(
                msg_type,
                len,
                max = MAX_MSG_SIZE,
                "swim: encoded message exceeds UDP receive buffer cap; peer may truncate/drop it",
            );
        }
    }
    debug_assert!(
        len <= MAX_MSG_SIZE,
        "SWIM message type {msg_type} encoded to {len} bytes, above MAX_MSG_SIZE {MAX_MSG_SIZE}",
    );
}

/// Configuration for the SWIM protocol.
#[derive(Debug, Clone)]
pub struct SwimConfig {
    pub self_id: NodeId,
    pub self_addr: SocketAddr,
    pub bind_addr: SocketAddr,
    pub seed_nodes: Vec<SocketAddr>,
    pub probe_interval: Duration,
    pub suspicion_timeout: Duration,
    /// Shared secret for HMAC-SHA256 message authentication.
    /// When set, outgoing messages are signed and incoming messages
    /// without a valid signature are silently dropped.
    pub cluster_secret: Option<Vec<u8>>,
    /// Persisted incarnation from the previous run.
    /// The SWIM runner will start from `persisted_incarnation + 1`.
    pub persisted_incarnation: u64,
    /// Shared reference to the topology authority's committed term.
    /// Piggybacked on gossip messages so lagging nodes can detect
    /// they're behind and trigger a catch-up.
    pub committed_term: Arc<std::sync::atomic::AtomicU64>,
}

/// State of a pending direct probe awaiting an ACK.
struct PendingProbe {
    /// The node we are probing.
    target: NodeId,
    /// When the probe was sent.
    started: Instant,
    /// Whether indirect (PING_REQ) probes have been sent.
    indirect_sent: bool,
    /// Number of indirect probe rounds attempted. Used by the exponential
    /// backoff in the suspect-timeout path so a transiently slow peer is
    /// not immediately marked suspect on the first ping-req failure.
    indirect_attempts: u32,
}

/// Number of indirect probe peers to ask when direct probe fails.
const INDIRECT_PROBE_K: usize = 3;

/// Return a jittered probe interval in `[0.75 * base, 1.25 * base]`.
///
/// Applies ±25% uniform jitter around the configured probe interval so
/// consecutive probe cycles don't align across peers — lockstep probing
/// creates network hot-spots and degrades failure-detection latency under
/// packet loss. The random draw uses Rust's per-thread CSPRNG via
/// [`getrandom`]-style [`std::hash::RandomState`] seeding, which is
/// sufficient: we only need an unbiased uniform draw, not cryptographic
/// unpredictability.
fn jittered_probe_interval(base: Duration) -> Duration {
    // Draw a u32 via the RandomState-seeded hasher trick: hashing an
    // empty tuple with a freshly-seeded RandomState yields a pseudo-random
    // u64 on every call. This avoids pulling in the `rand` crate for a
    // single uniform draw.
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut h = RandomState::new().build_hasher();
    h.write_u64(base.as_nanos() as u64);
    let r = h.finish();
    // Fraction in [0, 1).
    let frac = (r as f64) / (u64::MAX as f64 + 1.0);
    // Multiplier in [0.75, 1.25).
    let mult = 0.75 + 0.5 * frac;
    // Saturate against Duration overflow for safety.
    let nanos = (base.as_nanos() as f64 * mult) as u128;
    let clamped = nanos.min(u64::MAX as u128) as u64;
    Duration::from_nanos(clamped)
}

/// Compute the suspect-timeout deadline with exponential backoff on
/// repeated indirect probe rounds.
///
/// The first indirect round waits `2 * base` (the original behavior).
/// Each subsequent round doubles that wait, capped at `16 * base`, so a
/// transiently slow peer experiencing brief packet loss is not marked
/// suspect until it has genuinely failed multiple probe rounds.
fn suspect_backoff_delay(base: Duration, indirect_attempts: u32) -> Duration {
    // `indirect_attempts` is the number of completed ping-req rounds;
    // we back off starting from the first one.
    let shift = indirect_attempts.saturating_sub(1).min(3); // cap at 2^3 = 8×
    // Original wait was `base * 2`; add the backoff factor on top.
    let mult = 2u64.saturating_mul(1u64 << shift);
    let nanos = base.as_nanos().saturating_mul(mult as u128);
    let clamped = nanos.min(u64::MAX as u128) as u64;
    Duration::from_nanos(clamped)
}

/// Phase I — exponential backoff for seed-node retry attempts.
///
/// During cluster bootstrap a node retries its configured seed peers
/// when the initial join fails. A fixed retry interval either re-spams
/// the network too aggressively (small interval) or stalls bootstrap
/// for a long time (large interval). This helper computes the delay
/// before retry attempt `attempt` (0-indexed) by doubling `initial`
/// each step and clamping at `max`.
///
/// Examples (initial = 100ms, max = 5s):
/// - attempt 0 → 100ms
/// - attempt 1 → 200ms
/// - attempt 2 → 400ms
/// - …
/// - attempt 6 → 5s   (clamped)
/// - attempt N → 5s   (stays at the cap)
pub fn exponential_seed_backoff(attempt: u32, initial: Duration, max: Duration) -> Duration {
    if initial >= max {
        return max;
    }
    let initial_ms = initial.as_millis() as u64;
    if initial_ms == 0 {
        return max.min(Duration::from_millis(1));
    }
    let shift = attempt.min(63);
    let scaled = initial_ms.checked_shl(shift).unwrap_or(u64::MAX);
    let max_ms = max.as_millis() as u64;
    Duration::from_millis(scaled.min(max_ms))
}

/// Size (in bits) of the sliding replay-defense window kept per peer.
///
/// 256 bits = 32 bytes per peer. Sufficient for any reasonable
/// out-of-order UDP delivery window — SWIM messages are small and the
/// per-sender seq increments monotonically per outgoing message, so the
/// window only needs to absorb in-flight reordering, not multi-second
/// gaps. A peer that legitimately falls more than 256 messages behind
/// is dealing with a network event that warrants rejecting older
/// messages anyway.
const REPLAY_WINDOW_BITS: u64 = 256;

/// Per-peer replay-defense window for SWIM message sequence numbers.
///
/// Tracks the highest `(sender_id, seq)` seen and a bitmap of the
/// preceding `REPLAY_WINDOW_BITS` slots. Each accepted seq must be
/// either:
///
///   * strictly higher than `highest` — the common case for fresh
///     messages; the window slides forward and the previous bit for
///     `highest` is set, or
///   * within `[highest - REPLAY_WINDOW_BITS + 1, highest]` AND not yet
///     marked in the bitmap — covers normal out-of-order UDP delivery.
///
/// Any seq below the left edge of the window, or any seq with its bit
/// already set, is rejected as a replay.
#[derive(Debug, Clone, Default)]
struct ReplayWindow {
    /// Highest seq accepted from this peer.
    highest: u64,
    /// Bitmap of accepted seq positions in `[highest - 255, highest - 1]`.
    /// Bit 0 == `highest - 1`, bit 1 == `highest - 2`, etc.
    bitmap: [u64; 4], // 4 * 64 = 256 bits
}

impl ReplayWindow {
    /// Attempt to record `seq`. Returns true if accepted (not a replay),
    /// false if `seq` has already been seen or is too old.
    fn check_and_record(&mut self, seq: u64) -> bool {
        // First-message-from-peer: always accept and seed `highest`.
        if self.highest == 0 && self.bitmap == [0u64; 4] {
            self.highest = seq;
            return true;
        }

        match seq.cmp(&self.highest) {
            std::cmp::Ordering::Greater => {
                // Fresh seq — slide the window forward by `diff` bits.
                let diff = seq - self.highest;
                if diff >= REPLAY_WINDOW_BITS {
                    // Full shift: drop everything, set only the new
                    // "highest" position (which lives outside the bitmap).
                    self.bitmap = [0u64; 4];
                } else {
                    // Shift left by `diff` positions, then mark the old
                    // `highest` position (now at bit index `diff - 1`).
                    self.shift_left(diff);
                    self.set_bit(diff - 1);
                }
                self.highest = seq;
                true
            }
            std::cmp::Ordering::Equal => false, // exact duplicate
            std::cmp::Ordering::Less => {
                let lag = self.highest - seq;
                if lag > REPLAY_WINDOW_BITS {
                    return false; // below window's left edge
                }
                // `seq` lives at bit index `lag - 1` (0-based from `highest - 1`).
                let bit = lag - 1;
                if self.test_bit(bit) {
                    return false; // already seen
                }
                self.set_bit(bit);
                true
            }
        }
    }

    fn set_bit(&mut self, bit: u64) {
        let idx = (bit / 64) as usize;
        let off = bit % 64;
        if idx < self.bitmap.len() {
            self.bitmap[idx] |= 1u64 << off;
        }
    }

    fn test_bit(&self, bit: u64) -> bool {
        let idx = (bit / 64) as usize;
        let off = bit % 64;
        if idx >= self.bitmap.len() {
            return false;
        }
        self.bitmap[idx] & (1u64 << off) != 0
    }

    fn shift_left(&mut self, shift: u64) {
        if shift == 0 {
            return;
        }
        if shift >= REPLAY_WINDOW_BITS {
            self.bitmap = [0u64; 4];
            return;
        }
        let word_shift = (shift / 64) as usize;
        let bit_shift = shift % 64;
        let mut out = [0u64; 4];
        for i in 0..self.bitmap.len() {
            let src = i;
            let dst = i + word_shift;
            if dst >= out.len() {
                break;
            }
            if bit_shift == 0 {
                out[dst] |= self.bitmap[src];
            } else {
                out[dst] |= self.bitmap[src] << bit_shift;
                if dst + 1 < out.len() {
                    out[dst + 1] |= self.bitmap[src] >> (64 - bit_shift);
                }
            }
        }
        self.bitmap = out;
    }
}

/// A running SWIM protocol instance.
pub struct SwimRunner {
    config: SwimConfig,
    membership: Arc<Mutex<Membership>>,
    /// TCP addresses of peers (used for client routing / migration).
    peer_addrs: Arc<Mutex<HashMap<NodeId, SocketAddr>>>,
    /// SWIM (UDP) addresses of peers, learned from the actual source address
    /// of received UDP packets. Used for sending probes.
    swim_peer_addrs: Arc<Mutex<HashMap<NodeId, SocketAddr>>>,
    shutdown: Arc<AtomicBool>,
    incarnation: u64,
    /// Currently pending direct probe (at most one at a time).
    pending_probe: Option<PendingProbe>,
    /// Index for round-robin peer selection.
    probe_round_robin: usize,
    /// Tracks PING_REQ forwarding: maps (original_requester_addr) to pending
    /// indirect probe targets so we can forward ACKs back.
    ///
    /// Capped at [`PING_REQ_FORWARDING_MAX`] entries (F-G8-004); when the
    /// cap is reached, the oldest entry is evicted to make room. The
    /// FIFO order is tracked by `ping_req_forwarding_order`.
    ping_req_forwarding: HashMap<NodeId, SocketAddr>,
    /// Insertion-order list parallel to `ping_req_forwarding`, used to
    /// evict the oldest entry when the cap is hit. Front = oldest.
    ping_req_forwarding_order: std::collections::VecDeque<NodeId>,
    /// Per-sender monotonic counter for replay defense (F-G8-003).
    ///
    /// Increments once per outgoing SWIM message. The signed payload
    /// carries this value so that peers can reject any replayed packet
    /// whose seq has already been seen (or that falls below their
    /// sliding window). A reboot resumes from `0` — the
    /// `sender_incarnation` bump separates new boots from prior runs
    /// (incarnation is part of the signed payload too, so a replay from
    /// an old run cannot impersonate the fresh run's seq space).
    next_outbound_seq: u64,
    /// Per-peer sliding window of accepted SWIM message seqs. Sized
    /// at [`REPLAY_WINDOW_BITS`] bits per peer; a peer entry is created
    /// on first verified message. Bounded growth is enforced indirectly
    /// by the membership lifecycle: peers that drop out and are forgotten
    /// after [`DEAD_MEMBER_FORGET_AFTER`] can be GC'd from this map by
    /// the membership reaper.
    seen_seq: HashMap<NodeId, ReplayWindow>,
}

impl SwimRunner {
    /// Create a new SWIM runner.
    pub fn new(config: SwimConfig) -> Self {
        let membership = Arc::new(Mutex::new(Membership::new(
            config.self_id,
            config.suspicion_timeout,
        )));
        let incarnation = config.persisted_incarnation + 1;
        Self {
            config,
            membership,
            peer_addrs: Arc::new(Mutex::new(HashMap::new())),
            swim_peer_addrs: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            incarnation,
            pending_probe: None,
            probe_round_robin: 0,
            ping_req_forwarding: HashMap::new(),
            ping_req_forwarding_order: std::collections::VecDeque::new(),
            next_outbound_seq: 1,
            seen_seq: HashMap::new(),
        }
    }

    /// Get the current SWIM incarnation counter.
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// Get a reference to the membership state.
    pub fn membership(&self) -> Arc<Mutex<Membership>> {
        self.membership.clone()
    }

    /// Get the current alive members.
    pub fn alive_members(&self) -> Vec<NodeId> {
        self.membership.lock().alive_members()
    }

    /// Get the address of a node.
    pub fn node_addr(&self, node: &NodeId) -> Option<SocketAddr> {
        if *node == self.config.self_id {
            return Some(self.config.self_addr);
        }
        self.peer_addrs.lock().get(node).copied()
    }

    /// Start the SWIM protocol loop in a background thread.
    ///
    /// Returns a handle to the thread and a channel that receives cluster events.
    pub fn start(
        self,
    ) -> (
        Arc<AtomicBool>,
        std::thread::JoinHandle<()>,
        std::sync::mpsc::Receiver<ClusterEvent>,
    ) {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let shutdown = self.shutdown.clone();

        let handle = std::thread::spawn(move || {
            if let Err(e) = self.run_loop(event_tx) {
                tracing::error!(err = %e, "SWIM loop error");
            }
        });

        (shutdown, handle, event_rx)
    }

    fn run_loop(mut self, event_tx: std::sync::mpsc::Sender<ClusterEvent>) -> Result<(), String> {
        let socket = UdpSocket::bind(self.config.bind_addr)
            .map_err(|e| format!("SWIM bind {}: {e}", self.config.bind_addr))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking: {e}"))?;
        // Increase receive buffer to avoid dropping ACKs when many
        // self-looped or gossip packets arrive simultaneously.
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = socket.as_raw_fd();
            let size: libc::c_int = 1024 * 1024; // 1 MiB
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    &size as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        tracing::info!(bind_addr = %self.config.bind_addr, "SWIM listening");

        // Initial join attempt to seed nodes. Clone the seed list so
        // we don't hold an immutable borrow on `self.config` while
        // `encode_message` takes `&mut self` (it bumps the outbound seq).
        let seeds: Vec<SocketAddr> = self.config.seed_nodes.clone();
        for seed in seeds {
            let updates = self.collect_member_updates();
            let msg = self.encode_message(MSG_JOIN, &updates);
            let _ = socket.send_to(&msg, seed);
        }

        let probe_interval = self.config.probe_interval;
        let mut last_probe = Instant::now();
        // Current jittered probe deadline: probe fires when `last_probe.elapsed()
        // >= next_probe_delay`. Recomputed after every probe so consecutive
        // intervals are independently jittered across the ±25% window, which
        // breaks lockstep with peers and reduces collision on the network.
        let mut next_probe_delay = jittered_probe_interval(probe_interval);
        let mut last_seed_retry = Instant::now();
        // Phase I — exponential seed-retry backoff. `seed_attempt` counts
        // consecutive retries since the last "cluster looks healthy"
        // observation; it resets to `0` whenever the alive-count check
        // shows the cluster has settled. The initial healthy-check
        // cadence stays at `probe_interval * 10` (≈1s with the default
        // 100ms probe_interval) so we don't burn cycles polling alive
        // state at 100ms when nothing is wrong; once the cluster
        // *is* degraded, the backoff schedule is `100ms → 200ms → 400ms
        // → … → 5s` from the helper.
        let mut seed_attempt: u32 = 0;
        let seed_backoff_initial = Duration::from_millis(100);
        let seed_backoff_max = Duration::from_secs(5);
        let healthy_seed_check_interval = probe_interval * 10;
        let mut next_seed_retry_delay = healthy_seed_check_interval;
        let mut recv_buf = [0u8; MAX_MSG_SIZE];

        while !self.shutdown.load(Ordering::Relaxed) {
            // Receive incoming messages (bounded drain to prevent
            // probe-timer starvation under message bursts).
            for _ in 0..64 {
                match socket.recv_from(&mut recv_buf) {
                    Ok((len, from_addr)) => {
                        let events = self.handle_message(&recv_buf[..len], from_addr, &socket);
                        for event in events {
                            let _ = event_tx.send(event);
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => break,
                }
            }

            // Check pending probe timeout. The suspect deadline uses
            // exponential backoff on repeated ping-req failures: each
            // retry round doubles the wait so a transiently slow peer is
            // not marked suspect on the first hiccup. The initial direct
            // probe still uses the un-jittered `probe_interval` so we
            // don't prolong the normal RTT budget.
            let mut should_suspect = false;
            if let Some(ref pending) = self.pending_probe {
                let elapsed = pending.started.elapsed();
                let indirect_timeout =
                    suspect_backoff_delay(probe_interval, pending.indirect_attempts);
                if !pending.indirect_sent && elapsed >= probe_interval {
                    // Direct probe timed out — send indirect probes
                    self.send_indirect_probes(&socket);
                } else if pending.indirect_sent && elapsed >= indirect_timeout {
                    // Indirect round also failed — mark suspect. The caller
                    // will move the pending entry out via `take()` below.
                    should_suspect = true;
                }
            }
            if should_suspect && let Some(pending) = self.pending_probe.take() {
                if let Some(m) = swim_metrics() {
                    m.swim_probe_timeouts_total.inc();
                }
                let mut mem = self.membership.lock();
                // Use the member's current incarnation for local suspicion.
                // This is not a gossipped suspicion — it's our own probe
                // failure, so we always know the current incarnation.
                let inc = mem
                    .member_info(&pending.target)
                    .map(|i| i.incarnation)
                    .unwrap_or(0);
                let events = mem.mark_suspect(pending.target, inc);
                drop(mem);
                for event in events {
                    let _ = event_tx.send(event);
                }
            }

            // Periodic probe: select one random peer. Each cycle uses a
            // freshly-jittered delay so consecutive probes don't align
            // across peers.
            if last_probe.elapsed() >= next_probe_delay {
                self.send_probe(&socket);
                last_probe = Instant::now();
                next_probe_delay = jittered_probe_interval(probe_interval);

                // Expire suspects
                let events = self.membership.lock().expire_suspects();
                for event in events {
                    let _ = event_tx.send(event);
                }
            }

            // Periodically retry seed JOINs to rediscover nodes after
            // partitions heal or when the cluster is degraded. Without this,
            // nodes that were marked dead during a partition can never rejoin
            // because the SWIM probe cycle doesn't re-seed.
            //
            // Phase I — exponential backoff: when the cluster is healthy we
            // wait `healthy_seed_check_interval` between checks; when the
            // alive-count test shows degradation we retry on the curve from
            // `exponential_seed_backoff` so the recovery is fast at first
            // and gracefully backs off if seeds are unreachable.
            if !self.config.seed_nodes.is_empty()
                && last_seed_retry.elapsed() >= next_seed_retry_delay
            {
                let alive_count = self.membership.lock().alive_members().len();
                let total_known = self.peer_addrs.lock().len();
                // Retry seeds if we have fewer alive members than known peers
                // (some nodes are dead/suspect) or if we have no peers at all.
                let degraded = alive_count < total_known + 1 || total_known == 0;
                if degraded {
                    // Clone seed list so the inner encode_message can take &mut self.
                    let seeds: Vec<SocketAddr> = self.config.seed_nodes.clone();
                    for seed in seeds {
                        let updates = self.collect_member_updates();
                        let msg = self.encode_message(MSG_JOIN, &updates);
                        let _ = socket.send_to(&msg, seed);
                    }
                    next_seed_retry_delay = exponential_seed_backoff(
                        seed_attempt,
                        seed_backoff_initial,
                        seed_backoff_max,
                    );
                    seed_attempt = seed_attempt.saturating_add(1);
                } else {
                    // Cluster looks settled — reset the backoff and keep
                    // checking at the healthy cadence.
                    seed_attempt = 0;
                    next_seed_retry_delay = healthy_seed_check_interval;
                }
                last_seed_retry = Instant::now();

                // Garbage-collect dead nodes after a long retention window to
                // prevent unbounded memory growth. Membership keeps the
                // highest seen incarnation after removal, so a forgotten
                // NodeId cannot be reborn with a lower incarnation.
                let forgotten = self
                    .membership
                    .lock()
                    .forget_dead_older_than(DEAD_MEMBER_FORGET_AFTER);
                if !forgotten.is_empty() {
                    let mut peers = self.peer_addrs.lock();
                    let mut swim = self.swim_peer_addrs.lock();
                    for id in &forgotten {
                        peers.remove(id);
                        swim.remove(id);
                    }
                    tracing::info!(
                        count = forgotten.len(),
                        "SWIM: garbage-collected dead nodes"
                    );
                }
            }

            std::thread::sleep(Duration::from_millis(1));
        }

        Ok(())
    }

    /// Bounded-insert wrapper for [`Self::ping_req_forwarding`].
    ///
    /// Inserts `(target_id, from_addr)`. If `target_id` was already
    /// present, replaces the address but does not change FIFO order
    /// (the existing entry was inserted earlier and will still be
    /// evicted first when the cap hits). If the map is at the cap
    /// [`PING_REQ_FORWARDING_MAX`], evicts the oldest entry and
    /// bumps `SwimMetrics::swim_ping_req_dropped_total` (P2.4).
    fn ping_req_forwarding_put(&mut self, target_id: NodeId, from_addr: SocketAddr) {
        if !self.ping_req_forwarding.contains_key(&target_id)
            && self.ping_req_forwarding.len() >= PING_REQ_FORWARDING_MAX
        {
            // Pop oldest until at least one slot is free.
            while self.ping_req_forwarding.len() >= PING_REQ_FORWARDING_MAX {
                match self.ping_req_forwarding_order.pop_front() {
                    Some(oldest) => {
                        if self.ping_req_forwarding.remove(&oldest).is_some() {
                            if let Some(m) = crate::metrics::swim_metrics() {
                                m.swim_ping_req_dropped_total.inc();
                            }
                            tracing::warn!(
                                evicted = oldest.0,
                                cap = PING_REQ_FORWARDING_MAX,
                                "swim: ping_req_forwarding at capacity — evicting oldest entry",
                            );
                        }
                    }
                    None => break, // Order queue empty (map already drained).
                }
            }
        }
        if !self.ping_req_forwarding.contains_key(&target_id) {
            self.ping_req_forwarding_order.push_back(target_id);
        }
        self.ping_req_forwarding.insert(target_id, from_addr);
    }

    /// Remove the entry for `target_id` from the forwarding map and
    /// the parallel order queue. Returns the requester address if the
    /// entry existed.
    fn ping_req_forwarding_take(&mut self, target_id: &NodeId) -> Option<SocketAddr> {
        let removed = self.ping_req_forwarding.remove(target_id);
        if removed.is_some() {
            // O(n) but the queue is bounded by PING_REQ_FORWARDING_MAX
            // and removal happens once per matched ACK.
            if let Some(pos) = self.ping_req_forwarding_order.iter().position(|n| n == target_id) {
                self.ping_req_forwarding_order.remove(pos);
            }
        }
        removed
    }

    fn handle_message(
        &mut self,
        data: &[u8],
        from_addr: SocketAddr,
        socket: &UdpSocket,
    ) -> Vec<ClusterEvent> {
        // If a cluster secret is configured, verify HMAC before parsing.
        let data = if let Some(ref secret) = self.config.cluster_secret {
            match crate::cluster::auth::verify(secret, data) {
                Ok(payload) => payload,
                Err(_) => return vec![], // silently drop unauthenticated messages
            }
        } else {
            data
        };

        // Minimum header: msg_type(1) + sender_id(8) + incarnation(8) + seq(8) + addr_len(2) = 27
        if data.len() < 27 {
            return vec![];
        }

        let msg_type = data[0];
        let sender_id = NodeId(u64::from_le_bytes(data[1..9].try_into().unwrap()));
        let sender_incarnation = u64::from_le_bytes(data[9..17].try_into().unwrap());
        let sender_seq = u64::from_le_bytes(data[17..25].try_into().unwrap());
        let addr_len = u16::from_le_bytes(data[25..27].try_into().unwrap()) as usize;

        // Ignore our own messages (UDP loopback on Docker bridge networks).
        if sender_id == self.config.self_id {
            return vec![];
        }

        if data.len() < 27 + addr_len {
            return vec![];
        }

        // F-G8-003 replay defense: each peer maintains a monotonic seq;
        // reject anything we've already accepted (or that falls below the
        // sliding window). Tag verification above guarantees the seq
        // value is authentic — an attacker cannot forge a fresh seq
        // without the cluster_secret.
        let window = self.seen_seq.entry(sender_id).or_default();
        if !window.check_and_record(sender_seq) {
            return vec![];
        }

        let sender_addr_str = std::str::from_utf8(&data[27..27 + addr_len]).unwrap_or("");
        let sender_tcp_addr: SocketAddr = match sender_addr_str.parse() {
            Ok(a) => a,
            Err(_) => from_addr, // fallback
        };

        // Register the sender's TCP address (for client routing / migration)
        self.peer_addrs
            .lock()
            .insert(sender_id, sender_tcp_addr);

        // Register the sender's SWIM address (from the actual UDP source)
        self.swim_peer_addrs
            .lock()
            .insert(sender_id, from_addr);

        let mut events = self.membership.lock().mark_alive(
            sender_id,
            sender_tcp_addr,
            sender_incarnation,
            true,
        );

        // Process piggybacked membership updates
        // Wire format per entry:
        // [node_id:8][state:1][incarnation:8][tcp_addr_len:2][tcp_addr:N][swim_addr_len:2][swim_addr:M]
        let updates_offset = 27 + addr_len;
        let mut pos = updates_offset;
        if data.len() > updates_offset + 2 {
            let update_count =
                u16::from_le_bytes(data[updates_offset..updates_offset + 2].try_into().unwrap())
                    as usize;
            pos = updates_offset + 2;
            for _ in 0..update_count {
                if pos + 19 > data.len() {
                    break;
                }
                let nid = NodeId(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
                let state = data[pos + 8];
                let inc = u64::from_le_bytes(data[pos + 9..pos + 17].try_into().unwrap());
                let tcp_alen =
                    u16::from_le_bytes(data[pos + 17..pos + 19].try_into().unwrap()) as usize;
                pos += 19;
                if pos + tcp_alen > data.len() {
                    break;
                }
                let tcp_str = std::str::from_utf8(&data[pos..pos + tcp_alen]).unwrap_or("");
                pos += tcp_alen;

                // Parse swim address (new field)
                let swim_str = if pos + 2 <= data.len() {
                    let swim_alen =
                        u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    if pos + swim_alen <= data.len() {
                        let s = std::str::from_utf8(&data[pos..pos + swim_alen]).unwrap_or("");
                        pos += swim_alen;
                        s
                    } else {
                        ""
                    }
                } else {
                    ""
                };

                if let Ok(tcp_addr) = tcp_str.parse::<SocketAddr>() {
                    if nid == self.config.self_id {
                        continue;
                    }
                    self.peer_addrs.lock().insert(nid, tcp_addr);
                    store_piggybacked_swim_if_routable(
                        &mut self.swim_peer_addrs.lock(),
                        nid,
                        swim_str,
                    );
                    let mut mem = self.membership.lock();
                    let evts = match state {
                        0 => mem.mark_alive(nid, tcp_addr, inc, false),
                        1 => mem.mark_suspect(nid, inc),
                        2 => mem.mark_dead(nid, inc),
                        _ => vec![],
                    };
                    events.extend(evts);
                }
            }
        }

        // Parse extension + committed topology term (after piggybacked updates).
        // [`MSG_INDIRECT_ACK`] inserts `[probed_target_id:8]` before the committed term so the
        // original requester can clear `pending_probe` for the probed node while the message
        // header still identifies the relay (UDP source matches relay, not the probed target).
        if msg_type == MSG_INDIRECT_ACK {
            if data.len() < pos + 8 + 8 {
                return events;
            }
            let probed_target = NodeId(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
            pos += 8;
            if let Some(ref pending) = self.pending_probe
                && pending.target == probed_target
            {
                self.pending_probe = None;
            }
        }

        // Committed topology term (appended after updates, and after indirect-ACK probed id if present).
        if data.len() >= pos + 8 {
            let remote_committed = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            let local_committed = self
                .config
                .committed_term
                .load(std::sync::atomic::Ordering::Relaxed);
            if remote_committed > local_committed {
                events.push(ClusterEvent::TopologyStale(remote_committed));
            }
        }

        // Send ACK for PING/JOIN
        if msg_type == MSG_PING || msg_type == MSG_JOIN {
            let updates = self.collect_member_updates();
            let ack = self.encode_message(MSG_ACK, &updates);
            let _ = socket.send_to(&ack, from_addr);
        }

        // Handle ACK: clear pending probe if it matches
        if msg_type == MSG_ACK {
            if let Some(ref pending) = self.pending_probe
                && pending.target == sender_id
            {
                self.pending_probe = None;
            }
            // Check if we should forward this ACK to a requester (PING_REQ flow)
            if let Some(requester_addr) = self.ping_req_forwarding_take(&sender_id) {
                // Forward using MSG_INDIRECT_ACK: header/sender is this relay (UDP source matches),
                // probed target id is appended so the requester clears pending_probe for the target
                // without attributing the relay's address to the target in swim_peer_addrs.
                let updates = self.collect_member_updates();
                let ack = self.encode_indirect_ack_message(&updates, sender_id);
                let _ = socket.send_to(&ack, requester_addr);
            }
        }

        // Handle PING_REQ: probe the target on behalf of the requester
        if msg_type == MSG_PING_REQ {
            // Parse the appended target info: [target_id:8][target_addr_len:2][target_addr:N]
            let ping_req_offset = 27 + addr_len;
            // Skip past the piggybacked updates to find the PING_REQ payload
            let target_info = self.parse_ping_req_target(data, ping_req_offset);
            if let Some((target_id, target_swim_addr)) = target_info {
                // Remember that we need to forward the ACK back to the requester.
                // Bounded insert: when the map is at capacity, evict the oldest
                // entry (F-G8-004) and increment the dropped counter.
                self.ping_req_forwarding_put(target_id, from_addr);
                // Send PING to the target
                let updates = self.collect_member_updates();
                let ping = self.encode_message(MSG_PING, &updates);
                let _ = socket.send_to(&ping, target_swim_addr);
            }
        }

        events
    }

    /// Parse the target node info appended to a PING_REQ message.
    ///
    /// The target info is appended after the piggybacked membership updates:
    /// `[target_id:8][target_addr_len:2][target_addr:N]`
    ///
    /// Returns `(target_node_id, target_swim_addr)` if parsing succeeds.
    fn parse_ping_req_target(
        &self,
        data: &[u8],
        updates_start: usize,
    ) -> Option<(NodeId, SocketAddr)> {
        // First skip past the piggybacked updates
        if data.len() < updates_start + 2 {
            return None;
        }
        let update_count =
            u16::from_le_bytes(data[updates_start..updates_start + 2].try_into().unwrap()) as usize;
        let mut pos = updates_start + 2;

        // Skip each update entry (format: [id:8][state:1][inc:8][tcp_len:2][tcp:N][swim_len:2][swim:M])
        for _ in 0..update_count {
            if pos + 19 > data.len() {
                return None;
            }
            let tcp_alen =
                u16::from_le_bytes(data[pos + 17..pos + 19].try_into().unwrap()) as usize;
            pos += 19 + tcp_alen;
            if pos + 2 > data.len() {
                return None;
            }
            let swim_alen = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2 + swim_alen;
            if pos > data.len() {
                return None;
            }
        }

        // Now parse the target info
        if pos + 10 > data.len() {
            return None;
        }
        let target_id = NodeId(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
        let target_addr_len =
            u16::from_le_bytes(data[pos + 8..pos + 10].try_into().unwrap()) as usize;
        pos += 10;

        if pos + target_addr_len > data.len() {
            return None;
        }
        let target_addr_str = std::str::from_utf8(&data[pos..pos + target_addr_len]).ok()?;
        let target_addr: SocketAddr = target_addr_str.parse().ok()?;

        // The address in the PING_REQ payload is already the target's SWIM address.
        Some((target_id, target_addr))
    }

    /// Select ONE random peer and send a direct PING probe.
    ///
    /// This is the standard SWIM protocol: each probe interval, one peer is
    /// selected for probing. If it doesn't respond, indirect probes follow.
    ///
    /// If there is already a pending probe (awaiting ACK or indirect results),
    /// we skip starting a new one to avoid resetting the suspicion timer.
    fn send_probe(&mut self, socket: &UdpSocket) {
        use crate::cluster::membership::NodeState;

        // Don't start a new probe if one is already in flight.
        // The existing probe's timeout will handle failure detection.
        if self.pending_probe.is_some() {
            return;
        }

        // Filter to non-dead nodes that have a known SWIM (UDP) address.
        // Dead nodes can rejoin via seed retry (runs every 10 probe intervals).
        // Nodes discovered via gossip but never contacted directly have no
        // swim address yet — including them wastes the probe slot because
        // we can't send a UDP packet without a destination.
        let membership = self.membership.lock();
        let swim_addrs = self.swim_peer_addrs.lock();
        let peers: Vec<(NodeId, SocketAddr)> = self
            .peer_addrs
            .lock()
            .iter()
            .filter(|&(&id, _)| {
                membership
                    .member_info(&id)
                    .map(|info| info.state != NodeState::Dead)
                    .unwrap_or(true) // probe unknown nodes
            })
            .filter(|&(&id, _)| swim_addrs.contains_key(&id))
            .map(|(&id, _)| (id, *swim_addrs.get(&id).unwrap()))
            .collect();
        drop(swim_addrs);
        drop(membership);

        if peers.is_empty() {
            return;
        }

        // Round-robin selection of the peer to probe
        let idx = self.probe_round_robin % peers.len();
        self.probe_round_robin = self.probe_round_robin.wrapping_add(1);
        let (target_id, target_swim_addr) = peers[idx];

        let updates = self.collect_member_updates();
        let msg = self.encode_message(MSG_PING, &updates);

        let _ = socket.send_to(&msg, target_swim_addr);
        if let Some(m) = swim_metrics() {
            m.swim_probes_sent_total.inc();
        }
        self.pending_probe = Some(PendingProbe {
            target: target_id,
            started: Instant::now(),
            indirect_sent: false,
            indirect_attempts: 0,
        });
    }

    /// Send indirect PING_REQ probes to K other peers, asking them to probe
    /// the suspect node on our behalf.
    fn send_indirect_probes(&mut self, socket: &UdpSocket) {
        let suspect_id = match self.pending_probe {
            Some(ref p) => p.target,
            None => return,
        };

        // Always mark indirect_sent so the suspicion timer advances,
        // even if there are no other peers to ask (e.g. 2-node cluster).
        if let Some(ref mut p) = self.pending_probe {
            p.indirect_sent = true;
            p.indirect_attempts = p.indirect_attempts.saturating_add(1);
        }
        if let Some(m) = swim_metrics() {
            m.swim_indirect_probes_total.inc();
        }

        // Filter out the suspect itself and dead nodes — dead peers
        // cannot relay probes on our behalf.
        let membership = self.membership.lock();
        let peers: Vec<(NodeId, SocketAddr)> = self
            .peer_addrs
            .lock()
            .iter()
            .filter(|&(&id, _)| {
                id != suspect_id
                    && membership
                        .member_info(&id)
                        .map(|info| info.state != crate::cluster::membership::NodeState::Dead)
                        .unwrap_or(true)
            })
            .map(|(&id, &addr)| (id, addr))
            .collect();
        drop(membership);

        if peers.is_empty() {
            return;
        }

        // Get the suspect's SWIM address for the PING_REQ payload
        // Get the suspect's SWIM address. If we don't know it, we can't
        // ask others to probe it on our behalf.
        let suspect_swim_addr = match self
            .swim_peer_addrs
            .lock()
            .get(&suspect_id)
            .copied()
        {
            Some(a) => a,
            None => return,
        };

        // Build PING_REQ message with target info appended after updates
        let updates = self.collect_member_updates();
        let suspect_addr_str = suspect_swim_addr.to_string();
        let suspect_addr_bytes = suspect_addr_str.as_bytes();

        let mut payload = updates;
        payload.extend_from_slice(&suspect_id.0.to_le_bytes());
        payload.extend_from_slice(&(suspect_addr_bytes.len() as u16).to_le_bytes());
        payload.extend_from_slice(suspect_addr_bytes);

        let msg = self.encode_message(MSG_PING_REQ, &payload);

        // Send to up to K random other peers using their SWIM addresses.
        // Skip peers whose swim address is unknown.
        let swim_addrs = self.swim_peer_addrs.lock();
        let k = INDIRECT_PROBE_K.min(peers.len());
        for &(peer_id, _tcp_addr) in peers.iter().take(k) {
            if let Some(&addr) = swim_addrs.get(&peer_id) {
                let _ = socket.send_to(&msg, addr);
            }
        }
    }

    fn encode_message(&mut self, msg_type: u8, piggybacked_updates: &[u8]) -> Vec<u8> {
        let addr_str = self.config.self_addr.to_string();
        let addr_bytes = addr_str.as_bytes();
        let seq = self.next_outbound_seq;
        self.next_outbound_seq = self.next_outbound_seq.wrapping_add(1);

        let mut buf =
            Vec::with_capacity(27 + addr_bytes.len() + piggybacked_updates.len() + 8 + 32);
        buf.push(msg_type);
        buf.extend_from_slice(&self.config.self_id.0.to_le_bytes());
        buf.extend_from_slice(&self.incarnation.to_le_bytes());
        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(addr_bytes);
        buf.extend_from_slice(piggybacked_updates);

        // Append committed topology term for catch-up detection.
        // Receivers compare this against their local committed term
        // and emit TopologyStale if they are behind.
        let ct = self
            .config
            .committed_term
            .load(std::sync::atomic::Ordering::Relaxed);
        buf.extend_from_slice(&ct.to_le_bytes());

        // If a cluster secret is configured, append HMAC-SHA256 tag.
        if let Some(ref secret) = self.config.cluster_secret {
            buf = crate::cluster::auth::sign(secret, &buf);
        }
        observe_encoded_message_size(msg_type, buf.len());
        buf
    }

    /// Encode a relay-forwarded indirect probe result for the original PING_REQ requester.
    ///
    /// The header identifies the **relay** (same as a normal outgoing message from this node).
    /// `probed_target` is the node that responded to the relay's PING; the requester uses it to
    /// clear [`SwimRunner::pending_probe`] without mapping `probed_target` to the relay's UDP
    /// source address.
    fn encode_indirect_ack_message(
        &mut self,
        piggybacked_updates: &[u8],
        probed_target: NodeId,
    ) -> Vec<u8> {
        let addr_str = self.config.self_addr.to_string();
        let addr_bytes = addr_str.as_bytes();
        let seq = self.next_outbound_seq;
        self.next_outbound_seq = self.next_outbound_seq.wrapping_add(1);

        let mut buf =
            Vec::with_capacity(27 + addr_bytes.len() + piggybacked_updates.len() + 8 + 8 + 32);
        buf.push(MSG_INDIRECT_ACK);
        buf.extend_from_slice(&self.config.self_id.0.to_le_bytes());
        buf.extend_from_slice(&self.incarnation.to_le_bytes());
        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(addr_bytes);
        buf.extend_from_slice(piggybacked_updates);
        buf.extend_from_slice(&probed_target.0.to_le_bytes());

        let ct = self
            .config
            .committed_term
            .load(std::sync::atomic::Ordering::Relaxed);
        buf.extend_from_slice(&ct.to_le_bytes());

        if let Some(ref secret) = self.config.cluster_secret {
            buf = crate::cluster::auth::sign(secret, &buf);
        }
        observe_encoded_message_size(MSG_INDIRECT_ACK, buf.len());
        buf
    }

    fn collect_member_updates(&self) -> Vec<u8> {
        use crate::cluster::membership::NodeState;

        let membership = self.membership.lock();
        let peers = self.peer_addrs.lock();
        let swim_addrs = self.swim_peer_addrs.lock();

        let mut buf = Vec::new();
        let mut entries: Vec<(NodeId, u8, u64, String, String)> = Vec::new();

        // Always include self as alive.
        let self_swim = effective_swim_gossip_addr(self.config.self_addr, self.config.bind_addr);
        entries.push((
            self.config.self_id,
            0, // Alive
            self.incarnation,
            self.config.self_addr.to_string(),
            self_swim.to_string(),
        ));

        // Collect all known members with their actual state.
        // Prioritize suspect/dead entries — they carry convergence-critical
        // failure information that other nodes need to learn quickly.
        let all_states = membership.all_member_states();
        let mut suspect_dead: Vec<_> = all_states
            .iter()
            .filter(|(_, st, _, _)| *st != NodeState::Alive)
            .collect();
        let mut alive: Vec<_> = all_states
            .iter()
            .filter(|(_, st, _, _)| *st == NodeState::Alive)
            .collect();
        // Stable ordering within each group
        suspect_dead.sort_by_key(|(id, _, _, _)| *id);
        alive.sort_by_key(|(id, _, _, _)| *id);

        for &&(node, state, incarnation, _addr) in suspect_dead.iter().chain(alive.iter()) {
            if entries.len() >= 20 {
                break;
            }
            let state_byte: u8 = match state {
                NodeState::Alive => 0,
                NodeState::Suspect => 1,
                NodeState::Dead => 2,
            };
            let (tcp_str, swim_str) = if let Some(&tcp) = peers.get(&node) {
                let swim = swim_addrs.get(&node).copied().unwrap_or(tcp);
                (tcp.to_string(), swim.to_string())
            } else {
                continue;
            };
            entries.push((node, state_byte, incarnation, tcp_str, swim_str));
        }

        let count = entries.len().min(20) as u16;
        buf.extend_from_slice(&count.to_le_bytes());

        // Wire format per entry:
        // [node_id:8][state:1][incarnation:8][tcp_addr_len:2][tcp_addr:N][swim_addr_len:2][swim_addr:M]
        for (node, state, incarnation, tcp_str, swim_str) in &entries {
            let tcp_bytes = tcp_str.as_bytes();
            let swim_bytes = swim_str.as_bytes();
            buf.extend_from_slice(&node.0.to_le_bytes());
            buf.push(*state);
            buf.extend_from_slice(&incarnation.to_le_bytes());
            buf.extend_from_slice(&(tcp_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(tcp_bytes);
            buf.extend_from_slice(&(swim_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(swim_bytes);
        }

        buf
    }

    /// Signal the SWIM loop to stop.
    pub fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Test-only helper: encode a SWIM message identical to one the
    /// runner would emit at this point in its lifecycle. Exposed to the
    /// `tests/g8_*` integration tests for F-G8-003 replay-defense
    /// validation. Production callers must not use this — they go
    /// through the proper send paths in `start`.
    #[doc(hidden)]
    pub fn encode_message_for_test(&mut self, msg_type: u8, piggybacked_updates: &[u8]) -> Vec<u8> {
        self.encode_message(msg_type, piggybacked_updates)
    }

    /// Test-only helper: drive a single inbound SWIM message through the
    /// runner's parsing pipeline. See [`Self::encode_message_for_test`].
    #[doc(hidden)]
    pub fn handle_message_for_test(
        &mut self,
        data: &[u8],
        from_addr: SocketAddr,
        socket: &UdpSocket,
    ) -> Vec<crate::cluster::membership::ClusterEvent> {
        self.handle_message(data, from_addr, socket)
    }

    /// Test-only helper: snapshot the TCP peer-address map. See
    /// [`Self::encode_message_for_test`].
    #[doc(hidden)]
    pub fn peer_addrs_snapshot(&self) -> HashMap<NodeId, SocketAddr> {
        self.peer_addrs.lock().clone()
    }

    #[cfg(test)]
    fn test_set_pending_probe(&mut self, target: NodeId) {
        self.pending_probe = Some(PendingProbe {
            target,
            started: Instant::now(),
            indirect_sent: true,
            indirect_attempts: 1,
        });
    }

    #[cfg(test)]
    fn test_pending_probe_target(&self) -> Option<NodeId> {
        self.pending_probe.as_ref().map(|p| p.target)
    }

    #[cfg(test)]
    fn test_swim_addr(&self, id: NodeId) -> Option<SocketAddr> {
        self.swim_peer_addrs.lock().get(&id).copied()
    }
}

/// Address used in gossip as this node's SWIM (UDP) reachability hint.
///
/// When [`SwimConfig::bind_addr`] uses an unspecified IP (e.g. `0.0.0.0` in Docker),
/// piggybacking that address would advertise a non-routable destination. In that case
/// we combine [`SwimConfig::self_addr`]'s IP with the bind port so peers retain a
/// usable UDP target.
pub(crate) fn effective_swim_gossip_addr(
    self_addr: SocketAddr,
    bind_addr: SocketAddr,
) -> SocketAddr {
    if bind_addr.ip().is_unspecified() {
        SocketAddr::new(self_addr.ip(), bind_addr.port())
    } else {
        bind_addr
    }
}

/// Stores a peer SWIM address learned from gossip only when it is routable.
///
/// Unspecified addresses (`0.0.0.0` / `::`) are ignored so they cannot overwrite
/// a valid address learned from the UDP source.
fn store_piggybacked_swim_if_routable(
    swim_peer_addrs: &mut HashMap<NodeId, SocketAddr>,
    nid: NodeId,
    swim_str: &str,
) {
    if let Ok(swim_addr) = swim_str.parse::<SocketAddr>() {
        if swim_addr.ip().is_unspecified() {
            return;
        }
        swim_peer_addrs.insert(nid, swim_addr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    impl SwimRunner {
        pub fn collect_member_updates_for_test(&self) -> Vec<u8> {
            self.collect_member_updates()
        }
    }

    // ── Phase I: exponential seed backoff ────────────────────────────────

    #[test]
    fn swim_msg_size_warning_on_overflow() {
        assert_eq!(
            classify_encoded_message_size(MSG_SIZE_WARN_THRESHOLD - 1),
            EncodedMessageSize::Normal,
        );
        assert_eq!(
            classify_encoded_message_size(MSG_SIZE_WARN_THRESHOLD),
            EncodedMessageSize::NearLimit,
        );
        assert_eq!(
            classify_encoded_message_size(MAX_MSG_SIZE),
            EncodedMessageSize::NearLimit,
        );
        assert_eq!(
            classify_encoded_message_size(MAX_MSG_SIZE + 1),
            EncodedMessageSize::Oversize,
        );
    }

    #[test]
    fn seed_retry_uses_exponential_backoff() {
        // 100ms doubling, capped at 5s — the curve specified in the plan.
        let initial = Duration::from_millis(100);
        let cap = Duration::from_secs(5);
        assert_eq!(
            exponential_seed_backoff(0, initial, cap),
            Duration::from_millis(100)
        );
        assert_eq!(
            exponential_seed_backoff(1, initial, cap),
            Duration::from_millis(200)
        );
        assert_eq!(
            exponential_seed_backoff(2, initial, cap),
            Duration::from_millis(400)
        );
        assert_eq!(
            exponential_seed_backoff(3, initial, cap),
            Duration::from_millis(800)
        );
        assert_eq!(
            exponential_seed_backoff(4, initial, cap),
            Duration::from_millis(1600)
        );
        assert_eq!(
            exponential_seed_backoff(5, initial, cap),
            Duration::from_millis(3200)
        );
        // Step 6 would be 6.4s but cap pins at 5s.
        assert_eq!(exponential_seed_backoff(6, initial, cap), cap);
        assert_eq!(exponential_seed_backoff(50, initial, cap), cap);
    }

    #[test]
    fn seed_retry_backoff_initial_at_or_above_max_returns_max() {
        // Defensive: when callers misconfigure initial >= max, the
        // helper must not blow up — every attempt clamps to max.
        let initial = Duration::from_secs(10);
        let cap = Duration::from_secs(5);
        assert_eq!(exponential_seed_backoff(0, initial, cap), cap);
        assert_eq!(exponential_seed_backoff(50, initial, cap), cap);
    }

    #[test]
    fn seed_retry_backoff_zero_initial_falls_back_to_min_step() {
        // A zero initial would shift to zero forever; clamp the first
        // step to 1ms so the loop still progresses, then stays at max.
        let initial = Duration::from_millis(0);
        let cap = Duration::from_secs(2);
        let step = exponential_seed_backoff(0, initial, cap);
        assert!(
            step >= Duration::from_millis(1) && step <= cap,
            "zero-initial fallback must be at least 1ms and never exceed max",
        );
    }

    fn test_runner(bind: SocketAddr, self_addr: SocketAddr) -> SwimRunner {
        test_runner_id(NodeId(1), bind, self_addr)
    }

    fn test_runner_id(self_id: NodeId, bind: SocketAddr, self_addr: SocketAddr) -> SwimRunner {
        SwimRunner::new(SwimConfig {
            self_id,
            self_addr,
            bind_addr: bind,
            seed_nodes: vec![],
            probe_interval: Duration::from_millis(100),
            suspicion_timeout: Duration::from_secs(5),
            cluster_secret: None,
            persisted_incarnation: 0,
            committed_term: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Parses the first membership entry (self) swim address string from `collect_member_updates` output.
    fn parse_first_entry_swim_addr(buf: &[u8]) -> String {
        let mut pos = 2usize;
        assert!(buf.len() >= pos + 19, "buffer too short");
        pos += 8 + 1 + 8;
        let tcp_len = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2 + tcp_len;
        let swim_len = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        std::str::from_utf8(&buf[pos..pos + swim_len])
            .unwrap()
            .to_string()
    }

    #[test]
    fn collect_member_updates_self_swim_uses_routable_addr_when_bind_unspecified() {
        let bind: SocketAddr = "0.0.0.0:3301".parse().unwrap();
        let self_addr: SocketAddr = "192.168.1.42:9000".parse().unwrap();
        let runner = test_runner(bind, self_addr);
        let buf = runner.collect_member_updates_for_test();
        let swim = parse_first_entry_swim_addr(&buf);
        assert_eq!(swim, "192.168.1.42:3301");
    }

    #[test]
    fn piggyback_unspecified_swim_does_not_overwrite_routable_learned() {
        let mut map = HashMap::new();
        let peer = NodeId(2);
        let learned: SocketAddr = "10.0.0.2:3301".parse().unwrap();
        map.insert(peer, learned);
        store_piggybacked_swim_if_routable(&mut map, peer, "0.0.0.0:3301");
        assert_eq!(map.get(&peer), Some(&learned));
    }

    /// Regression: a relay must not forward an indirect probe ACK as a plain [`MSG_ACK`] with the
    /// relay's sender id — that leaves the requester's `pending_probe` stuck on the real target.
    #[test]
    fn forwarded_plain_ack_does_not_clear_pending_for_probed_target() {
        let requester_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let relay_udp: SocketAddr = "10.0.0.2:5000".parse().unwrap();
        let probed_target = NodeId(3);

        let mut requester = test_runner_id(NodeId(1), requester_addr, requester_addr);
        requester
            .swim_peer_addrs
            .lock()
            .insert(probed_target, "10.0.0.3:3301".parse().unwrap());
        requester.test_set_pending_probe(probed_target);

        let mut relay = test_runner_id(NodeId(2), relay_udp, "10.0.0.2:9000".parse().unwrap());
        let updates = relay.collect_member_updates_for_test();
        let wrong_forward = relay.encode_message(MSG_ACK, &updates);

        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let _ = requester.handle_message(&wrong_forward, relay_udp, &socket);

        assert_eq!(
            requester.test_pending_probe_target(),
            Some(probed_target),
            "plain ACK from relay must not clear pending_probe for the indirect target"
        );
        assert_eq!(
            requester.test_swim_addr(probed_target),
            Some("10.0.0.3:3301".parse().unwrap())
        );
    }

    /// Indirect ACK carries probed target id so the requester clears the right pending probe
    /// without mapping the probed node's SWIM address to the relay's UDP source.
    #[test]
    fn indirect_ack_clears_pending_for_probed_target_without_swim_addr_poisoning() {
        let requester_addr: SocketAddr = "127.0.0.1:7002".parse().unwrap();
        let relay_udp: SocketAddr = "10.0.0.2:5001".parse().unwrap();
        let probed_target = NodeId(3);
        let target_swim: SocketAddr = "10.0.0.3:3301".parse().unwrap();

        let mut requester = test_runner_id(NodeId(1), requester_addr, requester_addr);
        requester
            .swim_peer_addrs
            .lock()
            .insert(probed_target, target_swim);
        requester.test_set_pending_probe(probed_target);

        let mut relay = test_runner_id(NodeId(2), relay_udp, "10.0.0.2:9000".parse().unwrap());
        let updates = relay.collect_member_updates_for_test();
        let msg = relay.encode_indirect_ack_message(&updates, probed_target);

        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let _ = requester.handle_message(&msg, relay_udp, &socket);

        assert_eq!(requester.test_pending_probe_target(), None);
        assert_eq!(requester.test_swim_addr(probed_target), Some(target_swim));
        assert_eq!(requester.test_swim_addr(NodeId(2)), Some(relay_udp));
    }

    #[test]
    fn jittered_probe_interval_stays_within_bounds() {
        // Every draw must land in [0.75 * base, 1.25 * base]. Run many
        // trials to catch off-by-one bounds bugs.
        let base = Duration::from_millis(1000);
        let lo = Duration::from_millis(750);
        let hi = Duration::from_millis(1250);
        for _ in 0..1024 {
            let d = jittered_probe_interval(base);
            assert!(
                d >= lo && d <= hi,
                "jittered interval {d:?} out of [{lo:?}, {hi:?}]"
            );
        }
    }

    #[test]
    fn jittered_probe_interval_spreads_across_window() {
        // Take a lot of samples and verify the spread covers a non-trivial
        // fraction of the jitter window — otherwise we'd be getting
        // constant output (lockstep).
        let base = Duration::from_millis(1_000_000); // 1 s in μs for precision
        let mut min = Duration::MAX;
        let mut max = Duration::ZERO;
        for _ in 0..512 {
            let d = jittered_probe_interval(base);
            if d < min {
                min = d;
            }
            if d > max {
                max = d;
            }
        }
        let spread = max - min;
        // The full theoretical spread is 0.5 * base = 500 ms; we require
        // at least 20% of that to confirm we're not degenerate. In
        // practice the RandomState-based draw gives near-full coverage.
        let min_spread = base / 5;
        assert!(
            spread >= min_spread,
            "jitter spread {spread:?} too tight; expected >= {min_spread:?}"
        );
        // And the min must be below base, max must be above base, so we
        // actually see both sides of the window.
        assert!(min < base, "jitter never produces a value below base");
        assert!(max > base, "jitter never produces a value above base");
    }

    #[test]
    fn suspect_backoff_doubles_per_indirect_round() {
        let base = Duration::from_millis(100);
        // attempts=0: before any indirect round sent — treat as first.
        // attempts=1: first indirect round → 2 * base
        assert_eq!(suspect_backoff_delay(base, 1), Duration::from_millis(200));
        // attempts=2: second round → 4 * base
        assert_eq!(suspect_backoff_delay(base, 2), Duration::from_millis(400));
        // attempts=3: third round → 8 * base
        assert_eq!(suspect_backoff_delay(base, 3), Duration::from_millis(800));
        // attempts=4+: capped at 16 * base
        assert_eq!(suspect_backoff_delay(base, 4), Duration::from_millis(1600));
        assert_eq!(
            suspect_backoff_delay(base, 100),
            Duration::from_millis(1600)
        );
    }

    /// P2.4 / F-G8-004: when `ping_req_forwarding_put` evicts the oldest
    /// entry to honour the bounded-map cap, it must bump the new
    /// `SwimMetrics::swim_ping_req_dropped_total` counter (formerly a
    /// process-wide `AtomicU64`).
    ///
    /// Drives `ping_req_forwarding_put` past
    /// [`PING_REQ_FORWARDING_MAX`] entries with distinct NodeIds and
    /// observes the counter delta. The legacy `ping_req_dropped_total()`
    /// accessor is also asserted to remain consistent with the canonical
    /// metric so existing callers in `tests/g8_ping_req_cap.rs` continue
    /// to work without import churn.
    #[test]
    fn ping_req_eviction_bumps_metric() {
        use crate::metrics::{SwimMetrics, init_swim_metrics, swim_metrics};
        use std::sync::OnceLock;

        static TEST_METRICS: OnceLock<SwimMetrics> = OnceLock::new();
        let m_ref: &'static SwimMetrics = TEST_METRICS.get_or_init(SwimMetrics::new);
        init_swim_metrics(m_ref);
        let metrics = swim_metrics().expect("metrics installed");
        let before = metrics.swim_ping_req_dropped_total.get();
        let before_accessor = ping_req_dropped_total();

        // Build a runner with throwaway addrs — we never bind or send.
        let bind: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let cfg = SwimConfig {
            self_id: NodeId(1),
            self_addr: bind,
            bind_addr: bind,
            seed_nodes: vec![],
            probe_interval: Duration::from_millis(100),
            suspicion_timeout: Duration::from_secs(5),
            cluster_secret: None,
            persisted_incarnation: 0,
            committed_term: Arc::new(AtomicU64::new(0)),
        };
        let mut runner = SwimRunner::new(cfg);

        // Push PING_REQ_FORWARDING_MAX + 5 distinct entries; the last 5
        // must evict the 5 oldest.
        let from_addr: std::net::SocketAddr = "127.0.0.1:65001".parse().unwrap();
        let total = PING_REQ_FORWARDING_MAX as u64 + 5;
        for i in 0..total {
            runner.ping_req_forwarding_put(NodeId(10_000 + i), from_addr);
        }

        let after = metrics.swim_ping_req_dropped_total.get();
        let after_accessor = ping_req_dropped_total();
        assert!(
            after - before >= 5,
            "swim_ping_req_dropped_total must advance by ≥ 5 evictions, got {}",
            after - before,
        );
        // The legacy accessor must read the same counter.
        assert_eq!(
            after_accessor - before_accessor,
            after - before,
            "ping_req_dropped_total() wrapper must agree with SwimMetrics counter",
        );
    }
}
