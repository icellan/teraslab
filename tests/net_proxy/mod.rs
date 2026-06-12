//! N-05 — lossy/partition network proxy fixture for live-cluster chaos
//! tests.
//!
//! Provides a per-node TCP + UDP forwarding proxy so integration tests
//! can interpose on every inter-node link of an in-process TeraSlab
//! cluster and toggle behavior at runtime:
//!
//! * **UDP (SWIM membership)** — per *directed* link rules: `pass`
//!   (default) or `drop`. Dropping one direction only models an
//!   asymmetric partition; dropping both models a full partition.
//!   Additionally, per directed link: **delay** (hold every datagram a
//!   fixed duration before delivery) and **reorder** (buffer datagrams
//!   and release them out of order within a configurable window/
//!   probability). Delay and reorder compose with each other and with
//!   the drop rules; a dropped datagram is never buffered or delivered.
//! * **TCP (replication / migration / topology RPC)** — per-node
//!   inbound `pass`/`drop`. Engaging `drop` refuses new connections AND
//!   tears down established relay connections. Per-node inbound
//!   **delay** holds each forwarded request chunk for a fixed duration
//!   (segment/chunk granularity — see the delay/reorder section below).
//!
//! ## Delay & reorder (determinism)
//!
//! All non-determinism is driven by a single SEEDED PRNG ([`SplitMix64`])
//! constructed with a fixed constant in [`ProxyNet::new`]; there is no
//! use of system randomness or wall-clock entropy anywhere in the fault
//! injection, so a given sequence of rule toggles produces an identical
//! datagram schedule on every run.
//!
//! * **UDP delay** — per directed `(from → to)` link, every forwarded
//!   datagram is held in a release buffer until `enqueue_time + delay`,
//!   then delivered. Granularity is *per datagram*: SWIM sends discrete
//!   messages, so this is exact.
//! * **UDP reorder** — per directed link, with probability `p` a
//!   datagram's effective release time is pulled *earlier* by a random
//!   fraction of the configured reorder `window`, so a later datagram
//!   can overtake an earlier one within that window. The buffer is a
//!   min-heap on effective release time, so ordering is the realistic
//!   "datagrams arrive out of order but none is lost" model SWIM must
//!   tolerate (incarnation numbers, not arrival order, decide truth).
//!   With delay==0 and reorder set, datagrams are still reordered using
//!   the window as the spread.
//! * **TCP delay** — the request-relay path forwards whole frames; a
//!   per-node inbound delay sleeps before forwarding each frame
//!   (segment/chunk granularity, not per-byte). This is enough to delay
//!   connection establishment and every relayed control frame; it does
//!   not attempt sub-frame byte-stream delay (a TCP byte stream has no
//!   datagram boundaries to hold individually).
//!
//! Wiring: each node binds its real SWIM/TCP sockets to private ports
//! and *advertises* the proxy endpoints instead
//! (`ClusterConfig::swim_advertise_addr` for SWIM gossip,
//! `ClusterConfig::self_addr` for the TCP address gossiped to peers).
//! Seed lists point at peers' proxy UDP endpoints. Test clients talk to
//! the real TCP port directly, so client traffic is never affected by
//! partition rules.
//!
//! ## UDP relay topology
//!
//! For node `N` the proxy owns a *main* socket `M_N` (the advertised
//! address) plus one lazily created NAT-style relay socket `S_{N,X}`
//! per peer `X`. Every packet is attributed to a sender by its UDP
//! source address (each node sends from its single real SWIM bind
//! socket, which is registered with the fixture), so directed rules are
//! enforced on every path:
//!
//! * packet from peer `X` arriving on any of `P_N`'s sockets →
//!   rule `X→N`; forwarded to `N`'s real bind *from* `S_{N,X}` (so `N`
//!   consistently attributes peer `X` to the `S_{N,X}` address);
//! * packet from `N` itself arriving on `S_{N,X}` (replies/probes to
//!   the address `N` learned for `X`) → rule `N→X`; forwarded to `X`'s
//!   real bind.
//!
//! Gossiped third-party SWIM addresses may transiently point at another
//! node's relay socket; packets sent there are still attributed by
//! source and delivered to the proxied node under the correct rule, and
//! direct contact (which SWIM performs continuously) re-canonicalizes
//! the address, so the indirection is self-healing.
//!
//! ## TCP attribution (and its documented limitation)
//!
//! On loopback, the *initiator* of a TCP connection cannot be
//! identified from the socket (all connections originate from
//! `127.0.0.1:<ephemeral>`), so blanket TCP rules are per destination
//! node, not per link. The one place where directed TCP cuts matter for
//! partition modeling is the topology control plane: during staggered
//! failure detection an isolated node still *dials out* proposals
//! (`OP_TOPOLOGY_PROPOSE`/`OP_TOPOLOGY_COMMIT`), and a cross-partition
//! vote would let it commit a topology a real partition would prevent.
//! Topology frames embed the proposer/voter `NodeId` at a fixed offset,
//! so the relay parses the inbound frame stream and enforces directed
//! `(sender → dest)` rules for opcodes 251/252/253 by severing the
//! connection — exactly the observable behavior of a real cut (dial
//! succeeds, response never arrives).
//!
//! Residual limitation: non-topology inter-node frames (replication,
//! migration) from an isolated node are not attributable and therefore
//! not cut. Under quorum loss the isolated node never generates them
//! (writes are rejected with `ERR_NO_QUORUM` before replication), so
//! this does not weaken the partition scenarios modeled here. The same
//! loopback-attribution limit applies to TCP delay: it is enforced
//! per-destination-node on the inbound request path (the topology
//! control plane), not per directed link. UDP delay/reorder, by
//! contrast, *is* per directed link because every SWIM datagram is
//! attributable by source address.

use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Maximum SWIM datagram size the relay buffers (mirrors the SWIM
/// implementation's `MAX_MSG_SIZE` of 1400 with headroom).
const UDP_BUF: usize = 2048;

/// Fixed seed for the fixture's deterministic PRNG. Reorder decisions
/// are driven entirely by this so every test run is reproducible.
const PRNG_SEED: u64 = 0x5EED_1A5E_F00D_C0DE;

/// Deterministic, dependency-free PRNG (SplitMix64). Used to drive
/// reorder decisions; seeded once per fixture so the datagram schedule
/// is identical across runs. NOT cryptographic — reproducibility, not
/// unpredictability, is the goal.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }

    /// Next pseudo-random `u64` (the canonical SplitMix64 step).
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform `f64` in `[0, 1)`.
    fn next_f64(&mut self) -> f64 {
        // 53-bit mantissa precision.
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Per directed-link delay/reorder configuration.
#[derive(Clone, Copy, Default)]
struct LinkTiming {
    /// Hold every forwarded datagram this long before delivery.
    delay: Duration,
    /// Probability `[0,1]` a datagram is pulled earlier (reordered).
    reorder_prob: f64,
    /// Maximum amount a reordered datagram's release is pulled earlier.
    reorder_window: Duration,
}

impl LinkTiming {
    fn is_active(&self) -> bool {
        self.delay > Duration::ZERO
            || (self.reorder_prob > 0.0 && self.reorder_window > Duration::ZERO)
    }
}

/// A datagram buffered for delayed/reordered delivery on one UDP relay.
/// Ordered as a min-heap on `release_at` (earliest first), with `seq`
/// as a deterministic FIFO tie-break for datagrams sharing a release
/// instant.
struct DelayedDatagram {
    release_at: Instant,
    seq: u64,
    dst: SocketAddr,
    /// The NAT socket key to send from (peer's real addr), or `None`
    /// to send from the main relay path toward `dst` via that key.
    via_peer_real: SocketAddr,
    data: Vec<u8>,
}

impl PartialEq for DelayedDatagram {
    fn eq(&self, other: &Self) -> bool {
        self.release_at == other.release_at && self.seq == other.seq
    }
}
impl Eq for DelayedDatagram {}
impl PartialOrd for DelayedDatagram {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for DelayedDatagram {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reverse so BinaryHeap (a max-heap) yields the EARLIEST
        // release first; break ties on seq, also reversed.
        other
            .release_at
            .cmp(&self.release_at)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

#[derive(Clone, Copy)]
struct NodeAddrs {
    real_swim: SocketAddr,
    real_tcp: SocketAddr,
}

/// Proxy endpoints returned by [`ProxyNet::register`]; configure the
/// node to advertise these instead of its real bind addresses.
#[derive(Clone, Copy, Debug)]
pub struct ProxyEndpoints {
    /// Advertise as the node's SWIM address
    /// (`ClusterConfig::swim_advertise_addr`) and use in peer seed lists.
    pub swim: SocketAddr,
    /// Advertise as the node's TCP address (`ClusterConfig::self_addr`).
    pub tcp: SocketAddr,
}

#[derive(Default)]
struct Rules {
    /// Directed UDP links currently dropping all datagrams.
    udp_drop: std::collections::HashSet<(u64, u64)>,
    /// Per directed UDP link delay/reorder timing (absent ⇒ pass-through).
    udp_timing: HashMap<(u64, u64), LinkTiming>,
    /// Per-destination-node inbound TCP request-frame delay (loopback
    /// attribution limits this to per-node, not per-link — see docs).
    tcp_delay: HashMap<u64, Duration>,
    /// Nodes whose inter-node TCP inbound is blocked.
    tcp_block: std::collections::HashSet<u64>,
    /// Directed `(sender, dest)` pairs whose topology control frames
    /// (`OP_TOPOLOGY_PROPOSE`/`VOTE`/`COMMIT`) are severed at `dest`'s
    /// relay. Sender attribution comes from the NodeId embedded in the
    /// frame payload (see module docs).
    tcp_topology_block: std::collections::HashSet<(u64, u64)>,
}

impl Rules {
    fn udp_timing_for(&self, from: u64, to: u64) -> Option<LinkTiming> {
        self.udp_timing
            .get(&(from, to))
            .copied()
            .filter(LinkTiming::is_active)
    }
}

struct Shared {
    /// node_id → real bind addresses.
    nodes: Mutex<HashMap<u64, NodeAddrs>>,
    /// real SWIM bind addr → node_id (UDP source attribution).
    swim_index: Mutex<HashMap<SocketAddr, u64>>,
    rules: Mutex<Rules>,
    /// Live TCP relay stream pairs per destination node, retained so an
    /// inbound block can tear down established connections.
    tcp_conns: Mutex<HashMap<u64, Vec<TcpStream>>>,
    /// Seeded PRNG driving reorder decisions (shared across all relays
    /// behind a mutex so the schedule is a single deterministic stream).
    prng: Mutex<SplitMix64>,
    shutdown: AtomicBool,
}

/// The fixture: one [`ProxyNet`] per test, one registered proxy per node.
pub struct ProxyNet {
    shared: Arc<Shared>,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl ProxyNet {
    pub fn new() -> Self {
        ProxyNet {
            shared: Arc::new(Shared {
                nodes: Mutex::new(HashMap::new()),
                swim_index: Mutex::new(HashMap::new()),
                rules: Mutex::new(Rules::default()),
                tcp_conns: Mutex::new(HashMap::new()),
                prng: Mutex::new(SplitMix64::new(PRNG_SEED)),
                shutdown: AtomicBool::new(false),
            }),
            threads: Mutex::new(Vec::new()),
        }
    }

    /// Register a node's real bind addresses and spawn its proxy
    /// threads. Returns the proxy endpoints the node must advertise.
    pub fn register(
        &self,
        node_id: u64,
        real_swim: SocketAddr,
        real_tcp: SocketAddr,
    ) -> ProxyEndpoints {
        self.shared.nodes.lock().unwrap().insert(
            node_id,
            NodeAddrs {
                real_swim,
                real_tcp,
            },
        );
        self.shared
            .swim_index
            .lock()
            .unwrap()
            .insert(real_swim, node_id);

        let udp_main = UdpSocket::bind("127.0.0.1:0").unwrap();
        udp_main.set_nonblocking(true).unwrap();
        let udp_addr = udp_main.local_addr().unwrap();

        let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        tcp_listener.set_nonblocking(true).unwrap();
        let tcp_addr = tcp_listener.local_addr().unwrap();

        let shared = self.shared.clone();
        let udp_handle = std::thread::spawn(move || udp_relay_loop(node_id, udp_main, &shared));
        let shared = self.shared.clone();
        let tcp_handle =
            std::thread::spawn(move || tcp_accept_loop(node_id, tcp_listener, &shared));
        let mut threads = self.threads.lock().unwrap();
        threads.push(udp_handle);
        threads.push(tcp_handle);

        ProxyEndpoints {
            swim: udp_addr,
            tcp: tcp_addr,
        }
    }

    /// Drop all UDP datagrams flowing `from → to` (one direction only).
    pub fn drop_udp_one_way(&self, from: u64, to: u64) {
        self.shared
            .rules
            .lock()
            .unwrap()
            .udp_drop
            .insert((from, to));
    }

    /// Restore UDP passing `from → to`.
    pub fn pass_udp_one_way(&self, from: u64, to: u64) {
        self.shared
            .rules
            .lock()
            .unwrap()
            .udp_drop
            .remove(&(from, to));
    }

    /// Delay every UDP datagram flowing `from → to` by `delay` before
    /// delivery (per-datagram granularity). Composes with reorder; pass
    /// `Duration::ZERO` to clear the delay while keeping any reorder.
    pub fn delay_udp_one_way(&self, from: u64, to: u64, delay: Duration) {
        let mut rules = self.shared.rules.lock().unwrap();
        rules.udp_timing.entry((from, to)).or_default().delay = delay;
    }

    /// Reorder UDP datagrams flowing `from → to`: with probability `prob`
    /// a datagram's effective release is pulled earlier by a seeded
    /// random fraction of `window`, so later datagrams can overtake
    /// earlier ones within that window. Composes with delay. Reordering
    /// never drops a datagram — it only changes delivery order.
    pub fn reorder_udp_one_way(&self, from: u64, to: u64, prob: f64, window: Duration) {
        let mut rules = self.shared.rules.lock().unwrap();
        let t = rules.udp_timing.entry((from, to)).or_default();
        t.reorder_prob = prob.clamp(0.0, 1.0);
        t.reorder_window = window;
    }

    /// Clear all delay/reorder timing on `from → to` (drop rules, if any,
    /// are unaffected).
    pub fn clear_udp_timing(&self, from: u64, to: u64) {
        self.shared
            .rules
            .lock()
            .unwrap()
            .udp_timing
            .remove(&(from, to));
    }

    /// Delay each inbound inter-node TCP request frame to `node` by
    /// `delay` before forwarding (segment/chunk granularity — see module
    /// docs for the loopback per-node attribution limit). `Duration::ZERO`
    /// clears the delay.
    pub fn delay_tcp_inbound(&self, node: u64, delay: Duration) {
        let mut rules = self.shared.rules.lock().unwrap();
        if delay == Duration::ZERO {
            rules.tcp_delay.remove(&node);
        } else {
            rules.tcp_delay.insert(node, delay);
        }
    }

    /// Block inter-node TCP inbound to `node`: refuse new relay
    /// connections and tear down established ones.
    pub fn block_tcp_inbound(&self, node: u64) {
        self.shared.rules.lock().unwrap().tcp_block.insert(node);
        let streams = self
            .shared
            .tcp_conns
            .lock()
            .unwrap()
            .remove(&node)
            .unwrap_or_default();
        for s in streams {
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
    }

    /// Re-allow inter-node TCP inbound to `node`.
    pub fn unblock_tcp_inbound(&self, node: u64) {
        self.shared.rules.lock().unwrap().tcp_block.remove(&node);
    }

    /// Fully partition `node` from each peer in `peers`: SWIM dropped in
    /// both directions per link, inter-node TCP inbound to `node`
    /// blocked, and `node`'s outbound topology control frames severed at
    /// each peer's relay (see module docs for the TCP-attribution model).
    pub fn isolate(&self, node: u64, peers: &[u64]) {
        {
            let mut rules = self.shared.rules.lock().unwrap();
            for &peer in peers {
                rules.udp_drop.insert((node, peer));
                rules.udp_drop.insert((peer, node));
                rules.tcp_topology_block.insert((node, peer));
            }
        }
        self.block_tcp_inbound(node);
    }

    /// Remove every drop/block/delay/reorder rule (heal all partitions
    /// and clear all injected timing).
    pub fn heal_all(&self) {
        let mut rules = self.shared.rules.lock().unwrap();
        rules.udp_drop.clear();
        rules.udp_timing.clear();
        rules.tcp_delay.clear();
        rules.tcp_block.clear();
        rules.tcp_topology_block.clear();
    }
}

impl Drop for ProxyNet {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::SeqCst);
        // Unblock relay copy threads stuck in blocking reads.
        let conns: Vec<TcpStream> = self
            .shared
            .tcp_conns
            .lock()
            .unwrap()
            .drain()
            .flat_map(|(_, v)| v)
            .collect();
        for s in conns {
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
        for handle in self.threads.lock().unwrap().drain(..) {
            let _ = handle.join();
        }
    }
}

fn udp_dropped(shared: &Shared, from: u64, to: u64) -> bool {
    shared.rules.lock().unwrap().udp_drop.contains(&(from, to))
}

fn node_by_swim(shared: &Shared, addr: SocketAddr) -> Option<u64> {
    shared.swim_index.lock().unwrap().get(&addr).copied()
}

/// UDP relay loop for node `n`: polls the main (advertised) socket plus
/// all per-peer NAT sockets, attributing every datagram to a sender by
/// its source address and applying the directed link rules.
fn udp_relay_loop(n: u64, main: UdpSocket, shared: &Shared) {
    // peer real-swim addr → NAT relay socket used for that peer.
    let mut nat: HashMap<SocketAddr, UdpSocket> = HashMap::new();
    let mut buf = [0u8; UDP_BUF];

    let real_swim_of = |id: u64| -> Option<SocketAddr> {
        shared.nodes.lock().unwrap().get(&id).map(|a| a.real_swim)
    };
    let my_real = match real_swim_of(n) {
        Some(a) => a,
        None => return,
    };

    // Release buffer for delayed/reordered datagrams (min-heap on
    // effective release time). Datagrams on pass-through links bypass it
    // entirely; only delayed/reordered ones land here.
    let mut buffered: BinaryHeap<DelayedDatagram> = BinaryHeap::new();
    let mut seq: u64 = 0;

    // Compute a datagram's effective release `Instant` for a directed
    // link's timing: base delay plus, with the seeded reorder
    // probability, an earlier pull within the reorder window so a later
    // datagram can overtake an earlier one.
    let release_instant = |timing: &LinkTiming| -> Instant {
        let now = Instant::now();
        let mut release = now + timing.delay;
        if timing.reorder_prob > 0.0 && timing.reorder_window > Duration::ZERO {
            let mut prng = shared.prng.lock().unwrap();
            if prng.next_f64() < timing.reorder_prob {
                let frac = prng.next_f64();
                let pull = timing.reorder_window.mul_f64(frac);
                // Never pull earlier than `now` — a reordered datagram
                // still cannot be delivered before it arrived.
                release = release.checked_sub(pull).unwrap_or(now).max(now);
            }
        }
        release
    };

    while !shared.shutdown.load(Ordering::Relaxed) {
        let mut progressed = false;

        // Inbound on the main advertised socket: always peer → n.
        while let Ok((len, src)) = main.recv_from(&mut buf) {
            progressed = true;
            let Some(peer) = node_by_swim(shared, src) else {
                continue; // unknown source — drop
            };
            if peer == n || udp_dropped(shared, peer, n) {
                continue;
            }
            nat.entry(src).or_insert_with(new_relay_socket);
            let timing = shared.rules.lock().unwrap().udp_timing_for(peer, n);
            forward_or_buffer(
                &nat,
                src,
                my_real,
                buf[..len].to_vec(),
                timing,
                &release_instant,
                &mut buffered,
                &mut seq,
            );
        }

        // Inbound on NAT sockets. Collect forwarding actions first to
        // avoid mutating `nat` while iterating it.
        enum Action {
            /// Forward n's outbound packet to the peer's real bind.
            /// `via`==`dst`==peer_real; link is `n → peer`.
            ToPeer {
                peer_real: SocketAddr,
                peer: u64,
                data: Vec<u8>,
            },
            /// Forward a (possibly third-party) packet to n, attributed
            /// to `src` (the sender's real bind). link is `x → n`.
            ToSelf {
                src: SocketAddr,
                from: u64,
                data: Vec<u8>,
            },
        }
        let mut actions: Vec<Action> = Vec::new();
        for (&peer_real, sock) in nat.iter() {
            while let Ok((len, src)) = sock.recv_from(&mut buf) {
                progressed = true;
                if src == my_real {
                    // n replying/probing toward the peer this NAT socket
                    // represents.
                    let Some(peer) = node_by_swim(shared, peer_real) else {
                        continue;
                    };
                    if !udp_dropped(shared, n, peer) {
                        actions.push(Action::ToPeer {
                            peer_real,
                            peer,
                            data: buf[..len].to_vec(),
                        });
                    }
                } else if let Some(x) = node_by_swim(shared, src) {
                    // Third-party packet aimed at a relay address that
                    // leaked via gossip; deliver to n under rule x→n.
                    if x != n && !udp_dropped(shared, x, n) {
                        actions.push(Action::ToSelf {
                            src,
                            from: x,
                            data: buf[..len].to_vec(),
                        });
                    }
                }
                // Unknown source: drop.
            }
        }
        for action in actions {
            match action {
                Action::ToPeer {
                    peer_real,
                    peer,
                    data,
                } => {
                    let timing = shared.rules.lock().unwrap().udp_timing_for(n, peer);
                    forward_or_buffer(
                        &nat,
                        peer_real,
                        peer_real,
                        data,
                        timing,
                        &release_instant,
                        &mut buffered,
                        &mut seq,
                    );
                }
                Action::ToSelf { src, from, data } => {
                    nat.entry(src).or_insert_with(new_relay_socket);
                    let timing = shared.rules.lock().unwrap().udp_timing_for(from, n);
                    forward_or_buffer(
                        &nat,
                        src,
                        my_real,
                        data,
                        timing,
                        &release_instant,
                        &mut buffered,
                        &mut seq,
                    );
                }
            }
        }

        // Release any buffered datagrams whose hold has elapsed. Sending
        // from the same NAT socket the live path uses preserves source
        // attribution at the receiver.
        let now = Instant::now();
        while buffered.peek().is_some_and(|d| d.release_at <= now) {
            if let Some(d) = buffered.pop() {
                progressed = true;
                if let Some(sock) = nat.get(&d.via_peer_real) {
                    let _ = sock.send_to(&d.data, d.dst);
                }
            }
        }

        // Idle nap when nothing moved. When datagrams are still pending
        // in the buffer, the same 1ms cadence polls their release
        // promptly without busy-spinning.
        if !progressed {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Create a non-blocking NAT relay socket.
fn new_relay_socket() -> UdpSocket {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    s.set_nonblocking(true).unwrap();
    s
}

/// Either send a datagram immediately (no active timing) or buffer it
/// for delayed/reordered release. `via` is the NAT-socket key to send
/// from; `dst` is the destination address.
#[allow(clippy::too_many_arguments)]
fn forward_or_buffer(
    nat: &HashMap<SocketAddr, UdpSocket>,
    via: SocketAddr,
    dst: SocketAddr,
    data: Vec<u8>,
    timing: Option<LinkTiming>,
    release_instant: &dyn Fn(&LinkTiming) -> Instant,
    buffered: &mut BinaryHeap<DelayedDatagram>,
    seq: &mut u64,
) {
    match timing {
        None => {
            if let Some(sock) = nat.get(&via) {
                let _ = sock.send_to(&data, dst);
            }
        }
        Some(t) => {
            let release_at = release_instant(&t);
            *seq = seq.wrapping_add(1);
            buffered.push(DelayedDatagram {
                release_at,
                seq: *seq,
                dst,
                via_peer_real: via,
                data,
            });
        }
    }
}

/// TCP accept loop for node `n`: relays accepted connections to the
/// node's real listener unless inbound TCP for `n` is blocked.
fn tcp_accept_loop(n: u64, listener: TcpListener, shared: &Arc<Shared>) {
    let real_tcp = match shared.nodes.lock().unwrap().get(&n) {
        Some(a) => a.real_tcp,
        None => return,
    };
    while !shared.shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((client, _)) => {
                if shared.rules.lock().unwrap().tcp_block.contains(&n) {
                    // Blocked: drop the connection immediately.
                    let _ = client.shutdown(std::net::Shutdown::Both);
                    continue;
                }
                let server = match TcpStream::connect(real_tcp) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let _ = client.set_nodelay(true);
                let _ = server.set_nodelay(true);
                client.set_nonblocking(false).unwrap();

                // Retain clones so a later block can kill the relay.
                {
                    let mut conns = shared.tcp_conns.lock().unwrap();
                    let entry = conns.entry(n).or_default();
                    entry.push(client.try_clone().unwrap());
                    entry.push(server.try_clone().unwrap());
                }

                let c2 = client.try_clone().unwrap();
                let s2 = server.try_clone().unwrap();
                // Requests (peer → n): frame-aware so directed topology
                // cuts can be enforced; responses (n → peer): byte pump.
                let shared_in = Arc::clone(shared);
                std::thread::spawn(move || relay_requests(client, server, n, &shared_in));
                std::thread::spawn(move || copy_until_eof(s2, c2));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(2)),
        }
    }
}

/// Frame-aware request relay (peer → proxied node `n`): forwards whole
/// frames, severing the connection when a topology control frame
/// (`OP_TOPOLOGY_PROPOSE`/`VOTE`/`COMMIT`, opcodes 251-253) carries an
/// embedded sender NodeId with an active `(sender → n)` topology block.
/// A per-node inbound delay (if set) is applied per forwarded frame
/// (segment/chunk granularity — see module docs).
///
/// Frame layout (request): `[total_len:4][request_id:8][op_code:2]
/// [flags:2][payload…]`; for the three topology opcodes the payload
/// starts with `[term:8][proposer_or_voter NodeId:8]`, i.e. the NodeId
/// sits at body offset 20..28. HMAC signing appends a suffix and leaves
/// these offsets intact.
fn relay_requests(mut from: TcpStream, mut to: TcpStream, n: u64, shared: &Shared) {
    /// Defensive bound on relayed frame size (legitimate frames are
    /// capped well below this by the protocol's MAX_FRAME_SIZE).
    const MAX_RELAY_FRAME: usize = 64 * 1024 * 1024;
    const OP_TOPOLOGY_MIN: u16 = 251;
    const OP_TOPOLOGY_MAX: u16 = 253;

    let mut len_buf = [0u8; 4];
    loop {
        if from.read_exact(&mut len_buf).is_err() {
            break;
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > MAX_RELAY_FRAME {
            break;
        }
        let mut body = vec![0u8; len];
        if from.read_exact(&mut body).is_err() {
            break;
        }
        // Snapshot the topology-block decision and inbound delay under a
        // single rules lock, then release it before any sleep so runtime
        // toggles never block on a delayed frame.
        let (blocked, delay) = {
            let rules = shared.rules.lock().unwrap();
            let blocked = if body.len() >= 28 {
                let op = u16::from_le_bytes([body[8], body[9]]);
                if (OP_TOPOLOGY_MIN..=OP_TOPOLOGY_MAX).contains(&op) {
                    let sender = u64::from_le_bytes(body[20..28].try_into().unwrap());
                    rules.tcp_topology_block.contains(&(sender, n))
                } else {
                    false
                }
            } else {
                false
            };
            (blocked, rules.tcp_delay.get(&n).copied())
        };
        if blocked {
            break; // sever — models the partition cutting the dial
        }
        if let Some(d) = delay {
            std::thread::sleep(d);
        }
        if to.write_all(&len_buf).is_err() || to.write_all(&body).is_err() {
            break;
        }
    }
    let _ = from.shutdown(std::net::Shutdown::Both);
    let _ = to.shutdown(std::net::Shutdown::Both);
}

/// Pump bytes `from → to` until EOF or error, then shut both down so the
/// peer relay thread exits too.
fn copy_until_eof(mut from: TcpStream, mut to: TcpStream) {
    let mut buf = [0u8; 16 * 1024];
    loop {
        match from.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(len) => {
                if to.write_all(&buf[..len]).is_err() {
                    break;
                }
            }
        }
    }
    let _ = from.shutdown(std::net::Shutdown::Both);
    let _ = to.shutdown(std::net::Shutdown::Both);
}
