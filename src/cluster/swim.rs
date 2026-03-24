//! SWIM-style UDP membership protocol.
//!
//! Each node periodically probes a random peer. Membership updates are
//! piggybacked on probe/ack messages. Failure detection uses direct
//! probes with a suspicion timeout.

use crate::cluster::membership::{ClusterEvent, Membership};
use crate::cluster::shards::NodeId;
use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// SWIM protocol message types.
const MSG_PING: u8 = 1;
const MSG_ACK: u8 = 2;
const MSG_JOIN: u8 = 3;
/// Indirect probe request: "please probe this node for me".
const MSG_PING_REQ: u8 = 4;

/// On-wire message format:
/// [msg_type:1][sender_id:8][sender_incarnation:8][sender_addr_len:2][sender_addr:N]
/// [update_count:2][ [node_id:8][state:1][incarnation:8][addr_len:2][addr:N] × count ]
const MAX_MSG_SIZE: usize = 4096;

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
}

/// State of a pending direct probe awaiting an ACK.
struct PendingProbe {
    /// The node we are probing.
    target: NodeId,
    /// When the probe was sent.
    started: Instant,
    /// Whether indirect (PING_REQ) probes have been sent.
    indirect_sent: bool,
}

/// Number of indirect probe peers to ask when direct probe fails.
const INDIRECT_PROBE_K: usize = 3;

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
    ping_req_forwarding: HashMap<NodeId, SocketAddr>,
}

impl SwimRunner {
    /// Create a new SWIM runner.
    pub fn new(config: SwimConfig) -> Self {
        let membership = Arc::new(Mutex::new(Membership::new(
            config.self_id,
            config.suspicion_timeout,
        )));
        Self {
            config,
            membership,
            peer_addrs: Arc::new(Mutex::new(HashMap::new())),
            swim_peer_addrs: Arc::new(Mutex::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            incarnation: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            pending_probe: None,
            probe_round_robin: 0,
            ping_req_forwarding: HashMap::new(),
        }
    }

    /// Get a reference to the membership state.
    pub fn membership(&self) -> Arc<Mutex<Membership>> {
        self.membership.clone()
    }

    /// Get the current alive members.
    pub fn alive_members(&self) -> Vec<NodeId> {
        self.membership.lock().unwrap().alive_members()
    }

    /// Get the address of a node.
    pub fn node_addr(&self, node: &NodeId) -> Option<SocketAddr> {
        if *node == self.config.self_id {
            return Some(self.config.self_addr);
        }
        self.peer_addrs.lock().unwrap().get(node).copied()
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
                eprintln!("SWIM loop error: {e}");
            }
        });

        (shutdown, handle, event_rx)
    }

    fn run_loop(
        mut self,
        event_tx: std::sync::mpsc::Sender<ClusterEvent>,
    ) -> Result<(), String> {
        let socket = UdpSocket::bind(self.config.bind_addr)
            .map_err(|e| format!("SWIM bind {}: {e}", self.config.bind_addr))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking: {e}"))?;

        eprintln!("SWIM listening on {}", self.config.bind_addr);

        // Initial join attempt to seed nodes
        for seed in &self.config.seed_nodes {
            let updates = self.collect_member_updates();
            let msg = self.encode_message(MSG_JOIN, &updates);
            let _ = socket.send_to(&msg, seed);
        }

        let probe_interval = self.config.probe_interval;
        let mut last_probe = Instant::now();
        let mut last_seed_retry = Instant::now();
        let mut recv_buf = [0u8; MAX_MSG_SIZE];

        while !self.shutdown.load(Ordering::Relaxed) {
            // Receive incoming messages
            loop {
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

            // Check pending probe timeout
            let mut should_suspect = false;
            if let Some(ref pending) = self.pending_probe {
                let elapsed = pending.started.elapsed();
                if !pending.indirect_sent && elapsed >= probe_interval {
                    // Direct probe timed out — send indirect probes
                    self.send_indirect_probes(&socket);
                } else if pending.indirect_sent && elapsed >= probe_interval * 2 {
                    // Both direct and indirect probes failed — mark suspect
                    should_suspect = true;
                }
            }
            if should_suspect
                && let Some(pending) = self.pending_probe.take()
            {
                let mut mem = self.membership.lock().unwrap();
                // Use the member's current incarnation for local suspicion.
                // This is not a gossipped suspicion — it's our own probe
                // failure, so we always know the current incarnation.
                let inc = mem.member_info(&pending.target)
                    .map(|i| i.incarnation)
                    .unwrap_or(0);
                let events = mem.mark_suspect(pending.target, inc);
                drop(mem);
                for event in events {
                    let _ = event_tx.send(event);
                }
            }

            // Periodic probe: select one random peer
            if last_probe.elapsed() >= probe_interval {
                self.send_probe(&socket);
                last_probe = Instant::now();

                // Expire suspects
                let events = self.membership.lock().unwrap().expire_suspects();
                for event in events {
                    let _ = event_tx.send(event);
                }
            }

            // Periodically retry seed JOINs to rediscover nodes after
            // partitions heal or when the cluster is degraded. Without this,
            // nodes that were marked dead during a partition can never rejoin
            // because the SWIM probe cycle doesn't re-seed.
            if !self.config.seed_nodes.is_empty()
                && last_seed_retry.elapsed() >= probe_interval * 10
            {
                let alive_count = self.membership.lock().unwrap().alive_members().len();
                let total_known = self.peer_addrs.lock().unwrap().len();
                // Retry seeds if we have fewer alive members than known peers
                // (some nodes are dead/suspect) or if we have no peers at all.
                if alive_count < total_known + 1 || total_known == 0 {
                    for seed in &self.config.seed_nodes {
                        let updates = self.collect_member_updates();
                        let msg = self.encode_message(MSG_JOIN, &updates);
                        let _ = socket.send_to(&msg, seed);
                    }
                }
                last_seed_retry = Instant::now();

                // Garbage-collect dead nodes older than 1 hour to prevent
                // unbounded memory growth. Also clean up their address entries
                // so gossip stops including them.
                let forgotten = self.membership.lock().unwrap()
                    .forget_dead_older_than(Duration::from_secs(3600));
                if !forgotten.is_empty() {
                    let mut peers = self.peer_addrs.lock().unwrap();
                    let mut swim = self.swim_peer_addrs.lock().unwrap();
                    for id in &forgotten {
                        peers.remove(id);
                        swim.remove(id);
                    }
                    eprintln!("SWIM: garbage-collected {} dead node(s)", forgotten.len());
                }
            }

            std::thread::sleep(Duration::from_millis(10));
        }

        Ok(())
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

        // Minimum header: msg_type(1) + sender_id(8) + incarnation(8) + addr_len(2) = 19
        if data.len() < 19 {
            return vec![];
        }

        let msg_type = data[0];
        let sender_id = NodeId(u64::from_le_bytes(data[1..9].try_into().unwrap()));
        let sender_incarnation = u64::from_le_bytes(data[9..17].try_into().unwrap());
        let addr_len = u16::from_le_bytes(data[17..19].try_into().unwrap()) as usize;

        if data.len() < 19 + addr_len {
            return vec![];
        }

        let sender_addr_str = std::str::from_utf8(&data[19..19 + addr_len]).unwrap_or("");
        let sender_tcp_addr: SocketAddr = match sender_addr_str.parse() {
            Ok(a) => a,
            Err(_) => from_addr, // fallback
        };

        // Register the sender's TCP address (for client routing / migration)
        self.peer_addrs
            .lock()
            .unwrap()
            .insert(sender_id, sender_tcp_addr);

        // Register the sender's SWIM address (from the actual UDP source)
        self.swim_peer_addrs
            .lock()
            .unwrap()
            .insert(sender_id, from_addr);

        let mut events = self
            .membership
            .lock()
            .unwrap()
            .mark_alive(sender_id, sender_tcp_addr, sender_incarnation);

        // Process piggybacked membership updates
        // Wire format per entry:
        // [node_id:8][state:1][incarnation:8][tcp_addr_len:2][tcp_addr:N][swim_addr_len:2][swim_addr:M]
        let updates_offset = 19 + addr_len;
        if data.len() > updates_offset + 2 {
            let update_count =
                u16::from_le_bytes(data[updates_offset..updates_offset + 2].try_into().unwrap())
                    as usize;
            let mut pos = updates_offset + 2;
            for _ in 0..update_count {
                if pos + 19 > data.len() {
                    break;
                }
                let nid = NodeId(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
                let state = data[pos + 8];
                let inc = u64::from_le_bytes(data[pos + 9..pos + 17].try_into().unwrap());
                let tcp_alen = u16::from_le_bytes(data[pos + 17..pos + 19].try_into().unwrap()) as usize;
                pos += 19;
                if pos + tcp_alen > data.len() {
                    break;
                }
                let tcp_str = std::str::from_utf8(&data[pos..pos + tcp_alen]).unwrap_or("");
                pos += tcp_alen;

                // Parse swim address (new field)
                let swim_str = if pos + 2 <= data.len() {
                    let swim_alen = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
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
                    self.peer_addrs.lock().unwrap().insert(nid, tcp_addr);
                    // Store the swim address if present
                    if let Ok(swim_addr) = swim_str.parse::<SocketAddr>() {
                        self.swim_peer_addrs.lock().unwrap().insert(nid, swim_addr);
                    }
                    let mut mem = self.membership.lock().unwrap();
                    let evts = match state {
                        0 => mem.mark_alive(nid, tcp_addr, inc),
                        1 => mem.mark_suspect(nid, inc),
                        2 => mem.mark_dead(nid, inc),
                        _ => vec![],
                    };
                    events.extend(evts);
                }
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
            if let Some(requester_addr) = self.ping_req_forwarding.remove(&sender_id) {
                // Forward the ACK back to the original requester
                let updates = self.collect_member_updates();
                let ack = self.encode_message(MSG_ACK, &updates);
                let _ = socket.send_to(&ack, requester_addr);
            }
        }

        // Handle PING_REQ: probe the target on behalf of the requester
        if msg_type == MSG_PING_REQ {
            // Parse the appended target info: [target_id:8][target_addr_len:2][target_addr:N]
            let ping_req_offset = 19 + addr_len;
            // Skip past the piggybacked updates to find the PING_REQ payload
            let target_info = self.parse_ping_req_target(data, ping_req_offset);
            if let Some((target_id, target_swim_addr)) = target_info {
                // Remember that we need to forward the ACK back to the requester
                self.ping_req_forwarding.insert(target_id, from_addr);
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
            let tcp_alen = u16::from_le_bytes(data[pos + 17..pos + 19].try_into().unwrap()) as usize;
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

        // Filter out dead nodes — probing them wastes the probe budget.
        // Dead nodes can rejoin via seed retry (runs every 10 probe intervals).
        let membership = self.membership.lock().unwrap();
        let peers: Vec<(NodeId, SocketAddr)> = self
            .peer_addrs
            .lock()
            .unwrap()
            .iter()
            .filter(|&(&id, _)| {
                membership
                    .member_info(&id)
                    .map(|info| info.state != NodeState::Dead)
                    .unwrap_or(true) // probe unknown nodes
            })
            .map(|(&id, &addr)| (id, addr))
            .collect();
        drop(membership);

        if peers.is_empty() {
            return;
        }

        // Round-robin selection of the peer to probe
        let idx = self.probe_round_robin % peers.len();
        self.probe_round_robin = self.probe_round_robin.wrapping_add(1);
        let (target_id, _target_tcp_addr) = peers[idx];

        let updates = self.collect_member_updates();
        let msg = self.encode_message(MSG_PING, &updates);

        // Use the target's actual SWIM (UDP) address. If we don't know it
        // (node discovered via gossip but never contacted directly), skip
        // this probe — a future gossip round will populate the swim address.
        let swim_addr = self
            .swim_peer_addrs
            .lock()
            .unwrap()
            .get(&target_id)
            .copied();

        if let Some(addr) = swim_addr {
            let _ = socket.send_to(&msg, addr);
            self.pending_probe = Some(PendingProbe {
                target: target_id,
                started: Instant::now(),
                indirect_sent: false,
            });
        }
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
        }

        // Filter out the suspect itself and dead nodes — dead peers
        // cannot relay probes on our behalf.
        let membership = self.membership.lock().unwrap();
        let peers: Vec<(NodeId, SocketAddr)> = self
            .peer_addrs
            .lock()
            .unwrap()
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
            .unwrap()
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
        let swim_addrs = self.swim_peer_addrs.lock().unwrap();
        let k = INDIRECT_PROBE_K.min(peers.len());
        for &(peer_id, _tcp_addr) in peers.iter().take(k) {
            if let Some(&addr) = swim_addrs.get(&peer_id) {
                let _ = socket.send_to(&msg, addr);
            }
        }
    }

    fn encode_message(&self, msg_type: u8, piggybacked_updates: &[u8]) -> Vec<u8> {
        let addr_str = self.config.self_addr.to_string();
        let addr_bytes = addr_str.as_bytes();

        let mut buf = Vec::with_capacity(19 + addr_bytes.len() + piggybacked_updates.len() + 32);
        buf.push(msg_type);
        buf.extend_from_slice(&self.config.self_id.0.to_le_bytes());
        buf.extend_from_slice(&self.incarnation.to_le_bytes());
        buf.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(addr_bytes);
        buf.extend_from_slice(piggybacked_updates);

        // If a cluster secret is configured, append HMAC-SHA256 tag.
        if let Some(ref secret) = self.config.cluster_secret {
            buf = crate::cluster::auth::sign(secret, &buf);
        }
        buf
    }

    fn collect_member_updates(&self) -> Vec<u8> {
        use crate::cluster::membership::NodeState;

        let membership = self.membership.lock().unwrap();
        let peers = self.peer_addrs.lock().unwrap();
        let swim_addrs = self.swim_peer_addrs.lock().unwrap();

        let mut buf = Vec::new();
        let mut entries: Vec<(NodeId, u8, u64, String, String)> = Vec::new();

        // Always include self as alive.
        entries.push((
            self.config.self_id,
            0, // Alive
            self.incarnation,
            self.config.self_addr.to_string(),
            self.config.bind_addr.to_string(),
        ));

        // Collect all known members with their actual state.
        // Prioritize suspect/dead entries — they carry convergence-critical
        // failure information that other nodes need to learn quickly.
        let all_states = membership.all_member_states();
        let mut suspect_dead: Vec<_> = all_states.iter()
            .filter(|(_, st, _, _)| *st != NodeState::Alive)
            .collect();
        let mut alive: Vec<_> = all_states.iter()
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
}
