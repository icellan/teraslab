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
use std::time::Duration;

/// SWIM protocol message types.
const MSG_PING: u8 = 1;
const MSG_ACK: u8 = 2;
const MSG_JOIN: u8 = 3;

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
}

/// A running SWIM protocol instance.
pub struct SwimRunner {
    config: SwimConfig,
    membership: Arc<Mutex<Membership>>,
    peer_addrs: Arc<Mutex<HashMap<NodeId, SocketAddr>>>,
    shutdown: Arc<AtomicBool>,
    incarnation: u64,
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
            shutdown: Arc::new(AtomicBool::new(false)),
            incarnation: 1,
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

        // Join seed nodes
        for seed in &self.config.seed_nodes {
            let msg = self.encode_message(MSG_JOIN, &[]);
            let _ = socket.send_to(&msg, seed);
        }

        let probe_interval = self.config.probe_interval;
        let mut last_probe = std::time::Instant::now();
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

            // Periodic probe
            if last_probe.elapsed() >= probe_interval {
                self.send_probes(&socket);
                last_probe = std::time::Instant::now();

                // Expire suspects
                let events = self.membership.lock().unwrap().expire_suspects();
                for event in events {
                    let _ = event_tx.send(event);
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
        if data.len() < 11 {
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

        // Register the sender as alive
        self.peer_addrs
            .lock()
            .unwrap()
            .insert(sender_id, sender_tcp_addr);

        let mut events = self
            .membership
            .lock()
            .unwrap()
            .mark_alive(sender_id, sender_tcp_addr, sender_incarnation);

        // Process piggybacked membership updates
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
                let alen = u16::from_le_bytes(data[pos + 17..pos + 19].try_into().unwrap()) as usize;
                pos += 19;
                if pos + alen > data.len() {
                    break;
                }
                let addr_str = std::str::from_utf8(&data[pos..pos + alen]).unwrap_or("");
                pos += alen;

                if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                    self.peer_addrs.lock().unwrap().insert(nid, addr);
                    let mut mem = self.membership.lock().unwrap();
                    let evts = match state {
                        0 => mem.mark_alive(nid, addr, inc),
                        1 => mem.mark_suspect(nid),
                        2 => mem.mark_dead(nid),
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

        events
    }

    fn send_probes(&self, socket: &UdpSocket) {
        let peers = self.peer_addrs.lock().unwrap().clone();
        let updates = self.collect_member_updates();
        let msg = self.encode_message(MSG_PING, &updates);

        for (&_node_id, &addr) in &peers {
            // Send to the SWIM port (same port as bind_addr)
            let swim_addr = SocketAddr::new(addr.ip(), self.config.bind_addr.port());
            let _ = socket.send_to(&msg, swim_addr);
        }
    }

    fn encode_message(&self, msg_type: u8, piggybacked_updates: &[u8]) -> Vec<u8> {
        let addr_str = self.config.self_addr.to_string();
        let addr_bytes = addr_str.as_bytes();

        let mut buf = Vec::with_capacity(19 + addr_bytes.len() + piggybacked_updates.len());
        buf.push(msg_type);
        buf.extend_from_slice(&self.config.self_id.0.to_le_bytes());
        buf.extend_from_slice(&self.incarnation.to_le_bytes());
        buf.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(addr_bytes);
        buf.extend_from_slice(piggybacked_updates);
        buf
    }

    fn collect_member_updates(&self) -> Vec<u8> {
        let membership = self.membership.lock().unwrap();
        let peers = self.peer_addrs.lock().unwrap();

        let alive = membership.alive_members();
        let mut buf = Vec::new();
        let count = alive.len().min(20) as u16; // Limit piggybacked updates
        buf.extend_from_slice(&count.to_le_bytes());

        for &node in alive.iter().take(20) {
            let state: u8 = 0; // Alive
            let incarnation: u64 = 1;
            let addr_str = if node == self.config.self_id {
                self.config.self_addr.to_string()
            } else if let Some(&addr) = peers.get(&node) {
                addr.to_string()
            } else {
                continue;
            };
            let addr_bytes = addr_str.as_bytes();

            buf.extend_from_slice(&node.0.to_le_bytes());
            buf.push(state);
            buf.extend_from_slice(&incarnation.to_le_bytes());
            buf.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(addr_bytes);
        }

        buf
    }

    /// Signal the SWIM loop to stop.
    pub fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}
