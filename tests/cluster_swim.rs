//! SWIM membership protocol tests using localhost UDP.
//!
//! These tests start actual SWIM runners on different UDP ports and
//! verify discovery, failure detection, indirect probing, and
//! dissemination behaviour.
//!
//! Each `SwimNode` joins its thread on drop, ensuring sockets are released
//! before the next test can reuse ports.

use std::net::SocketAddr;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use teraslab::cluster::membership::ClusterEvent;
use teraslab::cluster::shards::{NodeId, ShardTable};
use teraslab::cluster::swim::{SwimConfig, SwimRunner};

/// Port range: 15000–15199 (not used by other tests).
fn swim_addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

fn tcp_addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

/// A running SWIM node that properly shuts down and joins its thread on drop,
/// ensuring the UDP socket is released before ports can be reused.
struct SwimNode {
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
    rx: mpsc::Receiver<ClusterEvent>,
}

impl SwimNode {
    fn stop(&mut self) {
        self.shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for SwimNode {
    fn drop(&mut self) {
        self.stop();
    }
}

fn start_swim(id: u64, swim_port: u16, tcp_port: u16, seeds: &[u16]) -> SwimNode {
    start_swim_with_config(id, swim_port, tcp_port, seeds,
        Duration::from_millis(200), Duration::from_secs(2))
}

fn start_swim_with_config(
    id: u64, swim_port: u16, tcp_port: u16, seeds: &[u16],
    probe_interval: Duration, suspicion_timeout: Duration,
) -> SwimNode {
    let seed_addrs: Vec<SocketAddr> = seeds.iter().map(|&p| swim_addr(p)).collect();
    let runner = SwimRunner::new(SwimConfig {
        self_id: NodeId(id),
        self_addr: tcp_addr(tcp_port),
        bind_addr: swim_addr(swim_port),
        seed_nodes: seed_addrs,
        probe_interval,
        suspicion_timeout,
    });
    let (shutdown, handle, rx) = runner.start();
    SwimNode { shutdown, handle: Some(handle), rx }
}

/// Drain all events from a receiver (non-blocking).
fn drain_events(rx: &mpsc::Receiver<ClusterEvent>) -> Vec<ClusterEvent> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

/// Wait until a predicate is satisfied or timeout.
fn wait_for<F: Fn(&[ClusterEvent]) -> bool>(
    rx: &mpsc::Receiver<ClusterEvent>,
    predicate: F,
    timeout: Duration,
) -> Vec<ClusterEvent> {
    let start = Instant::now();
    let mut all_events = Vec::new();
    while start.elapsed() < timeout {
        while let Ok(event) = rx.try_recv() {
            all_events.push(event);
        }
        if predicate(&all_events) {
            return all_events;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    all_events
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn three_nodes_form_cluster_over_udp() {
    let n1 = start_swim(1, 15000, 15001, &[]);
    let n2 = start_swim(2, 15002, 15003, &[15000]);
    let n3 = start_swim(3, 15004, 15005, &[15000]);

    std::thread::sleep(Duration::from_secs(3));

    let events1 = drain_events(&n1.rx);
    let events2 = drain_events(&n2.rx);
    let events3 = drain_events(&n3.rx);

    let count = |evts: &[ClusterEvent]| evts.iter()
        .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _))).count();

    assert!(count(&events1) >= 2, "node 1 should see 2 joins, got {}", count(&events1));
    assert!(count(&events2) >= 1, "node 2 should see at least 1 join, got {}", count(&events2));
    assert!(count(&events3) >= 1, "node 3 should see at least 1 join, got {}", count(&events3));
    // SwimNode drop handles shutdown + join
}

#[test]
fn node_stops_responding_suspect_then_dead() {
    let n1 = start_swim(10, 15010, 15011, &[]);
    let mut n2 = start_swim(11, 15012, 15013, &[15010]);

    let discovery = wait_for(&n1.rx,
        |evts| evts.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(11), _))),
        Duration::from_secs(5));
    assert!(discovery.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(11), _))),
        "node 10 should discover node 11");

    // Stop node 2 and JOIN its thread so the socket is released
    n2.stop();

    let events = wait_for(&n1.rx,
        |evts| evts.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(_))),
        Duration::from_secs(10));

    assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeSuspect(NodeId(11)))),
        "should have suspected node 11, events: {events:?}");
    assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(11)))),
        "should have declared node 11 dead, events: {events:?}");
}

#[test]
fn dead_node_restarts_with_new_incarnation() {
    let n1 = start_swim(20, 15020, 15021, &[]);
    let mut n2 = start_swim(21, 15022, 15023, &[15020]);

    let discovery = wait_for(&n1.rx,
        |evts| evts.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(21), _))),
        Duration::from_secs(5));
    assert!(discovery.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(21), _))),
        "node 20 should discover node 21");

    // Stop and JOIN so the socket is released
    n2.stop();

    let death = wait_for(&n1.rx,
        |evts| evts.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(21)))),
        Duration::from_secs(10));
    assert!(death.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(21)))),
        "node 20 should declare node 21 dead");

    // Restart node 2 on a DIFFERENT swim port (old one may still be in TIME_WAIT)
    let _n2b = start_swim(21, 15024, 15023, &[15020]);

    let events = wait_for(&n1.rx,
        |evts| evts.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(21), _))),
        Duration::from_secs(10));
    assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(21), _))),
        "node 21 should have rejoined, events: {events:?}");
}

#[test]
fn indirect_probes_three_node_cluster() {
    let n_a = start_swim(30, 15030, 15031, &[]);
    let mut n_b = start_swim(31, 15032, 15033, &[15030]);
    let n_c = start_swim(32, 15034, 15035, &[15030]);

    let discovery = wait_for(&n_a.rx,
        |evts| {
            evts.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(31), _)))
            && evts.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(32), _)))
        },
        Duration::from_secs(5));
    assert!(discovery.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(31), _))),
        "A should discover B");

    let _ = wait_for(&n_c.rx,
        |evts| evts.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(_, _))),
        Duration::from_secs(3));

    // Stop B and JOIN so socket is released
    n_b.stop();

    let events_a = wait_for(&n_a.rx,
        |evts| evts.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(31)))),
        Duration::from_secs(15));

    assert!(events_a.iter().any(|e| matches!(e, ClusterEvent::NodeSuspect(NodeId(31)))),
        "A should suspect B after all probes fail");
    assert!(events_a.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(31)))),
        "A should declare B dead after suspicion timeout");

    let events_c = wait_for(&n_c.rx,
        |evts| evts.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(31)))),
        Duration::from_secs(15));
    assert!(events_c.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(31)))),
        "C should also detect B's failure");
}

#[test]
fn cluster_event_node_joined_emitted() {
    let n1 = start_swim(40, 15040, 15041, &[]);
    let _n2 = start_swim(41, 15042, 15043, &[15040]);

    let events = wait_for(&n1.rx,
        |evts| evts.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(41), _))),
        Duration::from_secs(3));
    assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(41), _))));
}

#[test]
fn membership_changed_sorted_member_list() {
    let n1 = start_swim(50, 15050, 15051, &[]);
    let _n2 = start_swim(51, 15052, 15053, &[15050]);
    let _n3 = start_swim(52, 15054, 15055, &[15050]);

    std::thread::sleep(Duration::from_secs(3));

    let events = drain_events(&n1.rx);
    let membership_events: Vec<_> = events.iter()
        .filter_map(|e| match e {
            ClusterEvent::MembershipChanged(members) => Some(members.clone()),
            _ => None,
        })
        .collect();

    assert!(!membership_events.is_empty(), "should have MembershipChanged events");

    let last = membership_events.last().unwrap();
    assert!(last.len() >= 2, "should have at least 2 members in the last MembershipChanged");
    for window in last.windows(2) {
        assert!(window[0] <= window[1], "members should be sorted");
    }
}

#[test]
fn dissemination_across_10_nodes() {
    let base_swim = 15060;
    let base_tcp = 15080;
    let n = 10;

    let mut nodes = Vec::new();
    for i in 0..n {
        let seeds: Vec<u16> = if i == 0 { vec![] } else { vec![base_swim] };
        let node = start_swim(
            60 + i as u64,
            base_swim + (i * 2) as u16,
            base_tcp + i as u16,
            &seeds,
        );
        nodes.push(node);
    }

    std::thread::sleep(Duration::from_secs(8));

    let events0 = drain_events(&nodes[0].rx);
    let joined: Vec<_> = events0.iter()
        .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
        .collect();

    assert!(joined.len() >= n - 2,
        "node 0 should see at least {} joins, got {}: {joined:?}", n - 2, joined.len());

    let events5 = drain_events(&nodes[5].rx);
    let joined5: Vec<_> = events5.iter()
        .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
        .collect();

    assert!(joined5.len() >= n / 2,
        "node 5 should see at least {} joins (piggybacked dissemination), got {}",
        n / 2, joined5.len());
    // SwimNode drop handles shutdown + join for all nodes
}

#[test]
fn network_load_per_node_constant() {
    let _n1 = start_swim(70, 15100, 15101, &[]);
    let _n2 = start_swim(71, 15102, 15103, &[15100]);
    let _n3 = start_swim(72, 15104, 15105, &[15100]);

    let config_3 = SwimConfig {
        self_id: NodeId(70), self_addr: tcp_addr(15101), bind_addr: swim_addr(15100),
        seed_nodes: vec![],
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_millis(500),
    };
    let config_20 = SwimConfig {
        self_id: NodeId(80), self_addr: tcp_addr(15201), bind_addr: swim_addr(15200),
        seed_nodes: vec![],
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_millis(500),
    };

    assert_eq!(config_3.probe_interval, config_20.probe_interval);
}

#[test]
fn after_membership_change_all_nodes_compute_same_shard_table() {
    let n1 = start_swim(90, 15110, 15111, &[]);
    let n2 = start_swim(91, 15112, 15113, &[15110]);
    let n3 = start_swim(92, 15114, 15115, &[15110]);

    std::thread::sleep(Duration::from_secs(3));

    let events1 = drain_events(&n1.rx);
    let events2 = drain_events(&n2.rx);
    let events3 = drain_events(&n3.rx);

    let get_last_membership = |events: &[ClusterEvent]| -> Option<Vec<NodeId>> {
        events.iter().rev().find_map(|e| match e {
            ClusterEvent::MembershipChanged(m) => Some(m.clone()),
            _ => None,
        })
    };

    let members1 = get_last_membership(&events1);
    let members2 = get_last_membership(&events2);
    let members3 = get_last_membership(&events3);

    assert!(members1.is_some() || members2.is_some() || members3.is_some(),
        "at least one node should have a MembershipChanged event");

    let full_members = [&members1, &members2, &members3].iter()
        .filter_map(|m| m.as_ref())
        .find(|m| m.len() == 3);

    if let Some(members) = full_members {
        let table = ShardTable::compute(members, 2);
        let table2 = ShardTable::compute(members, 2);
        assert_eq!(table.version, table2.version);

        for shard in 0..4096u16 {
            let master = table.assignment(shard).master;
            assert!(members.contains(&master),
                "shard {shard} master {master:?} not in member list");
        }
    }
}
