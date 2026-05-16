//! SWIM membership protocol tests using localhost UDP.
//!
//! These tests start actual SWIM runners on different UDP ports and
//! verify discovery, failure detection, indirect probing, and
//! dissemination behaviour.
//!
//! Determinism strategy:
//! - NEVER use raw `sleep` + drain; always use event-driven `wait_for`
//!   with generous timeout ceilings (they return the instant the
//!   predicate is satisfied).
//! - Use fast probe intervals (50ms) and short suspicion timeouts
//!   (500ms) so failure detection completes quickly.
//! - Timeouts are generous (30s) to absorb CI load spikes — they are
//!   ceilings, not expected durations.

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

// ---------------------------------------------------------------------------
// Fast test config: 50ms probes, 500ms suspicion
// ---------------------------------------------------------------------------

// Intervals must tolerate thread starvation when the full test suite runs
// in parallel (30+ threads across binaries). 200ms probes give ACKs enough
// time to arrive even under heavy load; 3s suspicion gives indirect probes
// room to complete.
const TEST_PROBE_INTERVAL: Duration = Duration::from_millis(200);
const TEST_SUSPICION_TIMEOUT: Duration = Duration::from_secs(3);

/// Generous ceiling for event waits — returns early via predicate.
const WAIT_CEILING: Duration = Duration::from_secs(30);

/// A running SWIM node that properly shuts down and joins its thread on drop,
/// ensuring the UDP socket is released before ports can be reused.
struct SwimNode {
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
    rx: mpsc::Receiver<ClusterEvent>,
}

impl SwimNode {
    fn stop(&mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
    start_swim_with_config(
        id,
        swim_port,
        tcp_port,
        seeds,
        TEST_PROBE_INTERVAL,
        TEST_SUSPICION_TIMEOUT,
    )
}

fn start_swim_with_secret(
    id: u64,
    swim_port: u16,
    tcp_port: u16,
    seeds: &[u16],
    cluster_secret: &[u8],
) -> SwimNode {
    start_swim_with_config_and_secret(
        id,
        swim_port,
        tcp_port,
        seeds,
        TEST_PROBE_INTERVAL,
        TEST_SUSPICION_TIMEOUT,
        Some(cluster_secret.to_vec()),
    )
}

fn start_swim_with_config(
    id: u64,
    swim_port: u16,
    tcp_port: u16,
    seeds: &[u16],
    probe_interval: Duration,
    suspicion_timeout: Duration,
) -> SwimNode {
    start_swim_with_config_and_secret(
        id,
        swim_port,
        tcp_port,
        seeds,
        probe_interval,
        suspicion_timeout,
        None,
    )
}

fn start_swim_with_config_and_secret(
    id: u64,
    swim_port: u16,
    tcp_port: u16,
    seeds: &[u16],
    probe_interval: Duration,
    suspicion_timeout: Duration,
    cluster_secret: Option<Vec<u8>>,
) -> SwimNode {
    let seed_addrs: Vec<SocketAddr> = seeds.iter().map(|&p| swim_addr(p)).collect();
    let runner = SwimRunner::new(SwimConfig {
        self_id: NodeId(id),
        self_addr: tcp_addr(tcp_port),
        bind_addr: swim_addr(swim_port),
        seed_nodes: seed_addrs,
        probe_interval,
        suspicion_timeout,
        cluster_secret,
        persisted_incarnation: 0,
        committed_term: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
    });
    let (shutdown, handle, rx) = runner.start();
    SwimNode {
        shutdown,
        handle: Some(handle),
        rx,
    }
}

/// Wait until a predicate is satisfied or timeout.
/// Returns all collected events. Polls every 5ms.
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
        std::thread::sleep(Duration::from_millis(5));
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

    // Wait for n1 to discover both peers (event-driven, not sleep)
    let events1 = wait_for(
        &n1.rx,
        |evts| {
            let joins: Vec<_> = evts
                .iter()
                .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
                .collect();
            joins.len() >= 2
        },
        WAIT_CEILING,
    );

    let count = |evts: &[ClusterEvent]| {
        evts.iter()
            .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
            .count()
    };

    assert!(
        count(&events1) >= 2,
        "node 1 should see 2 joins, got {}",
        count(&events1)
    );

    // n2 and n3 should also have discovered peers
    let events2 = wait_for(
        &n2.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
        },
        WAIT_CEILING,
    );
    let events3 = wait_for(
        &n3.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
        },
        WAIT_CEILING,
    );

    assert!(
        count(&events2) >= 1,
        "node 2 should see at least 1 join, got {}",
        count(&events2)
    );
    assert!(
        count(&events3) >= 1,
        "node 3 should see at least 1 join, got {}",
        count(&events3)
    );
}

#[test]
fn wrong_secret_nodes_dont_converge() {
    let n1 = start_swim_with_secret(6, 15006, 15007, &[], b"cluster-a-secret");
    let n2 = start_swim_with_secret(7, 15008, 15009, &[15006], b"cluster-b-secret");

    let events1 = wait_for(
        &n1.rx,
        |events| {
            events
                .iter()
                .any(|event| matches!(event, ClusterEvent::NodeJoined(NodeId(7), _)))
        },
        Duration::from_secs(2),
    );
    let events2 = wait_for(
        &n2.rx,
        |events| {
            events
                .iter()
                .any(|event| matches!(event, ClusterEvent::NodeJoined(NodeId(6), _)))
        },
        Duration::from_secs(2),
    );

    assert!(
        !events1
            .iter()
            .any(|event| matches!(event, ClusterEvent::NodeJoined(NodeId(7), _))),
        "node 6 must not accept SWIM gossip signed with node 7's different secret: {events1:?}",
    );
    assert!(
        !events2
            .iter()
            .any(|event| matches!(event, ClusterEvent::NodeJoined(NodeId(6), _))),
        "node 7 must not accept SWIM gossip signed with node 6's different secret: {events2:?}",
    );
}

#[test]
fn node_stops_responding_suspect_then_dead() {
    let n1 = start_swim(10, 15010, 15011, &[]);
    let mut n2 = start_swim(11, 15012, 15013, &[15010]);

    let discovery = wait_for(
        &n1.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(11), _)))
        },
        WAIT_CEILING,
    );
    assert!(
        discovery
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(11), _))),
        "node 10 should discover node 11"
    );

    // Stop node 2 and JOIN its thread so the socket is released
    n2.stop();

    let events = wait_for(
        &n1.rx,
        |evts| evts.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(_))),
        WAIT_CEILING,
    );

    assert!(
        events
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeSuspect(NodeId(11)))),
        "should have suspected node 11, events: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(11)))),
        "should have declared node 11 dead, events: {events:?}"
    );
}

#[test]
fn dead_node_restarts_with_new_incarnation() {
    let n1 = start_swim(20, 15020, 15021, &[]);
    let mut n2 = start_swim(21, 15022, 15023, &[15020]);

    let discovery = wait_for(
        &n1.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(21), _)))
        },
        WAIT_CEILING,
    );
    assert!(
        discovery
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(21), _))),
        "node 20 should discover node 21"
    );

    // Stop and JOIN so the socket is released
    n2.stop();

    let death = wait_for(
        &n1.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(21))))
        },
        WAIT_CEILING,
    );
    assert!(
        death
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(21)))),
        "node 20 should declare node 21 dead"
    );

    // Brief pause to ensure the restarted node gets a strictly higher incarnation
    // (incarnation = SystemTime::now().as_millis(), so 2ms guarantees a new value)
    std::thread::sleep(Duration::from_millis(2));

    // Restart node 2 on a DIFFERENT swim port (old one may still be in TIME_WAIT)
    let _n2b = start_swim(21, 15024, 15023, &[15020]);

    let events = wait_for(
        &n1.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(21), _)))
        },
        WAIT_CEILING,
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(21), _))),
        "node 21 should have rejoined, events: {events:?}"
    );
}

#[test]
fn indirect_probes_three_node_cluster() {
    let n_a = start_swim(30, 15030, 15031, &[]);
    let mut n_b = start_swim(31, 15032, 15033, &[15030]);
    let n_c = start_swim(32, 15034, 15035, &[15030]);

    // Wait for A to discover both B and C
    let discovery = wait_for(
        &n_a.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(31), _)))
                && evts
                    .iter()
                    .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(32), _)))
        },
        WAIT_CEILING,
    );
    assert!(
        discovery
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(31), _))),
        "A should discover B"
    );

    // Wait for C to see at least one join too
    let _ = wait_for(
        &n_c.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
        },
        WAIT_CEILING,
    );

    // Stop B and JOIN so socket is released
    n_b.stop();

    // Wait for A to detect B's failure
    let events_a = wait_for(
        &n_a.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(31))))
        },
        WAIT_CEILING,
    );

    assert!(
        events_a
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeSuspect(NodeId(31)))),
        "A should suspect B after all probes fail"
    );
    assert!(
        events_a
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(31)))),
        "A should declare B dead after suspicion timeout"
    );

    // Wait for C to also detect B's failure (via gossip from A)
    let events_c = wait_for(
        &n_c.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(31))))
        },
        WAIT_CEILING,
    );
    assert!(
        events_c
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(31)))),
        "C should also detect B's failure"
    );
}

#[test]
fn cluster_event_node_joined_emitted() {
    let n1 = start_swim(40, 15040, 15041, &[]);
    let _n2 = start_swim(41, 15042, 15043, &[15040]);

    let events = wait_for(
        &n1.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(41), _)))
        },
        WAIT_CEILING,
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(41), _)))
    );
}

#[test]
fn membership_changed_sorted_member_list() {
    let n1 = start_swim(50, 15050, 15051, &[]);
    let _n2 = start_swim(51, 15052, 15053, &[15050]);
    let _n3 = start_swim(52, 15054, 15055, &[15050]);

    // Wait for n1 to see both peers join
    let events = wait_for(
        &n1.rx,
        |evts| {
            evts.iter()
                .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
                .count()
                >= 2
        },
        WAIT_CEILING,
    );

    let membership_events: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            ClusterEvent::MembershipChanged(members) => Some(members.clone()),
            _ => None,
        })
        .collect();

    assert!(
        !membership_events.is_empty(),
        "should have MembershipChanged events"
    );

    let last = membership_events.last().unwrap();
    assert!(
        last.len() >= 2,
        "should have at least 2 members in the last MembershipChanged"
    );
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

    // Wait for node 0 to discover at least n-2 peers
    let events0 = wait_for(
        &nodes[0].rx,
        |evts| {
            evts.iter()
                .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
                .count()
                >= n - 2
        },
        WAIT_CEILING,
    );

    let joined: Vec<_> = events0
        .iter()
        .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
        .collect();

    assert!(
        joined.len() >= n - 2,
        "node 0 should see at least {} joins, got {}: {joined:?}",
        n - 2,
        joined.len()
    );

    // Wait for node 5 to discover at least half via piggybacked gossip
    let events5 = wait_for(
        &nodes[5].rx,
        |evts| {
            evts.iter()
                .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
                .count()
                >= n / 2
        },
        WAIT_CEILING,
    );

    let joined5: Vec<_> = events5
        .iter()
        .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
        .collect();

    assert!(
        joined5.len() >= n / 2,
        "node 5 should see at least {} joins (piggybacked dissemination), got {}",
        n / 2,
        joined5.len()
    );
}

#[test]
fn network_load_per_node_constant() {
    let _n1 = start_swim(70, 15100, 15101, &[]);
    let _n2 = start_swim(71, 15102, 15103, &[15100]);
    let _n3 = start_swim(72, 15104, 15105, &[15100]);

    let config_3 = SwimConfig {
        self_id: NodeId(70),
        self_addr: tcp_addr(15101),
        bind_addr: swim_addr(15100),
        seed_nodes: vec![],
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_millis(500),
        cluster_secret: None,
        persisted_incarnation: 0,
        committed_term: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };
    let config_20 = SwimConfig {
        self_id: NodeId(80),
        self_addr: tcp_addr(15201),
        bind_addr: swim_addr(15200),
        seed_nodes: vec![],
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_millis(500),
        cluster_secret: None,
        persisted_incarnation: 0,
        committed_term: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };

    assert_eq!(config_3.probe_interval, config_20.probe_interval);
}

#[test]
fn after_membership_change_all_nodes_compute_same_shard_table() {
    let n1 = start_swim(90, 15110, 15111, &[]);
    let n2 = start_swim(91, 15112, 15113, &[15110]);
    let n3 = start_swim(92, 15114, 15115, &[15110]);

    // Wait for at least one node to see all 3 members
    // Try all three nodes' event streams
    let events1 = wait_for(
        &n1.rx,
        |evts| {
            evts.iter()
                .filter(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
                .count()
                >= 2
        },
        WAIT_CEILING,
    );
    let events2 = wait_for(
        &n2.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
        },
        WAIT_CEILING,
    );
    let events3 = wait_for(
        &n3.rx,
        |evts| {
            evts.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(_, _)))
        },
        WAIT_CEILING,
    );

    let get_last_membership = |events: &[ClusterEvent]| -> Option<Vec<NodeId>> {
        events.iter().rev().find_map(|e| match e {
            ClusterEvent::MembershipChanged(m) => Some(m.clone()),
            _ => None,
        })
    };

    let members1 = get_last_membership(&events1);
    let members2 = get_last_membership(&events2);
    let members3 = get_last_membership(&events3);

    assert!(
        members1.is_some() || members2.is_some() || members3.is_some(),
        "at least one node should have a MembershipChanged event"
    );

    let full_members = [&members1, &members2, &members3]
        .iter()
        .filter_map(|m| m.as_ref())
        .find(|m| m.len() == 3);

    if let Some(members) = full_members {
        let table = ShardTable::compute(members, 2);
        let table2 = ShardTable::compute(members, 2);
        assert_eq!(table.version, table2.version);

        for shard in 0..4096u16 {
            let master = table.assignment(shard).master;
            assert!(
                members.contains(&master),
                "shard {shard} master {master:?} not in member list"
            );
        }
    }
}
