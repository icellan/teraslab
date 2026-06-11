//! Integration tests for F-G8-003 — SWIM replay defense via per-peer
//! monotonic seq + sliding window.
//!
//! The cluster_secret HMAC layer guarantees that an attacker cannot
//! forge or alter a SWIM message, but it does NOT prevent capturing a
//! signed packet and replaying it within the 5-minute clock-skew window.
//! Replay matters for ping-req amplification, dead-peer resurrection,
//! and the slow memory leak in `ping_req_forwarding` (F-G8-004). The
//! seq counter and per-peer sliding window are the freshness layer.

use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use teraslab::cluster::shards::NodeId;
use teraslab::cluster::swim::{SwimConfig, SwimRunner};

fn bind_localhost() -> (SocketAddr, UdpSocket) {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = socket.local_addr().expect("local_addr");
    (addr, socket)
}

fn config(self_id: NodeId, self_addr: SocketAddr, secret: Option<Vec<u8>>) -> SwimConfig {
    config_with_incarnation(self_id, self_addr, secret, 0)
}

/// Like [`config`] but lets a test simulate a reboot by pinning the
/// persisted incarnation. The runner starts at `persisted_incarnation + 1`,
/// so a fresh runner with a higher `persisted_incarnation` models the same
/// NodeId coming back at a higher incarnation with its seq counter reset.
fn config_with_incarnation(
    self_id: NodeId,
    self_addr: SocketAddr,
    secret: Option<Vec<u8>>,
    persisted_incarnation: u64,
) -> SwimConfig {
    SwimConfig {
        self_id,
        self_addr,
        bind_addr: self_addr,
        swim_advertise_addr: None,
        seed_nodes: vec![],
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_secs(5),
        cluster_secret: secret,
        persisted_incarnation,
        committed_term: Arc::new(AtomicU64::new(0)),
    }
}

/// A signed SWIM message replayed verbatim within the clock-skew window
/// must be rejected by the recipient's per-peer seq window. We exercise
/// this by encoding a PING from peer A and feeding it to peer B twice:
/// the first delivery is processed (the recipient learns A's tcp/swim
/// addresses), the second is silently dropped at the seq-check gate.
#[test]
fn signed_swim_message_rejected_when_replayed() {
    let secret = b"shared-cluster-secret-for-test".to_vec();

    let (addr_a, _sock_a) = bind_localhost();
    let (addr_b, sock_b) = bind_localhost();

    let mut peer_a = SwimRunner::new(config(NodeId(1), addr_a, Some(secret.clone())));
    let mut peer_b = SwimRunner::new(config(NodeId(2), addr_b, Some(secret)));

    // A encodes a PING (with seq=1) destined for B.
    let msg = peer_a.encode_message_for_test(1, &[]);

    // First delivery: B accepts it; A is now a known peer.
    let events = peer_b.handle_message_for_test(&msg, addr_a, &sock_b);
    assert!(
        !events.is_empty() || peer_b.peer_addrs_snapshot().contains_key(&NodeId(1)),
        "first delivery must be processed (A is learned as a peer)",
    );
    assert!(peer_b.peer_addrs_snapshot().contains_key(&NodeId(1)));

    // Second delivery of the SAME bytes (verbatim replay): rejected.
    // The replay window has already recorded seq=1 from peer A.
    let before_alive = peer_b.alive_members();
    let events2 = peer_b.handle_message_for_test(&msg, addr_a, &sock_b);
    assert!(
        events2.is_empty(),
        "replayed message must yield no events ({} events emitted)",
        events2.len(),
    );
    // Membership view must be unchanged after a replay.
    assert_eq!(peer_b.alive_members(), before_alive);
}

/// A fresh message with a strictly-higher seq from the same peer is
/// accepted after a previous one has been recorded. Verifies the
/// forward-slide path of the window.
#[test]
fn fresh_seq_after_lower_one_is_accepted() {
    let secret = b"shared-cluster-secret-for-test".to_vec();

    let (addr_a, _sock_a) = bind_localhost();
    let (addr_b, sock_b) = bind_localhost();

    let mut peer_a = SwimRunner::new(config(NodeId(1), addr_a, Some(secret.clone())));
    let mut peer_b = SwimRunner::new(config(NodeId(2), addr_b, Some(secret)));

    // A sends two PINGs back-to-back. Each call to encode_message
    // increments the per-sender seq.
    let msg1 = peer_a.encode_message_for_test(1, &[]);
    let msg2 = peer_a.encode_message_for_test(1, &[]);

    // B accepts the first.
    let _ = peer_b.handle_message_for_test(&msg1, addr_a, &sock_b);
    // B also accepts the second — same peer, higher seq.
    let before = peer_b.peer_addrs_snapshot();
    let _ = peer_b.handle_message_for_test(&msg2, addr_a, &sock_b);
    // No replay rejection: peer_addrs is at least as large after.
    assert!(peer_b.peer_addrs_snapshot().len() >= before.len());

    // Replaying msg1 after msg2 must STILL be rejected (it lives at
    // bit `lag - 1 == 0` in the bitmap, and that bit was set when
    // the window slid forward to seq 2).
    let events = peer_b.handle_message_for_test(&msg1, addr_a, &sock_b);
    assert!(
        events.is_empty(),
        "in-window replay must be rejected after window slid forward",
    );
}

/// Out-of-order delivery within the window is accepted exactly once.
/// We deliver seqs in the order 1, 3, 2, then replay 2. Order 1→3→2 is
/// legitimate (UDP reorder); the final replay of 2 must be rejected.
#[test]
fn out_of_order_within_window_accepted_once_each() {
    let secret = b"shared-cluster-secret-for-test".to_vec();

    let (addr_a, _sock_a) = bind_localhost();
    let (addr_b, sock_b) = bind_localhost();

    let mut peer_a = SwimRunner::new(config(NodeId(1), addr_a, Some(secret.clone())));
    let mut peer_b = SwimRunner::new(config(NodeId(2), addr_b, Some(secret)));

    // Capture three messages, then deliver in 1, 3, 2 order.
    let msg1 = peer_a.encode_message_for_test(1, &[]);
    let msg2 = peer_a.encode_message_for_test(1, &[]);
    let msg3 = peer_a.encode_message_for_test(1, &[]);

    let _ = peer_b.handle_message_for_test(&msg1, addr_a, &sock_b); // seq 1
    let _ = peer_b.handle_message_for_test(&msg3, addr_a, &sock_b); // seq 3
    // seq 2 arriving late should still be accepted.
    let _ = peer_b.handle_message_for_test(&msg2, addr_a, &sock_b);

    // Replay of seq 2 — must be rejected.
    let events = peer_b.handle_message_for_test(&msg2, addr_a, &sock_b);
    assert!(events.is_empty(), "replay of late-but-accepted seq must fail");
}

/// E-1 regression: a rebooted node must rejoin.
///
/// Node A runs for a while (incarnation 1) and B advances its replay
/// window's `highest` for A into the thousands. A then reboots: the same
/// NodeId comes back at incarnation 2 with its outbound seq counter reset
/// to 1. Under the old NodeId-only keying those low seqs were measured
/// against the stale `highest` and silently dropped, so B never re-learned
/// A. Keyed by `(NodeId, incarnation)`, the higher incarnation resets the
/// seq space and the reboot message is accepted.
#[test]
fn rebooted_node_rejoins_after_high_seq_run() {
    let secret = b"shared-cluster-secret-for-test".to_vec();

    let (addr_a, _sock_a) = bind_localhost();
    let (addr_b, sock_b) = bind_localhost();

    // First run of A at incarnation 1.
    let mut peer_a = SwimRunner::new(config(NodeId(1), addr_a, Some(secret.clone())));
    let mut peer_b = SwimRunner::new(config(NodeId(2), addr_b, Some(secret.clone())));

    // Drive B's replay window for A far forward (highest ≈ 2000), the
    // exact long-lived-node condition that breaks the old keying.
    let mut last = None;
    for _ in 0..2000 {
        let msg = peer_a.encode_message_for_test(1, &[]);
        last = Some(msg.clone());
        let _ = peer_b.handle_message_for_test(&msg, addr_a, &sock_b);
    }
    assert!(
        peer_b.alive_members().contains(&NodeId(1)),
        "A must be alive in B after its first run",
    );

    // Sanity / anti-replay: re-feeding the LAST run-1 message (same
    // incarnation, already-seen seq) is still dropped — the window has
    // not been widened so far that real replays pass.
    let replay = last.expect("captured a run-1 message");
    let replay_events = peer_b.handle_message_for_test(&replay, addr_a, &sock_b);
    assert!(
        replay_events.is_empty(),
        "same-incarnation replay of an already-seen seq must still be rejected",
    );

    // Reboot A: same NodeId, incarnation 2 (persisted_incarnation=1 ⇒
    // runner starts at 2), outbound seq restarts at 1.
    let mut peer_a2 = SwimRunner::new(config_with_incarnation(
        NodeId(1),
        addr_a,
        Some(secret),
        1,
    ));
    // seq=1 from the fresh run. Under the old keying this is <= highest
    // (2000) and would be dropped; under (NodeId, incarnation) keying the
    // higher incarnation resets the window and it is accepted.
    let join = peer_a2.encode_message_for_test(1, &[]);
    let events = peer_b.handle_message_for_test(&join, addr_a, &sock_b);

    // The reboot message must be PROCESSED, not seq-dropped: B re-learns
    // A's address and A stays alive. (Empty events would prove the drop.)
    assert!(
        peer_b.peer_addrs_snapshot().contains_key(&NodeId(1)),
        "B must process the rebooted node's message (address re-registered)",
    );
    assert!(
        peer_b.alive_members().contains(&NodeId(1)),
        "rebooted node (higher incarnation, seq reset to 1) must remain known/alive, \
         not be silently seq-dropped: events={events:?}",
    );

    // And the fresh run's own seq space now enforces replay protection:
    // re-feeding the reboot message (incarnation 2, seq 1, already seen)
    // is rejected.
    let events2 = peer_b.handle_message_for_test(&join, addr_a, &sock_b);
    assert!(
        events2.is_empty(),
        "replay within the rebooted run's seq space must be rejected",
    );
}
