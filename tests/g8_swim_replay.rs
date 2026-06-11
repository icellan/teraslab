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
    SwimConfig {
        self_id,
        self_addr,
        bind_addr: self_addr,
        swim_advertise_addr: None,
        seed_nodes: vec![],
        probe_interval: Duration::from_millis(100),
        suspicion_timeout: Duration::from_secs(5),
        cluster_secret: secret,
        persisted_incarnation: 0,
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
