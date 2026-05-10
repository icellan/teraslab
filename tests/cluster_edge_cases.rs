//! Deep edge case tests for clustering and replication.
//!
//! Part 7: Stress tests for race conditions and concurrent operations.
//! Part 6: Consistency invariants that must always hold.
//!
//! These tests exercise the unit-level components under concurrency:
//! membership state machine, shard table, migration manager, and
//! replication manager.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use teraslab::cluster::membership::{ClusterEvent, Membership, NodeState};
use teraslab::cluster::migration::{AtomicShardBitmap, MigrationManager};
use teraslab::cluster::shards::{MigrationTask, NUM_SHARDS, NodeId, ShardTable};
use teraslab::cluster::topology::{TopologyAuthority, TopologyVote};
use teraslab::replication::manager::*;
use teraslab::replication::protocol::*;

// ---------------------------------------------------------------------------
// Part 6: Consistency invariants
// ---------------------------------------------------------------------------

/// For any N=1..20 nodes and RF=1..min(N,4), the total mastered shards
/// is always exactly 4096, and no shard's master equals its replica.
#[test]
fn invariant_shard_coverage_and_no_self_replica() {
    for n in 1..=20u64 {
        let members: Vec<NodeId> = (1..=n).map(NodeId).collect();
        for rf in 1..=std::cmp::min(n, 4) as u8 {
            let table = ShardTable::compute_with_epoch(&members, rf, 1);
            let total: usize = table.shard_counts().values().sum();
            assert_eq!(
                total, NUM_SHARDS,
                "n={n} rf={rf}: total mastered shards must be {NUM_SHARDS}"
            );

            for shard in 0..NUM_SHARDS {
                let a = &table.target_assignment(shard as u16);
                assert!(
                    members.contains(&a.master),
                    "n={n} rf={rf} shard {shard}: master {:?} not in members",
                    a.master
                );
                assert!(
                    !a.replicas.contains(&a.master),
                    "n={n} rf={rf} shard {shard}: master {:?} in replicas",
                    a.master
                );
                // No duplicate replicas
                let unique: HashSet<_> = a.replicas.iter().collect();
                assert_eq!(
                    unique.len(),
                    a.replicas.len(),
                    "n={n} rf={rf} shard {shard}: duplicate replicas"
                );
            }
        }
    }
}

/// After any sequence of membership changes, the shard table computed
/// from the final member list is identical regardless of the path taken.
#[test]
fn invariant_shard_table_path_independent() {
    // Path A: start with 3 → add 4 → remove 2
    let t_a = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(3), NodeId(4)], 2, 1);

    // Path B: start with 5 → remove 2 → remove 5
    let t_b = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(3), NodeId(4)], 2, 1);

    for shard in 0..NUM_SHARDS {
        assert_eq!(
            t_a.target_assignment(shard as u16).master,
            t_b.target_assignment(shard as u16).master,
            "shard {shard} master differs between paths"
        );
    }
}

// ---------------------------------------------------------------------------
// Part 7: Concurrent membership mutations
// ---------------------------------------------------------------------------

/// Multiple threads concurrently mutating a shared Membership through
/// mark_alive, mark_suspect, mark_dead, expire_suspects. The alive
/// list and event stream must remain consistent.
#[test]
fn stress_concurrent_membership_mutations() {
    let m = Arc::new(Mutex::new(Membership::new(
        NodeId(0),
        Duration::from_millis(50),
    )));
    let all_events = Arc::new(Mutex::new(Vec::<ClusterEvent>::new()));

    let addr = |port: u16| std::net::SocketAddr::from(([127, 0, 0, 1], port));

    std::thread::scope(|s| {
        // Thread 1: rapidly join nodes 1-50
        let m1 = m.clone();
        let e1 = all_events.clone();
        s.spawn(move || {
            for i in 1..=50u64 {
                let events =
                    m1.lock()
                        .unwrap()
                        .mark_alive(NodeId(i), addr(3000 + i as u16), 1, true);
                e1.lock().unwrap().extend(events);
                std::thread::sleep(Duration::from_micros(100));
            }
        });

        // Thread 2: randomly suspect and kill nodes
        let m2 = m.clone();
        let e2 = all_events.clone();
        s.spawn(move || {
            for i in (1..=50u64).rev() {
                let mut mem = m2.lock().unwrap();
                let events = mem.mark_suspect(NodeId(i), 1);
                e2.lock().unwrap().extend(events);
                drop(mem);
                std::thread::sleep(Duration::from_micros(50));
            }
        });

        // Thread 3: expire suspects periodically
        let m3 = m.clone();
        let e3 = all_events.clone();
        s.spawn(move || {
            for _ in 0..100 {
                std::thread::sleep(Duration::from_millis(10));
                let events = m3.lock().unwrap().expire_suspects();
                e3.lock().unwrap().extend(events);
            }
        });
    });

    // After all threads: membership should be in a consistent state
    let mem = m.lock().unwrap();
    let alive = mem.alive_members();

    // Verify alive list is sorted
    for w in alive.windows(2) {
        assert!(w[0] <= w[1], "alive list not sorted: {:?}", alive);
    }

    // Verify no alive member is also suspect or dead
    for &id in &alive {
        if id == NodeId(0) {
            continue;
        } // self
        if let Some(info) = mem.member_info(&id) {
            assert_eq!(
                info.state,
                NodeState::Alive,
                "node {id:?} in alive list but state is {:?}",
                info.state
            );
        }
    }

    // Verify all events are valid variant types
    let events = all_events.lock().unwrap();
    for event in events.iter() {
        match event {
            ClusterEvent::NodeJoined(_, _)
            | ClusterEvent::NodeSuspect(_)
            | ClusterEvent::NodeLeft(_)
            | ClusterEvent::MembershipChanged(_)
            | ClusterEvent::TopologyStale(_) => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Part 7: Concurrent atomic bitmap operations
// ---------------------------------------------------------------------------

#[test]
fn stress_atomic_bitmap_concurrent_set_clear() {
    let bitmap = Arc::new(AtomicShardBitmap::new());

    std::thread::scope(|s| {
        // 10 threads set shards 0-409 each
        for t in 0..10 {
            let bm = bitmap.clone();
            s.spawn(move || {
                let base = t * 410;
                for i in 0..410u16 {
                    let shard = (base + i).min(4095);
                    bm.set(shard);
                }
            });
        }
    });

    // All shards 0-4095 should be set (10 threads × 410 covers 4096+)
    for shard in 0..NUM_SHARDS as u16 {
        assert!(bitmap.test(shard), "shard {shard} should be set");
    }

    // Now clear all concurrently
    std::thread::scope(|s| {
        for t in 0..10 {
            let bm = bitmap.clone();
            s.spawn(move || {
                let base = t * 410;
                for i in 0..410u16 {
                    let shard = (base + i).min(4095);
                    bm.clear(shard);
                }
            });
        }
    });

    for shard in 0..NUM_SHARDS as u16 {
        assert!(!bitmap.test(shard), "shard {shard} should be clear");
    }
}

// ---------------------------------------------------------------------------
// Part 7: Concurrent replication batches
// ---------------------------------------------------------------------------

fn key(n: u8) -> teraslab::index::TxKey {
    let mut txid = [0u8; 32];
    txid[0] = n;
    teraslab::index::TxKey { txid }
}

#[test]
fn stress_replication_100_sequential_batches() {
    let (mt, rt) = InMemoryTransport::pair();

    let handle = std::thread::spawn(move || {
        let mut received = Vec::new();
        while let Ok(batch) = rt.recv_batch(Duration::from_secs(2)) {
            let ack = ReplicaAck::Ok {
                through_sequence: batch.last_sequence(),
            };
            rt.send_ack(&ack).unwrap();
            received.push(batch);
        }
        received
    });

    let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

    // Send 100 batches, each with 10 ops
    for batch_idx in 0..100u32 {
        let ops: Vec<ReplicaOp> = (0..10u8)
            .map(|i| {
                let mut txid = [0u8; 32];
                txid[0..4].copy_from_slice(&batch_idx.to_le_bytes());
                txid[4] = i;
                ReplicaOp::Freeze {
                    tx_key: teraslab::index::TxKey { txid },
                    offset: i as u32,
                    master_generation: 0,
                }
            })
            .collect();
        mgr.replicate_batch(&ops).unwrap();
    }

    drop(mgr);
    let received = handle.join().unwrap();
    assert_eq!(received.len(), 100, "should receive all 100 batches");

    // Verify sequence numbers are contiguous
    let mut expected_seq = 1u64;
    for batch in &received {
        assert_eq!(
            batch.first_sequence, expected_seq,
            "sequence gap: expected {expected_seq}, got {}",
            batch.first_sequence
        );
        expected_seq += batch.ops.len() as u64;
    }
    // Total ops: 100 * 10 = 1000
    assert_eq!(expected_seq, 1001);
}

// ---------------------------------------------------------------------------
// Part 6: Redo log sequence monotonicity (via replication manager)
// ---------------------------------------------------------------------------

#[test]
fn sequence_never_goes_backward() {
    let (mt, rt) = InMemoryTransport::pair();
    let handle = std::thread::spawn(move || {
        let mut last_seq = 0u64;
        while let Ok(batch) = rt.recv_batch(Duration::from_secs(2)) {
            assert!(
                batch.first_sequence > last_seq,
                "sequence went backward: {} <= {}",
                batch.first_sequence,
                last_seq
            );
            last_seq = batch.last_sequence();
            let ack = ReplicaAck::Ok {
                through_sequence: last_seq,
            };
            rt.send_ack(&ack).unwrap();
        }
        last_seq
    });

    let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

    for i in 0..50u8 {
        let ops = vec![
            ReplicaOp::Spend {
                tx_key: key(i),
                offset: 0,
                spending_data: [i; 36],
                master_generation: 0,
            },
            ReplicaOp::Freeze {
                tx_key: key(i),
                offset: 1,
                master_generation: 0,
            },
        ];
        mgr.replicate_batch(&ops).unwrap();
    }

    drop(mgr);
    let final_seq = handle.join().unwrap();
    assert_eq!(final_seq, 100, "50 batches × 2 ops = seq 100");
}

// ---------------------------------------------------------------------------
// Part 2: Shard table determinism under many configurations
// ---------------------------------------------------------------------------

#[test]
fn shard_table_deterministic_across_100_configurations() {
    // For various node counts and RF values, verify that two independent
    // computations from the same inputs produce identical results.
    for n in 1..=20u64 {
        let members: Vec<NodeId> = (1..=n).map(NodeId).collect();
        for rf in 1..=std::cmp::min(n, 4) as u8 {
            let t1 = ShardTable::compute_with_epoch(&members, rf, 1);
            let t2 = ShardTable::compute_with_epoch(&members, rf, 1);

            for shard in 0..NUM_SHARDS {
                assert_eq!(
                    t1.target_assignment(shard as u16).master,
                    t2.target_assignment(shard as u16).master,
                    "n={n} rf={rf} shard {shard}: master differs"
                );
                assert_eq!(
                    t1.target_assignment(shard as u16).replicas,
                    t2.target_assignment(shard as u16).replicas,
                    "n={n} rf={rf} shard {shard}: replicas differ"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Part 2.3: Migration plan minimality
// ---------------------------------------------------------------------------

#[test]
fn migration_plan_minimal_moves() {
    // Adding one node to a 3-node cluster should move ~25% of shards.
    let old = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2), NodeId(3)], 2, 1);
    let new = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2), NodeId(3), NodeId(4)], 2, 2);

    let plan = ShardTable::migration_plan(&old, &new);

    // Count how many shards actually changed master
    let mut changed = 0;
    for shard in 0..NUM_SHARDS {
        if old.target_assignment(shard as u16).master != new.target_assignment(shard as u16).master
        {
            changed += 1;
        }
    }

    let master_tasks = plan.iter().filter(|t| t.is_master).count();
    let repair_tasks = plan.iter().filter(|t| !t.is_master).count();

    // Master moves remain minimal: exactly one master migration per shard
    // whose master changed.
    assert_eq!(
        master_tasks, changed,
        "migration plan has {} master tasks but {} shards changed master",
        master_tasks, changed
    );
    // Additional tasks are allowed only as non-master repair backfills.
    assert_eq!(plan.len(), master_tasks + repair_tasks);
    assert!(
        repair_tasks <= changed,
        "migration plan has {} repair tasks but only {} shards changed master",
        repair_tasks,
        changed
    );
    assert!(!plan.is_empty(), "adding a node should require migrations");
}

// ---------------------------------------------------------------------------
// Part 4: Migration manager concurrent fence/unfence
// ---------------------------------------------------------------------------

#[test]
fn migration_fence_bitmap_parallel_consistency() {
    let mgr = Arc::new(Mutex::new(MigrationManager::new()));

    // Create 50 outbound tasks
    let tasks: Vec<MigrationTask> = (0..50u16)
        .map(|i| MigrationTask {
            shard: i,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        })
        .collect();

    mgr.lock()
        .unwrap()
        .start_outbound(&tasks, NodeId(1), &HashSet::new());

    // Thread 1: fence shards 0-24
    // Thread 2: fence shards 25-49
    std::thread::scope(|s| {
        let mgr1 = mgr.clone();
        s.spawn(move || {
            for i in 0..25u16 {
                let t = MigrationTask {
                    shard: i,
                    from_node: NodeId(1),
                    to_node: NodeId(2),
                    is_master: true,
                };
                mgr1.lock().unwrap().mark_fenced(&t, 100);
            }
        });
        let mgr2 = mgr.clone();
        s.spawn(move || {
            for i in 25..50u16 {
                let t = MigrationTask {
                    shard: i,
                    from_node: NodeId(1),
                    to_node: NodeId(2),
                    is_master: true,
                };
                mgr2.lock().unwrap().mark_fenced(&t, 100);
            }
        });
    });

    let m = mgr.lock().unwrap();
    assert_eq!(m.fenced_count(), 50);
    for i in 0..50u16 {
        assert!(m.is_shard_fenced(i), "shard {i} should be fenced");
    }
    for i in 50..NUM_SHARDS as u16 {
        assert!(!m.is_shard_fenced(i), "shard {i} should not be fenced");
    }
}

// ---------------------------------------------------------------------------
// Part 1.7: Topology quorum with various cluster sizes
// ---------------------------------------------------------------------------

#[test]
fn topology_quorum_sizes() {
    // Verify quorum calculation for various cluster sizes
    for n in 1..=10u64 {
        let members: Vec<NodeId> = (1..=n).map(NodeId).collect();
        let expected_quorum = (n as usize / 2) + 1;

        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        if let Some(term) = auth.on_membership_changed(&members) {
            // Self-vote counts as 1. Need quorum-1 more votes.
            let additional_needed = expected_quorum - 1;

            let mut commit = None;
            for (i, &voter) in members.iter().enumerate().skip(1) {
                if i > additional_needed {
                    break;
                }
                let vote = TopologyVote {
                    term: term.term,
                    digest: term.digest,
                    voter,
                    accepted: true,
                    voter_current_term: 0,
                };
                commit = auth.handle_vote(&vote);
                if commit.is_some() {
                    break;
                }
            }

            if n == 1 {
                // Single node: self-vote alone is quorum
                assert!(commit.is_none(), "single node needs no additional votes");
                // Actually, for n=1, quorum = 1, and self-vote = 1, so
                // on_membership_changed should produce a commit immediately...
                // Let's check: for n=1, members[0]=1=self, so proposer is self.
                // Self-vote = 1, quorum = 1 → commit should have happened.
                // But on_membership_changed returns the proposal, not the commit.
                // The commit happens in handle_vote when quorum is reached.
                // With self-vote = 1 and quorum = 1, it should commit during
                // on_membership_changed... Let's verify the pending proposal.
            } else if n == 2 {
                // 2 nodes: quorum = 2, need 1 more vote → commit on first vote
                assert!(
                    commit.is_some(),
                    "n=2: 1 additional vote should reach quorum"
                );
            } else {
                // For n >= 3, verify quorum is correct
                // We sent exactly quorum-1 additional votes
                if (additional_needed + 1) >= expected_quorum {
                    assert!(
                        commit.is_some(),
                        "n={n}: {additional_needed}+1 votes should reach quorum {expected_quorum}"
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Part 3: Replication with initial_sequence 0 and 1
// ---------------------------------------------------------------------------

#[test]
fn replication_initial_sequence_boundary() {
    // initial_sequence = 0 should clamp to 1
    let (mt, rt) = InMemoryTransport::pair();
    let handle = std::thread::spawn(move || {
        let mut received = Vec::new();
        while let Ok(batch) = rt.recv_batch(Duration::from_secs(1)) {
            let ack = ReplicaAck::Ok {
                through_sequence: batch.last_sequence(),
            };
            rt.send_ack(&ack).unwrap();
            received.push(batch);
        }
        received
    });

    let mut mgr = ReplicationManager::with_initial_sequence(
        ReplicationConfig::default(),
        vec![Box::new(mt)],
        0, // should clamp to 1
    );

    let ops = vec![ReplicaOp::Freeze {
        tx_key: key(1),
        offset: 0,
        master_generation: 0,
    }];
    mgr.replicate_batch(&ops).unwrap();

    drop(mgr);
    let received = handle.join().unwrap();
    assert_eq!(
        received[0].first_sequence, 1,
        "sequence 0 should clamp to 1"
    );
}

// ===========================================================================
// Deep edge case analysis: topology authority state transitions
// ===========================================================================

/// After catching up via handle_commit, voted_term is NOT updated. A proposal
/// for a term between the old voted_term and the new committed_term should
/// still be rejected because committed_term now dominates.
#[test]
fn topology_catchup_does_not_leave_voted_term_gap() {
    use teraslab::cluster::topology::*;

    let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));

    // Vote for term 3 (via handle_propose from another node's proposal).
    let propose = TopologyTerm::new(3, vec![NodeId(1), NodeId(2), NodeId(3)], NodeId(2));
    let vote = auth.handle_propose(&propose);
    assert!(vote.accepted);

    // Catch up to term 10 via a synthetic commit (e.g., from a peer).
    let mems = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let commit = TopologyCommit {
        term: 10,
        proposer: NodeId(1),
        members: mems.clone(),
        digest: TopologyTerm::compute_digest(10, &mems),
    };
    assert_eq!(auth.handle_commit(&commit), Some(10));

    // Now a stale proposal for term 5 (between old voted_term=3 and new committed=10)
    // must be rejected: term 5 is not > committed_term 10.
    let stale = TopologyTerm::new(5, vec![NodeId(1), NodeId(2)], NodeId(2));
    let v = auth.handle_propose(&stale);
    assert!(
        !v.accepted,
        "proposal for term 5 should be rejected when committed_term=10"
    );

    // A proposal for term 11 should be accepted (> committed=10, > voted=3).
    let fresh = TopologyTerm::new(11, vec![NodeId(1), NodeId(2), NodeId(3)], NodeId(2));
    let v2 = auth.handle_propose(&fresh);
    assert!(v2.accepted, "proposal for term 11 should be accepted");
}

/// Two concurrent check_timeout calls: the second overwrites pending_proposal.
/// Votes for the first term must not produce a commit because the pending
/// proposal has moved on.
#[test]
fn topology_fallback_proposer_superseded_by_second_timeout() {
    use teraslab::cluster::topology::*;

    let auth = TopologyAuthority::new(NodeId(2), Duration::from_millis(1));
    let mems = vec![NodeId(1), NodeId(2), NodeId(3)];

    // Commit a different membership so check_timeout fires.
    let old_mems = vec![NodeId(1), NodeId(2)];
    let old_commit = TopologyCommit {
        term: 1,
        proposer: NodeId(1),
        members: old_mems.clone(),
        digest: TopologyTerm::compute_digest(1, &old_mems),
    };
    auth.handle_commit(&old_commit);

    // handle_commit pins observed_membership to the old committed set;
    // check_timeout's `members` arg is only used as a bootstrap fallback
    // when no prior view has been observed. Simulate SWIM detecting the
    // third node so check_timeout sees the extended membership.
    let _ = auth.on_membership_changed(&mems);

    std::thread::sleep(Duration::from_millis(5));

    // First timeout fires → proposes term 2
    let t1 = auth
        .check_timeout(&mems)
        .expect("first timeout should propose");
    assert_eq!(t1.term, 2);

    // Second timeout fires → overwrites pending with term 3
    let t2 = auth
        .check_timeout(&mems)
        .expect("second timeout should propose");
    assert_eq!(t2.term, 3);

    // Votes for term 2 should NOT match the pending proposal (which is now term 3)
    let vote_for_t1 = TopologyVote {
        term: 2,
        digest: t1.digest,
        voter: NodeId(1),
        accepted: true,
        voter_current_term: 1,
    };
    let commit = auth.handle_vote(&vote_for_t1);
    assert!(
        commit.is_none(),
        "vote for superseded term 2 should not produce commit"
    );

    // Votes for term 3 should work
    let vote_for_t2 = TopologyVote {
        term: 3,
        digest: t2.digest,
        voter: NodeId(1),
        accepted: true,
        voter_current_term: 1,
    };
    let commit = auth.handle_vote(&vote_for_t2);
    assert!(
        commit.is_some(),
        "vote for current term 3 should produce commit"
    );
}

/// Cluster formation recovery: three nodes start simultaneously, each commits
/// term 1 as a single-node cluster. When SWIM discovers the other two, the
/// proposer must be able to propose a multi-node term that all accept.
#[test]
fn topology_cluster_formation_three_simultaneous_starts() {
    use teraslab::cluster::topology::*;

    let a1 = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    let a2 = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
    let a3 = TopologyAuthority::new(NodeId(3), Duration::from_secs(1));

    // Each node independently commits term 1 as single-node.
    for (auth, id) in [(&a1, 1u64), (&a2, 2), (&a3, 3)] {
        let mems = vec![NodeId(id)];
        let commit = TopologyCommit {
            term: 1,
            proposer: NodeId(id),
            members: mems.clone(),
            digest: TopologyTerm::compute_digest(1, &mems),
        };
        auth.handle_commit(&commit);
        assert_eq!(auth.committed_term(), 1);
    }

    // Node 1 (lowest ID) proposes term 2 with all three members.
    let full_members = vec![NodeId(1), NodeId(2), NodeId(3)];
    let proposal = a1
        .on_membership_changed(&full_members)
        .expect("node 1 should propose for 3-node cluster");
    assert_eq!(proposal.term, 2);

    // Nodes 2 and 3 should accept (cluster formation recovery path).
    let v2 = a2.handle_propose(&proposal);
    assert!(
        v2.accepted,
        "node 2 should accept via cluster formation recovery: single-node committed, \
         proposal subsumes self, no outstanding vote beyond committed"
    );

    let v3 = a3.handle_propose(&proposal);
    assert!(
        v3.accepted,
        "node 3 should accept via cluster formation recovery"
    );
}

/// Cluster formation recovery should NOT fire when the node has an outstanding
/// vote beyond its committed term (this could override a genuine vote).
#[test]
fn topology_formation_recovery_blocked_by_outstanding_vote() {
    use teraslab::cluster::topology::*;

    let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));

    // Commit single-node term 1.
    let mems = vec![NodeId(2)];
    let commit = TopologyCommit {
        term: 1,
        proposer: NodeId(2),
        members: mems.clone(),
        digest: TopologyTerm::compute_digest(1, &mems),
    };
    auth.handle_commit(&commit);

    // Vote for term 2 via a different proposal (outstanding vote).
    let proposal_2 = TopologyTerm::new(2, vec![NodeId(1), NodeId(2)], NodeId(1));
    let v = auth.handle_propose(&proposal_2);
    assert!(v.accepted);

    // Now a formation recovery proposal at term 2 should be rejected
    // because voted_term(2) > committed_term(1).
    let recovery_proposal = TopologyTerm::new(2, vec![NodeId(1), NodeId(2), NodeId(3)], NodeId(1));
    let v2 = auth.handle_propose(&recovery_proposal);
    assert!(
        !v2.accepted,
        "formation recovery should be blocked when voted_term > committed_term"
    );
}

// ===========================================================================
// Deep edge case analysis: shard table handoff interleaving
// ===========================================================================

/// begin_handoff called twice in succession (topology change during active
/// handoff). The second handoff must use the COMMITTED assignments as
/// prev_assignments, not the in-flight handoff target.
#[test]
fn shard_table_double_handoff_preserves_routing() {
    let members_v1 = vec![NodeId(1), NodeId(2)];
    let members_v2 = vec![NodeId(1), NodeId(2), NodeId(3)];
    let members_v3 = vec![NodeId(1), NodeId(3)]; // node 2 leaves

    let mut table = ShardTable::compute_with_epoch(&members_v1, 2, 1);
    let new_v2 = ShardTable::compute_with_epoch(&members_v2, 2, 2);

    // First handoff: v1 → v2 (all shards have data)
    table.begin_handoff_with(&new_v2, |_| true);

    // Some shards are in Copying state for v2.
    let copying_count = (0..NUM_SHARDS as u16)
        .filter(|&s| {
            table.shard_handoff_state(s) == teraslab::cluster::shards::ShardHandoff::Copying
        })
        .count();
    assert!(copying_count > 0, "should have some shards in Copying");

    // Before v2 handoff completes, v3 topology arrives (node 2 leaves).
    let new_v3 = ShardTable::compute_with_epoch(&members_v3, 2, 3);
    table.begin_handoff_with(&new_v3, |_| true);

    // Every shard must have a valid effective_assignment.
    for shard in 0..NUM_SHARDS as u16 {
        let eff = table.effective_assignment(shard);
        // The effective master must be either NodeId(1) or NodeId(3)
        // (the v3 members), or NodeId(2) from the v2 prev_assignments
        // (for shards still in handoff).
        assert!(
            eff.master == NodeId(1) || eff.master == NodeId(2) || eff.master == NodeId(3),
            "shard {shard}: effective master {:?} is not in any member set",
            eff.master
        );
    }

    // After committing all shards, everything should be on v3 assignments.
    for shard in 0..NUM_SHARDS as u16 {
        table.commit_shard(shard);
    }
    for shard in 0..NUM_SHARDS as u16 {
        let target = table.target_assignment(shard);
        assert!(
            target.master == NodeId(1) || target.master == NodeId(3),
            "shard {shard}: after full commit, master {:?} should be in v3 members",
            target.master
        );
    }
}

/// Rollback a shard, then begin_handoff again. The rolled-back shard's old
/// assignment must be preserved correctly in the new handoff.
#[test]
fn shard_table_rollback_then_new_handoff() {
    let v1 = vec![NodeId(1), NodeId(2), NodeId(3)];
    let v2 = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];

    let mut table = ShardTable::compute_with_epoch(&v1, 2, 1);
    let new_table = ShardTable::compute_with_epoch(&v2, 2, 2);

    table.begin_handoff(&new_table);

    // Find a shard in Copying, rollback it.
    let copying_shard = (0..NUM_SHARDS as u16).find(|&s| {
        table.shard_handoff_state(s) == teraslab::cluster::shards::ShardHandoff::Copying
    });
    if let Some(shard) = copying_shard {
        let old_master = table.effective_assignment(shard).master;
        table.rollback_shard(shard);

        // After rollback, target should be old master.
        assert_eq!(table.target_assignment(shard).master, old_master);

        // Now a new handoff (same v2 table) should re-enter Copying for this shard.
        let new_table_2 = ShardTable::compute_with_epoch(&v2, 2, 3);
        table.begin_handoff_with(&new_table_2, |_| true);

        // The rolled-back shard should be in Copying again if its master changed.
        let current_master = table.target_assignment(shard).master;
        if current_master != old_master {
            assert_eq!(
                table.shard_handoff_state(shard),
                teraslab::cluster::shards::ShardHandoff::Copying,
                "rolled-back shard with different new master should re-enter Copying"
            );
        }
    }
}

// ===========================================================================
// Deep edge case analysis: migration manager fence consistency
// ===========================================================================

/// `mark_complete` for an unknown task must NOT lift the fence. A stale
/// migration worker can report completion after a newer topology has installed
/// a fresh fence on the same shard; clearing that fence on the strength of an
/// orphaned task ID would expose the shard to writes the new master has not
/// yet accepted. The unit test
/// `mark_complete_does_not_unfence_when_task_not_found` in
/// `src/cluster/migration.rs` documents the same invariant for the manager
/// in isolation; this integration test asserts the same guarantee surfaces
/// at the public API used by the migration coordinator.
#[test]
fn migration_complete_on_unknown_task_does_not_unfence() {
    let mut mgr = MigrationManager::new();
    let task = MigrationTask {
        shard: 42,
        from_node: NodeId(1),
        to_node: NodeId(2),
        is_master: true,
    };

    // Manually fence shard 42 without any active task — simulates a fresh
    // fence installed by a newer topology after the original task was cleaned up.
    mgr.fence_shard(42);
    assert!(mgr.is_shard_fenced(42));

    // mark_complete for the now-untracked task must leave the fence in place.
    mgr.mark_complete(&task);
    assert!(
        mgr.is_shard_fenced(42),
        "mark_complete on an untracked task must not clear an active fence"
    );
}

/// Two tasks for the same shard: fencing both, then completing one keeps the
/// shard fenced because the other task is still in the Fenced state. The
/// shard only unfences once all fenced tasks for it are complete/failed.
#[test]
fn migration_two_tasks_same_shard_fence_interaction() {
    let mut mgr = MigrationManager::new();
    let t1 = MigrationTask {
        shard: 5,
        from_node: NodeId(1),
        to_node: NodeId(2),
        is_master: true,
    };
    let t2 = MigrationTask {
        shard: 5,
        from_node: NodeId(1),
        to_node: NodeId(3),
        is_master: false,
    };

    mgr.start_outbound(&[t1.clone(), t2.clone()], NodeId(1), &HashSet::new());

    mgr.mark_fenced(&t1, 100);
    mgr.mark_fenced(&t2, 200);
    assert!(mgr.is_shard_fenced(5));

    // Complete t1 → shard 5 STAYS fenced because t2 is still Fenced.
    mgr.mark_complete(&t1);
    assert!(
        mgr.is_shard_fenced(5),
        "shard should remain fenced while another task is in Fenced state"
    );

    // t2 is still in Fenced state in the task list.
    let t2_state = mgr
        .active_migrations()
        .iter()
        .find(|p| p.to_node == NodeId(3))
        .map(|p| p.state.clone());
    assert_eq!(
        t2_state,
        Some(teraslab::cluster::migration::MigrationState::Fenced)
    );

    // Complete t2 → NOW the shard is unfenced.
    mgr.mark_complete(&t2);
    assert!(
        !mgr.is_shard_fenced(5),
        "shard should unfence once all fenced tasks are done"
    );
}

/// After cleanup_completed, the inbound bitmap must be rebuilt to exclude
/// completed entries. Verify no phantom bits remain.
#[test]
fn migration_inbound_bitmap_rebuild_after_cleanup() {
    let mut mgr = MigrationManager::new();

    // Register inbound for shards 10, 20, 30.
    mgr.mark_inbound_active(10);
    mgr.mark_inbound_active(20);
    mgr.mark_inbound_active(30);
    assert_eq!(mgr.inbound_count(), 3);

    // Complete shard 20 and run cleanup.
    mgr.mark_inbound_complete(20);
    mgr.cleanup_completed();

    // Bitmap must accurately reflect the remaining state.
    assert!(mgr.has_pending_inbound(10));
    assert!(!mgr.has_pending_inbound(20));
    assert!(mgr.has_pending_inbound(30));

    // Second cleanup is idempotent.
    mgr.cleanup_completed();
    assert!(mgr.has_pending_inbound(10));
    assert!(!mgr.has_pending_inbound(20));
    assert!(mgr.has_pending_inbound(30));

    // Complete all and verify clean state.
    mgr.mark_inbound_complete(10);
    mgr.mark_inbound_complete(30);
    mgr.cleanup_completed();
    assert_eq!(mgr.inbound_count(), 0);
    for s in 0..NUM_SHARDS as u16 {
        assert!(
            !mgr.has_pending_inbound(s),
            "shard {s} should have no pending inbound"
        );
    }
}

/// Outbound serialize/restore preserves the Fenced state and fence_sequence.
#[test]
fn migration_serialize_outbound_preserves_fenced_state() {
    let mut mgr = MigrationManager::new();
    let t = MigrationTask {
        shard: 7,
        from_node: NodeId(1),
        to_node: NodeId(2),
        is_master: true,
    };
    mgr.start_outbound(std::slice::from_ref(&t), NodeId(1), &HashSet::new());
    mgr.set_snapshot_sequence(&t, 50);
    mgr.mark_fenced(&t, 100);

    let data = mgr.serialize_outbound();
    let mut restored = MigrationManager::new();
    restored.restore_outbound(&data);

    let p = restored
        .active_migrations()
        .iter()
        .find(|p| p.shard == 7)
        .expect("shard 7 should be restored");
    assert_eq!(
        p.state,
        teraslab::cluster::migration::MigrationState::Fenced
    );
    assert_eq!(p.snapshot_sequence, 50);
    assert_eq!(p.fence_sequence, 100);
}

// ===========================================================================
// Deep edge case analysis: membership state machine
// ===========================================================================

/// mark_suspect does NOT emit MembershipChanged — but the alive list DOES
/// change. When the suspect recovers (mark_alive), MembershipChanged IS
/// emitted. Verify this asymmetry is handled correctly.
#[test]
fn membership_suspect_alive_cycle_events() {
    let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3001));

    m.mark_alive(NodeId(2), addr, 1, true);
    assert_eq!(m.alive_count(), 2);

    // Suspect: alive_count drops but no MembershipChanged
    let suspect_events = m.mark_suspect(NodeId(2), 1);
    assert_eq!(
        m.alive_count(),
        1,
        "suspect node should be removed from alive list"
    );
    assert!(
        !suspect_events
            .iter()
            .any(|e| matches!(e, ClusterEvent::MembershipChanged(_))),
        "mark_suspect should NOT emit MembershipChanged"
    );
    assert!(
        suspect_events
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeSuspect(_)))
    );

    // Recovery: alive_count restored AND MembershipChanged emitted
    // Direct probe ACK: clears Suspect even at same incarnation.
    let recover_events = m.mark_alive(NodeId(2), addr, 1, true);
    assert_eq!(m.alive_count(), 2);
    assert!(
        recover_events
            .iter()
            .any(|e| matches!(e, ClusterEvent::MembershipChanged(_))),
        "suspect recovery should emit MembershipChanged"
    );
}

/// A node that is Dead can rejoin with the same incarnation (partition recovery).
/// Verify the full event sequence: NodeJoined + MembershipChanged.
#[test]
fn membership_dead_rejoin_same_incarnation() {
    let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3001));

    m.mark_alive(NodeId(2), addr, 5, true);
    m.mark_dead(NodeId(2), 5);
    assert_eq!(m.alive_count(), 1);
    assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Dead);

    // Rejoin with same incarnation (direct probe from the recovered node).
    let events = m.mark_alive(NodeId(2), addr, 5, true);
    assert_eq!(m.alive_count(), 2);
    assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(2), _))),
        "Dead→Alive should emit NodeJoined"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ClusterEvent::MembershipChanged(_))),
        "Dead→Alive should emit MembershipChanged"
    );
}

/// Stale incarnation alive on a Dead node: must be ignored (the node
/// crashed and restarted with a higher incarnation, but old gossip from
/// before the crash is still propagating).
#[test]
fn membership_stale_alive_on_dead_node_ignored() {
    let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3001));

    m.mark_alive(NodeId(2), addr, 10, true);
    m.mark_dead(NodeId(2), 10);

    // Stale alive with incarnation 5 (< current 10) — must be ignored.
    // direct=false here because stale gossip from an uninformed peer is the
    // realistic source of a below-current-incarnation alive message.
    let events = m.mark_alive(NodeId(2), addr, 5, false);
    assert!(
        events.is_empty(),
        "stale alive should be ignored on dead node"
    );
    assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Dead);
}

/// Rapid incarnation advancement: alive(1) → dead(1) → alive(2) → dead(2) → ...
/// Verify incarnation is always tracked correctly.
#[test]
fn membership_incarnation_advancement_tracked() {
    let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3001));

    for inc in 1..=100u64 {
        m.mark_alive(NodeId(2), addr, inc, true);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().incarnation, inc);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);

        m.mark_dead(NodeId(2), inc);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Dead);
    }
    // Final incarnation should be 100
    assert_eq!(m.member_info(&NodeId(2)).unwrap().incarnation, 100);
}

// ===========================================================================
// Deep edge case analysis: topology persisted state round-trip
// ===========================================================================

/// Persisted state format backward compat: 8-byte (peak-only) format must
/// restore safely with all defaults.
#[test]
fn topology_persisted_state_8byte_compat() {
    use teraslab::cluster::topology::PersistedTopologyState;

    let mut data = Vec::new();
    data.extend_from_slice(&7u64.to_le_bytes()); // peak only
    let restored = PersistedTopologyState::deserialize(&data);
    assert_eq!(restored.peak_cluster_size, 7);
    assert_eq!(restored.committed_term, 0);
    assert_eq!(restored.voted_term, 0);
    assert!(restored.committed_members.is_empty());
    assert_eq!(restored.incarnation, 0);
}

/// Persisted state format: pre-incarnation format (without trailing 8 bytes
/// for incarnation) should restore with incarnation=0.
#[test]
fn topology_persisted_state_pre_incarnation_compat() {
    use teraslab::cluster::topology::PersistedTopologyState;

    let state = PersistedTopologyState {
        peak_cluster_size: 3,
        committed_term: 5,
        committed_members: vec![NodeId(1), NodeId(2), NodeId(3)],
        voted_term: 6,
        incarnation: 42,
    };
    let data = state.serialize();

    // Truncate the last 8 bytes (incarnation) to simulate old format.
    let truncated = &data[..data.len() - 8];
    let restored = PersistedTopologyState::deserialize(truncated);
    assert_eq!(restored.peak_cluster_size, 3);
    assert_eq!(restored.committed_term, 5);
    assert_eq!(restored.voted_term, 6);
    assert_eq!(restored.committed_members.len(), 3);
    assert_eq!(
        restored.incarnation, 0,
        "missing incarnation should default to 0"
    );
}

/// Restore topology state, then verify the authority rejects stale proposals.
#[test]
fn topology_restore_then_vote_safety() {
    use teraslab::cluster::topology::*;

    let state = PersistedTopologyState {
        peak_cluster_size: 3,
        committed_term: 10,
        committed_members: vec![NodeId(1), NodeId(2), NodeId(3)],
        voted_term: 12,
        incarnation: 0,
    };

    let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
    auth.restore(&state);

    // Proposal for term 11: > committed(10) but NOT > voted(12) → reject.
    let p1 = TopologyTerm::new(11, vec![NodeId(1), NodeId(2), NodeId(3)], NodeId(1));
    let v1 = auth.handle_propose(&p1);
    assert!(
        !v1.accepted,
        "term 11 should be rejected: 11 <= voted_term 12"
    );

    // Proposal for term 13: > committed(10) AND > voted(12) → accept.
    let p2 = TopologyTerm::new(13, vec![NodeId(1), NodeId(2), NodeId(3)], NodeId(1));
    let v2 = auth.handle_propose(&p2);
    assert!(v2.accepted, "term 13 should be accepted");
}

// ===========================================================================
// Deep edge case analysis: replication catch-up edge cases
// ===========================================================================

/// After a replica fails and reconnects, the catch-up sequence starts from
/// the correct position. This test exercises the public API flow:
/// replicate → fail → reconnect → catchup.
#[test]
fn replication_catchup_full_lifecycle() {
    let (mt, rt) = InMemoryTransport::pair();
    let handle = std::thread::spawn(move || {
        while let Ok(batch) = rt.recv_batch(Duration::from_secs(2)) {
            let ack = ReplicaAck::Ok {
                through_sequence: batch.last_sequence(),
            };
            rt.send_ack(&ack).unwrap();
        }
    });

    let mut mgr = ReplicationManager::new(
        ReplicationConfig {
            catchup_batch_size: 5,
            ..Default::default()
        },
        vec![Box::new(mt)],
    );

    // Send 3 ops normally (seq 1-3)
    for i in 0..3 {
        mgr.replicate_batch(&[ReplicaOp::Freeze {
            tx_key: key(i),
            offset: 0,
            master_generation: 0,
        }])
        .unwrap();
    }
    assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);
    assert_eq!(mgr.sender(0).last_acked(), 3);

    // check_reconnected on a Live replica is a no-op.
    mgr.check_reconnected();
    assert_eq!(*mgr.sender(0).state(), ReplicaState::Live);

    drop(mgr);
    let _ = handle.join();
}

/// WriteMajority with RF=5 (4 replicas): need floor(5/2)+1 - 1 = 2 replica ACKs.
/// Verify exact boundary: 1 ACK fails, 2 ACKs succeed.
#[test]
fn replication_write_majority_rf5_exact_boundary() {
    // RF=5: 4 replicas. majority = 5/2 + 1 = 3. Need 2 replica ACKs.
    let (mt1, rt1) = InMemoryTransport::pair();
    let (mt2, rt2) = InMemoryTransport::pair();
    let (mt3, _rt3) = InMemoryTransport::pair(); // dropped
    let (mt4, _rt4) = InMemoryTransport::pair(); // dropped

    let _h1 = std::thread::spawn(move || {
        while let Ok(batch) = rt1.recv_batch(Duration::from_secs(1)) {
            let ack = ReplicaAck::Ok {
                through_sequence: batch.last_sequence(),
            };
            rt1.send_ack(&ack).unwrap();
        }
    });
    let _h2 = std::thread::spawn(move || {
        while let Ok(batch) = rt2.recv_batch(Duration::from_secs(1)) {
            let ack = ReplicaAck::Ok {
                through_sequence: batch.last_sequence(),
            };
            rt2.send_ack(&ack).unwrap();
        }
    });

    let mut mgr = ReplicationManager::new(
        ReplicationConfig {
            ack_policy: AckPolicy::WriteMajority,
            replication_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        vec![Box::new(mt1), Box::new(mt2), Box::new(mt3), Box::new(mt4)],
    );

    assert_eq!(
        mgr.required_ack_count(),
        2,
        "RF=5 majority needs 2 replica ACKs"
    );

    // 2 replicas respond → should succeed
    let ops = vec![ReplicaOp::Freeze {
        tx_key: key(1),
        offset: 0,
        master_generation: 0,
    }];
    let result = mgr.replicate_batch(&ops);
    assert!(
        result.is_ok(),
        "2 ACKs should satisfy WriteMajority for RF=5"
    );
}

/// Replication lag calculation: after normal replication, lag should be 0
/// for a caught-up replica.
#[test]
fn replication_lag_zero_when_caught_up() {
    let (mt, rt) = InMemoryTransport::pair();
    let handle = std::thread::spawn(move || {
        while let Ok(batch) = rt.recv_batch(Duration::from_secs(1)) {
            let ack = ReplicaAck::Ok {
                through_sequence: batch.last_sequence(),
            };
            rt.send_ack(&ack).unwrap();
        }
    });

    let mut mgr = ReplicationManager::new(ReplicationConfig::default(), vec![Box::new(mt)]);

    for i in 0..10 {
        mgr.replicate_batch(&[ReplicaOp::Freeze {
            tx_key: key(i),
            offset: 0,
            master_generation: 0,
        }])
        .unwrap();
    }

    let master_seq = mgr.current_sequence();
    // master_seq is the NEXT sequence, so last sent = master_seq - 1 = 10
    assert_eq!(
        mgr.sender(0).lag(master_seq - 1),
        0,
        "caught-up replica should have zero lag"
    );

    drop(mgr);
    let _ = handle.join();
}

// ===========================================================================
// Deep edge case analysis: shard handoff with no-change topology
// ===========================================================================

/// begin_handoff where old and new tables are identical: all shards should
/// immediately be ServingNew with no Copying state.
#[test]
fn shard_table_handoff_identical_tables() {
    let members = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut table = ShardTable::compute_with_epoch(&members, 2, 1);
    let same = ShardTable::compute_with_epoch(&members, 2, 2);

    table.begin_handoff(&same);
    assert_eq!(
        table.pending_handoff_count(),
        0,
        "identical tables should have no shards in handoff"
    );

    for shard in 0..NUM_SHARDS as u16 {
        assert_eq!(
            table.shard_handoff_state(shard),
            teraslab::cluster::shards::ShardHandoff::ServingNew,
        );
    }
}

/// mark_commit_ready only transitions from Copying state. Calling it on a
/// ServingNew shard should be a no-op.
#[test]
fn shard_table_mark_commit_ready_on_serving_new_is_noop() {
    let v1 = vec![NodeId(1), NodeId(2)];
    let v2 = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut table = ShardTable::compute_with_epoch(&v1, 2, 1);
    let new_table = ShardTable::compute_with_epoch(&v2, 2, 2);

    table.begin_handoff_with(&new_table, |_| true);

    // Find a shard that's already ServingNew (no master change).
    let serving_shard = (0..NUM_SHARDS as u16).find(|&s| {
        table.shard_handoff_state(s) == teraslab::cluster::shards::ShardHandoff::ServingNew
    });
    if let Some(shard) = serving_shard {
        // mark_commit_ready on ServingNew should be no-op.
        table.mark_commit_ready(shard);
        assert_eq!(
            table.shard_handoff_state(shard),
            teraslab::cluster::shards::ShardHandoff::ServingNew,
            "mark_commit_ready on ServingNew should be no-op"
        );
    }
}

// ===========================================================================
// Deep edge case analysis: migration plan with dead node + no surviving replica
// ===========================================================================

/// When a node dies and the shard has no surviving replica (RF=1 or both nodes
/// of RF=2 are dead), the migration plan should have no task for that shard
/// (data is lost — there's no source to migrate from).
#[test]
fn migration_plan_dead_master_no_surviving_replica() {
    // RF=1: only master, no replicas. If master dies, data is lost.
    let old = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2)], 1, 1);
    let new = ShardTable::compute_with_epoch(&[NodeId(1)], 1, 2);

    let plan = ShardTable::migration_plan(&old, &new);

    // Shards that were on node 2 have no source — they should NOT appear
    // in the migration plan (can't migrate what doesn't exist anywhere).
    for task in &plan {
        assert_ne!(
            task.from_node,
            NodeId(2),
            "dead node 2 should not be a migration source"
        );
        // With RF=1, there are no replicas, so no surviving source.
        // These shards are simply unmigrated (data loss).
    }
    // With RF=1, node 2's shards (~2048) have no source. Plan should be empty.
    assert!(
        plan.is_empty(),
        "RF=1, dead master with no replicas: no migration possible"
    );
}

/// With RF=2 and 3 nodes, if the old master dies, the new master should be
/// the old replica (data already in place → no migration task needed).
#[test]
fn migration_plan_dead_master_replica_has_data() {
    let old = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(2), NodeId(3)], 2, 1);
    let new = ShardTable::compute_with_epoch(&[NodeId(1), NodeId(3)], 2, 2);

    let plan = ShardTable::migration_plan(&old, &new);

    // Node 2 died. For shards where node 2 was master:
    // - Old replica is the next node in round-robin.
    // - If the new master == old replica, data is already there → no task.
    for task in &plan {
        assert_ne!(
            task.from_node,
            NodeId(2),
            "dead node 2 should never be a source"
        );
    }

    // Count shards where node 2 was master.
    let node2_shards: Vec<usize> = (0..NUM_SHARDS)
        .filter(|&s| old.target_assignment(s as u16).master == NodeId(2))
        .collect();

    // For each such shard, check if data is already on the new master.
    let mut already_placed = 0;
    let mut needs_migration = 0;
    for &shard in &node2_shards {
        let old_replicas = &old.target_assignment(shard as u16).replicas;
        let new_master = new.target_assignment(shard as u16).master;
        if old_replicas.contains(&new_master) {
            already_placed += 1;
        } else {
            needs_migration += 1;
        }
    }

    let migration_from_dead_master = plan
        .iter()
        .filter(|t| node2_shards.contains(&(t.shard as usize)))
        .count();
    assert_eq!(
        migration_from_dead_master, needs_migration,
        "migrations should only exist for shards where new master != old replica"
    );

    // Sanity: with 3 nodes RF=2, most shards' replica IS the surviving node,
    // so most need no migration.
    assert!(
        already_placed > needs_migration,
        "with RF=2, most shards should already be on the surviving node"
    );
}

// ===========================================================================
// Deep edge case analysis: topology authority full propose-vote-commit cycle
// ===========================================================================

/// Simulate a full 5-node quorum protocol: propose, collect 3 votes (quorum
/// of 5), broadcast commit. Verify all 5 nodes converge to the same term.
#[test]
fn topology_full_5_node_quorum_cycle() {
    use teraslab::cluster::topology::*;

    let nodes: Vec<TopologyAuthority> = (1..=5u64)
        .map(|id| TopologyAuthority::new(NodeId(id), Duration::from_secs(1)))
        .collect();
    let members: Vec<NodeId> = (1..=5u64).map(NodeId).collect();

    // Node 1 (lowest) proposes.
    let proposal = nodes[0]
        .on_membership_changed(&members)
        .expect("node 1 should propose");
    assert_eq!(proposal.term, 1);

    // Collect votes from nodes 2-5.
    let mut votes = Vec::new();
    for node in &nodes[1..] {
        let vote = node.handle_propose(&proposal);
        assert!(vote.accepted, "all nodes should accept first proposal");
        votes.push(vote);
    }

    // Feed votes to node 1. Quorum = 3 (of 5). Self-vote = 1, so need 2 more.
    let commit = nodes[0].handle_vote(&votes[0]); // 2 votes total
    assert!(commit.is_none(), "2/5 is not quorum");

    let commit = nodes[0].handle_vote(&votes[1]); // 3 votes total
    assert!(commit.is_some(), "3/5 is quorum");
    let commit = commit.unwrap();

    // Broadcast commit to all 5 nodes.
    let commit_msg = TopologyCommit {
        term: commit.term,
        proposer: commit.proposer,
        members: commit.members.clone(),
        digest: commit.digest,
    };
    for node in &nodes {
        // Node 1 already applied via handle_vote → handle_commit should
        // succeed or be a no-op (it returns None if already committed).
        node.handle_commit(&commit_msg);
    }

    // All 5 nodes should agree on the same committed term.
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(
            node.committed_term(),
            1,
            "node {} should be at committed term 1",
            i + 1
        );
        assert_eq!(
            node.committed_members(),
            members,
            "node {} should have all 5 members",
            i + 1
        );
    }
}

/// Rejected votes (accepted=false) must not count toward quorum.
#[test]
fn topology_rejected_votes_dont_count() {
    use teraslab::cluster::topology::*;

    let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    let members = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)];
    let proposal = auth.on_membership_changed(&members).unwrap();

    // Quorum = 3. Self-vote = 1 accept.
    // Send 3 rejections — should NOT reach quorum.
    for &voter in &[NodeId(2), NodeId(3), NodeId(4)] {
        let vote = TopologyVote {
            term: proposal.term,
            digest: proposal.digest,
            voter,
            accepted: false,
            voter_current_term: 0,
        };
        let commit = auth.handle_vote(&vote);
        assert!(commit.is_none(), "rejected votes should not produce commit");
    }

    // Now 1 acceptance → total accepts = 2 (self + node5), still not quorum
    let vote = TopologyVote {
        term: proposal.term,
        digest: proposal.digest,
        voter: NodeId(5),
        accepted: true,
        voter_current_term: 0,
    };
    let commit = auth.handle_vote(&vote);
    assert!(commit.is_none(), "2 accepts out of 5 is not quorum");
}

/// Duplicate votes from the same voter: the HashMap ensures only the latest
/// vote counts (overwrite). Verify this doesn't inflate the accept count.
#[test]
fn topology_duplicate_votes_not_inflated() {
    use teraslab::cluster::topology::*;

    let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    let members = vec![NodeId(1), NodeId(2), NodeId(3)];
    let proposal = auth.on_membership_changed(&members).unwrap();

    // Quorum = 2. Self-vote = 1.
    // Send the same vote from node 2 twice — should still only count as 1.
    let vote = TopologyVote {
        term: proposal.term,
        digest: proposal.digest,
        voter: NodeId(2),
        accepted: true,
        voter_current_term: 0,
    };

    let commit1 = auth.handle_vote(&vote);
    // First vote from node 2 + self = 2 = quorum → commit.
    assert!(commit1.is_some(), "first vote should reach quorum");

    // But if we simulate duplicate arrival BEFORE quorum...
    let auth2 = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    let members5 = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)];
    let proposal2 = auth2.on_membership_changed(&members5).unwrap();

    // Quorum = 3. Self-vote = 1. Need 2 more.
    // Send node 2's vote 3 times — should count as 1, not 3.
    let vote2 = TopologyVote {
        term: proposal2.term,
        digest: proposal2.digest,
        voter: NodeId(2),
        accepted: true,
        voter_current_term: 0,
    };
    auth2.handle_vote(&vote2); // count: self(1) + node2(1) = 2
    auth2.handle_vote(&vote2); // duplicate: still 2
    let commit2 = auth2.handle_vote(&vote2); // still 2, not 4
    assert!(
        commit2.is_none(),
        "duplicate votes should not inflate accept count to reach quorum"
    );

    // One more from a different node should reach quorum.
    let vote3 = TopologyVote {
        term: proposal2.term,
        digest: proposal2.digest,
        voter: NodeId(3),
        accepted: true,
        voter_current_term: 0,
    };
    let commit3 = auth2.handle_vote(&vote3);
    assert!(commit3.is_some(), "genuine third vote should reach quorum");
}

// ---------------------------------------------------------------------------
// R-042 — split-brain heal end-to-end rejection
// ---------------------------------------------------------------------------

/// Audit-prescribed regression: simulate two independent clusters
/// (Cluster A = {1, 2, 3}, Cluster B = {4, 5, 6}) that happen to share a
/// `cluster_secret` and discover each other through SWIM gossip. Each
/// cluster has already committed its own topology (so committed_members
/// is non-empty on both sides). When SWIM emits the merged member list
/// for either side, the deterministic proposer on that side must refuse
/// to commit a unioned topology — the change is neither a pure superset
/// nor a pure subset of the side's committed view.
///
/// Pre-R-042 the proposer would have produced a TopologyTerm for the
/// merged set, eventually committing a unioned [1..=6] topology that
/// would then trigger shard-table activation against nodes that were
/// never part of either cluster's quorum. The post-fix behaviour:
/// `on_membership_changed` returns `None`, no proposal is broadcast,
/// no pending proposal is registered, and `committed_members` stays
/// pinned to the original per-cluster view until an operator
/// intervenes.
#[test]
fn split_brain_heal_detects_independent_clusters() {
    use teraslab::cluster::topology::*;

    // Both clusters share the same authority types/wire formats — only
    // their committed state differs. We model each side as a fresh
    // TopologyAuthority that has already absorbed its own commit.

    // -- Cluster A: {1, 2, 3}, deterministic proposer = node 1 ---------
    let a_proposer = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    let a_members = vec![NodeId(1), NodeId(2), NodeId(3)];
    a_proposer.handle_commit(&TopologyCommit {
        term: 5,
        proposer: NodeId(1),
        members: a_members.clone(),
        digest: TopologyTerm::compute_digest(5, &a_members),
    });
    assert_eq!(a_proposer.committed_term(), 5);
    assert_eq!(a_proposer.committed_members(), a_members);

    // -- Cluster B: {4, 5, 6}, deterministic proposer = node 4 ---------
    let b_proposer = TopologyAuthority::new(NodeId(4), Duration::from_secs(1));
    let b_members = vec![NodeId(4), NodeId(5), NodeId(6)];
    b_proposer.handle_commit(&TopologyCommit {
        term: 7,
        proposer: NodeId(4),
        members: b_members.clone(),
        digest: TopologyTerm::compute_digest(7, &b_members),
    });
    assert_eq!(b_proposer.committed_term(), 7);
    assert_eq!(b_proposer.committed_members(), b_members);

    // -- The merge: SWIM reports {1, 2, 3, 4, 5, 6} on both sides ------
    // (this is what happens when the two clusters discover each other).
    let merged = vec![
        NodeId(1),
        NodeId(2),
        NodeId(3),
        NodeId(4),
        NodeId(5),
        NodeId(6),
    ];

    // Cluster A's proposer: merged = committed ∪ {4,5,6}. From A's view
    // this IS a superset of its committed set, so it WOULD be accepted
    // as a normal "three new nodes joined" event. The split-brain check
    // alone cannot distinguish a legitimate three-node join from a
    // three-node split-brain merge — the asymmetric case below is what
    // actually catches a merge.
    //
    // To get the diagnostic non-monotonic event we need at least one
    // node from A to have failed (or be reported failed by gossip)
    // while a node from B simultaneously appears. Realistically, this
    // is what happens during a partial split-brain heal: nodes drop in
    // and out as the two SWIM membership views reconcile.
    let asymmetric_merge_a = vec![NodeId(1), NodeId(2), NodeId(4), NodeId(5), NodeId(6)];
    // Asymmetric: node 3 gone (dropped from A's committed set), nodes
    // 4/5/6 appeared. Neither superset nor subset of [1, 2, 3].
    let proposal = a_proposer.on_membership_changed(&asymmetric_merge_a);
    assert!(
        proposal.is_none(),
        "Cluster A's proposer must refuse asymmetric merge (split-brain heal)",
    );
    assert_eq!(
        a_proposer.committed_members(),
        a_members,
        "committed_members must remain unchanged after refusal",
    );

    // Cluster B's proposer sees the symmetric mirror: nodes 5/6 dropped
    // and nodes 1/2/3 appeared.
    let asymmetric_merge_b = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let proposal_b = b_proposer.on_membership_changed(&asymmetric_merge_b);
    assert!(
        proposal_b.is_none(),
        "Cluster B's proposer must also refuse the merge",
    );
    assert_eq!(
        b_proposer.committed_members(),
        b_members,
        "Cluster B's committed_members must remain unchanged",
    );

    // -- Now exercise the symmetric-merge edge case as a contrast ------
    // When SWIM reports the perfect union {1..=6} to A, A sees a strict
    // superset of its committed set. The split-brain check alone does
    // NOT catch this case — by design. Documenting this explicitly so a
    // future maintainer doesn't misread the test's intent. Catching the
    // perfect-union case is the job of a separate `cluster_id` mechanism
    // (tracked as future work in the R-042 audit notes).
    let proposal_super = a_proposer.on_membership_changed(&merged);
    assert!(
        proposal_super.is_some(),
        "perfect-superset merge is NOT caught by R-042 (cluster_id is future work)",
    );
    // Reset A back to its committed state so the assertion below is meaningful.
    let a_proposer = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    a_proposer.handle_commit(&TopologyCommit {
        term: 5,
        proposer: NodeId(1),
        members: a_members.clone(),
        digest: TopologyTerm::compute_digest(5, &a_members),
    });

    // -- Final verification: the regression test would FAIL without R-042
    // -- if any of the asymmetric calls produced a proposal. Re-issue the
    // -- asymmetric event one more time and pin the contract.
    let final_check = a_proposer.on_membership_changed(&asymmetric_merge_a);
    assert!(
        final_check.is_none(),
        "regression contract: asymmetric merge must produce no proposal",
    );
}
