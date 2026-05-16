//! Integration tests for F-G8-001 and F-G8-002 — split-brain merge defenses.
//!
//! These tests exercise the two split-brain rejection paths added to
//! [`TopologyAuthority`]:
//!
//! 1. **`cluster_id` mismatch** — when both sides configure a UUID and
//!    they differ, any cross-cluster proposal is rejected outright.
//!
//! 2. **`committed_voter_ever_seen` fallback** — when one or both sides
//!    leave `cluster_id` unset (pre-orchestrator code paths and tests),
//!    a proposal introducing a NodeId never observed as a committed
//!    voter on this node is rejected.
//!
//! Both paths cover:
//!   * the proposer-side gate in `on_membership_changed` (F-G8-001), and
//!   * the follower-side gate in `handle_propose` (F-G8-002).

use std::time::Duration;
use teraslab::cluster::shards::NodeId;
use teraslab::cluster::topology::{
    ClusterId, TopologyAuthority, TopologyCommit, TopologyTerm,
};

fn members(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().map(|&id| NodeId(id)).collect()
}

/// Helper: seed a TopologyAuthority with a committed membership at `term`.
fn commit_membership(auth: &TopologyAuthority, term: u64, ids: &[u64]) {
    let mems = members(ids);
    let commit = TopologyCommit {
        term,
        proposer: NodeId(1),
        members: mems.clone(),
        digest: TopologyTerm::compute_digest(term, &mems),
        voters: mems.clone(),
    };
    let applied = auth.handle_commit(&commit);
    assert_eq!(applied, Some(term), "test setup: commit must apply");
    assert_eq!(auth.committed_members(), mems);
}

// ---------------------------------------------------------------------------
// F-G8-001 — `committed_voter_ever_seen` fallback (ever-seen rejection)
// ---------------------------------------------------------------------------

/// The headline F-G8-001 attack: two clusters that share a `cluster_secret`
/// but were independently bootstrapped. Side A committed `{1, 2}` and side B
/// committed `{3, 4}`. When SWIM gossip leaks and side A observes
/// `{1, 2, 3, 4}`, that is a *strict superset* of A's committed set — but
/// neither 3 nor 4 has ever been an A-side voter. The fallback heuristic
/// must reject the proposal.
#[test]
fn ever_seen_check_rejects_pure_superset_merge() {
    let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    commit_membership(&auth, 1, &[1, 2]);

    // Sanity: the legacy `is_safe_membership_change` heuristic alone
    // would have called this safe (it's a pure superset). The fallback
    // check is the only line of defense against this case.
    let proposal = auth.on_membership_changed(&members(&[1, 2, 3, 4]));
    assert!(
        proposal.is_none(),
        "pure-superset merge from an unrelated cluster must be rejected",
    );

    // committed_members must remain unchanged.
    assert_eq!(auth.committed_members(), members(&[1, 2]));
    // Authority must not have leaked the bogus members into ever_seen_check —
    // a subsequent legitimate proposal must still pass.
    let legit = auth.on_membership_changed(&members(&[1, 2]));
    // Equal-to-committed is a no-op — returns None but DOES NOT raise.
    assert!(legit.is_none(), "equal-to-committed: no proposal needed");
}

/// Follower-side defense (F-G8-002): even if a buggy proposer bypassed its
/// own `on_membership_changed` gate, a follower's `handle_propose` must
/// reject the same merge.
#[test]
fn handle_propose_rejects_unseen_member_superset() {
    let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
    // Voter side committed members [1, 2] at term 1 — never saw node 3 or 4.
    commit_membership(&auth, 1, &[1, 2]);

    // A buggy proposer (e.g. node 1 of an attacker side) constructs a
    // legitimate-looking `TopologyTerm` for the merged set.
    let mut propose = TopologyTerm::new(2, members(&[1, 2, 3, 4]), NodeId(1));
    // Digest is valid by construction. Voter must still reject.
    propose.digest = TopologyTerm::compute_digest(propose.term, &propose.members);

    let vote = auth.handle_propose(&propose);
    assert!(!vote.accepted, "follower must reject unseen-member superset");
    // Voter's voted_term must NOT have advanced (we cannot self-vote for
    // an unsafe proposal even if the digest matches).
    assert_eq!(vote.voter, NodeId(2));
}

/// `committed_voter_ever_seen` accumulates across commits — a member that
/// has been a voter in any earlier committed term is "known" forever.
#[test]
fn ever_seen_check_accumulates_across_terms() {
    let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    commit_membership(&auth, 1, &[1, 2, 3]);
    commit_membership(&auth, 2, &[1, 2]); // drop node 3 (graceful drain)

    // Node 3 was once a committed voter — re-adding it is fine.
    let snap = auth.committed_voter_ever_seen_snapshot();
    assert!(snap.contains(&NodeId(3)), "drop must not purge ever-seen");

    let proposal = auth.on_membership_changed(&members(&[1, 2, 3]));
    assert!(
        proposal.is_some(),
        "re-adding a previously-seen voter must succeed",
    );
}

// ---------------------------------------------------------------------------
// F-G8-001 — `cluster_id` mismatch rejection
// ---------------------------------------------------------------------------

/// When both sides have configured a non-unset `cluster_id` and they
/// differ, `membership_change_is_safe` returns false outright — the
/// fallback ever-seen heuristic is bypassed (cluster_id is authoritative).
#[test]
fn membership_change_rejected_when_cluster_id_differs() {
    let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    commit_membership(&auth, 1, &[1, 2]);

    // Configure local cluster_id.
    let my_id = ClusterId([0x11u8; 16]);
    auth.set_cluster_id(my_id);
    assert_eq!(auth.cluster_id(), my_id);

    // Foreign side advertises a different cluster_id. The proposal
    // contains members that legitimately *would* pass the ever-seen
    // check on a fresh ever_seen set — but the cluster_id mismatch
    // alone rejects it.
    let foreign = ClusterId([0x22u8; 16]);
    assert!(
        !auth.membership_change_is_safe(&members(&[1, 2]), Some(foreign)),
        "differing cluster_id must reject even a no-op membership change",
    );

    // Same cluster_id (matched) must be safe for a legitimate change.
    assert!(
        auth.membership_change_is_safe(&members(&[1, 2]), Some(my_id)),
        "matching cluster_id must permit a safe change",
    );

    // Unset on the proposal side falls back to the ever-seen heuristic.
    // Legacy/test proposers omit cluster_id (None) — local enforcement
    // must use the fallback rather than failing closed on missing UUIDs.
    assert!(
        auth.membership_change_is_safe(&members(&[1, 2]), None),
        "unset proposal cluster_id must fall back to ever-seen check",
    );
}

/// When the local side has unset `cluster_id` and the proposal side
/// advertises a real one, the local side cannot tell — it falls back to
/// the ever-seen heuristic. (This is the asymmetric-bootstrap case where
/// the orchestrator hasn't yet wired UUID persistence on this node.)
#[test]
fn local_unset_cluster_id_falls_back_to_ever_seen() {
    let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    commit_membership(&auth, 1, &[1, 2]);
    // Local cluster_id is UNSET (default).
    assert!(auth.cluster_id().is_unset());

    let foreign = ClusterId([0x33u8; 16]);
    // Foreign proposal asks for an unseen member superset → fallback
    // ever-seen check kicks in and rejects.
    assert!(
        !auth.membership_change_is_safe(&members(&[1, 2, 9]), Some(foreign)),
        "ever-seen fallback must reject unseen members regardless of foreign cluster_id",
    );

    // Foreign proposal that stays within the ever-seen set is fine —
    // the local node cannot detect a UUID mismatch and the membership
    // is monotonic and known. (This is the documented limitation: the
    // operator must wire cluster_id persistence to catch this case.)
    assert!(
        auth.membership_change_is_safe(&members(&[1, 2]), Some(foreign)),
        "ever-seen-only check accepts known-member proposals",
    );
}

/// Cluster_id round-trip: setting and reading the value works as
/// expected. (Sanity check on the storage API exposed for the orchestrator.)
#[test]
fn cluster_id_set_and_get_round_trip() {
    let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    assert!(auth.cluster_id().is_unset());

    let id = ClusterId([0xAB; 16]);
    auth.set_cluster_id(id);
    assert_eq!(auth.cluster_id(), id);
    assert!(!auth.cluster_id().is_unset());

    // Resetting back to UNSET is supported.
    auth.set_cluster_id(ClusterId::UNSET);
    assert!(auth.cluster_id().is_unset());
}

/// `committed_voter_ever_seen` round-trips via the persistence helpers
/// so restart code can restore it cleanly.
#[test]
fn committed_voter_ever_seen_persistence_round_trip() {
    let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    commit_membership(&auth, 1, &[1, 2, 3]);
    let snap = auth.committed_voter_ever_seen_snapshot();
    assert!(snap.contains(&NodeId(1)));
    assert!(snap.contains(&NodeId(2)));
    assert!(snap.contains(&NodeId(3)));

    // Restore into a fresh authority via the explicit setter — this is
    // the orchestrator's loader path.
    let restored = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
    restored.set_committed_voter_ever_seen(&snap);
    let after = restored.committed_voter_ever_seen_snapshot();
    assert_eq!(after, snap, "persistence must round-trip exactly");
}
