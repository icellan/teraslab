//! Quorum-committed topology authority.
//!
//! Layers a lightweight propose-vote-commit protocol on top of SWIM
//! membership detection to prevent split-brain shard table activation.
//!
//! # Design
//!
//! SWIM detects membership changes fast but is eventually consistent —
//! different nodes may briefly see different alive-member sets. The
//! topology authority adds a quorum gate: a new shard table is only
//! activated after a majority of members acknowledge the same topology
//! term. This prevents a minority partition from independently advancing
//! the shard table.
//!
//! # Protocol
//!
//! 1. On `MembershipChanged`, the deterministic proposer (`members[0]`)
//!    creates a `TopologyTerm` and broadcasts `OP_TOPOLOGY_PROPOSE`.
//! 2. Each node validates and votes (persist `voted_term` first).
//! 3. After quorum, the proposer broadcasts `OP_TOPOLOGY_COMMIT`.
//! 4. All nodes activate the shard table on commit.

use crate::cluster::auth;
use crate::cluster::shards::NodeId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Wire structures
// ---------------------------------------------------------------------------

/// A quorum-committed topology descriptor.
#[derive(Debug, Clone)]
pub struct TopologyTerm {
    /// Strictly monotonic term number.
    pub term: u64,
    /// Sorted list of alive members in this term.
    pub members: Vec<NodeId>,
    /// The node that proposed this term.
    pub proposer: NodeId,
    /// SHA-256 digest of (term || members), used for vote matching.
    pub digest: [u8; 32],
}

impl TopologyTerm {
    /// Create a new term with auto-computed digest.
    pub fn new(term: u64, members: Vec<NodeId>, proposer: NodeId) -> Self {
        let digest = Self::compute_digest(term, &members);
        Self { term, members, proposer, digest }
    }

    /// Compute the canonical digest for a (term, members) pair.
    pub fn compute_digest(term: u64, members: &[NodeId]) -> [u8; 32] {
        let mut buf = Vec::with_capacity(8 + 4 + members.len() * 8);
        buf.extend_from_slice(&term.to_le_bytes());
        buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
        for m in members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        auth::sha256(&buf)
    }

    /// Serialize for the wire.
    ///
    /// Format: `[term:8][proposer:8][member_count:4][member_id:8 * count][digest:32]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(52 + self.members.len() * 8);
        buf.extend_from_slice(&self.term.to_le_bytes());
        buf.extend_from_slice(&self.proposer.0.to_le_bytes());
        buf.extend_from_slice(&(self.members.len() as u32).to_le_bytes());
        for m in &self.members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        buf.extend_from_slice(&self.digest);
        buf
    }

    /// Deserialize from the wire.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 20 { return None; }
        let term = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let proposer = NodeId(u64::from_le_bytes(data[8..16].try_into().ok()?));
        let count = u32::from_le_bytes(data[16..20].try_into().ok()?) as usize;
        let members_end = 20 + count * 8;
        if data.len() < members_end + 32 { return None; }
        let mut members = Vec::with_capacity(count);
        for i in 0..count {
            let off = 20 + i * 8;
            members.push(NodeId(u64::from_le_bytes(data[off..off+8].try_into().ok()?)));
        }
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&data[members_end..members_end + 32]);
        Some(Self { term, members, proposer, digest })
    }
}

/// A node's response to a topology proposal.
#[derive(Debug, Clone)]
pub struct TopologyVote {
    /// The term being voted on.
    pub term: u64,
    /// Digest of the proposed term (must match proposer's).
    pub digest: [u8; 32],
    /// The voter's NodeId.
    pub voter: NodeId,
    /// Whether this node accepts the proposed term.
    pub accepted: bool,
    /// The voter's current highest known term.
    pub voter_current_term: u64,
}

impl TopologyVote {
    /// Serialize for the wire.
    ///
    /// Format: `[term:8][voter:8][digest:32][accepted:1][voter_current_term:8]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(57);
        buf.extend_from_slice(&self.term.to_le_bytes());
        buf.extend_from_slice(&self.voter.0.to_le_bytes());
        buf.extend_from_slice(&self.digest);
        buf.push(if self.accepted { 1 } else { 0 });
        buf.extend_from_slice(&self.voter_current_term.to_le_bytes());
        buf
    }

    /// Deserialize from the wire.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 57 { return None; }
        let term = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let voter = NodeId(u64::from_le_bytes(data[8..16].try_into().ok()?));
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&data[16..48]);
        let accepted = data[48] != 0;
        let voter_current_term = u64::from_le_bytes(data[49..57].try_into().ok()?);
        Some(Self { term, digest, voter, accepted, voter_current_term })
    }
}

/// Broadcast after quorum is achieved, signaling all nodes to activate.
#[derive(Debug, Clone)]
pub struct TopologyCommit {
    pub term: u64,
    pub proposer: NodeId,
    pub members: Vec<NodeId>,
    pub digest: [u8; 32],
}

impl TopologyCommit {
    /// Serialize for the wire.
    ///
    /// Format: `[term:8][proposer:8][member_count:4][member_id:8 * count][digest:32]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(52 + self.members.len() * 8);
        buf.extend_from_slice(&self.term.to_le_bytes());
        buf.extend_from_slice(&self.proposer.0.to_le_bytes());
        buf.extend_from_slice(&(self.members.len() as u32).to_le_bytes());
        for m in &self.members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        buf.extend_from_slice(&self.digest);
        buf
    }

    /// Deserialize from the wire.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        TopologyTerm::deserialize(data).map(|t| Self {
            term: t.term,
            proposer: t.proposer,
            members: t.members,
            digest: t.digest,
        })
    }
}

// ---------------------------------------------------------------------------
// Persisted state
// ---------------------------------------------------------------------------

/// Persisted topology state for crash recovery.
#[derive(Debug, Clone)]
pub struct PersistedTopologyState {
    /// Peak cluster size (existing field).
    pub peak_cluster_size: u64,
    /// Highest committed topology term.
    pub committed_term: u64,
    /// Members of the last committed term.
    pub committed_members: Vec<NodeId>,
    /// Highest term this node voted for (prevents double-voting).
    pub voted_term: u64,
}

impl PersistedTopologyState {
    /// Serialize to bytes.
    ///
    /// Format: `[peak:8][committed_term:8][voted_term:8][member_count:4][member_ids:8 * count]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(28 + self.committed_members.len() * 8);
        buf.extend_from_slice(&self.peak_cluster_size.to_le_bytes());
        buf.extend_from_slice(&self.committed_term.to_le_bytes());
        buf.extend_from_slice(&self.voted_term.to_le_bytes());
        buf.extend_from_slice(&(self.committed_members.len() as u32).to_le_bytes());
        for m in &self.committed_members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        buf
    }

    /// Deserialize from bytes.
    ///
    /// Backward compatible with the old 16-byte `[peak:8][epoch:8]` format.
    pub fn deserialize(data: &[u8]) -> Self {
        if data.len() >= 28 {
            let peak = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]));
            let committed_term = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
            let voted_term = u64::from_le_bytes(data[16..24].try_into().unwrap_or([0; 8]));
            let count = u32::from_le_bytes(data[24..28].try_into().unwrap_or([0; 4])) as usize;
            let mut members = Vec::with_capacity(count);
            for i in 0..count {
                let off = 28 + i * 8;
                if off + 8 <= data.len() {
                    members.push(NodeId(u64::from_le_bytes(data[off..off+8].try_into().unwrap_or([0; 8]))));
                }
            }
            Self { peak_cluster_size: peak.max(1), committed_term, committed_members: members, voted_term }
        } else if data.len() >= 16 {
            // Old format: [peak:8][epoch:8]
            let peak = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]));
            let epoch = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
            Self {
                peak_cluster_size: peak.max(1),
                committed_term: epoch,
                committed_members: Vec::new(),
                voted_term: epoch,
            }
        } else if data.len() >= 8 {
            // Oldest format: [peak:8] only
            let peak = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]));
            Self {
                peak_cluster_size: peak.max(1),
                committed_term: 0,
                committed_members: Vec::new(),
                voted_term: 0,
            }
        } else {
            Self { peak_cluster_size: 1, committed_term: 0, committed_members: Vec::new(), voted_term: 0 }
        }
    }
}

// ---------------------------------------------------------------------------
// TopologyAuthority
// ---------------------------------------------------------------------------

/// Pending proposal state (this node is the proposer).
struct PendingProposal {
    term: TopologyTerm,
    votes: std::collections::HashMap<NodeId, bool>,
    quorum_needed: usize,
    _started_at: Instant,
}

/// Encapsulates the propose-vote-commit state machine.
///
/// Thread-safe: all mutable state is behind a Mutex.
pub struct TopologyAuthority {
    self_id: NodeId,
    /// Highest committed term.
    committed_term: AtomicU64,
    /// Members of the committed term.
    committed_members: Arc<RwLock<Vec<NodeId>>>,
    /// Highest term this node voted for (persisted before responding).
    voted_term: AtomicU64,
    /// Pending proposal (if this node is the proposer).
    pending_proposal: Mutex<Option<PendingProposal>>,
    /// Timeout before a non-proposer becomes a fallback proposer.
    propose_timeout: Duration,
    /// Timestamp of last membership change (for fallback timing).
    last_membership_change: Mutex<Instant>,
}

impl TopologyAuthority {
    /// Create a new authority with default state.
    pub fn new(self_id: NodeId, propose_timeout: Duration) -> Self {
        Self {
            self_id,
            committed_term: AtomicU64::new(0),
            committed_members: Arc::new(RwLock::new(Vec::new())),
            voted_term: AtomicU64::new(0),
            pending_proposal: Mutex::new(None),
            propose_timeout,
            last_membership_change: Mutex::new(Instant::now()),
        }
    }

    /// Restore from persisted state on startup.
    pub fn restore(&self, state: &PersistedTopologyState) {
        self.committed_term.store(state.committed_term, Ordering::Relaxed);
        self.voted_term.store(state.voted_term, Ordering::Relaxed);
        *self.committed_members.write().unwrap() = state.committed_members.clone();
    }

    /// Current committed term.
    pub fn committed_term(&self) -> u64 {
        self.committed_term.load(Ordering::Relaxed)
    }

    /// Members of the committed term.
    pub fn committed_members(&self) -> Vec<NodeId> {
        self.committed_members.read().unwrap().clone()
    }

    /// Current persisted state for saving to disk.
    pub fn persisted_state(&self, peak: u64) -> PersistedTopologyState {
        PersistedTopologyState {
            peak_cluster_size: peak,
            committed_term: self.committed_term.load(Ordering::Relaxed),
            committed_members: self.committed_members.read().unwrap().clone(),
            voted_term: self.voted_term.load(Ordering::Relaxed),
        }
    }

    /// Called when SWIM reports a membership change.
    ///
    /// Returns `Some(TopologyTerm)` if this node should propose
    /// (i.e., this node is the deterministic proposer = `members[0]`).
    pub fn on_membership_changed(&self, members: &[NodeId]) -> Option<TopologyTerm> {
        *self.last_membership_change.lock().unwrap() = Instant::now();

        if members.is_empty() {
            return None;
        }

        // Deterministic proposer: lowest NodeId (members are sorted).
        let proposer = members[0];
        if proposer != self.self_id {
            return None; // Not our turn to propose
        }

        let committed = self.committed_term.load(Ordering::Relaxed);
        let voted = self.voted_term.load(Ordering::Relaxed);
        let new_term = committed.max(voted) + 1;

        let term = TopologyTerm::new(new_term, members.to_vec(), self.self_id);

        // Self-vote
        self.voted_term.store(new_term, Ordering::Relaxed);

        let quorum_needed = (members.len() / 2) + 1;
        let mut votes = std::collections::HashMap::new();
        votes.insert(self.self_id, true);

        *self.pending_proposal.lock().unwrap() = Some(PendingProposal {
            term: term.clone(),
            votes,
            quorum_needed,
            _started_at: Instant::now(),
        });

        Some(term)
    }

    /// Handle an incoming proposal from another node.
    ///
    /// Returns a vote to send back. The caller must persist `voted_term`
    /// before sending the vote (safety requirement).
    pub fn handle_propose(&self, propose: &TopologyTerm) -> TopologyVote {
        let committed = self.committed_term.load(Ordering::Relaxed);
        let voted = self.voted_term.load(Ordering::Relaxed);

        // Validate: term must be strictly higher than anything we've committed or voted for.
        let accepted = propose.term > committed
            && propose.term > voted
            && propose.digest == TopologyTerm::compute_digest(propose.term, &propose.members);

        if accepted {
            // Record vote (must be persisted by caller before sending).
            self.voted_term.store(propose.term, Ordering::Relaxed);
        }

        TopologyVote {
            term: propose.term,
            digest: propose.digest,
            voter: self.self_id,
            accepted,
            voter_current_term: committed,
        }
    }

    /// Handle an incoming vote for our pending proposal.
    ///
    /// Returns `Some(TopologyCommit)` if quorum is reached.
    pub fn handle_vote(&self, vote: &TopologyVote) -> Option<TopologyCommit> {
        let mut pending = self.pending_proposal.lock().unwrap();
        let proposal = pending.as_mut()?;

        // Must match our pending proposal.
        if vote.term != proposal.term.term || vote.digest != proposal.term.digest {
            return None;
        }

        proposal.votes.insert(vote.voter, vote.accepted);

        let accept_count = proposal.votes.values().filter(|&&v| v).count();
        if accept_count >= proposal.quorum_needed {
            let commit = TopologyCommit {
                term: proposal.term.term,
                proposer: proposal.term.proposer,
                members: proposal.term.members.clone(),
                digest: proposal.term.digest,
            };
            // Clear pending proposal
            *pending = None;
            Some(commit)
        } else {
            None
        }
    }

    /// Handle an incoming commit from a proposer.
    ///
    /// Returns `Some(term)` if the commit is valid and was applied,
    /// meaning the caller should activate the shard table with the
    /// committed members.
    pub fn handle_commit(&self, commit: &TopologyCommit) -> Option<u64> {
        let committed = self.committed_term.load(Ordering::Relaxed);

        // Validate: term must be strictly higher.
        if commit.term <= committed {
            return None;
        }

        // Validate digest.
        let expected_digest = TopologyTerm::compute_digest(commit.term, &commit.members);
        if commit.digest != expected_digest {
            return None;
        }

        // Apply commit.
        self.committed_term.store(commit.term, Ordering::Relaxed);
        *self.committed_members.write().unwrap() = commit.members.clone();

        // Clear any pending proposal (superseded by this commit).
        *self.pending_proposal.lock().unwrap() = None;

        Some(commit.term)
    }

    /// Check if the proposal timeout has fired for fallback proposer.
    ///
    /// If this node is not the deterministic proposer but the timeout has
    /// elapsed without receiving a proposal or commit, this node can step
    /// up as a fallback proposer to prevent stalemate.
    ///
    /// Returns `Some(TopologyTerm)` if this node should propose as fallback.
    pub fn check_timeout(&self, members: &[NodeId]) -> Option<TopologyTerm> {
        if members.is_empty() || members[0] == self.self_id {
            return None; // We are already the deterministic proposer
        }

        let elapsed = self.last_membership_change.lock().unwrap().elapsed();
        if elapsed < self.propose_timeout {
            return None; // Still within timeout
        }

        // Check if we already committed for a recent term
        let committed = self.committed_term.load(Ordering::Relaxed);
        let voted = self.voted_term.load(Ordering::Relaxed);

        // Only propose if we haven't already voted for a higher term
        // (which would mean another proposer is active).
        let new_term = committed.max(voted) + 1;

        let term = TopologyTerm::new(new_term, members.to_vec(), self.self_id);
        self.voted_term.store(new_term, Ordering::Relaxed);

        let quorum_needed = (members.len() / 2) + 1;
        let mut votes = std::collections::HashMap::new();
        votes.insert(self.self_id, true);

        *self.pending_proposal.lock().unwrap() = Some(PendingProposal {
            term: term.clone(),
            votes,
            quorum_needed,
            _started_at: Instant::now(),
        });

        Some(term)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn members(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().map(|&id| NodeId(id)).collect()
    }

    #[test]
    fn deterministic_proposer_is_lowest_id() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        // Node 1 is the lowest → should propose
        let term = auth.on_membership_changed(&members(&[1, 2, 3]));
        assert!(term.is_some());
        let t = term.unwrap();
        assert_eq!(t.term, 1);
        assert_eq!(t.proposer, NodeId(1));
        assert_eq!(t.members.len(), 3);
    }

    #[test]
    fn non_proposer_returns_none() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let term = auth.on_membership_changed(&members(&[1, 2, 3]));
        assert!(term.is_none());
    }

    #[test]
    fn vote_accept_valid_proposal() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let propose = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(1));
        let vote = auth.handle_propose(&propose);
        assert!(vote.accepted);
        assert_eq!(vote.term, 1);
        assert_eq!(auth.voted_term.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn vote_reject_stale_proposal() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        // Simulate already having voted for term 5
        auth.voted_term.store(5, Ordering::Relaxed);

        let propose = TopologyTerm::new(3, members(&[1, 2, 3]), NodeId(1));
        let vote = auth.handle_propose(&propose);
        assert!(!vote.accepted);
    }

    #[test]
    fn vote_reject_bad_digest() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let mut propose = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(1));
        propose.digest = [0xFF; 32]; // corrupt
        let vote = auth.handle_propose(&propose);
        assert!(!vote.accepted);
    }

    #[test]
    fn quorum_reached_produces_commit() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let term = auth.on_membership_changed(&members(&[1, 2, 3])).unwrap();

        // Self-vote already recorded. Need 1 more for quorum (2 of 3).
        let vote = TopologyVote {
            term: term.term,
            digest: term.digest,
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
        };
        let commit = auth.handle_vote(&vote);
        assert!(commit.is_some());
        let c = commit.unwrap();
        assert_eq!(c.term, 1);
        assert_eq!(c.members.len(), 3);
    }

    #[test]
    fn quorum_not_reached_without_enough_votes() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let _term = auth.on_membership_changed(&members(&[1, 2, 3, 4, 5])).unwrap();

        // 5 members, quorum = 3. Self-vote = 1. Need 2 more.
        let vote1 = TopologyVote {
            term: 1,
            digest: TopologyTerm::compute_digest(1, &members(&[1, 2, 3, 4, 5])),
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
        };
        let commit = auth.handle_vote(&vote1);
        assert!(commit.is_none()); // Only 2 votes, need 3

        let vote2 = TopologyVote {
            term: 1,
            digest: TopologyTerm::compute_digest(1, &members(&[1, 2, 3, 4, 5])),
            voter: NodeId(3),
            accepted: true,
            voter_current_term: 0,
        };
        let commit = auth.handle_vote(&vote2);
        assert!(commit.is_some()); // Now 3 votes
    }

    #[test]
    fn handle_commit_activates_term() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: mems.clone(),
            digest: TopologyTerm::compute_digest(5, &mems),
        };
        let result = auth.handle_commit(&commit);
        assert_eq!(result, Some(5));
        assert_eq!(auth.committed_term(), 5);
        assert_eq!(auth.committed_members(), mems);
    }

    #[test]
    fn handle_commit_rejects_stale_term() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        auth.committed_term.store(10, Ordering::Relaxed);

        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 5, // stale
            proposer: NodeId(1),
            members: mems.clone(),
            digest: TopologyTerm::compute_digest(5, &mems),
        };
        assert!(auth.handle_commit(&commit).is_none());
    }

    #[test]
    fn handle_commit_rejects_bad_digest() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: mems.clone(),
            digest: [0xFF; 32], // corrupt
        };
        assert!(auth.handle_commit(&commit).is_none());
    }

    #[test]
    fn persisted_state_round_trip() {
        let state = PersistedTopologyState {
            peak_cluster_size: 5,
            committed_term: 42,
            committed_members: members(&[1, 2, 3]),
            voted_term: 43,
        };
        let data = state.serialize();
        let restored = PersistedTopologyState::deserialize(&data);
        assert_eq!(restored.peak_cluster_size, 5);
        assert_eq!(restored.committed_term, 42);
        assert_eq!(restored.voted_term, 43);
        assert_eq!(restored.committed_members.len(), 3);
        assert_eq!(restored.committed_members[0], NodeId(1));
    }

    #[test]
    fn persisted_state_backward_compat_16_bytes() {
        // Old format: [peak:8][epoch:8]
        let mut data = Vec::new();
        data.extend_from_slice(&3u64.to_le_bytes()); // peak
        data.extend_from_slice(&7u64.to_le_bytes()); // epoch
        let restored = PersistedTopologyState::deserialize(&data);
        assert_eq!(restored.peak_cluster_size, 3);
        assert_eq!(restored.committed_term, 7);
        assert_eq!(restored.voted_term, 7);
        assert!(restored.committed_members.is_empty());
    }

    #[test]
    fn wire_format_round_trip() {
        let term = TopologyTerm::new(42, members(&[1, 2, 3]), NodeId(1));
        let data = term.serialize();
        let restored = TopologyTerm::deserialize(&data).unwrap();
        assert_eq!(restored.term, 42);
        assert_eq!(restored.proposer, NodeId(1));
        assert_eq!(restored.members.len(), 3);
        assert_eq!(restored.digest, term.digest);

        let vote = TopologyVote {
            term: 42, digest: term.digest, voter: NodeId(2),
            accepted: true, voter_current_term: 41,
        };
        let vdata = vote.serialize();
        let rv = TopologyVote::deserialize(&vdata).unwrap();
        assert_eq!(rv.term, 42);
        assert!(rv.accepted);
        assert_eq!(rv.voter, NodeId(2));
        assert_eq!(rv.voter_current_term, 41);
    }

    #[test]
    fn cannot_vote_twice_for_same_term() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));

        let p1 = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(1));
        let v1 = auth.handle_propose(&p1);
        assert!(v1.accepted);

        // Second proposal at same term from a different proposer
        let p2 = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(3));
        let v2 = auth.handle_propose(&p2);
        assert!(!v2.accepted); // Already voted for term 1
    }

    #[test]
    fn sequential_terms_advance() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));

        let t1 = auth.on_membership_changed(&members(&[1, 2, 3])).unwrap();
        assert_eq!(t1.term, 1);

        // Simulate commit
        auth.handle_commit(&TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: members(&[1, 2, 3]),
            digest: t1.digest,
        });

        // New membership change → term 2
        let t2 = auth.on_membership_changed(&members(&[1, 2])).unwrap();
        assert_eq!(t2.term, 2);
    }
}
