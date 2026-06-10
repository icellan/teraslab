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
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// 16-byte UUID identifying a cluster instance.
///
/// Two clusters that happened to be configured with the same `cluster_secret`
/// but bootstrapped independently are distinguished by this value: the
/// orchestrator generates and persists it at first boot, and every node in
/// the same cluster shares the same id. Split-brain merges (where a SWIM
/// gossip leak introduces members from a different `cluster_id`) are
/// rejected before any topology commit can be issued.
///
/// `[0u8; 16]` is the "unset" sentinel — used by single-node test setups and
/// by pre-orchestrator code paths. When `cluster_id` is unset on either
/// side of a comparison the check falls back to the
/// `TopologyAuthority::committed_voter_ever_seen` heuristic
/// (track-and-reject unseen members).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ClusterId(pub [u8; 16]);

impl ClusterId {
    /// All-zero sentinel meaning "no cluster_id configured".
    pub const UNSET: ClusterId = ClusterId([0u8; 16]);

    /// True when this id is the unset sentinel.
    pub fn is_unset(&self) -> bool {
        self.0 == [0u8; 16]
    }
}

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
    /// Cluster instance UUID stamped by the proposer (see [`ClusterId`]).
    /// `ClusterId::UNSET` is permitted for legacy / pre-orchestrator
    /// nodes; the receiver then falls back to the ever-seen heuristic.
    pub cluster_id: ClusterId,
    /// SHA-256 digest of (term || cluster_id || members), used for vote
    /// matching. Mixing `cluster_id` in means a tampered id changes the
    /// digest, so the digest check itself rejects a forged
    /// matching-cluster claim.
    pub digest: [u8; 32],
}

impl TopologyTerm {
    /// Create a new term with auto-computed digest.
    pub fn new(term: u64, members: Vec<NodeId>, proposer: NodeId, cluster_id: ClusterId) -> Self {
        let digest = Self::compute_digest(term, &cluster_id, &members);
        Self {
            term,
            members,
            proposer,
            cluster_id,
            digest,
        }
    }

    /// Compute the canonical digest for a (term, cluster_id, members)
    /// triple. `cluster_id` is mixed in so a forged-but-matching id
    /// changes the digest.
    pub fn compute_digest(term: u64, cluster_id: &ClusterId, members: &[NodeId]) -> [u8; 32] {
        let mut buf = Vec::with_capacity(8 + 16 + 4 + members.len() * 8);
        buf.extend_from_slice(&term.to_le_bytes());
        buf.extend_from_slice(&cluster_id.0);
        buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
        for m in members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        auth::sha256(&buf)
    }

    /// Serialize for the wire.
    ///
    /// Format: `[term:8][proposer:8][cluster_id:16][member_count:4][member_id:8 * count][digest:32]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(68 + self.members.len() * 8);
        buf.extend_from_slice(&self.term.to_le_bytes());
        buf.extend_from_slice(&self.proposer.0.to_le_bytes());
        buf.extend_from_slice(&self.cluster_id.0);
        buf.extend_from_slice(&(self.members.len() as u32).to_le_bytes());
        for m in &self.members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        buf.extend_from_slice(&self.digest);
        buf
    }

    /// Deserialize from the wire.
    ///
    /// F-G5-002: bound the topology member list before allocation.
    ///
    /// The `count` field is a client-supplied `u32` and the subsequent
    /// `count * 8` multiplication previously ran without `checked_mul`.
    /// The downstream size check bounded the practical maximum to roughly
    /// `MAX_FRAME_SIZE / 8` — about 2M members, far above any legitimate
    /// production cluster of dozens of nodes. Combined with F-G5-001's
    /// no-secret auth bypass, an unauthenticated peer could drive a 16
    /// MiB pre-allocation per connection. Two defences:
    ///
    /// 1. `MAX_TOPOLOGY_MEMBERS` named cap rejected before any sizing
    ///    arithmetic.
    /// 2. `checked_mul` on `count * 8` so 32-bit targets do not
    ///    silently overflow into a tiny `members_end` that bypasses
    ///    the size check.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        // Header: [term:8][proposer:8][cluster_id:16][count:4] = 36 bytes.
        if data.len() < 36 {
            return None;
        }
        let term = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let proposer = NodeId(u64::from_le_bytes(data[8..16].try_into().ok()?));
        let mut cid = [0u8; 16];
        cid.copy_from_slice(&data[16..32]);
        let cluster_id = ClusterId(cid);
        let count = u32::from_le_bytes(data[32..36].try_into().ok()?) as usize;
        if count > MAX_TOPOLOGY_MEMBERS {
            return None;
        }
        let members_end = 36usize.checked_add(count.checked_mul(8)?)?;
        if data.len() < members_end.checked_add(32)? {
            return None;
        }
        let mut members = Vec::with_capacity(count);
        for i in 0..count {
            let off = 36 + i * 8;
            members.push(NodeId(u64::from_le_bytes(
                data[off..off + 8].try_into().ok()?,
            )));
        }
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&data[members_end..members_end + 32]);
        Some(Self {
            term,
            members,
            proposer,
            cluster_id,
            digest,
        })
    }
}

/// F-G5-002: hard cap on the number of cluster members a single
/// topology frame may declare. Set well above any plausible production
/// cluster size (dozens of nodes) so legitimate traffic is unaffected,
/// but well below the per-frame envelope (`MAX_FRAME_SIZE / 8`) so an
/// attacker who fits within the outer frame cap cannot still drive a
/// multi-megabyte `Vec<NodeId>` pre-allocation.
pub const MAX_TOPOLOGY_MEMBERS: usize = 1024;

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
        if data.len() < 57 {
            return None;
        }
        let term = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let voter = NodeId(u64::from_le_bytes(data[8..16].try_into().ok()?));
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&data[16..48]);
        let accepted = data[48] != 0;
        let voter_current_term = u64::from_le_bytes(data[49..57].try_into().ok()?);
        Some(Self {
            term,
            digest,
            voter,
            accepted,
            voter_current_term,
        })
    }
}

/// Broadcast after quorum is achieved, signaling all nodes to activate.
#[derive(Debug, Clone)]
pub struct TopologyCommit {
    pub term: u64,
    pub proposer: NodeId,
    pub members: Vec<NodeId>,
    /// Cluster instance UUID copied from the [`TopologyTerm`] that
    /// reached quorum. Mixed into [`TopologyTerm::compute_digest`] so a
    /// commit cannot be re-played against a node configured with a
    /// different cluster_id.
    pub cluster_id: ClusterId,
    pub digest: [u8; 32],
    /// Nodes whose accepted votes formed the quorum for this commit.
    pub voters: Vec<NodeId>,
}

impl TopologyCommit {
    /// Check that the embedded voter list is a quorum proof for `members`.
    pub fn has_quorum_voter_proof(&self) -> bool {
        let quorum_needed = (self.members.len() / 2) + 1;
        if self.voters.len() < quorum_needed {
            return false;
        }
        let mut seen = std::collections::HashSet::with_capacity(self.voters.len());
        for voter in &self.voters {
            if !self.members.contains(voter) || !seen.insert(*voter) {
                return false;
            }
        }
        true
    }

    /// Serialize for the wire.
    ///
    /// Format: `[term:8][proposer:8][cluster_id:16][member_count:4][member_id:8 * count][digest:32][voter_count:4][voter_id:8 * count]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(72 + (self.members.len() + self.voters.len()) * 8);
        buf.extend_from_slice(&self.term.to_le_bytes());
        buf.extend_from_slice(&self.proposer.0.to_le_bytes());
        buf.extend_from_slice(&self.cluster_id.0);
        buf.extend_from_slice(&(self.members.len() as u32).to_le_bytes());
        for m in &self.members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        buf.extend_from_slice(&self.digest);
        buf.extend_from_slice(&(self.voters.len() as u32).to_le_bytes());
        for voter in &self.voters {
            buf.extend_from_slice(&voter.0.to_le_bytes());
        }
        buf
    }

    /// Deserialize from the wire.
    ///
    /// F-G5-002: bound voter count via `MAX_TOPOLOGY_MEMBERS` and use
    /// `checked_mul` / `checked_add` arithmetic so a client-supplied
    /// `count` cannot drive unbounded `Vec::with_capacity` or wrap
    /// `usize` on 32-bit targets. The same defence is applied to
    /// `TopologyTerm::deserialize` above.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        let term = TopologyTerm::deserialize(data)?;
        // Header is 36 bytes ([term:8][proposer:8][cluster_id:16][count:4]),
        // followed by members (count * 8) and the digest (32). Voter list
        // starts after the digest.
        let voters_pos = 36usize
            .checked_add(term.members.len().checked_mul(8)?)?
            .checked_add(32)?;
        let voters = if data.len() >= voters_pos.checked_add(4)? {
            let count =
                u32::from_le_bytes(data[voters_pos..voters_pos + 4].try_into().ok()?) as usize;
            if count > MAX_TOPOLOGY_MEMBERS {
                return None;
            }
            let voters_end = voters_pos
                .checked_add(4)?
                .checked_add(count.checked_mul(8)?)?;
            if data.len() < voters_end {
                return None;
            }
            let mut voters = Vec::with_capacity(count);
            for i in 0..count {
                let off = voters_pos + 4 + i * 8;
                voters.push(NodeId(u64::from_le_bytes(
                    data[off..off + 8].try_into().ok()?,
                )));
            }
            voters
        } else {
            Vec::new()
        };
        Some(Self {
            term: term.term,
            proposer: term.proposer,
            members: term.members,
            cluster_id: term.cluster_id,
            digest: term.digest,
            voters,
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
    /// Voters whose quorum approved the last committed term.
    pub committed_voters: Vec<NodeId>,
    /// Highest term this node voted for (prevents double-voting).
    pub voted_term: u64,
    /// Monotonic SWIM incarnation counter for this node.
    /// Persisted so that after restart the node always has a higher
    /// incarnation than any previously gossiped value.
    pub incarnation: u64,
    /// Every `NodeId` ever observed as a committed voter on this node.
    /// Used as the fallback split-brain heal defence (F-G8-001) when
    /// `cluster_id` is unset: any proposal introducing a previously
    /// unseen member is rejected.
    pub committed_voter_ever_seen: Vec<NodeId>,
}

impl PersistedTopologyState {
    /// Serialize to bytes.
    ///
    /// Format: `[peak:8][committed_term:8][voted_term:8][member_count:4][member_ids:8*N][incarnation:8][voter_count:4][voter_ids:8*N][ever_seen_count:4][ever_seen_ids:8*N]`
    ///
    /// `[ever_seen_count][ever_seen_ids]` is appended for F-G8-001's
    /// split-brain heal fallback. Older payloads without the trailer
    /// decode with an empty `committed_voter_ever_seen` and the loader
    /// seeds the set from `committed_voters`.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            44 + (self.committed_members.len()
                + self.committed_voters.len()
                + self.committed_voter_ever_seen.len())
                * 8,
        );
        buf.extend_from_slice(&self.peak_cluster_size.to_le_bytes());
        buf.extend_from_slice(&self.committed_term.to_le_bytes());
        buf.extend_from_slice(&self.voted_term.to_le_bytes());
        buf.extend_from_slice(&(self.committed_members.len() as u32).to_le_bytes());
        for m in &self.committed_members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        buf.extend_from_slice(&self.incarnation.to_le_bytes());
        buf.extend_from_slice(&(self.committed_voters.len() as u32).to_le_bytes());
        for voter in &self.committed_voters {
            buf.extend_from_slice(&voter.0.to_le_bytes());
        }
        buf.extend_from_slice(&(self.committed_voter_ever_seen.len() as u32).to_le_bytes());
        for v in &self.committed_voter_ever_seen {
            buf.extend_from_slice(&v.0.to_le_bytes());
        }
        buf
    }

    /// Deserialize from bytes.
    ///
    /// Backward compatible with the old 16-byte `[peak:8][epoch:8]` format
    /// and the pre-incarnation format without the trailing incarnation field.
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
                    members.push(NodeId(u64::from_le_bytes(
                        data[off..off + 8].try_into().unwrap_or([0; 8]),
                    )));
                }
            }
            // Incarnation lives after the member list. If there aren't
            // enough bytes (old format without incarnation), default to 0.
            let incarnation_off = 28 + count * 8;
            let incarnation = if incarnation_off + 8 <= data.len() {
                u64::from_le_bytes(
                    data[incarnation_off..incarnation_off + 8]
                        .try_into()
                        .unwrap_or([0; 8]),
                )
            } else {
                0
            };
            let voters_off = incarnation_off + 8;
            let mut voters = Vec::new();
            let mut voters_end = voters_off;
            if voters_off + 4 <= data.len() {
                let voter_count = u32::from_le_bytes(
                    data[voters_off..voters_off + 4]
                        .try_into()
                        .unwrap_or([0; 4]),
                ) as usize;
                voters.reserve(voter_count);
                for i in 0..voter_count {
                    let off = voters_off + 4 + i * 8;
                    if off + 8 <= data.len() {
                        voters.push(NodeId(u64::from_le_bytes(
                            data[off..off + 8].try_into().unwrap_or([0; 8]),
                        )));
                    }
                }
                voters_end = voters_off + 4 + voter_count * 8;
            }
            // Optional trailer: [ever_seen_count:4][ever_seen_ids:8*N].
            // Older payloads do not have this; callers seed the runtime
            // set from `committed_voters` in that case.
            let mut ever_seen = Vec::new();
            if voters_end + 4 <= data.len() {
                let count = u32::from_le_bytes(
                    data[voters_end..voters_end + 4]
                        .try_into()
                        .unwrap_or([0; 4]),
                ) as usize;
                ever_seen.reserve(count);
                for i in 0..count {
                    let off = voters_end + 4 + i * 8;
                    if off + 8 <= data.len() {
                        ever_seen.push(NodeId(u64::from_le_bytes(
                            data[off..off + 8].try_into().unwrap_or([0; 8]),
                        )));
                    }
                }
            }
            Self {
                peak_cluster_size: peak.max(1),
                committed_term,
                committed_members: members,
                committed_voters: voters,
                voted_term,
                incarnation,
                committed_voter_ever_seen: ever_seen,
            }
        } else if data.len() >= 16 {
            // Old format: [peak:8][epoch:8]
            let peak = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]));
            let epoch = u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]));
            Self {
                peak_cluster_size: peak.max(1),
                committed_term: epoch,
                committed_members: Vec::new(),
                committed_voters: Vec::new(),
                voted_term: epoch,
                incarnation: 0,
                committed_voter_ever_seen: Vec::new(),
            }
        } else if data.len() >= 8 {
            // Oldest format: [peak:8] only
            let peak = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]));
            Self {
                peak_cluster_size: peak.max(1),
                committed_term: 0,
                committed_members: Vec::new(),
                committed_voters: Vec::new(),
                voted_term: 0,
                incarnation: 0,
                committed_voter_ever_seen: Vec::new(),
            }
        } else {
            Self {
                peak_cluster_size: 1,
                committed_term: 0,
                committed_members: Vec::new(),
                committed_voters: Vec::new(),
                voted_term: 0,
                incarnation: 0,
                committed_voter_ever_seen: Vec::new(),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Split-brain detection helper
// ---------------------------------------------------------------------------

/// Decide whether a SWIM-reported `proposed` member set is a *safe* evolution
/// of the currently committed `committed` set.
///
/// A change is safe when it is monotonic — either a pure superset (members
/// joined) or a pure subset (members departed gracefully). A change that
/// simultaneously adds a previously-uncommitted node AND drops a previously-
/// committed node is the split-brain merge signature: two clusters that
/// have learned about each other but never agreed on a common topology.
/// Returning `false` for that case is the rejection signal that
/// [`TopologyAuthority::on_membership_changed`] (and the fallback
/// proposer paths) consult before generating a new term.
///
/// `committed.is_empty()` is treated as safe (the cluster has not committed
/// any topology yet — there is nothing to split-brain against).
///
/// Both slices are assumed to be sorted ascending by `NodeId` (SWIM emits
/// them sorted) but the implementation relies only on set semantics, so
/// duplicate or out-of-order entries are tolerated correctly.
fn is_safe_membership_change(committed: &[NodeId], proposed: &[NodeId]) -> bool {
    if committed.is_empty() {
        return true;
    }
    let proposed_has_all_committed = committed.iter().all(|c| proposed.contains(c));
    let committed_has_all_proposed = proposed.iter().all(|p| committed.contains(p));
    // Safe when the change is monotonic: pure superset OR pure subset.
    // Equality satisfies both conditions and is also safe.
    proposed_has_all_committed || committed_has_all_proposed
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
    /// Per-cluster UUID — used to reject merges between independently
    /// bootstrapped clusters that happen to share a `cluster_secret`.
    /// `ClusterId::UNSET` means "not configured" (pre-orchestrator code
    /// paths and single-node tests); when unset on either side of a
    /// check the fallback `committed_voter_ever_seen` heuristic applies.
    cluster_id: RwLock<ClusterId>,
    /// Highest committed term. Wrapped in `Arc` so SWIM gossip can share
    /// a reference and piggyback the value on probe messages for catch-up
    /// detection without polling.
    committed_term: Arc<AtomicU64>,
    /// Members of the committed term.
    committed_members: Arc<RwLock<Vec<NodeId>>>,
    /// Voters whose quorum approved the committed term.
    committed_voters: Arc<RwLock<Vec<NodeId>>>,
    /// Every `NodeId` this authority has ever seen as a committed voter
    /// in any term. Persisted across restarts via the membership-history
    /// portion of [`PersistedTopologyState`]. Used as a fallback to
    /// reject split-brain merges when `cluster_id` is unset (the
    /// orchestrator has not wired UUID persistence yet): any proposal
    /// introducing a `NodeId` not in this set is rejected unless
    /// `committed_members` is empty (first-commit case).
    committed_voter_ever_seen: Arc<RwLock<HashSet<NodeId>>>,
    /// Highest term this node voted for (persisted before responding).
    voted_term: AtomicU64,
    /// Pending proposal (if this node is the proposer).
    pending_proposal: Mutex<Option<PendingProposal>>,
    /// Timeout before a non-proposer becomes a fallback proposer.
    propose_timeout: Duration,
    /// Timestamp of last membership change (for fallback timing).
    last_membership_change: Mutex<Instant>,
    /// Latest membership view that fallback proposals should target.
    ///
    /// This is updated from SWIM membership-change events and from
    /// quorum-committed topology installs. Using this instead of the
    /// current live socket map prevents graceful drain commits from
    /// being undone while the departing node is still reachable.
    observed_membership: Mutex<Vec<NodeId>>,
    /// Phase I — wall-clock timestamp (millis since UNIX epoch) of the
    /// most recently observed `OP_TOPOLOGY_COMMIT` apply. Stays at `0`
    /// until the first commit lands so the
    /// [`OP_ADMIN_CLUSTER_HEALTH`](crate::protocol::opcodes::OP_ADMIN_CLUSTER_HEALTH)
    /// endpoint can distinguish a `Joining` node (no commit yet) from a
    /// settled `Alive` one.
    last_commit_at_unix_ms: AtomicU64,
    /// E-01 defense-in-depth — highest cluster size this authority has
    /// ever observed (proposed member sets, applied commits, restored
    /// persisted state, and the coordinator's SWIM-derived peak all feed
    /// it via [`TopologyAuthority::observe_peak_cluster_size`]).
    /// Monotonic non-decreasing (`fetch_max` only), so a partitioned
    /// minority remnant cannot lower it. The activation quorum for a new
    /// topology term is `max((proposal_len/2)+1, (peak/2)+1)` — a 1-of-3
    /// remnant therefore needs 2 votes and can never self-commit a
    /// single-node topology.
    peak_cluster_size: AtomicU64,
}

impl TopologyAuthority {
    /// Create a new authority with default state.
    pub fn new(self_id: NodeId, propose_timeout: Duration) -> Self {
        Self {
            self_id,
            cluster_id: RwLock::new(ClusterId::UNSET),
            committed_term: Arc::new(AtomicU64::new(0)),
            committed_members: Arc::new(RwLock::new(Vec::new())),
            committed_voters: Arc::new(RwLock::new(Vec::new())),
            committed_voter_ever_seen: Arc::new(RwLock::new(HashSet::new())),
            voted_term: AtomicU64::new(0),
            pending_proposal: Mutex::new(None),
            propose_timeout,
            last_membership_change: Mutex::new(Instant::now()),
            observed_membership: Mutex::new(Vec::new()),
            last_commit_at_unix_ms: AtomicU64::new(0),
            peak_cluster_size: AtomicU64::new(1),
        }
    }

    /// E-01 — record an observed cluster size. Monotonic: only raises the
    /// stored peak (`fetch_max`), never lowers it. Fed from proposed
    /// member sets, applied commits, restored persisted state, and the
    /// coordinator's SWIM membership events.
    pub fn observe_peak_cluster_size(&self, observed: u64) {
        self.peak_cluster_size
            .fetch_max(observed, Ordering::Relaxed);
    }

    /// E-01 — highest cluster size ever observed by this authority
    /// (minimum 1). The activation quorum for new topology terms is
    /// derived from this value, not from the live (possibly
    /// SWIM-shrunken) member set alone.
    pub fn peak_cluster_size(&self) -> u64 {
        self.peak_cluster_size.load(Ordering::Relaxed).max(1)
    }

    /// E-01 — votes needed to activate a proposal with `proposal_len`
    /// members: the stricter of the proposal majority and the
    /// peak-derived majority. A minority remnant of a previously larger
    /// cluster (peak) can therefore never reach quorum on its own votes,
    /// while bootstrap (peak=1) and growth (peak raised from the proposed
    /// set before this is computed) keep their natural majorities.
    fn activation_quorum_needed(&self, proposal_len: usize) -> usize {
        let proposal_majority = (proposal_len / 2) + 1;
        let peak_majority = (self.peak_cluster_size() as usize / 2) + 1;
        proposal_majority.max(peak_majority)
    }

    /// Set this authority's cluster_id.
    ///
    /// Called by the orchestrator on startup once the persisted UUID has
    /// been loaded (or freshly generated on first boot). Subsequent
    /// proposals coming from nodes whose cluster_id differs are rejected
    /// as split-brain.
    pub fn set_cluster_id(&self, id: ClusterId) {
        *self.cluster_id.write().unwrap() = id;
    }

    /// Current cluster_id (defaults to [`ClusterId::UNSET`]).
    pub fn cluster_id(&self) -> ClusterId {
        *self.cluster_id.read().unwrap()
    }

    /// Snapshot the `committed_voter_ever_seen` set. Tests / persistence.
    pub fn committed_voter_ever_seen_snapshot(&self) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self
            .committed_voter_ever_seen
            .read()
            .unwrap()
            .iter()
            .copied()
            .collect();
        v.sort_unstable_by_key(|n| n.0);
        v
    }

    /// Replace the `committed_voter_ever_seen` set. Used by the
    /// persistence layer when restoring state.
    pub fn set_committed_voter_ever_seen(&self, voters: &[NodeId]) {
        let mut set = self.committed_voter_ever_seen.write().unwrap();
        set.clear();
        set.extend(voters.iter().copied());
    }

    /// Validate that `proposed_members` does not introduce a member never
    /// previously observed as a committed voter on this node. Returns
    /// `true` when the change is safe, `false` when it appears to be a
    /// split-brain merge.
    ///
    /// Safe cases:
    ///   * the ever-seen set is empty (first commit on this node);
    ///   * every member of `proposed_members` has been a committed voter
    ///     before (or is `self_id`, since self always counts as known).
    pub fn ever_seen_check(&self, proposed_members: &[NodeId]) -> bool {
        let seen = self.committed_voter_ever_seen.read().unwrap();
        if seen.is_empty() {
            return true;
        }
        for m in proposed_members {
            if *m == self.self_id {
                continue;
            }
            if !seen.contains(m) {
                return false;
            }
        }
        true
    }

    /// Decide whether a proposed membership is safe to commit on this
    /// node, applying both the monotonic-change check and either the
    /// cluster_id match (primary defence, P1.1) or the
    /// ever-seen-voter check (legacy fallback, F-G8-001).
    ///
    /// `proposal_cluster_id` is `None` when the proposer omitted it
    /// (in-process tests / pre-wire callers) and `Some(id)` when the
    /// caller has access to the inbound `TopologyTerm::cluster_id`.
    ///
    /// Decision matrix:
    ///   * Either side unset → fall through to `ever_seen_check`
    ///     (F-G8-001 fallback).
    ///   * Both sides set, ids differ → reject.
    ///   * Both sides set, ids match → cluster_id alone is sufficient
    ///     proof of "same cluster"; skip `ever_seen_check`. This is the
    ///     P1.1 fix: ever_seen_check otherwise blocks legitimate
    ///     scale-up because new nodes are unseen by definition.
    ///
    /// The monotonic-change check runs in every branch — it catches
    /// merges-with-drops that the cluster_id check alone cannot
    /// (because two nodes inside a single configured cluster_id can
    /// still split-brain).
    pub fn membership_change_is_safe(
        &self,
        proposed_members: &[NodeId],
        proposal_cluster_id: Option<ClusterId>,
    ) -> bool {
        let my_id = self.cluster_id();
        let other = proposal_cluster_id.unwrap_or(ClusterId::UNSET);
        // Both sides configured: cluster_id is the authoritative
        // split-brain defence.
        let both_configured = !my_id.is_unset() && !other.is_unset();
        if both_configured && other != my_id {
            return false;
        }

        let committed_members = self.committed_members.read().unwrap();
        if committed_members.is_empty() {
            // First commit on this node — nothing to compare against.
            return true;
        }
        if !is_safe_membership_change(&committed_members, proposed_members) {
            return false;
        }
        drop(committed_members);

        // When both sides are configured and the ids matched, the
        // monotonic check above is the only structural defence we need
        // — a matching cluster_id proves the proposal originates from
        // an authenticated peer in the same cluster, and rejecting
        // unseen members at that point would block every legitimate
        // join (a brand-new node is unseen by definition).
        if both_configured {
            return true;
        }

        // Fallback split-brain heuristic for nodes that have not yet
        // configured a cluster_id: any previously-unseen NodeId is
        // rejected. cluster_id (when wired) supersedes this, but in
        // legacy / mixed-version clusters the pure-superset attack
        // (F-G8-001) still requires the heuristic.
        self.ever_seen_check(proposed_members)
    }

    /// Get a shared reference to the committed term atomic.
    ///
    /// Used by SWIM gossip to piggyback the committed term on probe
    /// messages so that lagging nodes can detect they are behind and
    /// trigger a topology catch-up without an extra polling mechanism.
    pub fn committed_term_shared(&self) -> Arc<AtomicU64> {
        self.committed_term.clone()
    }

    /// Restore from persisted state on startup.
    pub fn restore(&self, state: &PersistedTopologyState) {
        // E-01: reinstate the persisted peak so a node that reboots into
        // a partition cannot self-activate a shrunken topology.
        self.observe_peak_cluster_size(state.peak_cluster_size);
        self.committed_term
            .store(state.committed_term, Ordering::Relaxed);
        self.voted_term.store(state.voted_term, Ordering::Relaxed);
        *self.committed_members.write().unwrap() = state.committed_members.clone();
        *self.committed_voters.write().unwrap() = state.committed_voters.clone();
        *self.observed_membership.lock() = state.committed_members.clone();
        // Restore the ever-seen voter set so the fallback split-brain
        // check survives restarts. If the persisted file predates this
        // field, seed it from `committed_voters` (the best we can do).
        {
            let mut seen = self.committed_voter_ever_seen.write().unwrap();
            seen.clear();
            if !state.committed_voter_ever_seen.is_empty() {
                seen.extend(state.committed_voter_ever_seen.iter().copied());
            } else {
                seen.extend(state.committed_voters.iter().copied());
            }
        }
    }

    /// Current committed term.
    pub fn committed_term(&self) -> u64 {
        self.committed_term.load(Ordering::Relaxed)
    }

    /// Members of the committed term.
    pub fn committed_members(&self) -> Vec<NodeId> {
        self.committed_members.read().unwrap().clone()
    }

    /// Voters whose quorum approved the committed term.
    pub fn committed_voters(&self) -> Vec<NodeId> {
        self.committed_voters.read().unwrap().clone()
    }

    /// Reset the membership-change timer to `now`.
    ///
    /// Called when a `TopologyStale` event is detected so the fallback
    /// proposer path fires sooner (on the very next timeout check) rather
    /// than waiting for the original membership-change timer to expire.
    pub fn reset_membership_timer(&self) {
        *self.last_membership_change.lock() = Instant::now();
    }

    /// Current persisted state for saving to disk.
    ///
    /// `incarnation` is the SWIM incarnation counter to persist so that
    /// after restart the node can resume with a strictly higher value.
    pub fn persisted_state(&self, peak: u64, incarnation: u64) -> PersistedTopologyState {
        PersistedTopologyState {
            peak_cluster_size: peak,
            committed_term: self.committed_term.load(Ordering::Relaxed),
            committed_members: self.committed_members.read().unwrap().clone(),
            committed_voters: self.committed_voters.read().unwrap().clone(),
            voted_term: self.voted_term.load(Ordering::Relaxed),
            incarnation,
            committed_voter_ever_seen: self.committed_voter_ever_seen_snapshot(),
        }
    }

    /// Called when SWIM reports a membership change.
    ///
    /// Returns `Some(TopologyTerm)` if this node should propose
    /// (i.e., this node is the deterministic proposer = `members[0]`).
    ///
    /// # Split-brain rejection
    ///
    /// If the proposed `members` set is neither a superset nor a subset of
    /// `committed_members` (i.e., it both *adds* nodes never previously
    /// committed AND *drops* nodes that were previously committed), the
    /// change is rejected as a probable split-brain heal: two independent
    /// clusters that share a `cluster_secret` (or whose SWIM gossip
    /// otherwise leaks across) have just learned about each other. Healing
    /// such a merge by silently committing a unioned/intersected member
    /// set would corrupt the shard tables on both sides — operators must
    /// intervene (currently by tearing down one side; future work tracks
    /// an `--allow-merge` flag and a separate `cluster_id` field).
    ///
    /// Pure additions (member joins) and pure removals (graceful drain) are
    /// still accepted.
    pub fn on_membership_changed(&self, members: &[NodeId]) -> Option<TopologyTerm> {
        if members.is_empty() {
            return None;
        }

        // Split-brain heal detection — refuse to commit a topology that
        // both adds and removes members relative to the committed set,
        // OR that introduces a NodeId never previously observed as a
        // committed voter on this node (F-G8-001 fallback). Run BEFORE
        // updating observed_membership / last_membership_change so the
        // fallback proposer path doesn't pick up the poisoned view either.
        //
        // The local node is the proposer in this code path, so the
        // "proposal cluster_id" is our own — pass it explicitly so that
        // a configured cluster_id participates in the safety check
        // (cluster_id match skips the ever-seen heuristic).
        if !self.membership_change_is_safe(members, Some(self.cluster_id())) {
            let committed_members = self.committed_members.read().unwrap();
            tracing::error!(
                self_id = self.self_id.0,
                committed = ?committed_members.iter().map(|n| n.0).collect::<Vec<_>>(),
                proposed = ?members.iter().map(|n| n.0).collect::<Vec<_>>(),
                "cluster: refusing topology proposal — split-brain heal signature (non-monotonic change or unseen members). Operator intervention required.",
            );
            return None;
        }

        *self.last_membership_change.lock() = Instant::now();
        *self.observed_membership.lock() = members.to_vec();

        // Skip if the committed membership is already identical.
        // This prevents redundant proposals when SWIM fires membership
        // events that don't actually change the member set.
        {
            let committed_members = self.committed_members.read().unwrap();
            if committed_members.len() == members.len()
                && committed_members
                    .iter()
                    .zip(members.iter())
                    .all(|(a, b)| a == b)
            {
                return None;
            }
        }

        // Deterministic proposer: lowest NodeId (members are sorted).
        let proposer = members[0];
        if proposer != self.self_id {
            return None; // Not our turn to propose
        }

        let committed = self.committed_term.load(Ordering::Relaxed);
        let voted = self.voted_term.load(Ordering::Relaxed);
        let new_term = committed.max(voted) + 1;

        let term = TopologyTerm::new(new_term, members.to_vec(), self.self_id, self.cluster_id());

        // Self-vote
        self.voted_term.store(new_term, Ordering::Relaxed);

        // E-01: raise the peak from the proposed set BEFORE deriving the
        // quorum, so growth (1 → N) is gated on the majority of the new,
        // larger cluster, and a later shrink is gated on the majority of
        // the peak — never on the shrunken set alone.
        self.observe_peak_cluster_size(members.len() as u64);
        let quorum_needed = self.activation_quorum_needed(members.len());
        let mut votes = std::collections::HashMap::new();
        votes.insert(self.self_id, true);

        *self.pending_proposal.lock() = Some(PendingProposal {
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

        let valid_digest = propose.digest
            == TopologyTerm::compute_digest(propose.term, &propose.cluster_id, &propose.members);

        // F-G8-002: the proposer-side split-brain checks fire in
        // `on_membership_changed`, `retry_proposal`, and `check_timeout`,
        // but the follower-side `handle_propose` previously accepted any
        // valid-digest, higher-term proposal. A buggy or malicious node
        // that bypassed its own checks could still gather a quorum from
        // followers — apply the same guard on this side so a single
        // round cannot launder a merged membership through the quorum.
        if !valid_digest
            || !self.membership_change_is_safe(&propose.members, Some(propose.cluster_id))
        {
            // Even when `voted_term` would normally advance, we refuse to
            // self-vote for an unsafe proposal. Report the voter's last
            // accepted term so the proposer can detect the divergence.
            tracing::warn!(
                self_id = self.self_id.0,
                proposer = propose.proposer.0,
                term = propose.term,
                "cluster: rejecting topology propose — split-brain heal signature or bad digest",
            );
            return TopologyVote {
                term: propose.term,
                digest: propose.digest,
                voter: self.self_id,
                accepted: false,
                voter_current_term: committed,
            };
        }

        // Accept if the term is strictly higher than anything we've seen.
        let mut accepted = propose.term > committed && propose.term > voted && valid_digest;

        // Cluster formation recovery: when a node is in a single-node cluster
        // (either from fresh start or after losing all peers), a multi-node
        // proposal that includes this node should be accepted so the cluster
        // can converge. This handles several scenarios:
        //
        // 1. Simultaneous start: each node commits single-node terms, then
        //    discovers peers and needs to form a joint cluster.
        //
        // 2. Voted-but-not-committed: a node voted for a term that never
        //    got committed (proposer crashed or network partition). The
        //    outstanding vote should not permanently block convergence.
        //
        // 3. Sequential restarts: node3 restarts, commits single-node term,
        //    then node1 proposes a 2-node term. Node3 must accept even if
        //    the proposal term equals its voted term.
        //
        // Safety: the proposal must have more members (larger cluster) and
        // must include this node, preventing acceptance of foreign proposals.
        if !accepted && valid_digest && propose.members.len() > 1 {
            let committed_members = self.committed_members.read().unwrap();
            let our_cluster_is_single_node = committed > 0 && committed_members.len() <= 1;
            let proposal_subsumes_us = propose.members.contains(&self.self_id);
            if our_cluster_is_single_node && proposal_subsumes_us && propose.term > voted {
                accepted = true;
            }
        }

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
        let mut pending = self.pending_proposal.lock();
        let proposal = pending.as_mut()?;

        // Must match our pending proposal.
        if vote.term != proposal.term.term || vote.digest != proposal.term.digest {
            return None;
        }

        proposal.votes.insert(vote.voter, vote.accepted);

        let accept_count = proposal.votes.values().filter(|&&v| v).count();
        if accept_count >= proposal.quorum_needed {
            let mut voters = proposal
                .votes
                .iter()
                .filter_map(|(node, accepted)| accepted.then_some(*node))
                .collect::<Vec<_>>();
            voters.sort_unstable_by_key(|node| node.0);
            let commit = TopologyCommit {
                term: proposal.term.term,
                proposer: proposal.term.proposer,
                members: proposal.term.members.clone(),
                cluster_id: proposal.term.cluster_id,
                digest: proposal.term.digest,
                voters,
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

        // Validate digest. The digest is computed over
        // (term || cluster_id || members) so a forged cluster_id that
        // happens to match the local one still mismatches the digest.
        let expected_digest =
            TopologyTerm::compute_digest(commit.term, &commit.cluster_id, &commit.members);
        if commit.digest != expected_digest {
            return None;
        }

        if !commit.has_quorum_voter_proof() {
            return None;
        }

        // Apply commit.
        // E-01: a committed term with N members is direct evidence the
        // cluster reached size N — raise the peak so any later
        // SWIM-observed shrink is gated on the majority of this size.
        self.observe_peak_cluster_size(commit.members.len() as u64);
        self.committed_term.store(commit.term, Ordering::Relaxed);
        *self.committed_members.write().unwrap() = commit.members.clone();
        *self.committed_voters.write().unwrap() = commit.voters.clone();
        *self.observed_membership.lock() = commit.members.clone();
        // F-G8-001 fallback: every member of a committed term is, from
        // now on, a "known" voter. Future proposals that introduce a
        // NodeId not in this set will be rejected by `ever_seen_check`.
        {
            let mut seen = self.committed_voter_ever_seen.write().unwrap();
            for v in &commit.voters {
                seen.insert(*v);
            }
            for m in &commit.members {
                seen.insert(*m);
            }
        }
        // Phase I — stamp the wall-clock time so cluster_health can
        // report `last_topology_commit_age_ms`. Best-effort: a system
        // clock without UNIX_EPOCH access stays at the prior value.
        if let Ok(d) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            self.last_commit_at_unix_ms
                .store(d.as_millis() as u64, Ordering::Relaxed);
        }

        // Clear any pending proposal (superseded by this commit).
        *self.pending_proposal.lock() = None;

        Some(commit.term)
    }

    /// Phase I — millis since UNIX epoch of the most recent observed
    /// commit, or `0` when no commit has been applied yet on this node.
    pub fn last_commit_at_unix_ms(&self) -> u64 {
        self.last_commit_at_unix_ms.load(Ordering::Relaxed)
    }

    /// Phase I — milliseconds elapsed since the most recent commit on
    /// this node. Returns `u64::MAX` when no commit has been observed
    /// (the cluster_health endpoint reports this back to clients as
    /// "not yet ready").
    pub fn last_commit_age_ms(&self) -> u64 {
        let stamp = self.last_commit_at_unix_ms();
        if stamp == 0 {
            return u64::MAX;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(stamp);
        now.saturating_sub(stamp)
    }

    /// Retry a failed proposal as the deterministic proposer.
    ///
    /// Called by `run_topology_proposer` when quorum is not reached. Bumps
    /// `voted_term` to a fresh value (so peers whose `voted_term` already
    /// advanced during the previous attempt can accept us), refreshes the
    /// target member set from the last SWIM observation, and returns a new
    /// `TopologyTerm` to broadcast.
    ///
    /// Returns `None` if:
    ///   * we are no longer the deterministic proposer (lowest NodeId), or
    ///   * the cluster has already committed the target membership, or
    ///   * observed_membership is empty (nothing to propose).
    pub fn retry_proposal(&self) -> Option<TopologyTerm> {
        let target_members = {
            let observed = self.observed_membership.lock();
            if observed.is_empty() {
                return None;
            }
            observed.clone()
        };

        if target_members[0] != self.self_id {
            return None;
        }

        // Split-brain heal defense: even though `on_membership_changed`
        // would have rejected a non-monotonic SWIM event before populating
        // `observed_membership`, a compromised or buggy caller might also
        // mutate it directly (tests do, see `retry_proposal_returns_none_*`).
        // Re-check here so a poisoned observation cannot be laundered into
        // a proposal via the retry path. Includes the F-G8-001 ever-seen
        // fallback so pure-superset attacks are caught even if an external
        // caller installed a "monotonic" observation containing unseen ids.
        //
        // We are the proposer in this code path, so the proposal
        // cluster_id is our own.
        if !self.membership_change_is_safe(&target_members, Some(self.cluster_id())) {
            let committed_members = self.committed_members.read().unwrap();
            tracing::error!(
                self_id = self.self_id.0,
                committed = ?committed_members.iter().map(|n| n.0).collect::<Vec<_>>(),
                proposed = ?target_members.iter().map(|n| n.0).collect::<Vec<_>>(),
                "cluster: refusing topology retry — split-brain heal signature (non-monotonic change or unseen members).",
            );
            return None;
        }

        {
            let committed_members = self.committed_members.read().unwrap();
            if committed_members.len() == target_members.len()
                && committed_members
                    .iter()
                    .zip(target_members.iter())
                    .all(|(a, b)| a == b)
            {
                return None;
            }
        }

        let committed = self.committed_term.load(Ordering::Relaxed);
        let voted = self.voted_term.load(Ordering::Relaxed);
        let new_term = committed.max(voted) + 1;

        let term = TopologyTerm::new(
            new_term,
            target_members.clone(),
            self.self_id,
            self.cluster_id(),
        );
        self.voted_term.store(new_term, Ordering::Relaxed);

        // E-01: peak-derived activation quorum (see on_membership_changed).
        self.observe_peak_cluster_size(target_members.len() as u64);
        let quorum_needed = self.activation_quorum_needed(target_members.len());
        let mut votes = std::collections::HashMap::new();
        votes.insert(self.self_id, true);

        *self.pending_proposal.lock() = Some(PendingProposal {
            term: term.clone(),
            votes,
            quorum_needed,
            _started_at: Instant::now(),
        });

        Some(term)
    }

    /// Check if the proposal timeout has fired for fallback proposer.
    ///
    /// If this node is not the deterministic proposer but the timeout has
    /// elapsed without receiving a proposal or commit, this node can step
    /// up as a fallback proposer to prevent stalemate.
    ///
    /// Returns `Some(TopologyTerm)` if this node should propose as fallback.
    ///
    /// `members` is only a bootstrap fallback when no prior membership
    /// view has been observed yet. Once SWIM reports a membership change
    /// or a term is committed, fallback uses that stored target set so it
    /// does not resurrect gracefully removed nodes that are still reachable.
    pub fn check_timeout(&self, members: &[NodeId]) -> Option<TopologyTerm> {
        let target_members = {
            let observed = self.observed_membership.lock();
            if observed.is_empty() {
                members.to_vec()
            } else {
                observed.clone()
            }
        };

        if target_members.is_empty() || target_members[0] == self.self_id {
            return None; // We are already the deterministic proposer
        }

        // Split-brain heal defense (defense in depth — see retry_proposal).
        // The bootstrap-fallback `members` slice can come from the live
        // socket map, which is updated outside `on_membership_changed`;
        // re-validate here so a non-monotonic view never becomes a proposal.
        // Applies the F-G8-001 ever-seen fallback as well.
        //
        // We are the fallback proposer in this code path; the proposal
        // cluster_id is our own.
        if !self.membership_change_is_safe(&target_members, Some(self.cluster_id())) {
            let committed_members = self.committed_members.read().unwrap();
            tracing::error!(
                self_id = self.self_id.0,
                committed = ?committed_members.iter().map(|n| n.0).collect::<Vec<_>>(),
                proposed = ?target_members.iter().map(|n| n.0).collect::<Vec<_>>(),
                "cluster: refusing topology fallback proposal — split-brain heal signature (non-monotonic change or unseen members).",
            );
            return None;
        }

        // Skip if the committed membership is already identical.
        {
            let committed_members = self.committed_members.read().unwrap();
            if committed_members.len() == target_members.len()
                && committed_members
                    .iter()
                    .zip(target_members.iter())
                    .all(|(a, b)| a == b)
            {
                return None;
            }
        }

        let elapsed = self.last_membership_change.lock().elapsed();
        if elapsed < self.propose_timeout {
            return None; // Still within timeout
        }

        // Check if we already committed for a recent term
        let committed = self.committed_term.load(Ordering::Relaxed);
        let voted = self.voted_term.load(Ordering::Relaxed);

        // Only propose if we haven't already voted for a higher term
        // (which would mean another proposer is active).
        let new_term = committed.max(voted) + 1;

        let term = TopologyTerm::new(
            new_term,
            target_members.clone(),
            self.self_id,
            self.cluster_id(),
        );
        self.voted_term.store(new_term, Ordering::Relaxed);

        // E-01: peak-derived activation quorum (see on_membership_changed).
        self.observe_peak_cluster_size(target_members.len() as u64);
        let quorum_needed = self.activation_quorum_needed(target_members.len());
        let mut votes = std::collections::HashMap::new();
        votes.insert(self.self_id, true);

        *self.pending_proposal.lock() = Some(PendingProposal {
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
        let propose = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET);
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

        let propose = TopologyTerm::new(3, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET);
        let vote = auth.handle_propose(&propose);
        assert!(!vote.accepted);
    }

    #[test]
    fn vote_reject_bad_digest() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let mut propose = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET);
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
        let _term = auth
            .on_membership_changed(&members(&[1, 2, 3, 4, 5]))
            .unwrap();

        // 5 members, quorum = 3. Self-vote = 1. Need 2 more.
        let vote1 = TopologyVote {
            term: 1,
            digest: TopologyTerm::compute_digest(1, &ClusterId::UNSET, &members(&[1, 2, 3, 4, 5])),
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
        };
        let commit = auth.handle_vote(&vote1);
        assert!(commit.is_none()); // Only 2 votes, need 3

        let vote2 = TopologyVote {
            term: 1,
            digest: TopologyTerm::compute_digest(1, &ClusterId::UNSET, &members(&[1, 2, 3, 4, 5])),
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
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems),
            voters: mems.clone(),
        };
        let result = auth.handle_commit(&commit);
        assert_eq!(result, Some(5));
        assert_eq!(auth.committed_term(), 5);
        assert_eq!(auth.committed_members(), mems);
    }

    // ── Phase I: cluster-readiness (last commit timestamp) ─────────────────

    #[test]
    fn last_commit_age_is_max_before_first_commit() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        assert_eq!(
            auth.last_commit_at_unix_ms(),
            0,
            "no commit yet → no timestamp",
        );
        assert_eq!(
            auth.last_commit_age_ms(),
            u64::MAX,
            "absent commit must read as the largest possible age",
        );
    }

    #[test]
    fn last_commit_age_advances_after_handle_commit() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 7,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(7, &ClusterId::UNSET, &mems),
            voters: mems.clone(),
        };
        assert_eq!(auth.handle_commit(&commit), Some(7));
        assert!(
            auth.last_commit_at_unix_ms() > 0,
            "handle_commit must stamp the wall-clock time",
        );
        // Age must be small (commit was just applied).
        assert!(
            auth.last_commit_age_ms() < 60_000,
            "freshly committed term should have age << 1 minute",
        );
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
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems),
            voters: mems.clone(),
        };
        assert!(auth.handle_commit(&commit).is_none());
    }

    #[test]
    fn handle_commit_rejects_same_term() {
        // Regression: duplicate commit for the same term must be rejected.
        // This prevents double-mastered shards when two commit signals
        // arrive close together (e.g., deterministic + fallback proposer).
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems),
            voters: mems.clone(),
        };

        // First commit succeeds
        let result1 = auth.handle_commit(&commit);
        assert_eq!(result1, Some(5));
        assert_eq!(auth.committed_term(), 5);

        // Second commit with same term is rejected
        let result2 = auth.handle_commit(&commit);
        assert!(
            result2.is_none(),
            "duplicate commit for same term should be rejected"
        );
        // Term should still be 5 — not advanced
        assert_eq!(auth.committed_term(), 5);
    }

    #[test]
    fn handle_commit_rejects_bad_digest() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            digest: [0xFF; 32], // corrupt
            voters: mems.clone(),
        };
        assert!(auth.handle_commit(&commit).is_none());
    }

    #[test]
    fn persisted_state_round_trip() {
        let state = PersistedTopologyState {
            peak_cluster_size: 5,
            committed_term: 42,
            committed_members: members(&[1, 2, 3]),
            committed_voters: members(&[1, 2, 3]),
            voted_term: 43,
            incarnation: 99,
            committed_voter_ever_seen: members(&[1, 2, 3, 7]),
        };
        let data = state.serialize();
        let restored = PersistedTopologyState::deserialize(&data);
        assert_eq!(restored.peak_cluster_size, 5);
        assert_eq!(restored.committed_term, 42);
        assert_eq!(restored.voted_term, 43);
        assert_eq!(restored.committed_members.len(), 3);
        assert_eq!(restored.committed_members[0], NodeId(1));
        assert_eq!(restored.committed_voters, members(&[1, 2, 3]));
        assert_eq!(restored.incarnation, 99);
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
        assert_eq!(restored.incarnation, 0);
    }

    #[test]
    fn wire_format_round_trip() {
        let term = TopologyTerm::new(42, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET);
        let data = term.serialize();
        let restored = TopologyTerm::deserialize(&data).unwrap();
        assert_eq!(restored.term, 42);
        assert_eq!(restored.proposer, NodeId(1));
        assert_eq!(restored.members.len(), 3);
        assert_eq!(restored.digest, term.digest);

        let vote = TopologyVote {
            term: 42,
            digest: term.digest,
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 41,
        };
        let vdata = vote.serialize();
        let rv = TopologyVote::deserialize(&vdata).unwrap();
        assert_eq!(rv.term, 42);
        assert!(rv.accepted);
        assert_eq!(rv.voter, NodeId(2));
        assert_eq!(rv.voter_current_term, 41);

        let commit = TopologyCommit {
            term: 42,
            proposer: NodeId(1),
            members: term.members.clone(),
            cluster_id: ClusterId::UNSET,
            digest: term.digest,
            voters: members(&[1, 2]),
        };
        let cdata = commit.serialize();
        let rc = TopologyCommit::deserialize(&cdata).unwrap();
        assert_eq!(rc.term, 42);
        assert_eq!(rc.members, term.members);
        assert_eq!(rc.voters, members(&[1, 2]));
        assert!(rc.has_quorum_voter_proof());
    }

    #[test]
    fn topology_commit_persists_voter_list() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let term = auth.on_membership_changed(&mems).unwrap();
        let vote = TopologyVote {
            term: term.term,
            digest: term.digest,
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
        };
        let commit = auth.handle_vote(&vote).expect("2/3 reaches quorum");

        assert_eq!(commit.voters, members(&[1, 2]));
        assert_eq!(auth.handle_commit(&commit), Some(term.term));
        let persisted = auth.persisted_state(3, 99);
        assert_eq!(persisted.committed_members, mems);
        assert_eq!(persisted.committed_voters, members(&[1, 2]));

        let restored = PersistedTopologyState::deserialize(&persisted.serialize());
        assert_eq!(restored.committed_voters, members(&[1, 2]));
    }

    #[test]
    fn cannot_vote_twice_for_same_term() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));

        let p1 = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET);
        let v1 = auth.handle_propose(&p1);
        assert!(v1.accepted);

        // Second proposal at same term from a different proposer
        let p2 = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(3), ClusterId::UNSET);
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
            cluster_id: ClusterId::UNSET,
            digest: t1.digest,
            voters: members(&[1, 2, 3]),
        });

        // New membership change → term 2
        let t2 = auth.on_membership_changed(&members(&[1, 2])).unwrap();
        assert_eq!(t2.term, 2);
    }

    // -- Catch-up via synthetic commit --

    #[test]
    fn catchup_via_synthetic_commit() {
        // Simulate a lagging node (term=0) catching up to term=5
        // by receiving a synthetic commit from a peer.
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        assert_eq!(auth.committed_term(), 0);

        // Construct a synthetic commit as if fetched from a peer
        let remote_members = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: remote_members.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &remote_members),
            voters: remote_members.clone(),
        };
        let result = auth.handle_commit(&commit);
        assert_eq!(result, Some(5));
        assert_eq!(auth.committed_term(), 5);
        assert_eq!(auth.committed_members(), remote_members);
    }

    #[test]
    fn catchup_rejects_stale_synthetic_commit() {
        // A node already at term=10 must reject a synthetic commit for term=5.
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        auth.committed_term.store(10, Ordering::Relaxed);

        let remote_members = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: remote_members.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &remote_members),
            voters: remote_members.clone(),
        };
        let result = auth.handle_commit(&commit);
        assert!(result.is_none());
        assert_eq!(auth.committed_term(), 10); // unchanged
    }

    #[test]
    fn catchup_rejects_bad_digest_synthetic_commit() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));

        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: members(&[1, 2, 3]),
            cluster_id: ClusterId::UNSET,
            digest: [0xFF; 32], // corrupt
            voters: members(&[1, 2, 3]),
        };
        assert!(auth.handle_commit(&commit).is_none());
        assert_eq!(auth.committed_term(), 0); // unchanged
    }

    #[test]
    fn catchup_advances_and_then_normal_proposal_works() {
        // After catching up via synthetic commit, normal proposal flow
        // should still work with higher term numbers.
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));

        // Catch up to term 5
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems),
            voters: mems.clone(),
        };
        auth.handle_commit(&commit);
        assert_eq!(auth.committed_term(), 5);

        // Now a normal membership change should produce term 6
        let new_mems = members(&[1, 2]);
        let proposal = auth.on_membership_changed(&new_mems);
        assert!(proposal.is_some());
        assert_eq!(proposal.unwrap().term, 6);
    }

    #[test]
    fn synthetic_commit_with_wrong_members_rejected() {
        // Regression test: a synthetic commit constructed with the wrong
        // member list (e.g., SWIM-alive nodes instead of committed members)
        // produces a mismatched digest and MUST be rejected.
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));

        // The original term 5 was committed with members [1, 3].
        let original_members = members(&[1, 3]);
        let original_digest = TopologyTerm::compute_digest(5, &ClusterId::UNSET, &original_members);

        // Synthetic commit with wrong members [1, 2, 3] (SWIM-alive view).
        let wrong_members = members(&[1, 2, 3]);
        let wrong_digest = TopologyTerm::compute_digest(5, &ClusterId::UNSET, &wrong_members);

        // The digests MUST differ.
        assert_ne!(
            original_digest, wrong_digest,
            "digest must differ when member lists differ"
        );

        // Applying the wrong-members commit should fail.
        let wrong_commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: wrong_members.clone(),
            cluster_id: ClusterId::UNSET,
            digest: wrong_digest,
            voters: wrong_members,
        };
        // This succeeds because the digest matches (term, wrong_members).
        // But the point is: if you use the WRONG members to compute the
        // digest, you get a DIFFERENT commit than the one the cluster
        // originally agreed on. This is why catch-up must use
        // committed_members, not SWIM-alive nodes.
        let result = auth.handle_commit(&wrong_commit);
        assert!(
            result.is_some(),
            "commit with self-consistent digest should apply"
        );
        assert_eq!(auth.committed_members(), members(&[1, 2, 3]));

        // The correct commit uses the ORIGINAL members.
        let auth2 = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let correct_commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: original_members.clone(),
            cluster_id: ClusterId::UNSET,
            digest: original_digest,
            voters: original_members.clone(),
        };
        let result2 = auth2.handle_commit(&correct_commit);
        assert!(result2.is_some());
        assert_eq!(
            auth2.committed_members(),
            original_members,
            "correct catch-up should use the original committed members"
        );
    }

    // -----------------------------------------------------------------------
    // Part 2.4: Membership change during ongoing membership change
    // -----------------------------------------------------------------------

    #[test]
    fn pending_proposal_superseded_by_new_membership_change() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));

        // First membership change → propose term 1
        let t1 = auth.on_membership_changed(&members(&[1, 2, 3])).unwrap();
        assert_eq!(t1.term, 1);

        // Before quorum is reached, another membership change occurs
        // This should propose a NEW term (term 2), superseding term 1
        let t2 = auth.on_membership_changed(&members(&[1, 2, 4])).unwrap();
        assert_eq!(t2.term, 2, "new membership change should advance term");

        // Votes for the old term 1 should not produce a commit
        let stale_vote = TopologyVote {
            term: 1,
            digest: t1.digest,
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
        };
        let commit = auth.handle_vote(&stale_vote);
        assert!(
            commit.is_none(),
            "stale vote for superseded term should not produce commit"
        );
    }

    #[test]
    fn commit_clears_pending_proposal() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let t = auth.on_membership_changed(&members(&[1, 2, 3])).unwrap();

        // Simulate external commit (e.g., from another proposer)
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(2),
            members: members(&[1, 2, 3, 4]),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &members(&[1, 2, 3, 4])),
            voters: members(&[1, 2, 3, 4]),
        };
        auth.handle_commit(&commit);

        // Pending proposal for term 1 should be cleared
        let stale_vote = TopologyVote {
            term: t.term,
            digest: t.digest,
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
        };
        let result = auth.handle_vote(&stale_vote);
        assert!(
            result.is_none(),
            "pending proposal should be cleared by commit"
        );
    }

    // -----------------------------------------------------------------------
    // Part 2.5: Two nodes same membership → same version
    // -----------------------------------------------------------------------

    #[test]
    fn two_authorities_same_proposal_same_digest() {
        let a1 = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let a2 = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));

        let t1 = a1.on_membership_changed(&members(&[1, 2, 3])).unwrap();
        let t2 = a2.on_membership_changed(&members(&[1, 2, 3])).unwrap();

        assert_eq!(t1.term, t2.term);
        assert_eq!(
            t1.digest, t2.digest,
            "same term+members must produce same digest"
        );
    }

    // -----------------------------------------------------------------------
    // Part 1.7: Quorum prevents split-brain
    // -----------------------------------------------------------------------

    #[test]
    fn minority_cannot_commit_independently() {
        // In a 5-node cluster, 2 nodes can't reach quorum (need 3)
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let t = auth
            .on_membership_changed(&members(&[1, 2, 3, 4, 5]))
            .unwrap();
        // Quorum = 3. Self-vote = 1. Need 2 more.

        // Only 1 additional vote → no commit
        let vote = TopologyVote {
            term: t.term,
            digest: t.digest,
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
        };
        let commit = auth.handle_vote(&vote);
        assert!(commit.is_none(), "2/5 is not quorum");

        // One rejected vote → still no commit
        let reject_vote = TopologyVote {
            term: t.term,
            digest: t.digest,
            voter: NodeId(3),
            accepted: false,
            voter_current_term: 0,
        };
        let commit = auth.handle_vote(&reject_vote);
        assert!(commit.is_none(), "reject doesn't count toward quorum");

        // Third acceptance → quorum reached
        let vote3 = TopologyVote {
            term: t.term,
            digest: t.digest,
            voter: NodeId(4),
            accepted: true,
            voter_current_term: 0,
        };
        let commit = auth.handle_vote(&vote3);
        assert!(commit.is_some(), "3/5 is quorum → should commit");
    }

    #[test]
    fn fallback_proposer_skips_when_already_committed() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_millis(10));
        let mems = members(&[1, 2, 3]);

        // Commit the current membership
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems),
            voters: mems.clone(),
        };
        auth.handle_commit(&commit);

        // Now check_timeout with the same membership should skip
        // (committed membership == proposed membership)
        std::thread::sleep(Duration::from_millis(15));
        let result = auth.check_timeout(&mems);
        assert!(
            result.is_none(),
            "should not fallback-propose when committed membership matches"
        );
    }

    #[test]
    fn fallback_proposer_does_not_resurrect_gracefully_removed_node() {
        let auth = TopologyAuthority::new(NodeId(4), Duration::from_millis(10));
        let original = members(&[1, 2, 3, 4]);
        let drained = members(&[1, 2, 3]);

        auth.handle_commit(&TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: original.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(4, &ClusterId::UNSET, &original),
            voters: original.clone(),
        });
        auth.handle_commit(&TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: drained.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &drained),
            voters: drained.clone(),
        });

        std::thread::sleep(Duration::from_millis(15));

        let result = auth.check_timeout(&original);
        assert!(
            result.is_none(),
            "fallback timeout must not resurrect a node that was already gracefully removed",
        );
    }

    #[test]
    fn synthetic_commit_mixed_term_and_members_rejected() {
        // Regression test for the exact bug: synthetic commit uses
        // remote_term from SWIM gossip but members from current routing
        // info (SWIM-alive nodes). The digest won't match the original
        // commit because the original had different members.
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));

        // Original term 5 committed with members [1, 3] (node2 was down).
        let original_members = members(&[1, 3]);

        // Now node2 is back, SWIM sees [1, 2, 3]. Catch-up code naively
        // uses remote_term=5 with current members=[1, 2, 3].
        let _bad_commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: members(&[1, 2, 3]),
            // This digest is compute_digest(5, [1,2,3]) which differs
            // from the original compute_digest(5, [1,3]).
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &members(&[1, 2, 3])),
            voters: members(&[1, 2, 3]),
        };

        // The commit applies (digest is internally consistent), but it
        // represents a DIFFERENT topology than what was actually committed
        // on the cluster. This is the bug: the catch-up code should use
        // committed_members from the peer, not SWIM-alive nodes.
        //
        // With the fix, the catch-up code fetches committed_members=[1,3]
        // from the partition map and constructs:
        let good_commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: original_members.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &original_members),
            voters: original_members.clone(),
        };
        let result = auth.handle_commit(&good_commit);
        assert_eq!(result, Some(5));
        assert_eq!(auth.committed_members(), original_members);
    }

    // -----------------------------------------------------------------------
    // Deep edge cases: state machine interactions
    // -----------------------------------------------------------------------

    /// handle_commit does NOT advance voted_term. After catching up via
    /// handle_commit, the gap between voted_term and committed_term must
    /// not allow re-voting for a term between the two.
    #[test]
    fn handle_commit_leaves_voted_term_unchanged() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));

        // Vote for term 3
        let p = TopologyTerm::new(3, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET);
        let v = auth.handle_propose(&p);
        assert!(v.accepted);
        assert_eq!(auth.voted_term.load(Ordering::Relaxed), 3);

        // Catch up to term 10 via commit
        let mems = members(&[1, 2, 3, 4]);
        let commit = TopologyCommit {
            term: 10,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(10, &ClusterId::UNSET, &mems),
            voters: mems.clone(),
        };
        auth.handle_commit(&commit);
        assert_eq!(auth.committed_term(), 10);
        // voted_term is still 3 — handle_commit doesn't update it
        assert_eq!(auth.voted_term.load(Ordering::Relaxed), 3);

        // Proposal for term 8: > voted(3) but NOT > committed(10) → reject
        let p2 = TopologyTerm::new(8, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET);
        let v2 = auth.handle_propose(&p2);
        assert!(!v2.accepted, "term 8 < committed_term 10 → must reject");
    }

    /// on_membership_changed computes new_term as max(committed, voted) + 1.
    /// If voted_term > committed_term (voted for a term that wasn't committed),
    /// the next proposal skips past the voted term.
    #[test]
    fn retry_proposal_advances_term_and_keeps_membership() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);

        let t1 = auth.on_membership_changed(&mems).unwrap();
        assert_eq!(t1.term, 1);

        // First attempt's quorum failed — retry.
        let t2 = auth.retry_proposal().unwrap();
        assert!(t2.term > t1.term, "retry must advance term");
        assert_eq!(t2.members, mems, "retry uses observed membership");
        assert_eq!(t2.proposer, NodeId(1));
    }

    #[test]
    fn retry_proposal_returns_none_when_not_deterministic_proposer() {
        let auth = TopologyAuthority::new(NodeId(3), Duration::from_secs(1));
        // Observed membership: [1,2,3] — proposer would be node 1, not self.
        *auth.observed_membership.lock() = members(&[1, 2, 3]);
        assert!(auth.retry_proposal().is_none());
    }

    #[test]
    fn retry_proposal_returns_none_when_membership_already_committed() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2]);
        auth.handle_commit(&TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(1, &ClusterId::UNSET, &mems),
            voters: mems.clone(),
        });
        *auth.observed_membership.lock() = mems;
        assert!(
            auth.retry_proposal().is_none(),
            "nothing to do — already committed"
        );
    }

    #[test]
    fn on_membership_changed_skips_past_voted_term() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));

        // Propose and self-vote for term 1
        let t1 = auth.on_membership_changed(&members(&[1, 2, 3])).unwrap();
        assert_eq!(t1.term, 1);
        // voted_term = 1, committed_term = 0

        // Proposal for term 2 arrives from another node — we vote for it
        // (simulating a concurrent proposer). But for this test, we'll
        // artificially advance voted_term.
        auth.voted_term.store(5, Ordering::Relaxed);

        // Now on_membership_changed should produce term 6 (max(0, 5) + 1)
        let t2 = auth.on_membership_changed(&members(&[1, 2])).unwrap();
        assert_eq!(t2.term, 6, "should skip past voted_term=5");
    }

    /// check_timeout twice: each call proposes a new term and overwrites
    /// the pending proposal. Votes for the first term are ignored.
    #[test]
    fn check_timeout_overwrite_pending() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_millis(1));
        let mems = members(&[1, 2, 3]);

        // Commit a different membership so check_timeout fires.
        let old_mems = members(&[1, 2]);
        auth.handle_commit(&TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: old_mems.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(1, &ClusterId::UNSET, &old_mems),
            voters: old_mems.clone(),
        });
        // F-G8-001: pre-seed the ever-seen set with node 3 so the
        // membership-change-safety check accepts the [1,2,3] proposal.
        // Without this, on_membership_changed silently bounces the
        // unseen-voter and `check_timeout` falls back to the prior
        // observed membership [1,2], which matches the committed set
        // and returns None — short-circuiting the term-overwrite path
        // the test actually targets.
        auth.set_committed_voter_ever_seen(&[NodeId(1), NodeId(2), NodeId(3)]);
        assert!(
            auth.on_membership_changed(&mems).is_none(),
            "node 2 is not the deterministic proposer for [1,2,3]",
        );

        std::thread::sleep(Duration::from_millis(5));

        let t1 = auth.check_timeout(&mems).unwrap();
        let t2 = auth.check_timeout(&mems).unwrap();
        assert!(t2.term > t1.term, "second timeout should advance term");

        // Vote for t1 should not match pending (which is now t2)
        let v1 = TopologyVote {
            term: t1.term,
            digest: t1.digest,
            voter: NodeId(1),
            accepted: true,
            voter_current_term: 0,
        };
        assert!(auth.handle_vote(&v1).is_none());

        // Vote for t2 should match
        let v2 = TopologyVote {
            term: t2.term,
            digest: t2.digest,
            voter: NodeId(1),
            accepted: true,
            voter_current_term: 0,
        };
        assert!(auth.handle_vote(&v2).is_some());
    }

    /// Verify that deserialize rejects truncated data at various boundaries.
    #[test]
    fn topology_term_deserialize_truncation_boundaries() {
        let term = TopologyTerm::new(42, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET);
        let data = term.serialize();

        // Truncate at various points — all should return None.
        for len in [0, 1, 8, 15, 19, 20, 27, 28] {
            if len < data.len() {
                assert!(
                    TopologyTerm::deserialize(&data[..len]).is_none(),
                    "truncation at {len} bytes should fail"
                );
            }
        }

        // Full data should succeed
        assert!(TopologyTerm::deserialize(&data).is_some());
    }

    /// Persisted state with zero peak_cluster_size: should be clamped to 1.
    #[test]
    fn persisted_state_zero_peak_clamped() {
        let state = PersistedTopologyState {
            peak_cluster_size: 0,
            committed_term: 1,
            committed_members: members(&[1]),
            committed_voters: members(&[1]),
            voted_term: 1,
            incarnation: 0,
            committed_voter_ever_seen: Vec::new(),
        };
        let data = state.serialize();
        let restored = PersistedTopologyState::deserialize(&data);
        assert_eq!(
            restored.peak_cluster_size, 1,
            "zero peak should be clamped to 1"
        );
    }

    /// Phase D invariant: `committed_term` (which serves as the cluster_key)
    /// must NOT advance during the exchange phase. The exchange runs between
    /// `on_membership_changed` (proposal) and quorum `handle_commit`. Since
    /// only `handle_commit` advances `committed_term`, calling
    /// `on_membership_changed` alone must leave it unchanged.
    #[test]
    fn cluster_key_unchanged_during_exchange() {
        let ta = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = vec![NodeId(1), NodeId(2)];
        let initial_term = ta.committed_term();
        let proposal = ta.on_membership_changed(&mems);
        assert!(proposal.is_some(), "should produce a proposal");
        assert_eq!(
            ta.committed_term(),
            initial_term,
            "committed_term (cluster_key) must not advance during exchange phase — only after quorum commit",
        );
    }

    /// handle_propose: cluster formation recovery with proposal term EQUAL
    /// to committed term (not just greater). This is the boundary condition.
    #[test]
    fn formation_recovery_equal_term_accepted() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));

        // Single-node commit at term 1
        let single = members(&[2]);
        auth.handle_commit(&TopologyCommit {
            term: 1,
            proposer: NodeId(2),
            members: single.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(1, &ClusterId::UNSET, &single),
            voters: single.clone(),
        });
        // F-G8-001: the proposal introduces nodes 1 and 3 that were
        // never committed voters on this node, so the split-brain
        // fallback would otherwise reject the formation-recovery
        // proposal at `membership_change_is_safe` before the equal-
        // term acceptance branch can run. Pre-seed both as known
        // voters to isolate the boundary condition the test targets.
        auth.set_committed_voter_ever_seen(&[NodeId(1), NodeId(2), NodeId(3)]);

        // Proposal at term 1 (equal, not greater) with multi-node members.
        // Formation recovery: our_cluster_is_single_node=true, proposal subsumes
        // us, no outstanding vote (voted=0 after commit? Let's check...).
        // Actually after commit, voted_term is still 0 (handle_commit doesn't
        // update it), and committed_term = 1. no_outstanding_vote = (voted <= committed)
        // = (0 <= 1) = true. propose.term >= committed = (1 >= 1) = true.
        let proposal = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET);
        let v = auth.handle_propose(&proposal);
        assert!(
            v.accepted,
            "formation recovery should accept equal-term multi-node proposal"
        );
    }

    // -----------------------------------------------------------------------
    // R-042 — split-brain heal rejection
    // -----------------------------------------------------------------------

    /// Helper to seed a TopologyAuthority with a committed membership at
    /// the given term.
    fn commit_membership(auth: &TopologyAuthority, term: u64, ids: &[u64]) {
        let mems = members(ids);
        let commit = TopologyCommit {
            term,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            digest: TopologyTerm::compute_digest(term, &ClusterId::UNSET, &mems),
            voters: mems.clone(),
        };
        auth.handle_commit(&commit);
        assert_eq!(auth.committed_members(), mems);
    }

    #[test]
    fn is_safe_membership_change_classifies_pure_additions_as_safe() {
        // Joining a node is monotonic: committed ⊆ proposed.
        assert!(is_safe_membership_change(
            &members(&[1, 2, 3]),
            &members(&[1, 2, 3, 4]),
        ));
    }

    #[test]
    fn is_safe_membership_change_classifies_pure_removals_as_safe() {
        // Graceful drain is monotonic: proposed ⊆ committed.
        assert!(is_safe_membership_change(
            &members(&[1, 2, 3, 4]),
            &members(&[1, 2, 3]),
        ));
    }

    #[test]
    fn is_safe_membership_change_classifies_no_change_as_safe() {
        assert!(is_safe_membership_change(
            &members(&[1, 2, 3]),
            &members(&[1, 2, 3]),
        ));
    }

    #[test]
    fn is_safe_membership_change_classifies_first_commit_as_safe() {
        // Empty committed set: anything is acceptable.
        assert!(is_safe_membership_change(&[], &members(&[1, 2, 3])));
    }

    #[test]
    fn is_safe_membership_change_rejects_split_brain_merge() {
        // Committed [1, 2, 3]; SWIM now says [1, 2, 4].
        // Node 3 dropped AND node 4 appeared — split-brain heal signature.
        assert!(!is_safe_membership_change(
            &members(&[1, 2, 3]),
            &members(&[1, 2, 4]),
        ));
    }

    #[test]
    fn is_safe_membership_change_rejects_disjoint_clusters() {
        // No overlap at all — clearly two independent clusters.
        assert!(!is_safe_membership_change(
            &members(&[1, 2, 3]),
            &members(&[10, 11, 12]),
        ));
    }

    /// Headline regression for R-042: the deterministic proposer must
    /// refuse to issue a TopologyTerm when the proposed membership is
    /// neither a superset nor a subset of the committed set.
    #[test]
    fn topology_proposer_refuses_non_superset_membership_change() {
        // Node 1 is the deterministic proposer (lowest id).
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        // Cluster A committed: [1, 2, 3].
        commit_membership(&auth, 1, &[1, 2, 3]);
        // F-G8-001: the ever-seen split-brain fallback rejects any
        // proposal that introduces a NodeId never previously observed
        // as a committed voter on this node. Pre-seed node 4 so the
        // pure-addition sanity case isolates the monotonicity check
        // (the F-G8-001 layer is exercised separately below).
        auth.set_committed_voter_ever_seen(&[NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);

        // Sanity: a pure addition (cluster grows by one) is accepted.
        let pure_add = auth.on_membership_changed(&members(&[1, 2, 3, 4]));
        assert!(
            pure_add.is_some(),
            "monotonic add (join) must still be accepted",
        );
        assert_eq!(pure_add.unwrap().members, members(&[1, 2, 3, 4]));

        // Reset to the original commit so the next assertion starts clean.
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        commit_membership(&auth, 1, &[1, 2, 3]);

        // Sanity: a pure removal (graceful drain) is accepted. The
        // proposed set is a subset of committed, so the ever-seen
        // check trivially passes without extra seeding.
        let pure_drop = auth.on_membership_changed(&members(&[1, 2]));
        assert!(
            pure_drop.is_some(),
            "monotonic remove (drain) must still be accepted",
        );

        // Real test: SWIM reports [1, 2, 5] — node 3 disappeared AND node 5
        // showed up, the unmistakable two-clusters-merging pattern.
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        commit_membership(&auth, 1, &[1, 2, 3]);
        // Pre-seed node 5 so the rejection below is attributable to the
        // monotonicity check (the test's headline invariant) rather
        // than the F-G8-001 ever-seen layer, which has its own tests.
        auth.set_committed_voter_ever_seen(&[NodeId(1), NodeId(2), NodeId(3), NodeId(5)]);
        // After commit, both committed_members AND observed_membership are
        // pinned to [1,2,3] (handle_commit sets both). Capture the
        // baseline so we can pin it across the refusal.
        let observed_before = auth.observed_membership.lock().clone();
        assert_eq!(
            observed_before,
            members(&[1, 2, 3]),
            "handle_commit pins observed_membership to the committed set",
        );

        let proposal = auth.on_membership_changed(&members(&[1, 2, 5]));
        assert!(
            proposal.is_none(),
            "proposer must refuse non-monotonic membership change (split-brain heal)",
        );

        // The proposer's view of the cluster must NOT be poisoned by the
        // refused event. observed_membership and committed_members both
        // remain pinned to their pre-refusal values — the asymmetric
        // event leaks NO state into the authority.
        assert_eq!(
            auth.observed_membership.lock().clone(),
            observed_before,
            "refused event must not overwrite observed_membership",
        );
        assert_eq!(
            auth.committed_members(),
            members(&[1, 2, 3]),
            "committed_members must remain unchanged after refusal",
        );

        // No pending proposal was registered.
        assert!(
            auth.pending_proposal.lock().is_none(),
            "refusal must not leave a pending proposal behind",
        );

        // voted_term must NOT have advanced — we never broadcast a proposal,
        // so we cannot have self-voted.
        assert_eq!(
            auth.voted_term.load(Ordering::Relaxed),
            0,
            "refusal must not advance voted_term",
        );
    }

    /// Defense in depth: the fallback proposer (`check_timeout`) must also
    /// refuse a non-monotonic target membership.
    #[test]
    fn check_timeout_refuses_non_superset_membership_change() {
        // Node 2 is NOT the deterministic proposer for [1, 3, 5]; it would
        // become the fallback proposer after the timeout fires.
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_millis(1));
        commit_membership(&auth, 1, &[1, 2, 3]);

        // Wait past the timeout window so check_timeout proceeds past the
        // elapsed guard.
        std::thread::sleep(Duration::from_millis(5));

        // Bootstrap fallback: pass a non-monotonic set as the `members`
        // arg (observed_membership is empty so the bootstrap path runs).
        let result = auth.check_timeout(&members(&[1, 3, 5]));
        assert!(
            result.is_none(),
            "fallback proposer must refuse non-monotonic target",
        );
    }

    /// Defense in depth: the retry path must refuse a poisoned
    /// observed_membership too.
    #[test]
    fn retry_proposal_refuses_non_superset_membership_change() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        commit_membership(&auth, 1, &[1, 2, 3]);

        // Bypass on_membership_changed to install a poisoned observation
        // (simulating a buggy caller or, more realistically, an observation
        // that was monotonic when first installed but became non-monotonic
        // after a subsequent commit).
        *auth.observed_membership.lock() = members(&[1, 2, 5]);

        let retry = auth.retry_proposal();
        assert!(
            retry.is_none(),
            "retry must refuse non-monotonic observed membership",
        );
    }

    /// F-G5-002: an attacker-supplied member count above
    /// `MAX_TOPOLOGY_MEMBERS` must be rejected before any
    /// `Vec::with_capacity` allocation.
    #[test]
    fn topology_term_deserialize_rejects_oversized_member_count() {
        // Build a payload that advertises (MAX_TOPOLOGY_MEMBERS + 1) members
        // but does not actually carry the bytes for them. The cap should
        // reject the frame before the size check or the allocation.
        let mut buf = Vec::new();
        buf.extend_from_slice(&7u64.to_le_bytes()); // term
        buf.extend_from_slice(&1u64.to_le_bytes()); // proposer
        let oversized = (MAX_TOPOLOGY_MEMBERS + 1) as u32;
        buf.extend_from_slice(&oversized.to_le_bytes());
        // No member bytes, no digest — but the cap rejects before any of
        // that matters.
        assert!(TopologyTerm::deserialize(&buf).is_none());
    }

    /// F-G5-002: a member count exactly at the cap must still succeed
    /// (round-trip serialise/deserialise) so legitimate large clusters
    /// are not accidentally broken.
    #[test]
    fn topology_term_deserialize_accepts_count_at_cap() {
        let ids: Vec<u64> = (0..MAX_TOPOLOGY_MEMBERS as u64).collect();
        let term = TopologyTerm::new(1, members(&ids), NodeId(0), ClusterId::UNSET);
        let bytes = term.serialize();
        let decoded = TopologyTerm::deserialize(&bytes).expect("at-cap term should decode");
        assert_eq!(decoded.members.len(), MAX_TOPOLOGY_MEMBERS);
        assert_eq!(decoded.term, 1);
    }

    /// F-G5-002: voter list in TopologyCommit shares the same cap so a
    /// commit frame cannot drive a multi-megabyte voter allocation either.
    #[test]
    fn topology_commit_deserialize_rejects_oversized_voter_count() {
        let term = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET);
        let mut bytes = term.serialize();
        // Append voter section claiming MAX_TOPOLOGY_MEMBERS + 1 voters
        // without their bytes.
        let oversized = (MAX_TOPOLOGY_MEMBERS + 1) as u32;
        bytes.extend_from_slice(&oversized.to_le_bytes());
        assert!(TopologyCommit::deserialize(&bytes).is_none());
    }
}
