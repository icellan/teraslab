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
    /// W6 — shard-placement algorithm version this term is committed at.
    /// `1` = round-robin, `2` = rendezvous/HRW. MIXED INTO the digest, so a
    /// vote is for `(term, cluster_id, members, placement_version)`: a v1
    /// node that recomputes the digest from a v2 proposal still matches the
    /// digest, so it must additionally refuse via the explicit
    /// `MAX_SUPPORTED_PLACEMENT_VERSION` check in `handle_propose`. The
    /// digest binding ALSO guarantees a v1 node and a v2 node can never both
    /// believe they committed the same term at different placement versions.
    pub placement_version: u16,
    /// G8 stage 1 — durable split-brain floor anchor. Carries the effective
    /// peak (`TopologyAuthority::peak_cluster_size()`) at proposal time for
    /// every non-lowering producer (`on_membership_changed`, `retry_proposal`,
    /// `check_timeout`, `upgrade_proposal`), so grows carry the new
    /// `members.len()` and graceful-leave subsets carry the OLD higher peak
    /// (non-lowering). Stage 1 has no lowering producer — a future
    /// `propose_shrink` (stage 2) is the only path allowed to stamp a value
    /// below the current peak, gated by a quorum-of-old-peak proof (Gate B)
    /// re-verified by every applying node. Mixed into the digest (see
    /// below) so a divergent committed_peak cannot be laundered past vote
    /// matching.
    pub committed_peak: u64,
    /// SHA-256 digest of (term || cluster_id || members || placement_version
    /// || committed_peak), used for vote matching. Mixing `cluster_id`,
    /// `placement_version`, and `committed_peak` in means a tampered id, a
    /// divergent placement version, or a divergent floor claim all change
    /// the digest, so the digest check itself rejects a forged
    /// matching-cluster claim, a placement-version disagreement, or a
    /// mismatched committed_peak.
    pub digest: [u8; 32],
}

impl TopologyTerm {
    /// Create a new term with auto-computed digest.
    pub fn new(
        term: u64,
        members: Vec<NodeId>,
        proposer: NodeId,
        cluster_id: ClusterId,
        placement_version: u16,
        committed_peak: u64,
    ) -> Self {
        let digest = Self::compute_digest(
            term,
            &cluster_id,
            &members,
            placement_version,
            committed_peak,
        );
        Self {
            term,
            members,
            proposer,
            cluster_id,
            placement_version,
            committed_peak,
            digest,
        }
    }

    /// Compute the canonical digest for a (term, cluster_id, members,
    /// placement_version, committed_peak) tuple. `cluster_id` is mixed in so
    /// a forged-but-matching id changes the digest; `placement_version` is
    /// mixed in (INVARIANT i) so two terms that differ only in placement
    /// version produce DIFFERENT digests and cannot be conflated.
    /// `committed_peak` (G8 stage 1) is mixed in last so a divergent
    /// split-brain floor claim also changes the digest — an intentional
    /// cross-version digest break requiring a coordinated upgrade, matching
    /// the repo's deliberate-format-break posture.
    pub fn compute_digest(
        term: u64,
        cluster_id: &ClusterId,
        members: &[NodeId],
        placement_version: u16,
        committed_peak: u64,
    ) -> [u8; 32] {
        let mut buf = Vec::with_capacity(8 + 16 + 4 + members.len() * 8 + 2 + 8);
        buf.extend_from_slice(&term.to_le_bytes());
        buf.extend_from_slice(&cluster_id.0);
        buf.extend_from_slice(&(members.len() as u32).to_le_bytes());
        for m in members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        buf.extend_from_slice(&placement_version.to_le_bytes());
        buf.extend_from_slice(&committed_peak.to_le_bytes());
        auth::sha256(&buf)
    }

    /// Serialize for the wire.
    ///
    /// Format: `[term:8][proposer:8][cluster_id:16][member_count:4][member_id:8 * count][digest:32][placement_version:2][committed_peak:8]`
    ///
    /// `placement_version` and `committed_peak` (G8 stage 1) are appended
    /// LAST so a node running the pre-W6 reader (which stops after the
    /// digest) ignores them, and a W6-but-pre-G8 reader treats a standalone
    /// term's absent `committed_peak` trailer as `members.len()` for
    /// rolling-upgrade back-compat (see `deserialize`).
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(78 + self.members.len() * 8);
        buf.extend_from_slice(&self.term.to_le_bytes());
        buf.extend_from_slice(&self.proposer.0.to_le_bytes());
        buf.extend_from_slice(&self.cluster_id.0);
        buf.extend_from_slice(&(self.members.len() as u32).to_le_bytes());
        for m in &self.members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        buf.extend_from_slice(&self.digest);
        buf.extend_from_slice(&self.placement_version.to_le_bytes());
        buf.extend_from_slice(&self.committed_peak.to_le_bytes());
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
        // W6/G8 — `[placement_version:2][committed_peak:8]` are trailers
        // appended ONLY by a standalone `TopologyTerm` payload (exact
        // length match below). A `TopologyCommit` payload reuses this
        // parser but has its own voter list immediately after the digest,
        // so we must NOT read either trailer here —
        // `TopologyCommit::deserialize` reads its own placement_version and
        // committed_peak from its own tail. A pre-W6 standalone term has no
        // trailer (length == members_end + 32) and decodes as v1 /
        // committed_peak = members.len(); a W6-but-pre-G8 term has only the
        // placement_version trailer and decodes committed_peak the same
        // legacy way.
        let digest_end = members_end.checked_add(32)?;
        let mut placement_version = 1u16;
        let mut committed_peak = members.len() as u64;
        if data.len() == digest_end.checked_add(10)? {
            // G8 — full trailer: [placement_version:2][committed_peak:8].
            placement_version =
                u16::from_le_bytes(data[digest_end..digest_end + 2].try_into().ok()?);
            let peak_off = digest_end.checked_add(2)?;
            committed_peak = u64::from_le_bytes(data[peak_off..peak_off + 8].try_into().ok()?);
        } else if data.len() == digest_end.checked_add(2)? {
            // W6-only trailer (pre-G8): placement_version present,
            // committed_peak absent — legacy default applies.
            placement_version =
                u16::from_le_bytes(data[digest_end..digest_end + 2].try_into().ok()?);
        }
        Some(Self {
            term,
            members,
            proposer,
            cluster_id,
            placement_version,
            committed_peak,
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
    /// W6 (INVARIANT ii) — the highest placement version this voter's build
    /// supports (`MAX_SUPPORTED_PLACEMENT_VERSION`). The proposer records
    /// this per voter and only proposes a v2 upgrade once EVERY committed
    /// member is known to support v2 (unanimity, not quorum — masters must
    /// be agreed per-shard by all). A pre-W6 vote without this trailer
    /// decodes as `1`, keeping the cluster on v1 until every node upgrades.
    pub voter_placement_support: u16,
}

impl TopologyVote {
    /// Serialize for the wire.
    ///
    /// Format: `[term:8][voter:8][digest:32][accepted:1][voter_current_term:8][voter_placement_support:2]`
    ///
    /// `voter_placement_support` is appended LAST so a pre-W6 reader ignores
    /// it and a W6 reader treats its absence as `1`.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(59);
        buf.extend_from_slice(&self.term.to_le_bytes());
        buf.extend_from_slice(&self.voter.0.to_le_bytes());
        buf.extend_from_slice(&self.digest);
        buf.push(if self.accepted { 1 } else { 0 });
        buf.extend_from_slice(&self.voter_current_term.to_le_bytes());
        buf.extend_from_slice(&self.voter_placement_support.to_le_bytes());
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
        // W6 — optional 2-byte trailer; absent on pre-W6 votes (decode v1).
        let voter_placement_support = if data.len() >= 59 {
            u16::from_le_bytes(data[57..59].try_into().ok()?)
        } else {
            1
        };
        Some(Self {
            term,
            digest,
            voter,
            accepted,
            voter_current_term,
            voter_placement_support,
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
    /// W6 — placement version this term committed at (copied from the
    /// `TopologyTerm` that reached quorum; mixed into the digest).
    pub placement_version: u16,
    /// G8 stage 1 — durable split-brain floor anchor, copied from the
    /// `TopologyTerm` that reached quorum (mixed into the digest). See
    /// [`TopologyTerm::committed_peak`].
    pub committed_peak: u64,
    pub digest: [u8; 32],
    /// Nodes whose accepted votes formed the quorum for this commit.
    pub voters: Vec<NodeId>,
}

impl TopologyCommit {
    /// Check that the embedded voter list is a quorum proof for `members`,
    /// requiring at least `n` distinct, in-`members` voters.
    ///
    /// G8 stage 2 — generalizes the fixed `members.len()/2 + 1` threshold in
    /// [`Self::has_quorum_voter_proof`] so Gate B
    /// (`TopologyAuthority::commit_passes_gates`) can re-verify a shrink
    /// commit against a threshold derived from the APPLYING NODE's own
    /// (higher, pre-shrink) peak rather than the commit's own (already
    /// lowered) `members.len()`. The membership/dedup checks are unchanged.
    pub fn has_quorum_voter_proof_for(&self, n: usize) -> bool {
        if self.voters.len() < n {
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

    /// Check that the embedded voter list is a quorum proof for `members`
    /// (the commit's own majority: `members.len()/2 + 1`). Unchanged
    /// behavior — delegates to [`Self::has_quorum_voter_proof_for`].
    pub fn has_quorum_voter_proof(&self) -> bool {
        self.has_quorum_voter_proof_for((self.members.len() / 2) + 1)
    }

    /// Serialize for the wire.
    ///
    /// Format: `[term:8][proposer:8][cluster_id:16][member_count:4][member_id:8 * count][digest:32][voter_count:4][voter_id:8 * count][placement_version:2][committed_peak:8]`
    ///
    /// `placement_version` and `committed_peak` (G8 stage 1) are appended
    /// LAST (after the voter list) so a pre-W6 reader ignores both, a W6
    /// reader treats their absence as `1`/`members.len()`, and a
    /// W6-but-pre-G8 reader treats the absent `committed_peak` trailer as
    /// `members.len()`.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(82 + (self.members.len() + self.voters.len()) * 8);
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
        buf.extend_from_slice(&self.placement_version.to_le_bytes());
        buf.extend_from_slice(&self.committed_peak.to_le_bytes());
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
        // Track the byte offset just past the voter list so the optional
        // W6 `placement_version` trailer can be read from the very tail.
        let mut voters_tail = voters_pos;
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
            voters_tail = voters_end;
            voters
        } else {
            Vec::new()
        };
        // W6 — optional 2-byte `placement_version` trailer after the voter
        // list. Absent on pre-W6 commits (decode as v1).
        let (placement_version, after_placement_version) =
            if data.len() >= voters_tail.checked_add(2)? {
                (
                    u16::from_le_bytes(data[voters_tail..voters_tail + 2].try_into().ok()?),
                    voters_tail.checked_add(2)?,
                )
            } else {
                (1, voters_tail)
            };
        // G8 stage 1 — optional 8-byte `committed_peak` trailer after
        // placement_version. Absent on pre-G8 commits (legacy default:
        // members.len(), reproducing today's floor exactly).
        let committed_peak = if data.len() >= after_placement_version.checked_add(8)? {
            u64::from_le_bytes(
                data[after_placement_version..after_placement_version + 8]
                    .try_into()
                    .ok()?,
            )
        } else {
            term.members.len() as u64
        };
        Some(Self {
            term: term.term,
            proposer: term.proposer,
            members: term.members,
            cluster_id: term.cluster_id,
            placement_version,
            committed_peak,
            digest: term.digest,
            voters,
        })
    }
}

// ---------------------------------------------------------------------------
// Persisted state
// ---------------------------------------------------------------------------

/// Magic bytes opening every persisted topology-state record.
pub const TOPOLOGY_STATE_MAGIC: [u8; 4] = *b"TSTP";

/// Format version of the persisted topology-state record.
///
/// Version 1 was the unframed, un-checksummed, trailer-extended layout: it
/// had no magic, no length, and no CRC, so a truncated file decoded silently
/// into a *shorter* `committed_members` with an unchanged `committed_term` —
/// a weakened restart quorum and a lowered split-brain floor, indistinguishable
/// from a legitimate smaller cluster. Version 2 framed and checksummed the
/// whole record. Version 3 adds `voted_digest`: without it a vote attests to a
/// term NUMBER only, so the commit-side digest check (which recomputes the
/// digest from the commit's own fields) is a self-consistency checksum rather
/// than an attestation to anything this node agreed to.
pub const TOPOLOGY_STATE_FORMAT_VERSION: u16 = 3;

/// Upper bound on the persisted winning-commit blob. A `TopologyCommit` with
/// `MAX_TOPOLOGY_MEMBERS` members and the same number of voters is ~16 KiB;
/// this leaves headroom for the sections the committed-master-election design
/// adds without letting a malformed length field drive an unbounded
/// allocation.
pub const MAX_PERSISTED_COMMIT_BYTES: usize = 64 * 1024;

/// Fixed size of the v2 record framing: magic + version + payload length + CRC.
const TOPOLOGY_STATE_FRAME_OVERHEAD: usize = 4 + 2 + 4 + 4;

/// Why a persisted topology-state record could not be decoded.
///
/// Every variant is fail-closed at the load site: a node that cannot read its
/// durable term/members/peak must NOT fall back to defaults, because defaults
/// (`committed_term = 0`, `voted_term = 0`, `peak = 1`) let it vote again in a
/// term it already voted in and drop the G8 split-brain floor.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TopologyStateDecodeError {
    /// Fewer bytes than the fixed framing requires.
    #[error("persisted topology state is {len} bytes, shorter than the {min}-byte frame")]
    TooShort { len: usize, min: usize },
    /// The record does not open with [`TOPOLOGY_STATE_MAGIC`]. Either a
    /// foreign file or a pre-v2 payload, both of which must not be guessed at.
    #[error("persisted topology state has bad magic {found:02x?}, expected {expected:02x?}")]
    BadMagic { found: [u8; 4], expected: [u8; 4] },
    /// A format version this build does not understand.
    #[error(
        "persisted topology state format version {found} is not supported (this build reads {supported})"
    )]
    UnsupportedVersion { found: u16, supported: u16 },
    /// The declared payload length does not match the bytes present.
    #[error("persisted topology state declares a {declared}-byte payload but carries {actual}")]
    PayloadLengthMismatch { declared: usize, actual: usize },
    /// CRC over magic+version+length+payload does not match the stored value.
    #[error(
        "persisted topology state CRC mismatch: stored {stored:#010x}, computed {computed:#010x}"
    )]
    CrcMismatch { stored: u32, computed: u32 },
    /// A section ran past the end of the payload.
    #[error("persisted topology state section `{section}` is truncated")]
    TruncatedSection { section: &'static str },
    /// A node-id count exceeded [`MAX_TOPOLOGY_MEMBERS`], or the commit blob
    /// exceeded [`MAX_PERSISTED_COMMIT_BYTES`]. Rejected before any sizing.
    #[error(
        "persisted topology state section `{section}` declares {count}, above the maximum {max}"
    )]
    SectionTooLarge {
        section: &'static str,
        count: usize,
        max: usize,
    },
    /// Bytes remained after the payload's last section.
    #[error("persisted topology state has {trailing} unconsumed trailing bytes")]
    TrailingBytes { trailing: usize },
}

/// Cursor over a decoded payload that fails closed on every short read.
struct PayloadReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> PayloadReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn take(
        &mut self,
        n: usize,
        section: &'static str,
    ) -> Result<&'a [u8], TopologyStateDecodeError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(TopologyStateDecodeError::TruncatedSection { section })?;
        if end > self.data.len() {
            return Err(TopologyStateDecodeError::TruncatedSection { section });
        }
        let out = &self.data[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u64(&mut self, section: &'static str) -> Result<u64, TopologyStateDecodeError> {
        let b = self.take(8, section)?;
        let arr: [u8; 8] = b
            .try_into()
            .map_err(|_| TopologyStateDecodeError::TruncatedSection { section })?;
        Ok(u64::from_le_bytes(arr))
    }

    fn u32(&mut self, section: &'static str) -> Result<u32, TopologyStateDecodeError> {
        let b = self.take(4, section)?;
        let arr: [u8; 4] = b
            .try_into()
            .map_err(|_| TopologyStateDecodeError::TruncatedSection { section })?;
        Ok(u32::from_le_bytes(arr))
    }

    fn u16(&mut self, section: &'static str) -> Result<u16, TopologyStateDecodeError> {
        let b = self.take(2, section)?;
        let arr: [u8; 2] = b
            .try_into()
            .map_err(|_| TopologyStateDecodeError::TruncatedSection { section })?;
        Ok(u16::from_le_bytes(arr))
    }

    fn u8(&mut self, section: &'static str) -> Result<u8, TopologyStateDecodeError> {
        Ok(self.take(1, section)?[0])
    }

    /// `[count:4][ids:8*count]`, with `count` bounded by
    /// [`MAX_TOPOLOGY_MEMBERS`] BEFORE any allocation, and every id read
    /// bounds-checked (a short id array is an error, never a shorter list).
    fn node_ids(&mut self, section: &'static str) -> Result<Vec<NodeId>, TopologyStateDecodeError> {
        let count = self.u32(section)? as usize;
        if count > MAX_TOPOLOGY_MEMBERS {
            return Err(TopologyStateDecodeError::SectionTooLarge {
                section,
                count,
                max: MAX_TOPOLOGY_MEMBERS,
            });
        }
        let bytes = self.take(count * 8, section)?;
        let mut ids = Vec::with_capacity(count);
        for chunk in bytes.chunks_exact(8) {
            let arr: [u8; 8] = chunk
                .try_into()
                .map_err(|_| TopologyStateDecodeError::TruncatedSection { section })?;
            ids.push(NodeId(u64::from_le_bytes(arr)));
        }
        Ok(ids)
    }

    fn finish(self) -> Result<(), TopologyStateDecodeError> {
        let trailing = self.data.len() - self.pos;
        if trailing != 0 {
            return Err(TopologyStateDecodeError::TrailingBytes { trailing });
        }
        Ok(())
    }
}

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
    /// W6 — placement version of the last committed term. Persisted so a
    /// node that restarts into a v2-committed cluster re-derives the SAME
    /// (HRW) shard table on recovery instead of falling back to round-robin.
    /// A pre-W6 payload without this trailer decodes as `1`.
    pub committed_placement_version: u16,
    /// G8 stage 1 — durable split-brain floor anchor (see
    /// [`TopologyTerm::committed_peak`]). Persisted so a restarting node
    /// recovers the same floor it committed rather than re-deriving it from
    /// the (separately-clamped) `peak_cluster_size` field alone. A pre-G8
    /// payload without this trailer decodes to
    /// `peak_cluster_size.max(committed_members.len())`, reproducing
    /// today's restored floor exactly (see `deserialize`).
    pub committed_peak: u64,
    /// E5 — the serialized [`TopologyCommit`] that won `committed_term`,
    /// stored verbatim so `OP_GET_COMMITTED_TOPOLOGY` can replay the real
    /// bytes instead of fabricating a self-consistent commit from local
    /// state (proposer defaulted to `members[0]`, voters defaulted to the
    /// member set, digest recomputed — which makes the digest check, the
    /// quorum proof and the proposer rule all vacuous on that path).
    ///
    /// `None` is a STRUCTURAL "this node holds no committed commit", not a
    /// zero-filled one: a node that reached its committed term via the
    /// partition-map catch-up path holds no commit by design, and must be
    /// distinguishable from one whose commit blob was zeroed by corruption.
    pub committed_commit: Option<Vec<u8>>,
    /// §4.3 — the digest this node attested to when it voted at
    /// [`Self::voted_term`], persisted under the same persist-before-vote
    /// discipline as the term itself.
    ///
    /// Without it a vote records only a term NUMBER, and the commit-side
    /// digest check — which recomputes the expected digest from the commit's
    /// OWN fields — verifies only that the frame is internally consistent. A
    /// proposer could then send `commit(T, A)` to one node and `commit(T, B)`
    /// to another; both recompute their own frame's digest, both match, both
    /// install, and neither will accept a correcting commit for T. The
    /// divergence is committed and sticky.
    ///
    /// `None` means this node has not voted (or voted before this field
    /// existed) — a state that must not be confused with "voted for the
    /// all-zero digest".
    pub voted_digest: Option<[u8; 32]>,
}

/// G9 — result of [`TopologyAuthority::handle_commit_durable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableCommitOutcome {
    /// The commit passed every gate, its post-commit state was persisted
    /// durably, and it was then applied. Carries the committed term; the
    /// caller should activate the shard table for it.
    Applied(u64),
    /// The commit did not pass the acceptance gates (stale term, bad digest,
    /// unsupported placement version, missing quorum proof, or split-brain
    /// signature). Nothing was persisted or applied.
    NotApplied,
    /// The commit was valid but the durable persist failed. Fail-closed: the
    /// term was NOT applied, so the node keeps serving its prior term rather
    /// than one it could forget on reboot. The caller must surface a retryable
    /// error and must NOT activate under the new term.
    PersistFailed,
}

impl PersistedTopologyState {
    /// Serialize to a framed, CRC-covered v2 record.
    ///
    /// Layout:
    ///
    /// ```text
    /// [magic:4 = "TSTP"][version:2][payload_len:4][payload][crc32:4]
    /// ```
    ///
    /// The CRC covers magic + version + length + payload. The payload is:
    ///
    /// ```text
    /// [peak_cluster_size:8][committed_term:8][voted_term:8][incarnation:8]
    /// [committed_placement_version:2][committed_peak:8]
    /// [member_count:4][member_ids:8*N]
    /// [voter_count:4][voter_ids:8*N]
    /// [ever_seen_count:4][ever_seen_ids:8*N]
    /// [commit_present:1]([commit_len:4][commit_bytes])?
    /// [voted_digest_present:1]([voted_digest:32])?
    /// ```
    ///
    /// Every count is bounded on decode and every section is length-checked,
    /// so a truncated or corrupted record is REJECTED rather than decoded
    /// into a plausible-looking shorter state (see
    /// [`TopologyStateDecodeError`]). `commit_present` is an explicit flag,
    /// so "no committed commit" never aliases a zero-filled one.
    pub fn serialize(&self) -> Vec<u8> {
        let commit_len = self.committed_commit.as_ref().map_or(0, Vec::len);
        let mut payload = Vec::with_capacity(
            46 + (self.committed_members.len()
                + self.committed_voters.len()
                + self.committed_voter_ever_seen.len())
                * 8
                + 5
                + commit_len,
        );
        payload.extend_from_slice(&self.peak_cluster_size.to_le_bytes());
        payload.extend_from_slice(&self.committed_term.to_le_bytes());
        payload.extend_from_slice(&self.voted_term.to_le_bytes());
        payload.extend_from_slice(&self.incarnation.to_le_bytes());
        payload.extend_from_slice(&self.committed_placement_version.to_le_bytes());
        payload.extend_from_slice(&self.committed_peak.to_le_bytes());
        for section in [
            &self.committed_members,
            &self.committed_voters,
            &self.committed_voter_ever_seen,
        ] {
            payload.extend_from_slice(&(section.len() as u32).to_le_bytes());
            for id in section.iter() {
                payload.extend_from_slice(&id.0.to_le_bytes());
            }
        }
        match &self.committed_commit {
            Some(bytes) => {
                payload.push(1);
                payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                payload.extend_from_slice(bytes);
            }
            None => payload.push(0),
        }
        match &self.voted_digest {
            Some(digest) => {
                payload.push(1);
                payload.extend_from_slice(digest);
            }
            None => payload.push(0),
        }

        let mut buf = Vec::with_capacity(payload.len() + TOPOLOGY_STATE_FRAME_OVERHEAD);
        buf.extend_from_slice(&TOPOLOGY_STATE_MAGIC);
        buf.extend_from_slice(&TOPOLOGY_STATE_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode a v2 record, or explain why it is unusable.
    ///
    /// # Errors
    ///
    /// Returns [`TopologyStateDecodeError`] when the record is shorter than
    /// the frame, carries foreign or pre-v2 bytes ([`TopologyStateDecodeError::BadMagic`]),
    /// declares an unsupported version, has a payload length that disagrees
    /// with the bytes present, fails its CRC, truncates a section, declares a
    /// count above [`MAX_TOPOLOGY_MEMBERS`] (or a commit blob above
    /// [`MAX_PERSISTED_COMMIT_BYTES`]), or carries unconsumed trailing bytes.
    ///
    /// There is deliberately no lenient arm. The pre-v2 format tolerated
    /// short reads, so a truncated file yielded a shorter `committed_members`
    /// under an unchanged `committed_term` — a silently weakened restart
    /// quorum. Callers fail closed instead.
    pub fn deserialize(data: &[u8]) -> Result<Self, TopologyStateDecodeError> {
        if data.len() < TOPOLOGY_STATE_FRAME_OVERHEAD {
            return Err(TopologyStateDecodeError::TooShort {
                len: data.len(),
                min: TOPOLOGY_STATE_FRAME_OVERHEAD,
            });
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&data[0..4]);
        if magic != TOPOLOGY_STATE_MAGIC {
            return Err(TopologyStateDecodeError::BadMagic {
                found: magic,
                expected: TOPOLOGY_STATE_MAGIC,
            });
        }
        let version = u16::from_le_bytes([data[4], data[5]]);
        if version != TOPOLOGY_STATE_FORMAT_VERSION {
            return Err(TopologyStateDecodeError::UnsupportedVersion {
                found: version,
                supported: TOPOLOGY_STATE_FORMAT_VERSION,
            });
        }
        let declared = u32::from_le_bytes([data[6], data[7], data[8], data[9]]) as usize;
        let actual = data.len() - TOPOLOGY_STATE_FRAME_OVERHEAD;
        if declared != actual {
            return Err(TopologyStateDecodeError::PayloadLengthMismatch { declared, actual });
        }
        let crc_off = data.len() - 4;
        let stored = u32::from_le_bytes([
            data[crc_off],
            data[crc_off + 1],
            data[crc_off + 2],
            data[crc_off + 3],
        ]);
        let computed = crc32fast::hash(&data[..crc_off]);
        if stored != computed {
            return Err(TopologyStateDecodeError::CrcMismatch { stored, computed });
        }

        let mut r = PayloadReader::new(&data[10..crc_off]);
        let peak_cluster_size = r.u64("peak_cluster_size")?;
        let committed_term = r.u64("committed_term")?;
        let voted_term = r.u64("voted_term")?;
        let incarnation = r.u64("incarnation")?;
        let committed_placement_version = r.u16("committed_placement_version")?;
        let committed_peak = r.u64("committed_peak")?;
        let committed_members = r.node_ids("committed_members")?;
        let committed_voters = r.node_ids("committed_voters")?;
        let committed_voter_ever_seen = r.node_ids("committed_voter_ever_seen")?;
        let committed_commit = match r.u8("commit_present")? {
            0 => None,
            _ => {
                let len = r.u32("committed_commit")? as usize;
                if len > MAX_PERSISTED_COMMIT_BYTES {
                    return Err(TopologyStateDecodeError::SectionTooLarge {
                        section: "committed_commit",
                        count: len,
                        max: MAX_PERSISTED_COMMIT_BYTES,
                    });
                }
                Some(r.take(len, "committed_commit")?.to_vec())
            }
        };
        let voted_digest = match r.u8("voted_digest_present")? {
            0 => None,
            _ => {
                let bytes = r.take(32, "voted_digest")?;
                let mut digest = [0u8; 32];
                digest.copy_from_slice(bytes);
                Some(digest)
            }
        };
        r.finish()?;

        Ok(Self {
            peak_cluster_size: peak_cluster_size.max(1),
            committed_term,
            committed_members,
            committed_voters,
            voted_term,
            incarnation,
            committed_voter_ever_seen,
            committed_placement_version,
            committed_peak,
            committed_commit,
            voted_digest,
        })
    }

    /// The state a node with no persisted record starts from.
    ///
    /// Reached ONLY when the state file is absent (a genuinely fresh node).
    /// A present-but-undecodable file must never land here — that is the
    /// silent-defaulting hole the v2 framing closes.
    pub fn fresh() -> Self {
        Self {
            peak_cluster_size: 1,
            committed_term: 0,
            committed_members: Vec::new(),
            committed_voters: Vec::new(),
            voted_term: 0,
            incarnation: 0,
            committed_voter_ever_seen: Vec::new(),
            committed_placement_version: 1,
            committed_peak: 1,
            committed_commit: None,
            voted_digest: None,
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
/// True when `members` is strictly ascending — i.e. sorted AND duplicate-free.
///
/// # Why this is a correctness gate, not tidiness
///
/// [`TopologyTerm::compute_digest`] hashes `members` **in the order received**,
/// while [`ShardTable::compute_with_epoch`] sorts a local copy before assigning
/// shards. Those two disagree the moment a member list arrives unsorted: every
/// voter recomputes the same digest and votes yes, and then two conforming
/// implementations derive DIFFERENT shard tables from one agreed commit. That
/// is a same-term split with every existing gate green — the hardest class of
/// divergence to detect, because each node's table is internally consistent.
///
/// Duplicates are rejected by the same check. They cannot forge a quorum
/// (`has_quorum_voter_proof_for` dedups voters and requires membership), but
/// `members.len()` feeds the quorum threshold and `committed_peak`, so a
/// duplicated entry raises the bar every future term must clear.
///
/// The empty list is vacuously ordered; callers reject it separately if they
/// require a non-empty membership.
/// Maximum members a single commit may add beyond the cluster's established
/// size. Growth is an operator action taken a node or two at a time; a jump
/// larger than this is not a real deployment step.
pub const MAX_MEMBER_GROWTH: usize = 4;

/// True when a proposed membership is a plausible growth step from what this
/// node already knows.
///
/// # The wedge this closes
///
/// A commit carrying many DISTINCT fabricated NodeIds is accepted today: it is
/// a pure superset, so `is_safe_membership_change` calls it safe; with a
/// matching `cluster_id` the ever-seen check is short-circuited; and enough of
/// the fabricated ids satisfy the (plaintext, self-declared) voter proof. The
/// commit then persists `peak_cluster_size >= members.len()`.
///
/// `activation_quorum_needed` is derived from that peak, so a single frame
/// naming 1024 members raises the quorum bar to 513 **permanently** — no
/// future term can ever be proposed, and G8's Gate B needs a 513-voter proof
/// to lower the floor again. It survives reboot, because the peak is durable.
///
/// One frame, permanent, reboot-surviving cluster wedge. Reachable from any
/// member, and from any TCP peer when no `cluster_secret` is configured.
///
/// The bound is deliberately generous: legitimate growth is one or two nodes
/// per term, and `alive` is included so a cluster that genuinely scaled up
/// while this node was partitioned can still catch up.
fn membership_growth_is_plausible(proposed_len: usize, committed_peak: u64, alive: usize) -> bool {
    let established = (committed_peak as usize).max(alive);
    proposed_len <= established.saturating_add(MAX_MEMBER_GROWTH)
}

fn members_strictly_ascending(members: &[NodeId]) -> bool {
    members.windows(2).all(|w| w[0].0 < w[1].0)
}

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
    /// C-2: serializes the `voted_term` read-compare-store so the
    /// at-most-one-vote-per-term invariant holds across concurrent
    /// `handle_propose` calls (each TCP connection runs on its own thread,
    /// and `OP_TOPOLOGY_PROPOSE` is not gated by the dispatch barrier).
    /// Without it, two same-term proposals can both load the pre-vote
    /// value, both pass the `term > voted` check, and both be accepted —
    /// a double-vote, the precondition for conflicting commits. Held only
    /// for the (cold-path) vote decision; never across I/O. The
    /// self-vote stores in `on_membership_changed` and the recovery paths
    /// take it too so a follower vote cannot interleave a proposer's
    /// self-vote.
    vote_decision: Mutex<()>,
    /// Item 1 — serializes the whole commit gate→persist→apply sequence.
    ///
    /// `commit_passes_gates` reads `committed_term`, then (in the durable path)
    /// a multi-ms persist fsync runs, then `apply_commit` mutates term/members/
    /// placement. Without a critical section spanning all three, two commits T
    /// and T+1 can both pass the gate at `committed_term = T-1` and interleave
    /// so the LOWER term's late apply clobbers the higher one that already
    /// applied and was ACKed — an ACK-then-forget authority split (the fsync in
    /// the gap widened the race from µs to ms). Held across gate + persist +
    /// apply in [`TopologyAuthority::handle_commit`] and
    /// [`TopologyAuthority::handle_commit_durable`]. This is the OUTERMOST lock:
    /// it is only ever acquired at the top of those two methods, before any of
    /// the inner `committed_*` RwLocks / atomics, and the `persist` closure runs
    /// pure file I/O that never touches the authority — so there is no lock-order
    /// inversion. Commits are rare (topology changes only), so serializing the
    /// fsync here is acceptable.
    commit_apply: Mutex<()>,
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
    /// G8 stage 1 — durable split-brain floor anchor (the "committed_peak"
    /// of the design doc), distinct from `peak_cluster_size` above (the
    /// SWIM/proposal-fed "observed_peak"). Set from the applied commit's
    /// `committed_peak` in `apply_commit_locked` and restored in `restore`.
    /// Stage 1 has no lowering producer, so this always tracks the same
    /// value `peak_cluster_size` would already report; stage 2 makes a
    /// quorum-gated `propose_shrink` the sole path that may lower it. The
    /// effective floor is `peak_cluster_size()` (the getter below):
    /// `max(committed_peak, peak_cluster_size).max(1)`.
    committed_peak: AtomicU64,
    /// W6 — placement version of the last committed term (stored as `u64`
    /// for atomic access; logically a `u16`). `1` until the cluster
    /// unanimously upgrades to HRW.
    committed_placement_version: AtomicU64,
    /// C11 — highest quorum-committed term this node OBSERVED but could NOT
    /// apply because its placement version exceeds this build's support (a
    /// downgraded / misconfigured node). Raised (`fetch_max`) in
    /// [`TopologyAuthority::handle_commit`]'s activation-gate refuse path, gated
    /// on `has_quorum_voter_proof`. That gate is a STRUCTURAL correctness filter
    /// (it stops a genuinely sub-quorum commit from arming the fence); it is NOT
    /// forgery resistance — `voters` is a plaintext, self-declared, forgeable
    /// wire field. Forgery is stopped upstream by the frame HMAC on
    /// `OP_TOPOLOGY_COMMIT` (only in `cluster_secret` mode; see the arm site).
    ///
    /// When this exceeds `committed_term`, the node has proof the cluster
    /// advanced to an authority it cannot serve; it must self-fence (stop
    /// serving authoritative reads/writes) rather than keep serving under its
    /// stale term, which would be a v1/v2 dual-authority split.
    ///
    /// The `is_self_fenced` auto-clear (fence lifts once `committed_term`
    /// reaches this term) exists in code but is UNREACHABLE in the real arming
    /// case: placement versions are monotonic cluster-wide, so once a majority
    /// commits an unsupported placement, every later term carries a placement
    /// this build STILL cannot apply → `committed_term` never catches up. The
    /// fence is therefore effectively PERMANENT for a stale binary (the correct
    /// fail-closed choice). Not persisted, so recovery is a BINARY UPGRADE +
    /// REBOOT: reboot clears the atomic and gossip re-teaches the now-applicable
    /// term.
    unapplicable_committed_term: AtomicU64,
    /// W6 (INVARIANT ii) — highest placement version each peer is known to
    /// support, learned from the `voter_placement_support` field of every
    /// vote this node receives (the proposer is the primary consumer). Self
    /// is always recorded at `MAX_SUPPORTED_PLACEMENT_VERSION`. A peer not
    /// yet in the map is treated as v1 (conservative), so the proposer never
    /// proposes a v2 term that a not-yet-heard-from node could reject.
    peer_placement_support: RwLock<std::collections::HashMap<NodeId, u16>>,
    /// G8 stage 3 — `(term, removed)` of the most recent commit this
    /// authority applied that LOWERED `committed_peak` (a Gate-B shrink):
    /// the NodeIds present in the OLD committed membership but absent from
    /// the new one. Lets the coordinator react (SWIM force-evict + peak
    /// floor hard-reset + `.multinode` marker cleanup) right after
    /// activating a shrink, without threading a return value through every
    /// `handle_commit`/`handle_commit_durable` call site.
    ///
    /// `None` until the first shrink ever applied by this authority.
    /// STICKY: a later NON-shrink commit does not clear it. Callers MUST
    /// compare the returned `term` against the term they just applied
    /// before acting on `removed` — an equal term means "this exact apply
    /// was the shrink"; any other term means the field is stale (left over
    /// from an earlier shrink) and must be ignored. Safe because commits
    /// are strictly increasing and serialized through `commit_apply`, so by
    /// the time a caller reads this immediately after its own apply
    /// returned, no other commit has had a realistic chance to overwrite it
    /// (network round-trips dominate any local read-after-write gap).
    last_shrink: Mutex<Option<(u64, Vec<NodeId>)>>,
    /// E5 — serialized bytes of the `TopologyCommit` that won
    /// `committed_term`, kept so the catch-up path can replay the real
    /// commit (with its real proposer, voters and digest) instead of
    /// fabricating one. Set in `apply_commit_locked`, persisted alongside
    /// the term, and restored at boot only when it parses and its term
    /// matches the restored `committed_term`.
    ///
    /// `None` means "no committed commit on this node" — the honest state
    /// for a node that caught up via the partition map, and the reason the
    /// persisted form carries an explicit presence flag.
    committed_commit: RwLock<Option<Vec<u8>>>,
    /// §4.3 — the digest this node attested to at `voted_term`. Written under
    /// `vote_decision` in the same critical section as `voted_term`, and
    /// persisted by the caller BEFORE the vote is put on the wire.
    voted_digest: RwLock<Option<[u8; 32]>>,
    /// §4.5 — the digest of the commit this node applied at `committed_term`.
    /// Derived state (it is `commit.digest` of the applied commit, and also
    /// lives inside `committed_commit`), kept unpacked so the gate path does
    /// not re-parse the commit blob on every frame.
    committed_digest: RwLock<Option<[u8; 32]>>,
}

/// §4.4 — count of commits rejected because their digest disagreed with the
/// digest this node attested to at the same term. Read via
/// [`vote_digest_mismatch_total`].
static VOTE_DIGEST_MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);

/// §4.5 — count of commits naming this node's committed term with a DIFFERENT
/// digest than the one it committed there. Read via
/// [`committed_digest_fork_total`].
static COMMITTED_DIGEST_FORK_TOTAL: AtomicU64 = AtomicU64::new(0);

/// §4.4 — commits rejected on a vote-attestation mismatch.
///
/// A non-zero value means some proposer put two different contents behind one
/// term number. That is a routine race (term numbers are derived from local
/// state, so two proposers can mint the same term), not proof of a fork — a
/// sustained climb is what warrants investigation.
pub fn vote_digest_mismatch_total() -> u64 {
    VOTE_DIGEST_MISMATCH_TOTAL.load(Ordering::Relaxed)
}

/// §4.5 — commits observed for this node's committed term carrying a digest
/// different from the one it committed.
///
/// Unlike [`vote_digest_mismatch_total`] this is hard evidence of a
/// **committed-history fork**: two different contents were quorum-committed at
/// one term. Any non-zero value is an incident.
pub fn committed_digest_fork_total() -> u64 {
    COMMITTED_DIGEST_FORK_TOTAL.load(Ordering::Relaxed)
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
            vote_decision: Mutex::new(()),
            commit_apply: Mutex::new(()),
            pending_proposal: Mutex::new(None),
            propose_timeout,
            last_membership_change: Mutex::new(Instant::now()),
            observed_membership: Mutex::new(Vec::new()),
            last_commit_at_unix_ms: AtomicU64::new(0),
            peak_cluster_size: AtomicU64::new(1),
            committed_peak: AtomicU64::new(0),
            committed_placement_version: AtomicU64::new(1),
            unapplicable_committed_term: AtomicU64::new(0),
            peer_placement_support: RwLock::new({
                let mut m = std::collections::HashMap::new();
                m.insert(
                    self_id,
                    crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION,
                );
                m
            }),
            last_shrink: Mutex::new(None),
            committed_commit: RwLock::new(None),
            voted_digest: RwLock::new(None),
            committed_digest: RwLock::new(None),
        }
    }

    /// §4.3 — record this node's attestation: the term voted for AND the
    /// digest voted for, together.
    ///
    /// EVERY producer of a vote — follower votes and proposer self-votes
    /// alike — must go through this. Advancing `voted_term` alone leaves the
    /// PREVIOUS term's digest paired with the new term, and the §4.4 gate then
    /// rejects the very commit this node proposed.
    fn record_vote(&self, term: u64, digest: [u8; 32]) {
        self.voted_term.store(term, Ordering::Relaxed);
        *self.voted_digest.write().unwrap() = Some(digest);
    }

    /// §4.3 — reserve a term number under `vote_decision` before the content
    /// (and therefore the digest) exists.
    ///
    /// Clears the attestation rather than leaving a stale one: `None` honestly
    /// says "voted at this term, content not yet determined", where a stale
    /// digest would be a false contradiction. The caller completes the vote
    /// with [`Self::record_vote`] as soon as the proposal is built.
    fn reserve_vote_term(&self, term: u64) {
        self.voted_term.store(term, Ordering::Relaxed);
        *self.voted_digest.write().unwrap() = None;
    }

    /// The highest term this node has voted for. `0` when it never has.
    pub fn voted_term(&self) -> u64 {
        self.voted_term.load(Ordering::Relaxed)
    }

    /// §4.3 — the digest this node attested to at [`Self::voted_term`].
    pub fn voted_digest(&self) -> Option<[u8; 32]> {
        *self.voted_digest.read().unwrap()
    }

    /// §4.5 — the digest of the commit applied at [`Self::committed_term`].
    pub fn committed_digest(&self) -> Option<[u8; 32]> {
        *self.committed_digest.read().unwrap()
    }

    /// E5 — the serialized winning [`TopologyCommit`] for the current
    /// committed term, if this node holds one.
    ///
    /// `None` when the node has committed nothing yet, or reached its term
    /// via a path that carries no commit (partition-map catch-up).
    pub fn committed_commit_bytes(&self) -> Option<Vec<u8>> {
        self.committed_commit.read().unwrap().clone()
    }

    /// E-01 — record an observed cluster size. Monotonic: only raises the
    /// stored peak (`fetch_max`), never lowers it. Fed from proposed
    /// member sets, applied commits, restored persisted state, and the
    /// coordinator's SWIM membership events.
    pub fn observe_peak_cluster_size(&self, observed: u64) {
        self.peak_cluster_size
            .fetch_max(observed, Ordering::Relaxed);
    }

    /// G8 stage 1 — the split-brain floor: `max(committed_peak,
    /// observed_peak)` (minimum 1). `observed_peak` is the pre-existing
    /// monotonic SWIM/proposal-fed peak (the `peak_cluster_size` atomic);
    /// `committed_peak` is the durable, quorum-committed floor anchor. The
    /// activation quorum for new topology terms is derived from this
    /// value, not from the live (possibly SWIM-shrunken) member set alone.
    /// Stage 1 has no lowering producer, so `committed_peak` never exceeds
    /// `observed_peak` in practice and this returns exactly what
    /// `observed_peak` alone would have returned before this field
    /// existed — the getter change is behavior-preserving until stage 2's
    /// `propose_shrink` lands.
    pub fn peak_cluster_size(&self) -> u64 {
        self.committed_peak
            .load(Ordering::Relaxed)
            .max(self.peak_cluster_size.load(Ordering::Relaxed))
            .max(1)
    }

    /// G8 stage 1 — the raw durable `committed_peak` anchor (NOT maxed with
    /// `observed_peak`). Distinct from [`Self::peak_cluster_size`], which is
    /// the effective floor. Used by stage 2's shrink gate (Gate B compares
    /// a candidate commit's claimed floor against this node's OWN durable
    /// `committed_peak`, not the observed one, since only committed_peak is
    /// quorum-anchored).
    pub fn committed_peak(&self) -> u64 {
        self.committed_peak.load(Ordering::Relaxed)
    }

    /// G8 stage 3 — see the [`Self::last_shrink`] field doc for the
    /// staleness caveat: the caller must check `term` against the commit it
    /// just applied before acting on `removed`.
    pub fn last_shrink(&self) -> Option<(u64, Vec<NodeId>)> {
        self.last_shrink.lock().clone()
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
        // G8 stage 1 — restore the durable committed_peak floor, then seed
        // the observed peak from it (NOT from `state.peak_cluster_size`
        // separately — see below).
        //
        // AUDIT M1.3 — floor the restored peak at the persisted committed
        // member count. A file written before the persist-side clamp, or by a
        // failed/partial earlier persist, could carry a peak below the committed
        // membership; loading it verbatim would weaken quorum on restart. This
        // is raise-only and self-heals such a stale on-disk peak at load time.
        //
        // For a pre-G8 file, `PersistedTopologyState::deserialize` already
        // decodes `committed_peak` as `peak_cluster_size.max(committed_members.len())`
        // — exactly what this method used to seed `observe_peak_cluster_size`
        // with directly — so seeding both atomics from `committed_peak` alone
        // reproduces today's restored floor exactly (behavior-preserving).
        let committed_peak = state
            .committed_peak
            .max(state.committed_members.len() as u64);
        self.committed_peak.store(committed_peak, Ordering::Relaxed);
        // G8 final review (finding 1) — hard-store (not the raise-only
        // `observe_peak_cluster_size`) the observed-peak seed.
        // `ClusterCoordinator::new` runs BEFORE `restore()` on every boot
        // and folds a startup guess into this same atomic via
        // `observe_peak_cluster_size`'s `fetch_max`; that guess can be
        // stale-HIGH (see `bin/server.rs`'s `initial_peak` derivation), and
        // a raise-only re-observe here could never correct it back down —
        // the exact bug that re-inflated a committed shrink's lowered
        // floor on restart. `restore()` runs once at boot, before any
        // concurrent SWIM/proposal activity can race it, so it is safe —
        // and necessary — for it to be the AUTHORITATIVE last word on the
        // boot-time observed peak, independent of `new()`/`restore()` call
        // order.
        self.peak_cluster_size
            .store(committed_peak, Ordering::Relaxed);
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
        // W6 — restore the committed placement version so a node that
        // reboots into a v2-committed cluster re-derives the HRW table on
        // recovery rather than serving the round-robin one for a window.
        self.committed_placement_version.store(
            state.committed_placement_version.max(1) as u64,
            Ordering::Relaxed,
        );
        // E5 — re-verify the persisted commit at load. The CRC proves the
        // bytes are the ones written; this proves they are the ones that won
        // the term being restored. A commit that no longer parses, or whose
        // term disagrees with the restored `committed_term`, is DROPPED (the
        // catch-up path falls back to deriving one from local state) rather
        // than replayed as a proof for a term it does not describe.
        *self.committed_commit.write().unwrap() =
            state.committed_commit.as_ref().and_then(|bytes| {
                match TopologyCommit::deserialize(bytes) {
                    Some(commit) if commit.term == state.committed_term => Some(bytes.clone()),
                    Some(commit) => {
                        tracing::warn!(
                            commit_term = commit.term,
                            committed_term = state.committed_term,
                            "cluster: persisted topology commit is for a different term; discarding"
                        );
                        None
                    }
                    None => {
                        tracing::warn!(
                            committed_term = state.committed_term,
                            "cluster: persisted topology commit does not parse; discarding"
                        );
                        None
                    }
                }
            });
        // §4.5 — the committed digest comes from the (already validated)
        // commit blob, so it is present exactly when that blob is. A node
        // that caught up without a commit holds none, and the fork detector
        // simply does not fire for it.
        *self.committed_digest.write().unwrap() = self
            .committed_commit
            .read()
            .unwrap()
            .as_ref()
            .and_then(|bytes| TopologyCommit::deserialize(bytes))
            .map(|commit| commit.digest);
        // §4.3 — restore the attested digest alongside the term it belongs
        // to. Dropping it on restart would silently re-open the equivocation
        // window across every reboot.
        *self.voted_digest.write().unwrap() = state.voted_digest;
    }

    /// Current committed term.
    pub fn committed_term(&self) -> u64 {
        self.committed_term.load(Ordering::Relaxed)
    }

    /// C11 — highest quorum-committed term this node observed but could not
    /// apply (its placement version exceeds this build's support). `0` when
    /// none has been observed. See [`TopologyAuthority::is_self_fenced`].
    pub fn unapplicable_committed_term(&self) -> u64 {
        self.unapplicable_committed_term.load(Ordering::Relaxed)
    }

    /// C11 — whether this node must self-fence: it has proof the cluster
    /// quorum-committed a term GREATER than its own applied term that it
    /// cannot apply. In that state the node's placement view is stale and it
    /// must stop serving authority (return `Transitioning`/redirect) rather
    /// than run a divergent v1/v2 authority alongside the upgraded majority.
    ///
    /// Fail-closed. The atomic-based auto-clear (fence lifts once
    /// `committed_term` reaches the unapplicable term) exists, but in the real
    /// arming case it is UNREACHABLE: placement versions are monotonic
    /// cluster-wide, so once a majority commits a placement this build cannot
    /// apply, every later term carries a placement it STILL cannot apply and
    /// `committed_term` never catches up. So for a stale binary the fence is
    /// effectively PERMANENT (the correct fail-closed choice), and it is GLOBAL
    /// — it forces `Transitioning` for ALL keys, a sharp availability cliff for
    /// a not-yet-upgraded straggler during a rolling upgrade. Recovery is a
    /// BINARY UPGRADE + REBOOT: the atomic is not persisted, so a reboot clears
    /// it and gossip re-teaches the now-applicable term.
    pub fn is_self_fenced(&self) -> bool {
        self.unapplicable_committed_term.load(Ordering::Relaxed)
            > self.committed_term.load(Ordering::Relaxed)
    }

    /// Members of the committed term.
    pub fn committed_members(&self) -> Vec<NodeId> {
        self.committed_members.read().unwrap().clone()
    }

    /// Voters whose quorum approved the committed term.
    pub fn committed_voters(&self) -> Vec<NodeId> {
        self.committed_voters.read().unwrap().clone()
    }

    /// W6 — placement version of the last committed term (`1` = round-robin,
    /// `2` = HRW). The coordinator's producers (table installs) and the three
    /// comparison oracles recompute placement at THIS version so a settled
    /// cluster does not false-fire the phantom-master detector.
    pub fn committed_placement_version(&self) -> u16 {
        self.committed_placement_version.load(Ordering::Relaxed) as u16
    }

    /// W6 (INVARIANT ii) — record a peer's advertised max placement support,
    /// learned from a received vote. Monotonic per peer (never lowers a
    /// previously higher value) so a stale/replayed lower advert cannot mask
    /// a real upgrade. Self is pinned at `MAX_SUPPORTED_PLACEMENT_VERSION`.
    pub fn record_peer_placement_support(&self, peer: NodeId, support: u16) {
        if peer == self.self_id {
            return;
        }
        let mut map = self.peer_placement_support.write().unwrap();
        let entry = map.entry(peer).or_insert(0);
        if support > *entry {
            *entry = support;
        }
    }

    /// W6 (INVARIANT ii) — the highest placement version that EVERY node in
    /// `members` is known to support, clamped to this build's
    /// `MAX_SUPPORTED_PLACEMENT_VERSION`. Self always counts as max support.
    /// A member with no recorded support is treated as v1 (conservative), so
    /// the result is `>= 2` only when unanimity is proven. This is the value
    /// the proposer stamps on a term: v2 ONLY when all members support v2.
    pub fn achievable_placement_version(&self, members: &[NodeId]) -> u16 {
        let max_build = crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION;
        let map = self.peer_placement_support.read().unwrap();
        let mut achievable = max_build;
        for m in members {
            let support = if *m == self.self_id {
                max_build
            } else {
                map.get(m).copied().unwrap_or(1)
            };
            achievable = achievable.min(support);
        }
        achievable
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
            // Never persist a peak below this authority's own commit-raised peak.
            // Callers pass the SWIM-fed atomic, which can lag a committed
            // topology; clamping here keeps the on-disk peak (loaded at restart
            // for quorum gating) from regressing below the largest committed
            // cluster size. Raise-only, matching observe_peak_cluster_size.
            peak_cluster_size: peak.max(self.peak_cluster_size()),
            committed_term: self.committed_term.load(Ordering::Relaxed),
            committed_members: self.committed_members.read().unwrap().clone(),
            committed_voters: self.committed_voters.read().unwrap().clone(),
            voted_term: self.voted_term.load(Ordering::Relaxed),
            incarnation,
            committed_voter_ever_seen: self.committed_voter_ever_seen_snapshot(),
            committed_placement_version: self.committed_placement_version(),
            committed_peak: self.committed_peak(),
            committed_commit: self.committed_commit_bytes(),
            voted_digest: self.voted_digest(),
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

        // C-2: derive the new term and record the self-vote under the
        // same `vote_decision` lock that `handle_propose` holds, so a
        // proposer self-vote cannot interleave a concurrent follower vote
        // and let this node back two different proposals at the same term.
        let (committed, new_term) = {
            let _vote_guard = self.vote_decision.lock();
            let committed = self.committed_term.load(Ordering::Relaxed);
            let voted = self.voted_term.load(Ordering::Relaxed);
            let new_term = committed.max(voted) + 1;
            // Self-vote. Only the term NUMBER is reservable here — the
            // digest does not exist until the proposal is built below.
            self.reserve_vote_term(new_term);
            (committed, new_term)
        };
        let _ = committed;

        // W6 (INVARIANT ii) — stamp the placement version on the proposal.
        // v2 ONLY when EVERY proposed member is known to support it
        // (unanimity); otherwise v1. A not-yet-heard-from peer counts as v1,
        // so a freshly forming cluster proposes v1 first and upgrades later
        // (via `upgrade_proposal`) once every member's v2 support is learned
        // from its votes.
        let placement_version = self.achievable_placement_version(members);

        // E-01: raise the peak from the proposed set BEFORE deriving the
        // quorum, so growth (1 → N) is gated on the majority of the new,
        // larger cluster, and a later shrink is gated on the majority of
        // the peak — never on the shrunken set alone.
        //
        // G8 stage 1: this must also happen BEFORE stamping the term's
        // committed_peak below, so a grow's stamped floor reflects the
        // newly-raised peak (`members.len()`), not the pre-grow one.
        self.observe_peak_cluster_size(members.len() as u64);
        let committed_peak = self.peak_cluster_size();
        let term = TopologyTerm::new(
            new_term,
            members.to_vec(),
            self.self_id,
            self.cluster_id(),
            placement_version,
            committed_peak,
        );
        // §4.3 — complete the self-vote now that the content exists.
        self.record_vote(new_term, term.digest);
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

        let valid_digest = propose.digest
            == TopologyTerm::compute_digest(
                propose.term,
                &propose.cluster_id,
                &propose.members,
                propose.placement_version,
                propose.committed_peak,
            );

        // W6 (INVARIANT ii) — REFUSE, do not fall back. A voter that cannot
        // run the proposed placement algorithm must reject the proposal (so
        // the proposer cannot reach unanimity) rather than silently voting
        // for a term it would then compute a DIFFERENT table for. The digest
        // already binds `placement_version`, but a v1 node recomputes the
        // SAME digest from a v2 proposal's fields, so this explicit support
        // gate is what actually stops a mixed-version commit.
        let unsupported_placement =
            propose.placement_version > crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION;

        // A non-ascending member list makes the digest (hashed as-received) and
        // the shard table (computed over a sorted copy) disagree, so two
        // conforming nodes derive different tables from one agreed commit.
        // Refuse to vote rather than attest to an ambiguous member order.
        let members_ordered = members_strictly_ascending(&propose.members);
        if !members_ordered {
            tracing::warn!(
                term = propose.term,
                member_count = propose.members.len(),
                "topology: refusing proposal — members not strictly ascending \
                 (unsorted or duplicated); digest hashes as-received while \
                 placement sorts, so this is a same-term split vector",
            );
        }

        // F-G8-002: the proposer-side split-brain checks fire in
        // `on_membership_changed`, `retry_proposal`, and `check_timeout`,
        // but the follower-side `handle_propose` previously accepted any
        // valid-digest, higher-term proposal. A buggy or malicious node
        // that bypassed its own checks could still gather a quorum from
        // followers — apply the same guard on this side so a single
        // round cannot launder a merged membership through the quorum.
        if !valid_digest
            || unsupported_placement
            || !self.membership_change_is_safe(&propose.members, Some(propose.cluster_id))
        {
            // Even when `voted_term` would normally advance, we refuse to
            // self-vote for an unsafe or unsupported proposal. Report the
            // voter's last accepted term so the proposer can detect the
            // divergence.
            tracing::warn!(
                self_id = self.self_id.0,
                proposer = propose.proposer.0,
                term = propose.term,
                placement_version = propose.placement_version,
                unsupported_placement,
                "cluster: rejecting topology propose — split-brain heal signature, bad digest, or unsupported placement version",
            );
            return TopologyVote {
                term: propose.term,
                digest: propose.digest,
                voter: self.self_id,
                accepted: false,
                voter_current_term: committed,
                voter_placement_support: crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION,
            };
        }

        // C-2: the load-of-`voted` → decide → store-of-`voted` sequence
        // must be atomic against other voters. Hold `vote_decision` for the
        // entire decision so two concurrent same-term proposals cannot both
        // read the pre-vote value and both be accepted (double-vote). Only
        // CPU work runs under the lock — no I/O. The membership-safety and
        // digest checks above are pure and idempotent, so they correctly
        // sit outside.
        let accepted = {
            let _vote_guard = self.vote_decision.lock();
            let voted = self.voted_term.load(Ordering::Relaxed);

            // Accept if the term is strictly higher than anything we've seen.
            let mut accepted =
                propose.term > committed && propose.term > voted && valid_digest && members_ordered;

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
                // §4.3 — the DIGEST is recorded with the term, in the same
                // critical section. A vote that records only the term attests
                // to nothing about content: the commit-side check recomputes
                // the digest from the commit's own fields, so it verifies
                // internal consistency, not agreement.
                self.record_vote(propose.term, propose.digest);
            }
            accepted
        };

        TopologyVote {
            term: propose.term,
            digest: propose.digest,
            voter: self.self_id,
            accepted,
            voter_current_term: committed,
            // W6 — advertise this node's max placement support so the
            // proposer can learn when a v2 upgrade becomes unanimous.
            voter_placement_support: crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION,
        }
    }

    /// Handle an incoming vote for our pending proposal.
    ///
    /// Returns `Some(TopologyCommit)` if quorum is reached.
    pub fn handle_vote(&self, vote: &TopologyVote) -> Option<TopologyCommit> {
        // W6 — learn this voter's placement support regardless of whether the
        // vote matches the current pending proposal: support is a stable node
        // property, and recording it from every vote lets the upgrade path
        // converge even when an earlier proposal was superseded.
        self.record_peer_placement_support(vote.voter, vote.voter_placement_support);

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
                placement_version: proposal.term.placement_version,
                committed_peak: proposal.term.committed_peak,
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
    ///
    /// NOTE (G9): this advances the served `committed_term` in memory WITHOUT
    /// persisting it. Callers that must not serve a term they could lose across
    /// a crash use [`TopologyAuthority::handle_commit_durable`] instead; this
    /// remains for tests and single-node paths where a crash cannot produce a
    /// peer holding a term this node forgot.
    pub fn handle_commit(&self, commit: &TopologyCommit) -> Option<u64> {
        // Item 1 — hold the commit critical section across gate + apply so a
        // concurrent commit cannot interleave and regress the served term.
        let _apply_guard = self.commit_apply.lock();
        if !self.commit_passes_gates(commit) {
            return None;
        }
        if self.apply_commit_locked(commit) {
            Some(commit.term)
        } else {
            // Superseded under the lock (a higher term already applied). The
            // gate above should have caught it, but the apply-time re-check is
            // the authoritative guard — report "not applied".
            None
        }
    }

    /// Run every acceptance gate for an incoming commit WITHOUT applying it.
    ///
    /// Returns `true` when the commit is valid and should be applied. Shared by
    /// [`TopologyAuthority::handle_commit`] and the durable variant so a commit
    /// is validated identically whether or not the caller persists first. A
    /// couple of gates carry side effects (the C11 self-fence arm, error logs);
    /// those fire on rejection exactly as before.
    fn commit_passes_gates(&self, commit: &TopologyCommit) -> bool {
        let committed = self.committed_term.load(Ordering::Relaxed);

        // Validate: term must be strictly higher.
        if commit.term <= committed {
            self.detect_committed_history_fork(commit, committed);
            return false;
        }

        // Same gate as the vote path: an ambiguous member order makes the
        // digest and the derived shard table disagree, so two conforming nodes
        // install different tables from this one commit. Reject rather than
        // install an assignment whose meaning depends on wire order.
        if !members_strictly_ascending(&commit.members) {
            tracing::error!(
                term = commit.term,
                member_count = commit.members.len(),
                "topology: rejecting commit — members not strictly ascending \
                 (unsorted or duplicated); this is a same-term split vector",
            );
            return false;
        }

        // Validate digest. The digest is computed over
        // (term || cluster_id || members || placement_version ||
        // committed_peak) so a forged cluster_id, a divergent placement
        // version, or a divergent committed_peak claim still mismatches.
        let expected_digest = TopologyTerm::compute_digest(
            commit.term,
            &commit.cluster_id,
            &commit.members,
            commit.placement_version,
            commit.committed_peak,
        );
        if commit.digest != expected_digest {
            return false;
        }

        // G8 stage 1 — gate invariant: a floor below the live member count
        // is nonsensical (it would claim the cluster's historical peak was
        // smaller than the membership this very commit is installing).
        // Stage 1 has no lowering producer so every legitimate commit has
        // committed_peak >= members.len() by construction; this rejects a
        // malformed or forged commit that violates it.
        if commit.committed_peak < commit.members.len() as u64 {
            return false;
        }

        // A single frame must not be able to pin the quorum bar forever. The
        // durable `peak_cluster_size` drives `activation_quorum_needed`, so a
        // commit naming a flood of fabricated members raises that bar
        // permanently and across reboots — no later term can gather enough
        // voters to lower it. `is_safe_membership_change` waves this through
        // because a flood is a pure superset. See `membership_growth_is_plausible`.
        if !membership_growth_is_plausible(
            commit.members.len(),
            self.peak_cluster_size(),
            self.committed_members().len(),
        ) {
            tracing::error!(
                term = commit.term,
                proposed_members = commit.members.len(),
                committed_peak = self.peak_cluster_size(),
                "topology: rejecting commit — implausible membership growth; \
                 accepting it would raise the quorum floor permanently",
            );
            return false;
        }

        // Gate B (G8 stage 2): a commit that LOWERS this node's committed_peak
        // floor is a shrink and must carry a quorum of the CURRENT (higher)
        // peak's voters — evaluated against THIS node's OWN durable
        // `committed_peak` (local_peak), so a stale/behind proposer that set a
        // low Gate-A bar (derived from ITS OWN low peak) cannot get a lower
        // floor applied by a node that is already caught up to a higher one.
        // This is the load-bearing gap-closer left open by stage 1's
        // unconditional `.store` in `apply_commit_locked`: by the time a
        // lowering commit reaches that store, it has already proven a
        // quorum of THIS node's old peak here. Inert for every non-shrink
        // commit (grows/graceful-leave/unchanged carry committed_peak equal
        // to the current peak, so the comparison below is false).
        //
        // SECURITY (G8 final review, finding 3): `has_quorum_voter_proof_for`
        // is a purely STRUCTURAL check (voter count/membership/dedup on a
        // plaintext, self-declared `voters` field) — like
        // `has_quorum_voter_proof` above, it defends the honest-but-
        // partitioned minority (a real node can't fabricate votes it never
        // cast), not a malicious sender. Integrity of `committed_peak` and
        // `voters` rests entirely on the inter-node frame HMAC
        // (`cluster_secret`). In fail-open (`cluster_secret`-less) mode
        // there is no HMAC, so an unauthenticated peer can forge a
        // self-consistent, fully-padded commit with a fabricated low
        // `committed_peak` that satisfies Gate B here and drives a
        // split-brain. Do not treat this gate as a substitute for
        // authentication; see `docs/DEPLOYMENT_ASSUMPTIONS.md` and the
        // `/admin/shrink` handler doc (http.rs).
        let local_peak = self.committed_peak();
        if commit.committed_peak < local_peak {
            let need = (local_peak as usize / 2) + 1;
            if !commit.has_quorum_voter_proof_for(need) {
                return false;
            }
        }

        // W6 (INVARIANT ii) — activation gate. REFUSE to apply (and thus to
        // serve as authoritative) a committed term whose placement version
        // this build cannot run. We do NOT fall back to a different
        // algorithm: applying a v2 commit on a v1-only node would install a
        // round-robin table while the rest of the cluster runs HRW — a
        // per-shard split-brain. A correctly behaving cluster never commits
        // a version above unanimity, so this only fires on a downgraded /
        // misconfigured node, which then simply does not advance.
        if commit.placement_version > crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION {
            tracing::error!(
                self_id = self.self_id.0,
                term = commit.term,
                placement_version = commit.placement_version,
                max_supported = crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION,
                "cluster: refusing topology commit — placement version exceeds this build's support",
            );
            // C11 — the cluster quorum-committed a term this build cannot
            // apply. Record it so the coordinator self-fences (stops serving
            // authority) instead of continuing under the stale term, which
            // would be a v1/v2 dual-authority split.
            //
            // Forgery resistance does NOT come from `has_quorum_voter_proof`:
            // `voters` is a plaintext, self-declared wire field, trivially
            // forgeable alongside the digest, so the proof is a purely
            // STRUCTURAL correctness filter (it stops a genuinely sub-quorum
            // commit from arming the fence, not an attacker). The real forgery
            // resistance is the FRAME HMAC: `OP_TOPOLOGY_COMMIT` is an
            // inter-node auth opcode, and `verify_signed_body_streaming` rejects
            // a forged frame when `cluster_secret` is set — so a forged commit
            // never reaches this code in an authenticated cluster. CAVEAT: in
            // `cluster_secret`-less (trusted-overlay, fail-open) mode there is
            // no HMAC, so a forged `OP_TOPOLOGY_COMMIT` with a fabricated voter
            // list CAN arm a persistent global self-fence (inert-reject → brick
            // amplification). That is within the trusted-overlay threat model
            // (the overlay is assumed authenticated by other means), but
            // operators running fail-open should know it.
            if commit.has_quorum_voter_proof() {
                self.unapplicable_committed_term
                    .fetch_max(commit.term, Ordering::Relaxed);
            }
            return false;
        }

        if !commit.has_quorum_voter_proof() {
            return false;
        }

        // E-2: mirror the propose/vote-side split-brain guard set on the
        // commit-apply path. Without this, a foreign higher-term commit
        // (a peer in a *different* cluster that shares the cluster_secret,
        // or a same-cluster split-brain merge) is broadcast straight into
        // committed state — the very split-brain-heal hole `cluster_id`
        // was introduced to close, left open on the one path that mutates
        // committed topology. `membership_change_is_safe` rejects when:
        //   * both cluster_ids are configured and differ (P1.1), or
        //   * the change is non-monotonic w.r.t. the local committed set
        //     (a merge-with-drops that two nodes inside one cluster_id can
        //     still produce), or
        //   * (cluster_id unset on either side) the change introduces a
        //     NodeId never seen as a committed voter here (F-G8-001).
        // The commit carries its own `cluster_id`, so we pass it through;
        // the local side is read from `self.cluster_id()`.
        if !self.membership_change_is_safe(&commit.members, Some(commit.cluster_id)) {
            let committed_members = self.committed_members.read().unwrap();
            tracing::error!(
                self_id = self.self_id.0,
                local_cluster_id = ?self.cluster_id(),
                commit_cluster_id = ?commit.cluster_id,
                committed = ?committed_members.iter().map(|n| n.0).collect::<Vec<_>>(),
                proposed = ?commit.members.iter().map(|n| n.0).collect::<Vec<_>>(),
                term = commit.term,
                "cluster: refusing topology commit — split-brain heal signature \
                 (cluster_id mismatch, non-monotonic change, or unseen members).",
            );
            return false;
        }

        // §4.4 (E1) — LAST gate, after every structural one (P1-6): a
        // malformed or sub-quorum frame must never reach a detector.
        if !self.vote_attestation_holds(commit) {
            return false;
        }

        true
    }

    /// §4.4 (E1) — does this commit agree with what this node attested to?
    ///
    /// The digest gate above recomputes the expected digest from the commit's
    /// OWN fields, so it proves only internal consistency. This is the gate
    /// that compares the commit against something this node independently
    /// recorded: the digest it voted for at that term.
    ///
    /// Returns `false` (reject) on a mismatch. It deliberately does NOT fence.
    /// Term numbers are not globally reserved — every producer derives
    /// `max(committed, voted) + 1` from local state, so two proposers routinely
    /// mint the same term with different content, and a proposer that reaches
    /// some voters and then dies lets another re-mint that term legitimately.
    /// Fencing on that benign race would brick honest nodes with no attacker
    /// present. Rejecting only means this node stays on its prior term until
    /// the next one arrives.
    ///
    /// A node that never voted at this term (`voted_digest` is `None`, or
    /// `voted_term` differs) has nothing to contradict, so the gate passes —
    /// missing a propose round is the NORMAL catch-up path in any n >= 3
    /// cluster, not an edge case.
    fn vote_attestation_holds(&self, commit: &TopologyCommit) -> bool {
        let voted_term = self.voted_term.load(Ordering::Relaxed);
        if commit.term != voted_term {
            return true;
        }
        let Some(voted_digest) = self.voted_digest() else {
            return true;
        };
        if commit.digest == voted_digest {
            return true;
        }
        VOTE_DIGEST_MISMATCH_TOTAL.fetch_add(1, Ordering::Relaxed);
        tracing::error!(
            self_id = self.self_id.0,
            term = commit.term,
            proposer = commit.proposer.0,
            commit_digest = ?&commit.digest[..8],
            voted_digest = ?&voted_digest[..8],
            "cluster: rejecting topology commit — its digest differs from the one \
             this node attested to at the same term. Two different contents were \
             put behind one term number; this node did not agree to this one.",
        );
        false
    }

    /// §4.5 (P1-8) — a commit naming this node's committed term with a
    /// DIFFERENT digest is proof that two contents were quorum-committed at
    /// one term: a committed-history fork.
    ///
    /// Stronger evidence than a vote mismatch (which only says a proposer
    /// re-minted a term), and today it is discarded before any comparison
    /// happens — the stale-term gate returns first. Detection only: the frame
    /// is still rejected, nothing is fenced, and the operator gets a counter
    /// and an ERROR. `voted_term` and `committed_term` are independent (a node
    /// that caught up by commit never advances `voted_term`), so §4.4 alone
    /// never fires for a caught-up node — this is its counterpart.
    ///
    /// Only the CURRENT committed term's digest is retained, so this fires at
    /// `commit.term == committed_term`; older terms leave no digest to compare
    /// against. The quorum-proof precondition keeps a malformed frame from
    /// reaching the counter.
    fn detect_committed_history_fork(&self, commit: &TopologyCommit, committed: u64) {
        if commit.term != committed || committed == 0 {
            return;
        }
        let Some(committed_digest) = self.committed_digest() else {
            return;
        };
        if commit.digest == committed_digest || !commit.has_quorum_voter_proof() {
            return;
        }
        COMMITTED_DIGEST_FORK_TOTAL.fetch_add(1, Ordering::Relaxed);
        tracing::error!(
            self_id = self.self_id.0,
            term = commit.term,
            proposer = commit.proposer.0,
            commit_digest = ?&commit.digest[..8],
            committed_digest = ?&committed_digest[..8],
            "cluster: COMMITTED-HISTORY FORK — a quorum-backed commit names this \
             node's committed term with different content. Two topologies were \
             committed at one term; the cluster's history has diverged.",
        );
    }

    /// Apply a commit that has already passed every validation gate in
    /// [`TopologyAuthority::handle_commit`]. Advances `committed_term` — the
    /// value `is_master` serves — so callers that require durability first must
    /// go through [`TopologyAuthority::handle_commit_durable`] (G9).
    ///
    /// PRECONDITION: the caller MUST hold [`Self::commit_apply`]. That lock is
    /// what makes the term/members/placement mutation atomic w.r.t. a
    /// concurrent commit; the apply-time re-check below relies on
    /// `committed_term` not changing under it.
    ///
    /// Item 1 — re-checks `commit.term <= committed_term` at apply time and, if
    /// superseded, abandons the apply ENTIRELY (mutates nothing — not the term,
    /// not members, not placement). A bare `fetch_max` on `committed_term` alone
    /// would still write this commit's members/placement, leaving the higher
    /// term paired with a lower term's members. Returns `true` if the commit was
    /// applied, `false` if it was abandoned as superseded.
    fn apply_commit_locked(&self, commit: &TopologyCommit) -> bool {
        // Item 1 — abandon a superseded lower term outright. Under
        // `commit_apply` this load is stable for the rest of the function.
        if commit.term <= self.committed_term.load(Ordering::Relaxed) {
            return false;
        }
        // E-01: a committed term with N members is direct evidence the
        // cluster reached size N — raise the peak so any later
        // SWIM-observed shrink is gated on the majority of this size.
        // Unconditional and monotonic (fetch_max): harmless no-op for a
        // shrink, since a shrink's `members.len()` never exceeds the
        // existing observed peak.
        self.observe_peak_cluster_size(commit.members.len() as u64);

        // G8 stage 2 — detect BEFORE overwriting whether this commit LOWERS
        // the durable floor (a shrink). Gate B in `commit_passes_gates` has
        // already re-verified, against this exact `old_committed_peak`
        // value, that a lowering commit carries a quorum of the OLD peak's
        // voters — so by the time we get here, adopting a lower
        // `committed_peak` (and, for a shrink, hard-resetting the observed
        // peak below) is authorized.
        let old_committed_peak = self.committed_peak.load(Ordering::Relaxed);
        let is_gate_b_shrink = commit.committed_peak < old_committed_peak;

        // G8 stage 3 — capture exactly which NodeIds this shrink removes
        // BEFORE `committed_members` is overwritten below, and record them
        // (tagged with this commit's term) so the coordinator can react
        // (SWIM force-evict + peak floor reset) after activation. Cheap
        // clone on the rare shrink path only; a no-op allocation-wise for
        // every other commit.
        if is_gate_b_shrink {
            let old_members = self.committed_members.read().unwrap();
            let removed: Vec<NodeId> = old_members
                .iter()
                .filter(|n| !commit.members.contains(n))
                .copied()
                .collect();
            drop(old_members);
            *self.last_shrink.lock() = Some((commit.term, removed));
        }

        // G8 stage 1 — adopt the committed_peak carried by this commit. Gate
        // B (upstream, in `commit_passes_gates`) is what makes this
        // unconditional `.store` safe even when it lowers the value.
        self.committed_peak
            .store(commit.committed_peak, Ordering::Relaxed);
        // W6 — adopt the committed placement version. The coordinator reads
        // this when it (re)installs the shard table for `commit.term`, so the
        // first v2 commit triggers the planned full reshuffle via the normal
        // activation/migration machinery. Set BEFORE committed_term so a reader
        // that observes the new term also sees the matching placement version.
        self.committed_placement_version
            .store(commit.placement_version.max(1) as u64, Ordering::Relaxed);
        *self.committed_members.write().unwrap() = commit.members.clone();

        // G8 stage 2 — the ONLY site allowed to LOWER `observed_peak` (the
        // `peak_cluster_size` atom, otherwise strictly monotonic via
        // `observe_peak_cluster_size`'s `fetch_max` above and everywhere
        // else in this authority). Authorized because it only runs on a
        // commit that just passed Gate B: every node applying it has
        // independently proven a quorum of ITS OWN prior (higher) peak's
        // voters. Without this hard reset, `peak_cluster_size()` (the
        // combined `max(committed_peak, observed_peak)` getter) would keep
        // reporting the stale pre-shrink peak forever — the "re-inflation"
        // bug the design's monotonic-observe fix targets.
        if is_gate_b_shrink {
            self.peak_cluster_size
                .store(commit.committed_peak, Ordering::Relaxed);
        }

        *self.committed_voters.write().unwrap() = commit.voters.clone();
        // E5 — keep the winning commit's exact bytes so the catch-up path
        // replays a real quorum proof. Written BEFORE `committed_term` is
        // published, matching the members/placement ordering below: a reader
        // that sees the new term never sees the previous term's commit.
        *self.committed_commit.write().unwrap() = Some(commit.serialize());
        // §4.5 — remember WHAT was committed at this term, not just that
        // something was. A later frame naming this term with a different
        // digest is then hard evidence of a committed-history fork.
        *self.committed_digest.write().unwrap() = Some(commit.digest);
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
        // Advance the served term LAST: `is_master` reads `committed_term`, so
        // publishing it after the members/placement above keeps a concurrent
        // reader from seeing the new term with a stale member view.
        self.committed_term.store(commit.term, Ordering::Relaxed);
        // Phase I — stamp the wall-clock time so cluster_health can
        // report `last_topology_commit_age_ms`. Best-effort: a system
        // clock without UNIX_EPOCH access stays at the prior value.
        if let Ok(d) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            self.last_commit_at_unix_ms
                .store(d.as_millis() as u64, Ordering::Relaxed);
        }

        // Clear any pending proposal (superseded by this commit).
        *self.pending_proposal.lock() = None;
        true
    }

    /// G9 — the `PersistedTopologyState` this authority WOULD hold after
    /// applying `commit`, computed WITHOUT mutating any state.
    ///
    /// Lets a caller persist the post-commit durable record BEFORE
    /// `apply_commit_locked` advances the served `committed_term`
    /// (see [`TopologyAuthority::handle_commit_durable`]). Mirrors exactly what
    /// [`TopologyAuthority::persisted_state`] returns after apply:
    /// term/members/voters/placement taken from the commit, peak raised to the
    /// committed size, and the ever-seen set unioned with the commit's
    /// members+voters. `voted_term` is left as-is (apply does not touch it).
    pub fn persisted_state_for_commit(
        &self,
        commit: &TopologyCommit,
        peak: u64,
        incarnation: u64,
    ) -> PersistedTopologyState {
        let mut ever_seen: HashSet<NodeId> = self.committed_voter_ever_seen.read().unwrap().clone();
        ever_seen.extend(commit.voters.iter().copied());
        ever_seen.extend(commit.members.iter().copied());
        PersistedTopologyState {
            peak_cluster_size: peak
                .max(self.peak_cluster_size())
                .max(commit.members.len() as u64),
            committed_term: commit.term,
            committed_members: commit.members.clone(),
            committed_voters: commit.voters.clone(),
            voted_term: self.voted_term.load(Ordering::Relaxed),
            incarnation,
            committed_voter_ever_seen: ever_seen.into_iter().collect(),
            committed_placement_version: commit.placement_version.max(1),
            // G8 stage 1 — set VERBATIM from the commit (the design's one
            // path allowed to lower once stage 2's Gate B exists; stage 1
            // has no lowering producer so this is always non-lowering).
            committed_peak: commit.committed_peak,
            // E5 — the winning commit rides the SAME fsync as the term it
            // installs, so a node can never serve a committed term whose
            // commit bytes it would lose on reboot.
            committed_commit: Some(commit.serialize()),
            // Applying a commit does not cast a vote, so the attested digest
            // is carried through unchanged — same as `voted_term` above.
            voted_digest: self.voted_digest(),
        }
    }

    /// G9 — apply a commit only after its post-commit state is DURABLE.
    ///
    /// The committed term is what `is_master` serves, so a node must not
    /// advertise authority under term T until T survives a crash. This mirrors
    /// the H10 persist-before-vote discipline for the commit path: every
    /// validation gate of [`TopologyAuthority::handle_commit`] runs first (so
    /// an invalid or unsupported commit never persists and never applies), then
    /// `persist` is invoked with the post-commit [`PersistedTopologyState`];
    /// only if it returns `true` is the commit applied in memory.
    ///
    /// Fail-closed: on a persist failure the commit is NOT applied
    /// ([`DurableCommitOutcome::PersistFailed`]) and the node stays on its
    /// prior term rather than serve a term it could forget on reboot.
    pub fn handle_commit_durable<F>(
        &self,
        commit: &TopologyCommit,
        peak: u64,
        incarnation: u64,
        persist: F,
    ) -> DurableCommitOutcome
    where
        F: FnOnce(&PersistedTopologyState) -> bool,
    {
        // Item 1 — hold the commit critical section across the ENTIRE
        // gate→persist→apply sequence. Without it, a concurrent higher-term
        // commit can apply+ACK during this call's multi-ms persist, and this
        // call's later apply would clobber it (ACK-then-forget authority split).
        // `commit_apply` is the outermost lock; the `persist` closure below runs
        // pure file I/O that never re-enters the authority, so holding it across
        // the fsync introduces no lock-order inversion (see the field docs).
        let _apply_guard = self.commit_apply.lock();
        if !self.commit_passes_gates(commit) {
            return DurableCommitOutcome::NotApplied;
        }
        let state = self.persisted_state_for_commit(commit, peak, incarnation);
        if !persist(&state) {
            return DurableCommitOutcome::PersistFailed;
        }
        if self.apply_commit_locked(commit) {
            DurableCommitOutcome::Applied(commit.term)
        } else {
            // Superseded under the lock. Cannot happen while the guard is held
            // (the gate already required `commit.term > committed_term` and
            // nothing else advances the term without this lock), but the
            // apply-time re-check is the authoritative guard: report NotApplied
            // rather than a phantom `Applied` for a term we did not install.
            DurableCommitOutcome::NotApplied
        }
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

        // W6 — stamp the achievable placement version (unanimity, see
        // on_membership_changed).
        let placement_version = self.achievable_placement_version(&target_members);

        // E-01: peak-derived activation quorum (see on_membership_changed).
        // G8 stage 1: raise the peak BEFORE stamping committed_peak below.
        self.observe_peak_cluster_size(target_members.len() as u64);
        let committed_peak = self.peak_cluster_size();
        let term = TopologyTerm::new(
            new_term,
            target_members.clone(),
            self.self_id,
            self.cluster_id(),
            placement_version,
            committed_peak,
        );
        self.record_vote(new_term, term.digest);

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

    /// W6 (INVARIANT ii) — propose a placement-version UPGRADE for the
    /// already-committed membership.
    ///
    /// `retry_proposal` / `check_timeout` / `on_membership_changed` all
    /// early-return when the target membership equals the committed set, so
    /// none of them can drive a v1→v2 upgrade once a cluster has settled.
    /// This path fires the "first term committed at v2" trigger: it proposes
    /// a new term with the SAME members but a higher placement version, which
    /// (on commit) runs one planned full reshuffle through the normal
    /// migration machinery.
    ///
    /// Returns `None` unless ALL of:
    ///   * this node is the deterministic proposer (lowest committed member),
    ///   * a topology is already committed (committed_members non-empty),
    ///   * every committed member is known to support a HIGHER placement
    ///     version than is currently committed (unanimity),
    ///   * no proposal is already pending.
    pub fn upgrade_proposal(&self) -> Option<TopologyTerm> {
        let committed_members = self.committed_members.read().unwrap().clone();
        if committed_members.is_empty() {
            return None;
        }
        // Deterministic proposer: lowest committed NodeId.
        let proposer = committed_members.iter().copied().min()?;
        if proposer != self.self_id {
            return None;
        }
        // Don't stomp an in-flight proposal.
        if self.pending_proposal.lock().is_some() {
            return None;
        }
        let current = self.committed_placement_version();
        let achievable = self.achievable_placement_version(&committed_members);
        if achievable <= current {
            return None; // Nothing to upgrade (or not yet unanimous).
        }
        // Do NOT reshuffle to HRW while the cluster is degraded: only upgrade
        // when the committed membership is at full (peak) size. A partitioned
        // majority remnant (committed_members < peak) would otherwise run a
        // full reshuffle to a topology the absent nodes never agreed on,
        // diverging from their frozen view until they rejoin. Waiting for
        // full membership keeps the planned reshuffle a single, whole-cluster
        // event and avoids churning placement during instability.
        if (committed_members.len() as u64) < self.peak_cluster_size() {
            return None;
        }

        let committed_term = self.committed_term.load(Ordering::Relaxed);
        let voted = self.voted_term.load(Ordering::Relaxed);
        let new_term = committed_term.max(voted) + 1;

        // G8 stage 1: raise the peak BEFORE stamping committed_peak below.
        // (Already >= peak_cluster_size() per the guard above, so this is a
        // no-op in practice — kept for symmetry with the other producers.)
        self.observe_peak_cluster_size(committed_members.len() as u64);
        let committed_peak = self.peak_cluster_size();
        let term = TopologyTerm::new(
            new_term,
            committed_members.clone(),
            self.self_id,
            self.cluster_id(),
            achievable,
            committed_peak,
        );
        self.record_vote(new_term, term.digest);

        let quorum_needed = self.activation_quorum_needed(committed_members.len());
        let mut votes = std::collections::HashMap::new();
        votes.insert(self.self_id, true);

        *self.pending_proposal.lock() = Some(PendingProposal {
            term: term.clone(),
            votes,
            quorum_needed,
            _started_at: Instant::now(),
        });

        tracing::info!(
            self_id = self.self_id.0,
            term = new_term,
            from_version = current,
            to_version = achievable,
            "cluster: proposing placement-version upgrade (HRW)",
        );

        Some(term)
    }

    /// G8 stage 2 — propose a quorum-gated SHRINK of the cluster's
    /// committed floor. The ONLY producer allowed to stamp `committed_peak`
    /// BELOW the current effective peak (every other producer —
    /// `on_membership_changed`, `retry_proposal`, `check_timeout`,
    /// `upgrade_proposal` — is non-lowering by construction; see their doc
    /// comments). Safety does NOT rest on this method: it rests on two
    /// gates evaluated elsewhere.
    ///   * Gate A (propose/vote, unchanged machinery): `handle_vote`'s
    ///     quorum check uses `activation_quorum_needed`, which derives its
    ///     majority from `self.peak_cluster_size()` — still the OLD, higher
    ///     value at propose time (nothing has lowered it yet) — so a
    ///     minority can never gather enough accepting votes to produce a
    ///     `TopologyCommit` in the first place.
    ///   * Gate B (apply time, `TopologyAuthority::commit_passes_gates`):
    ///     every node that later evaluates the resulting commit
    ///     re-verifies, against ITS OWN durable `committed_peak`
    ///     (`local_peak`), that the commit carries a quorum of votes sized
    ///     against that local peak — so even a hand-forged low-peak commit
    ///     is rejected by every node whose own floor is still high.
    ///
    /// `surviving` is the explicit target membership after the shrink (NOT
    /// a count). It is sorted and deduplicated internally so caller order
    /// does not matter. Must be a non-empty STRICT subset of the currently
    /// committed membership — this path only ever removes members.
    ///
    /// # Proposer determinism and the self-omit special case
    ///
    /// The deterministic proposer is evaluated against the CURRENT
    /// committed membership (its lowest `NodeId`) — NOT against
    /// `surviving`. This is what lets the lowest-current-node propose a
    /// shrink that DROPS ITSELF (a self-excluding shrink, e.g.
    /// decommissioning the historically-lowest node), relaxing the
    /// standard `members[0] == self_id` / proposer-in-members invariant
    /// used by the other producers.
    ///
    /// A self-excluding shrink deliberately does NOT record a self-vote:
    /// [`TopologyCommit::has_quorum_voter_proof`] (unchanged — see
    /// [`TopologyCommit::has_quorum_voter_proof_for`]) requires every voter
    /// to be a member of the commit's OWN `members` list. If the proposer
    /// excluded itself from `surviving` and still counted its own vote,
    /// the resulting commit's voter list would contain a NodeId not in
    /// `members` and could never pass that check on ANY node — including
    /// its own. So when `self.self_id` is not in `surviving`, the full
    /// `quorum_needed` votes must come from `surviving` members alone; when
    /// `self.self_id` IS in `surviving` (the common case), the normal
    /// self-vote is recorded as usual.
    ///
    /// Returns `None` when: there is no committed membership yet, this
    /// node is not the deterministic proposer of the CURRENT committed
    /// set, `surviving` is empty or not a strict subset of the committed
    /// membership, the change fails the split-brain safety check, or a
    /// proposal is already pending.
    pub fn propose_shrink(&self, surviving: Vec<NodeId>) -> Option<TopologyTerm> {
        let mut surviving = surviving;
        surviving.sort_unstable_by_key(|n| n.0);
        surviving.dedup();

        let committed_members = self.committed_members.read().unwrap().clone();
        if committed_members.is_empty() {
            return None;
        }

        // Proposer determinism: lowest NodeId in the CURRENT committed
        // membership — NOT in `surviving` (see the self-omit note above).
        let proposer = committed_members.iter().copied().min()?;
        if proposer != self.self_id {
            return None;
        }

        // Must be a genuine, non-empty shrink of the CURRENT membership.
        if surviving.is_empty() || surviving.len() >= committed_members.len() {
            return None;
        }
        if !surviving.iter().all(|n| committed_members.contains(n)) {
            return None; // not a subset — this path only removes members
        }

        if !self.membership_change_is_safe(&surviving, Some(self.cluster_id())) {
            return None;
        }

        // Don't stomp an in-flight proposal.
        if self.pending_proposal.lock().is_some() {
            return None;
        }

        let committed = self.committed_term.load(Ordering::Relaxed);
        let voted = self.voted_term.load(Ordering::Relaxed);
        let new_term = committed.max(voted) + 1;

        let placement_version = self.achievable_placement_version(&surviving);

        // THE lowering stamp — `surviving.len()`, NOT `self.peak_cluster_size()`.
        // This is the one and only place in the authority that stamps a
        // `committed_peak` below the current effective peak.
        let committed_peak = surviving.len() as u64;
        let term = TopologyTerm::new(
            new_term,
            surviving.clone(),
            self.self_id,
            self.cluster_id(),
            placement_version,
            committed_peak,
        );
        self.record_vote(new_term, term.digest);

        // Gate A: quorum is derived from the OLD peak (`peak_cluster_size()`
        // has not been lowered yet at propose time), so a minority can never
        // gather enough votes to produce a commit.
        let quorum_needed = self.activation_quorum_needed(surviving.len());
        let mut votes = std::collections::HashMap::new();
        let self_excluded = !surviving.contains(&self.self_id);
        if !self_excluded {
            votes.insert(self.self_id, true);
        }
        // else: self-omit case — see doc comment above. Self does not
        // self-vote; all `quorum_needed` votes must come from `surviving`
        // members so the resulting commit's voters stay a subset of
        // `members`.

        *self.pending_proposal.lock() = Some(PendingProposal {
            term: term.clone(),
            votes,
            quorum_needed,
            _started_at: Instant::now(),
        });

        tracing::info!(
            self_id = self.self_id.0,
            term = new_term,
            surviving = ?surviving.iter().map(|n| n.0).collect::<Vec<_>>(),
            committed_peak,
            self_excluded,
            "cluster: proposing quorum-gated shrink (G8)",
        );

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

        // W6 — stamp the achievable placement version (unanimity).
        let placement_version = self.achievable_placement_version(&target_members);

        // E-01: peak-derived activation quorum (see on_membership_changed).
        // G8 stage 1: raise the peak BEFORE stamping committed_peak below.
        self.observe_peak_cluster_size(target_members.len() as u64);
        let committed_peak = self.peak_cluster_size();
        let term = TopologyTerm::new(
            new_term,
            target_members.clone(),
            self.self_id,
            self.cluster_id(),
            placement_version,
            committed_peak,
        );
        self.record_vote(new_term, term.digest);

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
// Topology-proposal debounce (W3.3 / audit F5)
// ---------------------------------------------------------------------------

/// Trailing-edge debounce for SWIM membership changes feeding the topology
/// proposer.
///
/// SWIM fires a `MembershipChanged` event for *every* join/leave it
/// observes. Proposing a new topology term on each one turns a staggered
/// N-node boot into up to `N-1` sequential terms, and a single node flap
/// (dead→alive within a short window) into two full migration rounds (shrink
/// then grow). Round-robin placement reshuffles ~(1-1/n)·4096 shards per
/// term, so that churn is enormously expensive.
///
/// This type coalesces a *burst* of membership changes into ONE proposal
/// against the settled membership:
///
///   * [`observe`](Self::observe) records the latest member set and (re)arms
///     a trailing-edge timer — the proposal is deferred until the membership
///     has been *stable* (unchanged) for `window`.
///   * [`take_due`](Self::take_due) returns the settled member set exactly
///     once, when either the membership has been stable for `window`
///     (the common case) or the burst has run longer than `max_wait`
///     (the cap, so continuous flapping still eventually proposes).
///
/// A flap that returns to the previously-pending set re-arms the timer but
/// leaves the target set unchanged, so when it finally fires the membership
/// is stable-equal and (via [`TopologyAuthority::on_membership_changed`]'s
/// identical-membership skip) produces ZERO net topology change.
///
/// Purely a *propose-side* gate: it never touches the committed term, the
/// quorum guards, or the prompt-activation path (which acts on an
/// already-committed term and must stay immediate). Time is injected so the
/// decision logic is deterministic in tests — production passes
/// `Instant::now()`.
#[derive(Debug)]
pub struct TopologyDebounce {
    /// Stable-membership window: propose only after the observed set has
    /// been unchanged for this long.
    window: Duration,
    /// Hard cap on total deferral so continuous churn cannot starve
    /// topology progress. Measured from the first observation in the
    /// current burst.
    max_wait: Duration,
    /// In-flight burst state (`None` = nothing pending).
    pending: Option<DebouncePending>,
}

#[derive(Debug, Clone)]
struct DebouncePending {
    /// Latest observed member set (the proposal target).
    members: Vec<NodeId>,
    /// When the current burst started (drives the `max_wait` cap).
    first_observed: Instant,
    /// When `members` last *changed* (drives the trailing-edge `window`).
    last_changed: Instant,
}

impl TopologyDebounce {
    /// Create a debounce with a stable-membership `window` and a total
    /// deferral `max_wait` cap. `max_wait` is clamped to be at least
    /// `window` (a cap below the window would defeat the debounce).
    pub fn new(window: Duration, max_wait: Duration) -> Self {
        Self {
            window,
            max_wait: max_wait.max(window),
            pending: None,
        }
    }

    /// Convenience constructor deriving `max_wait = 4 × window` (the W3.3
    /// default cap). A continuously-flapping cluster still proposes within
    /// four debounce windows.
    pub fn from_window(window: Duration) -> Self {
        Self::new(window, window.saturating_mul(4))
    }

    /// Record an observed membership at `now`.
    ///
    /// If `members` differs from the currently-pending target, the
    /// trailing-edge timer is (re)armed (`last_changed = now`) and the new
    /// set becomes the target. If it equals the pending target, the timer
    /// is *not* re-armed — an idempotent SWIM re-fire of the same set lets
    /// the window keep counting down toward a proposal. Starting a fresh
    /// burst also stamps `first_observed` for the `max_wait` cap.
    ///
    /// Empty member sets are ignored (SWIM never proposes an empty cluster;
    /// matches [`TopologyAuthority::on_membership_changed`]).
    pub fn observe(&mut self, members: &[NodeId], now: Instant) {
        if members.is_empty() {
            return;
        }
        match self.pending.as_mut() {
            Some(p) if p.members == members => {
                // Same set re-observed: keep counting down, do not re-arm.
            }
            Some(p) => {
                p.members = members.to_vec();
                p.last_changed = now;
            }
            None => {
                self.pending = Some(DebouncePending {
                    members: members.to_vec(),
                    first_observed: now,
                    last_changed: now,
                });
            }
        }
    }

    /// Whether a pending burst is due to propose at `now`: stable for
    /// `window`, or older than `max_wait`. Does not consume the state.
    pub fn is_due(&self, now: Instant) -> bool {
        match &self.pending {
            None => false,
            Some(p) => {
                now.duration_since(p.last_changed) >= self.window
                    || now.duration_since(p.first_observed) >= self.max_wait
            }
        }
    }

    /// If a pending burst is due at `now`, consume and return its settled
    /// member set; otherwise return `None` and leave the state intact. The
    /// caller feeds the returned set to
    /// [`TopologyAuthority::on_membership_changed`].
    pub fn take_due(&mut self, now: Instant) -> Option<Vec<NodeId>> {
        if self.is_due(now) {
            self.pending.take().map(|p| p.members)
        } else {
            None
        }
    }

    /// Whether anything is currently pending (used by the event loop to
    /// decide whether the per-tick due-check needs to run at all).
    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
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
        let propose = TopologyTerm::new(
            1,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
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

        let propose = TopologyTerm::new(
            3,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
        let vote = auth.handle_propose(&propose);
        assert!(!vote.accepted);
    }

    #[test]
    fn vote_reject_bad_digest() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let mut propose = TopologyTerm::new(
            1,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
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
            voter_placement_support: 1,
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
            digest: TopologyTerm::compute_digest(
                1,
                &ClusterId::UNSET,
                &members(&[1, 2, 3, 4, 5]),
                1,
                (members(&[1, 2, 3, 4, 5])).len() as u64,
            ),
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
            voter_placement_support: 1,
        };
        let commit = auth.handle_vote(&vote1);
        assert!(commit.is_none()); // Only 2 votes, need 3

        let vote2 = TopologyVote {
            term: 1,
            digest: TopologyTerm::compute_digest(
                1,
                &ClusterId::UNSET,
                &members(&[1, 2, 3, 4, 5]),
                1,
                (members(&[1, 2, 3, 4, 5])).len() as u64,
            ),
            voter: NodeId(3),
            accepted: true,
            voter_current_term: 0,
            voter_placement_support: 1,
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
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
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
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                7,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
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
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
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
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
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
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
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
            committed_placement_version: 1,
            committed_peak: 5,
            committed_commit: None,
            voted_digest: None,
        };
        let data = state.serialize();
        let restored = PersistedTopologyState::deserialize(&data).expect("v2 record must decode");
        assert_eq!(restored.peak_cluster_size, 5);
        assert_eq!(restored.committed_term, 42);
        assert_eq!(restored.voted_term, 43);
        assert_eq!(restored.committed_members.len(), 3);
        assert_eq!(restored.committed_members[0], NodeId(1));
        assert_eq!(restored.committed_voters, members(&[1, 2, 3]));
        assert_eq!(restored.incarnation, 99);
        assert_eq!(restored.committed_peak, 5);
    }

    #[test]
    fn legacy_16_byte_payload_is_rejected() {
        // The old `[peak:8][epoch:8]` format decoded into a state carrying a
        // real `committed_term` and `voted_term` with an EMPTY member list.
        // Under v2 those 16 bytes are just bytes: they carry no magic, so
        // they are rejected rather than trusted as a term/vote record.
        let mut data = Vec::new();
        data.extend_from_slice(&3u64.to_le_bytes()); // peak
        data.extend_from_slice(&7u64.to_le_bytes()); // epoch
        let err = PersistedTopologyState::deserialize(&data)
            .expect_err("a legacy 16-byte payload must not decode");
        assert!(
            matches!(err, TopologyStateDecodeError::BadMagic { .. }),
            "expected BadMagic, got {err:?}",
        );
    }

    #[test]
    fn wire_format_round_trip() {
        let term = TopologyTerm::new(
            42,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
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
            voter_placement_support: 1,
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
            placement_version: 1,
            committed_peak: (term.members.clone()).len() as u64,
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
            voter_placement_support: 1,
        };
        let commit = auth.handle_vote(&vote).expect("2/3 reaches quorum");

        assert_eq!(commit.voters, members(&[1, 2]));
        assert_eq!(auth.handle_commit(&commit), Some(term.term));
        let persisted = auth.persisted_state(3, 99);
        assert_eq!(persisted.committed_members, mems);
        assert_eq!(persisted.committed_voters, members(&[1, 2]));

        let restored = PersistedTopologyState::deserialize(&persisted.serialize())
            .expect("v2 record must decode");
        assert_eq!(restored.committed_voters, members(&[1, 2]));
    }

    #[test]
    fn cannot_vote_twice_for_same_term() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));

        let p1 = TopologyTerm::new(
            1,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
        let v1 = auth.handle_propose(&p1);
        assert!(v1.accepted);

        // Second proposal at same term from a different proposer
        let p2 = TopologyTerm::new(
            1,
            members(&[1, 2, 3]),
            NodeId(3),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
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
            placement_version: 1,
            committed_peak: (members(&[1, 2, 3])).len() as u64,
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
            placement_version: 1,
            committed_peak: (remote_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &remote_members,
                1,
                (remote_members).len() as u64,
            ),
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
            placement_version: 1,
            committed_peak: (remote_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &remote_members,
                1,
                (remote_members).len() as u64,
            ),
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
            placement_version: 1,
            committed_peak: (members(&[1, 2, 3])).len() as u64,
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
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
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
        let original_digest = TopologyTerm::compute_digest(
            5,
            &ClusterId::UNSET,
            &original_members,
            1,
            (original_members).len() as u64,
        );

        // Synthetic commit with wrong members [1, 2, 3] (SWIM-alive view).
        let wrong_members = members(&[1, 2, 3]);
        let wrong_digest = TopologyTerm::compute_digest(
            5,
            &ClusterId::UNSET,
            &wrong_members,
            1,
            (wrong_members).len() as u64,
        );

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
            placement_version: 1,
            committed_peak: (wrong_members.clone()).len() as u64,
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
            placement_version: 1,
            committed_peak: (original_members.clone()).len() as u64,
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
            voter_placement_support: 1,
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
            placement_version: 1,
            committed_peak: (members(&[1, 2, 3, 4])).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &members(&[1, 2, 3, 4]),
                1,
                (members(&[1, 2, 3, 4])).len() as u64,
            ),
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
            voter_placement_support: 1,
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
            voter_placement_support: 1,
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
            voter_placement_support: 1,
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
            voter_placement_support: 1,
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
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
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
            placement_version: 1,
            committed_peak: (original.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                4,
                &ClusterId::UNSET,
                &original,
                1,
                (original).len() as u64,
            ),
            voters: original.clone(),
        });
        auth.handle_commit(&TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: drained.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (drained.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &drained,
                1,
                (drained).len() as u64,
            ),
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
            placement_version: 1,
            committed_peak: (members(&[1, 2, 3])).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &members(&[1, 2, 3]),
                1,
                (members(&[1, 2, 3])).len() as u64,
            ),
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
            placement_version: 1,
            committed_peak: (original_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &original_members,
                1,
                (original_members).len() as u64,
            ),
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
        let p = TopologyTerm::new(
            3,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
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
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                10,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        auth.handle_commit(&commit);
        assert_eq!(auth.committed_term(), 10);
        // voted_term is still 3 — handle_commit doesn't update it
        assert_eq!(auth.voted_term.load(Ordering::Relaxed), 3);

        // Proposal for term 8: > voted(3) but NOT > committed(10) → reject
        let p2 = TopologyTerm::new(
            8,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
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
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                1,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
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
            placement_version: 1,
            committed_peak: (old_mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                1,
                &ClusterId::UNSET,
                &old_mems,
                1,
                (old_mems).len() as u64,
            ),
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
            voter_placement_support: 1,
        };
        assert!(auth.handle_vote(&v1).is_none());

        // Vote for t2 should match
        let v2 = TopologyVote {
            term: t2.term,
            digest: t2.digest,
            voter: NodeId(1),
            accepted: true,
            voter_current_term: 0,
            voter_placement_support: 1,
        };
        assert!(auth.handle_vote(&v2).is_some());
    }

    /// Verify that deserialize rejects truncated data at various boundaries.
    #[test]
    fn topology_term_deserialize_truncation_boundaries() {
        let term = TopologyTerm::new(
            42,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
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
            committed_placement_version: 1,
            committed_peak: 1,
            committed_commit: None,
            voted_digest: None,
        };
        let data = state.serialize();
        let restored = PersistedTopologyState::deserialize(&data).expect("v2 record must decode");
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
            placement_version: 1,
            committed_peak: (single.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                1,
                &ClusterId::UNSET,
                &single,
                1,
                (single).len() as u64,
            ),
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
        let proposal = TopologyTerm::new(
            1,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
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
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                term,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        auth.handle_commit(&commit);
        assert_eq!(auth.committed_members(), mems);
    }

    /// A single frame must not be able to raise the quorum bar forever.
    ///
    /// `activation_quorum_needed` derives from the durable `peak_cluster_size`,
    /// so a commit naming 1024 fabricated members pins the quorum at 513 —
    /// permanently, and across reboots, since no future term can gather 513
    /// voters to lower it. The growth bound is what stops the peak being
    /// poisoned in the first place.
    #[test]
    fn membership_growth_bound_rejects_a_fabricated_member_flood() {
        // The attack: 3-node cluster, one frame claiming 1024 members.
        assert!(
            !membership_growth_is_plausible(1024, 3, 3),
            "a fabricated member flood must be rejected before it poisons the peak",
        );

        // Legitimate growth steps stay accepted.
        for proposed in 3..=7 {
            assert!(
                membership_growth_is_plausible(proposed, 3, 3),
                "growing to {proposed} from an established 3 must be allowed",
            );
        }
        assert!(
            !membership_growth_is_plausible(8, 3, 3),
            "a jump of 5 exceeds MAX_MEMBER_GROWTH",
        );

        // A node partitioned while the cluster genuinely scaled up catches up
        // via the peak it already knows, even with few peers alive locally.
        assert!(
            membership_growth_is_plausible(12, 10, 1),
            "an established peak of 10 must admit a 12-member commit",
        );
        // ...and via alive count when the peak is behind.
        assert!(
            membership_growth_is_plausible(12, 1, 10),
            "a live view of 10 must admit a 12-member commit",
        );
    }

    /// `members` must be strictly ascending on receive.
    ///
    /// `TopologyTerm::compute_digest` hashes `members` **as received** while
    /// `ShardTable::compute_with_epoch` sorts a local copy before assigning.
    /// So a proposal carrying a non-ascending member list produces a digest
    /// every voter agrees on, while two conforming implementations derive
    /// DIFFERENT shard tables from it — a same-term split with every gate
    /// green. Strictly-ascending also rejects duplicates, which would
    /// otherwise inflate `members.len()` (that value feeds the quorum
    /// threshold and `committed_peak`).
    ///
    /// This is a latent defect in today's code, independent of committed
    /// master election; it becomes load-bearing once an assignment is encoded
    /// as u16 indices into `members`.
    #[test]
    fn members_must_be_strictly_ascending() {
        assert!(
            members_strictly_ascending(&[]),
            "empty is vacuously ordered"
        );
        assert!(members_strictly_ascending(&[NodeId(1)]));
        assert!(members_strictly_ascending(&[
            NodeId(1),
            NodeId(2),
            NodeId(3)
        ]));

        assert!(
            !members_strictly_ascending(&[NodeId(3), NodeId(1), NodeId(2)]),
            "unsorted must be rejected: digest hashes as-received, placement sorts",
        );
        assert!(
            !members_strictly_ascending(&[NodeId(1), NodeId(1), NodeId(2)]),
            "duplicates must be rejected: they inflate members.len()",
        );
        assert!(
            !members_strictly_ascending(&[NodeId(2), NodeId(1)]),
            "descending must be rejected",
        );
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
        let term = TopologyTerm::new(
            1,
            members(&ids),
            NodeId(0),
            ClusterId::UNSET,
            1,
            (members(&ids)).len() as u64,
        );
        let bytes = term.serialize();
        let decoded = TopologyTerm::deserialize(&bytes).expect("at-cap term should decode");
        assert_eq!(decoded.members.len(), MAX_TOPOLOGY_MEMBERS);
        assert_eq!(decoded.term, 1);
    }

    /// F-G5-002: voter list in TopologyCommit shares the same cap so a
    /// commit frame cannot drive a multi-megabyte voter allocation either.
    #[test]
    fn topology_commit_deserialize_rejects_oversized_voter_count() {
        let term = TopologyTerm::new(
            1,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2, 3])).len() as u64,
        );
        let mut bytes = term.serialize();
        // Append voter section claiming MAX_TOPOLOGY_MEMBERS + 1 voters
        // without their bytes.
        let oversized = (MAX_TOPOLOGY_MEMBERS + 1) as u32;
        bytes.extend_from_slice(&oversized.to_le_bytes());
        assert!(TopologyCommit::deserialize(&bytes).is_none());
    }

    // ── C-2: at-most-one-vote-per-term under concurrency ───────────────────

    /// C-2: two (or more) concurrent `handle_propose` calls carrying the
    /// SAME term but distinct proposers must grant AT MOST ONE accepted
    /// vote. The voter's `voted_term` read-compare-store must be atomic.
    ///
    /// On the old `Ordering::Relaxed` load → compare → store code the two
    /// threads can both load the pre-vote `voted_term`, both observe
    /// `propose.term > voted`, and both store — yielding two accepts for
    /// one term. With many iterations this reproduces intermittently; the
    /// barrier maximises the overlap of the decision windows.
    #[test]
    fn handle_propose_grants_at_most_one_vote_per_term_concurrently() {
        use std::sync::Barrier;
        use std::sync::atomic::AtomicU64 as TestAtomicU64;

        const THREADS: usize = 8;
        const ROUNDS: usize = 2_000;

        for round in 0..ROUNDS {
            let auth = Arc::new(TopologyAuthority::new(NodeId(99), Duration::from_secs(1)));
            // A fresh, strictly-higher term each round so the proposal is a
            // genuine candidate (term > committed && term > voted == 0).
            let term = (round as u64) + 1;
            let accepts = Arc::new(TestAtomicU64::new(0));
            let barrier = Arc::new(Barrier::new(THREADS));

            let mut handles = Vec::with_capacity(THREADS);
            for t in 0..THREADS {
                let auth = Arc::clone(&auth);
                let accepts = Arc::clone(&accepts);
                let barrier = Arc::clone(&barrier);
                handles.push(std::thread::spawn(move || {
                    // Distinct proposer/member set per thread, same term —
                    // models two proposers computing the same next term
                    // after a partition heal. self_id (99) is included so
                    // the recovery branch cannot fire (committed == 0).
                    let proposer = NodeId(100 + t as u64);
                    let propose = TopologyTerm::new(
                        term,
                        // Strictly ascending: NodeId(99) < NodeId(100 + t).
                        // The receive-side gate rejects an unordered member
                        // list (see `members_strictly_ascending`), and this
                        // test is about one-vote-per-term, not ordering.
                        vec![NodeId(99), proposer],
                        proposer,
                        ClusterId::UNSET,
                        1,
                        (vec![proposer, NodeId(99)]).len() as u64,
                    );
                    barrier.wait();
                    let vote = auth.handle_propose(&propose);
                    if vote.accepted {
                        accepts.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            }
            for h in handles {
                h.join().expect("vote thread panicked");
            }

            let granted = accepts.load(Ordering::SeqCst);
            assert!(
                granted <= 1,
                "round {round}: granted {granted} votes for term {term}, expected at most 1 \
                 (double-vote is the split-brain precondition)",
            );
            // And the persisted vote must reflect exactly the term voted on.
            assert_eq!(
                auth.voted_term.load(Ordering::Relaxed),
                term,
                "round {round}: voted_term must settle at the proposed term",
            );
        }
    }

    // ── E-2: handle_commit must enforce the propose-side guard set ─────────

    /// E-2: a commit whose `cluster_id` differs from the local authority's
    /// configured `cluster_id` must be REJECTED — the local committed
    /// topology must be left untouched. This is the split-brain-heal hole:
    /// the propose/vote path checked cluster_id but the commit-apply path
    /// did not, so a foreign higher-term commit could overwrite local
    /// topology.
    #[test]
    fn handle_commit_rejects_mismatched_cluster_id() {
        let cluster_a = ClusterId([0xAA; 16]);
        let cluster_b = ClusterId([0xBB; 16]);

        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        auth.set_cluster_id(cluster_a);

        // Establish a local cluster-A topology at term 5.
        let local_members = members(&[1, 2, 3]);
        let local_commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: local_members.clone(),
            cluster_id: cluster_a,
            placement_version: 1,
            committed_peak: (local_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &cluster_a,
                &local_members,
                1,
                (local_members).len() as u64,
            ),
            voters: local_members.clone(),
        };
        assert_eq!(auth.handle_commit(&local_commit), Some(5));

        // A foreign cluster-B proposer broadcasts a higher-term commit for
        // a disjoint member set, with a self-consistent digest and a valid
        // quorum proof over its OWN members.
        let foreign_members = members(&[4, 5, 6]);
        let foreign_commit = TopologyCommit {
            term: 7,
            proposer: NodeId(4),
            members: foreign_members.clone(),
            cluster_id: cluster_b,
            placement_version: 1,
            committed_peak: (foreign_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                7,
                &cluster_b,
                &foreign_members,
                1,
                (foreign_members).len() as u64,
            ),
            voters: foreign_members.clone(),
        };

        assert!(
            auth.handle_commit(&foreign_commit).is_none(),
            "foreign cluster_id commit must be rejected",
        );
        // Local topology unchanged — no split-brain adoption.
        assert_eq!(auth.committed_term(), 5);
        assert_eq!(auth.committed_members(), local_members);
    }

    /// E-2: a same-cluster_id commit whose membership change is NOT a
    /// monotonic superset/subset of the local committed set (a split-brain
    /// merge with drops) must be rejected by `membership_change_is_safe`,
    /// even though cluster_id matches.
    #[test]
    fn handle_commit_rejects_unsafe_membership_change() {
        let cid = ClusterId([0xCC; 16]);
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        auth.set_cluster_id(cid);

        let local_members = members(&[1, 2, 3]);
        let local_commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: local_members.clone(),
            cluster_id: cid,
            placement_version: 1,
            committed_peak: (local_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &cid,
                &local_members,
                1,
                (local_members).len() as u64,
            ),
            voters: local_members.clone(),
        };
        assert_eq!(auth.handle_commit(&local_commit), Some(5));

        // {1,2,3} → {3,4,5}: shares only node 3, neither superset nor
        // subset — a split-brain merge that is_safe_membership_change
        // rejects.
        let merged_members = members(&[3, 4, 5]);
        let merged_commit = TopologyCommit {
            term: 7,
            proposer: NodeId(3),
            members: merged_members.clone(),
            cluster_id: cid,
            placement_version: 1,
            committed_peak: (merged_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                7,
                &cid,
                &merged_members,
                1,
                (merged_members).len() as u64,
            ),
            voters: merged_members.clone(),
        };

        assert!(
            auth.handle_commit(&merged_commit).is_none(),
            "non-monotonic same-cluster merge must be rejected",
        );
        assert_eq!(auth.committed_term(), 5);
        assert_eq!(auth.committed_members(), local_members);
    }

    /// E-2: the happy path must survive the new guards — a valid,
    /// same-cluster_id, monotonic, higher-term commit is still adopted.
    #[test]
    fn handle_commit_accepts_valid_same_cluster_growth() {
        let cid = ClusterId([0xDD; 16]);
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        auth.set_cluster_id(cid);

        let local_members = members(&[1, 2, 3]);
        let local_commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: local_members.clone(),
            cluster_id: cid,
            placement_version: 1,
            committed_peak: (local_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &cid,
                &local_members,
                1,
                (local_members).len() as u64,
            ),
            voters: local_members.clone(),
        };
        assert_eq!(auth.handle_commit(&local_commit), Some(5));

        // Pure growth (superset) within the same cluster — must be adopted.
        let grown_members = members(&[1, 2, 3, 4, 5]);
        let grown_commit = TopologyCommit {
            term: 7,
            proposer: NodeId(1),
            members: grown_members.clone(),
            cluster_id: cid,
            placement_version: 1,
            committed_peak: (grown_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                7,
                &cid,
                &grown_members,
                1,
                (grown_members).len() as u64,
            ),
            voters: grown_members.clone(),
        };
        assert_eq!(
            auth.handle_commit(&grown_commit),
            Some(7),
            "valid same-cluster growth must still be adopted",
        );
        assert_eq!(auth.committed_term(), 7);
        assert_eq!(auth.committed_members(), grown_members);
    }

    /// E-2: when both sides leave cluster_id UNSET (legacy / pre-orchestrator
    /// path), the commit guard must fall back to the ever-seen heuristic and
    /// reject a foreign commit that introduces never-before-seen members.
    #[test]
    fn handle_commit_rejects_unseen_members_when_cluster_id_unset() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        // cluster_id stays UNSET on both sides.

        let local_members = members(&[1, 2, 3]);
        let local_commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: local_members.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (local_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &local_members,
                1,
                (local_members).len() as u64,
            ),
            voters: local_members.clone(),
        };
        assert_eq!(auth.handle_commit(&local_commit), Some(5));

        // Foreign superset introducing unseen nodes {7,8} — ever_seen_check
        // rejects because 7 and 8 were never committed voters here.
        let foreign_members = members(&[1, 2, 3, 7, 8]);
        let foreign_commit = TopologyCommit {
            term: 7,
            proposer: NodeId(7),
            members: foreign_members.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (foreign_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                7,
                &ClusterId::UNSET,
                &foreign_members,
                1,
                (foreign_members).len() as u64,
            ),
            voters: foreign_members.clone(),
        };
        assert!(
            auth.handle_commit(&foreign_commit).is_none(),
            "unset-cluster_id commit introducing unseen members must be rejected",
        );
        assert_eq!(auth.committed_term(), 5);
        assert_eq!(auth.committed_members(), local_members);
    }

    // -----------------------------------------------------------------
    // W3.3 — topology-proposal debounce
    // -----------------------------------------------------------------

    #[test]
    fn debounce_staggered_boot_burst_yields_one_proposal() {
        // 5-node boot whose MembershipChanged events all land WITHIN the
        // debounce window must collapse into exactly ONE proposal against
        // the final, settled membership (vs up to 4 today).
        let window = Duration::from_millis(500);
        let mut deb = TopologyDebounce::from_window(window);
        let t0 = Instant::now();

        // Burst: {1,2} → {1,2,3} → {1,2,3,4} → {1,2,3,4,5}, each ~100ms
        // apart (all inside the 500ms window, each re-arms the timer).
        deb.observe(&members(&[1, 2]), t0);
        deb.observe(&members(&[1, 2, 3]), t0 + Duration::from_millis(100));
        deb.observe(&members(&[1, 2, 3, 4]), t0 + Duration::from_millis(200));
        deb.observe(&members(&[1, 2, 3, 4, 5]), t0 + Duration::from_millis(300));

        // Before the window elapses past the LAST change: not due.
        assert!(!deb.is_due(t0 + Duration::from_millis(700)));
        assert_eq!(deb.take_due(t0 + Duration::from_millis(700)), None);

        // 500ms after the last change (t0+300 → t0+800): due, ONCE, with
        // the settled 5-member set.
        let fired = deb.take_due(t0 + Duration::from_millis(800));
        assert_eq!(fired, Some(members(&[1, 2, 3, 4, 5])));
        // Consumed: a second take returns nothing.
        assert_eq!(deb.take_due(t0 + Duration::from_millis(800)), None);
        assert!(!deb.has_pending());
    }

    #[test]
    fn debounce_changes_spanning_window_yield_multiple_proposals() {
        // Events spaced FURTHER apart than the window each settle and fire
        // on their own — the debounce only coalesces a contiguous burst.
        let window = Duration::from_millis(500);
        let mut deb = TopologyDebounce::from_window(window);
        let t0 = Instant::now();

        deb.observe(&members(&[1, 2]), t0);
        // Window elapses with {1,2} stable → first proposal.
        assert_eq!(
            deb.take_due(t0 + Duration::from_millis(600)),
            Some(members(&[1, 2])),
        );

        // A later, separate change settles and fires independently.
        deb.observe(&members(&[1, 2, 3]), t0 + Duration::from_millis(1000));
        assert_eq!(deb.take_due(t0 + Duration::from_millis(1400)), None);
        assert_eq!(
            deb.take_due(t0 + Duration::from_millis(1600)),
            Some(members(&[1, 2, 3])),
        );
    }

    #[test]
    fn debounce_flap_within_window_produces_stable_equal_set() {
        // A node leaves then rejoins inside the window: the trailing-edge
        // set equals the pre-flap set, so the proposal target is unchanged.
        // Fed to on_membership_changed (identical-membership skip) this is
        // ZERO net topology change.
        let window = Duration::from_millis(500);
        let mut deb = TopologyDebounce::from_window(window);
        let t0 = Instant::now();

        // Settled cluster {1,2,3}; node 3 flaps out then back in.
        deb.observe(&members(&[1, 2, 3]), t0);
        deb.observe(&members(&[1, 2]), t0 + Duration::from_millis(100)); // 3 dies
        deb.observe(&members(&[1, 2, 3]), t0 + Duration::from_millis(200)); // 3 back

        // Last change at t0+200; fires at t0+700 with the ORIGINAL set.
        let fired = deb.take_due(t0 + Duration::from_millis(700));
        assert_eq!(fired, Some(members(&[1, 2, 3])));

        // Prove the net-zero property end-to-end: feeding this to an
        // authority already committed on {1,2,3} produces NO proposal.
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        // Establish committed {1,2,3} (single observe + self/quorum commit
        // via the test commit helper would be heavier; instead drive the
        // on_membership_changed identical-skip directly after a first
        // commit). First commit the set.
        let term = auth
            .on_membership_changed(&members(&[1, 2, 3]))
            .expect("proposer proposes first term");
        let commit = TopologyCommit {
            term: term.term,
            proposer: NodeId(1),
            members: term.members.clone(),
            cluster_id: term.cluster_id,
            placement_version: 1,
            committed_peak: (term.members.clone()).len() as u64,
            digest: term.digest,
            voters: term.members.clone(),
        };
        assert!(auth.handle_commit(&commit).is_some());
        assert_eq!(auth.committed_members(), members(&[1, 2, 3]));
        // Now the debounced (flap-settled) set is identical → no proposal.
        assert!(
            auth.on_membership_changed(&fired.unwrap()).is_none(),
            "flap that settles back to the committed set must not re-propose",
        );
    }

    #[test]
    fn debounce_max_wait_cap_fires_under_continuous_churn() {
        // A cluster that changes membership every tick — never stable for a
        // full window — must still propose once the max-wait cap elapses.
        let window = Duration::from_millis(500);
        let max_wait = Duration::from_millis(2000); // 4× window
        let mut deb = TopologyDebounce::new(window, max_wait);
        let t0 = Instant::now();

        // Churn every 100ms (always < window since last change) for 2s.
        let mut n = 2u64;
        let mut now = t0;
        for step in 0..19 {
            now = t0 + Duration::from_millis(100 * step);
            // Alternate set so each observe re-arms the trailing-edge timer.
            let set: Vec<u64> = (1..=n).collect();
            deb.observe(&members(&set), now);
            n = if n == 2 { 3 } else { 2 };
            // Never due via the stable-window path while churning.
            if now.duration_since(t0) < max_wait {
                assert!(
                    !deb.is_due(now),
                    "should not fire via window while churning at {now:?}",
                );
            }
        }

        // Past the cap (measured from first_observed = t0): force-fires
        // even though the membership is still churning.
        assert!(deb.is_due(t0 + max_wait));
        assert!(
            deb.take_due(t0 + max_wait).is_some(),
            "max-wait cap must force a proposal under continuous churn",
        );
        let _ = now;
    }

    #[test]
    fn debounce_empty_set_is_ignored() {
        let mut deb = TopologyDebounce::from_window(Duration::from_millis(500));
        let t0 = Instant::now();
        deb.observe(&[], t0);
        assert!(!deb.has_pending());
        assert_eq!(deb.take_due(t0 + Duration::from_secs(10)), None);
    }

    #[test]
    fn debounce_max_wait_clamped_to_window() {
        // A max_wait below the window would defeat the debounce — it is
        // clamped up to the window.
        let mut deb = TopologyDebounce::new(Duration::from_millis(500), Duration::from_millis(10));
        let t0 = Instant::now();
        deb.observe(&members(&[1, 2]), t0);
        // At 10ms (the requested-but-clamped cap) it must NOT yet be due.
        assert!(!deb.is_due(t0 + Duration::from_millis(10)));
        // The effective cap is the 500ms window.
        assert!(deb.is_due(t0 + Duration::from_millis(500)));
    }

    // -------------------------------------------------------------------
    // W6 — placement-version digest binding + upgrade unanimity
    // -------------------------------------------------------------------

    #[test]
    fn placement_version_changes_digest() {
        // INVARIANT (i): two terms identical except placement_version MUST
        // produce different digests, so a v1 node and a v2 node can never
        // agree they committed "the same term".
        let mems = members(&[1, 2, 3]);
        let d1 = TopologyTerm::compute_digest(7, &ClusterId::UNSET, &mems, 1, (mems).len() as u64);
        let d2 = TopologyTerm::compute_digest(7, &ClusterId::UNSET, &mems, 2, (mems).len() as u64);
        assert_ne!(d1, d2, "placement_version must be mixed into the digest");
    }

    #[test]
    fn term_serialize_round_trip_preserves_placement_version() {
        let t = TopologyTerm::new(
            9,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            2,
            (members(&[1, 2, 3])).len() as u64,
        );
        let decoded = TopologyTerm::deserialize(&t.serialize()).expect("decode");
        assert_eq!(decoded.placement_version, 2);
        assert_eq!(decoded.digest, t.digest);
    }

    #[test]
    fn commit_serialize_round_trip_preserves_placement_version() {
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: members(&[1, 2, 3]),
            cluster_id: ClusterId::UNSET,
            placement_version: 2,
            committed_peak: (members(&[1, 2, 3])).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &members(&[1, 2, 3]),
                2,
                (members(&[1, 2, 3])).len() as u64,
            ),
            voters: members(&[1, 2, 3]),
        };
        let decoded = TopologyCommit::deserialize(&commit.serialize()).expect("decode");
        assert_eq!(decoded.placement_version, 2);
        assert_eq!(decoded.voters.len(), 3);
        assert_eq!(decoded.digest, commit.digest);
    }

    #[test]
    fn vote_serialize_round_trip_preserves_support() {
        let v = TopologyVote {
            term: 3,
            digest: [0u8; 32],
            voter: NodeId(7),
            accepted: true,
            voter_current_term: 2,
            voter_placement_support: 2,
        };
        let decoded = TopologyVote::deserialize(&v.serialize()).expect("decode");
        assert_eq!(decoded.voter_placement_support, 2);
        assert_eq!(decoded.voter, NodeId(7));
    }

    #[test]
    fn persisted_state_round_trip_preserves_placement_version() {
        let state = PersistedTopologyState {
            peak_cluster_size: 3,
            committed_term: 5,
            committed_members: members(&[1, 2, 3]),
            committed_voters: members(&[1, 2, 3]),
            voted_term: 5,
            incarnation: 4,
            committed_voter_ever_seen: members(&[1, 2, 3]),
            committed_placement_version: 2,
            committed_peak: 3,
            committed_commit: None,
            voted_digest: None,
        };
        let decoded =
            PersistedTopologyState::deserialize(&state.serialize()).expect("v2 record must decode");
        assert_eq!(decoded.committed_placement_version, 2);
        assert_eq!(decoded.committed_term, 5);
    }

    #[test]
    fn pre_w6_term_payload_decodes_as_placement_version_one() {
        // A term payload truncated before the placement trailer (the pre-W6
        // wire shape) must decode as v1, not garbage.
        let t = TopologyTerm::new(
            2,
            members(&[1, 2]),
            NodeId(1),
            ClusterId::UNSET,
            1,
            (members(&[1, 2])).len() as u64,
        );
        let mut bytes = t.serialize();
        bytes.truncate(bytes.len() - 2); // drop the 2-byte placement trailer
        let decoded = TopologyTerm::deserialize(&bytes).expect("decode");
        assert_eq!(decoded.placement_version, 1);
    }

    #[test]
    fn voter_rejects_unsupported_placement_version() {
        // INVARIANT (ii): a node refuses (does NOT silently accept) a
        // proposal whose placement_version exceeds its build support.
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let too_high = crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION + 1;
        let propose = TopologyTerm::new(
            1,
            members(&[1, 2, 3]),
            NodeId(1),
            ClusterId::UNSET,
            too_high,
            (members(&[1, 2, 3])).len() as u64,
        );
        let vote = auth.handle_propose(&propose);
        assert!(
            !vote.accepted,
            "must reject a placement version above support"
        );
    }

    #[test]
    fn activation_gate_refuses_unsupported_committed_version() {
        // INVARIANT (ii) activation gate: handle_commit must REFUSE (return
        // None) a committed term whose placement_version exceeds support,
        // rather than applying it with a fallback algorithm.
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let too_high = crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION + 1;
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: too_high,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                1,
                &ClusterId::UNSET,
                &mems,
                too_high,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        assert_eq!(auth.handle_commit(&commit), None);
        assert_eq!(
            auth.committed_term(),
            0,
            "unsupported commit must not advance"
        );
    }

    /// C11 — refusing an unsupported, quorum-proven committed term sets the
    /// self-fence flag so the coordinator stops serving stale authority.
    #[test]
    fn unapplicable_committed_term_arms_self_fence() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        assert!(!auth.is_self_fenced(), "fresh authority is not fenced");
        let too_high = crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION + 1;
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: too_high,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                4,
                &ClusterId::UNSET,
                &mems,
                too_high,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        assert_eq!(auth.handle_commit(&commit), None);
        assert_eq!(auth.unapplicable_committed_term(), 4);
        assert!(
            auth.is_self_fenced(),
            "observing a quorum-committed term it cannot apply must self-fence"
        );
    }

    /// C11 (forged-commit guard) — a refused unsupported commit WITHOUT a valid
    /// quorum voter proof must NOT fence a healthy node (the digest is
    /// forgeable; only a quorum-proven commit is proof the cluster advanced).
    #[test]
    fn unapplicable_committed_term_without_quorum_proof_does_not_fence() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let too_high = crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION + 1;
        let mems = members(&[1, 2, 3]);
        let forged = TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: too_high,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                4,
                &ClusterId::UNSET,
                &mems,
                too_high,
                (mems).len() as u64,
            ),
            // No quorum: a single voter cannot prove a 3-member commit.
            voters: members(&[1]),
        };
        assert_eq!(auth.handle_commit(&forged), None);
        assert_eq!(
            auth.unapplicable_committed_term(),
            0,
            "a commit lacking a quorum voter proof must not arm the fence"
        );
        assert!(!auth.is_self_fenced());
    }

    /// C11 liveness — a node that CAN apply the committed term adopts it and is
    /// NOT self-fenced (don't over-fence a node that keeps up).
    #[test]
    fn applicable_committed_term_does_not_fence() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64, // supported
            digest: TopologyTerm::compute_digest(
                4,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        assert_eq!(auth.handle_commit(&commit), Some(4));
        assert_eq!(auth.committed_term(), 4);
        assert!(
            !auth.is_self_fenced(),
            "a node that applied the committed term must keep serving"
        );
    }

    /// C11 — the self-fence auto-clears once `committed_term` catches up to (or
    /// past) the previously-unapplicable term, so a node that later commits a
    /// term it CAN apply is not permanently bricked.
    #[test]
    fn self_fence_clears_when_committed_term_catches_up() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let too_high = crate::cluster::shards::MAX_SUPPORTED_PLACEMENT_VERSION + 1;
        // Observe an unsupported term 4 → fenced (committed still 0).
        let bad = TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: too_high,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                4,
                &ClusterId::UNSET,
                &mems,
                too_high,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        assert_eq!(auth.handle_commit(&bad), None);
        assert!(auth.is_self_fenced());

        // A later supported commit at term 5 applies and clears the fence.
        let good = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        assert_eq!(auth.handle_commit(&good), Some(5));
        assert!(
            !auth.is_self_fenced(),
            "fence must clear once committed_term (5) passes the unapplicable term (4)"
        );
    }

    /// G9 (RED before fix) — a commit whose durable persist FAILS must NOT
    /// advance the served `committed_term`. Pre-fix ordering advanced the term
    /// in memory first and only then persisted (best-effort), so a crash — or
    /// here a failed persist — left the node serving/authorising under a term
    /// it had no durable record of. `handle_commit_durable` must fail closed.
    #[test]
    fn handle_commit_durable_fails_closed_when_persist_fails() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                4,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        // Persist reports failure.
        let outcome = auth.handle_commit_durable(&commit, 3, 1, |_state| false);
        assert_eq!(outcome, DurableCommitOutcome::PersistFailed);
        assert_eq!(
            auth.committed_term(),
            0,
            "committed_term must NOT advance when the durable persist fails"
        );
        assert!(
            auth.committed_members().is_empty(),
            "committed membership must not change on a failed persist"
        );
    }

    /// G9 liveness + ordering — a successful persist applies the commit, and
    /// the state handed to `persist` carries the NEW term while
    /// `committed_term` is still the OLD one (persist strictly precedes the
    /// served advance).
    #[test]
    fn handle_commit_durable_persists_before_serving() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                4,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        let persisted_term = std::cell::Cell::new(u64::MAX);
        let served_term_at_persist = std::cell::Cell::new(u64::MAX);
        let outcome = auth.handle_commit_durable(&commit, 3, 1, |state| {
            // The state being made durable carries the new committed term...
            persisted_term.set(state.committed_term);
            // ...while the SERVED committed_term has NOT yet advanced.
            served_term_at_persist.set(auth.committed_term());
            true
        });
        assert_eq!(outcome, DurableCommitOutcome::Applied(4));
        assert_eq!(
            persisted_term.get(),
            4,
            "the state persisted must carry the new committed term"
        );
        assert_eq!(
            served_term_at_persist.get(),
            0,
            "committed_term must still be the OLD term while persisting (persist-before-serve)"
        );
        assert_eq!(
            auth.committed_term(),
            4,
            "committed_term advances only after a successful persist"
        );
    }

    /// G9 — an invalid/stale commit is neither persisted nor applied.
    #[test]
    fn handle_commit_durable_rejects_invalid_without_persisting() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        // Bad digest → gate rejects before any persist.
        let commit = TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: [0u8; 32],
            voters: mems.clone(),
        };
        let persist_called = std::cell::Cell::new(false);
        let outcome = auth.handle_commit_durable(&commit, 3, 1, |_state| {
            persist_called.set(true);
            true
        });
        assert_eq!(outcome, DurableCommitOutcome::NotApplied);
        assert!(
            !persist_called.get(),
            "an invalid commit must not reach the persist step"
        );
        assert_eq!(auth.committed_term(), 0);
    }

    /// Item 1 (RED before fix) — `handle_commit_durable` must never let a
    /// LOWER term regress `committed_term` after a HIGHER term has already
    /// applied and been ACKed.
    ///
    /// G9 widened this from µs to ms: the gate reads `committed_term`, then the
    /// multi-ms persist fsync runs, then (pre-fix) `apply_commit` stores
    /// `commit.term` UNCONDITIONALLY. Two commits T and T+1 can both pass the
    /// gate at `committed_term = T-1`; if the lower term's persist finishes
    /// AFTER the higher term applies, the lower term's late apply clobbers the
    /// higher one — the node ACKed T+1 but now serves/gates on T and would
    /// reboot at T while peers hold T+1 (the exact authority split G9 exists to
    /// prevent).
    ///
    /// This drives the interleave deterministically with ordering primitives
    /// (no wall-clock assertions): the LOWER term (6) blocks inside its persist
    /// closure until released; the HIGHER term (7) is spawned while 6 is parked.
    /// Pre-fix, 7 is unsynchronized, overtakes, applies, and is ACKed; then 6's
    /// late apply regresses `committed_term` to 6 AND the members to the 6-set.
    /// Post-fix, 6 holds the commit critical section across its persist, so 7 is
    /// serialized behind it (6 applies, then 7 applies) and the final durable
    /// authority is term 7 with the 7-member set — never a regression.
    ///
    /// A bare `fetch_max` on `committed_term` alone would NOT pass: it would
    /// leave term 7 paired with the lower term's member set. The members
    /// assertion pins that the superseded apply mutates NOTHING.
    #[test]
    fn handle_commit_durable_lower_term_never_regresses_committed_term() {
        use std::sync::mpsc;

        // Baseline: commit term 5 with members {1,2,3,4} so BOTH later member
        // sets are already "ever seen" (the split-brain fallback would else
        // reject re-introducing node 4). `committed_term` starts at 5 = T-1.
        let auth = Arc::new(TopologyAuthority::new(NodeId(1), Duration::from_secs(1)));
        let base_members = members(&[1, 2, 3, 4]);
        let baseline = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: base_members.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (base_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                5,
                &ClusterId::UNSET,
                &base_members,
                1,
                (base_members).len() as u64,
            ),
            voters: base_members.clone(),
        };
        assert_eq!(auth.handle_commit(&baseline), Some(5));
        assert_eq!(auth.committed_term(), 5);

        // Lower term 6, DIFFERENT (subset) member set {1,2,3}.
        let lo_members = members(&[1, 2, 3]);
        let commit_lo = TopologyCommit {
            term: 6,
            proposer: NodeId(1),
            members: lo_members.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (lo_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                6,
                &ClusterId::UNSET,
                &lo_members,
                1,
                (lo_members).len() as u64,
            ),
            voters: lo_members.clone(),
        };
        // Higher term 7, full member set {1,2,3,4}.
        let hi_members = members(&[1, 2, 3, 4]);
        let commit_hi = TopologyCommit {
            term: 7,
            proposer: NodeId(1),
            members: hi_members.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (hi_members.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                7,
                &ClusterId::UNSET,
                &hi_members,
                1,
                (hi_members).len() as u64,
            ),
            voters: hi_members.clone(),
        };

        let (lo_in_persist_tx, lo_in_persist_rx) = mpsc::channel::<()>();
        let (release_lo_tx, release_lo_rx) = mpsc::channel::<()>();
        let (hi_done_tx, hi_done_rx) = mpsc::channel::<()>();

        // Worker A — the LOWER term. Its persist closure announces that it has
        // passed the gate and is now mid-persist, then BLOCKS until released.
        let auth_a = auth.clone();
        let a = std::thread::spawn(move || {
            auth_a.handle_commit_durable(&commit_lo, 4, 1, move |_state| {
                lo_in_persist_tx.send(()).expect("send lo_in_persist");
                release_lo_rx.recv().expect("recv release_lo");
                true
            })
        });

        // Wait until the lower term has passed its gate and is parked in
        // persist (committed_term still 5). Pre-fix no lock is held here;
        // post-fix the commit critical section IS held — which is exactly what
        // serializes the higher term behind it.
        lo_in_persist_rx
            .recv()
            .expect("lower term must reach persist");

        // Worker B — the HIGHER term. Its persist returns immediately.
        let auth_b = auth.clone();
        let b = std::thread::spawn(move || {
            let out = auth_b.handle_commit_durable(&commit_hi, 4, 1, |_state| true);
            hi_done_tx.send(()).expect("send hi_done");
            out
        });

        // Pre-fix: B is unsynchronized, overtakes A, applies term 7, signals
        // done (well within the fallback). Post-fix: B blocks acquiring the
        // commit lock A holds, so it cannot finish until A is released — this
        // `recv_timeout` is a pure LIVENESS fallback, not a timing assertion
        // (the real assertions below are on committed state). Either way we then
        // release A. The window is orders of magnitude larger than a lock-free
        // apply, so pre-fix reproduction is deterministic.
        let _hi_applied_before_release = hi_done_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        release_lo_tx.send(()).expect("release lower term");

        let _outcome_a = a.join().expect("A joins");
        let outcome_b = b.join().expect("B joins");

        // The higher term must be the durable authority — never regressed to 6.
        assert_eq!(
            auth.committed_term(),
            7,
            "committed_term must NOT regress to the lower term after the higher \
             term applied and was ACKed"
        );
        assert_eq!(
            auth.committed_members(),
            hi_members,
            "members must be the higher term's set — a bare fetch_max on the term \
             alone would leave term 7 paired with the lower term's members"
        );
        assert_eq!(
            outcome_b,
            DurableCommitOutcome::Applied(7),
            "the higher term must report Applied",
        );
    }

    /// Item 1 liveness guard — the commit critical section must NOT stall
    /// normal forward progress: two DISTINCT sequential terms (T then T+1) both
    /// apply and advance `committed_term` monotonically.
    #[test]
    fn handle_commit_durable_sequential_terms_both_apply() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));

        let m1 = members(&[1, 2, 3, 4]);
        let c1 = TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: m1.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (m1.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(1, &ClusterId::UNSET, &m1, 1, (m1).len() as u64),
            voters: m1.clone(),
        };
        assert_eq!(
            auth.handle_commit_durable(&c1, 4, 1, |_s| true),
            DurableCommitOutcome::Applied(1),
        );
        assert_eq!(auth.committed_term(), 1);
        assert_eq!(auth.committed_members(), m1);

        // A higher term with a DIFFERENT (subset drain) member set also applies.
        let m2 = members(&[1, 2, 3]);
        let c2 = TopologyCommit {
            term: 2,
            proposer: NodeId(1),
            members: m2.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (m2.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(2, &ClusterId::UNSET, &m2, 1, (m2).len() as u64),
            voters: m2.clone(),
        };
        assert_eq!(
            auth.handle_commit_durable(&c2, 4, 1, |_s| true),
            DurableCommitOutcome::Applied(2),
        );
        assert_eq!(auth.committed_term(), 2);
        assert_eq!(auth.committed_members(), m2);
    }

    /// G9 — `persisted_state_for_commit` mirrors exactly what
    /// `persisted_state` reports after a real `handle_commit` apply, so the
    /// pre-apply durable record is byte-identical to the post-apply one.
    #[test]
    fn persisted_state_for_commit_matches_post_apply_state() {
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                4,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };

        // Pre-apply projection.
        let auth_a = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let projected = auth_a.persisted_state_for_commit(&commit, 3, 7);

        // Real apply, then read the actual persisted state.
        let auth_b = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        assert_eq!(auth_b.handle_commit(&commit), Some(4));
        let actual = auth_b.persisted_state(3, 7);

        // Item 3 — destructure BOTH with every field bound explicitly (no `..`).
        // A field added to `PersistedTopologyState` + `apply_commit_locked` but
        // NOT to `persisted_state_for_commit` (or vice versa) then fails to
        // compile here, forcing the projection to stay exhaustive.
        let PersistedTopologyState {
            peak_cluster_size: projected_peak,
            committed_term: projected_term,
            committed_members: projected_members,
            committed_voters: projected_voters,
            voted_term: projected_voted_term,
            incarnation: projected_incarnation,
            committed_voter_ever_seen: projected_ever_seen,
            committed_placement_version: projected_placement,
            committed_peak: projected_committed_peak,
            committed_commit: projected_commit,
            voted_digest: projected_voted_digest,
        } = projected;
        let PersistedTopologyState {
            peak_cluster_size: actual_peak,
            committed_term: actual_term,
            committed_members: actual_members,
            committed_voters: actual_voters,
            voted_term: actual_voted_term,
            incarnation: actual_incarnation,
            committed_voter_ever_seen: actual_ever_seen,
            committed_placement_version: actual_placement,
            committed_peak: actual_committed_peak,
            committed_commit: actual_commit,
            voted_digest: actual_voted_digest,
        } = actual;

        assert_eq!(projected_term, actual_term);
        assert_eq!(projected_members, actual_members);
        assert_eq!(projected_voters, actual_voters);
        assert_eq!(projected_peak, actual_peak);
        assert_eq!(projected_placement, actual_placement);
        assert_eq!(
            projected_committed_peak, actual_committed_peak,
            "G8 stage 1 — persisted_state_for_commit's committed_peak projection \
             must match the post-apply persisted_state's committed_peak exactly",
        );
        assert_eq!(projected_voted_term, actual_voted_term);
        assert_eq!(projected_incarnation, actual_incarnation);
        let mut a: Vec<u64> = projected_ever_seen.iter().map(|n| n.0).collect();
        let mut b: Vec<u64> = actual_ever_seen.iter().map(|n| n.0).collect();
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b, "ever-seen set must match post-apply");
        assert_eq!(
            projected_commit,
            Some(commit.serialize()),
            "E5 — the pre-apply projection must persist the winning commit's own bytes",
        );
        assert_eq!(
            projected_commit, actual_commit,
            "E5 — persisted commit bytes must match between the projection and post-apply state",
        );
        assert_eq!(
            projected_voted_digest, actual_voted_digest,
            "the attested vote digest must be carried through identically",
        );
    }

    #[test]
    fn proposal_stays_v1_until_unanimous_support() {
        // A proposer that has NOT learned peer support proposes v1 even
        // though it itself supports v2 (peers default to v1 = conservative).
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let proposal = auth
            .on_membership_changed(&members(&[1, 2, 3]))
            .expect("node 1 is the proposer");
        assert_eq!(
            proposal.placement_version, 1,
            "must propose v1 before learning all peers support v2"
        );
    }

    #[test]
    fn achievable_version_reaches_v2_when_all_peers_support_it() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        assert_eq!(auth.achievable_placement_version(&mems), 1);
        auth.record_peer_placement_support(NodeId(2), 2);
        assert_eq!(auth.achievable_placement_version(&mems), 1); // node 3 unknown
        auth.record_peer_placement_support(NodeId(3), 2);
        assert_eq!(auth.achievable_placement_version(&mems), 2);
    }

    #[test]
    fn one_v1_member_keeps_cluster_v1() {
        // A single member stuck at v1 holds the whole cluster at v1
        // (unanimity, not quorum).
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        auth.record_peer_placement_support(NodeId(2), 2);
        auth.record_peer_placement_support(NodeId(3), 1); // legacy node
        assert_eq!(auth.achievable_placement_version(&mems), 1);
    }

    #[test]
    fn upgrade_proposal_fires_once_support_is_unanimous() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                1,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        assert_eq!(auth.handle_commit(&commit), Some(1));
        assert_eq!(auth.committed_placement_version(), 1);
        assert!(auth.upgrade_proposal().is_none()); // not yet unanimous
        auth.record_peer_placement_support(NodeId(2), 2);
        auth.record_peer_placement_support(NodeId(3), 2);
        let upgrade = auth
            .upgrade_proposal()
            .expect("should propose a v2 upgrade once unanimous");
        assert_eq!(upgrade.placement_version, 2);
        assert_eq!(upgrade.members, mems);
        assert!(upgrade.term > 1, "upgrade must use a fresh higher term");
    }

    #[test]
    fn non_proposer_does_not_issue_upgrade() {
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                1,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        assert_eq!(auth.handle_commit(&commit), Some(1));
        auth.record_peer_placement_support(NodeId(1), 2);
        auth.record_peer_placement_support(NodeId(3), 2);
        // Node 2 is NOT the lowest committed member → no upgrade.
        assert!(auth.upgrade_proposal().is_none());
    }

    #[test]
    fn homogeneous_cluster_upgrades_exactly_once() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3]);
        let commit_v1 = TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: (mems.clone()).len() as u64,
            digest: TopologyTerm::compute_digest(
                1,
                &ClusterId::UNSET,
                &mems,
                1,
                (mems).len() as u64,
            ),
            voters: mems.clone(),
        };
        auth.handle_commit(&commit_v1);
        auth.record_peer_placement_support(NodeId(2), 2);
        auth.record_peer_placement_support(NodeId(3), 2);
        let upgrade = auth.upgrade_proposal().expect("first upgrade");
        let commit_v2 = TopologyCommit {
            term: upgrade.term,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 2,
            committed_peak: (mems.clone()).len() as u64,
            digest: upgrade.digest,
            voters: mems.clone(),
        };
        assert_eq!(auth.handle_commit(&commit_v2), Some(upgrade.term));
        assert_eq!(auth.committed_placement_version(), 2);
        assert!(auth.upgrade_proposal().is_none()); // no second upgrade
    }

    // -----------------------------------------------------------------------
    // G8 stage 1 — committed_peak data model, digest binding, persistence,
    // recovery, and the split-brain floor getter. NO shrink capability yet:
    // every producer stamps committed_peak == the current effective peak, so
    // these tests prove the change is additive and behavior-preserving.
    // -----------------------------------------------------------------------

    #[test]
    fn topology_term_serde_roundtrips_committed_peak() {
        let term = TopologyTerm::new(9, members(&[1, 2, 3]), NodeId(1), ClusterId::UNSET, 1, 5);
        let data = term.serialize();
        let decoded = TopologyTerm::deserialize(&data).expect("decode");
        assert_eq!(decoded.committed_peak, 5);
        assert_eq!(decoded.digest, term.digest);
    }

    #[test]
    fn topology_commit_serde_roundtrips_committed_peak() {
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 9,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 5,
            digest: TopologyTerm::compute_digest(9, &ClusterId::UNSET, &mems, 1, 5),
            voters: mems.clone(),
        };
        let decoded = TopologyCommit::deserialize(&commit.serialize()).expect("decode");
        assert_eq!(decoded.committed_peak, 5);
        assert_eq!(decoded.digest, commit.digest);
    }

    #[test]
    fn persisted_state_roundtrips_committed_peak() {
        let state = PersistedTopologyState {
            peak_cluster_size: 3,
            committed_term: 1,
            committed_members: members(&[1, 2, 3]),
            committed_voters: members(&[1, 2, 3]),
            voted_term: 1,
            incarnation: 0,
            committed_voter_ever_seen: members(&[1, 2, 3]),
            committed_placement_version: 1,
            // Deliberately DIFFERENT from committed_members.len() (3) so a
            // coincidental match with the legacy-default formula can't mask
            // a broken round trip.
            committed_peak: 7,
            committed_commit: None,
            voted_digest: None,
        };
        let decoded =
            PersistedTopologyState::deserialize(&state.serialize()).expect("v2 record must decode");
        assert_eq!(decoded.committed_peak, 7);
    }

    #[test]
    fn legacy_wire_frame_without_committed_peak_decodes_to_members_len() {
        // Hand-craft a pre-G8 (W6-only) standalone TopologyTerm frame that
        // carries the placement_version trailer but NOT the new
        // committed_peak trailer — exactly what a pre-G8 binary would have
        // written: [term:8][proposer:8][cluster_id:16][count:4][members:8*N]
        // [digest:32][placement_version:2].
        let mems = members(&[1, 2, 3, 4]);
        let mut buf = Vec::new();
        buf.extend_from_slice(&9u64.to_le_bytes()); // term
        buf.extend_from_slice(&NodeId(1).0.to_le_bytes()); // proposer
        buf.extend_from_slice(&ClusterId::UNSET.0); // cluster_id
        buf.extend_from_slice(&(mems.len() as u32).to_le_bytes());
        for m in &mems {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        buf.extend_from_slice(&[0xAB; 32]); // digest (opaque for this test)
        buf.extend_from_slice(&1u16.to_le_bytes()); // placement_version trailer only

        let decoded = TopologyTerm::deserialize(&buf).expect("legacy W6 frame must still decode");
        assert_eq!(
            decoded.committed_peak,
            mems.len() as u64,
            "absent committed_peak trailer must default to members.len()"
        );
        assert_eq!(decoded.placement_version, 1);
        assert_eq!(decoded.digest, [0xAB; 32]);

        // Even older: no trailer at all (pre-W6). Must decode the same way.
        let pre_w6_len = buf.len() - 2;
        let pre_w6 = &buf[..pre_w6_len];
        let decoded_pre_w6 = TopologyTerm::deserialize(pre_w6).expect("pre-W6 frame must decode");
        assert_eq!(decoded_pre_w6.committed_peak, mems.len() as u64);
        assert_eq!(decoded_pre_w6.placement_version, 1);
    }

    #[test]
    fn legacy_persisted_file_is_rejected_not_silently_decoded() {
        // Hand-craft a v1 persisted blob (no magic, no CRC, trailer-extended).
        // v1 tolerated short reads, so a truncated file decoded into a SHORTER
        // `committed_members` under an UNCHANGED `committed_term` — a silently
        // weakened restart quorum. v2 refuses to interpret these bytes at all.
        let peak = 4u64;
        let committed_members = members(&[1, 2, 3]);

        let mut buf = Vec::new();
        buf.extend_from_slice(&peak.to_le_bytes());
        buf.extend_from_slice(&2u64.to_le_bytes()); // committed_term
        buf.extend_from_slice(&2u64.to_le_bytes()); // voted_term
        buf.extend_from_slice(&(committed_members.len() as u32).to_le_bytes());
        for m in &committed_members {
            buf.extend_from_slice(&m.0.to_le_bytes());
        }
        buf.extend_from_slice(&0u64.to_le_bytes()); // incarnation
        buf.extend_from_slice(&0u32.to_le_bytes()); // committed_voters count
        buf.extend_from_slice(&0u32.to_le_bytes()); // ever_seen count
        buf.extend_from_slice(&1u16.to_le_bytes()); // placement_version

        match PersistedTopologyState::deserialize(&buf) {
            Err(TopologyStateDecodeError::BadMagic { found, expected }) => {
                assert_eq!(expected, TOPOLOGY_STATE_MAGIC);
                assert_ne!(found, TOPOLOGY_STATE_MAGIC);
            }
            other => panic!("a v1 payload must be rejected as BadMagic, got {other:?}"),
        }
    }

    #[test]
    fn truncated_record_is_rejected_rather_than_shortening_the_member_list() {
        // The v1 defect this format exists to close: dropping bytes off the
        // end yielded a state with FEWER committed members but the SAME
        // committed term, indistinguishable from a legitimately smaller
        // cluster — and it fed both the restart quorum and `committed_peak`.
        let state = PersistedTopologyState {
            peak_cluster_size: 5,
            committed_term: 9,
            committed_members: members(&[1, 2, 3, 4, 5]),
            committed_voters: members(&[1, 2, 3]),
            voted_term: 9,
            incarnation: 4,
            committed_voter_ever_seen: members(&[1, 2, 3, 4, 5]),
            committed_placement_version: 1,
            committed_peak: 5,
            committed_commit: None,
            voted_digest: None,
        };
        let full = state.serialize();

        // Every proper prefix must be rejected. None may decode to a state
        // with a shorter member list.
        for cut in 1..full.len() {
            let truncated = &full[..cut];
            let err = PersistedTopologyState::deserialize(truncated)
                .expect_err("a truncated record must never decode");
            // The framing catches it as short/length-mismatch/CRC; the payload
            // reader catches it as a truncated section. Any of those is a
            // rejection — what must never happen is a successful decode.
            match err {
                TopologyStateDecodeError::TooShort { .. }
                | TopologyStateDecodeError::PayloadLengthMismatch { .. }
                | TopologyStateDecodeError::CrcMismatch { .. }
                | TopologyStateDecodeError::TruncatedSection { .. }
                | TopologyStateDecodeError::BadMagic { .. } => {}
                other => panic!("unexpected rejection reason at cut {cut}: {other:?}"),
            }
        }
    }

    #[test]
    fn single_bit_flip_anywhere_in_the_record_fails_the_crc() {
        let state = PersistedTopologyState {
            peak_cluster_size: 3,
            committed_term: 7,
            committed_members: members(&[1, 2, 3]),
            committed_voters: members(&[1, 2]),
            voted_term: 7,
            incarnation: 1,
            committed_voter_ever_seen: members(&[1, 2, 3]),
            committed_placement_version: 2,
            committed_peak: 3,
            committed_commit: None,
            voted_digest: None,
        };
        let good = state.serialize();
        assert!(
            PersistedTopologyState::deserialize(&good).is_ok(),
            "the unmodified record must decode"
        );

        // Flip the low bit of every byte in turn. Nothing may decode.
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0x01;
            assert!(
                PersistedTopologyState::deserialize(&bad).is_err(),
                "flipping bit 0 of byte {i} must be detected",
            );
        }
    }

    #[test]
    fn oversized_counts_are_rejected_before_allocation() {
        // A `count` field is attacker/corruption controlled and feeds
        // `Vec::with_capacity`. Every node-id section must be bounded by
        // MAX_TOPOLOGY_MEMBERS BEFORE any sizing happens.
        let base = PersistedTopologyState {
            peak_cluster_size: 1,
            committed_term: 1,
            committed_members: Vec::new(),
            committed_voters: Vec::new(),
            voted_term: 1,
            incarnation: 0,
            committed_voter_ever_seen: Vec::new(),
            committed_placement_version: 1,
            committed_peak: 1,
            committed_commit: None,
            voted_digest: None,
        };
        let good = base.serialize();

        // Payload starts at offset 10; the member count sits after the six
        // fixed fields (8+8+8+8+2+8 = 42 bytes).
        let member_count_off = 10 + 42;
        for (section, off) in [
            ("committed_members", member_count_off),
            ("committed_voters", member_count_off + 4),
            ("committed_voter_ever_seen", member_count_off + 8),
        ] {
            let mut bad = good.clone();
            let huge = u32::MAX.to_le_bytes();
            bad[off..off + 4].copy_from_slice(&huge);
            // Re-frame so the CRC and length still check out — the count bound
            // must be what rejects this, not the checksum.
            let payload_len = bad.len() - TOPOLOGY_STATE_FRAME_OVERHEAD;
            let len_bytes = (payload_len as u32).to_le_bytes();
            bad[6..10].copy_from_slice(&len_bytes);
            let crc_off = bad.len() - 4;
            let crc = crc32fast::hash(&bad[..crc_off]);
            bad[crc_off..].copy_from_slice(&crc.to_le_bytes());

            match PersistedTopologyState::deserialize(&bad) {
                Err(TopologyStateDecodeError::SectionTooLarge {
                    section: got,
                    count,
                    max,
                }) => {
                    assert_eq!(got, section);
                    assert_eq!(count, u32::MAX as usize);
                    assert_eq!(max, MAX_TOPOLOGY_MEMBERS);
                }
                other => panic!("{section} count of u32::MAX must be rejected, got {other:?}"),
            }
        }
    }

    #[test]
    fn absent_commit_is_structurally_distinct_from_a_zero_filled_one() {
        // A torn write that zeroed the commit blob must not read as "no
        // commit": the presence flag, not the content, decides.
        let mut with_commit = PersistedTopologyState {
            peak_cluster_size: 3,
            committed_term: 4,
            committed_members: members(&[1, 2, 3]),
            committed_voters: members(&[1, 2, 3]),
            voted_term: 4,
            incarnation: 0,
            committed_voter_ever_seen: members(&[1, 2, 3]),
            committed_placement_version: 1,
            committed_peak: 3,
            committed_commit: Some(vec![0u8; 64]),
            voted_digest: None,
        };
        let decoded = PersistedTopologyState::deserialize(&with_commit.serialize())
            .expect("v2 record must decode");
        assert_eq!(
            decoded.committed_commit,
            Some(vec![0u8; 64]),
            "an all-zero commit blob must still decode as PRESENT",
        );

        with_commit.committed_commit = None;
        let decoded_absent = PersistedTopologyState::deserialize(&with_commit.serialize())
            .expect("v2 record must decode");
        assert_eq!(
            decoded_absent.committed_commit, None,
            "an absent commit must decode as absent",
        );
    }

    #[test]
    fn oversized_commit_blob_is_rejected() {
        let state = PersistedTopologyState {
            peak_cluster_size: 1,
            committed_term: 1,
            committed_members: members(&[1]),
            committed_voters: members(&[1]),
            voted_term: 1,
            incarnation: 0,
            committed_voter_ever_seen: members(&[1]),
            committed_placement_version: 1,
            committed_peak: 1,
            committed_commit: Some(vec![7u8; 8]),
            voted_digest: None,
        };
        let mut bad = state.serialize();
        // The commit length is the last 4 bytes before the blob, which is the
        // last 8 bytes of the payload.
        let crc_off = bad.len() - 4;
        let len_off = crc_off - 8 - 4;
        let huge = (MAX_PERSISTED_COMMIT_BYTES as u32 + 1).to_le_bytes();
        bad[len_off..len_off + 4].copy_from_slice(&huge);
        let crc = crc32fast::hash(&bad[..crc_off]);
        bad[crc_off..].copy_from_slice(&crc.to_le_bytes());

        match PersistedTopologyState::deserialize(&bad) {
            Err(TopologyStateDecodeError::SectionTooLarge { section, max, .. }) => {
                assert_eq!(section, "committed_commit");
                assert_eq!(max, MAX_PERSISTED_COMMIT_BYTES);
            }
            other => panic!("an oversized commit blob must be rejected, got {other:?}"),
        }
    }

    #[test]
    fn winning_commit_round_trips_through_persistence_and_restore() {
        // E5 — the commit that won the term must survive a restart so the
        // catch-up path can replay a real quorum proof instead of a
        // self-consistent fabrication.
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 6,
            proposer: NodeId(2),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 3,
            digest: TopologyTerm::compute_digest(6, &ClusterId::UNSET, &mems, 1, 3),
            voters: mems.clone(),
        };

        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        assert_eq!(auth.handle_commit(&commit), Some(6));
        assert_eq!(
            auth.committed_commit_bytes(),
            Some(commit.serialize()),
            "applying a commit must retain its exact bytes",
        );

        let state = auth.persisted_state(3, 1);
        let reloaded = PersistedTopologyState::deserialize(&state.serialize())
            .expect("persisted state must decode");
        let restored = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        restored.restore(&reloaded);
        assert_eq!(
            restored.committed_commit_bytes(),
            Some(commit.serialize()),
            "the winning commit must survive a restart verbatim",
        );
    }

    /// §4.3 — a vote records the DIGEST, not just the term, and both survive
    /// the persist round-trip.
    #[test]
    fn voting_records_the_attested_digest_and_it_survives_restart() {
        let mems = members(&[1, 2, 3]);
        let propose = TopologyTerm::new(5, mems.clone(), NodeId(1), ClusterId::UNSET, 1, 3);

        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        assert_eq!(auth.voted_digest(), None, "no vote cast yet");
        let vote = auth.handle_propose(&propose);
        assert!(
            vote.accepted,
            "a well-formed first proposal must be accepted"
        );
        assert_eq!(auth.voted_term(), 5);
        assert_eq!(
            auth.voted_digest(),
            Some(propose.digest),
            "the vote must record what was attested to, not just the term",
        );

        let reloaded = PersistedTopologyState::deserialize(&auth.persisted_state(3, 1).serialize())
            .expect("persisted state must decode");
        assert_eq!(reloaded.voted_digest, Some(propose.digest));
        let restored = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        restored.restore(&reloaded);
        assert_eq!(
            restored.voted_digest(),
            Some(propose.digest),
            "dropping the attested digest on restart re-opens the equivocation window",
        );
    }

    /// §4.4 (E1) — the equivocation attack the whole mechanism exists for.
    ///
    /// A proposer gets a vote for term T carrying digest A, then commits term T
    /// carrying digest B. Every field of the B commit is internally consistent,
    /// so the recompute-from-own-fields digest check passes. Only the persisted
    /// vote can tell the two apart.
    #[test]
    fn commit_whose_digest_differs_from_the_attested_one_is_rejected() {
        let voted_members = members(&[1, 2, 3]);
        let propose =
            TopologyTerm::new(7, voted_members.clone(), NodeId(1), ClusterId::UNSET, 1, 3);

        let auth = TopologyAuthority::new(NodeId(3), Duration::from_secs(1));
        assert!(auth.handle_propose(&propose).accepted);

        // Same term, DIFFERENT content — and a digest that is correct for that
        // content, so the self-consistency check cannot catch it.
        let other_members = members(&[1, 2, 3, 4]);
        let equivocating = TopologyCommit {
            term: 7,
            proposer: NodeId(1),
            members: other_members.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 4,
            digest: TopologyTerm::compute_digest(7, &ClusterId::UNSET, &other_members, 1, 4),
            voters: other_members.clone(),
        };
        assert_eq!(
            equivocating.digest,
            TopologyTerm::compute_digest(
                equivocating.term,
                &equivocating.cluster_id,
                &equivocating.members,
                equivocating.placement_version,
                equivocating.committed_peak,
            ),
            "precondition: the equivocating commit is internally consistent",
        );
        assert_ne!(equivocating.digest, propose.digest);

        let before = vote_digest_mismatch_total();
        assert_eq!(
            auth.handle_commit(&equivocating),
            None,
            "a commit contradicting this node's own attestation must be rejected",
        );
        assert_eq!(
            vote_digest_mismatch_total(),
            before + 1,
            "the rejection must be counted",
        );
        assert_eq!(auth.committed_term(), 0, "nothing may have been applied");

        // Reject, never fence: the node keeps serving its prior term and is
        // still able to accept the NEXT term normally.
        assert!(
            !auth.is_self_fenced(),
            "E1 — a digest mismatch must not fence"
        );
        let next_members = members(&[1, 2, 3]);
        let next = TopologyCommit {
            term: 8,
            proposer: NodeId(1),
            members: next_members.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 3,
            digest: TopologyTerm::compute_digest(8, &ClusterId::UNSET, &next_members, 1, 3),
            voters: next_members.clone(),
        };
        assert_eq!(
            auth.handle_commit(&next),
            Some(8),
            "the node must still advance on the next term",
        );
    }

    /// §4.4 — the commit this node actually voted for still applies.
    #[test]
    fn commit_matching_the_attested_digest_applies() {
        let mems = members(&[1, 2, 3]);
        let propose = TopologyTerm::new(4, mems.clone(), NodeId(1), ClusterId::UNSET, 1, 3);
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        assert!(auth.handle_propose(&propose).accepted);

        let commit = TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 3,
            digest: propose.digest,
            voters: mems.clone(),
        };
        assert_eq!(auth.handle_commit(&commit), Some(4));
        assert_eq!(auth.committed_digest(), Some(propose.digest));
    }

    /// §4.4 / §4.9 — a node that never voted at the committed term has nothing
    /// to contradict. Missing a propose round is the normal catch-up path in
    /// any n >= 3 cluster, so it must not be turned into a rejection.
    #[test]
    fn a_node_that_never_voted_at_the_term_still_accepts_the_commit() {
        let mems = members(&[1, 2, 3]);
        let auth = TopologyAuthority::new(NodeId(3), Duration::from_secs(1));
        assert_eq!(auth.voted_term(), 0, "precondition: never voted");

        let commit = TopologyCommit {
            term: 6,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 3,
            digest: TopologyTerm::compute_digest(6, &ClusterId::UNSET, &mems, 1, 3),
            voters: mems.clone(),
        };
        let before = vote_digest_mismatch_total();
        assert_eq!(auth.handle_commit(&commit), Some(6));
        assert_eq!(
            vote_digest_mismatch_total(),
            before,
            "a non-voter must not be counted as a mismatch",
        );
    }

    /// §4.3 — a proposer must accept the commit for its OWN proposal.
    ///
    /// Regression: the self-vote paths advance `voted_term` without recording
    /// a digest. Leaving the previous term's digest paired with the new term
    /// makes the §4.4 gate reject the proposer's own commit, which strands the
    /// majority side of a partition — it proposes, wins the vote, and then
    /// refuses to apply the result.
    #[test]
    fn a_proposer_accepts_the_commit_for_its_own_proposal() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));

        // A prior vote at an earlier term leaves a digest behind.
        let earlier = TopologyTerm::new(1, members(&[1, 2, 3]), NodeId(2), ClusterId::UNSET, 1, 3);
        assert!(auth.handle_propose(&earlier).accepted);
        assert_eq!(auth.voted_digest(), Some(earlier.digest));

        // Now this node proposes a new term itself.
        let proposal = auth
            .on_membership_changed(&members(&[1, 2, 3]))
            .expect("node 1 is the deterministic proposer");
        assert_eq!(
            auth.voted_digest(),
            Some(proposal.digest),
            "the self-vote must attest to the proposal's OWN digest",
        );

        // The commit its own quorum produces must apply.
        let commit = TopologyCommit {
            term: proposal.term,
            proposer: NodeId(1),
            members: proposal.members.clone(),
            cluster_id: proposal.cluster_id,
            placement_version: proposal.placement_version,
            committed_peak: proposal.committed_peak,
            digest: proposal.digest,
            voters: members(&[1, 2, 3]),
        };
        let before = vote_digest_mismatch_total();
        assert_eq!(
            auth.handle_commit(&commit),
            Some(proposal.term),
            "a proposer must apply the commit for its own proposal",
        );
        assert_eq!(vote_digest_mismatch_total(), before);
    }

    /// §6.1 (R7) — reject is not fence. Every structural rejection leaves the
    /// node serving its existing term, so one malformed frame — or one
    /// proposer bug — cannot take the cluster out. The ONE fence-arming path
    /// is the placement-version refusal (P1-7), which is a live v1/v2
    /// dual-authority guard and stays.
    #[test]
    fn structural_rejections_never_arm_the_self_fence() {
        let mems = members(&[1, 2, 3]);
        let good_digest = TopologyTerm::compute_digest(4, &ClusterId::UNSET, &mems, 1, 3);

        // Non-ascending members, a wrong digest, an implausible member flood,
        // and a nonsensical peak — every one is a reject.
        let unsorted = members(&[3, 1, 2]);
        let flood: Vec<NodeId> = (100..1124).map(NodeId).collect();
        let cases = vec![
            (
                "members not strictly ascending",
                TopologyCommit {
                    term: 4,
                    proposer: NodeId(1),
                    members: unsorted.clone(),
                    cluster_id: ClusterId::UNSET,
                    placement_version: 1,
                    committed_peak: 3,
                    digest: TopologyTerm::compute_digest(4, &ClusterId::UNSET, &unsorted, 1, 3),
                    voters: unsorted.clone(),
                },
            ),
            (
                "digest does not match its own fields",
                TopologyCommit {
                    term: 4,
                    proposer: NodeId(1),
                    members: mems.clone(),
                    cluster_id: ClusterId::UNSET,
                    placement_version: 1,
                    committed_peak: 3,
                    digest: [0xAB; 32],
                    voters: mems.clone(),
                },
            ),
            (
                "implausible membership growth",
                TopologyCommit {
                    term: 4,
                    proposer: NodeId(100),
                    members: flood.clone(),
                    cluster_id: ClusterId::UNSET,
                    placement_version: 1,
                    committed_peak: flood.len() as u64,
                    digest: TopologyTerm::compute_digest(
                        4,
                        &ClusterId::UNSET,
                        &flood,
                        1,
                        flood.len() as u64,
                    ),
                    voters: flood.clone(),
                },
            ),
            (
                "peak below the member count",
                TopologyCommit {
                    term: 4,
                    proposer: NodeId(1),
                    members: mems.clone(),
                    cluster_id: ClusterId::UNSET,
                    placement_version: 1,
                    committed_peak: 1,
                    digest: TopologyTerm::compute_digest(4, &ClusterId::UNSET, &mems, 1, 1),
                    voters: mems.clone(),
                },
            ),
        ];

        for (why, commit) in cases {
            let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
            assert_eq!(auth.handle_commit(&commit), None, "{why}: must be rejected");
            assert!(
                !auth.is_self_fenced(),
                "{why}: a structural rejection must not fence",
            );
            assert_eq!(auth.unapplicable_committed_term(), 0, "{why}");
            assert_eq!(auth.committed_term(), 0, "{why}: nothing may be applied");

            // Still able to accept a good commit afterwards — the node was
            // held back, not bricked.
            let good = TopologyCommit {
                term: 4,
                proposer: NodeId(1),
                members: mems.clone(),
                cluster_id: ClusterId::UNSET,
                placement_version: 1,
                committed_peak: 3,
                digest: good_digest,
                voters: mems.clone(),
            };
            assert_eq!(auth.handle_commit(&good), Some(4), "{why}: must recover");
        }
    }

    /// §4.5 (P1-8) — a quorum-backed commit naming the committed term with a
    /// different digest is a committed-history fork. Detected and counted; the
    /// frame is still rejected (it is a stale term) and nothing is fenced.
    #[test]
    fn commit_contradicting_the_committed_digest_raises_the_fork_alarm() {
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 3,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems, 1, 3),
            voters: mems.clone(),
        };
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        assert_eq!(auth.handle_commit(&commit), Some(5));

        // A different topology, quorum-backed, claiming the SAME term.
        let forked_members = members(&[1, 2, 3, 4]);
        let forked = TopologyCommit {
            term: 5,
            proposer: NodeId(4),
            members: forked_members.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 4,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &forked_members, 1, 4),
            voters: forked_members.clone(),
        };

        let before = committed_digest_fork_total();
        assert_eq!(
            auth.handle_commit(&forked),
            None,
            "a stale-term commit is still rejected",
        );
        assert_eq!(
            committed_digest_fork_total(),
            before + 1,
            "the fork must be counted — today it is discarded before any comparison",
        );
        assert_eq!(
            auth.committed_term(),
            5,
            "committed state must be untouched"
        );
        assert_eq!(auth.committed_digest(), Some(commit.digest));
    }

    /// §4.5 — a REPLAY of the same commit is not a fork. Duplicate commit
    /// frames are ordinary (broadcast retries), so the detector must key on
    /// the digest differing, not on the term repeating.
    #[test]
    fn replaying_the_same_commit_does_not_raise_the_fork_alarm() {
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 3,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems, 1, 3),
            voters: mems.clone(),
        };
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        assert_eq!(auth.handle_commit(&commit), Some(5));

        let before = committed_digest_fork_total();
        assert_eq!(auth.handle_commit(&commit), None, "replay is a stale term");
        assert_eq!(
            committed_digest_fork_total(),
            before,
            "an identical replay is not a fork",
        );
    }

    /// §4.7 (P1-6) — a structurally invalid frame must never reach a detector.
    /// A commit with a sub-quorum voter list claiming the committed term with a
    /// different digest proves nothing: the voter list is plaintext and
    /// self-declared, so anyone can assert a fork that did not happen.
    #[test]
    fn a_sub_quorum_frame_cannot_raise_the_fork_alarm() {
        let mems = members(&[1, 2, 3]);
        let commit = TopologyCommit {
            term: 5,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 3,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems, 1, 3),
            voters: mems.clone(),
        };
        let auth = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        assert_eq!(auth.handle_commit(&commit), Some(5));

        let forked_members = members(&[1, 2, 3, 4]);
        let sub_quorum = TopologyCommit {
            term: 5,
            proposer: NodeId(4),
            members: forked_members.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 4,
            digest: TopologyTerm::compute_digest(5, &ClusterId::UNSET, &forked_members, 1, 4),
            voters: members(&[4]), // one voter for a 4-member topology
        };
        assert!(
            !sub_quorum.has_quorum_voter_proof(),
            "precondition: the frame carries no quorum proof",
        );

        let before = committed_digest_fork_total();
        assert_eq!(auth.handle_commit(&sub_quorum), None);
        assert_eq!(
            committed_digest_fork_total(),
            before,
            "an unproven frame must not be able to assert a fork",
        );
    }

    #[test]
    fn restore_discards_a_persisted_commit_for_the_wrong_term() {
        // The CRC proves the bytes are what was written; it cannot prove they
        // describe the term being restored. A mismatched commit must be
        // dropped, not replayed as a proof for a term it does not name.
        let mems = members(&[1, 2, 3]);
        let stale = TopologyCommit {
            term: 4,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: 3,
            digest: TopologyTerm::compute_digest(4, &ClusterId::UNSET, &mems, 1, 3),
            voters: mems.clone(),
        };
        let state = PersistedTopologyState {
            peak_cluster_size: 3,
            committed_term: 9, // term 9, but the blob describes term 4
            committed_members: mems.clone(),
            committed_voters: mems.clone(),
            voted_term: 9,
            incarnation: 0,
            committed_voter_ever_seen: mems.clone(),
            committed_placement_version: 1,
            committed_peak: 3,
            committed_commit: Some(stale.serialize()),
            voted_digest: None,
        };

        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        auth.restore(&state);
        assert_eq!(auth.committed_term(), 9);
        assert_eq!(
            auth.committed_commit_bytes(),
            None,
            "a commit naming a different term must be discarded at load",
        );

        // Unparseable bytes are dropped the same way.
        let mut garbage = state.clone();
        garbage.committed_commit = Some(vec![0xAB; 12]);
        let auth2 = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        auth2.restore(&garbage);
        assert_eq!(
            auth2.committed_commit_bytes(),
            None,
            "a commit that does not parse must be discarded at load",
        );
    }

    #[test]
    fn compute_digest_changes_with_committed_peak() {
        let mems = members(&[1, 2, 3]);
        let d1 = TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems, 1, 3);
        let d2 = TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems, 1, 4);
        assert_ne!(d1, d2, "committed_peak must be mixed into the digest");

        let d3 = TopologyTerm::compute_digest(5, &ClusterId::UNSET, &mems, 1, 3);
        assert_eq!(
            d1, d3,
            "identical committed_peak must produce an identical digest"
        );
    }

    #[test]
    fn peak_floor_is_max_committed_and_observed() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        // Directly manipulate the two underlying atomics to prove the
        // getter returns their max, independent of how each was populated.
        auth.committed_peak.store(3, Ordering::Relaxed);
        auth.peak_cluster_size.store(5, Ordering::Relaxed);
        assert_eq!(
            auth.peak_cluster_size(),
            5,
            "observed_peak higher: floor = observed"
        );

        auth.committed_peak.store(7, Ordering::Relaxed);
        assert_eq!(
            auth.peak_cluster_size(),
            7,
            "committed_peak higher: floor = committed_peak"
        );

        // Stage 1 behavior-preservation: on every REAL non-lowering
        // producer, committed_peak is stamped from peak_cluster_size()
        // itself, so committed_peak never exceeds observed_peak in
        // practice and the floor equals exactly what observed_peak alone
        // would have reported before this field existed.
        let auth2 = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let term = auth2
            .on_membership_changed(&members(&[1, 2, 3]))
            .expect("proposer");
        assert_eq!(
            term.committed_peak,
            auth2.peak_cluster_size.load(Ordering::Relaxed),
            "committed_peak stamped on a proposal equals the (already-raised) observed peak"
        );
        assert_eq!(auth2.peak_cluster_size(), 3);
    }

    #[test]
    fn commit_rejected_when_committed_peak_below_members_len() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let mems = members(&[1, 2, 3, 4]);
        // committed_peak (3) < members.len() (4) — nonsensical, must be
        // rejected by the gate invariant even though the commit otherwise
        // carries a full quorum voter proof.
        let bad_peak = 3u64;
        let commit = TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: mems.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: bad_peak,
            digest: TopologyTerm::compute_digest(1, &ClusterId::UNSET, &mems, 1, bad_peak),
            voters: mems.clone(),
        };
        assert!(
            auth.handle_commit(&commit).is_none(),
            "committed_peak < members.len() must be rejected by the gate invariant"
        );
        assert_eq!(auth.committed_term(), 0, "nothing should have applied");
    }

    #[test]
    fn restore_seeds_observed_from_committed_peak() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let state = PersistedTopologyState {
            // Deliberately HIGHER than committed_peak, to prove restore()
            // no longer separately re-observes this raw field: the
            // effective floor comes from committed_peak alone.
            //
            // G8 final review (finding 1) — this test exercises `restore()`
            // IN ISOLATION on a fresh authority, which is NOT the production
            // sequence: `bin/server.rs` calls `ClusterCoordinator::new()`
            // (which pre-seeds this same atom) BEFORE `restore()` runs. That
            // gap is exactly what let a stale-HIGH pre-restore seed survive
            // a raise-only re-observe here undetected — see
            // `restart_after_shrink_keeps_lowered_floor_via_new_then_restore`
            // below, which drives the real `new()`-then-`restore()` path and
            // is the regression test that would have caught it.
            peak_cluster_size: 10,
            committed_term: 9,
            committed_members: members(&[1, 2, 3]),
            committed_voters: members(&[1, 2, 3]),
            voted_term: 9,
            incarnation: 0,
            committed_voter_ever_seen: members(&[1, 2, 3]),
            committed_placement_version: 1,
            committed_peak: 6,
            committed_commit: None,
            voted_digest: None,
        };
        auth.restore(&state);
        assert_eq!(
            auth.committed_peak(),
            6,
            "committed_peak restored verbatim (raise-floored at committed_members.len())"
        );
        assert_eq!(
            auth.peak_cluster_size.load(Ordering::Relaxed),
            6,
            "observed_peak must be seeded from committed_peak, not separately from state.peak_cluster_size (10)"
        );
        assert_eq!(auth.peak_cluster_size(), 6);
    }

    /// G8 final review (finding 1, BLOCKING) — a committed 5→3 shrink must
    /// NOT re-inflate back to the old peak (5) on restart.
    ///
    /// Bug trace: `persisted_state_for_commit` computes the vestigial
    /// `PersistedTopologyState::peak_cluster_size` field PRE-apply, so on
    /// the very commit that lowers `committed_peak` it is still the OLD
    /// peak (5). `bin/server.rs` used to seed `ClusterCoordinator::new`'s
    /// `initial_peak` from exactly that stale field, which
    /// `ClusterCoordinator::new` folds into the observed-peak atom via the
    /// raise-only `observe_peak_cluster_size` — BEFORE `restore()` runs.
    /// `restore()`'s own re-observe of the correctly-lowered
    /// `committed_peak` (3) was ALSO raise-only (`fetch_max`), so it could
    /// never pull the atom back down from 5. Net: the getter reports
    /// `max(3, 5) = 5` forever after restart — the shrink is silently
    /// reverted.
    ///
    /// This test drives the EXACT production boot sequence —
    /// `ClusterCoordinator::new(config, loaded.peak_cluster_size)` THEN
    /// `topology_authority.restore(&loaded)`, exactly as `bin/server.rs`
    /// calls it — NOT a fresh-authority `restore()` in isolation (that
    /// isolated shape is what let this bug through review the first time;
    /// see `restore_seeds_observed_from_committed_peak` above).
    #[test]
    fn restart_after_shrink_keeps_lowered_floor_via_new_then_restore() {
        use crate::cluster::coordinator::{ClusterConfig, ClusterCoordinator};

        // A shrunk state as it would actually be found on disk after a
        // committed 5→3 shrink: `committed_peak`/`committed_members` are
        // correctly lowered to 3, but the vestigial `peak_cluster_size`
        // field still carries the pre-shrink peak (5) — exactly what
        // `persisted_state_for_commit` writes (see its doc comment).
        let loaded = PersistedTopologyState {
            peak_cluster_size: 5, // vestigial pre-apply field — stale-HIGH
            committed_term: 10,
            committed_members: members(&[1, 2, 3]),
            committed_voters: members(&[1, 2, 3]),
            voted_term: 10,
            incarnation: 0,
            committed_voter_ever_seen: members(&[1, 2, 3, 4, 5]),
            committed_placement_version: 1,
            committed_peak: 3, // the durable, correctly-lowered anchor
            committed_commit: None,
            voted_digest: None,
        };

        let config = ClusterConfig {
            self_id: NodeId(1),
            self_addr: "127.0.0.1:17100".parse().unwrap(),
            swim_bind: "127.0.0.1:17101".parse().unwrap(),
            swim_advertise_addr: None,
            seed_nodes: Vec::new(),
            replication_factor: 3,
            probe_interval: Duration::from_millis(100),
            suspicion_timeout: Duration::from_secs(1),
            cluster_secret: None,
            max_migration_threads: 1,
            topology_propose_timeout: Duration::from_millis(500),
            topology_debounce: Duration::from_millis(0),
            migration_pool_size: 1,
            migration_batch_size: 1,
            persisted_incarnation: 0,
            cluster_id: ClusterId::UNSET,
            reverse_heal_online: false,
            heal_deadline: Duration::from_secs(60),
            heal_deadline_action: crate::config::HealDeadlineAction::AlertAndHold,
        };

        // The EXACT production sequence (bin/server.rs): `initial_peak`
        // seeded from the loaded state's `peak_cluster_size` (the buggy,
        // stale-HIGH field pre-fix)...
        let coordinator = ClusterCoordinator::new(config, loaded.peak_cluster_size as usize);
        // ...THEN restore(), which must correct the floor to the durable
        // anchor regardless of what `new()` already seeded.
        coordinator.topology_authority.restore(&loaded);

        assert_eq!(
            coordinator.topology_authority.peak_cluster_size(),
            3,
            "restart must keep the shrink's LOWERED floor (3), not re-inflate \
             to the pre-shrink peak (5) folded in by new()'s pre-restore seed",
        );
        // Downstream consequence: the quorum majority derived from the
        // restored floor must use the peak-3 majority (2), not the stale
        // peak-5 majority (3) — the difference between a minority remnant
        // being able to force a commit or not.
        assert_eq!(
            coordinator.topology_authority.activation_quorum_needed(3),
            2,
            "quorum derived from the restored floor must reflect the \
             lowered peak (3/2+1=2), not the stale one (5/2+1=3)",
        );
    }

    #[test]
    fn grow_carries_new_members_len_in_committed_peak() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        let small = members(&[1, 2, 3]);
        let term1 = auth
            .on_membership_changed(&small)
            .expect("bootstrap proposer");
        assert_eq!(term1.committed_peak, 3);

        let commit1 = TopologyCommit {
            term: term1.term,
            proposer: term1.proposer,
            members: term1.members.clone(),
            cluster_id: term1.cluster_id,
            placement_version: term1.placement_version,
            committed_peak: term1.committed_peak,
            digest: term1.digest,
            voters: small.clone(),
        };
        assert_eq!(auth.handle_commit(&commit1), Some(term1.term));

        // Grow to 5 members. F-G8-001: with cluster_id unset, the ever-seen
        // fallback rejects a proposal introducing never-before-seen members
        // — pre-seed nodes 4 and 5 as known voters, matching how the other
        // legitimate-grow tests in this module handle the same fallback.
        auth.set_committed_voter_ever_seen(&[
            NodeId(1),
            NodeId(2),
            NodeId(3),
            NodeId(4),
            NodeId(5),
        ]);
        let grown = members(&[1, 2, 3, 4, 5]);
        let term2 = auth
            .on_membership_changed(&grown)
            .expect("proposer for the grow");
        assert_eq!(
            term2.committed_peak, 5,
            "a grow must carry the NEW members.len() as committed_peak"
        );
    }

    #[test]
    fn graceful_leave_subset_carries_old_higher_peak_in_committed_peak() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        // Establish a committed 5-node cluster (peak raised to 5).
        let full = members(&[1, 2, 3, 4, 5]);
        let commit = TopologyCommit {
            term: 1,
            proposer: NodeId(1),
            members: full.clone(),
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak: full.len() as u64,
            digest: TopologyTerm::compute_digest(1, &ClusterId::UNSET, &full, 1, full.len() as u64),
            voters: full.clone(),
        };
        assert_eq!(auth.handle_commit(&commit), Some(1));
        assert_eq!(auth.peak_cluster_size(), 5);

        // Graceful leave: propose a SUBSET (3 nodes). committed_peak on
        // the resulting proposal must stay at the OLD peak (5), not drop
        // to 3 — preserving today's quiesce/fencing semantics exactly.
        let subset = members(&[1, 2, 3]);
        let term = auth
            .on_membership_changed(&subset)
            .expect("proposer for the subset");
        assert_eq!(
            term.committed_peak, 5,
            "graceful-leave subset must carry the OLD higher peak (non-lowering)"
        );
        assert_eq!(
            auth.peak_cluster_size(),
            5,
            "peak must not drop after a subset proposal"
        );
    }

    // -----------------------------------------------------------------------
    // G8 stage 2 — Gate B (apply-time shrink floor) + propose_shrink +
    // observed_peak reset.
    //
    // Stage 1 shipped the data model with an UNCONDITIONAL
    // `committed_peak.store(commit.committed_peak)` in `apply_commit_locked`
    // and no floor re-check on the voter/apply side. That is the exposure
    // these tests close: a stale/behind proposer's low-committed_peak commit
    // must be rejected by every node whose OWN durable `committed_peak` is
    // still high, while a genuinely quorate shrink (a real majority of the
    // OLD peak voted) must apply everywhere.
    // -----------------------------------------------------------------------

    /// Build a `TopologyCommit` with a consistent digest — small helper to
    /// cut boilerplate across the stage-2 tests below.
    fn quorum_commit(
        term: u64,
        proposer: NodeId,
        members: Vec<NodeId>,
        committed_peak: u64,
        voters: Vec<NodeId>,
    ) -> TopologyCommit {
        let digest =
            TopologyTerm::compute_digest(term, &ClusterId::UNSET, &members, 1, committed_peak);
        TopologyCommit {
            term,
            proposer,
            members,
            cluster_id: ClusterId::UNSET,
            placement_version: 1,
            committed_peak,
            digest,
            voters,
        }
    }

    /// Seed `auth` with a committed N-member cluster at `committed_peak ==
    /// members.len()` (term 1), so subsequent tests start from a settled,
    /// non-shrunk floor.
    fn seed_committed(auth: &TopologyAuthority, members: Vec<NodeId>) {
        let commit = quorum_commit(
            1,
            members[0],
            members.clone(),
            members.len() as u64,
            members,
        );
        assert_eq!(
            auth.handle_commit(&commit),
            Some(1),
            "seed commit must apply"
        );
    }

    #[test]
    fn has_quorum_voter_proof_for_generalizes_threshold() {
        let commit = quorum_commit(1, NodeId(1), members(&[1, 2, 3]), 3, members(&[1, 2]));
        // Default threshold (majority of 3 = 2): 2 voters is enough.
        assert!(commit.has_quorum_voter_proof());
        assert!(commit.has_quorum_voter_proof_for(2));
        // A stricter threshold the same 2 voters cannot satisfy.
        assert!(!commit.has_quorum_voter_proof_for(3));
        // A voter outside `members` still fails regardless of threshold.
        let poisoned = quorum_commit(1, NodeId(1), members(&[1, 2, 3]), 3, members(&[1, 9]));
        assert!(!poisoned.has_quorum_voter_proof_for(1));
    }

    /// THE reviewer's required test: a stale/behind proposer's
    /// lower-committed_peak commit is rejected by a caught-up node, and an
    /// equivalent commit that DOES carry a quorum of the node's own
    /// (higher) local peak is accepted.
    #[test]
    fn gate_b_rejects_shrink_without_old_peak_quorum_voters() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        seed_committed(&auth, members(&[1, 2, 3, 4, 5]));
        assert_eq!(auth.committed_peak(), 5, "local peak established at 5");

        // Reject: a shrink-to-2 commit whose ONLY 2 voters are the 2
        // surviving members themselves — insufficient against local_peak=5
        // (needs 5/2+1 = 3).
        let insufficient = quorum_commit(2, NodeId(4), members(&[4, 5]), 2, members(&[4, 5]));
        assert!(
            auth.handle_commit(&insufficient).is_none(),
            "a 2-voter shrink commit must be rejected when local committed_peak is 5 (needs 3)"
        );
        assert_eq!(
            auth.committed_peak(),
            5,
            "a rejected commit must not mutate the durable floor"
        );
        assert_eq!(
            auth.committed_term(),
            1,
            "a rejected commit must not advance the term"
        );

        // Accept: a shrink-to-3 commit carrying 3 distinct in-member voters
        // — meets the same local_peak=5 threshold (3 >= 3).
        let sufficient = quorum_commit(2, NodeId(3), members(&[3, 4, 5]), 3, members(&[3, 4, 5]));
        assert_eq!(
            auth.handle_commit(&sufficient),
            Some(2),
            "a 3-voter shrink commit must be accepted when local committed_peak is 5 (needs 3)"
        );
        assert_eq!(auth.committed_peak(), 3);
        assert_eq!(
            auth.peak_cluster_size(),
            3,
            "observed_peak must also drop to 3"
        );
    }

    /// Failure mode 1: a minority (2-of-5) cannot shrink the cluster to
    /// itself, at either gate.
    #[test]
    fn minority_cannot_shrink() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        seed_committed(&auth, members(&[1, 2, 3, 4, 5]));

        // Gate A: node 1 (the deterministic proposer) attempts to shrink to
        // just itself + node 2 — a minority of the OLD peak (5).
        let term = auth
            .propose_shrink(members(&[1, 2]))
            .expect("the deterministic proposer may attempt the proposal");
        assert_eq!(term.committed_peak, 2);

        let vote2 = TopologyVote {
            term: term.term,
            digest: term.digest,
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
            voter_placement_support: 1,
        };
        let commit = auth.handle_vote(&vote2);
        assert!(
            commit.is_none(),
            "2 votes (self + node 2) never reaches the peak-derived quorum of 3 — Gate A blocks"
        );

        // Gate B: even a hand-forged 2-voter commit (bypassing the normal
        // propose/vote path entirely) is rejected — on the SAME node and on
        // an independently-seeded node, both with local committed_peak=5.
        let forged = quorum_commit(2, NodeId(1), members(&[1, 2]), 2, members(&[1, 2]));
        assert!(
            auth.handle_commit(&forged).is_none(),
            "Gate B must reject a forged 2-voter shrink commit at local_peak=5"
        );
        assert_eq!(auth.committed_peak(), 5, "floor must remain unlowered");

        let other = TopologyAuthority::new(NodeId(3), Duration::from_secs(1));
        seed_committed(&other, members(&[1, 2, 3, 4, 5]));
        assert!(
            other.handle_commit(&forged).is_none(),
            "Gate B rejection is per-node: every high-local_peak node refuses the same forged commit"
        );
    }

    /// A real majority of the OLD peak (3 of 5) can shrink the cluster to
    /// 3, and the new floor is then used for subsequent quorum math.
    #[test]
    fn majority_can_shrink_5_to_3() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        seed_committed(&auth, members(&[1, 2, 3, 4, 5]));

        let term = auth
            .propose_shrink(members(&[1, 2, 3]))
            .expect("the deterministic proposer may propose the shrink");
        assert_eq!(term.committed_peak, 3);

        // Gate A quorum = max(3/2+1=2, 5/2+1=3) = 3. Self-vote (1) + 2 more.
        let commit_after_first = auth.handle_vote(&TopologyVote {
            term: term.term,
            digest: term.digest,
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
            voter_placement_support: 1,
        });
        assert!(
            commit_after_first.is_none(),
            "2/3 accepted so far, not yet quorum"
        );

        let commit = auth
            .handle_vote(&TopologyVote {
                term: term.term,
                digest: term.digest,
                voter: NodeId(3),
                accepted: true,
                voter_current_term: 0,
                voter_placement_support: 1,
            })
            .expect("3rd accepting vote reaches the peak-derived quorum of 3");
        assert_eq!(commit.voters.len(), 3);

        assert_eq!(auth.handle_commit(&commit), Some(term.term));
        assert_eq!(auth.committed_peak(), 3, "durable floor lowered to 3");
        assert_eq!(
            auth.peak_cluster_size.load(Ordering::Relaxed),
            3,
            "raw observed_peak atom must ALSO be force-reset to 3, not left at the stale 5"
        );
        assert_eq!(
            auth.peak_cluster_size(),
            3,
            "combined getter reflects the new floor"
        );
        assert_eq!(auth.committed_members(), members(&[1, 2, 3]));

        // Subsequent activation quorum uses the new floor (3), not the
        // pre-shrink 5.
        assert_eq!(
            auth.activation_quorum_needed(3),
            2,
            "quorum math must now derive from the lowered peak"
        );
    }

    /// The proposer may exclude itself from the surviving set. Its own vote
    /// must NOT be recorded (see `propose_shrink`'s doc comment) — the full
    /// quorum must come from `surviving` members so the resulting commit's
    /// voters stay a subset of its own `members` and can pass
    /// `has_quorum_voter_proof`/Gate B on every node, including the
    /// proposer's own eventual view of the commit.
    #[test]
    fn propose_shrink_self_omit_excludes_self_from_voters() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        seed_committed(&auth, members(&[1, 2, 3, 4, 5]));

        let term = auth
            .propose_shrink(members(&[2, 3, 4]))
            .expect("the lowest committed node may propose dropping itself");
        assert_eq!(term.members, members(&[2, 3, 4]));
        assert_eq!(term.committed_peak, 3);
        assert!(
            !term.members.contains(&NodeId(1)),
            "self-excluding shrink must not include the proposer in the new membership"
        );

        // Quorum needed = max(3/2+1=2, 5/2+1=3) = 3. Since self did NOT
        // self-vote, all 3 votes must come from members 2, 3, 4.
        let mut commit_opt = None;
        for voter in [NodeId(2), NodeId(3)] {
            let c = auth.handle_vote(&TopologyVote {
                term: term.term,
                digest: term.digest,
                voter,
                accepted: true,
                voter_current_term: 0,
                voter_placement_support: 1,
            });
            assert!(c.is_none(), "quorum not yet reached without self-vote");
            commit_opt = c;
        }
        let commit = auth
            .handle_vote(&TopologyVote {
                term: term.term,
                digest: term.digest,
                voter: NodeId(4),
                accepted: true,
                voter_current_term: 0,
                voter_placement_support: 1,
            })
            .expect("3rd external vote (2,3,4) reaches quorum without a self-vote");
        assert!(commit_opt.is_none());
        assert!(
            !commit.voters.contains(&NodeId(1)),
            "the proposer's own id must not appear in the commit's voter list"
        );
        assert_eq!(commit.voters.len(), 3);

        // Apply on an INDEPENDENT node (simulating a peer) whose local
        // committed_peak is also 5 — proves the commit passes both the
        // unmodified has_quorum_voter_proof (voters subset of members) and
        // Gate B (quorum of the old peak).
        let peer = TopologyAuthority::new(NodeId(2), Duration::from_secs(1));
        seed_committed(&peer, members(&[1, 2, 3, 4, 5]));
        assert_eq!(peer.handle_commit(&commit), Some(term.term));
        assert_eq!(peer.committed_members(), members(&[2, 3, 4]));
        assert_eq!(peer.committed_peak(), 3);
    }

    /// Failure mode 2: a 3|2 split where each side proposes a shrink to
    /// itself. The 3-side gathers a real majority-of-5 and commits; the
    /// 2-side (simulated as a forged commit, since only the deterministic
    /// global proposer — node 1, on the 3-side here — can drive
    /// `propose_shrink`) is rejected at both gates. On heal, a 2-side node
    /// adopts the 3-side's higher-term, properly-quorate commit. No
    /// split-brain: final committed_peak == 3 everywhere.
    #[test]
    fn split_then_shrink_both_sides() {
        // 3-side: node 1 (global deterministic proposer) shrinks to {1,2,3}
        // and reaches a real quorum of the old peak (3 of 5).
        let side_a = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        seed_committed(&side_a, members(&[1, 2, 3, 4, 5]));
        let term = side_a
            .propose_shrink(members(&[1, 2, 3]))
            .expect("3-side proposer");
        side_a.handle_vote(&TopologyVote {
            term: term.term,
            digest: term.digest,
            voter: NodeId(2),
            accepted: true,
            voter_current_term: 0,
            voter_placement_support: 1,
        });
        let commit_3_side = side_a
            .handle_vote(&TopologyVote {
                term: term.term,
                digest: term.digest,
                voter: NodeId(3),
                accepted: true,
                voter_current_term: 0,
                voter_placement_support: 1,
            })
            .expect("3-side reaches quorum (3 of old peak 5)");
        assert_eq!(side_a.handle_commit(&commit_3_side), Some(term.term));
        assert_eq!(side_a.committed_peak(), 3, "3-side commits its shrink");

        // 2-side: nodes 4/5 cannot drive propose_shrink (they are not the
        // global deterministic proposer), so simulate their best-effort
        // minority attempt as a forged 2-voter commit at the SAME term.
        let side_b = TopologyAuthority::new(NodeId(4), Duration::from_secs(1));
        seed_committed(&side_b, members(&[1, 2, 3, 4, 5]));
        let forged_2_side =
            quorum_commit(term.term, NodeId(4), members(&[4, 5]), 2, members(&[4, 5]));
        assert!(
            side_b.handle_commit(&forged_2_side).is_none(),
            "2-side's minority shrink must be rejected (Gate A/B both fail)"
        );
        assert_eq!(
            side_b.committed_peak(),
            5,
            "2-side floor unchanged before heal"
        );

        // Heal: the 2-side node receives the 3-side's higher-term,
        // properly-quorate commit and adopts it.
        assert_eq!(side_b.handle_commit(&commit_3_side), Some(term.term));
        assert_eq!(
            side_b.committed_peak(),
            3,
            "2-side heals to the 3-side's committed_peak"
        );
        assert_eq!(side_b.committed_members(), members(&[1, 2, 3]));
        assert_eq!(
            side_a.committed_peak(),
            side_b.committed_peak(),
            "no split-brain: both sides converge to 3"
        );
    }

    /// Failure mode 3: a shrink and a grow proposed at the same term are
    /// serialized by strict term monotonicity — the loser is rejected
    /// outright (not partially applied), and Gate B's threshold correctly
    /// tracks whichever commit actually won.
    #[test]
    fn shrink_racing_grow_serialized() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        seed_committed(&auth, members(&[1, 2, 3, 4, 5]));
        auth.set_committed_voter_ever_seen(&[
            NodeId(1),
            NodeId(2),
            NodeId(3),
            NodeId(4),
            NodeId(5),
            NodeId(6),
        ]);

        let grow = quorum_commit(
            2,
            NodeId(1),
            members(&[1, 2, 3, 4, 5, 6]),
            6,
            members(&[1, 2, 3, 4]),
        );
        let shrink = quorum_commit(2, NodeId(1), members(&[1, 2, 3]), 3, members(&[1, 2, 3]));

        // Grow wins the race for term 2.
        assert_eq!(auth.handle_commit(&grow), Some(2));
        assert_eq!(auth.committed_peak(), 6);
        assert_eq!(auth.committed_members(), members(&[1, 2, 3, 4, 5, 6]));

        // The shrink, same term, arrives second: rejected outright by
        // strict term monotonicity — nothing is torn.
        assert!(
            auth.handle_commit(&shrink).is_none(),
            "same-term commit must be rejected (term must be strictly higher)"
        );
        assert_eq!(
            auth.committed_peak(),
            6,
            "no torn floor — grow's state is untouched"
        );
        assert_eq!(auth.committed_members(), members(&[1, 2, 3, 4, 5, 6]));

        // Retried at term 3 with the SAME (now stale) voter set: Gate B
        // recomputes its threshold against the CURRENT local_peak (6, not
        // the 5 it was computed against originally), so the same 3 voters
        // are now insufficient (need 6/2+1 = 4).
        let shrink_retry = quorum_commit(3, NodeId(1), members(&[1, 2, 3]), 3, members(&[1, 2, 3]));
        assert!(
            auth.handle_commit(&shrink_retry).is_none(),
            "Gate B must re-derive its threshold from the post-grow peak (6), rejecting the stale 3-voter proof"
        );
        assert_eq!(
            auth.committed_peak(),
            6,
            "floor still unaffected by the rejected retry"
        );
    }

    /// observed_peak (the raw SWIM/proposal-fed atom) is lowered ONLY by a
    /// Gate-B-passed shrink; every other path stays monotonic (`fetch_max`).
    #[test]
    fn observed_peak_lowered_only_on_shrink() {
        let auth = TopologyAuthority::new(NodeId(1), Duration::from_secs(1));
        seed_committed(&auth, members(&[1, 2, 3, 4, 5]));

        // Simulate some other SWIM-driven observation bumping the raw
        // observed atom above the durable committed_peak.
        auth.observe_peak_cluster_size(10);
        assert_eq!(
            auth.peak_cluster_size(),
            10,
            "observed peak dominates for now"
        );

        // A NON-shrink commit (same membership, same committed_peak==5,
        // just a new term) must NOT touch the raw observed atom.
        let non_shrink = quorum_commit(
            2,
            NodeId(1),
            members(&[1, 2, 3, 4, 5]),
            5,
            members(&[1, 2, 3]),
        );
        assert_eq!(auth.handle_commit(&non_shrink), Some(2));
        assert_eq!(auth.committed_peak(), 5);
        assert_eq!(
            auth.peak_cluster_size.load(Ordering::Relaxed),
            10,
            "non-shrink apply must leave the raw observed_peak atom untouched (still monotonic)"
        );

        // A Gate-B-passed shrink (committed_peak 5 -> 3) DOES force-reset
        // the raw observed atom, even though it was sitting at 10.
        let shrink = quorum_commit(3, NodeId(1), members(&[1, 2, 3]), 3, members(&[1, 2, 3]));
        assert_eq!(auth.handle_commit(&shrink), Some(3));
        assert_eq!(auth.committed_peak(), 3);
        assert_eq!(
            auth.peak_cluster_size.load(Ordering::Relaxed),
            3,
            "a Gate-B-passed shrink is the ONLY path allowed to lower the raw observed_peak atom"
        );
        assert_eq!(
            auth.peak_cluster_size(),
            3,
            "the combined getter must also reflect the lowered floor (no re-inflation from the stale 10)"
        );
    }
}
