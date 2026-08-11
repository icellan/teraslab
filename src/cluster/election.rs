//! Committed master election — the conditionally anchored assignment (§5)
//! and replica derivation (§11) of
//! `specs/COMMITTED_MASTER_ELECTION_DESIGN.md`.
//!
//! # Why an anchor, and why a *conditional* one
//!
//! The deterministic table ([`ShardTable::compute_with_epoch`]) is a fixed
//! point: every confused node recomputes it and converges. Electing a master
//! per node destroys that fixed point, which is how the cluster ends up with
//! two nodes each believing they master the same shard.
//!
//! Anchoring the election on the PREVIOUS COMMITTED assignment restores a
//! fixed point — an election with no new evidence reproduces its input — while
//! still allowing failover to move a master off the deterministic pick.
//!
//! An *unconditional* anchor has no reversion edge, and that is the trap this
//! module exists to avoid: a named master receives its migration, therefore
//! holds the data, therefore is never provably data-less — so one term of
//! influence (hostile, or merely a skewed partial view) buys PERMANENT
//! per-shard mastership, laundered by every honest term afterwards. A
//! deviation is therefore kept only while the reason for it still holds.
//!
//! # "Proven" means "self-reported"
//!
//! The holder signal is each peer's own report of `last_applied_seq`. A
//! malicious peer can report `0` for shards it holds and `> 0` for shards it
//! does not, steering an honest proposer's deviation. Nothing here treats a
//! report as proof; the hysteresis below is the mitigation, and the bound on
//! how much a single peer can seize is the candidate-set rule enforced by the
//! validator, not this module.

use std::collections::{HashMap, HashSet};

use crate::cluster::shards::{NUM_SHARDS, NodeId, ShardTable};

/// Consecutive terms a deviation's justification must hold before the
/// deviation is created or kept.
///
/// One term of a skewed view is not evidence. Two consecutive terms reporting
/// the same thing is the cheapest bar that a single stale exchange cannot
/// clear, and it costs at most one extra term of failover latency.
pub const DEVIATION_HYSTERESIS_TERMS: u32 = 2;

/// Self-reported holder signals gathered for one election round.
///
/// Two distinct facts per node, and conflating them is a defect: *did it
/// report at all* (absence means down or unreachable, which every node
/// observes identically) versus *what did it report* (a claim about data).
#[derive(Debug, Default, Clone)]
pub struct HolderReports {
    reported: HashSet<NodeId>,
    full: HashSet<(NodeId, u16)>,
}

impl HolderReports {
    /// Build from `(node, shard, last_applied_seq)` triples.
    ///
    /// Every node appearing in `entries` counts as having reported, even if
    /// all of its shards report `0` — "reported, holds nothing" and "did not
    /// report" are different states.
    pub fn from_entries(
        reporters: impl IntoIterator<Item = NodeId>,
        entries: impl IntoIterator<Item = (NodeId, u16, u64)>,
    ) -> Self {
        let mut reported: HashSet<NodeId> = reporters.into_iter().collect();
        let mut full = HashSet::new();
        for (node, shard, last_applied_seq) in entries {
            reported.insert(node);
            if last_applied_seq > 0 {
                full.insert((node, shard));
            }
        }
        Self { reported, full }
    }

    /// Did `node` answer the exchange at all?
    pub fn reported(&self, node: NodeId) -> bool {
        self.reported.contains(&node)
    }

    /// Does `node` self-report holding data for `shard`?
    pub fn is_full(&self, node: NodeId, shard: u16) -> bool {
        self.full.contains(&(node, shard))
    }

    /// No node answered — the election has no ownership signal at all.
    pub fn is_empty(&self) -> bool {
        self.reported.is_empty()
    }
}

/// Per-shard streak of consecutive terms in which a deviation's justification
/// held. Proposer-local: it steers what gets PROPOSED, never what a voter
/// accepts, so a proposer change simply resets the streaks.
#[derive(Debug, Default, Clone)]
pub struct DeviationHistory {
    streak: HashMap<u16, u32>,
}

impl DeviationHistory {
    /// Empty history — every deviation must earn its streak from scratch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record whether the justification held for `shard` this term and return
    /// the resulting streak length.
    fn observe(&mut self, shard: u16, justification_holds: bool) -> u32 {
        if !justification_holds {
            self.streak.remove(&shard);
            return 0;
        }
        let counter = self.streak.entry(shard).or_insert(0);
        *counter = counter.saturating_add(1);
        *counter
    }

    /// Streak length currently recorded for `shard`.
    pub fn streak(&self, shard: u16) -> u32 {
        self.streak.get(&shard).copied().unwrap_or(0)
    }
}

/// Everything the election reads. All of it is either digest-bound (`det`),
/// previously committed (`prev_committed`), or a self-reported signal.
pub struct ElectionInputs<'a> {
    /// The deterministic table for this term:
    /// `ShardTable::compute_with_epoch(members, rf, epoch, placement_version)`.
    /// Defines the candidate set for every shard.
    pub det: &'a ShardTable,
    /// The assignment committed by the previous term, `NUM_SHARDS` entries.
    /// `None` at genesis only — a node that holds no committed assignment
    /// must not propose (§5.2 P1-1), because anchoring on `det` for all 4096
    /// shards reverts every prior promotion and fires ~4096 migrations.
    pub prev_committed: Option<&'a [NodeId]>,
    /// Self-reported holder signals for this round.
    pub reports: &'a HolderReports,
    /// Members currently considered alive. An EMPTY set means "no liveness
    /// information", not "everyone is dead": the liveness repair is skipped
    /// rather than reverting the whole assignment.
    pub live: &'a HashSet<NodeId>,
}

/// Why a shard's entry ended up where it did. Diagnostics only — the
/// assignment is what gets committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardOutcome {
    /// The deterministic master, with no deviation in play.
    Deterministic,
    /// The previous committed master, carried forward.
    Anchored,
    /// A deviation that was created or kept this term because its
    /// justification held for [`DEVIATION_HYSTERESIS_TERMS`].
    Deviated,
    /// A deviation that was dropped because its justification no longer
    /// holds — the reversion edge that stops one term of influence becoming
    /// permanent.
    Reverted,
}

/// Result of one election.
#[derive(Debug, Clone)]
pub struct Election {
    /// `NUM_SHARDS` entries. Every entry is set — never padded, never left
    /// unset. An unset entry would encode as index 0 on the wire, handing the
    /// whole keyspace to `members[0]`.
    pub assignment: Vec<NodeId>,
    /// Per-shard outcome, same indexing as `assignment`.
    pub outcomes: Vec<ShardOutcome>,
    /// §7 — per-shard "the proposer could not prove the named master is a
    /// full holder": there WAS usable evidence for the shard and the named
    /// master did not self-report full. Deliberately false when evidence was
    /// unusable (empty or partial view) — a proposer that saw nothing has no
    /// business flagging the whole keyspace unproven.
    pub unproven: Vec<bool>,
}

impl Election {
    /// The wire-ready `(masters, unproven)` pair this election produced.
    pub fn committed(&self) -> CommittedAssignment {
        CommittedAssignment::new(self.assignment.clone(), &self.unproven)
    }

    /// Shards whose master differs from the deterministic pick.
    pub fn deviation_count(&self, det: &ShardTable) -> usize {
        self.assignment
            .iter()
            .enumerate()
            .filter(|(shard, master)| det.target_assignment(*shard as u16).master != **master)
            .count()
    }

    /// Shards whose master differs from `prev`, i.e. how many migrations this
    /// assignment would trigger.
    pub fn move_delta(&self, prev: &[NodeId]) -> usize {
        self.assignment
            .iter()
            .zip(prev.iter())
            .filter(|(next, previous)| next != previous)
            .count()
    }
}

/// The candidate set for a shard: `{det.master} ∪ det.replicas`, in that
/// order and deduplicated.
fn candidates(det: &ShardTable, shard: u16) -> Vec<NodeId> {
    let assignment = det.target_assignment(shard);
    let mut out = Vec::with_capacity(1 + assignment.replicas.len());
    out.push(assignment.master);
    for replica in &assignment.replicas {
        if !out.contains(replica) {
            out.push(*replica);
        }
    }
    out
}

/// Is `node` alive? An empty `live` set carries no information, so everything
/// is treated as alive rather than reverting the entire assignment.
fn is_live(live: &HashSet<NodeId>, node: NodeId) -> bool {
    live.is_empty() || live.contains(&node)
}

/// §5.1 tiebreak order: self-reported holder, then previous committed master,
/// then deterministic master, then lowest NodeId.
///
/// Lowest NodeId is the FINAL tiebreak, never the first. Ranking on it earlier
/// makes every candidate tie in steady state — where replication has shipped
/// to master and replicas alike, so every candidate holds the data — and the
/// lowest id then wins every shard: measured at n=3 RF=2, one node took 2731
/// shards (2.00x fair share), another 1365, the third **zero**.
fn tiebreak_rank(
    node: NodeId,
    shard: u16,
    reports: &HolderReports,
    prev_committed_master: Option<NodeId>,
    det_master: NodeId,
) -> (bool, bool, bool, std::cmp::Reverse<u64>) {
    (
        reports.is_full(node, shard),
        prev_committed_master == Some(node),
        node == det_master,
        std::cmp::Reverse(node.0),
    )
}

/// Run the election for every shard.
///
/// Returns an assignment with exactly [`NUM_SHARDS`] entries, each one a
/// candidate of that shard in `det` — so the result satisfies the
/// candidate-set rule by construction, independently of the validator that
/// re-checks it on the receiving side.
///
/// `history` is updated in place: each shard's streak advances while its
/// deviation justification holds and resets the moment it does not.
pub fn elect_committed_assignment(
    inputs: &ElectionInputs<'_>,
    history: &mut DeviationHistory,
) -> Election {
    let mut assignment = Vec::with_capacity(NUM_SHARDS);
    let mut outcomes = Vec::with_capacity(NUM_SHARDS);
    let mut unproven = Vec::with_capacity(NUM_SHARDS);

    for shard in 0..NUM_SHARDS as u16 {
        let det_master = inputs.det.target_assignment(shard).master;
        let shard_candidates = candidates(inputs.det, shard);

        // The anchor: the previous committed master, repaired when it is no
        // longer a legal candidate for this term (membership changed) or is
        // no longer alive (that IS the failover case).
        let prev = inputs
            .prev_committed
            .and_then(|prev| prev.get(shard as usize).copied());
        let mut base = match prev {
            Some(node) if shard_candidates.contains(&node) && is_live(inputs.live, node) => node,
            _ => det_master,
        };

        // Deviation preconditions (MUST, not inherited). Without a complete
        // view a candidate looks "full" merely because the real holder did
        // not report, and without any data signal there is nothing to
        // distinguish candidates at all. In either case: neither create nor
        // revert a deviation — carry the anchor. Falling back to plain `det`
        // here is what silently reverts every prior promotion.
        let all_candidates_reported = shard_candidates
            .iter()
            .all(|node| inputs.reports.reported(*node));
        let any_candidate_full = shard_candidates
            .iter()
            .any(|node| inputs.reports.is_full(*node, shard));
        let evidence_usable =
            !inputs.reports.is_empty() && all_candidates_reported && any_candidate_full;

        if !evidence_usable {
            // No usable evidence: the streak cannot advance on silence.
            history.observe(shard, false);
            let outcome = if base == det_master {
                ShardOutcome::Deterministic
            } else {
                ShardOutcome::Anchored
            };
            assignment.push(base);
            outcomes.push(outcome);
            // No usable evidence — nothing can be called unproven.
            unproven.push(false);
            continue;
        }

        // What the evidence argues for this term. Failover: the anchored
        // master reports no data while some candidate does, so promote by the
        // §5.1 order. Otherwise the anchor stands.
        let desired = if inputs.reports.is_full(base, shard) {
            base
        } else {
            shard_candidates
                .iter()
                .copied()
                .filter(|node| inputs.reports.is_full(*node, shard) && is_live(inputs.live, *node))
                .max_by_key(|node| tiebreak_rank(*node, shard, inputs.reports, prev, det_master))
                .unwrap_or(base)
        };

        // (E3) The hysteresis gates deviation CREATION; KEEPING an
        // already-committed deviation requires only that its justification
        // still holds. The two must not share the bar — field evidence
        // (scenario 14.1, post-heal reads 0/50) showed why: while a fresh
        // deviation's streak builds, sharing the bar REVERTS mastership onto
        // a det master that self-reports DATA-LESS, and with no serving
        // fence wired that master serves empty (reviewer H1's exact
        // scenario). An already-committed deviation carries a quorum's
        // agreement; holding it while its justification stands is not "one
        // term of influence becoming permanent", because the reversion edge
        // is untouched — the moment the det master proves full (its
        // migration landed) or the deviating holder stops proving full, the
        // deviation reverts. This is also what keeps a scale-up joiner from
        // being handed mastership before its data arrives: the anchored
        // holders stay masters exactly until the joiner reports full.
        //
        // CREATION keeps the full k-term bar: a single skewed view must not
        // move a master. Evaluate the shard's streak exactly ONCE per term —
        // advancing it twice in one pass lets a single term of evidence
        // clear a two-term bar.
        //
        // Without the reversion edge a deviation is self-justifying: the named
        // master receives its migration, so it holds the data from then on, so
        // it can never be proven wrong — one term of influence, hostile or
        // merely a skewed partial view, becomes permanent.
        let (master, outcome) = if desired == det_master {
            history.observe(shard, false);
            (det_master, ShardOutcome::Deterministic)
        } else {
            let justification_holds = inputs.reports.is_full(desired, shard)
                && !inputs.reports.is_full(det_master, shard);
            let streak = history.observe(shard, justification_holds);
            let was_committed_deviation = prev == Some(desired);
            if justification_holds
                && (was_committed_deviation || streak >= DEVIATION_HYSTERESIS_TERMS)
            {
                (desired, ShardOutcome::Deviated)
            } else {
                (det_master, ShardOutcome::Reverted)
            }
        };
        base = master;

        assignment.push(base);
        outcomes.push(outcome);
        // Usable evidence existed for this shard; the named master either
        // self-reported full (proven) or did not (unproven — §7's advisory
        // raise bit).
        unproven.push(!inputs.reports.is_full(base, shard));
    }

    Election {
        assignment,
        outcomes,
        unproven,
    }
}

/// §11 — the replica set that goes with `master` for `shard`.
///
/// ```text
/// replicas := det.replicas(s)
/// if master != det.master(s):
///     i := index of master in replicas
///     replicas[i] := det.master(s)        # swap: promoted out, old master in
/// ```
///
/// The swap preserves `{master} ∪ replicas == det`'s holder set, which is
/// what makes the safe-shrink guard (a shrink must not orphan a shard's only
/// holders) still valid under an elected assignment, and it guarantees
/// `master ∉ replicas`.
///
/// A `master` outside the candidate set cannot be honoured without fabricating
/// a holder set, so the deterministic replicas are returned unchanged; the
/// validator rejects such an assignment upstream.
pub fn derive_replicas(det: &ShardTable, shard: u16, master: NodeId) -> Vec<NodeId> {
    let det_assignment = det.target_assignment(shard);
    let mut replicas = det_assignment.replicas.clone();
    if master == det_assignment.master {
        return replicas;
    }
    match replicas.iter().position(|node| *node == master) {
        Some(index) => {
            replicas[index] = det_assignment.master;
            replicas
        }
        None => replicas,
    }
}

/// Build the serving table for `assignment` on top of the deterministic table.
///
/// Goes through [`ShardTable::set_master_for_shard`], which performs the same
/// swap as [`derive_replicas`] and keeps `intended_masters` in step. An entry
/// naming a non-candidate is refused there (and logged), so a malformed
/// assignment degrades to the deterministic pick for that shard rather than
/// fabricating an owner.
pub fn install_assignment(det: &mut ShardTable, assignment: &[NodeId]) {
    for (shard, master) in assignment.iter().enumerate() {
        det.set_master_for_shard(shard as u16, *master);
    }
}

/// Bytes in the per-shard `unproven` bitmap: one bit per shard.
pub const UNPROVEN_BITMAP_BYTES: usize = NUM_SHARDS / 8;

/// A committed assignment and its per-shard `unproven` bits, always carried
/// and digest-bound TOGETHER — a bitmap that could travel separately from the
/// assignment it annotates is a desync waiting to happen.
///
/// `unproven[s]` set means the PROPOSER could not prove the named master is a
/// full holder for `s` (it had usable evidence and the master did not
/// self-report full). §7: the bit is advisory and RAISE-ONLY on the receiving
/// side — it may raise a local fence, never lower one, and never overrides a
/// node's own provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedAssignment {
    /// One master per shard, `NUM_SHARDS` entries.
    pub masters: Vec<NodeId>,
    /// Fixed [`UNPROVEN_BITMAP_BYTES`] bitmap, bit `s` = shard `s` unproven.
    pub unproven: Vec<u8>,
}

impl CommittedAssignment {
    /// Build from per-shard masters and per-shard unproven flags.
    pub fn new(masters: Vec<NodeId>, unproven_flags: &[bool]) -> Self {
        let mut unproven = vec![0u8; UNPROVEN_BITMAP_BYTES];
        for (shard, flag) in unproven_flags.iter().enumerate() {
            if *flag {
                unproven[shard / 8] |= 1 << (shard % 8);
            }
        }
        Self { masters, unproven }
    }

    /// Is shard `s` flagged unproven by the proposer?
    pub fn is_unproven(&self, shard: u16) -> bool {
        let index = shard as usize / 8;
        self.unproven
            .get(index)
            .is_some_and(|byte| byte & (1 << (shard as usize % 8)) != 0)
    }

    /// Canonical wire encoding: the assignment's u16-index encoding followed
    /// by the unproven bitmap. This is the byte string the assignment digest
    /// covers, so the bitmap is digest-bound exactly like the masters.
    pub fn encode(&self, members: &[NodeId]) -> Option<Vec<u8>> {
        if self.unproven.len() != UNPROVEN_BITMAP_BYTES {
            return None;
        }
        let mut bytes = encode_assignment(&self.masters, members)?;
        bytes.extend_from_slice(&self.unproven);
        Some(bytes)
    }

    /// Decode the canonical encoding (assignment indices + bitmap).
    ///
    /// # Errors
    ///
    /// [`AssignmentRejection::WrongLength`] when the payload is not exactly
    /// `NUM_SHARDS * 2 + UNPROVEN_BITMAP_BYTES` bytes, or any error from
    /// [`decode_assignment`].
    pub fn decode(bytes: &[u8], members: &[NodeId]) -> Result<Self, AssignmentRejection> {
        let expected = NUM_SHARDS * 2 + UNPROVEN_BITMAP_BYTES;
        if bytes.len() != expected {
            return Err(AssignmentRejection::WrongLength {
                found: bytes.len(),
                expected,
            });
        }
        let masters = decode_assignment(&bytes[..NUM_SHARDS * 2], members)?;
        Ok(Self {
            masters,
            unproven: bytes[NUM_SHARDS * 2..].to_vec(),
        })
    }
}

/// §7 — the provenance inputs to [`local_fence`], all knowable locally.
///
/// Provenance, NOT data: any data-derived predicate (`record_count > 0` and
/// every variant) is false for a legitimately empty shard, so a data check
/// would fence most of a fresh cluster's keyspace — and deadlock: fenced
/// means no writes, means still empty, means still fenced.
pub struct HolderProvenance<'a> {
    /// The previous COMMITTED assignment's masters (`NUM_SHARDS` entries),
    /// or `None` at genesis. Holdership is `master ∪ derive_replicas` under
    /// the previous committed term.
    pub prev_committed: Option<&'a [NodeId]>,
    /// The deterministic table OF THE PREVIOUS committed term — replica
    /// derivation needs it to expand a master entry into the full holder set.
    pub prev_det: Option<&'a ShardTable>,
    /// Does this node hold a proven migration completion for the shard at an
    /// epoch >= the current commit's epoch? (The completion-handshake record.)
    pub proven_completion: &'a dyn Fn(u16) -> bool,
    /// The live inbound fence — raised while a migration into this node for
    /// the shard is incomplete.
    pub inbound_fenced: &'a dyn Fn(u16) -> bool,
}

/// §7 — `local_holder_check(s)`: may this node serve `s` on its own
/// provenance?
///
/// ```text
/// ( self ∈ prev_committed_holders(s)
///   OR proven completion for s at epoch >= current commit epoch
///   OR no previous committed assignment exists )       # genesis only
/// AND NOT inbound_fenced(s)
/// ```
///
/// `inbound_fenced` alone does not close the under-fence direction: a node
/// named master of a shard nobody ever sends it has a CLEAR bit and would
/// serve it empty, because that fence raises on data ARRIVAL. The
/// prev-committed-holders clause is what actually closes it.
pub fn local_holder_check(self_id: NodeId, shard: u16, provenance: &HolderProvenance<'_>) -> bool {
    let provenance_ok = match (provenance.prev_committed, provenance.prev_det) {
        (Some(prev), Some(prev_det)) => {
            let prev_master = prev.get(shard as usize).copied();
            prev_master == Some(self_id)
                || prev_master.is_some_and(|master| {
                    derive_replicas(prev_det, shard, master).contains(&self_id)
                })
                || (provenance.proven_completion)(shard)
        }
        // Genesis: no previous committed assignment exists. Every member
        // passes — an empty cluster has nothing to serve stale.
        _ => true,
    };
    provenance_ok && !(provenance.inbound_fenced)(shard)
}

/// §7 — what to do about shard `s` under the new committed assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceDecision {
    /// Serve: this node's provenance covers the shard and no advisory bit or
    /// inbound fence contradicts it.
    Serve,
    /// Withhold service and pull from `source` — the previous committed
    /// master, a concrete, real node. The clearing edge is the completion
    /// handshake of that pull, or the node's own provenance next term.
    FenceWithSource { source: NodeId },
    /// (P0-4) The fence condition holds but NO concrete pull source can be
    /// named. Alert instead of fencing: a fence with no source has no
    /// clearing edge — a `NodeId(0)` inbound is filtered out of pull-repair,
    /// and that exact shape is on record as having blocked repair.
    AlertNoSource,
}

/// §7 — `local_fence(s) := committed_unproven(s) OR NOT local_holder_check(s)`,
/// with the P0-4 no-source carve-out.
///
/// Raise-only composition: the committed bit can RAISE the fence for a node
/// whose provenance would otherwise pass, but a clear committed bit never
/// overrides a failing local check. I0-compatible — withholding service does
/// not change who the committed master is.
pub fn local_fence(
    self_id: NodeId,
    shard: u16,
    assignment: &CommittedAssignment,
    provenance: &HolderProvenance<'_>,
) -> FenceDecision {
    let fence = assignment.is_unproven(shard) || !local_holder_check(self_id, shard, provenance);
    if !fence {
        return FenceDecision::Serve;
    }
    // The pull source is the PREVIOUS committed master — always a real node
    // when one exists (rule 5 bans NodeId(0) from members). Pulling from
    // self is meaningless; treat it as no source.
    let source = provenance
        .prev_committed
        .and_then(|prev| prev.get(shard as usize).copied())
        .filter(|node| *node != self_id && *node != NodeId(0));
    match source {
        Some(source) => FenceDecision::FenceWithSource { source },
        None => FenceDecision::AlertNoSource,
    }
}

/// Rule 9 — how far ahead of this node's committed term a commit may claim to
/// be. A cluster advances one term per topology change; a jump of 16 is far
/// beyond any real burst and bounds how far a single frame can drag the term
/// space forward.
pub const MAX_TERM_JUMP: u64 = 16;

/// (E7) Rule 6's alert threshold as a ratio of fair share. NOT a rejection
/// bound: `k = 1.5` rejects the HONEST assignment on a routine
/// wipe-and-rejoin (n=3 RF=2, one node restored empty, all 1365 shards where
/// it is deterministic master deviate onto one peer, which then holds 2731
/// against a 2048 cap). Under all-or-nothing validation that wedges
/// permanently, because the deterministic proposer just re-proposes it.
pub const MASTER_COUNT_ALERT_RATIO: f64 = 1.1;

/// Why an assignment was refused.
///
/// (§6.1) Every one of these is a REJECT: metric + ERROR, and the node keeps
/// serving under its existing committed term. None of them fences. A
/// validation failure routed into a global, reboot-to-clear fence turns one
/// malformed frame — or one proposer bug — into a cluster-wide outage.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AssignmentRejection {
    /// Rule 1 — the assignment must carry exactly [`NUM_SHARDS`] entries.
    /// Never pad and never truncate: a padded tail encodes as index 0, which
    /// decodes as `members[0]` and hands that node the missing keyspace.
    #[error("assignment has {found} entries, expected {expected}")]
    WrongLength { found: usize, expected: usize },
    /// Rule 11 — a u16 index at or beyond `members.len()` names nobody.
    #[error("assignment entry for shard {shard} indexes member {index}, out of {member_count}")]
    IndexOutOfRange {
        shard: u16,
        index: u16,
        member_count: usize,
    },
    /// Rule 2 — `members` must be strictly ascending. The digest hashes
    /// members as received while the placement sorts a local copy, so an
    /// ambiguous order lets two conforming nodes derive different assignments
    /// from one digest-matching commit.
    #[error("members are not strictly ascending")]
    MembersNotAscending,
    /// Rule 5 — `NodeId(0)` collides with three live sentinels (stale-table
    /// marker, inbound-fence wildcard, filtered out of migration-source
    /// selection), so a shard assigned to it is masterless AND unrepairable.
    #[error("members contain NodeId(0)")]
    MemberIsNodeZero,
    /// Rule 3 — every entry must be a committed member of this term.
    #[error("shard {shard} is assigned to {node:?}, not a member of this term")]
    EntryNotAMember { shard: u16, node: NodeId },
    /// Rule 4 — the load-bearing containment: an entry must be one of the
    /// shard's RF candidates under the deterministic placement. This is the
    /// wire-level equivalent of `set_master_for_shard`'s refusal, and it is a
    /// pure function of digest-bound inputs, so every voter agrees on it.
    #[error("shard {shard} is assigned to {node:?}, not one of its candidates")]
    EntryNotACandidate { shard: u16, node: NodeId },
    /// Rule 7 — the derived replica set must not contain the master.
    #[error("shard {shard} lists its master {node:?} as a replica")]
    MasterInReplicas { shard: u16, node: NodeId },
    /// Rule 8 — a sanity check on a plaintext, self-declared field. NOT
    /// authorization: nothing verifies the sender.
    #[error("proposer {proposer:?} is not a member of this term")]
    ProposerNotAMember { proposer: NodeId },
    /// Rule 9 — checked BEFORE hashing, so a wild term cannot make a node do
    /// the digest work.
    #[error("term {term} exceeds committed term {committed} by more than {max}")]
    TermJumpTooLarge { term: u64, committed: u64, max: u64 },
}

/// What a valid assignment looks like, quantified. Rule 6 and the deleted
/// move-delta rule both live here as measurements rather than gates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AssignmentStats {
    /// Highest per-node master count divided by fair share
    /// (`NUM_SHARDS / members`). Exported as
    /// `assignment_master_count_ratio`; alert above
    /// [`MASTER_COUNT_ALERT_RATIO`].
    pub master_count_ratio: f64,
    /// Shards whose master differs from the previous committed assignment —
    /// i.e. migrations this assignment triggers. Exported as
    /// `assignment_move_delta_shards`.
    ///
    /// The move-delta RULE is deliberately deleted: it rejected the v1→v2
    /// placement upgrade outright (which reshuffles every shard with
    /// `members` unchanged), blocked the very repair this design exists to
    /// perform, and was bypassable by changing membership by one node. It
    /// survives only as this number.
    pub move_delta_shards: usize,
}

/// Rejected assignments, by rule. Read via [`assignment_rejected_total`].
static ASSIGNMENT_REJECTED_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Valid assignments whose worst per-node master count exceeded
/// [`MASTER_COUNT_ALERT_RATIO`]. Read via [`assignment_master_count_alerts_total`].
static ASSIGNMENT_MASTER_COUNT_ALERTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Assignments refused by [`validate_assignment`].
///
/// Every increment is a reject, never a fence: a climbing counter means a peer
/// is producing assignments this node will not install, and the node carries
/// on serving its existing committed term.
pub fn assignment_rejected_total() -> u64 {
    ASSIGNMENT_REJECTED_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// Valid assignments that concentrated mastership above the alert ratio.
///
/// (E7) Deliberately not a rejection — the honest assignment exceeds a hard
/// bound on a routine wipe-and-rejoin, and rejecting it wedges the cluster.
pub fn assignment_master_count_alerts_total() -> u64 {
    ASSIGNMENT_MASTER_COUNT_ALERTS.load(std::sync::atomic::Ordering::Relaxed)
}

/// §7 — serving-side fences raised (shards withheld from service with a
/// concrete pull source). Read via [`serving_fence_raised_total`].
static SERVING_FENCE_RAISED_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// §7 (P0-4) — fence conditions that held with NO nameable pull source, so
/// the node alerted and SERVED instead of fencing. Read via
/// [`serving_fence_no_source_alerts_total`].
static SERVING_FENCE_NO_SOURCE_ALERTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// §7 — shards the serving-side fence withheld from service, cumulatively
/// across activations. Each raise names a concrete pull source (the previous
/// committed master); the clearing edge is the completion handshake or the
/// node's own provenance at the next activation.
pub fn serving_fence_raised_total() -> u64 {
    SERVING_FENCE_RAISED_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// §7 (P0-4) — shards whose fence condition held with no concrete source to
/// pull from: the node alerted and served. A climbing counter means committed
/// unproven bits (or missing provenance) with nothing to repair from — an
/// operator signal, never a refusal.
pub fn serving_fence_no_source_alerts_total() -> u64 {
    SERVING_FENCE_NO_SOURCE_ALERTS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Count `n` §7 serving fences raised (coordinator activation path).
pub(crate) fn note_serving_fences_raised(n: u64) {
    SERVING_FENCE_RAISED_TOTAL.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}

/// Count `n` §7 no-source alert-and-serve events (coordinator activation path).
pub(crate) fn note_serving_fence_no_source_alerts(n: u64) {
    SERVING_FENCE_NO_SOURCE_ALERTS.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}

/// The canonical assignment encoding: a fixed [`NUM_SHARDS`] array of u16
/// indices into `members` **as received**, little-endian. 8 KiB exactly,
/// never length-prefixed — a length field is one more thing a sender can lie
/// about, and the length is already implied.
///
/// Returns `None` when an entry is not a member (it has no index) or when
/// `members` exceeds what a u16 index can address.
pub fn encode_assignment(assignment: &[NodeId], members: &[NodeId]) -> Option<Vec<u8>> {
    if assignment.len() != NUM_SHARDS || members.len() > u16::MAX as usize {
        return None;
    }
    let mut index_of: HashMap<NodeId, u16> = HashMap::with_capacity(members.len());
    for (index, member) in members.iter().enumerate() {
        index_of.entry(*member).or_insert(index as u16);
    }
    let mut buf = Vec::with_capacity(NUM_SHARDS * 2);
    for master in assignment {
        buf.extend_from_slice(&index_of.get(master)?.to_le_bytes());
    }
    Some(buf)
}

/// Decode the canonical encoding, enforcing rules 1 and 11.
///
/// # Errors
///
/// [`AssignmentRejection::WrongLength`] when the payload is not exactly
/// `NUM_SHARDS * 2` bytes, and [`AssignmentRejection::IndexOutOfRange`] when
/// an index names no member. Both are rejections, never fences.
pub fn decode_assignment(
    bytes: &[u8],
    members: &[NodeId],
) -> Result<Vec<NodeId>, AssignmentRejection> {
    if bytes.len() != NUM_SHARDS * 2 {
        return Err(AssignmentRejection::WrongLength {
            found: bytes.len(),
            expected: NUM_SHARDS * 2,
        });
    }
    let mut out = Vec::with_capacity(NUM_SHARDS);
    for (shard, chunk) in bytes.chunks_exact(2).enumerate() {
        let index = u16::from_le_bytes([chunk[0], chunk[1]]);
        let member = members
            .get(index as usize)
            .ok_or(AssignmentRejection::IndexOutOfRange {
                shard: shard as u16,
                index,
                member_count: members.len(),
            })?;
        out.push(*member);
    }
    Ok(out)
}

/// Digest over the canonical encoding. Recipients compute this from the bytes
/// they RECEIVED — no path may trust a shipped hash, or the binding is
/// vacuous: ship `(A, H(B))` to one node and `(A', H(B))` to another and both
/// match their own advertised digest.
pub fn assignment_digest(encoded: &[u8]) -> [u8; 32] {
    crate::cluster::auth::sha256(encoded)
}

/// Everything rules 3–9 need, all of it digest-bound or locally committed.
pub struct AssignmentProposal<'a> {
    /// Decoded assignment, `NUM_SHARDS` entries.
    pub assignment: &'a [NodeId],
    /// `commit.members`, as received (NOT sorted locally — rule 2 requires
    /// the received order to already be strictly ascending).
    pub members: &'a [NodeId],
    /// The deterministic table for this term, built from the same digest-bound
    /// `(members, rf, placement_version)` the commit carries.
    pub det: &'a ShardTable,
    /// `commit.proposer`.
    pub proposer: NodeId,
    /// `commit.term`.
    pub term: u64,
}

/// Run rules 1–9 and 11 over a decoded assignment (rule 10, the membership
/// growth bound, is enforced on `commit.members` by the commit gate).
///
/// # Errors
///
/// Any [`AssignmentRejection`]. Every one is reject-and-count: the caller must
/// refuse the commit and keep serving its existing term. No path here fences.
///
/// On success returns [`AssignmentStats`]; a master-count ratio above
/// [`MASTER_COUNT_ALERT_RATIO`] logs and counts but does NOT fail (E7).
pub fn validate_assignment(
    proposal: &AssignmentProposal<'_>,
    committed_term: u64,
    prev_committed: Option<&[NodeId]>,
) -> Result<AssignmentStats, AssignmentRejection> {
    let reject = |rejection: AssignmentRejection| -> AssignmentRejection {
        ASSIGNMENT_REJECTED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(
            term = proposal.term,
            proposer = proposal.proposer.0,
            reason = %rejection,
            "cluster: rejecting committed assignment — the node keeps serving its \
             existing term (reject, not fence)",
        );
        rejection
    };

    // Rule 9 FIRST — before any hashing or per-shard work, so a wild term
    // cannot make this node do the expensive part.
    if proposal.term > committed_term.saturating_add(MAX_TERM_JUMP) {
        return Err(reject(AssignmentRejection::TermJumpTooLarge {
            term: proposal.term,
            committed: committed_term,
            max: MAX_TERM_JUMP,
        }));
    }

    // Rule 2 — strictly ascending members (sorted and duplicate-free in one
    // check). Re-checked here so the validator is self-contained rather than
    // inheriting a guarantee from its caller.
    if proposal.members.windows(2).any(|w| w[0].0 >= w[1].0) {
        return Err(reject(AssignmentRejection::MembersNotAscending));
    }

    // Rule 5 — NodeId(0) anywhere in the member set.
    if proposal.members.contains(&NodeId(0)) {
        return Err(reject(AssignmentRejection::MemberIsNodeZero));
    }

    // Rule 8 — sanity only; the sender is never verified.
    if !proposal.members.contains(&proposal.proposer) {
        return Err(reject(AssignmentRejection::ProposerNotAMember {
            proposer: proposal.proposer,
        }));
    }

    // Rule 1 — exactly NUM_SHARDS entries.
    if proposal.assignment.len() != NUM_SHARDS {
        return Err(reject(AssignmentRejection::WrongLength {
            found: proposal.assignment.len(),
            expected: NUM_SHARDS,
        }));
    }

    let member_set: HashSet<NodeId> = proposal.members.iter().copied().collect();
    let mut master_counts: HashMap<NodeId, usize> = HashMap::with_capacity(proposal.members.len());

    for (shard, master) in proposal.assignment.iter().enumerate() {
        let shard = shard as u16;

        // Rule 3 — a committed member of this term.
        if !member_set.contains(master) {
            return Err(reject(AssignmentRejection::EntryNotAMember {
                shard,
                node: *master,
            }));
        }

        // Rule 4 — one of the shard's RF candidates. This is the entire
        // containment: without it a proposer can name any member for any
        // shard, including one holding none of that shard's data.
        if !candidates(proposal.det, shard).contains(master) {
            return Err(reject(AssignmentRejection::EntryNotACandidate {
                shard,
                node: *master,
            }));
        }

        // Rule 7 — the derived replica set must not contain the master.
        if derive_replicas(proposal.det, shard, *master).contains(master) {
            return Err(reject(AssignmentRejection::MasterInReplicas {
                shard,
                node: *master,
            }));
        }

        *master_counts.entry(*master).or_insert(0) += 1;
    }

    // Rule 6 (E7) — measure, alert, do NOT reject.
    let fair_share = NUM_SHARDS as f64 / proposal.members.len().max(1) as f64;
    let worst = master_counts.values().copied().max().unwrap_or(0);
    let master_count_ratio = worst as f64 / fair_share;
    if master_count_ratio > MASTER_COUNT_ALERT_RATIO {
        ASSIGNMENT_MASTER_COUNT_ALERTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(
            term = proposal.term,
            proposer = proposal.proposer.0,
            master_count_ratio,
            worst_node_shards = worst,
            fair_share,
            "cluster: committed assignment concentrates mastership above the alert \
             ratio — accepted (rejecting it would wedge a legitimate rejoin)",
        );
    }

    let move_delta_shards = prev_committed
        .map(|prev| {
            proposal
                .assignment
                .iter()
                .zip(prev.iter())
                .filter(|(next, previous)| next != previous)
                .count()
        })
        .unwrap_or(0);

    Ok(AssignmentStats {
        master_count_ratio,
        move_delta_shards,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().map(|&id| NodeId(id)).collect()
    }

    fn det_table(ids: &[u64], rf: u8) -> ShardTable {
        ShardTable::compute_with_epoch(&members(ids), rf, 1, 1)
    }

    fn live(ids: &[u64]) -> HashSet<NodeId> {
        members(ids).into_iter().collect()
    }

    /// Every node reports, and every candidate reports full — the steady
    /// state of a healthy cluster, where replication has shipped to master
    /// and replicas alike.
    fn everyone_full(det: &ShardTable, ids: &[u64]) -> HolderReports {
        let nodes = members(ids);
        let mut entries = Vec::new();
        for shard in 0..NUM_SHARDS as u16 {
            for node in candidates(det, shard) {
                entries.push((node, shard, 1u64));
            }
        }
        HolderReports::from_entries(nodes, entries)
    }

    fn det_assignment(det: &ShardTable) -> Vec<NodeId> {
        (0..NUM_SHARDS as u16)
            .map(|shard| det.target_assignment(shard).master)
            .collect()
    }

    /// Genesis with no signals must reproduce the deterministic table exactly.
    /// That is the fixed point the whole design is built to preserve.
    #[test]
    fn genesis_with_no_evidence_reproduces_the_deterministic_table() {
        let det = det_table(&[1, 2, 3], 2);
        let reports = HolderReports::default();
        let mut history = DeviationHistory::new();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: None,
                reports: &reports,
                live: &live(&[1, 2, 3]),
            },
            &mut history,
        );
        assert_eq!(election.assignment, det_assignment(&det));
        assert_eq!(election.deviation_count(&det), 0);
        assert_eq!(election.assignment.len(), NUM_SHARDS);
    }

    /// An election with no new evidence must reproduce its input. Without
    /// this the anchor is not a fixed point and the cluster churns forever.
    #[test]
    fn no_evidence_preserves_the_previous_committed_assignment() {
        let det = det_table(&[1, 2, 3], 2);
        // A previous assignment that deviates on shard 0.
        let mut prev = det_assignment(&det);
        let shard0_candidates = candidates(&det, 0);
        let deviating = shard0_candidates[1];
        prev[0] = deviating;

        let reports = HolderReports::default();
        let mut history = DeviationHistory::new();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &reports,
                live: &live(&[1, 2, 3]),
            },
            &mut history,
        );
        assert_eq!(
            election.assignment, prev,
            "an empty view must neither create nor revert deviations",
        );
        assert_eq!(election.outcomes[0], ShardOutcome::Anchored);
    }

    /// A partial view — some candidate did not answer — must not manufacture
    /// a deviation: the silent node may be the real holder.
    #[test]
    fn a_partial_view_cannot_create_a_deviation() {
        let det = det_table(&[1, 2, 3], 2);
        let prev = det_assignment(&det);
        let shard = 0u16;
        let shard_candidates = candidates(&det, shard);
        let det_master = shard_candidates[0];
        let other = shard_candidates[1];

        // `other` reports full; the deterministic master never answered.
        let reports = HolderReports::from_entries(vec![other], vec![(other, shard, 5u64)]);
        let mut history = DeviationHistory::new();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &reports,
                live: &live(&[1, 2, 3]),
            },
            &mut history,
        );
        assert_eq!(
            election.assignment[shard as usize], det_master,
            "a candidate that did not report cannot be assumed data-less",
        );
    }

    /// The no-data skip: nobody reports data for the shard (a fresh scale-up),
    /// so there is nothing to distinguish candidates.
    #[test]
    fn no_candidate_reporting_data_leaves_the_shard_alone() {
        let det = det_table(&[1, 2, 3], 2);
        let prev = det_assignment(&det);
        let shard = 0u16;
        let shard_candidates = candidates(&det, shard);
        let entries: Vec<_> = shard_candidates
            .iter()
            .map(|node| (*node, shard, 0u64))
            .collect();
        let reports = HolderReports::from_entries(shard_candidates.clone(), entries);

        let mut history = DeviationHistory::new();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &reports,
                live: &live(&[1, 2, 3]),
            },
            &mut history,
        );
        assert_eq!(election.assignment[shard as usize], prev[shard as usize]);
    }

    /// Failover: the deterministic master reports data-less while a replica
    /// reports full. The promotion must survive the hysteresis window and
    /// must not land before it.
    #[test]
    fn failover_promotes_a_full_candidate_after_hysteresis() {
        let det = det_table(&[1, 2, 3], 2);
        let prev = det_assignment(&det);
        let shard = 0u16;
        let shard_candidates = candidates(&det, shard);
        let det_master = shard_candidates[0];
        let holder = shard_candidates[1];

        let reports = HolderReports::from_entries(
            shard_candidates.clone(),
            vec![(det_master, shard, 0u64), (holder, shard, 9u64)],
        );
        let mut history = DeviationHistory::new();
        let inputs = ElectionInputs {
            det: &det,
            prev_committed: Some(&prev),
            reports: &reports,
            live: &live(&[1, 2, 3]),
        };

        let first = elect_committed_assignment(&inputs, &mut history);
        assert_eq!(
            first.assignment[shard as usize], det_master,
            "one term of evidence must not move a master",
        );

        let second = elect_committed_assignment(&inputs, &mut history);
        assert_eq!(
            second.assignment[shard as usize], holder,
            "a justification that held for the full window promotes",
        );
        assert_eq!(second.outcomes[shard as usize], ShardOutcome::Deviated);
    }

    /// (E3) The reversion edge. A deviation whose justification stops holding
    /// must be dropped — otherwise one term of influence is permanent,
    /// laundered by every honest term afterwards.
    #[test]
    fn a_deviation_reverts_once_its_justification_stops_holding() {
        let det = det_table(&[1, 2, 3], 2);
        let shard = 0u16;
        let shard_candidates = candidates(&det, shard);
        let det_master = shard_candidates[0];
        let deviating = shard_candidates[1];

        let mut prev = det_assignment(&det);
        prev[shard as usize] = deviating;

        // The deterministic master now reports full too — the deviation's
        // reason ("det master is data-less") no longer holds.
        let reports = HolderReports::from_entries(
            shard_candidates.clone(),
            vec![(det_master, shard, 4u64), (deviating, shard, 7u64)],
        );
        let mut history = DeviationHistory::new();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &reports,
                live: &live(&[1, 2, 3]),
            },
            &mut history,
        );
        assert_eq!(
            election.assignment[shard as usize], det_master,
            "a self-justifying deviation is exactly what E3 forbids",
        );
        assert_eq!(election.outcomes[shard as usize], ShardOutcome::Reverted);
        assert_eq!(history.streak(shard), 0);
    }

    /// A deviation whose justification keeps holding is kept, term after term.
    #[test]
    fn a_justified_deviation_is_kept_across_terms() {
        let det = det_table(&[1, 2, 3], 2);
        let shard = 0u16;
        let shard_candidates = candidates(&det, shard);
        let det_master = shard_candidates[0];
        let deviating = shard_candidates[1];

        let mut prev = det_assignment(&det);
        prev[shard as usize] = deviating;

        let reports = HolderReports::from_entries(
            shard_candidates.clone(),
            vec![(det_master, shard, 0u64), (deviating, shard, 7u64)],
        );
        let mut history = DeviationHistory::new();
        let inputs = ElectionInputs {
            det: &det,
            prev_committed: Some(&prev),
            reports: &reports,
            live: &live(&[1, 2, 3]),
        };

        // An already-COMMITTED deviation whose justification holds is kept
        // from the FIRST term — the hysteresis gates creation, not keeping.
        // Reverting it while a streak builds would land mastership on a det
        // master that self-reports data-less, which (with no serving fence)
        // serves reads empty: scenario 14.1's post-heal 0/50.
        let first = elect_committed_assignment(&inputs, &mut history);
        assert_eq!(
            first.assignment[shard as usize], deviating,
            "an anchored, justified deviation must be kept immediately",
        );
        let second = elect_committed_assignment(&inputs, &mut history);
        assert_eq!(second.assignment[shard as usize], deviating);
        let third = elect_committed_assignment(&inputs, &mut history);
        assert_eq!(third.assignment[shard as usize], deviating);
        assert_eq!(third.outcomes[shard as usize], ShardOutcome::Deviated);
    }

    /// Scenario 14.1's regression pinned: post-heal, the anchored holders
    /// keep mastership while the det masters still self-report data-less —
    /// reads never route to an empty master. And the scale-up variant: a
    /// data-less joiner is NOT handed its det masterships until it reports
    /// full, at which point the deviation reverts (the reversion edge).
    #[test]
    fn a_committed_deviation_never_reverts_onto_a_dataless_master() {
        let det = det_table(&[1, 2, 3], 2);
        let shard = 0u16;
        let shard_candidates = candidates(&det, shard);
        let det_master = shard_candidates[0];
        let holder = shard_candidates[1];

        let mut prev = det_assignment(&det);
        prev[shard as usize] = holder; // committed deviation onto the holder

        // det master data-less, holder full: justification holds → the
        // committed deviation is KEPT from the very first term.
        let dataless = HolderReports::from_entries(
            shard_candidates.clone(),
            vec![(det_master, shard, 0u64), (holder, shard, 9u64)],
        );
        let mut history = DeviationHistory::new();
        let kept = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &dataless,
                live: &live(&[1, 2, 3]),
            },
            &mut history,
        );
        assert_eq!(
            kept.assignment[shard as usize], holder,
            "mastership must stay on the proven holder, never an empty master",
        );
        assert_eq!(kept.outcomes[shard as usize], ShardOutcome::Deviated);

        // The det master's migration lands (it now reports full): the
        // justification fails and the deviation reverts immediately.
        let filled = HolderReports::from_entries(
            shard_candidates.clone(),
            vec![(det_master, shard, 5u64), (holder, shard, 9u64)],
        );
        let mut history = DeviationHistory::new();
        let reverted = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &filled,
                live: &live(&[1, 2, 3]),
            },
            &mut history,
        );
        assert_eq!(
            reverted.assignment[shard as usize], det_master,
            "the reversion edge stands: a full det master reclaims immediately",
        );
        assert_eq!(reverted.outcomes[shard as usize], ShardOutcome::Reverted);
    }

    /// A previously committed master that is no longer a candidate for this
    /// term (membership changed under it) is repaired to the deterministic
    /// pick — the anchor must never name a node outside the candidate set.
    #[test]
    fn an_anchor_outside_the_candidate_set_is_repaired() {
        let det = det_table(&[1, 2, 3], 2);
        let mut prev = det_assignment(&det);
        let shard = 0u16;
        prev[shard as usize] = NodeId(99); // never a member

        let reports = HolderReports::default();
        let mut history = DeviationHistory::new();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &reports,
                live: &live(&[1, 2, 3]),
            },
            &mut history,
        );
        assert_eq!(
            election.assignment[shard as usize],
            det.target_assignment(shard).master,
        );
    }

    /// A dead anchor is repaired to the deterministic pick — this is failover
    /// in the case where the old master is simply gone.
    #[test]
    fn a_dead_anchor_is_repaired() {
        let det = det_table(&[1, 2, 3], 2);
        let shard = 0u16;
        let shard_candidates = candidates(&det, shard);
        let deviating = shard_candidates[1];
        let mut prev = det_assignment(&det);
        prev[shard as usize] = deviating;

        let reports = HolderReports::default();
        let mut history = DeviationHistory::new();
        let alive: HashSet<NodeId> = members(&[1, 2, 3])
            .into_iter()
            .filter(|node| *node != deviating)
            .collect();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &reports,
                live: &alive,
            },
            &mut history,
        );
        assert_eq!(
            election.assignment[shard as usize],
            det.target_assignment(shard).master,
        );
    }

    /// Every entry is always set. An unset entry encodes as u16 index 0 on the
    /// wire, which decodes as `members[0]` — handing that node the whole
    /// keyspace.
    #[test]
    fn every_shard_gets_an_entry_from_the_candidate_set() {
        let det = det_table(&[1, 2, 3, 4, 5], 3);
        let reports = HolderReports::default();
        let mut history = DeviationHistory::new();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: None,
                reports: &reports,
                live: &HashSet::new(),
            },
            &mut history,
        );
        assert_eq!(election.assignment.len(), NUM_SHARDS);
        for shard in 0..NUM_SHARDS as u16 {
            let master = election.assignment[shard as usize];
            assert!(
                candidates(&det, shard).contains(&master),
                "shard {shard} assigned to a non-candidate",
            );
        }
    }

    /// The I16 invariant, and the regression that killed rev 2: in an honest
    /// steady state no node may exceed its fair share by more than ~10%.
    ///
    /// Ranking on lowest NodeId before the stickiness tiebreaks gave 2731 /
    /// 1365 / 0 at n=3 RF=2 — one node with 2x fair share and one with none,
    /// plus 1365-2048 migrations per term.
    #[test]
    fn an_honest_election_stays_within_fair_share() {
        let ids = [1u64, 2, 3];
        let det = det_table(&ids, 2);
        let reports = everyone_full(&det, &ids);
        let prev = det_assignment(&det);
        let mut history = DeviationHistory::new();

        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &reports,
                live: &live(&ids),
            },
            &mut history,
        );

        let mut counts: HashMap<NodeId, usize> = HashMap::new();
        for master in &election.assignment {
            *counts.entry(*master).or_insert(0) += 1;
        }
        let fair_share = NUM_SHARDS as f64 / ids.len() as f64;
        for id in ids {
            let count = counts.get(&NodeId(id)).copied().unwrap_or(0);
            assert!(
                (count as f64) <= fair_share * 1.1,
                "node {id} holds {count} shards, above 1.1x the {fair_share} fair share",
            );
            assert!(count > 0, "node {id} holds no shards at all");
        }
        assert_eq!(
            election.move_delta(&prev),
            0,
            "a healthy steady state must trigger no migrations",
        );
    }

    /// §5.1 — a self-reported holder outranks the previous committed master,
    /// which outranks the deterministic master, which outranks a lower NodeId.
    #[test]
    fn tiebreak_order_puts_the_holder_first_and_node_id_last() {
        let shard = 7u16;
        let det_master = NodeId(2);
        let prev_master = NodeId(3);
        let holder = NodeId(9);
        let reports = HolderReports::from_entries(
            vec![det_master, prev_master, holder],
            vec![(holder, shard, 1u64)],
        );

        let rank = |node| tiebreak_rank(node, shard, &reports, Some(prev_master), det_master);
        assert!(
            rank(holder) > rank(prev_master),
            "a self-reported holder must outrank the previous committed master",
        );
        assert!(
            rank(prev_master) > rank(det_master),
            "the previous committed master must outrank the deterministic one",
        );
        assert!(
            rank(det_master) > rank(NodeId(1)),
            "the deterministic master must outrank a lower, unrelated NodeId",
        );
        // The final tiebreak, with everything else equal.
        let plain = HolderReports::from_entries(vec![NodeId(4), NodeId(5)], vec![]);
        let plain_rank = |node| tiebreak_rank(node, shard, &plain, None, NodeId(0));
        assert!(
            plain_rank(NodeId(4)) > plain_rank(NodeId(5)),
            "with all else equal the lowest NodeId wins",
        );
    }

    fn proposal<'a>(
        assignment: &'a [NodeId],
        members: &'a [NodeId],
        det: &'a ShardTable,
    ) -> AssignmentProposal<'a> {
        AssignmentProposal {
            assignment,
            members,
            det,
            proposer: members[0],
            term: 5,
        }
    }

    /// The election's own output must pass its own validator — the two must
    /// never drift apart.
    #[test]
    fn an_elected_assignment_validates() {
        let ids = [1u64, 2, 3, 4];
        let det = det_table(&ids, 3);
        let reports = everyone_full(&det, &ids);
        let prev = det_assignment(&det);
        let mut history = DeviationHistory::new();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &reports,
                live: &live(&ids),
            },
            &mut history,
        );
        let member_list = members(&ids);
        let stats = validate_assignment(
            &proposal(&election.assignment, &member_list, &det),
            4,
            Some(&prev),
        )
        .expect("the election's own output must validate");
        assert!(stats.master_count_ratio <= MASTER_COUNT_ALERT_RATIO);
        assert_eq!(stats.move_delta_shards, 0);
    }

    /// Rule 4 — the containment rule. A member that is not one of the shard's
    /// candidates cannot be its master, however well-formed the frame is.
    #[test]
    fn rule_4_rejects_an_entry_outside_the_candidate_set() {
        let ids = [1u64, 2, 3, 4, 5];
        let det = det_table(&ids, 2);
        let member_list = members(&ids);
        let mut assignment = det_assignment(&det);

        let shard = 0u16;
        let outsider = member_list
            .iter()
            .copied()
            .find(|node| !candidates(&det, shard).contains(node))
            .expect("with RF=2 of 5 members some member is not a candidate");
        assignment[shard as usize] = outsider;

        let before = assignment_rejected_total();
        let err = validate_assignment(&proposal(&assignment, &member_list, &det), 4, None)
            .expect_err("a non-candidate master must be rejected");
        assert_eq!(
            err,
            AssignmentRejection::EntryNotACandidate {
                shard,
                node: outsider
            }
        );
        assert!(
            assignment_rejected_total() > before,
            "the rejection must be counted",
        );
    }

    /// Rule 3 — a node outside the member set entirely.
    #[test]
    fn rule_3_rejects_a_non_member() {
        let ids = [1u64, 2, 3];
        let det = det_table(&ids, 2);
        let member_list = members(&ids);
        let mut assignment = det_assignment(&det);
        assignment[0] = NodeId(99);
        let err = validate_assignment(&proposal(&assignment, &member_list, &det), 4, None)
            .expect_err("a non-member master must be rejected");
        assert_eq!(
            err,
            AssignmentRejection::EntryNotAMember {
                shard: 0,
                node: NodeId(99)
            }
        );
    }

    /// Rule 1 — never pad, never truncate. A short assignment used to be
    /// padded with zeros, and index 0 decodes as `members[0]`.
    #[test]
    fn rule_1_rejects_a_short_assignment() {
        let ids = [1u64, 2, 3];
        let det = det_table(&ids, 2);
        let member_list = members(&ids);
        let short = det_assignment(&det)[..NUM_SHARDS - 1].to_vec();
        let err = validate_assignment(&proposal(&short, &member_list, &det), 4, None)
            .expect_err("a short assignment must be rejected");
        assert_eq!(
            err,
            AssignmentRejection::WrongLength {
                found: NUM_SHARDS - 1,
                expected: NUM_SHARDS
            }
        );
    }

    /// Rules 2, 5, 8, 9 — the cheap structural gates.
    #[test]
    fn structural_rules_reject_their_own_violations() {
        let ids = [1u64, 2, 3];
        let det = det_table(&ids, 2);
        let assignment = det_assignment(&det);

        // Rule 2 — not strictly ascending (duplicate).
        let unsorted = members(&[1, 1, 2]);
        assert_eq!(
            validate_assignment(&proposal(&assignment, &unsorted, &det), 4, None).unwrap_err(),
            AssignmentRejection::MembersNotAscending,
        );

        // Rule 5 — NodeId(0).
        let with_zero = members(&[0, 1, 2]);
        assert_eq!(
            validate_assignment(&proposal(&assignment, &with_zero, &det), 4, None).unwrap_err(),
            AssignmentRejection::MemberIsNodeZero,
        );

        // Rule 8 — proposer outside the member set.
        let member_list = members(&ids);
        let foreign = AssignmentProposal {
            assignment: &assignment,
            members: &member_list,
            det: &det,
            proposer: NodeId(42),
            term: 5,
        };
        assert_eq!(
            validate_assignment(&foreign, 4, None).unwrap_err(),
            AssignmentRejection::ProposerNotAMember {
                proposer: NodeId(42)
            },
        );

        // Rule 9 — a wild term jump, checked before any per-shard work.
        let far = AssignmentProposal {
            assignment: &assignment,
            members: &member_list,
            det: &det,
            proposer: member_list[0],
            term: 4 + MAX_TERM_JUMP + 1,
        };
        assert_eq!(
            validate_assignment(&far, 4, None).unwrap_err(),
            AssignmentRejection::TermJumpTooLarge {
                term: 4 + MAX_TERM_JUMP + 1,
                committed: 4,
                max: MAX_TERM_JUMP,
            },
        );
    }

    /// (E7) Rule 6 measures and alerts; it must NOT reject. The honest
    /// assignment after a wipe-and-rejoin concentrates mastership, and
    /// rejecting it wedges the cluster permanently — the deterministic
    /// proposer simply re-proposes the same thing.
    #[test]
    fn rule_6_alerts_on_concentration_but_still_accepts() {
        let ids = [1u64, 2, 3];
        let det = det_table(&ids, 2);
        let member_list = members(&ids);

        // Every shard whose candidates include member[0] goes to member[0].
        let mut assignment = det_assignment(&det);
        for shard in 0..NUM_SHARDS as u16 {
            if candidates(&det, shard).contains(&member_list[0]) {
                assignment[shard as usize] = member_list[0];
            }
        }

        let alerts_before = assignment_master_count_alerts_total();
        let stats = validate_assignment(&proposal(&assignment, &member_list, &det), 4, None)
            .expect("concentration must NOT be a rejection");
        assert!(
            stats.master_count_ratio > MASTER_COUNT_ALERT_RATIO,
            "precondition: this assignment is concentrated, ratio {}",
            stats.master_count_ratio,
        );
        // Counters are process-global and other tests run in parallel, so
        // assert movement, not an exact delta.
        assert!(
            assignment_master_count_alerts_total() > alerts_before,
            "concentration must raise the alert counter",
        );
    }

    /// The move delta is reported, never enforced. The rule that enforced it
    /// rejected the v1→v2 placement upgrade outright.
    #[test]
    fn move_delta_is_reported_not_enforced() {
        let ids = [1u64, 2, 3];
        let det = det_table(&ids, 2);
        let member_list = members(&ids);
        let prev = det_assignment(&det);

        // Move every shard that can move onto its first replica.
        let mut assignment = prev.clone();
        for shard in 0..NUM_SHARDS as u16 {
            let shard_candidates = candidates(&det, shard);
            if shard_candidates.len() > 1 {
                assignment[shard as usize] = shard_candidates[1];
            }
        }

        let stats = validate_assignment(&proposal(&assignment, &member_list, &det), 4, Some(&prev))
            .expect("a large move delta must not be a rejection");
        assert!(
            stats.move_delta_shards > NUM_SHARDS / 2,
            "precondition: this assignment moves most shards",
        );
    }

    /// The canonical encoding round-trips, and rule 11 rejects an index that
    /// names nobody.
    #[test]
    fn assignment_encoding_round_trips_and_bounds_its_indices() {
        let ids = [1u64, 2, 3];
        let det = det_table(&ids, 2);
        let member_list = members(&ids);
        let assignment = det_assignment(&det);

        let encoded = encode_assignment(&assignment, &member_list).expect("must encode");
        assert_eq!(
            encoded.len(),
            NUM_SHARDS * 2,
            "8 KiB, never length-prefixed"
        );
        assert_eq!(
            decode_assignment(&encoded, &member_list).expect("must decode"),
            assignment,
        );

        // Rule 11 — an index at members.len() names nobody.
        let mut bad = encoded.clone();
        bad[0..2].copy_from_slice(&(member_list.len() as u16).to_le_bytes());
        assert_eq!(
            decode_assignment(&bad, &member_list).unwrap_err(),
            AssignmentRejection::IndexOutOfRange {
                shard: 0,
                index: member_list.len() as u16,
                member_count: member_list.len(),
            },
        );

        // Rule 1 at the decode layer — a truncated payload is not padded.
        assert_eq!(
            decode_assignment(&encoded[..encoded.len() - 2], &member_list).unwrap_err(),
            AssignmentRejection::WrongLength {
                found: NUM_SHARDS * 2 - 2,
                expected: NUM_SHARDS * 2,
            },
        );
    }

    /// The digest is computed from the bytes, so two different assignments
    /// cannot share one. Recipients must recompute it from what they received
    /// rather than trusting a shipped hash.
    #[test]
    fn the_assignment_digest_distinguishes_different_assignments() {
        let ids = [1u64, 2, 3];
        let det = det_table(&ids, 2);
        let member_list = members(&ids);
        let a = det_assignment(&det);
        let mut b = a.clone();
        b[0] = candidates(&det, 0)[1];

        let encoded_a = encode_assignment(&a, &member_list).expect("encode a");
        let encoded_b = encode_assignment(&b, &member_list).expect("encode b");
        assert_ne!(encoded_a, encoded_b);
        assert_ne!(assignment_digest(&encoded_a), assignment_digest(&encoded_b));
        assert_eq!(assignment_digest(&encoded_a), assignment_digest(&encoded_a));
    }

    /// §7 — genesis passes: no previous committed assignment means every
    /// member serves on its own provenance. Empty shards pass the same way —
    /// provenance, not data.
    #[test]
    fn genesis_and_prior_holders_pass_the_holder_check() {
        let det = det_table(&[1, 2, 3], 2);
        let never = |_shard: u16| false;

        // Genesis: no previous committed assignment.
        let genesis = HolderProvenance {
            prev_committed: None,
            prev_det: None,
            proven_completion: &never,
            inbound_fenced: &never,
        };
        assert!(local_holder_check(NodeId(1), 0, &genesis));

        // Post-genesis: only the previous committed holders pass.
        let prev = det_assignment(&det);
        let with_history = HolderProvenance {
            prev_committed: Some(&prev),
            prev_det: Some(&det),
            proven_completion: &never,
            inbound_fenced: &never,
        };
        let shard = 0u16;
        let holders: Vec<NodeId> = std::iter::once(prev[0])
            .chain(derive_replicas(&det, shard, prev[0]))
            .collect();
        for id in [1u64, 2, 3] {
            let node = NodeId(id);
            assert_eq!(
                local_holder_check(node, shard, &with_history),
                holders.contains(&node),
                "node {id}: exactly the previous committed holders pass",
            );
        }
    }

    /// §7 — the under-fence direction. A node named master of a shard nobody
    /// ever sent it has a CLEAR inbound bit (that fence raises on data
    /// arrival), so only the prev-committed-holders clause stops it serving
    /// the shard empty.
    #[test]
    fn a_non_holder_named_master_is_fenced_with_a_concrete_source() {
        let det = det_table(&[1, 2, 3], 2);
        let prev = det_assignment(&det);
        let shard = 0u16;
        let prev_master = prev[shard as usize];
        let outsider = members(&[1, 2, 3])
            .into_iter()
            .find(|node| {
                *node != prev_master && !derive_replicas(&det, shard, prev_master).contains(node)
            })
            .expect("rf=2 of 3 leaves one non-holder");

        let never = |_shard: u16| false;
        let provenance = HolderProvenance {
            prev_committed: Some(&prev),
            prev_det: Some(&det),
            proven_completion: &never,
            inbound_fenced: &never,
        };
        let assignment = CommittedAssignment::new(det_assignment(&det), &vec![false; NUM_SHARDS]);
        assert_eq!(
            local_fence(outsider, shard, &assignment, &provenance),
            FenceDecision::FenceWithSource {
                source: prev_master
            },
            "a data-less named master must fence AND get a real pull source",
        );

        // A proven completion at the current epoch clears it — that is the
        // clearing edge the fence must always have.
        let proven = |_shard: u16| true;
        let with_completion = HolderProvenance {
            prev_committed: Some(&prev),
            prev_det: Some(&det),
            proven_completion: &proven,
            inbound_fenced: &never,
        };
        assert_eq!(
            local_fence(outsider, shard, &assignment, &with_completion),
            FenceDecision::Serve,
        );
    }

    /// §7 — the committed unproven bit is raise-only: it fences a node whose
    /// own provenance would pass, and the fence still names a concrete source.
    #[test]
    fn the_committed_unproven_bit_raises_but_never_lowers() {
        let det = det_table(&[1, 2, 3], 2);
        let prev = det_assignment(&det);
        let shard = 0u16;
        let master = prev[shard as usize];

        let never = |_shard: u16| false;
        let provenance = HolderProvenance {
            prev_committed: Some(&prev),
            prev_det: Some(&det),
            proven_completion: &never,
            inbound_fenced: &never,
        };

        // Bit set for this shard → even the previous committed master fences.
        let mut flags = vec![false; NUM_SHARDS];
        flags[shard as usize] = true;
        let flagged = CommittedAssignment::new(det_assignment(&det), &flags);
        match local_fence(master, shard, &flagged, &provenance) {
            FenceDecision::FenceWithSource { .. } | FenceDecision::AlertNoSource => {}
            FenceDecision::Serve => {
                panic!("the committed unproven bit must raise the fence")
            }
        }

        // Bit clear → provenance decides; the inbound fence still raises.
        let clear = CommittedAssignment::new(det_assignment(&det), &vec![false; NUM_SHARDS]);
        assert_eq!(
            local_fence(master, shard, &clear, &provenance),
            FenceDecision::Serve,
        );
        let inbound = |s: u16| s == shard;
        let inbound_fenced = HolderProvenance {
            prev_committed: Some(&prev),
            prev_det: Some(&det),
            proven_completion: &never,
            inbound_fenced: &inbound,
        };
        assert_ne!(
            local_fence(master, shard, &clear, &inbound_fenced),
            FenceDecision::Serve,
            "a raised inbound fence must never be overridden by a clear committed bit",
        );
    }

    /// §7 (P0-4) — a fence with no concrete source has no clearing edge, so
    /// the decision must be an ALERT, never a fence. The genesis case (no
    /// previous committed assignment at all) is exactly that shape.
    #[test]
    fn a_fence_with_no_source_becomes_an_alert() {
        let det = det_table(&[1, 2, 3], 2);
        let shard = 0u16;
        let never = |_shard: u16| false;
        let inbound = |_shard: u16| true; // inbound fence raised, no history
        let provenance = HolderProvenance {
            prev_committed: None,
            prev_det: None,
            proven_completion: &never,
            inbound_fenced: &inbound,
        };
        let assignment = CommittedAssignment::new(det_assignment(&det), &vec![false; NUM_SHARDS]);
        assert_eq!(
            local_fence(NodeId(1), shard, &assignment, &provenance),
            FenceDecision::AlertNoSource,
            "no previous committed master exists, so nothing can be pulled from — alert",
        );

        // A previous master that IS this node is equally unusable as a source.
        let prev = vec![NodeId(1); NUM_SHARDS];
        let self_prev = HolderProvenance {
            prev_committed: Some(&prev),
            prev_det: Some(&det),
            proven_completion: &never,
            inbound_fenced: &inbound,
        };
        assert_eq!(
            local_fence(NodeId(1), shard, &assignment, &self_prev),
            FenceDecision::AlertNoSource,
            "pulling from self is meaningless — alert, do not fence",
        );
    }

    /// The election's unproven flags: set only where usable evidence existed
    /// and the named master did not self-report full. An empty view flags
    /// NOTHING — a proposer that saw nothing must not flag the keyspace.
    #[test]
    fn unproven_flags_require_usable_evidence() {
        let ids = [1u64, 2, 3];
        let det = det_table(&ids, 2);
        let prev = det_assignment(&det);

        // Empty view → no flags at all.
        let empty = HolderReports::default();
        let mut history = DeviationHistory::new();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &empty,
                live: &live(&ids),
            },
            &mut history,
        );
        assert!(
            election.unproven.iter().all(|flag| !flag),
            "an empty view proves nothing and must flag nothing",
        );

        // Full evidence, one shard's master data-less while a replica is full
        // → exactly the shards whose FINAL master is not proven full flag.
        let shard = 0u16;
        let shard_candidates: Vec<NodeId> = {
            let a = det.target_assignment(shard);
            std::iter::once(a.master)
                .chain(a.replicas.iter().copied())
                .collect()
        };
        let reports = {
            let mut entries = Vec::new();
            for s in 0..NUM_SHARDS as u16 {
                let a = det.target_assignment(s);
                for node in std::iter::once(a.master).chain(a.replicas.iter().copied()) {
                    let seq = if s == shard && node == a.master { 0 } else { 1 };
                    entries.push((node, s, seq));
                }
            }
            HolderReports::from_entries(members(&ids), entries)
        };
        let mut history = DeviationHistory::new();
        let election = elect_committed_assignment(
            &ElectionInputs {
                det: &det,
                prev_committed: Some(&prev),
                reports: &reports,
                live: &live(&ids),
            },
            &mut history,
        );
        // First term: hysteresis keeps det.master in place, and det.master is
        // not proven full → that one shard is flagged.
        assert!(
            election.unproven[shard as usize],
            "a master that could not be proven full must be flagged",
        );
        let flagged: usize = election.unproven.iter().filter(|f| **f).count();
        assert_eq!(flagged, 1, "only that shard may be flagged");
        assert_eq!(
            election.assignment[shard as usize],
            det.target_assignment(shard).master,
            "hysteresis holds the deterministic master on one term of evidence",
        );
        let _ = shard_candidates;

        // The committed() pair round-trips the flags bit-exactly.
        let pair = election.committed();
        assert!(pair.is_unproven(shard));
        let encoded = pair.encode(&members(&ids)).expect("encode");
        let decoded = CommittedAssignment::decode(&encoded, &members(&ids)).expect("decode");
        assert_eq!(decoded, pair);
    }

    /// §11 — the swap preserves the deterministic holder set and never leaves
    /// the master in its own replica list.
    #[test]
    fn replica_derivation_preserves_the_holder_set() {
        let det = det_table(&[1, 2, 3, 4], 3);
        for shard in 0..NUM_SHARDS as u16 {
            let det_assignment = det.target_assignment(shard);
            let det_holders: HashSet<NodeId> = std::iter::once(det_assignment.master)
                .chain(det_assignment.replicas.iter().copied())
                .collect();

            for master in candidates(&det, shard) {
                let replicas = derive_replicas(&det, shard, master);
                let holders: HashSet<NodeId> = std::iter::once(master)
                    .chain(replicas.iter().copied())
                    .collect();
                assert_eq!(
                    holders, det_holders,
                    "shard {shard}: promoting {master:?} changed the holder set",
                );
                assert!(
                    !replicas.contains(&master),
                    "shard {shard}: master {master:?} must not remain a replica",
                );
                assert_eq!(replicas.len(), det_assignment.replicas.len());
            }
        }
    }

    /// A master outside the candidate set cannot be honoured without
    /// fabricating a holder set, so the deterministic replicas stand.
    #[test]
    fn replica_derivation_refuses_a_non_candidate_master() {
        let det = det_table(&[1, 2, 3], 2);
        let shard = 0u16;
        let replicas = derive_replicas(&det, shard, NodeId(77));
        assert_eq!(replicas, det.target_assignment(shard).replicas);
    }

    /// Installing an assignment must reproduce exactly what `derive_replicas`
    /// describes — the two must not drift apart.
    #[test]
    fn installing_an_assignment_matches_the_derived_replicas() {
        let det = det_table(&[1, 2, 3, 4], 3);
        let mut assignment = det_assignment(&det);
        // Deviate every shard onto its first replica.
        for shard in 0..NUM_SHARDS as u16 {
            let shard_candidates = candidates(&det, shard);
            if shard_candidates.len() > 1 {
                assignment[shard as usize] = shard_candidates[1];
            }
        }

        let mut table = det.clone();
        install_assignment(&mut table, &assignment);

        for shard in 0..NUM_SHARDS as u16 {
            let installed = table.target_assignment(shard);
            assert_eq!(installed.master, assignment[shard as usize]);
            assert_eq!(
                installed.replicas,
                derive_replicas(&det, shard, assignment[shard as usize]),
                "shard {shard}: install and derive_replicas disagree",
            );
        }
    }
}
