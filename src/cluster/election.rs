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
}

impl Election {
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

        // (E3) KEEPING a deviation and CREATING one are the same decision, and
        // both must clear the same bar: the deviating master self-reports full
        // AND the deterministic master self-reports data-less, for
        // DEVIATION_HYSTERESIS_TERMS consecutive terms. Evaluate the shard's
        // streak exactly ONCE per term — advancing it twice in one pass lets a
        // single term of evidence clear a two-term bar.
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
            if justification_holds && streak >= DEVIATION_HYSTERESIS_TERMS {
                (desired, ShardOutcome::Deviated)
            } else {
                (det_master, ShardOutcome::Reverted)
            }
        };
        base = master;

        assignment.push(base);
        outcomes.push(outcome);
    }

    Election {
        assignment,
        outcomes,
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

        // First term: the streak is still building, so the deviation reverts.
        let first = elect_committed_assignment(&inputs, &mut history);
        assert_eq!(first.assignment[shard as usize], det_master);

        // Once the streak is met the deviation stands and stays.
        let second = elect_committed_assignment(&inputs, &mut history);
        assert_eq!(second.assignment[shard as usize], deviating);
        let third = elect_committed_assignment(&inputs, &mut history);
        assert_eq!(third.assignment[shard as usize], deviating);
        assert_eq!(third.outcomes[shard as usize], ShardOutcome::Deviated);
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
