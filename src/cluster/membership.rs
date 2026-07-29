//! SWIM-style membership state machine.
//!
//! Tracks node states (Alive, Suspect, Dead) and emits cluster events.
//! The actual UDP probe protocol is a transport concern — this module
//! manages the state transitions and event generation.

use crate::cluster::shards::NodeId;
use crate::metrics::{SwimChurnKind, swim_metrics};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// State of a cluster member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Node is healthy and responsive.
    Alive,
    /// Node failed probes and is suspected of being down.
    Suspect,
    /// Node has been declared dead after suspicion timeout.
    Dead,
}

/// Events emitted by the membership module.
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterEvent {
    /// A new node joined the cluster.
    NodeJoined(NodeId, SocketAddr),
    /// A node is suspected of being down.
    NodeSuspect(NodeId),
    /// A node has been declared dead and removed.
    NodeLeft(NodeId),
    /// The alive member list changed (sorted).
    MembershipChanged(Vec<NodeId>),
    /// A remote node has a higher committed topology term than ours.
    /// The coordinator should request the committed topology to catch up.
    TopologyStale(u64),
}

/// Information about a cluster member.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    /// Node address.
    pub addr: SocketAddr,
    /// Current state.
    pub state: NodeState,
    /// Incarnation number (incremented on rejoin).
    pub incarnation: u64,
    /// When the state last changed.
    pub state_changed_at: Instant,
    /// P2 — Lifeguard dynamic-suspicion state. `Some` only while the member
    /// is Suspect; cleared on every transition out of Suspect (refutation or
    /// death), so a later re-suspicion always starts from a fresh deadline.
    pub suspicion: Option<SuspicionState>,
}

/// P2 — maximum number of distinct suspicion confirmers retained per member.
/// Beyond this many independent confirmations the dynamic deadline has
/// already collapsed to its floor (`ln(C+1)/ln(K+2)` clamps at 1 well before
/// 8 with K ≤ 3), so additional entries buy nothing — the cap keeps the
/// per-member state O(1) even under a confirmation flood.
pub const SUSPICION_CONFIRMER_CAP: usize = 8;

/// P2 — Lifeguard dynamic-suspicion state for one Suspect member.
///
/// Tracks the verified confirmers of the suspicion and the shrinking
/// per-member deadline. The deadline starts at `armed_at + max` (zero
/// confirmations = today's fixed timeout) and shrinks toward
/// `armed_at + min` as confirmations arrive; it never extends and never
/// drops below the floor.
#[derive(Debug, Clone)]
pub struct SuspicionState {
    /// Verified confirmers, in arrival order (entry 0 is the first verified
    /// suspector — the local node for locally-originated suspicions).
    /// SECURITY (P2): the transport guarantees every entry here is the
    /// HMAC-authenticated *sender* of the datagram that delivered the
    /// confirmation; relayed third-party suspector claims never land here
    /// (they go to `reported_suspector`). Dedup by NodeId, capped at
    /// [`SUSPICION_CONFIRMER_CAP`].
    confirmers: Vec<NodeId>,
    /// Relayed original-suspector claim (hearsay), recorded for
    /// observability and onward gossip only — contributes ZERO to the
    /// deadline because the claim cannot be authenticated.
    reported_suspector: Option<NodeId>,
    /// Arm-time deadline ceiling (the transport's LHM-scaled suspicion
    /// timeout).
    max: Duration,
    /// Arm-time deadline floor.
    min: Duration,
    /// Expected number of independent confirmers (K in the Lifeguard
    /// formula), captured at arm time from the cluster size.
    k: usize,
    /// When the suspicion was armed.
    armed_at: Instant,
    /// Current absolute deadline. Only ever shrinks, never below
    /// `armed_at + min`.
    deadline: Instant,
}

impl SuspicionState {
    /// P2 — recompute the deadline from the current confirmer count using
    /// the Lifeguard formula:
    ///
    /// `deadline = armed_at + max(min, max - (max-min) * ln(C+1)/ln(K+2))`
    ///
    /// Monotonic: C only grows and the formula decreases in C, so the new
    /// deadline can only shrink; the explicit `min()` guards against f64
    /// rounding ever extending it.
    fn recompute_deadline(&mut self) {
        let c = self.confirmers.len() as f64;
        let k = self.k as f64;
        let frac = ((c + 1.0).ln() / (k + 2.0).ln()).clamp(0.0, 1.0);
        let span = self.max.saturating_sub(self.min);
        let timeout = self.max.saturating_sub(span.mul_f64(frac)).max(self.min);
        let new_deadline = self.armed_at + timeout;
        if new_deadline < self.deadline {
            self.deadline = new_deadline;
        }
    }
}

/// SWIM membership state machine.
///
/// Manages the set of known members, their states, and emits events
/// when the membership changes. The actual probe transport (UDP) is
/// handled externally.
pub struct Membership {
    self_id: NodeId,
    members: HashMap<NodeId, MemberInfo>,
    /// Highest incarnation observed for each NodeId, retained even after a
    /// Dead member is garbage-collected from `members`.
    max_seen_incarnation: HashMap<NodeId, u64>,
    suspicion_timeout: Duration,
    cached_alive: Vec<NodeId>,
}

impl Membership {
    /// Create a new membership tracker for this node.
    pub fn new(self_id: NodeId, suspicion_timeout: Duration) -> Self {
        Self {
            self_id,
            members: HashMap::new(),
            max_seen_incarnation: HashMap::new(),
            suspicion_timeout,
            cached_alive: vec![self_id],
        }
    }

    /// Recompute the cached sorted list of alive members (including self).
    ///
    /// E-03: Suspect nodes are retained — a suspect is still a cluster
    /// member until the suspicion timeout declares it Dead (SWIM
    /// convention). This keeps the polled view (`alive_count` /
    /// `alive_members`) consistent with the `MembershipChanged` event
    /// stream, which is only emitted when the set actually changes
    /// (join, death, revival) — never on suspicion.
    fn rebuild_alive_cache(&mut self) {
        let mut members: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(_, info)| info.state != NodeState::Dead)
            .map(|(&id, _)| id)
            .collect();
        members.push(self.self_id);
        members.sort();
        self.cached_alive = members;
    }

    /// Register a node as alive. Returns events if membership changed.
    ///
    /// Accepts the update if the incarnation is higher than what we know
    /// (standard SWIM refutation), or if the incarnation matches and the
    /// node is currently Suspect or Dead. Same-incarnation revival handles
    /// partition recovery: if the node itself is sending probes/joins it
    /// is clearly alive, and blocking it on incarnation would prevent
    /// recovery.
    /// Mark a node as alive.
    ///
    /// `direct` indicates whether the alive signal came directly from the
    /// node itself (probe ACK) vs from third-party gossip. Direct signals
    /// are authoritative — the node provably responded. Gossip signals
    /// could be stale (the gossiper hasn't probed the node recently).
    ///
    /// Same-incarnation alive clears Suspect only when `direct=true`.
    /// This prevents stale gossip from uninformed peers from delaying
    /// failure detection, while still allowing a node that's actually
    /// alive to clear false suspicions via its own probe responses.
    pub fn mark_alive(
        &mut self,
        node: NodeId,
        addr: SocketAddr,
        incarnation: u64,
        direct: bool,
    ) -> Vec<ClusterEvent> {
        if node == self.self_id {
            return vec![];
        }

        let mut events = Vec::new();
        let historic_incarnation = self.max_seen_incarnation.get(&node).copied().unwrap_or(0);
        if incarnation < historic_incarnation {
            return events;
        }

        match self.members.get_mut(&node) {
            Some(info) => {
                let dominated = incarnation > info.incarnation;
                let same_inc_dead =
                    incarnation == info.incarnation && info.state == NodeState::Dead;
                // Same-incarnation alive from a direct probe ACK can clear
                // suspicion (the node proved it's alive). But same-inc alive
                // from gossip cannot — the gossiper may not have probed
                // the suspect recently.
                let same_inc_suspect_direct =
                    direct && incarnation == info.incarnation && info.state == NodeState::Suspect;

                if dominated || same_inc_dead || same_inc_suspect_direct {
                    let was_dead = info.state == NodeState::Dead;
                    let was_suspect = info.state == NodeState::Suspect;
                    let suspect_started_at = info.state_changed_at;
                    info.state = NodeState::Alive;
                    info.incarnation = incarnation;
                    info.addr = addr;
                    // P2 — a refutation (any transition back to Alive) clears
                    // the confirmer set; a later re-suspicion starts fresh.
                    info.suspicion = None;
                    let now = Instant::now();
                    info.state_changed_at = now;

                    if was_dead || was_suspect {
                        self.rebuild_alive_cache();
                        if was_dead {
                            events.push(ClusterEvent::NodeJoined(node, addr));
                            if let Some(m) = swim_metrics() {
                                m.record_churn(SwimChurnKind::Join);
                            }
                        }
                        if was_suspect && let Some(m) = swim_metrics() {
                            m.record_churn(SwimChurnKind::AliveFromSuspect);
                            let elapsed = now.saturating_duration_since(suspect_started_at);
                            m.swim_suspicion_duration_ns
                                .record_ns(elapsed.as_nanos() as u64);
                        }
                        // Both Dead→Alive and Suspect→Alive emit MembershipChanged
                        // so routing recomputes.
                        events.push(ClusterEvent::MembershipChanged(self.alive_members()));
                    }
                }
            }
            None => {
                self.members.insert(
                    node,
                    MemberInfo {
                        addr,
                        state: NodeState::Alive,
                        incarnation,
                        state_changed_at: Instant::now(),
                        suspicion: None,
                    },
                );
                self.rebuild_alive_cache();
                events.push(ClusterEvent::NodeJoined(node, addr));
                events.push(ClusterEvent::MembershipChanged(self.alive_members()));
                if let Some(m) = swim_metrics() {
                    m.record_churn(SwimChurnKind::Join);
                }
            }
        }

        self.max_seen_incarnation
            .entry(node)
            .and_modify(|max| *max = (*max).max(incarnation))
            .or_insert(incarnation);

        events
    }

    /// Mark a node as suspect (probes failed). Returns events.
    ///
    /// The incarnation must be >= the node's current incarnation; a stale
    /// suspect notification (from an old gossip round) is silently ignored
    /// to prevent overriding a newer alive state.
    ///
    /// P2 — arms the dynamic suspicion deadline at the fixed configured
    /// timeout (max = `suspicion_timeout`, min = `max / 8`). The transport
    /// uses [`Self::mark_suspect_with_timeouts`] to pass LHM-scaled bounds;
    /// this plain form keeps the historical behavior for direct callers.
    pub fn mark_suspect(&mut self, node: NodeId, incarnation: u64) -> Vec<ClusterEvent> {
        let max = self.suspicion_timeout;
        self.mark_suspect_with_timeouts(node, incarnation, max, max / 8)
    }

    /// P2 — mark a node as suspect with explicit Lifeguard deadline bounds.
    ///
    /// Same transition rules as [`Self::mark_suspect`]; additionally arms a
    /// [`SuspicionState`] whose deadline starts at `now + max` (i.e. with
    /// zero confirmations the member expires exactly as under the fixed
    /// timeout) and shrinks toward `now + min` as confirmations arrive via
    /// [`Self::confirm_suspect`]. `min` is clamped to `max`. K (expected
    /// confirmers) is captured from the current cluster size:
    /// `min(INDIRECT_PROBE_K, alive_count - 2)` — everyone except the
    /// suspect and the local node, bounded by the indirect-probe fan-out.
    ///
    /// A no-op (and no deadline reset) when the member is already Suspect —
    /// this preserves the W3.2 invariant that the suspicion clock runs from
    /// the FIRST suspicion.
    pub fn mark_suspect_with_timeouts(
        &mut self,
        node: NodeId,
        incarnation: u64,
        max: Duration,
        min: Duration,
    ) -> Vec<ClusterEvent> {
        let mut events = Vec::new();

        // K depends on the membership size at arm time; compute before
        // mutably borrowing the member entry.
        let k = crate::cluster::swim::INDIRECT_PROBE_K.min(self.alive_count().saturating_sub(2));

        if let Some(info) = self.members.get_mut(&node)
            && info.state == NodeState::Alive
            && incarnation >= info.incarnation
        {
            let now = Instant::now();
            info.state = NodeState::Suspect;
            info.incarnation = incarnation;
            info.state_changed_at = now;
            let min = min.min(max);
            info.suspicion = Some(SuspicionState {
                confirmers: Vec::new(),
                reported_suspector: None,
                max,
                min,
                k,
                armed_at: now,
                deadline: now + max,
            });
            self.max_seen_incarnation
                .entry(node)
                .and_modify(|max| *max = (*max).max(incarnation))
                .or_insert(incarnation);
            // E-03: the alive view intentionally does NOT change here —
            // a Suspect remains a member until declared Dead, matching
            // the absence of a MembershipChanged event. No cache rebuild
            // is needed (Alive → Suspect keeps the node in the cache).
            events.push(ClusterEvent::NodeSuspect(node));
            if let Some(m) = swim_metrics() {
                m.record_churn(SwimChurnKind::Suspect);
            }
        }

        events
    }

    /// P2 — count a verified suspicion confirmation and shrink the deadline.
    ///
    /// SECURITY RULE (non-negotiable): callers must pass as `confirmed_by`
    /// the *authenticated sender* of the datagram that delivered the
    /// confirmation — the transport only calls this when the extension's
    /// claimed suspector id equals the HMAC-verified sender. Each sender
    /// therefore contributes at most one confirmation per suspect (dedup by
    /// NodeId here enforces the "at most one" half).
    ///
    /// Returns `true` when the confirmation was counted; `false` when it was
    /// a duplicate, the member is not Suspect, the confirmer set is at
    /// [`SUSPICION_CONFIRMER_CAP`], or `confirmed_by` is the suspect itself
    /// (a node cannot confirm its own suspicion — it would be refuting it).
    pub fn confirm_suspect(&mut self, node: NodeId, confirmed_by: NodeId) -> bool {
        if confirmed_by == node {
            return false;
        }
        let Some(info) = self.members.get_mut(&node) else {
            return false;
        };
        if info.state != NodeState::Suspect {
            return false;
        }
        let Some(st) = info.suspicion.as_mut() else {
            return false;
        };
        if st.confirmers.contains(&confirmed_by) || st.confirmers.len() >= SUSPICION_CONFIRMER_CAP {
            return false;
        }
        st.confirmers.push(confirmed_by);
        st.recompute_deadline();
        true
    }

    /// P2 — record a relayed (unverifiable) original-suspector claim.
    ///
    /// Kept for observability and onward gossip only: the claim names who
    /// originally suspected `node` according to a third party, so it MUST
    /// NOT shrink the deadline (see [`Self::confirm_suspect`]'s security
    /// rule). First claim wins; ignored when the member is not Suspect.
    pub fn note_reported_suspector(&mut self, node: NodeId, suspector: NodeId) {
        if suspector == node {
            return;
        }
        if let Some(info) = self.members.get_mut(&node)
            && info.state == NodeState::Suspect
            && let Some(st) = info.suspicion.as_mut()
            && st.reported_suspector.is_none()
        {
            st.reported_suspector = Some(suspector);
        }
    }

    /// P2 — the suspector this node advertises in its suspect gossip.
    ///
    /// Prefers first-hand knowledge (the first *verified* confirmer — the
    /// local node itself for locally-originated suspicions) and falls back
    /// to the relayed hearsay claim. Preferring the verified entry is what
    /// lets independent suspectors advertise THEMSELVES: under the
    /// direct-sender-only counting rule a relayed original-suspector id can
    /// never be counted by receivers, so gossiping only the relayed
    /// original would stall every remote confirmer count at one.
    pub fn original_suspector(&self, node: &NodeId) -> Option<NodeId> {
        let st = self.members.get(node)?.suspicion.as_ref()?;
        st.confirmers.first().copied().or(st.reported_suspector)
    }

    /// P2 — current dynamic suspicion deadline for a Suspect member, if any.
    pub fn suspicion_deadline(&self, node: &NodeId) -> Option<Instant> {
        Some(self.members.get(node)?.suspicion.as_ref()?.deadline)
    }

    /// P2 — number of verified suspicion confirmers for a member (0 when
    /// not Suspect).
    pub fn suspicion_confirmer_count(&self, node: &NodeId) -> usize {
        self.members
            .get(node)
            .and_then(|i| i.suspicion.as_ref())
            .map(|st| st.confirmers.len())
            .unwrap_or(0)
    }

    /// Mark a node as dead. Returns events.
    ///
    /// The incarnation must be >= the node's current incarnation; a stale
    /// dead notification is silently ignored to prevent overriding a newer
    /// alive state that the node refuted with a higher incarnation.
    pub fn mark_dead(&mut self, node: NodeId, incarnation: u64) -> Vec<ClusterEvent> {
        let mut events = Vec::new();

        let mut post_transition: Option<(bool, std::time::Duration)> = None;
        if let Some(info) = self.members.get_mut(&node)
            && info.state != NodeState::Dead
            && incarnation >= info.incarnation
        {
            let was_suspect = info.state == NodeState::Suspect;
            let suspect_started_at = info.state_changed_at;
            let now = Instant::now();
            info.state = NodeState::Dead;
            info.incarnation = incarnation;
            info.state_changed_at = now;
            // P2 — the suspicion is resolved; drop its confirmer state.
            info.suspicion = None;
            self.max_seen_incarnation
                .entry(node)
                .and_modify(|max| *max = (*max).max(incarnation))
                .or_insert(incarnation);
            let elapsed = now.saturating_duration_since(suspect_started_at);
            post_transition = Some((was_suspect, elapsed));
        }
        if let Some((was_suspect, elapsed)) = post_transition {
            self.rebuild_alive_cache();
            events.push(ClusterEvent::NodeLeft(node));
            events.push(ClusterEvent::MembershipChanged(self.alive_members()));
            if let Some(m) = swim_metrics() {
                m.record_churn(SwimChurnKind::Leave);
                if was_suspect {
                    m.swim_suspicion_duration_ns
                        .record_ns(elapsed.as_nanos() as u64);
                }
            }
        }

        events
    }

    /// Check suspects that have exceeded the suspicion timeout and declare them dead.
    ///
    /// Uses each suspect's current incarnation so that expiration always
    /// succeeds — the incarnation guard in `mark_dead` is satisfied because
    /// we pass the exact incarnation we already know.
    ///
    /// P2 — each suspect expires at its own dynamic (Lifeguard) deadline,
    /// which starts at the arm-time `max` and shrinks as verified
    /// confirmations arrive. A Suspect without armed suspicion state (not
    /// reachable through the public API, but defended against) falls back
    /// to the fixed configured timeout.
    pub fn expire_suspects(&mut self) -> Vec<ClusterEvent> {
        let now = Instant::now();
        let timeout = self.suspicion_timeout;
        let expired: Vec<(NodeId, u64)> = self
            .members
            .iter()
            .filter(|(_, info)| {
                info.state == NodeState::Suspect
                    && match info.suspicion.as_ref() {
                        Some(st) => now >= st.deadline,
                        None => now.duration_since(info.state_changed_at) >= timeout,
                    }
            })
            .map(|(&id, info)| (id, info.incarnation))
            .collect();

        let mut events = Vec::new();
        for (node, incarnation) in expired {
            events.extend(self.mark_dead(node, incarnation));
        }
        events
    }

    /// Get the sorted list of alive members (including self).
    ///
    /// Suspect nodes are included: a suspect is still a member until the
    /// suspicion timeout declares it Dead (E-03 — this keeps the polled
    /// view consistent with the `MembershipChanged` event stream).
    ///
    /// Returns a clone of the internally cached list. The cache is rebuilt
    /// whenever membership state changes, so this is O(n) only in the clone
    /// cost, not in filtering/sorting.
    pub fn alive_members(&self) -> Vec<NodeId> {
        self.cached_alive.clone()
    }

    /// Number of known members (all states).
    pub fn total_members(&self) -> usize {
        self.members.len() + 1 // +1 for self
    }

    /// Number of alive members (including self). Suspect nodes count as
    /// alive until declared Dead — see [`Membership::alive_members`].
    pub fn alive_count(&self) -> usize {
        self.cached_alive.len()
    }

    /// Get info about a specific member.
    pub fn member_info(&self, node: &NodeId) -> Option<&MemberInfo> {
        self.members.get(node)
    }

    /// This node's ID.
    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// Iterate over all known members with their state and incarnation.
    ///
    /// Used by SWIM gossip to propagate state information (alive, suspect, dead)
    /// to other nodes. Does NOT include self.
    pub fn all_member_states(&self) -> Vec<(NodeId, NodeState, u64, SocketAddr)> {
        self.members
            .iter()
            .map(|(&id, info)| (id, info.state, info.incarnation, info.addr))
            .collect()
    }

    /// Remove dead nodes that have been in the Dead state for longer than
    /// `max_age`. This prevents unbounded memory growth from accumulated
    /// dead nodes across many cluster restart cycles.
    ///
    /// Returns the IDs of removed nodes so the caller can clean up
    /// associated state (e.g., peer address maps).
    pub fn forget_dead_older_than(&mut self, max_age: Duration) -> Vec<NodeId> {
        let now = Instant::now();
        let to_remove: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(_, info)| {
                info.state == NodeState::Dead
                    && now.duration_since(info.state_changed_at) >= max_age
            })
            .map(|(&id, _)| id)
            .collect();
        for id in &to_remove {
            self.members.remove(id);
        }
        if !to_remove.is_empty() {
            self.rebuild_alive_cache();
        }
        to_remove
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn new_node_joins() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        let events = m.mark_alive(NodeId(2), addr(3001), 1, true);

        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(2), _)))
        );
        assert_eq!(m.alive_count(), 2);
    }

    #[test]
    fn three_nodes_form_cluster() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_alive(NodeId(3), addr(3002), 1, true);

        let alive = m.alive_members();
        assert_eq!(alive.len(), 3);
        assert!(alive.contains(&NodeId(1)));
        assert!(alive.contains(&NodeId(2)));
        assert!(alive.contains(&NodeId(3)));
    }

    #[test]
    fn suspect_then_dead() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1, true);

        let events = m.mark_suspect(NodeId(2), 1);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeSuspect(NodeId(2))))
        );
        // E-03: a Suspect is still a member until declared Dead, so the
        // polled alive view does not shrink during the suspicion window.
        assert_eq!(m.alive_count(), 2);

        std::thread::sleep(Duration::from_millis(15));
        let events = m.expire_suspects();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(2))))
        );
        assert_eq!(m.alive_count(), 1);
    }

    #[test]
    fn dead_node_rejoins() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_dead(NodeId(2), 1);
        assert_eq!(m.alive_count(), 1);

        let events = m.mark_alive(NodeId(2), addr(3001), 2, true); // Higher incarnation
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(2), _)))
        );
        assert_eq!(m.alive_count(), 2);
    }

    #[test]
    fn membership_changed_contains_sorted_list() {
        let mut m = Membership::new(NodeId(3), Duration::from_secs(5));
        m.mark_alive(NodeId(1), addr(3001), 1, true);
        let events = m.mark_alive(NodeId(2), addr(3002), 1, true);

        let changed = events.iter().find_map(|e| match e {
            ClusterEvent::MembershipChanged(members) => Some(members.clone()),
            _ => None,
        });
        let members = changed.expect("should have MembershipChanged event");
        assert_eq!(members, vec![NodeId(1), NodeId(2), NodeId(3)]); // sorted
    }

    #[test]
    fn self_node_not_tracked_as_member() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        let events = m.mark_alive(NodeId(1), addr(3000), 1, true);
        assert!(events.is_empty());
        assert_eq!(m.total_members(), 1); // Just self
    }

    /// E-03: Suspect nodes remain in the alive view until declared Dead
    /// (SWIM convention) — so the polled view never diverges from the
    /// `MembershipChanged` event stream, which is only emitted on real
    /// alive-set changes (join, death, revival).
    #[test]
    fn suspect_stays_in_alive_list_until_dead() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_suspect(NodeId(2), 1);

        assert!(
            m.alive_members().contains(&NodeId(2)),
            "suspect must remain in the alive view until declared dead"
        );
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);

        m.mark_dead(NodeId(2), 1);
        assert!(!m.alive_members().contains(&NodeId(2)));
    }

    #[test]
    fn dead_not_in_alive_list() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_dead(NodeId(2), 1);

        let alive = m.alive_members();
        assert!(!alive.contains(&NodeId(2)));
    }

    #[test]
    fn membership_changed_on_join_and_leave() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));

        let events = m.mark_alive(NodeId(2), addr(3001), 1, true);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MembershipChanged(_)))
        );

        let events = m.mark_dead(NodeId(2), 1);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MembershipChanged(_)))
        );
    }

    // --- P0-A: Incarnation-aware state transitions ---

    #[test]
    fn stale_suspect_ignored() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5, true);

        // Stale incarnation 3 < current 5: must be ignored
        let events = m.mark_suspect(NodeId(2), 3);
        assert!(events.is_empty());
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
    }

    #[test]
    fn suspect_at_current_incarnation() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5, true);

        let events = m.mark_suspect(NodeId(2), 5);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeSuspect(NodeId(2))))
        );
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);
    }

    #[test]
    fn stale_dead_ignored() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5, true);

        // Stale incarnation 3 < current 5: must be ignored
        let events = m.mark_dead(NodeId(2), 3);
        assert!(events.is_empty());
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
    }

    #[test]
    fn dead_at_current_incarnation() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5, true);

        let events = m.mark_dead(NodeId(2), 5);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(2))))
        );
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Dead);
    }

    #[test]
    fn same_incarnation_gossip_does_not_clear_suspicion() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5, true);
        m.mark_suspect(NodeId(2), 5);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);

        // Same incarnation alive from GOSSIP (direct=false) must NOT clear.
        let events = m.mark_alive(NodeId(2), addr(3001), 5, false);
        assert_eq!(
            m.member_info(&NodeId(2)).unwrap().state,
            NodeState::Suspect,
            "same-incarnation gossip must not clear suspicion"
        );
        assert!(events.is_empty());

        // Same incarnation alive from DIRECT probe ACK (direct=true) SHOULD clear.
        let events = m.mark_alive(NodeId(2), addr(3001), 5, true);
        assert_eq!(
            m.member_info(&NodeId(2)).unwrap().state,
            NodeState::Alive,
            "same-incarnation direct probe should clear suspicion"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MembershipChanged(_)))
        );
    }

    #[test]
    fn alive_refutes_suspicion_higher_incarnation() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5, true);
        m.mark_suspect(NodeId(2), 5);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);

        // Higher incarnation alive also refutes suspicion
        let events = m.mark_alive(NodeId(2), addr(3001), 6, true);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().incarnation, 6);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MembershipChanged(_)))
        );
    }

    #[test]
    fn forget_dead_removes_old_dead_nodes() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_alive(NodeId(3), addr(3002), 1, true);

        // Kill node 2.
        m.mark_dead(NodeId(2), 1);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Dead);

        // Immediately, the dead node should NOT be forgotten (too young).
        let forgotten = m.forget_dead_older_than(Duration::from_secs(3600));
        assert!(forgotten.is_empty());
        assert!(m.member_info(&NodeId(2)).is_some());

        // With zero max_age, dead nodes are immediately eligible.
        let forgotten = m.forget_dead_older_than(Duration::ZERO);
        assert_eq!(forgotten, vec![NodeId(2)]);
        assert!(m.member_info(&NodeId(2)).is_none());
        // Alive node 3 is unaffected.
        assert!(m.member_info(&NodeId(3)).is_some());
    }

    #[test]
    fn dead_node_reborn_cannot_use_lower_incarnation() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 10, true);
        m.mark_dead(NodeId(2), 10);

        let forgotten = m.forget_dead_older_than(Duration::ZERO);
        assert_eq!(forgotten, vec![NodeId(2)]);
        assert!(m.member_info(&NodeId(2)).is_none());

        let events = m.mark_alive(NodeId(2), addr(3001), 9, true);
        assert!(
            events.is_empty(),
            "lower-incarnation rebirth must be ignored"
        );
        assert!(
            m.member_info(&NodeId(2)).is_none(),
            "forgotten node must not be reinserted at a lower incarnation"
        );

        let events = m.mark_alive(NodeId(2), addr(3001), 10, true);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(2), _))),
            "same-or-higher incarnation may rejoin after GC"
        );
        assert_eq!(m.member_info(&NodeId(2)).unwrap().incarnation, 10);
    }

    #[test]
    fn forget_dead_ignores_alive_and_suspect() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_alive(NodeId(3), addr(3002), 1, true);
        m.mark_suspect(NodeId(3), 1);

        // Even with zero max_age, alive and suspect nodes survive.
        let forgotten = m.forget_dead_older_than(Duration::ZERO);
        assert!(forgotten.is_empty());
        assert_eq!(m.total_members(), 3); // self + 2 peers
    }

    // -----------------------------------------------------------------------
    // Part 1.2: Death event fires exactly once
    // -----------------------------------------------------------------------

    #[test]
    fn death_event_fires_exactly_once() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_alive(NodeId(3), addr(3002), 1, true);

        // Kill node 3 — first time should emit NodeLeft
        let events = m.mark_dead(NodeId(3), 1);
        let left_count = events
            .iter()
            .filter(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(3))))
            .count();
        assert_eq!(left_count, 1, "NodeLeft should fire exactly once");

        // Second mark_dead with same incarnation should NOT emit again
        let events2 = m.mark_dead(NodeId(3), 1);
        let left_count2 = events2
            .iter()
            .filter(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(3))))
            .count();
        assert_eq!(
            left_count2, 0,
            "repeated mark_dead should not fire NodeLeft again"
        );

        // Node 2 should still be alive and unaffected
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
        assert_eq!(m.alive_count(), 2); // self + node 2
    }

    // -----------------------------------------------------------------------
    // Part 1.3: Rejoin timing and events
    // -----------------------------------------------------------------------

    #[test]
    fn dead_node_rejoin_emits_joined_and_membership_changed() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_dead(NodeId(2), 1);

        // Rejoin with same incarnation (Dead→Alive same-inc is allowed)
        let events = m.mark_alive(NodeId(2), addr(3001), 1, true);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(2), _))),
            "should emit NodeJoined on rejoin from Dead"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MembershipChanged(_))),
            "should emit MembershipChanged on rejoin from Dead"
        );
        assert_eq!(m.alive_count(), 2);
    }

    #[test]
    fn suspect_rejoin_gossip_requires_higher_incarnation() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_suspect(NodeId(2), 1);

        // Same incarnation from gossip does NOT clear suspicion.
        let events = m.mark_alive(NodeId(2), addr(3001), 1, false);
        assert!(events.is_empty(), "same-inc gossip must not revive suspect");
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);

        // Higher incarnation DOES clear suspicion (the suspect proved it's alive).
        let events = m.mark_alive(NodeId(2), addr(3001), 2, true);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(_, _))),
            "Suspect→Alive should not emit NodeJoined"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MembershipChanged(_))),
            "Suspect→Alive should emit MembershipChanged"
        );
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
    }

    // -----------------------------------------------------------------------
    // Part 1.4: Simultaneous start / fresh state
    // -----------------------------------------------------------------------

    #[test]
    fn fresh_membership_does_not_expire_unknown_nodes() {
        // On fresh start with no prior state, expire_suspects should be no-op.
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        // No peers known yet — expire should not crash or declare anyone dead
        let events = m.expire_suspects();
        assert!(events.is_empty(), "no suspects to expire on fresh start");
        assert_eq!(m.alive_count(), 1); // just self
    }

    // -----------------------------------------------------------------------
    // Part 1.6: Flapping node (rapid alive/dead cycles)
    // -----------------------------------------------------------------------

    #[test]
    fn flapping_node_no_zombie_state() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1, true);

        // Simulate flapping: dead → alive → dead → alive in rapid succession
        for incarnation in 1..=20u64 {
            m.mark_dead(NodeId(2), incarnation);
            let state = m.member_info(&NodeId(2)).unwrap().state;
            assert_eq!(state, NodeState::Dead, "inc {incarnation}: should be dead");
            assert!(!m.alive_members().contains(&NodeId(2)));

            m.mark_alive(NodeId(2), addr(3001), incarnation + 1, true);
            let state = m.member_info(&NodeId(2)).unwrap().state;
            assert_eq!(
                state,
                NodeState::Alive,
                "inc {}: should be alive",
                incarnation + 1
            );
            assert!(m.alive_members().contains(&NodeId(2)));
        }

        // After flapping, final state should be consistent
        let alive = m.alive_members();
        assert_eq!(alive.len(), 2);
        assert!(alive.contains(&NodeId(1)));
        assert!(alive.contains(&NodeId(2)));
    }

    // -----------------------------------------------------------------------
    // Part 1.9: Message corruption / self-message rejection
    // -----------------------------------------------------------------------

    #[test]
    fn self_message_ignored() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        // Receiving a heartbeat from our own NodeId should be no-op
        let events = m.mark_alive(NodeId(1), addr(3000), 100, true);
        assert!(events.is_empty());
        assert_eq!(m.alive_count(), 1); // just self
        assert!(m.member_info(&NodeId(1)).is_none()); // self not tracked in members
    }

    // -----------------------------------------------------------------------
    // Part 1: Additional edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn alive_list_sorted_after_every_mutation() {
        let mut m = Membership::new(NodeId(5), Duration::from_secs(5));
        m.mark_alive(NodeId(3), addr(3001), 1, true);
        m.mark_alive(NodeId(1), addr(3002), 1, true);
        m.mark_alive(NodeId(7), addr(3003), 1, true);
        m.mark_alive(NodeId(2), addr(3004), 1, true);

        let alive = m.alive_members();
        assert_eq!(
            alive,
            vec![NodeId(1), NodeId(2), NodeId(3), NodeId(5), NodeId(7)]
        );

        // Remove one and check sort is maintained
        m.mark_dead(NodeId(3), 1);
        let alive = m.alive_members();
        assert_eq!(alive, vec![NodeId(1), NodeId(2), NodeId(5), NodeId(7)]);
    }

    #[test]
    fn mark_dead_on_unknown_node_no_op() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        // Marking an unknown node dead should be a no-op
        let events = m.mark_dead(NodeId(99), 1);
        assert!(events.is_empty());
        assert_eq!(m.alive_count(), 1);
    }

    #[test]
    fn mark_suspect_on_unknown_node_no_op() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        let events = m.mark_suspect(NodeId(99), 1);
        assert!(events.is_empty());
    }

    #[test]
    fn expire_suspects_only_after_timeout() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(10));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_suspect(NodeId(2), 1);

        // Immediately: should NOT expire (timeout is 10 seconds)
        let events = m.expire_suspects();
        assert!(events.is_empty(), "should not expire before timeout");
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);
    }

    #[test]
    fn stale_alive_with_lower_incarnation_ignored() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 10, true);

        // Stale alive with lower incarnation: no state change
        let events = m.mark_alive(NodeId(2), addr(3001), 5, true);
        assert!(events.is_empty());
        assert_eq!(m.member_info(&NodeId(2)).unwrap().incarnation, 10);
    }

    #[test]
    fn same_incarnation_alive_on_alive_is_noop() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5, true);

        // Same incarnation alive on already-alive node: no event
        let events = m.mark_alive(NodeId(2), addr(3001), 5, true);
        assert!(events.is_empty(), "same-inc alive on alive should be noop");
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
    }

    // -----------------------------------------------------------------------
    // Deep edge cases: state transition interactions
    // -----------------------------------------------------------------------

    /// mark_suspect does NOT emit MembershipChanged — and (E-03) the alive
    /// view does not change either: a Suspect remains a member until
    /// declared Dead. This verifies the exact event sequence during the
    /// Alive → Suspect → Dead → Alive cycle.
    #[test]
    fn full_lifecycle_event_sequence() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);

        // Alive → Suspect: only NodeSuspect, no MembershipChanged
        let ev1 = m.mark_suspect(NodeId(2), 1);
        assert_eq!(ev1.len(), 1);
        assert!(matches!(&ev1[0], ClusterEvent::NodeSuspect(NodeId(2))));

        // Suspect → Dead (via expire): NodeLeft + MembershipChanged
        std::thread::sleep(Duration::from_millis(10));
        let ev2 = m.expire_suspects();
        assert!(
            ev2.iter()
                .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(2))))
        );
        assert!(
            ev2.iter()
                .any(|e| matches!(e, ClusterEvent::MembershipChanged(_)))
        );

        // Dead → Alive (rejoin): NodeJoined + MembershipChanged
        let ev3 = m.mark_alive(NodeId(2), addr(3001), 2, true);
        assert!(
            ev3.iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(2), _)))
        );
        assert!(
            ev3.iter()
                .any(|e| matches!(e, ClusterEvent::MembershipChanged(_)))
        );
    }

    /// Suspect → Alive via gossip requires higher incarnation.
    /// Direct probe ACK with same incarnation clears suspicion.
    #[test]
    fn suspect_recovery_gossip_vs_direct() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_suspect(NodeId(2), 1);

        // Same incarnation from gossip: no effect
        let events = m.mark_alive(NodeId(2), addr(3001), 1, false);
        assert!(events.is_empty());
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);

        // Higher incarnation: clears suspicion
        let events = m.mark_alive(NodeId(2), addr(3001), 2, true);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeJoined(_, _))),
            "Suspect→Alive must NOT emit NodeJoined"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MembershipChanged(_))),
            "Suspect→Alive must emit MembershipChanged"
        );
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
    }

    /// expire_suspects with multiple suspects: all should expire, generating
    /// one NodeLeft + MembershipChanged per expired node.
    #[test]
    fn expire_multiple_suspects() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_alive(NodeId(3), addr(3002), 1, true);
        m.mark_alive(NodeId(4), addr(3003), 1, true);

        m.mark_suspect(NodeId(2), 1);
        m.mark_suspect(NodeId(3), 1);
        // Node 4 stays alive

        std::thread::sleep(Duration::from_millis(10));
        let events = m.expire_suspects();

        let left_nodes: Vec<NodeId> = events
            .iter()
            .filter_map(|e| match e {
                ClusterEvent::NodeLeft(id) => Some(*id),
                _ => None,
            })
            .collect();
        assert_eq!(left_nodes.len(), 2, "both suspects should expire");
        assert!(left_nodes.contains(&NodeId(2)));
        assert!(left_nodes.contains(&NodeId(3)));

        // Node 4 should still be alive
        assert_eq!(m.alive_count(), 2); // self + node 4
        assert!(m.alive_members().contains(&NodeId(4)));
    }

    /// Higher incarnation alive supersedes a lower incarnation suspect.
    /// Even though the suspect notification was valid at incarnation 5,
    /// incarnation 6 alive refutes it.
    #[test]
    fn higher_incarnation_alive_overrides_suspect() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5, true);
        m.mark_suspect(NodeId(2), 5);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);

        // Higher incarnation alive refutes
        let events = m.mark_alive(NodeId(2), addr(3001), 6, true);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().incarnation, 6);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::MembershipChanged(_)))
        );
    }

    /// forget_dead_older_than must not affect nodes that have since been
    /// revived. If a node was Dead but is now Alive, it must NOT be forgotten.
    #[test]
    fn forget_dead_does_not_affect_revived_node() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_dead(NodeId(2), 1);
        // Revive with higher incarnation
        m.mark_alive(NodeId(2), addr(3001), 2, true);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);

        // forget_dead should not remove the now-alive node
        let forgotten = m.forget_dead_older_than(Duration::ZERO);
        assert!(forgotten.is_empty());
        assert!(m.member_info(&NodeId(2)).is_some());
    }

    /// all_member_states returns all known members excluding self, with
    /// correct state, incarnation, and address.
    #[test]
    fn all_member_states_complete() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_alive(NodeId(3), addr(3002), 1, true);
        m.mark_suspect(NodeId(3), 1);

        let states = m.all_member_states();
        assert_eq!(states.len(), 2);

        let n2 = states
            .iter()
            .find(|(id, _, _, _)| *id == NodeId(2))
            .unwrap();
        assert_eq!(n2.1, NodeState::Alive);
        assert_eq!(n2.2, 1); // incarnation

        let n3 = states
            .iter()
            .find(|(id, _, _, _)| *id == NodeId(3))
            .unwrap();
        assert_eq!(n3.1, NodeState::Suspect);
    }

    /// Phase 5: driving state transitions must tick the churn counters
    /// in `SwimMetrics`. Observe deltas rather than absolute counts so
    /// the test is parallel-safe.
    #[test]
    fn swim_churn_counter_ticks_on_state_transitions() {
        use crate::metrics::{SwimChurnKind, SwimMetrics, init_swim_metrics, swim_metrics};
        use std::sync::OnceLock;

        static TEST_METRICS: OnceLock<SwimMetrics> = OnceLock::new();
        let m_ref: &'static SwimMetrics = TEST_METRICS.get_or_init(SwimMetrics::new);
        init_swim_metrics(m_ref);
        let metrics = swim_metrics().expect("metrics installed");
        let before = [
            metrics
                .swim_membership_churn_total
                .get(SwimChurnKind::Join as usize),
            metrics
                .swim_membership_churn_total
                .get(SwimChurnKind::Suspect as usize),
            metrics
                .swim_membership_churn_total
                .get(SwimChurnKind::AliveFromSuspect as usize),
            metrics
                .swim_membership_churn_total
                .get(SwimChurnKind::Leave as usize),
        ];

        let mut m = Membership::new(NodeId(1), Duration::from_millis(5));
        // 1 Join (new node Alive).
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        // 1 Suspect.
        m.mark_suspect(NodeId(2), 1);
        // 1 AliveFromSuspect (same-inc direct clears suspicion).
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        // 1 Leave.
        m.mark_dead(NodeId(2), 1);

        let after = [
            metrics
                .swim_membership_churn_total
                .get(SwimChurnKind::Join as usize),
            metrics
                .swim_membership_churn_total
                .get(SwimChurnKind::Suspect as usize),
            metrics
                .swim_membership_churn_total
                .get(SwimChurnKind::AliveFromSuspect as usize),
            metrics
                .swim_membership_churn_total
                .get(SwimChurnKind::Leave as usize),
        ];
        // Assert delta ≥ 1 rather than == 1: other parallel tests in the
        // same process also exercise these state transitions.
        assert!(
            after[0] - before[0] >= 1,
            "Join should tick ≥ 1 (delta={})",
            after[0] - before[0]
        );
        assert!(
            after[1] - before[1] >= 1,
            "Suspect should tick ≥ 1 (delta={})",
            after[1] - before[1]
        );
        assert!(
            after[2] - before[2] >= 1,
            "AliveFromSuspect should tick ≥ 1 (delta={})",
            after[2] - before[2]
        );
        assert!(
            after[3] - before[3] >= 1,
            "Leave should tick ≥ 1 (delta={})",
            after[3] - before[3]
        );
    }

    /// Address update: mark_alive with a new address should update the stored
    /// address without generating spurious events (if incarnation matches and
    /// node is already alive).
    #[test]
    fn address_update_on_alive_node() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);

        // Same incarnation, different address, already alive → no events
        let events = m.mark_alive(NodeId(2), addr(4001), 1, true);
        assert!(
            events.is_empty(),
            "same-inc alive-to-alive should be noop even with different addr"
        );
        // Address stays as original (no update on same-inc alive→alive)
        // This is the current behavior — the address is NOT updated.
        // This could be a problem if a node restarts on a different port
        // with the same incarnation, but that's prevented by incarnation
        // monotonicity (restart → higher incarnation).
    }

    // -----------------------------------------------------------------------
    // E-03: polled alive view and MembershipChanged event stream agree
    // -----------------------------------------------------------------------

    /// Extract the member list of the last `MembershipChanged` in `events`,
    /// if any.
    fn last_membership_changed(events: &[ClusterEvent]) -> Option<Vec<NodeId>> {
        events.iter().rev().find_map(|e| match e {
            ClusterEvent::MembershipChanged(m) => Some(m.clone()),
            _ => None,
        })
    }

    /// mark_suspect must not desynchronize the polled view from the event
    /// stream: no `MembershipChanged` is emitted AND the polled alive view
    /// is unchanged (the suspect is still a member).
    #[test]
    fn suspect_polled_view_matches_event_stream() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        let view_before = m.alive_members();
        assert_eq!(view_before, vec![NodeId(1), NodeId(2)]);

        let events = m.mark_suspect(NodeId(2), 1);
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], ClusterEvent::NodeSuspect(NodeId(2))));
        assert_eq!(
            last_membership_changed(&events),
            None,
            "suspicion must not emit MembershipChanged"
        );
        assert_eq!(
            m.alive_members(),
            view_before,
            "no event ⇒ no polled-view change: both must still include the suspect"
        );
        assert_eq!(m.alive_count(), 2);
    }

    /// Suspect → Dead: the `MembershipChanged` payload must equal the
    /// polled view at the moment the event is emitted.
    #[test]
    fn suspect_to_dead_polled_view_matches_event_stream() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_suspect(NodeId(2), 1);

        std::thread::sleep(Duration::from_millis(10));
        let events = m.expire_suspects();
        let payload =
            last_membership_changed(&events).expect("suspect expiry must emit MembershipChanged");
        assert_eq!(payload, vec![NodeId(1)], "dead node removed from payload");
        assert_eq!(
            payload,
            m.alive_members(),
            "event payload must equal the polled view"
        );
        assert_eq!(m.alive_count(), 1);
    }

    /// Suspect → Alive refutation (direct probe ACK): the polled view never
    /// changed during the suspicion window, and the recovery event's payload
    /// equals the polled view.
    #[test]
    fn suspect_refute_polled_view_matches_event_stream() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_suspect(NodeId(2), 1);
        assert_eq!(
            m.alive_count(),
            2,
            "suspect still counted during the suspicion window"
        );

        let events = m.mark_alive(NodeId(2), addr(3001), 1, true);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
        if let Some(payload) = last_membership_changed(&events) {
            assert_eq!(
                payload,
                m.alive_members(),
                "recovery event payload must equal the polled view"
            );
        }
        assert_eq!(m.alive_members(), vec![NodeId(1), NodeId(2)]);
    }

    // -----------------------------------------------------------------------
    // P2: Lifeguard dynamic suspicion deadline
    // -----------------------------------------------------------------------

    /// Build a membership with `peers` alive peers (NodeIds 2..2+peers) so
    /// K = min(3, cluster_size - 2) is deterministic in the tests below.
    fn membership_with_peers(timeout: Duration, peers: u64) -> Membership {
        let mut m = Membership::new(NodeId(1), timeout);
        for i in 0..peers {
            m.mark_alive(NodeId(2 + i), addr(3001 + i as u16), 1, true);
        }
        m
    }

    /// P2 — the same `confirmed_by` twice counts once: the second call is
    /// rejected and the deadline does not shrink further.
    #[test]
    fn confirm_suspect_dedups_same_sender() {
        let mut m = membership_with_peers(Duration::from_secs(8), 5); // K = 3
        m.mark_suspect(NodeId(2), 1);
        let d0 = m.suspicion_deadline(&NodeId(2)).unwrap();

        assert!(m.confirm_suspect(NodeId(2), NodeId(3)));
        let d1 = m.suspicion_deadline(&NodeId(2)).unwrap();
        assert!(d1 < d0, "first confirmation must shrink the deadline");
        assert_eq!(m.suspicion_confirmer_count(&NodeId(2)), 1);

        // Same sender again: rejected, no further shrink.
        assert!(!m.confirm_suspect(NodeId(2), NodeId(3)));
        assert_eq!(
            m.suspicion_deadline(&NodeId(2)).unwrap(),
            d1,
            "duplicate confirmer must not shrink the deadline again"
        );
        assert_eq!(m.suspicion_confirmer_count(&NodeId(2)), 1);
    }

    /// P2 — distinct confirmers shrink the deadline monotonically, and the
    /// suspect itself can never confirm its own suspicion.
    #[test]
    fn deadline_shrinks_monotonically_with_distinct_confirmers() {
        let mut m = membership_with_peers(Duration::from_secs(8), 5); // K = 3
        m.mark_suspect(NodeId(2), 1);
        let mut prev = m.suspicion_deadline(&NodeId(2)).unwrap();

        // The suspect confirming itself is rejected outright.
        assert!(!m.confirm_suspect(NodeId(2), NodeId(2)));
        assert_eq!(m.suspicion_deadline(&NodeId(2)).unwrap(), prev);

        for confirmer in [NodeId(1), NodeId(3), NodeId(4)] {
            assert!(m.confirm_suspect(NodeId(2), confirmer));
            let d = m.suspicion_deadline(&NodeId(2)).unwrap();
            assert!(
                d < prev,
                "each distinct confirmer must strictly shrink the deadline"
            );
            prev = d;
        }
        assert_eq!(m.suspicion_confirmer_count(&NodeId(2)), 3);
    }

    /// P2 — the deadline never drops below the arm-time floor, and the
    /// confirmer set is capped at [`SUSPICION_CONFIRMER_CAP`].
    #[test]
    fn deadline_floor_and_confirmer_cap_respected() {
        let mut m = membership_with_peers(Duration::from_secs(1), 3); // K = 2
        let max = Duration::from_millis(800);
        let min = Duration::from_millis(100);
        m.mark_suspect_with_timeouts(NodeId(2), 1, max, min);
        let d0 = m.suspicion_deadline(&NodeId(2)).unwrap(); // armed_at + max

        // Flood with distinct confirmers well past the point where the
        // formula clamps at the floor.
        let mut counted = 0usize;
        for i in 0..12u64 {
            if m.confirm_suspect(NodeId(2), NodeId(100 + i)) {
                counted += 1;
            }
        }
        assert_eq!(
            counted, SUSPICION_CONFIRMER_CAP,
            "confirmers past the cap must be rejected"
        );
        assert_eq!(
            m.suspicion_confirmer_count(&NodeId(2)),
            SUSPICION_CONFIRMER_CAP
        );

        // Floor: the deadline collapsed exactly to armed_at + min and no
        // further (ln(C+1)/ln(K+2) clamps at 1, span shrink is exact).
        let d_final = m.suspicion_deadline(&NodeId(2)).unwrap();
        assert_eq!(
            d_final,
            d0 - (max - min),
            "deadline must clamp at the floor (armed_at + min)"
        );
    }

    /// P2 — a refutation (Suspect → Alive) clears the confirmer set; a
    /// later re-suspicion starts from a fresh, un-shrunk deadline.
    #[test]
    fn refutation_clears_confirmers() {
        let mut m = membership_with_peers(Duration::from_secs(8), 5);
        m.mark_suspect(NodeId(2), 1);
        assert!(m.confirm_suspect(NodeId(2), NodeId(3)));
        assert!(m.confirm_suspect(NodeId(2), NodeId(4)));
        assert_eq!(m.suspicion_confirmer_count(&NodeId(2)), 2);

        // Direct same-incarnation ACK refutes the suspicion.
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
        assert_eq!(m.suspicion_confirmer_count(&NodeId(2)), 0);
        assert!(
            m.suspicion_deadline(&NodeId(2)).is_none(),
            "refutation must drop the suspicion state entirely"
        );

        // Re-suspect: fresh state, zero confirmers, deadline back at max.
        m.mark_suspect(NodeId(2), 1);
        assert_eq!(m.suspicion_confirmer_count(&NodeId(2)), 0);
        let d = m.suspicion_deadline(&NodeId(2)).unwrap();
        // With zero confirmers the deadline sits a full `max` out; two
        // confirmers earlier would have shrunk it well below that.
        assert!(d >= Instant::now() + Duration::from_secs(7));
    }

    /// P2 — direct-sender-only rule at the API level: a relayed suspector
    /// id (`note_reported_suspector`) is recorded for observability but
    /// contributes zero to the deadline; only `confirm_suspect` (which the
    /// transport calls exclusively with the authenticated datagram sender)
    /// shrinks it.
    #[test]
    fn relayed_suspector_contributes_zero() {
        let mut m = membership_with_peers(Duration::from_secs(8), 5);
        m.mark_suspect(NodeId(2), 1);
        let d0 = m.suspicion_deadline(&NodeId(2)).unwrap();

        // Relayed claim: "NodeId(9) originally suspected NodeId(2)".
        m.note_reported_suspector(NodeId(2), NodeId(9));
        assert_eq!(
            m.suspicion_deadline(&NodeId(2)).unwrap(),
            d0,
            "hearsay suspector must not shrink the deadline"
        );
        assert_eq!(m.suspicion_confirmer_count(&NodeId(2)), 0);
        // ...but it is visible for onward gossip.
        assert_eq!(m.original_suspector(&NodeId(2)), Some(NodeId(9)));

        // A verified confirmation still counts and takes precedence in the
        // advertised suspector.
        assert!(m.confirm_suspect(NodeId(2), NodeId(3)));
        assert!(m.suspicion_deadline(&NodeId(2)).unwrap() < d0);
        assert_eq!(m.original_suspector(&NodeId(2)), Some(NodeId(3)));
    }

    /// P2 — expire_suspects honors the per-member dynamic deadline: with
    /// enough confirmations the suspect dies at the floor, far before the
    /// configured suspicion timeout.
    #[test]
    fn expire_uses_dynamic_deadline() {
        // Configured timeout is LONG (10s) so a fixed-timeout expiry can
        // never fire inside this test; only the shrunk deadline can.
        let mut m = membership_with_peers(Duration::from_secs(10), 3); // K = 2
        let max = Duration::from_secs(10);
        let min = Duration::from_millis(20);
        m.mark_suspect_with_timeouts(NodeId(2), 1, max, min);

        // Not expired without confirmations.
        assert!(m.expire_suspects().is_empty());

        // Enough distinct confirmers to clamp the deadline at the floor
        // (C=7 ⇒ ln(8)/ln(4) > 1 with K=2).
        for i in 0..7u64 {
            m.confirm_suspect(NodeId(2), NodeId(100 + i));
        }
        std::thread::sleep(min + Duration::from_millis(10));
        let events = m.expire_suspects();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(2)))),
            "confirmed suspect must expire at the shrunk deadline, events: {events:?}"
        );
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Dead);
    }
}
