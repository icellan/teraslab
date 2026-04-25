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
}

/// SWIM membership state machine.
///
/// Manages the set of known members, their states, and emits events
/// when the membership changes. The actual probe transport (UDP) is
/// handled externally.
pub struct Membership {
    self_id: NodeId,
    members: HashMap<NodeId, MemberInfo>,
    suspicion_timeout: Duration,
    cached_alive: Vec<NodeId>,
}

impl Membership {
    /// Create a new membership tracker for this node.
    pub fn new(self_id: NodeId, suspicion_timeout: Duration) -> Self {
        Self {
            self_id,
            members: HashMap::new(),
            suspicion_timeout,
            cached_alive: vec![self_id],
        }
    }

    /// Recompute the cached sorted list of alive members (including self).
    fn rebuild_alive_cache(&mut self) {
        let mut members: Vec<NodeId> = self
            .members
            .iter()
            .filter(|(_, info)| info.state == NodeState::Alive)
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

        events
    }

    /// Mark a node as suspect (probes failed). Returns events.
    ///
    /// The incarnation must be >= the node's current incarnation; a stale
    /// suspect notification (from an old gossip round) is silently ignored
    /// to prevent overriding a newer alive state.
    pub fn mark_suspect(&mut self, node: NodeId, incarnation: u64) -> Vec<ClusterEvent> {
        let mut events = Vec::new();

        if let Some(info) = self.members.get_mut(&node)
            && info.state == NodeState::Alive
            && incarnation >= info.incarnation
        {
            info.state = NodeState::Suspect;
            info.state_changed_at = Instant::now();
            self.rebuild_alive_cache();
            events.push(ClusterEvent::NodeSuspect(node));
            if let Some(m) = swim_metrics() {
                m.record_churn(SwimChurnKind::Suspect);
            }
        }

        events
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
            info.state_changed_at = now;
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
    pub fn expire_suspects(&mut self) -> Vec<ClusterEvent> {
        let now = Instant::now();
        let timeout = self.suspicion_timeout;
        let expired: Vec<(NodeId, u64)> = self
            .members
            .iter()
            .filter(|(_, info)| {
                info.state == NodeState::Suspect
                    && now.duration_since(info.state_changed_at) >= timeout
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

    /// Number of alive members (including self).
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
        assert_eq!(m.alive_count(), 1);

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

    #[test]
    fn suspect_not_in_alive_list() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1, true);
        m.mark_suspect(NodeId(2), 1);

        let alive = m.alive_members();
        assert!(!alive.contains(&NodeId(2)));
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

    /// mark_suspect does NOT emit MembershipChanged. The alive list changes
    /// but the topology authority is not notified. This verifies the exact
    /// event sequence during the Alive → Suspect → Dead → Alive cycle.
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
}
