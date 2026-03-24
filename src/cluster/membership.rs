//! SWIM-style membership state machine.
//!
//! Tracks node states (Alive, Suspect, Dead) and emits cluster events.
//! The actual UDP probe protocol is a transport concern — this module
//! manages the state transitions and event generation.

use crate::cluster::shards::NodeId;
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
}

impl Membership {
    /// Create a new membership tracker for this node.
    pub fn new(self_id: NodeId, suspicion_timeout: Duration) -> Self {
        Self {
            self_id,
            members: HashMap::new(),
            suspicion_timeout,
        }
    }

    /// Register a node as alive. Returns events if membership changed.
    ///
    /// Accepts the update if the incarnation is higher than what we know
    /// (standard SWIM refutation), or if the incarnation matches and the
    /// node is currently Suspect or Dead. Same-incarnation revival handles
    /// partition recovery: if the node itself is sending probes/joins it
    /// is clearly alive, and blocking it on incarnation would prevent
    /// recovery.
    pub fn mark_alive(
        &mut self,
        node: NodeId,
        addr: SocketAddr,
        incarnation: u64,
    ) -> Vec<ClusterEvent> {
        if node == self.self_id {
            return vec![];
        }

        let mut events = Vec::new();

        match self.members.get_mut(&node) {
            Some(info) => {
                let dominated = incarnation > info.incarnation;
                let same_inc_dead = incarnation == info.incarnation && info.state == NodeState::Dead;
                let same_inc_suspect = incarnation == info.incarnation && info.state == NodeState::Suspect;

                if dominated || same_inc_dead || same_inc_suspect {
                    let was_dead = info.state == NodeState::Dead;
                    let was_suspect = info.state == NodeState::Suspect;
                    info.state = NodeState::Alive;
                    info.incarnation = incarnation;
                    info.addr = addr;
                    info.state_changed_at = Instant::now();

                    if was_dead {
                        events.push(ClusterEvent::NodeJoined(node, addr));
                        events.push(ClusterEvent::MembershipChanged(self.alive_members()));
                    } else if was_suspect {
                        // Suspect→Alive is a refutation — the node proved it is
                        // alive. Emit MembershipChanged so routing recomputes.
                        events.push(ClusterEvent::MembershipChanged(self.alive_members()));
                    }
                }
            }
            None => {
                self.members.insert(node, MemberInfo {
                    addr,
                    state: NodeState::Alive,
                    incarnation,
                    state_changed_at: Instant::now(),
                });
                events.push(ClusterEvent::NodeJoined(node, addr));
                events.push(ClusterEvent::MembershipChanged(self.alive_members()));
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
            events.push(ClusterEvent::NodeSuspect(node));
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

        if let Some(info) = self.members.get_mut(&node)
            && info.state != NodeState::Dead
            && incarnation >= info.incarnation
        {
            info.state = NodeState::Dead;
            info.state_changed_at = Instant::now();
            events.push(ClusterEvent::NodeLeft(node));
            events.push(ClusterEvent::MembershipChanged(self.alive_members()));
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
        let expired: Vec<(NodeId, u64)> = self.members.iter()
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
    pub fn alive_members(&self) -> Vec<NodeId> {
        let mut members: Vec<NodeId> = self.members.iter()
            .filter(|(_, info)| info.state == NodeState::Alive)
            .map(|(&id, _)| id)
            .collect();
        members.push(self.self_id);
        members.sort();
        members
    }

    /// Number of known members (all states).
    pub fn total_members(&self) -> usize {
        self.members.len() + 1 // +1 for self
    }

    /// Number of alive members (including self).
    pub fn alive_count(&self) -> usize {
        self.alive_members().len()
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
        let to_remove: Vec<NodeId> = self.members.iter()
            .filter(|(_, info)| {
                info.state == NodeState::Dead
                    && now.duration_since(info.state_changed_at) >= max_age
            })
            .map(|(&id, _)| id)
            .collect();
        for id in &to_remove {
            self.members.remove(id);
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
        let events = m.mark_alive(NodeId(2), addr(3001), 1);

        assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(2), _))));
        assert_eq!(m.alive_count(), 2);
    }

    #[test]
    fn three_nodes_form_cluster() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1);
        m.mark_alive(NodeId(3), addr(3002), 1);

        let alive = m.alive_members();
        assert_eq!(alive.len(), 3);
        assert!(alive.contains(&NodeId(1)));
        assert!(alive.contains(&NodeId(2)));
        assert!(alive.contains(&NodeId(3)));
    }

    #[test]
    fn suspect_then_dead() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1);

        let events = m.mark_suspect(NodeId(2), 1);
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeSuspect(NodeId(2)))));
        assert_eq!(m.alive_count(), 1);

        std::thread::sleep(Duration::from_millis(15));
        let events = m.expire_suspects();
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(2)))));
        assert_eq!(m.alive_count(), 1);
    }

    #[test]
    fn dead_node_rejoins() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1);
        m.mark_dead(NodeId(2), 1);
        assert_eq!(m.alive_count(), 1);

        let events = m.mark_alive(NodeId(2), addr(3001), 2); // Higher incarnation
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeJoined(NodeId(2), _))));
        assert_eq!(m.alive_count(), 2);
    }

    #[test]
    fn membership_changed_contains_sorted_list() {
        let mut m = Membership::new(NodeId(3), Duration::from_secs(5));
        m.mark_alive(NodeId(1), addr(3001), 1);
        let events = m.mark_alive(NodeId(2), addr(3002), 1);

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
        let events = m.mark_alive(NodeId(1), addr(3000), 1);
        assert!(events.is_empty());
        assert_eq!(m.total_members(), 1); // Just self
    }

    #[test]
    fn suspect_not_in_alive_list() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1);
        m.mark_suspect(NodeId(2), 1);

        let alive = m.alive_members();
        assert!(!alive.contains(&NodeId(2)));
    }

    #[test]
    fn dead_not_in_alive_list() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1);
        m.mark_dead(NodeId(2), 1);

        let alive = m.alive_members();
        assert!(!alive.contains(&NodeId(2)));
    }

    #[test]
    fn membership_changed_on_join_and_leave() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));

        let events = m.mark_alive(NodeId(2), addr(3001), 1);
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::MembershipChanged(_))));

        let events = m.mark_dead(NodeId(2), 1);
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::MembershipChanged(_))));
    }

    // --- P0-A: Incarnation-aware state transitions ---

    #[test]
    fn stale_suspect_ignored() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5);

        // Stale incarnation 3 < current 5: must be ignored
        let events = m.mark_suspect(NodeId(2), 3);
        assert!(events.is_empty());
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
    }

    #[test]
    fn suspect_at_current_incarnation() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5);

        let events = m.mark_suspect(NodeId(2), 5);
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeSuspect(NodeId(2)))));
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);
    }

    #[test]
    fn stale_dead_ignored() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5);

        // Stale incarnation 3 < current 5: must be ignored
        let events = m.mark_dead(NodeId(2), 3);
        assert!(events.is_empty());
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
    }

    #[test]
    fn dead_at_current_incarnation() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5);

        let events = m.mark_dead(NodeId(2), 5);
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(2)))));
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Dead);
    }

    #[test]
    fn alive_refutes_suspicion_same_incarnation() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5);
        m.mark_suspect(NodeId(2), 5);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);

        // Same incarnation alive refutes the suspicion
        let events = m.mark_alive(NodeId(2), addr(3001), 5);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::MembershipChanged(_))));
    }

    #[test]
    fn alive_refutes_suspicion_higher_incarnation() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 5);
        m.mark_suspect(NodeId(2), 5);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Suspect);

        // Higher incarnation alive also refutes suspicion
        let events = m.mark_alive(NodeId(2), addr(3001), 6);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().state, NodeState::Alive);
        assert_eq!(m.member_info(&NodeId(2)).unwrap().incarnation, 6);
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::MembershipChanged(_))));
    }

    #[test]
    fn forget_dead_removes_old_dead_nodes() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1);
        m.mark_alive(NodeId(3), addr(3002), 1);

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
        m.mark_alive(NodeId(2), addr(3001), 1);
        m.mark_alive(NodeId(3), addr(3002), 1);
        m.mark_suspect(NodeId(3), 1);

        // Even with zero max_age, alive and suspect nodes survive.
        let forgotten = m.forget_dead_older_than(Duration::ZERO);
        assert!(forgotten.is_empty());
        assert_eq!(m.total_members(), 3); // self + 2 peers
    }
}
