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
                if incarnation > info.incarnation
                    || (info.state != NodeState::Alive && incarnation >= info.incarnation)
                {
                    let was_dead = info.state == NodeState::Dead;
                    info.state = NodeState::Alive;
                    info.incarnation = incarnation;
                    info.addr = addr;
                    info.state_changed_at = Instant::now();

                    if was_dead {
                        events.push(ClusterEvent::NodeJoined(node, addr));
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
    pub fn mark_suspect(&mut self, node: NodeId) -> Vec<ClusterEvent> {
        let mut events = Vec::new();

        if let Some(info) = self.members.get_mut(&node)
            && info.state == NodeState::Alive
        {
            info.state = NodeState::Suspect;
            info.state_changed_at = Instant::now();
            events.push(ClusterEvent::NodeSuspect(node));
        }

        events
    }

    /// Mark a node as dead. Returns events.
    pub fn mark_dead(&mut self, node: NodeId) -> Vec<ClusterEvent> {
        let mut events = Vec::new();

        if let Some(info) = self.members.get_mut(&node)
            && info.state != NodeState::Dead
        {
            info.state = NodeState::Dead;
            info.state_changed_at = Instant::now();
            events.push(ClusterEvent::NodeLeft(node));
            events.push(ClusterEvent::MembershipChanged(self.alive_members()));
        }

        events
    }

    /// Check suspects that have exceeded the suspicion timeout and declare them dead.
    pub fn expire_suspects(&mut self) -> Vec<ClusterEvent> {
        let now = Instant::now();
        let timeout = self.suspicion_timeout;
        let expired: Vec<NodeId> = self.members.iter()
            .filter(|(_, info)| {
                info.state == NodeState::Suspect
                    && now.duration_since(info.state_changed_at) >= timeout
            })
            .map(|(&id, _)| id)
            .collect();

        let mut events = Vec::new();
        for node in expired {
            events.extend(self.mark_dead(node));
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

        let events = m.mark_suspect(NodeId(2));
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeSuspect(NodeId(2)))));
        assert_eq!(m.alive_count(), 1); // Suspect doesn't count as alive in member list...
        // Actually suspects are still in the alive list until declared dead
        // Let me check: alive_members filters for state == Alive
        // So a suspect node is NOT in alive_members
        // That's correct per SWIM: suspects are not used for routing

        std::thread::sleep(Duration::from_millis(15));
        let events = m.expire_suspects();
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::NodeLeft(NodeId(2)))));
        assert_eq!(m.alive_count(), 1);
    }

    #[test]
    fn dead_node_rejoins() {
        let mut m = Membership::new(NodeId(1), Duration::from_millis(10));
        m.mark_alive(NodeId(2), addr(3001), 1);
        m.mark_dead(NodeId(2));
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
        m.mark_suspect(NodeId(2));

        let alive = m.alive_members();
        assert!(!alive.contains(&NodeId(2)));
    }

    #[test]
    fn dead_not_in_alive_list() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));
        m.mark_alive(NodeId(2), addr(3001), 1);
        m.mark_dead(NodeId(2));

        let alive = m.alive_members();
        assert!(!alive.contains(&NodeId(2)));
    }

    #[test]
    fn membership_changed_on_join_and_leave() {
        let mut m = Membership::new(NodeId(1), Duration::from_secs(5));

        let events = m.mark_alive(NodeId(2), addr(3001), 1);
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::MembershipChanged(_))));

        let events = m.mark_dead(NodeId(2));
        assert!(events.iter().any(|e| matches!(e, ClusterEvent::MembershipChanged(_))));
    }
}
