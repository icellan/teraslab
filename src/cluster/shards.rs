//! Deterministic hash-based shard table.
//!
//! 4096 shards, assigned round-robin over sorted members. Every node
//! computes the identical table from the same member list — no consensus
//! protocol or leader election needed.

use crate::index::TxKey;

/// Total number of shards (12-bit hash → 4096).
pub const NUM_SHARDS: usize = 4096;

/// Identifies a node in the cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct NodeId(pub u64);

/// Assignment of a single shard to a master and zero or more replicas.
#[derive(Clone, Debug, PartialEq)]
pub struct ShardAssignment {
    /// Primary owner of this shard.
    pub master: NodeId,
    /// Replica nodes for this shard (empty if RF=1 or not enough nodes).
    pub replicas: Vec<NodeId>,
}

/// A task describing one shard that needs to migrate between nodes.
#[derive(Clone, Debug, PartialEq)]
pub struct MigrationTask {
    /// Shard number (0–4095).
    pub shard: u16,
    /// Node currently holding the shard's data.
    pub from_node: NodeId,
    /// Node that should become the new owner.
    pub to_node: NodeId,
    /// Whether this is a master migration (vs replica).
    pub is_master: bool,
}

/// Per-shard handoff state for two-phase topology activation.
///
/// Each shard transitions independently: the old assignment stays
/// authoritative for serving until the target has durably received
/// all data and the handoff is committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardHandoff {
    /// Shard is serving from the current (old) assignment.
    /// This is the state before any migration starts, or if no
    /// ownership change is needed for this shard.
    ServingCurrent,
    /// Handoff in progress — old owner still serves reads/writes.
    /// The target is receiving data but is NOT yet authoritative.
    Copying,
    /// Target has all data; waiting for commit confirmation.
    /// Old owner still serves to avoid gaps.
    CommitReady,
    /// Handoff committed — new assignment is authoritative.
    /// The shard is now served from the new assignment.
    ServingNew,
}

/// The shard table: maps each shard to its master and replicas.
///
/// Supports two-phase topology activation: when a membership change
/// computes a new assignment, each shard transitions individually
/// from the old assignment to the new one. Routing uses the old
/// assignment until a shard's handoff reaches `ServingNew`.
#[derive(Clone)]
pub struct ShardTable {
    assignments: Vec<ShardAssignment>,
    /// Previous assignments (before the current topology change).
    /// Used for routing shards that haven't completed handoff yet.
    prev_assignments: Option<Vec<ShardAssignment>>,
    /// Per-shard handoff state. When `None`, all shards use `assignments`
    /// directly (no handoff in progress).
    handoff_state: Option<Vec<ShardHandoff>>,
    /// Monotonic topology epoch. Incremented on every membership change.
    pub version: u64,
    /// Replication factor used to compute this table.
    rf: u8,
}

impl ShardTable {
    /// Compute a shard table deterministically from a sorted member list.
    ///
    /// This is a **pure function**: same inputs → same output on every node.
    ///
    /// Algorithm (round-robin):
    /// - Shard N's master = `members[N % len]`
    /// - Shard N's replica i = `members[(N + i) % len]` (if != master)
    /// - Replicas clamped to available nodes (no node appears twice per shard)
    ///
    /// # Panics
    ///
    /// Panics if `members` is empty.
    /// Compute a shard table with an explicit monotonic epoch.
    ///
    /// `epoch` must be strictly greater than the previous table's epoch.
    /// Each topology change increments the epoch so ownership transitions
    /// are totally ordered and stale views can be detected.
    pub fn compute_with_epoch(members: &[NodeId], replication_factor: u8, epoch: u64) -> Self {
        assert!(!members.is_empty(), "cannot compute shard table with 0 members");
        let n = members.len();
        let mut assignments = Vec::with_capacity(NUM_SHARDS);

        for shard in 0..NUM_SHARDS {
            let master = members[shard % n];
            let mut replicas = Vec::new();
            for r in 1..replication_factor as usize {
                if r >= n {
                    break;
                }
                let replica = members[(shard + r) % n];
                if replica != master {
                    replicas.push(replica);
                }
            }
            assignments.push(ShardAssignment { master, replicas });
        }

        ShardTable {
            assignments,
            prev_assignments: None,
            handoff_state: None,
            version: epoch,
            rf: replication_factor,
        }
    }

    /// Begin a two-phase topology activation.
    ///
    /// Saves the current assignments as `prev_assignments` and installs
    /// the new assignments. Shards whose master changed start in `Copying`;
    /// unchanged shards go directly to `ServingNew`.
    pub fn begin_handoff(&mut self, new_table: &ShardTable) {
        let mut handoff = vec![ShardHandoff::ServingNew; NUM_SHARDS];
        for (shard, h_state) in handoff.iter_mut().enumerate() {
            let old_master = self.assignments[shard].master;
            let new_master = new_table.assignments[shard].master;
            if old_master != new_master {
                *h_state = ShardHandoff::Copying;
            }
        }
        self.prev_assignments = Some(self.assignments.clone());
        self.assignments = new_table.assignments.clone();
        self.handoff_state = Some(handoff);
        self.version = new_table.version;
    }

    /// Commit the handoff for a single shard — it now serves from the
    /// new assignment.
    pub fn commit_shard(&mut self, shard: u16) {
        if let Some(ref mut hs) = self.handoff_state {
            hs[shard as usize] = ShardHandoff::ServingNew;
            // If all shards are now ServingNew, clear the handoff state.
            if hs.iter().all(|s| *s == ShardHandoff::ServingNew) {
                self.prev_assignments = None;
                self.handoff_state = None;
            }
        }
    }

    /// Mark a shard as ready to commit (target has all data).
    pub fn mark_commit_ready(&mut self, shard: u16) {
        if let Some(ref mut hs) = self.handoff_state
            && hs[shard as usize] == ShardHandoff::Copying
        {
            hs[shard as usize] = ShardHandoff::CommitReady;
        }
    }

    /// Get the effective assignment for a shard, considering handoff state.
    ///
    /// During two-phase activation, shards that haven't completed handoff
    /// use the previous (old) assignment. This ensures the old master
    /// remains authoritative until the target has all data.
    pub fn effective_assignment(&self, shard: u16) -> &ShardAssignment {
        match (&self.handoff_state, &self.prev_assignments) {
            (Some(hs), Some(prev)) => {
                match hs[shard as usize] {
                    ShardHandoff::ServingCurrent | ShardHandoff::Copying | ShardHandoff::CommitReady => {
                        &prev[shard as usize]
                    }
                    ShardHandoff::ServingNew => {
                        &self.assignments[shard as usize]
                    }
                }
            }
            _ => &self.assignments[shard as usize],
        }
    }

    /// Get the handoff state for a shard.
    pub fn shard_handoff_state(&self, shard: u16) -> ShardHandoff {
        match &self.handoff_state {
            Some(hs) => hs[shard as usize],
            None => ShardHandoff::ServingNew,
        }
    }

    /// Rollback a shard's handoff — revert to the old (previous) assignment.
    ///
    /// Used when a migration fails: the old master must remain authoritative
    /// for this shard instead of the new (unreachable) target. Without this,
    /// lifting the write fence while the shard table points to the new master
    /// creates a window where no node serves the shard.
    ///
    /// After rollback the shard is `ServingNew` with the old assignment
    /// restored, so routing sends traffic back to the original master.
    pub fn rollback_shard(&mut self, shard: u16) {
        let old_assignment = match &self.prev_assignments {
            Some(prev) => prev[shard as usize].clone(),
            None => return, // No handoff in progress.
        };
        self.assignments[shard as usize] = old_assignment;
        if let Some(hs) = &mut self.handoff_state {
            hs[shard as usize] = ShardHandoff::ServingNew;
            if hs.iter().all(|s| *s == ShardHandoff::ServingNew) {
                self.prev_assignments = None;
                self.handoff_state = None;
            }
        }
    }

    /// Number of shards still in handoff (not yet ServingNew).
    pub fn pending_handoff_count(&self) -> usize {
        match &self.handoff_state {
            Some(hs) => hs.iter().filter(|s| **s != ShardHandoff::ServingNew).count(),
            None => 0,
        }
    }

    /// Compute a shard table with a hash-based version (legacy).
    ///
    /// Prefer `compute_with_epoch` in production; this exists for
    /// backward compatibility in tests and bootstrap paths.
    pub fn compute(members: &[NodeId], replication_factor: u8) -> Self {
        let mut version_hash: u64 = 0;
        for (i, m) in members.iter().enumerate() {
            version_hash = version_hash.wrapping_add(m.0.wrapping_mul(i as u64 + 1));
        }
        Self::compute_with_epoch(members, replication_factor, version_hash)
    }

    /// The replication factor used to compute this table.
    pub fn replication_factor(&self) -> u8 {
        self.rf
    }

    /// Compute which shard a key belongs to (12-bit hash → 0–4095).
    pub fn shard_for_key(key: &TxKey) -> u16 {
        let h = u16::from_le_bytes([key.txid[0], key.txid[1]]);
        h & 0x0FFF
    }

    /// Which node is the master for this key?
    pub fn master_for_key(&self, key: &TxKey) -> NodeId {
        let shard = Self::shard_for_key(key) as usize;
        self.assignments[shard].master
    }

    /// Which nodes hold replicas for this key?
    pub fn replicas_for_key(&self, key: &TxKey) -> &[NodeId] {
        let shard = Self::shard_for_key(key) as usize;
        &self.assignments[shard].replicas
    }

    /// Get the assignment for a specific shard.
    /// Get the assignment for a shard.
    ///
    /// During two-phase activation, returns the **effective** assignment:
    /// old master for shards still in handoff, new master for committed
    /// shards. This ensures routing stays with the old owner until the
    /// target has all data.
    pub fn assignment(&self, shard: u16) -> &ShardAssignment {
        self.effective_assignment(shard)
    }

    /// Get the target (new) assignment for a shard, regardless of handoff state.
    /// Used by migration code to know where data should go.
    pub fn target_assignment(&self, shard: u16) -> &ShardAssignment {
        &self.assignments[shard as usize]
    }

    /// Count how many shards each node masters.
    pub fn shard_counts(&self) -> std::collections::HashMap<NodeId, usize> {
        let mut counts = std::collections::HashMap::new();
        for a in &self.assignments {
            *counts.entry(a.master).or_insert(0) += 1;
        }
        counts
    }

    /// Compute which shards need to migrate between an old and new table.
    ///
    /// Only master migrations are tracked (replica migrations follow).
    ///
    /// When the old master is no longer in the new member list (dead node),
    /// the migration source is set to the old replica instead (if one exists
    /// and is still alive). If the new master was already the old replica,
    /// no migration task is generated — the data is already in place.
    pub fn migration_plan(old: &ShardTable, new: &ShardTable) -> Vec<MigrationTask> {
        // Determine which nodes are alive in the new table
        let new_members: std::collections::HashSet<NodeId> = {
            let mut set = std::collections::HashSet::new();
            for a in &new.assignments {
                set.insert(a.master);
                for r in &a.replicas {
                    set.insert(*r);
                }
            }
            set
        };

        let mut tasks = Vec::new();
        for shard in 0..NUM_SHARDS {
            let old_assignment = &old.assignments[shard];
            let old_master = old_assignment.master;
            let new_master = new.assignments[shard].master;
            if old_master == new_master {
                continue;
            }

            // Check if the old master is dead (not in the new member set)
            if !new_members.contains(&old_master) {
                // Old master is dead. Check if the new master was the old replica.
                // If so, the data is already on the new master — no migration needed.
                if old_assignment.replicas.contains(&new_master) {
                    continue; // Data already in place via replication
                }
                // New master is NOT the old replica. Find a surviving replica
                // that can serve as the migration source.
                let surviving_replica = old_assignment.replicas.iter()
                    .find(|r| new_members.contains(r));
                if let Some(&source) = surviving_replica {
                    tasks.push(MigrationTask {
                        shard: shard as u16,
                        from_node: source,
                        to_node: new_master,
                        is_master: true,
                    });
                }
                // If no surviving replica, the data is lost (RF=2, both nodes dead)
            } else {
                // Normal case: old master is alive, migrate from it
                tasks.push(MigrationTask {
                    shard: shard as u16,
                    from_node: old_master,
                    to_node: new_master,
                    is_master: true,
                });
            }
        }
        tasks
    }

    /// Compute migration tasks for newly assigned replicas.
    ///
    /// When the shard table changes, replicas may be reassigned to different
    /// nodes. This method identifies shards where a new replica was assigned
    /// that was NOT a replica (or master) in the old table, meaning it needs
    /// the shard data backfilled.
    pub fn replica_migration_plan(old: &ShardTable, new: &ShardTable) -> Vec<MigrationTask> {
        let mut tasks = Vec::new();
        for shard in 0..NUM_SHARDS {
            let old_a = &old.assignments[shard];
            let new_a = &new.assignments[shard];

            for &new_replica in &new_a.replicas {
                // Skip if the node was already a replica or master for this shard
                if old_a.replicas.contains(&new_replica) || old_a.master == new_replica {
                    continue;
                }
                // This is a new replica — it needs data from the current master
                tasks.push(MigrationTask {
                    shard: shard as u16,
                    from_node: new_a.master,
                    to_node: new_replica,
                    is_master: false,
                });
            }
        }
        tasks
    }
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

/// Decision for an incoming request: handle locally or redirect.
#[derive(Debug, Clone, PartialEq)]
pub enum RouteDecision {
    /// This node is the master — handle the request.
    HandleLocally,
    /// Redirect the client to the correct master.
    RedirectTo {
        node: NodeId,
        shard_table_version: u64,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().map(|&id| NodeId(id)).collect()
    }

    #[test]
    fn shard_for_key_deterministic() {
        let mut txid = [0u8; 32];
        txid[0] = 0xAB;
        txid[1] = 0xCD;
        let key = TxKey { txid };
        let s1 = ShardTable::shard_for_key(&key);
        let s2 = ShardTable::shard_for_key(&key);
        assert_eq!(s1, s2);
        assert!(s1 < NUM_SHARDS as u16);
    }

    #[test]
    fn shard_for_key_distribution() {
        let mut counts = vec![0u32; NUM_SHARDS];
        for i in 0..100_000u32 {
            let mut txid = [0u8; 32];
            txid[0..4].copy_from_slice(&i.to_le_bytes());
            let key = TxKey { txid };
            let shard = ShardTable::shard_for_key(&key) as usize;
            counts[shard] += 1;
        }
        let expected = 100_000.0 / NUM_SHARDS as f64;
        let max_deviation = counts.iter().map(|&c| (c as f64 - expected).abs()).fold(0.0f64, f64::max);
        // Within 50% of expected per shard is reasonable for uniform distribution
        assert!(max_deviation < expected * 0.5, "distribution too skewed: max deviation {max_deviation}");
    }

    #[test]
    fn compute_deterministic() {
        let members = nodes(&[1, 2, 3]);
        let t1 = ShardTable::compute(&members, 2);
        let t2 = ShardTable::compute(&members, 2);
        assert_eq!(t1.version, t2.version);
        for i in 0..NUM_SHARDS {
            assert_eq!(t1.assignments[i], t2.assignments[i]);
        }
    }

    #[test]
    fn compute_same_on_different_nodes() {
        // Simulate two nodes independently computing the shard table
        let members = nodes(&[1, 2, 3]);
        let table_node_a = ShardTable::compute(&members, 2);
        let table_node_b = ShardTable::compute(&members, 2);

        for i in 0..NUM_SHARDS {
            assert_eq!(
                table_node_a.assignments[i],
                table_node_b.assignments[i],
                "shard {i} differs"
            );
        }
        assert_eq!(table_node_a.version, table_node_b.version);
    }

    #[test]
    fn three_nodes_rf2_round_robin() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute(&members, 2);

        // Shard 0 → master=members[0]=1
        assert_eq!(table.assignments[0].master, NodeId(1));
        // Shard 1 → master=members[1]=2
        assert_eq!(table.assignments[1].master, NodeId(2));
        // Shard 2 → master=members[2]=3
        assert_eq!(table.assignments[2].master, NodeId(3));
        // Shard 3 → master=members[0]=1 (wraps)
        assert_eq!(table.assignments[3].master, NodeId(1));
    }

    #[test]
    fn three_nodes_rf2_no_self_replica() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute(&members, 2);

        for (i, a) in table.assignments.iter().enumerate() {
            assert!(!a.replicas.contains(&a.master),
                "shard {i}: master {:?} is also a replica", a.master);
        }
    }

    #[test]
    fn three_nodes_rf2_every_shard_has_replica() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute(&members, 2);

        for (i, a) in table.assignments.iter().enumerate() {
            assert_eq!(a.replicas.len(), 1, "shard {i} should have 1 replica");
        }
    }

    #[test]
    fn five_nodes_rf2_balanced() {
        let members = nodes(&[1, 2, 3, 4, 5]);
        let table = ShardTable::compute(&members, 2);
        let counts = table.shard_counts();

        let expected = NUM_SHARDS / 5;
        for (&node, &count) in &counts {
            let deviation = (count as f64 - expected as f64).abs() / expected as f64;
            assert!(deviation < 0.05, "node {node:?} has {count} shards, expected ~{expected} (deviation {deviation:.2})");
        }
    }

    #[test]
    fn every_shard_has_master() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute(&members, 2);
        for (i, a) in table.assignments.iter().enumerate() {
            assert!(a.master.0 > 0, "shard {i} has no master");
        }
    }

    #[test]
    fn two_nodes_rf2_complementary() {
        let members = nodes(&[1, 2]);
        let table = ShardTable::compute(&members, 2);

        for a in &table.assignments {
            assert_eq!(a.replicas.len(), 1);
            assert_ne!(a.master, a.replicas[0]);
        }
    }

    #[test]
    fn one_node_rf2_no_replicas() {
        let members = nodes(&[1]);
        let table = ShardTable::compute(&members, 2);

        for a in &table.assignments {
            assert_eq!(a.master, NodeId(1));
            assert!(a.replicas.is_empty());
        }
    }

    #[test]
    fn migration_plan_node_added() {
        let old_members = nodes(&[1, 2, 3]);
        let new_members = nodes(&[1, 2, 3, 4]);
        let old_table = ShardTable::compute(&old_members, 2);
        let new_table = ShardTable::compute(&new_members, 2);

        let plan = ShardTable::migration_plan(&old_table, &new_table);
        assert!(!plan.is_empty(), "adding a node should trigger migrations");

        // Some shards should migrate TO node 4
        let to_node4: Vec<_> = plan.iter().filter(|t| t.to_node == NodeId(4)).collect();
        assert!(!to_node4.is_empty(), "node 4 should receive shards");
    }

    #[test]
    fn migration_plan_node_removed() {
        let old_members = nodes(&[1, 2, 3, 4]);
        let new_members = nodes(&[1, 2, 3]);
        let old_table = ShardTable::compute(&old_members, 2);
        let new_table = ShardTable::compute(&new_members, 2);

        let plan = ShardTable::migration_plan(&old_table, &new_table);

        // When node4 is removed, shards previously mastered by node4 that
        // had a replica on the new master need no migration (data already
        // in place). Only shards where the new master differs from the old
        // replica require migration from a surviving replica.
        // There should be no tasks with from_node == NodeId(4) since it's dead.
        let from_dead: Vec<_> = plan.iter().filter(|t| t.from_node == NodeId(4)).collect();
        assert!(from_dead.is_empty(),
            "dead node 4 should not be a migration source, but found {} tasks from it",
            from_dead.len());

        // All migration sources should be surviving nodes
        for task in &plan {
            assert!(new_members.contains(&task.from_node),
                "migration source {:?} should be a surviving node", task.from_node);
        }
    }

    #[test]
    fn migration_plan_no_unnecessary_moves() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute(&members, 2);

        // Same members → no migrations needed
        let plan = ShardTable::migration_plan(&table, &table);
        assert!(plan.is_empty());
    }

    #[test]
    fn master_for_key() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute(&members, 2);

        let mut txid = [0u8; 32];
        txid[0] = 42;
        let key = TxKey { txid };
        let master = table.master_for_key(&key);
        assert!(master == NodeId(1) || master == NodeId(2) || master == NodeId(3));
    }

    #[test]
    fn replicas_for_key() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute(&members, 2);

        let mut txid = [0u8; 32];
        txid[0] = 42;
        let key = TxKey { txid };
        let replicas = table.replicas_for_key(&key);
        assert_eq!(replicas.len(), 1);
        assert_ne!(replicas[0], table.master_for_key(&key));
    }

    #[test]
    fn shard_table_version_changes_with_members() {
        let t1 = ShardTable::compute(&nodes(&[1, 2, 3]), 2);
        let t2 = ShardTable::compute(&nodes(&[1, 2, 3, 4]), 2);
        assert_ne!(t1.version, t2.version);
    }

    #[test]
    fn route_decision() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute(&members, 2);
        let self_id = NodeId(1);

        let mut txid = [0u8; 32];
        // Find a key that maps to node 1
        let mut found_local = false;
        let mut found_remote = false;
        for i in 0..100u8 {
            txid[0] = i;
            let key = TxKey { txid };
            let master = table.master_for_key(&key);
            if master == self_id {
                found_local = true;
            } else {
                found_remote = true;
            }
            if found_local && found_remote {
                break;
            }
        }
        assert!(found_local, "should find at least one local key");
        assert!(found_remote, "should find at least one remote key");
    }

    #[test]
    fn rollback_shard_restores_old_master() {
        let old_members = nodes(&[1, 2, 3]);
        let new_members = nodes(&[1, 2, 3, 4]);
        let mut table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);

        // Find a shard that changes master between old and new.
        let mut changed_shard = None;
        for shard in 0..NUM_SHARDS as u16 {
            let old_master = table.assignment(shard).master;
            let new_master = new_table.target_assignment(shard).master;
            if old_master != new_master {
                changed_shard = Some((shard, old_master, new_master));
                break;
            }
        }
        let (shard, old_master, new_master) = changed_shard.expect("should have at least one changed shard");

        // Begin handoff — old master still serves during handoff.
        table.begin_handoff(&new_table);
        assert_eq!(table.effective_assignment(shard).master, old_master);
        assert_eq!(table.shard_handoff_state(shard), ShardHandoff::Copying);

        // Rollback — old master is restored as the canonical assignment.
        table.rollback_shard(shard);
        assert_eq!(table.assignment(shard).master, old_master);
        assert_eq!(table.shard_handoff_state(shard), ShardHandoff::ServingNew);
        // The target assignment is now also the old master (reverted).
        assert_eq!(table.target_assignment(shard).master, old_master);
    }

    #[test]
    fn rollback_clears_handoff_when_all_done() {
        let old_members = nodes(&[1, 2]);
        let new_members = nodes(&[1, 2, 3]);
        let mut table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);

        table.begin_handoff(&new_table);
        let pending_before = table.pending_handoff_count();
        assert!(pending_before > 0);

        // Commit all shards that changed, or rollback — either way clears handoff.
        for shard in 0..NUM_SHARDS as u16 {
            if table.shard_handoff_state(shard) != ShardHandoff::ServingNew {
                table.rollback_shard(shard);
            }
        }
        assert_eq!(table.pending_handoff_count(), 0);
    }

    #[test]
    fn rollback_noop_without_handoff() {
        let members = nodes(&[1, 2, 3]);
        let mut table = ShardTable::compute(&members, 2);
        let original_master = table.assignment(0).master;
        // Rollback without active handoff is a no-op.
        table.rollback_shard(0);
        assert_eq!(table.assignment(0).master, original_master);
    }
}
