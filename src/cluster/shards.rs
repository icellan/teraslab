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
    /// Tracks shards where the new master is still receiving inbound migration
    /// data. A subset master must not be treated as authoritative until
    /// migration completes — `is_master()` returns `Transitioning` for these.
    master_subset: Vec<bool>,
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
        assert!(
            !members.is_empty(),
            "cannot compute shard table with 0 members"
        );
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
            master_subset: vec![false; NUM_SHARDS],
        }
    }

    /// Begin a two-phase topology activation.
    ///
    /// Saves the current assignments as `prev_assignments` and installs
    /// the new assignments. Shards whose master changed start in `Copying`;
    /// unchanged shards go directly to `ServingNew`.
    pub fn begin_handoff(&mut self, new_table: &ShardTable) {
        self.begin_handoff_with(new_table, |_| true);
    }

    /// Like [`begin_handoff`](Self::begin_handoff), but with a callback that
    /// indicates whether each shard has data on this node. Empty shards skip
    /// the Copying state entirely, avoiding handoff stalls on fresh or
    /// sparsely populated clusters where there is nothing to migrate.
    pub fn begin_handoff_with(
        &mut self,
        new_table: &ShardTable,
        shard_has_data: impl Fn(u16) -> bool,
    ) {
        let mut handoff = vec![ShardHandoff::ServingNew; NUM_SHARDS];
        let mut master_subset = vec![false; NUM_SHARDS];
        for (shard, h_state) in handoff.iter_mut().enumerate() {
            let old_master = self.assignments[shard].master;
            let new_master = new_table.assignments[shard].master;
            if old_master != new_master {
                master_subset[shard] = true;
                if shard_has_data(shard as u16) {
                    *h_state = ShardHandoff::Copying;
                }
            }
        }
        let all_serving = handoff.iter().all(|s| *s == ShardHandoff::ServingNew);
        self.prev_assignments = Some(self.assignments.clone());
        self.assignments = new_table.assignments.clone();
        self.handoff_state = Some(handoff);
        self.master_subset = master_subset;
        self.version = new_table.version;

        // If no shards need copying, clear handoff state immediately.
        // Also clear master_subset: no inbound migration will run, so these
        // shards are never in a "receiving data" state regardless of ownership
        // change.
        if all_serving {
            self.prev_assignments = None;
            self.handoff_state = None;
            self.master_subset = vec![false; NUM_SHARDS];
        }
    }

    /// Commit the handoff for a single shard — it now serves from the
    /// new assignment.
    pub fn commit_shard(&mut self, shard: u16) {
        self.master_subset[shard as usize] = false;
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
            (Some(hs), Some(prev)) => match hs[shard as usize] {
                ShardHandoff::ServingCurrent
                | ShardHandoff::Copying
                | ShardHandoff::CommitReady => &prev[shard as usize],
                ShardHandoff::ServingNew => &self.assignments[shard as usize],
            },
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
        // Only rollback shards that are in Copying or CommitReady state.
        // Shards already ServingNew have been committed to the new
        // assignment — rolling them back to an old (possibly dead)
        // master would make the shard unreachable.
        if let Some(hs) = &mut self.handoff_state {
            match hs[shard as usize] {
                ShardHandoff::Copying
                | ShardHandoff::CommitReady
                | ShardHandoff::ServingCurrent => {
                    self.assignments[shard as usize] = old_assignment;
                    hs[shard as usize] = ShardHandoff::ServingNew;
                    // Rolled back to old master — no longer a subset master.
                    self.master_subset[shard as usize] = false;
                }
                ShardHandoff::ServingNew => {
                    // Already committed to the new assignment — don't rollback.
                }
            }
            if hs.iter().all(|s| *s == ShardHandoff::ServingNew) {
                self.prev_assignments = None;
                self.handoff_state = None;
            }
        } else {
            // No handoff state means all shards are using assignments directly.
            // Rollback is a no-op since there's no prev to restore.
        }
    }

    /// Number of shards still in handoff (not yet ServingNew).
    pub fn pending_handoff_count(&self) -> usize {
        match &self.handoff_state {
            Some(hs) => hs
                .iter()
                .filter(|s| **s != ShardHandoff::ServingNew)
                .count(),
            None => 0,
        }
    }

    /// Returns `true` if the new master for `shard` is still in the subset
    /// state — i.e. ownership changed in the last topology activation and
    /// migration data has not yet been committed for this shard.
    ///
    /// A subset master must not serve requests as authoritative until it
    /// receives all migration data. `is_master()` in the coordinator
    /// returns `Transitioning` for subset masters so callers retry.
    pub fn is_subset_master(&self, shard: u16) -> bool {
        self.master_subset[shard as usize]
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

    /// Phase F — override the master for `shard` to `new_master`, demoting
    /// the previous master into the replica set so the same node set is
    /// preserved.
    ///
    /// Intended to run on a freshly built target table (e.g. immediately
    /// after `compute_with_epoch`) BEFORE `begin_handoff_with` is
    /// invoked, so the per-shard `master_subset` flag is computed against
    /// the elected master rather than the round-robin master.
    ///
    /// `new_master` MUST be a member of the shard's current target
    /// assignment (master or replica). Calling with an unrelated node is a
    /// no-op so a stale partition-view entry cannot corrupt the table.
    pub fn set_master_for_shard(&mut self, shard: u16, new_master: NodeId) {
        let idx = shard as usize;
        let current = &mut self.assignments[idx];
        if current.master == new_master {
            return;
        }
        let promote_idx = current.replicas.iter().position(|n| *n == new_master);
        let Some(replica_idx) = promote_idx else {
            // `new_master` is not in this shard's assignment — refuse to
            // mutate so we don't fabricate an arbitrary cross-shard owner.
            tracing::warn!(
                shard,
                current_master = ?current.master,
                candidate_master = ?new_master,
                replicas = ?current.replicas,
                "set_master_for_shard ignored candidate outside shard assignment",
            );
            return;
        };
        let demoted = std::mem::replace(&mut current.master, new_master);
        current.replicas[replica_idx] = demoted;
    }

    /// Count how many shards each node masters.
    pub fn shard_counts(&self) -> std::collections::HashMap<NodeId, usize> {
        let mut counts = std::collections::HashMap::new();
        for a in &self.assignments {
            *counts.entry(a.master).or_insert(0) += 1;
        }
        counts
    }

    /// Return all shard numbers where `node` is master or replica in the
    /// current (target) assignments. Used by orphan cleanup to determine
    /// which records this node should keep after migrations complete.
    pub fn shards_owned_by(&self, node: NodeId) -> std::collections::HashSet<u16> {
        let mut owned = std::collections::HashSet::new();
        for shard in 0..NUM_SHARDS as u16 {
            let a = &self.assignments[shard as usize];
            if a.master == node || a.replicas.contains(&node) {
                owned.insert(shard);
            }
        }
        owned
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
                let surviving_replica = old_assignment
                    .replicas
                    .iter()
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
                // Old master is alive — always generate a migration task.
                // The full handoff (Copying + delta streaming) ensures that
                // any in-flight writes on the old master during the topology
                // change are captured and forwarded to the new master.
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
        let new_members: std::collections::HashSet<NodeId> = {
            let mut set = std::collections::HashSet::new();
            for a in &new.assignments {
                set.insert(a.master);
                for &r in &a.replicas {
                    set.insert(r);
                }
            }
            set
        };
        for shard in 0..NUM_SHARDS {
            let old_a = &old.assignments[shard];
            let new_a = &new.assignments[shard];

            for &new_replica in &new_a.replicas {
                // Skip if the node was already a replica or master for this shard
                if old_a.replicas.contains(&new_replica) || old_a.master == new_replica {
                    continue;
                }
                let source = if new_members.contains(&old_a.master) {
                    Some(old_a.master)
                } else if old_a.replicas.contains(&new_a.master) {
                    Some(new_a.master)
                } else {
                    old_a
                        .replicas
                        .iter()
                        .copied()
                        .find(|r| new_members.contains(r))
                };
                let Some(source) = source else {
                    continue;
                };
                tasks.push(MigrationTask {
                    shard: shard as u16,
                    from_node: source,
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
        let max_deviation = counts
            .iter()
            .map(|&c| (c as f64 - expected).abs())
            .fold(0.0f64, f64::max);
        // Within 50% of expected per shard is reasonable for uniform distribution
        assert!(
            max_deviation < expected * 0.5,
            "distribution too skewed: max deviation {max_deviation}"
        );
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
                table_node_a.assignments[i], table_node_b.assignments[i],
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
            assert!(
                !a.replicas.contains(&a.master),
                "shard {i}: master {:?} is also a replica",
                a.master
            );
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
            assert!(
                deviation < 0.05,
                "node {node:?} has {count} shards, expected ~{expected} (deviation {deviation:.2})"
            );
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
        assert!(
            from_dead.is_empty(),
            "dead node 4 should not be a migration source, but found {} tasks from it",
            from_dead.len()
        );

        // All migration sources should be surviving nodes
        for task in &plan {
            assert!(
                new_members.contains(&task.from_node),
                "migration source {:?} should be a surviving node",
                task.from_node
            );
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
    fn migration_plan_uses_single_source_for_live_master_move() {
        let old_members = nodes(&[1, 2, 3]);
        let new_members = nodes(&[1, 2, 3, 4]);
        let old_table = ShardTable::compute(&old_members, 2);
        let new_table = ShardTable::compute(&new_members, 2);

        let shard = (0..NUM_SHARDS)
            .find(|&shard| {
                let old_assignment = old_table.assignment(shard as u16);
                let new_assignment = new_table.assignment(shard as u16);
                old_assignment.master != new_assignment.master
                    && !old_assignment.replicas.contains(&new_assignment.master)
            })
            .expect("expected a shard whose new master is a brand-new holder");

        let plan = ShardTable::migration_plan(&old_table, &new_table);
        let shard_tasks: Vec<_> = plan
            .iter()
            .filter(|task| task.shard as usize == shard)
            .collect();

        assert_eq!(
            shard_tasks.len(),
            1,
            "a live master move should stream from the authoritative old master only",
        );
        assert!(shard_tasks[0].is_master);
        assert_eq!(
            shard_tasks[0].from_node,
            old_table.assignment(shard as u16).master,
            "the old master should be the single source for a live master move",
        );
        assert_eq!(
            shard_tasks[0].to_node,
            new_table.assignment(shard as u16).master,
            "the task should target the new master",
        );
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
        let (shard, old_master, _new_master) =
            changed_shard.expect("should have at least one changed shard");

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
    fn set_master_logs_when_node_not_in_assignment() {
        use std::sync::{Arc, Mutex};
        use tracing::Event;
        use tracing::field::{Field, Visit};
        use tracing_subscriber::Layer;
        use tracing_subscriber::layer::Context;
        use tracing_subscriber::prelude::*;
        use tracing_subscriber::registry::LookupSpan;

        #[derive(Default)]
        struct CaptureLayer {
            warnings: Arc<Mutex<Vec<String>>>,
        }

        #[derive(Default)]
        struct MessageVisitor {
            message: Option<String>,
        }

        impl Visit for MessageVisitor {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.message = Some(format!("{value:?}"));
                }
            }
        }

        impl<S> Layer<S> for CaptureLayer
        where
            S: tracing::Subscriber + for<'a> LookupSpan<'a>,
        {
            fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
                if event.metadata().level() != &tracing::Level::WARN {
                    return;
                }
                let mut visitor = MessageVisitor::default();
                event.record(&mut visitor);
                if let Some(message) = visitor.message {
                    self.warnings.lock().expect("capture lock").push(message);
                }
            }
        }

        let warnings = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("warn"))
            .with(CaptureLayer {
                warnings: warnings.clone(),
            });

        let mut table = ShardTable::compute_with_epoch(&nodes(&[1, 2]), 2, 1);
        let before = table.target_assignment(0).clone();

        tracing::subscriber::with_default(subscriber, || {
            table.set_master_for_shard(0, NodeId(99));
        });

        assert_eq!(table.target_assignment(0), &before);
        assert!(
            warnings
                .lock()
                .expect("capture lock")
                .iter()
                .any(|msg| msg.contains("set_master_for_shard ignored candidate")),
            "expected warning for ignored unrelated master candidate"
        );
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

    #[test]
    fn shards_owned_by_includes_master_and_replica() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute_with_epoch(&members, 2, 1);

        let owned1 = table.shards_owned_by(NodeId(1));
        let owned2 = table.shards_owned_by(NodeId(2));
        let owned3 = table.shards_owned_by(NodeId(3));

        // With 3 nodes and RF=2, each node owns ~2/3 of all shards
        // (master for ~1/3, replica for ~1/3).
        assert!(owned1.len() > 2700 && owned1.len() < 2740);
        assert!(owned2.len() > 2700 && owned2.len() < 2740);
        assert!(owned3.len() > 2700 && owned3.len() < 2740);

        // Every shard should be owned by exactly 2 nodes (RF=2).
        for shard in 0..NUM_SHARDS as u16 {
            let count = [&owned1, &owned2, &owned3]
                .iter()
                .filter(|s| s.contains(&shard))
                .count();
            assert_eq!(count, 2, "shard {shard} owned by {count} nodes, expected 2");
        }
    }

    #[test]
    fn shards_owned_by_excludes_non_member() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute_with_epoch(&members, 2, 1);
        let owned = table.shards_owned_by(NodeId(99));
        assert!(owned.is_empty());
    }

    // -----------------------------------------------------------------------
    // Part 2.1: Deterministic shard assignment — same inputs always same output
    // -----------------------------------------------------------------------

    #[test]
    fn compute_same_members_different_order_identical() {
        // [1,2,3] vs [3,1,2] — MUST produce identical tables.
        // This catches non-determinism from HashMap iteration order.
        // Note: members must be sorted before compute (the implementation
        // assumes sorted input). So we verify that if the caller sorts
        // them, order doesn't matter.
        let mut a = nodes(&[3, 1, 2]);
        a.sort();
        let mut b = nodes(&[2, 3, 1]);
        b.sort();

        let t1 = ShardTable::compute_with_epoch(&a, 2, 1);
        let t2 = ShardTable::compute_with_epoch(&b, 2, 1);

        for i in 0..NUM_SHARDS {
            assert_eq!(
                t1.assignments[i].master, t2.assignments[i].master,
                "shard {i} master differs"
            );
            assert_eq!(
                t1.assignments[i].replicas, t2.assignments[i].replicas,
                "shard {i} replicas differ"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Part 2.1: Single node RF=2 — no panic, no self-replica
    // -----------------------------------------------------------------------

    #[test]
    fn single_node_rf2_no_panic() {
        let table = ShardTable::compute_with_epoch(&nodes(&[1]), 2, 1);
        for shard in 0..NUM_SHARDS {
            assert_eq!(table.assignments[shard].master, NodeId(1));
            assert!(
                table.assignments[shard].replicas.is_empty(),
                "single node should have no replicas"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Part 2.1: RF=3 with 3 nodes — each shard has master + 2 replicas
    // -----------------------------------------------------------------------

    #[test]
    fn three_nodes_rf3_all_unique() {
        let members = nodes(&[1, 2, 3]);
        let table = ShardTable::compute_with_epoch(&members, 3, 1);

        for shard in 0..NUM_SHARDS {
            let a = &table.assignments[shard];
            assert_eq!(a.replicas.len(), 2, "shard {shard} should have 2 replicas");
            assert_ne!(
                a.master, a.replicas[0],
                "shard {shard} master == replica[0]"
            );
            assert_ne!(
                a.master, a.replicas[1],
                "shard {shard} master == replica[1]"
            );
            assert_ne!(
                a.replicas[0], a.replicas[1],
                "shard {shard} replica[0] == replica[1]"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Part 2.1: RF > node count — handled gracefully
    // -----------------------------------------------------------------------

    #[test]
    fn rf_greater_than_node_count_clamped() {
        // 2 nodes, RF=3: can't have 3 copies, should gracefully clamp
        let table = ShardTable::compute_with_epoch(&nodes(&[1, 2]), 3, 1);
        for shard in 0..NUM_SHARDS {
            let a = &table.assignments[shard];
            // Should have at most 1 replica (since only 2 nodes)
            assert!(
                a.replicas.len() <= 1,
                "shard {shard}: replicas should be clamped to 1"
            );
            if !a.replicas.is_empty() {
                assert_ne!(a.master, a.replicas[0]);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Part 2.2: Shard balance tests
    // -----------------------------------------------------------------------

    #[test]
    fn balance_3_nodes() {
        let table = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 1);
        let counts = table.shard_counts();
        let expected = NUM_SHARDS as f64 / 3.0;
        for (&node, &count) in &counts {
            let deviation = (count as f64 - expected).abs() / expected;
            assert!(
                deviation < 0.02,
                "node {node:?}: {count} shards, expected ~{expected:.0}, deviation {deviation:.4}"
            );
        }
    }

    #[test]
    fn balance_4_nodes() {
        let table = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3, 4]), 2, 1);
        let counts = table.shard_counts();
        let expected = NUM_SHARDS as f64 / 4.0;
        for (&node, &count) in &counts {
            let deviation = (count as f64 - expected).abs() / expected;
            assert!(
                deviation < 0.02,
                "node {node:?}: {count} shards, expected ~{expected:.0}, deviation {deviation:.4}"
            );
        }
    }

    #[test]
    fn balance_10_nodes() {
        let table = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 2, 1);
        let counts = table.shard_counts();
        let expected = NUM_SHARDS as f64 / 10.0;
        for (&node, &count) in &counts {
            let deviation = (count as f64 - expected).abs() / expected;
            assert!(
                deviation < 0.02,
                "node {node:?}: {count} shards, expected ~{expected:.0}, deviation {deviation:.4}"
            );
        }
    }

    #[test]
    fn balance_100_nodes() {
        let ids: Vec<u64> = (1..=100).collect();
        let members: Vec<NodeId> = ids.iter().map(|&id| NodeId(id)).collect();
        let table = ShardTable::compute_with_epoch(&members, 2, 1);
        let counts = table.shard_counts();
        let expected = NUM_SHARDS as f64 / 100.0;
        for (&node, &count) in &counts {
            // With 100 nodes, 4096 % 100 = 96, so 96 nodes get 41 shards
            // and 4 get 40. Max deviation is 1 shard from the mean (40.96).
            // Allow up to 1 shard off (tolerance ~3% for small per-node counts).
            let diff = (count as f64 - expected).abs();
            assert!(
                diff <= 1.0,
                "node {node:?}: {count} shards, expected ~{expected:.1}, diff {diff:.2}"
            );
        }
    }

    #[test]
    fn single_node_owns_all() {
        let table = ShardTable::compute_with_epoch(&nodes(&[1]), 2, 1);
        let counts = table.shard_counts();
        assert_eq!(*counts.get(&NodeId(1)).unwrap(), NUM_SHARDS);
    }

    #[test]
    fn two_nodes_rf2_symmetric() {
        let table = ShardTable::compute_with_epoch(&nodes(&[1, 2]), 2, 1);
        let counts = table.shard_counts();
        let n1 = *counts.get(&NodeId(1)).unwrap();
        let n2 = *counts.get(&NodeId(2)).unwrap();
        assert_eq!(n1 + n2, NUM_SHARDS);
        assert_eq!(n1, NUM_SHARDS / 2);
        assert_eq!(n2, NUM_SHARDS / 2);

        // Each node should be replica for the other's shards
        for shard in 0..NUM_SHARDS {
            let a = &table.assignments[shard];
            assert_eq!(a.replicas.len(), 1);
            assert_ne!(a.master, a.replicas[0]);
        }
    }

    // -----------------------------------------------------------------------
    // Part 2.3: Migration plan tests
    // -----------------------------------------------------------------------

    #[test]
    fn migration_plan_add_node_moves_correct_count() {
        let old = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 1);
        let new = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3, 4]), 2, 2);

        let plan = ShardTable::migration_plan(&old, &new);
        // ~1024 shards should move to node 4 (4096/4 = 1024)
        let to_4: Vec<_> = plan
            .iter()
            .filter(|t| t.to_node == NodeId(4) && t.is_master)
            .collect();
        let expected = NUM_SHARDS / 4;
        let deviation = (to_4.len() as f64 - expected as f64).abs() / expected as f64;
        assert!(
            deviation < 0.05,
            "expected ~{expected} shards to node 4, got {}",
            to_4.len()
        );

        // Moved shards should come from all 3 existing nodes (approximately evenly)
        let from_1 = plan.iter().filter(|t| t.from_node == NodeId(1)).count();
        assert!(from_1 > 0, "should move some from node 1");
        assert!(
            to_4.len() >= expected - 1 && to_4.len() <= expected + 1,
            "expected ~{expected} migrations to node 4, got {}",
            to_4.len()
        );
    }

    #[test]
    fn migration_plan_add_node_uses_authoritative_master_only() {
        let old = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 1);
        let new = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3, 4]), 2, 2);

        let plan = ShardTable::migration_plan(&old, &new);
        let shard = (0..NUM_SHARDS as u16)
            .find(|&shard| {
                let old_a = &old.assignments[shard as usize];
                let new_a = &new.assignments[shard as usize];
                old_a.master != new_a.master && !old_a.replicas.contains(&new_a.master)
            })
            .expect("expected at least one shard whose new master was not an old owner");

        let old_a = &old.assignments[shard as usize];
        let new_a = &new.assignments[shard as usize];
        let shard_tasks: Vec<_> = plan.iter().filter(|t| t.shard == shard).collect();

        assert_eq!(shard_tasks.len(), 1);
        assert_eq!(shard_tasks[0].from_node, old_a.master);
        assert_eq!(shard_tasks[0].to_node, new_a.master);
        assert!(shard_tasks[0].is_master);
    }

    #[test]
    fn migration_plan_remove_node_uses_surviving_replica() {
        let old = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 1);
        let new = ShardTable::compute_with_epoch(&nodes(&[1, 2]), 2, 2);

        let plan = ShardTable::migration_plan(&old, &new);
        // Node 3's ~1365 shards should be redistributed to nodes 1 and 2.
        for task in &plan {
            assert_ne!(
                task.from_node,
                NodeId(3),
                "dead node 3 should not be a migration source"
            );
            assert!(task.to_node == NodeId(1) || task.to_node == NodeId(2));
        }
    }

    #[test]
    fn migration_plan_remove_middle_node_never_sources_dead_member() {
        let old = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 2);
        let new = ShardTable::compute_with_epoch(&nodes(&[1, 3]), 2, 3);

        let plan = ShardTable::migration_plan(&old, &new);
        for task in &plan {
            assert_ne!(
                task.from_node,
                NodeId(2),
                "removed node 2 must never remain a master migration source"
            );
            assert!(task.to_node == NodeId(1) || task.to_node == NodeId(3));
        }
    }

    #[test]
    fn replica_migration_plan_remove_middle_node_never_sources_dead_member() {
        let old = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 2);
        let new = ShardTable::compute_with_epoch(&nodes(&[1, 3]), 2, 3);

        let plan = ShardTable::replica_migration_plan(&old, &new);
        for task in &plan {
            assert_ne!(
                task.from_node,
                NodeId(2),
                "removed node 2 must never remain a replica migration source"
            );
            assert!(task.to_node == NodeId(1) || task.to_node == NodeId(3));
        }
    }

    #[test]
    fn migration_plan_add_then_remove_net_zero() {
        let original = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 1);
        let back_to_3 = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 3);

        // After add then remove, the table should be identical to original
        // (same members, same algorithm)
        for shard in 0..NUM_SHARDS {
            assert_eq!(
                original.assignments[shard].master, back_to_3.assignments[shard].master,
                "shard {shard} master should be same after add+remove"
            );
        }

        // No migration needed between original and back_to_3
        // (same member set => same assignments => empty plan)
        let plan = ShardTable::migration_plan(&original, &back_to_3);
        // Versions differ, but assignments are the same.
        if let Some(task) = plan.first() {
            panic!(
                "unexpected migration: shard {} from {:?} to {:?}",
                task.shard, task.from_node, task.to_node
            );
        }
    }

    #[test]
    fn migration_plan_no_unnecessary_movements() {
        let old = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 1);
        let new = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3, 4]), 2, 2);
        let plan = ShardTable::migration_plan(&old, &new);

        // Verify no shard that stays on the same master appears in the plan
        for task in &plan {
            let old_master = old.assignments[task.shard as usize].master;
            let new_master = new.assignments[task.shard as usize].master;
            assert_ne!(
                old_master, new_master,
                "shard {} didn't change master ({:?}→{:?}) but is in migration plan",
                task.shard, old_master, new_master
            );
        }
    }

    #[test]
    fn replica_migration_plan_uses_existing_old_owner_when_master_changes() {
        let old = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 1);
        let new = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3, 4]), 2, 2);
        let plan = ShardTable::replica_migration_plan(&old, &new);

        // Shard 2179 is deterministic for this node set: its master changes
        // when node 4 joins, and the old master is no longer in the new
        // assignment, so the replica migration must source from a surviving
        // old owner (node 2) rather than the dead old master.
        let task = plan
            .iter()
            .find(|t| t.shard == 2179)
            .expect("expected replica migration task for shard 2179");

        assert_eq!(task.from_node, NodeId(2));
        assert_eq!(task.to_node, NodeId(1));
        assert!(!task.is_master);
    }

    // -----------------------------------------------------------------------
    // Part 2.5: Version consistency
    // -----------------------------------------------------------------------

    #[test]
    fn version_increments_on_every_membership_change() {
        let t1 = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 1);
        let t2 = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3, 4]), 2, 2);
        let t3 = ShardTable::compute_with_epoch(&nodes(&[1, 2, 3]), 2, 3);

        assert!(t2.version > t1.version);
        assert!(t3.version > t2.version);
    }

    // -----------------------------------------------------------------------
    // Part 6: Total shards always 4096
    // -----------------------------------------------------------------------

    #[test]
    fn total_mastered_shards_always_4096() {
        for n in 1..=20 {
            let ids: Vec<u64> = (1..=n).collect();
            let members: Vec<NodeId> = ids.iter().map(|&id| NodeId(id)).collect();
            let table = ShardTable::compute_with_epoch(&members, 2, 1);
            let total: usize = table.shard_counts().values().sum();
            assert_eq!(
                total, NUM_SHARDS,
                "with {n} nodes: total mastered shards should be {NUM_SHARDS}, got {total}"
            );
        }
    }

    #[test]
    fn no_shard_double_mastered() {
        for n in 1..=10 {
            let ids: Vec<u64> = (1..=n).collect();
            let members: Vec<NodeId> = ids.iter().map(|&id| NodeId(id)).collect();
            let table = ShardTable::compute_with_epoch(&members, 2, 1);
            // Each shard appears exactly once in assignments (by construction),
            // but verify via shard_counts summing to NUM_SHARDS
            let total: usize = table.shard_counts().values().sum();
            assert_eq!(total, NUM_SHARDS);
        }
    }

    #[test]
    fn master_and_replica_always_different() {
        for n in 2..=10 {
            let ids: Vec<u64> = (1..=n).collect();
            let members: Vec<NodeId> = ids.iter().map(|&id| NodeId(id)).collect();
            for rf in 2..=std::cmp::min(n, 5) {
                let table = ShardTable::compute_with_epoch(&members, rf as u8, 1);
                for shard in 0..NUM_SHARDS {
                    let a = &table.assignments[shard];
                    assert!(
                        !a.replicas.contains(&a.master),
                        "n={n} rf={rf} shard {shard}: master {:?} in replicas",
                        a.master
                    );
                    // No duplicate replicas
                    let mut seen = std::collections::HashSet::new();
                    for r in &a.replicas {
                        assert!(
                            seen.insert(r),
                            "n={n} rf={rf} shard {shard}: duplicate replica {:?}",
                            r
                        );
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Part 4.1: Handoff / migration integration
    // -----------------------------------------------------------------------

    #[test]
    fn handoff_empty_shards_skip_copying() {
        let old_members = nodes(&[1, 2, 3]);
        let new_members = nodes(&[1, 2, 3, 4]);
        let mut table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);

        // All shards are empty → shard_has_data returns false → skip Copying
        table.begin_handoff_with(&new_table, |_| false);
        assert_eq!(
            table.pending_handoff_count(),
            0,
            "all empty shards should skip directly to ServingNew"
        );
    }

    #[test]
    fn handoff_with_data_enters_copying() {
        let old_members = nodes(&[1, 2, 3]);
        let new_members = nodes(&[1, 2, 3, 4]);
        let mut table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);

        // All shards have data → should enter Copying
        table.begin_handoff_with(&new_table, |_| true);
        let copying_count = (0..NUM_SHARDS as u16)
            .filter(|&s| table.shard_handoff_state(s) == ShardHandoff::Copying)
            .count();
        assert!(copying_count > 0, "some shards should be in Copying state");
    }

    #[test]
    fn commit_shard_transitions_to_serving_new() {
        let old_members = nodes(&[1, 2]);
        let new_members = nodes(&[1, 2, 3]);
        let mut table = ShardTable::compute_with_epoch(&old_members, 2, 1);
        let new_table = ShardTable::compute_with_epoch(&new_members, 2, 2);
        table.begin_handoff_with(&new_table, |_| true);

        // Find a shard in Copying state
        let copying_shard =
            (0..NUM_SHARDS as u16).find(|&s| table.shard_handoff_state(s) == ShardHandoff::Copying);
        if let Some(shard) = copying_shard {
            table.commit_shard(shard);
            assert_eq!(table.shard_handoff_state(shard), ShardHandoff::ServingNew);
        }
    }

    // ── Phase C: subset/version tracking ───────────────────────────────────

    #[test]
    fn partition_version_starts_full_for_unchanged_shard() {
        let members_a = vec![NodeId(1), NodeId(2)];
        let table_a = ShardTable::compute_with_epoch(&members_a, 2, 1);
        let table_b = ShardTable::compute_with_epoch(&members_a, 2, 2);
        let mut active = table_a.clone();
        active.begin_handoff_with(&table_b, |_| true);
        for shard in 0..NUM_SHARDS as u16 {
            assert!(
                !active.is_subset_master(shard),
                "shard {shard} should not be subset when master didn't change"
            );
        }
    }

    #[test]
    fn partition_version_starts_subset_for_inbound_master() {
        let members_a = vec![NodeId(1), NodeId(2)];
        let members_b = vec![NodeId(2), NodeId(3)];
        let table_a = ShardTable::compute_with_epoch(&members_a, 2, 1);
        let table_b = ShardTable::compute_with_epoch(&members_b, 2, 2);
        let mut active = table_a.clone();
        active.begin_handoff_with(&table_b, |_| true);
        let changed_shard = (0..NUM_SHARDS as u16)
            .find(|&s| table_a.target_assignment(s).master != table_b.target_assignment(s).master)
            .expect("membership change must move at least one shard");
        assert!(
            active.is_subset_master(changed_shard),
            "shard {changed_shard} should be subset since its master changed"
        );
        active.commit_shard(changed_shard);
        assert!(
            !active.is_subset_master(changed_shard),
            "shard {changed_shard} should not be subset after commit"
        );
    }

    #[test]
    fn partition_version_cleared_when_no_data_to_copy() {
        // When shard_has_data returns false for all shards, begin_handoff_with
        // takes the all_serving fast path and must clear master_subset even
        // for shards whose master changed — no inbound migration will run.
        let members_a = vec![NodeId(1), NodeId(2)];
        let members_b = vec![NodeId(2), NodeId(3)];
        let table_a = ShardTable::compute_with_epoch(&members_a, 2, 1);
        let table_b = ShardTable::compute_with_epoch(&members_b, 2, 2);
        let changed_shard = (0..NUM_SHARDS as u16)
            .find(|&s| table_a.target_assignment(s).master != table_b.target_assignment(s).master)
            .expect("membership change must move at least one shard");
        let mut active = table_a.clone();
        active.begin_handoff_with(&table_b, |_| false); // no data to copy
        assert!(
            !active.is_subset_master(changed_shard),
            "shard {changed_shard} must not be subset when no inbound migration runs"
        );
    }
}
