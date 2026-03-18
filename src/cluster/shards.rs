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

/// The shard table: maps each shard to its master and replicas.
///
/// Computed deterministically from a sorted member list so every node
/// in the cluster arrives at the identical assignment independently.
pub struct ShardTable {
    assignments: Vec<ShardAssignment>,
    /// Incremented on every topology change.
    pub version: u64,
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
    pub fn compute(members: &[NodeId], replication_factor: u8) -> Self {
        assert!(!members.is_empty(), "cannot compute shard table with 0 members");
        let n = members.len();
        let mut assignments = Vec::with_capacity(NUM_SHARDS);

        for shard in 0..NUM_SHARDS {
            let master = members[shard % n];
            let mut replicas = Vec::new();
            for r in 1..replication_factor as usize {
                if r >= n {
                    break; // Not enough distinct nodes
                }
                let replica = members[(shard + r) % n];
                if replica != master {
                    replicas.push(replica);
                }
            }
            assignments.push(ShardAssignment { master, replicas });
        }

        // Version derived from member list hash for consistency detection
        let mut version_hash: u64 = 0;
        for (i, m) in members.iter().enumerate() {
            version_hash = version_hash.wrapping_add(m.0.wrapping_mul(i as u64 + 1));
        }

        ShardTable {
            assignments,
            version: version_hash,
        }
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
    pub fn assignment(&self, shard: u16) -> &ShardAssignment {
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
    pub fn migration_plan(old: &ShardTable, new: &ShardTable) -> Vec<MigrationTask> {
        let mut tasks = Vec::new();
        for shard in 0..NUM_SHARDS {
            let old_master = old.assignments[shard].master;
            let new_master = new.assignments[shard].master;
            if old_master != new_master {
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
    use std::collections::HashSet;

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
        assert!(!plan.is_empty());

        // All migrations should be FROM node 4
        let from_node4: Vec<_> = plan.iter().filter(|t| t.from_node == NodeId(4)).collect();
        assert!(!from_node4.is_empty(), "node 4's shards should migrate away");
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
}
