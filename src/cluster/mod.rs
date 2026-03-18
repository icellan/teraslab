//! Cluster management: hash-based sharding, SWIM membership, coordination,
//! and data migration.

pub mod coordinator;
pub mod membership;
pub mod migration;
pub mod shards;
pub mod swim;
