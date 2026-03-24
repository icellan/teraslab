//! Cluster management: hash-based sharding, SWIM membership, coordination,
//! and data migration.

pub mod auth;
pub mod coordinator;
pub mod membership;
pub mod migration;
pub mod routing;
pub mod shards;
pub mod swim;
pub mod topology;
