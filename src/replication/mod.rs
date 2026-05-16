//! Operation-based synchronous replication.
//!
//! The master sends batches of `ReplicaOp`s to replicas, which apply them
//! using the same idempotent mutation functions. Acknowledgment policies
//! (WriteAll, WriteMajority) control durability guarantees.

pub mod batching;
pub mod durable;
pub mod manager;
pub mod protocol;
pub mod receiver;
pub mod tcp_transport;

// F-G7-020: re-export the public surface so callers can use
// `crate::replication::ReplicationManager` rather than the verbose
// `crate::replication::manager::ReplicationManager`. Internal
// submodule paths remain available for fine-grained access.
pub use manager::{
    ReplicaState, ReplicaTransport, ReplicationConfig, ReplicationError, ReplicationManager,
};
pub use protocol::{ReplicaAck, ReplicaBatch, ReplicaOp};
pub use receiver::ReplicationReceiver;
