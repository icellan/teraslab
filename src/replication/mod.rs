//! Operation-based synchronous replication.
//!
//! The master sends batches of `ReplicaOp`s to replicas, which apply them
//! using the same idempotent mutation functions. Acknowledgment policies
//! (WriteAll, WriteMajority) control durability guarantees.

pub mod batching;
pub mod manager;
pub mod protocol;
pub mod receiver;
pub mod tcp_transport;
