//! Workload generator and state verifier for integration testing.
//!
//! Provides configurable workload generation with realistic BSV UTXO
//! operation sequences, and an independent in-memory state verifier
//! that validates TeraSlab state matches expected results.

pub mod generator;
pub mod verifier;
