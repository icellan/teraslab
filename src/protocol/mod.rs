//! Binary wire protocol for client-server communication.
//!
//! Batch-first design: every operation has a batch opcode. Single-item
//! operations are batches of size 1. Partial success is the norm —
//! per-item errors in sparse format.

pub mod codec;
pub mod frame;
pub mod opcodes;
