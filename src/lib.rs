#![warn(clippy::disallowed_macros)]

// F-G10-014: internal modules stay `pub` for now because they have
// legitimate cross-crate consumers (bins, integration tests under
// `tests/`, criterion benches under `benches/`). Demoting them
// requires moving those consumers under `pub(crate) use` boundaries
// via a dedicated re-export module — tracked as FUP-G10-014.
//
// The previously-gated `device_io` module (DeviceIo trait +
// io_uring/sync fallback backends) was deleted on 2026-05-28 per the
// May-2026 external review's code-reviewer P0: it was dead
// scaffolding that had been gated behind the `async-io` feature
// purely to keep `cargo clippy --all --all-features` from flagging
// 8 dead-code warnings on a module no caller wired up. Reintroduce
// the trait + backends only when a real caller is ready to land
// alongside.
pub mod allocator;
pub mod cache;
pub mod checkpoint;
pub mod cluster;
pub mod config;
pub mod device;
pub mod fault_injection;
pub(crate) mod fsutil;
pub mod index;
pub mod io;
pub mod locks;
pub mod metrics;
pub mod observability;
pub mod ops;
pub mod protocol;
pub mod record;
pub mod recovery;
pub mod redo;
pub mod redo_group;
pub mod replication;
pub mod server;
pub mod storage;
pub mod subdevice;
pub mod tombstone;
pub mod tombstone_gc;
