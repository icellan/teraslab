#![warn(clippy::disallowed_macros)]

// F-G10-014: the original blanket `pub mod` for every internal made the
// crate's public API surface infinite — any refactor inside
// `fault_injection`, `device_io`, `io`, `recovery`, `redo`, `locks`, etc.
// would be a SemVer-breaking change for hypothetical downstream consumers.
// `device_io` is genuinely internal (no external usage in bins / tests /
// benches) and has been demoted to `pub(crate)`. The rest stay `pub` for
// now because they have legitimate cross-crate consumers (bins,
// integration tests under `tests/`, criterion benches under `benches/`)
// — until those consumers move under `pub(crate) use` boundaries via a
// dedicated re-export module, demoting them would break the build.
//
// Audit follow-up FUP-G10-014 tracks the broader internal-visibility
// hygiene work; this commit only fixes the unambiguously-internal modules.
pub mod allocator;
pub mod checkpoint;
pub mod cluster;
pub mod config;
pub mod device;
pub(crate) mod device_io;
pub mod fault_injection;
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
pub mod replication;
pub mod server;
pub mod storage;
