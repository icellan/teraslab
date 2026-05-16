# Phase 0 — Orientation

**Date:** 2026-05-16
**Repo:** TeraSlab (purpose-built UTXO store for BSV Teranode)
**Branch:** `main` @ `52adbb2` (with substantial uncommitted changes — see `git status`)

## 1. Top-level layout (depth ≤3, excluding worktrees and target)

```
teraslab/
├── Cargo.toml                  workspace root, edition 2024, binaries: teraslab-server, teraslab-cli
├── clippy.toml                 disallows eprintln/println in lib code (force tracing)
├── README.md, LICENSE, CLAUDE.md
├── AUDIT.md (45 KB), AUDIT_CODEX.md (32 KB)   <-- prior internal audits (2026-05-06)
├── .github/workflows/          ci.yml, nightly.yml, release.yml
├── benches/                    7 Criterion benches (codec, allocator, index, mixed_workload, etc.)
├── client/
│   ├── go/                     Go client SDK
│   └── rust/                   Rust client crate
├── docker/                     Dockerfiles + cluster configs
├── docs/                       superpowers + design notes
├── fuzz/                       cargo-fuzz targets + corpus
├── phases/                     phase plans 00..13
├── scripts/                    cluster orchestration scripts
├── specs/                      SPEC_BRIEFING, BSV_UTXO_STORE_SPEC, teranode.lua
├── src/                        70 files, 103,689 LOC (main crate)
│   ├── bin/                    server.rs (1259), cli.rs (1280)
│   ├── cluster/                10 files, ~24k LOC (coordinator.rs 10,383 LOC alone)
│   ├── device_io/              io_uring + sync fallback (Linux uring; portable fallback)
│   ├── index/                  10 files, ~11k LOC (redb + in-mem hashtable backends)
│   ├── observability/          metrics, tracing
│   ├── ops/                    11 files (engine.rs 10,889 LOC dominant)
│   ├── protocol/               wire codec, frame, opcodes
│   ├── replication/            7 files (~10k LOC, includes durable redo, manager, receiver)
│   ├── server/                 dispatch.rs (13,399 LOC), http.rs (3,387), startup, mod
│   └── storage/                blobstore, tiers, manager, blob_gc, uploader, input_refs
├── teraslab-tests/             external integration harness (Docker + scenarios)
├── tests/                      24 integration test files (cluster_*, replication_tcp, recovery_*, e2e, fault_injection, prometheus_conformance, property_core_invariants, ui_xss, http_observability, ...)
└── ui/                         embedded admin UI assets
```

## 2. Languages and LOC

| Area              | Files | LOC      | Notes |
|-------------------|-------|----------|-------|
| Rust — `src/`     | 71    | 103,689  | Main crate, target of this review |
| Rust — `tests/`   | ~24   | ~25k     | Integration & property tests |
| Rust — `benches/` | 7     | ~3k      | Criterion benches |
| Rust — `fuzz/`    | ~5    | small    | cargo-fuzz targets |
| Rust — `client/rust/` | small | ~2k   | Client SDK |
| Go                | n/a   | ~1-2k    | `client/go/` |
| Lua               | 1     | `specs/teranode.lua` reference UDF |

Three Rust files dominate: `src/server/dispatch.rs` (13k), `src/ops/engine.rs` (10.9k), `src/cluster/coordinator.rs` (10.4k). A fourth tier 3-4k: `recovery.rs`, `protocol/codec.rs`, `replication/receiver.rs`, `server/http.rs`, `redo.rs`, `cluster/migration.rs`.

## 3. Build system, lint, tests

| Tool              | Config                                                             |
|-------------------|--------------------------------------------------------------------|
| Build             | `cargo build --release`, edition 2024 (requires Rust 1.85+)        |
| Lint              | `cargo clippy --all-targets -- -D warnings` (CI enforced)          |
| Format            | `cargo fmt --all -- --check`                                       |
| Test runner       | `cargo test --all` + `cargo test --features fault-injection --test fault_injection` |
| Docs              | `RUSTDOCFLAGS=-D warnings cargo doc --no-deps --all`               |
| Coverage          | `tarpaulin-report.json` present (6.8 MB JSON, stale)               |
| Bench             | Criterion (compiled on PR, run nightly)                            |
| Fuzz              | `cargo-fuzz` targets, not in CI                                    |
| E2E               | Dockerized cluster scenarios, `teraslab-tests/run_all.sh`          |

Custom lint: `clippy.toml` disallows `std::eprintln` and `std::println` in lib code (forces tracing).

Feature flag: `fault-injection` enables test-only sync points across the write path; production builds are zero-cost.

## 4. Entry points

| Binary / API surface          | Path                                | Purpose |
|-------------------------------|-------------------------------------|---------|
| `teraslab-server`             | `src/bin/server.rs`                 | The daemon: TCP wire protocol + HTTP observability + replication |
| `teraslab-cli`                | `src/bin/cli.rs`                    | Operator CLI (admin commands, repl, dump utilities) |
| TCP wire protocol             | `src/protocol/`, `src/server/dispatch.rs` | Binary frame protocol on `0.0.0.0:3300` (default) |
| HTTP observability            | `src/server/http.rs`                | Admin/metrics/UI on `0.0.0.0:9100` (default), bearer-auth (post R-056) |
| Library API                   | `src/lib.rs` re-exports             | All major modules are `pub`; consumers are the two bins + tests + the rust client crate |

## 5. External dependencies (security-relevant)

From `Cargo.toml`:

| Crate                          | Version | Role / concern |
|--------------------------------|---------|----------------|
| `axum`                         | 0.8     | HTTP server (admin UI, metrics) — auth, CSRF, XSS risk surface |
| `reqwest` (rustls)             | 0.12    | HTTP client (no default features; rustls-only) |
| `tokio`                        | 1       | Async runtime |
| `redb`                         | 2       | On-disk index backend |
| `serde` / `serde_json` / `toml`| —       | Untrusted-input deserialization risk if used on network paths |
| `crc32fast`                    | 1       | CRC for records — not crypto |
| `sha2`                         | 0.10    | SHA-2; used for `BlobDigest` and possibly auth |
| `subtle`                       | 2       | Constant-time comparison for admin bearer token (per `Cargo.toml` comment) |
| `getrandom`                    | 0.2     | Cryptographic randomness |
| `libc`                         | 0.2     | ioctl, `mmap`, `O_DIRECT` |
| `io-uring`                     | 0.7     | Linux-only async block I/O |
| `bitflags`                     | 2       | Status byte flags on UTXO slots |
| `rust-embed`                   | 8       | Embedded UI assets — path-traversal risk |
| `clap`                         | 4       | CLI parsing |
| `parking_lot`                  | 0.12    | Sync primitives |
| `opentelemetry*` (0.31/0.32)   | —       | OTLP tracing export |
| `tracing-subscriber`           | 0.3     | Logging (JSON/env-filter/fmt) |

No explicit auth/crypto library beyond `subtle` and `sha2`. SWIM signatures are home-rolled (`src/cluster/auth.rs`).

## 6. Hot paths & performance-critical modules

Identifiable from naming + comments:

| Module                                  | Role |
|-----------------------------------------|------|
| `src/ops/engine.rs`                     | Hot path: spend / unspend / create / set_mined / DAH eval |
| `src/io.rs`                             | Raw mmap'd reads + writes (unsafe API; documents stripe-lock contract) |
| `src/device.rs`                         | Block device abstraction; `O_DIRECT`, `AlignedBuf`, `MemoryDevice` |
| `src/device_io/io_uring_backend.rs`     | Async I/O backend (Linux) |
| `src/allocator.rs`                      | UTXO slot allocator (free list, bitmaps) |
| `src/index/hashtable.rs`                | In-memory hash bucket index (alt to redb) |
| `src/redo.rs`                           | WAL/redo log |
| `src/recovery.rs`                       | Crash recovery (replay) |
| `src/replication/manager.rs`            | Replication batching + fan-out |
| `src/cluster/coordinator.rs`            | Cluster control plane (quorum, topology) |
| `src/server/dispatch.rs`                | Per-opcode request dispatch |
| `src/protocol/codec.rs`                 | Wire codec (binary) |

`unsafe` usage (~123 occurrences) clusters in `device.rs`, `io.rs`, `record.rs` (packed-struct `bytemuck`-style copies), `config.rs` (env wrappers in tests).

## 7. Test layout

- `tests/` (integration): `cluster_*`, `replication_tcp`, `recovery_crash_boundaries`, `e2e_workload`, `fault_injection`, `prometheus_conformance`, `property_core_invariants` (proptest), `ui_xss`, `http_observability`, `tracing_lint`, `cli_integration`, `secondary_two_phase_durability`, `blob_gc_recovery`, `server_tcp`, `stress_tests`.
- Sub-modules under `tests/`: `simulation/`, `workload/{generator,verifier}.rs`, `stress/`.
- In-source unit tests: 53 of 70 src files contain `#[cfg(test)] mod tests`.
- `teraslab-tests/` is a separate Docker-driven E2E harness with versioned result snapshots.

Coverage: prior `tarpaulin-report.json` exists but is stale (March). No coverage gate in CI.

## 8. Prior audits on disk

Two prior internal audits (`AUDIT.md`, `AUDIT_CODEX.md`, 2026-05-06) catalogue ~70 findings (R-001 through R-100 ledger). Recent commits indicate active remediation (R-034/R-035/R-049/R-056). **These audits will not be treated as ground truth for this review** — they will be cross-referenced as Phase 1 ingestion notes only. Findings here are derived from a fresh read.

## 9. Stop-condition assessment

`src/` is 103,689 LOC in 70 files. Three files exceed 10k LOC; six exceed 3k. A single agent reading every line in one session is infeasible without degrading depth. **Phase 1 will propose a per-module phased split, with parallel sub-agent review.** This is the dispatch-parallel-agents pattern: each agent reviews a logically-isolated module against the same finding template, writing findings to its own file; the orchestrator aggregates.
