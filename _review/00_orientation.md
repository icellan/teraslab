# Phase 0 — Orientation

**Date:** 2026-05-18 (session start)
**Repo:** TeraSlab (purpose-built UTXO store for BSV Teranode)
**Branch:** `p1.1-cluster-id` @ `cd6570c` (working tree has untracked `tests/g8_cluster_id.rs` and _review scratch; src/ clean per initial `git status`)
**Prior context:** Extensive prior review (2026-05-16) with 10 parallel agents produced `_review/02_findings_G*.md` (216 findings total), many remediated (G1–G10 fix groups), `AUDIT.md`, `AUDIT_CODEX.md`, and existing `REVIEW_REPORT.md`. This run is a fresh pass on the post-fix tree.

## 1. Top-level directory tree (depth ≤3; exclude vendored/generated, target/, .git/, .claude/worktrees/, teraslab-tests/results/)

```
teraslab/
├── Cargo.toml                  workspace root, edition 2024, bins: teraslab-server, teraslab-cli
├── Cargo.lock
├── clippy.toml                 disallows eprintln/println in lib code (forces tracing)
├── deny.toml                   cargo-deny config (dependency audit policy)
├── README.md, LICENSE, CLAUDE.md, .github/
├── AUDIT.md (45 KB), AUDIT_CODEX.md (32 KB), REVIEW_REPORT.md (prior artifacts)
├── _review/                    prior + in-progress review artifacts (this run will overwrite 00/01/02/03)
├── _plans/                     planning artifacts
├── benches/                    Criterion benchmarks (allocator_ops, index_ops, mixed_workload, spend_throughput, ...)
├── client/
│   ├── go/                     Go client SDK
│   └── rust/                   Rust client crate + its target/
├── docker/                     20+ docker-compose.*.yml + config/*.toml for ts01..ts99 clusters
├── docs/                       superpowers/ (plans, specs)
├── phases/                     00_analysis_and_spec.md .. 13_admin_tooling.md (build order)
├── scripts/                    cluster/ (node*.toml), start-*.sh, cleanup-worktrees.sh, check_perf_budget.sh
├── specs/                      BSV_UTXO_STORE_SPEC.md, BSV_UTXO_STORE_RUST_CRATES.md, SPEC_BRIEFING.md, teranode.lua
├── src/                        71 *.rs files (~110 kLOC)
│   ├── bin/                    server.rs, cli.rs
│   ├── cluster/                auth.rs, coordinator.rs (large), membership.rs, migration.rs, mod.rs, routing.rs, shards.rs, swim.rs, topology.rs
│   ├── device_io/              io_uring_backend.rs (linux), mod.rs, sync_fallback.rs
│   ├── index/                  backend.rs, dah_index.rs, hashtable.rs, migration.rs, mod.rs, redb_*.rs, secondary_backend.rs, unmined_index.rs, util.rs
│   ├── observability/          mod.rs
│   ├── ops/                    create.rs, delete_eval.rs, engine.rs (large), error.rs, mark_longest_chain.rs, mod.rs, remaining.rs, set_mined.rs, signal.rs, spend.rs, unspend.rs
│   ├── protocol/               codec.rs, frame.rs, mod.rs, opcodes.rs
│   ├── replication/            batching.rs, durable.rs, manager.rs, mod.rs, protocol.rs, receiver.rs, tcp_transport.rs
│   ├── server/                 dispatch.rs (large), http.rs, mod.rs, startup.rs
│   └── (root)                  allocator.rs, checkpoint.rs, config.rs, device.rs, fault_injection.rs, io.rs, lib.rs, locks.rs, metrics.rs, record.rs, recovery.rs, redo.rs, storage/{blob_gc,blobstore,input_refs,manager,mod,tiers,uploader}.rs
├── tests/                      ~50 *.rs (integration, property, gX_review regression, stress, simulation/, workload/, fault_injection, cluster_*, replication_tcp, recovery_*, e2e, prometheus, http_observability, ui_xss, ...)
├── teraslab-tests/             external harness (Docker scenarios, client/ with its own Cargo)
├── ui/                         app.js, index.html, style.css (embedded admin UI via rust-embed)
└── (no fuzz/ at root on this tree; was referenced in prior orientation)
```

## 2. Languages detected, rough LOC per language

| Area                  | Files | LOC (approx) | Notes |
|-----------------------|-------|--------------|-------|
| Rust — `src/`         | 71    | ~110,435     | Core implementation (measured via wc) |
| Rust — `tests/`       | ~50   | ~30k+        | Heavy integration + property + review-driven regression tests (g1..g10, cluster, recovery, etc.) |
| Rust — `benches/`     | 7     | ~3–4k        | Criterion (html reports in target/criterion) |
| Rust — `client/rust/` | ~10   | ~2k          | Client SDK |
| Rust — `teraslab-tests/client/` | ~5 | small     | Test harness client |
| Go                    | small | ~1–2k        | `client/go/` SDK |
| Lua                   | 1     | reference    | `specs/teranode.lua` (old UDF being replaced) |
| JS/HTML/CSS           | 3     | small        | `ui/` embedded admin console |
| Shell/TOML/YAML       | many  | config-heavy | docker/, scripts/, .github/ |

Dominant files by size (from prior + wc intuition): `src/server/dispatch.rs`, `src/ops/engine.rs`, `src/cluster/coordinator.rs` are the multi-thousand LOC hotspots. Protocol codec, redo, recovery, index backends, storage tiers also significant.

## 3. Build system, package manager, test runner, lint/static-analysis config present

| Tool                  | Config / Usage |
|-----------------------|----------------|
| Build                 | `cargo build --release` (Rust 1.85+ for edition 2024) |
| Package               | Cargo workspace (single package "teraslab" + bins + [[bench]]) |
| Lint (enforced)       | `cargo clippy --all-targets -- -D warnings` (clippy.toml: disallows eprintln/println in lib) |
| Format                | `cargo fmt --all -- --check` |
| Test                  | `cargo test --all`; `cargo test --features fault-injection --test fault_injection`; slow-tests feature for heavy benches-as-tests; `cargo test --all 2>&1 | grep -E "test result|FAILED"` required at phase checkpoints per CLAUDE.md |
| Coverage              | tarpaulin (tarpaulin-report.json present, 6.8 MB, somewhat stale) |
| Bench                 | Criterion (7 benches: spend_throughput, mixed_workload, index_ops, allocator_ops, ...) |
| Fuzz                  | cargo-fuzz mentioned in prior docs but no `fuzz/` dir visible on this checkout |
| E2E / cluster         | `teraslab-tests/run_all.sh` + docker-compose for 1..99 node clusters; scripts/cluster/ |
| Dep audit             | `deny.toml` present (cargo-deny); used for advisory / license / duplicate detection |
| Docs                  | `RUSTDOCFLAGS=-D warnings cargo doc --no-deps --all` (per prior) |
| CI                    | `.github/workflows/ci.yml`, `nightly.yml`, `release.yml` |

Feature flags of note:
- `fault-injection`: test-only sync points in hot write path; zero-cost in prod builds.
- `slow-tests`: gates long-running index/allocator throughput tests.

## 4. Entry points (binaries, services, exported library API)

| Binary / Surface              | Path                        | Purpose / Exposure |
|-------------------------------|-----------------------------|--------------------|
| `teraslab-server`             | `src/bin/server.rs`         | Main daemon. Loads config, opens DirectDevice/allocator/index/redo, starts TCP listener (default :3300), HTTP admin/metrics/UI (:9100), replication manager, cluster swim/coordinator. Handles graceful shutdown intent (ctrlc wiring present). |
| `teraslab-cli`                | `src/bin/cli.rs`            | Operator tool: REPL, admin commands (top, dump, restore, cluster ops), loadgen, config inspection. |
| TCP wire protocol             | `src/protocol/*`, `src/server/dispatch.rs` | Binary length-prefixed frames, opcodes (CreateV2, Spend, SetMined, Reassign, GetSpend, Cluster control, Ping, etc.). Primary attack surface for untrusted clients and peer replicas. |
| HTTP admin/observability      | `src/server/http.rs`        | Axum server: /metrics (prometheus), /health/*, /admin/* (top, ws/top for cluster view), /ui/* (embedded), bearer token auth (subtle CT eq). Post-R-056 auth added; some endpoints historically unauthed. |
| Library API (`pub` surface)   | `src/lib.rs` (71 pub mods)  | Everything is re-exported pub; consumers = bins + tests + client/rust + benches. (Prior review noted this as excessive; some demotions to pub(crate) applied, e.g. device_io.) |
| Replication TCP               | `src/replication/tcp_transport.rs` | Inter-node redo shipping and ack protocol. |
| Cluster control (SWIM + custom) | `src/cluster/*`            | Membership, topology changes, migration, split-brain detection via cluster_id + secret. |

Hot paths identifiable from naming/comments/structure:
- `Engine` (ops/engine.rs) — all mutating ops (spend, create, delete, set_mined, unspend, reassign)
- `Server::dispatch` and codec encode/decode
- Index lookups (primary + secondary + unmined + DAH)
- Redo append + replay at startup/recovery
- Allocator (slot allocation/free, region management)
- Device I/O (io_uring or sync O_DIRECT)
- Cluster coordinator on topology changes

## 5. External dependencies of note (security-sensitive flagged)

From `Cargo.toml` (pinned major versions; some exact minors for OTEL coordination):

| Crate                          | Version | Risk / Role |
|--------------------------------|---------|-------------|
| `axum` + `tokio` + `tower`     | 0.8 / 1 | HTTP server surface (auth, routing, WS for /ws/top). XSS, auth bypass, resource exhaustion vectors. |
| `reqwest` (rustls only)        | 0.12    | Outbound HTTP (no default TLS features; explicit rustls). |
| `serde` / `serde_json` / `toml`| 1 / 1 / 0.8 | Deserialization of config (trusted) and wire? Protocol uses custom codec, not serde for hot path, but JSON in HTTP/metrics and possibly config. |
| `redb`                         | 2       | Embedded key-value for index backends (on-disk). Corruption or DoS via large keys/values? |
| `sha2`                         | 0.10    | Hashing for BlobDigest, content addressing. |
| `crc32fast`                    | 1       | Record integrity (non-crypto). |
| `subtle`                       | 2       | Constant-time eq for bearer token (good). |
| `getrandom`                    | 0.2     | Crypto RNG source. |
| `libc`                         | 0.2     | Direct syscalls: mmap, O_DIRECT, fallocate, fdatasync, ioctl — low-level device code. |
| `io-uring` (linux only)        | 0.7     | Kernel async I/O — unsafe ring submission, completion parsing. |
| `parking_lot`                  | 0.12    | Locks (faster Mutex/RwLock). |
| `opentelemetry*` stack         | 0.31/0.32 | OTLP trace export — network, protobuf, resource attribution. |
| `rust-embed`                   | 8       | Embeds ui/* assets into binary (served via HTTP). Path traversal or MIME issues? |
| `ctrlc`                        | 3       | Signal handling (recently added to fix prior stub). |

No obvious crypto primitives beyond sha2/getrandom/subtle. No SQL, no template engines, no zip, no XML parsers in deps.

## 6. Hot paths or performance-critical modules (from naming, Cargo, prior artifacts)

- `src/ops/engine.rs` — central state machine for all UTXO mutations; holds allocator + indexes + device + redo.
- `src/server/dispatch.rs` — per-connection request router + response framing; must be zero-copy where possible.
- `src/cluster/coordinator.rs` — topology voting, migration orchestration, split-brain guard.
- `src/protocol/codec.rs` — length-prefixed encode/decode for all opcodes; allocation and bounds critical.
- `src/index/*` (hashtable, redb_*, dah, unmined) — every op does multiple index lookups/mutations.
- `src/allocator.rs` + `src/device.rs` — slot allocation, region free, direct I/O.
- `src/redo.rs` + `src/replication/*` — append-only log + shipping; crash safety depends on ordering.
- `src/storage/*` (tiers, blobstore, gc, uploader) — cold data path (Phase 11 tiered storage).
- Fault-injection points are sprinkled in write path for testing only.

## 7. Test layout and rough shape

- **Unit tests**: `#[cfg(test)] mod tests` inside most src/*.rs (engine, codec, allocator, redo, index backends, etc.).
- **Integration tests** (`tests/*.rs`): 
  - Core: `integration.rs`, `server_tcp.rs`, `e2e_workload.rs`, `recovery_crash_boundaries.rs`, `replication_tcp.rs`, `fault_injection.rs`
  - Cluster: `cluster_edge_cases.rs`, `cluster_swim.rs`, `cluster_tcp.rs`, `g8_split_brain.rs`, `g8_swim_replay.rs`, `g8_cluster_id.rs` (new untracked)
  - Review regression: `g1_review.rs`, `g2_*`, `g4_*`, `g5_protocol_auth.rs`, `g8_*`, `g9_*`, `g10_*`
  - Observability: `prometheus_conformance.rs`, `http_observability.rs`, `tracing_integration.rs`
  - Property/stress: `stress_tests.rs`, `simulation/`, `workload/{generator,verifier}`
  - Misc: `blob_gc_recovery.rs`, `cli_integration.rs`, `ui_xss.rs`, `secondary_two_phase_durability.rs`, `g2_create_size_contract.rs` etc.
- **External harness**: `teraslab-tests/` — multi-node Docker scenarios, custom client, result dirs with historical runs.
- **Benches**: 7 Criterion suites, some gated behind `slow-tests`.
- **Coverage aspiration**: tarpaulin JSON present; CI does not appear to gate on %.
- Shape: Very thorough for a systems project. Many tests were written to lock in fixes from the prior 216-finding review. Property testing (proptest) used in a few places. Fault-injection feature for deterministic interleaving on crash/recovery paths.

**Gate status for Phase 0:** This file is now on disk. No deep logic reads of implementation bodies have occurred beyond manifest, lib.rs header, bin startup comments, and directory enumeration. Ready for Phase 1 scope declaration.

---

**Notes for this run:**
- The tree under review is post-G1–G10 remediation on `p1.1-cluster-id` (cluster identifier / split-brain work in progress).
- Several _review/ artifacts from prior run exist (02_findings_G*.md etc.); this pass will produce fresh 00/01/02/03/REPORT for the current commit.
- Untracked `tests/g8_cluster_id.rs` suggests active work on cluster-id auth — likely in scope.
