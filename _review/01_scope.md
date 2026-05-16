# Phase 1 — Scope declaration

## In-scope (review target)

The main crate's library and binary code under `src/`. All 70 files, 103,689 LOC.

Grouped by module for parallel-agent dispatch:

| Group | Files | LOC (approx) | Reviewer agent |
|-------|-------|--------------|----------------|
| **G1 — core data plane: device + io + record + allocator** | `src/device.rs`, `src/io.rs`, `src/record.rs`, `src/allocator.rs`, `src/device_io/mod.rs`, `src/device_io/sync_fallback.rs`, `src/device_io/io_uring_backend.rs`, `src/locks.rs`, `src/fault_injection.rs` | ~6,800 | Agent-G1 |
| **G2 — ops engine + ops sub-paths** | `src/ops/engine.rs`, `src/ops/create.rs`, `src/ops/spend.rs`, `src/ops/unspend.rs`, `src/ops/set_mined.rs`, `src/ops/delete_eval.rs`, `src/ops/remaining.rs`, `src/ops/mark_longest_chain.rs`, `src/ops/error.rs`, `src/ops/signal.rs`, `src/ops/mod.rs` | ~12,000 | Agent-G2 |
| **G3 — indexes (in-mem hashtable + redb backends + migration)** | `src/index/mod.rs`, `src/index/hashtable.rs`, `src/index/backend.rs`, `src/index/redb_primary.rs`, `src/index/redb_unmined.rs`, `src/index/redb_dah.rs`, `src/index/secondary_backend.rs`, `src/index/dah_index.rs`, `src/index/unmined_index.rs`, `src/index/migration.rs`, `src/index/util.rs` | ~11,000 | Agent-G3 |
| **G4 — recovery + redo + checkpoint** | `src/recovery.rs`, `src/redo.rs`, `src/checkpoint.rs` | ~7,700 | Agent-G4 |
| **G5 — wire protocol + dispatch (entry surface)** | `src/protocol/codec.rs`, `src/protocol/frame.rs`, `src/protocol/opcodes.rs`, `src/protocol/mod.rs`, `src/server/dispatch.rs` | ~17,800 | Agent-G5 |
| **G6 — HTTP server + server startup + server mod + observability + metrics** | `src/server/http.rs`, `src/server/startup.rs`, `src/server/mod.rs`, `src/observability/mod.rs`, `src/metrics.rs` | ~6,200 | Agent-G6 |
| **G7 — replication (manager + receiver + protocol + tcp + durable + batching)** | `src/replication/manager.rs`, `src/replication/receiver.rs`, `src/replication/protocol.rs`, `src/replication/tcp_transport.rs`, `src/replication/durable.rs`, `src/replication/batching.rs`, `src/replication/mod.rs` | ~10,200 | Agent-G7 |
| **G8 — cluster control plane (coordinator, swim, topology, migration, shards, membership, auth, routing)** | `src/cluster/coordinator.rs`, `src/cluster/swim.rs`, `src/cluster/topology.rs`, `src/cluster/migration.rs`, `src/cluster/shards.rs`, `src/cluster/membership.rs`, `src/cluster/auth.rs`, `src/cluster/routing.rs`, `src/cluster/mod.rs` | ~21,000 | Agent-G8 |
| **G9 — storage tiers (blobstore + gc + uploader + manager + tiers + input_refs)** | `src/storage/blobstore.rs`, `src/storage/manager.rs`, `src/storage/blob_gc.rs`, `src/storage/uploader.rs`, `src/storage/tiers.rs`, `src/storage/input_refs.rs`, `src/storage/mod.rs` | ~4,100 | Agent-G9 |
| **G10 — binaries + config + lib root** | `src/bin/server.rs`, `src/bin/cli.rs`, `src/config.rs`, `src/lib.rs` | ~4,300 | Agent-G10 |

Total in-scope: ~103.7k LOC across 70 files.

## Out-of-scope (with reason)

| Path | Reason |
|------|--------|
| `target/` | Build artifacts |
| `.claude/worktrees/*` | Prior parallel-agent worktrees (copies of source, not source of truth) |
| `tests/` | Test suite — read for context only; not graded as production surface |
| `benches/` | Criterion harnesses — performance, not correctness gates |
| `fuzz/` | Fuzz harnesses — useful as test fixtures, not production |
| `teraslab-tests/` | External Docker E2E harness (own crate) |
| `client/go/` | Separate Go module |
| `client/rust/` | Separate client crate; the server-side parser of client-sent frames is what matters and is covered in G5 |
| `docker/`, `scripts/`, `ui/`, `docs/`, `phases/`, `specs/`, `.github/` | Operational config, docs, plans — informational, not the implementation under review |
| `Cargo.lock` | Pinned versions; dependency advisory scanning is noted in cross-cutting (Phase 3), not as per-file findings |
| `tarpaulin-report.json` | Stale coverage artifact |
| `AUDIT.md`, `AUDIT_CODEX.md` | Prior internal audit reports; referenced for orientation only, never copied; this review re-verifies against current code |
| `_plans/`, `_review/` | Working area |
| `patch_*.sh`, `test_process_expired.rs` | Stray top-level scripts; not built |

## Scope rationale & feasibility

70 files / ~104k LOC. Three files exceed 10k LOC. One agent cannot read every line in one session with the required depth. The 10-group split lets parallel reviewer agents work on logically isolated modules, each producing its own findings file. The orchestrator (this session) aggregates and writes Phase 3 (cross-cutting) + Phase 4 (the consolidated report).

Each reviewer agent receives:
1. Its file list (read end-to-end, or section-by-section if >800 lines).
2. The Phase 2 finding template (verbatim) and severity rubric.
3. Output path `_review/02_findings_<group>.md`.
4. Ledger expectations: for every file, emit either ≥1 finding or a one-line positive verification note.

## Coverage ledger

Tick `[x]` after the file has been read end-to-end (or section-by-section for >800 lines) and findings/verification recorded. Format: `[ ] path (LOC) — pending` until the reviewer agent reports.

### G1 — core data plane

- [x] `src/device.rs` (1485)
- [x] `src/io.rs` (974)
- [x] `src/record.rs` (1451)
- [x] `src/allocator.rs` (2475)
- [x] `src/device_io/mod.rs` (111)
- [x] `src/device_io/sync_fallback.rs` (326)
- [x] `src/device_io/io_uring_backend.rs` (574)
- [x] `src/locks.rs` (140)
- [x] `src/fault_injection.rs` (310)

### G2 — ops engine + ops sub-paths

- [x] `src/ops/engine.rs` (10889)
- [x] `src/ops/create.rs` (140)
- [x] `src/ops/spend.rs` (210)
- [x] `src/ops/unspend.rs` (34)
- [x] `src/ops/set_mined.rs` (61)
- [x] `src/ops/delete_eval.rs` (528)
- [x] `src/ops/remaining.rs` (128)
- [x] `src/ops/mark_longest_chain.rs` (29)
- [x] `src/ops/error.rs` (141)
- [x] `src/ops/signal.rs` (20)
- [x] `src/ops/mod.rs` (12)

### G3 — indexes

- [x] `src/index/mod.rs` (1761)
- [x] `src/index/hashtable.rs` (2049)
- [x] `src/index/backend.rs` (1375)
- [x] `src/index/redb_primary.rs` (1580)
- [x] `src/index/redb_unmined.rs` (1074)
- [x] `src/index/redb_dah.rs` (939)
- [x] `src/index/secondary_backend.rs` (655)
- [x] `src/index/dah_index.rs` (254)
- [x] `src/index/unmined_index.rs` (362)
- [x] `src/index/migration.rs` (1267)
- [x] `src/index/util.rs` (16)

### G4 — recovery + redo + checkpoint

- [x] `src/recovery.rs` (4008)
- [x] `src/redo.rs` (3302)
- [x] `src/checkpoint.rs` (366)

### G5 — wire protocol + dispatch

- [x] `src/protocol/codec.rs` (3511)
- [x] `src/protocol/frame.rs` (525)
- [x] `src/protocol/opcodes.rs` (412)
- [x] `src/protocol/mod.rs` (9)
- [x] `src/server/dispatch.rs` (13399)

### G6 — HTTP server + observability + metrics

- [x] `src/server/http.rs` (3387)
- [x] `src/server/startup.rs` (905)
- [x] `src/server/mod.rs` (743)
- [x] `src/observability/mod.rs` (557)
- [x] `src/metrics.rs` (1659)

### G7 — replication

- [x] `src/replication/manager.rs` (2382)
- [x] `src/replication/receiver.rs` (3959)
- [x] `src/replication/protocol.rs` (1631)
- [x] `src/replication/tcp_transport.rs` (808)
- [x] `src/replication/durable.rs` (1342)
- [x] `src/replication/batching.rs` (120)
- [x] `src/replication/mod.rs` (12)

### G8 — cluster control plane

- [x] `src/cluster/coordinator.rs` (10383)
- [x] `src/cluster/swim.rs` (1340)
- [x] `src/cluster/topology.rs` (2107)
- [x] `src/cluster/migration.rs` (3314)
- [x] `src/cluster/shards.rs` (1560)
- [x] `src/cluster/membership.rs` (1148)
- [x] `src/cluster/auth.rs` (502)
- [x] `src/cluster/routing.rs` (384)
- [x] `src/cluster/mod.rs` (11)

### G9 — storage tiers

- [x] `src/storage/blobstore.rs` (1469)
- [x] `src/storage/manager.rs` (1463)
- [x] `src/storage/blob_gc.rs` (430)
- [x] `src/storage/uploader.rs` (367)
- [x] `src/storage/tiers.rs` (187)
- [x] `src/storage/input_refs.rs` (266)
- [x] `src/storage/mod.rs` (12)

### G10 — binaries + config + lib root

- [x] `src/bin/server.rs` (1259)
- [x] `src/bin/cli.rs` (1280)
- [x] `src/config.rs` (1778)
- [x] `src/lib.rs` (22)
