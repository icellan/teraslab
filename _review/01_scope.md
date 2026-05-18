# Phase 1 — Scope Declaration

**Date:** 2026-05-18
**Based on:** `_review/00_orientation.md`
**Current tree:** `p1.1-cluster-id @ cd6570c` (post G1–G10 remediation; active cluster-id / split-brain work)

---

## 1. In scope — directories and files to be reviewed, grouped by module

The review target is the **TeraSlab main crate implementation** (security, correctness, code quality). All files that can affect:

- Wire protocol correctness / DoS / auth bypass (TCP on 3300, replication, cluster control)
- Crash safety, durability, recovery, redo log
- UTXO mutation invariants (create/spend/setmined/unspend/reassign/delete)
- Cluster membership / split-brain / migration safety
- HTTP admin surface (auth, injection, resource exhaustion)
- Low-level device I/O and allocator accounting
- Dependency / build / config hygiene that affects security

### Group A — Wire protocol, server dispatch, HTTP admin (primary untrusted input surface)
- `src/protocol/mod.rs`
- `src/protocol/frame.rs`
- `src/protocol/opcodes.rs`
- `src/protocol/codec.rs`
- `src/server/mod.rs`
- `src/server/dispatch.rs`
- `src/server/http.rs`
- `src/server/startup.rs`
- `src/config.rs` (ServerConfig, bounds, auth secrets)
- `src/bin/server.rs` (startup wiring, shutdown)
- `src/bin/cli.rs` (CLI surface — lower priority but operator path)

### Group B — Cluster consensus, membership, auth, split-brain (active on p1.1-cluster-id)
- `src/cluster/mod.rs`
- `src/cluster/auth.rs`
- `src/cluster/coordinator.rs`
- `src/cluster/membership.rs`
- `src/cluster/migration.rs`
- `src/cluster/routing.rs`
- `src/cluster/shards.rs`
- `src/cluster/swim.rs`
- `src/cluster/topology.rs`
- `tests/g8_cluster_id.rs` (new untracked test — must be reviewed for what it claims)
- `tests/cluster_edge_cases.rs`
- `tests/cluster_swim.rs`
- `tests/cluster_tcp.rs`
- `tests/g8_split_brain.rs`
- `tests/g8_swim_replay.rs`
- `tests/g8_ping_req_cap.rs`

### Group C — Core engine, ops, record layout, allocator, device (hot mutation + crash paths)
- `src/lib.rs`
- `src/record.rs`
- `src/allocator.rs`
- `src/device.rs`
- `src/device_io/mod.rs`
- `src/device_io/io_uring_backend.rs`
- `src/device_io/sync_fallback.rs`
- `src/ops/mod.rs`
- `src/ops/engine.rs`
- `src/ops/error.rs`
- `src/ops/create.rs`
- `src/ops/spend.rs`
- `src/ops/unspend.rs`
- `src/ops/set_mined.rs`
- `src/ops/delete_eval.rs`
- `src/ops/mark_longest_chain.rs`
- `src/ops/remaining.rs`
- `src/ops/signal.rs`
- `src/locks.rs`
- `src/io.rs`
- `src/fault_injection.rs` (test-only but affects what is tested)

### Group D — Index, redo, replication, recovery, storage (durability + secondary paths)
- `src/index/mod.rs`
- `src/index/backend.rs`
- `src/index/hashtable.rs`
- `src/index/redb_primary.rs`
- `src/index/redb_unmined.rs`
- `src/index/redb_dah.rs`
- `src/index/dah_index.rs`
- `src/index/unmined_index.rs`
- `src/index/secondary_backend.rs`
- `src/index/util.rs`
- `src/index/migration.rs`
- `src/redo.rs`
- `src/recovery.rs`
- `src/checkpoint.rs`
- `src/replication/mod.rs`
- `src/replication/protocol.rs`
- `src/replication/manager.rs`
- `src/replication/receiver.rs`
- `src/replication/durable.rs`
- `src/replication/batching.rs`
- `src/replication/tcp_transport.rs`
- `src/storage/mod.rs`
- `src/storage/manager.rs`
- `src/storage/blobstore.rs`
- `src/storage/tiers.rs`
- `src/storage/blob_gc.rs`
- `src/storage/uploader.rs`
- `src/storage/input_refs.rs`

### Group E — Observability, metrics, glue, build config (supporting but security-relevant)
- `src/metrics.rs`
- `src/observability/mod.rs`
- `Cargo.toml`
- `deny.toml`
- `clippy.toml`
- `.github/workflows/ci.yml`
- `.github/workflows/nightly.yml`
- `.github/workflows/release.yml`
- `tests/g5_protocol_auth.rs`
- `tests/g10_config.rs`
- `tests/g10_lifecycle.rs`
- `tests/g10_review.rs`
- `tests/prometheus_conformance.rs`
- `tests/http_observability.rs`
- `tests/ui_xss.rs`
- `tests/tracing_lint.rs`
- `tests/integration.rs`
- `tests/server_tcp.rs`
- `tests/recovery_crash_boundaries.rs`
- `tests/replication_tcp.rs`
- `tests/e2e_workload.rs`
- `tests/fault_injection.rs`
- `tests/stress_tests.rs`
- `tests/simulation/mod.rs` (and sub)
- `tests/workload/mod.rs` (and sub)

**Total in-scope for full review:** ~71 src/*.rs + ~20 key test files + 3 workflow yamls + 3 Cargo/*.toml = ~100 artifacts.

---

## 2. Out of scope (with one-line reason)

- `target/` (build artifacts, 10s of GB, generated)
- `teraslab-tests/client/target/` (same)
- `Cargo.lock` (pinned deps; review via `cargo tree` + deny instead of manual)
- `docker/config/*.toml` and `docker-compose.*.yml` (deployment examples; configs are data, not code under review unless they expose new surfaces)
- `scripts/*.sh` and `scripts/cluster/*.toml` (orchestration glue; only if they exec untrusted input — low risk)
- `specs/*.md` and `specs/teranode.lua` (reference docs and old Lua UDF; not implementation)
- `phases/*.md` (build plan documents)
- `docs/` (design notes, superpowers)
- `benches/*.rs` (perf microbenchmarks; correctness not their job)
- `client/go/` and `client/rust/` (separate SDK crates; their own review)
- `ui/*.js|html|css` (static embedded assets; only the serving path in http.rs is in scope)
- `teraslab-tests/results/` (historical run artifacts)
- `*.md` at root except where they are the review deliverables themselves (AUDIT*, REVIEW_REPORT* are prior context)
- `patch_*.sh`, `test_process_expired.rs` (one-off patches)
- `.idea/`, `.DS_Store`, editor/IDE noise
- Any file under `.claude/worktrees/agent-*` (parallel agent workspaces — ephemeral, cleaned by script)
- Generated code inside `target/` or `out/` dirs of any sub-crate

**Rationale for excluding client/ and benches/:** They consume the library; bugs there do not compromise the server invariants directly. If time allows after core, a lightweight pass can be added.

---

## 3. Coverage ledger

Every in-scope file starts unticked. Will be updated in Phase 2 as `[x] path — N findings` (or `— 0 findings: <one-line verification note>`).

**Format:** `[ ] path/to/file — status`

### Group A ledger
- [ ] `src/protocol/mod.rs`
- [ ] `src/protocol/frame.rs`
- [ ] `src/protocol/opcodes.rs`
- [ ] `src/protocol/codec.rs`
- [ ] `src/server/mod.rs`
- [ ] `src/server/dispatch.rs`
- [ ] `src/server/http.rs`
- [ ] `src/server/startup.rs`
- [ ] `src/config.rs`
- [ ] `src/bin/server.rs`
- [ ] `src/bin/cli.rs`

### Group B ledger
- [ ] `src/cluster/mod.rs`
- [ ] `src/cluster/auth.rs`
- [ ] `src/cluster/coordinator.rs`
- [ ] `src/cluster/membership.rs`
- [ ] `src/cluster/migration.rs`
- [ ] `src/cluster/routing.rs`
- [ ] `src/cluster/shards.rs`
- [ ] `src/cluster/swim.rs`
- [ ] `src/cluster/topology.rs`
- [ ] `tests/g8_cluster_id.rs`
- [ ] `tests/cluster_edge_cases.rs`
- [ ] `tests/cluster_swim.rs`
- [ ] `tests/cluster_tcp.rs`
- [ ] `tests/g8_split_brain.rs`
- [ ] `tests/g8_swim_replay.rs`
- [ ] `tests/g8_ping_req_cap.rs`

### Group C ledger
- [ ] `src/lib.rs`
- [ ] `src/record.rs`
- [ ] `src/allocator.rs`
- [ ] `src/device.rs`
- [ ] `src/device_io/mod.rs`
- [ ] `src/device_io/io_uring_backend.rs`
- [ ] `src/device_io/sync_fallback.rs`
- [ ] `src/ops/mod.rs`
- [ ] `src/ops/engine.rs`
- [ ] `src/ops/error.rs`
- [ ] `src/ops/create.rs`
- [ ] `src/ops/spend.rs`
- [ ] `src/ops/unspend.rs`
- [ ] `src/ops/set_mined.rs`
- [ ] `src/ops/delete_eval.rs`
- [ ] `src/ops/mark_longest_chain.rs`
- [ ] `src/ops/remaining.rs`
- [ ] `src/ops/signal.rs`
- [ ] `src/locks.rs`
- [ ] `src/io.rs`
- [ ] `src/fault_injection.rs`

### Group D ledger
- [ ] `src/index/mod.rs`
- [ ] `src/index/backend.rs`
- [ ] `src/index/hashtable.rs`
- [ ] `src/index/redb_primary.rs`
- [ ] `src/index/redb_unmined.rs`
- [ ] `src/index/redb_dah.rs`
- [ ] `src/index/dah_index.rs`
- [ ] `src/index/unmined_index.rs`
- [ ] `src/index/secondary_backend.rs`
- [ ] `src/index/util.rs`
- [ ] `src/index/migration.rs`
- [ ] `src/redo.rs`
- [ ] `src/recovery.rs`
- [ ] `src/checkpoint.rs`
- [ ] `src/replication/mod.rs`
- [ ] `src/replication/protocol.rs`
- [ ] `src/replication/manager.rs`
- [ ] `src/replication/receiver.rs`
- [ ] `src/replication/durable.rs`
- [ ] `src/replication/batching.rs`
- [ ] `src/replication/tcp_transport.rs`
- [ ] `src/storage/mod.rs`
- [ ] `src/storage/manager.rs`
- [ ] `src/storage/blobstore.rs`
- [ ] `src/storage/tiers.rs`
- [ ] `src/storage/blob_gc.rs`
- [ ] `src/storage/uploader.rs`
- [ ] `src/storage/input_refs.rs`

### Group E ledger (support + config + CI + selected tests)
- [ ] `src/metrics.rs`
- [ ] `src/observability/mod.rs`
- [ ] `Cargo.toml`
- [ ] `deny.toml`
- [ ] `clippy.toml`
- [ ] `.github/workflows/ci.yml`
- [ ] `.github/workflows/nightly.yml`
- [ ] `.github/workflows/release.yml`
- [ ] `tests/g5_protocol_auth.rs`
- [ ] `tests/g10_config.rs`
- [ ] `tests/g10_lifecycle.rs`
- [ ] `tests/g10_review.rs`
- [ ] `tests/prometheus_conformance.rs`
- [ ] `tests/http_observability.rs`
- [ ] `tests/ui_xss.rs`
- [ ] `tests/tracing_lint.rs`
- [ ] `tests/integration.rs`
- [ ] `tests/server_tcp.rs`
- [ ] `tests/recovery_crash_boundaries.rs`
- [ ] `tests/replication_tcp.rs`
- [ ] `tests/e2e_workload.rs`
- [ ] `tests/fault_injection.rs`
- [ ] `tests/stress_tests.rs`
- [ ] `tests/simulation/mod.rs`
- [ ] `tests/workload/mod.rs`

**Total ledger entries:** 71 src + 20 tests + 6 config/CI = 97 files.

---

## 4. Assessment of session feasibility (per operating rule)

**This full set (~97 files, ~110 kLOC of complex concurrent systems code with unsafe-adjacent I/O, consensus, and crash-recovery invariants) is TOO LARGE to review thoroughly in a single interactive session without degrading quality.**

Per the explicit stop condition:
> "the codebase is too large for one session (propose a split in Phase 1 and stop)"

**Decision:** Write this scope file, declare the split, and **stop here**. Do not begin Phase 2 reads or findings on this run.

### Proposed phased split for follow-up sessions (recommended order)

1. **Session 2 (highest risk surface first):** Group A + Group B (protocol, dispatch, HTTP, cluster auth/coordinator) + the 6 new cluster tests + config + 2-3 workflow yamls. ~25 files. This covers the active `p1.1-cluster-id` changes and the historically worst auth/DoS findings (F-G5-001, F-G8-001, F-G5-002, etc.).

2. **Session 3:** Group C (engine + allocator + device + core ops) — the mutation hot path and delete/free ordering issues from prior (F-G2-001 etc.). ~22 files.

3. **Session 4:** Group D (index backends, redo, replication, recovery, storage) — durability and replica apply windows. ~28 files.

4. **Session 5 (lighter):** Group E remaining + any cross-cutting that emerged, plus a spot-check of 5-10 high-risk tests for test quality (do they actually assert the invariants they claim?). ~20 files.

Each sub-session would still:
- Re-read the current 00_orientation + this 01_scope (or a narrowed one)
- Produce its own `02_findings_G<letter>.md` using the exact template
- Update only its slice of the ledger
- Feed into a final consolidated REPORT only after all slices complete

**Alternative (if forced to single pass):** Narrow to "security-critical only" — protocol/*, server/dispatch+http, cluster/auth+coordinator+topology, config, redo, recovery, ops/error+engine (the parts that touch auth or untrusted frames), plus the g5/g8/g10 test files. This would be ~35 files and could be attempted, but still risks shallow coverage on the 10k+ LOC files.

**Recommendation to user:** Approve a split (start with Session 2 on Group A+B). The prior parallel-agent campaign already did the broad pass; the value now is a focused re-audit on the post-fix `cluster-id` branch where the most recent high-severity work (split-brain, cluster_secret enforcement, ping caps) landed.

---

**Gate status:** This file is on disk. Scope declared. Because the set is too large for one session, **Phase 2 is not started**. Awaiting user decision on split vs. narrowed scope before any source body is read for findings.

**Self-check against operating rules:**
- Evidence rule will be followed in any future findings (file:line + ≤10-line excerpt).
- No phantom coverage: ledger will only be ticked after end-to-end read (or systematic sections for >800 LOC files).
- No silent passes: every ticked file will have either ≥1 finding or a concrete one-line verification note of what was actually checked.
- Anti-rationalization: boilerplate and "small" files will still be read; severity applied mechanically.
