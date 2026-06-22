# TeraSlab v1 Release Review — Findings Ledger

Append-only ledger. Status: `open` | `verified` | `deferred`.

---

## Phase 0 — Baseline

| ID | Severity | Category | Status | Evidence | Description |
|----|----------|----------|--------|----------|-------------|
| REL-001 | major | CI / tests | verified | `tests/cluster_swim.rs:347,468,501` | Initial `cargo test --all` run failed 3 `cluster_swim` tests while another agent held build/file locks and ran tests concurrently. **Clean rerun (2026-06-22, no contention): 2710 passed / 0 failed / 0 ignored**, including `cluster_swim` 11/11 in 5.75s. Timing race remains possible under heavy parallel CPU load (Suspect→Alive vs Dead→Alive for `NodeJoined`). |
| REL-002 | minor | docs | verified | `README.md:34` vs clean rerun | README claims `2234 passed / 0 failed`; clean rerun counted **2710 passed** across 70 test binaries. Count drift, not functional failure. |
| REL-003 | — | CI / tests | verified | Grep `#[ignore]` across `**/*.rs` | Zero `#[ignore]` on correctness tests. Compliant with project rules. |
| REL-004 | — | build | verified | `cargo build --release`, `cargo build`, `cargo clippy --all -- -D warnings` | All clean, zero warnings (2026-06-22). |
| REL-005 | minor | code quality | open | `src/**/*.rs` — 3000+ `.unwrap()`/`.expect()` occurrences across library `src/` | Project rules ban `unwrap`/`expect` in library code; widespread use is latent panic surface on invariant violations. |
| REL-006 | — | code quality | verified | Grep `todo!`, `unimplemented!` in `src/` | None in `src/`. `unreachable!` only in `src/replication/manager.rs:1932` (test panic path). |
| REL-007 | minor | code quality | open | `client/rust/src/lib.rs:436,648,1061,1561` | Four `unreachable!()` in shipped Rust client library. |
| REL-008 | — | code quality | verified | Grep `dbg!` | Zero `dbg!` in tree. |
| REL-009 | minor | code quality | open | `#[allow(...)]` inventory — see session-log | Most allows justified (CLI `disallowed_macros`, `too_many_arguments`, test `dead_code`). `src/index/redb_primary.rs:213,227` `unnecessary_wraps` may mask API design issues. |

---

## Phase 1.1–1.2 — Operations & Error Codes

| ID | Severity | Category | Status | Evidence | Description |
|----|----------|----------|--------|----------|-------------|
| REL-101 | minor | coverage | open | `dispatch.rs:872`; no `tests/*.rs` TCP test | `OP_QUERY_OLD_UNMINED` (30) — unit tests only. |
| REL-102 | minor | coverage | open | `dispatch.rs:874` | `OP_PRESERVE_TRANSACTIONS` (31) — unit tests only. |
| REL-103 | minor | coverage | open | `dispatch.rs:909` | `OP_HEALTH` (101) — unit tests only. |
| REL-104 | **major** | coverage | open | `dispatch.rs:891`; no tests found | `OP_GET_COMMITTED_TOPOLOGY` (103) — zero tests anywhere. |
| REL-105 | **major** | coverage | open | `dispatch.rs:902`; `coordinator.rs:3670` | `OP_PARTITION_VERSION_REPORT` (105) — zero wire tests. |
| REL-106 | minor | docs | open | `opcodes.rs:21,35` vs `README.md:341-369` | Opcodes 13 (`RemoveConflictingChildBatch`) and 33 (`QueryConflicting`) implemented but undocumented in README. |
| REL-107 | minor | coverage | open | `dispatch.rs:914` | `OP_HELLO` (107) — no `tests/*.rs` wire test. |
| REL-108 | minor | coverage | open | `engine.rs:2146-2199`; `server_tcp.rs:706-761` | Idempotent re-spend (same spending_data → OK) not wire-tested. |
| REL-109 | — | correctness | verified | `server_tcp.rs:477+`, `error_code_conformance.rs` | Error codes 0–20, 255 have wire triggerability tests for core item errors. |
| REL-110 | minor | coverage | open | `record.rs:909-910`, `engine.rs:2230-2231` | No test asserts 37/41-byte logical spend write region vs 73-byte physical slot. |
| REL-111 | — | correctness | verified | `dispatch.rs:9271-9272`, `server_tcp.rs:706-761` | `ALREADY_SPENT` returns exact 36-byte spending data. |
| REL-112 | — | correctness | verified | `engine.rs:2204-2207`, `server_tcp.rs:737-761`, `recovery.rs:5651-5689` | Double-spend rejection and idempotent same-data path verified at engine level. |

---

## Phase 1.3 — Durability & Recovery

| ID | Severity | Category | Status | Evidence | Description |
|----|----------|----------|--------|----------|-------------|
| REL-200 | — | durability | verified | `dispatch.rs:633-658`, `redo.rs:2167-2182`, `recovery.rs:5-20` | WAL-first: redo fsync before engine apply and client ack. |
| REL-201 | minor | docs | open | `dispatch.rs:647-649` vs `recovery.rs:13-17` | Dispatch doc overstates O_DIRECT data-write durability on return. |
| REL-202 | major | durability | open | `dispatch.rs:2229-2231` | `redo_log=None` skips WAL entirely; no release-gate test proving production never starts without redo. |
| REL-203 | — | durability | verified | `recovery.rs:33-36`, `recovery.rs:1713-1715` | Idempotent replay via absolute state, not counter bumps. |
| REL-204 | major | durability | open | `recovery.rs:1691`, `index/backend.rs:104-115` | Recovery uses infallible `lookup()`; redb read errors collapse to benign `MissingPrimary`. |
| REL-205 | — | durability | verified | `redo.rs:6-14`, `redo.rs:2091-2095` | Linear-with-reset redo buffer, not in-place circular wrap. |
| REL-206 | — | durability | verified | `redo.rs:2570-2720` | Torn redo tail recovery stops at corruption, replays prefix. |
| REL-207 | — | durability | verified | `recovery.rs:1702-1726`, `io.rs:24-62` | Torn on-device slots recoverable from redo + CRC guards. |
| REL-208 | — | durability | verified | `redo.rs:2428-2453` | B-3 compaction relocation fixes in-place torn hazard. |
| REL-209 | — | durability | verified | `config.rs:473-474`, `device.rs:186-210` | O_DIRECT 4096-byte alignment enforced end-to-end. |
| REL-210 | — | durability | verified | `startup.rs:383-394`, `bin/server.rs:568-570` | Primary redb corrupt → fail-closed; operator must remove file to rebuild. |
| REL-211 | — | durability | verified | `bin/server.rs:576-588` | Secondary redb corrupt → degraded readiness + `ERR_INDEX_DEGRADED`. |
| REL-212 | minor | durability | open | `redb_dah.rs:249-300`, `redb_unmined.rs:268-317` | Secondary redb `range_query` returns empty vec on read error (logged). |
| REL-213 | — | ops | verified | `startup.rs:356-381` | Import sentinel blocks redb startup during interrupted migration. |

---

## Phase 1.5 — Cluster & Replication

| ID | Severity | Category | Status | Evidence | Description |
|----|----------|----------|--------|----------|-------------|
| REL-301 | — | cluster | verified | `shards.rs:521-524`, `client/go/cluster.go:36-37` | Shard formula `u16_le(txid[0..2]) & 0x0FFF` consistent server + clients. |
| REL-302 | — | cluster | verified | `dispatch.rs:4296-4315` | `NO_QUORUM` rejects minority writes against peak size. |
| REL-303 | — | cluster | verified | `coordinator.rs:8912-8916`, `topology.rs:790-793` | Peak cluster size persisted, raise-only. |
| REL-304 | — | replication | verified | `dispatch.rs:3206-3253`, `opcodes.rs:298` | Per-key replication quorum; `ERR_REPLICATION_FAILED` on timeout. |
| REL-305 | — | migration | verified | `dispatch.rs:4626-4664`, `migration.rs:474-477` | `MIGRATION_IN_PROGRESS` write fencing enforced. |
| REL-306 | — | security | verified | `swim.rs:786-794`, `auth.rs:159-181`, `config.rs:189-195` | HMAC-SHA256 on SWIM + inter-node TCP when `cluster_secret` set. |
| REL-307 | major | CI / tests | verified | `tests/cluster_swim.rs:347-373`; `membership.rs:158-174` | `cluster_swim` timing-sensitive under extreme parallel CPU contention (observed when another agent ran tests simultaneously). Passes cleanly in isolated rerun. `NodeJoined` only on Dead→Alive, not Suspect→Alive — latent flake under load. |
| REL-308 | **major** | tests | open | `scenario_13_data_migration_under_load.rs:311`, `scenario_14_split_brain_prevention.rs:295` | E2E harness uses `% 4096` instead of `& 0x0FFF` for shard routing — wrong for ~1/16 of keyspace. |
| REL-309 | minor | docs | open | `cluster_swim.rs:354-356` vs `swim.rs:455` | Stale test comment about `SystemTime` incarnation; actual uses `persisted_incarnation + 1`. |

---

## Phase 1.6 & 1.8 — Storage, Pruning, Index

| ID | Severity | Category | Status | Evidence | Description |
|----|----------|----------|--------|----------|-------------|
| REL-401 | minor | docs | deferred | `tiers.rs:34-37`, `phases/11_tiered_storage.md:3-9` | Middle separate-block tier (8KiB–1MiB) intentionally not implemented. Documented partial phase. |
| REL-402 | **major** | pruning | open | `dispatch.rs:8683-8755`, `config.rs:970` | `ProcessExpiredPreservations` DAH sweep fails when due set exceeds `max_batch_size` (8192). |
| REL-403 | **blocker** | clients / pruning | verified | `dispatch.rs:8636-8652`, `client/go/codec.go:330-333`, `client/rust/src/lib.rs:1751` | Shipped Go + Rust clients send 4-byte `ProcessExpiredPreservations` payload; server skips expiry phase (retention=0). Preservation leak for Teranode pruner parity. |
| REL-404 | major | pruning | open | `dispatch.rs:8364-8392` | `QueryOldUnmined` response unbounded (no pagination cap). |
| REL-405 | minor | protocol | open | `codec.rs:1878-1896` | No explicit per-chunk size cap in `decode_stream_chunk` (frame layer only). |
| REL-406 | minor | storage | open | `opcodes.rs:676`, `tiers.rs:22-27` | Inline cold data allowed up to 4 MiB; 8 KiB tier threshold advisory only. |
| REL-407 | major | coverage | open | `server_tcp.rs:3107-3113` | Index backend matrix omits pruning, streaming, external-blob paths. |
| REL-408 | minor | ops | open | `redb_primary.rs:601-603`, `backend.rs:402-405` | redb `snapshot()` no-op; operators must use `export_index`. Undocumented in README runbook. |
| REL-409 | minor | docs | open | `blobstore.rs:3-5` vs `tiers.rs:28` | Stale "> 1 MiB" comment; actual threshold client-driven at 8 KiB advisory. |
| REL-410 | minor | perf | open | `engine.rs:1706-1726` | Expired-preservation scan is O(primary_index) full scan. |
| REL-411 | major | security | open | `dispatch.rs:9042-9096` | Zero-byte `OP_STREAM_CHUNK` refreshes idle timer without advancing bytes — stream hold-open abuse. |
| REL-412 | minor | pruning | open | `dispatch.rs:8375-8385` | `QueryOldUnmined` silently drops candidates on metadata read failure. |
| REL-413 | major | coverage | open | `server_tcp.rs:2120-2198` | No TCP E2E: stream → external create → read cold data. |

---

## Phase 2 — Test Suite

| ID | Severity | Category | Status | Evidence | Description |
|----|----------|----------|--------|----------|-------------|
| REL-601 | minor | CI | open | `.github/workflows/ci.yml` vs `migration_crash.rs`, `migration_fence.rs` | `migration_crash` and `migration_fence` fault-injection tests exist but not wired into CI. |
| REL-602 | major | coverage | open | `protocol/deadline.rs` (no tests), `g10_lifecycle.rs` | No dedicated tests for request deadlines, real SIGTERM shutdown, `WriteAll` at cluster scale in Docker. |
| REL-603 | major | e2e | open | `scenario_15_crash_recovery_correctness.rs` — `sleep(5ms)` kill timing | Docker crash tests use probabilistic sleep-based kill timing, not deterministic WAL sync points. |
| REL-604 | — | durability | verified | `recovery_crash_boundaries.rs`, `crash_sweep_ops.rs`, `scenario_15` | Tier A deterministic in-process crash tests exist; Tier C Docker SIGKILL is smoke-level only. |
| REL-605 | major | clients | open | `client/go/integration_test.go` (`//go:build integration`); `client/rust` | Go live-server integration tests excluded from CI; Rust client has no pool/conn/cluster module tests or Docker e2e. |
| REL-606 | major | release process | open | `teraslab-tests/docker/run_all.sh` tier matrix | PR CI runs only scenarios 01–03; crash scenario 15 is weekly tier only. |

---

## Phase 3 — Performance

| ID | Severity | Category | Status | Evidence | Description |
|----|----------|----------|--------|----------|-------------|
| REL-701 | — | perf | verified | `benches/spend_throughput.rs`, run 2026-06-22 Apple M3 | Single spend: **~4.9 Melem/s** (~205 ns/op) on MemoryDevice. README "10M+ ops/sec" is design target, disclaimed at `README.md:7`. |
| REL-702 | — | perf | verified | `benches/mixed_workload.rs` | Mixed workload realistic_ratio: **~3.9 Melem/s** on MemoryDevice. |
| REL-703 | minor | perf | open | `benches/spend_throughput.rs` — no `DirectDevice` bench | No published NVMe + O_DIRECT + redo durability throughput numbers. Unverified-on-this-host. |
| REL-704 | minor | perf | open | No p99.9 latency histogram benchmark | README p99.9 claim marked "not yet measured" (`README.md:18`). No tail-latency bench in tree. |
| REL-705 | minor | clients | open | `client/go` bench results | Go client benches measure encode/decode only (~28 ns small request), not live-server throughput. |
| REL-706 | minor | clients | open | `client/rust` — no bench target | Rust client has no Criterion bench against live server. |

---

## Phase 4 — Documentation

| ID | Severity | Category | Status | Evidence | Description |
|----|----------|----------|--------|----------|-------------|
| REL-501 | major | docs | open | `config.rs:483-593` vs `README.md:137-225` | 18+ config keys in code undocumented in README (tombstone block, streaming caps, observability, index sub-paths). |
| REL-501a | minor | docs | open | `README.md:177` vs `config.rs:390-391` | `[index].backend` docs omit `file_backed` option. |
| REL-502 | — | docs | verified | `http.rs:333-433` vs `README.md:460-537` | All HTTP routes match README. |
| REL-502a | minor | docs | open | `http.rs:2372-2389` | `/admin/top?local=true` undocumented. |
| REL-502b | minor | docs | open | `http.rs:30-34,1703` | `/admin/drain/{node_id}?wait_seconds=` undocumented. |
| REL-503 | major | docs | open | `cli.rs:144-173` vs `README.md:768-785` | `export-index`, `import-index`, `repair` missing from CLI command table. |
| REL-503a | **major** | docs | open | `README.md:760-764` vs `cli.rs:330-333` | README says `status` works without admin token; CLI hits gated `/debug/*` endpoints. |
| REL-503b | minor | docs | open | `cli.rs:455` vs `README.md:760-761` | `storage` command gated but not listed among auth-required commands. |
| REL-503c | minor | docs | open | `cli.rs:425` | `shards` is public-surface only but not listed among unauthenticated commands. |
| REL-503d | minor | docs | open | `cli.rs:58-64` vs `phases/13_admin_tooling.md:114` | `--data-addr` default `127.0.0.1:3300` undocumented; phase doc says `localhost:3000`. |
| REL-503e | minor | docs | open | `README.md:783` vs `cli.rs:746-751` | `bench` sends OP_PING over binary wire, not spend/create throughput as spec §14 implies. |
| REL-504a | minor | docs | open | `phases/13_admin_tooling.md:3` vs `cli.rs:144-173` | Phase 13 status header omits newer CLI commands. |
| REL-504d | minor | docs | open | `BSV_UTXO_STORE_SPEC.md:2217-2218` vs `cli.rs:746-751` | Spec documents `bench spend`/`bench create`; shipped CLI runs PING only. |

---

## Phase 5 — Security

| ID | Severity | Category | Status | Evidence | Description |
|----|----------|----------|--------|----------|-------------|
| REL-801 | — | security | verified | `config.rs:189-195`, `README.md:236-237` | `strict_auth=true` (default) refuses clustered config without `cluster_secret`. |
| REL-802 | major | security | open | `dispatch.rs:9042-9096` | Zero-byte stream chunk hold-open (REL-411) — resource exhaustion vector. |
| REL-803 | — | security | verified | `opcodes.rs:612,676-692`, `frame.rs` tests | Frame size caps enforced; oversize → `PAYLOAD_MALFORMED`. |
| REL-804 | minor | security | open | `src/**` — 168 `unsafe` occurrences across 15 files | Requires per-block safety invariant audit; `device.rs`, `io.rs`, `hashtable.rs` highest density. Not fully re-audited block-by-block in this review pass. |