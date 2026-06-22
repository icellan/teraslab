# TeraSlab v1 Test Coverage Review

**Date:** 2026-06-22

---

## 1. Test Suite Baseline

| Command | Result (review host) |
|---------|---------------------|
| `cargo test --all` (initial, concurrent agent on host) | **FAILED** — 3 failures in `cluster_swim` (contention artifact) |
| `cargo test --all` (clean rerun, 2026-06-22) | **2710 passed / 0 failed / 0 ignored** across 70 test binaries |
| `cluster_swim` (within clean rerun) | **11 passed, 0 failed** in 5.75s |
| `cargo clippy --all -- -D warnings` | Clean |
| `cd client/go && go test ./...` | Pass (unit only; integration tag excluded) |
| `#[ignore]` on correctness tests | **0** |

---

## 2. Subsystem → Test Mapping

### Durability / WAL / Recovery

| Component | Unit (`src/`) | Integration (`tests/`) | Docker E2E | CI |
|-----------|---------------|------------------------|------------|-----|
| Redo log | `redo.rs` extensive | `g4_*` (14 files) | — | PR |
| Recovery replay | `recovery.rs` | `recovery_crash_boundaries.rs` | — | PR |
| Checkpoint | `checkpoint.rs` | — | — | PR |
| Crash sweep (all ops × 3 windows) | — | `crash_sweep_ops.rs` | — | PR (fault-injection) |
| Migration crash | — | `migration_crash.rs` | — | **NOT in CI** |
| Migration fence | — | `migration_fence.rs` | — | **NOT in CI** |
| Property crash+replay | — | `property_utxo.rs` | — | PR |
| SIGKILL mid-write | — | — | `scenario_15` | Weekly |

### UTXO Operations

| Component | Unit | Integration | Docker E2E | CI |
|-----------|------|-------------|------------|-----|
| Engine (all ops) | `ops/engine.rs` ~200+ | `integration.rs`, `server_tcp.rs` | `scenario_02` | PR |
| Error code conformance | `dispatch.rs` | `error_code_conformance.rs`, `server_tcp.rs:477` | — | PR |
| Spend correctness | `ops/spend.rs`, `engine.rs` | `server_tcp.rs:706-761`, `property_utxo.rs` | `scenario_02` | PR |
| Delete eval | `ops/delete_eval.rs` | `integration.rs` | — | PR |
| Pruning ops 30–32 | `dispatch.rs` unit | `server_tcp.rs:1631` (op 32 only, 8-byte form) | — | Partial |

### Index

| Component | Unit | Integration | CI |
|-----------|------|-------------|-----|
| Hashtable | `index/hashtable.rs` | `integration.rs` | PR |
| redb primary/DAH/unmined | `index/redb_*.rs` | `secondary_redb_degraded.rs` | PR |
| Backend matrix (TCP) | — | `server_tcp.rs:3107` — ping/create/spend/get only | PR |
| Export/import | `index/migration.rs` | `cli_integration.rs` | PR |

### Cluster / Replication

| Component | Unit | Integration | Docker E2E | CI |
|-----------|------|-------------|------------|-----|
| SWIM | `cluster/swim.rs` | `cluster_swim.rs` (11/11 clean; timing-sensitive under extreme CPU contention) | `scenario_01` | PR |
| Coordinator/topology | `cluster/coordinator.rs` | `cluster_tcp.rs`, `cluster_partition.rs` | `scenario_14` | PR |
| Replication TCP | `replication/*.rs` | `replication_tcp.rs` | `scenario_03` | PR |
| Split-brain | — | `g8_split_brain.rs` | `scenario_14` | Weekly |
| Migration under load | — | — | `scenario_13` | Weekly |

### Storage / Blobs / Streaming

| Component | Unit | Integration | CI |
|-----------|------|-------------|-----|
| BlobStore | `storage/blobstore.rs` | `server_tcp.rs:2120-2198` | PR |
| Stream errors | `dispatch.rs` | `g_h1_h2_stream_dos.rs` | PR |
| Stream → external create E2E | — | **None** | — |
| Tier boundaries (8KiB/1MiB) | `storage/tiers.rs` | Partial (`e2e_workload.rs` ~4KiB only) | — |

### Protocol / Wire

| Component | Unit | Integration | CI |
|-----------|------|-------------|-----|
| Frame/codec | `protocol/frame.rs`, `codec.rs` | `wire_fuzz_smoke.rs` | PR |
| Deadline enforcement | — | **None** | — |
| libFuzzer deep fuzz | `fuzz/decode_request` | — | Nightly manual |

### Clients

| Client | Unit/Mock | Live Integration | CI |
|--------|-----------|------------------|-----|
| Go (`client/go/`) | 16 `*_test.go` files | `integration_test.go` (build tag `integration`) | Unit only |
| Rust (`client/rust/`) | 13 tests in `lib.rs` | 5 in-process tokio tests | PR |
| Test harness (`teraslab-tests/client/`) | lib helpers | 17 Docker scenarios | PR: 01–03; Weekly: all |

---

## 3. Opcode Test Matrix (README-scoped)

| Opcode | Wire integration test | Unit test |
|--------|----------------------|-----------|
| 1 SpendBatch | ✅ `server_tcp.rs` | ✅ |
| 2 UnspendBatch | ✅ | ✅ |
| 3 SetMinedBatch | ✅ | ✅ |
| 4 CreateBatch | ✅ | ✅ |
| 5–12 mutations | ✅ | ✅ |
| 20 GetBatch | ✅ | ✅ |
| 21 GetSpendBatch | ✅ | ✅ |
| 30 QueryOldUnmined | ❌ | ✅ `dispatch.rs` |
| 31 PreserveTransactions | ❌ | ✅ `dispatch.rs` |
| 32 ProcessExpiredPreservations | ✅ (8-byte payload) | ✅ |
| 100 GetPartitionMap | ✅ `cluster_tcp.rs` | ✅ |
| 101 Health | ❌ | ✅ |
| 102 Ping | ✅ | ✅ |
| 200 StreamChunk | ✅ | ✅ |
| 201 StreamEnd | ✅ | ✅ |
| 103 GetCommittedTopology | ❌ | ❌ |
| 105 PartitionVersionReport | ❌ | ❌ |

---

## 4. Error Code Test Matrix (0–20, 255)

All codes 0–20 and 255 have at least one wire or unit test observing the code, except:
- Code 0 (OK) — ubiquitous
- Codes triggered only via cluster paths (14, 15, 19, 20) — covered in `cluster_tcp.rs`, `migration_fence.rs`, `g8_split_brain.rs`

**Client conformance gap:** `ProcessExpiredPreservations` server expiry phase untested via shipped clients (4-byte vs 8-byte payload). See REL-403.

---

## 5. Weak / Problematic Tests

| Test | Issue | Severity |
|------|-------|----------|
| `cluster_swim.rs` | Failed only under concurrent-agent CPU contention; passes cleanly in isolated rerun. Suspect-vs-Dead race latent under extreme load | **Medium** |
| `scenario_13/14` shard routing | `% 4096` ≠ `& 0x0FFF` — tests wrong shard for 1/16 keyspace | **Major** |
| `scenario_15` crash subtests | `sleep(5ms)` before SIGKILL — probabilistic, not mid-WAL deterministic | **Medium** |
| `ko1_legacy_payload_skips_expiry_phase` | Documents that 4-byte payload skips expiry — **confirms client bug** | Info |
| `backend_matrix!` | Would pass if pruning/streaming broken on redb backend | **Medium** |

---

## 6. Durability E2E Tiers

| Tier | Tests | Deterministic mid-write? |
|------|-------|--------------------------|
| A — In-process | `recovery_crash_boundaries.rs`, `crash_sweep_ops.rs`, `fault_injection.rs`, `secondary_two_phase_durability.rs` | ✅ Yes (sync-point injection) |
| B — Simulation | `e2e_workload.rs`, `property_utxo.rs` | Probabilistic |
| C — Docker SIGKILL | `scenario_15`, `scenario_04`, `scenario_16` | Probabilistic (sleep-gated) |

**Release gate implication:** PR CI does not run Tier C. Weekly tier (`run_all.sh --tier release`) is intended pre-release gate.

---

## 7. Coverage Gaps (Correctness-Critical, No Test)

1. `OP_GET_COMMITTED_TOPOLOGY` (103) — topology adoption wire path
2. `OP_PARTITION_VERSION_REPORT` (105) — post-commit migration planning
3. `src/protocol/deadline.rs` — request timeout enforcement
4. Real SIGTERM → graceful shutdown with in-flight writes
5. Shipped client `ProcessExpiredPreservations` 8-byte wire conformance
6. Stream → external blob create → read cold data (TCP E2E)
7. `WriteAll` replication under partition at Docker scale
8. redb backend pruning/streaming parity via `backend_matrix!`

---

## 8. CI Tier Summary

| Gate | Docker scenarios | Fault-injection | Client live |
|------|------------------|-----------------|-------------|
| PR | 01–03 | `fault_injection`, `crash_sweep_ops`, `cluster_delayed_activation` | Go unit; Rust in-process |
| Nightly | 01–11, 17 | + `e2e_workload` | Same |
| Weekly | 01–17 | + real SIGKILL | Same |

**Missing from PR fault-injection step:** `migration_crash`, `migration_fence` (REL-601).