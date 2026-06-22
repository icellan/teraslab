# TeraSlab v1 Release Review

**Reviewer:** Automated release-readiness review  
**Date:** 2026-06-22  
**Host:** Apple M3, 24 GiB RAM, macOS darwin 25.3.0  
**Tree state:** `main` (behind origin/main by 3 commits); local modifications in index/replication/server paths

---

## 1. Executive summary

| Item | Value |
|------|-------|
| **Recommendation** | **NO-GO** |
| **Blockers** | 1 |
| **Major** | 14 |
| **Minor** | 29 |
| **Single most important fact** | Shipped Go and Rust clients send a legacy 4-byte `ProcessExpiredPreservations` payload, causing the server to **skip the entire preservation-expiry phase** — a silent correctness failure for any deployment using the official clients with Teranode's pruner. The full test suite is green (2710 passed) on a clean isolated rerun; an initial failure was caused by concurrent test execution from another agent. |

Supporting artifacts: [FINDINGS.md](FINDINGS.md), [PERF.md](PERF.md), [COVERAGE.md](COVERAGE.md), [GO-NOGO.md](GO-NOGO.md), [session-log.md](session-log.md).

---

## 2. Release blockers

### REL-403 — Client preservation-expiry wire contract broken

**Evidence:**
- Server: `src/server/dispatch.rs:8636-8652` — 4-byte payload sets `block_height_retention = 0`, skipping expiry entirely
- Go client: `client/go/codec.go:330-333` — `encodeProcessExpired` sends only `currentHeight`
- Rust client: `client/rust/src/lib.rs:1751` — 4-byte payload only
- Server test pins behavior: `dispatch.rs:10700-10708` (`ko1_legacy_payload_skips_expiry_phase`)

**Why it blocks:** Records with expired `preserve_until` never enter DAH sweep. Pruner cannot reclaim them. This is silent incorrectness — no error returned.

**Remediation:** Extend both clients to 8-byte form; add client integration test; document breaking wire change if any external caller depends on 4-byte form.

### ~~REL-001~~ — Downgraded to major (not a blocker)

Initial `cargo test --all` failed 3 `cluster_swim` tests while another agent ran tests concurrently on the same host (build lock contention). **Clean isolated rerun: 2710 passed / 0 failed / 0 ignored**, including `cluster_swim` 11/11 in 5.75s. Timing race under extreme parallel CPU load remains a latent flake risk (REL-307) but is not a release blocker on clean CI.

---

## 3. Correctness review

### 3.1 UTXO operations (`src/ops/`)

All README-scoped opcodes (1–12, 20–21, 30–32, 100–102, 200–201) are implemented and dispatched (`dispatch.rs:762-946`). Spend path preserves hash bytes 0–31, mutates status+spending_data+CRC (`engine.rs:2230-2231`, `record.rs:238-244`). `ALREADY_SPENT` returns winner's 36-byte spending data (`server_tcp.rs:706-761`). Unspend, freeze/unfreeze/reassign cooldown, coinbase immaturity, conflicting/locked paths all have wire tests.

**Gaps:** Opcodes 103/105 untested (REL-104/105). Pruner ops 30/31 lack TCP integration tests. Idempotent re-spend not wire-tested (REL-108). Opcodes 13/33 implemented but undocumented (REL-106).

### 3.2 Error codes (`src/protocol/`)

Codes 0–20 and 255 are reachable and tested on wire for core item errors (`server_tcp.rs:477`, `error_code_conformance.rs`). Error data payloads match spec for `ALREADY_SPENT` (36 B), `COINBASE_IMMATURE` (4 B), `FROZEN_UNTIL` (4 B).

### 3.3 Durability (`src/redo.rs`, `src/recovery.rs`, `src/io.rs`)

WAL-first contract verified end-to-end (REL-200). Recovery replay is idempotent (REL-203). Redo buffer is linear-with-reset, not circular wrap (REL-205). Torn redo tail and on-device slot recovery handled (REL-206/207). O_DIRECT 4096 alignment enforced (REL-209). Primary redb corrupt file → fail-closed with operator recovery path (REL-210).

**Residual risks:** Recovery uses infallible `lookup()` on redb (REL-204). `redo_log=None` bypass in test harness (REL-202).

### 3.4 Concurrency (`src/locks.rs`, `src/allocator.rs`)

65536 lock stripes with power-of-two masking verified. Engine has 99-thread concurrent spend race test (`engine.rs:15648-15671`). Allocator has extensive unit tests. Threaded spend bench shows severe contention at 2+ threads.

### 3.5 Cluster & replication

Shard formula, NO_QUORUM, peak persistence, replication ACK policies, migration fencing, and HMAC auth all verified (REL-301–306). E2E test harness has wrong shard formula in scenarios 13/14 (REL-308).

### 3.6 Storage & pruning

Two-tier storage (inline + external) is intentional; middle tier deferred (REL-401). Streaming error paths well tested. `ProcessExpiredPreservations` DAH sweep can exceed `max_batch_size` (REL-402). Zero-byte stream chunk abuse vector (REL-411).

### 3.7 Wire protocol

Frame format, size caps, batch limits enforced (`opcodes.rs:612,676-692`). Fuzz smoke test in CI.

### 3.8 Index backends

Hashtable + redb + export/import functional. Backend TCP matrix omits pruning/streaming (REL-407).

---

## 4. Test & coverage review

See [COVERAGE.md](COVERAGE.md).

**Strengths:** 2250+ lib unit tests, 67 integration binaries, deterministic in-process crash tests (`crash_sweep_ops.rs`, `recovery_crash_boundaries.rs`), 17 Docker scenarios.

**Weaknesses:** Parallel `cluster_swim` failures; migration crash tests not in CI; Go live integration excluded; Docker crash timing probabilistic; client pruner wire bug untested.

---

## 5. Performance review

See [PERF.md](PERF.md).

| Metric | Measured (M3, MemoryDevice) |
|--------|----------------------------|
| Single spend | ~4.9 Melem/s (210 ns/op) |
| Mixed workload | ~3.9 Melem/s |
| Threaded spend (2T) | ~80 Kelem/s |

README disclaimer at line 7 correctly states numbers are design targets. **10M+ claim not met on this host** (~49% of ceiling). NVMe production throughput **unverified**.

---

## 6. Documentation review

HTTP routes match implementation (REL-502 pass). Config reference has 18+ undocumented keys (REL-501). CLI docs wrong about `status` auth requirements (REL-503a). `export-index`/`import-index`/`repair` missing from command table (REL-503). Phase 11 middle tier correctly marked partial.

---

## 7. Security & safety review

`strict_auth` enforces `cluster_secret` for clustered configs (REL-801). Frame size caps prevent trivial OOM (REL-803). Stream zero-byte hold-open is a resource exhaustion vector (REL-802/411). Full `unsafe` audit not completed block-by-block (REL-804).

---

## 8. Deferred items

| Item | Justification |
|------|---------------|
| REL-401 Middle NVMe tier | Intentional scope decision per `phases/11_tiered_storage.md` |
| REL-703–704 NVMe / p99.9 benches | Requires Linux NVMe hardware; README already disclaims |
| REL-804 Full unsafe audit | Sampled; no OOB issues found in spot checks of `device.rs` alignment checks |

---

## 9. Verification record

### Built and run

```bash
cargo build --release          # clean
cargo build                    # clean
cargo clippy --all -- -D warnings  # clean (verified on rerun)
cargo test --all               # 2710 passed / 0 failed / 0 ignored (clean rerun)
# Initial run failed 3 cluster_swim tests due to concurrent agent on same host
cargo bench --bench spend_throughput -- single_spend --noplot
cargo bench --bench mixed_workload -- --noplot
cargo bench --bench index_ops -- --noplot
cd client/go && go test -bench=. -benchtime=2s -run=^$ ./...
```

### Could NOT verify on this host

- Real NVMe `DirectDevice` throughput (macOS dev, no raw block device in CI loop)
- SSD wear / production p99.9 tail latency
- Linux `BLKGETSIZE64` against real `/dev/nvme`
- Docker weekly e2e tier (17 scenarios) — not executed in this review pass
- Live-server Go/Rust client throughput benchmarks
- Exhaustive per-`unsafe` block audit (168 occurrences)