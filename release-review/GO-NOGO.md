# TeraSlab v1 Release Recommendation

**Date:** 2026-06-22 (updated after clean test rerun)  
**Recommendation:** **NO-GO**

---

## Decision

TeraSlab is **not ready for a v1.0 durability-and-correctness tag** on the evidence collected in this review. Core server-side WAL/recovery, UTXO spend semantics, and the full test suite are strong — **2710 tests passed / 0 failed / 0 ignored** on a clean isolated rerun. One release-blocking gap remains: a **client–server wire contract bug** that silently disables preservation expiry.

---

## Blockers (must clear before tagging v1)

| ID | Finding | Why it blocks |
|----|---------|---------------|
| **REL-403** | Go + Rust clients send 4-byte `ProcessExpiredPreservations`; server skips expiry when `block_height_retention=0` | Teranode pruner parity broken; preserved transactions leak indefinitely — silent data retention failure |

---

## Strong secondary gates (should clear; escalate if deferred)

| ID | Finding | Risk if deferred |
|----|---------|------------------|
| REL-307 | `cluster_swim` timing-sensitive under extreme parallel CPU contention | Flaky CI if multiple heavy jobs share host; passes cleanly in isolated rerun |
| REL-308 | E2E scenarios 13/14 use `% 4096` shard formula | Migration/split-brain tests may validate wrong routing for 1/16 of keyspace |
| REL-104/105 | Opcodes 103, 105 have zero tests | Topology adoption and post-commit migration planning untested at wire boundary |
| REL-601 | `migration_crash` / `migration_fence` not in CI | Migration crash-safety regressions undetected at PR tier |
| REL-605 | Go integration tests excluded from CI; Rust client lacks pool/conn tests | Client bitrot against live cluster |

---

## What passed review

- **Full test suite green** — `cargo test --all`: 2710 passed, 0 failed, 0 ignored (clean rerun 2026-06-22)
- **`cluster_swim`** — 11/11 pass in 5.75s (after concurrent-agent contention ruled out)
- WAL-first durability ordering (redo fsync before ack) — REL-200 verified
- Idempotent recovery replay — REL-203 verified
- Spend correctness: `ALREADY_SPENT` 36-byte payload, double-spend rejection — REL-111/112 verified
- Error codes 0–20, 255 wire triggerability (core item errors) — REL-109 verified
- O_DIRECT 4096 alignment — REL-209 verified
- Cluster quorum / NO_QUORUM / HMAC auth — REL-301–306 verified
- Zero `#[ignore]` on correctness tests — REL-003 verified
- Clippy clean — REL-004 verified

---

## Performance posture

README honestly disclaims 10M+ ops/sec as a MemoryDevice design target. Measured ~4.9 Melem/s single spend on Apple M3. **No blocker** for performance claims given README disclaimer, but NVMe production numbers remain unpublished.

---

## Minimum remediation path to GO

1. **Fix REL-403:** Update `client/go/codec.go` and `client/rust/src/lib.rs` to send 8-byte payload `[current_height:4][block_height_retention:4]`. Add client conformance test.
2. **Fix REL-308:** Replace `% 4096` with `& 0x0FFF` in scenarios 13/14.
3. Run weekly Docker tier (`teraslab-tests/docker/run_all.sh --tier release`) and record results.
4. Consider hardening `cluster_swim` against parallel CPU starvation (REL-307).

After (1)–(3): reassess as **GO-WITH-CAVEATS** if remaining majors (REL-104/105, REL-402, REL-601) are explicitly deferred with operator runbook mitigations.

---

## Unreviewed / incomplete in this pass

- Per-`unsafe` block safety invariant audit (168 occurrences, 15 files) — sampled, not exhaustive
- Live-server client throughput benchmarks (Go + Rust)
- Linux NVMe block-device bench on real hardware
- Docker weekly e2e tier (17 scenarios) — not executed in this review pass