# TeraSlab v1 — Coverage Review

Per-subsystem, what is and is **not** covered by tests. Baseline: clean isolated `cargo test --all` = **2710 passed / 0 failed / 0 ignored** (70 suites). No `#[ignore]`, no `assert!(true)`, no bare `.is_ok()/.is_err()` asserts found anywhere; all `#[allow(...)]` carry justifications except those in REL-104.

The opcode→handler→test and code→trigger→test matrices live in `REVIEW.md` §3; this file is the gap inventory.

## Strong coverage (verified, non-vacuous)

- **UTXO ops / record** — all 12 mutation opcodes + GET/GET_SPEND have handler+test (matrix in REVIEW §3.1). Double-spend: 100-thread concurrent test asserts exactly one winner and that every loser reads the winner's exact 36-byte spending_data (`engine.rs:8910,15648`). Per-slot + per-header CRC with explicit torn-payload rejection. Coinbase maturity / frozen-until cooldown with boundary heights. Unspend ownership semantics. DAH eval matrix incl. overflow (checked, not saturating), reassigned-exclusion, preserve_until blocking.
- **Durability / recovery** — WAL-before-ack ordering enforced per mutation; idempotent replay with value-level + idempotent-second-pass assertions across the BeforeRedoFsync / AfterRedoFsync / AfterApplyBeforeSync crash windows (`tests/fault_injection.rs`, `tests/recovery_crash_boundaries.rs`, `tests/crash_sweep_ops.rs` — all 12 ops). Torn-tail recovery + sequence continuity; F-G4-001 header CRC; B-3 compaction crash-safety via CrashCowDevice; direct-I/O alignment rejection; checkpoint↔mutation mutual exclusion. Replay-op→handler→idempotency-token matrix in REVIEW §3.3.
- **Concurrency / allocator** — best-fit/coalesce, double-free + overlapping-free rejection, redo journaling + rollback on flush failure, replay idempotency, header CRC tamper detection; striped-lock seed-stability + collision-resistance + RwLock exclusion; simulation determinism pinned by `simulation_reproducibility` (`e2e_workload.rs:770`).
- **Cluster** — shard mapping deterministic + Go golden vector; HRW/round-robin placement permutation-invariance/RF-invariance/single-master/minimal-movement property tests; quorum/split-brain at unit (`activation_quorum_needed`, M1.3 restore-floors-peak) and e2e (`cluster_partition.rs::partitioned_minority_never_self_activates_topology`, `g8_split_brain.rs`); SWIM HMAC convergence; placement-version refusal on propose+commit. Error/feature→trigger→test matrix in REVIEW §3.5.
- **Replication** — per-key ACK quorum + status mapping (`d6_per_key_quorum_*`), strict-zero-acks-is-hard-error, degraded→status-5 only at RF=1, timeout escalation; the d39a612 double-length-prefix HMAC fix has a RED-before-fix apply-and-verify test; receiver sequencing (gap NAK, dup-skip, watermark) + wrapping generation guard. ReplicaOp→handler→token→round-trip matrix in REVIEW §3.5.
- **Storage / blobs** — external-blob double integrity (store SHA-256 sidecar + record-anchored ExternalRef re-validation; swap-attack test g9_002); GC orphan TOCTOU closed by mtime grace (g9_004) + pin handshake (g9_018) with real race tests; stream codes 16/17/18/34, byte cap, concurrent-stream cap, idle/close cleanup all e2e.
- **Protocol / server** — codec round-trip/truncation/oversize/sub-header; all batch decoders count-before-alloc + per-item caps + fuzz-non-panic (`wire_fuzz_smoke.rs`); real inflight-bytes + connection-cap accounting tests; admin auth SHA-256 + constant-time compare; error-code conformance asserts exact wire codes + data-payload sizes (full code matrix in REVIEW §3.7).
- **Index** — Robin Hood insert/get/remove/resize/collision/adversarial-DoS + file-backed reopen + crash-after-resize sentinel; primary device-scan rebuild tolerance across all 3 primary backends; secondary fail-closed → ERR_INDEX_DEGRADED; snapshot checksum/truncation/version/poison rejection; import-in-progress sentinel; redb durability (append+fsync before commit).
- **Config / startup** — RemoteBindRefused, StrictAuthRequiresSecret/ClusterId, AdminTokenRequired, ack_policy/degraded best-effort rejection at RF>1, device_id/cluster_id pinning, cluster-secret agreement — all with non-vacuous tests; startup fail-closed per-cause (no in-memory WAL fallback).
- **Security** — every non-test `unsafe` carries an upheld contract; vout validated vs utxo_count before direct slot access; oversize-frame/conn-cap/per-IP/slow-loris/stream-DoS all assert typed wire codes; AArch64 torn-read regression test backs the io_locks design.
- **End-to-end durability** — YES, it exists: in-process WAL-boundary kill/restart sweep for all 12 ops (`crash_sweep_ops.rs`), deterministic 10-seed crash injection (`e2e_workload.rs`), real-SIGKILL 3-node Docker suite with full consistency reconciliation + atomicity (`scenario_15`), cluster double-spend rejection (`scenario_03`), split-brain prevention (`scenario_14`). Op→crash-sweep→cluster-e2e→double-spend-guard matrix in REVIEW §4.

## Gaps (correctness-critical paths with thin/absent or mis-gated coverage)

| ID | Gap | Severity |
|----|-----|----------|
| REL-015 | Cluster crash-recovery (`scenario_15`) + split-brain (`scenario_14`) — the authoritative "no acked write lost / no double-spend after SIGKILL" proof — run **weekly only**, never PR/nightly/release gate. | major |
| REL-016 | Go client never exercised against a real server in CI (`integration_test.go` build-tag-gated; CI omits the tag). | major |
| REL-017 | Snapshot + portable export/import round-trip tests only insert all-zero cached fields and assert only `record_offset` — a field-zeroing/offset-swap regression on the most fidelity-critical serialization path would pass CI. | major (test-gap) |
| REL-001 | `g2_delete_race` asserts functional non-aliasing but cannot detect the torn-read/UB on the unguarded tombstone memcpy (no TSan/loom/aarch64-release torn-header assertion). | (blocker fix needs this test) |
| REL-111 | On-disk durability of persisted peak/committed-term across a real crash/rename boundary is untested (only in-memory `restore()`); this is exactly why REL-002 went uncaught. | minor (→ would catch a blocker) |
| REL-105 | Issue-#14 orphan absence asserted by in-memory `next_offset()` proxy, not a crash+recovery cycle. | minor |
| REL-107 | No `recover()` test for valid-magic header with out-of-range `count` (where REL-100 lives). | minor |
| REL-108 | `stress_random_operations` partitions txids per thread → no same-stripe/same-txid interleaving exercised at the engine level. | minor |
| REL-116 | `STREAM_END` declared-size-mismatch branch untested. | minor |
| REL-122 | `remove()` of a probe-distance-capped (>254) entry never executed by tests. | minor |
| REL-130 | Cluster e2e double-spend oracle checks `spent_count` only, not per-slot spending_data (in-process tests do check it). | minor |
| REL-131 | `http_observability` tests check metric name presence, not increment-on-op (value coverage exists in `prometheus_conformance`/`e5_auth_metrics`). | minor |
| REL-132 | `block_device_size` sandbox path can pass with zero assertions when a loop device can't attach (CI forces the real env-device variant). | minor |
| — | DELETED_CHILDREN (code 35) defense-in-depth: no focused engine test found that resurrects-then-prunes a child and asserts idempotent re-spend → `DeletedChildren`. Conformance test asserts the wire code/1B payload; verify an engine-level test exists or add one. | minor |
| — | Full "create external → delete → run GC → assert blob gone" loop covered piecewise (delete path + GC no-index deletion) but not as one regression. | minor |
| REL-200/201 | Reviewers flagged "config entropy/sizing validators" and "Go `classifyRetry`" as untested; adversarial verification **refuted both** (coverage exists / low-risk). Recorded as dropped; a `classifyRetry` table test is still cheap and worth adding. | dropped |

## CI gating summary (where the real risk is)

| Tier | Runs | Holds |
|------|------|-------|
| PR (`ci.yml`) | every PR | unit/lib (2710), `crash_sweep_ops`, `fault_injection`, e2e scenarios **01/02/03** (incl. cluster double-spend reject), Go unit (no `-tags integration`), Rust client tests |
| Nightly | nightly | `e2e_workload` crash injection (10 seeds) |
| Weekly | weekly | release tier: **scenario_14 split-brain, scenario_15 crash-recovery**, 16, 17 |

The cardinal-contract cluster tests (14, 15) are weekly-only and not a release gate — see REL-015.
