# TeraSlab v1 Release Review

Final pre-release gate. Method: 13 parallel per-subsystem reviewers (full source + tests read) producing `path:line` findings → 2 adversarial skeptics per blocker/major finding (majority-confirm to survive) → gate-owner reconciliation + independent re-verification of escalated items and the prior review's blocker. Companion files: `FINDINGS.md` (ledger), `COVERAGE.md`, `PERF.md`, `GO-NOGO.md`, `session-log.md`.

Baseline: HEAD `920ac32`, version bumped to **0.7.0** this session. `cargo build --release` clean (0 warnings); `cargo test --all` (clean isolated) **2710 / 0 / 0**; `cargo clippy --all --all-targets` clean.

---

## 1. Executive summary

**Recommendation: NO-GO** (clearable) — tag v1 after **2 small blockers** land.

- **Blockers: 2.** Both are narrow, on durability/correctness-critical paths, and both have small, well-understood fixes for which the codebase already has the right primitives:
  - **REL-001** — the delete-tombstone writer is a non-atomic data race (UB) against the lock-free read path. It bypasses the exact `io_locks().write` + `atomic_store_from` discipline every other direct writer uses and that the codebase itself documents as mandatory (CRC-alone torn protection is empirically insufficient on aarch64).
  - **REL-002** — the cluster peak/topology persist paths never fsync the parent directory after `rename`, so a crash can roll the persisted split-brain peak back to a smaller value → minority self-activation → double-spend window. The project already has `fsync_parent_dir`; the cluster layer just doesn't call it (a Rule-6 inconsistency with the storage layer).
- **Majors: 10.** Mostly clustered in the **client libraries** (Rust can't bootstrap a default secure cluster; Rust unspend/get_spend mis-route in cluster mode; Go ignores the per-item redirect the server actually emits; a Go test asserts a wire shape the server never sends) and **CI gating** (the authoritative cluster crash-recovery + split-brain e2e run weekly-only, not as a merge/release gate; the Go client is never exercised against a real server in CI). Plus REL-014 (both clients send a 4-byte `ProcessExpiredPreservations` → the expiry phase silently no-ops), REL-017 (the most fidelity-critical index serialization path is only round-trip-tested with all-zero fields), REL-018 (on-by-default tombstone subsystem undocumented), REL-019 (a security doc inverts the `strict_auth` default).
- **Minors: 45.** Overwhelmingly docs drift (stale sizes/defaults/io_uring references after the CRC/format changes) plus dead-code cleanup and small test gaps.

**The single most important thing:** the *engine* is genuinely v1-grade — durability ordering, idempotent replay, double-spend rejection, crash sweeps across all 12 ops, and split-brain logic are all real and well-tested. The v1 risk has moved to the **edges**: two small unguarded-write/unsynced-rename holes in otherwise-correct code, the **client libraries' cluster behavior**, and the fact that the **cluster durability contract is not a CI gate**. None of the engine's core correctness claims were found false; the blockers are discipline lapses, not design flaws.

| | Count |
|---|---|
| Blockers | 2 |
| Majors | 10 |
| Minors | 45 |
| Dropped (refuted by verification) | 2 |

---

## 2. Release blockers (must clear before tagging v1)

### REL-001 — Delete tombstone write is a non-atomic data race against lock-free readers (UB)
- **Evidence:** `src/ops/engine.rs:1518-1525` — `write_zeroed_metadata_header` direct branch does bare `std::ptr::copy_nonoverlapping` + a lone `Release` fence under only the per-tx stripe lock. Every other direct writer takes `io_locks().write` + `atomic_store_from` (`src/io.rs:863,941,1176`); the lock-free GET readers take `io_locks().read` + `atomic_load_into` (`src/io.rs:771,814,1073`; callers `engine.rs:6382,6446,6273`) and do **not** hold the stripe lock.
- **Why it blocks:** non-atomic write concurrent with the lock-free read of the same bytes = data race = UB. The codebase documents (`io.rs:858-862`) that CRC-alone torn protection fails ~90% on aarch64 release — which is why the atomic-store path exists. This is the one writer that skips it. Fails closed in practice (CRC/txid recheck → StorageError/TxNotFound) but "fails closed" is not a guarantee under UB.
- **Fix (small):** route the tombstone write through `io_locks().write(record_offset)` + `atomic_store_from` (or reuse `write_metadata_direct`). Add an aarch64-release/loom `g2_delete_race` variant asserting no torn header.

### REL-002 — Cluster peak/topology persist never fsyncs the parent directory after atomic rename
- **Evidence:** `src/cluster/coordinator.rs:7454-7470,7479-7522,7578-7590` — all three persist paths do `create(tmp)→write→sync_all→rename` with no parent-dir fsync. `fsync_parent_dir` already exists for this pattern at `src/index/util.rs:11-23` and `src/storage/blobstore.rs:133-155`.
- **Why it blocks:** `sync_all` makes file contents durable but not the rename's directory entry. A crash between rename and the next dir flush can roll `peak_cluster_size` back to a smaller value; peak gates restart quorum, so a smaller peak lets a rebooted node self-activate in a minority partition it should reject with `NO_QUORUM` — a split-brain double-spend window. The code's own comments call these persists "safety-critical for restart quorum."
- **Fix (small):** fsync the parent dir after each rename (promote `fsync_parent_dir` to shared util). Add a crash/rename-durability test (REL-111 — the existing test only exercises in-memory `restore()`).

Both fixes are localized, use existing in-tree primitives, and are independently testable. No design change required.

---

## 3. Correctness review (per subsystem)

Full coverage notes + matrices in `COVERAGE.md`; findings in `FINDINGS.md`. Verdicts below are gate-owner conclusions after adversarial verification.

### 3.1 UTXO operations & record layout — PASS (1 blocker REL-001, else clean)
All 12 mutation opcodes + GET/GET_SPEND implemented, dispatched, and tested with non-vacuous assertions. Double-spend rejection proven by a 100-thread test asserting a single winner and that every loser reads the winner's exact 36-byte spending_data. ALREADY_SPENT/PRUNED/FROZEN/FROZEN_UNTIL return exact payloads at boundary heights; coinbase maturity returns the 4-byte required height; per-slot CRC torn-write protection is written and verified. Unspend is a true inverse with a counter-zero guard. The only substantive defect is REL-001 (delete path). Opcode→handler→test matrix:

| Op | Handler (`engine.rs`) | Representative tests |
|----|----|----|
| 1 Spend | `validate_spend_multi`/`apply` (1830,6704) | double-spend 100-thread (8910,15648), frozen/pruned/immature/frozen-until + boundaries |
| 2 Unspend | `unspend` (2298) | no-op/error semantics, counter-zero guard, mismatch-no-mutate |
| 3 SetMined | `set_mined*` (2440-2862) | block-entry add/remove + `BlockEntriesFull` |
| 4 Create | `create*` (2957-3452) | external-ref-required, frozen/conflicting/locked, size-contract |
| 5-7 Freeze/Unfreeze/Reassign | (3974/4045/4095) | cooldown round-trip, u32-overflow rejection, old-hash-spend-fails |
| 8-12 Conflicting/Locked/Preserve/MarkLongest | (4983-5386,2862) | blocks-spend + DAH interaction |
| 11 Delete | `delete*` (5569-5853) | tombstone→sync→unregister→free ordering, KO-3 due-guard, race test |
| 20/21 Get/GetSpend | (6382/6273) | 36-byte spending_data for spent/frozen/pruned |

### 3.2 Error-code triggerability — PASS (docs drift only)
Every documented code 0-35 + 255 has a real trigger and an on-wire conformance assertion (full code→trigger→test matrix in COVERAGE / the protocol-server reviewer output). Error-data payload sizes verified: ALREADY_SPENT 36B, COINBASE_IMMATURE 4B, FROZEN_UNTIL 4B, DELETED_CHILDREN 1B. Two codes are emitted on the wire but missing from the README table — 36 `NOT_DUE`, 37 `MIGRATION_TARGET_NOT_READY` (REL-121). No documented code is unreachable.

### 3.3 Durability & crash recovery — PASS (strongest subsystem)
WAL-before-ack ordering enforced per mutation. Replay is genuinely idempotent (absolute slot state with recomputed counters for spend/unspend; generation/block_id tokens for metadata ops; skip-if-present for create) — verified by value-level, idempotent-second-pass assertions across the three crash windows. Linear log with CRC-protected persisted header (F-G4-001); torn-tail recovery; B-3 compaction crash-safety via CrashCowDevice; direct-I/O alignment rejected (not silently corrupted); checkpoint↔mutation mutual exclusion via shared RwLock; startup fail-closes (exit 1) on any replay error. The atomic AllocateRegion+CreateV2 batch (920ac32) is correctly gated by `is_allocated_range` on replay. Replay-op→handler→idempotency-token matrix in COVERAGE. No blocker/major found here; minors REL-103/105.

### 3.4 Concurrency, allocator, locks — PASS (1 minor REL-100)
The allocator is serialized engine-wide under one `Mutex<SlotAllocator>` — so "concurrent freelist races" are largely moot. Double-free/overlap rejection, redo journaling + rollback on flush failure, replay idempotency, and header CRC tamper detection are all tested. Striped locks use a per-process seeded mapping resistant to txid-grinding stripe-collision DoS. `reserve_batch`/`commit_pending`/`rollback_pending` are correctly fenced against checkpoint (both take the exclusive `dispatch_visibility_barrier`). One real but minor defect: `recover()` does `count*16` before bounds-checking `count` (REL-100) — panics in debug on a crafted header, wraps+fails-closed in release.

### 3.5 Clustering & replication — PASS (1 blocker REL-002, else strong)
Shard mapping is exactly `u16_le(txid[0..2]) & 0x0FFF` → 4096 shards (Go golden vector). Quorum uses a monotonic, persisted, raise-only peak with a majority-of-peak write gate; an isolated minority rejects writes (`NO_QUORUM`), proven by a live-partition integration test. SWIM has proper suspect/indirect-probe/backoff plus a trailing-edge debounce that stops a flapping node from thrashing the table. Migration enforces `MIGRATION_IN_PROGRESS` fencing + manifest required/mismatch (21/22) + a union-drop commit gate. HMAC-SHA256 is applied to SWIM and inter-node TCP; the d39a612 double-length-prefix fix is correct and has a RED-before-fix test. Replication: per-key ACK quorum → `REPLICATION_FAILED` under reject; `DEGRADED_DURABILITY` only at RF=1 (config rejects best-effort at RF>1); op-based payload confirmed. The **one** durability hole is REL-002 (persist not dir-fsynced). Error/feature and ReplicaOp matrices in COVERAGE.

### 3.6 Storage tiers & blobs — PASS
External-blob integrity is double-defended (store SHA-256 sidecar + record-anchored ExternalRef re-validation; swap-attack test). GC orphan TOCTOU closed by mtime grace + pin handshake with real race tests. Stream codes 16/17/18/34, byte cap, concurrent-stream cap, idle/close cleanup all e2e-tested. The 8 KiB inline threshold is advisory-client-side (README-consistent); the server doesn't second-guess. Minors only: a stale ">1 MiB" doc threshold (REL-114), an unwired `input_refs` module (REL-115), a missing STREAM_END size-mismatch test (REL-116).

### 3.7 Wire protocol & limits — PASS
Frame codec round-trips; rejects malformed/oversize → `PAYLOAD_MALFORMED`; all batch decoders count-before-alloc with per-item caps and checked-mul; fuzz-non-panic smoke test. `max_connections`, per-IP cap, `max_batch_size`, `max_inflight_request_bytes`→`RATE_LIMITED`, `max_migration_threads` are genuinely enforced (not just configured), at the point of allocation. One minor: sub-minimum frames (len 1-11) silently disconnect instead of returning `PAYLOAD_MALFORMED` (REL-119).

### 3.8 Index backends — PASS (1 major test-gap REL-017)
In-memory Robin Hood (64-byte bucket compile-asserted), redb (all 3 indexes, crash-durable via redo append+fsync-before-commit), and file-backed mmap all implemented for real. Primary device-scan rebuild is tolerant (skip+block-advance); secondary rebuild is fail-closed → `INDEX_DEGRADED`. Import-in-progress sentinel refuses startup. The gap: snapshot + export/import round-trip tests only insert all-zero cached fields and assert only `record_offset` (REL-017) — encode/decode are currently symmetric (verified by reading) so it's latent, but a regression on this fidelity-critical path would pass CI.

### 3.9 Clients — the weakest area (5 majors)
Both clients are well-structured (pipelining, per-node pools, bounded retry). But: Rust can't bootstrap a default `strict_auth` cluster (unsigned `GET_PARTITION_MAP`, no `cluster_secret` config) — REL-010; Rust `unspend_batch`/`get_spend_batch` don't shard-fan-out or follow redirects, silently mis-routing cross-shard reorg unspends — REL-011; Go doesn't follow the per-item `ERR_REDIRECT` the server actually emits for batch mutations — REL-012; Go redirect tests assert a whole-batch `StatusRedirect` the server never sends for batch ops, giving false confidence — REL-013. Plus REL-014 (both send 4-byte `ProcessExpired`). The Rust README also documents behavior the client doesn't have (REL-011, REL-133). Client wire-outcome matrix in COVERAGE.

---

## 4. Test & coverage review

See `COVERAGE.md`. Headline: a genuine end-to-end durability story exists — in-process WAL-boundary kill/restart sweep for all 12 ops (`crash_sweep_ops.rs`), 10-seed deterministic crash injection (`e2e_workload.rs`), real-SIGKILL 3-node Docker suite with full consistency reconciliation + atomicity (`scenario_15`), cluster double-spend rejection (`scenario_03`), split-brain prevention (`scenario_14`). No `#[ignore]`, no `assert!(true)`, no bare `.is_ok()` asserts. Docker e2e is condition-polled, not sleep-papered.

The real test risks are about **gating and clients**, not test existence:
- **REL-015** — `scenario_14`/`scenario_15` (the cardinal cluster contract) are **weekly-only**; not a merge or release gate. A split-brain/double-spend regression can merge green.
- **REL-016** — the Go client has a real-server integration test that CI never runs (`//go:build integration`, tag not passed).
- **REL-013/REL-130/REL-131/REL-132** — weak tests (Go redirect false-confidence; cluster oracle tracks spent count not identity; metric tests check name presence not increment; loop-device test can no-op).

Two reviewer-raised test-gaps were **refuted** by adversarial verification and dropped: config entropy/sizing validators (REL-200) and Go `classifyRetry` (REL-201).

## 5. Performance review

See `PERF.md`. Measured on MemoryDevice (Apple Silicon, macOS) only — no NVMe/Linux on this host. The README's perf claims are **already hedged** (10M+ = MemoryDevice ceiling, no fsync), and the code is consistent with the hedged claim: single-core in-memory hot ops are high-single to low-double-digit Melem/s (spend 8.4M, create 10.3M, index lookup 24-162M). Replication payload confirmed op-based from code. The "41-byte in-place spend" wording is inaccurate (full slot+header rewritten — REL-101). NVMe throughput, redb throughput, SSD wear, replication bandwidth, and sustained-load p99.9 tail are **`unverified-on-this-host`** and must be measured on Linux+NVMe before the README's quantitative table can be called validated.

## 6. Documentation review

The **README is accurate** — HTTP routes/gating, metric names, config defaults, error/opcode tables, slot/metadata/bucket sizes, and phase Status headers all match code. Drift is concentrated in **secondary docs** not updated after the CRC/format change, the F-X-002 debug-endpoint gating, the `strict_auth`-default flip, and the io_uring removal:
- **REL-019 (major)** — `DEPLOYMENT_ASSUMPTIONS.md` says `strict_auth` defaults false; it defaults true (security model inverted).
- **REL-018 (major)** — the on-by-default tombstone config subsystem (9 keys) is absent from the README config reference.
- Minors REL-124/125/126/136-144 — stale sizes (COMPARISON/PERFORMANCE/HARDWARE), wrong TUNING_GUIDE defaults (32× off on `migration_pool_size`, wrong listen addrs), io_uring described as live (+ `io-uring` still a declared dep, REL-137), README test count `2234` vs measured `2710` (REL-143), undocumented config keys, CLI token-doc errors.

Docker/Compose files referenced by the README exist (verified). The wire opcode/error/status tables were read but not exhaustively diffed against the enums (no contradiction spotted) — covered instead by §3.2/§3.7.

## 7. Security & safety review

Robust and v1-ready. Every non-test `unsafe` carries a documented, upheld contract: direct-I/O/mmap/pointer paths derive offsets from the trusted allocator/index, bounds-check before arithmetic, and validate attacker-controlled `vout` against `utxo_count` before any direct slot access. Resource limits (frame size, inflight-bytes, per-IP + global conn caps, stream count + idle reaper) are enforced at allocation time with deadline-bounded reads (slow-loris-resistant). Admin endpoints 404 when disabled, require a constant-time-compared bearer token (SHA-256 first → length-independent), and enforce a token-length floor on remote binds. Cluster auth is HMAC-SHA256 with timestamp freshness + constant-time verify; `strict_auth` defaults true. Minors only: REL-127/128 (fail-open path under `strict_auth=false` not documented as a risk / stale per-IP-exemption rationale), REL-129 (`from_bytes` length guard is `debug_assert`-only). No `unsafe`-soundness blocker; REL-001 is a *missing-lock* data race, not an OOB.

## 8. Deferred items

- NVMe/Linux perf measurement (throughput, SSD wear, replication bandwidth, p99.9 tail, RSS-vs-records) — host-limited; deferred to a Linux+NVMe run. Not a code blocker, but the README's quantitative table stays "unvalidated" until done.
- Raw `/dev/nvme` `BLKGETSIZE64` test (README already lists as a known residual; needs a root loop-device CI job).
- Deep `cargo-fuzz` nightly (README residual; seeded smoke fuzz runs every CI).

## 9. Verification record

- **Built:** `cargo build --release` (clean, 0 warnings). **Lint:** `cargo clippy --all --all-targets` (clean) — **not** re-run with `--features fault-injection` this session (README claims clean there too; residual). **Tested:** `cargo test --all` clean isolated run, real cargo exit 0 → **2710 passed / 0 failed / 0 ignored** across 70 suites (settles README `2234` and the contended `2514` as stale/partial). **Benched:** spend/create/read/index/mixed/codec/allocator on MemoryDevice, isolated (PERF.md).
- **Independently re-verified by the gate owner (own file reads):** REL-001 (`engine.rs:1490-1548`), REL-002 (`coordinator.rs:7454-7522`), REL-014 (`client/go/codec.go:331-333`, `client/rust/src/lib.rs:1751`, `dispatch.rs:8623-8677`), Docker files existence, version-bump propagation (`CARGO_PKG_VERSION` sites).
- **Could not verify on this host:** anything requiring Linux + NVMe + `O_DIRECT` (see §5/§8); the `--features fault-injection` clippy pass; live RSS measurement; the embedded `/ui/` dashboard behavior (route registration confirmed only).
- **Method limits:** subsystem findings come from agent reviewers; every blocker/major passed 2 adversarial skeptics, and the two escalated blockers + the prior review's blocker were re-read by hand. Minors are reviewer-asserted with `path:line` but not all hand-re-verified.
