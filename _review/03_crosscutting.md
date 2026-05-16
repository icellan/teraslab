# Phase 3 — Cross-cutting concerns

Themes that span multiple modules and would not be caught by per-file review alone. Each uses the standard finding template; `Location` lists the representative sites pulled from Phase 2.

---

### F-X-001: Inter-node TCP authentication is fail-open by default

- **Severity**: CRITICAL
- **Category**: Security
- **Location**: `src/server/mod.rs:422-425` (auth gate condition) + `src/server/dispatch.rs` (opcode list) — manifests in: `src/cluster/topology.rs`, `src/cluster/migration.rs`, `src/cluster/coordinator.rs`, `src/replication/manager.rs`, `src/cluster/routing.rs`
- **Code**:
  ```rust
  // src/server/mod.rs:422-425 (paraphrased — see G5-001, G7-001, G8-008)
  if is_inter_node_auth_opcode(op) && opts.cluster_secret.is_some() {
      verify_hmac(frame, &opts.cluster_secret.unwrap())?;
  }
  // else: no verification
  ```
- **Issue**: The HMAC verifier only runs when `opts.cluster_secret.is_some()`. `ConnectionOptions::default()` has `cluster_secret: None`. A node started without `--cluster-secret` (and the default config does not require it) silently accepts unsigned `OP_TOPOLOGY_PROPOSE/VOTE/COMMIT`, `OP_REPLICA_BATCH`, `OP_MIGRATION_COMPLETE`, `OP_MIGRATION_BATCH_COMPLETE`, and `OP_PARTITION_MAP` from any TCP peer reachable on the data port. Independently confirmed in G5 (`F-G5-001`, `F-G5-004`), G7 (`F-G7-001`), and G8 (`F-G8-008`).
- **Impact**: Anyone reachable on the data port can forge a topology commit, lift a migration fence, replicate fake operations, or steer the partition map — without TLS. Prior audit's `EF-01` / `D-20` anchors are still live in default deployments.
- **Recommendation**: Make `cluster_secret` mandatory whenever cluster membership has >1 node; reject startup if absent. Alternatively, flip the gate from "auth if secret configured" to "auth always; if absent, reject inter-node opcodes outright." Separately require TLS or a private overlay for the data port.
- **Confidence**: High

---

### F-X-002: Silent error swallowing pattern is endemic across persistence and replay paths

- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/device_io/sync_fallback.rs:89-113` (errno lost) · `src/index/redb_primary.rs:185-222` (unregister) · `src/index/redb_primary.rs:lookup` · `src/index/redb_dah.rs:clear/range_query` · `src/index/redb_unmined.rs:clear/range_query` · `src/recovery.rs` (replay continues past fatal) · `src/replication/receiver.rs` (`apply_op` Spend skip on tx-not-found) · `src/replication/manager.rs` (`AckTracker::flush_locked`) · `src/storage/manager.rs:136-145` (`read_cold_data` returns empty for missing blob) · `src/storage/blobstore.rs:walk_dir` (recursion errors swallowed) · `src/allocator.rs` (`replay_free` partial-overlap rejection silent)
- **Code**:
  ```rust
  // representative pattern (G3-002): RedbDahIndex::clear
  let _ = txn.commit();
  self.count = 0;
  ```
- **Issue**: Eleven distinct sites across nine modules silently absorb errors that previously triggered audit findings: the underlying I/O / commit / decode failure is logged at most (often not at all), the caller proceeds on a happy path, and corruption is encoded into in-memory state. Some sites diverge from their batch sibling: `unregister` swallows, `unregister_batch` propagates. The reviewer for G2 confirmed the engine's `write_slot_fast` write-error swallow (prior `A-01`) is now resolved — but the pattern lives elsewhere.
- **Impact**: A redb commit that fails during pruning leaves the disk entry intact while the in-memory count says zero (G3-002). A blob deletion that wins the race against a `create` returns "no cold data" (G9-001) and the spend path will then compute over zero bytes. A replay that hits an I/O error during recovery continues past it (G4-007) — partial recovery becomes silently committed state. None of these surface to a metric or to an alert pipeline.
- **Recommendation**: Treat every `let _ = ...`, `if let Err(e) = ... { tracing::warn!(...) }` over a persistence-class call as a CRITICAL discipline violation. Audit and convert each site to propagate the error or — if the design demands a soft failure — emit a distinct counter (`teraslab_silent_error_total{site=...}`) and a structured `WARN` log with the path that triggered. Make the prior A-01-style spend-write injection test mandatory.
- **Confidence**: High

---

### F-X-003: Length-prefixed wire allocations let an attacker pin server memory before any work is rejected

- **Severity**: HIGH
- **Category**: Security
- **Location**: `src/protocol/codec.rs` (`TopologyTerm::deserialize`, multiple `decode_*` paths) · `src/recovery.rs` (`CreateV2` decode `Vec::with_capacity(parents_count)`) · `src/replication/receiver.rs` (16 MiB pre-auth read buffer) · `src/cluster/routing.rs` (decode does not bound `node_count` / `cm_count`) · `src/cluster/swim.rs` (`ping_req_forwarding` map unbounded under flood) · `src/storage/uploader.rs` (unbounded submit queue) · `src/server/http.rs:handle_set_log_level` (no body size cap)
- **Code**:
  ```rust
  // representative (G5-002): TopologyTerm::deserialize
  let count = u32::from_le_bytes(...) as usize;
  let mut voters = Vec::with_capacity(count); // count is wire-controlled
  ```
- **Issue**: Multiple decode paths allocate based on a wire-controlled `u32` before bounding it against the remaining payload. Two of these sit on opcodes that may be processed pre-auth (F-X-001), so the auth gate is not even a backstop.
- **Impact**: A single 16-byte malicious frame can pin the server's resident set, and a small flood can OOM it. SWIM `ping_req` flood plays a similar role at the UDP layer (G8-004).
- **Recommendation**: Wrap every `Vec::with_capacity(n)` against attacker-controlled `n` in a `n.min(remaining_payload / per_item_min)` guard, and emit a per-opcode `decode_count_capped_total` counter so the bound is visible. Per `MAX_DECODE_BATCH` exists (G5-019); enforce it on the auth-required paths too.
- **Confidence**: High

---

### F-X-004: HTTP admin surface authentication is partial — read-only fan-out endpoints leak cluster-wide state without auth

- **Severity**: HIGH
- **Category**: Security
- **Location**: `src/server/http.rs` (`/admin/top`, `/ws/top`, `/metrics`, `/health/ready`, several `/admin/*` reads) — G6-002, G6-003, G6-014, G6-001
- **Code**:
  ```rust
  // public router (G6-002) — no admin-auth middleware applied
  Router::new()
      .route("/admin/top", get(handle_admin_top))
      .route("/ws/top", get(handle_ws_top))
      .route("/health/ready", get(handle_health_ready))
      .route("/metrics", get(handle_metrics))
  ```
- **Issue**: The R-056 bearer-token middleware is only applied to the mutating sub-router. `/admin/top` performs cluster-wide fan-out over plain HTTP, leaking allocator / redo / replication state, and amplifies DoS 32x. `/ws/top` runs a per-second snapshot loop with no auth. `/health/ready` reads a hard-coded boot-time `state.ready=true` and never reflects reality (G6-001). `extract_bearer_token` does not length-equalise inputs before its constant-time comparison (G6-004).
- **Impact**: Two attack surfaces. (a) Reconnaissance: anyone reachable on the observability port learns the full cluster shape, lag, and migration status. (b) Liveness: `/health/ready` always says "ready" — load-balancer probes cannot detect a degraded secondary or a recovery-in-progress node.
- **Recommendation**: Gate `/admin/top` and `/ws/top` behind the same bearer middleware that protects writes. Wire `/health/ready` to actually consult `secondary_status`, `recovery_completed`, and clustered-quorum state. Document explicitly that the observability port must be bound to a private interface, and add a startup assertion when it is bound to a public one without a token. Equalise input length before the constant-time compare (G6-004).
- **Confidence**: High

---

### F-X-005: Sequencing/cursor state is not robust to compaction or partial failure

- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/redo.rs` (`next_sequence` after `compact_prefix_through`-to-empty) · `src/replication/manager.rs` (`replicate_batch` advances `next_sequence` even on full failure) · `src/replication/receiver.rs` (`recv_ack` ignores `request_id`; per-thread tracker leaks) · `src/metrics.rs` / replication readiness scan (`Relaxed` load on `last_acked_seq`)
- **Code**:
  ```rust
  // G4-001 (paraphrased): scan_log on an empty file returns the default
  let next_sequence = scan_log().last_seq.map_or(1, |s| s + 1);
  ```
- **Issue**: The redo log's normal compaction path leaves the on-disk log empty; the high-water sequence number is not persisted out-of-band; a process restart re-opens the empty log and reseeds `next_sequence = 1`. Replicas (and the local intent tracker) cache "highest seen sequence" and will silently discard the re-issued numbers as already-ACKed. The replication manager advances `next_sequence` even when the replica returned an error (G7-007). `recv_ack` does not check that the ACK matches the outgoing `request_id` (G7-002) — fragile if any future transport layer reuses frames.
- **Impact**: Silent drop of post-restart mutations on replicas. Master believes everything is replicated; replicas have the data missing for the affected sequence range. Auditable only by full cross-node digest.
- **Recommendation**: Persist `next_sequence` alongside the redo-log header on every checkpoint advance and re-derive on open as `max(scan_tail, persisted_next_sequence)`. Make `replicate_batch` advance the cursor only on per-replica success; the `replicate_batch` worker that fails should re-queue from the previous cursor. Validate `request_id` on ACK frames.
- **Confidence**: High

---

### F-X-006: Crash-window between durable redo append and engine apply remains across the data plane

- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/replication/receiver.rs` (R-034/R-035 redo write happens AFTER engine apply — G7-016) · `src/cluster/migration.rs` (migration source releases fence on TCP-ACK; no two-phase commit — G8-012, G8-017) · `src/redo.rs` (`flush()` RMW of trailing aligned block — G4-004; concurrent appenders share buffer — G4-002)
- **Code**:
  ```rust
  // G7-016 (paraphrased)
  engine.apply_op(...)?;  // disk mutated
  durable_log.append(redo_entry)?;  // disk redo
  ack_to_master(...);
  ```
- **Issue**: R-034/R-035 added "replica writes local redo log" — but the order of `engine.apply_op` and `durable_log.append` puts the redo write AFTER the data mutation. If the engine path doesn't fsync per-op, a power loss after apply but before append leaves a state on disk that the redo log cannot reconstruct (no recovery point references it). Migration source releases its outbound fence on TCP-ACK, before the target has fsynced (G8-012, G8-017). The redo `flush()` reads-modifies-writes the trailing aligned block on every flush, doubling I/O and creating a torn-write window (G4-004). Concurrent appenders share a buffer — a failed flush leaves another thread's entries in place and a subsequent successful flush persists ops the originating client was told failed (G4-002).
- **Impact**: On a crash, replicas can diverge from the master in two directions: data present but no redo entry (recovery cannot validate it; CRC matches but provenance is missing), or redo entry present but the operation was reported as failed (recovery applies it on next boot, contradicting the client). Migration: source declares the shard handed off, target has not durably ingested it.
- **Recommendation**: Apply-before-append is the wrong order on the replica path: `durable_log.append(redo_entry).await?` then `engine.apply_op(...)` then ACK. Switch migration ACK to two-phase commit: target reports `received_durable` after fsync; source releases fence only after that. Replace RMW flush with append-only at aligned offsets, padding to alignment in-memory before write.
- **Confidence**: High

---

### F-X-007: Hot read paths violate the stripe-lock contract documented in `src/io.rs`; the violation is now codified rather than fixed

- **Severity**: MEDIUM
- **Category**: Concurrency
- **Location**: `src/io.rs:206` (the documented contract) · `src/ops/engine.rs` (`read_metadata`, `read_slot`, `lookup_cached`, `read_slots`, `read_block_entry`) — G2-010 · `src/device.rs` (`MemoryDevice` aliasing UB — G1-004) · `src/io.rs` (`read_metadata_direct` Acquire fence does not establish happens-before per Rust memory model — G1-003)
- **Issue**: The unsafe-API contract at `src/io.rs:206` says "caller holds the per-tx stripe lock" for the `*_direct` read helpers. The engine's hot read paths do not take that lock. Prior audit `BC-02` flagged this. The current code has "documented it away" — the read methods carry comments saying the contract is intentionally not held; CRC catches the visible torn read. Independently, the Acquire fence cited as a substitute does not establish happens-before in Rust's memory model. `MemoryDevice` aliases a `RwLock<Vec<u8>>` and a `raw_ptr` to the same allocation (G1-004) — concurrent use is UB under stacked borrows.
- **Impact**: `cargo miri` will flag these calls; some real CPU/compiler combinations (LTO, future LLVM versions, AArch64 weak-memory) could reorder a half-written 320-byte metadata such that CRC matches but a field has stale bytes. The `MemoryDevice` UB is only exercised in tests, which dampens the production blast radius, but it makes proptest results uninterpretable when failures arise.
- **Recommendation**: Either (a) take the per-tx stripe lock in read mode on the engine read paths (the safe answer), or (b) move the read helpers off the `unsafe { *_direct }` API onto an explicit `read_through_device(..) -> Result<_, _>` API that does the alignment dance behind a `RwLock`-style guarantee. Replace the `MemoryDevice` aliasing with a single `RwLock<Vec<u8>>` and remove `raw_ptr`. Strip the fence-as-substitute comment; either it's a real ordering or it's removed.
- **Confidence**: High

---

### F-X-008: Process lifecycle: SIGTERM/SIGINT is a no-op; graceful-shutdown cleanup is dead code

- **Severity**: CRITICAL
- **Category**: Correctness
- **Location**: `src/bin/server.rs:ctrlc_handler` (G10-001) · `src/bin/server.rs` `shutdown_flag` vs `Server` internal shutdown atomic (G10-002) · `src/bin/server.rs` (G10-003 — no redo-log fsync on shutdown) · `src/bin/server.rs` (G10-022 — leaked join handles for blob GC, lag monitor, checkpoint, redo log device)
- **Issue**: The Ctrl-C handler drops its closure; the `ctrlc` / `signal-hook` dependency was never added. The binary's `shutdown_flag` is a separate `Arc<AtomicBool>` from the one inside `Server`. The entire `ServerWithShutdown::run` cleanup path (index snapshot, allocator persist, replication-intent flush, device.sync, OTLP flush) is unreachable in production: the daemon is always hard-killed.
- **Impact**: Every shutdown is a crash. Recovery runs on every boot. Last-second writes that depend on the in-memory checkpoint mark / allocator persist / replication intent flush are lost or replayed. OTLP spans for the final few seconds are dropped. Compounds F-X-006: the apply-before-append crash window is hit on every restart.
- **Recommendation**: Add a real signal handler (`tokio::signal::ctrl_c` + `unix::signal::SIGTERM`), have it set the `Server`'s internal shutdown atomic (not a sibling), then `await` the existing cleanup path. Add a startup assertion that the cleanup runs on a SIGTERM during integration test.
- **Confidence**: High

---

### F-X-009: Cluster control plane lacks both replay protection and a documented lock-order

- **Severity**: HIGH
- **Category**: Security / Concurrency
- **Location**: `src/cluster/auth.rs` (no nonce; HMAC over a 5-minute clock-skew window — G8-003) · `src/cluster/auth.rs` (`SHA-256` hand-coded — G8-020) · `src/cluster/coordinator.rs::quiesce()` (commits a topology without quorum — G8-007) · `src/cluster/topology.rs` (split-brain pure-superset merge — G8-001; follower doesn't re-validate — G8-002) · `src/cluster/swim.rs` (lock-order `membership → peer_addrs → swim_peer_addrs` vs topology event loop's order — G8-018) · `src/cluster/coordinator.rs` (`MigrationManager::Mutex` held across full shard scan — G8-005)
- **Issue**: Signed SWIM packets carry a 5-minute clock-skew window for replay tolerance — any captured packet may be replayed within that window. Auth uses hand-rolled SHA-256 with no audit. `quiesce()` fabricates a `TopologyCommit` with `voters = new_members` and broadcasts it, bypassing quorum. Two clusters whose membership becomes a strict superset of the other (e.g. shared `cluster_secret` after a snapshot clone — entirely plausible operational scenario) silently merge with no coordinated handoff (R-042 partial). The follower side does not re-validate split-brain criteria. No documented total lock order between membership / topology / migration; deadlock risk under heavy membership churn.
- **Impact**: Replay attacks on cluster control; quorum bypass under degraded state; silent UTXO divergence + double-spend potential on split-brain heal; deadlock risk amplifies tail latency in production.
- **Recommendation**: Add a nonce and sequence number to every signed cluster frame; track seen-nonces in a sliding window per peer. Stop using hand-rolled SHA-256 — use the `hmac` crate over `sha2::Sha256`. Make `quiesce()` go through the same quorum proposer as any other topology change. Tighten split-brain check to "no peer outside the original committed_members may be in the proposed set without a coordinated rejoin event." Document a single global lock order: `membership → topology → migration → shards → peer_addrs → swim_peer_addrs`. Add a `parking_lot::deadlock_detector` build-flag gated to CI.
- **Confidence**: High

---

### F-X-010: Test, CI, and dependency hygiene gaps

- **Severity**: MEDIUM
- **Category**: Maintainability
- **Location**: `.github/workflows/ci.yml` · `tarpaulin-report.json` (stale — March 2026) · `AUDIT.md` / `AUDIT_CODEX.md` (stale `rebuild_*` failing-tests claim, G3-009) · `src/redo.rs::advance_checkpoint` (dead code surviving the live compaction path, G4-003) · `fuzz/` (not in CI)
- **Issue**:
  - CI runs clippy + tests + benches (compile-only) + Docker E2E PR tier on Linux and macOS. It does NOT run: `cargo audit` (RUSTSEC advisories), `cargo deny` (license / supply-chain), `cargo miri` (the UB sites flagged in F-X-007 would surface here), fault-injection tests in the matrix (only on a single line), the fuzz corpus, or a coverage gate. `tarpaulin-report.json` exists at the repo root but is two months stale.
  - Per-feature-flag-combinatorial build is missing — `fault-injection` is the only feature, but no CI step verifies the non-feature build is identical to the feature build at zero-cost.
  - The two prior audit files at the repo root are referenced by the project as authoritative and contain stale claims (e.g. "3 failing rebuild_* tests"); the live tests have been split and now pass (G3-009). The audit files thus mislead future contributors. The doc-only `RedoLog::advance_checkpoint` survives the live `compact_prefix_through` path and looks production-relevant on first read (G4-003).
  - Test coverage gap: no end-to-end test that simulates `cluster_secret = None` rejecting an unsigned `OP_TOPOLOGY_PROPOSE`. The F-X-001 default fail-open behaviour is not gated by a test.
- **Impact**: Regressions in the un-gated areas (auth fail-open, sequence rollback, miri-detectable UB) ship to `main` unobserved. Stale audit docs add to operator confusion; new contributors waste time chasing already-fixed findings or trusting them as still-live.
- **Recommendation**: Add to CI: `cargo audit --deny warnings`, `cargo miri test -p teraslab --lib` (gated on a smaller test set if cost prohibitive), `cargo deny check`, a coverage gate via `cargo llvm-cov` with a floor of 80% on `src/`. Move `AUDIT.md` / `AUDIT_CODEX.md` under `_audits/historical/` with a banner saying they are point-in-time snapshots. Delete `RedoLog::advance_checkpoint` (it is dead — F-G4-003) and the dead `replay_cause_label` (F-G6-018). Add a regression test that boots a 3-node cluster with `cluster_secret = None` and asserts that `OP_TOPOLOGY_PROPOSE` from a peer is rejected.
- **Confidence**: High

---

### F-X-011: Validation discipline at entry surfaces is uneven

- **Severity**: MEDIUM
- **Category**: Code Quality / Security
- **Location**: `src/server/dispatch.rs` (admin opcodes not in auth list — G5-005; `OP_QUERY_OLD_UNMINED` no ownership check — G5-003; `OP_INCREMENT_SPENT_EXTRA_RECS` no-op shim — G5-015; error responses echo internal `CodecError` — G5-008) · `src/protocol/codec.rs` (`from_utf8_lossy` on redirect target — G5-021; per-item `data_len` only bound via remaining-payload — G5-018) · `src/config.rs` (no range validation for numeric fields — G10-005; `device_paths[0]` panics on empty — G10-004; `cluster_secret` strength unvalidated — G10-011) · `src/server/http.rs` (no body cap on `handle_set_log_level` — G6-006; embedded-file fallback masks `..` traversal — G6-007)
- **Issue**: Some opcodes validate fully (frame size, per-item size, count cap) and return structured errors; others assume the dispatcher has already validated; some return `format!("malformed {op_label}: {err}")` to the client with the inner `CodecError` Display; some accept wire payloads with `from_utf8_lossy` (replaces invalid bytes silently); config has no range validation; `serve_embedded_file` falls back to `index.html` for any missing asset including `..`-traversal probes (mask, not exploit, but still smells).
- **Impact**: Inconsistent validation creates a non-uniform attack surface: an attacker probes each opcode separately because their length / count behaviour is not derivable from a shared validator. Error responses leak internal type names and parser positions. Config files with negative / overflowing numeric values crash on startup rather than being rejected with a structured error.
- **Recommendation**: Move every wire-decode into a shared `validated_decode<T>` helper that enforces (a) max length, (b) max element count, (c) total byte budget, (d) UTF-8 strictness (`String::from_utf8` not `lossy`). For dispatch error responses, never include the raw `Display` of an internal error — map to a stable `ERR_*` code + a short safe diagnostic. Add `#[derive(Validate)]`-style range checks (via `validator` crate or hand-rolled) to `ServerConfig` and reject impossible values at parse time. For embedded-file fallback, return 404 on any path containing `..` or `\\`.
- **Confidence**: High

---

## Quick takeaways

1. **Default fail-open auth and silent error swallowing dominate the risk profile.** Three CRITICAL-class concerns (F-X-001, F-X-008) and four HIGHs (F-X-002, F-X-004, F-X-005, F-X-006) chain into a credible cluster-divergence scenario under operational stress.
2. **The codebase has good architectural discipline but uneven execution.** Clippy is zero-warning; CI exists; property/fault-injection tests exist; per-opcode caps were tightened recently. But the same patterns (`let _`, fail-open conditional auth, ad-hoc `Vec::with_capacity`) recur because they aren't enforced by a shared validator or a CI lint.
3. **Prior audit findings have largely been remediated.** A-01 (silent slot-write swallow), A-04 (unspend authority), R-080 (resize crash-atomicity), R-034/R-035 (replica WAL), R-048 (BlobDigest), R-049 (orphan blob GC), R-054 (slow loris) all verified resolved. Recent commits are doing real work. The remaining CRITICALs are concentrated in lifecycle (SIGTERM), sequencing (compaction → next_sequence), and `delete`-vs-create-vs-read race (G2-001).
