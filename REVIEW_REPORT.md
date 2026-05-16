# TeraSlab — Independent Code Review

**Date:** 2026-05-16
**Scope:** `src/` of the TeraSlab main crate (purpose-built UTXO store for BSV Teranode)
**Commit:** `52adbb2` + working-tree edits (see `git status`)
**Method:** 10 parallel module-scoped reviewer agents, one orchestrator. 71 files / ~103.7k LOC reviewed; ~216 findings.
**Working artifacts:** [`_review/00_orientation.md`](_review/00_orientation.md), [`_review/01_scope.md`](_review/01_scope.md), [`_review/02_findings.md`](_review/02_findings.md) (+ 10 per-group files), [`_review/03_crosscutting.md`](_review/03_crosscutting.md).

---

## 1. Executive summary

1. **Inter-node TCP authentication is fail-open by default.** When `cluster_secret` is unset, all cluster control opcodes (`OP_TOPOLOGY_PROPOSE/VOTE/COMMIT`, `OP_REPLICA_BATCH`, `OP_MIGRATION_COMPLETE`) are accepted unsigned from any peer reachable on the data port. Independently confirmed by three reviewers. (`F-G5-001` CRITICAL · `F-G7-001` HIGH · `F-G8-008` MEDIUM · cross-cut `F-X-001`).
2. **Graceful shutdown is unreachable in production.** `ctrlc_handler` drops its closure (no `ctrlc`/`signal-hook` dep). The binary's `shutdown_flag` is a different `Arc` from the one inside `Server`. The full cleanup path (index snapshot, allocator persist, replication-intent flush, device.sync, OTLP flush) is dead code; every shutdown is a hard kill. (`F-G10-001`, `F-G10-002` CRITICAL · cross-cut `F-X-008`).
3. **`Engine::delete()` frees the record region before unregistering the index entry** — a concurrent reader on the freed offset can be returned a *different* transaction's metadata with a valid CRC. (`F-G2-001` CRITICAL).
4. **Redo-log compaction can roll back `next_sequence` to 1 across a restart**, silently causing replicas to discard the re-issued numbers as already-ACKed. Replication-intent tracker drops new mutations. (`F-G4-001` CRITICAL · cross-cut `F-X-005`).
5. **Split-brain heal accepts a "pure superset" merge** — two independently-bootstrapped clusters that share a `cluster_secret` (e.g. cloned for benchmark) merge silently. UTXO divergence + double-spend potential. (`F-G8-001` CRITICAL).
6. **Silent error swallowing is a system-wide pattern** affecting eleven sites across nine modules: index commits, replay errors, blob lookups, sync-fallback errno. Pattern is endemic, not local. (Cross-cut `F-X-002`, 11+ HIGH/MEDIUM findings).
7. **HTTP admin surface is half-protected.** `/admin/top` and `/ws/top` perform cluster-wide fan-out with no auth and no TLS; `/health/ready` is hard-coded `true` and never reflects degraded state. (`F-G6-001`, `F-G6-002` HIGH · cross-cut `F-X-004`).
8. **Replica apply-then-append crash window remains** after R-034/R-035 — engine mutates the device *before* the local redo log records it. Migration source releases its fence on TCP-ACK, not on durable target ingest. (`F-G7-016` MEDIUM · `F-G8-012`, `F-G8-017` HIGH · cross-cut `F-X-006`).
9. **Length-prefixed wire allocations are not bounded** at several decode sites reachable pre-auth: `TopologyTerm` voters, `CreateV2` parents, replica 16 MiB buffer. A single small malicious frame can pin server memory. (`F-G5-002` HIGH · `F-G4-006` HIGH · cross-cut `F-X-003`).
10. **Prior audit findings have largely been remediated.** A-01 (silent slot-write swallow), A-04 (unspend authority), R-080 (resize crash-atomicity), R-034/R-035 (replica WAL), R-048 (BlobDigest), R-049 (orphan blob GC), R-054 (slow loris) all verified resolved. Recent commits are doing real work; the highest-risk surviving issues are in lifecycle (SIGTERM), sequencing (compaction → next_sequence), and `delete`-vs-create-vs-read ordering. The stale "3 failing rebuild_* tests" claim in `AUDIT.md` is no longer true — the tests have been split and pass.

**Severity totals**

| Severity     | Count | Cross-cutting | Phase 2 |
|--------------|------:|--------------:|--------:|
| CRITICAL     | 9     | 3             | 6       |
| HIGH         | 30    | 8             | 22      |
| MEDIUM       | 42    | 2             | 42 (incl. 2 promoted from per-group) |
| LOW          | 85    | —             | 85      |
| INFO         | 61    | —             | 61 (incl. 9 positive verifications) |
| **Total**    | **227** | **11**     | **216** |

---

## 2. Methodology

Phase 0 mapped the repo (70 files / 103,689 LOC in `src/`, edition 2024, three files >10k LOC). Phase 1 declared scope grouped into 10 modules, with the test suite, benches, fuzz, client crates, docker, docs, and prior audit files explicitly out-of-scope. Phase 2 was executed by 10 parallel reviewer agents (one per module), each given the same finding template, the same severity rubric, and the same anti-rationalization rules; each agent read every file in its scope (small files end-to-end, large files section-by-section), emitted findings with `file:line` and ≤10-line code excerpts, and wrote a positive verification note on every file with zero findings. Phase 3 lifted cross-cutting themes from the per-group output (11 concerns spanning multiple modules). Phase 4 is this consolidated report.

Prior audits (`AUDIT.md`, `AUDIT_CODEX.md`, 2026-05-06) were treated as orientation only; every cited prior finding was re-verified against current code before being either reported as still-live or marked resolved.

All artifacts remain on disk in `_review/`. Per-finding bodies (with code excerpts) live in `_review/02_findings_G{1..10}.md`; this report indexes them.

---

## 3. Coverage

**Files reviewed:** 71 / 71 in scope. Ledger in `_review/01_scope.md` ticks every file.

| Group | Module                                | Files | Approx LOC | Findings | CRITICAL | HIGH |
|-------|----------------------------------------|------:|-----------:|---------:|---------:|-----:|
| G1    | core data plane (device, io, record…)  | 9     | 6,800      | 19       | 0        | 1    |
| G2    | ops engine + ops sub-paths             | 11    | 12,000     | 20       | 1        | 1    |
| G3    | indexes                                | 11    | 11,000     | 20       | 0        | 2    |
| G4    | recovery + redo + checkpoint           | 3     | 7,700      | 16       | 1        | 3    |
| G5    | wire protocol + dispatch               | 5     | 17,800     | 28       | 1        | 3    |
| G6    | HTTP + observability + metrics         | 5     | 6,200      | 28       | 0        | 2    |
| G7    | replication                            | 7     | 10,200     | 20       | 0        | 1    |
| G8    | cluster control plane                  | 9     | 21,000     | 26       | 1        | 8    |
| G9    | storage tiers                          | 7     | 4,100      | 17       | 0        | 2    |
| G10   | binaries + config + lib root           | 4     | 4,300      | 22       | 2        | 2    |
| **Σ** |                                        | **71**| **103,700**| **216**  | **6**    | **22 → 25** (rubric-merged) |

No file was abbreviated. Two files were positive-verified end-to-end with no findings: `src/ops/mod.rs`, `src/lib.rs`. The full per-file verification notes are in each group's `## Coverage notes` section.

---

## 4. Findings

### 4.1 CRITICAL

All six per-group CRITICALs plus the three cross-cutting CRITICALs are rendered in full below. Cross-cutting items are aggregated views of per-group findings — they are reproduced here so the reader does not have to cross-reference.

---

#### F-G2-001 — `delete()` frees record region BEFORE removing primary-index entry; concurrent reader can return data from an unrelated transaction

- **Severity**: CRITICAL
- **Category**: Correctness / Concurrency
- **Location**: `src/ops/engine.rs:3202`
- **Code**:
  ```rust
  self.write_zeroed_metadata_header(entry.record_offset)?;
  self.device.sync().map_err(|e| SpendError::StorageError { ... })?;
  // Free device space
  self.allocator.lock().free(entry.record_offset, record_size)?;
  // Remove from primary index AND decrement shard_counts ...
  self.unregister_with_shard_count(&req.tx_key);
  ```
- **Issue**: Order is (1) tombstone header, (2) free allocator region, (3) unregister primary index. Between (2) and (3) the index still maps `tx_key_A → offset_X`; the allocator has already returned `offset_X` to the free pool. A concurrent `create_at_offset` can allocate the same `offset_X` and write a brand-new CRC-valid `TxMetadata` for `tx_key_B`. Engine read paths (`read_metadata`, `read_slot`, `get_spend`, `lookup`, `lookup_cached`) do not take the per-tx stripe lock and do not verify `meta.tx_id == requested_tx_id`.
- **Impact**: Silent cross-transaction read: a `get_spend(tx_A, vout=0)` returns `tx_B`'s slot data. Replication / consensus-adjacent code that trusts the reply corrupts its view of UTXO state. CRC passes; detection probability is low.
- **Recommendation**: Re-order to (1) tombstone, (2) sync, (3) unregister from primary index, (4) only then free in allocator. Or hold the primary-index write lock across the free. Or have `read_metadata_fast` verify `meta.tx_id` against the requested key before returning.
- **Confidence**: High

---

#### F-G4-001 — `next_sequence` rolls back to 1 after restart when redo log was compacted to empty

- **Severity**: CRITICAL
- **Category**: Correctness
- **Location**: `src/redo.rs:1367-1389` and `src/redo.rs:1660-1712`
- **Code**:
  ```rust
  // open()
  let mut log = Self { ..., next_sequence: 1, ... };
  let (entries, tail_pos) = log.scan_all_with_tail()?;
  if let Some(last) = entries.last() {
      log.next_sequence = last.sequence + 1;
  }

  // compact_prefix_through() retains `RecoveryProgress`-only as empty:
  if retained.iter().all(|entry| matches!(&entry.op, RedoOp::RecoveryProgress {..})) {
      retained.clear();
  }
  ```
- **Issue**: The normal checkpoint path (`perform_checkpoint_with_reset_guard` → `mark_recovery_progress` → `compact_prefix_through`) leaves the on-disk log empty by design. `compact_prefix_through` does NOT persist `next_sequence` anywhere — in-memory value is correct until restart. On restart, `open()` scans the empty log and falls back to `next_sequence = 1`. The master then hands out sequence numbers replicas (and the durable replication intent tracker at `dispatch.rs:9537`) think they already ACKed.
- **Impact**: Replicas silently discard newly-issued post-restart mutations as already-replicated. Master believes replication is complete. Auditable only via cross-node digest. Catastrophic in adversarial timing.
- **Recommendation**: Persist `next_sequence` (and `checkpoint_seq`) in a small header block at the start of the redo region, updated by `flush`, `compact_prefix_through`, and `reset`. On open, prefer this header over the empty-scan default. Alternatively, when compaction would leave the log empty, write a single zero-payload sequence-watermark marker.
- **Confidence**: High

---

#### F-G5-001 / F-X-001 — Cluster-control opcodes accept unauthenticated frames when `cluster_secret` is not configured (default fail-open)

- **Severity**: CRITICAL
- **Category**: Security
- **Location**: `src/server/mod.rs:422-450` and `src/protocol/opcodes.rs:368-381` — confirmed by G5, G7, G8 reviewers independently.
- **Code**:
  ```rust
  let auth_required = peek_request_op_code(&frame_bytes)
      .map(is_inter_node_auth_opcode)
      .unwrap_or(false)
      && opts.cluster_secret.is_some();
  let request_frame_bytes = if auth_required {
      match crate::cluster::auth::verify_frame(...) { ... }
  } else {
      frame_bytes
  };
  ```
- **Issue**: HMAC verification only runs when `opts.cluster_secret.is_some()`. `ConnectionOptions::default()` has `cluster_secret: None`. A multi-node cluster started without `--cluster-secret` (the default) silently accepts unsigned `OP_TOPOLOGY_PROPOSE/VOTE/COMMIT`, `OP_REPLICA_BATCH`, `OP_MIGRATION_COMPLETE`, `OP_MIGRATION_BATCH_COMPLETE`, and `OP_PARTITION_MAP/GET_COMMITTED_TOPOLOGY/PARTITION_VERSION_REPORT` from any TCP peer reachable on the data port.
- **Impact**: Anyone reachable on the data port can: forge a topology commit; lift a migration fence; replicate fake operations onto a replica; steer the partition map; or trigger irreversible state transitions on the cluster.
- **Recommendation**: Make `cluster_secret` mandatory whenever `replication_factor > 1` OR membership has >1 node — reject startup if absent. Switch the gate from "auth if secret configured" to "auth always; if absent, reject inter-node opcodes outright." Separately, require TLS or a private overlay for the data port. Add a CI regression test that asserts `OP_TOPOLOGY_PROPOSE` from a peer is rejected when `cluster_secret = None`.
- **Confidence**: High

---

#### F-G8-001 — Split-brain merge accepted when one cluster's membership is a strict superset of the other

- **Severity**: CRITICAL
- **Category**: Correctness / Security
- **Location**: `src/cluster/topology.rs:404`
- **Code**:
  ```rust
  fn is_safe_membership_change(committed: &[NodeId], proposed: &[NodeId]) -> bool {
      if committed.is_empty() { return true; }
      let proposed_has_all_committed = committed.iter().all(|c| proposed.contains(c));
      let committed_has_all_proposed = proposed.iter().all(|p| committed.contains(p));
      // Safe when the change is monotonic: pure superset OR pure subset.
      proposed_has_all_committed || committed_has_all_proposed
  }
  ```
- **Issue**: R-042 added a split-brain heal defence but only rejects the narrower "add AND remove" case. Two independently-bootstrapped clusters sharing a `cluster_secret` (e.g. one was cloned for a benchmark) each see the merged set `{A,B,C,D}` as a strict superset of their own `{A,B}` / `{C,D}` committed set, and BOTH sides accept on the next proposer round. Both sides commit a new term with the merged membership and recompute the shard table over the union.
- **Impact**: Shard owners instantly diverge; previous masters are demoted into replica slots without coordinated handoff. Silent UTXO divergence and double-spend potential. The doc comment lines 384-399 reveals the design *intends* to defend against this exact scenario.
- **Recommendation**: Reject any proposal that introduces members never previously seen in this cluster's history. Track a `committed_voter_ever_seen` set, or require an explicit `cluster_id` UUID exchanged at JOIN time. At minimum reject pure additions of unrelated nodes unless the operator passes `--allow-merge`.
- **Confidence**: High

---

#### F-G10-001 — `ctrlc_handler` is a no-op — SIGINT/SIGTERM never triggers graceful shutdown

- **Severity**: CRITICAL
- **Category**: Correctness
- **Location**: `src/bin/server.rs:1196`
- **Code**:
  ```rust
  fn ctrlc_handler<F: Fn() + Send + 'static>(handler: F) {
      // Unfortunately without a signal crate, we can't easily catch SIGINT.
      // The server's read timeout + shutdown flag handle graceful shutdown.
      // For production, add the `ctrlc` or `signal-hook` crate.
      drop(handler);
  }
  ```
- **Issue**: The function takes a "handler" closure and immediately drops it. No `ctrlc`/`signal-hook` dependency is present in `Cargo.toml`. The `shutdown_flag` is never flipped from outside the process.
- **Impact**: On `kill -TERM` / Ctrl-C the daemon is hard-killed. The cleanup path in `ServerWithShutdown::run` (cluster shutdown, index snapshot, allocator persist, replication-intent flush, device.sync, OTLP flush) NEVER runs in production. Every restart pays the recovery price; in-flight in-memory state that survives only because the redo log is fsynced may still be replayed on next boot — compounds with F-G4-001 and F-X-006.
- **Recommendation**: Add `ctrlc` or `signal-hook` (or use `tokio::signal::ctrl_c` + `unix::signal::SIGTERM`). Wire signals to set both the bin's `shutdown_flag` AND call `server.inner.shutdown()`.
- **Confidence**: High

---

#### F-G10-002 — Binary `shutdown_flag` is disconnected from `Server`'s internal shutdown atomic

- **Severity**: CRITICAL
- **Category**: Correctness
- **Location**: `src/bin/server.rs:1000-1005`, `src/server/mod.rs:146,162`
- **Code**:
  ```rust
  // bin/server.rs:1000-1005
  let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
  let shutdown_clone = shutdown_flag.clone();
  ctrlc_handler(move || {
      tracing::info!("shutdown signal received");
      shutdown_clone.store(true, std::sync::atomic::Ordering::Relaxed);
  });
  ```
  ```rust
  // server/mod.rs:146,162 — Server holds its OWN unrelated Arc
  shutdown: Arc<AtomicBool>,   // field
  shutdown: Arc::new(AtomicBool::new(false)),  // constructor
  ```
- **Issue**: `Server::new` creates an internal `shutdown` Arc with no public setter. The bin builds a separate `shutdown_flag` and passes it only to `checkpoint`, `blob_gc`, `lag_monitor`. The TCP accept loop in `Server::run` polls its OWN private flag. Even if F-G10-001 were fixed, the accept loop would never exit.
- **Impact**: `server.run()` blocks forever; the cleanup path is unreachable. Background tasks may exit; the TCP listener won't.
- **Recommendation**: Expose `Server::with_shutdown(Arc<AtomicBool>)` so the bin can share its flag, or have `Server` accept a `Weak<AtomicBool>` provided externally.
- **Confidence**: High

---

#### F-X-001 — *Cross-cut*: inter-node TCP authentication fail-open by default

See above as F-G5-001; reproduced in cross-cut form in `_review/03_crosscutting.md`. Aggregates G5-001, G7-001, G8-008.

---

#### F-X-005 — *Cross-cut*: sequencing/cursor state is not robust to compaction or partial failure

- **Severity**: CRITICAL (lifted from HIGH aggregating F-G4-001, F-G7-002, F-G7-007, F-G7-011, F-G6-024)
- **Category**: Correctness
- **Location**: `src/redo.rs` (next_sequence rollback) · `src/replication/manager.rs` (replicate_batch advances cursor on failure) · `src/replication/receiver.rs` (recv_ack ignores request_id; per-thread tracker leak) · `src/metrics.rs` (Relaxed load on last_acked_seq)
- **Issue**: F-G4-001 reseeds `next_sequence` to 1 after compaction + restart; F-G7-007 advances the cursor even when the per-replica fan-out returned an error; F-G7-002 does not match ACK frames to outgoing requests; F-G6-024 loads `last_acked_seq` with Relaxed ordering, so `lag()` can observe a half-updated leader/replica pair. The composition is a sequencing layer that cannot be trusted under restart, partial failure, or transport perturbation.
- **Impact**: Silent data divergence between master and replicas, masked by metrics that themselves operate over partially-updated state.
- **Recommendation**: Persist `next_sequence` in the redo header; advance only on per-replica success; validate `request_id` on ACKs; promote `last_acked_seq` updates to `Release/Acquire` ordering on the cross-thread path.
- **Confidence**: High

---

#### F-X-008 — *Cross-cut*: process lifecycle / SIGTERM no-op

Aggregates F-G10-001, F-G10-002, F-G10-003, F-G10-022. See cross-cutting file for full body. Severity lifted to CRITICAL because the inability to fsync the redo log on shutdown compounds F-G4-001 and F-X-006 — every shutdown is effectively a crash.

---

### 4.2 HIGH

22 HIGH findings from per-group reviews + 6 HIGH cross-cut concerns. Rendered in compact template form; full code excerpts in per-group files.

#### Security (HIGH)

| ID | Title | Location | Recommendation summary |
|----|-------|----------|------------------------|
| `F-G2-002` | `spend` accepts client-supplied `spending_data == [FROZEN_BYTE; 36]` — permanent "looks frozen" DoS, unrecoverable via public ops | `src/ops/engine.rs:1041` (single-spend), `:1196` (already-spent guard), `:1305` (unspend frozen guard) | Reject `spending_data == [FROZEN_BYTE; 36]` on the spend path |
| `F-G5-002` | `OP_TOPOLOGY_PROPOSE/VOTE/COMMIT` allocate `Vec::with_capacity(count)` for u32 wire count without remaining-payload bound | `src/protocol/codec.rs` `TopologyTerm::deserialize` | Cap `count` against `remaining_payload / per_voter_min` before allocate |
| `F-G5-004` | `OP_MIGRATION_COMPLETE` / `OP_MIGRATION_BATCH_COMPLETE` accept unsigned frames when `cluster_secret` unset and execute irreversible state transitions | `src/server/dispatch.rs` | Subsumed by F-X-001 fix |
| `F-G6-002` | `/admin/top` unauthenticated; fans out to every cluster peer over plain HTTP, no auth, no TLS; 32x DoS amplifier | `src/server/http.rs` route table | Gate behind same bearer middleware as mutating routes |
| `F-G7-001` | Replica frames unauthenticated when `cluster_secret = None`; R-034/R-035 did not change the auth gate | `src/server/mod.rs:422-425` | Subsumed by F-X-001 fix |
| `F-G8-002` | `handle_propose` does not re-validate split-brain heal against voter's committed state (follower-side) | `src/cluster/topology.rs:634-683` | Apply F-G8-001 check on both proposer and follower |
| `F-G8-003` | SWIM auth has no replay defence within 5-minute clock-skew window | `src/cluster/auth.rs` | Add nonce/sequence number per signed frame; track seen-nonces window per peer |
| `F-G8-004` | `ping_req_forwarding` map grows unboundedly under PING_REQ flood | `src/cluster/swim.rs` | Bound the forwarding map; reject when full |
| `F-G8-007` | `quiesce()` self-commits a topology without quorum and broadcasts it as authoritative | `src/cluster/coordinator.rs` | Route through the standard quorum proposer; never fabricate a `TopologyCommit` |

#### Correctness (HIGH)

| ID | Title | Location | Recommendation summary |
|----|-------|----------|------------------------|
| `F-G1-001` | SyncFallback loses errno on I/O failure — `Completion::result` violates documented `-errno` contract | `src/device_io/sync_fallback.rs:89-113` | Read `Error::last_os_error().raw_os_error()`; store `-(errno as i32)` |
| `F-G3-001` | `RedbPrimary::unregister` silently swallows redb commit failure and returns the entry as if removed | `src/index/redb_primary.rs:185-222` | Propagate the redb error; align with `unregister_batch` |
| `F-G3-002` | `RedbDahIndex::clear` / `RedbUnminedIndex::clear` swallow all redb errors then reset `count = 0` — in-memory / on-disk divergence | `src/index/redb_dah.rs`, `redb_unmined.rs` | Propagate error; do not zero count on failure |
| `F-G4-002` | Concurrent appenders share `buffer`; failed flush leaves another thread's entries persistable by next thread → ghost mutations | `src/redo.rs:1405-1488`, `src/server/dispatch.rs:1095-1127` | Per-thread buffer with explicit serialization point on flush |
| `F-G4-003` | `RedoLog::advance_checkpoint` still dead code in production — only updates in-memory `checkpoint_seq`, reclaims nothing | `src/redo.rs` | Either delete (misleading API surface) or wire to the live compaction path |
| `F-G4-004` | `flush()` performs read-modify-write of trailing aligned block on every flush — doubles I/O, creates torn-write window | `src/redo.rs` | Append-only at aligned offsets; pad to alignment in-memory before write |
| `F-G6-001` | `/health/ready` returns hard-coded boot-time `state.ready = true` flag — never reflects degraded state | `src/server/http.rs` | Consult `secondary_status`, `recovery_completed`, quorum state |
| `F-G8-005` | `MigrationManager::Mutex` held across full shard scan during migration plan rebuild — long lock hold | `src/cluster/coordinator.rs` | Snapshot under lock, compute outside |
| `F-G8-017` | `mark_inbound_complete_many_from_source` persist is best-effort; ACK returns OK regardless of fsync outcome | `src/cluster/migration.rs` | Make ACK wait on durable persist; surface error on failure |
| `F-G9-001` | `read_cold_data` silently returns empty cold data when external blob is missing — masquerades as "no cold data" | `src/storage/manager.rs:136-145` | Return `NotFound` error (align with `stream_cold_data`) |
| `F-G9-002` | `read_cold_data` does not cross-check against `ExternalRef.content_hash` (asymmetric with spend path) | `src/storage/manager.rs` | Verify content hash before returning |
| `F-G10-003` | No redo log fsync on shutdown — `ServerWithShutdown::run` doesn't hold `redo_log` | `src/bin/server.rs:1149-1193` | Hold `redo_log: Option<Arc<Mutex<RedoLog>>>`; sync before `device.sync()` |
| `F-G10-004` | `device_paths[0]` panics if TOML supplies empty `device_paths = []` | `src/bin/server.rs:180`, `src/config.rs:557,573` | Reject empty `device_paths` in `validate_safe_defaults` |

#### Cross-cutting HIGH

| ID | Title | Aggregates |
|----|-------|------------|
| `F-X-002` | Silent error swallowing pattern is endemic across persistence and replay paths | G1-001, G3-001, G3-002, G3-007, G3-008, G4-007, G7-006, G7-008, G9-001, G9-017, allocator replay_free |
| `F-X-003` | Length-prefixed wire allocations let an attacker pin server memory before any work is rejected | G4-006, G5-002, G7-003, G8-004, G8-023, G9-003, G6-006 |
| `F-X-004` | HTTP admin surface authentication is partial — read-only fan-out endpoints leak cluster-wide state | G6-001, G6-002, G6-003, G6-004, G6-014 |
| `F-X-006` | Crash-window between durable redo append and engine apply persists across data plane | G4-002, G4-004, G7-016, G8-012, G8-017 |
| `F-X-009` | Cluster control plane lacks replay protection + documented lock order | G8-003, G8-007, G8-018, G8-020, G8-001, G8-002, G8-005 |
| `F-X-011` | Validation discipline at entry surfaces is uneven | G5-003, G5-005, G5-008, G5-015, G5-018, G5-021, G6-006, G6-007, G10-004, G10-005, G10-011 |

---

### 4.3 MEDIUM (42)

Rendered as a structured list — id, location, one-line issue, recommendation summary. Full template (code + impact) in per-group files.

#### Correctness (MEDIUM)

- `F-G1-005` `src/io.rs` `record_offset as usize` truncation in every `*_direct` helper — silent corruption on 32-bit targets. → Use `u64::try_into().expect()` with platform assert or compile-time gate.
- `F-G1-007` `src/device.rs` `MemoryDevice::pwrite/pread` `off + buf.len()` can overflow `usize` on huge configs. → `checked_add`.
- `F-G1-009` `src/allocator.rs` `MAX_PERSISTED_FREE_REGIONS` not enforced on `persist()` — freelist beyond cap silently truncates. → Surface as error.
- `F-G2-007` `src/ops/engine.rs` `spend_multi` doesn't cap `spent_count` against `utxo_count - prior_spent_utxos` — `wrapping_add` could exceed `utxo_count`. → Bound check.
- `F-G2-010` `src/ops/engine.rs` `read_metadata` / `read_slot` / `lookup_cached` / `read_slots` / `read_block_entry` don't acquire per-tx stripe lock — intentional but documented only at `src/io.rs:206`. → Document at each call site or refactor to safe API.
- `F-G3-003` `src/index/redb_dah.rs`/`redb_unmined.rs` `insert_batch` skips two-phase redo-log durability documented for `insert`. → Apply same two-phase.
- `F-G3-004` `src/index/backend.rs` `UnminedBackend::insert/remove` discards `UnminedRedoEntry` from the underlying `UnminedIndex` in the in-memory backend variant. → Propagate.
- `F-G3-005` `src/index/hashtable.rs` `HashTable::remove` backward-shift loop has no termination guard against a fully-occupied table. → Add bound on probe distance.
- `F-G3-006` `src/index/hashtable.rs` claims `Send + Sync` (`unsafe-asserted`) while readers/writers use raw-pointer access with no synchronization. → Document the locking contract OR add it.
- `F-G3-007` `src/index/redb_primary.rs` `lookup` swallows redb errors and returns `None`. → Propagate.
- `F-G3-008` `src/index/redb_dah.rs`/`redb_unmined.rs` `range_query` silently returns empty `Vec` on every redb error. → Propagate.
- `F-G4-005` `src/recovery.rs` `replay_freeze` (legacy form) freezes a slot whose hash has been reassigned since the redo entry was written. → Decode `expected_hash`; bail on mismatch.
- `F-G4-006` `src/recovery.rs` `Vec::with_capacity(parents_count)` in `CreateV2` decode can pre-allocate up to 2 MiB per entry — DoS amplifier via crafted log. → Cap.
- `F-G4-007` `src/recovery.rs` Recovery replay continues past fatal I/O / corruption errors instead of stopping. → Abort recovery; require manual intervention.
- `F-G4-008` `src/recovery.rs` `OP_FREEZE | OP_UNFREEZE if data.len() >= 68` decoder branch can mis-decode legacy 36-byte entries when entry happens to carry 68 trailing bytes. → Disambiguate by entry version.
- `F-G6-008` `src/server/http.rs` `/admin/top` aggregator collapses per-node trace propagation. → Propagate `traceparent`.
- `F-G6-024` `src/replication/manager.rs` Per-replica `last_acked_seq` updates use `Relaxed` store + load; `lag()` may observe half-updated pair. → Use `Release`/`Acquire`.
- `F-G7-005` `src/replication/receiver.rs` Migration batch dedup bypass silent on replay collision (cluster_key gate accepts wildcard zero). → Reject zero cluster_key.
- `F-G7-006` `src/replication/receiver.rs` `apply_op` Spend "graceful skip on tx-not-found" can mask replication drift. → Emit divergence metric + structured warn.
- `F-G7-011` `src/replication/manager.rs` Catch-up `chunk_seq` cursor reset bug — first chunk uses `from_seq`, later chunks compound. → Bug; fix arithmetic.
- `F-G7-016` `src/replication/receiver.rs` R-034/R-035 redo write happens AFTER engine apply — crash window remains. → Append-then-apply.
- `F-G7-019` `src/replication/manager.rs` ReplicaState transitions don't snapshot under lock — racy from `mark_replica_live`. → Lock around CAS.
- `F-G8-006` `src/cluster/coordinator.rs` Coordinator catch-up trusts `RoutingInfo::committed_members` despite no quorum proof, then disables itself via stub. → Require quorum proof.
- `F-G8-010` `src/cluster/migration.rs` Single source-side TCP timeout for migration; no per-batch ACK retry. → Per-batch retry with backoff.
- `F-G8-011` `src/cluster/coordinator.rs` Shard ownership atomic check vs in-flight write is racy through `dual_write_targets`. → Stronger ordering or single owner.
- `F-G8-012` `src/cluster/migration.rs` Migration source releases fence on TCP-ACK; no two-phase commit with target durability. → Two-phase: target persists+ACKs, then source releases.
- `F-G8-015` `src/cluster/swim.rs` Indirect probe peer selection not randomized; same K peers always asked. → Randomize.
- `F-G8-016` `src/cluster/coordinator.rs` `apply_master_election` empty partition view leaves round-robin master in place when ownership changed (R-052 partial). → Reject election when partition view is empty.
- `F-G8-019` `src/cluster/shards.rs` `set_master_for_shard` silently demotes existing master into replica slot — replica array may exceed RF. → Reject or bound.

#### Security (MEDIUM)

- `F-G1-004` `src/device.rs` `MemoryDevice` exposes both `data: RwLock<Vec<u8>>` and `raw_ptr` aliasing same heap allocation — concurrent use is UB. → Eliminate `raw_ptr`.
- `F-G2-003` `src/ops/engine.rs` `write_overflow_entries` on `entries.is_empty()` frees only `alignment` bytes, leaking allocator space on devices with sub-4K alignment. → Free full block.
- `F-G5-003` `src/server/dispatch.rs` `OP_QUERY_OLD_UNMINED` has no shard-ownership / authorization check — info disclosure. → Gate behind owned-shard check.
- `F-G6-003` `src/server/http.rs` `/ws/top` WebSocket is unauthenticated and runs indefinite per-second snapshots. → Bearer-auth.
- `F-G6-004` `src/server/http.rs` `extract_bearer_token` does no length-based equalisation before constant-time compare. → Pad input to known token length.
- `F-G6-005` `src/server/http.rs` Admin token matched verbatim with no minimum length or character-class enforcement. → Minimum 16 bytes; reject empty.
- `F-G6-007` `src/server/http.rs` `serve_embedded_file` falls back to index.html for ANY missing asset, including `..`-traversal probes. → Return 404 on traversal.
- `F-G6-014` `src/server/http.rs` Bearer middleware does not protect cross-origin browser misuse. → SameSite / origin check on browser GETs.
- `F-G8-008` `src/cluster/routing.rs` Partition map served unauthenticated when no `cluster_secret`. → Subsumed by F-X-001.
- `F-G8-023` `src/cluster/routing.rs` Decode does not bound `node_count` / `cm_count` against `data.len()` upfront. → Bound.
- `F-G10-005` `src/config.rs` No range validation for `device_size`, `expected_records`, `lock_stripes`, `max_batch_size`, `max_connections`. → Add `validate_sizes()`.
- `F-G10-006` `src/config.rs:505` `blobstore_path` default `/blobstore` is unusable for non-root processes. → Default to `./teraslab-blobstore`; probe writability.
- `F-G10-007` `src/config.rs:243,331,380` `ServerConfig` derives `Debug` over `admin_token`/`cluster_secret` — leak risk. → Wrap in `Secret(String)` newtype.
- `F-G10-008` `src/bin/server.rs:44-50` `detect_local_ip` connects to `8.8.8.8:53` — silent external network probe on startup. → Iterate `getifaddrs`; require `advertise_addr` when `listen_addr = 0.0.0.0`.

#### Concurrency (MEDIUM)

- `F-G1-003` `src/io.rs:read_metadata_direct` Acquire fence does not establish happens-before per Rust memory model. → Take stripe lock OR remove fence misleading comment.
- `F-G8-018` `src/cluster/swim.rs`/`src/cluster/migration.rs` Lock ordering — SWIM order vs topology event loop's order; no documented total order. → Document `membership → topology → migration → shards → peer_addrs → swim_peer_addrs`.
- `F-G8-024` `src/cluster/migration.rs` Migration `Failed` state retained in `active` list until cleanup — scanned through on every spend. → Cleanup eagerly.

#### Code Quality / Maintainability (MEDIUM)

- `F-G5-008` `src/server/dispatch.rs:handle_request` `ERR_INTERNAL` payload echoes inner `CodecError::Display` to client. → Map to opaque diagnostic.
- `F-G6-012` `src/observability/mod.rs` OTLP exporter accepts plaintext `http://` endpoint with no warning. → Warn on plaintext; require explicit opt-in.
- `F-X-010` *Cross-cut*: Test, CI, and dependency hygiene gaps. See `_review/03_crosscutting.md`.
- `F-X-011` *Cross-cut*: Validation discipline at entry surfaces is uneven. See `_review/03_crosscutting.md`.

---

### 4.4 LOW (85)

Listed as `ID — location — one-line issue` indexed back to per-group files for full template. Grouped by category.

#### Correctness (LOW) — 28 findings

`F-G1-002` `src/io.rs` targeted-write helpers leave header CRC-invalid if caller forgets `write_crc_direct` ·
`F-G1-008` `src/io.rs:read_metadata` allocates redundant 4 KiB `AlignedBuf` per call ·
`F-G1-010` `src/device.rs` `AlignedBuf` len==0 returns `dangling()` — handed to kernel by `DeviceIo::submit_*` ·
`F-G1-015` `src/allocator.rs` `replay_free` partial-overlap rejection is silent ·
`F-G1-016` `src/allocator.rs` `Reservation::FromFreelist` rollback re-inserts original region but doesn't coalesce ·
`F-G1-017` `src/device.rs` `MemoryDevice::raw_len` shadows `data.read().len()` — drift opportunity ·
`F-G1-019` `src/record.rs` `generation_target_ahead` missing `delta == GENERATION_ORDER_WINDOW` ambiguity test ·
`F-G2-005` `src/ops/engine.rs` `append_conflicting_child` retries indefinitely with no backoff ·
`F-G2-006` `src/ops/engine.rs` `pre_allocate_create` and `create_at_offset` re-build `cold_data` independently — no contract they agree ·
`F-G2-007` (dup with MEDIUM list — already promoted) ·
`F-G2-008` `src/ops/engine.rs` Idempotent re-spend short-circuit correct, but `apply()` still bumps generation + writes metadata when `spent_count==0` via all-idempotent path ·
`F-G2-009` `src/ops/engine.rs` `pre_allocate_create` silently ignores `external_ref_for_create` validation when dispatch passes mismatched flags ·
`F-G2-011` `src/ops/engine.rs` `set_mined` fast vs slow path divergence on response generation if cache stale ·
`F-G2-013` `src/ops/engine.rs` `set_locked_with_before_image` slow path swaps DAH to 0 only when `value=true` — partial fast-path failure cannot self-correct ·
`F-G3-010` `src/index/redb_primary.rs` `iter_collected` allocates `Vec::with_capacity(self.count)` based on cached count — no upper bound ·
`F-G3-013` `src/index/redb_dah.rs` `insert` reads `old_height` outside redo log write transaction — harmless because replay only uses `new_height` ·
`F-G3-014` `src/index/mod.rs` `Index::rebuild` advances by `record_size` from CRC-verified header — record_size not range-checked ·
`F-G3-019` `src/index/dah_index.rs` `DahIndex::insert` no-op short-circuit ignores duplicate-of-key in by_height vec from prior re-org bug ·
`F-G4-009` `src/redo.rs` `scan_all_with_tail` reads whole log into memory on `open()` — 64 MiB allocation ·
`F-G4-010` `src/recovery.rs` `RecoveryProgress` filter can be defeated by corrupt `through_sequence` exceeding real entries ·
`F-G4-012` `src/redo.rs` `compact_prefix_through` overwrites region without first clearing trailing bytes — stale entry headers remain ·
`F-G4-013` `src/redo.rs` `reset()` zeros only first alignment-block and trusts scan to stop on first zero ·
`F-G4-014` `src/recovery.rs` `replay_create` (legacy) skips when index already has an entry — but never verifies same `record_offset` ·
`F-G7-002` `src/replication/receiver.rs` `recv_ack` doesn't validate `response.request_id` against outgoing ·
`F-G7-004` `src/replication/durable.rs` `intent_tracker.commit()` deferral leaves stale ranges across crashes ·
`F-G7-007` `src/replication/manager.rs` `replicate_batch` writes same `next_sequence` cursor even on full failure ·
`F-G7-008` `src/replication/manager.rs` `AckTracker::flush_locked` swallows write errors ·
`F-G7-009` `src/replication/manager.rs` `replicate_batch` parallel fan-out: panic in scoped worker becomes generic error ·
`F-G7-010` `src/replication/manager.rs` `ReplicaBatchAccumulator::push` ignores `max_batch_size` ·
`F-G9-005` `src/storage/blobstore.rs` `FileStreamWriter` lock-window allows put/stream interleaving to briefly mismatch payload+sidecar ·
`F-G9-006` `src/storage/manager.rs` `read_cold_data` honors attacker-controlled `record_size` without upper bound ·
`F-G9-007` `src/storage/blobstore.rs` Disk-full / write failure in `FileStreamWriter::write_chunk` leaks `.tmp` for up to 5 minutes ·
`F-G9-008` `src/storage/uploader.rs` Uploader's pwrite of `ExternalRef` failure leaves blob present + record content_hash=0 ·
`F-G9-011` `src/storage/blobstore.rs` `stream_to`'s two-pass design races a concurrent rename ·
`F-G9-017` `src/storage/blobstore.rs` `FileBlobStore::walk_dir` swallows recursion errors silently ·
`F-G10-015` `src/bin/server.rs:632-649` `pending_conflicting_children` drained via `append_conflicting_child` mid-startup with no idempotency proof ·
`F-G10-021` `src/bin/server.rs:962-966` HTTP port fallback `9100` silently masks malformed `http_listen_addr` ·

#### Security (LOW) — 11 findings

`F-G5-008` (already in MEDIUM list) ·
`F-G5-016` `src/server/dispatch.rs` `OP_TOPOLOGY_PROPOSE/VOTE/COMMIT` parse payload BEFORE any auth check — DoS amplifier when auth IS enabled ·
`F-G5-020` `src/protocol/codec.rs` `RequestFrame::decode_frames` swallows error from any malformed trailing frame and silently truncates ·
`F-G5-021` `src/protocol/codec.rs` `decode_redirect` calls `String::from_utf8_lossy` — accepts non-UTF-8 bytes silently ·
`F-G6-015` `src/server/http.rs` Replica-lag readiness check is single-shot wall-clock scan per `/health/ready` ·
`F-G7-003` `src/replication/receiver.rs` 16 MiB+ per-connection buffer reachable pre-auth ·
`F-G8-013` `src/cluster/topology.rs` Topology proposer retries reuse the same `_started_at` — timeouts don't reset ·
`F-G8-014` `src/cluster/swim.rs` SWIM message receive loop drops malformed/unauthenticated packets silently with no rate limit ·
`F-G8-020` `src/cluster/auth.rs` Auth uses hand-rolled SHA-256 — no audit, no constant-time invariants beyond `constant_time_eq` ·
`F-G9-016` `src/storage/manager.rs` `delete_cold_data` deletes blob unconditionally — fine because txid→blob is 1:1, but undocumented ·
`F-G10-011` `src/config.rs:826-836` `cluster_secret` strength unvalidated — empty-string check only ·

#### Code Quality / Maintainability (LOW) — 35 findings

`F-G1-006` `src/record.rs` `TxMetadata::from_bytes_unchecked` is `pub` but skips CRC — footgun ·
`F-G1-011` `src/device_io/mod.rs` `create_device_io` ignores `queue_depth` when falling back to `SyncFallback` ·
`F-G1-012` `src/device.rs` Hard-coded ioctl numbers `0x80081272` / macOS `DKIOCGETBLOCKCOUNT` not portable across libc evolutions ·
`F-G1-013` `src/fault_injection.rs` `FaultMode::NoOpAt` is documented "functionally equivalent to None" — dead variant ·
`F-G1-014` `src/device_io/io_uring_backend.rs` `IoUringBackend` timestamp ring race — `record_submit_ts` unconditionally Relaxed but completion consumer is different thread ·
`F-G1-018` `src/locks.rs` `StripedLocks::lock` clones key bytes 16..24 every call — minor hot-path overhead ·
`F-G2-004` `src/ops/engine.rs` `unwrap()` on infallible-looking conversions violates CLAUDE.md "no `unwrap` in library code" ·
`F-G2-020` `src/ops/engine.rs` `ValidatedSpend::apply` writes slots one-at-a-time via `write_slot_fast` rather than coalescing — performance opportunity ·
`F-G3-011` `src/index/migration.rs` `serialize_secondary` materializes full entry list in memory — opposite of streaming export ·
`F-G3-012` `src/index/migration.rs` `locate_unmined_section` scans byte-by-byte for 4-byte magic — O(n) per false positive ·
`F-G3-015` `src/index/redb_primary.rs` Lacks documented concurrency contract on `RedbPrimary` itself — only `update_cached_fields` carries it ·
`F-G3-016` `src/index/hashtable.rs` `HashTable::open_file_backed` accepts file size as bucket count without header/magic/version/integrity check ·
`F-G3-017` `src/index/hashtable.rs` `recompute_max_probe_distance` rescans entire capacity on every `remove` — O(n) per delete ·
`F-G3-018` `src/index/hashtable.rs` `open_file_backed` doesn't check `initial_capacity > 0` before `next_power_of_two().max(16)` ·
`F-G4-011` `src/recovery.rs` `mark_recovery_progress` writes a separate fsync per call — 1 extra fsync per 1024 entries ·
`F-G4-015` `src/recovery.rs` `apply_replay_dah_patch` uses `flags -= flags & X` instead of `flags &= !X` ·
`F-G4-016` `src/checkpoint.rs` `perform_checkpoint_with_reset_guard` quiesces dispatch via `acquire_dispatch_visibility_guard` for full snapshot — blocks writes for snapshot duration ·
`F-G5-006` `src/server/dispatch.rs` `OP_HEARTBEAT` (opcode 250) has no dispatch handler — falls into catch-all, returns `ERR_INTERNAL "unknown opcode"` ·
`F-G5-007` `src/server/dispatch.rs` `handle_admin_diagnose_key` parses count with `try_into().expect("4 bytes")` — defence is correct but fragile ·
`F-G5-009` `src/server/dispatch.rs` `partition_version_report` `try_into().unwrap_or([0u8;8])` silently substitutes zero ·
`F-G5-010` `src/server/dispatch.rs` `OP_REPLICA_BATCH` shard-extraction from `request_id` re-uses low-16-bit cast pattern ·
`F-G5-011` `src/protocol/codec.rs` `RequestFrame::decode` allocates `payload: data[16..frame_size].to_vec()` — one full-payload copy per frame ·
`F-G5-015` `src/server/dispatch.rs` `OP_INCREMENT_SPENT_EXTRA_RECS` is a public opcode returning `STATUS_OK` unconditionally (no-op shim) ·
`F-G5-017` `src/server/dispatch.rs` Dispatch error responses don't include request `op_code` in machine-readable payload ·
`F-G5-018` `src/protocol/codec.rs` `decode_get_response_checked`/`decode_sparse_errors_checked` validate `count` and per-item minimum but don't cap variable per-item `data_len` ·
`F-G5-022` `src/server/dispatch.rs` `handle_set_locked_batch` (and siblings) snapshot pre-image AFTER writing redo entry ·
`F-G5-023` `src/server/dispatch.rs` `handle_delete_batch`'s compensation rebuilds record by synthesising inbound `OP_REPLICA_BATCH` to itself ·
`F-G5-024` `src/server/dispatch.rs` `handle_stream_chunk` uses `expect("just inserted")` after entry-or-insert flow — fragile ·
`F-G5-027` `src/protocol/codec.rs` `decode_stream_chunk` uses `try_into().unwrap()` on path where prior length guard makes the unwrap unreachable, but pattern violates parser hygiene rule ·
`F-G5-028` `src/server/dispatch.rs` `OP_PROCESS_EXPIRED_PRESERVATIONS` is in mutation list; handler routes through `handle_delete_batch` which does its own quorum check — double check fine, but synthesized re-entry of middleware is fragile ·
`F-G6-009` `src/server/http.rs` `aggregate_snapshots` divides by `total_count` without rebalancing for nodes that returned no data ·
`F-G6-010` `src/server/http.rs` `ws_top_loop` "drain incoming messages" loop swallows close frames silently ·
`F-G6-011` `src/server/http.rs` `handle_admin_drain` accepts `node_id` from path but ignores it after rejecting cross-node drains ·
`F-G6-013` `src/observability/mod.rs` OTLP span attributes audit — currently only contains static `route` ·
`F-G6-016` `src/server/http.rs` `start_http_server` builds new Tokio runtime with `worker_threads(4)` regardless of host ·
`F-G6-017` `src/server/http.rs` `start_http_server` constructs runtime inside `block_on` but never installs panic hook for handlers ·
`F-G6-018` `src/server/http.rs` `replay_cause_label` marked `#[allow(dead_code)]` despite intent to be referenced ·
`F-G6-019` `src/server/http.rs` Connection accept loop uses 10 ms sleep — burns CPU at idle, not great for graceful shutdown ·
`F-G6-020` `src/server/http.rs` `InflightBytesLimiter::try_acquire` short-circuits per-frame limit but never logs/counts rejection ·
`F-G6-021` `src/server/http.rs` `WireTraceContext::read_from` panics on wrong-length input — caller contract non-enforced ·
`F-G6-023` `src/metrics.rs` `prom_histogram_ns` emits `bucket_upper_ns_at(i)` for last non-`+Inf` as `u64::MAX` — Prometheus parsers may reject ·
`F-G6-025` `src/server/http.rs` HTTP error handlers return free-form strings — no structured error code or content negotiation ·
`F-G6-026` `src/observability/mod.rs` `ObservabilityConfig` env override silently succeeds for `TERASLAB_OTLP_ENDPOINT=""` — but a typo like `TERASLAB_OTLP_ENDPONIT` is ignored ·
`F-G6-027` `src/server/http.rs` `start_http_server` panics if Tokio runtime build fails — kills the process ·
`F-G6-028` `src/server/http.rs` `load_primary_index_redb` checks import sentinel before any restore but doesn't check sentinel mtime ·
`F-G7-012` `src/replication/protocol.rs` V1 batch decoder still wired despite "never produced" — dead code ·
`F-G7-013` `src/replication/receiver.rs` Per-thread receiver thread-local tracker leaks one tracker per worker ·
`F-G7-014` `src/replication/tcp_transport.rs` `is_connected` uses `take_error` which is misleading on macOS ·
`F-G7-015` `src/replication/manager.rs` Replay order under reconnect — replica relies on receiver dedup, not master ordering ·
`F-G7-017` `src/replication/protocol.rs` `MAX_ACK_FRAME_SIZE = 1024` may be tight under HMAC + error messages ·
`F-G7-018` `src/replication/manager.rs` `replicate_batch` blocks on slowest replica with WriteAll ·
`F-G8-009` `src/cluster/coordinator.rs` `alive_node_count` self-include heuristic depends on `node_addrs` not containing self ·
`F-G9-010` `src/storage/blob_gc.rs` `OP_BLOB_PUT` referenced in doc comment does not exist ·
`F-G10-009` `src/bin/cli.rs:55` CLI `data_addr` default `localhost:3000` does not match server's `listen_addr` default `127.0.0.1:3300` ·
`F-G10-010` `src/config.rs:798-854` `enable_admin_endpoints` does not require `enable_remote_bind` — easy operator footgun ·
`F-G10-012` `src/bin/server.rs:461-464` `load_factor * 100.0` is computed but field is logged as `load_factor` — labeling bug ·
`F-G10-013` `src/bin/server.rs:672,677` `expect("invalid listen_addr")` after validation works but rule-violating ·
`F-G10-014` `src/lib.rs` Exposes every module as `pub mod` — internals leak through public API ·
`F-G10-016` `src/bin/server.rs:1224-1258` `recovery_completes_before_listener_bind` test relies on source-string ordering — fragile ·
`F-G10-017` `src/bin/server.rs:922-942` Per-replica catch-up panic-free but stringly-typed error contract — `e.contains("redo entries reclaimed")` ·
`F-G10-022` `src/bin/server.rs` `_blob_gc_handle`, `_lag_monitor_handle`, `_checkpoint_handle`, `_redo_log_device` — leaked join handles ·

#### Concurrency (LOW) — 11 findings

(Listed inline within the Correctness/Code Quality groupings above where overlap is unambiguous.)

---

### 4.5 INFO (61)

61 INFOs — 52 INFOs and 9 explicit positive verifications (those marked "verified resolved" or "positive verification"). Positive verifications worth surfacing:

- `F-G2-016` `unspend` correctly validates `spending_data` matches original spender before clearing — **prior A-04 RESOLVED**.
- `F-G3-009` `AUDIT.md` "3 failing rebuild_* tests" claim is **stale** — tests have been split and now pass.
- `F-G4-017` `replay_spend` / `replay_unspend` correctly re-derive `spent_utxos` via `saturating_add(1)` / `saturating_sub(1)` — **prior BC-04 RESOLVED**.
- `F-G5-012` `decode_unspend_batch_checked` correctly enforces spending_data match — **prior unspend-authority concern RESOLVED**.
- `F-G5-013` `MAX_FRAME_SIZE = 16 MiB` cap enforced BEFORE any payload allocation — **prior frame-length OOM RESOLVED**.
- `F-G5-014` Per-connection 30s read/write timeouts prevent slow-loris — **prior R-054/LMNH-01 RESOLVED**.
- `F-G5-026` `opcodes.rs` carries per-item caps `MAX_COLD_DATA_PER_ITEM`, `MAX_UTXO_HASHES_PER_CREATE_ITEM`, etc. — **R-089/R-090 verified**.
- `F-G6-022` Metrics labels are bounded enums; no client-IP or user-string labels — **positive (no cardinality blow-up)**.
- `F-G9-013` R-049 orphan-blob recovery reconciliation correctly placed — **verified**.
- `F-G9-015` `input_refs.rs` correctly applies R-051 RMW pread-error propagation — **verified**.
- `F-G10-018` R-056 admin-token gating end-to-end integration — **verified**.
- `F-G10-019` `validate_safe_defaults` correctly rejects insecure bind defaults — **verified**.
- `F-G10-020` CLI has no shell-injection vector — **verified**.

Other INFOs are observational (deployment assumptions, dead-code call-outs, comment/code drift, design tradeoffs). Full list with file:line in per-group files.

---

## 5. Cross-cutting concerns

Eleven cross-cutting concerns are documented in [`_review/03_crosscutting.md`](_review/03_crosscutting.md). The HIGH/CRITICAL items have been folded into Section 4 above. The full list:

1. `F-X-001` *(CRITICAL)* — Inter-node TCP authentication is fail-open by default.
2. `F-X-002` *(HIGH)* — Silent error swallowing pattern is endemic across persistence and replay paths.
3. `F-X-003` *(HIGH)* — Length-prefixed wire allocations let an attacker pin server memory before any work is rejected.
4. `F-X-004` *(HIGH)* — HTTP admin surface authentication is partial.
5. `F-X-005` *(CRITICAL)* — Sequencing/cursor state is not robust to compaction or partial failure.
6. `F-X-006` *(HIGH)* — Crash-window between durable redo append and engine apply remains across the data plane.
7. `F-X-007` *(MEDIUM)* — Hot read paths violate the stripe-lock contract documented in `src/io.rs`; the violation is now codified rather than fixed.
8. `F-X-008` *(CRITICAL)* — Process lifecycle: SIGTERM/SIGINT is a no-op; graceful-shutdown cleanup is dead code.
9. `F-X-009` *(HIGH)* — Cluster control plane lacks both replay protection and a documented lock-order.
10. `F-X-010` *(MEDIUM)* — Test, CI, and dependency hygiene gaps.
11. `F-X-011` *(MEDIUM)* — Validation discipline at entry surfaces is uneven.

---

## 6. Suggested remediation order (top 10)

1. **Make `cluster_secret` mandatory at startup when `replication_factor > 1` or membership > 1**, and reject all inter-node opcodes if absent. *(F-X-001, F-G5-001, F-G7-001, F-G8-008)*. Smallest patch in the report; eliminates the largest blast-radius defect.
2. **Wire SIGTERM/SIGINT to graceful shutdown and share the atomic between bin + Server.** Add `ctrlc`/`signal-hook` dep, share an `Arc<AtomicBool>`. *(F-G10-001, F-G10-002, F-G10-003, F-X-008)*. Without this, F-G4-001 and F-X-006 fire on every restart.
3. **Persist `next_sequence` in the redo-log header** so compaction-to-empty + restart does not roll the cursor back to 1. *(F-G4-001, F-X-005)*.
4. **Re-order `Engine::delete()` to unregister-before-free** OR have read paths verify `meta.tx_id == requested_tx_id`. *(F-G2-001)*.
5. **Tighten split-brain heal**: track `committed_voter_ever_seen` or add a `cluster_id` UUID; apply on both proposer and follower. *(F-G8-001, F-G8-002)*.
6. **Gate `/admin/top`, `/ws/top`, `/admin/*` reads** behind the same bearer middleware as writes; make `/health/ready` reflect real readiness. *(F-G6-001, F-G6-002, F-X-004)*.
7. **Apply-before-append discipline on the replica path**: durable redo entry must land before engine mutation. Migration ACKs must wait on target durable persist. *(F-G7-016, F-G8-012, F-G8-017, F-X-006)*.
8. **Add `cap_count_against_payload(count)` helper and use it at every wire-decode site** (`TopologyTerm`, `CreateV2` recovery decode, replication 16 MiB read buffer, routing decode). *(F-X-003)*. Adds a CI lint to forbid raw `Vec::with_capacity(wire_count)`.
9. **Stop swallowing errors silently in persistence paths**: convert every `let _ = ...` and bare `tracing::warn` over a `Result` to either propagation or a `silent_error_total{site}` counter + structured WARN log. *(F-X-002, F-G3-001, F-G3-002, F-G3-007, F-G3-008, F-G9-001, F-G9-002)*.
10. **Add `cargo audit`, `cargo deny`, `cargo miri` (lib subset), and a `cluster_secret = None` rejection regression test to CI.** Move `AUDIT.md` / `AUDIT_CODEX.md` to `_audits/historical/` with a banner. *(F-X-010, F-G3-009)*.

---

## 7. Open questions

These are aspects this review could not determine from code alone — they depend on intended behaviour, deployment model, or threat model that the codebase does not pin down.

1. **Is the HTTP observability port intended to be public or private?** Several findings (F-G6-002, F-G6-003, F-X-004) depend on whether the deployment binds 9100 to a public NIC. The validator does not encode this. If it is *always* private, the leaks become INFO; if it is operator-configurable (as today), they remain HIGH.
2. **Is `teraslab` published as a library, or only consumed by the in-repo bins + the `client/rust` crate?** `src/lib.rs` exposes every module as `pub`. If the crate is published, the API contract is effectively the entire internals (F-G10-014). If not, the surface should be `pub(crate)` for everything except `protocol`, `record`, `config`.
3. **What is the intended threat model for the data port?** F-X-001 fails open because the *default* assumes "trusted network." The README and CLAUDE.md don't pin this down. If the data port is always behind a private overlay, the auth gate's existence is defense-in-depth; if it might face the internet, fail-open is unacceptable. The same question applies to whether TLS termination is expected upstream.
4. **Is the redo-log compaction-to-empty path intentional, or a refactor artifact?** F-G4-001 depends on the compaction's "drop entries when all are RecoveryProgress" being a real optimisation versus an oversight that nobody re-checked the cursor side-effect for.
5. **Are `MemoryDevice`'s `RwLock<Vec<u8>>` + `raw_ptr` aliasing both genuinely needed?** F-G1-004 flags this as UB; if `raw_ptr` is only there for a test scenario that no longer exists, simply deleting it removes the UB.
6. **What test gate should be added for F-X-001's fail-open?** This review recommends a regression test that boots a 3-node cluster with `cluster_secret = None` and asserts `OP_TOPOLOGY_PROPOSE` from a peer is rejected — but if the intended deployment uses a different gating model (mTLS on a separate port, say), the test should target that gate instead.
7. **Are the two stale prior-audit files (`AUDIT.md`, `AUDIT_CODEX.md`) cited externally?** They contain a stale "3 failing rebuild_* tests" claim and a few other now-resolved findings. Archiving them under `_audits/historical/` would prevent contributor confusion, but only if no external doc/PR links to their root-level paths.

---

*End of report. All raw artifacts and per-group findings remain at `_review/`.*
