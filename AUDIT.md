# TeraSlab Bulletproofing Audit

**Date:** 2026-06-11 · **Commit:** `5f47b2a` · **Standard:** "I would stake real money on this system."

**Method:** full build/test/clippy run, then 14 parallel deep-read passes (one per audit category), each producing a detailed report under `audit/raw/`. This document is the synthesis: every finding here is backed by a full write-up (what's wrong / why it matters / reproduction / suggested fix, with file:line) in the corresponding raw report. Matrices live in `audit/coverage-matrix.md` and `audit/error-codes.md`.

**Build/test/clippy record:**

- `cargo build --release` — clean.
- `cargo clippy --all -- -D warnings` — zero warnings.
- `cargo test --all --release` — **1 failure**: `tests/g2_delete_race.rs::delete_does_not_alias_concurrent_create` panics at the aliasing assertion (`g2_delete_race.rs:282`) in ~40% of runs (2 of 5 reruns). This is not a flaky test — it is a real product race, reproduced live during this audit (finding A-1). Everything else passes (~2,000 tests), 0 `#[ignore]`.

**Verdict:** single-node, single-process TeraSlab is genuinely strong — WAL-first ordering is real, CRC coverage is pervasive, wire-protocol hardening is textbook, double-spend protection under concurrency is proven by a real 16-thread stress test. The money-losing risk concentrates in three seams: **(1) checkpoint-time durability** (the redo log is reclaimed without ever syncing the data device), **(2) the production replication path** (which shares no code with the well-tested `ReplicationManager` and has a broken sequence space), and **(3) the migration commit window**. These are exactly the paths the test suite exercises least, and the only tests that would catch them (SIGKILL crash/chaos Docker scenarios) exist but are wired to CI tiers that no workflow ever runs.

---

## 1. Executive summary — the 10 most dangerous gaps

1. **[CRITICAL]** Checkpoint reclaims redo entries without ever syncing the data device — every acked mutation since the last data-device sync (including spends) can vanish on power loss, re-materializing spent UTXOs (`src/checkpoint.rs:338-403`, `src/allocator.rs:1145`).
2. **[CRITICAL]** Migration lifts the shard fence and closes dual-write *before* `commit_shard` — a write acked in that gap lands only on the old master and is then deleted by orphan cleanup (`src/cluster/coordinator.rs:180-227`).
3. **[CRITICAL]** Replication receiver's high-water-mark dedup ACKs out-of-order batches *without applying them* — acked-but-never-replicated ops, silent permanent replica divergence (`src/replication/receiver.rs:835-865`).
4. **[CRITICAL]** redb index backend commits with `Durability::Eventual` and checkpoint never flushes it, yet the redo prefix is compacted anyway — power loss silently loses index mutations with no rebuild trigger; README's "crash-durable by default" is false (`src/index/redb_primary.rs:94`, `src/checkpoint.rs:363`).
5. **[CRITICAL]** A torn/corrupt allocator header at boot silently falls back to a *fresh* allocator — subsequent creates overwrite live records (`src/bin/server.rs:325-357`).
6. **[CRITICAL]** `create` takes no stripe lock (every other mutating op does) — concurrent create/delete on the same key aliases another transaction's data under the requested txid. **Reproduced: the in-tree test fails ~40% of runs** (`src/ops/engine.rs:2058-2195`; `tests/g2_delete_race.rs:282`).
7. **[CRITICAL]** Replica catch-up has an off-by-one: chunk N+1 is labeled with the last *acked* sequence instead of +1, so the receiver's dedup skips one real op per 1,000 replayed (`src/replication/durable.rs:817`).
8. **[CRITICAL]** Per-replica batches are stamped with the master-global redo range while carrying per-address op subsets — the entire ACK/catch-up/lag sequence space is fictional and gaps are undetectable by design (`src/server/dispatch.rs:1536-1542`).
9. **[HIGH]** Topology `handle_commit` never checks `cluster_id` or quorum safety (only the propose/vote path does) — a foreign higher-term commit from a secret-sharing cluster is adopted wholesale; this is the split-brain-heal hole (`src/cluster/topology.rs`, called from `src/server/dispatch.rs:1065`).
10. **[HIGH]** Startup replica catch-up connects *without* the cluster secret, so in any secured deployment the only divergence-repair path is dead on arrival — every catch-up frame is rejected by the receiver's auth gate (`src/replication/durable.rs:808`).

Near-misses for the top 10: bidirectional master↔master replication can form a cross-node circular wait on the engine-wide visibility barrier (C-1); the file-backed index resize writes the clean-shutdown sentinel mid-run, defeating torn-write detection (G-2); migration never reconstructs secondary indexes on the target (F-2); `PreserveUntilBatch` preservations never expire (KO-1); `unspend` violates the Lua reference's idempotent-OK contract, deterministically wedging Teranode's conflict-resolution flow (LP-1); `setMined` drops block IDs from the wire response that `txmetacache.SetMinedMulti` consumes (LP-2); and the SIGKILL crash/chaos test suites exist but are never run by any CI workflow (N-1).

---

## 2. Findings by category

Severity counts across all categories: **8 CRITICAL · 26 HIGH · ~37 MEDIUM · ~50 LOW.** Full detail (reproductions, fixes) in `audit/raw/*.md`. (Counts include the post-audit Lua-parity pass, section 2-K/O below.)

### A. UTXO correctness invariants — `audit/raw/A-utxo-correctness.md`

The headline invariants hold and are genuinely tested: concurrent double-spend produces exactly one success (proven by `tests/g2_atomic_apply.rs`, a real 16-thread/200-iteration stress test asserting exactly one winner and N−1 `ALREADY_SPENT` with correct 36-byte spending data); spending data is stable; `Unspend` requires matching spending data; coinbase maturity, frozen/locked/conflicting flags, `FROZEN_UNTIL` cooldown, `VOUT_OUT_OF_RANGE`, and `UTXO_HASH_MISMATCH` no-mutation are all verified with evidence. 8/10 checklist items verified. One is broken:

- **[CRITICAL] A-1 — `create` is the only mutating op that takes no stripe lock; duplicate check is non-atomic read-then-insert, and `insert` silently overwrites.** `src/ops/engine.rs:2058-2195`, `2279-2428`, `670-710`; overwrite at `src/index/hashtable.rs:910`. Two concurrent creates of the same txid both succeed (breaking the `ALREADY_EXISTS` contract), orphaning a record and leaking its allocation; interleaved with `delete`, the index entry for tx_A can point at a region reallocated to tx_B. **This is not theoretical: `tests/g2_delete_race.rs::delete_does_not_alias_concurrent_create` fails its aliasing assertion in ~40% of runs on this machine** — readers received tx_B's metadata under tx_A's key. The existing duplicate-create regression test passes only because it asserts `successes >= 1` instead of `== 1` (`engine.rs:11514`). **Fix:** acquire the stripe lock in `create`/`pre_allocate_create`/`create_at_offset_inner` exactly as the other 18 mutating ops do, and make the index insert reject (not overwrite) an existing live key; tighten the test to `== 1`.
- **[LOW] A-2** — `engine.create()` leaks the allocation on index-register failure on direct (non-dispatch) call paths (`engine.rs:2154-2173`).
- **[LOW] A-3** — `unspend` writes the slot before decrementing the spent counter; an interrupt on non-WAL direct paths leaves an over-count (`engine.rs:1547-1588`).

### B. Crash recovery and durability — `audit/raw/B-crash-recovery.md`

Verified solid: WAL-first ordering at all forward mutation sites; CRC32 on metadata/slot/redo/header structures; snapshot writes are tempfile+fsync+rename+dir-fsync; `File::sync_all` is `F_FULLFSYNC` on Darwin; allocator journals allocations before returning offsets; recovery boundary tests genuinely restart and verify state. The systemic weakness: the per-op durability proof ("the redo entry covers the volatile data write") is invalidated at checkpoint time, and no test can catch it because every durability test runs on `MemoryDevice`, whose `sync()` is a no-op.

- **[CRITICAL] B-1 — Checkpoint reclaims redo entries without syncing the data device.** `src/checkpoint.rs:338-403`, `src/allocator.rs:1098-1147`. The checkpoint fsyncs the snapshot file and the redo device, but never the data device; slot/metadata pwrites for every fenced mutation may sit in the drive's volatile cache (or unjournalled sparse-file extents — `DirectDevice` preallocates via `set_len`, `src/device.rs:873-882`) when compaction destroys their only durable copy. Power loss ⇒ acked spends disappear ⇒ double-spend. The module doc at `checkpoint.rs:27-29` claims "allocator persist is fsynced" — it is not (`allocator.rs:1145`). **Fix:** `device.sync()` on the data device before `mark_recovery_progress`/`compact_prefix_through`, and add a sync to `SlotAllocator::persist`.
- **[CRITICAL] B-2 — Torn/corrupt allocator header silently falls back to a fresh allocator** (`Err(_) => SlotAllocator::new`), so the next creates overwrite live records, or device-scan rebuild yields an empty store. `src/bin/server.rs:325-357`. **Fix:** fail closed; require explicit operator action or device-scan reconstruction of the allocator.
- **[HIGH] B-3** — `compact_prefix_through` rewrites retained durable post-fence entries in place; a torn compaction write erases them (`src/redo.rs:2123-2193`).
- **[HIGH] B-4** — Spend/Unspend replay is non-idempotent across spend→unspend→respend histories; `spent_utxos` drifts +1 and can stamp `delete_at_height` on a record with a live UTXO (`src/recovery.rs:957-1110`).
- **[HIGH] B-5** — A torn slot/metadata write inside the WAL-protected window is CRC-detected but unrepairable (Spend redo entries lack the `utxo_hash` needed to reconstruct); recovery fails closed into a boot loop (`src/recovery.rs:971-974`).
- **[MEDIUM]** B-6 recovery-progress markers can hit `LogFull` on a nearly-full log → deterministic startup abort loop (`recovery.rs:469-490`); B-7 unconditional full-index secondary reconcile every boot, O(store size) (`recovery.rs:492,541-576`); B-8 fault injection covers only 10 hand-picked sync points, none inside checkpoint/compaction, all on a no-op-sync `MemoryDevice` (`src/fault_injection.rs`); B-9 snapshot-deleted rebuild test covers clean shutdown only (`tests/integration.rs:1447`).
- **[LOW]** B-10 replication-failure compensation applies engine mutations before logging compensation intents (documented, bounded).

### C. Concurrency — `audit/raw/C-concurrency.md`

Single-node discipline is strong: power-of-two stripes enforced; no multi-stripe holding anywhere (so no ordering protocol needed); no `.await` under any guard (data plane is fully synchronous); all hashtable mmap `unsafe` is justified and fenced by the engine RwLock including a sound upgradable-read resize; no fire-and-forget tokio tasks. The real problems are at the node boundary:

- **[HIGH] C-1 — Engine-wide exclusive `dispatch_visibility_barrier` is held across the synchronous replication network round-trip** (`src/ops/engine.rs:110`, `src/server/dispatch.rs:394`, `3063→1528→2459`). Since `OP_REPLICA_BATCH` on the peer needs *its* exclusive barrier, bidirectional master↔master write load forms a cross-node circular wait broken only by the multi-second ack timeout — and every in-flight mutation stalls all client reads for the full replication RTT.
- **[HIGH] C-2 — `TopologyAuthority::handle_propose` does a lock-free Relaxed load→compare→store on `voted_term`** (`src/cluster/topology.rs:978-1044`); concurrent same-term proposals can both be accepted — a double-vote per term, the precondition for conflicting topology commits.
- **[MEDIUM]** C-3 migration `stream_shard_baseline` reads metadata then slots with no stripe lock → torn per-record snapshots (`coordinator.rs:4528-4557`); C-4 the exported `ReplicaBatchReceiver::start()` listener applies ops with no locking at all (`receiver.rs:1619-1631`); C-5 zero loom models; the two broken interleavings above are untested.
- **[LOW]** stale `unsafe impl Send/Sync` justifications on `Engine`; unseeded stripe selection (txid grinding can collide stripes — the hashtable seeds against exactly this, the lock table doesn't); non-atomic migration gauge decrements.

### D. Replication — `audit/raw/D-replication.md`

**The headline structural fact: production replication (`replicate_all_ops`, `src/server/dispatch.rs:1476`) shares no code with the carefully-tested 3,056-line `ReplicationManager`, which is constructed only in `#[cfg(test)]` modules.** (Verified during synthesis: the `ReplicaTransport` trait and `TcpReplicaTransport` are live; the manager orchestration is test-only.) The three CRITICALs all stem from the receiver's single high-water-mark dedup operating on a sequence space that isn't per-replica sequential.

- **[CRITICAL] D-1 — Out-of-order batch delivery silently dropped by receiver high-water-mark dedup** (`src/replication/receiver.rs:835-865`): a late batch is ACKed as applied without applying. Master's `AckTracker` advances past it — acked-but-never-replicated ops, unrepairable divergence that becomes authoritative on failover.
- **[CRITICAL] D-2 — Catch-up off-by-one**: `run_catchup_for_replica` labels chunk N+1 with the last *acked* sequence instead of +1 (`src/replication/durable.rs:817`), so the receiver skips one real op per 1,000 replayed. The test-only `manager.rs:1082` version does it correctly.
- **[CRITICAL] D-3 — Per-replica batches stamped with the master-global redo range but carrying per-address subsets** (`src/server/dispatch.rs:1536-1542`) — sequence gaps are undetectable by design; D-1/D-2 are symptoms.
- **[HIGH] D-4** — Replication-failure compensation is local-only and clears the intent (`dispatch.rs:1976-2401`): replicas that ACKed (or crashed mid-batch) keep the "failed" mutation forever.
- **[HIGH] D-5** — Startup catch-up connects without the cluster secret (`durable.rs:808`): the only repair path is dead exactly in production-secured deployments.
- **[HIGH] D-6** — `write_majority` quorum is counted over the union of fan-out addresses (including dual-write extras), not per key — a key can get `STATUS_OK` with zero replica copies (RF=2 `write_majority`, RF≥3 `auto`).
- **[HIGH] D-7** — No runtime catch-up trigger: one pass at master startup, capped at 10k ops; a replica that misses batches while the master stays up is never repaired.
- **[MEDIUM]** D-8 lag monitor action is warn+readiness only, computed over the fictional ACK space; D-9 `best_effort`/`STATUS_DEGRADED_DURABILITY` unreachable in any valid config while README documents status 5 as live; D-10 dead/divergent code (`ReplicaBatchAccumulator` unused; `tests/replication_rollback.rs` referenced in comments doesn't exist).
- **[LOW]** no replay nonce/TLS on the replication transport; `AckTracker` flush failures only warn; intent tracker rewrites+fsyncs the whole file per batch.

### E. Clustering and quorum — `audit/raw/E-clustering.md`

The peak-size quorum design is sound and correctly threaded through the write gate, activation, fallback, and retry paths; peak size is persisted and restart-tested (`tests/g8_split_brain.rs:362`). Two real holes:

- **[HIGH] E-1 — SWIM replay window keyed by `NodeId` only** (`src/cluster/swim.rs:401,765`): a rebooted node restarts `next_outbound_seq` at 1 while receivers keep the old run's high-water mark, so its JOIN/PING/ACK are seq-dropped before reaching membership; incarnation is never consulted in `check_and_record`. Rejoin-after-reboot is effectively broken for long-lived clusters.
- **[HIGH] E-2 — `handle_commit` never checks `cluster_id` or `membership_change_is_safe`** (called from `dispatch.rs:1065` and catch-up `coordinator.rs:1548/1601`): a foreign higher-term commit from a secret-sharing, distinct-`cluster_id` cluster is adopted, abandoning local topology. All split-brain guards live only on the propose/vote path. This is the answer to "what happens when two clusters learn about each other" — and it's the wrong answer.
- **[MEDIUM]** E-3 minority can pass `check_quorum` during the SWIM suspicion window (suspects count as alive); E-4 inter-node frame auth has no nonce — only a 5-minute timestamp window, so replayed valid frames pass (relies on per-opcode idempotency); E-5 clock skew > 5 min silently partitions the cluster with no distinct diagnostic.
- **[LOW]** `seen_seq` map unbounded (dead-node GC omits it); no test for cluster formation with a dead seed.

### F. Sharding and migration — `audit/raw/F-sharding-migration.md`

Verified: shard mask `& 0x0FFF` correct with a single canonical computation site; deterministic round-robin; the migration fence is enforced on all 15 mutating handlers; `migration_pool_size`/`migration_batch_size` are live; drain genuinely waits; delete tombstones are forwarded.

- **[CRITICAL] F-1 — Acked writes lost in the unfence-before-commit window.** `complete_migration_task_current_epoch` (`src/cluster/coordinator.rs:180-227`) lifts the fence and closes dual-write *before* `commit_shard`. A write landing in the gap is acked on the old master, never reaches the new master (target manifest already verified), then is deleted by orphan cleanup. Same family: the fence is checked only at dispatch entry (TOCTOU), so a stalled request applying after manifest collection is also silently lost. **Fix:** commit the shard table (or at minimum re-check the fence under the write path) before lifting the fence; drain in-flight ops at fence time.
- **[HIGH] F-2 — Migration doesn't reconstruct secondary indexes on the target**: `apply_create_lifecycle_and_blob` (`receiver.rs:1036-1067`) patches `unmined_since`/`delete_at_height`/`preserve_until` via raw `write_metadata`, bypassing the unmined index, DAH index, and cached fields. Until the new master restarts, the DAH sweep never deletes migrated records and `QUERY_OLD_UNMINED` misses them.
- **[MEDIUM]** F-3 no crash-mid-migration integration test (the persistence/restore machinery looks correctly wired but is never exercised by killing a node); F-4 redirect-loop protection exists but is unused — no client follows redirects, no hop counter, and an unknown master emits a redirect with an *empty address*; F-5 spec drift: phases/09 mandates Redirect-on-write during migration, the implementation does dual-write+fence (README documents the implementation; the phase doc is stale).
- **[LOW]** legacy `ShardTable::compute` version hash is order-dependent (test/bootstrap-only).

### G. Index backends — `audit/raw/G-index-backends.md`

- **[CRITICAL] G-1 — redb commits use `Durability::Eventual` everywhere and checkpoint never flushes redb** (`snapshot_all` is a no-op for `OnDisk`), yet the checkpoint writes a `RecoveryProgress` fence and compacts the redo prefix (`src/index/redb_primary.rs:94`, `src/index/backend.rs:445`, `src/checkpoint.rs:363`, `src/redo.rs:1971`). Power loss after a checkpoint silently loses index mutations with no rebuild trigger. README's "crash-durable by default" claim is false for this backend.
- **[HIGH] G-2 — Auto-resize drops the old table, whose `Drop` writes the clean-shutdown sentinel mid-run** (`src/index/hashtable.rs:1420-1440`, `:1289`): any crash after the first resize is treated as a clean shutdown and torn bucket bytes are accepted — defeats the torn-write detection the sentinel exists for.
- **[HIGH] G-3 — An index file with invalid size is silently treated as fresh** (sentinel check skipped, file wiped via `set_len`) — server boots with an empty primary index instead of device-scan rebuild (`hashtable.rs:661-678`).
- **[HIGH] G-4 — ~40 engine call sites use the lossy `PrimaryBackend::lookup`/`unregister` shims** (error → "key absent"); with redb, a transient I/O error makes spends return `TX_NOT_FOUND` and lets creates pass the duplicate check (`src/index/backend.rs:99,188`).
- **[MEDIUM]** G-5 `tests/secondary_two_phase_durability.rs` never restarts a process (reuses the live in-memory primary across the simulated "crash"); G-6 engine/dispatch suites are memory-backend-only; G-7 the only deliberately-corrupt-redb test discards its result with `let _ =` and asserts nothing. (Note: the audit brief's premise "redb falls back to in-memory if corrupt" is stale — README documents fail-closed and the code matches.)
- **[LOW]** `u16` probe-distance counters make `dist >= capacity` guards dead above 65,536 buckets; checkpoint serializes the whole index into one heap Vec (~6.3 GB at 100M entries) while quiesced; snapshot/redb files have no device-identity binding.

### H. Wire protocol — `audit/raw/H-wire-protocol.md`

Strongest category. Verified with evidence: length-before-alloc enforced in both frame parser and connection loop (a `u32::MAX` frame never allocates); all batch decoders use `checked_mul` + `max_batch_size` before `Vec::with_capacity`; integer parsing is checked throughout with bounds-guarded `as` casts; streams are bound to their connection (no cross-connection hijack); fuzzing is real (3,000-iteration deterministic smoke over all 17 decoders plus a cargo-fuzz target `fuzz/fuzz_targets/decode_request.rs`).

- **[HIGH] H-1 — Unbounded concurrent blob-stream sessions per connection** (`ConnectionState.streams` has no count cap; each opens an fd + tmp file) — fd/memory exhaustion from one connection (`src/server/mod.rs:155-187`, `dispatch.rs:6354-6369`).
- **[HIGH] H-2 — Abandoned blob streams reaped only on connection close**; a pinging client holds fd/tmp/state forever (no idle-stream timer).
- **[MEDIUM] H-3** — A mutation can be applied but its response dropped on slow-reader `write_all` timeout; safe only because data-plane mutations happen to be idempotent, and that retry contract is undocumented (`mod.rs:947-977`).
- **[LOW]** `OP_MIGRATION_COMPLETE` malformed-payload arms return retryable `ERR_MIGRATION_IN_PROGRESS` instead of `ERR_PAYLOAD_MALFORMED` (`dispatch.rs:622-693`).

### I/J. Storage tiers, blobs, I/O — `audit/raw/IJ-storage-io.md`

Verified: Direct-I/O alignment is guaranteed by construction (`AlignedBuf` at every audited call site, offset+length+buffer-address all enforced); EINTR/short-I/O handling is textbook; block-device sizing validates kernel geometry; file devices are grown, never truncated.

- **[HIGH] IJ-1 — Missing external blob on the production read path returns `ERR_TX_NOT_FOUND` instead of `BLOB_NOT_FOUND`**: the F-G9-001 fix landed only in `StorageManager`, which has zero production callers (`src/ops/engine.rs:2631`).
- **[HIGH] IJ-2 — Blob-GC TOCTOU**: a blob older than the 60 s grace can be deleted between create's digest check and index registration → acknowledged EXTERNAL record with permanently lost cold data (`src/storage/blob_gc.rs` + `dispatch.rs:3661`).
- **[MEDIUM]** IJ-3 tier thresholds are advisory — production tier is the client's `FLAG_EXTERNAL_BLOB`; `INLINE_THRESHOLD` is never consulted; README's "inline if <8 KiB" is false; IJ-4 `StorageManager`/`BlobUploader` are production-dead code containing a latent metadata race; IJ-5 engine with no blob store configured silently returns *empty* cold data for EXTERNAL records; IJ-6 README documents the deleted `src/device_io/` io_uring module; IJ-7 allocator `free()` has no double-free/overlap rejection (silent freelist corruption in release on any caller bug).
- **[LOW]** `Engine::delete` never deletes the external blob (deferred to hourly GC, undocumented); `FileBlobStore::digest()` skips the per-key lock; macOS `F_NOCACHE` return ignored; Linux `BLKGETSIZE64` branch has no integration test.

### K/O. Pruning and Bitcoin/Teranode semantics — `audit/raw/KO-pruning-bitcoin.md` + `audit/raw/lua-parity.md`

**Update:** the original pass flagged the authoritative Lua reference as missing (KO-6); the user then provided it at `../teranode/stores/utxo/aerospike/teranode.lua`, and a full function-by-function parity audit was run against the real file *and the Go callers in the teranode repo* (`audit/raw/lua-parity.md`). Parity tally over 15 Lua functions: 7 match, 2 match-core-with-response-divergence, 4 diverge, 2+1 intentional-and-documented. Dispositions: **KO-4 retracted** (the real Lua `setConflicting` returns no spending data either — the Go client gathers it via GetSpend; spec §3.10 is wrong, not the code); **KO-8 closed in Rust's favor** (Lua uses `>` exactly like Rust; spec's `>=` is the outlier); **KO-9 upgraded LOW→HIGH** (became LP-1 below); KO-5 stands, sharpened (the reference flow is fatal-on-failure client-side, TeraSlab made it warn-only).

Verified against the real reference: coinbase maturity operator identical; `spend` precedence, freeze/ALREADY_FROZEN/SPENT-with-payload, setConflicting, preserveUntil, setLocked semantics all match; `incrementSpentExtraRecs` (opcode 255) is an intentional, safe-direction no-op vs Lua's clamp; the missing CREATING flag is documented-intentional (spec §2.2, covered by LOCKED). Reorg mined-status recompute on `MarkLongestChainBatch` is correct.

- **[HIGH] KO-1 — Expired preservations are never processed.** No code anywhere clears `preserve_until` or compares it to current height; `OP_PROCESS_EXPIRED_PRESERVATIONS` is actually the DAH delete sweep and *skips* all preserved records — preservation is permanent, store bloat is unbounded, spec §3.18 Phase 3 unimplemented (`dispatch.rs:6076`, `engine.rs:3887`).
- **[HIGH] KO-2 — DAH sweep re-validation excludes the CONFLICTING DAH-setting path** (`dispatch.rs:6083-6088` vs `delete_eval.rs:89`): conflicting transactions are never deleted and their stale DAH entries are re-scanned every block forever. Re-confirmed against the Lua: the reference's intent (lines 985-995) is that conflicting losers *are* DAH'd and deleted.
- **[HIGH] LP-1 — `unspend` violates the reference's idempotent-OK contract.** The real Lua (484-540) takes `expectedSpendingData` and on mismatch/nil does a *silent no-op returning STATUS_OK* (still running DAH housekeeping); it only clears the slot when the caller owns the spend. Rust errors with `InvalidSpend`/`Frozen` instead (`engine.rs:1525-1535`). The Go callers (`process_conflicting.go:193`, `un_spend.go:200-208`) abort on any error — so Teranode's conflict-resolution flow, which legitimately submits unspends whose stored spending data may belong to the winner, wedges deterministically and retry-proof against TeraSlab. **Fix:** match the Lua: mismatch → no-op OK; mutation only on ownership match.
- **[HIGH] LP-2 — `setMined` wire response drops block IDs.** The engine computes them but `dispatch.rs` returns an errors-only payload; Lua returns blockIDs unconditionally (line 633), spec §3.6 mandates them, and `txmetacache.SetMinedMulti` consumes the map. Also missing: the Lua #1037 `childCount = totalExtraRecs` on every mine, which the Go client uses to unlock pagination records (mitigated by TeraSlab internalizing pagination, but the response contract is unmet — verify the client tolerates it).
- **[MEDIUM]** KO-3 sweep re-validates without the stripe lock and `engine.delete` never rechecks `preserve_until` — a concurrent acked preservation can be silently overridden and the record deleted; KO-5 conflicting-children tracking is warn-only with a hard u8(255) cap — appends silently drop while the op returns OK (reference treats this update as fatal-on-failure); LP-3 `reassign` omits the Lua's `recordUtxos + 1` (line 945) — the reference makes reassigned records permanently un-DAH-able, TeraSlab deletes them retention blocks after final spend (audit trail and reorg evidence gone; verified *not* a live-UTXO loss); LP-4 freeze→unfreeze wipes the reassignment cooldown (Rust stores it in slot spending_data, which freeze/unfreeze overwrite; Lua's `utxoSpendableIn` bin survives freeze cycles) — cooldown bypass on reassigned outputs; LP-5 `signal`/`childCount` computed then discarded at dispatch (`dispatch.rs:3032`) — spec §3.4/§10.4 contract unmet, PRESERVE blob handling unverified.
- **[LOW]** height-0 sentinel collision; spec §3.4 says `>=` where both Lua and Rust use `>` (spec stale); README mislabels `block_height_retention`; stale cached `delete_at_height` write-back after failed `sync_index_cache`; FROZEN_UNTIL check scoped to unspent slots (Lua checks before spent-state — different error precedence on spent+cooldown slots); `unfreeze`/`reassign` on an unspent UTXO return `UTXO_NOT_FROZEN` vs Lua's `UTXO_INVALID_SIZE`; the Lua's own `getErrorCodeFromMessage` at lines 813/888 is an undefined global (reference-side runtime bug — Rust correctly does not replicate it); `specs/teranode.lua` is still missing from the teraslab repo itself (KO-6 partially resolved — copy the file in or fix the CLAUDE.md/spec references).

### L/M. Resource limits, DoS, observability — `audit/raw/LM-dos-observability.md`

Verified: idle/slow-loris controls present and on by default (30 s read/write timeouts, 60 s frame-assembly deadline, per-connection thread isolation, bounded buffers); per-op metrics are attempted-once / succeeded-xor-failed with no replication double-count; Prometheus label cardinality bounded; admin mutating endpoints are bearer-token-gated and default-off (better than expected).

- **[HIGH] LM-1** — Same root as H-1: no cap on concurrent streaming-upload sessions per connection (millions of half-open `OP_STREAM_CHUNK` sessions, each holding an fd + hasher).
- **[MEDIUM]** LM-2 the data plane is entirely unauthenticated with no rate limiting (HMAC covers inter-node opcodes only; mitigated by loopback default + `enable_remote_bind` gate — partly documented design, still flagged for a money store); LM-3 `/health/ready` replica-lag verdict cached 500 ms in a process-global static.
- **[LOW]** admin endpoints share the HTTP port with `/metrics` rather than a dedicated bind.

### N. Test infrastructure — `audit/raw/N-test-infrastructure.md`

- **[HIGH] N-1 — The only real-SIGKILL crash/chaos/split-brain tests (Docker scenarios 12–16, including `scenario_15_crash_recovery_correctness` and `scenario_16_chaos`) sit in `weekly`/`release` tiers of `teraslab-tests/run_all.sh` that no workflow ever invokes.** CI runs tier `pr` (01–03), nightly runs 01–11+17, `release.yml` runs no Docker E2E at all. The exact failure modes this audit found (checkpoint durability, migration crash, replica divergence) live where only these never-run tests could catch them.
- **[MEDIUM]** N-2 in-tree crash injection is in-process only, at hand-picked `SyncPoint`s; 7 of 12 mutation ops have only hand-built-redo replay tests; N-3 the cargo-fuzz target is never run in CI; N-4 `property_utxo.rs` is a genuinely strong model-based differential test but its generators never produce wrong `utxo_hash`, coinbase, FrozenUntil, or the 0xFF sentinel, and there's no crash-replay property; N-5 the entire TCP/cluster/stress/property surface runs the Memory index backend exclusively — no test ever runs a server on redb; N-6 error codes 32/33/34 have zero behavioral tests (only constant pins).
- **[LOW]** 6 bare `.is_err()` assertions violate the project's own banned-pattern rule; opcode 255 untested; v2 `OP_HELLO` never crosses a socket; empty-batch (count=0) semantics unpinned for every batch op.

---

## 3. Test coverage matrix

Full matrix: **`audit/coverage-matrix.md`** — every opcode crossed against happy path, each error code, batch boundaries, crash mid-op, replication failure, migration fence, single-node vs cluster.

Headline: **121 gradable cells → 40% ✅ / 31% ⚠️ / 29% ❌.** Best-covered: `SpendBatch` (every dimension, including wire error payloads, migration fence, strict-replication failure). Worst: `PreserveTransactions`/`ProcessExpiredPreservations` (dispatch-unit only — consistent with KO-1), opcode 255 (nothing), `Unfreeze`/`SetConflicting`/`PreserveUntil` (wire happy-path only). Cross-cutting gaps: migration-in-progress is tested for only 3 of 15 fenced ops; crash-mid-op for 5 of 12 mutating ops; cluster routing for ~5 ops.

## 4. Error-code triggerability matrix

Full matrix: **`audit/error-codes.md`**.

24 of 38 codes+statuses (63%) have true wire-level proof — `tests/error_code_conformance.rs` is a real wire path (real server, real `TcpStream`, exact payload bytes) but covers only 4 codes; the workhorse is `tests/server_tcp.rs::tcp_error_code_triggerability_core_item_errors` (codes 1–13 with payload assertions). **Codes 32, 33, 34 have no behavioral test at all; codes 21–26 and `STATUS_DEGRADED_DURABILITY` (5) are exercised in-process only** — and status 5 is additionally unreachable in any valid configuration (D-9), so the documented degraded-durability client contract has never been observed by any client, ever.

## 5. Spec-vs-implementation diff

Full diff: **`audit/raw/spec-vs-impl.md`** — ~140 claims checked, ~118 verified (with code+test citations), 13 divergent, 5 undocumented-behavior findings.

Highest-impact divergences:
- **[HIGH]** Every README TOML example binds `0.0.0.0` without the required `enable_remote_bind = true` — copy-pasting the quick-start produces a server that refuses to start (`config.rs:1143`); the documented default bind is wrong (actual: loopback).
- **[HIGH]** All documented `/debug/*`, `/admin/*`, `/ws/top` endpoints and 13 of 18 CLI commands return 404 by default — gated behind `enable_admin_endpoints` + `admin_token`, neither of which appears in the README config reference.
- **[HIGH]** `TxMetadata` documented as 256 bytes in four places; compile-time asserted **320** bytes (`record.rs:719`).
- **[MEDIUM]** `best_effort` rejected at startup whenever RF>1, making documented status 5 unreachable; redo log documented as "circular buffer" but implemented as linear+reset; README documents the deleted `src/device_io/` module; blobstore threshold comment says >1 MiB, actual >8 KiB; three-way conflict on index bucket size (README 72 B / code 64 B / spec ~16 B); spec §10's wire protocol (magic/version/CRC, Heartbeat=255) doesn't match the implementation at all.
- **Undocumented but live (operators must know):** `strict_auth` (default true, but its own doc comment says false — `config.rs:633` vs `:784`); `max_connections_per_ip` (64); `max_inflight_request_bytes` → `RATE_LIMITED`; `replica_lag_warn_threshold_ops` fails `/health/ready`; a third `file_backed` index backend; 16 MiB frame / 4 MiB item wire caps; interrupted `import-index` leaves a sentinel that blocks the next startup.

Verified correct (notable): opcode/error/status tables match the code exactly; shard formula; peak-quorum persistence including restart test; redb fail-closed; ack-policy auto mapping (auto→write_all at RF≤2, write_majority at RF≥3); **no dead config knobs** — every README knob is read and used.

## 6. Dead code and TODO inventory

Full inventory: **`audit/raw/dead-code-inventory.md`**.

Zero `todo!()`/`unimplemented!()` stubs, zero TODO/FIXME/HACK comments, the single `unreachable!` is test-only — the project's banned-pattern rules are genuinely enforced. 216 `unwrap`/`expect` hits: 166 infallible `try_into` on validated slices, ~30 policy lock-poison unwraps, ~16 proven-invariant expects; 3 LOW findings. 224 narrowing casts audited: all decode-side bounds-guarded, 0 findings. ~111 `unsafe` blocks: ~65 documented; 3 LOW findings for missing per-site SAFETY comments (engine.rs ×11, hashtable.rs ×15, swim.rs:486).

The one finding above LOW:
- **[HIGH] DC-1 — Rollback slot-restore `write_utxo_slot` failures silently ignored** (`src/server/dispatch.rs:2198,2248`): if a device write fails during replication-failure compensation, the local store silently diverges from what was reported rolled back.

Notable dead code: `migrate_single_shard` (reference impl, marked); `send_delta_ops` has a stale **wrong** `#[allow(dead_code)]` (it is live in the batch path); `persist_peak_cluster_size` dead except tests; `StorageManager` instance API + `BlobUploader` test-only/unwired (see IJ-4); `ReplicationManager` orchestrator test-only (see D).

## 7. Action plan

### Milestone 1 — things that can lose UTXO data (do these before any production traffic)

1. **B-1:** sync the data device before redo reclamation in `perform_checkpoint_with_reset_guard`; add the missing sync in `SlotAllocator::persist`. Then add a fault-injection sync point inside checkpoint and a test on a device whose un-synced writes are actually dropped on simulated power loss (extend `MemoryDevice` with a volatile-cache mode).
2. **G-1:** redb `Durability::Immediate` on commit (or explicit flush before the checkpoint fence); gate `compact_prefix_through` on backend flush success.
3. **B-2 + G-3:** fail closed on torn allocator header and on invalid-size index file; both must route to device-scan rebuild or operator intervention, never silent-fresh state.
4. **F-1:** reorder migration completion — commit the shard table before lifting the fence/closing dual-write; re-check the fence inside the write path (not just dispatch entry) or drain in-flight ops at fence time.
5. **A-1:** stripe-lock `create`; make index insert reject existing live keys; fix the `>= 1` assertion. (The failing `g2_delete_race` test then becomes the regression guard.)
6. **D-1/D-2/D-3:** give the replication stream a real per-replica contiguous sequence; receiver rejects (NAKs) gaps instead of high-water-mark-skipping; fix the catch-up `+1`. This is one design fix, not three patches.
7. **IJ-2:** make blob-GC re-check index registration under the create path's per-key lock (or extend the grace handshake to cover the digest-check→register window).

### Milestone 2 — distributed correctness

8. **D-5:** pass the cluster secret to catch-up connections (one-line class of fix; add a secured-cluster catch-up integration test).
9. **D-6:** count `write_majority` ACKs per key, not per fan-out union.
10. **E-2:** enforce `cluster_id` + quorum safety in `handle_commit` (same guards as propose).
11. **E-1:** key the SWIM replay window by (NodeId, incarnation) or reset on rejoin; GC `seen_seq`.
12. **C-1:** move replication fan-out outside the engine-wide visibility barrier (or make the barrier scope per-stripe); add a two-node bidirectional-write stress test.
13. **C-2:** make `voted_term` updates atomic CAS.
14. **F-2:** route migration-applied lifecycle fields through the secondary-index APIs.
15. **D-4/DC-1:** make compensation failures durable + repairing (don't clear the intent until replicas confirm rollback; don't swallow slot-restore write errors).
15a. **LP-1:** match the Lua unspend contract — spending-data mismatch is a no-op `STATUS_OK`, not `InvalidSpend`; without this, Teranode conflict resolution deterministically wedges.
15b. **LP-2:** return block IDs (and mine-time `childCount`) in the `setMined` wire response per spec §3.6.

### Milestone 3 — make the test suite able to catch milestones 1–2

16. **N-1:** wire Docker scenarios 12–16 (SIGKILL crash, chaos, split-brain) into nightly CI. This is the single highest-leverage test change.
17. **N-5/G-6:** run the engine/dispatch/TCP suites against redb and file_backed backends (parametrize the harness).
18. **B-8/N-2:** add fault-injection sync points inside checkpoint/compaction/persist; add a volatile-cache device model so "fsync missing" bugs are testable; add randomized kill-point crash sweeps per mutating op.
19. **N-4:** extend `property_utxo.rs` generators (wrong utxo_hash, coinbase, FrozenUntil, 0xFF sentinel) and add a crash-replay property.
20. **N-3:** run the cargo-fuzz target in CI (even 5 min/night).
21. **F-3:** crash-mid-migration integration test (kill during streaming, restart, verify no loss/duplication).

### Milestone 4 — contract integrity and hygiene

22. **KO-1/KO-2:** implement preservation expiry; include the CONFLICTING path in the DAH sweep re-validation (with stripe lock — KO-3).
22a. **LP-3/LP-4/LP-5:** decide and document the reassign DAH-retention divergence (or add the `recordUtxos+1` equivalent); persist the reassignment cooldown outside slot spending_data so freeze cycles can't wipe it; either wire `signal`/`childCount` into responses or amend spec §3.4/§10.4.
23. **IJ-1/IJ-5:** return `BLOB_NOT_FOUND` from the engine read path; error (not empty) on EXTERNAL records with no blob store configured.
24. **H-1/H-2/LM-1:** cap concurrent streams per connection; add an idle-stream reaper.
25. **B-4:** make spend/unspend replay idempotent on `spent_utxos` (recompute from slots, don't increment).
26. **Spec/README sweep:** fix the 13 divergences (bind examples, admin gating, 320-byte TxMetadata, circular-buffer wording, device_io removal, status-5 contract), document the security-relevant undocumented knobs, restore or delete the `specs/teranode.lua` reference (KO-6), and delete or wire the dead `StorageManager`/`BlobUploader`/`ReplicationManager` code (Rule: a 3,056-line tested orchestrator that production bypasses is a standing trap for the next maintainer).
27. Remaining MEDIUM/LOW items per category reports.

---

## Unauditable areas (explicit)

- **Lua parity (KO-6): resolved post-audit.** The reference was provided at `../teranode/stores/utxo/aerospike/teranode.lua` and a full function-by-function parity audit (including Go-caller impact analysis) is in `audit/raw/lua-parity.md`. The file is still absent from the teraslab repo itself, which CLAUDE.md and the specs reference — copy it in (with its upstream commit hash) or fix the references.
- **Linux raw-block-device behavior:** audit ran on macOS; the `BLKGETSIZE64` path and O_DIRECT-on-Linux semantics were verified by code reading only (IJ findings note the missing Linux integration test).
- **Performance claims** (10M ops/sec, replication MB/s): not benchmarked in this audit; README honestly labels them as targets.
