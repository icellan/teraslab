# TeraSlab Bulletproofing Audit

**Scope:** Every claim in `README.md`, `docs/`, and `phases/` cross-checked against the code under `src/` and the test suite under `tests/`. Goal: *would I stake real money on this UTXO store under adversarial production load?*

**Date:** 2026-05-06
**Branch:** `main` (commit `832bd7c` plus uncommitted edits)
**Audited by:** Claude (six parallel agents over `src/`, `tests/`, `docs/`, `phases/`, plus direct reads)

This audit is independent of the existing internal `docs/TERANODE_PRODUCTION_READINESS_GAPS.md` (2026-05-03). Where this audit confirms or strengthens an existing gap, it is noted; where it finds *new* problems, that is called out. Findings are technical; severity is justified by user-visible failure mode (lost UTXO, accepted double-spend, silent corruption, brick-the-master).

---

## Build / test baseline

| Probe | Result |
|---|---|
| `cargo build --release` | **clean** |
| `cargo clippy --all -- -D warnings` | **clean** |
| `cargo test --all` | **3 failed**, 1480 passed, **1 ignored**, 0 measured |
| Source LOC | 98,234 lines across 70 `*.rs` files |

### Failing tests (live in main)
1. `index::tests::rebuild_fails_on_corrupted_magic_in_allocated_region` — `src/index/mod.rs:1127` — assertion `detail.contains("invalid metadata magic")` fails because `TxMetadata::from_bytes` (`src/record.rs:557`) now rejects on **CRC mismatch** before the magic check at `src/index/mod.rs:393` is reached. Detail returned is `"corrupt metadata at allocated offset {offset}: {e}"`. **The test expectation is stale; either rewrite the test to corrupt only the magic without breaking CRC, or update the assertion to match the actual diagnostic.**
2. `index::tests::rebuild_secondary_fails_on_corrupted_allocated_record` — same root cause, `src/index/mod.rs:1191`.
3. `index::backend::tests::rebuild_redb_fails_on_corrupted_magic_in_allocated_region` — same root cause, `src/index/backend.rs:938`.

### Ignored test
`src/cluster/coordinator.rs:7505` — `#[ignore] // TODO: rewrite for pipelined migration flow`. Per the project's CLAUDE.md absolute rules, every `#[ignore]` is a finding unless its justification is documented; the inline comment is too thin (no rewrite tracker, no link, no expected schedule).

### Hazards inventory (repo-wide, non-test paths)
- `todo!()` / `unimplemented!()` / `unreachable!()` in production code: **0** (compliance win)
- `panic!` in production paths: 1 — `src/server/dispatch.rs:7301` is inside a `#[test]` body (false positive); `src/cluster/shards.rs:1256` has a defensive `panic!` (intentional); the rest are inside `match` arms of `#[test]` fns
- `tokio::spawn` fire-and-forget candidates: 1 (`src/server/http.rs:1510`, but joined via `handles.push`)
- `tokio::task::spawn_blocking` for replication fan-out: 1 (`src/server/dispatch.rs:1289`)
- `unsafe` blocks: ~80, all in `src/device.rs` (raw I/O, ioctl), `src/io.rs` (mmap targeted writes), `src/record.rs` (repr-packed copy), `src/config.rs` (test-only env wrappers). Each `unsafe` in `src/io.rs` carries a `# Safety` line; the contract it states ("caller holds the per-tx stripe lock") is **violated by every hot read path** — see BC-02.

---

## Executive summary — ten most dangerous gaps

These are ranked by blast radius. All have file:line references in the per-category sections and the per-category audit files under `audit/raw/`.

1. **`Engine::spend` silently swallows on-disk write errors at five sites and returns `Ok` to the client.** Pattern: `if let Err(e) = self.write_slot_fast(...) { tracing::warn!(...); }` then proceed to mutate metadata, return `Ok(SpendResponse {...})`. Sites: `src/ops/engine.rs:1013, 1042, 1066, 2920, 2948` (single-spend, idempotent-respend, and `ValidatedSpend::apply` for batch). On NVMe EIO / mmap fault / partial sector / device-full, the engine reports a successful spend while the slot on disk remains UNSPENT. A different transaction can then **double-spend the same UTXO**. No test exercises a write-failure injection at these sites. (A-01 — CRITICAL, new finding.)
2. **`Unspend` does not validate `spending_data` — wire format omits it entirely.** `UnspendRequest` (`src/ops/unspend.rs:9-22`) has no `spending_data` field. `WireSlotItem` (`src/protocol/codec.rs:407-411`) is `(txid, vout, utxo_hash)` — 68 bytes, no spending_data. `Engine::unspend` (`:1085-1181`) only checks the frozen sentinel; otherwise it overwrites the slot with `UtxoSlot::new_unspent`. **Anyone with public on-chain data `(txid, vout, utxo_hash)` can erase any spend they did not author.** Inverse-of-spend invariant is violated. (A-04 — CRITICAL, new finding.)
3. **`spend_multi` increments `meta.spent_utxos` by validation-time count even when slot writes silently failed.** `ValidatedSpend::apply` (`src/ops/engine.rs:2899-2950`) sets `metadata.spent_utxos = wrapping_add(spent_count)` unconditionally; per-slot writes drop errors per A-01. Result: counter says N spent, fewer than N slots actually have status SPENT on disk. DAH evaluation depends on `spent_utxos == utxo_count` → record can be flagged "all spent" when it isn't, causing **premature pruning** of UTXOs still spendable. Replication ships metadata that disagrees with the slot bytes. (A-03 — CRITICAL, new finding.)
4. **No production redo-log checkpointing — log fills and bricks the master.** `RedoLog::checkpoint()` / `advance_checkpoint()` / `reset()` have **zero callers outside `mod tests`** (`src/redo.rs:1185, 1238, 1258`). Default `redo_log_size = 64 MiB`; ~85 bytes per `Spend` redo entry → log fills in ~750k mutations (sub-second at the 10M ops/sec target). After fill, every mutation returns `ERR_INTERNAL` until process restart. (BC-01, gap #3 — confirmed.)
5. **Inter-node TCP frames are unauthenticated.** `cluster::auth::sign`/`verify` is wired only into SWIM UDP (`src/cluster/swim.rs:434, 845, 881`). `OP_TOPOLOGY_PROPOSE/VOTE/COMMIT`, `OP_REPLICA_BATCH`, `OP_MIGRATION_COMPLETE`, and `OP_MIGRATION_BATCH_COMPLETE` are sent and received as plain `RequestFrame`. Anyone reachable on the binary protocol port can forge a topology commit, replicate fake ops, or lift a migration fence. (EF-01, D-20, gap #1 — confirmed.)
6. **Hot read paths violate the documented stripe-lock safety contract — data-race UB on every concurrent read.** `Engine::lookup` (`src/ops/engine.rs:673`), `read_metadata` (`:2784`), `read_slot` (`:2807`), `lookup_cached` descend into `unsafe { io::read_metadata_direct(..) }` without taking the per-tx stripe lock that `src/io.rs:206` requires. CRC catches the visible torn read but the contract is broken; `cargo miri` would flag it. (BC-02 — new finding.)
7. **Concurrent `unspend`/`set_mined`/`freeze`/`reassign`/`set_locked`/`set_conflicting`/`preserve_until`/`delete` batches compute redo `new_spent_count` (and friends) OUTSIDE the per-tx stripe lock.** `src/server/dispatch.rs:2620-2640` reads `pre_spent` without locking; two concurrent unspends race the counter and persist redo entries that are wrong by the time they replay. After a crash, `recovery::replay_unspend` (`src/recovery.rs:586`) overwrites `meta.spent_utxos` with the stale snapshot count. (BC-04 — new finding; the dispatch comment at `:2561` *acknowledges* the pattern but assumes idempotency that is not satisfied for counter-bearing entries.)
8. **`alive_node_count` excludes self, causing false `NO_QUORUM` rejections in healthy clusters.** `src/cluster/coordinator.rs:5860` counts `node_addrs` members; `node_addrs` never contains self in production (`src/cluster/swim.rs:454` returns before peer registration). In a 3-node cluster losing 1 peer, the function returns 1 instead of 2 → the surviving 2-node majority is rejected as `NO_QUORUM`. The unit test only passes because it manually inserts self into the test's slice. (EF-02 — new finding.)
9. **`reassign` does not enforce `LOCKED`, `CONFLICTING`, or coinbase maturity flags.** Per spec, every spend path including via `reassign` must check these flags. `Engine::reassign` (`src/ops/engine.rs`) only validates the FROZEN_UNTIL cooldown. A reassign through a locked/conflicting transaction therefore bypasses the protections that `Spend` honors. (A-09 — new finding.)
10. **Replication intent is started AFTER local apply, and the replica silently drops `write_metadata` errors during apply.** Two intersecting bugs: (a) `src/server/dispatch.rs:1250` calls `begin_replication_intent` inside `replicate_all_ops`, after the engine has already applied — crash between local apply and intent fsync leaves a local-only mutation with no pending-replication marker (D-19, gap #5). (b) `src/replication/receiver.rs:684, 1127` use `let _ = io::write_metadata(...)` during replica apply — replica ACKs to the master while local state silently diverges from what was acknowledged (LMNH-31). The combination means an apparent "replicated" mutation can be on neither the master's pending list nor the replica's disk.

The eleventh through twentieth findings are also CRITICAL or HIGH and are not buried — see the full per-category lists below. In particular:
- **A-06** — `recovery::replay_spend` and `replay_unspend` (`src/recovery.rs:520-592`) drop metadata write errors with `let _ =` and never recompute generation, `LAST_SPENT_ALL`, or DAH. Post-recovery state is structurally inconsistent with a non-crashed run; replicas resyncing from generation watermark will not see the replayed change. (HIGH, new.)
- **A-08** — `freeze`/`unfreeze` (`src/ops/engine.rs:2161, 2202`) don't bump `meta.generation`, don't write metadata back, and don't sync the index cache. Subsequent fast-path ops read stale `tx_flags` and miscompute DAH. (HIGH, new.)
- **A-12** — `preserve_until` writes to disk but never calls `sync_index_cache`. Cached `tx_flags` does not get the `HAS_PRESERVE_UNTIL` bit; `set_mined`/`set_conflicting`/`set_locked` consult the cache, conclude `has_preserve = false`, and **bypass preserve_until protection on the fast path**. (HIGH, new.)
- **A-07 / A-10** — `Pruned` slots lose their preserved spending_data on the wire (engine returns `Pruned { offset }`, dispatch maps to `ERR_INVALID_SPEND` with empty payload). `FROZEN_UNTIL` similarly returns `vec![]` instead of the 4-byte `spendable_at_height`. README's "error data: 4-byte required height" claim is therefore wrong on the wire for `FROZEN_UNTIL`. (HIGH/MEDIUM, new.)
- **A-05** — `pre_allocate_create` + `create_at_offset` leak device space on `DuplicateTxId` race; neither dispatch nor engine calls `allocator.free(record_offset, base_size + cold_len)` on this branch. (HIGH, new.)
- **REDIRECT has no hop count, TTL, or loop counter.** `RouteDecision::RedirectTo` carries `shard_table_version` internally but never serializes it. (EF-09 — new finding.)
- **`device_io::IoUringBackend` and `device_io::SyncFallback` are dead code.** README's "io_uring fast path" is currently false. (IJK-04 — new finding.)
- **`OP_MIGRATION_COMPLETE` is unauthenticated and zero-record completions skip manifest verification.** (`src/server/dispatch.rs:567-571, 628-634`.) Combined with EF-01, an attacker can declare any shard migration complete and silently lose data. (EF-12 — new finding.)
- **`MarkLongestChainBatch` emits no `ReplicaOp`.** Reorg-driven DAH/unmined-index updates never reach replicas. Compensation rollback also doesn't restore the DAH index. (IJK-20, IJK-22 — new findings.)
- **LMNH-07** — `/health/ready` is hard-coded to `true` at boot in `src/bin/server.rs:894` and never consults `cluster.is_ready()`. Load balancers will route traffic to a node that immediately rejects it during cluster bootstrap.
- BC-03 (UTXO slot torn writes are undetectable, no slot CRC), BC-30 (hash table bucket bytes can tear under concurrent writers).
- GH-04 / GH-G1 (unchecked multiplications in `OP_MIGRATION_COMPLETE` and snapshot deserialize allow large allocations from poisoned input).
- IJK-01 (external-blob `content_hash` is permanently zero on the sync create path so reads always reject).
- IJK-08 (no GC for orphaned blobs — disk fills with failed creates / aborted replications).
- LMNH-01 (no write timeout on response stream — slow-reader pins a server thread forever).
- LMNH-08 (`/admin/quiesce`, `/admin/drain/N`, `/admin/rebalance` have no authentication once the registration flag is on).

---

## Spec-vs-implementation diff

### Documented but not implemented (or implemented differently)

| Claim | Reality | Reference |
|---|---|---|
| README "io_uring fast path" / `device_io/` module | Dead code; production uses `libc::pread`/`pwrite` one syscall per op | IJK-04 |
| README `ack_policy = "auto"`, `"write_all"`, `"write_majority"`, `"best_effort"` (4 values) | `AckPolicy` enum has only `WriteAll` and `WriteMajority`. `"auto"` and `"best_effort"` are resolved in `config.rs::resolved_ack_policy`. **Unknown strings (typos like `"writeall"`) silently fall through to "auto" behavior** — `src/config.rs:497` `_ => match replication_factor { ... }`. There is no `ConfigError::InvalidAckPolicy` variant. | direct |
| README error codes 0–20 + 255 | Code defines additional error codes 16–18 (stream/blob errors, also documented), 21 (`MIGRATION_MANIFEST_REQUIRED`), 22 (`MIGRATION_MANIFEST_MISMATCH`), 23 (`TOPOLOGY_PERSIST_FAILED`), 24 (`STALE_EPOCH`), 25 (`CLUSTER_NOT_READY`), 26 (`INDEX_DEGRADED`). README is missing 21–26. | `src/protocol/opcodes.rs:195-236` |
| README opcode list 1–12, 20–21, 30–32, 100–102, 200–201 | Code defines additional opcodes 103 (`GET_COMMITTED_TOPOLOGY`), 104 (`ADMIN_DIAGNOSE_KEY`), 105 (`PARTITION_VERSION_REPORT`), 106 (`ADMIN_CLUSTER_HEALTH`), 240 (`REPLICA_BATCH`), 241 (`REPLICA_ACK`), 242 (`MIGRATION_COMPLETE`), 243 (`MIGRATION_BATCH_COMPLETE`), 250 (`HEARTBEAT`), 251–253 (topology), 255 (`INCREMENT_SPENT_EXTRA_RECS`). README is silent on all of these — internal opcodes are not documented even where they impact migration / cluster admin. | `src/protocol/opcodes.rs:32-162` |
| README single STATUS_OK / STATUS_ERROR / STATUS_NOT_FOUND / STATUS_REDIRECT (4 values) | Code defines additional `STATUS_PARTIAL_ERROR = 4` and `STATUS_DEGRADED_DURABILITY = 5`. | `src/protocol/opcodes.rs:249-270` |
| README "redb falls back to in-memory if corrupt" | **Not implemented by design.** Code fails closed and preserves the corrupt file for forensics. The README claim is wrong. (Defensible behavior — but the README must be updated.) | GH-G5, `src/index/backend.rs` |
| `cluster_secret` "authenticates inter-node TCP" (per `src/config.rs` and `src/cluster/auth.rs` doc comments) | Only authenticates SWIM UDP. TCP frames go plain. | EF-01, D-20, gap #1 |
| README "peak cluster size is persisted to disk" | Confirmed: `src/cluster/coordinator.rs` writes `committed_members` to a `*.topo` file. **But** losing the `*.topo` file (operator error / disk swap) lets a remnant re-bootstrap as a fresh single-node cluster — the `committed_members.len() <= 1` clause is the only guard. | EF-04 |
| README cluster "rebalances automatically" | Migration zero-record fast-path skips manifest verification (`src/server/dispatch.rs:567-571`). No source authentication. An adversary on the cluster network can declare any shard migrated. | EF-12 |
| README "stream is bound to the connection that started it" | Confirmed via per-`ConnectionState` `streams: HashMap<txid, ActiveStream>` with `Drop` cleanup — but **no integration test verifies cross-connection isolation** (all coverage is unit-level on the dispatch helper). | GH-08 |
| README "replication ACK policy `write_majority`" | For RF=2 with `write_majority`, the formula yields `required_ack_count = 0` (`src/replication/manager.rs:487-496` returns `floor(rf/2)+1 - 1 = 0`). Effectively single-node durability under any replica loss. | D-02 |
| README `replica_lag_check_interval_secs` config option | Dead code. `spawn_lag_monitor` (`src/replication/durable.rs:679`) has zero callers. The config field is read nowhere. The `AckTracker` is updated on every ACK but no consumer reads it. | D-01 |
| README "Setting `replication_degraded_mode = "best_effort"` … " | Only allowed for RF≤1 (`src/config.rs:523`). RF>1 + `best_effort` is rejected at validation. | matches README |
| Compaction / pruning during reorg | `MarkLongestChainBatch` emits no `ReplicaOp` (IJK-20). Pruning during reorg can therefore diverge between master and replica without any protocol-level signal. | IJK-20 |
| README admin endpoints (`/admin/quiesce`, `/admin/drain/N`, `/admin/rebalance`) | **Unauthenticated** beyond the registration flag `enable_admin_endpoints`. Once enabled (which any operator must do for normal admin work), any HTTP client on the bind address can call them. | direct, `src/server/http.rs:142-147, 965-973, 1116, 1127` |

### Documented but clean (verified working as described)

- `shard = u16_le(txid[0..2]) & 0x0FFF` — confirmed at `src/cluster/shards.rs:314-317`. Mask, byte order, and shard count (4096) all correct.
- Length-prefix max enforced before allocation — confirmed at `src/server/mod.rs:240-250` with `MAX_FRAME_SIZE` ceiling and pre-resize check.
- `max_batch_size` enforced in batch decoders — `validate_batch_count` is centralized and called before every `Vec::with_capacity`.
- Per-connection stream isolation — `ConnectionState` is per-connection; aborted on drop.
- `TxMetadata` carries CRC32 — `src/record.rs:540-545, 569-572`. Caught for torn metadata writes; **does not cover the 69-byte UTXO slots** (BC-03).
- Allocator journals `AllocateRegion` / `FreeRegion` BEFORE returning offsets — `src/allocator.rs:455-564`.
- Replication wire protocol versioned (V1 / V2) — explicit `UnknownVersion` rejection on unknown bytes.
- Compensation captures real before-images for `unset_mined`, `reassign`, `prune` — recent gap #8 work, verified.
- `MAX_FRAME_SIZE` on replication / migration TCP — same ceiling.
- `Robin Hood probe distance` bounded; high-load-factor tested.
- Snapshot atomicity (tempfile + rename + fsync); snapshot format versioned.
- Loopback bind defaults; `validate_safe_defaults` rejects non-loopback without `enable_remote_bind = true`.
- RF>1 requires non-empty `cluster_secret` (config validation).
- `MultipleDevicePaths` rejected at config validation — closes gap #10 by validation rather than implementation.

---

## Findings by category

**Total: ~258 findings across 8,568 lines of per-category reports under `audit/raw/`.** The rest of this document indexes those files. Each linked file contains the full set of findings with reproduction steps and suggested fixes.

- **Category A — UTXO correctness invariants:** `audit/raw/category_A_utxo_correctness.md` (3 CRITICAL, 7 HIGH, 5 MEDIUM, 18 LOW = 33 findings). **Also**: `specs/teranode.lua` referenced by CLAUDE.md is missing from the repo; Lua-parity claims could not be cross-checked.
- **Category B+C — Crash recovery & concurrency:** `audit/raw/category_BC_recovery_concurrency.md` (4 CRITICAL, 15 HIGH, 22 MEDIUM, 22 LOW = 82 findings)
- **Category D — Replication:** `audit/raw/category_D_replication.md` (22 findings)
- **Category E+F — Cluster, quorum, sharding, migration:** `audit/raw/category_EF_cluster_migration.md` (30 findings)
- **Category G+H — Index backends & wire protocol DoS:** `audit/raw/category_GH_index_protocol.md` (19 + 17 sub-findings)
- **Category I+J+K — Storage tiers, I/O layer, pruning:** `audit/raw/category_IJK_storage_io_pruning.md` (5 HIGH, 6 MEDIUM, 5 LOW + sub-findings = 23 findings)
- **Category L+M+N — DoS limits, observability, test infra, repo-wide hazards:** `audit/raw/category_LMN_safety_obs_tests.md` (5 HIGH, 6 MEDIUM, 17 LOW, 4 INFO)

### Severity-ranked highlights from the per-category files

#### CRITICAL
- A-01 — Spend silently swallows on-disk write errors at 5 sites; client sees `Ok` while UTXO remains UNSPENT.
- A-03 — `spend_multi` increments counter even when slot writes silently fail; metadata diverges from slot bytes.
- A-04 — Unspend has no `spending_data` field; anyone with public on-chain triple can erase any spend.
- BC-01 — No production redo-log checkpointing.
- BC-02 — Hot read paths violate stripe-lock contract (data-race UB).
- BC-04 — Concurrent unspend/freeze/etc. compute redo payload outside lock.
- EF-01 — Inter-node TCP unauthenticated.
- D-20 — Replication socket lacks `cluster_secret`/TLS auth (same root cause as EF-01).

#### HIGH
- A-05 (pre_allocate_create leak on DuplicateTxId), A-06 (recovery replay swallows metadata write + skips derived state), A-07 (Pruned drops spending_data on wire), A-08 (freeze/unfreeze don't bump generation or sync cache), A-09 (reassign skips LOCKED/CONFLICTING/coinbase checks), A-10 (FROZEN_UNTIL drops 4-byte payload), A-12 (preserve_until doesn't sync cache → fast path bypasses protection).
- BC-03, BC-05 (gen wrap), BC-06/07 (no memory ordering on direct reads), BC-09 (`append_conflicting_child` no redo entry), BC-10 (allocate-before-redo), BC-11 (replay_spend not idempotent), BC-13 (linear redo log naming), BC-30 (torn buckets), BC-34 (replica skips local redo).
- D-01 (lag monitor dead), D-02 (RF=2 majority math = 0 ACKs).
- EF-02 (alive_node_count excludes self), EF-03 (no isolated-remnant test), EF-09 (REDIRECT loop), EF-10 (no split-brain heal), EF-12 (zero-record migration unauthed).
- GH-04 (migration_complete `entry_count*36` unchecked), GH-06/09 (stream chunk total cap missing), GH-G1 (snapshot deserialize unchecked multiplication), GH-G3 (`import_index` not transactional).
- IJK-01 (external blob hash always zero), IJK-02 (no orphan-blob GC), IJK-04 (device_io dead code), IJK-05 (silent zero-write of head bytes), IJK-20/22 (`MarkLongestChainBatch` no ReplicaOp).
- LMNH-01 (no write timeout on response — slow-reader pins thread), LMNH-07 (`/health/ready` always true), LMNH-08 (admin endpoints unauthenticated), LMNH-16/17/18 (no proptest, no fuzz, redb backend uncovered at server level), LMNH-31 (replica silently drops `write_metadata` errors during apply).

#### MEDIUM
- A-02 (concurrent-spend test never asserts winner spending_data), A-11 (wire GetSpend skips utxo_hash check), A-13 (`reassign` uses `saturating_add` for spendable_height — overflow pins UTXO unspendable forever), A-21 (`set_conflicting` fast path skips parent propagation), A-29 (delete tombstone vs allocator.free crash boundaries).
- BC-08, BC-12, BC-14–19, BC-22, BC-24–29, BC-31–33.
- D-03, D-05, D-06, D-11, D-15, D-19.
- EF-04, EF-05, EF-06, EF-08, EF-21, EF-29.
- GH-05, GH-08, GH-13, GH-14, GH-16, GH-G2, GH-G4, GH-G14, GH-G15, GH-G16.
- IJK-03, IJK-07, IJK-09, IJK-10, IJK-11, IJK-12, IJK-15, IJK-19, IJK-23.
- LMNH-04, LMNH-05, LMNH-09, LMNH-19, LMNH-22 (see Category L+M+N file).

#### LOW
A-14 through A-34 (18 LOW from Category A, mostly polish: boundary tests, generation u32 wrap, idempotent-respend write amplification, HashMap-vs-BTreeMap determinism, etc.); BC LOW set; D LOW set; EF LOW set; GH LOW set; IJK LOW set; LMNH LOW set. See per-category files; ~80+ in aggregate.

---

## Test coverage matrix

The full per-opcode × scenario matrix is at `audit/coverage-matrix.md`. Each opcode is crossed against {happy, error codes, batch boundaries, crash mid-op, replication-failure mid-op, migration-in-progress, single-node vs cluster}. The summary below highlights only the error-code triggerability table.

### Error-code triggerability

| Code | Name | Test that triggers it |
|---|---|---|
| 0 | OK | all happy paths |
| 1 | TX_NOT_FOUND | `engine.rs:tx_not_found` family |
| 2 | UTXO_HASH_MISMATCH | `engine.rs:hash_mismatch` family |
| 3 | ALREADY_SPENT | `engine.rs:already_spent` — payload-shape (36 bytes spending data) **not** asserted in any *integration* test; only unit-tested at engine level |
| 4 | ALREADY_FROZEN | covered at engine layer |
| 5 | UTXO_NOT_FROZEN | covered |
| 6 | INVALID_SPEND | covered |
| 7 | FROZEN | covered |
| 8 | CONFLICTING | covered |
| 9 | LOCKED | covered |
| 10 | COINBASE_IMMATURE | covered (engine-level); 4-byte payload shape not integration-asserted |
| 11 | VOUT_OUT_OF_RANGE | covered |
| 12 | ALREADY_EXISTS | covered |
| 13 | FROZEN_UNTIL | covered |
| 14 | REDIRECT | covered in cluster_*.rs |
| 15 | NO_QUORUM | **MISSING** integration test for "isolated 1-node remnant rejects" — EF-03 |
| 16 | STREAM_NOT_FOUND | covered at codec level only |
| 17 | BLOB_NOT_FOUND | covered |
| 18 | STREAM_OFFSET_MISMATCH | covered |
| 19 | MIGRATION_IN_PROGRESS | partial — covered for spend; not asserted for every write op separately |
| 20 | REPLICATION_FAILED | covered, but no test asserts no compensation leak |
| 21 | MIGRATION_MANIFEST_REQUIRED | covered |
| 22 | MIGRATION_MANIFEST_MISMATCH | covered |
| 23 | TOPOLOGY_PERSIST_FAILED | partial |
| 24 | STALE_EPOCH | covered (replication_tcp.rs) |
| 25 | CLUSTER_NOT_READY | partial |
| 26 | INDEX_DEGRADED | covered (degraded readiness gate) |
| 255 | INTERNAL | covered |

### Test infra deficits
- No `proptest` / `quickcheck` dep in `Cargo.toml` → **no property-based tests** for UTXO conservation invariants. (LMNH-16)
- No `cargo-fuzz` target → **no fuzz coverage** on the wire protocol parser. (LMNH-17)
- Integration tests instantiate only `IndexBackendMode::Memory`. The `Redb` and `FileBacked` backends have crash-injection coverage in `tests/fault_injection.rs` and `tests/secondary_two_phase_durability.rs`, but no full-stack server/cluster/replication coverage. (LMNH-18)
- Stress tests in `tests/stress_tests.rs` are gated behind `TERASLAB_FULL_WORKLOAD=1` env var → never run in default CI.
- Cluster chaos tests are in-process and deterministic; the only end-to-end chaos exists in `teraslab-tests/docker/`.

---

## Action plan (milestones, in priority order)

### Milestone 0 — "do not lose UTXO data"
Pre-requisite: cannot ship until all of these are closed.

1. **A-01 — Stop swallowing slot/metadata write errors in `Engine::spend`.** Replace every `tracing::warn!` / `let _ =` site at `src/ops/engine.rs:1013, 1042, 1066, 2920, 2948` with `?` propagation. The dispatcher must return `ERR_INTERNAL` on failure and trust the redo log to drive replay.
2. **A-03 — Track `actually_written` count in `ValidatedSpend::apply`.** Increment `meta.spent_utxos` only by the count of slots that successfully wrote. Best done together with A-01.
3. **A-04 — Add `spending_data: [u8; 36]` to `UnspendRequest` and `WireUnspendItem`.** In `Engine::unspend`, after the hash check, `if slot.spending_data != req.spending_data { return Err(SpendingDataMismatch) }`. New protocol-version-bumping change.
4. **A-06 — Make recovery replay update derived state (`generation`, `LAST_SPENT_ALL`, DAH/unmined indexes, `updated_at`)** or capture every derived field in the redo entry.
5. **A-08 — `freeze`/`unfreeze` must bump generation, write metadata back, and call `sync_index_cache`.**
6. **A-09 — `reassign` must check `LOCKED`, `CONFLICTING`, and coinbase maturity** before mutating.
7. **A-12 — `preserve_until` must call `sync_index_cache`** so fast paths see `HAS_PRESERVE_UNTIL`.
8. **Fix the failing tests.** `index::tests::rebuild_*` and `index::backend::tests::rebuild_redb_*` — either rewrite the corruption or update assertions.
9. **BC-01 — Wire redo-log checkpoint to a production cadence.** Background task: when `write_pos / log_size > 0.5`, snapshot index + persist allocator + DAH + unmined → `checkpoint()` → `reset()`. Reject mutations cleanly with a backoff status when watermark exceeded.
10. **BC-04 — Move `engine.lookup` + `running_spent` computation INSIDE the per-tx stripe lock.** Or change redo entries to carry deltas instead of absolute counts and make `replay_*` re-derive from on-device state under a lock.
11. **BC-02 — Either take stripe read-locks in `read_metadata`/`read_slot`/`lookup_cached`, or document the "torn read returns RecordCorruption; client must retry" contract and remove the misleading safety doc on `io.rs`.**
12. **BC-03 — Add a 4-byte CRC or generation-counter to `UtxoSlot`.** 69 → 73 bytes; recovery falls back to redo replay on torn detection.
13. **EF-02 — Fix `alive_node_count` to include self.** This is a one-line bug producing false `NO_QUORUM` rejections in healthy clusters. Add the integration test EF-03 calls out.
14. **D-19 / LMNH-31 / gap #5 — Begin and fsync the replication intent BEFORE local engine apply** (or fold pending-replication into the same redo entry). fsync the intent file and parent dir. Stop swallowing replica-side `write_metadata` errors at `src/replication/receiver.rs:684, 1127`.
15. **IJK-01 — Stop discarding `BlobStore::put` digest on the sync create path.** The current code makes every external-blob read fail integrity check.
16. **IJK-20 / IJK-22 — Emit `ReplicaOp` for `MarkLongestChainBatch`** so reorg DAH/unmined updates propagate. Add generation idempotency token.
17. **A-05 — `dispatch.rs:3271` (and the `Err(_)` branch at `:3278`) must call `engine.allocator().lock().free(...)` on `DuplicateTxId`** so device space does not leak under concurrent create races.

### Milestone 1 — "do not get rooted"
1. **EF-01 / D-20 / gap #1 — Apply `cluster::auth::sign`/`verify` to *every* TCP frame** (replication, topology, migration), or move to mTLS with role separation. Until this is done, the cluster is one trusted peer away from forged topology commits.
2. **EF-12 — Authenticate `OP_MIGRATION_COMPLETE`** sender; require manifest verification even on zero-record completions, or reject zero-record completions for non-empty source claims.
3. **Admin endpoints** — gate behind a token / mTLS / loopback-only by default; the `enable_admin_endpoints` flag is exposure control, not authentication.
4. **GH-G1, GH-04 — Bound checked-multiplications in `OP_MIGRATION_COMPLETE` and snapshot deserialize** before `Vec::with_capacity`.

### Milestone 2 — "do not brick on edge cases"
1. **EF-09 — REDIRECT hop count / TTL** in the wire format.
2. **EF-10 — Split-brain heal protection.** Two formerly-independent multi-node clusters that gossip with each other have no rejection path.
3. **BC-05 — Generation counter wrap.** Use u64 or detect wrap in recovery comparator.
4. **BC-30 — Torn hash-table buckets** under concurrent writers; align bucket access with versioning.
5. **IJK-08 — Orphan-blob GC.** Failed creates / aborted replications / dropped uploads currently leak forever.
6. **L-1 — Slow-Loris timeout.** Replace per-read timeout with a connection idle/total deadline.
7. **L-2 — Add a write timeout** on the response path.
8. **GH-G3 — Make `import_index` transactional across primary/dah/unmined files.**

### Milestone 3 — performance + observability
1. **IJK-04 — Wire `IoUringBackend` (or remove the dead module + correct README's "io_uring fast path" claim).**
2. **D-01 — Spawn the lag monitor; expose ack tracker via `/metrics`.**
3. **GH-G14, GH-G15 — Stream redb iter / `import_index` instead of materializing into Vec.**
4. **ConfigError::InvalidAckPolicy** — reject typos in `ack_policy`. Currently any unknown string silently behaves as `"auto"`.
5. **README sync** — document error codes 21–26, status codes 4–5, opcodes 103–106 / 240–253, the `STATUS_DEGRADED_DURABILITY` semantics, and remove the "redb falls back to in-memory if corrupt" claim.

---

## What this audit did NOT cover (and why)

- **Performance behavior under sustained load:** out of scope; this is a code-read, not a benchmark or chaos run.
- **Teranode adapter (`../teranode/stores/utxo/teraslab`):** referenced in the existing gap doc as incomplete; that worktree was not part of this audit.
- **Cryptographic primitives:** `cluster::auth::hmac_sha256` is hand-rolled with RFC 4231 test vectors. Adequate for a non-adversarial threat model; should be replaced with `ring` or `RustCrypto` if exposed to attackers.
- **OpenTelemetry / tracing pipeline correctness:** verified at the configuration / route level; trace fan-out under fault not exercised.
- **Web UI under `ui/`:** XSS / template-injection review out of scope.
- **Go client correctness:** out of scope.

---

## Methodology

Six parallel agents read disjoint subsets of the codebase, each writing a structured Markdown report to `audit/raw/`. The orchestrator (this document's author) ran `cargo build --release`, `cargo clippy --all`, `cargo test --all`, and direct file reads to verify build state, identify failing/ignored tests, build the spec-vs-implementation diff, and cross-check agent claims for high-severity findings.

Where this audit and the existing `docs/TERANODE_PRODUCTION_READINESS_GAPS.md` (2026-05-03) overlap, this audit confirms the existing gap with current file:line evidence. Where this audit identifies *new* problems not in that document, the finding is annotated "new finding" in its category file.
