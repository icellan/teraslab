# Residual MEDIUM/LOW Triage (post-Milestone 1–4)

Status verified against `main` HEAD `e223773` on 2026-06-12 by four read-only analysis agents.
Milestone 1–4 fixes were confirmed and are not relisted here. This is the long tail (audit item 27).

## OPEN — HIGH (milestones missed these; fix FIRST — Wave 1)

| ID | What | Where | Fix |
|----|------|-------|-----|
| **G-2** | Resize: displaced old table `Drop` writes clean-shutdown sentinel mid-run → crash-after-resize accepted as clean, torn buckets read → silent UTXO loss | `src/index/hashtable.rs:1446-1465` Drop + `:1316/1418` resize swap | Mark displaced table defunct / clear its FileBacked path so it skips the sentinel write; regression test "no sentinel while live after resize" |
| **G-4** | Engine uses lossy `PrimaryBackend::lookup`/`unregister` shims (redb I/O error → key-absent) at ~69/70 sites; only the create insert-if-absent uses `lookup_checked` | `src/index/backend.rs:99,188`; `src/ops/engine.rs:615` (spend), `:4415` (delete), `:2337/2521/2608` (dup-check) | Migrate hot-path sites to `*_checked`, map `Err → ERR_INTERNAL`, delete the shims |
| **B-3** | Torn redo compaction rewrites retained post-fence entries in place → partial `pwrite` loses acked entries | `src/redo.rs:2123` `compact_prefix_through` (`:2176` in-place pwrite) | Write retained set to scratch / flip an fsynced logical-start in the CRC'd header, or skip in-place rewrite when retained set ≠ ∅ |
| **B-5** | `SpendV2`/`UnspendV2` redo entries lack `utxo_hash` → torn slot in WAL window unrepairable; recovery fatal-bricks; no repair CLI | `src/redo.rs:330,348`; `src/recovery.rs:1004,1064` | Add `utxo_hash` to SpendV2/UnspendV2 (entries already 96B) to rebuild a CRC-failing slot like CreateV2; add an offline repair/rescan CLI subcommand |

## OPEN — MEDIUM worth fixing (Wave 2)

| ID | What | Recommendation |
|----|------|----------------|
| KO-5 | Conflicting-children list warn-only + hard u8(255) cap; 256th child silently dropped while `set_conflicting` returns OK (attacker-reachable cascade truncation) | Widen count to u16/u32 with overflow block, or surface a partial-status code so truncation isn't invisible |
| B-6 | Recovery-progress marker append can hit `LogFull` → deterministic boot-loop on near-full log | Treat marker-append failure as non-fatal (recovery is idempotent), or compact prefix before the final marker |
| B-7 | Unconditional full primary-index metadata scan to rebuild secondaries every boot → recovery O(store) not O(redo) | Reconcile only keys touched by replayed entries; full scan only when secondary backend reports unclean close |
| C-3 | Migration `stream_shard_baseline` reads metadata then slots with no stripe lock → torn per-record snapshot / generation drift | Take the stripe lock for the metadata+slots read, or seqlock-retry on generation change |
| C-4 | Receiver `PruneSlot` RMW + generation-sync write bypass stripe/io_locks; `ReplicaBatchReceiver::start` listener unprotected | Route `PruneSlot` + gen-sync writes through stripe-locked engine methods |
| D-7+D-8 | No runtime catch-up trigger (one startup pass, cap 10k); lag monitor warn-only — a steady-state replica that misses batches is never repaired until master restart | Drive catch-up from the lag monitor in a converge-loop (auth + dense-seq prerequisites now satisfied) |
| E-4 | Inter-node frame auth has no nonce — a captured valid frame replays for 5 min | Per-connection monotonic nonce under HMAC, or DOCUMENT delegation to per-opcode idempotency + audit each mutating opcode |
| E-5 | Clock skew >5 min silently partitions; no distinct diagnostic | Emit a distinct skew-rejection metric/log; optionally make window configurable |
| LM-3 | `/health/ready` lag verdict cached 500 ms in a process-global static (cross-instance leak in multi-node test process) | Move the cache into `HttpState` |
| N-6 | Wire error codes 32/33/34 have zero behavioral tests; 21-26 + STATUS_DEGRADED_DURABILITY in-process only | Add wire conformance tests (code 32 = any cluster opcode on a single-node server) |
| IJ-7 | Allocator `free()` has no double-free/overlap rejection → silent freelist corruption on any caller bug | Reject overlapping free with typed `DoubleFree` using existing `free_region_containing` |
| IJ-3 | Tier thresholds advisory: production tier = client `FLAG_EXTERNAL_BLOB`, `tier_for_size`/`INLINE_THRESHOLD` never consulted; README "inline if <8 KiB" false | Decide (Rule 6): enforce `tier_for_size` server-side OR update README/phase-11 and delete the false promise |
| G-5 | `secondary_two_phase_durability` test reuses live primary, no real restart | Add ≥1 kill-based / `load_primary_index_*`-rebuild restart test |
| G-7 | Corrupt-redb test uses `let _ =`, asserts nothing | `assert!(matches!(result, Err(RebuildError::RedbPrimary{..})))` |
| F-5 | phases/09 still says writes return Redirect (impl does fence+dual-write); stale `dispatch.rs:3475` comment | Update phases/09 to the implemented protocol; fix the comment (M4 doc sweep missed this) |
| F-4 | Empty-address redirect when master unknown; redirect-loop protection unused | Return a retryable error instead of an empty-address redirect |

## OPEN — LOW worth small fixes (Wave 2, mechanical)

| ID | What |
|----|------|
| G-LOW | `u16` probe-distance counters dead >65 536 buckets → change to `usize` |
| H-LOW | `OP_MIGRATION_COMPLETE` malformed → `ERR_MIGRATION_IN_PROGRESS` instead of `ERR_PAYLOAD_MALFORMED` (3 arms) |
| KO-11 | set_mined/set_conflicting fast path writes cached DAH/flags after a failed `sync_index_cache` (F-G2-011 covered generation only) — read old_dah/preserve/flags from the fresh `meta` |
| C-6 | Stale `unsafe impl Send/Sync` safety comments (engine.rs:156-157, device.rs:540) name only stripe locks |
| C-8 | Non-atomic migration gauge decrements (`migration.rs:10-13,25-28`) → `fetch_update` |
| C-7 | `StripedLocks` stripe selection unseeded (txid grinding) — one-line seed XOR |
| E-LOW | No dead-seed cluster-formation test |
| F-6 | Legacy `ShardTable::compute` order-dependent version hash → sort before hashing or `#[cfg(test)]` |
| DC | `send_delta_ops` stale `#[allow(dead_code)]` (it IS live); `migrate_single_shard` dead — delete; SAFETY comments missing (engine.rs direct-I/O ×11, hashtable.rs mmap ×15, swim.rs setsockopt); `getrandom().expect()` can abort (hashtable.rs:290); `protocol.rs:659` `hash_count*32` unchecked mul |
| N-LOW | Bare `.is_err()` assertions without variant check (index/mod.rs, backend.rs, blobstore.rs, frame.rs); opcode 255 untested; empty-batch count=0 semantics unpinned |
| IJ-LOW | `Engine::delete` doesn't delete external blob (document deferred-GC contract); macOS `F_NOCACHE` fcntl return ignored |

## DOCUMENT / ACCEPT (no code change, or by-design)

- **A-3** — unspend writes slot before decrementing counter; WAL + recovery re-derivation cover it. Document the invariant.
- **B-10** — compensation applies before logging intents; bounded double-fault, already doc'd.
- **D-9** — best_effort/STATUS_DEGRADED_DURABILITY dead branch for RF>1; README now honestly documents RF=1-only (M4). Cosmetic dead branch.
- **D-11/D-12** — no transport replay nonce/TLS; intent-tracker fsync-per-batch. Defer to an mTLS wave; document security posture.
- **LM-2** — data plane unauthenticated + no rate limit; documented trusted-overlay (`docs/DEPLOYMENT_ASSUMPTIONS.md`). Defer token-bucket to mTLS.
- **E-3** — minority accepts writes during SWIM suspicion window; bounded staleness, replication is the second gate. Document + add an in-window test.
- **KO-7** — `unmined_since` height-0 collision; genesis-only, unreachable on BSV mainnet.
- **D-10** — `ReplicaBatchAccumulator` unused + `ReplicationManager` 3065-line test-only duplicate; the two-implementation hazard persists but the production path is the fixed one. Delete/demote when convenient (Wave 2 dead-code).
- Detached HTTP/OTLP/migration threads; `persist_topology_state` discards on non-vote paths; `persist_peak_cluster_size` test-only — all by-design / defense-in-depth.
- **FIXED-since (re-confirmed):** A-2, E-LOW seen_seq GC, IJ-6, IJ-LOW digest per-key-lock, KO-8/9/10, KO-4 (retracted), KO-6, spec-vs-impl List-1 (13) + List-2 (5) via M4 doc sweep.
