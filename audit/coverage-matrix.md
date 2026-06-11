# Test Coverage Matrix — per-operation (Category N audit, 2026-06-11)

Authoritative opcode list: `src/protocol/opcodes.rs` (matches README §Operations).
Legend: ✅ covered (cited test verified real by spot-read) · ⚠️ partial (explained) · ❌ missing · n/a not applicable.

Column semantics:
- **Happy path**: success over the REAL wire (TCP client → framed request → response) unless noted.
- **Error codes**: every applicable wire error code asserted with the exact code value (not just `STATUS_ERROR`).
- **Batch edges**: empty batch (count=0), max/over-max batch, duplicate items in one batch.
- **Crash mid-op**: crash injected inside the op's write pipeline, recovery verified.
- **Repl fail**: replication-ACK failure during this op observed by a client.
- **Migration**: behavior while the key's shard is fenced/migrating.
- **Cluster**: op exercised through cluster routing (not just single-node).

## Mutations (1–12)

| Op | Happy path | Error codes | Batch edges | Crash mid-op | Repl fail | Migration | Cluster |
|---|---|---|---|---|---|---|---|
| 1 SpendBatch | ✅ `server_tcp.rs::create_spend_get_spend`, `integration.rs::full_lifecycle_single_tx` | ✅ codes 1,2,3,6,7,8,9,10,11,13 wire-asserted with payloads in `server_tcp.rs::tcp_error_code_triggerability_core_item_errors`; 30 `error_code_conformance.rs::t2/t3`; 6+payload `t4`/`t6`; 35 `t5` | ⚠️ 1024-item `server_tcp.rs::batch_spend_1024_items`; dup-txid `batch_spend_100_same_txid`; over-max tested only via op 11; empty batch ❌ | ✅ `recovery_crash_boundaries.rs::boundary_*` (4 boundaries), `fault_injection.rs::kill_after_redo_fsync_before_data_pwrite_recovers_slot`, `src/recovery.rs::crash_between_redo_and_data_write_spend` | ✅ strict mode wire: `cluster_tcp.rs::tcp_strict_replication_failure_returns_replication_failed`; ⚠️ best-effort `STATUS_DEGRADED_DURABILITY` only in-process (`src/server/dispatch.rs::best_effort_all_replicas_fail_yields_status_degraded_durability`) | ✅ `migration_fence.rs::fenced_shard_rejects_spend_serves_read_then_spend_succeeds_after_fence_lifts`, `cluster_tcp.rs::tcp_write_to_pending_inbound_shard_returns_migration_in_progress` | ✅ `cluster_tcp.rs::spend_routed_to_correct_master`, redirect `client_redirect_resends_to_new_node` |
| 2 UnspendBatch | ✅ `server_tcp.rs::all_operations_from_phases_3_through_6_over_tcp` | ⚠️ engine-level InvalidSpend/payload pinned by `property_utxo.rs`; wire-level per-code assertion for unspend path not present | ❌ none specific | ⚠️ replay-level only: `src/recovery.rs::replay_unspend_*` (incl. wrong-spending-data no-mutate); no dispatch-driven crash boundary | ⚠️ `src/replication/receiver.rs` unit (2 sites) | ❌ | ❌ |
| 3 SetMinedBatch | ✅ `server_tcp.rs::create_set_mined_delete` | ✅ 30 `error_code_conformance.rs::t3_set_mined_dah_overflow…` (sparse, empty payload asserted); 1 generic | ❌ none specific | ✅ `recovery_crash_boundaries.rs::boundary_set_mined_*` (WAL replay, overflow entries, idempotent 2nd pass) | ✅ `replication_tcp.rs::tcp_replicate_mixed_ops` | ❌ | ⚠️ via replication tests only |
| 4 CreateBatch | ✅ `server_tcp.rs::create_10_then_get_batch_all` | ✅ 12 wire (`tcp_error_code_triggerability_core_item_errors`); per-item caps R-089/R-090 in `g4_create_v2_caps.rs` | ⚠️ dup-in-one-batch in-process only (`src/server/dispatch.rs` ~L11710); empty batch ❌ | ✅ `recovery_crash_boundaries.rs::full_pipeline_recovery_reconstructs_create_v2`, `src/recovery.rs::replay_create_v2_idempotent_on_double_recovery` | ⚠️ receiver unit only | ✅ `cluster_tcp.rs::during_migration_writes_redirect_to_new_node` | ✅ `cluster_tcp.rs::start_three_node_cluster_create_records_distributed` |
| 5 FreezeBatch | ✅ `server_tcp.rs::freeze_unfreeze_over_tcp` | ✅ 4 wire; 3-on-spent wire (same triggerability test) | ❌ | ⚠️ replay-level: `g4_replay_freeze.rs` (skips SPENT slot), `src/recovery.rs::idempotent_freeze`; no dispatch crash boundary | ✅ `replication_tcp.rs` (ReplicaOp::Freeze x5) | ❌ | ❌ |
| 6 UnfreezeBatch | ✅ `server_tcp.rs::freeze_unfreeze_over_tcp` | ✅ 5 wire | ❌ | ⚠️ `src/recovery.rs::unfreeze_v2_replay_skips_non_frozen_slot` | ⚠️ receiver unit | ❌ | ❌ |
| 7 ReassignBatch | ✅ `server_tcp.rs::freeze_reassign_get_spend` | ✅ 13 wire + 4-byte height payload; 30 overflow `error_code_conformance.rs::t3_reassign_overflow…` | ❌ | ⚠️ `src/recovery.rs::replay_reassign` only | ⚠️ receiver unit (1 site) | ❌ | ❌ |
| 8 SetConflictingBatch | ✅ `server_tcp.rs::create_set_conflicting` | ⚠️ downstream 8-on-spend wire-asserted; no direct error paths tested for the op itself beyond TX_NOT_FOUND engine-level | ❌ | ⚠️ `src/recovery.rs::replay_set_conflicting` | ⚠️ receiver unit | ❌ | ❌ |
| 9 SetLockedBatch | ✅ `server_tcp.rs::create_set_locked` | ⚠️ downstream 9-on-spend wire-asserted | ❌ | ⚠️ `src/recovery.rs::replay_set_locked`, `replay_compensate_set_locked_restores_dah` | ✅ `replication_tcp.rs` (SetLocked) | ❌ | ❌ |
| 10 PreserveUntilBatch | ✅ `server_tcp.rs::create_preserve_until_get`, `integration.rs::preserve_until_blocks_pruning` | ⚠️ no per-code wire errors | ❌ | ⚠️ `src/recovery.rs::replay_preserve_until` | ⚠️ receiver unit (1 site) | ❌ | ❌ |
| 11 DeleteBatch | ✅ `server_tcp.rs::create_set_mined_delete` | ✅ 1 wire (`request_for_nonexistent_tx_partial_error` is GET; delete-side via conformance t4/t5 setup asserts STATUS_OK + downstream prune codes) | ⚠️ over-max (8193) `server_tcp.rs::batch_exceeding_max_batch_size_rejected` but asserts `STATUS_ERROR` only, code unchecked; empty ❌ | ✅ `src/recovery.rs::crash_between_redo_and_delete`, `recover_all_delete_tombstones_and_frees_region`; race: `g2_delete_race.rs` | ⚠️ receiver unit (3 sites) | ❌ | ❌ |
| 12 MarkLongestChainBatch | ✅ `server_tcp.rs::create_set_mined_mark_longest_chain` | ⚠️ no per-code wire errors | ❌ | ✅ `recovery_crash_boundaries.rs::boundary_mark_longest_chain_{off,on}…` + generation idempotency in `src/recovery.rs` | ✅ `replication_tcp.rs::cluster_mark_longest_chain_replicates_dah_unmined`, `mark_longest_chain_replay_idempotent` | ❌ | ⚠️ via replication tests |

## Reads (20–21)

| Op | Happy path | Error codes | Batch edges | Crash mid-op | Repl fail | Migration | Cluster |
|---|---|---|---|---|---|---|---|
| 20 GetBatch | ✅ `server_tcp.rs::create_10_then_get_batch_all` | ✅ NOT_FOUND sparse (`request_for_nonexistent_tx_partial_error`); ⚠️ per-item redirect `WireGetResult.data` shape not wire-asserted | ❌ empty/max | n/a (read) | n/a | ⚠️ reads-during-fence covered for GetSpend, not Get | ✅ `cluster_tcp.rs::query_reaches_correct_node_returns_data`; FLAG_LOCAL_READ via `replication_tcp.rs::tcp_consistency_verification` |
| 21 GetSpendBatch | ✅ `server_tcp.rs::create_then_get_spend` | ✅ 2 wire (`get_spend_wire_validates_utxo_hash`) | ❌ | n/a | n/a | ✅ `migration_fence.rs` (read served while fenced) | ✅ `cluster_partition.rs`, `migration_fence.rs` |

## Pruner (30–32)

| Op | Happy path | Error codes | Batch edges | Crash mid-op | Repl fail | Migration | Cluster |
|---|---|---|---|---|---|---|---|
| 30 QueryOldUnmined | ✅ wire `g5_protocol_auth.rs` (F-G5-003 single-node) + in-process `src/server/dispatch.rs::dispatch_query_old_unmined_*` | ⚠️ malformed-payload asserted in-process only | n/a | n/a | n/a | ❌ | ❌ |
| 31 PreserveTransactions | ⚠️ in-process only (`src/server/dispatch.rs::dispatch_preserve_transactions_*`) — no TCP test | ⚠️ malformed in-process | ❌ | ⚠️ engine preserve replay only | ❌ | ❌ | ❌ |
| 32 ProcessExpiredPreservations | ⚠️ in-process only (`dispatch_process_expired_*`) | ⚠️ malformed in-process | ❌ | ❌ (deletes during crash window untested) | ❌ | ❌ | ❌ |

## Admin / cluster client (100–107)

| Op | Happy path | Error codes | Notes |
|---|---|---|---|
| 100 GetPartitionMap | ✅ wire `cluster_tcp.rs::partition_map_served_over_tcp`; auth gate `g5_protocol_auth.rs::strict_auth_gates_admin_opcodes` | ❌ ERR_NOT_CLUSTERED(32) on single-node never asserted anywhere | |
| 101 Health | ⚠️ in-process only (`dispatch.rs` ~L8207) | — | no wire test |
| 102 Ping | ✅ wire `server_tcp.rs::ping_pong` | — | |
| 103 GetCommittedTopology | ⚠️ exercised only indirectly by live-cluster convergence tests; no direct request/response assertion in tests/ | ❌ | |
| 104 AdminDiagnoseKey | ⚠️ in-process (`dispatch.rs` ~L12789–12930: happy, empty, short, count>64) + auth-gate wire (`g5_protocol_auth.rs`) | ⚠️ ERR_PAYLOAD_MALFORMED asserted in-process only | |
| 105 PartitionVersionReport | ⚠️ in-process dispatch tests; indirectly via migration-plan cluster tests | ❌ | |
| 106 AdminClusterHealth | ⚠️ in-process (`dispatch.rs` ~L10140: bypasses readiness gate, payload shape) + auth-gate wire | ⚠️ | |
| 107 Hello | ⚠️ in-process only (`dispatch.rs` ~L8219) | ❌ | no wire test despite being the documented client handshake |

## Streaming (200–201)

| Op | Happy path | Error codes | Crash mid-op | Notes |
|---|---|---|---|---|
| 200 StreamChunk | ✅ wire `server_tcp.rs::stream_isolation_per_connection` | ✅ 18 wire (`server_tcp.rs` ~L1846), 255-no-blobstore `error_code_conformance.rs::t1`; ❌ ERR_STREAM_INVARIANT(34) never asserted | ⚠️ tmp-file cleanup `g9_007`, GC `blob_gc_recovery.rs` | slow-loris memory bound: `g5_slow_loris_streaming.rs` |
| 201 StreamEnd | ✅ wire | ✅ 16 wire (`stream_end_without_active_stream…`), 17 wire (`external_blob_create_without_uploaded_blob…`) | ✅ `blob_gc_recovery.rs::failed_create_blob_garbage_collected_on_recovery` | |

## Inter-node (240–243, 250–253, 255)

| Op | Coverage |
|---|---|
| 240 ReplicaBatch | ✅ over TCP: `replication_tcp.rs` (13 tests: catchup, mixed ops, timeout, write-majority boundaries); HMAC: `g5_slow_loris_streaming.rs`; stale epoch ⚠️ in-process (`src/replication/receiver.rs` ~L4475–4701) |
| 241 ReplicaAck | ✅ as response payload of 240 in `replication_tcp.rs`; request-path exclusion pinned by `g5_protocol_auth.rs` |
| 242/243 MigrationComplete / BatchComplete | ⚠️ manifest-required/mismatch (codes 21/22) asserted in-process only (`dispatch.rs` ~L10462/10531); live happy path indirectly via `cluster_tcp.rs::migrate_shard_with_records_to_new_node`, `no_records_lost_during_migration` |
| 250 Heartbeat | ✅ wire `g5_protocol_auth.rs::heartbeat_returns_status_ok_not_unknown_opcode` |
| 251–253 Topology propose/vote/commit | ✅ live formation `cluster_tcp.rs`, `cluster_edge_cases.rs` (quorum cycle, duplicate votes, rejected votes); persist-failure code 23 ⚠️ in-process only (`dispatch.rs` ~L11128); partition behavior `cluster_partition.rs::partitioned_minority_never_self_activates_topology` |
| 255 IncrementSpentExtraRecs | ❌ zero tests anywhere (handler at `dispatch.rs` ~L512 returns STATUS_OK no-op; the documented compatibility contract is unpinned) |

## Cross-cutting gaps (apply to every batch op)

- **Empty batch (count=0)**: no test pins the wire response for a zero-item batch on ANY opcode. Decoders are fuzzed (no panic), but the semantic contract (STATUS_OK with empty payload? error?) is unpinned.
- **Over-max batch**: tested for one opcode only (11), and asserts `STATUS_ERROR` without checking `ERR_PAYLOAD_MALFORMED`.
- **Migration-in-progress**: only ops 1/4/21 have fence-behavior tests; ops 2,3,5–12 against a fenced shard are untested.
- **Cluster routing**: ops 2,5–11 are never issued through cluster routing in any test.

## Stats

Counting the 19 client-facing ops × 7 columns (n/a cells excluded, 121 gradable cells):
**✅ 48 (40%) · ⚠️ 38 (31%) · ❌ 35 (29%)**

Worst-covered operations: **31 PreserveTransactions / 32 ProcessExpiredPreservations** (no wire test, no crash test), **255 IncrementSpentExtraRecs** (nothing), **10 PreserveUntilBatch / 8 SetConflictingBatch / 6 UnfreezeBatch** (happy-path wire only; everything else partial or missing), **107 Hello / 101 Health** (in-process only).
Best-covered: **1 SpendBatch** (every column ✅ or strong ⚠️), **4 CreateBatch**, **200/201 streaming**.
