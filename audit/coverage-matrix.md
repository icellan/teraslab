# Test Coverage Matrix

This matrix is per-opcode crossed against {happy path, every applicable error code, batch boundaries, crash mid-op, replication failure mid-op, migration in progress, single-node vs cluster}. Cells are `OK` / `partial (note)` / `MISSING`.

## Sources for verification

- Engine unit tests: `src/ops/engine.rs` (8875 lines, includes `#[cfg(test)] mod tests`)
- Dispatch unit tests: `src/server/dispatch.rs` (10315 lines)
- Integration: `tests/integration.rs`, `tests/e2e_workload.rs`, `tests/server_tcp.rs`
- Cluster: `tests/cluster_swim.rs`, `tests/cluster_tcp.rs`, `tests/cluster_edge_cases.rs`
- Replication: `tests/replication_tcp.rs` (8 integration tests)
- Crash: `tests/recovery_crash_boundaries.rs`, `tests/secondary_two_phase_durability.rs`, `tests/fault_injection.rs` (`--features fault-injection`)
- Stress: `tests/stress_tests.rs`, `tests/stress/mod.rs`

## Documented opcodes 1–12, 20–21, 30–32, 100–102, 200–201

| Op | Name | Happy | Error codes | Batch boundaries | Crash mid-op | Replication-fail | Migration-in-progress | Single vs cluster |
|---|---|---|---|---|---|---|---|---|
| 1 | SpendBatch | OK | OK at engine; **partial** at integration (no integration test asserts the 36-byte ALREADY_SPENT payload byte-for-byte) | OK (max_batch_size + per-item caps) | OK (`recovery_crash_boundaries.rs`, multiple boundaries) | OK | partial — only spend-during-migration is tested explicitly | OK in both |
| 2 | UnspendBatch | OK | OK | OK | **partial** — concurrent batches that race the counter (BC-04) are not tested | partial | partial | OK |
| 3 | SetMinedBatch | OK | OK | OK | OK | partial | partial | partial |
| 4 | CreateBatch | OK | OK | OK | OK (CreateV2 byte-for-byte replay verified) | partial | partial | OK |
| 5 | FreezeBatch | OK | OK | OK | partial | partial | partial | OK |
| 6 | UnfreezeBatch | OK | OK | OK | partial | partial | partial | OK |
| 7 | ReassignBatch | OK | OK including FROZEN_UNTIL | OK | partial — recent gap #8 work added compensation; the post-compensation slot-state correctness is now tested | partial | partial | OK |
| 8 | SetConflictingBatch | OK | OK | OK | partial | partial | partial | OK |
| 9 | SetLockedBatch | OK | OK | OK | partial | partial | partial | OK |
| 10 | PreserveUntilBatch | OK | OK | OK | partial | partial | partial | partial |
| 11 | DeleteBatch | OK | OK | OK | partial — large blob compensation OOM risk (IJK-19) not covered | partial | partial | OK |
| 12 | MarkLongestChainBatch | OK | OK | OK | partial | **MISSING** — no `ReplicaOp` is emitted (IJK-20), so master/replica divergence under reorg is not tested because it cannot be (the protocol does not carry the change) | partial | partial |
| 20 | GetBatch | OK | OK | OK | n/a (read-only) | n/a | partial | OK |
| 21 | GetSpendBatch | OK | OK | OK | n/a | n/a | partial | OK |
| 30 | QueryOldUnmined | OK | n/a | OK | n/a | n/a | n/a | partial — not filtered by `preserve_until` (IJK-11) |
| 31 | PreserveTransactions | OK | OK | OK | partial | partial | partial | partial |
| 32 | ProcessExpiredPreservations | OK | OK | OK | **MISSING** — does not re-validate `preserve_until` from on-device meta (IJK-09) | partial | partial | partial |
| 100 | GetPartitionMap | OK | n/a | n/a | n/a | n/a | n/a | OK — but EF-05: omits self |
| 101 | Health | OK | n/a | n/a | n/a | n/a | n/a | OK |
| 102 | Ping | OK | n/a | n/a | n/a | n/a | n/a | OK |
| 200 | StreamChunk | OK | OK | partial — no per-stream total cap (GH-06/09) | partial | n/a | n/a | OK |
| 201 | StreamEnd | OK | OK | OK | partial — discards `BlobStore::put` digest (IJK-01) so external blobs always reject on read | n/a | n/a | OK |

## Internal opcodes (not in README, but on the wire)

| Op | Name | Notes |
|---|---|---|
| 103 | GetCommittedTopology | read-only; auth gap (EF-01) |
| 104 | AdminDiagnoseKey | should be admin-gated |
| 105 | PartitionVersionReport | unauth (EF-01) |
| 106 | AdminClusterHealth | unauth |
| 240 | ReplicaBatch | covered by `replication_tcp.rs`; unauth (EF-01) |
| 241 | ReplicaAck | covered |
| 242 | MigrationComplete | unauth + zero-record skips manifest (EF-12); HIGH |
| 243 | MigrationBatchComplete | unauth |
| 250 | Heartbeat | covered in cluster tests |
| 251–253 | TopologyPropose/Vote/Commit | unauth (EF-01); CRITICAL |
| 255 | IncrementSpentExtraRecs | internal counter |

## Error-code triggerability

| Code | Name | Test that triggers it |
|---|---|---|
| 0 | OK | every happy path |
| 1 | TX_NOT_FOUND | `engine.rs:tx_not_found_*` family + `tests/server_tcp.rs` |
| 2 | UTXO_HASH_MISMATCH | `engine.rs:hash_mismatch_*` family |
| 3 | ALREADY_SPENT | `engine.rs:already_spent_*` — payload-shape (36 bytes spending data) **not** asserted in any *integration* test; only at engine level |
| 4 | ALREADY_FROZEN | engine-level only |
| 5 | UTXO_NOT_FROZEN | engine-level only |
| 6 | INVALID_SPEND | engine-level only |
| 7 | FROZEN | covered |
| 8 | CONFLICTING | covered |
| 9 | LOCKED | covered |
| 10 | COINBASE_IMMATURE | covered (engine-level); 4-byte payload shape not integration-asserted |
| 11 | VOUT_OUT_OF_RANGE | covered |
| 12 | ALREADY_EXISTS | covered |
| 13 | FROZEN_UNTIL | covered |
| 14 | REDIRECT | covered in cluster_*.rs — but EF-09: no hop counter, infinite-loop risk untested |
| 15 | NO_QUORUM | **MISSING** integration test for "isolated 1-node remnant rejects" (EF-03) — and EF-02: a healthy cluster can hit false NO_QUORUM, also untested |
| 16 | STREAM_NOT_FOUND | unit-level only |
| 17 | BLOB_NOT_FOUND | covered for `stream_to`, partial for `get`/`get_range`/`exists` (IJK-10) |
| 18 | STREAM_OFFSET_MISMATCH | unit-level |
| 19 | MIGRATION_IN_PROGRESS | partial — covered for spend; not asserted for every write op separately |
| 20 | REPLICATION_FAILED | covered, but no test asserts no compensation leak |
| 21 | MIGRATION_MANIFEST_REQUIRED | covered |
| 22 | MIGRATION_MANIFEST_MISMATCH | covered |
| 23 | TOPOLOGY_PERSIST_FAILED | partial |
| 24 | STALE_EPOCH | covered (replication_tcp.rs) |
| 25 | CLUSTER_NOT_READY | partial |
| 26 | INDEX_DEGRADED | covered (degraded readiness gate) |
| 255 | INTERNAL | covered (one of the catch-alls) |

## Test-infra deficits (LMNH-16/17/18)

- **No `proptest` / `quickcheck` in `Cargo.toml`** → no property-based tests for UTXO conservation, spend-once-only, or replication idempotency invariants. UTXO conservation is exactly the kind of invariant that proptest is designed to catch — its absence is a HIGH gap.
- **No `cargo-fuzz` target** → no fuzz coverage on the wire-protocol parser, despite the parser being the front-line for untrusted input.
- **Integration tests only run against `IndexBackendMode::Memory`** — `tests/server_tcp.rs`, `tests/integration.rs`, `tests/e2e_workload.rs` all instantiate the in-memory backend. The redb backend has crash-injection coverage in `tests/fault_injection.rs` and `tests/secondary_two_phase_durability.rs`, but no full-stack integration suite. So cluster + replication + DOS tests do not exercise redb at all.
- Cluster chaos tests (`cluster_*.rs`) are in-process and deterministic; the only end-to-end chaos exists in `teraslab-tests/docker/`.
- Stress tests (`tests/stress_tests.rs`) only have 2 distinct scenarios. Nightly stress runs are gated behind `TERASLAB_FULL_WORKLOAD=1` env var so they never run in CI by default.
- Crash-injection coverage is excellent at the WAL/data boundary (`tests/recovery_crash_boundaries.rs`, `tests/fault_injection.rs`) but sparse at the cluster boundary (membership state transitions, mid-migration crashes are mostly unit-tested rather than process-killed).

## Test scenarios that should exist but do not

1. Concurrent unspends on same txid drive on-device counter to a value that disagrees with redo entries (BC-04 reproducer).
2. Two concurrent spend attempts on same UTXO; assert exactly one wins and the loser receives the winner's 36-byte spending data byte-for-byte.
3. Coinbase spend before block 100 returns the 4-byte required-height in the error payload (the byte shape, not just the error code).
4. Isolated 1-node remnant of a 3-node cluster rejects writes with NO_QUORUM (EF-03).
5. Healthy 3-node cluster surviving 1 peer loss accepts writes (no false NO_QUORUM — would catch EF-02).
6. Two formerly-independent multi-node clusters that gossip with each other do not silently merge (EF-10).
7. Client following REDIRECT loops bounded by hop count or TTL (EF-09).
8. Migration declared complete by zero-record manifest does not lift the inbound fence (EF-12).
9. `MarkLongestChainBatch` on a master + cluster with a replica → assert replica DAH/unmined indexes update (IJK-20).
10. External blob upload + read round-trip — read should succeed (currently fails because `content_hash` is zero on sync create, IJK-01).
11. 1024 concurrent slow-reader connections → confirm server thread pool can still accept new connections (LMNH-01).
12. Connection that connects but never sends → confirm idle disconnect within bounded time (LMNH-03).
13. Property test: random sequence of `Spend`/`Unspend`/`Create`/`Delete` against a single key — invariant: `spent_utxos` on-device equals `count(slot.status == SPENT)`.
14. Wire-protocol fuzz target running against `RequestFrame::decode` and the per-op decoders.
15. Full integration suite running with `IndexBackendMode::Redb` and `IndexBackendMode::FileBacked`.
16. `ProcessExpiredPreservations` re-reads `preserve_until` from on-device metadata (IJK-09).
17. `OP_QUERY_OLD_UNMINED` filters by `preserve_until` (IJK-11).
18. Admin endpoint authentication (after MS1 fix) — confirm `/admin/quiesce` etc. require a token.
