# TeraSlab Bulletproofing Audit

Date: 2026-05-06
Target: current working tree at repository root
Output note: supporting audit files, if added later, should live under `audit_codex/`.

## Methodology And Build Results

I treated `README.md`, `specs/`, `phases/`, module docs, and `src/protocol/` as the spec surface and audited the current Rust implementation against it. I also searched the dangerous patterns requested in the prompt: `unwrap`, `expect`, `panic`, `todo`, `unimplemented`, `unreachable`, `unsafe`, `.ok()`, `let _ =`, `Err(_)`, narrowing casts, frame allocation paths, replication paths, and ignored tests.

Commands run:

| Command | Result |
| --- | --- |
| `cargo build --release` | ✅ succeeded |
| `cargo test --all` | ❌ failed: 1480 passed, 3 failed, 1 ignored |
| `cargo clippy --all -- -D warnings` | ✅ succeeded |

Failed tests:

| Test | Failure |
| --- | --- |
| `index::backend::tests::rebuild_redb_fails_on_corrupted_magic_in_allocated_region` | `src/index/backend.rs:954` expected `invalid metadata magic`, actual error is a CRC/corrupt metadata error |
| `index::tests::rebuild_fails_on_corrupted_magic_in_allocated_region` | `src/index/mod.rs:1144` same stale assertion |
| `index::tests::rebuild_secondary_fails_on_corrupted_allocated_record` | `src/index/mod.rs:1208` same stale assertion |

Ignored test:

| Test | Location | Finding |
| --- | --- | --- |
| `failed_data_migration_sends_abort_completion_handshake` | `src/cluster/coordinator.rs:7504-7506` | Ignored with `TODO: rewrite for pipelined migration flow`; this is migration failure handling, not optional coverage. |

## Executive Summary: 10 Most Dangerous Gaps

1. **CRITICAL:** Delete rollback can recreate previously spent, frozen, or pruned UTXOs as unspent after replication failure, reopening already-spent outputs (`src/server/dispatch.rs:3948`, `src/server/dispatch.rs:4055`, `src/replication/protocol.rs:177`).
2. **CRITICAL:** `OP_PROCESS_EXPIRED_PRESERVATIONS` deletes DAH-indexed transactions locally without shard ownership checks or replication, so cluster nodes can diverge or lose records (`src/server/dispatch.rs:395`, `src/server/dispatch.rs:4669`).
3. **HIGH:** `OP_MARK_LONGEST_CHAIN_BATCH` mutates `unmined_since`, `delete_at_height`, generation, and secondary indexes but is never replicated (`src/server/dispatch.rs:4197`, `src/ops/engine.rs:1531`, `src/replication/protocol.rs:106`).
4. **HIGH:** `OP_GET_BATCH` hides storage/corruption errors as zeroed slots, empty cold data, empty conflicting children, or `TX_NOT_FOUND` (`src/server/dispatch.rs:4462`, `src/server/dispatch.rs:4512`).
5. **HIGH:** Conflicting-child links are best-effort, not redo-durable, and recovery explicitly does not replay them, so descendant/conflict tracking can silently go stale (`src/ops/engine.rs:1742`, `src/recovery.rs:849`).
6. **HIGH:** A migration failure test for abort/completion handshakes is ignored, leaving an explicit untested crash/failure path in shard migration (`src/cluster/coordinator.rs:7504`).
7. **HIGH:** The default test suite is red; current CI cannot prove index rebuild behavior for corrupt allocated records (`src/index/mod.rs:385`, `src/index/mod.rs:1144`).
8. **HIGH:** Mutating admin endpoints have no authentication when enabled; `quiesce`, `rebalance`, and `drain` are exposed on the HTTP listener (`src/server/http.rs:88`, `src/server/http.rs:142`).
9. **HIGH:** Error-code triggerability is incomplete: several README error codes have no real client test proving the exact code reaches a client (`src/protocol/opcodes.rs:165`, matrix below).
10. **HIGH:** There are no property tests or fuzz targets for UTXO invariants or the wire parser, despite a high-risk binary protocol and conservation rules (`Cargo.toml:16`, `src/protocol/codec.rs:5010`).

## Findings

### UTXO Correctness And Data Loss

#### F1. Delete rollback can resurrect spent/frozen/pruned UTXOs as spendable

Severity: **CRITICAL**

Location:
- `src/server/dispatch.rs:3948-3952`
- `src/server/dispatch.rs:3970-3975`
- `src/server/dispatch.rs:4055-4061`
- `src/server/dispatch.rs:4081-4095`
- `src/replication/protocol.rs:177-183`

What's wrong: `handle_delete_batch` snapshots only metadata bytes, UTXO hashes, optional cold data, and `is_external`. It does not snapshot each slot's `status` or `spending_data`. On replication failure, it compensates by sending itself a `ReplicaOp::Create`, whose wire variant also only carries `utxo_hashes`, not slot state. The recreated record can therefore have metadata saying some UTXOs were spent while the slots themselves are recreated as default unspent/frozen-from-create state. Slot read errors during snapshot are also converted into zero hashes.

Why it matters: A client can delete a transaction containing a spent output, receive `ERR_REPLICATION_FAILED`, and the master compensates by recreating the transaction with that output unspent. The already-spent UTXO can then be spent again with different spending data. That is a double-spend acceptance path after a failed replicated delete.

Reproduction: In an RF=2 test, create a transaction, spend `vout=0`, force the replica target to fail during `OP_DELETE_BATCH`, and then call `OP_GET_SPEND_BATCH` / `OP_SPEND_BATCH` for `vout=0` after the delete response returns `ERR_REPLICATION_FAILED`. The slot should still be spent with the original 36-byte spending data. The current compensation path recreates from hashes only, so the slot state is not preserved.

Suggested fix: Replace `DeleteSnapshot` with a full record snapshot that includes every slot byte (`hash`, `status`, `spending_data`) and the exact metadata bytes needed to restore consistency. Add a `ReplicaOp::CreateFull` or reuse the durable `CreateV2` redo payload for compensation. Append the compensation redo before applying the local recreate, and fail closed if the compensation response or redo write fails. Add a regression test that deletes a tx containing spent, frozen, and pruned slots under forced replication failure and verifies all slot states after compensation and restart.

#### F2. `ProcessExpiredPreservations` deletes locally without ownership or replication

Severity: **CRITICAL**

Location:
- `src/server/dispatch.rs:391-395`
- `src/server/dispatch.rs:4669-4720`
- `src/server/dispatch.rs:4680-4708`

What's wrong: The dispatcher calls `handle_process_expired(request, engine, redo_log)` without passing cluster state. The handler queries the local DAH index, appends local delete redo entries, and calls `engine.delete` for every due key. It does not call `check_shard_ownership`, does not return `REDIRECT` or `MIGRATION_IN_PROGRESS`, and does not call `replicate_all_ops`.

Why it matters: In cluster mode, any node receiving opcode 32 can delete whatever due keys exist in its local DAH index, even if it is not the shard master. If the node is the master, the deletion still is not replicated, so replicas retain data. If the node is a replica, it deletes replica data behind the master's back. Both paths break shard consistency and can lose the only local copy needed during migration or failover.

Reproduction: In a two- or three-node cluster, create a record with a due `delete_at_height`, replicate it, then send `OP_PROCESS_EXPIRED_PRESERVATIONS` to a non-master that has a local replica. The handler will delete from the local engine instead of redirecting. Send the same opcode to the master and verify the replica still has the record because no `ReplicaOp::Delete` is emitted.

Suggested fix: Treat pruning deletes as regular clustered mutations. Pass `cluster` and `max_batch` to the handler, group due keys by ownership, return per-key `REDIRECT` / `MIGRATION_IN_PROGRESS`, append redo, apply, and replicate deletes with the same compensation rules as `OP_DELETE_BATCH`. If this is intended to be a local maintenance operation, remove it from the public client opcode surface and document/admin-gate it.

#### F3. `MarkLongestChainBatch` mutates durable state but is not replicated

Severity: **HIGH**

Location:
- `src/server/dispatch.rs:4131-4224`
- `src/server/dispatch.rs:4197-4198`
- `src/ops/engine.rs:1531-1601`
- `src/replication/protocol.rs:100-106`
- `src/replication/protocol.rs:117-190`

What's wrong: `handle_mark_longest_chain_batch` appends local redo and calls `engine.mark_on_longest_chain`, but the success branch explicitly emits no replica operation. The replication protocol reserves `_OP_MARK_LONGEST_CHAIN = 14`, but `ReplicaOp` has no corresponding variant. The engine operation is not metadata-only in the harmless sense: it changes `unmined_since`, increments generation, recomputes `delete_at_height`, writes metadata, and atomically updates primary, DAH, and unmined indexes.

Why it matters: After a reorg, replicas can retain stale mined/unmined and DAH state. A promoted replica can prune records the old master preserved, retain records the old master marked deletable, or answer unmined/pruning queries incorrectly. This is a cluster correctness bug under a normal Bitcoin/Teranode condition: longest-chain change.

Reproduction: In RF=2, create a mined transaction, call opcode 12 to mark it off longest chain, then read `unmined_since` / `delete_at_height` from the replica using `FLAG_LOCAL_READ` or promote the replica and query old unmined. The replica should match the master generation and secondary-index membership; current code has no replication path.

Suggested fix: Add `ReplicaOp::MarkLongestChain { tx_key, on_longest_chain, current_block_height, retention, master_generation }`, encode/decode opcode 14, apply it in the receiver with generation gating, and call `replicate_all_ops` from `handle_mark_longest_chain_batch`. Add cluster tests for reorg metadata and DAH/unmined indexes before and after replica promotion.

#### F4. `GetBatch` masks storage corruption as valid empty data

Severity: **HIGH**

Location:
- `src/server/dispatch.rs:4455-4465`
- `src/server/dispatch.rs:4469-4477`
- `src/server/dispatch.rs:4491-4501`
- `src/server/dispatch.rs:4506-4516`

What's wrong: `handle_get_batch` converts `read_slot` errors into 69 zero bytes, `read_cold_data` errors into length 0, `read_conflicting_children` errors into count 0, and any non-`TxNotFound` metadata error into item status 1 (`TX_NOT_FOUND`).

Why it matters: A torn metadata write, bad CRC, missing blob, or device read failure is exposed to clients as a clean "not found" or empty field. That is silent corruption. UTXO clients can make spend/reorg decisions using fabricated slot state instead of an explicit failure.

Reproduction: Corrupt one allocated record's slot or cold-data area, then request the affected field through `OP_GET_BATCH`. The correct response is a per-item storage/internal error. Current code returns an OK item containing zeros or an item status indistinguishable from `TX_NOT_FOUND`.

Suggested fix: Propagate non-`TxNotFound` read failures as explicit item errors. Extend `WireGetResult` status mapping if needed, or return top-level `STATUS_PARTIAL_ERROR` with `ERR_INTERNAL`. Do not synthesize UTXO slot bytes, cold-data length, or conflicting-child count after an I/O or checksum failure.

#### F5. Conflicting-child tracking is best-effort and not redo-durable

Severity: **HIGH**

Location:
- `src/ops/engine.rs:1738-1743`
- `src/ops/engine.rs:1910-1914`
- `src/ops/engine.rs:2520-2529`
- `src/recovery.rs:849-869`

What's wrong: Creating or marking a conflicting child calls `append_conflicting_child` and discards the result. Recovery explicitly states that conflicting-child link replay is intentionally not performed and that the dispatch path treats it as best-effort. The parent metadata update is not journaled by its own redo entry.

Why it matters: The README says metadata contains conflicting children tracking. In Bitcoin/Teranode conflict handling, stale descendant/conflict tracking can cause incomplete descendant identification during reorg/conflict processing. Because errors are swallowed, the system can claim success while the parent-child conflict graph is already incomplete.

Reproduction: Inject a write/allocator failure into `append_conflicting_child` during conflicting create or `SetConflicting(true)`. The operation succeeds, but `FieldMask::CONFLICTING_CHILDREN` on the parent omits the child. Crash after a conflicting create redo and recover through `src/recovery.rs`; the child link is not rebuilt.

Suggested fix: Make conflicting-child link updates first-class durable mutations. Either include parent link changes in the same redo operation and replay them after engine initialization, or maintain a secondary conflict graph rebuilt from primary records during recovery. Return an error if the link update fails unless the spec explicitly declares the field advisory.

### Crash Recovery, Durability, And Tests

#### F6. `cargo test --all` is red on corrupt allocated-record rebuild tests

Severity: **HIGH**

Location:
- `src/index/mod.rs:385-390`
- `src/index/mod.rs:1141-1147`
- `src/index/mod.rs:1202-1208`
- `src/index/backend.rs:951-954`

What's wrong: The implementation returns `corrupt metadata at allocated offset ...` when `TxMetadata::from_bytes` rejects the CRC before the explicit magic check. Three tests still assert the detail string contains `invalid metadata magic`. The suite fails before it can serve as a release gate.

Why it matters: These tests are on the recovery path for allocated-record corruption. A red default suite means CI cannot prove the index rebuild behavior used after snapshot loss or redb rebuild.

Reproduction: Run `cargo test --all`; the three tests above fail.

Suggested fix: Decide the intended error contract. If CRC-first rejection is correct, update the tests to assert `corrupt metadata`/CRC details and add a separate test for a valid CRC with wrong magic if that state is constructible. If magic-specific reporting is required, validate magic before CRC and adjust `TxMetadata::from_bytes` accordingly.

#### F7. Migration failure handshake test is ignored

Severity: **HIGH**

Location:
- `src/cluster/coordinator.rs:7504-7506`

What's wrong: `failed_data_migration_sends_abort_completion_handshake` is ignored with a TODO for pipelined migration flow. This is explicitly about failed data migration and abort completion, which is a data-movement failure path.

Why it matters: Shard migration is where UTXOs can vanish, duplicate, or remain on both old and new masters. An ignored abort/completion test means there is no enforced proof that failed migration state is cleaned up or communicated correctly.

Reproduction: Run ignored tests or remove `#[ignore]`; the test currently is not part of `cargo test --all`.

Suggested fix: Rewrite the test for the pipelined flow and enable it. Add crash variants: source crash mid-baseline, target crash after partial baseline, completion ACK lost, and abort ACK lost.

#### F8. README redb corruption fallback contradicts fail-closed startup code

Severity: **MEDIUM**

Location:
- `README.md:563-568`
- `src/server/startup.rs:220-243`
- `src/server/startup.rs:567-607`

What's wrong: The README says a corrupt redb file is deleted, recreated, and then falls back to in-memory if recreate fails. The current startup code says the opposite for the primary index: restore first, rebuild from device on clean restore error, and on rebuild failure preserve the redb file and fail closed. Tests assert the corrupt redb file is not deleted.

Why it matters: The implemented behavior is safer than the README, but the documented operator playbook is wrong. In an outage, an operator following README semantics may expect automatic fallback and miss that the server is intentionally refusing readiness.

Reproduction: Corrupt `primary.redb` and start with redb config. Code path preserves the file and either rebuilds from device or returns `RebuildError::RedbPrimary`; it does not delete and silently fall back to memory.

Suggested fix: Update README to the fail-closed contract. Document separate behavior for primary and secondary indexes, and add an operator procedure for explicit rebuild/fallback.

### Replication And Cluster Safety

#### F9. Replication compensation swallows local compensation errors

Severity: **HIGH**

Location:
- `src/server/dispatch.rs:4081-4088`
- `src/server/dispatch.rs:4088-4095`
- `src/server/dispatch.rs:1086-1090`

What's wrong: In delete compensation, the return from `handle_replica_batch` is discarded, and the redo write for compensation is also discarded. `clear_replication_intent_after_compensation` can then mark the intent handled even if local compensation failed or was not redo-durable.

Why it matters: After a failed replication attempt, the master may return `ERR_REPLICATION_FAILED` but also fail to restore the pre-request local state. If it then clears the replication intent, startup recovery will not know to repair or re-run the compensation.

Reproduction: Force `handle_replica_batch` or the compensation `write_redo_ops` to fail during delete compensation, then inspect local state and pending replication intent after restart. The code has no branch that reports or persists the failed compensation.

Suggested fix: Treat compensation as a transactional path: write compensation redo, apply it, verify the response, then clear the intent. If any step fails, keep the intent pending and return a hard internal error that marks the node degraded until recovery resolves it.

#### F10. Error-code triggerability is not proven for several README codes

Severity: **HIGH**

Location:
- `src/protocol/opcodes.rs:165-188`
- `src/protocol/opcodes.rs:238-246`
- `src/server/dispatch.rs:5074-5096`

What's wrong: Several README error codes are mapped in code but lack a real client/TCP test proving a client receives the exact code. Some are covered only by engine unit tests, which do not prove wire encoding, sparse error indexing, or client decoding.

Why it matters: For UTXO correctness, the specific error code and payload are part of the protocol contract. `ALREADY_SPENT` must carry the existing 36-byte spending data; `COINBASE_IMMATURE` must carry the height payload; `REDIRECT` must carry an address. Unit-only coverage leaves client-visible behavior unproven.

Reproduction: See the error-code triggerability matrix below. Entries marked missing or partial need client-level tests.

Suggested fix: Add a protocol conformance integration test that drives a real TCP connection or the Rust client for every README code 0-20 and 255, verifies top-level status, sparse item index, error code, and error payload bytes.

### Wire Protocol And Resource Limits

#### F11. No fuzz targets for the binary protocol

Severity: **HIGH**

Location:
- `src/protocol/codec.rs:5010-5028`
- `src/protocol/frame.rs:122`
- `src/server/mod.rs:233-260`

What's wrong: The protocol parser has many binary decoders and checked wrappers, and the TCP handler reconstructs and decodes length-prefixed frames. There is no `fuzz/` target and no `cargo-fuzz`, `libfuzzer`, `honggfuzz`, `arbitrary`, `proptest`, or `quickcheck` dependency found in the repo.

Why it matters: Malformed binary frames are a high-value DoS and panic surface. Unit tests cover selected malformed payloads, but fuzzing is the right tool for length/count/offset combinations and allocation-boundary behavior.

Reproduction: `rg -n "proptest|quickcheck"` returns no matches; `find . -maxdepth 3 -type f -name '*fuzz*'` returns no fuzz targets.

Suggested fix: Add fuzz targets for `RequestFrame::decode`, every `decode_*_checked` function, `ReplicaBatch::deserialize`, routing/topology decoders, and stream chunk/end decoders. Run them in CI with a time budget and preserve crashing seeds.

#### F12. Max-connection rejection is a TCP close, not a clean protocol error

Severity: **MEDIUM**

Location:
- `src/server/mod.rs:120-127`

What's wrong: When `active >= max_connections`, the listener logs a warning and drops the accepted stream. No `ResponseFrame` is sent.

Why it matters: The checklist requires connection N+1 to be rejected cleanly. A raw close forces clients to infer whether this was overload, network failure, or server crash, and it encourages retry storms.

Reproduction: Configure `max_connections = 1`, hold one connection open, and connect a second client. The second connection is closed with no protocol response.

Suggested fix: Send a small `STATUS_ERROR` frame with `ERR_INTERNAL` or add a dedicated overload code, then close. Add a TCP test that verifies the response.

#### F13. Slow response readers can pin server threads indefinitely

Severity: **MEDIUM**

Location:
- `src/server/mod.rs:140-153`
- `src/server/mod.rs:282-286`

What's wrong: Each accepted client gets an OS thread. Reads have a 30-second timeout, but writes use blocking `write_all` with no write timeout. A client that sends valid requests and reads responses at 1 byte/sec can pin its connection thread. Enough such clients exhaust `max_connections`/threads.

Why it matters: Other clients continue only until the connection/thread limit is exhausted. This is a realistic DoS path for an internet-exposed endpoint.

Reproduction: Open `max_connections` clients, send requests with large `GET` responses, and stop reading. The handler blocks in `write_all`.

Suggested fix: Set a write timeout, move to bounded async I/O, or use a bounded response queue with connection eviction on slow consumers. Add a slow-reader integration test.

### Observability And Admin Surface

#### F14. Mutating admin endpoints have no authentication when enabled

Severity: **HIGH**

Location:
- `src/server/http.rs:88-95`
- `src/server/http.rs:116-152`
- `src/config.rs:410-428`
- `tests/http_observability.rs:483-525`

What's wrong: Safe defaults disable mutating admin/debug routes, but if `enable_admin_endpoints = true`, `/admin/quiesce`, `/admin/rebalance`, `/admin/drain/{node_id}`, `PUT /debug/log-level`, `/debug/index`, `/debug/redo`, and `/debug/records/{txid}` are registered with no auth. The code logs a warning that mTLS is pending.

Why it matters: Anyone who can reach the HTTP listener can drain/rebalance/quiesce the node or read sensitive state if an operator enables these endpoints. The checklist calls missing auth on mutating admin endpoints a HIGH finding.

Reproduction: Start with `enable_admin_endpoints = true` and send an unauthenticated `PUT /admin/drain/1`. The route is registered; tests only prove it is 404 when disabled.

Suggested fix: Require mTLS, signed admin tokens, or bind-only local Unix socket for mutating admin/debug routes. Add tests for unauthenticated 401/403 and authenticated success.

### Test Infrastructure

#### F15. No property-based tests for UTXO conservation and concurrency invariants

Severity: **HIGH**

Location:
- `Cargo.toml:16-20`
- `src/ops/engine.rs:3312-3555`
- `src/server/dispatch.rs:2320-2555`

What's wrong: There are many hand-written unit and scenario tests, plus a test-only fault-injection feature, but no property-based testing framework is present. Core invariants like "one success for concurrent same-UTXO spends", "unspend is exact inverse only with same spending data", "spent count equals slot states", and "redo replay is idempotent for any operation sequence" are not generated over hostile operation sequences.

Why it matters: UTXO conservation bugs are exactly the kind of state-machine bugs property tests find. Hand-picked examples are not sufficient for a money-bearing UTXO store.

Reproduction: `rg -n "proptest|quickcheck"` returns no matches.

Suggested fix: Add proptest models for single-record and multi-record UTXO state, generate operation sequences across all mutations, crash/replay after random prefixes, and compare engine/device/index state to a pure model. Run small cases in CI and larger cases nightly.

## Test Coverage Matrix

Legend: ✅ covered, ⚠️ partial, ❌ missing.

| Opcode | Operation | Happy path | Error codes | Batch boundaries | Crash mid-op | Replication failure mid-op | Migration in progress | Single-node vs cluster |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | SpendBatch | ✅ engine/TCP/client | ⚠️ core codes covered, not every wire payload | ✅ duplicate/multi tests | ⚠️ fault tests exist, not full random crash matrix | ⚠️ compensation tests exist, broader cluster cases partial | ⚠️ dispatch unit coverage | ⚠️ cluster scenarios partial |
| 2 | UnspendBatch | ✅ engine/TCP | ⚠️ hash/vout partial | ⚠️ limited | ⚠️ local rollback tests | ⚠️ compensation path tested locally, not chaos | ⚠️ ownership check used | ⚠️ cluster replication partial |
| 3 | SetMinedBatch | ✅ engine/TCP | ⚠️ TxNotFound/internal partial | ⚠️ limited | ⚠️ crash scenario exists | ⚠️ compensation local tests | ⚠️ ownership check used | ⚠️ cluster partial |
| 4 | CreateBatch | ✅ engine/TCP/client | ⚠️ duplicate/blob partial; wire gaps | ✅ max batch TCP | ⚠️ crash scenario exists | ⚠️ compensation local tests | ⚠️ ownership check used | ⚠️ cluster create scenarios |
| 5 | FreezeBatch | ✅ engine/TCP/client | ⚠️ already frozen not real-client proven | ⚠️ limited | ❌ no targeted crash matrix | ⚠️ compensation local tests | ⚠️ ownership check used | ⚠️ cluster partial |
| 6 | UnfreezeBatch | ✅ engine/TCP/client | ⚠️ not frozen not real-client proven | ⚠️ limited | ❌ no targeted crash matrix | ⚠️ compensation local tests | ⚠️ ownership check used | ⚠️ cluster partial |
| 7 | ReassignBatch | ✅ engine/TCP | ⚠️ cooldown/hash partial | ⚠️ limited | ❌ no targeted crash matrix | ⚠️ rollback tests for frozen/reassign | ⚠️ ownership check used | ⚠️ cluster partial |
| 8 | SetConflictingBatch | ✅ engine/TCP | ⚠️ spend-blocking covered; child graph not | ⚠️ limited | ❌ child-link redo missing | ⚠️ replication exists, graph incomplete | ⚠️ ownership check used | ⚠️ cluster partial |
| 9 | SetLockedBatch | ✅ engine/TCP | ⚠️ spend-blocking covered | ⚠️ limited | ❌ no targeted crash matrix | ⚠️ replication exists | ⚠️ ownership check used | ⚠️ cluster partial |
| 10 | PreserveUntilBatch | ✅ engine/unit | ⚠️ wire errors partial | ⚠️ limited | ❌ no targeted crash matrix | ⚠️ replication exists | ⚠️ ownership check used | ⚠️ cluster partial |
| 11 | DeleteBatch | ✅ TCP happy path | ⚠️ TxNotFound partial | ⚠️ limited | ⚠️ redo exists | ❌ CRITICAL rollback corrupts slot state | ⚠️ ownership check used | ⚠️ cluster partial |
| 12 | MarkLongestChainBatch | ✅ local engine/dispatch | ⚠️ wire errors partial | ⚠️ limited | ⚠️ local redo only | ❌ no replication op | ⚠️ ownership check used | ❌ cluster divergence untested |
| 20 | GetBatch | ✅ TCP | ⚠️ `TX_NOT_FOUND` covered; storage errors hidden | ✅ max batch codec/TCP | N/A read | N/A read | ⚠️ transition tests | ⚠️ cluster reads partial |
| 21 | GetSpendBatch | ✅ TCP | ⚠️ vout/hash partial; wire gaps | ⚠️ limited | N/A read | N/A read | ⚠️ transition tests | ⚠️ cluster reads partial |
| 30 | QueryOldUnmined | ✅ dispatch/unit | ⚠️ malformed covered | ⚠️ limited | N/A read | N/A read | ❌ no ownership semantics | ⚠️ degraded secondary checks |
| 31 | PreserveTransactions | ✅ dispatch/unit | ⚠️ malformed covered | ⚠️ max batch checked | ⚠️ local redo | ⚠️ replication called | ⚠️ ownership check used | ⚠️ cluster partial |
| 32 | ProcessExpiredPreservations | ✅ local dispatch | ⚠️ malformed only | ❌ no batch controls | ⚠️ local redo only | ❌ no replication | ❌ no ownership/migration checks | ❌ cluster unsafe |
| 100 | GetPartitionMap | ✅ cluster TCP | ⚠️ not-clustered partial | N/A | N/A | N/A | ⚠️ topology movement partial | ✅ cluster-focused |
| 101 | Health | ✅ dispatch/TCP | ⚠️ not-clustered partial | N/A | N/A | N/A | ⚠️ readiness partial | ⚠️ single/cluster partial |
| 102 | Ping | ✅ TCP | ❌ malformed/error path trivial only | N/A | N/A | N/A | N/A | ✅ single-node; cluster irrelevant |
| 200 | StreamChunk | ⚠️ implementation present | ⚠️ offset mismatch code exists, real-client tests missing | ⚠️ frame cap only | ❌ abandoned/crash cleanup not fully tested | N/A until create | ⚠️ ownership check used | ⚠️ cluster partial |
| 201 | StreamEnd | ⚠️ implementation present | ⚠️ stream-not-found code exists, real-client tests missing | N/A | ❌ abandoned/crash cleanup not fully tested | N/A until create | ⚠️ same connection state only | ⚠️ cluster partial |

## Error-Code Triggerability Matrix

Strict standard: "covered" here means a real client/TCP-level test proves the exact code reaches the client. Engine-only or dispatch-unit tests are marked partial.

| Code | Name | Client triggerability | Evidence / gap |
| --- | --- | --- | --- |
| 0 | OK | ✅ covered | TCP happy paths such as `tests/server_tcp.rs:1404-1416` |
| 1 | TX_NOT_FOUND | ✅ covered | `tests/server_tcp.rs:1115-1120` asserts sparse `ERR_TX_NOT_FOUND` |
| 2 | UTXO_HASH_MISMATCH | ⚠️ partial | Engine tests at `src/ops/engine.rs:3406-3414`; dispatch-unit hash mismatch around `src/server/dispatch.rs:8516-8553`; no real-client proof found |
| 3 | ALREADY_SPENT | ✅ covered | Client scenario asserts `ERR_ALREADY_SPENT` at `teraslab-tests/client/tests/scenario_03_replication_correctness.rs:403-415`; engine payload stability at `src/ops/engine.rs:3433-3443` |
| 4 | ALREADY_FROZEN | ⚠️ partial | Engine only at `src/ops/engine.rs:7082-7103`; no real-client proof found |
| 5 | UTXO_NOT_FROZEN | ⚠️ partial | Engine only at `src/ops/engine.rs:7244-7258` and `src/ops/engine.rs:7334-7351`; no real-client proof found |
| 6 | INVALID_SPEND | ⚠️ partial | Mapped at `src/server/dispatch.rs:5090-5092`; engine pruned test at `src/ops/engine.rs:3460-3471`; no real-client proof found |
| 7 | FROZEN | ✅ covered | Client scenario asserts `ERR_FROZEN` at `teraslab-tests/client/tests/scenario_02_basic_operations.rs:478-510` |
| 8 | CONFLICTING | ✅ covered | Client scenario asserts `ERR_CONFLICTING` at `teraslab-tests/client/tests/scenario_02_basic_operations.rs:719-724` |
| 9 | LOCKED | ✅ covered | Client scenario asserts `ERR_LOCKED` at `teraslab-tests/client/tests/scenario_02_basic_operations.rs:792-797` |
| 10 | COINBASE_IMMATURE | ✅ covered | Client scenario asserts `ERR_COINBASE_IMMATURE` at `teraslab-tests/client/tests/scenario_02_basic_operations.rs:963-990` |
| 11 | VOUT_OUT_OF_RANGE | ⚠️ partial | Engine `GetSpend` at `src/ops/engine.rs:7961-7975`; dispatcher maps it at `src/server/dispatch.rs:4791-4793`; no real-client proof found |
| 12 | ALREADY_EXISTS | ⚠️ partial | Engine duplicate tests at `src/ops/engine.rs:6700` and concurrent duplicate at `src/ops/engine.rs:8530`; dispatch maps at `src/server/dispatch.rs:3154`; no real-client proof found |
| 13 | FROZEN_UNTIL | ⚠️ partial | Engine tests at `src/ops/engine.rs:3474-3491` and `src/ops/engine.rs:7381-7422`; no real-client proof found |
| 14 | REDIRECT | ✅ covered | Cluster TCP redirect tests in `tests/cluster_tcp.rs` around partition/routing scenarios; dispatcher emits address at `src/server/dispatch.rs:2290-2298` |
| 15 | NO_QUORUM | ⚠️ partial | Client scenario comments/assertions in `teraslab-tests/client/tests/scenario_12_concurrent_failures.rs:146-247`; dispatcher code at `src/server/dispatch.rs:2087-2101`; verify exact code/payload in CI |
| 16 | STREAM_NOT_FOUND | ❌ missing | Code emits at `src/server/dispatch.rs:4951-4957`; no real-client test found |
| 17 | BLOB_NOT_FOUND | ❌ missing | Code emits at `src/server/dispatch.rs:3024` and `src/server/dispatch.rs:3042`; no real-client test found |
| 18 | STREAM_OFFSET_MISMATCH | ❌ missing | Code emits at `src/server/dispatch.rs:4911-4918`; no real-client test found |
| 19 | MIGRATION_IN_PROGRESS | ⚠️ partial | Dispatch-unit tests at `src/server/dispatch.rs:6786-6867` and `src/server/dispatch.rs:7985`; no full client migration test proving every write op returns it |
| 20 | REPLICATION_FAILED | ⚠️ partial | Dispatch/replication unit coverage around `src/server/dispatch.rs:7191-7231` and many handlers returning it; no full client test for every replicated mutation |
| 255 | INTERNAL | ✅ covered | Malformed/unknown TCP tests at `tests/server_tcp.rs:1015-1068`; dispatch unit payload decoding at `src/server/dispatch.rs:5636-5856` |

Missing entries above are bugs against the prompt's standard.

## Spec-Vs-Implementation Diff

| Spec claim | Implementation status | Evidence |
| --- | --- | --- |
| README opcodes are 1-12, 20-21, 30-32, 100-102, 200-201 | ⚠️ implementation has more public/inter-node opcodes | `src/protocol/opcodes.rs:29-32` adds 103; `src/protocol/opcodes.rs:65-129` adds 104-106; `src/protocol/opcodes.rs:139-159` has 240-253 |
| README errors are 0-20 and 255 | ⚠️ implementation has additional errors 21-26 and status 5 | `src/protocol/opcodes.rs:195-236`, `src/protocol/opcodes.rs:254-270` |
| `ack_policy = "auto"` resolves by RF | ✅ implemented | `src/config.rs:491-501`: RF 0/1 none, RF 2 WriteAll, RF >=3 WriteMajority |
| `replication_degraded_mode = "best_effort"` is available when ACK policy fails | ⚠️ documented but rejected for RF > 1 | README says option at `README.md:149-152`; validation rejects at `src/config.rs:512-529` |
| Migration defaults are pool 4, batch 100 | ❌ README/config mismatch | README `README.md:153-155`; code defaults pool 128 and batch 500 at `src/config.rs:442-443`; code doc still says batch default 100 at `src/config.rs:380-383` |
| Redb corrupt startup deletes/recreates/falls back to memory | ❌ README stale | README `README.md:563-568`; code fail-closed/preserves file at `src/server/startup.rs:220-243`; test at `src/server/startup.rs:567-607` |
| Peak cluster size persisted to disk | ✅ implemented and tested | README `README.md:473`; persistence functions at `src/cluster/coordinator.rs:4973-5067`; test at `src/cluster/coordinator.rs:8086-8106` |
| Quorum is majority of peak cluster size | ✅ implemented | `src/server/dispatch.rs:2087-2101` |
| HMAC cluster auth drops invalid messages before parsing | ✅ implemented | `src/cluster/swim.rs:433-438`; RF > 1 requires secret at `src/config.rs:665-676` |
| Shard mask is 12 bits (`0x0FFF`) | ✅ implemented | `src/cluster/shards.rs:314-316` |
| 4096 shards, round-robin sorted members | ✅ implemented and tested | `src/cluster/shards.rs:3-10`, `src/cluster/shards.rs:109-111`, tests around `src/cluster/shards.rs:1049-1146` |
| Length-prefixed frame max enforced before allocation | ✅ implemented | `src/server/mod.rs:233-260`; `MAX_FRAME_SIZE` at `src/protocol/opcodes.rs:295-324` |
| Admin endpoints documented as operational surface | ⚠️ gated but unauthenticated when enabled | `src/server/http.rs:88-95`, `src/server/http.rs:142-152` |
| `io_uring` path is stub/sync fallback only | ⚠️ README tree says stub, code attempts real io_uring on Linux | README `README.md:622`; code `src/device_io/mod.rs:87-110` |
| Reads on new master during inbound migration wait briefly | ⚠️ partial | `check_shard_ownership` rejects writes at `src/server/dispatch.rs:2237-2263`; get-path transition tests exist, but full timeout/client behavior during real migration needs stronger coverage |

## Checklist Category Coverage Notes

This table maps the prompt's A-O checklist to the evidence above. "No finding" means I did not find a concrete bug in this pass, not that the area is production-proven.

| Category | Audit result |
| --- | --- |
| A. UTXO correctness invariants | ⚠️ Core engine tests cover spend/freeze/coinbase/hash/vout cases (`src/ops/engine.rs:3312-3555`, `src/ops/engine.rs:7040-7685`), but F1 is a CRITICAL delete rollback double-spend path and F5 leaves conflict graph correctness best-effort. |
| B. Crash recovery and durability | ⚠️ Mutations generally append and flush redo before local mutation through `write_redo_ops` (`src/server/dispatch.rs:985-1004`), and fault-injection exists (`Cargo.toml:16-20`), but the default suite is red (F6), delete compensation is not crash-safe (F1/F9), and ignored migration recovery remains (F7). |
| C. Concurrency | ⚠️ No `.await`-inside-lock issue was found in the synchronous engine paths audited. `lock_stripes` defaults to 65536 (`src/config.rs:422`), but I did not find validation that user-provided stripe counts are power-of-two despite docs (`src/config.rs:265-266`). Property tests for concurrent same-UTXO and overlapping-batch invariants are missing (F15). |
| D. Replication | ❌ `MarkLongestChainBatch` is not replicated (F3), delete compensation is unsafe (F1/F9), and some ACK-policy paths are unit-tested but not comprehensively chaos-tested. `ack_policy=auto` resolution is implemented (`src/config.rs:491-501`). |
| E. Clustering and quorum | ⚠️ Peak cluster size persistence and quorum checks are implemented (`src/cluster/coordinator.rs:4973-5067`, `src/server/dispatch.rs:2087-2101`). HMAC is checked before SWIM parsing (`src/cluster/swim.rs:433-438`). Split-brain healing behavior still needs explicit operator/spec coverage beyond current tests. |
| F. Sharding and migration | ❌ Shard mask is correct (`src/cluster/shards.rs:314-316`), but process-expired bypasses shard ownership (F2) and an explicit migration failure test is ignored (F7). Real migration timeout/redirect loop behavior needs stronger client-level tests. |
| G. Index backends | ⚠️ In-memory/redb implementations and secondary redb durability tests exist, but corrupt allocated-record rebuild tests fail (F6), README redb fallback is stale (F8), and secondary-index degradation behavior needs more client-trigger coverage. |
| H. Wire protocol | ⚠️ Frame max is enforced before allocation (`src/server/mod.rs:233-260`), malformed tests exist (`tests/server_tcp.rs:1015-1068`), but fuzz targets are missing (F11), stream errors lack client tests, and max-connection rejection is not a clean protocol error (F12). |
| I. Storage tiers and blobs | ⚠️ Tier/blob code exists, and create maps missing external blobs to `ERR_BLOB_NOT_FOUND` (`src/server/dispatch.rs:3024`, `src/server/dispatch.rs:3042`), but no real-client triggerability test was found for code 17, orphan cleanup and full-disk behavior need more proof. |
| J. I/O layer | ⚠️ Direct I/O alignment and partial read/write code has tests in `src/device.rs`, and frame/device alignment is heavily asserted. I did not find a direct misalignment bug in this pass. The io_uring README claim is stale because code attempts a real Linux backend (`src/device_io/mod.rs:87-110`). |
| K. Pruning | ❌ `ProcessExpiredPreservations` is unsafe in cluster mode (F2). `MarkLongestChainBatch` not replicating DAH/unmined changes also affects pruning after reorg (F3). |
| L. Resource limits and DoS | ⚠️ Read timeout and frame cap exist (`src/server/mod.rs:211-260`), but clean overload rejection and slow response readers are gaps (F12/F13). |
| M. Observability | ⚠️ Metrics and health endpoints exist; no unbounded txid-label metric was found in the audited metrics surface. Mutating admin endpoints are unauthenticated when enabled (F14). |
| N. Test infrastructure | ❌ No property tests or fuzz targets found (F11/F15). Crash/chaos scenarios exist in `teraslab-tests`, but the default `cargo test --all` is red and one migration failure test is ignored. |
| O. Bitcoin / Teranode-specific concerns | ⚠️ Coinbase immature client scenario exists (`teraslab-tests/client/tests/scenario_02_basic_operations.rs:963-990`), but reorg handling is not replicated (F3), conflicting descendants are not durable (F5), and pruning/reorg interaction is therefore not production-proven. |

## Dead Code, TODO, Panic, Unwrap, Unsafe Inventory

This inventory excludes ordinary test assertions unless they expose a production gap.

| Pattern | Location | Assessment |
| --- | --- | --- |
| `#[ignore] // TODO` | `src/cluster/coordinator.rs:7504-7506` | Finding F7; migration failure path untested |
| `let _ = append_conflicting_child` | `src/ops/engine.rs:1742`, `src/ops/engine.rs:1913`, `src/ops/engine.rs:2529` | Finding F5; silently drops parent conflict-link failures |
| Recovery intentionally skips conflict links | `src/recovery.rs:849-869` | Finding F5 |
| Dropped compensation response/redo | `src/server/dispatch.rs:4081-4095` | Finding F9 |
| Dropped slot/cold/child read errors | `src/server/dispatch.rs:4462-4516` | Finding F4 |
| `expect("failed to create replication tokio runtime")` | `src/server/dispatch.rs:76-83` | Startup/runtime construction panic; acceptable only if process abort on runtime creation failure is intended. Prefer returning fatal startup error. |
| `expect("failed to create tokio runtime for HTTP server")` | `src/server/http.rs:72-75` | HTTP thread panics if runtime creation fails. For production, return/log fatal error to startup supervisor instead of panic. |
| `expect("failed to spawn blob uploader thread")` | `src/storage/uploader.rs:90-95` | Non-test fallible path panics on OS thread spawn failure. Convert `BlobUploader::new` to return `Result`. |
| `unreachable!()` in client retry loops | `client/rust/src/lib.rs:930-941`, `client/rust/src/lib.rs:1429-1441` | Bounded loops make this logically unreachable, but `unreachable!` still panics in a client library. Replace with explicit retry-exhausted error. |
| `unreachable!("checked above")` | `src/index/hashtable.rs:992-996` | Justified by preceding branch over `Backing`, but can be changed to an error for hardening. |
| `panic!` in `SyncFallback` impossible branch | `src/device_io/mod.rs:110-116` | Justified as impossible by `SyncFallback::new`; acceptable low risk, but returning sync init error is cleaner. |
| `try_into().expect("4 bytes")` | `src/server/dispatch.rs:5228-5235` | Justified by explicit payload length check immediately before it. |
| Unsafe metadata byte casts | `src/record.rs:535-624` | Has safety comments and CRC validation; high-risk but locally justified. |
| Unsafe engine Send/Sync and direct pointer I/O | `src/ops/engine.rs:74-75`, `src/ops/engine.rs:550-608`, `src/ops/engine.rs:1010`, `src/ops/engine.rs:1577` | Safety depends on lock discipline around `device_ptr`; no direct finding found, but this is a critical invariant worth documenting with lock-order tests. |
| Unsafe mmap hash table impls | `src/index/hashtable.rs:238-316`, `src/index/hashtable.rs:498-499`, `src/index/hashtable.rs:649-657` | Mmap/raw-pointer implementation is expected for the backend. Continue requiring safety comments and stress/proptest coverage at high load factor. |
| Unsafe direct I/O allocation | `src/device.rs:250-311`, `src/device.rs:501-571`, `src/device.rs:618-664` | Alignment and partial I/O tests exist; no direct bug found. |
| `TODO` comments | `src/cluster/coordinator.rs:7505` | Only production-relevant TODO found by `rg TODO|FIXME|HACK` outside diagnosis docs. |
| Unused code | `src/server/startup.rs:209`, `src/server/dispatch.rs:1606`, `src/server/dispatch.rs:5116`, `src/cluster/coordinator.rs:3751`, `src/cluster/coordinator.rs:4910`, `src/cluster/coordinator.rs:5041`, `src/bin/server.rs:945` | Explicit `#[allow(dead_code)]`; `cargo clippy --all -- -D warnings` passes, but each allow should be revisited and either linked to a future issue or removed. |

## Action Plan

### Milestone 1: Things That Could Lose UTXO Data

1. Fix delete compensation so it restores exact slot status/spending data and writes durable compensation redo before clearing replication intent.
2. Make `ProcessExpiredPreservations` shard-owned, replicated, and migration-aware, or remove it from the client wire surface.
3. Add replication support for `MarkLongestChainBatch` and verify promoted replicas match master DAH/unmined state after reorg.
4. Stop masking `GetBatch` storage errors as zeros or `TX_NOT_FOUND`.
5. Make conflicting-child tracking durable or explicitly remove it from consensus-critical semantics.

### Milestone 2: Recovery And Migration Confidence

1. Fix the three failing index rebuild tests and restore green `cargo test --all`.
2. Rewrite and enable the ignored migration abort/completion test.
3. Add crash-injection tests for delete rollback, process-expired pruning, mark-longest-chain, conflicting-child graph updates, and stream upload finalization.
4. Add real cluster tests for source/target crash during baseline migration, delta streaming, completion ACK loss, and abort ACK loss.

### Milestone 3: Protocol And Client Contract

1. Add one real-client/TCP conformance test for every error code 0-20 and 255, including payload bytes.
2. Add fuzz targets for frame, codec, replica, topology, and stream decoders.
3. Make overload/max-connection rejection a protocol response.
4. Add write timeouts or bounded async output for slow clients.

### Milestone 4: Operator Safety And Documentation

1. Require authentication for mutating admin/debug routes when enabled.
2. Update README for redb fail-closed behavior, actual migration defaults, extra opcodes/errors, and io_uring behavior.
3. Add a documented production test matrix: memory and redb backends, RF=1/2/3, migration under load, crash recovery, and chaos scenarios.
4. Replace production `expect`/`unreachable` panics in client/server startup paths with explicit errors where practical.
