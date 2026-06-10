# Error-Code Triggerability Matrix — TeraSlab

**HEAD:** `1e5659b` · **Date:** 2026-05-29 · **Scope:** README error codes 0–20 + 255, plus every `SpendError` variant in `src/ops/error.rs`.

## Method

"Client-observable" = a test that decodes an actual wire response frame and asserts the numeric
`error_code` (u16) / `status` (u8) field the client receives — NOT a test that constructs the
enum variant or inspects an in-process engine `Result`.

Two tiers accepted as client-observable:

1. **Real TCP round-trip** (strongest): `tests/server_tcp.rs`, `tests/cluster_tcp.rs`,
   `tests/replication_tcp.rs`, `tests/cluster_edge_cases.rs` — open a socket, send a framed
   request, decode the framed response (`decode_sparse_errors` / `decode_error_payload` /
   `decode_get_spend_response`), assert the wire field.
2. **Dispatch round-trip**: a test that calls `dispatch_request(...)` and decodes the resulting
   `ResponseFrame` / `BatchItemError`.

Engine-only assertions — `engine.spend(...).unwrap_err()` matched against `SpendError::X`, or
inspecting `spend_multi` result `.errors[i]` — do NOT count. They prove the engine produces the
variant, not that a client receives the wire code. (e.g. `tests/g2_reserved_spending_data.rs`
exercises the `ReservedSpendingData` trigger thoroughly but only at the engine level — it never
builds a `ResponseFrame`, so the wire code is unverified.)

## Mapping reference (verified)

`SpendError` → wire `error_code` via `spend_error_to_batch_error()` at
`src/server/dispatch.rs:6368-6420`. Collapses:

- `UtxoNotFound` → `ERR_VOUT_OUT_OF_RANGE` (11) — dispatch.rs:6379 *(no distinct code)*
- `Pruned` and `ReservedSpendingData` → `ERR_INVALID_SPEND` (6) — dispatch.rs:6400, 6412
- `DahOverflow`, `ReassignOverflow` → `ERR_INTERNAL` (255)
- `StorageError` → `ERR_STORAGE_IO` (30)
- `DeletedChildren` → `ERR_DELETED_CHILDREN` (35, payload = 1-byte child_count) — dispatch.rs:6419

Constants: `src/protocol/opcodes.rs:201-423`. README table: `README.md:340-381`.

---

## A. README error codes 0–20 + 255

| Code | Name | Proving test (client-observable) | Status |
|---|---|---|---|
| 0 | OK | `tests/server_tcp.rs:228 ping_pong` + pervasive `assert_eq!(resp.status, STATUS_OK)` | COVERED |
| 1 | TX_NOT_FOUND | `tests/server_tcp.rs:1000 create_set_mined_delete` (get-spend after delete → `error_code, ERR_TX_NOT_FOUND`) + `:1729 request_for_nonexistent_tx_partial_error` | COVERED |
| 2 | UTXO_HASH_MISMATCH | `tests/server_tcp.rs:364 get_spend_wire_validates_utxo_hash` asserts `results[0].error_code, ERR_UTXO_HASH_MISMATCH` over TCP | COVERED |
| 3 | ALREADY_SPENT | `tests/server_tcp.rs:662` (in `tcp_error_code_triggerability_core_item_errors`) asserts `ERR_ALREADY_SPENT` + `error_data == winner_spending_data` over TCP | COVERED |
| 4 | ALREADY_FROZEN | `tests/server_tcp.rs:443` asserts `ERR_ALREADY_FROZEN` over TCP | COVERED |
| 5 | UTXO_NOT_FROZEN | `tests/server_tcp.rs:475` asserts `ERR_UTXO_NOT_FROZEN` over TCP | COVERED |
| 6 | INVALID_SPEND | `tests/server_tcp.rs:532` (wrong unspend-marker path) asserts `ERR_INVALID_SPEND` + 36-byte error_data over TCP | COVERED |
| 7 | FROZEN | `tests/server_tcp.rs:695` asserts `ERR_FROZEN` over TCP | COVERED |
| 8 | CONFLICTING | `tests/server_tcp.rs:727` asserts `ERR_CONFLICTING` over TCP | COVERED |
| 9 | LOCKED | `tests/server_tcp.rs:759` asserts `ERR_LOCKED` over TCP | COVERED |
| 10 | COINBASE_IMMATURE | `tests/server_tcp.rs:792` asserts `ERR_COINBASE_IMMATURE` + `error_data == 1_100u32.to_le_bytes()` over TCP | COVERED |
| 11 | VOUT_OUT_OF_RANGE | `tests/server_tcp.rs:412` asserts `ERR_VOUT_OUT_OF_RANGE` over TCP | COVERED |
| 12 | ALREADY_EXISTS | `tests/server_tcp.rs:383` asserts `ERR_ALREADY_EXISTS` over TCP | COVERED |
| 13 | FROZEN_UNTIL | `tests/server_tcp.rs:605` asserts `ERR_FROZEN_UNTIL` + `error_data == 1_010u32.to_le_bytes()` over TCP | COVERED |
| 14 | REDIRECT | `tests/cluster_tcp.rs:885` asserts `error_code, ERR_REDIRECT` over TCP (decodes shard_table_version) | COVERED |
| 15 | NO_QUORUM | `tests/cluster_tcp.rs:1749 isolated_node_rejects_writes_with_no_quorum` asserts `error_code, ERR_NO_QUORUM` over TCP | COVERED |
| 16 | STREAM_NOT_FOUND | `tests/server_tcp.rs:1872 stream_end_without_active_stream_returns_stream_not_found` asserts `code, ERR_STREAM_NOT_FOUND` over TCP | COVERED |
| 17 | BLOB_NOT_FOUND | `tests/server_tcp.rs:1902 external_blob_create_without_uploaded_blob_returns_blob_not_found` asserts `error_code, ERR_BLOB_NOT_FOUND` over TCP | COVERED |
| 18 | STREAM_OFFSET_MISMATCH | `tests/server_tcp.rs:1846 stream_isolation_per_connection` asserts `code, ERR_STREAM_OFFSET_MISMATCH` over TCP | COVERED |
| 19 | MIGRATION_IN_PROGRESS | `tests/cluster_tcp.rs:1835 tcp_write_to_pending_inbound_shard_returns_migration_in_progress` over TCP | COVERED |
| 20 | REPLICATION_FAILED | `tests/cluster_tcp.rs:1908 tcp_strict_replication_failure_returns_replication_failed` over TCP | COVERED |
| 255 | INTERNAL | none — see finding **T-1** | **NOT COVERED** |

---

## B. `SpendError` variants (`src/ops/error.rs`) — distinct trigger coverage

| Variant (error.rs) | Wire code | Proving test | Status |
|---|---|---|---|
| `TxNotFound` (l.13) | 1 | server_tcp:1000, :1729 | COVERED |
| `Conflicting` (l.17) | 8 | server_tcp:727 | COVERED |
| `Locked` (l.21) | 9 | server_tcp:759 | COVERED |
| `CoinbaseImmature` (l.25) | 10 | server_tcp:792 (+error_data) | COVERED |
| `UtxoNotFound` (l.34) | 11 | server_tcp:412 | COVERED |
| `UtxoHashMismatch` (l.41) | 2 | server_tcp:364 | COVERED |
| `AlreadySpent` (l.48) | 3 (+36B) | server_tcp:662 (+error_data) | COVERED |
| `Frozen` (l.57) | 7 | server_tcp:695 | COVERED |
| `FrozenUntil` (l.64) | 13 (+4B) | server_tcp:605 (+error_data) | COVERED |
| `InvalidSpend` (l.73) | 6 (+36B) | server_tcp:532 (+error_data) | COVERED |
| `Pruned` (l.82) | 6 (collapsed) | none drives the `Pruned` trigger over the wire — **T-4** | **NOT COVERED** |
| `AlreadyFrozen` (l.91) | 4 | server_tcp:443 | COVERED |
| `NotFrozen` (l.98) | 5 | server_tcp:475 | COVERED |
| `StorageError` (l.105) | 30 | none — **T-2** | **NOT COVERED** |
| `DahOverflow` (l.119) | 255 | none at any layer — **T-3** | **NOT COVERED** |
| `ReassignOverflow` (l.135) | 255 | engine-Result only (engine.rs:10023/10031); NO wire test — **T-3** | **NOT COVERED (wire)** |
| `DeletedChildren` (l.153) | 35 | none — **T-5** | **NOT COVERED** |
| `ReservedSpendingData` (l.173) | 6 (collapsed) | engine-only (`tests/g2_reserved_spending_data.rs` asserts `SpendError::ReservedSpendingData` from `engine.spend`/`spend_multi`, never a wire frame) — **T-6** | **NOT COVERED (wire)** |

---

## FINDINGS — codes lacking client-observable coverage

### T-1 — Code 255 `ERR_INTERNAL` has no client-observable test
No test in `tests/` asserts a client receives `ERR_INTERNAL` (255). Produced at multiple dispatch
sites (e.g. dispatch.rs:2382 cluster-control catch-all) and the wire code for both overflow
variants (T-3). grep for `ERR_INTERNAL`/`, 255)`/`== 255` across `tests/` returns only a comment
in `g5_protocol_auth.rs:94` ("must NOT be ERR_INTERNAL"), never a positive assertion. Only wire
code in the README 0–20+255 set with zero coverage; it carries the overflow guards whose failure
mode is permanently-pinned UTXOs.

### T-2 — `StorageError` → code 30 `ERR_STORAGE_IO` has no client-observable test
Maps to `ERR_STORAGE_IO` (30), produced at ~20 dispatch sites (dispatch.rs:2935, 2949, 3137,
3268, 3597, 3656, 3681, 3688, 3742, 3843, 3863, 4303, 4409, 4525, 4728, 4812, …). No test
asserts a client receives code 30. The fault-injection harness (`tests/fault_injection.rs`)
does not assert this wire code. A device I/O failure on a money-path mutation surfacing the wrong
code would not be caught. (Code 30 also undocumented in the README error table — see Notes.)

### T-3 — `DahOverflow` and `ReassignOverflow` (→ 255) are untested at the wire layer
Both guard against `u32` overflow that previously caused silent `saturating_add` clamping
(error.rs:126-140: a real bug that pinned UTXOs unspendable forever). `DahOverflow`: ZERO refs
under `tests/`. `ReassignOverflow`: engine-Result tests only (engine.rs:10023, :10031
`assert_eq!(err, SpendError::ReassignOverflow)`) — no test asserts the resulting wire code 255.
Both collapse to `ERR_INTERNAL` (untested, T-1). A regression re-introducing the silent clamp
would not produce a failing wire test.

### T-4 — `Pruned` (→ 6) distinct trigger has no client-observable test
`SpendError::Pruned` (error.rs:82, `UTXO_PRUNED` deleted-child rejection) maps to
`ERR_INVALID_SPEND` (6) with 36-byte spending_data (dispatch.rs:6400). Code 6 is wire-tested,
but server_tcp.rs:532 drives the *unspend wrong-marker* `InvalidSpend` path, NOT the `Pruned`
deleted-child trigger. The distinct `Pruned` rejection and its pruned-entry spending_data payload
are never exercised over the wire.

### T-5 — `DeletedChildren` (code 35) has no client-observable test
Maps to `ERR_DELETED_CHILDREN`=35 (dispatch.rs:6419, single-byte child_count payload). Neither
`DeletedChildren` nor `ERR_DELETED_CHILDREN` appears anywhere under `tests/` (grep returns
nothing; symbols live only in `src/`). This is the F-X-022 Aerospike `addDeletedChildren` parity
guard against resurrected-then-pruned double-spends — a money-safety path — yet no test verifies
the client receives code 35 or its child_count. A regression collapsing it back to code 6 would
pass silently.

### T-6 — `ReservedSpendingData` (→ 6) has no client-observable test
The F-G2-002 guard rejecting an all-`0xFF` spending_data that would brick a slot
(error.rs:173 → `ERR_INVALID_SPEND` 6, dispatch.rs:6412). `tests/g2_reserved_spending_data.rs`
exercises the trigger but ONLY at the engine level: asserts `engine.spend(...).unwrap_err()`
matches `SpendError::ReservedSpendingData` and inspects `spend_multi` `.errors[i]` — never builds
a `ResponseFrame` or asserts a wire `error_code`. A regression in this variant's dispatch mapping
(or loss of the guard at the dispatch boundary) would not be caught at the client level.

---

## Notes

- **README documentation gap (related):** wire codes 28–35 (`ERR_PAYLOAD_MALFORMED` …
  `ERR_DELETED_CHILDREN`) and `ERR_STORAGE_IO`=30 exist in `opcodes.rs:322-382` but are absent
  from the README error table (README.md:340-370). Two (`ERR_STORAGE_IO`=30,
  `ERR_DELETED_CHILDREN`=35) are the wire codes for live `SpendError` variants (T-2, T-5).

- **Strongest single test:** `tests/server_tcp.rs:370 tcp_error_code_triggerability_core_item_errors`
  drives codes 3,4,5,6,7,8,9,10,11,12,13 over real TCP with per-code error_data assertions. NOT
  incidental.

## Coverage summary

- **README scope (0–20 + 255):** 21/22 covered. **NOT COVERED: 255 (ERR_INTERNAL).**
- **`SpendError` variants (18):** 12/18 covered. **NOT COVERED (client-observable):**
  `Pruned`(6-collapsed), `StorageError`(30), `DahOverflow`(255), `ReassignOverflow`(255),
  `DeletedChildren`(35), `ReservedSpendingData`(6-collapsed).
