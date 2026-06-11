# Error-Code Triggerability Matrix (Category N audit, 2026-06-11)

Authoritative list: `src/protocol/opcodes.rs` (codes 0–35, 255; README §Error codes matches).

**Evidence tiers** — what "proven" means here:
- **WIRE**: a real TCP client (`TcpStream` + framed request) receives the exact code. This is the gold standard; it covers dispatch + frame encode/decode + the listener path.
- **DISPATCH**: asserted via `handle_request()` in `src/server/dispatch.rs` `#[cfg(test)]` (real engine, no socket). Skips TCP framing, HMAC auth, rate limiter, connection state.
- **UNIT**: asserted in some other in-process unit test.
- **NONE**: no test ever asserts a caller receives this code (constant-value pins like `assert_eq!(ERR_X, 34)` in `src/protocol/codec.rs::~L2864` do NOT count — they prove the number, not the behavior).

`tests/error_code_conformance.rs` verdict: **it is genuinely wire-level** — it starts a real `Server`, connects a `TcpStream`, and asserts status + sparse index + code + exact payload bytes. But it only covers codes 6, 30, 35, 255 (T-1..T-6). The broader wire workhorse is `tests/server_tcp.rs::tcp_error_code_triggerability_core_item_errors`.

| Code | Name | Tier | Proof / gap |
|---|---|---|---|
| 0 | OK | WIRE | implicit in every STATUS_OK assertion (e.g. `server_tcp.rs::create_spend_get_spend`) |
| 1 | TX_NOT_FOUND | WIRE | `server_tcp.rs::request_for_nonexistent_tx_partial_error` (sparse index + code asserted) |
| 2 | UTXO_HASH_MISMATCH | WIRE | `server_tcp.rs::get_spend_wire_validates_utxo_hash` + `tcp_error_code_triggerability_core_item_errors` |
| 3 | ALREADY_SPENT | WIRE | `tcp_error_code_triggerability_core_item_errors` — asserts the 36-byte FIRST-winner payload |
| 4 | ALREADY_FROZEN | WIRE | same test |
| 5 | UTXO_NOT_FROZEN | WIRE | same test |
| 6 | INVALID_SPEND | WIRE | `error_code_conformance.rs::t4` (Pruned → 6 + preserved 36-byte spending_data) and `t6` (ReservedSpendingData → 6 + EMPTY payload discriminator) |
| 7 | FROZEN | WIRE | `tcp_error_code_triggerability_core_item_errors` |
| 8 | CONFLICTING | WIRE | same test |
| 9 | LOCKED | WIRE | same test |
| 10 | COINBASE_IMMATURE | WIRE | same test — asserts 4-byte required-height payload (`server_tcp.rs` ~L792) |
| 11 | VOUT_OUT_OF_RANGE | WIRE | same test (~L385) |
| 12 | ALREADY_EXISTS | WIRE | same test (~L377, duplicate create) |
| 13 | FROZEN_UNTIL | WIRE | same test — asserts 4-byte spendable-height payload (~L605) |
| 14 | REDIRECT | WIRE | `cluster_tcp.rs::during_migration_writes_redirect_to_new_node` (~L877–886); follow-loop classification unit-tested in codec |
| 15 | NO_QUORUM | WIRE | `cluster_tcp.rs::isolated_node_rejects_writes_with_no_quorum` (~L1736–1751, asserts payload header shape too); live-partition variant in `cluster_partition.rs` |
| 16 | STREAM_NOT_FOUND | WIRE | `server_tcp.rs::stream_end_without_active_stream_returns_stream_not_found` |
| 17 | BLOB_NOT_FOUND | WIRE | `server_tcp.rs::external_blob_create_without_uploaded_blob_returns_blob_not_found` |
| 18 | STREAM_OFFSET_MISMATCH | WIRE | `server_tcp.rs` (~L1846) |
| 19 | MIGRATION_IN_PROGRESS | WIRE | `cluster_tcp.rs::tcp_write_to_pending_inbound_shard_returns_migration_in_progress` (~L1810) |
| 20 | REPLICATION_FAILED | WIRE | `cluster_tcp.rs::tcp_strict_replication_failure_returns_replication_failed` (~L1348) |
| 21 | MIGRATION_MANIFEST_REQUIRED | DISPATCH | `src/server/dispatch.rs` ~L10462 (also ~L9382/9445). No wire test — inter-node path, acceptable risk but the HMAC-framed path is unproven |
| 22 | MIGRATION_MANIFEST_MISMATCH | DISPATCH | `src/server/dispatch.rs` ~L10531, ~L9500 |
| 23 | TOPOLOGY_PERSIST_FAILED | DISPATCH | `src/server/dispatch.rs` ~L11128 (vote-fsync failure → no-vote semantics) |
| 24 | STALE_EPOCH | UNIT | `src/replication/receiver.rs` ~L4475, 4656, 4701 — receiver-level, never over a replication TCP socket |
| 25 | CLUSTER_NOT_READY | DISPATCH | `src/server/dispatch.rs` ~L10136 (Joining node rejects mutations; AdminClusterHealth bypass also pinned ~L10159). No wire test |
| 26 | INDEX_DEGRADED | DISPATCH | `src/server/dispatch.rs` ~L12984/13001/13023 (degraded-gated opcodes). `tests/integration.rs` covers the rebuild-degrades-explicitly half but never the client-visible code. HTTP readiness side in `src/server/http.rs`. No wire test |
| 27 | CLUSTER_AUTH_FAILED | WIRE | `g5_protocol_auth.rs::strict_auth_rejects_unsigned_inter_node_frame` + `strict_auth_gates_admin_opcodes` (real TCP); streaming variant `g5_slow_loris_streaming.rs` |
| 28 | PAYLOAD_MALFORMED | DISPATCH | `src/server/dispatch.rs` ~L7319/7397/7539 and others. **Gap**: the wire test that exists for this exact scenario (`server_tcp.rs::malformed_payload_returns_error`) asserts only `STATUS_ERROR` and never checks the code — written pre-P3.10 and never upgraded |
| 29 | OPCODE_UNSUPPORTED | DISPATCH | `src/server/dispatch.rs` ~L7570. Same gap: `server_tcp.rs::invalid_opcode_returns_error` sends opcode 999 over the wire but asserts only `STATUS_ERROR` |
| 30 | STORAGE_IO | WIRE | `error_code_conformance.rs::t2` (batch-wide, DAH overflow, message content asserted), `t3a/t3b` (sparse per-item, empty payload asserted) |
| 31 | RATE_LIMITED | WIRE | `server_tcp.rs::max_connection_rejection_sends_error_frame` (~L1802, message asserted). ⚠️ Only the max-connections trigger; the in-flight memory-limit trigger documented for this code is untested |
| 32 | NOT_CLUSTERED | **NONE** | emitted at `dispatch.rs` ~L991/1030/1059/6782; only a constant-value pin exists (`codec.rs` ~L2868). Trivially triggerable (send OP_GET_PARTITION_MAP to a single-node server) — no test does it |
| 33 | INVARIANT_VIOLATION | **NONE** | emitted at `dispatch.rs` ~L537/601/5000 (upper-48-bits-of-request_id guard); only constant-value pin. Triggerable from a client frame; untested |
| 34 | STREAM_INVARIANT | **NONE** | emitted at `dispatch.rs` ~L6395/6407/6465 (offset mismatch at apply, byte-counter overflow, stream byte cap); only constant-value pin. Note code 18 covers the *chunk-arrival* offset mismatch; the 34-paths are distinct and untested |
| 35 | DELETED_CHILDREN | WIRE | `error_code_conformance.rs::t5` — full resurrect-then-prune setup, asserts 1-byte child_count payload |
| 255 | INTERNAL | WIRE | `error_code_conformance.rs::t1` (stream chunk without blobstore, message asserted) |

Status codes: 0–4 all wire-asserted across `server_tcp.rs`/`cluster_tcp.rs`. **Status 5 `DEGRADED_DURABILITY`: DISPATCH only** (`src/server/dispatch.rs` ~L9720/9799) — no client over a socket has ever received it in a test, despite it being the durability-weakening response clients must special-case.

## Findings summary

- **5 codes have NO behavioral test**: 32, 33, 34 (none at all) — plus 28/29 whose only wire tests don't check the code. All five are client-facing v2 typed codes that README documents and that the `PROTOCOL_VERSION = 2` bump exists for.
- **6 codes are in-process only**: 21, 22, 23, 24, 25, 26 (+ status 5). The inter-node ones (21–24) are lower-risk; 25/26 and status 5 are client-facing and should have wire proof.
- 24/38 codes+statuses (63%) have true wire-level proof.
