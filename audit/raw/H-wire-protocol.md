# Category H — Wire Protocol Audit

Scope: `src/protocol/{frame,codec,opcodes,deadline,mod}.rs`, `src/server/{mod,dispatch,startup}.rs`, streaming op handling, and the wire-protocol tests.

Verdict up front: the frame/codec boundary is genuinely well-hardened — length-before-alloc, checked arithmetic, typed errors, real fuzzing (deterministic smoke + cargo-fuzz). The findings below are concentrated in the **streaming op state machine** (no concurrent-stream cap, no idle-stream reaper) and one **retry-safety hazard** in the response-write path. No remote pre-auth panic or memory-corruption was found.

---

### [HIGH] Unbounded concurrent blob-stream sessions per connection (fd / tmp-file exhaustion)

**Location:** `src/server/mod.rs:155-187` (`ConnectionState.streams: HashMap<[u8;32], ActiveStream>`); `src/server/dispatch.rs:6354-6369` (`handle_stream_chunk` get-or-create); `src/storage/blobstore.rs:842-859` (`FileBlobStore::begin_stream` — opens a real file + `.tmp`).

**What's wrong:** `OP_STREAM_CHUNK` inserts one `ActiveStream` per distinct txid into a per-connection `HashMap` with **no cap on the number of entries**. `grep` for `streams.len()` / `max_streams` / `max_active_streams` returns nothing — the only stream-related limit is `max_stream_total_bytes`, which is enforced *per stream* (`dispatch.rs:6400-6412`), not across streams. Each new txid causes `begin_stream` to `std::fs::File::create` a `.tmp` file and hold the handle open inside `ActiveStream.writer`. A client that owns a shard can send one tiny chunk (offset 0) for thousands of distinct txids on a single connection, each opening a file descriptor and a tmp file, and keep them all open.

**Why it matters:** A single connection (or `max_connections_per_ip` × 64 connections) can exhaust the process file-descriptor table and fill the blob tmp directory. The per-stream byte cap does not help — each stream needs only one chunk to stay resident. This is a clean DoS reachable by any client that passes the shard-ownership check (`dispatch.rs:6336`), which in single-node mode is everyone.

**Reproduction:** Unit-style: build a `ConnectionState`, then loop ~100k times calling `handle_stream_chunk` with `encode_stream_chunk(&random_txid, 0, &[0u8;16])` against an in-memory/file blob store; assert `conn_state.streams.len()` grows unbounded (it does). Against a real `FileBlobStore`, observe one open fd + one `.tmp` per iteration until `EMFILE`. Network: open a TCP connection, send N `OP_STREAM_CHUNK` frames each with a fresh txid and offset 0; server fd count climbs to the ulimit.

**Suggested fix:** Add `ServerConfig::max_active_streams_per_connection` (e.g. 256). In `handle_stream_chunk`, on the `Entry::Vacant` branch, reject with `ERR_STREAM_INVARIANT` (or a new `ERR_TOO_MANY_STREAMS`) when `conn_state.streams.len() >= cap` before calling `begin_stream`.

---

### [HIGH] Abandoned blob streams are only reaped on connection close — no idle-stream timer

**Location:** `src/server/mod.rs:180-187` (`Drop for ConnectionState` aborts streams); `src/server/dispatch.rs:6318-6489` (no per-stream last-activity timestamp anywhere); `src/protocol/deadline.rs` (the only timeout is `FRAME_ASSEMBLY_TIMEOUT`, which bounds a *single frame*, not a stream's lifetime).

**What's wrong:** The checklist asks "abandoned streams cleaned up (timer? on what trigger?)". The answer is: **only on connection drop**. There is no idle-stream reaper and no `last_activity: Instant` on `ActiveStream`. As long as the connection stays alive (the client can keep it alive cheaply with periodic `OP_PING` frames, which reset the per-read timeout), a half-finished stream — its open file, tmp file, and `bytes_received` state — lives forever. `FRAME_ASSEMBLY_TIMEOUT` (60 s, `deadline.rs:32`) only bounds the assembly of one frame; it imposes no bound on the gap *between* `OP_STREAM_CHUNK` frames or on a stream that never receives `OP_STREAM_END`.

**Why it matters:** Amplifies the previous finding. A client opens a stream, sends one chunk, then idles (or sends an occasional ping) and holds the fd/tmp/state indefinitely. Combined with no concurrent-stream cap, this is a durable resource leak that survives indefinitely without the connection ever erroring.

**Reproduction:** Connect, send one `OP_STREAM_CHUNK` (offset 0) for a txid, then send `OP_PING` every 10 s indefinitely. Observe that the `.tmp` file and fd persist with no server-side expiry. Contrast with the `silent_client_dropped_after_idle_timeout` test (`mod.rs:1028`) which only covers a client that sends *nothing* — a pinging client is never dropped.

**Suggested fix:** Stamp `ActiveStream` with `last_activity: Instant`, refresh on each chunk, and either (a) sweep on each request entry within the connection loop, aborting streams older than a configurable idle TTL, or (b) bound total stream lifetime from `begin_stream`. Abort + remove from the map and `writer.abort()` the tmp file.

---

### [MEDIUM] Mutation applied but response can be dropped on slow-reader write timeout (retry hazard)

**Location:** `src/server/mod.rs:947-977` (dispatch then `write_all` under `write_timeout`); `src/server/dispatch.rs:340-490` (no request-execution deadline — the only deadline in the codebase, `deadline.rs`, governs *inbound frame assembly* only).

**What's wrong:** The connection loop fully executes the handler (`dispatch::handle_request`, which performs the WAL-fsync'd mutation — see `handle_spend_batch` WAL-first ordering at `dispatch.rs:2829-2834`) and *then* calls `stream.write_all(&response_bytes)` under a 30 s `CONNECTION_WRITE_TIMEOUT` (`mod.rs:46-47`, `672-674`). If a slow-reading client never drains its socket, `write_all` returns `TimedOut`, the handler returns `Err`, and the connection closes — **after** the mutation is durably applied and replicated, but **with the response never delivered**. There is no request-level deadline that aborts execution before the mutation; the checklist's "mutation applied but response dropped" case is real here.

**Why it matters:** The client cannot distinguish "mutation applied, ack lost" from "mutation never ran", so it must retry. The blast radius is limited because the data-plane mutations are designed to be idempotent (spend re-apply is a counted no-op — `dispatch.rs:2908-2910`; create returns `ERR_ALREADY_EXISTS`; set-mined is idempotent), so a retry is generally safe. But the contract is not documented as "all client mutations are retry-safe", and any non-idempotent op (or an op whose idempotency depends on identical `spending_data`) would be a correctness hazard on retry. This is a protocol-contract gap, not a corruption bug — hence MEDIUM.

**Reproduction:** Connect, send a valid `OP_SPEND_BATCH`, and never read the response (don't drain the recv buffer); keep the socket open. After `write_timeout` the server closes with `write response` error. Independently query the record (e.g. `OP_GET_SPEND_BATCH`) on a fresh connection and confirm the spend *was* applied despite the client receiving no ack.

**Suggested fix:** Document the retry-safety contract explicitly (all client-facing mutations must be idempotent on `request_id`/payload), and ideally have the handler key idempotency on `request_id` so a retry is provably a no-op. At minimum, add a test asserting the applied-but-unacked invariant and the idempotent retry path.

---

### [LOW] Malformed inter-node payloads report `ERR_MIGRATION_IN_PROGRESS` instead of `ERR_PAYLOAD_MALFORMED`

**Location:** `src/server/dispatch.rs:622-693` (`OP_MIGRATION_COMPLETE` manifest parsing: "malformed exact-manifest entry count", "malformed exact-manifest generation", "malformed migration completion source node" all return `ERR_MIGRATION_IN_PROGRESS`).

**What's wrong:** Several genuine wire-decode failures on the migration-complete path are returned as `ERR_MIGRATION_IN_PROGRESS` (a transient/retryable signal) rather than the typed `ERR_PAYLOAD_MALFORMED` introduced in P3.10 for exactly this purpose. The batched sibling `OP_MIGRATION_BATCH_COMPLETE` (lines 920-946) correctly uses `ERR_PAYLOAD_MALFORMED`, so the two paths disagree.

**Why it matters:** A peer that receives `ERR_MIGRATION_IN_PROGRESS` will retry the same malformed frame indefinitely instead of treating it as a hard "your bytes are wrong" error. It's inter-node (authenticated when `cluster_secret` set), so the surface is a buggy/mismatched peer rather than an attacker — hence LOW. Inconsistency also undermines the typed-error-code conformance the codebase otherwise enforces.

**Reproduction:** Send `OP_MIGRATION_COMPLETE` with `payload.len() >= 60` and an `entry_count` that does not match the trailing bytes (e.g. set entry_count then truncate the generation of the last entry); observe `ERR_MIGRATION_IN_PROGRESS` rather than `ERR_PAYLOAD_MALFORMED`.

**Suggested fix:** Change the three "malformed …" arms at `dispatch.rs:625-630,670-676,684-690` to `ERR_PAYLOAD_MALFORMED`, matching the batched path.

---

### [LOW] `decode_error_payload` / `decode_redirect` length fields are not used to allocate but trust attacker length implicitly elsewhere — verified bounded (hardening note)

**Location:** `src/protocol/codec.rs:1422-1433` (`decode_error_payload`), `1834` (`decode_stream_chunk` data_len bound), `1867-1875` (`decode_stream_end`).

**What's wrong:** Nothing actively wrong — recorded as a positive verification. Every length field that gates a slice (`msg_len`, `data_len`, `total_size`) is checked against `data.len()` *before* the slice (`codec.rs:1428`, `1834`). No `Vec::with_capacity(attacker_len)` exists on these response/stream decoders; they slice or copy only after the bound check. The `as usize` narrowing of `u32` data_len is safe on 64-bit and the subsequent `payload.len() < 44 + data_len` check catches the truncated case. Left as LOW only because `decode_error_payload` returns `Option` (silently `None`) rather than a typed error, which is fine for a client-side response decoder.

**Reproduction:** Covered by `wire_fuzz_smoke.rs` and the cargo-fuzz target — both feed `u32::MAX` length-field inflation through these paths with no panic.

**Suggested fix:** None required.

---

## Detailed checklist walk-through (evidence)

**Length-prefixed frame: max length enforced BEFORE allocation.**
`parse_request_header` (`frame.rs:165-220`) checks `total_length < MIN_FRAME_BODY` then `total_length > MAX_FRAME_SIZE` (16 MiB, `opcodes.rs:477`) *before* computing `frame_size` and before any `Bytes::copy_from_slice`/`slice`. The server connection loop (`mod.rs:708-725`) re-checks `total_length > max_wire_frame_size` immediately after reading the 4-byte prefix and **before** `read_buf.resize(...)` — so a frame claiming `u32::MAX` is rejected with `ERR_STORAGE_IO`/"frame too large" without ever allocating 4 GiB. Verified ✅. The `bytes::BytesMut` read buffer only grows to `4 + frame_len` (`mod.rs:909-911`) after the cap passed, and is shrunk back via `reset_read_buf_if_oversized` right after `split_to` (`mod.rs:919-933`).

**Malformed payloads → clean error, not drop/panic.**
Every per-opcode decoder is a `decode_*_checked` returning `Result<_, CodecError>`; dispatch handlers convert via `codec_error_response` → `ERR_PAYLOAD_MALFORMED` (`dispatch.rs:2800-2802` and the parallel sites). Frame-level decode failure in the loop maps to a `String` error that closes the connection cleanly (`mod.rs:947-948`) — but note a frame that decodes structurally yet carries a bad payload produces an in-band error response, not a drop. Unknown opcode → `ERR_OPCODE_UNSUPPORTED` (`dispatch.rs:1103`). The fuzz harness asserts "Ok or typed error, never panic" across all 17 decoders. ✅

**max_batch_size enforced before processing.**
`validate_batch_count` (`codec.rs:141-169`) checks `count > max_batch` first, then `count * per_item_min <= available` with `checked_mul`, *before* `Vec::with_capacity(count)`. The configured `max_batch_size` is plumbed from `ServerConfig` through every handler. A `dispatch_does_not_use_legacy_unchecked_decoders` regex test (referenced `codec.rs:276`) forbids production use of the `MAX_DECODE_BATCH` wrappers. ✅

**max_connections enforced; N+1 rejected cleanly.**
`mod.rs:484-504`: if `active >= max_connections`, the server writes an `ERR_RATE_LIMITED` response and drops the stream, releasing the per-IP guard. Per-IP cap (`max_connections_per_ip`, default 64) enforced earlier (`mod.rs:457-482`) with a silent close (deliberately no frame, to avoid leaking the cap). ✅

**Connection close mid-request cleans up.**
`ConnectionState::Drop` (`mod.rs:180-187`) aborts all in-progress stream writers. `InflightBytesPermit::Drop` (`mod.rs:143-149`) releases the reserved bytes. `PerIpGuard::Drop` decrements the IP tally even on panic (`mod.rs:212-224`). Locks are striped and released at engine-call scope, not held across the connection lifetime. Cancellation path verified ✅ — **except** that abandoned streams on a *still-open* connection are never reaped (see HIGH finding above).

**Streaming: offset mismatch / hijack / cleanup.**
Offset mismatch → `ERR_STREAM_OFFSET_MISMATCH` (`dispatch.rs:6372-6381`). ✅ Stream bound to connection: `streams` lives in the per-connection `ConnectionState`, keyed by txid; another connection has a separate map, so no cross-connection hijack of a stream id is possible. ✅ Cleanup-by-timer: ❌ — only on connection drop (HIGH finding). Byte-counter overflow guarded with `checked_add` → `ERR_STREAM_INVARIANT` (`dispatch.rs:6387-6399`); per-stream total cap enforced before write (`6400-6412`). `OP_STREAM_END` size mismatch aborts + `ERR_STREAM_INVARIANT` (`6461-6471`); unknown txid → `ERR_STREAM_NOT_FOUND` (`6449-6457`).

**Integer parsing from network input.**
Checked arithmetic is used consistently at the hostile boundaries: `validate_batch_count` `checked_mul` (`codec.rs:154`); create-batch utxo/parent counts `checked_mul(32)` with explicit per-item caps `MAX_UTXO_HASHES_PER_CREATE_ITEM`/`MAX_PARENT_TXIDS_PER_CREATE_ITEM`/`MAX_COLD_DATA_PER_ITEM` (`codec.rs:821-948`); migration entry_count `checked_mul(36).checked_add(60)` (`dispatch.rs:639-653`); get-response cumulative `data_len` tally with `checked_add` (`codec.rs:1208-1220`); stream byte counter `checked_add` (`dispatch.rs:6387`); inflight bytes `checked_add` (`mod.rs:95`). Narrowing `as usize` casts are all preceded by a bound check against the buffer. No hostile multiplication overflow found. ✅

**Duplicate items within one batch.**
Defined-but-implicit: `handle_spend_batch` groups items `by_txid` (`dispatch.rs:2812-2816`); duplicate (txid, vout) pairs flow into `validate_spend_multi` where the second is treated as an idempotent re-spend (same spending_data) or a per-item error (different data) — not rejected at the wire layer. Behavior is deterministic but **not documented** as a wire-protocol contract. ⚠️ (folded into the retry-hazard MEDIUM as a documentation gap; not separately filed).

**Response framing under partial write.**
`write_all` is all-or-error under `write_timeout`; a partial write that times out returns `Err` and closes the connection (no half-frame is left "committed" on the wire from the server's perspective — the client simply sees a truncated/closed stream). This is the mechanism behind the MEDIUM retry-hazard finding. ⚠️

**Request deadline mid-execution.**
No request-execution deadline exists — `deadline.rs` governs inbound frame assembly only. Mutation runs to completion regardless of elapsed time; only the post-execution `write_all` can fail. See MEDIUM finding. ⚠️

**Fuzz coverage.**
Real, not token. `tests/wire_fuzz_smoke.rs` is a deterministic seeded fuzzer (3000 iterations: random + structure-aware mutations) over all 17 boundary decoders, asserting per-decoder that both Ok and Err paths were hit thousands of times and that `Display` is exercised — a genuine "never panic" contract test. A **cargo-fuzz target exists**: `fuzz/fuzz_targets/decode_request.rs` (libFuzzer, `#![no_main]`, dual batch-cap sweep), with `fuzz/{Cargo.toml,corpus,artifacts}` present. Both kept in sync by comment convention. ✅

---

## Checklist disposition

- Length-prefix max enforced before alloc: ✅ (`frame.rs:179-197`, `mod.rs:708-725`)
- Malformed payload → clean typed error, no panic/drop: ✅ (fuzz-proven)
- max_batch_size before processing: ✅ (`codec.rs:141-169`)
- max_connections / per-IP reject N+1 cleanly: ✅ (`mod.rs:457-504`)
- Connection-close cleanup (locks/streams/buffers/permits): ✅ for drop path; ❌ for idle-stream reaping on live connection (HIGH)
- Stream offset mismatch detected: ✅ (`dispatch.rs:6372`)
- Stream bound to originating connection (no hijack): ✅ (per-connection map)
- Abandoned stream cleanup (timer): ❌ — connection-drop only, no idle timer (HIGH)
- Concurrent-stream count cap: ❌ — none (HIGH)
- Integer parsing checked / no hostile overflow: ✅
- Duplicate items in batch defined: ⚠️ deterministic but undocumented
- Response framing under partial write: ⚠️ times out → close (feeds retry hazard)
- Request deadline mid-execution / applied-but-unacked: ⚠️ mutation applied, response can drop (MEDIUM)
- Typed error-code consistency on inter-node malformed payloads: ⚠️ `ERR_MIGRATION_IN_PROGRESS` vs `ERR_PAYLOAD_MALFORMED` (LOW)
- Fuzz coverage real + cargo-fuzz target present: ✅

Findings: 2 HIGH, 1 MEDIUM, 2 LOW. No CRITICAL (no pre-auth remote panic or corruption found).
