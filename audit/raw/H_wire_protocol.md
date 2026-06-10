# Audit Category H — Wire Protocol (TeraSlab @ HEAD 1e5659b)

Primary files read in full this session:
- `src/protocol/frame.rs` (688 lines)
- `src/server/mod.rs` (1421 lines — accept loop + connection handler)
- `src/server/dispatch.rs` regions: opcode router (473-590), stream handlers
  (6010-6291), spend handler (6480+), legacy-decoder guard test (7148+),
  stream tests (7550-7720)
- `src/protocol/codec.rs` regions: `validate_batch_count` (141-169),
  `decode_spend_batch_checked` (220-268)
- `src/protocol/opcodes.rs`: `MAX_FRAME_SIZE` (477), `is_inter_node_auth_opcode`
  (488-503), per-item caps (518-534)

Verdict: the wire protocol is **well-defended**. Every checklist item is correctly
handled and backed by a real test that asserts behavior (not just `Ok(_)`). No
CRITICAL/HIGH findings. One LOW hardening note and one LOW doc-coverage note.

---

## VERIFIED-OK checklist

### 1. Length-prefix max enforced BEFORE allocation — PASS (two layers)
- **Frame decoder layer:** `parse_request_header` rejects `total_length >
  MAX_FRAME_SIZE` (16 MiB, opcodes.rs:477) at `frame.rs:185-190`, before computing
  `frame_size` (191) or copying/slicing any payload. A `u32::MAX` length never
  allocates. Mirrored for responses at `frame.rs:259-263`.
- **Socket loop layer (the real money path):** `handle_connection_inner` reads only
  the 4-byte prefix, then at `mod.rs:665-682` computes
  `max_wire_frame_size = MAX_FRAME_SIZE (+ SIGNED_SUFFIX_LEN iff a cluster_secret is
  set)` and rejects with `STATUS_ERROR "frame too large"` BEFORE the per-connection
  `read_buf.resize(4 + frame_len, 0)` at `mod.rs:851-852`. So an advertised 4 GiB
  frame is refused before any multi-GB buffer growth. There is additionally an
  aggregate cross-connection cap (`InflightBytesLimiter`, mod.rs:53-142) acquired at
  `mod.rs:688` before the body read, bounding the sum of concurrent in-flight frame
  bytes (default 256 MiB).
- Tests: `too_large_frame_rejected` (frame.rs:462), `max_payload_frame`
  (frame.rs:436), `inflight_request_limiter_caps_aggregate_bytes` (mod.rs:1130).

### 2. Malformed payloads return a clean error response, not panic/drop — PASS
- Unknown opcode → `ERR_OPCODE_UNSUPPORTED` (dispatch.rs:588), not a drop.
- Batch handlers decode via the `_checked` decoders and convert any `CodecError`
  into a `STATUS_ERROR`/`ERR_PAYLOAD_MALFORMED` response via `codec_error_response`
  (dispatch.rs:6321). Confirmed in `handle_spend_batch` (dispatch.rs:6488-6491).
- `codec.rs` decoders are bounds-checked: `validate_batch_count` runs before
  `Vec::with_capacity` (codec.rs:237 then 239), and per-item loops re-check
  `pos + 104 > data.len()` (codec.rs:245) as belt-and-braces. `get_u32/get_u16`
  index only after a header-length guard (e.g. `data.len() < 14` at codec.rs:224).
- Stream decoders return `Option` and the handlers map `None` →
  `ERR_PAYLOAD_MALFORMED` (dispatch.rs:6148, 6248). `decode_stream_chunk`
  (dispatch.rs:6029) rejects any payload where `payload.len() != 44 + data_len`
  (line 6040) — fixing the documented pre-fix panic on a long read (R-045/GH-08).
- Frame header parse is panic-free: the four `try_into()` calls map to `FrameError`
  rather than unwrapping (frame.rs:175,205,210,215).
- Test guard `dispatch_does_not_use_legacy_unchecked_decoders` (dispatch.rs:7160)
  compile-time-scans production source to forbid the unchecked `decode_*_batch(`
  decoders (which would fall back to `MAX_DECODE_BATCH = 1<<20`).

### 3. max_batch_size enforced (size+1 rejected before processing) — PASS
- `validate_batch_count(count, max_batch, per_item_min, available)` returns
  `CodecError::BatchTooLarge` when `count > max_batch` (codec.rs:147-152) BEFORE the
  `Vec::with_capacity(count)`. `max_batch` is the operator's
  `ServerConfig::max_batch_size` (8192), threaded from `mod.rs:474` → dispatch →
  `decode_spend_batch_checked(&payload, max_batch_size)` (dispatch.rs:6488).

### 4. max_connections / max_connections_per_ip enforced, N+1 rejected cleanly — PASS
- Per-IP cap checked first (mod.rs:420-445): if `count >= max_connections_per_ip`
  (default 64) the socket is **silently dropped** (mod.rs:434) — deliberate, so an
  attacker can't measure the cap. RAII `PerIpGuard` (mod.rs:200-217) decrements on
  thread exit (normal/err/panic) and GCs empty map entries.
- Global cap checked next (mod.rs:447-467): if `active >= max_connections`
  (default 1024) it returns a `STATUS_ERROR`/`ERR_RATE_LIMITED` frame and drops the
  stream, AND `drop(per_ip_guard)` releases the per-IP slot reserved a moment
  earlier (mod.rs:465) — no per-IP leak on the global-cap path.

### 5. Connection close mid-request cleans up locks/streams/buffers — PASS
- Streams: `ConnectionState` is per-connection (created mod.rs:641, owned by the
  connection thread). Its `Drop` (mod.rs:173-180) drains `streams` and calls
  `writer.abort()` on every in-progress upload. Test
  `connection_state_drop_aborts_streams` (dispatch.rs ~7660).
- Buffers: the per-connection `read_buf` is a thread-local `BytesMut`; it is shrunk
  back to `READ_BUF_RETAINED_SIZE` after each frame (mod.rs:869) and freed when the
  thread ends. The `_inflight_permit` RAII (mod.rs:688, Drop at mod.rs:136-142)
  releases the aggregate-bytes reservation when the iteration/thread unwinds.
- Locks: batch handlers decode and validate BEFORE acquiring engine locks (comment
  + structure at dispatch.rs:6487); lock acquisition is scoped within the engine
  call per request, so a connection drop between requests holds no locks. (Engine
  lock-scoping is category-G territory; the dispatch layer holds no long-lived locks
  across the read loop.)

### 6. Streaming ops — offset mismatch, abandoned-stream cleanup, connection binding — PASS
- **Offset mismatch → ERR_STREAM_OFFSET_MISMATCH (18):** `handle_stream_chunk`
  compares `chunk.offset != stream.bytes_received` and returns the typed error
  (dispatch.rs:6184-6193), and crucially does NOT advance `bytes_received` on
  mismatch. Tests `stream_chunk_offset_mismatch_returns_error` and
  `stream_chunk_wrong_offset_does_not_advance` (dispatch.rs ~7560-7620).
- **Abandoned stream cleanup:** on connection close via `ConnectionState::Drop`
  (item 5). On error paths (write error, overflow, cap exceeded) the session is
  removed + aborted inline (dispatch.rs:6202, 6210, 6225). `OP_STREAM_END` removes
  the session (dispatch.rs:6251) and aborts on size mismatch (6263-6273).
- **Stream-id-to-connection binding (no cross-connection hijack):** STRUCTURALLY
  ENFORCED. There is no global stream registry. Streams live only in
  `ConnectionState.streams` (mod.rs:148-151), keyed by txid, reachable solely
  through the `&mut conn_state` that `handle_request` receives for the current
  connection (dispatch.rs:515-516). Connection B has its own `ConnectionState` and
  literally cannot name or reach connection A's `ActiveStream`. A second connection
  sending `OP_STREAM_CHUNK` for the same txid starts a fresh session in its own map;
  it cannot continue or finalize A's upload. Hijack is impossible by construction.
- **Per-stream byte cap:** `checked_add` + `max_stream_total_bytes` (default 4 GiB)
  enforced before write (dispatch.rs:6199-6220), with overflow → `ERR_STREAM_INVARIANT`.

### 7. Partial-read vs corrupt-stream distinction — PASS (defense in depth)
- The socket loop reads exact frame lengths (`read_exact`), so it never relies on
  `decode_frames`/`try_decode_frames` for live framing — partial TCP reads block
  inside `read_exact` and a clean EOF/timeout returns `Ok(())` (mod.rs:651-657).
- The helper `try_decode_frames` (frame.rs:338) correctly returns `Ok` on
  `Truncated` (refill) and `Err` on `TooLarge/TooShort/BelowMinimum` (disconnect),
  tested at frame.rs:527 and 552.

---

## Findings

### H-01 (LOW, hardening) — legacy `decode_frames` swallows the corrupt-frame variant
`frame.rs:310-323`: `decode_frames` matches `Err(_) => break`, collapsing corrupt
(`TooLarge`/`TooShort`/`BelowMinimum`) and partial (`Truncated`) tails into the same
outcome. It is **not** on the live socket path (the connection loop uses
`read_exact` + `decode_bytes`), and its doc-comment marks it compat-only, so impact
is negligible today. Risk is purely future-proofing: a new caller could adopt it on
a socket and lose the corrupt-vs-partial signal. Fix: gate `decode_frames` behind
`#[cfg(test)]` or delete it in favor of `try_decode_frames`.

### H-02 (LOW, docs) — README omits wire error codes 28–35 and OP_HELLO (107)
Cross-referenced from the surface inventory (opcodes.rs has `ERR_PAYLOAD_MALFORMED`
=28 … `ERR_DELETED_CHILDREN`=35 and `OP_HELLO`=107; README error/opcode tables stop
at 27/255 and don't list OP_HELLO). A client built strictly from README cannot
interpret those codes or the handshake op. Fix: extend the README tables. (Doc-only;
the wire behavior itself is correct and `ERR_PAYLOAD_MALFORMED` is actively returned
by the malformed-payload path.)
