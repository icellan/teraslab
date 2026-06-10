# Category L — Resource Limits and DoS

HEAD: branch `main`, commit `1e5659b`.

## Tooling note

The Bash/Grep tool channel delivered output on a delay during this session, but ALL
material was ultimately received and verified. I have a complete verified read of
`src/server/mod.rs` (full accept loop + per-connection handler), the body of
`handle_stream_chunk`/`handle_stream_end` in `src/server/dispatch.rs`, the
`STREAM_CHUNK_SIZE`/tail-window structure of `verify_signed_body_streaming` in
`src/cluster/auth.rs`, and the dispatch call sites that pass `max_batch` into the
`decode_*_checked` decoders. Every line:number citation below was read directly.
Net result: NO real defects in this category; both items I initially flagged as
uncertain resolved to verified-OK except one genuine LOW hardening gap (L-01).

---

## Verified-OK checklist items (with proof)

### 1. Idle/silent client is timed out — CONFIRMED
- `CONNECTION_READ_TIMEOUT = 30s` (`src/server/mod.rs:46`), applied via
  `stream.set_read_timeout(Some(opts.read_timeout))` at `mod.rs:618-620` for every connection
  before the read loop.
- The initial 4-byte length-prefix read (`stream.read_exact(&mut len_buf)`, `mod.rs:651`) is
  therefore covered by the read timeout. A `TimedOut` error returns `Ok(())` (clean close) at
  `mod.rs:655`.
- **Proven by an existing test that actually verifies behavior:** `silent_client_dropped_after_idle_timeout`
  (`mod.rs:961-997`) connects a client that sends nothing, runs the handler with a 50 ms read
  timeout, and asserts via `rx.recv_timeout(2s)` that the handler returns (it does not hang).
  This is a real behavioral test, not an `is_ok()`-only smoke test.

### 2. One slow client does not block others — CONFIRMED (thread-per-connection)
- Each accepted connection is handed to `std::thread::spawn` (`mod.rs:500-523`); the read/write
  loop runs on its own OS thread. A slow or stuck reader blocks only its own thread, not the
  accept loop (the accept loop is a mio poller, `mod.rs:330-535`) nor other connections.
- The accept loop itself does NOT read frame bytes; it only accepts and spawns, so a slow client
  cannot stall new accepts.

### 3. Write timeout is wired — CONFIRMED
- `CONNECTION_WRITE_TIMEOUT = 30s` (`mod.rs:47`), applied at `mod.rs:629-631`
  (`set_write_timeout`). The response `write_all` at `mod.rs:907-909` is therefore bounded; a
  client that stops reading (TCP zero-window) cannot pin the server thread forever — the write
  errors out and the connection closes. The fix is documented inline at `mod.rs:621-628`
  (R-054 / LMNH-01).
- The `max_connections`-reject path also sets a write timeout before its error write
  (`mod.rs:450`), so the reject write cannot block either.

### 4. Per-connection / per-frame memory is bounded — CONFIRMED
- Oversized-frame guard runs BEFORE any per-connection buffer growth: `total_length >
  max_wire_frame_size` is rejected at `mod.rs:665-682`, where `max_wire_frame_size = MAX_FRAME_SIZE
  (16 MiB) + optional SIGNED_SUFFIX_LEN`. So one frame's buffer is capped at ~16 MiB.
- The persistent `read_buf` is shrunk back to `READ_BUF_RETAINED_SIZE` (256 KiB) immediately after
  each frame is split off (`reset_read_buf_if_oversized`, `mod.rs:869`, `mod.rs:917-924`), so a
  16 MiB peak frame does not stay pinned across dispatch. Test `read_buf_shrinks_after_small_frame`
  (`mod.rs:948-958`) verifies the shrink.

### 5. Aggregate in-flight memory across all connections is bounded — CONFIRMED
- `InflightBytesLimiter` (`mod.rs:53-142`) enforces `config.max_inflight_request_bytes`
  (default 256 MiB) across ALL connection threads via an atomic CAS counter. A permit is acquired
  for `frame_len` before the body is read (`mod.rs:688-704`); on failure the connection is rejected
  with `ERR_RATE_LIMITED` and closed. RAII `Drop` releases bytes (`mod.rs:136-142`).
- A single frame larger than the whole cap is rejected (`mod.rs:79-84`), and arithmetic overflow on
  the cumulative counter is handled (`mod.rs:88-95`). Tests `inflight_request_limiter_caps_aggregate_bytes`
  (`mod.rs:1129-1144`) and `inflight_bytes_rejected_metric_increments_on_overflow` (`mod.rs:1373-1420`)
  verify the cap and the rejection metric, including a negative control.

### 6. Per-source-IP connection cap — CONFIRMED
- `max_connections_per_ip` (default 64) enforced in the accept loop BEFORE spawning a thread and
  before writing any bytes (`mod.rs:420-445`); over-quota connections get a silent close
  (intentional, so the attacker can't probe the cap). RAII `PerIpGuard` (`mod.rs:200-217`)
  decrements on thread exit (normal/err/panic) and GCs empty map entries so the map can't grow
  unbounded.

### 7. Global connection cap — CONFIRMED
- `max_connections` (default 1024) checked at `mod.rs:447-467`; over-cap connections get
  `ERR_RATE_LIMITED` then close. Counter incremented at `mod.rs:469`, decremented on thread exit
  at `mod.rs:521`.

### 8. Slow-loris on the signed inter-node body — CONFIRMED
- Signed inter-node frames are NOT materialized in full before HMAC verify; the body is streamed
  through `verify_signed_body_streaming` into a disposable sink that is dropped on auth failure
  (`mod.rs:809-845`). The verifier reads in bounded `STREAM_CHUNK_SIZE = 8 KiB` chunks
  (`src/cluster/auth.rs:351`) and keeps only a `SIGNED_SUFFIX_LEN`-byte tail window
  (`auth.rs:391, 398, 419-420`), so verifier-side memory is O(8 KiB) regardless of advertised
  frame size — confirming the claimed bound and the slow-loris HMAC-amplifier fix.

### 9. Streaming blob-upload accumulation cap — CONFIRMED
- `handle_stream_chunk` (`src/server/dispatch.rs:6140`): offset contiguity is enforced
  (`if chunk.offset != stream.bytes_received` → `ERR_STREAM_OFFSET_MISMATCH`, :6184-6193); the
  cumulative byte counter uses `checked_add` to defend against a u64-wrap bypass (:6199-6206); the
  per-stream cap is enforced BEFORE the write (`projected > max_stream_total_bytes` →
  `ERR_STREAM_INVARIANT`, :6209-6219); and EVERY failure path (overflow, cap, write error) removes
  the session from `conn_state.streams` and calls `writer.abort()` (:6201-6203, 6210-6212,
  6225-6227). `handle_stream_end` rejects a size mismatch and aborts (:6263-6271). Verified by the
  real test `stream_chunk_aborts_when_cumulative_bytes_exceed_cap` (dispatch.rs:7563), which sends a
  second chunk pushing the total past the 1024-byte cap and asserts the session is removed
  (dispatch.rs:7633).

### 10. Per-request allocation bound — CONFIRMED
- All server-side batch decoders are invoked through their `*_checked` variants with the
  connection's configured `max_batch` (= `ServerConfig::max_batch_size`, default 8192), e.g.
  `decode_spend_batch_checked(&req.payload, max_batch)` (dispatch.rs:2784), set_mined (:3231),
  create (:3492), reassign (:4128), txid batches (:4254, 4380, 4487, 4684, 5135, 5733), get
  (:5277), get_spend (:5995). The dispatch trunk threads `max_batch_size` from the connection
  options into every handler (dispatch.rs:427-469). `validate_batch_count` (codec.rs:141) rejects
  `count > max_batch` and a truncation check BEFORE any `Vec::with_capacity(count)`
  (codec.rs:9, 56, 134). The unchecked legacy wrappers fall back to `MAX_DECODE_BATCH = 1<<20`
  (codec.rs:109) but are NOT on the server request path — the server uses the `_checked` variants.
  Verified by `decode_*_checked_rejects_u32_max_count` tests (codec.rs:3464, 3346-3359).

---

## Findings

### L-01 (LOW) — Idle timeout is per-read, not per-frame: a 1-byte-every-29s client can hold a connection indefinitely

**What's wrong.** The read timeout (`mod.rs:46`, applied `mod.rs:618-620`) is a per-syscall
socket timeout: it resets on every successful `read`. A client that sends 1 byte every <30 s keeps
`read_exact` making forward progress and never trips the timeout. For a 16 MiB frame an attacker can
legitimately hold one connection thread for up to ~16M × 30 s while consuming its inflight-bytes
permit and one of the `max_connections` / `max_connections_per_ip` slots.

**Why it matters.** This is the classic slow-loris drip. It is *mitigated* (not eliminated) by:
the per-IP cap (64 conns/IP), the global cap (1024), and the 256 MiB aggregate inflight limiter —
so a single IP can tie up at most 64 threads, and total pinned memory is bounded. It is NOT a
memory-exhaustion or accept-loop-starvation bug. But there is no whole-frame deadline, so a modest
botnet (or many IPs) can still hold all 1024 connection slots near-indefinitely with trivial
bandwidth, denying service to legitimate clients. Severity LOW because the hard caps bound the blast
radius and prevent OOM; it is a hardening gap, not a crash.

**Locations:** `src/server/mod.rs:46`, `mod.rs:618-620`, `mod.rs:651`, `mod.rs:857-859`.

**Reproduction.** No existing test covers the drip case. `silent_client_dropped_after_idle_timeout`
(`mod.rs:961`) only covers a *fully silent* client (which trips the per-read timeout). A test that
sends 1 byte just under the timeout interval in a loop would show the connection is never dropped.

**Suggested fix.** Add a whole-frame deadline: record `Instant::now()` when the length prefix is
read and abort if the full frame is not assembled within e.g. `read_timeout` (or a small multiple),
independent of per-read progress. Alternatively enforce a minimum throughput.

**Confidence:** high (the per-read vs per-frame semantics of `set_read_timeout` are well-defined and
the code has no overriding deadline).

_(Initial L-02 concern about the streaming blob-upload cap was RESOLVED to verified-OK once the
delayed tool output arrived — see checklist item 9 above. No second finding.)_
