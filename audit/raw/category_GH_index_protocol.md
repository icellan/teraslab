# Audit — Categories G and H: Index Backends and Wire Protocol DoS

**Auditor:** Claude (Opus 4.7, 1M context)
**Scope:** TeraSlab src/index/* and src/protocol/* + src/server/dispatch.rs frame
dispatch surface, plus the related integration tests under tests/.
**Date:** 2026-05-06

## Overview

TeraSlab takes the wire-protocol DoS surface seriously. The `MAX_FRAME_SIZE`
ceiling at 16 MiB (lowered from a permissive 512 MiB) is enforced *before*
any per-connection buffer growth, every batch decoder has a `*_checked`
variant that bounds `Vec::with_capacity(count)` against both the configured
`max_batch_size` and the available payload bytes, and the streaming-blob
path is bound to a per-connection `ConnectionState` so a foreign connection
cannot hijack an active stream id. A handful of issues remain — the most
serious is **a u32-times-32 multiplication for migration-complete exact
manifest entries that bypasses the centralized batch validator** (see
GH-04), and **streaming chunks with attacker-controlled `chunk.data.len()`
are written straight to the blob writer with no per-stream cap** (GH-09).

Index backends are well-engineered for the in-memory and file-backed paths
(snapshot uses temp+rename, restore handles partial corruption per H6) and
the redb backend has an explicit fail-closed policy on rebuild failure that
preserves the file (no silent fall-back). Two notable gaps:

1. **`Index::deserialize_primary_with_offset` does an unchecked
   `count * PRIMARY_ENTRY_SIZE` multiplication** when reading the snapshot
   header (GH-G1). Snapshot files are local trusted state today, but this is
   the attack surface that any supply-chain or mis-mounted-volume attacker
   would target.
2. **Index `import_index` is not transactional across the three redb files**
   (GH-G3). A crash between writing primary and dah/unmined leaves the three
   files inconsistent, with no restart-time reconciliation.

Categories G and H findings follow. Severities: **CRITICAL** = must fix
before production exposure; **HIGH** = exploitable from network input but
constrained; **MEDIUM** = robustness/defence-in-depth; **LOW** = code
hygiene.

---

## Category H — Wire protocol DoS / parsing

### GH-01: `MAX_FRAME_SIZE` ceiling is enforced before allocation (POSITIVE)
**Category:** H
**Location:** `src/server/mod.rs:239-258`, `src/protocol/frame.rs:122-127`,
`src/protocol/opcodes.rs:295-324`
**What:** The TCP loop reads the 4-byte length prefix, checks `total_length
> MAX_FRAME_SIZE` (16 MiB) BEFORE growing the per-connection
`read_buf` via `resize`, and emits an error frame + closes the connection
when the cap is exceeded. The `RequestFrame::decode` and `ResponseFrame::
decode` paths repeat the same check at the codec layer and additionally
enforce a `MIN_FRAME_BODY = 1` floor (rejects `total_length = 0`) and a
fixed-header floor (`MIN_REQUEST_BODY = 12`).
**Why it matters:** A frame claiming `total_length = u32::MAX` (≈4 GiB) is
rejected before any `Vec::resize`, so a single hostile client cannot drive
a per-connection 4 GiB allocation. The `tests/server_tcp.rs:1125
oversized_frame_rejected` test exercises the path against a real socket.
**Reproduction:** `cargo test --test server_tcp oversized_frame_rejected`
covers it. Manual repro: send `[0x01, 0x00, 0x00, 0x01, ...]` (16 MiB + 1)
and observe the connection drop with `frame too large` payload.
**Suggested fix:** N/A — this is correctly implemented. Worth adding a
metric counter for `frame_too_large_count` so operators can detect probing.

### GH-02: Batch decoders enforce `max_batch_size` AND payload-fit before allocation (POSITIVE)
**Category:** H
**Location:** `src/protocol/codec.rs:121-149` (`validate_batch_count`); each
`decode_*_checked` decoder calls it. Used at
`src/server/dispatch.rs:2327, 2577, 2726, 2979, 3329, 3423, 3517, 3639,
3744, 3835, 3931, 4138, 4236, 4598, 4732`.
**What:** `validate_batch_count(count, max_batch, per_item_min, available)`
runs two guards:

1. `count <= max_batch_size` (configured server cap, default 8192).
2. `count.checked_mul(per_item_min) <= available_payload_bytes` —
   `checked_mul` returns `Err(TruncatedBatch)` on overflow.

Both checks fire BEFORE `Vec::with_capacity(count as usize)`. The legacy
`Option`-returning wrappers fall back to `MAX_DECODE_BATCH = 1 << 20` so
client/bench callers also get the protection.
**Why it matters:** A frame claiming `count = u32::MAX` with 14 bytes of
payload no longer triggers a 4 GiB allocation. The `*_checked` decoders
are exercised in `decode_spend_batch_checked_rejects_u32_max_count` and
13 sibling unit tests at `src/protocol/codec.rs:2740-2985`. The dispatcher
wires the configured `ServerConfig::max_batch_size` into every call.
**Reproduction:** Existing unit-test coverage is excellent.
**Suggested fix:** N/A. One nit: the in-tree integration test
`batch_exceeding_max_batch_size_rejected` (`tests/server_tcp.rs:914`) only
exercises `OP_DELETE_BATCH`. Adding a parametrized loop across all 14
batch opcodes would catch any future opcode that forgets to plumb
`max_batch_size`. Track as MEDIUM.

### GH-03: Connection state is per-connection so streams are not hijackable across connections (POSITIVE)
**Category:** H
**Location:** `src/server/mod.rs:28-53`, `src/server/dispatch.rs:4868-4938`
**What:** `ConnectionState { streams: HashMap<[u8; 32], ActiveStream> }` is
created on the connection thread (line 216) and passed by `&mut` into
`handle_request` (line 274). `handle_stream_chunk` and `handle_stream_end`
look up the stream by `(connection-local map, txid)`, so a different
connection's identical-txid frame cannot reach into another connection's
in-progress upload.
The `Drop for ConnectionState` impl at line 46 calls
`stream.writer.abort()` on every leftover entry when the connection
terminates, so abandoned streams are reaped — even when the client crashes
mid-upload.
**Why it matters:** If streams were keyed off a global table, a malicious
peer with a known txid prefix could inject `OP_STREAM_CHUNK` with a forged
offset and overwrite a legitimate uploader's in-progress blob.
**Reproduction:** Audited by inspection. **There is no integration test
verifying cross-connection isolation** — an explicit test would be high-
value. See GH-08.
**Suggested fix:** Add an integration test at `tests/server_tcp.rs` that
opens two TCP connections, starts a stream on connection A with txid `T`,
sends `OP_STREAM_CHUNK { txid: T, offset: 0, data: ... }` on connection B,
and asserts B receives `ERR_STREAM_NOT_FOUND` (because B's
`ConnectionState.streams` is empty).

### GH-04: Migration-complete `entry_count * 36` multiplication is not bounds-checked before `Vec::with_capacity` (HIGH)
**Category:** H
**Location:** `src/server/dispatch.rs:510-541`
**What:** The `OP_MIGRATION_COMPLETE` payload decoder reads
`entry_count = u32 from payload[56..60] as usize`, then computes
`needed = 60 + entry_count * 36` with **plain `*`, not `checked_mul`**.

```rust
let entry_count =
    u32::from_le_bytes(request.payload[56..60].try_into().unwrap()) as usize;
let needed = 60 + entry_count * 36;
if request.payload.len() < needed {
    return error_response(...);
}
let mut entries = Vec::with_capacity(entry_count);
```

On 64-bit hosts, `entry_count = u32::MAX` makes `entry_count * 36 ≈
1.5 × 10^11` — fits in usize (no overflow on 64-bit) but the
`request.payload.len() < needed` guard runs first. So the *length*
check protects us today, BUT only because frames are also bounded by
`MAX_FRAME_SIZE = 16 MiB`: payload.len() ≤ 16 MiB so a `needed` value of
billions cannot match. Were either layer of protection to regress (e.g.
`MAX_FRAME_SIZE` raised in a future change), this site would silently
pre-allocate up to 64 GB.
**Why it matters:** This is the only place in the dispatch layer that
hand-rolls a `count * fixed_size` multiplication instead of going through
the centralized `validate_batch_count` helper. The defense-in-depth
contract that "no decoder allocates without `checked_mul`" is broken here.
Furthermore, this opcode is intra-cluster — but `cluster_secret` is
optional in `ServerConfig`, so a bare TCP reachable on the internal
listener is the threat model.
**Reproduction:** Send an `OP_MIGRATION_COMPLETE` frame with payload of
exactly 60 bytes and `entry_count = u32::MAX`. Today: rejected by length
check. With a hypothetical `MAX_FRAME_SIZE` raised to 256 MiB and a
40-byte payload claiming `entry_count = 2^25`: the `Vec::with_capacity`
call would attempt to reserve `2^25 * (sizeof((TxKey, u32)) ≈ 36)` =
≈ 1.2 GB before the loop iteration runs out of bytes.
**Suggested fix:** Replace the unchecked multiply with
`entry_count.checked_mul(36).and_then(|n| n.checked_add(60))?` and use
`validate_batch_count`. Add a unit test sending
`entry_count = u32::MAX` with a tiny payload to assert the pre-allocation
path is never reached.

### GH-05: `OP_MIGRATION_BATCH_COMPLETE` shard-count multiplication is not bounds-checked (MEDIUM)
**Category:** H
**Location:** `src/server/dispatch.rs:751-779`
**What:** Same pattern as GH-04 but for the per-shard list:

```rust
let shard_count = u32::from_le_bytes(request.payload[..4].try_into().unwrap()) as usize;
let expected_len = 4 + shard_count * 2 + 8;
if request.payload.len() < expected_len { return error_response(...); }
let mut shards = Vec::with_capacity(shard_count);
```

`shard_count = u32::MAX` ⇒ `shard_count * 2 = ~8 GB`. Today bounded by
`MAX_FRAME_SIZE` only.
**Why it matters:** Same defense-in-depth concern as GH-04. Per-item is
2 bytes so the practical worst-case allocation under the current 16 MiB
frame ceiling is `(MAX_FRAME_SIZE - 12) / 2 ≈ 8 MiB` of shard-id u16s —
not catastrophic but uncentralized.
**Reproduction:** Same as GH-04 with the corresponding opcode.
**Suggested fix:** Use `checked_mul` and `checked_add` for `expected_len`,
or refactor to call `validate_batch_count(shard_count as u32,
MAX_SHARD_COUNT, 2, payload.len() - 12)` so the central guard sees it.

### GH-06: `decode_stream_chunk` accepts attacker-controlled `chunk_data_len` up to MAX_FRAME_SIZE (HIGH)
**Category:** H
**Location:** `src/protocol/codec.rs:1583-1599`,
`src/server/dispatch.rs:4923` (`stream.writer.write_chunk(chunk.data)`)
**What:** `decode_stream_chunk` validates only that `data_len` fits inside
the *frame* payload (which is bounded by `MAX_FRAME_SIZE = 16 MiB`). But
once decoded, the dispatcher calls
`stream.writer.write_chunk(chunk.data)` and increments
`stream.bytes_received += chunk.data.len() as u64` with **no per-stream
total cap and no minimum-chunk-size sanity floor**.

```rust
fn handle_stream_chunk(...) -> ResponseFrame {
    let chunk = match decode_stream_chunk(&req.payload) { ... };
    // ...
    if chunk.offset != stream.bytes_received { return ERR_STREAM_OFFSET_MISMATCH; }
    if let Err(e) = stream.writer.write_chunk(chunk.data) { ... }
    stream.bytes_received += chunk.data.len() as u64;  // u64 add — could wrap eventually
    // No max-size enforcement.
}
```

A client can begin a stream and pump 16 MiB chunks indefinitely, growing
the on-disk blob until the operator's filesystem fills. The
`OP_STREAM_END.total_size` is verified at finish, but only against the
running counter — there is no global ceiling on what the stream may grow
to before `OP_STREAM_END` arrives.
**Why it matters:** A single hostile connection can fill the operator's
blobstore disk by repeatedly calling `OP_STREAM_CHUNK` and never sending
`OP_STREAM_END`. The connection is single-threaded so 30s of pumping at
16 MiB/RTT will already exceed any reasonable transaction size (BSV
mainnet caps tx at ~300 MB; nothing legitimate exceeds maybe 1 GiB).
**Reproduction:** Open a TCP connection. Begin a stream with
`OP_STREAM_CHUNK { txid: X, offset: 0, data: [0u8; 16*1024*1024 - 50] }`.
Immediately follow with `OP_STREAM_CHUNK { offset: ~16 MiB, data: ... }`
ad infinitum. Server will keep accepting and writing to the on-disk
temp file under the blobstore. Disk fills.
**Suggested fix:**
1. Add `ServerConfig::max_stream_total_bytes` (default e.g. 4 GiB).
   Track `stream.bytes_received` against it; reject the chunk with
   `ERR_INTERNAL` and abort the stream when exceeded.
2. Use `checked_add` on the bytes_received counter (extra paranoia —
   `u64::MAX` is not reachable but the audit hard rule says no
   uncontrolled wire arithmetic).
3. Optionally add an idle-timeout on `ActiveStream` so a stream that
   receives no chunks for N seconds is auto-aborted by a background
   reaper.

### GH-07: `oversized_frame_rejected` test does not assert the error frame contents, only the disconnect (LOW)
**Category:** H
**Location:** `tests/server_tcp.rs:1125-1153`
**What:** The test sends a 4-byte length prefix `MAX_FRAME_SIZE + 1` and
then accepts EITHER a parseable error response OR a connection close as
"OK". The match arm `Err(_) => { /* Connection closed — also acceptable */
}` means a regression that drops the connection without sending the error
frame would still pass. The server code at `src/server/mod.rs:241-249`
*does* try to write a `frame too large` response before returning, but the
test doesn't verify it.
**Why it matters:** Operators rely on the error-frame-then-close behavior
to log the rejection on the client. A regression that elides the response
silently changes operator UX.
**Reproduction:** Already covered.
**Suggested fix:** Tighten the match arm to require a successful
`read_exact(&mut len_buf)` and decode the resulting `ResponseFrame` to
assert `payload == b"frame too large"` and `status == STATUS_ERROR`.

### GH-08: No integration test verifies cross-connection stream isolation (MEDIUM)
**Category:** H
**Location:** No test exists in `tests/`.
**What:** GH-03 confirms by inspection that streams are connection-scoped,
but no end-to-end test exists. Tests in `src/protocol/codec.rs:2509-2563`
only exercise the encode/decode functions, not the dispatch path.
**Why it matters:** A future refactor could move `ConnectionState.streams`
to a global `Mutex<HashMap<...>>` (e.g. for "background processing"
benefits) and break the isolation. Without a test, the change would land
silently.
**Reproduction:** N/A.
**Suggested fix:** Add `tests/server_tcp.rs::stream_isolation_per_
connection`:

```rust
let (server, port) = start_test_server();
let mut conn_a = TcpStream::connect(...);
let mut conn_b = TcpStream::connect(...);
// A starts a stream
send(conn_a, OP_STREAM_CHUNK, encode_stream_chunk(&txid, 0, b"hello"));
// B sends a chunk for the SAME txid — must be rejected with stream_not_found
let resp = send(conn_b, OP_STREAM_CHUNK, encode_stream_chunk(&txid, 5, b"world"));
// B has no active stream so a new one is created from offset 0.
// To make the hijack attempt impossible, validate the offset mismatch.
assert_eq!(parse_err(resp), ERR_STREAM_OFFSET_MISMATCH);
```

Note: today the implementation creates a new stream on B (because the txid
is not in B's map). To strengthen the contract, the server could refuse
to begin a new stream on B if A holds one for the same txid — but that
requires a global lookup. For the audit, document the current contract:
**streams are per-connection; a colliding txid on a second connection
just opens a brand-new local stream, harmless because each writer writes
to its own tmp file**. If `BlobStreamWriter::begin_stream` already
prevents overlapping writers for the same txid, the second
`begin_stream` would fail. Verify in `src/storage/blobstore.rs:523`.

### GH-09: Stream-chunk total-size cap is missing — see GH-06 (HIGH)
Cross-reference. The fix for GH-06 closes both DoS angles (per-chunk
unbounded data length and per-stream unbounded total length).

### GH-10: `error_response` payload uses `(msg.len() as u16)` cast which truncates >65535 (LOW)
**Category:** H
**Location:** `src/server/dispatch.rs:4997-5007`,
`src/server/dispatch.rs:5000`
**What:** All error response messages are built via:

```rust
payload.extend_from_slice(&(msg.len() as u16).to_le_bytes());
```

If a future error message ever exceeds 65 KiB, the length prefix
truncates and the trailing message becomes wire garbage. Today messages
are all short (≤ a few hundred bytes) so this is a code-hygiene issue,
not exploitable.
**Why it matters:** Defense in depth. A future contributor adding
`format!("...{:?}", huge_struct)` to an error path could trigger
silent wire corruption.
**Reproduction:** Synthetic: instrument
`error_response(0, ERR_INTERNAL, &"X".repeat(70000))` and observe wire
bytes show `len=4464` (`70000 % 65536`).
**Suggested fix:** Either truncate the message to 65535 bytes before the
cast, or use a u32 length prefix. The wire format
([`encode_error_payload`] in `src/protocol/codec.rs:1297`) already
uses `u16` for the length, so this is a wire-format constraint —
truncation is the pragmatic fix.

### GH-11: `decode_redirect` and `decode_error_payload` use `u16` length prefix (POSITIVE — but see GH-10)
**Category:** H
**Location:** `src/protocol/codec.rs:1297-1343`
**What:** `encode_error_payload` and `encode_redirect` cast `len() as u16`
as the length prefix. Decoders (`decode_error_payload:1306`,
`decode_redirect:1334`) verify `data.len() < 4 + msg_len` before reading,
so a malformed inbound length is rejected cleanly without panic.
**Why it matters:** Symmetric to GH-10 — incoming length is verified, but
outgoing length is silently truncated. See GH-10 for the outgoing fix.
**Reproduction:** N/A.
**Suggested fix:** See GH-10.

### GH-12: `decode_stream_chunk` returns `Option<&[u8]>` borrow tied to caller payload (POSITIVE)
**Category:** H
**Location:** `src/protocol/codec.rs:1583-1599`
**What:** Returns `StreamChunk<'a> { data: &'a [u8] }` without copying.
Bounds are correctly checked (`if payload.len() < 44 + data_len { return
None }`).
**Why it matters:** Avoids unnecessary allocation per chunk; `data_len` is
properly bounded against the parent frame.
**Suggested fix:** N/A.

### GH-13: `cold_data` length parsed from u32 with no per-item cap (MEDIUM)
**Category:** H
**Location:** `src/protocol/codec.rs:813-824`
**What:** Inside `decode_create_batch_checked`, each item reads:

```rust
let cold_len = get_u32(data, pos) as usize;
pos += 4;
if pos + cold_len > data.len() { return SectionTruncated; }
let cold_data = data[pos..pos + cold_len].to_vec();
```

So `cold_len` is bounded only by the residual payload (≤ 16 MiB). A
single create-batch item can therefore claim 16 MiB - 100 of cold data,
forcing a 16 MiB `to_vec()` per item. Combined with a `count` of (16 MiB
/ ~96) ≈ 170k items at minimum size, the *aggregate* cold_data a frame
can carry is bounded — but each individual item allocation is bounded
only by the frame, not by an explicit cap.
**Why it matters:** Real BSV transactions have cold data measured in tens
of KB at most. A 16 MiB cold-data section in a single item is well outside
spec. Adding an explicit `MAX_COLD_DATA_PER_ITEM` (say, 4 MiB) would
catch malformed-but-plausible frames earlier and be a useful operator
sanity check.
**Reproduction:** Construct a frame with `count=1`, an item with
`cold_len = 16 * 1024 * 1024 - 200`, and observe the 16 MiB allocation.
Server processes it as a normal large create.
**Suggested fix:** Add `MAX_COLD_DATA_BYTES` (e.g. 4 MiB) constant in
`src/protocol/opcodes.rs` and reject `cold_len > MAX_COLD_DATA_BYTES`
inside `decode_create_batch_checked`. Same for `utxo_count * 32` and
`parent_count * 32` — each item's variable-length sections deserve a
per-item cap, not just the aggregate-frame bound.

### GH-14: Unbounded `utxo_count`/`parent_count` per item (MEDIUM)
**Category:** H
**Location:** `src/protocol/codec.rs:781-905`
**What:** `decode_create_batch_checked` inside the per-item loop reads a
u32 count for utxo_hashes and parent_txids. The `checked_mul` against 32
catches overflow, and the `pos + utxo_bytes > data.len()` check rejects
the truncated case — so the absolute upper bound is again the frame size
(16 MiB / 32 = 512k entries per section). Per-item cap is missing.
**Why it matters:** A single create item carrying 500k utxo_hashes is
nonsense for BSV (largest transactions have a few thousand outputs
typically) but is still accepted. Allocating a 16 MiB `Vec<[u8; 32]>` per
item could slow down legitimate batches if mixed with a malicious item.
**Reproduction:** As GH-13, but for utxo_hashes / parent_txids.
**Suggested fix:** Add `MAX_UTXO_HASHES_PER_ITEM` (e.g. 65536) and
`MAX_PARENTS_PER_ITEM` (e.g. 4096) constants. Reject item early.

### GH-15: `parse_cold_data_fields` uses `u32 as usize` plus naive `pos + il` (LOW)
**Category:** H
**Location:** `src/server/dispatch.rs:2915-2969`
**What:** Reads three sequential `u32` length-prefixed sections from
`cold_data`. Uses `pos + il > cold_data.len()` (regular `+`, not
`checked_add`). On 64-bit, `pos + u32::MAX` cannot overflow `usize`, so
the bound check is correct in practice. Still: the audit's stated rule
("any narrowing `as u32`, multiplications without `checked_*`?") flags
this as code hygiene worth fixing.
**Why it matters:** Defense in depth. A 32-bit target build (unlikely for
this server but allowed by Rust) could overflow.
**Reproduction:** Synthetic on a 32-bit target.
**Suggested fix:** Use `pos.checked_add(il).is_some_and(|end| end <=
cold_data.len())`. Same pattern in lines 2935, 2952.

### GH-16: `max_connections` enforcement is correct but not integration-tested (MEDIUM)
**Category:** H
**Location:** `src/server/mod.rs:120-152`
**What:** Accept loop checks
`active_connections.load(Relaxed) >= max_connections` BEFORE
`fetch_add(1)`. Rejected connections are silently dropped (no error
response). On disconnect, `fetch_sub(1)` runs in the per-connection
thread's drop path. There is no integration test that verifies connection
N+1 is actually rejected.
**Why it matters:** A regression (e.g. someone moving the `fetch_add`
above the check) would silently uncap concurrent connections, allowing
DoS by FD exhaustion. The default cap of 1024 is reasonable but only
enforced in the accept loop; an attacker who managed to bypass would have
no other guard.
**Reproduction:** Open `max_connections + 1` connections, observe none of
them are refused — would prove a regression.
**Suggested fix:** Add `tests/server_tcp.rs::max_connections_enforced`:

```rust
let cfg = ServerConfig { max_connections: 5, ... };
// Open 5 connections successfully.
let conns: Vec<_> = (0..5).map(|_| TcpStream::connect(...).unwrap()).collect();
// Wait for the server thread to register them.
poll_until(|| server.active_connections() == 5);
// 6th connection: server should accept the TCP handshake (kernel does that)
// but immediately drop it without responding.
let mut sixth = TcpStream::connect(...).unwrap();
sixth.set_read_timeout(Some(Duration::from_millis(500)));
let mut buf = [0u8; 1];
assert!(sixth.read(&mut buf).is_err() || buf == [0u8; 1]);  // closed or no data
```

Also consider sending a `STATUS_ERROR` response with a
`connection limit reached` message before dropping, to give clients
better diagnostics. (Today they just see RST/FIN.)

### GH-17: `read_buf` resize is monotonic — never shrinks, even after a single oversized frame (LOW)
**Category:** H
**Location:** `src/server/mod.rs:215-258`
**What:** `let mut read_buf = vec![0u8; 256 * 1024];` initial. After a
legitimate 16 MiB frame, `read_buf` stays at 16 MiB for the rest of the
connection lifetime. With `max_connections = 1024`, a coordinated set of
clients each sending one large frame would inflate per-connection memory
to 1024 × 16 MiB = 16 GiB until the connections close.
**Why it matters:** This is the OOM amplification path the gap-doc
discusses, but mitigated by the 16 MiB ceiling. It's still a
*per-connection* steady-state cost: a single batch of 1024 connections
pumping 16 MiB then idling holds 16 GiB.
**Reproduction:** Open `max_connections` connections, each sending one
frame at the cap, then keeping the connection alive (without sending
more frames). Heap stays at 16 GiB.
**Suggested fix:** Two options:
1. Periodically `read_buf.shrink_to(256 * 1024)` after handling a frame
   smaller than e.g. 1 MiB.
2. Use `BytesMut` or a pooled allocator. The first option is the
   minimum-change fix.

### GH-18: All opcodes 1-12, 20-21, 30-32, 100-102, and stream 200-201 have decoder + cleanup paths (POSITIVE)
**Category:** H
**Location:** `src/server/dispatch.rs:353-417` (dispatch arms)
**What:** Audit confirms every opcode listed in the audit task has a
decoder path — either via `decode_*_checked` (batch ops) or via specific
inline decode-and-validate (admin/streaming/cluster opcodes).

| Opcode             | Decoder location                     | Bounds-checked? |
|---|---|---|
| OP_SPEND_BATCH (1) | `decode_spend_batch_checked`         | Yes |
| OP_UNSPEND_BATCH (2) | `decode_unspend_batch_checked`     | Yes |
| OP_SET_MINED_BATCH (3) | `decode_set_mined_batch_checked` | Yes |
| OP_CREATE_BATCH (4) | `decode_create_batch_checked`       | Yes (per-item caps missing — see GH-13/14) |
| OP_FREEZE_BATCH (5) | `decode_slot_item_batch_checked`    | Yes |
| OP_UNFREEZE_BATCH (6) | `decode_slot_item_batch_checked`  | Yes |
| OP_REASSIGN_BATCH (7) | `decode_reassign_batch_checked`   | Yes |
| OP_SET_CONFLICTING_BATCH (8) | `decode_txid_batch_checked` shared_len=9 | Yes |
| OP_SET_LOCKED_BATCH (9) | `decode_txid_batch_checked` shared_len=1 | Yes |
| OP_PRESERVE_UNTIL_BATCH (10) | `decode_txid_batch_checked` shared_len=4 | Yes |
| OP_DELETE_BATCH (11) | `decode_txid_batch_checked` shared_len=0 | Yes |
| OP_MARK_LONGEST_CHAIN_BATCH (12) | `decode_txid_batch_checked` shared_len=9 | Yes |
| OP_GET_BATCH (20) | `decode_get_batch_checked`             | Yes |
| OP_GET_SPEND_BATCH (21) | `decode_get_spend_batch_checked` | Yes |
| OP_QUERY_OLD_UNMINED (30) | inline u32 read at l. 4574 | Yes (4-byte minimum) |
| OP_PRESERVE_TRANSACTIONS (31) | `decode_txid_batch_checked` shared_len=4 | Yes |
| OP_PROCESS_EXPIRED_PRESERVATIONS (32) | inline u32 at 4678 | Yes |
| OP_GET_PARTITION_MAP (100) | empty payload | N/A |
| OP_HEALTH (101) | empty payload | N/A |
| OP_PING (102) | empty payload | N/A |
| OP_STREAM_CHUNK (200) | `decode_stream_chunk` | Yes (but no per-stream total cap — GH-06) |
| OP_STREAM_END (201) | `decode_stream_end` | Yes (40-byte minimum) |

Inter-node opcodes (240-243, 250-253) are not in scope for this audit but
share the same decoder discipline.
**Why it matters:** Nothing — every required opcode is covered.

### GH-19: Connection-close cleanup releases held locks via thread unwind (POSITIVE)
**Category:** H
**Location:** `src/server/mod.rs:140-152` (per-connection thread)
**What:** Each connection gets its own thread. The thread holds locks via
the `parking_lot::Mutex` guards inside `handle_request`, but those
guards are dropped at the end of each `handle_request` call (per-frame
scoped). When the connection thread exits (from EOF, error, or shutdown),
no locks are still held. The `ConnectionState::Drop` impl reaps any
in-flight blob streams. The `active_connections.fetch_sub(1, Relaxed)`
runs unconditionally on thread exit (line 152).
**Why it matters:** A panic inside `handle_request` would still trigger
unwinding and the `Drop` impl would still run. Locks acquired inside the
handler are released on guard-drop. No long-running resource is leaked.
**Reproduction:** Synthetic: kill a client mid-request; observe stream
files (under blobstore tmp dir) are removed via `writer.abort()` in
`Drop`. Tested by the lifecycle nature of the implementation; no explicit
integration test exists.
**Suggested fix:** N/A. Optionally add a fault-injection test that
panics inside a dispatcher handler and asserts the connection thread
unwinds cleanly.

---

## Category G — Index backends

### GH-G1: Snapshot deserialize uses unchecked `count * PRIMARY_ENTRY_SIZE` multiplication (HIGH)
**Category:** G
**Location:** `src/index/mod.rs:563-575`,
`src/index/mod.rs:687-715` (secondary)
**What:**

```rust
let count = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
let capacity = u64::from_le_bytes(data[16..24].try_into().unwrap()) as usize;

let body_size = count * PRIMARY_ENTRY_SIZE;   // <-- u64-sized count, naked mul
let total = header_size + body_size + 4;
if data.len() < total { ... }
```

A snapshot file with a forged `count = u64::MAX` makes `count *
PRIMARY_ENTRY_SIZE = ~10^21` which **silently wraps modulo
`usize::MAX`** on 64-bit targets, producing a small `total` that passes
the `data.len() < total` check. The subsequent `Self::new(capacity.max
(count))` then calls `next_power_of_two()` on a u64-derived value —
which **panics** for very large inputs (hashtable.rs:508). After CRC
fails, it returns ChecksumMismatch — IF the data length still allows the
checksum read at `data[total - 4..total]`, which uses the wrapped
`total`.

Same pattern in `deserialize_secondary` (line 688: `let body_size = count
* SECONDARY_ENTRY_SIZE`).

The `locate_unmined_section` at line 654 *does* use
`count.saturating_mul(SECONDARY_ENTRY_SIZE)` — that path is safe.
**Why it matters:** Snapshot files are local on-disk state today; the
attacker has to control the snapshot directory to exploit. Threat models
that include:
- A multi-tenant operator where one user's container can write to
  another's snapshot directory.
- A backup-restore pipeline where snapshot files are pulled from a remote
  source.
- A misconfigured shared volume.

…are all in scope. The HIGH severity reflects the supply-chain risk and
the explicit audit scope item ("Snapshot restore handles truncated/
corrupt snapshot by falling back to device scan").

Note: the audit point "falls back to device scan on corrupt" is **NOT**
implemented today. `Index::restore` returns `IndexError` on any failure;
the caller in `src/server/startup.rs` does not transparently fall back —
it logs and calls `Index::rebuild` only on the rebuild path (not the
restore path). For the in-memory backend, a corrupt snapshot causes
restart failure today.
**Reproduction:**

```rust
let mut data = b"TSIX".to_vec();
data.extend(&1u32.to_le_bytes()); // version
data.extend(&u64::MAX.to_le_bytes()); // count -- POISON
data.extend(&16u64.to_le_bytes()); // capacity
data.extend(&[0u8; 32 + 4]); // body + checksum (garbage)
let result = Index::restore_from_bytes(&data);  // private; via tempfile
```

Either OOM, panic on `next_power_of_two`, or a wrap-driven undersized
buffer access depending on actual byte values.
**Suggested fix:**
1. Use `count.checked_mul(PRIMARY_ENTRY_SIZE).ok_or(IndexError::
   FormatError {...})?`.
2. Cap `count` at a hard ceiling (say, `1 << 30` = 1 G entries) before
   any `Self::new(count)` call.
3. Document explicitly in the function header that snapshot files are
   trusted local state, but write defensive code anyway.
4. Add a test: write a poisoned snapshot file and assert
   `Index::restore` returns `FormatError`, not panic / OOM.

### GH-G2: `Index::restore_all` does NOT fall back to device scan on corrupt snapshot (MEDIUM — design gap, not bug)
**Category:** G
**Location:** `src/index/mod.rs:303-354`,
`src/server/startup.rs:230-280`
**What:** The audit task says: "Snapshot restore handles truncated/
corrupt snapshot by falling back to device scan." The actual behavior is
**stricter**: `restore_all` returns `RestoreFlags { dah_needs_rebuild,
unmined_needs_rebuild }` per-section so a corrupt DAH section flags only
DAH for rebuild. But if the **primary section** itself is corrupt
(checksum mismatch on the primary block), `restore` returns an error and
the caller (`startup.rs::load_primary_index_in_memory`) fails closed and
falls back to **device rebuild** — not in-memory empty.

So the fall-back IS implemented at the orchestration layer
(`load_primary_index_in_memory` calls `rebuild` if `restore` fails), just
not inside `restore` itself. This matches the audit's intent.

Tests covering this: `restore_all_dah_corrupt_but_unmined_intact` (line
898-954) and `restore_all_unmined_corrupt_but_dah_intact` (957-997).
Both pass.

**There is no test that exercises a primary-section corruption AND
verifies the orchestration layer falls back to `rebuild`.** The redb
counterpart `redb_primary_rebuild_failure_preserves_file` does test the
fail-closed contract for redb but not for the in-memory backend.
**Why it matters:** Operators rely on the documented "snapshot corruption
is recoverable" contract. Without an explicit integration test, a
regression in `startup.rs::load_primary_index_in_memory` could silently
remove the fallback.
**Suggested fix:** Add `tests/recovery_*.rs::corrupt_in_memory_snapshot_
falls_back_to_device_rebuild`:

```rust
let mut server = boot_with_records(N);
server.shutdown();
// Corrupt the snapshot.
corrupt_byte(server.config.index_snapshot_path);
// Restart — should rebuild from device.
let server = boot();
assert_eq!(server.index_count(), N);  // all records recovered via rebuild
```

### GH-G3: `import_index` is not transactional across primary/dah/unmined redb files (HIGH)
**Category:** G
**Location:** `src/index/migration.rs:79-128`
**What:** `import_index` opens three separate redb databases and writes
to them sequentially:

```rust
let mut redb_primary = RedbPrimary::open(&config.redb_path, ...)?;
redb_primary.register_batch(&primary_entries)?;          // <- commits

let mut redb_dah = RedbDahIndex::open(&config.redb_dah_path, ...)?;
redb_dah.insert_batch(&dah_entries);                     // <- commits

let mut redb_unmined = RedbUnminedIndex::open(...)?;
redb_unmined.insert_batch(&unmined_entries);             // <- commits
```

Each `register_batch` / `insert_batch` commits its own redb transaction.
A crash (or kill) between the primary commit and the dah/unmined commit
leaves the primary populated and dah/unmined empty. On restart, the
secondary readiness gate (`ERR_INDEX_DEGRADED`) would fire and the
operator would manually rebuild — but only AFTER receiving traffic.

The migration tool has no two-phase commit and no marker file indicating
"import in progress, do not start serving."
**Why it matters:** The audit explicitly asks: "redb: every transaction
commits or aborts cleanly. No path that writes one of the three redb
files but not the others when they should be in sync." The migration
import path violates this.
**Reproduction:** SIGKILL the import process between the primary and
dah commits. Restart server. Observe primary is populated, dah is empty,
secondary readiness gate trips on first DAH-dependent op.
**Suggested fix:** Either:
1. Add a sentinel file `.import-in-progress` written before any commit
   and removed only after all three commits succeed. On startup, refuse
   to come online if the sentinel exists.
2. Rewrite as a single redb database with three tables (the dah and
   unmined indexes already share `redb_dah_path` ↔ `redb_unmined_path`
   only by convention; consolidating would let one redb transaction span
   all three).

### GH-G4: redb runtime mutations (`update_cached_fields`, `unregister`) do NOT cross-sync with secondary indexes inside one redb transaction (MEDIUM)
**Category:** G
**Location:** `src/index/redb_primary.rs:280-318` (primary update),
`src/index/redb_dah.rs:93-153` (dah insert),
`src/index/secondary_backend.rs` (orchestration)
**What:** The primary and secondary redb backends each manage their own
redb database files. A primary-cached-field update commits to
`primary.redb` independently of the corresponding secondary update to
`dah.redb`. Two-phase durability via the redo log is the documented
defense (see redb_dah.rs:104 — "redo intent before commit"), but the
two redb commits themselves are not co-transactional.

A power loss between the redo-log fsync and one of the redb commits is
recovered via redo replay at startup (recovery::recover_all). The audit
question "every transaction commits or aborts cleanly" is satisfied
**within each redb backend independently**; cross-backend atomicity is
delegated to the redo log.
**Why it matters:** This is by design and tested via the
`secondary_two_phase_durability` integration test
(`tests/secondary_two_phase_durability.rs` exists). The pattern is
correct, but worth flagging that "primary committed but secondary did
not" is a transient state visible to *concurrent reads* until startup-
time recovery runs. For this server's contract, that's only relevant if
a crash happens — which is the redo-log replay's job.
**Suggested fix:** N/A. The design is sound; the audit ask is satisfied
modulo the import_index gap (GH-G3).

### GH-G5: redb corruption fallback (delete, recreate, fall back to memory) is NOT IMPLEMENTED — fail-closed instead (POSITIVE/MEDIUM)
**Category:** G
**Location:** `src/server/startup.rs:220-244`,
`src/server/startup.rs:567-609` (test)
**What:** The audit expects "redb corruption fallback (delete, recreate,
fall back to memory) is **actually tested** with a deliberately corrupt
file." The TeraSlab implementation **explicitly chose fail-closed** over
silent fall-back:

```rust
// startup.rs:220
/// Load the redb primary index. Restore first, fall back to a
/// device-rebuild on miss, fail closed otherwise.
pub fn load_primary_index_redb(
    config: &IndexConfig,
    device: &dyn BlockDevice,
    allocator: &SlotAllocator,
) -> Result<PrimaryBackend, RebuildError> {
    let restore_err = match PrimaryBackend::restore_redb(config) {
        Ok(b) => return Ok(b),
        Err(e) => e,
    };
    match PrimaryBackend::rebuild_redb(config, device, allocator) {
        Ok(b) => Ok(b),
        Err(rebuild_err) => Err(RebuildError::RedbPrimary { ... }),
    }
}
```

The test at line 567 (`redb_primary_rebuild_failure_preserves_file`)
verifies that a corrupted redb primary file is **preserved untouched** —
the loader does NOT delete and recreate. The intent is operator-visible
failure (via the `RebuildError` propagating to `main`) rather than a
silent in-memory fall-back.

The audit's expectation appears outdated: the codebase removed the
in-memory fallback intentionally (see file-level comment lines 4-9: "[…
rebuild were previously fail-open: a corrupt redb primary file was
silently replaced by an empty in-memory index].").
**Why it matters:** This is a deliberate safety property. Silently
falling back to memory could hide a real disk corruption issue and lead
to data loss the operator never noticed. The current behavior is the
right call.
**Reproduction:** `cargo test redb_primary_rebuild_failure_preserves_
file` exercises the path and asserts the byte-for-byte preservation.
**Suggested fix:** Update the audit document to reflect the new contract.
The audit should expect "corrupted redb file → fail-closed startup with
clear operator message and file preserved for forensics" rather than
the older "delete, recreate, fall back to memory."

### GH-G6: Secondary index (DAH/unmined) failure during rebuild → degraded readiness gate (POSITIVE)
**Category:** G
**Location:** `src/server/startup.rs:336-380`,
`src/server/dispatch.rs:2161-2218`
**What:** When a secondary index fails to rebuild at startup,
`fallback_dah_index` / `fallback_unmined_index` returns an empty
`InMemory` variant AND the secondary status is set to `Degraded`. The
dispatcher's `check_secondary_readiness(op_code, request_id)` then
returns `STATUS_ERROR / ERR_INDEX_DEGRADED` for any opcode that depends
on the degraded secondary (`OP_QUERY_OLD_UNMINED`,
`OP_PROCESS_EXPIRED_PRESERVATIONS`, etc.). The primary path stays alive.
**Why it matters:** Implements the gap-doc's H6 contract — partial
failure is observable and recoverable per-section without breaking the
whole server.
**Suggested fix:** N/A. Tests at `dispatch.rs:9700` cover the degraded
gate.

### GH-G7: `expected_records` hint affects sizing but tolerates exceeding (POSITIVE)
**Category:** G
**Location:** `src/index/mod.rs:118-145`,
`src/index/hashtable.rs:179-185` (`Index::register` auto-resize)
**What:** The hash table auto-resizes when load factor exceeds 0.7
(default `resize_threshold`). Initial capacity is `expected_records /
0.7` rounded to next power of two, but exceeding is handled by
`HashTable::resize(capacity * 2)`. For file-backed, the resize is
crash-atomic via redo log (`HashtableResizeBegin / Commit`).
**Why it matters:** A wrong `expected_records` hint costs memory at
startup but doesn't break correctness. The
`concurrent_register_produces_one_resize_per_threshold_crossing` test at
`src/index/mod.rs:1359` confirms growth.
**Suggested fix:** N/A.

### GH-G8: mmap region is resizable (file-backed) via tempfile + rename + redo log (POSITIVE)
**Category:** G
**Location:** `src/index/hashtable.rs:929-1069`
**What:** File-backed hash table resize is crash-atomic:
1. Append+fsync `RedoOp::HashtableResizeBegin { tmp_path, new_capacity }`.
2. Create tmp file at new size, rehash entries, msync+fsync.
3. Rename tmp over original (POSIX atomic).
4. fsync parent directory.
5. Append+fsync `RedoOp::HashtableResizeCommit`.

A crash between any two steps leaves recoverable state: pre-step-3 = old
file intact, post-step-3 = new file intact. The orphan tmp file is
cleaned up at recovery time via the redo log scan.

There's a fault-injection sync point at line 1041 (`MidHashtableResize`)
that simulates a crash AFTER the rename but BEFORE the parent-dir fsync.
**Why it matters:** Index growth is a critical operation; getting it
wrong loses the entire index. The implementation is correct and
testable.
**Suggested fix:** N/A.

### GH-G9: Snapshot atomicity uses tempfile + rename (POSITIVE)
**Category:** G
**Location:** `src/index/mod.rs:254-293`
**What:** Both `snapshot` (primary only) and `snapshot_all` (primary +
secondaries) use:

```rust
std::fs::write(&tmp_path, &data)?;
let f = std::fs::File::open(&tmp_path)?;
f.sync_all()?;
drop(f);
std::fs::rename(&tmp_path, path)?;
```

Subtle issue: the `tmp_path` is computed via
`path.with_extension("tmp")` which **collapses** if `path` doesn't have
an extension. E.g. `path = "snap"` → `tmp_path = "snap.tmp"` (OK). But
`path = "snap.dir/idx"` → `tmp_path = "snap.dir/idx.tmp"` (OK). The
common case is fine.

**Missing:** No fsync of the parent directory after `rename`, unlike the
hashtable resize path. This means a crash AFTER the rename but BEFORE
the parent dir is flushed could lose the rename on some filesystems
(ext4 with default `data=ordered` is fine; ext4 with `data=writeback` or
xfs with no `wsync` could lose it).
**Why it matters:** Snapshot durability after the `rename` returns
success is filesystem-dependent. The hashtable resize path got this
right (line 1044: `fsync_parent_dir(&old_path)?`); the snapshot path
should match.
**Reproduction:** On a filesystem that doesn't fsync the rename
implicitly, kill the process between `rename` and the parent dir fsync.
The snapshot directory entry may revert to the old name pointing at the
old inode (or no inode at all).
**Suggested fix:** After `rename`, call `fsync_parent_dir(path)?` (the
helper at `hashtable.rs:341` is private to that module — refactor it to
a shared `crate::index::util::fsync_parent_dir` and use it here too).

### GH-G10: Robin Hood probe distance is bounded; high-load-factor tested (POSITIVE)
**Category:** G
**Location:** `src/index/hashtable.rs:101-114, 1340-1399`
**What:** `BUCKET_EMPTY_SENTINEL = 0xFF`; `MAX_STORED_PROBE = 254`. Any
probe distance greater than 254 stores 254 and falls back to scan-mode
for that single chain (no early termination). Tests at:
- `fill_70_percent` (line 1330) — 716/1024 ≈ 70%
- `fill_90_percent` (line 1341) — 921/1024 ≈ 90%
- `max_probe_distance_reasonable` (line 1387) — at 88% load, probe
  distance < 100
- `adversarial_1000_all_same_bucket` (line 1369) — 1000 keys forced into
  one bucket via `make_colliding_key`
**Why it matters:** Robin Hood with a probe-distance ceiling AND
fingerprint-based fast rejection is the right algorithm. The
ceiling-handling for >254 is the trickiest part; the test
`adversarial_1000_all_same_bucket` exercises it.
**Suggested fix:** N/A.

### GH-G11: Snapshot format is versioned (POSITIVE)
**Category:** G
**Location:** `src/index/mod.rs:64-69`
**What:** `SNAPSHOT_MAGIC = b"TSIX"`, `SNAPSHOT_VERSION = 1`,
`DAH_SECTION_MAGIC = b"DAHI"`, `UNMINED_SECTION_MAGIC = b"UNMI"`,
`SECONDARY_VERSION = 1`. Deserializer reads the version but does NOT
reject unknown versions:

```rust
let _version = u32::from_le_bytes(data[4..8].try_into().unwrap());  // read but unused
```

A future format change must bump the version AND add a check, or risk
silently loading an incompatible newer-format snapshot.
**Why it matters:** Today only one version exists, so the check is
moot. But a forward-compatibility regression would land silently.
**Suggested fix:** Replace `_version` with:

```rust
let version = u32::from_le_bytes(...);
if version != SNAPSHOT_VERSION {
    return Err(IndexError::FormatError { detail: format!("unsupported snapshot version: {version} (expected {SNAPSHOT_VERSION})") });
}
```

Same for `deserialize_secondary` at line 686.

### GH-G12: redb backends use `Durability::Eventual` — relies on redo log for crash safety (POSITIVE)
**Category:** G
**Location:** `src/index/redb_primary.rs:55-65`,
`src/index/redb_dah.rs:33-42`, `src/index/redb_unmined.rs`
**What:** Every redb write transaction sets `Durability::Eventual`.
Documented rationale: "TeraSlab's redo log (WAL) provides crash
recovery, so the redb index does not need per-operation fsync."

This is the correct design choice (a 10-100x throughput multiplier on
small writes) but it depends on the redo log replay path being correct
at startup. The two-phase durability tests in
`tests/secondary_two_phase_durability.rs` and the fault-injection
sync points (`SyncPoint::BeforeSecondaryRedbCommit` etc.) cover the
crash boundary.
**Why it matters:** A bug in the redo replay would result in lost redb
mutations across a crash. This is the design's most subtle property.
**Suggested fix:** N/A. Continue investing in fault-injection coverage.

### GH-G13: `update_cached_fields` requires external locking (POSITIVE — documented)
**Category:** G
**Location:** `src/index/redb_primary.rs:273-318`
**What:** Doc comment at line 273 explicitly says: "The caller MUST hold
an exclusive lock (e.g. RwLock::write()) around the PrimaryBackend
before calling this method." The redb read-modify-write inside
`update_cached_fields` is NOT atomic against concurrent writers — the
read happens, then the modified value is written, and a concurrent
`update` on the same key could race.

Search confirms callers do hold `engine.locks` stripes around mutations
(lock module is out of scope for this audit).
**Why it matters:** Documenting concurrency requirements explicitly is
the right move; would be a footgun otherwise.
**Suggested fix:** N/A. Could add a `debug_assert!(/*lock held*/)`
helper using a `&LockedBackend` wrapper type if extra paranoia is
desired.

### GH-G14: Iterating redb materializes ALL entries into a `Vec` (MEDIUM — documented)
**Category:** G
**Location:** `src/index/redb_primary.rs:330-358`,
`src/index/secondary_backend.rs` (DahIter::Collected)
**What:** `RedbPrimary::iter_collected` collects all entries into a
`Vec`, with a `tracing::warn!` at >1M entries. At 10M entries this is
~630 MiB.
**Why it matters:** Documented but worth flagging: any code path that
calls `iter()` on the redb backend allocates O(N) memory. The doc
comment is at line 332 and the call sites should be audited for memory
amplification. (The cluster baseline-streaming path uses
`engine.keys_for_shard(shard)` which appears to be a different
iterator — out of scope here.)
**Suggested fix:** Provide a streaming iterator API (`iter_streaming`)
that holds the redb read transaction for the iterator's lifetime and
yields entries one at a time. Then deprecate `iter_collected` for
small-table use only.

### GH-G15: `import_index` collects all entries into a single in-memory `Index` first (MEDIUM)
**Category:** G
**Location:** `src/index/migration.rs:43-74`
**What:** `export_index` builds `mem_primary = Index::new(primary.len()
.max(16))?` and copies every entry from the (potentially redb-backed)
source. For a 10M-entry redb index this is ~630 MiB resident plus the
serialized snapshot bytes (~630 MiB more).
**Why it matters:** The migration is intended to be a one-shot tool, but
running on a 100M-entry production index would need 13 GiB free RAM.
**Suggested fix:** Add a streaming export that writes the snapshot file
chunk-by-chunk without materializing the full set. The on-disk format
is already fixed-size-per-entry, so streaming is straightforward.

### GH-G16: Snapshot deserialization does NOT cap `count` against a sanity ceiling (MEDIUM)
**Category:** G
**Location:** `src/index/mod.rs:548-610`
**What:** As covered in GH-G1, the snapshot's `count` field is read
straight from the file with no upper bound. A forged 10^15 count would
cause the `Self::new(count.max(capacity))` path to attempt an enormous
hashtable allocation, hitting OOM or `next_power_of_two` panic.
**Why it matters:** Snapshot files are local trusted state; the
practical risk is low. But the audit asks for hard ceilings here.
**Suggested fix:** Define `MAX_SNAPSHOT_COUNT` (e.g. 10^9 entries) and
reject snapshots that exceed it. Apply the same to
`deserialize_secondary`.

### GH-G17: Secondary indexes DO stay consistent with primary across crash/restart via redo log (POSITIVE)
**Category:** G
**Location:** `src/recovery.rs` (out of audit scope but referenced),
`tests/secondary_two_phase_durability.rs` (file referenced)
**What:** Audit asks: "Secondary indexes (DAH, unmined) stay consistent
with primary across crash, restart, and migration. Find the test that
mutates primary, kills, restarts, verifies secondaries."

The tests `tests/secondary_two_phase_durability.rs` and the
fault-injection paths (`SyncPoint::BeforeSecondaryRedbCommit`,
`AfterSecondaryRedbCommit` in the redb_dah/redb_unmined files) cover
this. The two-phase durability contract:
1. Primary mutation appends a redo entry.
2. Secondary mutation appends ITS OWN redo entry BEFORE its redb commit.
3. On restart, redo replay re-applies any missed secondary update.

The integration test exists; full audit of it is outside Category G.
**Why it matters:** Crash-safety of compound mutations is the gating
property for production. The tests are present.
**Suggested fix:** N/A. Continue investing in the existing test pattern.

---

## Summary

**CRITICAL (none):** No category-defining critical findings. The 16 MiB
frame ceiling and `*_checked` decoders close the worst DoS surface.

**HIGH (3):**
- GH-04: `OP_MIGRATION_COMPLETE` `entry_count * 36` is unchecked.
- GH-06/GH-09: `OP_STREAM_CHUNK` has no per-stream total cap; an
  attacker can fill the operator's blobstore disk with a single
  unbounded stream.
- GH-G1: Snapshot deserializer uses unchecked `count *
  PRIMARY_ENTRY_SIZE` multiplication; OOM/panic on poisoned snapshot.
- GH-G3: `import_index` is not transactional across the three redb
  files — a crash mid-import leaves dah/unmined empty while primary is
  populated.

**MEDIUM (8):**
- GH-05: `OP_MIGRATION_BATCH_COMPLETE` shard-count multiplication.
- GH-08: No integration test for cross-connection stream isolation.
- GH-13/14: No per-item cap on `cold_data` / `utxo_count` /
  `parent_count` inside CreateBatch.
- GH-16: No integration test for `max_connections` enforcement.
- GH-G2: No test for in-memory snapshot corruption → device rebuild
  fallback.
- GH-G14/15: redb `iter_collected` and migration export materialize
  full set in RAM.
- GH-G16: Snapshot `count` lacks a sanity ceiling.

**LOW (4):**
- GH-07: `oversized_frame_rejected` test accepts both error frame and
  bare disconnect.
- GH-10: `error_response` truncates messages >65 KiB silently.
- GH-15: `parse_cold_data_fields` uses unchecked `pos + il` (32-bit
  target only).
- GH-17: `read_buf` never shrinks after a large frame.

**POSITIVE / no-op (10):**
- GH-01: `MAX_FRAME_SIZE` enforcement is correct and tested.
- GH-02: Centralized batch validator (`validate_batch_count`) is
  comprehensive.
- GH-03: Streams are per-connection (no hijacking across connections).
- GH-11: Inbound length-prefix parsing is bounds-safe.
- GH-12: Stream-chunk decoder uses borrow, not copy.
- GH-18: All 25 audited opcodes have bounds-checked decoders.
- GH-19: Connection-close releases locks via thread unwind + `Drop`.
- GH-G5: redb fail-closed policy (audit's expected delete/recreate is
  no longer implemented — by deliberate choice).
- GH-G6: Degraded-readiness gate covers secondary failures cleanly.
- GH-G7/8/10/11/17: Existing index machinery is correct.

## Unverified

- The `secondary_two_phase_durability.rs` integration test was not read
  in full; the description in GH-G17 is based on the file name and the
  presence of `SyncPoint::Before/AfterSecondaryRedbCommit` fault
  injection points.
- The redb `iter` path was confirmed materializing for the primary
  backend; the equivalent for `RedbDahIndex::iter` and
  `RedbUnminedIndex::iter` was not read but the
  `secondary_backend.rs::DahIter::Collected` pattern strongly implies
  they share the same materialize-into-Vec contract.
- The `recovery.rs` redo-log replay correctness was not audited (out of
  scope; covered in another category). GH-G17 cites the existence of
  the test file, not its contents.
- `RedbPrimary::stats` returns a `memory_bytes` field computed from
  `t.len() * (32 + ENTRY_VALUE_SIZE + 64)` (line 369) which is a
  rough estimate, not actual file size. Not a vulnerability — flagged
  here only because the field name is misleading.
- The 32-bit `usize` panic concern in GH-15 was not exercised because
  the supported targets are 64-bit only; a 32-bit build is theoretical.
- `cluster_secret`-protected inter-node opcodes (240-243, 250-253) were
  not audited for DoS surface. They use the same frame decoder path as
  client opcodes, so GH-01/02/04/05 conclusions apply, but cluster-
  specific allocation amplifications were not enumerated.
- The threading model around `active_connections.fetch_add /
  fetch_sub` was assumed correct; the `Relaxed` ordering is fine for a
  counter but a careful audit of the counter's role in the
  `is_shutting_down`/drain path was deferred (out of scope for the DoS
  audit).
