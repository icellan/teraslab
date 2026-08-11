# Replay nonce for HMAC-authenticated inter-node TCP frames

Status: **PROPOSED — design for review, no code.** Closes the E-4 residual
documented in `src/cluster/auth.rs` (module docs, "Replay defense (E-4)").

All file references are against `fix/e2e-scenarios-green` @ `d5f5b4e`. Every
claim about current behaviour below was read out of a function body or the
module doc sitting directly on it, not inferred.

## 1. Threat model recap — what this closes that idempotency does not

The current inter-node frame auth is `HMAC-SHA256(cluster_secret,
payload || timestamp_ms)` with a wire suffix `[timestamp:8][tag:32]`
(`src/cluster/auth.rs`, `sign_with_timestamp` / `verify_frame_streaming`).
The timestamp is covered by the tag, and `verify` enforces a clock-skew
window (`max_clock_skew`, 5 minutes by default). This authenticates the
sender and bounds *how old* a captured frame can be — it does **not**
prevent an on-path attacker who captured a valid frame from re-sending the
identical bytes inside the window. The attacker needs no knowledge of the
secret; capture-and-resend is the entire capability assumed here.

The E-4 audit delegated replay defense to per-opcode idempotency and
recorded, in the auth.rs opcode table itself, exactly where that delegation
leaks:

1. **The OOB / migration bypass.** `OP_REPLICA_BATCH` frames that are
   migration-flagged (`FLAG_MIGRATION_BATCH`) or out-of-band
   (`first_sequence == 0` with non-empty ops) bypass the applied-sequence
   journal entirely (`src/replication/receiver.rs`,
   `handle_replica_batch_with_tracker` doc: "Out-of-band batches … bypass
   the tracker entirely: every op is applied and the watermark is
   untouched"). Below the journal, the per-record `master_generation` guard
   absorbs re-deliveries — but `Delete`, `PruneSlot`, `PruneSlotIfSpentBy`,
   and `RemoveConflictingChild` carry **no generation** and apply
   unconditionally on the receiver. A captured OOB frame containing one of
   these re-applies **with effect** for as long as its embedded timestamp
   stays inside the skew window. A replayed `Delete` against a re-created
   record is the concrete data-loss shape.

2. **The vouchable rule bounds a different arm.** The assignment-aware
   stale-epoch gate (`stale_batch_from_current_master`,
   `src/replication/receiver.rs`) refuses to vouch past the epoch fence for
   exactly those four guard-less ops. That bounds the *stale-cluster_key*
   arm; its own doc comment states the residual plainly: a replayer within
   the skew window "regains only the post-epoch-bump tail it already had
   pre-bump" — i.e. the **same-epoch** replay stands, accepted per the E-4
   table's escape clause. This design closes that clause.

3. **Response replay across transport generations.** The batch sender
   verifies ACK frames and rejects a `request_id` mismatch
   (`src/replication/tcp_transport.rs`, `recv_ack`, F-G7-002) — but
   `request_id` restarts at 1 on every new `TcpReplicaTransport`. An
   on-path attacker who suppresses a live batch and replays a captured
   `ReplicaAck::Ok` whose `request_id` happens to align (trivially
   predictable: 1, 2, 3, …) forges a durability acknowledgment for a batch
   the replica never applied. Nothing in the current scheme distinguishes
   ACKs from different connection generations.

4. **A standing maintenance tax.** The E-4 table ends with a MUST: any new
   mutating inter-node opcode must either prove idempotency under replay or
   force an upgrade of this layer first. Every feature that touches the
   inter-node protocol has to re-litigate this. A transport-level guarantee
   retires the escape clause instead of growing the table.

What idempotency **does** still cover, and keeps covering: benign
sender-driven retries (same content, re-sent by the real master after a
timeout), crash-recovery re-ships, and catch-up overlaps. Those are not
replays and the nonce layer must not break them — see §4.5. The per-opcode
mechanisms stay as defense-in-depth; they were built for retry correctness,
not just replay, and this design does not remove any of them.

## 2. Current state the design must fit

### 2.1 One signed-input layout, five callers

The signed-input layout (`payload || timestamp`) is shared by every
inter-node caller (auth.rs module doc, "Adding one is not free"):

| Caller | Role | Connection shape |
|---|---|---|
| `src/cluster/swim.rs` | SWIM UDP gossip, `sign`/`verify` on datagrams | connectionless UDP |
| `src/cluster/coordinator.rs` | topology propose/vote/commit, partition reports, migration transfer + baseline/delta streams — `exchange_frame` / `send_topology_frame` | mostly one fresh `TcpStream` per RPC (`send_topology_frame` connects per call); migration streams are long-lived and carry many frames |
| `src/replication/tcp_transport.rs` | master→replica batch send + ACK verify (`sign_frame` / `verify_frame`) | long-lived, pooled |
| `src/replication/receiver.rs` | dedicated replication listener, streaming verify + signed ACK responses (`handle_connection`) | long-lived, one thread per accepted connection, strictly serial frame loop |
| `src/server/mod.rs` | main port: streaming verify for inter-node opcodes, signed responses (`write_response`) | mixed client + inter-node traffic on one port; auth applies per-frame by opcode (`is_inter_node_auth_opcode`, `src/protocol/opcodes.rs`) |

Two structural facts make the chosen scheme cheap:

- **Signed frames are serial per connection, both directions.** On the main
  port, auth-required requests are excluded from pipelining and run inline
  behind a drain barrier (`is_pipelineable` in `src/server/mod.rs`: "nor
  requires per-frame HMAC verification/signing"), so their responses are
  written in request order. The replication receiver's `handle_connection`
  loop is fully serial. On the send side, the per-address pool slot
  (`PerAddrSlot` in `src/server/dispatch.rs`) serializes sequence
  assignment, send, and ACK under one mutex; migration streams and
  coordinator RPCs are single-threaded per stream. There is no code path
  today that interleaves two signed frames from concurrent writers on one
  TCP stream.
- **Reconnect machinery already exists at every sender.** The pool slot
  reconnects via `TcpReplicaTransport::connect_with_auth` on send failure
  (`send_batch_on_slot` retry path in `src/server/dispatch.rs`); migration
  and topology RPCs open fresh connections and have retry loops. Killing a
  connection is already a recoverable event everywhere.

### 2.2 The in-repo precedent: SWIM's F-G8-003

The SWIM UDP path already carries its own replay defense
(`src/cluster/swim.rs`): a per-sender monotonic `sender_seq` in every
signed datagram, checked against a per-peer 256-bit sliding window
(`ReplayWindow`), keyed by `(NodeId, incarnation)` so a rebooted peer —
whose seq counter restarts at 1 — resets the window cleanly
(`check_and_record_for_incarnation`). Three properties of that design are
consequences of UDP and do **not** transfer to TCP:

- the sliding window absorbs datagram reordering — TCP delivers in order;
- the incarnation exists because there is no connection whose lifetime
  could scope the seq space — TCP has exactly that;
- the persisted `persisted_incarnation + 1` bump exists because the seq
  space must survive across runs — a connection-scoped space does not.

The TCP design below therefore deliberately does *not* extend F-G8-003's
window/incarnation machinery; it takes only the principle (HMAC-covered
monotonic counter) and lets the TCP connection do the work UDP made SWIM
do by hand. SWIM itself is untouched (§9).

## 3. Options considered

### A — Per-peer (NodeId, incarnation, seq), SWIM-style, on TCP

Extend F-G8-003 verbatim: every signed TCP frame carries
`(sender_node_id, incarnation, seq)`; receivers hold a per-peer window.

Rejected:

- Frames do not carry a sender NodeId today; the auth envelope would need
  one, and the receiver would have to trust it pre-verification for window
  lookup.
- One sender legitimately holds **many concurrent connections** to one
  receiver (per-address pool slot + up to `migration_pool_size` = 128
  migration streams + coordinator one-shots). A single per-peer seq space
  interleaved across 128 pipelined streams needs a reorder window sized for
  cross-connection skew, not UDP jitter — 256 bits is not obviously enough,
  and a false reject here fails a migration. Sizing it honestly means
  sizing it for the worst concurrent fan-out, forever.
- It needs the incarnation-persistence machinery (SWIM persists
  `persisted_incarnation`) or a boot-random equivalent, plus receiver-side
  per-peer state with an eviction story.

### B — Per-connection session: receiver-issued token + exact-increment seq

Scope the replay defense to the TCP connection. The receiver issues a
random token at connection setup; a per-connection key is derived from it;
every signed frame carries a strictly-incrementing per-direction sequence
number under the MAC. Cross-connection replay fails the MAC (different
session key); same-connection replay fails the exact-increment check.
Reconnect and restart need no special handling because the session dies
with the connection.

**Chosen.** §4 specifies it. The decisive argument: every cost auth.rs
recorded against "adding a nonce" was a cost of Option A's shape —
threading counters through sender loops, per-peer receiver tracking,
incarnation-style resets. Scoping to the connection makes each of those
either free or local (§5 answers them one by one).

### C — TLS for inter-node links

Would close replay (and add confidentiality) wholesale. Rejected for this
design: certificate/key distribution and rotation machinery, a large new
dependency surface in a codebase that deliberately runs a single
shared-secret HMAC, and a per-connection handshake far heavier than one
HMAC round-trip. Revisit if inter-node confidentiality ever becomes a
requirement; nothing in §4 forecloses it.

## 4. The scheme

### 4.1 Session establishment

A new inter-node opcode `OP_AUTH_SESSION` (proposed value 245; the range
245–249 is free between `OP_MIGRATION_TRANSFER_REQUEST` = 244 and
`OP_HEARTBEAT` = 250. NOTE: `OP_HELLO` = 107 already exists as the
client-facing version/feature hello — the name `OP_AUTH_SESSION` is chosen
to avoid collision).

Handshake, one round-trip, sender-initiated (the receiver cannot speak
first: the main port is shared with plain clients, and a server-first hello
would corrupt every client connection):

1. Sender connects, generates a 16-byte `client_nonce` (via the existing
   `getrandom` dependency, already in `Cargo.toml`), and sends
   `OP_AUTH_SESSION { client_nonce }` signed with the **base** scheme
   (`payload || timestamp`, skew-checked) — no session exists yet, so the
   base scheme is the bootstrap.
2. Receiver verifies (HMAC + skew), generates a 16-byte random
   `session_token`, stores per-connection session state (see 4.3), and
   responds with `{ client_nonce, session_token }` signed with the base
   scheme. Echoing `client_nonce` under the tag binds the response to this
   request: a replayed *hello response* from an older connection fails the
   echo check, so an attacker cannot feed the sender a stale token
   (doing so would only produce a fail-closed connection anyway — the
   attacker gains DoS it already has — but the echo keeps the failure at
   the handshake where it is diagnosable).
3. Both sides derive the session key:

   ```
   K_conn = HMAC-SHA256(cluster_secret, "teraslab-conn-v1" || session_token)
   ```

   and initialize two counters: `seq_out = 1`, `seq_in_expected = 1`.

Replay of the *hello request* is harmless: it mints a fresh token on a
fresh connection that the attacker cannot use (producing a frame under
`K_conn` requires the secret). A second `OP_AUTH_SESSION` on a connection
that already has a session closes the connection (fail-closed; there is no
legitimate re-key mid-connection — reconnect instead).

### 4.2 Per-frame layout and MAC input

Session frames replace the base suffix with:

```
wire:      [payload][seq:8][timestamp:8][tag:32]      (suffix 48 bytes)
MAC input: payload || seq_le || timestamp_le          keyed by K_conn
```

- `seq` is the sender's per-connection, per-direction counter,
  post-incremented per signed frame. Requests and responses number their
  own directions independently (each side's `seq_out` is the other side's
  `seq_in_expected`).
- The session token itself never appears on the wire after the handshake;
  it is bound in through `K_conn`. A frame captured on connection X cannot
  verify on connection Y because Y's key differs — this is what closes
  full-connection capture replay, which a wire-carried sender-chosen token
  could not (a receiver seeing a "new" connection replaying old
  token+frames has no state to distinguish it).
- The timestamp is retained inside session frames. It is technically
  redundant once seq + token bind freshness, but it keeps
  `auth_skew_rejections_total` alive as the NTP-drift signal (E-5's
  operator remediation split: skew ⇒ fix clocks, tag ⇒ fix secret), keeps
  hello and session frames the same shape modulo the seq field, and costs
  8 bytes on frames that are typically KiB–MiB. Flagged as open question
  §10.1.
- Verification order mirrors today's streaming verifier
  (`verify_signed_body_streaming`): stream payload through the MAC, then
  the 16-byte seq+timestamp tail, constant-time tag compare, then the
  freshness/seq checks — tag first, so seq mismatch on a forged frame is
  never reported as a replay.
- The seq check is **exact increment**: `seq == seq_in_expected`, else the
  connection closes. TCP ordering plus the serial-writers fact (§2.1) makes
  exact increment achievable, and it is strictly stronger than
  monotonicity: a *deleted* frame (on-path suppression) is detected at the
  next frame, not silently absorbed. No sliding window exists to tune,
  which is the point of scoping to the connection.
- `SIGNED_SUFFIX_LEN` grows 40 → 48 for session frames. Every existing
  budget that adds `SIGNED_SUFFIX_LEN` on top of `MAX_FRAME_SIZE`
  (`send_batch`'s budget check and `recv_ack`'s ACK cap in
  `tcp_transport.rs`, `exchange_frame`'s response cap in `coordinator.rs`,
  the receiver's `max_wire_frame_size`, the server's accept-path cap)
  picks the new constant up by name — the audit in §5 lists them.

### 4.3 State, reset, and restart semantics

**Sender side:** `session_token`/`K_conn`, `seq_out`, `seq_in_expected`
live inside the connection object — `TcpReplicaTransport` (which already
owns `request_id` state per connection), the coordinator's authenticated
stream wrapper (new, §5.3), and nothing else. No global maps.

**Receiver side:** the same triple lives in the connection handler's stack
frame — next to `read_buf` in the server accept loop, next to `body` in
`receiver.rs`'s `handle_connection`. It dies when the handler returns. No
per-peer registry, no eviction policy, no cleanup task.

**Reconnect:** a new TCP connection performs a new handshake and gets a
fresh token and fresh counters. This is the entire reset story. The
incarnation-style reset auth.rs priced in is not a mechanism to build —
it is what a connection-scoped session does by construction.

**Process restart, either side:** all TCP connections die with the
process; every surviving peer's next send fails and reconnects through the
existing retry paths (§2.1). Nothing about sessions is persisted, so there
is no stale-state window and nothing analogous to SWIM's
`persisted_incarnation` bump.

**Counter exhaustion:** at `seq_out == u64::MAX` the sender closes the
connection instead of wrapping. Unreachable in practice (2^64 frames on
one connection); stated so the overflow behaviour is defined and
fail-closed.

### 4.4 Pooled connections and multi-frame chunked sends

- **Per-address pool** (`REPL_POOL` / `PerAddrSlot`,
  `src/server/dispatch.rs`): the slot's `TcpReplicaTransport` carries its
  session; all sends on the slot happen under the slot mutex, so `seq_out`
  needs no additional synchronization. The slot's existing
  reconnect-on-failure path constructs a new transport via
  `connect_with_auth`, which is where the handshake will live — the pool
  code itself does not change. The dense replication stream cursor
  (`next_sequence`, R-D1/D-3) is a *different* sequence space (per replica
  address, survives reconnects, drives the applied-sequence journal) and is
  untouched; the auth seq is per-connection and invisible above the
  transport.
- **Migration streams** (`run_migration_batch`, `send_delta_ops`,
  baseline streaming in `coordinator.rs`): one handshake per stream,
  amortized over the whole baseline/delta transfer. Chunked sub-batches
  (`split_for_wire` / `ReplicaOp::OpChunk`, scenario 11) are each their own
  frame and each consume one seq; `exchange_frame`'s strict
  request→response alternation on these streams means both directions stay
  exact-increment. A dropped or reordered chunk frame surfaces as a seq
  gap → connection close → the migration retry path re-runs, exactly as it
  does today for a torn connection. Chunk reassembly state
  (`ChunkStaging`, receiver.rs) is keyed above the auth layer and is
  dropped on NAK — unchanged.
- **One-shot topology RPCs** (`send_topology_frame`): pay one extra
  round-trip per RPC. These are rare (member changes, votes, commits,
  periodic partition reports) and already tolerate 500 ms connect + 2 s
  read timeouts; the added RTT is noise. Accepting this cost keeps the
  rule simple: *every* signed TCP frame is a session frame, no exempt
  opcodes (open question §10.2 records the alternative).

### 4.5 What must keep working: retries are not replays

The master's benign re-send of an un-ACKed batch travels either on the same
connection (next seq — fine) or on a reconnect (new session, fresh seqs —
fine). The applied-sequence journal and generation guards then classify it
as a duplicate at the *content* level, exactly as today. The nonce layer
sits strictly below content-level idempotency and never sees "the same
batch twice" as an error — it only rejects *the same bytes on the same
connection out of sequence*. No existing retry, catch-up, or crash-recovery
path re-sends bytes on the same connection without re-signing (all go
through `sign_frame` per attempt), so none can trip the seq check.

## 5. Per-caller inventory (file-level)

Every cost recorded in auth.rs's "Adding one is not free" paragraph,
answered concretely:

| # | File | Change |
|---|---|---|
| 1 | `src/cluster/auth.rs` | New: `SessionKey` derivation (`K_conn`), `sign_frame_session`, `verify_signed_body_streaming_session` (+ buffered wrapper), `SESSION_SUFFIX_LEN = 48`, session-aware rejection helper bumping a new `auth_replay_rejections_total` metric (distinct from hmac/skew per the E-5 pattern). Base `sign`/`verify`/`sign_with_timestamp` remain **unchanged** — SWIM and the hello bootstrap use them. Rewrite the E-4 module doc: the opcode table's escape clause is retired; the table survives as the retry-idempotency record. |
| 2 | `src/protocol/opcodes.rs` | `OP_AUTH_SESSION = 245`; add to `is_inter_node_auth_opcode`. Payloads: request `[client_nonce:16]`, response `[client_nonce:16][session_token:16]`. Update `tests/g5_protocol_auth.rs` pins. |
| 3 | `src/replication/tcp_transport.rs` | `TcpReplicaTransport` gains session fields; `connect_with_auth` performs the handshake when `auth_secret` is set (both production creation sites — the pool slot's connect and retry paths in `dispatch.rs` — inherit it with **zero call-site changes**); `send_batch` signs via session; `recv_ack` verifies ACK session seq (closing §1.3) in addition to the request_id check. `from_stream_with_auth` (test-double entry) takes/derives a session explicitly so tests exercise the real path. Suffix-size constants: budget check in `send_batch`, ACK cap in `recv_ack`. |
| 4 | `src/server/dispatch.rs` | No structural change — the pool holds transports that now carry sessions. Its one wire-crossing `first_sequence: 0` use is the empty-ops watermark probe in `send_replica_ops_loop`, which rides the pooled transport (covered by row 3); the in-process compensation path never crosses the wire and never touches auth. The effectful OOB batches all originate in the coordinator's migration streams — row 5. |
| 5 | `src/cluster/coordinator.rs` | New small wrapper `AuthedStream { stream, session }` produced by an authenticated-connect helper; `exchange_frame` takes it and signs/verifies with the session; `send_topology_frame` and the migration baseline/delta/transfer paths (`run_migration_batch`, `send_delta_ops`, the `OP_MIGRATION_TRANSFER_REQUEST` client) construct it once per stream. This is the largest mechanical diff (many `&mut TcpStream` parameters become `&mut AuthedStream`) but is type-driven and compiler-enforced. |
| 6 | `src/replication/receiver.rs` | `handle_connection`: stack-local session state; accept `OP_AUTH_SESSION` as the first signed frame (issue token); thereafter require session framing on every signed frame, exact-increment check before dispatch, session-sign the ACK responses. Signed frame with no session → close. |
| 7 | `src/server/mod.rs` | Accept loop: session state alongside the existing per-connection state; `OP_AUTH_SESSION` handled at the auth gate (before dispatch — it never reaches handlers); `auth_required` frames verified via the session streaming verifier; `write_response` signs auth responses with the session. Client (non-auth) frames on the same connection are untouched and consume no seq. |
| 8 | `src/cluster/swim.rs` | **Unchanged.** UDP keeps F-G8-003. A doc cross-reference in both directions (window/incarnation for UDP, session/exact-increment for TCP) so the next auditor finds the split deliberate. |

Receiver-side per-connection tracking (auth.rs cost #2): rows 6–7 — stack
locals, no registry. Sender-loop threading (cost #1): rows 3 and 5 — state
rides the objects that already own the socket; no fan-out through call
sites. Incarnation-style reset (cost #3): §4.3 — structural, not built.

## 6. Failure modes

All verification failures on the session layer are **fail-closed: close
the connection.** Justification: every sender already treats a torn
connection as a retryable event with working machinery behind it (§2.1),
and the alternative — logging and accepting — is indistinguishable from
not shipping the feature. Specific cases:

- **Seq mismatch (gap or repeat).** Either an on-path actor suppressed or
  re-injected a frame, or a code bug double-signed/skipped a counter. Both
  warrant the loudest cheap signal: close, `warn` with expected/got, bump
  `auth_replay_rejections_total`. The sender reconnects and re-sends; the
  content layer dedups. A *persistent* attacker on-path degrades the link
  to reconnect-churn — which is the DoS it already has today by dropping
  packets; no new availability surface is created.
- **Signed frame before handshake.** Close with
  `ERR_CLUSTER_AUTH_FAILED`. This is the mixed-version signature during a
  botched cutover (§7) and the diagnostic must say so.
- **Handshake response bad echo / bad tag.** Sender closes and retries
  through its normal connect-failure path.
- **Receiver restart mid-stream.** Connection dies with it; nothing
  session-related persists; reconnect handshakes fresh. The
  applied-sequence journal keyed by peer address (`stream_key`) preserves
  content-level dedup across the reconnect exactly as today.
- **Desync cannot be silent.** There is no arm in which the two sides'
  counters drift and traffic continues: any drift fails the next frame's
  exact-increment check and tears the connection down. This property is
  why exact-increment was chosen over "strictly greater".
- **Trusted-overlay mode (`cluster_secret` unset).** No HMAC today ⇒ no
  session layer either; behaviour is byte-identical to current. Replay
  protection has exactly the same activation condition as authentication
  itself. (The E2E harness runs trusted-overlay by design; see §8.)

## 7. Cutover

Wire-format changes are free right now (recorded project decision: not in
production, no migration constraints). The cutover is still designed, not
assumed:

- **Both-sides upgrade, no mixed-version support.** A new receiver
  requires session framing on signed frames (old sender's first signed
  frame → `ERR_CLUSTER_AUTH_FAILED`, "session required", close). A new
  sender's `OP_AUTH_SESSION` against an old receiver is rejected as an
  unknown opcode → sender fails the connect with a distinct error naming
  the version mismatch. Both failure directions are loud, immediate, and
  diagnosable — not silent acceptance.
- **No compatibility flag.** A flag would recreate the mixed-version
  matrix the free cutover exists to avoid, and an operator who could set
  it wrongly would silently disable replay defense cluster-wide. The
  activation condition stays exactly `cluster_secret.is_some()`.
  (Deviates from the ship-inert-behind-flags precedent used for failover;
  that precedent guards *behavioral* risk on live clusters — this is a
  wire format with no deployed peers. Flagged for review, §10.3.)
- Rollback = deploy previous binary on all nodes. Nothing on disk changes
  shape; sessions are RAM-only.

## 8. Test plan

Unit (`src/cluster/auth.rs`):

- Session round-trip: sign → verify with same `K_conn`/seq succeeds and
  strips the 48-byte suffix; buffered and streaming variants agree
  byte-for-byte (mirror `streaming_verify_round_trip_matches_verify_frame`).
- Exact replay of a session frame → seq-repeat rejection with the distinct
  error, `auth_replay_rejections_total` delta (metric assertions live in
  the serial integration binary per the existing `tests/e5_auth_metrics.rs`
  split — the in-file note at the bottom of auth.rs explains why).
- Cross-session replay: frame signed under token A fails verification
  under token B's key (PermissionDenied, i.e. indistinguishable from
  forgery — correct, since the tag genuinely doesn't verify).
- Seq gap, seq tamper (flip a seq bit without re-signing → tag failure,
  not seq failure — ordering pinned), stale timestamp inside a session
  frame, truncation to below `SESSION_SUFFIX_LEN`.
- Slow-loris parity: 16 MiB wrong-tag session frame rejects with bounded
  verifier memory (extend the existing `CountingSink` test).

Transport (`src/replication/tcp_transport.rs`):

- Handshake happens on `connect_with_auth`; batches then flow; replayed
  ACK bytes (captured from an earlier exchange on the same connection)
  are rejected on seq.
- ACK with correct session seq but wrong `request_id` still rejected
  (F-G7-002 retained independently).
- Reconnect mints a new session: transcript from connection 1 fed into
  connection 2 fails.

Receiver + integration:

- **The anchor regression, closing the E-4 caveat:** extend
  `tests/g8_e4_tcp_frame_replay.rs` — capture an honest signed OOB
  (`first_sequence == 0`) `OP_REPLICA_BATCH` whose ops include a `Delete`,
  re-send the identical bytes on the same connection and on a fresh
  connection; assert both are rejected and the record survives. Today this
  frame re-applies with effect; that test asserts the residual is gone.
- Same for a `FLAG_MIGRATION_BATCH` frame (the second bypass arm).
- Full-connection transcript replay against `receiver.rs`: hello replay
  yields a fresh token, first data frame fails the MAC.
- Main port (`server/mod.rs`): interleaved client (unsigned) and
  inter-node (session) frames on one connection — client traffic
  unaffected, seq counts only signed frames.
- Coordinator: topology propose/vote/commit over sessions; a migration
  baseline + delta with chunked sub-batches (50 MiB-tier record) over one
  session, then a forced mid-stream seq desync → stream closes →
  migration retry converges.
- Sandbox note: loopback-bind tests fail environmentally under the local
  sandbox; CI covers them (established pattern).

E2E: the standard harness runs trusted-overlay, which exercises the
inactive path for free. Session-active coverage runs under the
HMAC-enabled harness variant (the `feat/e2e-hmac-opt-in` branch's mode)
with restart/reconnect churn — the acceptance bar is **zero
`auth_replay_rejections_total` under honest churn** (no false positives),
plus convergence after forced reconnect storms.

## 9. Non-goals

- **SWIM UDP** — already defended (F-G8-003); different transport,
  different correct design; untouched.
- **Client-protocol replay defense** — client traffic is unauthenticated
  by design on a trusted client network; out of scope and unchanged.
- **Confidentiality / TLS** — §3-C; not foreclosed.
- **Trusted-overlay hardening** — no secret, no defense; same activation
  boundary as HMAC itself.
- **Removing per-opcode idempotency** — retained in full as the
  retry-correctness layer and defense-in-depth (§4.5).
- **Secret rotation / multiple keys** — orthogonal; sessions derive from
  whatever single secret is configured, and a rotation design would slot
  in at `K_conn` derivation without touching the seq scheme.
- **Persisting any session state** — explicitly rejected; RAM-only by
  construction.

## 10. Open questions for review

1. **Drop the timestamp from session frames?** Redundant for freshness
   once seq+token bind the frame to a live connection; kept in the draft
   for the NTP-drift operator signal and shape uniformity (§4.2). Dropping
   it saves 8 bytes/frame and removes the skew-reject arm from the
   steady-state path (hellos would still carry it). Either way is sound —
   preference wanted.
2. **Exempt read-only one-shot RPCs from the handshake?**
   `OP_GET_PARTITION_MAP` / `OP_GET_NODE_HEIGHT` / `OP_ADMIN_*` replays
   disclose nothing beyond what the original response disclosed, and the
   extra RTT is per-call on fresh connections. The draft includes them for
   a uniform "every signed frame is a session frame" invariant (no
   opcode-by-opcode reasoning ever again — the exact failure mode of the
   E-4 table). Exempting them would re-open a small version of that table.
   Draft position: no exemption; confirm.
3. **Ship without a compatibility flag?** §7's position (no flag, loud
   both-direction failures) deviates from the recent ship-inert precedent.
   If review wants a flag anyway, it must be a *receiver-enforcement* flag
   only (senders always handshake when the secret is set), so a
   mis-setting degrades to today's behaviour, never to a torn cluster.
4. **`getrandom` failure policy.** Token generation can theoretically fail
   at the OS boundary. Draft: fail the connect (fail-closed), never fall
   back to a weaker source. Confirm.
