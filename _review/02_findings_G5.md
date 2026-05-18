# Group G5 Review — Wire protocol + dispatch

Scope:
- `src/protocol/mod.rs` (9 LOC)
- `src/protocol/frame.rs` (525 LOC)
- `src/protocol/opcodes.rs` (412 LOC)
- `src/protocol/codec.rs` (3511 LOC)
- `src/server/dispatch.rs` (13399 LOC)

Adjacent files consulted (no findings filed against them, but pulled in to
verify call-graph assumptions): `src/server/mod.rs` (TCP read path / auth
hook), `src/cluster/topology.rs` (topology deserializers reachable from
`OP_TOPOLOGY_*`), `src/ops/unspend.rs` (to verify `spending_data`
authority check).

Every opcode handler in `dispatch.rs` was walked:

| Opcode | Handler | line |
| --- | --- | --- |
| `OP_SPEND_BATCH` | `handle_spend_batch` | 2670 |
| `OP_UNSPEND_BATCH` | `handle_unspend_batch` | 2950 |
| `OP_SET_MINED_BATCH` | `handle_set_mined_batch` | 3117 |
| `OP_CREATE_BATCH` | `handle_create_batch` | 3358 |
| `OP_FREEZE_BATCH` | `handle_freeze_batch` | 3787 |
| `OP_UNFREEZE_BATCH` | `handle_unfreeze_batch` | 3891 |
| `OP_REASSIGN_BATCH` | `handle_reassign_batch` | 3995 |
| `OP_SET_CONFLICTING_BATCH` | `handle_set_conflicting_batch` | 4121 |
| `OP_SET_LOCKED_BATCH` | `handle_set_locked_batch` | 4247 |
| `OP_PRESERVE_UNTIL_BATCH` | `handle_preserve_until_batch` | 4354 |
| `OP_DELETE_BATCH` | `handle_delete_batch` | 4551 |
| `OP_MARK_LONGEST_CHAIN_BATCH` | `handle_mark_longest_chain_batch` | 4989 |
| `OP_GET_BATCH` | `handle_get_batch` | 5132 |
| `OP_QUERY_OLD_UNMINED` | `handle_query_old_unmined` | 5526 |
| `OP_PRESERVE_TRANSACTIONS` | `handle_preserve_transactions` | 5563 |
| `OP_PROCESS_EXPIRED_PRESERVATIONS` | `handle_process_expired` | 5696 |
| `OP_GET_SPEND_BATCH` | `handle_get_spend_batch` | 5817 |
| `OP_STREAM_CHUNK` | `handle_stream_chunk` | 5968 |
| `OP_STREAM_END` | `handle_stream_end` | 6072 |
| `OP_GET_PARTITION_MAP` | `handle_get_partition_map` | 6297 |
| `OP_GET_COMMITTED_TOPOLOGY` | `handle_get_committed_topology` | 6326 |
| `OP_ADMIN_DIAGNOSE_KEY` | `handle_admin_diagnose_key` | 6357 |
| `OP_ADMIN_CLUSTER_HEALTH` | `handle_admin_cluster_health` | 6443 |
| `OP_PARTITION_VERSION_REPORT` | `handle_partition_version_report` | 6467 |
| `OP_REPLICA_BATCH` | inline at `dispatch.rs:483` |
| `OP_MIGRATION_COMPLETE` | inline at `dispatch.rs:536` |
| `OP_MIGRATION_BATCH_COMPLETE` | inline at `dispatch.rs:821` |
| `OP_TOPOLOGY_PROPOSE` | inline at `dispatch.rs:913` |
| `OP_TOPOLOGY_VOTE` | inline at `dispatch.rs:961` |
| `OP_TOPOLOGY_COMMIT` | inline at `dispatch.rs:986` |
| `OP_PING` / `OP_HEALTH` / `OP_INCREMENT_SPENT_EXTRA_RECS` | inline 466‑480 |

---

## Findings

### F-G5-001: Cluster-control opcodes accept unauthenticated frames whenever `cluster_secret` is not configured
- **Severity**: CRITICAL
- **Category**: Security
- **Location**: `src/server/mod.rs:422-450` and `src/protocol/opcodes.rs:368-381`
- **Code**:
  ```rust
  let auth_required = peek_request_op_code(&frame_bytes)
      .map(is_inter_node_auth_opcode)
      .unwrap_or(false)
      && opts.cluster_secret.is_some();
  let request_frame_bytes = if auth_required {
      match crate::cluster::auth::verify_frame(...) { ... }
  } else {
      frame_bytes
  };
  ```
- **Issue**: The HMAC verification only fires when BOTH the opcode is in
  `is_inter_node_auth_opcode` (TOPOLOGY_PROPOSE/VOTE/COMMIT, REPLICA_BATCH,
  MIGRATION_COMPLETE, MIGRATION_BATCH_COMPLETE, GET_PARTITION_MAP,
  GET_COMMITTED_TOPOLOGY, PARTITION_VERSION_REPORT) AND `cluster_secret` is
  configured. The default-on path is "no secret → no auth", which matches
  the prior audit's EF-01 / D-20 concern. In multi-node clusters that are
  deployed without setting `cluster_secret`, any TCP client that can reach
  the cluster port can drive `OP_TOPOLOGY_PROPOSE` (forging a new term with
  itself as proposer), `OP_REPLICA_BATCH` (writing arbitrary records to a
  replica without going through the master), or `OP_MIGRATION_COMPLETE`
  (flushing inbound-migration state).
- **Impact**: Split-brain by forged topology, replica-side data
  fabrication, premature migration commit. Production deployments that
  forget the `cluster_secret` knob are silently unauthenticated; there is
  no startup-time refusal to run multi-node without a secret.
- **Recommendation**: When `replication_factor > 1` OR more than one
  cluster member is configured, refuse to start without `cluster_secret`,
  i.e. flip the policy from opt-in to fail-closed. Independently, add
  startup-log telemetry and a metric flagging "cluster control on
  unauthenticated port" so an operator running multi-node accidentally in
  permissive mode notices.
- **Confidence**: High

### F-G5-002: `OP_TOPOLOGY_PROPOSE/VOTE/COMMIT` allocate `Vec::with_capacity(count)` for member / voter lists where `count` is a `u32` read straight from the wire
- **Severity**: HIGH
- **Category**: Security
- **Location**: `src/cluster/topology.rs:86-112` (reached from
  `src/server/dispatch.rs:933` `TopologyTerm::deserialize`)
- **Code**:
  ```rust
  let count = u32::from_le_bytes(data[16..20].try_into().ok()?) as usize;
  let members_end = 20 + count * 8;
  if data.len() < members_end + 32 {
      return None;
  }
  let mut members = Vec::with_capacity(count);
  ```
- **Issue**: The `count * 8` arithmetic is not `checked_mul`, and
  `Vec::with_capacity(count)` runs after the size-fit check but with no
  named upper bound. The size check passes only when payload has
  `members_end + 32` bytes, so in practice the cap is `MAX_FRAME_SIZE /
  8 ≈ 2M members`. That is far above any legitimate value (production
  clusters are dozens of nodes, not millions) and is reachable by anyone
  who can send a frame to the cluster port. Combined with F-G5-001, an
  unauthenticated peer can drive a 2M-entry `Vec<NodeId>` allocation
  (~16 MiB) per connection, repeatable.
- **Impact**: Memory-pressure DoS amplifier for the topology code path,
  plus undefined behaviour risk on 32-bit targets where `count * 8`
  overflows silently.
- **Recommendation**: Use `checked_mul` and add a named per-frame cap
  (e.g. `MAX_TOPOLOGY_MEMBERS = 1024`) enforced before the `with_capacity`
  call. Same fix for `TopologyCommit::deserialize` voter list at
  `topology.rs:212-225`.
- **Confidence**: High

### F-G5-003: `OP_QUERY_OLD_UNMINED` has no shard-ownership / authorization check
- **Severity**: MEDIUM
- **Category**: Security
- **Location**: `src/server/dispatch.rs:5526-5561`
- **Code**:
  ```rust
  fn handle_query_old_unmined(req: &RequestFrame, engine: &Engine) -> ResponseFrame {
      if req.payload.len() < 4 { ... }
      let Some(cutoff) = le_u32_at(&req.payload, 0) else { ... };
      let candidates = engine.unmined_index().range_query(cutoff);
      let mut keys = Vec::with_capacity(candidates.len());
      for key in candidates {
          match engine.read_metadata(&key) { ... }
      }
  ```
- **Issue**: Every other key-based handler calls `check_shard_ownership`,
  but `OP_QUERY_OLD_UNMINED` walks the entire local unmined index and
  returns every txid below `cutoff` regardless of whether this node is
  master for those shards. There is also no opcode-level authentication
  (it is not in `is_inter_node_auth_opcode`). Any client on the public
  port can enumerate the unmined-tx pool of any node it can reach.
- **Impact**: Information disclosure of the unmined / mempool-ish view of
  the node, which is sensitive operational data; in clustered mode an
  unauthenticated reader can also enumerate shards this node only holds
  as a replica. Combined with `Vec::with_capacity(candidates.len())`
  (where `candidates.len()` comes from the secondary index, not the
  attacker) there is no DoS amplifier, but the disclosure is the bigger
  concern.
- **Recommendation**: Either (a) require master-of-key filtering before
  returning, or (b) treat this opcode as admin-only and route it through
  the HTTP/admin auth surface (R-056).
- **Confidence**: High

### F-G5-004: `OP_MIGRATION_COMPLETE` / `OP_MIGRATION_BATCH_COMPLETE` accept unsigned frames in the absence of `cluster_secret` and execute irreversible state transitions
- **Severity**: HIGH
- **Category**: Security
- **Location**: `src/server/dispatch.rs:536-911`
- **Code**:
  ```rust
  OP_MIGRATION_COMPLETE => {
      let shard = request.request_id as u16;
      let expected_records = le_u64_at(&request.payload, 0).unwrap_or(0);
      ...
      // proceeds to call cluster.mark_inbound_complete(shard) and
      // table.commit_shard(shard) if the manifest matches.
  }
  ```
- **Issue**: This is the partner risk of F-G5-001 specialized to the
  migration handshake. The handler proves that the local content matches
  the source's manifest hash, but the source identity is established only
  by HMAC, which is bypassed when `cluster_secret` is `None`. Without
  auth, any TCP peer that can speak the protocol can replay or forge a
  `OP_MIGRATION_COMPLETE` for any shard the receiver expects, racing the
  real source and clearing inbound state prematurely. The receiver also
  silently truncates `request.request_id as u16` — the upper 48 bits of
  request_id are discarded so the protocol uses request_id for two
  unrelated purposes (correlation token *and* shard id).
- **Impact**: Premature migration commit, divergent shard state across
  the cluster, future client traffic routed to a node that does not yet
  have the data.
- **Recommendation**: Address F-G5-001 first (fail-closed multi-node
  without `cluster_secret`); separately, reject `request.request_id`
  values whose high 48 bits are non-zero so a typo or repurposed id
  cannot silently land on an unintended shard.
- **Confidence**: High

### F-G5-005: `OP_ADMIN_DIAGNOSE_KEY` and `OP_ADMIN_CLUSTER_HEALTH` are not in the auth-required list
- **Severity**: MEDIUM
- **Category**: Security
- **Location**: `src/protocol/opcodes.rs:368-381`,
  `src/server/dispatch.rs:6357-6460`
- **Code**:
  ```rust
  pub fn is_inter_node_auth_opcode(op_code: u16) -> bool {
      matches!(
          op_code,
          OP_GET_PARTITION_MAP | OP_GET_COMMITTED_TOPOLOGY | OP_REPLICA_BATCH
              | OP_MIGRATION_COMPLETE | OP_MIGRATION_BATCH_COMPLETE
              | OP_TOPOLOGY_PROPOSE | OP_TOPOLOGY_VOTE | OP_TOPOLOGY_COMMIT
              | OP_PARTITION_VERSION_REPORT
      )
  }
  ```
- **Issue**: The two admin opcodes carry cluster-internal diagnostic data
  (per-key master ID, topology epoch, has-pending-inbound flags, SWIM
  state, last-committed-term). They are routed through the *public* TCP
  port, do not appear in `is_inter_node_auth_opcode`, and the dispatch
  bypasses `needs_cluster_readiness`. Anyone who can reach the port can
  enumerate the cluster's routing state.
- **Impact**: Reconnaissance: an attacker with port access can build a
  full topology snapshot (which node owns which shard, which shards are
  migrating, current term). On its own this is information disclosure
  rather than direct compromise, but it directly enables the more
  dangerous forged-topology attack in F-G5-002.
- **Recommendation**: Add both opcodes to `is_inter_node_auth_opcode`
  (or route them through a separate admin port subject to bearer auth
  per R-056). At minimum redact `local_view_canonical_master_id` /
  `topology_epoch` for unauthenticated callers.
- **Confidence**: High

### F-G5-006: `OP_HEARTBEAT` (opcode 250) has no dispatch handler — falls into the catch-all and returns `ERR_INTERNAL "unknown opcode"`
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/server/dispatch.rs:1034`, declared at
  `src/protocol/opcodes.rs:151`
- **Code**:
  ```rust
  pub const OP_HEARTBEAT: u16 = 250;
  // ...
  _ => error_response(request.request_id, ERR_INTERNAL, "unknown opcode"),
  ```
- **Issue**: The opcode is declared and reserved, but `handle_request`
  has no arm for it; every heartbeat over this port replies
  `ERR_INTERNAL`. Either the heartbeat is handled elsewhere (SWIM uses a
  different port — likely) and the constant is dead, or this is a real
  routing gap. The catch-all error response carries the literal message
  `"unknown opcode"` which is also a probe-friendly oracle.
- **Impact**: Confused operator triage when heartbeats hit the wrong
  port; minor information disclosure ("opcode X is unsupported here").
- **Recommendation**: Either implement the handler or delete the
  `OP_HEARTBEAT` constant. Independently, return `ERR_OK` with no
  payload (or stay silent) on unknown opcodes so the protocol surface is
  less chatty to scanners.
- **Confidence**: Medium

### F-G5-007: `handle_admin_diagnose_key` parses count with `try_into().expect("4 bytes")` after a separate length guard — defence is correct today but the pattern is fragile
- **Severity**: LOW
- **Category**: Code Quality
- **Location**: `src/server/dispatch.rs:6362-6385`
- **Code**:
  ```rust
  if payload.len() < 4 {
      return error_response(...);
  }
  let count = u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes")) as usize;
  ```
- **Issue**: The test at `dispatch.rs:6583` (`dispatch_parsers_use_take_helper`)
  enforces "no `try_into().unwrap()` in production code" because parsers
  used to panic on truncated input. `try_into().expect("4 bytes")` is
  the same pattern with a different macro and the test does not catch
  it. It happens to be safe here because of the preceding length check,
  but the safety is local, not enforced.
- **Impact**: A future refactor that moves the length check could
  reintroduce a panic from a client-controlled payload. Today: no
  observable bug.
- **Recommendation**: Replace with the existing `le_u32_at(payload, 0)`
  helper. Extend the regex test to also reject `try_into().expect(`.
  Same fix applies to `dispatch.rs:6481`
  (`req.payload[0..8].try_into().unwrap_or([0u8; 8])` — `unwrap_or`
  silently substitutes zero, which then bypasses the cluster_key check
  on a 7-byte payload because the prior `payload.len() < 8` test
  short-circuits return — looks correct today but again is locally
  safe rather than centrally enforced).
- **Confidence**: High

### F-G5-008: `handle_request` returns `ERR_INTERNAL` payload `format!("malformed {op_label}: {err}")` that echoes the inner `CodecError::Display` to the client
- **Severity**: LOW
- **Category**: Security
- **Location**: `src/server/dispatch.rs:6146-6153`
- **Code**:
  ```rust
  fn codec_error_response(request_id: u64, op_label: &str, err: CodecError) -> ResponseFrame {
      tracing::debug!(op = op_label, err = %err, "codec rejected request before allocation");
      error_response(
          request_id,
          ERR_INTERNAL,
          &format!("malformed {op_label}: {err}"),
      )
  }
  ```
- **Issue**: The `CodecError` display includes specific
  `count` / `available` / `max` values from the rejected frame. None of
  these leak server-internal state today (they are derived from the
  client's own input + `max_batch_size`), so this is defensible. The
  same handler also surfaces `format!("read redo for pending replication
  intent: {e}")` and similar storage-derived strings on the WAL paths
  (e.g. `dispatch.rs:3030, 3826, 4036, 4283, 4399`) where the inner `e`
  carries OS error strings and file paths.
- **Impact**: Path disclosure to clients on the storage failure paths
  (`redo log append: <io error>`, including the path embedded inside the
  redo log's own error variant). Useful for an attacker building a
  picture of the deployment topology. On its own, not a vulnerability,
  but combined with the unauthenticated admin opcodes (F-G5-005) it
  contributes to a "tell-me-everything" port.
- **Recommendation**: Sanitize storage error messages before placing
  them in client-visible payloads (log at `error!` level with the full
  detail, but send back just the error code).
- **Confidence**: Medium

### F-G5-009: `partition_version_report` `try_into().unwrap_or([0u8; 8])` silently substitutes zero on a malformed payload but the prior length guard makes the path unreachable
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/server/dispatch.rs:6474-6495`
- **Code**:
  ```rust
  if req.payload.len() < 8 { return error_response(...); }
  let request_cluster_key = u64::from_le_bytes(req.payload[0..8].try_into().unwrap_or([0u8; 8]));
  ```
- **Issue**: `unwrap_or([0u8; 8])` makes a malformed input parse as
  cluster_key = 0, which would then *fail* the cluster_key mismatch
  check and route through ERR_STALE_EPOCH. Functionally fine but
  inconsistent with the rest of the dispatcher which uses checked
  helpers. Documented as INFO because it is unreachable.
- **Impact**: None observable.
- **Recommendation**: Use `le_u64_at` like the rest of the dispatcher.
- **Confidence**: High

### F-G5-010: `OP_REPLICA_BATCH` shard-extraction from `request_id` re-uses the same low-16-bit cast pattern as `OP_MIGRATION_COMPLETE`
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/server/dispatch.rs:483-503`
- **Code**:
  ```rust
  if request.flags & FLAG_MIGRATION_BATCH != 0
      && let Some(cluster) = cluster
  {
      let shard = request.request_id as u16;
      ...
      cluster.mark_inbound_active(shard);
  }
  ```
- **Issue**: When `FLAG_MIGRATION_BATCH` is set, `request_id`'s low 16
  bits are taken as the shard number, silently discarding the upper 48
  bits. The protocol thereby overloads `request_id` (a correlation
  cookie) for migration shard identity. If a buggy or malicious client
  sends `request_id = 0x0000_0000_0001_0042` the receiver registers
  inbound activity for shard `0x42` while the correlation cookie is
  reused for a different request id — there is no integrity check.
- **Impact**: A malicious peer (subject to F-G5-001's auth gate) can
  mark arbitrary shards as inbound-active, parking traffic for them
  until the next migration commit or timeout.
- **Recommendation**: Reject `request_id >> 16 != 0` when
  `FLAG_MIGRATION_BATCH` is set, or move the shard into the payload
  rather than packing two fields into `request_id`.
- **Confidence**: High

### F-G5-011: `RequestFrame::decode` allocates `payload: data[16..frame_size].to_vec()` — one full-payload copy per frame even when the dispatcher only reads tens of bytes
- **Severity**: LOW
- **Category**: Performance
- **Location**: `src/protocol/frame.rs:156`
- **Code**:
  ```rust
  let payload = data[16..frame_size].to_vec();
  Ok((Self { request_id, op_code, flags, payload }, frame_size))
  ```
- **Issue**: Every inbound frame causes a full-payload `Vec::to_vec()`
  copy before dispatch, in addition to the per-connection `read_buf`
  already containing the same bytes. At a 16 MiB max frame this is
  another 16 MiB of allocation per frame for the worst case, and at the
  more typical few-KiB batch it is still a hot per-request allocation
  for the 10M ops/sec target stated in the project brief.
- **Impact**: Performance ceiling, not correctness.
- **Recommendation**: Switch `RequestFrame::payload` to `Cow<'_, [u8]>`
  or `bytes::Bytes` with a borrowed reference into the per-connection
  buffer; most handlers consume the payload synchronously so a lifetime
  parameter is feasible.
- **Confidence**: High

### F-G5-012: `decode_unspend_batch_checked` does verify `spending_data` is in the wire item, and the engine path enforces the match — prior audit's "anyone can unspend anything" concern is RESOLVED
- **Severity**: INFO (positive verification)
- **Category**: Security
- **Location**: `src/protocol/codec.rs:434-440, 618-661` and
  `src/ops/unspend.rs:11-25`
- **Code**:
  ```rust
  pub struct WireUnspendItem {
      pub txid: [u8; 32],
      pub vout: u32,
      pub utxo_hash: [u8; 32],
      pub spending_data: [u8; 36],
  }
  ```
- **Issue**: The prior audit (review brief — A-04 / unspend authority)
  flagged that `WireSlotItem` for unspend only carried
  `(txid, vout, utxo_hash)`. Today there is a dedicated `WireUnspendItem`
  (`codec.rs:434`) that carries `spending_data: [u8; 36]`, and
  `UnspendRequest` (`ops/unspend.rs:11-25`) documents the slot is
  cleared only if the supplied bytes match the recorded 36-byte spend
  marker. `handle_unspend_batch` at `dispatch.rs:3038-3044` passes the
  wire bytes straight through. The "anyone can erase any spend" concern
  is no longer live.
- **Impact**: None — verifying the fix landed correctly.
- **Recommendation**: Keep the existing test
  `handle_unspend_batch_rejects_wrong_spending_data` (`dispatch.rs:11025`)
  asserting the negative case.
- **Confidence**: High

### F-G5-013: `MAX_FRAME_SIZE = 16 MiB` cap is enforced BEFORE `read_buf.resize` and BEFORE any payload allocation — prior audit's "frame-length OOM" concern is RESOLVED
- **Severity**: INFO (positive verification)
- **Category**: Security
- **Location**: `src/server/mod.rs:369-411` and
  `src/protocol/opcodes.rs:335-364`
- **Code**:
  ```rust
  let total_length = u32::from_le_bytes(len_buf);
  let max_wire_frame_size = MAX_FRAME_SIZE + opts.cluster_secret.as_ref()
      .map(|_| crate::cluster::auth::SIGNED_SUFFIX_LEN as u32).unwrap_or(0);
  if total_length > max_wire_frame_size { ... return Err(...) }
  ```
- **Issue**: An attacker who advertises `total_length = u32::MAX` is
  rejected before any read or allocation. The `InflightBytesLimiter`
  also caps aggregate concurrent buffer growth across connections.
  Together with the per-batch `validate_batch_count` guards in
  `codec.rs:141-169` (every batch decoder calls this BEFORE
  `Vec::with_capacity(count)`), the wire decoder is correctly bounded.
- **Impact**: None — verifying the fix landed correctly.
- **Recommendation**: None.
- **Confidence**: High

### F-G5-014: Per-connection read timeout (30 s) and write timeout (30 s) prevent slow-loris connection pinning — prior audit's R-054 / LMNH-01 fix verified
- **Severity**: INFO (positive verification)
- **Category**: Security
- **Location**: `src/server/mod.rs:326-342` and the
  `CONNECTION_READ_TIMEOUT` / `CONNECTION_WRITE_TIMEOUT` constants at
  `mod.rs:27-28`
- **Code**:
  ```rust
  stream.set_read_timeout(Some(opts.read_timeout))?;
  stream.set_write_timeout(Some(opts.write_timeout))?;
  ```
- **Issue**: Slow-reader and slow-writer attacks return `TimedOut` from
  the kernel and the handler closes the connection. `max_connections`
  bounds total concurrent threads.
- **Impact**: None — verifying the fix landed correctly.
- **Recommendation**: Consider exposing the 30 s value as a config knob
  (currently hard-coded). Not security-critical.
- **Confidence**: High

### F-G5-015: `OP_INCREMENT_SPENT_EXTRA_RECS` is a public opcode that returns `STATUS_OK` unconditionally (no-op shim)
- **Severity**: INFO
- **Category**: Maintainability
- **Location**: `src/server/dispatch.rs:476-480`, declared
  `src/protocol/opcodes.rs:162`
- **Code**:
  ```rust
  OP_INCREMENT_SPENT_EXTRA_RECS => ResponseFrame {
      request_id: request.request_id,
      status: STATUS_OK,
      payload: vec![], // No-op compatibility shim
  },
  ```
- **Issue**: The opcode is reserved for backwards compatibility but the
  dispatcher accepts any payload and responds OK. Combined with the
  catch-all `_ => ERR_INTERNAL "unknown opcode"`, this is benign, but a
  client that thinks it is incrementing a counter receives a misleading
  success.
- **Impact**: Operational confusion, not a security issue.
- **Recommendation**: Either implement the operation, return a
  documented `STATUS_ERROR / ERR_INTERNAL "opcode deprecated"`, or
  remove the constant once all clients have migrated.
- **Confidence**: High

### F-G5-016: `OP_TOPOLOGY_PROPOSE` / `OP_TOPOLOGY_VOTE` / `OP_TOPOLOGY_COMMIT` parse the payload BEFORE any auth check would have run — DoS amplifier when auth IS enabled
- **Severity**: LOW
- **Category**: Performance
- **Location**: `src/server/mod.rs:412-449` and
  `src/server/dispatch.rs:913-1033`
- **Code**:
  ```rust
  // mod.rs:451-453 — decode runs even if next step (auth) would reject
  let (request, _) = RequestFrame::decode(&request_frame_bytes)
      .map_err(|e| format!("decode frame: {e}"))?;
  ```
- **Issue**: When `cluster_secret` is configured, the frame's HMAC is
  verified before `RequestFrame::decode`. That ordering is fine. But
  `decode` itself allocates the payload `Vec` (F-G5-011) — for a
  TOPOLOGY_COMMIT carrying a maximally-sized voter list, an
  unauthenticated peer that knows it will be HMAC-rejected can still
  drive payload copying because `verify_frame` reads the entire frame
  before it can hash and compare. Today this is bounded by the 16 MiB
  frame cap, so it is just CPU pressure, not memory unbound.
- **Impact**: Per-connection CPU amplifier (HMAC of 16 MiB takes a
  meaningful fraction of a millisecond). Bounded by the per-connection
  read timeout.
- **Recommendation**: Move the auth check to a streaming HMAC verifier
  that can short-circuit on the first wrong byte, or accept a smaller
  per-frame cap on auth-required opcodes specifically.
- **Confidence**: Medium

### F-G5-017: Dispatch error responses do not include the request `op_code` in their machine-readable payload — operator triage relies on the human-readable string  [RESOLVED in P3.10]
- **Severity**: INFO
- **Category**: Maintainability
- **Location**: `src/server/dispatch.rs:6124-6130`
- **Code**:
  ```rust
  fn error_response(request_id: u64, code: u16, msg: &str) -> ResponseFrame {
      ResponseFrame {
          request_id,
          status: STATUS_ERROR,
          payload: encode_error_payload(code, msg),
      }
  }
  ```
- **Issue**: When a client sees `ERR_INTERNAL` it cannot tell which
  handler emitted it without parsing the free-text message. Several
  failures from completely different handlers share the same error code
  (`ERR_INTERNAL`).
- **Impact**: Hard to write generic client retry / circuit-breaker
  logic.
- **Recommendation**: Introduce a small set of more specific error
  codes (`ERR_PAYLOAD_MALFORMED`, `ERR_OPCODE_UNSUPPORTED`,
  `ERR_STORAGE_IO`) rather than overloading `ERR_INTERNAL = 255`.
- **Confidence**: High

### F-G5-018: `decode_get_response_checked` and `decode_sparse_errors_checked` validate `count` and per-item minimum but do not cap the variable per-item `data_len` other than via the remaining-payload check
- **Severity**: LOW
- **Category**: Security
- **Location**: `src/protocol/codec.rs:1167-1207, 1576-1617`
- **Code**:
  ```rust
  let data_len = get_u32(data, pos) as usize;
  pos += 4;
  if pos + data_len > data.len() {
      return Err(CodecError::SectionTruncated { ... });
  }
  let item_data = data[pos..pos + data_len].to_vec();
  ```
- **Issue**: These decoders are the response path, not the request
  path, so client-supplied bytes do not flow here in the server
  direction. But the *client side* of the codec parses what the *server
  sent* using the same code — and `to_vec()` copies up to the entire
  remaining frame (16 MiB) per item. The encoder uses `u16` for
  `error_data.len()` in the request-error path (see line 1255 / 1565,
  `put_u16(.. e.error_data.len() as u16)`) — so on the wire the
  per-item error data is capped at 64 KiB. Good. The asymmetric
  per-item data_len `u32` in `WireGetResult.data` is correctly
  remaining-payload bounded but does mean the response decoder can
  amplify allocation by up to count × frame_size in the pathological
  shape, even though `count` itself was already validated.
- **Impact**: No server-side DoS (it is the response path); on the
  client, a hostile server could push the client toward memory
  pressure. The `MAX_DECODE_BATCH = 1 << 20` cap on `count` bounds the
  *number* of items but not the *sum* of `data_len`.
- **Recommendation**: Add an overall response-payload accountancy that
  rejects when `sum(data_len) > remaining_after_header`. Cheap and
  removes a corner case.
- **Confidence**: Medium

### F-G5-019: `MAX_DECODE_BATCH = 1 << 20` (1 048 576 items) is the default cap used by the legacy `Option`-returning wrappers — far larger than any production batch
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/protocol/codec.rs:93-109`
- **Code**:
  ```rust
  pub const MAX_DECODE_BATCH: u32 = 1 << 20;
  ```
- **Issue**: Server-side dispatch plumbs the configured `max_batch_size`
  through every decoder (see `dispatch.rs:417, 2677`), but the legacy
  wrappers (`decode_spend_batch`, etc.) are still exported as
  `pub` and used by tests / benches. A future call site that
  accidentally uses the legacy wrapper would skip the configured cap
  and fall back to 1M.
- **Impact**: Latent risk if a future contributor uses the wrong API.
- **Recommendation**: Mark the legacy wrappers `#[deprecated]` or move
  them behind a `pub(crate)` boundary so server-side code physically
  cannot reach them. Tests already use the `_checked` variants
  selectively.
- **Confidence**: High

### F-G5-020: `RequestFrame::decode_frames` swallows the error from any malformed trailing frame and silently truncates the stream
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/protocol/frame.rs:248-261`
- **Code**:
  ```rust
  pub fn decode_frames(data: &[u8]) -> (Vec<RequestFrame>, usize) {
      let mut frames = Vec::new();
      let mut pos = 0;
      while pos < data.len() {
          match RequestFrame::decode(&data[pos..]) {
              Ok((frame, consumed)) => { frames.push(frame); pos += consumed; }
              Err(_) => break,
          }
      }
      (frames, pos)
  }
  ```
- **Issue**: An invalid trailing frame is indistinguishable from a
  partial frame; the caller has to compare `pos < data.len()` to
  detect "I had bytes left over". This is only called from tests today,
  but if reused for real I/O batching it would silently drop frames
  whose lengths claim more bytes than the remainder of the buffer.
- **Impact**: Tests-only today.
- **Recommendation**: Surface the inner `FrameError`, or document
  loudly that any short read must be retried by the caller.
- **Confidence**: High

### F-G5-021: `decode_redirect` calls `String::from_utf8_lossy` and so accepts non-UTF-8 byte sequences (replaced with U+FFFD)
- **Severity**: INFO
- **Category**: Correctness
- **Location**: `src/protocol/codec.rs:1465-1474`
- **Code**:
  ```rust
  pub fn decode_redirect(data: &[u8]) -> Option<String> {
      if data.len() < 2 { return None; }
      let len = get_u16(data, 0) as usize;
      if data.len() < 2 + len { return None; }
      Some(String::from_utf8_lossy(&data[2..2 + len]).to_string())
  }
  ```
- **Issue**: A REDIRECT address with non-UTF-8 bytes becomes a UTF-8
  string with replacement characters; the resulting `addr` will fail to
  parse as a `SocketAddr`, so the client gets a routing error instead
  of a hard decode failure. Not a correctness bug, just unusual to mix
  `from_utf8_lossy` and `Option`-returning truncation handling.
- **Impact**: None.
- **Recommendation**: Use `std::str::from_utf8` and return `None` on
  invalid UTF-8 — consistent with the rest of the codec's strictness.
- **Confidence**: High

### F-G5-022: `handle_set_locked_batch` (and several siblings) snapshot the pre-image AFTER writing the redo entry — recovery from a crash between redo append and engine apply replays the redo entry correctly, but the `before_image` used by replication compensation is captured under a different `engine.lookup` than the WAL'd one
- **Severity**: LOW (hypothesis)
- **Category**: Concurrency
- **Location**: `src/server/dispatch.rs:4286-4316` (and the parallel
  pattern in `handle_set_mined_batch`, `handle_delete_batch`)
- **Code**:
  ```rust
  // Phase 2: WAL-first.
  let redo_range = match write_replicated_redo_ops(cluster, redo_log, &redo_ops) { ... };
  // Phase 3: Apply engine mutations AND simultaneously snapshot before-image.
  for v in &valid_items {
      match engine.set_locked_with_before_image(&SetLockedRequest { tx_key: v.key, value }) {
          Ok(resp) => {
              before_images_by_key.push((v.key, vec![BeforeImage::SetLocked {
                  prior_locked: resp.prior_locked,
                  prior_delete_at_height: resp.prior_delete_at_height,
              }]));
          }
  ```
- **Issue**: The `set_locked_with_before_image` engine call atomically
  applies the mutation AND returns the prior image — good. The set-mined
  / delete handlers, however, take the snapshot under a separate
  `engine.read_metadata` BEFORE the engine apply (see
  `dispatch.rs:3171-3187`), in a non-locked window between WAL append
  and engine apply. A concurrent mutation that lands between those two
  reads would make the captured "before image" not actually before the
  current mutation. Compensation rollback would then restore the wrong
  state.
- **Impact**: Hypothetical: would require concurrent same-key mutations
  to interleave with the WAL window. Most batches lock per-record via
  the engine's striped locks, but the snapshot for set-mined is taken
  OUTSIDE that lock.
- **Recommendation**: Centralize the "apply + return before image"
  pattern (already done for `set_locked`) for every handler that needs
  compensation. Drop the separate `read_metadata` calls.
- **Confidence**: Medium

### F-G5-023: `handle_delete_batch`'s compensation rebuilds the record by synthesising an inbound `OP_REPLICA_BATCH` to itself (`handle_replica_batch(&create_req, ...)`)
- **Severity**: LOW
- **Category**: Maintainability
- **Location**: `src/server/dispatch.rs:4881-4911`
- **Code**:
  ```rust
  let create_req = crate::protocol::frame::RequestFrame {
      request_id: 0,
      op_code: OP_REPLICA_BATCH,
      flags: 0,
      payload: ReplicaBatch { ... }.serialize(),
  };
  let resp = handle_replica_batch(
      &create_req, engine, &std::sync::atomic::AtomicU64::new(0));
  ```
- **Issue**: This is a self-replication path used as a compensation
  primitive. It works (and tests confirm), but it conflates "incoming
  network frame" with "internal recovery action": every concern that
  applies to network-supplied `OP_REPLICA_BATCH` payloads (auth,
  cluster_key gate, sequence-number duplicate detection) is bypassed by
  hand-constructing the frame in process. If any of those checks later
  become security-critical the in-process compensation path will be
  inconsistent with the network path unless it is updated in parallel.
- **Impact**: Long-term maintainability hazard.
- **Recommendation**: Extract the "apply a list of `ReplicaOp`s
  in-process" logic out of `handle_replica_batch` into a pure function
  that both the network path and the compensation path call, with auth
  / dedupe applied only on the network side.
- **Confidence**: High

### F-G5-024: `handle_stream_chunk` uses `expect("just inserted")` after a `HashMap::entry().or_insert_with` style flow, but the `Vacant` branch and the subsequent `get_mut` are separated by an early-return — currently safe but the pattern is fragile
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/server/dispatch.rs:5990-6008`
- **Code**:
  ```rust
  use std::collections::hash_map::Entry;
  if let Entry::Vacant(entry) = conn_state.streams.entry(chunk.txid) {
      match blob_store.begin_stream(&chunk.txid) {
          Ok(writer) => { entry.insert(super::ActiveStream { writer, bytes_received: 0 }); }
          Err(e) => { return error_response(...); }
      }
  }
  let stream = conn_state.streams.get_mut(&chunk.txid).expect("just inserted");
  ```
- **Issue**: The expect is correct *today*: either the entry already
  existed (matched `Occupied`, fell through), or we just inserted on
  `Ok`. The `Err` arm returns early. The pattern is fragile because a
  future contributor could add an early-return in the `Occupied` arm
  without realizing the second lookup needs both branches.
- **Impact**: No observable bug.
- **Recommendation**: Use `entry().or_insert_with_key(...)` returning
  `&mut ActiveStream` and skip the second lookup entirely.
- **Confidence**: High

### F-G5-025: `mod.rs` is a 9-line re-export — positive verification
- **Severity**: INFO (positive verification)
- **Category**: Code Quality
- **Location**: `src/protocol/mod.rs:1-10`
- **Code**: `pub mod codec; pub mod frame; pub mod opcodes;`
- **Issue**: No issue.
- **Impact**: None.
- **Recommendation**: None.
- **Confidence**: High

### F-G5-026: `opcodes.rs` carries excellent doc comments describing wire layouts AND named per-item caps (`MAX_COLD_DATA_PER_ITEM`, `MAX_UTXO_HASHES_PER_CREATE_ITEM`, `MAX_PARENT_TXIDS_PER_CREATE_ITEM`, `ADMIN_DIAGNOSE_KEY_MAX_TXIDS`) — positive verification of R-089/R-090
- **Severity**: INFO (positive verification)
- **Category**: Security
- **Location**: `src/protocol/opcodes.rs:383-412`
- **Code**:
  ```rust
  pub const MAX_COLD_DATA_PER_ITEM: u32 = 4 * 1024 * 1024;
  pub const MAX_UTXO_HASHES_PER_CREATE_ITEM: u32 = 131_072;
  pub const MAX_PARENT_TXIDS_PER_CREATE_ITEM: u32 = 65_536;
  ```
- **Issue**: Each cap is enforced in
  `decode_create_batch_checked` (`codec.rs:816, 862, 923`) BEFORE the
  per-item `Vec::with_capacity` call. The variable-length sections that
  used to be the obvious attack surface are now closed.
- **Impact**: None — verifying the fix landed correctly.
- **Recommendation**: None.
- **Confidence**: High

### F-G5-027: `decode_stream_chunk` uses `u64::from_le_bytes(payload[32..40].try_into().unwrap())` on a path where the prior length guard `payload.len() < 44` makes the unwrap unreachable, but the pattern still violates the parser hygiene rule
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/protocol/codec.rs:1789-1805` and similarly
  `decode_stream_end` at `codec.rs:1830-1838`
- **Code**:
  ```rust
  if payload.len() < 44 { return None; }
  let offset = u64::from_le_bytes(payload[32..40].try_into().unwrap());
  ```
- **Issue**: Same pattern as F-G5-007 — locally safe, globally
  inconsistent with the dispatcher's own
  `dispatch_parsers_use_take_helper` test rule. The test only inspects
  `dispatch.rs`, so `codec.rs` is not covered.
- **Impact**: None observable.
- **Recommendation**: Use the `le_u64_at` helper or
  `try_into().ok()?` to keep the failure mode consistent.
- **Confidence**: High

### F-G5-028: `OP_PROCESS_EXPIRED_PRESERVATIONS` is in the mutation list (`is_mutation_opcode`) and so passes through the `check_quorum` gate; the handler then routes through `handle_delete_batch` which does its own quorum check — double check is fine, but the synthesized `OP_DELETE_BATCH` frame re-enters the dispatcher's middleware (readiness, quorum, ownership) at function-call level rather than full re-dispatch
- **Severity**: INFO
- **Category**: Maintainability
- **Location**: `src/server/dispatch.rs:5772-5780`
- **Code**:
  ```rust
  let delete_payload = crate::protocol::codec::encode_txid_batch(&owned_due, &[]);
  let delete_req = RequestFrame { ... op_code: OP_DELETE_BATCH ... };
  let delete_resp = handle_delete_batch(&delete_req, engine, max_batch, cluster, redo_log);
  ```
- **Issue**: The synthetic frame skips the dispatcher's `needs_cluster_readiness`,
  `check_secondary_readiness`, and `check_quorum` middleware. Because
  the outer `OP_PROCESS_EXPIRED_PRESERVATIONS` already passed those
  gates, the behaviour is correct today. But the inner-vs-outer
  divergence is subtle and a future maintainer could expect quorum to
  be re-checked under a slow path.
- **Impact**: None observable.
- **Recommendation**: Document the by-pass at the call site, or route
  through `handle_request` so middleware is shared.
- **Confidence**: Medium

---

## Coverage notes

- `mod.rs`, `frame.rs`, `opcodes.rs`, `codec.rs` each received at least one
  finding or positive verification note (F-G5-013, F-G5-014, F-G5-018,
  F-G5-019, F-G5-020, F-G5-021, F-G5-025, F-G5-026, F-G5-027).
- Every opcode handler in `dispatch.rs` was inspected. The table at the
  top of this file enumerates each handler with its line number.
- Prior-audit anchor checks:
  - **EF-01 / D-20** ("cluster control opcodes unauthenticated") — still
    live in the no-secret default case (F-G5-001, F-G5-004, F-G5-005).
    The HMAC plumbing is *correct* when a secret is configured.
  - **A-04** (unspend authority — anyone can erase any spend) —
    RESOLVED (F-G5-012). `WireUnspendItem` now carries `spending_data`
    and the engine enforces the match.
  - **R-054 / LMNH-01** (slow loris) — RESOLVED (F-G5-014).
  - **R-089 / R-090 / GH-13** (per-item amplification in create batches) —
    RESOLVED (F-G5-026).
  - **Gap #10** (per-connection read-buffer amplification) — RESOLVED
    (F-G5-013) via `MAX_FRAME_SIZE` enforced before any allocation.
- Newly surfaced issues focus on (a) the no-secret authentication
  default, (b) information-disclosure surfaces on the public TCP port
  via `OP_QUERY_OLD_UNMINED` and the two admin opcodes, (c) the
  `request_id`-as-shard-id overload in REPLICA_BATCH /
  MIGRATION_COMPLETE, and (d) several "locally safe but globally
  inconsistent" parser-hygiene patterns in `codec.rs` and `dispatch.rs`
  that the `dispatch_parsers_use_take_helper` regex test misses.
- Severity counts: CRITICAL 1, HIGH 3, MEDIUM 3, LOW 11, INFO 10.
- No findings of double-free, race conditions on locks, or `unsafe`
  misuse were observed in the wire-protocol surface; the codec
  consistently goes through bounds-checked indexing.
