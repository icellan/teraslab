# Group G7 — Replication findings

Scope:

- `src/replication/manager.rs` (2382 LOC)
- `src/replication/receiver.rs` (3959 LOC)
- `src/replication/protocol.rs` (1631 LOC)
- `src/replication/tcp_transport.rs` (808 LOC)
- `src/replication/durable.rs` (1342 LOC)
- `src/replication/batching.rs` (120 LOC)
- `src/replication/mod.rs` (12 LOC)

Cross-cutting context: production `OP_REPLICA_BATCH` traffic enters the same
TCP listener as client traffic (`src/server/mod.rs::handle_connection_inner`),
which HMAC-authenticates inter-node opcodes via
`is_inter_node_auth_opcode(OP_REPLICA_BATCH) == true` **only when**
`opts.cluster_secret.is_some()`. The standalone `ReplicationReceiver` struct
in `receiver.rs` has its own auth path but is not instantiated anywhere in
production (`grep` shows zero non-doc references outside the module).

---

### F-G7-001: Replica frames unauthenticated when `cluster_secret` is `None`
- **Severity**: HIGH
- **Category**: Security
- **Location**: `src/server/mod.rs:422-450` (gate) and `src/replication/tcp_transport.rs:204-226` (sender) / `src/replication/receiver.rs:307-328` (alt receiver path)
- **Code**:
  ```rust
  let auth_required = peek_request_op_code(&frame_bytes)
      .map(is_inter_node_auth_opcode)
      .unwrap_or(false)
      && opts.cluster_secret.is_some();  // <- gates auth on configuration
  ```
- **Issue**: Recent commits (R-034/R-035) added replica WAL durability but did
  not change the authentication contract. If a cluster is deployed without
  `cluster_secret` configured (a soft default — `ConnectionOptions::cluster_secret:
  None` is the test/default value), any TCP peer can submit
  `OP_REPLICA_BATCH` frames and mutate state. There is no TLS at any layer
  (only `rustls-tls` for the outbound `reqwest` HTTP client; no `rustls` in
  the replication path — confirmed by `grep -rn rustls src/replication/`).
  D-20 documented "inter-node TCP unauthenticated except SWIM"; R-034/R-035
  did not address this.
- **Impact**: Replication is fail-open on misconfiguration. A node started
  without `--cluster-secret` will silently accept unauthenticated batches.
  Cluster traffic crossing any untrusted network segment is in cleartext
  regardless of secret.
- **Recommendation**: (a) Make `cluster_secret` required when `cluster.is_some()`
  and refuse to start otherwise. (b) Add an integration test that asserts
  unsigned `OP_REPLICA_BATCH` is rejected when `cluster.is_some()` even
  with `cluster_secret == None`. (c) Document an explicit TLS/mTLS roadmap;
  HMAC over plaintext is integrity-only, not confidentiality.
- **Confidence**: High

---

### F-G7-002: `recv_ack` does not validate `response.request_id` against the outgoing one
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/replication/tcp_transport.rs:287-302`
- **Code**:
  ```rust
  let (resp, _) = ResponseFrame::decode(&frame)
      .map_err(|e| ReplicationError::Transport(format!("decode response frame: {e}")))?;

  if resp.status != STATUS_OK { ... }
  ReplicaAck::deserialize(&resp.payload)
      .map_err(|e| ReplicationError::Transport(format!("deserialize ack: {e}")))
  ```
- **Issue**: `request_id` is incremented inside `send_batch` (line 205) but
  the matching response's `request_id` is never compared against
  `self.request_id`. If a connection is ever reused with a buffered/stale
  ACK left in the kernel receive queue (e.g. a timed-out recv_ack on
  request N, then a new send for request N+1 reuses the cached transport
  in `send_replica_batch_to`'s slot — which only drops the connection on
  `send_batch` failure, NOT `recv_ack` failure), the next caller would
  read the wrong ACK as if it were for their request.
- **Impact**: Subtle: on `recv_ack` timeout, `send_replica_batch_to`
  (dispatch.rs:2382) does NOT re-cache the transport — so the next call
  reconnects. So this is currently masked. But the invariant is fragile:
  any future code path that re-caches a transport after a partial ACK
  read would silently mis-attribute ACKs to the wrong sequence.
- **Recommendation**: After `ResponseFrame::decode`, assert
  `resp.request_id == self.request_id`. Otherwise return
  `ReplicationError::Transport("ack request_id mismatch")` and force
  reconnect.
- **Confidence**: High

---

### F-G7-003: 16 MiB+ per-connection buffer reachable pre-auth
- **Severity**: MEDIUM
- **Category**: Security / Resource exhaustion
- **Location**: `src/replication/receiver.rs:276-293`
- **Code**:
  ```rust
  let max_wire_frame_size = MAX_FRAME_SIZE
      + auth_secret.as_ref()
          .map(|_| crate::cluster::auth::SIGNED_SUFFIX_LEN as u32)
          .unwrap_or(0);
  if total_length > max_wire_frame_size { return; }
  let frame_len = total_length as usize;
  if body.len() < frame_len { body.resize(frame_len, 0); }
  ```
- **Issue**: An unauthenticated peer can declare a 16 MiB frame length
  and force a 16 MiB allocation BEFORE the HMAC verification step at
  line 307. Each new TCP connection spawns its own handler thread
  (line 176) with its own `body` Vec; there is no per-process aggregate
  cap on inflight bytes here (unlike the main `server/mod.rs` path,
  which gates with `inflight_request_bytes`).
- **Impact**: Trivial DoS against the standalone `ReplicationReceiver`
  listener — 100 connections × 16 MiB = 1.6 GiB. Note: this struct is
  not instantiated in production today (see Coverage notes), but it is
  `pub` and reachable by external embedders.
- **Recommendation**: Either (a) gate the allocation on a global
  inflight-bytes limiter mirroring `server/mod.rs`, or (b) delete the
  `ReplicationReceiver::start()` listener entirely since production uses
  `server::handle_connection_inner` instead. Option (b) is cheaper and
  reduces attack surface.
- **Confidence**: High

---

### F-G7-004: `intent_tracker.commit()` deferral leaves stale ranges across crashes
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/replication/durable.rs:323-350`, `91-100`
- **Code**:
  ```rust
  inner.commit_dirty = true;
  inner.dirty_commit_count = inner.dirty_commit_count.saturating_add(1);
  let time_due = inner.last_flush.elapsed().as_millis() >= INTENT_COMMIT_FLUSH_INTERVAL_MS;
  let count_due = inner.dirty_commit_count >= INTENT_COMMIT_FLUSH_DIRTY_COUNT_THRESHOLD;
  if time_due || count_due { self.flush_locked(&mut inner)?; }
  ```
- **Issue**: `begin()` is immediately durable (line 315) — good. But
  `commit()` is coalesced (up to 1 s OR 100 commits between flushes).
  After a crash, every uncommitted range will be replayed as still-pending
  replication. Author comment at the test (`replication_intent_tracker_persistence_round_trip`)
  acknowledges: *"commit persistence is intentionally coalesced; stale
  ranges cause idempotent re-replication after a crash"*. That is correct
  if and only if the replica's `ReplicaAppliedTracker` is consulted on
  replay — which it is in `handle_replica_batch_with_tracker` (receiver.rs:585).
- **Impact**: Correct under current design — but the contract is brittle:
  a future change to skip the dedup tracker (e.g. for migration batches,
  which already bypass it — `is_migration` branch at receiver.rs:580-585)
  would silently re-apply ops. Recovery-time replay throughput also
  pays a cost proportional to uncommitted-range count.
- **Recommendation**: Add a recovery-time assertion that for every pending
  intent range, the receiver's dedup tracker is consulted before
  re-application; reject batches that bypass dedup at recovery time.
- **Confidence**: Medium

---

### F-G7-005: Migration batch dedup bypass is silent on replay collision
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/replication/receiver.rs:580-666`
- **Code**:
  ```rust
  let is_migration = request.flags & FLAG_MIGRATION_BATCH != 0;

  // Whole batch already applied — ACK with the existing high-water mark
  if !is_migration && through <= already_applied { ... return ACK Ok; }

  let skip_count = if is_migration { 0 } else if batch.first_sequence <= already_applied { ... };
  // ...
  if !is_migration {
      applied.set(&effective_stream_key, through);
      ...
  }
  ```
- **Issue**: Migration batches always start at `first_sequence: 0` and
  bypass both the "skip already-applied" check and the high-water-mark
  update. The author rationale (pattern A comment) is that migrations
  are coordinated out-of-band and one-shot per shard. But the bypass is
  unconditional on `FLAG_MIGRATION_BATCH` — a buggy or hostile sender
  that sets the flag bit can replay arbitrary mutations through the
  dedup-bypass path. The cluster-key gate (line 509) is the only
  remaining defense, but it accepts `batch.cluster_key == 0` from
  V1-compat senders and accepts any value when `local_cluster_key == 0`
  (line 491-506 comment).
- **Impact**: A misaligned migration handshake (e.g. a `OP_MIGRATION_COMPLETE`
  arriving without a preceding `cluster.mark_inbound_active`) could
  unconditionally re-apply ops the replica already had. The pre-apply
  generation guard at `apply_op` (line 816-826) is the last line of
  defense; it works for mutation ops but `Create`/`Delete`/`PruneSlot`
  have no generation field and rely on idempotency in their match arms.
- **Recommendation**: Either (a) introduce a separate
  `MigrationAppliedTracker` keyed on `(shard, manifest_id)` so migration
  batches still have dedup, or (b) require the cluster_key gate to be
  strict (no zero-wildcard) for migration batches in clustered mode.
- **Confidence**: Medium

---

### F-G7-006: `apply_op` Spend "graceful skip on tx-not-found" can mask replication drift
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/replication/receiver.rs:836-865`
- **Code**:
  ```rust
  let hash = match engine.read_slot(tx_key, *offset) {
      Ok(slot) => slot.hash,
      Err(_) => {
          // TX or slot not found — skip gracefully
          return Ok(());
      }
  };
  ```
- **Issue**: If a Spend arrives on a replica that never received the
  preceding Create (e.g. the Create batch was lost across a reconnect
  gap before AckTracker persisted, or the master-side intent didn't
  cover that range), the replica silently ACKs without applying. Same
  pattern in Unspend, Freeze, Unfreeze, Reassign, PruneSlot.
- **Impact**: The replica's `spent_utxos` counter stays at the wrong
  value, but no error is surfaced. A subsequent reads against this
  replica return divergent metadata. Catch-up from the redo log will
  not re-deliver missing Creates either, because the master tracks
  per-replica `last_acked` and the Spend was acked.
- **Recommendation**: Treat TxNotFound for non-Create non-Delete ops
  as a hard batch error when the receiver is in steady-state mode
  (cluster_key non-zero); skip only during recovery/replay or migration.
  At minimum, increment a `replica_apply_skipped_missing_tx` metric so
  operators can detect divergence.
- **Confidence**: Medium

---

### F-G7-007: `replicate_batch` writes the same `next_sequence` cursor even on full failure
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/replication/manager.rs:359-360, 481-506`
- **Code**:
  ```rust
  let batch = ReplicaBatch {
      first_sequence: self.next_sequence,
      ops: ops.to_vec(), ...
  };
  self.next_sequence += ops.len() as u64;  // <- always advances
  // ... fan-out ...
  match self.config.ack_policy { AckPolicy::WriteAll => { if successes == live_count { Ok(()) } else { Err(...) } } ... }
  ```
- **Issue**: `next_sequence` advances before the fan-out result is
  reconciled. If all replicas fail, the manager's view of the next
  outbound sequence is one batch ahead of any replica's `last_acked`,
  which is the intent — but a caller that retries the SAME ops must
  decide whether to re-use the old `first_sequence` (idempotent on the
  replica via the dedup tracker) or pick the new one. The current code
  picks the new one, which means the replica dedup tracker sees both
  sequence ranges as legitimate — no skip — and the master's view of
  "what's in the redo log at seq N" diverges from "what was sent at
  seq N".
- **Impact**: Combined with the master-side `ReplicationIntentTracker`,
  this is recoverable on restart. But during steady-state retry it
  bloats the replica's sequence space and the on-disk applied-tracker
  file. There is no test that drives a retry-after-all-replicas-failed
  to assert sequence-space contiguity.
- **Recommendation**: Either (a) treat retried ops as a new sequence
  range (current behavior) and document the contract, or (b) reset
  `next_sequence` on full failure. Option (a) is simpler and matches
  the durable-log invariant; just add a regression test that the master
  records both ranges as separate intents.
- **Confidence**: Medium

---

### F-G7-008: `AckTracker::flush_locked` swallows write errors with `tracing::warn`
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/replication/durable.rs:165-173`
- **Code**:
  ```rust
  fn flush_locked(&self, inner: &mut AckTrackerInner) {
      if let Err(e) = Self::write_to_disk(&self.path, &inner.last_acked) {
          tracing::warn!(err = %e, "ack_tracker: flush failed");
          return;
      }
      inner.dirty = false;
      inner.dirty_count = 0;
      inner.last_flush = Instant::now();
  }
  ```
- **Issue**: On disk-full, EIO, or permission errors, the in-memory
  state retains `dirty = true` so the next `record_ack` will retry —
  good. But the caller (`record_ack`, `flush()`) never observes the
  failure. A monitoring system has no way to know AckTracker is
  silently failing to persist except by tailing logs.
- **Impact**: Operationally invisible failure. Combined with the
  count-threshold flush (R-067), a stuck flush + sustained ACK
  rate leaves the at-risk window unbounded until log scraping
  catches up.
- **Recommendation**: Add a `Counter` for `ack_tracker_flush_failures`
  in the replication metrics subsystem (`metrics::replication_metrics()`)
  and bump it inside `flush_locked` on the error branch.
- **Confidence**: High

---

### F-G7-009: `replicate_batch` parallel fan-out: panic in scoped worker becomes generic error
- **Severity**: LOW
- **Category**: Observability
- **Location**: `src/replication/manager.rs:423-432`
- **Code**:
  ```rust
  handles.into_iter()
      .map(|h| {
          h.join()
              .unwrap_or(Outcome::TransportErr(ReplicationError::Transport(
                  "replica worker panicked".into(),
              )))
      })
      .collect()
  ```
- **Issue**: Worker thread panics are converted to a `TransportErr`
  with a constant string — the panic's actual payload (and location)
  is discarded. The replica is marked Down (line 469) and the master
  proceeds without diagnostics for the underlying bug.
- **Impact**: Hides correctness bugs in `send_batch` / `recv_ack`
  (e.g. a panic in a future codec change) behind a benign-looking
  transport error. A subsequent reconnection succeeds and the issue
  reappears intermittently.
- **Recommendation**: Capture the panic payload via
  `std::panic::AssertUnwindSafe` + `catch_unwind`, downcast to
  `&str` / `String`, and include in the error message. Bump a
  `replica_worker_panics_total` counter.
- **Confidence**: High

---

### F-G7-010: `ReplicaBatchAccumulator::push` ignores `max_batch_size`
- **Severity**: LOW
- **Category**: Resource bounding
- **Location**: `src/replication/batching.rs:27-50`
- **Code**:
  ```rust
  pub fn push(&mut self, op: ReplicaOp) {
      self.ops.push(op);  // <- no bound check
  }
  pub fn should_flush(&self) -> bool {
      self.ops.len() >= self.max_batch_size
  }
  ```
- **Issue**: `push` does not enforce the threshold — callers are
  expected to consult `should_flush()` between pushes. If a caller
  ever forgets to flush, the Vec grows unbounded. There are no
  call sites in the codebase that drive a write loop without
  flushing, but the type's API contract is "trust the caller."
- **Impact**: Defensive; no current path exercises it. But the type's
  name and signature suggest a hard limit that doesn't exist.
- **Recommendation**: Either rename to `max_batch_size_hint`, or make
  `push` return a `bool` (true = "you should drain now") and let it
  also reject pushes past 2× the threshold as a hard cap.
- **Confidence**: Medium

---

### F-G7-011: Catch-up `chunk_seq` cursor reset bug — first chunk uses `from_seq`, later chunks compounded
- **Severity**: LOW
- **Category**: Correctness (already fixed, but contract is fragile)
- **Location**: `src/replication/manager.rs:605-643`
- **Code**:
  ```rust
  let mut chunk_seq = from_seq;
  for chunk in ops.chunks(batch_size) {
      let batch = ReplicaBatch {
          first_sequence: chunk_seq, ...
      };
      ...
      Ok(ReplicaAck::Ok { through_sequence }) => {
          let expected_through = batch.last_sequence();
          if through_sequence != expected_through {
              sender.state = ReplicaState::Down;
              ok = false; break;
          }
          sender.last_acked = through_sequence;
          chunk_seq = through_sequence.saturating_add(1);
      }
  ```
- **Issue**: The post-fix correctly advances `chunk_seq` per chunk.
  But the strict-equality check `through_sequence != expected_through`
  closes the connection if the replica's dedup tracker had already
  applied the chunk (it would ACK with a HIGHER through-sequence).
  In particular, if a catch-up retry overlaps with a normal-replication
  resume, the replica's `already_applied` is ahead of the chunk's
  declared last_sequence, and the receiver ACKs with the *existing*
  high-water mark (receiver.rs:585-594) which is `>= expected_through`,
  not `==`. The master then marks the replica Down spuriously.
- **Impact**: Spurious replica-Down transitions during catch-up after
  a partial overlap. Recovers on next `check_reconnected`, but adds
  flap.
- **Recommendation**: Change the equality check to
  `through_sequence < expected_through` (replica behind => failure;
  ahead => fine).
- **Confidence**: Medium

---

### F-G7-012: V1 batch decoder still wired despite "never produced"
- **Severity**: INFO
- **Category**: Maintainability
- **Location**: `src/replication/protocol.rs:877-924`
- **Code**:
  ```rust
  pub fn deserialize(data: &[u8]) -> Result<Self> {
      need(data, 1)?;
      match data[0] {
          BATCH_PROTOCOL_V2 => Self::decode_v2(data),
          BATCH_PROTOCOL_V1 => Self::decode_v1(data),
          other => Err(ProtocolError::UnknownVersion(other)),
      }
  }
  ```
- **Issue**: V1 frames decode with `cluster_key = 0`, which the
  receiver gate (receiver.rs:509) explicitly accepts as "V1-compat
  sender; accept." So a peer that intentionally sends V1 bypasses
  the stale-epoch gate. The doc comment says senders never produce
  V1; the receiver SHOULD reject V1 in clustered mode for the same
  reason it rejects mismatched epochs.
- **Impact**: Security defense-in-depth gap. Not exploitable today
  given the HMAC layer above (so the attacker would need the
  cluster_secret), but it dilutes the cluster_key invariant.
- **Recommendation**: When `local_cluster_key != 0`, reject V1 frames
  unconditionally. Track in a metric.
- **Confidence**: Medium

---

### F-G7-013: Per-thread receiver thread-local tracker leaks one tracker per worker
- **Severity**: INFO
- **Category**: Resource
- **Location**: `src/replication/receiver.rs:412-422`
- **Code**:
  ```rust
  thread_local! {
      static IN_MEMORY_TRACKER: std::cell::RefCell<Option<Arc<ReplicaAppliedTracker>>> =
          const { std::cell::RefCell::new(None) };
  }
  let tracker = IN_MEMORY_TRACKER.with(|slot| { ... });
  ```
- **Issue**: Used only by `handle_replica_batch_with_cluster_key` test
  fallback path (line 400-431). Comments say "Per-thread isolation is
  required here because cargo runs unit tests in parallel." But the
  trackers persist for the lifetime of each worker thread and are never
  drained; in a busy server with high thread churn this leaks a tracker
  per thread until the thread exits.
- **Impact**: Trivial in practice (test path); production goes through
  the named tracker via `init_replica_applied_tracker`. Worth a TODO
  to delete this fallback once tests migrate.
- **Recommendation**: Document or delete. If kept, add a `Drop` impl
  on a wrapper that clears the tracker when the thread exits — or
  switch the test path to explicit tracker injection.
- **Confidence**: High

---

### F-G7-014: `tcp_transport::is_connected` uses `take_error` which is misleading on macOS
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/replication/tcp_transport.rs:305-316`
- **Code**:
  ```rust
  fn is_connected(&self) -> bool {
      self.stream.take_error()
          .map(|e| e.is_none())
          .unwrap_or(false)
  }
  ```
- **Issue**: `take_error()` consumes the pending socket error. If
  multiple callers race on `is_connected`, the second loses the error
  signal. Also, on macOS `SO_ERROR` only reports asynchronous errors
  (e.g. ECONNRESET reported via SIGPIPE timing), not graceful peer
  close — so a `FIN`'d connection still reports "is_connected" until
  the next read.
- **Impact**: `check_reconnected` (manager.rs:536-544) uses this to
  decide whether to transition a Down sender to CatchingUp. False
  positives mean the next batch send fails synchronously and the
  sender oscillates. The author comment acknowledges "next send/recv
  remains the authoritative liveness check" — so contract is intact.
- **Recommendation**: Rename to `has_no_pending_error` to reflect
  semantics, or document that this is best-effort only.
- **Confidence**: High

---

### F-G7-015: Replay order under reconnect — replica relies on receiver dedup, not master ordering
- **Severity**: INFO (positive verification)
- **Category**: Correctness
- **Location**: `src/replication/receiver.rs:601-631` (skip_count logic)
- **Code**:
  ```rust
  let skip_count = if is_migration { 0 }
      else if batch.first_sequence <= already_applied {
          (already_applied + 1 - batch.first_sequence) as usize
      } else { 0 };
  let mut seq = batch.first_sequence + skip_count as u64;
  for op in batch.ops.iter().skip(skip_count) { ... }
  ```
- **Issue**: This is correct. The receiver's `applied.get()` is the
  ground truth; any batch with `first_sequence <= already_applied`
  has its prefix skipped. If the replica reconnects mid-batch and
  the master retries from `last_acked + 1`, the receiver re-applies
  only the suffix — idempotent for the prefix.
- **Recommendation**: Add a regression test that intentionally sends
  the same batch twice with a stale connection in the middle and
  asserts no double-apply.
- **Confidence**: High (this is a verification note, not a defect)

---

### F-G7-016: R-034/R-035 redo write happens AFTER engine apply — crash window remains
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/replication/receiver.rs:1292-1304`, `1540-1561`
- **Code**:
  ```rust
  // R-034 (BC-34): write a local redo entry so the replica can replay...
  // The entry captures the POST-apply state read back from the device, matching
  // the discipline the master uses on its own write path. Failure to journal
  // the entry is a hard batch-level error...
  if let Some(redo_op) = build_post_apply_redo_op(engine, op)? {
      write_replica_redo_entry(engine, &redo_op)?;
  }
  ```
- **Issue**: The author comment is candid: *"ordering here is 'apply,
  fsync data, then journal' instead of the master's 'journal, then apply,
  then fsync data'. Both orderings are correct because all replica apply
  paths are idempotent and the redo replay guards check the device state
  before re-writing."*
  This is correct **only if** the engine's `apply_op` flushes its data
  pages BEFORE returning success. If `engine.spend()` returns Ok but the
  data write is still in the OS page cache, a crash between apply success
  and redo append leaves the replica with a mutated record but no redo
  entry — and on recovery the replica's state is one op ahead of its
  log. The R-035 metadata-write `write_metadata` call uses the same
  device (`engine.device()`) but `crate::io::write_metadata` does NOT
  necessarily fsync (`io::write_metadata` writes via `write_at`).
- **Impact**: Recovery is supposed to be idempotent — but if the post-
  apply redo entry is what advances the replica's recovery checkpoint,
  losing it means the master's catch-up logic believes the replica is
  at sequence N+1 (since the master got the ACK after the redo write,
  but here the redo write failed AFTER the ACK was implicitly committed
  by the apply). Actually: the redo write returning `Err` aborts the
  batch BEFORE the ACK is sent (the propagation chain: `apply_op` →
  `?` → caller's loop in `handle_replica_batch_with_tracker:618-631`
  → returns `STATUS_ERROR`). So this is safe under clean error paths.
  But a *crash* (not error return) between apply and redo append still
  has the divergence window.
- **Recommendation**: Reverse the order — append the redo entry to a
  pending buffer BEFORE calling `engine.apply_op`, then flush the redo
  log AFTER apply succeeds. Or document that the engine guarantees
  durability of every individual op before returning Ok.
- **Confidence**: Medium

---

### F-G7-017: `MAX_ACK_FRAME_SIZE = 1024` may be tight under HMAC + error messages
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/replication/tcp_transport.rs:22, 248-260`
- **Code**:
  ```rust
  const MAX_ACK_FRAME_SIZE: usize = 1024;
  ...
  let max_ack_frame_size = MAX_ACK_FRAME_SIZE
      + self.auth_secret.as_ref()
          .map(|_| crate::cluster::auth::SIGNED_SUFFIX_LEN)
          .unwrap_or(0);
  ```
- **Issue**: `ReplicaAck::Error { failed_sequence, message }`'s message
  is a `String` of arbitrary length. With HMAC suffix = 40 bytes, the
  budget for message bytes is ~950 after subtracting the ResponseFrame
  header (request_id(8) + status(2) + payload_len(4) = 14) and Ack
  variant tag(1) + failed_sequence(8) + msg_len(4) = ~13 → ~975 bytes
  of message text. `format!("flush applied tracker: {e}")` and similar
  diagnostic strings can exceed this if `e` contains a long path or
  stack trace.
- **Impact**: The master rejects the entire response as oversize and
  loses the diagnostic. The replica's send-side does not truncate.
- **Recommendation**: Truncate `ReplicaAck::Error::message` to ~512
  bytes at serialize time. Or raise `MAX_ACK_FRAME_SIZE` to 4 KiB.
- **Confidence**: High

---

### F-G7-018: `ReplicationManager::replicate_batch` blocks on slowest replica with WriteAll
- **Severity**: INFO (positive verification)
- **Category**: Performance
- **Location**: `src/replication/manager.rs:391-432`
- **Code**:
  ```rust
  let outcomes: Vec<Outcome> = std::thread::scope(|s| {
      let handles: Vec<_> = self.senders.iter_mut().enumerate()
          .map(|(idx, sender)| { s.spawn(move || { ... }) }).collect();
      handles.into_iter().map(|h| h.join().unwrap_or(...)).collect()
  });
  ```
- **Issue**: `WriteAll` correctly requires every replica to ACK; the
  fan-out is parallel, so wall time = slowest replica. `WriteMajority`
  also waits for all workers to finish before reconciling
  (`handles.into_iter().map(h.join)` is serial join in order). A
  faster majority could ACK and return, but the current implementation
  blocks on all joins.
- **Recommendation**: Consider an early-return path for `WriteMajority`
  using `mpsc` channels — abort the slow worker once majority is
  reached. Current behavior is correct, just leaves latency on the
  table.
- **Confidence**: High (verified)

---

### F-G7-019: ReplicaState transitions don't snapshot under lock — racy from `mark_replica_live`
- **Severity**: LOW
- **Category**: Concurrency
- **Location**: `src/replication/manager.rs:294-301, 536-544`
- **Code**:
  ```rust
  pub fn mark_replica_live(&mut self, sender_idx: usize) {
      if let Some(s) = self.senders.get_mut(sender_idx)
          && s.state == ReplicaState::NeedsResync { ... }
  }
  ```
- **Issue**: `ReplicationManager` takes `&mut self` for every state-mutating
  method but it is shared by the coordinator (which can call `mark_replica_live`
  asynchronously) and by the replication hot path (which calls `replicate_batch`).
  There is no per-manager `Mutex` here; if the coordinator holds its own
  lock and the hot path holds a different one, racing access is possible.
- **Impact**: Depends on the calling context. Need to verify the actual
  shared-state discipline in the coordinator. Worst case: a sender
  transitions to `Live` from `NeedsResync` while `replicate_batch` is
  iterating senders.
- **Recommendation**: Either document that callers must serialize, or
  internalize a `Mutex<Senders>` and have all state-mutating methods
  acquire it.
- **Confidence**: Low (would need a coordinator-side read)

---

### F-G7-020: `mod.rs` is purely declarative
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/replication/mod.rs:1-13`
- **Code**:
  ```rust
  pub mod batching;
  pub mod durable;
  pub mod manager;
  pub mod protocol;
  pub mod receiver;
  pub mod tcp_transport;
  ```
- **Verification**: Module list is clean, no dead modules. The doc
  comment correctly summarizes the model. No re-exports — every caller
  reaches into specific submodules. That's fine but verbose; consider
  re-exporting `ReplicationManager`, `ReplicationReceiver`,
  `ReplicaOp`, `ReplicaBatch`, `ReplicaAck` at the crate-replication
  level for ergonomics.

---

## Coverage notes

**Files inspected end-to-end:**

- `manager.rs` (full file, including all tests) — read in three passes
  covering: state machine, fan-out, catch-up, intent emission, tests.
- `receiver.rs` (~50% of production code path covered in detail,
  remainder of file is tests + apply-op branches that mirror engine
  contracts) — read 1-1300 plus targeted samples up through 2200.
  Stopped reading test bodies after structure was clear; ~1800 LOC of
  test scaffolding scanned only.
- `protocol.rs` (header layout, serialize/deserialize for every op
  variant, V1/V2 batch decoders) — full read 1-980.
- `durable.rs` (AckTracker, ReplicationIntentTracker, ReplicaAppliedTracker,
  spawn_lag_monitor) — full read 1-1100. Catch-up runner
  (`run_catchup_for_replica`) inspected at 706-759.
- `tcp_transport.rs` — full read 1-808 (production code + all tests).
- `batching.rs` — full read 1-120.
- `mod.rs` — full read.

**Cross-cuts verified:**

- HMAC framing gate at `src/server/mod.rs:422-450` confirmed
  `is_inter_node_auth_opcode(OP_REPLICA_BATCH) == true` and tested by
  `assert_unsigned_protected_opcode_rejected(OP_REPLICA_BATCH)`.
- `OP_REPLICA_BATCH` dispatch entry at `src/server/dispatch.rs:483-535`
  confirmed to route through `handle_replica_batch_with_tracker` with the
  coordinator's `local_cluster_key`.
- `ReplicationReceiver` struct in `receiver.rs` confirmed unused outside
  the module — no production instantiation found in `src/` or `tests/`.

**Prior audits status:**

- D-20 (inter-node TCP unauthenticated except SWIM) — **partially
  resolved**: HMAC frame layer exists, but auth is conditional on
  `cluster_secret.is_some()` (F-G7-001).
- R-034 (replica WAL durability) — verified in receiver.rs:1292-1304;
  ordering caveat in F-G7-016.
- R-035 (metadata-write error propagation) — verified at
  receiver.rs:1279-1290 and 762-779; errors propagate as hard batch
  failures.
- R-067 (count-threshold flush in AckTracker) — verified in
  durable.rs:84-143 with regression test at line 839; threshold of 100
  is sane (caps at-risk window to ~100 ACKs).

**Not inspected:**

- `cluster/coordinator.rs` interaction with `ReplicationManager` —
  flagged in F-G7-019 as a concurrency concern needing follow-up.
- `metrics.rs::ReplicationMetrics` — referenced but not audited;
  F-G7-008 / F-G7-009 propose additions there.
