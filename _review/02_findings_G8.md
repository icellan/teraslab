# G8 — Cluster control plane findings

Scope: `src/cluster/{coordinator.rs, swim.rs, topology.rs, migration.rs, shards.rs, membership.rs, auth.rs, routing.rs, mod.rs}` plus the topology / migration dispatch handlers in `src/server/{mod.rs, dispatch.rs}` that the prior audits flagged.

Prior audit verification (EF-01, EF-02, R-042, R-052, D-20) is summarised inline; the **Coverage notes** section at the bottom records overall coverage.

---

### F-G8-001: Split-brain merge accepted when one cluster's membership is a strict superset of the other
- **Severity**: CRITICAL
- **Category**: Correctness / Security
- **Location**: `src/cluster/topology.rs:404`
- **Code**:
  ```rust
  fn is_safe_membership_change(committed: &[NodeId], proposed: &[NodeId]) -> bool {
      if committed.is_empty() {
          return true;
      }
      let proposed_has_all_committed = committed.iter().all(|c| proposed.contains(c));
      let committed_has_all_proposed = proposed.iter().all(|p| committed.contains(p));
      // Safe when the change is monotonic: pure superset OR pure subset.
      proposed_has_all_committed || committed_has_all_proposed
  }
  ```
- **Issue**: The split-brain heal defence (R-042) treats *any pure superset* as safe. Consider two independently-bootstrapped clusters that happened to share a `cluster_secret`: side A committed members `{A,B}` and side C committed `{C,D}`. When SWIM gossip leaks and side A’s proposer sees `{A,B,C,D}`, this is a strict superset of A’s committed set — the guard accepts it. C’s proposer makes the same observation and also accepts. Both sides then commit a new term with the merged membership and recompute the shard table over `{A,B,C,D}`, instantly corrupting both shard owners. The hypothesis the design intends to defend against is exactly this scenario (see the doc comment lines 384–399); the code only rejects the narrower "add AND remove" case.
- **Impact**: Two clusters that accidentally share a secret (e.g. one was cloned from the other for a benchmark) merge silently. Every shard whose master changes loses its old master’s in-flight writes; the `is_subset_master` flag is not enough because the partition view is empty for the cross-cluster peers, so `apply_master_election` leaves the round-robin master in place. Likely silent UTXO divergence and double-spend possibility.
- **Recommendation**: Reject any change where committed_members is non-empty and proposed introduces members not previously seen in this cluster's history (track a `committed_voter_ever_seen` set, or require an explicit `cluster_id` field exchanged at JOIN time, as the doc comment hints). At minimum reject *pure additions of unrelated nodes* unless the operator passes `--allow-merge`.
- **Confidence**: High

### F-G8-002: `handle_propose` does not re-validate split-brain heal against the voter's committed state
- **Severity**: HIGH
- **Category**: Correctness / Security
- **Location**: `src/cluster/topology.rs:634-683`
- **Code**:
  ```rust
  pub fn handle_propose(&self, propose: &TopologyTerm) -> TopologyVote {
      let committed = self.committed_term.load(Ordering::Relaxed);
      let voted = self.voted_term.load(Ordering::Relaxed);
      let valid_digest =
          propose.digest == TopologyTerm::compute_digest(propose.term, &propose.members);
      let mut accepted = propose.term > committed && propose.term > voted && valid_digest;
      ...
      if accepted {
          self.voted_term.store(propose.term, Ordering::Relaxed);
      }
      TopologyVote { ... }
  }
  ```
- **Issue**: The split-brain heal defense fires in `on_membership_changed`, `retry_proposal`, and `check_timeout` (all proposer-side), but `handle_propose` (follower-side) accepts any proposal with a higher term and matching digest. A follower whose committed set is `{C,D}` will happily vote yes for a proposer-built `{A,B,C,D}` term coming from node A — the proposer's own guard is bypassed if A had not committed anything yet (`committed_members.is_empty()` returns `true`).
- **Impact**: Defense-in-depth gap. A buggy or malicious node that bypasses its own checks (e.g. via the catch-up re-proposal path that resets `last_membership_change`) can still get followers to vote. Combined with F-G8-001 the merge becomes a single-round process.
- **Recommendation**: Add the same `is_safe_membership_change(&self_committed, &propose.members)` gate inside `handle_propose` before accepting.
- **Confidence**: High

### F-G8-003: SWIM auth has no replay defence within the 5-minute clock-skew window
- **Severity**: HIGH
- **Category**: Security
- **Location**: `src/cluster/auth.rs:114-152`
- **Code**:
  ```rust
  pub fn verify_with_now<'a>(key: &[u8], data: &'a [u8], now_ms: u64) -> io::Result<&'a [u8]> {
      ...
      let skew_ms = now_ms.abs_diff(ts_ms);
      if skew_ms > MAX_CLOCK_SKEW.as_millis() as u64 {
          return Err(io::Error::new(InvalidData, "stale timestamp..."));
      }
      Ok(payload)
  }
  ```
- **Issue**: HMAC verification + ±5-minute timestamp window is the *only* freshness protection. There is no nonce tracking, no per-peer monotonic counter, and no cluster_id binding. Any signed UDP/TCP frame captured by an on-path observer can be replayed verbatim within 5 minutes against any node. Replayed `MSG_PING_REQ` triggers indirect probes; replayed `OP_REPLICA_BATCH` re-applies idempotent ops but consumes resources; replayed `OP_TOPOLOGY_PROPOSE` is rejected only because `voted_term` advanced — but a captured *current-term* proposal during a topology change is acceptable to followers.
- **Impact**: Network-level DoS amplification and possible (but bounded) state corruption via replay. The wider risk is operational: a process snapshot / core dump leaking traffic offers a 5-minute window for full impersonation.
- **Recommendation**: Append a per-sender monotonic sequence number to the signed payload and reject sequences <= last-seen for that NodeId. Persist last-seen across restarts to stop reboot-replay.
- **Confidence**: High

### F-G8-004: `ping_req_forwarding` map grows unboundedly under PING_REQ flood
- **Severity**: HIGH
- **Category**: Concurrency / Resource exhaustion
- **Location**: `src/cluster/swim.rs:210, 641, 659`
- **Code**:
  ```rust
  ping_req_forwarding: HashMap<NodeId, SocketAddr>,
  ...
  if let Some(requester_addr) = self.ping_req_forwarding.remove(&sender_id) { ... }
  ...
  self.ping_req_forwarding.insert(target_id, from_addr);
  ```
- **Issue**: Entries are inserted whenever a `MSG_PING_REQ` arrives requesting an indirect probe of `target_id`. They are only removed when `target_id` eventually ACKs (line 641). If the probed target is dead — or if a peer floods PING_REQ for non-existent NodeIds — entries accumulate forever. There is no TTL, no size cap, and no garbage-collection sweep. A signed peer (or an attacker who replays a signed PING_REQ within 5 min, per F-G8-003) can drive memory growth at ~16 bytes per entry × however many distinct NodeIds it invents.
- **Impact**: Slow memory leak under sustained adversarial gossip; trivial OOM if the attacker is willing to send millions of distinct synthetic NodeIds. Survives across the SWIM lifetime — the only cleanup is process restart.
- **Recommendation**: (a) replace the map with `LruCache<NodeId, (SocketAddr, Instant)>` capped at, say, 1024 entries; (b) sweep entries older than `2 * probe_interval` in the main loop where suspects are expired.
- **Confidence**: High

### F-G8-005: `MigrationManager::Mutex` held across the migration plan-rebuild while every shard is scanned
- **Severity**: HIGH
- **Category**: Concurrency / Performance
- **Location**: `src/cluster/coordinator.rs:1840-1928`
- **Code**:
  ```rust
  let preserved_tasks: std::collections::HashSet<(u16, NodeId, NodeId, bool)>;
  {
      let mut mgr = migration.lock().unwrap();
      ...
      preserved_tasks = mgr.active_migrations().iter().filter(|p| {...}).collect();
      let stale_tasks: Vec<MigrationTask> = mgr.active_migrations().iter().filter(|p|{...}).collect();
      for t in &stale_tasks { mgr.mark_failed(t); }
      mgr.clear_inbound();
      mgr.cleanup_completed();
      ...
  }
  ```
- **Issue**: `activate_topology_with_view` holds the `std::sync::Mutex<MigrationManager>` (the same Mutex that the dispatch hot path acquires for `dual_write_targets_for_shard` / `is_migrating_shard`) for the duration of the plan-rebuild — including a `cleanup_completed()` call that does `O(NUM_SHARDS)` work and a multi-pass scan of `active_migrations()`. Meanwhile, mutation dispatch (`OP_SPEND_BATCH` etc.) needs the same Mutex via `dual_write_targets_for_shard`. Under churn this blocks every write batch for tens of milliseconds.
- **Impact**: Migration storms (typical of a 4-node → 8-node scale-out) freeze incoming writes for the duration of every `activate_topology_*` call. Manifests as sporadic client-side `ERR_MIGRATION_IN_PROGRESS` or timeouts even on shards not involved in any migration.
- **Recommendation**: Split the manager into a `parking_lot::RwLock` over the per-shard state and an event log; or compute the preserved/stale set into a local `Vec` outside the lock and apply the mutations in a short second critical section.
- **Confidence**: Medium (timing-dependent — needs benchmarking under churn).

### F-G8-006: Coordinator catch-up trusts `RoutingInfo::committed_members` despite no quorum proof, then disables itself via stub
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/cluster/coordinator.rs:284-292, 1507-1547`
- **Code**:
  ```rust
  fn committed_topology_from_routing_snapshot(
      _routing: &crate::cluster::routing::RoutingInfo,
  ) -> Option<crate::cluster::topology::TopologyCommit> {
      // Partition maps carry the active assignments and committed member list,
      // but not the voter proof for the advertised term.
      None
  }
  ...
  if routing.shard_table_version > topology_authority.committed_term() {
      if let Some(commit) = committed_topology_from_routing_snapshot(&routing) {
          let _ = topology_authority.handle_commit(&commit);
      } else {
          tracing::debug!(...);
      }
  }
  if install_active_routing_snapshot(&routing, ...) { ... }
  ```
- **Issue**: The function is correctly stubbed `None` because partition maps lack a voter quorum proof. However, `install_active_routing_snapshot` runs *anyway*: a node that observes `routing.shard_table_version > local_active_version` overwrites its `ShardTable` (line 264 of the helper) and `active_topology_members` with the peer's view — without any quorum validation. The shard table itself is now in a state derived from a possibly-unverified gossip claim. The mismatch with `topology_authority.committed_term()` is then papered over by the subsequent `OP_GET_COMMITTED_TOPOLOGY` round-trip — but until that round-trip lands, the local node serves traffic from a shard table that was *not* derived from any committed term.
- **Impact**: Brief window (one TCP RTT per peer) where the local routing decisions follow an unverified partition map. With an adversarial peer or a buggy `RoutingInfo::decode`, that window is exploitable to misroute writes.
- **Recommendation**: Defer the `install_active_routing_snapshot` call until *after* `OP_GET_COMMITTED_TOPOLOGY` returns a validated `TopologyCommit`. Or sign the partition map with the same voter list that the commit carries.
- **Confidence**: Medium

### F-G8-007: `quiesce()` self-commits a topology without quorum and broadcasts it to peers as authoritative
- **Severity**: HIGH
- **Category**: Correctness
- **Location**: `src/cluster/coordinator.rs:6508-6565`
- **Code**:
  ```rust
  let commit = crate::cluster::topology::TopologyCommit {
      term: new_term,
      proposer: new_members[0],
      members: new_members.clone(),
      digest: ...,
      voters: new_members.clone(),
  };
  // Apply locally first.
  self.topology_authority.handle_commit(&commit);
  self.signal_topology_committed(new_members.clone(), new_term);
  // Broadcast to all peers so they activate the new topology.
  let commit_payload = commit.serialize();
  for &addr in &peer_addrs {
      let _ = send_topology_frame(addr, OP_TOPOLOGY_COMMIT, &commit_payload, ...);
  }
  ```
- **Issue**: `quiesce` (graceful drain) fabricates a `TopologyCommit` with `voters = new_members` — i.e. the voter quorum proof *includes nodes that never actually voted*. `has_quorum_voter_proof()` passes because every "voter" is in members. Peers then accept the commit via `handle_commit` which only checks the voter list is a subset of members. Effectively, any node can unilaterally evict itself by sending a fake commit to its peers, and (worse) the voter-list-as-proof invariant is now broken in the on-disk persisted state.
- **Impact**: An admin-initiated quiesce is racy with simultaneous SWIM-driven topology change. A buggy admin tool that calls `quiesce` on a non-departing node would forcibly commit an arbitrary membership across the cluster. Beyond that, the voter list is no longer a true quorum proof — disk audit of `committed_voters` is unreliable.
- **Recommendation**: Use the normal propose/vote/commit path (initiate a proposal with the new membership and let it converge through quorum) instead of fabricating a commit. Or, if a fast-path is needed, mark these commits with a distinct `proposer` sentinel so on-disk audit can distinguish them from quorum commits.
- **Confidence**: High

### F-G8-008: `routing.rs` partition map encodes node IDs and addresses with no auth-required check at decode site
- **Severity**: MEDIUM
- **Category**: Security
- **Location**: `src/cluster/routing.rs:60-94`, `src/server/dispatch.rs:6297-6324`, `src/protocol/opcodes.rs:368`
- **Code**:
  ```rust
  fn handle_get_partition_map(req: &RequestFrame, cluster: Option<&RunningCluster>) -> ResponseFrame {
      match cluster {
          Some(c) => ResponseFrame {
              request_id: req.request_id, status: STATUS_OK,
              payload: c.encode_partition_map(),
          },
          None => { ... // single-node trivial map }
      }
  }
  ```
- **Issue**: `OP_GET_PARTITION_MAP` *is* in the `is_inter_node_auth_opcode` set (opcodes.rs:371), so when `cluster_secret` is configured the frame must be HMAC-signed. **But clients legitimately need this opcode** — production clients ship with the cluster secret only if they're inter-node. The handler is dual-purpose: serves the routing map both to peers (during catch-up) and to external clients. When `cluster_secret` is configured, *external unauthenticated clients are locked out of partition maps entirely* — which is the safe default — but the documentation/CLI surface presumably allows running without `cluster_secret`, and in that case the partition map (which leaks the full cluster topology, every node's TCP address, and the alive/dead state of every member) is served to anyone who can reach the dispatch port. Confirming the bug: `peek_request_op_code` returns `OP_GET_PARTITION_MAP` but `auth_required = is_inter_node_auth_opcode(op) && cluster_secret.is_some()` (mod.rs:425), so without a secret the check passes through unauthenticated.
- **Impact**: Internet-exposed dispatch port (or a misconfigured proxy) leaks every node's address and shard ownership. This is the kind of recon that precedes targeted attacks on individual masters.
- **Recommendation**: Document loudly that `cluster_secret` is mandatory for any non-localhost deployment. Add a separate "client read" auth surface (e.g. a TLS termination or per-client token) so legitimate external clients can still fetch a partition map without sharing the inter-node HMAC key.
- **Confidence**: High

### F-G8-009: `alive_node_count` self-include heuristic depends on `node_addrs` not containing self (R-039 / EF-02 verification)
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/cluster/coordinator.rs:6326-6363`
- **Code**:
  ```rust
  pub fn alive_node_count(&self) -> usize {
      let committed = self.topology_authority.committed_members();
      let addrs = self.node_addrs.read().unwrap();
      if committed.is_empty() {
          if addrs.contains_key(&self.self_id) { addrs.len() } else { addrs.len() + 1 }
      } else {
          let peers = committed.iter().filter(|node| addrs.contains_key(node)).count();
          let self_committed = committed.contains(&self.self_id);
          let self_in_addrs = addrs.contains_key(&self.self_id);
          if self_committed && !self_in_addrs { peers + 1 } else { peers }
      }
  }
  ```
- **Issue**: The R-039/EF-02 fix is present and correct *for the production case where SWIM does not register self in `node_addrs`*. Two failure modes remain: (a) if a test harness OR future production code path inserts self into `node_addrs` (the existing `RunningCluster::new` does this at line 579, `addrs.insert(config.self_id, config.self_addr)`), the `else` branch returns `peers` *without* +1, but in this codebase that's still correct because self IS in `node_addrs` so it's counted in `peers`. (b) The branch only adds +1 when `self_committed && !self_in_addrs`. If `self` is *not in committed_members* (e.g. graceful drain mid-flight where coordinator has not yet committed eviction), this returns a count that excludes self — but self is still serving. This is the dual of EF-02 and could re-introduce false NO_QUORUM if drain races with writes.
- **Impact**: A node mid-drain rejects writes from clients that haven't yet learned of the topology change.
- **Recommendation**: Decide whether `alive_node_count` should be "members that count for quorum" (committed view) or "what this node sees alive" — they’re different concepts and the heuristic is fragile. Likely you want `topology_authority.committed_members().len()` directly for quorum math.
- **Confidence**: Medium

### F-G8-010: Single source-side TCP timeout for migration; no per-batch ACK retry
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/cluster/coordinator.rs:3447-3697`
- **Code**:
  ```rust
  let response = exchange_frame(stream, &request, auth_secret)?;
  ...
  if !response.payload.is_empty() {
      match ReplicaAck::deserialize(&response.payload) {
          Ok(ReplicaAck::Error { failed_sequence, message }) => {
              return Err(format!("migration batch: ...: {message}"));
          }
          Ok(ReplicaAck::Ok { .. }) => {} // success
          ...
      }
  }
  ```
- **Issue**: `run_migration_batch` does target-ACKs per baseline batch and per delta batch and per `OP_MIGRATION_COMPLETE`, but the source releases the shard fence as soon as `mark_complete` is called (line 3702). If `send_completion_only_handshakes` returns true but the target later crashes before persisting the inbound state to disk (`mark_inbound_complete_many_from_source` is best-effort persist, see migration.rs:1254-1267), the target on restart will *not* know about the migration. The source has already fenced-then-unfenced and forgotten the migration; the target serves stale data. The source-side `send_completion_only_handshakes` retries up to 40 times on TCP errors but does *not* re-poll for "did you persist?" — the protocol assumes a successful `STATUS_OK` ACK implies durable receipt on the target.
- **Impact**: Window for silent state loss on target-side crash mid-handshake. Low probability but non-zero; in chaos tests, this surfaces as a shard with no master after a topology change.
- **Recommendation**: The target must fsync inbound state *before* replying STATUS_OK to `OP_MIGRATION_BATCH_COMPLETE`. Confirm in the dispatch handler (dispatch.rs:821) that `mark_inbound_complete_many_from_source` writes durably synchronously. The current code path:
  ```rust
  cluster.mark_inbound_complete_many_from_source(&shards, from_node);  // calls persist_inbound_state best-effort
  ...
  ResponseFrame { status: STATUS_OK, ... }  // ack regardless of persist outcome
  ```
  ACKs even when persist fails (no `?` propagation). This violates the migration completeness invariant.
- **Confidence**: High

### F-G8-011: Shard ownership atomic check vs in-flight write is racy through `dual_write_targets`
- **Severity**: MEDIUM
- **Category**: Concurrency
- **Location**: `src/cluster/coordinator.rs:6095-6122`
- **Code**:
  ```rust
  pub fn is_migrating_outbound(&self, key: &TxKey) -> bool {
      let shard = ShardTable::shard_for_key(key);
      self.migrating_bitmap.test(shard)
  }
  ...
  pub fn is_shard_write_fenced(&self, key: &TxKey) -> bool {
      let shard = ShardTable::shard_for_key(key);
      self.fenced_bitmap.test(shard)
  }
  ```
- **Issue**: Hot-path checks use lock-free atomic bitmaps, but `dual_write_targets_for_shard` reads a `HashMap` under the migration Mutex (`migration.rs:477-482`). A write that takes the "I am still master, fan out replicas" path samples `migrating_bitmap.test(shard)` to decide if the migration is active, then takes the lock to fetch the destination list. Between those two reads the migration may have completed (atomic clear) AND the dual-write entry removed — the dispatch worker then fans out to the wrong node set (replicas of the *old* shard table). Worst case it fans out to NodeIds that no longer exist; best case it sends a stale `cluster_key`.
- **Impact**: Sporadic "stale epoch" rejections on replicas during topology activations; under heavy churn, possible loss of durability on the migration boundary.
- **Recommendation**: Combine the atomic test with a single Mutex acquisition that returns both "is migrating" and "destination list" atomically, or move dual-write targets into a per-shard atomic structure parallel to `fenced_bitmap`.
- **Confidence**: Medium

### F-G8-012: Migration source releases fence on TCP-ACK; no two-phase commit with target durability
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/cluster/coordinator.rs:3661-3722`
- **Code**:
  ```rust
  if !verified_tasks.is_empty() {
      let delivered = send_completion_only_handshakes(addr, &verified_tasks, self_id, auth_secret);
      for (task, delivered) in verified_tasks.iter().zip(delivered.into_iter()) {
          if !delivered { ... fail ... continue; }
          // Success: mark complete and commit.
          if complete_migration_task_current_epoch(...) {
              completed.fetch_add(1, Ordering::Relaxed);
              cleanup_orphaned_shard_if_settled(...);
          }
      }
  }
  ```
- **Issue**: Related to F-G8-010. The source unfences the shard and commits the local handoff on receipt of TCP `STATUS_OK`. The protocol is single-round — once the target ACKs the batched completion, the source assumes durability. In practice the dispatch handler (dispatch.rs:821) does best-effort persist of inbound state *after* clearing in-memory state, then returns OK. A crash on the target between in-memory clear and disk persist leaves the target ignorant of the migration.
- **Impact**: As in F-G8-010 — silent data loss window. Coupled with the receiver-side gap, this is a real production risk.
- **Recommendation**: Target must `sync_data` before returning STATUS_OK from `OP_MIGRATION_BATCH_COMPLETE`. Source should treat any non-OK as ABORT (it already does via `delivered=false`) and verify durability with a follow-up `OP_GET_INBOUND_STATE` query if needed.
- **Confidence**: Medium

### F-G8-013: Topology proposer retries reuse the same `_started_at` so timeouts don't reset
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/cluster/coordinator.rs:2469-2513`, `src/cluster/topology.rs:798-863`
- **Code**:
  ```rust
  for attempt in 0..5u32 {
      if attempt > 0 {
          let delay_ms = 200u64 * (1u64 << (attempt - 1).min(3));
          std::thread::sleep(Duration::from_millis(delay_ms));
          match topology_authority.retry_proposal() {
              Some(fresh) => { proposal = fresh; }
              None => { return; }
          }
      }
      if try_run_topology_proposal(...) { return; }
  }
  ```
- **Issue**: Each retry generates a *fresh term* but the `pending_proposal._started_at` is reset (`Instant::now()` in retry_proposal:859), which is correct. However, the field is `_started_at` (leading underscore = unused). Nothing in `handle_vote` actually consults it, so retry timeouts are wall-clock only via the proposer thread's `sleep`. If the proposer thread is descheduled and the cluster commits a higher term in parallel, the retry will keep cranking proposals at lower terms that nobody accepts — until 5 attempts exhaust. Mostly cosmetic but worth surfacing: the structure suggests an intended liveness guarantee that isn't actually enforced.
- **Impact**: Marginal latency under contention; no correctness issue.
- **Recommendation**: Either consume `_started_at` (drop pending_proposal when `Instant::now() - started_at > some_bound`) or remove the field.
- **Confidence**: Medium

### F-G8-014: SWIM message receive loop drops malformed/unauthenticated packets silently with no rate limit
- **Severity**: LOW
- **Category**: Security
- **Location**: `src/cluster/swim.rs:478-487`
- **Code**:
  ```rust
  let data = if let Some(ref secret) = self.config.cluster_secret {
      match crate::cluster::auth::verify(secret, data) {
          Ok(payload) => payload,
          Err(_) => return vec![], // silently drop unauthenticated messages
      }
  } else { data };
  ```
- **Issue**: No metric increment, no log line, no rate-limit on dropped packets. An attacker flooding the SWIM port with random bytes burns CPU on HMAC verification (a not-cheap operation given the hand-rolled SHA-256) with no observability. The dispatch loop drains 64 packets per iteration before yielding, so a sustained flood can starve probe scheduling.
- **Impact**: SWIM probe latency under attack increases; failure detection may be delayed.
- **Recommendation**: Add a counter for `swim_unauthenticated_drops` and structured log at `trace` level. Consider a token bucket per source address.
- **Confidence**: High

### F-G8-015: Indirect probe peer selection is not randomized; same K peers always asked
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/cluster/swim.rs:857-865`
- **Code**:
  ```rust
  let k = INDIRECT_PROBE_K.min(peers.len());
  for &(peer_id, _tcp_addr) in peers.iter().take(k) {
      if let Some(&addr) = swim_addrs.get(&peer_id) {
          let _ = socket.send_to(&msg, addr);
      }
  }
  ```
- **Issue**: `peers` is iterated in HashMap-insertion order (non-deterministic but stable per process). `.take(k)` always picks the same prefix. SWIM design intent is to fan out across a random subset of peers to defeat correlated failures. Currently if the first three peers happen to be unreachable, indirect probing never reaches the rest of the cluster.
- **Impact**: Failure-detection accuracy degrades in clusters where the SWIM table order correlates with reachability (e.g. all nodes in one rack appear first).
- **Recommendation**: Shuffle `peers` before `take(k)`. Use the same RandomState-hash trick as `jittered_probe_interval` to avoid a `rand` dep.
- **Confidence**: High

### F-G8-016: `apply_master_election` empty partition view leaves round-robin master in place even when ownership changed (R-052 partial verification)
- **Severity**: MEDIUM
- **Category**: Correctness
- **Location**: `src/cluster/coordinator.rs:5643-5708`
- **Code**:
  ```rust
  let has_data = if view_empty {
      true
  } else {
      seq_by_node_shard.get(&(node_id, shard)).copied().unwrap_or(0) > 0
  };
  MasterCandidate {
      node_id,
      was_previous_master: node_id == prev_master,
      is_subset: !has_data,
      was_evicted: evicted.contains(&node_id),
  }
  ```
- **Issue**: When the partition view is empty (exchange phase timed out or single-node), every candidate is marked `is_subset = false` (full). The election then picks the highest-scoring candidate with previous-master sticky preference. *But the round-robin pick may not be the previous master* — and with `is_subset=false` for everyone, the score tie is broken by `was_previous_master`, which corrects this. Verified live for the prev_master case. However: if `prev_master` is in the `evicted` set (R-052 / quiesce scenario), `prev_master` is filtered out by `filter(|c| !c.was_evicted)` in `elect_master`, and the round-robin pick wins regardless of its data state — but in the empty-view path, "data state" is assumed true, so a freshly-bootstrapped empty node could be elected master if it happens to be the round-robin pick. The `evicted` set is currently *always empty* (line 1802), so this branch is unreachable in practice — but a future Phase I wiring of evictions would surface the bug.
- **Impact**: Forward-looking gap. With current code (empty `evicted`), no immediate fault. If evictions are wired in (the doc comments anticipate this), this will silently elect an empty master.
- **Recommendation**: Document the invariant that `evicted` must remain empty until the empty-view fallback is hardened, OR make `view_empty` always set `is_subset=true` for candidates whose `was_previous_master=false`. The latter is the conservative choice.
- **Confidence**: Medium

### F-G8-017: `mark_inbound_complete_many_from_source` persist is best-effort and ACK returns OK regardless
- **Severity**: HIGH
- **Category**: Correctness / Durability
- **Location**: `src/cluster/coordinator.rs:6157-6167`, `src/cluster/migration.rs:1254-1267`
- **Code**:
  ```rust
  pub fn mark_inbound_complete_many_from_source(&self, shards: &[u16], from_node: NodeId) {
      ...
      mgr.mark_inbound_complete_many_from_source(shards.iter().copied(), from_node);
      self.inbound_atomic.load_from(mgr.inbound_bitmap());
      if let Some(ref path) = self.inbound_state_path {
          crate::cluster::migration::persist_inbound_state(path, mgr);  // best-effort
      }
  }
  ```
  And the persist:
  ```rust
  pub fn persist_inbound_state(path: &std::path::Path, mgr: &MigrationManager) {
      ...
      if let Err(e) = result {
          tracing::warn!(err = %e, "cluster: failed to persist inbound migration state");
      }
  }
  ```
- **Issue**: `persist_inbound_state` swallows IO errors. The caller (`mark_inbound_complete_*`) has no way to know persist failed. The dispatch handler for `OP_MIGRATION_BATCH_COMPLETE` (dispatch.rs:890-911) calls `mark_inbound_complete_many_from_source` and then unconditionally returns STATUS_OK. A crashed target post-ACK but pre-persist resurrects pending inbound entries on restart — but only the ones whose persist happened to land before the crash. The source has already cleared its outbound state on receipt of the OK.
- **Impact**: Identified as the durability gap behind F-G8-010 / F-G8-012. This is the actual mechanism: ACK-before-fsync on the target side.
- **Recommendation**: `persist_inbound_state` must return `io::Result`, and the dispatch handler must reject the frame with `ERR_TOPOLOGY_PERSIST_FAILED` (or a new dedicated error) on persist failure. The source then retries — matches the existing pattern in the `OP_TOPOLOGY_PROPOSE` handler.
- **Confidence**: High

### F-G8-018: Lock ordering: SWIM acquires (membership → peer_addrs → swim_peer_addrs) but topology event loop may take node_addrs (RwLock) before migration (Mutex) — no documented total order
- **Severity**: MEDIUM
- **Category**: Concurrency
- **Location**: `src/cluster/swim.rs:747-764`, `src/cluster/coordinator.rs:778-783, 1840-1928`
- **Code**:
  ```rust
  // SWIM (swim.rs:747)
  let membership = self.membership.lock().unwrap();
  let swim_addrs = self.swim_peer_addrs.lock().unwrap();
  let peers: Vec<...> = self.peer_addrs.lock().unwrap().iter()...;
  drop(swim_addrs); drop(membership);
  ```
  vs.
  ```rust
  // Coordinator event loop fallback (coordinator.rs:778)
  let members: Vec<NodeId> = {
      let addrs = node_addrs.read().unwrap();
      ...
  };
  ...
  let mut mgr = migration.lock().unwrap();  // (e.g. at line 880, 1841, 6473)
  ```
- **Issue**: The coordinator's `node_addrs` is the same `Arc<RwLock<HashMap<NodeId, SocketAddr>>>` (RwLock, std), while SWIM uses three separate `Mutex<HashMap<...>>` for its own membership state. There’s no single lock-ordering rule documented. Examples that approach the danger zone:
  - `quiesce()` reads `node_addrs` then sends UDP via the same socket SWIM uses (no shared lock, OK).
  - Hot path: `dispatch.rs` → `cluster.dual_write_targets_for_shard(shard)` → `migration.lock()`, while `node_addrs.read()` is taken elsewhere.
  Today no deadlock cycle exists, but the partial orders are not enforced. Future code that takes `node_addrs.write()` while holding `migration.lock()` (already happens implicitly in `quiesce`) and another path that takes them in reverse order would deadlock under churn.
- **Impact**: Latent deadlock risk; no current production trigger.
- **Recommendation**: Document the global lock order in `cluster/mod.rs` and add a debug-assert via lockdep-style instrumentation (or `parking_lot::deadlock::check_deadlock` in tests).
- **Confidence**: Medium

### F-G8-019: `shards.rs::set_master_for_shard` silently demotes existing master into replica slot — replica array may exceed RF
- **Severity**: LOW
- **Category**: Correctness
- **Location**: `src/cluster/shards.rs:360-381`
- **Code**:
  ```rust
  pub fn set_master_for_shard(&mut self, shard: u16, new_master: NodeId) {
      let idx = shard as usize;
      let current = &mut self.assignments[idx];
      if current.master == new_master { return; }
      let promote_idx = current.replicas.iter().position(|n| *n == new_master);
      let Some(replica_idx) = promote_idx else {
          tracing::warn!(...); return;
      };
      let demoted = std::mem::replace(&mut current.master, new_master);
      current.replicas[replica_idx] = demoted;
  }
  ```
- **Issue**: Looks correct, but `master_subset` is not updated. `apply_master_election` runs *before* `begin_handoff_with` (see coordinator.rs:1803,1818) so `master_subset` is recomputed in `begin_handoff_with`, so this is fine. The minor concern: `apply_master_election` calls `set_master_for_shard` in a tight loop over 4096 shards; the `replicas.iter().position()` is O(rf) which is fine, but the *outer* iteration of `apply_master_election` builds a fresh `candidates: Vec<MasterCandidate>` for each of the 4096 shards. With RF=3 this is 4096 * 4 * small_struct allocations per topology activation — measurable. Not a bug, but a perf gap worth a comment.
- **Impact**: ~50 KB transient allocation per activation; non-issue at current scale but easy to hoist outside the loop.
- **Recommendation**: Reuse a `Vec<MasterCandidate>` across iterations (clear() instead of new allocation).
- **Confidence**: High

### F-G8-020: Auth uses hand-rolled SHA-256 — no audit, no constant-time invariants beyond `constant_time_eq`
- **Severity**: LOW
- **Category**: Security
- **Location**: `src/cluster/auth.rs:248-332`
- **Code**:
  ```rust
  pub fn sha256(data: &[u8]) -> [u8; 32] { /* full implementation inline */ }
  ```
- **Issue**: A 100-line hand-rolled SHA-256 has been added "to avoid pulling in a heavy crypto crate for a single function" (line 53). RFC 4231 vectors are tested. But: (a) no `subtle` / `subtle-encoding` dependency means timing-side-channel of the inner state mix-in cannot be guaranteed without audit; (b) `inner_input = Vec::with_capacity(64 + data.len())` allocates per HMAC call, scaling poorly with `MAX_FRAME_SIZE`-sized messages — the hot path goes through this. The constant-time comparison is correct, but the underlying SHA-256 is plain Rust integer ops with no constant-time guarantees beyond what the compiler emits.
- **Impact**: Performance — one fresh `Vec` allocation per signed/verified frame. Security — a timing side-channel attack is unlikely at the network layer but not impossible.
- **Recommendation**: Use `sha2` / `hmac` crates (already pulled into Rust crypto WGs for audit). Drop the custom implementation. The "heavy crate" argument is unconvincing for a workspace already pulling in tracing, parking_lot, libc, etc.
- **Confidence**: Medium

### F-G8-021: `mod.rs` is a single re-export file; no architectural concerns
- **Severity**: INFO
- **Category**: Code Quality
- **Location**: `src/cluster/mod.rs:1-11`
- **Code**:
  ```rust
  pub mod auth;
  pub mod coordinator;
  pub mod membership;
  pub mod migration;
  pub mod routing;
  pub mod shards;
  pub mod swim;
  pub mod topology;
  ```
- **Issue**: Positive verification. `mod.rs` is a clean re-export. Worth noting: no shared lock-order documentation here (see F-G8-018 recommendation).
- **Impact**: None.
- **Recommendation**: Consider adding a `LOCK_ORDER` constant doc comment in `mod.rs` listing the canonical acquisition order of `topology_authority`, `migration`, `shard_table`, `node_addrs`, `peer_addrs`, `swim_peer_addrs`, `membership` so future contributors don't introduce a cycle.
- **Confidence**: High

### F-G8-022: `membership.rs` `forget_dead_older_than` retains incarnation in `max_seen_incarnation` — verified safe
- **Severity**: INFO
- **Category**: Correctness (positive verification)
- **Location**: `src/cluster/membership.rs:340-358, 124-127`
- **Code**:
  ```rust
  pub fn forget_dead_older_than(&mut self, max_age: Duration) -> Vec<NodeId> {
      ...
      for id in &to_remove { self.members.remove(id); }
      // max_seen_incarnation deliberately retained
      ...
  }
  ```
- **Issue**: Positive verification. `max_seen_incarnation` is *not* purged when a member is forgotten, so a "reborn" NodeId cannot replay an older incarnation. The check on alive update (line 125) `if incarnation < historic_incarnation { return events; }` enforces this. Combined with persisted-incarnation on restart (`coordinator.rs:573`), restart-replay is also blocked. Good.
- **Impact**: None — correctly designed.
- **Recommendation**: Add a comment in `forget_dead_older_than` explicitly explaining why `max_seen_incarnation.remove(id)` is intentionally absent.
- **Confidence**: High

### F-G8-023: `routing.rs` decode does not bound `node_count` or `cm_count` against `data.len()` upfront
- **Severity**: LOW
- **Category**: Security / Robustness
- **Location**: `src/cluster/routing.rs:103, 138-151`
- **Code**:
  ```rust
  let node_count = u32::from_le_bytes(data[8..12].try_into().ok()?) as usize;
  let mut pos = 12;
  let mut nodes = Vec::with_capacity(node_count);
  for _ in 0..node_count { ... }
  ...
  let cm_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap_or([0; 4])) as usize;
  pos += 4;
  for _ in 0..cm_count { if pos + 8 > data.len() { break; } ... }
  ```
- **Issue**: `Vec::with_capacity(node_count)` allocates `node_count * 24` bytes upfront. A peer sending `node_count = u32::MAX` causes a single allocation attempt of ~100 GiB before any bounds checking. Same with `cm_count` — though `cm_count`'s loop has a per-iteration break. The outer `MAX_FRAME_SIZE` of 16 MiB does provide an upper bound (a 16 MiB payload can encode ~700k nodes max), but `with_capacity` could still reserve far more than needed if `node_count` is set adversarially.
- **Impact**: Memory pressure from a single malformed authenticated peer (auth required for inter-node) or a misconfigured client. Likely caught by Rust's allocator returning failure rather than a crash, but degrades service.
- **Recommendation**: Sanity-check `node_count <= data.len() / 10` (min entry size) before `Vec::with_capacity`. Same for `cm_count <= data.len() / 8`.
- **Confidence**: High

### F-G8-024: Migration `Failed` state retained in `active` list until cleanup — `dispatch.dual_write_targets_for_shard` scans through them every spend
- **Severity**: LOW
- **Category**: Performance
- **Location**: `src/cluster/migration.rs:477-499, 769-790`
- **Code**:
  ```rust
  pub fn dual_write_targets_for_shard(&self, shard: u16) -> &[NodeId] {
      self.dual_write_targets
          .get(&shard)
          .map(|v| v.as_slice())
          .unwrap_or(&[])
  }
  ```
  with the storage:
  ```rust
  dual_write_targets: std::collections::HashMap<u16, Vec<NodeId>>,
  ```
- **Issue**: `dual_write_targets` is a HashMap, so per-shard lookup is O(1). But to take the value the dispatch hot path *still acquires the migration Mutex* (see `coordinator.rs:6629-6635`). Failed migrations are removed from `dual_write_targets` (via `mark_failed → dual_write_remove`), so the map stays small — verified positive. However, the hot path acquires the same Mutex that `cleanup_completed` holds (see F-G8-005). The interaction is unfortunate but not unsound.
- **Impact**: Per-spend Mutex acquisition cost; non-zero but small.
- **Recommendation**: As in F-G8-005, move `dual_write_targets` behind a `parking_lot::RwLock` or an atomic per-shard structure so dispatch doesn't contend with topology activations.
- **Confidence**: Medium

### F-G8-025: D-20 prior audit verification: `committed_topology_term` always reflects atomic state — live
- **Severity**: INFO
- **Category**: Correctness (positive verification)
- **Location**: `src/cluster/coordinator.rs:6493-6499, 6429-6432`
- **Code**:
  ```rust
  pub fn committed_topology_term(&self) -> u64 {
      self.topology_authority.committed_term()
  }
  ...
  pub fn local_cluster_key(&self) -> u64 {
      self.committed_cluster_key.load(Ordering::Acquire)
  }
  ```
- **Issue**: Positive verification. `committed_cluster_key` is sourced from `topology_authority.committed_term_shared()` (line 574, 606) so the atomic mirror is the *same* `Arc<AtomicU64>` — no setter call needed, no risk of drift. D-20 (replica batch cluster_key gate) is structurally sound.
- **Impact**: None — verified working as designed.
- **Recommendation**: None.
- **Confidence**: High

---

## Coverage notes

**Files covered with explicit findings:**
- `src/cluster/coordinator.rs` — read in five chunks (~1-2200, 2200-3000, 3000-3800, 3800-4600, 4600-5400, 5400-6200, 6200-end). Findings: F-G8-005 (lock contention in `activate_topology_with_view`), F-G8-006 (catch-up trusts unverified routing), F-G8-007 (`quiesce` fabricates voter quorum), F-G8-009 (alive_node_count drain gap), F-G8-010/F-G8-012 (migration ACK-before-durability), F-G8-013 (proposer retry plumbing), F-G8-016 (apply_master_election future-evicted gap), F-G8-017 (target-side persist), F-G8-025 (D-20 verification).
- `src/cluster/swim.rs` — read in full sections (1-600, 600-end). Findings: F-G8-004 (ping_req_forwarding leak), F-G8-014 (silent drop), F-G8-015 (indirect probe selection bias).
- `src/cluster/topology.rs` — read 1-700, 700-end. Findings: F-G8-001 (split-brain pure-superset gap), F-G8-002 (handle_propose missing check), F-G8-013 (retry timing).
- `src/cluster/migration.rs` — read 1-560, 560-1300. Findings: F-G8-017 (persist best-effort), F-G8-024 (Mutex contention).
- `src/cluster/shards.rs` — read 1-540. Findings: F-G8-019 (set_master_for_shard perf note). Positive: rollback / handoff state machine is well-isolated.
- `src/cluster/membership.rs` — read 1-365 (state machine), grep'd rest. Findings: F-G8-022 (positive verification of forget_dead).
- `src/cluster/auth.rs` — read in full. Findings: F-G8-003 (replay defense), F-G8-020 (hand-rolled SHA-256).
- `src/cluster/routing.rs` — read in full. Findings: F-G8-008 (partition map leak when no secret), F-G8-023 (unbounded with_capacity).
- `src/cluster/mod.rs` — read in full. Finding: F-G8-021 (info, recommend lock-order doc).

**Cross-cutting findings:** F-G8-011 (atomic vs mutex consistency on dual-write), F-G8-018 (lock ordering).

**Prior-audit verification status:**
- **EF-01 (topology message authentication):** LIVE — `is_inter_node_auth_opcode` includes `OP_TOPOLOGY_PROPOSE/VOTE/COMMIT` and `mod.rs:425-446` enforces signing when `cluster_secret` is set. Unsigned-frame-rejection tests pass (`mod.rs:580-593`).
- **EF-02 (alive_node_count excludes self):** LIVE — fix at `coordinator.rs:6326-6363` correctly counts self. New gap surfaced in F-G8-009 for the mid-drain case.
- **R-042 (split-brain merge rejection):** PARTIAL — narrow case (add AND remove) live, pure-superset case still vulnerable. See F-G8-001.
- **R-052 (compensation deltas during migration):** LIVE — `redo_entry_to_replica_op` correctly drops `CompensateUnsetMined` etc. (coordinator.rs:5225-5238).
- **D-20 (cluster_key gate):** LIVE — see F-G8-025.

**Out-of-scope but noted for parent reviewer:** The receiver-side persist invariant in `dispatch.rs:821-911` (OP_MIGRATION_BATCH_COMPLETE) is the actual durability bug, but the handler lives in `server/dispatch.rs` (group G6 / G7). F-G8-017 documents the cluster-side surface; the dispatch-side fix belongs to that group.

**Severity counts:** CRITICAL 1, HIGH 8, MEDIUM 8, LOW 6, INFO 3. Total 26.
