# Review follow-ups — outstanding work after fix-campaign + perf-audit

Per `_review/FIX_POLICY.md` "No drive-by fixes — anything unrelated →
`_review/follow_ups.md`". This file lists every tightly-scoped follow-up
that the campaign deferred, plus production bugs uncovered during the
later test/perf audits that were not in scope for any group fixer.

For ordering, sizing, and an end-to-end finish plan see
`_review/ROADMAP_TO_100.md`.

State of `main` at `c87339c`: `cargo test --all` = 2092 passed / 0 failed
across 54 binaries; `cargo check --lib` clean; `cargo check --bins`
clean.

## Recently resolved (2026-05-18)

- **F-G1-015 metric — RESOLVED** (P2.3 / B-1). Added
  `corrupt_redo_entries_total: PaddedCounter` to `AllocatorMetrics` in
  `src/metrics.rs` and bumped it at every `tracing::error!` rejection
  site inside `replay_allocate` / `replay_free` in `src/allocator.rs`.
  Wired into `/metrics` and `/admin/top`.
- **F-G1-019 metric / warn — RESOLVED** (P2.3 / B-2). Added
  `generation_wrap_warn_total: PaddedCounter` to `AllocatorMetrics`
  + `GENERATION_WRAP_WARN_DELTA = 1u32 << 30` in `src/record.rs`.
- **F-G8-004 SWIM ping_req metric — RESOLVED** (P2.4 / A-5). Moved
  `SWIM_PING_REQ_DROPPED_TOTAL` from `cluster::swim` to
  `SwimMetrics::swim_ping_req_dropped_total`.

---

- **F-G1-003 atomic-chunk migration — RESOLVED (FIXED, partial)** —
  `read_metadata_direct`, `write_metadata_direct`, `read_utxo_slot_direct`,
  and `write_utxo_slot_direct` in `src/io.rs` now perform the bulk byte
  transfer through `AtomicU64::load(Relaxed)` / `store(Relaxed)` chunks
  via `atomic_load_into` / `atomic_store_from`. Targeted footer
  helpers (`write_mutation_footer_direct`, `write_spend_footer_direct`,
  `write_mined_footer_direct`, `write_block_entry_direct`,
  `write_crc_direct`) still use non-atomic `ptr::copy_nonoverlapping`
  — deferred because no current miri test exercises them concurrently
  but production race remains.
- **F-G1-004 MemoryDevice aliasing — RESOLVED (FIXED)** — Option A:
  dropped `RwLock<Vec<u8>>`; backing allocation is `Box::into_raw` →
  raw `*mut u8` + `len`, with `Box::from_raw` in `Drop`. `pread` /
  `pwrite` rebuild a short-lived slice per call.

---

## A. Production bugs

### A-1. ~~`src/cluster/topology.rs:706` — `ever_seen_check` runs unconditionally~~ — RESOLVED 2026-05-18

**Status: FIXED in `4df687e merge(p1.1): cluster_id wired end-to-end`.**

`cluster_id` is now wired through `TopologyTerm` (wire-format bump),
`TopologyAuthority::new_with_config`, `ServerConfig.cluster_id`,
`--cluster-id` CLI flag. All 4 `membership_change_is_safe` call sites
pass `Some(...)`. When both sides have a configured matching
`cluster_id`, the ever-seen fallback is skipped (cluster_id is the
primary split-brain defence). Tests in `tests/g8_cluster_id.rs` lock
down both scale-up and refuse-foreign-superset paths.

Files: `src/cluster/topology.rs`, `src/cluster/coordinator.rs`,
`src/config.rs`, `src/bin/server.rs`, `src/server/dispatch.rs`,
`tests/cluster_tcp.rs`, `tests/cluster_edge_cases.rs`,
`tests/g8_split_brain.rs`, `tests/g8_cluster_id.rs` (new).

### A-2. `src/server/mod.rs:264-272` — 10 ms spin in accept loop (F-G6-019)

Sleeping in a polling loop. Replace with `mio::Poll` or a self-pipe so
shutdown is observed immediately and idle CPU drops to zero.

### A-2b. ~~Shard-table never recomputes for 2→3 scale-up~~ — RESOLVED 2026-05-18

**Status: FIXED in `0d3bd4f fix(A-2b): preserve round-robin pick when no candidate has shard data`.**

Root cause was inside `apply_master_election`
(`src/cluster/coordinator.rs:5701`). On a fresh empty cluster every
candidate reports `last_applied_seq == 0`, so every candidate is
classified `is_subset`; `elect_master`'s `was_previous_master`
stickiness tiebreaker then always picked the previous master over the
newcomer — silently un-doing every shard the round-robin assigned to
NodeId(223). Fix: when no candidate reports any data, skip the
election entirely and preserve the round-robin pick (same shape as
`view_empty`). Test now passes 5/5 with strict `shard_counts.len() ==
3 && all > 0` predicate. Final distribution: `{NodeId(221): 1366,
NodeId(222): 1365, NodeId(223): 1365}` — exact round-robin on 4096
shards / 3 members.

### A-3. F-G7-001 metric not incremented anywhere

`replica_unauthenticated_accept_total` counter exists in `metrics.rs`
(G7 added the schema) but no production site increments it. The auth
gate that decides "accept or reject when `cluster_secret = None`"
lives in `src/server/mod.rs::handle_connection_inner` (G5). The
increment is the visible-signal half of the trusted-overlay policy.

### A-4. F-G5-022 — engine-side atomic apply — RESOLVED 2026-05-18 (NOT-APPLICABLE)

Reproduction test
`tests/g2_atomic_apply.rs::concurrent_spend_same_utxo_yields_exactly_one_winner`
(16 threads × 200 iterations spending the same UTXO with distinct
`spending_data` per thread) passes today: exactly one thread returns
`Ok`, the other 15 return `Err(AlreadySpent { .. })`, and the on-device
slot reflects the unique winner's `spending_data`. The atomic-apply
invariant is already established by the per-tx stripe mutex
(`Engine::spend` and siblings acquire `self.locks.lock(&tx_key)` as
their first action and hold it through the full read → validate →
write → index-sync sequence).

Action taken (P1.3 → documentation only):
- Added an "Atomic-apply invariant" section to the `Engine` doc
  comment in `src/ops/engine.rs` enumerating the mutation entry points
  that hold the stripe mutex and pointing at the reproduction test as
  the regression guard.
- Replaced the hypothesis TODO at `src/server/dispatch.rs:3242` with
  a RESOLVED block explaining that the unlocked `read_block_entry`
  for the unset-mined before-image is a snapshot, not a write-
  correctness hazard: the engine still applies under its own lock,
  and compensation rollback runs before the response leaves the
  server.

No code-correctness change required.

### A-5. F-G7-024 / F-G8-004 metric integration

`SWIM_PING_REQ_DROPPED_TOTAL` counter lives inside `cluster::swim`
rather than in the registry. Promote to `metrics.rs` so the operator
dashboard can observe SWIM-flood drops.

---

## B. Wire-up follow-ups (telemetry / config)

### B-1. F-G1-015 — `corrupt_redo_entries_total` counter

`AllocatorMetrics` already logs via `tracing::error!` on
recovery-time corruption rejection; adding a Prometheus counter lets
dashboards alert on non-zero rates. Touched files: `src/metrics.rs`,
allocator call sites in `src/allocator.rs`.

### B-2. F-G1-019 — generation-wrap early-warning

`warn`-level log + counter when a record's generation jumps >2³⁰
(approaching the wrap-classification ambiguity window). Bundle with
B-1 — same metric module, same operator-dashboard target.

### B-3. F-G6-020 — `inflight_bytes_rejected_total`

Increment a counter in `InflightBytesLimiter::try_acquire` (lives in
`src/server/mod.rs:53-85`, G5 territory). Counter slot is G6's
responsibility but the call site is G5's. Land in a single commit.

### B-4. F-G10-017 — typed `CatchupError`

Replace `Err(String)` + substring match (`"redo entries reclaimed"`)
between `src/replication/durable.rs:728` and `src/bin/server.rs:1065`
with a `CatchupError::RedoReclaimed { ... }` enum variant. Signature
change in `run_catchup_for_replica`. Bin call site adopts the typed
arm once the lib side lands.

### B-5. F-G6-025 — HTTP error body envelope — RESOLVED

`HttpErrorBody { code, message, details? }` JSON envelope is defined in
`src/server/http.rs` and every error path on the observability surface
now returns `Content-Type: application/json` with that shape. Status
codes are preserved exactly so existing dashboards keep working.
Coverage: 401 (auth middleware: missing / wrong / malformed bearer);
400 (`not in cluster mode` on rebalance/quiesce/drain, drain node_id
mismatch with structured `details`, invalid log level, invalid txid
length, invalid txid hex); 404 (`tx not found`); 503
(`/health/ready` reason). The embedded `/ui/*` static-asset 404 path is
unchanged (it is not an API surface; SPA-fallback semantics remain).
Integration coverage in `tests/http_observability.rs::
error_responses_use_structured_json_envelope` plus the two companion
tests `unauthorized_response_advertises_json_content_type` and
`error_envelope_omits_details_when_not_attached`.

---

## C. Deferred performance / refactor

### C-1. F-G1-012 — `nix`/`rustix` port for ioctl portability — RESOLVED

Hard-coded `BLKGETSIZE64` / `DKIOCGETBLOCKCOUNT` in `src/device.rs`
were correct for x86_64 / aarch64 Linux + macOS but wrong on 32-bit
Linux (where `size_t` is 32-bit and the encoded ioctl number differs).
Ported to `nix::ioctl_read!` macros which compute the encoding from
`(magic, num, type)` at compile time per target, so the same call
site is portable across all Linux ABIs and macOS. The bare numeric
constants are gone. Added `nix = "0.31"` with only the `ioctl`
feature (no-std, libc-only) under `[target.'cfg(unix)'.dependencies]`.
See P3.3 in `_review/ROADMAP_TO_100.md`. Verified by
`cargo check --lib`, `cargo clippy --lib -- -D warnings`, and
`cargo test --lib device::tests::` (36/36 passing on macOS host).

### C-2. F-G1-002 — `#[must_use]` typestate guard for footer + CRC — RESOLVED 2026-05-18

**Status: FIXED.** `src/io.rs` now ships a `FooterPendingCrc<'a>`
typestate token marked `#[must_use]` and returned from a new
`write_mutation_footer_pending_crc(...)` entry point. Consumers MUST
call `.stamp_crc(meta)` to release the BC-02 record-level write guard;
dropping the token without stamping fires `debug_assert!` (release
builds still surface the bug via `RecordCorruption` on the next read).
The combined `write_mutation_footer_and_crc_direct` now delegates to
the typestate path so both spellings stay bit-identical. Coverage:
`io::tests::footer_pending_crc_panics_when_dropped_unstamped`,
`footer_pending_crc_stamp_round_trips`,
`footer_pending_crc_matches_combined_wrapper`, plus a `compile_fail`
doctest on `FooterPendingCrc` proving `unused_must_use` rejects a
forgotten token at compile time.

### C-3. F-G1-003 — atomic-chunk migration

Migrate metadata read/write to `AtomicU64::load(Relaxed)` /
`store(Relaxed)` so the unsynchronised access stops being UB-on-paper
under Stacked Borrows / Tree Borrows. Touches every G2 call site;
verify with `cargo miri test`. CRC + BC-06/BC-07 fences are the
current safety net.

### C-4. F-G1-004 — `MemoryDevice` aliasing

`data: RwLock<Vec<u8>>` paired with `raw_ptr` aliases the same heap
allocation (UB under Stacked/Tree Borrows). `cargo miri` against the
test suite will fail. Either drop the lock and route everything
through `raw_ptr`, or drop `as_raw_ptr` for `MemoryDevice`. F-G1-017
removed the parallel `raw_len` so this is the last aliasing piece.

### C-5. F-G1-016 — rollback coalesce — RESOLVED 2026-05-18

**Status: FIXED.** `SlotAllocator::rollback_reservation` now calls a
new `coalesce_adjacent(offset, size)` helper that merges the
re-inserted region with any adjacent free regions (matching the
invariant `free()` maintains). Unreachable under today's
single-threaded allocator — pure forward-looking hardening for any
future world where rollback could race with another `free()`.
Coverage:
`allocator::tests::rollback_coalesces_adjacent_free_regions`
(end-to-end via a redo-flush failure on a staged adjacent freelist)
plus `allocator::tests::coalesce_adjacent_merges_neighbours`
(direct invocation of the new helper).

### C-6. F-G5-011 — frame zero-copy

`RequestFrame::decode` allocates a full-payload `Vec` per frame.
Switching to `Bytes`/`Cow` requires lifetime-parameterising
`RequestFrame` and every handler. Performance ceiling, not correctness.

### C-7. ~~F-G5-016 — streaming HMAC~~ — RESOLVED 2026-05-18

**Status: FIXED.** `src/cluster/auth.rs` now exposes
`verify_frame_streaming<R: Read, W: Write>(key, reader, payload_sink)`
built on a streaming `HmacSha256` accumulator (`sha2::Sha256` under
the hood). The verifier reads the body in `STREAM_CHUNK_SIZE` (8 KiB)
chunks, keeps a `SIGNED_SUFFIX_LEN` (40 B) rolling tail to isolate
the timestamp + tag, and writes payload bytes to a caller-supplied
sink as they pass through HMAC.

Memory used during streaming: chunk buffer (8 KiB) + tail (40 B) —
independent of payload size. The slow-loris regression test
`slow_loris_16mib_wrong_hmac_rejects_without_buffering_payload`
proves a 16 MiB wrong-HMAC frame rejects with `PermissionDenied`
while the verifier writes payload bytes to a `CountingSink` (a
test-only sink that discards bytes), demonstrating the verifier does
not need to copy the full payload.

`verify_frame(&[u8])` is preserved as a thin wrapper that feeds
`Cursor<&[u8]>` into `verify_frame_streaming`; public API unchanged.
Production read sites (`src/server/mod.rs::handle_connection_inner`,
`src/replication/tcp_transport.rs`, `src/replication/receiver.rs`,
`src/cluster/coordinator.rs`) continue to call the buffered wrapper
because they need the buffered bytes for orthogonal reasons
(peek_request_id, peek_op_code, inflight-bytes permit sizing); a
follow-up could refactor those to stream through the new API once
the surrounding peek/permit machinery is streaming-aware too.

Tests: 8 new tests in `cluster::auth::tests` covering round-trip
equivalence with `verify_frame`, wrong-tag rejection, short-frame
bounds, oversized-body bounds, stale timestamp, chunked-reader
correctness (one-byte-at-a-time `OneByteReader`), the 16 MiB
slow-loris case, and underlying I/O error propagation.

### C-8. F-G5-017 — typed error codes

Introduce `ERR_PAYLOAD_MALFORMED`, `ERR_OPCODE_UNSUPPORTED`,
`ERR_STORAGE_IO` etc. Public-wire change — touches every client
adapter. Defer until a client team requests it.

### C-9. F-G2-020 — `delete()` perf opportunity — RESOLVED 2026-05-18 (DEFERRED-NO-WIN)

Re-evaluated as P3.6. Baseline `cargo bench --bench engine_remaining
-- 'delete/delete_one'` measures 4.96 µs median on this host.

The most surgical optimization is to skip the full 320-byte
`read_metadata_fast` call inside `Engine::delete` and read only the
4-byte `record_size` field from offset 8 of the on-device metadata
(saves a 320-byte memcpy + CRC32 verify per delete). A prototype that
adds `META_OFF_RECORD_SIZE` + an unsafe `read_record_size_direct` in
`src/io.rs` and dispatches `delete()` to it on the direct-pointer path
was implemented and benched.

A/B with separate `--save-baseline` / `--baseline` runs:

  - Before: 4.96 µs median (CI [4.87, 5.05])
  - After:  5.00 µs median (CI [4.89, 5.11])
  - Change: +0.9% (CI [-6.6%, +9.8%], p = 0.83)

Criterion reports "No change in performance detected". The 320-byte
memcpy + CRC compute is not the dominant cost in this microbench;
allocator-free + secondary-index updates + index write-lock /
unregister dominate. The acceptance criterion is ≥10% improvement
versus baseline `c87339c`; the actual win is buried in noise.

Disposition: ROADMAP `P3.6` closed. Per-delete cost is already <5 µs
in the bench; no further win available without a structural change
(e.g. cache `extended_size` in the index entry, but Bucket is already
exactly 64 bytes / one cache line and has no padding to repurpose,
or batch deletes to amortize the index write lock).

### C-10. F-G7-018 — WriteMajority early-return on majority via mpsc — RESOLVED 2026-05-18

`ReplicationManager::replicate_batch` no longer joins every worker
before returning. Each live replica now gets a detached worker thread
that owns its transport for the round-trip and posts the outcome on a
per-batch `std::sync::mpsc` channel. The master reads from the channel
until either:

- enough ACKs have arrived to satisfy the `AckPolicy` (`WriteMajority`
  → `required_ack_count()` replica ACKs; `WriteAll` → all live ACKs),
  at which point the master **returns early** while the slow followers
  keep running in detached threads, or
- enough failures have arrived to make the quota unreachable.

Straggler join handles live in `pending_stragglers` so the next
state-mutating call drains them, restores the transport, and applies
the late outcome (idempotent — Ok ACKs only advance `last_acked` if
strictly higher; Down stays Down). The durable-log invariant is
preserved: every assigned sequence still reaches every replica
because (a) the worker continues to its `recv_ack` regardless of the
master's early-return, and (b) the next batch joins it before touching
the same transport again.

The new `tests/replication_tcp.rs::write_majority_early_return_*`
tests prove the win: 3-replica fan-out with one 500ms-slow replica
returns within ~5ms (master + 2 fast replicas = majority of 4 copies).
Pre-fix would have waited 500ms.

Files: `src/replication/manager.rs`,
`tests/replication_tcp.rs`.

### C-11. Cluster_tcp `wait_until` — done

The 15 fixed sleeps in `cluster_tcp.rs` were converted (commit
`db9fb00`). Estimated savings from the audit (~40-50 s) realised: full
test binary now 3 s. **No remaining sleep sites to convert.**

---

## D. Doc / repo cleanup

### D-1. ~~Stale audit docs at repo root~~ — RESOLVED 2026-05-18

`AUDIT.md` and `AUDIT_CODEX.md` (both dated `2026-05-06`) now carry a
top-of-file `> **Status: SUPERSEDED 2026-05-17.**` banner pointing
readers at `REVIEW_REPORT.md` + `_review/`. Kept as historical
artifacts per the user-preferred non-destructive option.

Resolved together with ROADMAP P4.2 (README "Status" section) and
P4.3 (per-phase `Status:` ledger in `phases/NN_*.md`).

### D-2. `_review/ROADMAP_TO_100.md`

Added this session — see for ordered execution plan.

---

## Status legend

- **A-***: production bugs (correctness or operator-visible). P1.
- **B-***: telemetry / config wire-up (functional but not visible / not enforced). P2.
- **C-***: deferred perf or refactor (no current correctness risk). P3.
- **D-***: doc / repo hygiene. P4.

---

## From P1.2 / P2.1 / P2.2 (G5 + G6 + G7 touch)

- **`InflightBytesLimiter::record_rejection` is sync-only** — the P2.2
  bump path is a single `fetch_add` on the per-thread counter reached
  through `DISPATCH_METRICS`. If `init_dispatch_metrics` has not run
  (single-binary tests bypass startup), the bump is a no-op. All
  production paths init it; documented at the call site. Footgun
  surfaces only if a second test harness asserts on this counter
  without calling `init_dispatch_metrics`.
