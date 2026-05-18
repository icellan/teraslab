# Roadmap to 100% — TeraSlab finish plan

Sequenced execution plan to drive the project to a "no outstanding
correctness defects, no untyped errors, no stale telemetry, no
dead code" state. Each item has a fix shape, an acceptance gate, an
owner-group tag, and a size estimate.

State of `main` at `c87339c` (2026-05-17):

| Gate | Result |
|------|-------:|
| `cargo test --all` | **2092 / 0 / 0** |
| `cargo check --lib` | clean |
| `cargo check --bins` | clean |
| `cargo clippy --lib --no-deps` | **8 dead-code warnings** (all in `src/device_io/*`) |
| `cargo fmt --all -- --check` | clean |
| Worktrees | 0 |
| Disk free | 295 GB |

The 8 clippy warnings are the only remaining lib-level signal; the
test suite is green; production code compiles. Everything below is
incremental polish on a working system.

---

## Phase P1 — Production correctness (must finish first)

These are confirmed defects in production code paths. Treat as
release-blocking.

### P1.1 — Wire `cluster_id` end-to-end so legitimate scale-up isn't rejected

- **What**: `src/cluster/topology.rs::membership_change_is_safe` calls
  `ever_seen_check` after a cluster_id short-circuit, but every
  caller passes `proposal_cluster_id: None`. Result: in fresh
  production clusters, the third node's proposal is rejected because
  no committed voter has seen it before. Two integration tests fail
  on baseline; both were fixed only by surgical test-side pre-seeding.
- **Fix shape**:
  1. `src/config.rs` — persist `cluster_id: ClusterId` field; CLI
     accepts `--cluster-id`. Default `ClusterId::UNSET`.
  2. `src/cluster/topology.rs` — `TopologyAuthority::new_with_config`
     calls `set_cluster_id()` on startup. Already-existing serialize/
     deserialize carries the field.
  3. `src/cluster/coordinator.rs` (and any proposer-step site) — pass
     `Some(self.authority.cluster_id())` to `membership_change_is_safe`.
  4. `handle_propose` (`src/cluster/topology.rs`) — pass
     `Some(proposal.cluster_id)` instead of `None`.
  5. Remove the surgical test pre-seed once production wiring lands;
     the previously failing tests should pass on the new code path
     without `create_node_with_ever_seen`.
- **Acceptance**:
  - `cargo test --test cluster_tcp` continues green.
  - New test: `tests/g8_cluster_id.rs::scale_up_3_to_4_succeeds_without_pre_seed`.
  - New test: `tests/g8_cluster_id.rs::two_distinct_cluster_ids_refuse_superset`.
- **Group**: G8 + small G10 wiring.
- **Size**: M (~1 day).
- **Refs**: `_review/04_fixes_G8.md::F-G8-001`,
  `_review/10_cluster_tcp_fixes.md` §3, `_review/follow_ups.md` A-1.

### P1.2 — Replace 10 ms spin in accept loop with `mio::Poll` / self-pipe

- **What**: `src/server/mod.rs:264-272` sleeps 10 ms per shutdown
  check. Burns CPU, slows shutdown.
- **Fix shape**: Add `mio` (allowed per FIX_POLICY) or a self-pipe
  signalled from the shutdown flag's `Drop`. Convert the loop to a
  blocking `poll().with_timeout(longer)` that returns immediately on
  shutdown signal.
- **Acceptance**:
  - `tests/g10_lifecycle.rs::shared_shutdown_flag_visible_to_background_thread`
    still passes.
  - Manual probe: `cargo run --release` and SIGTERM — clean shutdown
    in <100 ms vs current 10 ms-bucketed.
  - New test: `accept_loop_idle_cpu_below_threshold` (asserts ≤0.5 %
    user-CPU on an idle listener over 5 s).
- **Group**: G5.
- **Size**: S (~half day).
- **Refs**: `_review/04_fixes_G6.md::F-G6-019`,
  `_review/follow_ups.md` A-2.

### P1.3 — Engine-side atomic apply (F-G5-022)

- **What**: Documented concurrency hypothesis in
  `src/server/dispatch.rs`. Fix belongs in `src/ops/`.
- **Fix shape**: Bracket apply paths (spend/unspend/set_mined) with
  a generation-bump that callers compare against, *or* return the
  before-image so callers can do their own optimistic-concurrency
  check.
- **Acceptance**:
  - Repro test in `tests/g2_atomic_apply.rs` demonstrates the race
    with the current code and passes with the fix.
  - If no repro is reachable today, downgrade to P3 documentation
    instead of code change.
- **Group**: G2.
- **Size**: M (depends on repro feasibility).
- **Refs**: `_review/04_fixes_G5.md::F-G5-022`,
  `_review/follow_ups.md` A-4.

---

## Phase P2 — Telemetry & wire-up gaps

Counters exist or are spec'd, but the production increment site is
missing. Operator-visible signals are the value.

### P2.1 — F-G7-001 increment `replica_unauthenticated_accept_total`

- **What**: Counter exists; no production site increments it.
- **Fix shape**: In `src/server/mod.rs::handle_connection_inner`,
  when the auth gate accepts a connection because
  `cluster_secret = None`, bump the counter and emit
  `tracing::warn!(target = "teraslab::security", "unauth replica accepted from {peer}")`.
- **Acceptance**: New unit test against `handle_connection_inner`
  asserts the counter increments by 1 per unauth accept.
- **Group**: G5 (call site) + G6 (counter already in `metrics.rs`).
- **Size**: S.
- **Refs**: `_review/04_fixes_G7.md::F-G7-001`,
  `_review/follow_ups.md` A-3.

### P2.2 — F-G6-020 `inflight_bytes_rejected_total`

- **What**: `InflightBytesLimiter::try_acquire` rejects silently.
- **Fix shape**: Add counter to `src/metrics.rs::ThreadMetrics`;
  bump in `src/server/mod.rs:53-85` on the rejection path.
- **Acceptance**: `tests/g5_inflight_limiter.rs::reject_bumps_counter`.
- **Group**: G5+G6.
- **Size**: S.
- **Refs**: `_review/follow_ups.md` B-3.

### P2.3 — F-G1-015 + F-G1-019 — allocator telemetry

- **What**: Two bundled counters:
  - `corrupt_redo_entries_total` (recovery-time corruption rejection)
  - `generation_wrap_warn_total` (record generation jumped >2³⁰)
- **Fix shape**: New `src/metrics.rs::AllocatorMetrics` group; bump
  in `src/allocator.rs::replay_free` and at the generation-jump site.
- **Acceptance**: Both counters expose via `/metrics`; existing
  `tracing::error!` / `tracing::warn!` calls unchanged.
- **Group**: G1+G6.
- **Size**: S.
- **Refs**: `_review/follow_ups.md` B-1, B-2.

### P2.4 — F-G8-004 promote SWIM drop counter

- **What**: `SWIM_PING_REQ_DROPPED_TOTAL` lives in `cluster::swim`
  rather than the metrics registry.
- **Fix shape**: Move to `src/metrics.rs::ClusterMetrics`; remove
  local static; bump from the same site.
- **Acceptance**: `/metrics` endpoint exposes
  `teraslab_swim_ping_req_dropped_total`.
- **Group**: G6+G8.
- **Size**: S.
- **Refs**: `_review/04_fixes_G8.md::F-G8-004`,
  `_review/follow_ups.md` A-5.

### P2.5 — F-G10-017 typed `CatchupError`

- **What**: `Err(String)` + substring match between
  `src/replication/durable.rs:728` and `src/bin/server.rs:1065`.
- **Fix shape**: Add `CatchupError { RedoReclaimed { from, to }, … }`
  enum; change `run_catchup_for_replica` signature; bin-side adopt
  the typed arm.
- **Acceptance**: Substring match deleted; existing catchup tests
  still pass; new test asserts the enum variant.
- **Group**: G7 (lib) + G10 (bin).
- **Size**: S.
- **Refs**: `_review/04_fixes_G10.md::F-G10-017`,
  `_review/follow_ups.md` B-4.

---

## Phase P3 — Dead code, deferred perf, refactor

Not load-bearing; address opportunistically. Each is independently
mergeable.

### P3.1 — Resolve `src/device_io/*` dead code (clippy clean)

- **What**: 8 dead-code warnings — `DeviceIo` trait, `create_device_io`,
  `BACKEND_ID`, `OpKind`, `PendingOp`, `SyncFallback` (+ `new`).
- **Fix shape**: Decision required:
  - **Option A**: Wire `SyncFallback` into the engine startup path
    behind a config flag (`use_async_io: false` → SyncFallback).
  - **Option B**: Delete the trait and the unused impl; reduce
    `device_io` to whatever paths are live.
  - **Option C**: `#[cfg(feature = "async-io")]` gate the trait so
    it's only compiled when the corresponding backend is wired.
- **Acceptance**: `cargo clippy --lib --no-deps -- -D warnings` clean.
- **Group**: G1.
- **Size**: S (B), M (A).

### P3.2 — `cargo miri test` clean (F-G1-003, F-G1-004)

- **What**: Two UB-on-paper issues in `src/io.rs` and
  `src/device.rs` (`MemoryDevice` aliasing).
- **Fix shape**:
  - F-G1-003: `AtomicU64` for metadata read/write chunks.
  - F-G1-004: drop `RwLock<Vec<u8>>` or drop `as_raw_ptr` for
    `MemoryDevice`.
- **Acceptance**: `MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly
  miri test --lib` runs to completion on the io+device test modules.
- **Group**: G1+G2.
- **Size**: L (touches every G2 call site).
- **Refs**: `_review/follow_ups.md` C-3, C-4.

### P3.3 — `nix`/`rustix` port for ioctl portability (F-G1-012)

- **What**: 32-bit Linux silently `ENOTTY`s. Currently no live target.
- **Acceptance**: Compile + run on `i686-unknown-linux-gnu` reports
  device size correctly.
- **Size**: S.

### P3.4 — Frame zero-copy (F-G5-011)

- **What**: `RequestFrame::decode` allocates full payload per frame.
- **Fix shape**: Lifetime-parameterise `RequestFrame<'a>` over a
  borrowed payload slice or switch to `bytes::Bytes`.
- **Acceptance**: Bench `codec_ops` shows ≥20 % fewer allocations on
  hot opcodes.
- **Size**: L (touches every handler).

### P3.5 — Streaming HMAC verify (F-G5-016)

- **What**: `cluster::auth::verify_frame` reads full payload before
  HMAC short-circuit.
- **Acceptance**: Slow-loris HMAC-mismatch microbench shows reject
  before payload-end.
- **Size**: M.

### P3.6 — F-G2-020 — `delete()` perf

- **Acceptance**: Bench `engine_remaining::delete` improves ≥10 % vs
  baseline `c87339c`.
- **Size**: M.

### P3.7 — F-G7-018 — WriteMajority early-return via mpsc — RESOLVED 2026-05-18

- **What**: `replicate_batch` joined all live replicas before
  returning; one slow follower dominated tail latency. Switched to
  detached worker threads + per-batch mpsc; master returns on first M
  ACKs, stragglers complete in background and the next batch joins
  them.
- **Acceptance**: `tests/replication_tcp.rs::write_majority_early_return_*`
  prove 3-replica fan-out with one 500ms-slow replica returns in
  ~5ms; before this fix the path waited 500ms (100x worse). Full
  details in `_review/follow_ups.md` C-10.
- **Size**: M.

### P3.8 — F-G1-002 typestate guard

- **What**: `#[must_use]` typestate variant for footer+CRC split.
- **Acceptance**: Compile-time prevents footer-without-CRC bug class.
- **Size**: S. Optional — only if a future caller surfaces the need.

### P3.9 — F-G1-016 rollback coalesce

- **What**: Defensive forward-looking only; no reachable bug today.
- **Acceptance**: Internal cleanup; doc the invariant.
- **Size**: S.

### P3.10 — F-G5-017 typed wire error codes

- **What**: Public-wire change — every client adapter.
- **Acceptance**: Bumped wire-protocol version; clients tagged.
- **Size**: L. Defer until a client requests it.

### P3.11 — F-G6-025 HTTP error body envelope

- **What**: `HttpErrorBody { code, message }` envelope.
- **Size**: M. Defer until a client depends on the body shape.

---

## Phase P4 — Documentation hygiene

### P4.1 — Resolve stale audit docs

- **What**: `AUDIT.md` and `AUDIT_CODEX.md` at repo root, both dated
  `2026-05-06`, pre-date the review campaign.
- **Fix shape**: Either:
  - Delete (REVIEW_REPORT.md + `_review/*` supersedes), or
  - Add a top-line banner pointing to `REVIEW_REPORT.md`.
- **Size**: XS.

### P4.2 — README "Status" section

- **What**: README has no machine-checkable "what works today" section.
- **Fix shape**: Add a `## Status` block enumerating: phases complete,
  test count, known limitations, license / disclaimer.
- **Size**: S.

### P4.3 — Phase-doc completion ledger

- **What**: `phases/` files describe intent; no per-phase "done" mark.
- **Fix shape**: Add a one-line status header to each phase doc
  (`Status: shipped / partial / blocked`).
- **Size**: S.

---

## Execution order recommendation

1. **Phase P1 first** (P1.1 → P1.2 → P1.3) — release-blocking.
2. **Phase P2** in any order; each is independent. Land before
   declaring "1.0". Suggested chunks of three telemetry items per PR.
3. **Phase P3.1** (clippy clean) before declaring "no warnings" SLA.
4. **Phase P3.2** (miri) before publishing to crates.io or running
   adversarial fuzz.
5. **Remaining P3** items as needed by benches.
6. **Phase P4** can ride along with any PR.

## Estimation summary

| Phase | Items | Aggregate size |
|-------|------:|----------------|
| P1 | 3 | M+S+M ≈ 2-3 dev-days |
| P2 | 5 | 5 × S ≈ 1-2 dev-days |
| P3 | 11 | mostly S-M; P3.2/P3.4/P3.10 are L |
| P4 | 3 | <1 day total |

## Definition of done

The project is "100 % finished" when:
- `cargo test --all` — green (already met).
- `cargo clippy --all-targets -- -D warnings` — green.
- `cargo miri test --lib` — green on io/device modules.
- All P1 + P2 items shipped; A-1, A-3, A-5 cleared from
  `_review/follow_ups.md`.
- README has a Status section and a publishable license stance.
- The `AUDIT*.md` confusion is resolved.

Anything below P3 — deferred-perf or hypothetical-future-caller items
— is acceptable to ship without; track in `_review/follow_ups.md` as
the long-tail backlog.
