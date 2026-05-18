# Review follow-ups (drive-by avoided)

Per `_review/FIX_POLICY.md` "No drive-by fixes — anything unrelated →
`_review/follow_ups.md`". This file lists tightly-scoped follow-up
work surfaced while fixing the reviewed findings.

## From G1

- **F-G1-015 metric — RESOLVED** (P2.3 / B-1 / A-5). Added
  `corrupt_redo_entries_total: PaddedCounter` to `AllocatorMetrics` in
  `src/metrics.rs` and bumped it at every `tracing::error!` rejection
  site inside `replay_allocate` / `replay_free` in `src/allocator.rs`.
  Wired into the Prometheus `/metrics` endpoint as
  `teraslab_allocator_corrupt_redo_entries_total` and into the
  `/admin/top` JSON shape. Covered by
  `src/allocator.rs::tests::corrupt_redo_replay_bumps_metric` and the
  `tests/http_observability.rs::metrics_includes_new_telemetry_counters`
  regression guard.

- **F-G1-019 metric / warn — RESOLVED** (P2.3 / B-1 / A-5). Added
  `generation_wrap_warn_total: PaddedCounter` to `AllocatorMetrics`
  and a `GENERATION_WRAP_WARN_DELTA = 1u32 << 30` constant in
  `src/record.rs`. `generation_target_ahead` now emits a `warn`-level
  log and bumps the counter whenever the forward delta exceeds the
  threshold (the classification result is unchanged — telemetry only).
  Wired into the `/metrics` endpoint as
  `teraslab_allocator_generation_wrap_warn_total` and into
  `/admin/top`. Covered by
  `src/allocator.rs::tests::generation_wrap_bumps_warn_metric` plus
  the http-observability regression guard.

- **F-G8-004 SWIM ping_req metric — RESOLVED** (P2.4 / B-2 / A-5).
  Moved the process-wide `AtomicU64` `SWIM_PING_REQ_DROPPED_TOTAL` out
  of `src/cluster/swim.rs` and onto
  `SwimMetrics::swim_ping_req_dropped_total` in `src/metrics.rs`. The
  eviction site in `ping_req_forwarding_put` now bumps the new
  counter; the legacy `ping_req_dropped_total()` accessor is preserved
  as a thin wrapper so `tests/g8_ping_req_cap.rs` continues to work
  without import churn. Wired into the `/metrics` endpoint as
  `teraslab_swim_ping_req_dropped_total` and into `/admin/top`.
  Covered by
  `src/cluster/swim.rs::tests::ping_req_eviction_bumps_metric` and the
  updated `tests/g8_ping_req_cap.rs` which now installs the test
  `SwimMetrics` table before asserting eviction counts.

- **F-G1-012 nix/rustix port** — the hard-coded `BLKGETSIZE64` /
  `DKIOCGETBLOCKCOUNT` constants in `src/device.rs` are correct for
  the platforms we currently target (x86_64 / aarch64, Linux + macOS).
  Migrating to `nix::ioctl_read!` or `rustix::fs::ioctl_blkgetsize64`
  would mean adding a new dep (allowed under FIX_POLICY item 4) but the
  win is "future port to a 32-bit Linux variant doesn't silently
  ENOTTY". Filing as a follow-up because no live target hits the bug
  and adding `nix` is heavier than the surgical fixes in this round.

- **F-G1-002 typestate guard** — review recommendation was to either
  (a) wrap footer + CRC behind a single `write_footer_and_crc_direct`
  helper (kept as primary entrypoint) or (b) return a `#[must_use]`
  guard struct that panics on drop in debug builds if not consumed.
  The simpler (a) shape is implemented in this round. The
  `#[must_use]` typestate variant is a follow-up if more callers land
  that genuinely need to split the footer-write from the CRC restamp.

- **F-G1-003 atomic-chunk migration** — the BC-06/BC-07 fences are
  documented and the CRC remains the safety net, but Rust's strict
  memory model still flags the unsynchronised metadata read/write as
  UB-on-paper. A future migration to `AtomicU64::load(Relaxed)` /
  `AtomicU64::store(Relaxed)` for the metadata field chunks would make
  the read/write legal racing access. Out of scope for this round
  because the change touches every direct-path call site (ops/* G2
  files), not just io.rs.

- **F-G1-004 MemoryDevice aliasing** — `data: RwLock<Vec<u8>>` paired
  with `raw_ptr` aliases the same heap allocation, which is UB under
  Stacked Borrows / Tree Borrows. Production picks one path at a time
  via `as_raw_ptr().is_some()`, but `cargo miri` against the test
  suite will fail. Recommendation was to either drop the lock and
  route everything through `raw_ptr`, or drop `as_raw_ptr` for
  `MemoryDevice`. Both are wider changes than the review-fix round's
  surgical budget allows and would interact with G2 callers. Filed as
  follow-up; F-G1-017 removed the parallel `raw_len` field so the
  remaining aliasing concern is the only outstanding piece.

- **F-G1-016 rollback coalesce** — review recommendation was to
  coalesce on rollback even though the allocator is single-threaded
  and the coalesce can never happen today. The defensive change is
  cheap but the current state has no reachable bug, and the in-crate
  tests would not exercise it. Filing as a forward-looking follow-up
  if interior-mutability is ever introduced to the allocator path.
