# Review follow-ups (drive-by avoided)

Per `_review/FIX_POLICY.md` "No drive-by fixes — anything unrelated →
`_review/follow_ups.md`". This file lists tightly-scoped follow-up
work surfaced while fixing the reviewed findings.

## From G1

- **F-G1-015 metric** — review recommendation (b) was to also drop a
  `corrupt_redo_entries_total` counter into `AllocatorMetrics`. Touched
  files would be `src/metrics.rs` (G6 ownership) plus the call sites.
  Behaviour-wise the `tracing::error!` already gives the operator the
  signal; the counter would let dashboards alert on non-zero
  recovery-time corruption-rejection rates. Out of G1 ownership and
  small-but-cross-cutting — keep as orchestrator follow-up.

- **F-G1-019 metric / warn** — review recommendation was to emit a
  `warn`-level log + metric when a record's generation jumps by more
  than `2^30` (approaching the wrap-classification ambiguity window).
  The classification function itself is correct and now pinned
  symmetrically by tests; the early-warning telemetry needs the same
  `AllocatorMetrics`-adjacent counter the G1-015 follow-up wants, so
  bundle both.

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
