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

- **F-G1-003 atomic-chunk migration — RESOLVED (FIXED, partial)** —
  `read_metadata_direct`, `write_metadata_direct`, `read_utxo_slot_direct`,
  and `write_utxo_slot_direct` in `src/io.rs` now perform the bulk
  byte transfer through `AtomicU64::load(Relaxed)` / `store(Relaxed)`
  chunks (with `AtomicU8` head/tail for misalignment) via the
  `atomic_load_into` / `atomic_store_from` helpers. The pre-fix
  `slice::from_raw_parts(_mut)` retag would race miri's data-race
  detector against the concurrent writer/reader path in
  `direct_read_write_concurrent_stress_never_returns_torn_data`;
  the atomic chunked transfer eliminates the race at the abstract-
  machine level. Public function signatures are unchanged so the G2
  `ops/*` call sites are untouched.

  Still DEFERRED — the targeted "footer" helpers
  (`write_mutation_footer_direct`, `write_spend_footer_direct`,
  `write_mined_footer_direct`, `write_block_entry_direct`,
  `write_crc_direct` and their `_and_crc_direct` wrappers) still use
  non-atomic `ptr::copy_nonoverlapping` for the 1-21 byte field
  edits. They are NOT exercised concurrently by any current miri
  test (the stress test only uses `write_metadata_direct`), but in
  production they race with the same atomic-chunked `read_*_direct`
  paths. Atomicising those helpers is a wider change because each
  field write needs to land at a field-aligned offset with its
  field-specific width — leaving as a follow-up because the existing
  test surface stays clean.

- **F-G1-004 MemoryDevice aliasing — RESOLVED (FIXED)** — Option A:
  `MemoryDevice` no longer holds `parking_lot::RwLock<Vec<u8>>`. The
  backing allocation is acquired via `vec![...].into_boxed_slice()` +
  `Box::into_raw`, stored as a raw `*mut u8` with a sibling `len: u64`,
  and reconstituted into a `Box<[u8]>` inside a new `Drop` impl. The
  pre-fix double-alias (Vec reborrow through the lock paired with the
  live `raw_ptr`) is gone, so the Stacked-Borrows tag rooted at
  `raw_ptr` survives the construction site. `pread` / `pwrite` rebuild
  a short-lived slice from `raw_ptr` for each call instead of going
  through the lock; the two `memory_device_lock_*_panic_*` tests now
  exercise `parking_lot::RwLock` semantics on a side-instance and the
  device's continued usability post-panic, with their comments
  updated to match.

- **F-G1-016 rollback coalesce** — review recommendation was to
  coalesce on rollback even though the allocator is single-threaded
  and the coalesce can never happen today. The defensive change is
  cheap but the current state has no reachable bug, and the in-crate
  tests would not exercise it. Filing as a forward-looking follow-up
  if interior-mutability is ever introduced to the allocator path.
