# 11 — F-X-007 / BC-02 torn-read fix

## Bug

`io::tests::direct_read_write_concurrent_stress_never_returns_torn_data`
fails ~90 % of release-build runs on aarch64 (Apple Silicon M-series).
A reader observed `tx_version = 0xB1B2B3B4` (meta_b) paired with
`fee = 0xAAAAAAAAAAAAAAAA` (meta_a) — bytes from two different writes —
and the CRC32 check at the end of `TxMetadata::from_bytes` accepted the
mix as valid. The CRC-as-only-defense story documented at
`src/io.rs:206` was empirically false.

## Root cause

`write_metadata_direct` does `dst_slice.copy_from_slice(&buf)` — a
plain LLVM-emitted memcpy of 320 bytes. On AArch64 release builds the
NEON-based memcpy is non-atomic and may publish bytes in any order.
The on-disk CRC slot sits at offset 253 inside the header (not
4-byte aligned) so the NEON store-pair instructions can land the new
CRC bytes before — or in the middle of — the matching field bytes. A
concurrent reader sees the new CRC paired with mostly-old fields,
recomputes a CRC that coincidentally matches the byte mix, and
returns garbage to the caller.

The previous "fix" (R-029 / R-030) added `fence(Acquire)` on the
reader and `fence(Release)` on the writer, but Rust's memory model
does **not** establish happens-before through fences alone — only
through paired atomic load/store operations on the *same* address.
The plain memcpy was never atomic; the fences were placebo on this
codepath.

## Fix path chosen — option (i): record-level stripe lock

The smallest change that makes the test pass under N = 100 release-build
iterations is to put real mutual exclusion around the
`*_direct` writer↔reader pair. A new `StripedRwLocks` table in
`src/locks.rs` is keyed by `record_offset`:

- `read_metadata_direct`, `read_utxo_slot_direct` take the *shared*
  read guard for the duration of the read.
- `write_metadata_direct`, `write_utxo_slot_direct`, and the combined
  `*_and_crc_direct` wrappers take the *exclusive* write guard.
- The bare footer primitives (`write_*_footer_direct`,
  `write_crc_direct`, `write_block_entry_direct`) do **not** lock —
  the combining wrappers hold the guard once across the multi-step
  sequence so a reader cannot squeeze in between primitive calls.

The lock table is a process-wide singleton initialised lazily via
`OnceLock`. Stripe count is 65 536 (same as the engine's
`StripedLocks` default), so false-sharing between different records
is statistically negligible. The dead R-029 / R-030 fences in the
read and write paths were removed; their comments described a
guarantee Rust's memory model never provided.

## Rejected alternatives

1. **Atomic CRC slot.** Would require the CRC field to be at a
   4-byte-aligned offset; today it lives at offset 253, which is
   not aligned. Aligning it is an on-disk format change.

2. **Two-CRC / sequence-number layout.** Same on-disk format change
   penalty.

3. **Take the engine's `StripedLocks` on the engine read paths.**
   The original F-X-007 recommendation. Closes the engine-level race
   but does not close the `_direct` raw helper contract that the
   regression test exercises — the test bypasses the engine. The
   io-level stripe table covers both.

## Test verification

- Regression test passes 100 / 100 in release mode (was failing
  ~90 % of runs before the fix).
- Three new `locks::tests::striped_rwlocks_*` tests pin the new
  table's semantics: stripe count rounds up to a power of two with a
  floor of 16, concurrent readers do not block, writer excludes
  readers, distinct page-aligned offsets hash to distinct stripes.
- Full `cargo test --lib` failure count is unchanged from baseline
  (37 pre-existing failures, none in `io::` or `locks::`).

## Performance impact

Benchmark: `engine_remaining/get_spend/get_spend_one` (the hottest
pure-read path — a single `read_metadata_direct` + `read_utxo_slot_direct`).

| Build    | Time         | Δ vs. baseline |
| -------- | ------------ | -------------- |
| baseline | 207 ns       |   —            |
| fixed    | 242 ns       | +17–20 %       |

That is `~35 ns` of `parking_lot::RwLock` read-acquire overhead per
metadata read. Mixed-mutation benches (`unspend_one`, `unfreeze_one`)
show no measurable regression — the lock cost is dominated by the
device I/O on those paths. The `+20 %` upper bound sits at the
threshold the orchestrator flagged for reporting; correctness over
speed on the read path was the explicit guidance.

## Files touched

- `src/io.rs` — adds the `io_locks()` accessor, guards each `*_direct`
  helper (or its combining wrapper) with read / write guards,
  removes the misleading R-029 / R-030 fence comments.
- `src/locks.rs` — new `StripedRwLocks` type with read/write guard
  acquisition and three coverage tests.

No files outside `src/io.rs` and `src/locks.rs` were touched. The
engine call sites in `src/ops/engine.rs` automatically benefit
because they route through the `*_direct` helpers; no changes there.
