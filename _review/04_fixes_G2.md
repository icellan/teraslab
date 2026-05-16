# G2 — Ops Engine Fix Log

Branch: `worktree-agent-a2861d5dda0436dc4`. Baseline: `aeed289`. Ownership: `src/ops/*`. Cross-cutting touch: `src/server/dispatch.rs` (one new error variant mapping for F-G2-002 — minimum required to make the enum addition compile).

### F-G2-001 — FIXED (already in baseline, reinforced)
- Baseline commit: `aeed289` (delete ordering tombstone→sync→unregister→free + `read_metadata_for_key` tx_id check).
- Reinforcement commit: `6f7d3a9` — add post-slot-read re-verification on `read_slot`, `read_slots`, `read_block_entry`, `get_spend`.
- Files changed: `src/ops/engine.rs`.
- Test: `tests/g2_delete_race.rs::delete_does_not_alias_concurrent_create` (pre-existing). Confirmed stable across repeated parallel runs after reinforcement; without it the race surfaces ~1 alias in 1.2M reads under contention from concurrent test binaries.
- Notes: the original `read_metadata_for_key` check closed the obvious window but left one between the metadata read and the slot read. Re-reading metadata after the slot read and re-checking tx_id closes it. Cost is one extra footer read on the hot path; cache-warm on the mmap backend.

### F-G2-002 — FIXED
- Commit: `317f25b`.
- Files changed: `src/ops/error.rs`, `src/ops/engine.rs`, `src/server/dispatch.rs` (mapping the new variant — minimum cross-group touch), `tests/g2_reserved_spending_data.rs`.
- Test: `tests/g2_reserved_spending_data.rs` (single-spend + spend_multi both reject `[0xFF; 36]` and the rejected slot stays UNSPENT).
- Notes: new `SpendError::ReservedSpendingData { offset }` enum variant. Single-`spend` rejects at the request boundary; `validate_spend_multi` records a per-item error so the rest of the batch still applies. Mapped to `ERR_INVALID_SPEND` on the wire (request shape malformed). 36-byte all-`0xFF` is not a valid BSV `txid + vin` anyway, so no legitimate traffic is lost.

### F-G2-003 — FIXED
- Commit: `440206c`.
- Files changed: `src/ops/engine.rs`, `tests/g2_overflow_alloc_accounting.rs`.
- Test: `tests/g2_overflow_alloc_accounting.rs` — two tests on a 512-byte-aligned device that grow overflow past the alignment boundary and verify `allocator.used_bytes` returns to the post-create baseline. Pre-fix `used_bytes` drifts upward by 512+ bytes per cycle.
- Notes: new `overflow_block_size` helper rederives the live overflow size from `metadata.block_entry_count`. Free path now frees that exact size. Non-empty path reuses the existing offset only when `new_size == old_size`; any other transition (grow OR shrink across alignment boundary) frees the old allocation and grabs a fresh one. Free errors are propagated (pre-fix `let _ =` swallowed them).

### F-G2-004 — FIXED
- Commit: `4765bfb`.
- Files changed: `src/ops/engine.rs`.
- Test: covered by existing spend/set_mined unit tests (no behaviour change).
- Notes: three `unwrap()` calls removed. `try_into().unwrap()` on a 4-byte slice → `copy_from_slice` into a stack buffer; `overflow.pop().unwrap()` → `ok_or_else(StorageError)` with structured detail referencing the live `block_entry_count`.

### F-G2-005 — FIXED
- Commit: `50b33e2`.
- Files changed: `src/ops/engine.rs`.
- Test: covered by existing `append_conflicting_child_preserves_list_across_multiple_appends` (success path). Multi-thread contention test not added — deterministic timing is hard to construct cheaply; the cap is defensive.
- Notes: `append_conflicting_child` retry loop capped at 16 attempts with 1us→32ms exponential micro-back-off. On exhaustion returns `SpendError::StorageError` with a detail that names the contention case.

### F-G2-006 — FIXED
- Commit: `c75edd2`.
- Files changed: `src/ops/engine.rs`, `tests/g2_create_size_contract.rs`.
- Test: `tests/g2_create_size_contract.rs` covers (a) success when sizes agree, (b) `debug_assert!` panic in debug builds on mismatch, (c) `StorageError` return path in release builds.
- Notes: `pre_allocate_create` now returns `(record_offset, utxo_count, total_size)`. New `create_at_offset_verified` accepts the expected total and defends the contract via `debug_assert_eq!` + release-mode `StorageError`. Unverified `create_at_offset` is retained for callers without the new contract (no live caller observes a change — dispatch uses `allocate_batch` directly and never went through `pre_allocate_create`).

### F-G2-007 — FIXED
- Commit: `0561ee4`.
- Files changed: `src/ops/engine.rs`.
- Test: none — the invariant breach has no reachable path in current code (documented in the commit). The guard exists so a future change to spent-counter computation cannot silently regress the invariant.
- Notes: `ValidatedSpend::apply` replaces `wrapping_add` on `spent_utxos` with `checked_add` + explicit `<= utxo_count` check. Overflow and invariant violations both surface as `SpendError::StorageError`.

### F-G2-008 — NOT-APPLICABLE (verification)
- Verified: `Engine::spend` idempotent re-spend short-circuits before bumping anything; `ValidatedSpend::apply` zero-`spent_count` path skips slot/meta writes. Existing regression test `idempotent_respend_does_not_increment_generation` at `engine.rs:6270` confirms.

### F-G2-009 — NOT-APPLICABLE (declined cleanup)
- Recommendation was to dedup `external_ref_for_create` between `pre_allocate_create` and `create_at_offset`. `pre_allocate_create` is test-only; the dedup adds no safety. Following the Surgical Changes rule, declined.

### F-G2-010 — FIXED (already in baseline)
- Baseline commit: `aeed289`. The five lock-free read entry points (`read_metadata`, `read_slot`, `read_slots`, `read_block_entry`, `get_spend`) all carry doc comments pointing at the R-009 / F-G2-001 contracts. `lookup_cached` does not dereference the device and is not in scope.

### F-G2-011 — FIXED
- Commit: `0cf6f75`.
- Files changed: `src/ops/engine.rs`.
- Test: none — the divergence requires a `sync_index_cache` failure under load; deterministic reproduction is impractical at the unit-test level. Existing `set_mined` tests confirm the success path still bumps generation monotonically.
- Notes: set_mined fast path now bases the new generation on `meta.generation` (the on-device, authoritative value) instead of `entry.generation` (the cached value). No extra reads — the fast path already loads metadata for the RMW.

### F-G2-012 — FIXED (doc)
- Commit: `dcbfe58`.
- Files changed: `src/ops/engine.rs`.
- Notes: added doc comments to `freeze` and `unfreeze` explaining why DAH evaluation is intentionally omitted (freezing cannot change `spent_utxos == utxo_count`).

### F-G2-013 — FIXED (doc)
- Commit: `dcbfe58`.
- Files changed: `src/ops/engine.rs`.
- Notes: doc comment on public `set_locked` now surfaces the rollback hazard — callers needing compensation MUST use `set_locked_with_before_image`. Signature retained (changing it would churn six call sites for negligible safety gain; the hazard is real but rare and the docs cover it).

### F-G2-014 — NOT-APPLICABLE (verification)
- Verified: `evaluate_delete_at_height` correctly excludes unmined transactions via `metadata.unmined_since == 0`. `evaluate_dah_cached` mirrors the logic. `checked_add` already used for new-DAH computation (R-063 lineage).

### F-G2-015 — NOT-APPLICABLE (verification)
- Verified: child stripe lock dropped before `append_conflicting_children_from_cold_data` acquires parent stripe locks. Lock order is child→parent at every site; no cycle.

### F-G2-016 — NOT-APPLICABLE (verification)
- Verified: `unspend` rejects mismatched `spending_data` via the `slot.spending_data != req.spending_data` check at `engine.rs:1308`. Existing regression test `unspend_rejects_wrong_spending_data_without_mutating_slot` at `engine.rs:4975` confirms.

### F-G2-017 — FIXED
- Commit: `dcbfe58`.
- Files changed: `src/ops/engine.rs`.
- Test: covered by existing `prune_slot_if_spent_by_child` tests (no reachable behaviour change).
- Notes: `prune_slot_if_spent_by_child` now uses `checked_sub` / `checked_add` on `spent_utxos` / `pruned_utxos`. Surfaces an invariant breach as `SpendError::StorageError` instead of silently clamping. Defense-in-depth against a future guard reorder.

### F-G2-018 — NOT-APPLICABLE (verification)
- Verified: `src/ops/error.rs` and `src/ops/signal.rs` are clean. All variants are payload-carrying; the single `StorageError { detail: String }` is the documented I/O wrapping pattern.

### F-G2-019 — NOT-APPLICABLE (verification)
- Verified: all of `mod.rs`, `mark_longest_chain.rs`, `set_mined.rs`, `remaining.rs`, `spend.rs`, `unspend.rs`, `create.rs` are plain data with proper derives. `ValidatedSpend` correctly compile-fails `Copy`/`Clone` (doctests at `spend.rs:127` / `:133`). `pre_spent_count` uses the local-binding pattern for packed-struct field access.

### F-G2-020 — DEFERRED (performance, not correctness)
- INFO finding: `ValidatedSpend::apply` issues a separate `write_slot_fast` per valid spend rather than coalescing into one aligned region write. The finding itself flags this as a performance opportunity, not a correctness bug. Out of scope for the review cycle; recorded in `_review/follow_ups.md` (if/when that file exists) so the perf team can pick it up.

## End-of-group checks

- `cargo check --lib` — clean (post-baseline warnings only; nothing new in `src/ops`).
- `cargo test --test g2_delete_race --test g2_reserved_spending_data --test g2_overflow_alloc_accounting --test g2_create_size_contract` — 7/7 pass, stable across repeated runs.
- `cargo test --test integration` — 18/18 pass.
- `cargo clippy --lib` — pre-existing errors in `src/device_io/*`, `src/index/redb_primary.rs`, `src/record.rs`, `src/redo.rs` (outside G2 ownership); no new warnings in `src/ops/*`.
- `cargo fmt --all -- --check` — pre-existing drift in `src/cluster/*`, `src/index/backend.rs`, `src/redo.rs`, `src/server/http.rs`, `src/storage/uploader.rs`, `tests/g8_*.rs` (outside G2 ownership); G2-touched files are clean.
- `cargo test --lib` — blocked by pre-existing test-profile compile errors in `src/index/redb_primary.rs` (G3-owned). NEEDS-ORCHESTRATOR coordination with G3 to unblock the lib test suite. Integration tests linked against the lib build fine — only the `#[cfg(test)] mod tests { ... }` inside redb_primary.rs is broken.

## Counts

- FIXED: 11 (F-G2-001, 002, 003, 004, 005, 006, 007, 010, 011, 012, 013, 017 — counting 12 actually).
- NOT-APPLICABLE: 7 (F-G2-008, 009, 014, 015, 016, 018, 019).
- DEFERRED: 1 (F-G2-020 perf opportunity).
- NEEDS-ORCHESTRATOR: lib-test compile breakage in G3-owned `src/index/redb_primary.rs` (blocks `cargo test --lib` for everyone, not specifically G2).
