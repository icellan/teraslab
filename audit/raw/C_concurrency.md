# Category C — Concurrency Audit (TeraSlab @ HEAD 1e5659b) — FINAL

## Method / completeness
Read and verified line-by-line (verbatim, full output delivered):
- `src/locks.rs` (entire, 303 lines).
- `src/ops/engine.rs`: lines 1-220 (struct + Atomic-apply invariant), 1009-1267
  (COMPLETE `validate_spend_multi`), 4080-4250 (all lock-free read paths), plus a
  1196-line line-numbered grep of every lock/generation/spent_count statement.
- `src/io.rs`: lines 1-237 (module header, `io_locks()` singleton, atomic chunk
  transfer helpers) + line-exact greps of every `io_locks()` call site and every
  `*_direct` fn signature.
- `src/server/dispatch.rs`: full `lock` grep + async/await counts.

Greps establishing structural facts (line-exact, full output):
- `dispatch.rs` and `engine.rs`: ZERO `async fn`; engine ZERO `.await`; dispatch
  exactly ONE `.await` (inside `REPL_RUNTIME.block_on(async {...})` on a blocking
  worker, dispatch.rs:1518).
- All four io.rs direct helpers acquire the record-level guard:
  `read_metadata_direct` (760)→`io_locks().read` (764); `write_metadata_direct`
  (799)→`.write` (805); `read_utxo_slot_direct` (842)→`.read` (850);
  `write_utxo_slot_direct` (876)→`.write` (883). The footer/CRC write wrappers
  also take `io_locks().write` (596, 626, 648, 672) and the FooterPendingCrc
  typestate holds the write guard across footer→CRC (io.rs:473).

CORRECTION to an earlier draft: there is NO placeholder/stub at io.rs:189. That
line is inside the real `atomic_store_u64_rmw` helper (io.rs:173-202). My earlier
"// ... existing code ... / let _ = 0;" note was a misread and is withdrawn.

---

## VERIFIED-OK (all primary checklist items)

1. **lock_stripes power-of-two + hash distribution — OK.** Both `StripedLocks`
   and `StripedRwLocks` do `next_power_of_two().max(16)`, `mask=count-1`, index
   `h & mask` (locks.rs:46-52, 65, 100-105, 130). Default 65536 already PoT. Txid
   stripe bytes [16..24] disjoint from bucket [0..8] / fingerprint [8..16].
   Tested: stripe_distribution (locks.rs:194), large-lock-count (locks.rs:209).

2. **Same-key serialization / no double-spend window — OK & tested.** Every
   mutation entry takes `self.locks.lock(&tx_key)` first and holds it across
   validate→apply (spend via the ValidatedSpend guard at engine.rs:1037; all
   other entries per grep). Test
   `tests/g2_atomic_apply.rs::concurrent_spend_same_utxo_yields_exactly_one_winner`
   (16×200 threads, exactly-one-winner) is non-vacuous.

3. **Multi-item batch lock ordering / deadlock — OK.** Engine mutation API is
   strictly single-key (one stripe locked+released per call). No engine-layer
   multi-key lock set ⇒ two-batch ABBA deadlock impossible for ordinary
   spend/create/set_mined; the dispatch layer iterates and calls per-key.
   Deterministic txid sort is therefore unnecessary at the engine layer.

4. **new_spent_count computed INSIDE the stripe lock — OK (full body read).**
   validate_spend_multi (engine.rs:1037-1267): guard at 1037, never dropped
   early; spent_count declared (1095), incremented in-loop (1173), committed into
   ValidatedSpend with pre_generation (1258-1260) while `_guard` (1254) still
   holds the lock. WAL write + on-device apply happen under one continuous lock
   via the carried guard. Idempotent-replay-critical field is lock-protected.

5. **No .await while holding a non-async lock — OK.** engine.rs/dispatch.rs are
   synchronous (0 async fn). The single dispatch `.await` is `h.await` on a
   JoinHandle inside `REPL_RUNTIME.block_on` on a blocking worker — it does NOT
   hold any engine stripe guard or `redo.lock()` parking_lot guard (those are
   acquired in the synchronous engine/redo path, not across this block_on).

6. **Lock-free reads + torn-read defense + mmap aliasing — OK.** The engine read
   paths (read_metadata/read_slot/read_slots/read_block_entry/get_spend) are
   intentionally stripe-lock-free but: (a) get torn-read protection from the
   io.rs `*_direct` helpers, ALL of which acquire the record-keyed
   `StripedRwLocks` (io_locks) read/write guard — proven line-exact above —
   closing the aarch64 NEON-memcpy torn-read window that CRC alone misses
   (io.rs:24-56); (b) defend the delete+create_at_offset aliasing race with a
   double `meta.tx_id == key.txid` re-check (read_slot 4124+4130, read_slots
   4147+4159, read_block_entry 4179+4199). The mmap region is thus never read
   non-atomically without the record RwLock, and the bulk transfer itself uses
   `AtomicU64`/`AtomicU8` chunked load/store (io.rs:112-237) so even miri sees no
   data race. Regression tests: direct_read_write_concurrent_stress... (io.rs:
   1409) and direct_footer_helpers_concurrent_stress... (io.rs:1695).

7. **Children-block child→parent lock relationship — OK.** Conflicting/deleted-
   children paths use allocate-out-of-lock then re-acquire the parent stripe lock
   ONLY to validate-snapshot-and-commit (parent_key locks at engine.rs:2962,
   3015, 3250, 3299), with `drop(_guard)` between phases and the documented
   drop-child-before-parent at 3647. No path holds two distinct stripe locks
   simultaneously. Tested by append_conflicting_child_lock_order (engine.rs:7030)
   which asserts the parent stripe lock is NOT held across allocator work.

---

## FINDINGS

### C-03 (LOW) — Stale/contradictory torn-read comment in engine.rs.
engine.rs:54-63 says lock-free reads "rely on the CRC32 over TxMetadata to detect
torn headers." That is misleading: per io.rs:24-56 (and the regression test) CRC
alone is empirically insufficient on aarch64; the actual torn-read defense is the
io.rs `io_locks()` StripedRwLocks wrapping every `*_direct` read. The CRC's real
job is corruption detection, and the tx_id re-check handles aliasing. Per project
Rule 6 (surface conflicts, don't average), update engine.rs:54-63 to credit the
io.rs record RwLock as the torn-read defense so a future maintainer does not
remove the io.rs locks believing CRC covers it (which would reopen a
double-spend-acceptance window on get_spend). Documentation hazard, not a runtime
bug — the locks ARE present and correct today.

(No CRITICAL/HIGH/MEDIUM concurrency findings. The two suspected money-loss
items — spend_multi counter-under-lock and the lock-free read torn-read defense
— were both confirmed correctly implemented.)

---

## Bottom line
The concurrency design is sound and well-tested. Striping is correct (PoT mask,
disjoint hash bytes). The same-key double-spend guard is real and stress-tested.
spend_multi's redo counter is computed and committed under one continuous stripe
lock (idempotent replay safe). The lock-free hot read paths are protected against
torn reads by the io.rs record-keyed RwLock on every `*_direct` helper (line-
exact verified) plus atomic chunked transfer plus a tx_id aliasing re-check. No
.await-while-locked hazard exists because the engine/dispatch request path is
synchronous. The only finding is a LOW documentation inconsistency (C-03).
