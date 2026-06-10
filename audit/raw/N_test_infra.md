# Category N — Test Infrastructure Audit (TeraSlab @ HEAD 1e5659b)

Auditor scope: `tests/`, `benches/`, `teraslab-tests/`, `Cargo.toml`, `.github/workflows/`.
Method: read current code, confirmed line numbers, traced each checklist item to either
proving code or a finding. No claims taken on faith.

---

## FINDINGS

### N-01 (HIGH) — "Crash injection" simulation never exercises the real recovery path; its zero-data-loss assertion is structurally incapable of failing
Files:
- `tests/simulation/mod.rs:178-193` (`SimulatedNode::recover`)
- `tests/simulation/mod.rs:277-314` (post-crash re-create-from-reference)
- `tests/e2e_workload.rs:234-252` (`e2e_crash_injection_10_seeds`)
- `tests/e2e_workload.rs:719-738` (`simulation_crash_1pct`)
- `tests/e2e_workload.rs:741-759` (`simulation_combined_faults`)

What's wrong: The deterministic simulation advertises (mod.rs:1-4) "crash recovery and
replication under adversarial conditions." But `recover()` builds a brand-new empty
`MemoryDevice`-backed `Engine` (mod.rs:179-190) — it discards the prior device and does
NOT replay a redo log or rescan device bytes. Then the workload loop (mod.rs:277-314)
re-`create()`s every record straight out of the in-process `reference` HashMap, and
resets each record's spent-count to 0 in the reference at the same time (mod.rs:311).
So after a "crash", engine state and reference state are re-synchronised from the SAME
in-memory source. The later assertion `!result.data_loss_detected`
(e2e_workload.rs:245, 752) and `inconsistencies_found.is_empty()` (e2e_workload.rs:730)
therefore compare the reference model to an engine that was just rebuilt FROM that model.
A genuine recovery bug (redo replay losing an entry, torn record, freelist corruption)
cannot be observed here because no real recovery runs.

Why it matters: This is the suite's headline "crash safety" coverage by name. A reader
(or CI gate) sees `e2e_crash_injection_10_seeds` passing and concludes the recovery path
survives 1% per-op crash injection across 10 seeds. It proves only that the test's own
HashMap bookkeeping is self-consistent. The REAL recovery path is covered separately and
well by `tests/recovery_crash_boundaries.rs` and `tests/fault_injection.rs` (see
verified-OK), so the gap is "misleading coverage that masks regressions," not "no crash
testing at all" — but the misleading green is itself a hazard for a money-handling store.

Reproduction: In `tests/simulation/mod.rs:179`, the recover() path could instead reopen
the existing `self.device` through `teraslab::recovery::recover(...)` + a rebuilt index
(as `recovery_crash_boundaries.rs:118,177,239` does) and NOT clear/rebuild the reference.
With that change, inject a deliberate replay bug and the test would catch it; today it
cannot. There is no test that fails if recover() silently drops a committed record.

Suggested fix: Either (a) rewire `SimulatedNode::recover` to reopen the same device via
the production `recovery::recover` + redo replay and stop re-creating from the reference,
turning these into true end-to-end crash-recovery tests; or (b) if that is out of scope
for the in-memory sim, rename the tests/docs to drop "crash injection / data loss after
recovery" claims (they are really model-consistency tests) and rely on
`recovery_crash_boundaries.rs` / `fault_injection.rs` for the durability contract.

Confidence: high.

---

### N-02 (MEDIUM) — `io_error_probability` and `network_partition_probability` are dead config; `simulation_combined_faults` injects only crashes despite its name/doc
Files:
- `tests/simulation/mod.rs:28,30` (fields declared)
- `tests/simulation/mod.rs:40-41` (defaulted to 0.0)
- `run_with_faults` body `tests/simulation/mod.rs:240-497` (fields never read)
- `tests/e2e_workload.rs:741-759` (`simulation_combined_faults`, doc "Combined faults: zero data loss")

What's wrong: `run_with_faults` reads only `crash_probability` (mod.rs:262). Grepping the
module, `io_error_probability` and `network_partition_probability` appear only at their
declaration/default sites — they are never consulted, and `partitions_injected`
(mod.rs:58,245) is initialised but never incremented. `simulation_combined_faults`
(e2e_workload.rs:742) sets only `crash_probability: 0.005` and inherits the 0.0 defaults
for IO/partition, so "combined faults" injects exactly one fault class: crashes (and
crashes that don't really recover, per N-01).

Why it matters: The cluster/replication audit checklist item "chaos: random kills,
partitions, packet loss" is reported as covered by this framework, but I/O errors and
network partitions are simulated nowhere in it. The dead fields advertise capability the
harness does not have.

Reproduction: `grep -n io_error_probability tests/simulation/mod.rs` → declaration +
default only, no read site. Same for `network_partition_probability`.

Suggested fix: Either implement the two fault classes in `run_with_faults` (inject
`SpendError::StorageError` / device write failures on `io_error_probability`; drop a
replica/peer link on `network_partition_probability`) and increment `partitions_injected`,
or delete the unused fields so the config does not over-promise.

Confidence: high.

---

### N-03 (MEDIUM) — No property-based tests (proptest/quickcheck) for UTXO-conservation invariants
Files: `Cargo.toml:116-120` (`[dev-dependencies]` — no proptest/quickcheck/arbitrary);
repo-wide `grep -rni proptest|quickcheck tests src benches` → zero hits.

What's wrong: The strongest invariant checker present is the hand-driven
`tests/workload/verifier.rs` `StateVerifier` (verifier.rs:449-517), which cross-checks
`spent_utxos`, `utxo_count`, and per-slot status against an independent model — good, but
it runs over a fixed, hand-coded operation sequence and a custom xorshift generator
(`tests/workload/generator.rs`), not a shrinking property-based explorer. There is no
proptest/quickcheck strategy that generates arbitrary create/spend/setMined/freeze/delete
op sequences and asserts conservation invariants (e.g. "spent_utxos never exceeds
utxo_count", "a spent slot never becomes unspent without unspend", "create+delete+recreate
preserves accounting") with automatic minimisation of a failing case.

Why it matters: The audit brief flags absence of property-based UTXO-conservation tests as
a HIGH gap. For a store where a missed edge case loses money, randomised exploration with
shrinking is the standard tool to surface the off-by-one / ordering bug that hand-written
cases miss. The custom generator gives some coverage but no shrinking and a fixed op-mix.

Reproduction: `cat Cargo.toml | grep -i prop` → empty; `find . -name fuzz -type d` (excl.
target) → empty.

Suggested fix: Add `proptest` as a dev-dependency and write a strategy over
`Vec<WorkloadOp>` that drives the engine and asserts the conservation invariants after
each op, reusing the existing `StateVerifier` as the oracle. Run it in default CI (cheap
case count per-PR, larger under `TERASLAB_FULL_WORKLOAD`/nightly).

Confidence: high. (Severity MEDIUM rather than HIGH because a real, value-checking
model-based verifier and a seeded generator already exist; this is a coverage-depth gap,
not a total absence of invariant checking.)

---

### N-04 (MEDIUM) — No fuzz target for the wire parser; codec/frame coverage is hand-crafted unit tests only
Files: no `fuzz/` directory anywhere (verified); `src/protocol/codec.rs` (93 `#[test]`),
`src/protocol/frame.rs` (17 `#[test]`), `tests/p3_4_frame_zero_copy_allocs.rs`.

What's wrong: The wire decoder is the untrusted-input boundary (network frames →
`parse_request_header` → per-opcode decoders, incl. the variable-length create-batch path
`decode_create_batch_checked`). It is exercised only by crafted unit tests and one
allocation-count test (p3_4). There is no libfuzzer/cargo-fuzz target nor a
randomized/garbage-bytes loop feeding `decode_*` to confirm it never panics, never
over-allocates, and always returns a typed error on arbitrary input. (RECON notes the
codec is well-defended by `validate_batch_count` and per-section bounds, which I confirmed
exists — so this is hardening, not a known live bug.)

Why it matters: A panic or unbounded allocation reachable from a single malformed frame is
a remote DoS on the UTXO store. Hand-written tests cannot enumerate the malformed-input
space for the variable-length create path; a 1-hour fuzz run routinely finds the missed
bound that review misses.

Reproduction: `find . -path ./target -prune -o -type d -name fuzz -print` → empty;
`grep -rni 'fuzz\|libfuzzer\|cargo-fuzz' Cargo.toml` → empty.

Suggested fix: Add a `cargo-fuzz` target (`fuzz/fuzz_targets/decode_request.rs`) that
feeds arbitrary bytes to `frame::parse_request_header` + the per-opcode `decode_*_checked`
functions and asserts "Ok or typed Err, never panic." Wire it into the nightly workflow
with a bounded time budget.

Confidence: high.

---

### N-05 (LOW) — Cluster "chaos" is full-node shutdown + logic-level split-brain; no asymmetric partitions / packet loss / reorder on live links
Files: `tests/cluster_tcp.rs:466-469` (`shutdown_node` = full stop of cluster+server),
many `shutdown_node(...)` call sites; `tests/g8_split_brain.rs` (logic-level merge-defense
unit tests); `tests/g8_swim_replay.rs:115` (single UDP-reorder scenario, replay-counter
level, not transport level).

What's wrong: Node failure is modelled only as a clean full shutdown. There is no
interposer (toxiproxy / lossy socket / netem-style harness) that drops, delays, reorders,
or duplicates messages between still-running nodes, and no asymmetric partition (A can see
B but B cannot see A). Searched for `toxiproxy|lossy|drop_prob|proxy|netem|reorder` across
`tests/` and `teraslab-tests/` — only the single SWIM-replay reorder case
(g8_swim_replay.rs:115) at the application-counter level, not the transport.

Why it matters: Split-brain and quorum bugs most often surface under asymmetric partitions
and message loss between live nodes, not clean kills. The split-brain defenses
(`g8_split_brain.rs`) are tested as pure functions, which is good for the logic but does
not exercise the live-cluster path under a real partition. This is the weakest of the
chaos dimensions.

Reproduction: `grep -rniE 'toxiproxy|lossy|drop_prob|netem|reorder' tests teraslab-tests`
→ only g8_swim_replay.rs:115.

Suggested fix: Add a thin TCP/UDP proxy fixture (or feature-gated lossy transport) that
the multi-node `cluster_tcp` tests can route through, with configurable drop/delay/partition
between specific node pairs; assert quorum/split-brain invariants hold under it. Lower
priority than N-01..N-04 because the existing logic-level + clean-kill coverage is
substantial.

Confidence: medium (absence verified; severity is a judgement call — clean-kill + logic
coverage exists).

---

## VERIFIED-OK (checklist items confirmed correctly handled)

1. **Real crash/recovery boundary tests EXIST and are strong.**
   `tests/recovery_crash_boundaries.rs` drives the production
   `teraslab::recovery::recover` over a real `RedoLog` + on-device bytes at four precise
   crash boundaries (before redo fsync, after redo fsync/before record write, after record
   write/before replication, after replication/before intent clear) and asserts replayed
   record contents, slot states, and idempotency (lines 102-335). Not vacuous.

2. **In-process fault-injection (SIGKILL-equivalent) suite is real.**
   `tests/fault_injection.rs` (feature `fault-injection`) arms `SyncPoint`s at fsync/rename
   boundaries, catches the induced panic, tears down in-memory state, reconstructs from
   persistent bytes, and asserts post-recovery invariants — including partial-`writev`
   prefix consistency (fault_injection.rs:736) and "durable only after sync"
   (fault_injection.rs:884). Runs in CI via the dedicated step `ci.yml:102-106`.

3. **Both index backends ARE exercised by default `cargo test --all`.**
   `tests/integration.rs:435-439` and `:483-490` loop over
   `[Memory, Redb, FileBacked]`; redb path wired via `RedbDahIndex::open` /
   `RedbUnminedIndex::open` (integration.rs:78-92, 115-129). Plus lib-level
   `both_backends_*` and dedicated redb suites. Not default-only-memory.

4. **Model-based value-checking verifier exists.** `tests/workload/verifier.rs`
   `StateVerifier::verify_against` (verifier.rs:449-517) cross-checks `spent_utxos`,
   `utxo_count`, and per-slot status against an independent model — does NOT merely assert
   `.is_ok()`.

5. **No `.is_ok()`/`.is_err()`-only vacuous assertions in `tests/*.rs`.**
   `grep -rcE 'assert!\(.*\.is_ok\(\)\)|assert!\(.*\.is_err\(\)\)' tests/*.rs` → zero.
   Tests assert on returned values/error variants (e.g. server_tcp.rs:662 checks
   `ERR_ALREADY_SPENT`).

6. **Stress tests actually execute (not `#[ignore]`d).** `tests/stress_tests.rs` declares
   7 `#[test]` fns (stress_tests.rs:8-43) that call into `tests/stress/mod.rs`; those run
   in default `cargo test --all` (no `[[test]]` exclusion in Cargo.toml) at CI scale, and
   at full scale in nightly (`nightly.yml:25-26`). The stress bodies assert real engine
   state (e.g. mod.rs:175-181, 199-201, 347-348).

7. **e2e_workload + stress run in default CI.** Not feature-gated, no `harness=false`,
   default-discovered. `scale()` (e2e_workload.rs:37) runs the fast tier per-PR and the
   full tier under `TERASLAB_FULL_WORKLOAD=1` (nightly.yml:11).

8. **Double-spend rejected at the wire level.** `tests/server_tcp.rs:608,662` send a
   second spend and assert `ERR_ALREADY_SPENT`; `tests/integration.rs:782` covers
   conflicting→blocked→clear→succeeds.

9. **Benches compile-gated on every PR** (`ci.yml:116-120` `cargo bench --no-run --all`)
   and run nightly (`nightly.yml:36-37`). No bench bitrot.

10. **No `#[ignore]` tests anywhere** (consistent with RECON; re-confirmed no
    feature-cfg hiding of e2e/stress).
