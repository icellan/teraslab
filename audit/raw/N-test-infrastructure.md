# Category N — Test Infrastructure (2026-06-11)

Scope reviewed: `tests/` (54 files, 405 integration tests), `src/**/#[cfg(test)]` (1,838 unit tests), `fuzz/`, `.github/workflows/{ci,nightly,release}.yml`, `src/fault_injection.rs`, `tests/simulation/`, `tests/net_proxy/`, `tests/stress/`, `tests/workload/`, `teraslab-tests/` (Docker E2E, 17 scenarios).

Companion deliverables: `audit/coverage-matrix.md`, `audit/error-codes.md`.

---

### [HIGH] Real-SIGKILL crash, chaos, and split-brain Docker scenarios exist but never run in any CI tier

**Location**: `teraslab-tests/run_all.sh` L25–28; `.github/workflows/ci.yml` L166–168; `.github/workflows/nightly.yml` L65–67; `.github/workflows/release.yml` (whole file)

**What's wrong**: The Docker E2E suite has the only tests in the repository that kill a real server process with SIGKILL and verify cluster-level data correctness afterwards: `scenario_12_concurrent_failures`, `scenario_13_data_migration_under_load`, `scenario_14_split_brain_prevention`, `scenario_15_crash_recovery_correctness`, `scenario_16_chaos`. `run_all.sh` assigns 12–15 to the `weekly` tier and 16 to `release` — but no workflow ever invokes either tier. CI runs `--tier pr` (scenarios 01–03), nightly runs `--tier nightly` (01–11, 17), and `release.yml` runs only `cargo test --all` plus binary builds (no `run_all.sh` at all). `teraslab-tests/results/` shows these scenarios were last run manually (timestamped log dirs); nothing guarantees they still pass on HEAD.

**Why it matters**: Every in-tree crash test is an in-process simulation (panic at a sync point, or drop-and-rebuild of engine state over a surviving `MemoryDevice`). The documented limitation in `src/fault_injection.rs` is explicit: "A panic here is NOT a real SIGKILL." Real-kill semantics — page-cache loss on a real filesystem, partially flushed redb files, a replica mid-stream when the master dies — are exactly what scenarios 12/15/16 exist to cover, and for a money-critical UTXO store, "crash-recovery correctness test exists but is never executed" is functionally the same as "does not exist."

**Reproduction**: `rg -n "weekly|release" teraslab-tests/run_all.sh .github/workflows/*.yml` — the tiers appear only in the script, never in a workflow.

**Suggested fix**: Add `--tier weekly` on a weekly cron (or fold 12/14/15 into the nightly tier — they fit the 90-min budget), and make `release.yml` run `--tier release` before publishing artifacts.

**Checklist disposition**: Cluster chaos tests — partially realistic (see LOW note below for the live in-tree fixture); the realistic SIGKILL half is orphaned. Crash-injection "kill process at random points" — does not exist in executed CI anywhere.

---

### [MEDIUM] In-tree crash injection never explores random intra-op kill points — only hand-picked boundaries plus op-granularity random crashes

**Location**: `src/fault_injection.rs` (SyncPoint enum); `tests/fault_injection.rs`; `tests/recovery_crash_boundaries.rs`; `tests/simulation/mod.rs` L440–445; `tests/e2e_workload.rs::e2e_crash_injection_10_seeds`

**What's wrong**: Two complementary mechanisms exist and both are good as far as they go: (1) named sync-points (`BeforeRedoFsync`, `AfterRedoFsync`, before-data-pwrite, before-redb-commit, hashtable-resize rename, freelist, secondary commit) armed via thread-local `FaultMode::PanicAt` — deterministic, hand-picked; (2) the seeded simulation framework, which crashes with per-op probability and replays through the REAL `recovery::recover_all_with_allocator` pipeline against a never-resynced reference model (the N-01 fix is real — I verified the model is not re-seeded after recovery). But the random dimension only selects *which op* precedes a crash (plus FlakyDevice I/O errors that force a crash after WAL fsync); *where inside* the write pipeline the crash lands is always one of the named points. There is no mode that picks a random `SyncPoint` per crash, and no exploration of crash windows the developers didn't think to name (e.g. between the two redb commits of a batched secondary update — covered only because someone hand-wrote `crash_after_batched_redo_fsync_before_both_redb_commits`). Coverage is also op-skewed: spend/create/set_mined/mark_longest_chain have dispatch-level crash-boundary tests; unspend/freeze/reassign/set_conflicting/set_locked/preserve_until/delete only have replay-level unit tests in `src/recovery.rs` (the redo entry is constructed by hand, not produced by a crashing dispatch path — a dispatcher that writes a wrong redo entry would pass).

**Why it matters**: Hand-picked boundaries verify the invariants the author already knew about. The bugs that survive to production are at the boundaries nobody named. Torn-WAL is covered (`tests/fault_injection.rs::before_redo_fsync_crash_after_partial_writev_returns_consistent_prefix` — genuinely good test), so the residual risk is moderate, but the WAL-first contract for 7 of 12 mutation opcodes rests on "the dispatcher writes the same redo entry the recovery test hand-built."

**Reproduction**: `rg -n "PanicAt|SyncPoint::" tests/ src/ | grep -v fault_injection.rs` — every arm site names a constant point; `rg -n "crash_probability" tests/simulation/mod.rs` — the only randomness is per-op.

**Suggested fix**: (a) add a simulation mode that arms a seeded-random `SyncPoint` per crash; (b) add dispatch-driven crash-boundary tests for the 7 uncovered mutation ops (drive `handle_request`, crash at `AfterRedoFsync`, recover, assert) — the harness already exists.

**Checklist disposition**: Crash-injection tests — present and real, but hand-picked-boundary + between-op random only; no random-point process kill in the cargo tree (see HIGH finding for the Docker half).

---

### [MEDIUM] Fuzzing is manual-only: the cargo-fuzz target is never run in CI

**Location**: `fuzz/` (target: `fuzz/fuzz_targets/decode_request.rs`); `.github/workflows/ci.yml`, `nightly.yml` (no fuzz job)

**What's wrong**: A well-built cargo-fuzz crate exists (covers frame parsing plus all 13 `decode_*_checked` decoders at two batch caps; 362-entry committed corpus; `fuzz/artifacts/decode_request/` is empty, i.e. no known-crash backlog). It requires nightly Rust and a manual `cargo +nightly fuzz run`. No workflow runs it — not even time-boxed on the nightly cron, which the README's own `-max_total_time=600` example makes trivial. Mitigation that IS in CI: `tests/wire_fuzz_smoke.rs` is a genuinely strong deterministic harness (seeded random + structure-aware mutations of valid payloads, per-decoder invocation counters asserting both Ok and Err paths are reached, decoder-set drift detection). Residual gap: both halves fuzz *decoders only* — `handle_request` dispatch (opcode × malformed payload × connection state) and the streaming state machine are not fuzzed; the slow-loris and `g5` tests cover a few hand-picked hostile sequences.

**Why it matters**: The wire decoders are the untrusted boundary of a server that holds UTXO state for real money. A decoder regression introduced after the last manual fuzz run would only be caught by the smoke test's fixed mutation strategies, never by coverage-guided exploration.

**Reproduction**: `rg -in "fuzz" .github/workflows/` → no matches.

**Suggested fix**: Add a nightly job: `cargo install cargo-fuzz && cargo +nightly fuzz run decode_request -- -max_total_time=600 -seed=...`, uploading any artifacts. Consider a second target that drives `handle_request` with arbitrary (opcode, payload) pairs.

**Checklist disposition**: Fuzz targets for wire protocol parser — exists, NOT in CI; deterministic smoke half IS in CI.

---

### [MEDIUM] Property-based tests never generate the hostile inputs that guard money: wrong utxo_hash, coinbase, FrozenUntil, ignore-flags, reserved sentinel

**Location**: `tests/property_utxo.rs` L96–113 (generators), L136–154 (op strategy)

**What's wrong**: The harness itself is excellent — a model-based differential test over a deliberately colliding keyspace (4 txids × ≤4 slots × 3 spenders) with op-by-op outcome equality *including error payloads* and full final-state equivalence (per-slot status/spending_data, counters, flags, mined sets, deleted-records absence). The four headline invariants (spent-exactly-once, idempotent re-spend, wrong-data unspend never mutates, delete-stays-deleted) are real and pinned deterministically too. But the generators are friendly in exactly the dimensions that matter most: `utxo_hash(tx, vout)` is always the CORRECT hash, so `UtxoHashMismatch` — the check that prevents spending someone else's output — has zero property coverage; `spending_data` is constructed to never be the reserved all-0xFF sentinel (deliberately, per the comment), so the F-G2-002 brick-guard is excluded; no coinbase records (`is_coinbase: false` always), no reassign/FrozenUntil ops, no `ignore_conflicting`/`ignore_locked` flag variation, no preserve_until/delete_eval interplay. There is also no property over crash-replay (generate ops → crash at random point → recover → model equality); that exists only as the separate simulation framework, which checks far weaker invariants (`utxo_count`/`spent_utxos`/slot status, not error-outcome equality).

**Why it matters**: Hash-mismatch rejection IS the UTXO-conservation property at the trust boundary; a regression that accepts a wrong-hash spend would sail through every property run. The error-paths the model does cover are the ones the engine was already known to handle.

**Reproduction**: `rg -n "utxo_hash|0xFF|is_coinbase" tests/property_utxo.rs` — hash is deterministic-correct, sentinel avoided, coinbase constant-false.

**Suggested fix**: Add a `wrong_hash: bool` (and `wrong_sentinel`, `is_coinbase`, `ignore_*`) dimension to the op strategy and extend the model's validation-order prediction; merge the simulation's crash/recover step into `run_sequence` as an optional random interleave.

**Checklist disposition**: Property-based tests — present, right core invariants, hostile-input generators incomplete.

---

### [MEDIUM] Only the in-memory index backend is exercised by 99% of the suite; no server/TCP/cluster/stress test ever runs on Redb or FileBacked

**Location**: `tests/integration.rs::backend_modes_create_spend_and_reopen`, `::backend_modes_secondary_indexes_survive_reopen` (the ONLY tests/ iterating all three `IndexBackendMode`s); every other harness (`tests/server_tcp.rs` L40, `error_code_conformance.rs` L40, `cluster_tcp.rs`, `property_utxo.rs` L73, `stress/`, `e2e_workload.rs`, `simulation/`) constructs `Index::new(...)` = memory hashtable

**What's wrong**: Three primary backends exist (Memory, Redb, FileBacked) plus on-disk DAH/unmined secondaries. Exactly two integration tests iterate them (create/spend/reopen and secondary-survival — both real, with restart verification). Everything else — the entire wire-protocol surface, replication, cluster routing, migration, recovery boundaries, stress, property tests — runs Memory only. Redb has 42 unit tests in `src/index/redb_primary.rs` (+30/35 for DAH/unmined), so the backend is not untested, but backend-specific behavior under the *production access patterns* (concurrent dispatch, crash-replay reconciliation against a redb snapshot — `tests/secondary_two_phase_durability.rs` does cover redb secondaries, credit where due) is thin, and zero TCP tests run against a redb-backed server.

**Why it matters**: The on-disk backends are presumably what production runs (the in-memory primary requires a full device scan rebuild on restart). A redb-only bug in, say, concurrent `set_mined` overflow handling under dispatch parallelism would be invisible to the whole suite.

**Reproduction**: `rg -ln "IndexBackendMode::Redb" tests/` → `integration.rs`, `g10_config.rs` (config parsing) only.

**Suggested fix**: Parameterize one representative TCP test file and the stress harness over `IndexBackendMode`, or add a nightly env-var (`TERASLAB_TEST_BACKEND=redb`) honored by the shared test-server constructors.

**Checklist disposition**: Does cargo test cover both index backends — NO for the integration surface; unit-level only.

---

### [MEDIUM] Five documented client-facing wire error codes have no behavioral test; the wire tests covering two of them assert only STATUS_ERROR

**Location**: `src/protocol/opcodes.rs` (codes 28, 29, 32, 33, 34); `tests/server_tcp.rs::invalid_opcode_returns_error` (~L1623, asserts status only), `::malformed_payload_returns_error` (~L1657, status only), `::batch_exceeding_max_batch_size_rejected` (~L1545, status only)

**What's wrong**: Full analysis in `audit/error-codes.md`. Summary: `ERR_NOT_CLUSTERED` (32), `ERR_INVARIANT_VIOLATION` (33), and `ERR_STREAM_INVARIANT` (34) have no test anywhere that triggers them — only `assert_eq!(ERR_X, n)` constant pins in `src/protocol/codec.rs` ~L2864–2870, which prove nothing about behavior. `ERR_PAYLOAD_MALFORMED` (28) and `ERR_OPCODE_UNSUPPORTED` (29) are asserted in dispatch unit tests, but the three wire tests that exercise those exact scenarios predate P3.10 and still assert only `STATUS_ERROR`, so a regression to `ERR_INTERNAL` (the exact pre-P3.10 behavior the `PROTOCOL_VERSION = 2` bump documents as fixed) would pass CI. Codes 21–26 and `STATUS_DEGRADED_DURABILITY` are in-process-only.

**Why it matters**: README and the `OP_HELLO` doc promise v2 clients can dispatch on these typed codes. The compatibility contract the version bump exists for is untested at the layer clients consume it. Code 32 is trivially triggerable (any cluster opcode to a single-node server — every test server in `server_tcp.rs` is one).

**Reproduction**: `rg -rn "ERR_STREAM_INVARIANT|ERR_INVARIANT_VIOLATION|ERR_NOT_CLUSTERED" --glob '*.rs' src tests | grep assert` → only the constant pins.

**Suggested fix**: Extend `tests/error_code_conformance.rs` (the right home, per its own header) with T-7..T-12: opcode 999 → 29; truncated spend payload → 28; OP_GET_PARTITION_MAP on single-node → 32; request_id upper-bits on a shard-carrying opcode → 33; stream byte-cap overflow → 34; and upgrade the three status-only assertions in `server_tcp.rs`.

**Checklist disposition**: feeds the error-code triggerability matrix.

---

### [LOW] Vacuous-pattern instances: bare `.is_err()` assertions without variant checks, despite the project's own ban

**Location**: `src/index/mod.rs` L1018 (truncated snapshot), L1096 (nonwritable path); `src/index/backend.rs` L1239; `src/storage/blobstore.rs` L1297; `src/replication/manager.rs` L1320; `src/protocol/frame.rs` L431 (truncated frame)

**What's wrong**: CLAUDE.md bans "Tests that only assert `.is_err()` without checking the error variant." A scan found ~6 violations (out of hundreds of error-path tests — the overwhelming majority DO match variants, often with payload field checks; e.g. `src/index/mod.rs` L997 checks `ChecksumMismatch`, `src/device.rs` alignment tests check `AlignmentViolation`). The three `assert!(result.is_ok())` instances found are all followed by stronger state assertions, so they're fine. No `assert!(true)`, no `#[ignore]`, no empty test bodies anywhere — the banned-pattern hygiene is otherwise genuinely enforced.

**Why it matters**: A truncated-frame decode that started returning `FrameError::TooLarge` instead of `Truncated` (wrong classification → wrong client retry behavior) would pass `frame.rs::truncated_frame_error`.

**Suggested fix**: Add variant matches at the six sites; mechanical, ~15 minutes.

**Checklist disposition**: Vacuous test scan — clean except these six; no assertion-free tests found; build-state-but-never-restart pattern not found (restart/reopen verification is pervasive in recovery/integration tests).

---

### [LOW] Coverage holes at the opcode level: op 255 untested anywhere; ops 31/32/101/107 never cross a socket; empty-batch semantics unpinned for every batch op

**Location**: `src/server/dispatch.rs` ~L512 (`OP_INCREMENT_SPENT_EXTRA_RECS` handler); dispatch tests ~L7338–7536 (pruner ops), ~L8207 (health), ~L8219 (hello)

**What's wrong**: Full matrix in `audit/coverage-matrix.md`. Highlights: `OP_INCREMENT_SPENT_EXTRA_RECS` (255) — a documented legacy-client compatibility contract ("callers expect success and do not parse a body") — has zero tests; a refactor that routes it into the unknown-opcode arm breaks legacy clients silently. The pruner ops (31, 32), `OP_HEALTH` (101), and `OP_HELLO` (107, the documented v2 handshake) are tested only via `handle_request()` in-process. No test on any opcode sends a count=0 batch and pins the response shape.

**Why it matters**: Mostly contract-drift risk rather than money risk; 107's absence is the oddest since it's the advertised first call of every v2 client session.

**Suggested fix**: One wire test each for 255/101/107 (3-line payloads), wire happy-path for 31/32, and a parameterized empty-batch conformance test across all batch opcodes.

---

### [LOW] (Disposition notes — remaining checklist items, no defect)

- **Cluster chaos realism (`tests/cluster_partition.rs` + `tests/net_proxy/`)**: better than typical. The proxy is fully seeded (SplitMix64, no wall-clock entropy), supports asymmetric per-directed-link UDP drop, delay, and probabilistic reorder, and TCP inbound drop/delay with frame-parsing directed cuts for topology opcodes; its loopback-attribution limitation is documented honestly and argued sound (quorum loss stops replication frames before they'd matter). Six live-cluster tests including the E-01 minority-never-activates headline. Residual: no packet-loss-*percentage* mode for UDP (binary drop per link only), no random node-kill within the cargo tree (Docker scenario_16 owns that and never runs — see HIGH), and `cluster_tcp.rs` node "kills" are clean shutdowns.
- **Stress tests (`tests/stress_tests.rs`, 7 tests)**: NOT manual-only — they run scaled-down in every CI `cargo test --all` and at full volume (`TERASLAB_FULL_WORKLOAD=1`) in nightly, which also runs `e2e_workload` full-scale and the `slow-tests` lib gate. Actually exercised.
- **`tests/error_code_conformance.rs`**: confirmed real wire path (real `Server::run()`, real `TcpStream`, exact payload-byte assertions) — but narrow (codes 6/30/35/255 only); see MEDIUM finding above.
- **fault-injection feature wiring**: CI runs `cargo test --features fault-injection --test fault_injection` explicitly (ci.yml L106) because `--all` doesn't enable features — correctly handled, including the `unmatched_fault_modes_are_silent` harness self-test.
