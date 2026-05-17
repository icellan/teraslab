# Test suite performance audit

Captured: 2026-05-17, branch `worktree-agent-a286cccbafcb3ee62`, base `dba8fcd`.
Hardware: macOS 25.3.0, Darwin Apple Silicon (user workstation; same box that
sees `cargo test --all` >1h).

Methodology — each stage was run as a separate `cargo test` invocation with
`/usr/bin/time -p`, after a warm build. Cold-build numbers are noted where
they materially change the picture. Default `cargo test` profile is `test`
(unoptimized + debug-info); the user-visible "over an hour" complaint is the
debug case, so debug numbers are primary.

## Wall-clock summary

| Stage                                     | Time            | Notes |
|-------------------------------------------|----------------:|-------|
| `cargo test --no-run --all` (warm)        | 0.8s            | Recompile cost is paid once during the first stage below; not the bottleneck on warm builds. |
| `cargo check --lib` (cold)                | **96s**         | First lib build dominates cold runs. |
| `cargo test --lib --release` (run only)   | **9s**          | 1750 unit tests run in 9s with optimizations. |
| `cargo test --lib` (debug, run only)      | **593s** (~10 min) | One test alone — `index::hashtable::tests::ten_million_entries` — burns ~508s. |
| `cargo test --tests --release` (run only) | **140s**        | All 50 integration test binaries together. |
| `cargo test --tests` (debug, run only)    | **1990s** (~33 min) | Debug `cargo test --tests`, parallel as cargo schedules them. |
| `cargo test --doc` (warm)                 | **36s** (15s test + ~20s build) |
| `cargo test --doc` (cold)                 | **53s**         | Only 2 trivial compile-fail doctests; the time is rustdoc spinning up rustc per fence. |
| `cargo bench --no-run --all`              | 7s              | Bench compile is cheap; benches themselves not part of `cargo test`. |

**`cargo test --all` (debug, fresh-ish build) totals ≈ 96s (cold lib build) + 593s (lib) + 1990s (tests) + 36–53s (doc) ≈ 2700–2740s ≈ 45 min.** On a slower disk, a cold compile, or with rustc-cache misses the user has seen >1h easily.

In release the same totals are 9s + 140s + 36s ≈ **185s wall-clock** — i.e. running release-mode tests would compress >1h down to ~3 min.

## Top 10 slowest individual tests (debug)

| Rank | Test | File | Time | Why |
|-----:|------|------|-----:|-----|
| 1 | `index::hashtable::tests::ten_million_entries` | `src/index/hashtable.rs:1766` | **508s** | Inserts 10M keys in debug; the inner `insert` + probing path is fully unoptimised. |
| 2 | `allocator::tests::persist_rejects_freelist_overflow` | `src/allocator.rs:2614` | **17.3s** | Pushes `MAX_PERSISTED_FREE_REGIONS + 1` ≈ 65 537 entries via `__test_force_push_free_region`, which calls `FreelistBackend::insert` on the *Small* variant — that's `Vec::insert` (O(n)) followed by `debug_assert_sorted` (O(n) scan) per call ⇒ O(n²) ≈ 4.3 G ops in debug. |
| 3 | `persist_rejects_freelist_overflow_via_integration_path` | `tests/g1_review.rs:134` | **17.6s** | Exact duplicate of #2 from the integration crate — same O(n²) loop, same helper. |
| 4 | `add_fourth_node_rebalance_triggers` | `tests/cluster_tcp.rs:478` | ~15s | Three back-to-back `thread::sleep(Duration::from_secs(4..5))` followed by an `assert_ne!` that *currently fails* (shard table version never bumps in this build). |
| 5 | `isolated_node_rejects_writes_with_no_quorum` | `tests/cluster_tcp.rs:1300` | 15s (timeout) | `wait_until` loop with `Duration::from_secs(15)` ceiling and 50 ms poll; currently never reaches 3 committed members ⇒ always burns the full 15s. |
| 6 | `bench_insert_throughput` | `src/index/hashtable.rs:1804` | **6.2s** | 1 M debug-mode inserts inside a `#[test]`. Output is `eprintln!` only — not asserted. |
| 7 | `bench_lookup_1m` | `src/index/hashtable.rs:1785` | **6.1s** | Same shape: 1 M inserts + 1 M lookups in debug, `eprintln!`-only. |
| 8 | `delete_does_not_alias_concurrent_create` | `tests/g2_delete_race.rs:97` | 1.5s (currently FAILS — 9 aliasing observations) | 1.5s wall-clock soak; the failure is a real correctness bug, not a perf issue. |
| 9 | Lib `direct_read_write_concurrent_stress_never_returns_torn_data` | `src/io.rs:1071` | <1s (FAILS) | Currently asserts `"torn read passed CRC"` panic — flake or real bug; not a perf cost. |
| 10 | `tests/cluster_swim::*` aggregate | `tests/cluster_swim.rs` | 5.6s | Genuine SWIM-protocol settling time; uses event-driven `wait_for`, not blind sleep. |

(Ranking is by debug wall-clock; #4–#5 in release drop to ~5s each because the `from_secs(N)` sleeps are real, while #1–#3, #6–#7 collapse to <0.5s in release.)

## Top 5 slowest test files (debug, full file run inside `cargo test --tests`)

| Rank | File | Time | #tests | Notes |
|-----:|------|-----:|-------:|-------|
| 1 | `tests/cluster_tcp.rs` | ~50–60s sleep budget on its own | 25 | 21 raw `thread::sleep` calls; 16 of them are `from_secs(2..6)` summing to **59 s of fixed blocking sleep**. Two tests currently FAIL. |
| 2 | `tests/g1_review.rs` | 18s (debug exec) / 0s (release) | 9 | 17.6s comes from a single test — see top-tests #3 above. The other 8 are sub-second. |
| 3 | `tests/cluster_swim.rs` | 5.6s | 10 | Event-driven (`wait_for`), so this is mostly real protocol settling; harder to compress without lowering SWIM intervals. |
| 4 | `tests/e2e_workload.rs` | 5.1s | 21 | Generator + verifier doing real allocator + index work. No fixed sleeps. |
| 5 | `tests/replication_tcp.rs` | 3.0s | 11 | One 3-second `from_secs(3)` sleep at line 793; rest are TCP-handshake timeouts. |

Everything else under 2s. The "long tail" of g4_*/g8_*/g9_*/g10_* files is ~0–1 s each in release; the 13–95 s per-file numbers I captured earlier reflect cargo *relinking* each binary when invoked one-at-a-time, not actual test execution.

## Smells found

- **2 tests duplicate the same O(n²) freelist-overflow loop** — once in `src/allocator.rs::tests::persist_rejects_freelist_overflow`, once in `tests/g1_review.rs::persist_rejects_freelist_overflow_via_integration_path`. Both push 65 537 entries through `FreelistBackend::insert` on the Small variant (Vec). Combined cost: **~35 s of debug-only quadratic work**, exercising the same overflow branch twice.
- **`debug_assert_sorted()` is called on every `insert`/`remove`/`best_fit`** in `FreelistBackend`. For the Small variant it does `v.windows(2).all(|w| w[0].offset < w[1].offset)` — an O(n) scan after every O(n) `Vec::insert`. Fine for production-shaped sizes (<64 entries), but the test-only helper drives n up to 65 537 in the Small variant because it skips `maybe_promote()`.
- **`ten_million_entries` is a release-shaped benchmark hiding in `#[test]`**. 10 M debug-mode inserts is **508 s by itself** — alone it accounts for ~8.5 min of every full debug `cargo test`. There is no scale-down for debug.
- **`bench_lookup_1m` and `bench_insert_throughput` are explicit micro-benchmarks** in `#[test]` form whose only output is `eprintln!`. They have no assertions on throughput — they will never "fail" — but they cost 12 s on every debug run. These belong in `benches/`, not `#[test]`.
- **`tests/cluster_tcp.rs` has 21 raw `thread::sleep` calls** totalling 59 s of fixed blocking. There is already a `wait_until` helper in the file (line ~435 comment: "Fixed sleeps were flaky on loaded CI runners; this helper waits only as long as needed") — it is used in a couple of spots but most of the file still uses the old fixed-sleep pattern. Replacing each `from_secs(N)` sleep with a `wait_until` over the relevant condition (committed members, shard-table version, write success) would drop the wall-clock by 40–50 s and also fix the two currently-flaky-or-broken tests at lines 478 and 1300.
- **136 `Duration::from_secs(N)` occurrences in `tests/` with N≥1**, most clustered in the 6 cluster/replication files. Many are TCP read/connect timeouts (fine), but 30+ are direct sleep durations or `wait_for(Duration::from_secs(...))` ceilings.
- **45 raw `thread::sleep`/`tokio::time::sleep` calls in tests**; **21 of them are in `cluster_tcp.rs` alone**.
- **Doctests cost 36–53 s for 2 trivial `compile_fail` examples**. Cargo runs `rustdoc --test`, which spawns a fresh rustc per fence and links against the full lib — so the "2 doctests" actually pay one full lib compile worth of latency. Acceptable but worth noting in CI.
- **Two real test failures on `dba8fcd`** are inflating wall-clock by waiting their full 15 s timeout: `cluster_tcp::isolated_node_rejects_writes_with_no_quorum` (never reaches 3 committed members) and `cluster_tcp::add_fourth_node_rebalance_triggers` (shard-table version stays at 1). These are correctness bugs, not perf bugs, but they pin ~30 s onto every debug run and ~20 s onto every release run.
- **One lib failure on `dba8fcd`**: `io::tests::direct_read_write_concurrent_stress_never_returns_torn_data` panics with `"torn read passed CRC"`. Either a real torn-read regression or a flaky-by-design stress test that needs hardening; either way the panic itself is fast (<1 s).

## Proposed fixes (priority order)

Each entry: file:line, expected saving in debug `cargo test --all`. "(release)" notes the same fix's effect on release runs.

1. **Move `ten_million_entries`, `bench_lookup_1m`, `bench_insert_throughput` out of `#[test]`.**
   - `src/index/hashtable.rs:1766, 1785, 1804`.
   - Either gate behind `#[cfg(feature = "slow-tests")]` (off by default), behind `if std::env::var_os("TERASLAB_SLOW_TESTS").is_some()` early-return, or move into `benches/`.
   - **Expected saving: ~520 s debug; ~10 s release.** This single change wipes >8 min off the suite.

2. **Drop `__test_force_push_free_region`-driven test from one of the two sites and shrink the iteration count.**
   - `src/allocator.rs:2614` (lib test) and `tests/g1_review.rs:134` (integration). Keep one (the lib one is closer to the assertion), delete the duplicate. The kept test should also assert the overflow branch with **`MAX_PERSISTED_FREE_REGIONS / 1024 + 1`** entries by lowering `MAX_PERSISTED_FREE_REGIONS` *under `#[cfg(test)]`* via a constant, or by short-circuiting the `persist()` overflow check before counting — i.e. test the boundary, not the worst case. Alternatively, fix `__test_force_push_free_region` to call `maybe_promote()` after each push so the Small variant doesn't blow up.
   - **Expected saving: ~30 s debug (almost all of it); negligible in release.**

3. **Replace fixed sleeps in `tests/cluster_tcp.rs` with `wait_until`.**
   - 16 occurrences of `thread::sleep(Duration::from_secs(2..6))` between lines 251 and 1300.
   - The `wait_until` helper at line ~435 already exists; the pattern is "loop with 50 ms poll until predicate or `from_secs(15)` ceiling". For every `sleep(Duration::from_secs(N))` followed by an assertion, swap the sleep for `wait_until(|| <the_assertion>)`.
   - **Expected saving: 40–50 s in both debug and release.** Also removes the two existing test failures' 15 s ceiling waits if they're fixed alongside (or 30 s if they continue to fail, since they currently burn the full ceiling).

4. **Split test runs in CI**:
   - `cargo test --lib --release` (target <15 s).
   - `cargo test --tests --release` (target <3 min, currently 140 s).
   - `cargo test --doc` (target <60 s, currently 36–53 s).
   - **Run debug-mode `cargo test --all` only on a nightly job, not per-PR.**
   - Net effect: per-PR signal drops from >40 min to ~4 min.
   - The `Cargo.toml` already has the bench harnesses split out cleanly; the existing `_review/FIX_POLICY.md` already documents the heavy-suite caveat ("3-6 min").

5. **Lower the cluster-test heartbeat / suspicion intervals under `#[cfg(test)]`** (or via a builder param threaded through the test fixtures).
   - Currently `cluster_swim` uses `Duration::from_secs(3..5)` as its suspicion timeout (`tests/cluster_swim.rs:42`, `tests/g8_ping_req_cap.rs:32`, `tests/g8_swim_replay.rs:31`). Lowering to ~200 ms in tests would compress `cluster_swim`'s 5.6 s real-protocol time to ~1 s.
   - **Expected saving: ~5 s debug + release.** Lower priority — it's already event-driven; the time is real protocol time.

6. **Mark currently-failing tests with their real failures and gate them with `#[should_panic(expected = ...)]` or fix the underlying bugs** so the suite stops eating their 15 s timeouts on every run.
   - `tests/cluster_tcp.rs:478` and `:1300` — wall-clock impact only; correctness impact is in `_review/02_findings_G8.md` (out of scope here).
   - `src/io.rs:1071` torn-read assertion — likely a real bug worth filing.
   - **Not a perf fix on its own, but combined with #3 it caps these at <0.5 s instead of 15 s.**

## CI implications

Recommend three jobs:

| Job | Command | Target |
|-----|---------|-------:|
| `lib-fast` | `cargo test --lib --release` | <15 s after warm cache |
| `integration` | `cargo test --tests --release --no-fail-fast` | <3 min |
| `doc` | `cargo test --doc` | <60 s |
| `nightly-debug` (separate cron, not gating PRs) | `cargo test --all` | best-effort |

Tests that should move to `--ignored` (opt-in, run only via `cargo test -- --ignored` or a `slow-tests` feature):

- `src/index/hashtable.rs::tests::ten_million_entries`
- `src/index/hashtable.rs::tests::bench_insert_throughput`
- `src/index/hashtable.rs::tests::bench_lookup_1m`
- one of the two `persist_rejects_freelist_overflow_*` (whichever is duplicated; or fix the helper to call `maybe_promote()`).

Tests that should stay inline but be timing-tuned:

- All of `tests/cluster_tcp.rs` (replace fixed sleeps with `wait_until`).
- `tests/cluster_swim.rs`, `tests/g8_*.rs` (lower SWIM intervals under `#[cfg(test)]`).
- `tests/replication_tcp.rs:793` (3 s sleep — collapse to a wait_until).

## Reproduction commands

```bash
# Per-test wall-clock (debug, release):
for t in $(ls tests/*.rs | sed 's|tests/||;s|\.rs||'); do
  start=$(date +%s)
  timeout 300 cargo test --test "$t" --release --no-fail-fast 2>&1 | tail -3
  echo "$t: $(( $(date +%s) - start ))s"
done

# Single slow test:
time cargo test --lib index::hashtable::tests::ten_million_entries

# Full suite, release:
time cargo test --all --release --no-fail-fast
```
