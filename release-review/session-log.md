# TeraSlab v1 Release Review — Session Log

Resumable, append-only log. **This run supersedes the 2026-06-22 review** (overwritten per instruction).

## Host & baseline (2026-06-23)

- Host: macOS (darwin 25.3.0), Apple Silicon. **No Linux, no real NVMe `O_DIRECT`** — perf claims assuming Linux/NVMe are marked `unverified-on-this-host`.
- Repo: `/Users/siggioskarsson/gitcheckout/teraslab`, branch `main`, HEAD `920ac32` (fix #14 atomic allocate+create).
- Working tree clean except stray test artifacts in root (`teraslab-data.dat*`, `teraslab-index.snap*`, `teraslab-tombstone.redb`, `tarpaulin-report.json`). Not touched.
- Execution mode (user choice): **multi-agent workflow**; perf: **measure what host allows**.

## Leads carried over from prior (2026-06-22) review — MUST independently re-verify

- **REL-403 (prior BLOCKER):** client 4-byte `ProcessExpiredPreservations` wire payload mismatch — `client/go/codec.go:330-333`, `client/rust/src/lib.rs:1751`, `dispatch.rs:8636-8652`. Re-verify in clients + protocol reviewers.
- **cluster_swim parallel-test flakiness:** 3 tests failed under parallel run, passed `--test-threads=1`. Prior run later got 2710 passed clean. Re-verify determinism (tests reviewer).
- README Status table claims `2234 passed` — prior measured 2710. Likely stale doc count. Re-verify.

## Phase 0 — Orient & build

- Read `README.md` (824 lines). README hedges headline perf (10M+ ops/sec = MemoryDevice ceiling, no fsync; NVMe not measured). Perf judged against the hedged claim.
- Source: 152,598 LOC in `src/`. Largest: `server/dispatch.rs` (23k), `ops/engine.rs` (16k), `cluster/coordinator.rs` (16k).
- `cargo build --release`: **exit 0, 0 warnings.**
- `cargo test --all`: running (bg `b2qsjcugq`).
- `cargo clippy --all --all-targets`: running (bg `b6btiwjjn`).
- Criterion benches (short): running (bg `bfmka3fq3`).
- Raw escape-hatch grep (incl. test modules, needs separation): unsafe 176, `.unwrap()` 4852, `.expect(` 617, `#[allow(` 74, todo/panic/FIXME-family 307. Security reviewer separating test vs non-test.

## Phase 1+5 — Correctness & security (workflow `wf_1a325ffb-056`)

- 13 per-subsystem reviewers in parallel → structured `path:line` findings → 2 adversarial skeptics per blocker/major finding (majority-confirm to survive).
- Subsystems: ops-spend, durability, concurrency, cluster, replication, storage, protocol-server, index, config-cli, security, tests, clients, docs-spec.
- Status: in progress.

## COMPLETE — all phases done (2026-06-23)

- Workflow `wf_1a325ffb-056` finished: 51 agents, 3.4M tokens. 13 subsystem reviews + adversarial verification.
- Verification refuted 2 findings (config validators, Go classifyRetry) → dropped. Gate owner escalated 2 reviewer-majors to BLOCKER (REL-001 delete-race UB, REL-002 cluster persist no dir-fsync) and re-read both by hand to confirm; also re-confirmed prior REL-403 (now REL-014, 4-byte ProcessExpired) which the workflow missed.
- Clean isolated `cargo test --all`: **2710 / 0 / 0** (70 suites, real cargo exit 0). Settles README 2234 (stale) and workflow reviewer's 2514 (contended partial).
- Isolated benches confirm earlier "negative thread scaling" was a contention artifact (isolated spend_threaded flat ~110 Kelem/s).
- Deliverables written: REVIEW.md, FINDINGS.md (2 blockers / 10 majors / 45 minors / 2 dropped), COVERAGE.md, PERF.md, GO-NOGO.md (**NO-GO**, clearable with 2 small fixes).
- Residual closed: Docker/Compose files referenced by README exist.
- Not done on this host: NVMe/Linux perf, `--features fault-injection` clippy, live RSS, /ui behavior — all flagged in GO-NOGO §"not verifiable".

## Phase 3 — Performance (measure-what-host-allows)

- Criterion MemoryDevice benches (short) ran under CPU contention from the concurrent workflow+test → numbers depressed, variance huge. Re-ran isolated (see PERF.md for credible numbers).
- Provisional (contended): index_lookup/hit/10k ~40 Melem/s; create/utxos/100 ~1 Melem/s; read_metadata ~1.5 Melem/s; spend_threaded/2 ~50 Kelem/s, /4 ~21K, /8 ~26K (negative scaling — investigate lock contention, but suspect contention artifact).
- NVMe/Linux numbers out of scope on this host.

## Build/lint/test gate status (Phase 0 cont.)

- `cargo clippy --all --all-targets`: **clean, 0 warnings**, exit 0 (7m11s). NOT yet run with `--features fault-injection` (README claims clean there too) — TODO.
- `cargo test --all`: background run was piped through `tail` → exit code and full counts UNRELIABLE. **Re-run clean+isolated pending** (after workflow). Visible tail showed no FAILED. README claims 2234 (prior review measured 2710) — re-verify count.

## Version bump to v0.7.0 (user directive: "reflected everywhere")

- `Cargo.toml` 0.5.1 → **0.7.0**; `Cargo.lock` updated. Server binary / `/status` / OTel read `CARGO_PKG_VERSION` → report 0.7.0 automatically (src/bin/server.rs:382, src/observability/mod.rs:180).
- `client/rust/Cargo.toml` (teraslab-client) 0.1.0 → **0.7.0**; its Cargo.lock updated. (User chose: align both clients.)
- Go client: no `go.mod` edit needed for 0.7.0 (path stays v0/v1-compatible); `client/go/v0.7.0` tag is cut at release time. No in-repo version refs.
- `teraslab-tests/client` uses a path dep — no pin to update.
- Only doc hits for old versions are historical `teraslab-tests/results/*.log` run artifacts — left untouched.
