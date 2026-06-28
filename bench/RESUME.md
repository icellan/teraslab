# RESUME — TeraSlab vs. the reference datastore (perf campaign)

**Start here after a context reset.** Full chronological detail is in
`PERF_LEDGER.md` (entries B0 → E13); fairness/setup in `METHODOLOGY.md`. This file
is the tight "where we are / what's next / how to run it" pointer.

## Mission & current standing
Make TeraSlab beat the reference datastore on the production UTXO workload (Spend
throughput + p99.9 at minimum) under a **fair matched config**, proven by
reproducible numbers. Constraint: never name/import the reference product anywhere
in this repo — call it "the reference datastore" (grep confirms zero refs).

**Current (2026-06-28, after E13-E19): TeraSlab WINS throughput + p99; misses ONLY
spend p99.9 (a macOS-Docker host artifact). Read PERF_LEDGER.md E18-E19 — latest.**
On the FAIR matched config (device_split=4, redo 256MB/store ≈1GiB total ≈ ref's
1024M cache, ring, buffered, flush 50ms), quiet host, 5-6 interleaved rounds @
IF=512, **0 failed both sides**:
- Spend throughput **TS +8.3%** (margin ~23σ), total **+8.2%**, every op **+8%**,
  spend **p99 wins** (19.4 vs 20.1ms, tighter), setMined p99.9 wins (24 vs 200ms).
- Spend **p99.9: TS ~28-29 vs ref ~24-26ms = +13% — the LONE strict-criterion miss.**
  Pinned (E19) to a **macOS-Docker O_DIRECT redo-flush fsync that freezes the VM's
  virtualized I/O ~100ms** (it stalls read-only GET too — not a write-path/logic
  flaw). NOT closable by flush cadence (tested 1000/50/20ms). On Linux/NVMe (the
  real Teranode target) O_DIRECT fsync is ~0.1ms with no VM freeze → TS p99.9 →
  ~p99 (~19ms) ≤ ref → strict 4/4 passes. Do NOT bound/abandon O_DIRECT to chase
  the macOS tail (core to the 10-50× SSD-wear goal; the freeze hits reads too).
- The arc: E13 fsync-coalescing → E14 secondary sharding → E15 redo ring (kills the
  ~10s checkpoint freeze) → E16 device_split=4 (lock-domain cap) → E17 ring-encode
  + index-probe → E18 Arc-ify RedoOp::Create.record_bytes (per-create clone = O(1)).
  All committed; `cargo test --all` 2965/0.
- **TO CERTIFY THE STRICT 4/4: re-run this exact suite on a Linux/bare-metal NVMe
  host (load<1/core).** Everything except the host-artifact p99.9 already passes.

## The arc (why we are where we are)
1. **B0**: baseline lost ~5× (closed-loop bench, masked the real issues).
2. **P1/P2**: built a fair harness; found it latency/lock-bound, not CPU.
3. **E1 (committed)**: `client/go` connection-pool bug — pool never grew past 1
   conn; fixed to grow to MaxConns under load.
4. **E5/E8/E10 (committed)**: redo-mutex hygiene — double-buffer pwrite out of the
   lock, pre-encode (serialize outside lock), `mem::replace` buffer-swap. These
   cut the redo lock-wait but **did not** move the disk-bound ceiling (they target
   a contention that was *masked* by fsync — see E12).
5. **E6 (open-loop harness)**: realistic load showed the reference scales to ~44k
   while TeraSlab capped ~6–8k.
6. **E11**: explored **redo sharding** — multi-store proxy gives ~2× then plateaus.
   **Do NOT build redo sharding for the main bottleneck** — more shards = more
   flushers fighting one device fsync budget.
7. **E12 (ROOT CAUSE, subagent + tmpfs)**: the ceiling is **redo fsync
   granularity**. RAM-backing the device → 5× → ~38.6k (matches reference). The
   `redo_commit_lock_wait` was a *downstream symptom* of an fsync-bound flusher.
8. **E13 (FIX, committed, verified)**: the ~600 fsync/s were `setMined`'s
   two-phase secondary-index durability calling `append_batch_and_flush` (an
   unconditional fsync **per key**) that **bypassed buffered mode**. Fix: engine
   `redo_buffered` flag + `journal_secondary_ops` (buffered → `append_atomic`,
   append-only; strict → unchanged). → **~3× (7.5k→22.5k), fsyncs 437/s→8.7/s,
   entries/flush 16→2445.** `cargo test --lib` 2467 pass, clippy/fmt clean.

## What's committed (branch `feat/device-cache`, this repo)
All of E1/E5/E8/E10/E13 + the ledger. Tree is clean. `git log --oneline -15`.
Key files touched: `src/redo.rs`, `src/redo_group.rs`, `src/ops/engine.rs`,
`src/metrics.rs`, `src/server/http.rs`, `client/go/pool.go`, `bench/*`.

## The harness (lives OUTSIDE this repo — it references the reference product)
- **Driver**: `/Users/siggioskarsson/gitcheckout/teranode-bench-wt/cmd/utxobench/bench_test.go`
  — a Go test (build tag `utxobench`), gated by `RUN_UTXOBENCH=1`. Has both
  closed-loop and **open-loop** (`OPEN_LOOP=1`, `IN_FLIGHT`, `DISPATCHERS`) modes.
  Drives BOTH backends through Teranode's production `utxo.Store` interface.
- **Worktree**: `teranode-bench-wt` = Teranode repo, branch `teraslab/integration-wip`.
  Uncommitted there (persists on disk): the driver (`cmd/utxobench/`), a `go.mod`
  `replace` → this repo's `client/go` (machine-local), and adapter perf changes
  (spend batcher in `stores/utxo/teraslab/`, a `ProcessExpiredPreservations` arity
  fix in `pruner.go`).
- **Backup** (insurance, outside both repos):
  `/Users/siggioskarsson/gitcheckout/teraslab-bench-harness-backup/` — copy of
  `cmd/utxobench/` + `adapter-and-gomod.patch`.
- **Server image**: `teraslab:bench` (built from this repo's
  `teraslab-tests/docker/Dockerfile`). Rebuild after any src change:
  `docker build -f teraslab-tests/docker/Dockerfile --build-arg CACHE_BUST=$(git rev-parse --short HEAD) -t teraslab:bench .`
- **Reference backend**: the reference datastore's official server container image
  (tag + the `BACKEND=<ref>` value are in the harness, not here), config =
  Teranode's production reference config (Teranode tree), copied to a writable
  temp dir before `docker run` (its entrypoint templates the conf in place).

## How to run the bench (the decisive measurement)
1-store, **disk-backed**, open-loop (the config that exposes the bottleneck):
```
# server (matched async config: buffered redo + writeback cache, 256 MiB redo):
cd <this repo>
docker rm -f utxobench-ts; docker volume rm utxobench-ts-vol; docker volume create utxobench-ts-vol
docker run -d --name utxobench-ts --ulimit memlock=-1:-1 -p 13300:3300 -p 19100:9100 \
  -v utxobench-ts-vol:/data -v "$PWD/bench/configs/teraslab-async.toml":/etc/teraslab/node.toml:ro teraslab:bench
sleep 7; curl -fsS localhost:19100/health/live   # "ok"
# load (IF=256 was the clean peak; failed=0):
cd /Users/siggioskarsson/gitcheckout/teranode-bench-wt
RUN_UTXOBENCH=1 OPEN_LOOP=1 BACKEND=teraslab TARGET=127.0.0.1:13300 \
  POOL_SIZE=60 IN_FLIGHT=256 DISPATCHERS=16 DURATION_SEC=15 WARMUP_SEC=3 \
  go test -count=1 -v -tags utxobench -run TestUTXOHeadToHead -timeout 10m ./cmd/utxobench/
# output: UTXOBENCH_RESULT {json} — sum per-op ops_sec for total.
```
- Metrics: `curl localhost:19100/metrics` (`redo_flush_latency_ns_*`,
  `redo_entries_per_flush_*`, `redo_commit_lock_wait_ns_*`, `create_*_latency_ns_*`,
  `spend_latency_ns_*`, `lock_wait_ns_*`); admin: `-H "Authorization: Bearer
  bench-local-admin-token-0001" .../admin/top`. CPU: `docker stats utxobench-ts`.
- **tmpfs sanity** (proves I/O vs lock): swap `-v utxobench-ts-vol:/data` for
  `--tmpfs /data:rw,size=3g` → expect ~38k (the engine ceiling).
- Always note `uptime` load — shared box; compare backends **same host, same time**.
- Clean up: `docker rm -f utxobench-ts; docker volume rm utxobench-ts-vol`.

## Next levers (the remaining ~40%, now lock-bound)
In priority order (the fsync cap is gone, so these now BIND):
1. **Redo-mutex contention at high concurrency** — now UNMASKED. E5/E8/E10 already
   reduced the in-lock hold; re-profile `redo_commit_lock_wait` at IF≥512 post-E13
   and decontend further if it dominates.
2. **Per-store allocator `commit_pending` lock** (E11) — serializes creates
   (`engine.rs`, one `Mutex<SlotAllocator>`/store). Shard the freelist or commit
   outside the lock.
3. **Colder secondary paths still fsync per-op in buffered mode** (subagent
   follow-up #1): `update_dah_index`/`update_unmined_index` (single-secondary) +
   conflicting/deleted-child intents. Same bug as E13; not the bottleneck here (0
   samples) but fixing needs a `buffered` flag threaded through the index-backend
   `insert/remove` signatures (~60 call sites). Apply the E13 pattern.
4. **Redo capacity vs checkpoint** under sustained high throughput (transient
   LogFull at 5×) — already bumped redo_log_size 64→256 MiB; watch checkpoint
   cadence.

## Hard constraints (do not violate)
- Never name/import the reference product in this repo (source/tests/benches/docs/
  commits/output). `git grep -i <name>` must stay empty.
- Fair matched durability: TeraSlab buffered redo + writeback ↔ reference's
  no-commit-to-device. No cheating (don't shrink workload, RAM-vs-disk mismatch,
  drop correctness, or misconfigure the reference).
- No correctness regressions: `cargo test --all`/`--lib` green, Docker e2e green,
  crash recovery / replication / quorum intact. Redo changes are crash-critical —
  TDD, and re-verify suite + clippy `-D warnings` + fmt before committing.
- Pre-push (when asked): also `cargo fmt --check` + `cargo test --manifest-path
  client/rust/Cargo.toml --all`. Commit each win separately; no AI attribution.
