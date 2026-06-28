# TeraSlab — Linux / raw-NVMe performance profile & 10M-tps scaling model

**Date:** 2026-06-28  **Branch:** `feat/device-cache`
**Host:** AWS EC2 **i3en.6xlarge** *spot* (us-east-1), 24 vCPU, 186 GiB RAM,
kernel 6.18, **2× 6.8 TiB NVMe instance-store SSD** (`/dev/nvme1n1`, `/dev/nvme2n1`).
Instance launched + torn down for this run (cost control).

**Workload** (per request): sustained mix **spend : create : unlock = 1:1:1**,
1-input/1-output tx semantics; `unlock` = `SetLocked(false)` (write-path probe:
generation bump + redo + DAH-index update). setMined fired as a periodic
concurrent burst (drain-time metric). Driver: the extended Rust `teraslab-loadgen`
(commit `ed10357`); records/s = `ops_sec × batch`.

---

## TL;DR

1. **TeraSlab is software-serialized at ~40–44k ops/s — *independent of hardware*.**
   The same ceiling appears on a MacBook (Docker, ~38k) and on a quiet 24-vCPU /
   2×NVMe server. At the cap the box is **90 %+ idle**: CPU ~30 % (≈7 of 24 cores),
   NVMe at ~1.6 % of its measured 314k-IOPS budget.
2. **Root cause: a single global dispatch funnel** — one shared `DispatchPool`
   with one `Mutex<VecDeque<WorkItem>>` + Condvar that *every* pipelined request
   passes through (`src/server/mod.rs:1407`). More cores, more devices, more
   `device_split`, more client processes, and bigger batches **do not** raise it.
3. **~31 % of the server's on-CPU time is wasted in `BytesMut::reserve_inner` →
   `memcpy`** (buffer realloc in the read/frame path, `src/server/mod.rs:1208`).
4. **io_uring is NOT the lever for the realistic (cache-on) path** — the threads
   are *blocked on a lock*, not on I/O. io_uring only helps the synchronous
   cache-off path (see §3), which is not the production durability posture.
5. **Answer to "what machine does 10M tps?": not a hardware question yet.** The
   global serialization must be removed first; today no amount of hardware exceeds
   ~44k. See §6 for the model.

---

## 1. fio raw-device baseline (per device; both identical)

| test | result |
|---|---|
| 4K randwrite, O_DIRECT, libaio QD256 | **157k IOPS**, 612 MiB/s, p99.9 **2.04 ms** |
| 4K randread, O_DIRECT, libaio QD256 | **277k IOPS**, 1084 MiB/s, p99.9 2.34 ms |
| 4K randwrite, O_DIRECT, **io_uring** QD256 | **175k IOPS** (**+11 %** vs libaio) |

Two devices ⇒ ~**314k** 4K-write IOPS / ~554k read IOPS aggregate ceiling. (io_uring
gives a real but modest device-level edge.)

## 2. Cache-ON (realistic async durability) — the representative numbers

`[cache] writeback` (32 GiB) + buffered redo. `device_split=4` (8 stores), batch=1:

| workers | records/s (create+spend+unlock) | CPU busy | p50 | p99.9 |
|---|---|---|---|---|
| 128 | **42,800** | 29 % | 3.0 ms | 13 ms |
| 256 | 42,000 | 30 % | 5.6 ms | 18 ms |
| 512 | 41,500 | 32 % | 11 ms | 24 ms |

Throughput is **flat**; only latency grows ⇒ a hard serialization cap, not load.
Per op ≈ 14k/s each for create / spend / unlock, **0 failures**.

## 3. Cache-OFF (pure raw O_DIRECT) — the synchronous-I/O worst case

batch=1: ~**4.9k records/s**, **CPU 7-12 %**, device ~5k IOPS (**1.6 %** of the fio
ceiling). Every op does a synchronous O_DIRECT write serialized on the
**per-physical-device fsync barrier** (sub-stores of one device share one barrier,
`src/subdevice.rs`). This is the one place blocking I/O leaves the device idle —
**the legitimate io_uring opportunity** (async submission, high QD per device) —
but it is the cache-off worst case, not the production posture.

## 4. Levers that do NOT move the ceiling

| lever | result |
|---|---|
| `device_split` 1→2→4→8 (cache-off) | flat ~4.3-4.8k (shared per-device barrier) |
| `device_split` 4→8 (cache-on) | **identical ~43k** |
| workers 128→512 | flat throughput, linear latency growth |
| **batch 1→64→512** | **throughput *collapses*** (per-RPC latency explodes to 0.4-5 s; records/s falls) — a real batch-path defect |
| 1→2→3 concurrent client processes | aggregate **38.9k / 36.9k** — clients *share* the same ~40k ⇒ **server-bound, not client-bound** |

## 5. Root-cause profile (cache-on, under load)

- **603 server threads in `S` (blocked), ~1 running** — thread-blocked, not computing.
- `perf` on-CPU: **31 % `BytesMut::reserve_inner`/`__memcpy`**, then `parking_lot`
  RwLock `lock_shared_slow`/`lock_exclusive_slow` futex waits.
- Structure: `DispatchPool { queue: Mutex<VecDeque<WorkItem>>, not_empty: Condvar }`
  — **one shared queue for all connections** (`src/server/mod.rs:1407`). Every
  request takes the queue mutex twice (enqueue+dequeue) + a condvar wake; the pool
  worker count bounds total concurrent dispatch. This is the global ~40k funnel.
- Secondary: `cluster.shard_table().read()` (a `parking_lot::RwLock`) is taken on
  **every** op (`src/server/dispatch.rs:173`, `:973`, `:1433`, …) — shared-read
  contention at high concurrency.

## 6. Scaling model toward 10M tps

The headline: **10M tps is unreachable on *any* hardware until the global
serialization is removed** — today the limit is ~44k regardless of cores/disks.

Once the funnel is fixed, the limit moves to **CPU cycles/op**, then device IOPS:
- **CPU:** at ~40k the server uses ~7 of 24 cores *and* wastes ~30 % of that on
  buffer realloc. Removing the funnel (use all cores) + the memcpy waste should put
  *this* box in the low-hundreds-of-k ops/s range. Honest caveat: the exact
  per-core rate can only be measured *after* the fix.
- **I/O is not the limit:** 10M records/s, coalesced at even ~20-50 records per
  physical write, is ~200-500k IOPS — within a handful of NVMe devices (one i3en
  device alone did 157k). The cache + group-commit already coalesce.
- **Rough sizing (post-fix, to be re-measured):** if a fixed dispatch reaches
  ~10-15k ops/s/core, 10M tps ⇒ ~700-1000 cores ⇒ ~**30-45 × 24-core boxes**, or a
  smaller fleet of large-core-count NVMe boxes (e.g. ~10 × 96-core). NVMe count is
  driven by capacity/redundancy, not IOPS.

**So the deliverable answer:** the path to 10M tps is *software first* (remove the
dispatch funnel, the BytesMut realloc, and the batch-path collapse), *then* it
becomes a CPU-core-count question with NVMe I/O comfortably in budget.

## 7. Concrete optimization targets (in priority order)

1. **Shard the dispatch path** — replace the single `DispatchPool` queue
   (`src/server/mod.rs:1407`) with per-core / per-connection-group queues or a
   lock-free MPMC, so requests don't serialize on one mutex+condvar. *This is the
   ~40k cap.*
2. **Kill the `BytesMut::reserve` realloc memcpy** (`src/server/mod.rs:1208` + the
   frame path) — right-size and reuse the per-connection read/response buffers so a
   frame never triggers grow-and-copy on the hot path (~31 % on-CPU today).
3. **Make `shard_table` reads lock-free** (e.g. `arc-swap` snapshot) — it is read
   on every op (`src/server/dispatch.rs:173`, …).
4. **Fix the batch-path collapse** — `--batch > 1` should *raise* records/s
   (amortize RPC + coalesce writes); today it serializes per item and latency
   explodes. Investigate `create_batch`/`spend_batch` server handling.
5. **io_uring** — only after the above, and only for a synchronous/strict-durability
   O_DIRECT path (§3); irrelevant to the cache-on bottleneck.

## 8. Caveats / not covered

- `device_split=16` failed to start in one run (transient header-wipe timing); 8
  stores already showed the plateau.
- Loadgen + server shared the same 24 cores (single instance); the multi-client
  test still isolates server-vs-client bound (aggregate did not rise).
- Spot capacity forced **2 NVMe** (i3en.6xlarge); the 4-disk run (i3.8xlarge /
  i4i.16xlarge) had no spot capacity in us-east-1 at test time. The ceiling is
  software, so disk count does not change the headline.
- Reproduce: `bench/configs/teraslab-linux-nvme.toml` + the sweep scripts; loadgen
  `teraslab-loadgen --saturate --mix "create=1,spend=1,unlock=1"`.
