# Category C — Concurrency Audit

Scope reviewed: `src/locks.rs`, `src/index/*` (hashtable, backend, mod, secondary backends, migration), `src/ops/engine.rs` + all ops, `src/server/dispatch.rs`, `src/server/mod.rs`, `src/server/http.rs`, `src/io.rs`, `src/device.rs`, `src/replication/receiver.rs`, `src/cluster/{topology,migration,coordinator}.rs`, `src/checkpoint.rs`. Static analysis only (orchestrator holds the build lock); every claim below cites file:line evidence.

**Architecture summary (what the design actually is).** The TCP data plane is synchronous (thread-per-connection, `std::net`; `server/mod.rs:951`). There is no async on the request path; tokio exists only in the axum HTTP server (`server/http.rs`), the observability exporter, and a dedicated `REPL_RUNTIME` used via `block_on` (`dispatch.rs:91,1528`). Concurrency control is four-layered:

1. **Global per-engine `dispatch_visibility_barrier`** (`parking_lot::RwLock<()>`, `ops/engine.rs:110`): client reads take the SHARED side, every mutation opcode **and** `OP_REPLICA_BATCH` take the EXCLUSIVE side (`dispatch.rs:394, 2546-2548`). Mutations are therefore fully serialized engine-wide while a mutation is in flight.
2. **Per-tx stripe mutexes** (`StripedLocks`, `locks.rs:92`) — acquired one-at-a-time per tx inside engine ops.
3. **Process-wide record-offset `StripedRwLocks`** (`io_locks()`, `io.rs:69`) closing the torn-read window for the direct-pointer path (F-X-007/BC-02).
4. **`RwLock<PrimaryBackend>`** around the mmap hashtable; secondary indexes behind `Mutex`.

There is **no seqlock / generation-counter optimistic read scheme**: the `generation` field in `TxMetadata` is a replication/replay version number (`record.rs:729-782`), not a synchronization primitive. Reads are protected by io_locks read guards + (for client reads) the shared barrier.

---

### [HIGH] Exclusive visibility barrier held across synchronous replication RTT — cross-node circular wait (deadlock-until-timeout) under bidirectional write load

**Location:** `src/server/dispatch.rs:394` (guard acquired), `:3063-3085` (`replicate_all_ops` called inside `handle_spend_batch` while the guard is in scope), `:1527-1558` (`REPL_RUNTIME.block_on` fan-out), `:2459` (`recv_ack(ack_timeout)` network wait), `:2546-2548` (`OP_REPLICA_BATCH` requires the same exclusive barrier on the receiving node); `src/ops/engine.rs:110,252-256`.

**What's wrong:** A mutation on node A holds A's engine-wide exclusive `dispatch_visibility_barrier` from before-apply through replication-ack (this is the documented "no observable rollback window" invariant). The replication send blocks waiting for B's ack. On node B, the incoming `OP_REPLICA_BATCH` is dispatched through `handle_request`, which must acquire **B's** exclusive barrier (`dispatch.rs:394`). If B is concurrently processing its own client mutation (holding B's barrier) that is replicating to A, then: A holds A-barrier → waits for B's ack; B's replica handler waits for B-barrier; B holds B-barrier → waits for A's ack; A's replica handler waits for A-barrier. Circular wait across two nodes, broken only when one side's `recv_ack(ack_timeout)` fires (replication timeout, seconds). The failing side then runs compensation/rollback and returns `ERR_REPLICATION_FAILED` to its client.

**Why it matters:** In any RF≥2 cluster where both nodes master some shards (the normal sharded topology), *every* window of concurrent bidirectional writes triggers this stall. Effective mutation throughput degrades to ~1 mutation per `replication_timeout` per node during contention, with spurious replication failures and client-visible errors/rollbacks — under completely healthy network conditions. Additionally, because reads take the shared side, **all client reads on a node stall for the full replication RTT (or full ack timeout) of any in-flight mutation**.

**Reproduction:** Two-node cluster, RF=2, shards split between A and B. Two client threads: one issuing `OP_SPEND_BATCH` against A-owned keys to A, one against B-owned keys to B, in tight loops. Measure p99 latency and `replication_degraded_acks` / `ERR_REPLICATION_FAILED` counts; expect stalls quantized at the replication ack timeout and mutual replication failures. (No test in the tree exercises concurrent bidirectional master↔master writes; existing multi-node tests drive writes unidirectionally or sequentially.)

**Suggested fix:** Do not require the full exclusive barrier for `OP_REPLICA_BATCH` (replica apply already goes through engine ops that take stripe locks internally; the receiver's two direct device writes would need stripe locks added — see separate finding). Alternatively, release/downgrade the barrier before fan-out and preserve the no-rollback-visibility invariant with a per-key "pending compensation" set that reads check, instead of a global lock held across network I/O.

---

### [HIGH] `TopologyAuthority::handle_propose` vote race — check-then-act on `voted_term` with no mutex allows double-voting in the same term (split-brain enabler)

**Location:** `src/cluster/topology.rs:978-979` (loads), `:1013` (decision), `:1044` (store) — all `Ordering::Relaxed`, no lock held; caller `src/server/dispatch.rs:972-1021` (`OP_TOPOLOGY_PROPOSE` is **not** in `needs_dispatch_visibility_barrier`, `dispatch.rs:2533-2539`, and each TCP connection runs on its own thread, `server/mod.rs`).

**What's wrong:** The vote decision is `load(voted_term) → compare → store(voted_term)` with plain Relaxed atomics and no critical section. Two concurrent `OP_TOPOLOGY_PROPOSE` requests from two different proposers carrying the **same term** (entirely plausible after a partition heal: both proposers compute `committed.max(voted)+1` from the same committed term, `topology.rs:945-947`) can interleave: both threads load the old `voted_term`, both pass `propose.term > voted`, both store, and **both return `accepted: true`**. The dispatch comment block (`dispatch.rs:975-984`, "H10") carefully orders in-memory vote → fsync → reply, but nothing serializes two concurrent proposals, so the at-most-one-vote-per-term invariant — the safety core of the quorum protocol — is not enforced. The single-node-recovery branch (`topology.rs:1033-1040`) has the same TOCTOU shape via `committed_members.read()`.

**Why it matters:** A voter that votes for two conflicting same-term proposals lets two proposers each assemble "quorum" from overlapping voter sets → two conflicting topology commits at the same term → two nodes believing they master the same shard → divergent writes. That is the precondition for a double-spend window, which is what the entire quorum/epoch machinery exists to prevent.

**Reproduction:** Unit test: one `TopologyAuthority`, two threads calling `handle_propose` with two distinct same-term proposals (different proposers/members, valid digests) in a loop with a barrier; assert that at most one returns `accepted == true` per round. Expect intermittent double-accepts. A loom model of `{load voted, store voted}` × 2 threads finds the interleaving deterministically.

**Suggested fix:** Put `voted_term`/vote bookkeeping behind a `Mutex` (this is cold-path code; a mutex costs nothing), or use `voted_term.fetch_update(SeqCst, SeqCst, |v| (propose.term > v && propose.term > committed).then_some(propose.term))` so the test-and-set is atomic — and route the entire propose-handling (including the membership-safety check) through one critical section.

---

### [MEDIUM] Migration baseline snapshot reads each record without any lock — torn per-record snapshot (metadata/slot mismatch, generation drift)

**Location:** `src/cluster/coordinator.rs:4528` (`engine.read_metadata`) then `:4551-4557` (`engine.read_slot` loop) in `stream_shard_baseline`; manifest folded from the early metadata at `:4546`.

**What's wrong:** The sender reads a record's metadata, then its slots, with no per-tx stripe lock and no dispatch barrier. Each individual read is torn-safe (io_locks read guard, `io.rs:767,853`), but the *sequence* is not: a concurrent client mutation (which holds only the barrier + stripe lock, neither of which the baseline reader participates in) can land between the metadata read and the slot reads. The streamed `ReplicaOp::Create` then carries generation `G` while the slot-replay ops encode post-`G+1` slot states. On the receiver, the idempotent re-apply of the catch-up redo ops does not bump generation for already-spent slots, so the new master can settle at generation `G` while the source is at `G+1` for the same record. The migration design (snapshot_sequence → fence_sequence redo replay) makes the *data* converge, but generation/manifest bookkeeping is computed from the torn view.

**Why it matters:** Generation is used for replica-apply ordering (`generation_target_ahead`, `record.rs:757`) and the migration manifest is the integrity check for handoff (`OP_MIGRATION_COMPLETE`). A record whose replica generation silently lags its master's invites subtle skips/misorders on subsequent replication, and the manifest "passes" because both sides were computed from the same torn read.

**Reproduction:** Engine-level test: start a thread mutating one key in a loop (spend/unspend); concurrently run the `stream_shard_baseline` read sequence (metadata → slots → re-read metadata) on the same key and assert `meta_before.generation == meta_after.generation` for each iteration — it will fail, demonstrating the window. Then assert generation equality between source and a receiver that applied the baseline + replay.

**Suggested fix:** Per key, either take `engine.locks` stripe lock for the duration of the metadata+slots reads (cheap, brief), or do a seqlock-style retry: read metadata, read slots, re-read metadata; retry if generation changed.

---

### [MEDIUM] Replication receiver `apply_op` writes the device directly, bypassing stripe locks and `io_locks` — safe only via the dispatch barrier; the standalone receiver listener has no such protection

**Location:** `src/replication/receiver.rs:1619-1631` (PruneSlotIfSpentBy: `io::read_utxo_slot(engine.device(), …)` then `io::write_utxo_slot(engine.device(), …)` — a read-modify-write with no lock), `:1707` (`io::write_metadata(engine.device(), …)`); the unguarded listener path: `receiver.rs:215-264` (`ReplicaBatchReceiver::start` spawns raw connection threads → `handle_connection` → `apply_replica_batch` with **no** visibility barrier). The block-I/O write path performs non-atomic `copy_from_slice` into the same allocation the direct read path accesses (`device.rs:660-663`, contract comment at `device.rs:499-506` explicitly assumes "Engine stripe locks").

**What's wrong:** In production, replica batches arrive via `OP_REPLICA_BATCH` on the data port and run under the exclusive barrier (`dispatch.rs:519-579`), which is the *only* thing making these lock-free direct device writes safe — they take neither the per-tx stripe lock nor the `io_locks` write guard, so they are unsafe against (a) any concurrent engine mutation on the same record and (b) any concurrent direct-pointer reader that doesn't hold the shared barrier (e.g. the migration baseline reader of the previous finding, or `checkpoint`-adjacent scans). The library also ships `ReplicaBatchReceiver::start()`, a complete TCP listener that calls the same `apply_replica_batch` with no barrier at all; today it appears to be wired only from tests, but it is a loaded public API — anyone enabling it gets lost-update RMW races and genuine data races (non-atomic device write vs. atomic direct read on the same bytes) for free.

**Why it matters:** The safety of a raw device RMW now depends on a non-local invariant ("all callers reach me through dispatch's exclusive barrier") that is not stated at the write sites and is already violated by an exported entry point. The MemoryDevice/DirectDevice `Send`/`Sync` justifications cite stripe locks that these paths do not take.

**Reproduction:** Wire `ReplicaBatchReceiver::start()` against an engine also serving local mutations (as the receiver's own doc suggests is its purpose); drive `ReplicaOp::PruneSlotIfSpentBy` and local `spend` on the same record concurrently; observe lost `spent_utxos`/slot updates. Under `cargo miri`, the non-atomic `pwrite` racing a direct-path `atomic_load_into` on the same range is a reportable data race.

**Suggested fix:** Make `apply_op`'s prune path go through `engine.prune_slot_if_spent_by_child` (which exists and takes the stripe lock, `engine.rs:2712`) instead of raw `io::write_utxo_slot`; have the remaining direct metadata write take the stripe lock + `io_locks().write`. Then the standalone receiver becomes safe by construction and the barrier is defense-in-depth instead of load-bearing.

---

### [MEDIUM] No test coverage for the two cross-node interleavings that matter

**Location:** test tree (`tests/`): `g2_atomic_apply.rs` (16-thread same-UTXO spend), `stress_tests.rs` (8-thread random ops), `io.rs::direct_read_write_concurrent_stress_never_returns_torn_data`, engine in-module scoped-thread tests (`engine.rs:11378-11536`, `:8921`), `g2_delete_race.rs`.

**What's wrong:** Single-node concurrency coverage is genuinely good (same-key races, torn reads, allocator-under-stripe-lock regression at `engine.rs:7045-7085`). But there is no test for (a) concurrent bidirectional master↔master replication (finding 1) and (b) concurrent same-term topology proposals at one voter (finding 2). No loom models exist anywhere (`rg loom` → empty).

**Why it matters:** Both untested interleavings are exactly the ones the static analysis above flags as broken.

**Reproduction:** n/a — the finding is the absence.

**Suggested fix:** Add the two reproductions described in findings 1 and 2; a loom model for `handle_propose` is ~40 lines.

---

### [LOW] Stale `unsafe impl Send/Sync` safety justifications

**Location:** `src/ops/engine.rs:147-150` ("All access through device_ptr is guarded by stripe locks" — false: read paths deliberately skip stripe locks and rely on `io_locks`, per the engine's own doc at `engine.rs:54-68`; receiver writes rely on the barrier); `src/device.rs:520-528` (MemoryDevice: "concurrent same-range pread/pwrite is the caller's responsibility (Engine stripe locks)" — the receiver's pwrite path takes no stripe lock).

**What's wrong / why it matters:** The stated safety contracts no longer describe the mechanisms actually providing safety. Whoever next refactors against the comment (e.g. "stripe locks cover device_ptr, so I can drop the io_locks read guard") reintroduces the exact BC-02 torn read the codebase already paid to find once.

**Reproduction:** n/a (documentation drift).

**Suggested fix:** Rewrite both comments to name the real guards: io_locks (record-level RwLock) for direct-path read/write, stripe locks for op-level RMW, dispatch barrier for receiver direct writes.

---

### [LOW] `StripedLocks` stripe selection is unseeded and attacker-influenceable

**Location:** `src/locks.rs:115-131` (raw `txid[16..24]` masked, no per-process seed); contrast `src/index/hashtable.rs:516-522` (hashtable adds a random seed precisely "to defeat directed Robin Hood DoS").

**What's wrong:** A txid is a hash of attacker-chosen data; grinding 16 bits (~65k attempts per colliding txid) yields arbitrarily many transactions mapping to one stripe, defeating lock striping for those keys. The same applies to `StripedRwLocks::stripe_index` (`locks.rs:61-66`) only via allocation adjacency, which is not attacker-controlled — that one is fine.

**Why it matters:** Contention DoS on one stripe. Currently masked by the global mutation barrier (finding 1) serializing everything anyway; becomes relevant the moment that barrier is relaxed.

**Reproduction:** Generate N txids with equal bytes 16-24, time concurrent spends vs. random txids.

**Suggested fix:** XOR a per-process random seed into the extracted 8 bytes before masking (one instruction; mirrors the hashtable's defense).

---

### [LOW] Non-atomic gauge decrements in migration metrics

**Location:** `src/cluster/migration.rs:9-13` and `:24-28` — `let prev = gauge.load(Relaxed); if prev > 0 { gauge.store(prev - 1, Relaxed); }`.

**What's wrong:** Load/store RMW; two concurrent decrements can lose one, permanently inflating `migration_active` / phase gauges. (Increments use `fetch_add` correctly, `:522-523`.)

**Why it matters:** Metrics only — but `migration_pressure_active` style decisions keyed on these gauges would act on a stuck-high value.

**Reproduction:** Two threads decrementing from 2 concurrently; final value 1 instead of 0.

**Suggested fix:** `fetch_update(Relaxed, Relaxed, |v| v.checked_sub(1))` (or `fetch_sub` with a saturation invariant maintained by callers).

---

## Checklist disposition

1. **Lock stripes — distribution & power-of-two:** ✅ Power-of-two enforced twice: config validation rejects non-pow2 (`config.rs:1288` `pow2("lock_stripes", …)`) and `StripedLocks::new` rounds up with floor 16 (`locks.rs:100`). Mask-based indexing (`locks.rs:130`). Distribution: bytes 16-24 of the (uniform, SHA-256) txid, disjoint from index bucket [0-8) and fingerprint [8-16) bytes; >16-bit configs keep distribution (test `stripe_index_large_lock_count_uses_more_than_16_bits`, `locks.rs:210`); distribution test at `locks.rs:194`. ⚠️ one caveat: unseeded → adversarial grinding (LOW finding above).
2. **Multi-item batch lock acquisition:** ✅ No multi-stripe holding exists anywhere, so no global sort is needed. All 19 `locks.lock(` sites in `engine.rs` are single-key; batch handlers (`handle_spend_batch` `dispatch.rs:2834-3033` and peers) process txid groups sequentially, releasing each `ValidatedSpend` guard before the next group. Child→parent transitions explicitly drop the child guard first (`engine.rs:3665-3668`, `:2767-2773`); parent-list rebuilds use lock/release/CAS-retry with allocator work outside the lock (`engine.rs:3027-3060`, regression test `:7045-7085`). Same-stripe parent/child collision therefore cannot self-deadlock.
3. **No `.await` while holding a non-async lock:** ✅ The data plane is synchronous (no async). Async surface = `http.rs` + `observability/mod.rs`: all guards are dropped before awaits (`http.rs:1378,1417,1453,1576 drop(table_guard)`; `cluster_drain_complete` is sync and called from the await loop at `:1392`; redo-log lock at `:1734` is inside the sync `build_local_top_snapshot`). `REPL_RUNTIME.block_on` (`dispatch.rs:1528`) runs on OS dispatch threads, and the only `.await` inside is on `spawn_blocking` JoinHandles — no engine lock guard is moved into the async block. ⚠️ note: async HTTP handlers do block tokio workers on parking_lot locks (`engine.index_stats()` etc.) — brief and diagnostic-only.
4. **Read-path locking design:** ✅ Determined: NOT a generation/seqlock design. Client reads take the shared `dispatch_visibility_barrier` (`dispatch.rs:2533-2539,2574-2576`); every direct-pointer byte read additionally holds an `io_locks` record-level read guard (`io.rs:767,853`) against writers holding the write guard (`io.rs:808,886,599`); index lookups go through `self.index.read()` everywhere (verified: all 25+ engine index accesses use `.read()`/`.write()`, lines listed at `engine.rs:503-4273`). The F-G2-001 `meta.tx_id == key.txid` re-check covers delete/recreate aliasing (`engine.rs:4171`).
5. **Generation counter / seqlock both sides:** ✅(n/a-verified) No seqlock exists; `TxMetadata.generation` is bumped exactly once per mutation while the stripe lock is held (e.g. `engine.rs:3579,3764,2750,2834`) and is consumed only for replication/replay ordering (`record.rs:757-782`), never for optimistic-read retry. Torn-read protection is the io_locks guard pair + Release fence after writer memcpy (`io.rs:826,898`) + atomic chunked transfer for miri/hardware (`io.rs:117-241`). No torn-read window found on the direct path; the empirical regression test exists (`io.rs` stress test cited at `engine.rs:62-64`). ⚠️ the *non-direct* receiver write path bypasses this (MEDIUM finding above).
6. **mmap aliasing / unsafe blocks:** ✅ All hashtable unsafe blocks carry safety comments and a module-level Send/Sync contract (`hashtable.rs:553-580`); `bucket()` (shared) vs `bucket_mut()` (`&mut self`) map onto the engine's `RwLock<PrimaryBackend>`; no engine access bypasses the RwLock; resize-without-blocking-readers uses `upgradable_read` → `resized_copy` → upgrade-and-swap (`engine.rs:712-728`), which is sound (upgradable excludes writers during the copy). File-backed `msync`/`munmap`/`madvise` blocks justified (`hashtable.rs:388-399,355-357,483`). No unsafe in other index files. ⚠️ Engine/MemoryDevice Send/Sync justifications are stale (LOW finding above).
7. **Lock ordering across subsystems:** ✅ with one documented convention and no cycle found: stripe → index → dah → unmined (declared and followed: `engine.rs:424-428,485-519`); redo-log lock is either taken-and-released *before* the index/secondary locks (`engine.rs:457-483,354-377`) or nested *inside* the dah/unmined mutex (`engine.rs:286-298` via `log_ref`) — no path holds redo and then takes a secondary or index lock, so dah→redo nesting cannot cycle. Barrier → shard_table read ordering in dispatch (`dispatch.rs:394` then `:1453`); coordinator takes shard_table without the barrier — no reverse edge. Checkpoint takes the exclusive barrier first (`checkpoint.rs:358`), excluding all mutations, so its index/allocator/redo acquisitions cannot interleave. ❌ The cross-NODE ordering is broken: barrier(A) → network → barrier(B) is a distributed cycle (HIGH finding 1).
8. **tokio fire-and-forget tasks:** ✅ No fire-and-forget `tokio::spawn` exists. The only `JoinSet` (`http.rs:2096-2103`) is joined; `spawn_blocking` handles are awaited (`dispatch.rs:1532-1556`). Background work uses std threads with `Arc` data (cannot outlive it) and shutdown flags/JoinHandles: checkpoint (`checkpoint.rs:113-141`), receiver accept loop (`receiver.rs:229-261`, `running` flag), uploader/blob_gc/manager threads similarly flagged.
9. **Atomics orderings:** ✅ mostly correct: `cached_millis` SeqCst (`engine.rs:565-573`); `shard_counts` Acquire/Release with mutations inside the index write lock (`engine.rs:610-752`); `AtomicShardBitmap` Acquire/Release (`migration.rs:120-153`); io.rs Relaxed chunked transfers are correct because consistency comes from the io_locks guard + explicit fences (`io.rs:98-104,826`); `initialize_shard_counts` double-check is benign (duplicate scans produce identical values; both run under the index read lock which excludes writers, `engine.rs:621-647`). ❌ Two real problems: `voted_term` Relaxed check-then-act race (HIGH finding 2) and the non-atomic migration gauge decrements (LOW finding).

## Findings summary

| # | Severity | Title |
|---|----------|-------|
| 1 | HIGH | Exclusive visibility barrier held across replication RTT → cross-node deadlock-until-timeout under bidirectional writes; all reads stalled per mutation |
| 2 | HIGH | `handle_propose` voted_term check-then-act race → double-vote per term → split-brain enabler |
| 3 | MEDIUM | Migration baseline reads records without lock → torn per-record snapshot, replica generation drift |
| 4 | MEDIUM | Receiver `apply_op` direct device RMW bypasses stripe/io locks; standalone receiver listener wholly unprotected |
| 5 | MEDIUM | No tests for cross-node barrier interleaving or concurrent topology proposals; no loom models |
| 6 | LOW | Stale Send/Sync safety justifications (Engine, MemoryDevice) |
| 7 | LOW | Unseeded stripe selection — txid-grinding contention DoS |
| 8 | LOW | Non-atomic migration metrics gauge decrements |
