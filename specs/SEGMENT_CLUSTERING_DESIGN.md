# Clustering the Segment (Log-Structured) Storage Engine

**Status:** IMPLEMENTED (Option A). Phases 0–6 landed; C+E (§7.3) is the future
reference-parity performance milestone and remains unimplemented.
**Scope:** make `storage.engine = "segment"` run on a clustered / replicated node
(`validate_cluster_safety` now allows it under buffered durability).

## Implementation status

| Phase | What | Status |
|---|---|---|
| 0 | Design + invariants (this doc) | ✅ |
| 1 | Emit logical `ReplicaOp::Spend` for segment spends (`dispatch.rs`) | ✅ |
| 2 | Replica-side `engine.spend()` relocates; post-apply redo `None` for segment (`engine.rs`, `receiver.rs`) | ✅ |
| 3 | Option A: WAL-first fat `RedoOp::RelocateV2` via group-commit, gated on `clustered()` (`redo.rs`, `recovery.rs`, `engine.rs`) | ✅ |
| 4 | Migration / rejoin validation for segment (non-spend ops RMW in place; migration-create; full-coordinator migration) | ✅ (tests) |
| 5 | Relax the config gate; keep buffered-durability requirement (`config.rs`) | ✅ |
| 6 | Cluster e2e tests (spend convergence, defrag convergence, non-spend replication, migration; crash-consistency via `replay_relocate_v2`) | ✅ |

**Durability note (as implemented).** Under the required buffered mode, a
clustered segment node's durability = replication quorum before ack + failover +
rejoin-resync — identical to the in-place clustered default (neither fsyncs on
the ack path). The fat `RelocateV2` is still load-bearing: it lets the redo be
flushed independently of the data device (background flusher / replication) and
still reconstruct the image on replay — the thin `Relocate` would read garbage
at the un-flushed new offset. The "true" Option A (local fsync-before-ack) is the
strict-durability variant, which the config gate currently reserves for a future
opt-in (strict + segment is refused today). Tests: `tests/segment_cluster_e2e.rs`,
`tests/cluster_tcp.rs::segment_cluster_migrates_shard_with_records_to_new_node`,
and the `redo.rs` / `recovery.rs` `relocate_v2*` unit tests.

---
**Naming:** the comparison KV store is referred to throughout as *the reference
datastore* — never by product name (repo naming rule).

---

## 1. Why the segment engine is non-clustered today

Two guards in `config.rs::validate_cluster_safety` refuse it:

1. **Non-clustered** (`config.rs:1551-1567`): *"the log-structured engine is
   non-clustered in v1: its spends relocate the record (a physical move) rather
   than journaling a logical op, so the redo entries cannot be converted to
   replica ops."*
2. **Requires buffered durability** (`config.rs:1568-1587`): the relocate
   journals its `Relocate` intent AFTER the data write (the new append-cursor
   offset is only known post-allocate), so crash safety relies on the checkpoint
   barrier rather than WAL-first ordering.

The spend path additionally suppresses replication for log-structured stores:
`handle_spend_batch` gates both the in-place `RedoOp::SpendV2` and the shipped
`ReplicaOp::Spend` on `!log_structured` (`dispatch.rs:4934-4966`).

## 2. Key insight — replication is *logical*, not physical

The stated blocker ("redo entries cannot be converted to replica ops") is
misleading. **Replicas never use the master's offsets, even for the in-place
engine.** The master ships a logical `ReplicaOp` (`tx_key` + vout +
`spending_data` + `master_generation` — *no offset*); the replica applies it
through its *own* `engine.spend()` / `engine.create()`, allocates its *own*
offsets, and writes its *own* redo log:

- `ReplicaOp` enum carries no device offset (`replication/protocol.rs:125`).
- Replica create applies via `engine.create()` → own allocator
  (`receiver.rs:1117-1176`); replica spend resolves `tx_key`→own index +
  `read_slot(vout)` (`receiver.rs:1420-1449`).
- Replica writes its OWN offset into its OWN redo (`receiver.rs:2207-2229`).

So per-node offset divergence is **already the norm**, and the segment
allocator's cross-node non-determinism (append cursor + defrag reuse) does **not**
block clustering. The real blockers are narrower (§3).

## 3. The real blockers

1. **Segment spend replication is simply gated off** — the `ReplicaOp::Spend` is
   logical and engine-agnostic; it just isn't emitted for log-structured stores.
2. **Durability ordering** — the real correctness gap. The in-place engine ships
   the `ReplicaOp` *after* a WAL-first `SpendV2` fsync; the segment relocate
   journals a `Relocate` **buffered, after the write**, and never fsyncs on the
   ack path (offset known only post-allocate). Replication's crash-consistency
   contract (master's write durable-or-replicated before it acks) is violated.
3. **Replica-side apply must journal a relocate** with the replica's own new
   offset (mirror of the master), baking in the master's `generation`.
4. **Migration / rejoin** must be validated for segment records.

## 4. The durability contract we must preserve

For the in-place engine, a clustered `STATUS_OK` on a spend means, in order
(`dispatch.rs` phases 3→5):

1. **redo append + fsync** (`GroupCommit` leader fsync, coalesced) — *before*
   apply (`dispatch.rs:5015`);
2. apply (`dispatch.rs:5073`);
3. release visibility barrier;
4. ship `ReplicaOp::Spend` (`dispatch.rs:5191`);
5. build client ack.

So an ack ⟹ **fsync-durable locally AND replica-quorum-ACKed** (`WriteAll` /
`WriteMajority` are the only policies allowed at RF>1; `best_effort` is
config-banned, `config.rs:1605-1620`). On quorum miss the master **rolls back
locally + compensates** (`compensate_replication_failure`, `dispatch.rs:3858`).
The in-place `SpendV2` redo is **self-sufficient** (carries `spending_data`), so
only the *redo* fsync is needed; the data device stays buffered.

The segment relocate is index-only (`recovery.rs:2869` reads the record back
from `new_offset`) and buffered — so it skips step 1's per-mutation fsync, and
because the redo is index-only the *data* at `new_offset` would also have to be
durable for replay. There is **no reverse catch-up** today: a master that loses
its buffered tail cannot heal it from a replica (recovery is local-redo-only).

---

## 5. Phase plan

Each phase is gated on the cluster e2e suite (Phase 6).

### Phase 0 — Design + invariants (this doc)
Decide the Phase-3 durability model (§7). Invariant: **a segment replica
applying a logical spend relocates its own record to its own offset, baking in
the master's `generation`** — logical state (tx_key / vout / spent-status /
generation) is identical across nodes; physical offsets legitimately diverge.
Defrag is **per-node and uncoordinated** (a relocate preserves logical identity).

### Phase 1 — Emit logical replication for segment spends
Un-gate `ReplicaOp::Spend` for log-structured stores in `handle_spend_batch`
(`dispatch.rs:4934-4966`). Keep suppressing the in-place `RedoOp::SpendV2` (the
master journals a relocate instead), but stop suppressing the shipped
`ReplicaOp::Spend`. The op is already vout-based + carries `master_generation`.
*Small, mechanical.*

### Phase 2 — Replica-side apply journals the relocate
`build_post_apply_redo_op` (`receiver.rs:2052+`) maps a replicated Create →
`RedoOp::ReplicaCreate`. Add/fix the **Spend** arm so that when the replica's
store is `log_structured`, a replicated spend journals a relocate with the
replica's own post-apply offset (mirroring how Create reads `entry.record_offset`
back). Ensure `engine.spend()` on the replica **bakes in `master_generation`**,
not a locally-incremented one, so idempotency (`receiver.rs:1395`) and cross-node
generation equality hold.

### Phase 3 — Reconcile buffered durability with the replication ordering
The hard part. See §7 (durability model) — Option A ships first; Option C+E is
the reference-parity performance milestone.

### Phase 4 — Migration / rejoin / rebalance validation
A joining node receives records via the migration Create path. Segment create
already emits index-only `CreateV2` (`dispatch.rs:6288`), and a joining segment
node's `engine.create()` allocates its own offset — so this *should* work, but
must be validated end-to-end: migration completion, the superset proof, and the
pre-existing **REASSIGNED-flag-not-carried-by-migration-Create** gap. Confirm
setMined / freeze / unspend / reassign / delete replicate fine on segment (they
still RMW in place on segment — only spend relocates in v1).

### Phase 5 — Relax the config gate
Drop the segment+clustering refusal in `validate_cluster_safety`
(`config.rs:1551`). Keep the buffered-durability requirement reconciled with the
Phase-3 choice. Update the stale coordinator comment (`coordinator.rs:6663`).

### Phase 6 — Cluster e2e tests (TDD; gate every phase)
- Spend replication master→replica; assert replica **logical** state == master
  (tx_key / vout / spent / generation), offsets may differ.
- **Master failover**: kill master mid-spend-stream, promote replica, assert no
  lost/duplicated spends.
- **Rejoin/resync** of a stale node under the segment engine.
- **Defrag under replication**: independent defrag on master + replica, assert
  logical convergence.
- Crash-consistency for the Phase-3 durability model.

---

## 6. Effort / risk

Phases 1–2 are small and mechanical. **Phase 3 is the design decision and the
main risk** (crash-consistency of buffered durability under replication —
consensus-adjacent). Phase 4 is mostly validation. Multi-PR effort.

---

## 7. Phase 3 deep-dive — the durability model

### 7.1 Option A — fat `RelocateV2`, WAL-first, group-committed (correct, low-risk, heavier)

Make the segment relocate self-sufficient like the in-place `SpendV2`/`Create`:
carry the image in the redo so **one coalesced redo fsync** restores the
guarantee, and the **data device stays buffered**.

New `RedoOp::RelocateV2 { tx_key, device_id, new_offset, utxo_count,
record_bytes: Arc<[u8]> }` — fat sibling of `Relocate`, mirroring the existing
`Create`/`CreateV2` split. Reorder the relocate to WAL-first (the new offset is
an in-memory cursor bump, known before any write):

```
allocate new_offset (in-memory cursor bump)
build relocated image in RAM (read old + overlay mutation)      # already done, steps 3–4
journal RelocateV2{new_offset, image} ─► committer.commit()     # GROUP-COMMIT fsync, WAL-first
apply: write image to write-back cache (buffered), repoint index, dead-mark old
ship ReplicaOp::Spend (logical, vout-based)
ack
```

On crash after the fsync, `replay_relocate_v2` re-writes `record_bytes` at
`new_offset` (like `replay_create`) — the buffered data write is fully
reconstructable, so **no data-device fsync is needed**.

**Crash sequences (Option A):**

| crash point | master | replica | outcome |
|---|---|---|---|
| before redo fsync | nothing durable | never shipped | write lost, never acked ✓ |
| after fsync, before ship | durable (replay re-writes image) | not yet | master ahead → `run_catchup` heals replica ✓ |
| after ship+replica-ACK, before client ack | durable | durable | client retries (idempotent via `master_generation`) ✓ |
| after client ack | durable | quorum durable | full commit ✓ |

Identical to the in-place contract. Quorum-miss rollback reuses
`compensate_replication_failure`.

**Performance:** one coalesced redo fsync per group (route the relocate through
`committer.commit()` instead of the bare `log.lock().append` — the single biggest
lever); data device stays buffered; sequential appends preserved; only
clustered/RF>1 nodes pay it (gate `RelocateV2` on `is_clustered()`, mirroring
`Create`/`CreateV2`); stack `redo_buffered_io` so even the redo fsync is a cheap
page-cache flush. **Cost:** the fat redo carries the full image, so a clustered
segment spend writes ~2× record bytes (data append + redo append) vs in-place's
small `spending_data` redo — but both are sequential and only the redo is
fsync'd, so on a write-bound device segment clustered should still beat in-place
clustered (needs h2h to confirm). The replica mirrors this (its own relocate
journals `RelocateV2` + fsyncs before it ACKs).

### 7.2 How the reference datastore does it (for contrast)

The reference datastore reaches maximum clustered performance by doing the
**opposite** of Option A. As understood from its public architecture:

- **Storage = log-structured, no separate WAL.** Index in RAM, data on SSD;
  records stream into large sequential write blocks; an update is copy-on-write
  (new location + index repoint), old slot reclaimed by defrag. The sequential
  device write *is* the durable record — there is no separate redo. This is the
  segment engine's relocate-on-spend + defrag, natively.
- **Durability = replication, not local fsync.** Its default acks a write once
  it is in the master's in-RAM streaming write buffer AND synchronously
  forwarded to the replica's buffer — *neither has fsync'd yet*. Buffers flush
  async on a timer. So an acked write survives a single-node crash (the replica
  has it); a *correlated* master+replica power loss before flush can lose it — a
  documented trade-off. A strict "commit-to-device" mode fsyncs the block before
  ack for those who need it.
- **Crash-heal from replicas via versioned rebalance.** On restart, a
  partition-version comparison decides which node holds the authoritative
  (freshest) copy; migration copies it back. A crashed master that lost its
  un-flushed tail is healed from the surviving replica.

That is: **the reference does E + C together** (no separate WAL; durability from
synchronous replication; crash-heal from replicas). Option A is deliberately
*heavier* — it keeps TeraSlab's separate redo AND adds a fsync before ack — so it
will be slower than the reference's clustered path. Matching the reference
requires C + E (§7.3).

### 7.3 Option C + E — reference parity (max performance, bigger, consensus-critical)

**Model:** keep the segment engine's existing fast async path unchanged — thin
index-only `Relocate`, buffered write-back data, **no pre-ack fsync** (that is E:
"the append is the store"; the thin redo is only a fast index-rebuild hint).
Durability comes from **synchronous replication** (write on the replica quorum's
buffers before ack — already the model) plus **C: a reverse catch-up** that heals
a crashed master from a fresher replica. The accepted cost is the
**correlated-crash window** (master + whole replica quorum lose un-flushed
buffers together) — bounded to ≤ one checkpoint interval and to the *unmined*
tail (finalized data is checkpoint-durable via `last_durable_height`). Offer
Option A as the strict opt-in (the "commit-to-device=true" analogue).

**What already exists to reuse:**
- sequence-bounded redo reader (`read_redo_from_sequence_merged`, `ops_from_seq`)
  + truncation detection (`RedoReclaimed`/`NeedsResync`);
- migration baseline+delta state machine (`MigrationProgress { snapshot_sequence,
  fence_sequence }`, `migration.rs:260`), with source-rewrite to "whichever peer
  has data" (`build_plan_from_partition_view`, `coordinator.rs:925`);
- per-shard serving fences (`Transitioning` / `inbound_atomic` / `fenced_bm` →
  `ERR_MIGRATION_IN_PROGRESS`; `No` → `STATUS_REDIRECT`, `dispatch.rs:4602-4740`);
- `elect_master` scoring (`is_subset`/`was_previous_master`, `coordinator.rs:7154`);
- per-record `generation` idempotency (`record.rs:1114`);
- `TopologyTerm.term` (cluster-wide ownership generation).

**What is missing** (the map is blunt: *"the engine does not currently track
per-shard replication sequence numbers"*; `PartitionVersionEntry.last_applied_seq`
ships `shard_record_count`, a `>0` proxy): a **per-shard durable write
high-water**, a **data-recency arbiter**, and a **pull-direction transport**.

**C.1 — Per-shard durable write high-water (the one foundational new primitive).**
Per shard, two monotonic markers on the global redo sequence:
- `applied_hwm[shard]` — max redo-seq applied (in-RAM);
- `durable_hwm[shard]` — max redo-seq made durable (advances at the checkpoint
  barrier after `flush_all_redo` + `sync_all_store_devices`). **Recoverable** as
  the max seq-per-shard in the recovered durable redo prefix — derive during
  `recover()`, no new persistence. **Must be conservative** — never claim a seq
  the checkpoint hasn't fsync'd, or a real gap goes undetected.

**C.2 — Exchange it.** Replace the record-count proxy in
`PartitionVersionEntry.last_applied_seq` (`dispatch.rs:9990`) with the real
`durable_hwm[shard]` (advertise `applied_hwm` too). The partition-view exchange
(`ExchangePhase`, `coordinator.rs:864`) already gathers one entry per shard from
every node → every node can see who holds the freshest copy of S.

**C.3 — Detect the gap on restart (the new arbiter).** Today `is_master()`
(`coordinator.rs:7773`) has no data-recency check — a restarted master that lost
its tail reads `Yes` and serves stale. Add: a node is authoritative for S only if
it wins HRW/election AND `durable_hwm[S] ≥ max(replica applied_hwm[S])`. If a
replica is ahead → the ex-master is **stale for S** → per-shard CATCHING-UP
state. Extend `elect_master` to also prefer the higher `durable_hwm` so the
fresher replica transiently masters S. This gate must **compose with `term`
ordering** (not fight it) to avoid split-brain.

**C.4 — Pull transport = reversed, delta-bounded migration.** Catch-up is
sequence-bounded today but push-only (source→target); there is no target pull.
Don't build a new transport — **reverse the existing migration**: emit a task
with `source = fresher replica`, `target = stale ex-master` (already supported by
`build_plan_from_partition_view`'s source-rewrite). Two tweaks: (a) **trigger** on
data-staleness at the same topology term (C.3), not only ownership change; (b)
**delta-only** — set the migration baseline `snapshot_sequence` = the target's
`durable_hwm[S]`, streaming only records touched after it. Crucially this is
**state-transfer, not op-replay**: the replica ships the *current record image*
(idempotent upsert carrying its `generation`) for each record with seq > the
target's high-water; the stale master upserts it, idempotent by
`master_generation`. This sidesteps "the replica's redo has physical offsets, not
logical ops" — we transfer state, not the replica's redo.

**C.5 — Availability gating (exists — reuse verbatim).** While S heals, mark it
inbound/`Transitioning` on the stale ex-master → `is_master()` returns
`Transitioning` → dispatch rejects/redirects to the acting (fresher) master.
Clear the fence when `durable_hwm[S]` reaches the high-water.

**Crash sequences (C+E):**

| event | outcome |
|---|---|
| single-node master crash after ack | replica quorum holds it; on restart C.3 detects the gap, C.5 fences S, C.4 pulls the delta as state-transfer, converges ✓ |
| ex-master re-applied a partial tail from its own redo | state-transfer idempotent by `generation` — no double-apply, no gap ✓ |
| correlated crash of the whole quorum before flush | write lost cluster-wide (accepted `commit-to-device=false` window, bounded to unmined tail) — use strict/Option-A to eliminate ✗-by-design |
| finalized/mined data | always checkpoint-durable locally — never in the window ✓ |

**Risks to nail before coding:** (1) `durable_hwm` conservatism (C.1); (2)
split-brain — two nodes each "freshest master" for S; the `TopologyTerm` quorum +
`elect_master` tie-break must arbitrate and compose with the recency gate; (3)
the correlated-crash window is a real durability-contract change — document it and
gate the strict/Option-A escape hatch on config.

### 7.4 Recommendation & sequencing

1. **Ship Option A first** (correct, heavy) — clustering works at all; build the
   e2e/failover suite (Phase 6).
2. Build **C.1 (the hwm primitive) + C.2/C.3** — get the arbiter + fencing correct
   under fault injection.
3. Build **C.4 (reversed delta migration)** — actually heal a lost tail.
4. Flip the default from A to C+E; measure the h2h against the reference. **E**
   falls out for free once C works (it is "keep the async path + don't fsync").

Option A restores the exact in-place durability+replication contract with proven
crash-consistency and reuses existing machinery; C+E imports the reference's
"rebalance heals durability" model for parity — the only new load-bearing
invention across the whole effort is the per-shard durable high-water (C.1).
