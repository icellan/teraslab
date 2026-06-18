# Deletion Tombstones — Architecture Design Document

Status: PROPOSED (review before implementation)
Author: design pass for task #36 (the reverted, unsound `rejoin deletion-reconciliation`)
Scope: design only. No production code changes accompany this document.
Audience: senior reviewer who can approve/reject before any code lands.

---

## 0. TL;DR recommendation

Add an **authoritative, durable, height-bounded deletion tombstone log**: when the
cluster physically removes a record (`Engine::delete`, `src/ops/engine.rs:4944`),
also append a tiny `(txid, shard, deletion_height, cause)` tombstone to a new
**append-only on-device tombstone log** with a redb-backed lookup index. Tombstones
replicate master→replica as a new `ReplicaOp::Tombstone`, survive restart because
they live outside the linear-reset redo window, and are garbage-collected once the
deletion's `deletion_height` falls below a cluster-wide **safe-rejoin horizon**
(`min(finalized_height) − rejoin_grace_blocks`) past which no node can rejoin still
holding a stale live copy of that key.

The payoff: `OP_MIGRATION_COMPLETE` reconciliation
(`src/server/dispatch.rs:1199-1352`) is redefined so a rejoinee `N` classifies every
local key it holds that the authoritative source omits as **either** "source has a
tombstone for it → DROP on N (authoritative deletion)" **or** "source has neither
a live copy nor a tombstone → never-received → TRANSFER to master (no-loss)". This
replaces today's conservative "retain on doubt, leave shard fenced" heuristic
(#29 prune gate) with an authoritative answer, letting the over-count converge while
staying no-loss and resurrection-proof.

The honest verdict (section 11): this is **worth building**, but it is a real
feature — roughly 1500–2500 LOC across 6 modules plus a new on-device region and a
new GC daemon — and the GC horizon is the part that can silently reintroduce
double-spends if gotten wrong. Recommend building it gated behind a config flag,
with the conservative #29 retain-path kept as the fallback until soak validates the
horizon.

---

## 1. The problem, grounded in the code

### 1.1 What "delete" does today

`Engine::delete` (`src/ops/engine.rs:4944-5038`) physically removes a record:

1. Zeroes the on-device metadata header (`write_zeroed_metadata_header`,
   `engine.rs:4981`) — a **crash-recovery skip-guard**, so a rebuild can't reparse
   the freed region. It is *not* a queryable record of the deletion.
2. `device.sync()` (`engine.rs:4983`).
3. `unregister_with_shard_count` — removes the **redb primary-index row**
   (`engine.rs:5002`). redb is durable and survives restart.
4. Frees the region to the allocator (`engine.rs:5011`).
5. Cleans DAH/unmined secondaries (`engine.rs:5030-5035`).

After this returns, the key is simply **absent**. There is no durable artifact that
says "this key was deleted." The absence is indistinguishable from "never created
here."

### 1.2 Who deletes, and when

Three physical-removal callers, all converging on `Engine::delete`:

- **DAH sweep** (the dominant path). A UTXO becomes deletable only when
  `evaluate_delete_at_height` (`src/ops/delete_eval.rs:71`) sets
  `delete_at_height = current_height + block_height_retention` under the predicate
  `spent_utxos == utxo_count && has_blocks && unmined_since == 0 && !REASSIGNED`
  (`delete_eval.rs:115-124`). The sweep later calls `delete` with
  `due_guard = Some(height)` and re-validates under the stripe lock
  (`engine.rs:4957-4972`). This is the **spent-and-mined → physically removed**
  case — the one that causes resurrection double-spends.
- **Admin delete** — direct client `DeleteRequest { due_guard: None }`
  (unconditional, spec §3.18).
- **Migration prune** (#29) — `OP_MIGRATION_COMPLETE` deleting local keys the
  authoritative manifest omits (`dispatch.rs:1210`).

The deletion is **authoritative at the cluster level**: once the DAH sweep removes a
spent+mined UTXO on the rightful master, that UTXO is gone for good. Spend-once
semantics (spec / SPEC_BRIEFING) mean it can never legitimately come back.

### 1.3 Why rejoin breaks (the proven failure)

The redo log is **linear-with-reset, not circular** (`src/redo.rs:6-15`): `write_pos`
advances monotonically and the checkpoint task resets it to zero after snapshotting
engine state. So a `RedoOp::Delete` (`redo.rs:527`) is **discarded at the next
checkpoint** — its lifetime is at most one checkpoint interval, far shorter than a
rolling-restart outage.

Node `N` goes down. While down, the cluster's rightful master DAH-sweeps some UTXOs
(spent+mined) and physically deletes them. `N`'s own redb primary index still
contains those rows (it was offline; nobody deleted them *on N*, and the deletes that
happened elsewhere were never durably recorded on N in a form that outlives the redo
reset). `N` reboots, replays its redo, and serves its **full pre-shutdown index** —
including the now-deleted records.

When `N` rejoins and reconciles against the rightful master, `N` holds an **extra
set** the master lacks. That set is an unmarkable mix of:

- **(a) never-received**: records the master never got (lost to baseline-stream
  failures during epoch churn). These must be **transferred TO the master** (no-loss).
- **(b) deleted-while-down**: records the cluster spent-and-deleted while `N` was
  down. These must be **DROPPED on N** (else resurrection = double-spend).

The master's **absence of a key is ambiguous** between (a) and (b). That ambiguity is
exactly why the three prior fixes failed (mirrored in tasks #36/#38):

- *drop-all the extras* → drops (a) → **data loss**.
- *source-serialization of migration* → **throughput regression** (#38 Fix A).
- *push-all the extras to master* → pushes (b) back → **resurrection / double-spend**.

A tombstone is the missing third value: it makes the master's absence
**self-describing** — "absent because deleted" vs "absent, unknown."

---

## 2. Tombstone content & granularity (RECOMMENDED)

**Granularity: per-record (per-txid), keyed by `TxKey`.**

Rationale: `Engine::delete` operates at whole-record granularity — a record is
removed only when **all** its UTXO slots are spent (`spent_utxos == utxo_count`,
`delete_eval.rs:115`). There is no partial-record physical deletion; slot-level
removal is `prune_slot`, which does not free the record. So the unit of "the cluster
authoritatively removed this" is the `TxKey` (the 32-byte txid; `src/index/mod.rs`
`TxKey`). Per-output tombstones would be strictly larger with no added discriminating
power for the rejoin problem, which is purely "does this key still exist."

**Tombstone record (fixed 56 bytes on the wire / in-log):**

| field | bytes | purpose |
|---|---|---|
| `txid` | 32 | the deleted key (matches `TxKey.txid`) |
| `shard` | 2 | shard id, so GC and migration reconciliation are O(shard) without recomputing placement |
| `deletion_height` | 4 | block height at which the deletion became authoritative (the sweep's `current_block_height`, or admin-delete's observed tip). Drives the GC horizon. |
| `generation` | 4 | the record's `generation` (`src/record.rs:524`) at deletion time. Lets reconciliation distinguish "this exact version was deleted" from "a newer re-creation exists" — defends against the create-after-delete race (section 8.4). |
| `cause` | 1 | enum: `SpentDah=0`, `Admin=1`, `MigrationPrune=2`. Diagnostic + GC policy hook (admin deletes may use a different horizon; see 4.4). |
| `flags` | 1 | reserved (e.g. EXTERNAL-blob-pending), zero today |
| `_pad` | 4 | align to 8 |
| `crc32` | 4 | integrity of the entry (matches redo-entry CRC convention, `redo.rs`) |

Total 56 bytes, 8-byte aligned. `#[repr(C, packed)]` with a compile-time size
assertion, per the project's byte-layout rules (CLAUDE.md).

Why `deletion_height` and not a wall-clock timestamp: the GC horizon is defined in
**block height** (finality is a height concept in BSV), and the DAH sweep already
operates on heights. Wall-clock would couple GC safety to clock skew across nodes —
unacceptable.

Why store `generation`: see section 8.4. Without it, a key that was deleted at gen 5
and *legitimately re-created* at gen 6 on the master (a new tx reusing the txid is
impossible under txid semantics, but a reorg-driven unspend→respend can re-create a
record) could be wrongly dropped by a tombstone. Carrying generation makes the
tombstone "this version is dead" rather than "this txid is forever dead."

---

## 3. Durable storage (RECOMMENDED)

**A dedicated append-only on-device tombstone log + a redb-backed lookup index.**

Two structures, mirroring the existing redo/redb split:

### 3.1 Tombstone log (on-device, append-only, NOT reset)

A new fixed device region (sized at config time, like the redo region —
`redo.rs:17-38`), written via the same `O_DIRECT` `device` module. Crucially, unlike
the redo log it is **not reset on checkpoint**. It is append-only and is **compacted
only by GC** (section 4), which physically reclaims the prefix below the safe
horizon. This is what makes tombstones outlive the linear-reset redo window.

Layout mirrors `redo.rs`: a `TombstoneHeader` block (magic + version + `next_seq` +
`compacted_through_height` + CRC) followed by 56-byte entries. Compaction rewrites the
header's `compacted_through_height` and drops the reclaimed prefix.

### 3.2 Tombstone index (redb)

A new redb table `tombstones: TxKey -> (deletion_height, generation, shard, cause)`,
alongside `redb_primary.rs` / `redb_dah.rs` / `redb_unmined.rs`. This gives O(1)
`is_tombstoned(key)` for the migration reconciliation hot path and the receiver's
idempotency check, without scanning the log. The log is the durable source of truth;
the redb table is a derived index rebuilt from the log on recovery (section 5).

### 3.3 Footprint & SSD-wear cost (quantified)

This store targets 10M+ ops/sec and is wear-sensitive, so the tombstone write cost
must be bounded.

- **Per-deletion cost**: one 56-byte append to the log + one redb insert. The log
  append is **batched and coalesced with the delete's existing `device.sync()`**
  (`engine.rs:4983`) — the delete already fsyncs, so the tombstone append rides the
  *same* sync. **Net new fsyncs: zero on the hot path.** The marginal write is 56
  bytes appended to an already-dirty, already-synced region.
- **Steady-state volume**: deletions happen at roughly the spend-and-mine rate, which
  is a fraction of total ops (most ops are spends/creates, not whole-record
  deletions). At a pessimistic 1M deletions/sec, 56 B/each = 56 MB/s of tombstone
  writes — small next to the record I/O, and append-only (sequential, the
  SSD-friendliest pattern). The redb index insert is the larger cost; it is the same
  order as the redb primary-index *delete* that `delete` already performs
  (`engine.rs:5002`), i.e. roughly doubling the index-write cost of a deletion, not
  of an op.
- **Bounded total size**: because GC reclaims everything below the horizon
  (section 4), the on-device log is bounded by `deletion_rate ×
  horizon_window_in_blocks × 56 B`. At 1M deletions/s and a ~2016-block (~2 week)
  horizon (~1.2M seconds), that is a worst-case ~67 GB — large, so the **horizon
  window must be tuned down** (section 4.5 recommends tying it to finality, ~hours not
  weeks, giving single-digit GB). This is the single most important sizing knob and is
  called out as a risk in section 11.

Alternatives for storage rejected in section 10 (reuse DAH index; on-device markers
in freed regions; keep-everything).

---

## 4. GC / bounded retention — the hard part

A tombstone exists to stop a **stale rejoinee** from resurrecting a deleted key. It is
safe to drop a tombstone for key `k` **only when no node that could still rejoin holds
a stale live copy of `k`.** Define that precisely.

### 4.1 The danger a tombstone guards against

A node `N` holds a stale live copy of `k` iff: `k` was deleted by the cluster after
`N`'s last durable view of `k`, AND `N` has not since reconciled (re-synced) against an
authoritative source. The window during which such an `N` can exist and later rejoin
is bounded by **how stale a rejoinee is allowed to be**.

### 4.2 The horizon rule (RECOMMENDED)

A tombstone with `deletion_height = h` is safe to GC once:

```
cluster_finalized_height − h  ≥  rejoin_grace_blocks
```

where:

- `cluster_finalized_height` = the **minimum** finalized/committed block height across
  all *current committed members* (not just self). A node only finalizes a height once
  it has the longest chain up to it. Taking the **min across members** ensures the
  horizon advances no faster than the slowest live member.
- `rejoin_grace_blocks` = a config bound = the **maximum staleness a rejoining node is
  permitted before it must do a full resync instead of an incremental rejoin**. A node
  whose last durable height is more than `rejoin_grace_blocks` behind the cluster tip
  is **refused incremental rejoin** and forced into full-baseline resync (which
  carries no stale extras because it discards local state). This is the load-bearing
  coupling: GC and rejoin-eligibility share the *same* bound, so a node that could
  still hold a tombstone-needing stale copy is *by definition* still inside the window
  where its tombstone is retained.

### 4.3 Why this cannot drop a tombstone a laggard still needs

Suppose tombstone for `k` (deleted at height `h`) is GC'd. By the rule,
`min_member_finalized_height − h ≥ rejoin_grace_blocks`. Now suppose a laggard `N`
rejoins still holding a stale live `k`. For `N` to hold a *stale* copy, `N`'s last
durable height `d_N < h` (it never saw the deletion). For `N` to be admitted to
**incremental** rejoin, rejoin-eligibility requires
`cluster_tip − d_N < rejoin_grace_blocks`, i.e. `d_N > cluster_tip −
rejoin_grace_blocks ≥ min_member_finalized_height − rejoin_grace_blocks ≥ h`. So
`d_N > h`, contradicting `d_N < h`. Therefore **any `N` stale enough to need the GC'd
tombstone is too stale to be admitted incrementally** and is instead full-resynced
(which drops its stale `k` anyway). The horizon is sound. ∎

The argument's only load-bearing assumption: **rejoin-eligibility is gated by the same
`rejoin_grace_blocks` bound and a node past it is forced to full resync.** This gate
must be implemented as part of this feature (section 9, step 4) — it is not optional
sugar; the GC proof depends on it.

### 4.4 Cause-specific horizons

- `SpentDah` / `MigrationPrune`: use the height horizon above.
- `Admin`: admin deletes are out-of-band and may not correspond to a spent UTXO. Keep
  admin tombstones for the **same** horizon (simplest, safe). A shorter horizon would
  be unsound; a longer one only wastes space. Recommend: same horizon, revisit only if
  admin-delete volume proves problematic (it is rare).

### 4.5 Tuning `rejoin_grace_blocks`

Tie it to **finality**, not to a generous "two week" outage window. BSV practical
finality is on the order of hours of blocks. Recommend defaulting `rejoin_grace_blocks`
to a finality-scale value (e.g. a few hundred blocks) and **refusing incremental rejoin
beyond it**. This keeps the tombstone log in the single-digit-GB range (section 3.3)
and makes the GC window operationally meaningful: "if you were down longer than
finality, you full-resync." A larger window trades disk for tolerating longer
incremental rejoins; that is the operator's dial.

### 4.6 GC mechanism

A periodic daemon (sibling to the checkpoint task), under config cadence:

1. Compute `safe_height = min_member_finalized_height − rejoin_grace_blocks`.
2. Delete redb tombstone rows with `deletion_height < safe_height`.
3. Compact the on-device log prefix, advancing `compacted_through_height` in the
   header and fsyncing the header before reclaiming the prefix (crash-safe: a crash
   mid-compaction re-derives the index from the surviving suffix; the dropped prefix
   was already proven safe).

GC must read `min_member_finalized_height` from the **committed membership view**
(coordinator), so a transiently-partitioned member that is no longer committed does not
pin the horizon forever. A member that is *down* (not committed) does not hold the
horizon back — but a *down* member that later rejoins is governed by the rejoin gate
(4.3), so this is safe.

---

## 5. Recovery reconstruction

Two requirements: (R1) tombstones survive the restart of the node that needs them;
(R2) a restarting node re-applies tombstones to **purge any record it resurrects from
its own durable index**.

### 5.1 R1 — durability across restart

The on-device tombstone log is the source of truth and is not reset (section 3.1). On
`recover()` (`src/recovery.rs:267`), after the redo replay and primary-index load, add
a **tombstone-log scan** that rebuilds the redb `tombstones` table from the log
(validating each entry's CRC; a torn tail entry is dropped exactly like a torn redo
tail). This is O(live-tombstones), bounded by section 3.3, and runs once at boot.

### 5.2 R2 — self-purge on restart (the critical step)

This is what makes a restarting `N` safe **even before it rejoins**. After
reconstructing the tombstone index, recovery runs a **purge pass**: for every key in
the rebuilt tombstone index, if `N`'s primary index still holds that key at a
`generation ≤ tombstone.generation`, **delete it locally** (route through
`Engine::delete`, which also writes a fresh tombstone — idempotent, the key is the
same). This collapses (b)-class records *on N's own device* before `N` ever serves a
read or rejoins.

But note the failure scenario: the deletions that orphaned `N` happened **on the
master while N was down**, so `N`'s *own* tombstone log does **not** contain them — `N`
never observed them. So R2 alone does **not** fix the rejoin problem; it only fixes
the case where `N` itself deleted-then-crashed-before-index-removal-durable. The rejoin
problem is fixed by **replicating tombstones to N** (section 6) and by **migration
reconciliation pulling the master's tombstones** (section 7). R2 is the local-crash
safety net; sections 6–7 are the cross-node fix.

---

## 6. Replication

A delete must propagate as a tombstone master→replicas, so a replica that applied the
delete *also* records the tombstone (and so a replica's own restart self-purges, 5.2).

**New `ReplicaOp::Tombstone { tx_key, deletion_height, generation, cause }`**
(extend the enum at `src/replication/protocol.rs:113`).

Why a new op rather than extending `ReplicaOp::Delete` (`protocol.rs:190`):
`ReplicaOp::Delete` carries no generation and is treated as idempotent-remove
(`receiver.rs:1850-1860`, and `master_generation` is `None` for Delete,
`protocol.rs:244`). Tombstones need `generation` and `deletion_height` as
first-class fields for GC and the create-after-delete defense. Cleaner to add an op
than to overload Delete with optional trailing fields and version-sniff. The master
emits **both** today's `Delete` (to remove the record on the replica) **and** the new
`Tombstone` in the same batch — or, simpler and recommended, the receiver's
`ReplicaOp::Delete` handler is changed to *also* write a tombstone when the op
carries the new fields, and `Delete` is bumped to a `DeleteV2` that carries
`deletion_height`/`generation` (mirroring the `CreateV2`/`SpendV2` versioning
precedent in `redo.rs:803` and `protocol.rs`). Either is fine; **recommend DeleteV2**
to keep one op per logical action and reuse the existing batch ordering/idempotency.

**Ordering & idempotency**: the tombstone write is idempotent on `tx_key` —
re-applying is a no-op (same row). It must be ordered *after* the record removal in
the batch so a crash between them leaves "record gone, tombstone pending," which R2
re-derives harmlessly, never "tombstone present, record still live and servable"
(which would let a read see a key the cluster considers dead — acceptable too, since
the tombstone purge would remove it, but record-first is cleaner). Use the existing
`OP_REPLICA_BATCH` framing (`receiver.rs:574`); tombstones piggyback, no new TCP op.

A replica that receives a `Tombstone` for a key it never had: still records the
tombstone (cheap, 56 B). That is *desirable* — it pre-arms the replica so if it later
rebuilds/receives that key from a stale source, it self-purges. (This is the same
"pre-arm" benefit as section 6's note on replicas receiving tombstones for absent
keys.)

---

## 7. Migration reconciliation — the payoff

Redefine `OP_MIGRATION_COMPLETE` (`src/server/dispatch.rs:1199-1352`) so the manifest
carries the source's **tombstones for the shard** alongside its live
`(txid, generation)` entries. The completion frame gains a `tombstone_entries:
Vec<(txid, generation)>` section (versioned frame flag, like the existing
`FLAG_MIGRATION_SUPERSET_OK`, `coordinator.rs:6006`).

Reconciliation for each local key `k` the rejoinee/target `N` holds for the shard,
against the authoritative source manifest:

| `k` in source-live | `k` in source-tombstones | action on N | rationale |
|---|---|---|---|
| yes | — | **keep + reconcile generation** (existing exact-entry path, `dispatch.rs:1251-1284`) | normal superset case |
| no | yes (gen ≥ k's gen) | **DROP on N** | authoritative deletion — resurrection-safe |
| no | no | **TRANSFER to master** (push k up) | never-received — no-loss |
| no | yes (gen < k's gen) | **keep** (k is a newer re-creation) | tombstone is for an older version (section 8.4) |

This is the exact disambiguation the three prior fixes lacked. Concretely:

- The **#29 prune gate** (`dispatch.rs:1199-1227`) today deletes local keys the
  manifest omits *only* when the source is `source_is_authoritative_complete`. With
  tombstones, the deletion decision no longer needs the conservative
  "authoritative-complete or retain everything" gate: a key is dropped iff the source
  presents a **tombstone** for it. A key the source merely *omits* (no live entry, no
  tombstone) is **never** dropped — it is transferred up. So the gate stops being
  "prune the whole shard down to the manifest" and becomes "drop exactly the
  tombstoned keys; push the rest up." This removes the data-loss footgun #29 was
  patching around.
- **`confirm_target_holds_superset`** (`coordinator.rs:5977`) and the
  `failed_handoff_disposition` `target_holds_superset` proof (`coordinator.rs:698,718`)
  become *stronger*: the relinquishing source no longer needs the target to hold a
  superset of *everything* — it needs the target to hold a superset of `self`'s
  **non-tombstoned** keys, with `self`'s tombstoned keys allowed to be absent on the
  target. This lets the legitimate relinquish complete in cases that today stall.
- The **commit gate** `!has_pending_inbound_shard` (`dispatch.rs:1415`) is unchanged;
  tombstone reconciliation is per-source and idempotent, so multi-source still unions
  correctly.

**Result**: the over-count `extras = (a) ∪ (b)` is now fully partitioned. `(b)`
(tombstoned) is dropped on `N` → over-count shrinks → convergence. `(a)` (neither live
nor tombstoned) is transferred up → no-loss. And `(b)` is *never* pushed to the master
→ no resurrection. The shard can commit (unfence) because the reconciliation is now
*authoritative*, not a retain-on-doubt stall.

---

## 8. Interaction with existing invariants

### 8.1 What stays

- **#28 orphan cleanup** (`coordinator.rs run_orphan_cleanup`, never drops the last
  holder): stays. Tombstones don't change "never drop the last *live* holder" — they
  change "is this extra a deletion or a never-received." Orphan cleanup still guards
  the last live copy; it now additionally consults tombstones to know that a
  tombstoned key is *not* a live holder to preserve.
- **#31 migration-abort** (expected=0 stranding guard): stays. Abort still rolls back;
  tombstones only affect the *reconciliation* decision, not the abort/rollback
  machinery.
- **redo replay tolerance** (`ReplicaRecordAbsent`, `recovery.rs:134,1840`): stays —
  orthogonal; it tolerates a replica op whose record bytes are absent. Tombstones make
  *more* of these legitimately-absent (the key was deleted), which the tolerance
  already accepts.

### 8.2 What is simplified / removed

- **#29 prune gate's `source_is_authoritative_complete`** conservatism
  (`dispatch.rs:1181-1227`): the *destructive prune-to-manifest* is replaced by
  *drop-exactly-the-tombstoned-keys*. The epoch-currency check stays (it still decides
  whether a completion may commit), but the "retain the whole over-count when not
  authoritative-complete, leaving the shard fenced" branch is no longer the only
  no-loss-safe option — tombstones give an authoritative drop decision regardless of
  whether the source is the committed master, as long as the source presents the
  tombstone evidence.
- **Fix B superset path** (`dispatch.rs:1319-1352`): stays as the *keep* decision for
  live keys, but the "extra local keys are KEPT and left unservable" residual
  (`dispatch.rs:1336`, the benign over-count of task #38) is resolved: the extras are
  now classified and either dropped (tombstoned) or transferred (never-received),
  so the shard converges instead of carrying a permanent benign over-count.

### 8.3 Net invariant after tombstones

> A key is dropped on a node **iff** an authoritative tombstone for that key (at
> generation ≥ the local generation) exists. A key is **never** dropped merely because
> a peer's manifest omits it. A key the master lacks and has no tombstone for is always
> transferred up.

### 8.4 Create-after-delete (the generation defense)

A txid is globally unique, but a record can be deleted (DAH) and then a reorg can
re-create activity. The `generation` on the tombstone guards this: a tombstone for
`(k, gen=5)` authorizes dropping a local `k` only when local `gen ≤ 5`. If the master
re-created `k` and it now lives at `gen=6` on the master, the master's manifest lists
`k` as **live at gen 6** (so row 1 of the table applies: keep+reconcile), and the
stale `gen=5` tombstone is irrelevant. If `N` holds `k` at `gen=6` and the source
only has a `gen=5` tombstone (no live entry), row 4 applies: **keep** — `N`'s copy is
newer than the deletion, so `N` transfers it up. Generation ordering uses the existing
wrapping-generation comparison (`protocol.rs:206-209`).

---

## 9. Failure modes & correctness argument

Notation: `live_S(k)` = source has k live; `tomb_S(k)` = source has a tombstone for k
at gen ≥ local; `live_N(k)` = N holds k live.

**Claim A (no-resurrection)**: a spent+deleted UTXO never comes back.
A UTXO is resurrected only if some node serves it live after the cluster deleted it.
The cluster deletion emits a tombstone (sections 1.2 + 6) that is durable past the redo
window (3.1) and GC'd only past the rejoin horizon (4.3). Any node holding it stale is
either (i) inside the horizon → the source's manifest carries the tombstone → row 2 →
**dropped on reconcile**, or (ii) past the horizon → **refused incremental rejoin →
full resync → stale copy discarded**. R2 (5.2) additionally purges any copy a node
resurrects from its *own* tombstone log on restart. No path serves the deleted key. ∎

**Claim B (no-loss)**: a never-received live record is never dropped.
A key is dropped only via row 2 (`tomb_S(k)`) — i.e. only when the source presents a
tombstone. A never-received record has, by definition, *no* tombstone on the source
(the cluster never deleted it — it was simply never delivered). So it hits row 3 →
**transferred to master**. The destructive prune-to-manifest of #29 is removed (8.2),
so "manifest omits k" alone never drops k. ∎

### 9.1 Dangerous interleavings

1. **Concurrent multi-source migration of the same shard** (the tolerated union,
   `dispatch.rs:1237-1245`): each source presents its own live+tombstone manifest. A
   key tombstoned by source X but live on source Y: row 1 wins for Y's completion
   (keep), and X's tombstone does not drop it because X's completion only drops keys
   *X tombstones AND no concurrent source holds live*. **Fix**: the drop decision must
   be evaluated against the **union of all pending sources' live sets**, not a single
   source — i.e. drop k only if *no* pending source has k live AND *some* source
   tombstones it. Implement by deferring drops until `!has_pending_inbound_shard`
   (the existing commit gate, `dispatch.rs:1415`): accumulate per-source tombstones,
   and at commit time drop `k` iff `k ∉ union(live)` and `k ∈ union(tombstones)`.
   This is the one place the design must be careful; calling it out explicitly.

2. **Rejoin-during-churn** (epoch N+1 activates mid-completion): the existing
   epoch-currency gate (`dispatch.rs:1311-1317`, `completion_epoch_current`) still
   guards commit. A stale-epoch completion's tombstones are *not* applied
   destructively (treated as untrusted, same as today's stale manifest). Only a
   current-epoch authoritative source's tombstones drive drops.

3. **Tombstone-GC racing a laggard** (4.3): proven sound — a laggard needing a GC'd
   tombstone is, by the shared bound, refused incremental rejoin. The race window is
   closed by *construction* (same constant gates GC and rejoin), not by timing.

4. **Crash mid-tombstone-write**: the tombstone append rides the delete's existing
   fsync (3.3). Orderings:
   - record removed, fsync, crash *before* tombstone append → on recovery the key is
     gone from the primary index (durable) and there is no tombstone. The deletion
     still happened; the only loss is the *tombstone evidence*. Mitigation: write the
     tombstone **before** the primary-index removal and within the **same fsync**
     (reorder vs `engine.rs:4981-5002`): tombstone-append → header-zero → single
     `device.sync()` → primary-index unregister. Then a crash after the fsync but
     before unregister leaves "tombstone present, record present" → R2 purges the
     record on recovery (consistent). A crash before the fsync loses both atomically
     (the delete never durably happened — the record is still live, no tombstone — also
     consistent). So the tombstone is **never durably lost while the deletion is
     durably committed.** This reordering is a required part of the implementation
     (section 11 risk note: it touches the audited F-G2-001 delete ordering, so it
     needs care + its existing tests re-validated).
   - tombstone appended, crash before redb index insert → recovery rebuilds the redb
     index from the log (5.1), so the index entry is re-derived. No loss.

5. **Admin delete of a key with no spend history**: tombstone with `cause=Admin`,
   same horizon. No special interleaving.

---

## 10. Alternatives considered & rejected

- **Keep-everything, no GC**: trivially correct (no horizon to get wrong) but the
  tombstone log grows unboundedly at deletion rate — at 1M del/s, ~56 MB/s forever.
  Unbounded SSD growth on a wear-sensitive store. Rejected: violates the bounded-store
  premise.
- **Version vector per key**: a per-key vector clock would also disambiguate
  deleted-vs-never-received, but it adds per-key metadata to *every live record*
  (10M+ records × cluster-size vector) — a permanent footprint on the hot record,
  versus tombstones' transient footprint on the *deleted* (rarer) set. Rejected: wrong
  cost locus for this workload (penalizes the 99% live path to solve a delete-path
  problem).
- **Full anti-entropy Merkle reconciliation**: build a Merkle tree per shard and
  diff. Resolves *content* divergence but **still cannot tell deleted from
  never-received** — a leaf present on N and absent on master is the same ambiguity
  this whole doc exists to resolve. Merkle tells you *that* they differ, not *why*.
  Rejected: doesn't solve the actual problem, and is far heavier (tree maintenance on
  the hot path).
- **Accept the benign residual** (task #38's fallback): leave the over-count, keep
  extras unservable (fenced). Cheapest (no new feature). But it means shards that hit
  this never fully commit/converge under rolling restart, the over-count metric stays
  nonzero, and "unservable extras" is a latent correctness smell (a future refactor
  that unfences them resurrects). Rejected as the *primary* path, but **retained as
  the fallback** behind the config flag until soak validates the horizon (section 11).
- **Source-serialize migrations** (#38 Fix A): regressed throughput. Rejected
  (already discarded).
- **On-device markers in freed regions** (leave a "deleted" magic in the freed
  metadata header instead of a separate log): the region is returned to the allocator
  and *reused* (`engine.rs:5011`), so the marker is overwritten by the next create.
  Cannot outlive reuse. Rejected.
- **Reuse the DAH secondary index as the tombstone store**: the DAH index entry is
  *removed* by `delete` (`engine.rs:5031`) precisely when the tombstone is needed.
  Repurposing it would fight its existing lifecycle. Rejected.

The recommended dedicated-log + redb-index wins because it: (1) puts the cost on the
rare delete path, not the hot live path; (2) reuses the proven redo/redb durability
pattern; (3) bounds size via a *provably sound* horizon; (4) gives O(1) reconciliation
lookups.

---

## 11. Implementation plan, cost & risk

### 11.1 Phased steps

1. **Types + on-device log** (new `src/tombstone.rs`, modeled on `redo.rs`):
   `Tombstone` struct (`#[repr(C, packed)]` + size assert), `TombstoneLog`
   (append/scan/compact), `TombstoneHeader`. Unit tests: append/scan round-trip, torn
   tail, CRC reject, compaction reclaims prefix and re-derives. *Locally validatable.*
2. **redb tombstone index** (`src/index/redb_tombstone.rs`, modeled on
   `redb_dah.rs`): insert/lookup/range-delete-below-height. Unit tests.
3. **Engine wiring** (`src/ops/engine.rs:4944` delete): reorder to
   tombstone-append → header-zero → single fsync → unregister (section 9.1.4); write
   redb tombstone row. Re-validate the F-G2-001 ordering tests. *Risk: touches audited
   delete ordering.*
4. **Rejoin-eligibility gate** (coordinator): refuse incremental rejoin when
   `cluster_tip − self_last_durable_height ≥ rejoin_grace_blocks`, force full resync.
   **This is load-bearing for GC soundness (4.3) — not optional.** Unit + scenario.
5. **GC daemon** (sibling to checkpoint task): horizon compute from committed-member
   min-finalized-height, redb range-delete, log compaction. Unit tests for the horizon
   math; the *cross-node* horizon needs CI soak.
6. **Replication** (`protocol.rs` `DeleteV2`/`Tombstone`, `receiver.rs:1850`):
   master emits, receiver applies tombstone + record removal. Unit + idempotency
   tests.
7. **Recovery** (`recovery.rs:267`): rebuild redb tombstone index from log; R2
   self-purge pass. Unit tests for both.
8. **Migration reconciliation** (`dispatch.rs:1199-1352`, `coordinator.rs:5977`,
   `:689`): extend completion frame with tombstone section; implement the 4-row
   decision with the *union-of-pending-sources* drop rule (9.1.1); relax superset
   proof to non-tombstoned keys. **The hardest correctness surface.**

### 11.2 Modules changed

`src/tombstone.rs` (new), `src/index/redb_tombstone.rs` (new), `src/ops/engine.rs`,
`src/recovery.rs`, `src/replication/protocol.rs`, `src/replication/receiver.rs`,
`src/server/dispatch.rs`, `src/cluster/coordinator.rs`, config (new
`rejoin_grace_blocks`, tombstone-region size).

### 11.3 Test strategy

- **Locally validatable**: all unit tests (log, index, engine ordering, recovery
  rebuild + self-purge, replication idempotency, the 4-row decision table as a pure
  function over manifests). The reconciliation decision should be extracted as a
  **pure function** (`classify(local_keys, source_live, source_tombstones) →
  {keep, drop, transfer}`) so its correctness — including the union-of-sources rule —
  is exhaustively unit-testable without a cluster.
- **Requires CI (Docker statistical soak)**:
  - **sc09** (rolling restart / rejoin-during-churn) — the headline validation:
    over-count must converge to zero, with no data loss and no resurrection, across
    repeated runs.
  - **sc05 / sc07** (handoff/drain convergence) — the relinquish-with-tombstones path
    and superset relaxation.
  - A new **resurrection-specific** scenario: delete-while-down, rejoin, assert the
    deleted UTXO is *not* spendable (no double-spend) AND a concurrently never-received
    record *is* present on the master.
  - **GC-horizon soak**: run long enough that GC fires, with a laggard rejoining right
    at the horizon boundary, asserting it is full-resynced (not incrementally admitted
    with stale data).

### 11.4 Rough size/complexity

~1500–2500 LOC (≈600 new for the two storage modules, the rest wiring + tests).
Medium-high complexity, concentrated in step 8 (multi-source union) and step 4
(rejoin gate, on which GC safety rests).

### 11.5 Honest risk verdict

The mechanism (durable log + redb index + new replica op) is **low-risk, well-precedented** —
it clones the redo/redb pattern.

The two real risks:

1. **GC horizon soundness depends on the rejoin-eligibility gate (4.3) actually being
   enforced.** If step 4 is weak or bypassed, GC can drop a tombstone a laggard needs →
   silent resurrection. Mitigation: ship GC **disabled** initially (keep-everything,
   bounded only by operational outage length) and enable it only after the rejoin gate
   and the horizon soak (11.3) pass. The conservative #29 retain-path stays as the
   fallback behind the config flag.

2. **The multi-source union drop rule (9.1.1)** is the subtle part of reconciliation;
   a single-source evaluation would wrongly drop a key live on a concurrent source.
   Mitigation: defer all drops to the commit gate and decide against the union; cover
   exhaustively in the pure-function unit tests.

**Is it worth building vs accepting the benign residual?** Yes — but conditionally.
The benign residual (#38) is *not* truly benign long-term: it leaves shards
non-converging under rolling restart and parks unservable extras that a future refactor
could resurrect. Tombstones are the only one of the four candidate fixes that is
simultaneously no-loss, resurrection-proof, and convergent. Build it, but: (a) gate it
behind config, (b) ship GC off until the horizon soak passes, (c) keep the #29
retain-path as the fallback. That sequencing de-risks the one part (the GC horizon)
that can silently reintroduce double-spends.
