# TeraSlab Durability Contract

## Commit Model

TeraSlab uses a **WAL-first** commit model with a **mandatory redo log**. The
redo log is the authoritative source of truth for the post-checkpoint window;
the store is always internally consistent — an acknowledged mutation either
appears in the redo log (and will survive recovery) or does not. What varies
between deployment modes is *when* that redo entry reaches durable storage.

### Default: Buffered Redo Durability

**The shipped default is `redo_buffered = true`** (`src/config.rs`). In this
mode, a mutation is acknowledged after its redo entry is appended to the
in-memory redo buffer. A background flusher calls `fsync` on the redo log
every `redo_flush_interval_ms` (default **5 ms**). This means:

- On a **single-node (`replication_factor = 1`) unclean shutdown**,
  acknowledged mutations written in the last flush interval (up to 5 ms by
  default) may be lost. The store will never expose a partially-applied
  mutation — the redo log is the source of truth, and a lost tail entry's
  mutation simply vanishes atomically. There is no corruption: recovered
  state is consistent but may lag acknowledged state by at most one flush
  window. **This window does not apply when `replication_factor > 1`** — see
  "RF>1: Concurrent Local fsync-Before-Ack (C1)" below.
- The store **remains internally consistent** after any crash. The B2 fix
  ensures lost tail entries never cause silent freelist reuse — the mutation
  is absent, not corrupted.
- Operators who need to reduce the RF=1 loss window can lower
  `redo_flush_interval_ms` at the cost of more frequent fsyncs.

This is the appropriate default for BSV Teranode deployments. For `RF=1`
deployments, the flush-interval loss window above applies as described —
there is no replica to fall back on. For `replication_factor > 1`
deployments, the loss window does not apply at all: the master's local redo
tail and data devices are forced fsync-durable BEFORE the client ack (C1,
below), and the replica separately holds a copy of every acked mutation — so
an RF>1 crash within the flush window loses nothing, locally or from the
cluster's perspective.

### RF>1: Concurrent Local fsync-Before-Ack (C1)

Under buffered durability (the default) with `replication_factor > 1` and
active replication, an acked write does not rely on the replica alone for
durability. `ensure_local_write_durable` (`src/server/dispatch.rs`) forces
the master's own redo tail (`Engine::flush_all_redo`) and every touched data
device (`Engine::sync_all_store_devices`) durable BEFORE the client ack is
classified. This runs on its own thread CONCURRENTLY with the synchronous
replica round-trip, so the local `fsync` hides behind the network RTT instead
of adding to request latency. The gate matches the master-side
replication-intent gate (`replication_active`) exactly, so every write this
mechanism protects is one whose replication intent was already durably
recorded.

The result: on an RF>1 node, `STATUS_OK` means the write is BOTH locally
fsync-durable AND replicated to a quorum — the RF=1 flush-interval loss
window described above does not exist for RF>1. See the
`rf_gt_1_mutation_is_locally_fsync_durable_before_ack` test
(`src/server/dispatch.rs`) for the behavior under test. Strict durability
(`redo_buffered = false`) and single-node (RF = 1, no migration) skip this
gate — they are already fsync-durable per commit, or have no replica RTT to
hide the local `fsync` behind.

**Reverse-heal, a separate mechanism:** `reverse_heal.tombstones` defaults ON
for `replication_factor > 1` (`ReverseHealConfig::tombstones_enabled`,
`src/config.rs`). It recovers a stale-suspect shard's missing/behind records
from a quorum-current replica via a master↔replica reverse-pull, both at boot
and via the Phase 3b runtime online re-heal. This is additional to, not a
substitute for, C1 above: C1 removes the RF>1 acked-write loss window at ack
time; reverse-heal is the recovery path for a node whose local state falls
behind for other reasons (e.g. it was offline).

### Strict Mode: fsync-Before-Ack

Set `redo_buffered = false` to enable **strict durability**: the redo entry is
fsynced to disk before the mutation is applied to the engine and before the
client receives a success response. An acknowledged mutation is durable on the
local device at the moment the ack is sent.

**Tradeoff:** strict mode disables the log-structured `segment` engine (which
requires buffered durability — see `src/config.rs` for the validation check).
You must also set `storage.engine = "in_place"` for strict mode. Throughput
drops significantly because every mutation blocks on an `fsync`.

```toml
redo_buffered = false

[storage]
engine = "in_place"
```

> **History note:** an earlier draft of this document described an
> "engine-first" model where O_DIRECT engine writes were the durability point
> and the redo log was a metadata consistency journal that ran *after* the
> engine write. That ordering does not match the implemented code and is unsafe
> under crashes that hit mid-engine-write. This document is the authoritative
> description. Operators integrating with TeraSlab MUST treat the WAL-first
> ordering (redo append before engine write) as part of the release contract.
> The durability *window* for the redo append (buffered vs. strict) is a
> configuration choice described above.

### Write Path Ordering

The WAL-first ordering is invariant across both modes — what differs is only
whether the redo append is fsynced before or after the ack:

1. **Validate under lock** — parse the request, check shard ownership,
   acquire the per-transaction stripe lock. Multi-spend additionally
   snapshots the metadata block and the slot-by-slot validation result
   under the same lock so the redo entry can be derived without re-reading
   the device.
2. **Pre-allocate** (creates only) — reserve device space via the
   allocator. The allocator is itself WAL-journalled
   (`RedoOp::AllocateRegion`), so allocations survive crashes.
3. **Append the redo entry** — `RedoLog::append` appends the redo record to
   the in-memory buffer. In strict mode (`redo_buffered = false`),
   `RedoLog::flush` is called immediately and blocks until the entry is on
   durable storage. In buffered mode (default), flush is deferred to the
   background flusher. Concretely:
   * `RedoOp::CreateV2` carries the full record bytes (metadata header +
     UTXO slots + cold data) plus the `is_conflicting` flag and
     `parent_txids` list.
   * `RedoOp::Spend` / `RedoOp::Unspend` carry the post-mutation
     `new_spent_count` computed from the metadata snapshot taken in step 1.
   * Other ops carry the per-key payload necessary to re-apply the
     metadata mutation.
   This step is **mandatory**: if the redo log open / create fails at
   startup the binary refuses to serve (no in-memory fallback). If the
   redo flush fails mid-request (strict mode), the client request fails with
   an internal error and no engine mutation runs.
4. **Apply to the engine** — write UTXO slots and/or metadata to the
   block device via `pwrite_all_at`. On `DirectDevice` (production), the
   write is durable on return because the device is opened with
   `O_DIRECT`, bypassing the OS page cache. The internal `pwrite_all_at`
   loop treats short writes as fatal corruption so a partial apply cannot
   silently land.
5. **Replicate** — fan out the mutation to replicas with the durable
   sequence numbers assigned in step 3. The current ack policy is
   best-effort: replication failures may degrade durability for the
   client response but do not roll back local state. RF>1 deployments
   reject `replication_degraded_mode = "best_effort"` at config load
   time.
6. **Respond** — send the success / error response to the client.

### What "Acknowledged" Means

A client success response guarantees:

- The mutation is recorded in the redo log. In **buffered mode** (default)
  with **`replication_factor = 1`**, the redo entry will reach durable
  storage within one flush interval (default ≤ 5 ms). In **buffered mode
  with `replication_factor > 1`**, the redo entry (and every touched data
  device) is already fsync-durable by ack time (C1) — there is no
  flush-interval window. In **strict mode** (`redo_buffered = false`), the
  entry is fsynced before the ack is sent regardless of RF.
- The mutation is applied to the engine (O_DIRECT write, durable if the redo
  entry is durable).
- The mutation was sent to all configured replicas. Replica failures may
  surface as a degraded-durability status byte but do not roll back the
  local commit.

In buffered mode with `replication_factor = 1`, an acknowledged mutation may
be lost on unclean shutdown if the crash occurs within the last flush window.
The store remains consistent — the lost mutation is absent, not corrupted.
In buffered mode with `replication_factor > 1`, this window does not apply
(C1, above); reverse-heal is additionally default-on for RF>1 as a separate
recovery path for a stale-suspect shard.

### Crash Recovery

On restart, recovery replays every redo entry after the last checkpoint:

- `RedoOp::CreateV2` reconstructs the on-device record byte-for-byte
  from the captured `record_bytes`, then registers the index entry with
  cached fields (`tx_flags`, `spent_utxos`, `dah_or_preserve`,
  `unmined_since`, `generation`, `block_entry_count`) populated from
  the reconstructed metadata header. A short read or write of the
  record area surfaces as `ReplayCause::MissingRecordBytes` and is
  fatal — the device is misbehaving.
- `RedoOp::Spend` / `RedoOp::Unspend` overwrite `meta.spent_utxos` with
  the dispatcher-computed post-state count and re-apply the slot
  transition idempotently.
- Other entries (`SetMined`, `Freeze`, `Reassign`, `PruneSlot`, etc.)
  re-apply their metadata mutation idempotently against whatever state
  the device currently shows.
- Allocator entries (`AllocateRegion`, `FreeRegion`) replay into the
  rebuilt allocator's freelist + high-water mark.

Every replay is idempotent: each entry checks the current on-device or
in-index state before writing and skips when the post-state already
matches. Replay can therefore run multiple times without divergence
(e.g. crash mid-replay).

### Failure Modes

| Failure point | Outcome |
|---------------|---------|
| **Buffered mode, RF=1** — crash before background fsync (within last flush window) | The redo entry is in the OS page cache but not yet on disk. The mutation is lost; the store recovers to consistent pre-mutation state. No corruption. |
| **Buffered mode, RF>1** — crash before background fsync | Not reachable for an ACKED write: C1 forces the local redo tail + data devices durable before the ack (concurrently with the replica round-trip), so an acked mutation's redo entry is always on disk by ack time. An in-flight write the client never got a response for can still be lost the same way as RF=1. |
| **Strict mode** — crash before redo fsync | No durable record. The mutation never happened from the perspective of every observer (client, replica, recovery). |
| Crash after redo fsync, before engine write | Recovery replays the entry. `CreateV2` reconstructs the record, spend/unspend write the correct counter, the slot transition is idempotently re-applied. |
| Crash after engine write, before replication | Local state is fully consistent. The replica is behind by the unsent batch and catches up via `RedoLog::read_from_sequence` on reconnect. |
| Crash after replication ACK, before intent clear | The persistent `ReplicationIntentTracker` carries the pending range across restart. The next startup `commit`s the range idempotently after reconciling with replicas. |
| Crash after intent clear, before client response | Client sees timeout / disconnect. The mutation is durable everywhere; client retry is idempotent because all redo entries are idempotent. |
| Redo log full | `RedoLog::append` returns `LogFull`, the dispatcher fails the client request with internal error, no engine mutation runs. The operator must enable / accelerate checkpoints. |
| Redo log open / create failure at startup | Fatal — startup exits with an operator-facing error message naming the path and underlying device error. There is **no** in-memory fallback in production code paths. |

### Design Decisions

1. **WAL-first, not engine-first.** The redo log is the durable source of
   truth for the post-checkpoint window. Engine writes are durable on
   return only for fully-completed `pwrite` calls; recovery cannot rely on
   the engine alone because a crash mid-pwrite leaves torn bytes that no
   amount of metadata replay can repair without the redo entry's payload
   (full `record_bytes` for creates, `new_spent_count` for spends/unspends,
   etc.). WAL-first ordering puts the durable record on disk before the
   torn-bytes window opens.

2. **Mandatory redo.** Redo log open / create failure is fatal at startup.
   In strict mode (`redo_buffered = false`), a redo flush failure mid-request
   fails the client request before any engine mutation runs. In buffered mode
   (default), the client request completes after the in-memory append; a
   background flush error is surfaced via server health metrics. There is no
   silent "skip redo" path — the redo log is always written before the engine.

3. **Full-payload redo entries.** Gap #2 parts 2 / 4 introduced
   `RedoOp::CreateV2` which captures the full record bytes plus the
   `is_conflicting` flag and `parent_txids`. This eliminates the previous
   recovery window where `RedoOp::Create` (legacy) registered the index
   without reconstructing the record, leaving an index entry pointing at
   missing or partial bytes. The legacy entry tag is retained for
   back-compat: redo logs written before this change still replay
   (registering the index without reconstructing record bytes — the same
   behaviour they had).

4. **Replication is best-effort for ack.** Under the current ack policy,
   a failed replication does not block the client. Operators tightening
   the ack policy must still keep WAL-first ordering: the redo entry is
   the local durability point, the replica fan-out is the cluster
   durability point, and the two are decoupled.

5. **Recovery fail-closed by cause class.** Replay failures are classified
   into `MissingPrimary` (benign — record was deleted later in the log),
   `IoError`, `CorruptEntry`, `LogicError`, and `MissingRecordBytes`.
   Only `MissingPrimary` is tolerated at startup, and only up to a high
   cap. Every other class fails closed regardless of count.

## Deletes and GC Are Outside the Durability Contract

Everything above describes MUTATIONS. Record removal is deliberately not one:
it is garbage collection of a record the retention policy has already released,
and it is exempt from the WAL-first, replicate-then-ack discipline. Full
behavioural contract: spec §3.18 / §3.18.1. The durability-relevant parts:

- **No redo entry.** A delete writes nothing to the WAL. A crash before the
  physical cleanup leaves the record present and internally consistent (record
  + parent-spent slots + allocated region all still agree); the pruner
  re-deletes it on its next pass. "Lost" delete = "delete has not happened yet",
  which is a legal state, not corruption.
- **No replication.** A master's delete is not shipped to its replicas. Each
  node reclaims its own copy.
- **Never acknowledged as durable.** `STATUS_OK` on `OP_DELETE_BATCH` /
  `OP_PROCESS_EXPIRED_PRESERVATIONS` means "removed from this node's live
  index", not "durably removed everywhere". There is no cluster-wide
  delete barrier.

### Who reclaims what (RF > 1)

A record physically lives on its shard master AND on every replica of that
shard. Mastership decides who is *authoritative*, not who pays to store it —
under RF = 2 roughly half of each node's device is replica copies. The DAH
sweep is the only driver that ever revisits a stored record, so it reclaims in
two distinct roles:

| Role | Applies to | Writes a tombstone | Prunes parent slots |
|---|---|---|---|
| **Master** (client delete, and the sweep over mastered keys) | keys this node masters | yes (when `reverse_heal.tombstones` is on) | yes |
| **Held copy** (the sweep over replica copies) | keys this node stores but does not master | **no** | **no** |

The held-copy role re-validates the identical due predicate under the identical
per-tx stripe lock; it is narrower only in those two columns, and it touches
exactly one record — its own.

### Why the held-copy reclaim writes no tombstone

A deletion tombstone is the authority's veto over any future copy of the key:
RULE-DS (`Engine::tombstone_blocks_heal_apply`) drops an incoming
migration-baseline record when a tombstone at-or-ahead of its generation
exists. That is correct for a shard master, which is the authority on whether
the key still exists.

A replica is not that authority. If it drops a copy it should have kept — the
`PreserveUntil` race, spec §3.18.1 — the master still holds the record and
MUST be able to put it back: via the resync a replica's NAK-on-missing
triggers, via a migration baseline, or via a reverse-heal pull. A
replica-written tombstone would veto exactly that repair and turn a transient,
self-healing divergence into permanent loss. **The absence of a tombstone on
the held-copy path is load-bearing, not an omission.**

Consequence for reverse-heal: a healing node can no longer assume its replicas
still hold every record it has since released. That is intended — both sides
apply the same retention predicate, so a record missing from both was garbage
by policy on both.

## Index Recovery on Startup

The in-memory index (primary hash table + DAH and unmined secondary
indexes) is a derived data structure — the block device is the on-disk
representation of every UTXO slot, and the redo log is the source of
truth for the post-checkpoint window. On clean shutdown the index is
snapshotted to disk as an optimization. On startup, the system restores
the index through three cascading layers:

### Layer 1: Block device records the steady state

All UTXO slot and metadata writes go through `pwrite_all_at` on an
`O_DIRECT` file descriptor. For mutations whose redo entry is durable
and whose engine write completed, the device bytes reflect the
post-mutation state. For mutations whose redo entry is durable but
whose engine write didn't complete, the redo replay below restores the
post-state.

### Layer 2: Redo log replays the post-checkpoint window

Every mutation since the last checkpoint is in the redo log. Recovery
replays every entry idempotently, reconstructing records (CreateV2),
fixing up counters (Spend/Unspend), re-applying metadata mutations, and
reconciling secondary-index intent records.

### Layer 3: Snapshot is a startup optimization

On clean shutdown, `Engine::snapshot_index()` writes the in-memory
index to a single file using atomic temp-file + rename. This avoids an
O(N) device scan on the next startup. The snapshot is never trusted
as the sole source of truth — redo log replay always runs afterwards.

### Startup Decision Tree

```
snapshot file exists?
 ├─ yes → restore from snapshot, verify CRC32 per section
 │         ├─ primary section corrupt?   → fail closed (gap #5);
 │         │                               file preserved untouched
 │         ├─ DAH section corrupt?       → rebuild DAH from device scan
 │         │                               (also marks unmined for rebuild)
 │         └─ unmined section corrupt?   → rebuild unmined from device scan
 └─ no  → full device scan
            (walk every aligned block, read metadata headers, register in index)

then ALWAYS → replay redo log entries after last checkpoint
              (idempotent; safe even if entries were already applied)
```

The `RestoreFlags` struct tracks which secondary indexes need rebuilding
after a partial snapshot restore. If the DAH section is corrupt, file
boundary tracking is considered unreliable and both secondaries are
rebuilt.

### Index Snapshot Format

Written by `Index::snapshot_all()`, read by `Index::restore_all()`:

```
Primary section:
  [magic "TSIX" (4)] [version (4)] [entry_count (8)] [capacity (8)]
  [TxKey(32) + TxIndexEntry(31)] * entry_count
  [CRC32 (4)]

DAH section:
  [magic "DAHI" (4)] [version (4)] [count (8)]
  [height(4) + txid(32)] * count
  [CRC32 (4)]

Unmined section:
  [magic "UNMI" (4)] [version (4)] [count (8)]
  [unmined_since(4) + txid(32)] * count
  [CRC32 (4)]
```

Atomicity: data is serialized to a `.tmp` file, fsynced, then renamed
to the final path. If a crash occurs during snapshotting, the previous
snapshot (or no snapshot) remains — the new file is never partially
visible.

### Device Scan Rebuild

When no valid snapshot exists, `Index::rebuild()` scans the device:

1. Walk every aligned block from `allocator.data_region_start()` to
   `allocator.next_offset()`.
2. Read the metadata header at each position; skip blocks with invalid
   magic or I/O errors.
3. For each valid record, register a `TxIndexEntry` in the hash table
   with the on-device offset and cached metadata fields.
4. Derived fields (`dah_or_preserve`, `unmined_since`, `generation`)
   are zeroed — they are recovered by redo log replay in the next step.

Secondary indexes are rebuilt by a separate `rebuild_secondary()` scan
that extracts `delete_at_height` and `unmined_since` from metadata
headers.

### Crash Scenario Matrix

| Scenario | Recovery path |
|----------|---------------|
| Clean shutdown | Restore snapshot (fast) + replay trailing redo entries |
| Crash with recent snapshot | Restore snapshot + redo replay reconstructs anything created or mutated since |
| Crash during snapshotting | Old snapshot survives (atomic rename); redo replay covers gap |
| Crash with no snapshot | Full device scan (slow at 50M+ records) + redo replay |
| Crash with corrupted snapshot primary | Fail closed (gap #5); operator must investigate |
| Crash during redo replay | Replay is idempotent; restarts from last checkpoint on next boot |

### Sequence Numbering

Each redo log entry receives a monotonically increasing sequence number
assigned by `RedoLog::append()`. This sequence:

- Orders mutations within the redo log for recovery replay.
- Provides the durable commit point — after `flush()` returns, all
  entries up to the assigned sequence are on persistent storage.
- Is used by replica catch-up (`read_from_sequence`) to identify missed
  mutations.

## Replication Sequence Model

Every replicated batch carries a durable sequence number assigned from
the global `ReplicationState`. The sequence is initialized from the
redo log's `current_sequence()` on startup, ensuring contiguity between
the local commit log and replication positions.

### Per-Replica State

The master tracks per-replica state:

| Field | Meaning |
|-------|---------|
| `last_acked` | Highest sequence the replica has acknowledged |
| Connection | Persistent TCP transport, pooled and reused |

When a replica fails, its `last_acked` position identifies exactly
which mutations it missed. Catch-up reads redo log entries from
`last_acked + 1` forward and replays them to the reconnected replica.

### Sequence Lifecycle

1. **Assign**: `REPL_STATE.next_sequence` is read and advanced atomically
   when building a replication batch.
2. **Send**: The batch carries `first_sequence` so the replica knows
   its position in the mutation stream.
3. **ACK**: Replica responds with `through_sequence` — the highest
   sequence it durably applied.
4. **Track**: Master records `last_acked[addr] = through_sequence`.
5. **Catch-up**: On reconnect, missed entries are replayed from
   `redo_log.read_from_sequence(last_acked + 1)`.

### Replication Intent Recovery

The dispatch path persists each pending replication range to a
`ReplicationIntentTracker` BEFORE fanning out to replicas, and `commit`s
the range only after the ACK policy is satisfied. A crash AFTER
replication ACKs but BEFORE the commit leaves the range in the
on-disk file; the next startup reconciles it (replay from redo or
re-confirm with replicas) and clears the intent idempotently. See
`tests/recovery_crash_boundaries.rs` for a worked example.

### Startup Recovery

On server restart (see also "Index Recovery on Startup" above):
1. Index is restored from snapshot or rebuilt from device scan.
2. Redo log is opened (mandatory — fail-closed on open failure) and
   recovery replays entries after the last checkpoint.
3. `init_replication_sequence(redo_log.current_sequence())` sets the
   replication counter so new batches continue from the correct
   position.
4. Pending replication intent ranges are reconciled and cleared.
5. Replica connections are re-established; catch-up runs automatically.

### Restore is Crash Recovery

The online backup (`docs/ONLINE_BACKUP_DESIGN.md`) relies on exactly this
contract. A backup is constructed to be a **crash-legal device image at an
instant `T`** — each 4 KiB block captured atomically under the same striped
block lock a writer holds across its RMW — **plus the complete redo tail
`(F, T]`** teed live at commit time, written into a fabricated linear-v2 redo
file with `checkpoint_seq = F`. Restore lays those artifacts down at their
configured paths and boots the node normally: step 2 above replays the tail on
top of the image with no special casing. Because replay is idempotent and
self-contained for the in-place ops (spend/setMined/freeze/…​ recompute absolute
state) and payload-carrying for creates, the restored state equals the source
state at `T`. There is **no backup-specific recovery code** — restore ≡ this
startup recovery path, which is why the WAL-first ordering and idempotent replay
guaranteed here are load-bearing for backup correctness.

## Topology Epochs and Ownership Fencing

### Monotonic Epoch Counter

Every membership change increments a monotonic `topology_epoch` counter.
The shard table carries this epoch as its `version` field. This replaces
the previous hash-based version which could collide.

**Guarantees:**
- Every shard table has a strictly increasing epoch
- Stale ownership views (from partitioned/restarted nodes) are
  detectable by comparing their epoch against the current cluster epoch
- The epoch is persisted alongside the peak cluster size so a restarted
  node resumes from its last-known epoch

### Persisted Cluster State

File format:
```
[peak_cluster_size:8 LE]
[committed_term:8 LE]
[voted_term:8 LE]
[member_count:4 LE]
[member_ids:8*N LE]       (N = member_count)
[incarnation:8 LE]
[voter_count:4 LE]
[voter_ids:8*N LE]        (N = voter_count)
[ever_seen_count:4 LE]
[ever_seen_ids:8*N LE]    (N = ever_seen_count)
```

Persisted on every membership change. On restart:
- Peak cluster size restores the quorum requirement
- Committed term restores the topology ordering baseline so new terms
  are strictly higher than any the node has seen
- Voted term prevents double-voting in the same term after restart
- Member list restores the last committed membership view
- Incarnation counter ensures SWIM refutation numbers are monotonic
  across restarts (loaded value + 1)
- Voter list restores the set of nodes whose quorum approved the last
  committed term
- Ever-seen list restores every node ever observed as a committed voter,
  used as the fallback split-brain heal defence (F-G8-001) when
  `cluster_id` is unset. Older payloads without the
  `[voter_count]`/`[ever_seen_count]` trailers decode with empty lists
  and the loader seeds the ever-seen set from the committed voters

### Ownership Safety Properties

1. **At most one primary view**: The epoch counter ensures that if two
   nodes both believe they own a shard, the one with the higher epoch
   wins. The other must re-enter through migration/catch-up.

2. **No stale ownership after restart**: A restarted node loads its
   persisted epoch and quorum requirement. It cannot accept writes
   until it re-joins the cluster and receives the current topology
   via SWIM membership events.

3. **Migration fencing**: During shard migration, the source node's
   writes are fenced (blocked) for migrating shards. The fence is
   lifted only when migration completes or fails, preventing split
   writes between old and new owners.
