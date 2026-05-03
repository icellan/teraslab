# TeraSlab Durability Contract

## Commit Model

TeraSlab uses a **WAL-first** commit model with a **mandatory redo log**.

> **History note:** an earlier draft of this document (and a comment block in
> `src/server/dispatch.rs`) described an "engine-first" model where O_DIRECT
> engine writes were the durability point and the redo log was a metadata
> consistency journal that ran *after* the engine write. That ordering does
> not match the implemented code and is unsafe under crashes that hit
> mid-engine-write: the engine can have a partial / torn write on the device
> while the redo log has no record of the operation, so recovery cannot
> reconstruct the intended post-state. Gap #2 in
> `docs/TERANODE_PRODUCTION_READINESS_GAPS.md` flagged this drift.
>
> The current document describes the actual implementation. Operators
> integrating with TeraSlab MUST treat WAL-first ordering as part of the
> release contract.

### Write Path Ordering

For every acknowledged mutation:

1. **Validate under lock** — parse the request, check shard ownership,
   acquire the per-transaction stripe lock. Multi-spend additionally
   snapshots the metadata block and the slot-by-slot validation result
   under the same lock so the redo entry can be derived without re-reading
   the device.
2. **Pre-allocate** (creates only) — reserve device space via the
   allocator. The allocator is itself WAL-journalled
   (`RedoOp::AllocateRegion`), so allocations survive crashes.
3. **Append + fsync the redo entry** — `RedoLog::append` + `RedoLog::flush`
   together produce a durable WAL record carrying every byte recovery needs
   to reconstruct the post-mutation state. Concretely:
   * `RedoOp::CreateV2` carries the full record bytes (metadata header +
     UTXO slots + cold data) plus the `is_conflicting` flag and
     `parent_txids` list.
   * `RedoOp::Spend` / `RedoOp::Unspend` carry the post-mutation
     `new_spent_count` computed from the metadata snapshot taken in step 1.
   * Other ops carry the per-key payload necessary to re-apply the
     metadata mutation.
   This step is **mandatory**: if the redo log open / create fails at
   startup the binary refuses to serve (no in-memory fallback). If the
   redo flush fails mid-request, the client request fails with an
   internal error and no engine mutation runs.
4. **Apply to the engine** — write UTXO slots and/or metadata to the
   block device via `pwrite_all_at`. On `DirectDevice` (production), the
   write is durable on return because the device is opened with
   `O_DIRECT`, bypassing the OS page cache. The internal `pwrite_all_at`
   loop treats short writes as fatal corruption (gap #4) so a partial
   apply cannot silently land between the WAL fsync and the engine write.
5. **Replicate** — fan out the mutation to replicas with the durable
   sequence numbers assigned in step 3. The current ack policy is
   best-effort: replication failures may degrade durability for the
   client response but do not roll back local state. RF>1 deployments
   reject `replication_degraded_mode = "best_effort"` at config load
   time.
6. **Respond** — send the success / error response to the client.

### What "Acknowledged" Means

A client success response guarantees:

- The mutation is recorded in the redo log and fsynced to disk.
- The mutation is durable on the local block device (engine writes via
  `O_DIRECT`).
- The mutation was sent to all configured replicas. Replica failures may
  surface as a degraded-durability status byte but do not roll back the
  local commit.

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
| Crash before redo fsync | No durable record. The mutation never happened from the perspective of every observer (client, replica, recovery). |
| Crash after redo fsync, before engine write | Recovery replays the entry. `CreateV2` reconstructs the record, spend/unspend write the correct counter, the slot transition is idempotently re-applied. |
| Crash after engine write, before replication | Local state is fully consistent. The replica is behind by the unsent batch and catches up via `RedoLog::read_from_sequence` on reconnect. |
| Crash after replication ACK, before intent clear | The persistent `ReplicationIntentTracker` carries the pending range across restart. The next startup `commit`s the range idempotently after reconciling with replicas. |
| Crash after intent clear, before client response | Client sees timeout / disconnect. The mutation is durable everywhere; client retry is idempotent because all redo entries are idempotent. |
| Redo log full | `RedoLog::append` returns `LogFull`, the dispatcher fails the client request with internal error, no engine mutation runs. The operator must enable / accelerate checkpoints (gap #3 — finite redo log is a separate readiness issue). |
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

2. **Mandatory redo.** Redo log open / create failure is fatal at startup
   (gap #2 part 1). Redo flush failure mid-request fails the client. There
   is no in-memory fallback because the resulting "ack" would be a lie —
   bytes in volatile memory disappear at shutdown.

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
[member_ids:8*N LE]     (N = member_count)
[incarnation:8 LE]
```

Persisted on every membership change. On restart:
- Peak cluster size restores the quorum requirement
- Committed term restores the topology ordering baseline so new terms
  are strictly higher than any the node has seen
- Voted term prevents double-voting in the same term after restart
- Member list restores the last committed membership view
- Incarnation counter ensures SWIM refutation numbers are monotonic
  across restarts (loaded value + 1)

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
