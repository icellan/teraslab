# TeraSlab Durability Contract

## Commit Model

TeraSlab uses a **durable-engine-first** commit model with a **mandatory redo journal** for metadata consistency recovery.

### Write Path Ordering

For every acknowledged mutation:

1. **Validate** — Parse request, check shard ownership, acquire per-txid lock.
2. **Apply to engine** — Write UTXO slot(s) and/or metadata to the block device via `pwrite`. On DirectDevice (production), these writes are durable on return because the device is opened with `O_DIRECT`, bypassing the OS page cache.
3. **Journal to redo log** — Append the mutation to the redo log and `fsync`. This step is **mandatory** — if the redo log write fails, the mutation is rolled back (where possible) or the request is failed with an internal error. The redo log captures the information needed to reconstruct derived metadata (e.g. `spent_utxos` counter) after a crash.
4. **Replicate** — Send the mutation to replica nodes synchronously. Replication failures are logged but do not fail the client — the replica will catch up from durable sequence state.
5. **Respond** — Send the success/error response to the client.

### What "Acknowledged" Means

A client success response guarantees:

- The mutation is durable on the local block device (engine writes via O_DIRECT).
- The mutation is recorded in the redo log and fsynced.
- The mutation was sent to all configured replicas (but replica failures do not block acknowledgment under the current ack policy).

### Crash Recovery

On restart, recovery replays all redo log entries after the last checkpoint:

- **Engine writes are already durable** — the UTXO slot and metadata bytes are on the device.
- **Redo replay fixes derived state** — counters (`spent_utxos`, `pruned_utxos`), secondary index entries (DAH, unmined), and any metadata fields that depend on read-modify-write sequences that may have been interrupted.
- **Replay is idempotent** — each entry checks whether the mutation was already applied before re-applying.

## Index Recovery on Startup

The in-memory index (primary hash table + DAH and unmined secondary indexes)
is a derived data structure — the block device is the source of truth.
On clean shutdown the index is snapshotted to disk as an optimization. On
startup, the system restores the index through three cascading layers:

### Layer 1: Block device is always correct

All UTXO slot and metadata writes go through `pwrite` on an `O_DIRECT`
file descriptor. By the time a client receives an acknowledgment, the
bytes are durable on the block device. After any crash, the on-disk UTXO
slots and metadata headers reflect the last completed operation. The
device is the ground truth — the index and redo log are derived from it.

### Layer 2: Redo log bridges snapshot-to-present gap

The redo log records every mutation between checkpoints. On recovery,
entries after the last checkpoint are replayed to bring derived metadata
(counters, secondary index entries, index registrations) up to date.
All replays are idempotent — each entry reads the current device state
before writing, skipping mutations that were already applied.

### Layer 3: Snapshot is a startup optimization

On clean shutdown, `Engine::snapshot_index()` writes the in-memory index
to a single file using atomic temp-file + rename. This avoids an O(N)
device scan on the next startup. The snapshot is never trusted as the
sole source of truth — redo log replay always runs afterwards.

### Startup Decision Tree

```
snapshot file exists?
 ├─ yes → restore from snapshot, verify CRC32 per section
 │         ├─ primary section corrupt?   → discard; full device scan
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
boundary tracking is considered unreliable and both secondaries are rebuilt.

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

Atomicity: data is serialized to a `.tmp` file, fsynced, then renamed to
the final path. If a crash occurs during snapshotting, the previous
snapshot (or no snapshot) remains — the new file is never partially visible.

### Device Scan Rebuild

When no valid snapshot exists, `Index::rebuild()` scans the device:

1. Walk every aligned block from `allocator.data_region_start()` to
   `allocator.next_offset()`.
2. Read the metadata header at each position; skip blocks with invalid
   magic or I/O errors.
3. For each valid record, register a `TxIndexEntry` in the hash table
   with the on-device offset and cached metadata fields.
4. Derived fields (`dah_or_preserve`, `unmined_since`, `generation`) are
   zeroed — they are recovered by redo log replay in the next step.

Secondary indexes are rebuilt by a separate `rebuild_secondary()` scan
that extracts `delete_at_height` and `unmined_since` from metadata headers.

### Crash Scenario Matrix

| Scenario | Recovery path |
|----------|---------------|
| Clean shutdown | Restore snapshot (fast) + replay trailing redo entries |
| Crash with recent snapshot | Restore snapshot + redo replay brings index current |
| Crash during snapshotting | Old snapshot survives (atomic rename); redo replay covers gap |
| Crash with no snapshot | Full device scan (slow at 50M+ records) + redo replay |
| Crash with corrupted snapshot | Full device scan + redo replay |
| Crash during redo replay | Replay is idempotent; restarts from last checkpoint on next boot |

### Sequence Numbering

Each redo log entry receives a monotonically increasing sequence number assigned by `RedoLog::append()`. This sequence:

- Orders mutations within the redo log for recovery replay.
- Provides the durable commit point — after `flush()` returns, all entries up to the assigned sequence are on persistent storage.
- Is used by replica catch-up (`read_from_sequence`) to identify missed mutations.

### Failure Modes

| Failure Point | Outcome |
|---------------|---------|
| Crash before engine write | No mutation occurred. No redo entry. Clean. |
| Crash after engine write, before redo flush | Engine state is durable but redo log may not have the entry. On recovery, the engine state is correct (writes were via O_DIRECT) but derived metadata may be stale. The redo journal is mandatory to prevent this gap. |
| Crash after redo flush, before replication | Local state is fully consistent. Replica is behind but will catch up. |
| Crash after replication, before response | Client sees timeout/disconnect. Mutation is durable everywhere. Client should retry (operations are idempotent). |
| Redo log full | Mutation fails with internal error. Client retries later. |

### Design Decisions

1. **Engine-first, not WAL-first**: TeraSlab uses O_DIRECT for all device I/O, making engine writes immediately durable without fsync. A traditional WAL-first approach would double the write amplification (write to WAL, then to engine). Instead, the redo log serves as a metadata consistency journal.

2. **Mandatory redo**: Redo log failures fail the client request. This ensures that every acknowledged mutation has a corresponding redo entry for crash recovery.

3. **Redo log is not the source of truth for data**: The engine (block device) is the authoritative store for UTXO slots and metadata. The redo log is authoritative only for the *ordering* of mutations and for recovering derived metadata after a crash.

4. **Replication is best-effort for ack**: Under the current ack policy, a failed replication does not block the client. The ack policy can be tightened to require replica quorum before acknowledgment.

## Replication Sequence Model

Every replicated batch carries a durable sequence number assigned from the
global `ReplicationState`. The sequence is initialized from the redo log's
`current_sequence()` on startup, ensuring contiguity between the local
commit log and replication positions.

### Per-Replica State

The master tracks per-replica state:

| Field | Meaning |
|-------|---------|
| `last_acked` | Highest sequence the replica has acknowledged |
| Connection | Persistent TCP transport, pooled and reused |

When a replica fails, its `last_acked` position identifies exactly which
mutations it missed. Catch-up reads redo log entries from `last_acked + 1`
forward and replays them to the reconnected replica.

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

### Startup Recovery

On server restart (see also "Index Recovery on Startup" above):
1. Index is restored from snapshot or rebuilt from device scan.
2. Redo log is opened and recovery replays entries after the last checkpoint.
3. `init_replication_sequence(redo_log.current_sequence())` sets the
   replication counter so new batches continue from the correct position.
4. Replica connections are re-established; catch-up runs automatically.

## Topology Epochs and Ownership Fencing

### Monotonic Epoch Counter

Every membership change increments a monotonic `topology_epoch` counter.
The shard table carries this epoch as its `version` field. This replaces
the previous hash-based version which could collide.

**Guarantees:**
- Every shard table has a strictly increasing epoch
- Stale ownership views (from partitioned/restarted nodes) are detectable
  by comparing their epoch against the current cluster epoch
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
