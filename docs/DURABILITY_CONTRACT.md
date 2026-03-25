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

On server restart:
1. Redo log is opened and recovery replays entries after the last checkpoint.
2. `init_replication_sequence(redo_log.current_sequence())` sets the
   replication counter so new batches continue from the correct position.
3. Replica connections are re-established; catch-up runs automatically.

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
