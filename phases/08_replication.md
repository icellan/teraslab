# Phase 8: Replication

## Goal

Implement operation-based synchronous replication. The master logs operations, ships them to replicas, and waits for ACK before responding to the client. Replicas apply operations in order using the same idempotent mutation functions.

## Dependencies

Phases 1-7 must be complete with all tests passing.

## Reference

- `specs/BSV_UTXO_STORE_SPEC.md` §8 (Replication) — configurable RF (2-3+), PruneSlot variant, batch framing, acknowledgment policies
- The redo log from Phase 7 doubles as the replication stream.

## What to build

### 8.1 ReplicaOp wire format — `src/replication/protocol.rs`

Define the on-wire representation for operation-based replication. This is a serialization of the RedoOp, but optimized for network transmission. Matches the spec's `ReplicaOp` enum exactly, including `PruneSlot`.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicaOp {
    Spend { tx_key: TxKey, offset: u32, spending_data: [u8; 36] },
    Unspend { tx_key: TxKey, offset: u32 },
    SetMined { tx_key: TxKey, block_id: u32, block_height: u32, subtree_idx: u32, on_longest_chain: bool },
    UnsetMined { tx_key: TxKey, block_id: u32 },
    Freeze { tx_key: TxKey, offset: u32 },
    Unfreeze { tx_key: TxKey, offset: u32 },
    Reassign { tx_key: TxKey, offset: u32, new_hash: [u8; 32], block_height: u32, spendable_after: u32 },
    SetConflicting { tx_key: TxKey, value: bool, current_block_height: u32, retention: u32 },
    SetLocked { tx_key: TxKey, value: bool },
    PreserveUntil { tx_key: TxKey, block_height: u32 },
    Create { tx_key: TxKey, metadata: Vec<u8>, utxo_hashes: Vec<[u8; 32]>, cold_data: Option<Vec<u8>> },
    Delete { tx_key: TxKey },
    PruneSlot { tx_key: TxKey, offset: u32 },
}
```

**`PruneSlot`** replicates the action of zeroing out a single UTXO slot on the master. This is distinct from `Delete` (which removes the entire transaction record). The pruner on the master emits `PruneSlot` ops so that replicas stay in sync without running their own pruner timers.

#### Wire messages

Individual operations are not sent one-per-frame. Instead they are batched (see §8.5). The wire types are:

```rust
/// A single operation tagged with a sequence number.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaMessage {
    pub sequence: u64,
    pub op: ReplicaOp,
}

/// A batch of operations sent as a single frame (OP_REPLICA_BATCH).
/// All ops in a batch share a contiguous sequence range:
/// first op has sequence `first_sequence`, last has `first_sequence + ops.len() - 1`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaBatch {
    pub first_sequence: u64,
    pub ops: Vec<ReplicaOp>,
}

/// Acknowledgment from a replica.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicaAck {
    /// All ops up to and including `through_sequence` have been applied.
    Ok { through_sequence: u64 },
    /// An error occurred applying the op at `failed_sequence`.
    Error { failed_sequence: u64, message: String },
}

/// Sent by a replica when it reconnects to request catchup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatchupRequest {
    pub last_ack_sequence: u64,
}
```

Use a compact binary serialization (e.g., `bincode` or a custom format). Measure serialized sizes — a Spend should be under 80 bytes on the wire. A batch header adds 8 bytes (first_sequence) + 4 bytes (op count) = 12 bytes of overhead.

### 8.2 Replication sender (single replica connection) — `src/replication/sender.rs`

A `ReplicationSender` manages the TCP connection and framing for one replica. It does not decide policy — that is the `ReplicationManager`'s job.

```rust
pub struct ReplicationSender {
    replica_addr: SocketAddr,
    connection: TcpStream,       // tokio TcpStream
    pending_acks: VecDeque<u64>, // sequences awaiting ACK
    next_sequence: u64,
    state: CatchupState,
}

/// Tracks whether this replica is catching up or fully live.
#[derive(Debug, Clone, PartialEq)]
pub enum CatchupState {
    /// Replica is behind. Master is streaming redo log entries
    /// starting from `from_sequence` forward.
    CatchingUp { from_sequence: u64 },
    /// Replica is fully caught up and receiving live ops.
    Live,
}

impl ReplicationSender {
    pub async fn connect(addr: SocketAddr) -> Result<Self>;

    /// Send a batch of operations as a single OP_REPLICA_BATCH frame.
    /// Returns the sequence number of the last op in the batch.
    pub async fn send_batch(&mut self, ops: &[ReplicaOp]) -> Result<u64>;

    /// Wait for ACK that covers at least `through_sequence`.
    /// The replica ACKs the highest contiguous sequence it has applied.
    pub async fn wait_ack(&mut self, through_sequence: u64, timeout: Duration) -> Result<ReplicaAck>;

    /// Send a batch and wait for ACK in one call.
    pub async fn send_batch_and_wait(
        &mut self,
        ops: &[ReplicaOp],
        timeout: Duration,
    ) -> Result<()>;

    /// Check connection health.
    pub fn is_connected(&self) -> bool;

    /// Reconnect after failure.
    pub async fn reconnect(&mut self) -> Result<()>;

    /// Current replication lag (highest sent - highest ACKed).
    pub fn lag(&self) -> u64;

    /// Current catchup state.
    pub fn catchup_state(&self) -> &CatchupState;
}
```

### 8.3 Replication manager (multi-replica orchestration) — `src/replication/manager.rs`

The `ReplicationManager` owns all `ReplicationSender` instances and implements the acknowledgment policy. For RF=3, it holds two senders (master + 2 replicas = RF 3, senders are for the 2 replicas).

```rust
/// Acknowledgment policy: how many replicas must ACK before the master
/// considers a write durable.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AckPolicy {
    /// Wait for ALL replicas to ACK. Default. Strongest durability.
    WriteAll,
    /// Wait for floor(RF/2)+1 total copies (including master).
    /// For RF=3: master + 1 replica = 2, so wait for 1 replica ACK.
    /// Lower latency; used for SC mode with Raft.
    WriteMajority,
}

pub struct ReplicationConfig {
    pub replica_addrs: Vec<SocketAddr>,
    pub ack_policy: AckPolicy,              // default: WriteAll
    pub replication_timeout: Duration,       // per-batch timeout
    pub max_retries: u32,
    pub retry_backoff: Duration,
    pub catchup_batch_size: usize,           // entries per catchup batch
}

pub struct ReplicationManager {
    senders: Vec<ReplicationSender>,         // one per replica
    config: ReplicationConfig,
    redo_log: Arc<RedoLog>,
}

impl ReplicationManager {
    pub fn new(config: ReplicationConfig, redo_log: Arc<RedoLog>) -> Self;

    /// Connect to all configured replicas. Returns errors for any that fail
    /// (those replicas are marked as CatchingUp and will be caught up later).
    pub async fn connect_all(&mut self) -> Vec<(SocketAddr, Result<()>)>;

    /// Replicate a batch of operations to all live replicas in parallel,
    /// then wait for ACKs according to the configured ack_policy.
    ///
    /// This is the main entry point called by the master write path.
    pub async fn replicate_batch(&mut self, ops: &[ReplicaOp]) -> Result<()> {
        let required_acks = self.required_ack_count();
        let live_senders = self.live_senders_mut();

        if live_senders.len() < required_acks {
            return Err(Error::InsufficientReplicas {
                available: live_senders.len(),
                required: required_acks,
            });
        }

        // Send to ALL live replicas in parallel.
        // Use tokio::join! for a fixed small number, or
        // FuturesUnordered for dynamic sets.
        let futures: Vec<_> = live_senders
            .iter_mut()
            .map(|sender| sender.send_batch_and_wait(ops, self.config.replication_timeout))
            .collect();

        let results = join_all(futures).await;

        // Count successes.
        let mut successes = 0;
        let mut first_error: Option<Error> = None;
        for result in results {
            match result {
                Ok(()) => successes += 1,
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                    // Mark this sender for catchup (handled below).
                }
            }
        }

        match self.config.ack_policy {
            AckPolicy::WriteAll => {
                if successes == live_senders.len() {
                    Ok(())
                } else {
                    Err(first_error.unwrap())
                }
            }
            AckPolicy::WriteMajority => {
                // required_acks counts replica ACKs needed (master is implicit).
                if successes >= required_acks {
                    Ok(())
                } else {
                    Err(first_error.unwrap())
                }
            }
        }
    }

    /// Number of replica ACKs required to satisfy the ack policy.
    fn required_ack_count(&self) -> usize {
        let rf = self.senders.len() + 1; // senders.len() replicas + 1 master
        match self.config.ack_policy {
            AckPolicy::WriteAll => self.senders.len(), // all replicas
            AckPolicy::WriteMajority => {
                // Majority of RF including master. E.g., RF=3 → majority=2,
                // master counts as 1, so need 1 replica ACK.
                let majority = rf / 2 + 1;
                majority.saturating_sub(1) // subtract 1 for the master
            }
        }
    }

    /// Mark a sender as needing catchup and spawn the catchup task.
    pub async fn start_catchup(&mut self, replica_index: usize) -> Result<()>;

    /// Handle a reconnecting replica (called when a replica sends a CatchupRequest).
    pub async fn handle_reconnect(
        &mut self,
        replica_index: usize,
        request: CatchupRequest,
    ) -> Result<()>;

    fn live_senders_mut(&mut self) -> Vec<&mut ReplicationSender> {
        self.senders
            .iter_mut()
            .filter(|s| *s.catchup_state() == CatchupState::Live)
            .collect()
    }
}
```

### 8.4 Replication receiver (replica side) — `src/replication/receiver.rs`

```rust
pub struct ReplicationReceiver {
    listener: TcpListener,
    device: Arc<dyn BlockDevice>,
    index: Arc<RwLock<Index>>,
    allocator: Arc<Mutex<SlotAllocator>>,
    locks: Arc<StripedLocks>,
    last_applied_sequence: AtomicU64,
}

impl ReplicationReceiver {
    pub fn new(/* dependencies */) -> Self;

    /// Start listening for replication connections.
    /// Spawns a handler task for each connection.
    pub async fn start(&self, addr: SocketAddr) -> Result<JoinHandle<()>>;

    /// Apply a batch of replica operations (called by handler).
    /// Returns a single ACK covering all ops in the batch.
    fn apply_batch(&self, batch: ReplicaBatch) -> Result<ReplicaAck> {
        let mut last_applied = self.last_applied_sequence.load(Ordering::Acquire);

        for (i, op) in batch.ops.iter().enumerate() {
            let seq = batch.first_sequence + i as u64;

            // Idempotency: skip already-applied ops.
            if seq <= last_applied {
                continue;
            }

            // Acquire the transaction lock (same StripedLocks as the master).
            // Apply the operation using the same functions from Phases 3-6.
            self.apply_single(op)?;

            self.last_applied_sequence.store(seq, Ordering::Release);
            last_applied = seq;
        }

        Ok(ReplicaAck::Ok {
            through_sequence: batch.first_sequence + batch.ops.len() as u64 - 1,
        })
    }

    /// Apply a single ReplicaOp by dispatching to the appropriate mutation function.
    fn apply_single(&self, op: &ReplicaOp) -> Result<()>;

    /// On reconnect, send a CatchupRequest to the master with our last applied sequence.
    pub fn last_applied_sequence(&self) -> u64 {
        self.last_applied_sequence.load(Ordering::Acquire)
    }
}
```

The `apply_single` dispatch:
1. Acquire the transaction lock (same `StripedLocks` as the master)
2. Call the appropriate operation function (spend, setMined, pruneSlot, etc.) from Phases 3-6
3. For `PruneSlot`: zero out the UTXO slot at the given offset — same logic as the master's pruner but without scheduling, since the master already decided to prune

### 8.5 Batch replication — `src/replication/batching.rs`

Operations are not sent one-per-frame. During a batch of client mutations (e.g., a `spendMulti` of 50 UTXOs or a block's worth of `setMined` calls), the master accumulates `ReplicaOp`s and flushes them to replicas as a single `OP_REPLICA_BATCH` frame.

```rust
/// Accumulates ReplicaOps during a batch of client mutations, then flushes
/// to all replicas as a single OP_REPLICA_BATCH frame.
pub struct ReplicaBatchAccumulator {
    ops: Vec<ReplicaOp>,
    max_batch_size: usize,   // flush automatically if this many ops accumulate
}

impl ReplicaBatchAccumulator {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            ops: Vec::with_capacity(max_batch_size),
            max_batch_size,
        }
    }

    /// Add an op to the current batch. Does NOT send anything yet.
    pub fn push(&mut self, op: ReplicaOp) {
        self.ops.push(op);
    }

    /// Take all accumulated ops, clearing the accumulator.
    /// The caller sends these as a single ReplicaBatch via the ReplicationManager.
    pub fn drain(&mut self) -> Vec<ReplicaOp> {
        std::mem::take(&mut self.ops)
    }

    /// Number of accumulated ops.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Whether the batch has reached the flush threshold.
    pub fn should_flush(&self) -> bool {
        self.ops.len() >= self.max_batch_size
    }
}
```

#### Integration with the master write path

```rust
// Example: spendMulti processing on the master.
// All spends in the batch share a single replication round-trip.

async fn spend_multi(
    requests: &[SpendRequest],
    redo_log: &RedoLog,
    device: &dyn BlockDevice,
    replication: &mut ReplicationManager,
) -> Result<Vec<SpendResult>> {
    let mut accumulator = ReplicaBatchAccumulator::new(requests.len());
    let mut results = Vec::with_capacity(requests.len());

    for req in requests {
        // 1. Local redo log + data write (same as before)
        redo_log.append_and_flush(RedoOp::Spend { /* ... */ })?;
        write_utxo_slot(device, /* ... */)?;
        write_metadata(device, /* ... */)?;

        // 2. Accumulate the ReplicaOp (no network I/O yet)
        accumulator.push(ReplicaOp::Spend {
            tx_key: req.tx_key,
            offset: req.offset,
            spending_data: req.spending_data,
        });

        results.push(SpendResult::Ok);
    }

    // 3. Flush entire batch to all replicas in one frame
    let ops = accumulator.drain();
    replication.replicate_batch(&ops).await?;

    // 4. Only now return success to the client
    Ok(results)
}
```

This batching reduces the number of network round-trips. A `spendMulti` of 50 UTXOs sends one `OP_REPLICA_BATCH` frame containing 50 ops rather than 50 individual frames.

### 8.6 Catchup protocol — `src/replication/catchup.rs`

When a replica reconnects after being down, it must catch up on missed operations before it can receive live replication traffic.

#### Protocol flow

1. Replica establishes TCP connection to master
2. Replica sends `CatchupRequest { last_ack_sequence }` — the highest sequence it has durably applied
3. Master reads redo log entries from `last_ack_sequence + 1` forward
4. Master streams them as `ReplicaBatch` frames (sized by `catchup_batch_size` from config)
5. Replica applies each batch, ACKs each batch
6. Once the replica's acknowledged sequence matches the master's current sequence, the master transitions the replica to live replication
7. The master's `ReplicationSender` for that replica changes state from `CatchingUp` to `Live`

#### Implementation

```rust
/// Run the catchup protocol for a single replica.
/// Called on the master side when a replica reconnects.
pub async fn run_catchup(
    redo_log: &RedoLog,
    sender: &mut ReplicationSender,
    from_sequence: u64,
    catchup_batch_size: usize,
    timeout: Duration,
) -> Result<()> {
    let mut current_seq = from_sequence + 1;
    let master_head = redo_log.current_sequence();

    log::info!(
        "Starting catchup for replica {} from sequence {} (master at {})",
        sender.replica_addr,
        current_seq,
        master_head,
    );

    // Transition sender to CatchingUp state.
    sender.state = CatchupState::CatchingUp { from_sequence };

    while current_seq <= master_head {
        // Read a batch of redo entries.
        let end_seq = std::cmp::min(current_seq + catchup_batch_size as u64, master_head + 1);
        let redo_entries = redo_log.read_range(current_seq, end_seq)?;

        // Convert RedoOps to ReplicaOps.
        let ops: Vec<ReplicaOp> = redo_entries
            .iter()
            .map(|entry| entry.to_replica_op())
            .collect();

        if ops.is_empty() {
            break;
        }

        // Send as a single batch frame and wait for ACK.
        sender.send_batch_and_wait(&ops, timeout).await?;

        current_seq = end_seq;

        // Re-check master head — new ops may have arrived during catchup.
        let new_head = redo_log.current_sequence();
        if new_head > master_head {
            // Master advanced while we were catching up. We will loop again
            // to pick up the new entries.
        }
    }

    // Caught up — transition to live replication.
    sender.state = CatchupState::Live;

    log::info!(
        "Replica {} caught up to sequence {}. Switching to live replication.",
        sender.replica_addr,
        current_seq - 1,
    );

    Ok(())
}
```

#### Receiver side (on the replica)

```rust
/// Called on replica startup or reconnection.
/// Sends CatchupRequest, then processes batches until master transitions us to live.
pub async fn request_catchup(
    connection: &mut TcpStream,
    last_applied: u64,
) -> Result<()> {
    // Send our last applied sequence to the master.
    let request = CatchupRequest {
        last_ack_sequence: last_applied,
    };
    send_frame(connection, &request).await?;

    // Master will now stream ReplicaBatch frames.
    // We apply them using the normal apply_batch path.
    // The master signals transition to live mode by sending live ops
    // (detected when the batch sequence is contiguous with our current state).
    Ok(())
}
```

### 8.7 Replication configuration

```rust
pub struct ReplicationConfig {
    pub replica_addrs: Vec<SocketAddr>,
    pub ack_policy: AckPolicy,              // default: WriteAll
    pub replication_timeout: Duration,       // per-batch timeout
    pub max_retries: u32,
    pub retry_backoff: Duration,
    pub catchup_batch_size: usize,           // entries per catchup batch (default: 1000)
}
```

### 8.8 Integration with master write path — failure handling

If replication fails (timeout, connection error):
- Retry with backoff up to `max_retries`
- If a replica is unreachable for longer than a threshold, mark it as down and transition its sender to `CatchingUp`
- Under `WriteAll` policy: if any replica is down, the master returns an error to the client (strict durability)
- Under `WriteMajority` policy: the master continues as long as a majority of replicas (including the master) have the data
- When a replica recovers, it sends a `CatchupRequest` and the master runs the catchup protocol (§8.6)

## Acceptance criteria

### Serialization tests

```
- [ ] Each ReplicaOp variant (including PruneSlot): serialize -> deserialize round-trip matches
- [ ] Spend op serialized size < 80 bytes
- [ ] PruneSlot op serialized size < 44 bytes
- [ ] Create op with 100 UTXOs: serialize/deserialize round-trip correct
- [ ] ReplicaBatch of 100 ops: serialize/deserialize round-trip correct
- [ ] Batch header overhead is exactly 12 bytes (8 first_sequence + 4 op count)
```

### Single-operation replication tests (using localhost TCP)

```
- [ ] Spend on master, replica receives and applies: replica state matches master
- [ ] SetMined on master: replica has the block entry
- [ ] Create on master: replica has the record
- [ ] Delete on master: replica no longer has the record
- [ ] Freeze/unfreeze on master: replica state matches
- [ ] Reassign on master: replica has new hash
- [ ] PruneSlot on master: replica has the slot zeroed out
- [ ] All flag operations replicated correctly
```

### Batch replication tests

```
- [ ] spendMulti of 50 ops: sent as a single OP_REPLICA_BATCH frame, not 50 frames
- [ ] setMined for 200 txs in a block: sent as batched frames (≤ catchup_batch_size per frame)
- [ ] ReplicaBatch with first_sequence=100 and 10 ops: replica ACKs with through_sequence=109
- [ ] Mixed op types in one batch (Spend + SetMined + PruneSlot): all applied correctly
```

### RF=3 parallel send tests

```
- [ ] With 2 replicas configured: both receive every batch
- [ ] Send timing: both replicas receive the batch concurrently (not sequentially)
- [ ] If one replica is slow (inject 100ms delay), the other is not blocked
- [ ] With WriteAll: master waits for BOTH replicas before returning success
- [ ] With WriteMajority: master returns success after 1 of 2 replicas ACKs
```

### Acknowledgment policy tests

```
- [ ] WriteAll with RF=3: if 1 replica fails, master returns error
- [ ] WriteAll with RF=3: if both replicas ACK, master returns success
- [ ] WriteMajority with RF=3: if 1 of 2 replicas ACKs, master returns success
- [ ] WriteMajority with RF=3: if 0 of 2 replicas ACK, master returns error
- [ ] WriteMajority with RF=2: need 1 replica ACK (majority of 2 = 1 + master)
- [ ] Policy is configurable at startup via ReplicationConfig
```

### Synchronous replication guarantee tests

```
- [ ] Master returns success ONLY after required replicas ACK per policy
- [ ] Master with timeout: if replica is slow, master returns error after timeout
- [ ] Client does NOT see success if required replicas haven't ACKed
```

### Idempotency under replication tests

```
- [ ] Send same Spend op twice to replica: applied once, second is no-op
- [ ] Send same SetMined twice: one entry, not two
- [ ] Send same PruneSlot twice: slot remains zeroed, no error
- [ ] Send ops out of order (sequence 5 before sequence 4): replica handles correctly
      (either buffers or applies if safe)
- [ ] Catchup replays ops already partially applied: no duplicates
```

### Catchup protocol tests

```
- [ ] Replica sends CatchupRequest with last_ack_sequence=100, master has sequence=200:
      master streams entries 101-200 as ReplicaBatch frames
- [ ] Catchup batching: 500 missed ops with catchup_batch_size=100 produces 5 batch frames
- [ ] Replica applies catchup batches idempotently: re-sending a batch is a no-op
- [ ] During catchup, new ops arrive on master: catchup loop picks them up and
      continues until fully caught up
- [ ] After catchup completes, sender state transitions from CatchingUp to Live
- [ ] Live ops sent after catchup are applied with correct contiguous sequence numbers
- [ ] Replica that was never connected (last_ack_sequence=0): full catchup from beginning
```

### Failure and recovery tests

```
- [ ] Replica goes down, master continues operating, replica comes back:
      catchup replays missed ops, states converge
- [ ] Kill replica mid-operation: master detects failure, retries or marks down
- [ ] Network partition (drop packets): master detects timeout, marks replica down
- [ ] After partition heals: replica reconnects, sends CatchupRequest, catchup succeeds,
      states converge
- [ ] Master crashes and recovers: redo log replay + replication catchup
- [ ] RF=3 with one replica down: under WriteMajority, master continues serving writes
      with the remaining replica; downed replica catches up on reconnect
```

### Consistency verification tests

```
- [ ] After 10000 random operations on master with live replica:
      full scan of both datasets, every record identical
- [ ] After 10000 ops with 5 replica disconnects/reconnects:
      states converge after final catchup
- [ ] RF=3: after 10000 ops, all three copies (master + 2 replicas) are identical
```

### Performance tests

```
- [ ] Replication overhead: measure spend throughput with 0, 1, 2 replicas
- [ ] Batch vs single-frame replication: measure throughput improvement from batching
- [ ] Replication bandwidth: measure bytes/sec at sustained operation rate
- [ ] Catchup speed: 10000 entries catchup time
- [ ] Replication latency: time from master send to replica ACK (localhost)
- [ ] RF=3 parallel send latency: confirm it is close to max(replica1_latency, replica2_latency),
      not the sum
```

## NOT in this phase

- No cluster management (which node is master is static for now)
- No automatic failover
- No multi-datacenter replication (XDR equivalent)
- No Raft-based leader election (that builds on top of this replication layer)
