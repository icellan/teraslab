# Phase 9: Clustering

## Goal

Implement hash-based sharding across multiple nodes, heartbeat-based membership detection, and data migration on topology changes. After this phase, TeraSlab operates as a distributed cluster.

## Dependencies

Phases 1-8 must be complete with all tests passing.

## Reference

- `specs/BSV_UTXO_STORE_SPEC.md` §9 (Cluster Management) — 4096 shards, 12-bit hash
- Aerospike uses 4096 partitions with Paxos-based gossip. TeraSlab uses simpler deterministic hash-based sharding since the workload doesn't need the complexity.

## What to build

### 9.1 Shard table — `src/cluster/shards.rs`

```rust
pub const NUM_SHARDS: usize = 4096;  // Match Aerospike for familiarity

pub struct ShardTable {
    /// For each shard: which node is the master, which nodes are replicas
    assignments: [ShardAssignment; NUM_SHARDS],
    version: u64,  // incremented on every topology change
}

#[derive(Clone, Debug)]
pub struct ShardAssignment {
    pub master: NodeId,
    pub replicas: Vec<NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct NodeId(pub u64);

impl ShardTable {
    /// Deterministically compute the shard table from a sorted member list.
    ///
    /// This is a **pure function**: given the same inputs, every node in the
    /// cluster will compute the identical shard table independently. No leader
    /// election or consensus protocol is needed.
    ///
    /// Algorithm (round-robin over sorted members):
    ///   - Sort `members` (they must already be sorted; caller ensures this).
    ///   - For shard N with replication_factor RF:
    ///       master  = members[N % members.len()]
    ///       replica = members[(N + 1) % members.len()]   (if RF >= 2)
    ///       ...up to RF-1 replicas, each at (N + i) % members.len()
    ///   - If members.len() < RF, replicas are clamped to available nodes
    ///     (no node appears twice for the same shard).
    ///
    /// Because SWIM guarantees eventual consistency of the member list,
    /// every node will converge on the same shard table after a membership
    /// change propagates.
    pub fn compute(members: &[NodeId], replication_factor: u8) -> ShardTable {
        assert!(!members.is_empty(), "cannot compute shard table with 0 members");
        let n = members.len();
        let mut assignments = Vec::with_capacity(NUM_SHARDS);
        for shard in 0..NUM_SHARDS {
            let master = members[shard % n];
            let mut replicas = Vec::new();
            for r in 1..replication_factor as usize {
                let replica = members[(shard + r) % n];
                if replica != master {
                    replicas.push(replica);
                }
            }
            assignments.push(ShardAssignment { master, replicas });
        }
        ShardTable {
            assignments: assignments.try_into().unwrap(),
            version: 0, // caller sets version
        }
    }

    /// Compute which shard a key belongs to.
    pub fn shard_for_key(key: &TxKey) -> u16 {
        // Use first 12 bits of txid (matching Aerospike's RIPEMD-160 → 12-bit partition)
        let h = u16::from_le_bytes([key.txid[0], key.txid[1]]);
        h & 0x0FFF  // 12 bits = 4096 shards
    }

    /// Which node is the master for this key?
    pub fn master_for_key(&self, key: &TxKey) -> NodeId;

    /// Which nodes hold replicas for this key?
    pub fn replicas_for_key(&self, key: &TxKey) -> &[NodeId];

    /// Compute which shards need to migrate when topology changes.
    pub fn migration_plan(old: &ShardTable, new: &ShardTable) -> Vec<MigrationTask>;
}

pub struct MigrationTask {
    pub shard: u16,
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub is_master: bool,  // master migration vs replica migration
}
```

#### Shard table consensus — why no leader election is needed

The shard table is computed deterministically from two inputs: the sorted list of alive members and the replication factor. Both are known to every node.

1. When SWIM detects a membership change (node join, node declared dead), the updated member list propagates to all nodes via piggybacked gossip.
2. Each node independently calls `ShardTable::compute(sorted_members, rf)`.
3. Because the function is pure — same inputs always produce the same output — every node arrives at the identical shard table without any coordination, voting, or leader election.
4. The `version` field is derived deterministically (e.g., hash of the sorted member list) so nodes can compare shard table versions to detect staleness.

This is strictly simpler than Aerospike's Paxos-based partition assignment and eliminates an entire class of split-brain bugs.

### 9.2 SWIM membership protocol — `src/cluster/swim.rs`

SWIM (Scalable Weakly-consistent Infection-style Membership) for detecting node join/leave. Scales to 1000+ nodes with O(1) network load per node. Use the `foca` crate or implement directly.

```rust
pub struct SwimConfig {
    pub probe_interval: Duration,    // default 200ms
    pub indirect_probes: u32,        // K = 3 (ask K peers to probe suspect)
    pub suspicion_timeout: Duration, // default 5s
    pub bind_addr: SocketAddr,       // UDP port for probes
    pub seed_nodes: Vec<SocketAddr>,
}

pub struct SwimMembership {
    config: SwimConfig,
    self_id: NodeId,
    members: HashMap<NodeId, MemberState>,
    event_tx: mpsc::Sender<ClusterEvent>,
}

struct MemberState {
    addr: SocketAddr,
    state: NodeState,  // Alive, Suspect, Dead
    incarnation: u64,
}

pub enum ClusterEvent {
    NodeJoined(NodeId, SocketAddr),
    NodeSuspect(NodeId),
    NodeLeft(NodeId),
    MembershipChanged(Vec<NodeId>),
}
```

The SWIM protocol loop:
1. Each probe interval (200ms), select one random member and send a UDP **ping**
2. If ping ACK received -> member is alive
3. If no ACK within timeout -> send **ping-req** to K (3) random other members, asking them to ping the suspect
4. If any indirect probe succeeds -> member is alive
5. If all fail -> mark as **suspect**, start suspicion timer
6. If suspicion timer expires without contradiction -> declare **dead**, emit `NodeLeft`
7. Membership updates (join/suspect/dead) are piggybacked on all probe messages — dissemination is O(log N) rounds

**Probe message** (UDP, piggybacked with membership updates):
```
[magic: u32][sender_id: u64][sender_incarnation: u64][shard_table_version: u64]
[update_count: u16][updates: {node_id: u64, state: u8, incarnation: u64, addr: SocketAddr}[]]
```

**Why SWIM over TCP mesh**: A TCP mesh requires N x (N-1) connections — at 256 nodes that's 65K connections. SWIM uses UDP probes with bounded fan-out: each node sends exactly 1 probe per interval regardless of cluster size.

### 9.3 Cluster coordinator — `src/cluster/coordinator.rs`

Manages the cluster lifecycle: responds to heartbeat events, triggers rebalancing, coordinates migration.

```rust
pub struct ClusterCoordinator {
    self_id: NodeId,
    shard_table: Arc<RwLock<ShardTable>>,
    swim: SwimMembership,
    migration_manager: MigrationManager,
    replication_factor: u8,
}

impl ClusterCoordinator {
    pub fn start(config: ClusterConfig) -> Result<Self>;

    /// Called when SWIM detects a membership change.
    /// Recomputes the shard table deterministically and initiates migrations.
    fn on_membership_change(&mut self, alive_members: Vec<NodeId>) {
        // 1. Sort members deterministically
        let mut sorted = alive_members.clone();
        sorted.sort();

        // 2. Compute new shard table (pure function — every node gets the same result)
        let old_table = self.shard_table.read();
        let new_table = ShardTable::compute(&sorted, self.replication_factor);

        // 3. Compute migration plan
        let plan = ShardTable::migration_plan(&old_table, &new_table);

        // 4. Apply new shard table
        *self.shard_table.write() = new_table;

        // 5. Execute migrations for shards this node is sending or receiving
        self.migration_manager.execute(plan);
    }

    /// Current shard table (for client routing).
    pub fn shard_table(&self) -> Arc<RwLock<ShardTable>>;

    /// Is this node the master for the given key?
    pub fn is_master(&self, key: &TxKey) -> bool;

    /// Route decision for an incoming request.
    pub fn route_or_handle(&self, key: &TxKey) -> RouteDecision;
}

pub enum RouteDecision {
    HandleLocally,
    RedirectTo(NodeId, SocketAddr),
}
```

### 9.4 Data migration — `src/cluster/migration.rs`

When shards move between nodes, their data must migrate.

```rust
pub struct MigrationManager {
    active_migrations: Vec<ActiveMigration>,
    device: Arc<dyn BlockDevice>,
    index: Arc<RwLock<Index>>,
}

struct ActiveMigration {
    shard: u16,
    target: NodeId,
    state: MigrationState,
    progress: MigrationProgress,
}

enum MigrationState {
    /// Scanning index, streaming records to target
    Streaming,
    /// All records sent, waiting for target acknowledgement
    WaitingForAck,
    /// Target confirmed, safe to remove local data
    Complete,
}

pub struct MigrationProgress {
    pub total_records: u64,
    pub migrated_records: u64,
    pub bytes_sent: u64,
}
```

Migration procedure (outbound — sending shard to new owner):
1. Scan the index for all records belonging to this shard
2. For each record:
   a. Read the full record from device (metadata + all UTXO slots + cold data)
   b. Send to the target node as a Create ReplicaOp
   c. Mark the shard locally as "migrating to B"
3. After all records sent:
   a. Send a "migration complete" message
   b. Remove the shard from local ownership
   c. Delete migrated records (return space to allocator)

#### Write proxying during migration

During migration of shard S from node A (old master) to node B (new master):

- **Reads arriving at node A**: served locally (node A still has the data).
- **Writes arriving at node A**: node A returns a **Redirect** response pointing the client to node B. The client re-sends the write directly to B. There is no server-side proxying — the Redirect tells the client where to go, keeping the hot path free of extra hops.
- **Why Redirect instead of proxy**: At millions of ops/sec, forwarding writes through an intermediary would add latency and create a bottleneck on the old master. A Redirect is a single small response; the client contacts B directly for the actual write.

Once migration completes and node A removes shard S from its table, both reads and writes for shard S go exclusively to node B.

### 9.5 Client routing info — `src/cluster/routing.rs`

The client needs to know which node to talk to for each key. Provide an endpoint that serves the shard table:

```rust
pub struct RoutingInfo {
    pub shard_table_version: u64,
    pub nodes: Vec<NodeInfo>,
    pub shard_assignments: Vec<(u16, NodeId)>,  // shard -> master node
}

pub struct NodeInfo {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub is_alive: bool,
}
```

### 9.6 Capacity planning

See spec §9.4 for the full capacity table, including per-node throughput assumptions, theoretical cluster maximums (4 nodes at 40M TPS through 512 nodes at 5B+ TPS), and scaling bottleneck analysis by tier. Key takeaways:

- The bottleneck at 10M ops/sec per node is typically NVMe IOPS, not CPU.
- At 64+ nodes, network bandwidth (RF=2 replication) becomes the constraint — upgrade to 25/100 GbE.
- The 4096-shard default supports up to 512 nodes at 8 shards/node; increase shard count for larger clusters.
- SWIM membership protocol itself scales to 1000+ nodes with O(1) network load.

## Acceptance criteria

### Shard table tests

```
- [ ] shard_for_key with uniform random keys: shards are approximately evenly distributed
- [ ] shard_for_key is deterministic: same key always maps to same shard
- [ ] ShardTable::compute is deterministic: same (members, rf) always produces the same assignments
- [ ] ShardTable::compute called independently on two separate "nodes" with the same sorted member list yields identical shard tables
- [ ] rebalance with 3 nodes, RF=2: each shard has 1 master and 1 replica, no node is both master and replica for the same shard
- [ ] rebalance with 3 nodes, RF=2: verify round-robin assignment — shard 0 master is members[0], shard 1 master is members[1], shard 2 master is members[2], shard 3 master is members[0], etc.
- [ ] rebalance with 5 nodes, RF=2: shards approximately equal across nodes (±5%)
- [ ] rebalance: no shard without a master
- [ ] rebalance: no shard with master == replica
- [ ] rebalance with 2 nodes, RF=2: every shard has master on one node and replica on the other
- [ ] rebalance with 1 node, RF=2: all shards mastered on the single node, no replicas (clamped)
- [ ] migration_plan: node added, correct shards identified for migration
- [ ] migration_plan: node removed, correct shards identified
- [ ] migration_plan: no unnecessary migrations (shards that don't need to move stay put)
```

### SWIM membership tests (using localhost UDP)

```
- [ ] Two nodes discover each other via seed: both see each other as alive
- [ ] Three nodes form cluster: all three see each other
- [ ] Node stops responding to probes: marked suspect, then dead after suspicion timeout
- [ ] Dead node restarts: detected as alive again via new incarnation number
- [ ] Indirect probes: node A can't reach B, but C can -> B stays alive
- [ ] ClusterEvent::NodeJoined emitted when new node appears
- [ ] ClusterEvent::NodeSuspect emitted when probe fails
- [ ] ClusterEvent::NodeLeft emitted after suspicion timeout
- [ ] MembershipChanged contains correct sorted member list after changes
- [ ] Membership updates disseminate across 10-node cluster within O(log N) rounds
- [ ] Network load per node is constant regardless of cluster size (test with 3 vs 20 nodes)
- [ ] After membership change, all nodes independently compute the same shard table
```

### Coordinator tests

```
- [ ] Start 3-node cluster: all shards assigned, all nodes see same shard table
- [ ] Add 4th node: rebalance triggers, shards redistribute, migration plan created
- [ ] Remove node: rebalance triggers, orphaned shards reassigned
- [ ] is_master returns true only for shards this node owns
- [ ] route_or_handle returns HandleLocally for owned shards, RedirectTo for others
- [ ] Two coordinators given the same MembershipChanged event produce identical shard tables
```

### Migration tests

```
- [ ] Migrate shard with 100 records to new node: all records appear on new node
- [ ] After migration: records no longer on old node (space freed)
- [ ] During migration: reads still served from old node
- [ ] During migration: writes return Redirect to new node (not proxied server-side)
- [ ] Client receiving Redirect re-sends write to new node: write succeeds
- [ ] After migration complete: all operations go to new node
- [ ] Verify: no records lost during migration (count before == count after)
- [ ] Verify: no duplicate records after migration
- [ ] Migration of empty shard: completes without error
```

### Cluster integration tests

```
- [ ] Start 3-node cluster, create 1000 records: distributed across shards
- [ ] Query each record: reaches correct node, returns correct data
- [ ] Spend operations routed to correct master: succeeds
- [ ] Add 4th node, wait for migration: all records still accessible
- [ ] Kill one node, wait for detection: operations on affected shards fail or proxy to replica
```

## NOT in this phase

- No automatic failover (replica promotion to master)
- No split-brain protection
- No rack awareness
- No XDR (cross-datacenter replication)
