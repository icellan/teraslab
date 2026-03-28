//! Data migration tracking for shard rebalancing.

use crate::cluster::shards::{MigrationTask, NodeId, NUM_SHARDS};

// ---------------------------------------------------------------------------
// ShardBitmap — O(1) per-shard flag storage
// ---------------------------------------------------------------------------

/// Fixed-size bitmap for 4096 shards. Each shard maps to one bit:
/// `word = shard / 64`, `bit = shard % 64`.
///
/// All operations are O(1). Memory footprint: 512 bytes.
#[derive(Clone, Debug)]
pub struct ShardBitmap {
    words: [u64; Self::WORDS],
}

impl ShardBitmap {
    const WORDS: usize = NUM_SHARDS / 64; // 64

    /// Create an empty bitmap (all bits clear).
    pub const fn new() -> Self {
        Self { words: [0u64; Self::WORDS] }
    }

    /// Set the bit for `shard`.
    pub fn set(&mut self, shard: u16) {
        let (w, b) = Self::pos(shard);
        self.words[w] |= 1u64 << b;
    }

    /// Clear the bit for `shard`.
    pub fn clear(&mut self, shard: u16) {
        let (w, b) = Self::pos(shard);
        self.words[w] &= !(1u64 << b);
    }

    /// Test whether `shard` is set.
    pub fn test(&self, shard: u16) -> bool {
        let (w, b) = Self::pos(shard);
        (self.words[w] >> b) & 1 == 1
    }

    /// Clear all bits.
    pub fn clear_all(&mut self) {
        self.words = [0u64; Self::WORDS];
    }

    /// Number of set bits.
    pub fn count(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    fn pos(shard: u16) -> (usize, u32) {
        ((shard as usize) / 64, (shard as u32) % 64)
    }
}

impl Default for ShardBitmap {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// AtomicShardBitmap — lock-free per-shard flags for the hot path
// ---------------------------------------------------------------------------

/// Atomic version of [`ShardBitmap`] for use on the request hot path.
///
/// Read operations (`test`) are a single `AtomicU64::load` + bit test,
/// giving O(1) with zero contention. Write operations (`set`/`clear`)
/// use `fetch_or`/`fetch_and` (also lock-free).
///
/// The bitmap is maintained as a shadow of the authoritative state inside
/// `MigrationManager`. Mutation methods on `RunningCluster` update both
/// the manager (under its Mutex) and the atomic bitmap.
pub struct AtomicShardBitmap {
    words: [std::sync::atomic::AtomicU64; Self::WORDS],
}

impl AtomicShardBitmap {
    const WORDS: usize = NUM_SHARDS / 64; // 64

    /// Create an empty atomic bitmap.
    pub fn new() -> Self {
        Self {
            words: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Set the bit for `shard` (lock-free).
    pub fn set(&self, shard: u16) {
        let (w, b) = Self::pos(shard);
        self.words[w].fetch_or(1u64 << b, std::sync::atomic::Ordering::Release);
    }

    /// Clear the bit for `shard` (lock-free).
    pub fn clear(&self, shard: u16) {
        let (w, b) = Self::pos(shard);
        self.words[w].fetch_and(!(1u64 << b), std::sync::atomic::Ordering::Release);
    }

    /// Test whether `shard` is set (lock-free, no contention).
    pub fn test(&self, shard: u16) -> bool {
        let (w, b) = Self::pos(shard);
        (self.words[w].load(std::sync::atomic::Ordering::Acquire) >> b) & 1 == 1
    }

    /// Clear all bits.
    pub fn clear_all(&self) {
        for w in &self.words {
            w.store(0, std::sync::atomic::Ordering::Release);
        }
    }

    /// Bulk-copy from a [`ShardBitmap`] snapshot.
    ///
    /// Used to synchronize the atomic bitmap after a batch update
    /// to the MigrationManager (e.g., after `cleanup_completed`).
    pub fn load_from(&self, bitmap: &ShardBitmap) {
        for (i, w) in self.words.iter().enumerate() {
            w.store(bitmap.words[i], std::sync::atomic::Ordering::Release);
        }
    }

    fn pos(shard: u16) -> (usize, u32) {
        ((shard as usize) / 64, (shard as u32) % 64)
    }
}

impl Default for AtomicShardBitmap {
    fn default() -> Self {
        Self::new()
    }
}

/// State of an active shard migration.
///
/// The lifecycle follows an explicit handoff protocol:
/// ```text
/// Preparing → Streaming → Fenced → Complete
///                  ↓          ↓
///               Failed     Failed
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationState {
    /// Migration registered; source is preparing the baseline snapshot.
    /// Writes continue on the source during this phase.
    Preparing,
    /// Baseline records are being streamed to the target.
    /// Writes continue on the source; any mutations during this phase
    /// will be captured as deltas via the redo log sequence checkpoint.
    Streaming,
    /// Baseline complete. Source writes for this shard are fenced
    /// (rejected with ERR_MIGRATION_IN_PROGRESS). Deltas from the
    /// redo log between the snapshot sequence and the fence sequence
    /// are streamed to the target.
    Fenced,
    /// Target confirmed receipt of baseline + deltas. Handoff committed.
    Complete,
    /// Migration failed after all retries; kept for visibility and retry.
    Failed,
}

/// Progress of a single shard migration.
#[derive(Debug, Clone)]
pub struct MigrationProgress {
    /// Shard being migrated.
    pub shard: u16,
    /// Source node.
    pub from_node: NodeId,
    /// Target node.
    pub to_node: NodeId,
    /// Current state.
    pub state: MigrationState,
    /// Total records to migrate.
    pub total_records: u64,
    /// Records migrated so far.
    pub migrated_records: u64,
    /// Bytes sent so far.
    pub bytes_sent: u64,
    /// Whether this is a master migration (true) or replica backfill (false).
    pub is_master: bool,
    /// Redo log sequence at the time the baseline snapshot was taken.
    /// Mutations after this sequence must be streamed as deltas before
    /// the handoff can be committed.
    pub snapshot_sequence: u64,
    /// Redo log sequence at the time writes were fenced on the source.
    /// All mutations between snapshot_sequence and fence_sequence are
    /// the delta that must be applied on the target.
    pub fence_sequence: u64,
}

impl MigrationProgress {
    /// Create a new migration progress from a task.
    pub fn from_task(task: &MigrationTask) -> Self {
        Self {
            shard: task.shard,
            from_node: task.from_node,
            to_node: task.to_node,
            is_master: task.is_master,
            state: MigrationState::Preparing,
            total_records: 0,
            migrated_records: 0,
            bytes_sent: 0,
            snapshot_sequence: 0,
            fence_sequence: 0,
        }
    }

    /// Fraction complete (0.0–1.0).
    pub fn fraction_complete(&self) -> f64 {
        if self.total_records == 0 {
            return 1.0;
        }
        self.migrated_records as f64 / self.total_records as f64
    }

    /// Whether the migration is finished.
    pub fn is_complete(&self) -> bool {
        self.state == MigrationState::Complete
    }
}

/// Tracks an inbound migration with its source for per-task granularity.
///
/// This replaces the previous `Vec<u16>` inbound tracking which was too
/// coarse — clearing all inbound state when outbound work finished could
/// remove protection for shards still receiving data from other nodes.
#[derive(Debug, Clone)]
struct InboundMigration {
    shard: u16,
    from_node: NodeId,
    /// True once `OP_MIGRATION_COMPLETE` confirmed data arrived.
    completed: bool,
    /// When this entry was created. Used for staleness-based cleanup:
    /// entries older than a threshold are removed to prevent indefinite
    /// write-blocking from abandoned migrations.
    created_at: std::time::Instant,
}

/// Manages active migrations for this node.
pub struct MigrationManager {
    active: Vec<MigrationProgress>,
    /// Per-task inbound migration tracking. Each entry represents a shard
    /// this node expects to receive data for, with its source node.
    /// Entries are only removed when explicitly marked completed.
    inbound_migrations: Vec<InboundMigration>,
    /// O(1) bitmap shadow of `inbound_migrations` for fast `has_pending_inbound`.
    /// Kept in sync: set when an entry is added, cleared when all entries for
    /// that shard are completed.
    inbound_bitmap: ShardBitmap,
    /// Shards where writes are fenced on this node (source is migrating out).
    /// Dispatch rejects mutations for these shards with ERR_MIGRATION_IN_PROGRESS.
    /// Reads continue to be served locally during the fence.
    fenced_shards: ShardBitmap,
}

impl MigrationManager {
    /// Create a new migration manager with no active migrations.
    pub fn new() -> Self {
        Self {
            active: Vec::new(),
            inbound_migrations: Vec::new(),
            inbound_bitmap: ShardBitmap::new(),
            fenced_shards: ShardBitmap::new(),
        }
    }

    /// Start migrations from a list of tasks.
    ///
    /// Outbound tasks (this node is source) are fully tracked with progress.
    /// Inbound tasks (this node is target) are registered per-task with
    /// the source node, only for shards that have data (`populated_shards`).
    /// Empty shards need no transfer and should not block reads/writes.
    pub fn start_outbound(
        &mut self,
        tasks: &[MigrationTask],
        self_id: NodeId,
        populated_shards: &std::collections::HashSet<u16>,
    ) {
        for task in tasks {
            if task.from_node == self_id {
                self.active.push(MigrationProgress::from_task(task));
            }
            // Register inbound for shards that have data on the source node.
            // Empty shards complete instantly with no data transfer.
            if task.to_node == self_id
                && populated_shards.contains(&task.shard)
                && !self.inbound_migrations.iter().any(|m| m.shard == task.shard && m.from_node == task.from_node)
            {
                self.inbound_migrations.push(InboundMigration {
                    shard: task.shard,
                    from_node: task.from_node,
                    completed: false,
                    created_at: std::time::Instant::now(),
                });
                self.inbound_bitmap.set(task.shard);
            }
        }
    }

    /// Register a shard as actively receiving inbound migration data.
    ///
    /// Called when the first `OP_REPLICA_BATCH` for this shard arrives,
    /// so the read/write path knows to wait for migration completion.
    /// Since we may not know the source node at dispatch time, register
    /// with `NodeId(0)` as a sentinel if no existing entry matches.
    pub fn mark_inbound_active(&mut self, shard: u16) {
        if !self.inbound_migrations.iter().any(|m| m.shard == shard && !m.completed) {
            self.inbound_migrations.push(InboundMigration {
                shard,
                from_node: NodeId(0),
                completed: false,
                created_at: std::time::Instant::now(),
            });
            self.inbound_bitmap.set(shard);
        }
    }

    /// Mark an inbound shard as received (data has arrived and been verified).
    ///
    /// Marks the first non-completed entry for this shard as completed.
    /// The entry is retained until `cleanup_completed()` removes it.
    pub fn mark_inbound_complete(&mut self, shard: u16) {
        if let Some(m) = self.inbound_migrations.iter_mut()
            .find(|m| m.shard == shard && !m.completed)
        {
            m.completed = true;
        }
        // Clear bitmap bit only if no more pending entries for this shard.
        if !self.inbound_migrations.iter().any(|m| m.shard == shard && !m.completed) {
            self.inbound_bitmap.clear(shard);
        }
    }

    /// Check if this node is expecting inbound data for the given shard.
    ///
    /// O(1) via bitmap lookup (no linear scan of inbound_migrations).
    pub fn has_pending_inbound(&self, shard: u16) -> bool {
        self.inbound_bitmap.test(shard)
    }

    /// Fence a shard on the source node — writes for this shard will be
    /// rejected with ERR_MIGRATION_IN_PROGRESS. Reads continue locally.
    pub fn fence_shard(&mut self, shard: u16) {
        self.fenced_shards.set(shard);
    }

    /// Remove the write fence for a shard (migration completed or failed).
    pub fn unfence_shard(&mut self, shard: u16) {
        self.fenced_shards.clear(shard);
    }

    /// Check if writes are fenced for the given shard on this node.
    ///
    /// O(1) via bitmap lookup.
    pub fn is_shard_fenced(&self, shard: u16) -> bool {
        self.fenced_shards.test(shard)
    }

    /// Transition a migration to the Fenced state and record the fence sequence.
    pub fn mark_fenced(&mut self, task: &MigrationTask, fence_sequence: u64) {
        if let Some(p) = self.find_task_mut(task) {
            p.state = MigrationState::Fenced;
            p.fence_sequence = fence_sequence;
        }
        self.fence_shard(task.shard);
    }

    /// Set the snapshot sequence checkpoint for a migration task.
    pub fn set_snapshot_sequence(&mut self, task: &MigrationTask, seq: u64) {
        if let Some(p) = self.find_task_mut(task) {
            p.snapshot_sequence = seq;
            p.state = MigrationState::Streaming;
        }
    }

    /// Mark records as migrated for a task identified by (shard, from, to).
    pub fn record_progress(&mut self, task: &MigrationTask, records: u64, bytes: u64) {
        if let Some(p) = self.find_task_mut(task) {
            p.migrated_records += records;
            p.bytes_sent += bytes;
        }
    }

    /// Mark a migration as complete and remove the write fence.
    ///
    /// The fence is only lifted if no other active migration task for
    /// this shard is still in the Fenced state. This prevents premature
    /// unfencing when multiple tasks target the same shard (e.g., master
    /// migration + replica backfill).
    pub fn mark_complete(&mut self, task: &MigrationTask) {
        if let Some(p) = self.find_task_mut(task) {
            p.state = MigrationState::Complete;
        }
        if !self.has_other_fenced_task(task.shard, task) {
            self.unfence_shard(task.shard);
        }
    }

    /// Mark a migration as failed after all retries exhausted.
    ///
    /// The write fence is lifted so the shard can continue serving on
    /// the old master, unless another task for the same shard is still
    /// in the Fenced state. Failed migrations are removed from the
    /// active list by the next call to `cleanup_completed()`.
    pub fn mark_failed(&mut self, task: &MigrationTask) {
        if let Some(p) = self.find_task_mut(task) {
            p.state = MigrationState::Failed;
        }
        if !self.has_other_fenced_task(task.shard, task) {
            self.unfence_shard(task.shard);
        }
    }

    /// Check if any active migration task for the given shard (other than
    /// the specified task) is still in the Fenced state.
    fn has_other_fenced_task(&self, shard: u16, exclude: &MigrationTask) -> bool {
        self.active.iter().any(|p| {
            p.shard == shard
                && p.state == MigrationState::Fenced
                && !(p.from_node == exclude.from_node && p.to_node == exclude.to_node)
        })
    }

    /// Number of failed migrations.
    pub fn failed_count(&self) -> usize {
        self.active.iter().filter(|p| p.state == MigrationState::Failed).count()
    }

    /// Reset a failed migration back to Streaming so it can be retried.
    ///
    /// Returns true if the migration was found and reset, false otherwise.
    pub fn retry_failed(&mut self, task: &MigrationTask) -> bool {
        if let Some(p) = self.active.iter_mut().find(|p| {
            p.shard == task.shard && p.from_node == task.from_node
                && p.to_node == task.to_node && p.state == MigrationState::Failed
        }) {
            p.state = MigrationState::Streaming;
            p.migrated_records = 0;
            p.bytes_sent = 0;
            true
        } else {
            false
        }
    }

    /// Collect all failed migration tasks for re-execution.
    pub fn take_failed_tasks(&mut self) -> Vec<MigrationTask> {
        let tasks: Vec<MigrationTask> = self.active.iter()
            .filter(|p| p.state == MigrationState::Failed)
            .map(|p| MigrationTask {
                shard: p.shard,
                from_node: p.from_node,
                to_node: p.to_node,
                is_master: p.is_master,
            })
            .collect();
        for t in &tasks {
            self.retry_failed(t);
        }
        tasks
    }

    /// Find a migration progress entry by full task identity.
    pub fn find_task_mut(&mut self, task: &MigrationTask) -> Option<&mut MigrationProgress> {
        self.active.iter_mut().find(|p| {
            p.shard == task.shard && p.from_node == task.from_node && p.to_node == task.to_node
        })
    }

    /// Remove completed and failed migrations from the active list.
    ///
    /// Outbound migrations in the Complete or Failed state are removed.
    /// Failed migrations have already had their shards rolled back, fences
    /// lifted, and bitmaps cleared — removing them just frees the tracking
    /// entry so `active_count()` and the HTTP status endpoint stay accurate.
    ///
    /// Inbound migrations marked as completed are also removed.
    /// Inbound and outbound tracking are independent — completing outbound
    /// work does NOT clear pending inbound migrations (which may still be
    /// receiving data from other nodes).
    pub fn cleanup_completed(&mut self) {
        // Collect shards that had fenced tasks being removed, so we can
        // unfence them if no remaining active task is still fenced.
        let mut maybe_unfence: Vec<u16> = Vec::new();
        for p in &self.active {
            if (p.is_complete() || p.state == MigrationState::Failed)
                && self.fenced_shards.test(p.shard)
            {
                maybe_unfence.push(p.shard);
            }
        }

        self.active.retain(|p| {
            !p.is_complete() && p.state != MigrationState::Failed
        });

        // Unfence shards that no longer have any fenced task.
        for shard in maybe_unfence {
            let still_fenced = self.active.iter().any(|p| {
                p.shard == shard && p.state == MigrationState::Fenced
            });
            if !still_fenced {
                self.unfence_shard(shard);
            }
        }

        self.inbound_migrations.retain(|m| !m.completed);
        // Rebuild inbound bitmap from remaining entries.
        self.inbound_bitmap.clear_all();
        for m in &self.inbound_migrations {
            self.inbound_bitmap.set(m.shard);
        }
    }

    /// Check if a shard is currently being migrated outbound.
    pub fn is_migrating_shard(&self, shard: u16) -> bool {
        self.active
            .iter()
            .any(|p| p.shard == shard && !p.is_complete() && p.state != MigrationState::Failed)
    }

    /// Number of in-progress migrations (excludes Complete and Failed).
    pub fn active_count(&self) -> usize {
        self.active.iter().filter(|p| {
            !p.is_complete() && p.state != MigrationState::Failed
        }).count()
    }

    /// Get all active migrations.
    pub fn active_migrations(&self) -> &[MigrationProgress] {
        &self.active
    }

    /// Number of shards pending inbound data.
    pub fn inbound_count(&self) -> usize {
        self.inbound_migrations.iter().filter(|m| !m.completed).count()
    }

    /// Number of shards with active write fences.
    pub fn fenced_count(&self) -> usize {
        self.fenced_shards.count()
    }

    /// Read-only access to the fenced-shards bitmap.
    pub fn fenced_bitmap(&self) -> &ShardBitmap {
        &self.fenced_shards
    }

    /// Read-only access to the inbound-migration bitmap.
    pub fn inbound_bitmap(&self) -> &ShardBitmap {
        &self.inbound_bitmap
    }

    /// Serialize pending (non-completed) inbound migrations to bytes.
    ///
    /// Format: `[count:4][shard:2 + from_node:8] × count`.
    /// Only pending entries are persisted — completed ones are omitted.
    pub fn serialize_inbound(&self) -> Vec<u8> {
        let pending: Vec<_> = self.inbound_migrations.iter()
            .filter(|m| !m.completed)
            .collect();
        let mut buf = Vec::with_capacity(4 + pending.len() * 10);
        buf.extend_from_slice(&(pending.len() as u32).to_le_bytes());
        for m in &pending {
            buf.extend_from_slice(&m.shard.to_le_bytes());
            buf.extend_from_slice(&m.from_node.0.to_le_bytes());
        }
        buf
    }

    /// Restore inbound migrations from bytes produced by `serialize_inbound`.
    ///
    /// Entries restored this way start as non-completed, so the node will
    /// refuse writes for these shards until migration completes or is
    /// explicitly cleared.
    pub fn restore_inbound(&mut self, data: &[u8]) {
        if data.len() < 4 {
            return;
        }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4])) as usize;
        let mut pos = 4;
        for _ in 0..count {
            if pos + 10 > data.len() {
                break;
            }
            let shard = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap_or([0; 2]));
            let from_node = NodeId(u64::from_le_bytes(
                data[pos + 2..pos + 10].try_into().unwrap_or([0; 8]),
            ));
            pos += 10;
            // Only add if not already present.
            if !self.inbound_migrations.iter().any(|m| m.shard == shard && m.from_node == from_node) {
                self.inbound_migrations.push(InboundMigration {
                    shard,
                    from_node,
                    completed: false,
                    created_at: std::time::Instant::now(),
                });
                self.inbound_bitmap.set(shard);
            }
        }
    }

    /// Clear all inbound migrations (used when the topology changes and
    /// supersedes any in-flight migrations).
    pub fn clear_inbound(&mut self) {
        self.inbound_migrations.clear();
        self.inbound_bitmap.clear_all();
    }

    /// Remove inbound migrations older than `max_age`.
    ///
    /// Unlike [`clear_inbound`] which removes everything, this only evicts
    /// entries that have been pending longer than the threshold. This
    /// prevents indefinite write-blocking from abandoned migrations while
    /// preserving protection for shards still actively receiving data.
    ///
    /// Returns the number of entries removed.
    pub fn clear_stale_inbound(&mut self, max_age: std::time::Duration) -> usize {
        let before = self.inbound_migrations.len();
        self.inbound_migrations.retain(|m| {
            !m.completed && m.created_at.elapsed() < max_age
        });
        let removed = before - self.inbound_migrations.len();
        if removed > 0 {
            // Rebuild bitmap from surviving entries.
            self.inbound_bitmap.clear_all();
            for m in &self.inbound_migrations {
                if !m.completed {
                    self.inbound_bitmap.set(m.shard);
                }
            }
        }
        removed
    }

    /// Serialize active outbound migration state to bytes.
    ///
    /// Format: `[count:4][ shard:2 + from_node:8 + to_node:8 + is_master:1
    ///   + state:1 + snapshot_seq:8 + fence_seq:8 ] × count`
    ///
    /// Per-entry size: 36 bytes. Only non-complete, non-failed entries
    /// are persisted — on restart these indicate migrations that were
    /// interrupted and may need to be re-initiated.
    pub fn serialize_outbound(&self) -> Vec<u8> {
        let active: Vec<_> = self.active.iter()
            .filter(|p| !p.is_complete() && p.state != MigrationState::Failed)
            .collect();
        let mut buf = Vec::with_capacity(4 + active.len() * 36);
        buf.extend_from_slice(&(active.len() as u32).to_le_bytes());
        for p in &active {
            buf.extend_from_slice(&p.shard.to_le_bytes());
            buf.extend_from_slice(&p.from_node.0.to_le_bytes());
            buf.extend_from_slice(&p.to_node.0.to_le_bytes());
            buf.push(if p.is_master { 1 } else { 0 });
            let state_byte: u8 = match p.state {
                MigrationState::Preparing => 0,
                MigrationState::Streaming => 1,
                MigrationState::Fenced => 2,
                MigrationState::Complete => 3,
                MigrationState::Failed => 4,
            };
            buf.push(state_byte);
            buf.extend_from_slice(&p.snapshot_sequence.to_le_bytes());
            buf.extend_from_slice(&p.fence_sequence.to_le_bytes());
        }
        buf
    }

    /// Restore outbound migration state from bytes produced by `serialize_outbound`.
    ///
    /// Restored entries start in the state they were serialized with.
    /// The coordinator can inspect these on startup to decide whether
    /// to resume, abort, or re-plan each migration.
    pub fn restore_outbound(&mut self, data: &[u8]) {
        if data.len() < 4 {
            return;
        }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4])) as usize;
        let mut pos = 4;
        for _ in 0..count {
            if pos + 36 > data.len() {
                break;
            }
            let shard = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap_or([0; 2]));
            let from_node = NodeId(u64::from_le_bytes(data[pos + 2..pos + 10].try_into().unwrap_or([0; 8])));
            let to_node = NodeId(u64::from_le_bytes(data[pos + 10..pos + 18].try_into().unwrap_or([0; 8])));
            let is_master = data[pos + 18] != 0;
            let state = match data[pos + 19] {
                0 => MigrationState::Preparing,
                1 => MigrationState::Streaming,
                2 => MigrationState::Fenced,
                3 => MigrationState::Complete,
                _ => MigrationState::Failed,
            };
            let snapshot_sequence = u64::from_le_bytes(data[pos + 20..pos + 28].try_into().unwrap_or([0; 8]));
            let fence_sequence = u64::from_le_bytes(data[pos + 28..pos + 36].try_into().unwrap_or([0; 8]));
            pos += 36;

            let task = MigrationTask { shard, from_node, to_node, is_master };
            // Only add if not already tracked.
            if self.find_task_mut(&task).is_none() {
                let mut progress = MigrationProgress::from_task(&task);
                progress.state = state;
                progress.snapshot_sequence = snapshot_sequence;
                progress.fence_sequence = fence_sequence;
                self.active.push(progress);
            }
        }
    }
}

/// Persist inbound migration state to disk (atomic write via temp + rename).
///
/// Best-effort: errors are logged but do not propagate. On restart the
/// node will refuse writes for these shards until migration completes.
pub fn persist_inbound_state(path: &std::path::Path, mgr: &MigrationManager) {
    let data = mgr.serialize_inbound();
    let tmp = path.with_extension("inbound.tmp");
    let result = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        std::io::Write::write_all(&mut f, &data)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    })();
    if let Err(e) = result {
        eprintln!("cluster: failed to persist inbound migration state: {e}");
    }
}

/// Load inbound migration state from disk.
///
/// Returns the raw bytes for `MigrationManager::restore_inbound()`.
/// Returns an empty Vec if the file doesn't exist or is corrupted.
pub fn load_inbound_state(path: &std::path::Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

/// Persist outbound migration state to disk (atomic write via temp + rename).
///
/// Best-effort: errors are logged but do not propagate. On restart the
/// node can inspect persisted outbound state to determine which
/// migrations were in-flight and need re-planning.
pub fn persist_outbound_state(path: &std::path::Path, mgr: &MigrationManager) {
    let data = mgr.serialize_outbound();
    let tmp = path.with_extension("outbound.tmp");
    let result = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        std::io::Write::write_all(&mut f, &data)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    })();
    if let Err(e) = result {
        eprintln!("cluster: failed to persist outbound migration state: {e}");
    }
}

/// Load outbound migration state from disk.
///
/// Returns the raw bytes for `MigrationManager::restore_outbound()`.
/// Returns an empty Vec if the file doesn't exist or is corrupted.
pub fn load_outbound_state(path: &std::path::Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn shard_bitmap_set_clear_test() {
        let mut bm = ShardBitmap::new();
        assert!(!bm.test(0));
        assert!(!bm.test(4095));
        assert_eq!(bm.count(), 0);

        bm.set(0);
        bm.set(63);
        bm.set(64);
        bm.set(4095);
        assert!(bm.test(0));
        assert!(bm.test(63));
        assert!(bm.test(64));
        assert!(bm.test(4095));
        assert!(!bm.test(1));
        assert_eq!(bm.count(), 4);

        bm.clear(63);
        assert!(!bm.test(63));
        assert_eq!(bm.count(), 3);

        bm.clear_all();
        assert_eq!(bm.count(), 0);
        assert!(!bm.test(0));
    }

    #[test]
    fn start_outbound_filters_by_self() {
        let mut mgr = MigrationManager::new();
        let tasks = vec![
            MigrationTask { shard: 0, from_node: NodeId(1), to_node: NodeId(2), is_master: true },
            MigrationTask { shard: 1, from_node: NodeId(2), to_node: NodeId(1), is_master: true },
            MigrationTask { shard: 2, from_node: NodeId(1), to_node: NodeId(3), is_master: true },
        ];

        mgr.start_outbound(&tasks, NodeId(1), &std::collections::HashSet::new());
        assert_eq!(mgr.active_count(), 2); // Only shards 0 and 2
    }

    #[test]
    fn progress_tracking() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask { shard: 5, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[task.clone()], NodeId(1), &std::collections::HashSet::new());

        let p = &mgr.active_migrations()[0];
        assert_eq!(p.state, MigrationState::Preparing);

        mgr.set_snapshot_sequence(&task, 100);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Streaming);

        mgr.record_progress(&task, 50, 50_000);
        mgr.mark_fenced(&task, 200);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Fenced);
        assert_eq!(mgr.active_migrations()[0].migrated_records, 50);
        assert!(mgr.is_shard_fenced(5));

        mgr.mark_complete(&task);
        assert!(mgr.active_migrations()[0].is_complete());
        assert!(!mgr.is_shard_fenced(5)); // fence lifted on complete

        mgr.cleanup_completed();
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn empty_migration() {
        let task = MigrationTask { shard: 0, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        let progress = MigrationProgress::from_task(&task);
        assert_eq!(progress.fraction_complete(), 1.0); // 0 total → 100%
    }

    #[test]
    fn failed_migration_cleaned_up_by_cleanup() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask { shard: 3, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[task.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.mark_failed(&task);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Failed);
        assert_eq!(mgr.active_count(), 0); // active_count excludes Failed
        assert_eq!(mgr.failed_count(), 1); // but failed_count tracks them

        // cleanup_completed removes both Complete and Failed migrations.
        mgr.cleanup_completed();
        assert_eq!(mgr.active_count(), 0);
        assert_eq!(mgr.failed_count(), 0);
        assert!(mgr.active_migrations().is_empty());
    }

    #[test]
    fn full_lifecycle_preparing_to_complete() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask { shard: 7, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[task.clone()], NodeId(1), &std::collections::HashSet::new());

        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Preparing);

        mgr.set_snapshot_sequence(&task, 50);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Streaming);

        mgr.mark_fenced(&task, 75);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Fenced);
        assert!(mgr.is_shard_fenced(7));

        mgr.mark_complete(&task);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Complete);
        assert!(mgr.active_migrations()[0].is_complete());
        assert!(!mgr.is_shard_fenced(7));

        mgr.cleanup_completed();
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn different_tasks_same_shard_tracked_independently() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 5, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        let t2 = MigrationTask { shard: 5, from_node: NodeId(1), to_node: NodeId(3), is_master: false };
        mgr.start_outbound(&[t1.clone(), t2.clone()], NodeId(1), &std::collections::HashSet::new());
        assert_eq!(mgr.active_count(), 2);

        mgr.mark_complete(&t1);
        assert_eq!(mgr.active_count(), 1);
        // t2 should still be in preparing state
        assert_eq!(mgr.active_migrations().iter()
            .find(|p| p.to_node == NodeId(3)).unwrap().state, MigrationState::Preparing);

        mgr.mark_complete(&t2);
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn cleanup_does_not_clear_active_inbound() {
        let mut mgr = MigrationManager::new();
        // Node 1 sends shard 10 to node 3 (outbound for node 1).
        // Node 2 sends shard 5 to node 1 (inbound for node 1).
        let outbound = MigrationTask { shard: 10, from_node: NodeId(1), to_node: NodeId(3), is_master: true };
        let inbound = MigrationTask { shard: 5, from_node: NodeId(2), to_node: NodeId(1), is_master: true };

        let mut populated = std::collections::HashSet::new();
        populated.insert(5);
        mgr.start_outbound(&[outbound.clone(), inbound.clone()], NodeId(1), &populated);

        // Node 1 has one outbound task and one inbound migration.
        assert_eq!(mgr.active_count(), 1); // outbound only
        assert!(mgr.has_pending_inbound(5));

        // Complete the outbound migration.
        mgr.mark_complete(&outbound);
        mgr.cleanup_completed();

        // The inbound shard 5 must still be protected.
        assert!(mgr.has_pending_inbound(5));
        assert_eq!(mgr.inbound_count(), 1);

        // Now mark the inbound as complete.
        mgr.mark_inbound_complete(5);
        assert!(!mgr.has_pending_inbound(5));

        mgr.cleanup_completed();
        assert_eq!(mgr.inbound_count(), 0);
    }

    #[test]
    fn mark_inbound_active_creates_entry() {
        let mut mgr = MigrationManager::new();
        assert!(!mgr.has_pending_inbound(42));

        mgr.mark_inbound_active(42);
        assert!(mgr.has_pending_inbound(42));
        assert_eq!(mgr.inbound_count(), 1);

        // Duplicate call should not create a second entry.
        mgr.mark_inbound_active(42);
        assert_eq!(mgr.inbound_count(), 1);

        mgr.mark_inbound_complete(42);
        assert!(!mgr.has_pending_inbound(42));
    }

    #[test]
    fn inbound_tracking_per_task() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 5, from_node: NodeId(2), to_node: NodeId(1), is_master: true };
        let t2 = MigrationTask { shard: 5, from_node: NodeId(3), to_node: NodeId(1), is_master: false };

        let mut populated = std::collections::HashSet::new();
        populated.insert(5);
        mgr.start_outbound(&[t1.clone(), t2.clone()], NodeId(1), &populated);

        // Two inbound entries for the same shard from different sources.
        assert_eq!(mgr.inbound_count(), 2);
        assert!(mgr.has_pending_inbound(5));

        // Complete one — shard should still be pending.
        mgr.mark_inbound_complete(5);
        assert!(mgr.has_pending_inbound(5));
        assert_eq!(mgr.inbound_count(), 1);

        // Complete the second — now clear.
        mgr.mark_inbound_complete(5);
        assert!(!mgr.has_pending_inbound(5));
    }

    #[test]
    fn serialize_restore_inbound_round_trip() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(10);
        let t = MigrationTask { shard: 20, from_node: NodeId(5), to_node: NodeId(1), is_master: true };
        let mut populated = std::collections::HashSet::new();
        populated.insert(20);
        mgr.start_outbound(&[t], NodeId(1), &populated);

        // Mark shard 10's migration complete — should NOT be serialized.
        mgr.mark_inbound_complete(10);

        let data = mgr.serialize_inbound();
        let mut restored = MigrationManager::new();
        restored.restore_inbound(&data);

        // Only shard 20 (pending) should be restored.
        assert!(!restored.has_pending_inbound(10));
        assert!(restored.has_pending_inbound(20));
        assert_eq!(restored.inbound_count(), 1);
    }

    #[test]
    fn restore_inbound_empty_data() {
        let mut mgr = MigrationManager::new();
        mgr.restore_inbound(&[]);
        assert_eq!(mgr.inbound_count(), 0);

        mgr.restore_inbound(&[0, 0, 0, 0]); // count = 0
        assert_eq!(mgr.inbound_count(), 0);
    }

    #[test]
    fn restore_inbound_truncated_data() {
        let mut mgr = MigrationManager::new();
        // count=2 but only 1 entry's worth of data (10 bytes).
        let mut data = Vec::new();
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&42u16.to_le_bytes());
        data.extend_from_slice(&7u64.to_le_bytes());
        mgr.restore_inbound(&data);
        // Should restore 1 entry, ignore the truncated second.
        assert_eq!(mgr.inbound_count(), 1);
        assert!(mgr.has_pending_inbound(42));
    }

    #[test]
    fn restore_inbound_no_duplicates() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(42);

        // Serialize (shard 42, from_node 0 sentinel).
        let data = mgr.serialize_inbound();

        // Restore onto the same manager — should not duplicate.
        mgr.restore_inbound(&data);
        assert_eq!(mgr.inbound_count(), 1);
    }

    #[test]
    fn persist_and_load_inbound_state() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("inbound.state");

        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(100);
        mgr.mark_inbound_active(200);

        super::persist_inbound_state(&path, &mgr);

        let data = super::load_inbound_state(&path);
        let mut restored = MigrationManager::new();
        restored.restore_inbound(&data);
        assert!(restored.has_pending_inbound(100));
        assert!(restored.has_pending_inbound(200));
        assert_eq!(restored.inbound_count(), 2);
    }

    #[test]
    fn load_inbound_state_missing_file() {
        let data = super::load_inbound_state(std::path::Path::new("/nonexistent/path"));
        assert!(data.is_empty());
    }

    #[test]
    fn clear_inbound_removes_all() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(10);
        mgr.mark_inbound_active(20);
        assert_eq!(mgr.inbound_count(), 2);

        mgr.clear_inbound();
        assert_eq!(mgr.inbound_count(), 0);
        assert!(!mgr.has_pending_inbound(10));
        assert!(!mgr.has_pending_inbound(20));
    }

    #[test]
    fn serialize_restore_outbound_round_trip() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 5, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        let t2 = MigrationTask { shard: 10, from_node: NodeId(1), to_node: NodeId(3), is_master: false };
        mgr.start_outbound(&[t1.clone(), t2.clone()], NodeId(1), &std::collections::HashSet::new());

        // Advance t1 to Streaming with a snapshot sequence.
        mgr.set_snapshot_sequence(&t1, 42);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Streaming);

        let data = mgr.serialize_outbound();
        let mut restored = MigrationManager::new();
        restored.restore_outbound(&data);

        // Both tasks should be restored.
        assert_eq!(restored.active_count(), 2);
        let p1 = restored.active_migrations().iter()
            .find(|p| p.shard == 5).expect("shard 5 restored");
        assert_eq!(p1.state, MigrationState::Streaming);
        assert_eq!(p1.snapshot_sequence, 42);
        assert!(p1.is_master);
        let p2 = restored.active_migrations().iter()
            .find(|p| p.shard == 10).expect("shard 10 restored");
        assert!(!p2.is_master);
    }

    #[test]
    fn serialize_outbound_skips_complete_and_failed() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 1, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        let t2 = MigrationTask { shard: 2, from_node: NodeId(1), to_node: NodeId(3), is_master: true };
        let t3 = MigrationTask { shard: 3, from_node: NodeId(1), to_node: NodeId(4), is_master: true };
        mgr.start_outbound(&[t1.clone(), t2.clone(), t3.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.mark_complete(&t1);
        mgr.mark_failed(&t2);

        let data = mgr.serialize_outbound();
        let mut restored = MigrationManager::new();
        restored.restore_outbound(&data);

        // Only t3 (Preparing) should be restored.
        assert_eq!(restored.active_count(), 1);
        assert_eq!(restored.active_migrations()[0].shard, 3);
    }

    #[test]
    fn persist_and_load_outbound_state() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("outbound.state");

        let mut mgr = MigrationManager::new();
        let t = MigrationTask { shard: 42, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[t.clone()], NodeId(1), &std::collections::HashSet::new());
        mgr.set_snapshot_sequence(&t, 100);

        super::persist_outbound_state(&path, &mgr);

        let data = super::load_outbound_state(&path);
        let mut restored = MigrationManager::new();
        restored.restore_outbound(&data);
        assert_eq!(restored.active_count(), 1);
        assert_eq!(restored.active_migrations()[0].snapshot_sequence, 100);
    }

    #[test]
    fn restore_outbound_empty_data() {
        let mut mgr = MigrationManager::new();
        mgr.restore_outbound(&[]);
        assert_eq!(mgr.active_count(), 0);

        mgr.restore_outbound(&[0, 0, 0, 0]); // count = 0
        assert_eq!(mgr.active_count(), 0);
    }

    // ---------- Bug fix regression tests ----------

    /// Verify that cleanup_completed removes Failed migrations, preventing
    /// them from accumulating indefinitely in the active list.
    /// Regression: Failed migrations previously stayed forever because
    /// cleanup_completed only removed Complete entries.
    #[test]
    fn cleanup_removes_failed_migrations() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 1, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        let t2 = MigrationTask { shard: 2, from_node: NodeId(1), to_node: NodeId(3), is_master: true };
        let t3 = MigrationTask { shard: 3, from_node: NodeId(1), to_node: NodeId(4), is_master: true };
        mgr.start_outbound(&[t1.clone(), t2.clone(), t3.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.mark_complete(&t1);
        mgr.mark_failed(&t2);
        // t3 still Preparing

        assert_eq!(mgr.active_count(), 1); // only t3
        assert_eq!(mgr.failed_count(), 1); // t2
        assert_eq!(mgr.active_migrations().len(), 3); // all tracked

        mgr.cleanup_completed();

        // After cleanup: only t3 (Preparing) remains
        assert_eq!(mgr.active_migrations().len(), 1);
        assert_eq!(mgr.active_migrations()[0].shard, 3);
        assert_eq!(mgr.active_count(), 1);
        assert_eq!(mgr.failed_count(), 0);
    }

    /// Verify that multiple Failed migrations are all cleaned up, not just the first.
    #[test]
    fn cleanup_removes_all_failed_migrations() {
        let mut mgr = MigrationManager::new();
        let tasks: Vec<MigrationTask> = (0..5).map(|i| {
            MigrationTask { shard: i, from_node: NodeId(1), to_node: NodeId(2), is_master: true }
        }).collect();
        mgr.start_outbound(&tasks, NodeId(1), &std::collections::HashSet::new());

        for t in &tasks {
            mgr.mark_failed(t);
        }
        assert_eq!(mgr.failed_count(), 5);
        assert_eq!(mgr.active_count(), 0);

        mgr.cleanup_completed();

        assert!(mgr.active_migrations().is_empty());
        assert_eq!(mgr.failed_count(), 0);
        assert_eq!(mgr.active_count(), 0);
    }

    /// Verify that active_count correctly excludes Failed and Complete,
    /// matching what the HTTP endpoint should report.
    #[test]
    fn active_count_matches_http_expectation() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 1, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        let t2 = MigrationTask { shard: 2, from_node: NodeId(1), to_node: NodeId(3), is_master: true };
        let t3 = MigrationTask { shard: 3, from_node: NodeId(1), to_node: NodeId(4), is_master: true };
        let t4 = MigrationTask { shard: 4, from_node: NodeId(1), to_node: NodeId(5), is_master: true };
        mgr.start_outbound(&[t1.clone(), t2.clone(), t3.clone(), t4.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.mark_complete(&t1);
        mgr.mark_failed(&t2);
        mgr.set_snapshot_sequence(&t3, 100); // Streaming
        // t4 still Preparing

        // active_count should only count Preparing + Streaming + Fenced
        assert_eq!(mgr.active_count(), 2); // t3 (Streaming) + t4 (Preparing)
        // The HTTP endpoint should report the same
        let all = mgr.active_migrations();
        let http_active = all.iter().filter(|m| {
            m.state != MigrationState::Complete && m.state != MigrationState::Failed
        }).count();
        assert_eq!(http_active, mgr.active_count());
    }

    /// Verify that take_failed_tasks works correctly before cleanup runs.
    /// This is the retry path used on NodeJoined events.
    #[test]
    fn take_failed_tasks_before_cleanup() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 1, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        let t2 = MigrationTask { shard: 2, from_node: NodeId(1), to_node: NodeId(3), is_master: true };
        mgr.start_outbound(&[t1.clone(), t2.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.mark_failed(&t1);
        mgr.mark_failed(&t2);

        // take_failed_tasks should return the tasks and reset them to Streaming.
        let retries = mgr.take_failed_tasks();
        assert_eq!(retries.len(), 2);
        assert_eq!(mgr.failed_count(), 0);
        assert_eq!(mgr.active_count(), 2); // now Streaming again
    }

    /// Verify that mark_failed lifts the write fence.
    #[test]
    fn mark_failed_lifts_fence() {
        let mut mgr = MigrationManager::new();
        let t = MigrationTask { shard: 42, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[t.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.mark_fenced(&t, 100);
        assert!(mgr.is_shard_fenced(42));

        mgr.mark_failed(&t);
        assert!(!mgr.is_shard_fenced(42));
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Failed);
    }

    // -----------------------------------------------------------------------
    // Part 4: Migration edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn fence_and_complete_lifecycle() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask { shard: 42, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[task.clone()], NodeId(1), &std::collections::HashSet::new());

        // Full lifecycle: Preparing → Streaming → Fenced → Complete
        mgr.set_snapshot_sequence(&task, 100);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Streaming);
        assert_eq!(mgr.active_migrations()[0].snapshot_sequence, 100);

        mgr.mark_fenced(&task, 200);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Fenced);
        assert_eq!(mgr.active_migrations()[0].fence_sequence, 200);
        assert!(mgr.is_shard_fenced(42));

        mgr.mark_complete(&task);
        assert!(!mgr.is_shard_fenced(42), "fence should be lifted on complete");
        assert!(mgr.active_migrations()[0].is_complete());
    }

    #[test]
    fn inbound_bitmap_consistency_after_cleanup() {
        let mut mgr = MigrationManager::new();

        // Register several inbound migrations
        mgr.mark_inbound_active(10);
        mgr.mark_inbound_active(20);
        mgr.mark_inbound_active(30);

        // Complete one
        mgr.mark_inbound_complete(20);

        // Cleanup
        mgr.cleanup_completed();

        // Bitmap should accurately reflect remaining state
        assert!(mgr.has_pending_inbound(10));
        assert!(!mgr.has_pending_inbound(20), "completed shard should be cleared");
        assert!(mgr.has_pending_inbound(30));
        assert_eq!(mgr.inbound_count(), 2);
    }

    #[test]
    fn failed_migration_retry_resets_progress() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask { shard: 5, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[task.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.set_snapshot_sequence(&task, 100);
        mgr.record_progress(&task, 500, 500_000);
        mgr.mark_failed(&task);

        // Retry should reset progress
        let retried = mgr.retry_failed(&task);
        assert!(retried);
        let p = mgr.find_task_mut(&task).unwrap();
        assert_eq!(p.state, MigrationState::Streaming);
        assert_eq!(p.migrated_records, 0);
        assert_eq!(p.bytes_sent, 0);
    }

    #[test]
    fn atomic_shard_bitmap_concurrent_ops() {
        let bitmap = AtomicShardBitmap::new();

        // Set from multiple threads
        let bitmap_ref = &bitmap;
        std::thread::scope(|s| {
            for shard in 0..100u16 {
                s.spawn(move || {
                    bitmap_ref.set(shard);
                });
            }
        });

        // All 100 should be set
        for shard in 0..100u16 {
            assert!(bitmap.test(shard), "shard {shard} should be set");
        }
        for shard in 100..NUM_SHARDS as u16 {
            assert!(!bitmap.test(shard), "shard {shard} should not be set");
        }

        // Clear from multiple threads
        std::thread::scope(|s| {
            for shard in 0..100u16 {
                s.spawn(move || {
                    bitmap_ref.clear(shard);
                });
            }
        });

        for shard in 0..NUM_SHARDS as u16 {
            assert!(!bitmap.test(shard));
        }
    }

    #[test]
    fn load_from_bitmap_snapshot() {
        let mut source = ShardBitmap::new();
        source.set(0);
        source.set(42);
        source.set(4095);

        let atomic = AtomicShardBitmap::new();
        atomic.load_from(&source);

        assert!(atomic.test(0));
        assert!(atomic.test(42));
        assert!(atomic.test(4095));
        assert!(!atomic.test(1));
        assert!(!atomic.test(100));
    }

    /// Verify that is_migrating_shard excludes Failed migrations.
    /// A failed migration should NOT block new migrations for the same shard.
    #[test]
    fn is_migrating_shard_excludes_failed() {
        let mut mgr = MigrationManager::new();
        let t = MigrationTask { shard: 7, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[t.clone()], NodeId(1), &std::collections::HashSet::new());

        assert!(mgr.is_migrating_shard(7));
        mgr.mark_failed(&t);
        assert!(!mgr.is_migrating_shard(7));
    }

    // -----------------------------------------------------------------------
    // Fence bitmap: conditional unfencing with multiple tasks per shard
    // -----------------------------------------------------------------------

    /// Two tasks for the same shard both fenced: completing one keeps the
    /// shard fenced because the other task is still in the Fenced state.
    /// The shard only unfences once ALL fenced tasks are complete/failed.
    #[test]
    fn two_fenced_tasks_same_shard_complete_one_keeps_fence() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 5, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        let t2 = MigrationTask { shard: 5, from_node: NodeId(1), to_node: NodeId(3), is_master: false };
        mgr.start_outbound(&[t1.clone(), t2.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.mark_fenced(&t1, 100);
        mgr.mark_fenced(&t2, 200);
        assert!(mgr.is_shard_fenced(5));

        // Complete t1 → shard 5 STAYS fenced because t2 is still Fenced.
        mgr.mark_complete(&t1);
        assert!(mgr.is_shard_fenced(5),
            "shard should remain fenced while another task is in Fenced state");

        // t2 is still tracked as Fenced in its progress entry.
        let t2_progress = mgr.active_migrations().iter()
            .find(|p| p.to_node == NodeId(3))
            .expect("t2 should still be active");
        assert_eq!(t2_progress.state, MigrationState::Fenced);

        // Complete t2 → NOW the shard is unfenced.
        mgr.mark_complete(&t2);
        assert!(!mgr.is_shard_fenced(5),
            "shard should unfence once all fenced tasks are done");
    }

    /// mark_complete and mark_failed always call unfence_shard regardless
    /// of whether find_task_mut found the task. This means calling
    /// mark_complete on a task that was already cleaned up still unfences.
    #[test]
    fn mark_complete_unfences_even_when_task_not_found() {
        let mut mgr = MigrationManager::new();

        // Fence a shard manually without registering a task.
        mgr.fence_shard(99);
        assert!(mgr.is_shard_fenced(99));

        // mark_complete with a non-existent task → find_task_mut returns None,
        // but unfence_shard(99) still runs.
        let phantom = MigrationTask { shard: 99, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.mark_complete(&phantom);
        assert!(!mgr.is_shard_fenced(99));
    }

    /// mark_failed also unfences even when the task isn't found.
    #[test]
    fn mark_failed_unfences_even_when_task_not_found() {
        let mut mgr = MigrationManager::new();
        mgr.fence_shard(42);
        assert!(mgr.is_shard_fenced(42));

        let phantom = MigrationTask { shard: 42, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.mark_failed(&phantom);
        assert!(!mgr.is_shard_fenced(42));
    }

    // -----------------------------------------------------------------------
    // Deep edge cases: inbound tracking precision
    // -----------------------------------------------------------------------

    /// Inbound tracking with multiple sources for the same shard: each
    /// source must be independently completable.
    #[test]
    fn inbound_multiple_sources_independent_completion() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 10, from_node: NodeId(2), to_node: NodeId(1), is_master: true };
        let t2 = MigrationTask { shard: 10, from_node: NodeId(3), to_node: NodeId(1), is_master: false };

        let mut pop = std::collections::HashSet::new();
        pop.insert(10);
        mgr.start_outbound(&[t1, t2], NodeId(1), &pop);

        // Two inbound entries for shard 10.
        assert_eq!(mgr.inbound_count(), 2);
        assert!(mgr.has_pending_inbound(10));

        // Complete one source.
        mgr.mark_inbound_complete(10);
        assert_eq!(mgr.inbound_count(), 1);
        assert!(mgr.has_pending_inbound(10), "shard still has one pending source");

        // Complete the second source.
        mgr.mark_inbound_complete(10);
        assert_eq!(mgr.inbound_count(), 0);
        assert!(!mgr.has_pending_inbound(10));
    }

    /// start_outbound skips inbound registration for empty shards.
    #[test]
    fn start_outbound_skips_empty_shards_for_inbound() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 10, from_node: NodeId(2), to_node: NodeId(1), is_master: true };
        let t2 = MigrationTask { shard: 20, from_node: NodeId(2), to_node: NodeId(1), is_master: true };

        // Only shard 10 is populated.
        let mut pop = std::collections::HashSet::new();
        pop.insert(10);
        mgr.start_outbound(&[t1, t2], NodeId(1), &pop);

        // Only shard 10 should be registered as inbound.
        assert!(mgr.has_pending_inbound(10));
        assert!(!mgr.has_pending_inbound(20), "empty shard 20 should not be inbound");
        assert_eq!(mgr.inbound_count(), 1);
    }

    /// Outbound serialize/restore round-trip preserves Streaming state
    /// with the correct snapshot_sequence, and skips Fenced tasks (which
    /// ARE serialized — this verifies both are preserved).
    #[test]
    fn outbound_serialize_preserves_all_active_states() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 1, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        let t2 = MigrationTask { shard: 2, from_node: NodeId(1), to_node: NodeId(3), is_master: true };
        let t3 = MigrationTask { shard: 3, from_node: NodeId(1), to_node: NodeId(4), is_master: false };
        mgr.start_outbound(&[t1.clone(), t2.clone(), t3.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.set_snapshot_sequence(&t1, 100); // Streaming
        mgr.mark_fenced(&t2, 200); // Fenced
        // t3 stays Preparing

        let data = mgr.serialize_outbound();
        let mut restored = MigrationManager::new();
        restored.restore_outbound(&data);

        assert_eq!(restored.active_count(), 3);
        let r1 = restored.active_migrations().iter().find(|p| p.shard == 1).unwrap();
        assert_eq!(r1.state, MigrationState::Streaming);
        assert_eq!(r1.snapshot_sequence, 100);

        let r2 = restored.active_migrations().iter().find(|p| p.shard == 2).unwrap();
        assert_eq!(r2.state, MigrationState::Fenced);
        assert_eq!(r2.fence_sequence, 200);

        let r3 = restored.active_migrations().iter().find(|p| p.shard == 3).unwrap();
        assert_eq!(r3.state, MigrationState::Preparing);
        assert!(!r3.is_master);
    }

    /// clear_inbound followed by start_outbound: new inbound registrations
    /// should work correctly after a full clear.
    #[test]
    fn clear_inbound_then_reregister() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(10);
        mgr.mark_inbound_active(20);
        assert_eq!(mgr.inbound_count(), 2);

        mgr.clear_inbound();
        assert_eq!(mgr.inbound_count(), 0);
        assert!(!mgr.has_pending_inbound(10));
        assert!(!mgr.has_pending_inbound(20));

        // Re-register via start_outbound.
        let t = MigrationTask { shard: 10, from_node: NodeId(2), to_node: NodeId(1), is_master: true };
        let mut pop = std::collections::HashSet::new();
        pop.insert(10);
        mgr.start_outbound(&[t], NodeId(1), &pop);
        assert_eq!(mgr.inbound_count(), 1);
        assert!(mgr.has_pending_inbound(10));
    }

    /// AtomicShardBitmap: load_from must completely overwrite the previous
    /// state, not OR with it.
    #[test]
    fn atomic_bitmap_load_from_overwrites() {
        let atomic = AtomicShardBitmap::new();
        atomic.set(0);
        atomic.set(100);
        atomic.set(4095);

        // Create a source bitmap with different bits.
        let mut source = ShardBitmap::new();
        source.set(50);
        source.set(200);

        // load_from should replace, not merge.
        atomic.load_from(&source);
        assert!(!atomic.test(0), "old bit 0 should be cleared");
        assert!(!atomic.test(100), "old bit 100 should be cleared");
        assert!(!atomic.test(4095), "old bit 4095 should be cleared");
        assert!(atomic.test(50), "new bit 50 should be set");
        assert!(atomic.test(200), "new bit 200 should be set");
    }

    // -----------------------------------------------------------------------
    // Staleness-based inbound clear
    // -----------------------------------------------------------------------

    /// Fresh inbound entries survive a staleness clear with a long timeout.
    #[test]
    fn clear_stale_inbound_preserves_recent() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(10);
        mgr.mark_inbound_active(20);

        // Both are fresh — 30s timeout should keep them.
        let removed = mgr.clear_stale_inbound(Duration::from_secs(30));
        assert_eq!(removed, 0);
        assert!(mgr.has_pending_inbound(10));
        assert!(mgr.has_pending_inbound(20));
        assert_eq!(mgr.inbound_count(), 2);
    }

    /// All entries are removed when max_age is zero (everything is stale).
    #[test]
    fn clear_stale_inbound_removes_old() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(10);
        mgr.mark_inbound_active(20);

        // Duration::ZERO means everything is stale.
        let removed = mgr.clear_stale_inbound(Duration::ZERO);
        assert_eq!(removed, 2);
        assert!(!mgr.has_pending_inbound(10));
        assert!(!mgr.has_pending_inbound(20));
        assert_eq!(mgr.inbound_count(), 0);
    }

    /// Completed entries are also cleared by staleness sweep (retain
    /// condition requires !completed AND young enough).
    #[test]
    fn clear_stale_inbound_removes_completed() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(10);
        mgr.mark_inbound_active(20);
        mgr.mark_inbound_complete(10);

        // Even with a long timeout, completed entries are removed.
        let removed = mgr.clear_stale_inbound(Duration::from_secs(3600));
        assert_eq!(removed, 1);
        assert!(!mgr.has_pending_inbound(10));
        assert!(mgr.has_pending_inbound(20));
    }

    /// Bitmap is correctly rebuilt after partial staleness clear.
    #[test]
    fn clear_stale_inbound_bitmap_consistency() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(10);
        mgr.mark_inbound_active(20);
        mgr.mark_inbound_active(30);

        // Remove shard 10 by completing it, then clear stale.
        mgr.mark_inbound_complete(10);
        let removed = mgr.clear_stale_inbound(Duration::from_secs(3600));
        assert_eq!(removed, 1); // shard 10 (completed)

        // Remaining: shards 20 and 30.
        assert!(!mgr.has_pending_inbound(10));
        assert!(mgr.has_pending_inbound(20));
        assert!(mgr.has_pending_inbound(30));
        assert_eq!(mgr.inbound_count(), 2);
    }

    // -----------------------------------------------------------------------
    // cleanup_completed must unfence shards with no remaining fenced tasks
    // -----------------------------------------------------------------------

    /// Two fenced tasks for the same shard, both completed. After
    /// cleanup_completed removes them, the shard must be unfenced.
    /// Without the fix, the shard stays permanently fenced because
    /// mark_complete on task A defers unfencing (task B is still Fenced),
    /// then mark_complete on task B also defers (task A is now Complete,
    /// not Fenced — so has_other_fenced_task returns false and unfences).
    /// But if cleanup_completed runs between the two mark_complete calls,
    /// it removes task A before task B is completed, leaving task B's
    /// mark_complete to correctly unfence. The dangerous case is when
    /// both are completed before cleanup runs: cleanup removes both,
    /// and no one unfences.
    #[test]
    fn cleanup_completed_unfences_shard_with_no_remaining_fenced_tasks() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask { shard: 5, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        let t2 = MigrationTask { shard: 5, from_node: NodeId(1), to_node: NodeId(3), is_master: false };
        mgr.start_outbound(&[t1.clone(), t2.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.mark_fenced(&t1, 100);
        mgr.mark_fenced(&t2, 200);
        assert!(mgr.is_shard_fenced(5));

        // Complete both tasks. mark_complete on t1 keeps the fence (t2 still
        // Fenced). mark_complete on t2 unfences (no other Fenced task).
        mgr.mark_complete(&t1);
        mgr.mark_complete(&t2);
        assert!(!mgr.is_shard_fenced(5), "both completed, should be unfenced");

        // Re-fence for the dangerous scenario: both completed, then cleanup.
        mgr.fence_shard(5);
        // Simulate: re-add two completed tasks.
        mgr.start_outbound(&[t1.clone(), t2.clone()], NodeId(1), &std::collections::HashSet::new());
        mgr.mark_fenced(&t1, 100);
        mgr.mark_fenced(&t2, 200);
        mgr.mark_complete(&t1);
        mgr.mark_complete(&t2);

        // Now fence shard 5 again manually to simulate the bug scenario
        // where unfencing didn't happen.
        mgr.fence_shard(5);
        // Call cleanup — it should unfence shard 5 since all tasks are
        // Complete (none remaining in Fenced state).
        mgr.cleanup_completed();
        assert!(!mgr.is_shard_fenced(5),
            "cleanup_completed must unfence shards with no remaining fenced tasks");
    }

    // -----------------------------------------------------------------------
    // Inbound entries must be clearable even when outbound task fails
    // -----------------------------------------------------------------------

    /// Simulates the scenario where a shard has both a master (empty) and
    /// replica migration task. The empty master task completes instantly,
    /// which commits the shard. The replica task then fails because the
    /// shard is already committed. The inbound entry (registered when
    /// migration data arrived) must be cleared by mark_inbound_complete
    /// even though the outbound task failed — otherwise writes to that
    /// shard are blocked indefinitely.
    #[test]
    fn inbound_cleared_when_migration_aborted_for_committed_shard() {
        let mut mgr = MigrationManager::new();
        let master_task = MigrationTask {
            shard: 42, from_node: NodeId(1), to_node: NodeId(3), is_master: true,
        };
        let replica_task = MigrationTask {
            shard: 42, from_node: NodeId(1), to_node: NodeId(3), is_master: false,
        };
        mgr.start_outbound(
            &[master_task.clone(), replica_task.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        // Simulate: empty master completes instantly.
        mgr.mark_complete(&master_task);

        // Simulate: replica migration sent data, receiver registered inbound.
        mgr.mark_inbound_active(42);
        assert!(mgr.has_pending_inbound(42), "inbound should be active after data arrived");

        // Simulate: replica task fails because shard is already committed.
        // The coordinator should call mark_inbound_complete BEFORE mark_failed.
        mgr.mark_inbound_complete(42);
        mgr.mark_failed(&replica_task);

        // Inbound must be cleared — writes should not be blocked.
        assert!(!mgr.has_pending_inbound(42),
            "inbound must be cleared when migration aborts for committed shard");
    }
}
