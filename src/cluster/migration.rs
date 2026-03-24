//! Data migration tracking for shard rebalancing.

use crate::cluster::shards::{MigrationTask, NodeId};

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
#[derive(Debug, Clone, PartialEq)]
struct InboundMigration {
    shard: u16,
    from_node: NodeId,
    /// True once `OP_MIGRATION_COMPLETE` confirmed data arrived.
    completed: bool,
}

/// Manages active migrations for this node.
pub struct MigrationManager {
    active: Vec<MigrationProgress>,
    /// Per-task inbound migration tracking. Each entry represents a shard
    /// this node expects to receive data for, with its source node.
    /// Entries are only removed when explicitly marked completed.
    inbound_migrations: Vec<InboundMigration>,
    /// Shards where writes are fenced on this node (source is migrating out).
    /// Dispatch rejects mutations for these shards with ERR_MIGRATION_IN_PROGRESS.
    /// Reads continue to be served locally during the fence.
    fenced_shards: Vec<u16>,
}

impl MigrationManager {
    /// Create a new migration manager with no active migrations.
    pub fn new() -> Self {
        Self {
            active: Vec::new(),
            inbound_migrations: Vec::new(),
            fenced_shards: Vec::new(),
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
            // Only register inbound for shards that actually have records.
            // Empty shards complete instantly with no data transfer.
            if task.to_node == self_id
                && populated_shards.contains(&task.shard)
                && !self.inbound_migrations.iter().any(|m| m.shard == task.shard && m.from_node == task.from_node)
            {
                self.inbound_migrations.push(InboundMigration {
                    shard: task.shard,
                    from_node: task.from_node,
                    completed: false,
                });
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
            });
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
    }

    /// Check if this node is expecting inbound data for the given shard.
    pub fn has_pending_inbound(&self, shard: u16) -> bool {
        self.inbound_migrations.iter().any(|m| m.shard == shard && !m.completed)
    }

    /// Fence a shard on the source node — writes for this shard will be
    /// rejected with ERR_MIGRATION_IN_PROGRESS. Reads continue locally.
    pub fn fence_shard(&mut self, shard: u16) {
        if !self.fenced_shards.contains(&shard) {
            self.fenced_shards.push(shard);
        }
    }

    /// Remove the write fence for a shard (migration completed or failed).
    pub fn unfence_shard(&mut self, shard: u16) {
        self.fenced_shards.retain(|&s| s != shard);
    }

    /// Check if writes are fenced for the given shard on this node.
    pub fn is_shard_fenced(&self, shard: u16) -> bool {
        self.fenced_shards.contains(&shard)
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
    pub fn mark_complete(&mut self, task: &MigrationTask) {
        if let Some(p) = self.find_task_mut(task) {
            p.state = MigrationState::Complete;
        }
        self.unfence_shard(task.shard);
    }

    /// Mark a migration as failed after all retries exhausted.
    ///
    /// Failed migrations are retained in the active list for visibility
    /// and potential retry. They are NOT removed by `cleanup_completed()`.
    /// The write fence is lifted so the shard can continue serving on
    /// the old master.
    pub fn mark_failed(&mut self, task: &MigrationTask) {
        if let Some(p) = self.find_task_mut(task) {
            p.state = MigrationState::Failed;
        }
        self.unfence_shard(task.shard);
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

    /// Remove completed migrations (but NOT failed ones).
    ///
    /// Outbound migrations in the Complete state are removed from the active
    /// list. Inbound migrations marked as completed are also removed.
    /// Inbound and outbound tracking are independent — completing outbound
    /// work does NOT clear pending inbound migrations (which may still be
    /// receiving data from other nodes).
    pub fn cleanup_completed(&mut self) {
        self.active.retain(|p| !p.is_complete());
        self.inbound_migrations.retain(|m| !m.completed);
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
        self.fenced_shards.len()
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
                });
            }
        }
    }

    /// Clear all inbound migrations (used when the topology changes and
    /// supersedes any in-flight migrations).
    pub fn clear_inbound(&mut self) {
        self.inbound_migrations.clear();
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

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn failed_migration_not_cleaned_up() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask { shard: 3, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[task.clone()], NodeId(1), &std::collections::HashSet::new());

        mgr.mark_failed(&task);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Failed);

        // cleanup_completed should NOT remove failed migrations
        mgr.cleanup_completed();
        assert_eq!(mgr.active_count(), 0); // active_count excludes Failed
        assert_eq!(mgr.failed_count(), 1); // but failed_count tracks them
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Failed);
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
        populated.insert(5); // shard 5 has data on the source
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
}
