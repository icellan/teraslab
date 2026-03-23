//! Data migration tracking for shard rebalancing.

use crate::cluster::shards::{MigrationTask, NodeId};

/// State of an active migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationState {
    /// Scanning and streaming records to the target.
    Streaming,
    /// All records sent, waiting for target acknowledgment.
    WaitingForAck,
    /// Target confirmed, migration is complete.
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
}

impl MigrationProgress {
    /// Create a new migration progress from a task.
    pub fn from_task(task: &MigrationTask) -> Self {
        Self {
            shard: task.shard,
            from_node: task.from_node,
            to_node: task.to_node,
            state: MigrationState::Streaming,
            total_records: 0,
            migrated_records: 0,
            bytes_sent: 0,
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

/// Manages active migrations for this node.
pub struct MigrationManager {
    active: Vec<MigrationProgress>,
    /// Shards that this node expects to receive data for (inbound migrations).
    inbound_shards: Vec<u16>,
}

impl MigrationManager {
    /// Create a new migration manager with no active migrations.
    pub fn new() -> Self {
        Self {
            active: Vec::new(),
            inbound_shards: Vec::new(),
        }
    }

    /// Start migrations from a list of tasks, tracking both outbound and inbound.
    ///
    /// Outbound tasks (this node is source) are fully tracked with progress.
    /// Inbound tasks (this node is target) are tracked by shard ID so the
    /// read path can wait for data to arrive before returning NotFound.
    pub fn start_outbound(&mut self, tasks: &[MigrationTask], self_id: NodeId) {
        for task in tasks {
            if task.from_node == self_id {
                self.active.push(MigrationProgress::from_task(task));
            }
            if task.to_node == self_id {
                self.inbound_shards.push(task.shard);
            }
        }
    }

    /// Mark an inbound shard as received (data has arrived).
    pub fn mark_inbound_complete(&mut self, shard: u16) {
        self.inbound_shards.retain(|&s| s != shard);
    }

    /// Check if this node is expecting inbound data for the given shard.
    pub fn has_pending_inbound(&self, shard: u16) -> bool {
        self.inbound_shards.contains(&shard)
    }

    /// Mark records as migrated for a shard.
    pub fn record_progress(&mut self, shard: u16, records: u64, bytes: u64) {
        if let Some(p) = self.active.iter_mut().find(|p| p.shard == shard) {
            p.migrated_records += records;
            p.bytes_sent += bytes;
        }
    }

    /// Mark a migration as waiting for ack.
    pub fn mark_waiting_for_ack(&mut self, shard: u16) {
        if let Some(p) = self.active.iter_mut().find(|p| p.shard == shard) {
            p.state = MigrationState::WaitingForAck;
        }
    }

    /// Mark a migration as complete.
    pub fn mark_complete(&mut self, shard: u16) {
        if let Some(p) = self.active.iter_mut().find(|p| p.shard == shard) {
            p.state = MigrationState::Complete;
        }
    }

    /// Mark a migration as failed after all retries exhausted.
    ///
    /// Failed migrations are retained in the active list for visibility
    /// and potential retry. They are NOT removed by `cleanup_completed()`.
    pub fn mark_failed(&mut self, shard: u16) {
        if let Some(p) = self.active.iter_mut().find(|p| p.shard == shard) {
            p.state = MigrationState::Failed;
        }
    }

    /// Number of failed migrations.
    pub fn failed_count(&self) -> usize {
        self.active.iter().filter(|p| p.state == MigrationState::Failed).count()
    }

    /// Remove completed migrations (but NOT failed ones).
    ///
    /// When all outbound migrations are complete (none active, none failed),
    /// also clears the inbound shard tracking — if we've finished sending
    /// data to all targets, the cluster has fully rebalanced and any
    /// pending inbound data should have arrived as well.
    pub fn cleanup_completed(&mut self) {
        self.active.retain(|p| !p.is_complete());
        if self.active.is_empty() {
            self.inbound_shards.clear();
        }
    }

    /// Check if a shard is currently being migrated outbound.
    pub fn is_migrating_shard(&self, shard: u16) -> bool {
        self.active
            .iter()
            .any(|p| p.shard == shard && !p.is_complete())
    }

    /// Number of active (non-complete) migrations.
    pub fn active_count(&self) -> usize {
        self.active.iter().filter(|p| !p.is_complete()).count()
    }

    /// Get all active migrations.
    pub fn active_migrations(&self) -> &[MigrationProgress] {
        &self.active
    }
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

        mgr.start_outbound(&tasks, NodeId(1));
        assert_eq!(mgr.active_count(), 2); // Only shards 0 and 2
    }

    #[test]
    fn progress_tracking() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask { shard: 5, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[task], NodeId(1));

        let p = &mgr.active_migrations()[0];
        assert_eq!(p.state, MigrationState::Streaming);

        mgr.record_progress(5, 50, 50_000);
        mgr.mark_waiting_for_ack(5);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::WaitingForAck);
        assert_eq!(mgr.active_migrations()[0].migrated_records, 50);

        mgr.mark_complete(5);
        assert!(mgr.active_migrations()[0].is_complete());

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
        mgr.start_outbound(&[task], NodeId(1));

        mgr.mark_failed(3);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Failed);

        // cleanup_completed should NOT remove failed migrations
        mgr.cleanup_completed();
        assert_eq!(mgr.active_count(), 1);
        assert_eq!(mgr.failed_count(), 1);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Failed);
    }

    #[test]
    fn waiting_for_ack_transitions_correctly() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask { shard: 7, from_node: NodeId(1), to_node: NodeId(2), is_master: true };
        mgr.start_outbound(&[task], NodeId(1));

        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Streaming);

        mgr.mark_waiting_for_ack(7);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::WaitingForAck);

        mgr.mark_complete(7);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Complete);
        assert!(mgr.active_migrations()[0].is_complete());

        mgr.cleanup_completed();
        assert_eq!(mgr.active_count(), 0);
    }
}
