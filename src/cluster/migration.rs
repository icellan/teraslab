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
}

impl MigrationManager {
    /// Create a new migration manager with no active migrations.
    pub fn new() -> Self {
        Self { active: Vec::new() }
    }

    /// Start migrations from a list of tasks.
    ///
    /// Only tasks where this node is the source (`from_node`) are tracked.
    pub fn start_outbound(&mut self, tasks: &[MigrationTask], self_id: NodeId) {
        for task in tasks {
            if task.from_node == self_id {
                self.active.push(MigrationProgress::from_task(task));
            }
        }
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

    /// Remove completed migrations.
    pub fn cleanup_completed(&mut self) {
        self.active.retain(|p| !p.is_complete());
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
}
