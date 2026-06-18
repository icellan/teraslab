//! Data migration tracking for shard rebalancing.

use crate::cluster::shards::{MigrationTask, NUM_SHARDS, NodeId};
use crate::metrics::{MigrationLabel, MigrationMetrics, migration_metrics};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Saturating-decrement of the active-migrations gauge.
///
/// C-8: uses `fetch_update` so the read-modify-write is atomic. The previous
/// `load` then `store(prev - 1)` could lose a decrement under concurrency
/// (two callers both read `prev`, both store `prev - 1`).
fn dec_active(m: &MigrationMetrics) {
    let _ = m
        .migration_active
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
            prev.checked_sub(1)
        });
}

/// Saturating-decrement of the phase gauge corresponding to `state`.
///
/// C-8: atomic via `fetch_update` (see [`dec_active`]). `checked_sub`
/// returning `None` at zero makes `fetch_update` a no-op, preserving the
/// saturating-at-zero semantics without a separate load.
fn dec_phase_gauge(m: &MigrationMetrics, state: &MigrationState) {
    let gauge = match state {
        MigrationState::Preparing => &m.migration_phase_preparing,
        MigrationState::Streaming => &m.migration_phase_copying,
        MigrationState::Fenced => &m.migration_phase_delta,
        MigrationState::Complete => &m.migration_phase_serving_new,
        MigrationState::Failed => return,
    };
    let _ = gauge.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
        prev.checked_sub(1)
    });
}

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
        Self {
            words: [0u64; Self::WORDS],
        }
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

/// Per-record diagnostic snapshot of a node's view of one txid's shard.
///
/// Returned (one per requested txid, in the same order) by
/// `OP_ADMIN_DIAGNOSE_KEY` so integration tests can dump rich diagnostic
/// information when the migration-reads barrier times out and figure out
/// *why* a record is unreadable on a given node (e.g., shard still
/// inbound-pending vs. fenced vs. owned by a different master).
///
/// The migration tracker can answer the migration-related fields on its
/// own; the dispatch handler fills in the routing and storage fields
/// from the shard table, this node's id, the index, and the
/// coordinator's topology epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyDiagnosis {
    /// Shard the txid maps to (`ShardTable::shard_for_key`).
    pub shard: u16,
    /// The id of the node producing this diagnosis (the responder).
    pub this_node_id: u64,
    /// The master id this node *believes* owns the shard, per its local
    /// shard table. May differ from the canonical / committed master
    /// during topology activation.
    pub local_view_canonical_master_id: u64,
    /// True iff this node's index has an entry for the txid.
    pub has_local_data: bool,
    /// True iff this node's local shard table assigns it as master of
    /// the shard.
    pub is_local_master_of_shard: bool,
    /// True iff this node is still expecting inbound migration data for
    /// the shard.
    pub has_pending_inbound: bool,
    /// True iff outbound writes for the shard are fenced on this node.
    pub is_shard_fenced: bool,
    /// True iff there is an outbound migration actively in progress for
    /// the shard from this node.
    pub is_migrating_shard: bool,
    /// Current monotonic topology epoch on the responding node.
    pub topology_epoch: u64,
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
    /// W1.1 residual fix — wall-clock instant at which this node last sent
    /// an `OP_MIGRATION_TRANSFER_REQUEST` for this shard (the pull-based
    /// repair). `None` means no outstanding request.
    ///
    /// The settled-inbound GC (which reaps inbound entries orphaned by a
    /// source that died mid-migration) must NOT reap an entry whose resend
    /// is still in flight: the source honours the request and pushes
    /// AFTER the request returns, so a freshly-requested entry that gets
    /// GC'd leaves the shard with the request's completion arriving at a
    /// node that no longer expects it. This stamp lets the GC skip
    /// recently-requested entries for a bounded grace window.
    ///
    /// Not serialized: an outstanding request does not survive a restart
    /// (the requester re-derives and re-sends from `pending_inbound_entries`
    /// after restore), so this is process-local timing state only.
    transfer_requested_at: Option<std::time::Instant>,
}

// ---------------------------------------------------------------------------
// MigrationThrottle — Phase G outbound-bytes admission control
// ---------------------------------------------------------------------------

/// Phase G — caps the *concurrent* outbound migration bytes admitted on
/// this node so a flood of overlapping migrations cannot starve replica
/// traffic or exhaust SSD bandwidth.
///
/// Lock-free: a single [`AtomicU64`](std::sync::atomic::AtomicU64) tracks
/// the bytes currently admitted. [`try_admit`](Self::try_admit) returns a
/// [`MigrationToken`] RAII guard whose `Drop` returns capacity to the
/// throttle. A request that would push the in-flight total over
/// `cap_bytes` is rejected (returns `None`) without consuming any
/// capacity, so the caller can retry later.
///
/// Zero-byte requests are admitted unconditionally and consume no
/// capacity (small empty shards must never block on the throttle).
///
/// Wire-up: the coordinator gates the `Preparing → Streaming` transition
/// on `try_admit` so a queued migration sits in `Preparing` until budget
/// becomes available. The cap is sourced from the
/// `TERASLAB_MAX_BYTES_EMIGRATING` env var, defaulting to 32 MiB.
pub struct MigrationThrottle {
    cap_bytes: u64,
    in_flight: std::sync::atomic::AtomicU64,
}

impl MigrationThrottle {
    /// Default cap (32 MiB) — matches Aerospike's `MAX_BYTES_EMIGRATING`.
    pub const DEFAULT_CAP_BYTES: u64 = 32 * 1024 * 1024;

    /// Env var that overrides the cap at process startup. Empty / unset /
    /// unparseable values fall back to [`DEFAULT_CAP_BYTES`](Self::DEFAULT_CAP_BYTES).
    pub const ENV_VAR: &'static str = "TERASLAB_MAX_BYTES_EMIGRATING";

    /// Build a throttle with a fixed byte cap.
    pub fn new(cap_bytes: u64) -> Self {
        Self {
            cap_bytes,
            in_flight: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Build a throttle from `TERASLAB_MAX_BYTES_EMIGRATING` (falling back to
    /// [`DEFAULT_CAP_BYTES`](Self::DEFAULT_CAP_BYTES) when unset / invalid).
    pub fn from_env() -> Self {
        let cap = std::env::var(Self::ENV_VAR)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|cap| *cap > 0)
            .unwrap_or(Self::DEFAULT_CAP_BYTES);
        Self::new(cap)
    }

    /// Configured cap.
    pub fn cap_bytes(&self) -> u64 {
        self.cap_bytes
    }

    /// Bytes currently admitted (sum of live tokens).
    pub fn in_flight_bytes(&self) -> u64 {
        self.in_flight.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Try to admit `bytes` of outbound migration work. Returns
    /// `Some(MigrationToken)` whose drop releases capacity, or `None`
    /// when the request would exceed the cap.
    pub fn try_admit(self: &Arc<Self>, bytes: u64) -> Option<MigrationToken> {
        if bytes == 0 {
            return Some(MigrationToken {
                throttle: Arc::clone(self),
                bytes: 0,
            });
        }
        let mut current = self.in_flight.load(std::sync::atomic::Ordering::Acquire);
        loop {
            if current.saturating_add(bytes) > self.cap_bytes {
                return None;
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + bytes,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(MigrationToken {
                        throttle: Arc::clone(self),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl std::fmt::Debug for MigrationThrottle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationThrottle")
            .field("cap_bytes", &self.cap_bytes)
            .field("in_flight_bytes", &self.in_flight_bytes())
            .finish()
    }
}

/// RAII admission token returned by [`MigrationThrottle::try_admit`].
/// Capacity is released on `Drop`.
pub struct MigrationToken {
    throttle: Arc<MigrationThrottle>,
    bytes: u64,
}

impl MigrationToken {
    /// Bytes this token represents.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for MigrationToken {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        let prev = self
            .throttle
            .in_flight
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::AcqRel);
        debug_assert!(prev >= self.bytes, "throttle underflow on token drop");
    }
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
    /// Phase E — dual-write window: shards being migrated outbound from
    /// this node, mapped to the destination NodeIds (new master and any
    /// new replicas) that must also receive replica batches while the
    /// migration is in flight. Cleared on `mark_complete` / `mark_failed`.
    dual_write_targets: std::collections::HashMap<u16, Vec<NodeId>>,
    /// Data-loss guard (task #28): shards this node has POSITIVELY,
    /// COMMITTED-ly handed off as the outbound source, mapped to the
    /// topology epoch at which the master move was committed to the
    /// shard table. Orphan cleanup may delete a non-owned shard's local
    /// records ONLY if it appears here — i.e. there is positive evidence
    /// the data is durably installed on the new owner.
    ///
    /// A shard that became non-owned WITHOUT a committed handoff from
    /// this node (e.g. the topology advanced while an in-flight handoff
    /// was discarded as stale) is deliberately ABSENT, so cleanup
    /// retains its last copy rather than stranding it. Cleared when this
    /// node re-acquires the shard as an inbound migration target.
    committed_handoffs: std::collections::HashMap<u16, u64>,
    /// Per-shard accumulation of each pending source's reconciliation manifest
    /// (deletion-tombstone Phase 8, design §9.1 #1 — the multi-source union).
    ///
    /// Keyed by shard; each value is the list of per-source manifests collected
    /// since the shard's inbound migration began. Drained when the shard
    /// commits and the union drop is applied. Co-located with the inbound
    /// state it shadows so EVERY inbound-clear path
    /// ([`Self::clear_inbound`], [`Self::clear_stale_inbound`],
    /// [`Self::clear_pending_inbound_for_shards`],
    /// [`Self::mark_inbound_complete_all`]) also drops the matching accumulator
    /// entries — preventing the leak where a non-committing handoff stranded an
    /// entry forever (BUG4 fix (b)). Empty and untouched unless
    /// `tombstone_reconciliation_enabled`, so the off-path is byte-identical
    /// (a remove from an empty map is a no-op).
    reconcile_accumulator: std::collections::HashMap<u16, Vec<SourceReconcileManifest>>,
}

/// One source's per-shard reconciliation manifest, accumulated across
/// `OP_MIGRATION_COMPLETE` arrivals for the multi-source UNION drop rule
/// (deletion-tombstone Phase 8, design §9.1 #1).
///
/// A rejoinee may receive completions from several concurrent sources for the
/// same shard. The Drop decision (§7 row 2) must be evaluated against the UNION
/// of ALL pending sources' live∪tombstone sets, NOT a single source — a key
/// tombstoned by source X but live on source Y must be KEPT. So each completion
/// stashes its `(live_keys, tombstones)` here and the actual drops are deferred
/// to the commit gate (`!has_pending_inbound_shard`), where the union is
/// computed. Touched ONLY on the `tombstone_reconciliation_enabled` path.
#[derive(Debug, Clone, Default)]
pub struct SourceReconcileManifest {
    /// The source's LIVE keys for the shard (its exact-entry manifest).
    pub live: Vec<crate::index::TxKey>,
    /// The source's TOMBSTONES for the shard: `(key, deletion-generation)`.
    pub tombstones: Vec<(crate::index::TxKey, u32)>,
    /// The `migration_epoch` of the completion that produced this manifest
    /// (BUG4 fix (a)). The commit gate DISCARDS any accumulated entry whose
    /// epoch is not the current committed/migration epoch BEFORE computing the
    /// union, so a STALE `{tomb k}` from a superseded plan can never drive a
    /// Drop of a now-live `k` in a later epoch's commit.
    pub epoch: u64,
}

impl MigrationManager {
    /// Create a new migration manager with no active migrations.
    pub fn new() -> Self {
        Self {
            active: Vec::new(),
            inbound_migrations: Vec::new(),
            inbound_bitmap: ShardBitmap::new(),
            fenced_shards: ShardBitmap::new(),
            dual_write_targets: std::collections::HashMap::new(),
            committed_handoffs: std::collections::HashMap::new(),
            reconcile_accumulator: std::collections::HashMap::new(),
        }
    }

    /// Accumulate one source's per-shard reconciliation manifest for the
    /// multi-source union (deletion-tombstone Phase 8, design §9.1 #1).
    ///
    /// Called once per `OP_MIGRATION_COMPLETE` arrival on the
    /// `tombstone_reconciliation_enabled` path. The manifests are unioned and
    /// the deferred drops applied at the commit gate via
    /// [`Self::take_reconcile_accumulator`]. No-op semantics off-path: nothing
    /// else reads this map unless reconciliation is enabled.
    pub fn accumulate_reconcile_manifest(&mut self, shard: u16, manifest: SourceReconcileManifest) {
        self.reconcile_accumulator
            .entry(shard)
            .or_default()
            .push(manifest);
    }

    /// Remove and return all accumulated source manifests for `shard`.
    ///
    /// Called at the commit gate to compute the union (`live` / `tombstone`
    /// sets across every pending source) before applying the deferred drops.
    /// Returns an empty vec if nothing was accumulated (e.g. reconciliation
    /// disabled, or a source sent no tombstone section).
    pub fn take_reconcile_accumulator(&mut self, shard: u16) -> Vec<SourceReconcileManifest> {
        self.reconcile_accumulator
            .remove(&shard)
            .unwrap_or_default()
    }

    /// Discard any accumulated source manifests for `shard` without applying
    /// them — used on abort / inbound-clear so a non-committing handoff does
    /// not leak accumulator entries (BUG4 fix (b)). No-op off-path.
    pub fn clear_reconcile_accumulator(&mut self, shard: u16) {
        self.reconcile_accumulator.remove(&shard);
    }

    /// Number of accumulated source manifests for `shard` (test-only peek that
    /// does NOT drain — used to assert the BUG4 accumulate-lifecycle).
    #[cfg(test)]
    pub(crate) fn reconcile_accumulator_len_for_test(&self, shard: u16) -> usize {
        self.reconcile_accumulator
            .get(&shard)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Drop accumulator entries for every shard that is no longer
    /// pending-inbound (BUG4 fix (b)).
    ///
    /// Called after a bulk inbound-clear ([`Self::clear_inbound`],
    /// [`Self::clear_stale_inbound`], [`Self::clear_pending_inbound_for_shards`]):
    /// a reconcile manifest is meaningful ONLY while its shard is still awaiting
    /// inbound data and may yet commit through the union. Once the shard's
    /// pending entry is gone (settled, superseded, or reaped) the handoff will
    /// not commit through the union, so its accumulated manifests must be
    /// dropped rather than stranded forever. Retains entries for shards still
    /// pending so a concurrent in-flight source is not lost. No-op off-path
    /// (map empty).
    fn prune_reconcile_accumulator_to_pending(&mut self) {
        if self.reconcile_accumulator.is_empty() {
            return;
        }
        // Split the borrows: `retain` mutably borrows the accumulator while the
        // closure only reads the (separate) inbound bitmap field.
        let bitmap = &self.inbound_bitmap;
        self.reconcile_accumulator
            .retain(|shard, _| bitmap.test(*shard));
    }

    /// Record that this node has COMMITTED-ly handed off `shard` as the
    /// outbound source at topology `epoch` (the master move was written to
    /// the shard table). This is the positive evidence orphan cleanup
    /// requires before deleting the shard's local records.
    ///
    /// Called from the migration-completion path only after `commit_shard`
    /// has transferred ownership to the new master.
    pub fn record_committed_handoff(&mut self, shard: u16, epoch: u64) {
        self.committed_handoffs.insert(shard, epoch);
    }

    /// Whether this node has positive committed-handoff evidence for `shard`
    /// that is STILL VALID at `current_epoch`. Orphan cleanup deletes a
    /// non-owned shard ONLY when this returns true.
    ///
    /// The match is epoch-EXACT: a handoff verified at epoch N authorizes
    /// deletion only while the table is still at epoch N — the epoch where
    /// this node positively confirmed the then-current owner durably held
    /// the data (count+manifest handshake). Once the topology advances, that
    /// evidence is stale: the new owner may be a DIFFERENT node this node
    /// never verified (the multi-hop churn case — e.g. the original target was
    /// killed and a fresh re-home from this node is the only way to restore
    /// the data). Honoring a stale entry across an epoch bump would let this
    /// node delete the last copy while the re-home is still in flight, which
    /// is exactly the data-loss bug. Stale entries cause retain-until-verified:
    /// the bytes linger until a fresh same-epoch handoff completes (or this
    /// node re-acquires the shard, which clears the entry).
    pub fn has_committed_handoff(&self, shard: u16, current_epoch: u64) -> bool {
        self.committed_handoffs.get(&shard) == Some(&current_epoch)
    }

    /// Drop any committed-handoff record for `shard`. Called when this
    /// node re-acquires the shard (becomes an inbound target again), so a
    /// stale record cannot authorize deleting freshly re-homed data after
    /// a subsequent un-assignment that lacks its own committed handoff.
    pub fn clear_committed_handoff(&mut self, shard: u16) {
        self.committed_handoffs.remove(&shard);
    }

    /// Phase E: NodeIds (new master + new replicas) that must additionally
    /// receive replica batches for `shard` while it is migrating outbound
    /// from this node. Returns an empty slice when no dual-write window is
    /// active for the shard.
    pub fn dual_write_targets_for_shard(&self, shard: u16) -> &[NodeId] {
        self.dual_write_targets
            .get(&shard)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Snapshot of the entire dual-write map. Used by the coordinator /
    /// dispatch to expand replica fan-out for migrating shards.
    pub fn dual_write_map(&self) -> &std::collections::HashMap<u16, Vec<NodeId>> {
        &self.dual_write_targets
    }

    fn dual_write_add(&mut self, shard: u16, node: NodeId) {
        let entry = self.dual_write_targets.entry(shard).or_default();
        if !entry.contains(&node) {
            entry.push(node);
        }
    }

    fn dual_write_remove(&mut self, shard: u16) {
        self.dual_write_targets.remove(&shard);
    }

    /// Start migrations from a list of tasks.
    ///
    /// Outbound tasks (this node is source) are fully tracked with progress.
    /// Inbound tasks (this node is target) are registered per-task with
    /// the source node for every shard in the new topology. Empty shards are
    /// still fenced until the source proves completion, preventing the new
    /// owner from serving writes before the handoff is durably installed.
    pub fn start_outbound(
        &mut self,
        tasks: &[MigrationTask],
        self_id: NodeId,
        _populated_shards: &std::collections::HashSet<u16>,
    ) {
        for task in tasks {
            if task.from_node == self_id {
                self.active.push(MigrationProgress::from_task(task));
                // Phase E: open dual-write window so writes during the
                // migration land on the new master / replica destination
                // as well as the old replica set.
                self.dual_write_add(task.shard, task.to_node);
                if let Some(m) = migration_metrics() {
                    m.migration_active.fetch_add(1, Ordering::Relaxed);
                    m.migration_phase_preparing.fetch_add(1, Ordering::Relaxed);
                }
            }
            if task.to_node == self_id {
                // Re-acquiring this shard: drop any prior committed-handoff
                // record so a stale entry cannot later authorize deleting the
                // data we are now receiving back (task #28).
                self.committed_handoffs.remove(&task.shard);
                if let Some(existing) = self
                    .inbound_migrations
                    .iter_mut()
                    .find(|m| m.shard == task.shard && m.from_node == task.from_node)
                {
                    if !existing.completed {
                        existing.completed = false;
                        self.inbound_bitmap.set(task.shard);
                    }
                } else if let Some(sentinel) = self
                    .inbound_migrations
                    .iter_mut()
                    .find(|m| m.shard == task.shard && m.from_node == NodeId(0))
                {
                    sentinel.from_node = task.from_node;
                    if !sentinel.completed {
                        sentinel.completed = false;
                        self.inbound_bitmap.set(task.shard);
                    }
                } else {
                    self.inbound_migrations.push(InboundMigration {
                        shard: task.shard,
                        from_node: task.from_node,
                        completed: false,
                        transfer_requested_at: None,
                    });
                    self.inbound_bitmap.set(task.shard);
                }
            }
        }
    }

    /// Register a shard as actively receiving inbound migration data.
    ///
    /// Called when the first `OP_REPLICA_BATCH` for this shard arrives,
    /// so the read/write path knows to wait for migration completion.
    /// Since we may not know the source node at dispatch time, register
    /// with `NodeId(0)` as a sentinel if no existing entry matches.
    pub fn mark_inbound_active(&mut self, shard: u16) -> bool {
        if self.inbound_migrations.iter().any(|m| m.shard == shard) {
            return false;
        }
        self.inbound_migrations.push(InboundMigration {
            shard,
            from_node: NodeId(0),
            completed: false,
            transfer_requested_at: None,
        });
        self.inbound_bitmap.set(shard);
        true
    }

    /// Mark an inbound shard as received (data has arrived and been verified).
    ///
    /// Marks the first non-completed entry for this shard as completed.
    /// The entry is retained until `cleanup_completed()` removes it.
    pub fn mark_inbound_complete(&mut self, shard: u16) {
        if let Some(m) = self
            .inbound_migrations
            .iter_mut()
            .find(|m| m.shard == shard && !m.completed)
        {
            m.completed = true;
        } else {
            self.record_completed_inbound_tombstone(shard, NodeId(0));
        }
        // Clear bitmap bit only if no more pending entries for this shard.
        if !self
            .inbound_migrations
            .iter()
            .any(|m| m.shard == shard && !m.completed)
        {
            self.inbound_bitmap.clear(shard);
        }
    }

    /// Mark all pending inbound entries for this shard as complete.
    pub fn mark_inbound_complete_all(&mut self, shard: u16) {
        let mut found = false;
        for inbound in self
            .inbound_migrations
            .iter_mut()
            .filter(|m| m.shard == shard)
        {
            inbound.completed = true;
            found = true;
        }
        if !found {
            self.record_completed_inbound_tombstone(shard, NodeId(0));
        }
        self.inbound_bitmap.clear(shard);
        // BUG4 (b): this clears the shard's inbound state without routing
        // through the commit-gate union, so drop any accumulated reconcile
        // manifests for it to prevent a leak. No-op off-path (map empty).
        self.clear_reconcile_accumulator(shard);
    }

    pub fn mark_inbound_complete_from_source(&mut self, shard: u16, from_node: NodeId) {
        if let Some(m) = self
            .inbound_migrations
            .iter_mut()
            .find(|m| m.shard == shard && m.from_node == from_node && !m.completed)
        {
            m.completed = true;
        } else if let Some(m) = self
            .inbound_migrations
            .iter_mut()
            .find(|m| m.shard == shard && m.from_node == NodeId(0) && !m.completed)
        {
            m.completed = true;
        } else {
            self.record_completed_inbound_tombstone(shard, from_node);
        }
        if !self
            .inbound_migrations
            .iter()
            .any(|m| m.shard == shard && !m.completed)
        {
            self.inbound_bitmap.clear(shard);
        }
    }

    pub fn mark_inbound_complete_many_from_source<I>(&mut self, shards: I, from_node: NodeId)
    where
        I: IntoIterator<Item = u16>,
    {
        for shard in shards {
            self.mark_inbound_complete_from_source(shard, from_node);
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
        let prev_state = self.find_task_mut(task).map(|p| p.state.clone());
        if let Some(p) = self.find_task_mut(task) {
            p.state = MigrationState::Fenced;
            p.fence_sequence = fence_sequence;
        }
        self.fence_shard(task.shard);
        if let Some(m) = migration_metrics() {
            if let Some(prev) = prev_state {
                dec_phase_gauge(m, &prev);
            }
            m.migration_phase_delta.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Set the snapshot sequence checkpoint for a migration task.
    pub fn set_snapshot_sequence(&mut self, task: &MigrationTask, seq: u64) {
        let prev_state = self.find_task_mut(task).map(|p| p.state.clone());
        if let Some(p) = self.find_task_mut(task) {
            p.snapshot_sequence = seq;
            p.state = MigrationState::Streaming;
        }
        if let Some(m) = migration_metrics() {
            if let Some(prev) = prev_state {
                dec_phase_gauge(m, &prev);
            }
            m.migration_phase_copying.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Mark records as migrated for a task identified by (shard, from, to).
    pub fn record_progress(&mut self, task: &MigrationTask, records: u64, bytes: u64) {
        let is_master = self
            .find_task_mut(task)
            .map(|p| p.is_master)
            .unwrap_or(true);
        if let Some(p) = self.find_task_mut(task) {
            p.migrated_records += records;
            p.bytes_sent += bytes;
        }
        if let Some(m) = migration_metrics() {
            m.migration_entries_applied_total.inc_by(records);
            // This node is the source (`from_node == self_id`) — outbound.
            let label = if is_master {
                MigrationLabel::OutboundMaster
            } else {
                MigrationLabel::OutboundReplica
            };
            m.record_bytes(label, bytes);
        }
    }

    /// Mark a migration as complete and remove the write fence.
    ///
    /// The fence is only lifted if no other active migration task for
    /// this shard is still in the Fenced state. This prevents premature
    /// unfencing when multiple tasks target the same shard (e.g., master
    /// migration + replica backfill).
    pub fn mark_complete(&mut self, task: &MigrationTask) {
        let prev_state = self.find_task_mut(task).map(|p| p.state.clone());
        if let Some(p) = self.find_task_mut(task) {
            p.state = MigrationState::Complete;
        } else {
            return;
        }
        if !self.has_other_fenced_task(task.shard, task) {
            self.unfence_shard(task.shard);
        }
        // Phase E: close dual-write window once any active outbound task for
        // this shard remains unresolved. We aggressively close on first
        // completion — the new master is now authoritative and any straggler
        // writes from the old master are no longer durability-critical.
        if !self.has_other_active_outbound(task.shard, task) {
            self.dual_write_remove(task.shard);
        }
        if let Some(m) = migration_metrics() {
            if let Some(prev) = prev_state {
                dec_phase_gauge(m, &prev);
            }
            m.migration_phase_serving_new
                .fetch_add(1, Ordering::Relaxed);
            dec_active(m);
        }
    }

    /// Mark a migration as failed after all retries exhausted.
    ///
    /// The write fence is lifted so the shard can continue serving on
    /// the old master, unless another task for the same shard is still
    /// in the Fenced state. Failed migrations are removed from the
    /// active list by the next call to `cleanup_completed()`.
    pub fn mark_failed(&mut self, task: &MigrationTask) {
        let prev_state = self.find_task_mut(task).map(|p| p.state.clone());
        if let Some(p) = self.find_task_mut(task) {
            p.state = MigrationState::Failed;
        } else {
            return;
        }
        if !self.has_other_fenced_task(task.shard, task) {
            self.unfence_shard(task.shard);
        }
        // Phase E: failure rolls back to old master; close the dual-write
        // window so writes stop fanning out to the failed destination.
        if !self.has_other_active_outbound(task.shard, task) {
            self.dual_write_remove(task.shard);
        }
        if let Some(m) = migration_metrics() {
            if let Some(prev) = prev_state {
                dec_phase_gauge(m, &prev);
            }
            dec_active(m);
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

    /// Check if any active migration task for the given shard (other than
    /// `exclude`) is still in flight (not yet Complete or Failed).
    ///
    /// Used by `mark_complete` / `mark_failed` to decide whether the
    /// dual-write window can be closed: we only retire the window once
    /// every outbound task targeting this shard has resolved.
    fn has_other_active_outbound(&self, shard: u16, exclude: &MigrationTask) -> bool {
        self.active.iter().any(|p| {
            p.shard == shard
                && !p.is_complete()
                && p.state != MigrationState::Failed
                && !(p.from_node == exclude.from_node && p.to_node == exclude.to_node)
        })
    }

    fn record_completed_inbound_tombstone(&mut self, shard: u16, from_node: NodeId) {
        if self
            .inbound_migrations
            .iter()
            .any(|m| m.shard == shard && m.from_node == from_node && m.completed)
        {
            return;
        }
        self.inbound_migrations.push(InboundMigration {
            shard,
            from_node,
            completed: true,
            transfer_requested_at: None,
        });
    }

    /// Number of failed migrations.
    pub fn failed_count(&self) -> usize {
        self.active
            .iter()
            .filter(|p| p.state == MigrationState::Failed)
            .count()
    }

    /// Reset a failed migration back to Streaming so it can be retried.
    ///
    /// Returns true if the migration was found and reset, false otherwise.
    pub fn retry_failed(&mut self, task: &MigrationTask) -> bool {
        if let Some(p) = self.active.iter_mut().find(|p| {
            p.shard == task.shard
                && p.from_node == task.from_node
                && p.to_node == task.to_node
                && p.state == MigrationState::Failed
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
        let tasks: Vec<MigrationTask> = self
            .active
            .iter()
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

        self.active
            .retain(|p| !p.is_complete() && p.state != MigrationState::Failed);

        // Unfence shards that no longer have any fenced task.
        for shard in maybe_unfence {
            let still_fenced = self
                .active
                .iter()
                .any(|p| p.shard == shard && p.state == MigrationState::Fenced);
            if !still_fenced {
                self.unfence_shard(shard);
            }
        }

        if self.active.is_empty() {
            self.fenced_shards.clear_all();
        }

        self.inbound_migrations.retain(|m| !m.completed);
        // Rebuild inbound bitmap from remaining entries.
        self.inbound_bitmap.clear_all();
        for m in &self.inbound_migrations {
            self.inbound_bitmap.set(m.shard);
        }
    }

    /// Remove completed outbound migrations while preserving Failed entries.
    ///
    /// Failed entries are the durable retry queue for membership/topology
    /// handlers. The coordinator uses this after a migration batch returns
    /// so a dead target cannot trigger an unbounded detached retry loop.
    pub fn cleanup_completed_keep_failed(&mut self) {
        let mut maybe_unfence: Vec<u16> = Vec::new();
        for p in &self.active {
            if p.is_complete() && self.fenced_shards.test(p.shard) {
                maybe_unfence.push(p.shard);
            }
        }

        self.active.retain(|p| !p.is_complete());

        for shard in maybe_unfence {
            let still_fenced = self
                .active
                .iter()
                .any(|p| p.shard == shard && p.state == MigrationState::Fenced);
            if !still_fenced {
                self.unfence_shard(shard);
            }
        }

        if self
            .active
            .iter()
            .all(|p| p.state != MigrationState::Fenced)
        {
            self.fenced_shards.clear_all();
        }

        self.inbound_migrations.retain(|m| !m.completed);
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

    /// Project the per-shard fields this tracker can answer into a
    /// [`KeyDiagnosis`]. The remaining fields (node id, shard-table view,
    /// has_local_data, topology epoch) are filled in by the caller because
    /// they live outside this struct.
    ///
    /// Used by `OP_ADMIN_DIAGNOSE_KEY` to dump per-record migration state
    /// when the integration-test migration-reads barrier times out.
    pub fn diagnose_key_routing(&self, shard: u16) -> KeyDiagnosis {
        KeyDiagnosis {
            shard,
            this_node_id: 0,
            local_view_canonical_master_id: 0,
            has_local_data: false,
            is_local_master_of_shard: false,
            has_pending_inbound: self.has_pending_inbound(shard),
            is_shard_fenced: self.is_shard_fenced(shard),
            is_migrating_shard: self.is_migrating_shard(shard),
            topology_epoch: 0,
        }
    }

    /// Number of in-progress migrations (excludes Complete and Failed).
    pub fn active_count(&self) -> usize {
        self.active
            .iter()
            .filter(|p| !p.is_complete() && p.state != MigrationState::Failed)
            .count()
    }

    /// Get all active migrations.
    pub fn active_migrations(&self) -> &[MigrationProgress] {
        &self.active
    }

    /// Number of shards pending inbound data.
    pub fn inbound_count(&self) -> usize {
        self.inbound_migrations
            .iter()
            .filter(|m| !m.completed)
            .count()
    }

    /// Snapshot the currently pending inbound migrations.
    pub fn pending_inbound_entries(&self) -> Vec<(u16, NodeId)> {
        self.inbound_migrations
            .iter()
            .filter(|m| !m.completed)
            .map(|m| (m.shard, m.from_node))
            .collect()
    }

    /// W1.1 residual fix — stamp the listed pending inbound shards with the
    /// current instant, recording that this node has just sent an
    /// `OP_MIGRATION_TRANSFER_REQUEST` (pull-based repair) for them.
    ///
    /// The settled-inbound GC consults this stamp via
    /// [`Self::pending_inbound_shards_excluding_recent_requests`] so it will
    /// not reap an entry whose resend is still in flight.
    pub fn mark_inbound_requested(&mut self, shards: &std::collections::HashSet<u16>) {
        let now = std::time::Instant::now();
        for m in &mut self.inbound_migrations {
            if !m.completed && shards.contains(&m.shard) {
                m.transfer_requested_at = Some(now);
            }
        }
    }

    /// W1.1 residual fix — the set of pending inbound shards that the
    /// settled-inbound fast-path GC is allowed to reap right now: those
    /// with no outstanding transfer request, OR whose last request is older
    /// than `request_grace` (so a lost request still gets reaped eventually
    /// and the normal pull-based retry takes over).
    ///
    /// Entries requested within `request_grace` are EXCLUDED: their source
    /// honours the request and pushes the completion handshake AFTER the
    /// request RPC returns, so reaping them mid-flight strands the shard.
    pub fn pending_inbound_shards_excluding_recent_requests(
        &self,
        request_grace: std::time::Duration,
    ) -> std::collections::HashSet<u16> {
        let now = std::time::Instant::now();
        self.inbound_migrations
            .iter()
            .filter(|m| !m.completed)
            .filter(|m| match m.transfer_requested_at {
                Some(at) => now.duration_since(at) >= request_grace,
                None => true,
            })
            .map(|m| m.shard)
            .collect()
    }

    /// W1.1 residual fix — number of pending inbound shards with an
    /// outstanding (within-grace) transfer request. Test/diagnostic helper.
    pub fn pending_inbound_requested_count(&self, request_grace: std::time::Duration) -> usize {
        let now = std::time::Instant::now();
        self.inbound_migrations
            .iter()
            .filter(|m| !m.completed)
            .filter(|m| {
                m.transfer_requested_at
                    .map(|at| now.duration_since(at) < request_grace)
                    .unwrap_or(false)
            })
            .count()
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
        let pending: Vec<_> = self
            .inbound_migrations
            .iter()
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
            if !self
                .inbound_migrations
                .iter()
                .any(|m| m.shard == shard && m.from_node == from_node)
            {
                self.inbound_migrations.push(InboundMigration {
                    shard,
                    from_node,
                    completed: false,
                    transfer_requested_at: None,
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
        // BUG4 (b): no shard is pending after a full clear → drop the whole
        // accumulator (no-op off-path).
        self.prune_reconcile_accumulator_to_pending();
    }

    /// Remove pending inbound entries for the selected shards.
    ///
    /// This is used when the coordinator knows those shards are fully
    /// settled in the active topology and any remaining inbound entries are
    /// stale bookkeeping that would otherwise block the hot path forever.
    ///
    /// Returns the number of entries removed.
    pub fn clear_pending_inbound_for_shards(
        &mut self,
        shards: &std::collections::HashSet<u16>,
    ) -> usize {
        let before = self.inbound_migrations.len();
        self.inbound_migrations
            .retain(|m| m.completed || !shards.contains(&m.shard));
        let removed = before - self.inbound_migrations.len();
        if removed > 0 {
            self.inbound_bitmap.clear_all();
            for m in &self.inbound_migrations {
                if !m.completed {
                    self.inbound_bitmap.set(m.shard);
                }
            }
            // BUG4 (b): drop accumulator entries for shards no longer pending
            // (no-op off-path).
            self.prune_reconcile_accumulator_to_pending();
        }
        removed
    }

    /// Remove completed inbound migrations.
    ///
    /// Unlike [`Self::clear_inbound`] which removes everything, this preserves all
    /// pending entries regardless of age. A wall-clock timeout must never
    /// reopen a shard for writes while a migration could still complete.
    ///
    /// Returns the number of entries removed.
    pub fn clear_stale_inbound(&mut self, _max_age: std::time::Duration) -> usize {
        let before = self.inbound_migrations.len();
        self.inbound_migrations.retain(|m| !m.completed);
        let removed = before - self.inbound_migrations.len();
        if removed > 0 {
            // Rebuild bitmap from surviving entries.
            self.inbound_bitmap.clear_all();
            for m in &self.inbound_migrations {
                if !m.completed {
                    self.inbound_bitmap.set(m.shard);
                }
            }
            // BUG4 (b): a COMPLETED-but-uncommitted shard removed here will not
            // commit through the union → drop its accumulator (no-op off-path).
            self.prune_reconcile_accumulator_to_pending();
        }
        removed
    }

    /// Serialize active outbound migration state to bytes.
    ///
    /// Format:
    /// ```text
    /// [count:4][ shard:2 + from_node:8 + to_node:8 + is_master:1
    ///   + state:1 + snapshot_seq:8 + fence_seq:8 ] × count
    /// ```
    ///
    /// Per-entry size: 36 bytes. Only non-complete, non-failed entries
    /// are persisted — on restart these indicate migrations that were
    /// interrupted and may need to be re-initiated.
    pub fn serialize_outbound(&self) -> Vec<u8> {
        let active: Vec<_> = self
            .active
            .iter()
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
            let from_node = NodeId(u64::from_le_bytes(
                data[pos + 2..pos + 10].try_into().unwrap_or([0; 8]),
            ));
            let to_node = NodeId(u64::from_le_bytes(
                data[pos + 10..pos + 18].try_into().unwrap_or([0; 8]),
            ));
            let is_master = data[pos + 18] != 0;
            let state = match data[pos + 19] {
                0 => MigrationState::Preparing,
                1 => MigrationState::Streaming,
                2 => MigrationState::Fenced,
                3 => MigrationState::Complete,
                _ => MigrationState::Failed,
            };
            let snapshot_sequence =
                u64::from_le_bytes(data[pos + 20..pos + 28].try_into().unwrap_or([0; 8]));
            let fence_sequence =
                u64::from_le_bytes(data[pos + 28..pos + 36].try_into().unwrap_or([0; 8]));
            pos += 36;

            let task = MigrationTask {
                shard,
                from_node,
                to_node,
                is_master,
            };
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
        tracing::warn!(err = %e, "cluster: failed to persist inbound migration state");
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
        tracing::warn!(err = %e, "cluster: failed to persist outbound migration state");
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
            MigrationTask {
                shard: 0,
                from_node: NodeId(1),
                to_node: NodeId(2),
                is_master: true,
            },
            MigrationTask {
                shard: 1,
                from_node: NodeId(2),
                to_node: NodeId(1),
                is_master: true,
            },
            MigrationTask {
                shard: 2,
                from_node: NodeId(1),
                to_node: NodeId(3),
                is_master: true,
            },
        ];

        mgr.start_outbound(&tasks, NodeId(1), &std::collections::HashSet::new());
        assert_eq!(mgr.active_count(), 2); // Only shards 0 and 2
    }

    #[test]
    fn progress_tracking() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

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
        let task = MigrationTask {
            shard: 0,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let progress = MigrationProgress::from_task(&task);
        assert_eq!(progress.fraction_complete(), 1.0); // 0 total → 100%
    }

    #[test]
    fn failed_migration_cleaned_up_by_cleanup() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask {
            shard: 3,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

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
        let task = MigrationTask {
            shard: 7,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

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
        let t1 = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: false,
        };
        mgr.start_outbound(
            &[t1.clone(), t2.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        assert_eq!(mgr.active_count(), 2);

        mgr.mark_complete(&t1);
        assert_eq!(mgr.active_count(), 1);
        // t2 should still be in preparing state
        assert_eq!(
            mgr.active_migrations()
                .iter()
                .find(|p| p.to_node == NodeId(3))
                .unwrap()
                .state,
            MigrationState::Preparing
        );

        mgr.mark_complete(&t2);
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn cleanup_does_not_clear_active_inbound() {
        let mut mgr = MigrationManager::new();
        // Node 1 sends shard 10 to node 3 (outbound for node 1).
        // Node 2 sends shard 5 to node 1 (inbound for node 1).
        let outbound = MigrationTask {
            shard: 10,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: true,
        };
        let inbound = MigrationTask {
            shard: 5,
            from_node: NodeId(2),
            to_node: NodeId(1),
            is_master: true,
        };

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

        assert!(mgr.mark_inbound_active(42));
        assert!(mgr.has_pending_inbound(42));
        assert_eq!(mgr.inbound_count(), 1);

        // Duplicate call should not create a second entry.
        assert!(!mgr.mark_inbound_active(42));
        assert_eq!(mgr.inbound_count(), 1);

        mgr.mark_inbound_complete(42);
        assert!(!mgr.has_pending_inbound(42));
    }

    #[test]
    fn inbound_tracking_per_task() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 5,
            from_node: NodeId(2),
            to_node: NodeId(1),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 5,
            from_node: NodeId(3),
            to_node: NodeId(1),
            is_master: false,
        };
        let populated: std::collections::HashSet<u16> = [5u16].into_iter().collect();
        mgr.start_outbound(&[t1.clone(), t2.clone()], NodeId(1), &populated);

        assert_eq!(mgr.inbound_count(), 2);
        assert!(mgr.has_pending_inbound(5));

        mgr.mark_inbound_complete(5);
        assert!(mgr.has_pending_inbound(5));
        assert_eq!(mgr.inbound_count(), 1);

        mgr.mark_inbound_complete(5);
        assert!(!mgr.has_pending_inbound(5));
        assert_eq!(mgr.inbound_count(), 0);
    }

    #[test]
    fn source_aware_inbound_complete_clears_exact_source() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 10,
            from_node: NodeId(1),
            to_node: NodeId(9),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 10,
            from_node: NodeId(2),
            to_node: NodeId(9),
            is_master: false,
        };
        let populated: std::collections::HashSet<u16> = [10u16].into_iter().collect();
        mgr.start_outbound(&[t1, t2], NodeId(9), &populated);

        mgr.mark_inbound_complete_from_source(10, NodeId(2));
        assert!(mgr.has_pending_inbound(10));
        assert_eq!(mgr.inbound_count(), 1);

        mgr.mark_inbound_complete_from_source(10, NodeId(1));
        assert!(!mgr.has_pending_inbound(10));
        assert_eq!(mgr.inbound_count(), 0);
    }

    #[test]
    fn source_aware_batch_inbound_complete_clears_exact_sources() {
        let mut mgr = MigrationManager::new();
        let tasks = [
            MigrationTask {
                shard: 10,
                from_node: NodeId(1),
                to_node: NodeId(9),
                is_master: true,
            },
            MigrationTask {
                shard: 11,
                from_node: NodeId(1),
                to_node: NodeId(9),
                is_master: true,
            },
            MigrationTask {
                shard: 10,
                from_node: NodeId(2),
                to_node: NodeId(9),
                is_master: false,
            },
        ];
        let populated: std::collections::HashSet<u16> = [10u16, 11u16].into_iter().collect();
        mgr.start_outbound(&tasks, NodeId(9), &populated);

        mgr.mark_inbound_complete_many_from_source([10, 11], NodeId(1));

        assert!(
            mgr.has_pending_inbound(10),
            "batch completion from source 1 must not clear source 2's pending entry"
        );
        assert!(!mgr.has_pending_inbound(11));
        assert_eq!(mgr.inbound_count(), 1);

        mgr.mark_inbound_complete_many_from_source([10], NodeId(2));
        assert!(!mgr.has_pending_inbound(10));
        assert_eq!(mgr.inbound_count(), 0);
    }

    #[test]
    fn source_aware_inbound_complete_falls_back_to_sentinel() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(42);
        assert!(mgr.has_pending_inbound(42));

        mgr.mark_inbound_complete_from_source(42, NodeId(7));
        assert!(!mgr.has_pending_inbound(42));
        assert_eq!(mgr.inbound_count(), 0);
    }

    #[test]
    fn source_aware_inbound_complete_does_not_clear_wrong_source() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask {
            shard: 43,
            from_node: NodeId(7),
            to_node: NodeId(9),
            is_master: true,
        };
        let populated: std::collections::HashSet<u16> = [43u16].into_iter().collect();
        mgr.start_outbound(&[task], NodeId(9), &populated);

        mgr.mark_inbound_complete_from_source(43, NodeId(8));
        assert!(
            mgr.has_pending_inbound(43),
            "completion from an unrelated source must not clear the authoritative pending source"
        );
        assert_eq!(mgr.inbound_count(), 1);
    }

    #[test]
    fn start_outbound_replaces_sentinel_inbound_entry_for_same_shard() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(42);
        assert!(mgr.has_pending_inbound(42));
        assert_eq!(mgr.inbound_count(), 1);

        let task = MigrationTask {
            shard: 42,
            from_node: NodeId(7),
            to_node: NodeId(9),
            is_master: true,
        };
        let populated: std::collections::HashSet<u16> = [42u16].into_iter().collect();
        mgr.start_outbound(&[task], NodeId(9), &populated);

        assert_eq!(
            mgr.inbound_count(),
            1,
            "authoritative inbound registration should replace the provisional sentinel entry",
        );

        mgr.mark_inbound_complete_from_source(42, NodeId(7));
        assert!(
            !mgr.has_pending_inbound(42),
            "completion from the real source must clear the shard once the authoritative entry is registered",
        );
        assert_eq!(mgr.inbound_count(), 0);
    }

    #[test]
    fn late_migration_batch_does_not_reopen_completed_inbound_shard() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask {
            shard: 42,
            from_node: NodeId(7),
            to_node: NodeId(9),
            is_master: true,
        };
        let populated: std::collections::HashSet<u16> = [42u16].into_iter().collect();
        mgr.start_outbound(&[task], NodeId(9), &populated);

        mgr.mark_inbound_complete_from_source(42, NodeId(7));
        assert!(!mgr.has_pending_inbound(42));
        assert_eq!(mgr.inbound_count(), 0);

        assert!(
            !mgr.mark_inbound_active(42),
            "a late migration batch must not recreate inbound state after the authoritative completion arrived",
        );
        assert!(
            !mgr.has_pending_inbound(42),
            "completed inbound state should stay cleared after late batches",
        );
        assert_eq!(mgr.inbound_count(), 0);
    }

    #[test]
    fn early_empty_completion_does_not_reopen_inbound_on_late_registration() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask {
            shard: 3,
            from_node: NodeId(3),
            to_node: NodeId(1),
            is_master: true,
        };

        // The source proved there was no data before the target finished
        // registering its inbound expectation for this shard.
        mgr.mark_inbound_complete_all(3);
        assert!(!mgr.has_pending_inbound(3));

        mgr.start_outbound(&[task], NodeId(1), &std::collections::HashSet::new());

        assert!(
            !mgr.has_pending_inbound(3),
            "a zero-record completion that wins the race must prevent late inbound registration",
        );
        assert_eq!(mgr.inbound_count(), 0);
    }

    #[test]
    fn inbound_tracking_per_task_on_empty_target() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 5,
            from_node: NodeId(2),
            to_node: NodeId(1),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 5,
            from_node: NodeId(3),
            to_node: NodeId(1),
            is_master: false,
        };

        mgr.start_outbound(
            &[t1.clone(), t2.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        assert_eq!(mgr.inbound_count(), 2);
        assert!(mgr.has_pending_inbound(5));
    }

    #[test]
    fn serialize_restore_inbound_round_trip() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(10);
        let t = MigrationTask {
            shard: 20,
            from_node: NodeId(5),
            to_node: NodeId(1),
            is_master: true,
        };
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
        let t1 = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 10,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: false,
        };
        mgr.start_outbound(
            &[t1.clone(), t2.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        // Advance t1 to Streaming with a snapshot sequence.
        mgr.set_snapshot_sequence(&t1, 42);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Streaming);

        let data = mgr.serialize_outbound();
        let mut restored = MigrationManager::new();
        restored.restore_outbound(&data);

        // Both tasks should be restored.
        assert_eq!(restored.active_count(), 2);
        let p1 = restored
            .active_migrations()
            .iter()
            .find(|p| p.shard == 5)
            .expect("shard 5 restored");
        assert_eq!(p1.state, MigrationState::Streaming);
        assert_eq!(p1.snapshot_sequence, 42);
        assert!(p1.is_master);
        let p2 = restored
            .active_migrations()
            .iter()
            .find(|p| p.shard == 10)
            .expect("shard 10 restored");
        assert!(!p2.is_master);
    }

    #[test]
    fn serialize_outbound_skips_complete_and_failed() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 1,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 2,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: true,
        };
        let t3 = MigrationTask {
            shard: 3,
            from_node: NodeId(1),
            to_node: NodeId(4),
            is_master: true,
        };
        mgr.start_outbound(
            &[t1.clone(), t2.clone(), t3.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

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
        let t = MigrationTask {
            shard: 42,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&t),
            NodeId(1),
            &std::collections::HashSet::new(),
        );
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
        let t1 = MigrationTask {
            shard: 1,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 2,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: true,
        };
        let t3 = MigrationTask {
            shard: 3,
            from_node: NodeId(1),
            to_node: NodeId(4),
            is_master: true,
        };
        mgr.start_outbound(
            &[t1.clone(), t2.clone(), t3.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

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
        let tasks: Vec<MigrationTask> = (0..5)
            .map(|i| MigrationTask {
                shard: i,
                from_node: NodeId(1),
                to_node: NodeId(2),
                is_master: true,
            })
            .collect();
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
        let t1 = MigrationTask {
            shard: 1,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 2,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: true,
        };
        let t3 = MigrationTask {
            shard: 3,
            from_node: NodeId(1),
            to_node: NodeId(4),
            is_master: true,
        };
        let t4 = MigrationTask {
            shard: 4,
            from_node: NodeId(1),
            to_node: NodeId(5),
            is_master: true,
        };
        mgr.start_outbound(
            &[t1.clone(), t2.clone(), t3.clone(), t4.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        mgr.mark_complete(&t1);
        mgr.mark_failed(&t2);
        mgr.set_snapshot_sequence(&t3, 100); // Streaming
        // t4 still Preparing

        // active_count should only count Preparing + Streaming + Fenced
        assert_eq!(mgr.active_count(), 2); // t3 (Streaming) + t4 (Preparing)
        // The HTTP endpoint should report the same
        let all = mgr.active_migrations();
        let http_active = all
            .iter()
            .filter(|m| m.state != MigrationState::Complete && m.state != MigrationState::Failed)
            .count();
        assert_eq!(http_active, mgr.active_count());
    }

    /// Verify that take_failed_tasks works correctly before cleanup runs.
    /// This is the retry path used on NodeJoined events.
    #[test]
    fn take_failed_tasks_before_cleanup() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 1,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 2,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: true,
        };
        mgr.start_outbound(
            &[t1.clone(), t2.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

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
        let t = MigrationTask {
            shard: 42,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&t),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

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
        let task = MigrationTask {
            shard: 42,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        // Full lifecycle: Preparing → Streaming → Fenced → Complete
        mgr.set_snapshot_sequence(&task, 100);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Streaming);
        assert_eq!(mgr.active_migrations()[0].snapshot_sequence, 100);

        mgr.mark_fenced(&task, 200);
        assert_eq!(mgr.active_migrations()[0].state, MigrationState::Fenced);
        assert_eq!(mgr.active_migrations()[0].fence_sequence, 200);
        assert!(mgr.is_shard_fenced(42));

        mgr.mark_complete(&task);
        assert!(
            !mgr.is_shard_fenced(42),
            "fence should be lifted on complete"
        );
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
        assert!(
            !mgr.has_pending_inbound(20),
            "completed shard should be cleared"
        );
        assert!(mgr.has_pending_inbound(30));
        assert_eq!(mgr.inbound_count(), 2);
    }

    #[test]
    fn clear_stale_inbound_preserves_pending_entries() {
        let mut mgr = MigrationManager::new();
        mgr.inbound_migrations.push(InboundMigration {
            shard: 10,
            from_node: NodeId(1),
            completed: false,
            transfer_requested_at: None,
        });
        mgr.inbound_bitmap.set(10);
        mgr.inbound_migrations.push(InboundMigration {
            shard: 20,
            from_node: NodeId(0),
            completed: false,
            transfer_requested_at: None,
        });
        mgr.inbound_bitmap.set(20);

        let removed = mgr.clear_stale_inbound(Duration::ZERO);
        assert_eq!(removed, 0);
        assert!(mgr.has_pending_inbound(10));
        assert!(mgr.has_pending_inbound(20));
        assert_eq!(mgr.inbound_count(), 2);
    }

    /// W1.1 residual fix (FIX 1) — the settled-inbound fast-path GC must
    /// not reap an inbound entry whose transfer request is still in flight.
    /// This is the exact race that left fresh-cluster shards masterless:
    /// the requester registered an inbound entry, sent
    /// OP_MIGRATION_TRANSFER_REQUEST, and the GC fired before the source's
    /// resend arrived.
    #[test]
    fn settled_gc_skips_recently_requested_inbound() {
        let mut mgr = MigrationManager::new();
        // Two inbound entries: shard 10 from node 1, shard 20 from node 2.
        mgr.inbound_migrations.push(InboundMigration {
            shard: 10,
            from_node: NodeId(1),
            completed: false,
            transfer_requested_at: None,
        });
        mgr.inbound_bitmap.set(10);
        mgr.inbound_migrations.push(InboundMigration {
            shard: 20,
            from_node: NodeId(2),
            completed: false,
            transfer_requested_at: None,
        });
        mgr.inbound_bitmap.set(20);

        let grace = Duration::from_secs(10);

        // Before any request: both are reapable (orphaned-source case).
        let reapable = mgr.pending_inbound_shards_excluding_recent_requests(grace);
        assert_eq!(reapable, std::collections::HashSet::from([10, 20]));
        assert_eq!(mgr.pending_inbound_requested_count(grace), 0);

        // Request a resend for shard 10 only.
        mgr.mark_inbound_requested(&std::collections::HashSet::from([10]));

        // Shard 10 is now protected; shard 20 (no request) stays reapable.
        let reapable = mgr.pending_inbound_shards_excluding_recent_requests(grace);
        assert_eq!(
            reapable,
            std::collections::HashSet::from([20]),
            "freshly-requested shard 10 must be excluded from the settled GC"
        );
        assert_eq!(mgr.pending_inbound_requested_count(grace), 1);

        // Simulate the settled fast-path GC running in the resend window:
        // it must remove only shard 20, leaving shard 10 to receive its
        // in-flight resend.
        let removed = mgr.clear_pending_inbound_for_shards(&reapable);
        assert_eq!(removed, 1, "only the unrequested shard is reaped");
        assert!(
            mgr.has_pending_inbound(10),
            "the requested shard's inbound entry must survive the GC"
        );
        assert!(!mgr.has_pending_inbound(20));

        // With a zero grace (request older than grace), the protection
        // lapses so a genuinely-lost request is still eventually reaped.
        let reapable_no_grace =
            mgr.pending_inbound_shards_excluding_recent_requests(Duration::ZERO);
        assert_eq!(
            reapable_no_grace,
            std::collections::HashSet::from([10]),
            "once the request grace lapses the entry becomes reapable again"
        );
    }

    #[test]
    fn failed_migration_retry_resets_progress() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

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
        let t = MigrationTask {
            shard: 7,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&t),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

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
        let t1 = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: false,
        };
        mgr.start_outbound(
            &[t1.clone(), t2.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        mgr.mark_fenced(&t1, 100);
        mgr.mark_fenced(&t2, 200);
        assert!(mgr.is_shard_fenced(5));

        // Complete t1 → shard 5 STAYS fenced because t2 is still Fenced.
        mgr.mark_complete(&t1);
        assert!(
            mgr.is_shard_fenced(5),
            "shard should remain fenced while another task is in Fenced state"
        );

        // t2 is still tracked as Fenced in its progress entry.
        let t2_progress = mgr
            .active_migrations()
            .iter()
            .find(|p| p.to_node == NodeId(3))
            .expect("t2 should still be active");
        assert_eq!(t2_progress.state, MigrationState::Fenced);

        // Complete t2 → NOW the shard is unfenced.
        mgr.mark_complete(&t2);
        assert!(
            !mgr.is_shard_fenced(5),
            "shard should unfence once all fenced tasks are done"
        );
    }

    /// mark_complete must not clear a fence for an untracked task. Stale
    /// migration workers can report completion after a newer topology has
    /// installed a fresh fence for the same shard.
    #[test]
    fn mark_complete_does_not_unfence_when_task_not_found() {
        let mut mgr = MigrationManager::new();

        // Fence a shard manually without registering a task.
        mgr.fence_shard(99);
        assert!(mgr.is_shard_fenced(99));

        let phantom = MigrationTask {
            shard: 99,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.mark_complete(&phantom);
        assert!(
            mgr.is_shard_fenced(99),
            "untracked completion must not clear a current fence"
        );
    }

    /// mark_failed must not clear a fence for an untracked task.
    #[test]
    fn mark_failed_does_not_unfence_when_task_not_found() {
        let mut mgr = MigrationManager::new();
        mgr.fence_shard(42);
        assert!(mgr.is_shard_fenced(42));

        let phantom = MigrationTask {
            shard: 42,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.mark_failed(&phantom);
        assert!(
            mgr.is_shard_fenced(42),
            "untracked failure must not clear a current fence"
        );
    }

    // -----------------------------------------------------------------------
    // Deep edge cases: inbound tracking precision
    // -----------------------------------------------------------------------

    /// Inbound tracking with multiple sources for the same shard: each
    /// source must be independently completable.
    #[test]
    fn inbound_multiple_sources_independent_completion() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 10,
            from_node: NodeId(2),
            to_node: NodeId(1),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 10,
            from_node: NodeId(3),
            to_node: NodeId(1),
            is_master: false,
        };

        let mut pop = std::collections::HashSet::new();
        pop.insert(10);
        mgr.start_outbound(&[t1, t2], NodeId(1), &pop);

        // Two inbound entries for shard 10.
        assert_eq!(mgr.inbound_count(), 2);
        assert!(mgr.has_pending_inbound(10));

        // Complete one source.
        mgr.mark_inbound_complete(10);
        assert_eq!(mgr.inbound_count(), 1);
        assert!(
            mgr.has_pending_inbound(10),
            "shard still has one pending source"
        );

        // Complete the second source.
        mgr.mark_inbound_complete(10);
        assert_eq!(mgr.inbound_count(), 0);
        assert!(!mgr.has_pending_inbound(10));
    }

    #[test]
    fn inbound_complete_all_clears_multi_source_shard() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 10,
            from_node: NodeId(2),
            to_node: NodeId(1),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 10,
            from_node: NodeId(3),
            to_node: NodeId(1),
            is_master: false,
        };
        let populated: std::collections::HashSet<u16> = [10u16].into_iter().collect();
        mgr.start_outbound(&[t1, t2], NodeId(1), &populated);

        mgr.mark_inbound_complete_all(10);
        assert_eq!(mgr.inbound_count(), 0);
        assert!(!mgr.has_pending_inbound(10));
    }

    /// start_outbound pre-registers inbound ownership even for empty shards.
    #[test]
    fn start_outbound_registers_empty_shards_for_inbound() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 10,
            from_node: NodeId(2),
            to_node: NodeId(1),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 20,
            from_node: NodeId(2),
            to_node: NodeId(1),
            is_master: true,
        };

        // Only shard 10 is populated.
        let mut pop = std::collections::HashSet::new();
        pop.insert(10);
        mgr.start_outbound(&[t1, t2], NodeId(1), &pop);

        // Both shards must be protected until the source proves completion.
        assert!(mgr.has_pending_inbound(10));
        assert!(
            mgr.has_pending_inbound(20),
            "empty shard 20 still needs an ownership fence"
        );
        assert_eq!(mgr.inbound_count(), 2);
    }

    /// Outbound serialize/restore round-trip preserves Streaming state
    /// with the correct snapshot_sequence, and skips Fenced tasks (which
    /// ARE serialized — this verifies both are preserved).
    #[test]
    fn outbound_serialize_preserves_all_active_states() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 1,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 2,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: true,
        };
        let t3 = MigrationTask {
            shard: 3,
            from_node: NodeId(1),
            to_node: NodeId(4),
            is_master: false,
        };
        mgr.start_outbound(
            &[t1.clone(), t2.clone(), t3.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        mgr.set_snapshot_sequence(&t1, 100); // Streaming
        mgr.mark_fenced(&t2, 200); // Fenced
        // t3 stays Preparing

        let data = mgr.serialize_outbound();
        let mut restored = MigrationManager::new();
        restored.restore_outbound(&data);

        assert_eq!(restored.active_count(), 3);
        let r1 = restored
            .active_migrations()
            .iter()
            .find(|p| p.shard == 1)
            .unwrap();
        assert_eq!(r1.state, MigrationState::Streaming);
        assert_eq!(r1.snapshot_sequence, 100);

        let r2 = restored
            .active_migrations()
            .iter()
            .find(|p| p.shard == 2)
            .unwrap();
        assert_eq!(r2.state, MigrationState::Fenced);
        assert_eq!(r2.fence_sequence, 200);

        let r3 = restored
            .active_migrations()
            .iter()
            .find(|p| p.shard == 3)
            .unwrap();
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
        let t = MigrationTask {
            shard: 10,
            from_node: NodeId(2),
            to_node: NodeId(1),
            is_master: true,
        };
        let mut pop = std::collections::HashSet::new();
        pop.insert(10);
        mgr.start_outbound(&[t], NodeId(1), &pop);
        assert_eq!(mgr.inbound_count(), 1);
        assert!(mgr.has_pending_inbound(10));
    }

    #[test]
    fn clear_pending_inbound_for_selected_shards() {
        let mut mgr = MigrationManager::new();
        let tasks = vec![
            MigrationTask {
                shard: 10,
                from_node: NodeId(2),
                to_node: NodeId(1),
                is_master: false,
            },
            MigrationTask {
                shard: 20,
                from_node: NodeId(3),
                to_node: NodeId(1),
                is_master: false,
            },
            MigrationTask {
                shard: 30,
                from_node: NodeId(4),
                to_node: NodeId(1),
                is_master: false,
            },
        ];
        mgr.start_outbound(&tasks, NodeId(1), &std::collections::HashSet::new());

        let mut clear = std::collections::HashSet::new();
        clear.insert(20u16);
        clear.insert(30u16);

        let removed = mgr.clear_pending_inbound_for_shards(&clear);
        assert_eq!(removed, 2);
        assert_eq!(mgr.pending_inbound_entries(), vec![(10, NodeId(2))]);
        assert!(mgr.has_pending_inbound(10));
        assert!(!mgr.has_pending_inbound(20));
        assert!(!mgr.has_pending_inbound(30));
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

    /// Pending inbound entries are never cleared just because time elapsed.
    #[test]
    fn clear_stale_inbound_keeps_old_pending_entries() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(10);
        mgr.mark_inbound_active(20);

        // Duration::ZERO must not drop an active migration fence.
        let removed = mgr.clear_stale_inbound(Duration::ZERO);
        assert_eq!(removed, 0);
        assert!(mgr.has_pending_inbound(10));
        assert!(mgr.has_pending_inbound(20));
        assert_eq!(mgr.inbound_count(), 2);
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
        let t1 = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: false,
        };
        mgr.start_outbound(
            &[t1.clone(), t2.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        mgr.mark_fenced(&t1, 100);
        mgr.mark_fenced(&t2, 200);
        assert!(mgr.is_shard_fenced(5));

        // Complete both tasks. mark_complete on t1 keeps the fence (t2 still
        // Fenced). mark_complete on t2 unfences (no other Fenced task).
        mgr.mark_complete(&t1);
        mgr.mark_complete(&t2);
        assert!(
            !mgr.is_shard_fenced(5),
            "both completed, should be unfenced"
        );

        // Re-fence for the dangerous scenario: both completed, then cleanup.
        mgr.fence_shard(5);
        // Simulate: re-add two completed tasks.
        mgr.start_outbound(
            &[t1.clone(), t2.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );
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
        assert!(
            !mgr.is_shard_fenced(5),
            "cleanup_completed must unfence shards with no remaining fenced tasks"
        );
    }

    #[test]
    fn cleanup_completed_clears_orphaned_fences_when_no_tasks_remain() {
        let mut mgr = MigrationManager::new();
        mgr.fence_shard(42);

        mgr.cleanup_completed();

        assert!(
            !mgr.is_shard_fenced(42),
            "cleanup_completed must clear ghost fence bits once no outbound tasks remain"
        );
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
            shard: 42,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: true,
        };
        let replica_task = MigrationTask {
            shard: 42,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: false,
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
        assert!(
            mgr.has_pending_inbound(42),
            "inbound should be active after data arrived"
        );

        // Simulate: replica task fails because shard is already committed.
        // The coordinator should call mark_inbound_complete BEFORE mark_failed.
        mgr.mark_inbound_complete(42);
        mgr.mark_failed(&replica_task);

        // Inbound must be cleared — writes should not be blocked.
        assert!(
            !mgr.has_pending_inbound(42),
            "inbound must be cleared when migration aborts for committed shard"
        );
    }

    #[test]
    fn pending_inbound_entries_excludes_completed_entries() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 10,
            from_node: NodeId(2),
            to_node: NodeId(1),
            is_master: true,
        };
        let t2 = MigrationTask {
            shard: 20,
            from_node: NodeId(3),
            to_node: NodeId(1),
            is_master: false,
        };
        let populated: std::collections::HashSet<u16> = [10u16, 20u16].into_iter().collect();

        mgr.start_outbound(&[t1, t2], NodeId(1), &populated);
        mgr.mark_inbound_complete(10);

        assert_eq!(mgr.pending_inbound_entries(), vec![(20u16, NodeId(3))]);
    }

    /// Phase 5: starting an outbound migration should bump the
    /// `migration_active` gauge; completing it should decrement back.
    ///
    /// `migration_metrics()` is a process-global singleton, so parallel
    /// tests that mutate the same gauge race on exact-equality checks.
    /// `MigrationManager`'s internal `self.active` Vec is the source of
    /// truth — the global gauge mirrors it. We verify the manager's
    /// internal book-keeping with exact assertions and the global gauge
    /// with delta-only assertions so the test is robust against parallel
    /// neighbours that also call `start_outbound` / `mark_complete`.
    #[test]
    fn migration_active_gauge_tracks_inflight_shards() {
        use crate::metrics::{MigrationMetrics, init_migration_metrics, migration_metrics};
        use std::sync::OnceLock;
        use std::sync::atomic::Ordering;

        static TEST_METRICS: OnceLock<MigrationMetrics> = OnceLock::new();
        let m_ref: &'static MigrationMetrics = TEST_METRICS.get_or_init(MigrationMetrics::new);
        init_migration_metrics(m_ref);
        let metrics = migration_metrics().expect("metrics installed");

        let mut mgr = MigrationManager::new();
        let task = MigrationTask {
            shard: 99,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let populated: std::collections::HashSet<u16> = std::iter::once(99u16).collect();

        // Source is self_id = NodeId(1), matches task.from_node, so this is
        // an outbound migration that should bump both the manager's
        // internal active list and the global gauge.
        let active_before = metrics.migration_active.load(Ordering::Relaxed);
        let entries_before = metrics.migration_entries_applied_total.get();

        mgr.start_outbound(std::slice::from_ref(&task), NodeId(1), &populated);

        // Internal state: exact — only one task was started by this manager.
        assert_eq!(
            mgr.active.len(),
            1,
            "manager must track exactly one active outbound migration",
        );
        // Global gauge: delta-only — neighbours may also be active.
        let active_after_start = metrics.migration_active.load(Ordering::Relaxed);
        assert!(
            active_after_start > active_before,
            "migration_active gauge must advance by ≥ 1 after start_outbound \
             (before={active_before}, after={active_after_start})",
        );

        // Transition through states + record progress.
        mgr.set_snapshot_sequence(&task, 42);
        mgr.record_progress(&task, 10, 1024);
        mgr.mark_fenced(&task, 50);
        mgr.mark_complete(&task);

        // After completion the manager's tracked task transitions to
        // `MigrationState::Complete`. `mark_complete` does not remove the
        // entry from `active` — that happens lazily in `cleanup_completed`.
        let completed = mgr
            .active
            .iter()
            .find(|p| p.shard == 99)
            .expect("manager must still track the completed task");
        assert!(
            matches!(completed.state, MigrationState::Complete),
            "completed task must be in state Complete, got {:?}",
            completed.state,
        );
        // Global gauge: net delta is 0 (one +1 from start_outbound, one -1
        // from mark_complete). Don't assert on the absolute value because
        // parallel tests can independently +1 / -1 the gauge in this
        // window. The internal state above is the deterministic check.
        let active_after_complete = metrics.migration_active.load(Ordering::Relaxed);
        assert!(
            active_after_complete <= active_after_start,
            "migration_active must not be higher after mark_complete than \
             after start_outbound (after_start={active_after_start}, \
             after_complete={active_after_complete})",
        );
        assert!(
            metrics.migration_entries_applied_total.get() - entries_before >= 10,
            "migration_entries_applied_total must advance by ≥ records migrated",
        );
    }

    // -----------------------------------------------------------------------
    // KeyDiagnosis: per-shard tracker projection used by OP_ADMIN_DIAGNOSE_KEY
    // -----------------------------------------------------------------------

    /// `diagnose_key_routing` must reflect inbound and fence state for the
    /// requested shard, and must report cleanly for shards the tracker has
    /// never heard of.
    #[test]
    fn diagnose_key_routing_returns_tracker_state() {
        let mut mgr = MigrationManager::new();

        // Shard 5: pending inbound from some source.
        mgr.mark_inbound_active(5);
        // Shard 7: writes fenced (we are the source, baseline complete).
        mgr.fence_shard(7);
        // Also drive `is_migrating_shard` so we can verify it: start an
        // outbound active migration for shard 7 from this node's view.
        let task = MigrationTask {
            shard: 7,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        let d5 = mgr.diagnose_key_routing(5);
        assert_eq!(d5.shard, 5);
        assert!(d5.has_pending_inbound, "shard 5 should be pending inbound");
        assert!(!d5.is_shard_fenced, "shard 5 should not be fenced");

        let d7 = mgr.diagnose_key_routing(7);
        assert_eq!(d7.shard, 7);
        assert!(d7.is_shard_fenced, "shard 7 should be fenced");
        assert!(
            d7.is_migrating_shard,
            "shard 7 should be reported as actively migrating"
        );

        // Shard the tracker has never seen — every flag must be false.
        let d99 = mgr.diagnose_key_routing(99);
        assert_eq!(d99.shard, 99);
        assert!(!d99.has_pending_inbound);
        assert!(!d99.is_shard_fenced);
        assert!(!d99.is_migrating_shard);
    }

    // ── Phase C: subset/inbound tracking ───────────────────────────────────

    #[test]
    fn mark_inbound_complete_clears_subset() {
        let mut mgr = MigrationManager::new();
        let shard = 42u16;
        assert!(mgr.mark_inbound_active(shard));
        assert!(
            mgr.has_pending_inbound(shard),
            "inbound should be active before completion"
        );
        mgr.mark_inbound_complete_all(shard);
        assert!(
            !mgr.has_pending_inbound(shard),
            "inbound (subset proxy) must be cleared after mark_inbound_complete_all"
        );
    }

    // ── Phase E: dual-write window during migration ──────────────────────

    #[test]
    fn dual_write_window_starts_when_migration_starts() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask {
            shard: 42,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        let targets = mgr.dual_write_targets_for_shard(42);
        assert_eq!(
            targets,
            &[NodeId(2)],
            "dual-write window should include the new master after start_outbound",
        );

        mgr.mark_complete(&task);
        assert!(
            mgr.dual_write_targets_for_shard(42).is_empty(),
            "dual-write window must close on mark_complete",
        );
    }

    #[test]
    fn dual_write_window_collects_new_master_and_replicas() {
        let mut mgr = MigrationManager::new();
        let master_task = MigrationTask {
            shard: 7,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let replica_task = MigrationTask {
            shard: 7,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: false,
        };
        mgr.start_outbound(
            &[master_task.clone(), replica_task.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        let mut targets = mgr.dual_write_targets_for_shard(7).to_vec();
        targets.sort_by_key(|n| n.0);
        assert_eq!(
            targets,
            vec![NodeId(2), NodeId(3)],
            "dual-write window must include both new master and new replica destinations",
        );
    }

    #[test]
    fn dual_write_window_clears_on_mark_failed() {
        let mut mgr = MigrationManager::new();
        let task = MigrationTask {
            shard: 99,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        assert_eq!(mgr.dual_write_targets_for_shard(99), &[NodeId(3)]);

        mgr.mark_failed(&task);
        assert!(
            mgr.dual_write_targets_for_shard(99).is_empty(),
            "dual-write window must close on mark_failed (failure rolls back to old master)",
        );
    }

    #[test]
    fn dual_write_window_ignores_inbound_only_tasks() {
        let mut mgr = MigrationManager::new();
        let inbound = MigrationTask {
            shard: 11,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        // self_id == NodeId(2): this node is the destination, not source.
        mgr.start_outbound(
            std::slice::from_ref(&inbound),
            NodeId(2),
            &std::collections::HashSet::new(),
        );
        assert!(
            mgr.dual_write_targets_for_shard(11).is_empty(),
            "dual-write window only applies to outbound (source) side",
        );
    }

    // ── Phase G: outbound migration throttle ─────────────────────────────

    #[test]
    fn throttle_admits_under_cap() {
        let throttle = std::sync::Arc::new(MigrationThrottle::new(100_000));
        let token = throttle.try_admit(50_000);
        assert!(
            token.is_some(),
            "request under cap (50KB / 100KB) must be admitted",
        );
        assert_eq!(throttle.in_flight_bytes(), 50_000);
    }

    #[test]
    fn throttle_blocks_over_cap() {
        let throttle = std::sync::Arc::new(MigrationThrottle::new(100_000));
        let _t1 = throttle
            .try_admit(80_000)
            .expect("first admission under cap");
        assert_eq!(throttle.in_flight_bytes(), 80_000);
        let t2 = throttle.try_admit(50_000);
        assert!(
            t2.is_none(),
            "second request must be rejected when 80KB+50KB exceeds 100KB cap",
        );
        assert_eq!(
            throttle.in_flight_bytes(),
            80_000,
            "rejected request must not consume capacity",
        );
    }

    #[test]
    fn throttle_releases_on_token_drop() {
        let throttle = std::sync::Arc::new(MigrationThrottle::new(100_000));
        {
            let _t = throttle
                .try_admit(80_000)
                .expect("admit 80KB under 100KB cap");
            assert_eq!(throttle.in_flight_bytes(), 80_000);
        } // drop releases
        assert_eq!(
            throttle.in_flight_bytes(),
            0,
            "RAII drop must return capacity to the throttle",
        );
        let t2 = throttle.try_admit(80_000);
        assert!(
            t2.is_some(),
            "capacity must be re-admittable after the prior token is dropped",
        );
    }

    #[test]
    fn throttle_zero_byte_request_admits_without_consuming_capacity() {
        let throttle = std::sync::Arc::new(MigrationThrottle::new(100));
        let token = throttle.try_admit(0).expect("zero-byte admission is free");
        assert_eq!(throttle.in_flight_bytes(), 0);
        drop(token);
        assert_eq!(throttle.in_flight_bytes(), 0);
    }

    #[test]
    fn throttle_from_env_falls_back_on_missing_var() {
        // Note: env var manipulation in tests is not race-free across
        // parallel test threads; we serialize on this var by reading it
        // immediately after clearing.
        unsafe { std::env::remove_var(MigrationThrottle::ENV_VAR) };
        let t = MigrationThrottle::from_env();
        assert_eq!(t.cap_bytes(), MigrationThrottle::DEFAULT_CAP_BYTES);
    }
}
