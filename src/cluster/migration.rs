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

    /// Set the bit for `shard`. A shard outside `0..NUM_SHARDS` is a no-op
    /// (see [`Self::pos`]).
    pub fn set(&mut self, shard: u16) {
        if let Some((w, b)) = Self::pos(shard) {
            self.words[w] |= 1u64 << b;
        }
    }

    /// Clear the bit for `shard`. A shard outside `0..NUM_SHARDS` is a no-op.
    pub fn clear(&mut self, shard: u16) {
        if let Some((w, b)) = Self::pos(shard) {
            self.words[w] &= !(1u64 << b);
        }
    }

    /// Test whether `shard` is set. A shard outside `0..NUM_SHARDS` is never
    /// set.
    pub fn test(&self, shard: u16) -> bool {
        Self::pos(shard).is_some_and(|(w, b)| (self.words[w] >> b) & 1 == 1)
    }

    /// Clear all bits.
    pub fn clear_all(&mut self) {
        self.words = [0u64; Self::WORDS];
    }

    /// Number of set bits.
    pub fn count(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// W13 — `None` for a shard outside `0..NUM_SHARDS`. The shard reaches
    /// these bitmaps from the wire (`request_id`-encoded shard ids on the
    /// migration opcodes) and `words` is a fixed `NUM_SHARDS / 64` array, so an
    /// unchecked index panicked the connection thread on a malformed frame.
    fn pos(shard: u16) -> Option<(usize, u32)> {
        if shard as usize >= NUM_SHARDS {
            return None;
        }
        Some(((shard as usize) / 64, (shard as u32) % 64))
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

    /// Set the bit for `shard` (lock-free). Out-of-range: no-op.
    pub fn set(&self, shard: u16) {
        if let Some((w, b)) = Self::pos(shard) {
            self.words[w].fetch_or(1u64 << b, std::sync::atomic::Ordering::Release);
        }
    }

    /// Clear the bit for `shard` (lock-free). Out-of-range: no-op.
    pub fn clear(&self, shard: u16) {
        if let Some((w, b)) = Self::pos(shard) {
            self.words[w].fetch_and(!(1u64 << b), std::sync::atomic::Ordering::Release);
        }
    }

    /// Test whether `shard` is set (lock-free, no contention). A shard outside
    /// `0..NUM_SHARDS` is never set.
    pub fn test(&self, shard: u16) -> bool {
        Self::pos(shard).is_some_and(|(w, b)| {
            (self.words[w].load(std::sync::atomic::Ordering::Acquire) >> b) & 1 == 1
        })
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

    /// W13 — `None` outside `0..NUM_SHARDS`; see [`ShardBitmap::pos`]. This is
    /// the one the `OP_REPLICA_BATCH` migration path reached with an unchecked
    /// wire shard (`inbound_bitmap().test(shard)`), panicking on
    /// `words: [AtomicU64; 64]` for any shard >= 4096.
    fn pos(shard: u16) -> Option<(usize, u32)> {
        if shard as usize >= NUM_SHARDS {
            return None;
        }
        Some(((shard as usize) / 64, (shard as u32) % 64))
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
    /// The master this node is actually SERVING the shard from, per its
    /// local table's effective assignment (F7). During a two-phase handoff
    /// this stays the pre-handoff owner until the handoff commits, so it can
    /// differ from `local_view_canonical_master_id` (the target assignment)
    /// — the serving-vs-target split CI run diagnostics previously could not
    /// distinguish.
    pub local_view_effective_master_id: u64,
    /// True iff the lock-free per-shard serving fence is up on this node —
    /// the same `inbound_atomic` bit `is_master` reads to answer
    /// `Transitioning` instead of `Yes` (F7). Unlike `has_pending_inbound`
    /// (the migration tracker's view), this is the bit that actually gates
    /// serving.
    pub is_serving_fenced: bool,
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
    /// W4 review P1 — drive-attempt generation stamp. A fresh value from the
    /// manager's monotonic counter is assigned whenever a driver takes (or
    /// re-takes) ownership of this entry: `start_outbound` on registration,
    /// `retry_failed` on the failed-task re-drive, and `restore_outbound` on
    /// boot restore. A batch captures its tasks' stamps at spawn
    /// (`capture_task_attempts`) and its end-of-batch abandoned sweep parks
    /// ONLY entries whose stamp still matches — an entry re-driven in place
    /// by the NodeJoined retry (same identity, same epoch, no new entry)
    /// carries a newer stamp and is left to its new driver. Process-local
    /// coordination state: NOT serialized by `serialize_outbound` (restored
    /// entries are re-stamped, and no pre-restart capture survives a boot).
    pub attempt: u64,
    /// W16 review P1-1 — the [`Self::attempt`] generation that stamped the
    /// CURRENT `snapshot_sequence`, or `None` when no live reader holds a redo
    /// read position for this entry.
    ///
    /// `snapshot_sequence` alone cannot answer "is a reader holding the redo log
    /// at this position right now": [`MigrationManager::retry_failed`] flips a
    /// parked entry back to `Streaming` and bumps `attempt` but LEAVES the
    /// previous attempt's `snapshot_sequence` in place, and
    /// [`MigrationManager::take_failed_tasks`] does that for every parked entry
    /// at once. A state-only holder test would therefore re-arm N
    /// permanently-unsatisfiable floors (sequences already below the log's
    /// earliest surviving entry) the instant the retry queue drains, and hold
    /// them for the whole re-drive latency — pool queueing plus connect ladders.
    /// Keying the hold to the attempt makes every re-drive invalidate it
    /// automatically, whatever the state machine does.
    ///
    /// Cleared by [`MigrationManager::release_delta_reader_hold`] as soon as
    /// Phase 3 has read the window (review P2-2), so the manifest fold and the
    /// completion handshake do not keep pinning the log.
    ///
    /// Process-local like `attempt`, and likewise NOT serialized by
    /// `serialize_outbound`: a restored entry gets `None`, so nothing carried
    /// across a boot can hold the floor.
    pub snapshot_hold_attempt: Option<u64>,
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
            attempt: 0,
            snapshot_hold_attempt: None,
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
    /// C8 — set when the settled-inbound GC reaps this entry as an orphan
    /// (its source died mid-migration and there is no completion handshake)
    /// WITHOUT a completeness proof. A `lost` entry stays in
    /// `inbound_migrations` with its `inbound_bitmap` fence bit SET, so the
    /// shard is never served as full authority — it is marked UNAVAILABLE
    /// (client-invisible) rather than cleared-to-Serving. The mark is
    /// cleared only by a genuine completeness proof (`mark_inbound_complete*`,
    /// which flips `completed` and thereby drops the `lost` predicate) or by
    /// a fresh migration re-acquiring the shard (`add_inbound` /
    /// `register_inbound_source`). Not serialized: a restart re-loads the
    /// entry as plain-pending (still fenced); the GC re-marks it lost after
    /// the next idle window — the fence is what matters and it survives.
    lost: bool,
    /// W12 TAIL 2 — set when [`MigrationManager::drop_refused_inbound`] was
    /// told by this entry's own `from_node` that it will NEVER satisfy the
    /// entry (`ERR_MIGRATION_NO_TASKS`) but the fail-closed guard
    /// (`inbound_entry_must_be_kept`) RETAINED it anyway, because the shard
    /// still has local records that would become readable on a non-holder if
    /// the fence came down.
    ///
    /// Such an entry is a fixpoint, not a transfer: the source has closed the
    /// only channel that could complete it, and the records can only be
    /// reclaimed by the (committed-handoff-gated) orphan cleanup. Armed
    /// scenario 08 @ fc5e5f7 held two of them for 300 s while `inbound_pending`
    /// reported them as live migration work. The mark exists so status,
    /// metrics and the log can name the condition for what it is instead of
    /// hiding it inside a count that means "data on its way".
    ///
    /// Cleared wherever `lost` is cleared for a RE-REGISTRATION (a fresh
    /// expectation of a real transfer for the same shard/source). Not
    /// serialized: like `transfer_requested_at` this is process-local, and the
    /// refusal round that produced it re-derives it within one
    /// `TRANSFER_REQUEST_INTERVAL` after a restart.
    ///
    /// W16 — also set on a [`InboundRetention::KeepHolder`] entry, but only
    /// after `terminal_after_rounds` CONSECUTIVE refusal rounds (see
    /// `refusal_rounds`). The holder arm is deliberately slow to reclassify:
    /// one refusal there is weak evidence (a diverged source table), a
    /// sustained streak is not.
    refused_by_source: bool,
    /// W16 — how many CONSECUTIVE [`MigrationManager::drop_refused_inbound`]
    /// rounds have answered `ERR_MIGRATION_NO_TASKS` for this entry.
    ///
    /// Only the [`InboundRetention::KeepHolder`] arm needs the count: the
    /// other two arms decide on the first refusal (`Drop` retires the entry,
    /// `KeepOrphan` marks it). A holder's inbound is satisfiable work whose
    /// source may merely be answering from a table that
    /// `terminally_abort_unshippable_task` diverged without a version bump, so
    /// it is given `terminal_after_rounds` rounds — long enough for the re-heal
    /// to re-plan the handoff — before the refusal is believed.
    ///
    /// Reset to zero wherever `refused_by_source` is cleared (see
    /// [`InboundMigration::clear_refusal`]): any evidence of real work — a
    /// batch arriving, a task registration, a re-registration, a re-park, or
    /// the source MATCHING the request in a later round — restarts the count,
    /// so "consecutive" means exactly that.
    ///
    /// Process-local, not serialized (like `transfer_requested_at`): a restart
    /// re-derives the streak from scratch, which is the conservative direction
    /// — the entry counts as in-flight again for another
    /// `terminal_after_rounds` rounds.
    refusal_rounds: u32,
    /// P0 (reverse-heal Phase 2c) — the no-serve-before-heal FENCE marker.
    ///
    /// Set when this inbound entry represents a boot reverse-heal PULL
    /// ([`Self::register_heal_source`]) or the no-source FAIL-CLOSED fence
    /// ([`Self::mark_heal_fence_active`]). A mid-heal shard is an unproven
    /// inbound — the same "fence-until-proven" intent as `lost` — but it is
    /// NOT `lost` in the C8 sense: its source is alive and being pulled, so it
    /// must NOT be reaped by the settled-inbound GC, must NOT raise the
    /// `migration_lost` gauge, and must NOT answer `is_shard_lost`.
    ///
    /// A DISTINCT marker (rather than overloading `lost`) is used precisely so
    /// those three compose cleanly: the GC skip
    /// ([`Self::orphaned_inbound_shards`]) and the
    /// `lost`-scoped counters all gate on `lost`, untouched, while
    /// [`Self::clear_inbound`] retains BOTH `lost` and `heal_pending` so a
    /// runtime topology commit cannot wipe the reverse-heal fence and serve an
    /// un-healed (lost/resurrected) tail as authority (the P0 double-spend).
    /// The pull requester loop keys off `pending_inbound_entries` (which gates
    /// only on `!completed`), so the marker never stops the pull. Cleared when
    /// the completion handshake proves the shard complete.
    ///
    /// Not serialized (like `lost`): a restart re-loads the entry as
    /// plain-pending (still fenced via the bitmap); the boot reverse-heal path
    /// re-detects the stale shard and re-raises `heal_pending`, and the GC
    /// re-marks a genuinely-orphaned entry `lost` — the fence is what matters
    /// and it survives.
    heal_pending: bool,
    /// Reverse-heal Phase 3c — wall-clock instant at which this entry's
    /// `heal_pending` fence was (last) raised, or `None` when it is not a heal
    /// fence. Drives the fenced-heal DEADLINE ([`Self::expired_heal_shards`]): a
    /// heal that has held the fence past the configured deadline without
    /// completing is judged STUCK and escalated (or alert-held).
    ///
    /// Process-local timing state (NOT serialized), exactly like
    /// `transfer_requested_at`: a restart re-loads the entry as plain-pending and
    /// the boot reverse-heal re-raises `heal_pending` with a FRESH timer. Resetting
    /// the deadline clock on restart is conservative-safe — it grants another full
    /// window before escalation rather than tripping instantly on a slow restart.
    heal_started_at: Option<std::time::Instant>,
}

/// W12 TAIL 2 — WHY a pending inbound entry is (or is not) retained by the
/// fail-closed judgement `inbound_entry_retention`.
///
/// The two KEEP arms were a single `bool` and that conflation is a defect:
/// they are opposite states that happen to share a disposition.
///
/// * [`Self::KeepHolder`] — this node IS the shard's target holder. The
///   inbound is LEGITIMATE, satisfiable work. A source can still refuse it
///   spuriously: `terminally_abort_unshippable_task` rewrites the source's
///   `assignments[shard]` WITHOUT bumping the table version, so the source
///   judges holder-ness against a diverged table while both sides' epoch
///   checks pass, and answers `ERR_MIGRATION_NO_TASKS`. The re-heal machinery
///   re-plans that handoff on a later round, so the entry must keep counting
///   as in-flight work.
/// * [`Self::KeepOrphan`] — this node is NOT a holder but still carries
///   records for the shard. Dropping the fence would expose those orphans to
///   local reads (a record readable on a non-holder counts as an extra
///   holder — the scenario-17 three-holder bug). This is the armed-08 shape:
///   nothing will ever be sent and only the committed-handoff-gated orphan
///   cleanup can remove what holds the fence up.
/// * [`Self::Drop`] — a non-holder with zero local records: the
///   stranded-forever case. Nothing will be sent and nothing is hidden.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundRetention {
    Drop,
    KeepHolder,
    KeepOrphan,
}

impl InboundRetention {
    /// Whether this judgement retains the entry (either KEEP arm).
    pub fn keeps(self) -> bool {
        !matches!(self, Self::Drop)
    }
}

/// W12 TAIL 2 — what one [`MigrationManager::drop_refused_inbound`] call did,
/// scoped to THAT refusal (source + named shards).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusedInboundOutcome {
    /// Entries retired because nothing will be sent and nothing is hidden.
    pub dropped: usize,
    /// Shards retained because THIS NODE IS THE TARGET HOLDER and the refusal
    /// streak has NOT yet reached `terminal_after_rounds` — still treated as
    /// satisfiable work a later re-heal round can complete. NOT marked
    /// refused.
    pub kept_holder: Vec<u16>,
    /// W16 — shards retained on the same HOLDER ground, but whose source has
    /// now refused them for `terminal_after_rounds` CONSECUTIVE rounds. Marked
    /// [`InboundMigration::refused_by_source`]; disjoint from `kept_holder`.
    ///
    /// The entry is NOT dropped and the shard stays FENCED — this node holds
    /// an unproven copy and serving it would be the un-healed-authority P0.
    /// What changes is only the classification: a fixpoint stops being
    /// reported as a transfer in flight.
    pub kept_holder_terminal: Vec<u16>,
    /// Shards retained because local orphan records would otherwise become
    /// readable on a non-holder. Marked
    /// [`InboundMigration::refused_by_source`].
    pub kept_orphan: Vec<u16>,
}

impl InboundMigration {
    /// A freshly-registered, not-yet-received inbound entry (fence up, not
    /// completed, no outstanding transfer request, not lost, not heal-pending).
    fn pending(shard: u16, from_node: NodeId) -> Self {
        Self {
            shard,
            from_node,
            completed: false,
            transfer_requested_at: None,
            lost: false,
            refused_by_source: false,
            refusal_rounds: 0,
            heal_pending: false,
            heal_started_at: None,
        }
    }

    /// W16 — forget everything the refusal machinery knows about this entry:
    /// the terminal mark AND the consecutive-refusal streak behind it.
    ///
    /// Called from every site that has evidence the entry is live work again.
    /// Clearing the mark without clearing the streak would be a trap: the
    /// entry would re-acquire the mark on the very next refusal instead of
    /// being given the full `terminal_after_rounds` window again.
    fn clear_refusal(&mut self) {
        self.refused_by_source = false;
        self.refusal_rounds = 0;
    }
}

/// #74 (re-review, F1×F4) — persisted inbound-entry flag: the entry is a
/// reverse-heal fence (a concrete-source heal pull or a parked `NodeId(0)`
/// no-source fence). Restores with `heal_pending = true` and a fresh Phase-3c
/// deadline clock, so a park stays a durable, F4-protected park across a
/// restart while a forward entry (flag clear) restores completable and
/// supersede-droppable exactly as before persistence.
const INBOUND_ENTRY_FLAG_HEAL_PENDING: u8 = 1 << 0;

/// #74 (re-review) — persisted inbound-entry flag: the C8 LOST
/// (unavailable-until-proven) mark. Restores with `lost = true` so the
/// fence-until-proven posture survives a restart instead of silently
/// downgrading to an ordinary droppable pending entry.
const INBOUND_ENTRY_FLAG_LOST: u8 = 1 << 1;

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
    /// Default cap (32 MiB) — matches the reference UDF's `MAX_BYTES_EMIGRATING`.
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

/// Why a Phase E dual-write window was opened — see
/// [`MigrationManager::dual_write_targets_with_origin_for_shard`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DualWriteOrigin {
    /// A handoff: `to_node` is a NEW-SIDE holder that is becoming
    /// authoritative for the shard.
    Handoff,
    /// A Phase-H resync backfill: `to_node` is an EXISTING holder being
    /// repaired. No ownership transition happens.
    Resync,
}

/// How one outbound handoff of a shard to ONE target node resolved, stamped
/// with the topology epoch it resolved at.
///
/// Recorded per `(shard, target)` in [`MigrationManager::handoff_outcomes`] and
/// read by the #28 data-loss guard ([`MigrationManager::has_committed_handoff`]).
/// The last outcome for a given target WINS: a re-drive that finally commits
/// supersedes its own earlier abort, which is what keeps the retry path from
/// wedging the shed shut.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HandoffOutcome {
    /// The master move was written to the shard table after the target passed
    /// the count+manifest handshake — positive evidence the data is durably
    /// installed elsewhere.
    Committed(u64),
    /// The transfer TERMINALLY aborted (`terminally_abort_unshippable_task`):
    /// the target refused a record this node was shipping, so the SOURCE keeps
    /// authority. Negative evidence — this node may hold the only copy of at
    /// least one of the shard's records.
    Aborted(u64),
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
    /// W10 composition (P1-2) — the subset of [`Self::dual_write_targets`]
    /// entries opened by a REPAIR (Phase-H resync backfill) rather than by a
    /// handoff. Repair targets still receive the dual-write fan-out, but they
    /// are NOT new-side handoff holders, so the per-shard "≥1 ACK from the
    /// new side" write invariant must not name them. See
    /// [`Self::dual_write_targets_with_origin_for_shard`].
    resync_dual_write: std::collections::HashMap<u16, std::collections::HashSet<NodeId>>,
    /// Data-loss guard (task #28): the outcome of every outbound handoff this
    /// node has RESOLVED, keyed by `(shard, target)` and epoch-stamped. Orphan
    /// cleanup consults it through [`Self::has_committed_handoff`].
    ///
    /// Stored as `shard -> [(target, outcome)]` rather than a flat
    /// `(shard, target)` key because the consumer asks a PER-SHARD question
    /// ("may I shed shard S?") once per shard across all
    /// [`crate::cluster::shards::NUM_SHARDS`] — a flat map would make that
    /// sweep quadratic. Targets per shard are bounded by the replication
    /// factor plus in-flight re-homes, so the inner scan is a handful of
    /// entries.
    ///
    /// W15 — the map used to be keyed by SHARD ALONE, holding only committed
    /// handoffs. One committed handoff of shard S at epoch E then authorized
    /// deleting EVERY local copy of S at E, including copies a second,
    /// DISTINCT handoff of the same shard had just proved were still needed:
    /// CI run 32637576348 scenario 05 aborted the handoff of shard 959 to
    /// node3 (*"source keeps authority"*) and deleted node1's two copies ten
    /// seconds later anyway, ending the run with those records held by NO node.
    ///
    /// A shard that became non-owned WITHOUT a committed handoff from this node
    /// (e.g. the topology advanced while an in-flight handoff was discarded as
    /// stale) is deliberately ABSENT, so cleanup retains its last copy rather
    /// than stranding it. Cleared wholesale when this node re-acquires the
    /// shard as an inbound migration target.
    handoff_outcomes: std::collections::HashMap<u16, Vec<(NodeId, HandoffOutcome)>>,
    /// W4 review P1 — monotonic source for [`MigrationProgress::attempt`]
    /// stamps. Bumped once per stamp so every (re-)drive of an entry gets a
    /// unique generation; starts at 1 so a stamp of 0 (the `from_task`
    /// default, never handed to a batch capture) matches nothing.
    next_attempt: u64,
    /// W17 — monotonic count of FORWARD advances of this node's outbound
    /// migration pipeline. See [`Self::pipeline_advances`].
    pipeline_advances: u64,
    /// W8 — pending self-retry arm set by a migration-batch disposition that
    /// ended with failures at a still-current epoch
    /// (`run_migration_batch`'s `f > 0` disposition, which runs on a worker
    /// thread) and drained by the coordinator event loop, which arms the
    /// event-repair trigger and the delayed failed-task re-drive from it.
    /// Transient routing state only: never persisted (the durable retry
    /// queue is the `Failed` entries themselves), reset naturally on
    /// restart, and coalescing by design — multiple failed dispositions
    /// before a drain collapse into one arm.
    failed_batch_retry_arm: bool,
    /// W9 — pending resync arm set by a REPLICA-side terminal abort
    /// (`terminally_abort_unshippable_task`, which retires the task WITHOUT
    /// touching the shard table, so diff-based re-heal never re-plans the
    /// fill) and drained by the coordinator event loop, which FORCE-arms the
    /// event-repair trigger — deliberately NOT gated on
    /// `under_replication_sweep_enabled`, because in sweep-off clusters this
    /// signal is the ONLY driver that retries the fill (with the W9
    /// CompensatedCreate tombstone the retry now succeeds instead of
    /// re-vetoing). Same transient/coalescing shape as
    /// [`Self::failed_batch_retry_arm`]: never persisted, reset on restart.
    replica_abort_resync_arm: bool,
    /// W9 Part B (review P1-2c) — whether a replica-side terminal abort may
    /// leave the resync arm at all (see `Config`
    /// `replica_abort_forced_resync_enabled`; default ON). Set once at
    /// coordinator construction; carried here so the abort site (a free fn
    /// with no config access) reads it lock-local, mirroring
    /// [`Self::vetoed_reduction_enabled`]. Never persisted.
    replica_abort_forced_resync_enabled: bool,
    /// W8 review P0-2 — while set, [`Self::cleanup_completed`] PRESERVES
    /// `Failed` entries (delegating to
    /// [`Self::cleanup_completed_keep_failed`]) so the durable retry queue
    /// actually survives until the delayed re-drive's backoff elapses.
    /// Without it the coordinator event loop's periodic prune deleted the
    /// Failed entries ~100ms after the disposition parked them, making the
    /// entire self-retry (and the historical NodeJoined re-drive on a busy
    /// loop) an empty-drain no-op.
    ///
    /// Lifecycle: set with the arm at the failed disposition
    /// ([`Self::arm_failed_batch_retry`]); cleared when
    /// [`Self::take_failed_tasks`] drains the queue (the entries become
    /// live Streaming re-drives) and by the topology activation's stale-task
    /// cancel ([`Self::clear_failed_retry_state`]) — the epoch fence: a new
    /// plan owns its own retries, so superseded Failed entries must reap
    /// exactly as before. Transient, never persisted.
    failed_retry_hold: bool,
    /// W8 review P0-1 — whether the migration source may REDUCE a completion
    /// manifest by tombstone-vetoed keys (see `Config`
    /// `migration_vetoed_reduction_enabled` for the rationale; default OFF).
    /// Set once at coordinator construction; carried here so the batch
    /// migration path reads it without signature plumbing. Never persisted.
    vetoed_reduction_enabled: bool,
    /// W10 review P2-6 — whether weak-veto ARBITRATION is armed on this node
    /// (see `Config` `migration_weak_veto_arbitration_enabled`; default ON).
    /// Set once at coordinator construction; gates both the source-side
    /// escalation and the target-side handler. Never persisted.
    weak_veto_arbitration_enabled: bool,
    /// W13 containment — whether the PROOF-OF-ELSEWHERE orphan reclaim is
    /// armed on this node (see `Config` `orphan_cleanup_proof_reclaim_enabled`
    /// for the data-loss rationale; default OFF). Set once at coordinator
    /// construction; carried here so `run_orphan_cleanup` (a free fn with no
    /// config access) reads it lock-local, mirroring
    /// [`Self::vetoed_reduction_enabled`]. Never persisted.
    orphan_cleanup_proof_reclaim_enabled: bool,
    /// GAP 1 (armed scenario 06) — per-shard streak of CONSECUTIVE code-22
    /// "manifest hash mismatch (count matched)" completion rejections, keyed
    /// by the rejected manifest's hash. Lives on the manager (not the batch
    /// worker) because each failed-task re-drive re-enters
    /// `run_migration_batch` fresh and rebuilds the identical fence-time
    /// manifest — the streak is the only way the source can prove "this
    /// exact manifest was already rejected" across invocations and escalate
    /// to a record-level re-sync instead of retrying forever. A different
    /// hash restarts the streak (the content changed, so a plain retry is
    /// meaningful again). Transient routing state: never persisted, reset
    /// naturally on restart (a restarted source re-streams the baseline
    /// anyway).
    manifest_mismatch_streaks: std::collections::HashMap<u16, ([u8; 32], u32)>,
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
            resync_dual_write: std::collections::HashMap::new(),
            handoff_outcomes: std::collections::HashMap::new(),
            next_attempt: 1,
            pipeline_advances: 0,
            failed_batch_retry_arm: false,
            replica_abort_resync_arm: false,
            replica_abort_forced_resync_enabled: true,
            failed_retry_hold: false,
            vetoed_reduction_enabled: false,
            weak_veto_arbitration_enabled: true,
            orphan_cleanup_proof_reclaim_enabled: false,
            manifest_mismatch_streaks: std::collections::HashMap::new(),
        }
    }

    /// GAP 1 — note a code-22 manifest-mismatch completion rejection for
    /// `shard` with the rejected manifest's hash; returns the streak of
    /// CONSECUTIVE rejections of this exact content (1 = first). An
    /// identical hash extends the streak; a different hash restarts it at 1.
    /// The completion path escalates to a record-level re-sync once the
    /// streak proves the identical manifest was already rejected (>= 2) —
    /// see `manifest_mismatch_streaks` for why this lives on the manager.
    pub fn note_completion_manifest_mismatch(
        &mut self,
        shard: u16,
        manifest_hash: &[u8; 32],
    ) -> u32 {
        let entry = self
            .manifest_mismatch_streaks
            .entry(shard)
            .or_insert((*manifest_hash, 0));
        if entry.0 == *manifest_hash {
            entry.1 = entry.1.saturating_add(1);
        } else {
            *entry = (*manifest_hash, 1);
        }
        entry.1
    }

    /// GAP 1 — drop `shard`'s manifest-mismatch streak: its completion
    /// verified, or its task was terminally aborted (a later re-plan starts
    /// with a fresh manifest and deserves fresh bookkeeping).
    pub fn clear_completion_manifest_mismatch(&mut self, shard: u16) {
        self.manifest_mismatch_streaks.remove(&shard);
    }

    /// W9 Part B — arm/disarm the replica-abort forced resync (default ON;
    /// set once at coordinator construction from
    /// `replica_abort_forced_resync_enabled`).
    pub fn set_replica_abort_forced_resync_enabled(&mut self, enabled: bool) {
        self.replica_abort_forced_resync_enabled = enabled;
    }

    /// W9 — record that a REPLICA-side migration task was terminally aborted
    /// at this node (the outbound source), leaving its shard under-RF with no
    /// re-planner. The coordinator event loop drains this and force-arms the
    /// event-repair pass (not gated on the sweep flag, but gated on the
    /// committed `replica_abort_forced_resync_enabled` policy — review
    /// P1-2c: a disabled policy records nothing, restoring the documented
    /// pre-W9 disposition). Idempotent / coalescing: repeated aborts before
    /// a drain collapse into one arm.
    pub fn arm_replica_abort_resync(&mut self) {
        if self.replica_abort_forced_resync_enabled {
            self.replica_abort_resync_arm = true;
        }
    }

    /// W9 — drain the pending replica-abort resync arm. Returns `true` exactly
    /// once per armed window ([`Self::arm_replica_abort_resync`]); subsequent
    /// calls return `false` until a new replica-side terminal abort arms again.
    pub fn take_replica_abort_resync_arm(&mut self) -> bool {
        std::mem::take(&mut self.replica_abort_resync_arm)
    }

    /// W8 review P0-1 — arm/disarm tombstone-vetoed manifest reduction for
    /// completions sourced from this node (default OFF; set once at
    /// coordinator construction from `migration_vetoed_reduction_enabled`).
    pub fn set_vetoed_reduction_enabled(&mut self, enabled: bool) {
        self.vetoed_reduction_enabled = enabled;
    }

    /// W8 review P0-1 — whether tombstone-vetoed manifest reduction is
    /// armed ([`Self::set_vetoed_reduction_enabled`]).
    pub fn vetoed_reduction_enabled(&self) -> bool {
        self.vetoed_reduction_enabled
    }

    /// W10 review P2-6 — arm/disarm weak-veto arbitration on this node
    /// (default ON; set once at coordinator construction from
    /// `migration_weak_veto_arbitration_enabled`).
    pub fn set_weak_veto_arbitration_enabled(&mut self, enabled: bool) {
        self.weak_veto_arbitration_enabled = enabled;
    }

    /// W10 review P2-6 — whether weak-veto arbitration is armed
    /// ([`Self::set_weak_veto_arbitration_enabled`]).
    pub fn weak_veto_arbitration_enabled(&self) -> bool {
        self.weak_veto_arbitration_enabled
    }

    /// W13 containment — arm/disarm the proof-of-elsewhere orphan reclaim on
    /// this node (default OFF; set once at coordinator construction from
    /// `orphan_cleanup_proof_reclaim_enabled`).
    ///
    /// DISARMED, `run_orphan_cleanup` never probes a holder and never deletes
    /// on the strength of a probe: a non-owned shard without committed-handoff
    /// evidence is retained, which is the fail-closed task-#28 posture. See
    /// `Config::orphan_cleanup_proof_reclaim_enabled` for why that is the
    /// shipped default.
    pub fn set_orphan_cleanup_proof_reclaim_enabled(&mut self, enabled: bool) {
        self.orphan_cleanup_proof_reclaim_enabled = enabled;
    }

    /// W13 containment — whether the proof-of-elsewhere orphan reclaim is
    /// armed ([`Self::set_orphan_cleanup_proof_reclaim_enabled`]).
    pub fn orphan_cleanup_proof_reclaim_enabled(&self) -> bool {
        self.orphan_cleanup_proof_reclaim_enabled
    }

    /// W13 review item 4 — drop every TRANSIENT migration state (inbound
    /// entries, outbound tasks, fences, dual-write windows, retry arms,
    /// mismatch streaks) while PRESERVING this node's config-carried policy
    /// flags.
    ///
    /// Use this wherever an activation previously did
    /// `*mgr = MigrationManager::new()`. That reverted every arming bit to its
    /// compile-time default with no log line and in both directions: a
    /// deliberately disarmed [`Self::weak_veto_arbitration_enabled`] (default
    /// ON) silently RE-ARMED, and a deliberately armed
    /// [`Self::orphan_cleanup_proof_reclaim_enabled`] (default OFF) silently
    /// disarmed. Arming is operator configuration, not in-flight work, so it
    /// survives the reset; a topology activation is not a config reload.
    ///
    /// Implemented as "rebuild, then restore the policy" so a newly added
    /// TRANSIENT field is cleared automatically.
    ///
    /// The classification is STRUCTURAL, not a hand-maintained list (W13
    /// round-2 review P2-2): the destructure below is EXHAUSTIVE — no `..`
    /// rest pattern — so adding any field to [`MigrationManager`] fails to
    /// compile here until its author classifies it as transient (matched and
    /// dropped) or policy (carried across). Silently dropping a new arming bit
    /// on every topology activation is exactly the failure this method exists
    /// to fix, so it must not be possible to reintroduce by omission.
    pub fn reset_transient_state(&mut self) {
        let (
            replica_abort_forced_resync_enabled,
            vetoed_reduction_enabled,
            weak_veto_arbitration_enabled,
            orphan_cleanup_proof_reclaim_enabled,
        ) = {
            let Self {
                // --- TRANSIENT: in-flight work, rebuilt by `Self::new()` ---
                active: _,
                inbound_migrations: _,
                inbound_bitmap: _,
                fenced_shards: _,
                dual_write_targets: _,
                resync_dual_write: _,
                handoff_outcomes: _,
                next_attempt: _,
                pipeline_advances: _,
                failed_batch_retry_arm: _,
                replica_abort_resync_arm: _,
                failed_retry_hold: _,
                manifest_mismatch_streaks: _,
                // --- POLICY: operator configuration, carried across ---
                replica_abort_forced_resync_enabled,
                vetoed_reduction_enabled,
                weak_veto_arbitration_enabled,
                orphan_cleanup_proof_reclaim_enabled,
            } = self;
            (
                *replica_abort_forced_resync_enabled,
                *vetoed_reduction_enabled,
                *weak_veto_arbitration_enabled,
                *orphan_cleanup_proof_reclaim_enabled,
            )
        };
        *self = Self::new();
        self.replica_abort_forced_resync_enabled = replica_abort_forced_resync_enabled;
        self.vetoed_reduction_enabled = vetoed_reduction_enabled;
        self.weak_veto_arbitration_enabled = weak_veto_arbitration_enabled;
        self.orphan_cleanup_proof_reclaim_enabled = orphan_cleanup_proof_reclaim_enabled;
    }

    /// W8 — record that a migration batch finished with failed tasks at a
    /// still-current epoch, so the coordinator event loop should arm the
    /// self-retry machinery (event-repair trigger + delayed failed-task
    /// re-drive). Also raises the retry HOLD (review P0-2): from here until
    /// [`Self::take_failed_tasks`] drains the queue (or the activation's
    /// stale-task cancel epoch-fences it away),
    /// [`Self::cleanup_completed`] preserves `Failed` entries so the queue
    /// survives the event loop's periodic prune. Idempotent: repeated arms
    /// before a drain coalesce.
    pub fn arm_failed_batch_retry(&mut self) {
        self.failed_batch_retry_arm = true;
        self.failed_retry_hold = true;
    }

    /// W8 — drain the pending failed-batch self-retry arm. Returns `true`
    /// exactly once per armed window ([`Self::arm_failed_batch_retry`]);
    /// subsequent calls return `false` until a new failed disposition arms
    /// again. Deliberately leaves the retry HOLD in place — the queue must
    /// keep surviving the periodic prune until the backoff elapses and
    /// [`Self::take_failed_tasks`] drains it.
    pub fn take_failed_batch_retry_arm(&mut self) -> bool {
        std::mem::take(&mut self.failed_batch_retry_arm)
    }

    /// W8 review P0-2 — drop the failed-batch retry hold AND any pending
    /// arm. Called by the topology activation's stale-task cancel just
    /// before its `cleanup_completed()`: the epoch fence. The new plan
    /// re-registers (and re-drives) everything it still wants, so `Failed`
    /// entries from the superseded plan must reap exactly as they always
    /// have — a hold surviving an activation would let a later
    /// `take_failed_tasks` resurrect stale-epoch tasks under the new epoch.
    pub fn clear_failed_retry_state(&mut self) {
        self.failed_batch_retry_arm = false;
        self.failed_retry_hold = false;
    }

    /// W4 review P1 — hand out the next drive-attempt generation stamp.
    fn bump_attempt(&mut self) -> u64 {
        let a = self.next_attempt;
        self.next_attempt += 1;
        a
    }

    /// Upsert the outcome of the `(shard, target)` handoff. The last outcome
    /// for a target replaces its predecessor — a re-drive that finally commits
    /// supersedes its own earlier abort, and vice versa.
    fn record_handoff_outcome(&mut self, shard: u16, target: NodeId, outcome: HandoffOutcome) {
        let entry = self.handoff_outcomes.entry(shard).or_default();
        match entry.iter_mut().find(|(node, _)| *node == target) {
            Some(slot) => slot.1 = outcome,
            None => entry.push((target, outcome)),
        }
    }

    /// Record that this node has COMMITTED-ly handed off `shard` to `target` as
    /// the outbound source at topology `epoch` (the master move was written to
    /// the shard table). This is the positive evidence orphan cleanup
    /// requires before deleting the shard's local records.
    ///
    /// Called from the migration-completion path only after `commit_shard`
    /// has transferred ownership to the new master.
    pub fn record_committed_handoff(&mut self, shard: u16, target: NodeId, epoch: u64) {
        self.record_handoff_outcome(shard, target, HandoffOutcome::Committed(epoch));
    }

    /// Record that this node's handoff of `shard` to `target` TERMINALLY
    /// aborted at topology `epoch` — the source keeps authority
    /// ([`crate::cluster::coordinator`]'s `terminally_abort_unshippable_task`).
    ///
    /// This is NEGATIVE evidence and it VETOES the shed of `shard` at `epoch`
    /// (see [`Self::has_committed_handoff`]). Only the TERMINAL disposition
    /// needs its own record: an ordinary failure
    /// (`fail_migration_task_current_epoch`) leaves a `Failed` tracking entry
    /// that both orphan-cleanup gates already refuse to reclaim under, whereas
    /// the terminal abort RETIRES the entry ([`Self::fail_and_retire_task`])
    /// and so erases the only trace those gates could see.
    pub fn record_aborted_handoff(&mut self, shard: u16, target: NodeId, epoch: u64) {
        self.record_handoff_outcome(shard, target, HandoffOutcome::Aborted(epoch));
    }

    /// Whether this node has positive committed-handoff evidence for `shard`
    /// that is STILL VALID at `current_epoch`, AND no handoff of `shard`
    /// terminally aborted at that epoch. Orphan cleanup deletes a non-owned
    /// shard ONLY when this returns true.
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
    ///
    /// # W15 — why one committed handoff is not enough
    ///
    /// The evidence used to be keyed by SHARD ALONE, so a single committed
    /// handoff of shard S at epoch E authorized deleting every local copy of S
    /// — including copies a second, DISTINCT handoff of the same shard had just
    /// proved the cluster still needs. That is the step that converted "one
    /// copy left" into "zero copies" in CI run 32637576348 scenario 05: shard
    /// 959's completion to node3 was rejected and terminally aborted at
    /// 12:00:04 (*"source keeps authority"*), and at 12:00:14 node1 deleted its
    /// two copies on the strength of an unrelated committed handoff of 959 at
    /// the same epoch.
    ///
    /// A terminal abort means the TARGET does not hold at least one record this
    /// node holds, so it vetoes the shed regardless of any sibling commit. Both
    /// directions stay live:
    ///
    /// * The veto is EPOCH-SCOPED exactly like the positive evidence, so it
    ///   cannot wedge the shed permanently — a fresh committed handoff at a
    ///   later epoch is judged on its own.
    /// * A re-drive whose completion the target VERIFIES retires that target's
    ///   abort ([`Self::clear_handoff_abort`]) whether or not the completion
    ///   commits, so the ordinary retry path re-opens the shed within the same
    ///   epoch. That the clear is separate from
    ///   [`Self::record_committed_handoff`] is load-bearing: a replica-only
    ///   completion from a node the new table makes a non-owner has
    ///   `should_commit == false`, so it never reaches the recorder — and it is
    ///   exactly the shape the veto bites on (review P2-1).
    ///
    /// # What is NOT an escape (review P2-1)
    ///
    /// The proof-of-elsewhere phase is NOT a general escape hatch. It is behind
    /// `run_orphan_cleanup`'s `proof_armed`, whose committed
    /// `orphan_cleanup_proof_reclaim_enabled` policy DEFAULTS TO FALSE, so in
    /// the shipped configuration it does not run at all; and
    /// `cleanup_orphaned_shard_if_settled` — the per-shard path this veto was
    /// written for — has no proof phase whatsoever. On shipped defaults a
    /// vetoed shard is retained until a verified completion, a re-acquisition,
    /// or an epoch bump resolves it. That is the intended trade, not an
    /// oversight.
    pub fn has_committed_handoff(&self, shard: u16, current_epoch: u64) -> bool {
        let Some(outcomes) = self.handoff_outcomes.get(&shard) else {
            return false;
        };
        let mut committed = false;
        for (_, outcome) in outcomes {
            match *outcome {
                // A same-epoch abort is decisive: the shed is vetoed even if a
                // sibling handoff of the same shard committed.
                HandoffOutcome::Aborted(epoch) if epoch == current_epoch => return false,
                HandoffOutcome::Committed(epoch) if epoch == current_epoch => committed = true,
                // Stale-epoch outcomes (either kind) neither authorize nor veto.
                _ => {}
            }
        }
        committed
    }

    /// Whether a handoff of `shard` TERMINALLY ABORTED at `current_epoch` —
    /// i.e. [`Self::has_committed_handoff`] is false because the shed is
    /// VETOED, not merely because no evidence was ever earned.
    ///
    /// The two are materially different states and the census must be able to
    /// tell them apart (W11 FIX 4(b)'s rule: a retention must be attributable
    /// to the gate that caused it). "No evidence" is the SIGKILL /
    /// never-handed-off case that the proof-of-elsewhere phase exists to earn
    /// its way out of; an abort veto is positive knowledge that a target
    /// refused a record this node holds, and no probe should talk it out of
    /// that.
    pub fn has_aborted_handoff(&self, shard: u16, current_epoch: u64) -> bool {
        self.handoff_outcomes.get(&shard).is_some_and(|outcomes| {
            outcomes
                .iter()
                .any(|(_, o)| matches!(*o, HandoffOutcome::Aborted(e) if e == current_epoch))
        })
    }

    /// Retire any ABORT record for `(shard, target)`, leaving a `Committed`
    /// record for that target untouched and never creating one.
    ///
    /// Called on a completion the TARGET VERIFIED (count+manifest handshake
    /// passed), which is the moment the abort's negative evidence becomes
    /// obsolete: the target has just confirmed it holds what this node
    /// streamed. Deliberately separate from
    /// [`Self::record_committed_handoff`] — see the P2-1 note on
    /// [`Self::has_committed_handoff`] for why a replica-only completion, which
    /// never commits and so never records evidence, must still be able to
    /// retire the veto.
    ///
    /// Only an `Aborted` slot is removed. Erasing a `Committed` slot here would
    /// silently withdraw positive evidence and strand the shard.
    pub fn clear_handoff_abort(&mut self, shard: u16, target: NodeId) {
        let Some(entry) = self.handoff_outcomes.get_mut(&shard) else {
            return;
        };
        entry.retain(|(node, outcome)| {
            *node != target || !matches!(outcome, HandoffOutcome::Aborted(_))
        });
        if entry.is_empty() {
            self.handoff_outcomes.remove(&shard);
        }
    }

    /// Drop every recorded handoff outcome for `shard`, committed and aborted
    /// alike.
    ///
    /// Two callers, both meaning "everything previously known about this
    /// shard's transfers is superseded":
    ///
    /// * this node RE-ACQUIRES the shard as an inbound migration target
    ///   ([`Self::start_outbound`]), so a stale committed record cannot
    ///   authorize deleting freshly re-homed data and a stale abort cannot
    ///   block a shed the fresh migration is entitled to authorize;
    /// * the periodic prune REAPS a `Failed` outbound entry for the shard
    ///   ([`Self::cleanup_completed`]) — see there for why.
    pub fn clear_handoff_outcomes(&mut self, shard: u16) {
        self.handoff_outcomes.remove(&shard);
    }

    /// Phase E: NodeIds (new master + new replicas) that must additionally
    /// receive replica batches for `shard` while it is migrating outbound
    /// from this node. Returns an empty slice when no dual-write window is
    /// active for the shard.
    ///
    /// ORIGIN-BLIND — do NOT use this on the replication path. It cannot tell
    /// a new-side handoff holder from a Phase-H repair destination, and
    /// treating the two alike is exactly how W10 P1-2 (a repair turning into
    /// a mandatory per-shard write ACK) happened. Use
    /// [`Self::dual_write_targets_with_origin_for_shard`] there; this stays
    /// for the plain "who is in the window" question (fan-out membership,
    /// admin/tests).
    pub fn dual_write_targets_for_shard(&self, shard: u16) -> &[NodeId] {
        self.dual_write_targets
            .get(&shard)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Phase E dual-write targets for `shard`, each paired with whether it is
    /// a NEW-SIDE HANDOFF holder (`true`) or a repair-only backfill
    /// destination (`false`).
    ///
    /// W10 composition (P1-2): the replication path turns every new-side
    /// handoff target into a hard per-shard write requirement ("≥1 ACK from
    /// this shard's new side"). That invariant protects a MASTERSHIP /
    /// holder-set TRANSITION — the target is about to become authoritative,
    /// so it must observe the writes that land during the window. A Phase-H
    /// resync backfill performs no transition: it is a repair toward a node
    /// the committed table ALREADY names as a holder (so it is already in the
    /// regular quorum set and governed by the normal ACK policy), and its
    /// task is `is_master = false` — nothing commits. Counting it as a
    /// handoff target promoted "R must ACK" into a per-shard WriteAll for the
    /// life of the backfill; a `shards: []` "I don't know" resync expands to
    /// EVERY shard R owns, so every client write to any of them would require
    /// an ACK from the very node the repair is catching up.
    pub fn dual_write_targets_with_origin_for_shard(&self, shard: u16) -> Vec<(NodeId, bool)> {
        let repair = self.resync_dual_write.get(&shard);
        self.dual_write_targets
            .get(&shard)
            .map(|v| {
                v.iter()
                    .map(|n| (*n, !repair.is_some_and(|r| r.contains(n))))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn dual_write_add(&mut self, shard: u16, node: NodeId, origin: DualWriteOrigin) {
        let entry = self.dual_write_targets.entry(shard).or_default();
        let already_present = entry.contains(&node);
        if !already_present {
            entry.push(node);
        }
        match origin {
            DualWriteOrigin::Handoff => {
                // A genuine handoff always UPGRADES the entry: the new-side
                // ACK invariant must hold even if a repair opened the window
                // first.
                if let Some(r) = self.resync_dual_write.get_mut(&shard) {
                    r.remove(&node);
                    if r.is_empty() {
                        self.resync_dual_write.remove(&shard);
                    }
                }
            }
            DualWriteOrigin::Resync => {
                // Never DOWNGRADE an entry a handoff already opened — that
                // would drop the genuine invariant. Only a window this repair
                // opened (or one an earlier repair already marked) is
                // repair-only.
                let already_repair = self
                    .resync_dual_write
                    .get(&shard)
                    .is_some_and(|r| r.contains(&node));
                if !already_present || already_repair {
                    self.resync_dual_write
                        .entry(shard)
                        .or_default()
                        .insert(node);
                }
            }
        }
    }

    fn dual_write_remove(&mut self, shard: u16) {
        self.dual_write_targets.remove(&shard);
        self.resync_dual_write.remove(&shard);
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
        populated_shards: &std::collections::HashSet<u16>,
    ) {
        self.start_outbound_with_origin(tasks, self_id, populated_shards, DualWriteOrigin::Handoff);
    }

    /// Register Phase-H RESYNC backfill tasks (a repair toward a node the
    /// committed table already names as a holder).
    ///
    /// Identical to [`Self::start_outbound`] except that the dual-write
    /// window it opens is tagged repair-only, so the replication path does
    /// NOT turn the repair target into a mandatory per-shard new-side ACK —
    /// see [`Self::dual_write_targets_with_origin_for_shard`] for why that
    /// distinction is the sound one.
    pub fn start_outbound_resync(
        &mut self,
        tasks: &[MigrationTask],
        self_id: NodeId,
        populated_shards: &std::collections::HashSet<u16>,
    ) {
        self.start_outbound_with_origin(tasks, self_id, populated_shards, DualWriteOrigin::Resync);
    }

    fn start_outbound_with_origin(
        &mut self,
        tasks: &[MigrationTask],
        self_id: NodeId,
        _populated_shards: &std::collections::HashSet<u16>,
        origin: DualWriteOrigin,
    ) {
        for task in tasks {
            if task.from_node == self_id {
                let attempt = self.bump_attempt();
                let mut progress = MigrationProgress::from_task(task);
                progress.attempt = attempt;
                self.active.push(progress);
                // Phase E: open dual-write window so writes during the
                // migration land on the new master / replica destination
                // as well as the old replica set.
                self.dual_write_add(task.shard, task.to_node, origin);
                if let Some(m) = migration_metrics() {
                    m.migration_active.fetch_add(1, Ordering::Relaxed);
                    m.migration_phase_preparing.fetch_add(1, Ordering::Relaxed);
                }
            }
            if task.to_node == self_id {
                // Re-acquiring this shard: drop every prior handoff outcome
                // (committed AND aborted) so a stale entry cannot later
                // authorize deleting the data we are now receiving back
                // (task #28), and a stale abort veto cannot block a shed the
                // fresh migration is entitled to authorize (W15).
                self.clear_handoff_outcomes(task.shard);
                // C8 — a fresh migration re-acquiring this shard SUPERSEDES any
                // prior orphaned/lost attempt (possibly from a now-dead
                // source): clear the `lost` mark on every existing entry for
                // the shard so a stale entry cannot leave the re-received shard
                // flagged unavailable once the fresh copy proves complete.
                for m in self
                    .inbound_migrations
                    .iter_mut()
                    .filter(|m| m.shard == task.shard)
                {
                    m.lost = false;
                    // W12 TAIL 2 — a task is being registered for the shard,
                    // so a previously terminally-refused entry is back in
                    // flight and must re-enter the in-flight count.
                    m.clear_refusal();
                }
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
                    self.inbound_migrations
                        .push(InboundMigration::pending(task.shard, task.from_node));
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
        // W12 TAIL 2 — records are arriving for this shard, so any entry
        // previously marked as terminally refused is live again and must
        // re-enter the in-flight count. Done BEFORE the early return: this
        // path no-ops on an existing entry, and leaving the mark set would
        // exclude a shard that is actually receiving.
        let mut existed = false;
        for m in self
            .inbound_migrations
            .iter_mut()
            .filter(|m| m.shard == shard)
        {
            existed = true;
            m.clear_refusal();
        }
        if existed {
            return false;
        }
        self.inbound_migrations
            .push(InboundMigration::pending(shard, NodeId(0)));
        self.inbound_bitmap.set(shard);
        true
    }

    /// Register a pending inbound migration for `shard` from a CONCRETE source
    /// (deletion-tombstone design §4.3; BUG1).
    ///
    /// Unlike [`Self::mark_inbound_active`] (which registers the `NodeId(0)`
    /// sentinel when the source is unknown at dispatch time), this records the
    /// committed master as the `from_node` so the pull-based repair loop —
    /// which sends `OP_MIGRATION_TRANSFER_REQUEST` only for entries whose source
    /// is a concrete peer (`from != self && from != NodeId(0)`) — will actually
    /// request the shard. Used by the Phase 4 rejoin gate after a full-resync
    /// discard: the discarded shard's master must re-push it, but the routing
    /// snapshot install wipes the migration manager, so the gate seeds these
    /// entries AFTER the install (and persists them via
    /// [`persist_inbound_state`]) so both the live requester loop and a restart
    /// re-trigger the re-push.
    ///
    /// Idempotent: if a non-completed entry for `(shard, from_node)` already
    /// exists this is a no-op. Sets the inbound bitmap bit so the read/write
    /// path treats the shard as still receiving until completion. Returns
    /// `true` if a new entry was added, `false` if one already existed.
    pub fn register_inbound_source(&mut self, shard: u16, from_node: NodeId) -> bool {
        if let Some(existing) = self
            .inbound_migrations
            .iter_mut()
            .find(|m| m.shard == shard && m.from_node == from_node && !m.completed)
        {
            // C8 — re-seeding a re-push expectation for a shard previously
            // marked lost revives it as an active (non-lost) pending entry so
            // the incoming re-home completes it, rather than leaving it stuck
            // as unavailable.
            existing.lost = false;
            // W12 TAIL 2 — a fresh registration is a fresh expectation of a
            // real transfer, so the previous terminal refusal no longer
            // describes this entry.
            existing.clear_refusal();
            return false;
        }
        self.inbound_migrations
            .push(InboundMigration::pending(shard, from_node));
        self.inbound_bitmap.set(shard);
        true
    }

    /// P0 (reverse-heal Phase 2c) — register a boot reverse-heal PULL from a
    /// CONCRETE `from_node`, raising the no-serve-before-heal FENCE that
    /// SURVIVES `clear_inbound`.
    ///
    /// Identical to [`Self::register_inbound_source`] (sets the inbound fence
    /// bit; the concrete source drives the pull requester loop) but ALSO raises
    /// `heal_pending`, so a runtime topology commit's [`Self::clear_inbound`]
    /// preserves the fence instead of dropping a `lost = false` heal entry and
    /// serving the un-healed shard as authority. Returns `true` if a new entry
    /// was added, `false` if an existing `(shard, from_node)` entry was revived.
    pub fn register_heal_source(&mut self, shard: u16, from_node: NodeId) -> bool {
        if let Some(existing) = self
            .inbound_migrations
            .iter_mut()
            .find(|m| m.shard == shard && m.from_node == from_node && !m.completed)
        {
            // Revive a prior orphaned/lost attempt as an active heal fence.
            existing.lost = false;
            // W12 review P2-4 — a heal fence is a NEW expectation with its own
            // source and its own deadline clock, so a previous terminal
            // refusal no longer describes this entry. (A `heal_pending` entry
            // can never acquire the mark afterwards: `drop_refused_inbound`
            // skips heal entries outright.)
            existing.clear_refusal();
            existing.heal_pending = true;
            // Phase 3c — (re)start the fenced-heal deadline clock on (re)raise.
            existing.heal_started_at = Some(std::time::Instant::now());
            return false;
        }
        self.inbound_migrations.push(InboundMigration {
            heal_pending: true,
            heal_started_at: Some(std::time::Instant::now()),
            ..InboundMigration::pending(shard, from_node)
        });
        self.inbound_bitmap.set(shard);
        true
    }

    /// P0 (reverse-heal Phase 2c) — raise the no-source FAIL-CLOSED heal fence
    /// for `shard` (a stale shard with no available heal source at boot).
    ///
    /// Like [`Self::mark_inbound_active`] it registers the `NodeId(0)` sentinel
    /// (no concrete peer, so the pull requester loop skips it) and sets the
    /// inbound fence bit, but ALSO raises `heal_pending` so the fence SURVIVES
    /// a runtime topology commit's [`Self::clear_inbound`]. The shard stays
    /// client-invisible (fenced fail-closed) until an operator or a Phase-3
    /// give-up path resolves it — it is never served un-healed. If an entry for
    /// the shard already exists — INCLUDING a forward-migration entry or its
    /// `NodeId(0)` sentinel — its `heal_pending` is raised and the fence bit
    /// re-set: the promotion is deliberate (#74 F6) and strictly fail-closed
    /// (the entry becomes an unproven heal fence; a promoted `NodeId(0)`
    /// sentinel is thereafter a PARK, resolvable by
    /// [`Self::resolve_heal_source`]). Returns `true` if a new entry was
    /// added.
    pub fn mark_heal_fence_active(&mut self, shard: u16) -> bool {
        let mut existed = false;
        for m in self
            .inbound_migrations
            .iter_mut()
            .filter(|m| m.shard == shard)
        {
            m.heal_pending = true;
            // Phase 3c — (re)start the fenced-heal deadline clock on (re)raise.
            m.heal_started_at = Some(std::time::Instant::now());
            existed = true;
        }
        self.inbound_bitmap.set(shard);
        if existed {
            return false;
        }
        self.inbound_migrations.push(InboundMigration {
            heal_pending: true,
            heal_started_at: Some(std::time::Instant::now()),
            ..InboundMigration::pending(shard, NodeId(0))
        });
        true
    }

    /// #74 — the shards currently PARKED under a no-source FAIL-CLOSED heal
    /// fence: an uncompleted `heal_pending` entry whose source is still the
    /// `NodeId(0)` sentinel (heal-source selection REFUSED — no quorum-current
    /// candidate — so there is nothing for the pull requester loop to drive).
    ///
    /// The Phase-3b online re-heal pass re-attempts quorum-current source
    /// selection for exactly these shards on every partition-view refresh and
    /// resolves a successful pick via [`Self::resolve_heal_source`] —
    /// park-and-retry, never a terminal give-up. A forward-migration
    /// `NodeId(0)` sentinel (`heal_pending` clear) is NOT parked and is not
    /// returned. Sorted ascending and deduplicated.
    pub fn parked_no_source_heal_shards(&self) -> Vec<u16> {
        let mut out: Vec<u16> = self
            .inbound_migrations
            .iter()
            .filter(|m| m.heal_pending && !m.completed && m.from_node == NodeId(0))
            .map(|m| m.shard)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// #74 — resolve a PARKED no-source heal fence to a CONCRETE quorum-current
    /// source so the pull requester loop can drive it.
    ///
    /// Rewrites the uncompleted `heal_pending` `NodeId(0)`-sentinel entry for
    /// `shard` to name `from_node` IN PLACE (mirroring the forward-migration
    /// sentinel replacement in [`Self::start_outbound`]) — never adding a
    /// second entry, because the completion handshake
    /// ([`Self::mark_inbound_complete_from_source`]) completes ONE entry and a
    /// leftover sibling sentinel would hold the fence bit forever. Clears any
    /// stale `lost` mark and restarts the Phase-3c fenced-heal deadline clock
    /// (the heal only now became drivable). If an uncompleted CONCRETE entry
    /// for `(shard, from_node)` already exists, the redundant sentinel is
    /// dropped instead — the concrete entry keeps the fence up and the pull
    /// driven, and the single completion handshake then clears the whole
    /// fence.
    ///
    /// Returns `true` iff a sentinel was resolved to `from_node`; `false` when
    /// there is nothing to resolve (no parked heal fence for `shard`), when
    /// `from_node` is not a concrete peer (`NodeId(0)`), or when the concrete
    /// entry already existed.
    ///
    /// #74 F6 — what counts as a park is `heal_pending` + `NodeId(0)`, by
    /// ORIGIN-BLIND design: a forward-migration sentinel
    /// ([`Self::mark_inbound_active`]) is only skipped while its
    /// `heal_pending` is clear (its source is assigned by the authoritative
    /// migration dispatch). Once PROMOTED to a heal fence —
    /// [`Self::mark_heal_fence_active`] raises `heal_pending` on every
    /// existing entry for the shard, and the promotion is persisted via the
    /// flag byte so a restart restores it as a park (#74 F1) — the entry IS a
    /// park and is resolvable here. That is deliberate and fail-closed: the
    /// promoted entry already fences the shard, and resolving it merely gives
    /// the fence a quorum-current-evidenced pull to complete through.
    pub fn resolve_heal_source(&mut self, shard: u16, from_node: NodeId) -> bool {
        if from_node == NodeId(0) {
            return false;
        }
        let concrete_exists = self
            .inbound_migrations
            .iter()
            .any(|m| m.shard == shard && m.from_node == from_node && !m.completed);
        if concrete_exists {
            self.inbound_migrations.retain(|m| {
                !(m.shard == shard && m.from_node == NodeId(0) && m.heal_pending && !m.completed)
            });
            return false;
        }
        if let Some(m) = self.inbound_migrations.iter_mut().find(|m| {
            m.shard == shard && m.from_node == NodeId(0) && m.heal_pending && !m.completed
        }) {
            m.from_node = from_node;
            m.lost = false;
            m.heal_started_at = Some(std::time::Instant::now());
            self.inbound_bitmap.set(shard);
            return true;
        }
        false
    }

    /// #74 F5 — RE-PARK resolved heal pulls whose concrete source TERMINALLY
    /// failed: the source is no longer in `alive_sources` (SWIM declared it
    /// Dead, or it left membership) and the entry has no transfer request in
    /// flight within `request_grace` (mirroring
    /// [`Self::orphaned_inbound_shards`]'s mid-flight exclusion). Each such
    /// entry's source is restored to the `NodeId(0)` PARKED sentinel —
    /// `heal_pending` kept, fence kept, deadline clock restarted — so the
    /// online re-source pass re-selects a FRESH quorum-current source on a
    /// later view instead of the shard staying pinned forever to a dead pick.
    ///
    /// This is the heal-entry sibling of the forward-migration orphan reap
    /// ([`Self::orphaned_inbound_shards`] → [`Self::mark_inbound_lost`]),
    /// which deliberately EXCLUDES `heal_pending` entries: a heal is never
    /// marked LOST — it re-parks and retries (alert-and-hold via Phase 3c if
    /// no candidate ever qualifies). Duplicate parks for one shard are
    /// collapsed to a single sentinel (a leftover sibling would outlive the
    /// first's resolution and pin the fence forever). A slow-but-alive
    /// source's entry is never touched. Returns the number of entries
    /// re-parked.
    pub fn repark_dead_source_heals(
        &mut self,
        request_grace: std::time::Duration,
        alive_sources: &std::collections::HashSet<NodeId>,
    ) -> usize {
        let now = std::time::Instant::now();
        let mut reparked = 0usize;
        for m in self.inbound_migrations.iter_mut() {
            if m.completed || !m.heal_pending || m.from_node == NodeId(0) {
                continue;
            }
            if alive_sources.contains(&m.from_node) {
                continue;
            }
            if let Some(at) = m.transfer_requested_at
                && now.duration_since(at) < request_grace
            {
                continue;
            }
            m.from_node = NodeId(0);
            m.transfer_requested_at = None;
            m.lost = false;
            m.heal_started_at = Some(now);
            reparked += 1;
        }
        if reparked > 0 {
            // Collapse duplicate parks per shard (keep the first).
            let mut seen: std::collections::HashSet<u16> = std::collections::HashSet::new();
            self.inbound_migrations.retain(|m| {
                if m.completed || !m.heal_pending || m.from_node != NodeId(0) {
                    return true;
                }
                seen.insert(m.shard)
            });
        }
        reparked
    }

    /// GAP 3a (armed scenario 07) — RE-PARK plain (non-heal) pending/LOST
    /// inbound entries whose source has LEFT the committed membership.
    ///
    /// The #74 heal sibling ([`Self::repark_dead_source_heals`]) re-sources
    /// only `heal_pending` entries; a PLAIN forward entry pinned to a removed
    /// node was left behind forever: the transfer requester kept asking a
    /// node with no address ("no address for transfer-request source"), and
    /// [`Self::mark_inbound_complete_from_source`] could never match a
    /// completion — the shard stayed fenced/LOST with no repair path
    /// (observed: 370-487 LOST inbound entries with `from_node = 4` after
    /// node 4's removal). Each such entry's source is restored to the plain
    /// `NodeId(0)` sentinel — fence kept, `heal_pending` kept CLEAR, `lost`
    /// kept AS-IS (kind discrimination: the persisted flag byte's bit0/bit1
    /// survive unchanged) — which makes the entry completable by whichever
    /// source actually streams the shard (the under-replication resync from
    /// the committed master; see the source-less-sentinel branch of
    /// `mark_inbound_complete_from_source`).
    ///
    /// Departure is judged against the COMMITTED membership, not the
    /// SWIM-alive set: a merely-dead member may rejoin with its identity and
    /// complete its own entries, while a node removed from membership never
    /// can — its entries are terminally unpullable. There is deliberately no
    /// request-grace exclusion (unlike the heal sibling): a request in flight
    /// toward a departed node cannot be honoured. An EMPTY membership set
    /// re-parks nothing — absence of membership evidence is not evidence of
    /// departure. Duplicate plain sentinels for one shard are collapsed to
    /// one, keeping the most conservative kind (`lost` if ANY duplicate was
    /// lost). Heal entries and already-parked sentinels are never touched.
    /// Returns the number of entries re-parked.
    pub fn repark_departed_source_inbound(
        &mut self,
        committed_members: &std::collections::HashSet<NodeId>,
    ) -> usize {
        if committed_members.is_empty() {
            return 0;
        }
        let mut reparked = 0usize;
        for m in self.inbound_migrations.iter_mut() {
            if m.completed || m.heal_pending || m.from_node == NodeId(0) {
                continue;
            }
            if committed_members.contains(&m.from_node) {
                continue;
            }
            m.from_node = NodeId(0);
            m.transfer_requested_at = None;
            // W12 review P2-4 — the mark records that THIS entry's own
            // `from_node` terminally refused it. Re-parking replaces that
            // source with the sentinel and (per this function's contract)
            // makes the entry completable by whichever source streams it, so
            // the refusal no longer applies — and leaving it set would both
            // discount a completable entry from the in-flight count and
            // report it as refused by node 0, which no source ever was.
            m.clear_refusal();
            reparked += 1;
        }
        if reparked > 0 {
            // Collapse duplicate plain sentinels per shard, keeping the
            // FIRST but folding the dropped duplicates' `lost` marks into it
            // (fail-closed: LOST if any duplicate was LOST).
            let mut lost_by_shard: std::collections::HashSet<u16> =
                std::collections::HashSet::new();
            for m in &self.inbound_migrations {
                if !m.completed && !m.heal_pending && m.from_node == NodeId(0) && m.lost {
                    lost_by_shard.insert(m.shard);
                }
            }
            let mut seen: std::collections::HashSet<u16> = std::collections::HashSet::new();
            self.inbound_migrations.retain(|m| {
                if m.completed || m.heal_pending || m.from_node != NodeId(0) {
                    return true;
                }
                seen.insert(m.shard)
            });
            for m in self.inbound_migrations.iter_mut() {
                if !m.completed
                    && !m.heal_pending
                    && m.from_node == NodeId(0)
                    && lost_by_shard.contains(&m.shard)
                {
                    m.lost = true;
                }
            }
        }
        reparked
    }

    /// Reverse-heal completion drop-awareness — is there an ACTIVE (uncompleted)
    /// `heal_pending` inbound entry for `shard` sourced from exactly `from_node`?
    ///
    /// A `true` answer means a completion handshake arriving for this
    /// `(shard, from_node)` is a REVERSE-HEAL completion, not a forward migration:
    /// the source ships/manifests its FULL live key set, while RULE-DS
    /// legitimately DROPS every source key this node holds a blocking tombstone
    /// for (and KEEPS its own higher-generation copy). The completion verify must
    /// therefore be drop-aware for it — a source key the target legitimately did
    /// NOT apply is NOT a gap. Forward migrations (no `heal_pending` entry)
    /// answer `false` and keep exact holding semantics unchanged.
    ///
    /// #74 F4 — a PARKED no-source fence (`from_node == NodeId(0)`,
    /// `heal_pending`) matches NO source: no source was ever selected for it,
    /// so no arriving completion can be "its" heal. Matching it (as this once
    /// did) let ANY source's completion classify as a heal, relax the verify's
    /// exact-holding gates, and clear the park with nothing healed. The
    /// exclusion mirrors [`Self::mark_inbound_complete_from_source`], which
    /// likewise refuses to complete a park, so the discriminator and the
    /// fence-clear agree: a park exits only via [`Self::resolve_heal_source`]
    /// + that concrete source's completion, or operator action.
    pub fn has_pending_heal_from_source(&self, shard: u16, from_node: NodeId) -> bool {
        self.inbound_migrations.iter().any(|m| {
            m.heal_pending
                && !m.completed
                && m.shard == shard
                && m.from_node == from_node
                && m.from_node != NodeId(0)
        })
    }

    /// W10 FIX 2 — is there ANY active (uncompleted) inbound entry for `shard`
    /// sourced from exactly `from_node` (forward migration OR reverse-heal)?
    ///
    /// This is the target-side "fence still held" check of the weak-veto
    /// arbitration (`OP_MIGRATION_WEAK_VETO_ARBITRATE`): an arbitration is
    /// only honored while the source's transfer for the shard is still open
    /// on this node — i.e. the completion whose rejection prompted the
    /// arbitration has not resolved and the shard is still inbound-fenced
    /// against client serving. The `NodeId(0)` sentinel (a parked no-source
    /// fence) matches no source, mirroring
    /// [`Self::has_pending_heal_from_source`].
    pub fn has_pending_inbound_from_source(&self, shard: u16, from_node: NodeId) -> bool {
        self.inbound_migrations.iter().any(|m| {
            !m.completed && m.shard == shard && m.from_node == from_node && m.from_node != NodeId(0)
        })
    }

    /// Reverse-heal Phase 3c (design §E3) — the shards whose `heal_pending` fence
    /// has been up (uncompleted) for at least `deadline` — i.e. STUCK heals whose
    /// deadline fallback (escalate / alert-and-hold) is due.
    ///
    /// A shard qualifies iff it has an inbound entry with `heal_pending` set,
    /// `completed` clear, and a `heal_started_at` at least `deadline` in the past.
    /// A heal without a timer (`heal_started_at == None`) or one raised more
    /// recently than `deadline` is NOT returned — so a slow-but-still-progressing
    /// pull is never escalated prematurely (the deadline is sized to comfortably
    /// exceed a normal round-trip). Deduplicated and returned ascending; a shard
    /// whose heal completed no longer qualifies (the completion cleared the fence).
    pub fn expired_heal_shards(&self, deadline: std::time::Duration) -> Vec<u16> {
        let mut out: Vec<u16> = self
            .inbound_migrations
            .iter()
            .filter(|m| {
                m.heal_pending
                    && !m.completed
                    && m.heal_started_at
                        .map(|t| t.elapsed() >= deadline)
                        .unwrap_or(false)
            })
            .map(|m| m.shard)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Reverse-heal Phase 3c — RESET the fenced-heal deadline clock for `shard`'s
    /// heal entries (set `heal_started_at` to now). Called after an ALERT-AND-HOLD
    /// (or an escalate that found no fresher source): the shard STAYS fenced, but
    /// resetting the timer bounds the alert cadence to one per deadline window
    /// rather than one per event-loop tick. Returns `true` if any entry was
    /// refreshed.
    pub fn refresh_heal_deadline(&mut self, shard: u16) -> bool {
        let now = std::time::Instant::now();
        let mut refreshed = false;
        for m in self
            .inbound_migrations
            .iter_mut()
            .filter(|m| m.shard == shard && m.heal_pending && !m.completed)
        {
            m.heal_started_at = Some(now);
            refreshed = true;
        }
        refreshed
    }

    /// Mark an inbound shard as received (data has arrived and been verified).
    ///
    /// Marks the first non-completed entry for this shard as completed. The entry
    /// is retained until `cleanup_completed()` removes it.
    ///
    /// This SOURCE-LESS variant is only reached by a completion that carries no
    /// `from_node` (a legacy / no-source frame). A REVERSE-HEAL completion always
    /// carries its source and routes through
    /// [`Self::mark_inbound_complete_from_source`], so a source-less completion is
    /// never a heal. Prefer a NON-`heal_pending` entry here so that if a future
    /// path ever co-registered a forward inbound AND a heal fence for one shard,
    /// this source-less completion cannot clear the heal fence (whose completeness
    /// it did not prove). Fall back to any non-completed entry only when no
    /// non-heal candidate exists, preserving the prior behaviour for the common
    /// single-entry case.
    pub fn mark_inbound_complete(&mut self, shard: u16) {
        let target = self
            .inbound_migrations
            .iter()
            .position(|m| m.shard == shard && !m.completed && !m.heal_pending)
            .or_else(|| {
                // #74 F4 — the fallback may complete a CONCRETE-source heal
                // (single-entry legacy case) but never a PARKED no-source
                // fence: a park has no selected source, so no completion can
                // prove it healed.
                self.inbound_migrations
                    .iter()
                    .position(|m| m.shard == shard && !m.completed && m.from_node != NodeId(0))
            });
        if let Some(idx) = target {
            let m = &mut self.inbound_migrations[idx];
            m.completed = true;
            // P0 — completion proves the shard: drop the reverse-heal fence marker.
            m.heal_pending = false;
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
            inbound.heal_pending = false;
            found = true;
        }
        if !found {
            self.record_completed_inbound_tombstone(shard, NodeId(0));
        }
        self.inbound_bitmap.clear(shard);
        // BUG4 (b): this clears the shard's inbound state without routing
        // through the commit-gate union, so drop any accumulated reconcile
        // manifests for it to prevent a leak. No-op off-path (map empty).
    }

    pub fn mark_inbound_complete_from_source(&mut self, shard: u16, from_node: NodeId) {
        if let Some(m) = self
            .inbound_migrations
            .iter_mut()
            .find(|m| m.shard == shard && m.from_node == from_node && !m.completed)
        {
            m.completed = true;
            m.heal_pending = false;
        } else if let Some(m) = self.inbound_migrations.iter_mut().find(|m| {
            // #74 F4 — the source-unknown-at-dispatch FORWARD sentinel is
            // completable by whichever source actually streamed. A PARKED heal
            // fence (`heal_pending` + NodeId(0)) is NOT: no source was ever
            // selected for it, so no completion can prove it healed — it exits
            // only via `resolve_heal_source` + that source's completion.
            m.shard == shard && m.from_node == NodeId(0) && !m.completed && !m.heal_pending
        }) {
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
    ///
    /// W17 — counts as a pipeline advance ([`Self::pipeline_advances`]). This
    /// is the ONLY manager call the empty-shard batch path makes between
    /// registering its tasks and completing them: it fences with this rather
    /// than [`Self::mark_fenced`], so its tasks stay in `Preparing` and no
    /// state transition is emitted, while `drain_in_flight_mutations`, a full
    /// `keys_by_shard_filtered` index pass and a batched completion handshake
    /// run. Without this bump an empty-dominated rebalance emits nothing at all
    /// and the stranded-task reaper fires on a working pipeline.
    ///
    /// [`Self::unfence_shard`] deliberately does NOT count: `mark_failed` lifts
    /// the fence through it, and a failure must never reset the reaper's clock.
    pub fn fence_shard(&mut self, shard: u16) {
        self.fenced_shards.set(shard);
        self.note_pipeline_advance();
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
        self.mark_fenced_with_origin(task, fence_sequence, true);
    }

    /// W11 FIX 3(b) — [`Self::mark_fenced`] with the CLIENT WRITE FENCE made
    /// optional.
    ///
    /// The delta-phase transition itself (state, `fence_sequence`, phase
    /// gauges) is identical for every migration origin: `fence_sequence` is
    /// just a redo position, and the delta replay of
    /// `[snapshot_sequence, fence_sequence)` is driven off it either way.
    ///
    /// `raise_write_fence == false` is the Phase-H RESYNC backfill: a repair
    /// toward a node the committed table already names as a holder. No
    /// ownership transition happens, so the source stays the authority
    /// throughout and must keep serving writes; the writes it admits past
    /// `fence_sequence` reach the repair target through the Phase E
    /// dual-write window (opened by [`Self::start_outbound_resync`] BEFORE
    /// any data moved) and the target's ordinary replica fan-out, instead of
    /// through this batch's delta. Fencing them instead was a pure
    /// availability loss (armed-04: 128/4096 shards per node write-fenced
    /// continuously, 5/200 spends rejected).
    ///
    /// The task still enters `Fenced` state, so an unfenced repair on a shard
    /// that ALSO has a live handoff keeps that handoff's fence up until the
    /// repair resolves (`has_other_fenced_task`) — bounded by the run, and
    /// the fail-safe direction.
    pub fn mark_fenced_with_origin(
        &mut self,
        task: &MigrationTask,
        fence_sequence: u64,
        raise_write_fence: bool,
    ) {
        let prev_state = self.find_task_mut(task).map(|p| p.state.clone());
        if let Some(p) = self.find_task_mut(task) {
            p.state = MigrationState::Fenced;
            p.fence_sequence = fence_sequence;
        }
        if prev_state.is_some() {
            self.note_pipeline_advance();
        }
        if raise_write_fence {
            self.fence_shard(task.shard);
        }
        if let Some(m) = migration_metrics() {
            if let Some(prev) = prev_state {
                dec_phase_gauge(m, &prev);
            }
            m.migration_phase_delta.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Set the snapshot sequence checkpoint for a migration task.
    ///
    /// W16 — this is Phase 1, where the worker starts holding a redo READ
    /// position at `seq`: it will not read the window until Phase 3, with the
    /// whole baseline stream in between. The hold is stamped with the entry's
    /// current [`MigrationProgress::attempt`] so any re-drive invalidates it
    /// (see [`MigrationProgress::snapshot_hold_attempt`]), and published to the
    /// checkpoint reset guard through [`Self::delta_reader_redo_floor`].
    ///
    /// The worker reads `current_sequence()` and calls this as two separate
    /// statements, so the hold is published a moment after the position is
    /// captured. That gap cannot lose a race with a checkpoint (review P2-4):
    /// the checkpoint samples `entries_before` at its START and only evaluates
    /// the guard at its END, after the snapshot, so a capture low enough to be
    /// harmed (`seq < entries_before`) necessarily happened before the
    /// checkpoint began — and this publish would have to be delayed past the
    /// checkpoint's ENTIRE duration to be missed. Closing it outright would mean
    /// taking the redo lock inside the migration lock, inverting the ordering
    /// this module enforces everywhere else; not worth it.
    pub fn set_snapshot_sequence(&mut self, task: &MigrationTask, seq: u64) {
        let prev_state = self.find_task_mut(task).map(|p| p.state.clone());
        if let Some(p) = self.find_task_mut(task) {
            p.snapshot_sequence = seq;
            p.snapshot_hold_attempt = Some(p.attempt);
            p.state = MigrationState::Streaming;
        }
        if prev_state.is_some() {
            self.note_pipeline_advance();
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
        let mut found = false;
        if let Some(p) = self.find_task_mut(task) {
            p.migrated_records += records;
            p.bytes_sent += bytes;
            found = true;
        }
        if found && (records != 0 || bytes != 0) {
            self.note_pipeline_advance();
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
        self.note_pipeline_advance();
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

    /// W9 nit — exact-task variant of [`Self::mark_failed`]: resolves the entry
    /// by the FULL task identity INCLUDING `is_master`, where
    /// `mark_failed`'s (shard, from, to) lookup can hit the twin entry
    /// when a master and a replica task share the same endpoints. Used by
    /// the det-degrade cancel path
    /// (`cancel_deferred_plan_launch`), which iterates the held plan's
    /// task list and must fail exactly the tasks it names — a twin left
    /// active would be preservable as a workerless task. Fence-lift,
    /// dual-write close, and metrics bookkeeping match [`Self::mark_failed`].
    pub fn mark_failed_exact(&mut self, task: &MigrationTask) {
        let Some(idx) = self.active.iter().position(|p| {
            p.shard == task.shard
                && p.from_node == task.from_node
                && p.to_node == task.to_node
                && p.is_master == task.is_master
        }) else {
            return;
        };
        let prev_state = self.active[idx].state.clone();
        self.active[idx].state = MigrationState::Failed;
        if !self.has_other_fenced_task(task.shard, task) {
            self.unfence_shard(task.shard);
        }
        if !self.has_other_active_outbound(task.shard, task) {
            self.dual_write_remove(task.shard);
        }
        if let Some(m) = migration_metrics() {
            dec_phase_gauge(m, &prev_state);
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
            completed: true,
            ..InboundMigration::pending(shard, from_node)
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
    /// W4 review P1 — the reset re-stamps [`MigrationProgress::attempt`]
    /// with a fresh generation: the retry re-drives the SAME entry in place
    /// (same identity, same epoch), so without the new stamp a still-draining
    /// earlier batch's end-of-batch abandoned sweep would see its captured
    /// stamp match and park the entry out from under the retry batch —
    /// lifting its fence mid fence-to-completion window.
    ///
    /// W16 review P1-1 — the fresh `attempt` stamp is ALSO what invalidates this
    /// entry's redo read hold. `snapshot_sequence` is deliberately left as-is
    /// (the re-drive overwrites it at its own Phase 1), so it is stale from the
    /// instant this returns; because
    /// [`MigrationProgress::snapshot_hold_attempt`] still names the PREVIOUS
    /// attempt, [`Self::delta_reader_redo_floor`] stops counting it here rather
    /// than pinning the redo log at an unsatisfiable position for the whole
    /// re-drive latency. Do not "fix" that by re-stamping the hold.
    ///
    /// Returns true if the migration was found and reset, false otherwise.
    pub fn retry_failed(&mut self, task: &MigrationTask) -> bool {
        let attempt = self.bump_attempt();
        if let Some(p) = self.active.iter_mut().find(|p| {
            p.shard == task.shard
                && p.from_node == task.from_node
                && p.to_node == task.to_node
                && p.state == MigrationState::Failed
        }) {
            p.state = MigrationState::Streaming;
            p.migrated_records = 0;
            p.bytes_sent = 0;
            p.attempt = attempt;
            true
        } else {
            false
        }
    }

    /// Terminally retire ONE failed outbound migration task (F3).
    ///
    /// Failed entries are normally the durable retry queue
    /// (`take_failed_tasks`). After the missing-exact-key completion
    /// escalation exhausts its attempts, re-driving THIS task can never
    /// succeed — the target keeps rejecting a manifest key the source cannot
    /// deliver — and each re-drive re-wedges the shard at two serving
    /// masters. The caller has already rolled the shard back to `self`
    /// (single serving master, data-safe); removing the entry stops the
    /// re-drive. A later re-heal round re-plans the handoff from a fresh
    /// manifest.
    ///
    /// Only an entry in the `Failed` state matching the task's full identity
    /// is removed (fence/dual-write bookkeeping was already handled by
    /// `mark_failed`). Returns `true` when an entry was removed.
    pub fn retire_failed_task(&mut self, task: &MigrationTask) -> bool {
        let before = self.active.len();
        self.active.retain(|p| {
            !(p.shard == task.shard
                && p.from_node == task.from_node
                && p.to_node == task.to_node
                && p.is_master == task.is_master
                && p.state == MigrationState::Failed)
        });
        before != self.active.len()
    }

    /// F3 / review finding 3 — atomically mark an outbound task Failed and
    /// remove its tracking entry under a single `&mut self` borrow.
    ///
    /// The caller (the terminal completion abort) holds the manager lock for
    /// exactly one call, so a concurrent `take_failed_tasks` re-drive can
    /// never observe the transient Failed state between the mark and the
    /// removal and resurrect the task. The entry must match the task's FULL
    /// identity (shard, from, to, is_master); `mark_failed`'s fence and
    /// dual-write bookkeeping runs as usual. Returns `true` when a matching
    /// entry was failed and removed.
    pub fn fail_and_retire_task(&mut self, task: &MigrationTask) -> bool {
        let tracked = self.active.iter().any(|p| {
            p.shard == task.shard
                && p.from_node == task.from_node
                && p.to_node == task.to_node
                && p.is_master == task.is_master
        });
        if !tracked {
            return false;
        }
        self.mark_failed(task);
        self.retire_failed_task(task)
    }

    /// W4 — park every task in `tasks` whose tracking entry is still
    /// UNRESOLVED (neither `Complete` nor `Failed`) as `Failed`, returning
    /// exactly the tasks that were marked.
    ///
    /// This is the batch-end abandoned-task sweep's manager half: a migration
    /// batch whose workers aborted early (e.g. the peer-superseded return)
    /// leaves its untouched tasks in `Preparing`/`Streaming`/`Fenced` with no
    /// worker left to drive them — and those entries hold `active_count()`
    /// above zero forever, which keeps the event loop's same-term re-heal
    /// gate shut (observed in run 31904708491 scenario 09: 288/1689 frozen
    /// entries, no re-heal for the rest of the run). Marking them `Failed`
    /// parks them in the durable retry queue (`take_failed_tasks`) and runs
    /// `mark_failed`'s fence/dual-write bookkeeping.
    ///
    /// Each element pairs the task with the drive-attempt stamp the batch
    /// captured at spawn ([`Self::capture_task_attempts`]); an entry whose
    /// stamp has moved on (re-driven by `retry_failed`, or re-registered by
    /// `start_outbound`) belongs to a newer driver and is skipped. The
    /// membership check and the marking happen under one `&mut self` borrow
    /// so a concurrent re-registration cannot interleave between the two.
    /// Entries already `Complete` or `Failed` are left untouched.
    pub fn fail_unresolved_tasks(&mut self, tasks: &[(MigrationTask, u64)]) -> Vec<MigrationTask> {
        let mut marked = Vec::new();
        for (task, expected_attempt) in tasks {
            let unresolved = self.active.iter().any(|p| {
                p.shard == task.shard
                    && p.from_node == task.from_node
                    && p.to_node == task.to_node
                    && p.is_master == task.is_master
                    // W4 review P1 — the generation guard: park ONLY the
                    // exact drive attempt this batch captured at spawn. A
                    // newer stamp means the entry was re-driven in place
                    // (`retry_failed`) or re-registered under the same
                    // identity (`start_outbound`) after the capture — it has
                    // a live driver and is not this batch's to park.
                    && p.attempt == *expected_attempt
                    && !p.is_complete()
                    && p.state != MigrationState::Failed
            });
            if unresolved {
                self.mark_failed(task);
                marked.push(task.clone());
            }
        }
        marked
    }

    /// W4 review P1 — snapshot each task's CURRENT drive-attempt stamp
    /// ([`MigrationProgress::attempt`]) so a batch can prove, at its
    /// end-of-batch abandoned sweep, that an entry was not re-driven in
    /// place (the NodeJoined `take_failed_tasks` -> `retry_failed` path
    /// resets the SAME entry to `Streaming` at the same epoch) or
    /// re-registered under the same identity since the batch spawned. Tasks
    /// with no tracked entry are omitted — there is nothing to park for
    /// them. Called under the same manager lock the batch registration used.
    pub fn capture_task_attempts(&self, tasks: &[MigrationTask]) -> Vec<(MigrationTask, u64)> {
        tasks
            .iter()
            .filter_map(|task| {
                self.active
                    .iter()
                    .find(|p| {
                        p.shard == task.shard
                            && p.from_node == task.from_node
                            && p.to_node == task.to_node
                            && p.is_master == task.is_master
                    })
                    .map(|p| (task.clone(), p.attempt))
            })
            .collect()
    }

    /// Collect all failed migration tasks for re-execution.
    ///
    /// W8 review P0-2 — draining also releases the failed-batch retry hold:
    /// the drained entries are live Streaming re-drives now, so
    /// [`Self::cleanup_completed`] may resume reaping any FUTURE `Failed`
    /// entries normally until the next failed disposition re-raises it.
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
        self.failed_retry_hold = false;
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
    ///
    /// W8 review P0-2 — while the failed-batch retry hold is raised
    /// ([`Self::arm_failed_batch_retry`]), `Failed` entries are PRESERVED by
    /// delegating to [`Self::cleanup_completed_keep_failed`]: the durable
    /// retry queue must survive the coordinator event loop's periodic prune
    /// until the delayed re-drive drains it. Every other effect (completed
    /// pruning, unfencing, inbound retention) is identical. The hold is
    /// released by the drain and by the activation's epoch-fenced
    /// [`Self::clear_failed_retry_state`], after which this reaps `Failed`
    /// exactly as before.
    pub fn cleanup_completed(&mut self) {
        if self.failed_retry_hold {
            return self.cleanup_completed_keep_failed();
        }
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

        // W15 review P1-1 — DATA-LOSS GUARD. A `Failed` entry is not just
        // bookkeeping: it is the block both orphan-cleanup gates key their
        // unresolved-task skip off (`run_orphan_cleanup`'s `unsettled` set and
        // `cleanup_orphaned_shard_if_settled`'s `shard_failed` test). Reaping
        // it here removes that block, and if a SIBLING handoff of the same
        // shard committed, `has_committed_handoff` then authorizes deleting
        // every local record of the shard — including the ones the failed task
        // was still trying to deliver.
        //
        // That is the scenario-05 chain one step over: master handoff S:n1→n2
        // commits (`Committed(E)`), the replica push S:n1→n3 fails ORDINARILY
        // (connection reset / target-not-ready budget exhausted / abandoned-batch
        // park) leaving a `Failed` entry, the event loop's periodic prune reaps
        // it once `failed_retry_hold` drops, and the steady-state sweep deletes
        // the shard. Loss iff n2's manifest did not cover those records —
        // precisely the scenario-05 condition.
        //
        // The fix is NOT to record an `Aborted` veto for an ordinary failure: a
        // connection reset is no evidence the target lacks a record, and
        // vetoing on it would be the over-conservatism this design avoids. It
        // is to keep the protection the reaped entry was providing — drop the
        // shard's evidence with it. Fail-closed, no new state, and it
        // self-heals on the next committing handoff, which re-earns the
        // evidence from scratch.
        //
        // `cleanup_completed_keep_failed` deliberately does NOT do this: it
        // PRESERVES the `Failed` entries, so the block is still standing.
        let reaped_failed_shards: Vec<u16> = self
            .active
            .iter()
            .filter(|p| p.state == MigrationState::Failed)
            .map(|p| p.shard)
            .collect();

        self.active
            .retain(|p| !p.is_complete() && p.state != MigrationState::Failed);

        for shard in reaped_failed_shards {
            self.clear_handoff_outcomes(shard);
        }

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
            local_view_effective_master_id: 0,
            is_serving_fenced: false,
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

    /// W17 — monotonic count of FORWARD advances of this node's OUTBOUND
    /// migration pipeline: a write fence raised ([`Self::fence_shard`]), a task
    /// entering `Streaming` ([`Self::set_snapshot_sequence`]), entering
    /// `Fenced` ([`Self::mark_fenced_with_origin`]), or completing
    /// ([`Self::mark_complete`]).
    ///
    /// [`Self::record_progress`] bumps it too, but is DEAD CODE today: the repo
    /// has no production caller (Phase 1 discards the count
    /// `stream_shard_baseline` returns), so no live migration ever carries
    /// `bytes_sent`/`migrated_records` above zero. That is a pre-existing gap
    /// with consequences beyond this counter — see
    /// `task_is_stranded_candidate`, whose "progress-carrying tasks are
    /// excluded" guard has therefore never excluded anything, and `/status`,
    /// which reports `records_transferred: 0` forever. Do not read this line as
    /// a live source until that is wired.
    ///
    /// Never resets and never decreases, so a sampler that misses ticks —
    /// the coordinator event loop routinely stalls ten seconds under I/O
    /// pressure — still sees every advance that happened while it was away.
    /// That is the whole point: an INSTANTANEOUS "is anything streaming right
    /// now" sample cannot distinguish a dead pipeline from a live one
    /// observed between phases.
    ///
    /// # The bug this exists for (CI run 32668957355, scenario 07)
    ///
    /// The stranded-task reaper retires a `Fenced` task that has made no
    /// progress for `STRANDED_TASK_REAP_AFTER` (45 s), on the premise that
    /// `Fenced` is a brief per-task cutover state no live task dwells in.
    /// The batch pipeline breaks that premise in bulk: a worker fences an
    /// ENTIRE sub-batch in one lock (`run_migration_batch_with_origin`
    /// Phase 2) and then works the completion handshakes serially, so every
    /// shard in the sub-batch — an EMPTY one moves no bytes and no records
    /// by definition — sits in `Fenced` at zero progress for as long as the
    /// whole sub-batch takes.
    ///
    /// On a host running ~3x slower than every comparison run (a 246-shard
    /// batch took 9.2/31.5 s against 1.3-7.2 s; a ~490-730-shard batch
    /// 54.7-74.0 s against 15.3-30.5 s) that exceeded 45 s, and the reaper
    /// destroyed 4877 in-flight tasks across two nodes at exactly T+45 s —
    /// node3 reaped 2261, re-planned from scratch (`outbound=2006`), and the
    /// scenario missed its 120 s gate. The run was CONVERGING, not wedged:
    /// successful handoffs per 10 s bucket ran 24, 6, 165, 222, 270, 481,
    /// 356, 487, 558 right through the reap. The deadline, not the cluster,
    /// was the failure.
    ///
    /// Gating the reap on this counter makes it mean "45 s with the pipeline
    /// IDLE" instead of "45 s of wall clock". The two recorded strandings it
    /// exists for are both fully quiesced — one leftover `Fenced` task with
    /// masters/handoffs/inbound all converged, and eight `Preparing` tasks
    /// that were the entire migration set — so nothing advances this counter
    /// and the reap still fires.
    ///
    /// # What is deliberately NOT counted
    ///
    /// `mark_failed*` (a resolution, not an advance — and the reaper's own
    /// call, which would otherwise feed itself) and `start_outbound*` (task
    /// REGISTRATION: a re-planning loop that keeps enqueuing tasks nothing
    /// ever drives must not look like progress).
    pub fn pipeline_advances(&self) -> u64 {
        self.pipeline_advances
    }

    /// Record one forward advance of the outbound migration pipeline.
    /// See [`Self::pipeline_advances`] for what qualifies and what does not.
    fn note_pipeline_advance(&mut self) {
        self.pipeline_advances = self.pipeline_advances.saturating_add(1);
    }

    /// W16 direction 1 — the REDO READ FLOOR held by this node's in-flight
    /// migration delta readers: `(holder_count, lowest_sequence_still_needed)`.
    ///
    /// A migration worker captures `snapshot_sequence` at Phase 1 and does not
    /// read the redo window `[snapshot_sequence, fence_sequence)` until Phase 3
    /// (`collect_migration_delta_ops`), with the whole baseline stream in
    /// between. Across that gap it is a redo CONSUMER holding a read position,
    /// exactly like a lagging replica's ACK watermark — but nothing published
    /// it, so the checkpoint reset guard (which consults only
    /// `min_acked_over_expected`, the replication ACK tracker) could reclaim
    /// the prefix out from under it. In armed scenario 06 (CI 32644361371) a
    /// checkpoint whose `entries_before` was 8143 reclaimed 67 ms before 195
    /// shards failed their delta with `need seq 6875, earliest available 8143`
    /// — and each failure calls `rollback_shard`, a node-local mutation of the
    /// target table whose repair is gated behind `active_count() == 0` plus a
    /// 30 s cooldown.
    ///
    /// # What qualifies as a holder
    ///
    /// An entry holds iff its [`MigrationProgress::snapshot_hold_attempt`]
    /// matches its CURRENT `attempt` — i.e. the live driver is the one that
    /// stamped the position — and it is still in `Streaming`/`Fenced`.
    ///
    /// The attempt stamp is the load-bearing half, and the state test alone is
    /// NOT sufficient (review P1-1): `retry_failed` flips a parked entry back to
    /// `Streaming` while leaving the PREVIOUS attempt's `snapshot_sequence` in
    /// place, and `take_failed_tasks` does that to every parked entry at once —
    /// so a state-only test re-arms a burst of permanently-unsatisfiable floors
    /// the moment the retry queue drains. `Preparing` has no stamp,
    /// `Complete`/`Failed` are excluded by both tests, restored entries get
    /// `None`, and [`Self::release_delta_reader_hold`] clears the stamp the
    /// instant Phase 3 has read the window.
    ///
    /// The returned sequence is the lowest one still NEEDED (inclusive), not an
    /// ACK watermark; see
    /// [`crate::server::dispatch::redo_reset_decision`] for the conversion and
    /// for the pressure escape hatch that keeps this soft hold from filling
    /// the log.
    pub fn delta_reader_redo_floor(&self) -> (usize, Option<u64>) {
        let mut holders = 0usize;
        let mut floor: Option<u64> = None;
        for p in &self.active {
            if p.snapshot_sequence == 0 || p.snapshot_hold_attempt != Some(p.attempt) {
                continue;
            }
            if p.state != MigrationState::Streaming && p.state != MigrationState::Fenced {
                continue;
            }
            holders += 1;
            floor = Some(floor.map_or(p.snapshot_sequence, |f: u64| f.min(p.snapshot_sequence)));
        }
        (holders, floor)
    }

    /// W16 review P2-2 — release this task's redo read hold, without touching
    /// the migration state machine or the recorded `snapshot_sequence`.
    ///
    /// Called by the migration worker the moment Phase 3 has resolved the delta
    /// window (`collect_migration_delta_ops` returned, either way). Pre-fix the
    /// hold lived until the task reached `Complete`/`Failed`, so it kept pinning
    /// the redo log across the manifest fold, the completion handshake, and its
    /// retries — long after nothing needed the window. On the failure path the
    /// release is equally correct: the task is about to be parked, and its
    /// re-drive stamps a fresh position at its own Phase 1.
    ///
    /// Returns `true` when a live hold was actually cleared.
    pub fn release_delta_reader_hold(&mut self, task: &MigrationTask) -> bool {
        match self.find_task_mut(task) {
            Some(p) if p.snapshot_hold_attempt.is_some() => {
                p.snapshot_hold_attempt = None;
                true
            }
            _ => false,
        }
    }

    /// Number of shards pending inbound data.
    pub fn inbound_count(&self) -> usize {
        self.inbound_migrations
            .iter()
            .filter(|m| !m.completed)
            .count()
    }

    /// W10 composition review P1-3b / P2-4 — uncompleted inbound entries that
    /// are genuine IN-FLIGHT MIGRATION WORK: [`Self::inbound_count`] minus
    /// every `heal_pending` reverse-heal fence.
    ///
    /// # Why the distinction exists
    ///
    /// [`Self::inbound_count`] counts EVERY uncompleted inbound entry, and the
    /// event-driven orphan-cleanup admissibility gate
    /// (`event_orphan_cleanup_admissible`) requires it to be ZERO. A
    /// reverse-heal fence is an inbound entry too, so a single heal fence
    /// disables that gate for as long as it is up — and #74 DESIGNS a parked
    /// no-source heal fence (`heal_pending` + the `NodeId(0)` sentinel) to
    /// hold FOREVER under alert-and-hold. One such park therefore disabled the
    /// event-driven orphan cleanup for the life of the process, bringing back
    /// the armed-17 disk-reclaim regression (a third RF=2 copy retained
    /// forever once the cluster settles and no batch-completion site fires
    /// again).
    ///
    /// # Why excluding heal fences is safe
    ///
    /// The gate's purpose is that "nothing this node is still RECEIVING as
    /// part of its topology plan can be misjudged around the pass". A
    /// `heal_pending` entry is not plan-driven inbound work:
    ///
    /// * the cleanup pass SKIPS any shard with a pending inbound entry
    ///   outright — `run_orphan_cleanup` and
    ///   `cleanup_orphaned_shard_if_settled` both gate on
    ///   [`Self::has_pending_inbound`] (W10 composition review P2-2), so a
    ///   heal-fenced shard is never an orphan candidate whether or not this
    ///   node masters it. Ownership alone would NOT be enough: the boot G3
    ///   path fences shards derived from lost create keys with no ownership
    ///   filter, and a persisted heal fence can be restored after a topology
    ///   change moved the shard away (#74 F1);
    /// * independently of that, the pass still refuses to delete without
    ///   positive committed-handoff evidence (#28) — the per-shard data-loss
    ///   guard is unchanged by this counter;
    /// * a park is ALERT-AND-HOLD state, not progress: waiting on it is
    ///   waiting on an operator, which is exactly the unbounded wait that
    ///   turned a transient A-side fence into a permanent B-side stall.
    ///
    /// Genuine forward inbound work (a plan-driven transfer, with or without a
    /// concrete source) still gates the pass exactly as before.
    pub fn inbound_migration_work_count(&self) -> usize {
        self.inbound_migrations
            .iter()
            .filter(|m| !m.completed && !m.heal_pending)
            .count()
    }

    /// W11 FIX 1 (default-09 CIRCULAR WAIT) — the subset of
    /// [`Self::inbound_migration_work_count`] that is this node's OWN PLAN
    /// work: uncompleted, non-heal inbound entries for shards `table`'s
    /// TARGET assignment actually gives `self_id`.
    ///
    /// # The starvation this exists to break
    ///
    /// The event-driven orphan-cleanup admissibility gate
    /// (`event_orphan_cleanup_admissible`) requires the pending-inbound count
    /// to be ZERO. Feeding it [`Self::inbound_migration_work_count`] let ONE
    /// entry the fail-closed inbound prune deliberately keeps
    /// (`inbound_entry_must_be_kept`: a shard this node does not hold but
    /// still has RECORDS for — dropping the fence would expose those orphans
    /// to local reads, the scenario-17 three-holder bug) disable the pass for
    /// EVERY OTHER SHARD. The entry cannot retire on its own either: the
    /// settled-inbound GC needs a SWIM-DEAD source
    /// ([`Self::orphaned_inbound_shards`]) and the 10 s pull requester
    /// re-stamps [`Self::mark_inbound_requested`] anyway.
    ///
    /// # Scope of the claim (W11 review P1-2 — corrected attribution)
    ///
    /// This does NOT unwedge the entry's own shard, and it never could: the
    /// pass skips any shard with a pending inbound entry one gate lower
    /// (`run_orphan_cleanup` / `cleanup_orphaned_shard_if_settled` both gate
    /// on [`Self::has_pending_inbound`]), and past that gate a node holding a
    /// merely-pushed copy has no committed-handoff evidence and is retained by
    /// #28. What it recovers is reclamation of the other ~4095 shards, which
    /// one such entry used to disable for the life of the process.
    ///
    /// The default-09 hang (CI 32055073890) is NOT this case and is not fixed
    /// here — the run's own artifacts settle it: node1 logged "per-shard
    /// orphan cleanup complete shard=1024 deleted=1" at 18:49:52, three
    /// minutes BEFORE the 18:52:51 hang, and `node1_migration_status.json` at
    /// the hang shows `active_count 0, failed_count 0, fenced_shards 0` with
    /// exactly the two entries `{from_node: 3, shard: 1024}` and
    /// `{from_node: 3, shard: 2902}`. The records were already gone; the
    /// entries survived because the 10 s transfer-request loop kept
    /// re-stamping them against a live node3 that had no tasks to send. The
    /// fix for that is the source's terminal refusal
    /// ([`Self::drop_refused_inbound`]).
    ///
    /// # Why excluding a NON-HELD inbound is the sound cut
    ///
    /// This is the mirror of the argument commit 39603fc already made for
    /// heal fences, inverted. A heal-fenced shard is structurally NOT an
    /// orphan candidate, so counting it could only ever block a pass that had
    /// nothing to do with it. A NON-HELD inbound is the exact opposite: the
    /// shard is not assigned here, so it IS precisely the orphan candidate
    /// the pass exists to reclaim — counting it is self-defeating.
    ///
    /// The gate's stated purpose survives intact: "nothing this node is still
    /// RECEIVING as part of its topology plan may be misjudged around the
    /// pass". An entry for a shard the target assignment does not give this
    /// node is, by definition, not plan work — nothing will ever be sent for
    /// it (the source refuses to hand off to a non-holder), which is why the
    /// prune's only reason to keep it is the leftover records.
    ///
    /// Safety is unchanged and does NOT rest on this counter:
    ///
    /// * the pass skips any shard with a pending inbound entry outright
    ///   (`run_orphan_cleanup` / `cleanup_orphaned_shard_if_settled` both gate
    ///   on [`Self::has_pending_inbound`]), so the excluded entry's own shard
    ///   is still never touched while the entry stands — the cut only lets the
    ///   pass judge the OTHER 4094 shards;
    /// * the per-shard #28 committed-handoff evidence guard is untouched.
    ///
    /// Uses `target_assignment` (not `effective_assignment`) so the predicate
    /// matches `inbound_entry_must_be_kept` exactly — the two must agree on
    /// "holder" or the cycle reopens under a mid-handoff table.
    pub fn inbound_plan_work_count(
        &self,
        table: &crate::cluster::shards::ShardTable,
        self_id: NodeId,
    ) -> usize {
        self.inbound_migrations
            .iter()
            .filter(|m| !m.completed && !m.heal_pending)
            .filter(|m| {
                let a = table.target_assignment(m.shard);
                a.master == self_id || a.replicas.contains(&self_id)
            })
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

    /// W12 TAIL 2 — the pending inbound entries whose own source has
    /// TERMINALLY refused them (`ERR_MIGRATION_NO_TASKS`) and which the
    /// fail-closed record guard retained anyway (see
    /// [`Self::drop_refused_inbound`]).
    ///
    /// These are a strict subset of [`Self::pending_inbound_entries`]: entries
    /// their own source has terminally refused and which the fail-closed guard
    /// retained anyway, on either ground —
    ///
    /// * [`InboundRetention::KeepOrphan`], immediately: this non-holder still
    ///   carries records for the shard, and only the
    ///   committed-handoff-gated orphan cleanup may reclaim them;
    /// * [`InboundRetention::KeepHolder`] (W16), after `terminal_after_rounds`
    ///   CONSECUTIVE refusals: this node IS the holder but the only source
    ///   that could prove its copy has said, round after round, that it never
    ///   will.
    ///
    /// Nothing is coming for either kind, so callers asking "is a migration
    /// still in flight?" must subtract them and callers asking "is anything
    /// wrong?" must report them. Both stay FENCED either way — subtracting
    /// them from the in-flight count is a statement about the transfer, never
    /// about the shard's availability.
    ///
    /// Not permanent by construction, and deliberately so: the mark clears the
    /// moment the entry becomes live again (a re-registration, a task
    /// registration, an inbound batch arriving, a re-park onto the sentinel, or
    /// the source matching the request in a later round) — see
    /// [`Self::drop_refused_inbound`] and [`Self::note_transfer_request_matched`].
    pub fn refused_retained_inbound_entries(&self) -> Vec<(u16, NodeId)> {
        self.inbound_migrations
            .iter()
            .filter(|m| !m.completed && m.refused_by_source)
            .map(|m| (m.shard, m.from_node))
            .collect()
    }

    /// W1.1 residual fix — stamp the listed pending inbound shards with the
    /// current instant, recording that this node has just sent an
    /// `OP_MIGRATION_TRANSFER_REQUEST` (pull-based repair) for them.
    ///
    /// The settled-inbound GC consults this stamp via
    /// [`Self::orphaned_inbound_shards`] so it will
    /// not reap an entry whose resend is still in flight.
    pub fn mark_inbound_requested(&mut self, shards: &std::collections::HashSet<u16>) {
        let now = std::time::Instant::now();
        for m in &mut self.inbound_migrations {
            if !m.completed && shards.contains(&m.shard) {
                m.transfer_requested_at = Some(now);
            }
        }
    }

    /// W1.1 residual fix + scenario 06 — the set of pending inbound shards
    /// the settled-inbound fast-path GC may treat as ORPHANED (source died
    /// mid-migration with no completion handshake) right now.
    ///
    /// An entry qualifies only when BOTH hold:
    ///
    /// * Its source is NOT in `alive_sources` — the SWIM-alive set (self
    ///   included; Suspect counts as alive until SWIM declares it Dead).
    ///   Request-interval settling alone is NOT death evidence: it
    ///   misclassified slow-but-live migrations as "source died" and fenced
    ///   up to ~2400/4096 shards LOST at once while every source was alive
    ///   (e2e armed scenario 06, 37% of writes NO_QUORUM). A slow-but-live
    ///   source's entries stay pending — fenced, and re-driven by the pull
    ///   requester every transfer-request interval until the completion
    ///   handshake lands. A sentinel `NodeId(0)` (no concrete source) is
    ///   never in the alive set, so a no-source entry stays reapable exactly
    ///   as before — the requester loop cannot pull it anyway.
    /// * It has no outstanding transfer request, OR its last request is
    ///   older than `request_grace` (so a lost request still gets reaped
    ///   eventually and the normal pull-based retry takes over). Entries
    ///   requested within `request_grace` are EXCLUDED: their source honours
    ///   the request and pushes the completion handshake AFTER the request
    ///   RPC returns, so reaping them mid-flight strands the shard.
    pub fn orphaned_inbound_shards(
        &self,
        request_grace: std::time::Duration,
        alive_sources: &std::collections::HashSet<NodeId>,
    ) -> std::collections::HashSet<u16> {
        let now = std::time::Instant::now();
        self.inbound_migrations
            .iter()
            .filter(|m| !m.completed)
            // C8 — a shard already marked LOST is terminal for the reap
            // fast-path: it stays fenced (unavailable) and must NOT be
            // re-processed, otherwise the GC would spin re-marking it every
            // cycle. It leaves this set only via a completeness proof or a
            // fresh re-acquiring migration (both clear the `lost` predicate).
            .filter(|m| !m.lost)
            // P0 — a `heal_pending` shard is a LIVE reverse-heal in flight: its
            // source is alive and being pulled, so the settled-inbound GC must
            // NOT reap it as an orphan (marking it `lost` would mis-raise the
            // `migration_lost` gauge and make `is_shard_lost` true for a shard
            // that is merely healing). It stays fenced fail-closed and keeps
            // being pulled; the completion handshake clears the marker.
            .filter(|m| !m.heal_pending)
            // Scenario 06 — a SWIM-alive source is slow, not dead: its entry
            // is never orphaned no matter how long it has settled. Only a
            // source SWIM has declared Dead (or dropped from membership) —
            // or the unpullable NodeId(0) sentinel — qualifies.
            .filter(|m| !alive_sources.contains(&m.from_node))
            .filter(|m| match m.transfer_requested_at {
                Some(at) => now.duration_since(at) >= request_grace,
                None => true,
            })
            .map(|m| m.shard)
            .collect()
    }

    /// C8 — mark the listed inbound shards as LOST (unavailable) rather than
    /// clearing their write fence.
    ///
    /// The settled-inbound GC calls this INSTEAD of
    /// [`Self::clear_pending_inbound_for_shards`] when it reaps an orphaned
    /// inbound entry (a source that died mid-migration with no completion
    /// handshake). Clearing the fence there is unsafe: it would let a shard
    /// this node holds only PARTIALLY answer `RunningCluster::is_master` as
    /// full authority and serve stale/incomplete reads and writes.
    ///
    /// The SAFE DEFAULT is fence-until-proven: only a NON-completed entry that
    /// was NOT already lost is marked (proven-complete entries are cleared by
    /// [`Self::cleanup_completed`] on the normal path and are never touched
    /// here). The shard's `inbound_bitmap` fence bit is deliberately KEPT set,
    /// so the shard stays client-invisible until a future migration completes
    /// it or an operator intervenes. Returns the number of shards newly marked
    /// lost.
    pub fn mark_inbound_lost(&mut self, shards: &std::collections::HashSet<u16>) -> usize {
        let mut marked = 0usize;
        for m in &mut self.inbound_migrations {
            if !m.completed && !m.lost && shards.contains(&m.shard) {
                // Fence bit is intentionally left SET — do NOT clear it. A
                // pending entry always holds its `inbound_bitmap` bit, so the
                // shard stays fenced/unavailable.
                m.lost = true;
                marked += 1;
            }
        }
        marked
    }

    /// C8 — whether `shard` currently has a pending inbound entry marked LOST
    /// (unavailable): received incompletely, source presumed dead, no
    /// completeness proof. Such a shard stays fenced and must never be served
    /// as full authority. Derived from the entries (off the hot path); the
    /// write hot path fences via the `inbound_bitmap` shadow, which a lost
    /// entry keeps set.
    pub fn is_shard_lost(&self, shard: u16) -> bool {
        self.inbound_migrations
            .iter()
            .any(|m| m.shard == shard && !m.completed && m.lost)
    }

    /// C8 — number of shards currently marked LOST (unavailable). Diagnostic /
    /// alerting hook.
    pub fn lost_count(&self) -> usize {
        self.inbound_migrations
            .iter()
            .filter(|m| !m.completed && m.lost)
            .count()
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
    /// Format: `[count:4][(shard:2 + from_node:8 + flags:1)] × count][crc32:4]`.
    /// The trailing CRC32 is computed over the count header and all entries.
    /// Only pending entries are persisted — completed ones are omitted.
    ///
    /// The per-entry `flags` byte (#74 re-review, F1×F4) persists the entry's
    /// KIND — [`INBOUND_ENTRY_FLAG_HEAL_PENDING`] and
    /// [`INBOUND_ENTRY_FLAG_LOST`] — so a restore can tell a reverse-heal
    /// fence / park from a forward migration and a C8 LOST mark survives a
    /// restart. Without it, a restored forward sentinel was byte-identical to
    /// a park (stranded uncompletable) and a restored concrete forward entry
    /// completed under the relaxed heal verify.
    ///
    /// The CRC lets [`Self::restore_inbound`] fail closed on a corrupt or
    /// truncated file rather than silently dropping write fences. This is a
    /// one-time on-disk format break from the flagless 10-byte-entry layout
    /// (itself a break from the pre-CRC layout): an old file fails the
    /// exact-length check and is rejected, which is the safe (still-fenced,
    /// loudly surfaced) outcome — deliberate, pre-production.
    pub fn serialize_inbound(&self) -> Vec<u8> {
        let pending: Vec<_> = self
            .inbound_migrations
            .iter()
            .filter(|m| !m.completed)
            .collect();
        let mut buf = Vec::with_capacity(4 + pending.len() * 11 + 4);
        buf.extend_from_slice(&(pending.len() as u32).to_le_bytes());
        for m in &pending {
            buf.extend_from_slice(&m.shard.to_le_bytes());
            buf.extend_from_slice(&m.from_node.0.to_le_bytes());
            let mut flags = 0u8;
            if m.heal_pending {
                flags |= INBOUND_ENTRY_FLAG_HEAL_PENDING;
            }
            if m.lost {
                flags |= INBOUND_ENTRY_FLAG_LOST;
            }
            buf.push(flags);
        }
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Restore inbound migrations from bytes produced by `serialize_inbound`.
    ///
    /// Entries restored this way start as non-completed, so the node will
    /// refuse writes for these shards until migration completes or is
    /// explicitly cleared.
    ///
    /// # Restored entries keep their persisted KIND — #74 F1 (re-review)
    ///
    /// Each entry's flag byte ([`INBOUND_ENTRY_FLAG_HEAL_PENDING`],
    /// [`INBOUND_ENTRY_FLAG_LOST`]) restores the entry as what it was:
    ///
    /// * a HEAL entry (concrete pull or parked `NodeId(0)` fence) restores
    ///   with `heal_pending = true` and a fresh Phase-3c deadline clock — a
    ///   park re-enters [`Self::parked_no_source_heal_shards`], survives the
    ///   join activation's [`Self::clear_inbound`], and stays F4-protected
    ///   (no foreign completion can consume it);
    /// * a LOST entry restores with `lost = true` — C8 fence-until-proven
    ///   survives the restart;
    /// * a FORWARD entry (flags clear) restores as a plain pending forward
    ///   inbound — completable by its source under the exact-holding forward
    ///   verify and droppable by a topology supersede, exactly as before
    ///   persistence. It is never mistaken for a park (the earlier
    ///   restore-as-unproven approach stranded a restored forward sentinel as
    ///   an uncompletable park — the F1×F4 interaction).
    ///
    /// Unknown flag bits are reserved and ignored (the CRC already rejects
    /// corruption; only a future writer could set them).
    ///
    /// # Fail-closed integrity
    ///
    /// The file is validated in full BEFORE any state is mutated: the declared
    /// entry count must match the file length exactly, and the trailing CRC32
    /// must match the checksum over the body. On any mismatch the manager is
    /// left untouched and an [`InboundRestoreError`] is returned. A corrupt or
    /// truncated fence file must never silently drop write fences — the caller
    /// must treat the affected shards as still fenced (unavailable) rather than
    /// serving them as complete authority. An OLD pre-CRC file has no trailing
    /// CRC and so fails this check (the safe outcome).
    ///
    /// Empty `data` (an absent state file) is not corruption and is a no-op.
    /// A bare 4-byte `[count=0]` file (the pre-CRC empty-state stub) is also
    /// accepted as an empty state, so an upgraded node with nothing to fence
    /// boots rather than bricking on the one-time format break.
    ///
    /// # Errors
    ///
    /// - [`InboundRestoreError::TooShort`] if `data` is non-empty but shorter
    ///   than the 8-byte minimum (count header + CRC) and is not the 4-byte
    ///   old empty-state stub.
    /// - [`InboundRestoreError::LengthMismatch`] if the declared count does not
    ///   match the file length (short read or trailing garbage).
    /// - [`InboundRestoreError::ChecksumMismatch`] if the trailing CRC32 does
    ///   not match the body.
    pub fn restore_inbound(&mut self, data: &[u8]) -> Result<(), InboundRestoreError> {
        // An absent file (empty bytes) carries no state — nothing to restore.
        if data.is_empty() {
            return Ok(());
        }
        // I-1 back-compat: the pre-CRC format wrote a bare 4-byte `[count=0]`
        // stub for the empty state (every completion that cleared the last
        // pending inbound rewrote the file this way, and it is never deleted).
        // It carries no fences and no CRC. Accept it as a valid empty state so
        // an upgraded node with NOTHING to fence boots cleanly instead of
        // crash-looping on a `TooShort` brick. A 4-byte file whose count is
        // non-zero cannot be this stub (an old file with N>=1 entries is
        // longer) — it is truncated/corrupt and still falls through to the
        // fail-closed `TooShort` below.
        if data.len() == 4 && u32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4])) == 0 {
            return Ok(());
        }
        if data.len() < 8 {
            return Err(InboundRestoreError::TooShort { len: data.len() });
        }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4]));
        // Exact-length check: [count:4] + count * [entry:11] + [crc:4]. This
        // rejects a truncated file (short read), trailing garbage, AND any
        // pre-flag-byte 10-byte-entry file (the one-time format break — the
        // old file fails closed here, the safe still-fenced outcome).
        let expected = 4usize
            .saturating_add((count as usize).saturating_mul(11))
            .saturating_add(4);
        if data.len() != expected {
            return Err(InboundRestoreError::LengthMismatch {
                count,
                expected,
                actual: data.len(),
            });
        }
        let crc_off = data.len() - 4;
        let stored = u32::from_le_bytes(data[crc_off..].try_into().unwrap_or([0; 4]));
        let computed = crc32fast::hash(&data[..crc_off]);
        if stored != computed {
            return Err(InboundRestoreError::ChecksumMismatch { stored, computed });
        }
        // Validation passed — apply the entries. No error path remains, so the
        // manager is never left partially mutated.
        let mut pos = 4;
        for _ in 0..count {
            let shard = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap_or([0; 2]));
            let from_node = NodeId(u64::from_le_bytes(
                data[pos + 2..pos + 10].try_into().unwrap_or([0; 8]),
            ));
            let flags = data[pos + 10];
            pos += 11;
            // Only add if not already present.
            if !self
                .inbound_migrations
                .iter()
                .any(|m| m.shard == shard && m.from_node == from_node)
            {
                // #74 F1 (re-review) — restore the entry AS ITS PERSISTED
                // KIND: a heal entry (concrete pull or park) comes back
                // `heal_pending` with a fresh Phase-3c deadline clock, a lost
                // entry comes back `lost` (C8 fence-until-proven), and a
                // forward entry comes back plain — completable and
                // supersede-droppable, never mistaken for a park.
                let heal = flags & INBOUND_ENTRY_FLAG_HEAL_PENDING != 0;
                let lost = flags & INBOUND_ENTRY_FLAG_LOST != 0;
                self.inbound_migrations.push(InboundMigration {
                    heal_pending: heal,
                    heal_started_at: heal.then(std::time::Instant::now),
                    lost,
                    ..InboundMigration::pending(shard, from_node)
                });
                self.inbound_bitmap.set(shard);
            }
        }
        Ok(())
    }

    /// Clear inbound migrations superseded by a topology change.
    ///
    /// C17 (SAFE DEFAULT) — a topology supersede must NOT silently unfence a
    /// shard this node received INCOMPLETELY and never proved complete (an
    /// entry marked LOST by [`Self::mark_inbound_lost`]). "No pending inbound"
    /// is not the same as "complete": dropping such a fence here would let
    /// `RunningCluster::is_master` serve a partial shard as full authority
    /// (the sibling of C8, at the supersede path instead of the GC path).
    ///
    /// LOST (unproven) entries are therefore RETAINED with their fence bit,
    /// so the shard stays client-invisible until it is genuinely completed.
    /// Ordinary in-flight (non-lost) inbound expectations are still dropped:
    /// the supersede's fresh plan re-registers whatever the new topology needs
    /// via [`Self::start_outbound`], and a re-acquiring migration clears the
    /// lost mark of any preserved entry it supersedes.
    ///
    /// P0 (reverse-heal Phase 2c) — a `heal_pending` entry is ALSO an unproven
    /// inbound (a boot reverse-heal pull, or the no-source fail-closed fence)
    /// that is deliberately NOT `lost` for the whole pull window (its source is
    /// alive / transfer-request grace keeps it non-lost). It is retained here
    /// for the SAME "fence-until-proven" reason as `lost`: dropping it would
    /// clear the no-serve-before-heal fence and let `is_master` serve an
    /// un-healed (lost/resurrected) tail as full authority — the P0
    /// double-spend. So the retain preserves BOTH `lost` and `heal_pending`.
    pub fn clear_inbound(&mut self) {
        self.inbound_migrations
            .retain(|m| !m.completed && (m.lost || m.heal_pending));
        self.inbound_bitmap.clear_all();
        for m in &self.inbound_migrations {
            // Every surviving entry is an unproven LOST or HEAL-PENDING shard —
            // keep it fenced.
            self.inbound_bitmap.set(m.shard);
        }
        // BUG4 (b): no non-lost shard is pending after this clear → drop the
        // whole accumulator (no-op off-path).
    }

    /// Remove pending inbound entries for the selected shards.
    ///
    /// This is used when the coordinator knows those shards are fully
    /// settled in the active topology and any remaining inbound entries are
    /// stale bookkeeping that would otherwise block the hot path forever. The
    /// migration-abort path (`RunningCluster::abort_inbound_migration`) also
    /// routes through here: a source that could not finish streaming tells the
    /// target to abandon the in-flight inbound.
    ///
    /// P0 (reverse-heal Phase 2c, ROUND 2) — a `heal_pending` entry is a boot
    /// reverse-heal PULL fence (or the no-source fail-closed fence) whose target
    /// IS the committed master of the shard. Unlike a FORWARD migration (where
    /// the target is not the master, so unfencing keeps `is_master` = `No`),
    /// dropping a reverse-heal fence here lets `RunningCluster::is_master`
    /// answer `Yes` for an UN-HEALED shard and serve a lost/resurrected tail as
    /// authority — the P0 double-spend. Reachable with no second fault: the
    /// source's baseline stream to the healing node hits a transient failure and
    /// sends `OP_MIGRATION_COMPLETE | FLAG_MIGRATION_ABORT`, unconditionally on
    /// `is_master`. So the retain PRESERVES `heal_pending` (mirroring
    /// [`Self::clear_inbound`]): an aborted reverse-heal shard STAYS fenced
    /// (`Transitioning`) and its non-completed entry stays pullable, so the
    /// requester loop re-requests the transient failure rather than fencing
    /// forever. A FORWARD-migration entry (heal_pending=false) for an aborted
    /// shard is still cleared (unchanged behaviour).
    ///
    /// Returns the number of entries removed.
    pub fn clear_pending_inbound_for_shards(
        &mut self,
        shards: &std::collections::HashSet<u16>,
    ) -> usize {
        let before = self.inbound_migrations.len();
        self.inbound_migrations
            .retain(|m| m.completed || !shards.contains(&m.shard) || m.heal_pending);
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
        }
        removed
    }

    /// Drop pending inbound entries for shards this node does not hold.
    ///
    /// `is_held(shard)` must answer from the node's own COMMITTED shard table:
    /// is this node the target master or a target replica for `shard`?
    ///
    /// # Why this is needed
    ///
    /// A pending inbound entry is otherwise only cleared two ways: the sender
    /// completes the handoff, or a topology change wipes the set wholesale via
    /// [`Self::clear_inbound`]. Once a cluster SETTLES on its final term neither
    /// happens again, so an entry for a shard that ended up assigned elsewhere
    /// waits forever — keeping the shard fenced and `inbound_count()` non-zero.
    ///
    /// The pull-based repair cannot rescue it either. The requester keeps asking
    /// the source every `TRANSFER_REQUEST_INTERVAL`; the source runs
    /// `split_transfer_request_tasks`, finds the requester is neither a target
    /// holder nor the intended master, produces no tasks, and returns
    /// `STATUS_OK`. Measured directly: node3 sent the request 11 times over
    /// ~110 s, node2 accepted all 11, and the 2 shards never moved.
    ///
    /// The requester does not need the source's cooperation to resolve this —
    /// "am I a holder for this shard" is decidable from its own committed table.
    /// Callers MUST only invoke this when the local table version equals the
    /// committed term, so a mid-transition table cannot drop an inbound the node
    /// is genuinely about to receive.
    ///
    /// Returns the number of entries removed.
    pub fn prune_inbound_not_held(&mut self, is_held: impl Fn(u16) -> bool) -> usize {
        let before = self.inbound_migrations.len();
        self.inbound_migrations
            .retain(|m| m.completed || is_held(m.shard));
        let removed = before - self.inbound_migrations.len();
        if removed > 0 {
            self.inbound_bitmap.clear_all();
            for m in &self.inbound_migrations {
                if !m.completed {
                    self.inbound_bitmap.set(m.shard);
                }
            }
        }
        removed
    }

    /// W11 FIX 4(a) — drop pending inbound entries for `shards` that `source`
    /// has TERMINALLY REFUSED (`ERR_MIGRATION_NO_TASKS`: the requester is
    /// neither a target holder nor its intended master at the epoch both
    /// sides activated).
    ///
    /// Scoped exactly to the refusal: only entries naming THAT source, only
    /// the listed shards, only uncompleted ones. Every other inbound entry —
    /// including one for the same shard from a different source — is
    /// untouched, so a refusal from a stale peer cannot cancel a live
    /// transfer.
    ///
    /// `retention` is the SAME fail-closed judgement the periodic not-held
    /// prune applies (`inbound_entry_retention`), but it must distinguish WHY
    /// an entry is kept — see [`InboundRetention`]. An entry whose shard still
    /// has local records is KEPT and stays fenced, because dropping the fence
    /// would expose those orphans to local reads (the scenario-17 three-holder
    /// bug). Orphan cleanup reclaims the records and the ordinary prune then
    /// drops the entry. The refusal therefore buys promptness for the safe
    /// case and changes nothing about the unsafe one.
    ///
    /// W11 review NIT — a `heal_pending` entry is never dropped either, no
    /// matter what the source says. A reverse-heal fence is designed to
    /// SURVIVE [`Self::clear_inbound`] precisely so a runtime topology commit
    /// cannot serve an un-healed tail as authority (the P0 double-spend); a
    /// peer's opinion about its own outbound tasks is weaker evidence than
    /// that, and it is not what the fence is waiting for.
    ///
    /// W12 TAIL 2 — an entry retained as [`InboundRetention::KeepOrphan`] is
    /// marked [`InboundMigration::refused_by_source`]. It is a fixpoint, not a
    /// transfer in flight: the only source that could complete it has said it
    /// never will, and only orphan cleanup (gated on committed-handoff
    /// evidence) can remove the records that hold the fence up.
    ///
    /// A [`InboundRetention::KeepHolder`] entry is not marked on the first
    /// refusal, even though it is retained by the same call. This node IS the
    /// shard's target holder, so the inbound is legitimate work — and the
    /// refusal that produced it can be a transient artefact of a DIVERGED
    /// source table (`terminally_abort_unshippable_task` rewrites
    /// `assignments[shard]` without bumping the version, so the source's holder
    /// test disagrees with ours while both epoch checks pass). The re-heal
    /// machinery re-plans that handoff on a later round; marking it there and
    /// then would tell status, the gauge and the convergence gate that a
    /// satisfiable transfer was terminal.
    ///
    /// # W16 — but a refusal STREAK is not a transient artefact
    ///
    /// "Not on the first refusal" was implemented as "never", and never is a
    /// FIXPOINT. CI 32644353574 (armed scenario 09) is the shape: nine shards
    /// failed their completion handshake on a record-count mismatch, node2
    /// terminally aborted the tasks, and node1 — the shards' target holder —
    /// then re-asked every 10 s and was answered `ERR_MIGRATION_NO_TASKS`
    /// every time. Eight consecutive sweeps logged `shards: 9, dropped: 0`
    /// with `refused_by_source: false` on all nine, so
    /// `in_flight_inbound_pending` counted them as live transfers and the
    /// convergence gate could never close, no matter what else converged.
    ///
    /// So the holder arm counts CONSECUTIVE refusals
    /// ([`InboundMigration::refusal_rounds`]) and believes the source after
    /// `terminal_after_rounds` of them — long enough for the re-heal to have
    /// re-planned the handoff if it was ever going to (the caller derives the
    /// number from the re-heal cooldown; see `REFUSED_HOLDER_TERMINAL_ROUNDS`).
    /// Any evidence of real work resets the streak, so a source that re-plans
    /// LATER still gets the full window again.
    ///
    /// What the mark does and does not do, because the distinction is the
    /// whole safety argument:
    ///
    /// * it does NOT drop the entry and does NOT lower the fence. The shard
    ///   stays client-invisible (`resolve_shard_ownership` answers
    ///   `ERR_MIGRATION_IN_PROGRESS` for a pending inbound), because this node
    ///   holds an UNPROVEN copy and serving it as authority is the P0 the
    ///   fence exists for;
    /// * it does change the CLASSIFICATION: `refused_retained_inbound_entries`
    ///   reports it, `/admin/migration_status` names it, the
    ///   `teraslab_migration_inbound_refused_retained` gauge counts it, and a
    ///   "is a migration still running?" question stops answering yes forever.
    ///
    /// `terminal_after_rounds` is clamped to at least 1: a zero would make the
    /// holder arm mark on the first refusal, which is the behaviour W12 review
    /// P1-1 removed.
    pub fn drop_refused_inbound(
        &mut self,
        shards: &[u16],
        source: NodeId,
        terminal_after_rounds: u32,
        retention: impl Fn(u16) -> InboundRetention,
    ) -> RefusedInboundOutcome {
        let terminal_after_rounds = terminal_after_rounds.max(1);
        let refused: std::collections::HashSet<u16> = shards.iter().copied().collect();
        let before = self.inbound_migrations.len();
        // Scoped to THIS refusal (P2-3): reporting the manager-global marked
        // set here named shards kept for a different source, and from earlier
        // rounds, in a line whose `source` field claimed otherwise.
        let mut kept_holder: Vec<u16> = Vec::new();
        let mut kept_holder_terminal: Vec<u16> = Vec::new();
        let mut kept_orphan: Vec<u16> = Vec::new();
        self.inbound_migrations.retain_mut(|m| {
            if m.completed || m.heal_pending || m.from_node != source || !refused.contains(&m.shard)
            {
                return true;
            }
            match retention(m.shard) {
                InboundRetention::Drop => false,
                InboundRetention::KeepHolder => {
                    m.refusal_rounds = m.refusal_rounds.saturating_add(1);
                    if m.refusal_rounds >= terminal_after_rounds {
                        m.refused_by_source = true;
                        kept_holder_terminal.push(m.shard);
                    } else {
                        kept_holder.push(m.shard);
                    }
                    true
                }
                InboundRetention::KeepOrphan => {
                    m.refusal_rounds = m.refusal_rounds.saturating_add(1);
                    m.refused_by_source = true;
                    kept_orphan.push(m.shard);
                    true
                }
            }
        });
        let removed = before - self.inbound_migrations.len();
        if removed > 0 {
            self.inbound_bitmap.clear_all();
            for m in &self.inbound_migrations {
                if !m.completed {
                    self.inbound_bitmap.set(m.shard);
                }
            }
        }
        RefusedInboundOutcome {
            dropped: removed,
            kept_holder,
            kept_holder_terminal,
            kept_orphan,
        }
    }

    /// W16 — record that `source` MATCHED (queued outbound work for) the named
    /// shards in this transfer-request round, resetting their
    /// consecutive-refusal streak and clearing any terminal mark.
    ///
    /// The streak that reclassifies a HOLDER entry
    /// ([`RefusedInboundOutcome::kept_holder_terminal`]) must count CONSECUTIVE
    /// refusals, so a round in which the source says "yes, I have a task for
    /// that shard" has to reset it. Without this a shard the source alternately
    /// queues and refuses — a source re-planning the handoff every re-heal
    /// round and rolling it back in between — would accumulate its way to a
    /// terminal mark it never earned.
    ///
    /// Scoped exactly like [`Self::drop_refused_inbound`]: only uncompleted
    /// entries naming THAT source and one of the listed shards. Returns the
    /// number of entries reset.
    pub fn note_transfer_request_matched(&mut self, shards: &[u16], source: NodeId) -> usize {
        let matched: std::collections::HashSet<u16> = shards.iter().copied().collect();
        let mut reset = 0usize;
        for m in self.inbound_migrations.iter_mut() {
            if m.completed || m.from_node != source || !matched.contains(&m.shard) {
                continue;
            }
            if m.refused_by_source || m.refusal_rounds > 0 {
                reset += 1;
            }
            m.clear_refusal();
        }
        reset
    }

    /// Serialize active outbound migration state to bytes.
    ///
    /// Format:
    /// ```text
    /// [count:4][ shard:2 + from_node:8 + to_node:8 + is_master:1
    ///   + state:1 + snapshot_seq:8 + fence_seq:8 ] × count [crc32:4]
    /// ```
    ///
    /// Per-entry size: 36 bytes. Only non-complete, non-failed entries
    /// are persisted — on restart these indicate migrations that were
    /// interrupted and may need to be re-initiated.
    ///
    /// The trailing CRC32 (computed over the count header and all entries,
    /// same scheme as [`Self::serialize_inbound`]) lets [`Self::restore_outbound`]
    /// fail closed on a corrupt or truncated file instead of silently
    /// mis-parsing it into wrong migration state (R17). This is a one-time
    /// on-disk format break; an old pre-CRC file with N>=1 entries is
    /// rejected by `restore_outbound` (the bare `[count=0]` empty-state stub
    /// is still accepted for back-compat, mirroring `restore_inbound`).
    pub fn serialize_outbound(&self) -> Vec<u8> {
        let active: Vec<_> = self
            .active
            .iter()
            .filter(|p| !p.is_complete() && p.state != MigrationState::Failed)
            .collect();
        let mut buf = Vec::with_capacity(4 + active.len() * 36 + 4);
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
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Restore outbound migration state from bytes produced by `serialize_outbound`.
    ///
    /// Restored entries start in the state they were serialized with.
    /// The coordinator can inspect these on startup to decide whether
    /// to resume, abort, or re-plan each migration.
    ///
    /// # Fail-closed integrity, advisory severity
    ///
    /// Unlike [`Self::restore_inbound`] (a safety fence — dropping a fence
    /// entry can let a partial shard serve as full authority), outbound state
    /// is ADVISORY: it only tells the coordinator which outbound migrations
    /// were in flight so it can resume/abort/re-plan them. So this still
    /// validates the WHOLE file before mutating anything (a truncated or
    /// CRC-corrupt file must never be silently mis-parsed into a wrong
    /// migration task — R17), but a caller that gets `Err` back should WARN
    /// and continue booting with no resumable outbound migrations rather than
    /// failing to come up, unlike a corrupt inbound-fence file. See
    /// `RunningCluster::restore_outbound_state` for the caller-side handling.
    ///
    /// Empty `data` (an absent state file) is not corruption and is a no-op.
    /// A bare 4-byte `[count=0]` file (the pre-CRC empty-state stub) is also
    /// accepted as an empty state.
    ///
    /// # Errors
    ///
    /// - [`OutboundRestoreError::TooShort`] if `data` is non-empty but
    ///   shorter than the 8-byte minimum (count header + CRC) and is not the
    ///   4-byte old empty-state stub.
    /// - [`OutboundRestoreError::LengthMismatch`] if the declared count does
    ///   not match the file length (short read or trailing garbage).
    /// - [`OutboundRestoreError::ChecksumMismatch`] if the trailing CRC32
    ///   does not match the body.
    pub fn restore_outbound(&mut self, data: &[u8]) -> Result<(), OutboundRestoreError> {
        // An absent file (empty bytes) carries no state — nothing to restore.
        if data.is_empty() {
            return Ok(());
        }
        // Back-compat: the pre-CRC format wrote a bare 4-byte `[count=0]`
        // stub for the empty state. Accept it as a valid empty state so an
        // upgraded node with nothing outstanding boots cleanly instead of
        // rejecting on the one-time format break (mirrors `restore_inbound`).
        if data.len() == 4 && u32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4])) == 0 {
            return Ok(());
        }
        if data.len() < 8 {
            return Err(OutboundRestoreError::TooShort { len: data.len() });
        }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4]));
        // Exact-length check: [count:4] + count * [entry:36] + [crc:4]. This
        // rejects both a truncated file (short read) and trailing garbage.
        let expected = 4usize
            .saturating_add((count as usize).saturating_mul(36))
            .saturating_add(4);
        if data.len() != expected {
            return Err(OutboundRestoreError::LengthMismatch {
                count,
                expected,
                actual: data.len(),
            });
        }
        let crc_off = data.len() - 4;
        let stored = u32::from_le_bytes(data[crc_off..].try_into().unwrap_or([0; 4]));
        let computed = crc32fast::hash(&data[..crc_off]);
        if stored != computed {
            return Err(OutboundRestoreError::ChecksumMismatch { stored, computed });
        }
        // Validation passed — apply the entries. No error path remains, so
        // `self.active` is never left partially mutated.
        let mut pos = 4;
        for _ in 0..count {
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
                let attempt = self.bump_attempt();
                let mut progress = MigrationProgress::from_task(&task);
                progress.attempt = attempt;
                progress.state = state;
                progress.snapshot_sequence = snapshot_sequence;
                progress.fence_sequence = fence_sequence;
                self.active.push(progress);
            }
        }
        Ok(())
    }
}

/// Error returned by [`MigrationManager::restore_inbound`] when the persisted
/// inbound-fence file fails its integrity checks.
///
/// Every variant is fail-closed: the manager's existing fences are left
/// untouched, and the caller must treat the affected shards as still fenced
/// (unavailable) rather than serving them as complete authority.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InboundRestoreError {
    /// The file is non-empty but too short to contain the 4-byte count header
    /// plus the trailing 4-byte CRC32.
    #[error("inbound-fence file too short: {len} bytes (need >= 8)")]
    TooShort { len: usize },

    /// The declared entry count does not match the file length — the file was
    /// truncated (short read) or carries trailing garbage.
    #[error(
        "inbound-fence file length mismatch: got {actual} bytes, expected {expected} for count={count}"
    )]
    LengthMismatch {
        count: u32,
        expected: usize,
        actual: usize,
    },

    /// The trailing CRC32 does not match the checksum computed over the body.
    #[error(
        "inbound-fence file checksum mismatch: stored={stored:#010x}, computed={computed:#010x}"
    )]
    ChecksumMismatch { stored: u32, computed: u32 },

    /// The inbound-fence file exists but could not be read — an I/O error
    /// distinct from "not found" (e.g. `EIO`/`EACCES`). Fail-closed: the file
    /// may record shards still fenced, so the node must not come up ignoring it
    /// (an absent file is instead reported as an empty state, not an error).
    #[error("inbound-fence file read error: {detail}")]
    ReadError { detail: String },
}

/// Error returned by [`MigrationManager::restore_outbound`] when the
/// persisted outbound-migration-state file fails its integrity checks.
///
/// Unlike [`InboundRestoreError`], outbound state is ADVISORY rather than a
/// safety fence: every variant still leaves the manager's existing `active`
/// list untouched (a corrupt/truncated file must never apply a partial or
/// wrong migration task — R17), but the caller treats an `Err` as "no
/// resumable outbound migrations" and continues booting rather than
/// bricking, unlike a corrupt inbound-fence file.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OutboundRestoreError {
    /// The file is non-empty but too short to contain the 4-byte count header
    /// plus the trailing 4-byte CRC32.
    #[error("outbound-migration-state file too short: {len} bytes (need >= 8)")]
    TooShort { len: usize },

    /// The declared entry count does not match the file length — the file was
    /// truncated (short read) or carries trailing garbage.
    #[error(
        "outbound-migration-state file length mismatch: got {actual} bytes, expected {expected} for count={count}"
    )]
    LengthMismatch {
        count: u32,
        expected: usize,
        actual: usize,
    },

    /// The trailing CRC32 does not match the checksum computed over the body.
    #[error(
        "outbound-migration-state file checksum mismatch: stored={stored:#010x}, computed={computed:#010x}"
    )]
    ChecksumMismatch { stored: u32, computed: u32 },
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
        // R5: the rename's directory entry is not durable until the parent
        // dir is fsync'd; without this a crash after rename but before the
        // dirent is durable can roll the fence file back to ABSENT, letting
        // a node fail to refuse writes for shards mid-migration (a
        // split-ownership / double-serve window). Same pattern as
        // `persist_cluster_state` / `persist_topology_state`.
        crate::fsutil::fsync_parent_dir(path)?;
        Ok(())
    })();
    if let Err(e) = result {
        tracing::warn!(err = %e, "cluster: failed to persist inbound migration state");
    }
}

/// Load inbound migration state from disk.
///
/// Returns the raw bytes for [`MigrationManager::restore_inbound`].
///
/// # Errors
///
/// I-2: an ABSENT file (`NotFound`) is a safe empty state and returns
/// `Ok(vec![])`. Any OTHER I/O error (e.g. `EIO`/`EACCES` on an existing fence
/// file) is fail-closed and returns [`InboundRestoreError::ReadError`] — the
/// bytes cannot be trusted as "no fences", so the caller must not silently drop
/// whatever fences the unreadable file recorded.
pub fn load_inbound_state(path: &std::path::Path) -> Result<Vec<u8>, InboundRestoreError> {
    match std::fs::read(path) {
        Ok(data) => Ok(data),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(InboundRestoreError::ReadError {
            detail: e.to_string(),
        }),
    }
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
        // R5: same durability gap as `persist_inbound_state` — make the
        // rename's directory entry durable so a crash cannot roll the
        // outbound-state file back to a stale/absent version.
        crate::fsutil::fsync_parent_dir(path)?;
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

    /// W16 — the `terminal_after_rounds` argument these tests pass to
    /// [`MigrationManager::drop_refused_inbound`].
    ///
    /// Mirrors the production `REFUSED_HOLDER_TERMINAL_ROUNDS`
    /// (`src/cluster/coordinator.rs`, private to that module) so the manager's
    /// behaviour is exercised at the real threshold; the `Drop` and
    /// `KeepOrphan` arms decide on the FIRST refusal and are insensitive to it.
    const TEST_TERMINAL_ROUNDS: u32 = 6;

    /// W17 — the pipeline-liveness counter the stranded-task reaper gates its
    /// dwell clock on. It must count exactly the FORWARD advances of a batch
    /// (a write fence raised, `Streaming`, `Fenced`, bytes/records moved,
    /// completion) and nothing else: task registration and task failure are not
    /// evidence that anything is being driven, and `mark_failed` is the
    /// reaper's own call, so counting it would let the reaper reset its own
    /// clock.
    ///
    /// Asserted as DELTAS rather than absolute totals: a fenced handoff
    /// legitimately advances twice (the write fence, then the state
    /// transition), and the reap gate only ever tests the counter for
    /// INEQUALITY between passes, so the exact magnitude of an advance carries
    /// no meaning and must not be pinned.
    #[test]
    fn pipeline_advances_counts_forward_batch_progress_only() {
        let self_id = NodeId(1);
        let no_keys = std::collections::HashSet::new();
        let mut mgr = MigrationManager::new();
        let task = MigrationTask {
            shard: 7,
            from_node: self_id,
            to_node: NodeId(2),
            is_master: true,
        };
        assert_eq!(
            mgr.pipeline_advances(),
            0,
            "a fresh manager has advanced nothing"
        );

        /// Advance delta produced by `step`.
        macro_rules! delta {
            ($mgr:expr, $step:expr) => {{
                let before = $mgr.pipeline_advances();
                $step;
                $mgr.pipeline_advances() - before
            }};
        }

        assert_eq!(
            delta!(
                mgr,
                mgr.start_outbound(std::slice::from_ref(&task), self_id, &no_keys)
            ),
            0,
            "registering a task is not driving it",
        );
        assert_eq!(
            delta!(mgr, mgr.set_snapshot_sequence(&task, 42)),
            1,
            "entering Streaming is an advance",
        );
        assert_eq!(
            delta!(mgr, mgr.record_progress(&task, 3, 128)),
            1,
            "moved records/bytes are an advance",
        );
        assert_eq!(
            delta!(mgr, mgr.record_progress(&task, 0, 0)),
            0,
            "a zero-work progress report moved nothing",
        );
        assert_eq!(
            delta!(mgr, mgr.mark_fenced(&task, 99)),
            2,
            "a fenced handoff advances twice: the write fence, then the state \
             transition (the RESYNC variant raises no fence and advances once)",
        );
        assert_eq!(
            delta!(mgr, mgr.mark_complete(&task)),
            1,
            "a completed handoff is an advance",
        );

        // A call naming a task this manager does not hold changes no task
        // state, so it must not read as progress. `mark_fenced` is the
        // exception and correctly so: it raises a real write fence on the
        // shard whether or not a task matches.
        let unknown = MigrationTask {
            shard: 4000,
            from_node: self_id,
            to_node: NodeId(3),
            is_master: true,
        };
        assert_eq!(
            delta!(mgr, mgr.set_snapshot_sequence(&unknown, 1)),
            0,
            "an unknown task must not advance the pipeline counter",
        );
        assert_eq!(delta!(mgr, mgr.record_progress(&unknown, 5, 5)), 0);
        assert_eq!(delta!(mgr, mgr.mark_complete(&unknown)), 0);
        assert_eq!(
            delta!(mgr, mgr.mark_fenced(&unknown, 1)),
            1,
            "the write fence it raises is real even with no matching task",
        );

        // Failure is a resolution, not an advance — and `unfence_shard`, which
        // both failure paths call, must not sneak one in either.
        let doomed = MigrationTask {
            shard: 8,
            from_node: self_id,
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(std::slice::from_ref(&doomed), self_id, &no_keys);
        assert_eq!(
            delta!(mgr, mgr.mark_failed(&doomed)),
            0,
            "failing a task must never reset the reaper's own clock",
        );
        assert_eq!(delta!(mgr, mgr.mark_failed_exact(&doomed)), 0);
        assert_eq!(
            delta!(mgr, mgr.unfence_shard(8)),
            0,
            "lifting a fence is not forward progress",
        );

        // W17 P2 — `retry_failed` re-drives a parked entry straight into
        // `Streaming` without going through `set_snapshot_sequence`. It is
        // deliberately UNCOUNTED, for the same reason `start_outbound` is: a
        // re-drive loop that keeps re-arming tasks nothing finishes must not
        // read as a live pipeline. (It does flip `queue_is_being_served`,
        // which shelters `Preparing` candidates — a pre-existing effect of the
        // `Streaming` state itself, not of this counter.)
        assert_eq!(
            delta!(mgr, mgr.retry_failed(&doomed)),
            0,
            "a re-drive is registration, not progress",
        );
    }

    /// W17 P1-1 — raising a BARE write fence is a pipeline advance.
    ///
    /// The empty-shard path in `run_migration_batch_with_origin` fences its
    /// shards with [`MigrationManager::fence_shard`] alone: the tasks stay in
    /// `Preparing`, nothing ever enters `Streaming`, and the next manager call
    /// that changes anything is the `mark_complete` AFTER the batched
    /// completion handshake — with `drain_in_flight_mutations` and a full
    /// `keys_by_shard_filtered` index pass in between. On an empty-dominated
    /// rebalance every per-target worker sits inside that window at once, so
    /// without this the node emits ZERO advances for the whole batch and the
    /// stranded-task reaper fires on a pipeline that is plainly working.
    #[test]
    fn raising_a_bare_write_fence_is_a_pipeline_advance() {
        let mut mgr = MigrationManager::new();
        assert_eq!(mgr.pipeline_advances(), 0);
        mgr.fence_shard(9);
        assert_eq!(
            mgr.pipeline_advances(),
            1,
            "the empty-shard path's bare fence is the only evidence it emits",
        );
        mgr.fence_shard(10);
        assert_eq!(mgr.pipeline_advances(), 2);
    }

    /// W16 direction 1 — a migration that has stamped its baseline snapshot
    /// sequence is holding a REDO READ POSITION, and the checkpoint reset guard
    /// must be able to see it.
    ///
    /// Armed scenario 06 (CI 32644361371) reclaimed the redo prefix 67 ms after
    /// a checkpoint that the reset guard let through, and 195 shards then failed
    /// their delta with `redo log truncated: need seq 6875, earliest available
    /// 8143` — where 8143 was that same checkpoint's `entries_before`. The guard
    /// consulted only the replication ACK tracker; nothing published the
    /// migration readers' positions.
    #[test]
    fn delta_reader_redo_floor_reports_streaming_and_fenced_holders() {
        let mut mgr = MigrationManager::new();
        let self_id = NodeId(1);
        let streaming = MigrationTask {
            shard: 10,
            from_node: self_id,
            to_node: NodeId(2),
            is_master: true,
        };
        let fenced = MigrationTask {
            shard: 11,
            from_node: self_id,
            to_node: NodeId(2),
            is_master: true,
        };
        let preparing = MigrationTask {
            shard: 12,
            from_node: self_id,
            to_node: NodeId(2),
            is_master: true,
        };
        let tasks = [streaming.clone(), fenced.clone(), preparing.clone()];
        mgr.start_outbound(&tasks, self_id, &std::collections::HashSet::new());

        // Nothing has stamped a snapshot sequence yet: no reader holds a
        // position, so nothing pins the redo log.
        assert_eq!(
            mgr.delta_reader_redo_floor(),
            (0, None),
            "a Preparing task has captured no redo position and must not pin the log",
        );

        mgr.set_snapshot_sequence(&streaming, 6875);
        mgr.set_snapshot_sequence(&fenced, 7000);
        mgr.mark_fenced(&fenced, 8000);

        assert_eq!(
            mgr.delta_reader_redo_floor(),
            (2, Some(6875)),
            "both the Streaming and the Fenced reader hold a position; the floor \
             is the minimum of them",
        );

        // Completing the lowest holder releases the floor up to the next one.
        mgr.mark_complete(&streaming);
        assert_eq!(
            mgr.delta_reader_redo_floor(),
            (1, Some(7000)),
            "a completed reader must release its hold",
        );
    }

    /// The floor must be released by FAILURE too — a parked entry in the
    /// durable retry queue keeps its stale `snapshot_sequence`, and honouring
    /// it would pin the redo log at a position no live reader needs (the
    /// re-drive stamps a fresh one at its own Phase 1).
    #[test]
    fn delta_reader_redo_floor_ignores_failed_and_completed_entries() {
        let mut mgr = MigrationManager::new();
        let self_id = NodeId(1);
        let task = MigrationTask {
            shard: 5,
            from_node: self_id,
            to_node: NodeId(3),
            is_master: false,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            self_id,
            &std::collections::HashSet::new(),
        );
        mgr.set_snapshot_sequence(&task, 4242);
        assert_eq!(mgr.delta_reader_redo_floor(), (1, Some(4242)));

        mgr.mark_failed(&task);
        assert_eq!(
            mgr.delta_reader_redo_floor(),
            (0, None),
            "a failed/parked entry must not pin the redo log",
        );
    }

    /// W16 review P1-1 — the retry drain must NOT republish a stale hold.
    ///
    /// `take_failed_tasks` calls `retry_failed` for EVERY parked entry at once,
    /// and `retry_failed` flips the state back to `Streaming` and bumps
    /// `attempt` but leaves `snapshot_sequence` at the PREVIOUS attempt's value
    /// — typically already below the log's earliest surviving sequence. A
    /// state-only holder filter therefore re-arms N permanently-unsatisfiable
    /// floors the instant the retry queue drains, and holds them until each
    /// worker reaches its own Phase 1 (pool queueing plus connect ladders, up
    /// to ~19 s per unreachable target). The hold must be keyed to the
    /// ATTEMPT that stamped it, so any re-drive invalidates it automatically.
    #[test]
    fn take_failed_tasks_does_not_republish_a_stale_redo_hold() {
        let mut mgr = MigrationManager::new();
        let self_id = NodeId(1);
        let task = MigrationTask {
            shard: 5,
            from_node: self_id,
            to_node: NodeId(3),
            is_master: false,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            self_id,
            &std::collections::HashSet::new(),
        );
        mgr.set_snapshot_sequence(&task, 4242);
        mgr.mark_failed(&task);

        let drained = mgr.take_failed_tasks();
        assert_eq!(drained.len(), 1, "the parked entry is drained for re-drive");
        assert_eq!(
            mgr.active_migrations()[0].state,
            MigrationState::Streaming,
            "precondition: retry_failed flipped it back to Streaming",
        );
        assert_eq!(
            mgr.active_migrations()[0].snapshot_sequence,
            4242,
            "precondition: retry_failed leaves the PREVIOUS attempt's sequence in place",
        );
        assert_eq!(
            mgr.delta_reader_redo_floor(),
            (0, None),
            "a re-driven entry holds nothing until its own Phase 1 stamps a fresh \
             sequence — the old one is unsatisfiable and would pin the log for the \
             whole re-drive latency",
        );

        // Its own Phase 1 re-arms the hold at the CURRENT position.
        mgr.set_snapshot_sequence(&task, 9000);
        assert_eq!(mgr.delta_reader_redo_floor(), (1, Some(9000)));
    }

    /// W16 review P2-2 — a `Fenced` entry keeps holding the floor long after
    /// its delta has been collected (manifest fold, completion handshake,
    /// retries). The natural release point is delta collection, not task
    /// completion.
    #[test]
    fn releasing_the_hold_at_delta_collection_frees_the_floor_before_completion() {
        let mut mgr = MigrationManager::new();
        let self_id = NodeId(1);
        let task = MigrationTask {
            shard: 9,
            from_node: self_id,
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            self_id,
            &std::collections::HashSet::new(),
        );
        mgr.set_snapshot_sequence(&task, 6875);
        mgr.mark_fenced(&task, 8000);
        assert_eq!(mgr.delta_reader_redo_floor(), (1, Some(6875)));

        // Phase 3 has read the window; nothing needs `[6875, 8000)` any more.
        assert!(
            mgr.release_delta_reader_hold(&task),
            "releasing a live hold reports the change",
        );
        assert_eq!(
            mgr.delta_reader_redo_floor(),
            (0, None),
            "the floor must be free before the completion handshake, not after it",
        );
        assert_eq!(
            mgr.active_migrations()[0].state,
            MigrationState::Fenced,
            "releasing the redo hold must not disturb the migration state machine",
        );
        assert_eq!(
            mgr.active_migrations()[0].snapshot_sequence,
            6875,
            "nor the recorded snapshot sequence itself",
        );
        assert!(
            !mgr.release_delta_reader_hold(&task),
            "releasing twice reports no change",
        );
    }

    /// W15 — the #28 evidence is per-`(shard, target)`, and a TERMINAL ABORT of
    /// any one target vetoes the shard's shed at that epoch even when a sibling
    /// handoff of the SAME shard committed.
    ///
    /// Shard-keyed evidence is what let CI run 32637576348 scenario 05 delete
    /// node1's last two copies of shard 959 ten seconds after node3 refused
    /// them.
    #[test]
    fn terminal_abort_of_one_target_vetoes_a_sibling_committed_handoff() {
        let mut mgr = MigrationManager::new();
        mgr.record_committed_handoff(959, NodeId(2), 4);
        assert!(
            mgr.has_committed_handoff(959, 4),
            "a committed handoff at the current epoch is the #28 evidence",
        );

        mgr.record_aborted_handoff(959, NodeId(3), 4);
        assert!(
            !mgr.has_committed_handoff(959, 4),
            "a DISTINCT handoff of the same shard that terminally aborted at the \
             same epoch must veto the shed — node3 just proved it does not hold \
             what node1 holds",
        );

        // The veto is per-SHARD-and-EPOCH, not global: an unrelated shard with
        // its own clean evidence is untouched.
        mgr.record_committed_handoff(960, NodeId(2), 4);
        assert!(
            mgr.has_committed_handoff(960, 4),
            "an abort on shard 959 must not block the shed of shard 960",
        );
    }

    /// W15, other direction — the veto must not wedge the legitimate shed shut,
    /// or it reintroduces the over-replication this campaign has been fighting.
    ///
    /// Two escapes, both pinned here: a re-drive that finally COMMITS to the
    /// same target supersedes its own abort within the epoch, and the veto is
    /// epoch-scoped exactly like the positive evidence, so a fresh handoff at a
    /// later epoch is judged on its own.
    #[test]
    fn abort_veto_is_superseded_by_a_later_commit_and_expires_with_the_epoch() {
        let mut mgr = MigrationManager::new();

        // Re-drive to the SAME target that aborted.
        mgr.record_aborted_handoff(700, NodeId(2), 4);
        assert!(
            !mgr.has_committed_handoff(700, 4),
            "the abort alone leaves no positive evidence at all",
        );
        mgr.record_committed_handoff(700, NodeId(2), 4);
        assert!(
            mgr.has_committed_handoff(700, 4),
            "a re-drive that commits to the aborted target must re-open the shed \
             in the SAME epoch — the ordinary retry path must not be wedged",
        );

        // Epoch scoping: an abort at epoch 4 says nothing about epoch 5.
        let mut mgr = MigrationManager::new();
        mgr.record_aborted_handoff(701, NodeId(3), 4);
        mgr.record_committed_handoff(701, NodeId(2), 5);
        assert!(
            mgr.has_committed_handoff(701, 5),
            "a stale-epoch abort must not veto a fresh committed handoff",
        );
        assert!(
            !mgr.has_committed_handoff(701, 4),
            "and the stale-epoch commit still does not apply to the old epoch",
        );
    }

    /// W15 — re-acquiring the shard as an inbound target supersedes EVERY prior
    /// transfer of it, aborts included: a retained veto would block a shed the
    /// fresh migration is entitled to authorize on its own.
    #[test]
    fn reacquiring_the_shard_clears_the_abort_veto_too() {
        let mut mgr = MigrationManager::new();
        mgr.record_aborted_handoff(702, NodeId(3), 4);
        mgr.record_committed_handoff(702, NodeId(2), 4);
        assert!(
            !mgr.has_committed_handoff(702, 4),
            "precondition: the abort is vetoing the shed",
        );

        let inbound = MigrationTask {
            shard: 702,
            from_node: NodeId(2),
            to_node: NodeId(1),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&inbound),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        // Everything is gone — no evidence AND no veto.
        assert!(
            !mgr.has_committed_handoff(702, 4),
            "re-acquisition drops the committed evidence (task #28)",
        );
        mgr.record_committed_handoff(702, NodeId(2), 4);
        assert!(
            mgr.has_committed_handoff(702, 4),
            "re-acquisition must also drop the stale abort veto, or a fresh \
             committed handoff can never authorize a shed again",
        );
    }

    /// W15 review P1-1 — reaping a `Failed` OUTBOUND entry must take the
    /// shard's handoff evidence with it.
    ///
    /// A `Failed` entry is not bookkeeping: it is the block both orphan-cleanup
    /// gates key their unresolved-task skip off. Reaping it removes the block,
    /// so if a sibling handoff of the same shard committed, the shed is
    /// authorized over records the failed task was still trying to deliver —
    /// the scenario-05 chain one step over. Recording an `Aborted` for an
    /// ordinary failure would be wrong (a connection reset is no evidence the
    /// target lacks a record); dropping the evidence with the block is the
    /// fail-closed equivalent.
    #[test]
    fn reaping_a_failed_outbound_task_drops_the_shards_handoff_evidence() {
        let mut mgr = MigrationManager::new();
        // Master handoff S:n1→n2 commits — the #28 evidence.
        mgr.record_committed_handoff(800, NodeId(2), 4);
        // Replica push S:n1→n3 fails ORDINARILY (not a terminal abort).
        let replica = MigrationTask {
            shard: 800,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: false,
        };
        mgr.start_outbound(
            std::slice::from_ref(&replica),
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        mgr.mark_failed(&replica);
        assert!(
            mgr.has_committed_handoff(800, 4),
            "precondition: the commit IS the evidence — what stops the shed \
             right now is the Failed entry, not the evidence check",
        );
        assert!(
            mgr.active_migrations()
                .iter()
                .any(|p| p.shard == 800 && p.state == MigrationState::Failed),
            "precondition: the Failed entry is the standing block",
        );

        // The event loop's periodic prune reaps it once the retry hold drops.
        mgr.cleanup_completed();

        assert!(
            !mgr.active_migrations().iter().any(|p| p.shard == 800),
            "precondition: the prune really reaped the entry",
        );
        assert!(
            !mgr.has_committed_handoff(800, 4),
            "reaping the Failed entry removed the only thing blocking the shed, \
             so the evidence must go with it — otherwise the next sweep deletes \
             every local record of shard 800",
        );
    }

    /// The other half of P1-1: while the retry HOLD is raised, the `Failed`
    /// entry is PRESERVED, so the block is still standing and the evidence must
    /// NOT be dropped. Pins that the drop is tied to the reap and not to
    /// `cleanup_completed` being called at all.
    #[test]
    fn preserving_a_failed_entry_keeps_the_shards_handoff_evidence() {
        let mut mgr = MigrationManager::new();
        mgr.record_committed_handoff(801, NodeId(2), 4);
        let replica = MigrationTask {
            shard: 801,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: false,
        };
        mgr.start_outbound(
            std::slice::from_ref(&replica),
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        mgr.mark_failed(&replica);
        mgr.arm_failed_batch_retry(); // raises the hold

        mgr.cleanup_completed();

        assert!(
            mgr.active_migrations()
                .iter()
                .any(|p| p.shard == 801 && p.state == MigrationState::Failed),
            "the hold must preserve the Failed entry (W8 P0-2)",
        );
        assert!(
            mgr.has_committed_handoff(801, 4),
            "the block is still standing, so the evidence must survive — \
             dropping it here would be pointless churn",
        );
    }

    /// W15 review P2-1 — a VERIFIED completion retires that target's abort even
    /// when it does not commit, and that is the case that matters.
    ///
    /// `should_commit` is `is_master || target_assignment names from_node`, so a
    /// replica push from a node the new table makes a NON-OWNER completes with
    /// `commit == false` and never reaches `record_committed_handoff`. Without a
    /// separate clear, its `Aborted` record survives every successful re-drive
    /// and the shard is retained until the epoch advances — real
    /// over-replication on exactly the shape the veto bites on.
    #[test]
    fn a_verified_completion_retires_that_targets_abort_without_manufacturing_evidence() {
        let mut mgr = MigrationManager::new();
        mgr.record_committed_handoff(810, NodeId(2), 4); // master handoff committed
        mgr.record_aborted_handoff(810, NodeId(3), 4); // replica push terminally aborted
        assert!(
            !mgr.has_committed_handoff(810, 4),
            "precondition: the abort is vetoing the shed",
        );

        // The replica push is re-driven and the target VERIFIES it — but the
        // completion does not commit, so no evidence is ever recorded for n3.
        mgr.clear_handoff_abort(810, NodeId(3));

        assert!(
            !mgr.has_aborted_handoff(810, 4),
            "the verified completion retires the veto",
        );
        assert!(
            mgr.has_committed_handoff(810, 4),
            "and the SIBLING committed handoff is usable again — this is the \
             escape hatch that did not exist for replica-side aborts",
        );

        // Clearing must never MANUFACTURE evidence out of nothing.
        let mut bare = MigrationManager::new();
        bare.clear_handoff_abort(811, NodeId(3));
        assert!(
            !bare.has_committed_handoff(811, 4),
            "clearing an abort on a shard with no evidence must not create any",
        );
    }

    /// P2-1 guard rail — the clear is ABORT-ONLY. Erasing a `Committed` slot
    /// would silently withdraw positive evidence and strand the shard forever.
    #[test]
    fn clearing_an_abort_never_erases_committed_evidence() {
        let mut mgr = MigrationManager::new();
        mgr.record_committed_handoff(812, NodeId(2), 4);
        mgr.clear_handoff_abort(812, NodeId(2));
        assert!(
            mgr.has_committed_handoff(812, 4),
            "the target's COMMITTED record must survive an abort-clear aimed at \
             the same target",
        );
    }

    /// W13 review item 4 — a state reset must not silently rewrite the
    /// OPERATOR's policy.
    ///
    /// The activation paths wipe the manager with `*mgr = MigrationManager::new()`
    /// to drop transient in-flight state. That also reverted every
    /// config-carried arming bit to its COMPILE-TIME default, in both
    /// directions and with no log line: a deliberately DISARMED weak-veto
    /// arbitration silently re-armed (its default is `true`), and a
    /// deliberately ARMED orphan-proof reclaim silently disarmed. Policy is not
    /// in-flight state, so [`MigrationManager::reset_transient_state`] carries
    /// it across the reset.
    #[test]
    fn reset_transient_state_preserves_operator_policy_but_clears_in_flight_work() {
        let mut mgr = MigrationManager::new();
        // A policy that differs from EVERY compile-time default, so a reset
        // that reverts to `new()` cannot pass by coincidence.
        mgr.set_orphan_cleanup_proof_reclaim_enabled(true); // default false
        mgr.set_weak_veto_arbitration_enabled(false); // default true
        mgr.set_vetoed_reduction_enabled(true); // default false
        mgr.set_replica_abort_forced_resync_enabled(false); // default true

        // Real in-flight state: an outbound task and a fence.
        let task = MigrationTask {
            shard: 11,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::from([11u16]),
        );
        mgr.fence_shard(11);
        assert!(
            !mgr.active_migrations().is_empty(),
            "fixture: task registered"
        );
        assert_eq!(mgr.fenced_count(), 1, "fixture: shard fenced");

        mgr.reset_transient_state();

        // Transient state is gone — the whole point of the reset.
        assert!(
            mgr.active_migrations().is_empty(),
            "reset must drop in-flight outbound tasks",
        );
        assert_eq!(mgr.fenced_count(), 0, "reset must drop fences");
        assert!(
            mgr.pending_inbound_entries().is_empty(),
            "reset must drop inbound entries",
        );

        // Policy survived, in both directions.
        assert!(
            mgr.orphan_cleanup_proof_reclaim_enabled(),
            "an ARMED orphan-proof reclaim must not silently disarm on reset",
        );
        assert!(
            !mgr.weak_veto_arbitration_enabled(),
            "a DISARMED weak-veto arbitration must not silently RE-ARM on reset",
        );
        assert!(
            mgr.vetoed_reduction_enabled(),
            "vetoed-reduction arming must survive a reset",
        );
        // No accessor for the replica-abort policy: assert its behaviour —
        // a disabled policy records no arm.
        mgr.arm_replica_abort_resync();
        assert!(
            !mgr.take_replica_abort_resync_arm(),
            "a DISABLED replica-abort forced resync must not silently re-enable \
             on reset",
        );
    }

    /// W13 CONTAINMENT — a freshly constructed manager carries the
    /// proof-of-elsewhere orphan reclaim DISARMED, so every code path that
    /// builds its own manager (tests, tools, future call sites) inherits the
    /// fail-closed #28 posture unless it explicitly opts in.
    #[test]
    fn orphan_cleanup_proof_reclaim_defaults_disarmed() {
        let mut mgr = MigrationManager::new();
        assert!(
            !mgr.orphan_cleanup_proof_reclaim_enabled(),
            "a new MigrationManager must leave the proof reclaim disarmed",
        );
        mgr.set_orphan_cleanup_proof_reclaim_enabled(true);
        assert!(
            mgr.orphan_cleanup_proof_reclaim_enabled(),
            "the setter must arm the reclaim (the operator opt-in path)",
        );
        mgr.set_orphan_cleanup_proof_reclaim_enabled(false);
        assert!(
            !mgr.orphan_cleanup_proof_reclaim_enabled(),
            "disarming must be a complete local rollback",
        );
    }

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

    /// W9 nit — `mark_failed` resolves by (shard, from, to) and can hit
    /// the TWIN entry when a master and a replica task share the same
    /// endpoints; the det-degrade cancel path must fail exactly the task
    /// it names. `mark_failed_exact` matches `is_master` too.
    #[test]
    fn mark_failed_exact_fails_only_the_named_twin() {
        let mut mgr = MigrationManager::new();
        let master = MigrationTask {
            shard: 9,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let replica = MigrationTask {
            is_master: false,
            ..master
        };
        mgr.start_outbound(
            &[master.clone(), replica.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        assert_eq!(mgr.active_count(), 2, "both twins registered");

        mgr.mark_failed_exact(&replica);

        let state_of = |mgr: &MigrationManager, is_master: bool| {
            mgr.active_migrations()
                .iter()
                .find(|p| p.shard == 9 && p.is_master == is_master)
                .map(|p| p.state.clone())
                .expect("twin entry present")
        };
        assert_eq!(
            state_of(&mgr, false),
            MigrationState::Failed,
            "the NAMED twin (replica) must be failed",
        );
        assert_eq!(
            state_of(&mgr, true),
            MigrationState::Preparing,
            "the other twin (master) must be untouched — the 3-tuple \
             lookup would have hit it first",
        );

        // Failing the remaining twin exactly works too.
        mgr.mark_failed_exact(&master);
        assert_eq!(state_of(&mgr, true), MigrationState::Failed);
        assert_eq!(mgr.active_count(), 0);
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
    fn has_pending_heal_from_source_discriminates_heal_from_forward() {
        let mut mgr = MigrationManager::new();
        let source = NodeId(2);

        // A plain forward inbound is NOT a heal.
        assert!(mgr.register_inbound_source(10, source));
        assert!(
            !mgr.has_pending_heal_from_source(10, source),
            "a forward inbound entry (heal_pending clear) is not a heal",
        );

        // A registered heal source IS a heal for its (shard, source).
        assert!(mgr.register_heal_source(11, source));
        assert!(mgr.has_pending_heal_from_source(11, source));
        // Wrong source does not match.
        assert!(!mgr.has_pending_heal_from_source(11, NodeId(9)));
        // Wrong shard does not match.
        assert!(!mgr.has_pending_heal_from_source(12, source));

        // #74 F4 — a PARKED no-source fence (NodeId(0) sentinel, heal_pending)
        // matches NO source: no source was ever selected for it, so no
        // arriving completion can be "its" heal (classifying one as such would
        // relax the verify and clear the park with nothing healed).
        assert!(mgr.mark_heal_fence_active(13));
        assert!(!mgr.has_pending_heal_from_source(13, NodeId(42)));

        // Once completed, the heal no longer matches.
        mgr.mark_inbound_complete_from_source(11, source);
        assert!(
            !mgr.has_pending_heal_from_source(11, source),
            "a completed heal entry is no longer pending",
        );
    }

    #[test]
    fn mark_inbound_complete_source_less_prefers_non_heal_entry() {
        // A shard with BOTH a forward inbound (no source, heal_pending clear) and a
        // heal fence: a SOURCE-LESS completion must complete the forward entry and
        // leave the heal fence UP (it did not prove the heal's completeness).
        let mut mgr = MigrationManager::new();
        let shard = 20u16;
        assert!(mgr.mark_inbound_active(shard)); // forward inbound, NodeId(0), no heal
        assert!(mgr.register_heal_source(shard, NodeId(3))); // heal fence
        assert_eq!(mgr.inbound_count(), 2);
        assert!(mgr.has_pending_heal_from_source(shard, NodeId(3)));

        mgr.mark_inbound_complete(shard);

        // The heal fence must survive — the source-less completion cleared the
        // forward entry, not the heal.
        assert!(
            mgr.has_pending_heal_from_source(shard, NodeId(3)),
            "source-less completion must not clear the heal fence",
        );
        assert_eq!(mgr.inbound_count(), 1, "only the forward entry completed");
        assert!(
            mgr.has_pending_inbound(shard),
            "the shard stays fenced while the heal is pending",
        );

        // A second source-less completion now (only the heal remains) does
        // complete it — the fallback path.
        mgr.mark_inbound_complete(shard);
        assert!(!mgr.has_pending_heal_from_source(shard, NodeId(3)));
        assert!(!mgr.has_pending_inbound(shard));
    }

    /// #74 F1 (re-review revision) — the round-trip preserves each entry's
    /// KIND via the persisted flag byte. A PARK restores as a park (durable:
    /// re-enters the parked set, survives the join activation's
    /// `clear_inbound`, carries a Phase-3c deadline clock); a CONCRETE heal
    /// restores as a heal (retained, deadline clock, completion stays
    /// drop-aware); a LOST entry restores lost (retained, fence-until-proven);
    /// a FORWARD entry restores as a FORWARD entry — droppable by the
    /// supersede exactly as before persistence, never mistaken for a park.
    #[test]
    fn restore_inbound_round_trip_preserves_entry_kinds() {
        let mut mgr = MigrationManager::new();
        assert!(mgr.mark_heal_fence_active(3)); // park (NodeId(0) sentinel)
        assert!(mgr.register_heal_source(9, NodeId(4))); // sourced heal
        assert!(mgr.mark_inbound_active(12)); // forward sentinel
        assert!(mgr.register_inbound_source(15, NodeId(6))); // forward, concrete
        mgr.mark_inbound_lost(&[15u16].into_iter().collect()); // C8 lost
        let bytes = mgr.serialize_inbound();

        let mut restored = MigrationManager::new();
        restored
            .restore_inbound(&bytes)
            .expect("round-trip restores");
        // Park → park.
        assert_eq!(
            restored.parked_no_source_heal_shards(),
            vec![3],
            "ONLY the park restores parked — the forward sentinel (12) must \
             not be mistaken for one",
        );
        // Concrete heal → heal (its completion stays drop-aware).
        assert!(restored.has_pending_heal_from_source(9, NodeId(4)));
        // Lost → lost (fence-until-proven survives the restart).
        assert!(restored.is_shard_lost(15), "the C8 lost mark is durable");
        // Forward entries restore as forward: not heals, not parks.
        assert!(!restored.has_pending_heal_from_source(15, NodeId(6)));
        // Only the HEAL entries carry the Phase-3c deadline clock.
        assert_eq!(
            restored.expired_heal_shards(std::time::Duration::ZERO),
            vec![3, 9],
        );

        // The supersede keeps the park, the heal, and the lost entry —
        // and drops the plain forward sentinel exactly as before a28ec4e.
        restored.clear_inbound();
        assert_eq!(restored.inbound_count(), 3);
        for shard in [3u16, 9, 15] {
            assert!(
                restored.inbound_bitmap().test(shard),
                "restored shard {shard} stays fenced until proven",
            );
        }
        assert!(
            !restored.inbound_bitmap().test(12),
            "a restored plain forward sentinel is droppable by the supersede \
             (the pre-a28ec4e behavior)",
        );
    }

    /// #74 F1×F4 (re-review P1) — the STRANDED-FORWARD shape: a restored
    /// forward sentinel must stay completable by whichever source streams it.
    /// Without the persisted kind flag, restore-as-unproven turned it into a
    /// park and F4's exclusions made it uncompletable by ANY source — the
    /// fence never cleared, and with reverse-heal disabled there was no exit
    /// at all (breaking the "off ⇒ prior runtime behavior" invariant).
    #[test]
    fn restored_forward_sentinel_stays_completable_by_any_source() {
        let mut mgr = MigrationManager::new();
        assert!(mgr.mark_inbound_active(42)); // forward, source unknown
        let bytes = mgr.serialize_inbound();

        let mut restored = MigrationManager::new();
        restored
            .restore_inbound(&bytes)
            .expect("round-trip restores");
        assert!(restored.has_pending_inbound(42));

        // The real source streams the shard and completes — the fence clears.
        restored.mark_inbound_complete_from_source(42, NodeId(7));
        assert!(
            !restored.has_pending_inbound(42),
            "a restored forward sentinel must be completable by its source — \
             never stranded as an uncompletable park",
        );
        assert!(!restored.inbound_bitmap().test(42));
    }

    /// #74 F1×F4 (re-review P1) — a restored CONCRETE forward entry completes
    /// under FORWARD tolerances: its completion must NOT classify as a heal
    /// (`has_pending_heal_from_source` false), so the exact-holding verify is
    /// not silently relaxed to the drop-aware heal gate.
    #[test]
    fn restored_concrete_forward_completes_under_forward_tolerances() {
        let mut mgr = MigrationManager::new();
        assert!(mgr.register_inbound_source(8, NodeId(7)));
        let bytes = mgr.serialize_inbound();

        let mut restored = MigrationManager::new();
        restored
            .restore_inbound(&bytes)
            .expect("round-trip restores");
        assert!(
            !restored.has_pending_heal_from_source(8, NodeId(7)),
            "a restored forward entry's completion must keep exact-holding \
             (forward) semantics, not the drop-aware heal tolerance",
        );
        // And it completes normally from its source.
        restored.mark_inbound_complete_from_source(8, NodeId(7));
        assert!(!restored.has_pending_inbound(8));
    }

    /// #74 F4 — a FOREIGN source's completion must not clear a PARK: the park
    /// never selected a source, so no completion can prove it healed. A park
    /// exits ONLY via `resolve_heal_source` + THAT concrete source's
    /// completion (or operator action).
    #[test]
    fn parked_fence_survives_foreign_source_completion() {
        let mut mgr = MigrationManager::new();
        assert!(mgr.mark_heal_fence_active(7));

        // Not classified as this park's heal completion...
        assert!(!mgr.has_pending_heal_from_source(7, NodeId(5)));
        // ...and not completed by it: nothing was healed.
        mgr.mark_inbound_complete_from_source(7, NodeId(5));
        assert_eq!(
            mgr.parked_no_source_heal_shards(),
            vec![7],
            "a foreign completion must not consume the park",
        );
        assert!(mgr.inbound_bitmap().test(7), "the park stays fenced");

        // The only heal exit: resolve to a concrete source, then complete
        // FROM that source.
        assert!(mgr.resolve_heal_source(7, NodeId(5)));
        assert!(mgr.has_pending_heal_from_source(7, NodeId(5)));
        mgr.mark_inbound_complete_from_source(7, NodeId(5));
        assert!(
            !mgr.inbound_bitmap().test(7),
            "the resolved source's completion clears the fence",
        );
        assert!(mgr.parked_no_source_heal_shards().is_empty());
    }

    /// #74 — a parked no-source heal fence (`NodeId(0)` sentinel,
    /// `heal_pending`) is enumerated by `parked_no_source_heal_shards` and
    /// RESOLVED IN PLACE to a concrete quorum-current source: same entry, new
    /// `from_node`, fence still up — never a second entry, so the single
    /// completion handshake clears the whole fence.
    #[test]
    fn resolve_heal_source_resolves_parked_sentinel_in_place() {
        let mut mgr = MigrationManager::new();
        assert!(mgr.mark_heal_fence_active(7));
        assert_eq!(mgr.parked_no_source_heal_shards(), vec![7]);

        assert!(mgr.resolve_heal_source(7, NodeId(3)));
        assert!(
            mgr.parked_no_source_heal_shards().is_empty(),
            "resolved — no longer parked",
        );
        assert_eq!(
            mgr.pending_inbound_entries(),
            vec![(7, NodeId(3))],
            "the SAME entry now names the concrete source (no sibling entry)",
        );
        assert!(
            mgr.inbound_bitmap().test(7),
            "the fence stays up until completion",
        );
        assert!(
            mgr.has_pending_heal_from_source(7, NodeId(3)),
            "the resolved entry is still a heal (completion stays drop-aware)",
        );

        // The single completion handshake clears the fence entirely.
        mgr.mark_inbound_complete_from_source(7, NodeId(3));
        assert!(
            !mgr.inbound_bitmap().test(7),
            "one completion clears the resolved fence",
        );
    }

    /// #74 — resolve is a strict no-op when there is nothing to resolve: no
    /// entry at all, a FORWARD-migration sentinel (`heal_pending` clear), or a
    /// `NodeId(0)` "source" each answer `false` and mutate nothing.
    #[test]
    fn resolve_heal_source_ignores_non_heal_and_missing_entries() {
        let mut mgr = MigrationManager::new();
        assert!(
            !mgr.resolve_heal_source(9, NodeId(2)),
            "nothing registered → nothing to resolve",
        );

        // A FORWARD sentinel (REPLICA_BATCH arrival, heal_pending clear) is
        // not a parked heal — its source is assigned by migration dispatch.
        assert!(mgr.mark_inbound_active(9));
        assert!(!mgr.resolve_heal_source(9, NodeId(2)));
        assert_eq!(
            mgr.pending_inbound_entries(),
            vec![(9, NodeId(0))],
            "the forward sentinel is untouched",
        );
        assert!(
            mgr.parked_no_source_heal_shards().is_empty(),
            "a forward sentinel is not a parked HEAL",
        );

        // NodeId(0) is never a resolvable source.
        assert!(mgr.mark_heal_fence_active(11));
        assert!(!mgr.resolve_heal_source(11, NodeId(0)));
        assert_eq!(
            mgr.parked_no_source_heal_shards(),
            vec![11],
            "a NodeId(0) \"source\" resolves nothing — the shard stays parked",
        );
    }

    /// #74 F5 — a RESOLVED heal pull whose concrete source terminally failed
    /// (SWIM-dead / left membership, no request in flight) is RE-PARKED: back
    /// to the `NodeId(0)` sentinel with the fence kept, so the re-source pass
    /// can pick a fresh quorum-current source later — the pick is never
    /// sticky-forever. A slow-but-ALIVE source and a mid-flight request are
    /// never re-parked, and a forward entry is never touched.
    #[test]
    fn repark_dead_source_heals_reparks_terminally_failed_pick() {
        let mut mgr = MigrationManager::new();
        let dead = NodeId(9);
        let alive_src = NodeId(4);
        assert!(mgr.register_heal_source(5, dead)); // resolved pick, source dies
        assert!(mgr.register_heal_source(6, alive_src)); // healthy heal
        assert!(mgr.register_inbound_source(8, dead)); // FORWARD entry (not a heal)

        let alive: std::collections::HashSet<NodeId> = [alive_src].into_iter().collect();
        assert_eq!(
            mgr.repark_dead_source_heals(std::time::Duration::from_secs(10), &alive),
            1,
            "only the dead-source HEAL re-parks",
        );
        assert_eq!(
            mgr.parked_no_source_heal_shards(),
            vec![5],
            "the terminally-failed pick is parked again for re-selection",
        );
        assert!(
            mgr.inbound_bitmap().test(5),
            "the re-parked shard stays fenced"
        );
        assert!(
            mgr.has_pending_heal_from_source(6, alive_src),
            "the alive-source heal is untouched",
        );
        assert!(
            mgr.pending_inbound_entries().contains(&(8, dead)),
            "a forward entry is never re-parked (the orphan reap owns it)",
        );

        // A dead-source heal with a transfer request still within grace is
        // NOT re-parked (mirrors the orphan reap's mid-flight exclusion).
        assert!(mgr.register_heal_source(7, dead));
        mgr.mark_inbound_requested(&[7u16].into_iter().collect());
        assert_eq!(
            mgr.repark_dead_source_heals(std::time::Duration::from_secs(10), &alive),
            0,
            "an in-grace request is honoured before re-parking",
        );

        // Re-parking twice for one shard collapses to a single sentinel.
        assert!(mgr.register_heal_source(5, NodeId(11)));
        assert_eq!(
            mgr.repark_dead_source_heals(std::time::Duration::ZERO, &alive),
            2,
            "the second dead pick (5) and the now-past-grace pick (7) re-park",
        );
        assert_eq!(
            mgr.parked_no_source_heal_shards(),
            vec![5, 7],
            "duplicate parks for shard 5 collapse to one sentinel",
        );
        assert_eq!(
            mgr.pending_inbound_entries()
                .iter()
                .filter(|(s, from)| *s == 5 && *from == NodeId(0))
                .count(),
            1,
            "exactly one parked sentinel remains for the twice-failed shard",
        );
    }

    /// GAP 3a (armed scenario 07) — a PLAIN (non-heal) pending or LOST
    /// inbound entry whose source has LEFT the committed membership is
    /// re-parked to the `NodeId(0)` sentinel: the transfer requester stops
    /// asking a node that no longer exists ("no address for transfer-request
    /// source" forever), and the sentinel entry is completable by whichever
    /// source actually streams the shard (the under-replication resync from
    /// the committed master). Kind discrimination is preserved: `lost` stays
    /// as it was, `heal_pending` stays clear, and heal entries are never
    /// touched (the #74 heal sibling owns those).
    #[test]
    fn repark_departed_source_inbound_reparks_plain_and_lost_entries() {
        let mut mgr = MigrationManager::new();
        let departed = NodeId(4);
        let member = NodeId(2);
        assert!(mgr.register_inbound_source(5, departed)); // plain pending
        assert!(mgr.register_inbound_source(6, member)); // member source
        assert!(mgr.register_heal_source(7, departed)); // heal entry (not ours)
        assert!(mgr.register_inbound_source(8, departed)); // will be LOST
        mgr.mark_inbound_lost(&[8u16].into_iter().collect());
        assert!(mgr.is_shard_lost(8), "precondition: entry 8 is LOST");

        let committed: std::collections::HashSet<NodeId> =
            [NodeId(1), member, NodeId(3)].into_iter().collect();
        assert_eq!(
            mgr.repark_departed_source_inbound(&committed),
            2,
            "exactly the two plain entries with the departed source re-park",
        );

        // Re-parked to the plain sentinel; the member-sourced entry and the
        // heal entry are untouched.
        assert!(mgr.pending_inbound_entries().contains(&(5, NodeId(0))));
        assert!(mgr.pending_inbound_entries().contains(&(8, NodeId(0))));
        assert!(mgr.pending_inbound_entries().contains(&(6, member)));
        assert!(
            mgr.has_pending_heal_from_source(7, departed),
            "heal entries belong to repark_dead_source_heals, not this pass",
        );

        // Kind discrimination preserved.
        assert!(
            !mgr.is_shard_lost(5),
            "a plain pending entry stays non-lost across the re-park",
        );
        assert!(
            mgr.is_shard_lost(8),
            "the LOST fence-until-proven posture survives the re-park",
        );
        assert!(
            mgr.parked_no_source_heal_shards().is_empty(),
            "plain re-parked sentinels are NOT heal parks (and 7's heal \
             entry still has its concrete source)",
        );

        // Fences stay up: the shards are still unproven.
        assert!(mgr.inbound_bitmap().test(5));
        assert!(mgr.inbound_bitmap().test(8));

        // The point of the re-park: whichever source actually streams the
        // shard (the committed master's resync) can complete the plain
        // sentinel and clear the fence — the departed pin could never
        // complete.
        mgr.mark_inbound_complete_from_source(5, NodeId(3));
        assert!(!mgr.has_pending_inbound(5), "resync completion clears 5");
        mgr.mark_inbound_complete_from_source(8, member);
        assert!(!mgr.has_pending_inbound(8), "resync completion clears 8");
        assert!(!mgr.is_shard_lost(8), "completion drops the lost predicate");

        // Idempotent: a second pass finds nothing left to re-park.
        assert_eq!(mgr.repark_departed_source_inbound(&committed), 0);
    }

    /// GAP 3a — duplicate plain entries for one shard (two departed sources)
    /// collapse to a single sentinel, and the collapsed sentinel keeps the
    /// most conservative kind: LOST if ANY collapsed duplicate was LOST.
    #[test]
    fn repark_departed_source_inbound_collapses_duplicates_keeping_lost() {
        let mut mgr = MigrationManager::new();
        assert!(mgr.register_inbound_source(9, NodeId(4)));
        assert!(mgr.register_inbound_source(9, NodeId(5)));
        // Only mark ONE of them LOST is not possible per-entry (mark is
        // per-shard), so mark the shard: both entries carry lost.
        mgr.mark_inbound_lost(&[9u16].into_iter().collect());

        let committed: std::collections::HashSet<NodeId> =
            [NodeId(1), NodeId(2)].into_iter().collect();
        assert_eq!(mgr.repark_departed_source_inbound(&committed), 2);
        assert_eq!(
            mgr.pending_inbound_entries(),
            vec![(9, NodeId(0))],
            "duplicate plain sentinels for one shard collapse to one",
        );
        assert!(mgr.is_shard_lost(9), "the collapsed sentinel stays LOST");
        assert!(mgr.inbound_bitmap().test(9), "the fence stays up");
    }

    /// GAP 1 (armed scenario 06) — the per-shard manifest-mismatch streak
    /// tracker that breaks the code-22 completion livelock. The count of
    /// CONSECUTIVE identical-manifest rejections persists across re-drive
    /// batch invocations (each rebuilds the same fence-time manifest), so
    /// the source can prove "the identical manifest was already rejected"
    /// and escalate to a record-level re-sync instead of retrying forever.
    #[test]
    fn manifest_mismatch_streak_counts_identical_and_resets() {
        let mut mgr = MigrationManager::new();
        let hash_a = [0xAA; 32];
        let hash_b = [0xBB; 32];
        assert_eq!(
            mgr.note_completion_manifest_mismatch(5, &hash_a),
            1,
            "first rejection opens the streak",
        );
        assert_eq!(
            mgr.note_completion_manifest_mismatch(5, &hash_a),
            2,
            "the IDENTICAL manifest rejected again extends the streak",
        );
        assert_eq!(
            mgr.note_completion_manifest_mismatch(5, &hash_b),
            1,
            "different manifest content restarts the streak",
        );
        assert_eq!(
            mgr.note_completion_manifest_mismatch(6, &hash_a),
            1,
            "streaks are per-shard",
        );
        mgr.clear_completion_manifest_mismatch(5);
        assert_eq!(
            mgr.note_completion_manifest_mismatch(5, &hash_b),
            1,
            "clearing (verified / terminal) resets the streak",
        );
    }

    /// GAP 3a — an empty committed-membership set (formation / no committed
    /// term yet) re-parks NOTHING: absence of membership evidence is not
    /// evidence of departure.
    #[test]
    fn repark_departed_source_inbound_noops_on_empty_membership() {
        let mut mgr = MigrationManager::new();
        assert!(mgr.register_inbound_source(3, NodeId(4)));
        assert_eq!(
            mgr.repark_departed_source_inbound(&std::collections::HashSet::new()),
            0,
        );
        assert_eq!(mgr.pending_inbound_entries(), vec![(3, NodeId(4))]);
    }

    /// #74 defensive — if a CONCRETE uncompleted entry for `(shard, source)`
    /// already exists alongside a parked sentinel, resolving drops the
    /// redundant sentinel instead of duplicating: exactly one entry remains,
    /// so the single completion handshake clears the whole fence.
    #[test]
    fn resolve_heal_source_drops_redundant_sentinel_when_concrete_exists() {
        let mut mgr = MigrationManager::new();
        assert!(mgr.mark_heal_fence_active(5));
        assert!(mgr.register_heal_source(5, NodeId(4)));
        assert_eq!(mgr.inbound_count(), 2, "precondition: sentinel + concrete");

        assert!(
            !mgr.resolve_heal_source(5, NodeId(4)),
            "concrete already registered → nothing resolved",
        );
        assert_eq!(
            mgr.pending_inbound_entries(),
            vec![(5, NodeId(4))],
            "the redundant sentinel is dropped, the concrete pull remains",
        );
        assert!(mgr.inbound_bitmap().test(5), "the fence stays up");

        mgr.mark_inbound_complete_from_source(5, NodeId(4));
        assert!(
            !mgr.inbound_bitmap().test(5),
            "one completion clears the whole fence",
        );
    }

    #[test]
    fn register_inbound_source_records_concrete_source() {
        // BUG1: unlike mark_inbound_active (sentinel NodeId(0)),
        // register_inbound_source records the concrete master so the
        // pull-based requester loop (which filters out NodeId(0)) will
        // actually request the shard.
        let mut mgr = MigrationManager::new();
        let master = NodeId(7);
        assert!(mgr.register_inbound_source(9, master));
        assert!(mgr.has_pending_inbound(9));
        assert_eq!(mgr.pending_inbound_entries(), vec![(9, master)]);

        // Idempotent for the same (shard, source).
        assert!(!mgr.register_inbound_source(9, master));
        assert_eq!(mgr.inbound_count(), 1);

        // Survives a serialize/restore round-trip with the source intact (the
        // durable restart trigger).
        let bytes = mgr.serialize_inbound();
        let mut restored = MigrationManager::new();
        restored
            .restore_inbound(&bytes)
            .expect("round-trip restores");
        assert_eq!(restored.pending_inbound_entries(), vec![(9, master)]);
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
        restored
            .restore_inbound(&data)
            .expect("round-trip restores");

        // Only shard 20 (pending) should be restored.
        assert!(!restored.has_pending_inbound(10));
        assert!(restored.has_pending_inbound(20));
        assert_eq!(restored.inbound_count(), 1);
    }

    #[test]
    fn restore_inbound_empty_data() {
        // An absent file (empty bytes) is not corruption — restore is a no-op.
        let mut mgr = MigrationManager::new();
        mgr.restore_inbound(&[]).expect("empty is a valid no-op");
        assert_eq!(mgr.inbound_count(), 0);

        // A valid zero-entry state round-trips through the CRC format.
        let empty_state = MigrationManager::new().serialize_inbound();
        mgr.restore_inbound(&empty_state)
            .expect("zero-entry state restores");
        assert_eq!(mgr.inbound_count(), 0);
    }

    #[test]
    fn restore_inbound_truncated_data() {
        // G5: count=2 but only 1 entry's worth of data and no CRC. The old
        // behavior silently restored a subset (dropping fences); the fix must
        // FAIL-CLOSED so incomplete shards are never served as full authority.
        let mut mgr = MigrationManager::new();
        let mut data = Vec::new();
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&42u16.to_le_bytes());
        data.extend_from_slice(&7u64.to_le_bytes());
        let err = mgr
            .restore_inbound(&data)
            .expect_err("truncated fence file must fail closed");
        assert!(
            matches!(err, InboundRestoreError::LengthMismatch { .. }),
            "expected LengthMismatch, got {err:?}"
        );
        assert_eq!(mgr.inbound_count(), 0, "no fences may be restored");
        assert!(!mgr.has_pending_inbound(42));
    }

    #[test]
    fn restore_inbound_corrupt_fails_closed() {
        // Build a valid persisted fence set with two pending inbound shards.
        let mut src = MigrationManager::new();
        src.mark_inbound_active(10);
        src.mark_inbound_active(20);
        let good = src.serialize_inbound();

        // (1) A short read (dropped tail bytes) must fail closed on length,
        //     not silently restore a subset of the fences.
        let truncated = &good[..good.len() - 3];
        let mut mgr = MigrationManager::new();
        let err = mgr
            .restore_inbound(truncated)
            .expect_err("short read must fail closed");
        assert!(
            matches!(err, InboundRestoreError::LengthMismatch { .. }),
            "expected LengthMismatch, got {err:?}"
        );
        assert_eq!(mgr.inbound_count(), 0);

        // (2) A single corrupted byte in the body must fail the CRC.
        let mut flipped = good.clone();
        flipped[4] ^= 0xFF;
        let mut mgr2 = MigrationManager::new();
        let err2 = mgr2
            .restore_inbound(&flipped)
            .expect_err("corrupt body must fail closed");
        assert!(
            matches!(err2, InboundRestoreError::ChecksumMismatch { .. }),
            "expected ChecksumMismatch, got {err2:?}"
        );
        assert_eq!(mgr2.inbound_count(), 0);

        // (3) A prior fence must survive a failed restore (keep shards fenced
        //     rather than dropping them).
        let mut mgr3 = MigrationManager::new();
        mgr3.mark_inbound_active(99);
        assert!(mgr3.restore_inbound(&flipped).is_err());
        assert!(
            mgr3.has_pending_inbound(99),
            "existing fence must survive a failed restore"
        );

        // (4) The valid round-trip still restores every fence.
        let mut ok = MigrationManager::new();
        ok.restore_inbound(&good).expect("valid fence set restores");
        assert!(ok.has_pending_inbound(10));
        assert!(ok.has_pending_inbound(20));
        assert_eq!(ok.inbound_count(), 2);
    }

    #[test]
    fn restore_inbound_no_duplicates() {
        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(42);

        // Serialize (shard 42, from_node 0 sentinel).
        let data = mgr.serialize_inbound();

        // Restore onto the same manager — should not duplicate.
        mgr.restore_inbound(&data).expect("round-trip restores");
        assert_eq!(mgr.inbound_count(), 1);
    }

    #[test]
    fn restore_inbound_accepts_old_empty_stub() {
        // I-1: the pre-CRC format wrote a bare 4-byte `[count=0]` stub for the
        // empty state (every completion that cleared the last pending inbound).
        // An upgraded node reading it has ZERO shards to fence — it must boot,
        // NOT crash-loop on a `TooShort` brick.
        let mut mgr = MigrationManager::new();
        mgr.restore_inbound(&[0, 0, 0, 0])
            .expect("old 4-byte empty stub must be accepted as empty state");
        assert_eq!(mgr.inbound_count(), 0);

        // But a 4-byte stub claiming entries it cannot contain is corrupt, not
        // an old-empty file — it must still fail closed.
        let mut mgr2 = MigrationManager::new();
        let err = mgr2
            .restore_inbound(&[5, 0, 0, 0])
            .expect_err("4-byte file claiming 5 entries must fail closed");
        assert!(
            matches!(err, InboundRestoreError::TooShort { .. }),
            "expected TooShort, got {err:?}"
        );
        assert_eq!(mgr2.inbound_count(), 0);
    }

    #[test]
    fn load_inbound_state_read_error_fails_closed() {
        // I-2: a real I/O error on an EXISTING fence path (here a directory,
        // which `std::fs::read` cannot read) must NOT be silently treated as an
        // absent (empty) file. Fences the file recorded as pending would be
        // dropped otherwise — the exact fail-open G5 closes.
        let dir = tempfile::tempdir().expect("create temp dir");
        let err = super::load_inbound_state(dir.path())
            .expect_err("reading a directory must be a fail-closed read error");
        assert!(
            matches!(err, InboundRestoreError::ReadError { .. }),
            "expected ReadError, got {err:?}"
        );
    }

    #[test]
    fn persist_and_load_inbound_state() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("inbound.state");

        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(100);
        mgr.mark_inbound_active(200);

        super::persist_inbound_state(&path, &mgr);

        let data = super::load_inbound_state(&path).expect("load persisted state");
        let mut restored = MigrationManager::new();
        restored
            .restore_inbound(&data)
            .expect("round-trip restores");
        assert!(restored.has_pending_inbound(100));
        assert!(restored.has_pending_inbound(200));
        assert_eq!(restored.inbound_count(), 2);
    }

    /// R5: `persist_inbound_state` must fsync the parent directory after the
    /// atomic rename so the dirent update survives a crash (a rename alone is
    /// not durable on crash-consistent filesystems until the containing dir
    /// is fsync'd). Exercising the actual fsync call in isolation isn't
    /// observable from a unit test, so this asserts the round trip through a
    /// real directory still succeeds with the added `fsync_parent_dir` call
    /// in the write path (i.e. the new `?` does not turn a normal persist
    /// into a failure).
    #[test]
    fn persist_inbound_state_fsyncs_parent_dir() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("inbound.state");

        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(7);

        super::persist_inbound_state(&path, &mgr);
        assert!(path.exists(), "persist must have written the file");

        let data = super::load_inbound_state(&path).expect("load persisted state");
        let mut restored = MigrationManager::new();
        restored
            .restore_inbound(&data)
            .expect("round-trip restores after the parent-dir fsync");
        assert!(restored.has_pending_inbound(7));
    }

    /// R5: a parent directory that does not exist must be handled via the
    /// existing best-effort warn-and-continue path (errors are logged, not
    /// propagated) rather than panicking — this exercises the new
    /// `fsync_parent_dir(path)?` call's error path alongside the pre-existing
    /// `File::create` failure on a missing directory.
    #[test]
    fn persist_inbound_state_missing_parent_dir_no_panic() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("no-such-subdir").join("inbound.state");

        let mut mgr = MigrationManager::new();
        mgr.mark_inbound_active(7);

        // Must not panic; the write fails (missing directory) and is logged.
        super::persist_inbound_state(&path, &mgr);
        assert!(!path.exists(), "no file should have been created");
    }

    #[test]
    fn load_inbound_state_missing_file() {
        // An absent file is a safe empty state, NOT a read error.
        let data = super::load_inbound_state(std::path::Path::new("/nonexistent/path"))
            .expect("absent file is an empty state, not an error");
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
        restored
            .restore_outbound(&data)
            .expect("valid round-trip data must restore cleanly");

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
        restored
            .restore_outbound(&data)
            .expect("valid round-trip data must restore cleanly");

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
        restored
            .restore_outbound(&data)
            .expect("round-trip restores");
        assert_eq!(restored.active_count(), 1);
        assert_eq!(restored.active_migrations()[0].snapshot_sequence, 100);
    }

    /// R5: same durability requirement as `persist_inbound_state_fsyncs_parent_dir`
    /// for the outbound-state persist path.
    #[test]
    fn persist_outbound_state_fsyncs_parent_dir() {
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

        super::persist_outbound_state(&path, &mgr);
        assert!(path.exists(), "persist must have written the file");

        let data = super::load_outbound_state(&path);
        let mut restored = MigrationManager::new();
        restored
            .restore_outbound(&data)
            .expect("round-trip restores after the parent-dir fsync");
        assert_eq!(restored.active_count(), 1);
    }

    /// R5: a missing parent directory must be handled via the existing
    /// warn-and-continue best-effort path, not a panic.
    #[test]
    fn persist_outbound_state_missing_parent_dir_no_panic() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("no-such-subdir").join("outbound.state");

        let mgr = MigrationManager::new();
        super::persist_outbound_state(&path, &mgr);
        assert!(!path.exists(), "no file should have been created");
    }

    /// R17: an absent-file empty state (`&[]`) and the pre-CRC old empty-state
    /// stub (`[count=0]`, no trailing CRC) must both be accepted as a valid
    /// empty state — this is the same back-compat carve-out `restore_inbound`
    /// already applies, so an upgraded node with nothing outstanding boots
    /// cleanly rather than bricking on the one-time format break.
    #[test]
    fn restore_outbound_accepts_empty_and_old_stub() {
        let mut mgr = MigrationManager::new();
        mgr.restore_outbound(&[]).expect("empty data is a no-op");
        assert_eq!(mgr.active_count(), 0);

        mgr.restore_outbound(&[0, 0, 0, 0]) // pre-CRC empty stub: count = 0
            .expect("old bare [count=0] stub is a no-op, not TooShort");
        assert_eq!(mgr.active_count(), 0);
    }

    /// R17: a truncated outbound-state file must fail closed (`Err`) instead
    /// of being silently best-effort-parsed. PRE-FIX, `restore_outbound`
    /// returned `()` and its loop did `if pos + 36 > data.len() { break; }` —
    /// a truncated file silently applied whatever entries fit and dropped the
    /// rest with no signal to the caller, exactly the "truncated/corrupt
    /// outbound file is silently mis-parsed into wrong migration state" bug
    /// this test proves is now closed.
    #[test]
    fn restore_outbound_rejects_truncated_file() {
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
            is_master: false,
        };
        mgr.start_outbound(
            &[t1.clone(), t2.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        let mut data = mgr.serialize_outbound();
        // Chop off the trailing CRC and part of the second entry — the
        // pre-fix loop would silently `break` here and apply only t1.
        let cut = data.len() - 6;
        data.truncate(cut);

        // `restored` already carries an unrelated pre-existing active entry
        // so this proves a rejected restore leaves EXISTING state untouched,
        // not merely that it adds nothing to an empty manager.
        let mut restored = MigrationManager::new();
        let pre_existing = MigrationTask {
            shard: 99,
            from_node: NodeId(5),
            to_node: NodeId(6),
            is_master: true,
        };
        restored.start_outbound(
            std::slice::from_ref(&pre_existing),
            NodeId(5),
            &std::collections::HashSet::new(),
        );
        let before_len = restored.active_count();
        assert_eq!(before_len, 1);

        let err = restored
            .restore_outbound(&data)
            .expect_err("a truncated file must be rejected, not silently partially applied");
        assert!(
            matches!(
                err,
                OutboundRestoreError::LengthMismatch { .. } | OutboundRestoreError::TooShort { .. }
            ),
            "expected LengthMismatch or TooShort, got {err:?}"
        );
        assert_eq!(
            restored.active_count(),
            before_len,
            "self.active must be untouched (unchanged len) when the restore is rejected"
        );
        assert_eq!(
            restored.active_migrations()[0].shard,
            99,
            "the pre-existing entry must survive unmodified"
        );
    }

    /// R17: a CRC-corrupt outbound-state file must fail closed (`Err`)
    /// instead of being applied with silently wrong field values (the old
    /// code had no checksum at all, so a single flipped bit produced a
    /// migration task with a corrupted `snapshot_sequence`/`fence_sequence`/
    /// node id with no detection).
    #[test]
    fn restore_outbound_rejects_corrupt_crc() {
        let mut mgr = MigrationManager::new();
        let t1 = MigrationTask {
            shard: 1,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&t1),
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        let mut data = mgr.serialize_outbound();
        // Flip a body byte (not the trailing CRC) so the length still
        // matches but the checksum no longer does.
        data[4] ^= 0xFF;

        let mut restored = MigrationManager::new();
        let pre_existing = MigrationTask {
            shard: 99,
            from_node: NodeId(5),
            to_node: NodeId(6),
            is_master: true,
        };
        restored.start_outbound(
            std::slice::from_ref(&pre_existing),
            NodeId(5),
            &std::collections::HashSet::new(),
        );
        let before_len = restored.active_count();

        let err = restored
            .restore_outbound(&data)
            .expect_err("a CRC-corrupt file must be rejected");
        assert!(
            matches!(err, OutboundRestoreError::ChecksumMismatch { .. }),
            "expected ChecksumMismatch, got {err:?}"
        );
        assert_eq!(
            restored.active_count(),
            before_len,
            "self.active must be untouched (unchanged len) when the restore is rejected"
        );
        assert_eq!(restored.active_migrations()[0].shard, 99);
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

    /// W8 review P0-2 — while the retry hold is raised, `cleanup_completed`
    /// preserves `Failed` entries (the durable retry queue survives the
    /// event loop's periodic prune); draining via `take_failed_tasks`
    /// releases the hold, and the activation's `clear_failed_retry_state`
    /// epoch-fences it so superseded `Failed` entries reap as before.
    #[test]
    fn cleanup_preserves_failed_while_retry_hold_is_live() {
        let task = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        // Hold raised: the Failed entry survives cleanup_completed.
        let mut mgr = MigrationManager::new();
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        mgr.mark_failed(&task);
        mgr.arm_failed_batch_retry();
        mgr.cleanup_completed();
        assert_eq!(
            mgr.failed_count(),
            1,
            "the retry hold must preserve Failed through cleanup_completed"
        );
        // Draining releases the hold: entries reset to Streaming, and a
        // NEW Failed entry (no fresh arm) reaps normally again.
        assert_eq!(mgr.take_failed_tasks(), vec![task.clone()]);
        mgr.mark_failed(&task);
        mgr.cleanup_completed();
        assert_eq!(
            mgr.failed_count(),
            0,
            "after the drain releases the hold, cleanup reaps Failed as before"
        );

        // Epoch fence: the activation's clear_failed_retry_state cancels a
        // live hold so the stale-task cancel still reaps.
        let mut mgr = MigrationManager::new();
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        mgr.mark_failed(&task);
        mgr.arm_failed_batch_retry();
        mgr.clear_failed_retry_state();
        mgr.cleanup_completed();
        assert_eq!(
            mgr.failed_count(),
            0,
            "the activation's epoch fence must let cleanup reap superseded Failed"
        );
        assert!(
            !mgr.take_failed_batch_retry_arm(),
            "the epoch fence also drops the pending arm"
        );
    }

    /// W8 — the failed-batch self-retry arm is a one-shot, coalescing signal:
    /// unarmed drains `false`, any number of arms drain as ONE `true`, and the
    /// drain resets it until the next arm.
    #[test]
    fn failed_batch_retry_arm_is_one_shot_and_coalescing() {
        let mut mgr = MigrationManager::new();
        assert!(
            !mgr.take_failed_batch_retry_arm(),
            "a fresh manager holds no pending arm"
        );
        mgr.arm_failed_batch_retry();
        mgr.arm_failed_batch_retry(); // coalesces with the first
        assert!(
            mgr.take_failed_batch_retry_arm(),
            "an armed manager drains exactly one pending arm"
        );
        assert!(
            !mgr.take_failed_batch_retry_arm(),
            "the drain resets the arm until the next failed disposition"
        );
    }

    /// W4 — `fail_unresolved_tasks` parks exactly the tasks whose entries are
    /// still non-terminal, leaving Complete and already-Failed entries alone.
    /// The parked entries drain `active_count()` (the re-heal gate) while
    /// staying re-drivable through `take_failed_tasks`.
    #[test]
    fn fail_unresolved_tasks_marks_only_unresolved_entries() {
        let mut mgr = MigrationManager::new();
        let make = |shard: u16| MigrationTask {
            shard,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let completed = make(1);
        let failed = make(2);
        let stranded_streaming = make(3);
        let stranded_preparing = make(4);
        let tasks = [
            completed.clone(),
            failed.clone(),
            stranded_streaming.clone(),
            stranded_preparing.clone(),
        ];
        mgr.start_outbound(&tasks, NodeId(1), &std::collections::HashSet::new());
        mgr.mark_complete(&completed);
        mgr.mark_failed(&failed);
        mgr.set_snapshot_sequence(&stranded_streaming, 7); // -> Streaming
        assert_eq!(mgr.active_count(), 2, "two entries are stranded pre-sweep");

        let captured = mgr.capture_task_attempts(&tasks);
        let marked = mgr.fail_unresolved_tasks(&captured);

        let marked_shards: Vec<u16> = marked.iter().map(|t| t.shard).collect();
        assert_eq!(
            marked_shards,
            vec![3, 4],
            "only the stranded entries may be parked"
        );
        assert_eq!(
            mgr.active_count(),
            0,
            "the sweep must drain active_count (the re-heal gate condition)"
        );
        assert_eq!(mgr.failed_count(), 3, "failed + both stranded are parked");
        let complete_entry = mgr
            .active_migrations()
            .iter()
            .find(|p| p.shard == 1)
            .expect("the Complete entry must survive the sweep");
        assert_eq!(
            complete_entry.state,
            MigrationState::Complete,
            "a Complete entry must never be flipped back to Failed"
        );
        assert_eq!(
            mgr.take_failed_tasks().len(),
            3,
            "parked tasks must remain re-drivable via take_failed_tasks"
        );
    }

    /// F3 — `retire_failed_task` removes exactly the matching FAILED entry:
    /// the exhausted exact-key completion escalation must terminate the retry
    /// loop for that one task without touching other in-flight work.
    #[test]
    fn retire_failed_task_removes_only_the_matching_failed_entry() {
        let mut mgr = MigrationManager::new();
        let failed = MigrationTask {
            shard: 7,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        let streaming = MigrationTask {
            shard: 9,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: true,
        };
        mgr.start_outbound(
            &[failed.clone(), streaming.clone()],
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        mgr.mark_failed(&failed);

        assert!(
            !mgr.retire_failed_task(&streaming),
            "a non-failed task must never be retired",
        );
        assert!(
            mgr.retire_failed_task(&failed),
            "the failed entry must be removed",
        );
        assert_eq!(mgr.failed_count(), 0);
        assert!(mgr.take_failed_tasks().is_empty());
        assert_eq!(
            mgr.active_migrations().len(),
            1,
            "the streaming task must survive",
        );
        assert_eq!(mgr.active_migrations()[0].shard, streaming.shard);
    }

    /// Review finding 3 — `fail_and_retire_task` marks Failed and removes the
    /// entry under a SINGLE `&mut self` borrow, so a concurrent
    /// `take_failed_tasks` re-drive can never observe (and resurrect) the
    /// transient Failed state. Identity must match in full — a near-miss task
    /// must leave the tracked entry untouched.
    #[test]
    fn fail_and_retire_task_is_atomic_and_matches_full_identity() {
        let mut mgr = MigrationManager::new();
        let t = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(2),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&t),
            NodeId(1),
            &std::collections::HashSet::new(),
        );

        let wrong_target = MigrationTask {
            shard: 5,
            from_node: NodeId(1),
            to_node: NodeId(3),
            is_master: true,
        };
        assert!(
            !mgr.fail_and_retire_task(&wrong_target),
            "a non-matching identity must be a no-op",
        );
        assert_eq!(
            mgr.active_count(),
            1,
            "the tracked entry must be untouched by the near-miss",
        );

        assert!(
            mgr.fail_and_retire_task(&t),
            "the tracked entry must retire"
        );
        assert_eq!(mgr.failed_count(), 0);
        assert!(mgr.take_failed_tasks().is_empty());
        assert!(mgr.active_migrations().is_empty());
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
        mgr.inbound_migrations
            .push(InboundMigration::pending(10, NodeId(1)));
        mgr.inbound_bitmap.set(10);
        mgr.inbound_migrations
            .push(InboundMigration::pending(20, NodeId(0)));
        mgr.inbound_bitmap.set(20);

        let removed = mgr.clear_stale_inbound(Duration::ZERO);
        assert_eq!(removed, 0);
        assert!(mgr.has_pending_inbound(10));
        assert!(mgr.has_pending_inbound(20));
        assert_eq!(mgr.inbound_count(), 2);
    }

    /// W11 FIX 4(a) (RED→GREEN) — a source's terminal
    /// `ERR_MIGRATION_NO_TASKS` refusal retires the dangling inbound entries
    /// it names, and NOTHING else.
    ///
    /// Scoping matters: a refusal from one source must not cancel a live
    /// transfer for the same shard from another, and an entry whose records
    /// are still local must be KEPT fenced — dropping it would expose those
    /// orphans to local reads (the scenario-17 three-holder bug), which is
    /// why the drop reuses the periodic prune's fail-closed predicate rather
    /// than trusting the refusal blindly.
    #[test]
    fn refused_transfer_request_drops_only_its_own_dangling_inbounds() {
        let refuser = NodeId(3);
        let other = NodeId(4);

        let mut mgr = MigrationManager::new();
        assert!(mgr.register_inbound_source(10, refuser)); // refused, no records
        assert!(mgr.register_inbound_source(11, refuser)); // refused, HAS records
        assert!(mgr.register_inbound_source(12, refuser)); // not in the request
        assert!(mgr.register_inbound_source(10, other)); // same shard, live peer

        // Shard 11 still holds local records → fail-closed keep.
        let outcome = mgr.drop_refused_inbound(&[10, 11], refuser, TEST_TERMINAL_ROUNDS, |shard| {
            if shard == 11 {
                InboundRetention::KeepOrphan
            } else {
                InboundRetention::Drop
            }
        });

        assert_eq!(
            outcome.dropped, 1,
            "only the record-free refused entry is retired"
        );
        assert!(
            !mgr.pending_inbound_entries().contains(&(10, refuser)),
            "the dangling entry the source will never satisfy must go",
        );
        assert!(
            mgr.pending_inbound_entries().contains(&(11, refuser)),
            "an entry whose records are still local stays fenced fail-closed \
             until orphan cleanup reclaims them",
        );
        assert!(
            mgr.pending_inbound_entries().contains(&(12, refuser)),
            "a shard the refusal did not name is untouched",
        );
        assert!(
            mgr.pending_inbound_entries().contains(&(10, other)),
            "a refusal from one source must never cancel another source's \
             live transfer for the same shard",
        );

        // W11 review NIT — a reverse-heal fence outranks a peer's opinion
        // about its own outbound tasks: it survives even a `clear_inbound`
        // topology commit, so a refusal must not retire it either.
        assert!(mgr.register_heal_source(13, refuser));
        assert_eq!(
            mgr.drop_refused_inbound(&[13], refuser, TEST_TERMINAL_ROUNDS, |_| {
                InboundRetention::Drop
            })
            .dropped,
            0,
            "a heal_pending entry is never dropped by a source refusal",
        );
        assert!(mgr.has_pending_inbound(13), "the heal fence stays up");
        // Shard 10 still has the other source's entry, so it stays fenced;
        // the bitmap must agree with the entries it shadows.
        assert!(mgr.has_pending_inbound(10));
        assert!(mgr.has_pending_inbound(11));
        assert!(mgr.has_pending_inbound(12));

        // Once the last entry for a shard goes, the fence bit goes with it.
        assert_eq!(
            mgr.drop_refused_inbound(&[10], other, TEST_TERMINAL_ROUNDS, |_| {
                InboundRetention::Drop
            })
            .dropped,
            1
        );
        assert!(
            !mgr.has_pending_inbound(10),
            "the inbound bitmap must be rebuilt from the surviving entries",
        );
    }

    /// W12 TAIL 2 (RED→GREEN) — an entry the source TERMINALLY refused but the
    /// fail-closed record guard RETAINED must be distinguishable from an
    /// inbound transfer that is still in flight.
    ///
    /// The two are the same shape today (`{shard, from_node}`, counted in
    /// `inbound_pending`) but they are opposites: one is waiting for data that
    /// is on its way, the other is a fixpoint. Armed scenario 08 @ fc5e5f7
    /// sat on two of the latter for 300 s — node1 re-sent the request 29
    /// times on the 10 s cadence, node3 answered `ERR_MIGRATION_NO_TASKS`
    /// every time, and the drop was vetoed every time by
    /// `local_record_count > 0`, while the records themselves could not be
    /// reclaimed (orphan cleanup skips shards with a pending inbound, and the
    /// #28 committed-handoff evidence for them never existed). Nothing in the
    /// status told an operator — or the convergence gate — that the entries
    /// could never progress.
    #[test]
    fn a_refused_but_retained_inbound_entry_is_marked_as_refused() {
        let refuser = NodeId(3);
        let other = NodeId(4);

        let mut mgr = MigrationManager::new();
        assert!(mgr.register_inbound_source(20, refuser)); // refused, HAS records
        assert!(mgr.register_inbound_source(21, refuser)); // refused, no records
        assert!(mgr.register_inbound_source(22, refuser)); // not in the request
        assert!(mgr.register_inbound_source(20, other)); // same shard, live peer

        assert!(
            mgr.refused_retained_inbound_entries().is_empty(),
            "nothing is refused before a refusal arrives",
        );

        let outcome = mgr.drop_refused_inbound(&[20, 21], refuser, TEST_TERMINAL_ROUNDS, |shard| {
            if shard == 20 {
                InboundRetention::KeepOrphan
            } else {
                InboundRetention::Drop
            }
        });
        assert_eq!(
            outcome.dropped, 1,
            "only the record-free refused entry is retired"
        );

        assert_eq!(
            mgr.refused_retained_inbound_entries(),
            vec![(20, refuser)],
            "the retained-because-of-records entry — and ONLY it — carries the \
             terminal-refusal mark: shard 22 was never named, and shard 20's \
             entry from the OTHER source is a live transfer",
        );
        assert_eq!(
            mgr.pending_inbound_entries().len(),
            3,
            "marking changes no fence: 20/refuser, 20/other and 22/refuser all \
             remain pending",
        );

        // A heal fence is never dropped and never marked — a peer's opinion
        // about its own outbound tasks does not touch the reverse-heal fence.
        assert!(mgr.register_heal_source(23, refuser));
        assert_eq!(
            mgr.drop_refused_inbound(&[23], refuser, TEST_TERMINAL_ROUNDS, |_| {
                InboundRetention::Drop
            })
            .dropped,
            0
        );
        assert_eq!(
            mgr.refused_retained_inbound_entries(),
            vec![(20, refuser)],
            "a heal_pending entry a refusal could not drop is NOT a \
             terminally-refused orphan fence",
        );

        // A fresh registration for the same (shard, source) means a real
        // transfer is expected again: the mark must clear, exactly as `lost`
        // does, or a revived entry would stay excluded from the in-flight
        // count forever.
        assert!(!mgr.register_inbound_source(20, refuser));
        assert!(
            mgr.refused_retained_inbound_entries().is_empty(),
            "re-registering the source clears the terminal-refusal mark",
        );
    }

    /// W12 TAIL 2 — data actually ARRIVING for a previously-refused shard
    /// must clear the mark even when the batch carries no source identity.
    ///
    /// The receive path stamps the concrete sender when it can
    /// (`register_inbound_source`) and falls back to `mark_inbound_active`
    /// when the batch does not name one. That fallback early-returns on any
    /// existing entry for the shard, so without an explicit clear a marked
    /// entry would stay excluded from the in-flight count while records were
    /// streaming into it — the gate would stop waiting on a live transfer.
    #[test]
    fn an_unstamped_inbound_batch_clears_the_refusal_mark() {
        let refuser = NodeId(3);
        let mut mgr = MigrationManager::new();
        assert!(mgr.register_inbound_source(30, refuser));
        assert_eq!(
            mgr.drop_refused_inbound(&[30], refuser, TEST_TERMINAL_ROUNDS, |_| {
                InboundRetention::KeepOrphan
            })
            .dropped,
            0
        );
        assert_eq!(mgr.refused_retained_inbound_entries(), vec![(30, refuser)]);

        // The source-less receive path: no new entry is created (one already
        // exists) but the shard is demonstrably receiving again.
        assert!(!mgr.mark_inbound_active(30));
        assert!(
            mgr.refused_retained_inbound_entries().is_empty(),
            "records arriving for the shard mean the entry is live again",
        );
        assert!(
            mgr.has_pending_inbound(30),
            "clearing the mark must not drop the fence",
        );
    }

    /// W16 (RED→GREEN) — a HOLDER's inbound entry its source refuses ROUND
    /// AFTER ROUND must become terminal, while staying retained and fenced.
    ///
    /// CI 32644353574 (armed scenario 09) is the fixpoint this closes. Nine
    /// shards failed their completion handshake on a record-count mismatch,
    /// node2 terminally aborted the tasks, and node1 — the shards' target
    /// holder — re-asked every 10 s. Eight consecutive sweeps logged
    /// `shards: 9, dropped: 0` with `refused_by_source: false` and
    /// `inbound_refused_retained: 0`, so `in_flight_inbound_pending` counted
    /// nine live transfers that could never move and the convergence gate
    /// could not close no matter what else converged.
    ///
    /// The two halves of the assertion are equally load-bearing. Believing the
    /// source is what unwedges the gate; NOT dropping the entry (and not
    /// lowering the fence) is what stops the fix from serving an unproven copy
    /// as authority — this node holds records the handshake never verified.
    #[test]
    fn a_holder_entry_refused_round_after_round_becomes_terminal_but_stays_fenced() {
        let refuser = NodeId(3);
        let shard = 40u16;

        let mut mgr = MigrationManager::new();
        assert!(mgr.register_inbound_source(shard, refuser));

        for round in 1..TEST_TERMINAL_ROUNDS {
            let outcome = mgr.drop_refused_inbound(&[shard], refuser, TEST_TERMINAL_ROUNDS, |_| {
                InboundRetention::KeepHolder
            });
            assert_eq!(
                outcome.dropped, 0,
                "round {round}: a holder is never dropped"
            );
            assert_eq!(
                outcome.kept_holder,
                vec![shard],
                "round {round}: still treated as satisfiable work the re-heal can re-plan",
            );
            assert!(
                outcome.kept_holder_terminal.is_empty(),
                "round {round}: a streak shorter than {TEST_TERMINAL_ROUNDS} is still \
                 consistent with a transiently-diverged source table",
            );
            assert!(
                mgr.refused_retained_inbound_entries().is_empty(),
                "round {round}: nothing is terminal yet",
            );
        }

        let outcome = mgr.drop_refused_inbound(&[shard], refuser, TEST_TERMINAL_ROUNDS, |_| {
            InboundRetention::KeepHolder
        });
        assert_eq!(
            outcome.kept_holder_terminal,
            vec![shard],
            "the {TEST_TERMINAL_ROUNDS}th consecutive refusal is the source answering the same \
             question the same way for two full re-heal cooldowns",
        );
        assert!(
            outcome.kept_holder.is_empty(),
            "a terminal entry is reported as terminal, not as still-hopeful work",
        );
        assert_eq!(
            outcome.dropped, 0,
            "believing the source never drops the entry"
        );

        // RETAINED and FENCED — the half that keeps this from being the
        // "obvious fix" that serves an unproven copy.
        assert_eq!(
            mgr.pending_inbound_entries(),
            vec![(shard, refuser)],
            "the entry stays; only its classification changed",
        );
        assert!(
            mgr.has_pending_inbound(shard),
            "the fence stays UP: this node holds a copy no completion handshake ever proved",
        );
        assert_eq!(mgr.inbound_count(), 1, "still counted as a pending inbound");
        // VISIBLE — what the gauge, the status JSON and the convergence gate read.
        assert_eq!(
            mgr.refused_retained_inbound_entries(),
            vec![(shard, refuser)],
            "a fixpoint must be reported as one",
        );
    }

    /// W16 (RED→GREEN) — the streak counts CONSECUTIVE refusals, so a round in
    /// which the source MATCHED the request resets it.
    ///
    /// Without the reset a source that re-plans the handoff every re-heal round
    /// and rolls it back in between — progress, just slow — would accumulate
    /// its way to a terminal mark it never earned, and the convergence gate
    /// would stop waiting on a transfer that was genuinely being retried.
    #[test]
    fn a_matched_transfer_request_resets_the_holder_refusal_streak() {
        let refuser = NodeId(3);
        let shard = 41u16;
        let mut mgr = MigrationManager::new();
        assert!(mgr.register_inbound_source(shard, refuser));

        let refuse_once = |mgr: &mut MigrationManager| {
            mgr.drop_refused_inbound(&[shard], refuser, TEST_TERMINAL_ROUNDS, |_| {
                InboundRetention::KeepHolder
            })
        };

        for _ in 1..TEST_TERMINAL_ROUNDS {
            assert!(refuse_once(&mut mgr).kept_holder_terminal.is_empty());
        }
        // The source has a task for it after all.
        assert_eq!(
            mgr.note_transfer_request_matched(&[shard], refuser),
            1,
            "the streak the reset undoes must be reported",
        );
        for round in 1..TEST_TERMINAL_ROUNDS {
            assert!(
                refuse_once(&mut mgr).kept_holder_terminal.is_empty(),
                "round {round} after the reset: the count restarted at zero",
            );
        }
        assert!(
            mgr.refused_retained_inbound_entries().is_empty(),
            "{} refusals split by one match are not {TEST_TERMINAL_ROUNDS} CONSECUTIVE refusals",
            2 * (TEST_TERMINAL_ROUNDS - 1),
        );
        assert_eq!(
            refuse_once(&mut mgr).kept_holder_terminal,
            vec![shard],
            "and the {TEST_TERMINAL_ROUNDS}th consecutive one still lands",
        );

        // A match is scoped exactly like a refusal: same source, listed shards
        // only. It also clears an existing mark — the source having work for
        // the shard is the strongest possible contradiction of "never".
        assert_eq!(
            mgr.note_transfer_request_matched(&[shard], NodeId(9)),
            0,
            "another node's answer says nothing about THIS entry's source",
        );
        assert_eq!(
            mgr.refused_retained_inbound_entries(),
            vec![(shard, refuser)]
        );
        assert_eq!(mgr.note_transfer_request_matched(&[shard], refuser), 1);
        assert!(
            mgr.refused_retained_inbound_entries().is_empty(),
            "the source queueing the shard un-marks it",
        );
    }

    /// W16 (RED→GREEN) — data arriving clears BOTH the terminal mark and the
    /// streak behind it.
    ///
    /// Clearing only the mark would be a trap: the entry would re-acquire it on
    /// the very next refusal instead of being given the full window again, so a
    /// shard that is genuinely streaming would flip back to "terminal" the
    /// first time a request raced ahead of the stream.
    #[test]
    fn an_inbound_batch_resets_the_holder_streak_not_just_the_mark() {
        let refuser = NodeId(3);
        let shard = 42u16;
        let mut mgr = MigrationManager::new();
        assert!(mgr.register_inbound_source(shard, refuser));

        for _ in 0..TEST_TERMINAL_ROUNDS {
            mgr.drop_refused_inbound(&[shard], refuser, TEST_TERMINAL_ROUNDS, |_| {
                InboundRetention::KeepHolder
            });
        }
        assert_eq!(
            mgr.refused_retained_inbound_entries(),
            vec![(shard, refuser)]
        );

        assert!(!mgr.mark_inbound_active(shard));
        assert!(
            mgr.refused_retained_inbound_entries().is_empty(),
            "records arriving mean the entry is live again",
        );
        let outcome = mgr.drop_refused_inbound(&[shard], refuser, TEST_TERMINAL_ROUNDS, |_| {
            InboundRetention::KeepHolder
        });
        assert!(
            outcome.kept_holder_terminal.is_empty(),
            "the next refusal starts a NEW streak; it does not resume the old one",
        );
        assert_eq!(outcome.kept_holder, vec![shard]);
    }

    /// W16 — a reverse-heal fence is not touched by ANY number of refusals.
    ///
    /// `drop_refused_inbound` skips `heal_pending` entries outright, so the
    /// streak can never start on one. Pinned because the escalation is exactly
    /// the kind of change that would grow a "…unless it has been refused N
    /// times" clause into the one place that must not have one: a peer's
    /// opinion about its own outbound tasks is weaker evidence than the
    /// no-serve-before-heal fence, which survives even a topology commit.
    #[test]
    fn a_heal_fence_never_accumulates_a_refusal_streak() {
        let refuser = NodeId(3);
        let shard = 43u16;
        let mut mgr = MigrationManager::new();
        assert!(mgr.register_heal_source(shard, refuser));

        for _ in 0..TEST_TERMINAL_ROUNDS * 4 {
            let outcome = mgr.drop_refused_inbound(&[shard], refuser, TEST_TERMINAL_ROUNDS, |_| {
                InboundRetention::KeepHolder
            });
            assert_eq!(outcome.dropped, 0);
            assert!(outcome.kept_holder.is_empty());
            assert!(outcome.kept_holder_terminal.is_empty());
        }
        assert!(
            mgr.refused_retained_inbound_entries().is_empty(),
            "a heal fence is never reclassified by a source's refusal",
        );
        assert!(mgr.has_pending_inbound(shard), "and it stays up");
    }

    /// W11 FIX 1 (RED→GREEN) — an inbound entry for a shard this node is NOT
    /// a target holder of is not PLAN work: it is the orphan candidate the
    /// cleanup pass exists to reclaim, so counting it at the admissibility
    /// gate deadlocks the pass against the fail-closed inbound prune that
    /// keeps the entry alive precisely because the records are still there.
    #[test]
    fn non_held_inbound_is_not_plan_work() {
        use crate::cluster::shards::ShardTable;

        let members = [NodeId(1), NodeId(2), NodeId(3)];
        let table = ShardTable::compute(&members, 2);
        // With RF=2 over 3 members exactly one node is left out of each
        // shard's target assignment — pick it as `self`.
        let shard = 0u16;
        let a = table.target_assignment(shard);
        let outsider = *members
            .iter()
            .find(|n| a.master != **n && !a.replicas.contains(n))
            .expect("RF=2 over 3 members always leaves one node out");
        let holder = a.master;

        let mut mgr = MigrationManager::new();
        assert!(mgr.register_inbound_source(shard, NodeId(9)));
        assert_eq!(
            mgr.inbound_migration_work_count(),
            1,
            "precondition: the old counter sees this entry as in-flight work",
        );
        assert_eq!(
            mgr.inbound_plan_work_count(&table, outsider),
            0,
            "a non-held inbound is an orphan candidate, not plan work — it \
             must not be able to disable the orphan-cleanup pass",
        );
        assert_eq!(
            mgr.inbound_plan_work_count(&table, holder),
            1,
            "the SAME entry on a target holder is genuine plan work and must \
             still gate the pass",
        );

        // The heal-fence exclusion composes: a heal fence on a HELD shard is
        // still excluded (39603fc), so the two rules are independent.
        let held_shard = (0..crate::cluster::shards::NUM_SHARDS as u16)
            .find(|s| {
                let a = table.target_assignment(*s);
                a.master == outsider || a.replicas.contains(&outsider)
            })
            .expect("the outsider holds some shard");
        assert!(mgr.register_heal_source(held_shard, NodeId(9)));
        assert_eq!(
            mgr.inbound_plan_work_count(&table, outsider),
            0,
            "a heal fence on a held shard is alert-and-hold state, not work",
        );

        // A completed non-held entry never counted and still does not.
        mgr.mark_inbound_complete_from_source(shard, NodeId(9));
        assert_eq!(mgr.inbound_plan_work_count(&table, holder), 0);
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
        mgr.inbound_migrations
            .push(InboundMigration::pending(10, NodeId(1)));
        mgr.inbound_bitmap.set(10);
        mgr.inbound_migrations
            .push(InboundMigration::pending(20, NodeId(2)));
        mgr.inbound_bitmap.set(20);

        let grace = Duration::from_secs(10);

        // Before any request: both are reapable (orphaned-source case).
        let reapable = mgr.orphaned_inbound_shards(grace, &std::collections::HashSet::new());
        assert_eq!(reapable, std::collections::HashSet::from([10, 20]));
        assert_eq!(mgr.pending_inbound_requested_count(grace), 0);

        // Request a resend for shard 10 only.
        mgr.mark_inbound_requested(&std::collections::HashSet::from([10]));

        // Shard 10 is now protected; shard 20 (no request) stays reapable.
        let reapable = mgr.orphaned_inbound_shards(grace, &std::collections::HashSet::new());
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
            mgr.orphaned_inbound_shards(Duration::ZERO, &std::collections::HashSet::new());
        assert_eq!(
            reapable_no_grace,
            std::collections::HashSet::from([10]),
            "once the request grace lapses the entry becomes reapable again"
        );
    }

    /// Scenario 06 (false "source died") — request-interval settling alone is
    /// NOT evidence of source death. The settled-inbound GC marked
    /// slow-but-live migrations LOST — up to ~2400/4096 shards fenced at
    /// once, 37% of writes NO_QUORUM — while every source was SWIM-alive.
    /// Only an entry whose source has actually left the SWIM-alive set is
    /// orphaned; a live source's entries stay pending (fenced, and still
    /// visible to the pull requester, which re-drives the transfer).
    #[test]
    fn settled_gc_reaps_only_dead_source_inbound() {
        let mut mgr = MigrationManager::new();
        mgr.register_inbound_source(10, NodeId(2)); // source SWIM-alive
        mgr.register_inbound_source(20, NodeId(3)); // source SWIM-dead
        let grace = Duration::from_secs(10);
        let alive = std::collections::HashSet::from([NodeId(1), NodeId(2)]);

        // Neither shard has an in-grace transfer request, so both are
        // "settled" — but only the dead source's shard is orphaned.
        let orphaned = mgr.orphaned_inbound_shards(grace, &alive);
        assert_eq!(
            orphaned,
            std::collections::HashSet::from([20]),
            "a settled inbound from a SWIM-alive source is slow, not orphaned"
        );

        assert_eq!(mgr.mark_inbound_lost(&orphaned), 1);
        assert!(
            !mgr.is_shard_lost(10),
            "the live source's shard must stay pending, never lost"
        );
        assert!(
            mgr.is_shard_lost(20),
            "a genuinely dead source's incomplete inbound IS marked lost"
        );
        assert!(
            mgr.has_pending_inbound(10),
            "the live source's shard stays fenced while it waits"
        );
        assert!(
            mgr.pending_inbound_entries().contains(&(10, NodeId(2))),
            "and stays visible to the pull requester so the transfer re-arms"
        );

        // The source later dies: its entry becomes orphaned on the next pass.
        let none_alive = std::collections::HashSet::from([NodeId(1)]);
        let orphaned = mgr.orphaned_inbound_shards(grace, &none_alive);
        assert_eq!(
            orphaned,
            std::collections::HashSet::from([10]),
            "once SWIM declares the source dead the entry is reapable"
        );
    }

    /// A sentinel inbound entry with no concrete source (`NodeId(0)`) can
    /// never be pulled — the transfer requester skips `NodeId(0)` — so the
    /// liveness gate must not immortalize it: it stays reapable exactly as
    /// before.
    #[test]
    fn settled_gc_still_reaps_sentinel_source_inbound() {
        let mut mgr = MigrationManager::new();
        mgr.inbound_migrations
            .push(InboundMigration::pending(30, NodeId(0)));
        mgr.inbound_bitmap.set(30);

        let alive = std::collections::HashSet::from([NodeId(1), NodeId(2), NodeId(3)]);
        let orphaned = mgr.orphaned_inbound_shards(Duration::from_secs(10), &alive);
        assert_eq!(
            orphaned,
            std::collections::HashSet::from([30]),
            "a no-source sentinel entry is reapable regardless of liveness"
        );
        assert_eq!(mgr.mark_inbound_lost(&orphaned), 1);
        assert!(mgr.is_shard_lost(30));
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
        restored
            .restore_outbound(&data)
            .expect("valid round-trip data must restore cleanly");

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
    // C8 — settled-inbound orphan reap must NOT serve an INCOMPLETE shard as
    // full authority. An orphaned incomplete inbound (source died, no
    // completion handshake) is marked LOST and stays FENCED, never cleared.
    // -----------------------------------------------------------------------

    /// The core C8 property: reaping a genuinely orphaned but INCOMPLETE
    /// inbound entry keeps the shard fenced (marks it lost/unavailable) rather
    /// than clearing the fence — so the partial shard is never served as full
    /// authority. Contrast `clear_pending_inbound_for_shards`, which drops the
    /// fence (the unsafe pre-C8 reap).
    /// An inbound for a shard this node does not hold must be droppable.
    ///
    /// Once the cluster settles on its final term nothing else clears it: the
    /// sender never completes (it does not consider us a holder) and no further
    /// topology change fires `clear_inbound`. The shard stays fenced and
    /// `inbound_count()` never reaches zero, which is exactly what left
    /// scenario 09 reporting `inbound=2` for the full 120s wait while the
    /// source accepted 11 transfer requests for those shards and moved nothing.
    #[test]
    fn prune_inbound_not_held_drops_only_unheld_shards() {
        let mut mgr = MigrationManager::new();
        mgr.register_inbound_source(10, NodeId(2));
        mgr.register_inbound_source(20, NodeId(2));
        mgr.register_inbound_source(30, NodeId(3));
        assert_eq!(mgr.inbound_count(), 3);

        // This node holds 10 and 30, but 20 ended up assigned elsewhere.
        let held = |shard: u16| shard == 10 || shard == 30;
        let removed = mgr.prune_inbound_not_held(held);

        assert_eq!(removed, 1, "exactly the unheld shard is dropped");
        assert_eq!(mgr.inbound_count(), 2);
        assert!(
            mgr.has_pending_inbound(10) && mgr.has_pending_inbound(30),
            "held shards keep their pending inbound and stay fenced"
        );
        assert!(
            !mgr.has_pending_inbound(20),
            "the unheld shard is no longer pending"
        );
        assert!(
            !mgr.inbound_bitmap().test(20),
            "and its fence bit is cleared, so the shard stops blocking"
        );
        assert!(
            mgr.inbound_bitmap().test(10) && mgr.inbound_bitmap().test(30),
            "held shards keep their fence bits"
        );
    }

    /// A LOST entry for a shard we still hold must survive the prune — it is
    /// fenced deliberately (C8) and only a real re-acquisition may clear it.
    #[test]
    fn prune_inbound_not_held_keeps_lost_entries_for_held_shards() {
        let mut mgr = MigrationManager::new();
        mgr.register_inbound_source(7, NodeId(2));
        mgr.mark_inbound_lost(&std::collections::HashSet::from([7u16]));
        assert!(mgr.is_shard_lost(7));

        assert_eq!(mgr.prune_inbound_not_held(|_| true), 0);
        assert!(
            mgr.is_shard_lost(7) && mgr.inbound_bitmap().test(7),
            "a held-but-lost shard stays fenced fail-closed"
        );

        // ...but if the shard is no longer ours, the fence serves no purpose.
        assert_eq!(mgr.prune_inbound_not_held(|_| false), 1);
        assert!(!mgr.inbound_bitmap().test(7));
    }

    #[test]
    fn mark_inbound_lost_keeps_fence_for_incomplete_orphan() {
        let mut mgr = MigrationManager::new();
        // A source began pushing but died mid-migration: incomplete, no
        // completion handshake seen.
        mgr.register_inbound_source(7, NodeId(2));
        assert!(mgr.has_pending_inbound(7), "shard fenced while receiving");
        assert!(!mgr.is_shard_lost(7));

        // Settled-inbound GC reap candidates: incomplete entries with no
        // outstanding transfer request.
        let settled =
            mgr.orphaned_inbound_shards(Duration::from_secs(1), &std::collections::HashSet::new());
        assert!(
            settled.contains(&7),
            "orphaned incomplete inbound is a reap candidate"
        );

        // SAFE DEFAULT: the reap marks the shard LOST but MUST NOT clear the
        // fence — an incomplete shard is never served as full authority.
        let marked = mgr.mark_inbound_lost(&settled);
        assert_eq!(marked, 1, "the incomplete orphan is marked lost");
        assert_eq!(mgr.lost_count(), 1);
        assert!(mgr.is_shard_lost(7), "shard marked lost/unavailable");
        assert!(
            mgr.has_pending_inbound(7),
            "fence MUST remain set after orphan-reap of an INCOMPLETE inbound",
        );

        // Idempotent: a lost shard leaves the reap candidate set, so a second
        // GC pass is a no-op — no re-mark, no fence churn.
        let settled2 =
            mgr.orphaned_inbound_shards(Duration::from_secs(1), &std::collections::HashSet::new());
        assert!(
            !settled2.contains(&7),
            "a lost shard is excluded from further reap candidates",
        );
        assert_eq!(mgr.mark_inbound_lost(&settled2), 0);
        assert!(
            mgr.has_pending_inbound(7),
            "fence still held on the second pass"
        );
    }

    /// A shard marked lost that LATER receives the completion handshake (source
    /// recovered, or a re-home completed it) is PROVEN complete: the lost mark
    /// and the fence both clear on the normal completion path.
    #[test]
    fn proven_complete_clears_lost_mark_and_fence() {
        let mut mgr = MigrationManager::new();
        mgr.register_inbound_source(9, NodeId(3));
        let settled =
            mgr.orphaned_inbound_shards(Duration::from_secs(1), &std::collections::HashSet::new());
        assert_eq!(mgr.mark_inbound_lost(&settled), 1);
        assert!(mgr.is_shard_lost(9));
        assert!(mgr.has_pending_inbound(9));

        // Completion handshake arrives → proven complete.
        mgr.mark_inbound_complete(9);
        assert!(
            !mgr.is_shard_lost(9),
            "completeness proof clears the lost mark"
        );
        assert!(
            !mgr.has_pending_inbound(9),
            "proven-complete shard is unfenced"
        );
        assert_eq!(mgr.lost_count(), 0);
    }

    /// A fresh migration re-acquiring a lost shard revives it as an active
    /// (non-lost) pending entry so the incoming data can complete it — the
    /// shard is being received again, not abandoned.
    #[test]
    fn re_acquiring_lost_shard_clears_lost_mark() {
        let mut mgr = MigrationManager::new();
        mgr.register_inbound_source(11, NodeId(4));
        let settled =
            mgr.orphaned_inbound_shards(Duration::from_secs(1), &std::collections::HashSet::new());
        mgr.mark_inbound_lost(&settled);
        assert!(mgr.is_shard_lost(11));

        // A new topology term hands shard 11 back to this node as an inbound
        // target from a fresh source.
        let task = MigrationTask {
            shard: 11,
            from_node: NodeId(5),
            to_node: NodeId(1),
            is_master: true,
        };
        mgr.start_outbound(
            std::slice::from_ref(&task),
            NodeId(1),
            &std::collections::HashSet::new(),
        );
        assert!(!mgr.is_shard_lost(11), "re-acquiring clears the lost mark");
        assert!(
            mgr.has_pending_inbound(11),
            "still fenced — now actively receiving"
        );
        // It is once again a reap candidate (fresh pending, not lost).
        let settled2 =
            mgr.orphaned_inbound_shards(Duration::from_secs(1), &std::collections::HashSet::new());
        assert!(settled2.contains(&11));
    }

    /// C17 (sibling of C8) — a topology supersede (`clear_inbound`) must NOT
    /// unfence a shard that was received incompletely and never proved
    /// completeness. "Not-pending-inbound" is not "complete".
    #[test]
    fn clear_inbound_preserves_unproven_lost_fence() {
        let mut mgr = MigrationManager::new();
        // One ordinary in-flight (non-lost) inbound and one orphaned+lost one.
        mgr.register_inbound_source(3, NodeId(2));
        mgr.register_inbound_source(9, NodeId(4));
        let settled: std::collections::HashSet<u16> = [9].into_iter().collect();
        assert_eq!(mgr.mark_inbound_lost(&settled), 1);
        assert!(mgr.is_shard_lost(9));

        // A topology change supersedes the in-flight migration bookkeeping.
        mgr.clear_inbound();

        // SAFE DEFAULT: the UNPROVEN lost shard keeps its fence — it is never
        // served as full authority just because the topology moved on.
        assert!(
            mgr.has_pending_inbound(9),
            "clear_inbound must NOT unfence an unproven lost shard",
        );
        assert!(mgr.is_shard_lost(9), "the lost mark survives the supersede");
        // The ordinary non-lost in-flight expectation is superseded as before;
        // the fresh plan re-registers whatever the new topology needs.
        assert!(
            !mgr.has_pending_inbound(3),
            "a non-lost in-flight inbound is still superseded by clear_inbound",
        );
    }

    /// P0 (reverse-heal Phase 2c, ROUND 2) — `clear_pending_inbound_for_shards`
    /// (the migration-abort fence-clearing path) must NOT drop a `heal_pending`
    /// entry for the aborted shard: a reverse-heal target IS the committed
    /// master, so unfencing it serves an un-healed tail = double-spend. A
    /// FORWARD-migration entry (heal_pending=false) for the same aborted shard
    /// is still cleared (unchanged behaviour).
    #[test]
    fn clear_pending_inbound_preserves_heal_fence() {
        let mut mgr = MigrationManager::new();
        // Shard 7: a reverse-heal PULL fence (heal_pending, non-lost).
        assert!(mgr.register_heal_source(7, NodeId(2)));
        assert!(mgr.has_pending_inbound(7));
        assert!(!mgr.is_shard_lost(7));
        // Shard 8: an ordinary forward inbound (heal_pending=false).
        assert!(mgr.register_inbound_source(8, NodeId(3)));
        assert!(mgr.has_pending_inbound(8));

        // Abort both shards.
        let abort: std::collections::HashSet<u16> = [7, 8].into_iter().collect();
        let removed = mgr.clear_pending_inbound_for_shards(&abort);

        // Only the forward (non-heal) entry is removed; the heal fence survives.
        assert_eq!(removed, 1, "only the forward-migration entry is cleared");
        assert!(
            mgr.has_pending_inbound(7),
            "the aborted reverse-heal shard STAYS fenced (heal_pending preserved)",
        );
        assert!(
            !mgr.has_pending_inbound(8),
            "an aborted forward-migration shard is still cleared",
        );
        // The heal entry survives as pullable (non-completed, concrete source)
        // so the requester re-requests the transient failure.
        assert!(
            mgr.pending_inbound_entries()
                .iter()
                .any(|&(s, from)| s == 7 && from == NodeId(2)),
            "the surviving heal entry stays pullable",
        );

        // Only a genuine completion proof lifts the fence.
        mgr.mark_inbound_complete_from_source(7, NodeId(2));
        assert!(
            !mgr.has_pending_inbound(7),
            "completion proof clears the heal fence",
        );
    }

    /// Reverse-heal Phase 3c — `expired_heal_shards` returns only heal fences
    /// older than the deadline; `refresh_heal_deadline` resets the clock (so an
    /// alert-and-held shard re-arms once per window). A completed heal never counts
    /// as expired, and a forward inbound is never subject to the heal deadline.
    #[test]
    fn expired_heal_shards_respects_deadline_and_refresh() {
        let mut mgr = MigrationManager::new();
        // A reverse-heal fence on shard 7 and a plain forward inbound on shard 8.
        assert!(mgr.register_heal_source(7, NodeId(2)));
        assert!(mgr.register_inbound_source(8, NodeId(3)));

        // A huge deadline → nothing is expired yet (the fence is fresh).
        assert!(
            mgr.expired_heal_shards(std::time::Duration::from_secs(3600))
                .is_empty(),
            "a fresh heal is not past its deadline",
        );
        // A zero deadline → the heal fence (only shard 7, NOT the forward 8) is due.
        assert_eq!(
            mgr.expired_heal_shards(std::time::Duration::from_millis(0)),
            vec![7],
            "only the heal_pending fence is subject to the deadline, not a forward inbound",
        );

        // Refreshing the clock re-arms it: a huge deadline still excludes it, but
        // the fence itself stays up (alert-and-hold never releases the fence).
        assert!(mgr.refresh_heal_deadline(7));
        assert!(
            mgr.expired_heal_shards(std::time::Duration::from_secs(3600))
                .is_empty(),
            "after a refresh a huge deadline is not yet exceeded",
        );
        assert!(
            mgr.has_pending_inbound(7),
            "alert-and-hold keeps the heal fence up — refresh only resets the clock",
        );
        // The zero deadline reports it again (elapsed since the reset is >= 0),
        // proving the alert re-arms once per window rather than releasing.
        assert_eq!(
            mgr.expired_heal_shards(std::time::Duration::from_millis(0)),
            vec![7],
            "a refreshed heal re-arms for the next deadline window",
        );
        // The forward inbound on shard 8 is untouched by the heal machinery.
        assert!(mgr.has_pending_inbound(8), "forward inbound survives");

        // A completed heal never counts as expired.
        assert!(mgr.register_heal_source(9, NodeId(4)));
        mgr.mark_inbound_complete_from_source(9, NodeId(4));
        assert!(
            !mgr.expired_heal_shards(std::time::Duration::from_millis(0))
                .contains(&9),
            "a completed heal is not subject to the deadline",
        );
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

        // The gauge is process-global; hold the shared lock so a neighbour's
        // bulk registration cannot land between this test's two reads.
        let _metrics_guard = crate::metrics::migration_metrics_test_lock();
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
