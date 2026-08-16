//! Reverse-heal Phase 2a — generation-aware deletion tombstones.
//!
//! A [`TombstoneLog`] is a dedicated, per-store durable record of the deletes
//! this node has executed, keyed by txid and carrying the record's FROZEN
//! generation `N` (the value it held at its last real mutation, discarded by the
//! pre-2a delete path) plus the `deletion_height` used as the retention-GC key.
//! It backs the delete-safe reverse-pull heal (Phase 2c): the healing node
//! consults its OWN tombstone via [`TombstoneLog::at_or_ahead`] to avoid
//! resurrecting a record it correctly deleted.
//!
//! # Durability model (Invariant TS-1)
//!
//! A delete is LOCAL, buffered prune GC: it fsyncs only the allocator
//! `FreeRegion`, while the primary-index unregister and the on-device header
//! tombstone stay in the write-back cache and become durable at the next
//! checkpoint. A crash before that checkpoint reverts them and
//! `recovery::reconcile_freelist_against_live_index` restores the record LIVE.
//!
//! The tombstone rides the SAME barrier. [`TombstoneLog::record`] updates the
//! in-RAM sharded index and buffers the on-disk append IN RAM only; the entry is
//! written to disk and fsynced solely by [`TombstoneLog::persist`], invoked from
//! the checkpoint. So a crash before checkpoint loses the un-persisted append
//! exactly as it loses the delete — **Invariant TS-1: a tombstone for `k` exists
//! on this node ⟺ this node's delete of `k` is durable.** Boot recovery adds a
//! belt-and-suspenders [`TombstoneLog::reconcile_against_live`] that drops any
//! tombstone whose key came back LIVE, so a dangling tombstone can never survive
//! over a resurrected record.
//!
//! # On-disk layout
//!
//! An 8-byte header (`magic || version`, little-endian) followed by a stream of
//! fixed 48-byte [`TombstoneEntry`] records. The file is APPEND-ONLY between
//! checkpoints and COMPACTED (rewritten atomically from the in-RAM index) at a
//! checkpoint whenever retention GC or a live-reconcile has dropped entries —
//! the same "advance a durable prefix, reclaim the dead" shape as
//! `redo::reclaim_covered_segments`. Duplicate keys (a re-delete after a
//! re-create) are resolved last-writer-wins on replay, matching append order.

use std::collections::HashMap;
use std::path::PathBuf;

use parking_lot::{Mutex, RwLock};

use crate::index::TxKey;
use crate::index::sharded::shard_for_key;
use crate::record::generation_at_or_ahead;

/// Why a record was deleted. Consulted by the RULE-DS heal-apply gate
/// ([`TombstoneLog::blocks_heal_apply`]) — the cause selects the veto rule —
/// and carried on-disk (preserved across compaction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TombstoneCause {
    /// Deleted by the DAH sweep (`due_guard` set).
    Dah = 0,
    /// Deleted by a direct client `OP_DELETE_BATCH` (`due_guard == None`).
    ClientDelete = 1,
    /// Deleted as part of a migration replace-duplicate reconcile.
    PruneReplace = 2,
    /// W9 — a create ROLLED BACK because its replication fan-out failed
    /// (`server::dispatch::compensate_replication_failure`'s Create arm), or a
    /// remote apply of the compensating delete that rollback fanned out /
    /// re-emitted through a redo-derived delta.
    ///
    /// This cause asserts "the create THIS write attempt applied was undone" —
    /// a claim about ONE failed write, NOT a claim that the client deleted the
    /// key. The client saw an ERROR for that create and may well have retried
    /// it successfully elsewhere, so a LIVE client-acked copy of the record
    /// can legitimately exist — which is exactly why this cause must not carry
    /// the unconditional [`Self::ClientDelete`] veto (pre-W9 it was recorded
    /// AS `ClientDelete`, and the unconditional veto turned the crash-window
    /// rollback into permanent acked-write loss: every heal/migration create
    /// of the surviving live copy was vetoed forever, and orphan cleanup then
    /// deleted the last live copy).
    CompensatedCreate = 3,
}

/// W9 — why a JOURNALLED (`RedoOp::Delete`) or REPLICATED (`ReplicaOp::Delete`)
/// delete removed the record, so the APPLYING node records the same
/// [`TombstoneCause`] the originating node did (and a redo-derived re-emit —
/// migration delta, crash-recovered replication intent — preserves it).
///
/// Only the two causes that ever travel: the DAH sweep is per-holder local GC
/// (never journalled as `Delete`, never replicated), and `PruneReplace` is a
/// local reconcile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteCause {
    /// A client `OP_DELETE_BATCH` — the key's authoritative deletion; the
    /// applying node records [`TombstoneCause::ClientDelete`] (unconditional
    /// RULE-DS veto, the #78 anti-resurrection posture).
    ClientDelete,
    /// A compensating delete rolling back a create whose replication fan-out
    /// failed; the applying node records
    /// [`TombstoneCause::CompensatedCreate`] (generation-overridable veto).
    CompensatedCreate,
}

impl DeleteCause {
    /// The [`TombstoneCause`] the applying node records for this delete.
    pub fn tombstone_cause(self) -> TombstoneCause {
        match self {
            Self::ClientDelete => TombstoneCause::ClientDelete,
            Self::CompensatedCreate => TombstoneCause::CompensatedCreate,
        }
    }
}

/// On-disk tombstone record. Fixed 48-byte little-endian layout; the field
/// offsets below are the single source of truth for [`encode_entry`] /
/// [`decode_entry`], tied to this struct via `offset_of!` so the byte codec and
/// the declared layout can never drift.
#[repr(C, packed)]
struct TombstoneEntry {
    txid: [u8; 32],
    deletion_generation: u32,
    deletion_height: u32,
    cause: u8,
    _pad: [u8; 3],
    crc: u32,
}

/// Serialized size of one [`TombstoneEntry`].
pub const TOMBSTONE_ENTRY_SIZE: usize = 48;
const _: () = assert!(std::mem::size_of::<TombstoneEntry>() == TOMBSTONE_ENTRY_SIZE);

const TXID_OFF: usize = std::mem::offset_of!(TombstoneEntry, txid);
const GEN_OFF: usize = std::mem::offset_of!(TombstoneEntry, deletion_generation);
const HEIGHT_OFF: usize = std::mem::offset_of!(TombstoneEntry, deletion_height);
const CAUSE_OFF: usize = std::mem::offset_of!(TombstoneEntry, cause);
const CRC_OFF: usize = std::mem::offset_of!(TombstoneEntry, crc);
const _: () = assert!(TXID_OFF == 0 && GEN_OFF == 32 && HEIGHT_OFF == 36);
const _: () = assert!(CAUSE_OFF == 40 && CRC_OFF == 44);

const TOMB_MAGIC: u32 = 0x5453_4C31; // "TSL1"
const TOMB_VERSION: u32 = 1;
const TOMB_HEADER_SIZE: usize = 8;

/// A decode failure for a single on-disk entry (skipped, not fatal to the log).
#[derive(Debug, thiserror::Error)]
enum TombstoneDecodeError {
    #[error("tombstone entry truncated: {got} < {want} bytes")]
    Truncated { got: usize, want: usize },
    #[error("tombstone entry CRC mismatch: stored {expected:#010x} != computed {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },
}

/// Encode a tombstone into its fixed 48-byte on-disk form (CRC over the first 44
/// bytes, matching [`decode_entry`]).
fn encode_entry(
    txid: &[u8; 32],
    generation: u32,
    height: u32,
    cause: u8,
) -> [u8; TOMBSTONE_ENTRY_SIZE] {
    let mut buf = [0u8; TOMBSTONE_ENTRY_SIZE];
    buf[TXID_OFF..TXID_OFF + 32].copy_from_slice(txid);
    buf[GEN_OFF..GEN_OFF + 4].copy_from_slice(&generation.to_le_bytes());
    buf[HEIGHT_OFF..HEIGHT_OFF + 4].copy_from_slice(&height.to_le_bytes());
    buf[CAUSE_OFF] = cause;
    // _pad stays zero.
    let crc = crc32fast::hash(&buf[..CRC_OFF]);
    buf[CRC_OFF..CRC_OFF + 4].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode + CRC-validate one 48-byte on-disk entry into its `(key, value)`.
fn decode_entry(src: &[u8]) -> Result<(TxKey, TombValue), TombstoneDecodeError> {
    if src.len() < TOMBSTONE_ENTRY_SIZE {
        return Err(TombstoneDecodeError::Truncated {
            got: src.len(),
            want: TOMBSTONE_ENTRY_SIZE,
        });
    }
    let expected = u32::from_le_bytes([
        src[CRC_OFF],
        src[CRC_OFF + 1],
        src[CRC_OFF + 2],
        src[CRC_OFF + 3],
    ]);
    let actual = crc32fast::hash(&src[..CRC_OFF]);
    if expected != actual {
        return Err(TombstoneDecodeError::CrcMismatch { expected, actual });
    }
    let mut txid = [0u8; 32];
    txid.copy_from_slice(&src[TXID_OFF..TXID_OFF + 32]);
    let generation = u32::from_le_bytes([
        src[GEN_OFF],
        src[GEN_OFF + 1],
        src[GEN_OFF + 2],
        src[GEN_OFF + 3],
    ]);
    let height = u32::from_le_bytes([
        src[HEIGHT_OFF],
        src[HEIGHT_OFF + 1],
        src[HEIGHT_OFF + 2],
        src[HEIGHT_OFF + 3],
    ]);
    Ok((
        TxKey { txid },
        TombValue {
            generation,
            height,
            cause: src[CAUSE_OFF],
        },
    ))
}

/// In-RAM per-key tombstone state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TombValue {
    generation: u32,
    height: u32,
    cause: u8,
}

/// File-side state guarded by a single mutex, disjoint from the per-shard
/// index locks: the un-persisted append buffer and a "compaction needed" flag
/// set whenever GC / live-reconcile drops entries from the in-RAM index.
struct FileState {
    pending: Vec<(TxKey, TombValue)>,
    needs_compaction: bool,
}

/// A per-store deletion-tombstone log: an in-RAM sharded index over
/// `(txid) -> (generation, height, cause)` plus its durable append-only backing
/// file. Sharded by the same `shard_for_key` (seed + count) as the primary
/// index so a per-shard heal never contends the whole map.
pub struct TombstoneLog {
    path: PathBuf,
    seed: u64,
    shard_count: usize,
    retention_blocks: u32,
    shards: Vec<RwLock<HashMap<TxKey, TombValue>>>,
    file: Mutex<FileState>,
    /// Test-only deterministic seam for the P1 compaction-window race repro: a
    /// `(key, generation, height, cause)` injected as a `record()` INSIDE
    /// [`Self::persist`]'s compaction branch, AFTER the atomic rewrite but
    /// BEFORE `pending` is reconciled — the exact window in which the old
    /// `pending.clear()` dropped a concurrently recorded delete from both the
    /// written file and `pending`. Fired at most once (taken on use).
    #[cfg(test)]
    inject_in_compaction_window: Mutex<Option<(TxKey, u32, u32, TombstoneCause)>>,
}

impl TombstoneLog {
    /// Create a fresh, empty tombstone log backed by `path` (not yet created on
    /// disk — the first [`Self::persist`] materializes it). `seed` +
    /// `shard_count` MUST match the primary index so keys route identically.
    pub fn new(path: PathBuf, seed: u64, shard_count: usize, retention_blocks: u32) -> Self {
        let shard_count = shard_count.max(1);
        let shards = (0..shard_count)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        Self {
            path,
            seed,
            shard_count,
            retention_blocks,
            shards,
            file: Mutex::new(FileState {
                pending: Vec::new(),
                needs_compaction: false,
            }),
            #[cfg(test)]
            inject_in_compaction_window: Mutex::new(None),
        }
    }

    /// Load a tombstone log from disk (boot replay), rebuilding the in-RAM
    /// index. A missing file yields an empty log (fresh boot). Torn trailing
    /// bytes and individual CRC-failed entries are skipped with a warning; a bad
    /// magic/version is fatal.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] on a read failure other than
    /// "not found", or `InvalidData` if the header magic/version is unrecognized.
    pub fn load(
        path: PathBuf,
        seed: u64,
        shard_count: usize,
        retention_blocks: u32,
    ) -> std::io::Result<Self> {
        let log = Self::new(path.clone(), seed, shard_count, retention_blocks);
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(log),
            Err(e) => return Err(e),
        };
        if data.len() < TOMB_HEADER_SIZE {
            // Empty or torn header (crash mid-create): treat as fresh.
            return Ok(log);
        }
        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        if magic != TOMB_MAGIC || version != TOMB_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("tombstone log: bad magic/version ({magic:#010x}/{version})"),
            ));
        }
        let mut off = TOMB_HEADER_SIZE;
        let mut skipped = 0u64;
        while off + TOMBSTONE_ENTRY_SIZE <= data.len() {
            match decode_entry(&data[off..off + TOMBSTONE_ENTRY_SIZE]) {
                // Append order = time order, so a later entry for a re-deleted
                // key overwrites the earlier one (insert wins).
                Ok((key, value)) => {
                    let idx = shard_for_key(log.seed, &key, log.shard_count);
                    log.shards[idx].write().insert(key, value);
                }
                Err(e) => {
                    skipped += 1;
                    tracing::warn!(
                        target: "teraslab::tombstone",
                        offset = off,
                        err = %e,
                        "tombstone log entry undecodable; skipping",
                    );
                }
            }
            off += TOMBSTONE_ENTRY_SIZE;
        }
        if skipped > 0 {
            tracing::warn!(
                target: "teraslab::tombstone",
                skipped,
                "tombstone log replay skipped {skipped} undecodable entr(y/ies)",
            );
        }
        Ok(log)
    }

    fn shard_index(&self, key: &TxKey) -> usize {
        shard_for_key(self.seed, key, self.shard_count)
    }

    /// Record a delete: insert `(key -> generation, height, cause)` into the
    /// in-RAM index and buffer the on-disk append IN RAM (no file I/O, no fsync
    /// — the delete-latency floor is untouched). Durability is deferred to
    /// [`Self::persist`] at the next checkpoint (Invariant TS-1).
    ///
    /// # W9 cause precedence (resurrection-safety pin)
    ///
    /// Successive records for the same key are last-writer-wins, with ONE
    /// carve-out: a [`TombstoneCause::CompensatedCreate`] never REPLACES an
    /// existing tombstone of a different cause — the weaker, generation-
    /// overridable rollback claim must not downgrade a stronger claim
    /// (`ClientDelete`'s unconditional veto above all). The reverse direction
    /// stays plain LWW: a later `ClientDelete` for the same key upgrades a
    /// `CompensatedCreate`. In production the carve-out is defense in depth —
    /// TS-1 clears the tombstone whenever the key comes back live, so two
    /// causes can only collide through a delete of an already-absent record
    /// (which records nothing) — but the downgrade must be structurally
    /// impossible, not merely unlikely (see
    /// [`Self::blocks_heal_apply`]'s safety argument).
    pub fn record(&self, key: &TxKey, generation: u32, height: u32, cause: TombstoneCause) {
        let value = TombValue {
            generation,
            height,
            cause: cause as u8,
        };
        // Two disjoint locks, never held simultaneously here: shard write drops
        // before the file lock is taken, so `persist` (file-then-shard) can
        // never invert against this path.
        {
            let mut shard = self.shards[self.shard_index(key)].write();
            if cause == TombstoneCause::CompensatedCreate
                && shard
                    .get(key)
                    .is_some_and(|existing| existing.cause != cause as u8)
            {
                // The stronger existing claim stands; record nothing (the
                // existing entry is already durable-or-pending on its own).
                return;
            }
            shard.insert(*key, value);
        }
        self.file.lock().pending.push((*key, value));
    }

    /// O(1) heal-apply query (design §A): is this node's delete of `key` at a
    /// generation at-or-ahead of `generation`? True ⇒ a shipped image at
    /// `generation` must be dropped as a resurrection (consumed by Phase 2c).
    pub fn at_or_ahead(&self, key: &TxKey, generation: u32) -> bool {
        match self.shards[self.shard_index(key)].read().get(key) {
            Some(v) => generation_at_or_ahead(v.generation, generation),
            None => false,
        }
    }

    /// Reverse-heal RULE-DS apply gate (design §C, consumed by Phase 2c): must a
    /// heal/migration-shipped image for `key` at `incoming_generation` be DROPPED
    /// as a resurrection of this node's delete?
    ///
    /// The rule is CAUSE-AWARE, because the generation guarantee differs:
    ///
    /// - A [`TombstoneCause::Dah`] tombstone records this node's delete of a
    ///   fully-spent record at generation `N`. The drop test is the wrapping
    ///   compare `generation_at_or_ahead(N, incoming)` — block iff `N` is
    ///   at-or-ahead of the incoming generation. The safety basis is
    ///   LAST-WRITER-WINS BY GENERATION, NOT terminal-maximality: every
    ///   generation in a record's lineage is assigned by a single master, so
    ///   `incoming <= N` is a genuine laggard (a stale pre-delete image) and is
    ///   correctly DROPPED, while `incoming > N` is a genuinely-NEWER state and
    ///   is correctly ADMITTED as convergence — not a resurrection.
    ///
    ///   NOTE — `incoming > N` is REACHABLE and correct: an earlier rationale
    ///   claimed a DAH-swept record is terminal so its generation `N` is the
    ///   per-record MAXIMUM and `incoming > N` "cannot happen". That is FALSE —
    ///   a reorg `unspend` legitimately re-mutates the record past `N` on a node
    ///   that kept it, so a diverged source can hold `g_src > N`. Admitting that
    ///   image is the RIGHT outcome (the source holds the newer state), and the
    ///   gate is airtight because it rests on LWW-by-generation, not on the
    ///   (false) maximality claim (design §C, P1-3 proof).
    ///
    /// - A [`TombstoneCause::ClientDelete`] / [`TombstoneCause::PruneReplace`]
    ///   tombstone carries NO terminal-generation guarantee: a client may delete
    ///   a STILL-MUTATING record whose generation a diverged source later exceeds
    ///   (the 2a/2b-review consensus concern — a generation-only gate would miss
    ///   it and resurrect the record). For those causes the heal drops
    ///   UNCONDITIONALLY: a record this node explicitly removed is never brought
    ///   back by a boot heal, closing the concern under a double-spend lens. A
    ///   LEGITIMATE re-create instead flows through the client-create / normal
    ///   master→replica path — which clears the tombstone
    ///   ([`Self::clear`]) — never the migration baseline this gate is scoped to.
    ///   The residual is a self-healing availability gap for a legitimately
    ///   superseded client-deleted record (design §G E5), NOT a correctness
    ///   violation: the divergence is re-detected and re-healed after the fence
    ///   clears.
    ///
    ///   ACCEPTED RESIDUAL (P1, consensus-critical, design-acked E5 — do NOT
    ///   treat as fixed): the unconditional drop LOSES a legitimately
    ///   RE-CREATED UTXO when the boot heal is its SOLE carrier. Sequence:
    ///   client deletes `k` → this node goes down → a reorg re-creates `k` (a
    ///   genuinely-newer state) while the node is down → at reboot the heal
    ///   ships `k`'s re-create but this gate drops it unconditionally, and the
    ///   tombstone stays live (blocking any further re-delivery of `k` the same
    ///   way) until it GCs at `tombstone_retention_blocks` (`src/config.rs`,
    ///   design §E1). This is the CONSERVATIVE direction — a LOSS, not a
    ///   double-spend (no single-fault double-spend exists).
    ///
    ///   As of Phase 3b, `reverse_heal.tombstones` defaults ON for a clustered
    ///   node (RF>1) (`ReverseHealConfig::tombstones_enabled`) and RUNTIME
    ///   ONLINE re-heal IS wired
    ///   ([`crate::cluster::coordinator::RunningCluster::run_online_reheal`]) —
    ///   not deferred. Online re-heal closes the common case: a reorg
    ///   re-create that arrives via the normal replica `Create` path is
    ///   admitted the instant it lands, because this gate only ever fires
    ///   against an ABSENT-key BOOT-heal baseline. The residual above survives
    ///   specifically when a node is down for the whole reorg/re-create AND its
    ///   only future delivery of `k` is a boot-heal baseline — online re-heal
    ///   does not help there because there is nothing to re-detect until the
    ///   boot heal itself runs.
    ///
    ///   A HEIGHT-AWARE ClientDelete gate (block iff `g_src <= N` AND the
    ///   shipped record's create-height `<= deletion_height`, admitting a
    ///   reorg re-create at height `> deletion_height`) was proposed to close
    ///   this and was REJECTED in review: there is no immutable create-height
    ///   to gate on, so the gate would itself open a latent double-spend
    ///   window. Sizing `tombstone_retention_blocks` at or above the
    ///   reorg/finality horizon is the accepted mitigation instead — it lifts
    ///   the block precisely when a legitimate re-mine of the key is no longer
    ///   possible, at which point the unconditional drop stops mattering.
    ///
    /// - W9 — a [`TombstoneCause::CompensatedCreate`] tombstone (a create
    ///   rolled back because its replication fan-out failed) is OVERRIDABLE:
    ///   it drops the incoming image only when the image is STRICTLY BEHIND
    ///   the tombstone (`!generation_at_or_ahead(incoming, N)`), so a
    ///   heal/migration create at `incoming >= N` APPLIES. The client was
    ///   NACKed for the rolled-back create and may have retried it
    ///   successfully elsewhere; the surviving live, client-ACKED copy — at
    ///   the same or a later generation of the same lineage — must defeat its
    ///   own crash-window rollback, or the acked write is permanently lost
    ///   (the CI-proven armed-05/08 loss: the veto starved every heal until
    ///   orphan cleanup deleted the last live copy).
    ///
    ///   SAFETY ARGUMENT — why this override can never resurrect a record the
    ///   CLIENT deleted:
    ///
    ///   1. **Cause separation at every producer.** `CompensatedCreate` is
    ///      recorded ONLY over a create this node just rolled back
    ///      (`compensate_replication_failure`'s Create arm) or by applying the
    ///      compensating delete that rollback fanned out / re-emitted
    ///      (`DeleteCause::CompensatedCreate` on the wire + redo). Every
    ///      client-delete path — local `OP_DELETE_BATCH`, its
    ///      `ReplicaOp::Delete` fan-out, redo-derived delete re-emits —
    ///      records `ClientDelete`. Neither path can produce the other's
    ///      cause.
    ///   2. **Precedence when both claims fire on one node.** [`Self::record`]
    ///      refuses to let a `CompensatedCreate` replace an existing tombstone
    ///      of any other cause, and a later `ClientDelete` replaces a
    ///      `CompensatedCreate` by plain last-writer-wins — so wherever a
    ///      client-delete claim exists for the key, the unconditional veto
    ///      stands (pinned by
    ///      `compensated_create_never_downgrades_a_stronger_tombstone`).
    ///   3. **The override adds no exposure a bare node lacks.** A node whose
    ///      ONLY tombstone for the key is `CompensatedCreate` never itself
    ///      executed a client delete of the key (a client delete applying to a
    ///      PRESENT record records `ClientDelete`; one finding the key ABSENT
    ///      records nothing — on this node or any other). Admitting an
    ///      at-or-ahead live image therefore leaves this node exactly as
    ///      exposed as a node with NO tombstone — the posture RULE-DS/#78
    ///      always accepted for non-deleting nodes ("a node that ITSELF
    ///      client-deleted never resurrects" is the invariant, and it is
    ///      untouched).
    ///   4. **Stale images still drop.** The `incoming >= N` floor keeps a
    ///      source strictly behind the rolled-back create's own frozen
    ///      generation vetoed, same LWW-by-generation basis as the Dah leg.
    pub fn blocks_heal_apply(&self, key: &TxKey, incoming_generation: u32) -> bool {
        match self.shards[self.shard_index(key)].read().get(key) {
            None => false,
            Some(v) if v.cause == TombstoneCause::Dah as u8 => {
                generation_at_or_ahead(v.generation, incoming_generation)
            }
            Some(v) if v.cause == TombstoneCause::CompensatedCreate as u8 => {
                !generation_at_or_ahead(incoming_generation, v.generation)
            }
            Some(_) => true,
        }
    }

    /// The recorded `(generation, height)` for `key`, if a tombstone exists.
    pub fn lookup(&self, key: &TxKey) -> Option<(u32, u32)> {
        self.shards[self.shard_index(key)]
            .read()
            .get(key)
            .map(|v| (v.generation, v.height))
    }

    /// The recorded [`TombstoneCause`] for `key`, if a tombstone exists —
    /// diagnostic companion to [`Self::blocks_heal_apply`] so a veto site can
    /// report WHY an apply was dropped. A stored byte no current cause maps to
    /// (possible only for a log written by a future version) is reported as
    /// [`TombstoneCause::ClientDelete`], matching how
    /// [`Self::blocks_heal_apply`] treats every unrecognized cause
    /// (unconditional block — the fail-closed direction).
    pub fn lookup_cause(&self, key: &TxKey) -> Option<TombstoneCause> {
        self.shards[self.shard_index(key)]
            .read()
            .get(key)
            .map(|v| match v.cause {
                c if c == TombstoneCause::Dah as u8 => TombstoneCause::Dah,
                c if c == TombstoneCause::PruneReplace as u8 => TombstoneCause::PruneReplace,
                c if c == TombstoneCause::CompensatedCreate as u8 => {
                    TombstoneCause::CompensatedCreate
                }
                _ => TombstoneCause::ClientDelete,
            })
    }

    /// Total live tombstone count across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Whether the in-RAM index holds no tombstones.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.read().is_empty())
    }

    /// Drop every tombstone past its retention horizon
    /// (`deletion_height + retention_blocks <= last_durable_height`) from the
    /// in-RAM index, EXCEPT those `is_protected` reports as guarded by an
    /// in-flight reverse-heal (Phase 2d). Flags the durable file for compaction
    /// if anything was dropped. Returns the number removed.
    ///
    /// # The heal-vs-GC race (Phase 2d)
    ///
    /// A reverse-heal that spans a checkpoint must not let the checkpoint GC a
    /// tombstone the heal still needs to gate an incoming record (which would
    /// open a resurrection window mid-heal). `is_protected(key)` returns `true`
    /// while a heal references the key's shard; a past-retention tombstone whose
    /// shard is protected is RETAINED (so RULE-DS — [`Self::blocks_heal_apply`]
    /// — can still drop a resurrecting image), and re-GC'd on the next
    /// checkpoint once the heal completes and the protection lifts. Pass
    /// `|_| false` when no heal can be in flight (the retention bound is then the
    /// sole horizon). Protection only DEFERS GC; it never retains a tombstone
    /// past the point the shard stops being referenced, so it cannot leak.
    pub fn gc(&self, last_durable_height: u32, is_protected: impl Fn(&TxKey) -> bool) -> usize {
        let retention = self.retention_blocks;
        let mut dropped = 0usize;
        for shard in &self.shards {
            let mut g = shard.write();
            let before = g.len();
            // Drop only when BOTH past retention AND not heal-protected.
            g.retain(|k, v| !expired(v.height, retention, last_durable_height) || is_protected(k));
            dropped += before - g.len();
        }
        if dropped > 0 {
            self.file.lock().needs_compaction = true;
        }
        dropped
    }

    /// Drop every tombstone whose key `is_live` reports as still present in the
    /// recovered primary index — a boot-time reconcile that enforces
    /// "no dangling tombstone over a live record" (Invariant TS-1) even against
    /// a delete that reverted after its append reached disk. Returns the number
    /// removed.
    pub fn reconcile_against_live<F: Fn(&TxKey) -> bool>(&self, is_live: F) -> usize {
        let mut dropped = 0usize;
        for shard in &self.shards {
            let mut g = shard.write();
            let before = g.len();
            g.retain(|k, _| !is_live(k));
            dropped += before - g.len();
        }
        if dropped > 0 {
            self.file.lock().needs_compaction = true;
        }
        dropped
    }

    /// Drop the tombstone for `key` from the in-RAM index AND any un-persisted
    /// append buffered for it, flagging the durable file for compaction when
    /// something was removed. Returns `true` if a tombstone was present.
    ///
    /// Called from the create / (re)register-live path (Invariant TS-1): a key
    /// that comes back LIVE must carry no tombstone, so a later online heal
    /// (Phase 3/4) cannot use a stale tombstone to drop the resurrected record.
    /// Draining `pending` here is what makes the removal durable-safe against
    /// [`Self::persist`]'s compaction `retain`: were the entry left in `pending`,
    /// the next compaction would re-append the tombstone we just cleared. O(1)
    /// for the common create of a never-deleted key (the shard `remove` misses,
    /// so neither the file lock nor the `pending` scan is taken).
    pub fn clear(&self, key: &TxKey) -> bool {
        let removed = self.shards[self.shard_index(key)]
            .write()
            .remove(key)
            .is_some();
        if removed {
            let mut fs = self.file.lock();
            fs.pending.retain(|(pk, _)| pk != key);
            fs.needs_compaction = true;
        }
        removed
    }

    /// Make the tombstone set durable at a checkpoint: GC past the retention
    /// horizon (retaining any tombstone `is_protected` guards for an in-flight
    /// reverse-heal, Phase 2d — see [`Self::gc`]), then either COMPACT (atomic
    /// rewrite from the in-RAM index, when GC / reconcile dropped entries) or
    /// APPEND the buffered tail. Fsyncs before returning.
    ///
    /// # Errors
    ///
    /// Returns a [`std::io::Error`] on any filesystem failure; on an append
    /// failure the buffered tail is restored so the next checkpoint retries it.
    pub fn persist(
        &self,
        last_durable_height: u32,
        is_protected: impl Fn(&TxKey) -> bool,
    ) -> std::io::Result<()> {
        self.gc(last_durable_height, is_protected);

        let needs_compaction = { self.file.lock().needs_compaction };
        if needs_compaction {
            let all = self.snapshot_all();
            self.write_all_atomic(&all)?;
            // Test-only: inject a `record()` into the compaction window (after the
            // rewrite, before the `pending` reconcile below) to reproduce the P1
            // race deterministically.
            #[cfg(test)]
            if let Some((k, generation, height, cause)) =
                self.inject_in_compaction_window.lock().take()
            {
                self.record(&k, generation, height, cause);
            }
            // Retain in `pending` exactly the entries this compaction did NOT make
            // durable — DO NOT blindly `clear()`. A `record()` (delete) can land in
            // the window above [after `snapshot_all` read its shard, before this
            // reconcile]; its `(key, value)` is absent from the just-written
            // snapshot, so it must stay buffered and be re-appended next checkpoint,
            // preserving Invariant TS-1 (tombstone durable ⟺ delete durable). An
            // unconditional `clear()` dropped it from `pending` while the delete
            // could still become durable → durable-delete-without-tombstone →
            // Phase-2c resurrection / double-spend.
            //
            // `record()` inserts into the shard map BEFORE pushing to `pending`, so
            // any `pending` entry whose exact `(key, value)` is in the written
            // snapshot had its shard-insert captured by `snapshot_all` — durable,
            // drop it. A value NOT in the snapshot was recorded after that shard was
            // read (or is a newer value for a re-deleted key) — keep it. Re-append
            // is harmless: replay is last-writer-wins by key. (An entry removed from
            // the shard AND drained from `pending` — see [`Self::clear`] — is not in
            // `pending` at all, so it can never be resurrected here.)
            let written: HashMap<TxKey, TombValue> = all.into_iter().collect();
            let mut fs = self.file.lock();
            fs.pending.retain(|(k, v)| written.get(k) != Some(v));
            fs.needs_compaction = false;
            return Ok(());
        }

        let pending = std::mem::take(&mut self.file.lock().pending);
        if pending.is_empty() {
            return Ok(());
        }
        if let Err(e) = self.append_entries(&pending) {
            // Restore the tail so the next checkpoint retries it.
            let mut fs = self.file.lock();
            let mut restored = pending;
            restored.append(&mut fs.pending);
            fs.pending = restored;
            return Err(e);
        }
        Ok(())
    }

    fn snapshot_all(&self) -> Vec<(TxKey, TombValue)> {
        let mut out = Vec::with_capacity(self.len());
        for shard in &self.shards {
            for (k, v) in shard.read().iter() {
                out.push((*k, *v));
            }
        }
        out
    }

    fn tmp_path(&self) -> PathBuf {
        let mut p = self.path.clone().into_os_string();
        p.push(".tmp");
        PathBuf::from(p)
    }

    /// Atomic rewrite (compaction): header + all live entries → tempfile →
    /// fsync → rename → parent-dir fsync.
    fn write_all_atomic(&self, entries: &[(TxKey, TombValue)]) -> std::io::Result<()> {
        let mut buf = Vec::with_capacity(TOMB_HEADER_SIZE + entries.len() * TOMBSTONE_ENTRY_SIZE);
        buf.extend_from_slice(&TOMB_MAGIC.to_le_bytes());
        buf.extend_from_slice(&TOMB_VERSION.to_le_bytes());
        for (k, v) in entries {
            buf.extend_from_slice(&encode_entry(&k.txid, v.generation, v.height, v.cause));
        }
        let tmp = self.tmp_path();
        std::fs::write(&tmp, &buf)?;
        let f = std::fs::File::open(&tmp)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, &self.path)?;
        crate::fsutil::fsync_parent_dir(&self.path)?;
        Ok(())
    }

    /// Append the buffered tail to the durable file (creating it with a header
    /// first if absent), then fsync.
    fn append_entries(&self, entries: &[(TxKey, TombValue)]) -> std::io::Result<()> {
        use std::io::Write;
        let existed = self.path.exists();
        let mut f = if existed {
            std::fs::OpenOptions::new().append(true).open(&self.path)?
        } else {
            let mut nf = std::fs::File::create(&self.path)?;
            let mut hdr = [0u8; TOMB_HEADER_SIZE];
            hdr[0..4].copy_from_slice(&TOMB_MAGIC.to_le_bytes());
            hdr[4..8].copy_from_slice(&TOMB_VERSION.to_le_bytes());
            nf.write_all(&hdr)?;
            nf
        };
        let mut buf = Vec::with_capacity(entries.len() * TOMBSTONE_ENTRY_SIZE);
        for (k, v) in entries {
            buf.extend_from_slice(&encode_entry(&k.txid, v.generation, v.height, v.cause));
        }
        f.write_all(&buf)?;
        f.sync_all()?;
        if !existed {
            crate::fsutil::fsync_parent_dir(&self.path)?;
        }
        Ok(())
    }
}

/// A tombstone at `height` is expired once `height + retention <= floor`.
/// Saturating so a pathological `height + retention` overflow keeps it retained
/// (the conservative direction: never GC early).
fn expired(height: u32, retention: u32, floor: u32) -> bool {
    height.saturating_add(retention) <= floor
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tk(b: u8) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0] = b;
        txid[1] = 0xAA;
        TxKey { txid }
    }

    #[test]
    fn entry_encode_decode_roundtrip() {
        let key = tk(7);
        let bytes = encode_entry(&key.txid, 42, 900, TombstoneCause::ClientDelete as u8);
        assert_eq!(bytes.len(), TOMBSTONE_ENTRY_SIZE);
        let (k, v) = decode_entry(&bytes).expect("roundtrip decodes");
        assert_eq!(k, key);
        assert_eq!(v.generation, 42);
        assert_eq!(v.height, 900);
        assert_eq!(v.cause, TombstoneCause::ClientDelete as u8);
    }

    #[test]
    fn decode_rejects_crc_corruption() {
        let key = tk(7);
        let mut bytes = encode_entry(&key.txid, 42, 900, 0);
        bytes[10] ^= 0xFF; // flip a payload byte, CRC now mismatches
        match decode_entry(&bytes) {
            Err(TombstoneDecodeError::CrcMismatch { .. }) => {}
            other => panic!("expected CrcMismatch, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_truncated() {
        match decode_entry(&[0u8; 10]) {
            Err(TombstoneDecodeError::Truncated { got: 10, want: 48 }) => {}
            other => panic!("expected Truncated, got {other:?}"),
        }
    }

    #[test]
    fn record_query_and_generation_ordering() {
        let log = TombstoneLog::new(PathBuf::from("/nonexistent/x.tombstones"), 0, 4, 100);
        let key = tk(3);
        log.record(&key, 5, 900, TombstoneCause::Dah);
        assert!(log.at_or_ahead(&key, 4));
        assert!(log.at_or_ahead(&key, 5));
        assert!(!log.at_or_ahead(&key, 6));
        assert!(!log.at_or_ahead(&tk(99), 0));
        assert_eq!(log.lookup(&key), Some((5, 900)));
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn gc_expires_only_past_horizon() {
        let log = TombstoneLog::new(PathBuf::from("/nonexistent/x.tombstones"), 0, 4, 10);
        log.record(&tk(1), 1, 100, TombstoneCause::Dah);
        log.record(&tk(2), 1, 200, TombstoneCause::Dah);
        // floor 109: tk(1) at 100 => 100+10=110 > 109 retained; tk(2) retained.
        assert_eq!(log.gc(109, |_| false), 0);
        // floor 110: tk(1) expires (100+10<=110), tk(2) retained (200+10>110).
        assert_eq!(log.gc(110, |_| false), 1);
        assert!(log.lookup(&tk(1)).is_none());
        assert!(log.lookup(&tk(2)).is_some());
    }

    /// Phase 2d — the retention/GC-vs-heal race guard at the log level: a
    /// past-retention tombstone whose shard `is_protected` reports as guarded by
    /// an in-flight reverse-heal is RETAINED (so RULE-DS can still gate an
    /// incoming image); an unprotected past-retention tombstone is dropped in
    /// the same pass. Once protection lifts, the next GC drops it normally — the
    /// guard only DEFERS, it never leaks.
    #[test]
    fn gc_retains_expired_tombstone_while_shard_protected() {
        let log = TombstoneLog::new(PathBuf::from("/nonexistent/x.tombstones"), 0, 4, 10);
        let healed = tk(1); // protected by an in-flight heal
        let unrelated = tk(2); // no heal references its shard
        log.record(&healed, 7, 100, TombstoneCause::Dah);
        log.record(&unrelated, 7, 100, TombstoneCause::Dah);

        // floor 200: BOTH are past retention (100 + 10 <= 200). While `healed`
        // is protected, only `unrelated` is dropped; `healed` is retained.
        let protect_healed = |k: &TxKey| *k == healed;
        assert_eq!(
            log.gc(200, protect_healed),
            1,
            "only the unprotected past-retention tombstone is GC'd mid-heal",
        );
        assert!(
            log.at_or_ahead(&healed, 7),
            "the protected tombstone is retained so RULE-DS still blocks a \
             resurrecting heal image at its shard",
        );
        assert!(
            log.lookup(&unrelated).is_none(),
            "the unrelated past-retention tombstone is GC'd normally mid-heal",
        );

        // Heal completes → protection lifts → the next GC drops it normally.
        assert_eq!(
            log.gc(200, |_| false),
            1,
            "once the heal completes the deferred tombstone is GC'd (no leak)",
        );
        assert!(log.lookup(&healed).is_none());
    }

    /// Phase 2d — with NO shard protected (the common no-heal-in-flight case),
    /// `gc` is byte-for-byte the pre-guard behaviour: every past-retention
    /// tombstone is dropped, none leaked.
    #[test]
    fn gc_unchanged_when_nothing_protected() {
        let log = TombstoneLog::new(PathBuf::from("/nonexistent/x.tombstones"), 0, 4, 10);
        log.record(&tk(1), 1, 100, TombstoneCause::Dah);
        log.record(&tk(2), 1, 100, TombstoneCause::Dah);
        // Both past retention (100 + 10 <= 200), nothing protected → both drop.
        assert_eq!(log.gc(200, |_| false), 2);
        assert!(log.is_empty());
    }

    #[test]
    fn persist_then_load_roundtrips_via_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.tombstones");
        let log = TombstoneLog::new(path.clone(), 0, 4, 100);
        log.record(&tk(1), 3, 500, TombstoneCause::ClientDelete);
        log.record(&tk(2), 7, 600, TombstoneCause::Dah);
        log.persist(0, |_| false).unwrap();

        let reloaded = TombstoneLog::load(path.clone(), 0, 4, 100).unwrap();
        assert_eq!(reloaded.len(), 2);
        assert_eq!(reloaded.lookup(&tk(1)), Some((3, 500)));
        assert_eq!(reloaded.lookup(&tk(2)), Some((7, 600)));
    }

    #[test]
    fn persist_compacts_after_gc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.tombstones");
        let log = TombstoneLog::new(path.clone(), 0, 4, 10);
        log.record(&tk(1), 1, 100, TombstoneCause::Dah);
        log.record(&tk(2), 1, 200, TombstoneCause::Dah);
        log.persist(0, |_| false).unwrap(); // both durable (append)

        // Advance the floor past tk(1)'s horizon and persist: GC drops tk(1) and
        // the file is COMPACTED, so a fresh reload sees only tk(2).
        log.persist(110, |_| false).unwrap();
        let reloaded = TombstoneLog::load(path, 0, 4, 10).unwrap();
        assert_eq!(reloaded.len(), 1);
        assert!(reloaded.lookup(&tk(1)).is_none());
        assert!(reloaded.lookup(&tk(2)).is_some());
    }

    #[test]
    fn reconcile_drops_live_keys() {
        let log = TombstoneLog::new(PathBuf::from("/nonexistent/x.tombstones"), 0, 4, 100);
        log.record(&tk(1), 1, 100, TombstoneCause::Dah);
        log.record(&tk(2), 1, 100, TombstoneCause::Dah);
        // tk(1) is "live" again (resurrected) — its dangling tombstone must go.
        let dropped = log.reconcile_against_live(|k| *k == tk(1));
        assert_eq!(dropped, 1);
        assert!(log.lookup(&tk(1)).is_none());
        assert!(log.lookup(&tk(2)).is_some());
    }

    /// P1 race repro. A `record()` that lands in the compaction window — after
    /// `snapshot_all()`/`write_all_atomic` but before `pending` is reconciled —
    /// must NOT be lost. The old `pending.clear()` dropped it from both the
    /// written file and `pending`, so it survived only in the volatile shard map
    /// until some later compaction; a crash in that window left a durable delete
    /// with no durable tombstone (→ Phase-2c resurrection). The `retain` keeps it
    /// buffered so the next checkpoint appends it. This drives the REAL `persist`
    /// via the deterministic in-window injection seam, then models the next
    /// checkpoint's append + a fresh reload from the DURABLE file only (pending is
    /// volatile — a crash keeps only what reached disk).
    #[test]
    fn compaction_window_record_survives_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.tombstones");
        let log = TombstoneLog::new(path.clone(), 0, 4, 100);

        // A first durable entry (append path), then force the COMPACT branch.
        log.record(&tk(1), 1, 500, TombstoneCause::Dah);
        log.persist(0, |_| false).unwrap();
        log.file.lock().needs_compaction = true;

        // Arm the in-window injection: this `record(tk(2))` runs after the atomic
        // rewrite (which captured only tk(1)) but before the `pending` reconcile.
        *log.inject_in_compaction_window.lock() =
            Some((tk(2), 9, 600, TombstoneCause::ClientDelete));
        log.persist(0, |_| false).unwrap();

        // Next checkpoint: append whatever stayed buffered in `pending`. With
        // `retain` tk(2) is still buffered and reaches disk here; with the old
        // `clear()` it was dropped and only the (now discarded) shard map held it.
        log.persist(0, |_| false).unwrap();

        // Reload from the DURABLE FILE only (no in-RAM carry-over): tk(2) must be
        // present, proving it survived a crash-equivalent reload.
        let reloaded = TombstoneLog::load(path, 0, 4, 100).unwrap();
        assert_eq!(
            reloaded.lookup(&tk(2)),
            Some((9, 600)),
            "a delete recorded in the compaction window must survive to the durable \
             file (retain), not be lost by pending.clear()",
        );
        assert!(reloaded.at_or_ahead(&tk(2), 0));
        // tk(1) is unaffected.
        assert_eq!(reloaded.lookup(&tk(1)), Some((1, 500)));
    }

    /// `clear` drops the tombstone from the in-RAM index and from any un-persisted
    /// `pending` append, so a subsequent compaction cannot re-append it (the P1
    /// `retain` + P2-2 `clear` interaction). A never-deleted key is a no-op.
    #[test]
    fn clear_removes_tombstone_from_index_and_pending() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.tombstones");
        let log = TombstoneLog::new(path.clone(), 0, 4, 100);
        log.record(&tk(1), 3, 500, TombstoneCause::Dah);

        assert!(!log.clear(&tk(99)), "clearing an absent key is a no-op");

        assert!(log.clear(&tk(1)), "clearing a present key reports removal");
        assert!(log.lookup(&tk(1)).is_none(), "in-RAM index entry is gone");
        assert!(
            log.file.lock().pending.iter().all(|(k, _)| *k != tk(1)),
            "the buffered append for the cleared key is drained too",
        );

        // Compaction after a clear must not resurrect the tombstone in the file.
        log.persist(0, |_| false).unwrap();
        let reloaded = TombstoneLog::load(path, 0, 4, 100).unwrap();
        assert!(
            reloaded.lookup(&tk(1)).is_none(),
            "a cleared tombstone must not be re-appended by compaction",
        );
    }

    /// Reverse-heal Phase 2c RULE-DS gate is CAUSE-AWARE: a `Dah` (terminal)
    /// tombstone drops by the wrapping generation compare; a `ClientDelete` /
    /// `PruneReplace` tombstone — with no terminal-generation guarantee — drops
    /// UNCONDITIONALLY, so a diverged source's strictly-newer image cannot
    /// resurrect a client-deleted record (the consensus closure).
    #[test]
    fn blocks_heal_apply_is_cause_aware() {
        let log = TombstoneLog::new(PathBuf::from("/nonexistent/x.tombstones"), 0, 4, 100);

        // Dah (terminal): generation-based — drops at-or-behind N, admits newer.
        let dah = tk(1);
        log.record(&dah, 5, 900, TombstoneCause::Dah);
        assert!(
            log.blocks_heal_apply(&dah, 4),
            "Dah drops a source behind N"
        );
        assert!(log.blocks_heal_apply(&dah, 5), "Dah drops a source at N");
        assert!(
            !log.blocks_heal_apply(&dah, 6),
            "Dah ADMITS a strictly-newer source (incoming > N): this is REACHABLE \
             (a reorg unspend legitimately pushes g_src past N) and correct — \
             LWW-by-generation treats g_src > N as a genuinely-newer state to \
             converge on, NOT a resurrection",
        );

        // ClientDelete: unconditional — drops even a strictly-newer source image.
        let client = tk(2);
        log.record(&client, 5, 900, TombstoneCause::ClientDelete);
        assert!(log.blocks_heal_apply(&client, 4));
        assert!(log.blocks_heal_apply(&client, 5));
        assert!(
            log.blocks_heal_apply(&client, 9),
            "ClientDelete drops a strictly-newer source (consensus closure)",
        );

        // PruneReplace: also unconditional.
        let prune = tk(3);
        log.record(&prune, 5, 900, TombstoneCause::PruneReplace);
        assert!(log.blocks_heal_apply(&prune, 99));

        // No tombstone → never blocks.
        assert!(!log.blocks_heal_apply(&tk(42), 0));
    }

    /// W9 — the CompensatedCreate leg of RULE-DS: a compensation tombstone at
    /// generation `N` is OVERRIDABLE by an incoming heal/migration create at
    /// `incoming >= N` (a live client-confirmed copy must defeat its own
    /// crash-window rollback) and still blocks a strictly-stale image
    /// (`incoming < N`). The ClientDelete veto stays unconditional — pinned
    /// again here side by side so the two legs can never be conflated.
    #[test]
    fn blocks_heal_apply_compensated_create_is_generation_overridable() {
        let log = TombstoneLog::new(PathBuf::from("/nonexistent/x.tombstones"), 0, 4, 100);

        let comp = tk(1);
        log.record(&comp, 5, 900, TombstoneCause::CompensatedCreate);
        assert!(
            log.blocks_heal_apply(&comp, 4),
            "CompensatedCreate still blocks a strictly-stale source (incoming < N)",
        );
        assert!(
            !log.blocks_heal_apply(&comp, 5),
            "CompensatedCreate ADMITS a source at N: the rolled-back create's own \
             surviving live copy carries exactly the tombstone generation",
        );
        assert!(
            !log.blocks_heal_apply(&comp, 6),
            "CompensatedCreate ADMITS a strictly-newer source (incoming > N)",
        );

        // The CI shape: compensation tombstones the fresh create at gen 0; the
        // surviving acked copy heals back in at gen >= 0.
        let comp_zero = tk(2);
        log.record(&comp_zero, 0, 900, TombstoneCause::CompensatedCreate);
        assert!(!log.blocks_heal_apply(&comp_zero, 0));
        assert!(!log.blocks_heal_apply(&comp_zero, 3));

        // Contrast pin: ClientDelete at the same generations stays an
        // unconditional veto (the #78 posture, untouched by W9).
        let client = tk(3);
        log.record(&client, 0, 900, TombstoneCause::ClientDelete);
        assert!(log.blocks_heal_apply(&client, 0));
        assert!(log.blocks_heal_apply(&client, 99));
    }

    /// W9 resurrection-safety pin — tombstone-cause PRECEDENCE for the same
    /// key. A later ClientDelete REPLACES a CompensatedCreate (the
    /// authoritative client claim upgrades the rollback claim), but a later
    /// CompensatedCreate must NOT downgrade an existing ClientDelete (or any
    /// other cause): were it to, a compensation racing a client delete would
    /// re-open the resurrection window #78 closed.
    #[test]
    fn compensated_create_never_downgrades_a_stronger_tombstone() {
        let log = TombstoneLog::new(PathBuf::from("/nonexistent/x.tombstones"), 0, 4, 100);

        // ClientDelete then CompensatedCreate → ClientDelete stands
        // (unconditional veto retained, generation retained).
        let k1 = tk(1);
        log.record(&k1, 7, 900, TombstoneCause::ClientDelete);
        log.record(&k1, 9, 901, TombstoneCause::CompensatedCreate);
        assert_eq!(log.lookup_cause(&k1), Some(TombstoneCause::ClientDelete));
        assert_eq!(log.lookup(&k1), Some((7, 900)));
        assert!(
            log.blocks_heal_apply(&k1, 99),
            "the client delete's unconditional veto must survive a later \
             compensation record for the same key",
        );

        // Dah then CompensatedCreate → Dah stands (defense in depth; TS-1
        // makes this unreachable in production but the downgrade must still
        // be structurally impossible).
        let k2 = tk(2);
        log.record(&k2, 7, 900, TombstoneCause::Dah);
        log.record(&k2, 9, 901, TombstoneCause::CompensatedCreate);
        assert_eq!(log.lookup_cause(&k2), Some(TombstoneCause::Dah));

        // CompensatedCreate then ClientDelete → the client delete REPLACES it
        // (normal last-writer-wins upgrade).
        let k3 = tk(3);
        log.record(&k3, 2, 900, TombstoneCause::CompensatedCreate);
        log.record(&k3, 5, 905, TombstoneCause::ClientDelete);
        assert_eq!(log.lookup_cause(&k3), Some(TombstoneCause::ClientDelete));
        assert_eq!(log.lookup(&k3), Some((5, 905)));
        assert!(log.blocks_heal_apply(&k3, 99));

        // CompensatedCreate then CompensatedCreate → last writer wins within
        // the same cause (a re-rolled-back re-create carries newer state).
        let k4 = tk(4);
        log.record(&k4, 2, 900, TombstoneCause::CompensatedCreate);
        log.record(&k4, 6, 905, TombstoneCause::CompensatedCreate);
        assert_eq!(log.lookup(&k4), Some((6, 905)));
        assert_eq!(
            log.lookup_cause(&k4),
            Some(TombstoneCause::CompensatedCreate)
        );
    }

    /// W9 — the on-disk codec round-trips the new cause byte, and
    /// `lookup_cause` maps it back distinctly (not folded into the
    /// ClientDelete fallback).
    #[test]
    fn compensated_create_cause_roundtrips_and_maps() {
        let key = tk(7);
        let bytes = encode_entry(&key.txid, 4, 800, TombstoneCause::CompensatedCreate as u8);
        let (k, v) = decode_entry(&bytes).expect("roundtrip decodes");
        assert_eq!(k, key);
        assert_eq!(v.cause, TombstoneCause::CompensatedCreate as u8);

        let log = TombstoneLog::new(PathBuf::from("/nonexistent/x.tombstones"), 0, 4, 100);
        log.record(&key, 4, 800, TombstoneCause::CompensatedCreate);
        assert_eq!(
            log.lookup_cause(&key),
            Some(TombstoneCause::CompensatedCreate)
        );
    }

    #[test]
    fn load_missing_file_is_empty() {
        let log = TombstoneLog::load(
            PathBuf::from("/nonexistent/definitely/missing.tombstones"),
            0,
            4,
            100,
        )
        .unwrap();
        assert!(log.is_empty());
    }
}
