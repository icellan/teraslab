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
    /// W9 P1-1 — a LOCAL migration reconcile, not a client delete: the #29
    /// completion prune (the target drops a key the authoritative source's
    /// manifest omitted, `server::dispatch` OP_MIGRATION_COMPLETE) and the
    /// receiver's replace-duplicate delete
    /// (`replication::receiver::apply_create_replica`). Generation-gated in
    /// [`TombstoneLog::blocks_heal_apply`] (Dah-style): the reconcile's
    /// anti-resurrection purpose holds for at-or-behind images, while a
    /// strictly-newer live copy heals back in — recording these as
    /// `ClientDelete` gave a node that never client-deleted the unconditional
    /// veto, the same permanent acked-write-loss chain as the compensation
    /// family. Both producers are LOCAL-ONLY deletes (no `RedoOp::Delete`
    /// journalled, nothing replicated), so [`DeleteCause`] deliberately has
    /// no `PruneReplace` variant — see its doc.
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
/// (never journalled as `Delete`, never replicated), and `PruneReplace`'s two
/// producers (the #29 completion prune, the receiver replace-duplicate
/// reconcile) are local-only `Engine` deletes that journal no `RedoOp::Delete`
/// and fan nothing out — a `PruneReplace` variant here would be dead wire code.
/// If a future path ever replicates or journals one, extend the tag scheme
/// exactly as for `CompensatedCreate` (new tag/opcode, same body).
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
            veto_suspended: false,
        },
    ))
}

/// W10 FIX 2 / W13 — outcome of a weak-veto arbitration
/// ([`TombstoneLog::suspend_weak_veto`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeakTombstoneClear {
    /// A weak-cause tombstone was present and its RULE-DS veto is now
    /// SUSPENDED. The marker survives so it keeps declaring the omission to
    /// peers — see [`TombstoneLog::suspend_weak_veto`] for why removing it
    /// re-opened the last-live-copy prune chain.
    Suspended,
    /// No tombstone covers the key (idempotent success for a retried round).
    Absent,
    /// The tombstone carries a STRONG cause (`ClientDelete` / `Dah` / an
    /// unrecognized future byte) — never arbitrable; nothing was changed.
    RefusedStrongCause,
}

/// In-RAM per-key tombstone state.
#[derive(Clone, Copy, Debug, Eq)]
struct TombValue {
    generation: u32,
    height: u32,
    cause: u8,
    /// W13 — the weak-veto arbitration has SUSPENDED this marker's RULE-DS
    /// veto ([`TombstoneLog::suspend_weak_veto`]), but the marker itself
    /// survives so it keeps DECLARING the omission to peers
    /// ([`TombstoneLog::weak_tombstone_keys`]).
    ///
    /// RAM-only and deliberately absent from the 48-byte on-disk entry: a
    /// suspension that does not survive a restart re-arms the veto, which is
    /// the conservative direction (the source re-drives a completion, gets a
    /// fresh refusal, and re-arbitrates). Because it is not durable it must
    /// also not perturb the compaction bookkeeping in [`TombstoneLog::persist`],
    /// which compares a buffered append against the written snapshot — hence
    /// the hand-written [`PartialEq`] below over the DURABLE fields only.
    veto_suspended: bool,
}

impl PartialEq for TombValue {
    /// Durable-field equality: `veto_suspended` is RAM-only (see the field
    /// doc), so two values that differ only in suspension describe the same
    /// on-disk entry and must compare equal — otherwise `persist`'s
    /// `pending.retain` would keep re-appending an already-written entry every
    /// time an arbitration suspended it.
    fn eq(&self, other: &Self) -> bool {
        self.generation == other.generation
            && self.height == other.height
            && self.cause == other.cause
    }
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
    /// W10 review P2-3 — secondary index over the WEAK-cause entries
    /// (`PruneReplace` / `CompensatedCreate`) only.
    ///
    /// [`Self::weak_tombstone_keys`] is read once per completion SEND (W10
    /// review P2-1 requires a FRESH capture per attempt, not a stale
    /// pre-fold snapshot), so it must not cost a full scan of every live
    /// tombstone. Weak entries are a small subset — local reconcile /
    /// rollback markers, not the client-delete population — so this set is
    /// typically tiny and the per-send read is O(weak) instead of O(all live
    /// tombstones).
    ///
    /// Maintained in lockstep with `shards` by EVERY mutation path
    /// ([`Self::record`], [`Self::clear`], [`Self::gc`],
    /// [`Self::reconcile_against_live`], and the [`Self::load`] replay).
    /// [`Self::suspend_weak_veto`] deliberately does NOT touch it: a suspended
    /// marker keeps declaring its omission (that is the point of suspending
    /// rather than removing). It is a pure accelerator: `shards` remains the authority, and
    /// [`Self::weak_tombstone_keys`] re-verifies each candidate's cause
    /// against `shards` before returning it, so a stale entry here can only
    /// cost a wasted lookup — never a wrong answer.
    weak_keys: RwLock<std::collections::HashSet<TxKey>>,
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
            weak_keys: RwLock::new(std::collections::HashSet::new()),
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
                    // P2-3: keep the weak accelerator in step with the
                    // replay (append order = time order, so the LAST entry
                    // for a re-deleted key decides its membership).
                    if is_weak_cause(value.cause) {
                        log.weak_keys.write().insert(key);
                    } else {
                        log.weak_keys.write().remove(&key);
                    }
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
    /// carve-out: a WEAK cause ([`TombstoneCause::CompensatedCreate`] or
    /// [`TombstoneCause::PruneReplace`] — the generation-overridable local
    /// rollback/reconcile markers) never REPLACES an existing tombstone of a
    /// different cause. For the strong causes this is protection — a weak
    /// claim must not downgrade `ClientDelete`'s (or `Dah`'s) veto. Between
    /// the TWO weak causes, first-claim-stands is NOT a protection rule:
    /// neither order is uniformly more conservative (each cause's window
    /// admits what the other blocks at some generations). It is chosen
    /// because the collision is TS-1-unreachable in production (the
    /// tombstone clears whenever the key comes back live, and deleting an
    /// absent record writes nothing), both causes are weak markers on a node
    /// that never client-deleted (safety leg 3 covers either outcome), and a
    /// fixed rule pinned in both directions beats an order-dependent one.
    /// The reverse direction stays plain LWW: a later `ClientDelete` (or
    /// `Dah`) for the same key upgrades either weak cause. The strong-cause
    /// downgrade must be structurally impossible, not merely unlikely (see
    /// [`Self::blocks_heal_apply`]'s safety argument).
    pub fn record(&self, key: &TxKey, generation: u32, height: u32, cause: TombstoneCause) {
        let value = TombValue {
            generation,
            height,
            cause: cause as u8,
            // W13 — a FRESH record always re-arms the veto: a re-delete of a
            // key whose earlier marker had been arbitration-suspended is new
            // evidence, not a continuation of the suspended one.
            veto_suspended: false,
        };
        let weak = matches!(
            cause,
            TombstoneCause::CompensatedCreate | TombstoneCause::PruneReplace
        );
        // Two disjoint locks, never held simultaneously here: shard write drops
        // before the file lock is taken, so `persist` (file-then-shard) can
        // never invert against this path.
        {
            let mut shard = self.shards[self.shard_index(key)].write();
            if weak
                && shard
                    .get(key)
                    .is_some_and(|existing| existing.cause != cause as u8)
            {
                // The existing (different-cause) claim stands; record nothing
                // (the existing entry is already durable-or-pending on its
                // own).
                return;
            }
            shard.insert(*key, value);
        }
        // P2-3: a weak cause joins the accelerator; a STRONG cause that
        // last-writer-wins over a weak one must LEAVE it (the strong claim now
        // governs, and a stale weak entry would be re-declared to peers).
        //
        // NOT ATOMIC with the shard write above (W10 review nit-3): the shard
        // guard is released before this insert, so a `weak_tombstone_keys()`
        // racing exactly here UNDER-declares that key for one completion
        // frame. Same class as the manifest fold's own snapshot race, narrowed
        // by the per-send recapture (P2-1) from a whole fold to a few
        // instructions — and the target-side enumeration-cutoff gate, not the
        // declaration, is the primary guard for the corresponding prune race.
        // Deliberately not widened to hold both locks: `shards` is the
        // authority and every read re-verifies the cause against it.
        if weak {
            self.weak_keys.write().insert(*key);
        } else {
            self.weak_keys.write().remove(key);
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
    /// - A [`TombstoneCause::ClientDelete`] tombstone carries NO
    ///   terminal-generation guarantee: a client may delete a STILL-MUTATING
    ///   record whose generation a diverged source later exceeds (the
    ///   2a/2b-review consensus concern — a generation-only gate would miss
    ///   it and resurrect the record). For this cause the heal drops
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
    /// - W9 P1-1 — a [`TombstoneCause::PruneReplace`] tombstone (the #29
    ///   completion prune / the receiver replace-duplicate reconcile) uses the
    ///   SAME generation rule as `Dah`: block iff the tombstone generation is
    ///   at-or-ahead of the incoming image. The reconcile's purpose — keeping
    ///   the stale copy the authoritative manifest omitted from resurrecting —
    ///   is exactly the at-or-behind window; a strictly-newer image is a live
    ///   copy the cluster still acknowledges, and a LOCAL reconcile marker on
    ///   a node that never client-deleted the key must not veto it forever
    ///   (pre-W9 these deletes recorded `ClientDelete` and re-opened the
    ///   permanent acked-write-loss chain through a third producer).
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
    ///   4. **The generation floor is a best-effort staleness filter, NOT a
    ///      safety guarantee** (review P2-4). A compensation tombstones a
    ///      just-created record, so `N` is 0 in practice and `incoming >= N`
    ///      admits essentially every image — the CompensatedCreate "veto" is
    ///      effectively vacuous, and legs 1–3 (cause separation, precedence,
    ///      no-worse-than-a-bare-node) are the ACTUAL safety argument. The
    ///      floor only filters the rare provably-stale same-lineage image;
    ///      cross-lineage generation comparability remains out of scope
    ///      (#78), exactly as for every other generation-gated leg.
    /// - W13 — a WEAK marker whose veto the arbitration has SUSPENDED
    ///   ([`Self::suspend_weak_veto`]) does not block at all. The marker stays
    ///   in the log for its DECLARATION role (see [`Self::gc`]); only its veto
    ///   is lifted, and only after this node itself refused a completion naming
    ///   the key and a ticketed holder redeemed that refusal. Suspension is
    ///   RAM-only, so a restart re-arms the veto.
    pub fn blocks_heal_apply(&self, key: &TxKey, incoming_generation: u32) -> bool {
        match self.shards[self.shard_index(key)].read().get(key) {
            None => false,
            // W13 — arbitration-suspended weak marker: declaration only.
            // Checked before the cause arms so it cannot be reached for a
            // strong cause (which `suspend_weak_veto` refuses to set it on).
            Some(v) if v.veto_suspended && is_weak_cause(v.cause) => false,
            // Generation-gated legs (Dah-style): block a source at-or-behind
            // the frozen generation, admit a strictly-newer one.
            Some(v)
                if v.cause == TombstoneCause::Dah as u8
                    || v.cause == TombstoneCause::PruneReplace as u8 =>
            {
                generation_at_or_ahead(v.generation, incoming_generation)
            }
            Some(v) if v.cause == TombstoneCause::CompensatedCreate as u8 => {
                !generation_at_or_ahead(incoming_generation, v.generation)
            }
            // ClientDelete + any unrecognized future cause: unconditional
            // (fail-closed, the #78 posture).
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

    /// W10 FIX 3 — every key currently covered by a WEAK-cause tombstone
    /// (`PruneReplace` / `CompensatedCreate`). Consumed by the migration
    /// completion builder so a source can declare which of its manifest
    /// omissions are its OWN local reconcile/rollback markers rather than
    /// deletion-intent. O(live tombstones) full scan — the tombstone
    /// population is bounded by deletes within the retention horizon, and
    /// the caller filters to one cluster shard.
    pub fn weak_tombstone_keys(&self) -> Vec<TxKey> {
        // W10 review P2-3 — read the weak ACCELERATOR (small), then re-verify
        // each candidate's cause against `shards` (the authority). A stale
        // accelerator entry therefore costs one wasted lookup and is dropped
        // from the result, never mis-declared to a peer.
        let candidates: Vec<TxKey> = self.weak_keys.read().iter().copied().collect();
        candidates
            .into_iter()
            .filter(|k| {
                self.shards[self.shard_index(k)]
                    .read()
                    .get(k)
                    .is_some_and(|v| is_weak_cause(v.cause))
            })
            .collect()
    }

    /// Total live tombstone count across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// W10 composition review P2-3 — live WEAK-cause tombstones
    /// (`PruneReplace` / `CompensatedCreate`), O(1) off the weak accelerator.
    ///
    /// This is the population [`Self::gc`] deliberately no longer bounds by
    /// the retention clock, so it must be operator-visible: it drains only
    /// when each key's repair lands, and a value that keeps climbing means
    /// repairs are not landing. Read off the accelerator rather than the
    /// authority, so it can transiently over-count a stale accelerator entry
    /// by exactly the amount [`Self::weak_tombstone_keys`] would drop on its
    /// re-verification pass — a gauge, not a decision input.
    pub fn weak_len(&self) -> usize {
        self.weak_keys.read().len()
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
    ///
    /// # W10 composition review P2-6 — WEAK causes have NO retention horizon
    ///
    /// A WEAK-cause tombstone ([`TombstoneCause::PruneReplace`] /
    /// [`TombstoneCause::CompensatedCreate`]) plays a SECOND role the
    /// block-height retention clock does not measure: it is this node's only
    /// proof that a key MISSING from the migration manifest it ships is its
    /// own local prune/rollback damage rather than deletion-intent
    /// ([`Self::weak_tombstone_keys`] → `Engine::weak_tombstone_keys_for_shard`
    /// → the completion frame's declaration). A migration target RETAINS its
    /// live copy of a DECLARED omission and PRUNES an undeclared one.
    ///
    /// Expiring a weak tombstone on the retention clock therefore silently
    /// converts "my own damage" into "deletion-intent" the moment repair is
    /// slower than `retention_blocks`: the source keeps omitting key K from
    /// its manifest, stops declaring it, and the target's #29 prune deletes
    /// its LAST LIVE COPY — the armed-05 loss chain, recurring past the
    /// horizon. The claim's lifetime must be bounded by the REPAIR, not by
    /// block height, so weak causes are never expired here.
    ///
    /// This does not strand a weak veto. TWO drains clear one, and both of them
    /// ARE the repair landing:
    ///
    /// * [`Self::clear`] — Invariant TS-1: the key comes back LIVE (client
    ///   create, replica create, migration baseline apply);
    /// * [`Self::reconcile_against_live`] — the boot reconcile.
    ///
    /// W13 — the `OP_MIGRATION_WEAK_VETO_ARBITRATE` handshake is NO LONGER one
    /// of them. It used to remove the marker, which withdrew the declaration
    /// whether or not the re-push that was supposed to follow ever landed —
    /// exactly the conversion this doc warns about, performed on demand. It now
    /// SUSPENDS the veto ([`Self::suspend_weak_veto`]) and leaves the marker in
    /// place, so the drain in that flow is the re-push landing, i.e.
    /// [`Self::clear`] above.
    ///
    /// ACCEPTED RESIDUALS (documented, not fixed here):
    /// 1. A key that is NEVER repaired keeps one tombstone entry (in RAM and
    ///    in the durable file) indefinitely. Weak causes are produced only by
    ///    the exceptional prune/rollback paths, never by steady-state deletes.
    ///    Because this trades a block-height bound for a repair bound, the
    ///    population is EXPORTED (W10 composition review P2-3): the metrics
    ///    endpoint renders `teraslab_tombstone_entries` and
    ///    `teraslab_tombstone_weak_entries` from [`Self::len`] /
    ///    [`Self::weak_len`]; a weak gauge that keeps climbing means repairs
    ///    are not landing.
    /// 2. With `migration_weak_veto_arbitration_enabled = false` the
    ///    arbitration drain is disarmed, so a retained weak veto can keep
    ///    blocking an at-or-behind same-generation re-push that retention GC
    ///    used to eventually unblock. That mode already accepts an
    ///    indefinitely under-replicated shard, but it USED to self-heal on the
    ///    retention clock and no longer does — the change note lives at the
    ///    flag itself (`Config::migration_weak_veto_arbitration_enabled`).
    ///    The trade is a stalled repair, never a deleted last copy.
    pub fn gc(&self, last_durable_height: u32, is_protected: impl Fn(&TxKey) -> bool) -> usize {
        let retention = self.retention_blocks;
        let mut dropped = 0usize;
        for shard in &self.shards {
            let mut g = shard.write();
            let before = g.len();
            // Drop only when past retention, NOT heal-protected, and NOT a
            // weak-cause omission claim (see the doc above). Since no weak
            // entry can be dropped here, the weak accelerator cannot go stale
            // through this path and needs no pruning.
            g.retain(|k, v| {
                is_weak_cause(v.cause)
                    || !expired(v.height, retention, last_durable_height)
                    || is_protected(k)
            });
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
        let mut dropped_weak: Vec<TxKey> = Vec::new();
        for shard in &self.shards {
            let mut g = shard.write();
            let before = g.len();
            g.retain(|k, v| {
                let keep = !is_live(k);
                if !keep && is_weak_cause(v.cause) {
                    dropped_weak.push(*k);
                }
                keep
            });
            dropped += before - g.len();
        }
        if !dropped_weak.is_empty() {
            let mut weak = self.weak_keys.write();
            for k in &dropped_weak {
                weak.remove(k);
            }
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
            self.weak_keys.write().remove(key);
            let mut fs = self.file.lock();
            fs.pending.retain(|(pk, _)| pk != key);
            fs.needs_compaction = true;
        }
        removed
    }

    /// W10 FIX 2 / W13 — weak-veto arbitration: SUSPEND `key`'s RULE-DS veto,
    /// ONLY when its recorded cause is WEAK ([`TombstoneCause::PruneReplace`]
    /// or [`TombstoneCause::CompensatedCreate`]). The marker itself SURVIVES.
    ///
    /// # W13 — why suspend rather than remove (the data-loss direction)
    ///
    /// W10 removed the entry outright. A weak marker has a SECOND role the
    /// removal destroyed: it is this node's only proof that a key missing from
    /// the manifest it SHIPS is its own prune/rollback damage rather than
    /// deletion-intent ([`Self::weak_tombstone_keys`] ->
    /// `Engine::weak_tombstone_keys_for_shard` -> the completion frame ->
    /// the peer's prune exclusion). [`Self::gc`] already refuses to expire weak
    /// causes on the retention clock for exactly that reason, and names the
    /// consequence: withdrawing the declaration "silently converts 'my own
    /// damage' into 'deletion-intent' … and the target's #29 prune deletes its
    /// LAST LIVE COPY — the armed-05 loss chain".
    ///
    /// `gc` lists arbitration as an acceptable drain only on the strength of
    /// "instructs this node to drop the marker AND re-pushes" — but the
    /// re-push is NOT atomic with the clear: `send_weak_veto_arbitration`
    /// returns the moment this node ACKs, and the caller's
    /// `repush_and_retry_reduced_completion` runs afterwards and can fail on a
    /// stream break, source crash, or a later refusal. Removing the entry
    /// therefore performed that conversion on demand.
    ///
    /// Suspension keeps both properties: the veto stops blocking the repair
    /// (the wedge W13 fixes), while the declaration survives regardless of
    /// whether the re-push lands. The entry's real drain is unchanged and
    /// remains the repair itself — [`Self::clear`] (Invariant TS-1) removes it
    /// when the record comes back LIVE.
    ///
    /// # Concurrency
    ///
    /// The cause check and the mutation happen under ONE shard write lock, so
    /// a concurrent strong-cause upgrade (a client delete landing between a
    /// caller's lookup and this call) can never be clobbered: whatever cause
    /// is present AT SUSPEND TIME decides. A strong cause (`ClientDelete`,
    /// `Dah`, or any unrecognized future byte — fail closed) is REFUSED; an
    /// absent tombstone reports [`WeakTombstoneClear::Absent`] so retried
    /// arbitration rounds are idempotent. Re-suspending an already-suspended
    /// marker is a no-op reported as [`WeakTombstoneClear::Suspended`].
    ///
    /// SAFETY ARGUMENT (why this cannot lift the veto of a record the CLIENT
    /// deleted ON THIS NODE): a weak cause is only ever produced by this
    /// node's own LOCAL rollback/reconcile markers, never by a client delete
    /// (cause separation at every producer — see [`Self::record`]);
    /// [`Self::record`]'s precedence never lets a weak cause REPLACE a strong
    /// one, while a later strong cause LWW-upgrades a weak one. So wherever a
    /// client delete was APPLIED TO A PRESENT RECORD on this node, the cause
    /// read here is `ClientDelete` and the suspension is refused.
    ///
    /// KNOWN LIMIT (the "W9 concern-A residual", `server::dispatch`): a client
    /// delete that finds the key ABSENT records NO tombstone at all
    /// (`Engine::delete_inner` returns `TxNotFound` before the log write; the
    /// replicated form swallows it). A node that pruned the key FIRST and
    /// received the authoritative delete SECOND therefore keeps a WEAK marker
    /// that is really the local proxy for another node's `ClientDelete`, and
    /// this call will suspend it. That predates W10/W13 — the marker was
    /// always weak on that path — but it is the residual that makes the
    /// arbitration's authority predicate load-bearing, and it is NOT closed
    /// here. See the P0-1 discussion on the handler.
    pub fn suspend_weak_veto(&self, key: &TxKey) -> WeakTombstoneClear {
        let mut shard = self.shards[self.shard_index(key)].write();
        match shard.get_mut(key) {
            None => WeakTombstoneClear::Absent,
            Some(v) if is_weak_cause(v.cause) => {
                v.veto_suspended = true;
                WeakTombstoneClear::Suspended
            }
            Some(_) => WeakTombstoneClear::RefusedStrongCause,
        }
    }

    /// Whether `key` carries a weak marker whose veto is currently suspended.
    /// Diagnostic companion to [`Self::suspend_weak_veto`]; the veto decision
    /// itself lives in [`Self::blocks_heal_apply`].
    pub fn weak_veto_suspended(&self, key: &TxKey) -> bool {
        self.shards[self.shard_index(key)]
            .read()
            .get(key)
            .is_some_and(|v| v.veto_suspended && is_weak_cause(v.cause))
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

/// W10 review P2-3 — is this stored cause byte one of the WEAK
/// (generation-overridable, locally-produced) markers? Unrecognized bytes are
/// NOT weak (fail-closed, matching [`TombstoneLog::blocks_heal_apply`]).
fn is_weak_cause(cause: u8) -> bool {
    cause == TombstoneCause::PruneReplace as u8 || cause == TombstoneCause::CompensatedCreate as u8
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

    /// W10 composition review P2-6 (RED→GREEN) — the armed-05 loss chain
    /// recurring past the retention horizon.
    ///
    /// A WEAK tombstone is the source's ONLY proof that a key missing from
    /// the manifest it ships is its own prune/rollback damage rather than
    /// deletion-intent. Pre-fix it expired on the block-height retention
    /// clock, so a repair slower than `retention_blocks` turned the source's
    /// omission into an undeclared one and the target's #29 prune deleted its
    /// LAST LIVE COPY. Weak causes must therefore survive the horizon while
    /// strong ones still expire on schedule, and the weak claim must still
    /// drain through every REPAIR path.
    #[test]
    fn gc_never_expires_weak_cause_omission_claims() {
        let log = TombstoneLog::new(PathBuf::from("/nonexistent/x.tombstones"), 0, 4, 10);
        let pruned = tk(1); // #29 prune damage — an omission claim
        let rolled_back = tk(2); // create rollback — an omission claim
        let client_deleted = tk(3); // real deletion-intent
        let dah = tk(4); // real deletion-intent
        log.record(&pruned, 6, 100, TombstoneCause::PruneReplace);
        log.record(&rolled_back, 0, 100, TombstoneCause::CompensatedCreate);
        log.record(&client_deleted, 6, 100, TombstoneCause::ClientDelete);
        log.record(&dah, 6, 100, TombstoneCause::Dah);

        // Every entry is past retention (100 + 10 <= 200) and nothing is
        // heal-protected: only the two DELETION-INTENT claims may expire.
        assert_eq!(
            log.gc(200, |_| false),
            2,
            "strong causes still expire on the retention clock",
        );
        assert!(log.lookup(&client_deleted).is_none());
        assert!(log.lookup(&dah).is_none());
        assert!(
            log.lookup(&pruned).is_some() && log.lookup(&rolled_back).is_some(),
            "a weak omission claim must outlive the retention horizon — its \
             lifetime is bounded by the REPAIR, not by block height",
        );
        let mut declared = log.weak_tombstone_keys();
        declared.sort_by_key(|k| k.txid);
        assert_eq!(
            declared,
            vec![pruned, rolled_back],
            "the source keeps DECLARING both omissions, so a migration target \
             retains its live copies instead of pruning them (armed-05)",
        );
        // Repeated passes do not erode it either.
        assert_eq!(log.gc(10_000, |_| false), 0);
        assert_eq!(log.weak_tombstone_keys().len(), 2);
        // The veto is still live for an at-or-behind image (anti-resurrection
        // is unchanged) and still admits a strictly-newer one.
        assert!(log.blocks_heal_apply(&pruned, 6));
        assert!(!log.blocks_heal_apply(&pruned, 7));

        // W13 — the ARBITRATION is NOT a drain. It suspends the veto so the
        // repair can land; the claim itself survives, because the re-push is
        // not atomic with the arbitration and a withdrawn declaration lets a
        // peer's #29 prune delete the key's last live copy.
        assert_eq!(
            log.suspend_weak_veto(&pruned),
            WeakTombstoneClear::Suspended,
            "the OP_MIGRATION_WEAK_VETO_ARBITRATE effect",
        );
        assert!(
            !log.blocks_heal_apply(&pruned, 6),
            "the veto stops blocking the repair…",
        );
        let mut still_declared = log.weak_tombstone_keys();
        still_declared.sort_by_key(|k| k.txid);
        let mut both = vec![pruned, rolled_back];
        both.sort_by_key(|k| k.txid);
        assert_eq!(
            still_declared, both,
            "…but the omission is STILL declared while the repair is in flight",
        );
        assert!(
            log.weak_veto_suspended(&pruned) && !log.weak_veto_suspended(&rolled_back),
            "and suspension is per-key",
        );

        // The REPAIR LANDING is the drain — Invariant TS-1's create-path clear,
        // for a suspended marker exactly as for an un-suspended one.
        assert!(log.clear(&pruned), "the repair landed for the pruned key");
        assert!(
            log.clear(&rolled_back),
            "the Invariant TS-1 re-create drain"
        );
        assert!(
            log.weak_tombstone_keys().is_empty() && log.is_empty(),
            "a repaired key leaves no residue",
        );

        // The boot reconcile is the third drain.
        let revived = tk(5);
        log.record(&revived, 0, 100, TombstoneCause::PruneReplace);
        assert_eq!(log.gc(200, |_| false), 0);
        assert_eq!(log.reconcile_against_live(|k| *k == revived), 1);
        assert!(log.weak_tombstone_keys().is_empty());
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

    /// W10 review P2-3 — the weak-key accelerator must stay in lockstep with
    /// the authoritative shard maps across EVERY mutation path, so a
    /// per-send `weak_tombstone_keys()` capture is both cheap and exact:
    /// record (weak in / strong-upgrade out), clear, gc,
    /// reconcile_against_live, and the on-disk load replay. Suspension is
    /// deliberately absent: it must NOT remove the key from the accelerator.
    #[test]
    fn weak_key_index_tracks_every_mutation_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.tombstones");
        let sorted = |mut v: Vec<TxKey>| {
            v.sort_by_key(|k| k.txid);
            v
        };

        let log = TombstoneLog::new(path.clone(), 0, 4, 10);
        // record: weak entries join, strong ones do not.
        log.record(&tk(1), 0, 100, TombstoneCause::PruneReplace);
        log.record(&tk(2), 0, 100, TombstoneCause::CompensatedCreate);
        log.record(&tk(3), 5, 100, TombstoneCause::ClientDelete);
        assert_eq!(
            sorted(log.weak_tombstone_keys()),
            sorted(vec![tk(1), tk(2)]),
            "weak causes are indexed, strong ones are not",
        );

        // record: a STRONG cause that LWW-upgrades a weak one must leave the
        // index (the strong claim governs; re-declaring it would be wrong).
        log.record(&tk(1), 7, 100, TombstoneCause::ClientDelete);
        assert_eq!(
            sorted(log.weak_tombstone_keys()),
            vec![tk(2)],
            "a strong upgrade removes the key from the weak set",
        );
        assert_eq!(
            log.lookup_cause(&tk(1)),
            Some(TombstoneCause::ClientDelete),
            "and the authority records the upgrade",
        );

        // W13 — suspend_weak_veto on a weak key lifts its veto but KEEPS the
        // entry (and therefore the declaration); on a strong key it is refused
        // and the index is unchanged.
        log.record(&tk(4), 0, 100, TombstoneCause::PruneReplace);
        assert_eq!(log.suspend_weak_veto(&tk(4)), WeakTombstoneClear::Suspended);
        assert!(!log.blocks_heal_apply(&tk(4), 0));
        assert_eq!(
            log.suspend_weak_veto(&tk(1)),
            WeakTombstoneClear::RefusedStrongCause
        );
        assert_eq!(
            sorted(log.weak_tombstone_keys()),
            sorted(vec![tk(2), tk(4)])
        );
        // The repair landing is what removes it.
        assert!(log.clear(&tk(4)));
        assert_eq!(sorted(log.weak_tombstone_keys()), vec![tk(2)]);

        // clear() drops a weak entry from the index too.
        log.record(&tk(5), 0, 100, TombstoneCause::CompensatedCreate);
        assert!(log.clear(&tk(5)));
        assert_eq!(sorted(log.weak_tombstone_keys()), vec![tk(2)]);

        // gc(): W10 composition review P2-6 — a weak entry SURVIVES the
        // retention horizon (its omission claim is bounded by the repair, not
        // by block height), so the accelerator keeps declaring it. This
        // assertion previously required the opposite; that was the defect.
        log.record(&tk(6), 0, 100, TombstoneCause::PruneReplace);
        assert_eq!(
            sorted(log.weak_tombstone_keys()),
            sorted(vec![tk(2), tk(6)])
        );
        assert_eq!(
            log.gc(200, |_| false),
            2,
            "the two past-retention STRONG entries (tk1, tk3) expire; no weak \
             entry may expire on the retention clock",
        );
        assert_eq!(
            sorted(log.weak_tombstone_keys()),
            sorted(vec![tk(2), tk(6)]),
            "the omission claim outlives the retention horizon",
        );
        // …and the accelerator still drains through the repair LANDING (W13:
        // the arbitration only suspends, so it is not a drain).
        assert_eq!(log.suspend_weak_veto(&tk(6)), WeakTombstoneClear::Suspended);
        assert_eq!(
            sorted(log.weak_tombstone_keys()),
            sorted(vec![tk(2), tk(6)]),
            "a suspended marker keeps declaring",
        );
        assert!(log.clear(&tk(6)));
        assert_eq!(sorted(log.weak_tombstone_keys()), vec![tk(2)]);

        // reconcile_against_live(): a weak entry whose key came back LIVE
        // leaves the index (Invariant TS-1).
        log.record(&tk(7), 0, 900, TombstoneCause::PruneReplace);
        assert_eq!(
            sorted(log.weak_tombstone_keys()),
            sorted(vec![tk(2), tk(7)]),
            "tk(2) is still declared — weak claims survive the GC above",
        );
        assert_eq!(
            log.reconcile_against_live(|k| *k == tk(7) || *k == tk(2)),
            2
        );
        assert!(log.weak_tombstone_keys().is_empty());

        // load(): the replay rebuilds the index from disk, and the LAST
        // entry for a re-deleted key decides membership.
        let fresh = TombstoneLog::new(path.clone(), 0, 4, 10_000);
        fresh.record(&tk(8), 0, 900, TombstoneCause::PruneReplace);
        fresh.record(&tk(9), 0, 900, TombstoneCause::PruneReplace);
        // tk(9) is later upgraded to a strong cause — the replay must honour
        // the newest entry and keep it OUT of the weak set.
        fresh.record(&tk(9), 4, 900, TombstoneCause::ClientDelete);
        fresh.persist(0, |_| false).unwrap();
        let reloaded = TombstoneLog::load(path, 0, 4, 10_000).unwrap();
        assert_eq!(
            sorted(reloaded.weak_tombstone_keys()),
            vec![tk(8)],
            "the load replay rebuilds the weak index with last-writer-wins",
        );
    }

    /// W10 FIX 2 / W13 — `suspend_weak_veto` lifts the RULE-DS veto of ONLY
    /// weak-cause tombstones (PruneReplace / CompensatedCreate): a strong
    /// cause (ClientDelete / Dah) is refused untouched, an absent key is
    /// idempotent, and the marker itself SURVIVES so it keeps declaring the
    /// omission to peers (W13 — removing it re-opened the last-live-copy prune
    /// chain, because the arbitration's re-push is not atomic with it).
    #[test]
    fn suspend_weak_veto_lifts_weak_vetoes_only_and_keeps_the_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.tombstones");
        let log = TombstoneLog::new(path.clone(), 0, 4, 100);
        log.record(&tk(1), 0, 500, TombstoneCause::PruneReplace);
        log.record(&tk(2), 0, 500, TombstoneCause::CompensatedCreate);
        log.record(&tk(3), 5, 500, TombstoneCause::ClientDelete);
        log.record(&tk(4), 5, 500, TombstoneCause::Dah);

        assert_eq!(log.suspend_weak_veto(&tk(1)), WeakTombstoneClear::Suspended);
        assert_eq!(log.suspend_weak_veto(&tk(2)), WeakTombstoneClear::Suspended);
        assert_eq!(
            log.suspend_weak_veto(&tk(3)),
            WeakTombstoneClear::RefusedStrongCause,
            "a ClientDelete veto is never arbitrable",
        );
        assert_eq!(
            log.suspend_weak_veto(&tk(4)),
            WeakTombstoneClear::RefusedStrongCause,
            "a Dah veto is never arbitrable",
        );
        assert_eq!(
            log.suspend_weak_veto(&tk(1)),
            WeakTombstoneClear::Suspended,
            "a retried suspension of an already-suspended key is idempotent",
        );
        assert_eq!(log.suspend_weak_veto(&tk(99)), WeakTombstoneClear::Absent);
        let mut declared = log.weak_tombstone_keys();
        declared.sort_by_key(|k| k.txid);
        let mut want = vec![tk(1), tk(2)];
        want.sort_by_key(|k| k.txid);
        assert_eq!(
            declared, want,
            "both suspended markers keep DECLARING their omission",
        );
        assert!(log.weak_veto_suspended(&tk(1)) && log.weak_veto_suspended(&tk(2)));
        assert!(
            !log.weak_veto_suspended(&tk(3)) && !log.weak_veto_suspended(&tk(4)),
            "a refused strong cause is never marked suspended",
        );
        // A FRESH delete re-arms the veto: new evidence, not a continuation.
        log.record(&tk(1), 0, 600, TombstoneCause::PruneReplace);
        assert!(
            !log.weak_veto_suspended(&tk(1)) && log.blocks_heal_apply(&tk(1), 0),
            "re-recording a weak marker re-arms its veto",
        );
        assert_eq!(log.suspend_weak_veto(&tk(1)), WeakTombstoneClear::Suspended);

        // The suspended weak markers no longer veto; the strong ones still do.
        assert!(!log.blocks_heal_apply(&tk(1), 0));
        assert!(!log.blocks_heal_apply(&tk(2), 0));
        assert!(log.blocks_heal_apply(&tk(3), 99));
        assert!(log.blocks_heal_apply(&tk(4), 5));

        // W13 durability contract — the MARKER is durable (it must keep
        // declaring across a restart), the SUSPENSION is not: a reload
        // re-arms the veto, which is the conservative direction. The source
        // re-drives a completion, gets a fresh refusal + ticket, and
        // re-arbitrates.
        log.persist(0, |_| false).unwrap();
        let reloaded = TombstoneLog::load(path, 0, 4, 100).unwrap();
        for k in [tk(1), tk(2), tk(3), tk(4)] {
            assert!(
                reloaded.lookup(&k).is_some(),
                "every marker survives the reload, suspended or not",
            );
        }
        assert!(
            !reloaded.weak_veto_suspended(&tk(1)) && !reloaded.weak_veto_suspended(&tk(2)),
            "suspension is RAM-only",
        );
        assert!(
            reloaded.blocks_heal_apply(&tk(1), 0),
            "so the PruneReplace veto re-arms on restart",
        );
        // `tk(2)` is a CompensatedCreate at generation 0, whose rule blocks
        // only a STRICTLY-BEHIND image — vacuous at 0, as its own doc concedes.
        // Drive it at a generation where the rule has content instead.
        log.record(&tk(9), 7, 500, TombstoneCause::CompensatedCreate);
        assert!(
            log.blocks_heal_apply(&tk(9), 6),
            "strictly behind is dropped"
        );
        assert_eq!(log.suspend_weak_veto(&tk(9)), WeakTombstoneClear::Suspended);
        assert!(
            !log.blocks_heal_apply(&tk(9), 6),
            "suspension lifts the CompensatedCreate veto too",
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

        // W9 P1-1 — PruneReplace: generation-gated like Dah (block a source
        // at-or-behind the frozen generation, admit a strictly-newer one).
        // The prune's anti-resurrection purpose — dropping the stale extra
        // copy the authoritative manifest omitted — survives for at-or-behind
        // images, while a strictly-newer live copy heals back in instead of
        // being vetoed forever by a node that never client-deleted the key.
        let prune = tk(3);
        log.record(&prune, 5, 900, TombstoneCause::PruneReplace);
        assert!(
            log.blocks_heal_apply(&prune, 4),
            "PruneReplace drops a source behind N (the reverse-heal window it closes)",
        );
        assert!(log.blocks_heal_apply(&prune, 5), "PruneReplace drops at N");
        assert!(
            !log.blocks_heal_apply(&prune, 6),
            "PruneReplace ADMITS a strictly-newer source — a live copy the \
             cluster still acks must not be vetoed by a local prune marker",
        );

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

        // W9 P1-1 — PruneReplace mirrors the CompensatedCreate rules exactly.
        // ClientDelete then PruneReplace → ClientDelete stands.
        let k5 = tk(5);
        log.record(&k5, 7, 900, TombstoneCause::ClientDelete);
        log.record(&k5, 9, 901, TombstoneCause::PruneReplace);
        assert_eq!(log.lookup_cause(&k5), Some(TombstoneCause::ClientDelete));
        assert_eq!(log.lookup(&k5), Some((7, 900)));
        assert!(
            log.blocks_heal_apply(&k5, 99),
            "a local prune marker must never downgrade the client delete's \
             unconditional veto",
        );

        // Dah then PruneReplace → Dah stands (defense in depth, as above).
        let k6 = tk(6);
        log.record(&k6, 7, 900, TombstoneCause::Dah);
        log.record(&k6, 9, 901, TombstoneCause::PruneReplace);
        assert_eq!(log.lookup_cause(&k6), Some(TombstoneCause::Dah));

        // PruneReplace then ClientDelete → the client delete REPLACES it.
        let k7 = tk(7);
        log.record(&k7, 2, 900, TombstoneCause::PruneReplace);
        log.record(&k7, 5, 905, TombstoneCause::ClientDelete);
        assert_eq!(log.lookup_cause(&k7), Some(TombstoneCause::ClientDelete));
        assert!(log.blocks_heal_apply(&k7, 99));

        // The two WEAK causes never displace each other either — the earlier
        // claim (and its generation) stands, so neither rollback marker can
        // weaken or re-scope the other's veto window.
        let k8 = tk(8);
        log.record(&k8, 3, 900, TombstoneCause::CompensatedCreate);
        log.record(&k8, 9, 901, TombstoneCause::PruneReplace);
        assert_eq!(
            log.lookup_cause(&k8),
            Some(TombstoneCause::CompensatedCreate)
        );
        assert_eq!(log.lookup(&k8), Some((3, 900)));
        let k9 = tk(9);
        log.record(&k9, 3, 900, TombstoneCause::PruneReplace);
        log.record(&k9, 9, 901, TombstoneCause::CompensatedCreate);
        assert_eq!(log.lookup_cause(&k9), Some(TombstoneCause::PruneReplace));
        assert_eq!(log.lookup(&k9), Some((3, 900)));
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
