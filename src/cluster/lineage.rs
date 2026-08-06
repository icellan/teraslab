//! P1 stage 3 — per-shard Full/Subset copy lineage (design §4.3).
//!
//! A node's lineage for shard `s` is a **self-observed**, persisted claim
//! about the completeness of its local copy:
//!
//! ```text
//! Lineage = Full { regime: u64 } | Subset
//! ```
//!
//! `Full { regime }` means "this node held a complete copy of `s` as of
//! the committed regime `regime`, and has remained in `s`'s holder set
//! (I2) at every commit it installed since". `Subset` means the copy is —
//! or cannot be proven not to be — incomplete. Promotion eligibility
//! (stage 4) and the DAH/retention held-copy sweep (§4.3) both read this
//! state; **every default is `Subset`** (fail-closed, §4.3):
//!
//! * file absent ⇒ all `Subset`;
//! * file unreadable / integrity mismatch ⇒ all `Subset` (+ ERROR log);
//! * partially decodable ⇒ `Subset` for every shard not positively
//!   decoded (with the checksummed envelope this collapses to
//!   all-`Subset` on any decode fault — nothing is ever defaulted);
//! * identity mismatch (`data_epoch` OR `node_id`) ⇒ all `Subset`, WARN
//!   naming which identity mismatched. The on-disk DATA baseline is NOT
//!   invalidated (§4.3 "Node replacement"): re-earning `Full` goes
//!   through catch-up / migration over the intact baseline, never a
//!   forced full resync.
//!
//! ## Identity binding
//!
//! The persisted stamps are bound to `(data_epoch, node_id)`:
//!
//! * `data_epoch` is a restore-stamped identity: 16 random bytes written
//!   by `backup::restore::restore()` (which also deletes this lineage
//!   file outright). At normal boot the stamp is read from
//!   [`data_epoch_path`]; an absent stamp is generated AND persisted on
//!   first clustered boot. That makes a **cloned data directory share the
//!   epoch** — which is exactly why `node_id` is ALSO in the binding: a
//!   clone brought up under a different `node_id` degrades to all-`Subset`
//!   via the `node_id` mismatch, and a clone under the SAME id is already
//!   rejected upstream as a duplicate NodeId. `data_epoch` MUST NOT
//!   change on device add/resize/reformat (it is deliberately NOT derived
//!   from device geometry) — only `restore()` stamps a fresh one.
//!
//! ## Durability
//!
//! Transitions are batched: one durable write (temp + rename + fsync +
//! parent-dir fsync, the `persist_inbound_state` pattern) per transition
//! batch — never per-shard fsyncs (I2/I13's whole-node-failover note: a
//! commit may re-stamp ~1024 shards in one batch inside the
//! `commit_apply` mutex).

use crate::cluster::migration::AtomicShardBitmap;
use crate::cluster::shards::{NUM_SHARDS, NodeId};
use crate::cluster::topology::{decode_envelope, encode_envelope};
use parking_lot::Mutex;
use std::path::{Path, PathBuf};

/// Envelope magic of the lineage sidecar file (same
/// `[magic][version:u16][len:u32][body][sha256]` framing as the topology
/// state file, §4.1 "Persistence").
pub const LINEAGE_STATE_ENVELOPE_MAGIC: [u8; 4] = *b"TSLN";

/// Magic prefix of the `data_epoch` stamp file.
pub const DATA_EPOCH_MAGIC: [u8; 4] = *b"TSDE";

/// Length of the restore-stamped data-epoch identity, in bytes.
pub const DATA_EPOCH_LEN: usize = 16;

/// The lineage sidecar path derived from the resolved cluster state path
/// (sibling of the `.inbound` / `.outbound` migration-fence files).
pub fn lineage_state_path(cluster_state_path: &Path) -> PathBuf {
    let mut s = cluster_state_path.as_os_str().to_os_string();
    s.push(".lineage");
    PathBuf::from(s)
}

/// The data-epoch stamp path derived from the resolved cluster state path.
/// `backup::restore::restore()` writes a FRESH stamp here; normal boot
/// reads (or first-creates) it.
pub fn data_epoch_path(cluster_state_path: &Path) -> PathBuf {
    let mut s = cluster_state_path.as_os_str().to_os_string();
    s.push(".data-epoch");
    PathBuf::from(s)
}

/// Generate a fresh random data epoch.
///
/// # Errors
/// Propagates the OS entropy failure ([`getrandom::Error`] mapped to
/// `std::io::Error`) — callers fail closed (non-persistent, all-`Subset`
/// lineage) rather than proceed with a guessable identity.
fn fresh_data_epoch() -> std::io::Result<[u8; DATA_EPOCH_LEN]> {
    let mut epoch = [0u8; DATA_EPOCH_LEN];
    getrandom::getrandom(&mut epoch)
        .map_err(|e| std::io::Error::other(format!("entropy source failed: {e}")))?;
    Ok(epoch)
}

/// Write `epoch` durably to `path` (`[magic:4][epoch:16]`, temp + rename +
/// fsync + parent-dir fsync).
///
/// # Errors
/// Any I/O failure of the write / rename / fsync chain.
fn write_data_epoch(path: &Path, epoch: &[u8; DATA_EPOCH_LEN]) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut bytes = Vec::with_capacity(4 + DATA_EPOCH_LEN);
    bytes.extend_from_slice(&DATA_EPOCH_MAGIC);
    bytes.extend_from_slice(epoch);
    let tmp = path.with_extension("data-epoch.tmp");
    std::fs::write(&tmp, &bytes)?;
    let f = std::fs::File::open(&tmp)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    crate::fsutil::fsync_parent_dir(path)?;
    Ok(())
}

/// Read the data-epoch stamp at `path`; generate AND persist a fresh one
/// when the file is absent (first clustered boot) or structurally invalid
/// (wrong magic / wrong length — WARN logged; the stamps it invalidated
/// were already unusable because the identity they were bound to is
/// unreadable, which is the fail-closed all-`Subset` direction).
///
/// # Errors
/// A read error other than `NotFound`, an entropy failure, or a failed
/// durable write of a freshly generated stamp. Callers treat an error as
/// "no trustworthy identity": lineage runs non-persistent, all-`Subset`.
pub fn load_or_create_data_epoch(path: &Path) -> std::io::Result<[u8; DATA_EPOCH_LEN]> {
    match std::fs::read(path) {
        Ok(bytes) => {
            if bytes.len() == 4 + DATA_EPOCH_LEN && bytes[..4] == DATA_EPOCH_MAGIC {
                let mut epoch = [0u8; DATA_EPOCH_LEN];
                epoch.copy_from_slice(&bytes[4..]);
                return Ok(epoch);
            }
            tracing::warn!(
                path = %path.display(),
                len = bytes.len(),
                "lineage: data-epoch stamp is structurally invalid — stamping a fresh epoch \
                 (all lineage degrades to Subset via the identity mismatch)",
            );
            let epoch = fresh_data_epoch()?;
            write_data_epoch(path, &epoch)?;
            Ok(epoch)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let epoch = fresh_data_epoch()?;
            write_data_epoch(path, &epoch)?;
            Ok(epoch)
        }
        Err(e) => Err(e),
    }
}

/// §4.3 (restore) — stamp a FRESH data epoch unconditionally. Called by
/// `backup::restore::restore()` after it deletes the lineage and
/// inbound/outbound cluster state files.
///
/// # Errors
/// Entropy or I/O failure of the durable stamp write.
pub fn stamp_fresh_data_epoch(path: &Path) -> std::io::Result<[u8; DATA_EPOCH_LEN]> {
    let epoch = fresh_data_epoch()?;
    write_data_epoch(path, &epoch)?;
    Ok(epoch)
}

/// One shard's self-observed copy lineage (§4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lineage {
    /// The local copy was complete as of committed regime `regime` and the
    /// node has remained in the shard's holder set at every installed
    /// commit since (I2).
    Full {
        /// The committed regime the stamp is current at.
        regime: u64,
    },
    /// The local copy is (or cannot be proven not to be) incomplete. The
    /// fail-closed default for every shard.
    Subset,
}

/// Outcome of one per-commit lineage transition batch
/// ([`LineageStore::apply_commit_transitions`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitTransitionOutcome {
    /// Shards degraded `Full → Subset` (holder-set exit / regime advance
    /// without membership — I2).
    pub degraded: usize,
    /// Shards whose `Full` stamp was refreshed to the new committed regime
    /// (I2 stamp refresh; includes the I13ii promotion re-stamp).
    pub refreshed: usize,
    /// Whether the batched durable write succeeded (`true` also when no
    /// stamp changed and no write was needed). `false` fails the carrying
    /// commit closed.
    pub persisted: bool,
}

/// The per-shard lineage store: in-memory state + atomic `Full` bitmap
/// shadow (the `fenced_shards` precedent — the sweep hot path reads one
/// atomic word, never a lock) + the persisted sidecar file.
pub struct LineageStore {
    /// Sidecar path; `None` = non-persistent (tests / no cluster state
    /// path / unrecoverable identity), which keeps every default
    /// fail-closed and makes every persist a successful no-op.
    path: Option<PathBuf>,
    /// The restore-stamped identity this node booted with.
    data_epoch: [u8; DATA_EPOCH_LEN],
    /// The claiming node.
    node_id: NodeId,
    /// Authoritative per-shard state: `Some(regime)` = `Full{regime}`,
    /// `None` = `Subset`.
    state: Mutex<Vec<Option<u64>>>,
    /// Lock-free shadow of "is `Full`" per shard for the sweep hot path
    /// and stage 4's serving-gate reads. Refreshed on every transition.
    full_bits: AtomicShardBitmap,
    /// P1 stage 4 (§4.3 catch-up trigger) — "stream-origin" evidence: the
    /// shard's ENTIRE local content arrived via the replica stream (the
    /// first tracked apply found the shard empty and no data-motion
    /// trigger has fired since), so the baseline under the streamed ops is
    /// trivially complete. In-memory only (cleared at boot — fail-closed:
    /// a rebooted non-empty `Subset` shard cannot re-earn through this
    /// path and goes through catch-up/heal/migration completion instead).
    /// Cleared for a shard by EVERY `Subset` degrade (`mark_subset`, the
    /// per-commit holder-exit/skipped-term degrade) — a fence or holder
    /// exit invalidates the "nothing but the stream touched this copy"
    /// claim.
    stream_origin: AtomicShardBitmap,
    /// P1 stage 4 — shards with stream-origin evidence touched by a
    /// tracked replica apply since the last flush. Drained (debounced) by
    /// the coordinator event loop, which re-validates every condition at
    /// flush time and stamps `Full` in ONE batched durable write.
    stream_candidates: AtomicShardBitmap,
}

impl LineageStore {
    /// Open the store, applying the §4.3 fail-closed default matrix (see
    /// the module docs). Never errors: every fault degrades to
    /// all-`Subset` with a log, because a node must be able to boot (and
    /// re-earn `Full` over its intact baseline) past any lineage-file
    /// fault.
    pub fn open(path: Option<PathBuf>, data_epoch: [u8; DATA_EPOCH_LEN], node_id: NodeId) -> Self {
        let mut state: Vec<Option<u64>> = vec![None; NUM_SHARDS];
        if let Some(ref p) = path {
            match std::fs::read(p) {
                Ok(bytes) => match decode_lineage_file(&bytes) {
                    Ok((file_epoch, file_node, entries)) => {
                        if file_epoch != data_epoch || file_node != node_id {
                            // §4.3 identity binding: name WHICH identity
                            // mismatched; do NOT invalidate the baseline.
                            tracing::warn!(
                                path = %p.display(),
                                data_epoch_mismatch = file_epoch != data_epoch,
                                node_id_mismatch = file_node != node_id,
                                stored_node_id = file_node.0,
                                claiming_node_id = node_id.0,
                                "lineage: identity mismatch — degrading ALL shards to Subset \
                                 (fail-closed); the data baseline is NOT invalidated: Full is \
                                 re-earned through catch-up/migration over the intact baseline",
                            );
                        } else {
                            for (shard, regime) in entries {
                                state[shard as usize] = Some(regime);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            path = %p.display(),
                            err = %e,
                            "lineage: state file unreadable/corrupt — ALL shards Subset \
                             (fail-closed §4.3); Full must be re-earned",
                        );
                    }
                },
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Absent ⇒ all Subset — the documented safe default.
                }
                Err(e) => {
                    tracing::error!(
                        path = %p.display(),
                        err = %e,
                        "lineage: state file read failed — ALL shards Subset (fail-closed §4.3)",
                    );
                }
            }
        }
        let full_bits = AtomicShardBitmap::new();
        for (shard, stamp) in state.iter().enumerate() {
            if stamp.is_some() {
                full_bits.set(shard as u16);
            }
        }
        Self {
            path,
            data_epoch,
            node_id,
            state: Mutex::new(state),
            full_bits,
            stream_origin: AtomicShardBitmap::new(),
            stream_candidates: AtomicShardBitmap::new(),
        }
    }

    /// P1 stage 4 — note one tracked replica-stream apply touching
    /// `shard` (the receiver observed the shard's pre-apply record count).
    ///
    /// Grants stream-origin evidence when the shard was EMPTY before the
    /// apply (everything it now holds arrived via the stream) or already
    /// carried the evidence; a non-empty shard without prior evidence is
    /// ignored (its baseline provenance is unknown — fail-closed). A
    /// granted shard is queued as a `Full`-stamp candidate for the
    /// debounced flush. The CALLER is responsible for never noting a shard
    /// with an inbound/heal fence up (dual-write during a fill would
    /// otherwise claim stream-origin for a copy that is part baseline).
    pub fn note_stream_apply(&self, shard: u16, was_empty: bool) {
        if shard as usize >= NUM_SHARDS {
            return;
        }
        if was_empty || self.stream_origin.test(shard) {
            self.stream_origin.set(shard);
            self.stream_candidates.set(shard);
        }
    }

    /// P1 stage 4 — whether `shard` currently carries stream-origin
    /// evidence (see [`Self::note_stream_apply`]).
    pub fn stream_origin(&self, shard: u16) -> bool {
        (shard as usize) < NUM_SHARDS && self.stream_origin.test(shard)
    }

    /// P1 stage 4 — drain the pending `Full`-stamp candidates (ascending
    /// shard order), clearing the candidate bits. The flush re-validates
    /// every condition (origin still granted, no fence, holder-set
    /// membership) before stamping; a shard dropped here is re-queued by
    /// its next tracked apply.
    pub fn take_stream_candidates(&self) -> Vec<u16> {
        let mut out = Vec::new();
        for shard in 0..NUM_SHARDS as u16 {
            if self.stream_candidates.test(shard) {
                self.stream_candidates.clear(shard);
                out.push(shard);
            }
        }
        out
    }

    /// P1 stage 4 — cheap "anything pending?" probe so the event loop
    /// skips the drain entirely in the common idle case.
    pub fn has_stream_candidates(&self) -> bool {
        (0..NUM_SHARDS as u16).any(|s| self.stream_candidates.test(s))
    }

    /// Revoke stream-origin evidence (and any pending candidate) for
    /// `shards` — every `Subset` degrade path calls this so the
    /// "nothing but the stream touched this copy" claim dies with the
    /// stamp.
    fn revoke_stream_origin(&self, shard: u16) {
        self.stream_origin.clear(shard);
        self.stream_candidates.clear(shard);
    }

    /// The lineage of `shard`.
    pub fn lineage(&self, shard: u16) -> Lineage {
        match self.state.lock().get(shard as usize).copied().flatten() {
            Some(regime) => Lineage::Full { regime },
            None => Lineage::Subset,
        }
    }

    /// Lock-free `Full` check for the sweep hot path and stage 4's
    /// serving gate. Out-of-range shards answer `false` (Subset).
    pub fn is_full(&self, shard: u16) -> bool {
        (shard as usize) < NUM_SHARDS && self.full_bits.test(shard)
    }

    /// Number of shards currently stamped `Full`.
    pub fn full_count(&self) -> usize {
        self.state.lock().iter().filter(|s| s.is_some()).count()
    }

    /// Degrade every shard in `shards` to `Subset` in ONE batch with ONE
    /// durable write (§4.3 data-motion triggers: inbound migration begins,
    /// full-shard resync begins, heal fence raised, boot re-assertion of a
    /// persisted inbound fence).
    ///
    /// Best-effort persistence with the SAFE polarity: the in-memory state
    /// and the atomic shadow always degrade immediately (the sweep fence
    /// takes effect at once); a failed durable write is an ERROR log —
    /// a crash before re-persist can at worst resurrect a stale `Full`
    /// stamp, which boot re-derivation and the persisted inbound/heal
    /// fences re-degrade (see `RunningCluster::restore_inbound_state`).
    pub fn mark_subset(&self, shards: &[u16], reason: &str) {
        let mut state = self.state.lock();
        let mut changed = false;
        for &shard in shards {
            if (shard as usize) < NUM_SHARDS {
                // P1 stage 4 — a data-motion degrade invalidates the
                // stream-origin claim even when the stamp was already
                // Subset (e.g. a fence raised on a never-Full shard).
                self.revoke_stream_origin(shard);
            }
            if let Some(slot) = state.get_mut(shard as usize)
                && slot.is_some()
            {
                *slot = None;
                self.full_bits.clear(shard);
                changed = true;
            }
        }
        if changed {
            tracing::info!(
                count = shards.len(),
                reason,
                "lineage: degraded shard(s) to Subset"
            );
            if !self.persist_locked(&state) {
                tracing::error!(
                    reason,
                    "lineage: durable write of Subset transition failed — in-memory state is \
                     degraded (fail-closed) but a crash may resurrect a stale Full stamp until \
                     the next successful persist",
                );
            }
        }
    }

    /// Stamp `shard` `Full { regime }` in ONE durable write (§4.3
    /// completion triggers: inbound migration completes with verified
    /// manifest, heal completes, resync completes — the caller has already
    /// verified the completion AND holder-set membership).
    ///
    /// Returns `false` (and logs ERROR, leaving the shard `Subset`) when
    /// the durable write fails: a `Full` claim that would not survive a
    /// crash must not be observable (fail-closed — the opposite polarity
    /// from [`Self::mark_subset`]).
    pub fn mark_full(&self, shard: u16, regime: u64, reason: &str) -> bool {
        self.mark_full_many(&[(shard, regime)], reason)
    }

    /// Batched [`Self::mark_full`]: stamp every `(shard, regime)` in ONE
    /// durable write (the bulk migration completion handshake completes
    /// thousands of shards in one frame — per-shard fsyncs are forbidden,
    /// I2's batched-write note). All-or-nothing: a failed durable write
    /// rolls every stamp in the batch back to `Subset`-as-before and
    /// returns `false`.
    pub fn mark_full_many(&self, stamps: &[(u16, u64)], reason: &str) -> bool {
        let valid: Vec<(u16, u64)> = stamps
            .iter()
            .copied()
            .filter(|&(shard, _)| (shard as usize) < NUM_SHARDS)
            .collect();
        if valid.is_empty() {
            return false;
        }
        let mut state = self.state.lock();
        let snapshot: Vec<(u16, Option<u64>)> = valid
            .iter()
            .map(|&(shard, _)| (shard, state[shard as usize]))
            .collect();
        for &(shard, regime) in &valid {
            state[shard as usize] = Some(regime);
        }
        if self.persist_locked(&state) {
            for &(shard, _) in &valid {
                self.full_bits.set(shard);
            }
            tracing::info!(count = valid.len(), reason, "lineage: stamped Full");
            true
        } else {
            for (shard, prior) in snapshot {
                state[shard as usize] = prior;
            }
            tracing::error!(
                count = valid.len(),
                reason,
                "lineage: durable write of Full stamp(s) failed — shard(s) stay Subset \
                 (fail-closed)",
            );
            false
        }
    }

    /// I2 / I13(ii) — the per-commit transition batch, run inside the
    /// `commit_apply` section by the topology commit-install hook:
    ///
    /// * `Full` and still in the shard's holder set (`in_holder_set`,
    ///   computed by the caller as `target_assignment(s) ∪
    ///   {effective_assignment(s).master}` — I2's union) ⇒ refresh to
    ///   `Full { regime_of(s) }`. When the installed commit names SELF
    ///   master of `s`, this same refresh IS the I13(ii) promotion
    ///   re-stamp.
    /// * `Full` and NOT in the holder set (holder exit, or the regime
    ///   advanced while the node is not in the new holder set) ⇒
    ///   `Subset`.
    /// * `Subset` stays `Subset` (re-earned only via the §4.3 completion
    ///   triggers / I13 completions).
    ///
    /// One batched durable write for the whole commit. `persisted: false`
    /// (durable-write failure with at least one stamp change) must fail
    /// the carrying commit closed — the caller (the install hook) returns
    /// `false` and the commit is not applied.
    pub fn apply_commit_transitions(
        &self,
        in_holder_set: &dyn Fn(u16) -> bool,
        regime_of: &dyn Fn(u16) -> u64,
    ) -> CommitTransitionOutcome {
        let mut state = self.state.lock();
        let snapshot: Vec<Option<u64>> = state.clone();
        let mut degraded = 0usize;
        let mut refreshed = 0usize;
        let mut changed = false;
        for shard in 0..NUM_SHARDS as u16 {
            let Some(stamp) = state[shard as usize] else {
                continue;
            };
            if in_holder_set(shard) {
                let new_regime = regime_of(shard);
                if stamp != new_regime {
                    state[shard as usize] = Some(new_regime);
                    refreshed += 1;
                    changed = true;
                }
            } else {
                state[shard as usize] = None;
                degraded += 1;
                changed = true;
                // P1 stage 4 — a holder-exit (or skipped-term) degrade
                // kills the stream-origin claim: the node missed the
                // fan-out for however long it was outside the holder set.
                self.revoke_stream_origin(shard);
            }
        }
        let persisted = if changed {
            if self.persist_locked(&state) {
                // Sync the shadow only after the stamps are durable.
                for shard in 0..NUM_SHARDS as u16 {
                    if state[shard as usize].is_some() {
                        self.full_bits.set(shard);
                    } else {
                        self.full_bits.clear(shard);
                    }
                }
                true
            } else {
                // Fail closed: roll the in-memory state back so a refused
                // commit leaves lineage exactly as it was.
                *state = snapshot;
                false
            }
        } else {
            true
        };
        CommitTransitionOutcome {
            degraded,
            refreshed,
            persisted,
        }
    }

    /// I13(i) + boot fail-closed re-derivation, run once at clustered boot
    /// against the LOADED committed state:
    ///
    /// * `committed_master_is_self(s)` ⇒ `Full { regime_of(s) }`
    ///   REGARDLESS of the stored stamp (I13i: the node's own baseline +
    ///   intact replayed redo tail IS the shard's authoritative copy —
    ///   reaching clustered startup at all requires boot recovery over the
    ///   node's own un-reclaimed redo to have succeeded; a corrupt or
    ///   reclaimed-needed-range redo fails boot upstream).
    /// * otherwise a stored `Full { r }` survives ONLY when the node is in
    ///   the shard's holder set AND `r == regime_of(s)` — a reboot cannot
    ///   prove holder-set continuity across the down window, so any regime
    ///   discrepancy degrades (fail-closed).
    ///
    /// One durable write; a persist failure here is logged (the masters'
    /// stamps are re-derived on every boot, so nothing is lost that the
    /// next boot cannot re-derive).
    pub fn boot_rederive(
        &self,
        committed_master_is_self: &dyn Fn(u16) -> bool,
        in_holder_set: &dyn Fn(u16) -> bool,
        regime_of: &dyn Fn(u16) -> u64,
    ) {
        let mut state = self.state.lock();
        let mut changed = false;
        let mut rederived = 0usize;
        let mut degraded = 0usize;
        for shard in 0..NUM_SHARDS as u16 {
            let idx = shard as usize;
            if committed_master_is_self(shard) {
                let regime = regime_of(shard);
                if state[idx] != Some(regime) {
                    state[idx] = Some(regime);
                    rederived += 1;
                    changed = true;
                }
                self.full_bits.set(shard);
            } else if let Some(stamp) = state[idx] {
                if in_holder_set(shard) && stamp == regime_of(shard) {
                    self.full_bits.set(shard);
                } else {
                    state[idx] = None;
                    self.full_bits.clear(shard);
                    degraded += 1;
                    changed = true;
                }
            }
        }
        if changed {
            tracing::info!(
                rederived,
                degraded,
                "lineage: boot re-derivation (I13i masters re-stamped; stale non-master \
                 stamps degraded)",
            );
            if !self.persist_locked(&state) {
                tracing::error!(
                    "lineage: durable write of boot re-derivation failed — continuing with \
                     the in-memory derivation (re-derived on every boot)",
                );
            }
        }
    }

    /// Encode + durably write the current state. `true` on success (and
    /// always for a pathless store).
    fn persist_locked(&self, state: &[Option<u64>]) -> bool {
        let Some(ref path) = self.path else {
            return true;
        };
        let bytes = encode_lineage_file(&self.data_epoch, self.node_id, state);
        let tmp = path.with_extension("lineage.tmp");
        let result = (|| -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            std::io::Write::write_all(&mut f, &bytes)?;
            f.sync_all()?;
            std::fs::rename(&tmp, path)?;
            // The rename's directory entry is not durable until the parent
            // dir is fsync'd (the persist_inbound_state R5 lesson).
            crate::fsutil::fsync_parent_dir(path)?;
            Ok(())
        })();
        match result {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(err = %e, path = %path.display(), "lineage: persist failed");
                false
            }
        }
    }
}

/// Encode the lineage file: the shared topology envelope
/// (`[magic][version:u16][len:u32][body][sha256]`) around
/// `[data_epoch:16][node_id:8][count:u16][(shard:2, regime:8)...]` with
/// entries in ascending shard order (only `Full` shards are present — an
/// absent shard IS `Subset`).
fn encode_lineage_file(
    data_epoch: &[u8; DATA_EPOCH_LEN],
    node_id: NodeId,
    state: &[Option<u64>],
) -> Vec<u8> {
    let count = state.iter().filter(|s| s.is_some()).count();
    let mut body = Vec::with_capacity(DATA_EPOCH_LEN + 8 + 2 + count * 10);
    body.extend_from_slice(data_epoch);
    body.extend_from_slice(&node_id.0.to_le_bytes());
    body.extend_from_slice(&(count as u16).to_le_bytes());
    for (shard, stamp) in state.iter().enumerate() {
        if let Some(regime) = stamp {
            body.extend_from_slice(&(shard as u16).to_le_bytes());
            body.extend_from_slice(&regime.to_le_bytes());
        }
    }
    encode_envelope(LINEAGE_STATE_ENVELOPE_MAGIC, &body)
}

/// The decoded lineage file body: `(data_epoch, node_id, Full entries)`.
type DecodedLineageFile = ([u8; DATA_EPOCH_LEN], NodeId, Vec<(u16, u64)>);

/// Decode a lineage file. Every fault is a hard error (the caller maps it
/// to all-`Subset`): bad envelope/checksum, truncated body, out-of-range
/// shard, non-ascending/duplicate entries, `count > NUM_SHARDS`, or
/// trailing bytes.
fn decode_lineage_file(bytes: &[u8]) -> Result<DecodedLineageFile, LineageDecodeError> {
    let (body, end) = decode_envelope(bytes, 0, LINEAGE_STATE_ENVELOPE_MAGIC)
        .map_err(LineageDecodeError::Envelope)?;
    if end != bytes.len() {
        return Err(LineageDecodeError::TrailingBytes);
    }
    if body.len() < DATA_EPOCH_LEN + 8 + 2 {
        return Err(LineageDecodeError::Truncated);
    }
    let mut data_epoch = [0u8; DATA_EPOCH_LEN];
    data_epoch.copy_from_slice(&body[..DATA_EPOCH_LEN]);
    let mut pos = DATA_EPOCH_LEN;
    let node_id = NodeId(u64::from_le_bytes(
        body[pos..pos + 8]
            .try_into()
            .map_err(|_| LineageDecodeError::Truncated)?,
    ));
    pos += 8;
    let count = u16::from_le_bytes(
        body[pos..pos + 2]
            .try_into()
            .map_err(|_| LineageDecodeError::Truncated)?,
    ) as usize;
    pos += 2;
    if count > NUM_SHARDS {
        return Err(LineageDecodeError::NonCanonical);
    }
    if body.len() != pos + count * 10 {
        return Err(LineageDecodeError::Truncated);
    }
    let mut entries = Vec::with_capacity(count);
    let mut prev: Option<u16> = None;
    for _ in 0..count {
        let shard = u16::from_le_bytes(
            body[pos..pos + 2]
                .try_into()
                .map_err(|_| LineageDecodeError::Truncated)?,
        );
        pos += 2;
        let regime = u64::from_le_bytes(
            body[pos..pos + 8]
                .try_into()
                .map_err(|_| LineageDecodeError::Truncated)?,
        );
        pos += 8;
        if shard as usize >= NUM_SHARDS {
            return Err(LineageDecodeError::NonCanonical);
        }
        if let Some(p) = prev
            && shard <= p
        {
            return Err(LineageDecodeError::NonCanonical);
        }
        prev = Some(shard);
        entries.push((shard, regime));
    }
    Ok((data_epoch, node_id, entries))
}

/// Decode faults of the lineage sidecar (all mapped to fail-closed
/// all-`Subset` by [`LineageStore::open`]).
#[derive(Debug, thiserror::Error)]
enum LineageDecodeError {
    /// The shared envelope failed (magic/version/length/checksum).
    #[error("lineage envelope: {0}")]
    Envelope(crate::cluster::topology::RegimeDecodeError),
    /// Bytes after the envelope's checksum.
    #[error("trailing bytes after lineage envelope")]
    TrailingBytes,
    /// The body is shorter than its own structure requires.
    #[error("lineage body truncated")]
    Truncated,
    /// Entry list violates canonical form (unsorted / duplicate /
    /// out-of-range shard / count over `NUM_SHARDS`).
    #[error("non-canonical lineage entry list")]
    NonCanonical,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "teraslab-lineage-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id(),
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    const EPOCH_A: [u8; DATA_EPOCH_LEN] = [7u8; DATA_EPOCH_LEN];
    const EPOCH_B: [u8; DATA_EPOCH_LEN] = [9u8; DATA_EPOCH_LEN];

    /// §4.3 fail-closed default: an ABSENT lineage file is all-Subset.
    #[test]
    fn i2_absent_file_is_all_subset() {
        let dir = tmpdir("absent");
        let store = LineageStore::open(Some(dir.join("lineage")), EPOCH_A, NodeId(1));
        assert_eq!(store.full_count(), 0);
        assert_eq!(store.lineage(0), Lineage::Subset);
        assert!(!store.is_full(0));
    }

    /// §4.3 fail-closed default: a corrupt (checksum-broken) file is
    /// all-Subset AND the store still boots (never an error).
    #[test]
    fn i2_corrupt_file_is_all_subset() {
        let dir = tmpdir("corrupt");
        let path = dir.join("lineage");
        let store = LineageStore::open(Some(path.clone()), EPOCH_A, NodeId(1));
        assert!(store.mark_full(3, 5, "test"));
        drop(store);
        // Flip one byte inside the envelope body — the sha256 check must
        // reject the whole file.
        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        let store = LineageStore::open(Some(path), EPOCH_A, NodeId(1));
        assert_eq!(
            store.full_count(),
            0,
            "integrity mismatch must be all-Subset"
        );
    }

    /// §4.3 fail-closed default: a truncated (partially decodable) file
    /// yields Subset for every shard — no partial adoption.
    #[test]
    fn i2_truncated_file_is_all_subset() {
        let dir = tmpdir("truncated");
        let path = dir.join("lineage");
        let store = LineageStore::open(Some(path.clone()), EPOCH_A, NodeId(1));
        assert!(store.mark_full(3, 5, "test"));
        assert!(store.mark_full(9, 6, "test"));
        drop(store);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 7]).unwrap();
        let store = LineageStore::open(Some(path), EPOCH_A, NodeId(1));
        assert_eq!(store.full_count(), 0, "no shard may be positively decoded");
    }

    /// §4.3 identity binding: a data_epoch mismatch degrades ALL shards to
    /// Subset but does NOT delete/overwrite the file at open (the baseline
    /// — and the file — are left for diagnosis; only the next transition
    /// rewrites it under the new identity).
    #[test]
    fn i2_data_epoch_mismatch_is_all_subset_and_baseline_untouched() {
        let dir = tmpdir("epoch-mismatch");
        let path = dir.join("lineage");
        let store = LineageStore::open(Some(path.clone()), EPOCH_A, NodeId(1));
        assert!(store.mark_full(3, 5, "test"));
        drop(store);
        let before = std::fs::read(&path).unwrap();
        let store = LineageStore::open(Some(path.clone()), EPOCH_B, NodeId(1));
        assert_eq!(store.full_count(), 0, "epoch mismatch must be all-Subset");
        let after = std::fs::read(&path).unwrap();
        assert_eq!(before, after, "open() must not rewrite the mismatched file");
    }

    /// §4.3 identity binding: the node_id half — a cloned data dir under a
    /// different NodeId degrades to all-Subset.
    #[test]
    fn i2_node_id_mismatch_is_all_subset() {
        let dir = tmpdir("node-mismatch");
        let path = dir.join("lineage");
        let store = LineageStore::open(Some(path.clone()), EPOCH_A, NodeId(1));
        assert!(store.mark_full(3, 5, "test"));
        drop(store);
        let store = LineageStore::open(Some(path), EPOCH_A, NodeId(2));
        assert_eq!(store.full_count(), 0, "node_id mismatch must be all-Subset");
    }

    /// Round trip: matching identity restores the exact stamps.
    #[test]
    fn i2_roundtrip_restores_full_stamps() {
        let dir = tmpdir("roundtrip");
        let path = dir.join("lineage");
        let store = LineageStore::open(Some(path.clone()), EPOCH_A, NodeId(1));
        assert!(store.mark_full(3, 5, "test"));
        assert!(store.mark_full(4095, 9, "test"));
        drop(store);
        let store = LineageStore::open(Some(path), EPOCH_A, NodeId(1));
        assert_eq!(store.lineage(3), Lineage::Full { regime: 5 });
        assert_eq!(store.lineage(4095), Lineage::Full { regime: 9 });
        assert!(store.is_full(3) && store.is_full(4095));
        assert_eq!(store.full_count(), 2);
    }

    /// I2 — a commit whose holder set no longer contains the node degrades
    /// its Full stamp; a shard where it remains a holder is refreshed to
    /// the new regime (I13ii's promotion re-stamp is the same refresh).
    #[test]
    fn i2_commit_transitions_degrade_on_holder_exit_and_refresh_holders() {
        let dir = tmpdir("commit-transitions");
        let store = LineageStore::open(Some(dir.join("lineage")), EPOCH_A, NodeId(1));
        assert!(store.mark_full(10, 4, "test"));
        assert!(store.mark_full(11, 4, "test"));
        let outcome = store.apply_commit_transitions(&|shard| shard == 10, &|_| 7);
        assert!(outcome.persisted);
        assert_eq!(outcome.degraded, 1, "shard 11 exited the holder set");
        assert_eq!(outcome.refreshed, 1, "shard 10 refreshed to the new regime");
        assert_eq!(store.lineage(10), Lineage::Full { regime: 7 });
        assert_eq!(store.lineage(11), Lineage::Subset);
        assert!(
            !store.is_full(11),
            "sweep shadow must degrade with the stamp"
        );
    }

    /// I2 — Subset never becomes Full through a commit transition (Full is
    /// re-earned only via the §4.3 completion triggers).
    #[test]
    fn i2_commit_transitions_never_promote_subset() {
        let store = LineageStore::open(None, EPOCH_A, NodeId(1));
        let outcome = store.apply_commit_transitions(&|_| true, &|_| 3);
        assert!(outcome.persisted);
        assert_eq!(outcome.refreshed + outcome.degraded, 0);
        assert_eq!(store.full_count(), 0);
    }

    /// I2 fail-closed: when the batched durable write fails, the commit
    /// transition reports `persisted: false` and the in-memory state rolls
    /// back — the carrying commit must not apply over a lineage state that
    /// would not survive a crash.
    #[test]
    fn i13_commit_transition_persist_failure_rolls_back_and_reports() {
        let dir = tmpdir("persist-fail");
        let path = dir.join("lineage");
        let store = LineageStore::open(Some(path.clone()), EPOCH_A, NodeId(1));
        assert!(store.mark_full(10, 4, "test"));
        // Make the persist fail: replace the parent dir path with a FILE so
        // the tmp-file create fails.
        drop(store);
        let blocked = dir.join("blocked");
        std::fs::create_dir_all(&blocked).unwrap();
        let blocked_path = blocked.join("lineage");
        let store = LineageStore::open(Some(blocked_path), EPOCH_A, NodeId(1));
        assert!(store.mark_full(10, 4, "seed"));
        std::fs::remove_dir_all(&blocked).unwrap();
        std::fs::write(&blocked, b"not a dir").unwrap();
        let outcome = store.apply_commit_transitions(&|_| true, &|_| 9);
        assert!(
            !outcome.persisted,
            "a failed durable write must be reported"
        );
        assert_eq!(
            store.lineage(10),
            Lineage::Full { regime: 4 },
            "the in-memory state must roll back to the pre-transition stamps",
        );
    }

    /// I13(i) — boot re-derivation stamps committed-master shards Full at
    /// the current regime REGARDLESS of the stored stamp, degrades a
    /// non-master stamp whose regime lags, and keeps a non-master holder
    /// stamp at the current regime.
    #[test]
    fn i13_boot_rederivation_regardless_of_stamp() {
        let dir = tmpdir("boot-rederive");
        let path = dir.join("lineage");
        let store = LineageStore::open(Some(path.clone()), EPOCH_A, NodeId(1));
        assert!(store.mark_full(1, 2, "stale master stamp"));
        assert!(store.mark_full(2, 2, "stale replica stamp"));
        assert!(store.mark_full(3, 6, "current replica stamp"));
        drop(store);
        let store = LineageStore::open(Some(path), EPOCH_A, NodeId(1));
        // Committed state: self masters shards 0 and 1; holder (replica) of
        // 2 and 3; current regime is 6 everywhere.
        store.boot_rederive(&|s| s == 0 || s == 1, &|s| (2..=3).contains(&s), &|_| 6);
        assert_eq!(
            store.lineage(0),
            Lineage::Full { regime: 6 },
            "I13i: a committed master re-derives Full even with NO stored stamp",
        );
        assert_eq!(
            store.lineage(1),
            Lineage::Full { regime: 6 },
            "I13i: a committed master re-derives Full REGARDLESS of a stale stamp",
        );
        assert_eq!(
            store.lineage(2),
            Lineage::Subset,
            "a non-master stamp at a lagging regime cannot prove holder continuity",
        );
        assert_eq!(
            store.lineage(3),
            Lineage::Full { regime: 6 },
            "a non-master holder stamp at the current regime survives boot",
        );
    }

    /// Data-epoch stamp: absent ⇒ generated + persisted (a second load
    /// returns the SAME epoch — a cloned dir would share it, which is why
    /// node_id is also in the binding); restore stamping replaces it.
    #[test]
    fn i2_data_epoch_created_once_and_restamped_by_restore() {
        let dir = tmpdir("epoch");
        let path = dir.join("cluster.data-epoch");
        let first = load_or_create_data_epoch(&path).unwrap();
        let second = load_or_create_data_epoch(&path).unwrap();
        assert_eq!(first, second, "a normal boot must observe a stable epoch");
        let restamped = stamp_fresh_data_epoch(&path).unwrap();
        assert_ne!(first, restamped, "restore must mint a FRESH identity");
        assert_eq!(load_or_create_data_epoch(&path).unwrap(), restamped);
    }
}
