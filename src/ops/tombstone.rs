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

/// Why a record was deleted. Diagnostic only in Phase 2a (carried on-disk and
/// preserved across compaction, but not consulted by any query).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TombstoneCause {
    /// Deleted by the DAH sweep (`due_guard` set).
    Dah = 0,
    /// Deleted by a direct client `OP_DELETE_BATCH` (`due_guard == None`).
    ClientDelete = 1,
    /// Deleted as part of a migration replace-duplicate reconcile.
    PruneReplace = 2,
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
#[derive(Clone, Copy, Debug)]
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
    pub fn record(&self, key: &TxKey, generation: u32, height: u32, cause: TombstoneCause) {
        let value = TombValue {
            generation,
            height,
            cause: cause as u8,
        };
        // Two disjoint locks, never held simultaneously here: shard write drops
        // before the file lock is taken, so `persist` (file-then-shard) can
        // never invert against this path.
        self.shards[self.shard_index(key)]
            .write()
            .insert(*key, value);
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

    /// The recorded `(generation, height)` for `key`, if a tombstone exists.
    pub fn lookup(&self, key: &TxKey) -> Option<(u32, u32)> {
        self.shards[self.shard_index(key)]
            .read()
            .get(key)
            .map(|v| (v.generation, v.height))
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
    /// in-RAM index. Flags the durable file for compaction if anything was
    /// dropped. Returns the number removed.
    pub fn gc(&self, last_durable_height: u32) -> usize {
        let retention = self.retention_blocks;
        let mut dropped = 0usize;
        for shard in &self.shards {
            let mut g = shard.write();
            let before = g.len();
            g.retain(|_, v| !expired(v.height, retention, last_durable_height));
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

    /// Make the tombstone set durable at a checkpoint: GC past the retention
    /// horizon, then either COMPACT (atomic rewrite from the in-RAM index, when
    /// GC / reconcile dropped entries) or APPEND the buffered tail. Fsyncs
    /// before returning.
    ///
    /// # Errors
    ///
    /// Returns a [`std::io::Error`] on any filesystem failure; on an append
    /// failure the buffered tail is restored so the next checkpoint retries it.
    pub fn persist(&self, last_durable_height: u32) -> std::io::Result<()> {
        self.gc(last_durable_height);

        let needs_compaction = { self.file.lock().needs_compaction };
        if needs_compaction {
            let all = self.snapshot_all();
            self.write_all_atomic(&all)?;
            let mut fs = self.file.lock();
            fs.pending.clear();
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
        assert_eq!(log.gc(109), 0);
        // floor 110: tk(1) expires (100+10<=110), tk(2) retained (200+10>110).
        assert_eq!(log.gc(110), 1);
        assert!(log.lookup(&tk(1)).is_none());
        assert!(log.lookup(&tk(2)).is_some());
    }

    #[test]
    fn persist_then_load_roundtrips_via_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.tombstones");
        let log = TombstoneLog::new(path.clone(), 0, 4, 100);
        log.record(&tk(1), 3, 500, TombstoneCause::ClientDelete);
        log.record(&tk(2), 7, 600, TombstoneCause::Dah);
        log.persist(0).unwrap();

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
        log.persist(0).unwrap(); // both durable (append)

        // Advance the floor past tk(1)'s horizon and persist: GC drops tk(1) and
        // the file is COMPACTED, so a fresh reload sees only tk(2).
        log.persist(110).unwrap();
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
