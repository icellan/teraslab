//! The online-backup job: the `Idle → Pinning → Fencing → Snapshotting →
//! Copying → CatchUp → Finalizing → Done|Failed` state machine.
//!
//! [`run_backup`] pins the segment lifecycle (RAII, so an abort/panic releases
//! it), samples the fence `F` under the checkpoint visibility guard, attaches a
//! bounded redo tail tee per store, snapshots the index, copies every used
//! segment through the throttled torn-read-safe [`crate::backup::copier`],
//! catches up as the frontier advances, and — in a final stall under the guard —
//! samples `T`, captures each store's allocator header from memory, and drains
//! the teed tail. It then assembles per-store images + fabricated redo files and
//! writes the [`Manifest`] LAST, so its appearance marks the backup complete.
//!
//! ## Image layout
//!
//! Each store's image is the concatenation of its ranges in *ascending
//! device offset*: the allocator header block (offset 0) first, then each copied
//! segment. Ranges are buffered in RAM during the run and written to
//! `store.{s}.img` at the end — acceptable for v1's bounded device sizes and far
//! simpler than a sparse writer. Restore pwrites each range back at its
//! `device_offset`.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use crate::backup::copier::{self, TokenBucket};
use crate::backup::manifest::{
    BlobEntry, Geometry, MANIFEST_FILE, MANIFEST_VERSION, Manifest, RangeEntry, StoreManifest,
    sha256_hex,
};
use crate::backup::{BackupError, BackupParams, BackupProgress, BackupState};
use crate::config::ServerConfig;
use crate::ops::engine::Engine;
use crate::redo::{RedoLog, RedoTee, redo_op_type_is_marker};
use crate::segment_allocator::SegmentBackupView;

/// RAII pin over the segment lifecycle + blob-GC for a backup.
///
/// [`Self::acquire`] pins the engine's segment lifecycle (freezes reclaim /
/// defrag / reuse so the used-segment set is stable and the append frontier
/// stays monotone) and pauses the blob-GC sweep. [`Drop`] releases both, so the
/// pins are lifted on every early return, error, and panic.
pub struct BackupPinGuard<'a> {
    engine: &'a Engine,
    blob_pause: &'a AtomicBool,
}

impl<'a> BackupPinGuard<'a> {
    /// Pin the segment lifecycle and pause blob GC. Released on drop.
    pub fn acquire(engine: &'a Engine, blob_pause: &'a AtomicBool) -> Self {
        engine.pin_segment_lifecycle();
        blob_pause.store(true, Ordering::Release);
        Self { engine, blob_pause }
    }
}

impl Drop for BackupPinGuard<'_> {
    fn drop(&mut self) {
        self.engine.unpin_segment_lifecycle();
        self.blob_pause.store(false, Ordering::Release);
    }
}

/// A bounded, order-preserving collector for one store's teed redo tail.
///
/// The tee closure is invoked under the store's redo mutex (so calls are
/// serialized and in commit order). It drops `RecoveryProgress`/`Checkpoint`
/// markers and refuses to grow past `cap`, flipping `overflow` instead of
/// blocking the appender — a full tail aborts the backup, never a client write.
struct TailCollector {
    frames: Mutex<Vec<Vec<u8>>>,
    bytes: AtomicUsize,
    cap: usize,
    overflow: AtomicBool,
}

impl TailCollector {
    fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            frames: Mutex::new(Vec::new()),
            bytes: AtomicUsize::new(0),
            cap,
            overflow: AtomicBool::new(false),
        })
    }

    /// Build the tee closure bound to this collector.
    fn tee(self: &Arc<Self>) -> RedoTee {
        let me = Arc::clone(self);
        Box::new(move |frame: &[u8], _seq: u64, op_type: u8| {
            // Markers must never enter the fabricated tail — they would falsely
            // fence the restore replay against a different snapshot point.
            if redo_op_type_is_marker(op_type) {
                return;
            }
            if me.overflow.load(Ordering::Relaxed) {
                return;
            }
            let n = frame.len();
            // Serialized under the redo mutex, so a plain load/store is race-free
            // for this collector.
            let mut frames = me.frames.lock();
            let cur = me.bytes.load(Ordering::Relaxed);
            if cur.saturating_add(n) > me.cap {
                me.overflow.store(true, Ordering::Relaxed);
                return;
            }
            frames.push(frame.to_vec());
            me.bytes.store(cur + n, Ordering::Relaxed);
        })
    }

    fn overflowed(&self) -> bool {
        self.overflow.load(Ordering::Relaxed)
    }

    /// Drain the collected frames (called at finalize after the tee is detached).
    fn take_frames(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut *self.frames.lock())
    }
}

/// A copied range plus its bytes, buffered for image assembly.
struct RangeBytes {
    entry: RangeEntry,
    bytes: Vec<u8>,
}

/// Run a full online backup into `target_dir`. See the module docs for the flow.
///
/// On success the manifest is durable and `progress.state == Done`. On any
/// error `progress.state == Failed` with the error string, the segment-lifecycle
/// pin is released (RAII), and the target directory is left WITHOUT a manifest
/// (restore refuses it).
#[allow(clippy::too_many_arguments)]
pub fn run_backup(
    engine: &Engine,
    blob_pause: &AtomicBool,
    config: &ServerConfig,
    params: &BackupParams,
    target_dir: &Path,
    cancel: &AtomicBool,
    progress: &Mutex<BackupProgress>,
) -> Result<Manifest, BackupError> {
    match run_backup_impl(
        engine, blob_pause, config, params, target_dir, cancel, progress,
    ) {
        Ok(manifest) => Ok(manifest),
        Err(e) => {
            let mut p = progress.lock();
            p.state = BackupState::Failed;
            p.error = Some(e.to_string());
            Err(e)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_backup_impl(
    engine: &Engine,
    blob_pause: &AtomicBool,
    config: &ServerConfig,
    params: &BackupParams,
    target_dir: &Path,
    cancel: &AtomicBool,
    progress: &Mutex<BackupProgress>,
) -> Result<Manifest, BackupError> {
    let store_count = engine.store_count();
    let align = config.device_alignment;

    // 1. Pre-flight: every store must be a segment store with enough headroom.
    set_state(progress, BackupState::Pinning);
    std::fs::create_dir_all(target_dir).map_err(BackupError::Io)?;
    let mut segments_total = 0u32;
    for s in 0..store_count as u8 {
        let view = engine.backup_view_for(s).ok_or_else(|| not_segment(s))?;
        let have = view.virgin_headroom_segments();
        if have < params.min_headroom_segments {
            return Err(BackupError::InsufficientHeadroom {
                store: s,
                have,
                need: params.min_headroom_segments,
            });
        }
        // Segments 0..=open plus one header per store.
        segments_total = segments_total.saturating_add(view.open_segment + 2);
    }
    {
        let mut p = progress.lock();
        p.segments_total = segments_total;
    }

    // 2. Pin the segment lifecycle + blob GC (RAII — released on all exits).
    let _guard = BackupPinGuard::acquire(engine, blob_pause);

    // 3. Fence + attach tees under the visibility guard.
    set_state(progress, BackupState::Fencing);
    let collectors: Vec<Arc<TailCollector>> = (0..store_count)
        .map(|_| TailCollector::new(params.tee_buffer_max_bytes))
        .collect();
    let redo_logs = engine.redo_logs();
    let fence = {
        let guard = engine.acquire_checkpoint_visibility_guard();
        let fence = engine
            .redo_log()
            .map(|l| l.lock().current_sequence().saturating_sub(1))
            .unwrap_or(0);
        for (i, log) in redo_logs.iter().enumerate() {
            if let Some(c) = collectors.get(i) {
                log.lock().attach_tee(c.tee());
            }
        }
        drop(guard);
        fence
    };
    {
        let mut p = progress.lock();
        p.fence = Some(fence);
    }

    // 4. Backup-owned index snapshot.
    set_state(progress, BackupState::Snapshotting);
    engine
        .snapshot_index(&target_dir.join("teraslab-index.snap"))
        .map_err(|e| BackupError::Index(e.to_string()))?;

    // 5. Copy sealed + growing segments.
    set_state(progress, BackupState::Copying);
    let mut copied_through: Vec<Option<u32>> = vec![None; store_count];
    let mut store_ranges: Vec<Vec<RangeBytes>> = (0..store_count).map(|_| Vec::new()).collect();
    let mut throttles: Vec<TokenBucket> = (0..store_count)
        .map(|_| TokenBucket::new(params.throttle_bytes_per_sec))
        .collect();
    let mut bytes_copied = 0u64;
    let mut segments_copied = 0u32;

    for s in 0..store_count as u8 {
        let view = engine.backup_view_for(s).ok_or_else(|| not_segment(s))?;
        let end = view.open_segment;
        copy_segments(
            engine,
            s,
            &view,
            0,
            end,
            align,
            &mut throttles[s as usize],
            &mut store_ranges[s as usize],
            cancel,
            progress,
            &mut bytes_copied,
            &mut segments_copied,
        )?;
        copied_through[s as usize] = Some(end);
    }

    // 6. Bounded catch-up as the frontier advances (per store).
    set_state(progress, BackupState::CatchUp);
    for s in 0..store_count as u8 {
        let mut round = 0u32;
        loop {
            let view = engine.backup_view_for(s).ok_or_else(|| not_segment(s))?;
            let new_open = view.open_segment;
            let base = copied_through[s as usize].unwrap_or(0);
            if new_open.saturating_sub(base) <= params.stall_copy_max_segments {
                break;
            }
            if round >= params.max_catchup_rounds {
                return Err(BackupError::CatchupExceeded { rounds: round });
            }
            copy_segments(
                engine,
                s,
                &view,
                base + 1,
                new_open,
                align,
                &mut throttles[s as usize],
                &mut store_ranges[s as usize],
                cancel,
                progress,
                &mut bytes_copied,
                &mut segments_copied,
            )?;
            copied_through[s as usize] = Some(new_open);
            round += 1;
            let mut p = progress.lock();
            p.catchup_round = round;
        }
    }

    // A tail that overran its bound aborts the backup (never a client write).
    for c in &collectors {
        if c.overflowed() {
            return Err(BackupError::TeeOverflow);
        }
    }

    // 7. Final stall: sample T, copy the last segments, capture headers, drain
    //    the tail — all under the visibility guard, no backup-dir I/O.
    set_state(progress, BackupState::Finalizing);
    let mut headers: Vec<Vec<u8>> = Vec::with_capacity(store_count);
    // The residual copy runs UNDER the visibility guard (all mutations blocked),
    // so it must never throttle-sleep and stall the engine. It is bounded by
    // `stall_copy_max_segments`, so an unthrottled read here is brief by design.
    let mut unthrottled = TokenBucket::new(0);
    let (tail_end, tail_frames) = {
        let guard = engine.acquire_checkpoint_visibility_guard();
        let tail_end = engine
            .redo_log()
            .map(|l| l.lock().current_sequence().saturating_sub(1))
            .unwrap_or(fence);
        for s in 0..store_count as u8 {
            let view = engine.backup_view_for(s).ok_or_else(|| not_segment(s))?;
            let base = copied_through[s as usize].unwrap_or(0);
            if view.open_segment > base {
                copy_segments(
                    engine,
                    s,
                    &view,
                    base + 1,
                    view.open_segment,
                    align,
                    &mut unthrottled,
                    &mut store_ranges[s as usize],
                    cancel,
                    progress,
                    &mut bytes_copied,
                    &mut segments_copied,
                )?;
                copied_through[s as usize] = Some(view.open_segment);
            }
            let header = engine
                .serialize_store_header(s)
                .ok_or_else(|| not_segment(s))?;
            headers.push(header[..].to_vec());
        }
        for log in redo_logs.iter() {
            log.lock().detach_tee();
        }
        let tail_frames: Vec<Vec<Vec<u8>>> = collectors.iter().map(|c| c.take_frames()).collect();
        drop(guard);
        (tail_end, tail_frames)
    };
    {
        let mut p = progress.lock();
        p.tail_end = Some(tail_end);
    }

    // 8. Assemble per-store images + fabricated redo files (outside the guard).
    let mut stores: Vec<StoreManifest> = Vec::with_capacity(store_count);
    for s in 0..store_count as u8 {
        let view = engine.backup_view_for(s).ok_or_else(|| not_segment(s))?;

        // Header range at offset 0, then the copied segments; sort by offset so
        // the image is [header, segments ascending].
        let mut slots = std::mem::take(&mut store_ranges[s as usize]);
        let header_bytes = std::mem::take(&mut headers[s as usize]);
        let header_entry = RangeEntry {
            device_offset: 0,
            len: header_bytes.len() as u64,
            sha256: sha256_hex(&header_bytes),
        };
        slots.push(RangeBytes {
            entry: header_entry,
            bytes: header_bytes,
        });
        slots.sort_by_key(|r| r.entry.device_offset);

        let mut image: Vec<u8> = Vec::new();
        let mut ranges: Vec<RangeEntry> = Vec::with_capacity(slots.len());
        for slot in &slots {
            image.extend_from_slice(&slot.bytes);
            ranges.push(slot.entry.clone());
        }
        let image_file = format!("store.{s}.img");
        std::fs::write(target_dir.join(&image_file), &image).map_err(BackupError::Io)?;
        let image_sha256 = sha256_hex(&image);

        let region =
            RedoLog::build_backup_redo_region(align, fence, tail_end, &tail_frames[s as usize]);
        let redo_file = format!("redo.{s}");
        std::fs::write(target_dir.join(&redo_file), &region).map_err(BackupError::Io)?;
        let redo_sha256 = sha256_hex(&region);

        stores.push(StoreManifest {
            store: s,
            device_id_hex: hex_bytes(&view.device_id),
            segment_size: view.segment_size,
            segment_count: view.segment_count,
            image_file,
            image_sha256,
            ranges,
            redo_file,
            redo_sha256,
        });
    }

    // 9. Blob store: copy the tree after T (skip *.tmp, tolerate ENOENT).
    let blobs = copy_blobstore(&config.blobstore_path, &target_dir.join("blobstore"))?;

    // 10. Manifest LAST.
    let index_snapshot_file = "teraslab-index.snap".to_string();
    let index_snapshot_sha256 = {
        let bytes =
            std::fs::read(target_dir.join(&index_snapshot_file)).map_err(BackupError::Io)?;
        sha256_hex(&bytes)
    };
    let manifest = Manifest {
        manifest_version: MANIFEST_VERSION,
        teraslab_version: env!("CARGO_PKG_VERSION").to_string(),
        fence,
        tail_end,
        last_durable_height: engine.last_durable_height(),
        seg_header_version: 2,
        redo_header_version: 2,
        geometry: Geometry {
            device_count: config.device_paths.len(),
            device_size: config.device_size,
            alignment: config.device_alignment,
            device_split: config.device_split,
            store_count: engine.store_count(),
        },
        stores,
        index_snapshot_file,
        index_snapshot_sha256,
        blobs,
    };
    manifest.write(target_dir)?;
    {
        let mut p = progress.lock();
        p.state = BackupState::Done;
        p.manifest_path = Some(target_dir.join(MANIFEST_FILE).display().to_string());
    }
    Ok(manifest)
}

/// Copy segments `start..=end` of store `device_id` into `out`, throttled and
/// torn-read-safe, updating progress and honouring `cancel`.
#[allow(clippy::too_many_arguments)]
fn copy_segments(
    engine: &Engine,
    device_id: u8,
    view: &SegmentBackupView,
    start: u32,
    end: u32,
    align: usize,
    throttle: &mut TokenBucket,
    out: &mut Vec<RangeBytes>,
    cancel: &AtomicBool,
    progress: &Mutex<BackupProgress>,
    bytes_copied: &mut u64,
    segments_copied: &mut u32,
) -> Result<(), BackupError> {
    for k in start..=end {
        if cancel.load(Ordering::Relaxed) {
            return Err(BackupError::Aborted);
        }
        let seg_offset = view.segment_offset(k);
        let seg_size = view.segment_size;
        let mut bytes: Vec<u8> = Vec::with_capacity(seg_size as usize);
        // `copy_range` insists on a running whole-image hasher; we compute the
        // image hash over the assembled concatenation later, so this one is a
        // scratch that is discarded.
        let mut scratch = Sha256::new();
        let entry = copier::copy_range(
            engine,
            device_id,
            seg_offset,
            seg_size,
            align,
            throttle,
            &mut bytes,
            &mut scratch,
        )?;
        *bytes_copied += seg_size;
        *segments_copied += 1;
        {
            let mut p = progress.lock();
            p.bytes_copied = *bytes_copied;
            p.segments_copied = *segments_copied;
        }
        out.push(RangeBytes { entry, bytes });
    }
    Ok(())
}

/// Copy `src_root` into `dst_root`, skipping `*.tmp` and tolerating files that
/// vanish mid-copy. Returns a manifest entry per copied file. An absent
/// `src_root` yields an empty list.
fn copy_blobstore(src_root: &Path, dst_root: &Path) -> Result<Vec<BlobEntry>, BackupError> {
    if !src_root.is_dir() {
        return Ok(Vec::new());
    }
    let mut blobs: Vec<BlobEntry> = Vec::new();
    let mut stack = vec![src_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(BackupError::Io(e)),
        };
        for entry in rd {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            let ft = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if ft.is_dir() {
                stack.push(path);
                continue;
            }
            if !ft.is_file() {
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                continue;
            }
            let rel = match path.strip_prefix(src_root) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let rel_str = rel.to_string_lossy().replace('\\', "/");
            let bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(BackupError::Io(e)),
            };
            let dst = dst_root.join(rel);
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent).map_err(BackupError::Io)?;
            }
            std::fs::write(&dst, &bytes).map_err(BackupError::Io)?;
            blobs.push(BlobEntry {
                rel_path: rel_str,
                sha256: sha256_hex(&bytes),
                len: bytes.len() as u64,
            });
        }
    }
    Ok(blobs)
}

fn set_state(progress: &Mutex<BackupProgress>, state: BackupState) {
    progress.lock().state = state;
}

fn not_segment(store: u8) -> BackupError {
    BackupError::UnsupportedConfig(format!(
        "store {store} is not a segment store; v1 backup requires the segment engine"
    ))
}

/// Lowercase-hex encode a byte slice (no hashing).
fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::manifest::Manifest;
    use crate::device::{BlockDevice, MemoryDevice};
    use crate::index::{DahIndex, Index, UnminedIndex};
    use crate::locks::StripedLocks;
    use crate::ops::create::CreateRequest;
    use crate::ops::engine::Engine;
    use crate::segment_allocator::SegmentAllocator;
    use tempfile::TempDir;

    const ALIGN: usize = 4096;
    const SEG: u64 = 1024 * 1024; // 1 MiB segments

    /// A segment engine over an in-memory device of `size` bytes.
    fn seg_engine(size: u64) -> Engine {
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(size, ALIGN).unwrap());
        let seg = SegmentAllocator::new(dev.clone(), SEG).unwrap();
        Engine::new(
            dev,
            Index::new(256).unwrap(),
            seg,
            StripedLocks::new(256),
            DahIndex::new(),
            UnminedIndex::new(),
        )
    }

    /// An in-place (slot) engine — NOT a segment store.
    fn slot_engine(size: u64) -> Engine {
        use crate::allocator::SlotAllocator;
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(size, ALIGN).unwrap());
        let slot = SlotAllocator::new(dev.clone()).unwrap();
        Engine::new(
            dev,
            Index::new(256).unwrap(),
            slot,
            StripedLocks::new(256),
            DahIndex::new(),
            UnminedIndex::new(),
        )
    }

    fn make_record(engine: &Engine, n: u64) {
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&n.to_le_bytes());
        let hashes = [[0x22u8; 32]];
        engine
            .create(&CreateRequest {
                tx_id: txid,
                tx_version: 1,
                locktime: 0,
                fee: 0,
                size_in_bytes: 100,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                utxo_hashes: &hashes,
                inputs: None,
                outputs: None,
                inpoints: None,
                is_external: false,
                created_at: 0,
                block_height: 1,
                mined_block_infos: &[],
                frozen: false,
                conflicting: false,
                locked: false,
                external_ref: None,
                parent_txids: &[],
            })
            .unwrap();
    }

    fn test_config(dir: &Path, size: u64) -> ServerConfig {
        let mut cfg = ServerConfig::default();
        cfg.device_paths = vec![dir.join("data.dat")];
        cfg.device_size = size;
        cfg.device_alignment = ALIGN;
        cfg.device_split = 1;
        // A non-existent blob dir → empty blobs.
        cfg.blobstore_path = dir.join("no-blobstore");
        cfg
    }

    fn low_headroom_params() -> BackupParams {
        BackupParams {
            throttle_bytes_per_sec: 0,
            min_headroom_segments: 1,
            ..BackupParams::default()
        }
    }

    #[test]
    fn pin_guard_unpins_on_drop() {
        let engine = seg_engine(8 * 1024 * 1024);
        let blob_pause = AtomicBool::new(false);
        assert!(!engine.is_segment_lifecycle_pinned());
        {
            let _g = BackupPinGuard::acquire(&engine, &blob_pause);
            assert!(engine.is_segment_lifecycle_pinned());
            assert!(blob_pause.load(Ordering::Acquire));
        }
        assert!(!engine.is_segment_lifecycle_pinned());
        assert!(!blob_pause.load(Ordering::Acquire));

        // Panic-safety: the pin must be released even on an unwinding panic.
        let engine = Arc::new(seg_engine(8 * 1024 * 1024));
        let blob_pause = Arc::new(AtomicBool::new(false));
        let e2 = engine.clone();
        let b2 = blob_pause.clone();
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = BackupPinGuard::acquire(&e2, &b2);
            assert!(e2.is_segment_lifecycle_pinned());
            panic!("boom");
        }));
        assert!(res.is_err(), "the closure must have panicked");
        assert!(
            !engine.is_segment_lifecycle_pinned(),
            "pin must be released after a panic"
        );
        assert!(!blob_pause.load(Ordering::Acquire));
    }

    #[test]
    fn run_backup_produces_manifest_and_images() {
        let dir = TempDir::new().unwrap();
        let size = 8 * 1024 * 1024;
        let engine = seg_engine(size);
        for i in 1..=4u64 {
            make_record(&engine, i);
        }
        let config = test_config(dir.path(), size);
        let params = low_headroom_params();
        let target = dir.path().join("bk");
        let cancel = AtomicBool::new(false);
        let progress = Mutex::new(BackupProgress::default());
        let blob_pause = AtomicBool::new(false);

        let manifest = run_backup(
            &engine,
            &blob_pause,
            &config,
            &params,
            &target,
            &cancel,
            &progress,
        )
        .expect("backup should succeed");

        assert!(target.join(MANIFEST_FILE).exists(), "manifest must exist");
        let read = Manifest::read(&target).expect("manifest reads back");
        assert_eq!(read, manifest);
        read.verify_checksums(&target).expect("checksums verify");
        assert_eq!(read.stores.len(), 1);
        assert!(
            read.stores[0].ranges.len() >= 2,
            "expected at least header + one segment range, got {}",
            read.stores[0].ranges.len()
        );
        // First range is the header at device offset 0.
        assert_eq!(read.stores[0].ranges[0].device_offset, 0);
        assert_eq!(progress.lock().state, BackupState::Done);
        // Every segment referenced in the image must be present in the image
        // file: image length equals the sum of range lengths.
        let image = std::fs::read(target.join(&read.stores[0].image_file)).unwrap();
        let total: u64 = read.stores[0].ranges.iter().map(|r| r.len).sum();
        assert_eq!(image.len() as u64, total);
        // The estimated total was populated at pre-flight.
        assert!(progress.lock().segments_total >= 1);
    }

    #[test]
    fn run_backup_refuses_in_place_engine() {
        let dir = TempDir::new().unwrap();
        let size = 8 * 1024 * 1024;
        let engine = slot_engine(size);
        let config = test_config(dir.path(), size);
        let params = low_headroom_params();
        let cancel = AtomicBool::new(false);
        let progress = Mutex::new(BackupProgress::default());
        let blob_pause = AtomicBool::new(false);

        match run_backup(
            &engine,
            &blob_pause,
            &config,
            &params,
            &dir.path().join("bk"),
            &cancel,
            &progress,
        ) {
            Err(BackupError::UnsupportedConfig(msg)) => {
                assert!(msg.contains("segment store"), "unexpected message: {msg}");
            }
            other => panic!("expected UnsupportedConfig, got {other:?}"),
        }
        assert_eq!(progress.lock().state, BackupState::Failed);
    }

    #[test]
    fn run_backup_aborts_on_low_headroom() {
        let dir = TempDir::new().unwrap();
        // 8 MiB device, 1 MiB segments, 1 MiB header reserve → 7 segments, so
        // virgin headroom is 6. The default floor (64) rejects it.
        let size = 8 * 1024 * 1024;
        let engine = seg_engine(size);
        let config = test_config(dir.path(), size);
        let params = BackupParams {
            throttle_bytes_per_sec: 0,
            ..BackupParams::default()
        };
        let cancel = AtomicBool::new(false);
        let progress = Mutex::new(BackupProgress::default());
        let blob_pause = AtomicBool::new(false);

        match run_backup(
            &engine,
            &blob_pause,
            &config,
            &params,
            &dir.path().join("bk"),
            &cancel,
            &progress,
        ) {
            Err(BackupError::InsufficientHeadroom { store, have, need }) => {
                assert_eq!(store, 0);
                assert_eq!(need, 64);
                assert!(have < 64, "have {have} should be below the floor");
            }
            other => panic!("expected InsufficientHeadroom, got {other:?}"),
        }
        // The pin must have been released even though pre-flight failed.
        assert!(!engine.is_segment_lifecycle_pinned());
    }
}
