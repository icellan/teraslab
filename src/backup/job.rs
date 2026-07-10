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

use std::fs::File;
use std::io::{BufWriter, Write};
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

/// A copied range plus its bytes, buffered in RAM for later image assembly.
/// Used ONLY for the bounded final-stall residual (copied under the visibility
/// guard, where no backup-dir I/O may happen); the bulk copy streams straight
/// to the image file and never buffers.
struct RangeBytes {
    entry: RangeEntry,
    bytes: Vec<u8>,
}

/// Where [`copy_segments`] deposits each copied segment. Two implementations:
/// [`StreamingSink`] writes bytes straight to the store's image file (so the
/// whole live-data set is never held in RAM — the OOM-safe bulk path), and
/// [`BufferSink`] holds them in RAM (the bounded final-stall residual, which
/// must not touch the backup dir while the visibility guard is held).
trait SegmentSink {
    fn accept(&mut self, entry: RangeEntry, bytes: Vec<u8>) -> Result<(), BackupError>;
}

/// Streams each copied segment to the image file, folds its bytes into the
/// running whole-image hash, and records the range (in FILE order).
struct StreamingSink<'a, W: Write> {
    file: &'a mut W,
    whole: &'a mut Sha256,
    ranges: &'a mut Vec<RangeEntry>,
}

impl<W: Write> SegmentSink for StreamingSink<'_, W> {
    fn accept(&mut self, entry: RangeEntry, bytes: Vec<u8>) -> Result<(), BackupError> {
        self.file.write_all(&bytes).map_err(BackupError::Io)?;
        self.whole.update(&bytes);
        self.ranges.push(entry);
        Ok(())
    }
}

/// Buffers each copied segment in RAM for the caller to flush after the
/// visibility guard is released. Bounded by `stall_copy_max_segments`.
struct BufferSink<'a> {
    out: &'a mut Vec<RangeBytes>,
}

impl SegmentSink for BufferSink<'_> {
    fn accept(&mut self, entry: RangeEntry, bytes: Vec<u8>) -> Result<(), BackupError> {
        self.out.push(RangeBytes { entry, bytes });
        Ok(())
    }
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

    // 4. Backup-owned index snapshot: the primary/DAH index AND the
    //    authoritative in-RAM MinedIndex. Post-Task-16d `set_mined` writes zero
    //    device bytes and (via its batch WAL) only a redo record, so mined-state
    //    lives ONLY in RAM + this `.mined` snapshot + the fenced redo tail. The
    //    `.mined` is fence-stamped with the SAME `fence` sampled under the
    //    visibility guard at step 3 (and stamped on the fabricated redo region
    //    below), so restore replays the tail STRICTLY ABOVE it — exactly the
    //    contract `checkpoint::perform_checkpoint_inner` uses when it stamps
    //    `snapshot_fence_sequence` into both its `.mined` and its redo fence.
    //    Both snapshots are taken live (fuzzy) at this same point; a mutation
    //    captured by the fuzzy scan but landing above the fence is folded into
    //    `.mined` AND replayed from the teed tail — `recover_mined_index`'s
    //    idempotent above-fence replay reconciles the overlap.
    set_state(progress, BackupState::Snapshotting);
    let index_snapshot_path = target_dir.join("teraslab-index.snap");
    engine
        .snapshot_index(&index_snapshot_path)
        .map_err(|e| BackupError::Index(e.to_string()))?;
    let mined_index_snapshot_path =
        crate::checkpoint::mined_index_snapshot_path(&index_snapshot_path);
    engine
        .snapshot_mined_index_by_key(&mined_index_snapshot_path, fence)
        .map_err(BackupError::Io)?;

    // 5. Copy sealed + growing segments — STREAMED straight to each store's
    //    image file so the whole live-data set is never held in RAM (only one
    //    128 KiB copy chunk is, inside `copy_range`). The image is laid out in
    //    copy order: streamed segments (ascending), then the bounded final-stall
    //    residual, then the header (device offset 0) last; the manifest range
    //    list is kept in that same file order so restore reads it with a plain
    //    cumulative cursor.
    set_state(progress, BackupState::Copying);
    let mut copied_through: Vec<Option<u32>> = vec![None; store_count];
    let mut store_ranges: Vec<Vec<RangeEntry>> = (0..store_count).map(|_| Vec::new()).collect();
    let mut throttles: Vec<TokenBucket> = (0..store_count)
        .map(|_| TokenBucket::new(params.throttle_bytes_per_sec))
        .collect();
    let mut bytes_copied = 0u64;
    let mut segments_copied = 0u32;

    // Per-store: the open image file (buffered) and its running whole-image hash.
    let mut image_files: Vec<BufWriter<File>> = Vec::with_capacity(store_count);
    let mut whole_hashers: Vec<Sha256> = Vec::with_capacity(store_count);
    for s in 0..store_count as u8 {
        let path = target_dir.join(format!("store.{s}.img"));
        let f = File::create(&path).map_err(BackupError::Io)?;
        image_files.push(BufWriter::new(f));
        whole_hashers.push(Sha256::new());
    }

    for s in 0..store_count as u8 {
        let view = engine.backup_view_for(s).ok_or_else(|| not_segment(s))?;
        let end = view.open_segment;
        let mut sink = StreamingSink {
            file: &mut image_files[s as usize],
            whole: &mut whole_hashers[s as usize],
            ranges: &mut store_ranges[s as usize],
        };
        copy_segments(
            engine,
            s,
            &view,
            0,
            end,
            align,
            params.abort_headroom_segments,
            &mut throttles[s as usize],
            &mut sink,
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
            let mut sink = StreamingSink {
                file: &mut image_files[s as usize],
                whole: &mut whole_hashers[s as usize],
                ranges: &mut store_ranges[s as usize],
            };
            copy_segments(
                engine,
                s,
                &view,
                base + 1,
                new_open,
                align,
                params.abort_headroom_segments,
                &mut throttles[s as usize],
                &mut sink,
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
    //    the tail — all under the visibility guard, no backup-dir I/O. The
    //    residual segments are buffered in RAM here (bounded by
    //    `stall_copy_max_segments`) and flushed to the image file AFTER the guard
    //    releases; nothing writes to the backup dir while the guard is held.
    set_state(progress, BackupState::Finalizing);
    let mut headers: Vec<Vec<u8>> = Vec::with_capacity(store_count);
    let mut residuals: Vec<Vec<RangeBytes>> = (0..store_count).map(|_| Vec::new()).collect();
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
                let mut sink = BufferSink {
                    out: &mut residuals[s as usize],
                };
                copy_segments(
                    engine,
                    s,
                    &view,
                    base + 1,
                    view.open_segment,
                    align,
                    params.abort_headroom_segments,
                    &mut unthrottled,
                    &mut sink,
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

    // 8. Finish per-store images + fabricated redo files (outside the guard).
    //    The streamed segments are already on disk; append the bounded residual
    //    and the header (device offset 0, written LAST in the file), then flush
    //    and fsync. The manifest range list stays in file order so restore reads
    //    it with a plain cumulative cursor.
    let mut stores: Vec<StoreManifest> = Vec::with_capacity(store_count);
    for s in 0..store_count as u8 {
        let view = engine.backup_view_for(s).ok_or_else(|| not_segment(s))?;
        let file = &mut image_files[s as usize];
        let whole = &mut whole_hashers[s as usize];
        let ranges = &mut store_ranges[s as usize];

        // Append the (bounded) final-stall residual segments buffered under the
        // guard.
        for rb in std::mem::take(&mut residuals[s as usize]) {
            file.write_all(&rb.bytes).map_err(BackupError::Io)?;
            whole.update(&rb.bytes);
            ranges.push(rb.entry);
        }
        // Append the in-memory allocator header last; restore pwrites it to
        // device offset 0 regardless of its position in the image.
        let header_bytes = std::mem::take(&mut headers[s as usize]);
        file.write_all(&header_bytes).map_err(BackupError::Io)?;
        whole.update(&header_bytes);
        ranges.push(RangeEntry {
            device_offset: 0,
            len: header_bytes.len() as u64,
            sha256: sha256_hex(&header_bytes),
        });

        // Flush the buffered writer to the file and fsync it durable.
        file.flush().map_err(BackupError::Io)?;
        file.get_ref().sync_all().map_err(BackupError::Io)?;

        let image_file = format!("store.{s}.img");
        let image_sha256 = copier::hex_digest(whole.clone());
        let ranges = std::mem::take(ranges);

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
    // The MinedIndex snapshot lives beside the primary snapshot under the same
    // `.mined` sibling convention restore/boot derive it by.
    let mined_index_snapshot_file = mined_index_snapshot_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("teraslab-index.snap.mined")
        .to_string();
    let mined_index_sha256 = {
        let bytes = std::fs::read(&mined_index_snapshot_path).map_err(BackupError::Io)?;
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
        mined_index_snapshot_file,
        mined_index_sha256,
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
///
/// Before each segment is copied, the store's LIVE virgin headroom is
/// re-sampled: if it has fallen below `abort_headroom` the copy aborts with
/// [`BackupError::InsufficientHeadroom`] so the backup fails BEFORE a client
/// allocation would (the "backups fail, client writes never do" guarantee).
/// The RAII pin guard held by the caller still unpins on this early return.
#[allow(clippy::too_many_arguments)]
fn copy_segments(
    engine: &Engine,
    device_id: u8,
    view: &SegmentBackupView,
    start: u32,
    end: u32,
    align: usize,
    abort_headroom: u32,
    throttle: &mut TokenBucket,
    sink: &mut impl SegmentSink,
    cancel: &AtomicBool,
    progress: &Mutex<BackupProgress>,
    bytes_copied: &mut u64,
    segments_copied: &mut u32,
) -> Result<(), BackupError> {
    for k in start..=end {
        if cancel.load(Ordering::Relaxed) {
            return Err(BackupError::Aborted);
        }
        // Live headroom check: sample the CURRENT virgin headroom (not the
        // snapshot in `view`) so a backup racing client writes that consume
        // segments aborts before allocation can fail.
        if let Some(h) = engine
            .backup_view_for(device_id)
            .map(|v| v.virgin_headroom_segments())
            && h < abort_headroom
        {
            return Err(BackupError::InsufficientHeadroom {
                store: device_id,
                have: h,
                need: abort_headroom,
            });
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
        sink.accept(entry, bytes)?;
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
    use crate::index::{DahIndex, Index};
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
        ServerConfig {
            device_paths: vec![dir.join("data.dat")],
            device_size: size,
            device_alignment: ALIGN,
            device_split: 1,
            // A non-existent blob dir → empty blobs.
            blobstore_path: dir.join("no-blobstore"),
            ..Default::default()
        }
    }

    fn low_headroom_params() -> BackupParams {
        BackupParams {
            throttle_bytes_per_sec: 0,
            min_headroom_segments: 1,
            // These tiny test devices have only a handful of virgin segments,
            // well under the production default abort floor (16). Drop the
            // mid-run abort floor to 0 so the live headroom monitor never
            // trips on a device this small — these tests exercise the success
            // path, not headroom exhaustion.
            abort_headroom_segments: 0,
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
        assert_eq!(progress.lock().state, BackupState::Done);

        let st = &read.stores[0];
        // Streaming layout: the header (device offset 0) is written LAST; every
        // earlier range is a segment (device offset > 0).
        assert_eq!(
            st.ranges.last().unwrap().device_offset,
            0,
            "the allocator header is the final range (streamed segments first)"
        );
        assert!(
            st.ranges[..st.ranges.len() - 1]
                .iter()
                .all(|r| r.device_offset > 0),
            "all non-final ranges are segments at non-zero device offsets"
        );

        // The image file equals the concatenation of the ranges IN LIST ORDER —
        // the invariant restore relies on (it reads the image with a plain
        // cumulative cursor). Verify by slicing the file at cumulative offsets
        // and matching each slice's SHA-256 to its manifest range.
        let image = std::fs::read(target.join(&st.image_file)).unwrap();
        let total: u64 = st.ranges.iter().map(|r| r.len).sum();
        assert_eq!(image.len() as u64, total, "image length == sum of ranges");
        let mut cursor = 0usize;
        for r in &st.ranges {
            let slice = &image[cursor..cursor + r.len as usize];
            assert_eq!(
                sha256_hex(slice),
                r.sha256,
                "range at file offset {cursor} (device {}) must match its manifest sha",
                r.device_offset
            );
            cursor += r.len as usize;
        }
        // The whole-image hash covers the file in that same order.
        assert_eq!(st.image_sha256, sha256_hex(&image));
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

    /// Mid-run monitor (R14-7): pre-flight passes (`min_headroom_segments`
    /// low), but `abort_headroom_segments` is set ABOVE the store's actual
    /// virgin headroom, so the FIRST per-segment live check inside
    /// `copy_segments` fails — proving the copy loop aborts on headroom
    /// exhaustion mid-run, not only at pre-flight. The pin must be released.
    #[test]
    fn run_backup_aborts_mid_run_when_headroom_below_abort_floor() {
        let dir = TempDir::new().unwrap();
        // 8 MiB device, 1 MiB segments → ~6 virgin segments of headroom.
        let size = 8 * 1024 * 1024;
        let engine = seg_engine(size);
        // Write a couple records so the copy loop has at least one segment to
        // reach the per-segment headroom check.
        for i in 1..=2u64 {
            make_record(&engine, i);
        }
        let headroom = engine
            .backup_view_for(0)
            .expect("segment store")
            .virgin_headroom_segments();
        assert!(headroom >= 1, "expected some headroom, got {headroom}");

        let config = test_config(dir.path(), size);
        let params = BackupParams {
            throttle_bytes_per_sec: 0,
            // Pre-flight passes: 1 <= actual headroom.
            min_headroom_segments: 1,
            // Mid-run trips: set the abort floor ABOVE the actual headroom so
            // the first per-segment live sample is below it.
            abort_headroom_segments: headroom + 1000,
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
                assert_eq!(need, headroom + 1000);
                assert!(
                    have <= headroom,
                    "mid-run sample {have} must be <= pre-flight headroom {headroom}"
                );
            }
            other => panic!("expected mid-run InsufficientHeadroom, got {other:?}"),
        }
        // The RAII pin must have been released on the mid-run abort.
        assert!(!engine.is_segment_lifecycle_pinned());
        assert_eq!(progress.lock().state, BackupState::Failed);
        // No manifest is written on abort.
        assert!(!dir.path().join("bk").join(MANIFEST_FILE).exists());
    }
}
