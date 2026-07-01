//! Log-structured segment allocator for TeraSlab records (Phase 1).
//!
//! This is the append-only counterpart to [`crate::allocator::SlotAllocator`].
//! Where the slot allocator places each record at a best-fit/high-water home
//! offset (scattering writes), the segment allocator carves the data region into
//! fixed-size **segments** and hands out offsets from a single **append cursor**
//! within the currently-open segment. Sequential creates therefore land at
//! contiguous offsets, which lets the write-back [`crate::cache::CachingDevice`]
//! coalesce them into large sequential flushes — the core of the log-structured
//! write model (see `bench/results/LOG_STRUCTURED_DATA_LAYER_DESIGN.md`).
//!
//! Phase 1 scope (this module):
//! - append-cursor allocation with seal-and-advance across fixed segments;
//! - the packed within-device-block placement invariant reused from the slot
//!   allocator (a record never straddles a device block; a large record is
//!   block-aligned), now also constrained to never straddle a segment boundary;
//! - [`SegmentAllocator::free`] marks bytes **dead** for wear/occupancy
//!   accounting — it does NOT reclaim space. Whole-segment reclaim and the
//!   background defrag worker are Phase 3.
//!
//! `record_offset` stays an absolute device byte offset (no packed
//! `segment_id<<k|intra` encoding): the read path consumes it directly as a
//! device address, and `segment_of(offset)` derives the segment id. See the
//! design doc §0.1.

use crate::allocator::{
    AllocatedRegion, AllocatorStats, BatchRollback, PendingBatchAllocation, RecordAllocator,
};
use crate::device::{AlignedBuf, BlockDevice, DeviceError};
use crate::redo::{RedoLog, RedoOp};
use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Start of the data region on device. Bytes before this hold the segment
/// allocator header. Matches [`crate::allocator::DATA_REGION_OFFSET`] so a
/// device can be reasoned about identically regardless of engine.
pub const DATA_REGION_OFFSET: u64 = 1024 * 1024; // 1 MiB reserved for header

/// Small reservation alignment used in packed mode, identical to
/// [`crate::allocator::RECORD_ALIGN`] so packed offsets are computed the same
/// way under either engine.
pub const RECORD_ALIGN: u64 = 8;

/// Magic for the segment allocator header. Deliberately DISTINCT from
/// [`crate::allocator`]'s `ALLOCATOR_MAGIC` so a device formatted by one engine
/// fails closed when opened by the other (rather than misreading the header).
const SEG_MAGIC: u64 = 0x5445_5241_5345_474C; // "TERASEGL"

/// Current segment-header layout version. v2 persists up to the highest-USED
/// segment (not just `open_segment`) so a defrag-reclaimed non-monotonic layout
/// round-trips; v1 (persist up to `open_segment`) is a subset (no reuse gaps) and
/// still reads correctly. See [`SegmentAllocator::persist_header_no_sync`].
const SEG_HEADER_VERSION: u32 = 2;
/// Minimum header version this build can read. v1 and v2 share the read format;
/// v2 merely allows `used == 0` reuse gaps below the highest-used segment.
const SEG_HEADER_MIN_READABLE: u32 = 1;

// Fixed byte offsets within the header (little-endian fields).
const OFF_MAGIC: usize = 0; // u64
const OFF_VERSION: usize = 8; // u32
const OFF_PACKED: usize = 12; // u8
const OFF_DEVICE_ID: usize = 16; // [u8;16]
const OFF_SEGMENT_SIZE: usize = 32; // u64
const OFF_SEGMENT_COUNT: usize = 40; // u32
const OFF_OPEN_SEGMENT: usize = 44; // u32
const OFF_CURSOR: usize = 48; // u64
const OFF_ENTRY_COUNT: usize = 56; // u32 (number of per-segment entries persisted)
const OFF_CRC: usize = 64; // u32 (over bytes 0..table_end, this field zeroed)
/// Byte offset where the per-segment table begins. Each entry is
/// `used: u64` + `dead: u64` = 16 bytes.
const SEG_TABLE_OFFSET: usize = 72;
/// Bytes per persisted per-segment table entry (`used` + `dead`).
const SEG_ENTRY_SIZE: usize = 16;

/// Maximum number of per-segment table entries the on-device header can hold.
/// At a 1 MiB header and 16 bytes/entry this is ~65k segments (≈512 GiB at an
/// 8 MiB segment size). [`SegmentAllocator::persist`] fails loud with
/// [`SegmentAllocatorError::SegmentTableOverflow`] rather than silently
/// truncating — the Phase 4 fix for very large devices is a larger header or a
/// recompute-only recovery (design §3.2).
pub const MAX_PERSISTED_SEGMENTS: usize =
    (DATA_REGION_OFFSET as usize - SEG_TABLE_OFFSET) / SEG_ENTRY_SIZE;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by the segment allocator.
#[derive(Error, Debug)]
pub enum SegmentAllocatorError {
    /// No free segment space remains for the requested allocation, or the
    /// request is larger than a whole segment (records cannot span segments).
    #[error(
        "device full: requested {requested} bytes; open segment {open_segment} has {remaining} bytes free and no further segments are available (segment_size={segment_size})"
    )]
    DeviceFull {
        /// Aligned reservation size requested.
        requested: u64,
        /// Bytes left in the open segment when the request was rejected.
        remaining: u64,
        /// The open segment index at rejection time.
        open_segment: u32,
        /// Configured segment size.
        segment_size: u64,
    },

    /// `segment_size` is not a positive multiple of the device alignment, or is
    /// larger than the data region. Segments must be block-aligned so the packed
    /// within-block placement invariant holds at every segment boundary.
    #[error(
        "invalid segment size {segment_size}: must be a positive multiple of device alignment {alignment} and fit the data region ({data_capacity} bytes)"
    )]
    InvalidSegmentSize {
        /// The rejected segment size.
        segment_size: u64,
        /// Device I/O alignment.
        alignment: usize,
        /// Usable data-region capacity.
        data_capacity: u64,
    },

    /// Attempted to free a region outside the data region or the device.
    #[error("invalid free: offset {offset} + size {size} outside data region")]
    InvalidFree {
        /// Start offset of the rejected free.
        offset: u64,
        /// Aligned size of the rejected free.
        size: u64,
    },

    /// A device I/O error occurred.
    #[error("device error: {0}")]
    Device(#[from] DeviceError),

    /// Failed to generate random bytes for device identity.
    #[error("failed to generate device identity: {0}")]
    Getrandom(getrandom::Error),

    /// The header region is all zeros — a genuinely fresh device that has never
    /// had a segment header persisted. The only error a caller may treat as
    /// "safe to initialize a fresh allocator"; every other variant fails closed.
    #[error("no persisted allocator state: header region is all zeros (fresh device)")]
    NoPersistedState,

    /// The on-disk header is non-zero garbage, has the wrong magic (e.g. a
    /// device formatted by the in-place engine), or fails CRC verification.
    #[error("corrupted segment header: {0}")]
    CorruptedHeader(&'static str),

    /// The on-disk header CRC32 did not match.
    #[error(
        "segment header corruption: CRC mismatch (expected={expected:#010x}, actual={actual:#010x})"
    )]
    HeaderCorruption {
        /// CRC stored in the header.
        expected: u32,
        /// CRC recomputed over the header bytes.
        actual: u32,
    },

    /// The on-disk header version is not supported by this build.
    #[error("unsupported segment header version: {0}")]
    UnsupportedVersion(u32),

    /// More segments have been touched than the on-device header can record.
    #[error("segment table overflow: {entries} entries, max persistable is {max}")]
    SegmentTableOverflow {
        /// Number of entries that would need persisting.
        entries: usize,
        /// The header's capacity.
        max: usize,
    },
}

/// Result type for segment allocator operations.
pub type Result<T> = std::result::Result<T, SegmentAllocatorError>;

// ---------------------------------------------------------------------------
// SegmentMeta + stats
// ---------------------------------------------------------------------------

/// Per-segment accounting. `live = used - dead`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SegmentMeta {
    /// Bytes consumed by the append cursor within this segment (high-water).
    used: u64,
    /// Bytes freed (logically dead) within this segment. Phase 1 never reclaims
    /// these; the field drives wear/occupancy stats and (Phase 3) defrag victim
    /// selection.
    dead: u64,
}

/// Summary statistics for the segment allocator (observability).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentAllocatorStats {
    /// Start of the data region (bytes).
    pub data_region_start: u64,
    /// Total device size (bytes).
    pub device_size: u64,
    /// Configured segment size (bytes).
    pub segment_size: u64,
    /// Total number of segments carved from the data region.
    pub segment_count: u32,
    /// The currently-open (appendable) segment index.
    pub open_segment: u32,
    /// Absolute device offset of the next allocation.
    pub cursor: u64,
    /// Bytes consumed across all segments (sum of per-segment `used`).
    pub used_bytes: u64,
    /// Bytes logically dead across all segments (sum of per-segment `dead`).
    pub dead_bytes: u64,
    /// Live bytes = `used_bytes - dead_bytes`.
    pub live_bytes: u64,
}

// ---------------------------------------------------------------------------
// SegmentAllocator
// ---------------------------------------------------------------------------

/// Append-cursor segment allocator. See the module docs.
pub struct SegmentAllocator {
    device: Arc<dyn BlockDevice>,
    data_region_start: u64,
    device_size: u64,
    alignment: usize,
    /// 128-bit device identity, generated at format time, persisted at bytes
    /// 16..32 of the header.
    device_id: [u8; 16],
    /// Size of each segment in bytes (block-aligned, ≥ alignment).
    segment_size: u64,
    /// Total segments carved from `[data_region_start, device_size)`.
    segment_count: u32,
    /// Per-segment accounting, length == `segment_count`.
    segments: Vec<SegmentMeta>,
    /// Segments reclaimed by defrag (fully dead) and available for reuse. When
    /// [`Self::advance_to_next_segment`] needs a fresh segment it pops one of
    /// these before advancing the high-water past `open_segment` — this is what
    /// BOUNDS device growth under the relocate-heavy segment engine (Phase 3
    /// defrag). Empty until [`Self::reclaim_fully_dead_segments`] runs; a fresh
    /// (never-reclaimed) device therefore behaves exactly as pure append. Held in
    /// FIFO order (oldest-reclaimed first) so reuse is deterministic.
    free_segments: std::collections::VecDeque<u32>,
    /// The currently-open (appendable) segment.
    open_segment: u32,
    /// Absolute device offset of the next allocation (inside `open_segment`).
    cursor: u64,
    /// Packed mode (records packed at [`RECORD_ALIGN`] within a device block).
    /// Persisted; the device's format wins over config across restarts.
    packed: bool,
    /// Optional redo log handle. Unlike [`crate::allocator::SlotAllocator`], the
    /// segment allocator journals NO region ops on allocate/free — its cursor is
    /// recomputed from the index at recovery (design §3.2), so there is no
    /// `AllocateRegion`/orphan window. The handle is retained for the relocate
    /// path (increment 4, `OP_RELOCATE`). Not persisted.
    redo_log: Option<Arc<Mutex<RedoLog>>>,
    /// Store tag stamped on this allocator's future redo entries (relocate).
    redo_device_id: u8,
}

impl std::fmt::Debug for SegmentAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Omit the `device` handle (not `Debug`); print the scalar state.
        f.debug_struct("SegmentAllocator")
            .field("segment_size", &self.segment_size)
            .field("segment_count", &self.segment_count)
            .field("open_segment", &self.open_segment)
            .field("cursor", &self.cursor)
            .field("packed", &self.packed)
            .finish()
    }
}

impl SegmentAllocator {
    /// Create a fresh segment allocator over `device` with `segment_size`.
    ///
    /// `segment_size` must be a positive multiple of the device alignment and
    /// fit within the data region, else [`SegmentAllocatorError::InvalidSegmentSize`].
    pub fn new(device: Arc<dyn BlockDevice>, segment_size: u64) -> Result<Self> {
        let alignment = device.alignment();
        let device_size = device.size();
        let data_region_start = DATA_REGION_OFFSET;
        let data_capacity = device_size.saturating_sub(data_region_start);

        if segment_size == 0
            || !segment_size.is_multiple_of(alignment as u64)
            || segment_size > data_capacity
        {
            return Err(SegmentAllocatorError::InvalidSegmentSize {
                segment_size,
                alignment,
                data_capacity,
            });
        }

        let segment_count = (data_capacity / segment_size) as u32;
        let mut device_id = [0u8; 16];
        getrandom::getrandom(&mut device_id).map_err(SegmentAllocatorError::Getrandom)?;

        Ok(Self {
            device,
            data_region_start,
            device_size,
            alignment,
            device_id,
            segment_size,
            segment_count,
            segments: vec![SegmentMeta::default(); segment_count as usize],
            free_segments: std::collections::VecDeque::new(),
            open_segment: 0,
            cursor: data_region_start,
            packed: false,
            redo_log: None,
            redo_device_id: 0,
        })
    }

    /// Enable or disable packed allocation mode. Set once at startup before any
    /// allocation; toggling on a device with records placed under the other mode
    /// is unsupported (offsets differ). See [`crate::allocator::SlotAllocator::set_packed`].
    pub fn set_packed(&mut self, packed: bool) {
        self.packed = packed;
    }

    /// Whether packed mode is enabled.
    pub fn is_packed(&self) -> bool {
        self.packed
    }

    /// Absolute device offset of the start of segment `id`.
    fn segment_start(&self, id: u32) -> u64 {
        self.data_region_start + (id as u64) * self.segment_size
    }

    /// The segment index that owns absolute device `offset`.
    ///
    /// Returns `None` if `offset` is before the data region or beyond the last
    /// segment.
    fn segment_of(&self, offset: u64) -> Option<u32> {
        if offset < self.data_region_start {
            return None;
        }
        let idx = (offset - self.data_region_start) / self.segment_size;
        if idx >= self.segment_count as u64 {
            None
        } else {
            Some(idx as u32)
        }
    }

    /// Compute the reservation size for `size`, honoring packed mode. Identical
    /// rule to [`crate::allocator::SlotAllocator`]: non-packed rounds to the
    /// device block; packed rounds a small record to [`RECORD_ALIGN`] and a
    /// large record (> block) to the device block.
    fn align_reservation(&self, size: u64) -> u64 {
        let block = self.alignment as u64;
        if !self.packed {
            return size.div_ceil(block) * block;
        }
        if size <= block {
            size.div_ceil(RECORD_ALIGN) * RECORD_ALIGN
        } else {
            size.div_ceil(block) * block
        }
    }

    /// Allocate a contiguous region of at least `size` bytes from the append
    /// cursor, returning its absolute device offset.
    ///
    /// The record is placed so it (a) stays within the open segment, and (b) in
    /// packed mode never straddles a device block (a large record is
    /// block-aligned). When the open segment cannot fit the record, it is sealed
    /// and the next segment is opened. Returns
    /// [`SegmentAllocatorError::DeviceFull`] when no segment can hold the record.
    pub fn allocate(&mut self, size: u64) -> Result<u64> {
        let aligned_size = self.align_reservation(size);
        if aligned_size == 0 || aligned_size > self.segment_size {
            // A record larger than a whole segment can never be placed.
            return Err(SegmentAllocatorError::DeviceFull {
                requested: aligned_size,
                remaining: self.open_segment_remaining(),
                open_segment: self.open_segment,
                segment_size: self.segment_size,
            });
        }

        // Try to place in the open segment; advance to the next segment if it
        // doesn't fit (including after the packed within-block bump).
        loop {
            let seg_start = self.segment_start(self.open_segment);
            let seg_end = seg_start + self.segment_size;

            // Packed within-device-block placement: a small record must not
            // straddle a block; a large record must start block-aligned. Bump
            // the cursor to the next block boundary if needed. Identical to
            // SlotAllocator::reserve_aligned's high-water path.
            let mut offset = self.cursor;
            if self.packed {
                let block = self.alignment as u64;
                let need_bump = if aligned_size <= block {
                    offset % block + aligned_size > block
                } else {
                    !offset.is_multiple_of(block)
                };
                if need_bump {
                    offset = offset.div_ceil(block) * block;
                }
            }

            if offset + aligned_size <= seg_end {
                // Fits in the open segment. Account the gap from the packed bump
                // as used too (it is unusable tail within this segment).
                let consumed = (offset + aligned_size) - self.cursor;
                self.segments[self.open_segment as usize].used += consumed;
                self.cursor = offset + aligned_size;
                return Ok(offset);
            }

            // Does not fit — seal the open segment and advance.
            if !self.advance_to_next_segment() {
                return Err(SegmentAllocatorError::DeviceFull {
                    requested: aligned_size,
                    remaining: seg_end.saturating_sub(self.cursor),
                    open_segment: self.open_segment,
                    segment_size: self.segment_size,
                });
            }
        }
    }

    /// Bytes left in the open segment from the cursor to the segment end.
    fn open_segment_remaining(&self) -> u64 {
        let seg_end = self.segment_start(self.open_segment) + self.segment_size;
        seg_end.saturating_sub(self.cursor)
    }

    /// Seal the open segment and open the next one. Returns `false` when there
    /// is no further segment (device full).
    ///
    /// A defrag-reclaimed segment ([`Self::free_segments`], FIFO) is REUSED before
    /// the high-water advances — this is what bounds device growth under the
    /// relocate-heavy segment engine: spent records relocate out of old segments,
    /// those segments drain to fully-dead and get reclaimed, and their space is
    /// handed back here instead of forever advancing into virgin segments. A
    /// reused segment starts empty (its `SegmentMeta` was reset at reclaim). With
    /// no reclaimed segments this is exactly the Phase 1 pure append
    /// (`open_segment + 1`).
    fn advance_to_next_segment(&mut self) -> bool {
        if let Some(reused) = self.free_segments.pop_front() {
            debug_assert!(
                self.segments[reused as usize].used == 0
                    && self.segments[reused as usize].dead == 0,
                "a reclaimed segment must be reset to empty before reuse"
            );
            self.open_segment = reused;
            self.cursor = self.segment_start(reused);
            return true;
        }
        let next = self.open_segment + 1;
        if next >= self.segment_count {
            return false;
        }
        self.open_segment = next;
        self.cursor = self.segment_start(next);
        true
    }

    /// Live bytes in segment `idx` (`used - dead`). A fully-drained segment has
    /// `live == 0` (every record it held has been relocated out or deleted). Used
    /// by the defrag worker to pick and verify victims (observability).
    pub fn segment_live(&self, idx: u32) -> u64 {
        let s = &self.segments[idx as usize];
        s.used.saturating_sub(s.dead)
    }

    /// Reclaim every FULLY-DEAD segment (`used > 0 && live == 0`) that is not the
    /// currently-open one: reset its accounting to empty and add it to the reuse
    /// free list. Returns the reclaimed segment indices.
    ///
    /// This is the defrag fast path — under the create→spend(relocate)→delete UTXO
    /// lifecycle a segment's records all eventually leave it (spend relocates them
    /// to the cursor; delete dead-marks them), so the segment drains to fully-dead
    /// WITHOUT any live-record copying and can be reclaimed for free. Partially-
    /// dead segments (some records still live) need compaction — relocate the live
    /// records out first — which is the next increment ([`Self::defrag_victims`]
    /// selects them). The open segment is never reclaimed (the cursor is inside
    /// it). Already-free segments are skipped (idempotent).
    pub fn reclaim_fully_dead_segments(&mut self) -> Vec<u32> {
        let mut reclaimed = Vec::new();
        for idx in 0..self.segment_count {
            if idx == self.open_segment {
                continue;
            }
            let s = &self.segments[idx as usize];
            // used == 0 means never-written OR already-reclaimed: nothing to do.
            if s.used == 0 || s.used != s.dead {
                continue;
            }
            self.segments[idx as usize] = SegmentMeta::default();
            self.free_segments.push_back(idx);
            reclaimed.push(idx);
        }
        reclaimed
    }

    /// Select partially-dead segments worth COMPACTING (relocate their few live
    /// records out, then reclaim), ordered most-dead-first. A segment qualifies
    /// when it is sealed (not the open segment), has been written (`used > 0`),
    /// still holds some live bytes (`live > 0` — fully-dead ones go through the
    /// free [`Self::reclaim_fully_dead_segments`] path), and its dead fraction is
    /// at least `min_dead_frac` (0.0..=1.0). Returns indices sorted by descending
    /// dead fraction so the caller compacts the cheapest-to-empty segments first.
    ///
    /// Read-only: the caller (the Phase 3 defrag worker) drives the actual live-
    /// record relocation via the engine, then reclaims the drained segment.
    pub fn defrag_victims(&self, min_dead_frac: f64) -> Vec<u32> {
        let frac = min_dead_frac.clamp(0.0, 1.0);
        let mut victims: Vec<(u32, f64)> = Vec::new();
        for idx in 0..self.segment_count {
            if idx == self.open_segment {
                continue;
            }
            let s = &self.segments[idx as usize];
            if s.used == 0 || s.dead == 0 || s.dead >= s.used {
                continue; // never-written, all-live, or fully-dead (free path)
            }
            let df = s.dead as f64 / s.used as f64;
            if df >= frac {
                victims.push((idx, df));
            }
        }
        victims.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        victims.into_iter().map(|(idx, _)| idx).collect()
    }

    /// Number of segments currently on the reclaimed free list (observability).
    pub fn free_segment_count(&self) -> usize {
        self.free_segments.len()
    }

    /// The device `[start, end)` byte ranges of up to `max` compaction victims
    /// (partially-dead segments, dead fraction ≥ `min_dead_frac`, most-dead-first).
    /// The engine relocates the live records in each range out, draining the
    /// segment so the fast path can reclaim it. See [`Self::defrag_victims`].
    pub fn defrag_victim_ranges(&self, min_dead_frac: f64, max: usize) -> Vec<(u64, u64)> {
        self.defrag_victims(min_dead_frac)
            .into_iter()
            .take(max)
            .map(|idx| {
                let start = self.segment_start(idx);
                (start, start + self.segment_size)
            })
            .collect()
    }

    /// Mark a previously-allocated region as dead.
    ///
    /// Phase 1 does NOT reclaim space — this only updates the owning segment's
    /// dead-byte accounting for wear/occupancy stats (and, in Phase 3, defrag
    /// victim selection). Validates that the region lies within the data region.
    pub fn free(&mut self, offset: u64, size: u64) -> Result<()> {
        let aligned_size = self.align_reservation(size);
        let Some(end) = offset.checked_add(aligned_size) else {
            return Err(SegmentAllocatorError::InvalidFree {
                offset,
                size: aligned_size,
            });
        };
        if aligned_size == 0 || offset < self.data_region_start || end > self.device_size {
            return Err(SegmentAllocatorError::InvalidFree {
                offset,
                size: aligned_size,
            });
        }
        let Some(seg) = self.segment_of(offset) else {
            return Err(SegmentAllocatorError::InvalidFree {
                offset,
                size: aligned_size,
            });
        };
        self.segments[seg as usize].dead += aligned_size;
        Ok(())
    }

    // -- batch reservation (orphan-prevention parity with SlotAllocator) -----

    /// Reserve a batch of regions IN MEMORY from the append cursor, deferring
    /// durable journaling to the caller (issue #14 parity). The segment allocator
    /// journals NO `AllocateRegion` ops — its cursor is recomputed from the index
    /// at recovery, so an un-journaled reservation simply leaves the cursor where
    /// it was on rollback and there is no durable orphan to compensate. The
    /// returned [`PendingBatchAllocation`] therefore has empty
    /// `allocate_region_redo_ops`; the caller must still pass it to
    /// [`Self::commit_pending`] or [`Self::rollback_pending`].
    pub fn reserve_batch(
        &mut self,
        sizes: &[u64],
    ) -> crate::allocator::Result<PendingBatchAllocation> {
        let pre_cursor = self.cursor;
        let pre_open_segment = self.open_segment;
        let pre_open_used = self.segments[pre_open_segment as usize].used;
        let mut regions = Vec::with_capacity(sizes.len());
        for size in sizes {
            match self.allocate(*size) {
                Ok(offset) => {
                    let aligned = self.align_reservation(*size);
                    regions.push(Some(AllocatedRegion {
                        offset,
                        size: aligned,
                    }));
                }
                Err(SegmentAllocatorError::DeviceFull { .. }) => regions.push(None),
                Err(e) => {
                    // Undo the whole batch in memory, then surface the error.
                    self.restore_cursor(pre_cursor, pre_open_segment, pre_open_used);
                    return Err(e.into());
                }
            }
        }
        Ok(PendingBatchAllocation {
            regions,
            rollback: BatchRollback::Segment {
                pre_cursor,
                pre_open_segment,
                pre_open_used,
            },
            alloc_redo_ops: Vec::new(),
        })
    }

    /// Finalize a reservation (no durable region op to confirm; nothing to do
    /// beyond consuming the handle).
    pub fn commit_pending(&mut self, pending: PendingBatchAllocation) {
        debug_assert!(
            matches!(pending.rollback, BatchRollback::Segment { .. }),
            "SegmentAllocator::commit_pending given a non-Segment rollback handle"
        );
        drop(pending);
    }

    /// Roll back a reservation: restore the append cursor + the open segment's
    /// `used` to their pre-batch values (the open segment index is implied by the
    /// cursor). Any segment opened during the batch is reset to empty.
    pub fn rollback_pending(&mut self, pending: PendingBatchAllocation) {
        let BatchRollback::Segment {
            pre_cursor,
            pre_open_segment,
            pre_open_used,
            ..
        } = pending.rollback
        else {
            unreachable!("SegmentAllocator::rollback_pending given a non-Segment rollback handle");
        };
        self.restore_cursor(pre_cursor, pre_open_segment, pre_open_used);
    }

    /// Restore the cursor/open-segment/used to a pre-batch snapshot. Segments
    /// opened during the batch (`> pre_open_segment`) had no prior allocations,
    /// so their `used` resets to 0; `pre_open_segment`'s `used` is restored.
    fn restore_cursor(&mut self, pre_cursor: u64, pre_open_segment: u32, pre_open_used: u64) {
        for seg in (pre_open_segment + 1)..=self.open_segment {
            self.segments[seg as usize].used = 0;
        }
        self.segments[pre_open_segment as usize].used = pre_open_used;
        self.open_segment = pre_open_segment;
        self.cursor = pre_cursor;
    }

    /// Allocate multiple regions (no deferred journaling). Returns one slot per
    /// requested size: `Some` when reserved, `None` when it did not fit.
    pub fn allocate_batch(
        &mut self,
        sizes: &[u64],
    ) -> crate::allocator::Result<Vec<Option<AllocatedRegion>>> {
        let mut out = Vec::with_capacity(sizes.len());
        for size in sizes {
            match self.allocate(*size) {
                Ok(offset) => out.push(Some(AllocatedRegion {
                    offset,
                    size: self.align_reservation(*size),
                })),
                Err(SegmentAllocatorError::DeviceFull { .. }) => out.push(None),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(out)
    }

    /// Whether `[offset, offset+size)` is a valid in-device record region.
    ///
    /// Recovery uses this to gate replayed creates against a stale offset that
    /// was freed and re-handed to a DIFFERENT record (the in-place SlotAllocator
    /// hazard). The append-cursor segment allocator NEVER reuses an offset in
    /// place, so that hazard does not exist — and crucially, during recovery the
    /// cursor is still at the last-checkpoint value while replayed post-checkpoint
    /// creates land BEYOND it (the cursor is recomputed AFTER replay via
    /// [`Self::set_cursor_at_least`]). Gating on the cursor would therefore falsely
    /// reject every post-checkpoint create. So this is a pure in-device bounds
    /// check; legitimacy is already guaranteed by the checkpoint fence (only
    /// post-fence entries are replayed).
    fn is_allocated_range_impl(&self, offset: u64, size: u64) -> bool {
        let aligned = self.align_reservation(size);
        let Some(end) = offset.checked_add(aligned) else {
            return false;
        };
        aligned != 0 && offset >= self.data_region_start && end <= self.device_size
    }

    /// Recovery: advance the append cursor so it is at least `end` (the end
    /// offset of the highest live record), so post-checkpoint records are never
    /// overwritten by a fresh allocation. The open segment is re-derived from the
    /// new cursor. A no-op if `end` is already at or below the cursor. `end` is
    /// clamped to the device size (a corrupt larger value just wedges allocation
    /// at full rather than reading out of bounds).
    ///
    /// The segment allocator journals no `AllocateRegion` ops (unlike the
    /// SlotAllocator, whose `replay_redo` re-derives its frontier), so this is how
    /// its frontier is restored after a crash (design §3.2).
    pub fn set_cursor_at_least(&mut self, end: u64) {
        if end <= self.cursor {
            return;
        }
        let end = end.min(self.device_size);
        if end <= self.cursor {
            return;
        }
        self.cursor = end;
        let idx = end.saturating_sub(self.data_region_start) / self.segment_size;
        self.open_segment = (idx as u32).min(self.segment_count.saturating_sub(1));
    }

    /// Crash-recovery reconciliation (design §3.2, defrag): rebuild the
    /// reclaimed-segment free list from the LIVE index so a defragged
    /// (bounded-growth, non-monotonic) layout survives a crash. Call AFTER
    /// [`Self::set_cursor_at_least`] has set the cursor + open segment from the
    /// highest live record offset.
    ///
    /// `live_offsets` are the record offsets of every LIVE index entry on this
    /// store. Every segment strictly BELOW `open_segment` (the already-used
    /// region) that holds NO live record is a defrag hole — its records were all
    /// relocated out (spend) or deleted — so reset it to empty and return it to the
    /// reuse free list. Segments with live records keep their (checkpoint-recovered)
    /// accounting.
    ///
    /// Correctness: this is EXACT for the free list, which is what matters — future
    /// appends only ever reuse a live-free segment, so no live record is
    /// overwritten. Per-segment dead-byte accounting for the non-free segments
    /// stays at its last-checkpoint value (a crash loses the post-checkpoint dead
    /// deltas); that only skews compaction victim RANKING, not safety, and
    /// self-heals as new spends dead-mark. The open segment is never freed (the
    /// cursor is inside it, and it holds the highest live record by construction).
    pub fn reconcile_recovered_free_list(&mut self, live_offsets: &[u64]) {
        let mut has_live = vec![false; self.segment_count as usize];
        for &off in live_offsets {
            if let Some(s) = self.segment_of(off) {
                has_live[s as usize] = true;
            }
        }
        self.free_segments.clear();
        for idx in 0..self.open_segment {
            if !has_live[idx as usize] {
                self.segments[idx as usize] = SegmentMeta::default();
                self.free_segments.push_back(idx);
            }
        }
    }

    /// The device identity formatted as a 32-character lowercase hex string.
    fn device_id_hex_impl(&self) -> String {
        self.device_id
            .iter()
            .fold(String::with_capacity(32), |mut s, b| {
                use std::fmt::Write as _;
                let _ = write!(s, "{b:02x}");
                s
            })
    }

    // -- accessors ----------------------------------------------------------

    /// Start of the data region on device.
    pub fn data_region_start(&self) -> u64 {
        self.data_region_start
    }

    /// Absolute device offset of the next allocation (the append cursor).
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// The currently-open (appendable) segment index.
    pub fn open_segment(&self) -> u32 {
        self.open_segment
    }

    /// Total number of segments carved from the data region.
    pub fn segment_count(&self) -> u32 {
        self.segment_count
    }

    /// Configured segment size in bytes.
    pub fn segment_size(&self) -> u64 {
        self.segment_size
    }

    /// Device I/O alignment in bytes.
    pub fn device_alignment(&self) -> usize {
        self.alignment
    }

    /// The 128-bit device identity stored in the header.
    pub fn device_id(&self) -> [u8; 16] {
        self.device_id
    }

    /// Compute a statistics snapshot for observability.
    pub fn stats(&self) -> SegmentAllocatorStats {
        let mut used_bytes = 0u64;
        let mut dead_bytes = 0u64;
        for s in &self.segments {
            used_bytes += s.used;
            dead_bytes += s.dead;
        }
        SegmentAllocatorStats {
            data_region_start: self.data_region_start,
            device_size: self.device_size,
            segment_size: self.segment_size,
            segment_count: self.segment_count,
            open_segment: self.open_segment,
            cursor: self.cursor,
            used_bytes,
            dead_bytes,
            live_bytes: used_bytes.saturating_sub(dead_bytes),
        }
    }

    // -- persistence --------------------------------------------------------

    /// Persist the allocator state to the device header and fsync.
    ///
    /// Persists scalar resume state (cursor, open_segment, segment_size, packed,
    /// device identity) plus the per-segment `used`/`dead` table for segments
    /// `0..=open_segment` (segments above the open one are pristine). Fails with
    /// [`SegmentAllocatorError::SegmentTableOverflow`] if the touched-segment
    /// table does not fit the header.
    pub fn persist(&self) -> Result<()> {
        self.persist_header_no_sync()?;
        self.device.sync()?;
        Ok(())
    }

    /// Write the header WITHOUT the durability fsync. The caller MUST sync the
    /// device afterwards. Mirrors [`crate::allocator::SlotAllocator::persist_header_no_sync`]
    /// so the checkpoint can write every store's header under the lock and sync
    /// all devices once, outside the lock.
    pub(crate) fn persist_header_no_sync(&self) -> Result<()> {
        // Persist every segment up to and including the HIGHEST-USED one (v2).
        //
        // Under defrag reuse the layout is non-monotonic: a reclaimed low segment
        // can be reused while higher segments hold live records, so `open_segment`
        // is no longer the high-water. Persisting only up to `open_segment` (the
        // v1 rule) would drop the accounting of higher used segments. Instead
        // persist up to `max(open_segment, highest used index)`; segments ABOVE
        // that are virgin (never written) and recovered as default, while a
        // `used == 0` gap BELOW it is a defrag-reclaimed free segment (rebuilt into
        // `free_segments` at recovery — the free list needs no separate on-disk
        // representation). Without reuse `highest_used == open_segment`, so this is
        // byte-identical to the v1 layout.
        let highest_used = self
            .segments
            .iter()
            .rposition(|s| s.used > 0)
            .map(|i| i as u32)
            .unwrap_or(0);
        let entry_count = self.open_segment.max(highest_used) as usize + 1;
        if entry_count > MAX_PERSISTED_SEGMENTS {
            return Err(SegmentAllocatorError::SegmentTableOverflow {
                entries: entry_count,
                max: MAX_PERSISTED_SEGMENTS,
            });
        }

        let table_end = SEG_TABLE_OFFSET + entry_count * SEG_ENTRY_SIZE;
        let aligned_len =
            (table_end as u64).div_ceil(self.alignment as u64) * self.alignment as u64;
        let mut buf = AlignedBuf::new(aligned_len as usize, self.alignment);

        buf[OFF_MAGIC..OFF_MAGIC + 8].copy_from_slice(&SEG_MAGIC.to_le_bytes());
        buf[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&SEG_HEADER_VERSION.to_le_bytes());
        buf[OFF_PACKED] = u8::from(self.packed);
        buf[OFF_DEVICE_ID..OFF_DEVICE_ID + 16].copy_from_slice(&self.device_id);
        buf[OFF_SEGMENT_SIZE..OFF_SEGMENT_SIZE + 8]
            .copy_from_slice(&self.segment_size.to_le_bytes());
        buf[OFF_SEGMENT_COUNT..OFF_SEGMENT_COUNT + 4]
            .copy_from_slice(&self.segment_count.to_le_bytes());
        buf[OFF_OPEN_SEGMENT..OFF_OPEN_SEGMENT + 4]
            .copy_from_slice(&self.open_segment.to_le_bytes());
        buf[OFF_CURSOR..OFF_CURSOR + 8].copy_from_slice(&self.cursor.to_le_bytes());
        buf[OFF_ENTRY_COUNT..OFF_ENTRY_COUNT + 4]
            .copy_from_slice(&(entry_count as u32).to_le_bytes());
        // CRC slot stays zero until hashed.
        buf[OFF_CRC..OFF_CRC + 4].copy_from_slice(&0u32.to_le_bytes());

        for i in 0..entry_count {
            let base = SEG_TABLE_OFFSET + i * SEG_ENTRY_SIZE;
            buf[base..base + 8].copy_from_slice(&self.segments[i].used.to_le_bytes());
            buf[base + 8..base + 16].copy_from_slice(&self.segments[i].dead.to_le_bytes());
        }

        let crc = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&buf[..table_end]);
            hasher.finalize()
        };
        buf[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());

        self.device.pwrite_all_at(&buf, 0)?;
        Ok(())
    }

    /// Recover allocator state from the device header.
    ///
    /// Validates magic, version, and CRC before trusting any field. An all-zero
    /// header returns [`SegmentAllocatorError::NoPersistedState`] (the only
    /// "safe to format fresh" signal); any other inconsistency fails closed.
    pub fn recover(device: Arc<dyn BlockDevice>) -> Result<Self> {
        let alignment = device.alignment();
        let device_size = device.size();
        let data_region_start = DATA_REGION_OFFSET;

        // Read the fixed-size header prefix first to learn entry_count.
        let prefix_len = alignment.max(SEG_TABLE_OFFSET);
        let mut prefix = AlignedBuf::new(prefix_len, alignment);
        device.pread_exact_at(&mut prefix, 0)?;

        let magic = u64::from_le_bytes(rd8(&prefix, OFF_MAGIC)?);
        if magic != SEG_MAGIC {
            if prefix.iter().all(|&b| b == 0) {
                return Err(SegmentAllocatorError::NoPersistedState);
            }
            return Err(SegmentAllocatorError::CorruptedHeader(
                "bad magic (wrong engine format or corruption)",
            ));
        }

        let version = u32::from_le_bytes(rd4(&prefix, OFF_VERSION)?);
        if !(SEG_HEADER_MIN_READABLE..=SEG_HEADER_VERSION).contains(&version) {
            return Err(SegmentAllocatorError::UnsupportedVersion(version));
        }

        let packed = match prefix[OFF_PACKED] {
            0 => false,
            1 => true,
            _ => {
                return Err(SegmentAllocatorError::CorruptedHeader(
                    "packed flag is not 0 or 1",
                ));
            }
        };
        let mut device_id = [0u8; 16];
        device_id.copy_from_slice(&prefix[OFF_DEVICE_ID..OFF_DEVICE_ID + 16]);
        let segment_size = u64::from_le_bytes(rd8(&prefix, OFF_SEGMENT_SIZE)?);
        let segment_count = u32::from_le_bytes(rd4(&prefix, OFF_SEGMENT_COUNT)?);
        let open_segment = u32::from_le_bytes(rd4(&prefix, OFF_OPEN_SEGMENT)?);
        let cursor = u64::from_le_bytes(rd8(&prefix, OFF_CURSOR)?);
        let entry_count = u32::from_le_bytes(rd4(&prefix, OFF_ENTRY_COUNT)?) as usize;

        // Bound entry_count and geometry before any arithmetic on disk-derived
        // values (fail-closed on a crafted/torn header).
        let data_capacity = device_size.saturating_sub(data_region_start);
        if segment_size == 0
            || !segment_size.is_multiple_of(alignment as u64)
            || segment_size > data_capacity
        {
            return Err(SegmentAllocatorError::CorruptedHeader(
                "invalid segment_size",
            ));
        }
        let expected_count = (data_capacity / segment_size) as u32;
        if segment_count != expected_count {
            return Err(SegmentAllocatorError::CorruptedHeader(
                "segment_count disagrees with device geometry",
            ));
        }
        // v2: `entry_count` covers up to the highest-USED segment, so it may
        // exceed `open_segment + 1` (a reused low open segment with higher used
        // segments) — require only that the open segment falls within the
        // persisted range and the count is geometrically sane. (v1 devices satisfy
        // `entry_count == open_segment + 1`, a subset of this.)
        if entry_count == 0
            || entry_count > MAX_PERSISTED_SEGMENTS
            || entry_count > segment_count as usize
            || open_segment >= segment_count
            || open_segment as usize >= entry_count
        {
            return Err(SegmentAllocatorError::CorruptedHeader(
                "entry_count/open_segment inconsistent",
            ));
        }

        let table_end = SEG_TABLE_OFFSET + entry_count * SEG_ENTRY_SIZE;
        let aligned_len = (table_end as u64).div_ceil(alignment as u64) * alignment as u64;
        let mut buf = AlignedBuf::new(aligned_len as usize, alignment);
        device.pread_exact_at(&mut buf, 0)?;

        let stored_crc = u32::from_le_bytes(rd4(&buf, OFF_CRC)?);
        let mut crc_input: Vec<u8> = buf[..table_end].to_vec();
        for byte in &mut crc_input[OFF_CRC..OFF_CRC + 4] {
            *byte = 0;
        }
        let computed_crc = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&crc_input);
            hasher.finalize()
        };
        if computed_crc != stored_crc {
            return Err(SegmentAllocatorError::HeaderCorruption {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        // Cursor must lie within the open segment.
        let open_start = data_region_start + (open_segment as u64) * segment_size;
        let open_end = open_start + segment_size;
        if cursor < open_start || cursor > open_end {
            return Err(SegmentAllocatorError::CorruptedHeader(
                "cursor outside open segment",
            ));
        }

        let mut segments = vec![SegmentMeta::default(); segment_count as usize];
        for (i, seg) in segments.iter_mut().enumerate().take(entry_count) {
            let base = SEG_TABLE_OFFSET + i * SEG_ENTRY_SIZE;
            seg.used = u64::from_le_bytes(rd8(&buf, base)?);
            seg.dead = u64::from_le_bytes(rd8(&buf, base + 8)?);
        }
        // Rebuild the reclaimed-segment free list: a `used == 0` gap BELOW the
        // highest-persisted index (`entry_count`) is a defrag-reclaimed segment
        // (segments above `entry_count` are the never-written virgin tail the
        // append cursor grows into first). Exclude the open segment (it may sit at
        // `used == 0` right after an advance but is not free). Ascending order is a
        // deterministic reuse order; it only affects future placement, never the
        // recovered records. `dead > 0 && used == 0` would be a torn header.
        let mut free_segments = std::collections::VecDeque::new();
        for (i, seg) in segments.iter().enumerate().take(entry_count) {
            if seg.used == 0 && i as u32 != open_segment {
                if seg.dead != 0 {
                    return Err(SegmentAllocatorError::CorruptedHeader(
                        "segment has dead bytes but zero used",
                    ));
                }
                free_segments.push_back(i as u32);
            }
        }

        Ok(Self {
            device,
            data_region_start,
            device_size,
            alignment,
            device_id,
            segment_size,
            segment_count,
            segments,
            // Reclaimed-segment free list, rebuilt above from the persisted table's
            // used==0 gaps (v2). Empty for a monotonic (v1 / never-defragged)
            // device.
            free_segments,
            open_segment,
            cursor,
            packed,
            redo_log: None,
            redo_device_id: 0,
        })
    }

    /// Test-only: force the open segment + cursor so `persist` can exercise the
    /// [`SegmentAllocatorError::SegmentTableOverflow`] branch deterministically
    /// without allocating across millions of segments.
    #[doc(hidden)]
    #[cfg(test)]
    fn __test_set_open_segment(&mut self, open_segment: u32) {
        self.open_segment = open_segment;
        self.cursor = self.segment_start(open_segment);
    }
}

/// Map a segment-allocator error into the common [`crate::allocator::AllocatorError`]
/// used by the [`RecordAllocator`] trait. Only the variants reachable from the
/// trait's `Result`-returning methods (allocate/free/persist/reserve) need a
/// precise mapping; constructor-only variants fall back to `CorruptedHeader`.
impl From<SegmentAllocatorError> for crate::allocator::AllocatorError {
    fn from(e: SegmentAllocatorError) -> Self {
        use crate::allocator::AllocatorError as A;
        match e {
            SegmentAllocatorError::DeviceFull { requested, .. } => A::DeviceFull {
                requested,
                largest_free: 0,
            },
            SegmentAllocatorError::InvalidFree { offset, size } => A::InvalidFree { offset, size },
            SegmentAllocatorError::Device(d) => A::Device(d),
            SegmentAllocatorError::Getrandom(g) => A::Getrandom(g),
            SegmentAllocatorError::NoPersistedState => A::NoPersistedState,
            SegmentAllocatorError::HeaderCorruption { expected, actual } => {
                A::HeaderCorruption { expected, actual }
            }
            SegmentAllocatorError::UnsupportedVersion(v) => A::UnsupportedVersion(v),
            SegmentAllocatorError::SegmentTableOverflow { entries, max } => {
                A::FreelistOverflow { entries, max }
            }
            SegmentAllocatorError::InvalidSegmentSize { .. }
            | SegmentAllocatorError::CorruptedHeader(_) => A::CorruptedHeader,
        }
    }
}

impl RecordAllocator for SegmentAllocator {
    fn allocate(&mut self, size: u64) -> crate::allocator::Result<u64> {
        Ok(SegmentAllocator::allocate(self, size)?)
    }
    fn allocate_batch(
        &mut self,
        sizes: &[u64],
    ) -> crate::allocator::Result<Vec<Option<AllocatedRegion>>> {
        SegmentAllocator::allocate_batch(self, sizes)
    }
    fn reserve_batch(&mut self, sizes: &[u64]) -> crate::allocator::Result<PendingBatchAllocation> {
        SegmentAllocator::reserve_batch(self, sizes)
    }
    fn commit_pending(&mut self, pending: PendingBatchAllocation) {
        SegmentAllocator::commit_pending(self, pending)
    }
    fn rollback_pending(&mut self, pending: PendingBatchAllocation) {
        SegmentAllocator::rollback_pending(self, pending)
    }
    fn free(&mut self, offset: u64, size: u64) -> crate::allocator::Result<()> {
        Ok(SegmentAllocator::free(self, offset, size)?)
    }
    fn persist(&self) -> crate::allocator::Result<()> {
        Ok(SegmentAllocator::persist(self)?)
    }
    fn persist_header_no_sync(&self) -> crate::allocator::Result<()> {
        Ok(SegmentAllocator::persist_header_no_sync(self)?)
    }
    fn replay_redo(&mut self, _op: &RedoOp) -> bool {
        // The segment allocator journals no region ops (cursor is recomputed from
        // the index at recovery); the relocate op arrives in increment 4.
        false
    }
    fn is_allocated_range(&self, offset: u64, size: u64) -> bool {
        self.is_allocated_range_impl(offset, size)
    }
    fn free_region_containing(&self, _offset: u64) -> Option<(u64, u64)> {
        // No freelist; dead records are reclaimed by defrag, not tracked as holes.
        None
    }
    fn free_region_count(&self) -> usize {
        0
    }
    fn stats(&self) -> AllocatorStats {
        let s = SegmentAllocator::stats(self);
        let data_capacity = s.device_size.saturating_sub(s.data_region_start);
        AllocatorStats {
            data_region_start: s.data_region_start,
            next_offset: s.cursor,
            device_size: s.device_size,
            alignment: self.alignment,
            free_region_count: 0,
            total_free_bytes: s.dead_bytes,
            largest_free_region: 0,
            used_bytes: s.live_bytes,
            utilization: if data_capacity > 0 {
                s.live_bytes as f64 / data_capacity as f64
            } else {
                0.0
            },
        }
    }
    fn next_offset(&self) -> u64 {
        self.cursor
    }
    fn data_region_start(&self) -> u64 {
        self.data_region_start
    }
    fn device_alignment(&self) -> usize {
        self.alignment
    }
    fn device_id(&self) -> [u8; 16] {
        self.device_id
    }
    fn device_id_hex(&self) -> String {
        self.device_id_hex_impl()
    }
    fn set_redo_log(&mut self, redo_log: Arc<Mutex<RedoLog>>) {
        self.redo_log = Some(redo_log);
    }
    fn set_redo_device_id(&mut self, device_id: u8) {
        self.redo_device_id = device_id;
    }
    fn redo_device_id(&self) -> u8 {
        self.redo_device_id
    }
    fn has_redo_log(&self) -> bool {
        self.redo_log.is_some()
    }
    fn set_packed(&mut self, packed: bool) {
        SegmentAllocator::set_packed(self, packed)
    }
    fn is_packed(&self) -> bool {
        SegmentAllocator::is_packed(self)
    }
    fn set_append_only(&mut self, _append_only: bool) {
        // The segment allocator is inherently append-only (records are placed at
        // the cursor and never reused in place); the flag is a no-op.
    }
    fn is_append_only(&self) -> bool {
        true
    }
    fn is_log_structured(&self) -> bool {
        true
    }
    fn recover_frontier_at_least(&mut self, end: u64) {
        self.set_cursor_at_least(end);
    }
    fn reconcile_recovered_free_list(&mut self, live_offsets: &[u64]) {
        SegmentAllocator::reconcile_recovered_free_list(self, live_offsets);
    }
    fn reclaim_fully_dead_segments(&mut self) -> usize {
        SegmentAllocator::reclaim_fully_dead_segments(self).len()
    }
    fn defrag_victim_ranges(&self, min_dead_frac: f64, max: usize) -> Vec<(u64, u64)> {
        SegmentAllocator::defrag_victim_ranges(self, min_dead_frac, max)
    }
    #[cfg(any(test, feature = "fault-injection"))]
    fn arm_fail_next_persist(&self) {
        // No fault-injection hook on the segment allocator (yet); no-op so the
        // trait object can be used uniformly in tests.
    }
}

/// Read 8 LE bytes at `off`, mapping a short buffer to `CorruptedHeader`.
fn rd8(buf: &[u8], off: usize) -> Result<[u8; 8]> {
    buf.get(off..off + 8)
        .and_then(|s| s.try_into().ok())
        .ok_or(SegmentAllocatorError::CorruptedHeader("header truncated"))
}

/// Read 4 LE bytes at `off`, mapping a short buffer to `CorruptedHeader`.
fn rd4(buf: &[u8], off: usize) -> Result<[u8; 4]> {
    buf.get(off..off + 4)
        .and_then(|s| s.try_into().ok())
        .ok_or(SegmentAllocatorError::CorruptedHeader("header truncated"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemoryDevice;

    const ALIGN: usize = 4096;

    /// A device of `size_mb` MiB at 4 KiB alignment.
    fn dev(size_mb: u64) -> Arc<MemoryDevice> {
        Arc::new(MemoryDevice::new(size_mb * 1024 * 1024, ALIGN).unwrap())
    }

    /// Fresh allocator with an 8 MiB segment size over an `size_mb` MiB device.
    fn alloc(size_mb: u64, segment_size: u64) -> SegmentAllocator {
        SegmentAllocator::new(dev(size_mb), segment_size).unwrap()
    }

    #[test]
    fn new_starts_at_data_region_with_geometry() {
        let a = alloc(64, 8 * 1024 * 1024);
        assert_eq!(a.cursor(), DATA_REGION_OFFSET);
        assert_eq!(a.open_segment(), 0);
        // (64 MiB - 1 MiB header) / 8 MiB segment = 7 whole segments.
        assert_eq!(a.segment_count(), 7);
        assert_eq!(a.segment_size(), 8 * 1024 * 1024);
        let s = a.stats();
        assert_eq!(s.used_bytes, 0);
        assert_eq!(s.live_bytes, 0);
    }

    #[test]
    fn new_rejects_bad_segment_size() {
        // Not a multiple of alignment.
        let e = SegmentAllocator::new(dev(64), 8 * 1024 * 1024 + 1).unwrap_err();
        assert!(matches!(
            e,
            SegmentAllocatorError::InvalidSegmentSize { .. }
        ));
        // Larger than the data region.
        let e = SegmentAllocator::new(dev(8), 64 * 1024 * 1024).unwrap_err();
        assert!(matches!(
            e,
            SegmentAllocatorError::InvalidSegmentSize { .. }
        ));
        // Zero.
        let e = SegmentAllocator::new(dev(64), 0).unwrap_err();
        assert!(matches!(
            e,
            SegmentAllocatorError::InvalidSegmentSize { .. }
        ));
    }

    #[test]
    fn allocate_is_sequential_within_segment() {
        // Non-packed: each reservation rounds up to a 4 KiB block.
        let mut a = alloc(64, 8 * 1024 * 1024);
        let o0 = a.allocate(600).unwrap();
        let o1 = a.allocate(600).unwrap();
        let o2 = a.allocate(4096).unwrap();
        assert_eq!(o0, DATA_REGION_OFFSET);
        assert_eq!(o1, o0 + 4096); // 600 -> one 4 KiB block
        assert_eq!(o2, o1 + 4096);
        assert_eq!(a.cursor(), o2 + 4096);
        assert_eq!(a.open_segment(), 0);
        // All accounted as used in segment 0.
        assert_eq!(a.stats().used_bytes, 3 * 4096);
    }

    #[test]
    fn packed_allocation_is_tightly_sequential() {
        let mut a = alloc(64, 8 * 1024 * 1024);
        a.set_packed(true);
        // Two 600-byte records pack at RECORD_ALIGN (8) within one 4 KiB block.
        let o0 = a.allocate(600).unwrap();
        let o1 = a.allocate(600).unwrap();
        assert_eq!(o0, DATA_REGION_OFFSET);
        // 600 -> 600.div_ceil(8)*8 = 600 (already 8-aligned).
        assert_eq!(o1, o0 + 600);
        assert!(a.is_packed());
    }

    #[test]
    fn packed_allocation_does_not_straddle_device_block() {
        let mut a = alloc(64, 8 * 1024 * 1024);
        a.set_packed(true);
        // Fill a 4 KiB block to near its end, then a record that would straddle.
        // 3000 + 1200 = 4200 > 4096 -> the second record must bump to the next
        // block boundary so it stays within a single block.
        let o0 = a.allocate(3000).unwrap();
        let o1 = a.allocate(1200).unwrap();
        let block = ALIGN as u64;
        assert_eq!(o0, DATA_REGION_OFFSET);
        // o1 must start at the next block boundary (no straddle).
        assert_eq!(o1 % block, 0);
        assert!(o1 % block + 1200 <= block);
        assert_eq!(o1, DATA_REGION_OFFSET + block);
    }

    #[test]
    fn allocate_advances_to_next_segment_when_full() {
        // Tiny segments to force an advance quickly: 2 blocks per segment.
        let seg = 2 * ALIGN as u64; // 8 KiB
        let mut a = alloc(64, seg);
        let o0 = a.allocate(4096).unwrap(); // segment 0, block 0
        let o1 = a.allocate(4096).unwrap(); // segment 0, block 1 (fills it)
        assert_eq!(a.open_segment(), 0);
        let o2 = a.allocate(4096).unwrap(); // must roll into segment 1
        assert_eq!(o0, DATA_REGION_OFFSET);
        assert_eq!(o1, DATA_REGION_OFFSET + 4096);
        assert_eq!(a.open_segment(), 1);
        assert_eq!(o2, a.data_region_start() + seg); // start of segment 1
    }

    #[test]
    fn reclaim_fully_dead_segment_and_reuse_bounds_growth() {
        // 2 blocks/segment. Fill seg0, advance to seg1, then dead-mark ALL of
        // seg0 -> reclaim -> it goes on the free list -> the NEXT segment advance
        // REUSES seg0 instead of growing into seg2. This is what bounds growth.
        let seg = 2 * ALIGN as u64;
        let mut a = alloc(64, seg);
        let o0 = a.allocate(4096).unwrap(); // seg0 b0
        let o1 = a.allocate(4096).unwrap(); // seg0 b1 (full)
        let _o2 = a.allocate(4096).unwrap(); // -> seg1 b0
        assert_eq!(a.open_segment(), 1);

        // Dead-mark all of seg0.
        a.free(o0, 4096).unwrap();
        a.free(o1, 4096).unwrap();
        assert_eq!(a.reclaim_fully_dead_segments(), vec![0]);
        assert_eq!(a.free_segment_count(), 1);
        // seg0's accounting was reset.
        assert_eq!(a.segment_live(0), 0);

        let _o3 = a.allocate(4096).unwrap(); // seg1 b1 (fills seg1)
        assert_eq!(a.open_segment(), 1);
        let o4 = a.allocate(4096).unwrap(); // must REUSE seg0, not grow to seg2
        assert_eq!(
            a.open_segment(),
            0,
            "advance must reuse the reclaimed seg0, not grow into seg2"
        );
        assert_eq!(o4, DATA_REGION_OFFSET, "reused seg0 starts at its base");
        assert_eq!(a.free_segment_count(), 0);
    }

    #[test]
    fn reclaim_skips_open_partially_dead_and_all_live_segments() {
        let seg = 2 * ALIGN as u64;
        let mut a = alloc(64, seg);
        let a0 = a.allocate(4096).unwrap();
        let a1 = a.allocate(4096).unwrap(); // seg0 full (all-live)
        let b0 = a.allocate(4096).unwrap();
        let _b1 = a.allocate(4096).unwrap(); // seg1 full
        let _c0 = a.allocate(4096).unwrap(); // seg2 open (1 live)
        assert_eq!(a.open_segment(), 2);

        a.free(b0, 4096).unwrap(); // seg1 partially dead (1/2)
        // Nothing fully dead yet: seg0 all-live, seg1 partial, seg2 open.
        assert!(a.reclaim_fully_dead_segments().is_empty());

        // Now fully dead-mark seg0.
        a.free(a0, 4096).unwrap();
        a.free(a1, 4096).unwrap();
        // seg0 reclaims; seg1 (partial) and seg2 (open) do NOT.
        assert_eq!(a.reclaim_fully_dead_segments(), vec![0]);
        assert_eq!(a.free_segment_count(), 1);
    }

    #[test]
    fn defrag_victims_ranks_partially_dead_segments_most_dead_first() {
        // 4 blocks/segment so we can make distinct dead fractions.
        let seg = 4 * ALIGN as u64;
        let mut a = alloc(64, seg);
        let mut seg_offsets: Vec<Vec<u64>> = Vec::new();
        for _ in 0..3 {
            let mut offs = Vec::new();
            for _ in 0..4 {
                offs.push(a.allocate(4096).unwrap());
            }
            seg_offsets.push(offs);
        }
        let _open = a.allocate(4096).unwrap(); // seg3 open
        assert_eq!(a.open_segment(), 3);

        // seg0: dead 1/4; seg1: dead 3/4; seg2: dead 2/4.
        a.free(seg_offsets[0][0], 4096).unwrap();
        for i in 0..3 {
            a.free(seg_offsets[1][i], 4096).unwrap();
        }
        for i in 0..2 {
            a.free(seg_offsets[2][i], 4096).unwrap();
        }

        // Most-dead-first: seg1 (0.75), seg2 (0.5), seg0 (0.25).
        assert_eq!(a.defrag_victims(0.0), vec![1, 2, 0]);
        // A threshold filters the low-dead segment.
        assert_eq!(a.defrag_victims(0.5), vec![1, 2]);
        // Dead-mark seg0's remaining 3 blocks so it is now FULLY dead (4/4) ->
        // excluded from victims (it goes the free reclaim path, not compaction).
        for i in 1..4 {
            a.free(seg_offsets[0][i], 4096).unwrap();
        }
        assert!(!a.defrag_victims(0.0).contains(&0));
    }

    #[test]
    fn record_never_straddles_segment_boundary() {
        // 3 blocks per segment; a 2-block record placed after 2 blocks would
        // straddle the boundary, so it must roll into the next segment.
        let seg = 3 * ALIGN as u64; // 12 KiB
        let mut a = alloc(64, seg);
        let _ = a.allocate(4096).unwrap(); // seg0 block0
        let _ = a.allocate(4096).unwrap(); // seg0 block1 (1 block left in seg0)
        let o = a.allocate(2 * 4096).unwrap(); // 2 blocks: won't fit -> seg1
        assert_eq!(a.open_segment(), 1);
        assert_eq!(o, a.data_region_start() + seg);
    }

    #[test]
    fn allocate_device_full_when_segments_exhausted() {
        // 9 MiB device -> 8 MiB usable -> with 8 MiB segments, exactly 1 segment.
        let seg = 8 * 1024 * 1024;
        let mut a = alloc(9, seg);
        assert_eq!(a.segment_count(), 1);
        // Fill the single segment with 4 KiB blocks.
        let blocks = seg / 4096;
        for _ in 0..blocks {
            a.allocate(4096).unwrap();
        }
        let e = a.allocate(4096).unwrap_err();
        assert!(matches!(e, SegmentAllocatorError::DeviceFull { .. }));
    }

    #[test]
    fn allocate_rejects_record_larger_than_segment() {
        let seg = 2 * ALIGN as u64; // 8 KiB
        let mut a = alloc(64, seg);
        let e = a.allocate(seg + 1).unwrap_err();
        assert!(matches!(e, SegmentAllocatorError::DeviceFull { .. }));
    }

    #[test]
    fn free_accounts_dead_bytes() {
        let mut a = alloc(64, 8 * 1024 * 1024);
        let o0 = a.allocate(600).unwrap();
        let _o1 = a.allocate(600).unwrap();
        assert_eq!(a.stats().used_bytes, 2 * 4096);
        assert_eq!(a.stats().dead_bytes, 0);
        a.free(o0, 600).unwrap();
        let s = a.stats();
        assert_eq!(s.dead_bytes, 4096); // non-packed: rounded to a block
        assert_eq!(s.live_bytes, 4096); // 2 used - 1 dead block
    }

    #[test]
    fn free_attributes_dead_to_owning_segment() {
        let seg = 2 * ALIGN as u64;
        let mut a = alloc(64, seg);
        let _ = a.allocate(4096).unwrap(); // seg0
        let _ = a.allocate(4096).unwrap(); // seg0 (fills)
        let o2 = a.allocate(4096).unwrap(); // seg1
        assert_eq!(a.segment_of(o2), Some(1));
        a.free(o2, 4096).unwrap();
        // Dead lands in segment 1 (where o2 lives), total dead == 1 block.
        assert_eq!(a.stats().dead_bytes, 4096);
    }

    #[test]
    fn free_rejects_out_of_range() {
        let mut a = alloc(64, 8 * 1024 * 1024);
        // Before the data region.
        let e = a.free(0, 4096).unwrap_err();
        assert!(matches!(e, SegmentAllocatorError::InvalidFree { .. }));
        // Past the device end.
        let e = a.free(a.device_size - 10, 4096).unwrap_err();
        assert!(matches!(e, SegmentAllocatorError::InvalidFree { .. }));
    }

    #[test]
    fn persist_then_recover_roundtrips_state() {
        let device = dev(64);
        let seg = 2 * ALIGN as u64; // small segments so we cross a boundary
        let mut a = SegmentAllocator::new(device.clone(), seg).unwrap();
        a.set_packed(true);
        let o0 = a.allocate(4096).unwrap();
        let _ = a.allocate(4096).unwrap(); // fills segment 0
        let o2 = a.allocate(4096).unwrap(); // segment 1
        a.free(o0, 4096).unwrap();
        let before = a.stats();
        let dev_id = a.device_id();
        a.persist().unwrap();

        let b = SegmentAllocator::recover(device).unwrap();
        assert_eq!(b.cursor(), a.cursor());
        assert_eq!(b.open_segment(), 1);
        assert_eq!(b.segment_size(), seg);
        assert_eq!(b.segment_count(), a.segment_count());
        assert!(b.is_packed());
        assert_eq!(b.device_id(), dev_id);
        assert_eq!(b.stats(), before);
        // The recovered allocator continues appending from the cursor.
        let mut b = b;
        let o3 = b.allocate(4096).unwrap();
        assert_eq!(o3, o2 + 4096);
    }

    #[test]
    fn persist_recover_roundtrips_a_reused_nonmonotonic_layout() {
        // v2: a defrag-reclaimed low segment (a used==0 gap below the highest-used
        // segment) must round-trip, and the recovered allocator must reuse it —
        // so bounded growth survives a restart.
        let device = dev(64);
        let seg = 2 * ALIGN as u64;
        let mut a = SegmentAllocator::new(device.clone(), seg).unwrap();
        let o0 = a.allocate(4096).unwrap();
        let o1 = a.allocate(4096).unwrap(); // seg0 full
        let _ = a.allocate(4096).unwrap(); // seg1 b0
        let _ = a.allocate(4096).unwrap(); // seg1 full
        let _ = a.allocate(4096).unwrap(); // seg2 open
        assert_eq!(a.open_segment(), 2);
        a.free(o0, 4096).unwrap();
        a.free(o1, 4096).unwrap();
        assert_eq!(a.reclaim_fully_dead_segments(), vec![0]);
        assert_eq!(a.free_segment_count(), 1);
        let before = a.stats();
        a.persist().unwrap();

        let mut b = SegmentAllocator::recover(device).unwrap();
        assert_eq!(b.open_segment(), 2);
        assert_eq!(b.cursor(), a.cursor());
        assert_eq!(b.stats(), before, "used/dead/live round-trip");
        assert_eq!(
            b.free_segment_count(),
            1,
            "the reclaimed-seg0 gap must be rebuilt into the free list"
        );
        // Fill seg2, then the next advance must REUSE seg0 (not grow to seg3).
        let _ = b.allocate(4096).unwrap(); // seg2 b1 (fills seg2)
        assert_eq!(b.open_segment(), 2);
        let o = b.allocate(4096).unwrap();
        assert_eq!(b.open_segment(), 0, "reclaimed seg0 reused after recovery");
        assert_eq!(o, DATA_REGION_OFFSET);
        assert_eq!(b.free_segment_count(), 0);
    }

    #[test]
    fn reconcile_recovered_free_list_rebuilds_holes_from_the_live_set() {
        // Crash recovery: the header restored the cursor/open segment, but the
        // free list must be rebuilt from the LIVE index — a used segment holding
        // no live record is a defrag hole to reclaim.
        let seg = 2 * ALIGN as u64;
        let mut a = alloc(64, seg);
        let o0 = a.allocate(4096).unwrap(); // seg0 b0
        let _o1 = a.allocate(4096).unwrap(); // seg0 b1
        let o2 = a.allocate(4096).unwrap(); // seg1 b0
        let _o3 = a.allocate(4096).unwrap(); // seg1 b1
        let o4 = a.allocate(4096).unwrap(); // seg2 open
        assert_eq!(a.open_segment(), 2);
        assert_eq!(a.free_segment_count(), 0);

        // Live records survive only in seg1 (o2) and the open seg2 (o4); seg0's
        // records (o0, o1) have relocated away (dead). Reconcile from that set.
        let _ = o0;
        a.reconcile_recovered_free_list(&[o2, o4]);
        assert_eq!(
            a.free_segment_count(),
            1,
            "seg0 (no live) becomes a free hole"
        );
        assert_eq!(a.segment_live(0), 0);

        // The reclaimed hole is reused: fill seg2, next advance goes to seg0.
        let _ = a.allocate(4096).unwrap(); // seg2 b1 (fills seg2)
        let o = a.allocate(4096).unwrap();
        assert_eq!(
            a.open_segment(),
            0,
            "reconciled free seg0 reused after recovery"
        );
        assert_eq!(o, DATA_REGION_OFFSET);
    }

    #[test]
    fn recover_fresh_device_returns_no_persisted_state() {
        let device = dev(64);
        let r = SegmentAllocator::recover(device);
        assert!(matches!(r, Err(SegmentAllocatorError::NoPersistedState)));
    }

    #[test]
    fn recover_rejects_foreign_header() {
        // Write a non-zero, non-SEG magic at offset 0 (e.g. an in-place engine
        // header would have a different magic): must fail closed, not be read
        // as fresh.
        let device = dev(64);
        let mut buf = AlignedBuf::new(ALIGN, ALIGN);
        buf[0..8].copy_from_slice(&0x5445_5241_414C_4C43u64.to_le_bytes()); // SlotAllocator magic
        device.pwrite_all_at(&buf, 0).unwrap();
        let r = SegmentAllocator::recover(device);
        assert!(matches!(r, Err(SegmentAllocatorError::CorruptedHeader(_))));
    }

    #[test]
    fn recover_detects_crc_corruption() {
        let device = dev(64);
        let seg = 8 * 1024 * 1024;
        let mut a = SegmentAllocator::new(device.clone(), seg).unwrap();
        a.allocate(4096).unwrap();
        a.persist().unwrap();
        // Flip a byte in the persisted cursor field.
        let mut buf = AlignedBuf::new(ALIGN, ALIGN);
        device.pread_exact_at(&mut buf, 0).unwrap();
        buf[OFF_CURSOR] ^= 0xFF;
        device.pwrite_all_at(&buf, 0).unwrap();
        let r = SegmentAllocator::recover(device);
        assert!(matches!(
            r,
            Err(SegmentAllocatorError::HeaderCorruption { .. })
        ));
    }

    #[test]
    fn persist_segment_table_overflow_is_loud() {
        // Force open_segment beyond the header capacity and assert persist
        // refuses rather than truncating. Use a device/segment geometry with
        // more segments than the header can hold.
        let small_seg = ALIGN as u64; // 4 KiB segments -> very many segments
        // Device large enough to have > MAX_PERSISTED_SEGMENTS segments.
        let need_bytes = DATA_REGION_OFFSET + (MAX_PERSISTED_SEGMENTS as u64 + 2) * small_seg;
        let device = Arc::new(MemoryDevice::new(need_bytes, ALIGN).unwrap());
        let mut a = SegmentAllocator::new(device, small_seg).unwrap();
        assert!(a.segment_count() as usize > MAX_PERSISTED_SEGMENTS);
        a.__test_set_open_segment(MAX_PERSISTED_SEGMENTS as u32); // entry_count = MAX+1
        let e = a.persist().unwrap_err();
        assert!(matches!(
            e,
            SegmentAllocatorError::SegmentTableOverflow { .. }
        ));
    }

    // -- RecordAllocator trait surface (increment 3) ------------------------

    #[test]
    fn reserve_batch_commit_advances_cursor_sequentially() {
        let mut a = alloc(64, 8 * 1024 * 1024);
        let pending = a.reserve_batch(&[600, 600, 4096]).unwrap();
        let regions: Vec<_> = pending.regions.iter().flatten().copied().collect();
        assert_eq!(regions.len(), 3);
        // Non-packed: each rounds to a 4 KiB block, contiguous.
        assert_eq!(regions[0].offset, DATA_REGION_OFFSET);
        assert_eq!(regions[1].offset, regions[0].offset + 4096);
        assert_eq!(regions[2].offset, regions[1].offset + 4096);
        // No region redo ops (segment recovers its cursor from the index).
        assert!(pending.allocate_region_redo_ops().is_empty());
        let cursor_after = a.cursor();
        a.commit_pending(pending);
        assert_eq!(a.cursor(), cursor_after, "commit does not move the cursor");
    }

    #[test]
    fn reserve_batch_rollback_restores_state_across_segment_boundary() {
        // 2 blocks per segment; pre-allocate 1 block, then a 3-block batch that
        // fills segment 0 and crosses into segment 1.
        let seg = 2 * ALIGN as u64;
        let mut a = alloc(64, seg);
        a.allocate(4096).unwrap(); // seg0 block0
        let pre_cursor = a.cursor();
        let pre_open = a.open_segment();
        let pre_stats = a.stats();
        let pending = a.reserve_batch(&[4096, 4096, 4096]).unwrap();
        assert!(
            a.open_segment() > pre_open,
            "batch must have crossed a segment boundary"
        );
        a.rollback_pending(pending);
        assert_eq!(a.cursor(), pre_cursor);
        assert_eq!(a.open_segment(), pre_open);
        assert_eq!(a.stats(), pre_stats, "used accounting fully restored");
        // The cursor is reusable: the next allocate lands where the batch did.
        let o = a.allocate(4096).unwrap();
        assert_eq!(o, pre_cursor);
    }

    #[test]
    fn trait_object_allocate_free_stats() {
        use crate::allocator::RecordAllocator;
        let mut a: Box<dyn RecordAllocator> = Box::new(alloc(64, 8 * 1024 * 1024));
        let o0 = a.allocate(600).unwrap();
        assert_eq!(o0, DATA_REGION_OFFSET);
        assert_eq!(a.next_offset(), o0 + 4096); // non-packed rounds to a block
        assert_eq!(a.data_region_start(), DATA_REGION_OFFSET);
        assert!(a.is_allocated_range(o0, 600));
        // In-device bounds check (NOT cursor-gated — see is_allocated_range_impl):
        // an in-device offset above the cursor is still "valid" (recovery needs
        // this); an offset before the data region or past the device is not.
        assert!(a.is_allocated_range(a.next_offset(), 4096)); // in-device, above cursor
        assert!(!a.is_allocated_range(0, 4096)); // before the data region
        a.free(o0, 600).unwrap();
        let s = a.stats();
        assert_eq!(s.next_offset, a.next_offset());
        assert_eq!(s.total_free_bytes, 4096); // freed block counted as dead
        assert_eq!(s.free_region_count, 0); // no freelist
        assert!(a.is_append_only());
        assert_eq!(a.free_region_containing(o0), None);
    }

    #[test]
    fn set_cursor_at_least_advances_and_rederives_open_segment() {
        let seg = 2 * ALIGN as u64; // 8 KiB, 2 blocks/segment
        let mut a = alloc(64, seg);
        assert_eq!(a.open_segment(), 0);
        // Advance into segment 3 (data_region + 3 segments + 1 block).
        let target = a.data_region_start() + 3 * seg + ALIGN as u64;
        a.set_cursor_at_least(target);
        assert_eq!(a.cursor(), target);
        assert_eq!(a.open_segment(), 3);
        // A subsequent allocate appends from the recovered cursor.
        let o = a.allocate(4096).unwrap();
        assert_eq!(o, target);
    }

    #[test]
    fn set_cursor_at_least_is_monotonic_and_clamped() {
        let mut a = alloc(64, 8 * 1024 * 1024);
        a.allocate(4096).unwrap();
        let c = a.cursor();
        a.set_cursor_at_least(c - 1); // below cursor: no-op
        assert_eq!(a.cursor(), c);
        a.set_cursor_at_least(0); // way below: no-op
        assert_eq!(a.cursor(), c);
        // Past the device end: clamped to device_size.
        a.set_cursor_at_least(u64::MAX);
        assert_eq!(a.cursor(), a.stats().device_size);
    }

    #[test]
    fn recover_frontier_via_trait_advances_segment_cursor() {
        use crate::allocator::RecordAllocator;
        let mut a: Box<dyn RecordAllocator> = Box::new(alloc(64, 8 * 1024 * 1024));
        let target = DATA_REGION_OFFSET + 5 * 4096;
        a.recover_frontier_at_least(target);
        assert_eq!(a.next_offset(), target);
        // Append resumes from the recovered frontier (no overwrite of [.., target)).
        let o = a.allocate(4096).unwrap();
        assert_eq!(o, target);
    }

    #[test]
    fn trait_device_full_maps_to_allocator_error() {
        use crate::allocator::{AllocatorError, RecordAllocator};
        // 9 MiB device, 8 MiB segment → exactly one segment.
        let mut a: Box<dyn RecordAllocator> = Box::new(alloc(9, 8 * 1024 * 1024));
        let blocks = 8 * 1024 * 1024u64 / 4096;
        for _ in 0..blocks {
            a.allocate(4096).unwrap();
        }
        let e = a.allocate(4096).unwrap_err();
        assert!(
            matches!(e, AllocatorError::DeviceFull { .. }),
            "segment DeviceFull must map to AllocatorError::DeviceFull, got {e:?}"
        );
    }
}
