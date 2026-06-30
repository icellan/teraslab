//! Device space allocator for TeraSlab records.
//!
//! Manages free space on a block device using a freelist. Allocations are
//! aligned to the device's minimum I/O size. Freed regions are merged with
//! adjacent free regions to reduce fragmentation.

use crate::device::{AlignedBuf, BlockDevice, DeviceError};
use crate::metrics::allocator_metrics;
use crate::redo::{RedoLog, RedoOp};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Start of the data region on device. The region before this is reserved for
/// the device header (freelist checkpoint, device metadata).
pub const DATA_REGION_OFFSET: u64 = 1024 * 1024; // 1 MiB reserved for header

/// Small reservation alignment used in packed mode (for struct field access),
/// applied instead of rounding every reservation up to the device block.
///
/// In packed mode a small record (`size <= device block`) is rounded up to this
/// boundary and packed contiguously within a single device block, instead of
/// being rounded up to the full `device_alignment` (typically 4096) as the
/// default non-packed mode does. See `docs/PACKED_RECORD_STORAGE_DESIGN.md`
/// §3.1.
pub const RECORD_ALIGN: u64 = 8;

/// Magic number for the allocator header on device.
const ALLOCATOR_MAGIC: u64 = 0x5445_5241_414C_4C43; // "TERAALLC"

/// Current header version. Stored at bytes 40..44 so `recover()` can reject
/// incompatible on-disk formats written by future builds.
///
/// This is the NON-PACKED (default) layout version. A packed device stamps
/// [`HEADER_VERSION_PACKED`] instead; see [`SlotAllocator::persist`] and
/// [`SlotAllocator::recover`].
const HEADER_VERSION: u32 = 1;

/// On-disk header version stamped by a PACKED allocator (sub-4 KiB record
/// offsets, packed within a device block — see
/// `docs/PACKED_RECORD_STORAGE_DESIGN.md`).
///
/// Packing must be persisted so a device's format wins over config across
/// restarts: reopening a packed device in non-packed mode would make `free()`
/// `align_up` to the full device block and over-free packed block-neighbours
/// (silent corruption). `recover` restores packed-ness from this marker; an
/// older binary that only knows [`HEADER_VERSION`] sees `version > max_known`
/// and fails CLOSED with [`AllocatorError::UnsupportedVersion`] rather than
/// misreading a packed device non-packed.
const HEADER_VERSION_PACKED: u32 = 2;

/// Byte offset of the header CRC32 field (little-endian u32). Computed over
/// the header bytes from offset 0 up through the freelist entries, with the
/// CRC field itself zeroed during computation.
const HEADER_CRC_OFFSET: usize = 44;

/// Byte offset where freelist entries begin in the header.
const FREELIST_OFFSET: usize = 48;

/// Maximum number of free regions that can be serialized to the device header.
///
/// Exposed publicly so observability surfaces (and the F-G1-009
/// regression test) can compare the current freelist size against the
/// persist-time cap.
pub const MAX_PERSISTED_FREE_REGIONS: usize = (DATA_REGION_OFFSET as usize - FREELIST_OFFSET) / 16;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by the allocator.
#[derive(Error, Debug)]
pub enum AllocatorError {
    /// The device has no free space large enough for the requested allocation.
    #[error(
        "device full: requested {requested} bytes, largest free region is {largest_free} bytes"
    )]
    DeviceFull { requested: u64, largest_free: u64 },

    /// A device I/O error occurred.
    #[error("device error: {0}")]
    Device(#[from] DeviceError),

    /// The freelist on device is corrupted.
    #[error("corrupted freelist header")]
    CorruptedHeader,

    /// Attempted to free a region that is outside the data area.
    #[error("invalid free: offset {offset} + size {size} outside data region")]
    InvalidFree { offset: u64, size: u64 },

    /// IJ-7: attempted to free a region `[offset, offset + size)` that
    /// overlaps an already-free region `[free_offset, free_offset +
    /// free_size)`. A double-free (or a free of a range that overlaps the
    /// freelist) would corrupt the freelist's disjoint-region invariant —
    /// the overlapping bytes would be counted free twice and could later be
    /// handed out to two distinct allocations (double-allocation). The
    /// allocator rejects it instead of silently merging the overlap.
    #[error(
        "double free: range [{offset}, {offset}+{size}) overlaps free region [{free_offset}, {free_offset}+{free_size})"
    )]
    DoubleFree {
        /// Start offset of the rejected free.
        offset: u64,
        /// Aligned size of the rejected free.
        size: u64,
        /// Start offset of the existing free region it overlaps.
        free_offset: u64,
        /// Size of the existing free region it overlaps.
        free_size: u64,
    },

    /// Failed to generate random bytes for device identity.
    #[error("failed to generate device identity: {0}")]
    Getrandom(getrandom::Error),

    /// The on-disk header version is not supported by this build.
    #[error("unsupported header version: {0}")]
    UnsupportedVersion(u32),

    /// The on-disk header's CRC32 checksum did not match the expected value
    /// computed over the header bytes (with the CRC field zeroed during
    /// computation). This indicates on-disk corruption — torn write, bit
    /// rot, or tampering — and recovery cannot safely use the header.
    #[error(
        "allocator header corruption: CRC mismatch (expected={expected:#010x}, actual={actual:#010x})"
    )]
    HeaderCorruption { expected: u32, actual: u32 },

    /// The header region is all zeros — the device has never had an
    /// allocator header persisted to it (a genuinely fresh device).
    ///
    /// Returned by [`SlotAllocator::recover`] so callers can distinguish
    /// "fresh device, safe to initialize with [`SlotAllocator::new`]" from
    /// every corruption form ([`AllocatorError::CorruptedHeader`],
    /// [`AllocatorError::HeaderCorruption`],
    /// [`AllocatorError::UnsupportedVersion`]), which must fail closed:
    /// falling back to a fresh allocator over a device with persisted
    /// state would re-allocate live regions and silently overwrite
    /// records (audit B-2).
    #[error("no persisted allocator state: header region is all zeros (fresh device)")]
    NoPersistedState,

    /// Appending or fsyncing the allocator's redo journal entry failed.
    ///
    /// Returned by [`SlotAllocator::allocate`] and [`SlotAllocator::free`]
    /// when a redo log is attached and the journal write fails. On this
    /// error the in-memory allocator state is rolled back — the reservation
    /// (or free) is undone — so the caller sees no externally-visible
    /// effect. The caller must treat the operation as not having happened.
    #[error("redo log failure: {detail}")]
    RedoLogFailure { detail: String },

    /// The freelist has more entries than the on-device header can store
    /// (`MAX_PERSISTED_FREE_REGIONS`). [`SlotAllocator::persist`] previously
    /// silently truncated the list to the first N entries (in offset
    /// order), permanently leaking the tail regions on the next
    /// `recover()`. F-G1-009: persist now returns this variant explicitly
    /// so the operator can react (compact, grow the header region, or
    /// add an overflow-chain implementation) before space is leaked.
    #[error("freelist overflow: {entries} entries, max persistable is {max}")]
    FreelistOverflow { entries: usize, max: usize },

    /// Test/fault-injection only: a `persist` call was deliberately failed
    /// via [`SlotAllocator::arm_fail_next_persist`]. Never returned in
    /// production builds (the variant and its trigger are compiled out
    /// unless `cfg(test)` or the `fault-injection` feature is active).
    #[cfg(any(test, feature = "fault-injection"))]
    #[error("allocator persist fault-injected")]
    PersistFaultInjected,
}

/// Result type for allocator operations.
pub type Result<T> = std::result::Result<T, AllocatorError>;

// ---------------------------------------------------------------------------
// FreeRegion
// ---------------------------------------------------------------------------

/// A contiguous free region on device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FreeRegion {
    offset: u64,
    size: u64,
}

// ---------------------------------------------------------------------------
// Hybrid freelist
// ---------------------------------------------------------------------------

/// Entry count at which the freelist promotes from Vec to BTree.
const PROMOTE_THRESHOLD: usize = 64;
/// Entry count at which the freelist demotes from BTree back to Vec.
const DEMOTE_THRESHOLD: usize = 32;

// ---------------------------------------------------------------------------
// AllocatorStats
// ---------------------------------------------------------------------------

/// Summary statistics for the device space allocator.
///
/// Returned by [`SlotAllocator::stats`] for observability endpoints.
#[derive(Debug, Clone)]
pub struct AllocatorStats {
    /// Start of the data region on device (bytes).
    pub data_region_start: u64,
    /// Current high-water mark — all allocations are below this offset.
    pub next_offset: u64,
    /// Total device size in bytes.
    pub device_size: u64,
    /// Device I/O alignment in bytes.
    pub alignment: usize,
    /// Number of free regions in the freelist.
    pub free_region_count: usize,
    /// Total free bytes across all freelist regions.
    pub total_free_bytes: u64,
    /// Size of the largest contiguous free region in bytes.
    pub largest_free_region: u64,
    /// Bytes used by allocated data (high-water minus data start minus free).
    pub used_bytes: u64,
    /// Device utilization as a fraction (0.0–1.0).
    pub utilization: f64,
}

/// A successfully reserved device-space region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocatedRegion {
    /// Device byte offset of the reserved region.
    pub offset: u64,
    /// Aligned reservation size in bytes.
    pub size: u64,
}

/// A batch of regions reserved IN MEMORY but not yet durably journaled, returned
/// by [`SlotAllocator::reserve_batch`]. The freelist already reflects the
/// reservations; the caller must journal [`Self::allocate_region_redo_ops`]
/// (atomically with its own redo, e.g. `Create`) and then call
/// [`SlotAllocator::commit_pending`], or call
/// [`SlotAllocator::rollback_pending`] if it cannot. See `reserve_batch` for the
/// issue-#14 orphan-prevention rationale.
#[must_use = "a PendingBatchAllocation must be passed to commit_pending or rollback_pending"]
pub struct PendingBatchAllocation {
    /// Per-input result in `sizes` order: `Some(region)` when reserved, `None`
    /// when the device was full for that size.
    pub regions: Vec<Option<AllocatedRegion>>,
    /// In-memory rollback handle so `rollback_pending` can undo the reservation.
    /// Variant determined by the producing allocator (each allocator only ever
    /// receives back a `PendingBatchAllocation` it produced). `pub(crate)` so the
    /// segment allocator (a sibling module) can build/destructure it.
    pub(crate) rollback: BatchRollback,
    /// `AllocateRegion` redo entries the caller must journal before committing.
    /// Empty for the segment allocator (which recovers its cursor from the index,
    /// so it journals no region ops — see `segment_allocator`).
    pub(crate) alloc_redo_ops: Vec<RedoOp>,
}

/// The allocator-specific in-memory rollback handle carried by a
/// [`PendingBatchAllocation`]. The in-place [`SlotAllocator`] undoes each
/// reservation individually (freelist re-insert or high-water reset); the
/// append-cursor [`crate::segment_allocator::SegmentAllocator`] restores a single
/// pre-batch cursor snapshot (the open segment is derivable from the cursor).
pub(crate) enum BatchRollback {
    /// In-place: per-reservation undo tokens, undone in reverse order.
    Slot(Vec<(u64, Reservation)>),
    /// Append-cursor: restore the cursor + the open segment's `used` to their
    /// pre-batch values (the open segment index is derivable from the cursor).
    Segment {
        /// Cursor before the batch.
        pre_cursor: u64,
        /// Open segment before the batch.
        pre_open_segment: u32,
        /// `used` of `pre_open_segment` before the batch.
        pre_open_used: u64,
    },
}

impl PendingBatchAllocation {
    /// The `AllocateRegion` redo entries to journal (atomically with the
    /// caller's own redo ops) before calling
    /// [`SlotAllocator::commit_pending`].
    pub fn allocate_region_redo_ops(&self) -> &[RedoOp] {
        &self.alloc_redo_ops
    }
}

// ---------------------------------------------------------------------------
// SlotAllocator
// ---------------------------------------------------------------------------

/// Manages device space allocation using a hybrid freelist.
///
/// Allocations are aligned to the device's minimum I/O alignment.
/// Freed regions are coalesced with adjacent free regions.
///
/// Internally uses a hybrid freelist:
/// - **Small** (≤64 entries): `Vec<FreeRegion>` sorted by offset — fast constant
///   overhead for the common case of few free regions.
/// - **Large** (>64 entries): dual `BTreeMap`/`BTreeSet` index — O(log n)
///   best-fit and coalescing for heavily fragmented devices.
///
/// Automatic promotion at 64 entries; demotion back to Vec at 32 entries
/// (hysteresis avoids thrashing near the boundary).
pub struct SlotAllocator {
    device: Arc<dyn BlockDevice>,
    freelist: FreelistBackend,
    /// Next append point for new allocations (grows upward).
    next_offset: u64,
    data_region_start: u64,
    device_size: u64,
    alignment: usize,
    /// 128-bit device identity, generated at format time and persisted in
    /// the superblock header at bytes 24..40.
    device_id: [u8; 16],
    /// Optional redo log handle for journaling allocate/free operations.
    ///
    /// When attached (via [`SlotAllocator::set_redo_log`]), every
    /// [`SlotAllocator::allocate`] and [`SlotAllocator::free`] appends and
    /// fsyncs a [`RedoOp::AllocateRegion`] or [`RedoOp::FreeRegion`] entry
    /// BEFORE the caller observes any effect. On crash, replay of these
    /// entries rebuilds the freelist and high-water mark so allocations
    /// and frees survive a power loss between [`SlotAllocator::persist`]
    /// snapshots.
    redo_log: Option<Arc<Mutex<RedoLog>>>,
    /// Store identifier this allocator owns, written into every
    /// `AllocateRegion`/`FreeRegion` redo entry it emits.
    ///
    /// 0 for a single-store deployment; set per store via
    /// [`SlotAllocator::set_redo_device_id`] at boot under `device_split` so
    /// recovery routes each region to the owning store's allocator and the
    /// entry to that store's redo log.
    redo_device_id: u8,
    /// Opt-in packed allocation mode (default `false`).
    ///
    /// When `false` (the default), every reservation is rounded up to the
    /// device block (`alignment`) exactly as before — byte-for-byte unchanged.
    /// When `true`, small reservations (`size <= alignment`) are rounded up only
    /// to [`RECORD_ALIGN`] and packed contiguously within a single device block
    /// (never straddling a block boundary), while large reservations (`size >
    /// alignment`) stay device-block-granular and block-aligned. See
    /// [`Self::set_packed`] and `docs/PACKED_RECORD_STORAGE_DESIGN.md` §3.1.
    packed: bool,
    /// Append-only allocation mode (default `false`).
    ///
    /// When `true`, [`Self::reserve_aligned`] never consults the freelist —
    /// every allocation extends the high-water mark, so records are placed
    /// strictly sequentially even as deletes free regions. Freed regions are
    /// still journaled and inserted into the freelist (so recovery replay and
    /// accounting are unchanged) but are NEVER handed back out. This is the
    /// Phase 1 log-structured write lever; see [`Self::set_append_only`] and
    /// `bench/results/LOG_STRUCTURED_DATA_LAYER_DESIGN.md`.
    ///
    /// Unlike [`Self::packed`], this is a pure placement policy: it does not
    /// affect the on-disk format and is NOT persisted in the header — a device
    /// can be reopened in either mode.
    append_only: bool,
    /// Test/fault-injection only: fail the next [`SlotAllocator::persist`]
    /// call with [`AllocatorError::PersistFaultInjected`], then auto-clear.
    ///
    /// Used by the checkpoint crash tests to drive `persist_allocator` to
    /// return `Err` AFTER the snapshot has already been renamed but BEFORE
    /// the recovery-progress fence is written, exercising the
    /// "no-fence ⇒ full redo replay re-derives the freelist" self-healing
    /// invariant. `Cell` so it can be flipped through `&self` (`persist`
    /// takes `&self`). Compiled out of production builds — only present
    /// under `cfg(test)` or the `fault-injection` feature, so it never
    /// affects release behaviour.
    #[cfg(any(test, feature = "fault-injection"))]
    fail_next_persist: std::cell::Cell<bool>,
}

/// Hybrid freelist backend: Vec for small, BTree for large.
enum FreelistBackend {
    /// Sorted by offset. Linear scan for best-fit, binary search for insert.
    Small(Vec<FreeRegion>),
    /// Dual-indexed: by_offset for coalescing, by_size for O(log n) best-fit.
    Large {
        by_offset: std::collections::BTreeMap<u64, u64>,
        by_size: std::collections::BTreeSet<(u64, u64)>,
    },
}

// Source of an in-memory reservation. Kept so redo failures can roll the
// allocator state back before the caller observes an unjournaled offset.
// `pub(crate)` because it appears in the `pub(crate)` [`BatchRollback::Slot`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum Reservation {
    FromFreelist {
        /// The allocation's returned offset (= region start).
        alloc_offset: u64,
        /// The original region's full size (needed for rollback if we split
        /// and took the head).
        region_size: u64,
    },
    FromHighWater {
        previous_next_offset: u64,
    },
}

impl FreelistBackend {
    fn new() -> Self {
        Self::Small(Vec::new())
    }

    fn len(&self) -> usize {
        match self {
            Self::Small(v) => v.len(),
            Self::Large { by_offset, .. } => by_offset.len(),
        }
    }

    /// Iterate (offset, size) pairs in offset order. Used for persist/stats.
    fn iter_offset_order(&self) -> Box<dyn Iterator<Item = (u64, u64)> + '_> {
        match self {
            Self::Small(v) => Box::new(v.iter().map(|r| (r.offset, r.size))),
            Self::Large { by_offset, .. } => Box::new(by_offset.iter().map(|(&o, &s)| (o, s))),
        }
    }

    fn largest(&self) -> u64 {
        match self {
            Self::Small(v) => v.iter().map(|r| r.size).max().unwrap_or(0),
            Self::Large { by_size, .. } => {
                by_size.iter().next_back().map(|&(sz, _)| sz).unwrap_or(0)
            }
        }
    }

    /// Best-fit allocation. Returns `Some((offset, region_size))` or `None`.
    ///
    /// `block` carries the packed-mode placement constraint:
    /// - `None` → non-packed: any region of sufficient size qualifies
    ///   (behavior unchanged).
    /// - `Some(block)` → packed: a region qualifies only if the head
    ///   allocation `[region.offset, region.offset + aligned_size)` stays
    ///   within a single `block` (small, `aligned_size <= block`) or starts
    ///   block-aligned (large, `aligned_size > block`). Regions that would
    ///   straddle a block boundary or misalign a large record are skipped so
    ///   the caller falls through to the high-water path.
    fn best_fit(&mut self, aligned_size: u64, block: Option<u64>) -> Option<(u64, u64)> {
        // Does placing `aligned_size` at `offset` satisfy the packed
        // within-one-block (small) / block-aligned (large) constraint?
        let qualifies = |offset: u64| -> bool {
            match block {
                None => true,
                Some(block) => {
                    if aligned_size <= block {
                        offset % block + aligned_size <= block
                    } else {
                        offset.is_multiple_of(block)
                    }
                }
            }
        };
        let result = match self {
            Self::Small(v) => {
                let mut best_idx: Option<usize> = None;
                let mut best_waste: u64 = u64::MAX;
                for (i, region) in v.iter().enumerate() {
                    if region.size >= aligned_size && qualifies(region.offset) {
                        let waste = region.size - aligned_size;
                        if waste < best_waste {
                            best_waste = waste;
                            best_idx = Some(i);
                            if waste == 0 {
                                break;
                            }
                        }
                    }
                }
                let idx = best_idx?;
                let region = v[idx];
                if region.size == aligned_size {
                    v.remove(idx);
                } else {
                    v[idx] = FreeRegion {
                        offset: region.offset + aligned_size,
                        size: region.size - aligned_size,
                    };
                }
                Some((region.offset, region.size))
            }
            Self::Large { by_offset, by_size } => {
                // Scan candidates in size order (smallest sufficient first) and
                // take the first that also satisfies the block constraint. In
                // non-packed mode `qualifies` is always true, so this stops at
                // the first candidate — identical to the prior behavior.
                let (region_size, region_offset) = by_size
                    .range((aligned_size, 0)..)
                    .map(|&(sz, off)| (sz, off))
                    .find(|&(_, off)| qualifies(off))?;
                by_size.remove(&(region_size, region_offset));
                by_offset.remove(&region_offset);
                if region_size > aligned_size {
                    let rem_off = region_offset + aligned_size;
                    let rem_sz = region_size - aligned_size;
                    by_offset.insert(rem_off, rem_sz);
                    by_size.insert((rem_sz, rem_off));
                }
                Some((region_offset, region_size))
            }
        };
        self.debug_assert_sorted();
        result
    }

    /// Insert a free region (after coalescing). Does NOT coalesce — caller
    /// must handle merges and call `remove` first.
    fn insert(&mut self, offset: u64, size: u64) {
        match self {
            Self::Small(v) => {
                let pos = v
                    .binary_search_by_key(&offset, |r| r.offset)
                    .unwrap_or_else(|p| p);
                v.insert(pos, FreeRegion { offset, size });
            }
            Self::Large { by_offset, by_size } => {
                by_offset.insert(offset, size);
                by_size.insert((size, offset));
            }
        }
        self.debug_assert_sorted();
    }

    /// Remove a free region by offset. Returns `Some(size)` if found.
    fn remove(&mut self, offset: u64) -> Option<u64> {
        let result = match self {
            Self::Small(v) => {
                let pos = v.binary_search_by_key(&offset, |r| r.offset).ok()?;
                let region = v.remove(pos);
                Some(region.size)
            }
            Self::Large { by_offset, by_size } => {
                let size = by_offset.remove(&offset)?;
                by_size.remove(&(size, offset));
                Some(size)
            }
        };
        self.debug_assert_sorted();
        result
    }

    /// Debug-only invariant check: the Small variant's `Vec<FreeRegion>`
    /// MUST remain sorted strictly by `offset` after any mutation. Binary
    /// search in `insert`, `remove`, `next_from`, and `prev_before` all
    /// rely on this ordering; a bug that lets the vector drift unsorted
    /// would silently break allocation and coalescing. Enabled only
    /// under `debug_assertions` so the invariant check is free in release.
    #[inline]
    fn debug_assert_sorted(&self) {
        debug_assert!(
            match self {
                Self::Small(v) => v.windows(2).all(|w| w[0].offset < w[1].offset),
                // The Large variant is trivially sorted by BTreeMap/BTreeSet
                // key invariants, so nothing to check at the Vec level.
                Self::Large { .. } => true,
            },
            "freelist Small variant lost its sort-by-offset invariant",
        );
    }

    /// Look up the next region at or after `boundary` (for forward coalesce).
    fn next_from(&self, boundary: u64) -> Option<(u64, u64)> {
        match self {
            Self::Small(v) => {
                let pos = v
                    .binary_search_by_key(&boundary, |r| r.offset)
                    .unwrap_or_else(|p| p);
                v.get(pos).map(|r| (r.offset, r.size))
            }
            Self::Large { by_offset, .. } => {
                by_offset.range(boundary..).next().map(|(&o, &s)| (o, s))
            }
        }
    }

    /// Look up the last region before `offset` (for backward coalesce).
    fn prev_before(&self, offset: u64) -> Option<(u64, u64)> {
        match self {
            Self::Small(v) => {
                let pos = v
                    .binary_search_by_key(&offset, |r| r.offset)
                    .unwrap_or_else(|p| p);
                if pos > 0 {
                    let r = &v[pos - 1];
                    Some((r.offset, r.size))
                } else {
                    None
                }
            }
            Self::Large { by_offset, .. } => {
                by_offset.range(..offset).next_back().map(|(&o, &s)| (o, s))
            }
        }
    }

    /// Promote from Small → Large if above threshold.
    fn maybe_promote(&mut self) {
        if let Self::Small(v) = self
            && v.len() > PROMOTE_THRESHOLD
        {
            let mut by_offset = std::collections::BTreeMap::new();
            let mut by_size = std::collections::BTreeSet::new();
            for r in v.drain(..) {
                by_offset.insert(r.offset, r.size);
                by_size.insert((r.size, r.offset));
            }
            *self = Self::Large { by_offset, by_size };
        }
    }

    /// Demote from Large → Small if below threshold.
    fn maybe_demote(&mut self) {
        if let Self::Large { by_offset, .. } = self
            && by_offset.len() < DEMOTE_THRESHOLD
        {
            let mut v: Vec<FreeRegion> = by_offset
                .iter()
                .map(|(&offset, &size)| FreeRegion { offset, size })
                .collect();
            v.sort_by_key(|r| r.offset);
            *self = Self::Small(v);
        }
    }
}

impl SlotAllocator {
    /// Create a new allocator for the given device, starting fresh.
    pub fn new(device: Arc<dyn BlockDevice>) -> Result<Self> {
        let alignment = device.alignment();
        let device_size = device.size();
        let mut device_id = [0u8; 16];
        getrandom::getrandom(&mut device_id).map_err(AllocatorError::Getrandom)?;
        Ok(Self {
            device,
            freelist: FreelistBackend::new(),
            next_offset: DATA_REGION_OFFSET,
            data_region_start: DATA_REGION_OFFSET,
            device_size,
            alignment,
            device_id,
            redo_log: None,
            redo_device_id: 0,
            packed: false,
            append_only: false,
            #[cfg(any(test, feature = "fault-injection"))]
            fail_next_persist: std::cell::Cell::new(false),
        })
    }

    /// Attach a redo log so subsequent [`SlotAllocator::allocate`] and
    /// [`SlotAllocator::free`] operations are journaled and fsynced before
    /// returning.
    ///
    /// Call this after construction (or after [`SlotAllocator::recover`])
    /// and before accepting any allocations. Without a redo log attached,
    /// allocator state is only durable across a [`SlotAllocator::persist`]
    /// snapshot — a crash between snapshots loses the freelist and may
    /// allow double-allocation of freed regions.
    pub fn set_redo_log(&mut self, redo_log: Arc<Mutex<RedoLog>>) {
        self.redo_log = Some(redo_log);
    }

    /// Tag this allocator's store so its `AllocateRegion`/`FreeRegion` redo
    /// entries carry the store's `device_id`. In a multi-store node each
    /// store's allocator is set to its store index, so recovery routes each
    /// region op to the right store and the per-allocator replay gate
    /// (`device_id == redo_device_id`) accepts only its own entries. Defaults
    /// to 0 (single store). Set once at startup before any allocation.
    pub fn set_redo_device_id(&mut self, device_id: u8) {
        self.redo_device_id = device_id;
    }

    /// The store tag this allocator stamps onto its `AllocateRegion`/`FreeRegion`
    /// redo entries and requires on replay (`device_id == redo_device_id`). See
    /// [`Self::set_redo_device_id`]. Recovery uses this to synthesize a
    /// `FreeRegion` that THIS store's allocator will accept.
    pub fn redo_device_id(&self) -> u8 {
        self.redo_device_id
    }

    /// Detach the redo log (mainly for tests).
    #[cfg(test)]
    fn clear_redo_log(&mut self) {
        self.redo_log = None;
    }

    /// Is a redo log currently attached?
    pub fn has_redo_log(&self) -> bool {
        self.redo_log.is_some()
    }

    /// Enable or disable packed allocation mode.
    ///
    /// Default is `false` (device-block reservations, unchanged behavior). When
    /// set to `true`, small reservations are packed within a single device block
    /// at [`RECORD_ALIGN`] granularity and large reservations stay block-aligned
    /// — see [`Self::align_reservation`] for the exact rule. Set once at startup
    /// before any allocation; toggling it on a device that already holds
    /// records placed under the other mode is unsupported (offsets differ).
    pub fn set_packed(&mut self, packed: bool) {
        self.packed = packed;
    }

    /// Whether packed allocation mode is currently enabled. See
    /// [`Self::set_packed`].
    pub fn is_packed(&self) -> bool {
        self.packed
    }

    /// Enable or disable append-only allocation mode.
    ///
    /// Default is `false` (best-fit freelist reuse, unchanged behavior). When
    /// `true`, [`Self::reserve_aligned`] never consults the freelist; every
    /// allocation extends the high-water mark, keeping records sequential even
    /// as deletes free space. Frees are still journaled and tracked but never
    /// reused, so the device grows unbounded (no reclamation). Set once at
    /// startup. See the field docs and the Phase 1 log-structured design.
    pub fn set_append_only(&mut self, append_only: bool) {
        self.append_only = append_only;
    }

    /// Whether append-only allocation mode is currently enabled. See
    /// [`Self::set_append_only`].
    pub fn is_append_only(&self) -> bool {
        self.append_only
    }

    /// Allocate a contiguous region of at least `size` bytes.
    ///
    /// The returned offset is aligned to the device's minimum I/O size.
    /// Returns [`AllocatorError::DeviceFull`] if no space is available.
    ///
    /// When a redo log has been attached via [`SlotAllocator::set_redo_log`],
    /// an [`RedoOp::AllocateRegion`] entry is appended and fsynced BEFORE
    /// the offset is returned to the caller. If the journal write fails,
    /// the in-memory reservation is rolled back and
    /// [`AllocatorError::RedoLogFailure`] is returned — the caller sees
    /// no externally-visible effect and must treat the allocation as not
    /// having happened.
    pub fn allocate(&mut self, size: u64) -> Result<u64> {
        let aligned_size = self.align_reservation(size);
        let (offset, reservation) = self.reserve_aligned(aligned_size)?;

        // Journal the reservation BEFORE returning — ensures the allocation
        // survives a crash between this point and the next `persist()`.
        if let Some(log_arc) = self.redo_log.clone() {
            let op = RedoOp::AllocateRegion {
                offset,
                size: aligned_size,
                device_id: self.redo_device_id,
            };
            let flush_result = {
                let mut log = log_arc.lock();
                log.append_and_flush(op)
            };
            if let Err(e) = flush_result {
                // Roll back the in-memory reservation so callers never
                // observe an allocation that is not durably journaled.
                self.rollback_reservation(aligned_size, reservation);
                return Err(AllocatorError::RedoLogFailure {
                    detail: format!("allocate redo append/flush failed: {e}"),
                });
            }
            // Fault-injection sync point: the redo entry is durable but
            // the caller has not yet received the offset. Simulates the
            // C6 window "AllocateRegion fsynced, caller unaware."
            crate::fault_injection::check(crate::fault_injection::SyncPoint::MidAllocatorPersist);
        }

        self.record_allocation_metrics(1, aligned_size);

        Ok(offset)
    }

    /// Allocate multiple regions, flushing all successful allocation redo
    /// entries with a single fsync.
    ///
    /// The returned vector has one entry per requested size. `Some(region)`
    /// means that item was reserved; `None` means that item did not fit at
    /// the point it was considered. Successfully reserved regions preserve
    /// the same ordering semantics as repeated [`Self::allocate`] calls.
    ///
    /// If the batch redo flush fails, every in-memory reservation made by
    /// this call is rolled back in reverse order and
    /// [`AllocatorError::RedoLogFailure`] is returned.
    pub fn allocate_batch(&mut self, sizes: &[u64]) -> Result<Vec<Option<AllocatedRegion>>> {
        let mut results = Vec::with_capacity(sizes.len());
        let mut reservations: Vec<(u64, Reservation)> = Vec::new();
        let mut redo_ops: Vec<RedoOp> = Vec::new();

        for size in sizes {
            let aligned_size = self.align_reservation(*size);
            match self.reserve_aligned(aligned_size) {
                Ok((offset, reservation)) => {
                    results.push(Some(AllocatedRegion {
                        offset,
                        size: aligned_size,
                    }));
                    reservations.push((aligned_size, reservation));
                    redo_ops.push(RedoOp::AllocateRegion {
                        offset,
                        size: aligned_size,
                        device_id: self.redo_device_id,
                    });
                }
                Err(AllocatorError::DeviceFull { .. }) => {
                    results.push(None);
                }
                Err(e) => {
                    for (aligned_size, reservation) in reservations.into_iter().rev() {
                        self.rollback_reservation(aligned_size, reservation);
                    }
                    return Err(e);
                }
            }
        }

        if let Some(log_arc) = self.redo_log.clone()
            && !redo_ops.is_empty()
        {
            let flush_result = {
                let mut log = log_arc.lock();
                log.append_batch_and_flush(&redo_ops)
            };
            if let Err(e) = flush_result {
                for (aligned_size, reservation) in reservations.into_iter().rev() {
                    self.rollback_reservation(aligned_size, reservation);
                }
                return Err(AllocatorError::RedoLogFailure {
                    detail: format!("allocate batch redo append/flush failed: {e}"),
                });
            }
            crate::fault_injection::check(crate::fault_injection::SyncPoint::MidAllocatorPersist);
        }

        let mut allocated_count = 0u64;
        let mut allocated_bytes = 0u64;
        for region in results.iter().flatten() {
            allocated_count += 1;
            allocated_bytes += region.size;
        }
        self.record_allocation_metrics(allocated_count, allocated_bytes);

        Ok(results)
    }

    /// Reserve a batch of regions IN MEMORY ONLY, deferring the durable
    /// `AllocateRegion` journaling to the caller.
    ///
    /// Issue #14 (orphan prevention): [`Self::allocate_batch`] fsyncs its
    /// `AllocateRegion` entries in its OWN flush, BEFORE the create path writes
    /// the matching `Create` entries. Under a redo-log-full window the
    /// `Create` write then fails and the compensating `free()` also can't
    /// journal — leaving a DURABLE allocation with no record (an orphan that
    /// crash-loops rebuild before the tolerant-rebuild fix, and leaks space
    /// after it). `reserve_batch` instead applies the reservations to the
    /// freelist in memory and returns the `AllocateRegion` redo ops UNwritten,
    /// so the caller can journal them ATOMICALLY in the same batch as the
    /// `Create` entries (one fsync — all-or-nothing) and roll the reservations
    /// back in memory if that batch fails. No `AllocateRegion` is ever made
    /// durable without its `Create`.
    ///
    /// The returned [`PendingBatchAllocation`] MUST be passed to exactly one of
    /// [`Self::commit_pending`] (after the caller durably journaled the redo
    /// ops) or [`Self::rollback_pending`] (if it could not). The caller must do
    /// this within the same exclusive mutation barrier it holds for the create,
    /// so a concurrent checkpoint cannot persist the allocator header while a
    /// reservation is in memory but not yet journaled.
    pub fn reserve_batch(&mut self, sizes: &[u64]) -> Result<PendingBatchAllocation> {
        let mut regions = Vec::with_capacity(sizes.len());
        let mut reservations: Vec<(u64, Reservation)> = Vec::new();
        let mut alloc_redo_ops: Vec<RedoOp> = Vec::new();

        for size in sizes {
            let aligned_size = self.align_reservation(*size);
            match self.reserve_aligned(aligned_size) {
                Ok((offset, reservation)) => {
                    regions.push(Some(AllocatedRegion {
                        offset,
                        size: aligned_size,
                    }));
                    reservations.push((aligned_size, reservation));
                    alloc_redo_ops.push(RedoOp::AllocateRegion {
                        offset,
                        size: aligned_size,
                        device_id: self.redo_device_id,
                    });
                }
                Err(AllocatorError::DeviceFull { .. }) => {
                    regions.push(None);
                }
                Err(e) => {
                    // Undo what we reserved so far (in memory; nothing journaled).
                    for (aligned_size, reservation) in reservations.into_iter().rev() {
                        self.rollback_reservation(aligned_size, reservation);
                    }
                    return Err(e);
                }
            }
        }

        Ok(PendingBatchAllocation {
            regions,
            rollback: BatchRollback::Slot(reservations),
            alloc_redo_ops,
        })
    }

    /// Finalize a [`PendingBatchAllocation`] whose `AllocateRegion` redo ops the
    /// caller has now durably journaled. The reservations are already reflected
    /// in the freelist, so this only records allocation metrics and drops the
    /// rollback handles.
    pub fn commit_pending(&mut self, pending: PendingBatchAllocation) {
        let BatchRollback::Slot(reservations) = &pending.rollback else {
            // A SlotAllocator only ever receives back a PendingBatchAllocation it
            // produced (the engine routes commit to the same store's allocator).
            unreachable!("SlotAllocator::commit_pending given a non-Slot rollback handle");
        };
        let count = reservations.len() as u64;
        let bytes: u64 = reservations.iter().map(|(sz, _)| *sz).sum();
        self.record_allocation_metrics(count, bytes);
    }

    /// Roll back a [`PendingBatchAllocation`] whose redo batch could NOT be
    /// durably journaled — restores the freelist to its pre-reserve state in
    /// memory. Because no `AllocateRegion` was journaled, there is nothing
    /// durable to compensate, so this needs no redo write (and works even when
    /// the redo log is full — the condition that motivated it).
    pub fn rollback_pending(&mut self, pending: PendingBatchAllocation) {
        let BatchRollback::Slot(reservations) = pending.rollback else {
            unreachable!("SlotAllocator::rollback_pending given a non-Slot rollback handle");
        };
        for (aligned_size, reservation) in reservations.into_iter().rev() {
            self.rollback_reservation(aligned_size, reservation);
        }
    }

    /// Return a region to the freelist.
    ///
    /// Adjacent free regions are automatically merged.
    ///
    /// When a redo log has been attached, a [`RedoOp::FreeRegion`] entry
    /// is appended and fsynced BEFORE any freelist mutation. On fsync
    /// failure the freelist is left untouched and
    /// [`AllocatorError::RedoLogFailure`] is returned.
    pub fn free(&mut self, offset: u64, size: u64) -> Result<()> {
        // Align the SAME way the region was reserved (`align_reservation`): in
        // packed mode that is the record granularity, NOT the device block, so
        // freeing one packed record returns exactly its byte range and never
        // over-frees the block-neighbours it shares a 4 KiB block with. In
        // non-packed mode this is `align_up` (device block), unchanged.
        let aligned_size = self.align_reservation(size);

        if aligned_size == 0 {
            return Err(AllocatorError::InvalidFree {
                offset,
                size: aligned_size,
            });
        }
        let Some(end) = offset.checked_add(aligned_size) else {
            return Err(AllocatorError::InvalidFree {
                offset,
                size: aligned_size,
            });
        };
        if offset < self.data_region_start || end > self.device_size {
            return Err(AllocatorError::InvalidFree {
                offset,
                size: aligned_size,
            });
        }

        // IJ-7: reject a double-free / overlapping-free BEFORE journaling or
        // mutating the freelist. Without this, freeing a range that overlaps
        // an existing free region would double-count those bytes and let a
        // later allocation hand the same offset out twice. Two free regions
        // can overlap `[offset, end)`:
        //   1. the region at or before `offset` (it may extend past it), and
        //   2. the first region whose start is `>= offset` (it overlaps iff
        //      its start is `< end`).
        // Both are O(log n) ordered-map lookups.
        if let Some((free_offset, free_size)) = self.free_region_containing(offset) {
            return Err(AllocatorError::DoubleFree {
                offset,
                size: aligned_size,
                free_offset,
                free_size,
            });
        }
        if let Some((free_offset, free_size)) = self.freelist.next_from(offset)
            && free_offset < end
        {
            return Err(AllocatorError::DoubleFree {
                offset,
                size: aligned_size,
                free_offset,
                free_size,
            });
        }

        // Journal the release BEFORE mutating the freelist — on fsync
        // failure the in-memory state is left unchanged so callers never
        // see a free that isn't durably journaled.
        if let Some(log_arc) = self.redo_log.clone() {
            let op = RedoOp::FreeRegion {
                offset,
                size: aligned_size,
                device_id: self.redo_device_id,
            };
            let flush_result = {
                let mut log = log_arc.lock();
                log.append_and_flush(op)
            };
            if let Err(e) = flush_result {
                return Err(AllocatorError::RedoLogFailure {
                    detail: format!("free redo append/flush failed: {e}"),
                });
            }
            // Fault-injection sync point: "redo durable, freelist not yet
            // mutated." Simulates a crash in the exact C6 window.
            crate::fault_injection::check(crate::fault_injection::SyncPoint::MidAllocatorPersist);
        }

        let mut final_offset = offset;
        let mut final_size = aligned_size;

        // Merge with the next region if adjacent.
        let next_boundary = offset + aligned_size;
        if let Some((next_off, next_sz)) = self.freelist.next_from(next_boundary)
            && next_off == next_boundary
        {
            self.freelist.remove(next_off);
            final_size += next_sz;
        }

        // Merge with the previous region if adjacent.
        if let Some((prev_off, prev_sz)) = self.freelist.prev_before(offset)
            && prev_off + prev_sz == offset
        {
            self.freelist.remove(prev_off);
            final_offset = prev_off;
            final_size += prev_sz;
        }

        self.freelist.insert(final_offset, final_size);
        self.freelist.maybe_promote();

        if let Some(m) = allocator_metrics() {
            m.free_total.inc();
            m.free_bytes_total.inc_by(aligned_size);
            self.refresh_freelist_gauges(m);
        }

        Ok(())
    }

    fn reserve_aligned(&mut self, aligned_size: u64) -> Result<(u64, Reservation)> {
        // In packed mode, pass the device block to `best_fit` so a reused hole
        // satisfies the within-one-block (small) / block-aligned (large)
        // constraint for the allocation it hands out. In non-packed mode the
        // constraint is absent and `best_fit` behaves exactly as before.
        let block_constraint = if self.packed {
            Some(self.alignment as u64)
        } else {
            None
        };
        // Append-only mode skips freelist reuse entirely so every allocation
        // extends the high-water mark — records stay strictly sequential even
        // after deletes (whose freed regions remain in the freelist for
        // accounting/recovery but are never handed back out).
        let from_freelist = if self.append_only {
            None
        } else {
            self.freelist.best_fit(aligned_size, block_constraint)
        };
        if let Some((region_offset, region_size)) = from_freelist {
            // best_fit already removed/split the region in the freelist.
            self.freelist.maybe_demote();
            Ok((
                region_offset,
                Reservation::FromFreelist {
                    alloc_offset: region_offset,
                    region_size,
                },
            ))
        } else {
            // No suitable free region — extend at the append point.
            let previous_next_offset = self.next_offset;
            let mut offset = self.next_offset;

            // Packed placement invariant: a record never straddles a device
            // block, and a large record starts block-aligned. If placing at
            // the current high-water mark would violate that, advance to the
            // next block boundary. The skipped tail
            // `[previous_next_offset, offset)` is acceptable waste for this
            // phase (~1%); `previous_next_offset` (the ORIGINAL high-water)
            // is recorded below so rollback reclaims the record AND the tail.
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

            // Overflow + device-size bounds are checked against the possibly
            // bumped offset.
            let Some(end) = offset.checked_add(aligned_size) else {
                return Err(AllocatorError::DeviceFull {
                    requested: aligned_size,
                    largest_free: self.freelist.largest(),
                });
            };
            if end > self.device_size {
                return Err(AllocatorError::DeviceFull {
                    requested: aligned_size,
                    largest_free: self.freelist.largest(),
                });
            }
            self.next_offset = end;
            Ok((
                offset,
                Reservation::FromHighWater {
                    previous_next_offset,
                },
            ))
        }
    }

    fn rollback_reservation(&mut self, aligned_size: u64, reservation: Reservation) {
        match reservation {
            Reservation::FromFreelist {
                alloc_offset,
                region_size,
            } => {
                // Re-insert the original region. The returned `region_size`
                // matches the size we took out in `best_fit` (which
                // internally split if needed). If it split, remove the
                // remainder before restoring the full region.
                if region_size > aligned_size {
                    let remainder_offset = alloc_offset + aligned_size;
                    self.freelist.remove(remainder_offset);
                }
                // F-G1-016: coalesce with adjacent free regions on the
                // way back into the freelist. The allocator is
                // single-threaded today so the original region's
                // neighbours cannot have changed between the
                // `best_fit` split and the rollback — but a future
                // change that interleaves rollback with other free()
                // calls would silently leave fragmentation. Coalescing
                // here keeps the freelist invariant ("no two adjacent
                // free regions") in lockstep with `free()`.
                let (final_offset, final_size) = self.coalesce_adjacent(alloc_offset, region_size);
                self.freelist.insert(final_offset, final_size);
                self.freelist.maybe_promote();
            }
            Reservation::FromHighWater {
                previous_next_offset,
            } => {
                self.next_offset = previous_next_offset;
            }
        }
    }

    /// F-G1-016: merge `[offset, offset+size)` with any adjacent free
    /// regions and return the resulting `(offset, size)`.
    ///
    /// Removes the merged neighbours from the freelist as a side
    /// effect; callers must `insert` the returned region themselves so
    /// the same primitive is reusable from both `free()` (pre-fix,
    /// inlined) and `rollback_reservation` (post-fix). The freelist
    /// invariant relied on by binary search and best-fit is "no two
    /// adjacent free regions" — keep both paths honouring it.
    fn coalesce_adjacent(&mut self, offset: u64, size: u64) -> (u64, u64) {
        let mut final_offset = offset;
        let mut final_size = size;

        // Merge with the next region if adjacent.
        let next_boundary = offset + size;
        if let Some((next_off, next_sz)) = self.freelist.next_from(next_boundary)
            && next_off == next_boundary
        {
            self.freelist.remove(next_off);
            final_size += next_sz;
        }

        // Merge with the previous region if adjacent.
        if let Some((prev_off, prev_sz)) = self.freelist.prev_before(offset)
            && prev_off + prev_sz == offset
        {
            self.freelist.remove(prev_off);
            final_offset = prev_off;
            final_size += prev_sz;
        }

        (final_offset, final_size)
    }

    fn record_allocation_metrics(&self, count: u64, bytes: u64) {
        if count == 0 {
            return;
        }
        if let Some(m) = allocator_metrics() {
            m.alloc_total.inc_by(count);
            m.alloc_bytes_total.inc_by(bytes);
            self.refresh_freelist_gauges(m);
        }
    }

    /// Refresh the freelist-shape gauges in [`crate::metrics::AllocatorMetrics`].
    ///
    /// Called from `allocate`/`free` after mutating the freelist. Walks the
    /// freelist once to compute the region count and the largest contiguous
    /// region — `best_fit` already does the same O(freelist) traversal so
    /// the worst-case complexity is unchanged.
    fn refresh_freelist_gauges(&self, m: &crate::metrics::AllocatorMetrics) {
        let mut count: u32 = 0;
        let mut largest: u64 = 0;
        for (_, size) in self.freelist.iter_offset_order() {
            count = count.saturating_add(1);
            if size > largest {
                largest = size;
            }
        }
        m.freelist_region_count.store(count, Ordering::Relaxed);
        m.freelist_largest_region_bytes
            .store(largest, Ordering::Relaxed);
    }

    /// Apply a redo-log entry during crash recovery.
    ///
    /// The caller is the recovery pipeline — after the allocator has been
    /// restored from its on-device snapshot, each post-checkpoint
    /// allocator redo entry is replayed here so the rebuilt state matches
    /// what the client observed before the crash.
    ///
    /// Both operations are idempotent:
    /// - `AllocateRegion`: if the region is below `next_offset` and not in
    ///   the freelist, the entry is already reflected — no-op. Otherwise
    ///   the region is removed from the freelist (if present) and
    ///   `next_offset` is bumped past the region if necessary.
    /// - `FreeRegion`: if the exact region is already in the freelist
    ///   (or fully covered by an existing free region), no-op. Otherwise
    ///   the region is inserted (with coalescing, same as [`Self::free`]).
    ///
    /// Returns `true` if the replay changed allocator state,
    /// `false` on an idempotent no-op.
    pub fn replay_redo(&mut self, op: &RedoOp) -> bool {
        match op {
            RedoOp::AllocateRegion {
                offset,
                size,
                device_id,
            } => {
                if *device_id != self.redo_device_id {
                    return false;
                }
                self.replay_allocate(*offset, *size)
            }
            RedoOp::FreeRegion {
                offset,
                size,
                device_id,
            } => {
                if *device_id != self.redo_device_id {
                    return false;
                }
                self.replay_free(*offset, *size)
            }
            _ => false,
        }
    }

    fn replay_allocate(&mut self, offset: u64, size: u64) -> bool {
        // Use the same size-alignment the live path used so packed offsets
        // reconstruct identically from the redo `AllocateRegion` entries. The
        // redo entry's `offset` is the exact placed offset, so no bump logic is
        // needed here — only the SIZE alignment must match `align_reservation`.
        let aligned_size = self.align_reservation(size);
        let Some(end) = offset.checked_add(aligned_size) else {
            tracing::error!(
                target = "teraslab::allocator",
                offset,
                size,
                "replay_allocate: redo entry has overflowing offset+size — dropped as corrupt",
            );
            if let Some(m) = allocator_metrics() {
                m.corrupt_redo_entries_total.inc();
            }
            return false;
        };
        if aligned_size == 0 || offset < self.data_region_start || end > self.device_size {
            // F-G1-015: distinguish "corrupt redo entry rejected" from
            // "idempotent no-op". A redo entry whose offset/size falls
            // outside the data region is a sign the log was tampered or
            // corrupted; recovery should not silently discard it. We
            // still return `false` (the caller's contract) but emit an
            // observable error so the operator sees the rejection.
            tracing::error!(
                target = "teraslab::allocator",
                offset,
                size,
                aligned_size,
                data_region_start = self.data_region_start,
                device_size = self.device_size,
                "replay_allocate: redo entry outside data region or zero-sized — dropped as corrupt",
            );
            if let Some(m) = allocator_metrics() {
                m.corrupt_redo_entries_total.inc();
            }
            return false;
        }

        // Bump high-water mark past the allocated region if necessary.
        let bumped = if end > self.next_offset {
            self.next_offset = end;
            true
        } else {
            false
        };

        // Remove (or carve out) any overlapping free region.
        let carved = self.carve_allocation(offset, aligned_size);

        bumped || carved
    }

    /// Remove the region `[offset, offset+size)` from the freelist.
    ///
    /// Handles partial overlap by splitting the surrounding free region
    /// into head/tail remainders. Returns `true` if any change was made.
    fn carve_allocation(&mut self, offset: u64, size: u64) -> bool {
        let end = offset + size;

        // Find a free region that overlaps [offset, end). Only one can
        // contain this range since free regions are non-overlapping.
        let overlap = self
            .freelist
            .prev_before(offset.saturating_add(1))
            .filter(|(o, s)| *o + *s > offset && *o < end)
            .or_else(|| {
                self.freelist
                    .next_from(offset)
                    .filter(|(o, s)| *o < end && *o + *s > offset)
            });

        let Some((free_off, free_sz)) = overlap else {
            return false;
        };

        // Remove the overlapping region entirely.
        self.freelist.remove(free_off);

        // Re-insert the head portion, if any.
        if free_off < offset {
            let head_size = offset - free_off;
            self.freelist.insert(free_off, head_size);
        }
        // Re-insert the tail portion, if any.
        let free_end = free_off + free_sz;
        if free_end > end {
            let tail_size = free_end - end;
            self.freelist.insert(end, tail_size);
        }
        self.freelist.maybe_promote();
        self.freelist.maybe_demote();
        true
    }

    fn replay_free(&mut self, offset: u64, size: u64) -> bool {
        // Align the same way the live path sized the reservation so replay
        // reconstructs identical packed ranges. `FreeRegion` redo entries store
        // an already-aligned size, on which `align_reservation` is idempotent
        // (it equals `align_up` for any block-multiple input), so this is a
        // no-op for the current free path and stays exact if a later phase
        // packs frees too.
        let aligned_size = self.align_reservation(size);
        if aligned_size == 0 {
            tracing::error!(
                target = "teraslab::allocator",
                offset,
                size,
                "replay_free: redo entry has zero aligned size — dropped as corrupt",
            );
            if let Some(m) = allocator_metrics() {
                m.corrupt_redo_entries_total.inc();
            }
            return false;
        }
        let Some(end) = offset.checked_add(aligned_size) else {
            tracing::error!(
                target = "teraslab::allocator",
                offset,
                size,
                "replay_free: redo entry has overflowing offset+size — dropped as corrupt",
            );
            if let Some(m) = allocator_metrics() {
                m.corrupt_redo_entries_total.inc();
            }
            return false;
        };

        // Idempotency: if the region is entirely inside an existing free
        // region, skip. This is the legitimate "already applied" path —
        // no error log.
        if let Some((prev_off, prev_sz)) = self.freelist.prev_before(offset + 1)
            && prev_off <= offset
            && prev_off.saturating_add(prev_sz) >= end
        {
            return false;
        }

        // F-G1-015: reject frees outside the valid data region — distinct
        // from the idempotent no-op above. Log as a corrupt-entry
        // rejection so the operator can see that recovery dropped
        // entries.
        if offset < self.data_region_start || end > self.device_size {
            tracing::error!(
                target = "teraslab::allocator",
                offset,
                size,
                aligned_size,
                data_region_start = self.data_region_start,
                device_size = self.device_size,
                "replay_free: redo entry outside data region — dropped as corrupt",
            );
            if let Some(m) = allocator_metrics() {
                m.corrupt_redo_entries_total.inc();
            }
            return false;
        }

        // F-G1-015: reject partial overlaps with existing free regions.
        // Idempotent contained frees were handled above; any remaining
        // overlap would create intersecting freelist regions and allow a
        // later allocation to hand out live space. Log as a corrupt-
        // entry rejection rather than the silent return-false in the
        // earlier code.
        if let Some((prev_off, prev_sz)) = self.freelist.prev_before(offset + 1)
            && prev_off.saturating_add(prev_sz) > offset
        {
            tracing::error!(
                target = "teraslab::allocator",
                offset,
                size,
                conflicting_offset = prev_off,
                conflicting_size = prev_sz,
                "replay_free: redo entry partially overlaps existing free region (prev) — dropped",
            );
            if let Some(m) = allocator_metrics() {
                m.corrupt_redo_entries_total.inc();
            }
            return false;
        }
        if let Some((next_off, _)) = self.freelist.next_from(offset)
            && next_off < end
        {
            tracing::error!(
                target = "teraslab::allocator",
                offset,
                size,
                conflicting_offset = next_off,
                "replay_free: redo entry partially overlaps existing free region (next) — dropped",
            );
            if let Some(m) = allocator_metrics() {
                m.corrupt_redo_entries_total.inc();
            }
            return false;
        }

        let mut final_offset = offset;
        let mut final_size = aligned_size;

        let next_boundary = end;
        if let Some((next_off, next_sz)) = self.freelist.next_from(next_boundary)
            && next_off == next_boundary
        {
            self.freelist.remove(next_off);
            final_size += next_sz;
        }

        if let Some((prev_off, prev_sz)) = self.freelist.prev_before(offset)
            && prev_off + prev_sz == offset
        {
            self.freelist.remove(prev_off);
            final_offset = prev_off;
            final_size += prev_sz;
        }

        self.freelist.insert(final_offset, final_size);
        self.freelist.maybe_promote();
        true
    }

    /// Persist the freelist and next_offset to the device header region.
    ///
    /// Header v1 layout:
    /// ```text
    /// Offset  Size  Field
    /// 0       8     Magic (TERAALLC)
    /// 8       8     next_offset
    /// 16      8     count (free region count)
    /// 24      16    Device identity (128-bit random)
    /// 40      4     Header version (little-endian u32, currently 1)
    /// 44      4     Header CRC32 (little-endian u32; computed with this
    ///                field zeroed over bytes 0..FREELIST_OFFSET + 16*count)
    /// 48+     16*N  free region entries (offset:8, size:8)
    /// ```
    ///
    /// The CRC32 at bytes 44..48 is stamped last. During computation the
    /// CRC field is treated as zero so [`SlotAllocator::recover`] can
    /// reproduce the value by zeroing the field before hashing.
    ///
    /// The header write is followed by a device sync, so the persisted
    /// state is durable (not merely in the drive's write cache) when this
    /// returns. Checkpointing relies on that barrier before it reclaims
    /// the redo entries covering the freelist delta.
    ///
    /// # Errors
    ///
    /// Returns [`AllocatorError::FreelistOverflow`] if the freelist does
    /// not fit in the on-device header, or [`AllocatorError::Device`] if
    /// the header write or the sync fails.
    /// Test/fault-injection only: arm the next [`SlotAllocator::persist`]
    /// to fail once with [`AllocatorError::PersistFaultInjected`], then
    /// auto-clear. Used by the checkpoint crash tests to fail
    /// `persist_allocator` after the snapshot has been renamed but before
    /// the recovery-progress fence is written. Compiled out of production
    /// builds.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_fail_next_persist(&self) {
        self.fail_next_persist.set(true);
    }

    pub fn persist(&self) -> Result<()> {
        self.persist_header_no_sync()?;
        // B-1 audit fix: barrier the header write so it is durable (not merely
        // in the device/drive write cache) before returning — recovery and the
        // checkpoint rely on this. See `persist_header_no_sync` for the path
        // that intentionally defers this sync to the caller.
        self.device.sync()?;
        Ok(())
    }

    /// Write the allocator header to the device WITHOUT the durability fsync.
    ///
    /// The caller MUST sync the device afterwards to make the header durable.
    /// This exists for the checkpoint, which writes every store's header and
    /// then syncs all store devices ONCE — crucially, *outside* the per-store
    /// allocator mutex. Folding the `device.sync()` into the lock (as `persist`
    /// does) means a slow sync — e.g. flushing a large write-back data cache —
    /// holds the allocator mutex for its whole duration, which blocks every
    /// create's `reserve_*` and stalls all writes (profiled: a single 90s
    /// checkpoint sync froze the server). Header writing is cheap and stays
    /// under the lock; the expensive sync is hoisted out by the caller.
    pub(crate) fn persist_header_no_sync(&self) -> Result<()> {
        // Test/fault-injection only: fail-once hook to drive a checkpoint
        // into the "snapshot renamed, allocator persist failed, no fence
        // written" crash window. Auto-clears so a retry succeeds. Compiled
        // out of production builds.
        #[cfg(any(test, feature = "fault-injection"))]
        if self.fail_next_persist.replace(false) {
            return Err(AllocatorError::PersistFaultInjected);
        }

        // F-G1-009: refuse to silently truncate a freelist that does not
        // fit in the on-device header. Pre-fix this branch was
        // `let count = self.freelist.len().min(MAX_PERSISTED_FREE_REGIONS);`
        // and any overflow entries were lost on the next `recover()` —
        // free space leaked permanently with no log line and no metric.
        // The cap is large (~65k entries on a 1 MiB header), but
        // pathologically fragmented workloads can reach it, and a leak
        // there is silent corruption from the operator's point of view.
        let entries = self.freelist.len();
        if entries > MAX_PERSISTED_FREE_REGIONS {
            return Err(AllocatorError::FreelistOverflow {
                entries,
                max: MAX_PERSISTED_FREE_REGIONS,
            });
        }
        let count = entries;
        let aligned_len = self.align_up(FREELIST_OFFSET as u64 + (count as u64) * 16);
        let mut buf = AlignedBuf::new(aligned_len as usize, self.alignment);

        buf[0..8].copy_from_slice(&ALLOCATOR_MAGIC.to_le_bytes());
        buf[8..16].copy_from_slice(&self.next_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&(count as u64).to_le_bytes());
        buf[24..40].copy_from_slice(&self.device_id);
        // Stamp the layout version: packed devices write
        // `HEADER_VERSION_PACKED` (2) so `recover` restores packed-ness and an
        // old v1-only binary fails closed; non-packed devices write
        // `HEADER_VERSION` (1), byte-identical to before.
        let version = if self.packed {
            HEADER_VERSION_PACKED
        } else {
            HEADER_VERSION
        };
        buf[40..44].copy_from_slice(&version.to_le_bytes());
        // CRC slot stays zero until we hash — this is part of the contract.
        buf[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].copy_from_slice(&0u32.to_le_bytes());

        for (i, (offset, size)) in self.freelist.iter_offset_order().take(count).enumerate() {
            let base = FREELIST_OFFSET + i * 16;
            buf[base..base + 8].copy_from_slice(&offset.to_le_bytes());
            buf[base + 8..base + 16].copy_from_slice(&size.to_le_bytes());
        }

        // Compute CRC32 over the populated header (bytes 0..covered_end)
        // with the CRC field zeroed. `covered_end` is the end of the
        // freelist entries (exclusive); trailing padding in the aligned
        // buffer is NOT part of the checksum because `recover()` may
        // read only the freelist-entry region based on `count`.
        let covered_end = FREELIST_OFFSET + count * 16;
        let crc = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&buf[..covered_end]);
            hasher.finalize()
        };
        buf[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());

        self.device.pwrite_all_at(&buf, 0)?;
        // NOTE: the durability fsync is intentionally NOT here — `persist` adds
        // it, and the checkpoint syncs all store devices once outside the
        // allocator lock. See this method's doc comment.
        Ok(())
    }

    /// Recover allocator state from the device header.
    ///
    /// Validates the header's CRC32 checksum (bytes 44..48) before trusting
    /// any other field. A mismatch returns
    /// [`AllocatorError::HeaderCorruption`] with both the expected and
    /// actual CRC values so the operator can distinguish this from other
    /// corruption forms.
    ///
    /// A bad magic value is classified by inspecting the header block
    /// (audit B-2): an all-zero region means the device never had a
    /// header persisted and returns
    /// [`AllocatorError::NoPersistedState`] — the only error a caller
    /// may treat as "safe to create a fresh allocator". Any non-zero
    /// garbage returns [`AllocatorError::CorruptedHeader`], which must
    /// fail closed: a fresh allocator over a device with persisted
    /// state would re-allocate live regions and overwrite records.
    pub fn recover(device: Arc<dyn BlockDevice>) -> Result<Self> {
        let alignment = device.alignment();
        let device_size = device.size();

        let header_size = alignment.max(FREELIST_OFFSET);
        let mut header_buf = AlignedBuf::new(header_size, alignment);
        device.pread_exact_at(&mut header_buf, 0)?;

        let magic = u64::from_le_bytes(
            header_buf[0..8]
                .try_into()
                .map_err(|_| AllocatorError::CorruptedHeader)?,
        );
        if magic != ALLOCATOR_MAGIC {
            // Distinguish "never persisted" (genuinely fresh device —
            // every byte of the header block is zero; `persist` always
            // writes the magic into this block first, so any persisted
            // or torn header leaves non-zero bytes here) from "garbage
            // header" (corruption — fail closed).
            if header_buf.iter().all(|&b| b == 0) {
                return Err(AllocatorError::NoPersistedState);
            }
            return Err(AllocatorError::CorruptedHeader);
        }

        let next_offset = u64::from_le_bytes(
            header_buf[8..16]
                .try_into()
                .map_err(|_| AllocatorError::CorruptedHeader)?,
        );
        let count = u64::from_le_bytes(
            header_buf[16..24]
                .try_into()
                .map_err(|_| AllocatorError::CorruptedHeader)?,
        ) as usize;

        let mut device_id = [0u8; 16];
        device_id.copy_from_slice(&header_buf[24..40]);

        let version = u32::from_le_bytes(
            header_buf[40..44]
                .try_into()
                .map_err(|_| AllocatorError::CorruptedHeader)?,
        );
        // Classify the on-disk layout from the version field. The DEVICE's
        // format wins: packed-ness comes from here, never from config. Any
        // version this build does not know (including a v2 packed header read
        // by an old v1-only binary, where `HEADER_VERSION_PACKED` would not be
        // a known value) fails CLOSED — opening a packed device non-packed
        // would corrupt it via `free()`'s block-rounding.
        let packed = match version {
            HEADER_VERSION => false,
            HEADER_VERSION_PACKED => true,
            other => return Err(AllocatorError::UnsupportedVersion(other)),
        };

        // Read the full freelist and verify CRC32.
        //
        // `count` is read unvalidated from the on-disk header, so bound it
        // BEFORE doing any arithmetic on it. A crafted/corrupt header could
        // otherwise overflow `count * 16` (panic under overflow-checks, wrap
        // in release) — a fail-open robustness defect on a disk-controlled
        // recovery path. Bound first, then use checked arithmetic so any
        // residual overflow maps to a clean `CorruptedHeader` rather than a
        // panic or silent wrap.
        if count > MAX_PERSISTED_FREE_REGIONS {
            return Err(AllocatorError::CorruptedHeader);
        }
        let total_size = count
            .checked_mul(16)
            .and_then(|n| n.checked_add(FREELIST_OFFSET))
            .ok_or(AllocatorError::CorruptedHeader)?;
        let aligned_total = total_size.div_ceil(alignment) * alignment;
        let mut buf = AlignedBuf::new(aligned_total, alignment);
        device.pread_exact_at(&mut buf, 0)?;

        // CRC32 verification: the stored CRC was computed with the CRC
        // field zeroed. Read it out, zero the field in a local copy of
        // the covered range, and recompute.
        let stored_crc = u32::from_le_bytes(
            buf[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4]
                .try_into()
                .map_err(|_| AllocatorError::CorruptedHeader)?,
        );
        // `total_size == FREELIST_OFFSET + count * 16` was already computed
        // (with overflow checked) above, so reuse it rather than repeating the
        // unchecked multiply.
        let covered_end = total_size;
        // Copy the covered header bytes so we can zero the CRC field
        // without mutating the read buffer (it's still used below to
        // decode the freelist).
        let mut crc_input: Vec<u8> = buf[..covered_end].to_vec();
        for byte in &mut crc_input[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4] {
            *byte = 0;
        }
        let computed_crc = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&crc_input);
            hasher.finalize()
        };
        if computed_crc != stored_crc {
            return Err(AllocatorError::HeaderCorruption {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        let mut freelist = FreelistBackend::new();
        for i in 0..count {
            let base = FREELIST_OFFSET + i * 16;
            let offset = u64::from_le_bytes(
                buf[base..base + 8]
                    .try_into()
                    .map_err(|_| AllocatorError::CorruptedHeader)?,
            );
            let size = u64::from_le_bytes(
                buf[base + 8..base + 16]
                    .try_into()
                    .map_err(|_| AllocatorError::CorruptedHeader)?,
            );
            freelist.insert(offset, size);
        }
        freelist.maybe_promote();

        Ok(Self {
            device,
            freelist,
            next_offset,
            data_region_start: DATA_REGION_OFFSET,
            device_size,
            alignment,
            device_id,
            redo_log: None,
            redo_device_id: 0,
            packed,
            // Append-only is a runtime placement policy, not an on-disk format;
            // it is never persisted, so a recovered allocator starts in the
            // default (best-fit) mode until the caller re-applies config.
            append_only: false,
            #[cfg(any(test, feature = "fault-injection"))]
            fail_next_persist: std::cell::Cell::new(false),
        })
    }

    /// Round `size` up to the device alignment boundary.
    fn align_up(&self, size: u64) -> u64 {
        let a = self.alignment as u64;
        size.div_ceil(a) * a
    }

    /// Compute the reservation size for `size` bytes, honoring packed mode.
    ///
    /// - Not packed → `align_up(size)` (device block — unchanged behavior).
    /// - Packed, small (`size <= device block`) → rounded up to [`RECORD_ALIGN`]
    ///   so several records pack within one block.
    /// - Packed, large (`size > device block`) → `align_up(size)` (device-block
    ///   multiple) so a large record owns whole blocks and stays block-granular.
    ///
    /// This sizes the reservation only; the within-one-block / block-aligned
    /// *placement* invariant is enforced in [`Self::reserve_aligned`].
    fn align_reservation(&self, size: u64) -> u64 {
        if !self.packed {
            return self.align_up(size);
        }
        let block = self.alignment as u64;
        if size <= block {
            // Small/packable: round up to the small struct-access alignment.
            size.div_ceil(RECORD_ALIGN) * RECORD_ALIGN
        } else {
            // Large: keep device-block granularity so it owns its blocks.
            self.align_up(size)
        }
    }

    /// The number of free regions in the freelist.
    ///
    /// Exposed for tests and recovery diagnostics — production code
    /// should read [`SlotAllocator::stats`] for a full snapshot.
    pub fn free_region_count(&self) -> usize {
        self.freelist.len()
    }

    /// Return the free region containing `offset`, if any.
    ///
    /// Rebuild scans use this to distinguish expected holes from corrupt
    /// allocated records. The returned tuple is `(region_offset, region_size)`.
    pub fn free_region_containing(&self, offset: u64) -> Option<(u64, u64)> {
        let (free_offset, free_size) = self.freelist.prev_before(offset.saturating_add(1))?;
        if offset >= free_offset && offset < free_offset.saturating_add(free_size) {
            Some((free_offset, free_size))
        } else {
            None
        }
    }

    /// Return true if `[offset, offset + size)` is inside the allocator's
    /// high-water mark and does not overlap any free region.
    ///
    /// Recovery uses this before trusting a `Create.record_offset`: a
    /// redo entry that points outside allocator-owned space must not
    /// register a primary index entry.
    pub fn is_allocated_range(&self, offset: u64, size: u64) -> bool {
        // Match the reservation granularity (record-aligned in packed mode, the
        // device block otherwise) so the checked range is exactly the record's,
        // not a 4 KiB-rounded span that would bleed into packed neighbours.
        let aligned_size = self.align_reservation(size);
        let Some(end) = offset.checked_add(aligned_size) else {
            return false;
        };
        if aligned_size == 0
            || offset < self.data_region_start
            || end > self.next_offset
            || end > self.device_size
        {
            return false;
        }

        !self
            .freelist
            .iter_offset_order()
            .any(|(free_offset, free_size)| {
                let free_end = free_offset.saturating_add(free_size);
                free_offset < end && free_end > offset
            })
    }

    /// Start of the data region on device.
    pub fn data_region_start(&self) -> u64 {
        self.data_region_start
    }

    /// Current high-water mark — all allocations are below this offset.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Device alignment in bytes.
    pub fn device_alignment(&self) -> usize {
        self.alignment
    }

    /// The 128-bit device identity stored in the superblock.
    pub fn device_id(&self) -> [u8; 16] {
        self.device_id
    }

    /// The device identity formatted as a 32-character lowercase hex string.
    pub fn device_id_hex(&self) -> String {
        self.device_id
            .iter()
            .fold(String::with_capacity(32), |mut s, b| {
                use std::fmt::Write as _;
                let _ = write!(s, "{b:02x}");
                s
            })
    }

    /// Compute a snapshot of allocator statistics for observability.
    pub fn stats(&self) -> AllocatorStats {
        let mut total_free: u64 = 0;
        let mut largest: u64 = 0;
        for (_, size) in self.freelist.iter_offset_order() {
            total_free += size;
            if size > largest {
                largest = size;
            }
        }
        let data_capacity = self.device_size.saturating_sub(self.data_region_start);
        let high_water = self.next_offset.saturating_sub(self.data_region_start);
        let used = high_water.saturating_sub(total_free);
        let utilization = if data_capacity > 0 {
            used as f64 / data_capacity as f64
        } else {
            0.0
        };
        AllocatorStats {
            data_region_start: self.data_region_start,
            next_offset: self.next_offset,
            device_size: self.device_size,
            alignment: self.alignment,
            free_region_count: self.freelist.len(),
            total_free_bytes: total_free,
            largest_free_region: largest,
            used_bytes: used,
            utilization,
        }
    }

    /// Test-only: directly push a free region into the freelist without
    /// coalescing or validation. Used by F-G1-009 regression coverage to
    /// force a freelist that exceeds `MAX_PERSISTED_FREE_REGIONS` so
    /// `persist()` exercises the overflow branch deterministically.
    ///
    /// Public (rather than `pub(crate)`) so the integration-test crate
    /// can use it — the only callers are F-G1-009 regressions. Do not
    /// use from production code: it bypasses every allocator invariant
    /// (offset/size validation, coalescing, overlap rejection).
    ///
    /// Calls `maybe_promote()` after each insert so callers pushing
    /// thousands of entries do not pay O(n²) `Vec::insert` +
    /// `debug_assert_sorted` cost on the `Small` variant. Production
    /// `allocate`/`free` paths already trigger promotion at the same
    /// threshold, so this matches the realistic insertion shape.
    /// Without it, seeding 65 537 entries cost ~17 s in debug; with it,
    /// the same loop is sub-second.
    #[doc(hidden)]
    pub fn __test_force_push_free_region(&mut self, offset: u64, size: u64) {
        self.freelist.insert(offset, size);
        self.freelist.maybe_promote();
    }
}

// ---------------------------------------------------------------------------
// RecordAllocator trait — the storage-engine seam
// ---------------------------------------------------------------------------

/// The device-space allocator interface the engine, recovery, index-rebuild, and
/// checkpoint paths depend on, so a store can be backed by either the in-place
/// [`SlotAllocator`] (best-fit freelist) or the log-structured
/// [`crate::segment_allocator::SegmentAllocator`] (append cursor + defrag).
///
/// This is the increment-2 abstraction of the log-structured engine build (see
/// `bench/results/LOG_STRUCTURED_DATA_LAYER_DESIGN.md`). The engine holds
/// `Box<dyn RecordAllocator>` per store; callers route by `device_id`.
///
/// The common error is [`AllocatorError`]; a segment allocator maps its own
/// errors into it. Test/fault-injection hooks (`arm_fail_next_persist`,
/// `__test_force_push_free_region`) are intentionally NOT on the trait — tests
/// that need them construct the concrete [`SlotAllocator`].
///
/// `Send` is required because the allocator lives in `Mutex<Box<dyn RecordAllocator>>`
/// inside an `Arc<Engine>` shared across worker threads.
pub trait RecordAllocator: Send {
    /// Allocate a contiguous region of at least `size` bytes; see
    /// [`SlotAllocator::allocate`].
    fn allocate(&mut self, size: u64) -> Result<u64>;
    /// Allocate multiple regions with a single redo flush; see
    /// [`SlotAllocator::allocate_batch`].
    fn allocate_batch(&mut self, sizes: &[u64]) -> Result<Vec<Option<AllocatedRegion>>>;
    /// Reserve a batch in memory, deferring the durable journaling to the caller
    /// (orphan prevention); see [`SlotAllocator::reserve_batch`].
    fn reserve_batch(&mut self, sizes: &[u64]) -> Result<PendingBatchAllocation>;
    /// Finalize a reservation whose redo ops the caller has journaled.
    fn commit_pending(&mut self, pending: PendingBatchAllocation);
    /// Roll back a reservation whose redo batch could not be journaled.
    fn rollback_pending(&mut self, pending: PendingBatchAllocation);
    /// Return a region; see [`SlotAllocator::free`].
    fn free(&mut self, offset: u64, size: u64) -> Result<()>;
    /// Persist allocator state to the device header and fsync.
    fn persist(&self) -> Result<()>;
    /// Write the header without the durability fsync (checkpoint hoists the sync).
    fn persist_header_no_sync(&self) -> Result<()>;
    /// Apply an allocator-relevant redo entry during recovery; returns whether
    /// state changed. See [`SlotAllocator::replay_redo`].
    fn replay_redo(&mut self, op: &RedoOp) -> bool;
    /// Whether `[offset, offset+size)` is allocated (inside high-water, not free).
    fn is_allocated_range(&self, offset: u64, size: u64) -> bool;
    /// The free region containing `offset`, if any.
    fn free_region_containing(&self, offset: u64) -> Option<(u64, u64)>;
    /// Number of free regions (diagnostics).
    fn free_region_count(&self) -> usize;
    /// Observability snapshot.
    fn stats(&self) -> AllocatorStats;
    /// Current high-water mark.
    fn next_offset(&self) -> u64;
    /// Start of the data region.
    fn data_region_start(&self) -> u64;
    /// Device I/O alignment.
    fn device_alignment(&self) -> usize;
    /// 128-bit device identity.
    fn device_id(&self) -> [u8; 16];
    /// Device identity as lowercase hex.
    fn device_id_hex(&self) -> String;
    /// Attach a redo log for journaling allocate/free.
    fn set_redo_log(&mut self, redo_log: Arc<Mutex<RedoLog>>);
    /// Tag this allocator's store for redo routing.
    fn set_redo_device_id(&mut self, device_id: u8);
    /// The store tag stamped on this allocator's redo entries.
    fn redo_device_id(&self) -> u8;
    /// Whether a redo log is attached.
    fn has_redo_log(&self) -> bool;
    /// Enable/disable packed allocation mode.
    fn set_packed(&mut self, packed: bool);
    /// Whether packed mode is enabled.
    fn is_packed(&self) -> bool;
    /// Enable/disable append-only allocation mode.
    fn set_append_only(&mut self, append_only: bool);
    /// Whether append-only mode is enabled.
    fn is_append_only(&self) -> bool;

    /// Whether this is the LOG-STRUCTURED (segment) allocator, whose records are
    /// relocated to a new append-cursor offset on mutation (relocate-on-spend)
    /// rather than updated in place. Default `false` (the in-place
    /// [`SlotAllocator`]); the segment allocator overrides it. The engine caches
    /// this per store (like packed-ness) to branch the spend write path without
    /// locking the allocator on the hot path.
    fn is_log_structured(&self) -> bool {
        false
    }

    /// Recovery: ensure the allocation frontier is at least `end` (the end offset
    /// of the highest live record), so post-checkpoint records are not overwritten
    /// by a fresh allocation. Default no-op: the in-place [`SlotAllocator`]
    /// re-derives its high-water mark from replayed `AllocateRegion` ops. The
    /// append-cursor segment allocator overrides this (it journals no region ops,
    /// so its cursor is recomputed from the index after replay — design §3.2).
    fn recover_frontier_at_least(&mut self, end: u64) {
        let _ = end;
    }

    /// Test/fault-injection only: arm the next persist to fail once. On the trait
    /// so checkpoint crash tests can trigger it through the engine's boxed
    /// allocator. Compiled out of production builds.
    #[cfg(any(test, feature = "fault-injection"))]
    fn arm_fail_next_persist(&self);
}

/// A heap-allocated, dynamically-dispatched [`RecordAllocator`] — the concrete
/// type a [`crate::ops::Engine`] store holds so it can be backed by either the
/// in-place or the log-structured allocator.
pub type BoxedAllocator = Box<dyn RecordAllocator>;

/// Blanket impl so a `Box<dyn RecordAllocator>` is itself a [`RecordAllocator`].
/// Lets a [`BoxedAllocator`] be passed anywhere an `impl RecordAllocator` is
/// expected (e.g. the `Engine` constructors) and a `&[BoxedAllocator]` satisfy
/// generic allocator-slice bounds — forwarding every call to the inner trait
/// object.
impl RecordAllocator for BoxedAllocator {
    fn allocate(&mut self, size: u64) -> Result<u64> {
        (**self).allocate(size)
    }
    fn allocate_batch(&mut self, sizes: &[u64]) -> Result<Vec<Option<AllocatedRegion>>> {
        (**self).allocate_batch(sizes)
    }
    fn reserve_batch(&mut self, sizes: &[u64]) -> Result<PendingBatchAllocation> {
        (**self).reserve_batch(sizes)
    }
    fn commit_pending(&mut self, pending: PendingBatchAllocation) {
        (**self).commit_pending(pending)
    }
    fn rollback_pending(&mut self, pending: PendingBatchAllocation) {
        (**self).rollback_pending(pending)
    }
    fn free(&mut self, offset: u64, size: u64) -> Result<()> {
        (**self).free(offset, size)
    }
    fn persist(&self) -> Result<()> {
        (**self).persist()
    }
    fn persist_header_no_sync(&self) -> Result<()> {
        (**self).persist_header_no_sync()
    }
    fn replay_redo(&mut self, op: &RedoOp) -> bool {
        (**self).replay_redo(op)
    }
    fn is_allocated_range(&self, offset: u64, size: u64) -> bool {
        (**self).is_allocated_range(offset, size)
    }
    fn free_region_containing(&self, offset: u64) -> Option<(u64, u64)> {
        (**self).free_region_containing(offset)
    }
    fn free_region_count(&self) -> usize {
        (**self).free_region_count()
    }
    fn stats(&self) -> AllocatorStats {
        (**self).stats()
    }
    fn next_offset(&self) -> u64 {
        (**self).next_offset()
    }
    fn data_region_start(&self) -> u64 {
        (**self).data_region_start()
    }
    fn device_alignment(&self) -> usize {
        (**self).device_alignment()
    }
    fn device_id(&self) -> [u8; 16] {
        (**self).device_id()
    }
    fn device_id_hex(&self) -> String {
        (**self).device_id_hex()
    }
    fn set_redo_log(&mut self, redo_log: Arc<Mutex<RedoLog>>) {
        (**self).set_redo_log(redo_log)
    }
    fn set_redo_device_id(&mut self, device_id: u8) {
        (**self).set_redo_device_id(device_id)
    }
    fn redo_device_id(&self) -> u8 {
        (**self).redo_device_id()
    }
    fn has_redo_log(&self) -> bool {
        (**self).has_redo_log()
    }
    fn set_packed(&mut self, packed: bool) {
        (**self).set_packed(packed)
    }
    fn is_packed(&self) -> bool {
        (**self).is_packed()
    }
    fn set_append_only(&mut self, append_only: bool) {
        (**self).set_append_only(append_only)
    }
    fn is_append_only(&self) -> bool {
        (**self).is_append_only()
    }
    fn is_log_structured(&self) -> bool {
        (**self).is_log_structured()
    }
    fn recover_frontier_at_least(&mut self, end: u64) {
        (**self).recover_frontier_at_least(end)
    }
    #[cfg(any(test, feature = "fault-injection"))]
    fn arm_fail_next_persist(&self) {
        (**self).arm_fail_next_persist()
    }
}

impl RecordAllocator for SlotAllocator {
    fn allocate(&mut self, size: u64) -> Result<u64> {
        SlotAllocator::allocate(self, size)
    }
    fn allocate_batch(&mut self, sizes: &[u64]) -> Result<Vec<Option<AllocatedRegion>>> {
        SlotAllocator::allocate_batch(self, sizes)
    }
    fn reserve_batch(&mut self, sizes: &[u64]) -> Result<PendingBatchAllocation> {
        SlotAllocator::reserve_batch(self, sizes)
    }
    fn commit_pending(&mut self, pending: PendingBatchAllocation) {
        SlotAllocator::commit_pending(self, pending)
    }
    fn rollback_pending(&mut self, pending: PendingBatchAllocation) {
        SlotAllocator::rollback_pending(self, pending)
    }
    fn free(&mut self, offset: u64, size: u64) -> Result<()> {
        SlotAllocator::free(self, offset, size)
    }
    fn persist(&self) -> Result<()> {
        SlotAllocator::persist(self)
    }
    fn persist_header_no_sync(&self) -> Result<()> {
        SlotAllocator::persist_header_no_sync(self)
    }
    fn replay_redo(&mut self, op: &RedoOp) -> bool {
        SlotAllocator::replay_redo(self, op)
    }
    fn is_allocated_range(&self, offset: u64, size: u64) -> bool {
        SlotAllocator::is_allocated_range(self, offset, size)
    }
    fn free_region_containing(&self, offset: u64) -> Option<(u64, u64)> {
        SlotAllocator::free_region_containing(self, offset)
    }
    fn free_region_count(&self) -> usize {
        SlotAllocator::free_region_count(self)
    }
    fn stats(&self) -> AllocatorStats {
        SlotAllocator::stats(self)
    }
    fn next_offset(&self) -> u64 {
        SlotAllocator::next_offset(self)
    }
    fn data_region_start(&self) -> u64 {
        SlotAllocator::data_region_start(self)
    }
    fn device_alignment(&self) -> usize {
        SlotAllocator::device_alignment(self)
    }
    fn device_id(&self) -> [u8; 16] {
        SlotAllocator::device_id(self)
    }
    fn device_id_hex(&self) -> String {
        SlotAllocator::device_id_hex(self)
    }
    fn set_redo_log(&mut self, redo_log: Arc<Mutex<RedoLog>>) {
        SlotAllocator::set_redo_log(self, redo_log)
    }
    fn set_redo_device_id(&mut self, device_id: u8) {
        SlotAllocator::set_redo_device_id(self, device_id)
    }
    fn redo_device_id(&self) -> u8 {
        SlotAllocator::redo_device_id(self)
    }
    fn has_redo_log(&self) -> bool {
        SlotAllocator::has_redo_log(self)
    }
    fn set_packed(&mut self, packed: bool) {
        SlotAllocator::set_packed(self, packed)
    }
    fn is_packed(&self) -> bool {
        SlotAllocator::is_packed(self)
    }
    fn set_append_only(&mut self, append_only: bool) {
        SlotAllocator::set_append_only(self, append_only)
    }
    fn is_append_only(&self) -> bool {
        SlotAllocator::is_append_only(self)
    }
    #[cfg(any(test, feature = "fault-injection"))]
    fn arm_fail_next_persist(&self) {
        SlotAllocator::arm_fail_next_persist(self)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemoryDevice;

    fn test_device(size_mb: u64) -> Arc<MemoryDevice> {
        Arc::new(MemoryDevice::new(size_mb * 1024 * 1024, 4096).unwrap())
    }

    #[test]
    fn allocate_returns_offset_in_data_region() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let offset = alloc.allocate(8192).unwrap();
        assert!(offset >= DATA_REGION_OFFSET);
    }

    #[test]
    fn allocate_returns_aligned_offset() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let offset = alloc.allocate(100).unwrap(); // Small, not aligned
        assert_eq!(offset % 4096, 0);
    }

    #[test]
    fn allocate_two_no_overlap() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let o1 = alloc.allocate(8192).unwrap();
        let o2 = alloc.allocate(8192).unwrap();
        assert_ne!(o1, o2);
        // With alignment, each alloc is at least 8192 bytes apart
        assert!(o2 >= o1 + 8192 || o1 >= o2 + 8192);
    }

    #[test]
    fn allocate_100_regions_no_overlap() {
        let dev = test_device(64);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let mut allocations: Vec<(u64, u64)> = Vec::new();

        for i in 0..100 {
            let size = 4096 + (i * 100); // Varying sizes
            let offset = alloc.allocate(size).unwrap();
            assert_eq!(offset % 4096, 0);
            let aligned = size.div_ceil(4096) * 4096;
            allocations.push((offset, aligned));
        }

        // Verify no overlap
        allocations.sort_by_key(|&(o, _)| o);
        for w in allocations.windows(2) {
            let (o1, s1) = w[0];
            let (o2, _) = w[1];
            assert!(
                o1 + s1 <= o2,
                "overlap: region [{o1}, {}) overlaps [{o2}, ...)",
                o1 + s1
            );
        }
    }

    #[test]
    fn free_and_reuse() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let o1 = alloc.allocate(4096).unwrap();
        alloc.free(o1, 4096).unwrap();
        let o2 = alloc.allocate(4096).unwrap();
        assert_eq!(o1, o2); // Reused the freed region
    }

    #[test]
    fn append_only_defaults_off() {
        let dev = test_device(16);
        let alloc = SlotAllocator::new(dev).unwrap();
        assert!(!alloc.is_append_only());
    }

    #[test]
    fn append_only_does_not_reuse_freed_region() {
        // Contrast with `free_and_reuse`: in append-only mode a freed region is
        // NOT handed back out — the next allocation extends the high-water mark.
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_append_only(true);
        assert!(alloc.is_append_only());

        let o1 = alloc.allocate(4096).unwrap();
        let o2 = alloc.allocate(4096).unwrap();
        assert_eq!(o2, o1 + 4096);
        alloc.free(o1, 4096).unwrap();

        let o3 = alloc.allocate(4096).unwrap();
        // Best-fit mode would return o1 here; append-only extends past o2.
        assert_ne!(o3, o1);
        assert_eq!(o3, o2 + 4096);
    }

    #[test]
    fn append_only_still_tracks_frees_for_accounting() {
        // Frees must still be journaled/tracked (recovery + accounting) even
        // though allocation never reuses them.
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_append_only(true);
        let o1 = alloc.allocate(4096).unwrap();
        let _o2 = alloc.allocate(4096).unwrap();
        assert_eq!(alloc.free_region_count(), 0);
        alloc.free(o1, 4096).unwrap();
        assert_eq!(alloc.free_region_count(), 1);
        // The freed region sits in the freelist but is not reused.
        let o3 = alloc.allocate(4096).unwrap();
        assert_ne!(o3, o1);
        assert_eq!(alloc.free_region_count(), 1);
    }

    #[test]
    fn append_only_composes_with_packed() {
        // Packed + append-only: small records pack sequentially at RECORD_ALIGN
        // within a block, and freed records are never reused.
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_packed(true);
        alloc.set_append_only(true);
        let o0 = alloc.allocate(600).unwrap();
        let o1 = alloc.allocate(600).unwrap();
        // Packed: 600 is already 8-aligned, so o1 packs immediately after o0.
        assert_eq!(o1, o0 + 600);
        alloc.free(o0, 600).unwrap();
        let o2 = alloc.allocate(600).unwrap();
        // Append-only: o0's hole is not reused; o2 extends past o1.
        assert_ne!(o2, o0);
        assert_eq!(o2, o1 + 600);
    }

    #[test]
    fn free_merges_adjacent() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let o1 = alloc.allocate(4096).unwrap();
        let o2 = alloc.allocate(4096).unwrap();
        assert_eq!(o2, o1 + 4096);

        alloc.free(o1, 4096).unwrap();
        alloc.free(o2, 4096).unwrap();

        // Should have merged into one free region
        assert_eq!(alloc.free_region_count(), 1);

        // Allocate 8192 — should fit in the merged region
        let o3 = alloc.allocate(8192).unwrap();
        assert_eq!(o3, o1);
    }

    #[test]
    fn free_smaller_reuse() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        let o1 = alloc.allocate(8192).unwrap();
        alloc.free(o1, 8192).unwrap();

        // Allocate something smaller — should use the freed region
        let o2 = alloc.allocate(4096).unwrap();
        assert_eq!(o2, o1);
    }

    /// IJ-7: an exact double-free of the same region must be rejected with
    /// [`AllocatorError::DoubleFree`], not silently corrupt the freelist.
    #[test]
    fn double_free_exact_rejected() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let o1 = alloc.allocate(4096).unwrap();

        alloc.free(o1, 4096).unwrap();
        let before = alloc.free_region_count();

        let err = alloc.free(o1, 4096).unwrap_err();
        assert!(
            matches!(
                err,
                AllocatorError::DoubleFree { offset, free_offset, .. }
                    if offset == o1 && free_offset == o1
            ),
            "exact double-free must yield DoubleFree, got {err:?}",
        );
        // Rejected free must not have mutated the freelist.
        assert_eq!(
            alloc.free_region_count(),
            before,
            "rejected double-free must leave the freelist unchanged",
        );

        // The single legitimately-freed region is still allocatable exactly
        // once (no double-allocation).
        let a = alloc.allocate(4096).unwrap();
        assert_eq!(a, o1);
        let b = alloc.allocate(4096).unwrap();
        assert_ne!(b, o1, "the freed region must not be handed out twice");
    }

    /// IJ-7: a free whose range partially overlaps an existing free region
    /// (sharing the leading or trailing bytes) must be rejected.
    #[test]
    fn overlapping_free_rejected() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        // Two adjacent 4 KiB regions, freed and merged into [o1, o1+8192).
        let o1 = alloc.allocate(4096).unwrap();
        let o2 = alloc.allocate(4096).unwrap();
        assert_eq!(o2, o1 + 4096);
        alloc.free(o1, 4096).unwrap();
        alloc.free(o2, 4096).unwrap();
        assert_eq!(alloc.free_region_count(), 1);
        let before = alloc.free_region_count();

        // Free [o1, o1+8192) — fully covers the existing merged free region.
        let err = alloc.free(o1, 8192).unwrap_err();
        assert!(
            matches!(err, AllocatorError::DoubleFree { .. }),
            "free overlapping the front of a free region must be rejected, got {err:?}",
        );

        // Free [o2, o2+4096) — sits inside the existing free region (its
        // start is after the region start). Must also be rejected.
        let err = alloc.free(o2, 4096).unwrap_err();
        assert!(
            matches!(err, AllocatorError::DoubleFree { .. }),
            "free overlapping the back of a free region must be rejected, got {err:?}",
        );

        assert_eq!(
            alloc.free_region_count(),
            before,
            "rejected overlapping frees must leave the freelist unchanged",
        );
    }

    /// IJ-7: a free of a never-allocated hole that overlaps the freelist is
    /// rejected, while a genuine free of a freshly allocated region still
    /// succeeds (no false positives on the legitimate path).
    #[test]
    fn legitimate_free_after_double_free_guard_still_works() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let o1 = alloc.allocate(4096).unwrap();
        let o2 = alloc.allocate(4096).unwrap();
        // o1 and o2 are non-adjacent in the freelist sense once freed in a
        // gap pattern; free o2 first, then o1 — both legitimate.
        alloc.free(o2, 4096).unwrap();
        alloc.free(o1, 4096).unwrap();
        // Re-freeing either now overlaps and must be rejected.
        assert!(matches!(
            alloc.free(o1, 4096).unwrap_err(),
            AllocatorError::DoubleFree { .. }
        ));
        assert!(matches!(
            alloc.free(o2, 4096).unwrap_err(),
            AllocatorError::DoubleFree { .. }
        ));
    }

    #[test]
    fn persist_and_recover() {
        let dev = test_device(16);

        let o1;
        let o2;
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            o1 = alloc.allocate(8192).unwrap();
            o2 = alloc.allocate(4096).unwrap();
            alloc.free(o1, 8192).unwrap();
            alloc.persist().unwrap();
        }

        // Recover
        let mut alloc2 = SlotAllocator::recover(dev).unwrap();

        // The freed region should still be available
        let o3 = alloc2.allocate(8192).unwrap();
        assert_eq!(o3, o1);

        // New allocation should not overlap with o2
        let o4 = alloc2.allocate(4096).unwrap();
        assert_ne!(o4, o2);
        assert!(o4 >= o2 + 4096 || o4 + 4096 <= o2);
    }

    /// B-1 audit fix: `persist()` must issue a device sync barrier so the
    /// header survives power loss with a volatile drive write cache. The
    /// checkpoint doc ("allocator persist is fsynced before returning")
    /// relied on this; pre-fix the pwrite sat in the cache and a power
    /// loss after redo compaction reverted the header while its covering
    /// `AllocateRegion` entries were already reclaimed.
    #[test]
    fn persist_survives_simulated_power_loss() {
        let dev = Arc::new(MemoryDevice::new_volatile(16 * 1024 * 1024, 4096).unwrap());

        let o1;
        let next_offset;
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            o1 = alloc.allocate(8192).unwrap();
            let _o2 = alloc.allocate(4096).unwrap();
            alloc.free(o1, 8192).unwrap();
            next_offset = alloc.next_offset();
            alloc.persist().unwrap();
        }

        assert!(dev.simulate_power_loss(), "device must be volatile");

        let mut alloc2 = SlotAllocator::recover(dev)
            .expect("persisted allocator header must survive power loss");
        assert_eq!(
            alloc2.next_offset(),
            next_offset,
            "high-water mark must match the persisted state"
        );
        // The freed region survived persist + power loss and is reusable.
        let o3 = alloc2.allocate(8192).unwrap();
        assert_eq!(o3, o1, "freelist must match the persisted state");
    }

    #[test]
    fn persist_recover_then_allocate_no_overlap() {
        let dev = test_device(16);
        let o1;
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            o1 = alloc.allocate(4096).unwrap();
            alloc.persist().unwrap();
        }

        let mut alloc2 = SlotAllocator::recover(dev).unwrap();
        let o2 = alloc2.allocate(4096).unwrap();
        // o2 must not overlap with o1
        assert!(o2 >= o1 + 4096 || o2 + 4096 <= o1);
    }

    #[test]
    fn allocate_until_full() {
        // 2 MB device, 1 MB header → 1 MB data region → ~256 × 4096 blocks
        let dev = test_device(2);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        let mut count = 0;
        loop {
            match alloc.allocate(4096) {
                Ok(_) => count += 1,
                Err(AllocatorError::DeviceFull { .. }) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(count > 0);

        // Next allocation must also fail
        assert!(alloc.allocate(4096).is_err());
    }

    #[test]
    fn fragment_allocate_free_pattern() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        // Allocate A, B, C, D
        let a = alloc.allocate(4096).unwrap();
        let b = alloc.allocate(4096).unwrap();
        let c = alloc.allocate(4096).unwrap();
        let d = alloc.allocate(4096).unwrap();

        // Free B and D
        alloc.free(b, 4096).unwrap();
        alloc.free(d, 4096).unwrap();

        // Allocate E with size = B (4096). Should reuse B's space.
        let e = alloc.allocate(4096).unwrap();
        assert!(e == b || e == d); // Reuses one of the freed regions

        // A and C should remain intact (they were never freed)
        assert_ne!(e, a);
        assert_ne!(e, c);
    }

    #[test]
    fn device_id_generated_on_new() {
        let dev = test_device(16);
        let alloc = SlotAllocator::new(dev).unwrap();
        // A freshly generated device identity must not be all zeros.
        assert_ne!(alloc.device_id(), [0u8; 16]);
    }

    #[test]
    fn device_id_persisted_and_recovered() {
        let dev = test_device(16);
        let original_id;
        {
            let alloc = SlotAllocator::new(dev.clone()).unwrap();
            original_id = alloc.device_id();
            alloc.persist().unwrap();
        }
        let recovered = SlotAllocator::recover(dev).unwrap();
        assert_eq!(recovered.device_id(), original_id);
    }

    #[test]
    fn device_id_different_per_allocator() {
        let dev1 = test_device(16);
        let dev2 = test_device(16);
        let alloc1 = SlotAllocator::new(dev1).unwrap();
        let alloc2 = SlotAllocator::new(dev2).unwrap();
        // Two independently created allocators must have different identities.
        assert_ne!(alloc1.device_id(), alloc2.device_id());
    }

    #[test]
    fn device_id_hex_format() {
        let dev = test_device(16);
        let alloc = SlotAllocator::new(dev).unwrap();
        let hex = alloc.device_id_hex();
        assert_eq!(hex.len(), 32, "device ID hex must be exactly 32 characters");
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()),
            "device ID hex must be lowercase hex digits, got: {hex}"
        );
    }

    #[test]
    fn header_version_persisted_and_recovered() {
        let dev = test_device(16);
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            alloc.allocate(4096).unwrap();
            alloc.persist().unwrap();
        }

        // Read raw header and verify the version field at bytes 40..44.
        let mut buf = crate::device::AlignedBuf::new(4096, 4096);
        dev.pread(&mut buf, 0).unwrap();
        let version = u32::from_le_bytes(buf[40..44].try_into().unwrap());
        assert_eq!(
            version, HEADER_VERSION,
            "persisted header version must be {HEADER_VERSION}"
        );
    }

    #[test]
    fn recover_rejects_future_version() {
        let dev = test_device(16);
        {
            let alloc = SlotAllocator::new(dev.clone()).unwrap();
            alloc.persist().unwrap();
        }

        // Overwrite the version field with a future version (999).
        let mut buf = crate::device::AlignedBuf::new(4096, 4096);
        dev.pread(&mut buf, 0).unwrap();
        buf[40..44].copy_from_slice(&999u32.to_le_bytes());
        dev.pwrite(&buf, 0).unwrap();

        match SlotAllocator::recover(dev) {
            Err(AllocatorError::UnsupportedVersion(999)) => {}
            Err(other) => panic!("expected UnsupportedVersion(999), got: {other}"),
            Ok(_) => panic!("expected UnsupportedVersion(999), but recover succeeded"),
        }
    }

    #[test]
    fn recover_on_fresh_device_returns_no_persisted_state() {
        // B-2: a genuinely fresh device (all-zero header region) must be
        // distinguishable from a corrupt header so startup can safely
        // create a fresh allocator ONLY in this case.
        let dev = test_device(16);
        match SlotAllocator::recover(dev) {
            Err(AllocatorError::NoPersistedState) => {}
            Err(other) => panic!("expected NoPersistedState, got: {other}"),
            Ok(_) => panic!("expected NoPersistedState, but recover succeeded"),
        }
    }

    #[test]
    fn recover_nonzero_garbage_header_returns_corrupted_header() {
        // B-2: a header region with non-zero garbage (bad magic) is
        // corruption, NOT a fresh device — recover must return
        // CorruptedHeader so startup fails closed instead of silently
        // creating a fresh allocator that would overwrite live records.
        let dev = test_device(16);
        let mut buf = crate::device::AlignedBuf::new(4096, 4096);
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (i % 251) as u8 ^ 0x5A;
        }
        dev.pwrite(&buf, 0).unwrap();

        match SlotAllocator::recover(dev) {
            Err(AllocatorError::CorruptedHeader) => {}
            Err(other) => panic!("expected CorruptedHeader, got: {other}"),
            Ok(_) => panic!("expected CorruptedHeader, but recover succeeded"),
        }
    }

    #[test]
    fn recover_torn_header_is_not_a_fresh_allocator() {
        // B-2 regression: persist a valid header, then tear it (flip
        // bytes inside the CRC-covered range, magic left intact). The
        // failure mode under audit was `Err(_) => SlotAllocator::new`,
        // which restarts allocation at DATA_REGION_OFFSET and overwrites
        // the live record at o1. Recover must return HeaderCorruption.
        let dev = test_device(16);
        let o1;
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            o1 = alloc.allocate(8192).unwrap();
            alloc.persist().unwrap();
        }
        assert_eq!(
            o1, DATA_REGION_OFFSET,
            "first allocation starts the data region"
        );

        // Tear the header: flip the next_offset field (bytes 8..16)
        // without touching the magic or the stored CRC.
        let mut buf = crate::device::AlignedBuf::new(4096, 4096);
        dev.pread(&mut buf, 0).unwrap();
        for b in &mut buf[8..16] {
            *b ^= 0xFF;
        }
        dev.pwrite(&buf, 0).unwrap();

        match SlotAllocator::recover(dev) {
            Err(AllocatorError::HeaderCorruption { expected, actual }) => {
                assert_ne!(expected, actual, "CRC mismatch must be reported");
            }
            Err(other) => panic!("expected HeaderCorruption, got: {other}"),
            Ok(_) => panic!("expected HeaderCorruption, but recover succeeded"),
        }
    }

    #[test]
    fn recover_rejects_out_of_range_count_without_panicking() {
        // REL-107: persist a valid header (good magic + good version), then
        // overwrite the freelist `count` field (bytes 16..24) with values
        // larger than MAX_PERSISTED_FREE_REGIONS. recover must return
        // CorruptedHeader — and critically must NOT panic on the
        // `count * 16` / `covered_end` arithmetic (REL-100), even under
        // overflow-checks (debug). The bound is checked before any CRC work,
        // so the variant is CorruptedHeader, not HeaderCorruption.
        for bad_count in [
            (MAX_PERSISTED_FREE_REGIONS as u64) + 1,
            u64::MAX,            // would overflow `count * 16` if multiplied unchecked
            (u64::MAX / 16) + 1, // smallest value whose *16 overflows usize on 64-bit
        ] {
            let dev = test_device(16);
            {
                let alloc = SlotAllocator::new(dev.clone()).unwrap();
                alloc.persist().unwrap();
            }

            let mut buf = crate::device::AlignedBuf::new(4096, 4096);
            dev.pread(&mut buf, 0).unwrap();
            buf[16..24].copy_from_slice(&bad_count.to_le_bytes());
            dev.pwrite(&buf, 0).unwrap();

            match SlotAllocator::recover(dev) {
                Err(AllocatorError::CorruptedHeader) => {}
                Err(other) => {
                    panic!("count={bad_count}: expected CorruptedHeader, got: {other}")
                }
                Ok(_) => {
                    panic!("count={bad_count}: expected CorruptedHeader, but recover succeeded")
                }
            }
        }
    }

    #[test]
    fn recover_reads_freelist_at_correct_offset() {
        let dev = test_device(16);
        let o1;
        let o2;
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            o1 = alloc.allocate(4096).unwrap();
            o2 = alloc.allocate(8192).unwrap();
            // Free o1 so the freelist has one entry.
            alloc.free(o1, 4096).unwrap();
            alloc.persist().unwrap();
        }

        let mut recovered = SlotAllocator::recover(dev).unwrap();

        // The recovered allocator must see the freed region and reuse it.
        let reused = recovered.allocate(4096).unwrap();
        assert_eq!(
            reused, o1,
            "recovered freelist should offer the freed region first"
        );

        // A subsequent allocation must not overlap with o2.
        let next = recovered.allocate(4096).unwrap();
        assert!(
            next >= o2 + 8192 || next + 4096 <= o2,
            "allocation at {next} must not overlap with o2 at {o2}"
        );
    }

    #[test]
    fn best_fit_picks_smallest_sufficient_region() {
        let dev = test_device(64);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        // Allocate three blocks, free them to create regions of different sizes.
        let o1 = alloc.allocate(4096).unwrap(); // 4K
        let _o2 = alloc.allocate(4096).unwrap(); // 4K (spacer, kept)
        let o3 = alloc.allocate(8192).unwrap(); // 8K
        let _o4 = alloc.allocate(4096).unwrap(); // 4K (spacer, kept)
        let o5 = alloc.allocate(16384).unwrap(); // 16K

        // Free them to create: 4K hole at o1, 8K hole at o3, 16K hole at o5.
        alloc.free(o1, 4096).unwrap();
        alloc.free(o3, 8192).unwrap();
        alloc.free(o5, 16384).unwrap();
        assert_eq!(alloc.free_region_count(), 3);

        // Allocating 4K should pick o1 (exact fit), not o3 or o5.
        let got = alloc.allocate(4096).unwrap();
        assert_eq!(got, o1, "best-fit should pick the 4K region");
        assert_eq!(alloc.free_region_count(), 2); // o3 and o5 remain

        // Allocating 8K should pick o3 (exact fit).
        let got = alloc.allocate(8192).unwrap();
        assert_eq!(got, o3, "best-fit should pick the 8K region");
    }

    #[test]
    fn freelist_consistent_after_many_operations() {
        let dev = test_device(64);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        // Allocate 100 regions, free even-indexed ones.
        let mut offsets = Vec::new();
        for _ in 0..100 {
            offsets.push(alloc.allocate(4096).unwrap());
        }
        for i in (0..100).step_by(2) {
            alloc.free(offsets[i], 4096).unwrap();
        }

        // Should have promoted to Large backend (50 free regions > 64 threshold).
        // In any case, length should be consistent.
        let count = alloc.free_region_count();
        assert!(count > 0, "should have free regions");

        // If Large backend, verify dual-index consistency.
        if let FreelistBackend::Large {
            ref by_offset,
            ref by_size,
        } = alloc.freelist
        {
            assert_eq!(
                by_offset.len(),
                by_size.len(),
                "dual indexes must stay in sync"
            );
            for (&off, &sz) in by_offset {
                assert!(
                    by_size.contains(&(sz, off)),
                    "by_size missing entry ({sz}, {off})"
                );
            }
        }

        // Allocate everything back from the freelist.
        let before_count = alloc.free_region_count();
        for _ in 0..before_count {
            alloc.allocate(4096).unwrap();
        }
        assert_eq!(alloc.free_region_count(), 0, "freelist should be empty");
    }

    #[test]
    fn three_way_coalesce() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        // Allocate three contiguous blocks.
        let o1 = alloc.allocate(4096).unwrap();
        let o2 = alloc.allocate(4096).unwrap();
        let o3 = alloc.allocate(4096).unwrap();
        assert_eq!(o2, o1 + 4096);
        assert_eq!(o3, o2 + 4096);

        // Free outer blocks first, then the middle.
        alloc.free(o1, 4096).unwrap();
        alloc.free(o3, 4096).unwrap();
        assert_eq!(alloc.free_region_count(), 2);

        // Freeing the middle should merge all three into one 12K region.
        alloc.free(o2, 4096).unwrap();
        assert_eq!(alloc.free_region_count(), 1);

        // The merged region should be at o1 with size 12K.
        let (off, sz) = alloc.freelist.iter_offset_order().next().unwrap();
        assert_eq!(off, o1);
        assert_eq!(sz, 12288);
    }

    #[test]
    fn promote_demote_transitions() {
        let dev = test_device(64);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        // Start as Small.
        assert!(matches!(alloc.freelist, FreelistBackend::Small(_)));

        // Allocate many blocks, then free enough to trigger promotion.
        let mut offsets = Vec::new();
        for _ in 0..200 {
            offsets.push(alloc.allocate(4096).unwrap());
        }
        // Free every other block to create 100 non-adjacent free regions.
        for i in (0..200).step_by(2) {
            alloc.free(offsets[i], 4096).unwrap();
        }
        // Should have promoted to Large (100 entries > 64 threshold).
        assert!(
            matches!(alloc.freelist, FreelistBackend::Large { .. }),
            "should promote to Large after 100 free regions"
        );

        // Allocate back most of the free regions to shrink below demote threshold.
        for _ in 0..75 {
            alloc.allocate(4096).unwrap();
        }
        // Should have demoted back to Small (25 entries < 32 threshold).
        assert!(
            matches!(alloc.freelist, FreelistBackend::Small(_)),
            "should demote to Small after allocating most free regions back"
        );

        // The remaining entries should still be correct.
        let remaining = alloc.free_region_count();
        assert!(remaining > 0 && remaining < DEMOTE_THRESHOLD);
    }

    // -----------------------------------------------------------------------
    // Redo-journaling tests (C6).
    // -----------------------------------------------------------------------

    /// Test helper: a redo log whose `flush()` can be made to fail on demand.
    ///
    /// Wraps a normal `RedoLog` on a backing `MemoryDevice` but, when the
    /// `fail` flag is set, pwrite is redirected to a zero-sized device so
    /// the underlying flush fails. Used to exercise the rollback path of
    /// [`SlotAllocator::allocate`] / [`SlotAllocator::free`].
    ///
    /// We can't easily override the RedoLog's internals, so instead we
    /// build the log against a custom `BlockDevice` that returns an error
    /// on pwrite when a flag is set.
    struct FailableDevice {
        inner: Arc<MemoryDevice>,
        fail: std::sync::atomic::AtomicBool,
    }

    impl FailableDevice {
        fn new(inner: Arc<MemoryDevice>) -> Self {
            Self {
                inner,
                fail: std::sync::atomic::AtomicBool::new(false),
            }
        }
        fn set_fail(&self, v: bool) {
            self.fail.store(v, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl crate::device::BlockDevice for FailableDevice {
        fn pread(&self, buf: &mut [u8], offset: u64) -> std::result::Result<usize, DeviceError> {
            self.inner.pread(buf, offset)
        }
        fn pwrite(&self, buf: &[u8], offset: u64) -> std::result::Result<usize, DeviceError> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(DeviceError::Io(std::io::Error::other(
                    "injected pwrite failure",
                )));
            }
            self.inner.pwrite(buf, offset)
        }
        fn sync(&self) -> std::result::Result<(), DeviceError> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(DeviceError::Io(std::io::Error::other(
                    "injected sync failure",
                )));
            }
            self.inner.sync()
        }
        fn size(&self) -> u64 {
            self.inner.size()
        }
        fn alignment(&self) -> usize {
            self.inner.alignment()
        }
    }

    fn make_redo_log(size: u64) -> (Arc<MemoryDevice>, Arc<Mutex<RedoLog>>) {
        let dev = Arc::new(MemoryDevice::new(size, 4096).unwrap());
        let log = RedoLog::open(dev.clone(), 0, size).unwrap();
        (dev, Arc::new(Mutex::new(log)))
    }

    fn make_failable_redo_log(size: u64) -> (Arc<FailableDevice>, Arc<Mutex<RedoLog>>) {
        let inner = Arc::new(MemoryDevice::new(size, 4096).unwrap());
        let dev = Arc::new(FailableDevice::new(inner));
        let log = RedoLog::open(dev.clone(), 0, size).unwrap();
        (dev, Arc::new(Mutex::new(log)))
    }

    #[test]
    fn allocate_journals_allocate_region_op() {
        let dev = test_device(16);
        let (_redo_dev, redo) = make_redo_log(1024 * 1024);

        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_redo_log(redo.clone());

        let offset = alloc.allocate(8192).unwrap();

        // The redo log must contain exactly one AllocateRegion entry
        // referencing the returned offset.
        let entries = redo.lock().recover().unwrap();
        assert_eq!(
            entries.len(),
            1,
            "expected exactly 1 redo entry after allocate"
        );
        match &entries[0].op {
            RedoOp::AllocateRegion {
                offset: redo_offset,
                size,
                device_id,
            } => {
                assert_eq!(*redo_offset, offset);
                assert_eq!(*size, 8192);
                assert_eq!(*device_id, 0);
            }
            other => panic!("expected AllocateRegion redo entry, got {other:?}"),
        }
    }

    #[test]
    fn free_journals_free_region_op() {
        let dev = test_device(16);
        let (_redo_dev, redo) = make_redo_log(1024 * 1024);

        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_redo_log(redo.clone());

        let offset = alloc.allocate(4096).unwrap();
        alloc.free(offset, 4096).unwrap();

        let entries = redo.lock().recover().unwrap();
        assert_eq!(
            entries.len(),
            2,
            "expected 2 redo entries (allocate + free)"
        );
        match &entries[1].op {
            RedoOp::FreeRegion {
                offset: redo_offset,
                size,
                device_id,
            } => {
                assert_eq!(*redo_offset, offset);
                assert_eq!(*size, 4096);
                assert_eq!(*device_id, 0);
            }
            other => panic!("expected FreeRegion redo entry, got {other:?}"),
        }
    }

    #[test]
    fn allocate_rollback_on_redo_flush_failure_high_water() {
        // Flush failure on a high-water allocation must roll back
        // next_offset and not consume any space.
        let data_dev = test_device(16);
        let (redo_dev, redo) = make_failable_redo_log(1024 * 1024);

        let mut alloc = SlotAllocator::new(data_dev).unwrap();
        alloc.set_redo_log(redo);

        let before_next = alloc.next_offset();
        let before_free_count = alloc.free_region_count();

        // Inject failure so the redo flush errors out.
        redo_dev.set_fail(true);
        let result = alloc.allocate(8192);
        redo_dev.set_fail(false);

        match result {
            Err(AllocatorError::RedoLogFailure { detail }) => {
                assert!(!detail.is_empty(), "detail message must be non-empty");
            }
            other => panic!("expected RedoLogFailure, got {other:?}"),
        }

        assert_eq!(
            alloc.next_offset(),
            before_next,
            "next_offset must be rolled back on redo flush failure"
        );
        assert_eq!(
            alloc.free_region_count(),
            before_free_count,
            "freelist must be unchanged on redo flush failure"
        );
    }

    #[test]
    fn allocate_rollback_on_redo_flush_failure_from_freelist() {
        // Set up: allocate and free a region so the next allocate() will
        // come from the freelist. Then inject failure and verify the
        // freelist is restored to its original state.
        //
        // F-G4-002: a redo flush failure poisons the log. Subsequent
        // append/flush calls return RedoError::Poisoned, so we can no
        // longer assert "the region is reusable by a follow-up
        // allocate()" — the next allocate() would fail with the
        // poisoned-log error rather than the freelist's split-fragment
        // history. Verify the freelist/next_offset invariants directly
        // instead.
        let data_dev = test_device(16);
        let (redo_dev, redo) = make_failable_redo_log(1024 * 1024);

        let mut alloc = SlotAllocator::new(data_dev).unwrap();
        alloc.set_redo_log(redo);

        let offset = alloc.allocate(8192).unwrap();
        alloc.free(offset, 8192).unwrap();
        let before_count = alloc.free_region_count();
        let before_next = alloc.next_offset();
        assert_eq!(before_count, 1, "freelist should contain the freed region");

        // Small allocation that would split the 8K free region.
        redo_dev.set_fail(true);
        let result = alloc.allocate(4096);
        redo_dev.set_fail(false);

        match result {
            Err(AllocatorError::RedoLogFailure { .. }) => {}
            other => panic!("expected RedoLogFailure, got {other:?}"),
        }

        assert_eq!(
            alloc.free_region_count(),
            before_count,
            "freelist entry count must be restored on redo flush failure"
        );
        assert_eq!(
            alloc.next_offset(),
            before_next,
            "next_offset must be unchanged"
        );
        assert_eq!(
            alloc.stats().total_free_bytes,
            8192,
            "the full 8 KiB region must still be intact in the freelist \
             (no left-over fragmentation from the rolled-back split)"
        );
    }

    /// F-G1-016 (C-5): on rollback, if the freelist contains free
    /// regions adjacent to the rolled-back allocation, the rollback
    /// must coalesce them into a single region — matching the
    /// invariant `free()` maintains.
    ///
    /// Single-threaded today this is unreachable (the freelist
    /// invariant says "no two adjacent free regions" because every
    /// `free()` already coalesces, so `best_fit` cannot expose a
    /// freelist where the selected region has a free neighbour). The
    /// defensive change is for a future world where rollback could
    /// race with another `free()` or where someone breaks the
    /// invariant somehow else. The test stages the invariant
    /// violation explicitly with `__test_force_push_free_region`,
    /// then drives the rollback through a redo-flush failure.
    ///
    /// We use a 2-region scenario (one neighbour on each side of the
    /// allocation) so a single forward+backward coalesce pass is
    /// sufficient — matching what `free()` does. A 3-region adjacent
    /// chain would require iterative coalescing and is intentionally
    /// out of scope; the freelist invariant rules it out anyway.
    #[test]
    fn rollback_coalesces_adjacent_free_regions() {
        let data_dev = test_device(16);
        let (redo_dev, redo) = make_failable_redo_log(1024 * 1024);

        let mut alloc = SlotAllocator::new(data_dev).unwrap();

        // Stage a freelist that breaks the "no adjacent" invariant:
        // [A=4K at base] [B=4K at base+4K] [C=4K at base+8K]
        // — all three are contiguous. `__test_force_push_free_region`
        // bypasses the coalesce step so we can fabricate this state.
        // Use a high offset to keep the alloc-from-high-water path
        // away from these fragments.
        let base = DATA_REGION_OFFSET + 8 * 4096;
        alloc.__test_force_push_free_region(base, 4096);
        alloc.__test_force_push_free_region(base + 4096, 4096);
        alloc.__test_force_push_free_region(base + 2 * 4096, 4096);
        assert_eq!(
            alloc.free_region_count(),
            3,
            "test setup: three adjacent fragments staged",
        );

        // Attach the failable redo log AFTER the staging so the
        // staging itself does not journal.
        alloc.set_redo_log(redo);

        // Trigger a best-fit allocate of 4 KiB. With three 4 KiB
        // fragments, `best_fit` picks the first one with waste=0
        // (region B at `base+4096`)... actually it picks the
        // lowest-offset hit (region A at `base`). Either way, after
        // best_fit removes the selected region, the freelist holds
        // exactly two regions and ONE of them is adjacent to the
        // selected slot on each side — the realistic forward-looking
        // shape coalesce_adjacent is designed to handle.
        redo_dev.set_fail(true);
        let result = alloc.allocate(4096);
        redo_dev.set_fail(false);

        match result {
            Err(AllocatorError::RedoLogFailure { .. }) => {}
            other => panic!("expected RedoLogFailure, got {other:?}"),
        }

        // best_fit picked region A (lowest offset, exact fit). After
        // rollback's forward-coalesce, A+B merge into a single
        // 8 KiB region at `base`; C remains separate at `base+8192`.
        // Pre-fix (no rollback coalesce) the freelist would carry
        // three separate adjacent regions; this assertion would see
        // `free_region_count == 3`.
        let stats = alloc.stats();
        assert_eq!(
            stats.free_region_count, 2,
            "rollback must coalesce the two now-adjacent free regions \
             into a single contiguous region — got {} regions",
            stats.free_region_count,
        );
        assert_eq!(
            stats.total_free_bytes, 12288,
            "total free bytes must be conserved by the rollback coalesce",
        );
        assert_eq!(
            stats.largest_free_region, 8192,
            "the merged region must span the two adjacent fragments",
        );

        // The merged region starts at `base` and spans 8 KiB; C is
        // still its own 4 KiB region at `base + 8192`.
        let mut iter = alloc.freelist.iter_offset_order();
        let (off1, sz1) = iter.next().unwrap();
        let (off2, sz2) = iter.next().unwrap();
        assert!(iter.next().is_none());
        assert_eq!(off1, base);
        assert_eq!(sz1, 8192);
        assert_eq!(off2, base + 8192);
        assert_eq!(sz2, 4096);
    }

    /// F-G1-016 negative control: the same scenario without
    /// `coalesce_adjacent` (i.e. pre-fix behaviour) would leave three
    /// separate adjacent regions in the freelist. We verify the
    /// coalesce step is doing real work by directly invoking it on a
    /// staged state and asserting the merge happens.
    #[test]
    fn coalesce_adjacent_merges_neighbours() {
        let data_dev = test_device(16);
        let mut alloc = SlotAllocator::new(data_dev).unwrap();
        let base = DATA_REGION_OFFSET + 8 * 4096;

        // Stage two regions flanking a 4 KiB gap that we'll feed to
        // `coalesce_adjacent` directly.
        alloc.__test_force_push_free_region(base, 4096);
        alloc.__test_force_push_free_region(base + 2 * 4096, 4096);

        let (off, sz) = alloc.coalesce_adjacent(base + 4096, 4096);
        assert_eq!(
            off, base,
            "coalesce must extend backward to the prev region"
        );
        assert_eq!(sz, 12288, "all three contiguous regions must merge");
        assert_eq!(
            alloc.free_region_count(),
            0,
            "coalesce_adjacent must remove both neighbours (caller re-inserts)",
        );
    }

    #[test]
    fn free_rollback_on_redo_flush_failure() {
        // F-G4-002: a redo flush failure poisons the log. Subsequent
        // append/flush calls return RedoError::Poisoned, so we can no
        // longer assert "the offset is NOT reused by a follow-up
        // allocate()" — the next allocate() would fail with the
        // poisoned-log error rather than the freelist's reuse history.
        // Verify the freelist invariant directly instead.
        let data_dev = test_device(16);
        let (redo_dev, redo) = make_failable_redo_log(1024 * 1024);

        let mut alloc = SlotAllocator::new(data_dev).unwrap();
        alloc.set_redo_log(redo);

        let offset = alloc.allocate(4096).unwrap();
        let before_count = alloc.free_region_count();
        let before_free_bytes = alloc.stats().total_free_bytes;
        let before_next = alloc.next_offset();

        redo_dev.set_fail(true);
        let result = alloc.free(offset, 4096);
        redo_dev.set_fail(false);

        match result {
            Err(AllocatorError::RedoLogFailure { .. }) => {}
            other => panic!("expected RedoLogFailure, got {other:?}"),
        }

        // Freelist unchanged.
        assert_eq!(
            alloc.free_region_count(),
            before_count,
            "freelist must be unchanged on free redo flush failure"
        );
        assert_eq!(
            alloc.stats().total_free_bytes,
            before_free_bytes,
            "freelist byte count must be unchanged on free redo flush failure"
        );
        assert_eq!(
            alloc.next_offset(),
            before_next,
            "next_offset must be unchanged — the free never happened"
        );
    }

    #[test]
    fn replay_allocate_then_free_is_equivalent_to_neither() {
        // A rebuilt allocator replays AllocateRegion(a) + FreeRegion(a).
        // The resulting state must match the baseline (no operations).
        let dev = test_device(16);
        let baseline = SlotAllocator::new(dev.clone()).unwrap();

        let mut replayed = SlotAllocator::new(dev).unwrap();
        let offset = DATA_REGION_OFFSET;
        let applied1 = replayed.replay_redo(&RedoOp::AllocateRegion {
            offset,
            size: 8192,
            device_id: 0,
        });
        let applied2 = replayed.replay_redo(&RedoOp::FreeRegion {
            offset,
            size: 8192,
            device_id: 0,
        });
        assert!(applied1, "first allocate replay must mutate state");
        assert!(applied2, "matching free replay must mutate state");

        // After allocate + free, the region is in the freelist and
        // next_offset is past it. That state is NOT identical to
        // baseline, but it must be *functionally equivalent*: a
        // subsequent same-size allocate must return the same offset.
        //
        // To satisfy the task's "equivalent to neither" invariant we
        // check: freelist contains exactly one entry covering the
        // allocated-then-freed region, and next_offset has advanced
        // past it. An allocate of the same size yields the same offset
        // that baseline would.
        let mut replayed_next = replayed;
        let replayed_offset = replayed_next.allocate(8192).unwrap();
        let mut baseline_next = baseline;
        let baseline_offset = baseline_next.allocate(8192).unwrap();
        assert_eq!(
            replayed_offset, baseline_offset,
            "replay of allocate+free must leave allocator state functionally equivalent"
        );
    }

    #[test]
    fn allocator_replay_free_overlap_detection() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        let o1 = alloc.allocate(8192).unwrap();
        let o2 = alloc.allocate(4096).unwrap();
        alloc.free(o1, 8192).unwrap();
        assert_eq!(alloc.free_region_containing(o1), Some((o1, 8192)));
        assert!(
            alloc.free_region_containing(o2).is_none(),
            "second allocation must still be live before replay"
        );

        let applied = alloc.replay_redo(&RedoOp::FreeRegion {
            offset: o1 + 4096,
            size: 8192,
            device_id: 0,
        });
        assert!(
            !applied,
            "partial-overlap replay free must be rejected as corrupt"
        );
        assert_eq!(alloc.free_region_containing(o1), Some((o1, 8192)));
        assert!(
            alloc.free_region_containing(o2).is_none(),
            "partial overlap must not add the live second allocation to freelist"
        );
    }

    #[test]
    fn allocated_range_validation_rejects_free_or_out_of_range_regions() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let off = alloc.allocate(8192).unwrap();

        assert!(alloc.is_allocated_range(off, 4096));
        assert!(alloc.is_allocated_range(off, 8192));
        assert!(!alloc.is_allocated_range(off + 4096, 8192));

        alloc.free(off, 8192).unwrap();
        assert!(!alloc.is_allocated_range(off, 4096));
        assert!(!alloc.is_allocated_range(alloc.next_offset(), 4096));
    }

    #[test]
    fn replay_is_idempotent() {
        // Replaying the same stream twice must yield the same state.
        let dev = test_device(16);

        let ops = vec![
            RedoOp::AllocateRegion {
                offset: DATA_REGION_OFFSET,
                size: 4096,
                device_id: 0,
            },
            RedoOp::AllocateRegion {
                offset: DATA_REGION_OFFSET + 4096,
                size: 8192,
                device_id: 0,
            },
            RedoOp::FreeRegion {
                offset: DATA_REGION_OFFSET,
                size: 4096,
                device_id: 0,
            },
        ];

        let mut once = SlotAllocator::new(dev.clone()).unwrap();
        for op in &ops {
            once.replay_redo(op);
        }

        let mut twice = SlotAllocator::new(dev.clone()).unwrap();
        for _ in 0..2 {
            for op in &ops {
                twice.replay_redo(op);
            }
        }

        assert_eq!(
            once.next_offset(),
            twice.next_offset(),
            "next_offset must match"
        );
        assert_eq!(
            once.free_region_count(),
            twice.free_region_count(),
            "freelist entry count must match"
        );
        let regions_once: Vec<_> = once.freelist.iter_offset_order().collect();
        let regions_twice: Vec<_> = twice.freelist.iter_offset_order().collect();
        assert_eq!(regions_once, regions_twice, "freelist contents must match");
    }

    #[test]
    fn idempotent_allocate_replay_noop_when_already_reflected() {
        // If the allocator was recovered from snapshot and the region is
        // already allocated (below next_offset, not in freelist), replay
        // must be a no-op.
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let offset = DATA_REGION_OFFSET;
        // First replay: fresh allocator, so it bumps next_offset.
        let applied1 = alloc.replay_redo(&RedoOp::AllocateRegion {
            offset,
            size: 4096,
            device_id: 0,
        });
        assert!(applied1, "first replay must mutate state");
        // Second replay: state already reflects it — must be a no-op.
        let applied2 = alloc.replay_redo(&RedoOp::AllocateRegion {
            offset,
            size: 4096,
            device_id: 0,
        });
        assert!(!applied2, "second replay must be a no-op");
    }

    #[test]
    fn allocator_replay_bounds_check() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let before_next = alloc.next_offset();

        let applied_past_end = alloc.replay_redo(&RedoOp::AllocateRegion {
            offset: alloc.device_size - 2048,
            size: 4096,
            device_id: 0,
        });
        assert!(!applied_past_end, "out-of-bounds replay must be ignored");
        assert_eq!(
            alloc.next_offset(),
            before_next,
            "out-of-bounds replay must not advance next_offset"
        );

        let applied_before_data = alloc.replay_redo(&RedoOp::AllocateRegion {
            offset: DATA_REGION_OFFSET - 4096,
            size: 4096,
            device_id: 0,
        });
        assert!(!applied_before_data, "header-region replay must be ignored");
        assert_eq!(
            alloc.next_offset(),
            before_next,
            "header-region replay must not advance next_offset"
        );
    }

    #[test]
    fn idempotent_free_replay_noop_when_already_in_freelist() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        // First allocate + free so the freelist contains the region.
        let offset = alloc.allocate(4096).unwrap();
        alloc.free(offset, 4096).unwrap();
        let before_count = alloc.free_region_count();

        let applied = alloc.replay_redo(&RedoOp::FreeRegion {
            offset,
            size: 4096,
            device_id: 0,
        });
        assert!(
            !applied,
            "replay of a free already in freelist must be a no-op"
        );
        assert_eq!(
            alloc.free_region_count(),
            before_count,
            "freelist must be unchanged by idempotent replay"
        );
    }

    #[test]
    fn redo_journaled_free_survives_crash_and_replay() {
        // Simulate: allocate twice, free one, persist snapshot, then CRASH
        // before the next persist. Recovery: re-open allocator from
        // snapshot + replay redo entries. The freed region must still be
        // available.
        let data_dev = test_device(32);
        let (_redo_dev, redo) = make_redo_log(1024 * 1024);

        let o1;
        let o2;

        {
            let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
            alloc.set_redo_log(redo.clone());

            o1 = alloc.allocate(8192).unwrap();
            o2 = alloc.allocate(4096).unwrap();

            // Persist a snapshot BEFORE the free — snapshot captures
            // only the allocations.
            alloc.persist().unwrap();

            // Free o1 AFTER snapshot — only the redo log captures it.
            alloc.free(o1, 8192).unwrap();
            // Simulated crash here: drop the allocator without another
            // persist() call.
        }

        // Recovery: re-open from snapshot, then replay redo entries that
        // came AFTER the snapshot's high-water mark. For this test we
        // pre-assume the snapshot was taken right before the free, so
        // ALL entries in the redo log happened after the snapshot from
        // the allocator's point of view. In the real server, the server
        // takes a snapshot and truncates the redo log; here we simulate
        // by replaying everything and relying on idempotency.
        let mut recovered = SlotAllocator::recover(data_dev).unwrap();
        // Without replay, the freed region is NOT in the freelist.
        assert_eq!(
            recovered.free_region_count(),
            0,
            "before replay, freelist is empty (matches snapshot)"
        );

        // Replay the redo log — only the Free matters for this test,
        // but AllocateRegion replays are idempotent no-ops because the
        // recovered allocator's next_offset already covers them.
        let entries = redo.lock().recover().unwrap();
        assert!(
            entries
                .iter()
                .any(|e| matches!(e.op, RedoOp::FreeRegion { .. })),
            "redo log must contain a FreeRegion entry"
        );
        for e in &entries {
            recovered.replay_redo(&e.op);
        }

        // After replay, the freelist must contain the freed region.
        assert_eq!(
            recovered.free_region_count(),
            1,
            "after replay, freelist should have the free region"
        );
        let reused = recovered.allocate(8192).unwrap();
        assert_eq!(
            reused, o1,
            "recovered allocator must reuse the freed region"
        );

        // Allocations after o2 must not overlap with o2.
        let next = recovered.allocate(4096).unwrap();
        assert!(
            next >= o2 + 4096 || next + 4096 <= o2,
            "allocation at {next} must not overlap with o2 at {o2}"
        );
    }

    #[test]
    fn no_redo_log_attached_allocate_and_free_still_work() {
        // Without a redo log, allocator falls back to snapshot-only
        // durability. Behaviour must be identical to the pre-journaling
        // implementation.
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        assert!(!alloc.has_redo_log());
        let o1 = alloc.allocate(4096).unwrap();
        alloc.free(o1, 4096).unwrap();
        let o2 = alloc.allocate(4096).unwrap();
        assert_eq!(o1, o2, "freed region must be reused even without redo log");
    }

    // -----------------------------------------------------------------------
    // Header CRC32 tests (H9).
    // -----------------------------------------------------------------------

    #[test]
    fn allocator_header_round_trips_crc() {
        // Persist a non-trivial header (magic + next_offset + freelist + id
        // + version + CRC). Recover must succeed and the CRC bytes 44..48
        // must match a freshly computed CRC over the serialized header
        // with the CRC field zeroed.
        let dev = test_device(16);
        let o1;
        let expected_crc;
        let next_offset_persisted;
        let device_id_persisted;
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            o1 = alloc.allocate(8192).unwrap();
            alloc.allocate(4096).unwrap();
            alloc.free(o1, 8192).unwrap();
            next_offset_persisted = alloc.next_offset();
            device_id_persisted = alloc.device_id();
            alloc.persist().unwrap();

            // Read raw buffer and compute the expected CRC independently.
            let mut raw = crate::device::AlignedBuf::new(4096, 4096);
            dev.pread(&mut raw, 0).unwrap();
            let count = u64::from_le_bytes(raw[16..24].try_into().unwrap()) as usize;
            let covered_end = FREELIST_OFFSET + count * 16;
            let mut crc_input: Vec<u8> = raw[..covered_end].to_vec();
            for b in &mut crc_input[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4] {
                *b = 0;
            }
            expected_crc = {
                let mut h = crc32fast::Hasher::new();
                h.update(&crc_input);
                h.finalize()
            };
            let stored_crc = u32::from_le_bytes(
                raw[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4]
                    .try_into()
                    .unwrap(),
            );
            assert_eq!(
                stored_crc, expected_crc,
                "stored CRC must match recomputed CRC over zeroed-CRC header bytes"
            );
            assert_ne!(
                stored_crc, 0,
                "stamped CRC must be non-zero for a non-empty header"
            );
        }

        // Recover: CRC is valid, so this must succeed and yield the same state.
        let recovered = SlotAllocator::recover(dev).unwrap();
        assert_eq!(
            recovered.next_offset(),
            next_offset_persisted,
            "recovered next_offset must match persisted value"
        );
        assert_eq!(
            recovered.device_id(),
            device_id_persisted,
            "recovered device_id must match persisted value"
        );
        assert_eq!(
            recovered.free_region_count(),
            1,
            "recovered freelist must have one entry (the freed 8K region)"
        );
    }

    #[test]
    fn allocator_header_crc_rejects_single_bit_flip() {
        // Persist a valid header, then flip a single bit somewhere in the
        // covered region (excluding the CRC field itself, since flipping
        // the CRC would also be detected but for a less interesting
        // reason). Recover must return HeaderCorruption with non-equal
        // expected and actual values.
        let dev = test_device(16);
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            alloc.allocate(8192).unwrap();
            alloc.persist().unwrap();
        }

        // Load header, corrupt one bit in next_offset (bytes 8..16) —
        // fully inside the CRC-covered range, outside the CRC slot.
        let mut raw = crate::device::AlignedBuf::new(4096, 4096);
        dev.pread(&mut raw, 0).unwrap();
        let original_byte = raw[8];
        raw[8] ^= 0x01;
        assert_ne!(
            raw[8], original_byte,
            "test setup: bit flip must actually change the byte"
        );
        dev.pwrite(&raw, 0).unwrap();

        // Recompute what the stored CRC claims vs. what the corrupted
        // bytes now hash to, so we can assert the reported values match.
        let stored_crc = u32::from_le_bytes(
            raw[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4]
                .try_into()
                .unwrap(),
        );
        let count = u64::from_le_bytes(raw[16..24].try_into().unwrap()) as usize;
        let covered_end = FREELIST_OFFSET + count * 16;
        let mut crc_input: Vec<u8> = raw[..covered_end].to_vec();
        for b in &mut crc_input[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4] {
            *b = 0;
        }
        let actual_crc = {
            let mut h = crc32fast::Hasher::new();
            h.update(&crc_input);
            h.finalize()
        };
        assert_ne!(
            stored_crc, actual_crc,
            "test setup: corrupted header must have mismatching CRC"
        );

        match SlotAllocator::recover(dev) {
            Err(AllocatorError::HeaderCorruption { expected, actual }) => {
                assert_ne!(
                    expected, actual,
                    "HeaderCorruption must report non-equal expected/actual values"
                );
                assert_eq!(
                    expected, stored_crc,
                    "HeaderCorruption.expected must equal the stored CRC field"
                );
                assert_eq!(
                    actual, actual_crc,
                    "HeaderCorruption.actual must equal the recomputed CRC"
                );
            }
            Err(other) => panic!("expected HeaderCorruption, got: {other}"),
            Ok(_) => panic!("expected HeaderCorruption, but recover succeeded"),
        }
    }

    #[test]
    fn allocator_header_crc_detects_crc_field_tampering() {
        // Flipping a bit in the CRC field itself must also be detected.
        let dev = test_device(16);
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            alloc.allocate(4096).unwrap();
            alloc.persist().unwrap();
        }
        let mut raw = crate::device::AlignedBuf::new(4096, 4096);
        dev.pread(&mut raw, 0).unwrap();
        raw[HEADER_CRC_OFFSET] ^= 0x01;
        dev.pwrite(&raw, 0).unwrap();

        match SlotAllocator::recover(dev) {
            Err(AllocatorError::HeaderCorruption { expected, actual }) => {
                assert_ne!(
                    expected, actual,
                    "CRC-field tampering must produce mismatching expected/actual"
                );
            }
            Err(other) => panic!("expected HeaderCorruption, got: {other}"),
            Ok(_) => panic!("expected HeaderCorruption, but recover succeeded"),
        }
    }

    #[test]
    fn clear_redo_log_disables_journaling() {
        let dev = test_device(16);
        let (_redo_dev, redo) = make_redo_log(1024 * 1024);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_redo_log(redo.clone());
        alloc.allocate(4096).unwrap();
        assert_eq!(redo.lock().recover().unwrap().len(), 1);

        alloc.clear_redo_log();
        alloc.allocate(4096).unwrap();
        // Second allocate is not journaled.
        assert_eq!(redo.lock().recover().unwrap().len(), 1);
    }

    /// Phase 5: allocate 3 regions, then free 2. Observe deltas on the
    /// allocator counters so the test is robust to parallel tests that
    /// also install and tick the metrics.
    #[test]
    fn allocator_ticks_alloc_and_free_counters() {
        use crate::metrics::{AllocatorMetrics, allocator_metrics, init_allocator_metrics};
        use std::sync::OnceLock;

        static TEST_METRICS: OnceLock<AllocatorMetrics> = OnceLock::new();
        let m_ref: &'static AllocatorMetrics = TEST_METRICS.get_or_init(AllocatorMetrics::new);
        init_allocator_metrics(m_ref);
        let metrics = allocator_metrics().expect("metrics installed");

        let before_alloc = metrics.alloc_total.get();
        let before_alloc_bytes = metrics.alloc_bytes_total.get();
        let before_free = metrics.free_total.get();

        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        // Allocate 3 distinct 4 KiB regions.
        let off1 = alloc.allocate(4096).unwrap();
        let off2 = alloc.allocate(4096).unwrap();
        let _off3 = alloc.allocate(4096).unwrap();

        // Also verify that `allocator_stats` reports ≥ 1 freelist region
        // after this allocator's own free calls — this is a fresh local
        // allocator so the invariant holds regardless of what other tests
        // are doing on the shared `OnceLock` gauge.
        alloc.free(off1, 4096).unwrap();
        alloc.free(off2, 4096).unwrap();
        let local_stats = alloc.stats();

        // Other parallel tests may also drive the allocator metrics, so
        // assert ≥ rather than == to remain robust.
        assert!(
            metrics.alloc_total.get() - before_alloc >= 3,
            "alloc_total must advance by ≥ 3, got {}",
            metrics.alloc_total.get() - before_alloc,
        );
        assert!(
            metrics.alloc_bytes_total.get() - before_alloc_bytes >= 3 * 4096,
            "alloc_bytes_total must advance by ≥ 3*4096, got {}",
            metrics.alloc_bytes_total.get() - before_alloc_bytes,
        );
        assert!(
            metrics.free_total.get() - before_free >= 2,
            "free_total must advance by ≥ 2, got {}",
            metrics.free_total.get() - before_free,
        );

        // Invariant specific to this allocator's state: after freeing two
        // regions, the freelist must contain at least one entry. Read it
        // from the local allocator rather than the global gauge, which is
        // raced by parallel tests running their own fresh allocators.
        assert!(
            local_stats.free_region_count >= 1,
            "local freelist should contain ≥ 1 region after freeing (got {})",
            local_stats.free_region_count,
        );
    }

    /// F-G1-009 regression: when the freelist contains more entries than
    /// the on-device header can hold (`MAX_PERSISTED_FREE_REGIONS`),
    /// `persist()` must return `FreelistOverflow` rather than silently
    /// truncating to the first N entries and leaking the tail regions
    /// on the next `recover()`.
    ///
    /// Uses the test-only `__test_force_push_free_region` helper to seed
    /// the freelist past the cap deterministically. The cap is large
    /// (~65k entries) so a natural allocate/free workload to reach it
    /// would take seconds; the helper makes the test instant.
    #[test]
    fn persist_rejects_freelist_overflow() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        // The freelist begins empty; push `MAX_PERSISTED_FREE_REGIONS + 1`
        // entries at non-overlapping offsets above DATA_REGION_OFFSET so
        // the structural invariants stay sane (the helper bypasses
        // coalescing but offsets are still distinct).
        for i in 0..=MAX_PERSISTED_FREE_REGIONS {
            // Each entry occupies a 4096-byte region; offsets must be
            // strictly increasing and not collide.
            let off = DATA_REGION_OFFSET + (i as u64) * 4096;
            alloc.__test_force_push_free_region(off, 4096);
        }
        assert!(alloc.freelist.len() > MAX_PERSISTED_FREE_REGIONS);

        match alloc.persist() {
            Err(AllocatorError::FreelistOverflow { entries, max }) => {
                assert!(entries > max, "entries ({entries}) must exceed max ({max})");
                assert_eq!(max, MAX_PERSISTED_FREE_REGIONS);
            }
            other => panic!("expected FreelistOverflow, got {other:?}"),
        }
    }

    /// P2.3 / F-G1-015: replaying a corrupt redo entry (offset outside the
    /// data region) must bump
    /// `AllocatorMetrics::corrupt_redo_entries_total` so dashboards can
    /// alert on a non-zero recovery-time corruption-rejection rate.
    ///
    /// Drives `replay_redo` with three differently-shaped corrupt
    /// `RedoOp` values — an out-of-range free, an overflowing
    /// offset+size on a free, and an out-of-range allocate — and asserts
    /// the counter advances by ≥ 3. The metric global is shared with
    /// other tests so the delta is bounded by ≥ 3, not == 3.
    #[test]
    fn corrupt_redo_replay_bumps_metric() {
        use crate::metrics::{AllocatorMetrics, allocator_metrics, init_allocator_metrics};
        use std::sync::OnceLock;

        static TEST_METRICS: OnceLock<AllocatorMetrics> = OnceLock::new();
        let m_ref: &'static AllocatorMetrics = TEST_METRICS.get_or_init(AllocatorMetrics::new);
        init_allocator_metrics(m_ref);
        let metrics = allocator_metrics().expect("metrics installed");
        let before = metrics.corrupt_redo_entries_total.get();

        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        // 1) FreeRegion at offset 0 — below `data_region_start`, must be
        //    rejected as corrupt (not as idempotent no-op).
        let op1 = RedoOp::FreeRegion {
            offset: 0,
            size: 4096,
            device_id: 0,
        };
        assert!(!alloc.replay_redo(&op1));

        // 2) FreeRegion with offset+size overflowing u64.
        let op2 = RedoOp::FreeRegion {
            offset: u64::MAX - 1024,
            size: 8192,
            device_id: 0,
        };
        assert!(!alloc.replay_redo(&op2));

        // 3) AllocateRegion past device end — must be rejected as corrupt.
        let op3 = RedoOp::AllocateRegion {
            offset: alloc.device_size + 4096,
            size: 4096,
            device_id: 0,
        };
        assert!(!alloc.replay_redo(&op3));

        let after = metrics.corrupt_redo_entries_total.get();
        assert!(
            after - before >= 3,
            "corrupt_redo_entries_total must advance by ≥ 3, got {}",
            after - before,
        );
    }

    /// P2.3 / F-G1-019: when a record's generation jumps forward by more
    /// than `2^30`, the classifier emits a warn-level log AND bumps
    /// `AllocatorMetrics::generation_wrap_warn_total`. Below the
    /// threshold the counter must NOT advance.
    ///
    /// The classifier lives in `src/record.rs` but the metric lives on
    /// `AllocatorMetrics`, so the test goes here next to the
    /// corrupt-redo coverage where both metrics share an init dance.
    #[test]
    fn generation_wrap_bumps_warn_metric() {
        use crate::metrics::{AllocatorMetrics, allocator_metrics, init_allocator_metrics};
        use crate::record::{GENERATION_WRAP_WARN_DELTA, generation_target_ahead};
        use std::sync::OnceLock;

        static TEST_METRICS: OnceLock<AllocatorMetrics> = OnceLock::new();
        let m_ref: &'static AllocatorMetrics = TEST_METRICS.get_or_init(AllocatorMetrics::new);
        init_allocator_metrics(m_ref);
        let metrics = allocator_metrics().expect("metrics installed");

        // Below threshold: must not bump.
        let before_below = metrics.generation_wrap_warn_total.get();
        assert!(generation_target_ahead(0, GENERATION_WRAP_WARN_DELTA));
        assert!(generation_target_ahead(0, GENERATION_WRAP_WARN_DELTA - 1));
        let after_below = metrics.generation_wrap_warn_total.get();
        assert_eq!(
            after_below, before_below,
            "deltas at or below 2^30 must NOT bump generation_wrap_warn_total",
        );

        // Above threshold: must bump twice (two calls).
        let before_above = metrics.generation_wrap_warn_total.get();
        assert!(generation_target_ahead(0, GENERATION_WRAP_WARN_DELTA + 1));
        assert!(generation_target_ahead(0, GENERATION_WRAP_WARN_DELTA + 2));
        let after_above = metrics.generation_wrap_warn_total.get();
        assert!(
            after_above - before_above >= 2,
            "generation_wrap_warn_total must advance by ≥ 2 when delta > 2^30, got {}",
            after_above - before_above,
        );
    }

    // -----------------------------------------------------------------------
    // Phase 1: packed, block-aware allocation (docs/PACKED_RECORD_STORAGE_DESIGN
    // §3.1). All tests use the 4096-alignment `test_device`.
    // -----------------------------------------------------------------------

    /// Default-off regression: with packing disabled, reservations are still
    /// rounded up to the full 4 KB device block exactly as before. Pins the
    /// pre-packed behavior so the opt-in cannot silently change the default.
    #[test]
    fn packed_default_off_reservations_stay_block_aligned() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        assert!(!alloc.is_packed(), "packing must default to OFF");

        let o1 = alloc.allocate(600).unwrap();
        let o2 = alloc.allocate(600).unwrap();
        let o3 = alloc.allocate(600).unwrap();

        assert_eq!(o1, DATA_REGION_OFFSET);
        // Each ~600 B record consumes a full 4096 B block — 4096 B apart.
        assert_eq!(o2 - o1, 4096, "non-packed records must be one block apart");
        assert_eq!(o3 - o2, 4096, "non-packed records must be one block apart");
    }

    /// Packed mode places multiple small records within ONE 4 KB block: offsets
    /// are RECORD_ALIGN-aligned, share a block, and are < a block apart —
    /// contrasting with the 4096-apart non-packed layout above.
    #[test]
    fn packed_small_records_share_one_block() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_packed(true);
        assert!(alloc.is_packed());

        let block = 4096u64;
        let o1 = alloc.allocate(600).unwrap();
        let o2 = alloc.allocate(600).unwrap();
        let o3 = alloc.allocate(600).unwrap();

        assert_eq!(o1, DATA_REGION_OFFSET, "first packed record at data start");

        // 600 rounds up to 600.div_ceil(8)*8 = 600 (already 8-aligned) -> 608?
        // 600 % 8 == 0, so aligned size is 600. Spacing is the aligned size.
        for (i, &o) in [o1, o2, o3].iter().enumerate() {
            assert_eq!(
                o % RECORD_ALIGN,
                0,
                "record {i} must be RECORD_ALIGN-aligned"
            );
            assert_eq!(
                o / block,
                o1 / block,
                "record {i} must share the first block with o1"
            );
        }
        assert_eq!(o2 - o1, 600, "packed spacing == RECORD_ALIGN-aligned size");
        assert_eq!(o3 - o2, 600, "packed spacing == RECORD_ALIGN-aligned size");
        assert!(o3 - o1 < block, "all three must fit within one block");
    }

    #[test]
    fn packed_free_does_not_overfree_block_neighbours() {
        // The corruption GATE (PACKED_RECORD_STORAGE_DESIGN.md §3): freeing one
        // packed record must return EXACTLY its byte range, not a 4 KiB-rounded
        // span that would also free the records sharing its block.
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_packed(true);

        let o1 = alloc.allocate(600).unwrap();
        let o2 = alloc.allocate(600).unwrap();
        let o3 = alloc.allocate(600).unwrap();
        let block = 4096u64;
        assert_eq!(o1 / block, o3 / block, "all three packed into one block");
        assert!(alloc.is_allocated_range(o1, 600));
        assert!(alloc.is_allocated_range(o3, 600));

        // Free the MIDDLE record with its exact size.
        alloc.free(o2, 600).unwrap();

        // Neighbours must remain allocated — a 4 KiB-rounded free would have
        // swept o3 (forward, within o2+4096) into the freelist.
        assert!(
            alloc.is_allocated_range(o1, 600),
            "o1 must not be over-freed by free(o2)"
        );
        assert!(
            alloc.is_allocated_range(o3, 600),
            "o3 must not be over-freed by free(o2)"
        );

        // The freed hole is exactly o2's range and is reused as-is.
        let reused = alloc.allocate(600).unwrap();
        assert_eq!(
            reused, o2,
            "freed packed hole reused exactly; neighbours intact"
        );
    }

    /// Packed reservation size rounds non-8-multiple sizes up to RECORD_ALIGN.
    #[test]
    fn packed_reservation_rounds_up_to_record_align() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_packed(true);

        // 393 -> rounds up to 400 (393.div_ceil(8)*8 = 400).
        let o1 = alloc.allocate(393).unwrap();
        let o2 = alloc.allocate(393).unwrap();
        assert_eq!(
            o2 - o1,
            400,
            "393 B must reserve 400 B (RECORD_ALIGN multiple)"
        );
        assert_eq!(o1 % RECORD_ALIGN, 0);
        assert_eq!(o2 % RECORD_ALIGN, 0);
    }

    /// No small record straddles a block boundary: reserve until the next
    /// record would cross, and assert it bumps to the next block start (leaving
    /// the block tail as waste).
    #[test]
    fn packed_no_small_record_straddles_block() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_packed(true);
        let block = 4096u64;

        // 1024 B records: 4 fit in a block exactly (4*1024 == 4096). The 5th
        // would start at block+0 already since 4*1024 fills the block.
        // Use 1000 B (rounds to 1000, 8-aligned) so 4 fit (4000) and the 5th
        // would straddle (4000 + 1000 = 5000 > 4096) -> bump to next block.
        let mut offsets = Vec::new();
        for _ in 0..5 {
            offsets.push(alloc.allocate(1000).unwrap());
        }
        // First four within block 0.
        let first_block = DATA_REGION_OFFSET / block;
        for (i, &o) in offsets.iter().take(4).enumerate() {
            assert_eq!(o / block, first_block, "record {i} in block 0");
            assert!(
                o % block + 1000 <= block,
                "record {i} at {o} must not straddle a block boundary"
            );
        }
        // Fifth bumped to the start of the next block (no straddle).
        let fifth = offsets[4];
        assert_eq!(
            fifth % block,
            0,
            "the straddling record must be bumped to a block boundary"
        );
        assert_eq!(
            fifth,
            DATA_REGION_OFFSET + block,
            "fifth record must start at the next block"
        );
    }

    /// A large record (> one block) gets a block-aligned offset and a
    /// block-multiple reservation size, and stays block-granular even in packed
    /// mode.
    #[test]
    fn packed_large_record_is_block_aligned() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_packed(true);
        let block = 4096u64;

        // Place a small record first so the high-water is mid-block.
        let small = alloc.allocate(600).unwrap();
        assert_eq!(small, DATA_REGION_OFFSET);

        // A large record (5000 B > block) must be bumped to a block boundary
        // and reserve a block multiple (8192).
        let large = alloc.allocate(5000).unwrap();
        assert_eq!(large % block, 0, "large record must start block-aligned");
        assert_eq!(
            large,
            DATA_REGION_OFFSET + block,
            "large record bumped to next block after the small one"
        );

        // The next allocation must start a block-multiple past the large one
        // (5000 -> align_up -> 8192).
        let after = alloc.allocate(600).unwrap();
        assert_eq!(
            after,
            large + 8192,
            "large reservation must be a block multiple (8192)"
        );
    }

    /// Freelist reuse in packed mode returns a within-block hole and hands out a
    /// within-block, RECORD_ALIGN-aligned allocation from it.
    ///
    /// Holes are seeded as precise byte ranges via the test helper because
    /// `free()` still block-rounds in this phase (the free path is not yet
    /// packing-aware — out of scope for §3.1); best_fit's block-awareness is
    /// what we exercise here.
    #[test]
    fn packed_freelist_reuse_respects_block_boundary() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_packed(true);
        let block = 4096u64;
        let d = DATA_REGION_OFFSET;

        // Advance the high-water past block 0 so reuse (not bump) is exercised.
        let _hw = alloc.allocate(8000).unwrap(); // large -> blocks 0..1 owned

        // Seed a 1000 B hole wholly within block 2: [D+2*block, D+2*block+1000).
        let hole = d + 2 * block;
        alloc.__test_force_push_free_region(hole, 1000);
        assert_eq!(alloc.free_region_count(), 1);

        // A 1000 B packed alloc must reuse that within-block hole exactly.
        let reused = alloc.allocate(1000).unwrap();
        assert_eq!(reused, hole, "packed reuse must take the within-block hole");
        assert!(
            reused % block + 1000 <= block,
            "reuse must stay within one block"
        );
        assert_eq!(alloc.free_region_count(), 0, "exact-fit hole consumed");
    }

    /// A freelist hole that would force a straddling allocation is skipped:
    /// best_fit must reject a hole whose head allocation crosses a block
    /// boundary and fall through to the high-water mark instead, leaving the
    /// hole untouched.
    #[test]
    fn packed_skips_straddling_freelist_hole() {
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_packed(true);
        let block = 4096u64;
        let d = DATA_REGION_OFFSET;

        // Seed a 2000 B hole that begins late in block 0:
        // [D+3000, D+5000) — it spans the block-0/block-1 boundary at D+4096.
        let hole = d + 3000;
        alloc.__test_force_push_free_region(hole, 2000);
        assert_eq!(alloc.free_region_count(), 1);

        // A 1500 B request (aligned 1504). At the hole start D+3000:
        // 3000 % 4096 + 1504 = 4504 > 4096 -> would straddle. best_fit must SKIP
        // the hole and fall through to the high-water mark.
        let placed = alloc.allocate(1500).unwrap();
        assert!(
            placed % block + 1504 <= block,
            "1500 B packed alloc at {placed} must not straddle a block"
        );
        assert_ne!(
            placed, hole,
            "the straddling hole must be skipped, not reused"
        );
        // The 2000 B hole must still be in the freelist (untouched).
        assert_eq!(
            alloc.free_region_containing(hole),
            Some((hole, 2000)),
            "skipped hole must remain in the freelist"
        );
    }

    /// Rolling back a bumped high-water reservation restores `next_offset`
    /// fully, reclaiming BOTH the record bytes AND the skipped block tail.
    #[test]
    fn packed_rollback_of_bumped_reservation_reclaims_tail() {
        // Drive the rollback via reserve_batch/rollback_pending (no redo log
        // needed) so we exercise the exact rollback_reservation path.
        let dev = test_device(16);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_packed(true);
        let d = DATA_REGION_OFFSET;
        let block = 4096u64;

        // Fill block 0 to 4000 B (4 x 1000 B); high-water = D+4000, mid-block.
        for _ in 0..4 {
            alloc.allocate(1000).unwrap();
        }
        let hw_before = alloc.stats().next_offset;
        assert_eq!(hw_before, d + 4000);

        // A 1000 B packed alloc here straddles -> bumps to D+4096, advancing
        // next_offset to D+4096+1000. Use reserve_batch so we can roll back.
        let pending = alloc.reserve_batch(&[1000]).unwrap();
        let region = pending.regions[0].expect("reservation must succeed");
        assert_eq!(region.offset, d + block, "must bump to next block start");
        assert_eq!(alloc.stats().next_offset, d + block + 1000);

        // Roll back: next_offset must return to the ORIGINAL pre-bump value,
        // reclaiming the 96 B tail [D+4000, D+4096) plus the 1000 B record.
        alloc.rollback_pending(pending);
        assert_eq!(
            alloc.stats().next_offset,
            hw_before,
            "rollback must restore next_offset to before the bump (reclaim record + tail)"
        );
        assert_eq!(
            alloc.free_region_count(),
            0,
            "high-water rollback must not leave a freelist hole"
        );
    }

    /// Replay parity: drive packed allocations through a redo-logged allocator,
    /// capture the `AllocateRegion` redo ops, replay them into a FRESH packed
    /// allocator, and assert next_offset + freelist match the live allocator
    /// exactly (no double-allocation, identical packed layout).
    #[test]
    fn packed_replay_parity_matches_live_layout() {
        let dev = test_device(16);
        let (_redo_dev, redo) = make_redo_log(1024 * 1024);

        // Live packed allocator with a redo log attached.
        let mut live = SlotAllocator::new(dev.clone()).unwrap();
        live.set_packed(true);
        live.set_redo_log(redo.clone());

        // A mix of small (packed, sharing blocks + a bump) and one large
        // (block-aligned) reservation.
        let sizes = [600u64, 600, 1000, 1000, 1000, 1000, 5000, 393];
        for &s in &sizes {
            live.allocate(s).unwrap();
        }

        // Capture the AllocateRegion redo ops the live path journaled.
        let entries = redo.lock().read_from_sequence(1).unwrap();
        let alloc_ops: Vec<RedoOp> = entries
            .into_iter()
            .map(|e| e.op)
            .filter(|op| matches!(op, RedoOp::AllocateRegion { .. }))
            .collect();
        assert_eq!(
            alloc_ops.len(),
            sizes.len(),
            "one AllocateRegion op per allocate"
        );

        // Replay into a FRESH packed allocator over a fresh device of the same
        // geometry (no redo log on the replay target).
        let replay_dev = test_device(16);
        let mut replayed = SlotAllocator::new(replay_dev).unwrap();
        replayed.set_packed(true);
        for op in &alloc_ops {
            replayed.replay_redo(op);
        }

        // next_offset must match exactly.
        assert_eq!(
            replayed.stats().next_offset,
            live.stats().next_offset,
            "replayed high-water must match the live packed layout"
        );

        // Freelist must match exactly (offset-ordered).
        let live_free: Vec<(u64, u64)> = live.freelist.iter_offset_order().collect();
        let replayed_free: Vec<(u64, u64)> = replayed.freelist.iter_offset_order().collect();
        assert_eq!(
            replayed_free, live_free,
            "replayed freelist must match the live packed freelist"
        );

        // Sanity: the large record (5000 B) is block-aligned in the live layout.
        // Its offset is the 7th AllocateRegion op.
        if let RedoOp::AllocateRegion { offset, size, .. } = alloc_ops[6] {
            assert_eq!(offset % 4096, 0, "large record must be block-aligned");
            assert_eq!(size, 8192, "5000 B large record reserves 8192 B");
        } else {
            panic!("expected AllocateRegion op for the large record");
        }
    }

    /// Block-aware best_fit on the Large/BTree freelist variant: seed enough
    /// holes to promote past PROMOTE_THRESHOLD, then verify a straddling hole is
    /// skipped and a within-block hole is reused — covering the BTree code path
    /// (the small-freelist tests cover the Vec variant).
    #[test]
    fn packed_btree_best_fit_is_block_aware() {
        let dev = test_device(64);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        alloc.set_packed(true);
        let block = 4096u64;
        let d = DATA_REGION_OFFSET;

        // Seed PROMOTE_THRESHOLD + 2 non-adjacent 64 B holes, one per block,
        // forcing the freelist to promote to the BTree backend.
        let n = PROMOTE_THRESHOLD + 2;
        for i in 0..n as u64 {
            // 64 B hole at the START of block (i+10): wholly within that block.
            alloc.__test_force_push_free_region(d + (i + 10) * block, 64);
        }
        assert!(
            alloc.free_region_count() > PROMOTE_THRESHOLD,
            "freelist must be on the BTree backend"
        );

        // Add ONE straddling hole big enough to be the best-fit by size for a
        // 100 B request, positioned to cross a block boundary: [block20+4050, +200).
        let straddle = d + 20 * block + 4050;
        alloc.__test_force_push_free_region(straddle, 200);

        // A 100 B request (aligned 104). The 200 B straddling hole is the
        // largest candidate but crosses the boundary (4050 % 4096 + 104 = 4154
        // > 4096) -> must be skipped. A 64 B hole is too small. So it falls to
        // the high-water mark (block-aligned, within one block).
        let placed = alloc.allocate(100).unwrap();
        assert!(
            placed % block + 104 <= block,
            "BTree packed alloc at {placed} must not straddle a block"
        );
        assert_ne!(placed, straddle, "straddling BTree hole must be skipped");
        assert_eq!(
            alloc.free_region_containing(straddle),
            Some((straddle, 200)),
            "skipped straddling hole must remain in the BTree freelist"
        );

        // A 64 B request (aligned 64) MUST reuse one of the within-block holes
        // (exact fit), not the high-water mark. best_fit picks the smallest
        // sufficient qualifying region.
        let small = alloc.allocate(64).unwrap();
        assert!(
            small % block + 64 <= block,
            "reused within-block hole must not straddle"
        );
        assert_eq!(
            alloc.free_region_containing(small),
            None,
            "the reused hole must have been removed from the freelist"
        );
        assert_eq!(
            small % RECORD_ALIGN,
            0,
            "reused offset must be RECORD_ALIGN-aligned"
        );
    }

    // -----------------------------------------------------------------------
    // Phase 4: on-disk packed format marker (HEADER_VERSION_PACKED).
    // The device's persisted packed-ness must win over config across restarts;
    // opening a packed device non-packed would corrupt via `free()`'s 4 KiB
    // align_up (PACKED_RECORD_STORAGE_DESIGN.md §1/§5).
    // -----------------------------------------------------------------------

    /// A packed allocator persists header version 2 and recovers as packed.
    #[test]
    fn packed_allocator_persists_version_2_and_recovers_packed() {
        let dev = test_device(16);
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            alloc.set_packed(true);
            alloc.allocate(600).unwrap();
            alloc.persist().unwrap();
        }

        // Raw header: version field at 40..44 must be HEADER_VERSION_PACKED.
        let mut buf = crate::device::AlignedBuf::new(4096, 4096);
        dev.pread(&mut buf, 0).unwrap();
        let version = u32::from_le_bytes(buf[40..44].try_into().unwrap());
        assert_eq!(
            version, HEADER_VERSION_PACKED,
            "a packed allocator must stamp header version {HEADER_VERSION_PACKED}"
        );

        let recovered = SlotAllocator::recover(dev).unwrap();
        assert!(
            recovered.is_packed(),
            "recover of a v2 header must restore packed mode from the device"
        );
    }

    /// A non-packed allocator persists header version 1 and recovers non-packed.
    #[test]
    fn nonpacked_allocator_persists_version_1_and_recovers_nonpacked() {
        let dev = test_device(16);
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            assert!(!alloc.is_packed());
            alloc.allocate(600).unwrap();
            alloc.persist().unwrap();
        }

        let mut buf = crate::device::AlignedBuf::new(4096, 4096);
        dev.pread(&mut buf, 0).unwrap();
        let version = u32::from_le_bytes(buf[40..44].try_into().unwrap());
        assert_eq!(
            version, HEADER_VERSION,
            "a non-packed allocator must stamp header version {HEADER_VERSION} (unchanged)"
        );

        let recovered = SlotAllocator::recover(dev).unwrap();
        assert!(
            !recovered.is_packed(),
            "recover of a v1 header must restore non-packed mode"
        );
    }

    /// The DEVICE wins: a v2 (packed) device recovers as packed regardless of
    /// how the allocator was constructed — there is no config override on the
    /// recover path. (`recover` cannot be told to be non-packed.)
    #[test]
    fn recovered_v2_device_is_always_packed() {
        let dev = test_device(16);
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            alloc.set_packed(true);
            alloc.allocate(600).unwrap();
            alloc.allocate(600).unwrap();
            alloc.persist().unwrap();
        }
        let recovered = SlotAllocator::recover(dev).unwrap();
        assert!(
            recovered.is_packed(),
            "a v2 device must always recover packed (device-format-wins)"
        );
    }

    /// Old-binary fail-closed: a v1-only build (one that does not understand
    /// version 2) must reject a v2/packed header rather than misread it
    /// non-packed and corrupt it via `free()`. Simulated by checking the
    /// version gate: bumping HEADER_VERSION to anything below the persisted
    /// packed marker would make `recover` return `UnsupportedVersion`.
    #[test]
    fn v1_build_rejects_v2_packed_header() {
        let dev = test_device(16);
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            alloc.set_packed(true);
            alloc.persist().unwrap();
        }

        // Confirm the persisted version really is the packed marker (2).
        let mut buf = crate::device::AlignedBuf::new(4096, 4096);
        dev.pread(&mut buf, 0).unwrap();
        let on_disk = u32::from_le_bytes(buf[40..44].try_into().unwrap());
        assert_eq!(on_disk, HEADER_VERSION_PACKED);

        // A v1-only build's max-known version is HEADER_VERSION (1). The recover
        // gate rejects any on-disk version it does not know. Assert that the
        // packed marker is strictly greater than the v1 max, so such a build
        // hits the `version > max_known` branch and fails CLOSED.
        assert!(
            HEADER_VERSION_PACKED > HEADER_VERSION,
            "packed marker must exceed the v1 max-known version so an old binary fails closed"
        );

        // Drive the actual rejection: rewrite the version to a value ABOVE the
        // CURRENT build's max-known (HEADER_VERSION_PACKED) so `recover` takes
        // the same `UnsupportedVersion` branch a v1 build takes on a v2 header.
        // (CRC must be recomputed so we exercise the version gate, not the CRC
        // gate.) This proves the gate is the fail-closed path.
        let future = HEADER_VERSION_PACKED + 1;
        buf[40..44].copy_from_slice(&future.to_le_bytes());
        // Recompute CRC over the covered range with the CRC field zeroed.
        let count = u64::from_le_bytes(buf[16..24].try_into().unwrap()) as usize;
        let covered_end = FREELIST_OFFSET + count * 16;
        for b in &mut buf[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4] {
            *b = 0;
        }
        let crc = {
            let mut h = crc32fast::Hasher::new();
            h.update(&buf[..covered_end]);
            h.finalize()
        };
        buf[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
        dev.pwrite(&buf, 0).unwrap();

        match SlotAllocator::recover(dev) {
            Err(AllocatorError::UnsupportedVersion(v)) => assert_eq!(v, future),
            Err(other) => panic!("expected UnsupportedVersion({future}), got: {other}"),
            Ok(_) => panic!("expected UnsupportedVersion, but recover succeeded"),
        }
    }

    /// End-to-end packing corruption gate across a persist/recover cycle:
    /// several packed records share one block; free the middle; recover; the
    /// surviving neighbours must read back intact and must not have been
    /// over-freed (the freed hole is reused exactly, not a 4 KiB span).
    #[test]
    fn packed_persist_recover_preserves_block_neighbours() {
        let dev = test_device(16);
        let (o1, o2, o3);
        {
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
            alloc.set_packed(true);
            o1 = alloc.allocate(600).unwrap();
            o2 = alloc.allocate(600).unwrap();
            o3 = alloc.allocate(600).unwrap();
            let block = 4096u64;
            assert_eq!(o1 / block, o3 / block, "all three packed in one block");
            // Free the middle with its exact size, then snapshot.
            alloc.free(o2, 600).unwrap();
            alloc.persist().unwrap();
        }

        // Recover: packed mode restored from the header, neighbours still live,
        // the middle hole still free and reused exactly.
        let mut recovered = SlotAllocator::recover(dev).unwrap();
        assert!(recovered.is_packed(), "must recover packed");
        assert!(
            recovered.is_allocated_range(o1, 600),
            "o1 must survive persist/recover (not over-freed)"
        );
        assert!(
            recovered.is_allocated_range(o3, 600),
            "o3 must survive persist/recover (not over-freed)"
        );
        let reused = recovered.allocate(600).unwrap();
        assert_eq!(
            reused, o2,
            "the freed packed hole is reused exactly after recovery"
        );
    }
}
