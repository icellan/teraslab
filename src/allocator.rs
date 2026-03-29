//! Device space allocator for TeraSlab records.
//!
//! Manages free space on a block device using a freelist. Allocations are
//! aligned to the device's minimum I/O size. Freed regions are merged with
//! adjacent free regions to reduce fragmentation.

use crate::device::{AlignedBuf, BlockDevice, DeviceError};
use std::sync::Arc;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Start of the data region on device. The region before this is reserved for
/// the device header (freelist checkpoint, device metadata).
pub const DATA_REGION_OFFSET: u64 = 1024 * 1024; // 1 MiB reserved for header

/// Magic number for the allocator header on device.
const ALLOCATOR_MAGIC: u64 = 0x5445_5241_414C_4C43; // "TERAALLC"

/// Current header version. Stored at bytes 40..44 so `recover()` can reject
/// incompatible on-disk formats written by future builds.
const HEADER_VERSION: u32 = 1;

/// Byte offset where freelist entries begin in the header.
const FREELIST_OFFSET: usize = 48;

/// Maximum number of free regions that can be serialized to the device header.
const MAX_PERSISTED_FREE_REGIONS: usize =
    (DATA_REGION_OFFSET as usize - FREELIST_OFFSET) / 16;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by the allocator.
#[derive(Error, Debug)]
pub enum AllocatorError {
    /// The device has no free space large enough for the requested allocation.
    #[error("device full: requested {requested} bytes, largest free region is {largest_free} bytes")]
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

    /// Failed to generate random bytes for device identity.
    #[error("failed to generate device identity: {0}")]
    Getrandom(getrandom::Error),

    /// The on-disk header version is not supported by this build.
    #[error("unsupported header version: {0}")]
    UnsupportedVersion(u32),
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
    fn best_fit(&mut self, aligned_size: u64) -> Option<(u64, u64)> {
        match self {
            Self::Small(v) => {
                let mut best_idx: Option<usize> = None;
                let mut best_waste: u64 = u64::MAX;
                for (i, region) in v.iter().enumerate() {
                    if region.size >= aligned_size {
                        let waste = region.size - aligned_size;
                        if waste < best_waste {
                            best_waste = waste;
                            best_idx = Some(i);
                            if waste == 0 { break; }
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
                let &(region_size, region_offset) =
                    by_size.range((aligned_size, 0)..).next()?;
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
        }
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
    }

    /// Remove a free region by offset. Returns `Some(size)` if found.
    fn remove(&mut self, offset: u64) -> Option<u64> {
        match self {
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
        }
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
        })
    }

    /// Allocate a contiguous region of at least `size` bytes.
    ///
    /// The returned offset is aligned to the device's minimum I/O size.
    /// Returns [`AllocatorError::DeviceFull`] if no space is available.
    pub fn allocate(&mut self, size: u64) -> Result<u64> {
        let aligned_size = self.align_up(size);

        if let Some((region_offset, _)) = self.freelist.best_fit(aligned_size) {
            // best_fit already removed/split the region in the freelist.
            self.freelist.maybe_demote();
            return Ok(region_offset);
        }

        // No suitable free region — extend at the append point.
        let offset = self.next_offset;
        if offset + aligned_size > self.device_size {
            return Err(AllocatorError::DeviceFull {
                requested: aligned_size,
                largest_free: self.freelist.largest(),
            });
        }
        self.next_offset = offset + aligned_size;
        Ok(offset)
    }

    /// Return a region to the freelist.
    ///
    /// Adjacent free regions are automatically merged.
    pub fn free(&mut self, offset: u64, size: u64) -> Result<()> {
        let aligned_size = self.align_up(size);

        if offset < self.data_region_start
            || offset + aligned_size > self.device_size
        {
            return Err(AllocatorError::InvalidFree {
                offset,
                size: aligned_size,
            });
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

        Ok(())
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
    /// 44      4     padding (reserved, zeros)
    /// 48+     16*N  free region entries (offset:8, size:8)
    /// ```
    pub fn persist(&self) -> Result<()> {
        let count = self.freelist.len().min(MAX_PERSISTED_FREE_REGIONS);
        let aligned_len = self.align_up(FREELIST_OFFSET as u64 + (count as u64) * 16);
        let mut buf = AlignedBuf::new(aligned_len as usize, self.alignment);

        buf[0..8].copy_from_slice(&ALLOCATOR_MAGIC.to_le_bytes());
        buf[8..16].copy_from_slice(&self.next_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&(count as u64).to_le_bytes());
        buf[24..40].copy_from_slice(&self.device_id);
        buf[40..44].copy_from_slice(&HEADER_VERSION.to_le_bytes());

        for (i, (offset, size)) in self.freelist.iter_offset_order().take(count).enumerate() {
            let base = FREELIST_OFFSET + i * 16;
            buf[base..base + 8].copy_from_slice(&offset.to_le_bytes());
            buf[base + 8..base + 16].copy_from_slice(&size.to_le_bytes());
        }

        self.device.pwrite(&buf, 0)?;
        Ok(())
    }

    /// Recover allocator state from the device header.
    pub fn recover(device: Arc<dyn BlockDevice>) -> Result<Self> {
        let alignment = device.alignment();
        let device_size = device.size();

        let header_size = alignment.max(FREELIST_OFFSET);
        let mut header_buf = AlignedBuf::new(header_size, alignment);
        device.pread(&mut header_buf, 0)?;

        let magic = u64::from_le_bytes(header_buf[0..8].try_into().unwrap());
        if magic != ALLOCATOR_MAGIC {
            return Err(AllocatorError::CorruptedHeader);
        }

        let next_offset = u64::from_le_bytes(header_buf[8..16].try_into().unwrap());
        let count = u64::from_le_bytes(header_buf[16..24].try_into().unwrap()) as usize;

        let mut device_id = [0u8; 16];
        device_id.copy_from_slice(&header_buf[24..40]);

        let version = u32::from_le_bytes(header_buf[40..44].try_into().unwrap());
        if version > HEADER_VERSION {
            return Err(AllocatorError::UnsupportedVersion(version));
        }

        // Read the full freelist.
        let total_size = FREELIST_OFFSET + count * 16;
        let aligned_total = total_size.div_ceil(alignment) * alignment;
        let mut buf = AlignedBuf::new(aligned_total, alignment);
        device.pread(&mut buf, 0)?;

        let mut freelist = FreelistBackend::new();
        for i in 0..count {
            let base = FREELIST_OFFSET + i * 16;
            let offset = u64::from_le_bytes(buf[base..base + 8].try_into().unwrap());
            let size = u64::from_le_bytes(buf[base + 8..base + 16].try_into().unwrap());
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
        })
    }

    /// Round `size` up to the device alignment boundary.
    fn align_up(&self, size: u64) -> u64 {
        let a = self.alignment as u64;
        size.div_ceil(a) * a
    }

    /// The number of free regions in the freelist (for testing).
    #[cfg(test)]
    fn free_region_count(&self) -> usize {
        self.freelist.len()
    }

    /// Start of the data region on device.
    pub fn data_region_start(&self) -> u64 { self.data_region_start }

    /// Current high-water mark — all allocations are below this offset.
    pub fn next_offset(&self) -> u64 { self.next_offset }

    /// Device alignment in bytes.
    pub fn device_alignment(&self) -> usize { self.alignment }

    /// The 128-bit device identity stored in the superblock.
    pub fn device_id(&self) -> [u8; 16] { self.device_id }

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
            if size > largest { largest = size; }
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
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemoryDevice;

    fn test_device(size_mb: u64) -> Arc<MemoryDevice> {
        Arc::new(
            MemoryDevice::new(size_mb * 1024 * 1024, 4096).unwrap(),
        )
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
            hex.chars().all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()),
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
        assert_eq!(version, HEADER_VERSION, "persisted header version must be {HEADER_VERSION}");
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
        assert_eq!(reused, o1, "recovered freelist should offer the freed region first");

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
        let o1 = alloc.allocate(4096).unwrap();  // 4K
        let _o2 = alloc.allocate(4096).unwrap(); // 4K (spacer, kept)
        let o3 = alloc.allocate(8192).unwrap();  // 8K
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
        if let FreelistBackend::Large { ref by_offset, ref by_size } = alloc.freelist {
            assert_eq!(by_offset.len(), by_size.len(),
                "dual indexes must stay in sync");
            for (&off, &sz) in by_offset {
                assert!(by_size.contains(&(sz, off)),
                    "by_size missing entry ({sz}, {off})");
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
        assert!(matches!(alloc.freelist, FreelistBackend::Large { .. }),
            "should promote to Large after 100 free regions");

        // Allocate back most of the free regions to shrink below demote threshold.
        for _ in 0..75 {
            alloc.allocate(4096).unwrap();
        }
        // Should have demoted back to Small (25 entries < 32 threshold).
        assert!(matches!(alloc.freelist, FreelistBackend::Small(_)),
            "should demote to Small after allocating most free regions back");

        // The remaining entries should still be correct.
        let remaining = alloc.free_region_count();
        assert!(remaining > 0 && remaining < DEMOTE_THRESHOLD);
    }
}
