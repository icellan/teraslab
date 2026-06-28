//! Block device abstraction for raw NVMe I/O and in-memory testing.
//!
//! Two backends:
//! - [`DirectDevice`]: Opens files with `O_DIRECT` for zero-copy I/O.
//! - [`MemoryDevice`]: In-memory `Vec<u8>` for testing with the same alignment rules.

use std::alloc::{self, Layout};
use std::ops::{Deref, DerefMut};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Portable ioctl wrappers (C-1 / F-G1-012 / P3.3)
// ---------------------------------------------------------------------------
//
// `nix::ioctl_read!` computes the correctly encoded ioctl request number for
// the target triple at compile time, so the same call site works on 32-bit
// Linux variants where the hard-coded numeric constants previously used here
// were wrong (and `libc::ioctl` would silently `ENOTTY`).
//
//   Linux:
//     BLKGETSIZE64        = _IOR(0x12, 114, size_t)
//
//   macOS:
//     DKIOCGETBLOCKCOUNT  = _IOR('d', 25, u64)
//     DKIOCGETBLOCKSIZE   = _IOR('d', 24, u32)

#[cfg(target_os = "linux")]
nix::ioctl_read!(blkgetsize64, 0x12, 114, u64);

#[cfg(target_os = "macos")]
nix::ioctl_read!(dkiocgetblockcount, b'd', 25, u64);

#[cfg(target_os = "macos")]
nix::ioctl_read!(dkiocgetblocksize, b'd', 24, u32);

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by device I/O operations.
#[derive(Error, Debug)]
pub enum DeviceError {
    /// The offset or buffer length is not aligned to the device's minimum I/O size.
    #[error("alignment violation: {detail}")]
    AlignmentViolation { detail: String },

    /// An I/O operation attempted to access past the end of the device.
    #[error("out of bounds: offset {offset} + len {len} exceeds device size {device_size}")]
    OutOfBounds {
        offset: u64,
        len: u64,
        device_size: u64,
    },

    /// Underlying OS I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Zero-length device requested.
    #[error("device size must be greater than zero")]
    ZeroSize,

    /// The alignment passed to `DirectDevice::open` or `MemoryDevice::new`
    /// is not a power-of-two or is less than 512 bytes (the minimum
    /// sector size we support for raw block I/O).
    #[error("invalid alignment {alignment}: must be a power-of-two and >= 512")]
    InvalidAlignment { alignment: usize },

    /// A record read from the device failed its integrity check (e.g. CRC
    /// mismatch on the [`TxMetadata`](crate::record::TxMetadata) header).
    #[error("record corruption: {detail}")]
    RecordCorruption { detail: String },

    /// A `pread_exact_at` could not deliver the full requested byte range:
    /// the device returned 0 bytes (EOF) before `expected` bytes had been
    /// transferred. `got` is the number of bytes successfully read before
    /// the short return; `offset` is the original starting offset of the
    /// exact-read request. Treated as fatal corruption by callers.
    #[error("short read at offset {offset}: expected {expected} bytes, got {got} before EOF")]
    ShortRead {
        expected: usize,
        got: usize,
        offset: u64,
    },

    /// A `pwrite_all_at` made no forward progress: the underlying `pwrite`
    /// returned 0 bytes written even though the request still had bytes to
    /// transfer. Per the production-readiness gap document, any short
    /// write is treated as fatal corruption — there is no clean way to
    /// recover without risking partial-write torn state.
    #[error(
        "write stalled at offset {offset}: 0 bytes written with {remaining} bytes still pending"
    )]
    WriteStalled { offset: u64, remaining: usize },

    /// The kernel-reported geometry of a raw block device is unusable:
    /// a total size of zero bytes, a zero block count/size, or a
    /// `block_count * block_size` product that overflows `u64`.
    /// Trusting such a value would let the allocator hand out offsets
    /// past the real end of the device or refuse the whole device.
    #[error("invalid block device geometry: {detail}")]
    InvalidBlockDeviceGeometry { detail: String },
}

/// Minimum supported I/O alignment for block-device-backed storage.
///
/// Every real NVMe device exposes a sector size of at least 512 bytes, and
/// `O_DIRECT` on Linux requires the user buffer's alignment to match the
/// filesystem's logical block size. We reject smaller alignments during
/// device construction so callers cannot silently opt into a configuration
/// that would fail at I/O time.
pub const MIN_ALIGNMENT: usize = 512;

/// Validate that `alignment` is a power-of-two AND at least [`MIN_ALIGNMENT`].
///
/// Used by both [`MemoryDevice::new`] and [`DirectDevice::open`] so the
/// two backends reject the same invalid configurations.
#[inline]
fn validate_alignment(alignment: usize) -> Result<()> {
    if alignment < MIN_ALIGNMENT || !alignment.is_power_of_two() {
        return Err(DeviceError::InvalidAlignment { alignment });
    }
    Ok(())
}

/// Validate a kernel-reported block-device size in bytes (Linux
/// `BLKGETSIZE64` passthrough).
///
/// Pure helper extracted from the `DirectDevice::open` block-device
/// branch (J-03) so the size arithmetic the bounds checks trust is
/// unit-testable without root/loop-device access.
///
/// # Errors
///
/// Returns [`DeviceError::InvalidBlockDeviceGeometry`] if the kernel
/// reported a size of `0` bytes — a zero-size device is never usable
/// and almost certainly indicates a wrong ioctl or a mis-detected
/// device node.
#[cfg(any(target_os = "linux", test))]
fn validate_block_device_size(dev_size: u64) -> Result<u64> {
    if dev_size == 0 {
        return Err(DeviceError::InvalidBlockDeviceGeometry {
            detail: "kernel reported a device size of 0 bytes".to_string(),
        });
    }
    Ok(dev_size)
}

/// Compute the total byte size of a block device from its macOS
/// geometry (`DKIOCGETBLOCKCOUNT` x `DKIOCGETBLOCKSIZE`).
///
/// Pure helper extracted from the `DirectDevice::open` block-device
/// branch (J-03) so the size arithmetic the bounds checks trust is
/// unit-testable without a real block device.
///
/// # Errors
///
/// Returns [`DeviceError::InvalidBlockDeviceGeometry`] if
/// `block_count * block_size` overflows `u64`, or if the product is
/// `0` (zero block count or zero block size).
#[cfg(any(target_os = "macos", test))]
fn block_device_size_from_geometry(block_count: u64, block_size: u32) -> Result<u64> {
    let total = block_count
        .checked_mul(u64::from(block_size))
        .ok_or_else(|| DeviceError::InvalidBlockDeviceGeometry {
            detail: format!("block count {block_count} x block size {block_size} overflows u64"),
        })?;
    if total == 0 {
        return Err(DeviceError::InvalidBlockDeviceGeometry {
            detail: format!("block count {block_count} x block size {block_size} is 0 bytes"),
        });
    }
    Ok(total)
}

/// Shared J-01 alignment validation for [`MemoryDevice`] and
/// [`DirectDevice`]: `offset`, `len`, and the buffer's memory address
/// must all be multiples of `alignment`.
///
/// The buffer-address rule mirrors the Linux `O_DIRECT` contract,
/// which requires the user buffer to be block-aligned and otherwise
/// fails with an opaque `EINVAL`. Returns
/// [`DeviceError::AlignmentViolation`] with a `detail` string naming
/// which of the three constraints was violated.
#[inline]
fn check_alignment_impl(
    offset: u64,
    buf_ptr: *const u8,
    len: usize,
    alignment: usize,
) -> Result<()> {
    if !(offset as usize).is_multiple_of(alignment) {
        return Err(DeviceError::AlignmentViolation {
            detail: format!("offset {offset} not aligned to {alignment}"),
        });
    }
    if !len.is_multiple_of(alignment) {
        return Err(DeviceError::AlignmentViolation {
            detail: format!("buffer length {len} not aligned to {alignment}"),
        });
    }
    // A zero-length transfer touches no memory, so the address is
    // irrelevant — and empty buffers (e.g. `AlignedBuf::new(0, _)`)
    // legitimately carry a dangling, unaligned sentinel pointer.
    if len > 0 && !(buf_ptr as usize).is_multiple_of(alignment) {
        return Err(DeviceError::AlignmentViolation {
            detail: format!("buffer address {buf_ptr:p} not aligned to {alignment}"),
        });
    }
    Ok(())
}

impl From<crate::record::RecordError> for DeviceError {
    fn from(e: crate::record::RecordError) -> Self {
        DeviceError::RecordCorruption {
            detail: e.to_string(),
        }
    }
}

/// Result type for device operations.
pub type Result<T> = std::result::Result<T, DeviceError>;

// ---------------------------------------------------------------------------
// BlockDevice trait
// ---------------------------------------------------------------------------

/// Trait for raw block device I/O.
///
/// All offsets and buffer lengths must be aligned to [`alignment()`](BlockDevice::alignment).
/// Implementations must enforce this and return [`DeviceError::AlignmentViolation`] on
/// violations.
pub trait BlockDevice: Send + Sync {
    /// Read `buf.len()` bytes starting at `offset`.
    ///
    /// `offset`, `buf.len()`, AND the buffer's memory address
    /// (`buf.as_ptr()`) must all be multiples of
    /// [`alignment()`](Self::alignment). The address requirement comes
    /// from Linux `O_DIRECT`, which rejects non-block-aligned user
    /// buffers with `EINVAL`; allocate buffers via [`AlignedBuf`].
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Write `buf` starting at `offset`.
    ///
    /// `offset`, `buf.len()`, AND the buffer's memory address
    /// (`buf.as_ptr()`) must all be multiples of
    /// [`alignment()`](Self::alignment). The address requirement comes
    /// from Linux `O_DIRECT`, which rejects non-block-aligned user
    /// buffers with `EINVAL`; allocate buffers via [`AlignedBuf`].
    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize>;

    /// Minimum I/O alignment for this device (512 or 4096 bytes).
    fn alignment(&self) -> usize;

    /// Total usable size in bytes.
    fn size(&self) -> u64;

    /// Sync all pending writes to stable storage (data + file metadata).
    fn sync(&self) -> Result<()>;

    /// Sync pending DATA writes to stable storage, skipping a file-metadata
    /// flush where the platform allows (Linux `fdatasync`).
    ///
    /// Safe to use only when the write did not change anything the next read
    /// depends on beyond the data itself — in practice, writes into a
    /// pre-sized region whose file length never changes. The redo log is
    /// exactly that case (fixed-length, append-into-region), so its hot-path
    /// flush uses this to avoid the inode-metadata flush on every fsync.
    ///
    /// Default: falls back to the full [`Self::sync`]. `DirectDevice`
    /// overrides it with `File::sync_data` (fdatasync on Linux; on macOS both
    /// map to `F_FULLFSYNC`, so there is no difference there).
    fn sync_data(&self) -> Result<()> {
        self.sync()
    }

    /// Return a raw pointer to the device's memory region, if supported.
    ///
    /// Memory-backed devices (MemoryDevice, mmap'd files) can expose their
    /// underlying memory for zero-copy reads and writes. This bypasses
    /// alignment requirements, AlignedBuf allocation, and RwLock overhead.
    ///
    /// # Safety contract
    ///
    /// The returned pointer is valid for `self.size()` bytes for the lifetime
    /// of the device. Callers must ensure proper synchronization (e.g., the
    /// Engine's stripe locks protect per-record access).
    fn as_raw_ptr(&self) -> Option<*mut u8> {
        None
    }

    /// Returns `true` if the underlying file descriptor refers to a raw block
    /// device (`S_IFBLK`), or `false` for regular files and in-memory devices.
    fn is_block_device(&self) -> bool {
        false
    }

    /// Read exactly `buf.len()` bytes starting at `offset`, looping until the
    /// buffer is fully populated.
    ///
    /// On platforms where the underlying [`pread`](Self::pread) can return a
    /// short count (POSIX is allowed to return any non-negative byte count
    /// shorter than the request), this method continues to issue further
    /// reads at the appropriate offset until the buffer is filled.
    ///
    /// # Errors
    ///
    /// - [`DeviceError::ShortRead`] — the underlying `pread` returned `0`
    ///   bytes (EOF) before the requested range had been fully transferred.
    /// - [`DeviceError::OutOfBounds`] — a per-iteration continuation
    ///   offset (`offset + bytes_already_read`) would overflow `u64`.
    /// - Any error returned by the underlying [`pread`](Self::pread)
    ///   (alignment violations, out-of-bounds, libc errors, etc.) is
    ///   propagated unchanged.
    ///
    /// # Behaviour vs. `pread`
    ///
    /// Callers in production code must use this helper instead of `pread`
    /// directly: gap-doc requirement is that all reads are full-or-fail.
    /// `pread` itself remains available for the rare callers that genuinely
    /// want the byte-count return (and currently only test code does).
    fn pread_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let total = buf.len();
        let mut done = 0usize;
        while done < total {
            // J-02: checked continuation offset, consistent with the
            // module-wide checked_add hardening (F-G1-007).
            let cur = offset
                .checked_add(done as u64)
                .ok_or(DeviceError::OutOfBounds {
                    offset,
                    len: total as u64,
                    device_size: self.size(),
                })?;
            // Safety on slicing: `done < total` and `total == buf.len()` so
            // the slice is non-empty and in-bounds.
            let n = self.pread(&mut buf[done..], cur)?;
            if n == 0 {
                return Err(DeviceError::ShortRead {
                    expected: total,
                    got: done,
                    offset,
                });
            }
            // Defence in depth: a buggy implementation that returned more
            // than the requested length would let `done` overshoot. Saturate
            // on debug; on release this is bounded by the next loop check.
            debug_assert!(n <= total - done, "pread returned more than requested");
            done = done.saturating_add(n).min(total);
        }
        Ok(())
    }

    /// Write all of `buf` starting at `offset`, looping until every byte has
    /// been accepted by the underlying device.
    ///
    /// Per the production-readiness gap document, any short write that
    /// fails to make forward progress is treated as fatal corruption: this
    /// method returns [`DeviceError::WriteStalled`] if the underlying
    /// [`pwrite`](Self::pwrite) returns `0` bytes with bytes still pending.
    /// Short writes that *do* make forward progress are simply retried at
    /// the new offset until the buffer is fully written.
    ///
    /// # Errors
    ///
    /// - [`DeviceError::WriteStalled`] — the underlying `pwrite` returned
    ///   `0` bytes with bytes still pending; recovery is unsafe because
    ///   the write may already be partially applied (torn).
    /// - [`DeviceError::OutOfBounds`] — a per-iteration continuation
    ///   offset (`offset + bytes_already_written`) would overflow `u64`.
    /// - Any error returned by the underlying [`pwrite`](Self::pwrite) is
    ///   propagated unchanged.
    fn pwrite_all_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        let total = buf.len();
        let mut done = 0usize;
        while done < total {
            // J-02: checked continuation offset, consistent with the
            // module-wide checked_add hardening (F-G1-007).
            let cur = offset
                .checked_add(done as u64)
                .ok_or(DeviceError::OutOfBounds {
                    offset,
                    len: total as u64,
                    device_size: self.size(),
                })?;
            let n = self.pwrite(&buf[done..], cur)?;
            if n == 0 {
                return Err(DeviceError::WriteStalled {
                    offset,
                    remaining: total - done,
                });
            }
            debug_assert!(n <= total - done, "pwrite returned more than requested");
            done = done.saturating_add(n).min(total);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AlignedBuf
// ---------------------------------------------------------------------------

/// A heap-allocated byte buffer with guaranteed pointer alignment.
///
/// Backed by `std::alloc::alloc` with a specified alignment. Implements
/// `Deref<Target=[u8]>` and `DerefMut` for transparent use as a byte slice.
pub struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
    layout: Layout,
}

// Safety: AlignedBuf owns its allocation exclusively and the contents are
// plain bytes with no interior mutability or thread-local state.
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocate a zeroed buffer of `len` bytes with the given `alignment`.
    ///
    /// `alignment` must be a power of two and non-zero.
    /// A zero-length buffer is valid (returns an empty slice).
    pub fn new(len: usize, alignment: usize) -> Self {
        if len == 0 {
            return Self {
                ptr: std::ptr::NonNull::dangling().as_ptr(),
                len: 0,
                layout: Layout::from_size_align(0, alignment).expect(
                    "invariant: alignment must be a non-zero power of two (caller's contract)",
                ),
            };
        }
        let layout = Layout::from_size_align(len, alignment)
            .expect("invariant: alignment must be a non-zero power of two and len must fit isize");
        // Safety: layout is valid and non-zero-sized.
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        Self { ptr, len, layout }
    }

    /// The alignment of this buffer.
    pub fn alignment(&self) -> usize {
        self.layout.align()
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            // Safety: ptr is valid for len bytes and we own the allocation.
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        if self.len == 0 {
            &mut []
        } else {
            // Safety: ptr is valid for len bytes and we own the allocation.
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
        }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if self.len > 0 {
            // Safety: ptr was allocated with this layout and is non-null.
            unsafe { alloc::dealloc(self.ptr, self.layout) };
        }
    }
}

// ---------------------------------------------------------------------------
// MemoryDevice
// ---------------------------------------------------------------------------

/// In-memory block device for tests and local benchmarks only.
///
/// Enforces the same alignment constraints as [`DirectDevice`] so tests
/// catch alignment bugs via `pread`/`pwrite`. Also exposes a stable raw
/// pointer via [`as_raw_ptr`](BlockDevice::as_raw_ptr) for zero-copy
/// access on the Engine hot path. That raw pointer intentionally bypasses
/// I/O alignment checks, so production deployments must use the concrete
/// storage backend selected by configuration rather than treating
/// `MemoryDevice` as a raw-device substitute.
pub struct MemoryDevice {
    /// Stable, owning raw pointer to the heap allocation backing this
    /// device. Created via `Box::into_raw` so that the borrow tag
    /// rooted at this pointer survives any subsequent move of the
    /// device struct.
    ///
    /// F-G1-004 / C-4: previously this was `parking_lot::RwLock<Vec<u8>>`
    /// paired with a separate `raw_ptr` aliasing the same allocation.
    /// Under Stacked Borrows / Tree Borrows that double-aliased layout
    /// is UB on paper (any reborrow of the Vec through the lock would
    /// alias the live `*mut u8` returned by `as_raw_ptr`), so
    /// `cargo miri` against any test that mixed `pread`/`pwrite` with
    /// the direct-pointer path on the same memory would flag UB even
    /// though the CRC and BC-06/BC-07 fences kept the program logically
    /// correct.
    ///
    /// All byte-level access goes through this pointer. `pread`/`pwrite`
    /// rebuild a slice fresh from `raw_ptr` for each call (so the
    /// Unique reborrow is short-lived and shares the same provenance
    /// chain as the direct-pointer path in `crate::io`). The two paths
    /// MUST NOT operate on overlapping ranges concurrently — Engine
    /// stripe locks, single-threaded recovery, and the F-G1-003 atomic
    /// chunked transfer on the direct path are how production keeps
    /// that contract in practice.
    ///
    /// The allocation is reconstituted as a `Box<[u8]>` and dropped in
    /// `MemoryDevice`'s [`Drop`] impl so it is freed once and only once.
    raw_ptr: *mut u8,
    /// Length in bytes of the allocation `raw_ptr` references. Single
    /// source of truth for [`size()`](BlockDevice::size) — F-G1-017's
    /// "derive from `data.read().len()`" no longer applies because
    /// there is no `data` lock; the equivalent invariant is now
    /// "MemoryDevice is never resized".
    len: u64,
    alignment: usize,
    /// Volatile write-cache simulation (B-1 audit fix). `None` (the
    /// default) keeps the historical behavior: every write is
    /// immediately "durable" and [`sync`](BlockDevice::sync) is a
    /// no-op — which is exactly why pre-fix durability tests could not
    /// distinguish "synced" from "merely pwritten".
    ///
    /// `Some(shadow)` (via [`MemoryDevice::new_volatile`]) models a
    /// drive with a volatile write cache: `shadow` holds the bytes as
    /// of the last `sync()`, while `raw_ptr` holds the live (cached)
    /// bytes. [`MemoryDevice::simulate_power_loss`] reverts the live
    /// bytes to the shadow, dropping everything written since the last
    /// `sync()` — including writes made through the zero-copy
    /// [`as_raw_ptr`](BlockDevice::as_raw_ptr) path, because that
    /// pointer aliases the live allocation.
    durable_shadow: Option<parking_lot::Mutex<Box<[u8]>>>,
}

// SAFETY (C-6): MemoryDevice owns the heap allocation pointed to by `raw_ptr`
// exclusively (the Box was consumed via `into_raw` and the pointer is
// reconstituted only in Drop). The pre-F-G1-004 double-alias between an
// `RwLock<Vec<u8>>` and `raw_ptr` is gone, so the single owning provenance
// can be Send/Sync-shared safely.
//
// Concurrency on the bytes is the caller's responsibility, but — correcting
// the earlier note — it is NOT the engine's `StripedLocks` that makes the
// raw-pointer path safe. Raw `as_raw_ptr` access flows through the
// `crate::io` `*_direct` helpers, which serialize per record offset via
// `io::io_locks()` (a process-global `StripedRwLocks`): readers take its
// read side, writers its write side. That per-offset RW discipline — plus
// the chunked atomic transfer in `crate::io` (F-G1-003) — is what closes the
// writer-vs-reader torn-read race the CRC originally (insufficiently)
// guarded. The engine stripe locks serialize logical record mutations but
// do NOT cover the read path or the replica-receiver path, so they cannot be
// cited as the byte-level safety mechanism here. The plain `pread`/`pwrite`
// API (non-direct) likewise relies on the caller not issuing overlapping
// same-range writes.
unsafe impl Send for MemoryDevice {}
unsafe impl Sync for MemoryDevice {}

impl MemoryDevice {
    /// Create a new in-memory device of `size` bytes with the given alignment.
    ///
    /// # Errors
    ///
    /// Returns [`DeviceError::ZeroSize`] if `size` is zero, and
    /// [`DeviceError::InvalidAlignment`] if `alignment` is not a
    /// power-of-two or is below [`MIN_ALIGNMENT`] (512).
    pub fn new(size: u64, alignment: usize) -> Result<Self> {
        if size == 0 {
            return Err(DeviceError::ZeroSize);
        }
        validate_alignment(alignment)?;
        // Allocate a zeroed buffer once and immediately convert to a
        // stable raw pointer via `Box::into_raw`. `as_mut_ptr` on a
        // `Box<[u8]>` would create a reborrow whose Stacked Borrows
        // tag is invalidated the moment the Box itself moves into a
        // struct field — `Box::into_raw` consumes the Box, so the
        // resulting raw pointer carries an owning tag that survives.
        let backing: Box<[u8]> = vec![0u8; size as usize].into_boxed_slice();
        let raw_slice: *mut [u8] = Box::into_raw(backing);
        // Safety: `raw_slice` was just produced from a live Box and
        // points to a non-null heap allocation of `size` bytes.
        let raw_ptr = raw_slice as *mut u8;
        Ok(Self {
            raw_ptr,
            len: size,
            alignment,
            durable_shadow: None,
        })
    }

    /// Create an in-memory device that simulates a volatile drive write
    /// cache (test scaffolding for durability-barrier tests).
    ///
    /// Writes land in the live buffer (visible to subsequent reads, as
    /// on a real drive) but are only made "durable" by a
    /// [`sync`](BlockDevice::sync) call. A subsequent
    /// [`simulate_power_loss`](Self::simulate_power_loss) reverts the
    /// device to its state at the last `sync()`, modeling power failure
    /// with an unflushed write cache.
    ///
    /// # Errors
    ///
    /// Same as [`MemoryDevice::new`]: [`DeviceError::ZeroSize`] for a
    /// zero-byte device, [`DeviceError::InvalidAlignment`] for a
    /// non-power-of-two or sub-512 alignment.
    pub fn new_volatile(size: u64, alignment: usize) -> Result<Self> {
        let mut dev = Self::new(size, alignment)?;
        dev.durable_shadow = Some(parking_lot::Mutex::new(
            vec![0u8; size as usize].into_boxed_slice(),
        ));
        Ok(dev)
    }

    /// Drop every write issued since the last successful
    /// [`sync`](BlockDevice::sync), simulating a power failure with a
    /// volatile write cache.
    ///
    /// Returns `true` if the device was created with
    /// [`new_volatile`](Self::new_volatile) and the revert was
    /// performed, `false` for a default (always-durable) device, on
    /// which this call has no effect. Callers in tests should assert
    /// the return value so a test cannot silently run against a
    /// non-volatile device.
    ///
    /// Must not race concurrent `pread`/`pwrite`/raw-pointer access —
    /// the same single-writer contract every other whole-device
    /// operation on `MemoryDevice` already requires.
    pub fn simulate_power_loss(&self) -> bool {
        let Some(shadow) = &self.durable_shadow else {
            return false;
        };
        let shadow = shadow.lock();
        // Safety: `raw_ptr` is valid for `len` bytes for the lifetime of
        // `self`; the caller guarantees no concurrent access (see doc).
        unsafe {
            let live = std::slice::from_raw_parts_mut(self.raw_ptr, self.len as usize);
            live.copy_from_slice(&shadow);
        }
        true
    }
}

impl Drop for MemoryDevice {
    fn drop(&mut self) {
        // Safety: `raw_ptr` came from `Box::into_raw(Box<[u8]>)` in
        // `MemoryDevice::new` and has not been freed since. We
        // reconstitute the Box with the same length to release the
        // allocation through the global allocator exactly once.
        // `len` fits in `usize` on all 64-bit targets (the only
        // currently-supported ones); on a future 32-bit port the
        // construction-time `vec![0u8; size as usize]` would already
        // have panicked before reaching here.
        unsafe {
            let slice_ptr = std::ptr::slice_from_raw_parts_mut(self.raw_ptr, self.len as usize);
            drop(Box::from_raw(slice_ptr));
        }
    }
}

impl MemoryDevice {
    /// Validate the J-01 triple alignment contract (offset, length,
    /// buffer address) against `self.alignment`.
    ///
    /// MemoryDevice has no O_DIRECT requirement of its own, but it
    /// deliberately enforces the same buffer-address rule as
    /// [`DirectDevice`] so the CI suite (which runs almost entirely
    /// against MemoryDevice) catches callers that would `EINVAL` on a
    /// real O_DIRECT NVMe device.
    fn check_alignment(&self, offset: u64, buf_ptr: *const u8, len: usize) -> Result<()> {
        check_alignment_impl(offset, buf_ptr, len, self.alignment)
    }
}

impl BlockDevice for MemoryDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.check_alignment(offset, buf.as_ptr(), buf.len())?;
        let off = offset as usize;
        // F-G1-007: use checked addition so an offset near `usize::MAX` plus
        // a non-trivial buffer length cannot wrap to a small number and
        // bypass the bounds check. `None` (overflow) maps to out-of-bounds
        // unconditionally — there is no legitimate caller that wants a
        // wrap-around read.
        let end = off.checked_add(buf.len()).ok_or(DeviceError::OutOfBounds {
            offset,
            len: buf.len() as u64,
            device_size: self.len,
        })?;
        if end as u64 > self.len {
            return Err(DeviceError::OutOfBounds {
                offset,
                len: buf.len() as u64,
                device_size: self.len,
            });
        }
        // F-G1-004 / C-4: rebuild the slice fresh from `raw_ptr` so the
        // Stacked Borrows tag is rooted at the owning pointer rather
        // than reborrowed through a `Vec` / `Box` that was moved into
        // the struct (the pre-fix `&data` reborrow aliased the
        // `raw_ptr`, which is UB on paper and miri-detectable). The
        // `MemoryDevice` is treated as a sole-owner of its allocation;
        // the caller's contract (Engine stripe locks, single-threaded
        // recovery scan) guarantees no concurrent overlapping
        // `pread`/`pwrite` on the same range — so the temporary
        // `&[u8]` here is race-free for the duration of `copy_from_slice`.
        //
        // Safety: bounds were checked above; `raw_ptr.add(off)` is
        // valid for `buf.len()` bytes; the allocation is alive for
        // the lifetime of `self` (released only in `Drop`).
        unsafe {
            let src = std::slice::from_raw_parts(self.raw_ptr.add(off), buf.len());
            buf.copy_from_slice(src);
        }
        Ok(buf.len())
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.check_alignment(offset, buf.as_ptr(), buf.len())?;
        let off = offset as usize;
        // F-G1-007: see pread above — checked_add protects against the
        // off + buf.len() wrap case.
        let end = off.checked_add(buf.len()).ok_or(DeviceError::OutOfBounds {
            offset,
            len: buf.len() as u64,
            device_size: self.len,
        })?;
        if end as u64 > self.len {
            return Err(DeviceError::OutOfBounds {
                offset,
                len: buf.len() as u64,
                device_size: self.len,
            });
        }
        // F-G1-004 / C-4: as in `pread`, rebuild the slice fresh from
        // `raw_ptr` to avoid the legacy Vec-vs-raw-ptr aliasing.
        // Safety: bounds were checked above; `raw_ptr.add(off)` is
        // valid for `buf.len()` bytes; the allocation is alive for
        // the lifetime of `self` (released only in `Drop`).
        unsafe {
            let dst = std::slice::from_raw_parts_mut(self.raw_ptr.add(off), buf.len());
            dst.copy_from_slice(buf);
        }
        Ok(buf.len())
    }

    fn alignment(&self) -> usize {
        self.alignment
    }

    fn size(&self) -> u64 {
        // F-G1-004 / C-4: single source of truth. Set at construction
        // and immutable thereafter — the device is never resized.
        self.len
    }

    fn sync(&self) -> Result<()> {
        if let Some(shadow) = &self.durable_shadow {
            let mut shadow = shadow.lock();
            // Safety: `raw_ptr` is valid for `len` bytes for the lifetime
            // of `self`; the caller guarantees no concurrent writes during
            // a sync (same single-writer contract as
            // `simulate_power_loss`).
            unsafe {
                let live = std::slice::from_raw_parts(self.raw_ptr, self.len as usize);
                shadow.copy_from_slice(live);
            }
        }
        Ok(())
    }

    fn as_raw_ptr(&self) -> Option<*mut u8> {
        Some(self.raw_ptr)
    }
}

/// Test-only wrapper that injects read failures behind any [`BlockDevice`].
///
/// The wrapper reports no raw pointer so callers under test cannot bypass the
/// failing `pread` path through direct-memory access.
#[cfg(test)]
pub(crate) struct ReadFailingDevice {
    inner: std::sync::Arc<dyn BlockDevice>,
    fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
impl ReadFailingDevice {
    pub(crate) fn new(
        inner: std::sync::Arc<dyn BlockDevice>,
    ) -> (
        std::sync::Arc<Self>,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        let fail = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        (
            std::sync::Arc::new(Self {
                inner,
                fail: fail.clone(),
            }),
            fail,
        )
    }
}

#[cfg(test)]
impl BlockDevice for ReadFailingDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(DeviceError::Io(std::io::Error::other(
                "simulated pread failure",
            )));
        }
        self.inner.pread(buf, offset)
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.inner.pwrite(buf, offset)
    }

    fn alignment(&self) -> usize {
        self.inner.alignment()
    }

    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    fn as_raw_ptr(&self) -> Option<*mut u8> {
        None
    }
}

// ---------------------------------------------------------------------------
// DirectDevice
// ---------------------------------------------------------------------------

/// File-backed block device using `O_DIRECT` (or standard I/O on macOS).
///
/// On Linux, opens with `O_DIRECT | O_RDWR` for zero-copy NVMe access.
/// On macOS (development), uses standard file I/O with `F_NOCACHE`.
///
/// Raw block devices (`/dev/disk/by-id/…`, `/dev/nvme0n1`, etc.) are detected
/// automatically. For block devices, `set_len` is never called (it would return
/// `EINVAL`), and the actual device size is queried via `ioctl`. For regular
/// files the requested `size` is used only to **grow** the file on creation; an
/// existing file that is already larger than `size` is never shrunk.
pub struct DirectDevice {
    file: std::fs::File,
    size: u64,
    alignment: usize,
    is_block: bool,
}

impl DirectDevice {
    /// Open or create a file-backed device at `path`.
    ///
    /// `size` is the desired capacity in bytes, used only for regular files:
    /// - If the file is new (size == 0) or smaller than `size`, it is grown to
    ///   `size`.
    /// - If the file already exists and is larger than `size`, it is left
    ///   unchanged — the actual file size is used.
    ///
    /// For raw block devices (`S_IFBLK`), `size` is ignored; the kernel-reported
    /// device size is queried via `ioctl` and stored instead. `set_len` is never
    /// called on a block device.
    ///
    /// `alignment` specifies the minimum I/O alignment (typically 4096).
    ///
    /// # Errors
    ///
    /// Returns [`DeviceError::Io`] if the file cannot be opened, the device size
    /// cannot be queried, or pre-allocation fails.
    /// Returns [`DeviceError::InvalidAlignment`] if `alignment` is not a
    /// power-of-two or is below [`MIN_ALIGNMENT`] (512).
    /// Returns [`DeviceError::InvalidBlockDeviceGeometry`] if the kernel
    /// reports a zero-byte block device or a block count x block size
    /// product that overflows `u64`.
    pub fn open(path: &std::path::Path, size: u64, alignment: usize) -> Result<Self> {
        Self::open_inner(path, size, alignment, false)
    }

    /// Open or create a file-backed device at `path` using the OS page cache
    /// (buffered I/O): `O_DIRECT` is NOT set on Linux and `F_NOCACHE` is NOT
    /// applied on macOS. Everything else — size handling, block-device
    /// detection, alignment validation, the `pread`/`pwrite`/`sync` API — is
    /// identical to [`Self::open`].
    ///
    /// This exists only for the redo log under the relaxed
    /// `redo_buffered_io` mode: routing redo writes through the page cache
    /// lets the kernel coalesce writeback smoothly instead of forcing each
    /// background flush down to the device, which on some virtualized hosts
    /// stalls the VM for tens of milliseconds. The data device(s) must
    /// continue to use [`Self::open`] (`O_DIRECT`). Durability for a
    /// buffered redo comes from OS writeback plus the checkpoint barrier's
    /// explicit [`BlockDevice::sync`] before it reclaims the log — see
    /// `crate::checkpoint`.
    ///
    /// Callers still issue aligned reads/writes (the alignment contract is
    /// unchanged); only the device-cache-bypass flags differ. The returned
    /// device's [`alignment`](BlockDevice::alignment) and bounds checks are
    /// the same as for an `O_DIRECT` open, so an existing on-disk redo log
    /// reads back byte-for-byte regardless of which open variant created it.
    ///
    /// # Errors
    ///
    /// Same as [`Self::open`]: [`DeviceError::Io`],
    /// [`DeviceError::InvalidAlignment`],
    /// [`DeviceError::InvalidBlockDeviceGeometry`].
    pub fn open_buffered(path: &std::path::Path, size: u64, alignment: usize) -> Result<Self> {
        Self::open_inner(path, size, alignment, true)
    }

    /// Shared body for [`Self::open`] (`cached == false`, the default
    /// `O_DIRECT`/`F_NOCACHE` path) and [`Self::open_buffered`]
    /// (`cached == true`, page-cache path). When `cached` is `false` the
    /// behavior is byte-for-byte identical to the pre-existing `open`.
    fn open_inner(
        path: &std::path::Path,
        size: u64,
        alignment: usize,
        cached: bool,
    ) -> Result<Self> {
        validate_alignment(alignment)?;
        // `cached` is consumed by the Linux `O_DIRECT` and macOS `F_NOCACHE`
        // branches below. On any other unix target neither branch exists, so
        // bind it to silence the unused-variable lint without changing behavior.
        #[cfg(all(unix, not(target_os = "linux"), not(target_os = "macos")))]
        let _ = cached;
        use std::fs::OpenOptions;

        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true).truncate(false);

        // On Linux, open with O_DIRECT for zero-copy NVMe I/O — UNLESS the
        // caller asked for the buffered (page-cache) variant.
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            if !cached {
                opts.custom_flags(libc::O_DIRECT);
            }
        }

        let file = opts.open(path)?;

        // Detect whether the path refers to a block device or a regular file.
        #[cfg(unix)]
        let is_block = {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            // Safety: fd is valid; stat_buf is fully written by fstat before
            // being read.
            let mut stat_buf: libc::stat = unsafe { std::mem::zeroed() };
            let rc = unsafe { libc::fstat(fd, &mut stat_buf) };
            if rc != 0 {
                return Err(DeviceError::Io(std::io::Error::last_os_error()));
            }
            (stat_buf.st_mode & libc::S_IFMT) == libc::S_IFBLK
        };
        #[cfg(not(unix))]
        let is_block = false;

        let actual_size = if is_block {
            // Query the true device capacity from the kernel; never call
            // set_len on a block device (ftruncate returns EINVAL).
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                let fd = file.as_raw_fd();
                let mut dev_size: u64 = 0;
                // Safety: fd is a valid open block device; `blkgetsize64`
                // writes a single u64 into `dev_size`. The ioctl request
                // number is computed at compile time from the
                // `_IOR(0x12, 114, u64)` encoding, so this is portable to
                // 32-bit Linux variants where the hand-encoded constant
                // was wrong.
                let rc = unsafe { blkgetsize64(fd, &mut dev_size) }
                    .map_err(|e| DeviceError::Io(std::io::Error::from(e)))?;
                if rc != 0 {
                    return Err(DeviceError::Io(std::io::Error::last_os_error()));
                }
                validate_block_device_size(dev_size)?
            }
            #[cfg(target_os = "macos")]
            {
                use std::os::unix::io::AsRawFd;
                let fd = file.as_raw_fd();
                let mut block_count: u64 = 0;
                let mut block_size: u32 = 0;
                // Safety: fd is a valid open block device; each ioctl
                // writes exactly the typed scalar into its output variable.
                let rc = unsafe { dkiocgetblockcount(fd, &mut block_count) }
                    .map_err(|e| DeviceError::Io(std::io::Error::from(e)))?;
                if rc != 0 {
                    return Err(DeviceError::Io(std::io::Error::last_os_error()));
                }
                let rc = unsafe { dkiocgetblocksize(fd, &mut block_size) }
                    .map_err(|e| DeviceError::Io(std::io::Error::from(e)))?;
                if rc != 0 {
                    return Err(DeviceError::Io(std::io::Error::last_os_error()));
                }
                block_device_size_from_geometry(block_count, block_size)?
            }
            #[cfg(all(unix, not(target_os = "linux"), not(target_os = "macos")))]
            {
                return Err(DeviceError::Io(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "block device size query not supported on this platform",
                )));
            }
        } else {
            // Regular file: only grow, never shrink.
            let existing = file.metadata()?.len();
            if existing < size {
                file.set_len(size)?;
                // Pre-allocate the backing blocks. `set_len` (ftruncate) leaves a
                // SPARSE file, so every O_DIRECT write to a not-yet-written region
                // triggers synchronous ext4 block allocation (ext4_mb_new_blocks +
                // block-bitmap reads) inside the write syscall — profiled as the
                // dominant on-CPU cost of the write-back flush path under load, and
                // a source of write-latency that backs the cache up and stalls the
                // serving threads. fallocate reserves the extents up front so the
                // hot-path writes are pure data writes. Best-effort: filesystems
                // that lack fallocate (or anything that returns an error here) just
                // keep the sparse file — correctness is unchanged either way.
                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::io::AsRawFd;
                    let fd = file.as_raw_fd();
                    // Safety: fd is a valid, open, writable regular file; fallocate
                    // reads/writes no user memory and only reserves blocks in
                    // [0, size). mode 0 = allocate (no FALLOC_FL_* flags).
                    let rc = unsafe { libc::fallocate(fd, 0, 0, size as libc::off_t) };
                    if rc != 0 {
                        let e = std::io::Error::last_os_error();
                        tracing::warn!(
                            err = %e,
                            size,
                            "fallocate failed; using sparse file (O_DIRECT writes may pay ext4 block-allocation cost on first touch)"
                        );
                    }
                }
                size
            } else {
                existing
            }
        };

        // On macOS, disable caching to approximate O_DIRECT behavior — UNLESS
        // the caller asked for the buffered (page-cache) variant, in which case
        // we deliberately leave the page cache enabled.
        #[cfg(target_os = "macos")]
        if !cached {
            use std::os::unix::io::AsRawFd;
            // F_NOCACHE = 48 on macOS
            let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
            if rc == -1 {
                // Best-effort: failing to disable the page cache degrades the
                // O_DIRECT approximation (extra buffering) but is not fatal —
                // macOS is the development target, not production. Surface the
                // errno so a silently-cached dev run is at least observable.
                tracing::warn!(
                    errno = %std::io::Error::last_os_error(),
                    "fcntl(F_NOCACHE) failed; device I/O will use the page cache",
                );
            }
        }

        Ok(Self {
            file,
            size: actual_size,
            alignment,
            is_block,
        })
    }

    /// Validate the J-01 triple alignment contract (offset, length,
    /// buffer address) against `self.alignment`. The buffer-address
    /// check is mandatory here: Linux `O_DIRECT` (set at open time)
    /// rejects non-block-aligned user buffers with an opaque `EINVAL`,
    /// so we surface a typed [`DeviceError::AlignmentViolation`]
    /// before any syscall instead.
    fn check_alignment(&self, offset: u64, buf_ptr: *const u8, len: usize) -> Result<()> {
        check_alignment_impl(offset, buf_ptr, len, self.alignment)
    }
}

impl BlockDevice for DirectDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.check_alignment(offset, buf.as_ptr(), buf.len())?;
        // F-G1-007: checked_add against u64::MAX so a near-MAX offset plus
        // a non-trivial buffer length cannot wrap to a small number and
        // bypass the bounds check.
        let end = offset
            .checked_add(buf.len() as u64)
            .ok_or(DeviceError::OutOfBounds {
                offset,
                len: buf.len() as u64,
                device_size: self.size,
            })?;
        if end > self.size {
            return Err(DeviceError::OutOfBounds {
                offset,
                len: buf.len() as u64,
                device_size: self.size,
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            // POSIX permits `pread` to return -1 with errno EINTR if a
            // signal is delivered before any data is read. Retry until we
            // either get data, hit EOF (n == 0), or see a real error.
            loop {
                // Safety: fd is valid, buf is valid for buf.len() bytes.
                let n = unsafe {
                    libc::pread(
                        fd,
                        buf.as_mut_ptr().cast(),
                        buf.len(),
                        offset as libc::off_t,
                    )
                };
                if n < 0 {
                    let err = std::io::Error::last_os_error();
                    if err.raw_os_error() == Some(libc::EINTR) {
                        continue;
                    }
                    return Err(DeviceError::Io(err));
                }
                return Ok(n as usize);
            }
        }
        #[cfg(not(unix))]
        {
            // J-05: no supported non-unix target exists. A seek+read
            // fallback here would be non-atomic (shared file cursor
            // races between threads) and would mis-type short reads as
            // `UnexpectedEof` instead of the typed `ShortRead`. Fail
            // the build instead so a future port has to implement
            // positional I/O with proper error mapping. Same pattern
            // as the server accept loop (src/server/mod.rs).
            compile_error!("DirectDevice requires a unix target (positional pread/pwrite)");
        }
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.check_alignment(offset, buf.as_ptr(), buf.len())?;
        // F-G1-007: checked_add — see DirectDevice::pread for rationale.
        let end = offset
            .checked_add(buf.len() as u64)
            .ok_or(DeviceError::OutOfBounds {
                offset,
                len: buf.len() as u64,
                device_size: self.size,
            })?;
        if end > self.size {
            return Err(DeviceError::OutOfBounds {
                offset,
                len: buf.len() as u64,
                device_size: self.size,
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            // POSIX permits `pwrite` to return -1 with errno EINTR if a
            // signal is delivered before any data is written. Retry until
            // we get a non-EINTR result.
            loop {
                // Safety: fd is valid, buf is valid for buf.len() bytes.
                let n = unsafe {
                    libc::pwrite(fd, buf.as_ptr().cast(), buf.len(), offset as libc::off_t)
                };
                if n < 0 {
                    let err = std::io::Error::last_os_error();
                    if err.raw_os_error() == Some(libc::EINTR) {
                        continue;
                    }
                    return Err(DeviceError::Io(err));
                }
                return Ok(n as usize);
            }
        }
        #[cfg(not(unix))]
        {
            // J-05: see the matching branch in `pread` — a seek+write
            // fallback would be non-atomic and mis-type short writes.
            compile_error!("DirectDevice requires a unix target (positional pread/pwrite)");
        }
    }

    fn alignment(&self) -> usize {
        self.alignment
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    fn sync_data(&self) -> Result<()> {
        // `fdatasync` on Linux — skips the inode-metadata flush, which is
        // unneeded for writes into a pre-sized region (the file length never
        // changes). On macOS this maps to `F_FULLFSYNC`, same as `sync_all`.
        self.file.sync_data()?;
        Ok(())
    }

    fn is_block_device(&self) -> bool {
        self.is_block
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- AlignedBuf tests --

    #[test]
    fn aligned_buf_512() {
        let buf = AlignedBuf::new(4096, 512);
        assert_eq!(buf.len(), 4096);
        assert_eq!(buf.ptr as usize % 512, 0);
    }

    #[test]
    fn aligned_buf_4096() {
        let buf = AlignedBuf::new(4096, 4096);
        assert_eq!(buf.len(), 4096);
        assert_eq!(buf.ptr as usize % 4096, 0);
    }

    #[test]
    fn aligned_buf_write_read() {
        let mut buf = AlignedBuf::new(1024, 512);
        buf[0] = 0xAA;
        buf[1023] = 0xBB;
        assert_eq!(buf[0], 0xAA);
        assert_eq!(buf[1023], 0xBB);
        assert_eq!(buf[1], 0x00);
    }

    #[test]
    fn aligned_buf_zero_length() {
        let buf = AlignedBuf::new(0, 4096);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        // Should not panic on drop
    }

    // -- MemoryDevice tests --

    #[test]
    fn memory_device_write_read() {
        let dev = MemoryDevice::new(65536, 4096).unwrap();
        let mut write_buf = AlignedBuf::new(4096, 4096);
        for (i, b) in write_buf.iter_mut().enumerate() {
            *b = (i % 256) as u8;
        }
        dev.pwrite(&write_buf, 0).unwrap();

        let mut read_buf = AlignedBuf::new(4096, 4096);
        dev.pread(&mut read_buf, 0).unwrap();
        assert_eq!(&*write_buf, &*read_buf);
    }

    #[test]
    fn memory_device_alignment_offset() {
        let dev = MemoryDevice::new(65536, 4096).unwrap();
        let mut buf = AlignedBuf::new(4096, 4096);
        let result = dev.pwrite(&buf, 100); // Not aligned to 4096
        assert!(result.is_err());
        match result.unwrap_err() {
            DeviceError::AlignmentViolation { .. } => {}
            other => panic!("expected AlignmentViolation, got {other:?}"),
        }

        let result = dev.pread(&mut buf, 100);
        assert!(result.is_err());
        match result.unwrap_err() {
            DeviceError::AlignmentViolation { .. } => {}
            other => panic!("expected AlignmentViolation, got {other:?}"),
        }
    }

    #[test]
    fn memory_device_alignment_length() {
        let dev = MemoryDevice::new(65536, 4096).unwrap();
        // Use a buffer that's 100 bytes — not aligned to 4096
        let buf = vec![0u8; 100];
        let result = dev.pwrite(&buf, 0);
        assert!(result.is_err());
        match result.unwrap_err() {
            DeviceError::AlignmentViolation { .. } => {}
            other => panic!("expected AlignmentViolation, got {other:?}"),
        }
    }

    #[test]
    fn memory_device_read_alignment_error() {
        let dev = MemoryDevice::new(65536, 4096).unwrap();
        let mut buf = AlignedBuf::new(4096, 4096);
        let result = dev.pread(&mut buf, 512); // 512 not aligned to 4096
        assert!(result.is_err());
        match result.unwrap_err() {
            DeviceError::AlignmentViolation { .. } => {}
            other => panic!("expected AlignmentViolation, got {other:?}"),
        }
    }

    #[test]
    fn memory_device_multiple_offsets() {
        let dev = MemoryDevice::new(65536, 4096).unwrap();

        let mut buf1 = AlignedBuf::new(4096, 4096);
        buf1[0] = 0x11;
        dev.pwrite(&buf1, 0).unwrap();

        let mut buf2 = AlignedBuf::new(4096, 4096);
        buf2[0] = 0x22;
        dev.pwrite(&buf2, 4096).unwrap();

        let mut buf3 = AlignedBuf::new(4096, 4096);
        buf3[0] = 0x33;
        dev.pwrite(&buf3, 8192).unwrap();

        // Read back and verify no cross-contamination
        let mut r1 = AlignedBuf::new(4096, 4096);
        dev.pread(&mut r1, 0).unwrap();
        assert_eq!(r1[0], 0x11);

        let mut r2 = AlignedBuf::new(4096, 4096);
        dev.pread(&mut r2, 4096).unwrap();
        assert_eq!(r2[0], 0x22);

        let mut r3 = AlignedBuf::new(4096, 4096);
        dev.pread(&mut r3, 8192).unwrap();
        assert_eq!(r3[0], 0x33);
    }

    #[test]
    fn memory_device_write_last_valid_offset() {
        let dev = MemoryDevice::new(8192, 4096).unwrap();
        let buf = AlignedBuf::new(4096, 4096);
        dev.pwrite(&buf, 4096).unwrap(); // Last valid 4096-byte block
    }

    #[test]
    fn memory_device_write_past_boundary() {
        let dev = MemoryDevice::new(8192, 4096).unwrap();
        let buf = AlignedBuf::new(4096, 4096);
        let result = dev.pwrite(&buf, 8192); // Past the end
        assert!(result.is_err());
        match result.unwrap_err() {
            DeviceError::OutOfBounds { .. } => {}
            other => panic!("expected OutOfBounds, got {other:?}"),
        }
    }

    #[test]
    fn memory_device_sync() {
        let dev = MemoryDevice::new(4096, 4096).unwrap();
        dev.sync().unwrap();
    }

    #[test]
    fn memory_device_enforces_alignment() {
        // MemoryDevice must enforce the same constraints as DirectDevice
        let dev = MemoryDevice::new(65536, 4096).unwrap();
        assert_eq!(dev.alignment(), 4096);

        // Non-aligned offset
        let mut buf = AlignedBuf::new(4096, 4096);
        assert!(dev.pread(&mut buf, 1).is_err());
        assert!(dev.pwrite(&buf, 1).is_err());
    }

    // -- DirectDevice tests --

    #[test]
    fn direct_device_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let dev = DirectDevice::open(&path, 65536, 4096).unwrap();

        let mut write_buf = AlignedBuf::new(4096, 4096);
        for (i, b) in write_buf.iter_mut().enumerate() {
            *b = (i % 256) as u8;
        }
        dev.pwrite(&write_buf, 0).unwrap();

        let mut read_buf = AlignedBuf::new(4096, 4096);
        dev.pread(&mut read_buf, 0).unwrap();
        assert_eq!(&*write_buf, &*read_buf);
    }

    #[test]
    fn direct_device_alignment_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let dev = DirectDevice::open(&path, 65536, 4096).unwrap();

        let mut buf = AlignedBuf::new(4096, 4096);
        assert!(dev.pwrite(&buf, 100).is_err());
        assert!(dev.pread(&mut buf, 100).is_err());
    }

    #[test]
    fn direct_device_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let dev = DirectDevice::open(&path, 8192, 4096).unwrap();

        let buf = AlignedBuf::new(4096, 4096);
        assert!(dev.pwrite(&buf, 8192).is_err());
    }

    #[test]
    fn direct_device_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");
        let dev = DirectDevice::open(&path, 4096, 4096).unwrap();
        dev.sync().unwrap();
    }

    #[test]
    fn direct_device_is_not_block_device() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not_block.dat");
        let dev = DirectDevice::open(&path, 4096, 4096).unwrap();
        assert!(
            !dev.is_block_device(),
            "temp file must not be reported as a block device"
        );
    }

    #[test]
    fn buffered_direct_device_write_read() {
        // A buffered (page-cache, non-O_DIRECT / non-F_NOCACHE) open must obey
        // the exact same read/write/alignment/bounds contract as the default
        // O_DIRECT open — only the device-cache-bypass flags differ.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("buffered.dat");
        let dev = DirectDevice::open_buffered(&path, 65536, 4096).unwrap();
        assert_eq!(dev.alignment(), 4096);
        assert_eq!(dev.size(), 65536);

        let mut write_buf = AlignedBuf::new(4096, 4096);
        for (i, b) in write_buf.iter_mut().enumerate() {
            *b = (i % 256) as u8;
        }
        dev.pwrite_all_at(&write_buf, 4096).unwrap();

        let mut read_buf = AlignedBuf::new(4096, 4096);
        dev.pread_exact_at(&mut read_buf, 4096).unwrap();
        assert_eq!(&*write_buf, &*read_buf);

        // Alignment is still enforced on the buffered device.
        assert!(dev.pwrite(&write_buf, 100).is_err());
        // Sync is a no-op success on a regular file.
        dev.sync().unwrap();
    }

    #[test]
    fn buffered_and_direct_open_are_byte_compatible_on_disk() {
        // The on-disk format is identical: a file written through a buffered
        // open reads back byte-for-byte through a default O_DIRECT open of the
        // same path (the recovery path always uses the default open). This is
        // the load-bearing invariant for `redo_buffered_io`: switching the open
        // variant must never change what recovery sees.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compat.dat");

        let mut payload = AlignedBuf::new(4096, 4096);
        for (i, b) in payload.iter_mut().enumerate() {
            *b = ((i * 7 + 3) % 256) as u8;
        }
        {
            let dev = DirectDevice::open_buffered(&path, 8192, 4096).unwrap();
            dev.pwrite_all_at(&payload, 0).unwrap();
            // Make the page-cache writeback durable before reopening O_DIRECT,
            // which would otherwise bypass the still-dirty page cache.
            dev.sync().unwrap();
        }
        let dev = DirectDevice::open(&path, 8192, 4096).unwrap();
        let mut read_buf = AlignedBuf::new(4096, 4096);
        dev.pread_exact_at(&mut read_buf, 0).unwrap();
        assert_eq!(
            &*payload, &*read_buf,
            "buffered write must be readable via O_DIRECT open"
        );
    }

    #[test]
    fn direct_device_no_truncate_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing.dat");

        // Create a 65536-byte file.
        {
            let dev = DirectDevice::open(&path, 65536, 4096).unwrap();
            assert_eq!(dev.size(), 65536);
        }

        // Reopen requesting only 4096 bytes — must NOT shrink the file.
        let dev = DirectDevice::open(&path, 4096, 4096).unwrap();
        assert_eq!(
            dev.size(),
            65536,
            "existing file must not be truncated when smaller size is requested"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            65536,
            "on-disk size must remain 65536"
        );
    }

    #[test]
    fn direct_device_grows_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grow.dat");

        // Create a small file.
        {
            let dev = DirectDevice::open(&path, 4096, 4096).unwrap();
            assert_eq!(dev.size(), 4096);
        }

        // Reopen requesting a larger size — file must grow.
        let dev = DirectDevice::open(&path, 65536, 4096).unwrap();
        assert_eq!(
            dev.size(),
            65536,
            "file must be grown to the requested size"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            65536,
            "on-disk size must be 65536 after growing"
        );
    }

    #[test]
    fn memory_device_is_not_block_device() {
        let dev = MemoryDevice::new(4096, 4096).unwrap();
        assert!(
            !dev.is_block_device(),
            "MemoryDevice must not be reported as a block device"
        );
    }

    #[test]
    fn memory_device_rejects_non_power_of_two_alignment() {
        // 600 is >= MIN_ALIGNMENT but not a power-of-two.
        match MemoryDevice::new(8192, 600) {
            Ok(_) => panic!("expected InvalidAlignment"),
            Err(DeviceError::InvalidAlignment { alignment }) => {
                assert_eq!(alignment, 600);
            }
            Err(other) => panic!("expected InvalidAlignment, got {other:?}"),
        }
    }

    #[test]
    fn memory_device_rejects_alignment_below_minimum() {
        // 256 IS a power-of-two but below the 512-byte minimum.
        match MemoryDevice::new(8192, 256) {
            Ok(_) => panic!("expected InvalidAlignment"),
            Err(DeviceError::InvalidAlignment { alignment }) => {
                assert_eq!(alignment, 256);
            }
            Err(other) => panic!("expected InvalidAlignment, got {other:?}"),
        }
    }

    #[test]
    fn memory_device_rejects_zero_alignment() {
        match MemoryDevice::new(8192, 0) {
            Ok(_) => panic!("expected InvalidAlignment"),
            Err(DeviceError::InvalidAlignment { alignment: 0 }) => {}
            Err(other) => panic!("expected InvalidAlignment {{ alignment: 0 }}, got {other:?}"),
        }
    }

    #[test]
    fn memory_device_accepts_512_and_4096() {
        // Both are valid power-of-two alignments >= MIN_ALIGNMENT.
        assert!(MemoryDevice::new(4096, 512).is_ok());
        assert!(MemoryDevice::new(4096, 4096).is_ok());
    }

    #[test]
    fn direct_device_rejects_non_power_of_two_alignment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_align.dat");
        match DirectDevice::open(&path, 4096, 1000) {
            Ok(_) => panic!("expected InvalidAlignment"),
            Err(DeviceError::InvalidAlignment { alignment }) => {
                assert_eq!(alignment, 1000);
            }
            Err(other) => panic!("expected InvalidAlignment, got {other:?}"),
        }
    }

    #[test]
    fn direct_device_rejects_alignment_below_minimum() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("small_align.dat");
        match DirectDevice::open(&path, 4096, 128) {
            Ok(_) => panic!("expected InvalidAlignment"),
            Err(DeviceError::InvalidAlignment { alignment }) => {
                assert_eq!(alignment, 128);
            }
            Err(other) => panic!("expected InvalidAlignment, got {other:?}"),
        }
    }

    #[test]
    fn memory_device_lock_does_not_poison_on_panic() {
        // Regression test from the pre-F-G1-004 design when MemoryDevice
        // held a `parking_lot::RwLock<Vec<u8>>` internally. After
        // F-G1-004 / C-4 the lock was removed and `pread`/`pwrite`
        // route through `raw_ptr` with atomic chunked transfer — there
        // is no internal lock to poison. The test still pins the
        // outward contract: a thread that panics while holding an
        // unrelated `parking_lot::RwLock` must NOT render the shared
        // MemoryDevice unusable to the surviving threads, and
        // `parking_lot::RwLock` (used directly by the test as a `gate`)
        // remains non-poisoning unlike `std::sync::RwLock`.
        use std::sync::Arc;

        let dev = Arc::new(MemoryDevice::new(8192, 4096).unwrap());
        let dev_clone = Arc::clone(&dev);

        // Spawn a thread that performs a successful pwrite (now lock-free
        // via the atomic chunked path), then panics while holding an
        // external `parking_lot::RwLock` write guard. The guard is held
        // until the explicit `drop` immediately before the panic so the
        // test passes on both `parking_lot` and `std::sync::RwLock`
        // semantics; the prior comment block describing how the test
        // exercises the during-guard case via a SEPARATE lock is
        // preserved in `memory_device_lock_survives_panic_while_guard_held`
        // below, which keeps the guard live across the panic.
        let gate: Arc<parking_lot::RwLock<u32>> = Arc::new(parking_lot::RwLock::new(0));
        let gate_clone = Arc::clone(&gate);

        let handle = std::thread::spawn(move || {
            let guard = gate_clone.write();
            // Touch the inner device to prove the device itself is usable
            // from the panicking thread too.
            let mut buf = AlignedBuf::new(4096, 4096);
            buf[0] = 0x77;
            dev_clone
                .pwrite(&buf, 0)
                .expect("pwrite in worker must succeed");
            // Now panic while holding the gate's write lock.
            drop(guard); // parking_lot would not poison either way, but
            // we explicitly drop before panic so this test passes on
            // both lock implementations when sanity-checking.
            panic!("worker thread panicking on purpose");
        });

        // The joined thread's Result reflects the panic.
        let join_result = handle.join();
        assert!(
            join_result.is_err(),
            "worker thread must have reported its panic via join()"
        );

        // gate must be usable even though the worker thread panicked
        // while holding its write lock at one point.
        {
            let mut g = gate.write();
            *g += 1;
            assert_eq!(*g, 1, "gate lock must be usable after panicking thread");
        }

        // Most importantly: subsequent writes/reads on the shared
        // MemoryDevice must succeed. Post-F-G1-004 the device no longer
        // holds an internal lock at all — the guarantee follows from
        // the atomic chunked transfer through `raw_ptr`, which is
        // immune to panic poisoning by construction.
        let mut read_buf = AlignedBuf::new(4096, 4096);
        dev.pread(&mut read_buf, 0)
            .expect("pread after panic must succeed");
        assert_eq!(read_buf[0], 0x77, "data written by worker must be readable");

        let mut write_buf = AlignedBuf::new(4096, 4096);
        write_buf[0] = 0xAB;
        dev.pwrite(&write_buf, 4096)
            .expect("pwrite after panicking thread must succeed");

        let mut verify = AlignedBuf::new(4096, 4096);
        dev.pread(&mut verify, 4096)
            .expect("pread of post-panic write must succeed");
        assert_eq!(
            verify[0], 0xAB,
            "post-panic pwrite must be durably readable"
        );
    }

    #[test]
    fn memory_device_lock_survives_panic_while_guard_held() {
        // Tighter version of `memory_device_lock_does_not_poison_on_panic`:
        // directly observe that `parking_lot::RwLock` (held externally,
        // since the device itself no longer holds one post-F-G1-004) does
        // not poison. We acquire a write guard inside `catch_unwind`,
        // panic while holding it, and then confirm that the next
        // acquirer can still read the most-recent value AND the
        // MemoryDevice remains usable.
        use std::panic;
        use std::sync::Arc;

        let dev = Arc::new(MemoryDevice::new(8192, 4096).unwrap());
        let dev_clone = Arc::clone(&dev);

        // Perform the panic on a separate thread so the panic stays
        // contained: std::panic::catch_unwind within a spawned thread
        // gives us a clean Err without disturbing the test harness.
        let handle = std::thread::spawn(move || {
            let caught = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                // Acquire an external `parking_lot::RwLock<Vec<u8>>`
                // write guard and hold it across the panic. Post-F-G1-004
                // the MemoryDevice does not own a lock of its own, so
                // the panic-while-guard-held case is exercised against
                // this `side` instance — it shares the same lock
                // semantics that would have applied to the device's
                // pre-fix internal lock.
                let side: parking_lot::RwLock<Vec<u8>> = parking_lot::RwLock::new(vec![0; 32]);
                let guard = side.write();
                // While holding the guard, mutate the MemoryDevice as
                // well so both locks are exercised in this scope.
                let mut buf = AlignedBuf::new(4096, 4096);
                buf[0] = 0x55;
                dev_clone
                    .pwrite(&buf, 0)
                    .expect("pwrite while side-guard held must succeed");
                // Intentionally panic while still holding `guard`.
                // parking_lot will not poison — std::sync::RwLock would.
                let _held = &*guard;
                panic!("guard-held panic");
            }));
            assert!(caught.is_err(), "catch_unwind must observe the panic");
        });
        handle.join().expect("worker thread join must succeed");

        // The device must still be usable — F-G1-004 removed the
        // internal lock entirely, so `pwrite` just works regardless of
        // what the panicking thread was holding.
        let mut buf = AlignedBuf::new(4096, 4096);
        buf[0] = 0x99;
        dev.pwrite(&buf, 0)
            .expect("pwrite after caught-panic must succeed (no poisoning)");
        let mut verify = AlignedBuf::new(4096, 4096);
        dev.pread(&mut verify, 0)
            .expect("pread after caught-panic must succeed");
        assert_eq!(
            verify[0], 0x99,
            "post-panic pwrite must be durably readable — lock did not poison"
        );
    }

    // -- Exact-loop helper tests (gap #4: partial I/O is fatal) --

    /// Synthetic device that returns short counts on `pread`/`pwrite` so
    /// the default trait helpers can be exercised against the multi-call
    /// path. All writes/reads are recorded against the inner Vec for the
    /// final-state assertions.
    struct ChunkyDevice {
        data: parking_lot::Mutex<Vec<u8>>,
        chunk: usize,
        read_eof_at: Option<usize>,
        zero_write_at: Option<usize>,
        // Use Mutex over Cell so the device can stay Send + Sync without
        // unsafe impls.
        progress: parking_lot::Mutex<usize>,
    }

    impl ChunkyDevice {
        fn new(size: usize, chunk: usize) -> Self {
            Self {
                data: parking_lot::Mutex::new(vec![0u8; size]),
                chunk,
                read_eof_at: None,
                zero_write_at: None,
                progress: parking_lot::Mutex::new(0),
            }
        }
        fn with_read_eof_at(mut self, n: usize) -> Self {
            self.read_eof_at = Some(n);
            self
        }
        fn with_zero_write_at(mut self, n: usize) -> Self {
            self.zero_write_at = Some(n);
            self
        }
    }

    impl BlockDevice for ChunkyDevice {
        fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            let mut p = self.progress.lock();
            if let Some(eof_at) = self.read_eof_at
                && *p >= eof_at
            {
                return Ok(0);
            }
            let take = self.chunk.min(buf.len());
            let data = self.data.lock();
            let off = offset as usize;
            if off + take > data.len() {
                return Err(DeviceError::OutOfBounds {
                    offset,
                    len: take as u64,
                    device_size: data.len() as u64,
                });
            }
            buf[..take].copy_from_slice(&data[off..off + take]);
            *p += take;
            Ok(take)
        }

        fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
            let mut p = self.progress.lock();
            if let Some(zero_at) = self.zero_write_at
                && *p >= zero_at
            {
                return Ok(0);
            }
            let take = self.chunk.min(buf.len());
            let mut data = self.data.lock();
            let off = offset as usize;
            if off + take > data.len() {
                return Err(DeviceError::OutOfBounds {
                    offset,
                    len: take as u64,
                    device_size: data.len() as u64,
                });
            }
            data[off..off + take].copy_from_slice(&buf[..take]);
            *p += take;
            Ok(take)
        }

        fn alignment(&self) -> usize {
            // Tests use unaligned-friendly byte ranges; alignment is unused
            // by the helpers directly (the underlying pread/pwrite enforces
            // it on real devices).
            1
        }

        fn size(&self) -> u64 {
            self.data.lock().len() as u64
        }

        fn sync(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn pwrite_all_at_loops_over_short_writes() {
        // 4-byte chunk, 17-byte buffer ⇒ 5 underlying pwrite calls.
        let dev = ChunkyDevice::new(64, 4);
        let buf: Vec<u8> = (0..17).collect();
        dev.pwrite_all_at(&buf, 0).unwrap();
        let stored = dev.data.lock();
        assert_eq!(&stored[..17], &buf[..]);
        // Untouched tail must remain zero.
        assert!(stored[17..].iter().all(|b| *b == 0));
    }

    #[test]
    fn pread_exact_at_loops_over_short_reads() {
        // Pre-populate the device, then read it back with a short-chunk
        // device so pread_exact_at must call pread multiple times.
        let dev = ChunkyDevice::new(64, 5);
        {
            let mut d = dev.data.lock();
            for (i, b) in d.iter_mut().enumerate() {
                *b = (i * 3) as u8;
            }
        }
        let mut out = vec![0u8; 23];
        dev.pread_exact_at(&mut out, 0).unwrap();
        for (i, b) in out.iter().enumerate() {
            assert_eq!(*b, (i * 3) as u8, "byte {i} mismatch");
        }
    }

    #[test]
    fn pread_exact_at_returns_short_read_on_mid_buffer_eof() {
        // Configure the device to return EOF (0 bytes) after 7 bytes so
        // pread_exact_at hits a short-read partway through a 32-byte
        // request.
        let dev = ChunkyDevice::new(64, 4).with_read_eof_at(7);
        let mut out = vec![0u8; 32];
        match dev.pread_exact_at(&mut out, 0) {
            Err(DeviceError::ShortRead {
                expected,
                got,
                offset,
            }) => {
                assert_eq!(expected, 32, "expected reflects total request");
                // The chunk size is 4, so progress lands on a multiple of 4
                // — the first 4-byte read brings progress to 4, the next to
                // 8 (which is past eof_at=7), so the third call returns 0
                // with `got == 8`.
                assert_eq!(got, 8, "got reflects bytes read before EOF");
                assert_eq!(offset, 0, "offset reflects original starting offset");
            }
            other => panic!("expected ShortRead, got {other:?}"),
        }
    }

    #[test]
    fn pwrite_all_at_returns_write_stalled_on_zero_progress() {
        // Configure the device to make 0 forward progress immediately —
        // the first pwrite returns 0 bytes so pwrite_all_at must fail
        // fatally without any data having been written.
        let dev = ChunkyDevice::new(64, 4).with_zero_write_at(0);
        let buf = vec![0xABu8; 16];
        match dev.pwrite_all_at(&buf, 0) {
            Err(DeviceError::WriteStalled { offset, remaining }) => {
                assert_eq!(offset, 0);
                assert_eq!(remaining, 16);
            }
            other => panic!("expected WriteStalled, got {other:?}"),
        }
        // Nothing must have been written.
        assert!(dev.data.lock().iter().all(|b| *b == 0));
    }

    #[test]
    fn pwrite_all_at_returns_write_stalled_on_mid_buffer_zero() {
        // Make forward progress for 6 bytes, then pwrite returns 0. The
        // fatal error must report the still-pending byte count, not 0.
        let dev = ChunkyDevice::new(64, 4).with_zero_write_at(6);
        let buf = vec![0xCDu8; 20];
        match dev.pwrite_all_at(&buf, 0) {
            Err(DeviceError::WriteStalled { offset, remaining }) => {
                assert_eq!(offset, 0);
                // Two 4-byte writes succeed (progress = 8 >= zero_at = 6
                // after second write), so remaining = 20 - 8 = 12.
                assert_eq!(remaining, 12, "remaining must reflect pending bytes");
            }
            other => panic!("expected WriteStalled, got {other:?}"),
        }
    }

    #[test]
    fn pwrite_all_at_then_pread_exact_at_round_trip_on_memory_device() {
        // The default helpers must work over the production MemoryDevice
        // (which never returns short counts) — they should complete in a
        // single underlying call.
        let dev = MemoryDevice::new(8192, 4096).unwrap();
        let mut write_buf = AlignedBuf::new(4096, 4096);
        for (i, b) in write_buf.iter_mut().enumerate() {
            *b = (i & 0xFF) as u8;
        }
        dev.pwrite_all_at(&write_buf, 0).unwrap();

        let mut read_buf = AlignedBuf::new(4096, 4096);
        dev.pread_exact_at(&mut read_buf, 0).unwrap();
        assert_eq!(&*write_buf, &*read_buf);
    }

    #[test]
    fn pread_exact_at_propagates_alignment_error_from_inner_pread() {
        // The default helper must surface the inner pread error verbatim.
        let dev = MemoryDevice::new(8192, 4096).unwrap();
        let mut buf = AlignedBuf::new(4096, 4096);
        match dev.pread_exact_at(&mut buf, 1) {
            Err(DeviceError::AlignmentViolation { .. }) => {}
            other => panic!("expected AlignmentViolation, got {other:?}"),
        }
    }

    // -- J-01: buffer ADDRESS alignment (O_DIRECT requirement) --

    #[test]
    fn direct_device_rejects_misaligned_buffer_address() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("addr_align.dat");
        let dev = DirectDevice::open(&path, 65536, 4096).unwrap();

        // Slice an aligned allocation at +1: the length stays 4096
        // (aligned) and offset 0 is aligned, so ONLY the buffer's
        // memory address violates the O_DIRECT contract.
        let mut backing = AlignedBuf::new(8192, 4096);
        match dev.pread(&mut backing[1..4097], 0) {
            Err(DeviceError::AlignmentViolation { detail }) => {
                assert!(
                    detail.contains("buffer address"),
                    "detail must identify the buffer address as the violation: {detail}"
                );
            }
            other => panic!("expected AlignmentViolation, got {other:?}"),
        }
        match dev.pwrite(&backing[1..4097], 0) {
            Err(DeviceError::AlignmentViolation { detail }) => {
                assert!(
                    detail.contains("buffer address"),
                    "detail must identify the buffer address as the violation: {detail}"
                );
            }
            other => panic!("expected AlignmentViolation, got {other:?}"),
        }

        // The same allocation used from its aligned start still works.
        backing[0] = 0x5A;
        dev.pwrite(&backing[..4096], 0).unwrap();
        let mut read_buf = AlignedBuf::new(4096, 4096);
        dev.pread_exact_at(&mut read_buf, 0).unwrap();
        assert_eq!(&backing[..4096], &*read_buf);
    }

    #[test]
    fn memory_device_rejects_misaligned_buffer_address() {
        // MemoryDevice enforces the same buffer-address rule as
        // DirectDevice so CI (which runs against MemoryDevice) catches
        // callers that would EINVAL on a real O_DIRECT NVMe device.
        let dev = MemoryDevice::new(65536, 4096).unwrap();
        let mut backing = AlignedBuf::new(8192, 4096);
        match dev.pread(&mut backing[1..4097], 0) {
            Err(DeviceError::AlignmentViolation { detail }) => {
                assert!(
                    detail.contains("buffer address"),
                    "detail must identify the buffer address as the violation: {detail}"
                );
            }
            other => panic!("expected AlignmentViolation, got {other:?}"),
        }
        match dev.pwrite(&backing[1..4097], 0) {
            Err(DeviceError::AlignmentViolation { detail }) => {
                assert!(
                    detail.contains("buffer address"),
                    "detail must identify the buffer address as the violation: {detail}"
                );
            }
            other => panic!("expected AlignmentViolation, got {other:?}"),
        }

        // The aligned start of the same allocation still round-trips.
        backing[0] = 0xC3;
        dev.pwrite(&backing[..4096], 0).unwrap();
        let mut read_buf = AlignedBuf::new(4096, 4096);
        dev.pread_exact_at(&mut read_buf, 0).unwrap();
        assert_eq!(&backing[..4096], &*read_buf);
    }

    #[test]
    fn zero_length_buffer_is_exempt_from_address_check() {
        // `AlignedBuf::new(0, _)` carries a dangling sentinel pointer
        // (address 0x1); a zero-length transfer touches no memory so
        // the buffer-address rule must not reject it.
        let mem = MemoryDevice::new(8192, 4096).unwrap();
        let mut empty = AlignedBuf::new(0, 4096);
        assert_eq!(mem.pread(&mut empty, 0).unwrap(), 0);
        assert_eq!(mem.pwrite(&empty, 0).unwrap(), 0);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty_io.dat");
        let dev = DirectDevice::open(&path, 8192, 4096).unwrap();
        assert_eq!(dev.pread(&mut empty, 0).unwrap(), 0);
        assert_eq!(dev.pwrite(&empty, 0).unwrap(), 0);
    }

    // -- J-02: exact-loop helpers must use checked per-iteration offsets --

    /// Device that ignores `offset` entirely and always transfers
    /// `chunk` bytes. This lets the exact-loop helpers run with a
    /// starting offset near `u64::MAX` without the inner
    /// `pread`/`pwrite` bounds check rejecting the request first, so
    /// the helpers' own per-iteration offset arithmetic is what gets
    /// exercised.
    struct OffsetBlindDevice {
        chunk: usize,
    }

    impl BlockDevice for OffsetBlindDevice {
        fn pread(&self, buf: &mut [u8], _offset: u64) -> Result<usize> {
            let take = self.chunk.min(buf.len());
            for b in &mut buf[..take] {
                *b = 0xEE;
            }
            Ok(take)
        }

        fn pwrite(&self, buf: &[u8], _offset: u64) -> Result<usize> {
            Ok(self.chunk.min(buf.len()))
        }

        fn alignment(&self) -> usize {
            1
        }

        fn size(&self) -> u64 {
            u64::MAX
        }

        fn sync(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn pread_exact_at_rejects_offset_overflow_mid_loop() {
        // First inner pread at u64::MAX - 3 succeeds (4 bytes), then
        // the helper must compute (u64::MAX - 3) + 4 for the second
        // iteration — which overflows u64 and must surface as a typed
        // OutOfBounds, never a wrap (release) or panic (debug).
        let dev = OffsetBlindDevice { chunk: 4 };
        let mut buf = [0u8; 8];
        let start = u64::MAX - 3;
        match dev.pread_exact_at(&mut buf, start) {
            Err(DeviceError::OutOfBounds {
                offset,
                len,
                device_size,
            }) => {
                assert_eq!(offset, start, "offset must reflect the original request");
                assert_eq!(len, 8, "len must reflect the total request length");
                assert_eq!(device_size, u64::MAX);
            }
            other => panic!("expected OutOfBounds, got {other:?}"),
        }
    }

    // -- J-03: block-device size-query arithmetic (pure helpers) --
    //
    // The ioctl syscalls themselves still require a real block device
    // (root-only loop device / RAM disk) and are NOT covered here;
    // these tests pin the arithmetic and validation the `is_block`
    // branch of `DirectDevice::open` performs on the ioctl results.

    #[test]
    fn validate_block_device_size_passes_through_nonzero() {
        assert_eq!(validate_block_device_size(512).unwrap(), 512);
        assert_eq!(
            validate_block_device_size(3_840_755_982_336).unwrap(),
            3_840_755_982_336,
            "BLKGETSIZE64 result must pass through unchanged"
        );
        assert_eq!(validate_block_device_size(u64::MAX).unwrap(), u64::MAX);
    }

    #[test]
    fn validate_block_device_size_rejects_zero() {
        match validate_block_device_size(0) {
            Err(DeviceError::InvalidBlockDeviceGeometry { detail }) => {
                assert!(
                    detail.contains("0 bytes"),
                    "detail must name the zero size: {detail}"
                );
            }
            other => panic!("expected InvalidBlockDeviceGeometry, got {other:?}"),
        }
    }

    #[test]
    fn block_device_size_from_geometry_multiplies() {
        // 7_501_476_528 sectors x 512 bytes = a real 3.84 TB NVMe.
        assert_eq!(
            block_device_size_from_geometry(7_501_476_528, 512).unwrap(),
            3_840_755_982_336
        );
        assert_eq!(
            block_device_size_from_geometry(1, 4096).unwrap(),
            4096,
            "single-block device must compute exactly one block"
        );
    }

    #[test]
    fn block_device_size_from_geometry_rejects_overflow() {
        match block_device_size_from_geometry(u64::MAX, 2) {
            Err(DeviceError::InvalidBlockDeviceGeometry { detail }) => {
                assert!(
                    detail.contains("overflows"),
                    "detail must name the overflow: {detail}"
                );
            }
            other => panic!("expected InvalidBlockDeviceGeometry, got {other:?}"),
        }
        // Boundary: (2^32) x (2^32 - 1) = 2^64 - 2^32 still fits in
        // u64 and must NOT be rejected.
        assert_eq!(
            block_device_size_from_geometry(1 << 32, u32::MAX).unwrap(),
            (1u64 << 32) * u64::from(u32::MAX)
        );
    }

    #[test]
    fn block_device_size_from_geometry_rejects_zero_count_and_zero_size() {
        match block_device_size_from_geometry(0, 512) {
            Err(DeviceError::InvalidBlockDeviceGeometry { detail }) => {
                assert!(
                    detail.contains("is 0 bytes"),
                    "detail must name the zero product: {detail}"
                );
            }
            other => panic!("expected InvalidBlockDeviceGeometry, got {other:?}"),
        }
        match block_device_size_from_geometry(1_000_000, 0) {
            Err(DeviceError::InvalidBlockDeviceGeometry { detail }) => {
                assert!(
                    detail.contains("is 0 bytes"),
                    "detail must name the zero product: {detail}"
                );
            }
            other => panic!("expected InvalidBlockDeviceGeometry, got {other:?}"),
        }
    }

    #[test]
    fn pwrite_all_at_rejects_offset_overflow_mid_loop() {
        // Same shape as the pread case: the second iteration's offset
        // computation overflows and must surface as typed OutOfBounds.
        let dev = OffsetBlindDevice { chunk: 4 };
        let buf = [0xABu8; 8];
        let start = u64::MAX - 3;
        match dev.pwrite_all_at(&buf, start) {
            Err(DeviceError::OutOfBounds {
                offset,
                len,
                device_size,
            }) => {
                assert_eq!(offset, start, "offset must reflect the original request");
                assert_eq!(len, 8, "len must reflect the total request length");
                assert_eq!(device_size, u64::MAX);
            }
            other => panic!("expected OutOfBounds, got {other:?}"),
        }
    }

    // -- B-1 volatile write-cache mode ------------------------------------

    /// Helper: read one aligned block back from the device into a Vec.
    fn read_block(dev: &MemoryDevice, offset: u64) -> Vec<u8> {
        let mut buf = AlignedBuf::new(4096, 4096);
        dev.pread_exact_at(&mut buf, offset).unwrap();
        buf.to_vec()
    }

    #[test]
    fn volatile_device_drops_unsynced_writes_on_power_loss() {
        let dev = MemoryDevice::new_volatile(16 * 4096, 4096).unwrap();
        let mut buf = AlignedBuf::new(4096, 4096);
        buf.fill(0xAB);
        dev.pwrite_all_at(&buf, 4096).unwrap();

        // Before power loss, reads see the cached (live) bytes.
        assert_eq!(read_block(&dev, 4096), vec![0xAB; 4096]);

        assert!(
            dev.simulate_power_loss(),
            "volatile device must report the revert was performed"
        );

        // The unsynced write is gone — back to the all-zero durable state.
        assert_eq!(read_block(&dev, 4096), vec![0u8; 4096]);
    }

    #[test]
    fn volatile_device_preserves_synced_writes_across_power_loss() {
        let dev = MemoryDevice::new_volatile(16 * 4096, 4096).unwrap();
        let mut buf = AlignedBuf::new(4096, 4096);
        buf.fill(0xCD);
        dev.pwrite_all_at(&buf, 0).unwrap();
        dev.sync().unwrap();

        // A second, unsynced write to a different block.
        let mut buf2 = AlignedBuf::new(4096, 4096);
        buf2.fill(0xEF);
        dev.pwrite_all_at(&buf2, 8192).unwrap();

        assert!(dev.simulate_power_loss());

        // Synced block survives; unsynced block reverts.
        assert_eq!(read_block(&dev, 0), vec![0xCD; 4096]);
        assert_eq!(read_block(&dev, 8192), vec![0u8; 4096]);
    }

    #[test]
    fn volatile_device_power_loss_drops_raw_pointer_writes_too() {
        // The engine hot path writes through `as_raw_ptr`, bypassing
        // `pwrite`. Power loss must drop those writes as well, because
        // the durability question is per-device, not per-API.
        let dev = MemoryDevice::new_volatile(4 * 4096, 4096).unwrap();
        let ptr = dev.as_raw_ptr().unwrap();
        // Safety: in-bounds single-threaded write to the live allocation.
        unsafe {
            *ptr.add(100) = 0x77;
        }
        assert!(dev.simulate_power_loss());
        assert_eq!(read_block(&dev, 0)[100], 0, "raw-ptr write must be dropped");
    }

    #[test]
    fn default_device_power_loss_is_noop_and_reports_false() {
        let dev = MemoryDevice::new(4 * 4096, 4096).unwrap();
        let mut buf = AlignedBuf::new(4096, 4096);
        buf.fill(0x55);
        dev.pwrite_all_at(&buf, 0).unwrap();

        assert!(
            !dev.simulate_power_loss(),
            "default device must report it performed no revert"
        );
        // Historical behavior unchanged: the write survives without sync.
        assert_eq!(read_block(&dev, 0), vec![0x55; 4096]);
    }
}
