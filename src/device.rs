//! Block device abstraction for raw NVMe I/O and in-memory testing.
//!
//! Two backends:
//! - [`DirectDevice`]: Opens files with `O_DIRECT` for zero-copy I/O.
//! - [`MemoryDevice`]: In-memory `Vec<u8>` for testing with the same alignment rules.

use std::alloc::{self, Layout};
use std::ops::{Deref, DerefMut};
use thiserror::Error;

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
    /// Both `offset` and `buf.len()` must be multiples of [`alignment()`](Self::alignment).
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Write `buf` starting at `offset`.
    ///
    /// Both `offset` and `buf.len()` must be multiples of [`alignment()`](Self::alignment).
    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize>;

    /// Minimum I/O alignment for this device (512 or 4096 bytes).
    fn alignment(&self) -> usize;

    /// Total usable size in bytes.
    fn size(&self) -> u64;

    /// Sync all pending writes to stable storage.
    fn sync(&self) -> Result<()>;

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
                layout: Layout::from_size_align(0, alignment)
                    .expect("invalid alignment"),
            };
        }
        let layout =
            Layout::from_size_align(len, alignment).expect("invalid layout");
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

/// In-memory block device for testing and benchmarking.
///
/// Enforces the same alignment constraints as [`DirectDevice`] so tests
/// catch alignment bugs via `pread`/`pwrite`. Also exposes a stable raw
/// pointer via [`as_raw_ptr`](BlockDevice::as_raw_ptr) for zero-copy
/// access on the Engine hot path.
pub struct MemoryDevice {
    data: std::sync::RwLock<Vec<u8>>,
    /// Stable pointer into the Vec's heap allocation. Valid for the lifetime
    /// of this device because the Vec is never resized after construction.
    raw_ptr: *mut u8,
    raw_len: usize,
    alignment: usize,
}

// Safety: MemoryDevice owns its allocation exclusively. The raw_ptr points
// into the Vec's heap buffer which is stable (never resized). Concurrent
// access through raw_ptr is the caller's responsibility (Engine stripe locks).
unsafe impl Send for MemoryDevice {}
unsafe impl Sync for MemoryDevice {}

impl MemoryDevice {
    /// Create a new in-memory device of `size` bytes with the given alignment.
    ///
    /// # Errors
    ///
    /// Returns [`DeviceError::ZeroSize`] if `size` is zero.
    pub fn new(size: u64, alignment: usize) -> Result<Self> {
        if size == 0 {
            return Err(DeviceError::ZeroSize);
        }
        let mut data = vec![0u8; size as usize];
        let raw_ptr = data.as_mut_ptr();
        let raw_len = data.len();
        Ok(Self {
            data: std::sync::RwLock::new(data),
            raw_ptr,
            raw_len,
            alignment,
        })
    }
}

impl MemoryDevice {
    fn check_alignment(&self, offset: u64, len: usize) -> Result<()> {
        if !(offset as usize).is_multiple_of(self.alignment) {
            return Err(DeviceError::AlignmentViolation {
                detail: format!(
                    "offset {offset} not aligned to {}",
                    self.alignment
                ),
            });
        }
        if !len.is_multiple_of(self.alignment) {
            return Err(DeviceError::AlignmentViolation {
                detail: format!(
                    "buffer length {len} not aligned to {}",
                    self.alignment
                ),
            });
        }
        Ok(())
    }
}

impl BlockDevice for MemoryDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.check_alignment(offset, buf.len())?;
        let data = self.data.read().unwrap();
        let off = offset as usize;
        if off + buf.len() > data.len() {
            return Err(DeviceError::OutOfBounds {
                offset,
                len: buf.len() as u64,
                device_size: data.len() as u64,
            });
        }
        buf.copy_from_slice(&data[off..off + buf.len()]);
        Ok(buf.len())
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.check_alignment(offset, buf.len())?;
        let mut data = self.data.write().unwrap();
        let off = offset as usize;
        if off + buf.len() > data.len() {
            return Err(DeviceError::OutOfBounds {
                offset,
                len: buf.len() as u64,
                device_size: data.len() as u64,
            });
        }
        data[off..off + buf.len()].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn alignment(&self) -> usize {
        self.alignment
    }

    fn size(&self) -> u64 {
        self.raw_len as u64
    }

    fn sync(&self) -> Result<()> {
        Ok(())
    }

    fn as_raw_ptr(&self) -> Option<*mut u8> {
        Some(self.raw_ptr)
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
    pub fn open(
        path: &std::path::Path,
        size: u64,
        alignment: usize,
    ) -> Result<Self> {
        use std::fs::OpenOptions;

        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true).truncate(false);

        // On Linux, open with O_DIRECT for zero-copy NVMe I/O.
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(libc::O_DIRECT);
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
                // BLKGETSIZE64 = 0x80081272 — returns byte count as u64.
                // Safety: fd is a valid open block device; dev_size is a
                // properly-sized output variable for this ioctl.
                let rc = unsafe {
                    libc::ioctl(fd, 0x8008_1272 as libc::c_ulong, &mut dev_size)
                };
                if rc != 0 {
                    return Err(DeviceError::Io(std::io::Error::last_os_error()));
                }
                dev_size
            }
            #[cfg(target_os = "macos")]
            {
                use std::os::unix::io::AsRawFd;
                let fd = file.as_raw_fd();
                // DKIOCGETBLOCKCOUNT = 0x40086419  (returns u64 block count)
                // DKIOCGETBLOCKSIZE  = 0x40046418  (returns u32 block size)
                let mut block_count: u64 = 0;
                let mut block_size: u32 = 0;
                // Safety: fd is a valid open block device; the output
                // variables are correctly sized for the respective ioctls.
                let rc = unsafe {
                    libc::ioctl(fd, 0x4008_6419 as libc::c_ulong, &mut block_count)
                };
                if rc != 0 {
                    return Err(DeviceError::Io(std::io::Error::last_os_error()));
                }
                let rc = unsafe {
                    libc::ioctl(fd, 0x4004_6418 as libc::c_ulong, &mut block_size)
                };
                if rc != 0 {
                    return Err(DeviceError::Io(std::io::Error::last_os_error()));
                }
                block_count * u64::from(block_size)
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
                size
            } else {
                existing
            }
        };

        // On macOS, disable caching to approximate O_DIRECT behavior.
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::io::AsRawFd;
            // F_NOCACHE = 48 on macOS
            unsafe {
                libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
            }
        }

        Ok(Self {
            file,
            size: actual_size,
            alignment,
            is_block,
        })
    }

    fn check_alignment(&self, offset: u64, len: usize) -> Result<()> {
        if !(offset as usize).is_multiple_of(self.alignment) {
            return Err(DeviceError::AlignmentViolation {
                detail: format!(
                    "offset {offset} not aligned to {}",
                    self.alignment
                ),
            });
        }
        if !len.is_multiple_of(self.alignment) {
            return Err(DeviceError::AlignmentViolation {
                detail: format!(
                    "buffer length {len} not aligned to {}",
                    self.alignment
                ),
            });
        }
        Ok(())
    }
}

impl BlockDevice for DirectDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.check_alignment(offset, buf.len())?;
        if offset + buf.len() as u64 > self.size {
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
                return Err(DeviceError::Io(std::io::Error::last_os_error()));
            }
            Ok(n as usize)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(buf)?;
            Ok(buf.len())
        }
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.check_alignment(offset, buf.len())?;
        if offset + buf.len() as u64 > self.size {
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
            // Safety: fd is valid, buf is valid for buf.len() bytes.
            let n = unsafe {
                libc::pwrite(
                    fd,
                    buf.as_ptr().cast(),
                    buf.len(),
                    offset as libc::off_t,
                )
            };
            if n < 0 {
                return Err(DeviceError::Io(std::io::Error::last_os_error()));
            }
            Ok(n as usize)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(buf)?;
            Ok(buf.len())
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
}
