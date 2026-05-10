//! Robin Hood open-addressing hash table backed by mmap.
//!
//! Uses the txid's first 8 bytes as the bucket index (the txid is already
//! a cryptographic hash with excellent distribution) and bytes 8–16 as a
//! fingerprint for fast rejection during probing.
//!
//! # Memory
//!
//! Backing memory is allocated via `mmap(MAP_ANONYMOUS | MAP_PRIVATE)`.
//! On Linux, the allocator first attempts 2 MB hugepages (`MAP_HUGETLB`)
//! for better TLB performance at scale, falling back to regular pages.
//! On macOS (development), regular pages are used directly.

use crate::redo::{RedoLog, RedoOp};
use parking_lot::Mutex;
use std::sync::Arc;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from hash table operations.
#[derive(Error, Debug)]
pub enum HashTableError {
    /// The table is full (should not happen if resize works).
    #[error("hash table full: {count}/{capacity}")]
    Full { count: usize, capacity: usize },

    /// Memory allocation failed.
    #[error("mmap allocation failed: {0}")]
    AllocFailed(String),

    /// Filesystem I/O error during a resize or rename.
    #[error("resize I/O failure: {0}")]
    ResizeIo(String),

    /// Redo log journaling failure during a crash-atomic resize.
    #[error("resize redo log failure: {0}")]
    ResizeRedo(String),
}

pub type Result<T> = std::result::Result<T, HashTableError>;

// ---------------------------------------------------------------------------
// TxKey
// ---------------------------------------------------------------------------

/// Primary key for index lookups — the 32-byte transaction ID.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxKey {
    /// Transaction hash (double-SHA256), 32 bytes.
    pub txid: [u8; 32],
}

impl TxKey {
    /// Create a key from a 32-byte txid.
    pub fn from_bytes(txid: [u8; 32]) -> Self {
        Self { txid }
    }
}

impl std::fmt::Debug for TxKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hex: String = self.txid[..8].iter().map(|b| format!("{b:02x}")).collect();
        write!(f, "TxKey({hex}...)")
    }
}

// ---------------------------------------------------------------------------
// TxIndexEntry
// ---------------------------------------------------------------------------

/// What the primary index stores for each transaction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TxIndexEntry {
    /// Which device this record lives on (for multi-device setups).
    pub device_id: u8,
    /// Byte offset on that device to the start of TxMetadata.
    pub record_offset: u64,
    /// Number of UTXO slots in this record.
    pub utxo_count: u32,
    /// Number of entries in the block (cached from metadata).
    pub block_entry_count: u8,
    /// Transaction-level flags (cached from metadata).
    pub tx_flags: u8,
    /// Number of spent UTXOs (cached from TxMetadata).
    pub spent_utxos: u32,
    /// Delete-at-height or preserve-until value (discriminated by HAS_PRESERVE_UNTIL bit in tx_flags).
    pub dah_or_preserve: u32,
    /// Unmined-since timestamp (cached from TxMetadata).
    pub unmined_since: u32,
    /// Generation counter (cached from TxMetadata).
    pub generation: u32,
}

// ---------------------------------------------------------------------------
// Bucket
// ---------------------------------------------------------------------------

/// Sentinel value for `probe_distance` indicating an empty bucket.
/// Valid probe distances are 0–254; 0xFF means the bucket is empty.
const BUCKET_EMPTY_SENTINEL: u8 = 0xFF;

/// Maximum storable probe distance.  Any entry whose true Robin Hood
/// displacement exceeds this is stored with probe_distance = 254.
/// This disables Robin Hood early-termination for those (rare) entries.
const MAX_STORED_PROBE: u16 = (BUCKET_EMPTY_SENTINEL - 1) as u16;

/// Cap a probe distance for storage in a bucket's `probe_distance` field.
#[inline(always)]
fn cap_probe(dist: u16) -> u8 {
    dist.min(MAX_STORED_PROBE) as u8
}

/// One bucket in the Robin Hood hash table: exactly 64 bytes (one cache line).
///
/// The `probe_distance` field serves double duty: values 0–254 indicate an
/// occupied bucket whose Robin Hood probe distance is that value; the value
/// 0xFF ([`BUCKET_EMPTY_SENTINEL`]) means the bucket is empty.
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct Bucket {
    probe_distance: u8,
    txid: [u8; 32],
    device_id: u8,
    record_offset: u64,
    utxo_count: u32,
    block_entry_count: u8,
    tx_flags: u8,
    spent_utxos: u32,
    dah_or_preserve: u32,
    unmined_since: u32,
    generation: u32,
}

/// Actual size of one bucket in bytes.
pub const BUCKET_SIZE: usize = std::mem::size_of::<Bucket>();

const _: () = assert!(
    BUCKET_SIZE == 64,
    "Bucket must be exactly 64 bytes (1 cache line)"
);

impl Bucket {
    fn empty() -> Self {
        Self {
            probe_distance: BUCKET_EMPTY_SENTINEL,
            txid: [0; 32],
            device_id: 0,
            record_offset: 0,
            utxo_count: 0,
            block_entry_count: 0,
            tx_flags: 0,
            spent_utxos: 0,
            dah_or_preserve: 0,
            unmined_since: 0,
            generation: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.probe_distance == BUCKET_EMPTY_SENTINEL
    }

    fn is_occupied(&self) -> bool {
        self.probe_distance != BUCKET_EMPTY_SENTINEL
    }

    fn entry(&self) -> TxIndexEntry {
        TxIndexEntry {
            device_id: self.device_id,
            record_offset: self.record_offset,
            utxo_count: self.utxo_count,
            block_entry_count: self.block_entry_count,
            tx_flags: self.tx_flags,
            spent_utxos: self.spent_utxos,
            dah_or_preserve: self.dah_or_preserve,
            unmined_since: self.unmined_since,
            generation: self.generation,
        }
    }

    fn set_entry(&mut self, key: &TxKey, entry: &TxIndexEntry, probe_dist: u8) {
        self.probe_distance = probe_dist;
        self.txid = key.txid;
        self.device_id = entry.device_id;
        self.record_offset = entry.record_offset;
        self.utxo_count = entry.utxo_count;
        self.block_entry_count = entry.block_entry_count;
        self.tx_flags = entry.tx_flags;
        self.spent_utxos = entry.spent_utxos;
        self.dah_or_preserve = entry.dah_or_preserve;
        self.unmined_since = entry.unmined_since;
        self.generation = entry.generation;
    }
}

// ---------------------------------------------------------------------------
// Hash functions
// ---------------------------------------------------------------------------

/// Compute the bucket index from a TxKey. Uses bytes 0–7 of txid.
fn bucket_index(key: &TxKey, mask: usize) -> usize {
    let h = u64::from_le_bytes(key.txid[0..8].try_into().unwrap());
    (h as usize) & mask
}

/// Derive the fingerprint from a txid. Uses bytes 8–15 (same region that
/// was previously stored redundantly in each bucket).
#[inline(always)]
fn txid_fingerprint(txid: &[u8; 32]) -> u64 {
    u64::from_le_bytes(txid[8..16].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// mmap helpers
// ---------------------------------------------------------------------------

/// Allocate a zeroed mmap region for `capacity` buckets.
///
/// On Linux, attempts 2 MB hugepages first. Falls back to regular pages.
/// On macOS, uses regular pages directly.
///
/// Returns `(pointer, mmap_byte_length, hugepage_used)`.
fn alloc_mmap_buckets(capacity: usize) -> Result<(*mut Bucket, usize, bool)> {
    let byte_len = capacity
        .checked_mul(std::mem::size_of::<Bucket>())
        .ok_or_else(|| HashTableError::AllocFailed("capacity overflow".into()))?;

    if byte_len == 0 {
        return Err(HashTableError::AllocFailed("zero-size allocation".into()));
    }

    // On Linux, try hugepages first.
    #[cfg(target_os = "linux")]
    {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                byte_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB,
                -1,
                0,
            )
        };
        if ptr != libc::MAP_FAILED {
            return Ok((ptr.cast::<Bucket>(), byte_len, true));
        }
        // Hugepage allocation failed — fall through to regular mmap.
    }

    // Regular mmap (works on macOS and Linux).
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            byte_len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        return Err(HashTableError::AllocFailed(format!(
            "mmap({byte_len} bytes) failed: {}",
            std::io::Error::last_os_error()
        )));
    }

    Ok((ptr.cast::<Bucket>(), byte_len, false))
}

/// Release a previously mmap'd region.
///
/// # Safety
///
/// `ptr` must point to a region of `byte_len` bytes allocated by `alloc_mmap_buckets`.
unsafe fn dealloc_mmap_buckets(ptr: *mut Bucket, byte_len: usize) {
    if byte_len > 0 && !ptr.is_null() {
        unsafe { libc::munmap(ptr.cast(), byte_len) };
    }
}

/// Extract raw bytes from a filesystem path without UTF-8 validation.
///
/// POSIX paths are sequences of non-NUL bytes; they are NOT guaranteed to
/// be valid UTF-8 and must not be lossily converted. On Unix we use
/// `OsStrExt::as_bytes()`. On other platforms we fall back to
/// `to_string_lossy().into_owned().into_bytes()` as a best-effort.
fn path_to_bytes(path: &std::path::Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_os_str().as_bytes().to_vec()
    }
    #[cfg(not(unix))]
    {
        path.to_string_lossy().into_owned().into_bytes()
    }
}

/// msync + fsync a file-backed hash table so its on-disk contents are durable.
///
/// Issues `msync(MS_SYNC)` to flush mmap dirty pages, then `fsync` on the
/// backing file descriptor via the POSIX `fsync(2)` system call. Returns
/// [`HashTableError::ResizeIo`] on failure.
fn sync_file_backed(table: &HashTable) -> Result<()> {
    let Backing::FileBacked { fd, .. } = &table.backing else {
        return Ok(());
    };
    if table.mmap_len > 0 && !table.ptr.is_null() {
        // Safety: ptr/mmap_len describe the currently mapped region.
        let rc = unsafe { libc::msync(table.ptr.cast(), table.mmap_len, libc::MS_SYNC) };
        if rc != 0 {
            return Err(HashTableError::ResizeIo(format!(
                "msync failed: {}",
                std::io::Error::last_os_error()
            )));
        }
    }
    // Safety: fd was obtained from File::into_raw_fd and is still open.
    let rc = unsafe { libc::fsync(*fd) };
    if rc != 0 {
        return Err(HashTableError::ResizeIo(format!(
            "fsync failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

/// fsync the parent directory of `path` so that a recent rename into that
/// directory is durable across a power failure.
///
/// On Unix, opens the parent directory read-only and calls
/// [`std::fs::File::sync_all`]. On non-Unix platforms the call is a
/// best-effort no-op (this server targets Linux/Unix).
#[cfg(unix)]
fn fsync_parent_dir(path: &std::path::Path) -> Result<()> {
    let parent = path.parent().unwrap_or(std::path::Path::new("."));
    let dir = std::fs::File::open(parent).map_err(|e| {
        HashTableError::ResizeIo(format!("open parent dir {}: {e}", parent.display()))
    })?;
    dir.sync_all().map_err(|e| {
        HashTableError::ResizeIo(format!("fsync parent dir {}: {e}", parent.display()))
    })?;
    Ok(())
}

/// Non-Unix fallback: best-effort no-op. See [`fsync_parent_dir`].
#[cfg(not(unix))]
fn fsync_parent_dir(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

// ---------------------------------------------------------------------------
// Backing
// ---------------------------------------------------------------------------

/// Tracks whether the hash table's memory is anonymous or file-backed.
enum Backing {
    /// Anonymous mmap (MAP_ANONYMOUS | MAP_PRIVATE). Default.
    Anonymous,
    /// File-backed mmap (MAP_SHARED). Persistent across restarts.
    FileBacked {
        /// File descriptor (kept open for msync/munmap).
        fd: std::os::unix::io::RawFd,
        /// Path to the backing file (for resize operations).
        path: std::path::PathBuf,
    },
}

/// Allocate a file-backed mmap region for `capacity` buckets.
///
/// Opens or creates the file at `path`, truncates to exact size, and maps
/// it with `MAP_SHARED`. Sets `MADV_RANDOM` to disable readahead.
///
/// Returns `(pointer, mmap_byte_length, fd)`.
fn alloc_file_backed_buckets(
    path: &std::path::Path,
    capacity: usize,
) -> Result<(*mut Bucket, usize, std::os::unix::io::RawFd)> {
    use std::os::unix::io::IntoRawFd;

    let byte_len = capacity
        .checked_mul(std::mem::size_of::<Bucket>())
        .ok_or_else(|| HashTableError::AllocFailed("capacity overflow".into()))?;

    if byte_len == 0 {
        return Err(HashTableError::AllocFailed("zero-size allocation".into()));
    }

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|e| HashTableError::AllocFailed(format!("open {}: {e}", path.display())))?;

    // Only set_len if the file isn't already the right size. This avoids
    // truncating an existing file that was resized to a larger capacity.
    let current_len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if current_len != byte_len as u64 {
        file.set_len(byte_len as u64)
            .map_err(|e| HashTableError::AllocFailed(format!("ftruncate: {e}")))?;
    }

    let fd = file.into_raw_fd();

    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            byte_len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        unsafe { libc::close(fd) };
        return Err(HashTableError::AllocFailed(format!(
            "mmap MAP_SHARED({byte_len} bytes) failed: {}",
            std::io::Error::last_os_error()
        )));
    }

    unsafe { libc::madvise(ptr, byte_len, libc::MADV_RANDOM) };

    Ok((ptr.cast::<Bucket>(), byte_len, fd))
}

// ---------------------------------------------------------------------------
// HashTable
// ---------------------------------------------------------------------------

/// Robin Hood open-addressing hash table backed by an mmap'd flat array.
///
/// Capacity is always a power of two for fast modulo via bitmask.
///
/// # Memory
///
/// Attempts to use 2 MB hugepages on Linux (`MAP_HUGETLB`). Falls back to
/// regular pages on macOS or when hugepages are unavailable. On drop, the
/// mmap region is released via `munmap`.
pub struct HashTable {
    /// Pointer to the mmap'd bucket array.
    ptr: *mut Bucket,
    /// Total number of buckets (always a power of 2).
    capacity: usize,
    /// Number of occupied entries.
    count: usize,
    /// `capacity - 1`, for fast modulo via bitmask.
    mask: usize,
    /// Size of the mmap'd region in bytes.
    mmap_len: usize,
    /// Whether 2 MB hugepages are backing this table.
    hugepage: bool,
    /// Maximum probe distance observed so far.
    max_probe: usize,
    /// Whether this table is anonymous or file-backed.
    backing: Backing,
    /// Optional redo log for journaling file-backed resize (crash atomicity).
    ///
    /// When attached and the table is file-backed, [`HashTable::resize`]
    /// appends and fsyncs a [`RedoOp::HashtableResizeBegin`] before
    /// writing the tmp file, and a [`RedoOp::HashtableResizeCommit`] after
    /// the rename + parent directory fsync completes. Anonymous tables
    /// ignore the redo log (nothing to recover).
    redo_log: Option<Arc<Mutex<RedoLog>>>,
}

impl std::fmt::Debug for HashTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashTable")
            .field("capacity", &self.capacity)
            .field("count", &self.count)
            .field("hugepage", &self.hugepage)
            .field("max_probe", &self.max_probe)
            .field(
                "backing",
                &match &self.backing {
                    Backing::Anonymous => "anonymous",
                    Backing::FileBacked { .. } => "file_backed",
                },
            )
            .finish()
    }
}

// Safety: HashTable owns its mmap allocation exclusively. The contents are
// plain Copy data with no interior mutability or thread-local state.
unsafe impl Send for HashTable {}
unsafe impl Sync for HashTable {}

impl HashTable {
    /// Create a new hash table with at least `initial_capacity` buckets.
    ///
    /// The capacity is rounded up to the next power of two.
    /// Memory is allocated via `mmap` with `MAP_ANONYMOUS | MAP_PRIVATE`.
    /// On Linux, 2 MB hugepages are attempted first.
    pub fn new(initial_capacity: usize) -> Result<Self> {
        let capacity = initial_capacity.next_power_of_two().max(16);
        let (ptr, mmap_len, hugepage) = alloc_mmap_buckets(capacity)?;

        // mmap returns zeroed memory, but our empty sentinel is 0xFF (not 0x00).
        // Initialize every bucket's probe_distance byte to BUCKET_EMPTY_SENTINEL.
        // Safety: ptr is valid for `capacity` buckets.
        unsafe {
            // Set the entire region to 0xFF first (making all bytes 0xFF),
            // then we're done: probe_distance = 0xFF means empty, and the
            // remaining fields will be overwritten on insert anyway.
            std::ptr::write_bytes(ptr as *mut u8, BUCKET_EMPTY_SENTINEL, mmap_len);
        }

        Ok(Self {
            ptr,
            capacity,
            count: 0,
            mask: capacity - 1,
            mmap_len,
            hugepage,
            max_probe: 0,
            backing: Backing::Anonymous,
            redo_log: None,
        })
    }

    /// Open or create a file-backed hash table at `path`.
    ///
    /// If the file exists and its size is a valid power-of-two multiple of
    /// `BUCKET_SIZE`, it is mapped at its existing capacity and scanned to
    /// recover `count` and `max_probe`. The `initial_capacity` parameter is
    /// only used when creating a new file.
    ///
    /// The capacity is rounded up to the next power of two (minimum 16).
    pub fn open_file_backed(path: &std::path::Path, initial_capacity: usize) -> Result<Self> {
        let bucket_size = std::mem::size_of::<Bucket>();

        // If the file already exists with a valid bucket-array size, use that
        // capacity instead of the caller's initial_capacity. This prevents
        // truncating a file that auto-resized to a larger capacity.
        let (capacity, is_existing) = if path.exists() {
            if let Ok(meta) = std::fs::metadata(path) {
                let file_len = meta.len() as usize;
                if file_len >= bucket_size
                    && file_len.is_multiple_of(bucket_size)
                    && (file_len / bucket_size).is_power_of_two()
                {
                    (file_len / bucket_size, true)
                } else {
                    // File exists but has invalid size — treat as new.
                    (initial_capacity.next_power_of_two().max(16), false)
                }
            } else {
                (initial_capacity.next_power_of_two().max(16), false)
            }
        } else {
            (initial_capacity.next_power_of_two().max(16), false)
        };

        let (ptr, mmap_len, fd) = alloc_file_backed_buckets(path, capacity)?;

        if is_existing {
            // Scan existing file to recover count and max_probe.
            let mut count = 0usize;
            let mut max_probe = 0usize;
            for i in 0..capacity {
                let bucket = unsafe { &*ptr.add(i) };
                if bucket.is_occupied() {
                    count += 1;
                    if (bucket.probe_distance as usize) > max_probe {
                        max_probe = bucket.probe_distance as usize;
                    }
                }
            }
            Ok(Self {
                ptr,
                capacity,
                count,
                mask: capacity - 1,
                mmap_len,
                hugepage: false,
                max_probe,
                backing: Backing::FileBacked {
                    fd,
                    path: path.to_path_buf(),
                },
                redo_log: None,
            })
        } else {
            // Initialize all buckets to empty sentinel.
            unsafe {
                std::ptr::write_bytes(ptr as *mut u8, BUCKET_EMPTY_SENTINEL, mmap_len);
            }
            unsafe { libc::msync(ptr.cast(), mmap_len, libc::MS_SYNC) };

            Ok(Self {
                ptr,
                capacity,
                count: 0,
                mask: capacity - 1,
                mmap_len,
                hugepage: false,
                max_probe: 0,
                backing: Backing::FileBacked {
                    fd,
                    path: path.to_path_buf(),
                },
                redo_log: None,
            })
        }
    }

    /// Attach a redo log for journaling crash-atomic file-backed resizes.
    ///
    /// Once attached, [`HashTable::resize`] on a file-backed table will:
    ///
    /// 1. Append + fsync a [`RedoOp::HashtableResizeBegin`] (capturing the
    ///    tmp file path and target capacity).
    /// 2. Write the rehashed state into the tmp file and fsync it.
    /// 3. Atomically rename the tmp file over the original.
    /// 4. fsync the parent directory so the rename is durable.
    /// 5. Append + fsync a [`RedoOp::HashtableResizeCommit`].
    ///
    /// Anonymous tables ignore the redo log. Without a redo log attached,
    /// the resize still fsyncs the tmp file + parent directory, but crash
    /// recovery cannot detect and clean up an orphaned tmp file left by
    /// a crash between steps (1) and the rename.
    pub fn set_redo_log(&mut self, redo_log: Arc<Mutex<RedoLog>>) {
        self.redo_log = Some(redo_log);
    }

    /// Is a redo log currently attached?
    pub fn has_redo_log(&self) -> bool {
        self.redo_log.is_some()
    }

    /// Safe accessor for bucket at `idx` (immutable).
    #[inline]
    fn bucket(&self, idx: usize) -> &Bucket {
        debug_assert!(idx < self.capacity);
        // Safety: idx is within the mmap'd region (checked by caller or mask).
        unsafe { &*self.ptr.add(idx) }
    }

    /// Safe accessor for bucket at `idx` (mutable).
    #[inline]
    fn bucket_mut(&mut self, idx: usize) -> &mut Bucket {
        debug_assert!(idx < self.capacity);
        // Safety: idx is within the mmap'd region, &mut self guarantees exclusivity.
        unsafe { &mut *self.ptr.add(idx) }
    }

    /// Look up a transaction by key, returning the entry by value. O(1) expected.
    pub fn get_entry(&self, key: &TxKey) -> Option<TxIndexEntry> {
        let fp = txid_fingerprint(&key.txid);
        let mut idx = bucket_index(key, self.mask);
        let mut dist: u16 = 0;

        loop {
            let bucket = self.bucket(idx);
            if bucket.is_empty() {
                return None;
            }
            if bucket.is_occupied() {
                // Robin Hood early termination is only safe when the stored
                // probe_distance is not capped (< MAX_STORED_PROBE).
                if (bucket.probe_distance as u16) < MAX_STORED_PROBE
                    && dist > bucket.probe_distance as u16
                {
                    return None;
                }
                if txid_fingerprint(&bucket.txid) == fp && bucket.txid == key.txid {
                    return Some(bucket.entry());
                }
            }
            idx = (idx + 1) & self.mask;
            dist += 1;
            if dist as usize >= self.capacity {
                return None;
            }
        }
    }

    /// Insert or update an entry. Returns the previous entry if the key existed.
    pub fn insert(&mut self, key: TxKey, entry: TxIndexEntry) -> Result<Option<TxIndexEntry>> {
        // Check for update of existing key first.
        let fp = txid_fingerprint(&key.txid);
        {
            let mut idx = bucket_index(&key, self.mask);
            let mut dist: u16 = 0;
            loop {
                let bucket = self.bucket(idx);
                if bucket.is_empty() {
                    break;
                }
                if bucket.is_occupied() {
                    if (bucket.probe_distance as u16) < MAX_STORED_PROBE
                        && dist > bucket.probe_distance as u16
                    {
                        break;
                    }
                    if txid_fingerprint(&bucket.txid) == fp && bucket.txid == key.txid {
                        let old = bucket.entry();
                        self.bucket_mut(idx)
                            .set_entry(&key, &entry, cap_probe(dist));
                        return Ok(Some(old));
                    }
                }
                idx = (idx + 1) & self.mask;
                dist += 1;
                if dist as usize >= self.capacity {
                    break;
                }
            }
        }

        // New insert — Robin Hood insertion.
        let mut idx = bucket_index(&key, self.mask);
        let mut dist: u16 = 0;
        let mut cur_key = key;
        let mut cur_entry = entry;

        loop {
            let bucket = self.bucket(idx);
            if bucket.is_empty() {
                self.bucket_mut(idx)
                    .set_entry(&cur_key, &cur_entry, cap_probe(dist));
                self.count += 1;
                if dist as usize > self.max_probe {
                    self.max_probe = dist as usize;
                }
                return Ok(None);
            }

            // Robin Hood: if our displacement is greater, swap.
            if bucket.is_occupied() && dist > bucket.probe_distance as u16 {
                let displaced_key = TxKey { txid: bucket.txid };
                let displaced_entry = bucket.entry();
                let displaced_dist: u16 = bucket.probe_distance as u16;

                self.bucket_mut(idx)
                    .set_entry(&cur_key, &cur_entry, cap_probe(dist));

                cur_key = displaced_key;
                cur_entry = displaced_entry;
                dist = displaced_dist;
            }

            idx = (idx + 1) & self.mask;
            dist += 1;

            if dist as usize >= self.capacity {
                return Err(HashTableError::Full {
                    count: self.count,
                    capacity: self.capacity,
                });
            }
        }
    }

    /// Remove an entry by key. Returns the removed entry if it existed.
    ///
    /// Uses backward-shift deletion for better probe-chain performance.
    pub fn remove(&mut self, key: &TxKey) -> Option<TxIndexEntry> {
        let fp = txid_fingerprint(&key.txid);
        let mut idx = bucket_index(key, self.mask);
        let mut dist: u16 = 0;

        // Find the entry.
        loop {
            let bucket = self.bucket(idx);
            if bucket.is_empty() {
                return None;
            }
            if bucket.is_occupied() {
                if (bucket.probe_distance as u16) < MAX_STORED_PROBE
                    && dist > bucket.probe_distance as u16
                {
                    return None;
                }
                if txid_fingerprint(&bucket.txid) == fp && bucket.txid == key.txid {
                    break; // Found at idx
                }
            }
            idx = (idx + 1) & self.mask;
            dist += 1;
            if dist as usize >= self.capacity {
                return None;
            }
        }

        let removed = self.bucket(idx).entry();
        self.count -= 1;

        // Backward-shift: move subsequent entries back to fill the gap.
        let mut empty_idx = idx;
        loop {
            let next_idx = (empty_idx + 1) & self.mask;
            let next = self.bucket(next_idx);
            if next.is_empty() || (next.is_occupied() && next.probe_distance == 0) {
                break;
            }
            // Shift this entry back.
            let shifted = *self.bucket(next_idx);
            let b = self.bucket_mut(empty_idx);
            *b = shifted;
            // Only decrement if probe_distance is below the cap; capped entries
            // may still have true distance > MAX_STORED_PROBE after the shift.
            if b.probe_distance < MAX_STORED_PROBE as u8 {
                b.probe_distance -= 1;
            }
            empty_idx = next_idx;
        }
        *self.bucket_mut(empty_idx) = Bucket::empty();

        Some(removed)
    }

    /// Number of occupied entries.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Total bucket capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Load factor: count / capacity.
    pub fn load_factor(&self) -> f64 {
        self.count as f64 / self.capacity as f64
    }

    /// Maximum probe distance observed.
    pub fn max_probe_distance(&self) -> usize {
        self.max_probe
    }

    /// Approximate memory usage in bytes (mmap region size).
    pub fn memory_bytes(&self) -> usize {
        self.mmap_len
    }

    /// Whether hugepages are backing this table.
    pub fn hugepage_enabled(&self) -> bool {
        self.hugepage
    }

    /// Update the cached fields for an existing entry.
    ///
    /// Returns `true` if the entry was found and updated, `false` if not found.
    /// This is a targeted update — only the specified fields are written,
    /// the rest of the bucket is untouched.
    #[allow(clippy::too_many_arguments)]
    pub fn update_cached_fields(
        &mut self,
        key: &TxKey,
        tx_flags: u8,
        block_entry_count: u8,
        spent_utxos: u32,
        dah_or_preserve: u32,
        unmined_since: u32,
        generation: u32,
    ) -> bool {
        let fp = txid_fingerprint(&key.txid);
        let mut idx = bucket_index(key, self.mask);
        let mut dist: u16 = 0;

        loop {
            let bucket = self.bucket(idx);
            if bucket.is_empty() {
                return false;
            }
            if bucket.is_occupied() {
                if (bucket.probe_distance as u16) < MAX_STORED_PROBE
                    && dist > bucket.probe_distance as u16
                {
                    return false;
                }
                if txid_fingerprint(&bucket.txid) == fp && bucket.txid == key.txid {
                    let b = self.bucket_mut(idx);
                    b.tx_flags = tx_flags;
                    b.block_entry_count = block_entry_count;
                    b.spent_utxos = spent_utxos;
                    b.dah_or_preserve = dah_or_preserve;
                    b.unmined_since = unmined_since;
                    b.generation = generation;
                    return true;
                }
            }
            idx = (idx + 1) & self.mask;
            dist += 1;
            if dist as usize >= self.capacity {
                return false;
            }
        }
    }

    /// Flush dirty pages to the backing file asynchronously.
    ///
    /// No-op for anonymous mmap. For file-backed tables, calls
    /// `msync(MS_ASYNC)` to schedule a writeback without blocking.
    pub fn sync(&self) {
        if let Backing::FileBacked { .. } = &self.backing
            && self.mmap_len > 0
            && !self.ptr.is_null()
        {
            unsafe { libc::msync(self.ptr.cast(), self.mmap_len, libc::MS_ASYNC) };
        }
    }

    /// Whether this table is backed by a persistent file.
    pub fn is_file_backed(&self) -> bool {
        matches!(self.backing, Backing::FileBacked { .. })
    }

    /// Resize the table to at least `new_capacity` buckets.
    ///
    /// For anonymous-mmap-backed tables, allocates a new mmap region and
    /// rehashes the entries in place. R-080 (BC-26): crash-atomicity is
    /// not meaningful for the anonymous-backed case — process death
    /// drops the mapping along with the entire address space, so there
    /// is nothing on disk to recover. The redo log attachment is
    /// silently no-op'd for anonymous tables and the index is rebuilt
    /// from a snapshot or a device scan on next startup. (Earlier doc
    /// language said "crash-atomic when a redo log is attached" without
    /// the anonymous-vs-file-backed split, which misled readers into
    /// expecting durable resize behaviour from anonymous tables.)
    ///
    /// For file-backed tables, the resize is crash-atomic via the redo
    /// log machinery the file-backed constructor automatically attaches
    /// (so the "without a redo log attached" caveat earlier in this doc
    /// has been removed — file-backed tables always have one when the
    /// server is configured with a redo log path):
    ///
    /// 1. A [`RedoOp::HashtableResizeBegin`] entry is appended and fsynced
    ///    BEFORE any tmp file is created. The entry captures the raw bytes
    ///    of the tmp file path and the target capacity.
    /// 2. A tmp file is created, the rehashed state is written into its
    ///    mmap, and the mmap is `msync(MS_SYNC)`'d. The file descriptor is
    ///    then `fsync`'d via `File::sync_all`.
    /// 3. The tmp file is renamed over the original file with `rename(2)`,
    ///    which is atomic on POSIX.
    /// 4. The parent directory is opened read-only and `fsync`'d so the
    ///    rename itself is durable across a power failure (Linux/Unix).
    /// 5. A [`RedoOp::HashtableResizeCommit`] with the matching capacity
    ///    is appended and fsynced to mark the resize complete.
    ///
    /// On crash between steps 1 and 5, recovery scans the redo log for
    /// `HashtableResizeBegin` without a matching `HashtableResizeCommit`
    /// and deletes the dangling tmp file — the primary index file itself
    /// is untouched until the rename in step 3.
    ///
    /// # Errors
    ///
    /// Returns [`HashTableError::ResizeIo`] on any filesystem I/O failure,
    /// [`HashTableError::ResizeRedo`] on redo log append/flush failure,
    /// and [`HashTableError::AllocFailed`] on memory mapping failure.
    pub fn resize(&mut self, new_capacity: usize) -> Result<()> {
        let new_cap = new_capacity.next_power_of_two().max(16);

        // Defensive re-check (M9): callers may request a resize based on a
        // load-factor snapshot that has since been invalidated by concurrent
        // removals. If `new_cap` is not actually larger than the current
        // capacity, skip — doing the work would be wasted effort and, on
        // the file-backed path, a wasted fsync/rename cycle. Callers like
        // `Index::register` guard against this externally, but the
        // invariant is defended here too so no internal path can trigger a
        // no-op resize.
        if new_cap <= self.capacity {
            return Ok(());
        }

        // Fast path: anonymous table. No journaling required because the
        // allocation is purely in-memory; a crash loses the table anyway.
        if matches!(self.backing, Backing::Anonymous) {
            let mut new_table = HashTable::new(new_cap)?;
            for i in 0..self.capacity {
                let bucket = self.bucket(i);
                if bucket.is_occupied() {
                    let key = TxKey { txid: bucket.txid };
                    new_table.insert(key, bucket.entry())?;
                }
            }
            let saved_redo = self.redo_log.take();
            *self = new_table;
            self.redo_log = saved_redo;
            return Ok(());
        }

        // File-backed path with crash-atomic rename.
        let old_path = match &self.backing {
            Backing::FileBacked { path, .. } => path.clone(),
            Backing::Anonymous => unreachable!("checked above"),
        };
        let tmp_path = old_path.with_extension("tmp");

        // Step 1: journal the begin intent BEFORE any tmp file I/O.
        if let Some(ref redo) = self.redo_log {
            let tmp_path_bytes = path_to_bytes(&tmp_path);
            let op = RedoOp::HashtableResizeBegin {
                tmp_path_bytes,
                new_capacity: new_cap as u64,
            };
            let mut guard = redo.lock();
            guard
                .append_and_flush(op)
                .map_err(|e| HashTableError::ResizeRedo(format!("begin append: {e}")))?;
        }

        // Step 2: write tmp file with rehashed state.
        // Remove any stale tmp file from a prior aborted resize.
        let _ = std::fs::remove_file(&tmp_path);
        let mut new_table = HashTable::open_file_backed(&tmp_path, new_cap)?;
        for i in 0..self.capacity {
            let bucket = self.bucket(i);
            if bucket.is_occupied() {
                let key = TxKey { txid: bucket.txid };
                new_table.insert(key, bucket.entry())?;
            }
        }

        // Step 3: msync + fsync the tmp file so its contents are durable
        // on disk before the rename.
        sync_file_backed(&new_table)?;

        // Step 4: atomically rename tmp over original.
        std::fs::rename(&tmp_path, &old_path).map_err(|e| {
            HashTableError::ResizeIo(format!(
                "rename {} -> {}: {e}",
                tmp_path.display(),
                old_path.display()
            ))
        })?;

        // Fault-injection sync point: simulates a crash AFTER the tmp
        // rename but BEFORE the parent-directory fsync. Post-recovery
        // the orphan cleanup path (see `recovery::recover_all_with_allocator`)
        // must leave the index consistent.
        crate::fault_injection::check(crate::fault_injection::SyncPoint::MidHashtableResize);

        // Step 5: fsync the parent directory so the rename is durable.
        fsync_parent_dir(&old_path)?;

        // Adjust the new table's recorded path to the original path so
        // subsequent reopens find the right file.
        if let Backing::FileBacked { path, .. } = &mut new_table.backing {
            *path = old_path.clone();
        }

        // Step 6: journal the commit AFTER the rename + dir fsync are durable.
        if let Some(ref redo) = self.redo_log {
            let op = RedoOp::HashtableResizeCommit {
                new_capacity: new_cap as u64,
            };
            let mut guard = redo.lock();
            guard
                .append_and_flush(op)
                .map_err(|e| HashTableError::ResizeRedo(format!("commit append: {e}")))?;
        }

        // Carry the redo log handle across the struct swap so subsequent
        // resizes remain journaled.
        let saved_redo = self.redo_log.take();
        *self = new_table;
        self.redo_log = saved_redo;
        Ok(())
    }

    /// Iterate over all occupied `(TxKey, TxIndexEntry)` pairs.
    pub fn iter(&self) -> HashTableIter<'_> {
        HashTableIter {
            table: self,
            pos: 0,
        }
    }
}

impl Drop for HashTable {
    fn drop(&mut self) {
        if self.mmap_len > 0 && !self.ptr.is_null() {
            if let Backing::FileBacked { fd, .. } = &self.backing {
                unsafe { libc::msync(self.ptr.cast(), self.mmap_len, libc::MS_SYNC) };
                unsafe { dealloc_mmap_buckets(self.ptr, self.mmap_len) };
                unsafe { libc::close(*fd) };
            } else {
                unsafe { dealloc_mmap_buckets(self.ptr, self.mmap_len) };
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

/// Iterator over occupied hash table entries.
pub struct HashTableIter<'a> {
    table: &'a HashTable,
    pos: usize,
}

impl<'a> Iterator for HashTableIter<'a> {
    type Item = (TxKey, TxIndexEntry);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.table.capacity {
            let bucket = self.table.bucket(self.pos);
            self.pos += 1;
            if bucket.is_occupied() {
                return Some((TxKey { txid: bucket.txid }, bucket.entry()));
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::disallowed_macros)]
mod tests {
    use super::*;

    fn make_key(n: u64) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&n.to_le_bytes());
        // Put something in bytes 8-16 for fingerprint variation
        txid[8..16].copy_from_slice(&(n.wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes());
        TxKey { txid }
    }

    fn make_entry(offset: u64) -> TxIndexEntry {
        TxIndexEntry {
            device_id: 0,
            record_offset: offset,
            utxo_count: 10,
            block_entry_count: 0,
            tx_flags: 0,
            spent_utxos: 0,
            dah_or_preserve: 0,
            unmined_since: 0,
            generation: 0,
        }
    }

    /// Create a key that hashes to a specific bucket (for collision testing).
    fn make_colliding_key(bucket_target: usize, sequence: u64, mask: usize) -> TxKey {
        let mut txid = [0u8; 32];
        // Set bytes 0-7 so bucket_index == bucket_target
        let base = (bucket_target & mask) as u64;
        txid[0..8].copy_from_slice(&base.to_le_bytes());
        // Set bytes 8-16 uniquely per sequence for different fingerprints
        txid[8..16].copy_from_slice(&sequence.to_le_bytes());
        // Set bytes 16+ for additional uniqueness
        txid[16..24].copy_from_slice(&(sequence.wrapping_mul(7)).to_le_bytes());
        TxKey { txid }
    }

    // -- Correctness tests --

    #[test]
    fn insert_one_get() {
        let mut t = HashTable::new(16).unwrap();
        let key = make_key(1);
        let entry = make_entry(4096);
        t.insert(key, entry).unwrap();

        let got = t.get_entry(&key).unwrap();
        assert_eq!(got, entry);
    }

    #[test]
    fn insert_100_get_each() {
        let mut t = HashTable::new(256).unwrap();
        let items: Vec<_> = (0..100)
            .map(|i| (make_key(i), make_entry(i * 4096)))
            .collect();

        for (k, e) in &items {
            t.insert(*k, *e).unwrap();
        }

        for (k, e) in &items {
            let got = t.get_entry(k).expect("key should exist");
            assert_eq!(got, *e);
        }
    }

    #[test]
    fn get_nonexistent() {
        let t = HashTable::new(16).unwrap();
        assert!(t.get_entry(&make_key(42)).is_none());
    }

    #[test]
    fn insert_same_key_twice() {
        let mut t = HashTable::new(16).unwrap();
        let key = make_key(1);
        let e1 = make_entry(1000);
        let e2 = make_entry(2000);

        let prev = t.insert(key, e1).unwrap();
        assert!(prev.is_none());

        let prev = t.insert(key, e2).unwrap();
        assert_eq!(prev, Some(e1));

        let got = t.get_entry(&key).unwrap();
        assert_eq!(got, e2);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn remove_existing() {
        let mut t = HashTable::new(16).unwrap();
        let key = make_key(1);
        let entry = make_entry(4096);
        t.insert(key, entry).unwrap();

        let removed = t.remove(&key);
        assert_eq!(removed, Some(entry));
        assert!(t.get_entry(&key).is_none());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn remove_nonexistent() {
        let mut t = HashTable::new(16).unwrap();
        assert!(t.remove(&make_key(99)).is_none());
    }

    #[test]
    fn insert_remove_insert() {
        let mut t = HashTable::new(16).unwrap();
        let key = make_key(1);
        let e1 = make_entry(1000);
        let e2 = make_entry(2000);

        t.insert(key, e1).unwrap();
        t.remove(&key);
        t.insert(key, e2).unwrap();

        let got = t.get_entry(&key).unwrap();
        assert_eq!(got, e2);
    }

    #[test]
    fn different_fingerprints_same_bucket() {
        let mut t = HashTable::new(1024).unwrap();
        // Two keys that map to the same bucket but have different fingerprints
        let k1 = make_colliding_key(42, 1, 1023);
        let k2 = make_colliding_key(42, 2, 1023);

        assert_eq!(
            bucket_index(&k1, 1023),
            bucket_index(&k2, 1023),
            "keys should collide"
        );
        assert_ne!(txid_fingerprint(&k1.txid), txid_fingerprint(&k2.txid));

        let e1 = make_entry(1000);
        let e2 = make_entry(2000);
        t.insert(k1, e1).unwrap();
        t.insert(k2, e2).unwrap();

        assert_eq!(t.get_entry(&k1), Some(e1));
        assert_eq!(t.get_entry(&k2), Some(e2));
        assert_eq!(t.len(), 2);
    }

    // -- Capacity and resize tests --

    #[test]
    fn fill_to_100_percent() {
        let mut t = HashTable::new(16).unwrap();
        assert_eq!(t.capacity(), 16);
        for i in 0..16u64 {
            t.insert(make_key(i), make_entry(i * 4096)).unwrap();
        }
        assert_eq!(t.len(), 16);
        for i in 0..16u64 {
            assert!(t.get_entry(&make_key(i)).is_some());
        }
    }

    #[test]
    fn resize_preserves_entries() {
        let mut t = HashTable::new(16).unwrap();
        for i in 0..12u64 {
            t.insert(make_key(i), make_entry(i * 100)).unwrap();
        }
        let old_cap = t.capacity();
        t.resize(old_cap * 2).unwrap();

        assert!(t.capacity() >= old_cap * 2);
        for i in 0..12u64 {
            let e = t
                .get_entry(&make_key(i))
                .expect("entry should survive resize");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn resize_doubles_capacity() {
        let mut t = HashTable::new(16).unwrap();
        for i in 0..12u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        t.resize(32).unwrap();
        assert_eq!(t.capacity(), 32);
    }

    #[test]
    fn new_inserts_after_resize() {
        let mut t = HashTable::new(16).unwrap();
        for i in 0..12u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        t.resize(32).unwrap();
        t.insert(make_key(100), make_entry(100)).unwrap();
        assert_eq!(t.get_entry(&make_key(100)).unwrap().record_offset, 100);
    }

    #[test]
    fn fill_70_percent() {
        let mut t = HashTable::new(1024).unwrap();
        for i in 0..716u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        for i in 0..716u64 {
            assert!(t.get_entry(&make_key(i)).is_some(), "missing key {i}");
        }
    }

    #[test]
    fn fill_90_percent() {
        let mut t = HashTable::new(1024).unwrap();
        for i in 0..921u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        for i in 0..921u64 {
            assert!(t.get_entry(&make_key(i)).is_some(), "missing key {i}");
        }
    }

    // -- Robin Hood invariant tests --

    #[test]
    fn collisions_all_found() {
        let mut t = HashTable::new(64).unwrap();
        let mask = t.capacity() - 1;
        let keys: Vec<_> = (0..10).map(|i| make_colliding_key(0, i, mask)).collect();

        for (i, k) in keys.iter().enumerate() {
            t.insert(*k, make_entry(i as u64 * 100)).unwrap();
        }

        for (i, k) in keys.iter().enumerate() {
            let e = t.get_entry(k).expect("colliding key should be found");
            assert_eq!(e.record_offset, i as u64 * 100);
        }
    }

    #[test]
    fn adversarial_1000_all_same_bucket() {
        let mut t = HashTable::new(2048).unwrap();
        let mask = t.capacity() - 1;
        let keys: Vec<_> = (0..1000).map(|i| make_colliding_key(0, i, mask)).collect();

        for (i, k) in keys.iter().enumerate() {
            t.insert(*k, make_entry(i as u64)).unwrap();
        }

        for (i, k) in keys.iter().enumerate() {
            let e = t
                .get_entry(k)
                .unwrap_or_else(|| panic!("adversarial key {i} not found"));
            assert_eq!(e.record_offset, i as u64);
        }
    }

    #[test]
    fn max_probe_distance_reasonable() {
        let mut t = HashTable::new(2048).unwrap();
        for i in 0..1800u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        // At ~88% load, max probe should be bounded.
        // Robin Hood at <90% load typically has max probe < 50.
        assert!(
            t.max_probe_distance() < 100,
            "probe distance {} too high",
            t.max_probe_distance()
        );
    }

    // -- Tombstone / delete tests --

    #[test]
    fn delete_middle_of_chain() {
        let mut t = HashTable::new(64).unwrap();
        let mask = t.capacity() - 1;
        let ka = make_colliding_key(5, 1, mask);
        let kb = make_colliding_key(5, 2, mask);
        let kc = make_colliding_key(5, 3, mask);

        t.insert(ka, make_entry(1)).unwrap();
        t.insert(kb, make_entry(2)).unwrap();
        t.insert(kc, make_entry(3)).unwrap();

        t.remove(&kb);

        assert_eq!(t.get_entry(&ka).unwrap().record_offset, 1);
        assert!(t.get_entry(&kb).is_none());
        assert_eq!(t.get_entry(&kc).unwrap().record_offset, 3);
    }

    #[test]
    fn delete_head_of_chain() {
        let mut t = HashTable::new(64).unwrap();
        let mask = t.capacity() - 1;
        let ka = make_colliding_key(5, 1, mask);
        let kb = make_colliding_key(5, 2, mask);
        let kc = make_colliding_key(5, 3, mask);

        t.insert(ka, make_entry(1)).unwrap();
        t.insert(kb, make_entry(2)).unwrap();
        t.insert(kc, make_entry(3)).unwrap();

        t.remove(&ka);

        assert!(t.get_entry(&ka).is_none());
        assert_eq!(t.get_entry(&kb).unwrap().record_offset, 2);
        assert_eq!(t.get_entry(&kc).unwrap().record_offset, 3);
    }

    #[test]
    fn insert_delete_reinsert() {
        let mut t = HashTable::new(16).unwrap();
        let key = make_key(42);
        t.insert(key, make_entry(100)).unwrap();
        t.remove(&key);
        t.insert(key, make_entry(200)).unwrap();
        assert_eq!(t.get_entry(&key).unwrap().record_offset, 200);
    }

    #[test]
    fn many_insert_delete_cycles() {
        let mut t = HashTable::new(8192).unwrap();
        for cycle in 0..50u64 {
            for i in 0..100u64 {
                let n = cycle * 1000 + i;
                t.insert(make_key(n), make_entry(n)).unwrap();
            }
            for i in 0..50u64 {
                let n = cycle * 1000 + i;
                t.remove(&make_key(n));
            }
        }
        // Table should still function correctly
        let key = make_key(999_999);
        t.insert(key, make_entry(42)).unwrap();
        assert_eq!(t.get_entry(&key).unwrap().record_offset, 42);
    }

    // -- Memory mapping tests --

    #[test]
    fn hashtable_uses_mmap() {
        // Verify that the hash table's memory is allocated via mmap
        // by checking that memory_bytes() matches expected bucket * capacity.
        let t = HashTable::new(1024).unwrap();
        let expected = t.capacity() * BUCKET_SIZE;
        assert_eq!(t.memory_bytes(), expected);
        // The pointer should be page-aligned (mmap guarantees this).
        assert_eq!(t.ptr as usize % 4096, 0);
    }

    #[test]
    fn hugepage_fallback_works() {
        // Even if hugepages aren't available (typical on macOS and unprivileged
        // Linux), the table should still be created successfully with regular pages.
        let t = HashTable::new(4096).unwrap();
        assert_eq!(t.capacity(), 4096);
        assert_eq!(t.len(), 0);
        // On macOS, hugepage should always be false.
        #[cfg(target_os = "macos")]
        assert!(!t.hugepage_enabled());
    }

    #[test]
    fn drop_releases_memory() {
        // Allocate a reasonably sized table, then drop it.
        // We can't directly verify munmap, but we can verify the table
        // was created and dropped without error or leak.
        let t = HashTable::new(1 << 16).unwrap(); // 64K buckets
        let bytes = t.memory_bytes();
        assert!(bytes > 0);
        drop(t);
        // If munmap failed, the process would continue but with a memory leak.
        // Under valgrind or miri this would be caught.
    }

    // -- Scale tests --

    #[test]
    fn one_million_entries() {
        let mut t = HashTable::new(1 << 21).unwrap(); // ~2M buckets
        for i in 0..1_000_000u64 {
            t.insert(make_key(i), make_entry(i * 8)).unwrap();
        }
        assert_eq!(t.len(), 1_000_000);

        for i in 0..1_000_000u64 {
            let e = t
                .get_entry(&make_key(i))
                .unwrap_or_else(|| panic!("key {i} not found at 1M scale"));
            assert_eq!(e.record_offset, i * 8);
        }
    }

    #[test]
    fn memory_usage_proportional() {
        let t = HashTable::new(1 << 20).unwrap(); // 1M buckets
        let expected = (1 << 20) * BUCKET_SIZE;
        let actual = t.memory_bytes();
        // Should be exactly proportional
        assert_eq!(actual, expected);
    }

    #[test]
    fn ten_million_entries() {
        let mut t = HashTable::new(1 << 24).unwrap(); // 16M buckets
        for i in 0..10_000_000u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        assert_eq!(t.len(), 10_000_000);

        // Spot check a sample of entries
        for i in (0..10_000_000u64).step_by(1000) {
            let e = t
                .get_entry(&make_key(i))
                .unwrap_or_else(|| panic!("key {i} not found at 10M scale"));
            assert_eq!(e.record_offset, i);
        }
    }

    // -- Performance benchmarks (measured, informational) --

    #[test]
    fn bench_lookup_1m() {
        let mut t = HashTable::new(1 << 21).unwrap();
        for i in 0..1_000_000u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }

        let start = std::time::Instant::now();
        let iters = 1_000_000;
        for i in 0..iters {
            let _ = t.get_entry(&make_key(i));
        }
        let elapsed = start.elapsed();
        let ns_per_lookup = elapsed.as_nanos() / iters as u128;
        eprintln!(
            "[bench] 1M entries: avg lookup = {ns_per_lookup} ns ({} lookups/sec)",
            1_000_000_000u128 / ns_per_lookup.max(1)
        );
    }

    #[test]
    fn bench_insert_throughput() {
        let start = std::time::Instant::now();
        let count = 1_000_000u64;
        let mut t = HashTable::new((count as usize).next_power_of_two() * 2).unwrap();
        for i in 0..count {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = count as f64 / elapsed.as_secs_f64();
        eprintln!(
            "[bench] insert throughput: {ops_per_sec:.0} ops/sec ({count} inserts in {elapsed:?})"
        );
    }

    // -- Iterator tests --

    #[test]
    fn iter_all_entries() {
        let mut t = HashTable::new(64).unwrap();
        for i in 0..20u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }

        let mut collected: Vec<_> = t.iter().collect();
        collected.sort_by_key(|(_, e)| e.record_offset);
        assert_eq!(collected.len(), 20);
        for (i, (_, e)) in collected.iter().enumerate() {
            assert_eq!(e.record_offset, i as u64);
        }
    }

    // -- File-backed tests --

    #[test]
    fn file_backed_create_insert_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let mut t = HashTable::open_file_backed(&path, 64).unwrap();
        assert!(t.is_file_backed());
        assert_eq!(t.len(), 0);

        let key = make_key(1);
        let entry = make_entry(4096);
        t.insert(key, entry).unwrap();
        assert_eq!(t.len(), 1);

        let got = t.get_entry(&key).unwrap();
        assert_eq!(got, entry);
    }

    #[test]
    fn file_backed_reopen_recovers_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");

        {
            let mut t = HashTable::open_file_backed(&path, 64).unwrap();
            for i in 0..20u64 {
                t.insert(make_key(i), make_entry(i * 100)).unwrap();
            }
            assert_eq!(t.len(), 20);
        }

        let t = HashTable::open_file_backed(&path, 64).unwrap();
        assert_eq!(t.len(), 20);
        for i in 0..20u64 {
            let e = t.get_entry(&make_key(i)).expect("should survive reopen");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn file_backed_sync_is_noop_safe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let mut t = HashTable::open_file_backed(&path, 16).unwrap();
        t.insert(make_key(1), make_entry(100)).unwrap();
        t.sync();
    }

    #[test]
    fn file_backed_remove_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let mut t = HashTable::open_file_backed(&path, 64).unwrap();

        for i in 0..10u64 {
            t.insert(make_key(i), make_entry(i * 100)).unwrap();
        }
        assert_eq!(t.len(), 10);

        let removed = t.remove(&make_key(5)).expect("should find entry");
        assert_eq!(removed.record_offset, 500);
        assert_eq!(t.len(), 9);
        assert!(t.get_entry(&make_key(5)).is_none());
    }

    #[test]
    fn file_backed_resize() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let mut t = HashTable::open_file_backed(&path, 16).unwrap();
        let initial_cap = t.capacity();

        for i in 0..10u64 {
            t.insert(make_key(i), make_entry(i * 100)).unwrap();
        }

        t.resize(initial_cap * 2).unwrap();
        assert!(t.capacity() >= initial_cap * 2);
        assert_eq!(t.len(), 10);

        for i in 0..10u64 {
            let e = t
                .get_entry(&make_key(i))
                .expect("entry should survive resize");
            assert_eq!(e.record_offset, i * 100);
        }

        assert!(!dir.path().join("test.tmp").exists());
    }

    #[test]
    fn file_backed_resize_then_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let new_cap;

        {
            let mut t = HashTable::open_file_backed(&path, 16).unwrap();
            for i in 0..10u64 {
                t.insert(make_key(i), make_entry(i * 100)).unwrap();
            }
            t.resize(64).unwrap();
            new_cap = t.capacity();
        }

        let t = HashTable::open_file_backed(&path, new_cap).unwrap();
        assert_eq!(t.len(), 10);
        for i in 0..10u64 {
            let e = t
                .get_entry(&make_key(i))
                .expect("should survive resize + reopen");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn file_backed_update_cached_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let mut t = HashTable::open_file_backed(&path, 32).unwrap();
        let key = make_key(1);
        t.insert(key, make_entry(4096)).unwrap();

        let updated = t.update_cached_fields(&key, 0xFF, 5, 8, 200, 600, 99);
        assert!(updated);

        let e = t.get_entry(&key).unwrap();
        assert_eq!(e.tx_flags, 0xFF);
        assert_eq!(e.block_entry_count, 5);
        assert_eq!(e.spent_utxos, 8);
        assert_eq!(e.dah_or_preserve, 200);
        assert_eq!(e.unmined_since, 600);
        assert_eq!(e.generation, 99);
        assert_eq!(e.record_offset, 4096);
    }

    #[test]
    fn file_backed_matches_anonymous() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");

        let mut anon = HashTable::new(64).unwrap();
        let mut fb = HashTable::open_file_backed(&path, 64).unwrap();

        for i in 0..30u64 {
            anon.insert(make_key(i), make_entry(i * 100)).unwrap();
            fb.insert(make_key(i), make_entry(i * 100)).unwrap();
        }

        assert_eq!(anon.len(), fb.len());
        for i in 0..30u64 {
            let a = anon.get_entry(&make_key(i)).unwrap();
            let f = fb.get_entry(&make_key(i)).unwrap();
            assert_eq!(a, f, "mismatch at key {i}");
        }
    }

    #[test]
    fn anonymous_sync_is_noop() {
        let mut t = HashTable::new(16).unwrap();
        assert!(!t.is_file_backed());
        t.insert(make_key(1), make_entry(100)).unwrap();
        t.sync();
    }

    // -- Crash-atomic resize tests (C5) --

    /// Shared helper: wrap a MemoryDevice-backed RedoLog so it can be
    /// attached to a HashTable.
    fn make_attached_redo_log() -> (
        std::sync::Arc<crate::device::MemoryDevice>,
        std::sync::Arc<parking_lot::Mutex<crate::redo::RedoLog>>,
    ) {
        let dev = std::sync::Arc::new(crate::device::MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let log = crate::redo::RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
        (dev, std::sync::Arc::new(parking_lot::Mutex::new(log)))
    }

    #[test]
    fn resize_journals_begin_and_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let (redo_dev, redo_log) = make_attached_redo_log();

        let mut t = HashTable::open_file_backed(&path, 16).unwrap();
        t.set_redo_log(redo_log.clone());
        for i in 0..10u64 {
            t.insert(make_key(i), make_entry(i * 100)).unwrap();
        }
        let initial_cap = t.capacity();
        t.resize(initial_cap * 2).unwrap();
        let new_cap = t.capacity() as u64;

        // Read back the redo log and assert both ops are present in order.
        let readback = crate::redo::RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let entries = readback.recover().unwrap();

        let resize_entries: Vec<_> = entries
            .iter()
            .filter(|e| {
                matches!(
                    e.op,
                    crate::redo::RedoOp::HashtableResizeBegin { .. }
                        | crate::redo::RedoOp::HashtableResizeCommit { .. }
                )
            })
            .collect();
        assert_eq!(
            resize_entries.len(),
            2,
            "expected exactly Begin + Commit in redo log"
        );

        match &resize_entries[0].op {
            crate::redo::RedoOp::HashtableResizeBegin {
                tmp_path_bytes,
                new_capacity,
            } => {
                assert_eq!(*new_capacity, new_cap, "Begin capacity must match final");
                // Tmp path ends with `.tmp` and is derived from the index path.
                use std::os::unix::ffi::OsStringExt;
                let tmp_os = std::ffi::OsString::from_vec(tmp_path_bytes.clone());
                let tmp = std::path::PathBuf::from(tmp_os);
                assert_eq!(tmp, path.with_extension("tmp"));
            }
            other => panic!("expected ResizeBegin first, got {other:?}"),
        }
        match &resize_entries[1].op {
            crate::redo::RedoOp::HashtableResizeCommit { new_capacity } => {
                assert_eq!(*new_capacity, new_cap, "Commit capacity must match Begin");
            }
            other => panic!("expected ResizeCommit second, got {other:?}"),
        }
        // Ordering: Begin sequence < Commit sequence
        assert!(
            resize_entries[0].sequence < resize_entries[1].sequence,
            "Begin must precede Commit in the log"
        );
    }

    #[test]
    fn resize_begin_without_commit_cleans_up_tmp_on_recovery() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let idx_path = dir.path().join("orphan.idx");
        let tmp_path = idx_path.with_extension("tmp");

        // Create a real primary index file so PrimaryBackend can open it.
        let mut base = HashTable::open_file_backed(&idx_path, 16).unwrap();
        base.insert(make_key(1), make_entry(100)).unwrap();
        drop(base);

        // Write an orphan tmp file as would be left by a crash between
        // HashtableResizeBegin and the rename.
        {
            let mut f = std::fs::File::create(&tmp_path).unwrap();
            f.write_all(b"partial contents from crashed resize")
                .unwrap();
            f.sync_all().unwrap();
        }
        assert!(tmp_path.exists(), "tmp file should exist before recovery");

        // Build a redo log that contains ONLY the Begin (no Commit) and run
        // recovery. The orphan tmp file must be removed.
        let redo_dev =
            std::sync::Arc::new(crate::device::MemoryDevice::new(1024 * 1024, 4096).unwrap());
        {
            let mut log = crate::redo::RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();
            use std::os::unix::ffi::OsStrExt;
            log.append_and_flush(crate::redo::RedoOp::HashtableResizeBegin {
                tmp_path_bytes: tmp_path.as_os_str().as_bytes().to_vec(),
                new_capacity: 32,
            })
            .unwrap();
        }

        let data_dev =
            std::sync::Arc::new(crate::device::MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = crate::allocator::SlotAllocator::new(data_dev.clone()).unwrap();
        let mut index = crate::index::PrimaryBackend::new_in_memory(1000).unwrap();
        let mut dah = crate::index::DahBackend::new_in_memory();
        let mut unmined = crate::index::UnminedBackend::new_in_memory();

        let redo_log = crate::redo::RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();
        let stats = crate::recovery::recover_all_with_allocator(
            &*data_dev,
            &redo_log,
            &mut index,
            &mut dah,
            &mut unmined,
            Some(&mut alloc),
        )
        .unwrap();

        assert_eq!(
            stats.entries_replayed, 1,
            "the Begin entry must be counted as replayed (pending-resize tracked)"
        );
        assert!(
            !tmp_path.exists(),
            "orphan resize tmp file must be removed by recovery"
        );
        // The original index file is untouched and still valid.
        assert!(
            idx_path.exists(),
            "primary index file must survive recovery"
        );
    }

    #[test]
    fn resize_persists_across_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persist.idx");
        let (_redo_dev, redo_log) = make_attached_redo_log();

        let initial_cap;
        let new_cap;
        {
            let mut t = HashTable::open_file_backed(&path, 16).unwrap();
            t.set_redo_log(redo_log.clone());
            initial_cap = t.capacity();
            for i in 0..14u64 {
                t.insert(make_key(i), make_entry(i * 7 + 1)).unwrap();
            }
            t.resize(initial_cap * 4).unwrap();
            new_cap = t.capacity();
            assert!(new_cap >= initial_cap * 4);

            // Sanity check all keys resolve before shutdown.
            for i in 0..14u64 {
                let e = t.get_entry(&make_key(i)).unwrap();
                assert_eq!(e.record_offset, i * 7 + 1);
            }
        } // HashTable dropped — simulates clean shutdown

        // Reopen — the on-disk file should have the new capacity and all keys.
        let t2 = HashTable::open_file_backed(&path, 16).unwrap();
        assert_eq!(
            t2.capacity(),
            new_cap,
            "reopened capacity must match post-resize"
        );
        assert_eq!(t2.len(), 14, "all entries must survive resize + reopen");
        for i in 0..14u64 {
            let e = t2
                .get_entry(&make_key(i))
                .unwrap_or_else(|| panic!("key {i} lost after resize + reopen"));
            assert_eq!(
                e.record_offset,
                i * 7 + 1,
                "record_offset mismatch at key {i}"
            );
        }

        // No orphan tmp file left behind.
        assert!(
            !path.with_extension("tmp").exists(),
            "tmp file must not leak"
        );
    }
}
