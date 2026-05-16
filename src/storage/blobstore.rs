//! Blob store trait and file-based implementation.
//!
//! Large transaction data (> 1 MiB) is stored in an external blob store
//! keyed by txid. The file-based implementation uses a directory tree
//! organized by hash prefix.
//!
//! # Durability and integrity
//!
//! All write paths in [`FileBlobStore`] are crash-atomic and content-verified:
//!
//! 1. Payload bytes are written to a sibling `.tmp` file, `fsync`'d, then
//!    renamed into place.
//! 2. A small sidecar file (`<blob>.meta`) is written containing the
//!    SHA-256 digest and length of the payload, again via tmp+rename.
//! 3. The parent directory is `fsync`'d after both renames so the
//!    directory entries themselves are durable across power failure.
//!
//! Reads validate the payload digest against the sidecar before returning
//! data to callers, defending against bit rot and on-disk tampering.

use sha2::{Digest, Sha256};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlobError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("blob not found: {key}")]
    NotFound { key: String },
    /// The on-disk payload digest disagrees with the sidecar's expected digest,
    /// indicating bit rot, tampering, or torn write.
    #[error("digest mismatch for blob {key}")]
    DigestMismatch {
        key: String,
        expected: [u8; 32],
        actual: [u8; 32],
    },
    /// The sidecar metadata file is missing, truncated, or malformed.
    #[error("blob meta missing or invalid for {key}")]
    InvalidMeta { key: String },
    /// The asynchronous blob uploader's bounded task queue is full.
    ///
    /// Returned by [`crate::storage::uploader::BlobUploader::submit`] when the
    /// in-flight queue is saturated. Callers should apply backpressure (retry
    /// with delay, fail the request, or fall back to a synchronous upload) —
    /// the original unbounded channel let memory grow without limit under
    /// bursty external-tier load (F-G9-003).
    #[error("uploader queue full ({queued} tasks queued; capacity {capacity})")]
    UploaderQueueFull { queued: usize, capacity: usize },
}

pub type Result<T> = std::result::Result<T, BlobError>;

/// Suffix used for the sidecar file storing `(sha256, length)` next to every blob.
const META_SUFFIX: &str = ".meta";

/// Suffix used for in-progress write tmp files.
const TMP_SUFFIX: &str = ".tmp";
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// On-disk sidecar layout: 32-byte SHA-256 digest followed by 8-byte length (little-endian).
///
/// Stored as a separate file (`<blob>.meta`) so the blob payload itself
/// remains byte-identical with what callers see on read.
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct BlobMetaLayout {
    /// SHA-256 of the full payload.
    sha256: [u8; 32],
    /// Payload length in bytes (little-endian on disk).
    length: u64,
}

const _BLOB_META_LAYOUT_SIZE: () = {
    assert!(std::mem::size_of::<BlobMetaLayout>() == 40);
    assert!(std::mem::align_of::<BlobMetaLayout>() == 1);
};

/// Size of the on-disk blob metadata sidecar in bytes.
const BLOB_META_SIZE: usize = std::mem::size_of::<BlobMetaLayout>();

/// Content digest and length recorded for a stored blob.
///
/// Returned by [`BlobStore::put`] and [`BlobStreamWriter::finish`] so callers
/// can record the actual payload digest in record metadata (e.g. an
/// `ExternalRef` on the device record).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobDigest {
    /// SHA-256 of the payload bytes.
    pub sha256: [u8; 32],
    /// Payload length in bytes.
    pub length: u64,
}

/// Format a 32-byte key as a hex string.
fn hex_key(key: &[u8; 32]) -> String {
    key.iter().map(|b| format!("{b:02x}")).collect()
}

/// Compute SHA-256 of `data` and return the 32-byte digest.
fn sha256_of(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let out = hasher.finalize();
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&out);
    digest
}

/// Encode a [`BlobMetaLayout`] to its 40-byte on-disk representation.
fn encode_meta(digest: &[u8; 32], length: u64) -> [u8; BLOB_META_SIZE] {
    let mut buf = [0u8; BLOB_META_SIZE];
    buf[..32].copy_from_slice(digest);
    buf[32..40].copy_from_slice(&length.to_le_bytes());
    buf
}

/// Decode the on-disk meta sidecar bytes. Returns `None` if the buffer is the
/// wrong size.
fn decode_meta(bytes: &[u8]) -> Option<([u8; 32], u64)> {
    if bytes.len() != BLOB_META_SIZE {
        return None;
    }
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&bytes[..32]);
    let mut len_buf = [0u8; 8];
    len_buf.copy_from_slice(&bytes[32..40]);
    Some((sha, u64::from_le_bytes(len_buf)))
}

/// fsync the parent directory of `path` so that a recent rename into that
/// directory is durable across a power failure.
///
/// On Unix, opens the parent directory read-only and calls
/// [`std::fs::File::sync_all`]. On non-Unix platforms the call is a
/// best-effort no-op (this server targets Linux/Unix).
#[cfg(unix)]
fn fsync_parent_dir(path: &Path) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let dir = std::fs::File::open(parent)?;
    dir.sync_all()
}

/// Non-Unix fallback: best-effort no-op. See [`fsync_parent_dir`].
#[cfg(not(unix))]
fn fsync_parent_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn fsync_dir(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn fsync_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

fn create_dir_all_durable(base_dir: &Path, path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    let mut dirs = Vec::new();
    for dir in path.ancestors() {
        dirs.push(dir);
        if dir == base_dir {
            break;
        }
    }
    for dir in dirs {
        fsync_dir(dir)?;
    }
    Ok(())
}

fn unique_tmp_path(final_path: &Path) -> PathBuf {
    let id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut tmp = final_path.as_os_str().to_os_string();
    tmp.push(format!(
        ".{}.{}.{}",
        std::process::id(),
        id,
        TMP_SUFFIX.trim_start_matches('.')
    ));
    PathBuf::from(tmp)
}

/// Atomically write `data` to `final_path` via a sibling `.tmp` file with
/// fsync on the file. The parent directory is **not** fsync'd here — the
/// caller must call [`fsync_parent_dir`] once after all related files
/// (payload + sidecar) have been renamed.
fn atomic_write_no_dir_fsync(final_path: &Path, data: &[u8]) -> std::io::Result<()> {
    let tmp_path = unique_tmp_path(final_path);

    // Scope the file handle so it's closed before the rename.
    {
        let mut f = std::fs::File::create(&tmp_path)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, final_path)?;
    Ok(())
}

/// Trait for streaming large blob writes in chunks.
///
/// Created by [`BlobStore::begin_stream`]. The caller appends data via
/// [`Self::write_chunk`] and finalizes with [`Self::finish`]. If the stream is dropped
/// without finishing, [`Self::abort`] cleans up any partial data.
pub trait BlobStreamWriter: Send {
    /// Append a chunk of data to the stream.
    fn write_chunk(&mut self, data: &[u8]) -> Result<()>;

    /// Finalize the stream, making the blob available for reads.
    ///
    /// Returns the SHA-256 digest and total length of the bytes written.
    /// Callers should record both in any metadata that references the blob
    /// so the blob can be integrity-checked on subsequent reads.
    fn finish(self: Box<Self>) -> Result<BlobDigest>;

    /// Abort the stream and clean up any partial data.
    fn abort(self: Box<Self>) -> Result<()>;
}

/// Trait for external blob storage.
pub trait BlobStore: Send + Sync {
    /// Write a blob keyed by txid.
    ///
    /// Returns the SHA-256 digest and length of the written payload so callers
    /// can stamp the digest into record metadata. On the file-backed
    /// implementation the write is atomic (tmp+rename+fsync of payload,
    /// sidecar, and parent directory).
    fn put(&self, key: &[u8; 32], data: &[u8]) -> Result<BlobDigest>;

    /// Read a blob, verifying its content digest against the sidecar.
    ///
    /// Returns `None` if not found. Returns [`BlobError::DigestMismatch`] if
    /// the on-disk payload disagrees with the recorded digest.
    fn get(&self, key: &[u8; 32]) -> Result<Option<Vec<u8>>>;

    /// Read a byte range from a blob.
    ///
    /// **Integrity note:** verifying a SHA-256 digest on a partial read is
    /// not meaningful, so this method reads the entire payload, verifies the
    /// full content against the sidecar digest, and then returns the
    /// requested slice. Callers that want byte-range access without a full
    /// digest check should not use this method.
    fn get_range(&self, key: &[u8; 32], offset: u64, length: u64) -> Result<Option<Vec<u8>>>;

    /// Delete a blob.
    fn delete(&self, key: &[u8; 32]) -> Result<()>;

    /// Check if a blob exists.
    fn exists(&self, key: &[u8; 32]) -> Result<bool>;

    /// Return the durable digest sidecar for a blob without reading the full payload.
    ///
    /// Returns `Ok(None)` only when the payload or sidecar is absent. Returns
    /// [`BlobError::InvalidMeta`] if the sidecar is present but malformed.
    fn digest(&self, key: &[u8; 32]) -> Result<Option<BlobDigest>>;

    /// Stream a blob to a writer (for large blobs).
    ///
    /// Returns the number of bytes written, or [`BlobError::NotFound`] if the
    /// blob does not exist. The full payload is digest-verified against the
    /// sidecar before any bytes are sent to `writer`.
    fn stream_to(&self, key: &[u8; 32], writer: &mut dyn std::io::Write) -> Result<u64>;

    /// Begin a streaming write for a blob keyed by txid.
    ///
    /// Returns a writer that accumulates chunks. Call [`BlobStreamWriter::finish`]
    /// to finalize. If the stream is abandoned, call [`BlobStreamWriter::abort`]
    /// or drop the writer (which will NOT clean up — always call abort explicitly).
    fn begin_stream(&self, key: &[u8; 32]) -> Result<Box<dyn BlobStreamWriter>>;

    /// Enumerate every txid currently materialised by the store.
    ///
    /// Used by the orphan-blob garbage collector (R-049): recovery and the
    /// periodic background sweep call `list()` and reconcile each returned key
    /// against the primary index. Any txid whose index entry is absent or not
    /// flagged [`crate::record::TxFlags::EXTERNAL`] is an orphan from a failed
    /// create, aborted upload, or migration cancellation, and is deleted.
    ///
    /// Implementations MAY also use this entry-point to perform incidental
    /// housekeeping (e.g. [`FileBlobStore::list`] sweeps stale `.tmp` files
    /// older than [`FileBlobStore::STALE_TMP_AGE_SECS`] seconds while it walks
    /// the directory tree). Such side effects must never delete a finalised
    /// blob payload or its sidecar — only `.tmp` debris from interrupted writes.
    ///
    /// Returns the list of txids whose payload **and** sidecar are present
    /// (matching the [`Self::exists`] contract). Half-written blobs (payload
    /// without sidecar, or vice versa) are NOT returned — they are unusable
    /// and the caller has no way to reconcile them.
    fn list(&self) -> Result<Vec<[u8; 32]>>;

    /// Enumerate blobs eligible for orphan-blob garbage collection.
    ///
    /// F-G9-004 (race mitigation): the periodic blob-GC sweep can race with a
    /// concurrent create that has just put a blob but whose index
    /// registration has not landed yet — without a min-age filter the
    /// freshly-uploaded blob would be classified as an orphan and deleted
    /// out from under the in-flight create. `list_for_gc(min_age)` filters
    /// the returned set to blobs whose payload mtime is at least `min_age`
    /// old (file backend) or to all blobs (in-memory backends used by tests
    /// where the race cannot manifest).
    ///
    /// The default implementation falls back to [`Self::list`] for stores
    /// without per-blob mtime — recovery and tests are unaffected; only the
    /// production [`FileBlobStore`] (and other backends that override this)
    /// pay the grace cost.
    fn list_for_gc(&self, _min_age: std::time::Duration) -> Result<Vec<[u8; 32]>> {
        self.list()
    }
}

/// File-based blob store organized by hash prefix directories.
///
/// ```text
/// base_dir/ab/cd/abcdef01...789a       (full txid hex as filename)
/// base_dir/ab/cd/abcdef01...789a.meta  (sha256 + length sidecar)
/// ```
pub struct FileBlobStore {
    base_dir: PathBuf,
    prefix_depth: usize,
    key_locks: Arc<Vec<parking_lot::Mutex<()>>>,
}

impl FileBlobStore {
    /// Maximum age of a `.tmp` upload artefact before [`Self::list`] deletes it.
    ///
    /// Any in-progress streaming write (`begin_stream` → `write_chunk`* →
    /// `finish`) must complete within this window. The default is intentionally
    /// short (5 minutes) so that crashed uploads, dropped clients, and
    /// abandoned migration-side blob writes do not leak inodes between GC
    /// cycles. Blob payloads themselves are NEVER swept by age — only `.tmp`
    /// files whose mtime indicates a write that started but never finished.
    pub const STALE_TMP_AGE_SECS: u64 = 5 * 60;

    /// Create a new file blob store at the given directory.
    ///
    /// `prefix_depth` controls how many hex-byte pairs are used for
    /// subdirectory nesting (default 2 → `ab/cd/`).
    pub fn new(base_dir: &Path, prefix_depth: usize) -> Self {
        Self {
            base_dir: base_dir.to_path_buf(),
            prefix_depth,
            key_locks: Arc::new((0..256).map(|_| parking_lot::Mutex::new(())).collect()),
        }
    }

    fn lock_index(key: &[u8; 32]) -> usize {
        key[0] as usize
    }

    /// Decode a hex filename back to a 32-byte txid. Returns `None` for any
    /// name that is not exactly 64 lowercase-hex characters — this filters
    /// `.tmp`, `.meta`, and any non-blob debris that may live in the tree.
    fn decode_blob_filename(name: &str) -> Option<[u8; 32]> {
        if name.len() != 64 {
            return None;
        }
        let bytes = name.as_bytes();
        let mut out = [0u8; 32];
        for (i, slot) in out.iter_mut().enumerate() {
            let hi = (bytes[i * 2] as char).to_digit(16)?;
            let lo = (bytes[i * 2 + 1] as char).to_digit(16)?;
            *slot = ((hi << 4) | lo) as u8;
        }
        Some(out)
    }

    /// Recursively walk the prefix directory tree under `dir`, accumulating
    /// every txid whose payload **and** sidecar are present, and removing any
    /// `.tmp` upload artefact whose mtime is older than `stale_cutoff`.
    ///
    /// `min_age_cutoff` is an optional `SystemTime`: when set, blobs whose
    /// payload mtime is newer than the cutoff are excluded from the returned
    /// list. Used by the orphan-blob GC sweep (F-G9-004) to skip blobs that
    /// may belong to an in-flight create whose index registration has not
    /// landed yet. `None` returns the full set (used by `list()` and
    /// recovery, which is race-free).
    ///
    /// Errors from individual directory entries (race with another process
    /// removing the file mid-walk, transient stat failures) are logged at
    /// `warn` and skipped — the GC sweep must make forward progress on
    /// healthy entries even if one is misbehaving.
    fn walk_dir(
        dir: &Path,
        stale_cutoff: std::time::SystemTime,
        min_age_cutoff: Option<std::time::SystemTime>,
        out: &mut Vec<[u8; 32]>,
    ) -> std::io::Result<()> {
        let read_dir = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        for entry in read_dir {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(dir = %dir.display(), err = %e, "blob list: read_dir entry failed");
                    continue;
                }
            };
            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(path = %path.display(), err = %e, "blob list: file_type failed");
                    continue;
                }
            };
            if file_type.is_dir() {
                if let Err(e) = Self::walk_dir(&path, stale_cutoff, min_age_cutoff, out) {
                    tracing::warn!(path = %path.display(), err = %e, "blob list: subdir walk failed");
                }
                continue;
            }
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };
            // Stale .tmp sweep — see STALE_TMP_AGE_SECS for rationale.
            if name.ends_with(TMP_SUFFIX) {
                let mtime = entry.metadata().ok().and_then(|m| m.modified().ok());
                if let Some(mtime) = mtime
                    && mtime <= stale_cutoff
                {
                    match std::fs::remove_file(&path) {
                        Ok(()) => {
                            tracing::info!(path = %path.display(), "blob list: removed stale .tmp file");
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => {
                            tracing::warn!(path = %path.display(), err = %e, "blob list: failed to remove stale .tmp file");
                        }
                    }
                }
                continue;
            }
            // Sidecar files are not blobs — they are accounted for via the
            // companion payload's existence check below.
            if name.ends_with(META_SUFFIX) {
                continue;
            }
            let Some(txid) = Self::decode_blob_filename(name) else {
                continue;
            };
            // Match the `exists` contract: only return blobs whose sidecar is
            // also present. A payload without a sidecar (or vice versa) is a
            // half-written blob and must not be returned to the GC reconciler.
            let meta_path = Self::meta_path_for(&path);
            if !meta_path.exists() {
                continue;
            }
            // F-G9-004: skip blobs that are too fresh to be candidates for
            // orphan-blob GC. A concurrent create that has just put the blob
            // but whose index `register` has not landed yet would be
            // mis-classified as an orphan without this guard.
            if let Some(cutoff) = min_age_cutoff {
                let payload_mtime = entry.metadata().ok().and_then(|m| m.modified().ok());
                // Use the sidecar's mtime too if the payload's is missing or
                // newer; the later of the two is the right answer (a sidecar
                // rewrite leaves the payload mtime stale, and vice versa).
                let meta_mtime = std::fs::metadata(&meta_path)
                    .ok()
                    .and_then(|m| m.modified().ok());
                let mtime = match (payload_mtime, meta_mtime) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                };
                if let Some(mtime) = mtime
                    && mtime > cutoff
                {
                    // Too young — re-examine on the next sweep.
                    continue;
                }
            }
            out.push(txid);
        }
        Ok(())
    }

    fn blob_path(&self, key: &[u8; 32]) -> PathBuf {
        let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
        let mut path = self.base_dir.clone();
        for i in 0..self.prefix_depth {
            let start = i * 2;
            if start + 2 <= hex.len() {
                path = path.join(&hex[start..start + 2]);
            }
        }
        path.join(&hex)
    }

    /// Sidecar path for the given blob path.
    fn meta_path_for(blob_path: &Path) -> PathBuf {
        let mut p = blob_path.as_os_str().to_os_string();
        p.push(META_SUFFIX);
        PathBuf::from(p)
    }

    /// Read and decode the sidecar metadata for `blob_path`.
    fn read_meta(blob_path: &Path, key: &[u8; 32]) -> Result<([u8; 32], u64)> {
        let meta_path = Self::meta_path_for(blob_path);
        let bytes = match std::fs::read(&meta_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(BlobError::InvalidMeta { key: hex_key(key) });
            }
            Err(e) => return Err(BlobError::Io(e)),
        };
        decode_meta(&bytes).ok_or(BlobError::InvalidMeta { key: hex_key(key) })
    }

    /// Read the payload at `path`, then verify it against the sidecar.
    ///
    /// Returns the verified bytes, or a digest-mismatch / invalid-meta error
    /// if integrity checks fail.
    fn read_and_verify(blob_path: &Path, key: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let data = match std::fs::read(blob_path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(BlobError::Io(e)),
        };
        let (expected_sha, expected_len) = Self::read_meta(blob_path, key)?;
        if data.len() as u64 != expected_len {
            return Err(BlobError::DigestMismatch {
                key: hex_key(key),
                expected: expected_sha,
                actual: sha256_of(&data),
            });
        }
        let actual_sha = sha256_of(&data);
        if actual_sha != expected_sha {
            return Err(BlobError::DigestMismatch {
                key: hex_key(key),
                expected: expected_sha,
                actual: actual_sha,
            });
        }
        Ok(Some(data))
    }
}

/// Streaming writer that appends chunks to a temporary file,
/// then atomically renames to the final blob path on finish. The SHA-256
/// digest is computed incrementally so streams of arbitrary size do not
/// require buffering the full payload to hash it at finish time.
///
/// `finished` is set true by both [`BlobStreamWriter::finish`] (after a
/// successful rename) and [`BlobStreamWriter::abort`] (after intentional
/// teardown), so the `Drop` backstop only removes the `.tmp` file when
/// neither completion path ran — for example after a panic between
/// `begin_stream` and the dispatcher's `abort` registration (F-G9-007).
struct FileStreamWriter {
    key_locks: Arc<Vec<parking_lot::Mutex<()>>>,
    lock_index: usize,
    temp_path: PathBuf,
    final_path: PathBuf,
    file: Option<std::fs::File>,
    bytes_written: u64,
    hasher: Sha256,
    finished: bool,
}

impl BlobStreamWriter for FileStreamWriter {
    fn write_chunk(&mut self, data: &[u8]) -> Result<()> {
        // F-G9-007: if the underlying write fails (ENOSPC, EIO) the `.tmp`
        // file is left on disk. The `Drop` impl below removes it as a
        // backstop if neither `finish` nor `abort` runs. The dispatch path
        // already calls `abort` on write error (src/server/dispatch.rs).
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| BlobError::Io(std::io::Error::other("stream writer poisoned")))?;
        file.write_all(data)?;
        self.hasher.update(data);
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<BlobDigest> {
        let _guard = self.key_locks[self.lock_index].lock();
        let file = self
            .file
            .take()
            .ok_or_else(|| BlobError::Io(std::io::Error::other("stream writer already finished")))?;
        let bytes_written = self.bytes_written;
        let hasher = std::mem::take(&mut self.hasher);

        // 1. fsync the payload temp file, then rename into place.
        file.sync_all()?;
        drop(file);
        std::fs::rename(&self.temp_path, &self.final_path)?;

        // 2. Finalize the digest and write the sidecar atomically.
        let mut sha256 = [0u8; 32];
        sha256.copy_from_slice(&hasher.finalize());
        let meta_bytes = encode_meta(&sha256, bytes_written);
        let meta_path = FileBlobStore::meta_path_for(&self.final_path);
        atomic_write_no_dir_fsync(&meta_path, &meta_bytes)?;

        // 3. fsync the parent directory so both renames are durable.
        fsync_parent_dir(&self.final_path)?;

        // Mark finished so the Drop backstop does not delete the renamed
        // payload (the rename moved it out of `temp_path`, but defence in
        // depth — if a future change keeps the tmp around, the flag
        // prevents the backstop from racing it).
        self.finished = true;

        Ok(BlobDigest {
            sha256,
            length: bytes_written,
        })
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        self.file.take();
        let _ = std::fs::remove_file(&self.temp_path);
        self.finished = true;
        Ok(())
    }
}

/// F-G9-007 backstop: if a `FileStreamWriter` is dropped without `finish`
/// or `abort` (e.g. after a panic between `begin_stream` and the
/// dispatcher's stream-registration), remove the `.tmp` file rather than
/// leaving it on disk until the periodic `list()` sweep collects it five
/// minutes later. The normal `finish`/`abort` paths set `finished = true`,
/// so this is purely a safety net.
impl Drop for FileStreamWriter {
    fn drop(&mut self) {
        if !self.finished {
            self.file.take();
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

impl BlobStore for FileBlobStore {
    fn put(&self, key: &[u8; 32], data: &[u8]) -> Result<BlobDigest> {
        let _guard = self.key_locks[Self::lock_index(key)].lock();
        let path = self.blob_path(key);
        if let Some(parent) = path.parent() {
            create_dir_all_durable(&self.base_dir, parent)?;
        }

        // Atomic payload write: tmp -> fsync -> rename.
        atomic_write_no_dir_fsync(&path, data)?;

        // Compute digest and write the sidecar atomically.
        let sha256 = sha256_of(data);
        let length = data.len() as u64;
        let meta_path = Self::meta_path_for(&path);
        atomic_write_no_dir_fsync(&meta_path, &encode_meta(&sha256, length))?;

        // fsync the parent directory so both renames are durable across
        // power failure. Without this, the kernel may have flushed the file
        // contents but not the directory entries that point at them.
        fsync_parent_dir(&path)?;

        Ok(BlobDigest { sha256, length })
    }

    fn get(&self, key: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        // F-G9-005: take the per-key lock so the payload+sidecar pair we
        // observe is consistent with whatever writer most recently held the
        // lock. Without this guard, a concurrent `FileStreamWriter::finish`
        // that renames the payload before writing its sidecar leaves a brief
        // window where a reader sees the new payload bytes against a stale
        // sidecar digest — transient `DigestMismatch` errors.
        let _guard = self.key_locks[Self::lock_index(key)].lock();
        let path = self.blob_path(key);
        Self::read_and_verify(&path, key)
    }

    fn get_range(&self, key: &[u8; 32], offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        // Per the trait doc: verify the full payload digest before slicing.
        // Partial digests would not detect tampering of bytes outside the
        // requested window, so we read+verify the whole payload first.
        // F-G9-005: per-key lock for the same consistency reason as `get`.
        let _guard = self.key_locks[Self::lock_index(key)].lock();
        let data = match Self::read_and_verify(&self.blob_path(key), key)? {
            Some(d) => d,
            None => return Ok(None),
        };
        let start = offset as usize;
        if start >= data.len() {
            return Ok(Some(vec![]));
        }
        let end = (offset + length) as usize;
        let actual_end = end.min(data.len());
        Ok(Some(data[start..actual_end].to_vec()))
    }

    fn delete(&self, key: &[u8; 32]) -> Result<()> {
        let path = self.blob_path(key);
        let meta_path = Self::meta_path_for(&path);
        // Best-effort: remove sidecar first, then payload. Either may already
        // be missing if a previous write was interrupted.
        match std::fs::remove_file(&meta_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(BlobError::Io(e)),
        }
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(BlobError::Io(e)),
        }
    }

    fn exists(&self, key: &[u8; 32]) -> Result<bool> {
        // A blob is "present" only if both the payload and its sidecar exist;
        // a half-written blob (payload but no meta, or vice versa) must not
        // be considered usable.
        let path = self.blob_path(key);
        let meta_path = Self::meta_path_for(&path);
        Ok(path.exists() && meta_path.exists())
    }

    fn digest(&self, key: &[u8; 32]) -> Result<Option<BlobDigest>> {
        let path = self.blob_path(key);
        if !path.exists() {
            return Ok(None);
        }
        let meta_path = Self::meta_path_for(&path);
        if !meta_path.exists() {
            return Ok(None);
        }
        let (sha256, length) = Self::read_meta(&path, key)?;
        Ok(Some(BlobDigest { sha256, length }))
    }

    fn stream_to(&self, key: &[u8; 32], writer: &mut dyn std::io::Write) -> Result<u64> {
        let path = self.blob_path(key);
        // F-G9-005: take the per-key lock briefly to snapshot a consistent
        // (open-fd, sidecar) pair. We drop the lock before the long-lived
        // streaming work — the open file descriptor (inode) is then stable
        // across any subsequent rename, since Linux's rename-while-open is
        // inode-based, so the pages we hash in pass 1 and stream in pass 2
        // are the same bytes as the sidecar read here describes.
        let (mut file, expected_sha, expected_len) = {
            let _guard = self.key_locks[Self::lock_index(key)].lock();
            // Open both the payload and sidecar up front so a missing payload
            // surfaces NotFound (matching the original behavior) before we try
            // to verify integrity.
            let file = match std::fs::File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(BlobError::NotFound { key: hex_key(key) });
                }
                Err(e) => return Err(BlobError::Io(e)),
            };
            let (expected_sha, expected_len) = Self::read_meta(&path, key)?;
            (file, expected_sha, expected_len)
        };

        // Pass 1: verify by hashing fixed-size chunks. Do not write to the
        // caller until the full digest has been proven.
        let mut hasher = Sha256::new();
        let mut buf = [0u8; 64 * 1024];
        let mut total: u64 = 0;
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            total += n as u64;
        }
        if total != expected_len {
            let mut actual = [0u8; 32];
            actual.copy_from_slice(&hasher.finalize());
            return Err(BlobError::DigestMismatch {
                key: hex_key(key),
                expected: expected_sha,
                actual,
            });
        }
        let mut actual_sha = [0u8; 32];
        actual_sha.copy_from_slice(&hasher.finalize());
        if actual_sha != expected_sha {
            return Err(BlobError::DigestMismatch {
                key: hex_key(key),
                expected: expected_sha,
                actual: actual_sha,
            });
        }

        // Pass 2: stream the verified payload without retaining it in memory.
        file.rewind()?;
        let mut copied: u64 = 0;
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            writer.write_all(&buf[..n])?;
            copied += n as u64;
        }
        debug_assert_eq!(copied, total);
        Ok(total)
    }

    fn begin_stream(&self, key: &[u8; 32]) -> Result<Box<dyn BlobStreamWriter>> {
        let final_path = self.blob_path(key);
        let temp_path = unique_tmp_path(&final_path);
        if let Some(parent) = final_path.parent() {
            create_dir_all_durable(&self.base_dir, parent)?;
        }
        let file = std::fs::File::create(&temp_path)?;
        Ok(Box::new(FileStreamWriter {
            key_locks: Arc::clone(&self.key_locks),
            lock_index: Self::lock_index(key),
            temp_path,
            final_path,
            file: Some(file),
            bytes_written: 0,
            hasher: Sha256::new(),
            finished: false,
        }))
    }

    fn list(&self) -> Result<Vec<[u8; 32]>> {
        let mut keys = Vec::new();
        // Compute the stale-tmp cutoff once per sweep so every `.tmp` we
        // examine is judged against the same instant — a file racing with the
        // sweep cannot be deleted on one tick and survive on the next based
        // on clock drift mid-walk.
        let stale_cutoff = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(Self::STALE_TMP_AGE_SECS))
            .unwrap_or(std::time::UNIX_EPOCH);
        Self::walk_dir(&self.base_dir, stale_cutoff, None, &mut keys)?;
        Ok(keys)
    }

    fn list_for_gc(&self, min_age: std::time::Duration) -> Result<Vec<[u8; 32]>> {
        let mut keys = Vec::new();
        let stale_cutoff = std::time::SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(Self::STALE_TMP_AGE_SECS))
            .unwrap_or(std::time::UNIX_EPOCH);
        // F-G9-004: blobs whose payload OR sidecar mtime is newer than this
        // cutoff are excluded from the orphan-blob GC candidate set.
        let min_age_cutoff = std::time::SystemTime::now()
            .checked_sub(min_age)
            .unwrap_or(std::time::UNIX_EPOCH);
        Self::walk_dir(
            &self.base_dir,
            stale_cutoff,
            Some(min_age_cutoff),
            &mut keys,
        )?;
        Ok(keys)
    }
}

/// In-memory blob store for testing.
///
/// Stores `(payload, digest, length)` so behavior matches the file-backed
/// store including digest verification on read paths.
pub struct MemoryBlobStore {
    blobs: Arc<parking_lot::Mutex<std::collections::HashMap<[u8; 32], MemoryEntry>>>,
}

#[derive(Clone)]
struct MemoryEntry {
    data: Vec<u8>,
    sha256: [u8; 32],
}

impl MemoryBlobStore {
    /// Create a new empty in-memory blob store.
    pub fn new() -> Self {
        Self {
            blobs: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        }
    }
}

impl Default for MemoryBlobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobStore for MemoryBlobStore {
    fn put(&self, key: &[u8; 32], data: &[u8]) -> Result<BlobDigest> {
        let sha256 = sha256_of(data);
        let length = data.len() as u64;
        self.blobs.lock().insert(
            *key,
            MemoryEntry {
                data: data.to_vec(),
                sha256,
            },
        );
        Ok(BlobDigest { sha256, length })
    }

    fn get(&self, key: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let blobs = self.blobs.lock();
        match blobs.get(key) {
            Some(entry) => {
                let actual = sha256_of(&entry.data);
                if actual != entry.sha256 {
                    return Err(BlobError::DigestMismatch {
                        key: hex_key(key),
                        expected: entry.sha256,
                        actual,
                    });
                }
                Ok(Some(entry.data.clone()))
            }
            None => Ok(None),
        }
    }

    fn get_range(&self, key: &[u8; 32], offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        // Verify full payload before slicing — see trait docs.
        let data = match self.get(key)? {
            Some(d) => d,
            None => return Ok(None),
        };
        let start = offset as usize;
        if start >= data.len() {
            return Ok(Some(vec![]));
        }
        let end = (offset + length) as usize;
        Ok(Some(data[start..end.min(data.len())].to_vec()))
    }

    fn delete(&self, key: &[u8; 32]) -> Result<()> {
        self.blobs.lock().remove(key);
        Ok(())
    }

    fn exists(&self, key: &[u8; 32]) -> Result<bool> {
        Ok(self.blobs.lock().contains_key(key))
    }

    fn digest(&self, key: &[u8; 32]) -> Result<Option<BlobDigest>> {
        let blobs = self.blobs.lock();
        Ok(blobs.get(key).map(|entry| BlobDigest {
            sha256: entry.sha256,
            length: entry.data.len() as u64,
        }))
    }

    fn stream_to(&self, key: &[u8; 32], writer: &mut dyn std::io::Write) -> Result<u64> {
        let payload = match self.get(key)? {
            Some(d) => d,
            None => return Err(BlobError::NotFound { key: hex_key(key) }),
        };
        writer.write_all(&payload)?;
        Ok(payload.len() as u64)
    }

    fn begin_stream(&self, key: &[u8; 32]) -> Result<Box<dyn BlobStreamWriter>> {
        Ok(Box::new(MemoryStreamWriter {
            key: *key,
            buffer: Vec::new(),
            store: Arc::clone(&self.blobs),
        }))
    }

    fn list(&self) -> Result<Vec<[u8; 32]>> {
        Ok(self.blobs.lock().keys().copied().collect())
    }
}

/// Streaming writer for [`MemoryBlobStore`] that accumulates chunks in memory,
/// then inserts the complete blob into the shared map on finish.
struct MemoryStreamWriter {
    key: [u8; 32],
    buffer: Vec<u8>,
    store: Arc<parking_lot::Mutex<std::collections::HashMap<[u8; 32], MemoryEntry>>>,
}

impl BlobStreamWriter for MemoryStreamWriter {
    fn write_chunk(&mut self, data: &[u8]) -> Result<()> {
        self.buffer.extend_from_slice(data);
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<BlobDigest> {
        let length = self.buffer.len() as u64;
        let sha256 = sha256_of(&self.buffer);
        self.store.lock().insert(
            self.key,
            MemoryEntry {
                data: self.buffer,
                sha256,
            },
        );
        Ok(BlobDigest { sha256, length })
    }

    fn abort(self: Box<Self>) -> Result<()> {
        // Nothing to clean up for the in-memory store; buffer is dropped.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key(n: u8) -> [u8; 32] {
        let mut k = [0u8; 32];
        k[0] = n;
        k
    }

    fn expected_sha(data: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(data);
        let out = h.finalize();
        let mut d = [0u8; 32];
        d.copy_from_slice(&out);
        d
    }

    // -- File blob store tests --

    #[test]
    fn file_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let digest = store.put(&test_key(1), b"hello world").unwrap();
        assert_eq!(digest.length, 11);
        assert_eq!(digest.sha256, expected_sha(b"hello world"));
        let data = store.get(&test_key(1)).unwrap().unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn file_put_writes_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(0xAB);
        store.put(&key, b"payload").unwrap();
        let blob_path = store.blob_path(&key);
        let meta_path = FileBlobStore::meta_path_for(&blob_path);
        assert!(meta_path.exists(), "sidecar must exist after put");
        let meta_bytes = std::fs::read(&meta_path).unwrap();
        assert_eq!(meta_bytes.len(), BLOB_META_SIZE);
        let (sha, len) = decode_meta(&meta_bytes).unwrap();
        assert_eq!(sha, expected_sha(b"payload"));
        assert_eq!(len, b"payload".len() as u64);
    }

    #[test]
    fn file_digest_reads_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(0x4A);
        let digest = store.put(&key, b"digest only").unwrap();

        let observed = store.digest(&key).unwrap().unwrap();
        assert_eq!(observed, digest);
        assert_eq!(observed.sha256, expected_sha(b"digest only"));
        assert_eq!(observed.length, b"digest only".len() as u64);
    }

    #[test]
    fn file_put_delete_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        store.put(&test_key(2), b"data").unwrap();
        store.delete(&test_key(2)).unwrap();
        assert!(store.get(&test_key(2)).unwrap().is_none());
        // Sidecar must also be removed.
        let p = store.blob_path(&test_key(2));
        assert!(!FileBlobStore::meta_path_for(&p).exists());
    }

    #[test]
    fn file_exists_requires_meta() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        assert!(!store.exists(&test_key(3)).unwrap());
        store.put(&test_key(3), b"x").unwrap();
        assert!(store.exists(&test_key(3)).unwrap());

        // Removing the sidecar should make `exists` report false — a
        // payload-without-meta state is not a usable blob.
        let p = store.blob_path(&test_key(3));
        std::fs::remove_file(FileBlobStore::meta_path_for(&p)).unwrap();
        assert!(!store.exists(&test_key(3)).unwrap());
    }

    #[test]
    fn file_get_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        store.put(&test_key(4), b"0123456789").unwrap();
        let range = store.get_range(&test_key(4), 3, 4).unwrap().unwrap();
        assert_eq!(range, b"3456");
    }

    #[test]
    fn file_same_prefix_different_keys() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let mut k1 = [0u8; 32];
        k1[0] = 0xAB;
        k1[1] = 0xCD;
        k1[2] = 1;
        let mut k2 = [0u8; 32];
        k2[0] = 0xAB;
        k2[1] = 0xCD;
        k2[2] = 2;
        store.put(&k1, b"data1").unwrap();
        store.put(&k2, b"data2").unwrap();
        assert_eq!(store.get(&k1).unwrap().unwrap(), b"data1");
        assert_eq!(store.get(&k2).unwrap().unwrap(), b"data2");
    }

    #[test]
    fn file_large_blob() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let data = vec![0x42u8; 10 * 1024 * 1024]; // 10 MB
        let digest = store.put(&test_key(5), &data).unwrap();
        assert_eq!(digest.length, data.len() as u64);
        assert_eq!(digest.sha256, expected_sha(&data));
        let read = store.get(&test_key(5)).unwrap().unwrap();
        assert_eq!(read.len(), data.len());
        assert_eq!(read, data);
    }

    #[test]
    fn file_get_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        assert!(store.get(&test_key(99)).unwrap().is_none());
    }

    #[test]
    fn file_stream_to() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let data = b"stream this data to the writer";
        store.put(&test_key(10), data).unwrap();

        let mut buf = Vec::new();
        let bytes = store.stream_to(&test_key(10), &mut buf).unwrap();
        assert_eq!(bytes, data.len() as u64);
        assert_eq!(buf, data);
    }

    #[test]
    fn file_stream_to_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let mut buf = Vec::new();
        let result = store.stream_to(&test_key(99), &mut buf);
        assert!(matches!(result, Err(BlobError::NotFound { .. })));
    }

    #[test]
    fn file_concurrent_puts() {
        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(FileBlobStore::new(dir.path(), 2));

        let handles: Vec<_> = (0..10u8)
            .map(|i| {
                let s = store.clone();
                std::thread::spawn(move || {
                    let key = test_key(i);
                    let data = vec![i; 1024];
                    s.put(&key, &data).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        for i in 0..10u8 {
            let data = store.get(&test_key(i)).unwrap().unwrap();
            assert_eq!(data, vec![i; 1024]);
        }
    }

    #[test]
    fn file_concurrent_puts_same_key_do_not_corrupt_blob() {
        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(FileBlobStore::new(dir.path(), 2));
        let key = test_key(7);

        let payloads: Vec<Vec<u8>> = (0..16u8).map(|i| vec![i; 1024 + i as usize]).collect();
        let start = std::sync::Arc::new(std::sync::Barrier::new(payloads.len()));
        let handles: Vec<_> = payloads
            .clone()
            .into_iter()
            .map(|data| {
                let s = store.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    start.wait();
                    s.put(&key, &data).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let stored = store.get(&key).unwrap().unwrap();
        assert!(
            payloads.iter().any(|candidate| candidate == &stored),
            "final blob must match one complete writer payload"
        );
        assert!(
            store.digest(&key).unwrap().is_some(),
            "sidecar must remain readable after same-key concurrency"
        );
    }

    #[test]
    fn file_put_non_writable_dir() {
        let store = FileBlobStore::new(Path::new("/nonexistent/path/blobs"), 2);
        let result = store.put(&test_key(1), b"data");
        assert!(result.is_err());
    }

    // -- Integrity tests --

    #[test]
    fn file_get_detects_payload_tampering() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(0x42);
        store.put(&key, b"original payload").unwrap();

        // Mutate one byte of the payload on disk.
        let blob_path = store.blob_path(&key);
        let mut data = std::fs::read(&blob_path).unwrap();
        data[0] ^= 0xFF;
        std::fs::write(&blob_path, &data).unwrap();

        match store.get(&key) {
            Err(BlobError::DigestMismatch {
                expected, actual, ..
            }) => assert_ne!(expected, actual),
            other => panic!("expected DigestMismatch, got {other:?}"),
        }
    }

    #[test]
    fn file_get_detects_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(0x43);
        store.put(&key, b"abcdefghij").unwrap();

        // Truncate the on-disk payload — sidecar still records length 10.
        let blob_path = store.blob_path(&key);
        let truncated = std::fs::read(&blob_path).unwrap()[..5].to_vec();
        std::fs::write(&blob_path, &truncated).unwrap();

        match store.get(&key) {
            Err(BlobError::DigestMismatch { .. }) => {}
            other => panic!("expected DigestMismatch on length mismatch, got {other:?}"),
        }
    }

    #[test]
    fn file_get_missing_sidecar_is_invalid_meta() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(0x44);
        store.put(&key, b"hello").unwrap();
        let blob_path = store.blob_path(&key);
        std::fs::remove_file(FileBlobStore::meta_path_for(&blob_path)).unwrap();

        match store.get(&key) {
            Err(BlobError::InvalidMeta { .. }) => {}
            other => panic!("expected InvalidMeta, got {other:?}"),
        }
    }

    #[test]
    fn file_stream_to_detects_tampering() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(0x45);
        let data = vec![0xABu8; 4096];
        store.put(&key, &data).unwrap();

        let blob_path = store.blob_path(&key);
        let mut on_disk = std::fs::read(&blob_path).unwrap();
        on_disk[100] = 0x00;
        std::fs::write(&blob_path, &on_disk).unwrap();

        let mut sink: Vec<u8> = Vec::new();
        match store.stream_to(&key, &mut sink) {
            Err(BlobError::DigestMismatch { .. }) => {}
            other => panic!("expected DigestMismatch on stream_to, got {other:?}"),
        }
        // No bytes should have been written to the caller after a digest
        // mismatch — verification happens before any write.
        assert!(sink.is_empty(), "stream_to must not emit bytes on mismatch");
    }

    #[test]
    fn file_get_range_detects_tampering_outside_window() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(0x46);
        store.put(&key, b"0123456789abcdef").unwrap();

        // Tamper with a byte outside the requested [0,4) window. A naive
        // partial read would miss this; the trait contract is that the full
        // payload is verified.
        let blob_path = store.blob_path(&key);
        let mut on_disk = std::fs::read(&blob_path).unwrap();
        on_disk[10] = b'Z';
        std::fs::write(&blob_path, &on_disk).unwrap();

        match store.get_range(&key, 0, 4) {
            Err(BlobError::DigestMismatch { .. }) => {}
            other => panic!("expected DigestMismatch on get_range, got {other:?}"),
        }
    }

    #[test]
    fn file_atomic_put_uses_tmp_then_rename() {
        // After a successful put, no `.tmp` file should remain in the
        // directory — the rename must have moved it into place. This is a
        // structural check that the helper is wired up correctly even when
        // we cannot exercise a real crash between fsync and rename.
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(0x47);
        store.put(&key, b"durable").unwrap();

        let blob_path = store.blob_path(&key);
        let parent = blob_path.parent().unwrap();
        for entry in std::fs::read_dir(parent).unwrap() {
            let p = entry.unwrap().path();
            assert!(
                !p.to_string_lossy().ends_with(TMP_SUFFIX),
                "leftover tmp file: {p:?}"
            );
        }
    }

    #[test]
    fn file_blobstore_uses_durable_directory_creation() {
        let source = include_str!("blobstore.rs");
        let calls = source
            .matches("create_dir_all_durable(&self.base_dir, parent)?")
            .count();
        assert!(
            calls >= 2,
            "put() and begin_stream() must fsync newly-created prefix directories"
        );
    }

    #[test]
    fn file_stream_finish_writes_sidecar_and_no_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(0x48);
        let mut writer = store.begin_stream(&key).unwrap();
        writer.write_chunk(b"chunk-").unwrap();
        writer.write_chunk(b"abcdef").unwrap();
        let digest = writer.finish().unwrap();
        assert_eq!(digest.length, 12);
        assert_eq!(digest.sha256, expected_sha(b"chunk-abcdef"));

        let blob_path = store.blob_path(&key);
        assert!(FileBlobStore::meta_path_for(&blob_path).exists());

        let parent = blob_path.parent().unwrap();
        for entry in std::fs::read_dir(parent).unwrap() {
            let p = entry.unwrap().path();
            assert!(
                !p.to_string_lossy().ends_with(TMP_SUFFIX),
                "leftover tmp file: {p:?}"
            );
        }

        // Sidecar matches the streamed digest.
        let read = store.get(&key).unwrap().unwrap();
        assert_eq!(read, b"chunk-abcdef");
    }

    // -- Memory blob store tests --

    #[test]
    fn memory_put_get() {
        let store = MemoryBlobStore::new();
        let digest = store.put(&test_key(1), b"test").unwrap();
        assert_eq!(digest.sha256, expected_sha(b"test"));
        assert_eq!(digest.length, 4);
        assert_eq!(store.get(&test_key(1)).unwrap().unwrap(), b"test");
    }

    #[test]
    fn memory_put_delete_get() {
        let store = MemoryBlobStore::new();
        store.put(&test_key(2), b"data").unwrap();
        store.delete(&test_key(2)).unwrap();
        assert!(store.get(&test_key(2)).unwrap().is_none());
    }

    #[test]
    fn memory_get_range() {
        let store = MemoryBlobStore::new();
        store.put(&test_key(3), b"abcdefgh").unwrap();
        let range = store.get_range(&test_key(3), 2, 3).unwrap().unwrap();
        assert_eq!(range, b"cde");
    }

    // -- Streaming write tests --

    #[test]
    fn file_stream_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(20);
        let mut writer = store.begin_stream(&key).unwrap();
        writer.write_chunk(b"hello ").unwrap();
        writer.write_chunk(b"world").unwrap();
        let digest = writer.finish().unwrap();
        assert_eq!(digest.length, 11);
        assert_eq!(digest.sha256, expected_sha(b"hello world"));
        let data = store.get(&key).unwrap().unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn file_stream_abort_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(21);
        let mut writer = store.begin_stream(&key).unwrap();
        writer.write_chunk(b"partial data").unwrap();
        writer.abort().unwrap();
        assert!(store.get(&key).unwrap().is_none());
    }

    #[test]
    fn file_stream_large_blob() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(22);
        let chunk = vec![0x42u8; 4 * 1024 * 1024]; // 4 MiB chunk
        let mut writer = store.begin_stream(&key).unwrap();
        for _ in 0..5 {
            writer.write_chunk(&chunk).unwrap(); // 20 MiB total
        }
        let digest = writer.finish().unwrap();
        assert_eq!(digest.length, 20 * 1024 * 1024);
        let data = store.get(&key).unwrap().unwrap();
        assert_eq!(data.len(), 20 * 1024 * 1024);
    }

    #[test]
    fn memory_stream_write() {
        let store = MemoryBlobStore::new();
        let key = test_key(23);
        let mut writer = store.begin_stream(&key).unwrap();
        writer.write_chunk(b"chunk1").unwrap();
        writer.write_chunk(b"chunk2").unwrap();
        let digest = writer.finish().unwrap();
        assert_eq!(digest.length, 12);
        assert_eq!(digest.sha256, expected_sha(b"chunk1chunk2"));
        let data = store.get(&key).unwrap().unwrap();
        assert_eq!(data, b"chunk1chunk2");
    }

    #[test]
    fn memory_stream_abort() {
        let store = MemoryBlobStore::new();
        let key = test_key(24);
        let mut writer = store.begin_stream(&key).unwrap();
        writer.write_chunk(b"data").unwrap();
        writer.abort().unwrap();
        assert!(store.get(&key).unwrap().is_none());
    }

    // -- list / GC enumerator tests (R-049) --

    #[test]
    fn file_list_returns_finalised_blobs_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let k1 = test_key(0x60);
        let k2 = test_key(0x61);
        let k3 = test_key(0x62);
        store.put(&k1, b"a").unwrap();
        store.put(&k2, b"bb").unwrap();
        store.put(&k3, b"ccc").unwrap();

        // Half-written blob: payload exists but sidecar is missing — must NOT
        // appear in `list` (matches the `exists` contract).
        let half_key = test_key(0x63);
        let half_path = store.blob_path(&half_key);
        std::fs::create_dir_all(half_path.parent().unwrap()).unwrap();
        std::fs::write(&half_path, b"orphan-payload").unwrap();
        assert!(!FileBlobStore::meta_path_for(&half_path).exists());

        let listed: std::collections::HashSet<[u8; 32]> =
            store.list().unwrap().into_iter().collect();
        assert_eq!(listed.len(), 3);
        assert!(listed.contains(&k1));
        assert!(listed.contains(&k2));
        assert!(listed.contains(&k3));
        assert!(!listed.contains(&half_key));
    }

    #[test]
    fn file_list_skips_meta_and_unrelated_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        let key = test_key(0x70);
        store.put(&key, b"payload").unwrap();

        // Drop a junk file in the same prefix dir — must NOT be returned.
        let parent = store.blob_path(&key).parent().unwrap().to_path_buf();
        std::fs::write(parent.join("README"), b"junk").unwrap();
        std::fs::write(parent.join("not-a-hex-name.dat"), b"nope").unwrap();

        let listed = store.list().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], key);
    }

    #[test]
    fn file_list_sweeps_stale_tmp_files() {
        use std::time::{Duration, SystemTime};
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);

        // Create a directory layout under the prefix tree by putting one
        // legitimate blob — its parent dir is where we stash the .tmp.
        let key = test_key(0x80);
        store.put(&key, b"keep").unwrap();
        let parent = store.blob_path(&key).parent().unwrap().to_path_buf();

        // Stale .tmp: mtime backdated past the cutoff.
        let stale_tmp =
            parent.join("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899.tmp");
        std::fs::write(&stale_tmp, b"interrupted-upload").unwrap();
        let stale_when =
            SystemTime::now() - Duration::from_secs(FileBlobStore::STALE_TMP_AGE_SECS + 60);
        let stale_ft = filetime::FileTime::from_system_time(stale_when);
        filetime::set_file_mtime(&stale_tmp, stale_ft).unwrap();

        // Fresh .tmp: mtime now — must NOT be deleted.
        let fresh_tmp =
            parent.join("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff.tmp");
        std::fs::write(&fresh_tmp, b"in-flight").unwrap();

        // list() runs the sweep as a side effect.
        let listed = store.list().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], key);

        assert!(!stale_tmp.exists(), "stale .tmp must be swept");
        assert!(fresh_tmp.exists(), "fresh .tmp must survive");
    }

    #[test]
    fn memory_list_returns_keys() {
        let store = MemoryBlobStore::new();
        let k1 = test_key(0xAA);
        let k2 = test_key(0xBB);
        store.put(&k1, b"a").unwrap();
        store.put(&k2, b"b").unwrap();

        let listed: std::collections::HashSet<[u8; 32]> =
            store.list().unwrap().into_iter().collect();
        assert_eq!(listed.len(), 2);
        assert!(listed.contains(&k1));
        assert!(listed.contains(&k2));
    }

    #[test]
    fn memory_list_after_delete() {
        let store = MemoryBlobStore::new();
        let k1 = test_key(0xCC);
        store.put(&k1, b"x").unwrap();
        store.delete(&k1).unwrap();
        assert!(store.list().unwrap().is_empty());
    }
}
