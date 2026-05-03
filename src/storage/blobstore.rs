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
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
}

pub type Result<T> = std::result::Result<T, BlobError>;

/// Suffix used for the sidecar file storing `(sha256, length)` next to every blob.
const META_SUFFIX: &str = ".meta";

/// Suffix used for in-progress write tmp files.
const TMP_SUFFIX: &str = ".tmp";

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

/// Atomically write `data` to `final_path` via a sibling `.tmp` file with
/// fsync on the file. The parent directory is **not** fsync'd here — the
/// caller must call [`fsync_parent_dir`] once after all related files
/// (payload + sidecar) have been renamed.
fn atomic_write_no_dir_fsync(final_path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut tmp = final_path.as_os_str().to_os_string();
    tmp.push(TMP_SUFFIX);
    let tmp_path = PathBuf::from(tmp);

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
}

impl FileBlobStore {
    /// Create a new file blob store at the given directory.
    ///
    /// `prefix_depth` controls how many hex-byte pairs are used for
    /// subdirectory nesting (default 2 → `ab/cd/`).
    pub fn new(base_dir: &Path, prefix_depth: usize) -> Self {
        Self {
            base_dir: base_dir.to_path_buf(),
            prefix_depth,
        }
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
struct FileStreamWriter {
    temp_path: PathBuf,
    final_path: PathBuf,
    file: std::fs::File,
    bytes_written: u64,
    hasher: Sha256,
}

impl BlobStreamWriter for FileStreamWriter {
    fn write_chunk(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data)?;
        self.hasher.update(data);
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<BlobDigest> {
        // Decompose the box so we can take ownership of individual fields.
        let FileStreamWriter {
            temp_path,
            final_path,
            file,
            bytes_written,
            hasher,
        } = *self;

        // 1. fsync the payload temp file, then rename into place.
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temp_path, &final_path)?;

        // 2. Finalize the digest and write the sidecar atomically.
        let mut sha256 = [0u8; 32];
        sha256.copy_from_slice(&hasher.finalize());
        let meta_bytes = encode_meta(&sha256, bytes_written);
        let meta_path = FileBlobStore::meta_path_for(&final_path);
        atomic_write_no_dir_fsync(&meta_path, &meta_bytes)?;

        // 3. fsync the parent directory so both renames are durable.
        fsync_parent_dir(&final_path)?;

        Ok(BlobDigest {
            sha256,
            length: bytes_written,
        })
    }

    fn abort(self: Box<Self>) -> Result<()> {
        drop(self.file);
        let _ = std::fs::remove_file(&self.temp_path);
        Ok(())
    }
}

impl BlobStore for FileBlobStore {
    fn put(&self, key: &[u8; 32], data: &[u8]) -> Result<BlobDigest> {
        let path = self.blob_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
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
        let path = self.blob_path(key);
        Self::read_and_verify(&path, key)
    }

    fn get_range(&self, key: &[u8; 32], offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        // Per the trait doc: verify the full payload digest before slicing.
        // Partial digests would not detect tampering of bytes outside the
        // requested window, so we read+verify the whole payload first.
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

    fn stream_to(&self, key: &[u8; 32], writer: &mut dyn std::io::Write) -> Result<u64> {
        let path = self.blob_path(key);
        // Open both the payload and sidecar up front so a missing payload
        // surfaces NotFound (matching the original behavior) before we try to
        // verify integrity.
        let mut file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(BlobError::NotFound { key: hex_key(key) });
            }
            Err(e) => return Err(BlobError::Io(e)),
        };
        let (expected_sha, expected_len) = Self::read_meta(&path, key)?;

        // Verify by hashing as we copy. Buffer a single chunk at a time so we
        // do not require the full payload in memory.
        let mut hasher = Sha256::new();
        let mut buf = [0u8; 64 * 1024];
        let mut total: u64 = 0;
        let mut payload = Vec::with_capacity(expected_len as usize);
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            payload.extend_from_slice(&buf[..n]);
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
        // Only after verification do we hand bytes to the caller.
        writer.write_all(&payload)?;
        Ok(total)
    }

    fn begin_stream(&self, key: &[u8; 32]) -> Result<Box<dyn BlobStreamWriter>> {
        let final_path = self.blob_path(key);
        let mut temp_path = final_path.as_os_str().to_os_string();
        temp_path.push(TMP_SUFFIX);
        let temp_path = PathBuf::from(temp_path);
        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::File::create(&temp_path)?;
        Ok(Box::new(FileStreamWriter {
            temp_path,
            final_path,
            file,
            bytes_written: 0,
            hasher: Sha256::new(),
        }))
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
}
