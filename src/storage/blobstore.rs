//! Blob store trait and file-based implementation.
//!
//! Large transaction data (> 1 MiB) is stored in an external blob store
//! keyed by txid. The file-based implementation uses a directory tree
//! organized by hash prefix.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlobError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("blob not found: {key}")]
    NotFound { key: String },
}

pub type Result<T> = std::result::Result<T, BlobError>;

/// Format a 32-byte key as a hex string.
fn hex_key(key: &[u8; 32]) -> String {
    key.iter().map(|b| format!("{b:02x}")).collect()
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
    /// Returns the total number of bytes written.
    fn finish(self: Box<Self>) -> Result<u64>;

    /// Abort the stream and clean up any partial data.
    fn abort(self: Box<Self>) -> Result<()>;
}

/// Trait for external blob storage.
pub trait BlobStore: Send + Sync {
    /// Write a blob keyed by txid.
    fn put(&self, key: &[u8; 32], data: &[u8]) -> Result<()>;

    /// Read a blob. Returns None if not found.
    fn get(&self, key: &[u8; 32]) -> Result<Option<Vec<u8>>>;

    /// Read a byte range from a blob.
    fn get_range(&self, key: &[u8; 32], offset: u64, length: u64) -> Result<Option<Vec<u8>>>;

    /// Delete a blob.
    fn delete(&self, key: &[u8; 32]) -> Result<()>;

    /// Check if a blob exists.
    fn exists(&self, key: &[u8; 32]) -> Result<bool>;

    /// Stream a blob to a writer (for large blobs).
    ///
    /// Returns the number of bytes written, or `BlobError::NotFound` if the
    /// blob does not exist.
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
/// base_dir/ab/cd/abcdef01...789a  (full txid hex as filename)
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
}

/// Streaming writer that appends chunks to a temporary file,
/// then atomically renames to the final blob path on finish.
struct FileStreamWriter {
    temp_path: PathBuf,
    final_path: PathBuf,
    file: std::fs::File,
    bytes_written: u64,
}

impl BlobStreamWriter for FileStreamWriter {
    fn write_chunk(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data)?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<u64> {
        self.file.sync_all()?;
        std::fs::rename(&self.temp_path, &self.final_path)?;
        Ok(self.bytes_written)
    }

    fn abort(self: Box<Self>) -> Result<()> {
        drop(self.file);
        let _ = std::fs::remove_file(&self.temp_path);
        Ok(())
    }
}

impl BlobStore for FileBlobStore {
    fn put(&self, key: &[u8; 32], data: &[u8]) -> Result<()> {
        let path = self.blob_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, data)?;
        Ok(())
    }

    fn get(&self, key: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let path = self.blob_path(key);
        match std::fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BlobError::Io(e)),
        }
    }

    fn get_range(&self, key: &[u8; 32], offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let path = self.blob_path(key);
        match std::fs::read(&path) {
            Ok(data) => {
                let start = offset as usize;
                let end = (offset + length) as usize;
                if start >= data.len() {
                    Ok(Some(vec![]))
                } else {
                    let actual_end = end.min(data.len());
                    Ok(Some(data[start..actual_end].to_vec()))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BlobError::Io(e)),
        }
    }

    fn delete(&self, key: &[u8; 32]) -> Result<()> {
        let path = self.blob_path(key);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(BlobError::Io(e)),
        }
    }

    fn exists(&self, key: &[u8; 32]) -> Result<bool> {
        Ok(self.blob_path(key).exists())
    }

    fn stream_to(&self, key: &[u8; 32], writer: &mut dyn std::io::Write) -> Result<u64> {
        let path = self.blob_path(key);
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(BlobError::NotFound { key: hex_key(key) });
            }
            Err(e) => return Err(BlobError::Io(e)),
        };
        let mut reader = std::io::BufReader::new(file);
        let bytes_written = std::io::copy(&mut reader, writer)?;
        Ok(bytes_written)
    }

    fn begin_stream(&self, key: &[u8; 32]) -> Result<Box<dyn BlobStreamWriter>> {
        let final_path = self.blob_path(key);
        let mut temp_path = final_path.as_os_str().to_os_string();
        temp_path.push(".tmp");
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
        }))
    }
}

/// In-memory blob store for testing.
pub struct MemoryBlobStore {
    blobs: Arc<parking_lot::Mutex<std::collections::HashMap<[u8; 32], Vec<u8>>>>,
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
    fn put(&self, key: &[u8; 32], data: &[u8]) -> Result<()> {
        self.blobs.lock().insert(*key, data.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        Ok(self.blobs.lock().get(key).cloned())
    }

    fn get_range(&self, key: &[u8; 32], offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let blobs = self.blobs.lock();
        match blobs.get(key) {
            Some(data) => {
                let start = offset as usize;
                let end = (offset + length) as usize;
                if start >= data.len() {
                    Ok(Some(vec![]))
                } else {
                    Ok(Some(data[start..end.min(data.len())].to_vec()))
                }
            }
            None => Ok(None),
        }
    }

    fn delete(&self, key: &[u8; 32]) -> Result<()> {
        self.blobs.lock().remove(key);
        Ok(())
    }

    fn exists(&self, key: &[u8; 32]) -> Result<bool> {
        Ok(self.blobs.lock().contains_key(key))
    }

    fn stream_to(&self, key: &[u8; 32], writer: &mut dyn std::io::Write) -> Result<u64> {
        let blobs = self.blobs.lock();
        match blobs.get(key) {
            Some(data) => {
                writer.write_all(data)?;
                Ok(data.len() as u64)
            }
            None => Err(BlobError::NotFound { key: hex_key(key) }),
        }
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
    store: Arc<parking_lot::Mutex<std::collections::HashMap<[u8; 32], Vec<u8>>>>,
}

impl BlobStreamWriter for MemoryStreamWriter {
    fn write_chunk(&mut self, data: &[u8]) -> Result<()> {
        self.buffer.extend_from_slice(data);
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<u64> {
        let len = self.buffer.len() as u64;
        self.store.lock().insert(self.key, self.buffer);
        Ok(len)
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

    // -- File blob store tests --

    #[test]
    fn file_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        store.put(&test_key(1), b"hello world").unwrap();
        let data = store.get(&test_key(1)).unwrap().unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn file_put_delete_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        store.put(&test_key(2), b"data").unwrap();
        store.delete(&test_key(2)).unwrap();
        assert!(store.get(&test_key(2)).unwrap().is_none());
    }

    #[test]
    fn file_exists() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBlobStore::new(dir.path(), 2);
        assert!(!store.exists(&test_key(3)).unwrap());
        store.put(&test_key(3), b"x").unwrap();
        assert!(store.exists(&test_key(3)).unwrap());
        store.delete(&test_key(3)).unwrap();
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
        store.put(&test_key(5), &data).unwrap();
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

    // -- Memory blob store tests --

    #[test]
    fn memory_put_get() {
        let store = MemoryBlobStore::new();
        store.put(&test_key(1), b"test").unwrap();
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
        let total = writer.finish().unwrap();
        assert_eq!(total, 11);
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
        let total = writer.finish().unwrap();
        assert_eq!(total, 20 * 1024 * 1024);
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
        let total = writer.finish().unwrap();
        assert_eq!(total, 12);
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
