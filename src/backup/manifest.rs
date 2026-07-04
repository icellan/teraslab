//! Online-backup manifest — the versioned index of a backup directory.
//!
//! The manifest is written LAST (after every data file is fsynced), so its
//! presence marks a backup complete: a directory without a valid
//! `MANIFEST.json` is a partial/aborted backup and is refused by restore. It
//! records the fence/tail sequence numbers, the device geometry, and a SHA-256
//! over every file so restore can verify integrity before touching the target.

use std::path::Path;

use serde::{Deserialize, Serialize};

/// Current manifest schema version. Restore refuses a manifest it does not
/// recognise.
pub const MANIFEST_VERSION: u32 = 1;

/// The manifest filename inside a backup directory.
pub const MANIFEST_FILE: &str = "MANIFEST.json";

/// A single copied byte range within a store's device image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeEntry {
    /// Store-relative device byte offset the range was read from (and is
    /// written back to on restore).
    pub device_offset: u64,
    /// Length in bytes.
    pub len: u64,
    /// Lowercase-hex SHA-256 of the range bytes.
    pub sha256: String,
}

/// Per-store backup metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreManifest {
    /// Store index (== `device_id`).
    pub store: u8,
    /// 128-bit device identity, lowercase hex.
    pub device_id_hex: String,
    /// Segment size in bytes.
    pub segment_size: u64,
    /// Total segments carved from the store's data region.
    pub segment_count: u32,
    /// The image file (relative to the backup dir) holding this store's ranges
    /// concatenated in list order.
    pub image_file: String,
    /// SHA-256 of the whole image file bytes (the concatenated ranges).
    pub image_sha256: String,
    /// The copied ranges: the allocator header block first, then each used
    /// segment. Restore pwrites each back at its `device_offset`. Each carries
    /// its own SHA-256 so restore can verify a range as it writes it.
    pub ranges: Vec<RangeEntry>,
    /// The fabricated redo file for this store (relative to the backup dir).
    pub redo_file: String,
    /// SHA-256 of the redo file bytes.
    pub redo_sha256: String,
}

/// A backed-up external blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobEntry {
    /// Path relative to the blob-store root.
    pub rel_path: String,
    /// SHA-256 of the blob file bytes.
    pub sha256: String,
    /// Length in bytes.
    pub len: u64,
}

/// The device geometry the backup was taken from. Restore validates this
/// against the target node's config and refuses on mismatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Geometry {
    /// Number of configured device paths.
    pub device_count: usize,
    /// Total device size (bytes) per path.
    pub device_size: u64,
    /// Device I/O alignment.
    pub alignment: usize,
    /// Sub-device split factor per path.
    pub device_split: usize,
    /// Total store count (`device_count * device_split`).
    pub store_count: usize,
}

/// The complete backup manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Schema version ([`MANIFEST_VERSION`]).
    pub manifest_version: u32,
    /// The `teraslab` crate version that produced the backup.
    pub teraslab_version: String,
    /// The checkpoint fence `F`: the `.snap` is taken here and the redo tail
    /// replays `(F, T]`.
    pub fence: u64,
    /// The tail end `T`: the last sequence captured.
    pub tail_end: u64,
    /// Last durable node height at backup time (informational + restore
    /// convenience; also recoverable from replay).
    pub last_durable_height: u32,
    /// Segment storage-engine header version.
    pub seg_header_version: u32,
    /// Redo header version (linear = 2).
    pub redo_header_version: u16,
    /// Device geometry.
    pub geometry: Geometry,
    /// Per-store backup metadata.
    pub stores: Vec<StoreManifest>,
    /// The index snapshot file (relative to the backup dir).
    pub index_snapshot_file: String,
    /// SHA-256 of the index snapshot file bytes.
    pub index_snapshot_sha256: String,
    /// External blobs (empty when the store has no EXTERNAL records).
    pub blobs: Vec<BlobEntry>,
}

/// Errors reading, writing, or validating a manifest.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// I/O error touching the manifest or a referenced file.
    #[error("manifest I/O error at {path}: {source}")]
    Io {
        /// The path involved.
        path: String,
        /// The underlying error.
        source: std::io::Error,
    },
    /// The manifest JSON could not be parsed or serialized.
    #[error("manifest (de)serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    /// The manifest schema version is not supported.
    #[error("unsupported manifest version {found} (this build supports {supported})")]
    UnsupportedVersion {
        /// The version found in the file.
        found: u32,
        /// The version this build supports.
        supported: u32,
    },
    /// A file's SHA-256 did not match the manifest.
    #[error("checksum mismatch for {file}: manifest {expected}, actual {actual}")]
    ChecksumMismatch {
        /// The file whose checksum failed.
        file: String,
        /// The expected (manifest) checksum.
        expected: String,
        /// The actual computed checksum.
        actual: String,
    },
}

impl Manifest {
    /// Serialize this manifest to `<dir>/MANIFEST.json`, written last and
    /// fsynced (tmp + rename + parent-dir fsync) so its appearance is atomic.
    pub fn write(&self, dir: &Path) -> Result<(), ManifestError> {
        let path = dir.join(MANIFEST_FILE);
        let tmp = dir.join(format!("{MANIFEST_FILE}.tmp"));
        let json = serde_json::to_vec_pretty(self)?;
        std::fs::write(&tmp, &json).map_err(|source| ManifestError::Io {
            path: tmp.display().to_string(),
            source,
        })?;
        let f = std::fs::File::open(&tmp).map_err(|source| ManifestError::Io {
            path: tmp.display().to_string(),
            source,
        })?;
        f.sync_all().map_err(|source| ManifestError::Io {
            path: tmp.display().to_string(),
            source,
        })?;
        drop(f);
        std::fs::rename(&tmp, &path).map_err(|source| ManifestError::Io {
            path: path.display().to_string(),
            source,
        })?;
        crate::fsutil::fsync_parent_dir(&path).map_err(|source| ManifestError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Ok(())
    }

    /// Read and version-check the manifest from `<dir>/MANIFEST.json`.
    pub fn read(dir: &Path) -> Result<Self, ManifestError> {
        let path = dir.join(MANIFEST_FILE);
        let bytes = std::fs::read(&path).map_err(|source| ManifestError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        if manifest.manifest_version != MANIFEST_VERSION {
            return Err(ManifestError::UnsupportedVersion {
                found: manifest.manifest_version,
                supported: MANIFEST_VERSION,
            });
        }
        Ok(manifest)
    }

    /// Verify the SHA-256 of every file the manifest references against the
    /// on-disk bytes. Returns the first mismatch as an error.
    pub fn verify_checksums(&self, dir: &Path) -> Result<(), ManifestError> {
        // Index snapshot.
        verify_file(dir, &self.index_snapshot_file, &self.index_snapshot_sha256)?;
        // Per-store image + redo.
        for store in &self.stores {
            verify_file(dir, &store.image_file, &store.image_sha256)?;
            verify_file(dir, &store.redo_file, &store.redo_sha256)?;
        }
        // Blobs.
        for blob in &self.blobs {
            let rel = format!("blobstore/{}", blob.rel_path);
            verify_file(dir, &rel, &blob.sha256)?;
        }
        Ok(())
    }
}

/// Hash a file and compare against `expected` (lowercase hex).
fn verify_file(dir: &Path, rel: &str, expected: &str) -> Result<(), ManifestError> {
    let path = dir.join(rel);
    let bytes = std::fs::read(&path).map_err(|source| ManifestError::Io {
        path: path.display().to_string(),
        source,
    })?;
    let actual = sha256_hex(&bytes);
    if actual != expected {
        return Err(ManifestError::ChecksumMismatch {
            file: rel.to_string(),
            expected: expected.to_string(),
            actual,
        });
    }
    Ok(())
}

/// Lowercase-hex SHA-256 of a byte slice.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    let mut s = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample() -> Manifest {
        Manifest {
            manifest_version: MANIFEST_VERSION,
            teraslab_version: "test".into(),
            fence: 3,
            tail_end: 6,
            last_durable_height: 100,
            seg_header_version: 2,
            redo_header_version: 2,
            geometry: Geometry {
                device_count: 1,
                device_size: 64 * 1024 * 1024,
                alignment: 4096,
                device_split: 1,
                store_count: 1,
            },
            stores: vec![StoreManifest {
                store: 0,
                device_id_hex: "00112233445566778899aabbccddeeff".into(),
                segment_size: 8 * 1024 * 1024,
                segment_count: 7,
                image_file: "store.0.img".into(),
                image_sha256: sha256_hex(&[0u8; 4096]),
                ranges: vec![RangeEntry {
                    device_offset: 0,
                    len: 4096,
                    sha256: sha256_hex(&[0u8; 4096]),
                }],
                redo_file: "redo.0".into(),
                redo_sha256: sha256_hex(b"redo"),
            }],
            index_snapshot_file: "teraslab-index.snap".into(),
            index_snapshot_sha256: sha256_hex(b"snap"),
            blobs: vec![],
        }
    }

    #[test]
    fn round_trip_write_read() {
        let dir = TempDir::new().unwrap();
        let m = sample();
        m.write(dir.path()).unwrap();
        let read = Manifest::read(dir.path()).unwrap();
        assert_eq!(m, read);
    }

    #[test]
    fn read_rejects_unsupported_version() {
        let dir = TempDir::new().unwrap();
        let mut m = sample();
        m.manifest_version = 999;
        m.write(dir.path()).unwrap();
        match Manifest::read(dir.path()) {
            Err(ManifestError::UnsupportedVersion { found, supported }) => {
                assert_eq!(found, 999);
                assert_eq!(supported, MANIFEST_VERSION);
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    #[test]
    fn verify_checksums_detects_mismatch() {
        let dir = TempDir::new().unwrap();
        let m = sample();
        // Write the referenced files with the EXPECTED content...
        std::fs::write(dir.path().join("teraslab-index.snap"), b"snap").unwrap();
        std::fs::write(dir.path().join("redo.0"), b"redo").unwrap();
        std::fs::write(dir.path().join("store.0.img"), [0u8; 4096]).unwrap();
        m.write(dir.path()).unwrap();
        m.verify_checksums(dir.path()).expect("checksums match");

        // ...then corrupt the redo file.
        std::fs::write(dir.path().join("redo.0"), b"XXXX").unwrap();
        match m.verify_checksums(dir.path()) {
            Err(ManifestError::ChecksumMismatch { file, .. }) => assert_eq!(file, "redo.0"),
            other => panic!("expected ChecksumMismatch, got {other:?}"),
        }
    }

    #[test]
    fn missing_manifest_is_an_error() {
        let dir = TempDir::new().unwrap();
        match Manifest::read(dir.path()) {
            Err(ManifestError::Io { .. }) => {}
            other => panic!("expected Io error for missing manifest, got {other:?}"),
        }
    }

    #[test]
    fn sha256_hex_is_lowercase_64_chars() {
        let h = sha256_hex(b"hello");
        assert_eq!(h.len(), 64);
        assert!(
            h.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        // Known vector for "hello".
        assert_eq!(
            h,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }
}
