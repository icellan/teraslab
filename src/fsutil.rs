//! Filesystem durability helpers shared across modules.
//!
//! Consolidates the "fsync the parent directory after an atomic rename" pattern
//! so the durability discipline is identical everywhere it is needed (index
//! snapshots, cluster topology/peak persistence, …). Previously each subsystem
//! carried its own copy, which let the cluster persist paths drift out of sync
//! and omit the directory fsync entirely (a restart-quorum durability hole).

use std::io;
use std::path::Path;

/// fsync the parent directory of `path` so directory-entry updates (renames,
/// creates) survive a crash.
///
/// `File::create(tmp) → write → sync_all → rename(tmp, path)` makes the file
/// *contents* durable, but on crash-consistent filesystems (ext4 default, XFS,
/// …) the directory-entry update produced by `rename` is not guaranteed durable
/// until the directory itself is fsync'd. Call this after the rename for any
/// state whose survival across a crash is required.
///
/// Falls back to fsyncing `.` if `path.parent()` is empty (e.g. a bare relative
/// name with no directory component) — `Path::parent()` returns `Some("")`, not
/// `None`, for such a path, so a naive `unwrap_or(".")` would reach
/// `File::open("")` and fail with ENOENT (issue #13).
pub(crate) fn fsync_parent_dir(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let dir = std::fs::File::open(parent)?;
    dir.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fsync_parent_dir_handles_bare_relative_path() {
        // Issue #13: a bare relative name (no directory component) has
        // `parent() == Some("")`, which previously reached `File::open("")` and
        // failed with ENOENT. It must now fsync the current directory instead.
        let r = fsync_parent_dir(Path::new("teraslab-index.snap"));
        assert!(
            r.is_ok(),
            "bare relative path must fsync cwd, not ENOENT: {r:?}"
        );
    }

    #[test]
    fn fsync_parent_dir_with_directory_component_still_works() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("snap.bin");
        std::fs::write(&p, b"x").unwrap();
        assert!(fsync_parent_dir(&p).is_ok());
    }
}
