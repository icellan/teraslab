//! Small helpers shared across `src/index/*` modules.

use std::io;
use std::path::Path;

/// fsync the parent directory of `path` so directory-entry updates (renames,
/// creates) survive a crash.
///
/// Falls back to fsyncing `.` if `path.parent()` is empty (e.g. relative path
/// with no directory component).
pub(crate) fn fsync_parent_dir(path: &Path) -> io::Result<()> {
    // `Path::parent()` returns `Some("")` (an EMPTY path) for a bare relative
    // name like `teraslab-index.snap`, not `None` — so `unwrap_or(".")` did not
    // catch it and `File::open("")` failed with ENOENT, failing every
    // checkpoint for a relative `index_snapshot_path` (issue #13). Treat an
    // empty parent as the current directory, matching the documented behavior.
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
        // failed with ENOENT, failing every checkpoint. It must now fsync the
        // current directory instead and succeed.
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
