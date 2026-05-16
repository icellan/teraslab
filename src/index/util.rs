//! Small helpers shared across `src/index/*` modules.

use std::io;
use std::path::Path;

/// fsync the parent directory of `path` so directory-entry updates (renames,
/// creates) survive a crash.
///
/// Falls back to fsyncing `.` if `path.parent()` is empty (e.g. relative path
/// with no directory component).
pub(crate) fn fsync_parent_dir(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    let dir = std::fs::File::open(parent)?;
    dir.sync_all()
}
