//! Single-instance advisory lock.
//!
//! A running server takes an exclusive advisory lock on a sibling lockfile next
//! to its first data device (`<device_paths[0]>.lock`). This lets the offline
//! tools — notably `teraslab-cli restore`, which overwrites the data device —
//! refuse to touch files a live server owns. The lock is advisory
//! (`flock(2)` / `LOCK_EX`), held for the lifetime of the [`InstanceLock`], and
//! released automatically when the guard drops or the process exits (including
//! on crash). It is NOT a substitute for filesystem permissions — it only
//! coordinates cooperating TeraSlab processes.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use thiserror::Error;

/// Errors acquiring the single-instance lock.
#[derive(Debug, Error)]
pub enum InstanceLockError {
    /// The lockfile could not be created or opened.
    #[error("failed to open lockfile {path}: {source}")]
    Open {
        /// The lockfile path.
        path: PathBuf,
        /// The underlying I/O error.
        source: io::Error,
    },
    /// Another process already holds the lock (a server is running).
    #[error("another TeraSlab instance holds the lock on {path} — stop the running server first")]
    Held {
        /// The lockfile path.
        path: PathBuf,
    },
    /// The lock syscall failed for a reason other than contention.
    #[error("flock on {path} failed: {source}")]
    Flock {
        /// The lockfile path.
        path: PathBuf,
        /// The underlying I/O error.
        source: io::Error,
    },
}

/// An held exclusive advisory lock. Dropping it releases the lock (the OS also
/// releases it if the process exits). Keep it alive for the whole server run.
#[derive(Debug)]
pub struct InstanceLock {
    // The open file whose fd carries the advisory lock. Kept solely so the lock
    // stays held; closing the file (drop) releases it.
    _file: File,
    path: PathBuf,
}

impl InstanceLock {
    /// The lockfile path for a given primary data-device path: the device path
    /// with a `.lock` extension appended (e.g. `teraslab-data.dat.lock`).
    pub fn path_for(device_path: &Path) -> PathBuf {
        let mut s = device_path.as_os_str().to_os_string();
        s.push(".lock");
        PathBuf::from(s)
    }

    /// Try to take the exclusive lock for `device_path`, non-blocking. Returns
    /// [`InstanceLockError::Held`] immediately if another process holds it.
    pub fn acquire(device_path: &Path) -> Result<Self, InstanceLockError> {
        let path = Self::path_for(device_path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| InstanceLockError::Open {
                path: path.clone(),
                source,
            })?;

        // SAFETY: `flock` takes a valid open fd (from `file`, alive for this
        // call) and a constant operation; it has no memory effects. The fd
        // remains owned by `file`.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            let ewouldblock = err.raw_os_error() == Some(libc::EWOULDBLOCK)
                || err.raw_os_error() == Some(libc::EAGAIN);
            return Err(if ewouldblock {
                InstanceLockError::Held { path }
            } else {
                InstanceLockError::Flock { path, source: err }
            });
        }

        Ok(Self { _file: file, path })
    }

    /// The lockfile path this guard holds.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn path_for_appends_lock_extension() {
        let p = InstanceLock::path_for(Path::new("/data/teraslab-data.dat"));
        assert_eq!(p, PathBuf::from("/data/teraslab-data.dat.lock"));
    }

    #[test]
    fn acquire_succeeds_on_free_device_and_reports_path() {
        let dir = TempDir::new().unwrap();
        let dev = dir.path().join("teraslab-data.dat");
        let lock = InstanceLock::acquire(&dev).expect("first acquire should succeed");
        assert_eq!(lock.path(), InstanceLock::path_for(&dev).as_path());
    }

    #[test]
    fn second_acquire_is_rejected_while_first_is_held() {
        let dir = TempDir::new().unwrap();
        let dev = dir.path().join("teraslab-data.dat");
        let held = InstanceLock::acquire(&dev).expect("first acquire should succeed");
        match InstanceLock::acquire(&dev) {
            Err(InstanceLockError::Held { path }) => {
                assert_eq!(path, InstanceLock::path_for(&dev));
            }
            other => panic!("expected Held, got {other:?}"),
        }
        drop(held);
        // Once released, a fresh acquire succeeds again.
        InstanceLock::acquire(&dev).expect("acquire after release should succeed");
    }
}
