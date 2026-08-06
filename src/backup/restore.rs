//! Offline restore: lay a backup back down for a normal boot to recover.
//!
//! [`restore`] runs with the server stopped. It validates the manifest version,
//! every file checksum, and the device geometry against the target config, then
//! — holding the single-instance flock so a live server cannot be clobbered —
//! writes each store's device image ranges back at their recorded offsets,
//! installs the fabricated per-store redo files, copies the primary index
//! snapshot and its authoritative `.mined` MinedIndex companion (post-Task-16d
//! mined-state lives only in RAM + this snapshot + the fenced redo tail) and
//! the durable-height file, and restores the blob tree. It does NOT boot an
//! engine: the node's normal startup (snapshot load + redo tail replay) does the
//! recovery, because a backup is exactly a crash-legal image plus its redo tail.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::backup::BackupError;
use crate::backup::manifest::{Manifest, ManifestError, sha256_hex};
use crate::config::ServerConfig;
use crate::device::{AlignedBuf, BlockDevice, DirectDevice};
use crate::instance_lock::{InstanceLock, InstanceLockError};

/// Restore `backup_dir` onto the devices/paths named by `config`. Offline only.
///
/// # Errors
/// * [`BackupError::Manifest`] — the manifest is missing, an unsupported
///   version, or a file checksum failed.
/// * [`BackupError::GeometryMismatch`] — the manifest geometry does not match
///   `config`.
/// * [`BackupError::UnsupportedConfig`] — a live server holds the instance lock.
/// * [`BackupError::Device`] / [`BackupError::Io`] — a device or filesystem
///   write failed.
pub fn restore(backup_dir: &Path, config: &ServerConfig) -> Result<(), BackupError> {
    // 1. Read + version-check the manifest.
    let manifest = Manifest::read(backup_dir)?;
    // 2. Verify every referenced file's checksum before touching a target.
    manifest.verify_checksums(backup_dir)?;
    // 3. Geometry must match the target node's configuration.
    check_geometry(&manifest, config)?;

    // 4. Take the instance lock so we never overwrite a running server's device.
    //    Held for the whole restore (dropped at return).
    let _lock = match InstanceLock::acquire(&config.device_paths[0]) {
        Ok(lock) => lock,
        Err(InstanceLockError::Held { .. }) => {
            return Err(BackupError::UnsupportedConfig(
                "a server holds the instance lock; stop it before restoring".to_string(),
            ));
        }
        Err(e) => return Err(BackupError::Io(std::io::Error::other(e.to_string()))),
    };

    // 5. Reconstruct the store devices exactly as the server does.
    let align = config.device_alignment;
    let store_devices = reconstruct_store_devices(config)?;

    // 6. Write each store's image ranges back at their device offsets.
    for sm in &manifest.stores {
        let dev = store_devices.get(sm.store as usize).ok_or_else(|| {
            BackupError::GeometryMismatch(format!(
                "manifest references store {} but only {} target device(s) exist",
                sm.store,
                store_devices.len()
            ))
        })?;
        let image = std::fs::read(backup_dir.join(&sm.image_file)).map_err(BackupError::Io)?;
        let mut cursor = 0usize;
        for r in &sm.ranges {
            let end = cursor
                .checked_add(r.len as usize)
                .ok_or_else(|| range_overflow(&sm.image_file))?;
            let slice = image
                .get(cursor..end)
                .ok_or_else(|| range_overflow(&sm.image_file))?;
            let actual = sha256_hex(slice);
            if actual != r.sha256 {
                return Err(BackupError::Manifest(ManifestError::ChecksumMismatch {
                    file: sm.image_file.clone(),
                    expected: r.sha256.clone(),
                    actual,
                }));
            }
            let mut buf = AlignedBuf::new(slice.len(), align);
            buf[..slice.len()].copy_from_slice(slice);
            dev.pwrite_all_at(&buf, r.device_offset)?;
            cursor = end;
        }
        dev.sync()?;
    }

    // 7. Install the fabricated per-store redo files.
    for sm in &manifest.stores {
        let mut region = std::fs::read(backup_dir.join(&sm.redo_file)).map_err(BackupError::Io)?;
        // A redo region always carries a full header block + a marker, so it is
        // never shorter than two blocks in practice; zero-extend defensively so
        // `RedoLog::open` always sees at least header*2 bytes.
        let min_len = 2 * align;
        if region.len() < min_len {
            region.resize(min_len, 0);
        }
        let target = redo_path_for_store(config, sm.store);
        write_and_fsync(&target, &region)?;
    }

    // 8. Index snapshot.
    let snap_src = backup_dir.join(&manifest.index_snapshot_file);
    let snap_bytes = std::fs::read(&snap_src).map_err(BackupError::Io)?;
    let primary_snapshot_path = config.resolved_index_snapshot_path();
    write_and_fsync(&primary_snapshot_path, &snap_bytes)?;

    // 8a. Authoritative MinedIndex snapshot — installed as the `.mined` sibling
    //     of the primary snapshot at the EXACT path boot derives (see
    //     `checkpoint::mined_index_snapshot_path`). Post-Task-16d mined-state
    //     lives only in RAM + this snapshot + the fenced redo tail, so without
    //     it a restored node's `recover_mined_index` FATALs (present primary
    //     snapshot, absent `.mined`) and acknowledged mined-state is lost. Its
    //     bytes were already integrity-checked by `verify_checksums` at step 2.
    let mined_snap_src = backup_dir.join(&manifest.mined_index_snapshot_file);
    let mined_snap_bytes = std::fs::read(&mined_snap_src).map_err(BackupError::Io)?;
    let mined_snapshot_path = crate::checkpoint::mined_index_snapshot_path(&primary_snapshot_path);
    write_and_fsync(&mined_snapshot_path, &mined_snap_bytes)?;

    // 9. Durable node height.
    let height_bytes = crate::ops::engine::encode_durable_height(manifest.last_durable_height);
    write_and_fsync(&config.resolved_last_durable_height_path(), &height_bytes)?;

    // 10. Blob tree.
    for blob in &manifest.blobs {
        let src = backup_dir.join("blobstore").join(&blob.rel_path);
        let dst = config.blobstore_path.join(&blob.rel_path);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent).map_err(BackupError::Io)?;
        }
        let bytes = std::fs::read(&src).map_err(BackupError::Io)?;
        std::fs::write(&dst, &bytes).map_err(BackupError::Io)?;
    }

    // 11. Cluster lineage + migration-fence state (P1 stage 3, design §4.3).
    //     A restored image is a DIFFERENT data lineage from whatever this
    //     node held before: every per-shard Full/Subset stamp and every
    //     inbound/outbound migration-fence record refers to the pre-restore
    //     copy, so they are deleted OUTRIGHT (fail-closed — the restored
    //     node boots all-`Subset` and re-earns `Full` through the normal
    //     catch-up / heal machinery), and a FRESH `data_epoch` identity is
    //     stamped so any surviving stale lineage file elsewhere (or a clone
    //     of the pre-restore dir) degrades via the identity mismatch. The
    //     restore runbook documents this.
    let cluster_state_path = config.resolved_cluster_state_path();
    for sidecar in [
        {
            let mut s = cluster_state_path.as_os_str().to_os_string();
            s.push(".inbound");
            PathBuf::from(s)
        },
        {
            let mut s = cluster_state_path.as_os_str().to_os_string();
            s.push(".outbound");
            PathBuf::from(s)
        },
        crate::cluster::lineage::lineage_state_path(&cluster_state_path),
    ] {
        match std::fs::remove_file(&sidecar) {
            Ok(()) => {
                crate::fsutil::fsync_parent_dir(&sidecar).map_err(BackupError::Io)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(BackupError::Io(e)),
        }
    }
    crate::cluster::lineage::stamp_fresh_data_epoch(&crate::cluster::lineage::data_epoch_path(
        &cluster_state_path,
    ))
    .map_err(BackupError::Io)?;

    Ok(())
}

/// Validate manifest geometry against the target config.
fn check_geometry(manifest: &Manifest, config: &ServerConfig) -> Result<(), BackupError> {
    let g = &manifest.geometry;
    let want_stores = config.device_paths.len() * config.device_split;
    if g.device_count != config.device_paths.len()
        || g.device_size != config.device_size
        || g.alignment != config.device_alignment
        || g.device_split != config.device_split
        || g.store_count != want_stores
    {
        return Err(BackupError::GeometryMismatch(format!(
            "manifest {{devices:{}, size:{}, align:{}, split:{}, stores:{}}} \
             != config {{devices:{}, size:{}, align:{}, split:{}, stores:{}}}",
            g.device_count,
            g.device_size,
            g.alignment,
            g.device_split,
            g.store_count,
            config.device_paths.len(),
            config.device_size,
            config.device_alignment,
            config.device_split,
            want_stores,
        )));
    }
    Ok(())
}

/// Open each configured device and carve sub-devices per `device_split`,
/// yielding the flat store device list indexed by store id.
fn reconstruct_store_devices(
    config: &ServerConfig,
) -> Result<Vec<Arc<dyn BlockDevice>>, BackupError> {
    let align = config.device_alignment;
    let mut devices: Vec<Arc<dyn BlockDevice>> = Vec::new();
    for path in &config.device_paths {
        let direct = DirectDevice::open(path, config.device_size, align)?;
        let arc: Arc<dyn BlockDevice> = Arc::new(direct);
        if config.device_split <= 1 {
            devices.push(arc);
        } else {
            for sub in crate::subdevice::split_device(arc, config.device_split)? {
                devices.push(sub as Arc<dyn BlockDevice>);
            }
        }
    }
    Ok(devices)
}

/// The redo file path for store `store`: store 0 is
/// [`ServerConfig::resolved_redo_log_path`]; store `i` appends `.{i}`.
fn redo_path_for_store(config: &ServerConfig, store: u8) -> PathBuf {
    let base = config.resolved_redo_log_path();
    if store == 0 {
        base
    } else {
        let mut s = base.into_os_string();
        s.push(format!(".{store}"));
        PathBuf::from(s)
    }
}

/// Write `bytes` to `path` (creating parent dirs) and fsync the file + parent.
fn write_and_fsync(path: &Path, bytes: &[u8]) -> Result<(), BackupError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(BackupError::Io)?;
    }
    std::fs::write(path, bytes).map_err(BackupError::Io)?;
    let f = std::fs::File::open(path).map_err(BackupError::Io)?;
    f.sync_all().map_err(BackupError::Io)?;
    crate::fsutil::fsync_parent_dir(path).map_err(BackupError::Io)?;
    Ok(())
}

fn range_overflow(image_file: &str) -> BackupError {
    BackupError::GeometryMismatch(format!(
        "image {image_file} is shorter than the sum of its manifest ranges"
    ))
}
