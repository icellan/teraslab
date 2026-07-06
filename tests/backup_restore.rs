//! Integration: a real online backup → offline restore round-trip over
//! file-backed `DirectDevice`s (restore reopens the files).
//!
//! The source engine writes a set of records into a segment store; a backup is
//! taken; the source is dropped; `restore` lays the artifacts onto a FRESH set
//! of files; then we verify three ways:
//!
//! 1. `Manifest::verify_checksums` passes on the backup directory.
//! 2. Byte level: every copied device range on the restored device is
//!    byte-identical to the same range on the source device.
//! 3. Record level: recovering the allocator + loading the restored index
//!    snapshot + reading each record back yields the original records — proving
//!    the round-trip survives to the record layer (the tail is empty here, so no
//!    redo replay is needed; the restored redo file is separately opened and
//!    confirmed to replay nothing).

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use parking_lot::Mutex;
use tempfile::TempDir;

use teraslab::backup::job::run_backup;
use teraslab::backup::restore::restore;
use teraslab::backup::{BackupParams, BackupProgress, Manifest};
use teraslab::config::ServerConfig;
use teraslab::device::{AlignedBuf, BlockDevice, DirectDevice};
use teraslab::index::{DahIndex, Index, ShardedIndex, TxKey};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::redo::RedoLog;
use teraslab::segment_allocator::SegmentAllocator;

const ALIGN: usize = 4096;
const SEG: u64 = 1024 * 1024; // 1 MiB segments
const DEVICE_SIZE: u64 = 16 * 1024 * 1024; // 16 MiB

#[allow(clippy::field_reassign_with_default)]
fn source_config(dir: &std::path::Path) -> ServerConfig {
    let mut cfg = ServerConfig::default();
    cfg.device_paths = vec![dir.join("data.dat")];
    cfg.device_size = DEVICE_SIZE;
    cfg.device_alignment = ALIGN;
    cfg.device_split = 1;
    cfg.redo_log_size = 4 * 1024 * 1024;
    cfg.redo_log_path = Some(dir.join("data.dat.redo"));
    cfg.index_snapshot_path = dir.join("data.dat.snap");
    cfg.blobstore_path = dir.join("no-blobstore");
    cfg
}

fn write_record(engine: &Engine, n: u64) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[0..8].copy_from_slice(&n.to_le_bytes());
    txid[31] = 0xAA;
    let hashes = [[0x22u8; 32]];
    engine
        .create(&CreateRequest {
            tx_id: txid,
            tx_version: 1,
            locktime: 0,
            fee: n,
            size_in_bytes: 250,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            utxo_hashes: &hashes,
            inputs: None,
            outputs: None,
            inpoints: None,
            is_external: false,
            created_at: 0,
            block_height: 1,
            mined_block_infos: &[],
            frozen: false,
            conflicting: false,
            locked: false,
            external_ref: None,
            parent_txids: &[],
        })
        .expect("create should succeed");
    txid
}

#[test]
fn backup_then_restore_round_trips_device_and_records() {
    // --- Source: build a segment engine over a file device and write records.
    let src_dir = TempDir::new().unwrap();
    let config = source_config(src_dir.path());
    let dev_path = config.device_paths[0].clone();

    let dev: Arc<dyn BlockDevice> =
        Arc::new(DirectDevice::open(&dev_path, DEVICE_SIZE, ALIGN).unwrap());
    let seg = SegmentAllocator::new(dev.clone(), SEG).unwrap();
    let engine = Engine::new(
        dev.clone(),
        Index::new(256).unwrap(),
        seg,
        StripedLocks::new(256),
        DahIndex::new(),
    );

    let mut txids = Vec::new();
    for i in 1..=8u64 {
        txids.push(write_record(&engine, i));
    }
    // Persist the allocator header to disk so the on-disk offset-0 header matches
    // the in-memory header the backup captures — otherwise the byte-level range
    // comparison would trip on a never-written (all-zero) source header.
    engine.persist_allocator().unwrap();
    dev.sync().unwrap();

    // --- Backup into a backup directory.
    let backup_root = TempDir::new().unwrap();
    let target = backup_root.path().join("bk1");
    let params = BackupParams {
        throttle_bytes_per_sec: 0,
        min_headroom_segments: 1,
        // Tiny (16 MiB) test device: keep the mid-run abort floor at 0 so the
        // live headroom monitor never trips on so few virgin segments.
        abort_headroom_segments: 0,
        ..BackupParams::default()
    };
    let cancel = AtomicBool::new(false);
    let progress = Mutex::new(BackupProgress::default());
    let blob_pause = AtomicBool::new(false);

    let manifest = run_backup(
        &engine,
        &blob_pause,
        &config,
        &params,
        &target,
        &cancel,
        &progress,
    )
    .expect("backup should succeed");
    assert!(target.join("MANIFEST.json").exists());
    assert_eq!(manifest.stores.len(), 1);
    assert!(manifest.stores[0].ranges.len() >= 2);

    // Capture the source bytes for every copied range BEFORE dropping the source.
    let expected_ranges: Vec<(u64, Vec<u8>)> = manifest.stores[0]
        .ranges
        .iter()
        .map(|r| {
            let mut buf = AlignedBuf::new(r.len as usize, ALIGN);
            dev.pread_exact_at(&mut buf, r.device_offset).unwrap();
            (r.device_offset, buf[..].to_vec())
        })
        .collect();

    drop(engine);
    drop(dev);

    // --- Restore into a FRESH set of files.
    let restore_dir = TempDir::new().unwrap();
    let mut rconfig = config.clone();
    rconfig.device_paths = vec![restore_dir.path().join("data.dat")];
    rconfig.redo_log_path = Some(restore_dir.path().join("data.dat.redo"));
    rconfig.index_snapshot_path = restore_dir.path().join("data.dat.snap");

    restore(&target, &rconfig).expect("restore should succeed");

    // (1) Manifest checksums verify.
    Manifest::read(&target)
        .unwrap()
        .verify_checksums(&target)
        .expect("backup checksums verify");

    // (2) Byte level: restored device ranges byte-match the source.
    let rdev: Arc<dyn BlockDevice> =
        Arc::new(DirectDevice::open(&rconfig.device_paths[0], DEVICE_SIZE, ALIGN).unwrap());
    for (off, expected) in &expected_ranges {
        let mut buf = AlignedBuf::new(expected.len(), ALIGN);
        rdev.pread_exact_at(&mut buf, *off).unwrap();
        assert_eq!(
            &buf[..],
            &expected[..],
            "restored range at offset {off} does not match the source"
        );
    }

    // (3) Record level: recover allocator + index, read every record back.
    let ralloc = SegmentAllocator::recover(rdev.clone()).expect("recover allocator from header");
    let (rindex, rdah, _flags) =
        ShardedIndex::restore_all(&rconfig.resolved_index_snapshot_path(), 1)
            .expect("restore index snapshot");
    let rengine =
        Engine::new_with_sharded_index(rdev.clone(), rindex, ralloc, StripedLocks::new(256), rdah);
    for txid in &txids {
        let (meta, slots) = rengine
            .read_record_snapshot(&TxKey { txid: *txid })
            .unwrap_or_else(|e| panic!("record {txid:?} must be readable after restore: {e:?}"));
        assert_eq!(
            meta.tx_id, *txid,
            "metadata identity mismatch after restore"
        );
        assert_eq!(slots.len(), 1, "expected exactly one UTXO slot");
    }

    // The restored redo file is a valid linear log with an empty tail
    // (all records predate the fence, so nothing to replay).
    let redo_dev: Arc<dyn BlockDevice> = Arc::new(
        DirectDevice::open(
            &rconfig.resolved_redo_log_path(),
            config.redo_log_size,
            ALIGN,
        )
        .unwrap(),
    );
    let rlog = RedoLog::open(redo_dev, 0, config.redo_log_size).unwrap();
    let entries = rlog.recover().expect("restored redo replays cleanly");
    assert!(
        entries.is_empty(),
        "empty backup tail must replay nothing, got {} entries",
        entries.len()
    );
}

#[test]
fn restore_refuses_geometry_mismatch() {
    // Take a real backup, then attempt restore with a mismatched device size.
    let src_dir = TempDir::new().unwrap();
    let config = source_config(src_dir.path());
    let dev_path = config.device_paths[0].clone();

    let dev: Arc<dyn BlockDevice> =
        Arc::new(DirectDevice::open(&dev_path, DEVICE_SIZE, ALIGN).unwrap());
    let seg = SegmentAllocator::new(dev.clone(), SEG).unwrap();
    let engine = Engine::new(
        dev.clone(),
        Index::new(256).unwrap(),
        seg,
        StripedLocks::new(256),
        DahIndex::new(),
    );
    write_record(&engine, 1);
    dev.sync().unwrap();

    let backup_root = TempDir::new().unwrap();
    let target = backup_root.path().join("bk");
    let params = BackupParams {
        throttle_bytes_per_sec: 0,
        min_headroom_segments: 1,
        // Tiny (16 MiB) test device: keep the mid-run abort floor at 0 so the
        // live headroom monitor never trips on so few virgin segments.
        abort_headroom_segments: 0,
        ..BackupParams::default()
    };
    let cancel = AtomicBool::new(false);
    let progress = Mutex::new(BackupProgress::default());
    let blob_pause = AtomicBool::new(false);
    run_backup(
        &engine,
        &blob_pause,
        &config,
        &params,
        &target,
        &cancel,
        &progress,
    )
    .unwrap();
    drop(engine);
    drop(dev);

    let restore_dir = TempDir::new().unwrap();
    let mut rconfig = config.clone();
    rconfig.device_paths = vec![restore_dir.path().join("data.dat")];
    rconfig.redo_log_path = Some(restore_dir.path().join("data.dat.redo"));
    rconfig.index_snapshot_path = restore_dir.path().join("data.dat.snap");
    // Mismatch the device size.
    rconfig.device_size = DEVICE_SIZE * 2;

    match restore(&target, &rconfig) {
        Err(teraslab::backup::BackupError::GeometryMismatch(msg)) => {
            assert!(msg.contains("size"), "unexpected message: {msg}");
        }
        other => panic!("expected GeometryMismatch, got {other:?}"),
    }
}
