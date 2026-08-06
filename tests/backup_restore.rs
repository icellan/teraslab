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
use teraslab::ops::set_mined::SetMinedRequest;
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
fn backup_restore_recovers_mined_state() {
    // P0 regression: online backup must snapshot the authoritative in-RAM
    // MinedIndex so a restored node can recover acknowledged mined-state.
    //
    // Post-Task-16d `set_mined` performs ZERO device writes and (via the direct
    // path) writes no redo — mined-state lives ONLY in the in-RAM MinedIndex and
    // its `.mined` checkpoint snapshot. If the backup captures the primary index
    // snapshot but not the `.mined` companion, restore lays down a primary
    // snapshot with NO `.mined` sibling. Boot's `recover_mined_index` then sees
    // `checkpoint_ever_taken == true` (primary snapshot present) with an absent
    // `.mined` and FATALs — every restore is unbootable, and the acknowledged
    // mined-state is gone. This test drives the REAL boot recovery over the
    // restored artifacts and asserts the mined record comes back MINED.

    // --- Source: segment engine over a file device; write records, mine one.
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
    for i in 1..=4u64 {
        txids.push(write_record(&engine, i));
    }
    // Mine record #1 on the longest chain. This mutates ONLY the in-RAM
    // MinedIndex (no device write, no redo) — the exact state the backup must
    // capture via the `.mined` snapshot.
    let mined_key = TxKey { txid: txids[0] };
    const MINED_BLOCK_ID: u32 = 7;
    const MINED_BLOCK_HEIGHT: u32 = 100;
    const MINED_SUBTREE_IDX: u32 = 3;
    engine
        .set_mined(&SetMinedRequest {
            tx_key: mined_key,
            block_id: MINED_BLOCK_ID,
            block_height: MINED_BLOCK_HEIGHT,
            subtree_idx: MINED_SUBTREE_IDX,
            current_block_height: 1000,
            block_height_retention: 288,
            on_longest_chain: true,
            unset_mined: false,
        })
        .expect("set_mined should succeed");
    // Sanity: the source really holds the mined-state only in RAM.
    let (src_blocks, src_unmined) = engine
        .mined_block_entries(&mined_key)
        .expect("source mined-state present");
    assert_eq!(
        src_unmined, 0,
        "source record is mined on the longest chain"
    );
    assert_eq!(
        src_blocks.iter().map(|b| b.block_id).collect::<Vec<_>>(),
        vec![MINED_BLOCK_ID],
        "source record carries the mined block",
    );

    engine.persist_allocator().unwrap();
    dev.sync().unwrap();

    // --- Online backup.
    let backup_root = TempDir::new().unwrap();
    let target = backup_root.path().join("bk");
    let params = BackupParams {
        throttle_bytes_per_sec: 0,
        min_headroom_segments: 1,
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
    .expect("backup should succeed");

    drop(engine);
    drop(dev);

    // --- Restore into a FRESH set of files.
    let restore_dir = TempDir::new().unwrap();
    let mut rconfig = config.clone();
    rconfig.device_paths = vec![restore_dir.path().join("data.dat")];
    rconfig.redo_log_path = Some(restore_dir.path().join("data.dat.redo"));
    rconfig.index_snapshot_path = restore_dir.path().join("data.dat.snap");

    restore(&target, &rconfig).expect("restore should succeed");

    // Manifest checksums must still verify (covers the new `.mined` file).
    Manifest::read(&target)
        .unwrap()
        .verify_checksums(&target)
        .expect("backup checksums verify");

    // --- Reconstruct the store exactly as boot does, then run the REAL
    //     mined-index recovery over the restored primary + `.mined` snapshots
    //     and the restored fabricated redo tail.
    let rdev: Arc<dyn BlockDevice> =
        Arc::new(DirectDevice::open(&rconfig.device_paths[0], DEVICE_SIZE, ALIGN).unwrap());
    let ralloc = SegmentAllocator::recover(rdev.clone()).expect("recover allocator from header");
    let (rindex, rdah, _flags) =
        ShardedIndex::restore_all(&rconfig.resolved_index_snapshot_path(), 1)
            .expect("restore index snapshot");
    let rengine =
        Engine::new_with_sharded_index(rdev.clone(), rindex, ralloc, StripedLocks::new(256), rdah);

    let redo_dev: Arc<dyn BlockDevice> = Arc::new(
        DirectDevice::open(
            &rconfig.resolved_redo_log_path(),
            config.redo_log_size,
            ALIGN,
        )
        .unwrap(),
    );
    let rlog = Arc::new(Mutex::new(
        RedoLog::open(redo_dev, 0, config.redo_log_size).unwrap(),
    ));

    let primary_snapshot_path = rconfig.resolved_index_snapshot_path();
    let mined_snapshot_path =
        teraslab::checkpoint::mined_index_snapshot_path(&primary_snapshot_path);
    let used_snapshot = rengine
        .recover_mined_index(
            &primary_snapshot_path,
            &mined_snapshot_path,
            std::slice::from_ref(&rlog),
        )
        .expect(
            "mined-index recovery must succeed after restore: the backup must install a \
             `.mined` companion so a present primary snapshot is not FATAL",
        );
    assert!(
        used_snapshot,
        "restore installs a checkpoint-style primary snapshot, so recovery must take the \
         snapshot+redo-tail path (true), not the fresh-boot full replay",
    );

    // --- The record must come back MINED with the exact block tuple.
    let (blocks, unmined_since) = rengine
        .mined_block_entries(&mined_key)
        .expect("mined record must be recoverable after restore");
    assert_eq!(
        unmined_since, 0,
        "recovered record must be mined on the longest chain (unmined_since == 0)",
    );
    assert_eq!(
        blocks.len(),
        1,
        "recovered record carries exactly one block"
    );
    let block_ids: Vec<u32> = blocks.iter().map(|b| b.block_id).collect();
    let block_heights: Vec<u32> = blocks.iter().map(|b| b.block_height).collect();
    let subtree_idxs: Vec<u32> = blocks.iter().map(|b| b.subtree_idx).collect();
    assert_eq!(block_ids, vec![MINED_BLOCK_ID], "recovered block_id");
    assert_eq!(
        block_heights,
        vec![MINED_BLOCK_HEIGHT],
        "recovered block_height"
    );
    assert_eq!(
        subtree_idxs,
        vec![MINED_SUBTREE_IDX],
        "recovered subtree_idx"
    );

    // The other records restore as UNMINED (present, no block entries).
    for txid in &txids[1..] {
        let (b, _u) = rengine
            .mined_block_entries(&TxKey { txid: *txid })
            .expect("non-mined record must still be present after restore");
        assert!(b.is_empty(), "un-mined record must carry no block entries");
    }
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

/// P1 stage 3 (design §4.3) — `restore()` treats the restored image as a NEW
/// data lineage: it deletes the per-shard lineage sidecar and the
/// inbound/outbound migration-fence files OUTRIGHT (fail-closed — the node
/// boots all-Subset and re-earns Full over the intact baseline) and stamps a
/// FRESH `data_epoch` identity, so any pre-restore lineage stamps that
/// somehow survive elsewhere degrade via the identity mismatch.
#[test]
fn restore_deletes_lineage_and_migration_fence_state_and_stamps_fresh_epoch() {
    // --- Source: minimal engine + one record + a backup.
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
    engine.persist_allocator().unwrap();
    dev.sync().unwrap();

    let backup_root = TempDir::new().unwrap();
    let target = backup_root.path().join("bk-lineage");
    let params = BackupParams {
        throttle_bytes_per_sec: 0,
        min_headroom_segments: 1,
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
    .expect("backup should succeed");
    drop(engine);
    drop(dev);

    // --- Restore target with PRE-EXISTING cluster sidecar state, as if the
    //     node had a prior life: lineage + inbound/outbound fences + an old
    //     data-epoch stamp.
    let restore_dir = TempDir::new().unwrap();
    let mut rconfig = config.clone();
    rconfig.device_paths = vec![restore_dir.path().join("data.dat")];
    rconfig.redo_log_path = Some(restore_dir.path().join("data.dat.redo"));
    rconfig.index_snapshot_path = restore_dir.path().join("data.dat.snap");

    let cluster_path = rconfig.resolved_cluster_state_path();
    let sidecar = |suffix: &str| {
        let mut s = cluster_path.as_os_str().to_os_string();
        s.push(suffix);
        std::path::PathBuf::from(s)
    };
    let inbound = sidecar(".inbound");
    let outbound = sidecar(".outbound");
    let lineage = teraslab::cluster::lineage::lineage_state_path(&cluster_path);
    let epoch_path = teraslab::cluster::lineage::data_epoch_path(&cluster_path);
    for p in [&inbound, &outbound, &lineage] {
        std::fs::write(p, b"pre-restore cluster state").unwrap();
    }
    let old_epoch = teraslab::cluster::lineage::stamp_fresh_data_epoch(&epoch_path)
        .expect("pre-restore epoch stamps");

    restore(&target, &rconfig).expect("restore should succeed");

    // §4.3: the lineage and inbound/outbound state files are DELETED.
    assert!(
        !inbound.exists(),
        "restore must delete the inbound migration-fence file"
    );
    assert!(
        !outbound.exists(),
        "restore must delete the outbound migration-state file"
    );
    assert!(!lineage.exists(), "restore must delete the lineage sidecar");
    // §4.3: a FRESH data_epoch is stamped (a different identity from the
    // pre-restore one), durably readable by the next boot.
    let new_epoch = teraslab::cluster::lineage::load_or_create_data_epoch(&epoch_path)
        .expect("restored epoch loads");
    assert_ne!(
        old_epoch, new_epoch,
        "restore must mint a FRESH data-epoch identity"
    );
}

/// §8 review F4 (P0) — `restore()` must ALSO delete the `.topo` topology
/// state and its `.regime-armed` sidecar (while KEEPING `.multinode`, which
/// only pins a peak floor of 2). With `.topo` intact, the restored node
/// boots as committed master of its pre-restore shards and I13(i) re-stamps
/// `Full` over BACKUP-ERA data — serving stale data as authoritative master
/// and advertising itself as a legal promotion target. Deleting `.topo`
/// makes it rejoin, learn topology from peers, master nothing until a
/// commit says so, and re-earn `Full` via catch-up.
#[test]
fn f4_restore_deletes_topology_state_and_armed_marker_keeps_multinode() {
    // --- Source: minimal engine + one record + a backup.
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
    engine.persist_allocator().unwrap();
    dev.sync().unwrap();

    let backup_root = TempDir::new().unwrap();
    let target = backup_root.path().join("bk-topo");
    let params = BackupParams {
        throttle_bytes_per_sec: 0,
        min_headroom_segments: 1,
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
    .expect("backup should succeed");
    drop(engine);
    drop(dev);

    // --- Restore target with a PRE-EXISTING topology identity, as if the
    //     node had a prior clustered life: a valid `.topo` envelope, its
    //     `.regime-armed` marker, and a `.multinode` marker.
    let restore_dir = TempDir::new().unwrap();
    let mut rconfig = config.clone();
    rconfig.device_paths = vec![restore_dir.path().join("data.dat")];
    rconfig.redo_log_path = Some(restore_dir.path().join("data.dat.redo"));
    rconfig.index_snapshot_path = restore_dir.path().join("data.dat.snap");

    let cluster_path = rconfig.resolved_cluster_state_path();
    let topo_path =
        teraslab::cluster::coordinator::topology_state_path_for_cluster_state(&cluster_path);
    let armed_path = {
        let mut s = topo_path.as_os_str().to_os_string();
        s.push(".regime-armed");
        std::path::PathBuf::from(s)
    };
    let multinode_path = {
        let mut s = cluster_path.as_os_str().to_os_string();
        s.push(".multinode");
        std::path::PathBuf::from(s)
    };
    let pre_state = teraslab::cluster::topology::PersistedTopologyState {
        peak_cluster_size: 3,
        committed_term: 7,
        committed_members: vec![
            teraslab::cluster::shards::NodeId(1),
            teraslab::cluster::shards::NodeId(2),
            teraslab::cluster::shards::NodeId(3),
        ],
        committed_voters: vec![teraslab::cluster::shards::NodeId(1)],
        voted_term: 7,
        incarnation: 1,
        committed_voter_ever_seen: vec![teraslab::cluster::shards::NodeId(1)],
        committed_placement_version: 1,
        committed_peak: 3,
        regime_block: Default::default(),
        data_epoch: None,
    };
    std::fs::write(&topo_path, pre_state.serialize_envelope()).unwrap();
    std::fs::write(&armed_path, 1u64.to_le_bytes()).unwrap();
    std::fs::write(&multinode_path, 2u64.to_le_bytes()).unwrap();

    restore(&target, &rconfig).expect("restore should succeed");

    assert!(
        !topo_path.exists(),
        "F4: restore must delete the .topo topology state — an intact one makes the \
         restored node boot as committed master and re-stamp Full over backup-era data",
    );
    assert!(
        !armed_path.exists(),
        "F4: restore must delete the .regime-armed sidecar with the state it guards",
    );
    assert!(
        multinode_path.exists(),
        "F4: restore must KEEP .multinode — it only pins the peak floor of 2, which \
         remains true of the cluster this node rejoins",
    );
}
