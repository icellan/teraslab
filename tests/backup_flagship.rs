//! I2 (flagship) — a backup taken UNDER concurrent, journaled, mixed client
//! load, then restored offline, must reproduce every record's state exactly.
//!
//! # Sandbox note
//!
//! No sockets: this drives the `Engine`, `run_backup`, and `restore` in-process
//! (the sandbox denies loopback binding). Two threads race: one runs the online
//! backup, the other applies a journaled mixed workload (spend / freeze /
//! unfreeze / setMined / delete) to the SAME live engine while the copier is
//! copying.
//!
//! # Fidelity — this is a TRUE concurrent-journaled-tail restore
//!
//! The restore correctness model is `restored == snapshot(F) + replay(F, T]`.
//! For that to be exercised, the concurrent window's mutations must actually
//! *journal their primary redo intents* so the backup's tee captures them and
//! the fabricated tail replays them on restore. It does: the writer mirrors the
//! dispatch WAL-first path (`RedoLog::append_and_flush(primary op)` then the
//! engine mutation), exactly as `tests/crash_sweep_ops.rs` drives WAL-first
//! recovery. So the tail is genuinely non-empty (asserted: `tail_end > fence`)
//! and the restored index/device state is reconstructed by replaying real teed
//! frames — not fabricated in the test.
//!
//! ## Why it is race-free despite being concurrent
//!
//! A backup captures a consistent prefix at `T`. The verifier's model, however,
//! is built from *every* op the writer applied — some of which may land after
//! `T`. Comparing the full model to the restored (state-at-`T`) engine would be
//! a race. We avoid it WITHOUT any timing assumption: every window op records
//! the redo sequence right after it journals its primary frame, and the
//! reference model is rebuilt by replaying ONLY the ops whose primary frame is
//! `<= T` (`tail_end`) onto a fresh reference engine. That is precisely
//! `snapshot(F) + replay(F, T]`, so the reference equals the restored state for
//! ANY interleaving. A post-`T` write never reaches the image either (the image
//! reflects state at `T` — the tee detaches and the final stall samples `T`
//! under the visibility guard, after Copying), so no post-`T` op can leak in.
//!
//! The single timing-sensitive property is *coverage* (that the tail is
//! non-empty): the copier is throttled so the copy phase overlaps the writer,
//! and the writer only starts once the fence is sampled, so its journaled ops
//! land in `(F, T]`. If that margin ever failed the `tail_end > fence` assertion
//! fires loudly — it is never a silent vacuous pass.
//!
//! Note on the op mix: `create` runs in the pre-fence baseline (captured by the
//! index snapshot + device image); the concurrent WINDOW journals
//! spend/freeze/unfreeze/setMined/delete. Create-in-the-tail replay is covered
//! by `crash_sweep_ops::sweep_create` and backup-2's fabricated-tail tests.

// Only `WorkloadOp` + `StateVerifier` are used here (not `WorkloadGenerator`),
// so the generator's helpers are dead in this binary.
#[allow(dead_code)]
mod workload;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use parking_lot::Mutex;
use tempfile::TempDir;

use teraslab::allocator::BoxedAllocator;
use teraslab::backup::job::run_backup;
use teraslab::backup::{BackupParams, BackupProgress, Manifest};
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, DirectDevice, MemoryDevice};
use teraslab::index::{
    DahBackend, DahIndex, Index, ShardedIndex, TxKey, UnminedBackend, UnminedIndex,
};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::recovery::recover_all_with_allocator;
use teraslab::redo::{RedoLog, RedoOp};
use teraslab::segment_allocator::SegmentAllocator;

use workload::generator::WorkloadOp;
use workload::verifier::StateVerifier;

const ALIGN: usize = 4096;
const SEG: u64 = 1024 * 1024; // 1 MiB segments
const DEVICE_SIZE: u64 = 64 * 1024 * 1024; // 64 MiB → 63 segments of headroom
const REDO_SIZE: u64 = 16 * 1024 * 1024;
const K: u32 = 10; // UTXO slots per baseline record
const RETENTION: u32 = 288; // matches StateVerifier's hard-coded retention
const WINDOW_HEIGHT: u32 = 5000;

fn make_tx_id(n: u32) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[0..4].copy_from_slice(&n.to_le_bytes());
    txid[8..12].copy_from_slice(&(n.wrapping_mul(0x9E37)).to_le_bytes());
    txid[16..18].copy_from_slice(&(n as u16).to_le_bytes());
    txid
}

fn make_utxo_hash(tx_n: u32, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = (vout & 0xFF) as u8;
    h[1] = ((vout >> 8) & 0xFF) as u8;
    h[4..8].copy_from_slice(&tx_n.to_le_bytes());
    h
}

fn spending_data(tx_n: u32, vout: u32) -> [u8; 36] {
    let mut sd = [0u8; 36];
    sd[0..4].copy_from_slice(&(tx_n + 10_000).to_le_bytes());
    sd[32..36].copy_from_slice(&vout.to_le_bytes());
    sd
}

fn key(n: u32) -> TxKey {
    TxKey {
        txid: make_tx_id(n),
    }
}

#[allow(clippy::field_reassign_with_default)]
fn source_config(dir: &std::path::Path) -> ServerConfig {
    let mut cfg = ServerConfig::default();
    cfg.device_paths = vec![dir.join("data.dat")];
    cfg.device_size = DEVICE_SIZE;
    cfg.device_alignment = ALIGN;
    cfg.device_split = 1;
    cfg.redo_log_size = REDO_SIZE;
    cfg.redo_log_path = Some(dir.join("data.dat.redo"));
    cfg.index_snapshot_path = dir.join("data.dat.snap");
    cfg.blobstore_path = dir.join("no-blobstore");
    cfg
}

/// A `WorkloadOp::Create` for record `n` with `K` fresh UTXOs.
fn create_op(n: u32) -> WorkloadOp {
    WorkloadOp::Create {
        tx_id: make_tx_id(n),
        utxo_hashes: (0..K).map(|v| make_utxo_hash(n, v)).collect(),
        is_coinbase: false,
        spending_height: 0,
        is_external: false,
        block_height: 1000,
    }
}

/// The deterministic concurrent-window workload over baseline records
/// `0..n_records`. Every op succeeds by construction (spends target unspent
/// slots, deletes target fully-spent records, freeze/unfreeze are matched).
///
/// * records `0..n_delete` — spend ALL slots, then `Delete`.
/// * records `n_delete..n_delete+n_mixed` — spend slots 0,1,2; a matched
///   freeze/unfreeze on slots 3 and 4; then `SetMined`.
fn build_window(n_delete: u32, n_mixed: u32) -> Vec<WorkloadOp> {
    let mut ops = Vec::new();
    for r in 0..n_delete {
        for v in 0..K {
            ops.push(WorkloadOp::Spend {
                tx_key: key(r),
                offset: v,
                utxo_hash: make_utxo_hash(r, v),
                spending_data: spending_data(r, v),
                current_block_height: WINDOW_HEIGHT,
            });
        }
        ops.push(WorkloadOp::Delete { tx_key: key(r) });
    }
    for r in n_delete..(n_delete + n_mixed) {
        for v in 0..3u32 {
            ops.push(WorkloadOp::Spend {
                tx_key: key(r),
                offset: v,
                utxo_hash: make_utxo_hash(r, v),
                spending_data: spending_data(r, v),
                current_block_height: WINDOW_HEIGHT,
            });
        }
        for v in [3u32, 4u32] {
            ops.push(WorkloadOp::Freeze {
                tx_key: key(r),
                offset: v,
                utxo_hash: make_utxo_hash(r, v),
            });
            ops.push(WorkloadOp::Unfreeze {
                tx_key: key(r),
                offset: v,
                utxo_hash: make_utxo_hash(r, v),
            });
        }
        ops.push(WorkloadOp::SetMined {
            tx_key: key(r),
            block_id: 900_000 + r,
            block_height: WINDOW_HEIGHT,
            current_block_height: WINDOW_HEIGHT,
        });
    }
    ops
}

/// Journal a window op's PRIMARY redo intent (WAL-first, mirroring dispatch and
/// `crash_sweep_ops`), returning the redo sequence AFTER the append. Spends read
/// the live generation to stamp the replay idempotency token, exactly as the
/// dispatch path does.
fn journal_primary(op: &WorkloadOp, engine: &Engine, redo: &Mutex<RedoLog>) -> u64 {
    let redo_op = match op {
        WorkloadOp::Spend {
            tx_key,
            offset,
            utxo_hash,
            spending_data,
            current_block_height,
        } => {
            let meta = engine
                .read_metadata(tx_key)
                .expect("record exists for spend");
            let target_generation = { meta.generation }.wrapping_add(1);
            let new_spent_count = { meta.spent_utxos } + 1;
            RedoOp::SpendV2 {
                tx_key: *tx_key,
                offset: *offset,
                spending_data: *spending_data,
                new_spent_count,
                current_block_height: *current_block_height,
                block_height_retention: RETENTION,
                target_generation,
                updated_at: 0,
                utxo_hash: Some(*utxo_hash),
            }
        }
        WorkloadOp::Freeze {
            tx_key,
            offset,
            utxo_hash,
        } => RedoOp::FreezeV2 {
            tx_key: *tx_key,
            offset: *offset,
            utxo_hash: *utxo_hash,
        },
        WorkloadOp::Unfreeze {
            tx_key,
            offset,
            utxo_hash,
        } => RedoOp::UnfreezeV2 {
            tx_key: *tx_key,
            offset: *offset,
            utxo_hash: *utxo_hash,
        },
        WorkloadOp::SetMined {
            tx_key,
            block_id,
            block_height,
            ..
        } => RedoOp::SetMinedBatch {
            block_id: *block_id,
            block_height: *block_height,
            subtree_idx: 0,
            on_longest_chain: true,
            current_block_height: *block_height,
            block_height_retention: RETENTION,
            unset: false,
            txids: vec![*tx_key],
        },
        WorkloadOp::Delete { tx_key } => {
            let entry = engine.lookup(tx_key).expect("record exists for delete");
            let record_size = { engine.read_metadata(tx_key).expect("meta").record_size } as u64;
            RedoOp::Delete {
                tx_key: *tx_key,
                record_offset: entry.record_offset,
                record_size,
            }
        }
        other => panic!("window contains an op with no journaling recipe: {other:?}"),
    };
    let mut g = redo.lock();
    g.append_and_flush(redo_op)
        .expect("window redo append must succeed");
    g.current_sequence()
}

/// Boot a fresh engine over the RESTORED files exactly as production startup
/// does: recover the segment allocator from its header, load the index
/// snapshot, replay the fabricated redo tail, then build the engine.
fn boot_restored_engine(cfg: &ServerConfig) -> Engine {
    let rdev: Arc<dyn BlockDevice> =
        Arc::new(DirectDevice::open(&cfg.device_paths[0], DEVICE_SIZE, ALIGN).unwrap());
    let mut boxed_alloc: BoxedAllocator =
        Box::new(SegmentAllocator::recover(rdev.clone()).unwrap());

    let (rindex, rdah_index, runmined_index, _flags) =
        ShardedIndex::restore_all(&cfg.resolved_index_snapshot_path(), 1).unwrap();
    let mut rdah = DahBackend::from(rdah_index);
    let mut runmined = UnminedBackend::from(runmined_index);

    let redo_dev: Arc<dyn BlockDevice> = Arc::new(
        DirectDevice::open(&cfg.resolved_redo_log_path(), cfg.redo_log_size, ALIGN).unwrap(),
    );
    let rlog = RedoLog::open(redo_dev, 0, cfg.redo_log_size).unwrap();

    recover_all_with_allocator(
        &*rdev,
        &rlog,
        &rindex,
        &mut rdah,
        &mut runmined,
        Some(&mut boxed_alloc),
    )
    .expect("restored redo tail must replay cleanly");

    Engine::new_with_sharded_index(
        rdev,
        rindex,
        boxed_alloc,
        StripedLocks::new(256),
        rdah,
        runmined,
    )
}

#[test]
fn backup_under_concurrent_journaled_load_restores_and_verifies() {
    let src_dir = TempDir::new().unwrap();
    let config = source_config(src_dir.path());

    // --- Source segment engine over a file device. No redo attached yet, so
    //     the (pre-fence) baseline journals nothing — it is captured by the
    //     index snapshot + device image.
    let dev: Arc<dyn BlockDevice> =
        Arc::new(DirectDevice::open(&config.device_paths[0], DEVICE_SIZE, ALIGN).unwrap());
    let seg = SegmentAllocator::new(dev.clone(), SEG).unwrap();
    let engine = Arc::new(Engine::new(
        dev.clone(),
        Index::new(200_000).unwrap(),
        seg,
        StripedLocks::new(256),
        DahIndex::new(),
        UnminedIndex::new(),
    ));

    // --- Baseline: create records until the store spans a few segments (so the
    //     throttled copy phase lasts long enough to overlap the writer).
    let mut baseline_ops: Vec<WorkloadOp> = Vec::new();
    let mut lv = StateVerifier::new(); // throwaway model, drives the live engine
    let mut next_tx = 0u32;
    while engine
        .backup_view_for(0)
        .expect("segment store")
        .open_segment
        < 2
    {
        let op = create_op(next_tx);
        lv.apply(&op, &engine)
            .expect("baseline create must succeed");
        baseline_ops.push(op);
        next_tx += 1;
    }
    let baseline_count = next_tx;
    assert!(
        baseline_count >= 60,
        "baseline must have enough records for the window, got {baseline_count}"
    );
    engine.persist_allocator().unwrap();
    dev.sync().unwrap();

    // --- Concurrent window: journaled spend/freeze/unfreeze/setMined/delete.
    let n_delete = 12u32;
    let n_mixed = 40u32;
    assert!(n_delete + n_mixed <= baseline_count);
    let window = build_window(n_delete, n_mixed);

    // Attach the live redo log (memory-backed → fast, no fsync). run_backup
    // fences on it and tees onto it; the writer journals onto it.
    let redo_dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(REDO_SIZE, ALIGN).unwrap());
    let redo_log = Arc::new(Mutex::new(RedoLog::open(redo_dev, 0, REDO_SIZE).unwrap()));
    engine.set_redo_log(redo_log.clone());

    let backup_root = TempDir::new().unwrap();
    let target = backup_root.path().join("bk");
    let params = BackupParams {
        // Throttle so the copy phase overlaps the writer (coverage, not
        // correctness — see the module docs).
        throttle_bytes_per_sec: 3 * 1024 * 1024,
        min_headroom_segments: 1,
        abort_headroom_segments: 0,
        ..BackupParams::default()
    };
    let progress = Arc::new(Mutex::new(BackupProgress::default()));
    let cancel = Arc::new(AtomicBool::new(false));
    let blob_pause = Arc::new(AtomicBool::new(false));

    // Backup thread.
    let b_engine = engine.clone();
    let b_progress = progress.clone();
    let b_cancel = cancel.clone();
    let b_blob = blob_pause.clone();
    let b_config = config.clone();
    let b_target = target.clone();
    let backup = std::thread::spawn(move || {
        run_backup(
            &b_engine,
            &b_blob,
            &b_config,
            &params,
            &b_target,
            &b_cancel,
            &b_progress,
        )
    });

    // Writer thread: once the fence is sampled (so its journaled frames land in
    // the teed tail `(F, T]`), apply the journaled window to the live engine,
    // recording each op's post-journal redo sequence.
    let w_engine = engine.clone();
    let w_progress = progress.clone();
    let w_redo = redo_log.clone();
    let writer = std::thread::spawn(move || {
        while w_progress.lock().fence.is_none() {
            std::hint::spin_loop();
        }
        let mut applied: Vec<(WorkloadOp, u64)> = Vec::with_capacity(window.len());
        for op in &window {
            let seq_after = journal_primary(op, &w_engine, &w_redo);
            lv.apply(op, &w_engine)
                .expect("window op must succeed on the live engine");
            applied.push((op.clone(), seq_after));
        }
        applied
    });

    let applied = writer.join().expect("writer thread panicked");
    let manifest = backup
        .join()
        .expect("backup thread panicked")
        .expect("backup must succeed");

    // The concurrent window advanced the fence: the tail is genuinely non-empty
    // (frames were teed and fabricated into the restore redo). This is the
    // coverage guard that the flagship path was exercised, not degenerate.
    assert!(
        manifest.tail_end > manifest.fence,
        "expected a non-empty redo tail (window ops journaled after the fence): \
         fence={} tail_end={}",
        manifest.fence,
        manifest.tail_end
    );
    assert!(
        target.join("MANIFEST.json").exists(),
        "a completed backup writes its manifest"
    );

    // How many window ops the backup actually captured: an op is in the backup
    // iff its primary frame sequence (`seq_after - 1`) is <= tail_end.
    let t = manifest.tail_end;
    let captured = applied
        .iter()
        .filter(|(_, seq_after)| *seq_after <= t + 1)
        .count();
    assert!(
        captured > 0,
        "the backup must have captured at least one window op (tail non-empty)"
    );

    drop(engine);
    drop(dev);

    // --- Offline restore into a FRESH set of files.
    let restore_dir = TempDir::new().unwrap();
    let mut rconfig = config.clone();
    rconfig.device_paths = vec![restore_dir.path().join("data.dat")];
    rconfig.redo_log_path = Some(restore_dir.path().join("data.dat.redo"));
    rconfig.index_snapshot_path = restore_dir.path().join("data.dat.snap");
    teraslab::backup::restore::restore(&target, &rconfig).expect("restore must succeed");

    // Manifest checksums verify on the backup dir.
    Manifest::read(&target)
        .unwrap()
        .verify_checksums(&target)
        .expect("backup checksums verify");

    // --- Boot the restored engine (snapshot load + tail replay).
    let restored = boot_restored_engine(&rconfig);

    // --- Reference model = snapshot(F) + replay(F, T] = baseline + the window
    //     prefix the backup captured (ops with primary frame <= T). Built on a
    //     fresh throwaway engine so the model reflects EXACTLY state-at-T.
    let ref_dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(DEVICE_SIZE, ALIGN).unwrap());
    let ref_engine = Engine::new(
        ref_dev.clone(),
        Index::new(200_000).unwrap(),
        teraslab::allocator::SlotAllocator::new(ref_dev).unwrap(),
        StripedLocks::new(256),
        DahIndex::new(),
        UnminedIndex::new(),
    );
    let mut reference = StateVerifier::new();
    for op in &baseline_ops {
        reference
            .apply(op, &ref_engine)
            .expect("reference baseline create");
    }
    for (op, seq_after) in &applied {
        if *seq_after <= t + 1 {
            reference
                .apply(op, &ref_engine)
                .expect("reference window op");
        }
    }

    // --- The restored (state-at-T) engine must match the reference model with
    //     ZERO mismatches over every record.
    let mismatches = reference.verify_against(&restored);
    assert!(
        mismatches.is_empty(),
        "restored state diverged from the state-at-T reference: {} mismatch(es):\n{}",
        mismatches.len(),
        mismatches
            .iter()
            .take(25)
            .map(|m| m.detail.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );
}
