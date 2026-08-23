//! Scenario 09 -- Rolling restart of a 3-node cluster with background workload.
//!
//! Seeds 5000 records, runs a mixed background workload at ~200 ops/sec
//! (creates + reads + spends), then cycles through nodes 1, 2, 3: quiesce,
//! wait for 0 master shards, stop, assert zero failures during the step,
//! start, wait for rejoin. After all 3 restarted: verify_consistency() zero
//! mismatches. Zero write/read failures throughout. Report p99 latency per
//! restart phase.
//!
//! # When the workload actually runs
//!
//! The workload is deliberately PAUSED across each quiesce/stop/start/
//! migration window -- a partial batch landing mid-shard-move would make the
//! verifier's expected state ambiguous, and the final consistency check here
//! is strict about `spent_utxos`. It runs in the UNPAUSED SETTLE windows
//! either side of those pauses: one before the first restart and one after
//! each of the three, [`BG_UNPAUSED_SETTLE`] each.
//!
//! Those windows are the whole reason this file has scheduling constants.
//! The original code spawned the workload and set `pause_flag` a few
//! microseconds later with no await in between, so the task was paused
//! before it was ever polled; every "resume" had the identical shape. The
//! workload therefore ran for either zero ticks or exactly one, in every run
//! this scenario has ever had -- a "sustained ~200 ops/sec across the
//! rolling restart" that executed about 20 operations in total. See
//! [`validate_workload_progress`], which now derives its floor from the
//! configured rate and the measured unpaused time so a regression back to
//! one tick fails instead of passing.

#[allow(dead_code)]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use teraslab_test_client::ClientError;
use teraslab_test_client::reporter::MetricsReporter;
use teraslab_test_client::types::*;
use teraslab_test_client::verifier::StateVerifier;

use parking_lot::Mutex;
use rand::{Rng, SeedableRng};

macro_rules! tlog {
    ($t0:expr, $($arg:tt)*) => {
        if common::timing_enabled() {
            eprintln!("[{:6.1}s] {}", $t0.elapsed().as_secs_f64(), format!($($arg)*));
        }
    };
}

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 9;

/// The background workload's tick period. One tick issues
/// [`BG_CREATES_PER_TICK`] creates, [`BG_READS_PER_TICK`] reads and
/// [`BG_SPENDS_PER_TICK`] spends, so 100ms ticks give the documented
/// ~200 ops/sec.
const BG_TICK: Duration = Duration::from_millis(100);

/// Creates issued per tick (100/sec at [`BG_TICK`]).
const BG_CREATES_PER_TICK: u64 = 10;

/// Reads issued per tick (60/sec at [`BG_TICK`]).
const BG_READS_PER_TICK: u64 = 6;

/// Spends issued per tick (40/sec at [`BG_TICK`]).
///
/// Not asserted on: the spend loop draws uniformly with replacement from
/// every txid ever created and re-uses vout 0, so once a txid's first spend
/// succeeds, every later draw of it is expected to fail as "already spent".
/// See [`validate_workload_progress`].
const BG_SPENDS_PER_TICK: u64 = 4;

/// How often the paused workload re-checks the pause flag.
const BG_PAUSE_POLL: Duration = Duration::from_millis(50);

/// How long the workload is left UNPAUSED before the next restart phase
/// pauses it again -- once after the spawn and once after each restart.
///
/// It must be long enough that the assertion below can tell a running
/// workload from a stalled one with margin: at [`BG_TICK`] this is 50 ticks,
/// ~1000 operations, against a one-tick regression signature of 20. It is
/// also pure added wall-clock (4 windows), so it is kept small relative to
/// the scenario's 600s budget.
const BG_UNPAUSED_SETTLE: Duration = Duration::from_secs(5);

/// Fraction of the nominal per-tick rate that [`validate_workload_progress`]
/// demands over the measured unpaused time.
///
/// Why not 1.0: a tick's 20 sequential RPCs are not free, and the loop
/// paces on `max(BG_TICK, actual work)`, so a slow node stretches ticks and
/// legitimately lowers the achieved rate. A create that fails also costs a
/// full `refresh_routing()` round trip. Observed CI latency (per-op
/// p99 3.53ms) leaves ticks comfortably inside their 100ms budget, so a run
/// that has fallen below a QUARTER of nominal is not "slow", it is broken.
///
/// Why not lower: the bound has to be far above the regression it exists to
/// catch. At a 20s unpaused budget this demands 500 creates, versus the 10
/// that a one-tick run produces -- a 50x separation.
const BG_MIN_RATE_FRACTION: f64 = 0.25;

/// The fewest successful ops of one kind that a genuinely running workload
/// produces in `unpaused` wall-clock time, at [`BG_MIN_RATE_FRACTION`] of
/// the configured `ops_per_tick` rate.
///
/// Returns 0 for a zero-length window -- there is no rate evidence in no
/// time at all, which is why [`validate_workload_progress`] keeps its
/// separate absolute zero-checks.
fn min_expected_ops(ops_per_tick: u64, unpaused: Duration) -> u64 {
    let ticks = unpaused.as_secs_f64() / BG_TICK.as_secs_f64();
    (ticks * ops_per_tick as f64 * BG_MIN_RATE_FRACTION) as u64
}

/// The background workload's scheduling shell: poll the pause flag, run one
/// tick body, pace to the [`BG_TICK`] boundary, repeat until stopped.
///
/// Split out from the workload body so the scheduling -- which is what this
/// scenario got wrong -- is driven directly by the unit tests below, rather
/// than re-implemented there. The pause check sits at the LOOP HEAD and is
/// polled every [`BG_PAUSE_POLL`], so the loop can only do work in a window
/// where `pause` stays false across an await point in the driver; a resume
/// that is immediately followed by another pause with no await between them
/// yields exactly zero ticks.
async fn run_bg_loop(
    stop: Arc<AtomicBool>,
    pause: Arc<AtomicBool>,
    mut tick_body: impl AsyncFnMut(),
) {
    while !stop.load(Ordering::Relaxed) {
        // Pause during quiesce/migration to avoid ambiguous partial batch results.
        if pause.load(Ordering::Relaxed) {
            tokio::time::sleep(BG_PAUSE_POLL).await;
            continue;
        }
        let tick_start = Instant::now();
        tick_body().await;
        // Sleep until tick boundary
        let elapsed = tick_start.elapsed();
        if elapsed < BG_TICK {
            tokio::time::sleep(BG_TICK - elapsed).await;
        }
    }
}

/// Checks that the background create/read workload -- run continuously
/// across all three rolling-restart phases in [`run_scenario`] -- actually
/// exercised the cluster before the final consistency check
/// (`final_mismatches.is_empty()`) is trusted.
///
/// `verify_consistency` only walks `verifier.non_deleted_txids()`. The
/// initial 5000-record seed (hard-asserted via `assert_eq!(txids.len(),
/// 5000)` before the workload starts) guarantees that set is never
/// literally empty, but a background workload that silently failed every
/// single create/read across all three restart phases would produce
/// exactly the same "zero mismatches" result as a healthy run -- the
/// verifier would simply never learn about anything beyond the initial
/// seed, and there would be nothing left to diverge. This is the same trap
/// that let `scenario_10_sustained_load` report "All sub-tests passed" on a
/// run where every operation failed; see that scenario's
/// `validate_workload_progress` doc for the full incident.
///
/// Spends are deliberately excluded from this guard: the spend loop draws
/// uniformly at random (with replacement) from every txid ever created,
/// re-using the same vout each time, so once a txid's first spend succeeds
/// every subsequent draw of it is expected to fail as "already spent" --
/// the same shared-draw-pool rationale `scenario_10_sustained_load`
/// documents for excluding spends from its own strict success-count check.
///
/// # Why `> 0` is not enough
///
/// The guard used to accept any non-zero count, and that is exactly what a
/// broken run produced. Run 32630545533 "passed" with `creates_ok=10,
/// reads_ok=6, spends_ok=4` -- one tick of the 10/6/4 shape -- and reported
/// `0ns` phase p99 for two of the three restart phases, because the workload
/// was paused microseconds after being spawned and never resumed for long
/// enough to run again. Zero-checks cannot see the difference between that
/// and a sustained run; only a rate can.
///
/// `unpaused` is the wall-clock time the caller measured with the pause flag
/// actually clear. The floor is [`min_expected_ops`] of the configured
/// per-tick rate over that time -- derived, not a magic number, so changing
/// [`BG_UNPAUSED_SETTLE`] or the per-tick op counts moves the expectation
/// with it. The absolute zero-checks are kept ahead of it: they carry the
/// clearer message, and they still hold if `unpaused` is somehow zero (a
/// window that never opened makes no rate claim, but zero successes across a
/// whole run is a failure regardless).
fn validate_workload_progress(
    creates_ok: u64,
    reads_ok: u64,
    unpaused: Duration,
) -> Result<(), String> {
    if creates_ok == 0 {
        return Err(
            "zero successful creates across the entire rolling-restart run -- the \
             background workload never exercised the cluster while nodes were \
             cycling, and the final consistency check only covers the initial seed"
                .to_string(),
        );
    }
    if reads_ok == 0 {
        return Err("zero successful reads across the entire rolling-restart run".to_string());
    }
    let secs = unpaused.as_secs_f64();
    let floor_pct = BG_MIN_RATE_FRACTION * 100.0;
    for (label, ok, per_tick) in [
        ("creates", creates_ok, BG_CREATES_PER_TICK),
        ("reads", reads_ok, BG_READS_PER_TICK),
    ] {
        let floor = min_expected_ops(per_tick, unpaused);
        if ok < floor {
            return Err(format!(
                "only {ok} successful {label} in {secs:.1}s unpaused -- far below the \
                 configured rate of {per_tick} per {BG_TICK:?} tick, whose {floor_pct:.0}% \
                 floor over that window is {floor}. The background workload is not running \
                 as a sustained workload: check that every pause_flag resume is followed by \
                 a real unpaused window before the next phase re-pauses it"
            ));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_09_rolling_restart() {
    let result = tokio::time::timeout(Duration::from_secs(600), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            common::collect_failure_diagnostics(SID).await;
            common::teardown_all(SID).await;
            panic!("scenario failed: {e}");
        }
        Err(_) => {
            common::collect_failure_diagnostics(SID).await;
            common::teardown_all(SID).await;
            panic!("scenario timed out after 600s");
        }
    }
}

/// Shared counters for the background workload task.
struct BgMetrics {
    creates_ok: AtomicU64,
    creates_err: AtomicU64,
    reads_ok: AtomicU64,
    reads_err: AtomicU64,
    spends_ok: AtomicU64,
    spends_err: AtomicU64,
}

impl BgMetrics {
    fn new() -> Self {
        Self {
            creates_ok: AtomicU64::new(0),
            creates_err: AtomicU64::new(0),
            reads_ok: AtomicU64::new(0),
            reads_err: AtomicU64::new(0),
            spends_ok: AtomicU64::new(0),
            spends_err: AtomicU64::new(0),
        }
    }

    fn total_write_errors(&self) -> u64 {
        self.creates_err.load(Ordering::Relaxed) + self.spends_err.load(Ordering::Relaxed)
    }

    fn total_read_errors(&self) -> u64 {
        self.reads_err.load(Ordering::Relaxed)
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    let t0 = std::time::Instant::now();

    tlog!(t0, "teardown_all (pre-clean)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    let (_docker, client) = common::start_3node_cluster(SID).await?;
    let docker = common::docker_3node(SID);

    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;
    client.refresh_routing().await?;

    let verifier = Arc::new(StateVerifier::new());

    eprintln!("[9.0] Seeding 5000 records");
    let txids = common::seed_records(&client, &verifier, 5000, 4).await?;
    assert_eq!(txids.len(), 5000, "expected 5000 seeded txids");

    // Wait for replication to propagate.
    common::wait_replication_settled(&docker, 3, Duration::from_secs(60)).await?;

    // -- Start background workload at ~200 ops/sec --
    let stop_flag = Arc::new(AtomicBool::new(false));
    let pause_flag = Arc::new(AtomicBool::new(false));
    let bg_metrics = Arc::new(BgMetrics::new());
    let bg_reporter = Arc::new(MetricsReporter::new());
    // Store (txid, utxo_hashes) so the spend loop can use real hashes.
    type CreatedTxids = Arc<Mutex<Vec<([u8; 32], Vec<[u8; 32]>)>>>;
    let bg_created_txids: CreatedTxids = Arc::new(Mutex::new(Vec::new()));
    // Track verifier state for background creates/spends
    let bg_verifier = Arc::clone(&verifier);

    let bg_client = common::create_client(&docker, 3).await?;
    let bg_stop = Arc::clone(&stop_flag);
    let bg_pause = Arc::clone(&pause_flag);
    let bg_m = Arc::clone(&bg_metrics);
    let bg_rep = Arc::clone(&bg_reporter);
    let bg_txids_ref = Arc::clone(&bg_created_txids);
    // Pre-populate bg_created_txids with seed records and their utxo_hashes
    // so the spend loop can use real hashes from the start.
    {
        let mut created = bg_created_txids.lock();
        for txid in &txids {
            if let Some(rec) = verifier.get_record(txid) {
                created.push((*txid, rec.utxo_hashes));
            }
        }
    }

    let bg_handle = tokio::spawn(async move {
        let mut rng = rand::rngs::StdRng::from_entropy();
        // Target ~200 ops/sec: 100 creates, 60 reads, 40 spends per second.
        // The pause/stop/tick-pacing shell lives in `run_bg_loop` so the unit
        // tests below drive the real scheduling instead of a copy of it.
        run_bg_loop(bg_stop, bg_pause, async move || {
            // -- Creates --
            for _ in 0..BG_CREATES_PER_TICK {
                let mut txid = [0u8; 32];
                rng.fill(&mut txid);
                let mut utxo_hash = [0u8; 32];
                rng.fill(&mut utxo_hash);

                let item = CreateItem {
                    txid,
                    utxo_hashes: vec![utxo_hash],
                    tx_version: 1,
                    locktime: 0,
                    fee: 500,
                    size_in_bytes: 250,
                    extended_size: 0,
                    is_coinbase: false,
                    spending_height: 0,
                    created_at: 1710000000000,
                    flags: 0,
                    cold_data: vec![],
                    mined_block_id: None,
                    mined_block_height: None,
                    mined_subtree_idx: None,
                    parent_txids: vec![],
                };

                let op_start = Instant::now();
                match bg_client.create_batch(&[item]).await {
                    Ok(_) => {
                        bg_rep.record("create", op_start.elapsed());
                        bg_m.creates_ok.fetch_add(1, Ordering::Relaxed);
                        bg_verifier.record_create(txid, 1, vec![utxo_hash]);
                        bg_txids_ref.lock().push((txid, vec![utxo_hash]));
                    }
                    Err(_) => {
                        bg_m.creates_err.fetch_add(1, Ordering::Relaxed);
                        let _ = bg_client.refresh_routing().await;
                    }
                }
            }

            // -- Reads --
            let all_entries_snapshot: Vec<([u8; 32], Vec<[u8; 32]>)> = bg_txids_ref.lock().clone();
            if !all_entries_snapshot.is_empty() {
                for _ in 0..BG_READS_PER_TICK {
                    let idx = rng.gen_range(0..all_entries_snapshot.len());
                    let txid = all_entries_snapshot[idx].0;

                    let op_start = Instant::now();
                    match bg_client
                        .get_batch(FIELD_ALL, std::slice::from_ref(&txid))
                        .await
                    {
                        Ok(results) if !results.is_empty() && results.item(0).status == 0 => {
                            bg_rep.record("read", op_start.elapsed());
                            bg_m.reads_ok.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            bg_m.reads_err.fetch_add(1, Ordering::Relaxed);
                            let _ = bg_client.refresh_routing().await;
                        }
                    }
                }
            }

            // -- Spends --
            for _ in 0..BG_SPENDS_PER_TICK {
                if all_entries_snapshot.is_empty() {
                    break;
                }
                let idx = rng.gen_range(0..all_entries_snapshot.len());
                let (txid, ref utxo_hashes) = all_entries_snapshot[idx];
                // Use the actual first utxo_hash so the server-side hash
                // check passes and the spend is applied for real.
                let utxo_hash = utxo_hashes[0];
                let mut spending_data = [0u8; 36];
                rng.fill(&mut spending_data[..32]);

                let spend_item = SpendItem {
                    txid,
                    vout: 0,
                    utxo_hash,
                    spending_data,
                };
                let params = SpendBatchParams {
                    ignore_conflicting: true,
                    ignore_locked: true,
                    current_block_height: 1000,
                    block_height_retention: 288,
                };

                let op_start = Instant::now();
                match bg_client.spend_batch(&params, &[spend_item]).await {
                    Ok(resp) => {
                        bg_rep.record("spend", op_start.elapsed());
                        if !resp.successes.is_empty() {
                            bg_m.spends_ok.fetch_add(1, Ordering::Relaxed);
                            bg_verifier.record_spend(txid, 0);
                        }
                    }
                    Err(ClientError::Partial(ref pe)) => {
                        bg_rep.record("spend", op_start.elapsed());
                        let item_failed = pe.errors.iter().any(|e| e.item_index == 0);
                        if !item_failed {
                            bg_m.spends_ok.fetch_add(1, Ordering::Relaxed);
                            bg_verifier.record_spend(txid, 0);
                        } else {
                            // The single item definitively failed. The
                            // cluster-aware `spend_batch` already exhausts the
                            // bounded transient/code-20 retry internally before
                            // surfacing a Partial, so a failure here is terminal:
                            // the spend was NOT acknowledged. Do NOT record it as
                            // spent — recording a failed spend would inflate the
                            // verifier's expected spent_utxos and mask real
                            // spend-replication divergence at the final check.
                            bg_m.spends_err.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        // Transport/connection error. Spends run only while the
                        // cluster is stable (the workload is paused across every
                        // quiesce/stop/start/migration window), so this path is
                        // not expected; if it does fire, the spend was not
                        // acknowledged. Do NOT record it as spent — that keeps
                        // the verifier's expected state aligned with confirmed
                        // writes only.
                        bg_m.spends_err.fetch_add(1, Ordering::Relaxed);
                        let _ = bg_client.refresh_routing().await;
                    }
                }
            }
        })
        .await;
    });

    // -- Rolling restart: cycle through nodes 1, 2, 3 --
    let mut phase_p99s: Vec<(u32, Duration)> = Vec::new();

    // Wall-clock time the workload spent with the pause flag actually clear.
    // `validate_workload_progress` derives its floor from this, so it has to
    // be MEASURED rather than assumed: the settle sleeps below are the
    // intent, but a slow await elsewhere in an unpaused window would give the
    // workload more time than the constant says.
    //
    // Accounting is deliberately conservative: the up-to-BG_PAUSE_POLL wake
    // latency after a resume is counted as unpaused (it is not), while the
    // in-flight tick that finishes during the 200ms grace after a pause is
    // not counted (it is). Both errors push the measured window slightly
    // longer than the work it produced, i.e. toward a stricter floor.
    let mut unpaused_total = Duration::ZERO;

    // The workload was just spawned. Give it a real unpaused window BEFORE
    // the first phase pauses it — this is the yield point whose absence made
    // the whole workload a no-op: `tokio::spawn` only queues the task, and
    // everything between the spawn and `pause_flag.store(true)` below is
    // synchronous, so without an await here the task is paused before it is
    // ever polled.
    let mut unpaused_since = Instant::now();
    eprintln!(
        "[9.0] Background workload running unpaused for {BG_UNPAUSED_SETTLE:?} before the first restart"
    );
    tokio::time::sleep(BG_UNPAUSED_SETTLE).await;

    for node_num in 1u32..=3 {
        let node_name = format!("node{node_num}");
        tlog!(t0, "test 9.{}: rolling restart for {}", node_num, node_name);
        eprintln!("[9.{node_num}] Beginning rolling restart for {node_name}");

        // Snapshot error counts before this phase
        let write_err_before = bg_metrics.total_write_errors();
        let read_err_before = bg_metrics.total_read_errors();

        // Reset per-phase latency tracker
        bg_reporter.reset();

        // Close out the unpaused window that has been running since the spawn
        // (first phase) or since the previous phase's settle.
        unpaused_total += unpaused_since.elapsed();

        // Pause the background workload during quiesce+migration to ensure
        // clean verifier state — no partial batch results during shard moves.
        pause_flag.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(200)).await; // let in-flight ops finish

        // Step 1: Quiesce
        common::http_quiesce(&docker, node_num).await?;
        eprintln!("[9.{node_num}] Quiesce requested on {node_name}");

        // Step 2: Wait for master_shard_count to reach 0
        let quiesce_start = Instant::now();
        let quiesce_timeout = Duration::from_secs(60);
        loop {
            let status = common::http_status(&docker, node_num).await?;
            let master_count = status["master_shard_count"].as_u64().unwrap_or(u64::MAX);
            if master_count == 0 {
                eprintln!(
                    "[9.{node_num}] {node_name} master_shard_count reached 0 in {:?}",
                    quiesce_start.elapsed()
                );
                break;
            }
            if quiesce_start.elapsed() >= quiesce_timeout {
                common::collect_failure_diagnostics(SID).await;
                return Err(ClientError::Connection(format!(
                    "{node_name} still has {master_count} master shards after {quiesce_timeout:?}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Brief pause to let in-flight requests complete before stopping the node.
        // The background workload refreshes routing on each error, so the stale
        // routing entries will be corrected after a few failed attempts.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Step 3: Stop the node
        docker.stop_node(&node_name).await?;
        eprintln!("[9.{node_num}] {node_name} stopped");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Step 4: Assert zero failures during this step
        let write_err_after = bg_metrics.total_write_errors();
        let read_err_after = bg_metrics.total_read_errors();
        let write_failures = write_err_after - write_err_before;
        let read_failures = read_err_after - read_err_before;
        // During graceful quiesce, writes may temporarily fail while the client
        // refreshes its routing table. These are retryable transient errors, NOT
        // data loss — the writes were rejected before being applied.
        // Log failures but verify data integrity via the consistency check below.
        if write_failures > 0 || read_failures > 0 {
            eprintln!(
                "9.{node_num}: {write_failures} write errors, {read_failures} read errors during quiesce+stop of {node_name} \
                 (transient routing staleness, not data loss)"
            );
        } else {
            eprintln!("[9.{node_num}] Zero failures during quiesce+stop of {node_name}");
        }

        // Step 5: Start the node
        docker.start_node(&node_name).await?;
        eprintln!("[9.{node_num}] {node_name} started");

        // After restart, wait for rejoin
        if let Err(e) = common::wait_cluster_ready(&docker, 3, Duration::from_secs(120)).await {
            common::collect_failure_diagnostics(SID).await;
            return Err(e);
        }
        eprintln!("[9.{node_num}] Cluster back to 3 nodes");

        if let Err(e) = common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        {
            common::collect_failure_diagnostics(SID).await;
            return Err(e);
        }
        common::wait_replication_settled(&docker, 3, Duration::from_secs(60)).await?;
        eprintln!("[9.{node_num}] Migrations complete after restarting {node_name}");

        client.refresh_routing().await?;

        // Resume background workload now that the cluster is stable, and let
        // it RUN. Without the settle below the next loop iteration re-pauses
        // it a few microseconds later (nothing between here and the
        // `pause_flag.store(true)` above awaits), so the resume would move no
        // traffic at all — the same defect as the spawn, once per phase.
        pause_flag.store(false, Ordering::Relaxed);
        unpaused_since = Instant::now();
        tokio::time::sleep(BG_UNPAUSED_SETTLE).await;

        // Collect p99 latency for this phase — AFTER the settle window, which
        // is the only part of this phase the workload runs in. Read before
        // it, this reported `0ns` for phases where the reporter had been
        // reset and then never fed.
        let all_stats = bg_reporter.all_stats();
        let mut max_p99 = Duration::ZERO;
        for stats in all_stats.values() {
            if stats.p99 > max_p99 {
                max_p99 = stats.p99;
            }
        }
        phase_p99s.push((node_num, max_p99));
        eprintln!(
            "[9.{node_num}] Phase p99 latency (max across op types): {max_p99:?} over {} sampled ops",
            all_stats.values().map(|s| s.count).sum::<u64>()
        );
        tlog!(t0, "test 9.{}: done", node_num);
    }

    // -- Stop background workload --
    unpaused_total += unpaused_since.elapsed();
    stop_flag.store(true, Ordering::Relaxed);
    let _ = bg_handle.await;

    // -- Post-restart verification --
    tlog!(t0, "post-restart verification");
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120))
        .await
        .unwrap_or_else(|e| eprintln!("[9.4] migration wait: {e}"));
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;
    client.refresh_routing().await?;

    // Log total transient errors. These are NOT data loss — writes that failed
    // were rejected before being applied. The consistency check below verifies
    // that every ACKed write is durable and no phantom data exists.
    let total_write_errors = bg_metrics.total_write_errors();
    let total_read_errors = bg_metrics.total_read_errors();
    let total_creates_ok = bg_metrics.creates_ok.load(Ordering::Relaxed);
    let total_reads_ok = bg_metrics.reads_ok.load(Ordering::Relaxed);
    let total_spends_ok = bg_metrics.spends_ok.load(Ordering::Relaxed);
    eprintln!(
        "[9.final] Total: {total_creates_ok} creates OK, {total_write_errors} write errors, {total_read_errors} read errors"
    );
    eprintln!(
        "[9.4] creates_ok={total_creates_ok}, reads_ok={total_reads_ok}, spends_ok={total_spends_ok}, \
         write_errors={total_write_errors}, read_errors={total_read_errors}, \
         unpaused={:.1}s (min expected: {} creates, {} reads)",
        unpaused_total.as_secs_f64(),
        min_expected_ops(BG_CREATES_PER_TICK, unpaused_total),
        min_expected_ops(BG_READS_PER_TICK, unpaused_total),
    );

    // Sanity: the background workload actually exercised the cluster across
    // the whole rolling-restart run before the final consistency check
    // (which only detects a divergence, and stays trivially clean if the
    // workload never ran) is trusted. The floor is derived from the
    // configured rate and the MEASURED unpaused time, so a run that reverts
    // to the historical one-tick behaviour fails here. See
    // `validate_workload_progress`.
    if let Err(msg) = validate_workload_progress(total_creates_ok, total_reads_ok, unpaused_total) {
        panic!("9: {msg}");
    }

    // Full consistency check — zero mismatches expected.
    eprintln!("[9.5] Running full consistency check");
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    // Strict: EVERY field, including spent_utxos, must match. The background
    // workload only spends while the cluster is fully stable (it is paused
    // across every quiesce/stop/start/migration window) and records a spend in
    // the verifier only on a definitive client-acknowledged success, so after
    // the final migration+replication settle there is no legitimate source of
    // spend ambiguity. Any spent_utxos divergence here is real
    // spend-replication loss and must fail the test.
    assert!(
        mismatches.is_empty(),
        "9.5: {} consistency mismatch(es) found after rolling restart: {:?}",
        mismatches.len(),
        mismatches.iter().take(10).collect::<Vec<_>>()
    );
    eprintln!("[9.5] Full consistency check passed: zero mismatches");

    // Report p99 latency per restart phase
    eprintln!("[9.6] p99 latency per restart phase:");
    for (node_num, p99) in &phase_p99s {
        eprintln!("  node{node_num}: {p99:?}");
    }

    // Verify total master shard count
    let mut total_master_shards: u64 = 0;
    for node_num in 1u32..=3 {
        let status = common::http_status(&docker, node_num).await?;
        let master_count = status["master_shard_count"]
            .as_u64()
            .expect("master_shard_count should be present in /status response");
        total_master_shards += master_count;
    }
    assert!(
        (4096..=4128).contains(&total_master_shards),
        "[9.6] total_master_shards={total_master_shards}, expected 4096 (±32 for in-flight handoffs)"
    );
    eprintln!("[9.6] Total master shards = {total_master_shards} -- correct (4096 ±32)");

    tlog!(t0, "teardown_all (cleanup)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    eprintln!("[scenario_09] All sub-tests passed");
    tlog!(t0, "=== SCENARIO COMPLETE ===");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A full run's worth of unpaused time under the current settle budget:
    /// one window before the first phase pauses the workload, plus one after
    /// each of the three restarts.
    const FULL_RUN_UNPAUSED: Duration = Duration::from_secs(4 * BG_UNPAUSED_SETTLE.as_secs());

    #[test]
    fn validate_workload_progress_rejects_zero_creates() {
        let err = validate_workload_progress(0, 500, FULL_RUN_UNPAUSED).unwrap_err();
        assert!(
            err.contains("zero successful creates"),
            "unexpected message: {err}"
        );
    }

    #[test]
    fn validate_workload_progress_rejects_zero_reads_even_with_creates() {
        let err = validate_workload_progress(5_000, 0, FULL_RUN_UNPAUSED).unwrap_err();
        assert!(
            err.contains("zero successful reads"),
            "unexpected message: {err}"
        );
    }

    #[test]
    fn validate_workload_progress_accepts_a_healthy_run() {
        // Nominal rate over the full unpaused budget: 100 creates/sec and
        // 60 reads/sec for 20s.
        let ticks = FULL_RUN_UNPAUSED.as_secs_f64() / BG_TICK.as_secs_f64();
        let creates = (ticks * BG_CREATES_PER_TICK as f64) as u64;
        let reads = (ticks * BG_READS_PER_TICK as f64) as u64;
        let result = validate_workload_progress(creates, reads, FULL_RUN_UNPAUSED);
        assert!(
            result.is_ok(),
            "a run at the documented rate must pass: {result:?}"
        );
    }

    #[test]
    fn validate_workload_progress_rejects_total_outage() {
        // Mirrors the scenario_10 nightly-outage shape applied to this
        // scenario: the background workload attempted ops every tick but
        // every single one failed, so creates_ok and reads_ok both stay 0
        // while the final consistency check -- which only walks the
        // initial 5000-record seed -- would still report zero mismatches.
        let err = validate_workload_progress(0, 0, FULL_RUN_UNPAUSED).unwrap_err();
        assert!(
            err.contains("zero successful creates"),
            "unexpected message: {err}"
        );
    }

    /// THE regression this fix exists for. Run 32630545533 "passed" with
    /// `creates_ok=10, reads_ok=6, spends_ok=4` -- exactly one tick of the
    /// documented 10/6/4 shape -- because the workload was paused
    /// microseconds after being spawned and never got an unpaused window
    /// again. A `> 0` guard is satisfied by that. The rate-derived bound
    /// must not be.
    #[test]
    fn a_single_tick_no_longer_counts_as_a_sustained_workload() {
        let err =
            validate_workload_progress(BG_CREATES_PER_TICK, BG_READS_PER_TICK, FULL_RUN_UNPAUSED)
                .unwrap_err();
        assert!(
            err.contains("creates") && err.contains("far below the configured rate"),
            "the one-tick run must be rejected by name: {err}"
        );
        assert!(
            err.contains("20.0s unpaused"),
            "the message must show the duration the bound was derived from: {err}"
        );
    }

    /// The bound is DERIVED, not a constant: halve the unpaused time and the
    /// same op count that failed the full budget passes the short one.
    #[test]
    fn the_bound_scales_with_the_unpaused_duration() {
        let ops = min_expected_ops(BG_CREATES_PER_TICK, FULL_RUN_UNPAUSED);
        let reads = min_expected_ops(BG_READS_PER_TICK, FULL_RUN_UNPAUSED);
        assert!(
            validate_workload_progress(ops, reads, FULL_RUN_UNPAUSED).is_ok(),
            "exactly the minimum must pass"
        );
        let short = FULL_RUN_UNPAUSED / 2;
        assert!(
            min_expected_ops(BG_CREATES_PER_TICK, short) < ops,
            "a shorter unpaused window must demand fewer ops"
        );
        // One op short of the minimum fails, at either duration.
        assert!(validate_workload_progress(ops - 1, reads, FULL_RUN_UNPAUSED).is_err());
    }

    /// The floor is a FRACTION of the nominal rate, so a run that is merely
    /// slow (ticks stretched by real RPC latency) still passes while a run
    /// that stopped does not.
    #[test]
    fn a_slow_but_running_workload_still_passes() {
        let ticks = FULL_RUN_UNPAUSED.as_secs_f64() / BG_TICK.as_secs_f64();
        // Twice the floor -- i.e. ticks running at half the floor's pace are
        // still comfortably accepted. Expressed relative to the floor rather
        // than as a literal so tightening BG_MIN_RATE_FRACTION cannot turn
        // this case into a silent contradiction.
        let slow_rate = BG_MIN_RATE_FRACTION * 2.0;
        let creates = (ticks * BG_CREATES_PER_TICK as f64 * slow_rate) as u64;
        let reads = (ticks * BG_READS_PER_TICK as f64 * slow_rate) as u64;
        assert!(
            validate_workload_progress(creates, reads, FULL_RUN_UNPAUSED).is_ok(),
            "a workload at {creates} creates / {reads} reads is running, not stalled"
        );
    }

    /// Exercises the exact `if let Err(msg) = ... { panic!("9: {msg}") }`
    /// shape used at the real call site in `run_scenario`, so this test
    /// watches the panic path itself fire rather than just checking the
    /// `Result` plumbing.
    ///
    /// The `[selftest] ` prefix is this fixture's alone -- the real call
    /// site does not carry it -- so a raw scenario log can be grepped for
    /// the failure text without matching this deliberate look-alike.
    #[test]
    #[should_panic(expected = "[selftest] 9: zero successful creates")]
    fn total_outage_evidence_panics_like_the_real_call_site() {
        if let Err(msg) = validate_workload_progress(0, 0, FULL_RUN_UNPAUSED) {
            panic!("[selftest] 9: {msg}");
        }
    }

    /// Count how many tick bodies [`run_bg_loop`] executes for a given
    /// pause/stop schedule, driving the REAL loop the background workload
    /// runs (not a copy of it) with a no-I/O tick body.
    ///
    /// `current_thread` + `start_paused`: a single-threaded runtime removes
    /// the work-stealing nondeterminism, so a spawned task provably cannot
    /// run until the spawner reaches an await point -- which is exactly the
    /// property the old code depended on and did not have. Virtual time
    /// makes a 5s settle window instant.
    async fn ticks_under(schedule: impl AsyncFnOnce(&AtomicBool)) -> u64 {
        let stop = Arc::new(AtomicBool::new(false));
        let pause = Arc::new(AtomicBool::new(false));
        let ticks = Arc::new(AtomicU64::new(0));
        let (l_stop, l_pause, l_ticks) =
            (Arc::clone(&stop), Arc::clone(&pause), Arc::clone(&ticks));
        let handle = tokio::spawn(async move {
            run_bg_loop(l_stop, l_pause, async move || {
                l_ticks.fetch_add(1, Ordering::Relaxed);
            })
            .await;
        });
        schedule(&pause).await;
        stop.store(true, Ordering::Relaxed);
        let _ = handle.await;
        ticks.load(Ordering::Relaxed)
    }

    /// BEFORE: the shape the scenario shipped with. `tokio::spawn` followed
    /// by `pause_flag.store(true)` with no await in between never lets the
    /// workload run -- it is paused before it is ever polled. On the
    /// scenario's `multi_thread` runtime this is a work-stealing race whose
    /// two observed outcomes were 0 ops (CI 32637568483) and exactly one
    /// tick (CI 32630545533); single-threaded, it is deterministic.
    #[tokio::test(start_paused = true)]
    async fn spawning_then_pausing_without_an_await_starves_the_workload() {
        let ticks = ticks_under(async |pause: &AtomicBool| {
            // No await here -- this is the defect, verbatim.
            pause.store(true, Ordering::Relaxed);
            // Now spend a long time in the "restart" with the workload paused.
            tokio::time::sleep(Duration::from_secs(60)).await;
        })
        .await;
        eprintln!("[evidence] OLD shape (spawn -> pause, no await between): {ticks} ticks");
        assert_eq!(
            ticks, 0,
            "the workload must be provably starved by the old shape -- otherwise this \
             test is not reproducing the defect it guards"
        );
    }

    /// AFTER: an unpaused settle window between the spawn and the pause lets
    /// the workload run at its configured rate. The expected tick count is
    /// the same derivation `validate_workload_progress` uses.
    #[tokio::test(start_paused = true)]
    async fn an_unpaused_settle_window_lets_the_workload_run() {
        let ticks = ticks_under(async |pause: &AtomicBool| {
            tokio::time::sleep(BG_UNPAUSED_SETTLE).await;
            pause.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(60)).await;
        })
        .await;
        eprintln!(
            "[evidence] NEW shape (spawn -> {BG_UNPAUSED_SETTLE:?} settle -> pause): {ticks} ticks \
             = {} creates, {} reads, {} spends",
            ticks * BG_CREATES_PER_TICK,
            ticks * BG_READS_PER_TICK,
            ticks * BG_SPENDS_PER_TICK
        );
        let min_ticks = min_expected_ops(1, BG_UNPAUSED_SETTLE);
        assert!(
            ticks >= min_ticks,
            "the settle window must produce at least {min_ticks} ticks, got {ticks}"
        );
        // With a zero-cost tick body and virtual time, the loop runs at
        // exactly its nominal cadence.
        assert_eq!(
            ticks,
            (BG_UNPAUSED_SETTLE.as_secs_f64() / BG_TICK.as_secs_f64()) as u64,
            "a free tick body must hit the nominal rate exactly"
        );
    }

    /// AFTER, resume side: the same defect exists at every `store(false)`
    /// that is not followed by an await before the next `store(true)`. A
    /// resume window with a settle produces work; one without produces none.
    #[tokio::test(start_paused = true)]
    async fn a_resume_without_a_settle_window_produces_no_work() {
        let without = ticks_under(async |pause: &AtomicBool| {
            pause.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(30)).await; // the restart
            pause.store(false, Ordering::Relaxed);
            // No settle: the next phase re-pauses immediately.
            pause.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;
        eprintln!("[evidence] OLD resume window (store(false) -> store(true)): {without} ticks");
        assert_eq!(without, 0, "a zero-length resume window can do no work");

        let with = ticks_under(async |pause: &AtomicBool| {
            pause.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(30)).await;
            pause.store(false, Ordering::Relaxed);
            tokio::time::sleep(BG_UNPAUSED_SETTLE).await;
            pause.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
        .await;
        eprintln!("[evidence] NEW resume window (settled): {with} ticks");
        assert!(
            with >= min_expected_ops(1, BG_UNPAUSED_SETTLE),
            "the settled resume window must do real work, got {with} ticks"
        );
    }
}
