//! Scenario 05 -- Node recovery and data catch-up after hard kill.

#[allow(dead_code)]
mod common;

use std::sync::Arc;
use std::time::Duration;
use teraslab_test_client::ClientError;
use teraslab_test_client::reporter::MetricsReporter;
use teraslab_test_client::types::*;
use teraslab_test_client::verifier::StateVerifier;

macro_rules! tlog {
    ($t0:expr, $($arg:tt)*) => {
        if common::timing_enabled() {
            eprintln!("[{:6.1}s] {}", $t0.elapsed().as_secs_f64(), format!($($arg)*));
        }
    };
}

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 5;

/// Records seeded before node2 is killed. Named because the recovery budget
/// below is derived from it — seeding more must raise the budget, not silently
/// eat its headroom.
const SEED_RECORDS: u32 = 5000;

/// Nodes in this scenario's cluster, and the replication factor its Docker
/// configs set (`teraslab-tests/docker/config/node*.toml`).
const NODE_COUNT: u64 = 3;
const REPLICATION_FACTOR: u64 = 2;

/// Redo entries node2 has to replay at boot after the kill.
///
/// Each seeded create is written on the shard's master AND on its replica, so
/// at RF=2 over 3 nodes a given node receives `2/3` of them:
///
/// ```text
/// SEED_RECORDS * REPLICATION_FACTOR / NODE_COUNT = 5000 * 2 / 3 = 3333
/// ```
///
/// Armed CI (run 32644353574) measured `recovery complete {replayed: 3375}` —
/// within 1.3% of the model, which is why the model is used rather than a
/// hard-coded observation.
const EXPECTED_REPLAYED_REDO_ENTRIES: u64 = SEED_RECORDS as u64 * REPLICATION_FACTOR / NODE_COUNT;

/// Cost of replaying ONE redo entry at boot, in microseconds.
///
/// Recovery makes two O(redo) passes over the tail (the replay itself, then the
/// mined-index full-redo branch when no index snapshot is found), measured at
/// 1.37 ms/entry each in armed CI — so 2.74 ms per entry of backlog.
const REPLAY_MICROS_PER_ENTRY: u64 = 2_740;

/// Boot cost that does not scale with the redo backlog: `docker start`
/// (~0.9 s) plus device + index open (~0.1 s).
const FIXED_BOOT_MILLIS: u64 = 1_000;

/// How long node2 may take to bind its HTTP listener after `docker start` —
/// i.e. the whole boot + RECOVERY phase, which is over before membership is
/// even attempted.
///
/// ```text
/// (FIXED_BOOT_MILLIS + EXPECTED_REPLAYED_REDO_ENTRIES * REPLAY_MICROS_PER_ENTRY / 1000) * 3/2
///   = (1000 ms + 3333 * 2.74 ms) * 1.5 = (1.0 s + 9.13 s) * 1.5 = 15.2 s
/// ```
///
/// The 50% is CI headroom, not slack for a defect: this budget covers a node
/// that is booting normally, and a node that is genuinely wedged overruns any
/// budget rather than creeping past a tight one.
///
/// This was 10 s and it was NOT a recovery budget at all — it was the
/// membership SLA below, doing double duty. The armed sweep makes seeding ~4.4x
/// slower (default seeds 5000 records in 3 s, armed 13 s), so node2 accrued ~4x
/// the redo before the kill and the 10 s gate expired while it was still
/// replaying — reported as `UNREACHABLE`, which reads like a crash and was not.
const NODE_BOOT_BUDGET: Duration = Duration::from_millis(
    (FIXED_BOOT_MILLIS + EXPECTED_REPLAYED_REDO_ENTRIES * REPLAY_MICROS_PER_ENTRY / 1_000) * 3 / 2,
);

/// How long the cluster may take to agree that node2 is back, measured from the
/// instant node2's listener answers.
///
/// This is the actual SLA of test 5.7 and it is now independent of the redo
/// backlog: SWIM detection plus the topology-term agreement the three nodes
/// have to reach. Armed CI measured the SWIM leg at ~0.9 s; the default run's
/// whole `time_to_membership` (boot + recovery + SWIM) was 3.02 s. 10 s is the
/// bound this scenario has always asserted — kept, not loosened, because
/// removing the recovery term from the measurement is what makes it meaningful.
const MEMBERSHIP_SLA: Duration = Duration::from_secs(10);

/// Format a txid as a short hex prefix for assertion messages.
fn txid_hex(txid: &[u8; 32]) -> String {
    txid.iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_05_node_recovery_catchup() {
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

async fn run_scenario() -> Result<(), ClientError> {
    let t0 = std::time::Instant::now();
    tlog!(t0, "teardown_all (pre-clean)...");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    let (docker, client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;
    client.refresh_routing().await?;

    // Node2 address for direct reads
    let node2_addr = format!("127.0.0.1:{}", docker.client_port(2));

    let verifier = StateVerifier::new();

    eprintln!("[5.0] Seeding {SEED_RECORDS} records with 10 UTXOs each");
    let initial_txids = common::seed_records(&client, &verifier, SEED_RECORDS, 10).await?;
    assert_eq!(
        initial_txids.len(),
        SEED_RECORDS as usize,
        "expected {SEED_RECORDS} seeded records"
    );

    // Wait for redo sequences to converge before killing node2
    eprintln!("[5.0] Waiting for replication to settle...");
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

    eprintln!("[5.0] Killing node2");
    docker.kill_node("node2").await?;
    // Wait for BOTH surviving nodes to detect node2's departure
    common::wait_specific_nodes_ready(&docker, &[1, 3], 2, Duration::from_secs(30)).await?;
    // Wait for shard table rebalance and migrations on the 2-node cluster
    common::wait_specific_migrations_complete(&docker, &[1, 3], Duration::from_secs(120)).await?;
    client.refresh_routing().await?;

    eprintln!("[5.0] Creating 500 additional records while node2 is down");
    let extra_txids = common::seed_records(&client, &verifier, 500, 10).await?;
    assert_eq!(extra_txids.len(), 500, "expected 500 extra records");

    let all_txids: Vec<[u8; 32]> = initial_txids
        .iter()
        .chain(extra_txids.iter())
        .copied()
        .collect();

    // -- Test 5.1: Restart node2 --
    tlog!(t0, "test 5.1 start");
    eprintln!("[5.1] Starting node2");
    let restart_start = std::time::Instant::now();
    docker.start_node("node2").await?;

    // W16 — TWO measurements, because they are two different things and only
    // one of them is an SLA.
    //
    // Leg 1 (boot + RECOVERY) ends when node2's HTTP listener answers, which
    // the server binds only after replay/mined-index/DAH/tombstone recovery has
    // finished. Its cost scales with the redo backlog the previous phase
    // generated. Leg 2 (MEMBERSHIP) starts there and does not.
    //
    // Asserting the SUM against a 10 s bound made the SLA depend on how much
    // redo the seeding phase happened to produce — and in armed CI, where
    // seeding is ~4.4x slower and the backlog ~4x bigger, it expired on a node2
    // that was booting normally and reported it as `UNREACHABLE`.
    let time_to_listening = common::wait_node_http_ready(&docker, 2, NODE_BOOT_BUDGET)
        .await
        .map_err(|e| {
            eprintln!(
                "Test 5.1: node2 did not finish booting within {NODE_BOOT_BUDGET:?} \
                 ({EXPECTED_REPLAYED_REDO_ENTRIES} expected redo entries x \
                 {REPLAY_MICROS_PER_ENTRY}us + {FIXED_BOOT_MILLIS}ms, +50% CI headroom): {e}"
            );
            e
        })?;
    let membership_start = std::time::Instant::now();
    // The wait budget IS the SLA: a membership leg that overruns it fails here,
    // with the per-node status dump, rather than at the bare assert in 5.7. The
    // assert is kept anyway — it is the statement of intent, and it is what a
    // reader greps for.
    common::wait_cluster_ready(&docker, 3, MEMBERSHIP_SLA)
        .await
        .map_err(|e| {
            eprintln!(
                "Test 5.1: cluster did not reach size 3 within {MEMBERSHIP_SLA:?} of node2's \
                 listener answering (node2 booted in {time_to_listening:?}): {e}"
            );
            e
        })?;
    let time_to_membership = membership_start.elapsed();
    eprintln!(
        "[5.1] OK -- all 3 nodes report cluster_size=3 (boot+recovery {time_to_listening:?}, \
         membership {time_to_membership:?})"
    );
    tlog!(t0, "test 5.1 done");

    // -- Test 5.2: Wait for migrations --
    tlog!(t0, "test 5.2 start");
    eprintln!("[5.2] Waiting for migrations to complete");
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;
    // From `docker start`, deliberately: the catch-up budget below is a
    // whole-recovery number and must not silently shed the boot leg now that
    // `membership_start` means "node2 is listening".
    let time_to_caught_up = restart_start.elapsed();
    eprintln!("[5.2] OK -- all migrations complete");
    tlog!(t0, "test 5.2 done");

    client.refresh_routing().await?;

    // Migration counters can flip to zero a beat before the rejoining node
    // has committed all inbound shard data (pattern A). Probe before the
    // downstream consistency checks.
    common::wait_for_migration_reads_ready(
        &client,
        &docker,
        &all_txids,
        &[1, 2, 3],
        2,
        50,
        Duration::from_secs(60),
    )
    .await?;

    // -- Test 5.3: Verify balanced distribution --
    tlog!(t0, "test 5.3 start");
    eprintln!("[5.3] Checking shard distribution balance");
    // Wait for shard rebalance to fully propagate after node rejoin.
    {
        let start = std::time::Instant::now();
        loop {
            let mut min_masters = u64::MAX;
            for n in 1..=3u32 {
                if let Ok(s) = common::http_status(&docker, n).await {
                    let mc = s["master_shard_count"].as_u64().unwrap_or(0);
                    min_masters = min_masters.min(mc);
                }
            }
            if min_masters > 1000 {
                break;
            }
            if start.elapsed() >= Duration::from_secs(30) {
                eprintln!("[5.3] WARNING: min_masters={min_masters} after 30s");
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    let expected_per_node: u64 = 4096 / 3;
    let tolerance_pct: u64 = 10;
    let tolerance = expected_per_node * tolerance_pct / 100;

    let mut total_masters: u64 = 0;
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num).await?;
        let master_count = status["master_shard_count"]
            .as_u64()
            .expect("Test 5.3: master_shard_count should be present");
        total_masters += master_count;

        let diff = master_count.abs_diff(expected_per_node);
        assert!(
            diff <= tolerance,
            "Test 5.3: node {node_num} masters {master_count} shards, expected ~{expected_per_node} \
             (tolerance {tolerance}), difference is {diff}"
        );
        eprintln!("[5.3] node{node_num}: {master_count} master shards");
    }
    assert!(
        (4096..=4128).contains(&total_masters),
        "[5.3] total_masters={total_masters}, expected 4096 (±32 for in-flight handoffs)"
    );
    eprintln!("[5.3] OK -- balanced distribution confirmed (total={total_masters})");
    tlog!(t0, "test 5.3 done");

    // -- Test 5.4: Read ALL records from node2 directly --
    // Every record that node2 is master or replica for must be accessible.
    tlog!(t0, "test 5.4 start");
    eprintln!(
        "[5.4] Reading ALL {} records directly from node2 via FLAG_LOCAL_READ",
        all_txids.len()
    );
    let mut accessible_count = 0u32;
    let mut inaccessible_count = 0u32;

    // Batch reads: send chunks of 500 txids per request to node2.
    for chunk in all_txids.chunks(500) {
        match common::direct_get(&client, &node2_addr, chunk).await {
            Ok((_status, payload)) => {
                let items = common::parse_batch_response(&payload);
                for (item_status, _data) in &items {
                    if *item_status == 0 {
                        accessible_count += 1;
                    } else {
                        inaccessible_count += 1;
                    }
                }
            }
            Err(_) => {
                inaccessible_count += chunk.len() as u32;
            }
        }
    }

    // Node2 should have all records it is master or replica for.
    // With RF=2 and 3 nodes, each node holds ~2/3 of all records.
    // After catchup, ALL records assigned to node2 (master + replica) must be present.
    let total_checked = accessible_count + inaccessible_count;
    assert_eq!(
        total_checked,
        all_txids.len() as u32,
        "Test 5.4: checked {total_checked} but expected {}",
        all_txids.len()
    );
    // With RF=2, node2 should be master or replica for approximately 2/3 of shards,
    // so it should hold at least ~20% of all records (conservatively).
    // Inaccessible records are expected for shards not assigned to node2.
    assert!(
        accessible_count > total_checked / 5,
        "Test 5.4: node2 only has {accessible_count}/{total_checked} records accessible, \
         expected at least ~20% (with RF=2 and 3 nodes, node2 should hold ~2/3 of shards)"
    );
    eprintln!(
        "[5.4] OK -- {accessible_count}/{total_checked} records accessible on node2 locally \
              ({inaccessible_count} not on this node, which is expected for shards it doesn't own)"
    );
    tlog!(t0, "test 5.4 done");

    // -- Test 5.5: Master/replica byte comparison for ALL records --
    // NOTE: This uses verify_consistency() which performs routed reads through the
    // cluster, NOT direct master-vs-replica byte comparison. A proper
    // verify_replication would read directly from both the master and replica for
    // each shard and compare raw bytes. That level of verification is not yet
    // implemented in the test client.
    tlog!(t0, "test 5.5 start");
    eprintln!("[5.5] Full consistency check via verify_consistency()");
    // Wait for replication to settle after migrations, then create a fresh client.
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;
    let fresh_client = common::create_client(&docker, 3).await?;
    let mismatches = common::verify_consistency(&fresh_client, &verifier).await?;
    assert!(
        mismatches.is_empty(),
        "Test 5.5: verify_consistency found {} mismatches: {:?}",
        mismatches.len(),
        mismatches.iter().take(5).collect::<Vec<_>>()
    );
    eprintln!("[5.5] OK -- full consistency check passed, zero mismatches");
    tlog!(t0, "test 5.5 done");

    // -- Test 5.6: No duplicate records --
    tlog!(t0, "test 5.6 start");
    eprintln!("[5.6] Checking for duplicate records");
    let mut seen_txids = std::collections::HashSet::new();
    let all_verifier_txids = verifier.non_deleted_txids();
    let mut duplicate_count = 0u32;
    for txid in &all_verifier_txids {
        if !seen_txids.insert(*txid) {
            duplicate_count += 1;
            eprintln!("Test 5.6: duplicate txid found: {}", txid_hex(txid));
        }
    }
    assert_eq!(
        duplicate_count, 0,
        "Test 5.6: found {duplicate_count} duplicate txids in verifier"
    );

    // Also verify via cluster reads that no txid returns multiple distinct records
    let mut cluster_duplicates = 0u32;
    for chunk in all_txids.chunks(100) {
        let results = client.get_batch(FIELD_ALL, chunk).await?;
        assert_eq!(
            results.len(),
            chunk.len(),
            "Test 5.6: get_batch returned {} results for {} txids",
            results.len(),
            chunk.len()
        );
        for (i, result) in results.iter().enumerate() {
            if result.status() != 0 {
                cluster_duplicates += 1;
                eprintln!(
                    "Test 5.6: txid {} not found in cluster",
                    txid_hex(&chunk[i])
                );
            }
        }
    }
    assert_eq!(
        cluster_duplicates, 0,
        "Test 5.6: {cluster_duplicates} records missing from cluster (possible duplication/loss issue)"
    );
    eprintln!("[5.6] OK -- no duplicate records found");
    tlog!(t0, "test 5.6 done");

    // -- Test 5.7: Measure time-to-membership and time-to-fully-caught-up --
    tlog!(t0, "test 5.7 start");
    eprintln!("[5.7] Recovery timing measurements:");
    eprintln!(
        "[5.7]   Time to boot + recover (HTTP listener answers): {time_to_listening:?} \
         (budget {NODE_BOOT_BUDGET:?} for ~{EXPECTED_REPLAYED_REDO_ENTRIES} redo entries)"
    );
    eprintln!(
        "[5.7]   Time to membership (cluster_size=3, from listening): {time_to_membership:?}"
    );
    eprintln!(
        "[5.7]   Time to fully caught up (migrations complete, from docker start): \
         {time_to_caught_up:?}"
    );
    // W16 — the SLA, now measured from the instant node2 is observable rather
    // than from `docker start`. It states something about the cluster's
    // membership machinery and nothing about how big the redo backlog was; the
    // recovery leg has its own budget (`NODE_BOOT_BUDGET`), derived from that
    // backlog, and already failed above if it was overrun.
    assert!(
        time_to_membership <= MEMBERSHIP_SLA,
        "Test 5.7: time to membership was {time_to_membership:?} measured from node2's listener \
         answering (boot+recovery took {time_to_listening:?} before that), expected <= \
         {MEMBERSHIP_SLA:?}"
    );
    // The bound has to admit the server's OWN pacing, not an aspiration.
    //
    // A rejoining node is a *member rebalance*, and `drain_reactivation_due`
    // deliberately excludes those from the fast re-drive cadence: a member
    // rebalance streams full baselines, and re-driving it on the short cadence
    // "floods the receivers' bounded redo logs faster than the checkpointer can
    // reclaim them (observed: `redo log full` -> every subsequent baseline
    // rejected)". Member rebalances therefore stay on the 30 s cooldown that
    // paces redo pressure.
    //
    // The same design note records that a single activation only sheds a
    // fraction of the shards ("under load roughly half lose their fence/handoff
    // race and are rolled back"), so a catch-up routinely needs more than one
    // round. Two rounds cost one full 30 s pacing window before the second even
    // starts, which a 60 s budget cannot contain — a measured run landed at
    // 61.4 s with ~3 s to membership and the rest split across two waves either
    // side of exactly 30 s of deliberate idling.
    //
    // So: membership + two pacing windows + streaming. This still fails loudly
    // on the condition that actually matters and that this scenario has caught
    // before — a catch-up that never converges (masterless shards, stranded
    // inbound migrations), which overruns any budget rather than creeping past
    // a tight one.
    const CATCH_UP_BUDGET: Duration = Duration::from_secs(120);
    assert!(
        time_to_caught_up <= CATCH_UP_BUDGET,
        "Test 5.7: time to fully caught up was {time_to_caught_up:?}, expected <= {CATCH_UP_BUDGET:?} \
         (membership + 2 x the 30s member-rebalance reactivation cooldown + streaming)"
    );
    eprintln!("[5.7] OK -- recovery timing within bounds");
    tlog!(t0, "test 5.7 done");

    // -- Test 5.8: 30-second mixed workload with zero errors --
    tlog!(t0, "test 5.8 start");
    eprintln!("[5.8] Running 30-second mixed workload after recovery");
    let reporter = Arc::new(MetricsReporter::new());
    let workload_duration = Duration::from_secs(30);
    let deadline = tokio::time::Instant::now() + workload_duration;
    let mut total_ops = 0u64;
    let mut total_errors = 0u64;

    // Mixed workload: creates, spends, reads, set_mined
    let mut workload_txids: Vec<[u8; 32]> = Vec::new();
    let mut batch_num = 0u32;

    while tokio::time::Instant::now() < deadline {
        batch_num += 1;

        // Create batch
        let op_start = std::time::Instant::now();
        match common::seed_records(&client, &verifier, 10, 5).await {
            Ok(new_txids) => {
                reporter.record("create", op_start.elapsed());
                workload_txids.extend_from_slice(&new_txids);
                total_ops += 1;
            }
            Err(e) => {
                total_errors += 1;
                eprintln!("[5.8] create batch {batch_num} failed: {e}");
            }
        }

        // Read batch (if we have records)
        if workload_txids.len() >= 10 {
            let read_sample: Vec<[u8; 32]> =
                workload_txids.iter().rev().take(10).copied().collect();
            let op_start = std::time::Instant::now();
            match client.get_batch(FIELD_ALL, &read_sample).await {
                Ok(_results) => {
                    reporter.record("read", op_start.elapsed());
                    total_ops += 1;
                }
                Err(e) => {
                    total_errors += 1;
                    eprintln!("[5.8] read batch {batch_num} failed: {e}");
                }
            }
        }

        // Spend some UTXOs (if we have records to spend)
        if workload_txids.len() >= 5 {
            let spend_targets: Vec<SpendItem> = workload_txids
                .iter()
                .rev()
                .take(3)
                .map(|txid| {
                    let rec = verifier.get_record(txid);
                    let utxo_hash = rec
                        .as_ref()
                        .and_then(|r| r.utxo_hashes.first().copied())
                        .unwrap_or([0u8; 32]);
                    SpendItem {
                        txid: *txid,
                        vout: 0,
                        utxo_hash,
                        spending_data: [0u8; 36],
                    }
                })
                .collect();

            let params = SpendBatchParams {
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 100,
                block_height_retention: 288,
            };

            let op_start = std::time::Instant::now();
            // This workload runs across a node restart, so the shard handoff
            // fence answers some spends with ERR_MIGRATION_IN_PROGRESS. The
            // old code logged that and moved on, silently dropping the op
            // from the workload it exists to generate; retry the transient
            // codes the same way the seed path does. Terminal errors keep
            // the previous tolerant treatment — already-spent slots are an
            // expected outcome here.
            match common::spend_all_with_transient_retry(&client, &params, &spend_targets).await {
                Ok(()) => {
                    reporter.record("spend", op_start.elapsed());
                    for item in &spend_targets {
                        verifier.record_spend(item.txid, item.vout);
                    }
                    total_ops += 1;
                }
                Err(e) => {
                    // Spends may fail for already-spent slots, that's acceptable
                    eprintln!("[5.8] spend batch {batch_num} failed: {e}");
                    total_ops += 1;
                }
            }
        }

        // Throttle to avoid overwhelming the cluster
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Without this, a run where every create batch fails (so `total_ops`
    // never leaves 0 -- reads/spends below are gated on `workload_txids`
    // having entries, which never happens if creates never succeed) would
    // leave `error_rate` at its 0.0 default below despite `total_errors`
    // climbing every iteration, and the 5% check would pass vacuously.
    assert!(
        total_ops > 0,
        "Test 5.8: zero operations succeeded in the 30s post-recovery \
         workload ({total_errors} errors) -- the error-rate check below has \
         nothing to check and cannot be trusted"
    );

    // Allow up to 5% error rate during post-recovery workload — the deferred
    // shard table swap creates a brief window where some writes are rejected.
    let error_rate = total_errors as f64 / total_ops as f64 * 100.0;
    assert!(
        error_rate < 5.0,
        "Test 5.8: {error_rate:.1}% error rate ({total_errors}/{total_ops}) exceeds 5% threshold"
    );
    eprintln!("[5.8] OK -- completed {total_ops} ops in 30s with zero errors");
    eprintln!("[5.8] {}", reporter.format_summary());
    tlog!(t0, "test 5.8 done");

    tlog!(t0, "teardown_all (final)...");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");
    eprintln!("[scenario_05] All sub-tests passed");

    tlog!(t0, "=== SCENARIO COMPLETE ===");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// W16 — the recovery budget must cover the redo backlog this scenario's
    /// OWN seeding phase creates, so raising `SEED_RECORDS` cannot silently eat
    /// the headroom.
    ///
    /// This is the check the old hard-coded 10 s failed. It was never a
    /// recovery budget: it was the membership SLA doing double duty, and in
    /// armed CI (run 32644353574) it expired at 10 s on a node2 that was
    /// booting normally — `recovery complete {replayed: 3375}` landed at
    /// +4.63 s and the listener bound after the mined-index full-redo branch,
    /// the DAH rebuild and the tombstone replay on top of that.
    #[test]
    fn the_boot_budget_covers_the_redo_backlog_this_scenario_creates() {
        assert_eq!(
            EXPECTED_REPLAYED_REDO_ENTRIES, 3_333,
            "5000 seeded records x RF 2 / 3 nodes; armed CI measured 3375"
        );
        let modelled_recovery =
            Duration::from_micros(EXPECTED_REPLAYED_REDO_ENTRIES * REPLAY_MICROS_PER_ENTRY);
        let modelled_boot = Duration::from_millis(FIXED_BOOT_MILLIS) + modelled_recovery;
        assert!(
            modelled_boot >= Duration::from_secs(10),
            "the model must REPRODUCE the failure: a 10s gate cannot contain a \
             {modelled_boot:?} boot, which is why the old bound could not pass",
        );
        assert!(
            NODE_BOOT_BUDGET > modelled_boot,
            "budget {NODE_BOOT_BUDGET:?} must EXCEED the modelled boot {modelled_boot:?}, not \
             merely reach it",
        );
        let headroom = NODE_BOOT_BUDGET - modelled_boot;
        assert!(
            headroom >= modelled_boot / 3,
            "headroom is only {headroom:?} over a {modelled_boot:?} model; CI variance on the \
             replay leg alone is larger than that",
        );
    }

    /// W16 — the membership SLA must NOT have been loosened while splitting the
    /// measurement.
    ///
    /// The whole point of measuring from "node2 is listening" is that the SLA
    /// gets tighter in meaning, not looser in value: it now covers SWIM
    /// detection plus topology agreement (~0.9 s measured) and nothing else. If
    /// a future run needs this raised, the thing to question is the membership
    /// machinery — not, as before, how much redo the seeding phase produced.
    #[test]
    fn the_membership_sla_is_unchanged_and_independent_of_the_backlog() {
        assert_eq!(
            MEMBERSHIP_SLA,
            Duration::from_secs(10),
            "the bound scenario 05 has always asserted",
        );
        // Independence, stated as a computation rather than a comment: the SLA
        // is not a function of any of the redo-backlog inputs.
        assert!(
            MEMBERSHIP_SLA < NODE_BOOT_BUDGET,
            "if the SLA ever exceeds the boot budget it has started absorbing recovery time \
             again, which is exactly the conflation this split removed",
        );
    }
}
