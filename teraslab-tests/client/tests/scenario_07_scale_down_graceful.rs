//! Scenario 07 -- Graceful scale-down from 4 nodes to 3 via quiesce + drain.

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
const SID: u16 = 7;

/// Format a txid as a short hex prefix for assertion messages.
fn txid_hex(txid: &[u8; 32]) -> String {
    txid.iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

/// Validates a single Test 7.4 read-verification pass before it is trusted.
///
/// The retry loop in [`run_scenario`] catches `ClientError::Connection` and
/// breaks out of the chunk loop early, but its failure counter only counts
/// items actually returned by a completed `get_batch` call. If the
/// connection drops on the very first chunk, `failures` stays 0 even though
/// almost nothing was checked -- the retry loop can then exhaust all
/// attempts while the cluster is still unreachable and the caller would
/// otherwise assert `read_failures == 0` and print "all N records
/// accessible, zero loss" having verified none of them. This is the same
/// vacuous-pass shape as `validate_workload_progress` in
/// `scenario_10_sustained_load.rs`.
fn validate_read_pass(
    unreachable: bool,
    checked: usize,
    total: usize,
    failures: u32,
) -> Result<(), String> {
    if unreachable {
        return Err(format!(
            "cluster unreachable mid-pass -- only checked {checked}/{total} records"
        ));
    }
    if checked != total {
        return Err(format!(
            "incomplete pass -- only checked {checked}/{total} records"
        ));
    }
    if failures > 0 {
        return Err(format!("{failures}/{total} records failed read-back"));
    }
    Ok(())
}

/// Validates that node4 actually drained (`master_shard_count` reached 0)
/// before Test 7.2 hands off to forced removal.
///
/// This scenario is `scale_down_graceful`: its entire point is to exercise
/// the quiesce+drain path. If node4 never drained, the downstream generic
/// checks (cluster reforms to size 3, records readable, `verify_consistency`)
/// still pass -- but only because they exercised forced removal, a
/// different scenario, while silently reporting success for the graceful
/// one. A scenario that cannot exercise its titular feature must fail, not
/// warn.
fn validate_node4_drained(node4_drained: bool, drain_timeout_secs: u64) -> Result<(), String> {
    if !node4_drained {
        return Err(format!(
            "Test 7.2: node4 did not fully drain (master_shard_count never reached 0 within \
             {drain_timeout_secs}s) -- the graceful-quiesce mechanism this scenario is named \
             for never worked"
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_07_scale_down_graceful() {
    let result = tokio::time::timeout(Duration::from_secs(300), run_scenario()).await;
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
            panic!("scenario timed out after 300s");
        }
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    let t0 = std::time::Instant::now();

    tlog!(t0, "teardown_all (pre-clean)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    eprintln!("[7.0] Starting 3-node cluster and adding node4");
    let (_docker3, client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&_docker3, 3, Duration::from_secs(120)).await?;

    let mut docker5 = common::docker_5node(SID);
    docker5.compose_up_nodes(&["node4"]).await?;
    common::wait_cluster_ready(&docker5, 4, Duration::from_secs(30)).await?;
    common::wait_migrations_complete(&docker5, 4, Duration::from_secs(120)).await?;
    client.refresh_routing().await?;

    for node_num in 1..=4u32 {
        let status = common::http_status(&docker5, node_num).await?;
        let size = status["cluster_size"].as_u64().unwrap_or(0);
        assert_eq!(
            size, 4,
            "Test 7.0: node {node_num} reports cluster_size={size}, expected 4"
        );
    }

    let verifier = StateVerifier::new();
    eprintln!("[7.0] Seeding 5000 records with 10 UTXOs each");
    let txids = common::seed_records(&client, &verifier, 5000, 10).await?;
    assert_eq!(txids.len(), 5000, "expected 5000 seeded records");

    // Wait for replication of all 5000 records to propagate
    // to all 4 replica nodes via background TCP connections.
    common::wait_replication_settled(&docker5, 4, Duration::from_secs(10)).await?;

    // -- Test 7.6: Start background workload DURING drain --
    // Start this BEFORE quiesce so it runs throughout the drain process.
    eprintln!("[7.6] Starting background workload during drain");
    let bg_client = common::create_client(&docker5, 4).await?;
    bg_client.refresh_routing().await?;
    let reporter = Arc::new(MetricsReporter::new());
    let reporter_bg = Arc::clone(&reporter);

    let workload_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let workload_running_bg = Arc::clone(&workload_running);
    let workload_write_errors = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let workload_write_errors_bg = Arc::clone(&workload_write_errors);
    let workload_ops = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let workload_ops_bg = Arc::clone(&workload_ops);

    let bg_handle = tokio::spawn(async move {
        let interval = Duration::from_millis(10);
        let mut batch_idx = 0u32;

        while workload_running_bg.load(std::sync::atomic::Ordering::Relaxed) {
            batch_idx += 1;

            // Create a small record
            let mut txid = [0u8; 32];
            txid[0..4].copy_from_slice(&batch_idx.to_le_bytes());
            txid[4] = 0x07; // scenario marker
            let utxo_hash = [batch_idx as u8; 32];

            let item = CreateItem {
                txid,
                utxo_hashes: vec![utxo_hash],
                tx_version: 1,
                locktime: 0,
                fee: 100,
                size_in_bytes: 100,
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

            let op_start = std::time::Instant::now();
            match bg_client.create_batch(&[item]).await {
                Ok(_) => {
                    reporter_bg.record("create", op_start.elapsed());
                    workload_ops_bg.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(_) => {
                    workload_write_errors_bg.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    workload_ops_bg.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }

            tokio::time::sleep(interval).await;
        }
    });

    // -- Test 7.1: Trigger quiesce on node4 --
    tlog!(t0, "test 7.1: trigger quiesce on node4");
    eprintln!("[7.1] Triggering quiesce on node4");
    common::http_quiesce(&docker5, 4).await?;
    eprintln!("[7.1] OK -- quiesce request accepted");

    tlog!(t0, "test 7.1: done");

    // -- Test 7.2: Wait for node4 to drain --
    tlog!(t0, "test 7.2: wait for node4 drain");
    eprintln!("[7.2] Polling node4 until master_shard_count reaches 0");
    let drain_timeout = Duration::from_secs(120);
    let drain_start = std::time::Instant::now();
    let mut node4_drained = false;

    loop {
        if drain_start.elapsed() >= drain_timeout {
            break;
        }
        match common::http_status(&docker5, 4).await {
            Ok(status) => {
                let master_count = status["master_shard_count"].as_u64().unwrap_or(u64::MAX);
                if master_count == 0 {
                    node4_drained = true;
                    eprintln!("[7.2] node4 master_shard_count reached 0");
                    break;
                }
                eprintln!("[7.2] node4 master_shard_count = {master_count}, waiting...");
            }
            Err(e) => {
                // HTTP failure does NOT mean the node has drained. It may be a
                // transient network issue. Keep polling until we get a definitive
                // master_shard_count == 0 or the timeout expires.
                eprintln!("[7.2] WARNING: node4 http_status failed, still polling: {e}");
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    if let Err(msg) = validate_node4_drained(node4_drained, drain_timeout.as_secs()) {
        panic!("{msg}");
    }
    eprintln!("[7.2] OK -- node4 fully drained");

    tlog!(t0, "test 7.2: done");

    // Stop background workload before stopping node4 — the workload's
    // purpose is to verify writes during drain, not during topology
    // transition. Stopping it first avoids a retry storm that can
    // overwhelm surviving nodes during SWIM failure detection.
    workload_running.store(false, std::sync::atomic::Ordering::Relaxed);
    let _ = bg_handle.await;

    // -- Test 7.3: Stop node4, wait for cluster_size=3 --
    tlog!(t0, "test 7.3: stop node4, wait for cluster_size=3");
    eprintln!("[7.3] Force-removing node4 (releases Docker network interface)");
    docker5.remove_node("node4").await?;

    // SWIM detects the departed node. Docker networking may delay UDP
    // probe failure detection (silently dropped vs ICMP unreachable).
    // Allow up to 30s for SWIM suspicion + topology proposal + commit.
    common::wait_specific_nodes_ready(&docker5, &[1, 2, 3], 3, Duration::from_secs(30)).await?;

    for node_num in 1..=3u32 {
        let status = common::http_status(&docker5, node_num).await?;
        let size = status["cluster_size"].as_u64().unwrap_or(0);
        assert_eq!(
            size, 3,
            "Test 7.3: node {node_num} reports cluster_size={size}, expected 3"
        );
    }
    eprintln!("[7.3] OK -- cluster stabilized at size 3");

    // Wait for migrations to complete after topology change. Scale-down
    // migrations are heavier (data from departed node must be redistributed).
    common::wait_migrations_complete(&docker5, 3, Duration::from_secs(120))
        .await
        .map_err(|e| {
            eprintln!("[7.3] ERROR: migrations did not complete: {e}");
            e
        })?;
    // Extra settle after migration for any lagging handoff completions.
    common::wait_replication_settled(&docker5, 3, Duration::from_secs(5)).await?;
    // Second migration wait: handoffs that were "orphaned" on first pass
    // may have completed now.
    common::wait_migrations_complete(&docker5, 3, Duration::from_secs(120))
        .await
        .ok();
    client.refresh_routing().await?;

    // Evaluate background workload results (test 7.6)
    let bg_write_errors = workload_write_errors.load(std::sync::atomic::Ordering::Relaxed);
    let bg_total_ops = workload_ops.load(std::sync::atomic::Ordering::Relaxed);
    // The point of 7.6 is to observe writes *during* the drain; if the
    // background task died immediately (e.g. panicked before its first
    // iteration) it would silently report 0 ops / 0 errors and this whole
    // sub-test would have verified nothing while still printing "OK" below.
    assert!(
        bg_total_ops > 0,
        "Test 7.6: background workload during drain executed zero operations -- \
         cannot validate write availability during drain"
    );
    eprintln!(
        "[7.6] Background workload during drain: {bg_total_ops} ops, {bg_write_errors} write failures"
    );
    eprintln!("[7.6] {}", reporter.format_summary());
    // Log transient errors during drain. These are NOT data loss — writes that
    // failed were rejected before being applied (routing staleness during shard
    // migration). The consistency check below verifies data integrity.
    if bg_write_errors > 0 {
        eprintln!(
            "[7.6] {bg_write_errors} transient write errors during drain ({bg_total_ops} total ops) \
             — rejected writes, not data loss"
        );
    } else {
        eprintln!("[7.6] OK -- zero write errors during drain");
    }

    // Wait for shard rebalance to fully settle after workload stops.
    common::wait_migrations_complete(&docker5, 3, Duration::from_secs(120))
        .await
        .unwrap_or_else(|e| eprintln!("[7.3b] migration wait: {e}"));
    client.refresh_routing().await?;

    let mut total_masters: u64 = 0;
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker5, node_num).await?;
        let master_count = status["master_shard_count"]
            .as_u64()
            .expect("Test 7.3: master_shard_count should be present");
        total_masters += master_count;
    }
    assert!(
        (4096..=4128).contains(&total_masters),
        "[7.3] total_masters={total_masters}, expected 4096 (±32 for in-flight handoffs)"
    );

    tlog!(t0, "test 7.3: done");

    // -- Test 7.4: Read ALL records --
    tlog!(t0, "test 7.4: read all records");
    eprintln!("[7.4] Reading ALL {} records", txids.len());

    // After force-removing node4 the client's routing may still point at
    // it for a moment; settle and refresh before the first read pass.
    common::wait_specific_replication_settled(&docker5, &[1, 2, 3], Duration::from_secs(10))
        .await?;
    client.refresh_routing().await?;

    let mut read_failures = 0u32;
    let mut read_checked = 0usize;
    let mut read_unreachable = false;
    for attempt in 0..3u32 {
        if attempt > 0 {
            eprintln!("[7.4] retry {attempt} after settle");
            common::wait_specific_replication_settled(&docker5, &[1, 2, 3], Duration::from_secs(5))
                .await?;
            tokio::time::sleep(Duration::from_millis(500)).await;
            client.refresh_routing().await?;
        }
        let mut this_pass_failures = 0u32;
        let mut this_pass_checked = 0usize;
        let mut unreachable = false;
        for chunk in txids.chunks(100) {
            match client.get_batch(FIELD_ALL, chunk).await {
                Ok(results) => {
                    this_pass_checked += results.len();
                    for (i, result) in results.iter().enumerate() {
                        if result.status() != 0 {
                            this_pass_failures += 1;
                            if this_pass_failures <= 5 && attempt == 2 {
                                eprintln!(
                                    "Test 7.4: txid {} returned unexpected result",
                                    txid_hex(&chunk[i])
                                );
                            }
                        }
                    }
                }
                Err(ClientError::Connection(_)) => {
                    unreachable = true;
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        read_failures = this_pass_failures;
        read_checked = this_pass_checked;
        read_unreachable = unreachable;
        if validate_read_pass(
            unreachable,
            this_pass_checked,
            txids.len(),
            this_pass_failures,
        )
        .is_ok()
        {
            break;
        }
    }
    if let Err(msg) = validate_read_pass(read_unreachable, read_checked, txids.len(), read_failures)
    {
        panic!("Test 7.4: {msg}");
    }
    eprintln!(
        "[7.4] OK -- all {} records accessible, zero loss",
        txids.len()
    );

    tlog!(t0, "test 7.4: done");

    // -- Test 7.5: Full consistency check via verify_consistency() --
    tlog!(t0, "test 7.5: consistency check");
    eprintln!("[7.5] Running full consistency check via verify_consistency()");
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    assert!(
        mismatches.is_empty(),
        "Test 7.5: verify_consistency found {} mismatches: {:?}",
        mismatches.len(),
        mismatches.iter().take(5).collect::<Vec<_>>()
    );
    eprintln!("[7.5] OK -- full consistency check passed, zero mismatches");

    tlog!(t0, "test 7.5: done");

    tlog!(t0, "teardown_all (cleanup)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    eprintln!("[scenario_07] All sub-tests passed");
    tlog!(t0, "=== SCENARIO COMPLETE ===");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_read_pass_rejects_unreachable_even_with_zero_failures() {
        // The exact vacuous-pass shape this guard exists for: a connection
        // error broke the chunk loop on the very first chunk, so nothing
        // was checked and the failure counter is still 0.
        let err = validate_read_pass(true, 0, 5000, 0).unwrap_err();
        assert!(
            err.contains("cluster unreachable mid-pass"),
            "unexpected message: {err}"
        );
        assert!(err.contains("0/5000"), "unexpected message: {err}");
    }

    #[test]
    fn validate_read_pass_rejects_an_incomplete_pass_even_when_reachable() {
        let err = validate_read_pass(false, 3200, 5000, 0).unwrap_err();
        assert!(err.contains("incomplete pass"), "unexpected message: {err}");
        assert!(err.contains("3200/5000"), "unexpected message: {err}");
    }

    #[test]
    fn validate_read_pass_rejects_a_complete_pass_with_failures() {
        let err = validate_read_pass(false, 5000, 5000, 12).unwrap_err();
        assert!(
            err.contains("12/5000 records failed read-back"),
            "unexpected message: {err}"
        );
    }

    #[test]
    fn validate_read_pass_accepts_a_complete_pass_with_zero_failures() {
        let result = validate_read_pass(false, 5000, 5000, 0);
        assert!(
            result.is_ok(),
            "expected a clean pass to succeed: {result:?}"
        );
    }

    /// Exercises the exact `if let Err(msg) = ... { panic!("Test 7.4: {msg}") }`
    /// shape used at the real call site, with the actual outage shape: a
    /// dead connection on chunk 1 of a 5000-record pass.
    #[test]
    #[should_panic(expected = "Test 7.4: cluster unreachable mid-pass -- only checked 0/5000")]
    fn total_outage_panics_like_the_real_call_site() {
        if let Err(msg) = validate_read_pass(true, 0, 5000, 0) {
            panic!("Test 7.4: {msg}");
        }
    }

    #[test]
    fn validate_node4_drained_rejects_a_never_drained_node() {
        // The exact vacuous-pass shape this guard exists for: the poll loop
        // exhausted its timeout without ever observing
        // master_shard_count == 0, but the old code only logged a WARNING
        // and let the scenario continue into forced removal.
        let err = validate_node4_drained(false, 120).unwrap_err();
        assert!(err.contains("did not fully drain"), "err was: {err}");
        assert!(err.contains("120s"), "err was: {err}");
    }

    #[test]
    fn validate_node4_drained_accepts_a_drained_node() {
        let result = validate_node4_drained(true, 120);
        assert!(
            result.is_ok(),
            "expected a drained node to pass: {result:?}"
        );
    }

    /// Exercises the exact `if let Err(msg) = ... { panic!("{msg}") }` shape
    /// used at the real call site, with the actual failure this guard
    /// exists to catch: node4 never drained within the timeout.
    #[test]
    #[should_panic(expected = "Test 7.2: node4 did not fully drain")]
    fn node4_never_drained_panics_like_the_real_call_site() {
        if let Err(msg) = validate_node4_drained(false, 120) {
            panic!("{msg}");
        }
    }
}
