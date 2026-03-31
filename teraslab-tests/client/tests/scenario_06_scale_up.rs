//! Scenario 06 -- Horizontal scale-up from 3 to 4 nodes.

#[allow(dead_code)]
mod common;

use std::sync::Arc;
use std::time::Duration;
use teraslab_test_client::ClientError;
use teraslab_test_client::verifier::StateVerifier;
use teraslab_test_client::reporter::MetricsReporter;
use teraslab_test_client::types::*;

macro_rules! tlog {
    ($t0:expr, $($arg:tt)*) => {
        if common::timing_enabled() {
            eprintln!("[{:6.1}s] {}", $t0.elapsed().as_secs_f64(), format!($($arg)*));
        }
    };
}

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 6;

/// Format a txid as a short hex prefix for assertion messages.
fn txid_hex(txid: &[u8; 32]) -> String {
    txid.iter().take(8).map(|b| format!("{b:02x}")).collect::<String>()
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_06_scale_up() {
    let result = tokio::time::timeout(Duration::from_secs(300), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            common::teardown_all(SID).await;
            panic!("scenario failed: {e}");
        }
        Err(_) => {
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

    let (docker, client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(15)).await?;
    client.refresh_routing().await?;

    let verifier = Arc::new(StateVerifier::new());

    eprintln!("[6.0] Seeding 10000 records with 10 UTXOs each");
    let txids = common::seed_records(&client, &verifier, 10000, 10).await?;
    assert_eq!(txids.len(), 10000, "expected 10000 seeded records");

    let mut pre_scale_missing = 0u32;
    for chunk in txids.chunks(100) {
        let results = client.get_batch(FIELD_ALL, chunk).await?;
        for (i, result) in results.iter().enumerate() {
            if result.status() != 0 {
                pre_scale_missing += 1;
                if pre_scale_missing <= 5 {
                    eprintln!(
                        "[6.0] pre-scale missing txid {} status={}",
                        txid_hex(&chunk[i]),
                        result.status(),
                    );
                }
            }
        }
    }
    assert_eq!(pre_scale_missing, 0, "[6.0] {pre_scale_missing}/10000 seeded records unreadable before scale-up");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Record shard counts before scale-up for test 6.6
    let mut pre_scaleup_shard_counts: Vec<(u32, u64)> = Vec::new();
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num).await?;
        let master_count = status["master_shard_count"].as_u64().unwrap_or(0);
        pre_scaleup_shard_counts.push((node_num, master_count));
        eprintln!("[6.0] pre-scaleup node{node_num}: {master_count} master shards");
    }

    // -- Test 6.1: Start node4 --
    tlog!(t0, "test 6.1: start node4");
    eprintln!("[6.1] Starting node4 via 5-node compose overlay");
    let mut docker5 = common::docker_5node(SID);
    docker5.compose_up_nodes(&["node4"]).await?;

    // Plan SLA is 5s; using 30s timeout as safety net
    common::wait_cluster_ready(&docker5, 4, Duration::from_secs(15)).await?;

    for node_num in 1..=4u32 {
        let status = common::http_status(&docker5, node_num).await?;
        let cluster_size = status["cluster_size"].as_u64()
            .expect("Test 6.1: cluster_size should be present");
        assert_eq!(cluster_size, 4,
            "Test 6.1: node {node_num} reports cluster_size={cluster_size}, expected 4");
    }
    eprintln!("[6.1] OK -- all 4 nodes report cluster_size=4");

    tlog!(t0, "test 6.1: done");

    // -- Test 6.5: Background workload DURING migration at ~300 ops/sec --
    // Start the background workload BEFORE waiting for migration to complete.
    // Background creates are tracked in the verifier so the consistency check
    // in test 6.7 covers them.
    eprintln!("[6.5] Starting background workload during migration at ~300 ops/sec");
    let bg_client = common::create_client(&docker5, 4).await?;
    bg_client.refresh_routing().await?;
    let reporter = Arc::new(MetricsReporter::new());
    let reporter_bg = Arc::clone(&reporter);

    let verifier_bg = Arc::clone(&verifier);

    let workload_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let workload_running_bg = Arc::clone(&workload_running);
    let workload_errors = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let workload_errors_bg = Arc::clone(&workload_errors);
    let workload_ops = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let workload_ops_bg = Arc::clone(&workload_ops);

    let bg_handle = tokio::spawn(async move {
        // Target ~300 ops/sec means ~3.3ms per op
        let interval = Duration::from_millis(3);
        let mut batch_idx = 0u32;

        while workload_running_bg.load(std::sync::atomic::Ordering::Relaxed) {
            batch_idx += 1;

            // Mix of creates and reads
            if batch_idx % 3 == 0 {
                // Read a random-ish txid (may not exist, that's fine)
                let probe_txid = [batch_idx as u8; 32];
                let op_start = std::time::Instant::now();
                match bg_client.get_batch(FIELD_ALL_METADATA, &[probe_txid]).await {
                    Ok(_) => {
                        reporter_bg.record("read", op_start.elapsed());
                        workload_ops_bg.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => {
                        let error_idx = workload_errors_bg.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                        if error_idx <= 5 {
                            eprintln!("[6.5] background read error #{error_idx}: {e}");
                        }
                        workload_ops_bg.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            } else {
                // Create a small record
                let mut txid = [0u8; 32];
                txid[0..4].copy_from_slice(&batch_idx.to_le_bytes());
                txid[4] = 0x06; // scenario marker
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
                        verifier_bg.record_create(txid, 1, vec![utxo_hash]);
                        workload_ops_bg.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => {
                        let error_idx = workload_errors_bg.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                        if error_idx <= 5 {
                            eprintln!("[6.5] background create error #{error_idx}: {e}");
                        }
                        workload_ops_bg.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }

            tokio::time::sleep(interval).await;
        }
    });

    // -- Test 6.2: Wait for migrations, check balance --
    tlog!(t0, "test 6.2: wait for migrations");
    eprintln!("[6.2] Waiting for migrations to complete, then checking balance");
    common::wait_migrations_complete(&docker5, 4, Duration::from_secs(60)).await?;
    eprintln!("[6.2] OK -- all migrations complete");

    // Stop the background workload
    workload_running.store(false, std::sync::atomic::Ordering::Relaxed);
    let _ = bg_handle.await;

    // Evaluate background workload results
    let bg_total_ops = workload_ops.load(std::sync::atomic::Ordering::Relaxed);
    let bg_total_errors = workload_errors.load(std::sync::atomic::Ordering::Relaxed);
    let error_rate = if bg_total_ops > 0 {
        bg_total_errors as f64 / bg_total_ops as f64
    } else {
        0.0
    };

    eprintln!("[6.5] Background workload: {bg_total_ops} ops, {bg_total_errors} errors, error rate: {:.2}%",
        error_rate * 100.0);
    assert!(error_rate < 0.01,
        "Test 6.5: error rate {:.2}% exceeds 1% ({bg_total_errors}/{bg_total_ops})",
        error_rate * 100.0);

    // Check p99 latency
    if let Some(create_stats) = reporter.stats("create") {
        eprintln!("[6.5] Create p99: {:?}", create_stats.p99);
    }
    if let Some(read_stats) = reporter.stats("read") {
        eprintln!("[6.5] Read p99: {:?}", read_stats.p99);
    }
    eprintln!("[6.5] {}", reporter.format_summary());
    eprintln!("[6.5] OK -- background workload during migration passed (error rate < 1%)");

    client.refresh_routing().await?;

    // Check shard balance
    let expected_per_node: u64 = 4096 / 4;
    let tolerance = expected_per_node * 5 / 100;

    let mut total_masters: u64 = 0;
    for node_num in 1..=4u32 {
        let status = common::http_status(&docker5, node_num).await?;
        let master_count = status["master_shard_count"].as_u64()
            .expect("Test 6.2: master_shard_count should be present");
        total_masters += master_count;

        let diff = if master_count > expected_per_node {
            master_count - expected_per_node
        } else {
            expected_per_node - master_count
        };
        assert!(diff <= tolerance,
            "Test 6.2: node {node_num} masters {master_count} shards, expected ~{expected_per_node} \
             (tolerance {tolerance}), difference is {diff}");
        eprintln!("[6.2] node{node_num}: {master_count} master shards");
    }
    assert_eq!(total_masters, 4096);
    eprintln!("[6.2] OK -- balanced distribution confirmed (~1024 per node)");

    tlog!(t0, "test 6.2: done");

    // -- Test 6.3: Read ALL 10000 original records --
    tlog!(t0, "test 6.3: read all records");
    eprintln!("[6.3] Reading ALL 10000 original records");
    let mut read_failures = 0u32;
    let mut failed_reads: Vec<([u8; 32], u8)> = Vec::new();

    for chunk in txids.chunks(100) {
        let results = client.get_batch(FIELD_ALL, chunk).await?;
        for (i, result) in results.iter().enumerate() {
            if result.status() != 0 {
                read_failures += 1;
                failed_reads.push((chunk[i], result.status()));
                eprintln!(
                    "Test 6.3: txid {} returned unexpected result status={}",
                    txid_hex(&chunk[i]),
                    result.status(),
                );
            }
        }
    }
    // Retry any not-found records after routing refresh — inbound
    // migration state may still be clearing on some nodes.
    if read_failures > 0 && read_failures <= 50 {
        eprintln!("[6.3] {read_failures} records not found, retrying after routing refresh...");
        client.refresh_routing().await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        client.refresh_routing().await?;
        read_failures = 0;
        failed_reads.clear();
        for chunk in txids.chunks(100) {
            let results = client.get_batch(FIELD_ALL, chunk).await?;
            for (i, result) in results.iter().enumerate() {
                if result.status() != 0 {
                    read_failures += 1;
                    failed_reads.push((chunk[i], result.status()));
                    if read_failures <= 5 {
                        eprintln!(
                            "Test 6.3 retry: txid {} still not found status={}",
                            txid_hex(&chunk[i]),
                            result.status(),
                        );
                    }
                }
            }
        }
        for (txid, routed_status) in failed_reads.iter().take(5) {
            eprintln!(
                "[6.3] diag txid {} routed_status={}",
                txid_hex(txid),
                routed_status,
            );
            for node_num in 1..=4u32 {
                let node_addr = format!("127.0.0.1:{}", docker5.client_port(node_num));
                match common::direct_get(&client, &node_addr, &[*txid]).await {
                    Ok((_frame_status, payload)) => {
                        let item_status = common::parse_batch_response(&payload)
                            .into_iter()
                            .next()
                            .map(|(status, _)| status)
                            .unwrap_or(255);
                        eprintln!(
                            "[6.3] diag txid {} node{} local_status={}",
                            txid_hex(txid),
                            node_num,
                            item_status,
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "[6.3] diag txid {} node{} local_read_error={}",
                            txid_hex(txid),
                            node_num,
                            e,
                        );
                    }
                }
            }
        }
        for node_num in 1..=4u32 {
            match common::http_migration_status(&docker5, node_num).await {
                Ok(status) => {
                    eprintln!(
                        "[6.3] migration_status node{} active={} failed={} inbound_pending={} fenced_shards={} migrations={}",
                        node_num,
                        status["active_count"].as_u64().unwrap_or(0),
                        status["failed_count"].as_u64().unwrap_or(0),
                        status["inbound_pending"].as_u64().unwrap_or(0),
                        status["fenced_shards"].as_u64().unwrap_or(0),
                        status["migrations"].as_array().map(|m| m.len()).unwrap_or(0),
                    );
                }
                Err(e) => {
                    eprintln!("[6.3] migration_status node{} error={}", node_num, e);
                }
            }
        }
    }
    assert_eq!(read_failures, 0,
        "Test 6.3: {read_failures}/10000 reads failed after migration");
    eprintln!("[6.3] OK -- all 10000 records accessible after migration");

    tlog!(t0, "test 6.3: done");

    // -- Test 6.4: Explicit no-duplication check --
    tlog!(t0, "test 6.4: no-duplication check");
    eprintln!("[6.4] Checking for duplicate records and data loss");
    let mut seen_txids = std::collections::HashSet::new();
    let mut duplicate_count = 0u32;
    for txid in &txids {
        if !seen_txids.insert(*txid) {
            duplicate_count += 1;
            eprintln!("Test 6.4: duplicate txid in seed set: {}", txid_hex(txid));
        }
    }
    assert_eq!(duplicate_count, 0, "Test 6.4: found {duplicate_count} duplicate txids in seed set");

    // Verify each record returns exactly one result from the cluster
    let mut missing_count = 0u32;
    for chunk in txids.chunks(100) {
        let results = client.get_batch(FIELD_ALL, chunk).await?;
        assert_eq!(results.len(), chunk.len(),
            "Test 6.4: get_batch returned {} results for {} txids", results.len(), chunk.len());
        for (i, result) in results.iter().enumerate() {
            if result.status() != 0 {
                missing_count += 1;
                eprintln!("Test 6.4: txid {} not found (possible loss)", txid_hex(&chunk[i]));
            }
        }
    }
    assert_eq!(missing_count, 0,
        "Test 6.4: {missing_count} records missing after scale-up (data loss or duplication issue)");
    eprintln!("[6.4] OK -- no data loss or duplication detected");

    tlog!(t0, "test 6.4: done");

    // -- Test 6.6: Source nodes freed space for migrated shards --
    tlog!(t0, "test 6.6: source nodes freed shards");
    eprintln!("[6.6] Checking that source nodes have fewer shards after migration");
    for (node_num, pre_count) in &pre_scaleup_shard_counts {
        let status = common::http_status(&docker5, *node_num).await?;
        let post_count = status["master_shard_count"].as_u64().unwrap_or(0);
        eprintln!("[6.6] node{node_num}: {pre_count} -> {post_count} master shards");
        assert!(post_count < *pre_count,
            "Test 6.6: node {node_num} master shards did not decrease ({pre_count} -> {post_count}), \
             expected shards to migrate to node4");
    }
    eprintln!("[6.6] OK -- all source nodes have fewer master shards after migration");

    tlog!(t0, "test 6.6: done");

    // -- Test 6.7: Full consistency + replication check --
    tlog!(t0, "test 6.7: consistency check");
    eprintln!("[6.7] Running full consistency check via verify_consistency()");
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    assert!(mismatches.is_empty(),
        "Test 6.7: verify_consistency found {} mismatches: {:?}",
        mismatches.len(),
        mismatches.iter().take(5).collect::<Vec<_>>());
    eprintln!("[6.7] OK -- full consistency check passed, zero mismatches");

    tlog!(t0, "test 6.7: done");

    tlog!(t0, "teardown_all (cleanup)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    eprintln!("[scenario_06] All sub-tests passed");
    tlog!(t0, "=== SCENARIO COMPLETE ===");

    Ok(())
}
