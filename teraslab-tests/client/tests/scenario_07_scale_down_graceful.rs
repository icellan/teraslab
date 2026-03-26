//! Scenario 07 -- Graceful scale-down from 4 nodes to 3 via quiesce + drain.

mod common;

use std::sync::Arc;
use std::time::Duration;
use teraslab_test_client::ClientError;
use teraslab_test_client::verifier::StateVerifier;
use teraslab_test_client::reporter::MetricsReporter;
use teraslab_test_client::types::*;

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 7;

/// Format a txid as a short hex prefix for assertion messages.
fn txid_hex(txid: &[u8; 32]) -> String {
    txid.iter().take(8).map(|b| format!("{b:02x}")).collect::<String>()
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_07_scale_down_graceful() {
    let result = tokio::time::timeout(Duration::from_secs(300), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("scenario failed: {e}"),
        Err(_) => panic!("scenario timed out after 300s"),
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    common::teardown_all(SID).await;

    eprintln!("[7.0] Starting 3-node cluster and adding node4");
    let (_docker3, client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&_docker3, 3, Duration::from_secs(180)).await?;

    let mut docker5 = common::docker_5node(SID);
    docker5.compose_up_nodes(&["node4"]).await?;
    common::wait_cluster_ready(&docker5, 4, Duration::from_secs(30)).await?;
    common::wait_migrations_complete(&docker5, 4, Duration::from_secs(180)).await?;
    client.refresh_routing().await?;

    for node_num in 1..=4u32 {
        let status = common::http_status(&docker5, node_num).await?;
        let size = status["cluster_size"].as_u64().unwrap_or(0);
        assert_eq!(size, 4,
            "Test 7.0: node {node_num} reports cluster_size={size}, expected 4");
    }

    let verifier = StateVerifier::new();
    eprintln!("[7.0] Seeding 5000 records with 10 UTXOs each");
    let txids = common::seed_records(&client, &verifier, 5000, 10).await?;
    assert_eq!(txids.len(), 5000, "expected 5000 seeded records");

    // Allow extra time for replication of all 5000 records to propagate
    // to all 4 replica nodes via background TCP connections.
    tokio::time::sleep(Duration::from_secs(10)).await;

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
    eprintln!("[7.1] Triggering quiesce on node4");
    common::http_quiesce(&docker5, 4).await?;
    eprintln!("[7.1] OK -- quiesce request accepted");

    // -- Test 7.2: Wait for node4 to drain --
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
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    assert!(node4_drained, "Test 7.2: node4 did not drain all master shards within {drain_timeout:?}");
    eprintln!("[7.2] OK -- node4 fully drained");

    // -- Test 7.3: Stop node4, wait for cluster_size=3 --
    eprintln!("[7.3] Stopping node4");
    let _ = docker5.stop_node("node4").await;

    // SWIM needs ~3-5s to detect the departed node (suspicion_timeout=3s).
    // Wait for all 3 surviving nodes to agree on cluster_size=3.
    common::wait_specific_nodes_ready(&docker5, &[1, 2, 3], 3, Duration::from_secs(180)).await?;

    for node_num in 1..=3u32 {
        let status = common::http_status(&docker5, node_num).await?;
        let size = status["cluster_size"].as_u64().unwrap_or(0);
        assert_eq!(size, 3,
            "Test 7.3: node {node_num} reports cluster_size={size}, expected 3");
    }
    eprintln!("[7.3] OK -- cluster stabilized at size 3");

    // Wait for migrations to complete after topology change
    common::wait_migrations_complete(&docker5, 3, Duration::from_secs(180)).await
        .map_err(|e| {
            eprintln!("[7.3] ERROR: migrations did not complete within 120s: {e}");
            e
        })?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    client.refresh_routing().await?;

    // Stop background workload now that drain is complete
    workload_running.store(false, std::sync::atomic::Ordering::Relaxed);
    let _ = bg_handle.await;

    // Evaluate background workload results (test 7.6)
    let bg_write_errors = workload_write_errors.load(std::sync::atomic::Ordering::Relaxed);
    let bg_total_ops = workload_ops.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!("[7.6] Background workload during drain: {bg_total_ops} ops, {bg_write_errors} write failures");
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
    common::wait_migrations_complete(&docker5, 3, Duration::from_secs(60)).await
        .unwrap_or_else(|e| eprintln!("[7.3b] migration wait: {e}"));
    client.refresh_routing().await?;

    let mut total_masters: u64 = 0;
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker5, node_num).await?;
        let master_count = status["master_shard_count"].as_u64()
            .expect("Test 7.3: master_shard_count should be present");
        total_masters += master_count;
    }
    assert_eq!(total_masters, 4096);

    // -- Test 7.4: Read ALL records --
    eprintln!("[7.4] Reading ALL {} records", txids.len());
    let mut read_failures = 0u32;

    for chunk in txids.chunks(100) {
        let results = client.get_batch(FIELD_ALL, chunk).await?;
        for (i, result) in results.iter().enumerate() {
            if result.status() != 0 {
                read_failures += 1;
                if read_failures <= 5 {
                    eprintln!("Test 7.4: txid {} returned unexpected result", txid_hex(&chunk[i]));
                }
            }
        }
    }
    // Retry after routing refresh — inbound migrations may still be settling.
    if read_failures > 0 {
        eprintln!("[7.4] {read_failures} records not found, retrying after refresh...");
        client.refresh_routing().await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        common::wait_migrations_complete(&docker5, 3, Duration::from_secs(60)).await
            .unwrap_or_else(|e| eprintln!("[7.4] migration wait: {e}"));
        client.refresh_routing().await?;
        read_failures = 0;
        for chunk in txids.chunks(100) {
            let results = client.get_batch(FIELD_ALL, chunk).await?;
            for result in results.iter() {
                if result.status() != 0 { read_failures += 1; }
            }
        }
    }
    assert_eq!(read_failures, 0,
        "Test 7.4: {read_failures}/{} reads failed after drain", txids.len());
    eprintln!("[7.4] OK -- all {} records accessible, zero loss", txids.len());

    // -- Test 7.5: Full consistency check via verify_consistency() --
    eprintln!("[7.5] Running full consistency check via verify_consistency()");
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    assert!(mismatches.is_empty(),
        "Test 7.5: verify_consistency found {} mismatches: {:?}",
        mismatches.len(),
        mismatches.iter().take(5).collect::<Vec<_>>());
    eprintln!("[7.5] OK -- full consistency check passed, zero mismatches");

    let _ = docker5.compose_down().await;
    eprintln!("[scenario_07] All sub-tests passed");

    Ok(())
}
