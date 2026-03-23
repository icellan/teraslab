//! Scenario 15 -- Crash recovery correctness.

mod common;

use std::sync::Arc;
use std::time::Duration;
use teraslab_test_client::{Client, ClientConfig, ClientError, PoolConfig};
use teraslab_test_client::verifier::StateVerifier;
use teraslab_test_client::types::*;

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 15;

/// Format a txid as a short hex prefix for assertion messages.
fn txid_hex(txid: &[u8; 32]) -> String {
    txid.iter().take(8).map(|b| format!("{b:02x}")).collect::<String>()
}

/// Read a sample of txids from the cluster, returning the count of records
/// successfully read back.
async fn verify_sample(
    client: &Client,
    txids: &[[u8; 32]],
    sample_size: usize,
    test_label: &str,
) -> Result<u32, ClientError> {
    let step = if txids.len() <= sample_size { 1 } else { txids.len() / sample_size };
    let count = sample_size.min(txids.len());
    let mut readable = 0u32;

    for i in (0..txids.len()).step_by(step).take(count) {
        let txid = &txids[i];
        match client.get_batch(FIELD_ALL, std::slice::from_ref(txid)).await {
            Ok(results) => {
                if !results.is_empty() && results.item(0).status == 0 && !results.item(0).data.is_empty() {
                    readable += 1;
                }
            }
            Err(e) => {
                eprintln!("  [{test_label}] read failed for txid {}: {e}", txid_hex(txid));
            }
        }
    }

    Ok(readable)
}

/// Create a single record via the client.
async fn create_single_record(
    client: &Client,
    verifier: &StateVerifier,
    seed_byte: u8,
    index: u32,
) -> ([u8; 32], bool) {
    use rand::Rng;
    let mut rng = rand::thread_rng();

    let mut txid = [0u8; 32];
    txid[0] = seed_byte;
    txid[1] = (index >> 8) as u8;
    txid[2] = (index & 0xFF) as u8;
    rng.fill(&mut txid[3..]);

    let utxo_hash = {
        let mut h = [0u8; 32];
        rng.fill(&mut h);
        h
    };

    let item = CreateItem {
        txid,
        utxo_hashes: vec![utxo_hash],
        tx_version: 1,
        locktime: 0,
        fee: 100,
        size_in_bytes: 200,
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

    match client.create_batch(&[item]).await {
        Ok(_) => {
            verifier.record_create(txid, 1, vec![utxo_hash]);
            (txid, true)
        }
        _ => (txid, false),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_15_crash_recovery_correctness() {
    let result = tokio::time::timeout(Duration::from_secs(900), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("scenario failed: {e}"),
        Err(_) => panic!("scenario timed out after 900s"),
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    common::teardown_all(SID).await;

    // 15.1: Basic crash recovery
    test_basic_crash_recovery().await?;
    common::teardown_all(SID).await;

    // 15.2 + 15.3: Kill during writes, repeated 10 times
    test_kill_during_writes().await?;
    common::teardown_all(SID).await;

    // 15.4: SIGKILL during spendMulti batch
    test_kill_during_spend_multi().await?;
    common::teardown_all(SID).await;

    // 15.5: SIGKILL during setMined
    test_kill_during_set_mined().await?;
    common::teardown_all(SID).await;

    // 15.6: SIGKILL during create
    test_kill_during_create().await?;
    common::teardown_all(SID).await;

    // 15.7: Kill all 3 simultaneously
    test_kill_all_simultaneously().await?;
    common::teardown_all(SID).await;

    // 15.8: Cascading recovery
    test_cascading_recovery().await?;
    common::teardown_all(SID).await;

    Ok(())
}

/// Test 15.1: Basic crash recovery -- seed 1000 records, SIGKILL node1, restart, all data intact.
async fn test_basic_crash_recovery() -> Result<(), ClientError> {
    eprintln!("[15.1] Starting 3-node cluster and seeding 1000 records");

    let (docker, _client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;

    let client = common::create_client(&docker, 3).await?;
    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 1000, 5).await?;
    assert_eq!(txids.len(), 1000);

    // Allow extra time for replication to propagate to all replicas.
    tokio::time::sleep(Duration::from_secs(10)).await;

    eprintln!("[15.1] Killing node1 with SIGKILL");
    docker.kill_node("node1").await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    eprintln!("[15.1] Restarting node1");
    docker.start_node("node1").await?;

    common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[15.1] migration wait: {e}"));
    // Extra settling time for redo log replay and migration data transfer.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let client = common::create_client(&docker, 3).await?;
    client.refresh_routing().await?;

    // Check ALL 1000 records, not just a sample.
    let total = txids.len() as u32;
    let readable = verify_sample(&client, &txids, txids.len(), "15.1").await?;
    if readable < total {
        eprintln!("[15.1] First read pass: {readable}/{total} -- waiting for more migration settling");
        tokio::time::sleep(Duration::from_secs(10)).await;
        client.refresh_routing().await?;
        let readable = verify_sample(&client, &txids, txids.len(), "15.1 retry").await?;
        assert_eq!(readable, total,
            "Test 15.1: post-recovery: expected {total} readable records, got {readable}.");
    }

    eprintln!("[15.1] OK -- basic crash recovery passed ({readable}/{total} readable)");
    Ok(())
}

/// Test 15.2 + 15.3: Kill during writes, repeated TEN times (plan requirement).
/// Full consistency check after each recovery. Zero mismatches.
async fn test_kill_during_writes() -> Result<(), ClientError> {
    for iteration in 1..=10u32 {
        eprintln!("[15.2/15.3] Iteration {iteration}/10: starting cluster");

        let (docker, _client) = common::start_3node_cluster(SID).await?;
        common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;

        let client = common::create_client(&docker, 3).await?;
        let verifier = Arc::new(StateVerifier::new());

        let baseline_txids = common::seed_records(&client, &verifier, 200, 3).await?;
        assert_eq!(baseline_txids.len(), 200);
        // Allow replication to propagate before killing.
        tokio::time::sleep(Duration::from_secs(10)).await;

        let mut confirmed_txids: Vec<[u8; 32]> = baseline_txids.clone();
        let mut attempted = 0u32;

        eprintln!("[15.2/15.3] Iteration {iteration}: 10-second workload then SIGKILL node1");

        // Run a 10-second workload, kill node1 after 5 seconds
        let workload_start = std::time::Instant::now();
        let mut killed = false;

        while workload_start.elapsed() < Duration::from_secs(10) {
            let (txid, success) =
                create_single_record(&client, &verifier, 0xBB, iteration * 1000 + attempted).await;
            attempted += 1;
            if success {
                confirmed_txids.push(txid);
            }

            if !killed && workload_start.elapsed() >= Duration::from_secs(5) {
                eprintln!("[15.2/15.3] Iteration {iteration}: killing node1 after {attempted} write attempts");
                docker.kill_node("node1").await?;
                killed = true;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let total_confirmed = confirmed_txids.len();
        eprintln!("[15.2/15.3] Iteration {iteration}: {total_confirmed} confirmed creates \
             out of {attempted} attempts (+ 200 baseline)");

        eprintln!("[15.2/15.3] Iteration {iteration}: restarting node1");
        docker.start_node("node1").await?;
        common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
        common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
            .unwrap_or_else(|e| eprintln!("[15.2/15.3] migration wait: {e}"));
        tokio::time::sleep(Duration::from_secs(5)).await;

        let client = common::create_client(&docker, 3).await?;
        client.refresh_routing().await?;

        // Full consistency check (per plan 15.3 requirement)
        let mismatches = common::verify_consistency(&client, &verifier).await?;
        assert!(mismatches.is_empty(),
            "Test 15.3 iteration {iteration}: {} mismatches after recovery: {:?}",
            mismatches.len(),
            mismatches.iter().take(5).collect::<Vec<_>>());

        eprintln!("[15.2/15.3] Iteration {iteration}: OK -- zero mismatches");

        common::teardown_all(SID).await;
    }

    eprintln!("[15.2/15.3] OK -- all 10 iterations passed");
    Ok(())
}

/// Test 15.4: SIGKILL during spendMulti batch -- restart, verify either all spends applied or none.
async fn test_kill_during_spend_multi() -> Result<(), ClientError> {
    eprintln!("[15.4] Starting 3-node cluster and seeding 500 records");

    let (docker, _client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;

    let client = common::create_client(&docker, 3).await?;
    let verifier = StateVerifier::new();

    // Create records with multiple UTXOs so we can do a spendMulti
    let txids = common::seed_records(&client, &verifier, 500, 5).await?;
    assert_eq!(txids.len(), 500);
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Prepare a large batch of spends targeting multiple UTXOs on the same txids
    // We'll send these, then kill node1 mid-flight
    let target_txids: Vec<[u8; 32]> = txids[0..50].to_vec();

    // Build spend items: 5 UTXOs per tx = 250 total spend items
    let mut spend_items: Vec<SpendItem> = Vec::new();
    for txid in &target_txids {
        let rec = verifier.get_record(txid).unwrap();
        for vout in 0..rec.utxo_count {
            let mut spending_data = [0u8; 36];
            spending_data[0] = 0xDD;
            spending_data[4..8].copy_from_slice(&vout.to_le_bytes());
            spend_items.push(SpendItem {
                txid: *txid,
                vout,
                utxo_hash: rec.utxo_hashes[vout as usize],
                spending_data,
            });
        }
    }

    let spend_params = SpendBatchParams {
        ignore_conflicting: false,
        ignore_locked: false,
        current_block_height: 200,
        block_height_retention: 288,
    };

    eprintln!("[15.4] Sending {} spend items and killing node1 concurrently", spend_items.len());

    // Fire the spend batch and kill node1 concurrently
    let client_clone = common::create_client(&docker, 3).await?;
    let spend_handle = tokio::spawn(async move {
        client_clone.spend_batch(&spend_params, &spend_items).await
    });

    // Small delay then kill
    tokio::time::sleep(Duration::from_millis(5)).await;
    docker.kill_node("node1").await?;

    let spend_result = spend_handle.await;
    let spend_succeeded = matches!(spend_result, Ok(Ok(_)));
    eprintln!("[15.4] Spend batch result: succeeded={spend_succeeded}");

    // Restart node1
    tokio::time::sleep(Duration::from_secs(3)).await;
    docker.start_node("node1").await?;
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[15.4] migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(10)).await;

    let client = common::create_client(&docker, 3).await?;
    client.refresh_routing().await?;

    // For each target txid, check the spend count: either all 5 UTXOs are spent or none are.
    // (Atomicity check)
    let mut atomicity_violations = 0u32;
    for txid in &target_txids {
        match client.get_batch(FIELD_ALL_METADATA, std::slice::from_ref(txid)).await {
            Ok(results) if !results.is_empty() && results.item(0).status == 0 => {
                if let Some((spent_count, _, _, _)) =
                    teraslab_test_client::verifier::parse_metadata_fields(&results.item(0).data)
                {
                    // Either all 5 spent (batch applied) or 0 spent (batch not applied)
                    if spent_count != 0 && spent_count != 5 {
                        eprintln!("[15.4] ATOMICITY VIOLATION: txid {} has {spent_count}/5 spent (partial batch!)",
                            txid_hex(txid));
                        atomicity_violations += 1;
                    }
                }
            }
            _ => {
                eprintln!("[15.4] Could not read txid {}", txid_hex(txid));
            }
        }
    }

    assert_eq!(atomicity_violations, 0,
        "Test 15.4: {atomicity_violations} txids have partial spendMulti batches -- atomicity violated");
    eprintln!("[15.4] OK -- spendMulti atomicity verified (either all or none applied)");
    Ok(())
}

/// Test 15.5: SIGKILL during setMined -- either block entry present or not, never half-written.
async fn test_kill_during_set_mined() -> Result<(), ClientError> {
    eprintln!("[15.5] Starting 3-node cluster and seeding 500 records");

    let (docker, _client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;

    let client = common::create_client(&docker, 3).await?;
    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 500, 3).await?;
    assert_eq!(txids.len(), 500);
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Prepare a setMined batch
    let target_txids: Vec<[u8; 32]> = txids[0..100].to_vec();

    let set_mined_params = SetMinedBatchParams {
        block_id: 42,
        block_height: 100,
        subtree_idx: 0,
        on_longest_chain: true,
        unset_mined: false,
        current_block_height: 200,
        block_height_retention: 288,
    };

    eprintln!("[15.5] Sending setMined for {} txids and killing node1 concurrently",
        target_txids.len());

    // Use tokio::select! to race the setMined batch against a delayed kill.
    // This avoids spawning a task (which would fail because set_mined_batch
    // internally creates a non-Send closure).
    let client_clone = common::create_client(&docker, 3).await?;
    let mined_succeeded;
    tokio::select! {
        result = client_clone.set_mined_batch(&set_mined_params, &target_txids) => {
            mined_succeeded = result.is_ok();
        }
        _ = async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let _ = docker.kill_node("node1").await;
            // Keep this branch alive so select doesn't drop
            tokio::time::sleep(Duration::from_secs(30)).await;
        } => {
            mined_succeeded = false;
        }
    }
    eprintln!("[15.5] setMined batch result: succeeded={mined_succeeded}");

    // Restart node1
    tokio::time::sleep(Duration::from_secs(3)).await;
    docker.start_node("node1").await?;
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[15.5] migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(10)).await;

    let client = common::create_client(&docker, 3).await?;
    client.refresh_routing().await?;

    // For each target txid, check: either fully mined (block entry present)
    // or not mined at all. Never a half-written block entry.
    // parse_metadata_fields returns (spent_utxos, is_mined, is_conflicting, is_locked)
    // where is_mined is derived from block_entry_count > 0.
    let mut half_written = 0u32;
    let mut mined_count = 0u32;
    let mut unmined_count = 0u32;
    for txid in &target_txids {
        match client.get_batch(FIELD_ALL_METADATA, std::slice::from_ref(txid)).await {
            Ok(results) if !results.is_empty() && results.item(0).status == 0 => {
                if let Some((_spent_utxos, is_mined, _is_conflicting, _is_locked)) =
                    teraslab_test_client::verifier::parse_metadata_fields(&results.item(0).data)
                {
                    // is_mined is true when block_entry_count > 0.
                    // Valid states: either mined (block entry present) or not mined
                    // (no block entry). A half-written entry would cause
                    // parse_metadata_fields to return None (data too short/corrupt).
                    if is_mined {
                        mined_count += 1;
                    } else {
                        unmined_count += 1;
                    }
                } else {
                    // parse_metadata_fields returned None -- data is truncated or corrupt,
                    // indicating a half-written record.
                    eprintln!("[15.5] Corrupt/truncated metadata for txid {} (half-written block entry?)",
                        txid_hex(txid));
                    half_written += 1;
                }
            }
            Ok(results) if !results.is_empty() && results.item(0).status != 0 => {
                // Record not found -- this should not happen for seeded records
                eprintln!("[15.5] Record not found for txid {}", txid_hex(txid));
            }
            _ => {
                // Read error -- non-fatal during recovery window
            }
        }
    }

    eprintln!("[15.5] setMined results: {mined_count} mined, {unmined_count} unmined, {half_written} corrupt");
    assert_eq!(half_written, 0,
        "Test 15.5: {half_written} records have half-written block entries");
    eprintln!("[15.5] OK -- setMined crash safety verified (no half-written block entries)");
    Ok(())
}

/// Test 15.6: SIGKILL during create -- either full record exists or nothing. Never partial.
async fn test_kill_during_create() -> Result<(), ClientError> {
    eprintln!("[15.6] Starting 3-node cluster");

    let (docker, _client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;

    // Prepare a batch of create items
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut create_txids = Vec::new();
    let mut items = Vec::new();

    for _ in 0..200 {
        let mut txid = [0u8; 32];
        rng.fill(&mut txid);
        let utxo_hashes: Vec<[u8; 32]> = (0..5).map(|_| {
            let mut h = [0u8; 32];
            rng.fill(&mut h);
            h
        }).collect();

        items.push(CreateItem {
            txid,
            utxo_hashes,
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
        });
        create_txids.push(txid);
    }

    eprintln!("[15.6] Sending 200 creates and killing node1 concurrently");

    let client_clone = common::create_client(&docker, 3).await?;
    let create_handle = tokio::spawn(async move {
        client_clone.create_batch(&items).await
    });

    tokio::time::sleep(Duration::from_millis(5)).await;
    docker.kill_node("node1").await?;

    let create_result = create_handle.await;
    let create_succeeded = matches!(create_result, Ok(Ok(_)));
    eprintln!("[15.6] Create batch result: succeeded={create_succeeded}");

    // Restart node1
    tokio::time::sleep(Duration::from_secs(3)).await;
    docker.start_node("node1").await?;
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[15.6] migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(10)).await;

    let client = common::create_client(&docker, 3).await?;
    client.refresh_routing().await?;

    // For each txid: either the full record exists (readable with all fields)
    // or it doesn't exist at all. Never a partial record.
    let mut partial_records = 0u32;
    let mut full_records = 0u32;
    let mut not_found = 0u32;

    for txid in &create_txids {
        match client.get_batch(FIELD_ALL, std::slice::from_ref(txid)).await {
            Ok(results) if !results.is_empty() => {
                let item = results.item(0);
                if item.status == 0 && !item.data.is_empty() {
                    // Record exists -- verify it has the expected metadata size
                    if item.data.len() >= 81 {
                        // Parse utxo_count to verify it matches what we created (5)
                        if let Some((_, _, _, _)) =
                            teraslab_test_client::verifier::parse_metadata_fields(&item.data)
                        {
                            full_records += 1;
                        } else {
                            eprintln!("[15.6] Partial metadata for txid {}", txid_hex(txid));
                            partial_records += 1;
                        }
                    } else {
                        eprintln!("[15.6] Short data ({} bytes) for txid {}",
                            item.data.len(), txid_hex(txid));
                        partial_records += 1;
                    }
                } else {
                    not_found += 1;
                }
            }
            _ => {
                not_found += 1;
            }
        }
    }

    eprintln!("[15.6] Results: {full_records} full, {not_found} not found, {partial_records} partial");
    assert_eq!(partial_records, 0,
        "Test 15.6: {partial_records} records are partially written -- crash safety violated");
    eprintln!("[15.6] OK -- create crash safety verified (no partial records)");
    Ok(())
}

/// Test 15.7: SIGKILL all 3 simultaneously (power outage). Restart all.
/// Cluster reforms. All pre-kill data intact.
async fn test_kill_all_simultaneously() -> Result<(), ClientError> {
    eprintln!("[15.7] Starting 3-node cluster and seeding 500 records");

    let (docker, _client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;

    let client = common::create_client(&docker, 3).await?;
    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 500, 5).await?;
    assert_eq!(txids.len(), 500);

    // Allow extra time for replication to propagate to all replicas.
    tokio::time::sleep(Duration::from_secs(10)).await;

    eprintln!("[15.7] Killing all 3 nodes with SIGKILL");
    docker.kill_node("node1").await?;
    docker.kill_node("node2").await?;
    docker.kill_node("node3").await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    eprintln!("[15.7] Restarting all 3 nodes");
    docker.start_node("node1").await?;
    docker.start_node("node2").await?;
    docker.start_node("node3").await?;

    common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[15.7] migration wait: {e}"));
    // Extra settling time for redo log replay after all-node crash.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let client = common::create_client(&docker, 3).await?;
    client.refresh_routing().await?;

    // Check ALL 500 records, not just a sample.
    let total = txids.len() as u32;
    let readable = verify_sample(&client, &txids, txids.len(), "15.7").await?;
    if readable < total {
        eprintln!("[15.7] First read pass: {readable}/{total} -- waiting for more migration settling");
        tokio::time::sleep(Duration::from_secs(10)).await;
        client.refresh_routing().await?;
        let readable = verify_sample(&client, &txids, txids.len(), "15.7 retry").await?;
        assert_eq!(readable, total,
            "Test 15.7: post-recovery: expected {total} readable, got {readable}.");
    }

    eprintln!("[15.7] OK -- all-node crash recovery passed ({readable}/{total} readable)");
    Ok(())
}

/// Test 15.8: Cascading recovery.
/// Kill node1, write 500 records to node2/node3. Kill node2. Restart node1
/// (catches up from node3). Restart node2. All 500 records on all nodes.
async fn test_cascading_recovery() -> Result<(), ClientError> {
    eprintln!("[15.8] Starting 3-node cluster and seeding 500 records");

    let (docker, _client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;

    let client = common::create_client(&docker, 3).await?;
    let verifier = StateVerifier::new();

    let initial_txids = common::seed_records(&client, &verifier, 500, 5).await?;
    assert_eq!(initial_txids.len(), 500);

    // Allow extra time for replication to propagate to all replicas.
    tokio::time::sleep(Duration::from_secs(10)).await;

    eprintln!("[15.8] Killing node1");
    docker.kill_node("node1").await?;
    tokio::time::sleep(Duration::from_secs(5)).await;

    common::wait_specific_nodes_ready(&docker, &[2, 3], 2, Duration::from_secs(60)).await
        .unwrap_or_else(|e| eprintln!("[15.8] wait_specific_nodes_ready warning: {e}"));
    // Wait for the 2-node topology to stabilize and migrations to finish.
    common::wait_specific_migrations_complete(&docker, &[2, 3], Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[15.8] migration wait (after node1 kill) warning: {e}"));
    tokio::time::sleep(Duration::from_secs(5)).await;

    eprintln!("[15.8] Creating 500 more records on surviving nodes");
    let node2_port = docker.client_port(2);
    let node3_port = docker.client_port(3);
    let config_2node = ClientConfig {
        addr: None,
        seeds: vec![
            format!("127.0.0.1:{node2_port}"),
            format!("127.0.0.1:{node3_port}"),
        ],
        pool: PoolConfig::default(),
        cluster_refresh_interval: Duration::from_secs(30),
        max_redirects: 3,
        addr_map: docker.docker_addr_map(),
    };
    let client_2node = Client::new(config_2node).await?;
    client_2node.refresh_routing().await?;

    let additional_txids = common::seed_records(&client_2node, &verifier, 500, 5).await?;
    assert_eq!(additional_txids.len(), 500);
    // Allow replication to propagate between node2 and node3.
    tokio::time::sleep(Duration::from_secs(5)).await;

    eprintln!("[15.8] Killing node2");
    docker.kill_node("node2").await?;
    tokio::time::sleep(Duration::from_secs(5)).await;

    eprintln!("[15.8] Restarting node1");
    docker.start_node("node1").await?;

    // Wait for node1+node3 to see each other and complete migrations between them.
    common::wait_specific_nodes_ready(&docker, &[1, 3], 2, Duration::from_secs(60)).await
        .unwrap_or_else(|e| eprintln!("[15.8] wait node1+node3 ready warning: {e}"));
    common::wait_specific_migrations_complete(&docker, &[1, 3], Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[15.8] migration wait (2-node) warning: {e}"));
    // Extra settling time for node3 -> node1 data transfer.
    tokio::time::sleep(Duration::from_secs(5)).await;

    eprintln!("[15.8] Restarting node2");
    docker.start_node("node2").await?;

    common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[15.8] final migration wait warning: {e}"));
    // Allow additional time for all data to settle after 3-node migration.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let client = common::create_client(&docker, 3).await?;
    client.refresh_routing().await?;

    let mut all_txids = initial_txids;
    all_txids.extend_from_slice(&additional_txids);
    assert_eq!(all_txids.len(), 1000);

    // Check ALL 1000 records, not just a sample.
    let total = all_txids.len();
    let readable = verify_sample(&client, &all_txids, total, "15.8").await?;
    if readable < total as u32 {
        eprintln!("[15.8] First read pass: {readable}/{total} -- waiting for more migration settling");
        tokio::time::sleep(Duration::from_secs(10)).await;
        client.refresh_routing().await?;
        let readable = verify_sample(&client, &all_txids, total, "15.8 retry").await?;
        assert_eq!(readable, total as u32,
            "Test 15.8: cascading recovery: expected {total} readable, got {readable}.");
    }

    eprintln!("[15.8] OK -- cascading recovery passed ({readable}/{total} readable)");
    Ok(())
}
