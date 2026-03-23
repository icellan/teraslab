//! Scenario 12 -- Concurrent node failures.
//!
//! Tests 12.1-12.2: Kill multiple nodes, verify quorum and recovery.
//! Tests 12.3-12.5: Combined partition + kill, migration + kill,
//!   rolling restart + partition scenarios.

mod common;

use std::time::Duration;
use teraslab_test_client::ClientError;
use teraslab_test_client::verifier::StateVerifier;
use teraslab_test_client::types::*;

use rand::Rng;

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 12;

#[tokio::test(flavor = "multi_thread")]
async fn scenario_12_concurrent_failures() {
    let result = tokio::time::timeout(Duration::from_secs(600), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("scenario failed: {e}"),
        Err(_) => panic!("scenario timed out after 600s"),
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    common::teardown_all(SID).await;

    test_kill_two_of_three().await?;
    common::teardown_all(SID).await;

    test_sequential_kills().await?;
    common::teardown_all(SID).await;

    test_partition_plus_kill().await?;
    common::teardown_all(SID).await;

    test_kill_during_migration().await?;
    common::teardown_all(SID).await;

    test_rolling_restart_plus_partition().await?;
    common::teardown_all(SID).await;

    Ok(())
}

/// Helper: create a single random record for write testing.
fn make_test_create_item() -> CreateItem {
    let mut rng = rand::thread_rng();
    let mut txid = [0u8; 32];
    rng.fill(&mut txid);
    let mut utxo_hash = [0u8; 32];
    rng.fill(&mut utxo_hash);

    CreateItem {
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
    }
}

// =========================================================================
// Test 12.1: Kill 2 out of 3 nodes simultaneously
// =========================================================================

async fn test_kill_two_of_three() -> Result<(), ClientError> {
    eprintln!("[12.1] Starting kill-2-of-3 test");

    let (_docker, client) = common::start_3node_cluster(SID).await?;
    let docker = common::docker_3node(SID);
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 5000, 4).await?;
    assert_eq!(txids.len(), 5000);
    // Allow replication to propagate AND ensure all nodes have fully converged
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify all 3 nodes report cluster_size=3
    for node_num in 1..=3u32 {
        common::wait_node_cluster_size(&docker, node_num, 3, Duration::from_secs(30)).await?;
    }

    eprintln!("[12.1] Killing node2 and node3");
    docker.kill_node("node2").await?;
    docker.kill_node("node3").await?;
    // Wait for SWIM failure detection on node1
    tokio::time::sleep(Duration::from_secs(10)).await;

    eprintln!("[12.1] Attempting create with only 1 of 3 nodes alive");
    let test_item = make_test_create_item();

    // Try the write multiple times -- should fail (no quorum)
    let mut write_ever_failed = false;
    for attempt in 0..3u32 {
        let create_result = client.create_batch(&[test_item.clone()]).await;
        if create_result.is_err() {
            write_ever_failed = true;
            break;
        }
        eprintln!(
            "[12.1] Write attempt {attempt} unexpectedly succeeded, retrying after delay..."
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    assert!(
        write_ever_failed,
        "12.1: write should fail with 2 of 3 nodes killed (quorum requires 2 alive)"
    );
    eprintln!("[12.1] Confirmed writes fail with 2/3 nodes down");

    eprintln!("[12.1] Restarting node2 and node3");
    docker.start_node("node2").await?;
    docker.start_node("node3").await?;

    common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[12.1] migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(5)).await;
    client.refresh_routing().await?;
    eprintln!("[12.1] Cluster restored to 3 nodes");

    // Verify data intact
    let sample_size = 100;
    let step = txids.len() / sample_size;
    for i in 0..sample_size {
        let txid = txids[i * step];
        let results = client
            .get_batch(FIELD_ALL, std::slice::from_ref(&txid))
            .await?;
        assert!(
            !results.is_empty() && results.item(0).status == 0,
            "12.1: post-recovery read for sample {i} returned unexpected result"
        );
        assert!(
            !results.item(0).data.is_empty(),
            "12.1: post-recovery read for sample {i} returned empty data"
        );
    }
    eprintln!("[12.1] All {sample_size} sampled records accessible after recovery");

    // Full consistency check against verifier state
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    assert!(
        mismatches.is_empty(),
        "12.1: {} consistency mismatches after kill-2-of-3 recovery: {:?}",
        mismatches.len(),
        mismatches.iter().take(10).collect::<Vec<_>>()
    );
    eprintln!("[12.1] Full consistency check passed: zero mismatches");

    eprintln!("[12.1] PASSED");

    Ok(())
}

// =========================================================================
// Test 12.2: Sequential kills
// =========================================================================

async fn test_sequential_kills() -> Result<(), ClientError> {
    eprintln!("[12.2] Starting sequential kills test");

    let (_docker, client) = common::start_3node_cluster(SID).await?;
    let docker = common::docker_3node(SID);
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 5000, 4).await?;
    assert_eq!(txids.len(), 5000);
    tokio::time::sleep(Duration::from_secs(10)).await;

    eprintln!("[12.2] Killing node2");
    docker.kill_node("node2").await?;
    common::wait_node_cluster_size(&docker, 1, 2, Duration::from_secs(30)).await?;
    eprintln!("[12.2] Cluster size = 2 (node1 + node3)");

    eprintln!("[12.2] Killing node3");
    docker.kill_node("node3").await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    eprintln!("[12.2] Only node1 remains");

    // 12.2: Only node1 remains -- writes should fail (no quorum)
    let write_item = make_test_create_item();
    let mut write_ever_failed = false;
    for _ in 0..3u32 {
        if client.create_batch(&[write_item.clone()]).await.is_err() {
            write_ever_failed = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    assert!(
        write_ever_failed,
        "writes should fail with only 1 of 3 nodes alive"
    );
    eprintln!("[12.2] Confirmed writes fail with only node1 alive (no quorum)");

    eprintln!("[12.2] Restarting node3 -- majority restored");
    docker.start_node("node3").await?;
    common::wait_specific_nodes_ready(&docker, &[1, 3], 2, Duration::from_secs(60)).await?;
    common::wait_specific_migrations_complete(&docker, &[1, 3], Duration::from_secs(60)).await?;
    client.refresh_routing().await?;
    eprintln!("[12.2] Cluster size = 2 (node1 + node3), writes should resume");

    eprintln!("[12.2] Creating 100 new records with 2 nodes");
    let new_txids = common::seed_records(&client, &verifier, 100, 2).await?;
    assert_eq!(new_txids.len(), 100);
    eprintln!("[12.2] 100 new records created successfully");

    eprintln!("[12.2] Restarting node2 -- full cluster restored");
    docker.start_node("node2").await?;
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[12.2] migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(5)).await;
    client.refresh_routing().await?;
    eprintln!("[12.2] Full 3-node cluster restored");

    // Verify original records
    let sample_size = 100;
    let step = txids.len() / sample_size;
    for i in 0..sample_size {
        let txid = txids[i * step];
        let results = client
            .get_batch(FIELD_ALL, std::slice::from_ref(&txid))
            .await?;
        assert!(
            !results.is_empty() && results.item(0).status == 0,
            "12.2: original record sample {i} not accessible after full recovery"
        );
    }

    // Verify new records
    for (i, txid) in new_txids.iter().enumerate() {
        let results = client
            .get_batch(FIELD_ALL, std::slice::from_ref(txid))
            .await?;
        assert!(
            !results.is_empty() && results.item(0).status == 0,
            "12.2: new record {i} (created with 2 nodes) not accessible"
        );
    }
    eprintln!("[12.2] All data accessible after full recovery");

    // Full consistency check against verifier state
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    assert!(
        mismatches.is_empty(),
        "12.2: {} consistency mismatches after sequential kills recovery: {:?}",
        mismatches.len(),
        mismatches.iter().take(10).collect::<Vec<_>>()
    );
    eprintln!("[12.2] Full consistency check passed: zero mismatches");

    eprintln!("[12.2] PASSED");

    Ok(())
}

// =========================================================================
// Test 12.3: Partition node3 + SIGKILL node2 simultaneously.
//   node1 alone. Heal + restart. Consistent state.
// =========================================================================

async fn test_partition_plus_kill() -> Result<(), ClientError> {
    eprintln!("[12.3] Starting partition + kill test");

    let (_docker, client) = common::start_3node_cluster(SID).await?;
    let docker = common::docker_3node(SID);
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 5000, 4).await?;
    assert_eq!(txids.len(), 5000);
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Ensure all nodes see cluster_size=3
    for node_num in 1..=3u32 {
        common::wait_node_cluster_size(&docker, node_num, 3, Duration::from_secs(30)).await?;
    }

    eprintln!("[12.3] Partitioning node3 from node1 and node2");
    docker.partition_node("node3", &["node1", "node2"]).await?;

    eprintln!("[12.3] Killing node2 with SIGKILL");
    docker.kill_node("node2").await?;

    // Wait for failure detection. node1 is now alone:
    //   - node2 is dead
    //   - node3 is partitioned (unreachable)
    tokio::time::sleep(Duration::from_secs(10)).await;
    eprintln!("[12.3] node1 is alone (node2 dead, node3 partitioned)");

    // Writes should fail on node1 (no quorum: 1 out of 3)
    let test_item = make_test_create_item();
    let mut write_failed = false;
    for _ in 0..3 {
        if client.create_batch(&[test_item.clone()]).await.is_err() {
            write_failed = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    // node1 alone should fail writes (peak_size=3, alive=1)
    assert!(write_failed, "writes should fail when only node1 remains (no quorum)");
    eprintln!("[12.3] Confirmed writes fail with node1 alone (no quorum)");

    // Heal partition and restart node2
    eprintln!("[12.3] Healing partitions and restarting node2");
    docker.heal_all_partitions().await?;
    docker.start_node("node2").await?;

    common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[12.3] migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(5)).await;
    client.refresh_routing().await?;
    eprintln!("[12.3] Cluster restored to 3 nodes");

    // Full consistency check
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    assert!(
        mismatches.is_empty(),
        "12.3: {} consistency mismatches after partition+kill recovery: {:?}",
        mismatches.len(),
        mismatches.iter().take(10).collect::<Vec<_>>()
    );
    eprintln!("[12.3] Full consistency check passed: zero mismatches");
    eprintln!("[12.3] PASSED");

    Ok(())
}

// =========================================================================
// Test 12.4: During migration (add node4): kill node2.
//   Migration completes or rolls back. No data loss. Restart node2. Consistent.
// =========================================================================

async fn test_kill_during_migration() -> Result<(), ClientError> {
    eprintln!("[12.4] Starting kill-during-migration test");

    let (_docker, client) = common::start_3node_cluster(SID).await?;
    let docker = common::docker_3node(SID);
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    eprintln!("[12.4] Seeding 10000 records on 3-node cluster");
    let txids = common::seed_records(&client, &verifier, 10000, 4).await?;
    assert_eq!(txids.len(), 10000);
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Add node4 to trigger migration
    eprintln!("[12.4] Adding node4 to trigger migration");
    let mut docker_5 = common::docker_5node(SID);
    docker_5.compose_up_nodes(&["node4"]).await?;

    // Wait just for node4 to join (cluster_size may not be stable yet)
    eprintln!("[12.4] Waiting for node4 to be recognized");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Kill node2 during migration
    eprintln!("[12.4] Killing node2 during migration");
    docker.kill_node("node2").await?;
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Wait for the remaining nodes to stabilize. We now have nodes 1, 3, 4.
    // The cluster may report different sizes depending on how SWIM reacts.
    // Wait for at least 3 nodes to agree.
    eprintln!("[12.4] Waiting for remaining nodes to stabilize");
    common::wait_specific_nodes_ready(&docker_5, &[1, 3, 4], 3, Duration::from_secs(120))
        .await
        .unwrap_or_else(|e| {
            eprintln!("[12.4] partial stabilization: {e}");
        });

    // Wait for migration to complete or roll back
    common::wait_specific_migrations_complete(&docker_5, &[1, 3, 4], Duration::from_secs(120))
        .await
        .unwrap_or_else(|e| {
            eprintln!("[12.4] migration wait: {e}");
        });

    // Restart node2
    eprintln!("[12.4] Restarting node2");
    docker.start_node("node2").await?;

    // Wait for full cluster to converge (may be 3 or 4 nodes depending on
    // whether node4 stayed in the cluster)
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Try to wait for 4-node cluster first; fall back to 3 if node4 was ejected
    let cluster_size = if common::wait_cluster_ready(&docker_5, 4, Duration::from_secs(60))
        .await
        .is_ok()
    {
        4u32
    } else {
        common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
        3u32
    };

    let wait_nodes: Vec<u32> = (1..=cluster_size).collect();
    common::wait_specific_migrations_complete(
        &docker_5,
        &wait_nodes,
        Duration::from_secs(120),
    )
    .await
    .unwrap_or_else(|e| eprintln!("[12.4] final migration wait: {e}"));

    tokio::time::sleep(Duration::from_secs(5)).await;
    client.refresh_routing().await?;
    eprintln!("[12.4] Cluster restored to {cluster_size} nodes");

    // Verify no data loss: all 10000 original records should be accessible
    let sample_size = 200;
    let step = txids.len() / sample_size;
    let mut failures = 0u32;
    for i in 0..sample_size {
        let txid = txids[i * step];
        match client
            .get_batch(FIELD_ALL, std::slice::from_ref(&txid))
            .await
        {
            Ok(results)
                if !results.is_empty()
                    && results.item(0).status == 0
                    && !results.item(0).data.is_empty() => {}
            _ => {
                failures += 1;
            }
        }
    }
    assert_eq!(
        failures, 0,
        "12.4: {failures}/{sample_size} sampled records lost after kill-during-migration"
    );
    eprintln!("[12.4] All {sample_size} sampled records accessible: no data loss");

    // Full consistency check
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    assert!(
        mismatches.is_empty(),
        "12.4: {} consistency mismatches: {:?}",
        mismatches.len(),
        mismatches.iter().take(10).collect::<Vec<_>>()
    );
    eprintln!("[12.4] Full consistency check passed: zero mismatches");

    let _ = docker_5.compose_down().await;
    eprintln!("[12.4] PASSED");

    Ok(())
}

// =========================================================================
// Test 12.5: During rolling restart of node1: partition node2<->node3.
//   All three impaired. Heal + restart. Consistent.
// =========================================================================

async fn test_rolling_restart_plus_partition() -> Result<(), ClientError> {
    eprintln!("[12.5] Starting rolling-restart + partition test");

    let (_docker, client) = common::start_3node_cluster(SID).await?;
    let docker = common::docker_3node(SID);
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 5000, 4).await?;
    assert_eq!(txids.len(), 5000);
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Ensure all nodes see cluster_size=3
    for node_num in 1..=3u32 {
        common::wait_node_cluster_size(&docker, node_num, 3, Duration::from_secs(30)).await?;
    }

    // Step 1: Begin rolling restart of node1 -- quiesce and stop
    eprintln!("[12.5] Quiescing node1 for rolling restart");
    common::http_quiesce(&docker, 1).await?;

    // Wait for node1 master_shard_count to reach 0
    let quiesce_start = std::time::Instant::now();
    let quiesce_timeout = Duration::from_secs(60);
    loop {
        let status = common::http_status(&docker, 1).await?;
        let master_count = status["master_shard_count"].as_u64().unwrap_or(u64::MAX);
        if master_count == 0 {
            eprintln!(
                "[12.5] node1 master_shard_count reached 0 in {:?}",
                quiesce_start.elapsed()
            );
            break;
        }
        if quiesce_start.elapsed() >= quiesce_timeout {
            return Err(ClientError::Connection(format!(
                "node1 still has {master_count} master shards after {quiesce_timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    docker.stop_node("node1").await?;
    eprintln!("[12.5] node1 stopped");

    // Step 2: While node1 is down, partition node2 <-> node3
    eprintln!("[12.5] Partitioning node2 <-> node3");
    docker.partition_node("node2", &["node3"]).await?;

    // All three nodes are now impaired:
    //   - node1: stopped
    //   - node2: partitioned from node3
    //   - node3: partitioned from node2
    tokio::time::sleep(Duration::from_secs(10)).await;
    eprintln!("[12.5] All three nodes impaired");

    // Step 3: Heal partition and restart node1
    eprintln!("[12.5] Healing all partitions");
    docker.heal_all_partitions().await?;

    eprintln!("[12.5] Restarting node1");
    docker.start_node("node1").await?;

    // Wait for full cluster to reform
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(120)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[12.5] migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(10)).await;
    client.refresh_routing().await?;
    eprintln!("[12.5] Cluster restored to 3 nodes");

    // Full consistency check
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    assert!(
        mismatches.is_empty(),
        "12.5: {} consistency mismatches after rolling-restart + partition: {:?}",
        mismatches.len(),
        mismatches.iter().take(10).collect::<Vec<_>>()
    );
    eprintln!("[12.5] Full consistency check passed: zero mismatches");

    // Verify we can still write and read
    let new_txids = common::seed_records(&client, &verifier, 100, 2).await?;
    assert_eq!(new_txids.len(), 100);
    eprintln!("[12.5] 100 new records created successfully after recovery");

    // Verify total master shard count
    let mut total_master_shards: u64 = 0;
    for node_num in 1u32..=3 {
        let status = common::http_status(&docker, node_num).await?;
        let master_count = status["master_shard_count"]
            .as_u64()
            .expect("master_shard_count should be present");
        total_master_shards += master_count;
    }
    assert_eq!(total_master_shards, 4096);
    eprintln!("[12.5] Total master shards = 4096 -- correct");
    eprintln!("[12.5] PASSED");

    Ok(())
}
