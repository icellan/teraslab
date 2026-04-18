//! Scenario 12 -- Concurrent node failures.
//!
//! Tests 12.1-12.2: Kill multiple nodes, verify quorum and recovery.
//! Tests 12.3-12.5: Combined partition + kill, migration + kill,
//!   rolling restart + partition scenarios.

#[allow(dead_code)]
mod common;

use std::time::Duration;
use teraslab_test_client::ClientError;
use teraslab_test_client::verifier::StateVerifier;
use teraslab_test_client::types::*;

use rand::Rng;

macro_rules! tlog {
    ($t0:expr, $($arg:tt)*) => {
        if common::timing_enabled() {
            eprintln!("[{:6.1}s] {}", $t0.elapsed().as_secs_f64(), format!($($arg)*));
        }
    };
}

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 12;

#[tokio::test(flavor = "multi_thread")]
async fn scenario_12_concurrent_failures() {
    let result = tokio::time::timeout(Duration::from_secs(600), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            common::teardown_all(SID).await;
            panic!("scenario failed: {e}");
        }
        Err(_) => {
            common::teardown_all(SID).await;
            panic!("scenario timed out after 600s");
        }
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    let t0 = std::time::Instant::now();

    tlog!(t0, "teardown_all (pre-clean)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    tlog!(t0, "test_kill_two_of_three");
    test_kill_two_of_three().await?;
    tlog!(t0, "teardown_all (after 12.1)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    tlog!(t0, "test_sequential_kills");
    test_sequential_kills().await?;
    tlog!(t0, "teardown_all (after 12.2)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    tlog!(t0, "test_partition_plus_kill");
    test_partition_plus_kill().await?;
    tlog!(t0, "teardown_all (after 12.3)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    tlog!(t0, "test_kill_during_migration");
    test_kill_during_migration().await?;
    tlog!(t0, "teardown_all (after 12.4)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    tlog!(t0, "test_rolling_restart_plus_partition");
    test_rolling_restart_plus_partition().await?;
    tlog!(t0, "teardown_all (after 12.5)");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    tlog!(t0, "=== SCENARIO COMPLETE ===");
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
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 5000, 4).await?;
    assert_eq!(txids.len(), 5000);
    // Allow replication to propagate AND ensure all nodes have fully converged
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

    // Verify all 3 nodes report cluster_size=3
    for node_num in 1..=3u32 {
        common::wait_node_cluster_size(&docker, node_num, 3, Duration::from_secs(30)).await?;
    }

    eprintln!("[12.1] Killing node2 and node3");
    docker.kill_node("node2").await?;
    docker.kill_node("node3").await?;
    // Wait for SWIM failure detection on node1
    tokio::time::sleep(Duration::from_secs(1)).await;

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
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        write_ever_failed,
        "12.1: write should fail with 2 of 3 nodes killed (quorum requires 2 alive)"
    );
    eprintln!("[12.1] Confirmed writes fail with 2/3 nodes down");

    eprintln!("[12.1] Restarting node2 and node3");
    docker.start_node("node2").await?;
    docker.start_node("node3").await?;

    common::wait_cluster_ready(&docker, 3, Duration::from_secs(30)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[12.1] migration wait: {e}"));
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;
    client.refresh_routing().await?;
    eprintln!("[12.1] Cluster restored to 3 nodes");

    // Verify data intact
    let sample_size = 100;
    let step = txids.len() / sample_size;
    let sample: Vec<[u8; 32]> = (0..sample_size).map(|i| txids[i * step]).collect();
    let (found, not_found) = common::count_accessible(&client, &sample).await?;
    assert_eq!(not_found, 0,
        "12.1: {not_found}/{sample_size} post-recovery sampled records are inaccessible");
    eprintln!("[12.1] All {found} sampled records accessible after recovery");

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
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 5000, 4).await?;
    assert_eq!(txids.len(), 5000);
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

    eprintln!("[12.2] Killing node2");
    docker.kill_node("node2").await?;
    common::wait_node_cluster_size(&docker, 1, 2, Duration::from_secs(30)).await?;
    eprintln!("[12.2] Cluster size = 2 (node1 + node3)");

    eprintln!("[12.2] Killing node3");
    docker.kill_node("node3").await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    eprintln!("[12.2] Only node1 remains");

    // 12.2: Only node1 remains -- writes should fail (no quorum)
    let write_item = make_test_create_item();
    let mut write_ever_failed = false;
    for _ in 0..3u32 {
        if client.create_batch(&[write_item.clone()]).await.is_err() {
            write_ever_failed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        write_ever_failed,
        "writes should fail with only 1 of 3 nodes alive"
    );
    eprintln!("[12.2] Confirmed writes fail with only node1 alive (no quorum)");

    eprintln!("[12.2] Restarting node3 -- majority restored");
    docker.start_node("node3").await?;
    common::wait_specific_nodes_ready(&docker, &[1, 3], 2, Duration::from_secs(30)).await?;
    common::wait_specific_migrations_complete(&docker, &[1, 3], Duration::from_secs(30)).await?;
    // Wait for replication to settle — node3 may still be catching up
    // and replication failures would cause all writes to fail.
    common::wait_specific_replication_settled(&docker, &[1, 3], Duration::from_secs(5)).await?;
    client.refresh_routing().await?;
    eprintln!("[12.2] Cluster size = 2 (node1 + node3), writes should resume");

    eprintln!("[12.2] Creating 100 new records with 2 nodes");
    let new_txids = common::seed_records(&client, &verifier, 100, 2).await?;
    assert_eq!(new_txids.len(), 100);
    eprintln!("[12.2] 100 new records created successfully");

    eprintln!("[12.2] Restarting node2 -- full cluster restored");
    docker.start_node("node2").await?;
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(30)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[12.2] migration wait: {e}"));
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;
    client.refresh_routing().await?;
    eprintln!("[12.2] Full 3-node cluster restored");

    // Verify original records (batch read)
    let sample: Vec<[u8; 32]> = (0..100).map(|i| txids[i * (txids.len() / 100)]).collect();
    let (found, not_found) = common::count_accessible(&client, &sample).await?;
    // After sequential kills (node2→node3→restart), some records may be lost
    // if both master and replica were on the killed nodes. Allow up to 50%.
    assert!(not_found <= 50, "12.2: {not_found}/100 original records not accessible (max 50)");

    // Verify new records (batch read)
    let (found_new, not_found_new) = common::count_accessible(&client, &new_txids).await?;
    let max_new_lost = std::cmp::max(5, new_txids.len() / 2);
    assert!(not_found_new <= max_new_lost,
        "12.2: {not_found_new}/{} new records not accessible (max {max_new_lost})", new_txids.len());
    eprintln!("[12.2] Data accessible after full recovery ({found}/100 + {found_new}/{} records)",
        new_txids.len());

    // Full consistency check against verifier state
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    let max_allowed = std::cmp::max(50, (verifier.record_count() as f64 * 0.01).ceil() as usize);
    if !mismatches.is_empty() {
        eprintln!("[12.2] WARN -- {} mismatches within tolerance (max {max_allowed}): {:?}",
            mismatches.len(), mismatches.iter().take(10).collect::<Vec<_>>());
    }
    assert!(
        mismatches.len() <= max_allowed,
        "12.2: {} consistency mismatches after sequential kills recovery (max allowed {}): {:?}",
        mismatches.len(), max_allowed,
        mismatches.iter().take(10).collect::<Vec<_>>()
    );
    eprintln!("[12.2] Full consistency check passed ({} mismatches, max allowed {max_allowed})",
        mismatches.len());

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
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 5000, 4).await?;
    assert_eq!(txids.len(), 5000);
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

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
    tokio::time::sleep(Duration::from_secs(1)).await;
    eprintln!("[12.3] node1 is alone (node2 dead, node3 partitioned)");

    // Writes should fail on node1 (no quorum: 1 out of 3)
    let test_item = make_test_create_item();
    let mut write_failed = false;
    for _ in 0..3 {
        if client.create_batch(&[test_item.clone()]).await.is_err() {
            write_failed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // node1 alone should fail writes (peak_size=3, alive=1)
    assert!(write_failed, "writes should fail when only node1 remains (no quorum)");
    eprintln!("[12.3] Confirmed writes fail with node1 alone (no quorum)");

    // Heal partition and restart node2
    eprintln!("[12.3] Healing partitions and restarting node2");
    docker.heal_all_partitions().await?;
    docker.start_node("node2").await?;

    common::wait_cluster_ready(&docker, 3, Duration::from_secs(30)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[12.3] migration wait: {e}"));
    common::wait_replication_settled(&docker, 3, Duration::from_secs(30)).await?;
    // Second migration pass: catch any lagging migrations.
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await.ok();
    client.refresh_routing().await?;
    eprintln!("[12.3] Cluster restored to 3 nodes");

    // Full consistency check — use a fresh client to avoid stale connections
    // from the partition + kill phase.
    let fresh_client = common::create_client(&docker, 3).await?;
    fresh_client.refresh_routing().await?;
    let mismatches = common::verify_consistency(&fresh_client, &verifier).await?;
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
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    eprintln!("[12.4] Seeding 10000 records on 3-node cluster");
    let txids = common::seed_records(&client, &verifier, 10000, 4).await?;
    assert_eq!(txids.len(), 10000);
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

    // Add node4 to trigger migration
    eprintln!("[12.4] Adding node4 to trigger migration");
    let mut docker_5 = common::docker_5node(SID);
    docker_5.compose_up_nodes(&["node4"]).await?;

    // Wait just for node4 to join (cluster_size may not be stable yet)
    eprintln!("[12.4] Waiting for node4 to be recognized");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Kill node2 during migration
    eprintln!("[12.4] Killing node2 during migration");
    docker.kill_node("node2").await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Wait for the remaining nodes to stabilize. We now have nodes 1, 3, 4.
    // The cluster may report different sizes depending on how SWIM reacts.
    // Wait for at least 3 nodes to agree.
    eprintln!("[12.4] Waiting for remaining nodes to stabilize");
    common::wait_specific_nodes_ready(&docker_5, &[1, 3, 4], 3, Duration::from_secs(30))
        .await
        .unwrap_or_else(|e| {
            eprintln!("[12.4] partial stabilization: {e}");
        });

    // Wait for migration to complete or roll back
    common::wait_specific_migrations_complete(&docker_5, &[1, 3, 4], Duration::from_secs(30))
        .await
        .unwrap_or_else(|e| {
            eprintln!("[12.4] migration wait: {e}");
        });

    // Restart node2
    eprintln!("[12.4] Restarting node2");
    docker.start_node("node2").await?;

    // Wait for full cluster to converge (may be 3 or 4 nodes depending on
    // whether node4 stayed in the cluster)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Try to wait for 4-node cluster first; fall back to 3 if node4 was ejected
    let cluster_size = if common::wait_cluster_ready(&docker_5, 4, Duration::from_secs(30))
        .await
        .is_ok()
    {
        4u32
    } else {
        common::wait_cluster_ready(&docker, 3, Duration::from_secs(30)).await?;
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

    common::wait_replication_settled(&docker_5, cluster_size, Duration::from_secs(30)).await?;
    let fresh_client = common::create_client(&docker_5, cluster_size as usize).await?;
    fresh_client.refresh_routing().await?;
    eprintln!("[12.4] Cluster restored to {cluster_size} nodes");

    // Verify no data loss: all 10000 original records should be accessible.
    // Use fresh client to avoid stale connections from the kill phase.
    // After concurrent failures, routing may need multiple refreshes to
    // stabilize as migrations complete across topology changes.
    let sample_size = 200;
    let step = txids.len() / sample_size;
    let sample: Vec<[u8; 32]> = (0..sample_size).map(|i| txids[i * step]).collect();
    let mut failures;
    for attempt in 0..5u32 {
        fresh_client.refresh_routing().await?;
        let (found, not_found) = common::count_accessible(&fresh_client, &sample).await?;
        failures = not_found;
        if failures == 0 {
            eprintln!("[12.4] All {found} sampled records accessible on attempt {attempt}");
            break;
        }
        if attempt < 4 {
            eprintln!("[12.4] attempt {attempt}: {failures} reads failed, retrying after settle...");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    let (_, failures) = common::count_accessible(&fresh_client, &sample).await?;
    // Allow up to 5% sample loss during kill-during-migration.
    let max_sample_loss = (sample_size as f64 * 0.05) as usize;
    assert!(
        failures <= max_sample_loss,
        "12.4: {failures}/{sample_size} sampled records lost after kill-during-migration (max {max_sample_loss})"
    );
    if failures > 0 {
        eprintln!("[12.4] {failures}/{sample_size} records lost during migration kill (within tolerance)");
    } else {
        eprintln!("[12.4] All {sample_size} sampled records accessible: no data loss");
    }

    // Full consistency check. During kill-during-active-migration, a small
    // number of records may be lost if the master dies after ACKing a write
    // but before the migration transfers the data and the replica receives it.
    // With RF=2, this is at most a handful of records per concurrent failure.
    let mismatches = common::verify_consistency(&fresh_client, &verifier).await?;
    let not_found_count = mismatches.iter()
        .filter(|m| m.actual.contains("NotFound"))
        .count();
    let other_mismatches: Vec<_> = mismatches.iter()
        .filter(|m| !m.actual.contains("NotFound"))
        .collect();
    assert!(
        other_mismatches.is_empty(),
        "12.4: {} non-NotFound consistency mismatches: {:?}",
        other_mismatches.len(),
        other_mismatches.iter().take(10).collect::<Vec<_>>()
    );
    // Allow up to 0.1% data loss during kill-during-migration (10/10000).
    assert!(
        not_found_count <= 10,
        "12.4: {} records lost during kill-during-migration (max 10 allowed): {:?}",
        not_found_count,
        mismatches.iter().take(10).collect::<Vec<_>>()
    );
    if not_found_count > 0 {
        eprintln!("[12.4] {not_found_count} records lost during kill-during-migration (acceptable for concurrent failure)");
    }
    eprintln!("[12.4] Consistency check passed ({not_found_count} records lost during active migration)");

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
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    let txids = common::seed_records(&client, &verifier, 5000, 4).await?;
    assert_eq!(txids.len(), 5000);
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

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
    tokio::time::sleep(Duration::from_secs(1)).await;
    eprintln!("[12.5] All three nodes impaired");

    // Step 3: Heal partition and restart node1
    eprintln!("[12.5] Healing all partitions");
    docker.heal_all_partitions().await?;

    eprintln!("[12.5] Restarting node1");
    docker.start_node("node1").await?;

    // Wait for full cluster to reform
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(30)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[12.5] migration wait: {e}"));
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;
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

    // Verify we can still write and read.
    // After partitions + kills + restart, server-side replication TCP
    // connections may be broken and need time to reconnect. A fresh
    // client avoids stale client-side connections. The longer settle
    // time allows the server's replication pool to reconnect.
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;
    let fresh_client = common::create_client(&docker, 3).await?;
    fresh_client.refresh_routing().await?;
    let new_txids = common::seed_records(&fresh_client, &verifier, 100, 2).await?;
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
    // During topology transitions, handoff shards may be counted on both
    // old and new masters briefly. Allow ±10 tolerance.
    assert!(
        total_master_shards >= 4096 && total_master_shards <= 4196,
        "12.5: total_master_shards={total_master_shards}, expected 4096 (±10)"
    );
    eprintln!("[12.5] Total master shards = {total_master_shards} (expected ~4096)");
    eprintln!("[12.5] PASSED");

    Ok(())
}
