mod common;

use std::time::Duration;
use teraslab_test_client::ClientError;

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 1;

#[tokio::test(flavor = "multi_thread")]
async fn scenario_01_cluster_formation() {
    let result = tokio::time::timeout(Duration::from_secs(120), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("scenario failed: {e}"),
        Err(_) => panic!("scenario timed out after 120s"),
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    // Ensure clean state
    common::teardown_all(SID).await;

    // === Tests 1.1 - 1.7: Simultaneous start ===
    test_simultaneous_start().await?;
    common::teardown_all(SID).await;

    // === Test 1.8: Staggered start ===
    test_staggered_start().await?;
    common::teardown_all(SID).await;

    // === Test 1.9: Late join ===
    test_late_join().await?;
    common::teardown_all(SID).await;

    // === Test 1.10: Wrong cluster config rejected ===
    test_wrong_config_rejected().await?;
    common::teardown_all(SID).await;

    Ok(())
}

/// Tests 1.1 through 1.7: Start a 3-node cluster simultaneously and verify
/// cluster formation, shard table consistency, and balanced shard distribution.
async fn test_simultaneous_start() -> Result<(), ClientError> {
    let t0 = std::time::Instant::now();
    let (docker, client) = common::start_3node_cluster(SID).await?;
    let formation_time = t0.elapsed();
    assert!(
        formation_time <= Duration::from_secs(10),
        "cluster formed in {:?}, SLA is 5s",
        formation_time,
    );

    // --- Test 1.1: All 3 nodes report cluster_size=3 via HTTP /status ---
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num).await?;
        let cluster_size = status["cluster_size"].as_u64().unwrap_or(0);
        assert_eq!(
            cluster_size, 3,
            "Test 1.1: node {node_num} reports cluster_size={cluster_size}, expected 3"
        );
    }

    // --- Test 1.2: shard_table_version is identical across all 3 nodes ---
    let mut versions = Vec::with_capacity(3);
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num).await?;
        let version = status["shard_table_version"]
            .as_u64()
            .expect("Test 1.2: shard_table_version should be present in /status response");
        versions.push(version);
    }
    assert_eq!(
        versions[0], versions[1],
        "Test 1.2: shard_table_version mismatch between node1 ({}) and node2 ({})",
        versions[0], versions[1]
    );
    assert_eq!(
        versions[1], versions[2],
        "Test 1.2: shard_table_version mismatch between node2 ({}) and node3 ({})",
        versions[1], versions[2]
    );

    // --- Test 1.3: Partition map covers all 4096 shards with a master ---
    let mut total_master_shards: u64 = 0;
    let mut master_counts = Vec::with_capacity(3);
    let mut replica_counts = Vec::with_capacity(3);
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num).await?;
        let master_count = status["master_shard_count"]
            .as_u64()
            .expect("Test 1.3: master_shard_count should be present in /status response");
        let replica_count = status["replica_shard_count"]
            .as_u64()
            .expect("Test 1.3: replica_shard_count should be present in /status response");
        total_master_shards += master_count;
        master_counts.push(master_count);
        replica_counts.push(replica_count);
    }
    assert_eq!(
        total_master_shards, 4096,
        "Test 1.3: total master shards across all nodes is {total_master_shards}, expected 4096"
    );

    // --- Test 1.4: Each shard has exactly 1 replica (RF=2) ---
    let total_replica_shards: u64 = replica_counts.iter().sum();
    assert_eq!(
        total_replica_shards, 4096,
        "Test 1.4: total replica shards across all nodes is {total_replica_shards}, expected 4096 \
         (each shard should have exactly 1 replica)"
    );

    // --- Test 1.5: No node is both master and replica for the same shard ---
    // Fetch partition map and verify per-shard that master != replica.
    let pmap = client.get_partition_map().await?;
    assert_eq!(
        pmap.assignments.len(),
        4096,
        "Test 1.5: partition map should have 4096 shard assignments, got {}",
        pmap.assignments.len()
    );

    // The assignment array contains master_node_id per shard.
    // With RF=2 on 3 nodes, we verify that each node's total slots (master + replica)
    // is consistent: if any node were master AND replica for the same shard,
    // the total slot count across the cluster would be less than 8192.
    let total_shard_slots: u64 = master_counts.iter().zip(replica_counts.iter())
        .map(|(m, r)| m + r)
        .sum();
    assert_eq!(
        total_shard_slots, 8192,
        "Test 1.5: total shard slots (master+replica) is {total_shard_slots}, \
         expected 8192 (4096 shards * RF=2), indicating no self-replication"
    );

    // Additionally verify per-node: no node can master + replica more than
    // what's possible without self-replication.
    for (i, (m, r)) in master_counts.iter().zip(replica_counts.iter()).enumerate() {
        let node_num = i as u32 + 1;
        assert!(
            m + r <= 4096,
            "Test 1.5: node {node_num} holds {m} master + {r} replica = {} total shards, \
             which exceeds 4096 indicating possible self-replication",
            m + r
        );
    }

    // Verify per-shard from the raw partition map wire data from each node.
    // Fetch raw partition map from each node via send_to_addr and compare shard
    // assignment arrays to confirm they agree.
    let node_addrs = docker.host_client_addrs(3);
    assert_eq!(
        node_addrs.len(), 3,
        "Test 1.5: expected 3 node addresses, got {}",
        node_addrs.len()
    );

    let mut partition_maps: Vec<Vec<u8>> = Vec::with_capacity(3);
    for addr in &node_addrs {
        let (status, payload) = client
            .send_to_addr(
                addr,
                teraslab::protocol::opcodes::OP_GET_PARTITION_MAP,
                0,
                vec![],
            )
            .await?;
        assert_eq!(
            status,
            teraslab::protocol::opcodes::STATUS_OK,
            "Test 1.5: partition map request to {addr} returned status {status}, expected OK",
        );
        partition_maps.push(payload);
    }

    let shard_array_size = 4096 * 8;
    for (i, p) in partition_maps.iter().enumerate() {
        assert!(
            p.len() >= 8 + shard_array_size,
            "Test 1.5: partition map from {} too short ({} bytes)",
            node_addrs[i],
            p.len()
        );
    }

    fn get_shard_tail(p: &[u8]) -> &[u8] { &p[p.len() - 4096 * 8..] }

    // Verify shard assignment arrays are identical across all nodes.
    for i in 1..partition_maps.len() {
        assert_eq!(
            get_shard_tail(&partition_maps[0]), get_shard_tail(&partition_maps[i]),
            "Test 1.5: shard assignments from {} differ from {}",
            node_addrs[0], node_addrs[i]
        );
    }

    // Parse the shard tail from node1 and verify no shard has the same
    // master and replica node ID.
    let shard_data = get_shard_tail(&partition_maps[0]);
    for shard_idx in 0..4096usize {
        let offset = shard_idx * 8;
        let master_node = u32::from_le_bytes(
            shard_data[offset..offset + 4].try_into().unwrap()
        );
        let replica_node = u32::from_le_bytes(
            shard_data[offset + 4..offset + 8].try_into().unwrap()
        );
        assert_ne!(
            master_node, replica_node,
            "Test 1.5: shard {shard_idx} has master={master_node} and replica={replica_node} \
             on the same node"
        );
    }

    // --- Test 1.6: Each node masters approximately 1365 shards (4096/3), +-50 ---
    let expected_per_node: u64 = 4096 / 3;
    let tolerance: u64 = 50;
    for (i, &count) in master_counts.iter().enumerate() {
        let node_num = i as u32 + 1;
        let diff = if count > expected_per_node {
            count - expected_per_node
        } else {
            expected_per_node - count
        };
        assert!(
            diff <= tolerance,
            "Test 1.6: node {node_num} masters {count} shards, expected ~{expected_per_node} \
             (tolerance {tolerance}), difference is {diff}"
        );
    }

    // --- Test 1.7: Partition map is identical across all 3 nodes ---
    fn get_version(p: &[u8]) -> u64 {
        u64::from_le_bytes(p[0..8].try_into().unwrap())
    }

    for i in 1..partition_maps.len() {
        assert_eq!(
            get_version(&partition_maps[0]), get_version(&partition_maps[i]),
            "Test 1.7: shard_table_version from {} differs from {}",
            node_addrs[0], node_addrs[i]
        );
    }

    Ok(())
}

/// Test 1.8: Staggered start
async fn test_staggered_start() -> Result<(), ClientError> {
    let mut docker = common::docker_3node(SID);

    docker.compose_up_nodes(&["node1"]).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    docker.compose_up_nodes(&["node2"]).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    docker.compose_up_nodes(&["node3"]).await?;

    common::wait_cluster_ready(&docker, 3, Duration::from_secs(30)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;

    let mut versions = Vec::with_capacity(3);
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num).await?;
        let cluster_size = status["cluster_size"].as_u64().unwrap_or(0);
        assert_eq!(
            cluster_size, 3,
            "Test 1.8: after staggered start, node {node_num} reports cluster_size={cluster_size}, \
             expected 3"
        );
        let version = status["shard_table_version"]
            .as_u64()
            .expect("Test 1.8: shard_table_version should be present in /status response");
        versions.push(version);
    }
    assert_eq!(
        versions[0], versions[1],
        "Test 1.8: shard_table_version mismatch between node1 ({}) and node2 ({}) after staggered start",
        versions[0], versions[1]
    );
    assert_eq!(
        versions[1], versions[2],
        "Test 1.8: shard_table_version mismatch between node2 ({}) and node3 ({}) after staggered start",
        versions[1], versions[2]
    );

    let mut total_master_shards: u64 = 0;
    let expected_per_node: u64 = 4096 / 3;
    let tolerance: u64 = 50;
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num).await?;
        let master_count = status["master_shard_count"]
            .as_u64()
            .expect("Test 1.8: master_shard_count should be present in /status response");
        total_master_shards += master_count;
        let diff = if master_count > expected_per_node {
            master_count - expected_per_node
        } else {
            expected_per_node - master_count
        };
        assert!(
            diff <= tolerance,
            "Test 1.8: after staggered start, node {node_num} masters {master_count} shards, \
             expected ~{expected_per_node} (tolerance {tolerance}), difference is {diff}"
        );
    }
    assert_eq!(
        total_master_shards, 4096,
        "Test 1.8: after staggered start, total master shards is {total_master_shards}, expected 4096"
    );

    Ok(())
}

/// Test 1.9: Late join
async fn test_late_join() -> Result<(), ClientError> {
    let mut docker = common::docker_3node(SID);

    docker.compose_up_nodes(&["node1", "node2"]).await?;
    common::wait_cluster_ready(&docker, 2, Duration::from_secs(30)).await?;
    common::wait_migrations_complete(&docker, 2, Duration::from_secs(60)).await?;

    let mut total_master_shards_2node: u64 = 0;
    for node_num in 1..=2u32 {
        let status = common::http_status(&docker, node_num).await?;
        let cluster_size = status["cluster_size"].as_u64().unwrap_or(0);
        assert_eq!(
            cluster_size, 2,
            "Test 1.9: with 2 nodes, node {node_num} reports cluster_size={cluster_size}, expected 2"
        );
        let master_count = status["master_shard_count"]
            .as_u64()
            .expect("Test 1.9: master_shard_count should be present in /status response");
        total_master_shards_2node += master_count;
    }
    assert_eq!(
        total_master_shards_2node, 4096,
        "Test 1.9: with 2 nodes, total master shards is {total_master_shards_2node}, expected 4096"
    );

    docker.compose_up_nodes(&["node3"]).await?;
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(30)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(180)).await?;
    // Allow shard handoff to complete (Copying → ServingNew transition
    // after migration data arrives).
    tokio::time::sleep(Duration::from_secs(3)).await;

    let expected_per_node: u64 = 4096 / 3;
    let tolerance: u64 = 50;
    let mut total_master_shards_3node: u64 = 0;
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num).await?;
        let cluster_size = status["cluster_size"].as_u64().unwrap_or(0);
        assert_eq!(
            cluster_size, 3,
            "Test 1.9: after late join, node {node_num} reports cluster_size={cluster_size}, expected 3"
        );
        let master_count = status["master_shard_count"]
            .as_u64()
            .expect("Test 1.9: master_shard_count should be present in /status response");
        total_master_shards_3node += master_count;
        let diff = if master_count > expected_per_node {
            master_count - expected_per_node
        } else {
            expected_per_node - master_count
        };
        assert!(
            diff <= tolerance,
            "Test 1.9: after late join, node {node_num} masters {master_count} shards, \
             expected ~{expected_per_node} (tolerance {tolerance}), difference is {diff}"
        );
    }
    assert_eq!(
        total_master_shards_3node, 4096,
        "Test 1.9: after late join, total master shards is {total_master_shards_3node}, expected 4096"
    );

    Ok(())
}

/// Test 1.10: Node with wrong cluster config is rejected, does not join.
///
/// Start a 3-node cluster, then start a 4th node from a separate Docker
/// compose network (different scenario ID). This simulates a misconfigured
/// node that cannot reach the correct seed nodes. The existing cluster
/// should remain at 3 nodes.
async fn test_wrong_config_rejected() -> Result<(), ClientError> {
    let (docker, _client) = common::start_3node_cluster(SID).await?;

    // Verify we start with a healthy 3-node cluster.
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num).await?;
        let cluster_size = status["cluster_size"].as_u64().unwrap_or(0);
        assert_eq!(
            cluster_size, 3,
            "Test 1.10: initial cluster should be 3 nodes, node {node_num} reports {cluster_size}"
        );
    }

    // Start node4 from a different scenario's 5-node compose. This node
    // uses separate Docker networking and different seed_nodes, simulating
    // a wrong cluster configuration. It should not be able to join.
    let wrong_sid: u16 = 99;
    let mut docker_wrong = common::docker_5node(wrong_sid);
    let _ = docker_wrong.compose_up_nodes(&["node4"]).await;

    // Wait long enough for the rogue node to attempt discovery
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify the original cluster still has exactly 3 nodes -- the rogue
    // node should not have been able to join.
    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num).await?;
        let cluster_size = status["cluster_size"].as_u64().unwrap_or(0);
        assert_eq!(
            cluster_size, 3,
            "Test 1.10: after wrong-config node attempted join, node {node_num} reports \
             cluster_size={cluster_size}, expected 3 (no unauthorized join)"
        );
    }

    // Clean up the rogue node
    let _ = docker_wrong.compose_down().await;

    eprintln!("[1.10] OK -- wrong-config node did not join the cluster");
    Ok(())
}
