//! Scenario 08 -- Network partition and degraded-network resilience.

mod common;

use std::sync::Arc;
use std::time::Duration;
use teraslab_test_client::{Client, ClientConfig, ClientError, PoolConfig};
use teraslab_test_client::verifier::StateVerifier;
use teraslab_test_client::reporter::MetricsReporter;
use teraslab_test_client::types::*;

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 8;

/// Format a txid as a short hex prefix for assertion messages.
fn txid_hex(txid: &[u8; 32]) -> String {
    txid.iter().take(8).map(|b| format!("{b:02x}")).collect::<String>()
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_08_network_partition() {
    let result = tokio::time::timeout(Duration::from_secs(600), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("scenario failed: {e}"),
        Err(_) => panic!("scenario timed out after 600s"),
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    common::teardown_all(SID).await;

    // == Test 8a -- Minority isolation ==
    {
        eprintln!("[8a] === Minority isolation sub-scenario ===");

        let (mut docker, client) = common::start_3node_cluster(SID).await?;
        common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
        client.refresh_routing().await?;

        let verifier = StateVerifier::new();

        eprintln!("[8a.0] Seeding 1000 records with 10 UTXOs each");
        let initial_txids = common::seed_records(&client, &verifier, 1000, 10).await?;
        assert_eq!(initial_txids.len(), 1000);

        // Allow extra time for replication to propagate to all replicas.
        tokio::time::sleep(Duration::from_secs(10)).await;

        eprintln!("[8a.1] Partitioning node3 from node1 and node2");
        docker.partition_node("node3", &["node1", "node2"]).await?;

        tokio::time::sleep(Duration::from_secs(5)).await;

        let status_n1 = common::http_status(&docker, 1).await?;
        let cluster_size_n1 = status_n1["cluster_size"].as_u64().unwrap_or(0);
        eprintln!("[8a.1] node1 reports cluster_size={cluster_size_n1}");
        // The minority partition should result in exactly 2 nodes in the majority side.
        assert!(cluster_size_n1 == 2,
            "Test 8a.1: node1 reports cluster_size={cluster_size_n1}, expected exactly 2 \
             (majority partition of node1+node2 with node3 isolated)");

        client.refresh_routing().await?;

        // Verify that node3 REJECTS writes during partition.
        // Create a single-node client that connects ONLY to the partitioned node3.
        eprintln!("[8a.1b] Verifying node3 rejects writes during partition");
        let node3_addr = format!("127.0.0.1:{}", docker.client_port(3));
        let node3_config = ClientConfig {
            addr: Some(node3_addr.clone()),
            seeds: vec![],
            pool: PoolConfig::default(),
            cluster_refresh_interval: Duration::from_secs(300),
            max_redirects: 0,
            addr_map: std::collections::HashMap::new(),
        };
        // The connection to node3 may fail outright (if node3 is refusing connections)
        // or the create may fail with a replication error (since node3 cannot reach peers).
        let mut node3_write_rejected = match Client::new(node3_config).await {
            Ok(node3_client) => {
                let mut txid_for_node3 = [0u8; 32];
                txid_for_node3[0] = 0xFF;
                txid_for_node3[1] = 0x08;
                let reject_item = CreateItem {
                    txid: txid_for_node3,
                    utxo_hashes: vec![[0xAA; 32]],
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

                match node3_client.create_batch(&[reject_item]).await {
                    Ok(_) => {
                        eprintln!("[8a.1b] WARNING: node3 accepted write during partition");
                        false
                    }
                    Err(e) => {
                        eprintln!("[8a.1b] node3 correctly rejected write: {e}");
                        true
                    }
                }
            }
            Err(e) => {
                eprintln!("[8a.1b] node3 connection failed (also acceptable): {e}");
                true
            }
        };
        // Node3 should reject writes because it is in the minority partition.
        // If the SWIM protocol has not yet detected the partition, node3 may
        // still accept the write on the first attempt. Retry up to 10 times
        // with 1s sleep to allow SWIM detection to propagate.
        if !node3_write_rejected {
            eprintln!("[8a.1b] First attempt was not rejected, retrying up to 10 times...");
            for retry in 1..=10u32 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let node3_addr_retry = format!("127.0.0.1:{}", docker.client_port(3));
                let retry_config = ClientConfig {
                    addr: Some(node3_addr_retry),
                    seeds: vec![],
                    pool: PoolConfig::default(),
                    cluster_refresh_interval: Duration::from_secs(300),
                    max_redirects: 0,
                    addr_map: std::collections::HashMap::new(),
                };
                let rejected = match Client::new(retry_config).await {
                    Ok(retry_client) => {
                        let mut retry_txid = [0u8; 32];
                        retry_txid[0] = 0xFF;
                        retry_txid[1] = 0x08;
                        retry_txid[2] = retry as u8;
                        let retry_item = CreateItem {
                            txid: retry_txid,
                            utxo_hashes: vec![[0xBB; 32]],
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
                        retry_client.create_batch(&[retry_item]).await.is_err()
                    }
                    Err(_) => true,
                };
                if rejected {
                    eprintln!("[8a.1b] node3 correctly rejected write on retry {retry}");
                    node3_write_rejected = true;
                    break;
                }
                eprintln!("[8a.1b] retry {retry}: node3 still accepted write");
            }
        }
        assert!(node3_write_rejected,
            "node3 should reject writes during minority partition");

        eprintln!("[8a.2] Creating 200 records while node3 is isolated");
        let partition_txids = common::seed_records(&client, &verifier, 200, 10).await?;
        assert_eq!(partition_txids.len(), 200);
        eprintln!("[8a.2] OK -- created 200 records during partition");

        eprintln!("[8a.3] Healing partition on all nodes");
        docker.heal_partition("node3").await?;
        docker.heal_partition("node1").await?;
        docker.heal_partition("node2").await?;

        // After partition heal, SWIM must go through its full rediscovery cycle.
        tokio::time::sleep(Duration::from_secs(15)).await;
        common::wait_cluster_ready(&docker, 3, Duration::from_secs(120)).await?;
        common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
        client.refresh_routing().await?;

        eprintln!("[8a.3] OK -- cluster reconverged to size 3");

        // Verify ALL records post-heal (not a sample)
        eprintln!("[8a.4] Verifying ALL data integrity after healing");
        let all_txids: Vec<[u8; 32]> = initial_txids
            .iter()
            .chain(partition_txids.iter())
            .copied()
            .collect();

        let mut read_failures = 0u32;
        for chunk in all_txids.chunks(100) {
            let results = client.get_batch(FIELD_ALL, chunk).await?;
            for (i, result) in results.iter().enumerate() {
                if result.status() != 0 {
                    read_failures += 1;
                    eprintln!("Test 8a.4: txid {} returned unexpected result", txid_hex(&chunk[i]));
                }
            }
        }
        assert_eq!(read_failures, 0,
            "Test 8a.4: {read_failures}/{} records not accessible after partition heal", all_txids.len());
        eprintln!("[8a.4] OK -- all {} records intact after partition healing", all_txids.len());

        let _ = docker.compose_down().await;
        eprintln!("[8a] === Minority isolation sub-scenario complete ===");
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // == Test 8b -- Full isolation ==
    {
        eprintln!("[8b] === Full isolation sub-scenario ===");

        let (mut docker, client) = common::start_3node_cluster(SID).await?;
        common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
        client.refresh_routing().await?;

        let verifier = StateVerifier::new();

        eprintln!("[8b.0] Seeding 1000 records with 10 UTXOs each");
        let pre_partition_txids = common::seed_records(&client, &verifier, 1000, 10).await?;
        assert_eq!(pre_partition_txids.len(), 1000);

        // Allow replication to propagate
        tokio::time::sleep(Duration::from_secs(10)).await;

        eprintln!("[8b.1] Partitioning ALL 3 nodes from each other");
        // node1 isolated from node2 and node3
        docker.partition_node("node1", &["node2", "node3"]).await?;
        // node2 isolated from node3 (already isolated from node1 by the above)
        docker.partition_node("node2", &["node3"]).await?;

        tokio::time::sleep(Duration::from_secs(5)).await;

        // All writes should fail on all nodes when fully isolated
        eprintln!("[8b.2] Verifying all writes fail on all nodes");
        let mut all_writes_failed = true;
        for attempt in 0..3u32 {
            let mut txid = [0u8; 32];
            txid[0..4].copy_from_slice(&attempt.to_le_bytes());
            txid[4] = 0x8B;
            let item = CreateItem {
                txid,
                utxo_hashes: vec![[attempt as u8; 32]],
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

            match client.create_batch(&[item]).await {
                Ok(_) => {
                    eprintln!("[8b.2] WARNING: write succeeded during full isolation (attempt {attempt})");
                    // In a full partition, no node has majority so writes should fail.
                    // But the client may have cached routes from before the partition.
                    // Give the cluster time to detect the partition.
                    all_writes_failed = false;
                }
                Err(_) => {
                    eprintln!("[8b.2] Write correctly rejected during full isolation (attempt {attempt})");
                }
            }
        }
        // If the initial attempts did not all fail (e.g., due to cached routes before
        // partition detection), retry after longer delays to confirm writes eventually fail.
        if !all_writes_failed {
            eprintln!("[8b.2] Initial attempts had some successes, retrying with longer delays...");
            for round in 1..=3u32 {
                tokio::time::sleep(Duration::from_secs(3)).await;
                let mut round_all_failed = true;
                for attempt in 0..3u32 {
                    let mut txid = [0u8; 32];
                    txid[0..4].copy_from_slice(&(100 + round * 10 + attempt).to_le_bytes());
                    txid[4] = 0x8B;
                    let item = CreateItem {
                        txid,
                        utxo_hashes: vec![[attempt as u8; 32]],
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
                    match client.create_batch(&[item]).await {
                        Ok(_) => {
                            eprintln!("[8b.2] round {round} attempt {attempt}: write still succeeded");
                            round_all_failed = false;
                        }
                        Err(_) => {
                            eprintln!("[8b.2] round {round} attempt {attempt}: write rejected");
                        }
                    }
                }
                if round_all_failed {
                    all_writes_failed = true;
                    eprintln!("[8b.2] All writes failed in round {round}");
                    break;
                }
            }
        }
        assert!(all_writes_failed,
            "all nodes should reject writes during full isolation");

        eprintln!("[8b.3] Healing all partitions");
        docker.heal_all_partitions().await?;

        tokio::time::sleep(Duration::from_secs(15)).await;
        common::wait_cluster_ready(&docker, 3, Duration::from_secs(120)).await?;
        common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
        client.refresh_routing().await?;

        eprintln!("[8b.3] OK -- cluster reformed after full isolation");

        // Verify all pre-partition data is intact
        eprintln!("[8b.4] Verifying all pre-partition data intact");
        let mut read_failures = 0u32;
        for chunk in pre_partition_txids.chunks(100) {
            let results = client.get_batch(FIELD_ALL, chunk).await?;
            for (i, result) in results.iter().enumerate() {
                if result.status() != 0 {
                    read_failures += 1;
                    eprintln!("Test 8b.4: txid {} not found after heal", txid_hex(&chunk[i]));
                }
            }
        }
        assert_eq!(read_failures, 0,
            "Test 8b.4: {read_failures}/{} pre-partition records lost after full isolation heal",
            pre_partition_txids.len());
        eprintln!("[8b.4] OK -- all {} pre-partition records intact", pre_partition_txids.len());

        let _ = docker.compose_down().await;
        eprintln!("[8b] === Full isolation sub-scenario complete ===");
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // == Test 8c -- Slow network ==
    {
        eprintln!("[8c] === Slow network sub-scenario ===");

        let (mut docker, _client_orig) = common::start_3node_cluster(SID).await?;
        common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;

        // Create a client with extended timeouts for degraded network
        let slow_config = ClientConfig {
            addr: None,
            seeds: docker.host_client_addrs(3),
            pool: PoolConfig::default(),
            cluster_refresh_interval: Duration::from_secs(30),
            max_redirects: 5,
            addr_map: docker.docker_addr_map(),
        };
        let client = Client::new(slow_config).await?;
        client.refresh_routing().await?;

        let verifier = StateVerifier::new();

        eprintln!("[8c.0] Seeding 1000 records with 10 UTXOs each");
        let baseline_txids = common::seed_records(&client, &verifier, 1000, 10).await?;
        assert_eq!(baseline_txids.len(), 1000);

        // Allow extra time for replication to propagate to all replicas.
        tokio::time::sleep(Duration::from_secs(10)).await;

        eprintln!("[8c.1] Applying slow_network (200ms, 5%% loss) to all nodes");
        docker.slow_network("node1", 200, 5.0).await?;
        docker.slow_network("node2", 200, 5.0).await?;
        docker.slow_network("node3", 200, 5.0).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        // 60-second sustained workload under degraded network
        eprintln!("[8c.2] Running 60-second sustained workload under degraded network");
        let reporter = Arc::new(MetricsReporter::new());
        let workload_duration = Duration::from_secs(60);
        let deadline = tokio::time::Instant::now() + workload_duration;
        let mut slow_txids: Vec<[u8; 32]> = Vec::new();
        let mut slow_errors = 0u32;
        let mut total_ops = 0u32;
        let mut batch_idx = 0u32;

        while tokio::time::Instant::now() < deadline {
            batch_idx += 1;

            // Mix of creates and reads
            if batch_idx % 3 == 0 {
                // Read some baseline records
                let read_idx = (batch_idx as usize) % baseline_txids.len();
                let op_start = std::time::Instant::now();
                match client.get_batch(FIELD_ALL_METADATA, std::slice::from_ref(&baseline_txids[read_idx])).await {
                    Ok(_) => {
                        reporter.record("read", op_start.elapsed());
                        total_ops += 1;
                    }
                    Err(_) => {
                        slow_errors += 1;
                        total_ops += 1;
                    }
                }
            } else {
                // Create records
                let op_start = std::time::Instant::now();
                match common::seed_records(&client, &verifier, 5, 5).await {
                    Ok(batch) => {
                        reporter.record("create", op_start.elapsed());
                        slow_txids.extend_from_slice(&batch);
                        total_ops += 1;
                    }
                    Err(e) => {
                        slow_errors += 1;
                        total_ops += 1;
                        eprintln!("[8c.2] batch {batch_idx} failed under slow network: {e}");
                    }
                }
            }

            // Throttle to ~50 ops/sec
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        eprintln!("[8c.2] Workload complete: {total_ops} ops, {slow_errors} errors, {} records created",
            slow_txids.len());
        eprintln!("[8c.2] {}", reporter.format_summary());

        eprintln!("[8c.3] Clearing network degradation");
        docker.clear_all_networks().await?;

        tokio::time::sleep(Duration::from_secs(5)).await;
        common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
        common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await
            .unwrap_or_else(|e| eprintln!("[8c.3] migration wait: {e}"));
        client.refresh_routing().await?;

        // Check for false-positive node deaths: all 3 nodes still in cluster
        eprintln!("[8c.4] Verifying no false-positive node deaths");
        for node_num in 1..=3u32 {
            let status = common::http_status(&docker, node_num).await?;
            let cluster_size = status["cluster_size"].as_u64().unwrap_or(0);
            assert_eq!(cluster_size, 3,
                "Test 8c.4: node {node_num} reports cluster_size={cluster_size}, expected 3 \
                 (false-positive node death detected)");
        }
        eprintln!("[8c.4] OK -- all 3 nodes still in cluster after clearing degradation");

        eprintln!("[8c.5] Verifying records written during degradation");
        // Wait extra for deferred shard table swaps to complete, then use a fresh client
        tokio::time::sleep(Duration::from_secs(5)).await;
        let fresh_client = common::create_client(&docker, 3).await?;
        let mut verify_failures = 0u32;

        for txid in &slow_txids {
            let results = fresh_client.get_batch(FIELD_ALL, std::slice::from_ref(txid)).await?;
            if results.is_empty() || results.item(0).status != 0 {
                verify_failures += 1;
                eprintln!("Test 8c.5: txid {} returned unexpected result", txid_hex(txid));
            }
        }
        assert_eq!(verify_failures, 0,
            "Test 8c.5: {verify_failures}/{} records written during degradation are unreadable",
            slow_txids.len());
        eprintln!("[8c.5] OK -- all {} records written during degradation are readable",
            slow_txids.len());

        // Verify baseline records too
        let mut baseline_failures = 0u32;
        for chunk in baseline_txids.chunks(100) {
            let results = client.get_batch(FIELD_ALL, chunk).await?;
            for (i, result) in results.iter().enumerate() {
                if result.status() != 0 {
                    baseline_failures += 1;
                    eprintln!("Test 8c.5: baseline txid {} not found", txid_hex(&chunk[i]));
                }
            }
        }
        assert_eq!(baseline_failures, 0,
            "Test 8c.5: {baseline_failures}/{} baseline records lost", baseline_txids.len());
        eprintln!("[8c.5] OK -- baseline data also intact");

        // Full consistency check
        eprintln!("[8c.6] Full consistency check");
        let mismatches = common::verify_consistency(&client, &verifier).await?;
        assert!(mismatches.is_empty(),
            "Test 8c.6: verify_consistency found {} mismatches: {:?}",
            mismatches.len(),
            mismatches.iter().take(5).collect::<Vec<_>>());
        eprintln!("[8c.6] OK -- full consistency check passed");

        let _ = docker.compose_down().await;
        eprintln!("[8c] === Slow network sub-scenario complete ===");
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // == Test 8d -- Asymmetric partition ==
    {
        eprintln!("[8d] === Asymmetric partition sub-scenario ===");

        let (mut docker, client) = common::start_3node_cluster(SID).await?;
        common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
        client.refresh_routing().await?;

        let verifier = StateVerifier::new();

        eprintln!("[8d.0] Seeding 1000 records with 10 UTXOs each");
        let initial_txids = common::seed_records(&client, &verifier, 1000, 10).await?;
        assert_eq!(initial_txids.len(), 1000);

        // Allow replication to propagate
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Asymmetric partition: node1 <-> node3 broken, but node1 <-> node2 and node2 <-> node3 ok
        eprintln!("[8d.1] Creating asymmetric partition: node1 <-> node3 broken");
        docker.partition_node("node1", &["node3"]).await?;

        tokio::time::sleep(Duration::from_secs(5)).await;

        // 30-second workload during asymmetric partition
        eprintln!("[8d.2] Running 30-second workload during asymmetric partition");
        let reporter = Arc::new(MetricsReporter::new());
        let workload_duration = Duration::from_secs(30);
        let deadline = tokio::time::Instant::now() + workload_duration;
        let mut partition_txids: Vec<[u8; 32]> = Vec::new();
        let mut errors = 0u32;
        let mut total_ops = 0u32;
        let mut batch_idx = 0u32;

        while tokio::time::Instant::now() < deadline {
            batch_idx += 1;

            // Create records
            let op_start = std::time::Instant::now();
            match common::seed_records(&client, &verifier, 5, 5).await {
                Ok(batch) => {
                    reporter.record("create", op_start.elapsed());
                    partition_txids.extend_from_slice(&batch);
                    total_ops += 1;
                }
                Err(e) => {
                    errors += 1;
                    total_ops += 1;
                    eprintln!("[8d.2] batch {batch_idx} failed: {e}");
                }
            }

            // Read some records
            if !initial_txids.is_empty() {
                let read_idx = (batch_idx as usize) % initial_txids.len();
                let op_start = std::time::Instant::now();
                match client.get_batch(FIELD_ALL_METADATA, std::slice::from_ref(&initial_txids[read_idx])).await {
                    Ok(_) => {
                        reporter.record("read", op_start.elapsed());
                        total_ops += 1;
                    }
                    Err(_) => {
                        errors += 1;
                        total_ops += 1;
                    }
                }
            }

            // Throttle to ~50 ops/sec
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        eprintln!("[8d.2] Workload complete: {total_ops} ops, {errors} errors, {} records created",
            partition_txids.len());
        eprintln!("[8d.2] {}", reporter.format_summary());

        // Heal the asymmetric partition
        eprintln!("[8d.3] Healing asymmetric partition");
        docker.heal_all_partitions().await?;

        tokio::time::sleep(Duration::from_secs(15)).await;
        common::wait_cluster_ready(&docker, 3, Duration::from_secs(120)).await?;
        common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
        client.refresh_routing().await?;

        eprintln!("[8d.3] OK -- cluster reconverged after asymmetric partition heal");

        // Verify: no shard had writes accepted on two different masters
        // This is validated by checking full consistency -- if two masters accepted
        // conflicting writes for the same shard, the consistency check would fail.
        eprintln!("[8d.4] Verifying no split-brain writes (full consistency check)");
        let mismatches = common::verify_consistency(&client, &verifier).await?;
        assert!(mismatches.is_empty(),
            "Test 8d.4: verify_consistency found {} mismatches (possible split-brain): {:?}",
            mismatches.len(),
            mismatches.iter().take(5).collect::<Vec<_>>());
        eprintln!("[8d.4] OK -- no split-brain detected, full consistency check passed");

        // Verify all data accessible
        let all_txids: Vec<[u8; 32]> = initial_txids
            .iter()
            .chain(partition_txids.iter())
            .copied()
            .collect();

        let mut read_failures = 0u32;
        for chunk in all_txids.chunks(100) {
            let results = client.get_batch(FIELD_ALL, chunk).await?;
            for (i, result) in results.iter().enumerate() {
                if result.status() != 0 {
                    read_failures += 1;
                    eprintln!("Test 8d.4: txid {} not found", txid_hex(&chunk[i]));
                }
            }
        }
        assert_eq!(read_failures, 0,
            "Test 8d.4: {read_failures}/{} records not accessible after heal", all_txids.len());
        eprintln!("[8d.4] OK -- all {} records accessible after asymmetric partition heal", all_txids.len());

        let _ = docker.compose_down().await;
        eprintln!("[8d] === Asymmetric partition sub-scenario complete ===");
    }

    eprintln!("[scenario_08] All sub-tests passed");
    Ok(())
}
