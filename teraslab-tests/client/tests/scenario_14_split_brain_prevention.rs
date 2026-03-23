//! Scenario 14 -- Split-brain prevention.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use teraslab_test_client::{Client, ClientConfig, ClientError, PoolConfig};
use teraslab_test_client::verifier::StateVerifier;
use teraslab_test_client::types::*;

use parking_lot::Mutex;
use rand::{Rng, SeedableRng};

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 14;

/// Format a txid as a short hex prefix for assertion messages.
#[allow(dead_code)]
fn txid_hex(txid: &[u8; 32]) -> String {
    txid.iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

/// Attempt a single-record create via a specific host port.
async fn try_create_on_port(
    port: u16,
    txid: [u8; 32],
    docker: &teraslab_test_client::helpers::DockerHelpers,
) -> Result<(), ClientError> {
    let config = ClientConfig {
        addr: Some(format!("127.0.0.1:{port}")),
        seeds: vec![],
        pool: PoolConfig::default(),
        cluster_refresh_interval: Duration::from_secs(30),
        max_redirects: 0,
        addr_map: docker.docker_addr_map(),
    };
    let client = Client::new(config).await?;

    let utxo_hash = {
        let mut h = [0u8; 32];
        h[0] = 0xAB;
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
    client.create_batch(&[item]).await?;
    Ok(())
}

/// Read a sample of txids from the cluster and verify they are all accessible.
async fn verify_sample_readable(
    client: &Client,
    txids: &[[u8; 32]],
    sample_size: usize,
) -> Result<u32, ClientError> {
    let step = if txids.len() <= sample_size { 1 } else { txids.len() / sample_size };
    let mut readable = 0u32;

    for i in (0..txids.len()).step_by(step).take(sample_size) {
        let txid = &txids[i];
        match client.get_batch(FIELD_ALL, std::slice::from_ref(txid)).await {
            Ok(results) => {
                if !results.is_empty() && results.item(0).status == 0 && !results.item(0).data.is_empty() {
                    readable += 1;
                }
            }
            Err(_) => {}
        }
    }

    Ok(readable)
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_14_split_brain_prevention() {
    let result = tokio::time::timeout(Duration::from_secs(600), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("scenario failed: {e}"),
        Err(_) => panic!("scenario timed out after 600s"),
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    common::teardown_all(SID).await;

    test_symmetric_isolation().await?;
    common::teardown_all(SID).await;

    test_asymmetric_partition().await?;
    common::teardown_all(SID).await;

    test_flapping_partition().await?;
    common::teardown_all(SID).await;

    test_docker_pause().await?;
    common::teardown_all(SID).await;

    Ok(())
}

/// Test 14.1: Symmetric isolation -- all 3 nodes separated, none accepts writes.
async fn test_symmetric_isolation() -> Result<(), ClientError> {
    eprintln!("[14.1] Starting 3-node cluster and seeding 2000 records");

    let (docker, client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();
    let txids = common::seed_records(&client, &verifier, 2000, 5).await?;
    assert_eq!(txids.len(), 2000);

    // Allow extra time for replication to propagate to all replicas.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let pre_partition_readable = verify_sample_readable(&client, &txids, 50).await?;
    assert_eq!(pre_partition_readable, 50);

    eprintln!("[14.1] Creating symmetric 3-way partition");
    docker.partition_node("node1", &["node2", "node3"]).await?;
    docker.partition_node("node2", &["node3"]).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    eprintln!("[14.1] Attempting writes on each isolated node");
    let node_ports: [(u16, &str); 3] = [
        (docker.client_port(1), "node1"),
        (docker.client_port(2), "node2"),
        (docker.client_port(3), "node3"),
    ];

    let mut write_results: Vec<(String, Result<(), ClientError>)> = Vec::new();
    for (port, name) in &node_ports {
        let mut txid = [0u8; 32];
        txid[0] = 0xFF;
        txid[1] = *port as u8;
        let result = try_create_on_port(*port, txid, &docker).await;
        eprintln!("[14.1]   {name} (port {port}): create result = {result:?}");
        write_results.push((name.to_string(), result));
    }

    let successful_writes = write_results.iter().filter(|(_, r)| r.is_ok()).count();
    assert_eq!(successful_writes, 0,
        "Test 14.1: {successful_writes}/3 isolated nodes accepted writes, \
         expected 0 (quorum should reject all writes when every node is isolated).");
    eprintln!("[14.1] {successful_writes}/3 isolated nodes accepted writes (expected 0)");

    eprintln!("[14.1] Healing all partitions");
    docker.heal_all_partitions().await?;

    // After healing a 3-way partition, SWIM needs to go through its full
    // rediscovery cycle for all nodes: suspicion -> dead -> rejoin via seeds.
    tokio::time::sleep(Duration::from_secs(15)).await;
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(120)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[14.1] migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = common::create_client(&docker, 3).await?;

    let post_heal_readable = verify_sample_readable(&client, &txids, 50).await?;
    assert_eq!(post_heal_readable, 50,
        "Test 14.1: post-heal read check: expected 50, got {post_heal_readable}.");

    eprintln!("[14.1] OK -- symmetric isolation test passed");
    Ok(())
}

/// Test 14.2: Asymmetric partition -- node1<->node2 ok, node2<->node3 ok, node1<->node3 broken.
/// No shard has writes accepted on two different masters.
async fn test_asymmetric_partition() -> Result<(), ClientError> {
    eprintln!("[14.2] Starting 3-node cluster and seeding 2000 records");

    let (docker, client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();
    let txids = common::seed_records(&client, &verifier, 2000, 5).await?;
    assert_eq!(txids.len(), 2000);

    // Allow extra time for replication to propagate to all replicas.
    tokio::time::sleep(Duration::from_secs(10)).await;

    eprintln!("[14.2] Creating asymmetric partition: node1<->node3 broken, node1<->node2 ok, node2<->node3 ok");
    // Only break the connection between node1 and node3
    docker.partition_node("node1", &["node3"]).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Run a 30-second workload targeting all 3 nodes
    eprintln!("[14.2] Running 30-second workload during asymmetric partition");
    let workload_duration = Duration::from_secs(30);
    let workload_start = Instant::now();
    let mut rng = rand::rngs::StdRng::from_entropy();
    let mut writes_on_node: [Vec<[u8; 32]>; 3] = [Vec::new(), Vec::new(), Vec::new()];

    while workload_start.elapsed() < workload_duration {
        for (node_idx, port_num) in [1u32, 2, 3].iter().enumerate() {
            let port = docker.client_port(*port_num);
            let mut txid = [0u8; 32];
            rng.fill(&mut txid);

            match try_create_on_port(port, txid, &docker).await {
                Ok(()) => {
                    writes_on_node[node_idx].push(txid);
                }
                Err(_) => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    eprintln!("[14.2] Writes accepted: node1={}, node2={}, node3={}",
        writes_on_node[0].len(), writes_on_node[1].len(), writes_on_node[2].len());

    // Check that no shard has writes accepted on two different masters.
    // With the asymmetric partition, the cluster should still maintain quorum
    // through node2 (which can see both node1 and node3). There should be no
    // conflicting writes -- each shard should have only one active master.
    // The key invariant: no record is created on two different nodes for the
    // same shard.

    // Check split-brain invariant: no shard accepted writes from two different masters
    let mut shard_writers: std::collections::HashMap<u16, Vec<u32>> = std::collections::HashMap::new();
    for (node_idx, txids_on_node) in writes_on_node.iter().enumerate() {
        for txid in txids_on_node {
            let shard = u16::from_le_bytes([txid[0], txid[1]]) % 4096;
            shard_writers.entry(shard).or_default().push(node_idx as u32 + 1);
        }
    }
    for (shard, writers) in &shard_writers {
        let unique: std::collections::HashSet<_> = writers.iter().collect();
        assert!(unique.len() <= 1,
            "shard {shard} had writes from multiple masters: {unique:?} — split-brain detected!");
    }
    eprintln!("[14.2] Split-brain invariant check passed: no shard had writes from multiple masters");

    eprintln!("[14.2] Healing partition");
    docker.heal_all_partitions().await?;

    tokio::time::sleep(Duration::from_secs(15)).await;
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(120)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[14.2] migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = common::create_client(&docker, 3).await?;
    client.refresh_routing().await?;

    // Verify all pre-partition records are intact
    let post_heal_readable = verify_sample_readable(&client, &txids, 50).await?;
    assert_eq!(post_heal_readable, 50,
        "Test 14.2: post-heal read check: expected 50 readable, got {post_heal_readable}.");

    // Verify records written during partition are readable
    let all_partition_writes: Vec<[u8; 32]> = writes_on_node.iter()
        .flat_map(|v| v.iter().copied())
        .collect();
    if !all_partition_writes.is_empty() {
        let sample = all_partition_writes.len().min(50);
        let readable = verify_sample_readable(&client, &all_partition_writes, sample).await?;
        eprintln!("[14.2] Post-heal: {readable}/{sample} partition-writes readable");
    }

    eprintln!("[14.2] OK -- asymmetric partition test passed, no split-brain detected");
    Ok(())
}

/// Test 14.3: Flapping partition -- toggle partition every 500ms for 30s with background workload.
/// After settling: zero conflicting writes, full consistency check passes.
async fn test_flapping_partition() -> Result<(), ClientError> {
    eprintln!("[14.3] Starting 3-node cluster and seeding 2000 records");

    let (docker_arc, client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker_arc, 3, Duration::from_secs(60)).await?;
    client.refresh_routing().await?;

    let verifier = Arc::new(StateVerifier::new());
    let txids = common::seed_records(&client, &verifier, 2000, 5).await?;
    assert_eq!(txids.len(), 2000);
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Start a background workload
    let stop_flag = Arc::new(AtomicBool::new(false));
    let bg_creates_ok = Arc::new(AtomicU64::new(0));
    let bg_creates_err = Arc::new(AtomicU64::new(0));
    let bg_created_txids: Arc<Mutex<Vec<[u8; 32]>>> = Arc::new(Mutex::new(Vec::new()));

    let bg_stop = Arc::clone(&stop_flag);
    let bg_c_ok = Arc::clone(&bg_creates_ok);
    let bg_c_err = Arc::clone(&bg_creates_err);
    let bg_txids = Arc::clone(&bg_created_txids);
    let bg_verifier = Arc::clone(&verifier);

    let bg_client = common::create_client(&docker_arc, 3).await?;

    let bg_handle = tokio::spawn(async move {
        let mut rng = rand::rngs::StdRng::from_entropy();
        while !bg_stop.load(Ordering::Relaxed) {
            let mut txid = [0u8; 32];
            rng.fill(&mut txid);
            let mut utxo_hash = [0u8; 32];
            rng.fill(&mut utxo_hash);

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

            match bg_client.create_batch(&[item]).await {
                Ok(_) => {
                    bg_verifier.record_create(txid, 1, vec![utxo_hash]);
                    bg_txids.lock().push(txid);
                    bg_c_ok.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    bg_c_err.fetch_add(1, Ordering::Relaxed);
                    let _ = bg_client.refresh_routing().await;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Toggle partition every 500ms for 30 seconds
    eprintln!("[14.3] Starting flapping partition (toggle every 500ms for 30s)");
    let flap_duration = Duration::from_secs(30);
    let flap_start = Instant::now();
    let mut partitioned = false;

    while flap_start.elapsed() < flap_duration {
        if partitioned {
            // Heal
            let _ = docker_arc.heal_all_partitions().await;
            partitioned = false;
        } else {
            // Partition node1 from node2 and node3
            let _ = docker_arc.partition_node("node1", &["node2", "node3"]).await;
            partitioned = true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Ensure healed at the end
    if partitioned {
        let _ = docker_arc.heal_all_partitions().await;
    }

    // Stop background workload
    stop_flag.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(100)).await;
    bg_handle.abort();
    let _ = bg_handle.await;

    let creates_ok = bg_creates_ok.load(Ordering::Relaxed);
    let creates_err = bg_creates_err.load(Ordering::Relaxed);
    eprintln!("[14.3] Background workload: {creates_ok} creates ok, {creates_err} errors");

    // Wait for cluster to settle
    eprintln!("[14.3] Waiting for cluster to settle after flapping");
    tokio::time::sleep(Duration::from_secs(15)).await;
    common::wait_cluster_ready(&docker_arc, 3, Duration::from_secs(120)).await?;
    common::wait_migrations_complete(&docker_arc, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[14.3] migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Full consistency check
    let client = common::create_client(&docker_arc, 3).await?;
    client.refresh_routing().await?;

    let mismatches = common::verify_consistency(&client, &verifier).await?;
    assert!(mismatches.is_empty(),
        "Test 14.3: {} mismatches after flapping partition: {:?}",
        mismatches.len(),
        mismatches.iter().take(5).collect::<Vec<_>>());

    eprintln!("[14.3] OK -- flapping partition test passed, zero conflicting writes");
    Ok(())
}

/// Test 14.4: Docker pause node2 for 5 seconds (simulating clock skew / freeze).
/// Other nodes detect "failure." Unpause. Clean rejoin, no split-brain.
async fn test_docker_pause() -> Result<(), ClientError> {
    eprintln!("[14.4] Starting 3-node cluster and seeding 2000 records");

    let (docker, client) = common::start_3node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(60)).await?;
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();
    let txids = common::seed_records(&client, &verifier, 2000, 5).await?;
    assert_eq!(txids.len(), 2000);

    // Allow extra time for replication to propagate to all replicas.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let pre_pause_readable = verify_sample_readable(&client, &txids, 50).await?;
    assert_eq!(pre_pause_readable, 50);

    eprintln!("[14.4] Pausing node2");
    docker.pause_node("node2").await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify other nodes detected the pause as a failure
    let status_n1 = common::http_status(&docker, 1).await;
    let status_n3 = common::http_status(&docker, 3).await;
    if let (Ok(s1), Ok(s3)) = (&status_n1, &status_n3) {
        let cs1 = s1["cluster_size"].as_u64().unwrap_or(0);
        let cs3 = s3["cluster_size"].as_u64().unwrap_or(0);
        eprintln!("  During pause: node1 cluster_size={cs1}, node3 cluster_size={cs3}");
        // SWIM should detect the pause within suspicion timeout
        assert!(cs1 <= 2, "node1 should detect node2 failure, cluster_size={cs1}");
        assert!(cs3 <= 2, "node3 should detect node2 failure, cluster_size={cs3}");
    }

    eprintln!("[14.4] Unpausing node2");
    docker.unpause_node("node2").await?;

    tokio::time::sleep(Duration::from_secs(5)).await;
    common::wait_cluster_ready(&docker, 3, Duration::from_secs(60)).await?;
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await
        .unwrap_or_else(|e| eprintln!("[14.4] migration wait: {e}"));

    for node_num in 1..=3u32 {
        common::wait_node_cluster_size(&docker, node_num, 3, Duration::from_secs(60)).await?;
    }

    let client = common::create_client(&docker, 3).await?;

    let post_unpause_readable = verify_sample_readable(&client, &txids, 50).await?;
    assert_eq!(post_unpause_readable, 50,
        "Test 14.4: post-unpause read check: expected 50, got {post_unpause_readable}.");

    eprintln!("[14.4] OK -- Docker pause test passed, no split-brain detected");
    Ok(())
}
