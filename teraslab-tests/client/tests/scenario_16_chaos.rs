//! Scenario 16 -- Chaos test.
//!
//! Exercises a 5-node TeraSlab cluster under continuous chaos events
//! (node kills, restarts, network partitions, latency injection, packet
//! loss, pauses) while a mixed workload of creates, reads, spends,
//! setMined, and deletes runs concurrently. Periodic healing checkpoints
//! verify full consistency between the in-memory state verifier and the
//! cluster, including cross-node replication sampling.
//!
//! Orphaned blob detection is not implemented because it requires
//! blobstore filesystem access from inside Docker containers.

mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use teraslab_test_client::{Client, ClientError};
use teraslab_test_client::helpers::DockerHelpers;
use teraslab_test_client::verifier::{StateVerifier, parse_metadata_fields};
use teraslab_test_client::types::*;

use teraslab::protocol::codec::encode_get_batch;
use teraslab::protocol::opcodes::{FLAG_LOCAL_READ, OP_GET_BATCH, STATUS_OK};

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 16;

// ---------------------------------------------------------------------------
// Chaos event types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ChaosEvent {
    KillNode(String),
    RestartNode(String),
    PartitionNode(String),
    HealNode(String),
    InjectLatency(String, u32),       // node, latency_ms
    ClearLatency(String),
    InjectPacketLoss(String, f32),    // node, loss_pct
    ClearPacketLoss(String),
    PauseNode(String, u64),           // node, auto-unpause after N seconds
}

const ALL_NODES: [&str; 5] = ["node1", "node2", "node3", "node4", "node5"];

#[allow(dead_code)]
fn txid_hex(txid: &[u8; 32]) -> String {
    txid.iter().take(8).map(|b| format!("{b:02x}")).collect::<String>()
}

// ---------------------------------------------------------------------------
// Chaos state tracker
// ---------------------------------------------------------------------------

struct ChaosState {
    dead_nodes: HashSet<String>,
    partitioned_nodes: HashSet<String>,
    paused_nodes: HashSet<String>,
    latency_nodes: HashSet<String>,
    loss_nodes: HashSet<String>,
}

impl ChaosState {
    fn new() -> Self {
        Self {
            dead_nodes: HashSet::new(),
            partitioned_nodes: HashSet::new(),
            paused_nodes: HashSet::new(),
            latency_nodes: HashSet::new(),
            loss_nodes: HashSet::new(),
        }
    }

    /// Count of nodes that are alive AND connected (not dead, not partitioned, not paused).
    fn alive_and_connected_count(&self) -> usize {
        let mut count = 0;
        for node in &ALL_NODES {
            let name = node.to_string();
            if !self.dead_nodes.contains(&name)
                && !self.partitioned_nodes.contains(&name)
                && !self.paused_nodes.contains(&name)
            {
                count += 1;
            }
        }
        count
    }

    fn alive_count(&self) -> usize {
        ALL_NODES.iter()
            .filter(|n| !self.dead_nodes.contains(&n.to_string()))
            .count()
    }

    /// Enforce the at-least-2-alive-and-connected constraint before choosing events.
    fn pick_event(&self, rng: &mut impl Rng) -> Option<ChaosEvent> {
        let mut candidates: Vec<ChaosEvent> = Vec::new();

        for node in &ALL_NODES {
            let name = node.to_string();

            if self.dead_nodes.contains(&name) {
                // Only option for a dead node: restart it
                candidates.push(ChaosEvent::RestartNode(name.clone()));
                continue;
            }

            if self.paused_nodes.contains(&name) {
                // Paused nodes can only have their network effects cleared
                if self.latency_nodes.contains(&name) {
                    candidates.push(ChaosEvent::ClearLatency(name.clone()));
                }
                if self.loss_nodes.contains(&name) {
                    candidates.push(ChaosEvent::ClearPacketLoss(name.clone()));
                }
                continue;
            }

            if self.partitioned_nodes.contains(&name) {
                // Partitioned nodes can be healed
                candidates.push(ChaosEvent::HealNode(name.clone()));
                // Also allow clearing network effects
                if self.latency_nodes.contains(&name) {
                    candidates.push(ChaosEvent::ClearLatency(name.clone()));
                }
                if self.loss_nodes.contains(&name) {
                    candidates.push(ChaosEvent::ClearPacketLoss(name.clone()));
                }
                continue;
            }

            // Node is alive and connected -- check constraint before destructive events
            let would_remain = self.alive_and_connected_count() - 1;

            if would_remain >= 2 {
                // Can do destructive things
                if self.alive_count() - 1 >= 2 {
                    candidates.push(ChaosEvent::KillNode(name.clone()));
                }
                candidates.push(ChaosEvent::PartitionNode(name.clone()));
                // Pause with auto-unpause after 1-5 seconds
                let pause_secs = rng.gen_range(1..=5u64);
                candidates.push(ChaosEvent::PauseNode(name.clone(), pause_secs));
            }

            // Network effects are always safe (don't remove nodes from cluster)
            if !self.latency_nodes.contains(&name) {
                let latency = rng.gen_range(100..=500u32);
                candidates.push(ChaosEvent::InjectLatency(name.clone(), latency));
            } else {
                candidates.push(ChaosEvent::ClearLatency(name.clone()));
            }

            if !self.loss_nodes.contains(&name) {
                let loss = rng.gen_range(1..=10) as f32;
                candidates.push(ChaosEvent::InjectPacketLoss(name.clone(), loss));
            } else {
                candidates.push(ChaosEvent::ClearPacketLoss(name.clone()));
            }
        }

        if candidates.is_empty() {
            return None;
        }

        let idx = rng.gen_range(0..candidates.len());
        Some(candidates[idx].clone())
    }
}

// ---------------------------------------------------------------------------
// Chaos event execution
// ---------------------------------------------------------------------------

async fn apply_event(
    docker: &DockerHelpers,
    state: &mut ChaosState,
    event: &ChaosEvent,
) -> Result<(), ClientError> {
    match event {
        ChaosEvent::KillNode(name) => {
            docker.kill_node(name).await?;
            state.dead_nodes.insert(name.clone());
            state.partitioned_nodes.remove(name);
            state.paused_nodes.remove(name);
            state.latency_nodes.remove(name);
            state.loss_nodes.remove(name);
        }
        ChaosEvent::RestartNode(name) => {
            docker.start_node(name).await?;
            state.dead_nodes.remove(name);
        }
        ChaosEvent::PartitionNode(name) => {
            let targets: Vec<String> = ALL_NODES.iter()
                .map(|n| n.to_string())
                .filter(|n| n != name && !state.dead_nodes.contains(n))
                .collect();
            let target_refs: Vec<&str> = targets.iter().map(|s| s.as_str()).collect();
            if !target_refs.is_empty() {
                docker.partition_node(name, &target_refs).await?;
            }
            state.partitioned_nodes.insert(name.clone());
        }
        ChaosEvent::HealNode(name) => {
            docker.heal_partition(name).await?;
            state.partitioned_nodes.remove(name);
        }
        ChaosEvent::InjectLatency(name, latency_ms) => {
            // Inject latency only (no loss). Use slow_network with 0% loss.
            docker.slow_network(name, *latency_ms, 0.0).await?;
            state.latency_nodes.insert(name.clone());
        }
        ChaosEvent::ClearLatency(name) => {
            docker.clear_network(name).await?;
            state.latency_nodes.remove(name);
            // Also clears loss if both were set via the same tc qdisc
            state.loss_nodes.remove(name);
        }
        ChaosEvent::InjectPacketLoss(name, loss_pct) => {
            // Inject loss only (no extra latency). Use slow_network with 0ms latency.
            docker.slow_network(name, 0, *loss_pct).await?;
            state.loss_nodes.insert(name.clone());
        }
        ChaosEvent::ClearPacketLoss(name) => {
            docker.clear_network(name).await?;
            state.loss_nodes.remove(name);
            // Also clears latency if both were set via the same tc qdisc
            state.latency_nodes.remove(name);
        }
        ChaosEvent::PauseNode(name, auto_unpause_secs) => {
            docker.pause_node(name).await?;
            state.paused_nodes.insert(name.clone());

            // Schedule auto-unpause
            let docker_container = format!("ts{:02}-{name}", SID);
            let unpause_secs = *auto_unpause_secs;
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(unpause_secs)).await;
                let _ = tokio::process::Command::new("docker")
                    .args(["unpause", &docker_container])
                    .output()
                    .await;
            });
            // Note: we immediately mark as paused. The spawned task will unpause,
            // but we'll update the state at the next check.
        }
    }
    Ok(())
}

async fn heal_everything(
    docker: &DockerHelpers,
    state: &mut ChaosState,
) -> Result<(), ClientError> {
    let paused: Vec<String> = state.paused_nodes.iter().cloned().collect();
    for name in &paused {
        let _ = docker.unpause_node(name).await;
        state.paused_nodes.remove(name);
    }

    docker.heal_all_partitions().await?;
    state.partitioned_nodes.clear();

    docker.clear_all_networks().await?;
    state.latency_nodes.clear();
    state.loss_nodes.clear();

    let dead: Vec<String> = state.dead_nodes.iter().cloned().collect();
    for name in &dead {
        docker.start_node(name).await?;
        state.dead_nodes.remove(name);
    }

    Ok(())
}

/// Refresh paused_nodes state by checking if auto-unpaused nodes have come back.
fn refresh_paused_state(state: &mut ChaosState) {
    // After healing (which unpauses all nodes), clear the paused set.
    // During normal operation, auto-unpause spawned tasks handle clearing,
    // but during checkpoints we heal everything which includes unpausing.
    state.paused_nodes.clear();
}

// ---------------------------------------------------------------------------
// Workload: 1000 mixed ops/sec (creates + spends + reads + setMined + deletes)
// ---------------------------------------------------------------------------

struct ChaosWorkloadState {
    /// All confirmed created (txid, utxo_hashes) pairs (can be read/spent/mined/deleted).
    confirmed_txids: Arc<Mutex<Vec<([u8; 32], Vec<[u8; 32]>)>>>,
    /// Txids that have been successfully deleted
    deleted_txids: Arc<Mutex<HashSet<[u8; 32]>>>,
    /// Txids pending timeout verification
    timeout_txids: Arc<Mutex<Vec<([u8; 32], String)>>>, // (txid, op_type)
}

async fn run_workload_tick(
    client: &Client,
    verifier: &StateVerifier,
    ws: &ChaosWorkloadState,
    rng: &mut impl Rng,
    metrics_creates_ok: &AtomicU64,
    metrics_creates_err: &AtomicU64,
    metrics_reads_ok: &AtomicU64,
    metrics_reads_err: &AtomicU64,
    metrics_spends_ok: &AtomicU64,
    metrics_spends_err: &AtomicU64,
    metrics_set_mined_ok: &AtomicU64,
    metrics_set_mined_err: &AtomicU64,
    metrics_deletes_ok: &AtomicU64,
    metrics_deletes_err: &AtomicU64,
    metrics_total_ops: &AtomicU64,
    metrics_total_errors: &AtomicU64,
) {
    // Distribution: 30% creates, 25% reads, 20% spends, 15% setMined, 10% deletes
    let roll = rng.gen_range(0..100u32);

    match roll {
        0..=29 => {
            // CREATE
            let mut txid = [0u8; 32];
            rng.fill(&mut txid);
            let mut utxo_hash = [0u8; 32];
            rng.fill(&mut utxo_hash);

            // 1% chance of large tx with 5 MiB cold data
            let is_large = rng.gen_range(0..100u32) == 0;
            let cold_data = if is_large { vec![0xABu8; 5 * 1024 * 1024] } else { vec![] };

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
                cold_data,
                mined_block_id: None,
                mined_block_height: None,
                mined_subtree_idx: None,
                parent_txids: vec![],
            };

            match tokio::time::timeout(Duration::from_secs(5), client.create_batch(&[item])).await {
                Ok(Ok(_)) => {
                    verifier.record_create(txid, 1, vec![utxo_hash]);
                    ws.confirmed_txids.lock().push((txid, vec![utxo_hash]));
                    metrics_creates_ok.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(_)) => {
                    // Failed write: confirm NOT applied by reading back
                    if let Ok(results) = client.get_batch(FIELD_ALL_METADATA, std::slice::from_ref(&txid)).await {
                        if !results.is_empty() && results.item(0).status == 0 {
                            // Unexpectedly applied despite error! Record it anyway.
                            verifier.record_create(txid, 1, vec![utxo_hash]);
                            ws.confirmed_txids.lock().push((txid, vec![utxo_hash]));
                            metrics_creates_ok.fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics_creates_err.fetch_add(1, Ordering::Relaxed);
                            metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        metrics_creates_err.fetch_add(1, Ordering::Relaxed);
                        metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    // Timeout -- query later to determine actual outcome
                    ws.timeout_txids.lock().push((txid, "create".to_string()));
                    metrics_creates_err.fetch_add(1, Ordering::Relaxed);
                    metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        30..=54 => {
            // READ
            let entries = ws.confirmed_txids.lock().clone();
            if !entries.is_empty() {
                let idx = rng.gen_range(0..entries.len());
                let txid = entries[idx].0;
                match tokio::time::timeout(Duration::from_secs(5),
                    client.get_batch(FIELD_ALL_METADATA, std::slice::from_ref(&txid))).await
                {
                    Ok(Ok(_)) => { metrics_reads_ok.fetch_add(1, Ordering::Relaxed); }
                    _ => {
                        metrics_reads_err.fetch_add(1, Ordering::Relaxed);
                        metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        55..=74 => {
            // SPEND (using correct utxo_hash from confirmed records)
            let entries = ws.confirmed_txids.lock().clone();
            if !entries.is_empty() {
                let idx = rng.gen_range(0..entries.len());
                let (txid, ref utxo_hashes) = entries[idx];
                let utxo_hash = utxo_hashes[0];
                let mut spending_data = [0u8; 36];
                rng.fill(&mut spending_data[..32]);
                rng.fill(&mut spending_data[32..]);

                let spend = SpendItem {
                    txid,
                    vout: 0,
                    utxo_hash,
                    spending_data,
                };
                let spend_params = SpendBatchParams {
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 200,
                    block_height_retention: 288,
                };
                match tokio::time::timeout(Duration::from_secs(5),
                    client.spend_batch(&spend_params, &[spend])).await
                {
                    Ok(Ok(resp)) => {
                        if !resp.successes.is_empty() {
                            verifier.record_spend(txid, 0);
                            metrics_spends_ok.fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics_spends_err.fetch_add(1, Ordering::Relaxed);
                            metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(Err(ClientError::Partial(ref pe))) => {
                        // Items NOT in pe.errors succeeded implicitly.
                        let item_failed = pe.errors.iter().any(|e| e.item_index == 0);
                        if !item_failed {
                            verifier.record_spend(txid, 0);
                            metrics_spends_ok.fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics_spends_err.fetch_add(1, Ordering::Relaxed);
                            metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ => {
                        metrics_spends_err.fetch_add(1, Ordering::Relaxed);
                        metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        75..=89 => {
            // SET_MINED
            let entries = ws.confirmed_txids.lock().clone();
            if !entries.is_empty() {
                let idx = rng.gen_range(0..entries.len());
                let txid = entries[idx].0;
                let params = SetMinedBatchParams {
                    block_id: 1,
                    block_height: 100,
                    subtree_idx: 0,
                    on_longest_chain: true,
                    unset_mined: false,
                    current_block_height: 200,
                    block_height_retention: 288,
                };
                match tokio::time::timeout(Duration::from_secs(5),
                    client.set_mined_batch(&params, std::slice::from_ref(&txid))).await
                {
                    Ok(Ok(_)) => {
                        verifier.record_set_mined(txid);
                        metrics_set_mined_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(_)) => {
                        metrics_set_mined_err.fetch_add(1, Ordering::Relaxed);
                        metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        // Timeout -- resolve later by reading back
                        ws.timeout_txids.lock().push((txid, "set_mined".to_string()));
                        metrics_set_mined_err.fetch_add(1, Ordering::Relaxed);
                        metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        _ => {
            // DELETE
            let entries = ws.confirmed_txids.lock().clone();
            let deleted = ws.deleted_txids.lock().clone();
            // Find a non-deleted txid
            let candidate: Option<[u8; 32]> = entries.iter()
                .map(|(txid, _)| *txid)
                .find(|t| !deleted.contains(t));

            if let Some(txid) = candidate {
                match tokio::time::timeout(Duration::from_secs(5),
                    client.delete_batch(std::slice::from_ref(&txid))).await
                {
                    Ok(Ok(_)) => {
                        verifier.record_delete(txid);
                        ws.deleted_txids.lock().insert(txid);
                        metrics_deletes_ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(_)) => {
                        metrics_deletes_err.fetch_add(1, Ordering::Relaxed);
                        metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        // Timeout -- resolve later by reading back
                        ws.timeout_txids.lock().push((txid, "delete".to_string()));
                        metrics_deletes_err.fetch_add(1, Ordering::Relaxed);
                        metrics_total_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    metrics_total_ops.fetch_add(1, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Timeout resolution
// ---------------------------------------------------------------------------

/// Resolve timed-out operations by reading back from the cluster and updating
/// the verifier to match actual state.
async fn resolve_timeouts(
    check_client: &Client,
    verifier: &StateVerifier,
    ws: &ChaosWorkloadState,
) {
    let timeout_list: Vec<([u8; 32], String)> = ws.timeout_txids.lock().drain(..).collect();
    for (txid, op_type) in &timeout_list {
        if let Ok(results) = check_client.get_batch(FIELD_ALL_METADATA, std::slice::from_ref(txid)).await {
            if !results.is_empty() && results.item(0).status == 0 {
                // The record exists on the cluster
                match op_type.as_str() {
                    "create" => {
                        if verifier.get_record(txid).is_none() {
                            let mut dummy_hash = [0u8; 32];
                            dummy_hash[0] = 0xEE;
                            verifier.record_create(*txid, 1, vec![dummy_hash]);
                            ws.confirmed_txids.lock().push((*txid, vec![dummy_hash]));
                        }
                    }
                    "set_mined" => {
                        // Check if block_entry_count > 0 (offset 147 in FIELD_ALL_METADATA)
                        let data = &results.item(0).data;
                        if let Some((_spent, is_mined, _conflicting, _locked)) = parse_metadata_fields(data) {
                            if is_mined {
                                verifier.record_set_mined(*txid);
                            }
                        }
                    }
                    "delete" => {
                        // Record exists despite delete timeout -- do not mark as deleted.
                        // The delete was not applied.
                    }
                    _ => {}
                }
            } else {
                // Record NOT found on cluster
                match op_type.as_str() {
                    "delete" => {
                        // The delete was applied -- record it
                        verifier.record_delete(*txid);
                        ws.deleted_txids.lock().insert(*txid);
                    }
                    "create" => {
                        // Create was not applied -- nothing to do
                    }
                    "set_mined" => {
                        // Record not found (possibly deleted by another op) -- nothing to do
                    }
                    _ => {}
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Replication verification
// ---------------------------------------------------------------------------

/// Sample replication check: for a random subset of non-deleted txids, verify
/// that each record exists on at least 2 nodes (RF=2) by reading directly
/// from each node via FLAG_LOCAL_READ.
async fn verify_replication_sample(
    client: &Client,
    verifier: &StateVerifier,
    docker: &DockerHelpers,
    deleted_txids: &HashSet<[u8; 32]>,
    sample_size: usize,
) {
    let non_deleted = verifier.non_deleted_txids();
    if non_deleted.is_empty() {
        return;
    }

    // Pick up to sample_size random txids
    let mut rng = rand::rngs::StdRng::from_entropy();
    let sample: Vec<[u8; 32]> = {
        let count = sample_size.min(non_deleted.len());
        let mut indices: Vec<usize> = (0..non_deleted.len()).collect();
        // Fisher-Yates partial shuffle for the first `count` elements
        for i in 0..count {
            let j = rng.gen_range(i..non_deleted.len());
            indices.swap(i, j);
        }
        indices[..count].iter()
            .map(|&i| non_deleted[i])
            .filter(|t| !deleted_txids.contains(t))
            .collect()
    };

    if sample.is_empty() {
        return;
    }

    let node_addrs = docker.host_client_addrs(5);
    let mut low_replica_count = 0u32;

    for txid in &sample {
        let mut holders = 0u32;
        let payload = encode_get_batch(FIELD_ALL_METADATA, std::slice::from_ref(txid));

        for addr in &node_addrs {
            match client.send_to_addr(addr, OP_GET_BATCH, FLAG_LOCAL_READ, payload.clone()).await {
                Ok((status, ref resp_payload)) if status == STATUS_OK && resp_payload.len() >= 5 => {
                    // Parse per-item result: [count:4][status:1][...]
                    if resp_payload.len() >= 5 {
                        let count = u32::from_le_bytes(resp_payload[0..4].try_into().unwrap_or([0; 4]));
                        if count >= 1 && resp_payload[4] == 0 {
                            holders += 1;
                        }
                    }
                }
                _ => {
                    // Node unreachable or error -- skip
                }
            }
        }

        if holders < 2 {
            low_replica_count += 1;
        }
    }

    assert!(
        low_replica_count == 0,
        "Replication check failed: {low_replica_count}/{} sampled records found on fewer than 2 nodes (RF=2)",
        sample.len(),
    );

    eprintln!("[16]   Replication sample: {}/{} records verified on >= 2 nodes",
        sample.len(), sample.len());
}

// ---------------------------------------------------------------------------
// Main test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn scenario_16_chaos() {
    // Default 5 minutes + buffer for healing/verification checkpoints.
    // Override with TERASLAB_CHAOS_DURATION_SECS for longer soak tests.
    let result = tokio::time::timeout(Duration::from_secs(600), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("scenario failed: {e}"),
        Err(_) => panic!("scenario timed out after 2700s"),
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    common::teardown_all(SID).await;

    // Default duration: 5 minutes (300 seconds). Override via env var for
    // longer soak tests (e.g., TERASLAB_CHAOS_DURATION_SECS=1800 for 30min).
    let chaos_duration_secs: u64 = std::env::var("TERASLAB_CHAOS_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    let chaos_duration = Duration::from_secs(chaos_duration_secs);

    eprintln!("[16] Starting 5-node cluster (chaos duration = {chaos_duration_secs}s)");

    let (mut docker, _client) = common::start_5node_cluster(SID).await?;
    common::wait_migrations_complete(&docker, 5, Duration::from_secs(90)).await?;

    let client = Arc::new(common::create_client(&docker, 5).await?);
    let verifier = Arc::new(StateVerifier::new());

    eprintln!("[16] Seeding 5000 initial records");
    let seed_txids = common::seed_records(&client, &verifier, 5000, 3).await?;
    assert_eq!(seed_txids.len(), 5000);

    // Allow extra time for replication to propagate to all 5 replicas.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Initial consistency check
    let mismatches = common::verify_consistency(&client, &verifier).await?;
    assert!(mismatches.is_empty(),
        "Baseline consistency check failed with {} mismatches", mismatches.len());
    eprintln!("[16] Baseline consistency verified: 0 mismatches");

    // Build initial confirmed_txids with utxo_hashes from the verifier
    let seed_entries: Vec<([u8; 32], Vec<[u8; 32]>)> = seed_txids.iter().map(|txid| {
        let rec = verifier.get_record(txid).expect("seed record must exist in verifier");
        (*txid, rec.utxo_hashes.clone())
    }).collect();

    // Workload state
    let ws = Arc::new(ChaosWorkloadState {
        confirmed_txids: Arc::new(Mutex::new(seed_entries)),
        deleted_txids: Arc::new(Mutex::new(HashSet::new())),
        timeout_txids: Arc::new(Mutex::new(Vec::new())),
    });

    // Shared metrics
    let m_creates_ok = Arc::new(AtomicU64::new(0));
    let m_creates_err = Arc::new(AtomicU64::new(0));
    let m_reads_ok = Arc::new(AtomicU64::new(0));
    let m_reads_err = Arc::new(AtomicU64::new(0));
    let m_spends_ok = Arc::new(AtomicU64::new(0));
    let m_spends_err = Arc::new(AtomicU64::new(0));
    let m_set_mined_ok = Arc::new(AtomicU64::new(0));
    let m_set_mined_err = Arc::new(AtomicU64::new(0));
    let m_deletes_ok = Arc::new(AtomicU64::new(0));
    let m_deletes_err = Arc::new(AtomicU64::new(0));
    let m_total_ops = Arc::new(AtomicU64::new(0));
    let m_total_errors = Arc::new(AtomicU64::new(0));

    // Workload control flags
    let pause_flag = Arc::new(AtomicBool::new(false));
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Spawn workload in a separate tokio task so Docker commands don't block it
    let workload_handle = {
        let client = Arc::clone(&client);
        let verifier = Arc::clone(&verifier);
        let ws = Arc::clone(&ws);
        let pause_flag = Arc::clone(&pause_flag);
        let stop_flag = Arc::clone(&stop_flag);
        let m_creates_ok = Arc::clone(&m_creates_ok);
        let m_creates_err = Arc::clone(&m_creates_err);
        let m_reads_ok = Arc::clone(&m_reads_ok);
        let m_reads_err = Arc::clone(&m_reads_err);
        let m_spends_ok = Arc::clone(&m_spends_ok);
        let m_spends_err = Arc::clone(&m_spends_err);
        let m_set_mined_ok = Arc::clone(&m_set_mined_ok);
        let m_set_mined_err = Arc::clone(&m_set_mined_err);
        let m_deletes_ok = Arc::clone(&m_deletes_ok);
        let m_deletes_err = Arc::clone(&m_deletes_err);
        let m_total_ops = Arc::clone(&m_total_ops);
        let m_total_errors = Arc::clone(&m_total_errors);

        tokio::spawn(async move {
            let mut rng = rand::rngs::StdRng::from_entropy();
            loop {
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                if pause_flag.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                run_workload_tick(
                    &client, &verifier, &ws, &mut rng,
                    &m_creates_ok, &m_creates_err, &m_reads_ok, &m_reads_err,
                    &m_spends_ok, &m_spends_err, &m_set_mined_ok, &m_set_mined_err,
                    &m_deletes_ok, &m_deletes_err, &m_total_ops, &m_total_errors,
                ).await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };

    let mut state = ChaosState::new();
    let mut rng = rand::thread_rng();
    let mut total_chaos_events = 0u32;
    let mut checkpoint_count = 0u32;

    let chaos_start = Instant::now();
    let mut last_event_time = Instant::now();
    let mut checkpoint_timer = Instant::now();

    eprintln!("[16] Beginning chaos loop (workload on separate task, chaos on main thread)");

    while chaos_start.elapsed() < chaos_duration {
        // -- Checkpoint every 60 seconds --
        if checkpoint_timer.elapsed() >= Duration::from_secs(60) {
            checkpoint_count += 1;
            eprintln!("[16] Checkpoint {checkpoint_count} at {:.0}s: healing everything",
                chaos_start.elapsed().as_secs_f64());

            // Pause workload and wait for it to settle
            pause_flag.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(100)).await;

            heal_everything(&docker, &mut state).await?;
            refresh_paused_state(&mut state);

            // Ensure all nodes are running after healing
            let _ = docker.compose_up().await;
            tokio::time::sleep(Duration::from_secs(15)).await;
            common::wait_cluster_ready(&docker, 5, Duration::from_secs(180)).await?;
            common::wait_migrations_complete(&docker, 5, Duration::from_secs(180)).await
                .unwrap_or_else(|e| eprintln!("[16] checkpoint migration wait: {e}"));

            // Resolve timeout txids before consistency check
            {
                let check_client = common::create_client(&docker, 5).await?;
                resolve_timeouts(&check_client, &verifier, &ws).await;
            }

            // Full consistency check on ALL records
            let check_client = common::create_client(&docker, 5).await?;
            check_client.refresh_routing().await?;

            let mismatches = common::verify_consistency(&check_client, &verifier).await?;

            let total_ops_now = m_total_ops.load(Ordering::Relaxed);
            let total_errors_now = m_total_errors.load(Ordering::Relaxed);
            let creates_ok_now = m_creates_ok.load(Ordering::Relaxed);
            let record_count = verifier.record_count();

            assert!(mismatches.is_empty(),
                "Checkpoint {checkpoint_count}: {} mismatches after {:.0}s. \
                 First 5: {:?}",
                mismatches.len(),
                chaos_start.elapsed().as_secs_f64(),
                mismatches.iter().take(5).collect::<Vec<_>>());

            // Replication sample check (50 random non-deleted txids across all 5 nodes)
            let deleted_snapshot = ws.deleted_txids.lock().clone();
            verify_replication_sample(&check_client, &verifier, &docker, &deleted_snapshot, 50).await;

            eprintln!("[16] Checkpoint {checkpoint_count} passed: 0 mismatches, \
                 {total_chaos_events} events, {total_ops_now} ops, {total_errors_now} errors, \
                 {creates_ok_now} creates, {record_count} records tracked. Resuming chaos.");

            // Resume workload
            pause_flag.store(false, Ordering::Relaxed);
            checkpoint_timer = Instant::now();
        }

        // -- Chaos event every 5-15 seconds --
        let event_interval = Duration::from_secs(rng.gen_range(5..=15));
        if last_event_time.elapsed() >= event_interval {
            // Clear paused nodes that may have been auto-unpaused
            let paused_copy: Vec<String> = state.paused_nodes.iter().cloned().collect();
            for name in paused_copy {
                let node_num: u32 = name.trim_start_matches("node").parse().unwrap_or(0);
                if node_num > 0 {
                    let port = docker.http_port(node_num);
                    let url = format!("http://127.0.0.1:{port}/status");
                    if reqwest::get(&url).await.is_ok() {
                        state.paused_nodes.remove(&name);
                    }
                }
            }

            if let Some(event) = state.pick_event(&mut rng) {
                eprintln!("[16] Event #{total_chaos_events} at {:.0}s: {event:?}",
                    chaos_start.elapsed().as_secs_f64());

                match apply_event(&docker, &mut state, &event).await {
                    Ok(()) => {
                        total_chaos_events += 1;

                        // Refresh client routing after topology-changing events
                        // so the workload sends requests to the correct nodes.
                        let is_topology_change = matches!(
                            event,
                            ChaosEvent::KillNode(_)
                            | ChaosEvent::RestartNode(_)
                            | ChaosEvent::PartitionNode(_)
                            | ChaosEvent::HealNode(_)
                        );
                        if is_topology_change {
                            let _ = client.refresh_routing().await;
                        }
                    }
                    Err(e) => {
                        eprintln!("[16] Event failed (non-fatal): {event:?} -> {e}");
                    }
                }
            }
            last_event_time = Instant::now();
        }

        // Yield to allow other async work (like auto-unpause tasks and the workload task)
        tokio::task::yield_now().await;
    }

    // -- Stop workload task --
    stop_flag.store(true, Ordering::Relaxed);
    let _ = workload_handle.await;

    // -- Final cleanup and verification --
    eprintln!("[16] Chaos loop complete after {:.0}s. Total events: {total_chaos_events}. \
         Healing everything for final verification.",
        chaos_start.elapsed().as_secs_f64());

    heal_everything(&docker, &mut state).await?;

    // After chaos healing, nodes need extended time to rediscover each other
    tokio::time::sleep(Duration::from_secs(5)).await;
    let _ = docker.compose_up().await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    common::wait_cluster_ready(&docker, 5, Duration::from_secs(180)).await?;
    common::wait_migrations_complete(&docker, 5, Duration::from_secs(180)).await
        .unwrap_or_else(|e| eprintln!("[16] final migration wait: {e}"));
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Resolve any remaining timeout txids
    {
        let check_client = common::create_client(&docker, 5).await?;
        resolve_timeouts(&check_client, &verifier, &ws).await;
    }

    // Full consistency check on ALL records
    let final_client = common::create_client(&docker, 5).await?;
    final_client.refresh_routing().await?;

    let final_mismatches = common::verify_consistency(&final_client, &verifier).await?;

    // Final replication sample check
    let deleted_snapshot = ws.deleted_txids.lock().clone();
    verify_replication_sample(&final_client, &verifier, &docker, &deleted_snapshot, 50).await;

    let total_ops_final = m_total_ops.load(Ordering::Relaxed);
    let total_errors_final = m_total_errors.load(Ordering::Relaxed);
    let creates_ok_final = m_creates_ok.load(Ordering::Relaxed);
    let creates_err_final = m_creates_err.load(Ordering::Relaxed);
    let reads_ok_final = m_reads_ok.load(Ordering::Relaxed);
    let reads_err_final = m_reads_err.load(Ordering::Relaxed);
    let spends_ok_final = m_spends_ok.load(Ordering::Relaxed);
    let spends_err_final = m_spends_err.load(Ordering::Relaxed);
    let set_mined_ok_final = m_set_mined_ok.load(Ordering::Relaxed);
    let set_mined_err_final = m_set_mined_err.load(Ordering::Relaxed);
    let deletes_ok_final = m_deletes_ok.load(Ordering::Relaxed);
    let deletes_err_final = m_deletes_err.load(Ordering::Relaxed);
    let record_count = verifier.record_count();

    assert!(final_mismatches.is_empty(),
        "Final verification: {} mismatches found. First 10: {:?}",
        final_mismatches.len(),
        final_mismatches.iter().take(10).collect::<Vec<_>>());

    eprintln!("[16] FINAL RESULTS:\n\
         - Chaos duration: {chaos_duration_secs}s\n\
         - Total chaos events: {total_chaos_events}\n\
         - Checkpoints passed: {checkpoint_count}\n\
         - Total ops: {total_ops_final}\n\
         - Total errors: {total_errors_final}\n\
         - Creates: {creates_ok_final} ok, {creates_err_final} err\n\
         - Reads: {reads_ok_final} ok, {reads_err_final} err\n\
         - Spends: {spends_ok_final} ok, {spends_err_final} err\n\
         - SetMined: {set_mined_ok_final} ok, {set_mined_err_final} err\n\
         - Deletes: {deletes_ok_final} ok, {deletes_err_final} err\n\
         - Records tracked: {record_count}\n\
         - Final consistency mismatches: 0\n\
         - PASS");

    common::teardown_all(SID).await;

    Ok(())
}
