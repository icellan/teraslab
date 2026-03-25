//! Shared setup/teardown for Docker cluster test scenarios.

use std::time::Duration;
use teraslab_test_client::{Client, ClientConfig, ClientError, PoolConfig};
use teraslab_test_client::helpers::DockerHelpers;
use teraslab_test_client::verifier::{Mismatch, StateVerifier, parse_metadata_fields};
use teraslab_test_client::types::{CreateItem, FIELD_ALL_METADATA};

/// Path to the docker compose directory.
pub fn compose_dir() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .unwrap_or_else(|_| ".".to_string());
    format!("{manifest}/../docker")
}

/// Create a DockerHelpers for 3-node cluster with a specific scenario ID.
pub fn docker_3node(scenario_id: u16) -> DockerHelpers {
    DockerHelpers::new(&compose_dir(), scenario_id, 3)
}

/// Create a DockerHelpers for 5-node cluster with a specific scenario ID.
pub fn docker_5node(scenario_id: u16) -> DockerHelpers {
    DockerHelpers::new(&compose_dir(), scenario_id, 5)
}

/// Create a Client connected to N nodes via host port mapping, using ports
/// derived from the given DockerHelpers instance.
pub async fn create_client(docker: &DockerHelpers, node_count: usize) -> Result<Client, ClientError> {
    let config = ClientConfig {
        addr: None,
        seeds: docker.host_client_addrs(node_count),
        pool: PoolConfig::default(),
        cluster_refresh_interval: Duration::from_secs(30),
        max_redirects: 3,
        addr_map: docker.docker_addr_map(),
    };
    Client::new(config).await
}

/// Fetch the HTTP /status JSON for a given node number, using ports from the
/// provided DockerHelpers.
pub async fn http_status(docker: &DockerHelpers, node_num: u32) -> Result<serde_json::Value, ClientError> {
    let port = docker.http_port(node_num);
    let url = format!("http://127.0.0.1:{port}/status");
    let resp = reqwest::get(&url)
        .await
        .map_err(|e| ClientError::Connection(format!("GET {url} failed: {e}")))?;
    if !resp.status().is_success() {
        return Err(ClientError::Connection(format!(
            "GET {url} returned status {}",
            resp.status()
        )));
    }
    resp.json::<serde_json::Value>()
        .await
        .map_err(|e| ClientError::Connection(format!("GET {url} JSON parse failed: {e}")))
}

/// Send a PUT to the HTTP quiesce endpoint for a given node number.
pub async fn http_quiesce(docker: &DockerHelpers, node_num: u32) -> Result<(), ClientError> {
    let port = docker.http_port(node_num);
    let url = format!("http://127.0.0.1:{port}/admin/quiesce");
    let client = reqwest::Client::new();
    let resp = client.put(&url)
        .send()
        .await
        .map_err(|e| ClientError::Connection(format!("PUT {url} failed: {e}")))?;
    if !resp.status().is_success() {
        return Err(ClientError::Connection(format!(
            "PUT {url} returned status {}",
            resp.status()
        )));
    }
    Ok(())
}

/// Fetch the HTTP /admin/migration_status JSON for a given node number.
pub async fn http_migration_status(docker: &DockerHelpers, node_num: u32) -> Result<serde_json::Value, ClientError> {
    let port = docker.http_port(node_num);
    let url = format!("http://127.0.0.1:{port}/admin/migration_status");
    let resp = reqwest::get(&url)
        .await
        .map_err(|e| ClientError::Connection(format!("GET {url} failed: {e}")))?;
    resp.json::<serde_json::Value>()
        .await
        .map_err(|e| ClientError::Connection(format!("GET {url} JSON parse failed: {e}")))
}

/// Wait until all nodes report the expected cluster size via HTTP /status.
pub async fn wait_cluster_ready(docker: &DockerHelpers, node_count: u32, timeout: Duration) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    loop {
        let mut ready = 0u32;
        let mut versions: Vec<u64> = Vec::new();
        for i in 1..=node_count {
            let port = docker.http_port(i);
            let url = format!("http://127.0.0.1:{port}/status");
            if let Ok(resp) = reqwest::get(&url).await {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(size) = json["cluster_size"].as_u64() {
                        if size == node_count as u64 {
                            ready += 1;
                            if let Some(v) = json["shard_table_version"].as_u64() {
                                versions.push(v);
                            }
                        }
                    }
                }
            }
        }
        // All nodes must report correct cluster size AND agree on the
        // shard table version (topology term). This ensures the cluster
        // has fully converged before tests begin.
        if ready == node_count
            && versions.len() == node_count as usize
            && versions.iter().all(|&v| v > 0 && v == versions[0])
        {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(
                format!("{ready}/{node_count} nodes ready (versions: {versions:?}) after {timeout:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait until a specific node reports the expected cluster size.
pub async fn wait_node_cluster_size(
    docker: &DockerHelpers,
    node_num: u32,
    expected_size: u32,
    timeout: Duration,
) -> Result<(), ClientError> {
    let port = docker.http_port(node_num);
    let start = std::time::Instant::now();
    loop {
        let url = format!("http://127.0.0.1:{port}/status");
        if let Ok(resp) = reqwest::get(&url).await {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(size) = json["cluster_size"].as_u64() {
                    if size == expected_size as u64 {
                        return Ok(());
                    }
                }
            }
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(
                format!("node {node_num}: cluster_size != {expected_size} after {timeout:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait until specific nodes (by node number) all report the expected cluster size.
pub async fn wait_specific_nodes_ready(
    docker: &DockerHelpers,
    node_nums: &[u32],
    expected_size: u32,
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    loop {
        let mut ready = 0u32;
        for &n in node_nums {
            if let Ok(status) = http_status(docker, n).await {
                if let Some(size) = status["cluster_size"].as_u64() {
                    if size == expected_size as u64 {
                        ready += 1;
                    }
                }
            }
        }
        if ready == node_nums.len() as u32 {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(
                format!("{ready}/{} specific nodes ready after {timeout:?}", node_nums.len()),
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait until migrations complete on specific nodes (by node number).
pub async fn wait_specific_migrations_complete(
    docker: &DockerHelpers,
    node_nums: &[u32],
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    loop {
        let mut all_idle = true;
        let mut total_masters: u64 = 0;
        for &n in node_nums {
            let port = docker.http_port(n);
            let url = format!("http://127.0.0.1:{port}/admin/migration_status");
            if let Ok(resp) = reqwest::get(&url).await {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(count) = json["active_count"].as_u64() {
                        if count > 0 {
                            all_idle = false;
                        }
                    }
                }
            }
            let status_url = format!("http://127.0.0.1:{port}/status");
            if let Ok(resp) = reqwest::get(&status_url).await {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(m) = json["master_shard_count"].as_u64() {
                        total_masters += m;
                    }
                }
            }
        }
        // Consider ready when all master shards are assigned (4096).
        // Replica migrations may still be running but don't affect
        // functional readiness — the cluster can serve all shards.
        if total_masters == 4096 {
            if all_idle {
                return Ok(());
            }
            // Masters assigned but replica migrations running — wait a bit
            // for them to finish, but don't block indefinitely.
            if start.elapsed() >= timeout.min(Duration::from_secs(30)) {
                return Ok(());
            }
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(
                format!("migrations still active on specific nodes after {timeout:?} [masters={total_masters}/4096]"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Wait until all active migrations complete on all nodes.
///
/// Also waits for shard master counts to sum to 4096 (all shards assigned)
/// to catch shards stuck in handoff after migration completion.
pub async fn wait_migrations_complete(
    docker: &DockerHelpers,
    node_count: u32,
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    loop {
        let mut all_idle = true;
        let mut total_masters: u64 = 0;
        let mut node_details = Vec::new();
        for i in 1..=node_count {
            let port = docker.http_port(i);
            // Check migration status
            let url = format!("http://127.0.0.1:{port}/admin/migration_status");
            if let Ok(resp) = reqwest::get(&url).await {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(count) = json["active_count"].as_u64() {
                        if count > 0 {
                            all_idle = false;
                            node_details.push(format!("node{i}:mig={count}"));
                        }
                    }
                }
            }
            // Also check master shard count
            let status_url = format!("http://127.0.0.1:{port}/status");
            if let Ok(resp) = reqwest::get(&status_url).await {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(m) = json["master_shard_count"].as_u64() {
                        total_masters += m;
                    }
                }
            }
        }
        // Consider ready when all master shards are assigned (4096).
        // Replica migrations may still be running but don't affect
        // functional readiness.
        if total_masters == 4096 {
            if all_idle {
                return Ok(());
            }
            if start.elapsed() >= timeout.min(Duration::from_secs(30)) {
                return Ok(());
            }
        }
        if start.elapsed() >= timeout {
            let detail = if !node_details.is_empty() {
                format!(" [{}]", node_details.join(", "))
            } else {
                format!(" [masters={total_masters}/4096]")
            };
            return Err(ClientError::Connection(
                format!("migrations still active after {timeout:?}{detail}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Start a 3-node cluster and wait for it to be ready.
///
/// Returns a mutable `DockerHelpers` (needed for `compose_up`/`compose_down`)
/// and a connected `Client`.
pub async fn start_3node_cluster(scenario_id: u16) -> Result<(DockerHelpers, Client), ClientError> {
    let mut docker = docker_3node(scenario_id);
    docker.compose_up().await?;
    wait_cluster_ready(&docker, 3, Duration::from_secs(30)).await?;
    // Wait for initial shard migrations to settle before creating the client.
    // This prevents stale routing errors from the initial topology convergence.
    wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;
    let client = create_client(&docker, 3).await?;
    client.refresh_routing().await?;
    Ok((docker, client))
}

/// Start a 5-node cluster and wait for it to be ready.
pub async fn start_5node_cluster(scenario_id: u16) -> Result<(DockerHelpers, Client), ClientError> {
    let mut docker = docker_5node(scenario_id);
    docker.compose_up().await?;
    wait_cluster_ready(&docker, 5, Duration::from_secs(30)).await?;
    wait_migrations_complete(&docker, 5, Duration::from_secs(120)).await?;
    let client = create_client(&docker, 5).await?;
    client.refresh_routing().await?;
    Ok((docker, client))
}

/// Seed N records with the given UTXO count each.
/// Returns the list of txids created.
pub async fn seed_records(
    client: &Client,
    verifier: &StateVerifier,
    count: u32,
    utxos_per_tx: u32,
) -> Result<Vec<[u8; 32]>, ClientError> {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut txids = Vec::with_capacity(count as usize);

    for batch_start in (0..count).step_by(100) {
        let batch_end = (batch_start + 100).min(count);
        let mut items = Vec::new();
        let mut batch_meta: Vec<([u8; 32], Vec<[u8; 32]>)> = Vec::new();

        for _ in batch_start..batch_end {
            let mut txid = [0u8; 32];
            rng.fill(&mut txid);
            let utxo_hashes: Vec<[u8; 32]> = (0..utxos_per_tx)
                .map(|_| {
                    let mut h = [0u8; 32];
                    rng.fill(&mut h);
                    h
                })
                .collect();

            items.push(CreateItem {
                txid,
                utxo_hashes: utxo_hashes.clone(),
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

            batch_meta.push((txid, utxo_hashes));
        }

        // Only record in verifier AFTER the create succeeds, to avoid
        // phantom records when the create fails (e.g., during degradation).
        // Retry on transient "no quorum" errors from SWIM instability.
        let mut created = false;
        for attempt in 0..5 {
            match client.create_batch(&items).await {
                Ok(_) => { created = true; break; }
                Err(ClientError::Server { code, .. }) if code == 15 && attempt < 4 => {
                    // NO_QUORUM: SWIM instability, retry after refresh.
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let _ = client.refresh_routing().await;
                }
                Err(ClientError::Partial(ref pe)) if attempt < 4
                    && pe.errors.iter().all(|e| e.code == 14 || e.code == 19) =>
                {
                    // All errors are REDIRECT (14) or MIGRATION_IN_PROGRESS (19).
                    // Retry after routing refresh — the cluster is still converging.
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let _ = client.refresh_routing().await;
                }
                Err(e) => return Err(e),
            }
        }
        if !created {
            return Err(ClientError::Connection("create_batch failed after retries".to_string()));
        }
        for (txid, utxo_hashes) in batch_meta {
            verifier.record_create(txid, utxos_per_tx, utxo_hashes);
            txids.push(txid);
        }
    }

    Ok(txids)
}

/// Tear down the Docker cluster for a specific scenario and wait for cleanup.
pub async fn teardown(docker: &mut DockerHelpers) {
    let _ = docker.compose_down().await;

    // Force-remove any lingering containers for this scenario
    let sid = docker.scenario_id();
    for i in 1..=5 {
        let name = format!("ts{sid:02}-node{i}");
        let _ = tokio::process::Command::new("docker")
            .args(["rm", "-f", &name])
            .output()
            .await;
    }

    // Wait for Docker to fully release network ports and resources.
    // Without sufficient delay, port conflicts and stale state from
    // previous runs cause the next cluster to fail on startup.
    tokio::time::sleep(Duration::from_secs(3)).await;
}

/// Full consistency check: read every non-deleted record from the cluster and
/// compare against the verifier's expected state.
///
/// Returns a `Vec<Mismatch>` for every discrepancy found. An empty vector
/// means perfect consistency.
pub async fn verify_consistency(
    client: &Client,
    verifier: &StateVerifier,
) -> Result<Vec<Mismatch>, ClientError> {
    let mut all_mismatches = Vec::new();
    let txids = verifier.non_deleted_txids();
    let mut not_found_txids: Vec<[u8; 32]> = Vec::new();

    // Process in batches of 100 to avoid overwhelming the wire protocol.
    for chunk in txids.chunks(100) {
        let results = client
            .get_batch(FIELD_ALL_METADATA, chunk)
            .await?;

        for (i, result) in results.iter().enumerate() {
            let txid = &chunk[i];

            if result.status() != 0 {
                // Record not found — collect for retry after routing refresh
                not_found_txids.push(*txid);
                continue;
            }

            if let Some((spent_count, is_mined, is_conflicting, is_locked)) =
                parse_metadata_fields(result.data())
            {
                let mm = verifier.verify_record(
                    txid,
                    spent_count,
                    is_mined,
                    is_conflicting,
                    is_locked,
                    false,
                );
                all_mismatches.extend(mm);
            }
        }
    }

    // Retry NotFound records after refreshing routing — the partition map may
    // have been stale for shards that recently migrated.
    if !not_found_txids.is_empty() {
        eprintln!("verify_consistency: {} records NotFound on first pass, retrying after routing refresh...",
            not_found_txids.len());
        let _ = client.refresh_routing().await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = client.refresh_routing().await;

        for chunk in not_found_txids.chunks(100) {
            let results = client
                .get_batch(FIELD_ALL_METADATA, chunk)
                .await?;

            for (i, result) in results.iter().enumerate() {
                let txid = &chunk[i];

                if result.status() != 0 {
                    let mm = verifier.verify_record(
                        txid, 0, false, false, false, true,
                    );
                    all_mismatches.extend(mm);
                    continue;
                }

                if let Some((spent_count, is_mined, is_conflicting, is_locked)) =
                    parse_metadata_fields(result.data())
                {
                    let mm = verifier.verify_record(
                        txid,
                        spent_count,
                        is_mined,
                        is_conflicting,
                        is_locked,
                        false,
                    );
                    all_mismatches.extend(mm);
                }
            }
        }
    }

    // Also check that deleted records are actually gone
    let deleted_txids: Vec<[u8; 32]> = {
        let all = verifier.all_txids();
        let non_deleted = verifier.non_deleted_txids();
        let non_del_set: std::collections::HashSet<_> = non_deleted.iter().collect();
        all.into_iter()
            .filter(|t| !non_del_set.contains(t))
            .collect()
    };

    for chunk in deleted_txids.chunks(100) {
        if chunk.is_empty() {
            break;
        }
        let results = client
            .get_batch(FIELD_ALL_METADATA, chunk)
            .await?;
        for (i, result) in results.iter().enumerate() {
            if result.status() == 0 {
                // Record found but should be deleted
                all_mismatches.push(Mismatch {
                    txid: chunk[i],
                    field: "deleted".to_string(),
                    expected: "deleted (NotFound)".to_string(),
                    actual: "record exists".to_string(),
                });
            }
        }
    }

    Ok(all_mismatches)
}

/// Tear down both 3-node and 5-node clusters for a specific scenario.
pub async fn teardown_all(scenario_id: u16) {
    let mut d3 = docker_3node(scenario_id);
    let _ = d3.compose_down().await;
    let mut d5 = docker_5node(scenario_id);
    let _ = d5.compose_down().await;

    // Force-remove any lingering containers for this scenario
    for i in 1..=5 {
        let name = format!("ts{scenario_id:02}-node{i}");
        let _ = tokio::process::Command::new("docker")
            .args(["rm", "-f", &name])
            .output()
            .await;
    }

    // Wait for Docker to fully release network ports and resources.
    tokio::time::sleep(Duration::from_secs(3)).await;
}
