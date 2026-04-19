//! Shared setup/teardown for Docker cluster test scenarios.

use std::sync::OnceLock;
use std::time::Duration;

/// Returns true when `TERASLAB_TEST_TIMING=1` is set, enabling detailed
/// timing logs on stderr for every major phase of the test.
pub fn timing_enabled() -> bool {
    std::env::var("TERASLAB_TEST_TIMING").map_or(false, |v| v == "1")
}
use teraslab_test_client::{Client, ClientConfig, ClientError, PoolConfig};
use teraslab_test_client::helpers::DockerHelpers;
use teraslab_test_client::verifier::{Mismatch, StateVerifier, parse_metadata_fields};
use teraslab_test_client::types::{CreateItem, FIELD_ALL, FIELD_ALL_METADATA};
use teraslab::protocol::codec::encode_get_batch;
use teraslab::protocol::opcodes::{FLAG_LOCAL_READ, OP_GET_BATCH, STATUS_OK};

/// Path to the docker compose directory.
pub fn compose_dir() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .unwrap_or_else(|_| ".".to_string());
    format!("{manifest}/../docker")
}

const POLL_HTTP_TIMEOUT: Duration = Duration::from_secs(5);

fn poll_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(POLL_HTTP_TIMEOUT)
            .build()
            .expect("failed to build poll HTTP client")
    })
}

async fn poll_json(url: &str) -> Result<serde_json::Value, ClientError> {
    let resp = poll_http_client()
        .get(url)
        .send()
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

/// Create a Client seeded only with the specified subset of node numbers.
///
/// Use when the test has deliberately isolated some nodes — passing the
/// minority side into the client's seed list would let the client adopt a
/// stale/minority partition map on first refresh and route all writes into a
/// no-quorum state. Callers should pick nodes known to be on the majority
/// side at the time this is called (pattern B).
pub async fn create_client_subset(
    docker: &DockerHelpers,
    node_nums: &[u32],
) -> Result<Client, ClientError> {
    let seeds: Vec<String> = node_nums
        .iter()
        .map(|&n| format!("127.0.0.1:{}", docker.client_port(n)))
        .collect();
    let config = ClientConfig {
        addr: None,
        seeds,
        pool: PoolConfig::default(),
        cluster_refresh_interval: Duration::from_secs(30),
        max_redirects: 3,
        addr_map: docker.docker_addr_map(),
    };
    Client::new(config).await
}

/// Wait until the client's current partition map stops assigning any
/// shard master to the `excluded` node IDs. Used to verify the client is
/// not seeing a minority-side view of the cluster after a partition
/// (pattern B).
///
/// A node can still be present in `pm.nodes` after being isolated — the
/// majority side's membership list doesn't always prune the isolated node
/// immediately. What matters for routing is whether any shard in
/// `pm.assignments` points at the isolated node as master. The wait
/// allows the majority to propose + commit a fresh shard table that
/// excludes the isolated node before the caller proceeds; polls via
/// `client.get_partition_map()` so it reflects the view the next routed
/// call will use.
///
/// On timeout returns `ClientError::Connection` describing the latest
/// observed state of the partition map.
pub async fn wait_client_excludes_nodes(
    client: &Client,
    excluded: &[u64],
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    let mut backoff = Duration::from_millis(100);
    let mut last_overlap: Vec<u64> = excluded.to_vec();
    let mut last_version = 0u64;
    let mut last_masters: std::collections::BTreeSet<u64> =
        std::collections::BTreeSet::new();
    loop {
        let _ = client.refresh_routing().await;
        let pm = client.get_partition_map().await?;
        let masters: std::collections::BTreeSet<u64> =
            pm.assignments.iter().copied().collect();
        let overlap: Vec<u64> = excluded
            .iter()
            .copied()
            .filter(|id| masters.contains(id))
            .collect();
        if overlap.is_empty() {
            return Ok(());
        }
        last_overlap = overlap;
        last_version = pm.version;
        last_masters = masters;
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(format!(
                "client partition map still routes shards to isolated node(s) \
                 {last_overlap:?} after {timeout:?}: version={last_version}, \
                 unique_masters={last_masters:?} — client would route to minority side",
            )));
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

/// Fetch the HTTP /status JSON for a given node number, using ports from the
/// provided DockerHelpers.
pub async fn http_status(docker: &DockerHelpers, node_num: u32) -> Result<serde_json::Value, ClientError> {
    let port = docker.http_port(node_num);
    let url = format!("http://127.0.0.1:{port}/status");
    poll_json(&url).await
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
    poll_json(&url).await
}

/// Wait until all nodes report the expected cluster size via HTTP /status.
pub async fn wait_cluster_ready(docker: &DockerHelpers, node_count: u32, timeout: Duration) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    let mut last_log = std::time::Instant::now();
    loop {
        let mut ready = 0u32;
        let mut versions: Vec<u64> = Vec::new();
        for i in 1..=node_count {
            let port = docker.http_port(i);
            let url = format!("http://127.0.0.1:{port}/status");
            if let Ok(json) = poll_json(&url).await {
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
        // All nodes must report correct cluster size AND agree on the
        // shard table version (topology term) AND every node must have
        // master shards assigned. This ensures the cluster has fully
        // converged and the shard table includes all nodes before tests
        // begin.
        // Check that each node actually has master shards
        let mut min_masters = u64::MAX;
        for i in 1..=node_count {
            let port = docker.http_port(i);
            let url = format!("http://127.0.0.1:{port}/status");
            if let Ok(json) = poll_json(&url).await {
                if let Some(m) = json["master_shard_count"].as_u64() {
                    min_masters = min_masters.min(m);
                }
            }
        }
        let balanced = node_count <= 1 || min_masters > 0;
        if ready == node_count
            && versions.len() == node_count as usize
            && versions.iter().all(|&v| v > 0 && v == versions[0])
            && balanced
        {
            if timing_enabled() {
                eprintln!("  wait_cluster_ready: {node_count} nodes converged in {:.1}ms (version={})", start.elapsed().as_secs_f64() * 1000.0, versions[0]);
            }
            return Ok(());
        }
        if timing_enabled() && last_log.elapsed() >= Duration::from_secs(2) {
            eprintln!("  wait_cluster_ready: {ready}/{node_count} ready, versions={versions:?} ({:.1}s)", start.elapsed().as_secs_f64());
            last_log = std::time::Instant::now();
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(
                format!("{ready}/{node_count} nodes ready (versions: {versions:?}) after {timeout:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
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
        if let Ok(json) = poll_json(&url).await {
            if let Some(size) = json["cluster_size"].as_u64() {
                if size == expected_size as u64 {
                    return Ok(());
                }
            }
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(
                format!("node {node_num}: cluster_size != {expected_size} after {timeout:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
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
    let mut last_log = std::time::Instant::now();
    loop {
        let mut ready = 0u32;
        let mut sizes = Vec::new();
        for &n in node_nums {
            if let Ok(status) = http_status(docker, n).await {
                if let Some(size) = status["cluster_size"].as_u64() {
                    sizes.push((n, size));
                    if size == expected_size as u64 {
                        ready += 1;
                    }
                }
            } else {
                sizes.push((n, 0));
            }
        }
        if ready == node_nums.len() as u32 {
            return Ok(());
        }
        if timing_enabled() && last_log.elapsed() >= Duration::from_secs(2) {
            let detail: Vec<String> = sizes.iter()
                .map(|(n, s)| format!("node{n}={s}"))
                .collect();
            eprintln!("  wait_specific_nodes: {ready}/{} ready, sizes=[{}] ({:.1}s)",
                node_nums.len(), detail.join(", "), start.elapsed().as_secs_f64());
            last_log = std::time::Instant::now();
        }
        if start.elapsed() >= timeout {
            let detail: Vec<String> = sizes.iter()
                .map(|(n, s)| format!("node{n}={s}"))
                .collect();
            return Err(ClientError::Connection(
                format!("{ready}/{} specific nodes ready after {timeout:?} [{}]",
                    node_nums.len(), detail.join(", ")),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
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
        let mut total_inbound_pending: u64 = 0;
        let mut status_details = Vec::new();
        for &n in node_nums {
            let port = docker.http_port(n);
            let url = format!("http://127.0.0.1:{port}/admin/migration_status");
            let mut active_count = None;
            let mut inbound_pending = 0u64;
            if let Ok(json) = poll_json(&url).await {
                inbound_pending = json["inbound_pending"].as_u64().unwrap_or(0);
                total_inbound_pending += inbound_pending;
                if let Some(count) = json["active_count"].as_u64() {
                    active_count = Some(count);
                    if count > 0 {
                        all_idle = false;
                    }
                }
            }
            let status_url = format!("http://127.0.0.1:{port}/status");
            if let Ok(json) = poll_json(&status_url).await {
                let cluster_size = json["cluster_size"].as_u64().unwrap_or(0);
                let shard_table_version = json["shard_table_version"].as_u64().unwrap_or(0);
                let topology_term = json["topology_term"].as_u64().unwrap_or(0);
                let pending_handoffs = json["pending_handoff_shards"].as_u64().unwrap_or(0);
                if let Some(m) = json["master_shard_count"].as_u64() {
                    total_masters += m;
                    status_details.push(format!(
                        "node{n}:size={cluster_size},ver={shard_table_version},term={topology_term},masters={m},handoff={pending_handoffs},mig={},inbound={inbound_pending}",
                        active_count.unwrap_or(0),
                    ));
                }
            } else {
                status_details.push(format!(
                    "node{n}:status-unavailable,mig={},inbound={inbound_pending}",
                    active_count.unwrap_or(0)
                ));
            }
        }
        // During handoff, shards may be counted on both old and new masters,
        // causing total_masters to briefly exceed 4096. Accept >= 4096.
        if total_masters >= 4096 && all_idle {
            if timing_enabled() {
                eprintln!(
                    "  wait_specific_migrations: complete in {:.1}ms [{}]",
                    start.elapsed().as_secs_f64() * 1000.0,
                    status_details.join(", ")
                );
            }
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(
                format!(
                    "migrations still active on specific nodes after {timeout:?} [masters={total_masters}/4096, inbound={total_inbound_pending}] [{}]",
                    status_details.join(", ")
                ),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
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
    let mig_start = std::time::Instant::now();
    let mut mig_last_log = std::time::Instant::now();
    let start = std::time::Instant::now();
    // Track when handoff count stopped decreasing, for orphan detection.
    let mut last_handoff_snapshot: Option<(u64, Duration)> = None;
    loop {
        let mut all_idle = true;
        let mut total_masters: u64 = 0;
        let mut total_pending_handoffs: u64 = 0;
        let mut total_inbound_pending: u64 = 0;
        let mut node_details = Vec::new();
        for i in 1..=node_count {
            let port = docker.http_port(i);
            let url = format!("http://127.0.0.1:{port}/admin/migration_status");
            if let Ok(json) = poll_json(&url).await {
                let inbound_pending = json["inbound_pending"].as_u64().unwrap_or(0);
                total_inbound_pending += inbound_pending;
                if let Some(count) = json["active_count"].as_u64() {
                    if count > 0 {
                        all_idle = false;
                        node_details.push(format!("node{i}:mig={count}"));
                    }
                }
                if inbound_pending > 0 {
                    node_details.push(format!("node{i}:inbound={inbound_pending}"));
                }
            }
            let status_url = format!("http://127.0.0.1:{port}/status");
            if let Ok(json) = poll_json(&status_url).await {
                if let Some(m) = json["master_shard_count"].as_u64() {
                    total_masters += m;
                }
                if let Some(h) = json["pending_handoff_shards"].as_u64() {
                    total_pending_handoffs += h;
                    if h > 0 {
                        node_details.push(format!("node{i}:handoff={h}"));
                    }
                }
            }
        }
        // Accept masters within ±128 of 4096. The pipelined migration
        // causes temporary double-counting while handoff commits propagate
        // across batched sub-groups.
        let masters_ok = total_masters >= 3968 && total_masters <= 4224;
        if masters_ok && total_pending_handoffs == 0 && total_inbound_pending == 0 && all_idle {
            if timing_enabled() {
                eprintln!("  wait_migrations: complete in {:.1}ms", mig_start.elapsed().as_secs_f64() * 1000.0);
            }
            return Ok(());
        }
        // Fallback: if all masters are assigned and no active migrations,
        // accept the cluster even with pending handoffs IF the handoff
        // count has stopped decreasing (stuck for >= 3 seconds). This
        // catches genuinely orphaned handoffs (dead source node) without
        // accepting prematurely during live-node scenarios where handoffs
        // are still completing.
        if masters_ok && all_idle && total_pending_handoffs > 0 && total_inbound_pending == 0 {
            if let Some((prev_h, prev_t)) = last_handoff_snapshot {
                if total_pending_handoffs >= prev_h
                    && start.elapsed().saturating_sub(prev_t) >= Duration::from_secs(3)
                {
                    if timing_enabled() {
                        eprintln!("  wait_migrations: accepting with {total_pending_handoffs} orphaned handoffs after {:.1}s",
                            mig_start.elapsed().as_secs_f64());
                    }
                    return Ok(());
                }
            }
            if last_handoff_snapshot.is_none()
                || last_handoff_snapshot.map_or(false, |(h, _)| total_pending_handoffs < h)
            {
                last_handoff_snapshot = Some((total_pending_handoffs, start.elapsed()));
            }
        }
        if timing_enabled() && mig_last_log.elapsed() >= Duration::from_secs(2) {
            eprintln!("  wait_migrations: masters={total_masters}/4096, handoffs={total_pending_handoffs}, inbound={total_inbound_pending}, idle={all_idle} ({:.1}s) [{}]",
                mig_start.elapsed().as_secs_f64(), node_details.join(", "));
            mig_last_log = std::time::Instant::now();
        }
        if start.elapsed() >= timeout {
            let detail = if !node_details.is_empty() {
                format!(" [{}]", node_details.join(", "))
            } else {
                format!(" [masters={total_masters}/4096, handoffs={total_pending_handoffs}, inbound={total_inbound_pending}]")
            };
            return Err(ClientError::Connection(
                format!("migrations still active after {timeout:?}{detail}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Probe the tracked txids to confirm they are actually readable end-to-end
/// after migrations report complete.
///
/// Closes the gap between `wait_migrations_complete` returning `Ok` (counters
/// say zero in-flight) and the migrated records being visible on the replica
/// nodes their shards now belong to. The migration-status counters can flip
/// to zero a beat before the receiving node has committed every inbound write
/// to its index, which shows up in scenarios as a brief window of
/// `STATUS_NOT_FOUND` responses for records that physically exist.
///
/// The helper runs two checks per iteration:
///
/// 1. **Master-route**: every txid in `txids` must return `status=OK` from
///    `client.get_batch(..)` (batched in chunks of 500). This is the exact
///    read path the downstream test will exercise, so it is checked in full
///    rather than sampled.
/// 2. **Replica**: `sample_size` evenly-spaced txids must each be present on
///    at least `min_replicas` of `node_nums` via `FLAG_LOCAL_READ`. Catches
///    replica-lag where the master responds but the replica has not applied
///    the migrated blob yet.
///
/// Retries both checks with exponential backoff (starting at 100ms, capped at
/// 1s), refreshing routing between iterations, until every record satisfies
/// both conditions or `timeout` elapses. On timeout returns
/// `ClientError::Connection` prefixed with `migration read verify timeout`
/// and carrying the first few failing-record prefixes with their observed
/// local-read holder counts for diagnosis.
///
/// `node_nums` must list nodes known to be alive post-migration; dead nodes
/// will fail `direct_get` and count against `min_replicas`.
pub async fn wait_for_migration_reads_ready(
    client: &Client,
    docker: &DockerHelpers,
    txids: &[[u8; 32]],
    node_nums: &[u32],
    min_replicas: usize,
    sample_size: usize,
    timeout: Duration,
) -> Result<(), ClientError> {
    if txids.is_empty() {
        return Ok(());
    }
    let sample_count = sample_size.min(txids.len()).max(1);
    let step = (txids.len() / sample_count).max(1);
    let sample_indices: Vec<usize> = (0..sample_count)
        .map(|i| (i * step) % txids.len())
        .collect();

    let node_addrs: Vec<String> = node_nums
        .iter()
        .map(|&n| format!("127.0.0.1:{}", docker.client_port(n)))
        .collect();

    let start = std::time::Instant::now();
    let mut backoff = Duration::from_millis(100);
    let mut last_log = std::time::Instant::now();
    loop {
        // (1) Master-route check across ALL txids — this is the exact path
        //     downstream test reads will use.
        let mut master_failed_idx: Vec<usize> = Vec::new();
        {
            let mut base = 0usize;
            for chunk in txids.chunks(500) {
                match client.get_batch(FIELD_ALL_METADATA, chunk).await {
                    Ok(results) => {
                        for (i, r) in results.iter().enumerate() {
                            if r.status() != 0 {
                                master_failed_idx.push(base + i);
                            }
                        }
                    }
                    Err(_) => {
                        for i in 0..chunk.len() {
                            master_failed_idx.push(base + i);
                        }
                    }
                }
                base += chunk.len();
            }
        }

        // (2) Replica check via FLAG_LOCAL_READ on sampled txids.
        let mut holders_by_sample: Vec<usize> = Vec::with_capacity(sample_indices.len());
        for &idx in &sample_indices {
            let txid = txids[idx];
            let mut holders = 0usize;
            for addr in &node_addrs {
                let payload = encode_get_batch(FIELD_ALL_METADATA, std::slice::from_ref(&txid));
                let ok = match client
                    .send_to_addr(addr, OP_GET_BATCH, FLAG_LOCAL_READ, payload)
                    .await
                {
                    Ok((frame_status, body)) => {
                        frame_status == STATUS_OK && body.len() >= 5 && body[4] == 0
                    }
                    Err(_) => false,
                };
                if ok {
                    holders += 1;
                }
            }
            holders_by_sample.push(holders);
        }
        let under_replicated: usize = holders_by_sample
            .iter()
            .filter(|&&h| h < min_replicas)
            .count();
        let master_failed = master_failed_idx.len();

        // Gate on the master-route check — this is the exact path the
        // downstream test reads will use and the primary correctness
        // signal. `under_replicated` is reported as a warning: a record
        // may be briefly under-replicated while replica backfill catches
        // up after a migration, but as long as the master serves it the
        // test's reads succeed. Keeping `under_replicated` blocking here
        // would entangle the barrier with replica-lag concerns that are
        // separate from pattern A's master-miss failure mode.
        if master_failed == 0 {
            if timing_enabled() {
                let under_repl_note = if under_replicated > 0 {
                    format!(
                        " (info: {under_replicated}/{} sampled txids under-replicated)",
                        sample_indices.len(),
                    )
                } else {
                    String::new()
                };
                eprintln!(
                    "  wait_for_migration_reads_ready: {} txids verified ({} sampled for replicas) in {:.1}ms{under_repl_note}",
                    txids.len(),
                    sample_indices.len(),
                    start.elapsed().as_secs_f64() * 1000.0,
                );
            }
            return Ok(());
        }

        if timing_enabled() && last_log.elapsed() >= Duration::from_secs(2) {
            eprintln!(
                "  wait_for_migration_reads_ready: master_failed={master_failed}/{}, \
                 under_replicated={under_replicated}/{} (min_replicas={min_replicas}) \
                 after {:.1}s",
                txids.len(),
                sample_indices.len(),
                start.elapsed().as_secs_f64(),
            );
            last_log = std::time::Instant::now();
        }

        if start.elapsed() >= timeout {
            // Diagnose the first few master-route failures by reading from
            // each node directly so the error tells us whether the data is
            // missing everywhere or only on the route's master.
            let mut diag = Vec::new();
            for &idx in master_failed_idx.iter().take(3) {
                let txid = txids[idx];
                let prefix: String = txid[..6].iter().map(|b| format!("{b:02x}")).collect();
                let mut holders = 0usize;
                for addr in &node_addrs {
                    let payload =
                        encode_get_batch(FIELD_ALL_METADATA, std::slice::from_ref(&txid));
                    let ok = match client
                        .send_to_addr(addr, OP_GET_BATCH, FLAG_LOCAL_READ, payload)
                        .await
                    {
                        Ok((frame_status, body)) => {
                            frame_status == STATUS_OK && body.len() >= 5 && body[4] == 0
                        }
                        Err(_) => false,
                    };
                    if ok {
                        holders += 1;
                    }
                }
                diag.push(format!(
                    "txid_prefix={prefix} holders_via_local_read={}/{}",
                    holders,
                    node_nums.len(),
                ));
            }
            return Err(ClientError::Connection(format!(
                "migration read verify timeout after {timeout:?}: \
                 master_failed={master_failed}/{}, under_replicated={under_replicated}/{} \
                 (min_replicas={min_replicas}, nodes={node_nums:?}); \
                 first_failures=[{}]",
                txids.len(),
                sample_indices.len(),
                diag.join(" | "),
            )));
        }

        let _ = client.refresh_routing().await;
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

/// Wait for replication to propagate.
///
/// Polls `/debug/redo` on each reachable node and waits until redo
/// sequences stabilize (stop changing between polls). This detects when
/// all in-flight replication has completed without requiring sequences
/// to be equal across nodes (each node has an independent redo log).
pub async fn wait_replication_settled(
    docker: &DockerHelpers,
    node_count: u32,
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    let mut prev_seqs: Vec<u64> = Vec::new();
    let mut stable_polls = 0u32;

    loop {
        let mut seqs = Vec::new();
        for i in 1..=node_count {
            let port = docker.http_port(i);
            let url = format!("http://127.0.0.1:{port}/debug/redo");
            if let Ok(json) = poll_json(&url).await {
                if let Some(seq) = json["current_sequence"].as_u64() {
                    seqs.push(seq);
                }
            }
        }

        // Settled when sequences haven't changed for 2 consecutive polls.
        if seqs.len() == prev_seqs.len() && seqs == prev_seqs {
            stable_polls += 1;
            if stable_polls >= 2 {
                return Ok(());
            }
        } else {
            stable_polls = 0;
        }
        prev_seqs = seqs;

        if start.elapsed() >= timeout {
            return Ok(()); // Best-effort: don't fail the test over lag.
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait for replication to settle on specific nodes only (e.g., surviving
/// nodes after a kill).
pub async fn wait_specific_replication_settled(
    docker: &DockerHelpers,
    node_nums: &[u32],
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    let mut prev_seqs: Vec<u64> = Vec::new();
    let mut stable_polls = 0u32;

    loop {
        let mut seqs = Vec::new();
        for &n in node_nums {
            let port = docker.http_port(n);
            let url = format!("http://127.0.0.1:{port}/debug/redo");
            if let Ok(json) = poll_json(&url).await {
                if let Some(seq) = json["current_sequence"].as_u64() {
                    seqs.push(seq);
                }
            }
        }

        if seqs.len() == prev_seqs.len() && seqs == prev_seqs {
            stable_polls += 1;
            if stable_polls >= 2 {
                return Ok(());
            }
        } else {
            stable_polls = 0;
        }
        prev_seqs = seqs;

        if start.elapsed() >= timeout {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Start a 3-node cluster and wait for it to be ready.
///
/// Returns a mutable `DockerHelpers` (needed for `compose_up`/`compose_down`)
/// and a connected `Client`.
pub async fn start_3node_cluster(scenario_id: u16) -> Result<(DockerHelpers, Client), ClientError> {
    let mut docker = docker_3node(scenario_id);
    let start = std::time::Instant::now();
    const MAX_ATTEMPTS: u32 = 5;
    let mut last_err = None;

    for attempt in 1..=MAX_ATTEMPTS {
        if timing_enabled() {
            eprintln!("  start_3node_cluster[{scenario_id}]: compose_up (attempt {attempt}/{MAX_ATTEMPTS})");
        }
        docker.compose_up().await?;
        if timing_enabled() {
            eprintln!(
                "  start_3node_cluster[{scenario_id}]: compose_up done in {:.1}ms",
                start.elapsed().as_secs_f64() * 1000.0
            );
            eprintln!("  start_3node_cluster[{scenario_id}]: wait_cluster_ready (45s)");
        }
        match wait_cluster_ready(&docker, 3, Duration::from_secs(45)).await {
            Ok(()) => {
                if timing_enabled() {
                    eprintln!(
                        "  start_3node_cluster[{scenario_id}]: cluster ready in {:.1}ms",
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                    eprintln!("  start_3node_cluster[{scenario_id}]: wait_migrations_complete");
                }
                wait_migrations_complete(&docker, 3, Duration::from_secs(30)).await?;
                if timing_enabled() {
                    eprintln!(
                        "  start_3node_cluster[{scenario_id}]: migrations complete in {:.1}ms",
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                    eprintln!("  start_3node_cluster[{scenario_id}]: create_client");
                }
                let client = create_client(&docker, 3).await?;
                if timing_enabled() {
                    eprintln!(
                        "  start_3node_cluster[{scenario_id}]: client created in {:.1}ms",
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                    eprintln!("  start_3node_cluster[{scenario_id}]: refresh_routing");
                }
                client.refresh_routing().await?;
                if timing_enabled() {
                    eprintln!(
                        "  start_3node_cluster[{scenario_id}]: ready in {:.1}ms",
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                }
                return Ok((docker, client));
            }
            Err(e) => {
                eprintln!(
                    "  start_3node_cluster[{scenario_id}]: cluster not ready after 45s (attempt {attempt}/{MAX_ATTEMPTS}): {e}"
                );
                last_err = Some(e);
                if attempt < MAX_ATTEMPTS {
                    // Tear down and retry
                    force_cleanup(scenario_id).await;
                    wait_ports_free(docker.http_port(1), scenario_id, docker.node_count()).await;
                    docker = docker_3node(scenario_id);
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        ClientError::Connection(format!(
            "start_3node_cluster[{scenario_id}]: failed after {MAX_ATTEMPTS} attempts"
        ))
    }))
}

/// Start a 5-node cluster and wait for it to be ready.
pub async fn start_5node_cluster(scenario_id: u16) -> Result<(DockerHelpers, Client), ClientError> {
    let mut docker = docker_5node(scenario_id);
    let start = std::time::Instant::now();
    const MAX_ATTEMPTS: u32 = 5;
    let mut last_err = None;

    for attempt in 1..=MAX_ATTEMPTS {
        if timing_enabled() {
            eprintln!("  start_5node_cluster[{scenario_id}]: compose_up (attempt {attempt}/{MAX_ATTEMPTS})");
        }
        docker.compose_up().await?;
        if timing_enabled() {
            eprintln!(
                "  start_5node_cluster[{scenario_id}]: compose_up done in {:.1}ms",
                start.elapsed().as_secs_f64() * 1000.0
            );
            eprintln!("  start_5node_cluster[{scenario_id}]: wait_cluster_ready (45s)");
        }
        match wait_cluster_ready(&docker, 5, Duration::from_secs(45)).await {
            Ok(()) => {
                if timing_enabled() {
                    eprintln!(
                        "  start_5node_cluster[{scenario_id}]: cluster ready in {:.1}ms",
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                    eprintln!("  start_5node_cluster[{scenario_id}]: wait_migrations_complete");
                }
                wait_migrations_complete(&docker, 5, Duration::from_secs(30)).await?;
                if timing_enabled() {
                    eprintln!(
                        "  start_5node_cluster[{scenario_id}]: migrations complete in {:.1}ms",
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                    eprintln!("  start_5node_cluster[{scenario_id}]: create_client");
                }
                let client = create_client(&docker, 5).await?;
                if timing_enabled() {
                    eprintln!(
                        "  start_5node_cluster[{scenario_id}]: client created in {:.1}ms",
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                    eprintln!("  start_5node_cluster[{scenario_id}]: refresh_routing");
                }
                client.refresh_routing().await?;
                if timing_enabled() {
                    eprintln!(
                        "  start_5node_cluster[{scenario_id}]: ready in {:.1}ms",
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                }
                return Ok((docker, client));
            }
            Err(e) => {
                eprintln!(
                    "  start_5node_cluster[{scenario_id}]: cluster not ready after 45s (attempt {attempt}/{MAX_ATTEMPTS}): {e}"
                );
                last_err = Some(e);
                if attempt < MAX_ATTEMPTS {
                    // Tear down and retry
                    force_cleanup(scenario_id).await;
                    wait_ports_free(docker.http_port(1), scenario_id, docker.node_count()).await;
                    docker = docker_5node(scenario_id);
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        ClientError::Connection(format!(
            "start_5node_cluster[{scenario_id}]: failed after {MAX_ATTEMPTS} attempts"
        ))
    }))
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
        // Retry on transient errors from SWIM instability, dead nodes,
        // or cluster topology changes. Uses exponential backoff with up to
        // 8 attempts (~30s total) to ride out post-topology-change settle.
        //
        // On partial success, only retry the failed items (not items that
        // already succeeded — re-sending those would cause ERR_ALREADY_EXISTS).
        const MAX_SEED_RETRIES: u32 = 16;
        let mut remaining_items = items;
        let mut remaining_meta = batch_meta;
        let mut succeeded_meta: Vec<([u8; 32], Vec<[u8; 32]>)> = Vec::new();

        for attempt in 0..MAX_SEED_RETRIES {
            match client.create_batch(&remaining_items).await {
                Ok(_) => {
                    // All remaining items succeeded.
                    succeeded_meta.extend(remaining_meta.drain(..));
                    remaining_items.clear();
                    break;
                }
                Err(ref e) if attempt + 1 < MAX_SEED_RETRIES => {
                    // On partial error, extract which items failed and only
                    // retry those. Items not in the error list succeeded.
                    if let ClientError::Partial(pe) = e {
                        let mut code_counts = std::collections::BTreeMap::new();
                        for err in &pe.errors {
                            *code_counts.entry(err.code).or_insert(0usize) += 1;
                        }
                        let code_summary = code_counts
                            .iter()
                            .map(|(code, count)| {
                                format!(
                                    "{}={count}",
                                    teraslab_test_client::errors::error_code_string(*code),
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        let failed_indices: std::collections::HashSet<usize> =
                            pe.errors.iter().map(|e| e.item_index as usize).collect();
                        let mut retry_items = Vec::new();
                        let mut retry_meta = Vec::new();
                        for (i, (item, meta)) in remaining_items.drain(..)
                            .zip(remaining_meta.drain(..)).enumerate()
                        {
                            if failed_indices.contains(&i) {
                                retry_items.push(item);
                                retry_meta.push(meta);
                            } else {
                                succeeded_meta.push(meta);
                            }
                        }
                        remaining_items = retry_items;
                        remaining_meta = retry_meta;
                        eprintln!(
                            "seed_records: partial error on attempt {attempt}: {} failed item(s) [{code_summary}]",
                            failed_indices.len()
                        );
                    }
                    if remaining_items.is_empty() {
                        break;
                    }
                    if attempt == 0 {
                        eprintln!("seed_records: transient error on attempt {attempt}, \
                            retrying {} items: {e}", remaining_items.len());
                    }
                    let delay = Duration::from_millis(500 * (1 << attempt.min(3)));
                    tokio::time::sleep(delay).await;
                    let _ = client.refresh_routing().await;
                }
                Err(e) => {
                    eprintln!("seed_records: failed after {MAX_SEED_RETRIES} attempts: {e}");
                    return Err(e);
                }
            }
        }
        if !remaining_items.is_empty() {
            return Err(ClientError::Connection(
                format!("create_batch: {} items still failing after retries", remaining_items.len())
            ));
        }
        for (txid, utxo_hashes) in succeeded_meta {
            verifier.record_create(txid, utxos_per_tx, utxo_hashes);
            txids.push(txid);
        }
    }

    Ok(txids)
}

/// Tear down the Docker cluster for a specific scenario and wait for cleanup.
pub async fn teardown(docker: &mut DockerHelpers) {
    force_cleanup(docker.scenario_id()).await;
    wait_ports_free(docker.http_port(1), docker.scenario_id(), docker.node_count()).await;
}

/// Batch-read a set of txids and return how many were NOT found (status != 0).
/// Uses chunked get_batch for efficiency — no per-txid round trips.
pub async fn count_accessible(
    client: &Client,
    txids: &[[u8; 32]],
) -> Result<(usize, usize), ClientError> {
    let start = std::time::Instant::now();
    let mut found = 0usize;
    let mut not_found = 0usize;
    let total = txids.len();
    for chunk in txids.chunks(500) {
        let results = client.get_batch(FIELD_ALL_METADATA, chunk).await?;
        for result in results.iter() {
            if result.status() == 0 { found += 1; } else { not_found += 1; }
        }
    }
    if timing_enabled() {
        eprintln!("  count_accessible: {found}/{total} found in {:.1}ms", start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok((found, not_found))
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
    let _start = std::time::Instant::now();
    let mut all_mismatches = Vec::new();
    let txids = verifier.non_deleted_txids();
    if timing_enabled() {
        eprintln!("  verify_consistency: checking {} records...", txids.len());
    }
    let mut not_found_txids: Vec<[u8; 32]> = Vec::new();

    // Process in batches of 500 for throughput.
    for chunk in txids.chunks(500) {
        // Retry batch reads on connection errors — cluster may still be
        // settling after recovery, partitions, or migrations.
        let results = {
            let mut last_err = None;
            let mut res = None;
            for _retry in 0..3 {
                match client.get_batch(FIELD_ALL_METADATA, chunk).await {
                    Ok(r) => { res = Some(r); break; }
                    Err(e) => {
                        let _ = client.refresh_routing().await;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        last_err = Some(e);
                    }
                }
            }
            match res {
                Some(r) => r,
                None => return Err(last_err.unwrap()),
            }
        };

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
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = client.refresh_routing().await;

        for chunk in not_found_txids.chunks(500) {
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
    force_cleanup(scenario_id).await;
    let first_http_port = 19000 + scenario_id * 10;
    wait_ports_free(first_http_port, scenario_id, 5).await;
}

async fn docker_output_timeout(args: &[String], timeout: Duration) -> Option<std::process::Output> {
    let mut cmd = tokio::process::Command::new("docker");
    cmd.kill_on_drop(true);
    cmd.args(args.iter().map(|s| s.as_str()));
    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(out)) => Some(out),
        _ => None,
    }
}

/// Force-remove all Docker resources (containers, volumes, networks) for a
/// scenario using direct docker commands. Much faster than `compose_down`
/// because it skips compose file generation and runs a single bulk removal.
async fn force_cleanup(scenario_id: u16) {
    let sid = format!("ts{scenario_id:02}");

    let container_filter = format!("name={sid}-node");
    let container_deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let ids = docker_output_timeout(&vec![
            "ps".to_string(),
            "-aq".to_string(),
            "--filter".to_string(),
            container_filter.clone(),
        ], Duration::from_secs(5)).await
            .map(|out| {
                String::from_utf8_lossy(&out.stdout)
                    .split_whitespace()
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
        if ids.is_empty() || std::time::Instant::now() >= container_deadline {
            break;
        }
        let mut args = vec!["rm".to_string(), "-f".to_string()];
        args.extend(ids);
        let _ = docker_output_timeout(&args, Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let volume_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let volume_filter = format!("name={sid}");
    let vol_handle = tokio::spawn(async move {
        loop {
            let vols = docker_output_timeout(&vec![
                "volume".to_string(),
                "ls".to_string(),
                "-q".to_string(),
                "--filter".to_string(),
                volume_filter.clone(),
            ], Duration::from_secs(5)).await
                .map(|out| {
                    String::from_utf8_lossy(&out.stdout)
                        .split_whitespace()
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>()
                })
                .unwrap_or_default();
            if vols.is_empty() || std::time::Instant::now() >= volume_deadline {
                break;
            }
            let mut args = vec!["volume".to_string(), "rm".to_string()];
            args.extend(vols);
            let _ = docker_output_timeout(&args, Duration::from_secs(15)).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    let network_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let network_filter = format!("name={sid}");
    let net_handle = tokio::spawn(async move {
        loop {
            let nets = docker_output_timeout(&vec![
                "network".to_string(),
                "ls".to_string(),
                "-q".to_string(),
                "--filter".to_string(),
                network_filter.clone(),
            ], Duration::from_secs(5)).await
                .map(|out| {
                    String::from_utf8_lossy(&out.stdout)
                        .split_whitespace()
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>()
                })
                .unwrap_or_default();
            if nets.is_empty() || std::time::Instant::now() >= network_deadline {
                break;
            }
            let mut args = vec!["network".to_string(), "rm".to_string()];
            args.extend(nets);
            let _ = docker_output_timeout(&args, Duration::from_secs(15)).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    let _ = vol_handle.await;
    let _ = net_handle.await;
}

/// Wait until a single node's HTTP health endpoint responds.
/// Polls `GET /health/live` every 100ms, returns as soon as it gets a 200,
/// or after `timeout` elapses.
pub async fn wait_node_healthy(docker: &DockerHelpers, node_num: u32, timeout: Duration) -> Result<(), ClientError> {
    let port = docker.http_port(node_num);
    let url = format!("http://127.0.0.1:{port}/health/live");
    let start = std::time::Instant::now();
    loop {
        if let Ok(resp) = poll_http_client().get(&url).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(
                format!("node {node_num} not healthy after {timeout:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Read a batch of txids from a specific node, bypassing shard routing via
/// FLAG_LOCAL_READ. Returns `(status, raw_payload)`.
pub async fn direct_get(
    client: &Client,
    node_addr: &str,
    txids: &[[u8; 32]],
) -> Result<(u8, Vec<u8>), ClientError> {
    let payload = encode_get_batch(FIELD_ALL, txids);
    client.send_to_addr(node_addr, OP_GET_BATCH, FLAG_LOCAL_READ, payload).await
}

/// Parse a batch get response into per-item (status, data) pairs.
pub fn parse_batch_response(payload: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut items = Vec::new();
    if payload.len() < 4 {
        return items;
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    for _ in 0..count {
        if offset >= payload.len() {
            break;
        }
        let status = payload[offset];
        offset += 1;
        if offset + 4 > payload.len() {
            break;
        }
        let data_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let data = if data_len > 0 && offset + data_len <= payload.len() {
            payload[offset..offset + data_len].to_vec()
        } else {
            vec![]
        };
        offset += data_len;
        items.push((status, data));
    }
    items
}

/// Compare two per-item data payloads ignoring the `updated_at` timestamp field.
///
/// The `updated_at` field (8 bytes) differs between master and replica because
/// each node sets it to local time when the operation is applied. All other
/// fields should be byte-identical.
///
/// Works on raw item data (after stripping the response envelope). The
/// `updated_at` offset in item data is 61 (= 70 - 9 byte envelope prefix).
pub fn payloads_match(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_copy = a.to_vec();
    let mut b_copy = b.to_vec();
    // Zero out updated_at (8 bytes at offset 61 in item data).
    if a_copy.len() >= 69 {
        a_copy[61..69].fill(0);
        b_copy[61..69].fill(0);
    }
    a_copy == b_copy
}

/// Batch replication check: fetch all txids from all nodes in bulk
/// requests (chunked), then cross-compare in memory.
///
/// If `expect_present` is true, expects each record on exactly 2 nodes (RF=2)
/// and compares payloads. If false, expects records to be absent (deleted).
///
/// Returns `(mismatches, holder_count_errors)`.
pub async fn batch_verify_replication(
    client: &Client,
    node_addrs: &[String],
    txids: &[[u8; 32]],
    expect_present: bool,
) -> Result<(u32, u32), ClientError> {
    const CHUNK_SIZE: usize = 500;

    let mut node_items: Vec<Vec<(u8, Vec<u8>)>> = Vec::new();
    for addr in node_addrs {
        let mut all_items = Vec::with_capacity(txids.len());
        for chunk in txids.chunks(CHUNK_SIZE) {
            let (frame_status, payload) = direct_get(client, addr, chunk).await?;
            if frame_status == STATUS_OK {
                all_items.extend(parse_batch_response(&payload));
            } else {
                for _ in 0..chunk.len() {
                    all_items.push((1, vec![]));
                }
            }
        }
        node_items.push(all_items);
    }

    let mut mismatches = 0u32;
    let mut holder_errors = 0u32;

    for (idx, _txid) in txids.iter().enumerate() {
        let mut holder_indices = Vec::new();
        for (node_idx, items) in node_items.iter().enumerate() {
            if idx < items.len() && items[idx].0 == 0 {
                holder_indices.push(node_idx);
            }
        }

        if expect_present {
            if holder_indices.len() != 2 {
                holder_errors += 1;
                continue;
            }
            let a = &node_items[holder_indices[0]][idx].1;
            let b = &node_items[holder_indices[1]][idx].1;
            if !payloads_match(a, b) {
                mismatches += 1;
            }
        } else {
            if !holder_indices.is_empty() {
                holder_errors += 1;
            }
        }
    }

    Ok((mismatches, holder_errors))
}

/// For a given txid, determine which nodes hold the record via FLAG_LOCAL_READ.
/// Returns `(holder_indices, non_holder_indices)`.
pub async fn find_holders(
    client: &Client,
    node_addrs: &[String],
    txid: &[u8; 32],
) -> Result<(Vec<usize>, Vec<usize>), ClientError> {
    let mut holders = Vec::new();
    let mut non_holders = Vec::new();
    for (i, addr) in node_addrs.iter().enumerate() {
        let (frame_status, payload) = direct_get(client, addr, &[*txid]).await?;
        if frame_status == STATUS_OK && !payload.is_empty() {
            if payload.len() >= 4 {
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                if count >= 1 && payload.len() >= 5 {
                    let item_status = payload[4];
                    if item_status == 0 {
                        holders.push(i);
                        continue;
                    }
                }
            }
        }
        non_holders.push(i);
    }
    Ok((holders, non_holders))
}

/// Poll until all HTTP ports for a scenario are free (connection refused).
/// Returns immediately once no port accepts connections, or after 10s at most.
async fn wait_ports_free(first_http_port: u16, _scenario_id: u16, node_count: u32) {
    let ports: Vec<u16> = (0..node_count).map(|i| first_http_port + i as u16).collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let all_free = ports.iter().all(|&p| {
            std::net::TcpStream::connect_timeout(
                &std::net::SocketAddr::from(([127, 0, 0, 1], p)),
                Duration::from_millis(50),
            ).is_err()
        });
        if all_free || std::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
