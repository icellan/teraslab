//! Shared setup/teardown for Docker cluster test scenarios.

use std::sync::OnceLock;
use std::time::Duration;

/// Returns true when `TERASLAB_TEST_TIMING=1` is set, enabling detailed
/// timing logs on stderr for every major phase of the test.
pub fn timing_enabled() -> bool {
    std::env::var("TERASLAB_TEST_TIMING").is_ok_and(|v| v == "1")
}
use teraslab::protocol::codec::encode_get_batch;
use teraslab::protocol::opcodes::{
    ADMIN_DIAGNOSE_KEY_MAX_TXIDS, FLAG_LOCAL_READ, OP_ADMIN_CLUSTER_HEALTH, OP_ADMIN_DIAGNOSE_KEY,
    OP_GET_BATCH, STATUS_OK,
};
use teraslab_test_client::helpers::DockerHelpers;
use teraslab_test_client::types::{CreateItem, FIELD_ALL, FIELD_ALL_METADATA};
use teraslab_test_client::verifier::{Mismatch, StateVerifier, parse_metadata_fields};
use teraslab_test_client::{Client, ClientConfig, ClientError, PoolConfig};

/// Path to the docker compose directory.
pub fn compose_dir() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    format!("{manifest}/../docker")
}

const POLL_HTTP_TIMEOUT: Duration = Duration::from_secs(5);

fn poll_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        // Attach the test-only admin bearer token on EVERY request: the
        // gated /admin/* and /debug/* routes require it (401 otherwise),
        // and the public routes (/status, /health/*, /metrics) have no
        // auth middleware, so they ignore the extra header. The token
        // matches the `admin_token` written into the generated node
        // configs by `render_node_config`.
        let mut headers = reqwest::header::HeaderMap::new();
        let bearer = format!(
            "Bearer {}",
            teraslab_test_client::helpers::DOCKER_TEST_ADMIN_TOKEN
        );
        let mut auth = reqwest::header::HeaderValue::from_str(&bearer)
            .expect("admin token must be a valid header value");
        auth.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, auth);
        reqwest::Client::builder()
            .timeout(POLL_HTTP_TIMEOUT)
            .default_headers(headers)
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
pub async fn create_client(
    docker: &DockerHelpers,
    node_count: usize,
) -> Result<Client, ClientError> {
    let config = ClientConfig {
        addr: None,
        seeds: docker.host_client_addrs(node_count),
        pool: PoolConfig::default(),
        cluster_refresh_interval: Duration::from_secs(30),
        max_redirects: 3,
        addr_map: docker.docker_addr_map(),
        ..Default::default()
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
        ..Default::default()
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
    loop {
        let _ = client.refresh_routing().await;
        let pm = client.get_partition_map().await?;
        let masters: std::collections::BTreeSet<u64> = pm.assignments.iter().copied().collect();
        let overlap: Vec<u64> = excluded
            .iter()
            .copied()
            .filter(|id| masters.contains(id))
            .collect();
        if overlap.is_empty() {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(format!(
                "client partition map still routes shards to isolated node(s) \
                 {overlap:?} after {timeout:?}: version={}, \
                 unique_masters={masters:?} — client would route to minority side",
                pm.version,
            )));
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

/// Fetch the HTTP /status JSON for a given node number, using ports from the
/// provided DockerHelpers.
pub async fn http_status(
    docker: &DockerHelpers,
    node_num: u32,
) -> Result<serde_json::Value, ClientError> {
    let port = docker.http_port(node_num);
    let url = format!("http://127.0.0.1:{port}/status");
    poll_json(&url).await
}

/// Send a PUT to the HTTP quiesce endpoint for a given node number.
pub async fn http_quiesce(docker: &DockerHelpers, node_num: u32) -> Result<(), ClientError> {
    let port = docker.http_port(node_num);
    let url = format!("http://127.0.0.1:{port}/admin/quiesce");
    // Use the shared poll client so the request carries the admin bearer
    // token — /admin/quiesce is on the gated router and 401s without it.
    let resp = poll_http_client()
        .put(&url)
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
pub async fn http_migration_status(
    docker: &DockerHelpers,
    node_num: u32,
) -> Result<serde_json::Value, ClientError> {
    let port = docker.http_port(node_num);
    let url = format!("http://127.0.0.1:{port}/admin/migration_status");
    poll_json(&url).await
}

/// Wait until all nodes report the expected cluster size via HTTP /status.
pub async fn wait_cluster_ready(
    docker: &DockerHelpers,
    node_count: u32,
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    let mut last_log = std::time::Instant::now();
    loop {
        let mut ready = 0u32;
        let mut versions: Vec<u64> = Vec::new();
        for i in 1..=node_count {
            let port = docker.http_port(i);
            let url = format!("http://127.0.0.1:{port}/status");
            if let Ok(json) = poll_json(&url).await
                && let Some(size) = json["cluster_size"].as_u64()
                && size == node_count as u64
            {
                ready += 1;
                if let Some(v) = json["shard_table_version"].as_u64() {
                    versions.push(v);
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
            if let Ok(json) = poll_json(&url).await
                && let Some(m) = json["master_shard_count"].as_u64()
            {
                min_masters = min_masters.min(m);
            }
        }
        let balanced = node_count <= 1 || min_masters > 0;
        if ready == node_count
            && versions.len() == node_count as usize
            && versions.iter().all(|&v| v > 0 && v == versions[0])
            && balanced
        {
            if timing_enabled() {
                eprintln!(
                    "  wait_cluster_ready: {node_count} nodes converged in {:.1}ms (version={})",
                    start.elapsed().as_secs_f64() * 1000.0,
                    versions[0]
                );
            }
            return Ok(());
        }
        if timing_enabled() && last_log.elapsed() >= Duration::from_secs(2) {
            eprintln!(
                "  wait_cluster_ready: {ready}/{node_count} ready, versions={versions:?} ({:.1}s)",
                start.elapsed().as_secs_f64()
            );
            last_log = std::time::Instant::now();
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(format!(
                "{ready}/{node_count} nodes ready (versions: {versions:?}) after {timeout:?}"
            )));
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
        if let Ok(json) = poll_json(&url).await
            && let Some(size) = json["cluster_size"].as_u64()
            && size == expected_size as u64
        {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(format!(
                "node {node_num}: cluster_size != {expected_size} after {timeout:?}"
            )));
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
            let detail: Vec<String> = sizes.iter().map(|(n, s)| format!("node{n}={s}")).collect();
            eprintln!(
                "  wait_specific_nodes: {ready}/{} ready, sizes=[{}] ({:.1}s)",
                node_nums.len(),
                detail.join(", "),
                start.elapsed().as_secs_f64()
            );
            last_log = std::time::Instant::now();
        }
        if start.elapsed() >= timeout {
            let detail: Vec<String> = sizes.iter().map(|(n, s)| format!("node{n}={s}")).collect();
            return Err(ClientError::Connection(format!(
                "{ready}/{} specific nodes ready after {timeout:?} [{}]",
                node_nums.len(),
                detail.join(", ")
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Phase I — wait until every node in `node_nums` reports `Alive` via
/// `OP_ADMIN_CLUSTER_HEALTH` for at least `stability_window` of
/// continuous time. Without the stability window, a transient
/// flap (committed term advances and immediately gets superseded) can
/// fool a "snap-poll" caller into seeding records against a half-formed
/// cluster.
///
/// Returns `Ok(())` when all named nodes have been continuously
/// `Alive` for `stability_window`. Returns
/// `ClientError::Connection` on `timeout`, with a short snapshot of
/// each node's last-observed state.
///
/// Pass `Duration::from_millis(500)` for `stability_window` to match
/// the planned `STABILITY_WINDOW_MS` constant.
pub async fn wait_specific_nodes_alive(
    client: &Client,
    docker: &DockerHelpers,
    node_nums: &[u32],
    stability_window: Duration,
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    let mut last_log = std::time::Instant::now();
    let mut alive_since: Option<std::time::Instant> = None;

    loop {
        let mut node_states: Vec<(u32, String)> = Vec::with_capacity(node_nums.len());
        let mut all_alive = true;

        for &n in node_nums {
            let addr = format!("127.0.0.1:{}", docker.client_port(n));
            let state = match client
                .send_to_addr(&addr, OP_ADMIN_CLUSTER_HEALTH, 0, Vec::new())
                .await
            {
                Ok((status, body)) if status == STATUS_OK && body.len() >= 17 => {
                    // Wire layout (Phase I): byte 0 is the SWIM state
                    // enum: 0=Joining, 1=Alive, 2=Suspect, 3=Dead.
                    match body[0] {
                        1 => "Alive".to_string(),
                        0 => {
                            all_alive = false;
                            "Joining".to_string()
                        }
                        2 => {
                            all_alive = false;
                            "Suspect".to_string()
                        }
                        3 => {
                            all_alive = false;
                            "Dead".to_string()
                        }
                        other => {
                            all_alive = false;
                            format!("Unknown({other})")
                        }
                    }
                }
                Ok((status, _)) => {
                    all_alive = false;
                    format!("status={status}")
                }
                Err(e) => {
                    all_alive = false;
                    format!("ERR({e})")
                }
            };
            node_states.push((n, state));
        }

        if all_alive {
            let since = alive_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() >= stability_window {
                if timing_enabled() {
                    eprintln!(
                        "  wait_specific_nodes_alive: {} nodes Alive for {:?} after {:.1}ms",
                        node_nums.len(),
                        stability_window,
                        start.elapsed().as_secs_f64() * 1000.0,
                    );
                }
                return Ok(());
            }
        } else {
            alive_since = None;
        }

        if timing_enabled() && last_log.elapsed() >= Duration::from_secs(2) {
            let detail: Vec<String> = node_states
                .iter()
                .map(|(n, s)| format!("node{n}={s}"))
                .collect();
            eprintln!(
                "  wait_specific_nodes_alive: states=[{}] ({:.1}s)",
                detail.join(", "),
                start.elapsed().as_secs_f64()
            );
            last_log = std::time::Instant::now();
        }

        if start.elapsed() >= timeout {
            let detail: Vec<String> = node_states
                .iter()
                .map(|(n, s)| format!("node{n}={s}"))
                .collect();
            return Err(ClientError::Connection(format!(
                "wait_specific_nodes_alive: not all nodes Alive after {timeout:?} [{}]",
                detail.join(", ")
            )));
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
    let mut ready_polls = 0u32;
    loop {
        let mut all_idle = true;
        let mut total_masters: u64 = 0;
        let mut total_inbound_pending: u64 = 0;
        let mut total_pending_handoffs: u64 = 0;
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
                total_pending_handoffs += pending_handoffs;
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
        if total_masters == 4096
            && total_pending_handoffs == 0
            && total_inbound_pending == 0
            && all_idle
        {
            ready_polls += 1;
            if ready_polls < 3 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            if timing_enabled() {
                eprintln!(
                    "  wait_specific_migrations: complete in {:.1}ms [{}]",
                    start.elapsed().as_secs_f64() * 1000.0,
                    status_details.join(", ")
                );
            }
            return Ok(());
        }
        ready_polls = 0;
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(format!(
                "migrations still active on specific nodes after {timeout:?} [masters={total_masters}/4096, handoffs={total_pending_handoffs}, inbound={total_inbound_pending}] [{}]",
                status_details.join(", ")
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One node's ACTIVATION-time shard view, as read from `/status`.
///
/// `serving_masters` is `master_shard_count` — shards this node currently
/// answers for, i.e. `ShardTable::effective_assignment`, which stays on the
/// OLD owner for any shard still in handoff. `target_masters` is
/// `target_master_shard_count` — `ShardTable::target_assignment`, the table
/// this node has actually ACTIVATED.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NodeShardView {
    /// 1-based node number the view was polled from.
    node: u32,
    /// Shards this node is serving as master right now.
    serving_masters: u64,
    /// Shards this node's activated table says it should master.
    target_masters: u64,
}

/// Decide whether the polled `/status` views describe a fully ACTIVATED and
/// settled shard table.
///
/// The migration gate's master-count condition (`sum == 4096`) is satisfied
/// by the topology that came BEFORE a membership change just as well as by
/// the one that follows it: right after a 4th node joins a 3-node cluster,
/// `1366 + 1365 + 1365 + 0 == 4096` holds while the new node still serves
/// nothing. That is how a scale-up scenario's "wait for migrations" returned
/// in 0.17s and let the shard-balance assertion read the pre-scale-up
/// distribution.
///
/// The commit-time predicates cannot close that race, and checking them here
/// would be a no-op: `wait_cluster_ready` already runs before these waits and
/// already pins `cluster_size == node_count` (which is
/// `alive_node_count()` — committed ∩ alive, so it implies the new member
/// list is committed) plus cross-node agreement on `shard_table_version`
/// (which IS the committed topology term). The window that stays open is
/// commit → ACTIVATION: a multi-node commit spawns a 2000ms exchange phase
/// and only activates the new table when that reports back
/// (`src/cluster/coordinator.rs`, "Phase D"), so a newcomer can be a
/// committed member, at the agreed term, while its shard counts still
/// describe the previous table. Only activation-time observables can see it:
///
/// 1. `target_masters > 0` on every node (for `node_count > 1`) — a
///    committed-but-not-yet-activated newcomer reports `target = 0`.
/// 2. `serving_masters == target_masters` on every node — serving has caught
///    up with the activated table. Completion-gated handoffs move serving
///    masters onto the newcomer one shard at a time while the SUM stays at
///    4096 the whole way, so the sum check cannot see that window at all.
///    (The existing per-node `pending_handoff_shards == 0` term — emitted
///    unconditionally by both `/status` branches — already implies this
///    clause; it is kept for the failure message, which names the exact
///    serving/target mismatch instead of a bare handoff count.)
///
/// Returns `None` when both hold for every polled node, otherwise the reason
/// the gate is being held, including the per-node serving/target dump so a
/// timeout is self-explanatory.
///
/// Nodes that did not answer `/status` contribute no view — same as the
/// master-count sum, which cannot see them either.
fn shard_activation_gate_reason(views: &[NodeShardView], node_count: u32) -> Option<String> {
    let detail = || {
        views
            .iter()
            .map(|v| {
                format!(
                    "node{}:serving={},target={}",
                    v.node, v.serving_masters, v.target_masters
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    if views.is_empty() {
        return Some("no node answered /status".to_string());
    }
    if node_count > 1
        && let Some(v) = views.iter().find(|v| v.target_masters == 0)
    {
        return Some(format!(
            "node{} has activated no master shards (target=0), so the current \
             shard table predates this membership [{}]",
            v.node,
            detail()
        ));
    }
    if let Some(v) = views.iter().find(|v| v.serving_masters != v.target_masters) {
        return Some(format!(
            "node{} serves {} master shards but its activated table targets {} \
             (handoff still moving) [{}]",
            v.node,
            v.serving_masters,
            v.target_masters,
            detail()
        ));
    }
    None
}

/// Wait until all active migrations complete on all nodes.
///
/// Also waits for shard master counts to sum to 4096 (all shards assigned)
/// to catch shards stuck in handoff after migration completion, and for every
/// polled node to have ACTIVATED the current shard table and finished moving
/// serving masters onto it — see [`shard_activation_gate_reason`] for why the
/// master-count sum alone is satisfied by the PREVIOUS topology.
pub async fn wait_migrations_complete(
    docker: &DockerHelpers,
    node_count: u32,
    timeout: Duration,
) -> Result<(), ClientError> {
    let mig_start = std::time::Instant::now();
    let mut mig_last_log = std::time::Instant::now();
    let start = std::time::Instant::now();
    let mut ready_polls = 0u32;
    loop {
        let mut all_idle = true;
        let mut total_masters: u64 = 0;
        let mut total_pending_handoffs: u64 = 0;
        let mut total_inbound_pending: u64 = 0;
        let mut node_details = Vec::new();
        let mut shard_views: Vec<NodeShardView> = Vec::new();
        for i in 1..=node_count {
            let port = docker.http_port(i);
            let url = format!("http://127.0.0.1:{port}/admin/migration_status");
            if let Ok(json) = poll_json(&url).await {
                let inbound_pending = json["inbound_pending"].as_u64().unwrap_or(0);
                total_inbound_pending += inbound_pending;
                if let Some(count) = json["active_count"].as_u64()
                    && count > 0
                {
                    all_idle = false;
                    node_details.push(format!("node{i}:mig={count}"));
                }
                if inbound_pending > 0 {
                    node_details.push(format!("node{i}:inbound={inbound_pending}"));
                }
            } else {
                node_details.push(format!("node{i}:migration-status-unavailable"));
            }
            let status_url = format!("http://127.0.0.1:{port}/status");
            if let Ok(json) = poll_json(&status_url).await {
                // Always record per-node master counts: a node silently
                // skipped here is indistinguishable from one reporting
                // masters=0, and that ambiguity has already cost a CI
                // failure investigation its evidence.
                let cluster_size = json["cluster_size"].as_u64().unwrap_or(0);
                let version = json["shard_table_version"].as_u64().unwrap_or(0);
                let m = json["master_shard_count"].as_u64().unwrap_or(0);
                let target_m = json["target_master_shard_count"].as_u64().unwrap_or(0);
                total_masters += m;
                node_details.push(format!(
                    "node{i}:size={cluster_size},ver={version},m={m},target={target_m}"
                ));
                shard_views.push(NodeShardView {
                    node: i,
                    serving_masters: m,
                    target_masters: target_m,
                });
                if let Some(h) = json["pending_handoff_shards"].as_u64() {
                    total_pending_handoffs += h;
                    if h > 0 {
                        node_details.push(format!("node{i}:handoff={h}"));
                    }
                }
            } else {
                node_details.push(format!("node{i}:status-unavailable"));
            }
        }
        let activation_reason = shard_activation_gate_reason(&shard_views, node_count);
        let masters_ok = total_masters == 4096 && activation_reason.is_none();
        if masters_ok && total_pending_handoffs == 0 && total_inbound_pending == 0 && all_idle {
            ready_polls += 1;
            if ready_polls < 3 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            if timing_enabled() {
                eprintln!(
                    "  wait_migrations: complete in {:.1}ms",
                    mig_start.elapsed().as_secs_f64() * 1000.0
                );
            }
            return Ok(());
        }
        ready_polls = 0;
        if timing_enabled() && mig_last_log.elapsed() >= Duration::from_secs(2) {
            eprintln!(
                "  wait_migrations: masters={total_masters}/4096, handoffs={total_pending_handoffs}, inbound={total_inbound_pending}, idle={all_idle}, activation={} ({:.1}s) [{}]",
                activation_reason.as_deref().unwrap_or("ok"),
                mig_start.elapsed().as_secs_f64(),
                node_details.join(", ")
            );
            mig_last_log = std::time::Instant::now();
        }
        if start.elapsed() >= timeout {
            // The activation reason carries the per-node serving/target dump,
            // so a gate held ONLY by an unactivated table names the nodes
            // holding it instead of leaving `masters=4096` looking settled.
            return Err(ClientError::Connection(format!(
                "migrations still active after {timeout:?} [masters={total_masters}/4096, handoffs={total_pending_handoffs}, inbound={total_inbound_pending}, activation={}] [{}]",
                activation_reason.as_deref().unwrap_or("ok"),
                node_details.join(", ")
            )));
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
/// and carrying — via [`format_master_failed_diagnostic`] — a per-record /
/// per-node breakdown derived from `OP_ADMIN_DIAGNOSE_KEY` (shard, master,
/// holder, inbound, fenced, migrating, topology_epoch) for the first 32
/// failing txids.
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

        if master_failed == 0 && under_replicated == 0 {
            if timing_enabled() {
                eprintln!(
                    "  wait_for_migration_reads_ready: {} txids verified ({} sampled for replicas) in {:.1}ms",
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
            // Diagnose the first 32 master-route failures via the
            // OP_ADMIN_DIAGNOSE_KEY admin op, which returns each
            // node's per-shard state (shard, master, holder, inbound,
            // fenced, migrating, topology epoch) for every txid in a
            // single batched call. The collection + formatting lives
            // in `collect_admin_diagnose_dump` so other helpers
            // (e.g. `wait_migrations_complete_with_diag`) can reuse it.
            let cap = (ADMIN_DIAGNOSE_KEY_MAX_TXIDS as usize).min(32);
            let failing_txids: Vec<[u8; 32]> = master_failed_idx
                .iter()
                .take(cap)
                .map(|&i| txids[i])
                .collect();

            let dump =
                collect_admin_diagnose_dump(client, &node_addrs, node_nums, &failing_txids).await;

            return Err(ClientError::Connection(format!(
                "migration read verify timeout after {timeout:?}: \
                 master_failed={master_failed}/{}, under_replicated={under_replicated}/{} \
                 (min_replicas={min_replicas}, nodes={node_nums:?}); \
                 first_failures (rich, n={}):{dump}",
                txids.len(),
                sample_indices.len(),
                failing_txids.len(),
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
            if let Ok(json) = poll_json(&url).await
                && let Some(seq) = json["current_sequence"].as_u64()
            {
                seqs.push(seq);
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
            return Err(ClientError::Connection(format!(
                "replication did not settle on {node_count} nodes after {timeout:?}; last redo sequences: {prev_seqs:?}",
            )));
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
            if let Ok(json) = poll_json(&url).await
                && let Some(seq) = json["current_sequence"].as_u64()
            {
                seqs.push(seq);
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
            return Err(ClientError::Connection(format!(
                "replication did not settle on nodes {node_nums:?} after {timeout:?}; last redo sequences: {prev_seqs:?}",
            )));
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
            eprintln!(
                "  start_3node_cluster[{scenario_id}]: compose_up (attempt {attempt}/{MAX_ATTEMPTS})"
            );
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
                wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;
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
            eprintln!(
                "  start_5node_cluster[{scenario_id}]: compose_up (attempt {attempt}/{MAX_ATTEMPTS})"
            );
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
                wait_migrations_complete(&docker, 5, Duration::from_secs(120)).await?;
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
type SeedMeta = ([u8; 32], Vec<[u8; 32]>);

/// Settle interval between the two reconcile read-backs. A record observed
/// present immediately after an ambiguous `ERR_REPLICATION_FAILED` (code 20)
/// may still be compensation-deleted moments later (spec §8.7); we re-read
/// after this delay and only treat a record as durably seeded if it is
/// still present, so reconcile does not race in-flight compensation.
const RECONCILE_SETTLE_INTERVAL: Duration = Duration::from_millis(750);

/// Read back a set of txids for reconcile, retrying once after a routing
/// refresh on connection error. Returns `None` if the read could not be
/// completed (the caller then leaves the items in the retry set).
async fn read_back_seed_records(
    client: &Client,
    txids: &[[u8; 32]],
) -> Option<teraslab_test_client::client::GetBatchResult> {
    match client.get_batch(FIELD_ALL_METADATA, txids).await {
        Ok(results) => Some(results),
        Err(_) => {
            let _ = client.refresh_routing().await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            client.get_batch(FIELD_ALL_METADATA, txids).await.ok()
        }
    }
}

/// For items that failed a create with an ambiguous outcome, check whether
/// the record actually landed and converged — and, if so, drop it from the
/// retry set so we do not re-create (which would surface `ERR_ALREADY_EXISTS`).
///
/// A record observed present immediately after an ambiguous code-20 outcome
/// is NOT yet authoritative: the server's compensation machinery may delete
/// it as it converges the partial-durability state (spec §8.7). To avoid
/// racing in-flight compensation, a record is only counted reconciled if it
/// is present on an initial read AND still present after
/// [`RECONCILE_SETTLE_INTERVAL`] (a proxy for "replication has quiesced"
/// that does not require Docker handles in scope). Records that disappear
/// in the interval are left in the retry set and re-created on the next
/// attempt — safe because create is idempotent by txid.
async fn reconcile_existing_seed_records(
    client: &Client,
    remaining_items: &mut Vec<CreateItem>,
    remaining_meta: &mut Vec<SeedMeta>,
    succeeded_meta: &mut Vec<SeedMeta>,
) -> usize {
    if remaining_items.is_empty() {
        return 0;
    }

    let txids: Vec<[u8; 32]> = remaining_meta.iter().map(|(txid, _)| *txid).collect();
    let Some(first) = read_back_seed_records(client, &txids).await else {
        return 0;
    };

    // Re-read after a settle interval so a record that was transiently
    // present (and is about to be compensation-deleted) is not mistaken for
    // a durable seed.
    tokio::time::sleep(RECONCILE_SETTLE_INTERVAL).await;
    let Some(second) = read_back_seed_records(client, &txids).await else {
        return 0;
    };

    let old_items = std::mem::take(remaining_items);
    let old_meta = std::mem::take(remaining_meta);
    let mut reconciled = 0usize;

    for (idx, (item, meta)) in old_items.into_iter().zip(old_meta).enumerate() {
        let durably_present =
            idx < first.len() && first.found(idx) && idx < second.len() && second.found(idx);
        if durably_present {
            succeeded_meta.push(meta);
            reconciled += 1;
        } else {
            remaining_items.push(item);
            remaining_meta.push(meta);
        }
    }

    reconciled
}

/// Publish every create the server has already acknowledged: record it in the
/// verifier and append its txid to the caller's list, draining the staging
/// buffer.
///
/// Chaos scenarios routinely wrap `seed_records` in `tokio::time::timeout`,
/// and a timeout DROPS the future — anything still sitting in a local staging
/// buffer at that moment is lost even though the server really did create
/// those records, so the caller reports "zero records created" for a window in
/// which the cluster created plenty. Publishing at every point where an ack is
/// observed, always BEFORE the next `.await`, keeps the verifier correct on
/// the cancellation path as well as on the give-up error path.
///
/// # Parameters
/// - `verifier`: shared expected-state tracker to record the creates into.
/// - `utxos_per_tx`: output count each seeded record was created with.
/// - `succeeded_meta`: staging buffer of acknowledged creates; drained.
/// - `txids`: caller's accumulating list of created txids.
fn publish_seeded_records(
    verifier: &StateVerifier,
    utxos_per_tx: u32,
    succeeded_meta: &mut Vec<SeedMeta>,
    txids: &mut Vec<[u8; 32]>,
) {
    for (txid, utxo_hashes) in succeeded_meta.drain(..) {
        verifier.record_create(txid, utxos_per_tx, utxo_hashes);
        txids.push(txid);
    }
}

/// Split a partial-failure response into "retry these" and "these landed".
///
/// Items whose index does not appear in the error list were applied by the
/// server, so they must not be re-sent (that would surface
/// `ERR_ALREADY_EXISTS`) and must not be forgotten. This runs on EVERY failed
/// attempt, including the last one, so a partial success on the final attempt
/// is accounted for instead of being discarded along with the returned error.
///
/// A non-partial error leaves both sets untouched: nothing is known to have
/// landed, so the whole batch stays in the retry set (and is reconciled).
///
/// Two more cases credit NOTHING and leave the whole batch in the retry set:
///
/// * `PartialError::degraded` — the applied items were replicated below
///   quorum (single-node durable, may be lost if that node dies before
///   catch-up streaming; see `client/rust/src/errors.rs`). Callers of
///   `seed_records` feed the verifier into hard assertions such as
///   `assert_rf2_replication_exact` and the acked-write-durability check
///   after a SIGKILL, so a degraded ack is not a durable seed. Leaving those
///   items in the retry set hands them to `reconcile_existing_seed_records`,
///   whose two-read settle check confirms them only once they survive.
/// * an item index at or past the end of the retry set — the client's
///   sub-batch→batch index remap leaves an out-of-range index UNMAPPED
///   (`client/rust/src/lib.rs`, `remap_batch_errors`), and credit here is by
///   COMPLEMENT of the failed set, so one malformed index would silently
///   promote a failed item into a phantom "created" record that a later hard
///   assertion demands to exist.
///
/// # Parameters
/// - `err`: the error returned by `create_batch`.
/// - `attempt`: 0-based attempt number, for the diagnostic line only.
/// - `remaining_items`/`remaining_meta`: the retry set, filtered in place.
/// - `succeeded_meta`: staging buffer the acknowledged items are moved into.
fn split_partial_successes(
    err: &ClientError,
    attempt: u32,
    remaining_items: &mut Vec<CreateItem>,
    remaining_meta: &mut Vec<SeedMeta>,
    succeeded_meta: &mut Vec<SeedMeta>,
) {
    let ClientError::Partial(pe) = err else {
        return;
    };
    let mut code_counts = std::collections::BTreeMap::new();
    for item_err in &pe.errors {
        *code_counts.entry(item_err.code).or_insert(0usize) += 1;
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
    if pe.degraded {
        eprintln!(
            "seed_records: partial error on attempt {attempt} is degraded=true \
             (below-quorum durability) [{code_summary}]: crediting none of the \
             {} item(s), reconcile confirms them instead",
            remaining_items.len()
        );
        return;
    }
    let failed_indices: std::collections::HashSet<usize> =
        pe.errors.iter().map(|e| e.item_index as usize).collect();
    if let Some(out_of_range) = failed_indices.iter().find(|i| **i >= remaining_items.len()) {
        eprintln!(
            "seed_records: partial error on attempt {attempt} reports item_index \
             {out_of_range} for a {}-item batch [{code_summary}]: indices are \
             untrustworthy, crediting none of them",
            remaining_items.len()
        );
        return;
    }
    let mut retry_items = Vec::new();
    let mut retry_meta = Vec::new();
    for (i, (item, meta)) in remaining_items
        .drain(..)
        .zip(remaining_meta.drain(..))
        .enumerate()
    {
        if failed_indices.contains(&i) {
            retry_items.push(item);
            retry_meta.push(meta);
        } else {
            succeeded_meta.push(meta);
        }
    }
    *remaining_items = retry_items;
    *remaining_meta = retry_meta;
    eprintln!(
        "seed_records: partial error on attempt {attempt}: {} failed item(s) [{code_summary}]",
        failed_indices.len()
    );
}

/// Fold ONE `create_batch` attempt into the seed accounting.
///
/// `attempt_error` is `None` when the attempt returned `Ok` — every remaining
/// item was acknowledged — and `Some(err)` otherwise. Acknowledged items are
/// moved out of the retry set and published into the verifier here, before
/// control returns to the loop and therefore before the loop's next `.await`
/// (see [`publish_seeded_records`] for why that ordering is load-bearing).
///
/// This runs on EVERY attempt, the final one included: a partial success on
/// the last attempt used to be discarded along with the returned error.
///
/// The staging buffer is local: split and publish happen back-to-back, so
/// nothing an ack produced can be stranded between them.
///
/// # Parameters
/// - `attempt_error`: `None` for a fully successful attempt, else the error.
/// - `attempt`: 0-based attempt number, for diagnostics only.
/// - `verifier`/`utxos_per_tx`/`txids`: where acknowledged creates are published.
/// - `remaining_items`/`remaining_meta`: the retry set, filtered in place.
///
/// Returns true when the retry set is now empty, i.e. the batch is complete.
fn record_batch_attempt(
    attempt_error: Option<&ClientError>,
    attempt: u32,
    verifier: &StateVerifier,
    utxos_per_tx: u32,
    remaining_items: &mut Vec<CreateItem>,
    remaining_meta: &mut Vec<SeedMeta>,
    txids: &mut Vec<[u8; 32]>,
) -> bool {
    let mut succeeded_meta: Vec<SeedMeta> = Vec::new();
    match attempt_error {
        None => {
            succeeded_meta.append(remaining_meta);
            remaining_items.clear();
        }
        Some(err) => split_partial_successes(
            err,
            attempt,
            remaining_items,
            remaining_meta,
            &mut succeeded_meta,
        ),
    }
    publish_seeded_records(verifier, utxos_per_tx, &mut succeeded_meta, txids);
    remaining_items.is_empty()
}

/// The client round-trips one seed batch makes, behind a seam.
///
/// [`seed_one_batch`]'s value is in its COMPOSITION — publish at every point
/// an ack is observed, and always before the next `.await` — which no
/// helper-level test can pin. This trait lets the unit tests drive the real
/// loop with scripted outcomes and a backoff that never returns, so a
/// regression in that composition (a publish moved back to the end of the
/// batch, the final-attempt split re-gated behind `attempt + 1 < MAX`) fails
/// a test instead of silently under-reporting creates in Docker.
trait SeedBatchDriver {
    /// Send one create batch. `Ok` means every item in it was acknowledged.
    async fn send_batch(&mut self, items: &[CreateItem]) -> Result<(), ClientError>;

    /// Read back the ambiguous items and move the durably-present ones into
    /// `succeeded_meta`. Returns how many were reconciled.
    async fn reconcile(
        &mut self,
        remaining_items: &mut Vec<CreateItem>,
        remaining_meta: &mut Vec<SeedMeta>,
        succeeded_meta: &mut Vec<SeedMeta>,
    ) -> usize;

    /// Wait out the retry backoff for `attempt`, then refresh routing.
    async fn backoff(&mut self, attempt: u32);
}

/// The production driver: a real clustered client.
struct ClientSeedDriver<'a> {
    client: &'a Client,
}

impl SeedBatchDriver for ClientSeedDriver<'_> {
    async fn send_batch(&mut self, items: &[CreateItem]) -> Result<(), ClientError> {
        self.client.create_batch(items).await.map(|_| ())
    }

    async fn reconcile(
        &mut self,
        remaining_items: &mut Vec<CreateItem>,
        remaining_meta: &mut Vec<SeedMeta>,
        succeeded_meta: &mut Vec<SeedMeta>,
    ) -> usize {
        reconcile_existing_seed_records(
            self.client,
            remaining_items,
            remaining_meta,
            succeeded_meta,
        )
        .await
    }

    async fn backoff(&mut self, attempt: u32) {
        tokio::time::sleep(teraslab_test_client::retry::backoff_for_attempt(attempt)).await;
        let _ = self.client.refresh_routing().await;
    }
}

/// Drive one batch of creates to completion, retrying only what has not been
/// acknowledged and publishing what has.
///
/// Retries transient errors from SWIM instability, dead nodes, cluster
/// topology changes, or ambiguous `ERR_REPLICATION_FAILED` (code 20). Backoff
/// and the attempt budget come from the shared `retry` policy module so every
/// mutation helper rides out the same post-topology-change settle window.
///
/// Errors: returns the last error from the driver if the retry budget is
/// exhausted, or `ClientError::Connection` if items remain unacknowledged
/// without a final error to attribute it to.
async fn seed_one_batch<D: SeedBatchDriver>(
    driver: &mut D,
    verifier: &StateVerifier,
    utxos_per_tx: u32,
    mut remaining_items: Vec<CreateItem>,
    mut remaining_meta: Vec<SeedMeta>,
    txids: &mut Vec<[u8; 32]>,
) -> Result<(), ClientError> {
    const MAX_SEED_RETRIES: u32 = teraslab_test_client::retry::MAX_TRANSIENT_ATTEMPTS;

    for attempt in 0..MAX_SEED_RETRIES {
        let attempt_result = driver.send_batch(&remaining_items).await;
        // Account for the attempt BEFORE anything else awaits: from here the
        // future can be cancelled by a caller's timeout, and a dropped future
        // must not take server-acked creates with it.
        let complete = record_batch_attempt(
            attempt_result.as_ref().err(),
            attempt,
            verifier,
            utxos_per_tx,
            &mut remaining_items,
            &mut remaining_meta,
            txids,
        );
        let Err(e) = attempt_result else {
            break;
        };
        if complete {
            // The response failed as a whole but every item in it landed.
            break;
        }
        if attempt + 1 >= MAX_SEED_RETRIES {
            let degraded = matches!(&e, ClientError::Partial(pe) if pe.degraded);
            eprintln!(
                "seed_records: failed after {MAX_SEED_RETRIES} attempts (degraded={degraded}): {e}"
            );
            return Err(e);
        }
        let mut reconciled_meta: Vec<SeedMeta> = Vec::new();
        let reconciled = driver
            .reconcile(
                &mut remaining_items,
                &mut remaining_meta,
                &mut reconciled_meta,
            )
            .await;
        if reconciled > 0 {
            eprintln!(
                "seed_records: reconciled {reconciled} ambiguous existing record(s) after attempt {attempt}"
            );
        }
        // Reconcile promotes items too; publish before the backoff sleep for
        // the same cancellation reason.
        publish_seeded_records(verifier, utxos_per_tx, &mut reconciled_meta, txids);
        if remaining_items.is_empty() {
            break;
        }
        if attempt == 0 {
            eprintln!(
                "seed_records: transient error on attempt {attempt}, \
                retrying {} items: {e}",
                remaining_items.len()
            );
        }
        driver.backoff(attempt).await;
    }

    if !remaining_items.is_empty() {
        return Err(ClientError::Connection(format!(
            "create_batch: {} items still failing after retries",
            remaining_items.len()
        )));
    }
    Ok(())
}

pub async fn seed_records(
    client: &Client,
    verifier: &StateVerifier,
    count: u32,
    utxos_per_tx: u32,
) -> Result<Vec<[u8; 32]>, ClientError> {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut txids = Vec::with_capacity(count as usize);
    let mut driver = ClientSeedDriver { client };

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
        //
        // On partial success, only retry the failed items (not items that
        // already succeeded — re-sending those would cause ERR_ALREADY_EXISTS).
        // Acknowledged items are published into the verifier as soon as they
        // are observed (see `publish_seeded_records`) rather than at the end of
        // the batch, so a caller that wraps this call in a timeout still sees
        // every create the cluster actually performed.
        seed_one_batch(
            &mut driver,
            verifier,
            utxos_per_tx,
            items,
            batch_meta,
            &mut txids,
        )
        .await?;
    }

    Ok(txids)
}

/// Tear down the Docker cluster for a specific scenario and wait for cleanup.
pub async fn teardown(docker: &mut DockerHelpers) {
    force_cleanup(docker.scenario_id()).await;
    wait_ports_free(
        docker.http_port(1),
        docker.scenario_id(),
        docker.node_count(),
    )
    .await;
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
            if result.status() == 0 {
                found += 1;
            } else {
                not_found += 1;
            }
        }
    }
    if timing_enabled() {
        eprintln!(
            "  count_accessible: {found}/{total} found in {:.1}ms",
            start.elapsed().as_secs_f64() * 1000.0
        );
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
                    Ok(r) => {
                        res = Some(r);
                        break;
                    }
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

            match parse_metadata_fields(result.data()) {
                Some((spent_count, is_mined, is_conflicting, is_locked)) => {
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
                None => {
                    // Status=0 (found) but metadata failed to parse — this is
                    // a corrupt record, not a missing one. Count it as a
                    // mismatch so verify_consistency fails rather than skipping.
                    all_mismatches.push(Mismatch {
                        txid: *txid,
                        field: "metadata_parse".to_string(),
                        expected: "parseable FIELD_ALL_METADATA response".to_string(),
                        actual: format!(
                            "status=0 but data too short or corrupt ({} bytes)",
                            result.data().len()
                        ),
                    });
                }
            }
        }
    }

    // Retry NotFound records after refreshing routing — the partition map may
    // have been stale for shards that recently migrated.
    if !not_found_txids.is_empty() {
        eprintln!(
            "verify_consistency: {} records NotFound on first pass, retrying after routing refresh...",
            not_found_txids.len()
        );
        let _ = client.refresh_routing().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = client.refresh_routing().await;

        for chunk in not_found_txids.chunks(500) {
            let results = client.get_batch(FIELD_ALL_METADATA, chunk).await?;

            for (i, result) in results.iter().enumerate() {
                let txid = &chunk[i];

                if result.status() != 0 {
                    let mm = verifier.verify_record(txid, 0, false, false, false, true);
                    all_mismatches.extend(mm);
                    continue;
                }

                match parse_metadata_fields(result.data()) {
                    Some((spent_count, is_mined, is_conflicting, is_locked)) => {
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
                    None => {
                        // Status=0 (found) but metadata failed to parse — corrupt
                        // record on retry pass; flag as mismatch.
                        all_mismatches.push(Mismatch {
                            txid: *txid,
                            field: "metadata_parse".to_string(),
                            expected: "parseable FIELD_ALL_METADATA response".to_string(),
                            actual: format!(
                                "status=0 but data too short or corrupt ({} bytes)",
                                result.data().len()
                            ),
                        });
                    }
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
        let results = client.get_batch(FIELD_ALL_METADATA, chunk).await?;
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

/// Capture container logs and HTTP state snapshots for a failed scenario
/// BEFORE teardown destroys the containers.
///
/// Writes into the directory named by `TERASLAB_DIAG_DIR` (exported per
/// scenario by run_all.sh, so CI artifacts pick it up); silently does nothing
/// when the variable is unset (e.g. direct `cargo test` runs). The
/// harness-side collect_logs.sh cannot do this: the in-test teardown on the
/// failure path removes the containers before it runs.
pub async fn collect_failure_diagnostics(scenario_id: u16) {
    let Ok(dir) = std::env::var("TERASLAB_DIAG_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    for n in 1..=5u16 {
        let name = format!("ts{scenario_id:02}-node{n}");
        if let Ok(out) = std::process::Command::new("docker")
            .args(["logs", &name])
            .output()
        {
            let mut buf = out.stdout;
            buf.extend_from_slice(&out.stderr);
            if !buf.is_empty() {
                let _ = std::fs::write(dir.join(format!("{name}.log")), &buf);
            }
        }
        let port = 19000 + scenario_id * 10 + (n - 1);
        for (ep, fname) in [
            ("status", "status"),
            ("admin/migration_status", "migration_status"),
        ] {
            let url = format!("http://127.0.0.1:{port}/{ep}");
            if let Ok(json) = poll_json(&url).await {
                let _ = std::fs::write(dir.join(format!("node{n}_{fname}.json")), json.to_string());
            }
        }
    }
    eprintln!(
        "  [diag] in-test failure diagnostics written to {}",
        dir.display()
    );
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
        let ids = docker_output_timeout(
            &[
                "ps".to_string(),
                "-aq".to_string(),
                "--filter".to_string(),
                container_filter.clone(),
            ],
            Duration::from_secs(5),
        )
        .await
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
            let vols = docker_output_timeout(
                &[
                    "volume".to_string(),
                    "ls".to_string(),
                    "-q".to_string(),
                    "--filter".to_string(),
                    volume_filter.clone(),
                ],
                Duration::from_secs(5),
            )
            .await
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
            let nets = docker_output_timeout(
                &[
                    "network".to_string(),
                    "ls".to_string(),
                    "-q".to_string(),
                    "--filter".to_string(),
                    network_filter.clone(),
                ],
                Duration::from_secs(5),
            )
            .await
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
/// Polls `GET /health/live` every 50ms, returns as soon as it gets a 200,
/// or after `timeout` elapses.
pub async fn wait_node_healthy(
    docker: &DockerHelpers,
    node_num: u32,
    timeout: Duration,
) -> Result<(), ClientError> {
    let port = docker.http_port(node_num);
    let url = format!("http://127.0.0.1:{port}/health/live");
    let start = std::time::Instant::now();
    loop {
        if let Ok(resp) = poll_http_client().get(&url).send().await
            && resp.status().is_success()
        {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(ClientError::Connection(format!(
                "node {node_num} not healthy after {timeout:?}"
            )));
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
    client
        .send_to_addr(node_addr, OP_GET_BATCH, FLAG_LOCAL_READ, payload)
        .await
}

/// Parse a batch get response into per-item (status, data) pairs.
pub fn parse_batch_response(payload: &[u8]) -> Vec<(u8, Vec<u8>)> {
    parse_batch_response_exact(payload, None).unwrap_or_default()
}

fn parse_batch_response_exact(
    payload: &[u8],
    expected_count: Option<usize>,
) -> Option<Vec<(u8, Vec<u8>)>> {
    let mut items = Vec::new();
    if payload.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    if let Some(expected) = expected_count
        && count != expected
    {
        return None;
    }
    let mut offset = 4;
    for _ in 0..count {
        if offset >= payload.len() {
            return None;
        }
        let status = payload[offset];
        offset += 1;
        if offset + 4 > payload.len() {
            return None;
        }
        let data_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + data_len > payload.len() {
            return None;
        }
        let data = payload[offset..offset + data_len].to_vec();
        offset += data_len;
        items.push((status, data));
    }
    if offset != payload.len() {
        return None;
    }
    Some(items)
}

async fn direct_get_items(
    client: &Client,
    addr: &str,
    txids: &[[u8; 32]],
) -> Result<Vec<(u8, Vec<u8>)>, ClientError> {
    let (frame_status, payload) = direct_get(client, addr, txids).await?;
    if frame_status != STATUS_OK {
        return Ok(vec![(1, vec![]); txids.len()]);
    }
    if let Some(items) = parse_batch_response_exact(&payload, Some(txids.len())) {
        return Ok(items);
    }

    let mut items = Vec::with_capacity(txids.len());
    for txid in txids {
        let (single_status, single_payload) =
            direct_get(client, addr, std::slice::from_ref(txid)).await?;
        if single_status != STATUS_OK {
            items.push((1, vec![]));
            continue;
        }
        match parse_batch_response_exact(&single_payload, Some(1)) {
            Some(mut parsed) => items.push(parsed.remove(0)),
            None => items.push((1, vec![])),
        }
    }
    Ok(items)
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

#[derive(Debug, Default)]
struct ReplicationCheckReport {
    mismatches: u32,
    holder_errors: u32,
    holder_count_histogram: Vec<u32>,
    holder_examples: Vec<String>,
    mismatch_examples: Vec<String>,
}

impl ReplicationCheckReport {
    fn new(node_count: usize) -> Self {
        Self {
            holder_count_histogram: vec![0; node_count + 1],
            ..Self::default()
        }
    }

    fn record_holder_error(&mut self, txid: &[u8; 32], holder_indices: &[usize]) {
        self.holder_errors += 1;
        let count = holder_indices.len();
        if count >= self.holder_count_histogram.len() {
            self.holder_count_histogram.resize(count + 1, 0);
        }
        self.holder_count_histogram[count] += 1;
        if self.holder_examples.len() < 8 {
            self.holder_examples.push(format!(
                "{} holders={:?}",
                txid_prefix(txid),
                holder_indices
            ));
        }
    }

    fn record_mismatch(&mut self, txid: &[u8; 32], holder_indices: &[usize], a: &[u8], b: &[u8]) {
        self.mismatches += 1;
        if self.mismatch_examples.len() < 8 {
            let diffs = first_payload_diffs(a, b, 4);
            self.mismatch_examples.push(format!(
                "{} holders={:?} diffs={}",
                txid_prefix(txid),
                holder_indices,
                diffs,
            ));
        }
    }

    fn holder_diagnostics(&self) -> String {
        let histogram = self
            .holder_count_histogram
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .map(|(holders, count)| format!("{holders}:{count}"))
            .collect::<Vec<_>>()
            .join(", ");
        let examples = if self.holder_examples.is_empty() {
            "none".to_string()
        } else {
            self.holder_examples.join("; ")
        };
        format!("holder_count_histogram=[{histogram}], examples=[{examples}]")
    }

    fn mismatch_diagnostics(&self) -> String {
        if self.mismatch_examples.is_empty() {
            "examples=[none]".to_string()
        } else {
            format!("examples=[{}]", self.mismatch_examples.join("; "))
        }
    }
}

fn txid_prefix(txid: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(16);
    for byte in txid.iter().take(8) {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
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
    let report = batch_verify_replication_report(client, node_addrs, txids, expect_present).await?;
    Ok((report.mismatches, report.holder_errors))
}

async fn batch_verify_replication_report(
    client: &Client,
    node_addrs: &[String],
    txids: &[[u8; 32]],
    expect_present: bool,
) -> Result<ReplicationCheckReport, ClientError> {
    const CHUNK_SIZE: usize = 500;

    let mut node_items: Vec<Vec<(u8, Vec<u8>)>> = Vec::new();
    for addr in node_addrs {
        let mut all_items = Vec::with_capacity(txids.len());
        for chunk in txids.chunks(CHUNK_SIZE) {
            all_items.extend(direct_get_items(client, addr, chunk).await?);
        }
        node_items.push(all_items);
    }

    let mut report = ReplicationCheckReport::new(node_addrs.len());

    for (idx, txid) in txids.iter().enumerate() {
        let mut holder_indices = Vec::new();
        for (node_idx, items) in node_items.iter().enumerate() {
            if idx < items.len() && items[idx].0 == 0 {
                holder_indices.push(node_idx);
            }
        }

        if expect_present {
            if holder_indices.len() != 2 {
                report.record_holder_error(txid, &holder_indices);
                continue;
            }
            let a = &node_items[holder_indices[0]][idx].1;
            let b = &node_items[holder_indices[1]][idx].1;
            if !payloads_match(a, b) {
                let precise_a = direct_get_items(
                    client,
                    node_addrs[holder_indices[0]].as_str(),
                    std::slice::from_ref(txid),
                )
                .await?
                .into_iter()
                .next()
                .unwrap_or((1, vec![]));
                let precise_b = direct_get_items(
                    client,
                    node_addrs[holder_indices[1]].as_str(),
                    std::slice::from_ref(txid),
                )
                .await?
                .into_iter()
                .next()
                .unwrap_or((1, vec![]));
                if precise_a.0 != STATUS_OK || precise_b.0 != STATUS_OK {
                    report.record_holder_error(txid, &holder_indices);
                } else if !payloads_match(&precise_a.1, &precise_b.1) {
                    report.record_mismatch(txid, &holder_indices, &precise_a.1, &precise_b.1);
                }
            }
        } else {
            if !holder_indices.is_empty() {
                report.record_holder_error(txid, &holder_indices);
            }
        }
    }

    Ok(report)
}

fn first_payload_diffs(a: &[u8], b: &[u8], limit: usize) -> String {
    let mut diffs = Vec::new();
    for i in 0..a.len().min(b.len()) {
        if (61..69).contains(&i) {
            continue;
        }
        if a[i] != b[i] {
            diffs.push(format!("{i}:{}!={}", a[i], b[i]));
            if diffs.len() >= limit {
                break;
            }
        }
    }
    if a.len() != b.len() {
        diffs.push(format!("len:{}!={}", a.len(), b.len()));
    }
    if diffs.is_empty() {
        "none-after-ignored-fields".to_string()
    } else {
        diffs.join(",")
    }
}

/// How long [`assert_rf2_replication_exact`] keeps re-taking the holder
/// census before it calls a violation permanent.
///
/// Both directions of an RF=2 violation are CONVERGING conditions right
/// after a migration: over-replication clears when the coordinator's orphan
/// cleanup — deliberately spawned detached once `active_count` reaches 0 —
/// finishes its sweep, and under-replication clears when repair backfills
/// the missing holder. A single instantaneous sample taken the moment the
/// migration counters hit zero therefore fails on records that are merely
/// mid-sweep; one observed run had cleanup finish 0.4-0.7s AFTER the
/// assertion fired, with 16/294 records still showing 3 holders.
const RF2_CENSUS_WINDOW: Duration = Duration::from_secs(15);

/// Delay between holder-census rounds inside [`RF2_CENSUS_WINDOW`].
const RF2_CENSUS_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// What to do after one round of the RF=2 holder census.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rf2CensusVerdict {
    /// Every record has exactly RF=2 holders with identical payloads.
    Settled,
    /// Violations remain but the convergence window has time left.
    Retry,
    /// Violations survived the whole window — assert on THIS census.
    Fail,
}

/// Classify one census round.
///
/// `elapsed` is the time since the FIRST round started, so the window is a
/// bound on total convergence time rather than a per-round timeout. The
/// exact-equality requirement is unchanged; it is simply evaluated at the
/// deadline instead of at the first sample.
fn rf2_census_verdict(
    holder_errors: u32,
    mismatches: u32,
    elapsed: Duration,
    window: Duration,
) -> Rf2CensusVerdict {
    if holder_errors == 0 && mismatches == 0 {
        Rf2CensusVerdict::Settled
    } else if elapsed < window {
        Rf2CensusVerdict::Retry
    } else {
        Rf2CensusVerdict::Fail
    }
}

/// Assert that every present record has exactly the RF=2 holder count and
/// byte-identical local-read payloads across its holders.
///
/// The census is re-taken every [`RF2_CENSUS_POLL_INTERVAL`] for up to
/// [`RF2_CENSUS_WINDOW`]; violations only fail the scenario if they are
/// still present at the deadline, and the assertion then reports the LAST
/// census's histogram and examples.
pub async fn assert_rf2_replication_exact(
    client: &Client,
    docker: &DockerHelpers,
    node_count: usize,
    txids: &[[u8; 32]],
    label: &str,
) -> Result<(), ClientError> {
    if txids.is_empty() {
        return Ok(());
    }
    let node_addrs = docker.host_client_addrs(node_count);
    let start = std::time::Instant::now();
    let mut rounds = 0u32;
    loop {
        let report = batch_verify_replication_report(client, &node_addrs, txids, true).await?;
        rounds += 1;
        match rf2_census_verdict(
            report.holder_errors,
            report.mismatches,
            start.elapsed(),
            RF2_CENSUS_WINDOW,
        ) {
            Rf2CensusVerdict::Settled => {
                if rounds > 1 {
                    eprintln!(
                        "[{label}] RF=2 census settled after {rounds} rounds ({:.1}s of convergence)",
                        start.elapsed().as_secs_f64()
                    );
                }
                return Ok(());
            }
            Rf2CensusVerdict::Retry => {
                tokio::time::sleep(RF2_CENSUS_POLL_INTERVAL).await;
            }
            Rf2CensusVerdict::Fail => {
                let elapsed = start.elapsed().as_secs_f64();
                assert_eq!(
                    report.holder_errors,
                    0,
                    "{label}: {}/{} records did not have exactly RF=2 local holders ({}) \
                     [still violated after {rounds} census rounds over {elapsed:.1}s]",
                    report.holder_errors,
                    txids.len(),
                    report.holder_diagnostics(),
                );
                assert_eq!(
                    report.mismatches,
                    0,
                    "{label}: {}/{} records had non-identical local holder payloads ({}) \
                     [still violated after {rounds} census rounds over {elapsed:.1}s]",
                    report.mismatches,
                    txids.len(),
                    report.mismatch_diagnostics(),
                );
                unreachable!(
                    "Rf2CensusVerdict::Fail requires holder_errors or mismatches to be non-zero"
                );
            }
        }
    }
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
        if frame_status == STATUS_OK && !payload.is_empty() && payload.len() >= 4 {
            let count = u32::from_le_bytes(payload[0..4].try_into().unwrap());
            if count >= 1 && payload.len() >= 5 {
                let item_status = payload[4];
                if item_status == 0 {
                    holders.push(i);
                    continue;
                }
            }
        }
        non_holders.push(i);
    }
    Ok((holders, non_holders))
}

/// Format a per-record / per-node diagnostic dump for the
/// `wait_for_migration_reads_ready` timeout path.
///
/// `failing_txids[i]` is the i-th failing txid; the result contains one
/// line per failing txid (joined by `\n  ` with a leading `\n  `). Each
/// line summarizes — across every surveyed node — the
/// `(shard, master, holder, inbound, fenced, migrating, topology_epoch)`
/// state derived from `OP_ADMIN_DIAGNOSE_KEY`.
///
/// `node_nums[j]` is the cluster node number of the j-th surveyed node,
/// `per_node_responses[j]` is its response:
///
/// - `Ok(diagnoses)`: `diagnoses[i]` corresponds to `failing_txids[i]`,
///   in the same order. A length mismatch is surfaced inline as an
///   `ERR(...)` value rather than panicking, so callers always get a
///   useful dump.
/// - `Err(s)`: that node could not be reached. Every column for that
///   node renders as `n<num>:ERR(<s>)`.
///
/// If two nodes disagree on the shard for a given txid, the line is
/// suffixed with ` (SHARD_MISMATCH)`. The shown shard is the first
/// successful response's shard.
///
/// This is a pure function so it can be unit-tested without a live
/// cluster — see the `format_master_failed_diagnostic_*` tests in the
/// same module.
pub fn format_master_failed_diagnostic(
    failing_txids: &[[u8; 32]],
    node_nums: &[u32],
    per_node_responses: &[Result<Vec<teraslab::cluster::migration::KeyDiagnosis>, String>],
) -> String {
    use teraslab::cluster::migration::KeyDiagnosis;

    debug_assert_eq!(node_nums.len(), per_node_responses.len());

    // Per-node lookup of the i-th diagnosis, or an Err describing why
    // it is unavailable for this node. Cloning the error string per
    // call keeps the function pure and easy to reason about for tests.
    let lookup = |node_idx: usize, txid_idx: usize| -> Result<&KeyDiagnosis, String> {
        match &per_node_responses[node_idx] {
            Err(e) => Err(e.clone()),
            Ok(v) => v.get(txid_idx).ok_or_else(|| {
                format!(
                    "missing entry: node returned {} of {}",
                    v.len(),
                    failing_txids.len()
                )
            }),
        }
    };

    // Render `[n1:Y, n2:N, n3:ERR(...)]` for one boolean column.
    let render_bool_row = |txid_idx: usize, pick: &dyn Fn(&KeyDiagnosis) -> bool| -> String {
        let cells: Vec<String> = node_nums
            .iter()
            .enumerate()
            .map(|(j, n)| match lookup(j, txid_idx) {
                Ok(d) => format!("n{n}:{}", if pick(d) { 'Y' } else { 'N' }),
                Err(e) => format!("n{n}:ERR({e})"),
            })
            .collect();
        format!("[{}]", cells.join(", "))
    };

    // Same shape but renders the topology epoch (a u64) per node.
    let render_epoch_row = |txid_idx: usize| -> String {
        let cells: Vec<String> = node_nums
            .iter()
            .enumerate()
            .map(|(j, n)| match lookup(j, txid_idx) {
                Ok(d) => format!("n{n}:{}", d.topology_epoch),
                Err(e) => format!("n{n}:ERR({e})"),
            })
            .collect();
        format!("[{}]", cells.join(", "))
    };

    let mut lines: Vec<String> = Vec::with_capacity(failing_txids.len());
    for (i, txid) in failing_txids.iter().enumerate() {
        let prefix: String = txid[..6].iter().map(|b| format!("{b:02x}")).collect();

        // Shard agreement: pick the first successful response's shard
        // and check all other successful responses against it. If none
        // succeed (every node erred), fall back to a literal `?`.
        let mut shard_repr = String::from("?");
        let mut shard_seen: Option<u16> = None;
        let mut shard_mismatch = false;
        for j in 0..node_nums.len() {
            if let Ok(d) = lookup(j, i) {
                match shard_seen {
                    None => {
                        shard_seen = Some(d.shard);
                        shard_repr = d.shard.to_string();
                    }
                    Some(s) if s != d.shard => {
                        shard_mismatch = true;
                    }
                    _ => {}
                }
            }
        }

        let masters = render_bool_row(i, &|d| d.is_local_master_of_shard);
        let holders = render_bool_row(i, &|d| d.has_local_data);
        let inbound = render_bool_row(i, &|d| d.has_pending_inbound);
        let fenced = render_bool_row(i, &|d| d.is_shard_fenced);
        let migrating = render_bool_row(i, &|d| d.is_migrating_shard);
        let epoch = render_epoch_row(i);

        let suffix = if shard_mismatch {
            " (SHARD_MISMATCH)"
        } else {
            ""
        };
        lines.push(format!(
            "txid={prefix} shard={shard_repr} masters_per_node={masters} holders={holders} \
             inbound={inbound} fenced={fenced} migrating={migrating} topo_epoch={epoch}{suffix}",
        ));
    }

    if lines.is_empty() {
        String::new()
    } else {
        format!("\n  {}", lines.join("\n  "))
    }
}

/// Phase A follow-up: query `OP_ADMIN_DIAGNOSE_KEY` on every listed
/// node for `failing_txids`, then format the responses via
/// [`format_master_failed_diagnostic`] into the same per-record /
/// per-node table that `wait_for_migration_reads_ready` produces on
/// timeout.
///
/// Reusable across helpers: `wait_migrations_complete` calls it on
/// timeout when the caller supplies a sample of tracked txids, and
/// scenario tests can call it ad-hoc when they detect an unexpected
/// state in the middle of a run. `node_addrs` and `node_nums` must be
/// in the same order — the i-th address is queried, and the dump
/// renders columns labelled `n{node_nums[i]}`.
///
/// Returns an empty string when `failing_txids` is empty so callers
/// can append the result unconditionally.
pub async fn collect_admin_diagnose_dump(
    client: &Client,
    node_addrs: &[String],
    node_nums: &[u32],
    failing_txids: &[[u8; 32]],
) -> String {
    debug_assert_eq!(node_addrs.len(), node_nums.len());
    if failing_txids.is_empty() {
        return String::new();
    }
    let mut per_node_responses: Vec<
        Result<Vec<teraslab::cluster::migration::KeyDiagnosis>, String>,
    > = Vec::with_capacity(node_addrs.len());
    for addr in node_addrs {
        let payload = encode_admin_diagnose_key(failing_txids);
        let result = match client
            .send_to_addr(addr, OP_ADMIN_DIAGNOSE_KEY, 0, payload)
            .await
        {
            Ok((frame_status, body)) => {
                if frame_status == STATUS_OK {
                    decode_admin_diagnose_key(&body)
                } else {
                    Err(format!("admin op returned status={frame_status}"))
                }
            }
            Err(e) => Err(e.to_string()),
        };
        per_node_responses.push(result);
    }
    format_master_failed_diagnostic(failing_txids, node_nums, &per_node_responses)
}

/// Phase A follow-up — variant of [`wait_migrations_complete`] that
/// collects an `OP_ADMIN_DIAGNOSE_KEY` dump on timeout for the first
/// `min(sample_txids.len(), ADMIN_DIAGNOSE_KEY_MAX_TXIDS, 32)` of
/// `sample_txids`. Use this when the caller has a representative set
/// of tracked records to probe — the dump is appended to the timeout
/// error so the failure log shows per-shard / per-node state for the
/// stuck records, not just aggregate counters.
///
/// Behaviour matches `wait_migrations_complete` on the success path.
/// Pass `&[]` for `sample_txids` to skip the dump.
pub async fn wait_migrations_complete_with_diag(
    docker: &DockerHelpers,
    node_count: u32,
    timeout: Duration,
    client: &Client,
    sample_txids: &[[u8; 32]],
) -> Result<(), ClientError> {
    match wait_migrations_complete(docker, node_count, timeout).await {
        Ok(()) => Ok(()),
        Err(e) if !sample_txids.is_empty() => {
            let cap = (ADMIN_DIAGNOSE_KEY_MAX_TXIDS as usize).min(32);
            let failing: Vec<[u8; 32]> = sample_txids.iter().take(cap).copied().collect();
            let node_nums: Vec<u32> = (1..=node_count).collect();
            let node_addrs: Vec<String> = node_nums
                .iter()
                .map(|&n| format!("127.0.0.1:{}", docker.client_port(n)))
                .collect();
            let dump = collect_admin_diagnose_dump(client, &node_addrs, &node_nums, &failing).await;
            let base = match &e {
                ClientError::Connection(s) => s.clone(),
                other => other.to_string(),
            };
            Err(ClientError::Connection(format!(
                "{base}; diag (first n={}):{dump}",
                failing.len(),
            )))
        }
        Err(e) => Err(e),
    }
}

/// Encode a request payload for `OP_ADMIN_DIAGNOSE_KEY`.
///
/// Layout: `[count: u32 LE][txid: 32B] * count`. The server enforces
/// `count <= ADMIN_DIAGNOSE_KEY_MAX_TXIDS` (currently 64) — passing more
/// will be rejected with `STATUS_ERROR` / `ERR_INTERNAL`.
pub fn encode_admin_diagnose_key(txids: &[[u8; 32]]) -> Vec<u8> {
    let count = txids.len() as u32;
    let mut payload = Vec::with_capacity(4 + txids.len() * 32);
    payload.extend_from_slice(&count.to_le_bytes());
    for txid in txids {
        payload.extend_from_slice(txid);
    }
    payload
}

/// Decode an `OP_ADMIN_DIAGNOSE_KEY` response payload (the body of a
/// `STATUS_OK` reply) into a `Vec<KeyDiagnosis>`.
///
/// Returns `Err(String)` describing the parse failure if the body is
/// truncated or its declared count does not match the byte length.
/// See [`teraslab::protocol::opcodes::OP_ADMIN_DIAGNOSE_KEY`] for the
/// per-entry layout.
pub fn decode_admin_diagnose_key(
    body: &[u8],
) -> Result<Vec<teraslab::cluster::migration::KeyDiagnosis>, String> {
    use teraslab::cluster::migration::KeyDiagnosis;
    use teraslab::protocol::opcodes::KEY_DIAGNOSIS_ENCODED_SIZE;

    if body.len() < 4 {
        return Err(format!(
            "diagnose response too short: {} bytes (need >=4)",
            body.len()
        ));
    }
    let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    let expected = 4 + count * KEY_DIAGNOSIS_ENCODED_SIZE;
    if body.len() != expected {
        return Err(format!(
            "diagnose response length {} != expected {} (count={})",
            body.len(),
            expected,
            count,
        ));
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let off = 4 + i * KEY_DIAGNOSIS_ENCODED_SIZE;
        let entry = &body[off..off + KEY_DIAGNOSIS_ENCODED_SIZE];
        let shard = u16::from_le_bytes(entry[0..2].try_into().unwrap());
        let this_node_id = u64::from_le_bytes(entry[2..10].try_into().unwrap());
        let local_view_canonical_master_id = u64::from_le_bytes(entry[10..18].try_into().unwrap());
        let has_local_data = entry[18] != 0;
        let is_local_master_of_shard = entry[19] != 0;
        let has_pending_inbound = entry[20] != 0;
        let is_shard_fenced = entry[21] != 0;
        let is_migrating_shard = entry[22] != 0;
        let topology_epoch = u64::from_le_bytes(entry[23..31].try_into().unwrap());
        let local_view_effective_master_id = u64::from_le_bytes(entry[31..39].try_into().unwrap());
        let is_serving_fenced = entry[39] != 0;
        out.push(KeyDiagnosis {
            shard,
            this_node_id,
            local_view_canonical_master_id,
            has_local_data,
            is_local_master_of_shard,
            has_pending_inbound,
            is_shard_fenced,
            is_migrating_shard,
            topology_epoch,
            local_view_effective_master_id,
            is_serving_fenced,
        });
    }
    Ok(out)
}

/// Poll until all HTTP ports for a scenario are free (connection refused).
/// Returns immediately once no port accepts connections, or after 10s at most.
async fn wait_ports_free(first_http_port: u16, _scenario_id: u16, node_count: u32) {
    let ports: Vec<u16> = (0..node_count)
        .map(|i| first_http_port + i as u16)
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let all_free = ports.iter().all(|&p| {
            std::net::TcpStream::connect_timeout(
                &std::net::SocketAddr::from(([127, 0, 0, 1], p)),
                Duration::from_millis(50),
            )
            .is_err()
        });
        if all_free || std::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod migration_gate_tests {
    use super::*;

    fn view(node: u32, serving_masters: u64, target_masters: u64) -> NodeShardView {
        NodeShardView {
            node,
            serving_masters,
            target_masters,
        }
    }

    /// Every shape below keeps `sum(serving) == 4096`, which is what makes
    /// them invisible to the master-count condition on its own.
    fn serving_sum(views: &[NodeShardView]) -> u64 {
        views.iter().map(|v| v.serving_masters).sum()
    }

    #[test]
    fn a_committed_but_unactivated_newcomer_holds_the_gate() {
        // The exact scenario-06 shape at 0.17s: node4 is a committed member
        // at the agreed term (wait_cluster_ready already passed), but the
        // 2000ms exchange phase has not reported back, so node4 has
        // activated nothing while nodes 1-3 still serve -- and target --
        // the 3-member table.
        let views = vec![
            view(1, 1366, 1366),
            view(2, 1365, 1365),
            view(3, 1365, 1365),
            view(4, 0, 0),
        ];
        assert_eq!(serving_sum(&views), 4096, "the sum check cannot see this");
        let reason = shard_activation_gate_reason(&views, 4)
            .expect("an unactivated newcomer holds the gate");
        assert!(
            reason.contains("node4 has activated no master shards (target=0)"),
            "reason was: {reason}"
        );
        // The dump the timeout path prints must name every node's counts.
        assert!(reason.contains("node4:serving=0,target=0"), "{reason}");
        assert!(
            reason.contains("node1:serving=1366,target=1366"),
            "{reason}"
        );
    }

    #[test]
    fn serving_behind_target_mid_handoff_holds_the_gate() {
        // Post-activation: all four nodes target the balanced 4-member table,
        // but completion-gated handoffs have not yet moved a single serving
        // master onto node4. The sum is STILL 4096 for this entire window.
        let views = vec![
            view(1, 1366, 1024),
            view(2, 1365, 1024),
            view(3, 1365, 1024),
            view(4, 0, 1024),
        ];
        assert_eq!(serving_sum(&views), 4096, "the sum check cannot see this");
        let reason =
            shard_activation_gate_reason(&views, 4).expect("mid-handoff must hold the gate");
        assert!(
            reason.contains("node1 serves 1366 master shards but its activated table targets 1024"),
            "reason was: {reason}"
        );
    }

    #[test]
    fn a_converged_four_node_table_passes_the_gate() {
        let views = vec![
            view(1, 1024, 1024),
            view(2, 1024, 1024),
            view(3, 1024, 1024),
            view(4, 1024, 1024),
        ];
        assert_eq!(serving_sum(&views), 4096);
        assert_eq!(shard_activation_gate_reason(&views, 4), None);
    }

    #[test]
    fn a_fully_drained_node_deadlocks_the_gate_by_design() {
        // serving 0 / target 0 on a polled node: clause (a) holds the gate
        // for the full timeout. No current call site polls a drained-but-
        // running node; a future "wait while node N is drained" caller must
        // poll only the remaining nodes or this gate will time out.
        let views = vec![view(1, 2048, 2048), view(2, 2048, 2048), view(3, 0, 0)];
        assert_eq!(serving_sum(&views), 4096);
        let reason =
            shard_activation_gate_reason(&views, 3).expect("a zero-target node must hold the gate");
        assert!(
            reason.contains("target=0"),
            "reason must name the zero-target clause, got: {reason}"
        );
    }

    #[test]
    fn a_converged_three_node_table_passes_the_gate() {
        // Scenario 07 after the shrink: nodes 1-3 are polled (node4's
        // container is gone), each serving exactly what it targets.
        let views = vec![
            view(1, 1366, 1366),
            view(2, 1365, 1365),
            view(3, 1365, 1365),
        ];
        assert_eq!(serving_sum(&views), 4096);
        assert_eq!(shard_activation_gate_reason(&views, 3), None);
    }

    #[test]
    fn an_unreachable_node_does_not_by_itself_hold_the_gate() {
        // node3 did not answer /status, so it contributes no view. The nodes
        // that did answer are converged, so the gate does not hold on their
        // account -- the master-count sum is what judges the missing node.
        // The counts deliberately sum to 2731 (not 4096): this shape can only
        // arise with a genuinely missing third node, so if unreachable nodes
        // ever started synthesizing zero-views, clause (a) would fire and
        // this assertion would catch it.
        let views = vec![view(1, 1366, 1366), view(2, 1365, 1365)];
        assert_eq!(serving_sum(&views), 2731);
        assert_eq!(shard_activation_gate_reason(&views, 3), None);
    }

    #[test]
    fn no_reachable_node_holds_the_gate() {
        let reason = shard_activation_gate_reason(&[], 3).expect("zero views must hold the gate");
        assert_eq!(reason, "no node answered /status");
    }

    #[test]
    fn a_single_node_cluster_is_exempt_from_the_target_floor() {
        // node_count == 1 covers the non-clustered /status shape, which
        // reports zeros for every shard count; the floor would deadlock it.
        assert_eq!(shard_activation_gate_reason(&[view(1, 0, 0)], 1), None);
        // The floor still applies the moment more than one node is expected.
        assert!(shard_activation_gate_reason(&[view(1, 0, 0)], 2).is_some());
    }

    #[test]
    fn a_drained_node_that_still_targets_shards_holds_the_gate() {
        // A quiesced node has handed its shards away but its activated table
        // still targets them: serving 0 != target 1365.
        let views = vec![view(1, 2048, 1366), view(2, 2048, 1365), view(3, 0, 1365)];
        let reason =
            shard_activation_gate_reason(&views, 3).expect("a mid-drain node holds the gate");
        assert!(
            reason.contains("node1 serves 2048 master shards but its activated table targets 1366"),
            "reason was: {reason}"
        );
    }
}

#[cfg(test)]
mod rf2_census_tests {
    use super::*;

    #[test]
    fn a_clean_census_settles_immediately() {
        assert_eq!(
            rf2_census_verdict(0, 0, Duration::ZERO, RF2_CENSUS_WINDOW),
            Rf2CensusVerdict::Settled
        );
    }

    #[test]
    fn over_replication_inside_the_window_retries() {
        // 16 records still on 3 holders 0.4s after migration counters hit
        // zero: the detached orphan sweep is mid-flight, not broken.
        assert_eq!(
            rf2_census_verdict(16, 0, Duration::from_millis(400), RF2_CENSUS_WINDOW),
            Rf2CensusVerdict::Retry
        );
    }

    #[test]
    fn under_replication_inside_the_window_retries() {
        // Under-replication converges via repair, so it gets the same window.
        assert_eq!(
            rf2_census_verdict(5, 0, Duration::from_secs(14), RF2_CENSUS_WINDOW),
            Rf2CensusVerdict::Retry
        );
    }

    #[test]
    fn payload_mismatches_inside_the_window_retry() {
        assert_eq!(
            rf2_census_verdict(0, 3, Duration::from_secs(1), RF2_CENSUS_WINDOW),
            Rf2CensusVerdict::Retry
        );
    }

    #[test]
    fn violations_surviving_the_window_fail() {
        assert_eq!(
            rf2_census_verdict(16, 0, RF2_CENSUS_WINDOW, RF2_CENSUS_WINDOW),
            Rf2CensusVerdict::Fail
        );
        assert_eq!(
            rf2_census_verdict(
                0,
                1,
                RF2_CENSUS_WINDOW + Duration::from_secs(1),
                RF2_CENSUS_WINDOW
            ),
            Rf2CensusVerdict::Fail
        );
    }

    #[test]
    fn a_census_that_converges_at_the_deadline_still_settles() {
        // Exact equality is preserved: it is evaluated at the deadline, and
        // a clean census there is a pass, not a failure.
        assert_eq!(
            rf2_census_verdict(0, 0, RF2_CENSUS_WINDOW, RF2_CENSUS_WINDOW),
            Rf2CensusVerdict::Settled
        );
    }
}

#[cfg(test)]
mod replication_report_tests {
    use super::*;

    #[test]
    fn replication_report_records_holder_distribution_examples() {
        let txid = [0xab; 32];
        let mut report = ReplicationCheckReport::new(3);

        report.record_holder_error(&txid, &[0, 1, 2]);
        report.record_holder_error(&txid, &[1]);

        assert_eq!(report.holder_errors, 2);
        assert_eq!(report.holder_count_histogram[1], 1);
        assert_eq!(report.holder_count_histogram[3], 1);
        assert!(report.holder_diagnostics().contains("1:1"));
        assert!(report.holder_diagnostics().contains("3:1"));
        assert!(report.holder_diagnostics().contains("abababababababab"));
    }

    #[test]
    fn txid_prefix_uses_first_eight_bytes() {
        let mut txid = [0u8; 32];
        txid[..9].copy_from_slice(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xff]);

        assert_eq!(txid_prefix(&txid), "0123456789abcdef");
    }

    #[test]
    fn parse_batch_response_exact_rejects_truncated_item() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&2u32.to_le_bytes());
        payload.push(0);
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.push(7);

        assert!(parse_batch_response_exact(&payload, Some(2)).is_none());
        assert!(parse_batch_response_exact(&payload, Some(1)).is_none());
    }

    /// Encoding two txids into an `OP_ADMIN_DIAGNOSE_KEY` request and
    /// decoding a synthetic response round-trips every field. This is
    /// pure-helper coverage — server behavior is asserted by
    /// `dispatch_admin_diagnose_key_returns_per_txid_state` in the
    /// teraslab crate.
    #[test]
    fn encode_decode_admin_diagnose_key_round_trip() {
        use teraslab::cluster::migration::KeyDiagnosis;
        use teraslab::protocol::opcodes::KEY_DIAGNOSIS_ENCODED_SIZE;

        let txid_a = [0xAAu8; 32];
        let txid_b = [0x42u8; 32];
        let req = encode_admin_diagnose_key(&[txid_a, txid_b]);
        // 4 bytes count + 2 * 32 bytes txids
        assert_eq!(req.len(), 4 + 64);
        assert_eq!(u32::from_le_bytes(req[0..4].try_into().unwrap()), 2);
        assert_eq!(&req[4..36], &txid_a);
        assert_eq!(&req[36..68], &txid_b);

        let entries = vec![
            KeyDiagnosis {
                shard: 5,
                this_node_id: 7,
                local_view_canonical_master_id: 7,
                has_local_data: true,
                is_local_master_of_shard: true,
                has_pending_inbound: false,
                is_shard_fenced: true,
                is_migrating_shard: false,
                topology_epoch: 42,
                local_view_effective_master_id: 7,
                is_serving_fenced: false,
            },
            KeyDiagnosis {
                shard: 4095,
                this_node_id: 7,
                local_view_canonical_master_id: 9,
                has_local_data: false,
                is_local_master_of_shard: false,
                has_pending_inbound: true,
                is_shard_fenced: false,
                is_migrating_shard: true,
                topology_epoch: 42,
                // Serving-vs-target split: the pre-handoff owner (3) still
                // serves while the target assignment names 9, behind an
                // up serving fence.
                local_view_effective_master_id: 3,
                is_serving_fenced: true,
            },
        ];

        // Build a synthetic STATUS_OK body using the documented layout.
        let mut body = Vec::with_capacity(4 + 2 * KEY_DIAGNOSIS_ENCODED_SIZE);
        body.extend_from_slice(&2u32.to_le_bytes());
        for d in &entries {
            body.extend_from_slice(&d.shard.to_le_bytes());
            body.extend_from_slice(&d.this_node_id.to_le_bytes());
            body.extend_from_slice(&d.local_view_canonical_master_id.to_le_bytes());
            body.push(u8::from(d.has_local_data));
            body.push(u8::from(d.is_local_master_of_shard));
            body.push(u8::from(d.has_pending_inbound));
            body.push(u8::from(d.is_shard_fenced));
            body.push(u8::from(d.is_migrating_shard));
            body.extend_from_slice(&d.topology_epoch.to_le_bytes());
            body.extend_from_slice(&d.local_view_effective_master_id.to_le_bytes());
            body.push(u8::from(d.is_serving_fenced));
        }
        let decoded = decode_admin_diagnose_key(&body).unwrap();
        assert_eq!(decoded, entries);

        // Truncated body → error (claim 2 entries, supply 1).
        let mut bad = Vec::new();
        bad.extend_from_slice(&2u32.to_le_bytes());
        bad.extend_from_slice(&body[4..4 + KEY_DIAGNOSIS_ENCODED_SIZE]);
        assert!(decode_admin_diagnose_key(&bad).is_err());
    }

    #[test]
    fn parse_batch_response_exact_requires_expected_count() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.push(0);
        payload.extend_from_slice(&3u32.to_le_bytes());
        payload.extend_from_slice(&[1, 2, 3]);

        let parsed = parse_batch_response_exact(&payload, Some(1)).unwrap();
        assert_eq!(parsed, vec![(0, vec![1, 2, 3])]);
        assert!(parse_batch_response_exact(&payload, Some(2)).is_none());
    }

    /// Bundled inputs for `diag()`, kept compact to satisfy
    /// `clippy::too_many_arguments` while staying readable in tests.
    struct DiagSpec {
        shard: u16,
        this_node_id: u64,
        master_id: u64,
        has_local_data: bool,
        is_local_master: bool,
        has_pending_inbound: bool,
        is_fenced: bool,
        is_migrating: bool,
        epoch: u64,
    }

    /// Build a `KeyDiagnosis` from a `DiagSpec`. All fields are
    /// explicit so each test can focus on the dimensions it cares
    /// about.
    fn diag(s: DiagSpec) -> teraslab::cluster::migration::KeyDiagnosis {
        teraslab::cluster::migration::KeyDiagnosis {
            shard: s.shard,
            this_node_id: s.this_node_id,
            local_view_canonical_master_id: s.master_id,
            has_local_data: s.has_local_data,
            is_local_master_of_shard: s.is_local_master,
            has_pending_inbound: s.has_pending_inbound,
            is_shard_fenced: s.is_fenced,
            is_migrating_shard: s.is_migrating,
            topology_epoch: s.epoch,
            // These format-oriented tests exercise the tracker/routing
            // columns; the F7 serving-side fields default to "no split".
            local_view_effective_master_id: s.master_id,
            is_serving_fenced: false,
        }
    }

    /// Two failing txids surveyed across three nodes (1, 2, 3): node 1
    /// is the master of shard 7 for txid_a; only node 1 currently
    /// holds the data; node 2 has it inbound; node 3 is fenced; and
    /// txid_b is in a different state with a different shard. The
    /// dump must surface every one of those columns per-node.
    #[test]
    fn format_master_failed_diagnostic_includes_per_node_state() {
        let txid_a = [0xAAu8; 32];
        let txid_b = [0x42u8; 32];
        let node_nums = vec![1u32, 2, 3];

        // For txid_a (shard 7): node 1 is master AND holder; node 2
        // has it pending inbound; node 3 is fenced and on an older
        // epoch. For txid_b (shard 9): node 2 is master AND holder.
        let n1 = vec![
            diag(DiagSpec {
                shard: 7,
                this_node_id: 1,
                master_id: 1,
                has_local_data: true,
                is_local_master: true,
                has_pending_inbound: false,
                is_fenced: false,
                is_migrating: true,
                epoch: 42,
            }),
            diag(DiagSpec {
                shard: 9,
                this_node_id: 1,
                master_id: 2,
                has_local_data: false,
                is_local_master: false,
                has_pending_inbound: false,
                is_fenced: false,
                is_migrating: false,
                epoch: 42,
            }),
        ];
        let n2 = vec![
            diag(DiagSpec {
                shard: 7,
                this_node_id: 2,
                master_id: 1,
                has_local_data: false,
                is_local_master: false,
                has_pending_inbound: true,
                is_fenced: false,
                is_migrating: false,
                epoch: 42,
            }),
            diag(DiagSpec {
                shard: 9,
                this_node_id: 2,
                master_id: 2,
                has_local_data: true,
                is_local_master: true,
                has_pending_inbound: false,
                is_fenced: false,
                is_migrating: false,
                epoch: 42,
            }),
        ];
        let n3 = vec![
            diag(DiagSpec {
                shard: 7,
                this_node_id: 3,
                master_id: 1,
                has_local_data: false,
                is_local_master: false,
                has_pending_inbound: false,
                is_fenced: true,
                is_migrating: false,
                epoch: 41,
            }),
            diag(DiagSpec {
                shard: 9,
                this_node_id: 3,
                master_id: 2,
                has_local_data: false,
                is_local_master: false,
                has_pending_inbound: true,
                is_fenced: false,
                is_migrating: false,
                epoch: 41,
            }),
        ];
        let responses = vec![Ok(n1), Ok(n2), Ok(n3)];

        let dump = format_master_failed_diagnostic(&[txid_a, txid_b], &node_nums, &responses);

        // One line per failing txid, each prefixed with `\n  `.
        assert!(
            dump.starts_with("\n  "),
            "dump should start with newline+indent: {dump:?}"
        );
        let lines: Vec<&str> = dump.split("\n  ").filter(|s| !s.is_empty()).collect();
        assert_eq!(lines.len(), 2, "expected 2 lines, got: {dump}");

        // Both txid prefixes appear.
        assert!(
            dump.contains("txid=aaaaaaaaaaaa"),
            "missing txid_a prefix: {dump}"
        );
        assert!(
            dump.contains("txid=424242424242"),
            "missing txid_b prefix: {dump}"
        );

        // Shard rendered for each (no mismatch in this case).
        assert!(
            lines[0].contains("shard=7"),
            "missing shard=7 on line 0: {}",
            lines[0]
        );
        assert!(
            lines[1].contains("shard=9"),
            "missing shard=9 on line 1: {}",
            lines[1]
        );
        assert!(
            !dump.contains("SHARD_MISMATCH"),
            "unexpected SHARD_MISMATCH: {dump}"
        );

        // For txid_a, node 1 is master, nodes 2/3 are not.
        assert!(
            lines[0].contains("masters_per_node=[n1:Y, n2:N, n3:N]"),
            "wrong masters row: {}",
            lines[0]
        );
        // For txid_a, only n1 holds data.
        assert!(
            lines[0].contains("holders=[n1:Y, n2:N, n3:N]"),
            "wrong holders row: {}",
            lines[0]
        );
        // n2 has inbound for txid_a.
        assert!(
            lines[0].contains("inbound=[n1:N, n2:Y, n3:N]"),
            "wrong inbound row: {}",
            lines[0]
        );
        // n3 is fenced for txid_a.
        assert!(
            lines[0].contains("fenced=[n1:N, n2:N, n3:Y]"),
            "wrong fenced row: {}",
            lines[0]
        );
        // n1 is migrating shard for txid_a.
        assert!(
            lines[0].contains("migrating=[n1:Y, n2:N, n3:N]"),
            "wrong migrating row: {}",
            lines[0]
        );
        // topo_epoch carries each node's number.
        assert!(
            lines[0].contains("topo_epoch=[n1:42, n2:42, n3:41]"),
            "wrong topo_epoch row: {}",
            lines[0]
        );

        // Sanity: txid_b's master shifted to n2.
        assert!(
            lines[1].contains("masters_per_node=[n1:N, n2:Y, n3:N]"),
            "wrong masters row for txid_b: {}",
            lines[1]
        );
    }

    /// One node returns an admin-call error; the dump must surface
    /// `n2:ERR(connect refused)` in EVERY column for that node and
    /// not abort the whole dump.
    #[test]
    fn format_master_failed_diagnostic_handles_node_error() {
        let txid_a = [0x11u8; 32];
        let node_nums = vec![1u32, 2, 3];
        let n1 = vec![diag(DiagSpec {
            shard: 3,
            this_node_id: 1,
            master_id: 1,
            has_local_data: true,
            is_local_master: true,
            has_pending_inbound: false,
            is_fenced: false,
            is_migrating: false,
            epoch: 5,
        })];
        let n3 = vec![diag(DiagSpec {
            shard: 3,
            this_node_id: 3,
            master_id: 1,
            has_local_data: false,
            is_local_master: false,
            has_pending_inbound: false,
            is_fenced: false,
            is_migrating: false,
            epoch: 5,
        })];
        let responses: Vec<Result<Vec<teraslab::cluster::migration::KeyDiagnosis>, String>> =
            vec![Ok(n1), Err("connect refused".to_string()), Ok(n3)];

        let dump = format_master_failed_diagnostic(&[txid_a], &node_nums, &responses);

        // Single failing line.
        let lines: Vec<&str> = dump.split("\n  ").filter(|s| !s.is_empty()).collect();
        assert_eq!(lines.len(), 1, "expected 1 line, got: {dump}");
        let line = lines[0];

        // n2 must show ERR(connect refused) in every per-node column.
        // We verify each column substring directly so a regression in
        // any single column is pinpointed.
        let needle = "n2:ERR(connect refused)";
        for col in [
            "masters_per_node",
            "holders",
            "inbound",
            "fenced",
            "migrating",
            "topo_epoch",
        ] {
            // Look for the column name immediately followed (eventually)
            // by an ERR cell for n2.
            assert!(line.contains(col), "column {col} missing in line: {line}",);
            assert!(
                line.contains(needle),
                "column {col} missing n2 error in line: {line}",
            );
        }

        // n1 and n3 still rendered with their booleans.
        assert!(line.contains("n1:Y"), "missing n1:Y data in line: {line}");
        assert!(line.contains("n3:N"), "missing n3:N data in line: {line}");
        // Shard known via at least one healthy node.
        assert!(line.contains("shard=3"), "missing shard=3 in line: {line}");
    }

    /// Two nodes disagree on the shard for a given txid (e.g. one is
    /// pre-rebalance, one is post). The line must be flagged with
    /// `SHARD_MISMATCH` so triage spots topology divergence.
    #[test]
    fn format_master_failed_diagnostic_flags_shard_mismatch() {
        let txid_a = [0x77u8; 32];
        let node_nums = vec![1u32, 2];
        let n1 = vec![diag(DiagSpec {
            shard: 7,
            this_node_id: 1,
            master_id: 1,
            has_local_data: true,
            is_local_master: true,
            has_pending_inbound: false,
            is_fenced: false,
            is_migrating: false,
            epoch: 10,
        })];
        let n2 = vec![diag(DiagSpec {
            shard: 8,
            this_node_id: 2,
            master_id: 2,
            has_local_data: true,
            is_local_master: true,
            has_pending_inbound: false,
            is_fenced: false,
            is_migrating: false,
            epoch: 11,
        })];
        let responses = vec![Ok(n1), Ok(n2)];

        let dump = format_master_failed_diagnostic(&[txid_a], &node_nums, &responses);

        assert!(
            dump.contains("SHARD_MISMATCH"),
            "expected SHARD_MISMATCH flag: {dump}"
        );
        // The first successful node's shard should be reported.
        assert!(
            dump.contains("shard=7"),
            "expected shard=7 (first response): {dump}"
        );
    }
}

#[cfg(test)]
mod seed_records_accounting_tests {
    use super::*;
    use teraslab_test_client::PartialError;
    use teraslab_test_client::types::BatchItemError;

    /// Build a minimal create item for the given txid.
    fn item(txid: [u8; 32]) -> CreateItem {
        CreateItem {
            txid,
            utxo_hashes: vec![[1u8; 32]],
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

    fn partial_error_with(failed_indices: &[u32], degraded: bool) -> ClientError {
        ClientError::Partial(PartialError {
            successes: vec![],
            errors: failed_indices
                .iter()
                .map(|&item_index| BatchItemError {
                    item_index,
                    // ERR_REPLICATION_FAILED -- the ambiguous outcome a
                    // partition produces for shards whose holder pair is cut.
                    code: 20,
                    data: vec![],
                })
                .collect(),
            degraded,
        })
    }

    fn partial_error(failed_indices: &[u32]) -> ClientError {
        partial_error_with(failed_indices, false)
    }

    /// Scripted [`SeedBatchDriver`] for the loop-composition tests: replays a
    /// canned sequence of `create_batch` outcomes with no Docker cluster in
    /// sight, and can block forever in `backoff` so a caller's timeout
    /// cancels the loop exactly where the real one would be sleeping.
    struct ScriptedDriver {
        /// Outcomes for successive `send_batch` calls, in order. Once the
        /// script runs out every further send reports a connection error.
        sends: std::collections::VecDeque<Result<(), ClientError>>,
        /// Txids the simulated reconcile read-back finds durably present.
        reconcilable: std::collections::HashSet<[u8; 32]>,
        /// Retry-set size observed by each `send_batch` call.
        sent_sizes: Vec<usize>,
        /// When set, `backoff` never returns.
        block_in_backoff: bool,
    }

    impl ScriptedDriver {
        fn new(sends: Vec<Result<(), ClientError>>) -> Self {
            Self {
                sends: sends.into(),
                reconcilable: std::collections::HashSet::new(),
                sent_sizes: Vec::new(),
                block_in_backoff: false,
            }
        }
    }

    impl SeedBatchDriver for ScriptedDriver {
        async fn send_batch(&mut self, items: &[CreateItem]) -> Result<(), ClientError> {
            self.sent_sizes.push(items.len());
            self.sends
                .pop_front()
                .unwrap_or_else(|| Err(ClientError::Connection("script exhausted".to_string())))
        }

        async fn reconcile(
            &mut self,
            remaining_items: &mut Vec<CreateItem>,
            remaining_meta: &mut Vec<SeedMeta>,
            succeeded_meta: &mut Vec<SeedMeta>,
        ) -> usize {
            let items = std::mem::take(remaining_items);
            let meta = std::mem::take(remaining_meta);
            let mut reconciled = 0usize;
            for (item, m) in items.into_iter().zip(meta) {
                if self.reconcilable.contains(&m.0) {
                    succeeded_meta.push(m);
                    reconciled += 1;
                } else {
                    remaining_items.push(item);
                    remaining_meta.push(m);
                }
            }
            reconciled
        }

        async fn backoff(&mut self, _attempt: u32) {
            if self.block_in_backoff {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Two items, `txids[0]` and `txids[1]`, ready to hand to
    /// [`seed_one_batch`].
    fn two_item_batch() -> (Vec<[u8; 32]>, Vec<CreateItem>, Vec<SeedMeta>) {
        let txids: Vec<[u8; 32]> = (0u8..2).map(|i| [i; 32]).collect();
        let items = txids.iter().map(|t| item(*t)).collect();
        let meta = txids.iter().map(|t| (*t, vec![[1u8; 32]])).collect();
        (txids, items, meta)
    }

    const MAX_SEED_RETRIES: u32 = teraslab_test_client::retry::MAX_TRANSIENT_ATTEMPTS;

    #[test]
    fn split_partial_successes_moves_only_acked_items_out_of_the_retry_set() {
        let txids: Vec<[u8; 32]> = (0u8..3).map(|i| [i; 32]).collect();
        let mut remaining_items: Vec<CreateItem> = txids.iter().map(|t| item(*t)).collect();
        let mut remaining_meta: Vec<SeedMeta> =
            txids.iter().map(|t| (*t, vec![[1u8; 32]])).collect();
        let mut succeeded_meta: Vec<SeedMeta> = Vec::new();

        // Item 1 failed; items 0 and 2 were applied by the server.
        split_partial_successes(
            &partial_error(&[1]),
            0,
            &mut remaining_items,
            &mut remaining_meta,
            &mut succeeded_meta,
        );

        assert_eq!(remaining_items.len(), 1);
        assert_eq!(remaining_items[0].txid, txids[1]);
        assert_eq!(remaining_meta.len(), 1);
        assert_eq!(remaining_meta[0].0, txids[1]);
        assert_eq!(
            succeeded_meta.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
            vec![txids[0], txids[2]]
        );
    }

    /// `degraded = true` means the items that DID apply are only single-node
    /// durable and may be lost (`client/rust/src/errors.rs`). Crediting them
    /// would leak a below-quorum write into `assert_rf2_replication_exact`
    /// and the acked-write-durability-after-SIGKILL check, which then fail
    /// for a reason the harness invented.
    #[test]
    fn split_partial_successes_credits_nothing_when_the_partial_is_degraded() {
        let txids: Vec<[u8; 32]> = (0u8..3).map(|i| [i; 32]).collect();
        let mut remaining_items: Vec<CreateItem> = txids.iter().map(|t| item(*t)).collect();
        let mut remaining_meta: Vec<SeedMeta> =
            txids.iter().map(|t| (*t, vec![[1u8; 32]])).collect();
        let mut succeeded_meta: Vec<SeedMeta> = Vec::new();

        // Item 1 failed; items 0 and 2 applied -- but below quorum.
        split_partial_successes(
            &partial_error_with(&[1], true),
            0,
            &mut remaining_items,
            &mut remaining_meta,
            &mut succeeded_meta,
        );

        assert!(
            succeeded_meta.is_empty(),
            "a degraded partial must not credit its applied items"
        );
        assert_eq!(
            remaining_items.len(),
            3,
            "every item stays in the retry set so reconcile can confirm it"
        );
        assert_eq!(
            remaining_meta.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
            txids
        );
    }

    /// Credit here is by COMPLEMENT of the reported failures, and the
    /// client's sub-batch -> batch index remap leaves an out-of-range
    /// `item_index` UNMAPPED. One malformed index would therefore turn a
    /// FAILED item into a credited phantom record that a later hard
    /// assertion demands to exist.
    #[test]
    fn split_partial_successes_credits_nothing_for_an_out_of_range_item_index() {
        let txids: Vec<[u8; 32]> = (0u8..3).map(|i| [i; 32]).collect();
        let mut remaining_items: Vec<CreateItem> = txids.iter().map(|t| item(*t)).collect();
        let mut remaining_meta: Vec<SeedMeta> =
            txids.iter().map(|t| (*t, vec![[1u8; 32]])).collect();
        let mut succeeded_meta: Vec<SeedMeta> = Vec::new();

        // Index 7 cannot refer to a 3-item batch.
        split_partial_successes(
            &partial_error(&[1, 7]),
            0,
            &mut remaining_items,
            &mut remaining_meta,
            &mut succeeded_meta,
        );

        assert!(
            succeeded_meta.is_empty(),
            "an out-of-range index makes the whole index set untrustworthy"
        );
        assert_eq!(remaining_items.len(), 3);
        assert_eq!(
            remaining_meta.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
            txids
        );
    }

    #[test]
    fn split_partial_successes_leaves_the_batch_intact_for_a_non_partial_error() {
        let txids: Vec<[u8; 32]> = (0u8..2).map(|i| [i; 32]).collect();
        let mut remaining_items: Vec<CreateItem> = txids.iter().map(|t| item(*t)).collect();
        let mut remaining_meta: Vec<SeedMeta> =
            txids.iter().map(|t| (*t, vec![[1u8; 32]])).collect();
        let mut succeeded_meta: Vec<SeedMeta> = Vec::new();

        split_partial_successes(
            &ClientError::Connection("peer unreachable".to_string()),
            2,
            &mut remaining_items,
            &mut remaining_meta,
            &mut succeeded_meta,
        );

        // Nothing is known to have landed: the whole batch stays retryable.
        assert_eq!(remaining_items.len(), 2);
        assert_eq!(remaining_meta.len(), 2);
        assert!(succeeded_meta.is_empty());
    }

    #[test]
    fn publish_seeded_records_drains_into_the_verifier_and_the_txid_list() {
        let verifier = StateVerifier::new();
        let mut txids: Vec<[u8; 32]> = Vec::new();
        let hashes_a = vec![[0xaa; 32], [0xab; 32]];
        let hashes_b = vec![[0xba; 32], [0xbb; 32]];
        let mut succeeded_meta: Vec<SeedMeta> =
            vec![([7u8; 32], hashes_a.clone()), ([8u8; 32], hashes_b)];

        publish_seeded_records(&verifier, 2, &mut succeeded_meta, &mut txids);

        assert!(
            succeeded_meta.is_empty(),
            "staging buffer must be drained so a later publish cannot double-record"
        );
        assert_eq!(txids, vec![[7u8; 32], [8u8; 32]]);
        assert_eq!(verifier.record_count(), 2);
        let rec = verifier
            .get_record(&[7u8; 32])
            .expect("published record must be tracked by the verifier");
        assert_eq!(rec.utxo_count, 2);
        assert_eq!(rec.utxo_hashes, hashes_a);
        assert_eq!(rec.spent_utxos, 0);
    }

    /// The cancellation shape the harness actually hits: a caller wraps
    /// `seed_records` in a timeout, the future is dropped mid-flight, and the
    /// return value is lost. Anything published before the drop must survive
    /// in the verifier -- that is the only channel a cancelled caller has.
    #[tokio::test]
    async fn records_published_before_a_cancellation_survive_in_the_verifier() {
        let verifier = StateVerifier::new();
        let mut txids: Vec<[u8; 32]> = Vec::new();
        let mut succeeded_meta: Vec<SeedMeta> = vec![([9u8; 32], vec![[0xcc; 32]])];

        let cancelled = tokio::time::timeout(Duration::from_millis(50), async {
            publish_seeded_records(&verifier, 1, &mut succeeded_meta, &mut txids);
            // Stand in for the awaits that follow a publish in `seed_records`
            // (reconcile read-back, backoff sleep, routing refresh).
            tokio::time::sleep(Duration::from_secs(30)).await;
            txids.clone()
        })
        .await;

        assert!(
            cancelled.is_err(),
            "the future must be cancelled mid-flight"
        );
        assert_eq!(
            verifier.record_count(),
            1,
            "an ack published before the cancellation point must not be lost"
        );
        assert!(verifier.all_txids().contains(&[9u8; 32]));
    }

    // ---------------------------------------------------------------------
    // Loop-composition tests. These drive the REAL `seed_one_batch` loop
    // through the `SeedBatchDriver` seam, so they fail when the split or a
    // publish is moved to the wrong place -- which the helper-level tests
    // above cannot detect.
    // ---------------------------------------------------------------------

    /// The final attempt's response is still a response: items it
    /// acknowledged were created by the cluster and must be credited, even
    /// though the batch as a whole gives up and returns an error. Fails if
    /// the split is ever re-gated behind `attempt + 1 < MAX_SEED_RETRIES`.
    #[tokio::test]
    async fn final_attempt_partial_still_publishes_the_items_that_landed() {
        let verifier = StateVerifier::new();
        let mut txids_out: Vec<[u8; 32]> = Vec::new();
        let (txids, items, meta) = two_item_batch();

        // Every attempt but the last fails opaquely (nothing creditable);
        // the last one comes back partial with item 1 failed.
        let mut sends: Vec<Result<(), ClientError>> = (0..MAX_SEED_RETRIES - 1)
            .map(|_| Err(ClientError::Connection("peer unreachable".to_string())))
            .collect();
        sends.push(Err(partial_error(&[1])));
        let mut driver = ScriptedDriver::new(sends);

        let result = seed_one_batch(&mut driver, &verifier, 1, items, meta, &mut txids_out).await;

        assert!(
            matches!(&result, Err(ClientError::Partial(pe)) if pe.errors.len() == 1),
            "the batch must still report the failure it gave up on: {result:?}"
        );
        assert_eq!(
            txids_out,
            vec![txids[0]],
            "the item the final response acknowledged must be credited"
        );
        assert_eq!(verifier.record_count(), 1);
        assert_eq!(driver.sent_sizes.len() as u32, MAX_SEED_RETRIES);
    }

    /// A degraded partial acknowledges items at below-quorum durability. The
    /// loop must credit none of them and keep re-sending, so the record only
    /// ever enters the verifier via a non-degraded ack or reconcile.
    #[tokio::test]
    async fn degraded_partial_never_credits_through_the_loop() {
        let verifier = StateVerifier::new();
        let mut txids_out: Vec<[u8; 32]> = Vec::new();
        let (_txids, items, meta) = two_item_batch();

        let sends: Vec<Result<(), ClientError>> = (0..MAX_SEED_RETRIES)
            .map(|_| Err(partial_error_with(&[1], true)))
            .collect();
        let mut driver = ScriptedDriver::new(sends);

        let result = seed_one_batch(&mut driver, &verifier, 1, items, meta, &mut txids_out).await;

        assert!(
            matches!(&result, Err(ClientError::Partial(pe)) if pe.degraded),
            "the degraded partial must surface as the batch error: {result:?}"
        );
        assert_eq!(verifier.record_count(), 0, "no degraded item may be seeded");
        assert!(txids_out.is_empty());
        assert!(
            driver.sent_sizes.iter().all(|n| *n == 2),
            "the retry set must never shrink on a degraded ack: {:?}",
            driver.sent_sizes
        );
    }

    /// The cancellation shape 8d.2 actually hits: the caller wraps the seed
    /// in a timeout and the future is dropped while the loop is sleeping out
    /// its retry backoff. Whatever the response before that sleep
    /// acknowledged must already be in the verifier. Fails if the publish is
    /// moved back to the end of the batch.
    #[tokio::test]
    async fn acked_items_survive_a_cancellation_during_the_retry_backoff() {
        let verifier = StateVerifier::new();
        let mut txids_out: Vec<[u8; 32]> = Vec::new();
        let (txids, items, meta) = two_item_batch();

        let mut driver = ScriptedDriver::new(vec![Err(partial_error(&[1]))]);
        // Item 1 stays unacknowledged, so the loop reaches the backoff --
        // where it now hangs until the caller's timeout drops it.
        driver.block_in_backoff = true;

        let cancelled = tokio::time::timeout(
            Duration::from_millis(50),
            seed_one_batch(&mut driver, &verifier, 1, items, meta, &mut txids_out),
        )
        .await;

        assert!(
            cancelled.is_err(),
            "the future must be cancelled mid-flight"
        );
        assert_eq!(
            txids_out,
            vec![txids[0]],
            "the acked item must survive the dropped future"
        );
        assert_eq!(verifier.record_count(), 1);
    }

    /// Reconcile promotes ambiguous items too, and the very next thing the
    /// loop does is sleep. Fails if the publish between reconcile and the
    /// backoff is removed.
    #[tokio::test]
    async fn reconciled_items_are_published_before_the_backoff_sleep() {
        let verifier = StateVerifier::new();
        let mut txids_out: Vec<[u8; 32]> = Vec::new();
        let (txids, items, meta) = two_item_batch();

        // Opaque failure: nothing creditable from the response itself.
        let mut driver =
            ScriptedDriver::new(vec![Err(ClientError::Connection("ambiguous".to_string()))]);
        // The read-back finds item 0 durably present; item 1 is really gone,
        // so the loop still has work and reaches the (blocking) backoff.
        driver.reconcilable.insert(txids[0]);
        driver.block_in_backoff = true;

        let cancelled = tokio::time::timeout(
            Duration::from_millis(50),
            seed_one_batch(&mut driver, &verifier, 1, items, meta, &mut txids_out),
        )
        .await;

        assert!(
            cancelled.is_err(),
            "the future must be cancelled mid-flight"
        );
        assert_eq!(
            txids_out,
            vec![txids[0]],
            "a reconciled item must be published before the backoff sleep"
        );
        assert_eq!(verifier.record_count(), 1);
    }
}
