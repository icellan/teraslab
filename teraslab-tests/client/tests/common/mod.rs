//! Shared setup/teardown for Docker cluster test scenarios.

use std::collections::BTreeMap;
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
use teraslab_test_client::types::{
    BatchItemError, CreateItem, FIELD_ALL, FIELD_ALL_METADATA, SpendBatchParams, SpendItem,
};
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

/// Fetch a plain-text endpoint (`/metrics` is Prometheus text, not JSON) with
/// the same bounded, admin-authenticated client [`poll_json`] uses.
///
/// An empty body is reported as an error rather than as a successful scrape:
/// a 0-byte metrics file is indistinguishable from a scrape that never ran,
/// and that ambiguity has already burned one nightly's worth of archived
/// metrics (see the port-mapping note in `teraslab-tests/scripts/collect_logs.sh`).
async fn poll_text(url: &str) -> Result<String, ClientError> {
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
    let body = resp
        .text()
        .await
        .map_err(|e| ClientError::Connection(format!("GET {url} body read failed: {e}")))?;
    if body.is_empty() {
        return Err(ClientError::Connection(format!(
            "GET {url} returned an empty body"
        )));
    }
    Ok(body)
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
/// W16 — wait until ONE node's HTTP listener answers, and return how long that
/// took.
///
/// This is the earliest instant a restarting node is observable at all, and it
/// is a real phase boundary rather than a convenient one: `src/bin/server.rs`
/// starts the HTTP listener only AFTER synchronous recovery — redo replay,
/// mined-index recovery, DAH rebuild, tombstone replay — and flips the
/// readiness flag immediately before spawning it. So a `/status` that answers
/// means "this process finished recovering", and a `/status` that does not is
/// "still booting", NOT "wedged".
///
/// Separating the two is what stops a recovery-time budget from being smuggled
/// into a membership SLA: how long recovery takes scales with how much redo the
/// previous phase happened to generate, while how long membership takes after
/// that does not. Scenario 05 asserted the sum and failed in armed CI
/// (run 32644353574) on a node2 that was booting normally — zero ERROR lines,
/// no `FormatError`, no CRC-zero signature — but had 3375 redo entries to
/// replay first.
pub async fn wait_node_http_ready(
    docker: &DockerHelpers,
    node_num: u32,
    timeout: Duration,
) -> Result<Duration, ClientError> {
    let start = std::time::Instant::now();
    let port = docker.http_port(node_num);
    let url = format!("http://127.0.0.1:{port}/status");
    loop {
        match poll_json(&url).await {
            Ok(_) => return Ok(start.elapsed()),
            // The deadline is checked HERE so the failing poll's own error can
            // be quoted without keeping it in a binding that is dead on the
            // success path.
            Err(e) if start.elapsed() >= timeout => {
                return Err(ClientError::Connection(format!(
                    "wait_node_http_ready: node{node_num} did not bind its HTTP listener \
                     within {timeout:?} (last: {e})"
                )));
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn wait_cluster_ready(
    docker: &DockerHelpers,
    node_count: u32,
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    let mut last_log = std::time::Instant::now();
    loop {
        // Collect every node's answer UNCONDITIONALLY: a survivor answering
        // with the WRONG cluster_size is otherwise rendered identically to a
        // node that never answered ("0/3 nodes ready (versions: [])"), which
        // is useless for diagnosing a wedged rolling restart.
        let mut ready = 0u32;
        let mut versions: Vec<u64> = Vec::new();
        let mut min_masters = u64::MAX;
        let mut node_states: Vec<String> = Vec::new();
        for i in 1..=node_count {
            let port = docker.http_port(i);
            let url = format!("http://127.0.0.1:{port}/status");
            match poll_json(&url).await {
                Ok(json) => {
                    let size = json["cluster_size"].as_u64();
                    let ver = json["shard_table_version"].as_u64();
                    let masters = json["master_shard_count"].as_u64();
                    if size == Some(node_count as u64) {
                        ready += 1;
                        if let Some(v) = ver {
                            versions.push(v);
                        }
                    }
                    if let Some(m) = masters {
                        min_masters = min_masters.min(m);
                    }
                    let fmt = |v: Option<u64>| {
                        v.map(|v| v.to_string()).unwrap_or_else(|| "?".to_string())
                    };
                    // Task #75 — a degraded answer omits size/masters; name
                    // the wedge instead of rendering a misleading "?" row.
                    if let Some(marker) = status_degraded_marker(&json) {
                        node_states.push(format!("node{i}:{marker}"));
                    } else {
                        node_states.push(format!(
                            "node{i}:size={},ver={},masters={}",
                            fmt(size),
                            fmt(ver),
                            fmt(masters)
                        ));
                    }
                }
                Err(_) => node_states.push(format!("node{i}:UNREACHABLE")),
            }
        }
        // All nodes must report correct cluster size AND agree on the
        // shard table version (topology term) AND every node must have
        // master shards assigned. This ensures the cluster has fully
        // converged and the shard table includes all nodes before tests
        // begin.
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
                "{ready}/{node_count} nodes ready ({}) after {timeout:?}",
                node_states.join(" ")
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

/// W12 TAIL 2 — the entries a node's `/admin/migration_status` reports as
/// TERMINALLY REFUSED by their own source and retained anyway as a fail-closed
/// fence: over local orphan records (a non-holder), or — W16 — over an
/// unproven local copy on a HOLDER whose source has refused the transfer for
/// `REFUSED_HOLDER_TERMINAL_ROUNDS` consecutive rounds.
///
/// Zero for a server that predates the field, which is the fail-closed
/// reading: an answer that cannot express the distinction is treated as
/// "everything is potentially in flight".
///
/// A non-zero count is NOT "fine". Every entry it counts is a shard still
/// FENCED — client-invisible on that node — that no transfer will ever
/// un-fence. It is discounted from [`in_flight_inbound_pending`] because THAT
/// question ("is a migration still running?") genuinely answers no; it is FATAL
/// to the convergence gates, which ask the other question. See
/// [`refused_residue_error`].
pub fn refused_retained_inbound(json: &serde_json::Value) -> u64 {
    json["inbound_refused_retained"].as_u64().unwrap_or(0)
}

/// W17 — the same census, SPLIT BY RETENTION CLASS.
///
/// The two classes are opposite states that happen to share a disposition, and
/// they promise different things about how long they may legitimately stand —
/// see [`RefusedResidueClass`]. The combined count above stays the answer to
/// "is a migration still running?"; this is the answer to "is anything stuck,
/// and stuck how?".
///
/// `inbound_refused_retained` is the AUTHORITY for how much residue exists;
/// only the ATTRIBUTION is read from the split. The orphan half is derived as
/// `total - holder_terminal`, never trusted from the wire, so:
///
/// * a server that reports the total but not the split (any build before W17)
///   has its whole residue attributed to the ORPHAN class;
/// * a split that does not add up — a partial rollout, a rename, a bug —
///   cannot shrink the residue. The remainder lands in the orphan class.
///
/// W17 review P2-5. Reading the two halves independently was FAIL-OPEN,
/// contrary to what this doc claimed: a server reporting `total=5` with both
/// halves zero produced a residue whose `total()` is ZERO, which satisfies the
/// gates' convergence condition and turns a wedged cluster into a GREEN
/// verdict. Deriving from the authority makes that unrepresentable. The
/// remainder taking the ORPHAN class is also the fail-safe direction for
/// TIMING — that is the longer of the two graces, so an unclassifiable residue
/// is never judged on the shorter clock, which is the wave-16 defect this split
/// exists to remove.
pub fn refused_residue_counts(json: &serde_json::Value) -> RefusedResidue {
    let total = refused_retained_inbound(json);
    if total == 0 {
        return RefusedResidue::default();
    }
    // Clamped to the total: the holder class is the one with the SHORT grace,
    // so an overstated half must never enlarge it.
    let holder_terminal = json["inbound_refused_retained_holder_terminal"]
        .as_u64()
        .unwrap_or(0)
        .min(total);
    RefusedResidue {
        holder_terminal,
        orphan: total - holder_terminal,
    }
}

/// W12 TAIL 2 — the inbound entries that can still make progress.
///
/// `inbound_pending` counts two opposite things. A plain pending entry is
/// waiting on data that a source is sending. A `refused_by_source` entry has
/// been answered `ERR_MIGRATION_NO_TASKS` by the only node that could ever
/// satisfy it and was RETAINED by `inbound_entry_must_be_kept` — because the
/// shard still has local records only the committed-handoff-gated orphan
/// cleanup may reclaim, or (W16) because this node is the shard's holder and
/// its copy is unproven. The second kind is a fixpoint: re-polling it for
/// 300 s produces the identical answer 300 s later (armed scenario 08 @
/// fc5e5f7: `dropped:0, kept:2` on all 29 refusal rounds; armed scenario 09 @
/// CI 32644353574: `shards: 9, dropped: 0` on eight consecutive sweeps).
///
/// "Is a migration still running?" must use this. It must NOT be read as "the
/// condition is fine": the refused count is a separate, FATAL gate condition
/// (see [`refused_residue_error`]), it is named in the per-node detail and in
/// the timeout message, and the server publishes it as the
/// `teraslab_migration_inbound_refused_retained` gauge.
///
/// The two must stay separate rather than being folded back into one number.
/// Counting a refused entry as in-flight is what wedged armed 09 for the whole
/// budget with no explanation; not counting it at all is what would have let a
/// run go green over nine permanently-fenced shards.
pub fn in_flight_inbound_pending(json: &serde_json::Value) -> u64 {
    let pending = json["inbound_pending"].as_u64().unwrap_or(0);
    pending.saturating_sub(refused_retained_inbound(json))
}

/// W17 — the per-class census of terminally-refused retained inbound entries a
/// node reports.
///
/// `refused_by_source` is set on two OPPOSITE states that happen to share a
/// disposition, and the gate below has to tell them apart. See
/// [`RefusedResidueClass`] for what each one costs to earn and how it clears.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RefusedResidue {
    /// Entries this node HOLDS whose source has refused them for
    /// `REFUSED_HOLDER_TERMINAL_ROUNDS` consecutive rounds.
    pub holder_terminal: u64,
    /// Entries for shards this node does NOT hold, retained only over local
    /// orphan records. Marked on the FIRST refusal.
    pub orphan: u64,
}

impl RefusedResidue {
    /// Every retained entry, of either class — what `inbound_refused_retained`
    /// reports and what [`in_flight_inbound_pending`] subtracts.
    pub fn total(self) -> u64 {
        self.holder_terminal.saturating_add(self.orphan)
    }
}

/// W17 — which retention class a residue belongs to, and therefore which grace
/// it is judged by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusedResidueClass {
    /// This node is the shard's HOLDER and its copy is unproven. Nothing may
    /// reclaim it — not orphan cleanup (the ownership test excludes it), not
    /// the W17 exemption. Only a re-planned handoff clears it, so the only
    /// question the grace asks is whether one arrives.
    HolderTerminal,
    /// This node is a NON-holder sitting on orphan records. Resolvable, on the
    /// orphan-cleanup path: the pass reclaims the records and the ordinary
    /// prune then retires the entry and drops the fence.
    Orphan,
}

impl RefusedResidueClass {
    /// How long a residue of this class may stand before the gate calls it
    /// terminal.
    fn grace(self) -> Duration {
        match self {
            Self::HolderTerminal => REFUSED_HOLDER_TERMINAL_GRACE,
            Self::Orphan => REFUSED_ORPHAN_GRACE,
        }
    }
}

/// W16 — how long a HOLDER-TERMINAL residue may persist before a convergence
/// gate calls it FATAL.
///
/// Mirrors `SAME_TERM_REACTIVATION_COOLDOWN` (`src/cluster/coordinator.rs`,
/// private to that module): one full same-term re-heal window, which is the
/// only window in which a late re-plan could still clear the mark — a re-planned
/// handoff's first batch calls `mark_inbound_active`, which clears mark and
/// streak together. Waiting exactly one such window is therefore the difference
/// between "the source refused six times in a row" (what the mark proves) and
/// "and nothing re-planned it afterwards either" (what makes it terminal).
///
/// This class's server-side mark costs six consecutive refusals — 60 s at the
/// 10 s `TRANSFER_REQUEST_INTERVAL` — so a gate can only fail on it after ~90 s
/// of a shard being fenced with nothing coming for it. It cannot fire on a
/// transient source-table divergence.
///
/// W17 — that guarantee is TRUE only of this class, and until W17 the gate
/// could not demand it: it read the combined count, which also carries
/// `KeepOrphan` entries marked on their FIRST refusal. CI 32668963874 failed
/// twelve of those after three refusal rounds while the message claimed six.
/// The orphan class has its own, longer window — see [`REFUSED_ORPHAN_GRACE`].
const REFUSED_HOLDER_TERMINAL_GRACE: Duration = Duration::from_secs(30);

/// W17 — how long an ORPHAN-class residue may persist before a convergence gate
/// calls it FATAL.
///
/// Longer than [`REFUSED_HOLDER_TERMINAL_GRACE`], for two reasons that both
/// point the same way:
///
/// * the mark is EARNED CHEAPLY. `drop_refused_inbound` sets it on the FIRST
///   refusal for a non-holder, ~10 s after the entry is registered — there is
///   no streak behind it to spend part of the budget;
/// * it CLEARS SLOWLY. The path that resolves it is orphan cleanup reclaiming
///   the records, and the steady-state pass is rate-limited to one per
///   `EVENT_ORPHAN_CLEANUP_MIN_INTERVAL` (60 s, `src/cluster/coordinator.rs`).
///   A 30 s verdict can therefore fail a residue that was about to clear on its
///   own before the first pass even fires.
///
/// Two cleanup intervals plus a pass. Still decisive — a genuinely stranded
/// residue (the W17 circular wait: no committed-handoff evidence, disarmed
/// proof-of-elsewhere) never clears at all, and fails here.
const REFUSED_ORPHAN_GRACE: Duration = Duration::from_secs(120);

/// W17 — the per-class window state a polling gate carries: `Some(first_seen)`
/// while that class's residue stands, `None` the moment it clears.
///
/// Per class, not per node and not combined. A holder-terminal residue that
/// appears while an orphan residue is already standing must get its own full
/// window, and an orphan residue that clears must not leave a half-spent window
/// behind for a later one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RefusedResidueWindows {
    holder_terminal: Option<std::time::Instant>,
    orphan: Option<std::time::Instant>,
}

/// W16/W17 — advance the per-class windows with this poll's observation.
///
/// Clearing on zero is what keeps the grace honest. The mark is revocable by
/// design (a batch arriving, a task or re-registration, a re-park, the source
/// matching the request in a later round, or — for the orphan class — orphan
/// cleanup reclaiming the records), so a residue that comes back later starts a
/// NEW window rather than resuming a half-spent one.
///
/// # Known diagnostic gap (W17 review P2-6): a residue that FLAPS class
///
/// An entry can be re-classified between refusal rounds — the shard's records
/// or this node's holder-ness can change, and `drop_refused_inbound` assigns
/// the class every round. A residue that alternates holder-terminal and orphan
/// therefore zeroes one window each time it moves, so neither matures and the
/// run degrades to a plain budget timeout instead of the named residue verdict.
/// The verdict is not WRONG — a flapping entry is genuinely being re-judged,
/// and the timeout dump still reports the residue and its split — but it is
/// less legible than it should be. Deliberately not "fixed" by carrying a
/// window across a class change: that would judge an entry on a clock it earned
/// in a different state, which is the wave-16 defect in a new costume.
fn note_refused_residue(
    windows: RefusedResidueWindows,
    seen: RefusedResidue,
    now: std::time::Instant,
) -> RefusedResidueWindows {
    let advance = |window: Option<std::time::Instant>, count: u64| {
        if count == 0 {
            None
        } else {
            Some(window.unwrap_or(now))
        }
    };
    RefusedResidueWindows {
        holder_terminal: advance(windows.holder_terminal, seen.holder_terminal),
        orphan: advance(windows.orphan, seen.orphan),
    }
}

/// W17 — the class whose residue has outlasted ITS OWN grace, if any.
///
/// Checked holder-first only for determinism when both have expired; the two
/// windows are otherwise independent.
fn refused_residue_fatal_class(
    windows: RefusedResidueWindows,
    now: std::time::Instant,
) -> Option<RefusedResidueClass> {
    let expired = |window: Option<std::time::Instant>, class: RefusedResidueClass| {
        window
            .is_some_and(|since| now.saturating_duration_since(since) >= class.grace())
            .then_some(class)
    };
    expired(windows.holder_terminal, RefusedResidueClass::HolderTerminal)
        .or_else(|| expired(windows.orphan, RefusedResidueClass::Orphan))
}

/// W16/W17 — the diagnostic a fatal residue fails with, stating the guarantee
/// the gate ACTUALLY enforced for the class that expired.
///
/// This is NOT convergence. Every entry it counts is a shard left FENCED and
/// client-invisible on its own node. The holder variant is the armed-09 shape:
/// not slow-but-completable, but uncompletable — the source's completion
/// handshake failed the strict `actual == expected_records` check against a
/// target holding a SUPERSET, which retrying cannot satisfy. The orphan variant
/// is the W17 circular wait: the shard's records could only ever be reclaimed by
/// orphan cleanup, and they were not.
///
/// The count is discounted from `in_flight_inbound_pending` because the question
/// THAT asks — "is a migration still running?" — genuinely answers no. This is
/// the separate question, asked separately.
fn refused_residue_error(
    class: RefusedResidueClass,
    seen: RefusedResidue,
    node_details: &[String],
) -> String {
    let refused_details: Vec<&str> = node_details
        .iter()
        .filter(|d| d.contains("inbound-refused-retained"))
        .map(|d| d.as_str())
        .collect();
    let (count, condition) = match class {
        RefusedResidueClass::HolderTerminal => (
            seen.holder_terminal,
            format!(
                "refused by {{POSS}} own source for `REFUSED_HOLDER_TERMINAL_ROUNDS` consecutive \
                 rounds and still FENCED after a further {REFUSED_HOLDER_TERMINAL_GRACE:?}"
            ),
        ),
        RefusedResidueClass::Orphan => (
            seen.orphan,
            format!(
                "refused by {{POSS}} own source and still FENCED over local orphan records that \
                 orphan cleanup did not reclaim in {REFUSED_ORPHAN_GRACE:?}"
            ),
        ),
    };
    let condition = condition.replace("{POSS}", if count == 1 { "its" } else { "their" });
    format!(
        "TERMINALLY REFUSED INBOUND RESIDUE — {count} inbound entr{} {condition}: the shard(s) \
         stay client-invisible on that node and no transfer will ever un-fence them. This is not \
         convergence [{}]",
        if count == 1 { "y" } else { "ies" },
        refused_details.join(", "),
    )
}

/// W17 — accumulate ONE node's per-class residue into the run-wide census, and
/// name it in the per-node detail the fatal diagnostic quotes.
///
/// Both convergence gates poll the same endpoint and must reach the same
/// verdict, so the accumulation and the detail formatting live in one place.
/// The detail carries the split, not just the total: a residue that fails the
/// run has to say which class expired, on which node.
fn accumulate_refused_residue(
    total: &mut RefusedResidue,
    details: &mut Vec<String>,
    node_num: u32,
    json: &serde_json::Value,
) {
    let seen = refused_residue_counts(json);
    if seen.total() == 0 {
        return;
    }
    total.holder_terminal = total.holder_terminal.saturating_add(seen.holder_terminal);
    total.orphan = total.orphan.saturating_add(seen.orphan);
    details.push(format!(
        "node{node_num}:inbound-refused-retained={}(holder-terminal={},orphan={})",
        seen.total(),
        seen.holder_terminal,
        seen.orphan,
    ));
}

/// Wait until migrations complete on specific nodes (by node number).
///
/// Used when some node is deliberately absent (killed, restarting, being
/// removed) and therefore cannot be polled. The polled nodes must still
/// cover all 4096 master shards between them — a shard left to the absent
/// node is exactly the non-convergence this gate exists to catch.
///
/// On timeout the error carries the same master-census dump as
/// [`wait_migrations_complete`] (see [`master_divergence_block`]), so a
/// divergent sum names the offending shards and their claimants instead of
/// reporting only `masters=N/4096`.
pub async fn wait_specific_migrations_complete(
    docker: &DockerHelpers,
    node_nums: &[u32],
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    let mut ready_polls = 0u32;
    let mut refused_windows = RefusedResidueWindows::default();
    loop {
        let mut all_idle = true;
        let mut total_masters: u64 = 0;
        let mut total_inbound_pending: u64 = 0;
        let mut total_refused_retained = RefusedResidue::default();
        let mut total_pending_handoffs: u64 = 0;
        let mut status_details = Vec::new();
        for &n in node_nums {
            let port = docker.http_port(n);
            let url = format!("http://127.0.0.1:{port}/admin/migration_status");
            let mut active_count = None;
            let mut inbound_pending = 0u64;
            if let Ok(json) = poll_json(&url).await {
                // W12 TAIL 2 — only entries that can still progress hold the
                // gate; terminally-refused ones are reported below.
                inbound_pending = in_flight_inbound_pending(&json);
                total_inbound_pending += inbound_pending;
                accumulate_refused_residue(
                    &mut total_refused_retained,
                    &mut status_details,
                    n,
                    &json,
                );
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
                } else if let Some(marker) = status_degraded_marker(&json) {
                    // Task #75 — a degraded answer omits the master count;
                    // name the wedge instead of silently skipping the node.
                    status_details.push(format!("node{n}:{marker}"));
                }
            } else {
                status_details.push(format!(
                    "node{n}:status-unavailable,mig={},inbound={inbound_pending}",
                    active_count.unwrap_or(0)
                ));
            }
        }
        // W16 — a terminally-refused residue is NOT convergence, so it holds
        // the gate exactly like live work does, and after one re-heal window it
        // ends the wait outright: nothing will clear it, and burning the rest of
        // the budget only delays a failure that is already decided.
        let now = std::time::Instant::now();
        refused_windows = note_refused_residue(refused_windows, total_refused_retained, now);
        if let Some(class) = refused_residue_fatal_class(refused_windows, now) {
            return Err(ClientError::Connection(format!(
                "{} [masters={total_masters}/4096, handoffs={total_pending_handoffs}, \
                 inbound={total_inbound_pending}] [{}]",
                refused_residue_error(class, total_refused_retained, &status_details),
                status_details.join(", "),
            )));
        }
        if total_masters == 4096
            && total_pending_handoffs == 0
            && total_inbound_pending == 0
            && total_refused_retained.total() == 0
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
            // W15 — the specific-nodes gate gets the SAME master-census dump
            // as `wait_migrations_complete`. It did not have one, which is
            // why a 4103/4096 excess could name its seven overlapping shards
            // while a 4094/4096 deficit through this gate could only report
            // the sum. Only the polled nodes contribute claims, so
            // `master_divergence_block` names the polled set alongside the
            // orphan list.
            let overlap_detail = if total_masters != 4096 {
                let (per_node, unfetched) = fetch_master_sets(docker, node_nums).await;
                format!(
                    " [{}]",
                    master_divergence_block(&per_node, &unfetched, node_nums)
                )
            } else {
                String::new()
            };
            let residue = if total_refused_retained.total() > 0 {
                format!(
                    " [{}]",
                    refused_residue_error(
                        // The wait ran out of budget rather than expiring a
                        // window, so no class is decided; report the one that
                        // dominates the residue.
                        if total_refused_retained.holder_terminal >= total_refused_retained.orphan {
                            RefusedResidueClass::HolderTerminal
                        } else {
                            RefusedResidueClass::Orphan
                        },
                        total_refused_retained,
                        &status_details,
                    )
                )
            } else {
                String::new()
            };
            return Err(ClientError::Connection(format!(
                "migrations still active on specific nodes after {timeout:?} [masters={total_masters}/4096, handoffs={total_pending_handoffs}, inbound={total_inbound_pending}] [{}]{overlap_detail}{residue}",
                status_details.join(", ")
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Task #75 — compact marker for a DEGRADED `/status` answer (the server
/// timed out on a coordinator lock and served the reduced payload).
///
/// A degraded payload omits `cluster_size` and every shard-count field, so
/// without this marker the detail lines above render it as the misleading
/// `size=0,...,m=0,target=0`. The marker names WHICH locks were
/// unacquirable (`T`=shard_table, `M`=migration, `A`=node_addrs; `-` =
/// free) plus the server-reported event-loop heartbeat silence, so a CI
/// timeout message carries the wedge fingerprint directly.
///
/// Gate semantics are unchanged: the degraded payload still parses as
/// answering-but-not-converged (missing `cluster_size` -> not ready;
/// missing `target_master_shard_count` -> target=0 -> activation gate
/// holds). This helper is display-only.
fn status_degraded_marker(json: &serde_json::Value) -> Option<String> {
    if json["status_degraded"].as_bool() != Some(true) {
        return None;
    }
    let flag = |key: &str, mark: char| {
        if json[key].as_bool() == Some(true) {
            mark
        } else {
            '-'
        }
    };
    let stall_ms = json["event_loop"]["last_beat_age_ms"].as_u64().unwrap_or(0);
    Some(format!(
        "DEGRADED(locks={}{}{},loop_stall={}ms)",
        flag("table_lock_unavailable", 'T'),
        flag("migration_lock_unavailable", 'M'),
        flag("addrs_lock_unavailable", 'A'),
        stall_ms,
    ))
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

/// The cluster-wide sum of per-node ACTIVATED target-master counts, when it
/// diverges from the required total of 4096: each shard must have exactly
/// ONE target master, so any other sum means the polled nodes hold
/// divergent target tables (double-targeted or orphaned shards). Returns
/// `None` when the sum is exactly 4096. Only meaningful over a COMPLETE
/// poll — every node answered a full (non-degraded) `/status` — because a
/// missing node's uncounted targets fake a `< 4096` sum; the caller in
/// [`wait_migrations_complete`] enforces that, plus a 5s persistence window
/// against transient mid-install disagreement.
fn divergent_target_sum(views: &[NodeShardView]) -> Option<u64> {
    let sum: u64 = views.iter().map(|v| v.target_masters).sum();
    (sum != 4096).then_some(sum)
}

/// Advance the divergence-suspicion state machine for one ELIGIBLE poll
/// (every node answered a full `/status` and agreed on `agreed_version`);
/// returns `true` when the fail-fast is due.
///
/// `suspicion` records when a wrong target sum was FIRST seen and at which
/// agreed version. The fail-fast fires only when the sum is still wrong,
/// at the SAME agreed version, `persistence` after it was first recorded.
/// A wrong sum observed at a DIFFERENT version re-arms the clock instead
/// of inheriting the older suspicion: a transient at ver=8, a gate-closed
/// reactivation window of any length, and a fresh transient at ver=9 are
/// two unrelated samples — not one persistent divergence. A correct sum
/// clears the suspicion; ineligible polls must simply not call this.
fn divergence_fail_due(
    suspicion: &mut Option<(std::time::Instant, u64)>,
    now: std::time::Instant,
    agreed_version: u64,
    divergent: bool,
    persistence: Duration,
) -> bool {
    if !divergent {
        *suspicion = None;
        return false;
    }
    match *suspicion {
        Some((since, ver)) if ver == agreed_version => now.duration_since(since) >= persistence,
        _ => {
            *suspicion = Some((now, agreed_version));
            false
        }
    }
}

/// W4 — name the shards behind a diverging cluster-wide master sum.
///
/// `per_node` holds each node's EFFECTIVE mastered shard IDs (from
/// `/status?master_shards=1`). Returns a compact diagnostic naming the
/// OVERLAPPING shards (mastered by two or more nodes — the "4464/4096"
/// shape) and the ORPHANED shards (mastered by nobody — the "<4096" shape),
/// each list capped at [`MASTER_OVERLAP_DIAGNOSTIC_CAP`] entries with a
/// total count, so the timeout error names the divergent shards instead of
/// only their sum. Overlapping entries carry the claimant node numbers.
///
/// `unfetched` names nodes whose master set could not be fetched (review
/// nit): every shard such a node masters is counted as "orphaned" here, so
/// naming the missing nodes keeps an inflated orphan count honest.
pub fn master_overlap_diagnostic(per_node: &[(u32, Vec<u16>)], unfetched: &[u32]) -> String {
    const NUM_SHARDS: usize = 4096;
    let mut claimants: Vec<Vec<u32>> = vec![Vec::new(); NUM_SHARDS];
    for (node, shards) in per_node {
        for &s in shards {
            if let Some(c) = claimants.get_mut(s as usize) {
                c.push(*node);
            }
        }
    }
    let overlapping: Vec<(u16, &Vec<u32>)> = claimants
        .iter()
        .enumerate()
        .filter(|(_, c)| c.len() >= 2)
        .map(|(s, c)| (s as u16, c))
        .collect();
    let orphaned: Vec<u16> = claimants
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_empty())
        .map(|(s, _)| s as u16)
        .collect();
    let overlap_names: Vec<String> = overlapping
        .iter()
        .take(MASTER_OVERLAP_DIAGNOSTIC_CAP)
        .map(|(s, c)| {
            let nodes: Vec<String> = c.iter().map(|n| format!("n{n}")).collect();
            format!("{s}({})", nodes.join("+"))
        })
        .collect();
    let orphan_names: Vec<String> = orphaned
        .iter()
        .take(MASTER_OVERLAP_DIAGNOSTIC_CAP)
        .map(|s| s.to_string())
        .collect();
    let more = |total: usize| {
        if total > MASTER_OVERLAP_DIAGNOSTIC_CAP {
            format!(" +{} more", total - MASTER_OVERLAP_DIAGNOSTIC_CAP)
        } else {
            String::new()
        }
    };
    let unfetched_suffix = if unfetched.is_empty() {
        String::new()
    } else {
        let names: Vec<String> = unfetched.iter().map(|n| format!("n{n}")).collect();
        format!(
            ", unfetched=[{}] (their masters count as orphaned)",
            names.join(", ")
        )
    };
    format!(
        "overlapping={} [{}{}], orphaned={} [{}{}]{}",
        overlapping.len(),
        overlap_names.join(", "),
        more(overlapping.len()),
        orphaned.len(),
        orphan_names.join(", "),
        more(orphaned.len()),
        unfetched_suffix,
    )
}

/// Cap on the shard IDs [`master_overlap_diagnostic`] prints per list.
pub const MASTER_OVERLAP_DIAGNOSTIC_CAP: usize = 20;

/// W15 — the complete master-census block a convergence gate stamps onto its
/// timeout error: the DIRECTION of the divergence, its magnitude, the polled
/// set that defines the census, and the shard-level detail from
/// [`master_overlap_diagnostic`].
///
/// The direction is the part that was missing. Both convergence gates report
/// a bare `masters=N/4096` sum, and the two ways that sum goes wrong have
/// opposite causes and opposite repairs:
///
/// * **EXCESS** (`4103/4096`) — shards claimed by two nodes at once. The
///   shard IDs come with their claimant nodes, so the pair is named.
/// * **DEFICIT** (`4094/4096`) — shards claimed by NOBODY in the polled set.
///   Named just as explicitly here (`orphaned=2 [7, 1234]`), because reading
///   a deficit off a sum alone tells you only that two shards are missing,
///   not which — that asymmetry is exactly what made one CI triage cost an
///   afternoon while the excess case named its seven shards immediately.
/// * **BALANCED-BUT-DIVERGENT** — a surplus that exactly cancels a shortfall
///   sums to 4096 and slips through the gate's sum check entirely. It is
///   still a divergent table, so it is never rendered as a bare "0".
///
/// `polled` is named because it defines what "nobody" means: only the polled
/// nodes contribute claims, so a shard mastered by an unpolled node (killed,
/// restarting, deliberately excluded from a specific-nodes gate) is counted
/// as orphaned here. That is precisely what the gate means by "not
/// converged" — the polled survivors must cover all 4096 — but it must not
/// be misread as "no node anywhere masters this shard". `unfetched` names
/// polled nodes that failed to answer, whose claims inflate the orphan count
/// the same way.
pub fn master_divergence_block(
    per_node: &[(u32, Vec<u16>)],
    unfetched: &[u32],
    polled: &[u32],
) -> String {
    const NUM_SHARDS: usize = 4096;
    let node_list = |nodes: &[u32]| {
        nodes
            .iter()
            .map(|n| format!("n{n}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let polled_suffix = format!("polled=[{}]", node_list(polled));
    if per_node.is_empty() {
        // No claims at all is a statement about the POLL, not the cluster:
        // rendering it as 4096 orphans would fabricate a total-orphaning
        // wedge out of an HTTP failure.
        return format!(
            "master sets unavailable: {polled_suffix}, unfetched=[{}]",
            node_list(unfetched)
        );
    }
    let mut claim_count = vec![0u32; NUM_SHARDS];
    for (_, shards) in per_node {
        for &s in shards {
            if let Some(c) = claim_count.get_mut(s as usize) {
                *c += 1;
            }
        }
    }
    let total_claims: i64 = claim_count.iter().map(|&c| i64::from(c)).sum();
    let orphaned = claim_count.iter().filter(|&&c| c == 0).count();
    let overlapping = claim_count.iter().filter(|&&c| c >= 2).count();
    let net = total_claims - NUM_SHARDS as i64;
    let verdict = if net > 0 {
        format!("MASTER CENSUS EXCESS: {net} claim(s) over {NUM_SHARDS}")
    } else if net < 0 {
        format!(
            "MASTER CENSUS DEFICIT: {} shard(s) mastered by none of the polled nodes",
            -net
        )
    } else if orphaned > 0 || overlapping > 0 {
        format!(
            "MASTER CENSUS BALANCED-BUT-DIVERGENT: {overlapping} surplus claim(s) exactly \
             cancel {orphaned} orphan(s), so the sum reads {NUM_SHARDS} while the table is \
             still split"
        )
    } else {
        format!("MASTER CENSUS SETTLED: every one of {NUM_SHARDS} shards claimed exactly once")
    };
    format!(
        "{verdict}; {polled_suffix}; {}",
        master_overlap_diagnostic(per_node, unfetched)
    )
}

/// Fetch each node's EFFECTIVE mastered shard IDs via
/// `/status?master_shards=1`, for [`master_divergence_block`].
///
/// Returns `(per_node, unfetched)`: nodes that did not answer (or answered a
/// DEGRADED payload, which omits the field) land in `unfetched` rather than
/// contributing an empty claim set, so the caller can keep the resulting
/// orphan inflation honest.
async fn fetch_master_sets(
    docker: &DockerHelpers,
    nodes: &[u32],
) -> (Vec<(u32, Vec<u16>)>, Vec<u32>) {
    let mut per_node: Vec<(u32, Vec<u16>)> = Vec::new();
    let mut unfetched: Vec<u32> = Vec::new();
    for &i in nodes {
        let port = docker.http_port(i);
        let url = format!("http://127.0.0.1:{port}/status?master_shards=1");
        if let Ok(json) = poll_json(&url).await
            && let Some(list) = json["master_shards"].as_array()
        {
            let shards: Vec<u16> = list
                .iter()
                .filter_map(|v| v.as_u64())
                .filter_map(|v| u16::try_from(v).ok())
                .collect();
            per_node.push((i, shards));
        } else {
            unfetched.push(i);
        }
    }
    (per_node, unfetched)
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
    // Divergence fail-fast state: when the target sum was FIRST seen wrong
    // and at which agreed shard_table_version; cleared whenever a complete
    // poll sees it right, re-armed when the agreed version moves. See
    // `divergence_fail_due` and the check below the node loop.
    let mut divergent_since: Option<(std::time::Instant, u64)> = None;
    // Retained divergence verdict for the timeout dump (diagnostic only —
    // see the demotion note at the check site).
    let mut divergence_note: Option<String> = None;
    // W16/W17 — when a terminally-refused residue of each CLASS was FIRST
    // seen, cleared whenever a poll sees none of that class (the mark is
    // revocable, so a residue that returns starts a fresh window). See
    // `note_refused_residue`.
    let mut refused_windows = RefusedResidueWindows::default();
    loop {
        let mut all_idle = true;
        let mut total_masters: u64 = 0;
        let mut total_pending_handoffs: u64 = 0;
        let mut total_inbound_pending: u64 = 0;
        let mut total_refused_retained = RefusedResidue::default();
        let mut node_details = Vec::new();
        let mut shard_views: Vec<NodeShardView> = Vec::new();
        let mut complete_status_answers: u32 = 0;
        let mut status_versions: Vec<u64> = Vec::new();
        for i in 1..=node_count {
            let port = docker.http_port(i);
            let url = format!("http://127.0.0.1:{port}/admin/migration_status");
            if let Ok(json) = poll_json(&url).await {
                // W12 TAIL 2 — an entry whose source terminally refused it is
                // a fixpoint, not migration work: it is discounted from the
                // gate and reported separately (see
                // `in_flight_inbound_pending`).
                let inbound_pending = in_flight_inbound_pending(&json);
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
                accumulate_refused_residue(
                    &mut total_refused_retained,
                    &mut node_details,
                    i,
                    &json,
                );
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
                // Task #75 — a degraded answer omits the shard counts (they
                // parse as 0 and hold the activation gate below); name the
                // wedge in the detail line instead of a misleading m=0 row.
                if let Some(marker) = status_degraded_marker(&json) {
                    node_details.push(format!("node{i}:{marker}"));
                } else {
                    complete_status_answers += 1;
                    status_versions.push(version);
                    node_details.push(format!(
                        "node{i}:size={cluster_size},ver={version},m={m},target={target_m}"
                    ));
                }
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
        // Divergence fail-fast (scenario 09 diagnosis): when every node
        // answered a full (non-degraded) `/status` AND agrees on the
        // activated `shard_table_version`, the per-node ACTIVATED target
        // counts must sum to exactly 4096 — one target master per shard,
        // cluster-wide. A sum that stays wrong for >= 5s at the SAME agreed
        // version means the nodes hold DIVERGENT target tables for that
        // version (double-targeted or orphaned shards); no migration
        // progress can settle that, so burning the remaining timeout only
        // mislabels the failure as slow migration. A single wrong sample is
        // tolerated (a node polled mid-install can legitimately disagree
        // transiently), an incomplete poll neither confirms nor clears the
        // suspicion, the suspicion clock is scoped to the agreed version
        // (two unrelated transients straddling a reactivation must not
        // combine into one "persistent" divergence), and the
        // version-agreement gate keeps STAGGERED activation honest: a late
        // activator (the reactivation cooldown reaches 30s) reports
        // old-table targets under an old version — a legitimate >= 5s
        // transient, not divergence.
        if complete_status_answers == node_count && status_versions.windows(2).all(|w| w[0] == w[1])
        {
            let agreed_version = status_versions.first().copied().unwrap_or(0);
            let divergent = divergent_target_sum(&shard_views);
            if divergence_fail_due(
                &mut divergent_since,
                std::time::Instant::now(),
                agreed_version,
                divergent.is_some(),
                Duration::from_secs(5),
            ) {
                // DIAGNOSTIC ONLY — never an early bail. Run 32010681108
                // proved persistent same-version wrong sums occur inside
                // legitimately-converging windows: the degraded-term
                // UPGRADE re-activates at the SAME version (its 2s-backoff
                // + 2s-exchange rescue sits exactly at this 5s bound), and
                // live handoff waves keep per-node targets disagreeing for
                // longer still — the early bail killed five scenarios
                // including durably-green 11. The verdict is retained and
                // stamped onto the timeout error below, so a genuinely
                // wedged divergence is still named instead of reading as
                // slow migration.
                let sum = divergent.unwrap_or(0);
                let wrong_for = divergent_since
                    .map(|(since, _)| since.elapsed().as_secs_f64())
                    .unwrap_or(0.0);
                divergence_note = Some(format!(
                    " [DIVERGENT TARGET TABLES: targets sum to {sum} != 4096 at agreed \
                     shard_table_version {agreed_version}, wrong for {wrong_for:.1}s across \
                     consecutive polls]"
                ));
            } else if divergent.is_none() {
                // A correct sum at an agreed version clears any stale note:
                // the timeout dump must only name divergence that was live
                // in the final observed state.
                divergence_note = None;
            }
        }
        let activation_reason = shard_activation_gate_reason(&shard_views, node_count);
        let masters_ok = total_masters == 4096 && activation_reason.is_none();
        // W16 — a terminally-refused residue is NOT convergence, so it holds the
        // gate exactly like live work does, and after one re-heal window it ends
        // the wait outright: nothing will clear it, and burning the rest of the
        // budget only delays a failure that is already decided.
        let now = std::time::Instant::now();
        refused_windows = note_refused_residue(refused_windows, total_refused_retained, now);
        if let Some(class) = refused_residue_fatal_class(refused_windows, now) {
            return Err(ClientError::Connection(format!(
                "{} [masters={total_masters}/4096, handoffs={total_pending_handoffs}, \
                 inbound={total_inbound_pending}, activation={}] [{}]",
                refused_residue_error(class, total_refused_retained, &node_details),
                activation_reason.as_deref().unwrap_or("ok"),
                node_details.join(", "),
            )));
        }
        if masters_ok
            && total_pending_handoffs == 0
            && total_inbound_pending == 0
            && total_refused_retained.total() == 0
            && all_idle
        {
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
            // W4 — when the cluster-wide master sum diverges from 4096
            // (double-mastered or orphaned shards), fetch each node's
            // EFFECTIVE mastered shard IDs and NAME the divergent shards, so
            // the next "4464/4096" comes with a shard list instead of a sum.
            let overlap_detail = if total_masters != 4096 {
                let polled: Vec<u32> = (1..=node_count).collect();
                let (per_node, unfetched) = fetch_master_sets(docker, &polled).await;
                format!(
                    " [{}]",
                    master_divergence_block(&per_node, &unfetched, &polled)
                )
            } else {
                String::new()
            };
            // The activation reason carries the per-node serving/target dump,
            // so a gate held ONLY by an unactivated table names the nodes
            // holding it instead of leaving `masters=4096` looking settled.
            let residue = if total_refused_retained.total() > 0 {
                format!(
                    " [{}]",
                    refused_residue_error(
                        // The wait ran out of budget rather than expiring a
                        // window, so no class is decided; report the one that
                        // dominates the residue.
                        if total_refused_retained.holder_terminal >= total_refused_retained.orphan {
                            RefusedResidueClass::HolderTerminal
                        } else {
                            RefusedResidueClass::Orphan
                        },
                        total_refused_retained,
                        &node_details,
                    )
                )
            } else {
                String::new()
            };
            return Err(ClientError::Connection(format!(
                "migrations still active after {timeout:?} [masters={total_masters}/4096, handoffs={total_pending_handoffs}, inbound={total_inbound_pending}, activation={}] [{}]{overlap_detail}{}{residue}",
                activation_reason.as_deref().unwrap_or("ok"),
                node_details.join(", "),
                divergence_note.as_deref().unwrap_or("")
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Split the records that are still failing on a
/// [`wait_for_migration_reads_ready`] timeout into the two DISTINCT failure
/// classes the helper polls for, capping each at `cap` records.
///
/// The two classes fail for different reasons and need different reading:
/// `master_failed` means the master route would not serve the record at all,
/// `under_replicated` means the master served it but fewer than `min_replicas`
/// nodes hold a local copy. Returning them separately (rather than merged) is
/// the whole point — run 31946515845 scenario 08 timed out with
/// `master_failed=0/1200, under_replicated=1/50` and printed
/// `first_failures (rich, n=0)`, because the dump was built exclusively from
/// the master-route indices and the under-replicated sample indices were
/// computed and then dropped. The surviving failure mode reported nothing.
///
/// `master_failed_idx` and `under_replicated_idx` index into `txids`;
/// out-of-range indices are skipped rather than panicking. An index that
/// appears in BOTH lists is reported only under `master_failed` (the stronger
/// symptom), so the two sections never duplicate a record and the
/// `under_replicated` section spends its cap on records the first section
/// does not already cover.
///
/// Both classes are always returned, in order, even when empty — a section
/// reading `n=0` is what tells the reader that class was clean.
fn select_migration_failure_samples(
    txids: &[[u8; 32]],
    master_failed_idx: &[usize],
    under_replicated_idx: &[usize],
    cap: usize,
) -> Vec<(&'static str, Vec<[u8; 32]>)> {
    let master: Vec<usize> = master_failed_idx
        .iter()
        .copied()
        .filter(|&i| i < txids.len())
        .take(cap)
        .collect();
    let shown: std::collections::HashSet<usize> = master.iter().copied().collect();
    let under: Vec<usize> = under_replicated_idx
        .iter()
        .copied()
        .filter(|&i| i < txids.len() && !shown.contains(&i))
        .take(cap)
        .collect();
    vec![
        (
            "master_failed",
            master.into_iter().map(|i| txids[i]).collect(),
        ),
        (
            "under_replicated",
            under.into_iter().map(|i| txids[i]).collect(),
        ),
    ]
}

/// Render the labelled per-class diagnose sections appended to the
/// [`wait_for_migration_reads_ready`] timeout error.
///
/// Each entry is `(class_label, sampled_record_count, dump)` where `dump` is
/// the [`collect_admin_diagnose_dump`] output for that class (empty when the
/// class had no records). The label is what tells a master-route failure
/// apart from an under-replication failure in the log; the count is the
/// number of records actually diagnosed, which is capped and so can be lower
/// than the class total reported in the summary line. A class with no
/// records renders as ` none` rather than a dangling colon.
fn format_migration_failure_sections(sections: &[(&str, usize, String)]) -> String {
    sections
        .iter()
        .map(|(label, n, dump)| {
            let body = if dump.is_empty() { " none" } else { dump };
            format!(" {label} first_failures (rich, n={n}):{body}")
        })
        .collect::<Vec<_>>()
        .join(";")
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
/// records of EACH failure class: master-route failures and under-replicated
/// samples are dumped and labelled separately (see
/// [`select_migration_failure_samples`]), so a timeout held only by
/// under-replication still names records instead of dumping nothing.
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
        // The txid indices of the under-replicated samples are kept, not just
        // counted: when this class is the ONLY one failing it is the only
        // evidence the timeout dump can show (see
        // `select_migration_failure_samples`).
        let mut under_replicated_idx: Vec<usize> = Vec::new();
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
            if holders < min_replicas {
                under_replicated_idx.push(idx);
            }
        }
        let under_replicated = under_replicated_idx.len();
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
            // Diagnose the first 32 records of EACH failure class via the
            // OP_ADMIN_DIAGNOSE_KEY admin op, which returns each
            // node's per-shard state (shard, master, holder, inbound,
            // fenced, migrating, topology epoch) for every txid in a
            // single batched call. The collection + formatting lives
            // in `collect_admin_diagnose_dump` so other helpers
            // (e.g. `wait_migrations_complete_with_diag`) can reuse it.
            // Both classes are dumped and labelled separately, so a timeout
            // held ONLY by under-replication still shows per-record evidence.
            let cap = (ADMIN_DIAGNOSE_KEY_MAX_TXIDS as usize).min(32);
            let classes = select_migration_failure_samples(
                txids,
                &master_failed_idx,
                &under_replicated_idx,
                cap,
            );
            let mut dumps: Vec<(&str, usize, String)> = Vec::with_capacity(classes.len());
            for (label, sample) in &classes {
                let dump =
                    collect_admin_diagnose_dump(client, &node_addrs, node_nums, sample).await;
                dumps.push((label, sample.len(), dump));
            }
            let sections = format_migration_failure_sections(&dumps);

            return Err(ClientError::Connection(format!(
                "migration read verify timeout after {timeout:?}: \
                 master_failed={master_failed}/{}, under_replicated={under_replicated}/{} \
                 (min_replicas={min_replicas}, nodes={node_nums:?});{sections}",
                txids.len(),
                sample_indices.len(),
            )));
        }

        let _ = client.refresh_routing().await;
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

/// Node-keyed redo-settle predicate for [`wait_replication_settled`] /
/// [`wait_specific_replication_settled`].
///
/// Each poll round feeds the map `node -> current_sequence` of the nodes that
/// actually answered `/debug/redo`. Settled means the RESPONDING SET and every
/// sequence in it were unchanged for two consecutive rounds after the first.
///
/// Keyed by node on purpose (run 31936746201 armed-15). The predecessor
/// collected a positional `Vec<u64>` that silently OMITTED non-responders,
/// with two failure modes:
/// (a) a flapping node alternated the vector length, resetting the stability
///     counter forever ("did not settle after 5s; last redo sequences:
///     [1033, 1058]" -- two entries for three nodes); and
/// (b) a MIDDLE node dropping out shifted positions, so one node's sequence
///     compared against a DIFFERENT node's previous one -- capable of a
///     FALSE-POSITIVE settle over sequences that never stabilized.
/// Keying by node makes a missing node "unknown" (never a positional shift),
/// and any change in the responding set explicitly resets the counter. An
/// empty responding round can never settle: settling must be evidence of
/// observed stability, not of nobody answering.
#[derive(Default)]
struct SettleTracker {
    /// The previous round's responses; `None` before the first round.
    prev: Option<BTreeMap<u32, u64>>,
    /// Consecutive rounds with an identical (set, sequences) observation.
    stable_polls: u32,
}

impl SettleTracker {
    /// Rounds after the first that must repeat the identical observation.
    const REQUIRED_STABLE_POLLS: u32 = 2;

    /// Feed one poll round; returns `true` once settled (see type docs).
    fn observe(&mut self, seqs: BTreeMap<u32, u64>) -> bool {
        match self.prev.as_ref() {
            // Responding-set change: a node vanished or (re)appeared. Reset --
            // a missing node is "unknown", and this round is not comparable to
            // the previous one, positionally or otherwise.
            Some(prev) if !prev.keys().eq(seqs.keys()) => self.stable_polls = 0,
            // Same set, identical sequences: one more stable round.
            Some(prev) if *prev == seqs => self.stable_polls += 1,
            // Same set but some sequence moved, or the very first round.
            _ => self.stable_polls = 0,
        }
        let settled = self.stable_polls >= Self::REQUIRED_STABLE_POLLS && !seqs.is_empty();
        self.prev = Some(seqs);
        settled
    }
}

/// Wait for replication to propagate.
///
/// Polls `/debug/redo` on each node `1..=node_count` and waits until the
/// per-node redo sequences stabilize (stop changing between polls). This
/// detects when all in-flight replication has completed without requiring
/// sequences to be equal across nodes (each node has an independent redo log).
/// See [`SettleTracker`] for the settle predicate.
pub async fn wait_replication_settled(
    docker: &DockerHelpers,
    node_count: u32,
    timeout: Duration,
) -> Result<(), ClientError> {
    let nodes: Vec<u32> = (1..=node_count).collect();
    wait_specific_replication_settled(docker, &nodes, timeout).await
}

/// Wait for replication to settle on specific nodes only (e.g., surviving
/// nodes after a kill). See [`SettleTracker`] for the settle predicate; the
/// timeout error names the nodes that were not answering `/debug/redo`.
pub async fn wait_specific_replication_settled(
    docker: &DockerHelpers,
    node_nums: &[u32],
    timeout: Duration,
) -> Result<(), ClientError> {
    let start = std::time::Instant::now();
    let mut tracker = SettleTracker::default();

    loop {
        let mut seqs = BTreeMap::new();
        for &n in node_nums {
            let port = docker.http_port(n);
            let url = format!("http://127.0.0.1:{port}/debug/redo");
            if let Ok(json) = poll_json(&url).await
                && let Some(seq) = json["current_sequence"].as_u64()
            {
                seqs.insert(n, seq);
            }
        }

        if tracker.observe(seqs.clone()) {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            let non_responding: Vec<u32> = node_nums
                .iter()
                .copied()
                .filter(|n| !seqs.contains_key(n))
                .collect();
            return Err(ClientError::Connection(format!(
                "replication did not settle on nodes {node_nums:?} after {timeout:?}; \
                 last redo sequences by node: {seqs:?}; \
                 non-responding nodes: {non_responding:?}",
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

    let mut txids = Vec::with_capacity(count as usize);
    let mut driver = ClientSeedDriver { client };

    for batch_start in (0..count).step_by(100) {
        let batch_end = (batch_start + 100).min(count);
        // The RNG handle lives and dies inside this block, never across the
        // await below: `ThreadRng` is `Rc`-backed and therefore `!Send`, and
        // holding one across an await makes this whole future `!Send` — which
        // `tokio::spawn` rejects. Callers that issue seed batches
        // concurrently (scenario 08's 8d.2) spawn this future.
        let (items, batch_meta) = {
            let mut rng = rand::thread_rng();
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
            (items, batch_meta)
        };

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

/// What to do with the per-item errors of one spend attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum SpendAttemptOutcome {
    /// Every submitted item landed.
    Done,
    /// These indices (into the slice that was submitted) failed with a
    /// transient code and must be re-sent.
    Retry(Vec<usize>),
    /// A per-item error the retry policy does not classify as transient.
    /// Terminal — surfacing it is the whole point.
    Fatal(u16),
}

/// Classify one spend attempt's per-item errors.
///
/// Transience is decided by the shared policy
/// ([`teraslab_test_client::retry::is_transient_code`]) so spend rides out
/// exactly the same server conditions `seed_records` does — most importantly
/// `ERR_MIGRATION_IN_PROGRESS` (19), which a shard handoff raises for as long
/// as the fence is up and which clears on its own.
///
/// A single non-transient per-item code makes the whole attempt
/// [`SpendAttemptOutcome::Fatal`]: retrying must never be a way to make a
/// real failure (e.g. `ERR_ALREADY_SPENT`) disappear.
///
/// Two cases re-send the WHOLE batch rather than the erroring subset,
/// matching `split_partial_successes`' stance for seeds — in both, nothing
/// in the response can be trusted to have landed, and re-sending an item
/// that did land is a no-op because a spend is idempotent for identical
/// `spending_data`:
///
/// * `degraded` — the server could not confirm the ack, so the reported
///   successes are not durable-confirmed either.
/// * an `item_index` at or past `submitted` — the response cannot be mapped
///   onto the request at all.
pub fn classify_spend_attempt(
    errors: &[BatchItemError],
    submitted: usize,
    degraded: bool,
) -> SpendAttemptOutcome {
    if let Some(fatal) = errors
        .iter()
        .find(|e| !teraslab_test_client::retry::is_transient_code(e.code))
    {
        return SpendAttemptOutcome::Fatal(fatal.code);
    }
    if degraded {
        return SpendAttemptOutcome::Retry((0..submitted).collect());
    }
    if errors.is_empty() {
        return SpendAttemptOutcome::Done;
    }
    if errors.iter().any(|e| e.item_index as usize >= submitted) {
        return SpendAttemptOutcome::Retry((0..submitted).collect());
    }
    let mut idx: Vec<usize> = errors.iter().map(|e| e.item_index as usize).collect();
    idx.sort_unstable();
    idx.dedup();
    SpendAttemptOutcome::Retry(idx)
}

/// Spend every item, retrying only the ones that failed transiently.
///
/// The hand-rolled spend loops in the scenarios counted a transient
/// `ERR_MIGRATION_IN_PROGRESS` as a hard failure, so a spend issued while a
/// shard handoff fence was up failed the scenario even though the identical
/// call two seconds later (via `seed_records`, which does retry) succeeded.
/// Backoff, attempt budget, and the transient set all come from the shared
/// [`teraslab_test_client::retry`] policy.
///
/// Returns `Ok(())` only when every item has been acknowledged, so the caller
/// may record all of them in the verifier. Returns the terminal error (or,
/// once the budget is exhausted, the last transient one) otherwise.
pub async fn spend_all_with_transient_retry(
    client: &Client,
    params: &SpendBatchParams,
    items: &[SpendItem],
) -> Result<(), ClientError> {
    use teraslab_test_client::retry::MAX_TRANSIENT_ATTEMPTS;

    let mut remaining: Vec<SpendItem> = items.to_vec();
    let mut last_transient: Option<ClientError> = None;

    for attempt in 0..MAX_TRANSIENT_ATTEMPTS {
        if remaining.is_empty() {
            return Ok(());
        }
        let (errors, degraded) = match client.spend_batch(params, &remaining).await {
            Ok(resp) => (resp.errors, false),
            Err(ClientError::Partial(pe)) => (pe.errors, pe.degraded),
            // Whole-op transient (connection blip during a topology change,
            // or a single server code for the batch): nothing is known to
            // have landed, so the whole set is re-sent. Safe because a spend
            // is idempotent for identical `spending_data`.
            Err(e) if teraslab_test_client::retry::is_transient_error(&e) => {
                last_transient = Some(e);
                if attempt + 1 < MAX_TRANSIENT_ATTEMPTS {
                    spend_retry_backoff(client, attempt).await;
                }
                continue;
            }
            Err(e) => return Err(e),
        };

        match classify_spend_attempt(&errors, remaining.len(), degraded) {
            SpendAttemptOutcome::Done => return Ok(()),
            SpendAttemptOutcome::Fatal(code) => {
                return Err(ClientError::Server {
                    code,
                    message: format!(
                        "spend_batch: non-transient per-item error {code} \
                         ({} of {} items failed)",
                        errors.len(),
                        remaining.len()
                    ),
                });
            }
            SpendAttemptOutcome::Retry(idx) => {
                if attempt == 0 {
                    eprintln!(
                        "  spend: transient error on attempt 0, retrying {} of {} items",
                        idx.len(),
                        remaining.len()
                    );
                }
                last_transient = Some(match errors.first() {
                    Some(e) => ClientError::Server {
                        code: e.code,
                        message: format!("spend_batch: {} items still transient", idx.len()),
                    },
                    // A degraded ack with no per-item errors: nothing is
                    // confirmed durable, so the batch is re-sent whole.
                    None => ClientError::Connection(format!(
                        "spend_batch: degraded ack, {} items unconfirmed",
                        idx.len()
                    )),
                });
                remaining = idx.into_iter().map(|i| remaining[i].clone()).collect();
                if attempt + 1 >= MAX_TRANSIENT_ATTEMPTS {
                    break;
                }
                spend_retry_backoff(client, attempt).await;
            }
        }
    }

    Err(last_transient.unwrap_or_else(|| {
        ClientError::Connection(format!(
            "spend_batch: {} items still failing after {MAX_TRANSIENT_ATTEMPTS} attempts",
            remaining.len()
        ))
    }))
}

/// Wait out the shared backoff for `attempt`, then refresh routing — the
/// same shape `ClientSeedDriver::backoff` uses, because the transient codes
/// this rides out are usually accompanied by a routing change.
async fn spend_retry_backoff(client: &Client, attempt: u32) {
    tokio::time::sleep(teraslab_test_client::retry::backoff_for_attempt(attempt)).await;
    let _ = client.refresh_routing().await;
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
///
/// Captures, per node: container logs, `/status`, `/admin/migration_status`
/// and the `/metrics` scrape as `nodeN_final_metrics.txt` — the same file
/// name collect_logs.sh writes, so the CI artifact glob picks up either
/// path's copy. The metrics were the gap: a scenario that dies here takes
/// its containers down with it, so counters like
/// `teraslab_under_replication_*` were unavailable for exactly the runs that
/// needed them (scenario 11's topology failure had none). A file is written
/// only when the scrape really returned a body — an absent file is honest,
/// an empty one is not.
pub async fn collect_failure_diagnostics(scenario_id: u16) {
    let Ok(dir) = std::env::var(teraslab_test_client::helpers::ENV_DIAG_DIR) else {
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
            // A node removed mid-scenario (scenario 07 removes node4 at
            // Test 7.3) makes `docker logs` fail with "No such container";
            // `captured_log_bytes` rejects that so the copy
            // `DockerHelpers::remove_node` archived here before the removal
            // survives.
            if let Some(buf) = teraslab_test_client::helpers::captured_log_bytes(
                out.status.success(),
                out.stdout,
                &out.stderr,
            ) {
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
        // W15 — the Prometheus scrape, which this dump omitted entirely.
        // collect_logs.sh writes `*_final_metrics.txt` from the same
        // `/metrics` route, but it only runs AFTER the harness teardown, and
        // the in-test failure path destroys the containers first — so on every
        // in-test failure the counters were simply unavailable. Triaging the
        // scenario-05 acked-loss chain (CI run 32637576348) needed exactly
        // these: the migration prune / orphan-cleanup counters that say
        // whether a deleting path ran at all.
        //
        // Same filename shape and the same "an absent file is honest, an empty
        // one is not" rule as collect_logs.sh.
        let metrics_url = format!("http://127.0.0.1:{port}/metrics");
        if let Ok(body) = poll_text(&metrics_url).await {
            let _ = std::fs::write(dir.join(format!("node{n}_final_metrics.txt")), body);
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

    /// W13 round-2 review P2-4 — the TRAP, rendered into the PANIC MESSAGE of
    /// an over-replication failure (records at more than RF holders).
    ///
    /// A CI triager reads the assertion failure, not this file's doc comments.
    /// The obvious move on seeing "3:45" in the histogram is to arm
    /// `orphan_cleanup_proof_reclaim_enabled` so the third copies get
    /// reclaimed — which is exactly the configuration that destroyed four
    /// acked records. Empty for a clean or UNDER-replicated report: that is a
    /// different failure with a different fix, and a note that always fires is
    /// a note nobody reads.
    fn over_replication_trap_note(&self) -> String {
        let over_replicated: u32 = self
            .holder_count_histogram
            .iter()
            .enumerate()
            .filter(|(holders, _)| *holders > RF2_EXPECTED_HOLDERS)
            .map(|(_, count)| *count)
            .sum();
        if over_replicated == 0 {
            return String::new();
        }
        format!(
            "\n  NOTE — {over_replicated} of these are OVER-replicated (more than \
             {RF2_EXPECTED_HOLDERS} holders), which is the SAFE state and is EXPECTED on \
             scenarios 15/17: after a SIGKILL the committed-handoff evidence is \
             unearnable, so the fail-closed #28 guard retains the extra copy \
             forever. Do NOT 'fix' this by setting \
             orphan_cleanup_proof_reclaim_enabled = true — that is the \
             configuration that DELETED FOUR ACKED RECORDS in CI (a unanimous \
             superset confirmation is an instant-in-time attestation with no \
             retention commitment, so confirming holders draining the same copy \
             leave this node deleting the last one). Over-replication is wasted \
             space; a destroyed UTXO is not recoverable. Fix the retention \
             protocol or scope this assertion — never the flag."
        )
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
            if holder_indices.len() != RF2_EXPECTED_HOLDERS {
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

/// Holders every present record must have under the harness's RF=2 configs.
/// More than this is OVER-replication (see
/// `ReplicationCheckReport::over_replication_trap_note`); fewer is
/// under-replication.
const RF2_EXPECTED_HOLDERS: usize = 2;

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
///
/// # TRAP — do NOT "fix" an over-replication failure here by arming `orphan_cleanup_proof_reclaim_enabled`
///
/// This assertion is EXPECTED to red on scenarios 15 and 17 with a
/// three-holder (over-replication) histogram, and that is the SAFE state.
///
/// After a SIGKILL the surviving node's committed-handoff evidence for a
/// shard is unearnable, so the fail-closed task-#28 guard retains its copy
/// forever — W12 measured 45/295 (default-15) and 192/2000 (default-17)
/// records sitting at three holders. W12 added a "proof-of-elsewhere" reclaim
/// to clear exactly that, and it DESTROYED FOUR ACKED RECORDS in CI: a
/// unanimous superset confirmation is an instant-in-time attestation with no
/// retention commitment, so holders draining the same copy through the legacy
/// path (observed ~100 ms after confirming) — or a retired issue-#46 parking
/// refusal — leave this node deleting the LAST copy. It now ships behind
/// `orphan_cleanup_proof_reclaim_enabled`, default OFF.
///
/// So the obvious move when this assertion goes red — flip that flag to
/// `true` and watch the extra holders disappear — is a trade of BOUNDED
/// OVER-REPLICATION (this red) for SILENT DATA LOSS (records that read
/// `NotFound` after being acked). Over-replication is wasted space; a
/// destroyed UTXO is not recoverable. Fix the retention protocol (lease /
/// transfer-then-relinquish) or scope the assertion, never the flag.
/// The server-side rationale lives on
/// `ServerConfig::orphan_cleanup_proof_reclaim_enabled`.
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
                     [still violated after {rounds} census rounds over {elapsed:.1}s]{}",
                    report.holder_errors,
                    txids.len(),
                    report.holder_diagnostics(),
                    // W13 P2-4 — the TRAP travels with the failure a triager
                    // actually reads, not just the doc comment above.
                    report.over_replication_trap_note(),
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

    /// W12 TAIL 2 — an inbound entry whose SOURCE has terminally refused it
    /// is not migration work, and must not hold the convergence gate.
    ///
    /// The two shapes are indistinguishable in `inbound_pending` alone but
    /// they are opposites. A plain entry is waiting for data that is on its
    /// way. A `refused_by_source` entry has been answered
    /// `ERR_MIGRATION_NO_TASKS` by the only node that could satisfy it and
    /// was retained purely as a fail-closed fence over local orphan records
    /// (`inbound_entry_must_be_kept`) — records that only the
    /// committed-handoff-gated orphan cleanup may reclaim. Nothing about it
    /// changes with time.
    ///
    /// Armed scenario 08 @ fc5e5f7 proves the cost of conflating them: node1
    /// held two, re-sent the transfer request 29 times on the 10 s cadence,
    /// was refused all 29 times, and the gate burned its full 300 s budget
    /// even though `masters=4096/4096, handoffs=0, activation=ok` on the
    /// first poll. This is NOT a licence to ignore them — the count is still
    /// reported in the per-node detail and in the timeout message, and the
    /// server gauges it as `teraslab_migration_inbound_refused_retained`.
    ///
    /// W16 — "does not hold the gate" is scoped to the IN-FLIGHT question this
    /// test drives, and only to it. The refused count is now a separate and
    /// FATAL gate condition; see
    /// `a_refused_residue_fails_the_gate_after_one_reheal_window`.
    #[test]
    fn a_terminally_refused_inbound_entry_does_not_hold_the_gate() {
        let refused = serde_json::json!({
            "active_count": 0,
            "failed_count": 0,
            "fenced_shards": 0,
            "inbound_pending": 2,
            "inbound_refused_retained": 2,
        });
        assert_eq!(
            in_flight_inbound_pending(&refused),
            0,
            "two entries their source terminally refused are a fixpoint, not \
             migration work in flight",
        );
        assert_eq!(
            refused_retained_inbound(&refused),
            2,
            "the condition must still be reported, not silently dropped",
        );

        // A genuinely in-flight transfer still holds the gate.
        let live = serde_json::json!({
            "inbound_pending": 3,
            "inbound_refused_retained": 0,
        });
        assert_eq!(in_flight_inbound_pending(&live), 3);

        // Mixed: only the refused subset is discounted.
        let mixed = serde_json::json!({
            "inbound_pending": 5,
            "inbound_refused_retained": 2,
        });
        assert_eq!(in_flight_inbound_pending(&mixed), 3);

        // A pre-W12 server omits the field entirely: fall back to the
        // fail-closed reading (every entry counts), never to zero.
        let legacy = serde_json::json!({ "inbound_pending": 4 });
        assert_eq!(
            in_flight_inbound_pending(&legacy),
            4,
            "a server that cannot report the distinction must keep holding \
             the gate on every entry",
        );
        assert_eq!(refused_retained_inbound(&legacy), 0);

        // A nonsense payload (refused > pending) must not underflow into a
        // huge number and it must not report negative progress.
        let inconsistent = serde_json::json!({
            "inbound_pending": 1,
            "inbound_refused_retained": 9,
        });
        assert_eq!(in_flight_inbound_pending(&inconsistent), 0);
    }

    /// W16/W17 (RED→GREEN) — a terminally-refused residue that outlasts the
    /// grace its OWN CLASS earns FAILS the convergence gate. It is not
    /// convergence.
    ///
    /// Discounting it from `in_flight_inbound_pending` is right — no migration
    /// is running — and it is what stops armed 09's nine entries from wedging
    /// every scenario for the whole budget. But letting the gate then return
    /// `Ok` would be reporting something false: those shards are FENCED, and
    /// the transfer is not slow, it is UNCOMPLETABLE.
    ///
    /// W17 — the grace is per class, because the two classes cost different
    /// amounts to earn and resolve on different paths. A HOLDER-terminal mark
    /// already costs six consecutive refusals (60 s at the 10 s transfer-request
    /// interval), so a further [`REFUSED_HOLDER_TERMINAL_GRACE`] means the gate
    /// fails on "refused six times AND nothing re-planned it for a whole
    /// re-heal window after that".
    #[test]
    fn a_holder_terminal_residue_fails_the_gate_after_one_reheal_window() {
        let t0 = std::time::Instant::now();
        let none = RefusedResidueWindows::default();
        let seen = RefusedResidue {
            holder_terminal: 9,
            orphan: 0,
        };

        // Nothing observed: no window, never fatal.
        assert_eq!(
            note_refused_residue(none, RefusedResidue::default(), t0),
            none,
        );
        assert_eq!(
            refused_residue_fatal_class(none, t0 + REFUSED_ORPHAN_GRACE * 10),
            None,
        );

        // First observation opens the window and does NOT fail immediately —
        // the mark is revocable, so an instant verdict would fail a run whose
        // re-heal was about to re-plan the handoff.
        let opened = note_refused_residue(none, seen, t0);
        assert_eq!(
            opened.holder_terminal,
            Some(t0),
            "the window starts when the residue is FIRST seen",
        );
        assert_eq!(
            opened.orphan, None,
            "a class that was never seen must not open a window",
        );
        assert_eq!(
            refused_residue_fatal_class(opened, t0),
            None,
            "an instant verdict would leave no room for a late re-plan",
        );
        assert_eq!(
            refused_residue_fatal_class(opened, t0 + REFUSED_HOLDER_TERMINAL_GRACE / 2),
            None,
            "half a re-heal window is not a full one",
        );

        // A later poll that still sees the residue must NOT restart the clock,
        // or the gate could never reach a verdict at a 50 ms poll cadence.
        let still = note_refused_residue(opened, seen, t0 + REFUSED_HOLDER_TERMINAL_GRACE / 2);
        assert_eq!(
            still, opened,
            "a continuing residue keeps its window, per class",
        );

        assert_eq!(
            refused_residue_fatal_class(still, t0 + REFUSED_HOLDER_TERMINAL_GRACE),
            Some(RefusedResidueClass::HolderTerminal),
            "refused six consecutive rounds AND unchanged for a further \
             {REFUSED_HOLDER_TERMINAL_GRACE:?} is terminal",
        );
    }

    /// W17 (RED→GREEN) — the ORPHAN class must not be judged by the holder
    /// class's clock, and the gate's stated guarantee must be true of it.
    ///
    /// `drop_refused_inbound` marks a `KeepOrphan` entry on its FIRST refusal —
    /// W12 behaviour that predates the six-round streak entirely — so the
    /// wave-16 doc's *"the server-side mark already costs six consecutive
    /// refusals … so a gate can only fail on this after ~90 s"* was simply
    /// false for half of what the gauge counted. CI 32668963874 failed on
    /// twelve such entries after three refusal rounds and 30 s.
    ///
    /// An orphan residue is also RESOLVABLE, on a slower path: orphan cleanup
    /// reclaims the records and the ordinary prune then retires the entry. That
    /// pass is rate-limited to one per `EVENT_ORPHAN_CLEANUP_MIN_INTERVAL`
    /// (60 s), so a 30 s verdict could fail a residue that was about to clear
    /// on its own. The grace has to cover the cleanup cadence, not the refusal
    /// cadence.
    #[test]
    fn an_orphan_residue_gets_the_orphan_cleanup_cadence_not_the_reheal_window() {
        assert!(
            REFUSED_ORPHAN_GRACE > REFUSED_HOLDER_TERMINAL_GRACE,
            "an orphan entry is marked on its FIRST refusal and clears on a \
             60 s cleanup cadence — judging it on the holder clock is the \
             wave-16 false positive",
        );
        let t0 = std::time::Instant::now();
        let seen = RefusedResidue {
            holder_terminal: 0,
            orphan: 12,
        };
        let opened = note_refused_residue(RefusedResidueWindows::default(), seen, t0);
        assert_eq!(opened.orphan, Some(t0));
        assert_eq!(opened.holder_terminal, None);

        assert_eq!(
            refused_residue_fatal_class(opened, t0 + REFUSED_HOLDER_TERMINAL_GRACE),
            None,
            "the holder grace says nothing about an orphan residue — this is \
             exactly the run the wave-16 gate failed at 30 s",
        );
        assert_eq!(
            refused_residue_fatal_class(opened, t0 + REFUSED_ORPHAN_GRACE),
            Some(RefusedResidueClass::Orphan),
            "…but a genuinely terminal orphan residue must still fail — the \
             gate is narrowed to the honest window, not until nothing fails",
        );
    }

    /// W17 — each class runs its own clock, and the first to expire decides.
    #[test]
    fn the_two_classes_track_independent_windows() {
        let t0 = std::time::Instant::now();
        // The orphan residue is seen first, the holder residue much later.
        let w = note_refused_residue(
            RefusedResidueWindows::default(),
            RefusedResidue {
                holder_terminal: 0,
                orphan: 4,
            },
            t0,
        );
        let holder_seen_at = t0 + REFUSED_HOLDER_TERMINAL_GRACE;
        let w = note_refused_residue(
            w,
            RefusedResidue {
                holder_terminal: 1,
                orphan: 4,
            },
            holder_seen_at,
        );
        assert_eq!(w.orphan, Some(t0), "the orphan window is not restarted");
        assert_eq!(w.holder_terminal, Some(holder_seen_at));

        // The holder window expires first even though it opened later.
        assert_eq!(
            refused_residue_fatal_class(w, holder_seen_at + REFUSED_HOLDER_TERMINAL_GRACE),
            Some(RefusedResidueClass::HolderTerminal),
        );
    }

    /// W16/W17 (RED→GREEN) — a residue that CLEARS closes its window, per
    /// class, so a later one starts a fresh full grace period.
    ///
    /// The mark is revoked by any evidence of real work (a batch arriving, a
    /// task or re-registration, a re-park, or the source matching the request
    /// in a later round), and an ORPHAN mark also clears when orphan cleanup
    /// reclaims the records holding its fence up. Carrying a half-spent window
    /// across that revocation would let a cluster that demonstrably recovered
    /// be failed by the ghost of an earlier residue.
    #[test]
    fn a_residue_that_clears_starts_a_fresh_window() {
        let t0 = std::time::Instant::now();
        let seen = RefusedResidue {
            holder_terminal: 0,
            orphan: 2,
        };
        let opened = note_refused_residue(RefusedResidueWindows::default(), seen, t0);
        assert_eq!(opened.orphan, Some(t0));

        // Orphan cleanup reclaimed the records and the prune retired the
        // entry: the gate must forget the window.
        let cleared = note_refused_residue(
            opened,
            RefusedResidue::default(),
            t0 + REFUSED_ORPHAN_GRACE / 2,
        );
        assert_eq!(
            cleared,
            RefusedResidueWindows::default(),
            "zero residue closes the window",
        );
        assert_eq!(
            refused_residue_fatal_class(cleared, t0 + REFUSED_ORPHAN_GRACE * 5),
            None,
        );

        // A residue appearing again later is judged on its OWN window.
        let reopened_at = t0 + REFUSED_ORPHAN_GRACE;
        let reopened = note_refused_residue(cleared, seen, reopened_at);
        assert_eq!(reopened.orphan, Some(reopened_at));
        assert_eq!(
            refused_residue_fatal_class(reopened, reopened_at + REFUSED_ORPHAN_GRACE / 2),
            None,
            "the new residue must get a full window, not the remainder of the old one",
        );
        assert_eq!(
            refused_residue_fatal_class(reopened, reopened_at + REFUSED_ORPHAN_GRACE),
            Some(RefusedResidueClass::Orphan),
        );
    }

    /// W17 — a server that cannot NAME the class must be judged by the LONGER
    /// grace, never the shorter one.
    ///
    /// `inbound_refused_retained` predates the split. Attributing an
    /// unclassifiable residue to the holder class would resurrect the wave-16
    /// false positive against exactly the servers that cannot defend
    /// themselves; attributing it to the orphan class keeps the residue
    /// visible and gives it the window the slower resolution path needs.
    #[test]
    fn an_unclassifiable_residue_is_judged_by_the_longer_grace() {
        let legacy = serde_json::json!({
            "inbound_pending": 5,
            "inbound_refused_retained": 5,
        });
        let counts = refused_residue_counts(&legacy);
        assert_eq!(counts.total(), 5, "the residue must not be lost");
        assert_eq!(
            counts,
            RefusedResidue {
                holder_terminal: 0,
                orphan: 5,
            },
            "an unnamed class gets the longer grace",
        );
        // …and the classified form is read as sent.
        let classified = serde_json::json!({
            "inbound_pending": 5,
            "inbound_refused_retained": 5,
            "inbound_refused_retained_holder_terminal": 2,
            "inbound_refused_retained_orphan": 3,
        });
        assert_eq!(
            refused_residue_counts(&classified),
            RefusedResidue {
                holder_terminal: 2,
                orphan: 3,
            },
        );
        // A server with no residue at all reports nothing in either class.
        assert_eq!(
            refused_residue_counts(&serde_json::json!({"inbound_pending": 0})),
            RefusedResidue::default(),
        );
    }

    /// W17 review P2-5 (RED→GREEN) — the parse must be FAIL-CLOSED on schema
    /// drift, which its own doc claims and the first cut did not deliver.
    ///
    /// The classes were read independently of the total, so a server reporting
    /// `total=5, holder=0, orphan=0` — any partial rollout, any future rename,
    /// any bug in the split — yielded a residue whose `.total()` is ZERO. That
    /// satisfies the gates' `total_refused_retained.total() == 0` condition: a
    /// stuck cluster becomes a GREEN verdict, which is the one outcome the
    /// residue machinery exists to prevent.
    ///
    /// `inbound_refused_retained` is the authority. The orphan half is DERIVED
    /// from it, so the two classes always sum to the total, and any
    /// inconsistency lands entirely in the class with the longer grace.
    #[test]
    fn an_inconsistent_split_is_never_read_as_an_absent_residue() {
        // The exact drift: total present, both halves zero.
        let drifted = serde_json::json!({
            "inbound_pending": 5,
            "inbound_refused_retained": 5,
            "inbound_refused_retained_holder_terminal": 0,
            "inbound_refused_retained_orphan": 0,
        });
        let counts = refused_residue_counts(&drifted);
        assert_eq!(
            counts.total(),
            5,
            "the residue must survive a split that does not add up — reading \
             it as zero is a green verdict over a wedged cluster",
        );
        assert_eq!(counts.orphan, 5, "the remainder takes the longer grace");

        // Halves that OVERSTATE the total are clamped the same way: the total
        // is the authority, and the holder half can never exceed it.
        let overstated = serde_json::json!({
            "inbound_refused_retained": 2,
            "inbound_refused_retained_holder_terminal": 9,
            "inbound_refused_retained_orphan": 0,
        });
        let counts = refused_residue_counts(&overstated);
        assert_eq!(counts.total(), 2, "the total is the authority");
        assert_eq!(counts.holder_terminal, 2);
        assert_eq!(counts.orphan, 0);

        // And the honest case is unchanged.
        let consistent = serde_json::json!({
            "inbound_refused_retained": 5,
            "inbound_refused_retained_holder_terminal": 2,
            "inbound_refused_retained_orphan": 3,
        });
        assert_eq!(
            refused_residue_counts(&consistent),
            RefusedResidue {
                holder_terminal: 2,
                orphan: 3,
            },
        );
    }

    /// W16/W17 — the failure has to EXPLAIN itself, and it must not claim a
    /// guarantee it did not enforce.
    ///
    /// The wave-16 message asserted a six-round streak over every entry it
    /// counted; the gate now demands the class it names, and the ORPHAN
    /// variant states what is actually true of that class — marked on the
    /// first refusal, and held open by records orphan cleanup has not
    /// reclaimed.
    #[test]
    fn the_residue_diagnostic_names_the_class_it_actually_enforced() {
        let details = vec![
            "node1:size=3,ver=7,masters=1365".to_string(),
            "node1:inbound-refused-retained=9(holder-terminal=9,orphan=0)".to_string(),
            "node2:inbound-refused-retained=1(holder-terminal=1,orphan=0)".to_string(),
        ];
        let msg = refused_residue_error(
            RefusedResidueClass::HolderTerminal,
            RefusedResidue {
                holder_terminal: 10,
                orphan: 0,
            },
            &details,
        );
        assert!(msg.contains("node1:inbound-refused-retained=9"), "{msg}");
        assert!(msg.contains("node2:inbound-refused-retained=1"), "{msg}");
        assert!(
            !msg.contains("masters=1365"),
            "unrelated per-node detail belongs to the surrounding dump, not to \
             the residue block: {msg}",
        );
        assert!(
            msg.contains("FENCED"),
            "the availability fact must be stated: {msg}"
        );
        assert!(
            msg.contains("This is not convergence"),
            "the verdict must be explicit, not inferable: {msg}",
        );
        assert!(
            msg.contains("REFUSED_HOLDER_TERMINAL_ROUNDS"),
            "the holder variant names the streak it enforced: {msg}",
        );

        // The orphan variant must NOT claim the streak — that claim is what
        // wave 16 got wrong.
        let orphan = refused_residue_error(
            RefusedResidueClass::Orphan,
            RefusedResidue {
                holder_terminal: 0,
                orphan: 12,
            },
            &["node3:inbound-refused-retained=12(holder-terminal=0,orphan=12)".to_string()],
        );
        assert!(
            !orphan.contains("REFUSED_HOLDER_TERMINAL_ROUNDS"),
            "a KeepOrphan entry is marked on its FIRST refusal — claiming a \
             six-round streak over it is the wave-16 defect: {orphan}",
        );
        assert!(
            orphan.contains("orphan cleanup"),
            "the orphan variant must name the path that would have resolved \
             it: {orphan}",
        );
        assert!(orphan.contains("12 inbound entries"), "{orphan}");

        // Singular/plural, because a one-entry residue is the common case and
        // "1 inbound entries ... their own source" reads like a formatting bug
        // in a failure message someone has to trust.
        let one = refused_residue_error(
            RefusedResidueClass::Orphan,
            RefusedResidue {
                holder_terminal: 0,
                orphan: 1,
            },
            &["node3:inbound-refused-retained=1(holder-terminal=0,orphan=1)".to_string()],
        );
        assert!(one.contains("1 inbound entry"), "{one}");
        assert!(one.contains("its own source"), "{one}");
    }

    /// Task #75 — a degraded `/status` payload renders the wedge
    /// fingerprint, and its missing shard fields keep parsing exactly like
    /// the gate-holding zero view (never as converged).
    #[test]
    fn degraded_status_renders_marker_and_holds_gates() {
        let degraded = serde_json::json!({
            "node_id": 2,
            "shard_table_version": 7,
            "topology_term": 7,
            "status_degraded": true,
            "table_lock_unavailable": true,
            "migration_lock_unavailable": false,
            "addrs_lock_unavailable": false,
            "event_loop": { "last_beat_age_ms": 45_000, "phase": "commit_drain" },
        });
        assert_eq!(
            status_degraded_marker(&degraded).as_deref(),
            Some("DEGRADED(locks=T--,loop_stall=45000ms)"),
        );

        // Gate semantics: the missing fields parse as the same
        // (serving=0, target=0) view the activation gate already holds on.
        assert!(degraded["cluster_size"].as_u64().is_none());
        let serving = degraded["master_shard_count"].as_u64().unwrap_or(0);
        let target = degraded["target_master_shard_count"].as_u64().unwrap_or(0);
        let views = [view(2, serving, target)];
        let reason = shard_activation_gate_reason(&views, 3)
            .expect("a degraded node must hold the activation gate");
        assert!(
            reason.contains("node2 has activated no master shards"),
            "unexpected gate reason: {reason}"
        );

        // A healthy payload gets no marker.
        let healthy = serde_json::json!({
            "cluster_size": 3,
            "master_shard_count": 1366,
        });
        assert_eq!(status_degraded_marker(&healthy), None);
    }

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

    /// Scenario 09 diagnosis — the target-table divergence detector: a
    /// cluster whose per-node ACTIVATED target counts do not sum to 4096
    /// holds divergent tables (double-targeted or orphaned shards), which
    /// migration progress can never settle.
    #[test]
    fn divergent_target_sum_flags_only_a_wrong_total() {
        // Agreeing tables: one target master per shard, sum exactly 4096.
        let agreed = vec![
            view(1, 1366, 1366),
            view(2, 1365, 1365),
            view(3, 1365, 1365),
        ];
        assert_eq!(divergent_target_sum(&agreed), None);

        // Double-targeted shards: two nodes both target the same shards.
        let doubled = vec![
            view(1, 1366, 1366),
            view(2, 1365, 1365),
            view(3, 1365, 1733),
        ];
        assert_eq!(divergent_target_sum(&doubled), Some(4464));

        // Orphaned shards: nobody targets part of the range.
        let orphaned = vec![view(1, 1366, 1366), view(2, 1365, 1365), view(3, 1365, 0)];
        assert_eq!(divergent_target_sum(&orphaned), Some(2731));
    }

    /// The suspicion clock is scoped to the agreed shard_table_version: a
    /// transient wrong sum at ver=8, a gate-closed reactivation window of
    /// any length, and a fresh transient at ver=9 are two UNRELATED samples
    /// and must re-arm the clock — never combine into one immediate
    /// hard-fail. Genuine same-version persistence still fires, and a
    /// correct sum clears the suspicion entirely. Driven with synthetic
    /// instants so no assertion depends on real elapsed time.
    #[test]
    fn divergence_suspicion_clock_is_version_scoped() {
        let persistence = Duration::from_secs(5);
        let t0 = std::time::Instant::now();
        let mut suspicion: Option<(std::time::Instant, u64)> = None;

        // First wrong sample at ver=8 arms the clock but never fires.
        assert!(!divergence_fail_due(
            &mut suspicion,
            t0,
            8,
            true,
            persistence
        ));
        assert_eq!(suspicion.map(|(_, v)| v), Some(8));

        // 6s later (>= persistence) the next eligible wrong sample arrives
        // at ver=9 — a NEW version: re-arm, do not fail on two unrelated
        // transients straddling a reactivation.
        let t1 = t0 + Duration::from_secs(6);
        assert!(!divergence_fail_due(
            &mut suspicion,
            t1,
            9,
            true,
            persistence
        ));
        assert_eq!(
            suspicion,
            Some((t1, 9)),
            "a wrong sum at a new agreed version must restart the clock"
        );

        // Still wrong at ver=9 but under the persistence window: no fail.
        let t2 = t1 + Duration::from_secs(4);
        assert!(!divergence_fail_due(
            &mut suspicion,
            t2,
            9,
            true,
            persistence
        ));

        // Persistently wrong at the SAME version past the window: fail.
        let t3 = t1 + persistence;
        assert!(
            divergence_fail_due(&mut suspicion, t3, 9, true, persistence),
            "same-version divergence persisting past the window must fire"
        );

        // A correct sum clears the suspicion; the next wrong sample at the
        // same version starts over instead of firing instantly.
        assert!(!divergence_fail_due(
            &mut suspicion,
            t3,
            9,
            false,
            persistence
        ));
        assert_eq!(suspicion, None, "a correct sum must clear the suspicion");
        let t4 = t3 + Duration::from_secs(60);
        assert!(
            !divergence_fail_due(&mut suspicion, t4, 9, true, persistence),
            "after a clear, a fresh wrong sample re-arms rather than fires"
        );
        assert_eq!(suspicion.map(|(_, v)| v), Some(9));
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
mod master_overlap_diagnostic_tests {
    use super::*;

    /// The run-31904708491 scenario-09 shape in miniature: shards mastered by
    /// two nodes at once must be NAMED with their claimants, and shards
    /// mastered by nobody must be named as orphaned.
    #[test]
    fn overlapping_and_orphaned_shards_are_named() {
        // node1 masters 0..=2, node2 masters 2..=4, node3 masters 5 — shard 2
        // is double-mastered and every shard >= 6 is orphaned.
        let per_node = vec![
            (1u32, vec![0u16, 1, 2]),
            (2u32, vec![2u16, 3, 4]),
            (3u32, vec![5u16]),
        ];
        let d = master_overlap_diagnostic(&per_node, &[]);
        assert!(
            d.contains("overlapping=1 [2(n1+n2)]"),
            "the double-mastered shard must be named with both claimants: {d}"
        );
        assert!(
            d.contains("orphaned=4090"),
            "4096-6 shards are orphaned: {d}"
        );
        // The orphan list starts at the first unclaimed shard and is capped.
        assert!(d.contains("[6, 7,"), "orphan IDs must be listed: {d}");
        assert!(
            d.contains(&format!(" +{} more", 4090 - MASTER_OVERLAP_DIAGNOSTIC_CAP)),
            "the orphan list must be capped with a remainder count: {d}"
        );
    }

    /// Nodes whose master set could not be fetched are NAMED, because every
    /// shard they master shows up as "orphaned" — the reader must see the
    /// inflation's source instead of trusting the orphan count.
    #[test]
    fn unfetched_nodes_are_named_next_to_the_inflated_orphan_count() {
        let per_node = vec![(1u32, vec![0u16, 1])];
        let d = master_overlap_diagnostic(&per_node, &[2, 3]);
        assert!(
            d.contains("unfetched=[n2, n3] (their masters count as orphaned)"),
            "missing nodes must be named with the inflation caveat: {d}"
        );
        assert!(d.contains("orphaned=4094"), "{d}");
        // No unfetched nodes -> no suffix at all.
        let clean = master_overlap_diagnostic(&per_node, &[]);
        assert!(
            !clean.contains("unfetched"),
            "no suffix when every node answered: {clean}"
        );
    }

    /// A fully-claimed, non-overlapping table produces empty lists — the
    /// diagnostic must not invent divergence.
    #[test]
    fn a_clean_partition_reports_no_overlap_and_no_orphans() {
        let mut per_node: Vec<(u32, Vec<u16>)> = vec![(1, vec![]), (2, vec![])];
        for s in 0..4096u16 {
            per_node[(s % 2) as usize].1.push(s);
        }
        let d = master_overlap_diagnostic(&per_node, &[]);
        assert_eq!(
            d, "overlapping=0 [], orphaned=0 []",
            "a clean table must produce empty lists"
        );
    }

    /// The overlap list itself is capped at the configured maximum with a
    /// remainder count (the observed wedge had 368 overlapping shards — the
    /// error must stay one line, not 368).
    #[test]
    fn the_overlap_list_is_capped() {
        let all: Vec<u16> = (0..4096).collect();
        // Every shard is claimed by both nodes.
        let per_node = vec![(1u32, all.clone()), (2u32, all)];
        let d = master_overlap_diagnostic(&per_node, &[]);
        assert!(d.contains("overlapping=4096"), "{d}");
        assert!(
            d.contains(&format!(" +{} more", 4096 - MASTER_OVERLAP_DIAGNOSTIC_CAP)),
            "the overlap list must be capped with a remainder count: {d}"
        );
        let named = d.matches("(n1+n2)").count();
        assert_eq!(
            named, MASTER_OVERLAP_DIAGNOSTIC_CAP,
            "exactly the cap's worth of shard IDs may be printed: {d}"
        );
    }
}

#[cfg(test)]
mod master_divergence_block_tests {
    use super::*;

    /// Build a per-node census that partitions 0..4096 across `nodes`, then
    /// lets the caller perturb it.
    fn clean_census(nodes: &[u32]) -> Vec<(u32, Vec<u16>)> {
        let mut per_node: Vec<(u32, Vec<u16>)> = nodes.iter().map(|&n| (n, Vec::new())).collect();
        for s in 0..4096u16 {
            per_node[s as usize % nodes.len()].1.push(s);
        }
        per_node
    }

    /// The EXCESS shape (the 4103/4096 case that could name its shards):
    /// the direction is stated up front, the surplus is counted, and the
    /// double-claimed shards still carry their claimant nodes.
    #[test]
    fn an_excess_census_names_the_direction_the_count_and_the_claimants() {
        let mut per_node = clean_census(&[1, 3, 4]);
        // node3 additionally claims two shards node1 already masters.
        per_node[1].1.push(0);
        per_node[1].1.push(3);
        let block = master_divergence_block(&per_node, &[], &[1, 3, 4]);
        assert!(
            block.starts_with("MASTER CENSUS EXCESS: 2 claim(s) over 4096"),
            "the direction and surplus must lead the block: {block}"
        );
        assert!(
            block.contains("polled=[n1, n3, n4]"),
            "the polled set must be named: {block}"
        );
        assert!(
            block.contains("0(n1+n3)") && block.contains("3(n1+n3)"),
            "the dual-claimed shards must name both claimants: {block}"
        );
    }

    /// The DEFICIT shape (the 4094/4096 case that could NOT name its shards).
    /// It must be as legible as the excess: direction, count, the orphaned
    /// shard IDs, and the polled set that defines "nobody".
    #[test]
    fn a_deficit_census_names_the_direction_the_count_and_the_orphans() {
        let mut per_node = clean_census(&[1, 3, 4]);
        // Drop shard 7 from node2's slot and shard 1234 from wherever it sits.
        for (_, shards) in per_node.iter_mut() {
            shards.retain(|&s| s != 7 && s != 1234);
        }
        let block = master_divergence_block(&per_node, &[], &[1, 3, 4]);
        assert!(
            block.starts_with(
                "MASTER CENSUS DEFICIT: 2 shard(s) mastered by none of the polled nodes"
            ),
            "the direction and shortfall must lead the block: {block}"
        );
        assert!(
            block.contains("orphaned=2 [7, 1234]"),
            "the orphaned shard IDs must be named: {block}"
        );
        assert!(
            block.contains("polled=[n1, n3, n4]"),
            "the polled set defines what 'nobody' means: {block}"
        );
    }

    /// A census whose surplus exactly cancels its shortfall sums to 4096 and
    /// therefore slips through the gate's sum check. It is still divergent,
    /// and the block must say so instead of printing a bare "0".
    #[test]
    fn a_balanced_census_is_still_reported_as_divergent() {
        let mut per_node = clean_census(&[1, 2]);
        for (_, shards) in per_node.iter_mut() {
            shards.retain(|&s| s != 7);
        }
        per_node[1].1.push(0); // node2 dual-claims a node1 shard
        let block = master_divergence_block(&per_node, &[], &[1, 2]);
        assert!(
            block.starts_with("MASTER CENSUS BALANCED-BUT-DIVERGENT"),
            "a cancelling census must not read as clean: {block}"
        );
        assert!(
            block.contains("overlapping=1 [0(n1+n2)]") && block.contains("orphaned=1 [7]"),
            "both halves must still be named: {block}"
        );
    }

    /// Every shard claimed exactly once by a polled node: nothing to report
    /// beyond the census being settled. (Reached when the gate times out on
    /// migration activity rather than on the master sum, or when the state
    /// moved between the gate's last poll and this second fetch.)
    #[test]
    fn a_settled_census_says_so() {
        let per_node = clean_census(&[1, 2, 3]);
        let block = master_divergence_block(&per_node, &[], &[1, 2, 3]);
        assert!(
            block.starts_with("MASTER CENSUS SETTLED"),
            "a clean census must be reported as clean: {block}"
        );
    }

    /// Nodes that did not answer are named, and the caveat travels with the
    /// orphan count they inflate — a deficit built entirely out of unfetched
    /// nodes must not read as real orphaning.
    #[test]
    fn unfetched_nodes_are_named_inside_the_block() {
        let per_node = vec![(1u32, vec![0u16, 1])];
        let block = master_divergence_block(&per_node, &[3, 4], &[1, 3, 4]);
        assert!(
            block.contains("unfetched=[n3, n4] (their masters count as orphaned)"),
            "the inflation source must be named: {block}"
        );
        assert!(block.starts_with("MASTER CENSUS DEFICIT"), "{block}");
    }

    /// No node answered at all: there is no census to report, and inventing
    /// "4096 orphans" would be a lie about the cluster rather than about the
    /// poll. Say the poll failed and name who was asked.
    #[test]
    fn an_empty_census_reports_the_failed_poll_not_4096_orphans() {
        let block = master_divergence_block(&[], &[1, 2, 3], &[1, 2, 3]);
        assert!(
            block.contains("master sets unavailable"),
            "an all-unfetched poll must report itself as such: {block}"
        );
        assert!(
            !block.contains("orphaned=4096"),
            "a failed poll must not be rendered as total orphaning: {block}"
        );
        assert!(block.contains("polled=[n1, n2, n3]"), "{block}");
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

    /// W13 round-2 review P2-4 — a CI triager reads the assertion FAILURE, not
    /// the source. When the census fails with records at THREE holders, the
    /// panic itself must say that over-replication is the safe state and that
    /// arming `orphan_cleanup_proof_reclaim_enabled` to clear it is the
    /// configuration that destroyed four acked records.
    ///
    /// Under-replication (fewer than RF holders) is a different failure with a
    /// different fix, so it must NOT carry the note — otherwise the warning
    /// becomes noise that gets skimmed past.
    #[test]
    fn over_replication_failure_carries_the_orphan_proof_trap_note() {
        // 3 holders under RF=2 — the default-15/17 shape.
        let mut over = ReplicationCheckReport::new(3);
        over.record_holder_error(&[0x11u8; 32], &[0, 1, 2]);
        let note = over.over_replication_trap_note();
        assert!(
            note.contains("orphan_cleanup_proof_reclaim_enabled"),
            "the note must name the flag a triager would otherwise reach for: {note}",
        );
        assert!(
            note.contains("SAFE"),
            "the note must say the over-replication red is the safe state: {note}",
        );
        assert!(
            note.contains("DELETED FOUR ACKED RECORDS"),
            "the note must state the cost of the obvious 'fix': {note}",
        );

        // 1 holder under RF=2 — under-replication, a different problem.
        let mut under = ReplicationCheckReport::new(3);
        under.record_holder_error(&[0x22u8; 32], &[0]);
        assert!(
            under.over_replication_trap_note().is_empty(),
            "an under-replication failure must not carry the over-replication note",
        );

        // A clean report says nothing either.
        let clean = ReplicationCheckReport::new(3);
        assert!(clean.over_replication_trap_note().is_empty());
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
mod migration_failure_class_tests {
    use super::*;
    use teraslab::cluster::migration::KeyDiagnosis;

    fn txid(n: u8) -> [u8; 32] {
        [n; 32]
    }

    fn corpus(n: u8) -> Vec<[u8; 32]> {
        (0..n).map(txid).collect()
    }

    /// A `KeyDiagnosis` for one node's view of a record, varying only the
    /// dimension these tests care about: does this node hold the data.
    fn holder_view(node_id: u64, master_id: u64, has_local_data: bool) -> KeyDiagnosis {
        KeyDiagnosis {
            shard: 11,
            this_node_id: node_id,
            local_view_canonical_master_id: master_id,
            has_local_data,
            is_local_master_of_shard: node_id == master_id,
            has_pending_inbound: false,
            is_shard_fenced: false,
            is_migrating_shard: false,
            topology_epoch: 4,
            local_view_effective_master_id: master_id,
            is_serving_fenced: false,
        }
    }

    /// The run-31946515845 scenario-08 shape: the master route served every
    /// record (`master_failed=0/1200`) and the ONLY failing class was
    /// under-replication (`under_replicated=1/50`). The under-replicated
    /// sample index must reach the dump — the predecessor built the dump
    /// from the master-route indices alone and reported `n=0`.
    #[test]
    fn an_under_replicated_only_timeout_still_names_records() {
        let txids = corpus(20);
        let classes = select_migration_failure_samples(&txids, &[], &[7], 32);

        assert_eq!(classes.len(), 2, "both classes must always be reported");
        assert_eq!(classes[0].0, "master_failed");
        assert!(
            classes[0].1.is_empty(),
            "no master-route failure in this shape: {:?}",
            classes[0].1.len()
        );
        assert_eq!(classes[1].0, "under_replicated");
        assert_eq!(
            classes[1].1,
            vec![txid(7)],
            "the under-replicated sample index must survive into the dump input"
        );
    }

    /// Both classes failing: each is reported under its own label, and the
    /// master-route sample is not polluted with under-replicated records.
    #[test]
    fn the_two_classes_are_reported_separately() {
        let txids = corpus(20);
        let classes = select_migration_failure_samples(&txids, &[1, 2], &[15, 16], 32);

        assert_eq!(classes[0].1, vec![txid(1), txid(2)]);
        assert_eq!(classes[1].1, vec![txid(15), txid(16)]);
    }

    /// A record that is both master-failed AND under-replicated is reported
    /// once, under the stronger symptom, so the second section spends its
    /// cap on records the first does not already cover.
    #[test]
    fn a_record_in_both_classes_is_reported_only_as_master_failed() {
        let txids = corpus(20);
        let classes = select_migration_failure_samples(&txids, &[3], &[3, 9], 32);

        assert_eq!(classes[0].1, vec![txid(3)]);
        assert_eq!(
            classes[1].1,
            vec![txid(9)],
            "the duplicate must be dropped from the second section, not the unique record"
        );
    }

    /// The cap applies per class, so a flood of master-route failures can
    /// never squeeze the under-replicated evidence out of the dump.
    #[test]
    fn each_class_is_capped_independently() {
        let txids = corpus(40);
        let master: Vec<usize> = (0..20).collect();
        let under: Vec<usize> = (20..40).collect();
        let classes = select_migration_failure_samples(&txids, &master, &under, 4);

        assert_eq!(classes[0].1, vec![txid(0), txid(1), txid(2), txid(3)]);
        assert_eq!(classes[1].1, vec![txid(20), txid(21), txid(22), txid(23)]);
    }

    /// Indices are filtered, not indexed blindly: a stale index must not
    /// panic the diagnostic path that is only reached when a test is
    /// ALREADY failing.
    #[test]
    fn out_of_range_indices_are_skipped() {
        let txids = corpus(3);
        let classes = select_migration_failure_samples(&txids, &[99], &[1, 42], 32);

        assert!(classes[0].1.is_empty(), "{:?}", classes[0].1.len());
        assert_eq!(classes[1].1, vec![txid(1)]);
    }

    /// End-to-end through the formatter: an under-replication-only timeout
    /// renders a labelled section carrying the rich per-node holder columns
    /// (which nodes hold the record, which do not), and the clean class is
    /// still shown as `n=0` rather than omitted.
    #[test]
    fn sections_label_the_classes_and_carry_the_rich_holder_columns() {
        let txids = corpus(10);
        let classes = select_migration_failure_samples(&txids, &[], &[2], 32);

        let node_nums = vec![1u32, 2];
        let responses = vec![
            Ok(vec![holder_view(1, 1, true)]),
            Ok(vec![holder_view(2, 1, false)]),
        ];
        let under_dump = format_master_failed_diagnostic(&classes[1].1, &node_nums, &responses);

        let sections = format_migration_failure_sections(&[
            (classes[0].0, classes[0].1.len(), String::new()),
            (classes[1].0, classes[1].1.len(), under_dump),
        ]);

        assert!(
            sections.contains("master_failed first_failures (rich, n=0): none"),
            "the clean class must still be named: {sections}"
        );
        assert!(
            sections.contains("under_replicated first_failures (rich, n=1):"),
            "the failing class must be labelled distinctly: {sections}"
        );
        assert!(
            sections.contains("txid=020202020202"),
            "the under-replicated record must be named: {sections}"
        );
        assert!(
            sections.contains("holders=[n1:Y, n2:N]"),
            "the dump must show which nodes hold the record: {sections}"
        );
    }
}

#[cfg(test)]
mod spend_retry_tests {
    use super::*;
    use teraslab::protocol::opcodes::{
        ERR_ALREADY_SPENT, ERR_MIGRATION_IN_PROGRESS, ERR_NO_QUORUM, ERR_STALE_EPOCH,
    };

    fn err(item_index: u32, code: u16) -> BatchItemError {
        BatchItemError {
            item_index,
            code,
            data: Vec::new(),
        }
    }

    #[test]
    fn no_errors_completes_the_batch() {
        assert_eq!(
            classify_spend_attempt(&[], 50, false),
            SpendAttemptOutcome::Done
        );
    }

    /// A degraded ack cannot confirm durability for the items it reports as
    /// successful either, so the whole batch is re-sent — the same stance
    /// `split_partial_successes` takes for seeds.
    #[test]
    fn a_degraded_ack_retries_the_whole_batch() {
        assert_eq!(
            classify_spend_attempt(&[err(1, ERR_MIGRATION_IN_PROGRESS)], 4, true),
            SpendAttemptOutcome::Retry(vec![0, 1, 2, 3]),
        );
        assert_eq!(
            classify_spend_attempt(&[], 3, true),
            SpendAttemptOutcome::Retry(vec![0, 1, 2]),
            "a degraded ack with no per-item errors still confirms nothing",
        );
    }

    /// Degradation must not outrank a real failure.
    #[test]
    fn a_degraded_ack_still_surfaces_a_non_transient_code() {
        assert_eq!(
            classify_spend_attempt(&[err(0, ERR_ALREADY_SPENT)], 4, true),
            SpendAttemptOutcome::Fatal(ERR_ALREADY_SPENT),
        );
    }

    /// The armed-04 failure: Test 4.5 spent 200 UTXOs while a shard handoff
    /// fence was up and counted the 5 resulting `ERR_MIGRATION_IN_PROGRESS`
    /// as hard failures. Code 19 is transient by the shared policy and only
    /// the fenced items are re-sent.
    #[test]
    fn migration_in_progress_retries_only_the_failed_items() {
        let errors = [err(3, ERR_MIGRATION_IN_PROGRESS), err(7, ERR_STALE_EPOCH)];
        assert_eq!(
            classify_spend_attempt(&errors, 50, false),
            SpendAttemptOutcome::Retry(vec![3, 7]),
            "only the fenced items are re-sent — items that landed must not be re-spent"
        );
    }

    /// A retry must never launder a real failure. `ERR_ALREADY_SPENT` is a
    /// genuine double-spend signal and stays terminal even when it arrives
    /// alongside transient siblings.
    #[test]
    fn a_non_transient_code_is_terminal_even_when_mixed_with_transient_ones() {
        let errors = [err(1, ERR_MIGRATION_IN_PROGRESS), err(2, ERR_ALREADY_SPENT)];
        assert_eq!(
            classify_spend_attempt(&errors, 50, false),
            SpendAttemptOutcome::Fatal(ERR_ALREADY_SPENT),
        );
    }

    /// An index the request cannot be mapped onto means nothing is known to
    /// have landed: re-send everything (a spend is idempotent for identical
    /// `spending_data`).
    #[test]
    fn an_out_of_range_item_index_retries_the_whole_batch() {
        let errors = [err(9, ERR_NO_QUORUM)];
        assert_eq!(
            classify_spend_attempt(&errors, 3, false),
            SpendAttemptOutcome::Retry(vec![0, 1, 2]),
        );
    }

    /// Duplicate indices in a response must not duplicate items in the
    /// retry set.
    #[test]
    fn duplicate_indices_are_collapsed() {
        let errors = [
            err(2, ERR_MIGRATION_IN_PROGRESS),
            err(2, ERR_MIGRATION_IN_PROGRESS),
            err(0, ERR_MIGRATION_IN_PROGRESS),
        ];
        assert_eq!(
            classify_spend_attempt(&errors, 5, false),
            SpendAttemptOutcome::Retry(vec![0, 2]),
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

#[cfg(test)]
mod settle_predicate_tests {
    use super::*;

    fn round(entries: &[(u32, u64)]) -> BTreeMap<u32, u64> {
        entries.iter().copied().collect()
    }

    #[test]
    fn full_responder_set_with_stable_seqs_settles() {
        let mut t = SettleTracker::default();
        assert!(
            !t.observe(round(&[(1, 10), (2, 20), (3, 30)])),
            "the first round can never settle"
        );
        assert!(
            !t.observe(round(&[(1, 10), (2, 20), (3, 30)])),
            "one stable repeat is not yet settled"
        );
        assert!(
            t.observe(round(&[(1, 10), (2, 20), (3, 30)])),
            "two stable repeats over the full responding set must settle"
        );
    }

    #[test]
    fn advancing_sequences_never_settle() {
        let mut t = SettleTracker::default();
        for i in 0..10u64 {
            assert!(
                !t.observe(round(&[(1, 10 + i), (2, 20), (3, 30)])),
                "round {i}: an advancing sequence must never settle"
            );
        }
    }

    #[test]
    fn middle_node_dropout_positional_shift_is_not_a_false_settle() {
        // The exact positional-shift shape: node2 answered nothing in round 1,
        // node3 answered nothing in rounds 2-4, and node2 reappeared at a
        // sequence numerically equal to node3's PREVIOUS one. Positionally
        // every round is [10, 20] -- the predecessor compared exactly that and
        // SETTLED at round 3 while node2's sequence had moved unobserved and
        // node3 went dark. Keyed by node, round 2 is a responding-set change
        // (reset), so nothing may settle before round 4.
        let mut t = SettleTracker::default();
        assert!(!t.observe(round(&[(1, 10), (3, 20)])));
        assert!(
            !t.observe(round(&[(1, 10), (2, 20)])),
            "a responding-set change must reset stability"
        );
        assert!(
            !t.observe(round(&[(1, 10), (2, 20)])),
            "the false-positive shape: positionally identical rounds must NOT \
             settle across a responding-set change"
        );
        assert!(
            t.observe(round(&[(1, 10), (2, 20)])),
            "a genuinely stable subset settles once the set itself is stable"
        );
    }

    #[test]
    fn flapping_responder_resets_stability_and_never_false_settles() {
        // node2 flaps in and out (the armed-15 wedge shape): the responding
        // set alternates {1,2,3} / {1,3}, so stability must reset every round
        // and the tracker must never report settled while the flap lasts.
        let mut t = SettleTracker::default();
        for i in 0..8 {
            let settled = if i % 2 == 0 {
                t.observe(round(&[(1, 10), (2, 20), (3, 30)]))
            } else {
                t.observe(round(&[(1, 10), (3, 30)]))
            };
            assert!(
                !settled,
                "round {i}: a flapping responder must never settle"
            );
        }
        // Once the flap stops, the stable survivor subset settles: a missing
        // node is "unknown" (tolerated), not a reason to hang forever.
        assert!(
            !t.observe(round(&[(1, 10), (3, 30)])),
            "first stable repeat after the flap: not yet settled"
        );
        assert!(
            t.observe(round(&[(1, 10), (3, 30)])),
            "a stable survivor subset settles after the flap stops"
        );
    }

    #[test]
    fn no_responders_never_settles() {
        // All nodes down: the predecessor's `[] == []` compare counted empty
        // rounds as stable and settled VACUOUSLY after two of them. An empty
        // responding set is evidence of nothing.
        let mut t = SettleTracker::default();
        for i in 0..4 {
            assert!(
                !t.observe(round(&[])),
                "round {i}: an empty responding set must never settle"
            );
        }
    }
}
