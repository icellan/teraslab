use crate::ClientError;
use std::collections::HashMap;

const DEFAULT_DOCKER_MIGRATION_POOL_SIZE: usize = 128;
const DEFAULT_DOCKER_MIGRATION_BATCH_SIZE: usize = 1000;
const ENV_DOCKER_MIGRATION_POOL_SIZE: &str = "TERASLAB_DOCKER_MIGRATION_POOL_SIZE";
const ENV_DOCKER_MIGRATION_BATCH_SIZE: &str = "TERASLAB_DOCKER_MIGRATION_BATCH_SIZE";

fn parse_docker_migration_pool_size(raw: &str) -> Result<usize, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(DEFAULT_DOCKER_MIGRATION_POOL_SIZE);
    }
    trimmed.parse::<usize>().map_err(|e| {
        format!("{ENV_DOCKER_MIGRATION_POOL_SIZE} must be a non-negative integer: {e}")
    })
}

fn docker_migration_pool_size_from_env() -> Result<usize, ClientError> {
    match std::env::var(ENV_DOCKER_MIGRATION_POOL_SIZE) {
        Ok(raw) => parse_docker_migration_pool_size(&raw).map_err(ClientError::Connection),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_DOCKER_MIGRATION_POOL_SIZE),
        Err(e) => Err(ClientError::Connection(format!(
            "{ENV_DOCKER_MIGRATION_POOL_SIZE} could not be read: {e}"
        ))),
    }
}

fn parse_docker_migration_batch_size(raw: &str) -> Result<usize, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(DEFAULT_DOCKER_MIGRATION_BATCH_SIZE);
    }
    trimmed.parse::<usize>().map_err(|e| {
        format!("{ENV_DOCKER_MIGRATION_BATCH_SIZE} must be a non-negative integer: {e}")
    })
}

fn docker_migration_batch_size_from_env() -> Result<usize, ClientError> {
    match std::env::var(ENV_DOCKER_MIGRATION_BATCH_SIZE) {
        Ok(raw) => parse_docker_migration_batch_size(&raw).map_err(ClientError::Connection),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_DOCKER_MIGRATION_BATCH_SIZE),
        Err(e) => Err(ClientError::Connection(format!(
            "{ENV_DOCKER_MIGRATION_BATCH_SIZE} could not be read: {e}"
        ))),
    }
}

fn render_node_config(
    node_id: u32,
    node_ip: &str,
    seeds_str: &str,
    migration_pool_size: usize,
    migration_batch_size: usize,
) -> String {
    format!(
        r#"node_id = {node_id}
listen_addr = "{node_ip}:3300"
http_listen_addr = "0.0.0.0:9100"
swim_port = 3301
seed_nodes = [{seeds_str}]
replication_factor = 2
migration_pool_size = {migration_pool_size}
migration_batch_size = {migration_batch_size}
swim_probe_interval_ms = 150
swim_suspicion_timeout_ms = 1000
device_paths = ["/data/teraslab.dat"]
device_size = 2147483648
device_alignment = 4096
redo_log_size = 67108864
index_snapshot_path = "/data/index.snap"
expected_records = 1000000
lock_stripes = 65536
max_batch_size = 8192
max_connections = 1024
block_height_retention = 288

# Required for the Docker test cluster: nodes bind to per-scenario subnet
# IPs (non-loopback). The safe-defaults check (commit 96b4fc4) fatals
# on non-loopback binds without an explicit opt-in.
enable_remote_bind = true
# Shared cluster_id for every test node so P1.1's matching-id
# short-circuit accepts legitimate scale-up. Without this, F-G8-001's
# `ever_seen_check` fallback rejects the second proposal because the
# third node has never been a committed voter; the cluster stays
# stuck at 2 nodes and every scenario fails at `wait_cluster_ready`.
# 32 hex chars = 16 bytes — see `ServerConfig::resolved_cluster_id`.
cluster_id = "ababababababababababababababab01"
# Intentionally NOT setting `cluster_secret`: the test client does not
# HMAC-sign its frames, and `OP_GET_PARTITION_MAP` is an inter-node
# opcode that gets auth_required when a secret is configured. The
# default (no secret) is fail-open with a per-event warn — exactly
# what we want for in-process docker integration tests. RF>1 also
# emits a warn but accepts the config in non-strict mode.
#
# F-X-002 (production default flipped strict_auth to `true`): explicit
# opt-out is required to keep the trusted-overlay shape this test
# harness relies on. The daemon still emits a prominent boot-time
# warn naming the opt-out so the audit trail surfaces the missing
# secret.
strict_auth = false
"#
    )
}

/// Docker control helpers for cluster tests.
///
/// Provides methods to manage Docker containers and network conditions
/// for integration testing of TeraSlab clusters. All operations are
/// executed via `tokio::process::Command` running docker CLI commands.
///
/// Each `DockerHelpers` instance is associated with a unique `scenario_id`
/// that determines container names, host port mappings, Docker network/volume
/// names, and the Docker Compose project name. This allows multiple test
/// scenarios to run sequentially (or even in parallel) without port or
/// container-name collisions.
///
/// ## Port scheme
///
/// Given a `scenario_id` (1--16) and a node index (1--5):
///
/// - Client host port: `13000 + scenario_id * 10 + (node_index - 1)`
/// - HTTP host port: `19000 + scenario_id * 10 + (node_index - 1)`
///
/// Internal container ports are always 3300 (client) and 9100 (HTTP).
///
/// ## Naming scheme
///
/// - Container names: `ts{scenario_id:02}-node{N}`
/// - Docker network: `ts{scenario_id:02}-net`
/// - Docker volumes: `ts{scenario_id:02}-node{N}-data`, `ts{scenario_id:02}-blobstore`
/// - Compose project: `ts{scenario_id:02}`
pub struct DockerHelpers {
    compose_dir: String,
    node_ips: HashMap<String, String>,
    scenario_id: u16,
    node_count: u32,
    /// Path to the generated compose YAML file (written by `compose_up`).
    generated_compose_path: Option<String>,
}

impl DockerHelpers {
    /// Creates a new `DockerHelpers` instance for the given compose directory
    /// and scenario identifier.
    ///
    /// The `scenario_id` must be in the range 1--16 and uniquely identifies the
    /// test scenario. It determines host port offsets, container name prefixes,
    /// and Docker resource names so that different scenarios never collide.
    ///
    /// The `node_count` specifies how many nodes (3 or 5) this cluster has.
    ///
    /// # Parameters
    /// - `compose_dir`: Path to the directory containing node config files.
    /// - `scenario_id`: Unique scenario number (1--16).
    /// - `node_count`: Number of nodes in the cluster (3 or 5).
    pub fn new(compose_dir: &str, scenario_id: u16, node_count: u32) -> Self {
        // Each scenario gets a unique /24 subnet to avoid Docker network conflicts.
        let subnet_second_octet = 30 + scenario_id;
        let mut node_ips = HashMap::new();
        node_ips.insert(
            "node1".to_string(),
            format!("172.{subnet_second_octet}.0.11"),
        );
        node_ips.insert(
            "node2".to_string(),
            format!("172.{subnet_second_octet}.0.12"),
        );
        node_ips.insert(
            "node3".to_string(),
            format!("172.{subnet_second_octet}.0.13"),
        );
        node_ips.insert(
            "node4".to_string(),
            format!("172.{subnet_second_octet}.0.14"),
        );
        node_ips.insert(
            "node5".to_string(),
            format!("172.{subnet_second_octet}.0.15"),
        );

        Self {
            compose_dir: compose_dir.to_string(),
            node_ips,
            scenario_id,
            node_count,
            generated_compose_path: None,
        }
    }

    /// Returns the scenario ID for this helper instance.
    pub fn scenario_id(&self) -> u16 {
        self.scenario_id
    }

    /// Returns the number of nodes in this cluster.
    pub fn node_count(&self) -> u32 {
        self.node_count
    }

    /// Returns the host-mapped client port for a given node number (1-based).
    ///
    /// Formula: `13000 + scenario_id * 10 + (node_num - 1)`
    pub fn client_port(&self, node_num: u32) -> u16 {
        13000 + self.scenario_id * 10 + (node_num as u16 - 1)
    }

    /// Returns the host-mapped HTTP port for a given node number (1-based).
    ///
    /// Formula: `19000 + scenario_id * 10 + (node_num - 1)`
    pub fn http_port(&self, node_num: u32) -> u16 {
        19000 + self.scenario_id * 10 + (node_num as u16 - 1)
    }

    /// Returns the host-mapped client addresses for nodes 1..=count.
    pub fn host_client_addrs(&self, count: usize) -> Vec<String> {
        (1..=count as u32)
            .map(|n| format!("127.0.0.1:{}", self.client_port(n)))
            .collect()
    }

    /// Returns the address mapping from Docker-internal container IPs to
    /// host-accessible port-mapped addresses for this scenario.
    pub fn docker_addr_map(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        for n in 1..=5u32 {
            let subnet = 30 + self.scenario_id;
            let ip = format!("172.{subnet}.0.{}:3300", 10 + n);
            let host = format!("127.0.0.1:{}", self.client_port(n));
            m.insert(ip, host);
        }
        m
    }

    /// Returns the IP address for a given node name.
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if the node name is not in the IP mapping.
    fn node_ip(&self, name: &str) -> Result<&str, ClientError> {
        self.node_ips
            .get(name)
            .map(|s| s.as_str())
            .ok_or_else(|| ClientError::Connection(format!("unknown node: {name}")))
    }

    /// Returns the docker container name for a given node name, prefixed
    /// with the scenario identifier.
    fn container_name(&self, name: &str) -> String {
        format!("ts{:02}-{name}", self.scenario_id)
    }

    /// Returns the Docker Compose project name for this scenario.
    fn project_name(&self) -> String {
        format!("ts{:02}", self.scenario_id)
    }

    /// Returns the Docker network name for this scenario.
    fn network_name(&self) -> String {
        format!("ts{:02}-net", self.scenario_id)
    }

    /// Generates the docker-compose YAML for this scenario.
    ///
    /// The generated YAML contains unique container names, volume names,
    /// network name, and host port mappings based on the scenario_id.
    /// Each scenario uses a unique /24 subnet (172.{30+sid}.0.0/24) to avoid
    /// Docker network conflicts when multiple scenarios run concurrently or
    /// when teardown from one overlaps with startup of another.
    fn generate_compose_yaml(&self) -> String {
        let sid = self.scenario_id;
        let subnet_second = 30 + sid;
        let net = self.network_name();
        let config_dir = format!("{}/config", self.compose_dir);
        let debug_shards = std::env::var("TERASLAB_DEBUG_SHARDS").ok();

        let mut yaml = format!(
            r#"networks:
  {net}:
    driver: bridge
    ipam:
      config:
        - subnet: 172.{subnet_second}.0.0/24

volumes:
"#
        );

        // Volume declarations
        for n in 1..=self.node_count {
            yaml.push_str(&format!("  ts{sid:02}-node{n}-data:\n"));
        }
        yaml.push_str(&format!("  ts{sid:02}-blobstore:\n"));

        // Common config anchor
        yaml.push_str(
            r#"
x-teraslab-common: &teraslab-common
  image: teraslab:test
  restart: "no"
  cap_add:
    - NET_ADMIN
  ulimits:
    memlock:
      soft: -1
      hard: -1

services:
"#,
        );

        // Service definitions
        for n in 1..=self.node_count {
            let ip = format!("172.{subnet_second}.0.{}", 10 + n);
            let client_host_port = self.client_port(n);
            let http_host_port = self.http_port(n);

            yaml.push_str(&format!(
                r#"  node{n}:
    <<: *teraslab-common
    container_name: ts{sid:02}-node{n}
    hostname: node{n}
    networks:
      {net}:
        ipv4_address: {ip}
    volumes:
      - ts{sid:02}-node{n}-data:/data
      - ts{sid:02}-blobstore:/blobstore
      - {config_dir}/ts{sid:02}-node{n}.toml:/etc/teraslab/node.toml:ro
    ports:
      - "{client_host_port}:3300"
      - "{http_host_port}:9100"
"#
            ));
            if let Some(debug_shards) = &debug_shards {
                yaml.push_str(&format!(
                    "    environment:\n      TERASLAB_DEBUG_SHARDS: \"{}\"\n",
                    debug_shards,
                ));
            }
        }

        yaml
    }

    /// Ensures the generated compose file exists, writing it if needed.
    /// Returns the path to the generated file.
    async fn ensure_compose_file(&mut self) -> Result<String, ClientError> {
        if let Some(ref path) = self.generated_compose_path {
            return Ok(path.clone());
        }

        // Generate per-scenario node config files with unique subnet IPs
        self.generate_node_configs().await?;

        let yaml = self.generate_compose_yaml();
        let path = format!(
            "{}/docker-compose.ts{:02}.yml",
            self.compose_dir, self.scenario_id
        );

        tokio::fs::write(&path, yaml.as_bytes())
            .await
            .map_err(|e| {
                ClientError::Connection(format!("failed to write compose file {path}: {e}"))
            })?;

        self.generated_compose_path = Some(path.clone());
        Ok(path)
    }

    /// Generate node TOML config files with IPs matching this scenario's subnet.
    async fn generate_node_configs(&self) -> Result<(), ClientError> {
        let subnet = 30 + self.scenario_id;
        let config_dir = format!("{}/config", self.compose_dir);
        let migration_pool_size = docker_migration_pool_size_from_env()?;
        let migration_batch_size = docker_migration_batch_size_from_env()?;

        // Create config directory if it doesn't exist
        let _ = tokio::fs::create_dir_all(&config_dir).await;

        for n in 1..=self.node_count {
            let node_ip = format!("172.{subnet}.0.{}", 10 + n);
            let seed_nodes: Vec<String> = (1..=self.node_count)
                .filter(|&s| s != n)
                .map(|s| format!("\"172.{subnet}.0.{}:3301\"", 10 + s))
                .collect();
            let seeds_str = seed_nodes.join(", ");

            let config = render_node_config(
                n,
                &node_ip,
                &seeds_str,
                migration_pool_size,
                migration_batch_size,
            );

            let path = format!("{config_dir}/ts{:02}-node{n}.toml", self.scenario_id);
            tokio::fs::write(&path, config.as_bytes())
                .await
                .map_err(|e| {
                    ClientError::Connection(format!("failed to write config {path}: {e}"))
                })?;
        }

        Ok(())
    }

    /// Returns compose command args with project name and file.
    fn compose_base_args(&self, compose_file: &str) -> Vec<String> {
        vec![
            "compose".to_string(),
            "-p".to_string(),
            self.project_name(),
            "-f".to_string(),
            compose_file.to_string(),
        ]
    }

    // ── Node lifecycle ──────────────────────────────────────────────

    /// Kills a node container immediately with SIGKILL.
    ///
    /// # Parameters
    /// - `name`: Node name (e.g. "node1").
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if the docker command fails.
    pub async fn kill_node(&self, name: &str) -> Result<(), ClientError> {
        let container = self.container_name(name);
        run_docker_cmd(&["kill", "--signal=SIGKILL", &container]).await?;
        Ok(())
    }

    /// Force-remove a node container (kill + rm). This releases the Docker
    /// network interface immediately, allowing SWIM to detect the node as
    /// unreachable via ICMP unreachable rather than silent UDP timeouts.
    pub async fn remove_node(&self, name: &str) -> Result<(), ClientError> {
        let container = self.container_name(name);
        run_docker_cmd(&["rm", "-f", &container]).await?;
        Ok(())
    }

    /// Gracefully stops a node container with a 10-second timeout.
    ///
    /// # Parameters
    /// - `name`: Node name (e.g. "node1").
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if the docker command fails.
    pub async fn stop_node(&self, name: &str) -> Result<(), ClientError> {
        let container = self.container_name(name);
        run_docker_cmd(&["stop", "--time=1", &container]).await?;
        Ok(())
    }

    /// Starts a previously stopped node container.
    ///
    /// # Parameters
    /// - `name`: Node name (e.g. "node1").
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if the docker command fails.
    pub async fn start_node(&self, name: &str) -> Result<(), ClientError> {
        let container = self.container_name(name);
        run_docker_cmd(&["start", &container]).await?;
        Ok(())
    }

    /// Pauses all processes in a node container.
    ///
    /// # Parameters
    /// - `name`: Node name (e.g. "node1").
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if the docker command fails.
    pub async fn pause_node(&self, name: &str) -> Result<(), ClientError> {
        let container = self.container_name(name);
        run_docker_cmd(&["pause", &container]).await?;
        Ok(())
    }

    /// Unpauses all processes in a node container.
    ///
    /// # Parameters
    /// - `name`: Node name (e.g. "node1").
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if the docker command fails.
    pub async fn unpause_node(&self, name: &str) -> Result<(), ClientError> {
        let container = self.container_name(name);
        run_docker_cmd(&["unpause", &container]).await?;
        Ok(())
    }

    // ── Network manipulation ────────────────────────────────────────

    /// Creates a network partition between a node and one or more target nodes.
    ///
    /// Uses `iptables` to DROP packets in both directions between source and
    /// each target. This works inside Docker Desktop containers (the rules
    /// apply in the container's network namespace) and doesn't conflict with
    /// `tc netem` (used by `slow_network` for latency/loss injection).
    ///
    /// # Parameters
    /// - `name`: The node to partition (e.g. "node3").
    /// - `targets`: Slice of target node names to partition from.
    pub async fn partition_node(&self, name: &str, targets: &[&str]) -> Result<(), ClientError> {
        let source_ip = self.node_ip(name)?.to_string();
        let source_container = self.container_name(name);

        for &target in targets {
            let target_ip = self.node_ip(target)?.to_string();
            let target_container = self.container_name(target);

            // Block traffic from target on source node
            let _ = run_docker_cmd(&[
                "exec",
                &source_container,
                "iptables",
                "-A",
                "INPUT",
                "-s",
                &target_ip,
                "-j",
                "DROP",
            ])
            .await;
            let _ = run_docker_cmd(&[
                "exec",
                &source_container,
                "iptables",
                "-A",
                "OUTPUT",
                "-d",
                &target_ip,
                "-j",
                "DROP",
            ])
            .await;

            // Block traffic from source on target node
            let _ = run_docker_cmd(&[
                "exec",
                &target_container,
                "iptables",
                "-A",
                "INPUT",
                "-s",
                &source_ip,
                "-j",
                "DROP",
            ])
            .await;
            let _ = run_docker_cmd(&[
                "exec",
                &target_container,
                "iptables",
                "-A",
                "OUTPUT",
                "-d",
                &source_ip,
                "-j",
                "DROP",
            ])
            .await;
        }

        Ok(())
    }

    /// Heals all network partitions on a single node by flushing iptables.
    ///
    /// Errors are silently ignored because the node may be stopped or killed.
    pub async fn heal_partition(&self, name: &str) -> Result<(), ClientError> {
        let container = self.container_name(name);
        let _ = run_docker_cmd(&["exec", &container, "iptables", "-F"]).await;
        Ok(())
    }

    /// Heals all network partitions on all 5 nodes.
    ///
    /// Errors from individual nodes are silently ignored because some nodes
    /// may not exist or may be stopped/killed.
    pub async fn heal_all_partitions(&self) -> Result<(), ClientError> {
        for i in 1..=5 {
            let name = format!("node{i}");
            let _ = self.heal_partition(&name).await;
        }
        Ok(())
    }

    /// Adds network latency and packet loss to a node using tc netem.
    ///
    /// Any existing tc qdisc on `eth0` is removed first (errors ignored)
    /// before applying the new netem configuration.
    ///
    /// # Parameters
    /// - `name`: Node name (e.g. "node1").
    /// - `latency_ms`: Added delay in milliseconds.
    /// - `loss_pct`: Packet loss percentage (e.g. 5.0 for 5%).
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if the tc add command fails.
    pub async fn slow_network(
        &self,
        name: &str,
        latency_ms: u32,
        loss_pct: f32,
    ) -> Result<(), ClientError> {
        let container = self.container_name(name);

        // Remove existing qdisc (ignore errors if none exists)
        let _ = run_docker_cmd(&[
            "exec", &container, "tc", "qdisc", "del", "dev", "eth0", "root",
        ])
        .await;

        let delay_arg = format!("{latency_ms}ms");
        let loss_arg = format!("{loss_pct}%");
        run_docker_cmd(&[
            "exec", &container, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay",
            &delay_arg, "loss", &loss_arg,
        ])
        .await?;

        Ok(())
    }

    /// Removes any tc netem configuration from a node's eth0 interface.
    ///
    /// Errors from the removal are ignored (e.g. if no qdisc was configured).
    ///
    /// # Parameters
    /// - `name`: Node name (e.g. "node1").
    ///
    /// # Errors
    /// Returns `ClientError::Connection` only if the docker exec invocation
    /// itself fails (not if the tc command reports no qdisc to remove).
    pub async fn clear_network(&self, name: &str) -> Result<(), ClientError> {
        let container = self.container_name(name);
        let _ = run_docker_cmd(&[
            "exec", &container, "tc", "qdisc", "del", "dev", "eth0", "root",
        ])
        .await;
        Ok(())
    }

    /// Clears tc netem configuration on all 5 nodes.
    ///
    /// Errors from individual nodes are silently ignored because some nodes
    /// may not exist or may be stopped/killed.
    pub async fn clear_all_networks(&self) -> Result<(), ClientError> {
        for i in 1..=5 {
            let name = format!("node{i}");
            let _ = self.clear_network(&name).await;
        }
        Ok(())
    }

    // ── Cluster operations ──────────────────────────────────────────

    /// Starts the full cluster using docker compose.
    ///
    /// Generates a unique compose YAML for this scenario (if not already
    /// generated) and runs `docker compose -p <project> -f <file> up -d`.
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if the compose command fails.
    pub async fn compose_up(&mut self) -> Result<(), ClientError> {
        let compose_file = self.ensure_compose_file().await?;
        let mut args = self.compose_base_args(&compose_file);
        args.push("up".to_string());
        args.push("-d".to_string());

        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run_docker_cmd(&arg_refs).await?;
        Ok(())
    }

    /// Tears down the cluster and removes volumes using docker compose.
    ///
    /// Runs `docker compose -p <project> -f <file> down -v`.
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if the compose command fails.
    pub async fn compose_down(&mut self) -> Result<(), ClientError> {
        let compose_file = self.ensure_compose_file().await?;
        let mut args = self.compose_base_args(&compose_file);
        args.push("down".to_string());
        args.push("-v".to_string());

        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run_docker_cmd(&arg_refs).await?;
        Ok(())
    }

    /// Starts specific nodes using docker compose.
    ///
    /// Runs `docker compose -p <project> -f <file> up -d <node1> <node2> ...`.
    ///
    /// # Parameters
    /// - `nodes`: Slice of node names to start (e.g. `&["node1", "node3"]`).
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if the compose command fails.
    pub async fn compose_up_nodes(&mut self, nodes: &[&str]) -> Result<(), ClientError> {
        let compose_file = self.ensure_compose_file().await?;
        let mut args = self.compose_base_args(&compose_file);
        args.push("up".to_string());
        args.push("-d".to_string());
        // Don't recreate already-running containers when adding new nodes.
        // Without this, Docker Compose may restart existing containers if the
        // compose file changed (e.g., when docker_5node overwrites docker_3node's file).
        args.push("--no-recreate".to_string());
        for node in nodes {
            args.push(node.to_string());
        }

        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run_docker_cmd(&arg_refs).await?;
        Ok(())
    }

    // ── Diagnostics ─────────────────────────────────────────────────

    /// Collects logs from all 5 node containers and writes them to the output directory.
    ///
    /// Each node's logs are written to `{output_dir}/{container_name}.log`.
    /// The output directory is created if it does not exist.
    ///
    /// # Parameters
    /// - `output_dir`: Directory path where log files will be written.
    ///
    /// # Errors
    /// Returns `ClientError::Connection` if any docker logs command fails
    /// or if the output directory cannot be created.
    pub async fn collect_logs(&self, output_dir: &str) -> Result<(), ClientError> {
        tokio::fs::create_dir_all(output_dir).await.map_err(|e| {
            ClientError::Connection(format!(
                "{output_dir}: failed to create log output directory: {e}"
            ))
        })?;

        for i in 1..=5 {
            let name = format!("node{i}");
            let container = self.container_name(&name);
            let log_output = run_docker_cmd(&["logs", &container])
                .await
                .unwrap_or_default();
            let log_path = format!("{output_dir}/{container}.log");
            tokio::fs::write(&log_path, log_output.as_bytes())
                .await
                .map_err(|e| {
                    ClientError::Connection(format!("{log_path}: failed to write log file: {e}"))
                })?;
        }

        Ok(())
    }
}

/// Runs a docker command with the given arguments and returns stdout on success.
///
/// # Parameters
/// - `args`: Arguments to pass to the `docker` command.
///
/// # Errors
/// Returns `ClientError::Connection` if the process cannot be spawned or
/// if the command exits with a non-zero status.
async fn run_docker_cmd(args: &[&str]) -> Result<String, ClientError> {
    let output = tokio::process::Command::new("docker")
        .args(args)
        .output()
        .await
        .map_err(|e| ClientError::Connection(format!("docker: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(ClientError::Connection(format!("docker: {stderr}")));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docker_migration_pool_parse_defaults_on_empty() {
        assert_eq!(
            parse_docker_migration_pool_size("").unwrap(),
            DEFAULT_DOCKER_MIGRATION_POOL_SIZE,
        );
        assert_eq!(
            parse_docker_migration_pool_size("   ").unwrap(),
            DEFAULT_DOCKER_MIGRATION_POOL_SIZE,
        );
    }

    #[test]
    fn docker_migration_pool_parse_accepts_higher_values() {
        assert_eq!(parse_docker_migration_pool_size("256").unwrap(), 256);
    }

    #[test]
    fn docker_migration_pool_parse_rejects_invalid_values() {
        let err = parse_docker_migration_pool_size("wide").unwrap_err();
        assert!(
            err.contains(ENV_DOCKER_MIGRATION_POOL_SIZE),
            "err was: {err}",
        );
    }

    #[test]
    fn docker_migration_batch_parse_defaults_on_empty() {
        assert_eq!(
            parse_docker_migration_batch_size("").unwrap(),
            DEFAULT_DOCKER_MIGRATION_BATCH_SIZE,
        );
        assert_eq!(
            parse_docker_migration_batch_size("   ").unwrap(),
            DEFAULT_DOCKER_MIGRATION_BATCH_SIZE,
        );
    }

    #[test]
    fn docker_migration_batch_parse_accepts_higher_values() {
        assert_eq!(parse_docker_migration_batch_size("4096").unwrap(), 4096);
    }

    #[test]
    fn docker_migration_batch_parse_rejects_invalid_values() {
        let err = parse_docker_migration_batch_size("wide").unwrap_err();
        assert!(
            err.contains(ENV_DOCKER_MIGRATION_BATCH_SIZE),
            "err was: {err}",
        );
    }

    #[test]
    fn node_config_contains_configured_migration_tuning() {
        let config = render_node_config(
            2,
            "172.38.0.12",
            "\"172.38.0.11:3301\", \"172.38.0.13:3301\"",
            96,
            2048,
        );

        assert!(config.contains("node_id = 2"));
        assert!(config.contains("listen_addr = \"172.38.0.12:3300\""));
        assert!(config.contains("migration_pool_size = 96"));
        assert!(config.contains("migration_batch_size = 2048"));
        assert!(config.contains("seed_nodes = [\"172.38.0.11:3301\", \"172.38.0.13:3301\"]"));
    }

    /// F-X-002 regression: the rendered docker-test config must pass
    /// `ServerConfig::validate_safe_defaults` even though it leaves
    /// `cluster_secret` unset. Pre-F-X-002 this was the trusted-overlay
    /// default; post-flip the test config has to explicitly emit
    /// `strict_auth = false` to opt back into that shape. If a future
    /// edit drops the opt-out, the safe-defaults gate would refuse to
    /// start every docker test node — this assertion catches that
    /// regression before the docker cycle takes 30+ seconds to fail.
    #[test]
    fn rendered_node_config_passes_safe_defaults_under_fx002_default() {
        use teraslab::config::ServerConfig;

        let rendered = render_node_config(
            1,
            "172.38.0.11",
            "\"172.38.0.12:3301\", \"172.38.0.13:3301\"",
            128,
            500,
        );
        let cfg: ServerConfig = toml::from_str(&rendered).expect(
            "rendered docker node config must be a valid ServerConfig TOML payload",
        );
        // The opt-out must be present in the rendered TOML and must
        // round-trip through `serde::Deserialize` into the in-memory
        // struct. Without it, `validate_safe_defaults` would reject
        // the clustered config below for the missing `cluster_secret`.
        assert!(
            !cfg.strict_auth,
            "rendered docker config must emit `strict_auth = false` (F-X-002 opt-out); \
             without it, the multi-node test cluster cannot start without a cluster_secret",
        );
        cfg.validate_safe_defaults().expect(
            "rendered docker node config must pass safe-defaults validation under the \
             F-X-002 production default (strict_auth = true) via the explicit \
             `strict_auth = false` opt-out",
        );
    }

    /// Pin a single-node (non-clustered) config through the same gate.
    /// This is the other half of the F-X-002 contract — single-node
    /// configs need no opt-out because the multi-node check does not
    /// fire.
    #[test]
    fn single_node_default_config_passes_safe_defaults_under_fx002_default() {
        use teraslab::config::ServerConfig;

        // Mirrors the daemon's behaviour when launched without a
        // --config flag: `ServerConfig::default()` carries the
        // F-X-002 production default (`strict_auth = true`) and must
        // still validate at single-node.
        let cfg = ServerConfig::default();
        assert!(
            cfg.strict_auth,
            "F-X-002: default config must have strict_auth = true",
        );
        assert_eq!(
            cfg.node_id, 0,
            "default config must be single-node (node_id = 0)",
        );
        cfg.validate_safe_defaults().expect(
            "F-X-002: single-node default config must validate without a cluster_secret",
        );
    }
}
