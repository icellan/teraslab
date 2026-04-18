//! Server configuration.

use serde::Deserialize;
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Index backend configuration
// ---------------------------------------------------------------------------

/// Index backend mode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum IndexBackendMode {
    /// In-memory Robin Hood hash table (default, fastest).
    #[default]
    Memory,
    /// On-disk B+ tree via redb (low-RAM deployments).
    Redb,
    /// File-backed mmap (persistent, relies on redo log for crash recovery).
    FileBacked,
}

impl<'de> Deserialize<'de> for IndexBackendMode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "memory" | "" => Ok(Self::Memory),
            "redb" => Ok(Self::Redb),
            "file_backed" => Ok(Self::FileBacked),
            other => Err(serde::de::Error::custom(format!(
                "unknown index backend: {other:?} (expected \"memory\", \"redb\", or \"file_backed\")"
            ))),
        }
    }
}

/// Configuration for the index subsystem.
///
/// Controls which backend is used for the primary and secondary indexes.
/// When `backend` is `"memory"` (default), the existing in-memory Robin Hood
/// hash table is used. When `"redb"`, a crash-durable B+ tree backed by redb
/// is used instead, trading throughput for dramatically lower RAM requirements.
/// When `"file_backed"`, a memory-mapped file is used for the primary index
/// (persistent across restarts, relying on the redo log for crash recovery);
/// secondary indexes remain in-memory.
///
/// The redb backend uses three separate database files: one for the primary
/// index (`redb_path`), one for the DAH secondary index (`redb_dah_path`),
/// and one for the unmined secondary index (`redb_unmined_path`).
///
/// # Example (TOML)
///
/// ```toml
/// [index]
/// backend = "redb"
/// redb_path = "/data/teraslab-index.redb"
/// redb_dah_path = "/data/teraslab-dah.redb"
/// redb_unmined_path = "/data/teraslab-unmined.redb"
/// redb_cache_size = 268435456
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IndexConfig {
    /// Backend mode: `"memory"` (default), `"redb"`, or `"file_backed"`.
    pub backend: IndexBackendMode,

    /// Path for the redb primary index database file.
    /// Only used when `backend = "redb"`.
    pub redb_path: PathBuf,

    /// Path for the redb DAH secondary index database.
    /// Only used when `backend = "redb"`.
    pub redb_dah_path: PathBuf,

    /// Path for the redb unmined secondary index database.
    /// Only used when `backend = "redb"`.
    pub redb_unmined_path: PathBuf,

    /// redb page cache size in bytes. Default: 256 MiB.
    /// Only applies to the redb backend.
    pub redb_cache_size: usize,

    /// Path for the file-backed mmap primary index.
    /// Only used when `backend = "file_backed"`.
    pub file_backed_path: PathBuf,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            backend: IndexBackendMode::Memory,
            redb_path: PathBuf::from("teraslab-index.redb"),
            redb_dah_path: PathBuf::from("teraslab-dah.redb"),
            redb_unmined_path: PathBuf::from("teraslab-unmined.redb"),
            redb_cache_size: 256 * 1024 * 1024, // 256 MiB
            file_backed_path: PathBuf::from("teraslab-index.dat"),
        }
    }
}

impl IndexConfig {
    /// Whether the in-memory backend is selected.
    pub fn is_memory(&self) -> bool {
        self.backend == IndexBackendMode::Memory
    }

    /// Whether the redb on-disk backend is selected.
    pub fn is_redb(&self) -> bool {
        self.backend == IndexBackendMode::Redb
    }

    /// Whether the file-backed mmap backend is selected.
    pub fn is_file_backed(&self) -> bool {
        self.backend == IndexBackendMode::FileBacked
    }
}

/// TeraSlab server configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// TCP listen address for the binary wire protocol.
    pub listen_addr: String,

    /// Address to advertise to other cluster nodes. If not set, defaults to
    /// `listen_addr`. Set this when `listen_addr` uses `0.0.0.0` (e.g. in
    /// Docker containers) so other nodes can reach this node by its actual IP.
    pub advertise_addr: Option<String>,

    /// Device file paths for data storage.
    /// Each path is a file that will be created if it doesn't exist.
    pub device_paths: Vec<PathBuf>,

    /// Size of each device file in bytes (only used when creating new files).
    pub device_size: u64,

    /// Device I/O alignment in bytes (4096 for most NVMe/SSDs).
    pub device_alignment: usize,

    /// Size of the redo log region in bytes.
    pub redo_log_size: u64,

    /// Path for the redo log file. If not set, derived from the first device
    /// path by appending `.redo`.
    pub redo_log_path: Option<PathBuf>,

    /// Path for the index snapshot file.
    pub index_snapshot_path: PathBuf,

    /// Expected number of records (for initial index sizing).
    pub expected_records: usize,

    /// Lock stripe count (power of 2, default 65536).
    pub lock_stripes: usize,

    /// Maximum batch size accepted from clients.
    pub max_batch_size: u32,

    /// Maximum concurrent client connections.
    pub max_connections: usize,

    /// HTTP listen address for observability endpoints (metrics, health, debug).
    pub http_listen_addr: String,

    /// Block height retention for DAH evaluation.
    pub block_height_retention: u32,

    // -- Cluster settings --

    /// Unique node ID. Must be different for each node in the cluster.
    /// 0 = single-node mode (no clustering).
    pub node_id: u64,

    /// UDP port for SWIM membership protocol.
    pub swim_port: u16,

    /// Seed node addresses for cluster discovery (host:swim_port).
    pub seed_nodes: Vec<String>,

    /// Replication factor (1 = no replication, 2 = master + 1 replica).
    pub replication_factor: u8,

    /// SWIM probe interval in milliseconds.
    pub swim_probe_interval_ms: u64,

    /// SWIM suspicion timeout in milliseconds.
    pub swim_suspicion_timeout_ms: u64,

    /// Directory for external blob storage (large transaction cold data).
    pub blobstore_path: String,

    /// Path for persisted cluster state (peak cluster size for quorum safety).
    /// If not set, derived from the first device path by appending `.cluster`.
    pub cluster_state_path: Option<PathBuf>,

    /// Shared secret for cluster authentication (HMAC-SHA256).
    ///
    /// When set, all SWIM messages and inter-node TCP connections are
    /// authenticated. Peers that cannot produce a valid HMAC are rejected.
    /// All nodes in the cluster must use the same secret.
    pub cluster_secret: Option<String>,

    /// Maximum concurrent migration threads per topology change.
    /// Prevents resource exhaustion during rapid churn. Default: 16.
    pub max_migration_threads: usize,

    // -- Replication durability settings --

    /// Replication acknowledgment policy.
    ///
    /// - `"auto"` (default): WriteAll for RF=2, WriteMajority for RF>=3,
    ///   best_effort for RF=1.
    /// - `"write_all"`: Wait for ALL replicas to ACK before client success.
    /// - `"write_majority"`: Wait for floor(RF/2)+1 copies (including master).
    /// - `"best_effort"`: Log replication failures but don't fail the client.
    pub ack_policy: String,

    /// Timeout in milliseconds for each replication batch ACK. Default: 3000.
    pub replication_timeout_ms: u64,

    /// Behavior when the replication ACK policy cannot be satisfied.
    ///
    /// - `"reject"` (default): Fail the mutation with ERR_REPLICATION_FAILED.
    /// - `"best_effort"`: Log the failure but succeed the client request.
    ///
    /// # WARNING — DATA LOSS RISK
    ///
    /// Setting this to `"best_effort"` means that acknowledged writes can be
    /// **permanently lost** if the master node crashes before replicas catch up.
    /// In best-effort mode, the client receives STATUS_OK even when zero
    /// replicas have confirmed the write. If the master then dies, those writes
    /// exist only on the dead master's device and are irrecoverable.
    ///
    /// Only use `"best_effort"` when availability is more important than
    /// durability — e.g., for idempotent workloads where the client can
    /// safely retry, or during planned maintenance windows. For production
    /// deployments where every acknowledged write must survive a single node
    /// failure, keep the default `"reject"`.
    pub replication_degraded_mode: String,

    // -- Migration performance settings --

    /// Number of parallel TCP connections per migration target.
    /// More connections = higher throughput for large migrations.
    /// Default: 4.
    pub migration_pool_size: usize,

    /// Number of records per baseline streaming batch during migration.
    /// Larger batches reduce round-trip overhead but increase memory per batch.
    /// Default: 100.
    pub migration_batch_size: usize,

    /// Interval in seconds between replica lag checks. Default: 30.
    /// Set to 0 to disable lag monitoring.
    pub replica_lag_check_interval_secs: u64,

    // -- Index backend settings --

    /// Index backend configuration. Controls whether the primary and secondary
    /// indexes use in-memory hash tables or on-disk redb B+ trees.
    pub index: IndexConfig,

    /// Expected device identity (hex string). If set, the server refuses to
    /// start if the on-disk identity does not match. Use this to prevent
    /// accidentally pointing at the wrong device.
    ///
    /// The expected value is a 32-character lowercase hex string, as printed
    /// by `device_id_hex()` and logged on first startup.
    pub device_id: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:3300".to_string(),
            advertise_addr: None,
            device_paths: vec![PathBuf::from("teraslab-data.dat")],
            device_size: 1024 * 1024 * 1024, // 1 GiB
            device_alignment: 4096,
            redo_log_size: 64 * 1024 * 1024, // 64 MiB
            redo_log_path: None,
            index_snapshot_path: PathBuf::from("teraslab-index.snap"),
            expected_records: 100_000,
            lock_stripes: 65536,
            max_batch_size: 8192,
            max_connections: 1024,
            http_listen_addr: "0.0.0.0:9100".to_string(),
            block_height_retention: 288,
            node_id: 0,
            swim_port: 3301,
            seed_nodes: vec![],
            replication_factor: 1,
            swim_probe_interval_ms: 200,
            swim_suspicion_timeout_ms: 5000,
            blobstore_path: "/blobstore".to_string(),
            cluster_state_path: None,
            cluster_secret: None,
            max_migration_threads: 16,
            ack_policy: "auto".to_string(),
            replication_timeout_ms: 3000,
            replication_degraded_mode: "reject".to_string(),
            migration_pool_size: 32,
            migration_batch_size: 500,
            replica_lag_check_interval_secs: 30,
            index: IndexConfig::default(),
            device_id: None,
        }
    }
}

impl ServerConfig {
    /// Whether clustering is enabled (node_id > 0).
    pub fn is_clustered(&self) -> bool {
        self.node_id > 0
    }

    /// Resolve the redo log file path. Uses `redo_log_path` if explicitly set,
    /// otherwise derives it from the first device path by appending `.redo`.
    pub fn resolved_redo_log_path(&self) -> PathBuf {
        match &self.redo_log_path {
            Some(p) => p.clone(),
            None => {
                let mut p = self.device_paths[0].clone().into_os_string();
                p.push(".redo");
                PathBuf::from(p)
            }
        }
    }

    /// Resolve the cluster state file path. Uses `cluster_state_path` if set,
    /// otherwise derives from the first device path by appending `.cluster`.
    pub fn resolved_cluster_state_path(&self) -> PathBuf {
        match &self.cluster_state_path {
            Some(p) => p.clone(),
            None => {
                let mut p = self.device_paths[0].clone().into_os_string();
                p.push(".cluster");
                PathBuf::from(p)
            }
        }
    }

    /// Resolve the replication ACK policy based on config and replication factor.
    ///
    /// Returns `None` when replication is best-effort (RF=1 or explicit "best_effort").
    /// Returns the appropriate `AckPolicy` otherwise.
    pub fn resolved_ack_policy(&self) -> Option<crate::replication::manager::AckPolicy> {
        use crate::replication::manager::AckPolicy;
        match self.ack_policy.as_str() {
            "write_all" => Some(AckPolicy::WriteAll),
            "write_majority" => Some(AckPolicy::WriteMajority),
            "best_effort" => None,
            _ => {
                match self.replication_factor {
                    0 | 1 => None,
                    2 => Some(AckPolicy::WriteAll),
                    _ => Some(AckPolicy::WriteMajority),
                }
            }
        }
    }

    /// Whether replication failures should be tolerated (best_effort mode).
    pub fn is_replication_best_effort(&self) -> bool {
        self.replication_degraded_mode == "best_effort"
    }

    /// Validate cluster durability settings against the server safety contract.
    ///
    /// Rejects `replication_degraded_mode = "best_effort"` when the configured
    /// replication factor (RF) is greater than 1. With RF > 1 the cluster has
    /// replicas whose ACKs define durability; allowing best-effort mode would
    /// silently degrade the contract to single-node durability without any
    /// operator-visible signal. If RF = 1 (or 0) there are no replicas to
    /// ACK, so the flag is a no-op and the combination is allowed.
    ///
    /// See also: [`STATUS_DEGRADED_DURABILITY`](crate::protocol::opcodes::STATUS_DEGRADED_DURABILITY)
    /// — the runtime signal emitted when RF > 1 best-effort is *not* in use
    /// but individual best-effort paths fall back because replicas ACK-failed.
    pub fn validate_cluster_safety(&self) -> std::result::Result<(), String> {
        if self.replication_factor > 1 && self.replication_degraded_mode == "best_effort" {
            return Err(format!(
                "replication_degraded_mode = \"best_effort\" is not allowed with \
                 replication_factor = {} (> 1): acknowledged writes could be lost \
                 if the master crashes before replicas catch up. Either set \
                 replication_degraded_mode = \"reject\" or lower replication_factor to 1.",
                self.replication_factor,
            ));
        }
        Ok(())
    }

    /// Validate the `device_id` config value, if set.
    ///
    /// Returns `Ok(())` if absent or a valid 32-char lowercase hex string.
    /// Returns `Err` with a descriptive message otherwise.
    pub fn validate_device_id(&self) -> std::result::Result<(), String> {
        if let Some(ref id) = self.device_id {
            if id.len() != 32 {
                return Err(format!(
                    "device_id must be exactly 32 hex characters, got {} characters",
                    id.len()
                ));
            }
            if !id.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()) {
                return Err(
                    "device_id must contain only lowercase hex digits (0-9, a-f)".to_string()
                );
            }
        }
        Ok(())
    }

    /// Maximum allowed `block_height_retention` value.
    ///
    /// At BSV's 10-minute target block time, 10,000,000 blocks is roughly
    /// 190 years — well beyond any realistic retention policy. Capping here
    /// ensures `current_block_height + block_height_retention` cannot
    /// overflow `u32` for any remotely plausible current height, turning
    /// the defense-in-depth `checked_add` in `evaluate_delete_at_height`
    /// into an impossibility guard in practice. The runtime path still
    /// returns `SpendError::DahOverflow` if overflow ever does occur.
    pub const MAX_BLOCK_HEIGHT_RETENTION: u32 = 10_000_000;

    /// Validate `block_height_retention` against the sanity bound.
    ///
    /// Returns `Err` if `block_height_retention` exceeds
    /// [`Self::MAX_BLOCK_HEIGHT_RETENTION`] — a value so large that
    /// configuring it is almost certainly an operator mistake and would
    /// leave no headroom for `current_block_height` before `u32` overflow.
    pub fn validate_block_height_retention(&self) -> std::result::Result<(), String> {
        if self.block_height_retention > Self::MAX_BLOCK_HEIGHT_RETENTION {
            return Err(format!(
                "block_height_retention = {} exceeds maximum allowed value {} \
                 (roughly 190 years at 10-minute target); configuring this \
                 large a retention would risk u32 overflow on \
                 current_block_height + retention",
                self.block_height_retention,
                Self::MAX_BLOCK_HEIGHT_RETENTION,
            ));
        }
        Ok(())
    }
}

impl ServerConfig {
    /// Load configuration from a TOML file, falling back to defaults.
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file: {e}"))?;
        toml::from_str(&content)
            .map_err(|e| format!("failed to parse config: {e}"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_index_config_is_memory() {
        let cfg = IndexConfig::default();
        assert_eq!(cfg.backend, IndexBackendMode::Memory);
        assert!(cfg.is_memory());
        assert!(!cfg.is_redb());
    }

    #[test]
    fn parse_index_backend_memory() {
        let toml_str = r#"
[index]
backend = "memory"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.index.is_memory());
    }

    #[test]
    fn parse_index_backend_redb() {
        let toml_str = r#"
[index]
backend = "redb"
redb_path = "/data/primary.redb"
redb_dah_path = "/data/dah.redb"
redb_unmined_path = "/data/unmined.redb"
redb_cache_size = 536870912
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.index.is_redb());
        assert_eq!(cfg.index.redb_path, PathBuf::from("/data/primary.redb"));
        assert_eq!(cfg.index.redb_dah_path, PathBuf::from("/data/dah.redb"));
        assert_eq!(cfg.index.redb_unmined_path, PathBuf::from("/data/unmined.redb"));
        assert_eq!(cfg.index.redb_cache_size, 536870912);
    }

    #[test]
    fn parse_no_index_section_defaults_to_memory() {
        let toml_str = r#"
listen_addr = "0.0.0.0:3300"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.index.is_memory());
        assert_eq!(cfg.index.redb_cache_size, 256 * 1024 * 1024);
    }

    #[test]
    fn parse_unknown_backend_is_error() {
        let toml_str = r#"
[index]
backend = "rocksdb"
"#;
        let result: std::result::Result<ServerConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown index backend"), "error was: {err}");
    }

    #[test]
    fn parse_empty_backend_defaults_to_memory() {
        let toml_str = r#"
[index]
backend = ""
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.index.is_memory());
    }

    #[test]
    fn file_backed_config() {
        let cfg = IndexConfig {
            backend: IndexBackendMode::FileBacked,
            ..IndexConfig::default()
        };
        assert!(cfg.is_file_backed());
        assert!(!cfg.is_memory());
        assert!(!cfg.is_redb());
        assert_eq!(cfg.file_backed_path, PathBuf::from("teraslab-index.dat"));
    }

    #[test]
    fn deserialize_file_backed_backend() {
        let toml_str = r#"backend = "file_backed""#;
        let cfg: IndexConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.backend, IndexBackendMode::FileBacked);
    }

    #[test]
    fn best_effort_with_rf_3_is_rejected() {
        let cfg = ServerConfig {
            node_id: 7,
            replication_factor: 3,
            replication_degraded_mode: "best_effort".to_string(),
            ..ServerConfig::default()
        };

        let err = cfg.validate_cluster_safety().unwrap_err();
        assert!(err.contains("replication_degraded_mode"));
        assert!(err.contains("best_effort"));
        assert!(err.contains("replication_factor = 3"));
    }

    #[test]
    fn best_effort_with_rf_2_is_rejected() {
        let cfg = ServerConfig {
            node_id: 2,
            replication_factor: 2,
            replication_degraded_mode: "best_effort".to_string(),
            ..ServerConfig::default()
        };

        let err = cfg.validate_cluster_safety().unwrap_err();
        assert!(err.contains("replication_factor = 2"));
    }

    #[test]
    fn best_effort_with_rf_1_is_accepted() {
        // RF=1 means no replicas — best_effort is a no-op and permitted.
        let cfg = ServerConfig {
            node_id: 7,
            replication_factor: 1,
            replication_degraded_mode: "best_effort".to_string(),
            ..ServerConfig::default()
        };

        cfg.validate_cluster_safety()
            .expect("RF=1 with best_effort must validate successfully");
    }

    #[test]
    fn reject_mode_with_rf_3_is_accepted() {
        let cfg = ServerConfig {
            node_id: 7,
            replication_factor: 3,
            replication_degraded_mode: "reject".to_string(),
            ..ServerConfig::default()
        };

        cfg.validate_cluster_safety()
            .expect("reject mode must always validate");
    }

    #[test]
    fn default_config_validates_cluster_safety() {
        let cfg = ServerConfig::default();
        cfg.validate_cluster_safety()
            .expect("default config must validate");
    }

    #[test]
    fn default_block_height_retention_passes_validation() {
        let cfg = ServerConfig::default();
        cfg.validate_block_height_retention()
            .expect("default retention (288) must be well under the bound");
    }

    #[test]
    fn block_height_retention_at_bound_is_accepted() {
        let cfg = ServerConfig {
            block_height_retention: ServerConfig::MAX_BLOCK_HEIGHT_RETENTION,
            ..ServerConfig::default()
        };
        cfg.validate_block_height_retention()
            .expect("exactly at the bound is allowed");
    }

    #[test]
    fn block_height_retention_u32_max_is_rejected() {
        let cfg = ServerConfig {
            block_height_retention: u32::MAX,
            ..ServerConfig::default()
        };
        let err = cfg
            .validate_block_height_retention()
            .expect_err("u32::MAX retention must be rejected");
        assert!(err.contains("block_height_retention"));
        assert!(err.contains("exceeds maximum"));
    }

    #[test]
    fn block_height_retention_one_past_bound_is_rejected() {
        let cfg = ServerConfig {
            block_height_retention: ServerConfig::MAX_BLOCK_HEIGHT_RETENTION + 1,
            ..ServerConfig::default()
        };
        let err = cfg.validate_block_height_retention().unwrap_err();
        assert!(err.contains("block_height_retention"));
    }
}
