//! Server configuration.

use serde::Deserialize;
use std::path::PathBuf;

/// TeraSlab server configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// TCP listen address for the binary wire protocol.
    pub listen_addr: String,

    /// Device file paths for data storage.
    /// Each path is a file that will be created if it doesn't exist.
    pub device_paths: Vec<PathBuf>,

    /// Size of each device file in bytes (only used when creating new files).
    pub device_size: u64,

    /// Device I/O alignment in bytes (4096 for most NVMe/SSDs).
    pub device_alignment: usize,

    /// Size of the redo log region in bytes.
    pub redo_log_size: u64,

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

    /// Block height retention for DAH evaluation.
    pub block_height_retention: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:3300".to_string(),
            device_paths: vec![PathBuf::from("teraslab-data.dat")],
            device_size: 1024 * 1024 * 1024, // 1 GiB
            device_alignment: 4096,
            redo_log_size: 64 * 1024 * 1024, // 64 MiB
            index_snapshot_path: PathBuf::from("teraslab-index.snap"),
            expected_records: 100_000,
            lock_stripes: 65536,
            max_batch_size: 8192,
            max_connections: 1024,
            block_height_retention: 288,
        }
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
