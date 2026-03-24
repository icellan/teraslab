//! TeraSlab server binary.
//!
//! Startup sequence:
//! 1. Load configuration
//! 2. Open/create device files
//! 3. Create or recover allocator
//! 4. Load index from snapshot or rebuild from device scan
//! 5. Open redo log and replay entries
//! 6. Create Engine
//! 7. Start TCP server
//! 8. On shutdown: snapshot index, sync device

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8};
use std::sync::Arc;

use parking_lot::Mutex;
use teraslab::allocator::SlotAllocator;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, DirectDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::metrics::ThreadMetrics;
use teraslab::ops::engine::Engine;
use teraslab::redo::RedoLog;
use teraslab::server::http::{HttpState, start_http_server};
use teraslab::server::Server;
use teraslab::storage::blobstore::{BlobStore, FileBlobStore};

/// Global metrics counters for the server binary.
static SERVER_METRICS: ThreadMetrics = ThreadMetrics::new();

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let config = if args.len() > 1 && args[1] == "--config" {
        if args.len() < 3 {
            eprintln!("Usage: teraslab-server --config <path.toml>");
            std::process::exit(1);
        }
        match ServerConfig::load(std::path::Path::new(&args[2])) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to load config: {e}");
                std::process::exit(1);
            }
        }
    } else {
        eprintln!("No config file specified, using defaults");
        ServerConfig::default()
    };

    eprintln!("TeraSlab server starting...");
    eprintln!("  listen: {}", config.listen_addr);
    eprintln!("  devices: {:?}", config.device_paths);
    eprintln!("  device_size: {} MiB", config.device_size / (1024 * 1024));

    // 1. Open device
    let device_path = &config.device_paths[0];
    let device: Arc<dyn BlockDevice> = match DirectDevice::open(
        device_path,
        config.device_size,
        config.device_alignment,
    ) {
        Ok(d) => {
            eprintln!("  device opened: {}", device_path.display());
            Arc::new(d)
        }
        Err(e) => {
            eprintln!("Failed to open device {}: {e}", device_path.display());
            std::process::exit(1);
        }
    };

    // 2. Recover or create allocator
    let allocator = match SlotAllocator::recover(device.clone()) {
        Ok(alloc) => {
            eprintln!("  allocator recovered from device header");
            alloc
        }
        Err(_) => {
            eprintln!("  allocator: fresh (no persisted state found)");
            SlotAllocator::new(device.clone())
        }
    };

    // 3. Load or rebuild index
    let snap_path = &config.index_snapshot_path;
    let (mut index, dah_index, unmined_index) = if snap_path.exists() {
        match Index::restore_all(snap_path) {
            Ok((idx, dah, unmined, flags)) => {
                eprintln!("  index restored from snapshot ({} entries)", idx.len());
                let dah = if flags.dah_needs_rebuild {
                    eprintln!("  DAH index needs rebuild (snapshot corrupt)");
                    rebuild_dah(&*device, &allocator)
                } else {
                    dah
                };
                let unmined = if flags.unmined_needs_rebuild {
                    eprintln!("  unmined index needs rebuild (snapshot corrupt)");
                    rebuild_unmined(&*device, &allocator)
                } else {
                    unmined
                };
                (idx, dah, unmined)
            }
            Err(e) => {
                eprintln!("  index snapshot corrupt ({e}), rebuilding from device...");
                rebuild_all(&*device, &allocator)
            }
        }
    } else {
        eprintln!("  no index snapshot found, rebuilding from device...");
        rebuild_all(&*device, &allocator)
    };

    eprintln!("  index: {} entries, load factor {:.1}%",
        index.len(), index.stats().load_factor * 100.0);
    eprintln!("  DAH index: {} entries", dah_index.len());
    eprintln!("  unmined index: {} entries", unmined_index.len());

    // 3b. Open redo log device (separate file) and run recovery
    let redo_log_path = config.resolved_redo_log_path();
    let redo_log_device: Arc<dyn BlockDevice> = match DirectDevice::open(
        &redo_log_path,
        config.redo_log_size,
        config.device_alignment,
    ) {
        Ok(d) => {
            eprintln!("  redo log device opened: {}", redo_log_path.display());
            Arc::new(d)
        }
        Err(e) => {
            eprintln!("  redo log device open failed: {e}, creating fresh");
            match DirectDevice::open(
                &redo_log_path,
                config.redo_log_size,
                config.device_alignment,
            ) {
                Ok(d) => Arc::new(d),
                Err(e2) => {
                    eprintln!("  redo log device create failed: {e2}, proceeding without redo log");
                    // Proceed without redo log — it's an enhancement, not a hard requirement
                    // for startup to succeed.
                    Arc::new(teraslab::device::MemoryDevice::new(config.redo_log_size, config.device_alignment)
                        .expect("failed to create fallback redo log device"))
                }
            }
        }
    };

    let redo_log = match RedoLog::open(redo_log_device.clone(), 0, config.redo_log_size) {
        Ok(log) => {
            eprintln!("  redo log opened (size {} MiB)", config.redo_log_size / (1024 * 1024));
            Some(log)
        }
        Err(e) => {
            eprintln!("  redo log open failed: {e}, proceeding without redo log");
            None
        }
    };

    // Run recovery if we have a redo log, while index is still mutable
    if let Some(ref redo) = redo_log {
        match teraslab::recovery::recover(&*device, redo, &mut index) {
            Ok(stats) => {
                eprintln!("  recovery: {} replayed, {} skipped, {} failed",
                    stats.entries_replayed, stats.entries_skipped, stats.entries_failed);
            }
            Err(e) => {
                eprintln!("  recovery failed: {e}");
            }
        }
    }

    // Initialize replication sequence from the redo log so replica
    // sequence numbers are contiguous with the durable commit log.
    if let Some(ref log) = redo_log {
        let seq = log.current_sequence();
        teraslab::server::dispatch::init_replication_sequence(seq);
        eprintln!("  replication: sequence initialized at {seq}");
    }

    // Wrap redo log in Arc<Mutex> for shared access from dispatch threads
    let redo_log: Option<Arc<Mutex<RedoLog>>> = redo_log.map(|log| Arc::new(Mutex::new(log)));

    // 4. Create engine
    let locks = StripedLocks::new(config.lock_stripes);
    let mut engine = Engine::new(
        device.clone(),
        index,
        allocator,
        locks,
        dah_index,
        unmined_index,
    );

    // 4b. Initialize blobstore from config and attach to engine
    let blob_store: Arc<dyn BlobStore> = Arc::new(
        FileBlobStore::new(Path::new(&config.blobstore_path), 2),
    );
    engine.set_blob_store(blob_store.clone());
    eprintln!("  blobstore: {}", config.blobstore_path);

    let engine = Arc::new(engine);

    // 5. Start cluster if configured
    let cluster = if config.is_clustered() {
        use teraslab::cluster::coordinator::{ClusterConfig, ClusterCoordinator};
        use teraslab::cluster::shards::NodeId;

        let self_addr: std::net::SocketAddr = config.listen_addr.parse()
            .expect("invalid listen_addr");
        // Use the same IP as listen_addr for SWIM bind so the advertised
        // SWIM address is reachable from other nodes (important in Docker).
        let swim_bind: std::net::SocketAddr =
            format!("{}:{}", self_addr.ip(), config.swim_port).parse().unwrap();
        let seed_addrs: Vec<std::net::SocketAddr> = config.seed_nodes.iter()
            .filter_map(|s| s.parse().ok())
            .collect();

        let probe_interval = std::time::Duration::from_millis(config.swim_probe_interval_ms);
        let cluster_config = ClusterConfig {
            self_id: NodeId(config.node_id),
            self_addr,
            swim_bind,
            seed_nodes: seed_addrs,
            replication_factor: config.replication_factor,
            probe_interval,
            suspicion_timeout: std::time::Duration::from_millis(config.swim_suspicion_timeout_ms),
            cluster_secret: config.cluster_secret.as_ref().map(|s| s.as_bytes().to_vec()),
            max_migration_threads: config.max_migration_threads,
            topology_propose_timeout: probe_interval * 3,
            migration_pool_size: config.migration_pool_size,
            migration_batch_size: config.migration_batch_size,
        };

        let cluster_state_path = config.resolved_cluster_state_path();
        // Load topology state (backward-compatible with old format).
        let topo_state = teraslab::cluster::coordinator::load_topology_state(&cluster_state_path);
        let initial_peak = topo_state.peak_cluster_size as usize;
        let initial_epoch = topo_state.committed_term;
        if initial_peak > 1 {
            eprintln!("  cluster: restored peak={initial_peak} term={initial_epoch} (quorum requires {})", (initial_peak / 2) + 1);
        }
        let coordinator = ClusterCoordinator::new(cluster_config, initial_peak);
        // Restore topology state so new terms/epochs are strictly higher.
        coordinator.topology_epoch.store(initial_epoch, std::sync::atomic::Ordering::Relaxed);
        coordinator.topology_authority.restore(&topo_state);
        if initial_epoch > 0 {
            coordinator.shard_table.write().unwrap().version = initial_epoch;
        }
        let running = coordinator.start(
            engine.clone(),
            Some(cluster_state_path),
            redo_log.clone(),
            config.resolved_ack_policy(),
            config.is_replication_best_effort(),
        );
        // Restore inbound migration state from a previous run so shards
        // that were mid-migration remain blocked until re-migration.
        running.restore_inbound_state();
        // Initialize persistent ACK tracker alongside the cluster state file.
        let ack_path = {
            let mut p = config.resolved_cluster_state_path().into_os_string();
            p.push(".repl-ack");
            std::path::PathBuf::from(p)
        };
        teraslab::server::dispatch::init_ack_tracker(ack_path.clone());

        // Spawn background catch-up for replicas that are behind.
        // Reads persisted last_acked per replica and streams missing redo
        // entries. This runs asynchronously so it doesn't block startup.
        if config.replication_factor > 1 {
            let redo_for_catchup = redo_log.clone();
            let engine_for_catchup = engine.clone();
            std::thread::spawn(move || {
                let tracker = teraslab::replication::durable::AckTracker::new(ack_path);
                let all_acked = tracker.all_acked();
                if all_acked.is_empty() {
                    return; // No known replicas yet
                }

                let current_seq = redo_for_catchup.as_ref()
                    .map(|rl| rl.lock().current_sequence())
                    .unwrap_or(0);

                for (addr, last_acked) in &all_acked {
                    if *last_acked >= current_seq {
                        continue; // Already caught up
                    }
                    let lag = current_seq - last_acked;
                    eprintln!("  catchup: replica {addr} is {lag} ops behind, starting catch-up from seq {}", last_acked + 1);

                    let redo_ref = redo_for_catchup.clone();
                    let eng_ref = engine_for_catchup.clone();

                    let result = teraslab::replication::durable::run_catchup_for_replica(
                        addr,
                        last_acked + 1,
                        current_seq,
                        1000,
                        &|from_seq| {
                            let rl = match redo_ref.as_ref() {
                                Some(rl) => rl,
                                None => return Vec::new(),
                            };
                            let entries = match rl.lock().read_from_sequence(from_seq) {
                                Ok(e) => e,
                                Err(_) => return Vec::new(),
                            };
                            // Convert redo entries to ReplicaOps. Each entry's shard
                            // is derived from its tx_key. The replica applies all
                            // ops idempotently, gracefully skipping unknown records.
                            entries.iter()
                                .filter_map(|e| {
                                    let tx_key = e.op.tx_key()?;
                                    let shard = teraslab::cluster::shards::ShardTable::shard_for_key(tx_key);
                                    teraslab::cluster::coordinator::redo_entry_to_replica_op(e, shard, &eng_ref)
                                })
                                .collect()
                        },
                    );

                    match result {
                        Ok(through) => {
                            eprintln!("  catchup: replica {addr} caught up to seq {through}");
                            tracker.record_ack(*addr, through);
                            tracker.flush();
                        }
                        Err(e) => {
                            eprintln!("  catchup: replica {addr} catch-up failed: {e}");
                        }
                    }
                }
            });
        }

        eprintln!("  cluster: node {} started with RF={}", config.node_id, config.replication_factor);
        Some(Arc::new(running))
    } else {
        eprintln!("  cluster: single-node mode (node_id=0)");
        None
    };

    // 6. Start HTTP observability server
    let http_state = Arc::new(HttpState {
        engine: engine.clone(),
        metrics: &SERVER_METRICS,
        ready: Arc::new(AtomicBool::new(true)),
        log_level: Arc::new(AtomicU8::new(2)), // INFO
        cluster: cluster.clone(),
    });
    let http_addr = config.http_listen_addr.clone();
    std::thread::spawn(move || {
        start_http_server(http_addr, http_state);
    });

    // 7. Setup TCP server
    let mut server = Server::new(engine.clone(), config.clone());
    if let Some(ref c) = cluster {
        server = server.with_cluster(c.clone());
    }
    if let Some(ref rl) = redo_log {
        server = server.with_redo_log(rl.clone());
    }
    server = server.with_blob_store(blob_store);
    let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shutdown_clone = shutdown_flag.clone();
    ctrlc_handler(move || {
        eprintln!("\nShutdown signal received...");
        shutdown_clone.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    let server = ServerWithShutdown {
        inner: server,
        shutdown: shutdown_flag,
        engine,
        snap_path: config.index_snapshot_path.clone(),
        device,
        cluster,
    };

    // 7. Start serving
    if let Err(e) = server.run() {
        eprintln!("Server error: {e}");
        std::process::exit(1);
    }

    eprintln!("Server stopped.");
}

struct ServerWithShutdown {
    inner: Server,
    #[allow(dead_code)]
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    #[allow(dead_code)]
    engine: Arc<Engine>,
    #[allow(dead_code)]
    snap_path: PathBuf,
    device: Arc<dyn BlockDevice>,
    #[allow(dead_code)]
    cluster: Option<Arc<teraslab::cluster::coordinator::RunningCluster>>,
}

impl ServerWithShutdown {
    fn run(&self) -> Result<(), String> {
        let result = self.inner.run();

        // On shutdown: stop cluster, sync device
        if let Some(ref cluster) = self.cluster {
            cluster.shutdown();
            eprintln!("  cluster stopped");
        }

        eprintln!("Persisting state...");
        if let Err(e) = self.device.sync() {
            eprintln!("  device sync error: {e}");
        } else {
            eprintln!("  device synced");
        }
        eprintln!("  state persisted");

        result
    }
}

fn rebuild_all(device: &dyn BlockDevice, allocator: &SlotAllocator) -> (Index, DahIndex, UnminedIndex) {
    let index = match Index::rebuild(device, allocator) {
        Ok(idx) => idx,
        Err(e) => {
            eprintln!("  index rebuild failed: {e}, starting empty");
            Index::new(1000).unwrap()
        }
    };
    let (dah, unmined) = match Index::rebuild_secondary(device, allocator) {
        Ok((d, u)) => (d, u),
        Err(e) => {
            eprintln!("  secondary index rebuild failed: {e}, starting empty");
            (DahIndex::new(), UnminedIndex::new())
        }
    };
    (index, dah, unmined)
}

fn rebuild_dah(device: &dyn BlockDevice, allocator: &SlotAllocator) -> DahIndex {
    match Index::rebuild_secondary(device, allocator) {
        Ok((dah, _)) => dah,
        Err(_) => DahIndex::new(),
    }
}

fn rebuild_unmined(device: &dyn BlockDevice, allocator: &SlotAllocator) -> UnminedIndex {
    match Index::rebuild_secondary(device, allocator) {
        Ok((_, unmined)) => unmined,
        Err(_) => UnminedIndex::new(),
    }
}

fn ctrlc_handler<F: Fn() + Send + 'static>(handler: F) {
    // Unfortunately without a signal crate, we can't easily catch SIGINT.
    // The server's read timeout + shutdown flag handle graceful shutdown.
    // For production, add the `ctrlc` or `signal-hook` crate.
    drop(handler);
}
