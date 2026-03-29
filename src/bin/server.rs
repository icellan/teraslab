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
use teraslab::config::IndexBackendMode;
use teraslab::index::{DahBackend, DahIndex, PrimaryBackend, UnminedBackend, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::metrics::{ThreadHistograms, ThreadMetrics};
use teraslab::ops::engine::Engine;
use teraslab::redo::RedoLog;
use teraslab::server::http::{HttpState, start_http_server};
use teraslab::server::Server;
use teraslab::storage::blobstore::{BlobStore, FileBlobStore};

/// Detect the first non-loopback IPv4 address on this host.
/// Used when `listen_addr` is `0.0.0.0` and no `advertise_addr` is configured,
/// so the node can advertise a reachable IP to cluster peers.
fn detect_local_ip() -> Option<std::net::IpAddr> {
    // Connect a UDP socket to a public IP to discover our default route address.
    // No traffic is sent — this just causes the OS to select the outgoing interface.
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:53").ok()?;
    socket.local_addr().ok().map(|a| a.ip())
}

/// Global metrics counters for the server binary.
static SERVER_METRICS: ThreadMetrics = ThreadMetrics::new();

/// Global latency histograms for the server binary.
static SERVER_HISTOGRAMS: ThreadHistograms = ThreadHistograms::new();

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

    teraslab::server::dispatch::init_dispatch_metrics(&SERVER_METRICS);

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

    // Validate device_id config format before using it.
    if let Err(e) = config.validate_device_id() {
        eprintln!("FATAL: invalid config: {e}");
        std::process::exit(1);
    }

    // 2. Recover or create allocator
    let allocator = match SlotAllocator::recover(device.clone()) {
        Ok(alloc) => {
            eprintln!("  allocator recovered from device header");
            let device_id_hex = alloc.device_id_hex();
            eprintln!("  device identity: {device_id_hex}");

            if let Some(ref expected) = config.device_id {
                if expected != &device_id_hex {
                    eprintln!("FATAL: device identity mismatch!");
                    eprintln!("  config expects: {expected}");
                    eprintln!("  device contains: {device_id_hex}");
                    eprintln!("  This likely means the device path points to the wrong device.");
                    std::process::exit(1);
                }
                eprintln!("  device identity verified");
            }

            alloc
        }
        Err(_) => {
            match SlotAllocator::new(device.clone()) {
                Ok(fresh) => {
                    let device_id_hex = fresh.device_id_hex();
                    eprintln!("  allocator: fresh (no persisted state found)");
                    eprintln!("  device identity: {device_id_hex}  (copy to config device_id to enable verification)");
                    fresh
                }
                Err(e) => {
                    eprintln!("Failed to create allocator: {e}");
                    std::process::exit(1);
                }
            }
        }
    };

    // 3. Load or rebuild index (backend selected by config)
    eprintln!("  index backend: {}", match &config.index.backend {
        IndexBackendMode::Memory => "memory",
        IndexBackendMode::Redb => "redb",
        IndexBackendMode::FileBacked => "file_backed",
    });

    let (mut index, dah_index, unmined_index): (PrimaryBackend, DahBackend, UnminedBackend) =
        if config.index.backend == IndexBackendMode::Redb {
            // ReDB on-disk backend
            let primary = match PrimaryBackend::restore_redb(&config.index) {
                Ok(idx) => {
                    eprintln!("  redb primary index opened ({} entries)", idx.len());
                    idx
                }
                Err(_) => {
                    eprintln!("  redb primary index not found, rebuilding from device...");
                    match PrimaryBackend::rebuild_redb(&config.index, &*device, &allocator) {
                        Ok(idx) => idx,
                        Err(e) => {
                            eprintln!("  redb rebuild failed: {e}, removing stale file and creating empty");
                            let _ = std::fs::remove_file(&config.index.redb_path);
                            PrimaryBackend::new_on_disk(&config.index).unwrap()
                        }
                    }
                }
            };
            let dah = match teraslab::index::redb_dah::RedbDahIndex::open(
                &config.index.redb_dah_path,
                config.index.redb_cache_size,
            ) {
                Ok(idx) => DahBackend::OnDisk(idx),
                Err(e) => {
                    eprintln!("  redb DAH index error: {e}, removing corrupt file and retrying");
                    let _ = std::fs::remove_file(&config.index.redb_dah_path);
                    match teraslab::index::redb_dah::RedbDahIndex::open(
                        &config.index.redb_dah_path,
                        config.index.redb_cache_size,
                    ) {
                        Ok(idx) => {
                            eprintln!("  redb DAH index: fresh database created");
                            DahBackend::OnDisk(idx)
                        }
                        Err(e2) => {
                            eprintln!("  redb DAH index: fresh creation also failed: {e2}, falling back to in-memory");
                            DahBackend::new_in_memory()
                        }
                    }
                }
            };
            let unmined = match teraslab::index::redb_unmined::RedbUnminedIndex::open(
                &config.index.redb_unmined_path,
                config.index.redb_cache_size,
            ) {
                Ok(idx) => UnminedBackend::OnDisk(idx),
                Err(e) => {
                    eprintln!("  redb unmined index error: {e}, removing corrupt file and retrying");
                    let _ = std::fs::remove_file(&config.index.redb_unmined_path);
                    match teraslab::index::redb_unmined::RedbUnminedIndex::open(
                        &config.index.redb_unmined_path,
                        config.index.redb_cache_size,
                    ) {
                        Ok(idx) => {
                            eprintln!("  redb unmined index: fresh database created");
                            UnminedBackend::OnDisk(idx)
                        }
                        Err(e2) => {
                            eprintln!("  redb unmined index: fresh creation also failed: {e2}, falling back to in-memory");
                            UnminedBackend::new_in_memory()
                        }
                    }
                }
            };
            (primary, dah, unmined)
        } else if config.index.backend == IndexBackendMode::FileBacked {
            // File-backed mmap backend
            let fb_path = &config.index.file_backed_path;
            let primary = if fb_path.exists() {
                match PrimaryBackend::restore_file_backed(fb_path, 1024) {
                    Ok(idx) => {
                        eprintln!("  file-backed index opened ({} entries)", idx.len());
                        idx
                    }
                    Err(e) => {
                        eprintln!("  file-backed index corrupt ({e}), rebuilding from device...");
                        match PrimaryBackend::rebuild_file_backed(fb_path, &*device, &allocator) {
                            Ok(idx) => idx,
                            Err(e2) => {
                                eprintln!("  file-backed rebuild failed: {e2}, removing stale file and creating empty");
                                let _ = std::fs::remove_file(fb_path);
                                PrimaryBackend::new_file_backed(fb_path, 1024).unwrap()
                            }
                        }
                    }
                }
            } else {
                eprintln!("  no file-backed index found, rebuilding from device...");
                match PrimaryBackend::rebuild_file_backed(fb_path, &*device, &allocator) {
                    Ok(idx) => idx,
                    Err(e) => {
                        eprintln!("  file-backed rebuild failed: {e}, creating empty");
                        PrimaryBackend::new_file_backed(fb_path, 1024).unwrap()
                    }
                }
            };
            // File-backed mode: secondary indexes stay in-memory
            let (dah, unmined) = match PrimaryBackend::rebuild_secondary(&*device, &allocator) {
                Ok((d, u)) => (d, u),
                Err(e) => {
                    eprintln!("  secondary index rebuild failed: {e}, starting empty");
                    (DahIndex::new(), UnminedIndex::new())
                }
            };
            (primary, DahBackend::from(dah), UnminedBackend::from(unmined))
        } else {
            // In-memory backend (default)
            let snap_path = &config.index_snapshot_path;
            let (idx, dah, unmined) = if snap_path.exists() {
                match PrimaryBackend::restore_all(snap_path) {
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
            (idx, DahBackend::from(dah), UnminedBackend::from(unmined))
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

        let bind_addr: std::net::SocketAddr = config.listen_addr.parse()
            .expect("invalid listen_addr");
        // Determine the address to advertise to other nodes.
        // If advertise_addr is set, use it. Otherwise, if listen_addr uses
        // 0.0.0.0 (common in Docker), auto-detect a non-loopback IP.
        let self_addr: std::net::SocketAddr = if let Some(ref adv) = config.advertise_addr {
            adv.parse().expect("invalid advertise_addr")
        } else if bind_addr.ip().is_unspecified() {
            let ip = detect_local_ip().unwrap_or(bind_addr.ip());
            std::net::SocketAddr::new(ip, bind_addr.port())
        } else {
            bind_addr
        };
        let swim_bind: std::net::SocketAddr =
            format!("{}:{}", bind_addr.ip(), config.swim_port).parse().unwrap();
        let seed_addrs: Vec<std::net::SocketAddr> = config.seed_nodes.iter()
            .filter_map(|s| {
                // Try direct parse first (IP:port), then fall back to DNS resolution.
                s.parse().ok().or_else(|| {
                    use std::net::ToSocketAddrs;
                    s.to_socket_addrs().ok().and_then(|mut addrs| addrs.next())
                })
            })
            .collect();

        let probe_interval = std::time::Duration::from_millis(config.swim_probe_interval_ms);

        let cluster_state_path = config.resolved_cluster_state_path();
        // Load topology state (backward-compatible with old format).
        let topo_state = teraslab::cluster::coordinator::load_topology_state(&cluster_state_path);
        let initial_peak = topo_state.peak_cluster_size as usize;
        let initial_epoch = topo_state.committed_term;

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
            persisted_incarnation: topo_state.incarnation,
        };
        if initial_peak > 1 {
            eprintln!("  cluster: restored peak={initial_peak} term={initial_epoch} (quorum requires {})", (initial_peak / 2) + 1);
        }
        let coordinator = ClusterCoordinator::new(cluster_config, initial_peak);
        // Restore topology state so new terms/epochs are strictly higher.
        coordinator.topology_epoch.store(initial_epoch, std::sync::atomic::Ordering::Relaxed);
        coordinator.topology_authority.restore(&topo_state);
        if initial_epoch > 0 {
            coordinator.shard_table.write().version = initial_epoch;
        }
        let running = coordinator.start(
            engine.clone(),
            Some(cluster_state_path),
            redo_log.clone(),
            config.resolved_ack_policy(),
            config.is_replication_best_effort(),
        );
        // Restore migration state from a previous run so shards that were
        // mid-migration remain blocked (inbound) or tracked (outbound).
        running.restore_inbound_state();
        running.restore_outbound_state();
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

                    // Determine the earliest available redo sequence for
                    // truncation detection. If the redo log has wrapped past
                    // the replica's last-acked position, catch-up is impossible
                    // and the replica needs a full resync.
                    let first_avail_seq = redo_ref.as_ref().and_then(|rl| {
                        rl.lock().earliest_sequence().ok().flatten()
                    });

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
                        first_avail_seq,
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
    let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let http_port: u16 = config.http_listen_addr
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse().ok())
        .unwrap_or(9100);
    let http_state = Arc::new(HttpState {
        engine: engine.clone(),
        metrics: &SERVER_METRICS,
        histograms: &SERVER_HISTOGRAMS,
        ready: Arc::new(AtomicBool::new(true)),
        log_level: Arc::new(AtomicU8::new(2)), // INFO
        cluster: cluster.clone(),
        redo_log: redo_log.clone(),
        active_connections: active_connections.clone(),
        http_port,
    });
    let http_addr = config.http_listen_addr.clone();
    std::thread::spawn(move || {
        start_http_server(http_addr, http_state);
    });

    // 7. Setup TCP server
    let mut server = Server::new(engine.clone(), config.clone())
        .with_active_connections(active_connections);
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
    engine: Arc<Engine>,
    snap_path: PathBuf,
    device: Arc<dyn BlockDevice>,
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

        // Snapshot index to disk for fast restart
        match self.engine.snapshot_index(&self.snap_path) {
            Ok(()) => eprintln!("  index snapshot written to {}", self.snap_path.display()),
            Err(e) => eprintln!("  index snapshot failed: {e}"),
        }

        // Persist allocator freelist
        match self.engine.persist_allocator() {
            Ok(()) => eprintln!("  allocator state persisted"),
            Err(e) => eprintln!("  allocator persist failed: {e}"),
        }

        // Sync device
        if let Err(e) = self.device.sync() {
            eprintln!("  device sync error: {e}");
        } else {
            eprintln!("  device synced");
        }
        eprintln!("  state persisted");

        result
    }
}

fn rebuild_all(device: &dyn BlockDevice, allocator: &SlotAllocator) -> (PrimaryBackend, DahIndex, UnminedIndex) {
    let index = match PrimaryBackend::rebuild(device, allocator) {
        Ok(idx) => idx,
        Err(e) => {
            eprintln!("  index rebuild failed: {e}, starting empty");
            PrimaryBackend::new_in_memory(1000).unwrap()
        }
    };
    let (dah, unmined) = match PrimaryBackend::rebuild_secondary(device, allocator) {
        Ok((d, u)) => (d, u),
        Err(e) => {
            eprintln!("  secondary index rebuild failed: {e}, starting empty");
            (DahIndex::new(), UnminedIndex::new())
        }
    };
    (index, dah, unmined)
}

fn rebuild_dah(device: &dyn BlockDevice, allocator: &SlotAllocator) -> DahIndex {
    match PrimaryBackend::rebuild_secondary(device, allocator) {
        Ok((dah, _)) => dah,
        Err(_) => DahIndex::new(),
    }
}

fn rebuild_unmined(device: &dyn BlockDevice, allocator: &SlotAllocator) -> UnminedIndex {
    match PrimaryBackend::rebuild_secondary(device, allocator) {
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
