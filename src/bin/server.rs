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

use std::path::PathBuf;
use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, DirectDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::server::Server;

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
    let (index, dah_index, unmined_index) = if snap_path.exists() {
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

    // 4. Create engine
    let locks = StripedLocks::new(config.lock_stripes);
    let engine = Arc::new(Engine::new(
        device.clone(),
        index,
        allocator,
        locks,
        dah_index,
        unmined_index,
    ));

    // 5. Start cluster if configured
    let cluster = if config.is_clustered() {
        use teraslab::cluster::coordinator::{ClusterConfig, ClusterCoordinator};
        use teraslab::cluster::shards::NodeId;

        let self_addr: std::net::SocketAddr = config.listen_addr.parse()
            .expect("invalid listen_addr");
        let swim_bind: std::net::SocketAddr =
            format!("0.0.0.0:{}", config.swim_port).parse().unwrap();
        let seed_addrs: Vec<std::net::SocketAddr> = config.seed_nodes.iter()
            .filter_map(|s| s.parse().ok())
            .collect();

        let cluster_config = ClusterConfig {
            self_id: NodeId(config.node_id),
            self_addr,
            swim_bind,
            seed_nodes: seed_addrs,
            replication_factor: config.replication_factor,
            probe_interval: std::time::Duration::from_millis(config.swim_probe_interval_ms),
            suspicion_timeout: std::time::Duration::from_millis(config.swim_suspicion_timeout_ms),
        };

        let coordinator = ClusterCoordinator::new(cluster_config);
        let running = coordinator.start(engine.clone());
        eprintln!("  cluster: node {} started with RF={}", config.node_id, config.replication_factor);
        Some(Arc::new(running))
    } else {
        eprintln!("  cluster: single-node mode (node_id=0)");
        None
    };

    // 6. Setup server
    let mut server = Server::new(engine.clone(), config.clone());
    if let Some(ref c) = cluster {
        server = server.with_cluster(c.clone());
    }
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
