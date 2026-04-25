//! Connection pool for a single TeraSlab node.
//!
//! Since each [`PipeConn`] supports pipelining, multiple tasks can share
//! connections. The pool round-robins across healthy connections and
//! maintains a minimum number of idle connections via a background health
//! check task.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::conn::PipeConn;
use crate::errors::ClientError;

/// Configuration for a per-node connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of idle connections to maintain (default: 2).
    pub min_conns: usize,
    /// Maximum number of connections (default: 16).
    pub max_conns: usize,
    /// Timeout for establishing new connections (default: 5s).
    pub dial_timeout: Duration,
    /// Interval for health-checking idle connections (default: 15s).
    pub health_check: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_conns: 2,
            max_conns: 16,
            dial_timeout: Duration::from_secs(5),
            health_check: Duration::from_secs(15),
        }
    }
}

impl PoolConfig {
    /// Apply defaults for any zero/unset fields.
    fn with_defaults(mut self) -> Self {
        if self.min_conns == 0 {
            self.min_conns = 2;
        }
        if self.max_conns == 0 {
            self.max_conns = 16;
        }
        if self.dial_timeout == Duration::ZERO {
            self.dial_timeout = Duration::from_secs(5);
        }
        if self.health_check == Duration::ZERO {
            self.health_check = Duration::from_secs(15);
        }
        self
    }
}

/// A connection pool managing pipelined connections to a single TeraSlab node.
///
/// Connections are round-robined for load distribution. A background task
/// periodically checks connection health and replenishes to `min_conns`.
pub(crate) struct ConnPool {
    /// Target server address.
    addr: String,
    /// Pool configuration.
    config: PoolConfig,
    /// Active connections, shared with the health check task.
    conns: Arc<Mutex<Vec<Arc<PipeConn>>>>,
    /// Round-robin counter.
    robin: AtomicU64,
    /// Whether the pool has been closed.
    closed: AtomicBool,
    /// Handle to the background health check task.
    _health_task: JoinHandle<()>,
    /// Channel to signal the health task to stop.
    close_tx: tokio::sync::watch::Sender<bool>,
}

impl ConnPool {
    /// Create a new connection pool for the given address.
    ///
    /// Starts a background health check task immediately.
    pub fn new(addr: String, config: PoolConfig) -> Self {
        let config = config.with_defaults();
        let (close_tx, close_rx) = tokio::sync::watch::channel(false);

        let conns: Arc<Mutex<Vec<Arc<PipeConn>>>> = Arc::new(Mutex::new(Vec::new()));
        let health_conns = Arc::clone(&conns);
        let health_addr = addr.clone();
        let health_config = config.clone();

        let health_task = tokio::spawn(async move {
            health_loop(health_addr, health_config, health_conns, close_rx).await;
        });

        Self {
            addr,
            config,
            conns,
            robin: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            _health_task: health_task,
            close_tx,
        }
    }

    /// Get a healthy connection from the pool, creating one if needed.
    ///
    /// Uses round-robin to distribute requests across connections.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::PoolClosed`] if the pool is closed, or
    /// [`ClientError::Connection`] if no connection could be established.
    pub async fn get(&self) -> Result<Arc<PipeConn>, ClientError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ClientError::PoolClosed);
        }

        // Try to find a healthy connection via round-robin.
        {
            let mut conns = self.conns.lock();
            let n = conns.len();
            if n > 0 {
                let idx = (self.robin.fetch_add(1, Ordering::Relaxed) % n as u64) as usize;
                if conns[idx].alive() {
                    return Ok(Arc::clone(&conns[idx]));
                }
                // Remove dead connection.
                conns.swap_remove(idx);
            }
        }

        // No healthy connection available -- create a new one.
        self.create_conn().await
    }

    /// Create a new connection and add it to the pool.
    async fn create_conn(&self) -> Result<Arc<PipeConn>, ClientError> {
        {
            let conns = self.conns.lock();
            if conns.len() >= self.config.max_conns {
                // At capacity -- try to find any alive one.
                for c in conns.iter() {
                    if c.alive() {
                        return Ok(Arc::clone(c));
                    }
                }
                // All dead -- we'll clear and recreate below.
                drop(conns);
                self.conns.lock().clear();
            }
        }

        let c = Arc::new(PipeConn::dial(&self.addr, self.config.dial_timeout).await?);

        {
            let mut conns = self.conns.lock();
            conns.push(Arc::clone(&c));
        }

        Ok(c)
    }

    /// Close the pool, stopping the health check and closing all connections.
    pub async fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.close_tx.send(true);

        let conns: Vec<Arc<PipeConn>> = {
            let mut guard = self.conns.lock();
            std::mem::take(&mut *guard)
        };
        for c in conns {
            c.close().await;
        }
    }
}

/// Background health check loop. Periodically removes dead connections
/// and replenishes to `min_conns`.
async fn health_loop(
    addr: String,
    config: PoolConfig,
    conns: Arc<Mutex<Vec<Arc<PipeConn>>>>,
    mut close_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(config.health_check);
    interval.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            _ = interval.tick() => {
                check_health(&addr, &config, &conns).await;
            }
            _ = close_rx.changed() => {
                return;
            }
        }
    }
}

/// Remove dead connections and replenish to min_conns.
async fn check_health(addr: &str, config: &PoolConfig, conns: &Arc<Mutex<Vec<Arc<PipeConn>>>>) {
    let deficit = {
        let mut guard = conns.lock();
        // Remove dead connections.
        guard.retain(|c| c.alive());
        let current = guard.len();
        config.min_conns.saturating_sub(current)
    };

    // Replenish to min_conns.
    for _ in 0..deficit {
        match PipeConn::dial(addr, config.dial_timeout).await {
            Ok(c) => {
                let mut guard = conns.lock();
                guard.push(Arc::new(c));
            }
            Err(_) => break,
        }
    }
}
