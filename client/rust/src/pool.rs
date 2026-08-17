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
use teraslab::protocol::opcodes::{OP_PING, STATUS_OK};
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
    /// Per-request round-trip timeout (default: 30s).
    pub request_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_conns: 2,
            max_conns: 16,
            dial_timeout: Duration::from_secs(5),
            health_check: Duration::from_secs(15),
            request_timeout: Duration::from_secs(30),
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
        if self.request_timeout == Duration::ZERO {
            self.request_timeout = Duration::from_secs(30);
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
    /// W11 FIX 4 — whether the LAST dial attempt to `addr` failed.
    ///
    /// Set by every dial site (`create_conn` and the health loop's
    /// replenish) and cleared by the next successful one, so it reads as
    /// "this node is currently unreachable, and we know because we tried".
    /// Shared with the health task so a node that comes back clears the flag
    /// on the next health tick even while no request is routed to it.
    ///
    /// The cluster router consults this ONLY for a node the partition map
    /// advertises dead ([`crate::cluster::Cluster::pool_for_shard`]): an
    /// advertised-dead master is usually just SUSPECT and perfectly
    /// reachable, so observed unreachability — not the advertised flag — is
    /// what licenses refusing to route to it.
    dial_failing: Arc<AtomicBool>,
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
        let dial_failing = Arc::new(AtomicBool::new(false));
        let health_conns = Arc::clone(&conns);
        let health_failing = Arc::clone(&dial_failing);
        let health_addr = addr.clone();
        let health_config = config.clone();

        let health_task = tokio::spawn(async move {
            health_loop(
                health_addr,
                health_config,
                health_conns,
                health_failing,
                close_rx,
            )
            .await;
        });

        Self {
            addr,
            config,
            conns,
            robin: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            dial_failing,
            _health_task: health_task,
            close_tx,
        }
    }

    /// The target server address this pool dials.
    ///
    /// Used by the FU#4 streaming-read path, which opens a dedicated
    /// (non-pooled) connection so a multi-frame server-push burst does not
    /// perturb the pipelined read loop.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// The dial timeout configured for this pool (used when opening the
    /// dedicated streaming-read connection).
    pub fn dial_timeout(&self) -> std::time::Duration {
        self.config.dial_timeout
    }

    /// The per-request round-trip timeout configured for this pool.
    pub fn request_timeout(&self) -> std::time::Duration {
        self.config.request_timeout
    }

    /// W11 FIX 4 — whether the most recent dial to this pool's address
    /// FAILED (and no dial has succeeded since).
    ///
    /// `false` before the first dial: absence of proof is not proof of
    /// unreachability, so a fresh pool is always given one chance.
    pub fn dial_failing(&self) -> bool {
        self.dial_failing.load(Ordering::Acquire)
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

        let dialed = PipeConn::dial(
            &self.addr,
            self.config.dial_timeout,
            self.config.request_timeout,
        )
        .await;
        // W11 FIX 4 — record the outcome before propagating it, so the
        // cluster router can stop re-dialling a node proven unreachable.
        self.dial_failing.store(dialed.is_err(), Ordering::Release);
        let c = Arc::new(dialed?);

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
    dial_failing: Arc<AtomicBool>,
    mut close_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(config.health_check);
    interval.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            _ = interval.tick() => {
                check_health(&addr, &config, &conns, &dial_failing).await;
            }
            _ = close_rx.changed() => {
                return;
            }
        }
    }
}

/// Actively probe connections, drop dead ones, and replenish to min_conns.
///
/// A connection only tracks a local `alive` flag, so a half-open TCP peer that
/// went away without an explicit close still reports `alive() == true`. To
/// surface those, each live connection is probed with an `OP_PING` round-trip
/// (matching the Go pool's `checkHealth`); a failed ping or non-OK status
/// marks the connection dead and it is dropped.
async fn check_health(
    addr: &str,
    config: &PoolConfig,
    conns: &Arc<Mutex<Vec<Arc<PipeConn>>>>,
    dial_failing: &Arc<AtomicBool>,
) {
    // Snapshot connections so the ping round-trips happen without holding the
    // pool lock.
    let snapshot: Vec<Arc<PipeConn>> = {
        let guard = conns.lock();
        guard.clone()
    };

    // Identify dead connections by their Arc pointer address (a stable
    // identity key; using `usize` keeps the future `Send` across awaits).
    let mut dead: Vec<usize> = Vec::new();
    for c in &snapshot {
        if !c.alive() {
            dead.push(Arc::as_ptr(c) as usize);
            continue;
        }
        match c.round_trip(OP_PING, 0, Vec::new()).await {
            Ok(resp) if resp.status == STATUS_OK => {
                // W11 P1-C — a successful PING is PROOF of reachability and
                // must clear `dial_failing`, not just a successful dial. A
                // transiently-failed replacement dial can latch the flag while
                // live connections remain; from then on `deficit` is 0, the
                // replenish loop below never runs, and the flag would stay set
                // forever — so the first time SWIM suspects this node and the
                // map advertises it dead, the router would refuse a perfectly
                // reachable master for the whole suspicion window.
                dial_failing.store(false, Ordering::Release);
            }
            _ => {
                // Failed or non-OK ping: the peer is gone. Close so the
                // background read loop is aborted and waiters wake.
                c.close().await;
                dead.push(Arc::as_ptr(c) as usize);
            }
        }
    }

    let deficit = {
        let mut guard = conns.lock();
        // Remove connections we found dead during probing, plus any that
        // died concurrently.
        guard.retain(|c| c.alive() && !dead.contains(&(Arc::as_ptr(c) as usize)));
        let current = guard.len();
        config.min_conns.saturating_sub(current)
    };

    // Replenish to min_conns.
    //
    // W11 FIX 4 — together with the PING arm above, this is what CLEARS a
    // `dial_failing` node that has come back: the cluster router refuses to
    // route to a proven-unreachable dead-advertised master, so no
    // request-path dial would ever retry it.
    //
    // W11 P1-C — a zero deficit does NOT imply the flag already reads false
    // (a failed replacement dial can latch it while live connections remain),
    // which is exactly why the successful-PING arm above clears it and this
    // loop is not the only clearing path.
    for _ in 0..deficit {
        match PipeConn::dial(addr, config.dial_timeout, config.request_timeout).await {
            Ok(c) => {
                dial_failing.store(false, Ordering::Release);
                let mut guard = conns.lock();
                guard.push(Arc::new(c));
            }
            Err(_) => {
                dial_failing.store(true, Ordering::Release);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Minimal server that answers every frame with `STATUS_OK` and an empty
    /// payload — enough for the health loop's `OP_PING` round-trip.
    async fn spawn_ping_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    loop {
                        let mut len_buf = [0u8; 4];
                        if sock.read_exact(&mut len_buf).await.is_err() {
                            return;
                        }
                        let total = u32::from_le_bytes(len_buf) as usize;
                        if total < 12 {
                            return;
                        }
                        let mut body = vec![0u8; total];
                        if sock.read_exact(&mut body).await.is_err() {
                            return;
                        }
                        let request_id = u64::from_le_bytes(body[0..8].try_into().unwrap());
                        let mut out = 9u32.to_le_bytes().to_vec();
                        out.extend_from_slice(&request_id.to_le_bytes());
                        out.push(STATUS_OK);
                        if sock.write_all(&out).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    fn test_pool_config(min_conns: usize) -> PoolConfig {
        PoolConfig {
            min_conns,
            max_conns: 16,
            dial_timeout: Duration::from_millis(300),
            request_timeout: Duration::from_secs(5),
            // Long enough that the background loop never races the explicit
            // `check_health` calls these tests drive.
            health_check: Duration::from_secs(3600),
        }
    }

    /// W11 P1-C — `dial_failing` must be cleared by a SUCCESSFUL PING, not
    /// only by a successful dial.
    ///
    /// Reachable staleness: a pool holding several connections loses one,
    /// `create_conn` dials a replacement, and THAT dial fails transiently (a
    /// server-side per-IP rejection under load, a brief ECONNREFUSED during a
    /// rolling restart) — so `dial_failing` latches true while live
    /// connections remain. Every later health tick then finds `deficit == 0`,
    /// never enters the replenish loop, and the flag never clears. Minutes
    /// later SWIM suspects that node, the map advertises `is_alive = 0`, and
    /// `Cluster::pool_for_shard` refuses a perfectly reachable master for the
    /// whole suspicion window — the P0 that
    /// `dead_advertised_master_keeps_its_pool` pins, re-opened via a stale
    /// flag.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_ping_clears_a_stale_dial_failing_flag() {
        let addr = spawn_ping_server().await;
        // min_conns = 1 so a single live connection leaves deficit == 0 and
        // the replenish loop — the ONLY pre-fix clearing path — never runs.
        let config = test_pool_config(1);
        let pool = ConnPool::new(addr.clone(), config.clone());

        // One healthy connection, established by a successful dial.
        pool.get().await.expect("the ping server is reachable");
        assert!(
            !pool.dial_failing(),
            "a successful dial leaves the flag clear"
        );
        assert_eq!(pool.conns.lock().len(), 1, "one live connection is pooled");

        // A transient replacement dial fails while that connection stays live.
        pool.dial_failing.store(true, Ordering::Release);

        check_health(&addr, &config, &pool.conns, &pool.dial_failing).await;

        assert_eq!(
            pool.conns.lock().len(),
            1,
            "the live connection pinged OK, so deficit stayed 0 and the \
             replenish loop never ran — the pre-fix clearing path is absent",
        );
        assert!(
            !pool.dial_failing(),
            "a successful health PING proves the node is reachable and MUST \
             clear the flag, or routing to it stays refused forever",
        );
        pool.close().await;
    }

    /// The flag must still LATCH when the node is genuinely gone: no live
    /// connection to ping, and the replenish dial fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn health_check_latches_dial_failing_when_the_node_is_unreachable() {
        // Bind then drop, so the port is closed and dials are refused.
        let addr = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().to_string()
        };
        let config = test_pool_config(1);
        let pool = ConnPool::new(addr.clone(), config.clone());
        assert!(!pool.dial_failing(), "a fresh pool is given one chance");

        check_health(&addr, &config, &pool.conns, &pool.dial_failing).await;

        assert!(
            pool.dial_failing(),
            "a failed replenish dial must record unreachability",
        );
        pool.close().await;
    }
}
