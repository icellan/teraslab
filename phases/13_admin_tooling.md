# Phase 13: Admin CLI and Web UI

## Goal

Build operator tooling: a command-line tool (`teraslab-cli`) for scripting and ops, and a browser-based dashboard for visual cluster management. Both consume the HTTP observability endpoints built in Phase 10.

## Dependencies

Phases 1-12 must be complete with all tests passing. The HTTP endpoints (`/status`, `/metrics`, `/debug/*`) from Phase 10 are the data source for both tools.

## Reference

- `specs/BSV_UTXO_STORE_SPEC.md` §14 (Admin CLI) and §15 (Admin Web UI)

## What to build

### 13.1 Admin CLI — `teraslab-cli`

Separate binary in the same Rust workspace. Uses `clap` for argument parsing.

#### Core commands

```rust
#[derive(Parser)]
enum Command {
    /// Cluster overview
    Status {
        #[arg(long)]
        node: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// List cluster nodes
    Nodes {
        #[arg(long)]
        json: bool,
    },
    /// Shard distribution
    Shards {
        #[arg(long)]
        node: Option<String>,
    },
    /// Storage capacity per device
    Storage {
        #[arg(long)]
        device: Option<u32>,
    },
    /// Memory breakdown
    Memory,
    /// Record inventory
    Records {
        #[arg(long)]
        external: bool,
    },
    /// Index statistics
    Index {
        #[arg(long)]
        secondary: bool,
    },
    /// Inspect a single record
    Record {
        txid: String,
        #[arg(long)]
        slots: bool,
        #[arg(long)]
        raw: bool,
    },
    /// Replication status
    Replication {
        #[arg(long)]
        history: bool,
    },
    /// Redo log info
    Redo {
        #[arg(long)]
        tail: Option<u32>,
    },
    /// Trigger cluster rebalance
    Rebalance {
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        execute: bool,
    },
    /// Drain a node (migrate shards off)
    Drain {
        node_id: String,
        #[arg(long)]
        cancel: bool,
    },
    /// Log level management
    LogLevel {
        level: Option<String>,
        #[arg(long)]
        target: Option<String>,
    },
    /// Quick benchmark / smoke test
    Bench {
        operation: String,  // "spend", "create"
        #[arg(long, default_value = "10000")]
        count: u32,
    },
    /// Health check all nodes
    Healthcheck,
}
```

#### Communication

- HTTP client (e.g., `reqwest`) for `/status`, `/metrics`, `/debug/*` endpoints
- Binary protocol client for `record` lookups and `bench` commands
- Default connection: `--addr localhost:9100` (HTTP) and `--data-addr localhost:3000` (binary)
- All commands support `--json` for machine-readable output

#### Output formatting

Table format by default using a lightweight table library. Example `teraslab-cli status` output:

```
TeraSlab Cluster Status
=======================
Nodes:       4 (all healthy)
Records:     2,500,000,000
UTXO slots:  12,000,000,000 (spent: 9.8B, unspent: 2.0B, pruned: 150M, frozen: 10M)
Storage:     2.9 TB / 3.8 TB (75.5%)
Memory:      107.3 GB (index: 107.0 GB, cache: 52 MB, other: 248 MB)
Throughput:  2.1M ops/sec (spend: 1.25M, setMined: 380K, create: 95K, get: 420K)
Replication: RF=2, all replicas synced (max lag: 150 ops)
Redo log:    12% utilized, checkpoint lag: 10,000 ops
```

### 13.2 Admin Web UI — `/ui/*`

Static SPA served by the same axum HTTP server (port 9100). Bundled into the binary via `rust-embed`.

#### Technology

- **No build toolchain**: vanilla HTML + CSS + JS, or a single-file lightweight framework (Alpine.js or htmx)
- **No npm, no webpack, no Node.js** — the entire UI is a handful of static files embedded in the Rust binary
- **Data**: polls `/status`, `/debug/*` JSON endpoints via `fetch()` every 2-5 seconds
- **Styling**: clean, minimal CSS. Dark/light mode toggle. Responsive for desktop and tablet.

#### Pages

1. **Dashboard** (`/ui/`) — cluster map, throughput gauges, storage bars, memory breakdown, record inventory, replication status, alerts panel
2. **Nodes** (`/ui/nodes`) — node table with drill-down
3. **Storage** (`/ui/storage`) — per-device capacity, freelist, I/O rates
4. **Records** (`/ui/records`) — search by txid, bulk stats
5. **Replication** (`/ui/replication`) — per-replica lag timeline, redo log status
6. **Migrations** (`/ui/migrations`) — active/historical migration progress
7. **Config** (`/ui/config`) — current config, runtime log level control

#### Alert conditions

Displayed in dashboard alerts panel and node status indicators:

| Condition | Default threshold | Severity |
|-----------|-------------------|----------|
| Device utilization > 85% | Configurable | Warning |
| Device utilization > 95% | Configurable | Critical |
| Index load factor > 0.85 | Fixed | Warning |
| Replication lag > 10K ops | Configurable | Warning |
| Replication lag > 100K ops | Configurable | Critical |
| Node unreachable | 3 missed heartbeats | Critical |
| Redo log utilization > 80% | Fixed | Warning |
| Freelist fragmentation > 30% | Configurable | Warning |

## Acceptance criteria

### CLI tests

```
- [ ] teraslab-cli status: returns formatted cluster overview
- [ ] teraslab-cli status --json: returns valid JSON matching /status schema
- [ ] teraslab-cli nodes: lists all nodes with state and shard count
- [ ] teraslab-cli storage: shows per-device capacity and utilization
- [ ] teraslab-cli memory: shows memory breakdown
- [ ] teraslab-cli records: shows record inventory with slot breakdown
- [ ] teraslab-cli record <txid>: shows metadata for existing record
- [ ] teraslab-cli record <txid> --slots: includes UTXO slot details
- [ ] teraslab-cli record <nonexistent>: shows "not found" error
- [ ] teraslab-cli index: shows index stats
- [ ] teraslab-cli replication: shows per-replica lag and state
- [ ] teraslab-cli redo: shows redo log position and utilization
- [ ] teraslab-cli log-level debug: changes level, verified via /debug/log-level GET
- [ ] teraslab-cli healthcheck: returns 0 exit code when all nodes healthy
- [ ] teraslab-cli healthcheck: returns non-zero when any node unreachable
- [ ] teraslab-cli bench spend --count 1000: completes and reports ops/sec
- [ ] All commands with --json: output parses as valid JSON
```

### Web UI tests

```
- [ ] GET /ui/ returns HTML page
- [ ] Dashboard loads and displays cluster data (not empty/broken)
- [ ] Dashboard auto-refreshes (data updates visible after mutation)
- [ ] Nodes page lists all nodes with correct state
- [ ] Storage page shows per-device bars with utilization
- [ ] Records page: search by txid returns record metadata
- [ ] Replication page shows per-replica lag
- [ ] Config page shows current log level, can change it
- [ ] Alert panel shows warning when device utilization > 85% (simulated)
- [ ] UI works in Chrome, Firefox, Safari (basic compatibility)
- [ ] UI is responsive (renders on 1024px and 1440px widths)
- [ ] Static files are embedded in binary (no external file dependencies)
```

### Integration tests

```
- [ ] Start cluster, use CLI to verify status, create records via binary protocol,
      verify records appear in CLI output and Web UI
- [ ] Drain a node via CLI, verify migrations visible in Web UI migrations page
- [ ] Simulate high utilization, verify alert appears in Web UI dashboard
```

## NOT in this phase

- No authentication on CLI or Web UI (can be added later)
- No multi-cluster management (one cluster per deployment)
- No historical time-series graphs (requires external Prometheus + Grafana; the UI shows current state only)
