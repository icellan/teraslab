// TeraSlab Admin UI — vanilla JS SPA

(function() {
    'use strict';

    // ---------------------------------------------------------------------------
    // Data Store
    // ---------------------------------------------------------------------------

    const store = {
        status: null, index: null, freelist: null, redo: null,
        nodes: null, memory: null, records: null, replication: null,
        migrations: null, logLevel: null, topSnapshot: null,
        prevSnapshot: null, wsConnected: false
    };

    let refreshTimer = null;
    let ws = null;

    // ---------------------------------------------------------------------------
    // Formatting helpers
    // ---------------------------------------------------------------------------

    function fmt(n) {
        if (n == null) return '-';
        if (typeof n !== 'number') return String(n);
        if (n >= 1e12) return (n / 1e12).toFixed(1) + 'T';
        if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
        if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
        if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
        return n.toLocaleString();
    }

    function fmtBytes(n) {
        if (n == null) return '-';
        if (n >= 1e12) return (n / 1e12).toFixed(1) + ' TB';
        if (n >= 1e9) return (n / 1e9).toFixed(1) + ' GB';
        if (n >= 1e6) return (n / 1e6).toFixed(1) + ' MB';
        if (n >= 1e3) return (n / 1e3).toFixed(1) + ' KB';
        return n + ' B';
    }

    function fmtNs(ns) {
        if (ns == null || ns === 0) return '-';
        if (ns >= 1e9) return (ns / 1e9).toFixed(1) + 's';
        if (ns >= 1e6) return (ns / 1e6).toFixed(1) + 'ms';
        if (ns >= 1e3) return (ns / 1e3).toFixed(1) + 'us';
        return ns + 'ns';
    }

    function pct(val) {
        if (val == null) return '-';
        return (val * 100).toFixed(1) + '%';
    }

    function barClass(val) {
        if (val > 0.95) return 'red';
        if (val > 0.85) return 'yellow';
        return 'green';
    }

    function bar(value, label) {
        const p = Math.min(value * 100, 100);
        const cls = barClass(value);
        return '<div class="bar-wrap"><div class="bar-fill ' + cls + '" style="width:' + p + '%"></div></div>' +
               '<div style="font-size:0.8rem;color:var(--text-muted)">' + label + '</div>';
    }

    // ---------------------------------------------------------------------------
    // API
    // ---------------------------------------------------------------------------

    async function fetchJson(path) {
        try {
            const r = await fetch(path);
            if (!r.ok) return null;
            return await r.json();
        } catch { return null; }
    }

    async function refreshAll() {
        const [status, index, freelist, redo, nodes, memory, records, replication, migrations, logLevel] =
            await Promise.all([
                fetchJson('/status'),
                fetchJson('/debug/index'),
                fetchJson('/debug/freelist'),
                fetchJson('/debug/redo'),
                fetchJson('/admin/nodes'),
                fetchJson('/admin/memory'),
                fetchJson('/admin/records'),
                fetchJson('/admin/replication'),
                fetchJson('/admin/migration_status'),
                fetch('/debug/log-level').then(r => r.text()).catch(() => null),
            ]);
        Object.assign(store, { status, index, freelist, redo, nodes, memory, records, replication, migrations, logLevel });
        renderCurrentPage();
    }

    // ---------------------------------------------------------------------------
    // Alert evaluation
    // ---------------------------------------------------------------------------

    function evaluateAlerts() {
        const alerts = [];
        if (store.freelist) {
            if (store.freelist.utilization > 0.95)
                alerts.push({ severity: 'critical', message: 'Device utilization > 95%' });
            else if (store.freelist.utilization > 0.85)
                alerts.push({ severity: 'warning', message: 'Device utilization > 85%' });
        }
        if (store.index && store.index.load_factor > 0.85)
            alerts.push({ severity: 'warning', message: 'Index load factor > 85%' });
        if (store.redo && store.redo.available && store.redo.utilization > 0.80)
            alerts.push({ severity: 'warning', message: 'Redo log utilization > 80%' });
        return alerts;
    }

    function renderAlerts(alerts) {
        if (!alerts.length) return '';
        return '<div class="alerts">' +
            alerts.map(a => '<div class="alert ' + a.severity + '">' +
                (a.severity === 'critical' ? '&#9888; ' : '&#9888; ') + a.message + '</div>'
            ).join('') + '</div>';
    }

    // ---------------------------------------------------------------------------
    // Page renderers
    // ---------------------------------------------------------------------------

    function renderDashboard() {
        const s = store.status || {};
        const idx = store.index || {};
        const fl = store.freelist || {};
        const rd = store.redo || {};
        const alerts = evaluateAlerts();

        return renderAlerts(alerts) +
            '<div class="grid">' +
                '<div class="card"><h2>Records</h2><div class="stat"><div class="stat-value">' + fmt(s.records?.total) + '</div><div class="stat-label">Total records</div></div></div>' +
                '<div class="card"><h2>Index</h2><div class="stat"><div class="stat-value">' + fmt(idx.entries) + '</div><div class="stat-label">Entries (LF: ' + pct(idx.load_factor) + ')</div></div></div>' +
                '<div class="card"><h2>Cluster</h2><div class="stat"><div class="stat-value">' + (s.cluster_size || 1) + '</div><div class="stat-label">Nodes (migrations: ' + (s.active_migrations || 0) + ')</div></div></div>' +
            '</div>' +
            '<div class="grid">' +
                '<div class="card"><h2>Storage</h2>' + bar(fl.utilization || 0, fmtBytes(fl.used_bytes) + ' / ' + fmtBytes(fl.device_size) + ' (' + pct(fl.utilization) + ')') + '</div>' +
                '<div class="card"><h2>Memory</h2><div class="stat"><div class="stat-value">' + fmtBytes(idx.memory_bytes) + '</div><div class="stat-label">Index memory</div></div></div>' +
                '<div class="card"><h2>Redo Log</h2>' + (rd.available !== false ? bar(rd.utilization || 0, 'Seq: ' + fmt(rd.current_sequence) + ' (' + pct(rd.utilization) + ')') : '<div class="stat-label">Not available</div>') + '</div>' +
            '</div>' +
            '<div class="card"><h2>Throughput</h2><table>' +
                '<tr><th>Metric</th><th>Value</th></tr>' +
                '<tr><td>Spends attempted</td><td>' + fmt(s.throughput?.spends_attempted) + '</td></tr>' +
                '<tr><td>Spends succeeded</td><td>' + fmt(s.throughput?.spends_succeeded) + '</td></tr>' +
                '<tr><td>Spends failed</td><td>' + fmt(s.throughput?.spends_failed) + '</td></tr>' +
                '<tr><td>Unspends attempted</td><td>' + fmt(s.throughput?.unspends_attempted) + '</td></tr>' +
                '<tr><td>SpendMulti batches</td><td>' + fmt(s.throughput?.spend_multi_batches) + '</td></tr>' +
            '</table></div>';
    }

    function renderNodes() {
        const n = store.nodes;
        if (!n) return '<div class="card">Loading...</div>';
        return '<div class="card"><h2>Cluster Nodes</h2><table>' +
            '<tr><th>Node ID</th><th>Address</th><th>State</th><th>Master Shards</th><th>Replica Shards</th></tr>' +
            (n.nodes || []).map(node =>
                '<tr><td>' + node.node_id + '</td><td>' + node.address + '</td><td>' + node.state +
                '</td><td>' + node.master_shards + '</td><td>' + node.replica_shards + '</td></tr>'
            ).join('') + '</table></div>';
    }

    function renderStorage() {
        const fl = store.freelist || {};
        return '<div class="card"><h2>Storage</h2>' +
            bar(fl.utilization || 0, fmtBytes(fl.used_bytes) + ' / ' + fmtBytes(fl.device_size)) +
            '<table><tr><th>Metric</th><th>Value</th></tr>' +
            '<tr><td>Device size</td><td>' + fmtBytes(fl.device_size) + '</td></tr>' +
            '<tr><td>Used</td><td>' + fmtBytes(fl.used_bytes) + '</td></tr>' +
            '<tr><td>Utilization</td><td>' + pct(fl.utilization) + '</td></tr>' +
            '<tr><td>Free regions</td><td>' + fmt(fl.free_region_count) + '</td></tr>' +
            '<tr><td>Largest free</td><td>' + fmtBytes(fl.largest_free_region) + '</td></tr>' +
            '<tr><td>Total free</td><td>' + fmtBytes(fl.total_free_bytes) + '</td></tr>' +
            '<tr><td>Alignment</td><td>' + fmt(fl.alignment) + ' bytes</td></tr>' +
            '</table></div>';
    }

    function renderRecords() {
        const r = store.records || {};
        return '<div class="card"><h2>Record Inventory</h2><table>' +
            '<tr><th>Metric</th><th>Value</th></tr>' +
            '<tr><td>Total records</td><td>' + fmt(r.total_records) + '</td></tr>' +
            '<tr><td>DAH index</td><td>' + fmt(r.dah_index_count) + '</td></tr>' +
            '<tr><td>Unmined</td><td>' + fmt(r.unmined_count) + '</td></tr>' +
            '</table></div>' +
            '<div class="card"><h2>Lookup Record</h2>' +
            '<div class="search-box"><input type="text" id="txid-input" placeholder="Enter 64-char hex txid">' +
            '<button onclick="window._searchRecord()">Search</button></div>' +
            '<div id="record-result"></div></div>';
    }

    function renderReplication() {
        const r = store.replication || {};
        if (!r.enabled) return '<div class="card"><h2>Replication</h2><p>Not enabled (single-node mode)</p></div>';
        return '<div class="card"><h2>Replication</h2><table>' +
            '<tr><th>Metric</th><th>Value</th></tr>' +
            '<tr><td>Enabled</td><td>' + r.enabled + '</td></tr>' +
            '<tr><td>ACK policy</td><td>' + r.ack_policy + '</td></tr>' +
            '<tr><td>Best effort</td><td>' + r.best_effort + '</td></tr>' +
            '<tr><td>Topology term</td><td>' + r.topology_term + '</td></tr>' +
            '<tr><td>Topology epoch</td><td>' + r.topology_epoch + '</td></tr>' +
            '<tr><td>Peak cluster size</td><td>' + r.peak_cluster_size + '</td></tr>' +
            '</table></div>';
    }

    function renderMigrations() {
        const m = store.migrations || {};
        return '<div class="card"><h2>Migrations</h2>' +
            '<div class="grid"><div class="stat"><div class="stat-value">' + (m.active_count || 0) + '</div><div class="stat-label">Active</div></div>' +
            '<div class="stat"><div class="stat-value">' + (m.inbound_pending || 0) + '</div><div class="stat-label">Inbound pending</div></div>' +
            '<div class="stat"><div class="stat-value">' + (m.fenced_shards || 0) + '</div><div class="stat-label">Fenced shards</div></div></div>' +
            ((m.migrations || []).length > 0 ? '<table><tr><th>Shard</th><th>From</th><th>To</th><th>State</th><th>Progress</th><th>Bytes</th></tr>' +
                m.migrations.map(mi =>
                    '<tr><td>' + mi.shard + '</td><td>' + mi.from_node + '</td><td>' + mi.to_node + '</td><td>' + mi.state +
                    '</td><td>' + fmt(mi.migrated_records) + '/' + fmt(mi.total_records) + '</td><td>' + fmtBytes(mi.bytes_sent) + '</td></tr>'
                ).join('') + '</table>' : '<p style="color:var(--text-muted);margin-top:0.5rem">No active migrations</p>') +
            '</div>';
    }

    function renderConfig() {
        const levels = ['error', 'warn', 'info', 'debug', 'trace'];
        const current = store.logLevel || 'info';
        return '<div class="card"><h2>Log Level</h2>' +
            '<div class="log-levels">' +
            levels.map(l => '<button class="' + (l === current ? 'active' : '') + '" onclick="window._setLogLevel(\'' + l + '\')">' + l + '</button>').join('') +
            '</div></div>';
    }

    // ---------------------------------------------------------------------------
    // Live Monitor (#/top) — WebSocket-based
    // ---------------------------------------------------------------------------

    function connectWs() {
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        ws = new WebSocket(proto + '//' + location.host + '/ws/top');
        ws.onopen = function() {
            store.wsConnected = true;
            renderCurrentPage();
        };
        ws.onmessage = function(e) {
            store.prevSnapshot = store.topSnapshot;
            store.topSnapshot = JSON.parse(e.data);
            if (currentPage() === 'top') renderCurrentPage();
        };
        ws.onclose = function() {
            store.wsConnected = false;
            if (currentPage() === 'top') {
                renderCurrentPage();
                setTimeout(connectWs, 2000);
            }
        };
        ws.onerror = function() { ws.close(); };
    }

    function disconnectWs() {
        if (ws) { ws.close(); ws = null; }
    }

    // Toggle between aggregate and per-node views
    let topViewMode = 'aggregate'; // 'aggregate' or 'pernode'

    // Extract the aggregate data from a response (handles both formats)
    function getAggregate(resp) {
        if (!resp) return null;
        return resp.aggregate || resp;
    }

    function computeRatesFrom(cur, prev) {
        if (!cur || !prev) return null;
        const dt = (cur.timestamp_ms - prev.timestamp_ms) / 1000;
        if (dt <= 0) return null;
        function rate(key) { return ((cur.counters[key] || 0) - (prev.counters[key] || 0)) / dt; }
        return {
            spends: rate('spends_attempted'),
            spend_multi: rate('spend_multi_batches'),
            creates: rate('creates_attempted'),
            set_mined: rate('set_mined_attempted'),
            gets: rate('gets_attempted'),
            unspends: rate('unspends_attempted'),
        };
    }

    function renderOpsTable(snap, rates) {
        const c = snap.counters;
        const lat = snap.latency || {};
        function opRow(name, attemptedKey, successKey, latKey) {
            const rateKey = name.toLowerCase().replace(' ', '_');
            const r = rates ? Math.round(rates[rateKey] || 0) : 0;
            return '<tr><td>' + name + '</td><td>' + fmt(r) + '</td><td>' + fmt(c[attemptedKey]) +
                   '</td><td>' + fmt((c[attemptedKey]||0) - (c[successKey]||0)) +
                   '</td><td>' + fmtNs(lat[latKey]?.p50_ns) + '</td><td>' + fmtNs(lat[latKey]?.p99_ns) + '</td></tr>';
        }
        return '<table>' +
            '<tr><th>Operation</th><th>Ops/sec</th><th>Total</th><th>Errors</th><th>p50</th><th>p99</th></tr>' +
            opRow('Spends', 'spends_attempted', 'spends_succeeded', 'spend') +
            opRow('Spend Multi', 'spend_multi_batches', 'spend_multi_batches', 'spend_multi') +
            opRow('Creates', 'creates_attempted', 'creates_succeeded', 'spend') +
            opRow('Set Mined', 'set_mined_attempted', 'set_mined_succeeded', 'spend') +
            opRow('Gets', 'gets_attempted', 'gets_succeeded', 'spend') +
            opRow('Unspends', 'unspends_attempted', 'unspends_succeeded', 'unspend') +
            '</table>';
    }

    function renderPerNodeTable(nodes, prevNodes) {
        if (!nodes || nodes.length <= 1) return '<p style="color:var(--text-muted)">Single node — no per-node breakdown</p>';
        let html = '<table><tr><th>Node</th><th>Spends/s</th><th>Creates/s</th><th>Gets/s</th><th>Records</th><th>Storage</th><th>Conns</th></tr>';
        for (const node of nodes) {
            const prev = (prevNodes || []).find(function(p) { return p.node_id === node.node_id; });
            const dt = prev ? (node.timestamp_ms - prev.timestamp_ms) / 1000 : 0;
            function nr(key) {
                if (!prev || dt <= 0) return 0;
                return Math.round(((node.counters[key]||0) - (prev.counters[key]||0)) / dt);
            }
            html += '<tr><td>node ' + node.node_id + '</td><td>' + fmt(nr('spends_attempted')) +
                '</td><td>' + fmt(nr('creates_attempted')) + '</td><td>' + fmt(nr('gets_attempted')) +
                '</td><td>' + fmt(node.index?.entries) + '</td><td>' + pct(node.storage?.utilization) +
                '</td><td>' + (node.connections || 0) + '</td></tr>';
        }
        return html + '</table>';
    }

    function renderTop() {
        const resp = store.topSnapshot;
        const prevResp = store.prevSnapshot;
        const dot = '<span class="live-dot ' + (store.wsConnected ? 'connected' : 'disconnected') + '"></span>';

        if (!resp) {
            return '<div class="live-header"><div>' + dot + (store.wsConnected ? 'Connected' : 'Connecting...') + '</div></div>' +
                   '<div class="card">Waiting for data...</div>';
        }

        const agg = getAggregate(resp);
        const prevAgg = getAggregate(prevResp);
        const nodeCount = agg.node_count || 1;
        const viewLabel = topViewMode === 'aggregate'
            ? 'Cluster (' + nodeCount + ' nodes)'
            : 'Per-Node (' + nodeCount + ' nodes)';
        const toggleBtn = '<button onclick="window._toggleTopView()" style="margin-left:1rem;padding:0.25rem 0.75rem;background:var(--accent);color:#fff;border:none;border-radius:4px;cursor:pointer;font-size:0.8rem">' +
            (topViewMode === 'aggregate' ? 'Show Per-Node' : 'Show Aggregate') + '</button>';

        let header = '<div class="live-header"><div>' + dot + (store.wsConnected ? 'Live' : 'Disconnected') +
            ' &mdash; ' + viewLabel + ' &mdash; ' + (agg.connections || 0) + ' connections' + toggleBtn + '</div></div>';

        let opsHtml;
        if (topViewMode === 'aggregate') {
            const rates = computeRatesFrom(agg, prevAgg);
            opsHtml = '<div class="card"><h2>Operations (Cluster Aggregate)</h2>' + renderOpsTable(agg, rates) + '</div>';
        } else {
            const nodes = resp.nodes || [agg];
            const prevNodes = prevResp ? (prevResp.nodes || [getAggregate(prevResp)]) : [];
            opsHtml = '<div class="card"><h2>Per-Node Breakdown</h2>' + renderPerNodeTable(nodes, prevNodes) + '</div>';
        }

        return header + opsHtml +
            '<div class="grid">' +
                '<div class="card"><h2>Index</h2><div class="stat"><div class="stat-value">' + fmt(agg.index?.entries) +
                '</div><div class="stat-label">LF: ' + pct(agg.index?.load_factor) + ' &middot; Memory: ' + fmtBytes(agg.index?.memory_bytes) + '</div></div></div>' +
                '<div class="card"><h2>Storage</h2>' + bar(agg.storage?.utilization || 0,
                    fmtBytes(agg.storage?.used_bytes) + ' / ' + fmtBytes(agg.storage?.total_bytes) + ' (' + pct(agg.storage?.utilization) + ')') + '</div>' +
                '<div class="card"><h2>Redo Log</h2>' + bar(agg.redo?.utilization || 0,
                    'Seq: ' + fmt(agg.redo?.current_sequence) + ' (' + pct(agg.redo?.utilization) + ')') + '</div>' +
            '</div>';
    }

    window._toggleTopView = function() {
        topViewMode = topViewMode === 'aggregate' ? 'pernode' : 'aggregate';
        renderCurrentPage();
    };

    // ---------------------------------------------------------------------------
    // Router
    // ---------------------------------------------------------------------------

    const pages = {
        dashboard: renderDashboard,
        top: renderTop,
        nodes: renderNodes,
        storage: renderStorage,
        records: renderRecords,
        replication: renderReplication,
        migrations: renderMigrations,
        config: renderConfig,
    };

    function currentPage() {
        const hash = location.hash.replace('#/', '') || 'dashboard';
        return hash in pages ? hash : 'dashboard';
    }

    function renderCurrentPage() {
        const page = currentPage();
        document.getElementById('content').innerHTML = pages[page]();

        // Update nav
        document.querySelectorAll('.nav-link').forEach(function(link) {
            link.classList.toggle('active', link.dataset.page === page);
        });

        // WebSocket management for top page
        if (page === 'top') {
            connectWs();
        } else {
            disconnectWs();
        }
    }

    // ---------------------------------------------------------------------------
    // Global actions (called from onclick handlers)
    // ---------------------------------------------------------------------------

    window._searchRecord = async function() {
        const txid = document.getElementById('txid-input').value.trim();
        const el = document.getElementById('record-result');
        if (!txid || txid.length !== 64) {
            el.innerHTML = '<div class="alert warning">Enter a valid 64-character hex txid</div>';
            return;
        }
        const r = await fetch('/debug/records/' + txid);
        if (!r.ok) {
            el.innerHTML = '<div class="alert warning">Record not found</div>';
            return;
        }
        const data = await r.json();
        el.innerHTML = '<table>' + Object.entries(data).map(function(kv) {
            return '<tr><td>' + kv[0] + '</td><td>' + kv[1] + '</td></tr>';
        }).join('') + '</table>';
    };

    window._setLogLevel = async function(level) {
        await fetch('/debug/log-level', { method: 'PUT', body: level });
        store.logLevel = level;
        renderCurrentPage();
    };

    // ---------------------------------------------------------------------------
    // Init
    // ---------------------------------------------------------------------------

    // Theme
    const savedTheme = localStorage.getItem('teraslab-theme');
    if (savedTheme === 'light') document.body.classList.add('light');

    document.getElementById('theme-toggle').addEventListener('click', function() {
        document.body.classList.toggle('light');
        localStorage.setItem('teraslab-theme', document.body.classList.contains('light') ? 'light' : 'dark');
    });

    // Route changes
    window.addEventListener('hashchange', function() {
        renderCurrentPage();
    });

    // Initial load
    refreshAll();
    refreshTimer = setInterval(function() {
        if (currentPage() !== 'top') refreshAll();
    }, 3000);

})();
