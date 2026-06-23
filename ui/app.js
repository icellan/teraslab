// TeraSlab Admin UI — Mission Control + Flow
// Drop-in replacement for ui/app.js

(function () {
    'use strict';

    // ---------------------------------------------------------------------------
    // Data Store
    // ---------------------------------------------------------------------------
    const store = {
        status: null, index: null, freelist: null, redo: null,
        nodes: null, memory: null, records: null, replication: null,
        migrations: null, logLevel: null,
        topSnapshot: null, prevSnapshot: null, wsConnected: false,
        // rolling history buffers (N samples)
        history: {
            ops: [], p99: [], p50: [], storage: [], redo: [], repl: [], errors: [],
            repl_lag: [], redo_flush_p99: [], alloc_rate: [],
        },
        _prevObs: null,
    };
    const HISTORY_MAX = 60;
    let refreshTimer = null;
    let ws = null;

    // ---------------------------------------------------------------------------
    // Admin auth
    //
    // The live API (/admin/*, /debug/*, /ws/top) is bearer-gated by
    // require_admin_bearer in src/server/http.rs. fetch() calls send the token
    // in the Authorization header; new WebSocket() cannot set that header, so
    // /ws/top takes the token as a Sec-WebSocket-Protocol subprotocol offer
    // (see connectWs). The token is kept in localStorage so it survives
    // reloads; a 401 surfaces the login prompt for re-entry.
    // ---------------------------------------------------------------------------
    const TOKEN_KEY = 'teraslab-admin-token';
    let authFailed = false;
    let loginShown = false;
    function getToken() { return localStorage.getItem(TOKEN_KEY) || ''; }
    function setToken(t) { localStorage.setItem(TOKEN_KEY, t); }
    function authHeaders(extra) {
        const h = Object.assign({}, extra || {});
        const t = getToken();
        if (t) h['Authorization'] = 'Bearer ' + t;
        return h;
    }
    // Single choke point for every authenticated request: injects the bearer
    // header and surfaces the login prompt on 401 so a missing/rotated token
    // is recoverable without a reload.
    async function apiFetch(path, opts) {
        opts = opts || {};
        opts.headers = authHeaders(opts.headers);
        const r = await fetch(path, opts);
        if (r.status === 401) { authFailed = true; showLogin('Token rejected (401). Enter a valid admin token.'); }
        return r;
    }
    function ensureLoginOverlay() {
        let el = document.getElementById('login-overlay');
        if (el) return el;
        const style = document.createElement('style');
        style.textContent =
            '#login-overlay{position:fixed;inset:0;z-index:10000;display:flex;align-items:center;' +
            'justify-content:center;background:rgba(5,7,10,0.72);backdrop-filter:blur(3px);' +
            'font-family:var(--ts-sans,sans-serif)}' +
            '#login-overlay[hidden]{display:none}' +
            '#login-overlay .login-card{width:min(92vw,380px);background:var(--ts-bg-1,#0f1217);' +
            'border:1px solid var(--ts-line,#232a36);border-radius:10px;padding:24px;' +
            'box-shadow:0 16px 48px rgba(0,0,0,0.5)}' +
            '#login-overlay .login-title{font-size:18px;font-weight:600;color:var(--ts-text,#e6ecf3);margin-bottom:6px}' +
            '#login-overlay .login-sub{font-size:13px;color:var(--ts-text-2,#a8b2c0);margin-bottom:16px;line-height:1.4}' +
            '#login-overlay input{width:100%;box-sizing:border-box;padding:10px 12px;font-size:13px;' +
            'font-family:var(--ts-mono,monospace);color:var(--ts-text,#e6ecf3);background:var(--ts-bg,#0a0c10);' +
            'border:1px solid var(--ts-line-2,#2e3644);border-radius:6px;outline:none}' +
            '#login-overlay input:focus{border-color:var(--ts-accent,#c8ff5e)}' +
            '#login-overlay button{width:100%;margin-top:12px;padding:10px 12px;font-size:13px;font-weight:600;' +
            'cursor:pointer;color:#0a0c10;background:var(--ts-accent,#c8ff5e);border:0;border-radius:6px}' +
            '#login-overlay .login-hint{margin-top:14px;font-size:11px;color:var(--ts-text-3,#6b7686);line-height:1.4}';
        document.head.appendChild(style);
        el = document.createElement('div');
        el.id = 'login-overlay';
        el.hidden = true;
        el.innerHTML =
            '<div class="login-card">' +
            '<div class="login-title">TeraSlab Admin</div>' +
            '<div class="login-sub" id="login-msg">Enter the admin token to view live metrics.</div>' +
            '<input id="login-token" type="password" placeholder="admin token" autocomplete="off" spellcheck="false" />' +
            '<button id="login-submit">Connect</button>' +
            '<div class="login-hint">Stored in this browser (localStorage) and sent as a bearer credential ' +
            'to the admin API and the live metrics WebSocket.</div>' +
            '</div>';
        document.body.appendChild(el);
        const submit = () => {
            const v = el.querySelector('#login-token').value.trim();
            if (!v) return;
            setToken(v);
            authFailed = false;
            hideLogin();
            if (ws) { try { ws.close(); } catch (e) { /* already closing */ } ws = null; }
            connectWs();
            refreshAll();
        };
        el.querySelector('#login-submit').addEventListener('click', submit);
        el.querySelector('#login-token').addEventListener('keydown', e => { if (e.key === 'Enter') submit(); });
        return el;
    }
    function showLogin(msg) {
        const el = ensureLoginOverlay();
        if (msg) el.querySelector('#login-msg').textContent = msg;
        const input = el.querySelector('#login-token');
        // Pre-fill the stored (rejected) token once so a typo is editable;
        // don't clobber what the operator is actively typing on repeat calls.
        if (!loginShown) input.value = getToken();
        el.hidden = false;
        loginShown = true;
        input.focus();
    }
    function hideLogin() {
        const el = document.getElementById('login-overlay');
        if (el) el.hidden = true;
        loginShown = false;
    }

    // ---------------------------------------------------------------------------
    // Formatting
    // ---------------------------------------------------------------------------
    function fmt(n) {
        if (n == null) return '-';
        if (typeof n !== 'number') return String(n);
        if (n >= 1e12) return (n / 1e12).toFixed(2) + 'T';
        if (n >= 1e9)  return (n / 1e9).toFixed(2) + 'B';
        if (n >= 1e6)  return (n / 1e6).toFixed(2) + 'M';
        if (n >= 1e3)  return (n / 1e3).toFixed(1) + 'K';
        return n.toLocaleString();
    }
    function fmtBytes(n) {
        if (n == null) return '-';
        if (n >= 1e12) return (n / 1e12).toFixed(2) + ' TB';
        if (n >= 1e9)  return (n / 1e9).toFixed(2) + ' GB';
        if (n >= 1e6)  return (n / 1e6).toFixed(1) + ' MB';
        if (n >= 1e3)  return (n / 1e3).toFixed(1) + ' KB';
        return n + ' B';
    }
    function fmtNs(ns) {
        if (ns == null || ns === 0) return '-';
        if (ns >= 1e9) return (ns / 1e9).toFixed(1) + 's';
        if (ns >= 1e6) return (ns / 1e6).toFixed(1) + 'ms';
        if (ns >= 1e3) return (ns / 1e3).toFixed(0) + 'µs';
        return ns + 'ns';
    }
    function pct(v) { return v == null ? '-' : (v * 100).toFixed(1) + '%'; }
    function barClass(v) { return v > 0.95 ? 'bad' : v > 0.85 ? 'warn' : ''; }

    // ---------------------------------------------------------------------------
    // Sparklines (from concept1)
    // ---------------------------------------------------------------------------
    function sparkline(values, { w = 220, h = 28, color = 'var(--ts-ok)', fill = true, stroke = 1.4 } = {}) {
        if (!values || values.length < 2) {
            return `<svg viewBox="0 0 ${w} ${h}" width="${w}" height="${h}"><line x1="0" y1="${h / 2}" x2="${w}" y2="${h / 2}" stroke="var(--ts-line-2)" stroke-dasharray="2 3"/></svg>`;
        }
        const min = Math.min(...values), max = Math.max(...values);
        const span = max - min || 1;
        const pts = values.map((v, i) => [(i / (values.length - 1)) * w, h - 2 - ((v - min) / span) * (h - 4)]);
        const d = pts.map((p, i) => (i === 0 ? 'M' : 'L') + p[0].toFixed(1) + ',' + p[1].toFixed(1)).join(' ');
        const area = fill ? `<path d="${d} L ${w},${h} L 0,${h} Z" fill="${color}" opacity="0.12"/>` : '';
        return `<svg viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" style="display:block">${area}<path d="${d}" stroke="${color}" stroke-width="${stroke}" fill="none" stroke-linejoin="round" stroke-linecap="round"/></svg>`;
    }

    function pushHistory() {
        const ts = store.topSnapshot;
        if (!ts) return;
        const agg = ts.aggregate || ts;
        // Observability sub-shapes (replication_metrics / redo_metrics / etc.)
        // are produced by build_local_top_snapshot (see src/server/http.rs) but
        // NOT propagated by aggregate_snapshots. When the WS envelope wraps a
        // cluster snapshot, those live on nodes[0]. Fall back gracefully.
        const localNode = ts.nodes && ts.nodes.length ? ts.nodes[0] : agg;
        const c = agg.counters || {};
        const lat = agg.latency || {};
        const prev = store._prevForRate;
        let opsRate = 0;
        if (prev && agg.timestamp_ms > prev.ts) {
            const dt = (agg.timestamp_ms - prev.ts) / 1000;
            opsRate = ((c.spends_attempted || 0) - prev.spends) / dt;
        }
        store._prevForRate = { ts: agg.timestamp_ms, spends: c.spends_attempted || 0 };

        push(store.history.ops, opsRate);
        push(store.history.p99, (lat.spend?.p99_ns || 0) / 1000);
        push(store.history.p50, (lat.spend?.p50_ns || 0) / 1000);
        push(store.history.storage, (agg.storage?.utilization || 0) * 100);
        push(store.history.redo, (agg.redo?.utilization || 0) * 100);

        // Replication lag: max (leader_seq - last_acked_seq) across replicas
        const rm = localNode.replication_metrics || {};
        const leaderSeq = rm.leader_sequence || 0;
        let maxLag = 0;
        for (const pr of rm.per_replica || []) {
            const lag = pr.lag != null ? pr.lag : Math.max(0, leaderSeq - (pr.last_acked_seq || 0));
            if (lag > maxLag) maxLag = lag;
        }
        push(store.history.repl_lag, maxLag);
        // Keep the old .repl buffer populated for KPI continuity — using the
        // replication batch latency p99 in ms if available, else 0.
        const replLatP99Ns = rm.latency?.p99_ns || 0;
        push(store.history.repl, replLatP99Ns / 1e6);

        // Redo flush p99 in µs
        const redoFlushP99 = (localNode.redo_metrics?.flush_latency?.p99_ns || 0) / 1000;
        push(store.history.redo_flush_p99, redoFlushP99);

        // Allocator alloc-rate (alloc_total deltas per second)
        const am = localNode.allocator_metrics || {};
        const nowTs = agg.timestamp_ms || 0;
        let allocRate = 0;
        if (store._prevObs && nowTs > store._prevObs.ts) {
            const dt = (nowTs - store._prevObs.ts) / 1000;
            if (dt > 0) {
                allocRate = Math.max(0, ((am.alloc_total || 0) - store._prevObs.alloc_total) / dt);
            }
        }
        store._prevObs = { ts: nowTs, alloc_total: am.alloc_total || 0 };
        push(store.history.alloc_rate, allocRate);

        const errRate = ((c.spends_attempted || 0) - (c.spends_succeeded || 0));
        push(store.history.errors, errRate);
    }
    function push(buf, v) { buf.push(v); if (buf.length > HISTORY_MAX) buf.shift(); }
    function escapeHtml(value) {
        return String(value ?? '').replace(/[&<>"']/g, ch => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#39;',
        }[ch]));
    }
    function displayValue(value) {
        if (value === null || value === undefined) return '';
        if (typeof value === 'object') return JSON.stringify(value);
        return value;
    }

    // ---------------------------------------------------------------------------
    // API
    // ---------------------------------------------------------------------------
    async function fetchJson(path) {
        try { const r = await apiFetch(path); if (!r.ok) return null; return await r.json(); } catch { return null; }
    }
    async function refreshAll() {
        const [status, index, freelist, redo, nodes, memory, records, replication, migrations, logLevel] =
            await Promise.all([
                fetchJson('/status'), fetchJson('/debug/index'), fetchJson('/debug/freelist'),
                fetchJson('/debug/redo'), fetchJson('/admin/nodes'), fetchJson('/admin/memory'),
                fetchJson('/admin/records'), fetchJson('/admin/replication'),
                fetchJson('/admin/migration_status'),
                apiFetch('/debug/log-level').then(r => r.ok ? r.text() : null).catch(() => null),
            ]);
        Object.assign(store, { status, index, freelist, redo, nodes, memory, records, replication, migrations, logLevel });
        updateClusterPill();
        renderCurrentPage();
    }

    function updateClusterPill() {
        const el = document.getElementById('cluster-pill-text');
        const pill = document.getElementById('cluster-pill');
        if (!el || !pill) return;
        const s = store.status;
        if (!s) { el.textContent = 'connecting…'; pill.classList.add('bad'); return; }
        pill.classList.remove('bad');
        const size = s.cluster_size || 1;
        const rf = store.replication?.enabled ? store.replication.replication_factor || 2 : 1;
        el.textContent = `${size} node${size > 1 ? 's' : ''} · RF ${rf} · quorum OK`;
    }

    // ---------------------------------------------------------------------------
    // Alerts
    // ---------------------------------------------------------------------------
    function evaluateAlerts() {
        const alerts = [];
        if (store.freelist) {
            if (store.freelist.utilization > 0.95) alerts.push({ severity: 'critical', message: 'Device utilization > 95%' });
            else if (store.freelist.utilization > 0.85) alerts.push({ severity: 'warning', message: 'Device utilization > 85%' });
        }
        if (store.index && store.index.load_factor > 0.85) alerts.push({ severity: 'warning', message: 'Index load factor > 85%' });
        if (store.redo?.available && store.redo.utilization > 0.80) alerts.push({ severity: 'warning', message: 'Redo log utilization > 80%' });
        const mig = store.migrations;
        if (mig && (mig.active_count || 0) > 0) alerts.push({ severity: 'info', message: `${mig.active_count} migration${mig.active_count > 1 ? 's' : ''} in progress` });
        (store.nodes?.nodes || []).forEach(n => {
            if (n.state && n.state !== 'alive') alerts.push({ severity: 'warning', message: `node-${n.node_id} ${n.state}` });
        });
        return alerts;
    }
    function renderAlerts(alerts) {
        if (!alerts.length) return '';
        return '<div class="alerts">' + alerts.map(a =>
            `<div class="alert ${escapeHtml(a.severity)}"><span class="ts-kicker">${escapeHtml(a.severity)}</span><span>${escapeHtml(a.message)}</span></div>`
        ).join('') + '</div>';
    }

    // ---------------------------------------------------------------------------
    // Page: Mission Control (Dashboard)
    // ---------------------------------------------------------------------------
    function kpiCard({ label, value, unit, delta, deltaKind = 'ok', series, color = 'var(--ts-ok)' }) {
        const deltaColor = `color:var(--ts-${deltaKind})`;
        return `<div class="ts-panel is-interactive ts-kpi">
            <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                <div class="ts-kpi__label">${label}</div>
                <span class="ts-kpi__delta" style="${deltaColor}">${delta || ''}</span>
            </div>
            <div><span class="ts-kpi__value">${value}</span><span class="ts-kpi__unit">${unit || ''}</span></div>
            <div class="ts-kpi__spark">${sparkline(series, { w: 240, h: 28, color })}</div>
        </div>`;
    }

    function renderDashboard() {
        const s = store.status || {};
        const idx = store.index || {};
        const fl = store.freelist || {};
        const rd = store.redo || {};
        const ts = store.topSnapshot?.aggregate || store.topSnapshot || {};
        const lat = ts.latency || {};
        const c = ts.counters || {};
        const alerts = evaluateAlerts();

        const opsNow = store.history.ops.length ? store.history.ops[store.history.ops.length - 1] : 0;
        const p99Now = store.history.p99.length ? store.history.p99[store.history.p99.length - 1] : 0;

        // KPI strip
        const kpis = `<div class="grid" style="grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:10px;">
            ${kpiCard({ label: 'OPS / SEC', value: fmt(opsNow), unit: '', delta: '', series: store.history.ops, color: 'var(--ts-accent)' })}
            ${kpiCard({ label: 'P99 LATENCY', value: p99Now ? p99Now.toFixed(0) : '-', unit: 'µs', series: store.history.p99, color: 'var(--ts-info)' })}
            ${kpiCard({ label: 'RECORDS', value: fmt(s.records?.total), unit: '', series: [], color: 'var(--ts-accent)' })}
            ${kpiCard({ label: 'STORAGE', value: ((fl.utilization || 0) * 100).toFixed(1), unit: '%', series: store.history.storage, color: 'var(--ts-warn)' })}
            ${kpiCard({ label: 'REDO PRESSURE', value: ((rd.utilization || 0) * 100).toFixed(1), unit: '%', series: store.history.redo, color: 'var(--ts-ok)' })}
            ${kpiCard({ label: 'REPL LAG', value: store.replication?.enabled ? (store.history.repl.at(-1) || 0).toFixed(0) : '-', unit: 'ms', series: store.history.repl, color: 'var(--ts-warn)' })}
        </div>`;

        // Main row
        const mainRow = `<div class="grid" style="grid-template-columns:2.1fr 1fr 1fr;gap:10px;margin-bottom:10px;">
            <div class="ts-panel">
                <div class="ts-panel__head">
                    <div class="ts-panel__title"><span class="ts-dot"></span>Throughput &amp; p99 · cluster aggregate</div>
                    <div class="ts-panel__meta"><span style="color:var(--ts-accent)">●</span> ops/s · <span style="color:var(--ts-info)">- -</span> p99 µs</div>
                </div>
                <div class="ts-panel__body">
                    ${renderDualChart(store.history.ops, store.history.p99)}
                    <div class="grid" style="grid-template-columns:repeat(4,1fr);gap:10px;margin-top:10px;padding-top:12px;border-top:1px solid var(--ts-line);">
                        <div><div class="ts-label">CURRENT</div><div class="ts-num" style="font-size:20px;color:var(--ts-accent)">${fmt(opsNow)}/s</div></div>
                        <div><div class="ts-label">PEAK 60s</div><div class="ts-num" style="font-size:20px">${fmt(Math.max(0, ...store.history.ops))}/s</div></div>
                        <div><div class="ts-label">AVG 60s</div><div class="ts-num" style="font-size:20px">${fmt(avg(store.history.ops))}/s</div></div>
                        <div><div class="ts-label">ERRORS 60s</div><div class="ts-num ts-warn" style="font-size:20px">${fmt((c.spends_attempted || 0) - (c.spends_succeeded || 0))}</div></div>
                    </div>
                </div>
            </div>

            <div class="ts-panel">
                <div class="ts-panel__head">
                    <div class="ts-panel__title"><span class="ts-dot"></span>Latency distribution · spend</div>
                    <div class="ts-panel__meta">p50/p95/p99/p999</div>
                </div>
                <div class="ts-panel__body">
                    ${renderLatencyBars(lat.spend || {})}
                </div>
            </div>

            <div class="ts-panel">
                <div class="ts-panel__head">
                    <div class="ts-panel__title"><span class="ts-dot" style="background:var(--ts-warn)"></span>Active alerts</div>
                    <div class="ts-panel__meta">${alerts.length} firing</div>
                </div>
                <div class="ts-panel__body" style="padding:0;">
                    ${alerts.length ? alerts.map(a => `
                        <div style="border-left:3px solid var(--ts-${a.severity === 'critical' ? 'bad' : a.severity === 'warning' ? 'warn' : 'info'});padding:10px 14px;border-bottom:1px solid var(--ts-line);">
                            <div class="ts-kicker">${a.severity}</div>
                            <div style="font-size:12px;margin-top:3px;color:var(--ts-text);">${a.message}</div>
                        </div>`).join('')
                        : '<div style="padding:18px;color:var(--ts-text-3);font-family:var(--ts-mono);font-size:12px;">No active alerts</div>'}
                </div>
            </div>
        </div>`;

        // Ops table
        const opsRows = [
            ['Spend',      'spends_attempted',      'spends_succeeded',      'spend'],
            ['SpendMulti', 'spend_multi_batches',   'spend_multi_batches',   'spend_multi'],
            ['Create',     'creates_attempted',     'creates_succeeded',     'spend'],
            ['SetMined',   'set_mined_attempted',   'set_mined_succeeded',   'spend'],
            ['Get',        'gets_attempted',        'gets_succeeded',        'spend'],
            ['Unspend',    'unspends_attempted',    'unspends_succeeded',    'unspend'],
        ];
        const opsTable = `<table>
            <thead><tr><th>OP</th><th style="text-align:right">COUNT</th><th style="text-align:right">SUCCESS</th><th style="text-align:right">P50</th><th style="text-align:right">P99</th></tr></thead>
            <tbody>${opsRows.map(r => {
                const attempted = c[r[1]] || 0;
                const succeeded = c[r[2]] || 0;
                const sr = attempted ? (succeeded / attempted * 100).toFixed(2) : '-';
                const l = lat[r[3]] || {};
                return `<tr>
                    <td style="color:var(--ts-text)">${r[0]}</td>
                    <td style="text-align:right">${fmt(attempted)}</td>
                    <td style="text-align:right" class="${parseFloat(sr) < 99 ? 'ts-warn' : 'ts-ok'}">${sr}%</td>
                    <td style="text-align:right">${fmtNs(l.p50_ns)}</td>
                    <td style="text-align:right">${fmtNs(l.p99_ns)}</td>
                </tr>`;
            }).join('')}</tbody>
        </table>`;

        // Nodes grid
        const nodeCards = (store.nodes?.nodes || []).map(n => {
            const state = n.state === 'alive' ? 'ok' : n.state === 'dead' ? 'bad' : 'warn';
            return `<div class="ts-panel is-interactive" style="padding:12px 14px;margin:0;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                    <div style="display:flex;gap:8px;align-items:center;">
                        <span class="ts-sdot ${state === 'ok' ? '' : state}"></span>
                        <span class="ts-num" style="font-size:12px;font-weight:500;color:var(--ts-text)">node-${String(n.node_id).padStart(2, '0')}</span>
                        <span class="ts-label">${n.address || ''}</span>
                    </div>
                    <span class="ts-num ts-${state}" style="font-size:9px;letter-spacing:.1em">${(n.state || '').toUpperCase()}</span>
                </div>
                <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:8px;">
                    <div><div class="ts-label">MASTER</div><div class="ts-num" style="font-size:14px;color:var(--ts-text)">${n.master_shards || 0}</div></div>
                    <div><div class="ts-label">REPLICA</div><div class="ts-num" style="font-size:14px;color:var(--ts-text)">${n.replica_shards || 0}</div></div>
                </div>
            </div>`;
        }).join('') || '<div style="padding:18px;color:var(--ts-text-3);font-family:var(--ts-mono);font-size:12px;">Single-node mode</div>';

        // Lower row
        const lowerRow = `<div class="grid" style="grid-template-columns:1.4fr 1fr 1fr;gap:10px;">
            <div class="ts-panel">
                <div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Operations · recent</div><div class="ts-panel__meta">6 opcodes</div></div>
                <div class="ts-panel__body" style="padding:0;">${opsTable}</div>
            </div>
            <div class="ts-panel">
                <div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Nodes · ${(store.nodes?.nodes || []).length || 1}</div><div class="ts-panel__meta">${store.replication?.topology_term != null ? 'term ' + store.replication.topology_term : ''}</div></div>
                <div class="ts-panel__body" style="display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:10px;">${nodeCards}</div>
            </div>
            <div class="ts-panel">
                <div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Capacity</div><div class="ts-panel__meta">${fmtBytes(fl.used_bytes)} / ${fmtBytes(fl.device_size)}</div></div>
                <div class="ts-panel__body">
                    ${capacityRow('STORAGE', fl.utilization || 0, fmtBytes(fl.used_bytes) + ' / ' + fmtBytes(fl.device_size))}
                    ${capacityRow('INDEX LF', idx.load_factor || 0, fmt(idx.entries) + ' entries · ' + fmtBytes(idx.memory_bytes))}
                    ${capacityRow('REDO', rd.utilization || 0, 'seq ' + fmt(rd.current_sequence))}
                </div>
            </div>
        </div>`;

        return renderAlerts(alerts.filter(a => a.severity === 'critical')) + kpis + mainRow + lowerRow;
    }
    function avg(a) { return a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0; }

    function capacityRow(label, v, detail) {
        return `<div style="margin-bottom:12px;">
            <div style="display:flex;justify-content:space-between;align-items:baseline;">
                <span class="ts-label">${label}</span>
                <span class="ts-num" style="font-size:13px;color:var(--ts-text)">${(v * 100).toFixed(1)}%</span>
            </div>
            <div class="ts-bar"><div class="ts-bar__fill ${barClass(v)}" style="width:${Math.min(100, v * 100)}%"></div></div>
            <div class="ts-label" style="font-size:10px;margin-top:4px;">${detail}</div>
        </div>`;
    }

    function renderDualChart(ops, p99) {
        const w = 820, h = 220;
        const parts = [];
        for (let i = 1; i < 5; i++) parts.push(`<line x1="0" y1="${(i / 5) * h}" x2="${w}" y2="${(i / 5) * h}" stroke="var(--ts-line)" stroke-width=".5"/>`);
        if (ops && ops.length >= 2) {
            const min = Math.min(...ops), max = Math.max(...ops), span = max - min || 1;
            const d = ops.map((v, i) => (i === 0 ? 'M' : 'L') + ((i / (ops.length - 1)) * w).toFixed(1) + ',' + (h - 20 - ((v - min) / span) * (h - 40)).toFixed(1)).join(' ');
            parts.push(`<path d="${d} L ${w},${h} L 0,${h} Z" fill="var(--ts-accent)" opacity=".08"/>`);
            parts.push(`<path d="${d}" stroke="var(--ts-accent)" stroke-width="1.8" fill="none"/>`);
        }
        if (p99 && p99.length >= 2) {
            const min = Math.min(...p99), max = Math.max(...p99), span = max - min || 1;
            const d = p99.map((v, i) => (i === 0 ? 'M' : 'L') + ((i / (p99.length - 1)) * w).toFixed(1) + ',' + (h - 20 - ((v - min) / span) * (h - 40) * 0.6).toFixed(1)).join(' ');
            parts.push(`<path d="${d}" stroke="var(--ts-info)" stroke-width="1.4" fill="none" stroke-dasharray="3 2"/>`);
        }
        ['60s', '45s', '30s', '15s', 'now'].forEach((t, i) =>
            parts.push(`<text x="${(i / 4) * w}" y="${h + 14}" fill="var(--ts-text-3)" font-size="10" font-family="JetBrains Mono" text-anchor="${i === 0 ? 'start' : i === 4 ? 'end' : 'middle'}">${t}</text>`)
        );
        return `<svg viewBox="0 0 ${w} ${h + 22}" preserveAspectRatio="none" style="width:100%;height:${h + 22}px">${parts.join('')}</svg>`;
    }

    function renderLatencyBars(lat) {
        const rows = [
            ['P50', lat.p50_ns, 'var(--ts-ok)'],
            ['P95', lat.p95_ns, 'var(--ts-ok)'],
            ['P99', lat.p99_ns, 'var(--ts-info)'],
            ['P99.9', lat.p999_ns, 'var(--ts-warn)'],
        ];
        const max = Math.max(1, ...rows.map(r => r[1] || 0));
        return rows.map(r => `<div style="display:grid;grid-template-columns:60px 1fr 80px;gap:10px;align-items:center;padding:6px 0;font-family:var(--ts-mono);font-size:11px;">
            <span class="ts-label">${r[0]}</span>
            <div class="ts-bar"><div class="ts-bar__fill" style="width:${(r[1] || 0) / max * 100}%;background:${r[2]}"></div></div>
            <span style="text-align:right;color:var(--ts-text)">${fmtNs(r[1])}</span>
        </div>`).join('');
    }

    // ---------------------------------------------------------------------------
    // Page: Flow (Concept 3)
    // ---------------------------------------------------------------------------
    function renderFlow() {
        const ts = store.topSnapshot?.aggregate || store.topSnapshot || {};
        const c = ts.counters || {};
        const lat = ts.latency || {};
        const fl = store.freelist || {};
        const rd = store.redo || {};
        const conns = ts.connections || 0;
        const opsNow = store.history.ops.at(-1) || 0;

        const heroStrip = `<div class="grid" style="grid-template-columns:1.6fr repeat(4,1fr);gap:10px;margin-bottom:10px;">
            <div class="ts-panel" style="padding:18px 22px;display:flex;align-items:center;justify-content:space-between;">
                <div>
                    <div class="ts-kicker">CLUSTER THROUGHPUT · CURRENT</div>
                    <div style="display:flex;align-items:baseline;gap:10px;margin-top:4px;">
                        <span class="ts-num" style="font-size:52px;font-weight:500;letter-spacing:-.03em;color:var(--ts-accent)">${fmt(opsNow)}</span>
                        <span class="ts-label">ops/sec aggregate</span>
                    </div>
                </div>
                <div style="text-align:right;min-width:300px;">
                    ${sparkline(store.history.ops, { w: 280, h: 56, color: 'var(--ts-accent)', fill: true })}
                    <div class="ts-label" style="margin-top:4px;">60s · peak ${fmt(Math.max(0, ...store.history.ops))} · avg ${fmt(avg(store.history.ops))}</div>
                </div>
            </div>
            ${heroStat('ACTIVE CONNECTIONS', conns, '')}
            ${heroStat('ERRORS / MIN', (c.spends_attempted || 0) - (c.spends_succeeded || 0), 'UTXO mismatch + conflict', 'var(--ts-warn)')}
            ${heroStat('REPL ACK P99', store.replication?.ack_policy || '-', store.replication?.enabled ? 'write_majority' : 'single-node')}
            ${heroStat('QUORUM', store.replication?.enabled ? 'OK' : 'N/A', `rf ${store.replication?.replication_factor || 1}`, 'var(--ts-ok)')}
        </div>`;

        const flowPanel = `<div class="grid" style="grid-template-columns:1fr 340px;gap:10px;margin-bottom:10px;">
            <div class="ts-panel">
                <div class="ts-panel__head">
                    <div class="ts-panel__title"><span class="ts-dot"></span>Data flow · clients → wire → dispatch → shards → storage</div>
                    <div class="ts-panel__meta">edge width ∝ ops/s · live</div>
                </div>
                <div class="ts-panel__body flow-wrap" style="padding:12px;">${renderFlowSVG(c, ts, fl, rd)}</div>
            </div>
            ${renderPipelineChecks(c, lat, fl, rd, conns)}
        </div>`;

        return heroStrip + flowPanel;
    }
    function heroStat(l, v, sub, color) {
        return `<div class="ts-panel" style="padding:18px 22px;">
            <div class="ts-kicker">${l}</div>
            <div class="ts-num" style="font-size:30px;font-weight:500;letter-spacing:-.02em;margin-top:4px;color:${color || 'var(--ts-text)'}">${typeof v === 'number' ? fmt(v) : v}</div>
            <div class="ts-label" style="margin-top:2px;">${sub || ''}</div>
        </div>`;
    }

    function renderFlowSVG(c, ts, fl, rd) {
        const W = 1180, H = 540;
        const X = [80, 320, 580, 840, 1100];
        const opcodes = [
            { label: 'Spend',      y: 100, rate: rate(c, 'spends_attempted'),      color: '#c8ff5e' },
            { label: 'Create',     y: 180, rate: rate(c, 'creates_attempted'),     color: '#7cf0b3' },
            { label: 'Get',        y: 260, rate: rate(c, 'gets_attempted'),        color: '#8ab8ff' },
            { label: 'SetMined',   y: 340, rate: rate(c, 'set_mined_attempted'),   color: '#7cf0b3' },
            { label: 'SpendMulti', y: 420, rate: rate(c, 'spend_multi_batches'),   color: '#c8ff5e' },
            { label: 'Unspend',    y: 480, rate: rate(c, 'unspends_attempted'),    color: '#f2c45a' },
        ];
        const maxRate = Math.max(1, ...opcodes.map(o => o.rate));

        const nodes = store.nodes?.nodes || [{ node_id: 0, address: 'local', master_shards: 4096, replica_shards: 0, state: 'alive' }];
        const nodeColors = ['#c8ff5e', '#8ab8ff', '#7cf0b3', '#f2c45a', '#ff9a6b', '#b494ff'];
        const bands = nodes.slice(0, 6).map((n, i) => ({
            label: `node-${String(n.node_id).padStart(2, '0')}`,
            y: 110 + i * 75, h: 60, color: nodeColors[i % nodeColors.length],
            ops: (n.master_shards || 0) + ' shards',
        }));

        const tiers = [
            { label: 'HOT · NVMe',  y: 150, ops: fmtBytes(fl.used_bytes || 0),              color: '#c8ff5e', detail: 'metadata + UTXO slots' },
            { label: 'COLD · blob', y: 290, ops: fmtBytes(ts.blobstore_bytes || 0),          color: '#8ab8ff', detail: 'tx bodies > 8 KiB' },
            { label: 'REDO log',    y: 420, ops: ((rd.utilization || 0) * 100).toFixed(1) + '%', color: '#7cf0b3', detail: 'seq ' + fmt(rd.current_sequence) },
        ];

        const parts = [];
        parts.push(flowPipe(X[0] + 50, H / 2, X[1] - 50, H / 2, 44, 'var(--ts-accent)'));
        parts.push(flowPipe(X[1] + 50, H / 2, X[2] - 60, H / 2, 44, 'var(--ts-accent)'));

        opcodes.forEach(o => {
            const t = Math.max(2, (o.rate / maxRate) * 40);
            parts.push(flowCurve(X[2] + 60, H / 2, X[3] - 180, o.y, t, o.color, .7));
        });
        opcodes.forEach(o => {
            bands.forEach(n => {
                const t = Math.max(0.6, (o.rate / maxRate) * 8);
                parts.push(flowCurve(X[3] - 120, o.y, X[3] + 30, n.y + n.h / 2, t, o.color, .3));
            });
        });
        bands.forEach(n => {
            tiers.forEach((t, ti) => {
                const w = ti === 0 ? 10 : ti === 1 ? 2 : 4;
                parts.push(flowCurve(X[3] + 100, n.y + n.h / 2, X[4] - 60, t.y, w, t.color, .4));
            });
        });

        parts.push(stageBox(X[0] - 40, 220, 90, 100, 'CLIENTS', fmt(ts.connections || 0), 'conn', 'var(--ts-accent)'));
        parts.push(stageBox(X[1] - 50, 220, 100, 100, 'WIRE', fmt(rate(c, 'spends_attempted') + rate(c, 'creates_attempted') + rate(c, 'gets_attempted')), 'ops/s', 'var(--ts-accent)'));
        parts.push(stageBox(X[2] - 60, 220, 120, 100, 'DISPATCHER', String(opcodes.length), 'opcodes', '#8ab8ff'));

        opcodes.forEach(o => {
            parts.push(`<rect x="${X[3] - 170}" y="${o.y - 16}" width="120" height="32" rx="3" fill="var(--ts-bg-1)" stroke="${o.color}" stroke-width=".8" opacity=".9"/>`);
            parts.push(`<text x="${X[3] - 160}" y="${o.y - 2}" fill="${o.color}" font-family="JetBrains Mono" font-size="11" font-weight="500">${o.label}</text>`);
            parts.push(`<text x="${X[3] - 160}" y="${o.y + 11}" fill="var(--ts-text-3)" font-family="JetBrains Mono" font-size="9">${fmt(o.rate)}/s</text>`);
        });

        bands.forEach(n => {
            parts.push(`<rect x="${X[3] + 30}" y="${n.y}" width="130" height="${n.h}" rx="4" fill="var(--ts-bg-1)" stroke="${n.color}" stroke-width="1"/>`);
            parts.push(`<text x="${X[3] + 40}" y="${n.y + 18}" fill="${n.color}" font-family="JetBrains Mono" font-size="12" font-weight="500">${n.label}</text>`);
            parts.push(`<text x="${X[3] + 40}" y="${n.y + 34}" fill="var(--ts-text-2)" font-family="JetBrains Mono" font-size="10">${n.ops}</text>`);
        });

        tiers.forEach(t => {
            parts.push(`<rect x="${X[4] - 60}" y="${t.y - 30}" width="170" height="60" rx="4" fill="var(--ts-bg-1)" stroke="${t.color}" stroke-width="1"/>`);
            parts.push(`<text x="${X[4] - 50}" y="${t.y - 10}" fill="${t.color}" font-family="JetBrains Mono" font-size="12" font-weight="500">${t.label}</text>`);
            parts.push(`<text x="${X[4] - 50}" y="${t.y + 6}" fill="var(--ts-text)" font-family="JetBrains Mono" font-size="14">${t.ops}</text>`);
            parts.push(`<text x="${X[4] - 50}" y="${t.y + 20}" fill="var(--ts-text-3)" font-family="JetBrains Mono" font-size="9">${t.detail}</text>`);
        });

        ['INGRESS', 'PROTOCOL', 'DISPATCH', 'SHARDS', 'STORAGE'].forEach((label, i) =>
            parts.push(`<text x="${i < 4 ? X[i] : X[4] + 25}" y="40" fill="var(--ts-text-3)" font-family="JetBrains Mono" font-size="10" letter-spacing="3" text-anchor="middle">${label}</text>`)
        );

        return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet" style="width:100%;height:540px">${parts.join('')}</svg>`;
    }
    function rate(c, key) {
        // Rolling rate from last two topSnapshots — rough, since /top is 1s cadence
        const prev = store.prevSnapshot?.aggregate || store.prevSnapshot;
        if (!prev || !prev.counters || !prev.timestamp_ms) return 0;
        const dt = ((store.topSnapshot.aggregate?.timestamp_ms || store.topSnapshot.timestamp_ms) - prev.timestamp_ms) / 1000;
        if (dt <= 0) return 0;
        return Math.max(0, ((c[key] || 0) - (prev.counters[key] || 0)) / dt);
    }
    function flowPipe(x1, y1, x2, y2, t, color) {
        return `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="${color}" stroke-width="${t}" opacity=".25" stroke-linecap="round"/>
            <line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="${color}" stroke-width="${t * .55}" opacity=".9" stroke-linecap="round" stroke-dasharray="6 8">
                <animate attributeName="stroke-dashoffset" from="14" to="0" dur=".8s" repeatCount="indefinite"/></line>`;
    }
    function flowCurve(x1, y1, x2, y2, t, color, o = .5) {
        const mx = (x1 + x2) / 2;
        const d = `M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`;
        return `<path d="${d}" stroke="${color}" stroke-width="${t}" fill="none" opacity="${o * .4}"/>
            <path d="${d}" stroke="${color}" stroke-width="${t * .6}" fill="none" opacity="${o}" stroke-dasharray="4 6">
                <animate attributeName="stroke-dashoffset" from="10" to="0" dur="1.2s" repeatCount="indefinite"/></path>`;
    }
    function stageBox(x, y, w, h, label, value, unit, color) {
        return `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="4" fill="var(--ts-bg-1)" stroke="${color}" stroke-width="1.2"/>
            <text x="${x + w / 2}" y="${y + 22}" fill="${color}" font-family="JetBrains Mono" font-size="10" text-anchor="middle" letter-spacing="2">${label}</text>
            <line x1="${x + 12}" y1="${y + 30}" x2="${x + w - 12}" y2="${y + 30}" stroke="var(--ts-line)" stroke-width=".5"/>
            <text x="${x + w / 2}" y="${y + 60}" fill="var(--ts-text)" font-family="JetBrains Mono" font-size="22" font-weight="500" text-anchor="middle">${value}</text>
            <text x="${x + w / 2}" y="${y + 82}" fill="var(--ts-text-3)" font-family="JetBrains Mono" font-size="10" text-anchor="middle">${unit}</text>`;
    }

    function renderPipelineChecks(c, lat, fl, rd, conns) {
        const rows = [
            { s: 'INGRESS',   v: fmt(conns) + ' conn',           d: '', c: 'ok' },
            { s: 'PROTOCOL',  v: fmt(rate(c, 'spends_attempted') + rate(c, 'gets_attempted')) + ' ops/s', d: 'p99 ' + fmtNs(lat.spend?.p99_ns), c: 'ok' },
            { s: 'DISPATCH',  v: '6 opcodes', d: '0 dropped', c: 'ok' },
            { s: 'SHARDS',    v: ((store.nodes?.nodes || []).length || 1) + ' nodes', d: (store.migrations?.active_count || 0) + ' migrations', c: (store.migrations?.active_count || 0) > 0 ? 'warn' : 'ok' },
            { s: 'HOT TIER',  v: ((fl.utilization || 0) * 100).toFixed(1) + '%',      d: fmtBytes(fl.used_bytes), c: barClass(fl.utilization || 0) || 'ok' },
            { s: 'REDO LOG',  v: ((rd.utilization || 0) * 100).toFixed(1) + '%',      d: 'seq ' + fmt(rd.current_sequence), c: barClass(rd.utilization || 0) || 'ok' },
            { s: 'REPLICATION', v: store.replication?.enabled ? store.replication.ack_policy : 'off', d: store.replication?.enabled ? `rf ${store.replication.replication_factor}` : 'single-node', c: store.replication?.enabled ? 'ok' : 'idle' },
        ];
        return `<div class="ts-panel">
            <div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Pipeline health</div><div class="ts-panel__meta">${rows.length} stages</div></div>
            <div class="ts-panel__body" style="padding:0;">
            ${rows.map((r, i) => `<div style="padding:10px 14px;border-bottom:${i < rows.length - 1 ? '1px solid var(--ts-line)' : 'none'};display:grid;grid-template-columns:16px 1fr auto;gap:10px;align-items:center;">
                <span class="ts-sdot ${r.c}"></span>
                <div>
                    <div class="ts-label">${r.s}</div>
                    <div class="ts-num" style="font-size:13px;color:var(--ts-text);margin-top:1px">${r.v}</div>
                </div>
                <div class="ts-label" style="text-align:right">${r.d}</div>
            </div>`).join('')}
            </div>
        </div>`;
    }

    // ---------------------------------------------------------------------------
    // Other pages (kept compact, styled by new theme)
    // ---------------------------------------------------------------------------
    function renderNodes() {
        const n = store.nodes; if (!n) return '<div class="ts-panel"><div class="ts-panel__body">Loading…</div></div>';
        return `<div class="ts-panel"><div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Cluster nodes</div><div class="ts-panel__meta">${(n.nodes || []).length} nodes</div></div>
            <div class="ts-panel__body" style="padding:0"><table><thead><tr><th>NODE</th><th>ADDRESS</th><th>STATE</th><th style="text-align:right">MASTER</th><th style="text-align:right">REPLICA</th></tr></thead>
            <tbody>${(n.nodes || []).map(x => `<tr><td>node-${String(x.node_id).padStart(2,'0')}</td><td>${x.address}</td><td class="ts-${x.state === 'alive' ? 'ok' : 'warn'}">${x.state}</td><td style="text-align:right">${x.master_shards}</td><td style="text-align:right">${x.replica_shards}</td></tr>`).join('')}</tbody></table></div></div>`;
    }
    function renderStorage() {
        const fl = store.freelist || {};
        return `<div class="ts-panel"><div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Storage</div></div>
            <div class="ts-panel__body">${capacityRow('DEVICE', fl.utilization || 0, fmtBytes(fl.used_bytes) + ' / ' + fmtBytes(fl.device_size))}
            <table><tbody>
            ${row('Free regions', fmt(fl.free_region_count))}
            ${row('Largest free', fmtBytes(fl.largest_free_region))}
            ${row('Total free', fmtBytes(fl.total_free_bytes))}
            ${row('Alignment', fmt(fl.alignment) + ' B')}
            </tbody></table></div></div>`;
    }
    function row(k, v) { return `<tr><td>${k}</td><td style="text-align:right;color:var(--ts-text)">${v}</td></tr>`; }
    function renderRecords() {
        const r = store.records || {};
        return `<div class="ts-panel"><div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Record inventory</div></div>
            <div class="ts-panel__body" style="padding:0"><table><tbody>
            ${row('Total records', fmt(r.total_records))}${row('DAH index', fmt(r.dah_index_count))}${row('Unmined', fmt(r.unmined_count))}
            </tbody></table></div></div>
            <div class="ts-panel"><div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Lookup record</div></div>
            <div class="ts-panel__body"><div class="search-box"><input type="text" id="txid-input" placeholder="64-char hex txid"><button onclick="window._searchRecord()">Search</button></div><div id="record-result"></div></div></div>`;
    }
    function renderReplication() {
        const r = store.replication || {};
        if (!r.enabled) return '<div class="ts-panel"><div class="ts-panel__body">Replication not enabled (single-node mode)</div></div>';
        return `<div class="ts-panel"><div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Replication</div></div>
            <div class="ts-panel__body" style="padding:0"><table><tbody>
            ${row('ACK policy', r.ack_policy)}${row('Best effort', r.best_effort)}${row('Topology term', r.topology_term)}${row('Topology epoch', r.topology_epoch)}${row('Peak cluster size', r.peak_cluster_size)}
            </tbody></table></div></div>`;
    }
    function renderMigrations() {
        const m = store.migrations || {};
        return `<div class="ts-panel"><div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Migrations</div><div class="ts-panel__meta">${m.active_count || 0} active</div></div>
            <div class="ts-panel__body">${(m.migrations || []).length ? `<table><thead><tr><th>SHARD</th><th>FROM</th><th>TO</th><th>STATE</th><th style="text-align:right">PROGRESS</th></tr></thead><tbody>
            ${(m.migrations || []).map(mi => `<tr><td>${mi.shard}</td><td>node-${mi.from_node}</td><td>node-${mi.to_node}</td><td>${mi.state}</td><td style="text-align:right">${fmt(mi.migrated_records)}/${fmt(mi.total_records)}</td></tr>`).join('')}
            </tbody></table>` : '<div class="ts-label">No active migrations</div>'}</div></div>`;
    }
    function renderConfig() {
        const levels = ['error', 'warn', 'info', 'debug', 'trace'];
        const cur = store.logLevel || 'info';
        return `<div class="ts-panel"><div class="ts-panel__head"><div class="ts-panel__title"><span class="ts-dot"></span>Log level</div></div>
            <div class="log-levels">${levels.map(l => `<button class="${l === cur ? 'active' : ''}" onclick="window._setLogLevel('${l}')">${l}</button>`).join('')}</div></div>`;
    }

    // ---------------------------------------------------------------------------
    // Live (/top) — kept from original, restyled
    // ---------------------------------------------------------------------------
    function connectWs() {
        // No token, or the token was already rejected by a fetch: prompt
        // instead of hammering the gate with doomed handshakes.
        if (!getToken() || authFailed) { showLogin(authFailed ? null : 'Enter the admin token to view live metrics.'); return; }
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const url = proto + '//' + location.host + '/ws/top';
        // Browsers can't set Authorization on a WebSocket handshake, so the
        // token rides as a second offered subprotocol. The server selects and
        // echoes the non-secret 'teraslab.v1' marker (see WS_TOP_SUBPROTOCOL /
        // require_admin_bearer in src/server/http.rs).
        ws = new WebSocket(url, ['teraslab.v1', 'Bearer.' + getToken()]);
        ws.onopen = () => { store.wsConnected = true; renderCurrentPage(); };
        ws.onmessage = e => {
            store.prevSnapshot = store.topSnapshot;
            store.topSnapshot = JSON.parse(e.data);
            pushHistory();
            if (['top', 'dashboard', 'flow', 'observability'].includes(currentPage())) renderCurrentPage();
        };
        ws.onclose = () => {
            store.wsConnected = false;
            renderCurrentPage();
            // Reconnect only while a token is present and not known-bad; a 401
            // on the fetch side flips authFailed and stops the retry storm.
            if (getToken() && !authFailed) setTimeout(connectWs, 2000);
        };
        ws.onerror = () => ws.close();
    }

    function renderTop() {
        const resp = store.topSnapshot;
        const dot = `<span class="live-dot ${store.wsConnected ? 'connected' : 'disconnected'}"></span>`;
        if (!resp) return `<div class="live-header">${dot}${store.wsConnected ? 'Connected' : 'Connecting…'}</div><div class="ts-panel"><div class="ts-panel__body">Waiting for data…</div></div>`;
        const agg = resp.aggregate || resp;
        return `<div class="live-header">${dot}${store.wsConnected ? 'Live' : 'Disconnected'} · ${agg.node_count || 1} nodes · ${agg.connections || 0} connections</div>
            ${kpiCard({ label: 'OPS / SEC', value: fmt(store.history.ops.at(-1) || 0), unit: '', series: store.history.ops, color: 'var(--ts-accent)' })}`;
    }

    // ---------------------------------------------------------------------------
    // Page: Observability — per-op outcomes, latencies, replication,
    //                     redo, migrations, swim, allocator
    //
    // Each panel is guarded against missing data: `fmt()` returns '-' for null,
    // and panels print "no activity" placeholders when a subsystem has zero
    // traffic. Field names mirror the server JSON (src/server/http.rs:853-1135).
    // ---------------------------------------------------------------------------

    // Stable opcode list for outcome table. Must match OpCode::all() in
    // src/metrics.rs so rows render in a consistent order.
    const OP_CODES = [
        'spend', 'unspend', 'create', 'set_mined', 'freeze', 'unfreeze',
        'reassign', 'set_conflicting', 'set_locked', 'preserve_until',
        'delete', 'mark_longest_chain', 'get', 'get_spend',
    ];
    // Per-op latency histograms are only exposed for 3 opcodes today
    // (spend, spend_multi, unspend, lock_wait). Map OP_CODE → histogram key.
    const OP_LATENCY_KEY = {
        spend: 'spend', unspend: 'unspend',
        // Everything else shares the spend histogram as a rough proxy.
        // When phase-4 adds per-op histograms this map will grow; for now we
        // leave the latency cells showing '-' for ops without a dedicated
        // histogram so operators aren't misled by stale data.
    };
    const OUTCOMES = [
        'ok', 'idempotent', 'err_not_found', 'err_conflicting',
        'err_frozen', 'err_storage', 'redirect', 'other',
    ];
    const ERR_OUTCOMES = new Set(['err_not_found', 'err_conflicting', 'err_frozen', 'err_storage']);

    function getLocalSnap() {
        const ts = store.topSnapshot;
        if (!ts) return null;
        // The WS envelope is { aggregate, nodes: [...] }; the new observability
        // sub-shapes are per-node. Pick nodes[0] if available; else treat the
        // payload itself as the local snapshot.
        if (ts.nodes && ts.nodes.length) return ts.nodes[0];
        return ts.aggregate || ts;
    }
    function getAggSnap() {
        const ts = store.topSnapshot;
        if (!ts) return null;
        return ts.aggregate || ts;
    }

    function renderObservability() {
        const ts = store.topSnapshot;
        if (!ts) {
            return '<div class="ts-panel"><div class="ts-panel__body">Waiting for metrics stream…</div></div>';
        }
        const agg = getAggSnap();
        const local = getLocalSnap();

        return [
            opsOutcomePanel(agg),
            replicationPanel(local),
            redoPanel(local),
            migrationPanel(local),
            swimPanel(local),
            allocatorPanel(local),
        ].join('');
    }

    // --- Panel: per-op outcome breakdown + latency percentiles --------------
    function opsOutcomePanel(agg) {
        const ops = agg?.operations || {};
        const lat = agg?.latency || {};
        // Build rows with totals, % per outcome, error-severity flag
        const rows = OP_CODES.map(op => {
            const row = ops[op] || {};
            const total = OUTCOMES.reduce((s, o) => s + (row[o] || 0), 0);
            const errs = ['err_not_found', 'err_conflicting', 'err_frozen', 'err_storage']
                .reduce((s, o) => s + (row[o] || 0), 0);
            const errRatio = total > 0 ? errs / total : 0;
            return { op, row, total, errs, errRatio };
        });
        const activeRows = rows.filter(r => r.total > 0);
        const totalOps = rows.reduce((s, r) => s + r.total, 0);
        const totalErrs = rows.reduce((s, r) => s + r.errs, 0);

        // Outcome column headers with short label
        const head = `<thead><tr>
            <th>OP</th>
            <th style="text-align:right">TOTAL</th>
            <th style="text-align:right">OK</th>
            <th style="text-align:right">IDEMP</th>
            <th style="text-align:right">NOT-FOUND</th>
            <th style="text-align:right">CONFLICT</th>
            <th style="text-align:right">FROZEN</th>
            <th style="text-align:right">STORAGE</th>
            <th style="text-align:right">REDIRECT</th>
            <th style="text-align:right">OTHER</th>
            <th style="text-align:right">ERR %</th>
            <th style="text-align:right">P50</th>
            <th style="text-align:right">P99</th>
            <th style="text-align:left">MIX</th>
        </tr></thead>`;

        const body = rows.map(r => {
            if (r.total === 0) {
                return `<tr>
                    <td style="color:var(--ts-text)">${r.op}</td>
                    <td style="text-align:right;color:var(--ts-text-3)">-</td>
                    ${OUTCOMES.map(() => '<td style="text-align:right;color:var(--ts-text-4)">-</td>').join('')}
                    <td style="text-align:right;color:var(--ts-text-4)">-</td>
                    <td style="text-align:right;color:var(--ts-text-4)">-</td>
                    <td style="text-align:right;color:var(--ts-text-4)">-</td>
                    <td></td>
                </tr>`;
            }
            const errPct = (r.errRatio * 100);
            const errClass = errPct > 1 ? 'ts-bad' : errPct > 0.1 ? 'ts-warn' : 'ts-ok';
            const latKey = OP_LATENCY_KEY[r.op];
            const p50 = latKey ? lat[latKey]?.p50_ns : null;
            const p99 = latKey ? lat[latKey]?.p99_ns : null;
            return `<tr>
                <td style="color:var(--ts-text)">${r.op}</td>
                <td style="text-align:right;color:var(--ts-text)">${fmt(r.total)}</td>
                ${OUTCOMES.map(o => {
                    const v = r.row[o] || 0;
                    if (v === 0) return `<td style="text-align:right;color:var(--ts-text-4)">-</td>`;
                    const isErr = ERR_OUTCOMES.has(o);
                    const color = isErr && v > 0 ? 'var(--ts-warn)' : 'var(--ts-text-2)';
                    return `<td style="text-align:right;color:${color}">${fmt(v)}</td>`;
                }).join('')}
                <td style="text-align:right" class="${errClass}">${errPct.toFixed(2)}%</td>
                <td style="text-align:right">${fmtNs(p50)}</td>
                <td style="text-align:right">${fmtNs(p99)}</td>
                <td>${stackedMix(r.row, r.total)}</td>
            </tr>`;
        }).join('');

        const summary = totalOps > 0
            ? `${fmt(totalOps)} total ops · ${fmt(totalErrs)} errors · ${(totalErrs / totalOps * 100).toFixed(3)}% err rate · ${activeRows.length}/${OP_CODES.length} ops with activity`
            : 'no activity';

        return `<div class="ts-panel">
            <div class="ts-panel__head">
                <div class="ts-panel__title"><span class="ts-dot"></span>Per-op outcomes · cluster aggregate</div>
                <div class="ts-panel__meta">${summary}</div>
            </div>
            <div class="ts-panel__body" style="padding:0;overflow-x:auto">
                <table class="ts-obs-table">${head}<tbody>${body}</tbody></table>
            </div>
        </div>`;
    }

    // Render a horizontal stacked bar showing the outcome mix for an op.
    function stackedMix(row, total) {
        if (total === 0) return '';
        const segs = [];
        const colorFor = o => {
            if (o === 'ok') return 'var(--ts-ok)';
            if (o === 'idempotent') return 'var(--ts-info)';
            if (o === 'redirect') return 'var(--ts-text-3)';
            if (o === 'other') return 'var(--ts-text-4)';
            if (ERR_OUTCOMES.has(o)) {
                // darker for rarer errors, red for storage
                if (o === 'err_storage') return 'var(--ts-bad)';
                return 'var(--ts-warn)';
            }
            return 'var(--ts-text-4)';
        };
        for (const o of OUTCOMES) {
            const v = row[o] || 0;
            if (v === 0) continue;
            const pct = (v / total) * 100;
            segs.push(`<span title="${o}: ${fmt(v)} (${pct.toFixed(2)}%)" style="background:${colorFor(o)};display:inline-block;height:10px;width:${pct}%;"></span>`);
        }
        return `<div class="ts-stackbar" style="display:flex;width:160px;border-radius:3px;overflow:hidden;background:var(--ts-bg-3);">${segs.join('')}</div>`;
    }

    // --- Panel: replication -------------------------------------------------
    function replicationPanel(local) {
        const rm = local?.replication_metrics;
        const replState = store.replication || {};
        if (!rm) {
            return panel('Replication', 'not populated', '<div style="padding:14px;color:var(--ts-text-3)">Replication metrics not available.</div>');
        }
        const leader = rm.leader_sequence || 0;
        const lat = rm.latency || {};
        const perRep = rm.per_replica || [];
        // Only show replicas that have non-zero activity OR are known from cluster topology
        const interesting = perRep.filter(p =>
            (p.bytes_sent || 0) > 0 ||
            (p.batches_acked || 0) > 0 ||
            (p.batches_failed || 0) > 0 ||
            (p.in_flight || 0) > 0 ||
            (p.last_acked_seq || 0) > 0
        );

        const lagColor = lag => lag < 100 ? 'var(--ts-ok)' : lag < 10000 ? 'var(--ts-warn)' : 'var(--ts-bad)';
        const lagClass = lag => lag < 100 ? 'ts-ok' : lag < 10000 ? 'ts-warn' : 'ts-bad';

        const rows = interesting.length === 0
            ? `<tr><td colspan="7" style="color:var(--ts-text-3);text-align:center;padding:14px;">no replica activity</td></tr>`
            : interesting.map(p => {
                const lag = p.lag != null ? p.lag : Math.max(0, leader - (p.last_acked_seq || 0));
                return `<tr>
                    <td style="color:var(--ts-text)">replica-${p.replica_idx}</td>
                    <td style="text-align:right">${fmt(p.last_acked_seq)}</td>
                    <td style="text-align:right;color:${lagColor(lag)}" class="${lagClass(lag)}">${fmt(lag)}</td>
                    <td style="text-align:right">${fmt(p.in_flight)}</td>
                    <td style="text-align:right">${fmt(p.batches_acked)}</td>
                    <td style="text-align:right">${p.batches_failed > 0 ? '<span class="ts-bad">' + fmt(p.batches_failed) + '</span>' : fmt(p.batches_failed)}</td>
                    <td style="text-align:right">${fmtBytes(p.bytes_sent)}</td>
                </tr>`;
            }).join('');

        const meta = [
            `batch p99 ${fmtNs(lat.p99_ns)}`,
            `p50 ${fmtNs(lat.p50_ns)}`,
            `${fmt(rm.batches_sent)} sent`,
            `${fmtBytes(rm.bytes_sent)} out`,
            `leader seq ${fmt(leader)}`,
            replState.enabled ? `rf ${replState.replication_factor || '-'}` : 'single-node',
        ].join(' · ');

        const kpis = `<div class="grid" style="grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px;">
            <div class="ts-panel" style="padding:10px 14px;margin:0;">
                <div class="ts-label">MAX LAG (SEQS)</div>
                <div class="ts-num" style="font-size:20px;color:${lagColor(store.history.repl_lag.at(-1) || 0)};">${fmt(store.history.repl_lag.at(-1) || 0)}</div>
                <div class="ts-kpi__spark">${sparkline(store.history.repl_lag, { w: 200, h: 24, color: 'var(--ts-warn)' })}</div>
            </div>
            <div class="ts-panel" style="padding:10px 14px;margin:0;">
                <div class="ts-label">BATCH LATENCY P99</div>
                <div class="ts-num" style="font-size:20px">${fmtNs(lat.p99_ns)}</div>
                <div class="ts-label" style="margin-top:6px">p50 ${fmtNs(lat.p50_ns)} · p95 ${fmtNs(lat.p95_ns)}</div>
            </div>
            <div class="ts-panel" style="padding:10px 14px;margin:0;">
                <div class="ts-label">BATCHES SENT</div>
                <div class="ts-num" style="font-size:20px">${fmt(rm.batches_sent)}</div>
                <div class="ts-label" style="margin-top:6px">${fmt(lat.count)} sampled · ${fmtBytes(rm.bytes_sent)}</div>
            </div>
            <div class="ts-panel" style="padding:10px 14px;margin:0;">
                <div class="ts-label">FAILURES</div>
                <div class="ts-num" style="font-size:20px;${totalFailures(perRep) > 0 ? 'color:var(--ts-bad)' : ''}">${fmt(totalFailures(perRep))}</div>
                <div class="ts-label" style="margin-top:6px">across ${perRep.filter(p => p.batches_acked > 0 || p.batches_failed > 0).length} replicas</div>
            </div>
        </div>`;

        const table = `<table>
            <thead><tr>
                <th>REPLICA</th>
                <th style="text-align:right">LAST ACKED SEQ</th>
                <th style="text-align:right">LAG</th>
                <th style="text-align:right">IN FLIGHT</th>
                <th style="text-align:right">ACKED</th>
                <th style="text-align:right">FAILED</th>
                <th style="text-align:right">BYTES SENT</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>`;

        return `<div class="ts-panel">
            <div class="ts-panel__head">
                <div class="ts-panel__title"><span class="ts-dot"></span>Replication</div>
                <div class="ts-panel__meta">${meta}</div>
            </div>
            <div class="ts-panel__body">
                ${kpis}
                <div style="overflow-x:auto">${table}</div>
            </div>
        </div>`;
    }
    function totalFailures(per) { return (per || []).reduce((s, p) => s + (p.batches_failed || 0), 0); }

    // --- Panel: redo log ----------------------------------------------------
    function redoPanel(local) {
        const r = local?.redo_metrics;
        if (!r) {
            return panel('Redo log', 'not populated', '<div style="padding:14px;color:var(--ts-text-3)">Redo metrics not available.</div>');
        }
        const flush = r.flush_latency || {};
        const bytes = r.bytes_per_flush || {};
        const entries = r.entries_per_flush || {};
        if (!r.append_total && !flush.count) {
            return panel('Redo log', 'no activity', '<div style="padding:14px;color:var(--ts-text-3)">No redo appends since boot.</div>');
        }
        // Derive append-rate from rolling op-rate: we don't track redo append_total
        // per-tick, but flush_latency.count * avg(entries_per_flush.mean) gives a
        // close proxy. Prefer the direct mean if available.
        const appendRate = flush.count > 0 && store.history.ops.length >= 2
            ? store.history.ops.at(-1) // rough: ops/s is a decent proxy for append/s when all writes redo
            : 0;

        return `<div class="ts-panel">
            <div class="ts-panel__head">
                <div class="ts-panel__title"><span class="ts-dot"></span>Redo log</div>
                <div class="ts-panel__meta">${fmt(r.append_total)} appends · ${fmt(flush.count)} flushes · ${fmt(r.flush_errors_total)} errors</div>
            </div>
            <div class="ts-panel__body">
                <div class="grid" style="grid-template-columns:repeat(4,1fr);gap:10px;">
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">FLUSH P99</div>
                        <div class="ts-num" style="font-size:20px">${fmtNs(flush.p99_ns)}</div>
                        <div class="ts-kpi__spark">${sparkline(store.history.redo_flush_p99, { w: 220, h: 24, color: 'var(--ts-info)' })}</div>
                    </div>
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">FLUSH P50</div>
                        <div class="ts-num" style="font-size:20px">${fmtNs(flush.p50_ns)}</div>
                        <div class="ts-label" style="margin-top:6px">mean ${fmtNs(flush.mean_ns)}</div>
                    </div>
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">BYTES / FLUSH</div>
                        <div class="ts-num" style="font-size:20px">${bytes.mean_ns != null ? fmtBytes(bytes.mean_ns) : '-'}</div>
                        <div class="ts-label" style="margin-top:6px">p99 ${bytes.p99_ns != null ? fmtBytes(bytes.p99_ns) : '-'}</div>
                    </div>
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">ENTRIES / FLUSH</div>
                        <div class="ts-num" style="font-size:20px">${entries.mean_ns != null ? fmt(entries.mean_ns) : '-'}</div>
                        <div class="ts-label" style="margin-top:6px">p99 ${entries.p99_ns != null ? fmt(entries.p99_ns) : '-'}</div>
                    </div>
                </div>
                <div style="margin-top:10px;display:flex;gap:18px;padding:10px 14px;background:var(--ts-bg-2);border-radius:4px;font-family:var(--ts-mono);font-size:11px;">
                    <div><span class="ts-label">APPEND TOTAL</span> <span style="color:var(--ts-text)">${fmt(r.append_total)}</span></div>
                    <div><span class="ts-label">APPEND/s (proxy)</span> <span style="color:var(--ts-text)">${fmt(appendRate)}</span></div>
                    <div><span class="ts-label">FLUSH ERRORS</span> <span style="${r.flush_errors_total > 0 ? 'color:var(--ts-bad)' : 'color:var(--ts-text)'}">${fmt(r.flush_errors_total)}</span></div>
                </div>
                <div class="ts-label" style="margin-top:8px;font-size:9px;">
                    Note: BYTES / ENTRIES per flush reuse the latency-histogram
                    shape, so "mean_ns" is the server-reported mean sample value
                    (bytes or entries, not nanoseconds).
                </div>
            </div>
        </div>`;
    }

    // --- Panel: migrations --------------------------------------------------
    function migrationPanel(local) {
        const m = local?.migration_metrics;
        if (!m) {
            return panel('Migrations', 'not populated', '<div style="padding:14px;color:var(--ts-text-3)">Migration metrics not available.</div>');
        }
        const bytes = m.bytes_transferred || {};
        const phase = m.phase || {};
        const migs = m.migrations || [];

        const phaseStrip = `<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px;">
            ${phaseCell('PREPARING', phase.preparing, 'var(--ts-text-2)')}
            ${phaseCell('COPYING', phase.copying, 'var(--ts-info)')}
            ${phaseCell('DELTA', phase.delta, 'var(--ts-warn)')}
            ${phaseCell('SERVING NEW', phase.serving_new, 'var(--ts-ok)')}
        </div>`;

        const bytesStrip = `<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px;font-family:var(--ts-mono);font-size:11px;">
            <div><span class="ts-label">OUT · MASTER</span><div style="color:var(--ts-text)">${fmtBytes(bytes.outbound_master)}</div></div>
            <div><span class="ts-label">OUT · REPLICA</span><div style="color:var(--ts-text)">${fmtBytes(bytes.outbound_replica)}</div></div>
            <div><span class="ts-label">IN · MASTER</span><div style="color:var(--ts-text)">${fmtBytes(bytes.inbound_master)}</div></div>
            <div><span class="ts-label">IN · REPLICA</span><div style="color:var(--ts-text)">${fmtBytes(bytes.inbound_replica)}</div></div>
        </div>`;

        // ETA: extrapolate using live per-migration rate. We key off
        // `migrations[i].records_transferred` deltas vs prior topSnapshot.
        const prevLocal = store.prevSnapshot ? (store.prevSnapshot.nodes?.[0] || store.prevSnapshot.aggregate || store.prevSnapshot) : null;
        const prevMigs = prevLocal?.migration_metrics?.migrations || [];
        const prevBy = {};
        for (const pm of prevMigs) prevBy[pm.shard + ':' + pm.to_node] = pm;
        const dtMs = (local?.timestamp_ms || 0) - (prevLocal?.timestamp_ms || 0);

        const table = migs.length === 0
            ? '<div style="padding:14px;color:var(--ts-text-3)">No active migrations.</div>'
            : `<table>
                <thead><tr>
                    <th>SHARD</th>
                    <th>FROM → TO</th>
                    <th>ROLE</th>
                    <th>PHASE</th>
                    <th style="text-align:right">RECORDS</th>
                    <th style="text-align:right">BYTES</th>
                    <th style="min-width:160px">PROGRESS</th>
                    <th style="text-align:right">ETA</th>
                </tr></thead>
                <tbody>${migs.map(mi => {
                    const done = mi.records_transferred || 0;
                    const tot = mi.total_records || 0;
                    const pct = tot > 0 ? Math.min(100, (done / tot) * 100) : 0;
                    const key = mi.shard + ':' + mi.to_node;
                    const prev = prevBy[key];
                    let eta = '-';
                    if (prev && dtMs > 0 && tot > 0) {
                        const rate = (done - (prev.records_transferred || 0)) * 1000 / dtMs;
                        if (rate > 0) {
                            const secs = (tot - done) / rate;
                            eta = secs > 3600 ? (secs / 3600).toFixed(1) + 'h' : secs > 60 ? (secs / 60).toFixed(1) + 'm' : secs.toFixed(0) + 's';
                        }
                    }
                    return `<tr>
                        <td style="color:var(--ts-text)">${mi.shard}</td>
                        <td>node-${mi.from_node} → node-${mi.to_node}</td>
                        <td>${mi.is_master ? 'master' : 'replica'}</td>
                        <td>${mi.phase}</td>
                        <td style="text-align:right">${fmt(done)} / ${fmt(tot)}</td>
                        <td style="text-align:right">${fmtBytes(mi.bytes_transferred)}</td>
                        <td>
                            <div class="ts-bar"><div class="ts-bar__fill" style="width:${pct}%;background:var(--ts-info)"></div></div>
                            <div class="ts-label" style="margin-top:2px">${pct.toFixed(1)}%</div>
                        </td>
                        <td style="text-align:right">${eta}</td>
                    </tr>`;
                }).join('')}</tbody>
            </table>`;

        return `<div class="ts-panel">
            <div class="ts-panel__head">
                <div class="ts-panel__title"><span class="ts-dot"></span>Shard migrations</div>
                <div class="ts-panel__meta">${fmt(m.active)} active · ${fmt(m.entries_applied_total)} entries applied</div>
            </div>
            <div class="ts-panel__body">
                ${phaseStrip}
                ${bytesStrip}
                <div style="overflow-x:auto">${table}</div>
            </div>
        </div>`;
    }
    function phaseCell(label, v, color) {
        return `<div class="ts-panel" style="padding:10px 14px;margin:0;">
            <div class="ts-label">${label}</div>
            <div class="ts-num" style="font-size:20px;color:${(v || 0) > 0 ? color : 'var(--ts-text-3)'}">${fmt(v || 0)}</div>
        </div>`;
    }

    // --- Panel: SWIM --------------------------------------------------------
    function swimPanel(local) {
        const sw = local?.swim_metrics;
        if (!sw) {
            return panel('SWIM failure detector', 'not populated', '<div style="padding:14px;color:var(--ts-text-3)">SWIM metrics not available.</div>');
        }
        const churn = sw.churn || {};
        const susp = sw.suspicion_duration || {};
        const totalProbes = sw.probes_sent || 0;
        const anyActivity = totalProbes + (sw.probe_timeouts || 0) + (sw.indirect_probes || 0) +
            Object.values(churn).reduce((s, v) => s + (v || 0), 0);
        if (anyActivity === 0) {
            return panel('SWIM failure detector', 'no activity', '<div style="padding:14px;color:var(--ts-text-3)">No SWIM activity yet.</div>');
        }

        // Probe-rate derived from rolling history (reusing cadence of pushHistory):
        // probes are cumulative — compute last delta per second if we have prev.
        const prev = store.prevSnapshot ? (store.prevSnapshot.nodes?.[0] || store.prevSnapshot.aggregate || store.prevSnapshot) : null;
        const prevSw = prev?.swim_metrics;
        const now = local.timestamp_ms || 0;
        const then = prev?.timestamp_ms || 0;
        let probeRate = 0;
        if (prevSw && now > then) {
            probeRate = Math.max(0, (totalProbes - (prevSw.probes_sent || 0)) * 1000 / (now - then));
        }

        const timeoutPct = totalProbes > 0 ? (sw.probe_timeouts / totalProbes) * 100 : 0;
        const timeoutClass = timeoutPct > 5 ? 'ts-bad' : timeoutPct > 1 ? 'ts-warn' : 'ts-ok';

        const churnRows = `<tr>
            <td><span class="ts-label">JOIN</span></td>
            <td style="text-align:right;color:var(--ts-ok)">${fmt(churn.join || 0)}</td>
            <td><span class="ts-label">SUSPECT</span></td>
            <td style="text-align:right;color:${churn.suspect > 0 ? 'var(--ts-warn)' : 'var(--ts-text-3)'}">${fmt(churn.suspect || 0)}</td>
            <td><span class="ts-label">ALIVE←SUSPECT</span></td>
            <td style="text-align:right;color:var(--ts-info)">${fmt(churn.alive_from_suspect || 0)}</td>
            <td><span class="ts-label">LEAVE</span></td>
            <td style="text-align:right;color:${churn.leave > 0 ? 'var(--ts-bad)' : 'var(--ts-text-3)'}">${fmt(churn.leave || 0)}</td>
        </tr>`;

        return `<div class="ts-panel">
            <div class="ts-panel__head">
                <div class="ts-panel__title"><span class="ts-dot"></span>SWIM failure detector</div>
                <div class="ts-panel__meta">${fmt(totalProbes)} probes · ${fmt(sw.probe_timeouts)} timeouts · ${fmt(sw.indirect_probes)} indirect</div>
            </div>
            <div class="ts-panel__body">
                <div class="grid" style="grid-template-columns:repeat(4,1fr);gap:10px;">
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">PROBE RATE</div>
                        <div class="ts-num" style="font-size:20px">${fmt(probeRate)}<span class="ts-label"> /s</span></div>
                        <div class="ts-label" style="margin-top:6px">total ${fmt(totalProbes)}</div>
                    </div>
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">TIMEOUT %</div>
                        <div class="ts-num ${timeoutClass}" style="font-size:20px">${timeoutPct.toFixed(2)}%</div>
                        <div class="ts-label" style="margin-top:6px">${fmt(sw.probe_timeouts)} timeouts</div>
                    </div>
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">SUSPICION P99</div>
                        <div class="ts-num" style="font-size:20px">${fmtNs(susp.p99_ns)}</div>
                        <div class="ts-label" style="margin-top:6px">${fmt(susp.count)} events · p50 ${fmtNs(susp.p50_ns)}</div>
                    </div>
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">INDIRECT PROBES</div>
                        <div class="ts-num" style="font-size:20px">${fmt(sw.indirect_probes)}</div>
                        <div class="ts-label" style="margin-top:6px">PING_REQ rounds</div>
                    </div>
                </div>
                <div class="ts-panel" style="margin:10px 0 0 0;padding:10px 14px;">
                    <div class="ts-label" style="margin-bottom:8px">MEMBERSHIP CHURN</div>
                    <table style="font-size:11px"><tbody>${churnRows}</tbody></table>
                </div>
            </div>
        </div>`;
    }

    // --- Panel: allocator ---------------------------------------------------
    function allocatorPanel(local) {
        const a = local?.allocator_metrics;
        if (!a) {
            return panel('Allocator', 'not populated', '<div style="padding:14px;color:var(--ts-text-3)">Allocator metrics not available.</div>');
        }
        if (!a.alloc_total && !a.free_total && !a.freelist_region_count) {
            return panel('Allocator', 'no activity', '<div style="padding:14px;color:var(--ts-text-3)">No allocations recorded yet.</div>');
        }
        // Free-rate: deltas vs prev snapshot.
        const prev = store.prevSnapshot ? (store.prevSnapshot.nodes?.[0] || store.prevSnapshot.aggregate || store.prevSnapshot) : null;
        const prevA = prev?.allocator_metrics;
        const now = local.timestamp_ms || 0;
        const then = prev?.timestamp_ms || 0;
        let freeRate = 0, freeBytesRate = 0, allocBytesRate = 0;
        if (prevA && now > then) {
            const dt = (now - then) / 1000;
            freeRate = Math.max(0, ((a.free_total || 0) - (prevA.free_total || 0)) / dt);
            freeBytesRate = Math.max(0, ((a.free_bytes_total || 0) - (prevA.free_bytes_total || 0)) / dt);
            allocBytesRate = Math.max(0, ((a.alloc_bytes_total || 0) - (prevA.alloc_bytes_total || 0)) / dt);
        }
        const allocRate = store.history.alloc_rate.at(-1) || 0;
        const liveBytes = Math.max(0, (a.alloc_bytes_total || 0) - (a.free_bytes_total || 0));
        const liveAllocs = Math.max(0, (a.alloc_total || 0) - (a.free_total || 0));

        // Simple fragmentation estimate: 1 - (largest_region / avg_region)
        // When regions are well-coalesced, largest ≈ avg and fragmentation → 0.
        // When fragmented, largest ≪ total/regions, so ratio grows.
        const regions = a.freelist_region_count || 0;
        const largest = a.freelist_largest_region_bytes || 0;
        // Free bytes available can't be derived from allocator_metrics alone
        // (free_bytes_total is cumulative, not a live balance). Use store.freelist
        // snapshot when available for total_free_bytes.
        const freeBytesLive = store.freelist?.total_free_bytes || 0;
        let fragPct = null;
        if (regions > 0 && freeBytesLive > 0) {
            const avgRegion = freeBytesLive / regions;
            fragPct = avgRegion > 0 ? Math.max(0, Math.min(100, (1 - largest / freeBytesLive) * 100)) : 0;
        }
        const fragClass = fragPct == null ? '' : fragPct > 80 ? 'ts-bad' : fragPct > 50 ? 'ts-warn' : 'ts-ok';

        return `<div class="ts-panel">
            <div class="ts-panel__head">
                <div class="ts-panel__title"><span class="ts-dot"></span>Allocator</div>
                <div class="ts-panel__meta">${fmt(liveAllocs)} live · ${fmtBytes(liveBytes)} · ${fmt(regions)} free regions</div>
            </div>
            <div class="ts-panel__body">
                <div class="grid" style="grid-template-columns:repeat(4,1fr);gap:10px;">
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">ALLOC RATE</div>
                        <div class="ts-num" style="font-size:20px">${fmt(allocRate)}<span class="ts-label"> /s</span></div>
                        <div class="ts-kpi__spark">${sparkline(store.history.alloc_rate, { w: 220, h: 24, color: 'var(--ts-accent)' })}</div>
                    </div>
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">FREE RATE</div>
                        <div class="ts-num" style="font-size:20px">${fmt(freeRate)}<span class="ts-label"> /s</span></div>
                        <div class="ts-label" style="margin-top:6px">${fmtBytes(freeBytesRate)}/s released</div>
                    </div>
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">ALLOC BYTES/s</div>
                        <div class="ts-num" style="font-size:20px">${fmtBytes(allocBytesRate)}</div>
                        <div class="ts-label" style="margin-top:6px">total ${fmtBytes(a.alloc_bytes_total)}</div>
                    </div>
                    <div class="ts-panel" style="padding:10px 14px;margin:0;">
                        <div class="ts-label">FRAGMENTATION</div>
                        <div class="ts-num ${fragClass}" style="font-size:20px">${fragPct == null ? '-' : fragPct.toFixed(1) + '%'}</div>
                        <div class="ts-label" style="margin-top:6px">largest ${fmtBytes(largest)}</div>
                    </div>
                </div>
                <div style="margin-top:10px;display:grid;grid-template-columns:repeat(3,1fr);gap:8px;padding:10px 14px;background:var(--ts-bg-2);border-radius:4px;font-family:var(--ts-mono);font-size:11px;">
                    <div><span class="ts-label">ALLOC TOTAL</span> <span style="color:var(--ts-text)">${fmt(a.alloc_total)}</span></div>
                    <div><span class="ts-label">FREE TOTAL</span> <span style="color:var(--ts-text)">${fmt(a.free_total)}</span></div>
                    <div><span class="ts-label">REGIONS</span> <span style="color:var(--ts-text)">${fmt(regions)}</span></div>
                    <div><span class="ts-label">ALLOC BYTES</span> <span style="color:var(--ts-text)">${fmtBytes(a.alloc_bytes_total)}</span></div>
                    <div><span class="ts-label">FREE BYTES</span> <span style="color:var(--ts-text)">${fmtBytes(a.free_bytes_total)}</span></div>
                    <div><span class="ts-label">LARGEST REGION</span> <span style="color:var(--ts-text)">${fmtBytes(largest)}</span></div>
                </div>
            </div>
        </div>`;
    }

    // Small helper for panels with no data
    function panel(title, meta, body) {
        return `<div class="ts-panel">
            <div class="ts-panel__head">
                <div class="ts-panel__title"><span class="ts-dot" style="background:var(--ts-text-3)"></span>${title}</div>
                <div class="ts-panel__meta">${meta}</div>
            </div>
            <div class="ts-panel__body" style="padding:0">${body}</div>
        </div>`;
    }

    // ---------------------------------------------------------------------------
    // Router
    // ---------------------------------------------------------------------------
    const pages = {
        dashboard: renderDashboard,
        flow: renderFlow,
        observability: renderObservability,
        top: renderTop,
        nodes: renderNodes,
        storage: renderStorage,
        records: renderRecords,
        replication: renderReplication,
        migrations: renderMigrations,
        config: renderConfig,
    };
    function currentPage() {
        const h = location.hash.replace('#/', '') || 'dashboard';
        return h in pages ? h : 'dashboard';
    }
    function renderCurrentPage() {
        const p = currentPage();
        document.getElementById('content').innerHTML = pages[p]();
        document.querySelectorAll('.nav-link').forEach(l => l.classList.toggle('active', l.dataset.page === p));
        // WS is always-on so history fills on all pages
        connectWs();
    }

    // ---------------------------------------------------------------------------
    // Global handlers
    // ---------------------------------------------------------------------------
    window._searchRecord = async function () {
        const txid = document.getElementById('txid-input').value.trim();
        const el = document.getElementById('record-result');
        if (!txid || txid.length !== 64) { el.innerHTML = '<div class="alert warning">Enter a valid 64-char hex txid</div>'; return; }
        const r = await apiFetch('/debug/records/' + txid);
        if (!r.ok) { el.innerHTML = '<div class="alert warning">Record not found</div>'; return; }
        const d = await r.json();
        el.innerHTML = '<table><tbody>' + Object.entries(d).map(kv => `<tr><td>${escapeHtml(kv[0])}</td><td style="color:var(--ts-text)">${escapeHtml(displayValue(kv[1]))}</td></tr>`).join('') + '</tbody></table>';
    };
    window._setLogLevel = async function (level) {
        await apiFetch('/debug/log-level', { method: 'PUT', body: level });
        store.logLevel = level; renderCurrentPage();
    };

    // ---------------------------------------------------------------------------
    // Init
    // ---------------------------------------------------------------------------
    if (localStorage.getItem('teraslab-theme') === 'light') document.body.classList.add('light');
    document.getElementById('theme-toggle').addEventListener('click', () => {
        document.body.classList.toggle('light');
        localStorage.setItem('teraslab-theme', document.body.classList.contains('light') ? 'light' : 'dark');
    });
    window.addEventListener('hashchange', renderCurrentPage);

    // Prompt up front when there is no stored token; otherwise the first
    // admin fetch would 401 and surface the same prompt a beat later.
    if (!getToken()) showLogin('Enter the admin token to view live metrics.');
    refreshAll();
    refreshTimer = setInterval(refreshAll, 3000);
})();
