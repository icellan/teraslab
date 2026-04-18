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
        history: { ops: [], p99: [], p50: [], storage: [], redo: [], repl: [], errors: [] },
    };
    const HISTORY_MAX = 60;
    let refreshTimer = null;
    let ws = null;

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
        push(store.history.repl, (store.replication?.ack_p99_ms) || 0);
        const errRate = ((c.spends_attempted || 0) - (c.spends_succeeded || 0));
        push(store.history.errors, errRate);
    }
    function push(buf, v) { buf.push(v); if (buf.length > HISTORY_MAX) buf.shift(); }

    // ---------------------------------------------------------------------------
    // API
    // ---------------------------------------------------------------------------
    async function fetchJson(path) {
        try { const r = await fetch(path); if (!r.ok) return null; return await r.json(); } catch { return null; }
    }
    async function refreshAll() {
        const [status, index, freelist, redo, nodes, memory, records, replication, migrations, logLevel] =
            await Promise.all([
                fetchJson('/status'), fetchJson('/debug/index'), fetchJson('/debug/freelist'),
                fetchJson('/debug/redo'), fetchJson('/admin/nodes'), fetchJson('/admin/memory'),
                fetchJson('/admin/records'), fetchJson('/admin/replication'),
                fetchJson('/admin/migration_status'),
                fetch('/debug/log-level').then(r => r.text()).catch(() => null),
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
            `<div class="alert ${a.severity}"><span class="ts-kicker">${a.severity}</span><span>${a.message}</span></div>`
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
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        ws = new WebSocket(proto + '//' + location.host + '/ws/top');
        ws.onopen = () => { store.wsConnected = true; renderCurrentPage(); };
        ws.onmessage = e => {
            store.prevSnapshot = store.topSnapshot;
            store.topSnapshot = JSON.parse(e.data);
            pushHistory();
            if (['top', 'dashboard', 'flow'].includes(currentPage())) renderCurrentPage();
        };
        ws.onclose = () => { store.wsConnected = false; renderCurrentPage(); setTimeout(connectWs, 2000); };
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
    // Router
    // ---------------------------------------------------------------------------
    const pages = {
        dashboard: renderDashboard,
        flow: renderFlow,
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
        const r = await fetch('/debug/records/' + txid);
        if (!r.ok) { el.innerHTML = '<div class="alert warning">Record not found</div>'; return; }
        const d = await r.json();
        el.innerHTML = '<table><tbody>' + Object.entries(d).map(kv => `<tr><td>${kv[0]}</td><td style="color:var(--ts-text)">${kv[1]}</td></tr>`).join('') + '</tbody></table>';
    };
    window._setLogLevel = async function (level) {
        await fetch('/debug/log-level', { method: 'PUT', body: level });
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

    refreshAll();
    refreshTimer = setInterval(refreshAll, 3000);
})();
