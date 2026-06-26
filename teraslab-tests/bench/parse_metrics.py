#!/usr/bin/env python3
"""Summarize TeraSlab /metrics pipeline histograms for bottleneck attribution.

Usage: parse_metrics.py <variant>_metrics.txt [...]
Prints, per histogram: count, mean, p50, p99 (ns histograms render as time).
"""
import sys, re

HISTS = [
    "teraslab_create_latency_ns", "teraslab_spend_latency_ns",
    "teraslab_get_latency_ns", "teraslab_set_mined_latency_ns",
    "teraslab_lock_wait_ns", "teraslab_redo_flush_latency_ns",
    "teraslab_redo_entries_per_flush", "teraslab_redo_bytes_per_flush",
    "teraslab_redo_checkpoint_duration_ns",
]
COUNTERS = [
    "teraslab_creates_succeeded_total", "teraslab_spends_succeeded_total",
    "teraslab_gets_succeeded_total", "teraslab_set_mined_items_succeeded_total",
]

def load(path):
    buckets, scalars = {}, {}
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r'(\w+)\{le="([^"]+)"\}\s+(\d+)', line)
        if m:
            name, le, cum = m.group(1), m.group(2), int(m.group(3))
            le = float("inf") if le == "+Inf" else float(le)
            buckets.setdefault(name, []).append((le, cum))
            continue
        m = re.match(r'(\w+)\s+(\d+(?:\.\d+)?)', line)
        if m:
            scalars[m.group(1)] = float(m.group(2))
    return buckets, scalars

def pct(bkts, total, q):
    target = q * total
    for le, cum in bkts:
        if cum >= target:
            return le
    return bkts[-1][0] if bkts else 0

def fmt_ns(ns):
    if ns == float("inf"): return "+Inf"
    if ns >= 1e9: return f"{ns/1e9:.2f}s"
    if ns >= 1e6: return f"{ns/1e6:.2f}ms"
    if ns >= 1e3: return f"{ns/1e3:.1f}us"
    return f"{ns:.0f}ns"

def report(path):
    bkts, sc = load(path)
    print(f"\n=== {path} ===")
    print(f"{'metric':<38} {'count':>9} {'mean':>10} {'p50':>10} {'p99':>10}")
    for h in HISTS:
        cnt = sc.get(h + "_count", 0)
        if cnt == 0:
            continue
        mean = sc.get(h + "_sum", 0) / cnt
        b = sorted(bkts.get(h + "_bucket", []))
        is_ns = h.endswith("_ns")
        unit = fmt_ns if is_ns else (lambda x: f"{x:.0f}")
        mean_s = fmt_ns(mean) if is_ns else f"{mean:.1f}"
        print(f"{h:<38} {int(cnt):>9} {mean_s:>10} {unit(pct(b,cnt,.50)):>10} {unit(pct(b,cnt,.99)):>10}")
    print("counters:", {c.replace('teraslab_','').replace('_total',''): int(sc.get(c,0)) for c in COUNTERS})

for p in sys.argv[1:]:
    report(p)
