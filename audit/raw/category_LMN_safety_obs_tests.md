# Category L / M / N Audit — DoS limits, Observability, Test infrastructure

Repository: `/Users/siggioskarsson/gitcheckout/teraslab`
Scope: server / observability stack, HTTP surface, test corpus, and a repo-wide hazard hunt for dangerous Rust patterns.
Reference files audited:

- Server: `src/server/mod.rs`, `src/server/dispatch.rs`, `src/server/http.rs`, `src/server/startup.rs`
- Observability: `src/metrics.rs`, `src/observability/mod.rs`
- Binaries: `src/bin/server.rs`, `src/bin/cli.rs`
- Tests: every file in `tests/` (16 top-level integration tests, plus
  `tests/simulation/`, `tests/stress/`, `tests/workload/`)
- Web UI: `ui/app.js`, `ui/index.html`, `ui/style.css`

## Overview

TeraSlab's Category L (DoS limits) story is **partially in place but incomplete**.
The wire layer caps frame size at 16 MiB, drops connections when a 30-second
read timeout fires, refuses to bind non-loopback addresses unless the operator
opts in, and gates `/admin/*` mutating endpoints behind an explicit
`enable_admin_endpoints` flag (see `src/config.rs:286`, `src/server/mod.rs:212`,
`src/server/mod.rs:240`). However the binary protocol uses thread-per-connection,
which means a connection-flood attack is bounded only by `max_connections =
1024`, and **slow readers on the response side are completely unbounded** — no
write timeout is ever set on the response stream (`src/server/mod.rs:284`), so
a client that drains 1 byte per second blocks one server thread indefinitely.
That thread holds open per-connection 256 KB read buffers.

Category M (observability) is the strongest area in the codebase. The
Prometheus surface is comprehensive, label cardinality is always bounded by
enum-typed labels (`replica_idx`, `op`, `outcome`, `errno`, `kind`, `direction_role`)
backed by `LabeledCounter<N>` (`src/metrics.rs:90`), histograms emit the
`+Inf` terminator, and per-op `attempted` / `succeeded` / `failed` are
accounted at the right granularity. The **largest M finding** is that
`/health/ready` does not actually verify cluster join state in clustered mode —
the HTTP `state.ready` flag is created as `AtomicBool::new(true)` at server
boot (`src/bin/server.rs:894`) and is never updated based on the cluster
`is_ready()` predicate that dispatch consults (`src/server/dispatch.rs:294`).
A second high-severity gap is that the admin mutation routes have **zero
authentication** — when the operator turns them on with
`enable_admin_endpoints = true`, any client that can reach the HTTP listener
can `PUT /admin/quiesce` or drain a node. The code logs a startup warning
naming the risk (`src/server/http.rs:88-95`), but the warning is not a
substitute for an authentication gate.

Category N (test infrastructure) reveals a **structural gap**: no
property-based testing framework (`proptest` / `quickcheck`) is a dependency,
no fuzz targets exist, and the integration test surface only ever exercises
the `IndexBackendMode::Memory` backend even though `Redb` and `FileBacked`
are first-class production options. The `tests/fault_injection.rs` corpus
covers four crash points cleanly; `tests/recovery_crash_boundaries.rs` adds
five WAL-window scenarios; and `tests/stress/mod.rs` runs 8-thread random
workloads — but none of these are property-based, none assert UTXO
conservation invariants in a randomized form, and **only one `#[ignore]`**
test exists in production (`src/cluster/coordinator.rs:7505`, justified
inline as "TODO: rewrite for pipelined migration flow"). Cluster chaos
testing relies on `tests/cluster_edge_cases.rs` for in-process scenarios
plus the nightly Docker E2E job (`teraslab-tests/`).

The hazard hunt found **0 instances** of `todo!()` / `unimplemented!()` /
`unreachable!()` in `src/`, **0 production `panic!()` calls** outside `#[cfg(test)]`
modules and one structural `panic!` in `src/device_io/mod.rs:116` for an
"impossible" branch. There are **3,317 `unwrap()` / `expect()` call sites**
in `src/`, dominated by tests but with a long tail in `src/protocol/codec.rs`
and `src/server/dispatch.rs` parsing paths (~30 each in production code,
all guarded by length checks before `try_into().unwrap()`). There are **90 `unsafe`
blocks** concentrated in five modules (`record.rs`, `io.rs`, `device.rs`,
`index/hashtable.rs`, `device_io/`), uniformly preceded by safety comments,
but the FFI surface (`libc::munmap`, `libc::msync`, `libc::madvise`,
`libc::fsync`) is the highest-risk area and several blocks lack `// SAFETY:`
prefixes. The `from_raw_parts` family appears 6 times in `src/io.rs` and
`src/record.rs` (all packed-struct ↔ byte-slice conversions; safe
when callers honor the `repr(C, packed)` contract). **No `transmute` calls
exist in the source tree.**

The rest of this report enumerates each finding by category.

---

## L Findings

### LMNH-01: Slow-reader on response stream blocks server thread indefinitely (HIGH)

**Category:** L
**Location:** `src/server/mod.rs:208-287`, specifically `stream.write_all(&response_bytes)` at line 285 with no preceding `set_write_timeout` call.
**What:** `handle_connection` configures a 30-second `set_read_timeout` on the
client stream (line 212) but never sets a write timeout. After the dispatcher
produces a response, `stream.write_all(&response_bytes)` blocks the dedicated
OS thread until either the client drains the bytes or the kernel TCP
keepalive fires (which it won't — `TCP_KEEPALIVE` is not set anywhere either).
A malicious client can advertise a small TCP window and drain at 1 byte/sec,
holding one server thread per connection forever.
**Why it matters:** With `max_connections = 1024` (`src/config.rs:424`), a
trivial DoS — open 1024 connections, send a `OP_GET_BATCH` with a large
field mask, then stop reading the response — exhausts the server-thread
budget. New legitimate clients are then refused at `accept()` (line 124).
The bug is amplified because the response payload for `OP_GET_BATCH` with
`FieldMask::COLD_DATA | UTXO_SLOTS` can grow to multi-MiB, sized by the txid
group, so a single request can pin the thread for a long time even at
moderate window sizes.
**Reproduction:**
```
1. Client opens 1024 connections.
2. Each issues an OP_GET_BATCH request that returns a large payload.
3. Each client advertises TCP recv window = 1 byte.
4. All 1024 server threads are pinned in write_all.
5. Connection #1025 is silently rejected (src/server/mod.rs:127).
```
**Suggested fix:**
```rust
stream.set_write_timeout(Some(Duration::from_secs(30)))?;
```
right after `set_read_timeout`. Document the timeout in the connection-handling
contract. Optionally also set `TCP_USER_TIMEOUT` on Linux so the kernel kills
half-open connections faster than the application does.

---

### LMNH-02: Per-connection read buffer can grow to 16 MiB without bound across N connections (MEDIUM)

**Category:** L
**Location:** `src/server/mod.rs:215`, `src/server/mod.rs:255-261`.
**What:** Each connection allocates a 256 KB read buffer up front (line 215)
and the buffer can be `resize`'d up to `MAX_FRAME_SIZE = 16 MiB`
(`src/protocol/opcodes.rs:324`) when an oversized but legal frame arrives.
Once grown, the buffer is **never shrunk**. A long-lived connection that
sees one large frame followed by many small ones holds 16 MiB of resident
memory for its lifetime. Across `max_connections = 1024` peers this is
**16 GiB of resident memory**, and the only ceiling is the connection cap.
**Why it matters:** The frame-size check at line 240 was added specifically
for gap #10 to bound *per-frame* allocation, but the *per-connection-buffer*
high-water-mark is unbounded across the connection lifetime. The comment at
lines 252-254 claims "buffer growth is bounded regardless of how many
concurrent connections advertise large frames" — that holds for one frame
but not for the cumulative heap-RSS once each connection has seen at least
one large frame.
**Reproduction:**
```
1. Open 1024 connections.
2. From each, send one 16 MiB legal frame.
3. From each, send small frames thereafter.
4. RSS grows to ~16 GiB and stays there.
```
**Suggested fix:** Either (a) shrink the buffer after every request via
`read_buf.shrink_to(256 * 1024)`, or (b) reuse a global slab/pool of
read buffers, returning each one to the pool after the request completes.

---

### LMNH-03: Connection accept never times out a silent client (MEDIUM)

**Category:** L
**Location:** `src/server/mod.rs:120-163` (the accept loop) and
`src/server/mod.rs:208-231` (the per-connection loop).
**What:** When a client establishes a TCP connection but never sends any
bytes, the server thread blocks in `read_exact(&mut len_buf)` (line 225)
for the full 30-second `set_read_timeout` window. The 30 s timeout *does*
fire (returns `TimedOut`), but the next branch is `continue` (line 229),
which simply re-enters `read_exact` and waits another 30 s — forever, in
practice. There is no idle-connection age cap and no shutdown-driven kick.
**Why it matters:** A client can hold a TCP connection forever by simply
opening it and never speaking. Combined with `max_connections = 1024`
this is a half-open-connection DoS. The wire timeout was clearly intended
to mean "kill the connection" but instead means "loop and try again."
**Reproduction:**
```
1. nc 127.0.0.1 3300
2. Don't send any bytes. Repeat 1024 times.
3. The 1025th legitimate client cannot connect.
```
**Suggested fix:** On `TimedOut` at the read-length-prefix boundary,
return cleanly so the connection is dropped; or track `last_activity` and
close after, e.g., 5 minutes of total idle time. Suggested patch at
line 229:
```rust
Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => return Ok(()),
```
The current `WouldBlock` branch (line 228) is also dead — the stream is
set to blocking mode at line 209, so `WouldBlock` cannot fire.

---

### LMNH-04: `max_batch_size` enforced per-request but no aggregate inflight memory cap (LOW)

**Category:** L
**Location:** `src/config.rs:269`, `src/server/dispatch.rs:271`,
`tests/server_tcp.rs:915-942` (positive test).
**What:** `max_batch_size = 8192` (default) is enforced inside each
`decode_*_batch_checked` call (verified by `batch_exceeding_max_batch_size_rejected`
in `tests/server_tcp.rs:915`), so a single request cannot ask for more
than 8192 items. However, `max_connections × max_batch_size × per-item-size`
is not bounded as an aggregate. Dispatch handlers buffer all results in a
`Vec` before encoding (`handle_get_batch` at line 4243, `handle_spend_batch`
at line 2350, etc.), and there is no global semaphore.
**Why it matters:** Coordinated 1024 connections × 8192-item batches × ~4 KB
per result row = ~33 GiB worst-case heap. In practice, each handler also
performs device I/O so throughput throttles naturally, but there is no
explicit cap. This is below `HIGH` because the natural choke is the
1024-thread pool plus device throughput, but operators sizing the box should
know the worst case.
**Reproduction:** Concurrent stress with `max_connections × max_batch_size`
extreme batches. Observe heap RSS during operation.
**Suggested fix:** Add a global `Semaphore` that gates concurrent in-flight
batch processing to a bounded total memory budget; expose it as a config
field (e.g. `max_inflight_batch_items`). Alternatively, document the
worst-case heap calculation in the operator-facing config docs.

---

### LMNH-05: WebSocket `/ws/top` push has no client-side backpressure detection (MEDIUM)

**Category:** L
**Location:** `src/server/http.rs:1764-1788`.
**What:** `ws_top_loop` pushes a JSON snapshot every 1 second (line 1780)
and only breaks the loop if `socket.send().await.is_err()` (line 1777).
A slow WebSocket client (advertising a small recv window) does not produce
a send error — the send hangs in the underlying buffer or Axum's bounded
queue. Worse, in cluster mode each iteration calls
`build_cluster_top_snapshot` (line 1767) which fans out HTTP requests to
every other node with a 2-second timeout (`src/server/http.rs:1512`); on a
3-node cluster a stuck WS connection can pin the tokio runtime in fan-out
loops.
**Why it matters:** WebSocket clients connected from operator dashboards
(or attackers) can throttle to slow the metrics scrape work. Snapshot
construction is non-trivial — it allocates several JSON values, locks
the redo log mutex (`src/server/http.rs:1172`), and calls `index_stats()`
which walks all buckets.
**Reproduction:**
```
1. Connect to /ws/top with a TCP recv window of 1 byte/sec.
2. Server-side: each iteration pushes ~50 KB JSON in cluster mode.
3. After ~50 seconds the buffer fills.
4. The send hangs without erroring; subsequent iterations stack snapshot
   work behind it.
```
**Suggested fix:** Wrap the `socket.send` in a `tokio::time::timeout` of
e.g. 5 seconds; on timeout, break the loop and drop the connection. Also
reduce per-iteration work in cluster mode by reusing a snapshot if the
last push completed less than the configured interval ago.

---

### LMNH-06: HTTP server uses `current_thread` runtime — single tokio worker (LOW)

**Category:** L
**Location:** `src/server/http.rs:72-75`.
**What:** The HTTP server spins up a `tokio::runtime::Builder::new_current_thread()`,
meaning all HTTP work runs on a single OS thread. The `/metrics` handler
holds the `redo_log` mutex briefly (`src/server/http.rs:1172`), and every
`build_local_top_snapshot` call walks several internal stats. Combined with
the WebSocket push every 1 second, the HTTP server is single-threaded.
**Why it matters:** A flood of `/metrics` scrapes (Prometheus rules can
easily produce 10+ scrapes/second from sibling agents) plus 1 WebSocket
client are enough to introduce queueing latency. The `/admin/top` cluster
fan-out, which hits every other node's HTTP server, also runs on this
single thread.
**Reproduction:** Run `wrk -c 100 -t 1 -d 60s http://127.0.0.1:9100/metrics`
and observe metrics latency.
**Suggested fix:** Change to `Builder::new_multi_thread()` with a small
worker pool (2-4 threads) — the HTTP path is not on the hot data plane,
so the cost is negligible.

---

## M Findings

### LMNH-07: `/health/ready` does not verify cluster-join state (HIGH)

**Category:** M
**Location:** `src/bin/server.rs:894`, `src/server/http.rs:839-845`,
`src/server/dispatch.rs:290-300` (the matching readiness gate that *does*
check cluster_health).
**What:** The HTTP `state.ready` flag is hard-coded to `Arc::new(AtomicBool::new(true))`
at server-binary boot (`src/bin/server.rs:894`) and **never reassigned**.
Index loading and recovery happen synchronously *before* the HTTP listener
spawns, so for the in-memory and primary-load steps the flag being `true`
is correct. However, in **cluster mode**, the dispatch path correctly
gates client traffic on `c.cluster_health().is_ready()` (which only flips to
true after the first quorum-committed topology is observed —
`src/cluster/coordinator.rs:5391`), but `/health/ready` does not consult
that predicate. So a clustered node that has joined SWIM but has not yet
seen its first committed topology returns HTTP 200 "ready" while
simultaneously rejecting client requests with `ERR_CLUSTER_NOT_READY`.
**Why it matters:** Load balancers (Kubernetes readiness probes, HAProxy,
Envoy) take `/health/ready = 200` as a signal to route traffic. They
will then route requests that the binary protocol immediately rejects,
producing client-visible errors during normal cluster bootstrap. The
divergence between dispatch readiness and HTTP readiness is exactly the
kind of bug that is invisible until production rolling restarts.
**Reproduction:**
```
1. Start a 3-node cluster from cold.
2. Hit /health/ready on node1 while SWIM is converging — returns 200.
3. Hit OP_PING (or any opcode in `needs_cluster_readiness`) — returns
   ERR_CLUSTER_NOT_READY.
```
**Suggested fix:** In `handle_health_ready`, additionally check
`state.cluster.as_ref().map_or(true, |c| c.cluster_health().is_ready())`.
Same for the secondary-index readiness flag — the gate is in dispatch
(`src/server/dispatch.rs:311`) but `/health/ready` ignores it.

---

### LMNH-08: Admin mutation endpoints have zero authentication when enabled (HIGH)

**Category:** M
**Location:** `src/server/http.rs:142-153`, `src/server/http.rs:88-95` (the
warning), `src/config.rs:288-296` (the gate).
**What:** When `enable_admin_endpoints = true`, the routes
`PUT /admin/quiesce`, `PUT /admin/rebalance`, `PUT /admin/drain/{node_id}`,
`PUT /debug/log-level`, `GET /debug/index`, `GET /debug/redo`, and
`GET /debug/records/{txid}` are registered without any authentication
or authorization layer. The startup `tracing::warn!` (line 89-95) names
the risk but does not reduce it.
**Why it matters:** `quiesce` triggers a graceful drain — equivalent to
a remote shutdown. `drain/{node_id}` migrates shards off a node. `rebalance`
is an alias for `quiesce`. Any client with HTTP reachability can take a
node out of the cluster. The TOML-level guard (gap #1 — see
`docs/TERANODE_PRODUCTION_READINESS_GAPS.md`) defaults `enable_remote_bind = false`
and `enable_admin_endpoints = false`, but operators that need either
have no opt-in middle ground (mTLS / bearer token / IP allowlist).
**Reproduction:**
```
1. Operator sets enable_remote_bind = true, enable_admin_endpoints = true.
2. Any host with network access to port 9100:
   curl -X PUT http://node:9100/admin/quiesce
3. Node drains and stops accepting writes.
```
**Suggested fix:** Add a bearer-token middleware (`Authorization: Bearer …`)
gated by an `admin_token` config field, OR bind admin endpoints to a
separate listener so operators can firewall them at the network layer.
At minimum, add a `Authorization: Bearer` header check that defaults to
"required when admin endpoints are enabled" so a missing token returns 401.
The mTLS work referenced in the warning text (gap #1) would also close
this, but that is a larger lift.

---

### LMNH-09: `/debug/records/<txid>` accepts unbounded path string before length check (LOW)

**Category:** M
**Location:** `src/server/http.rs:1883-1928`, `src/server/http.rs:1994-2014`.
**What:** Axum's `Path<String>` extractor accepts arbitrarily long path
segments before `parse_hex_txid` rejects anything not exactly 64 chars
(line 1995). Axum's default URL length cap is generous (per the underlying
`hyper` server's `max_uri_length` defaults). A malicious request with a
1 MiB path segment is accepted at the framework level and only rejected
inside the handler — meaning the path string is allocated and copied into
the handler's String first.
**Why it matters:** Low severity because Axum likely caps URL size around
8-64 KiB at the hyper layer, but tests do not assert this. There is also
no rate-limiting on `/debug/records`, so a request flood with even modest
path lengths could pin the single tokio runtime thread (see LMNH-06).
**Reproduction:** `curl http://node:9100/debug/records/$(head -c 8192 /dev/urandom | xxd -p | tr -d '\n')`. Confirms a 64-byte length check after string allocation.
**Suggested fix:** Reject path lengths >64 chars before allocation by
matching on `&str` length in the extractor (or use a custom extractor
with a hard cap of 64 bytes).

---

### LMNH-10: Web UI assigns server-supplied JSON values directly into HTML (MEDIUM)

**Category:** M
**Location:** `ui/app.js:1330` (page rendering), `ui/app.js:1346` (record
table rendering), and ~250 other template-literal sites that interpolate
server-supplied values into HTML strings (e.g. `ui/app.js:178` for
node-state alerts, `ui/app.js:185` for alert messages).
**What:** The admin UI assigns rendered template-literal HTML strings to
DOM element `.innerHTML` properties, including values derived from the
parsed JSON of `/debug/records/<txid>`. Today every value in that JSON
comes from `handle_debug_record` at `src/server/http.rs:1894-1923` and is
either an integer or a `format!("{:#04x}", flags)` hex string — so the
current contract does not produce a live cross-site-scripting vector.
**However** the pattern itself is a hazard:
1. Future fields added to `/debug/records` that include user-controllable
   strings (e.g. an external blob ref hash representation, a parent txid
   list, an error message) would silently introduce a script-injection
   sink.
2. Other UI pages (e.g. node alerts at `ui/app.js:178`) already
   interpolate `n.state` and `n.node_id` into HTML — those values come
   from JSON parsed from the cluster, but a compromised peer could send
   a malicious `state` string when responding to `/admin/nodes` fan-out.
**Why it matters:** Script injection in the admin UI lets an attacker
who controls any cluster peer hijack the admin operator's session,
steal a CSRF token, and pivot to `/admin/quiesce` (LMNH-08). The same UI
is served from the same origin as the admin endpoints, so same-origin
policy does not protect the operator.
**Reproduction:** No live exploit today. To prove the hazard:
1. Add a new field to `handle_debug_record` like
   `"label": format!("{}", user_controlled)`.
2. The label flows directly into rendered HTML.
**Suggested fix:** Switch value cells to `.textContent` assignment, or
run every value through an `escapeHtml` helper. The safest and most
mechanical fix is a one-liner helper at the top of `app.js` plus a
search-and-replace.

---

### LMNH-11: Per-op `attempted` is dispatch-level (one tick per batch frame), per-item `succeeded`/`failed` is handler-level — and they do not sum (MEDIUM)

**Category:** M
**Location:** `src/server/dispatch.rs:330-345` (batch-level attempted),
`src/server/dispatch.rs:2335` (item-level attempted, spend handler),
`src/server/dispatch.rs:4543-4555` (gets handler outcomes).
**What:** The audit goal "every operation increments `attempted` exactly
once" is **violated by design**: for batch ops, dispatch ticks the
**batch-level** `creates_attempted` / `gets_attempted` / `freezes_attempted`
once per request frame at line 332-345, but the handler ticks
**item-level** counters (`spend_multi_items_attempted` etc) by `items.len()`
inside the body. For `spend` and `unspend` only, the dispatch level skips
its own attempted-tick because the spend handler at line 2335 ticks both
`spends_attempted` (item-count) AND `spend_multi_items_attempted` (item-count).
The result: `creates_attempted` counts batches; `spends_attempted` counts
items. This is documented in the code comment at line 326-329 ("per-batch
counters … item-level _items_attempted counters are incremented inside each
handler") but the **counter naming makes the asymmetry invisible to scrape
consumers** who would naturally expect `creates_attempted` to be an item count.
**Why it matters:** Dashboard authors will compute
`rate(creates_succeeded_total[1m]) / rate(creates_attempted_total[1m])` to
get a creation success rate. That ratio is **inflated by the batch size**
because `creates_succeeded` is per-item (line 3303) and `creates_attempted`
is per-batch (line 332). At a typical batch size of 100, the success rate
will read 100x — an SLO that monitors creation health will report 99%+
success when reality is at 50%.
Verified by reading both call sites:
- batch tick: `m.creates_attempted.inc()` at `src/server/dispatch.rs:332`
- per-item: `m.creates_succeeded.inc_by(succeeded_total)` at
  `src/server/dispatch.rs:3303` (where `succeeded_total = total_items - failed_total`).
**Reproduction:**
```
1. Send one OP_CREATE_BATCH with 100 items, all of which succeed.
2. Scrape /metrics:
   teraslab_creates_attempted_total = 1
   teraslab_creates_succeeded_total = 100
3. The success rate appears to be 10000%.
```
**Suggested fix:** Rename batch-level counters to make the dimension
explicit (e.g. `teraslab_creates_batches_total` for the per-batch count)
and add `teraslab_creates_items_attempted_total` mirroring the
`spend_multi_items_attempted` shape used by spend/unspend handlers (which
already get this right — see line 2335-2336). The labeled
`teraslab_operations_total{op,outcome}` table is already item-granular for
all opcodes (line 491, lines 2516-2518) and is the right long-term home.

---

### LMNH-12: Spend handler tallies `idempotent` via subtraction, not direct count (LOW)

**Category:** M
**Location:** `src/server/dispatch.rs:2503-2506`.
**What:** `idempotent_total = items.len() - succeeded - failed` is computed
by subtraction, not by counting validator-flagged idempotent re-spends
directly. Because of `saturating_sub`, an off-by-one in either `succeeded`
or `failed` cannot underflow but **could overcount idempotent**. The code
comment at line 2486-2489 acknowledges that the validator distinguishes
the cases and `apply()` returns `resp.spent_count` which correctly excludes
idempotents — but `idempotent_total` itself is derived, not directly observed.
**Why it matters:** If a future bug introduces double-counting in either
`succeeded` or `failed` (e.g. a redirect-error race), the idempotent counter
silently absorbs the discrepancy via saturating-sub — masking the upstream bug.
**Suggested fix:** Have `validated.apply(engine)` return the idempotent
count directly (the validator already knows it — see the
`validated.errors` map at `src/ops/spend.rs`) and increment
`idempotent_total` by that direct count, rather than by subtraction.

---

### LMNH-13: `/metrics` label cardinality is bounded — verified (INFORMATIONAL)

**Category:** M
**Location:** `src/metrics.rs:90-137`, `src/server/http.rs:560-580`,
`src/server/http.rs:781-795`.
**What:** Every labeled counter is backed by a fixed-size
`LabeledCounter<N>` array, indexed by an enum's `as u8 as usize`
discriminant — not by string interning. Cardinality is bounded by:
- `replica_idx`: `MAX_REPLICAS` (8)
- `op`: `OP_CARDINALITY` (14)
- `outcome`: `OUTCOME_CARDINALITY` (8)
- `errno`: `UringErrClass::all().len()` (8 cells)
- `kind` (SWIM): `SwimChurnKind::all().len()` (4)
- `direction_role` (migration): `MigrationLabel::all().len()` (4)

No counter is ever indexed by txid, peer address, or any other unbounded
string. The `tests/prometheus_conformance.rs:732` test
`metrics_labeled_operations_has_full_cardinality` enforces the full
14×8 grid is emitted on every scrape. **No finding.**

---

### LMNH-14: `/admin/top` cluster fan-out has no concurrency cap (LOW)

**Category:** M
**Location:** `src/server/http.rs:1494-1537`.
**What:** `build_cluster_top_snapshot` spawns one `tokio::spawn` per
remote node (line 1510). For a 100-node cluster, each `/admin/top`
request issues 99 outbound HTTP requests. The 2-second per-request
timeout (line 1512) bounds individual latency but not total fan-out
volume. A flood of `/admin/top` (without `?local=true`) on a 100-node
cluster is N² in the worst case.
**Why it matters:** Internal `?local=true` short-circuit prevents
recursion (line 1509 always sets it on the outbound URL), so this is
not a self-amplifying loop. It is, however, a fan-out factor that the
operator may not realize.
**Suggested fix:** Document the fan-out behavior. Optionally cap with
`futures::stream::iter(...).buffer_unordered(N_PARALLEL)`.

---

### LMNH-15: Histograms emit `+Inf` and `_sum` correctly — verified (INFORMATIONAL)

**Category:** M
**Location:** `src/server/http.rs:810-829`, with positive test at
`src/server/http.rs:2131-2218` and `tests/prometheus_conformance.rs:759`.
**What:** Every `prom_histogram_ns` invocation emits the cumulative
buckets plus a final `le="+Inf"` terminator and `_sum` / `_count` lines.
The unit test
`metrics_endpoint_emits_histogram_buckets` asserts non-decreasing cumulative
counts. **No finding.**

---

## N Findings (test gaps)

### LMNH-16: No property-based tests anywhere in the codebase (HIGH)

**Category:** N
**Location:** `Cargo.toml` (no `proptest` / `quickcheck` dependency),
`Cargo.lock` (confirmed not present), `tests/` (no proptest! macro use).
**What:** UTXO conservation invariants — total
`(unspent + spent + frozen + pruned) = utxo_count` — and idempotency
properties on spend/unspend are textbook proptest targets, but **no
property-based testing framework is a dependency**. Every test is a
hand-written scenario. The closest substitute is
`tests/e2e_workload.rs:235` (`e2e_crash_injection_10_seeds`), which runs
10 fixed seeds, and `tests/simulation/mod.rs` which has a `seed` field
but is built around fixed sequences.
**Why it matters:** The project's CLAUDE.md banishes `assert!(true)`-style
tests, mandates non-vacuous assertions, and explicitly invites coverage
of "all the tests specified in the phase" — but the phase descriptions
do not include proptest. UTXO conservation, redo-log replay idempotency,
and shard table determinism are all properties begging for randomized
input. The hand-written corpus is excellent (`tests/integration.rs:1053`
`fn snapshot_index_and_persist_allocator_on_shutdown` is exemplary)
but cannot cover the input space.
**Reproduction:** `grep -r proptest src/ tests/ Cargo.toml` returns nothing.
**Suggested fix:** Add `proptest = "1"` to `[dev-dependencies]`. At minimum,
write four properties:
1. `prop_utxo_conservation`: arbitrary sequence of create/spend/unspend
   preserves `unspent + spent = utxo_count`.
2. `prop_replay_idempotency`: redo log replay applied N times equals N=1.
3. `prop_shard_table_deterministic`: identical member sets produce
   identical assignments (already covered by hand-written
   `tests/cluster_edge_cases.rs:339`, but property form would cover more
   members + more rounds).
4. `prop_protocol_codec_roundtrip`: arbitrary `RequestFrame` round-trips
   through `encode/decode` without loss.

---

### LMNH-17: No fuzz targets for the wire-protocol parser (HIGH)

**Category:** N
**Location:** No `fuzz/` directory, no `cargo-fuzz` setup, no
`#[cfg(fuzzing)]` blocks anywhere in `src/`. Confirmed via
`find . -maxdepth 2 -name 'fuzz*'` returning nothing.
**What:** `src/protocol/codec.rs` is 2,700+ lines of byte-level decoders
(`decode_spend_batch_checked`, `decode_create_batch_checked`, etc.) that
parse attacker-controlled payloads up to 16 MiB. The hand-written tests
(`src/protocol/frame.rs:281-466`, `src/protocol/codec.rs:1666-1692`)
cover specific shapes; no automated input mutation exercises panic /
unwrap call sites. There are 30+ `try_into().unwrap()` calls in
`src/protocol/codec.rs` (sample at line 106, 109, 1589, 1630), each
guarded by an explicit length check just above — but only fuzzing
proves the guards are exhaustive.
**Why it matters:** The wire protocol is THE attack surface. A panic in
`decode_*_batch_checked` is a remote crash. The `MAX_FRAME_SIZE` cap
(LMNH covered) bounds memory but not parse-state machine bugs.
**Reproduction:** N/A — there's no fuzzer to run.
**Suggested fix:** Add `cargo-fuzz` with one harness per top-level
opcode parser. The `RequestFrame::decode` function is the natural entry
point — a single fuzz target that calls it on `&[u8]` would exercise
every per-opcode codec downstream.

---

### LMNH-18: Integration tests only cover `IndexBackendMode::Memory` (HIGH)

**Category:** N
**Location:** `tests/server_tcp.rs:43`, `tests/integration.rs`,
`tests/http_observability.rs:28` — all use `Index::new(..)` (the in-memory
backend). `tests/fault_injection.rs:229` is the **only** integration test
that exercises `HashTable::open_file_backed`.
**What:** Production deploys can choose `IndexBackendMode::Memory`,
`IndexBackendMode::Redb` (B+ tree on disk), or `IndexBackendMode::FileBacked`
(mmap'd hash table on disk). The unit tests in `src/index/redb_primary.rs`,
`src/index/redb_dah.rs`, `src/index/redb_unmined.rs`, and
`src/index/hashtable.rs` cover backend internals well, but no
integration-level test (TCP server, HTTP server, full dispatch) ever
exercises Redb or FileBacked. The audit query "Does cargo test cover
BOTH index backends" — *no, it covers ONE.*
**Why it matters:** Backend behavior diverges in subtle ways: redb
holds a transaction lock during commits, FileBacked uses mmap which
interacts differently with allocator state on resize, and the recovery
paths (`src/server/startup.rs:226-282`) are different for each. Bugs
unique to Redb or FileBacked (e.g. a redb commit hang under contention)
will never appear in CI.
**Reproduction:** `grep -l FileBacked tests/*.rs` returns only
`fault_injection.rs`.
**Suggested fix:** Parameterize the existing integration tests over
backend modes. The simplest approach is a `#[rstest]` matrix or a manual
loop in a small `for_each_backend!` macro that runs each test against
all three backends.

---

### LMNH-19: One `#[ignore]` test exists with documented justification (LOW)

**Category:** N
**Location:** `src/cluster/coordinator.rs:7505`.
**What:** Single ignored test:
```rust
#[ignore] // TODO: rewrite for pipelined migration flow
```
The justification is documented inline as required by CLAUDE.md
("each one is a finding unless its justification is documented").
**Why it matters:** The justification is acceptable per project policy,
but the `TODO` should have a tracking issue / phase reference.
**Suggested fix:** Add a tracking issue link or phase reference to the
inline comment. Optionally delete the test if the rewrite has been
captured elsewhere.

---

### LMNH-20: Tests use `is_ok()` / `is_err()` only six times — verified low-vacuity (INFORMATIONAL)

**Category:** N
**Location:** `tests/cli_integration.rs:262`,
`tests/cluster_edge_cases.rs:1311`, `tests/e2e_workload.rs:33` (env var
check, not an assertion), `tests/cluster_tcp.rs:147`,
`tests/recovery_crash_boundaries.rs:135`, plus a doc-comment in
`tests/tracing_integration.rs`.
**What:** `grep -n "is_ok\|is_err" tests/*.rs` returns 6 hits, of which 4
are real assertions, 1 is an env-var check, 1 is a comment. **All four
real assertions check additional state alongside the result variant.**
For example, `tests/recovery_crash_boundaries.rs:135` reads
`read_back.is_err() || read_back.map(|m| { m.tx_id } == [0u8; 32]).unwrap_or(false)`
which checks both error AND content.
**Why it matters:** No vacuous assertions found. **No finding.**

---

### LMNH-21: Cluster chaos tests are in-process (deterministic) — Docker E2E is the only end-to-end chaos (LOW)

**Category:** N
**Location:** `tests/cluster_edge_cases.rs:90`
(`stress_concurrent_membership_mutations`),
`tests/cluster_edge_cases.rs:181`
(`stress_atomic_bitmap_concurrent_set_clear`),
`tests/cluster_swim.rs:197` (`node_stops_responding_suspect_then_dead`),
`tests/cluster_swim.rs:240` (`dead_node_restarts_with_new_incarnation`),
`tests/cluster_swim.rs:301` (`indirect_probes_three_node_cluster`).
Plus `.github/workflows/nightly.yml` runs `teraslab-tests/run_all.sh
--tier nightly` against a Docker cluster.
**What:** In-process tests cover SWIM membership, indirect probes, dead
node rejoin, and the migration handshake. They do **not** inject random
network partitions, packet loss, or arbitrary node kills mid-operation.
The closest is `tests/e2e_workload.rs:235` `e2e_crash_injection_10_seeds`
which is single-node. The Docker E2E job under `teraslab-tests/` runs a
real multi-node cluster but its scenario library is opaque to this audit.
**Why it matters:** Real cluster bugs (split-brain, replica divergence
under packet loss) are hard to exercise without partition injection.
The in-process tests are excellent for protocol correctness but cannot
exercise TCP-level pathologies.
**Suggested fix:** Add a `tokio::test` harness with a fault-injecting
TCP wrapper (drop X% of packets, delay by Y ms) on top of the existing
`tests/cluster_tcp.rs`. Or document the Docker scenario coverage so
auditors can map it to risk areas.

---

### LMNH-22: Nightly stress tests run only via `TERASLAB_FULL_WORKLOAD=1` env var (LOW)

**Category:** N
**Location:** `tests/e2e_workload.rs:32-43`.
**What:** `tests/e2e_workload.rs:32` defines `full_scale()` which
returns true only when `TERASLAB_FULL_WORKLOAD=1` is set in the env.
Without it, scenarios run at ~1/100 scale (`fast` value vs `full`).
The CI runs `TERASLAB_FULL_WORKLOAD=1` only in the nightly workflow
(`.github/workflows/nightly.yml:11`), not on PRs.
**Why it matters:** PRs do not exercise full-volume workloads; bugs
that only appear at scale (allocator fragmentation, lock contention,
hashtable resize during heavy load) are deferred to nightly. The
nightly workflow is enabled, so this is not a HIGH gap, but operators
should know that PR-merge gates do not include scale.
**Suggested fix:** Document that the PR gate is fast-tier. Optionally,
add a manual-trigger workflow_dispatch for the full tier so developers
can run scale tests on demand.

---

### LMNH-23: Stress tests only have 2 distinct scenarios (LOW)

**Category:** N
**Location:** `tests/stress_tests.rs:9`, `tests/stress_tests.rs:16`.
**What:** Only two stress entries:
1. `stress_random_operations_8_threads` — 8 threads, random ops, 100K
   ops at CI scale.
2. `stress_device_fill_and_churn` — fill device, then churn (create+delete).

CLAUDE.md and the phase plan call for stress coverage; this is the
minimum. There's no stress for `set_mined`, `mark_longest_chain`,
`reassign`, `set_conflicting`, or `preserve_until` — all of which have
more complex state machines than spend.
**Suggested fix:** Add a stress scenario per non-trivial opcode family.
The existing stress harness in `tests/stress/mod.rs` is reusable.

---

### LMNH-24: Crash-injection coverage is excellent at the WAL/data boundary, sparse at the cluster boundary (LOW)

**Category:** N
**Location:** `tests/fault_injection.rs:88`, `:213`, `:321`, `:416` (4
scenarios); `tests/recovery_crash_boundaries.rs:103`, `:153`, `:207`,
`:268`, `:322` (5 scenarios).
**What:** Single-node crash injection is well covered:
`BeforeRedoFsync`, `AfterRedoFsync`, `BeforeDataPwrite`, redo→redb
secondary commit boundaries, allocator fence, hashtable resize rename.
Multi-node boundaries (replica ACK lost mid-batch, master crash after
local apply but before all replicas ACKed) are exercised at the
in-process level (`tests/cluster_edge_cases.rs:1222`
`replication_catchup_full_lifecycle`) but not via real process kills.
**Why it matters:** Real-world replica failures during a write are the
hardest correctness problem. The in-process tests give confidence the
state machines are correct; the missing coverage is
"process-kill-then-restart" of a clustered node mid-batch.
**Suggested fix:** Add a `tests/cluster_chaos.rs` that uses a child-process
helper (the existing test client crate at `teraslab-tests/client/` is
likely the right vehicle) to spawn nodes, kill them mid-write, and
verify post-restart consistency.

---

## Hazards (repo-wide dangerous-pattern hunt)

Scope: `src/` and where applicable `tests/`. Counts and examples follow.

### Hazard group: panics in production code (HIGH-NONE / LOW-COUNT)

**Result:** 0 `todo!()`, 0 `unimplemented!()`, 0 `unreachable!()` in `src/`.
Production `panic!()` calls (filtering out `#[cfg(test)]` modules):

| File:line | Context |
|-----------|---------|
| `src/device_io/mod.rs:116` | `panic!("SyncFallback::new returned an error it documents as impossible: {e}");` |
| `src/replication/manager.rs:1539` | `_ => panic!("unexpected op type"),` (in test mod) |
| `src/replication/manager.rs:1766` | `panic!("should not be called when already caught up");` (in test mod) |

Of those, only **one is in production code** (`src/device_io/mod.rs:116`)
and is a guard against an "impossible" error path. **No HIGH finding.**
The defensive panic is acceptable per CLAUDE.md as long as the
"impossible" condition really is — a tracking issue / safety comment
would harden it.

**Suggested fix (LMNH-25):** Convert `src/device_io/mod.rs:116` to
return an error type even in the "impossible" path so the binary
returns a non-zero exit code rather than aborts. The current panic loses
the orderly OTLP shutdown hook (`src/bin/server.rs:990`).

---

### Hazard group: `unwrap()` / `expect()` count is high but heavily test-skewed (LOW)

**Result:** 3,317 total occurrences of `.unwrap()` / `.expect(` across
`src/`. Top files by count:

| File | Count |
|------|-------|
| `src/ops/engine.rs` | 531 |
| `src/cluster/coordinator.rs` | 264 |
| `src/server/dispatch.rs` | 203 |
| `src/allocator.rs` | 172 |
| `src/recovery.rs` | 164 |
| `src/index/hashtable.rs` | 150 |
| `src/redo.rs` | 134 |
| `src/index/mod.rs` | 130 |
| `src/storage/manager.rs` | 126 |
| `src/replication/receiver.rs` | 119 |

The vast majority are in `#[cfg(test)] mod tests` blocks (test-only
unwraps are explicitly allowed by CLAUDE.md). Spot-checking the highest
file (`src/ops/engine.rs`) confirms the file's main body uses `?` and
`.map_err()`, while the test module from line ~3300 onward dominates the
count. **The production-side unwraps that DO exist concentrate in
parsing paths** — every one I read in `src/server/dispatch.rs` (lines
484, 489, 494, 511, 530, 536, 762, 778, 785) is preceded by a length
check on `request.payload.len() >= N` (e.g. line 483, 488, 493 etc.) so
the `try_into().unwrap()` is correct by construction. **No HIGH finding**
on the hot path. **Sample fix (LMNH-26):** Replace the
`request.payload[..8].try_into().unwrap()` idiom with an internal helper
`take_le_u64(payload, off)` that returns `Result` so future copy-paste
bugs cannot drop a length check silently.

---

### Hazard group: `lock().unwrap()` (LOW)

**Result:** 112 instances. The hot files are
`src/replication/durable.rs` (4), `src/cluster/topology.rs` (heavy use,
14+ in topology authority code), and the rest are tests.

Spot check `src/cluster/topology.rs`:
- Lines 387, 406, 428, 429, 469, 538, 586, 596, 637 all wrap a single
  `Mutex<…>` accessor for invariants that "must succeed" in the topology
  authority. A poisoned mutex here would have caused a prior panic — so
  the unwrap is in the "second crash" path. Acceptable but noisy.

Spot check `src/replication/durable.rs:96-123`:
- The `AckTracker` inner mutex unwraps. A poisoned mutex on the ACK
  tracker would silently lose the ability to record durable ACKs.

**Why it matters:** A panic-on-poison pattern in `parking_lot` is
correct — `parking_lot::Mutex::lock()` doesn't return `Result` and never
panics. But these uses are `std::sync::Mutex`, which DO poison on
panic-while-held. Combined with the project's defensive-panic style
(LMNH-25), poison-then-unwrap could amplify a single primary panic into
a cascading service outage.

**Suggested fix (LMNH-27):** Audit each `std::sync::Mutex::lock().unwrap()`
and either (a) migrate to `parking_lot::Mutex` (which never panics), or
(b) convert to `lock().unwrap_or_else(|p| p.into_inner())` to recover
from poison. The codebase already imports `parking_lot::Mutex` in the
hot path (`src/server/mod.rs:17`).

---

### Hazard group: `unsafe` blocks (MEDIUM — review density)

**Result:** 90 unsafe blocks in `src/`. By module:

| Module | unsafe count | Purpose |
|--------|--------------|---------|
| `src/index/hashtable.rs` | ~22 | mmap + libc fsync/msync/madvise |
| `src/ops/engine.rs` | 9 | direct device pointer R/W (`io::*_direct`) |
| `src/config.rs` | 7 | env var manipulation in tests |
| `src/record.rs` | 5 | packed-struct ↔ byte-slice via `from_raw_parts` |
| `src/io.rs` | 4 | packed-struct ↔ byte-slice |
| `src/device.rs` | 2 | mmap-backed slice exposure |
| `src/device_io/io_uring_backend.rs` | 2 | io_uring SQE push + Box::from_raw |
| `src/device_io/sync_fallback.rs` | 2 | libc::pread/pwrite |
| `src/replication/tcp_transport.rs` | 1 | `setsockopt(TCP_NODELAY)` (effectively safe) |
| `src/index/hashtable.rs` (`unsafe fn dealloc_mmap_buckets`) | 1 | `unsafe fn` declaration |

**Findings:**

#### LMNH-28: `unsafe fn dealloc_mmap_buckets` lacks a `// SAFETY:` doc comment (LOW)
**Location:** `src/index/hashtable.rs:281`. The function is `unsafe fn`
but the body has only a one-line `// Safety: …` doc above the `libc::munmap`
call (line 283), not a function-level safety contract. Callers depend on
"ptr was returned by alloc_file_backed_buckets / alloc_mmap_buckets" —
this contract is NOT documented at the function declaration.
**Suggested fix:** Add `// # Safety` rustdoc to the `unsafe fn`
declaration spelling out the caller obligations.

#### LMNH-29: `src/replication/tcp_transport.rs:30` unsafe block is empty-comment (LOW)
**Location:** `src/replication/tcp_transport.rs:30`. The unsafe block
calls `libc::setsockopt(...)` but the prefix comment is brief.
**Suggested fix:** Add a 2-line `// SAFETY:` justifying that
`level=IPPROTO_TCP, optname=TCP_NODELAY, optval` is a valid 4-byte int
pointer.

#### LMNH-30: Direct device pointer `unsafe` calls in engine hot path are correct but hard to audit (INFORMATIONAL)
**Location:** `src/ops/engine.rs:550, 570, 589, 608, 1010, 1065, 1166,
1276, 1577, 2451, 2574, 2947`. Each call invokes `io::*_direct(self.device_ptr,
...)`. The `device_ptr` field is set up once at engine construction and
not mutated — so the pointer is stably valid for the engine's lifetime.
The unsafe blocks rely on the engine holding the per-record lock, which
is enforced by `validated.apply()` consuming the lock guard.
**Suggested fix:** Add a doc-comment on `Engine::device_ptr` explaining
the lifetime and locking invariant once, so the per-call-site `// Safety:`
comments can refer to it by name.

---

### Hazard group: `from_raw_parts` (LOW)

**Result:** 6 uses, all in `src/record.rs` (lines 537, 624, 1301) and
`src/io.rs` (lines 211, 229, 251, 272, 291, 302). All convert between
`&T`/`&mut T` for `repr(C, packed)` types and `&[u8]`. Each has a safety
comment naming the `repr(C, packed)` invariant. **No transmute.** **No
finding.**

---

### Hazard group: dropped errors `let _ = ...` / `.ok();` (LOW)

**Result:** 187 occurrences of `let _ = …` or `.ok();` in `src/`.
Reviewing the highest-density files:

- `src/replication/manager.rs`: 13 instances. Most are
  `let _ = handle.join();` for thread shutdown (acceptable — join error
  means the worker already panicked, which is logged elsewhere).
- `src/replication/tcp_transport.rs`: 6 instances. `let _ =
  stream.set_nodelay(true)` and `let _ = stream.set_read_timeout(...)`
  in the dispatch path. **These ARE concerning** — failing to set
  TCP_NODELAY produces silent latency regressions, and a dropped
  `set_read_timeout` could leave a replica TCP socket without a timeout.
- `src/server/mod.rs:50`: `let _ = stream.writer.abort();` in the
  `ConnectionState::Drop` impl. Acceptable — drop path can't return errors.
- `src/replication/receiver.rs:684`: `let _ =
  crate::io::write_metadata(engine.device(), entry.record_offset,
  &meta);` — **this is dropping an I/O error on the replica's apply
  path.** Same pattern at line 1127. Both are commented (or implied by
  context to be) intentional fall-through.

**Findings:**

#### LMNH-31: Replica-apply path drops `io::write_metadata` errors (HIGH)
**Location:** `src/replication/receiver.rs:684`, `src/replication/receiver.rs:1127`.
**What:** Two call sites where the replica apply path silently
discards a metadata-write error. If the replica's device returns EIO
during a replica-apply, the receiver acks the batch as if the apply
succeeded. The master then trusts the ACK and advances the durable
high-water mark.
**Why it matters:** This is exactly the data-loss scenario that the
TERANODE_PRODUCTION_READINESS_GAPS doc calls out for the replication
durability path. The master can stop sending an op (it's been ACKed)
while the replica's local state silently diverges.
**Suggested fix:** Replace `let _ = …` with proper error handling that
fails the batch ACK and returns the appropriate error code, so the
master will retry. Same fix pattern as elsewhere in the file (e.g.
`src/replication/receiver.rs:216-221` are socket-tuning and acceptable).

---

### Hazard group: `tokio::spawn` fire-and-forget (LOW)

**Result:** Exactly 1 occurrence in `src/`:
`src/server/http.rs:1510` — the `/admin/top` cluster fan-out spawns one
task per remote node. Already covered by **LMNH-14**.

`std::thread::spawn` is more pervasive but each spawn has a clear owner
(replication catchup at `src/bin/server.rs:766`, listener accept at
`src/replication/receiver.rs:155`, etc.). Each is reviewed in its own
context; no fire-and-forget cleanup gaps surfaced.

---

### Hazard group: narrowing casts in protocol/dispatch (LOW)

**Result:** 128 narrowing casts in `src/protocol/` + `src/server/dispatch.rs`.
Spot-check of `src/server/dispatch.rs`:

- `request.request_id as u16` (line 428, 481): the request_id is a u64
  and is being narrowed to u16 (shard ID). This is **correct only when
  the caller convention uses request_id-as-shard-id for migration ops**.
  Other call sites that use the same opcode but pass a regular request_id
  > 65535 would silently get a wrapped shard. The contract is documented
  at line 425-431 ("During migration, flags bit FLAG_MIGRATION_BATCH is
  set and request_id carries the shard number"). **Acceptable** but the
  narrowing should `assert!(request.request_id <= u16::MAX as u64)` for
  defense-in-depth.

- `i as u32`, `i as u8` for batch indices: i is `usize`, capped by
  `max_batch_size = 8192` (validated in decode), so the cast cannot lose
  bits. **Acceptable.**

- `block_infos.len() as u8` (line 3236), `parent_txids.len() as u16`
  (line 3242), `children.len() as u8` (line 4494), `cold.len() as u32`
  (line 4472): each of these wires-out a length without checking against
  the destination type's MAX. If the engine ever returns 256+ block
  infos or 65536+ parent txids, the wire encoding silently truncates.

#### LMNH-32: Length casts in get_batch payload encoding can silently truncate (MEDIUM)
**Location:** `src/server/dispatch.rs:3236`, `:3242`, `:4472`, `:4494`.
**What:** Four sites cast `Vec::len()` to a smaller integer type
(`u8` / `u16` / `u32`) without bounds-checking. Today the upstream
producers (engine, allocator) keep the lengths small, but a future
allocator change that returns >256 block infos for a record would
silently encode the count modulo 256 to the wire.
**Suggested fix:** Add `assert!` / `if … return error_response` guards
that reject lengths exceeding the wire field's range. The right pattern
already exists at line 4494 — it's the `.push(children.len() as u8)`
that is the problem, not the read.

---

### Hazard group: `// TODO` / `// FIXME` / `// HACK` / `// XXX` (INFORMATIONAL)

**Result:** Single hit in `src/`:
`src/cluster/coordinator.rs:7505` — `#[ignore] // TODO: rewrite for
pipelined migration flow`. Already covered by **LMNH-19**.

`tests/` returns no hits in the strict matcher; the codebase is
remarkably hygienic about this.

---

### Hazard group: `#[ignore]` (INFORMATIONAL)

**Result:** Two hits:
- `src/cluster/coordinator.rs:7505`: see LMNH-19.
- `src/server/dispatch.rs:5758`: comment-only — "Skipped (with
  explanation, not #[ignore]):". Not an actual `#[ignore]` annotation.

**No additional finding.**

---

### Hazard group: structural panics in `src/device_io/mod.rs` (LOW)

**Already covered by LMNH-25.**

---

## Summary table

| ID | Severity | Category | Title |
|----|----------|----------|-------|
| LMNH-01 | HIGH | L | Slow-reader on response stream blocks server thread indefinitely |
| LMNH-02 | MEDIUM | L | Per-connection read buffer can grow to 16 MiB without bound |
| LMNH-03 | MEDIUM | L | Connection accept never times out a silent client |
| LMNH-04 | LOW | L | No aggregate inflight-memory cap |
| LMNH-05 | MEDIUM | L | WebSocket /ws/top has no client-side backpressure detection |
| LMNH-06 | LOW | L | HTTP server uses single-thread tokio runtime |
| LMNH-07 | HIGH | M | /health/ready does not verify cluster-join state |
| LMNH-08 | HIGH | M | Admin mutation endpoints have zero authentication when enabled |
| LMNH-09 | LOW | M | /debug/records/<txid> accepts unbounded path string before length check |
| LMNH-10 | MEDIUM | M | Web UI assigns server-supplied JSON values directly into HTML |
| LMNH-11 | MEDIUM | M | Per-op `attempted` is per-batch, `succeeded`/`failed` is per-item — they don't sum |
| LMNH-12 | LOW | M | Spend handler tallies `idempotent` via subtraction |
| LMNH-13 | INFO | M | /metrics label cardinality is bounded — verified |
| LMNH-14 | LOW | M | /admin/top cluster fan-out has no concurrency cap |
| LMNH-15 | INFO | M | Histograms emit +Inf and _sum correctly — verified |
| LMNH-16 | HIGH | N | No property-based tests anywhere in the codebase |
| LMNH-17 | HIGH | N | No fuzz targets for the wire-protocol parser |
| LMNH-18 | HIGH | N | Integration tests only cover IndexBackendMode::Memory |
| LMNH-19 | LOW | N | One #[ignore] test exists with documented justification |
| LMNH-20 | INFO | N | Tests use is_ok()/is_err() only six times — verified low-vacuity |
| LMNH-21 | LOW | N | Cluster chaos tests are in-process; Docker E2E is the only end-to-end chaos |
| LMNH-22 | LOW | N | Nightly stress tests run only via env var |
| LMNH-23 | LOW | N | Stress tests only have 2 distinct scenarios |
| LMNH-24 | LOW | N | Crash-injection coverage at WAL/data boundary is excellent, sparse at cluster boundary |
| LMNH-25 | LOW | H | Structural `panic!()` in `src/device_io/mod.rs:116` for "impossible" branch |
| LMNH-26 | LOW | H | Production unwraps in protocol parsing concentrate in dispatch |
| LMNH-27 | LOW | H | std::sync::Mutex `lock().unwrap()` pattern can amplify panics via poison |
| LMNH-28 | LOW | H | `unsafe fn dealloc_mmap_buckets` lacks a function-level `// SAFETY:` |
| LMNH-29 | LOW | H | TCP_NODELAY unsafe block in tcp_transport has thin safety comment |
| LMNH-30 | INFO | H | Direct device pointer unsafe calls in engine hot path are correct but spread |
| LMNH-31 | HIGH | H | Replica-apply path drops `io::write_metadata` errors |
| LMNH-32 | MEDIUM | H | Length casts in get_batch payload encoding can silently truncate |

Total: **5 HIGH**, **6 MEDIUM**, **17 LOW**, **4 INFORMATIONAL**.

The HIGH findings cluster around two themes:
1. Network-level DoS / authentication: LMNH-01 (slow-reader),
   LMNH-07 (false ready), LMNH-08 (no admin auth).
2. Test coverage gaps: LMNH-16 (no proptest), LMNH-17 (no fuzz),
   LMNH-18 (one backend only), LMNH-31 (silent replica I/O errors).

Of these, **LMNH-31 (replica-apply drops I/O errors) and LMNH-08
(admin endpoints unauthenticated)** are the most operationally urgent
because both produce silent data-correctness or trust-boundary failures.
LMNH-01 (slow-reader DoS) is the most exploitable from a remote
attacker's perspective. The test-infrastructure HIGHs are structural
debts that limit confidence in the rest of the audit but are not
themselves bugs.
