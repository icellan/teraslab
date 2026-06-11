# Spec-vs-Implementation Diff — README.md / specs/BSV_UTXO_STORE_SPEC.md / phases/*.md

Audit date: 2026-06-11. Method: line-by-line read of README.md (729 lines), spec headline
sections, and all 14 phase Status headers; every testable claim traced to source with `rg`/Read
only (no cargo runs). Counts at the bottom.

---

## List 1 — Documented but NOT implemented, or implemented differently

### [HIGH] README's own config examples refuse to start (0.0.0.0 bind without `enable_remote_bind`)
- **Location:** README.md:103, 113-114, 131-132, 196-228 (quick start "Minimal single-node config",
  "Low-RAM deployment", "Full configuration reference", and all three "Cluster deployment" node configs)
  vs `src/config.rs:1143-1162` (`RemoteBindRefused`) and `src/bin/server.rs:193`.
- **What's wrong:** Every TOML example in the README sets `listen_addr = "0.0.0.0:3300"` but never sets
  `enable_remote_bind = true`. `validate_safe_defaults` rejects any non-loopback bind unless
  `enable_remote_bind = true`, and the server refuses to start (`ConfigError::RemoteBindRefused`).
  The knob `enable_remote_bind` does not appear anywhere in README. The project's own docker test
  configs know this (`teraslab-tests/docker/config/ts99-node1.toml:25` sets it).
- **Why it matters:** An operator copy-pasting the documented cluster deployment gets a fatal startup
  error with no README guidance. Worse, the README's "Quick start" claims defaults of
  `0.0.0.0:3300` / `0.0.0.0:9100` (README.md:86-87) — actual defaults are `127.0.0.1:3300` /
  `127.0.0.1:9100` (`src/config.rs:740,755`), so the no-config quick start is loopback-only and a
  remote Teranode client cannot connect.
- **Reproduction:** Save README's "Node 1" TOML; `teraslab-server --config node1.toml` → exits with
  `listen_addr ... bound to non-loopback address ... but enable_remote_bind = false`.
- **Suggested fix:** Add `enable_remote_bind = true` (with its security caveat) to every non-loopback
  example; correct the quick-start default addresses to 127.0.0.1; document the knob in the full
  configuration reference.

### [HIGH] Documented HTTP admin/debug endpoints and most CLI commands are 404/unauthorized by default
- **Location:** README.md:417-482 ("Debug endpoints", "Admin endpoints", "WebSocket", "Web UI") and
  README.md:662-690 ("Admin CLI") vs `src/server/http.rs:318-435` and `src/config.rs:757-758`.
- **What's wrong:** All `/debug/*`, `/admin/*`, and `/ws/top` routes live on a gated sub-router that is
  only mounted when `enable_admin_endpoints = true` AND a non-empty `admin_token` is configured;
  every request must carry `Authorization: Bearer <token>` (`require_admin_bearer`,
  http.rs:428-431). Both knobs default to off/unset (`config.rs:757-758`) and neither appears in
  README. With defaults, every documented `curl http://localhost:9100/debug/...` /
  `/admin/...` / `wscat .../ws/top` example returns 404. Consequently 13 of the 18 documented
  `teraslab-cli` commands (nodes, shards, memory, records, record, index, replication, redo,
  rebalance, drain, log-level, top, bench in part) hit gated endpoints and fail; the CLI's
  `--admin-token` flag (cli.rs:70-79) is undocumented in README.
- **Why it matters:** Operators relying on the documented observability/drain/quiesce surface for
  production runbooks (graceful shutdown via `/admin/quiesce`, drain via `/admin/drain/{id}`) will
  find them missing exactly when needed, with no README pointer to the gate.
- **Reproduction:** `./teraslab-server` (defaults) then `curl -X PUT localhost:9100/admin/quiesce` → 404.
- **Suggested fix:** Document `enable_admin_endpoints`, `admin_token` / `TERASLAB_ADMIN_TOKEN`, and the
  CLI `--admin-token` flag; mark each gated endpoint in the endpoint tables. Only `/metrics`,
  `/health/live`, `/health/ready`, `/status`, `/ui/*` are public (http.rs:327-335).

### [HIGH] TxMetadata documented as 256 bytes; actual on-disk header is 320 bytes
- **Location:** README.md:15 ("256-byte metadata update"), :17, :570 (`[TxMetadata: 256 bytes]`), :573,
  :575 vs `src/record.rs:716-720` (`const _: () = assert!(METADATA_SIZE == 320);` — "grew from 256
  to accommodate the trailing crc32, task C7").
- **What's wrong:** The on-disk layout claim `[TxMetadata: 256 bytes][UtxoSlot 0: 73 bytes]...` is wrong:
  the header is 320 bytes (compile-time asserted). Slot size 73 is correct.
- **Why it matters:** This is the README's authoritative on-disk format description. Capacity planning
  (records per `device_size`), external tooling parsing the layout, and the per-spend write-amplification
  arithmetic all derive from it; 64 bytes/record of error compounds at billions of records.
- **Reproduction:** `rg "METADATA_SIZE == " src/record.rs`.
- **Suggested fix:** Replace every 256 with 320 (and recompute the "Logical spend payload" row, which
  says "+ 256-byte metadata update").

### [MEDIUM] `replication_degraded_mode = "best_effort"` is documented as a choice but rejected at startup whenever it would matter; `STATUS_DEGRADED_DURABILITY` (5) is unreachable in any valid config
- **Location:** README.md:184 (`replication_degraded_mode = "reject" # "reject" or "best_effort"`),
  README.md:389 (status 5) vs `src/config.rs:965-983` (`validate_cluster_safety` rejects
  `best_effort` for both `replication_degraded_mode` and `ack_policy` when `replication_factor > 1`),
  called at `src/bin/server.rs:294`; emission path `src/server/dispatch.rs:1632-1645`
  (requires best-effort mode AND ≥1 replica target with 0 ACKs).
- **What's wrong:** With RF > 1 the server refuses to start in best-effort mode
  (tests `best_effort_with_rf_2_is_rejected` / `_rf_3_` at config.rs:1513-1538); with RF ≤ 1 there are
  no replica targets, so the zero-ack-best-effort branch can never fire. The README presents the knob
  value and response status 5 as live operational semantics without noting the startup rejection.
- **Why it matters:** An operator designing an availability-over-durability deployment around the
  documented best-effort + DEGRADED_DURABILITY contract will discover at deploy time that the config
  is rejected; a client author may write dead handling code for status 5.
- **Reproduction:** Config with `replication_factor = 2, replication_degraded_mode = "best_effort"` →
  startup error "not allowed with replication_factor = 2".
- **Suggested fix:** Document the RF>1 rejection next to the knob and mark status 5 as
  "reserved / only reachable in non-validated (programmatic/test) configurations".

### [MEDIUM] Redo log documented as a "circular buffer"; implementation is explicitly linear (no wrap)
- **Location:** README.md:589 ("The redo log is a fixed-size circular buffer on a separate device file"),
  README.md:705 vs `src/redo.rs:6-13` (R-027/BC-13: "the on-disk layout is **linear**, not circular ...
  there is no in-place wrap; a full log returns `RedoError::LogFull` until the checkpoint task" resets it).
- **What's wrong:** The code's own module doc was corrected away from "circular" because it "set false
  expectations"; README still carries the old claim.
- **Why it matters:** Behavior under redo pressure differs materially: a circular log silently overwrites
  old entries; this log stalls writes with `LogFull` until checkpoint. Sizing `redo_log_size` and
  reasoning about backpressure depends on which one is true.
- **Reproduction:** `rg -n "linear" src/redo.rs`.
- **Suggested fix:** s/circular buffer/fixed-size linear log with checkpoint-and-reset/ in README.

### [MEDIUM] `src/device_io/` documented in two places; module was deleted 2026-05-28
- **Location:** README.md:26 ("The `io_uring` backend in `src/device_io/` is scaffolding only...") and
  README.md:700 (project structure: `device_io/  I/O backend scaffolding`) vs `src/lib.rs:9-16`
  ("The previously-gated `device_io` module ... was deleted on 2026-05-28 ... dead scaffolding").
  Also stale in `phases/03_spend_path.md:3` ("io_uring backend lives behind a pub(crate) boundary —
  see `src/device_io/`").
- **What's wrong:** The directory does not exist; there is no io_uring scaffolding at all. The statement
  "every device write goes through the synchronous pwrite fallback at queue-depth-1" mischaracterizes
  the design — synchronous O_DIRECT via `src/device.rs` is the only path, not a "fallback".
- **Why it matters:** Misleads contributors and reviewers about an async I/O capability that has been
  removed, and about where the production write path lives.
- **Reproduction:** `ls src/device_io` → no such directory.
- **Suggested fix:** Remove both README references and fix phases/03; CLAUDE.md already records the
  removal.

### [MEDIUM] Blobstore threshold: config comment says ">1 MiB"; actual external threshold is >8 KiB; default path wrong
- **Location:** README.md:168 (`blobstore_path = "/blobstore"  # Directory for large transaction blobs
  (>1 MiB)`) vs `src/storage/manager.rs:128-160` (Inline ≤ 8192 bytes serialized; External > 8192;
  test `tier_8192_bytes_inline` manager.rs:529) and `src/config.rs:768`
  (default `./teraslab-blobstore`, changed from `/blobstore` per F-G10-006 because `/blobstore` is
  unwritable for non-root). README.md:580 ("Stored inline if <8 KiB") is also off-by-one at the
  boundary (8192 bytes exactly is inline).
- **What's wrong:** README contradicts itself (1 MiB vs 8 KiB) and documents the abandoned root-owned
  default path.
- **Why it matters:** Operators provisioning blobstore capacity/inode budget based on ">1 MiB" will be
  off by orders of magnitude in blob count; the `/blobstore` default was changed precisely because it
  broke first creates on fresh deploys.
- **Reproduction:** `rg -n "INLINE_THRESHOLD" src/storage/`.
- **Suggested fix:** Fix the config comment to ">8 KiB serialized" and the default to `./teraslab-blobstore`.

### [MEDIUM] README phase summary contradicts the phase files' own Status headers
- **Location:** README.md:30 ("phases 1–7, 12, 13 are fully shipped, while phases 0, 8–11 are partial")
  vs `phases/08_replication.md:3`, `phases/09_clustering.md:3`, `phases/10_wire_protocol.md:3`
  (all three: "**Status:** shipped"), `phases/11_tiered_storage.md:3` and
  `phases/00_analysis_and_spec.md:3` (partial).
- **What's wrong:** Per the phase headers the partial set is {0, 11}, not {0, 8, 9, 10, 11}. One of the
  two documents is stale; per Rule 6 the phase headers (updated with the fix campaigns they cite) look
  more recent.
- **Why it matters:** "Is replication/clustering/wire-protocol production-complete?" gets two different
  answers from the project's own docs.
- **Reproduction:** `rg -m1 "Status:" phases/*.md`.
- **Suggested fix:** Align README's Status paragraph with the phase headers (or vice versa, once).

### [MEDIUM] Memory-per-record: README says 72-byte bucket; bucket is exactly 64 bytes; spec says ~16 bytes
- **Location:** README.md:20 ("72 bytes in-memory (hash table bucket)"), README.md:597
  ("approximately **72 bytes per record** ... For 100M records ~7.2 GB") vs
  `src/index/hashtable.rs:150-155` (`BUCKET_SIZE == 64`, "Bucket must be exactly 64 bytes (1 cache
  line)") vs `specs/BSV_UTXO_STORE_SPEC.md` §1.2 ("Memory per index entry ... ~16 bytes (hash table)").
- **What's wrong:** Three documents, three numbers. The implementation is 64 bytes per bucket;
  per-record RAM is 64 / load_factor (so >64, but not "72 bytes (hash table bucket)" — the bucket
  is not 72 bytes). The spec's ~16 bytes is off by 4x.
- **Why it matters:** RAM provisioning for the default backend (the README's flagship low-RAM
  comparison table at :635-639 repeats the derived numbers).
- **Reproduction:** `rg -n "BUCKET_SIZE == 64" src/index/hashtable.rs`.
- **Suggested fix:** State 64 B/bucket and an effective bytes/record at the configured max load factor;
  fix the spec table.

### [MEDIUM] Spec §10 wire protocol does not match the implemented protocol (frame header, Heartbeat opcode)
- **Location:** `specs/BSV_UTXO_STORE_SPEC.md:1464-1510` vs `src/protocol/frame.rs:4-5` and
  `src/protocol/opcodes.rs:187,198`.
- **What's wrong:** Spec frame = Magic `0x55545830` + Version u8 + Flags u8 + OpCode + RequestID +
  PayloadLength + trailing CRC32. Implementation = `[total_length:4][request_id:8][op_code:2][flags:2]
  [payload]` — no magic, no version byte, no frame CRC. Spec assigns Heartbeat = `0x00FF` (255);
  implementation has `OP_HEARTBEAT = 250` and 255 is `OP_INCREMENT_SPENT_EXTRA_RECS`. Spec also lacks
  all post-v1 opcodes (103-107, 242-243, 251-253). Spec replication-bandwidth target (~400 MB/s,
  §1.2) contradicts README's ~120 MB/s (README.md:19); spec per-spend wear "69 bytes" contradicts
  README's 41-byte spend region + sector amplification.
- **Why it matters:** The spec is labeled Draft, but anyone implementing a client from it produces
  frames the server cannot parse, and opcode 255 collides with a real (different) opcode.
- **Reproduction:** Compare spec §10.2/10.3 with frame.rs / opcodes.rs.
- **Suggested fix:** Either stamp spec §10 as superseded-by-implementation or regenerate it from
  `opcodes.rs`/`frame.rs`.

### [LOW] Go client README example does not compile (wrong type name, argument order, field name)
- **Location:** README.md:488-513 vs `client/go/client.go:308,678` and `client/go/types.go` CreateItem.
- **What's wrong:** README calls `client.SpendBatch(ctx, []SpendItem{...}, teraslab.SpendParams{...})`;
  actual signature is `SpendBatch(ctx context.Context, params SpendBatchParams, items []SpendItem)`
  (params first, type is `SpendBatchParams`, not `SpendParams`). README's `CreateItem` field
  `UTXOHashes [][32]byte` is actually `UtxoHashes []UtxoHash`. `CreateBatch` returns
  `(*BatchResult, error)`, not bare `error`.
- **Why it matters:** Copy-paste integration code fails; minor but it is the documented entry point.
- **Suggested fix:** Sync the snippet with `client/go/README.md` / actual signatures.

### [LOW] Rust client README example does not compile (`Client::connect` doesn't exist; `addr` is `Option<String>`)
- **Location:** README.md:519-536 vs `client/rust/src/lib.rs:122-134` (`Client::new(cfg)`) and
  lib.rs:80 (`pub addr: Option<String>`).
- **What's wrong:** Constructor is `Client::new`, and `addr` must be `Some("localhost:3300".to_string())`.
  The crate's own doc example (lib.rs:10-23) shows the correct form.
- **Suggested fix:** Copy the lib.rs doctest into README.

### [LOW] CLI invocation example uses a scheme-less `--addr localhost:9100`
- **Location:** README.md:667 vs `src/bin/cli.rs:55` (default `http://localhost:9100`; URLs built as
  `format!("{base_url}{path}")` for reqwest, cli.rs:200).
- **What's wrong:** Without `http://`, reqwest fails to parse the URL (relative URL without a base).
- **Suggested fix:** `--addr http://localhost:9100`.

---

## List 2 — Implemented but NOT documented (operator-relevant surprises)

### [HIGH] Safe-defaults gate: `enable_remote_bind`, `enable_admin_endpoints`, `admin_token`, `strict_auth` absent from README config reference
- **Location:** `src/config.rs:512-541, 629-641, 784` vs README.md:129-190 (full configuration reference).
- **What's wrong / surprising:** Four security-critical knobs control whether the server starts at all
  (non-loopback bind, clustered-without-secret) and whether the admin surface exists. `strict_auth`
  defaults to `true` (config.rs:784, tests config.rs:2030, tests/g10_config.rs:282) — README only
  mentions it in passing comments in the cluster examples. Note the `strict_auth` field doc comment at
  config.rs:633 still says "Default is `false`" while the `Default` impl sets `true` — an internal
  contradiction that should be cleaned up in code.
- **Why it matters:** These are the knobs an operator hits first on any real deployment.
- **Suggested fix:** Add all four to the configuration reference with defaults; fix the stale field
  doc comment.

### [MEDIUM] Undocumented config knobs (all real, all read): per-IP connection cap, stream cap, in-flight memory cap, checkpoint watermarks, blob GC, cluster identity, lag thresholds, device identity, observability
- **Location:** `src/config.rs:474-734`: `max_connections_per_ip` (default 64),
  `max_stream_total_bytes` (4 GiB; env `TERASLAB_MAX_STREAM_TOTAL_BYTES`),
  `max_inflight_request_bytes` (256 MiB → `ERR_RATE_LIMITED` when exhausted),
  `blob_gc_interval_secs` (3600), `checkpoint_high_water`/`checkpoint_low_water`/
  `checkpoint_poll_interval_ms` (0.75/0.25/1000), `cluster_state_path`, `cluster_id`,
  `replication_timeout_during_migration_ms` (30000), `replica_lag_warn_threshold_ops` (10000 —
  degrades `/health/ready`), `recovery_missing_primary_tolerance` (65536), `device_id` (startup
  refuses on mismatch), `advertise_addr`, `[observability]` + `TERASLAB_*` env overrides.
- **Why it matters:** Several have hard behavioral consequences README never hints at: a NAT'd client
  fleet hits the 64-conn/IP cap; replica lag can fail readiness probes; a wrong device is refused at
  boot if `device_id` is pinned.
- **Suggested fix:** Extend the README configuration reference (or link a generated reference).

### [MEDIUM] Third index backend `file_backed` exists; README says "two index backends"
- **Location:** `src/config.rs:322-330, 395-397` (`IndexBackendMode::FileBacked`, `file_backed_path`,
  default `teraslab-index.dat`) vs README.md:593 ("TeraSlab supports two index backends").
- **What's wrong / surprising:** `backend = "file_backed"` is accepted and runs (mmap'd persistent
  primary, in-memory secondaries, redo-based crash recovery). README only mentions it once, negatively,
  in the export/import section (:660).
- **Why it matters:** Operators should know it exists, if only to know it is not the recommended path;
  silently-accepted config values that docs claim are invalid erode trust in validation.
- **Suggested fix:** One paragraph in "Index backends" stating its status and tradeoffs.

### [MEDIUM] Admin opcodes 104/106 require cluster HMAC; error-data payloads on several error codes; wire caps
- **Location:** `src/protocol/opcodes.rs:509-524` (`is_inter_node_auth_opcode` includes
  `OP_ADMIN_DIAGNOSE_KEY`, `OP_ADMIN_CLUSTER_HEALTH`; pinned by `tests/g5_protocol_auth.rs`);
  `src/server/dispatch.rs:6566-6630` (ERR_FROZEN_UNTIL carries 4-byte `spendable_at_height`;
  ERR_INVALID_SPEND carries the 36-byte stored spending data, including for pruned slots);
  `opcodes.rs:477,539,548,555` (`MAX_FRAME_SIZE` 16 MiB, `MAX_COLD_DATA_PER_ITEM` 4 MiB,
  `MAX_UTXO_HASHES_PER_CREATE_ITEM` 131072, `MAX_PARENT_TXIDS_PER_CREATE_ITEM` 65536).
- **What's wrong / surprising:** README's opcode table presents 104/106 as plain admin ops — with a
  `cluster_secret` configured, unsigned clients get `CLUSTER_AUTH_FAILED`. README's error table
  documents error-data only for codes 3, 10, 35 but codes 6 and 13 also carry payloads. None of the
  wire-level size caps are documented.
- **Suggested fix:** Footnote the HMAC requirement in the opcode table; complete the error-data column;
  list the frame/item caps under "Wire protocol".

### [LOW] redb import sentinel: interrupted `import-index` makes the next startup refuse
- **Location:** `src/server/startup.rs:281-298` (import-in-progress sentinel detected → startup refuses
  with remediation message); `src/index/migration.rs`.
- **What's wrong / surprising:** README documents export/import (:650-660) but not that a crashed import
  leaves a sentinel that blocks server startup until cleaned (deliberately, to avoid opening
  partially-imported redb files).
- **Suggested fix:** One sentence in the migration section.

---

## List 3 — Verified claims (claim → code → test)

| # | Claim (README line) | Code | Test |
|---|---|---|---|
| 1 | Opcode table: all 30 documented opcodes exist with exactly the documented numbers (274-337) | src/protocol/opcodes.rs:6-198 | dispatch wiring src/server/dispatch.rs:462-518; tests/g5_protocol_auth.rs (pins auth set) |
| 2 | Error-code table: all 37 codes (0-35, 255) match names/numbers exactly (340-378) | src/protocol/opcodes.rs:201-399 | per-code mapping src/server/dispatch.rs:6566-6630; codec tests src/protocol/codec.rs:2128+ |
| 3 | Response status codes 0-5 match (382-389) | src/protocol/opcodes.rs:402-423 | (status 5 reachability caveat — List 1) |
| 4 | Frame format `[total_length:u32][request_id:u64][op_code:u16][flags:u16][payload]` (265) | src/protocol/frame.rs:4-48 | frame.rs:336 `request_frame_round_trip`, :369 `response_frame_round_trip` |
| 5 | `Hello`(107) → 2-byte LE protocol version; version is 2 (322) | src/server/dispatch.rs:486-490; opcodes.rs:397 | dispatch.rs:8217 `dispatch_hello_returns_protocol_version` |
| 6 | Shard formula `shard = u16_le(txid[0..2]) & 0x0FFF`, 4096 shards (546) | src/cluster/shards.rs:9-10, 323-326 | shards.rs:565 `shard_for_key_deterministic`, :577 `_distribution`, :1431 `total_mastered_shards_always_4096` |
| 7 | Round-robin shard assignment over members (546) | src/cluster/shards.rs:3 (module doc + compute) | shards.rs:1206, 1252 distribution tests |
| 8 | Quorum = majority of peak observed cluster size; isolated node rejects writes (554) | src/cluster/topology.rs:650-672 (`peak/2 + 1`) | tests/cluster_tcp.rs:1634 `isolated_node_rejects_writes_with_no_quorum`; tests/cluster_edge_cases.rs:474 |
| 9 | Peak cluster size persisted to disk, survives restart (554) | src/cluster/coordinator.rs:643-648, 796-800 `persist_cluster_state` (fsync + atomic rename); topology.rs:346,382,813 | tests/g8_split_brain.rs:362 `restored_peak_blocks_minority_after_restart`; topology.rs:1593, 2288 |
| 10 | `ack_policy` auto: RF=1→best-effort, RF=2→WriteAll, RF≥3→WriteMajority (654, README 182) | src/config.rs:912-928 `resolved_ack_policy` (wired at src/bin/server.rs:898) | WriteMajority math: src/replication/manager.rs:76-85 + manager.rs:1331-1354 tests. (No direct unit test of the auto→policy mapping itself — gap, low risk) |
| 11 | `write_majority` = floor(RF/2)+1 copies incl. master | src/replication/manager.rs:76-85 | manager.rs:1338 ("WriteMajority with RF=3: need 1 replica ACK") |
| 12 | `replication_degraded_mode = "reject"` → ERR_REPLICATION_FAILED (184, 362) | src/server/dispatch.rs:3083, 3288 | dispatch unit tests around :1809; config.rs:1513-1538 reject best_effort w/ RF>1 |
| 13 | Clustered config without `cluster_secret` rejected (strict_auth default on) (199) | src/config.rs:784 (default true), :1182-1190 | config.rs:2030 `rf_gt_one_without_cluster_secret_under_default_auth_is_rejected`; tests/g10_config.rs:282 |
| 14 | redb primary fail-closed: open → rebuild-from-scan → fatal exit preserving file (644) | src/server/startup.rs:1-16, 261-307 | startup.rs:657 `redb_primary_rebuild_failure_preserves_file`, :768 |
| 15 | Secondary redb failure → degraded start, `INDEX_DEGRADED` on dependent endpoints (646) | startup.rs:16, opcodes.rs:296-305 | tests/integration.rs, tests/cluster_tcp.rs (ERR_INDEX_DEGRADED assertions); tests/secondary_two_phase_durability.rs:60,135,197 |
| 16 | No silent delete of corrupt redb / no auto fallback to in-memory primary (646) | startup.rs:53-69 (file "preserved at {path}") | startup.rs:657 |
| 17 | UtxoSlot = 73 bytes (32 hash + 1 status + 36 spending + 4 CRC) (16, 575) | src/record.rs:25-31 | record.rs:712-713 compile-time asserts |
| 18 | Per-slot CRC32 torn-write protection; metadata CRC (575, 573) | record.rs:27-28, 494, 723 | compile asserts record.rs:709-723 |
| 19 | Slots pre-allocated full-size at creation; spend mutates in place; O_DIRECT writes sector-aligned (11, 17, 575) | src/ops/create.rs; src/ops/engine.rs:842-850 (align_up to device alignment); src/device.rs:751+ | engine/spend unit tests; benches/spend_throughput.rs |
| 20 | Block devices: kernel size used, `device_size` ignored; files: grow-only, never truncate (141-142) | src/device.rs:756-827 (`set_len` never on S_IFBLK; grow-only) | tests/block_device_size.rs (macOS RAM-disk; Linux /dev/nvme untested — README admits this at :46) |
| 21 | Defaults: device 1 GiB `teraslab-data.dat`, redo 64 MiB `<dev>.redo`, snap `teraslab-index.snap`, expected_records 100000, lock_stripes 65536, max_batch 8192, max_conn 1024, retention 288, swim 3301/200ms/5000ms, topology timeout 0→max(3×probe,500), RF 1, repl timeout 3000, pool 128, batch 500, lag check 30s, max_migration_threads 16, redb paths + 256 MiB cache (88-189, 623-629) | src/config.rs:737-799; IndexConfig default config.rs:400-411; resolved_redo_log_path config.rs:871-885; resolved_topology_propose_timeout_ms config.rs:822-828 | config.rs default/TOML tests (e.g. :1500+); g10_config.rs |
| 22 | All README-listed config knobs are actually read (no dead knobs in the documented set) | rg sweep: every knob consumed outside config.rs (e.g. allocator.rs:624 device_size; server.rs:869 topology timeout) | — (structural) |
| 23 | Public HTTP endpoints `/metrics`, `/health/live`, `/health/ready`, `/status`, `/ui/` exist (397-440, 482) | src/server/http.rs:327-335 | http.rs:2860+ /metrics shape tests, :3255 name pinning |
| 24 | Gated endpoints `/debug/index|freelist|redo|records/{txid}|log-level(GET/PUT)`, `/admin/migration_status|nodes|memory|records|replication|top|quiesce|rebalance|drain/{id}`, `/ws/top` all exist (417-477) | http.rs:386-432 | http.rs:3326 (/admin/top fields), :3348 `ws_top_push_includes_new_metrics` |
| 25 | Prometheus counters named `teraslab_<op>s_{attempted,succeeded,failed}_total` (412) | http.rs:581-741 | http.rs:3255 "/metrics output missing {name}" test |
| 26 | `/ws/top` pushes JSON snapshot every second (477) | http.rs:2352-2378 (`sleep(Duration::from_secs(1))`) | http.rs:3348 |
| 27 | Web UI embedded at `/ui/` (482) | http.rs:333-334; ui/{index.html,app.js,style.css} | — |
| 28 | All 18 CLI commands incl. export-index/import-index exist; both offline w/ redb lock note (670-689, 650-660) | src/bin/cli.rs:86-162 | tests/cli_integration.rs |
| 29 | Export format = in-memory snapshot binary, backend-agnostic; file_backed unsupported (660) | src/index/migration.rs; cli.rs:144-161 | tests/cli_integration.rs |
| 30 | Clean shutdown snapshots index; restored on startup; corrupt/missing snapshot → rebuild from device scan + redo replay (599) | src/bin/server.rs:7-11, 433-521 | tests/fault_injection.rs:416; recovery tests in src/recovery.rs |
| 31 | Crash recovery: scan to last checkpoint, idempotent replay (584-587) | src/redo.rs:1-13; src/recovery.rs; src/checkpoint.rs | tests/secondary_two_phase_durability.rs; tests/fault_injection.rs |
| 32 | Cold data inline ≤8 KiB else external blob store; middle tier intentionally absent (579-580) | src/storage/manager.rs:128-160; src/storage/tiers.rs:5 | manager.rs:529 `tier_8192_bytes_inline`; phases/11 status confirms middle-tier non-enablement |
| 33 | Streaming upload path OP_STREAM_CHUNK/END with FLAG_EXTERNAL_BLOB (304-309, 359) | dispatch.rs:517-518; opcodes.rs:171-172, 428 | dispatch stream tests (~:6308 region) |
| 34 | Migration: reads served locally on old master; pending-inbound on new master → MIGRATION_IN_PROGRESS, retryable (561) | src/server/dispatch.rs:627-774, 2737, 5479; src/cluster/migration.rs:452 | tests/migration_fence.rs; tests/cluster_tcp.rs |
| 35 | Migration completion requires manifest; mismatch → code 22, missing → code 21 (363-364) | opcodes.rs:259-270; migration verify path | migration tests in src/cluster/migration.rs (e.g. :3228) |
| 36 | Inter-node HMAC: cluster-authority opcodes require signed frames when secret set; failure → CLUSTER_AUTH_FAILED (27) (369) | opcodes.rs:307-312, 509-524; src/cluster/auth.rs | tests/g5_protocol_auth.rs |
| 37 | Benchmarks run against MemoryDevice only (24) | benches/*.rs (no DirectDevice usage found) | — (consistent with README's own caveat) |
| 38 | Docker: `teraslab-tests/docker/Dockerfile` (ENTRYPOINT teraslab-server, CMD --config /etc/teraslab/node.toml, EXPOSE 3300/3301/9100); `docker-compose.3node.yml` exists (236-258) | teraslab-tests/docker/Dockerfile:40-46; teraslab-tests/docker/docker-compose.3node.yml | teraslab-tests harness |
| 39 | Go module path `github.com/icellan/teraslab/client/go`; batch APIs exist (489) | client/go/go.mod:1; client.go:54, 308, 678 | client/go/*_test.go |
| 40 | Rust client async/Tokio with pooling + cluster routing (717) | client/rust/src/lib.rs:1-40, 122+ | client/rust tests (17 per README — count not re-run) |
| 41 | In-tree wire-decoder fuzz smoke on CI; deep cargo-fuzz manual, not nightly (47) | tests/wire_fuzz_smoke.rs; fuzz/; .github/workflows/nightly.yml (no fuzz step) | wire_fuzz_smoke.rs itself |
| 42 | License is Open BSV License Version 6 (49, 729) | LICENSE:1 | — |
| 43 | Single-interval freeze (`spendable_height` single u32) as documented design choice (42) | record.rs slot spending-data layout; ops/remaining.rs freeze path | freeze/unfreeze tests in src/ops/remaining.rs |
| 44 | Secret redaction in Debug output (operator-safety adjunct to `cluster_secret` doc) | config.rs:21-57 | config.rs Secret tests |
| 45 | Per-key diagnosis opcode 104 layout (31-byte entries, ≤64 txids) (319) | opcodes.rs:33-112 | dispatch admin-diagnose tests |

**Not verifiable in this audit (flagged, not failed):** test-count/lint probe table (README.md:32-38 —
requires cargo, which this audit was instructed not to run); 10M+ ops/sec MemoryDevice observation,
~120 MB/s replication target, p99.9 latency target (README labels all as targets/unmeasured, which is
honest); "10-50x less SSD wear" (CLAUDE.md, design claim).

---

## Counts

- Claims checked: ~140 (30 opcodes, 37 error codes, 6 status codes, ~30 config knobs + defaults,
  18 HTTP endpoints, 18 CLI commands, ~30 architecture/behavior claims, 2 client snippets,
  14 phase statuses, 6 spec headline claims)
- Verified: ~118 (List 3, rows often bundling multiple claims)
- Divergent (documented differently than implemented): 13 findings (List 1)
- Documented but no longer implemented at all: 1 (`src/device_io/`)
- Implemented but undocumented: 5 findings (List 2)
- Flagged as unverifiable without cargo/hardware: 5 claim groups
