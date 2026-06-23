# TeraSlab v1 — Findings Ledger

Flat, append-only. IDs are stable. Every finding cites `path:line`. Status: `open` (needs fix), `verified` (confirmed real, awaiting fix decision), `deferred` (acceptable past v1 with justification), `dropped` (raised then refuted).

Severity for the **v1 contract**: Blocker = data loss / corruption / double-spend / silent incorrectness / security hole. Major = documented feature that doesn't work, real perf regression vs target, or missing test on a correctness-critical path. Minor = polish / docs drift / cleanup.

Method: 13 parallel subsystem reviewers → structured findings → 2 adversarial skeptics per blocker/major (majority-confirm to survive). Two findings refuted by verification are recorded as `dropped`. Two findings the gate owner escalated above the reviewers' rating are marked **(escalated)** with rationale.

Baseline (this checkout, HEAD `920ac32`, version bumped to 0.7.0):
- `cargo build --release`: clean, 0 warnings.
- `cargo test --all` (clean isolated run, real cargo exit 0): **2710 passed / 0 failed / 0 ignored** across 70 suites.
- `cargo clippy --all --all-targets`: clean, 0 warnings.

---

## REMEDIATION STATUS (2026-06-23)

All findings below have been implemented on branch `release-review-v0.7.0` except the two `dropped` (refuted) items. Post-fix gate on the merged tree: **`cargo test --all` = 2720/0/0**, clippy `--all --all-targets -D warnings` clean, `cargo fmt --all --check` clean, Rust client 20+6/0, Go client build+vet+test clean. Each blocker/major has a new or strengthened test. The implementation surfaced one new issue (**REL-145**, below), also fixed. Statuses on individual findings reflect `verified` = the original defect; treat all non-dropped findings as FIXED in this branch.

---

## BLOCKERS

### REL-001 — Delete tombstone write is a non-atomic data race against lock-free readers (UB) — `verified` (escalated: reviewers rated major)
- **Category:** concurrency / correctness (UB)
- **Evidence:** `src/ops/engine.rs:1518-1525` (`write_zeroed_metadata_header` direct branch: bare `std::ptr::copy_nonoverlapping` + a lone `Release` fence, holding only the per-tx stripe lock). Contrast every other direct-ptr writer which takes the record `io_locks().write` guard + `atomic_store_from`: `src/io.rs:863` (`write_metadata_direct`), `:941` (`write_utxo_slot_direct`), `:1176` (`write_record_bytes`). Racing lock-free readers take `io_locks().read` + `atomic_load_into`: `src/io.rs:771`, `:814`, `:1073`; the GET callers (`src/ops/engine.rs:6382`, `:6446`, `:6273`) do **not** take the stripe lock.
- **Why it blocks:** A non-atomic write concurrent with the lock-free read of the same bytes is a data race = undefined behavior. The codebase itself documents (`src/io.rs:858-862`, `src/ops/engine.rs:120-130`) that CRC-alone torn-read protection is empirically insufficient on aarch64 release builds — which is exactly why every other writer uses the atomic-store path. The tombstone path is the sole writer that bypasses it. Practical blast radius is bounded (a torn header usually fails CRC → `StorageError`, or the tx_id re-check → `TxNotFound`, i.e. fails closed), so silent cross-tx data return is unlikely — but shipping known UB on the hot read path of a durability product is not v1-worthy, and "fails closed via CRC" is not a guarantee under UB semantics.
- **Fix (small):** Route the direct tombstone write through `io::io_locks().write(record_offset)` + `atomic_store_from` (or the shared `write_metadata_direct` helper) instead of bare `copy_nonoverlapping`. Add an aarch64-release / loom variant of `g2_delete_race` that asserts no torn header, not just no aliasing.

### REL-002 — Cluster peak/topology persist never fsyncs the parent directory after atomic rename — `verified` (escalated: reviewers rated major)
- **Category:** durability / correctness (split-brain safety state)
- **Evidence:** `src/cluster/coordinator.rs:7454-7470` (`persist_cluster_state`), `:7479-7522` (`persist_topology_state`), `:7578-7590` (`persist_topology_multi_node_marker`): all do `File::create(tmp) → write_all → sync_all → rename(tmp, path)` with **no** parent-dir fsync. The project already has `fsync_parent_dir` for this exact rename pattern: `src/index/util.rs:11-23` (which even documents the identical issue #13) and `src/storage/blobstore.rs:133-155`.
- **Why it blocks:** `sync_all()` makes the file *contents* durable but not the directory-entry update from `rename`. On a crash/power loss between the rename and the next directory flush (ext4 default, XFS, etc.), the on-disk `peak_cluster_size` can roll back to the previous (smaller) value. `peak` only grows and gates restart quorum; a rolled-back peak lowers the quorum threshold, letting a rebooted node self-activate and accept writes in a minority partition it should reject with `NO_QUORUM` — a split-brain double-spend window. The code's own comments (`coordinator.rs:7476-7478`, `:7503-7519`) call these persists "safety-critical for restart quorum." This is also a Rule-6 inconsistency: the storage/index layers fsync the dir, the cluster layer does not.
- **Fix (small):** After the rename in all three persist functions (and the legacy load-path writers), fsync the parent directory — promote `index/util.rs::fsync_parent_dir` to a shared util and call it post-rename. Add a crash/fsync-order test (the existing `restored_peak_blocks_minority_after_restart` only exercises in-memory `restore()`, not on-disk rename durability).

---

## MAJORS

### REL-010 — Rust client cannot bootstrap a default (strict_auth) clustered server — `verified`
- **Category:** correctness (client, cluster) / documented-feature-broken
- **Evidence:** `client/rust/src/cluster.rs:182,354` send `OP_GET_PARTITION_MAP` unsigned; `ClusterConfig`/`ClientConfig` (`cluster.rs:19-50`, `lib.rs:76-107`) have no `cluster_secret` field. Server treats the op as inter-node-auth (`src/protocol/opcodes.rs:644-647`) and rejects unsigned frames with `ERR_CLUSTER_AUTH_FAILED` when `cluster_secret` is set under `strict_auth` (default true, `src/config.rs:998-1005`, required non-empty `:188-206`). The Go client signs it (`client/go/cluster.go:90,232`, `auth.go:37`).
- **Impact:** Against a production-default secure cluster, Rust `bootstrap_from_seeds` fails every seed → `Client::new` returns `ClientError::Connection`. The documented Rust cluster mode (`client/rust/README.md:31-42`) is unusable on a default cluster. (Single-node / non-strict / no-secret clusters work — `handle_get_partition_map` ignores the payload, `dispatch.rs:9465-9491`.)
- **Fix:** Add `cluster_secret` to Rust `ClusterConfig`/`ClientConfig` and HMAC-sign the `OP_GET_PARTITION_MAP` payload (mirror `client/go/auth.go`). Until then, document the limitation.

### REL-145 — Both clients signed payload-only; the server HMACs the whole frame → no client could authenticate to a secure cluster — `verified` (discovered during REL-010 remediation)
- **Category:** correctness (client, cluster) / security / documented-feature-broken
- **Evidence:** Server strict_auth gate `src/server/mod.rs:1077-1116` calls `cluster::auth::verify_signed_body_streaming`, which delegates to `verify_frame_streaming` and HMACs the **entire frame body** `request_id || op_code || flags || payload || ts` (the gate splices the peeked `request_id||op_code` back onto the stream, `mod.rs:1083`); canonical signer is `cluster::auth::sign_frame` (`src/cluster/auth.rs:240`, signs `encoded_frame[4..]`). Before fix: Go `client/go/auth.go` `signFramePayload` and Rust `client/rust/src/cluster.rs` `sign_inter_node_payload` both HMAC'd **payload-only** and could not know the on-wire `request_id`. `OP_GET_PARTITION_MAP` is gated (`src/protocol/opcodes.rs:644-660`).
- **Why it matters:** the original REL-010 assumed the Go client signed correctly; it did not. Against a production-default `strict_auth` cluster with a `cluster_secret`, **neither** shipped client could fetch the partition map → cluster routing unusable for both clients. Not caught earlier because the Go client is never run against a real server in CI (REL-016) and the Rust client never signed at all.
- **Fix:** both clients now sign the **whole frame** after `request_id` is assigned. Rust adds `PipeConn::round_trip_signed` which calls the server's own `teraslab::cluster::auth::sign_frame` (byte-exact); Go adds `signFrame` mirroring `sign_frame` + `pipeConn.roundTripSigned`. Verified: Rust test signs a frame and round-trips it through `teraslab::cluster::auth::verify_frame`; Go test reconstructs the server's HMAC input and matches the tag; both include a request_id-tamper negative test. Caveat: end-to-end validation against a live `strict_auth` cluster still pending (host can't run the Docker cluster) — see GO-NOGO caveat 1.

### REL-011 — Rust `unspend_batch`/`get_spend_batch` don't shard-fan-out or follow redirects in cluster mode — `verified`
- **Category:** correctness (client, cluster) / doc contradiction
- **Evidence:** `client/rust/src/lib.rs:889-902` (`unspend_batch`) and `:1673-1695` (`get_spend_batch`) route a single `round_trip` to `items[0]`'s node; they never `group_txids` / `collect_redirect_groups` and surface `ClientError::Redirect` raw. Contrast `spend_batch` (`:655-881`) and `send_item_batch_cluster` (`:929-1062`) which fan out + retry. README claims the opposite: `client/rust/README.md:247`.
- **Impact:** Multi-shard unspend in a cluster sends misrouted items to the wrong master → per-item `ERR_REDIRECT` (`dispatch.rs:4728-4732`) surfaced as `ClientError::Partial` without retry. Unspend is the reorg-reversal path → a cross-shard reorg unspend silently fails for misrouted items.
- **Fix:** Route both ops through the shard-grouping + redirect-retry machinery used by `spend_batch`, or document them as single-node-routed.

### REL-012 — Go client does not follow per-item `ERR_REDIRECT` for batch mutations — `verified`
- **Category:** correctness (client, cluster) / doc contradiction
- **Evidence:** `client/go/client.go:227-287` (`followRedirects`) acts only on whole-batch `resp.Status==StatusRedirect(3)`. Batch mutations return per-item `ERR_REDIRECT(14)` inside `STATUS_PARTIAL_ERROR` (`src/server/dispatch.rs:4728-4732`, `:5541-5542`, `:5059-5060`). `ErrCodeRedirect` is not in `isRetryableErrorCode` (`client/go/opcodes.go:145-152`), so the merge loop (`client.go:526-531`, `:695-707`) returns it as `PartialError` without re-routing. README claims the missing behavior: `client/go/README.md:557-558`.
- **Impact:** During a shard rebalance, batch mutations to a moved shard surface to the caller as partial errors instead of being transparently re-sent to the new owner. The two clients diverge (Rust implements it: `lib.rs:1098-1109,1183-1262`).
- **Fix:** Detect per-item `ErrCodeRedirect`, refresh the partition map, re-send redirected items (port the Rust logic) — or correct the README.

### REL-013 — Go redirect tests assert a wire shape the server never emits for batch ops (false confidence) — `verified`
- **Category:** test-gap / weak test
- **Evidence:** `client/go/redirect_test.go:158-160` mock returns whole-batch `StatusRedirect` for every non-partition-map op; `TestUnspendBatchFollowsRedirectInCluster` (`:201-239`), `TestGetSpendBatchFollowsRedirectInCluster` (`:241+`), `TestDeleteBatchFollowsRedirectInCluster` (`:280+`) rely on it. The real server emits per-item `ERR_REDIRECT` inside `STATUS_PARTIAL_ERROR` for batch mutations.
- **Impact:** These green tests create the impression cluster redirects are handled for spend/unspend/get_spend/delete; they cannot catch REL-012 because they never produce the real wire shape.
- **Fix:** Add tests where the mock returns `STATUS_PARTIAL_ERROR` with per-item `ERR_REDIRECT` and assert the client refreshes routing + re-sends only the redirected items.

### REL-014 — Both shipped clients send a 4-byte `ProcessExpiredPreservations` → server skips the expiry phase entirely — `verified` (gate-owner finding; missed by the workflow, independently confirmed)
- **Category:** correctness / documented-feature-broken
- **Evidence:** Go `client/go/codec.go:331-333` (`encodeProcessExpired` appends only `currentHeight`); Rust `client/rust/src/lib.rs:1751` (`current_height.to_le_bytes().to_vec()` = 4 bytes). Server `src/server/dispatch.rs:8640-8677`: a payload `< 8` bytes is the legacy form → `block_height_retention = 0` → the expired-preservation processing phase (`:8655-8677`) is **skipped**; only the DAH sweep runs.
- **Impact:** Through either shipped client, `ProcessExpiredPreservations` never actually expires preservations. The server doc warns (`dispatch.rs:8608-8612`): "Without this the preserved set grows monotonically and is never reclaimed." This is over-retention (disk/index growth + a no-op pruner op), not UTXO incorrectness or premature deletion — hence major, not blocker. (Prior 2026-06-22 review rated this a blocker; downgraded here because the failure direction is safe-but-wasteful.)
- **Fix:** Have both clients send the 8-byte form `[current_height:4][block_height_retention:4]` (the value Teranode already passes per-request on the hot path), or change the server default so the 4-byte form uses the configured retention rather than 0.

### REL-015 — Cluster crash-recovery and split-brain e2e tests run **weekly only** — never a PR/nightly/release gate — `verified`
- **Category:** test-gap (process) on the cardinal v1 contract
- **Evidence:** `teraslab-tests/run_all.sh:25-28` (`pr=(01 02 03)`, release tier holds 14/15/16/17); `.github/workflows/ci.yml:204` runs only PR-tier 01/02/03; `.github/workflows/weekly.yml:41-43` runs the release tier with `scenario_15_crash_recovery_correctness` and `scenario_14_split_brain_prevention`.
- **Impact:** The authoritative cluster proof of "SIGKILL mid-write → no acknowledged write lost; spendMulti all-or-nothing; no split-brain double-spend" can break and merge green; the regression surfaces up to 7 days later in a job nobody blocks on. For a product whose value proposition is the no-double-spend durability contract, that contract is not a merge/release gate.
- **Fix:** Promote scenario_15 + scenario_14 (or a time-boxed subset) to nightly, a fast single-iteration crash variant to PR, and gate `release.yml` on the e2e result so a broken cluster-durability contract cannot ship.

### REL-016 — Go client is never exercised against a real server in CI — `verified`
- **Category:** test-gap (client wire compatibility)
- **Evidence:** `client/go/integration_test.go:1` (`//go:build integration`); `.github/workflows/ci.yml:175` runs `go test ./...` with **no** `-tags integration` and no server. (The Rust client *is* exercised against real in-process nodes: `client/rust/src/lib.rs:2386-2453`.)
- **Impact:** The shipped Go client's wire round-trip against an actual server is unverified in CI; a Go-side framing/wire regression ships untested. This is the client Teranode integrates with.
- **Fix:** Add a CI job that starts the server (the Docker image exists) and runs `go test -tags integration ./...` against it — even a single-node create/get/spend smoke run closes the gap.

### REL-017 — Snapshot + portable export/import round-trip tests never verify cached-field fidelity — `verified` (tie verdict: 1 confirm / 1 refute, kept)
- **Category:** test-gap on the most fidelity-critical index path
- **Evidence:** `src/index/migration.rs:744` (`make_entry` sets every field but `utxo_count` to 0), `:796/:840/:962/:1039` (round-trip asserts check only `record_offset`); `src/index/mod.rs:1144` (same), `:1176` (`snapshot_restore_1000` asserts only `record_offset`). The encoder/decoder read/write all 9 `TxIndexEntry` fields at hand-coded offsets (`migration.rs:669-707`, `mod.rs:795-927`).
- **Impact:** The fields that drive pruning (`dah_or_preserve`), mining decisions (`unmined_since`), spend accounting (`spent_utxos`), and conflict logic (`generation`,`tx_flags`) are always 0 in these tests; a swapped offset / truncated width / zeroed field on encode would silently corrupt index state on every snapshot/restore and backend migration, and the suite would still pass. Encode/decode are currently symmetric (verified by reading) → latent-risk test gap, not a live bug.
- **Fix:** Populate every field with distinct non-zero values and assert full `TxIndexEntry` equality after restore, for at least one snapshot test and one export/import round-trip per backend (memory, redb, redb↔memory). `TxIndexEntry` derives `PartialEq`.

### REL-018 — On-by-default deletion-tombstone config subsystem is absent from the README "Full configuration reference" — `verified`
- **Category:** docs / hidden behavior on a durability-critical path
- **Evidence:** `grep -i tombstone README.md` → nothing; `src/config.rs:491-593` declares 9 operator-facing keys (incl. `tombstones_enabled` default **true**, `config.rs:957`), `redb_tombstone_path` (`:412`); `src/bin/server.rs:1032-1121` opens a `.tombstone` device file + redb index and runs an R1/R2 self-purge recovery pass.
- **Impact:** A correctness-critical, on-by-default behavior with on-disk artifacts an operator must provision/size is undocumented in a section titled "Full configuration reference."
- **Fix:** Add a tombstone/deletion section documenting all 9 keys + defaults, and which are soak-gated/off-by-default (gc, reconciliation).

### REL-019 — `docs/DEPLOYMENT_ASSUMPTIONS.md` states `strict_auth` defaults to false; code defaults it true — `verified`
- **Category:** docs (security model inverted)
- **Evidence:** `docs/DEPLOYMENT_ASSUMPTIONS.md:60-65` ("default (strict_auth = false)…") vs `src/config.rs:1007` (default true). README correctly says true (`:154,236`).
- **Impact:** The security doc operators read describes the opposite of shipped behavior: it says a clustered config without `cluster_secret` starts with a warning; in reality it refuses to start (`ConfigError::StrictAuthRequiresSecret`). Code behavior is the safe one; the doc is misleading.
- **Fix:** Update the doc: `strict_auth` defaults true; clustered configs without a secret refuse to start; `strict_auth=false` is the demo opt-out.

---

## MINORS

(Real but polish/cleanup/docs-drift; do not block v1. Grouped; each cites evidence.)

- **REL-100** (concurrency) `recover()` computes `count*16` before bounds-checking `count` → debug/test panic, release wraps. `src/allocator.rs:1448` before guard `:1449`; `count` read unvalidated `:1429-1433`; same at `:1464`. Release fails closed (CRC mismatch) but a panic on disk-controlled input in a recovery path is a fail-open robustness defect. *(Reviewers raised as major; both skeptics downgraded to minor — release fails closed.)* Fix: move the bound + use `checked_mul/checked_add` before any arithmetic.
- **REL-101** (docs) README/comments claim a targeted "41-byte in-place spend write"; production rewrites the full 73-byte slot + 320-byte header. `README.md:11,15,651`; `src/io.rs:947-948,872-874`.
- **REL-102** (cleanup) Targeted-footer direct-write io helpers are `pub` but called only by their own unit tests. `src/io.rs:626,348,648,363,669`.
- **REL-103** (docs) `redo.rs` module doc describes a `mark_checkpoint`+`reset`/`RedoOp::Checkpoint` flow production never uses (real checkpoint at `src/checkpoint.rs:432-448` uses `mark_recovery_progress`+`compact_prefix_through`). `src/redo.rs:6-15,2256-2262`.
- **REL-104** (cleanup) `#[allow(clippy::too_many_arguments)]` without justification comments: `src/recovery.rs:822,1680,1786,1920,2516`; `src/replication/durable.rs:797,878`.
- **REL-105** (test-gap) Issue-#14 orphan-rollback test proves absence-of-orphan via in-memory `next_offset()` only, not a crash+recovery cycle. `src/server/dispatch.rs:20139-20169`.
- **REL-106** (concurrency) Single-slot `read_utxo_slot` lacks the `io_locks().read` guard that `read_all_utxo_slots` holds. `src/io.rs:1020-1037` vs `:1073`; live callers `engine.rs:1568`, `dispatch.rs:3623,3682,12647`. *(Related to REL-001's locking discipline.)*
- **REL-107** (test-gap) No `recover()` test for valid-magic/valid-version header with out-of-range `count`. `src/allocator.rs:2068-2136`.
- **REL-108** (test-gap) `stress_random_operations` "8 threads" partitions txids per thread → no same-stripe/same-txid interleaving exercised. `tests/stress/mod.rs:122-156`.
- **REL-109** (cluster) `ClusterSecretRequired` error variant defined but never constructed; actual enforcement is `StrictAuthRequiresSecret`. `src/config.rs:130-146` vs `:1511-1513`.
- **REL-110** (cluster) Dead helper `persist_peak_cluster_size` writes `epoch=0`, gated behind `#[allow(dead_code)]`. `src/cluster/coordinator.rs:7640-7644`.
- **REL-111** (test-gap) No test exercises on-disk durability of persisted peak/committed-term across a real crash boundary. `tests/g8_split_brain.rs:361-364` (in-memory `restore()` only). *(This is the test that would catch REL-002.)*
- **REL-112** (replication) Catch-up path uses a hardcoded 5s ACK timeout instead of `replication_timeout_ms`. `src/bin/server.rs:223-231` vs `dispatch.rs:2719`.
- **REL-113** (replication) Dropped resync request on catch-up `RedoReclaimed` is only logged, no confirmed re-trigger. `src/bin/server.rs:244-257`.
- **REL-114** (docs) `blobstore.rs` module doc claims a ">1 MiB" external-blob threshold that exists nowhere; contradicts the 8 KiB advisory. `src/storage/blobstore.rs:3` vs `storage/mod.rs:5-7`, `tiers.rs:28`, `README.md:656`.
- **REL-115** (cleanup) `input_refs` module fully implemented, zero production callers; its doc implies a live spend-validation feature that doesn't exist. `src/storage/input_refs.rs:1-4`; real path is `engine.parent_txids_for_child`→`read_cold_data` (`engine.rs:3709-3711`).
- **REL-116** (test-gap) No test for the `STREAM_END` declared-size-mismatch branch (`bytes_received != total_size` → `ERR_STREAM_INVARIANT`). `src/server/dispatch.rs:9152-9162`.
- **REL-117** (weak test) Byte-cap unit test asserts a message substring + a stale "ERR_INTERNAL" comment instead of the real wire code `ERR_STREAM_INVARIANT(34)`. `src/server/dispatch.rs:11293-11320` (path returns 34 at `:9080`).
- **REL-118** (cleanup) Duplicate tier-classification test (`tier_separate` and `tier_external` assert the same boundary; `_separate` is a vestige of the removed middle tier). `src/storage/tiers.rs:183-193`.
- **REL-119** (protocol) Sub-minimum frames (total_length 1–11) silently disconnect instead of returning `PAYLOAD_MALFORMED(28)`. `src/server/mod.rs:1112`, `src/protocol/frame.rs:196-201`.
- **REL-120** (docs) README opcode table omits four wire-active opcodes the dispatcher handles: 13, 33, 108, 244. `src/server/dispatch.rs:828,873,896,1932` vs `README.md:341-403`.
- **REL-121** (docs) Error codes 36 (`NOT_DUE`) and 37 (`MIGRATION_TARGET_NOT_READY`) are emitted on the wire but absent from the README error-code table. `src/protocol/opcodes.rs:434-447`, `tests/error_code_conformance.rs:716-733` vs `README.md:407-445`.
- **REL-122** (test-gap) No test for `remove()` of an entry whose probe distance was capped at `MAX_STORED_PROBE(254)`. `src/index/hashtable.rs:1182-1186`.
- **REL-123** (index) Snapshot restore re-sizes the table ~2x larger than the snapshot's saved capacity (passes saved bucket-count as `expected_records`, then /0.7 → next pow2). `src/index/mod.rs:904` vs `:299-304`.
- **REL-124** (docs) CLI `--admin-token` doc + README claim read-only `/admin/*`, `/debug/freelist`, `GET /debug/log-level` work without a token, but the router gates them; `cli storage` (→`/debug/freelist`) is omitted from the token-required list. `src/bin/cli.rs:70-77,455`, `README.md:760-764` vs `src/server/http.rs:392-438`. *(Reviewers raised as major; skeptics downgraded to minor — docs-only.)*
- **REL-125** (docs) README `[index]` block omits `redb_tombstone_path` and `file_backed_path`; inline comment omits the `file_backed` backend. `src/config.rs:412,420,355` vs `README.md:176-181`.
- **REL-126** (docs) Read-but-undocumented operational config keys: `topology_debounce_ms`, `max_active_streams_per_connection`, `stream_idle_timeout_secs`, `replication_timeout_during_migration_ms`, `recovery_missing_primary_tolerance`. `src/config.rs:776,650,670,873,921` vs `README.md:241-247`.
- **REL-127** (docs/security) README does not document the unauthenticated fail-open cluster path that `strict_auth=false` enables. `src/server/mod.rs:952-997`, `src/config.rs:998-1007`.
- **REL-128** (security) Per-IP connection-cap exemption for cluster peers rests on a stale "authenticated by cluster_secret" rationale under fail-open. `src/server/mod.rs:578-591`, `coordinator.rs:8734`.
- **REL-129** (robustness) `TxMetadata::from_bytes` guards its length precondition with `debug_assert` only. `src/record.rs:692-702`.
- **REL-130** (weak test) Cluster e2e verifier tracks spent COUNT but not per-slot spending_data → a cluster double-spend that rewrites spender identity would not be caught at the e2e layer. `teraslab-tests/client/src/verifier.rs:195-203`, `.../tests/common/mod.rs:1399-1410`.
- **REL-131** (weak test) `http_observability` metric tests assert only that counter NAMES appear in `/metrics`, not that ops increment them (server scraped without issuing any ops). `tests/http_observability.rs:190-222`.
- **REL-132** (weak test) `block_device_size` sandbox path can pass with zero assertions when a loop device can't attach. `tests/block_device_size.rs:18-25,51-55` (real assertion only in CI `ci.yml:136`).
- **REL-133** (docs) Rust client README examples don't match the API (won't compile): `e.error_code` vs `code`, `upload_blob` arity, by-value vs `&[..]`. `client/rust/README.md:228,242,86,101,124` vs `types.rs:204-211`, `lib.rs:1282,1336,593,912`.
- **REL-134** (client) Rust pool health check doesn't actively probe (half-open TCP stays "alive"); Go pool pings. `client/rust/src/pool.rs:208-227` vs `client/go/pool.go:164-177`.
- **REL-135** (client) Rust `round_trip` hard-codes a 30s timeout with no caller cancellation/context. `client/rust/src/conn.rs:90-138`.
- **REL-136** (docs) Several docs still describe io_uring as a live/recommended backend after its removal. `docs/observability.md:69,198`, `docs/PERFORMANCE_REPORT.md:50`, `docs/HARDWARE_RECOMMENDATIONS.md:9,40`, `specs/BSV_UTXO_STORE_RUST_CRATES.md:11,16,33,181,237` vs `README.md:26`.
- **REL-137** (cleanup) `io-uring` crate still a declared Linux dependency despite README claiming the path was removed. `Cargo.toml:120`; only src reference is the deletion comment `src/lib.rs:10`.
- **REL-138** (docs) `COMPARISON_REPORT.md` advertises a "separate NVMe" tier intentionally not built. `docs/COMPARISON_REPORT.md:14` vs `README.md:30,656`, `phases/11_tiered_storage.md:3`.
- **REL-139** (docs) `DURABILITY_CONTRACT.md` references a deleted file `docs/TERANODE_PRODUCTION_READINESS_GAPS.md`. `docs/DURABILITY_CONTRACT.md:15,106`.
- **REL-140** (docs) `observability.md` log-level curl examples use port 9090 (default is 9100) and omit the now-required auth header. `docs/observability.md:132,135`.
- **REL-141** (docs) `observability.md` / README point to `src/metrics.rs` / `src/observability` for metric names actually defined in `src/server/http.rs:587-855`. `README.md:7,488-489`.
- **REL-142** (docs) Stale slot/metadata/bucket sizes in `COMPARISON_REPORT.md`, `PERFORMANCE_REPORT.md`, `HARDWARE_RECOMMENDATIONS.md` predate the per-slot CRC + 64-byte bucket (say 72B bucket / 37B+256B spend / 27B entry; code asserts 64B bucket, 73B slot, 320B metadata, 31B entry). `docs/COMPARISON_REPORT.md:12,25,31-34`, `PERFORMANCE_REPORT.md:44-46,60`, `HARDWARE_RECOMMENDATIONS.md:13-15` vs `src/record.rs:910,916`, `index/hashtable.rs:170-171`, `index/mod.rs:77`.
- **REL-143** (docs) README Status table claims `2234` tests passed; clean isolated run measures **2710** (0 failed / 0 ignored). `README.md:34`. *(0-failed/0-ignored holds; the headline count is stale.)*
- **REL-144** (docs) `TUNING_GUIDE.md` lists wrong defaults: `migration_pool_size` 4 (real 128), `migration_batch_size` 100 (real 500), listen addrs `0.0.0.0` (real `127.0.0.1` loopback). `docs/TUNING_GUIDE.md:41-42,48-49` vs `src/config.rs:1013-1014,947,977`.

---

## DROPPED (raised then refuted by adversarial verification)

- **REL-200** Config entropy/sizing validators (`AdminTokenTooShort`, `ClusterSecretTooShort`, malformed `cluster_id`, `validate_sizes`) "have no negative-path test." Both skeptics refuted (coverage exists / not a defect). `src/config.rs:1519-1527,1561-1571,1086-1109,1590-1678`. Re-confirm during remediation if touching config validation.
- **REL-201** Go `classifyRetry` (no-double-spend retry gate) "has no direct test." Both skeptics refuted. `client/go/retry.go:32-60`. Low-risk; a table test is still worth adding (cheap) but not a finding.
