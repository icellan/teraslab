# Spec-vs-Implementation Diff — TeraSlab

HEAD: 1e5659b | date: 2026-05-29 | scope: every concrete claim in README.md + docs/
verified against current code at HEAD. Each claim classified and cited file:line.

Legend:
- **(a)** documented-but-not-implemented
- **(b)** implemented-but-not-as-documented
- **(c)** documented + implemented but UNTESTED
- **OK** documented + implemented + tested (no action)

---

## A. DOCUMENTED-BUT-NOT-IMPLEMENTED

### A1. `teraslab-cli export-index` / `import-index` subcommands DO NOT EXIST  (a) — MATERIAL
README.md:642-652 documents a whole "Migration between backends" workflow:
```
teraslab-cli export-index --output /tmp/index-export.snap
teraslab-cli import-index --input /tmp/index-export.snap
```
Neither subcommand exists. `grep export src/bin/cli.rs` → no hits; the `Command` enum
(cli.rs:81-164) has no Export/Import variant. The portable migration format the doc relies on
DOES exist in code (`src/index/migration.rs:48` `PORTABLE_MAGIC` "TSMI", export/import fns), but
there is no CLI surface to invoke it. An operator following the README to migrate memory↔redb
hits "unrecognized subcommand". Either wire the subcommands or delete the section.

---

## B. IMPLEMENTED-BUT-NOT-AS-DOCUMENTED

### B1. README slot-size self-contradiction: 69 vs 73 bytes  (b)
README is internally inconsistent and one number is wrong vs code:
- README:562 on-disk layout diagram: `[UtxoSlot 0: 69 bytes]...`
- README:16 & 567: "Slot total size on disk 73 bytes" / "UtxoSlot (73 bytes each)".
Code (record.rs): `UTXO_SLOT_PAYLOAD_SIZE = 69` (record.rs:25), `UTXO_SLOT_SIZE = payload + 4 = 73`
(record.rs:31), with compile-time asserts `UTXO_SLOT_PAYLOAD_SIZE == 69` (record.rs:712) and
`UTXO_SLOT_SIZE == 73` (record.rs:713). The on-disk slot is **73** bytes (69 payload + 4 CRC).
README:562's "69 bytes" in the layout diagram is the payload-only number mislabeled as the slot.
Fix the diagram to 73.

### B2. Persisted cluster-state file format in DURABILITY_CONTRACT is stale  (b) — minor
DURABILITY_CONTRACT.md:337-345 documents the format as:
`[peak:8][committed_term:8][voted_term:8][member_count:4][member_ids:8N][incarnation:8]`.
Actual `PersistedTopologyState::serialize` (topology.rs:375-398) writes:
`[peak:8][committed_term:8][voted_term:8][member_count:4][member_ids:8N][incarnation:8]
[voter_count:4][voter_ids:8N][ever_seen_count:4][ever_seen_ids:8N]`.
The doc omits the trailing committed_voters and committed_voter_ever_seen (F-G8-001 split-brain
heal) sections. Functionally a superset, deserialize is back-compat (topology.rs:405-499), so not
dangerous — but the documented format would round-trip lossy if an operator hand-built it. Update
the doc. (Note: the doc's struct-field section is NOT the index snapshot — see B3.)

### B3. README cluster config sample would FAIL to start  (b) — MATERIAL for operators
README.md:182 config reference shows `cluster_secret = ""` and the README cluster examples
(lines 196-229) set `node_id > 0` / `replication_factor = 2` but never set `cluster_secret` or
`strict_auth`. Default `strict_auth = true` (config.rs:755). A clustered config copy-pasted from
the README fails `validate_*` with StrictAuthRequiresSecret (empty secret + strict_auth=true).
The README's own 3-node deployment recipe (README:196-231) is non-bootable as written. Either
show a real secret + document strict_auth, or document that clustering requires `cluster_secret`.
(Cross-referenced and consistent with sibling Surface-Inventory finding.)

### B4. CLI flags parsed but ignored (no-op)  (b) — minor
Sibling CLI inventory (cli.rs dispatch 1256-1262) confirmed `--target` (log-level), `--history`
(replication), `--tail` (redo), `--execute` (rebalance), `--cancel` (drain), `--slots`/`--raw`
(record) are accepted by clap but dropped in dispatch. Documented behavior (e.g. README CLI table
implies these do something) does not match — they are inert.

### B5. README omits wire error codes 28-35 and OP_HELLO (107)  (b)/(c) — doc coverage gap
README error table (README:340-370) stops at code 27 + 255. Code defines ERR_PAYLOAD_MALFORMED(28)
through ERR_DELETED_CHILDREN(35) (opcodes.rs:322-382) and ERR_INTERNAL(255). Opcode table
(README:275-336) omits OP_HELLO=107 (opcodes.rs:101). The codes/opcodes ARE implemented and
returned; the README is just missing rows. Wire-contract docs should be complete.

---

## C. DOCUMENTED + IMPLEMENTED + TESTED (verified OK — no action)

### C1. ack_policy semantics — OK
README:186 / config.rs:622-626 / DURABILITY_CONTRACT:53-58.
- `resolved_ack_policy` config.rs:885-898: write_all→WriteAll, write_majority→WriteMajority,
  best_effort→None, auto: RF 0|1→None, 2→WriteAll, ≥3→WriteMajority. Matches doc exactly.
- `required_replica_acks` manager.rs:76-85: WriteAll = all replica targets; WriteMajority =
  (RF/2+1) total copies minus the 1 master copy. Matches "floor(RF/2)+1 copies including master".
- Config rejects best_effort with RF>1 (config.rs:936-949), matching DURABILITY_CONTRACT:56-58.
- Tested: manager.rs threshold tests (1472-1540, 1349-1372); config.rs:1436,1451,1464 validation.

### C2. Shard masking `shard = u16_le(txid[0..2]) & 0x0FFF`, 4096 shards — OK
README:534,538. Code: `NUM_SHARDS = 4096` (shards.rs:10), `shard_for_key` returns `h & 0x0FFF`
(shards.rs:314-316). Tested: shards.rs:556 shard_for_key_deterministic, :568 distribution.

### C3. Coinbase maturity = 100 — OK
README:352. The "+100" offset is applied at CREATE time: coinbase `spending_height = block_height
+ 100` (create.rs:66, record.rs:440; hardcoded `let coinbase_maturity = 100;` spend.rs:106 — not a
named const). The SPEND-time gate is `spending_height > 0 && spending_height > current_block_height`
(engine.rs:5188-5189). Tested at the exact boundary: spend_immature_coinbase (engine.rs:5151)
spending_height=100 / height=50 → CoinbaseImmature; spend_mature_coinbase_equal (engine.rs:5166)
height==100 → Ok (boundary is inclusive-mature); spend_mature_coinbase_above (engine.rs:5176);
spend_coinbase_zero_spending_height_boundary (engine.rs:5186, height 0 = "no maturity recorded",
must NOT be immature at genesis); reassign_rejects_immature_coinbase (engine.rs:10039);
create_non_coinbase_no_maturity_check (engine.rs:9561).

### C4. io_uring is scaffolding only; sync pwrite at QD1 — OK (honest documented limitation)
README:26,37,692. device_io/* is not production-wired; recon confirms dead-code warnings there.
Honestly disclosed gap, not a divergence.

### C5. Peak cluster size persisted to disk; quorum = floor(peak/2)+1 — OK
README:546 "peak cluster size persisted so split-brain safety survives restarts."
- Persist/load: coordinator.rs persist_peak_cluster_size:5505, load_peak_cluster_size:5530;
  serialized via PersistedTopologyState (topology.rs:344, serialize:375, deserialize:405,
  peak clamped `.max(1)` on load topology.rs:473).
- Write-gate: dispatch.rs:2489 `let quorum_needed = (peak / 2) + 1;` then rejects with
  `ERR_NO_QUORUM` (dispatch.rs:2491) when alive < quorum_needed. `peak` comes from
  `peak_cluster_size()` (coordinator.rs:6420, atomic peak_size) — uses PEAK, not current, exactly
  as README claims (isolated ex-3-node node rejects writes until it sees ≥2). Alive-count math
  (coordinator.rs:6400-6411) correctly +1's self in production (R-039/EF-02 fix).
- Tested: coordinator.rs:8912 peak_cluster_size_persists_and_loads; quorum behavior at
  coordinator.rs:9225 synthetic_commit_requires_quorum_proof and the 2-of-3 survival test :9345.

### C6. WAL-first ordering (redo append+flush BEFORE engine apply) — OK  [HIGH PRIORITY, VERIFIED]
DURABILITY_CONTRACT:21-59 + history-note disclaiming the old engine-first comment.
Confirmed in the live spend path: handle_spend_batch (dispatch.rs:2777) validates under lock
(validate_spend_multi, dispatch.rs:2850), builds RedoOp::Spend with post-spend count
(dispatch.rs:2859-2868), then `write_redo_ops(...)` which calls `log.append` + `log.flush`
(dispatch.rs:1172,1191) and **returns early without applying** on redo failure
(dispatch.rs:2871-2877, comment "Persist redo entries first. If this fails, do NOT apply."),
only THEN `validated.apply(engine)` (dispatch.rs:2880). The stale engine-first comment the
history-note warns about is gone. Redo flush-failure aborts the request before any engine write.

### C7. O_DIRECT + alignment enforcement — OK
README:567 / DURABILITY_CONTRACT:47-52. DirectDevice opens with `.custom_flags(libc::O_DIRECT)`
(device.rs:268). Alignment validated: `MIN_ALIGNMENT` ≥512, power-of-two (device.rs:106-113,
InvalidAlignment device.rs:67); default device_alignment 4096 (config.rs:715). Short-write-as-
fatal (gap #4) is the pwrite_all_at contract per DURABILITY_CONTRACT:50-52.

### C8. redb primary FAIL-CLOSED, NO in-memory fallback; secondaries → INDEX_DEGRADED — OK
NOTE: the audit-prompt phrasing "redb falls back to in-memory if corrupt" is a TRAP — README:636-638
explicitly says the OPPOSITE, and the code agrees with the README:
- Primary rebuild failure is **fatal**, file **preserved** (not deleted): startup.rs:1-16 module
  doc, error variants RedbPrimaryUnavailable / FileBackedPrimaryUnavailable / MemoryPrimary-
  Unavailable (startup.rs:54-99), all "file preserved at {path}" / "investigate". No silent
  in-memory fallback for primary.
- Secondaries degrade: ERR_INDEX_DEGRADED=26 (opcodes.rs:305), README:638. Documented behavior
  matches. There is NO automatic primary fallback-to-memory anywhere — claim is correctly absent.

### C9. Index snapshot format magics TSIX / DAHI / UNMI, CRC32 per section — OK
DURABILITY_CONTRACT:201-220. Code persistence.rs: PRIMARY_MAGIC b"TSIX" (:83), DAH_MAGIC b"DAHI"
(:89), UNMINED_MAGIC b"UNMI" (:95). Atomic temp+rename + per-section CRC per the contract.
(Legacy TSIX import fallback also at migration.rs:263.)

### C10. STATUS_DEGRADED_DURABILITY (5) returned when best-effort replication misses ack — OK
README:381. Returned in dispatch.rs:3611, 3700, 3744, 3897 (post-mutation replication-degraded
paths). STATUS_DEGRADED_DURABILITY const = 5 (opcodes.rs:423).

### C11. Single-interval freeze model (`spendable_height` single u32) — OK (documented scope choice)
README:48 explicitly documents single-interval freeze as a deliberate scope decision vs svnode's
multi-interval. Slot layout uses a single `spendable_height:4` (record.rs:112). Matches doc.

---

## SUMMARY — most material divergences

1. **(a) export-index/import-index CLI is documented but does not exist** (README:642-652 vs
   cli.rs:81-164). The backend format exists (migration.rs) but no CLI entrypoint — the entire
   backend-migration runbook is dead on arrival.
2. **(b) README 3-node cluster config recipe is non-bootable** — `cluster_secret = ""` with
   default `strict_auth = true` fails config validation (README:182,196-231 vs config.rs:755,
   936-949). Operators copy-pasting the README cannot start a cluster.
3. **(b) README slot-size contradiction** — layout diagram says 69 bytes, prose says 73; code
   asserts 73 (record.rs:713). Low risk but it's the headline storage number.
4. **(b) doc coverage gaps** — wire error codes 28-35 and OP_HELLO(107) implemented but undocumented
   (opcodes.rs vs README error/opcode tables); DURABILITY_CONTRACT persisted-state format stale
   (omits committed_voters/ever_seen sections, topology.rs:375-398).
5. **(b) CLI flags parsed-but-ignored** (--target/--history/--tail/--execute/--cancel/--slots/--raw).

**No money-safety divergences found.** The load-bearing correctness claims — WAL-first ordering
(C6), O_DIRECT durability (C7), redb fail-closed primary (C8), coinbase maturity boundary (C3),
ack-policy quorum math (C1), peak-based write quorum (C5) — are all implemented AND tested exactly
as documented. The divergences are documentation/operability defects, not correctness bugs.
