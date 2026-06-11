# Audit Category B — Crash Recovery and Durability

Auditor scope: `src/redo.rs`, `src/recovery.rs`, `src/checkpoint.rs`, `src/device.rs`,
`src/allocator.rs`, `src/record.rs`, redo call sites in `src/ops/` and
`src/server/dispatch.rs`, plus the recovery/fault-injection test suite.
All line numbers refer to the working tree at commit `5f47b2a`.

Durability model under audit (from `src/recovery.rs:1-30` and `phases/07_crash_safety.md`):
WAL-first — (1) validate under lock, (2) append redo + fsync redo device,
(3) pwrite data device, (4) replicate. Redo log is a **separate**
`DirectDevice` file (`<device>.redo`, `src/bin/server.rs:576-599`,
`src/server/startup.rs:509-533`), so a redo fsync does **not** flush the data
device. The data device is synced only at: delete tombstone
(`src/ops/engine.rs:3980`), end of recovery replay (`src/recovery.rs:1502`),
replication receiver (`src/replication/receiver.rs:898`), and graceful
shutdown (`src/bin/server.rs:1442`). That asymmetry is the root of the two
critical findings below.

---

### [CRITICAL] Checkpoint reclaims redo entries without syncing the data device — acked mutations lost on power failure

**Location:** `src/checkpoint.rs:338-403` (`perform_checkpoint_with_reset_guard`), `src/allocator.rs:1098-1147` (`SlotAllocator::persist`), `src/ops/engine.rs:4257-4275`

**What's wrong:** The per-op durability story is "redo entry fsynced before
apply; data pwrite may be volatile because the redo entry can replay it."
That holds only while the redo entry exists. `perform_checkpoint_with_reset_guard`
does: (1) `snapshot_index` — fsyncs the **snapshot file** only (`src/index/mod.rs:307-347`);
(2) `persist_allocator` → `SlotAllocator::persist` — `pwrite_all_at(&buf, 0)`
with **no `device.sync()`** (`src/allocator.rs:1145-1146`), despite the module
doc at `src/checkpoint.rs:27-29` claiming "allocator persist is fsynced before
returning"; (3) `mark_recovery_progress` + (4) `compact_prefix_through` —
which sync the **redo device** only (`src/redo.rs:2177`). At no point is the
**data device** synced. Slot/metadata pwrites for every mutation covered by
the fence sit in the drive's volatile write cache (and, for file-backed
devices, in not-yet-journalled filesystem extent allocations — `DirectDevice`
preallocates with `set_len`, i.e. a sparse file, `src/device.rs:873-882`;
O_DIRECT writes into holes are not durable without fsync). Compaction then
destroys the only durable copy of those mutations.

**Why it matters:** Power loss any time after a checkpoint compaction can
silently revert acked spends/creates/set-mineds whose data writes were still
in the device cache: the redo entries are gone, the index snapshot has only
index state (slot spent/unspent status lives solely in the data region), and
recovery reports a clean empty replay. A reverted spend is a double-spendable
UTXO — direct money-loss class. Note the spend slot state is not recoverable
from the snapshot at all. The same window loses the allocator header write
from step (2): the old header survives, the `AllocateRegion`/`FreeRegion` redo
entries covering the delta were just compacted, and the next boot
double-allocates live regions (silent record overwrite). MemoryDevice-based
tests cannot detect this because `MemoryDevice::sync` is a no-op.

**Reproduction:** On a Linux box with a file-backed `DirectDevice`: run the
server, issue a spend (acked), force a checkpoint
(`perform_checkpoint`), then drop the page/device cache simulation — e.g. run
under a fault layer (dm-flakey / `dm-log-writes`) that discards non-flushed
writes to the data file at the instant after `compact_prefix_through`'s redo
sync; restart and `GET` the spent output: it reads UNSPENT, recovery stats
show 0 replayed. Deterministic in-tree variant: wrap the data device in a
`BlockDevice` that buffers pwrites until `sync()` (a "volatile cache" test
double), drive `perform_checkpoint`, drop the unsynced buffer, re-open, run
recovery, assert the spend is missing.

**Suggested fix:** In `perform_checkpoint_with_reset_guard`, call
`engine.device().sync()` after `persist_allocator()` and **before**
`mark_recovery_progress`/`compact_prefix_through` (this also makes the
allocator header durable). Document that redo reclamation is only legal after
a data-device barrier; fix the stale "fsynced" claim in
`src/checkpoint.rs:27-29` and `src/recovery.rs:13-14` ("durable on return for
DirectDevice via O_DIRECT" is false w.r.t. drive cache and sparse-file
extents).

---

### [CRITICAL] Corrupt/torn allocator header silently falls back to a fresh allocator — next creates overwrite live records

**Location:** `src/bin/server.rs:325-357`; `src/allocator.rs:1098-1147` (`persist`), `src/allocator.rs:1156-1261` (`recover`)

**What's wrong:** `SlotAllocator::persist` writes a multi-block header
(48 B + 16 B/freelist-entry; >4 KiB once the freelist exceeds ~253 entries)
in a single unsynced pwrite at offset 0. A power loss mid-write tears it; the
CRC check in `recover()` then returns `HeaderCorruption`. The startup branch
treats **every** recover error identically:
`Err(_) => SlotAllocator::new(device)` (`src/bin/server.rs:345-356`) — a
fresh allocator with `next_offset = DATA_REGION_OFFSET` and an empty
freelist. Redo replay of `AllocateRegion` entries only restores
post-checkpoint allocations; everything persisted before the fence is now
considered free space. The fresh-allocator path also generates a brand-new
random `device_id` and skips the configured `device_id` verification
(verification only runs on the `Ok` arm, lines 331-342).

**Why it matters:** Two catastrophic outcomes, both silent: (a) if the index
snapshot loads, the first creates after boot are allocated at
`DATA_REGION_OFFSET` and pwrite directly over existing records — silent
destruction of live UTXOs; (b) if the snapshot is absent, the device-scan
rebuild iterates the (empty) allocated set and "successfully" produces an
empty index — the entire store vanishes without an error. A torn header is
the exact artifact of power loss during the checkpoint's `persist_allocator`
step, so this is a realistic single-fault scenario, not a double fault.

**Reproduction:** Create >300 free regions (fragmented freelist), call
`persist()`, then overwrite the second 4 KiB block of the header region with
garbage (simulating a torn multi-block write — magic in block 0 intact, CRC
fails). Start the server: observe `allocator: fresh (no persisted state
found)` in the log, then issue a create and read back a previously-existing
txid's record — corrupted. Unit-level: `SlotAllocator::recover` returns
`HeaderCorruption`, then assert `SlotAllocator::new` + create overlaps a
previously allocated offset.

**Suggested fix:** Distinguish error classes in the startup match: only an
all-zero header region (genuinely fresh device) may fall through to
`SlotAllocator::new`; `HeaderCorruption` / `UnsupportedVersion` /
short-read must fail closed with an explicit "run rescan tooling" error.
Belt-and-braces: make `persist` write to an A/B double-buffered header (two
slots, sequence-numbered, pick highest valid CRC) so a torn write always
leaves one valid copy, and sync after persist (see finding 1).

---

### [HIGH] `compact_prefix_through` rewrites retained (durable, possibly acked) entries in place — a torn compaction write loses them

**Location:** `src/redo.rs:2123-2193`

**What's wrong:** Compaction serializes all post-fence entries and pwrites
them at the **start** of the entries region, overwriting the log in place,
then syncs. The retained entries were previously durable (flushed at their
original offsets) and may include acked work from non-dispatch producers
(checkpoint.rs:373-376 explicitly says such entries can exist —
`AllocateRegion`/`FreeRegion`, secondary-intent records, engine-internal
`AppendConflictingChild`). A multi-block pwrite is not atomic under power
loss: a crash mid-compaction leaves the front of the region partially
rewritten; the scan (`scan_entries_region_with_tail`, src/redo.rs:2224-2365)
stops at the first CRC-failing entry and treats everything after as
end-of-log. Durable entries with sequence > fence are gone, while their
covered state (e.g. allocator freelist mutations) may or may not have been
applied/synced.

**Why it matters:** This converts a previously-durable byte range into a
window where power loss erases it. Lost `AllocateRegion` entries reproduce
the double-allocation corruption of finding 2 (post-fence allocations vanish
from both the header — persisted pre-fence — and the log). Lost
secondary-intent or conflicting-child entries silently drop the
corresponding reconciliation at next boot.

**Reproduction:** Append 1000 entries; `compact_prefix_through(n)` retaining
~500; inject a device that persists only the first aligned block of the
compaction pwrite then errors/halts (extend the existing `ReadFailingDevice`
pattern in `src/redo.rs` tests with a partial-pwrite mode); re-open the log;
`recover()` returns only the entries that fit in the first block — the rest
of the retained, previously-durable entries are gone.

**Suggested fix:** Never overwrite the only copy: write the retained set to
a scratch area (or simply leave the log as-is and record the new logical
start offset in the fsynced header — the header already exists per F-G4-001
and is rewritten under CRC), flip the header atomically, then lazily zero.
Alternatively require retained == empty before in-place rewrite (fall back to
`reset()` semantics) and skip compaction otherwise.

---

### [HIGH] Spend/unspend replay is not idempotent across re-spend sequences — `spent_utxos` drifts, which can set `delete_at_height` on a record with live UTXOs

**Location:** `src/recovery.rs:957-1029` (`replay_spend`), `src/recovery.rs:1031-1110` (`replay_unspend`)

**What's wrong:** `replay_spend` skips only when the slot is `UTXO_SPENT`
**with identical spending_data**; otherwise it rewrites the slot and does
`meta.spent_utxos += 1` (R-010 re-derivation). Consider the in-window history
on one slot: `Spend(A)` → `Unspend(A)` → `Spend(B)` (a reorg pattern), with
all three already applied to the device before the crash
(`spent_utxos = c+1`, slot SPENT/B). Replay: entry 1 sees SPENT/B ≠ A → not
skipped → writes SPENT/A, count = c+2; entry 2 matches A → UNSPENT, c+1;
entry 3 applies → SPENT/B, **c+2**. Final counter is one higher than truth.
The contract "replaying the entire log from any checkpoint produces the same
final state regardless of whether entries were already applied" is violated:
slot status converges, the counter does not. The same +1 drift occurs per
spend/unspend/respend cycle and compounds.

**Why it matters:** `spent_utxos` feeds `evaluate_delete_at_height` (invoked
in the SpendV2 replay path with the derived context, recovery.rs:1009-1023).
An overcounted record with one genuinely **unspent** slot can satisfy the
all-spent condition, get `delete_at_height`/`LAST_SPENT_ALL` stamped, and be
pruned at DAH — destroying a live UTXO. That is fund loss triggered by a
crash during a reorg window.

**Reproduction:** Unit test against `recover()`: build a 2-slot record on a
MemoryDevice with `spent_utxos = 1`, slot0 = SPENT/B (slot1 UNSPENT); redo
log contains SpendV2(slot0, A, target_gen g), UnspendV2(slot0, A),
SpendV2(slot0, B). Run `recover`; assert `meta.spent_utxos == 1` — observed
value will be 2. Extend with retention context to show DAH gets stamped while
slot1 is unspent.

**Suggested fix:** Give SpendV2/UnspendV2 replay the same generation-token
idempotency MarkOnLongestChain has (H7, recovery.rs:1823-1838): skip when
`meta.generation` is at-or-ahead of `target_generation` (the entries already
carry it); only then fall back to slot-state comparison. Alternatively,
recompute `spent_utxos` by scanning slot statuses at the end of replay for
every touched record instead of incremental adjustment.

---

### [HIGH] Torn data-region write inside the WAL-protected window is unrecoverable — recovery fails closed with no repair path

**Location:** `src/recovery.rs:971-974, 1045-1048, 176-178` (`is_fatal_replay_cause`); `src/record.rs:192-213, 593-636`; `src/io.rs:965-982`

**What's wrong:** Torn-write **detection** exists and is good: every
`TxMetadata` (256 B) and `UtxoSlot` (64 B) carries a CRC32, validated on
every read. But when replay of a Spend/Unspend/Freeze/PruneSlot entry hits a
CRC-failing slot or metadata header — the *exact* artifact of a crash during
the post-WAL data pwrite the redo log exists to cover — `read_utxo_slot`/
`read_metadata` return `Err`, the handler returns `Failed(ReplayCause::IoError)`,
which `is_fatal_replay_cause` classifies fatal, replay short-circuits
(recovery.rs:478-483), and startup aborts (`src/bin/server.rs:662-668`).
There is no repair: `Spend`/`SpendV2` entries don't carry the slot's
`utxo_hash`, so replay cannot reconstruct the slot from the WAL the way
`CreateV2` reconstructs whole records. Mitigating layout fact: records are
4 KiB-aligned, metadata (256 B) and slots (64 B, offsets 256+64·i) never
straddle a 512 B sector, so sector-atomic devices won't produce a CRC-torn
slot — the exposure is devices that tear inside a sector on power loss
and any latent bitrot in the region.

**Why it matters:** Fail-closed is the right default against corruption, but
here it bricks the node (boot loop until manual surgery) for a state the WAL
nominally covers, and the operator has no tool to apply the durable redo
intent. The module doc (recovery.rs:18-21) claims torn writes are handled by
replay; they are only detected.

**Reproduction:** Write a valid record; append a flushed SpendV2 for slot 0;
corrupt one byte inside slot 0 on the device (simulated intra-sector tear);
run `recover` — returns with `failed_io = 1`, replay short-circuits, and
`check_replay_tolerance` aborts startup. No subsequent run can ever succeed.

**Suggested fix:** Carry `utxo_hash` in SpendV2/UnspendV2 (32 bytes — the
entries already carry 96) so replay can rebuild a CRC-failing slot from the
WAL exactly like CreateV2 does for records; restrict fail-closed to
CRC failures on regions with *no* covering redo entry. Add an offline
`teraslab-cli repair` that replays the redo log with reconstruct-on-CRC-failure.

---

### [MEDIUM] Recovery-progress markers can hit `LogFull` during replay — deterministic startup boot-loop on a nearly-full log

**Location:** `src/recovery.rs:469-475, 486-490`; `src/redo.rs:1776-1801, 1965-1968`; `src/bin/server.rs:662-668`

**What's wrong:** A crash is most likely precisely when the redo log is
nearly full (checkpoint pressure). On the next boot,
`recover_all_..._progress` appends `RecoveryProgress` markers into the same
log (every 16 384 entries and once at the end). `append` rejects with
`LogFull` when `write_pos + entry > capacity`; the `?` at recovery.rs:473/489
propagates it as a top-level `RecoveryError`, and the bin exits
(`recovery failed — aborting startup`). Nothing reclaims space before
recovery (compaction runs only via the checkpoint task, which starts later),
so the failure repeats on every restart.

**Why it matters:** Availability: a node that crashed with ≥ ~99.99 % redo
usage cannot boot without manual intervention (grow `redo_log_size`). No data
loss, but it is a self-inflicted, deterministic outage in exactly the
high-load scenario the log-full path is supposed to survive.

**Reproduction:** Fill a small redo log to within < 30 bytes of capacity
with flushed entries, then call
`recover_all_with_allocator_collecting_pending_conflicts_progress`; the final
`mark_recovery_progress` returns `LogFull` and the function errors after
having replayed everything.

**Suggested fix:** Treat marker-append failure as non-fatal (log + skip —
the marker is an optimization, recovery is idempotent), or compact the
covered prefix before writing the final marker.

---

### [MEDIUM] Secondary-index reconcile does a full primary-index metadata scan on every startup — recovery time bounded by store size, not by redo size

**Location:** `src/recovery.rs:492, 541-576` (`reconcile_secondary_indexes_from_metadata`)

**What's wrong:** After replay, recovery unconditionally `clear()`s the DAH
and unmined secondaries (including the redb-backed, crash-durable ones) and
re-derives them by iterating **every** primary-index entry and issuing a
per-key `read_metadata` (random 4 KiB read). This runs even when the redo
log contained zero entries. It is O(index), not O(redo).

**Why it matters:** Not O(n²), so no correctness issue, but the checklist
target "recovery time bounded" fails at scale: at 10⁹ records and ~500 K
read IOPS this is ≥ 30 minutes of mandatory boot time per restart, and it
nullifies the entire point of the crash-durable redb secondaries (README's
"No (crash-durable by default)" claim for redb is operationally false — they
are wiped and rebuilt every boot).

**Reproduction:** Populate 10 M records, restart with an empty redo log,
time startup; profile shows the reconcile loop dominating.

**Suggested fix:** Reconcile only keys touched by replayed entries (the
replay loop already knows them); fall back to the full scan only when the
secondary backend reports it was not cleanly closed.

---

### [MEDIUM] Crash-injection coverage is hand-picked sync points only — no arbitrary-point kills, no torn-write injection, no write-ordering permutations

**Location:** `src/fault_injection.rs:59-129`; `tests/fault_injection.rs`; `tests/recovery_crash_boundaries.rs`

**What's wrong:** The framework is deterministic and well-placed (10 named
`SyncPoint`s: redo fsync ±, data pwrite ±, redb commit ±, hashtable rename,
allocator persist) and the tests genuinely restart-and-verify state (e.g.
`kill_after_redo_fsync_before_data_pwrite_recovers_slot`,
`before_redo_fsync_crash_after_partial_writev_returns_consistent_prefix`).
But: (a) faults fire only at the author-enumerated boundaries — there is no
"kill at every pwrite/sync, exhaustively" harness, so the windows in findings
1-4 (inside `perform_checkpoint`, inside `compact_prefix_through`, inside
`persist`) have **no** sync points at all and are untestable with the current
enum; (b) crashes are in-process panics, never SIGKILL of a real process on a
real filesystem; (c) there is no torn/partial-write injection on the data
device (only redo-side partial-flush tests); (d) all durability tests run on
`MemoryDevice`, whose `sync()` is a no-op — the suite cannot distinguish
"synced" from "merely pwritten", which is exactly the bug class of finding 1.

**Why it matters:** The test suite proves the boundaries the authors thought
of; the critical findings above all live between the enumerated points.

**Reproduction:** n/a (coverage gap). Inspect `SyncPoint` enum vs.
`perform_checkpoint_with_reset_guard` — zero `fault_injection::check` calls in
checkpoint.rs or in `compact_prefix_through`/`persist`.

**Suggested fix:** Add a "volatile-cache" BlockDevice test double that drops
unsynced writes on simulated crash, and an exhaustive harness that kills
after the N-th device operation for all N (state-machine sweep). Add
sync points inside checkpoint, compaction, and allocator persist. Longer
term, a `dm-log-writes`-based CI job for the file-backed device.

---

### [MEDIUM] "Snapshot lost + crash" combination untested; the existing rebuild test only covers clean shutdown

**Location:** `tests/integration.rs:1447-1601` (`snapshot_deletion_forces_device_scan_rebuild_with_exact_state`); `src/bin/server.rs:521-533, 617-668`

**What's wrong:** The B-04 test deletes the snapshot **after a clean
shutdown** (`snapshot_index` + `persist_allocator`, empty redo log). The
production-relevant case — snapshot missing/corrupt **and** unreplayed redo
entries pending (device-scan rebuild first, then redo replay on top of the
scanned index) — has no test. The interplay is non-trivial: the device scan
registers records from device bytes; replay then re-applies post-crash
entries against those entries (e.g. a `Delete` whose record was already
tombstoned, a `CreateV2` whose bytes are partially present).

**Why it matters:** This is the worst-case-but-supported recovery path the
README advertises for the memory backend; an ordering bug here surfaces only
in a real double-failure incident.

**Reproduction:** n/a (coverage gap). Extend the B-04 test: skip the clean
shutdown, leave flushed-but-unapplied redo entries (create + spend), delete
the snapshot, run `load_primary_index_in_memory` + `recover_all_with_allocator`,
assert exact final state.

**Suggested fix:** Add that test; also one for "snapshot corrupt" (the
`server.rs:508` branch) with pending redo.

---

### [LOW] Compensation path applies engine mutations before appending compensation redo entries

**Location:** `src/server/dispatch.rs:1976-2380` (`compensate_replication_failure`)

**What's wrong:** Unlike every forward path (WAL-first verified), the
replication-failure rollback applies `engine.unspend`/`engine.freeze`/... and
only afterwards batches `comp_redo` entries for append+flush. A crash between
the engine apply and the comp-redo flush replays the original forward entry,
re-instating the mutation the client was told failed. The file's doc comment
(lines 1956-1975) acknowledges and accepts this ("client received no
response… double-fault required"). Also note the compensation `Spend`/`Unspend`
entries carry `new_spent_count: 0`, which is harmless only because replay
re-derives the counter (R-010) — worth a comment so nobody "fixes" replay to
trust the field again.

**Why it matters:** Documented, bounded ambiguity window; client contract
already requires handling unknown-outcome. Listed for completeness as the
one mutation family that is not WAL-first.

**Reproduction:** Arm a panic between `engine.unspend(&req)`
(dispatch.rs:2017) and the comp-redo flush; restart; the original spend is
re-applied by replay though the client saw an error.

**Suggested fix:** Append+flush compensation intents *before* applying the
inverse mutations (the before-images are already captured); or leave as-is
with the documented contract.

---

## Checklist disposition

1. **Redo append before device write at every mutation site** — ✅ for all
   client-facing forward paths: spend (`dispatch.rs:2933-2982` — validate →
   redo flush → apply), create (`dispatch.rs:3812-3853`), set_mined
   (`3330→3411`), freeze/unfreeze (`4043→4070`, `4157`), reassign (`4270`),
   set_conflicting (`4421`), set_locked (`4542`), preserve_until (`4664`),
   delete (`5029-5114`), mark_longest_chain (`5339`); allocator
   allocate/free journal-before-return (`allocator.rs:494-527, 609-652`);
   engine-internal `AppendConflictingChild`/`AppendDeletedChild`
   (`engine.rs:3014, 3302`); two-phase secondary intents before redb commit
   (`engine.rs:336-480`). ⚠️ one exception: the replication-failure
   compensation path applies before logging (LOW finding above, documented
   in-code).
2. **Replay idempotency per record type** — ⚠️. CreateV2 (skip-if-indexed +
   byte rewrite), SetMined (block-id dedup incl. overflow), MarkOnLongestChain
   (generation token H7), Freeze/Unfreeze/Prune/SetConflicting/SetLocked/
   PreserveUntil (absolute-state writes with skip checks,
   recovery.rs:1644-1840), allocator replay (`replay_redo` no-op on
   duplicates), secondary intents (primary-authoritative staleness check) are
   idempotent and several have explicit two-pass tests
   (`tests/recovery_crash_boundaries.rs:346-575`). ❌ Spend/Unspend counter
   drift on spend→unspend→respend sequences (HIGH finding 4).
3. **Torn 4 KiB writes detected** — ✅ detection: CRC32 on metadata
   (`record.rs:563-636`), slots (`record.rs:181-213`), redo entries
   (`redo.rs:1509-1554`), redo header (`redo.rs:188-231`, refused at open),
   allocator header (`allocator.rs:1205-1231`). ⚠️ recovery: CreateV2-covered
   records are repaired; slot/metadata tears fail closed with no repair path
   (HIGH finding 5); allocator-header corruption falls back to a **fresh
   allocator** (CRITICAL finding 2).
4. **Power loss between redo append and metadata write** — ✅ recoverable;
   tested at `tests/fault_injection.rs:88-210`
   (`kill_after_redo_fsync_before_data_pwrite_recovers_slot`, restart +
   state verify) and `tests/recovery_crash_boundaries.rs:152-196`.
5. **Power loss between metadata write and slot write** — ✅ replay rewrites
   both slot and counter from the entry (`replay_spend` writes slot then
   metadata); `AfterDataPwrite` sync point exists (`engine.rs:4351`);
   boundary-3 test covers data-written/replication-skipped
   (`recovery_crash_boundaries.rs:206-256`). Caveat: counter drift of
   finding 4 applies in re-spend histories.
6. **Power loss during checkpoint** — ❌. Snapshot itself is atomic
   (tempfile + fsync + rename + dir-fsync, `index/mod.rs:307-347`, tested) and
   a half-written snapshot cannot break replay; but the checkpoint sequence
   reclaims redo without a data-device barrier (CRITICAL finding 1), the
   unsynced allocator persist can be lost after its covering entries are
   compacted (folded into finding 1), torn allocator header → fresh-allocator
   fallback (CRITICAL finding 2), and torn compaction rewrite loses retained
   durable entries (HIGH finding 3). No fault-injection points exist inside
   the checkpoint path.
7. **Oldest entries not overwritten until known applied** — ✅ design is
   linear-with-reset, not circular (redo.rs:1-38): `append` refuses past
   capacity (`redo.rs:1786-1791`); reclamation only via
   `compact_prefix_through` gated on the snapshot fence and the replica-ack
   `reset_guard` (`checkpoint.rs:383-393`). Tests:
   `checkpoint.rs:522-558` (guard rejection preserves bytes + catch-up
   readable), `tests/g4_redo_reclamation.rs`, `g4_compact_zero_tail.rs`.
   ⚠️ subject to the torn-compaction caveat (finding 3).
8. **Redo log full → clean rejection** — ✅ `RedoError::LogFull` from
   `append` propagates to a clean `ERR_STORAGE_IO` response with rollback of
   reservations (dispatch.rs:2948-2974, 3815-3846); allocator rolls back
   in-memory reservations on journal failure (`allocator.rs:510-517,
   579-586`, tested at `allocator.rs:1990-2240`); flush failure poisons the
   log rather than silently retrying (`redo.rs:1599-1605`, `g4_redo_poison.rs`);
   background checkpoint keeps it from bricking
   (`checkpoint.rs:709-770` sustained-mutation test). ⚠️ recovery-time
   marker appends can still hit LogFull and abort boot (MEDIUM finding 6).
9. **Recovery time bounded** — ⚠️. Redo scan is single-pass, chunked, memory-
   bounded (F-G4-009, redo.rs:2224-2365); replay is O(entries) with durable
   progress markers every 16 384 entries (F-G4-011) so crash-during-recovery
   does not re-replay (tested: `g4_recovery_progress_bound.rs`,
   `g4_recovery_short_circuit.rs`); no O(n²) rescan found. ❌ however the
   unconditional full-index secondary reconcile makes total recovery O(store
   size) every boot (MEDIUM finding 7).
10. **Rebuild-from-device-scan exercised by snapshot-deletion test** — ⚠️.
    `tests/integration.rs:1447` deletes the snapshot and verifies exact
    rebuilt state (plus the corrupt-header fail-closed companion at :1604),
    but only after a **clean shutdown** — the snapshot-lost **+ crash**
    (pending redo) combination is untested (MEDIUM finding 9).
11. **fsync discipline / macOS F_FULLFSYNC** — ✅ with caveats. All barriers
    go through `BlockDevice::sync` → `File::sync_all`
    (`device.rs:1028-1031`); Rust std maps `sync_all` to
    `fcntl(F_FULLFSYNC)` on Darwin, so the macOS disk-cache hole does not
    exist (it relies on std behavior — worth a code comment; `F_NOCACHE` at
    `device.rs:884-892` is only a cache hint, not a barrier, and the comment
    overstates it as "approximate O_DIRECT"). Snapshot writes fsync file +
    parent dir (`index/mod.rs:312-317`, self-test at :1071). ❌ the real gap
    is *where* barriers are issued, not how: no data-device barrier before
    redo reclamation (finding 1) and none after allocator persist
    (finding 1/2). README's platform claim ("Linux or macOS") is consistent
    with the sync implementation.
12. **Crash-injection test quality** — ⚠️. Deterministic thread-local
    framework with 10 sync points, zero-cost when disabled
    (`src/fault_injection.rs`); integration tests genuinely tear down and
    re-open state and verify post-recovery values (not just `is_ok`), and
    include meta-tests that the sync points still exist on the hot path
    (`tests/fault_injection.rs:545-590, 964-985`). ❌ hand-picked boundaries
    only — no arbitrary-kill sweep, no SIGKILL/process-level kills, no torn-
    write injection on the data device, no sync points inside checkpoint/
    compaction/persist, and all durability tests run on `MemoryDevice` whose
    `sync()` is a no-op (MEDIUM finding 8).
