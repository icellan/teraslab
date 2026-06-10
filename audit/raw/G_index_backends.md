# Audit Category G — Index Backends

HEAD: 1e5659b.

## IMPORTANT METHOD / RELIABILITY CAVEAT

A session-wide tooling fault caused the Bash tool and (later) the Read tool to
return empty output for the second half of this audit. As a result this audit is
INCOMPLETE. Everything below is based ONLY on output I directly and faithfully
observed while the tools were working:

1. A full Read of `src/index/hashtable.rs` lines 1–1440.
2. A large successful grep dump across `src/index/*.rs` and `src/recovery.rs`
   (the line-numbered hits quoted below are from that dump and are reliable as
   *locations*, though I could not in every case read the surrounding code).
3. Full reads of `src/index/backend.rs:1–80` and
   `src/index/secondary_backend.rs:1–289`.

I could NOT complete verification of: redb three-file atomicity, redb corruption
fallback, the in-memory probe high-load-factor test contents, and the
cross-crash/migration integration test contents. Those are listed as
"unverified — needs follow-up", NOT as findings.

### Correction of a draft finding

An earlier draft of this report asserted (G-01) that secondary indexes are NOT
rebuilt from a device scan and cited `src/storage/engine.rs`. **That file does
not exist**, and the claim is **WRONG** — retracted. The grep dump shows the
opposite: `Index::rebuild_secondary` (`src/index/mod.rs:510`, wrapper
`PrimaryBackend::rebuild_secondary` `backend.rs:672`) rebuilds the DAH+unmined
indexes from a device scan of metadata flags, and
`reconcile_secondary_indexes_from_metadata` (`src/recovery.rs:473`) rebuilds the
in-memory unmined/DAH state from each primary record's `meta.unmined_since` /
DAH after redo replay. `recovery.rs` is NOT empty (it has `recover_all*`,
`reconcile_secondary_indexes_from_metadata`, and ~20 recovery tests). I am
recording this correction rather than silently dropping it, per the project rule
to surface mistakes.

---

## Findings (low confidence; verify before acting)

### G-LOW-1 (LOW) — In-memory primary snapshot has NO checksum/version on the bucket bytes themselves; integrity relies on a sidecar sentinel file

Locations:
- `src/index/hashtable.rs:608-614` (doc), `:633-677` (`open_file_backed`),
  `:1381-1400` (`Drop`), `:1408-1412` (`sentinel_path_for`).

What's wrong: For the **file-backed** Robin Hood table, the raw 64-byte bucket
array on disk has "no header, magic, or per-bucket CRC" (the code's own words at
hashtable.rs:609). Torn writes cannot be distinguished from valid state by
inspecting the file. The only durable integrity signal is a sidecar
`.shutdown_clean` file written in `Drop` (hashtable.rs:1394-1395). On reopen,
absence of the sentinel only emits a `tracing::warn!` (hashtable.rs:662-669) and
**the open still succeeds with the possibly-torn bytes** — it does NOT
automatically drive `rebuild_file_backed`. An operator who misses the warn line
runs on a potentially corrupt primary index.

Note this is distinct from the *serialized* snapshot path (`Index::snapshot` /
`restore` in `index/mod.rs`), which IS versioned + CRC'd (see verified-OK).

Why it matters: A crash during a file-backed resize/operation can leave torn
bucket bytes; reopen accepts them silently (modulo a log line), and the redo log
is the only safety net. If the redo window has advanced past the affected
mutations, a torn bucket yields a wrong `record_offset` → lookups resolve to the
wrong on-device record → potential UTXO mismatch served to clients.

Reproduction (not run — tools dark): open a file-backed primary, insert records,
kill -9 mid-write to a bucket, delete/avoid the sentinel, reopen, look up an
affected txid. Confirm whether `open_file_backed` returns the torn entry vs.
forces a rebuild.

Suggested fix: On missing sentinel, either (a) fail closed and require an
explicit rebuild, or (b) auto-invoke `rebuild_file_backed` from the device scan
rather than only warning. At minimum add a test asserting the
missing-sentinel-on-reopen behavior.

Confidence: LOW (behavior read directly, but downstream impact depends on redo
coverage I could not trace this session).

### G-LOW-2 (LOW) — `unreachable!("checked above")` in build_resized is sound today but fragile

Location: `src/index/hashtable.rs:1298`.

What's wrong: `Backing::Anonymous => unreachable!(...)`. It IS currently
unreachable: the anonymous case returns early at hashtable.rs:1282-1293 before
this match. So this is correct today, NOT a live bug. Flagged only because a
future edit that adds a new `Backing` variant or reorders the early return turns
a logic slip into a hard panic on the resize path (availability). Prefer
returning a `ResizeIo` error over `unreachable!`.

Confidence: LOW (it is sound now; this is hardening).

---

## Verified-OK (confirmed correct from directly observed code)

- **In-memory Robin Hood probe distance is bounded.** `MAX_STORED_PROBE`
  (hashtable.rs:108) + `cap_probe` (112-114) cap stored probe distance at 254;
  every probe loop (`get_entry` 838-860, `insert` 902-939, `remove` 1002-1022,
  `update_cached_fields` 1155-1182) has a hard `if dist as usize >= self.capacity
  { ... }` termination guard, so probing cannot run unbounded.
- **Backward-shift delete cannot spin under corruption.** F-G3-005:
  hashtable.rs:1036-1067 caps the shift loop at `self.capacity` iterations and
  logs + bails on overrun; capped (254) entries are not decremented past the cap
  (1062-1064). Test `remove_backward_shift_terminates_under_corruption`
  (hashtable.rs:2426).
- **High-load-factor / probe-reasonableness test exists.**
  `max_probe_distance_reasonable` (hashtable.rs:1812-1822) asserts max probe
  < 100; `max_probe_distance_recomputed_after_remove` (1827-1843) verifies the
  recompute-on-read contract. (I did not read the insert volume to confirm the
  load factor reached; flagged as partially verified under follow-up.)
- **DoS-hardened hashing.** Per-process random `seed` (hashtable.rs:262-266)
  mixed via SplitMix64 (238-246) defeats directed Robin-Hood collision DoS;
  file-backed reopen rehashes under a fresh seed (`rehash_to_seed` 965-991).
- **Serialized snapshot format IS versioned + checksummed.** `SNAPSHOT_MAGIC` /
  `SNAPSHOT_VERSION` (index/mod.rs:65-66), validated on restore
  (mod.rs:322 (restore)). Tests: `snapshot_checksum_verified` (mod.rs:982),
  `snapshot_truncated` (1005), `snapshot_restore_rejects_unknown_version`
  (1022), `snapshot_restore_rejects_poisoned_primary_count` (see tests ~1173),
  `snapshot_restore_rejects_poisoned_secondary_count` (see tests ~1204).
- **Truncated/corrupt snapshot falls back to targeted device-scan rebuild via
  RestoreFlags.** `RestoreFlags { dah_needs_rebuild, unmined_needs_rebuild }`
  (mod.rs:113-119 (RestoreFlags)) is set per-section by `restore_all (mod.rs:359) when a section's
  magic/count is corrupt. Granular tests:
  `snapshot_all_corrupt_dah_section` (1134),
  `restore_all_dah_corrupt_but_unmined_intact` (1226),
  `restore_all_unmined_corrupt_but_dah_intact` (restore_all_unmined_corrupt_but_dah_intact, see tests),
  `snapshot_all_corrupt_unmined_section` (snapshot_all_corrupt_unmined_section, see tests). A forged-magic burst that fails
  CRC is skipped so the real section is still found
  (`locate_unmined_section_skips_forged_magic_when_real_follows`, mod.rs:1291 /
  ~6800 range).
- **Secondary indexes ARE rebuilt from device scan.** `Index::rebuild_secondary`
  (mod.rs:510) scans allocated regions for DAH/unmined metadata; tests
  `rebuild_secondary_from_device` (1630),
  `rebuild_secondary_dah_range_query_correct` (1702),
  `rebuild_secondary_unmined_range_query_correct` (rebuild_secondary_unmined_range_query_correct). Corrupt allocated
  record FAILS the rebuild (does not silently skip):
  `rebuild_secondary_fails_on_corrupted_allocated_record` (1642),
  `rebuild_secondary_fails_on_crc_mismatch_in_allocated_record` (rebuild_secondary_fails_on_crc_mismatch_in_allocated_record).
- **Primary rebuild fails closed on corruption** (does not silently drop
  records): `rebuild_fails_on_corrupted_magic_in_allocated_region` (1501),
  `rebuild_fails_on_crc_mismatch_in_allocated_region` (1521),
  `rebuild_fails_on_record_size_inconsistent_with_utxo_count` (rebuild_fails_on_record_size_inconsistent_with_utxo_count);
  corruption inside a freelist hole IS skipped, as intended
  (`rebuild_skips_corruption_inside_freelist_hole`, 7135).
- **Cross-crash secondary reconciliation is tested.** recovery.rs:
  `recover_all_applies_unmined_secondary_when_stale` (3451),
  `recover_all_skips_stale_unmined_redo_relative_to_primary` (3499),
  `recover_all_skips_when_secondary_already_matches_primary` (3536),
  `recover_all_applies_dah_secondary_when_stale` (3587),
  `recovery_post_replay_dah_index_matches_live_engine` (3995). Reconcile is
  wired into `recover_all_with_allocator` at recovery.rs:473.
- **redb backends are versioned/tested for rebuild + corruption at the metadata
  layer.** backend.rs: `rebuild_redb_fails_on_corrupted_magic_in_allocated_region`
  (1083), `rebuild_redb_fails_on_crc_mismatch_in_allocated_region` (1105),
  `rebuild_redb_matches_in_memory_rebuild` (1131). (NOTE: this covers rebuild
  *from device*, NOT redb-file corruption fallback — see follow-up.)
- **Crash-atomic file-backed resize.** `build_resized` (hashtable.rs:1271-1369):
  redo `HashtableResizeBegin` fsynced before tmp I/O (1303-1313), tmp written +
  msync+fsync (1318-1329), atomic rename (1332-1338), parent-dir fsync
  (1347-1349), then `HashtableResizeCommit` (1358-1366); orphan tmp cleaned by
  recovery on begin-without-commit. Fault-injection sync point at 1344.
- **`expected_records` is a sizing hint only.** `HashTable::new` rounds capacity
  up to next_power_of_two().max(16) (hashtable.rs:577); exceeding the hint
  triggers `resize` (power-of-two growth), not failure.

---

## Unverified — needs follow-up (NOT findings)

1. **redb three-file atomicity** (redb_primary / redb_dah / redb_unmined): three
   independent redb databases with no cross-database transaction. A crash
   between committing the primary redb txn and the DAH/unmined redb txn could
   leave them inconsistent. The two-phase redo intent (secondary_backend.rs:3-7,
   103-108) is meant to cover this, but I could NOT verify the commit ordering
   or that a partial multi-file commit is recoverable. HIGH-priority follow-up.
2. **redb FILE corruption fallback** (delete/recreate/fall back to memory) tested
   with a genuinely corrupt redb file: the tests I saw cover rebuild-from-device
   on corrupt *metadata*, not a corrupt *.redb* file. Verify
   `redb_primary.rs` / `redb_dah.rs:234` / `redb_unmined.rs:253`
   ("falling back to an empty ..." doc hits) actually handle a corrupt redb file
   and that a test plants a corrupt redb file.
3. **In-memory high-load-factor probe test** actually drives load factor high
   enough to be meaningful (saw the assertion at hashtable.rs:1812, not the
   insert volume).
4. **integration tests** `backend_modes_*` (tests/integration.rs:435,483) mutate
   → kill → reopen and assert secondary CONTENTS, not just Ok(_).
