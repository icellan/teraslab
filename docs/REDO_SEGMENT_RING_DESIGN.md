# Redo Segment Ring — Design

Status: PROPOSED (not yet implemented). This is the "lever 7" redo redesign that
retires the fixed-size-region space race the lever-6 fixes (6a/6c/6d) work
around. It keeps TeraSlab's O_DIRECT / raw-device thesis — it is an **in-device**
segment ring (no per-append filesystem files), not a file-per-segment WAL.

## 1. Problem: the fixed region makes space management a real-time race

Today the redo entries region is a single fixed byte range. The live window is
`[logical_start, write_pos)`; `write_pos` advances monotonically toward the end
of the region and is only ever rewound by reclaiming a durable prefix. Two
consequences flow from that single-region shape, and every lever-6 fix is a
patch for one of them:

- **Reclaim must relocate.** Freeing the prefix `[logical_start, fence)` frees
  bytes at the *front*, but `write_pos` keeps marching toward the *end*. So
  `compact_prefix_through` physically **relocates** the retained post-fence
  entries to a stale region. Under sustained writes the retained set is larger
  than the free space → the relocate fails with `LogFull` (lever 6d's
  "fuzzy-overtaken" stall: the checkpoint loses the race for its own work area).
- **Reclaim needs log space.** The recovery fence is written as a
  `RecoveryProgress` **log entry**, so a 100%-full log cannot even *start*
  reclaiming — the fence append is rejected (the lever-6c deadlock). 6c reserves
  a tail block so the fence always fits; that is a band-aid over "reclaim should
  not need to append at all".

The root cause is that the writer and the checkpoint compete, in real time, for
one fixed byte budget, *and* reclamation itself consumes that budget (relocate
work area + fence entry).

## 2. Goal and prize

Divide the entries region into **N fixed segments** and append into them in ring
order. A segment is reclaimed **whole** — by advancing a pointer — once a durable
snapshot covers every entry it holds. The byte cursor **wraps** into freed
segments, so there is **no relocation** and **no fence entry**:

- **Reclaim is O(1) pointer advance** (free covered segments), not a relocate.
  → retires `compact_prefix_through` and the lever-6d fuzzy/blocking race.
- **The fence lives in the header** (a field already exists: `checkpoint_seq`),
  set on every reclaim with no log append. → retires the lever-6c reserve.
- **`LogFull` becomes rare and honest**: it fires only when the writer fills the
  *entire ring* before a snapshot completes — true overload, not a per-cycle
  artifact. The lever-6a "don't poison on `LogFull`" semantics are **kept** (a
  rare true-full is still transient backpressure, never a poison).

What is **kept**: the on-device entry format (length | seq | type | data | crc),
the global monotonic sequence (`shared_seq`) across per-store logs, the
replication `read_from_sequence` contract, and lever 6b (fsync outside the log
lock — orthogonal, still a win).

## 3. Design overview

The entries region becomes a ring of `N` equal, alignment-sized segments. Each
segment is itself a linear append area; **no entry straddles a segment
boundary**. Append fills the active segment; when the next entry would not fit,
the segment's tail is left zero (the existing end-of-data sentinel) and the
append rolls to the next segment in ring order. The cursor wraps from the last
segment back to segment 0 as long as the segment it lands on is **free**.

```
entries region (log_size - HEADER_BLOCK_SIZE), divided into N segments:

  ┌─────────┬─────────┬─────────┬─────────┬─────────┬─────────┐
  │  seg0   │  seg1   │  seg2   │  seg3   │  seg4   │  seg5    │   (ring)
  └─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘
     free      free   [oldest_seg .......... active_seg]  free
                       └── live window (un-reclaimed) ──┘
                                          write_pos ────┘ (within active_seg)
```

- `oldest_seg` — the oldest segment still holding live (un-reclaimed) entries.
- `active_seg` + `write_pos` — where the next append lands.
- Live window = segments `[oldest_seg, active_seg]` in ring order. Everything
  else is free and available to wrap into.
- **Ring-full** = rolling to the next segment would land on `oldest_seg` (a live
  segment). Only then does append return `LogFull`.

This is the "circular" model the current module doc explicitly rejected — but
rejected at *byte* granularity, where variable-length entries, torn wraps, and
recovery scanning are genuinely hard. Wrapping at **segment** granularity (no
entry straddles a segment) makes each segment an independent linear region: a
torn write only ever damages the active segment's tail, and recovery scans
segments in sequence order. The hard part of "circular" disappears.

### 3.1 On-disk layout (header v3)

The first aligned block stays the `RedoHeader`. Bump `REDO_HEADER_VERSION` to 3
and extend it (still CRC-32 protected, still written with the existing
double-buffered atomic flip):

```
magic(8) | version(2) | checkpoint_seq(8) | next_sequence(8)
        | segment_size(8) | segment_count(4)
        | oldest_seg(4) | active_seg(4) | write_pos_in_seg(8)
        | crc32(4)
```

- `segment_size`, `segment_count` — the ring geometry, fixed at format time.
- `oldest_seg`, `active_seg`, `write_pos_in_seg` — replace the v2
  `logical_start` / `write_pos` pair (which were single-region byte offsets).
- `checkpoint_seq` — **repurposed as the durable recovery fence** (see §3.3). In
  v1/v2 this field was only set by the test-only `mark_checkpoint`; in v3 it is
  the production fence, set on every reclaim.

The header is the crash authority for the ring pointers and the fence. The
per-segment *sequence ranges* are NOT stored in the header — they are recovered
by scanning the (few) live segments at open (§3.4), so the header stays small and
a single atomic block.

### 3.2 Append + flush (segment roll)

`append` is unchanged except for the capacity/roll logic:

- Compute the serialized entry length `L` (≤ `segment_size`; enforced at open,
  see §6).
- If `write_pos_in_seg + L > segment_size`: the entry does not fit the active
  segment. Zero-pad the active segment's tail (already implicit — the scan stops
  at the first length-0 word) and **roll**: `next = (active_seg + 1) % N`.
  - If `next == oldest_seg` (the ring is full): return `RedoError::LogFull`
    (transient, **non-poisoning** per 6a). Nothing is buffered (the
    `append_atomic` all-or-nothing guarantee from lever 6 is preserved).
  - Else `active_seg = next`, `write_pos_in_seg = 0`, and append there.
- Append into the active segment as today (buffer + sequence draw).

`flush` (and the lever-6b `flush_pwrite_no_sync` / lock-free `sync_device`
split) is unchanged except that the device offset is computed from
`active_seg * segment_size + write_pos_in_seg` within the entries region. A flush
never crosses a segment boundary because a roll happens at append time, so the
aligned-pwrite math is per-segment and identical to today's per-region math.

### 3.3 Reclaim via the header fence (no append, no relocate)

The checkpoint flow (`crate::checkpoint`) changes from "fence-entry + compact" to
"header-fence + free-segments":

1. Snapshot the engine + run the data/index durability barrier (**unchanged** —
   this is the slow part, and it is now decoupled from redo space).
2. **Set the fence in the header**: `checkpoint_seq = snapshot_fence_sequence`,
   then write + fsync the header. No log append → the lever-6c reserve is gone.
3. **Free covered segments**: advance `oldest_seg` over every segment in ring
   order whose **maximum sequence ≤ `checkpoint_seq`** (every entry it holds is
   covered by the durable snapshot). Each freed segment is now available for the
   ring to wrap into. The active segment is never freed (it is being written).
   Persist the new `oldest_seg` in the header (folded into the same header write
   as step 2, or a second cheap header write — header writes do not touch the
   entries region).

Reclaim is now O(number of freed segments), each a pointer advance — no
relocation, no work area, no competition with the writer for entry space. There
is no longer any "fuzzy vs blocking" distinction for *space*: while the snapshot
runs, writes flow into free segments; if there are no free segments the ring is
full and writes backpressure (rare, see §6 sizing). Lever-6d's
`fuzzy_would_be_overtaken` / escalation heuristic is **removed**.

Per-segment "maximum sequence" for the reclaim test is the max sequence observed
when the segment was written (tracked in memory as an `N`-entry array; rebuilt on
recovery by scanning live segments). It does not need to be in the header — a
crash simply re-derives it from the live segments at open.

### 3.4 Recovery (multi-segment scan, header-bounded replay)

`open`:

1. Read + CRC-check the header. Get geometry, `oldest_seg`, `active_seg`,
   `write_pos_in_seg`, `next_sequence`, `checkpoint_seq` (the fence).
2. Scan the **live** segments in ring order `[oldest_seg .. active_seg]`,
   parsing entries (CRC per entry, stop each segment at its first length-0 word).
   The active segment is scanned up to `write_pos_in_seg`; a **torn tail** is
   dropped exactly as today (the last partial/!CRC entry is not replayed). Build
   `entries_cache` and the per-segment max-sequence array.
3. **Replay set** = live entries with `sequence > checkpoint_seq` (the header
   fence). Entries `≤ checkpoint_seq` that happen to still sit in an un-freed
   segment are skipped — the snapshot already covers them. This replaces the
   "find the last `RecoveryProgress` marker" scan; `RecoveryProgress`/`Checkpoint`
   ops remain *recognized* (skipped) on replay so v1/v2 logs and in-flight
   markers stay readable.

Crash-safety of reclaim falls out of the fence: even if a crash happens after the
snapshot is durable but before `oldest_seg` advanced (or before the header fence
write landed), recovery replays from the redo, and replay is **idempotent** and
**bounded by `checkpoint_seq`** — a lagging `oldest_seg` only means a few covered
segments are re-scanned and their entries skipped. No acked write is lost: the
snapshot covers `≤ fence`, the live segments cover `> fence`. The existing
`AfterSnapshotRenameBeforeReclaim` fault-injection point maps to "after header
fence write, before `oldest_seg` advance".

### 3.5 Sequence counter, per-store, replication — unchanged

- The global `shared_seq` atomic and per-log `next_sequence` high-water
  (persisted in the header, restored as the max over headers at boot via
  `shared_sequence_floor`) are unchanged: sequences are still globally
  monotonic; the ring only changes *where* an entry's bytes land.
- Each store's redo is its own ring on its own region/file (per-store redo
  unchanged).
- `read_from_sequence` (replica catch-up) reads `entries_cache`, which now spans
  the live segments. Its contract is unchanged: entries below the fence may have
  been reclaimed (segment freed) → the read returns what it has, and a replica
  needing pre-fence entries does a full resync, exactly as today.

## 4. What this retires vs keeps

| Lever-6 mechanism | Fate under the segment ring |
|---|---|
| 6a — `LogFull` must not poison | **KEEP** (ring-full is rare but still transient backpressure) |
| 6b — fsync outside the log lock | **KEEP** (orthogonal; still removes the per-flush serialization) |
| 6c — fence reserve (`fence_reserve_bytes`/`append_reclaim_marker`) | **RETIRE** — fence is a header field, reclaim never appends |
| 6d — `fuzzy_would_be_overtaken` + no-backoff escalation | **RETIRE** — reclaim never competes for entry space, so no doomed fuzzy |
| `compact_prefix_through` (relocate retained entries) | **RETIRE** — replaced by free-covered-segments pointer advance |
| `mark_recovery_progress` as a log entry | **RETIRE** for reclaim (fence → header); op still recognized on replay for back-compat |

Net: the ring removes more code than it adds (the relocate-compaction, the fence
reserve, and the checkpoint-mode heuristic all go away), at the cost of the
segment directory in the header and the multi-segment scan.

## 5. Format version & migration

- Header `version = 3`. `open` still **reads** v1/v2 headers so a node that
  crashed under an old binary can recover: drain the legacy linear log via normal
  recovery + the first checkpoint, then **reformat** the region as a v3 ring
  (write a fresh v3 header with `oldest_seg = active_seg = 0`,
  `write_pos_in_seg = 0`, `checkpoint_seq = next_sequence - 1`). Because the redo
  is a WAL (not durable user data) and recovery has made its contents
  authoritative in the snapshot + device, reformatting after a clean checkpoint
  is safe.
- New deployments: fresh region in v3.
- Gate behind config until the upgrade path is validated (mirrors `storage.packed`:
  `[redo] segment_ring = true`, default false), so the change ships dark and is
  flipped per deployment.

## 6. Sizing, edge cases & risks

- **Segment size ≥ max entry.** No entry may straddle a segment, so
  `segment_size` must exceed the largest serialized entry. The biggest is a
  `Create`/`ReplicaCreate` carrying the full record image (metadata 320 B +
  73 B/utxo; a few hundred bytes typical, tens of KB for a many-output tx) and
  the hashtable-resize ops. `open` validates `segment_size ≥ MAX_ENTRY_BYTES`
  (with margin) and rejects otherwise. Recommend `segment_size` of a few MiB.
- **Ring depth vs snapshot time.** The ring backpressures only if the writer
  fills all `N` free segments before a snapshot completes. Required:
  `N × segment_size  >  write_bytes_per_sec × snapshot_duration`. Size the total
  redo (`segment_count × segment_size`) to cover the worst-case snapshot wall
  time at peak write rate, plus headroom. This is the honest limit that replaces
  the per-cycle fixed-region stall; document it and expose ring usage as a metric.
- **Straggler entry pins a segment.** A segment is freed only when *all* its
  entries are `≤ fence`; one late entry (`> fence`) keeps the whole segment live.
  More, smaller segments reduce the waste; fewer, larger segments reduce header
  churn and per-roll tail waste. `segment_count` in the 8–64 range is a
  reasonable default; tune by bench.
- **Tail waste per roll.** Up to one max-entry of zero padding per segment when an
  entry doesn't fit the remainder. Negligible for multi-MiB segments.
- **Config minimums.** `segment_count ≥ 3` (one active, ≥1 free to wrap into, ≥1
  reclaimable) — validate; `N = 2` can livelock the ring against a slow snapshot.
- **Raw device.** Everything is in-region offset math — no filesystem files — so
  the ring works on a raw block device, preserving the O_DIRECT/raw-device thesis
  that a file-per-segment WAL would break (per-segment `creat` + parent-dir
  `F_FULLFSYNC` is exactly the metadata cost the fixed pre-allocated region
  avoids).

## 7. Phased implementation plan (TDD)

1. **Header v3 + segment directory.** Serialize/deserialize geometry + ring
   pointers; CRC; back-compat read of v1/v2. Tests: round-trip, version gate,
   v2→v3 field defaulting.
2. **Ring append + flush.** Segment-roll on full, no-straddle invariant, wrap
   into a free segment, ring-full → `LogFull` (non-poisoning, all-or-nothing).
   Tests: roll, no-straddle, wrap after a free, ring-full backpressure, lever-6b
   lock-free flush still holds.
3. **Header fence + reclaim.** `set_fence(seq)` (header) + `free_covered_segments`
   (advance `oldest_seg` over segments with max-seq ≤ fence). Tests: frees
   covered, retains uncovered, straggler pins its segment, wrap after reclaim.
4. **Multi-segment recovery.** Scan `[oldest_seg..active_seg]` in order;
   header-fence-bounded replay; torn active-tail dropped; crash between fence
   write and `oldest_seg` advance loses nothing. Tests + fault-injection at the
   remapped `AfterSnapshotRenameBeforeReclaim`.
5. **Replication read_from_sequence across segments.** Test catch-up spanning a
   wrap; pre-fence read returns the reclaimed-gap signal.
6. **checkpoint.rs integration.** Replace `mark_recovery_progress_all` +
   `compact_all_redo_through` with `set_fence_all` + `free_covered_segments_all`;
   delete the fuzzy/blocking-for-space distinction and the 6d heuristic; keep the
   snapshot + barrier ordering. Tests: sustained mutations never brick; existing
   crash-safety tests pass against the new reclaim.
7. **config + format gate + v2→v3 upgrade.** `[redo] segment_ring`,
   `segment_size`/`segment_count`, validation; recover-then-reformat path.
8. **Bench (packed + buffered, single node).** Expect the periodic fill→reset
   stall to disappear: writes only backpressure if the whole ring fills before a
   snapshot completes. Compare sustained throughput + tail latency to the
   fixed-region + 6a–6d baseline on a quiet machine.

## 8. Expected outcome

The writer and the checkpoint stop competing for one byte budget: reclaim frees
whole covered segments by advancing a pointer, the fence is a header field, and
the cursor wraps into freed segments with no relocation. `LogFull` degrades from
a per-cycle, self-inflicted stall to a rare true-overload signal. The lever-6c
reserve, the lever-6d heuristic, and `compact_prefix_through` are deleted; 6a and
6b carry over unchanged. The on-device entry format, sequence contract, and
replication semantics are preserved.
