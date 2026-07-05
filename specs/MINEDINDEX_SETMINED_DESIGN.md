# MinedIndex — Decoupled Mined-State for MAX setMined Throughput

- **Date:** 2026-07-05
- **Status:** Design — forks approved in brainstorming; pending spec review.
- **Base:** `main` @ `f00ad34` (the slim pure-locator primary index; `TxIndexEntry = { device_id, record_offset }`, bucket 20 B).
- **Branch:** `feat/mined-index-setmined`.

## 1. Goal

Sustain **maximum setMined throughput** for hundreds of millions of transactions
(block-mining bursts and chain sync). setMined is the throughput-critical write
path: when a block is found, every transaction in it is marked mined in one
batch.

## 2. Problem (measured)

Today mined-state (block entries + `unmined_since`) lives **inline in the
on-device record**, so marking a block's txs mined is a scattered in-place RMW:

- Per tx: primary lookup + 256 B device read + a **4 KiB scattered block write**
  (16× amplification for a 256 B change) + secondary-index maintenance.
- Measured production shape (O_DIRECT + write-back cache, cache-hot): **~2.45 µs/tx
  CPU** before any device write; the "fast path" is dead in production (it needs a
  raw device pointer, which O_DIRECT does not expose).
- At 100M txs: up to ~400 GB of scattered device writes.

Root cause: mined-state is co-located with the record, so a block's worth of
mined txs = a device's worth of scattered writes.

## 3. Core idea

Relocate mined-state out of the on-device record into a dedicated, in-RAM,
**authoritative `MinedIndex`**. setMined becomes: **one batched WAL append + in-RAM
slot writes — zero data-device I/O.** Reads that need mined-state consult the
MinedIndex, reached via the primary lookup they already perform.

This is greenfield (no users yet): no migration, no on-device mined-state region,
breaking format changes are free.

## 4. Locked decisions

| # | Decision | Choice |
|---|---|---|
| D1 | Authority / recovery | **Pure store-auth.** No mined-state on the device at all. Recovery = MinedIndex snapshot + redo replay only. No device-scan fallback for mined-state. |
| D2 | Foundation | On top of the slim pure-locator primary index (`f00ad34`). |
| D3 | Scope | MinedIndex holds block-entries + `unmined_since`; **subsumes the unmined secondary index** (its height-bucket range queries move here). The DAH secondary index stays separate. |
| D4 | Keying | A 4-byte `mined_slot` pointer in the primary entry → dense slot. **No duplicate txid key.** |
| D5 | Batch-native | WAL and replication carry **one batched op per setMined RPC**, exploiting that all txs in a batch share every field but the txid. |
| D6 | `subtree_idx` | UI-only; with no device home under D1 it lives in the MinedEntry slot (and redo/snapshot). A cold side-structure to reclaim its 4 B is a later follow-up. |

## 5. Data model — `src/index/mined_index.rs`

Sharded by txid (256 shards, matching the primary index). Per shard:

- **Dense arena** `entries: Vec<MinedEntry>` + `free: Vec<u32>` free-list.
  ```
  MinedEntry {                 // ~20 B (packed); the FIRST block tuple, inline
      block_id:      u32,   // identity of the mining (dedup, unset, replication)
      block_height:  u32,   // client reorg/maturity; replication
      subtree_idx:   u32,   // UI-only, but no device home under D1 (D6)
      unmined_since: u32,   // 0 == mined-on-longest-chain
      flags:         u8,    // bit0 = all_spent (spend path; §10); bit1 = has_overflow
  }
  ```
  The single-block common case (the overwhelming majority) is fully inline at
  ~20 B. A tx mined in **multiple** competing blocks (reorg — rare) sets
  `has_overflow` and spills its *extra* tuples to a sparse per-shard side-map
  `overflow: HashMap<u32 /*slot*/, Vec<BlockEntry>>`. Keeping overflow out of the
  inline entry avoids paying 8 B for a null pointer on every one of 100M records.
- **Height-bucket view** `unmined: HashMap<u32 /*height*/, HashSet<u32 /*slot*/>>` —
  **replaces the unmined secondary index**; serves its range/expiry queries
  (`ProcessExpiredPreservations`, DAH sweeps).

The slim primary entry gains **one field**: `mined_slot: u32` (locator becomes
`{ device_id, record_offset, mined_slot }`, ~20 B → ~24 B). It is a **stable
pointer**, not a cached copy of device state — it survives index rehash (it
travels with the entry) and segment relocate (mined-state is offset-independent),
so it does **not** reintroduce the coherence-bug class the slim rework removed
(there remains exactly one home per fact).

## 6. Lifecycle integration

| Event | MinedIndex action |
|---|---|
| **create** | Allocate a slot: `unmined_since = block_height`, no blocks, `all_spent = false`. Add slot to `unmined[block_height]`. Store `mined_slot` in the primary entry. |
| **setMined** (on longest chain) | Fill block fields; `unmined_since = 0`; remove slot from its `unmined` bucket. |
| **setMined** (not longest chain) | Append the block tuple; leave `unmined_since`. |
| **unsetMined** | Remove the tuple by `block_id`; if it drops to 0 blocks, `unmined_since = current_height` and re-add to the `unmined` bucket. |
| **spend** (last UTXO) | Set `all_spent = true` in the slot (cheap RAM write on a path already device-bound). |
| **delete** | Free the slot (push to `free`), remove from any bucket, clear the primary `mined_slot`. |
| **spend / segment relocate** | Record offset changes; `mined_slot` is unchanged and carried on the relocated primary entry. |

## 7. Hot path — setMined batch (the MAX path)

Input is already batch-native: `OP_SET_MINED_BATCH` = shared `SetMinedBatchParams`
+ txid list (all txs in one block).

1. Decode → shared params + `txids`.
2. Per txid: cluster ownership check + primary lookup (exists? → `mined_slot`).
   Fan out across shards at/above the read-fanout threshold.
3. **WAL-first:** append **one** `RedoOp::SetMinedBatch { shared, txids }` (buffered;
   one append + one length header + one CRC + one fsync-batch for the whole block).
4. Apply per txid into the MinedIndex slot (RAM), fanned out across shards.
   **No device I/O.**
5. Build **one** `ReplicaOp::SetMinedBatch { shared, txids }` per replica; replicate.
6. Response: per-item `block_ids` from the slots (in hand — no read-back).

Target: **~200–400 ns/tx** CPU, **zero data-device writes**. Per-tx redo cost
collapses to the 32-B txid (block fields amortized once per batch); one redo entry
per block instead of ~50k.

## 8. Read path

- `GET(BLOCK_ENTRIES / BLOCK_ENTRY_COUNT / UNMINED_SINCE / BLOCK_ENTRIES_ALL)`:
  served from the MinedIndex slot (the primary lookup already yields `mined_slot`).
  `GET(other fields)` still reads the device record. `subtree_idx` from the slot.
- `delete_eval`: `has_blocks` + `on_longest_chain` from the slot; `spent_utxos`
  from the device record (the read-side merge) — see §10.
- Unmined range queries (expiry/preservation): the MinedIndex height-bucket view.

Note: because mined-state is normally read *bundled* with a full-record GET, this
is a **write-side** optimization. Reads that want block fields now merge two
sources (device record + MinedIndex); the merge is a RAM lookup with no extra hash
(the slot pointer is already in hand).

## 9. Batch WAL

- New `RedoOp::SetMinedBatch { block_id, block_height, subtree_idx, on_longest_chain,
  current_block_height, block_height_retention, unset: bool, txids: Vec<TxKey> }`.
  Shared fields once + `[count][txids]`.
- The current per-tx `RedoOp::SetMined` is **removed** (greenfield).
- **Replay:** iterate `txids`, apply into the MinedIndex. Idempotent — re-applying an
  already-mined tx is a no-op (matches today's SetMined replay idempotency).
- **Recovery order:** MinedIndex snapshot (checkpoint baseline) + replay of
  post-checkpoint `SetMinedBatch` / `Create` / `Delete` **into the MinedIndex**
  (today these replay into the device record; the replay *target* changes).

## 10. DAH (delete-at-height) coupling — the key detail

`delete_eval` decides deletability from **all-spent AND mined-state** together. Both
the last spend and setMined can be the deciding trigger, so both must re-check.

**Resolution:** keep the `all_spent` bit in the MinedEntry slot, maintained by the
spend path (spend already reads the record and knows when the last UTXO is spent —
one cheap RAM write). setMined then evaluates DAH **entirely in RAM** from the slot
(`all_spent` + mined-state), with no device read; at mining time `all_spent` is
almost always `false` (a tx being mined is rarely already fully spent), so the DAH
branch is usually skipped. The DAH secondary index is updated only on the rare
transition, from RAM inputs.

This is what makes setMined **zero device I/O** (not just zero device *writes*). It
costs the spend path one RAM bit-write on its last-spend. **Flagged for review:** if
coupling spend → MinedIndex is unwanted, the fallback is one device read per
setMined (spent_utxos) — still eliminates the write, but not the read.

## 11. Batch replication

- New `ReplicaOp::SetMinedBatch { shared, txids }` — one op per replica per RPC.
- Replica apply: iterate `txids` into its MinedIndex. Idempotent.
- The per-tx `ReplicaOp::SetMined` path is **removed**.

## 12. Durability & recovery

- setMined durability = the batch redo (buffered WAL; the checkpoint fsyncs the redo
  before reclaiming, as today). A crash loses at most the last flush-interval of
  setMined batches → those txs recover as unmined and are re-setMined when the block
  is reprocessed (idempotent). Same contract create/spend already live under.
- The checkpoint **snapshots the MinedIndex** (new versioned section: dense arenas +
  free-lists + height buckets; primary `mined_slot` pointers are snapshotted or
  rebuilt from the MinedIndex on load).
- **No device-scan fallback for mined-state** (D1). The MinedIndex snapshot + redo is
  the sole source of truth — the accepted tradeoff for true MAX. Recovery correctness
  therefore rests entirely on snapshot + redo and must be bulletproof (see §14).

## 13. Memory budget @ 100M

- MinedIndex dense arenas: 100M × 20 B (incl. `all_spent`, padding) ≈ **~2.0 GB**
  (+ rare overflow).
- Primary `mined_slot` pointers: 100M × 4 B ≈ **~0.4 GB**.
- Height buckets: bounded by the *unmined* backlog (≈ mempool, single-digit millions),
  small.
- Total ≈ **~2.4 GB**, vs ~3 GB+ for the key duplication a standalone txid-keyed map
  would incur (D4 avoids it).

## 14. Testing strategy

- **Unit:** slot alloc/free/overflow; height-bucket range queries; unset-to-zero
  restores `unmined_since` + bucket membership.
- **Property/equivalence:** MinedIndex state == what the old on-device block-entries
  would have been, across randomized create/setMined/unset/delete/reorg sequences.
- **Redo:** `SetMinedBatch` encode/replay round-trip; replay idempotency; crash-in-
  batch recovery.
- **Snapshot:** MinedIndex snapshot/restore round-trip; recovery (snapshot + redo
  replay) reproduces live state exactly (the whole durability contract under D1).
- **Cluster:** `SetMinedBatch` replication apply; master/replica MinedIndex convergence.
- **DAH:** mined(store) + spent(device/`all_spent` bit) merge produces the same
  delete-eligibility as today, incl. both trigger orders (spend-then-mine, mine-then-spend).
- **Perf:** dispatch-level setMined burst before/after (target ~6–10× ns/tx; assert
  zero data-device writes via a write-counting device).

## 15. Risks & non-goals

- **Consensus-adjacent:** mined-state correctness affects double-spend / finality →
  heavy adversarial review required.
- **Cross-cutting:** touches create, setMined, unset, delete, spend (the `all_spent`
  bit), GET, delete_eval, recovery, checkpoint/snapshot, replication.
- **Recovery robustness:** D1 removes the device fallback — snapshot + redo must be
  proven bulletproof.
- **Re-touches the slim primary entry** (adds `mined_slot`) — coordinate with the
  slim rework's authors.
- **Non-goals:** DAH secondary-index redesign (stays); any device backstop (dropped
  under D1); migration (greenfield); moving *other* record state (spent slots, cold
  data, flags) off the device.

## 16. Build sequencing

1. `MinedIndex` module + unit tests (isolated; no engine wiring).
2. Primary entry `mined_slot` + slot lifecycle (create/delete).
3. setMined rewrite onto the MinedIndex (hot path, §7) + `all_spent` on spend (§10).
4. Batch WAL (`SetMinedBatch`) + replay + snapshot/recovery.
5. Batch replication (`SetMinedBatch`).
6. Reroute readers (GET, delete_eval, unmined range queries); remove the unmined
   secondary index and the on-device block-entry region.
7. Perf validation + adversarial recovery/consensus review.
