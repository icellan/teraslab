# Spec Validation Report

## Date: 2026-03-18
## Repos analyzed: teranode@38d2a04e, aerospike-server@028f9403

---

## Findings

### Confirmed correct

**Data model:**
- UtxoSlot size: 69 bytes (32 hash + 1 status + 36 spending_data) matches Go `SpendingData` struct (32-byte txid + 4-byte vin LE) and Lua constants (`UTXO_HASH_SIZE=32`, `SPENDING_DATA_SIZE=36`, `FULL_UTXO_SIZE=68` — the extra byte is the status byte, not in Lua's `FULL_UTXO_SIZE` which only counts hash+spending_data)
- All 16 Lua error codes match the spec's §3.1 error code table exactly
- All 5 Lua signal codes (ALLSPENT, NOTALLSPENT, DAHSET, DAHUNSET, PRESERVE) match §3.2
- All 32 Go field definitions from `stores/utxo/fields/fields.go` are accounted for in the spec's Appendix A field cross-reference
- Eliminated fields correctly identified: `totalExtraRecs`, `spentExtraRecs`, `recordUtxos`, `totalUtxos`, `creating`, `conflictingChildren` (application-layer), `deletedChildren` (→ PRUNED status), `utxoSpendableIn` (→ slot spending_data encoding)
- `utxoBatchSize` default is 128 (correctly eliminated in TeraSlab — no pagination)
- Key construction: primary key is 32-byte txid alone; pagination keys add 4-byte LE index (correctly eliminated)
- Block entry structure replaces three parallel lists (`blockIDs`, `blockHeights`, `subtreeIdxs`) as spec describes
- Frozen sentinel: all 36 spending_data bytes = 0xFF — matches both Lua and C
- Spending data hex display: txid bytes reversed (32→1), vin bytes in order (33→36) — matches Lua `spendingDataBytesToHex`
- `ReAssignedUtxoSpendableAfterBlocks` = 1000 — matches spec §3.9 default
- Configuration defaults (Appendix B) match `settings/utxostore_settings.go` values

**Operation flows:**
- `spend`/`spendMulti` validation rules match Lua lines 284-466 exactly (creating check, conflicting check, locked check, coinbase maturity, hash comparison, frozen detection, already-spent idempotency, deletedChildren → PRUNED mapping)
- `spendMulti` batches within a single transaction only (spends grouped by Aerospike key = single txid record) — spec correctly describes this
- `setMined`/`unsetMined` flow matches Lua lines 543-656 (block entry add/remove, unmined_since management, locked clear, creating clear)
- `unspend` flow matches Lua lines 478-540
- `freeze`/`unfreeze`/`reassign` flows match Lua functions
- `setConflicting`/`setLocked`/`preserveUntil` flows match Lua functions
- `setDeleteAtHeight` internal logic matches Lua lines 927-1008 (blockHeightRetention, preserveUntil, conflicting, all-spent evaluation, state transition signaling)
- `incrementSpentExtraRecs` correctly marked as ELIMINATED
- Inline vs external threshold: Go uses `MaxTxSizeInStoreInBytes = 32 * 1024` (32 KB) for externalization decision, which aligns with spec's tiered storage thresholds

**C native module:**
- The `mod-teranode` C module in aerospike-server is a native C port of the Lua UDF, not a separate implementation. All 12 functions present with identical semantics (except minor differences noted below)
- No additional Lua files exist in the aerospike-server repo — the C module completely replaces Lua
- The reference `specs/teranode.lua` is the original Lua implementation; the C module is the production replacement

---

### Gaps found

#### Gap 1: `MarkTransactionsOnLongestChain` not documented as separate operation

**Source:** `stores/utxo/Interface.go` line:
```go
MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error
```

**Details:** This is a separate batch operation from `SetMined` that ONLY manages `unmined_since` without touching block entries. During chain reorganizations:
- `onLongestChain=true`: clears `unmined_since` to 0 (nil)
- `onLongestChain=false`: sets `unmined_since` to current block height

Uses worker-pool pattern with `MaxMinedRoutines` (128) workers and `MaxMinedBatchSize` (1024) batch size. Missing transactions trigger a FATAL error (data corruption indicator).

**Spec impact:** The spec bundles longest-chain status into `setMined`'s `on_longest_chain` parameter (§3.6 step 5), but the Go code has a distinct method called at different points in the block processing flow.

**Which spec section:** Add §3.X for MarkTransactionsOnLongestChain
**Which phase file:** Phase 4 (`04_setmined_path.md`) should explicitly cover this as a separate operation

#### Gap 2: `GetSpend` not documented as an API operation

**Source:** `stores/utxo/Interface.go`:
```go
GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error)
```

**Details:** Returns spending data for a specific UTXO slot. Used for double-spend detection. Returns:
- `SpendResponse { Status int, SpendingData *spend.SpendingData, LockTime uint32 }`

This is a point read of a single UTXO slot plus the record's locktime field. Different from `Get` (which reads whole records) — this reads only the specific slot and one metadata field.

**Spec impact:** The wire protocol has `0x0015 GetSpendBatch` but §3 has no dedicated operations section.

**Which spec section:** Add §3.X for GetSpend between §3.15 and §3.16
**Which phase file:** Phase 6 (`06_remaining_ops.md`) or Phase 10 (`10_wire_protocol.md`) should cover the read-side implementation

#### Gap 3: `PreviousOutputsDecorate` not documented

**Source:** `stores/utxo/Interface.go`:
```go
PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error
```

**Details:** For each input of a transaction, looks up the parent UTXO and retrieves its output data. Used for transaction validation (script execution needs previous outputs). This is a batch read operation that fans out across multiple parent records.

**Spec impact:** Not mentioned in §3 or wire protocol. This may be handled entirely client-side (Go client reads individual records and assembles the data), in which case it doesn't need a server-side operation. But if the server implements it as an optimization, it should be documented.

**Which spec section:** §3.15 (Point Read / Batch Read) should mention this use case
**Which phase file:** May not need a dedicated phase — can be served by batch Get operations

#### Gap 4: `SetConflicting` return values not documented

**Source:** `stores/utxo/Interface.go`:
```go
SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*Spend, []chainhash.Hash, error)
```

**Details:** Returns two additional values:
- `[]*Spend` — spending data from the conflicting transaction's UTXOs (for counter-conflicting tracking)
- `[]chainhash.Hash` — child transaction hashes that need to be cascaded as conflicting

The spec §3.10 only documents the flag-setting behavior. The return values drive the counter-conflicting cascade logic in `process_conflicting.go`.

**Spec impact:** The TeraSlab server may need to return record data as part of the SetConflicting response, or the Go client may need to do a separate read. Design decision needed.

**Which spec section:** §3.10 should document the response format
**Which phase file:** Phase 6 and Phase 10

#### Gap 5: Node block state management not documented

**Source:** `stores/utxo/Interface.go`:
```go
SetBlockHeight(height uint32) error
GetBlockHeight() uint32
SetMedianBlockTime(height uint32) error
GetMedianBlockTime() uint32
GetBlockState() BlockState  // { Height uint32, MedianTime uint32 }
```

**Details:** The Teranode Go client maintains node-level block height and median block time. These are used for:
- Coinbase maturity checking (`spending_height > current_block_height` in spend path)
- Retention calculations (`currentBlockHeight + blockHeightRetention`)
- Unmined transaction age calculations

**Spec impact:** Currently `current_block_height` is passed as a parameter to each operation. The Go interface also supports global state via these methods. The TeraSlab server could either:
1. Accept `current_block_height` per-request (as spec currently describes) — simpler, no server state
2. Accept `SetBlockHeight` updates and use the stored value — matches Go interface

**Which spec section:** §3 should clarify whether block height is per-request or server-state
**Which phase file:** Phase 10 (wire protocol) — the approach affects frame format

#### Gap 6: `GetMeta` separate from `Get`

**Source:** `stores/utxo/Interface.go`:
```go
GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error
```

**Details:** Reads metadata into a pre-allocated `meta.Data` struct. This is a metadata-only read (no UTXO slots, no cold data). Different from `Get` which may read any combination of fields.

**Spec impact:** §3.15 covers this as "if field selection specifies only metadata: pread metadata region only". The optimization is already implied but not explicitly named as a separate method. This is minor — the server always reads metadata for any Get, and field selection is a client-side concern.

**Which spec section:** §3.15 — no change needed, current coverage is sufficient

#### Gap 7: `ProcessExpiredPreservations` not in §3 as API operation

**Source:** `stores/utxo/Interface.go`:
```go
ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error
```

**Details:** Pruner operation that finds records where `preserve_until <= currentHeight`, clears `preserve_until`, and evaluates `setDeleteAtHeight`. The wire protocol has `0x0022 ProcessExpiredPreservations`.

**Spec impact:** Described in §3.16 pruning lifecycle (Phase 3: "Query records where `preserve_until <= current_height`, set `delete_at_height` and clear `preserve_until`") but not as a standalone §3.X operation.

**Which spec section:** §3.16 coverage is adequate — this is an internal pruner operation, not a client-facing API
**Which phase file:** Phase 6 covers this in deletion/pruning

#### Gap 8: Unmined transaction iterators

**Source:** `stores/utxo/Interface.go`:
```go
GetUnminedTxIterator(fullScan bool) (UnminedTxIterator, error)
GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (UnminedTxIterator, error)
```

**Details:** Returns iterator over unmined transactions. `UnminedTxIterator` yields batches of `UnminedTransaction` structs containing: subtree node, txInpoints, createdAt, locked, skip, unminedSince, blockIDs. The `fullScan` parameter controls whether to use secondary index or full scan.

**Spec impact:** The spec's §5.5.2 Unmined Index describes the query capability, and §3.16 describes the pruning lifecycle. But the iterator-based access pattern (streaming results) is not in the wire protocol. The closest is `0x0020 QueryOldUnmined`.

**Which spec section:** §10.3 wire protocol — the QueryOldUnmined opcode should clarify it supports streaming/pagination
**Which phase file:** Phase 10

---

### Discrepancies

#### Discrepancy 1: C module `incrementSpentExtraRecs` semantic difference

**Lua (reference):** Clamps `spentExtraRecs` to `[0, totalExtraRecs]` silently (lines 1172-1181). Comment explains drift can occur during DEVICE_OVERLOAD.

**C module:** Returns errors (`ERR_SPENT_EXTRA_RECS_NEGATIVE`, `ERR_SPENT_EXTRA_RECS_EXCEED`) instead of clamping.

**Resolution:** Not relevant to TeraSlab — `incrementSpentExtraRecs` is ELIMINATED (no pagination). No action needed.

#### Discrepancy 2: C module adds `UPDATE_FAILED` error code

**C module:** Has `ERROR_CODE_UPDATE_FAILED` / `ERR_UPDATE_FAILED` constants. Checks `as_aerospike_rec_update()` return value and returns this error on failure.

**Lua:** Calls `aerospike:update(rec)` without error checking.

**Resolution:** TeraSlab should have I/O failure error handling. The spec's error code table (§3.1) doesn't include a generic I/O or storage error. However, the Go error types include `ErrStorageError` ("storage error"). **Recommend adding a `STORAGE_ERROR` to the spec's error code table.**

#### Discrepancy 3: Spec §3.4 spend validation rule for spendable_height

**Spec says (§3.4 rule 5):** "If status == 0x00 and u32_from_le(spending_data[0..4]) != 0 and >= current_block_height → FROZEN_UNTIL"

**Lua says (spendMulti lines 371-383):** Checks `spendableIn[offset]` map, not the UTXO slot data. The condition is `spendableHeight >= currentBlockHeight` (greater-than-or-equal).

**Resolution:** The spec correctly redesigns this check — in TeraSlab the spendable height is encoded in the slot's spending_data instead of a separate map. The comparison semantics match. The spec should clarify the check is `spendable_height >= current_block_height` (not `>`), matching the Lua behavior. **Minor wording fix needed in §3.4.**

#### Discrepancy 4: `setMined` creating flag handling

**Spec §3.6:** Does not mention clearing the `creating` flag because it's listed as eliminated (§2.2).

**Lua/C:** `setMined` clears the `creating` flag/bin.

**Resolution:** Correct — `creating` is eliminated in TeraSlab. The Lua clears it because the Aerospike design uses it for multi-record 2-phase commit. No action needed.

#### Discrepancy 5: `setLocked` returns `childCount` in Lua but spec doesn't mention it

**Lua `setLocked` (lines 1120-1121):** Returns `totalExtraRecs` as `childCount` in the response.

**Spec §3.11:** Does not mention returning child count.

**Resolution:** `totalExtraRecs` is eliminated in TeraSlab (no pagination). No action needed — the Go client can derive this information from the record if needed.

---

### Spec amendments needed

#### Amendment 1: Add §3.X — MarkTransactionsOnLongestChain

Add a new operation section:

```markdown
### 3.X markOnLongestChain

**Go interface**: `MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain) error`

**Request parameters:**
- `txids: Vec<[u8; 32]>` — batch of transaction IDs
- `on_longest_chain: bool`

**Behavior:**
1. For each txid, acquire per-txid lock
2. If `on_longest_chain == true`: set `unmined_since = 0`
3. If `on_longest_chain == false`: set `unmined_since = current_block_height`
4. Update unmined secondary index accordingly
5. pwrite metadata
6. Release lock

**Atomicity**: Per-record.
**Idempotency**: Setting same value is a no-op.
**Disk regions written**: Metadata only.
```

#### Amendment 2: Add §3.X — GetSpend

Add a new operation section:

```markdown
### 3.X getSpend

**Go interface**: `GetSpend(ctx, spend) (*SpendResponse, error)`

**Request parameters:**
- `txid: [u8; 32]`
- `vout: u32`
- `utxo_hash: [u8; 32]`

**Behavior:**
1. Index lookup: `txid → record_offset`
2. Read metadata at record_offset (for locktime)
3. Read UTXO slot at `record_offset + METADATA_SIZE + vout * 69`
4. Validate hash matches utxo_hash
5. Return status, spending_data (if spent/frozen), locktime

**Response:**
- `status: u8` (0x00=unspent, 0x01=spent, 0x02=pruned, 0xFF=frozen)
- `spending_data: Option<[u8; 36]>` (if status == 0x01 or 0xFF)
- `locktime: u32`

**Disk regions read**: Metadata + single UTXO slot.
```

#### Amendment 3: Add `STORAGE_ERROR` to §3.1 error codes

Add to the error code table:

```
| `STORAGE_ERROR` | Device I/O failure during operation |
```

#### Amendment 4: Fix spendable_height comparison in §3.4

Change rule 5 from:
```
If status == 0x00 and u32_from_le(spending_data[0..4]) != 0 and >= current_block_height → FROZEN_UNTIL
```
To:
```
If status == 0x00 and u32_from_le(spending_data[0..4]) != 0 and u32_from_le(spending_data[0..4]) >= current_block_height → FROZEN_UNTIL
```
(Clarify that >= matches the Lua behavior: `spendableHeight >= currentBlockHeight`)

#### Amendment 5: Document SetConflicting response data

In §3.10, add to the response section:

```markdown
**Response:**
- `status: OK | ERROR`
- `signal: Option<Signal>` — DAHSET
- For each txid: spending data from UTXO slots (needed for counter-conflicting cascade)
```

---

### Phase file amendments needed

#### Phase 4 (`04_setmined_path.md`)

Add a section for `MarkTransactionsOnLongestChain`:
- Separate operation from setMined
- Batch operation on txids
- Only modifies `unmined_since` field — does NOT touch block entries
- Must update unmined secondary index
- Uses same lock striping as setMined
- Wire protocol: `0x000C MarkLongestChainBatch`

#### Phase 6 (`06_remaining_ops.md`)

Add `GetSpend` read operation:
- Point read of single UTXO slot + locktime from metadata
- Hash validation before returning
- Wire protocol: `0x0015 GetSpendBatch`

Add note on `SetConflicting` response:
- Server should return UTXO slot data in the response for counter-conflicting cascade

#### Phase 10 (`10_wire_protocol.md`)

No changes needed — wire protocol already has all required opcodes:
- `0x000C MarkLongestChainBatch` ✓
- `0x0015 GetSpendBatch` ✓
- `0x0022 ProcessExpiredPreservations` ✓

---

### UtxoSlot size decision

- **Resolved**: 4-byte vin matching Bitcoin protocol and Go `SpendingData` struct (`spending_data.go`: 32-byte txid + 4-byte vin, both little-endian)
- **Final UtxoSlot size**: 69 bytes (32 hash + 1 status + 36 spending_data)
- **Status values**: 0x00=unspent, 0x01=spent, 0x02=pruned, 0xFF=frozen

---

### Additional notes

#### C native module replaces Lua entirely

The `mod-teranode` C module in the aerospike-server repo is a complete native C port of `teranode.lua`. The Aerospike server's `udf.c` has been modified to route UDF calls to the C module when the filename contains "teranode". No Lua files remain in the repo. The C module is approximately 2,650 lines (vs ~1,200 lines of Lua) due to explicit memory management and error handling.

Key C module optimizations that TeraSlab inherits by design:
- Native `memcmp` for hash comparison (vs Lua byte-by-byte)
- Integer comparison for frozen detection (vs Lua byte-by-byte loop)
- Single-allocation spend vs Lua's per-spend table allocations
- Direct record access vs Lua's deserialization/serialization cycle

#### Spending data in Lua vs spec

The Lua uses variable-size UTXOs: unspent = 32 bytes (hash only), spent = 68 bytes (hash + spending_data). The spec uses fixed 69-byte slots with a status byte. This is the fundamental design improvement — fixed-size slots enable in-place mutation. The Lua's variable-size approach forces Aerospike's copy-on-write.

#### ExternalizeAllTransactions setting

The Go config has `externalizeAllTransactions` (default false) that forces ALL transactions to blob storage. The spec doesn't mention this setting. For TeraSlab, this could be useful for testing or for deployments that want to minimize NVMe usage. Low priority — can be added as a configuration option without spec changes.
