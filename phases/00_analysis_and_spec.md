# Phase 0: Validate specification against source repos

**Status:** partial — spec amendments shipped and all of phases 1-13 were implemented against the refined `BSV_UTXO_STORE_SPEC.md` / `BSV_UTXO_STORE_RUST_CRATES.md`; the standalone `SPEC_VALIDATION_REPORT.md` deliverable was rolled into the per-phase docs rather than produced as a separate artifact.

## Goal

The formal specification (`BSV_UTXO_STORE_SPEC.md`) and crate recommendations (`BSV_UTXO_STORE_RUST_CRATES.md`) already exist and have been refined. Before implementation begins, validate them against the actual Teranode source code and the original UTXO store implementation to catch any gaps, stale assumptions, or missing operations.

NO implementation code is written in this phase — only analysis and spec amendments.

## Dependencies

None — this is the first thing you do.

## Preparation

Read these documents in order:

1. `specs/SPEC_BRIEFING.md` — architecture analysis from the design session
2. `BSV_UTXO_STORE_SPEC.md` — the formal specification (already refined)
3. `BSV_UTXO_STORE_RUST_CRATES.md` — crate recommendations (already refined)
4. `specs/teranode.lua` — the current Lua UDF

Clone the repository:

```bash
git clone https://github.com/bsv-blockchain/teranode.git repos/teranode
```

## What to validate

### In `repos/teranode`

Find the UTXO store package. Use `grep -r "utxostore\|UTXOStore\|UtxoStore" --include="*.go" -l` to locate it.

#### A. Interface completeness

Find the Go interface that defines all UTXO store operations. For EVERY method in the interface:

- [ ] Is it documented in `BSV_UTXO_STORE_SPEC.md`?
- [ ] Do the parameter types match what the spec says?
- [ ] Are the return types and error conditions covered?

If any method is missing from the spec, document it and flag it for addition.

#### B. Field completeness

Find `fields.go` or equivalent. For EVERY field constant:

- [ ] Is it in the spec's data model?
- [ ] Does the type match?
- [ ] Are the operations that read/write it correctly listed?

If any field is missing, document it and flag it.

#### C. Key structure confirmation

- [ ] How are record keys constructed in the original implementation? (txid only? txid + index?)
- [ ] What is `utxoBatchSize` and its default?
- [ ] Does the spec correctly describe the key scheme for TeraSlab?

#### D. Operation flow verification

For each hot-path operation, trace the Go code to understand:

**Spend path:**
- [ ] When is Lua UDF used vs `spend_expressions.go`?
- [ ] What validation happens Go-side before the UDF call?
- [ ] Does `spendMulti` batch across transactions or only within one?
- [ ] Confirm the spec covers all error propagation paths

**SetMined path:**
- [ ] Call pattern during block processing (per-tx? per-block? batched?)
- [ ] When is `unsetMined` triggered? (block reorg only?)
- [ ] Confirm longest chain management matches spec

**Creation path:**
- [ ] What is the inline vs external size threshold?
- [ ] How does `createBatch` work for multi-record txs?
- [ ] Confirm the `creating` flag is eliminated per spec (no multi-record creation)

#### E. Pruning verification

Find the pruner package:
- [ ] How does `deleteAtHeight` trigger actual deletion?
- [ ] Confirm PRUNED status (0x02) on UtxoSlot replaces `deletedChildren` tracking per spec
- [ ] How are external blobs cleaned up on deletion?

#### F. Configuration

From `settings.conf` and Go config code:
- [ ] Extract `utxoBatchSize` default
- [ ] Extract namespace config (replication factor, storage settings)
- [ ] Extract connection pool sizes, timeouts, retry policies
- [ ] Confirm spec's configuration section covers these

### Lua UDF comparison

#### G. Lua UDF verification

- [ ] Compare the Lua UDF source against `specs/teranode.lua` — note any differences
- [ ] Are there additional Lua files beyond `teranode.lua`?
- [ ] Are there any C extensions or custom modifications?

### Cross-reference with phase files

For every finding, check whether the relevant implementation phase (01-12) covers it:

- [ ] Any operation in the Go interface not covered by phases 03-06?
- [ ] Any field not accounted for in the Phase 1 record layout?
- [ ] Any error code or validation rule not listed in Phase 3 acceptance criteria?
- [ ] Any configuration parameter that affects Phase 10 wire protocol design?

## Output

Produce a validation report as `SPEC_VALIDATION_REPORT.md` in the project root:

```markdown
# Spec Validation Report

## Date: [date]
## Repos analyzed: teranode@[commit]

## Findings

### Confirmed correct
- [List items in the spec that match the source code exactly]

### Gaps found
- [List anything in the Go code not covered by the spec]
- For each gap: which spec section and which phase file need updating

### Discrepancies
- [List anything where spec says X but code does Y]
- For each: recommended resolution

### Spec amendments needed
- [Concrete changes to make to BSV_UTXO_STORE_SPEC.md]

### Phase file amendments needed  
- [Concrete changes to make to specific phase files]

### UtxoSlot size decision
- **Resolved**: 4-byte vin matching Bitcoin protocol and Go SpendingData struct
- **Final UtxoSlot size**: 69 bytes (32 hash + 1 status + 36 spending_data)
- **Status values**: 0x00=unspent, 0x01=spent, 0x02=pruned, 0xFF=frozen
```

## Acceptance criteria

```
- [ ] Both repos cloned and analyzed
- [ ] SPEC_VALIDATION_REPORT.md exists in project root
- [ ] Every method in the Go UTXO store interface is accounted for
- [ ] Every field constant in the Go code is accounted for
- [ ] UtxoSlot byte size is finalized based on actual vout encoding
- [ ] Any gaps or discrepancies have proposed resolutions
- [ ] If amendments to BSV_UTXO_STORE_SPEC.md are needed, they are listed explicitly
- [ ] If amendments to phase files are needed, they are listed explicitly with the phase number
```

## What happens next

Review the validation report. Apply any amendments to the spec and phase files. Then proceed to Phase 1.
