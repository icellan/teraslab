# Provenance of `specs/teranode.lua`

`specs/teranode.lua` is a copy of the UTXO-store Lua UDF from upstream Teranode,
**de-identified**: the storage-engine object identifier has been renamed (to
`store`) so no external product name appears, with the line count preserved. It
is the authoritative reference for the UTXO semantics TeraSlab reimplements
(referenced by `CLAUDE.md`, `specs/BSV_UTXO_STORE_SPEC.md`, and several
`src/ops/*.rs` doc comments by line number).

| | |
|---|---|
| Upstream repo | `git@github.com:bsv-blockchain/teranode.git` |
| Upstream path | upstream Teranode UTXO-store UDF |
| Upstream commit | `88efbced3b6eed68da44267b827d6c8b38ad853f` (2026-06-05) |
| Line count | 1280 (unchanged) |
| Copied | 2026-06-11 |

The only edit to the copy is the storage-engine object rename; line numbers are
preserved so the citations in the spec and code comments still resolve against a
version-pinned baseline. If the upstream UDF changes and the parity baseline must
move, re-copy the new revision (applying the same rename) and update the commit
hash above. TeraSlab's UTXO ops are implemented for function-by-function parity
against this file (see the `src/ops/*.rs` doc comments that cite it by line
number).
