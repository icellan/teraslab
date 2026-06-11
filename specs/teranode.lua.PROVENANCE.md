# Provenance of `specs/teranode.lua`

`specs/teranode.lua` is a **verbatim, unmodified copy** of the Aerospike UTXO-store
Lua UDF from upstream Teranode. It is the authoritative reference for the UTXO
semantics TeraSlab reimplements (referenced by `CLAUDE.md`, `specs/BSV_UTXO_STORE_SPEC.md`,
and several `src/ops/*.rs` doc comments by line number).

| | |
|---|---|
| Upstream repo | `git@github.com:bsv-blockchain/teranode.git` |
| Upstream path | `stores/utxo/aerospike/teranode.lua` |
| Upstream commit | `88efbced3b6eed68da44267b827d6c8b38ad853f` (2026-06-05) |
| Line count | 1280 |
| Copied | 2026-06-11 |

The file is checked in unchanged so the line-number citations in the spec and code
comments resolve against a version-pinned baseline. Do **not** edit the copy; if the
upstream UDF changes and the parity baseline must move, re-copy the new revision and
update the commit hash above. The function-by-function parity audit against this exact
file lives in `audit/raw/lua-parity.md`.
