# TeraSlab wire-protocol fuzzing (N-04 / LMNH-17)

This directory is a standalone [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz)
crate targeting the untrusted network boundary: request/response frame
header parsing (`src/protocol/frame.rs`) and every per-opcode
`decode_*_checked` payload decoder (`src/protocol/codec.rs`).

The contract under fuzz is **"Ok or typed error, never panic"** — the
target simply calls every decoder on the input bytes; libFuzzer flags any
panic, abort, OOM, or sanitizer fault.

It is intentionally **detached from the parent package** (empty
`[workspace]` table in `Cargo.toml`), so `cargo check` / `cargo test`
/ `cargo clippy` at the repo root never build it. The CI-enforced,
deterministic half of this coverage lives in `tests/wire_fuzz_smoke.rs`
and runs in the default test suite.

## Running

Requires a nightly toolchain (libFuzzer instrumentation):

```bash
cargo install cargo-fuzz          # once
cd <repo root>
cargo +nightly fuzz run decode_request
```

Useful variants:

```bash
# Time-boxed run (e.g. 10 minutes):
cargo +nightly fuzz run decode_request -- -max_total_time=600

# Parallel jobs:
cargo +nightly fuzz run decode_request --jobs 8

# Reproduce a crash artifact:
cargo +nightly fuzz run decode_request fuzz/artifacts/decode_request/<file>

# Minimize a crash input:
cargo +nightly fuzz tmin decode_request fuzz/artifacts/decode_request/<file>

# Coverage report (needs llvm-tools-preview):
cargo +nightly fuzz coverage decode_request
```

## Seeding the corpus

A good starting corpus is valid encoded frames/payloads. The
`tests/wire_fuzz_smoke.rs` generators show how to build them with the
`encode_*` functions; dump any of those byte vectors into
`fuzz/corpus/decode_request/` to give the fuzzer structured seeds.

## Keeping the target in sync

When a new `decode_*_checked` function is added to
`src/protocol/codec.rs`, add it to BOTH:

- `fuzz/fuzz_targets/decode_request.rs` (this crate), and
- `tests/wire_fuzz_smoke.rs` (`feed_all` + the `expected_decoders` list —
  that test fails if the harness decoder set drifts).
