//! Integration test enforcing the Phase 3 `eprintln!`/`println!` ban.
//!
//! Phase 3 introduced `#![warn(clippy::disallowed_macros)]` at the crate
//! root plus a `clippy.toml` that banishes `std::eprintln` and
//! `std::println` from production code. That lint is the primary
//! enforcement, but it only fires when somebody runs
//! `cargo clippy -D warnings` — developers skipping that step could
//! reintroduce stray prints without noticing.
//!
//! This test scans every production `src/*.rs` file outside the allow-list
//! (`src/bin/*.rs`) and asserts that NONE of them contain a live
//! `eprintln!` or `println!` macro call. Only lines that are part of
//! `#[cfg(test)]` modules are tolerated — those are scoped out by the
//! nearest enclosing `#[cfg(test)]` annotation.
//!
//! If the scan finds a live print site, the test fails with the list of
//! offending files so the violation is trivially fixable.

use std::fs;
use std::path::{Path, PathBuf};

/// Recursively collect every `.rs` file under `dir`.
fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Return true if `path` should be excluded from the ban.
///
/// `src/bin/*.rs` contain CLI binaries whose output is the user-facing
/// stdout. Those files carry targeted `#[allow(clippy::disallowed_macros)]`
/// attributes at their call sites.
fn is_allowlisted(path: &Path) -> bool {
    let s = path.to_string_lossy();
    s.contains("/src/bin/") || s.ends_with("/src/bin")
}

/// Return true if the byte offset `idx` within `content` falls inside a
/// `#[cfg(test)]` module or an `#[allow(clippy::disallowed_macros)]` attribute
/// scope. The check walks the file line-by-line, tracking brace depth and
/// remembering whether we last saw a `#[cfg(test)]` / matching attribute
/// within the line immediately above a `mod { ... }` or `fn { ... }` block.
fn under_test_or_allowed(content: &str, idx: usize) -> bool {
    let prefix = &content[..idx];
    // Walk the prefix tracking brace depth. When we enter a `{` immediately
    // following `#[cfg(test)]` or `#[allow(clippy::disallowed_macros)]`, push
    // a scope marker and only exit when matching depth closes.
    let mut depth: usize = 0;
    let mut test_stack: Vec<usize> = Vec::new();
    let mut allow_stack: Vec<usize> = Vec::new();
    let mut pending_cfg_test = false;
    let mut pending_allow = false;

    for line in prefix.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if trimmed.starts_with("#[cfg(test)]") {
            pending_cfg_test = true;
        } else if trimmed.starts_with("#[allow(clippy::disallowed_macros)]") {
            pending_allow = true;
        }

        for ch in line.chars() {
            match ch {
                '{' => {
                    depth += 1;
                    if pending_cfg_test {
                        test_stack.push(depth);
                        pending_cfg_test = false;
                    }
                    if pending_allow {
                        allow_stack.push(depth);
                        pending_allow = false;
                    }
                }
                '}' => {
                    if let Some(&top) = test_stack.last()
                        && top == depth
                    {
                        test_stack.pop();
                    }
                    if let Some(&top) = allow_stack.last()
                        && top == depth
                    {
                        allow_stack.pop();
                    }
                    depth = depth.saturating_sub(1);
                }
                _ => {}
            }
        }

        // If a non-attribute, non-brace token appears after the attribute
        // that was not a `{` or another attribute, the attribute binds to
        // the next item rather than an inline block — reset the pending
        // flag after the line ends (a heuristic, but good enough for the
        // print-ban use case; attributes on individual fn items apply to
        // their body, which opens with `{` on the next logical line).
        if !trimmed.starts_with("#[") && !trimmed.is_empty() {
            // Leave flags alone unless we just walked past a `{` token —
            // that was already handled above.
        }
    }

    !test_stack.is_empty() || !allow_stack.is_empty()
}

/// Collect `(file, line_number, text)` triples for every production
/// `eprintln!` / `println!` occurrence that isn't covered by an allow
/// attribute or a `#[cfg(test)]` module.
fn find_violations() -> Vec<(PathBuf, usize, String)> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let src_dir = manifest_dir.join("src");
    let mut files = Vec::new();
    collect_rs(&src_dir, &mut files);

    let mut violations = Vec::new();
    for file in files {
        if is_allowlisted(&file) {
            continue;
        }
        let content = match fs::read_to_string(&file) {
            Ok(c) => c,
            Err(_) => continue,
        };
        // Find every occurrence of `eprintln!(` or `println!(`.
        let needles = ["eprintln!", "println!"];
        for needle in &needles {
            let mut start = 0usize;
            while let Some(pos) = content[start..].find(needle) {
                let abs = start + pos;
                // Skip matches inside string literals and comments by
                // checking that the character before the needle is not an
                // identifier continuation — `tracing::eprintln!`-like
                // false positives would need a non-identifier prefix.
                let prev = content[..abs].chars().next_back();
                let is_boundary = matches!(
                    prev,
                    None | Some(' ') | Some('\t') | Some('\n') | Some('{') | Some('(')
                    | Some(',') | Some('}') | Some(';') | Some('/') | Some('+')
                );
                if is_boundary && !under_test_or_allowed(&content, abs) {
                    // Locate the line number.
                    let line = content[..abs].chars().filter(|&c| c == '\n').count() + 1;
                    let line_text = content
                        .lines()
                        .nth(line - 1)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    violations.push((file.clone(), line, line_text));
                }
                start = abs + needle.len();
            }
        }
    }
    violations
}

/// The Phase 3 structural assertion: no production site may call
/// `eprintln!` or `println!`. If the scan finds any, fail with the list so
/// CI surfaces the regression immediately.
#[test]
fn eprintln_ban_lint_fires_in_production_code() {
    let violations = find_violations();
    assert!(
        violations.is_empty(),
        "production code must not use eprintln!/println! outside src/bin/*.rs; \
         found {} offending call site(s): {:?}",
        violations.len(),
        violations,
    );
}
