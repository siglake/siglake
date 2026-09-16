//! The budgets the real `siglake` binary resolves for its two roles (#4082).
//!
//! The unit tests cover the classification and the arithmetic; this covers the
//! wiring, which is the part that was missing: every process except
//! `siglake-query-server` reached its text-index caches through the fork's
//! env-or-constant fallback, so the compactor pod held ceilings for 1 GiB of
//! parsed indexes and 256 MiB of blobs inside a 1Gi limit.
//!
//! The compactor has no query memory pool to read the subtraction off — nothing
//! in a maintenance process builds one (no DataFusion in `siglake-compactor`,
//! and a Tier-2 rebuild counts from footers and raw pages) — so the startup log
//! line is the observable, and it is read off a real run of the shipped binary
//! rather than a re-derivation.

use std::path::Path;
use std::process::Command;

/// Run the shipped binary with the environment cleared of `SIGLAKE_*`, so the
/// run is the same on every developer's shell and the budgets below are the
/// role's own answer rather than an operator override. `RUST_LOG` is unset too:
/// the CLI's default filter is `info,siglake=debug`, which is what carries the
/// line this reads.
fn run(data_dir: &Path, args: &[&str]) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_siglake"));
    cmd.arg("--data-dir").arg(data_dir).args(args);
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("SIGLAKE_") {
            cmd.env_remove(k);
        }
    }
    cmd.env_remove("RUST_LOG");
    let out = cmd.output().expect("spawn siglake");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        out.status.success(),
        "siglake {args:?} exited {:?}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status.code()
    );
    let line = strip_ansi(&stderr)
        .lines()
        .find(|line| line.contains("cache budgets resolved for this role"))
        .map(str::to_string);
    line.unwrap_or_else(|| {
        panic!("siglake {args:?} logged no resolved cache budgets\n--- stderr ---\n{stderr}")
    })
}

/// `field=value` out of a tracing line, ANSI already stripped.
fn field(line: &str, name: &str) -> String {
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(&format!("{name}=")))
        .unwrap_or_else(|| panic!("no {name}= in {line:?}"))
        .to_string()
}

fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\x1b' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'[') {
            chars.next();
            for c in chars.by_ref() {
                if ('@'..='~').contains(&c) {
                    break;
                }
            }
        }
    }
    out
}

/// The packaged compactor pod's own startup: no text-index ceilings at all, and
/// the byte-range object cache still off, which is what an unconfigured process
/// actually holds.
#[test]
fn the_compactor_resolves_zero_cache_budgets() {
    let tmp = tempfile::tempdir().unwrap();
    let line = run(tmp.path(), &["compactor", "--once"]);
    assert_eq!(field(&line, "role"), "Maintenance", "{line}");
    assert_eq!(field(&line, "parsed_index_cache_bytes"), "0", "{line}");
    assert_eq!(field(&line, "puffin_blob_cache_bytes"), "0", "{line}");
    assert_eq!(field(&line, "object_cache_bytes"), "0", "{line}");
}

/// Same binary, a role that can fill those caches: `sql-direct` registers the
/// Iceberg tables here and runs the query in process, so it resolves what the
/// query server would at this machine's limit. Cross-checked against the
/// resolver rather than a constant, because the answer depends on whether this
/// process has a cgroup limit to read.
#[test]
fn sql_direct_resolves_the_query_servers_text_index_budgets() {
    let tmp = tempfile::tempdir().unwrap();
    let line = run(tmp.path(), &["sql-direct", "--query", "SELECT 1"]);
    assert_eq!(field(&line, "role"), "InProcessQuery", "{line}");

    let expected = siglake_storage::resolve_text_index_cache_config(
        siglake_storage::iceberg::cgroup_memory_limit_bytes(),
        None,
        None,
    );
    assert_eq!(
        field(&line, "parsed_index_cache_bytes"),
        expected.parsed_index_max_bytes.to_string(),
        "{line}"
    );
    assert_eq!(
        field(&line, "puffin_blob_cache_bytes"),
        expected.puffin_blob_max_bytes.to_string(),
        "{line}"
    );
    // Still not a read cache this role turns on for itself.
    assert_eq!(field(&line, "object_cache_bytes"), "0", "{line}");
}
