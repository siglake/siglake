//! `siglake rebuild-time-aggregates` run as a real process against a local
//! warehouse, asserting the text an operator reads. The storage behaviour is
//! pinned by `siglake-storage/tests/storage/pre_coverage_time_agg.rs`; this
//! covers the report `main.rs` layers on top, which nothing else runs.
//!
//! The table is built in-process and its inline aggregate is put back into the
//! pre-#2920 shape — the `coverage` keys deleted, every count left alone, which
//! IS the legacy encoding because both fields are `skip_serializing_if`. The
//! binary is then spawned via `CARGO_BIN_EXE_siglake` so the clap surface, the
//! env defaults and `open_iceberg`'s local-path routing are all exercised.
//!
//! Two runs, because the first one's state is the second one's setup: the
//! repair, then the reported no-op that proves re-running is safe.

use std::path::Path;
use std::process::Command;

use chrono::{Duration, TimeZone, Utc};
use siglake_core::index_config::IndexConfig;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, SnapshotAggregates};

const NAMESPACE: &str = "siglake";
const TABLE: &str = "tagged";
const APPENDS: i64 = 4;
const PER_APPEND: i64 = 300;

fn config() -> IndexConfig {
    IndexConfig {
        index_id: TABLE.into(),
        ..IndexConfig::builtin_events()
    }
}

/// Four commits, rows a quarter-hour apart so the span needs many hourly
/// buckets and each file covers more than one — the shape that exercises the
/// decode arm rather than the footer shortcut.
async fn seed(warehouse: &Path) {
    let ice = IcebergContext::open(warehouse).await.unwrap();
    let config = config();
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident(TABLE);
    let bloom_cols: Vec<String> = config.doc_mapping.tag_fields.to_vec();
    let base = Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap();
    let levels = ["info", "warn", "debug", "error"];
    for a in 0..APPENDS {
        let mut events = Vec::with_capacity(PER_APPEND as usize);
        for i in 0..PER_APPEND {
            let k = a * PER_APPEND + i;
            let mut e = Event::now(format!("row {k}"));
            e.timestamp = base + Duration::minutes(k * 15);
            e.host = format!("host-{}", k % 3);
            e.sourcetype = levels[(k % 4) as usize].to_string();
            events.push(e);
        }
        let batch = siglake_core::events_to_record_batch(&events).unwrap();
        let mapped = siglake_core::mapping::map_carrier_batch(&batch, &config).unwrap();
        let bloom: Vec<&str> = bloom_cols.iter().map(String::as_str).collect();
        ice.append_to_table(&ident, mapped, &bloom).await.unwrap();
    }
}

/// The one incarnation directory holding the aggregate artifacts, found rather
/// than hardcoded so the test survives a layout move.
fn side_object_path(warehouse: &Path) -> std::path::PathBuf {
    fn find(dir: &Path) -> Option<std::path::PathBuf> {
        for e in std::fs::read_dir(dir).ok()? {
            let p = e.ok()?.path();
            if !p.is_dir() {
                continue;
            }
            if p.join("siglake-aggregates.json").exists() {
                return Some(p.join("siglake-aggregates.json"));
            }
            if let Some(found) = find(&p) {
                return Some(found);
            }
        }
        None
    }
    find(warehouse).expect("aggregate object")
}

fn strip_coverage(warehouse: &Path) {
    let path = side_object_path(warehouse);
    let mut doc: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let obj = doc.as_object_mut().expect("a JSON object");
    assert!(
        obj.remove("coverage").is_some(),
        "the seeded object must carry a coverage edge, else this strips nothing"
    );
    obj.remove("coverage_links");
    std::fs::write(&path, serde_json::to_vec(&doc).unwrap()).unwrap();
}

fn read_side(warehouse: &Path) -> SnapshotAggregates {
    serde_json::from_slice(&std::fs::read(side_object_path(warehouse)).unwrap()).unwrap()
}

/// Run the real binary with a clean `SIGLAKE_*` environment, so the run is the
/// same on every shell. `RUST_LOG` is unset too: the child logs under the CLI's
/// default filter and the subscriber writes to stderr, which is what the
/// stdout-purity assertion pins.
fn run(data_dir: &Path) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_siglake"));
    cmd.arg("--data-dir")
        .arg(data_dir)
        .arg("rebuild-time-aggregates")
        .arg("--namespace")
        .arg(NAMESPACE)
        .arg("--table")
        .arg(TABLE);
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
        "rebuild-time-aggregates exited {:?}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status.code()
    );
    if let Some(line) = stdout.lines().find(|l| is_tracing_line(l)) {
        panic!("a log line reached stdout: {line:?}\n--- stdout ---\n{stdout}");
    }
    stdout
}

/// A `tracing_subscriber::fmt` line: RFC 3339 stamp, then the level. Shares the
/// shape with `rebuild_group_counts_cli`, which pins the detector itself.
fn is_tracing_line(line: &str) -> bool {
    const LEVELS: [&str; 5] = ["ERROR ", " WARN ", " INFO ", "DEBUG ", "TRACE "];
    let plain: String = {
        let mut out = String::with_capacity(line.len());
        let mut chars = line.chars().peekable();
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
    };
    let Some((stamp, rest)) = plain.split_once(' ') else {
        return false;
    };
    stamp.len() >= 20
        && stamp.starts_with(|c: char| c.is_ascii_digit())
        && stamp.ends_with('Z')
        && LEVELS.iter().any(|lvl| rest.starts_with(lvl))
}

fn line_for<'a>(stdout: &'a str, component: &str) -> &'a str {
    let prefix = format!("  {component} ");
    stdout
        .lines()
        .find(|l| l.starts_with(&prefix))
        .unwrap_or_else(|| panic!("no report line for `{component}` in:\n{stdout}"))
}

#[tokio::test]
async fn rebuild_time_aggregates_reports_each_component_then_no_ops() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    // The default-tenant local path `open_iceberg` builds: SQLite + local FS
    // under `{data_dir}/warehouse`.
    let warehouse = data_dir.join("warehouse");
    seed(&warehouse).await;
    let rows = APPENDS * PER_APPEND;
    strip_coverage(&warehouse);

    let stdout = run(&data_dir);
    assert!(
        stdout.contains(&format!("table rows: {rows}")),
        "the report must name the row count every component was held to:\n{stdout}"
    );
    let buckets = line_for(&stdout, "time_buckets");
    assert!(
        buckets.contains(&format!("rows={rows}")) && buckets.contains("restored"),
        "time_buckets line does not report a restore: {buckets:?}\n{stdout}"
    );
    let sourcetype = line_for(&stdout, "time_group_counts.sourcetype");
    assert!(
        sourcetype.contains(&format!("rows={rows}")) && sourcetype.contains("restored"),
        "the maintained rollup column does not report a restore: {sourcetype:?}\n{stdout}"
    );
    assert!(
        stdout.contains("group counts were dropped"),
        "the report must say the inline group counts were dropped — an operator \
         reading only the restores would not know:\n{stdout}"
    );
    assert_eq!(
        read_side(&warehouse).group_counts,
        None,
        "the report claimed a drop that did not happen"
    );
    assert!(
        read_side(&warehouse).coverage.is_some(),
        "no coverage edge was published"
    );

    // Second run, against its own output.
    let again = run(&data_dir);
    assert!(
        again.contains("already proves coverage") && again.contains("nothing to rebuild"),
        "a second run must report the no-op rather than repeat the work:\n{again}"
    );
}
