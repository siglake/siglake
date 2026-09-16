//! `siglake rebuild-group-counts` run as a real process against a local
//! warehouse, asserting the text an operator reads: the per-column
//! REPAIRED/ADMITTED origin lines, the `--admit-typed-columns` hint, and the
//! closing summaries that separate what a flag fixes from what only a rewrite
//! fixes. The storage-level behaviour is pinned by
//! `siglake-storage/tests/rebuild_admits_typed_columns.rs`; this covers the
//! report `main.rs` layers on top, which nothing else runs.
//!
//! The table is built in-process, its aggregates are put back into the
//! pre-2c597f8 shape (typed columns stripped, data files untouched), and the
//! binary is spawned via `CARGO_BIN_EXE_siglake` so the clap surface, the env
//! defaults and `open_iceberg`'s local-path routing are all exercised. The
//! child gets a clean `SIGLAKE_*` environment; nothing in the test process is
//! set, so the default typed cap (1024) is what the report shows.
//!
//! Three runs, in order, because each one's state is the next one's setup:
//! 1. no flag — repairs `method`, names the columns the flag would add;
//! 2. `--admit-typed-columns` — three admitted columns, three outcomes
//!    (written / over the cap / unreadable), each with its closing summary;
//! 3. no flag again, after the snapshot's `total-records` is blanked — the
//!    row count is unknown, so every repaired column is "still short", and
//!    `status` now shows up as REPAIRED because run 2 really did admit it.

use std::path::{Path, PathBuf};
use std::process::Command;

use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_storage::iceberg::{
    GroupCountDelta, IcebergContext, SnapshotAggregates, WideGroupCounts,
};

const ROWS: i64 = 3_000;
const BATCHES: i64 = 4;
/// Distinct `size` values per batch: under the per-file footer cap and the
/// typed cap (both 1024), so every file carries a footer for it — but disjoint
/// across batches, so the WHOLE-TABLE count (2,000) is over the typed cap.
const SIZES_PER_BATCH: i64 = 500;
/// Distinct `bytes` values per batch: over the per-file cap, so no file has a
/// footer for it, and an Int64 column is not something the raw-page decode
/// reads. Nothing can serve it.
const BYTES_PER_BATCH: i64 = 1_500;

const NAMESPACE: &str = "siglake";
const TABLE: &str = "prefix";

fn field(name: &str, field_type: FieldType) -> FieldMapping {
    FieldMapping {
        name: name.to_string(),
        field_type,
        required: name == "timestamp",
    }
}

/// http_logs in miniature: one declared text dim and three typed columns of
/// three different widths.
fn config() -> IndexConfig {
    IndexConfig {
        index_id: TABLE.to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                field("timestamp", FieldType::Datetime),
                field(
                    "method",
                    FieldType::Text {
                        tokenizer: Some("raw".into()),
                    },
                ),
                field("status", FieldType::Long),
                field("size", FieldType::Long),
                field("bytes", FieldType::Long),
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec!["method".to_string()],
            default_search_fields: vec![],
        },
        retention: None,
        index_at_flush: None,
    }
}

fn batch(config: &IndexConfig, b: i64) -> RecordBatch {
    // Microseconds: `timestamp` is a microsecond `timestamptz` on every siglake
    // schema, and one row per microsecond keeps the values distinct.
    let base = 1_700_000_000_000_000i64 + b * 100_000;
    RecordBatch::try_new(
        config.to_arrow_schema(),
        vec![
            std::sync::Arc::new(
                TimestampMicrosecondArray::from(
                    (0..ROWS).map(|i| Some(base + i)).collect::<Vec<_>>(),
                )
                .with_timezone("+00:00"),
            ),
            std::sync::Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|i| if i % 2 == 0 { "GET" } else { "POST" })
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(Int64Array::from(
                (0..ROWS)
                    .map(|i| [200i64, 200, 304, 404, 500, 302][(i % 6) as usize])
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(Int64Array::from(
                (0..ROWS)
                    .map(|i| b * SIZES_PER_BATCH + (i % SIZES_PER_BATCH))
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(Int64Array::from(
                (0..ROWS)
                    .map(|i| b * BYTES_PER_BATCH + (i % BYTES_PER_BATCH))
                    .collect::<Vec<_>>(),
            )),
            // Dynamic mapping mode appends the WS-7 residual `attributes` column.
            std::sync::Arc::new(StringArray::from(vec![None::<&str>; ROWS as usize])),
        ],
    )
    .unwrap()
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

/// Files under `warehouse` that belong to `table`.
fn table_files(warehouse: &Path, table: &str) -> Vec<PathBuf> {
    walk(warehouse)
        .into_iter()
        .filter(|p| p.to_string_lossy().contains(&format!("/{table}/")))
        .collect()
}

/// Put `table`'s aggregates back into the pre-2c597f8 shape: `columns` gone
/// from the inline object, the wide object and every un-absorbed delta, while
/// the data files — and their footers — are untouched. Returns how many
/// objects it edited, so a layout change cannot silently turn this into a
/// no-op.
fn strip_columns_from_aggregates(warehouse: &Path, table: &str, columns: &[&str]) -> usize {
    let mut edited = 0;
    for path in table_files(warehouse, table) {
        let s = path.to_string_lossy().to_string();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let bytes = std::fs::read(&path).unwrap();
        let out = if name == "siglake-aggregates.json" {
            let mut aggs: SnapshotAggregates = serde_json::from_slice(&bytes).unwrap();
            if let Some(gc) = aggs.group_counts.as_mut() {
                for c in columns {
                    gc.columns.remove(*c);
                }
            }
            serde_json::to_vec(&aggs).unwrap()
        } else if name == "siglake-agg-wide.json" {
            let mut wide: WideGroupCounts = serde_json::from_slice(&bytes).unwrap();
            if let Some(mut all) = wide.decode_all() {
                for c in columns {
                    all.columns.remove(*c);
                }
                wide.set_group_counts(Some(all));
            }
            serde_json::to_vec(&wide).unwrap()
        } else if s.contains("/siglake-agg-deltas/") {
            let mut delta: GroupCountDelta = serde_json::from_slice(&bytes).unwrap();
            if let Some(gc) = delta.group_counts.as_mut() {
                for c in columns {
                    gc.columns.remove(*c);
                }
            }
            serde_json::to_vec(&delta).unwrap()
        } else {
            continue;
        };
        std::fs::write(&path, out).unwrap();
        edited += 1;
    }
    edited
}

/// Remove `total-records` from every snapshot summary in `table`'s metadata
/// files, so the table's row count is unknown — one of the two causes the
/// "still short" summary names. Every reader of that key treats it as optional.
/// Returns how many summaries lost the key.
fn blank_total_records(warehouse: &Path, table: &str) -> usize {
    let mut blanked = 0;
    for path in table_files(warehouse, table) {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if !name.ends_with(".metadata.json") {
            continue;
        }
        let mut meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        for snap in meta["snapshots"].as_array_mut().into_iter().flatten() {
            if let Some(summary) = snap["summary"].as_object_mut() {
                if summary.remove("total-records").is_some() {
                    blanked += 1;
                }
            }
        }
        std::fs::write(&path, serde_json::to_vec(&meta).unwrap()).unwrap();
    }
    blanked
}

/// Run the real binary. The child sees no `SIGLAKE_*` at all — the CLI reads
/// `SIGLAKE_WAREHOUSE_URL`, `SIGLAKE_CATALOG_URI` and `SIGLAKE_TENANT_NAMESPACE`
/// as flag defaults, and the storage layer reads the cardinality caps — so the
/// run is the same on every developer's shell. `RUST_LOG` is unset too, so the
/// child logs under the CLI's default `info,siglake=debug` filter; the
/// subscriber writes to stderr, and the assertion below pins that stdout holds
/// only the report.
fn run_rebuild(data_dir: &Path, admit: bool) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_siglake"));
    cmd.arg("--data-dir")
        .arg(data_dir)
        .arg("rebuild-group-counts")
        .arg("--namespace")
        .arg(NAMESPACE)
        .arg("--table")
        .arg(TABLE);
    if admit {
        cmd.arg("--admit-typed-columns");
    }
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
        "rebuild-group-counts (admit={admit}) exited {:?}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        out.status.code()
    );
    if let Some(line) = stdout.lines().find(|l| is_tracing_line(l)) {
        panic!(
            "rebuild-group-counts (admit={admit}) wrote a log line to stdout: {line:?}\n--- stdout ---\n{stdout}"
        );
    }
    stdout
}

/// A `tracing_subscriber::fmt` line: RFC 3339 timestamp, a space, the level
/// right-aligned to five columns (` INFO`, `DEBUG`), a space, then the target.
/// The subscriber colours the line with ANSI escapes even into a pipe unless
/// `NO_COLOR` is set, so they are stripped first. Report lines are indented by
/// two spaces or start with a word, never with a year.
fn is_tracing_line(line: &str) -> bool {
    const LEVELS: [&str; 5] = ["ERROR ", " WARN ", " INFO ", "DEBUG ", "TRACE "];
    let plain = strip_ansi(line);
    let Some((stamp, rest)) = plain.split_once(' ') else {
        return false;
    };
    stamp.len() >= 20
        && stamp.starts_with(|c: char| c.is_ascii_digit())
        && stamp.ends_with('Z')
        && LEVELS.iter().any(|lvl| rest.starts_with(lvl))
}

/// Drop CSI sequences (`ESC [ params final`), which is all the fmt layer emits.
fn strip_ansi(line: &str) -> String {
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
}

#[test]
fn tracing_line_detector_matches_fmt_output_and_not_report_lines() {
    // Plain: INFO/WARN carry a leading pad space, DEBUG/ERROR/TRACE do not.
    assert!(is_tracing_line(
        "2026-09-03T16:20:01.123456Z  INFO siglake_storage::iceberg: opened catalog"
    ));
    assert!(is_tracing_line(
        "2026-09-03T16:20:01.123456Z DEBUG siglake_storage::iceberg: 4 files"
    ));
    // Coloured, as the subscriber writes into a pipe without NO_COLOR.
    assert!(is_tracing_line(
        "\x1b[2m2026-09-03T16:20:01.123456Z\x1b[0m \x1b[32m INFO\x1b[0m \x1b[2msiglake\x1b[0m\x1b[2m:\x1b[0m msg"
    ));
    assert!(is_tracing_line(
        "\x1b[2m2026-09-03T16:20:01.123456Z\x1b[0m \x1b[34mDEBUG\x1b[0m \x1b[2msiglake\x1b[0m\x1b[2m:\x1b[0m msg"
    ));
    // Report shapes.
    assert!(!is_tracing_line(""));
    assert!(!is_tracing_line(
        "  method                   REPAIRED  12000 rows"
    ));
    assert!(!is_tracing_line("rebuild-group-counts: events (4 files)"));
    assert!(!is_tracing_line("12000 rows in 4 files"));
}

/// The per-column report line for `column`, if the report has one. Lines are
/// `"  {column:<24} {origin:<9} ..."`, so a column shorter than 24 chars is
/// followed by at least one space.
fn column_line<'a>(stdout: &'a str, column: &str) -> Option<&'a str> {
    let prefix = format!("  {column} ");
    stdout.lines().find(|l| l.starts_with(&prefix))
}

fn expect_column_line<'a>(stdout: &'a str, column: &str) -> &'a str {
    column_line(stdout, column)
        .unwrap_or_else(|| panic!("no report line for `{column}` in:\n{stdout}"))
}

#[tokio::test]
async fn rebuild_group_counts_reports_origin_hint_and_closing_summaries() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    // `open_iceberg(data_dir, "warehouse", None, None, Some("siglake"))` is the
    // default-tenant local path: SQLite + local FS under `{data_dir}/warehouse`,
    // which is exactly what `IcebergContext::open` builds here.
    let warehouse = data_dir.join("warehouse");
    let total = ROWS * BATCHES;

    {
        let ice = IcebergContext::open(&warehouse).await.unwrap();
        let config = config();
        ice.create_index(&config).await.unwrap();
        let ident = ice.index_table_ident(&config.index_id);
        for b in 0..BATCHES {
            ice.append_to_table(&ident, batch(&config, b), &["method"])
                .await
                .unwrap();
        }
        // Now make it a table born BEFORE the fix: typed columns in every
        // file's footer and in no aggregate.
        let edited = strip_columns_from_aggregates(&warehouse, ident.name(), &["status", "size"]);
        assert!(
            edited >= 2,
            "expected to edit the inline object and at least one delta, edited {edited}"
        );
        // Dropped before the binary runs so the child is the only open handle
        // on the SQLite catalog.
    }

    // ---- 1. Without the flag: repair what the aggregate holds, name the rest.
    let out = run_rebuild(&data_dir, false);
    assert!(
        out.contains(&format!("{NAMESPACE}.{TABLE}: rebuilt through sequence")),
        "header line missing:\n{out}"
    );
    assert!(
        out.contains(&format!("(table rows: {total})")),
        "header must carry the snapshot row count:\n{out}"
    );
    assert!(
        !out.contains("holds no columns; nothing to rebuild"),
        "the aggregate still carries `method`, so this is not the skipped path:\n{out}"
    );
    let method = expect_column_line(&out, "method");
    assert!(method.contains(" repaired "), "{method}");
    assert!(method.contains(&format!("rows={total}")), "{method}");
    assert!(method.contains("distinct=2"), "{method}");
    assert!(method.contains("Tier-1 restored"), "{method}");
    for stripped in ["status", "size", "bytes"] {
        assert!(
            column_line(&out, stripped).is_none(),
            "a plain rebuild must not touch `{stripped}`:\n{out}"
        );
    }
    assert!(
        !out.lines()
            .any(|l| l.starts_with("  ") && l.contains(" admitted ")),
        "nothing is ADMITTED without the flag:\n{out}"
    );
    // The hint: which columns the flag would add, and that no rewrite is needed.
    assert!(
        out.contains("not in the aggregate: bytes, size, status."),
        "admissible hint missing or misordered:\n{out}"
    );
    assert!(
        out.contains("re-run with --admit-typed-columns to add them."),
        "the hint must name the remedy:\n{out}"
    );
    assert!(
        out.contains("No rewrite is needed while every live file carries them"),
        "{out}"
    );
    for absent in ["not readable:", "over the typed cap:", "still short:"] {
        assert!(
            !out.contains(absent),
            "`{absent}` must not be printed when no column earned it:\n{out}"
        );
    }

    // ---- 2. With the flag: three admitted columns, three outcomes.
    let out = run_rebuild(&data_dir, true);
    assert!(
        out.contains(&format!("(table rows: {total})")),
        "header:\n{out}"
    );

    // Maintained: repaired, exactly as before.
    let method = expect_column_line(&out, "method");
    assert!(method.contains(" repaired "), "{method}");
    assert!(method.contains("Tier-1 restored"), "{method}");

    // Admitted and in every file's footer: written, exact, covers the table.
    let status = expect_column_line(&out, "status");
    assert!(status.contains(" admitted "), "{status}");
    assert!(status.contains(&format!("rows={total}")), "{status}");
    assert!(status.contains("distinct=5"), "{status}");
    assert!(
        status.contains("Tier-1 enabled for a column the aggregate never carried"),
        "{status}"
    );

    // Admitted, readable, 2,000 distinct table-wide: counted, then held back.
    let size = expect_column_line(&out, "size");
    let size_distinct = SIZES_PER_BATCH * BATCHES;
    assert!(size.contains(" admitted "), "{size}");
    assert!(size.contains(&format!("rows={total}")), "{size}");
    assert!(
        size.contains(&format!("distinct={size_distinct}")),
        "{size}"
    );
    assert!(
        size.contains(&format!(
            "NOT written: {size_distinct} distinct exceeds the typed cardinality cap (1024)"
        )),
        "{size}"
    );

    // Admitted but unreadable: left absent, never a partial total.
    let bytes = expect_column_line(&out, "bytes");
    assert!(bytes.contains(" admitted "), "{bytes}");
    assert!(
        bytes.contains("NOT READABLE from any tier — left absent rather than written wrong"),
        "{bytes}"
    );

    // Closing summaries: one per outcome that needs a next step, and nothing
    // for the outcomes that do not.
    assert!(
        out.contains("not readable: bytes."),
        "unreadable summary:\n{out}"
    );
    assert!(
        out.contains("No flag recovers that; only rewriting those files does"),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "over the typed cap: size ({size_distinct} distinct)."
        )),
        "over-cap summary:\n{out}"
    );
    assert!(
        out.contains("Raise SIGLAKE_TYPED_GROUP_COUNT_CARDINALITY above the distinct count"),
        "{out}"
    );
    assert!(
        !out.contains("still short:"),
        "every written column covers the table:\n{out}"
    );
    assert!(
        !out.contains("not in the aggregate:"),
        "with the flag on, the admissible columns are in the report, not the hint:\n{out}"
    );

    // ---- 3. Row count unknown: "still short" for every repaired column, and
    // `status` is now REPAIRED — run 2 wrote it into the aggregate for real.
    let blanked = blank_total_records(&warehouse, TABLE);
    assert!(
        blanked >= 1,
        "expected to blank total-records in at least one snapshot summary"
    );
    let out = run_rebuild(&data_dir, false);
    assert!(
        out.contains("(table rows: unknown)"),
        "header must say the row count is unknown:\n{out}"
    );
    let method = expect_column_line(&out, "method");
    assert!(method.contains(" repaired "), "{method}");
    assert!(
        method.contains("still short of the table row count"),
        "{method}"
    );
    let status = expect_column_line(&out, "status");
    assert!(
        status.contains(" repaired "),
        "status was admitted by run 2, so run 3 repairs it:\n{status}"
    );
    assert!(
        status.contains("still short of the table row count"),
        "{status}"
    );
    assert!(
        out.contains("still short: method, status."),
        "short summary:\n{out}"
    );
    assert!(
        out.contains("a footer over-claims, or the row count is unknown"),
        "{out}"
    );
    // `size` and `bytes` were never written, so they are still what the flag
    // would add — and neither was touched by this flagless run.
    assert!(column_line(&out, "size").is_none(), "{out}");
    assert!(column_line(&out, "bytes").is_none(), "{out}");
    assert!(
        out.contains("not in the aggregate: bytes, size."),
        "the hint must shrink to what is still missing:\n{out}"
    );
    assert!(!out.contains("not readable:"), "{out}");
    assert!(!out.contains("over the typed cap:"), "{out}");
}
