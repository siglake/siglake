//! Does the SHIPPED compaction policy converge an overlapping layout?
//!
//! Every AWS round overrides the pass budgets, and by large factors:
//!
//! | knob | `ReclusterPolicy::default()` | bench harness | ratio |
//! |---|---|---|---|
//! | `max_pass_bytes` | 256 MiB | 4096 MiB | 16x |
//! | `max_pass_rows` | 2,000,000 | 200,000,000 | 100x |
//! | `max_files_per_pass` | 128 | 64 | 0.5x |
//! | `max_bins_per_pass` | 16 | 8 | 0.5x |
//!
//! So the measured configuration and the shipped one are not the same thing,
//! and the shipped one has never been exercised. This runs an overlapping
//! layout to quiescence under each and reports whether it converges, in how
//! many passes, and at what write amplification.
//!
//! ```text
//! cargo test --release -p siglake-storage --test storage default_policy_convergence:: -- --ignored --nocapture
//! ```

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::{
    IcebergContext, LevelPolicy, LeveledPassOptions, ReclusterPolicy, BLOOM_FILTER_COLUMNS,
};

fn ev(secs: i64, i: i64) -> Event {
    let w = |v: u64| v.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: format!("host-{:05}", i % 2000),
        source: format!("/var/log/svc{}.log", i % 10),
        sourcetype: ["debug", "info", "warn", "error"][(i % 4) as usize].to_string(),
        index: "main".into(),
        raw: format!("GET /api/v1/x{} 200 in {}ms", i % 8, i % 4000),
        attributes: Some(format!(
            r#"{{"trace":{{"id":"{:016x}","span_id":"{:016x}"}},"http":{{"status_code":200,"response_time_ms":{}}}}}"#,
            w(i as u64),
            w(i as u64 ^ 0x5EED),
            i % 4000
        )),
    }
}

async fn converge(label: &str, policy: ReclusterPolicy, files: usize, rows: usize) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let base = Utc
        .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
        .unwrap()
        .timestamp();

    // Time-SHUFFLED parallel ingest: every file spans the whole range, which is
    // what several concurrent ingesters actually produce and the shape
    // compaction exists to disjoin.
    for f in 0..files {
        let batch: Vec<Event> = (0..rows)
            .map(|r| {
                let i = (f * rows + r) as i64;
                ev(base + (r as i64 * 7919) % (rows as i64), i)
            })
            .collect();
        ice.append_events(&batch).await.unwrap();
    }

    let ident = ice.events_table_ident().clone();
    let before = ice.live_data_files(&ident).await.unwrap();
    let rows_before: u64 = before.iter().map(|f| f.record_count()).sum();
    let bytes_before: u64 = before.iter().map(|f| f.file_size_in_bytes()).sum();

    let levels = LevelPolicy::default();
    let opts = LeveledPassOptions::default();
    let start = std::time::Instant::now();
    let mut passes = 0usize;
    let mut rewritten = 0usize;
    // Generous ceiling: the question is whether it converges at all, and in how
    // many passes — not whether it does so within some product budget.
    for _ in 0..200 {
        let stats = ice
            .recluster_pass_leveled(&ident, BLOOM_FILTER_COLUMNS, &levels, policy, &opts)
            .await
            .unwrap();
        if stats.is_empty() {
            break;
        }
        passes += 1;
        rewritten += stats.iter().map(|s| s.rows).sum::<usize>();
    }
    let wall = start.elapsed().as_secs_f64();

    let after = ice.live_data_files(&ident).await.unwrap();
    let rows_after: u64 = after.iter().map(|f| f.record_count()).sum();
    assert_eq!(rows_before, rows_after, "{label}: rows must be conserved");

    let depth = overlap_depth(&after);
    println!(
        "{label:22} files {:>3} -> {:>3}   depth {:>3}   passes {:>3}   \
         write_amp {:>5.2}x   {:>6.1}s   in {:.1} MB",
        before.len(),
        after.len(),
        depth,
        passes,
        rewritten as f64 / rows_before as f64,
        wall,
        bytes_before as f64 / (1024.0 * 1024.0),
    );
}

/// Max number of files covering any single instant — the fan-in an ordered scan
/// needs, and the thing compaction is trying to bound.
// Nested `if let` rather than a let-chain: the workspace is on Rust 2021, where
// let chains do not parse.
#[allow(clippy::collapsible_if)]
fn overlap_depth(files: &[iceberg::spec::DataFile]) -> usize {
    let mut ev: Vec<(i64, i32)> = Vec::new();
    for f in files {
        let path = f.file_path().trim_start_matches("file://").to_string();
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let Ok(b) = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(bytes),
        ) else {
            continue;
        };
        let md = b.metadata();
        let (mut lo, mut hi) = (i64::MAX, i64::MIN);
        for rg in md.row_groups() {
            for c in rg.columns() {
                if c.column_path().string() == "timestamp" {
                    if let Some(parquet::file::statistics::Statistics::Int64(v)) = c.statistics() {
                        if let (Some(a), Some(b)) = (v.min_opt(), v.max_opt()) {
                            lo = lo.min(*a);
                            hi = hi.max(*b);
                        }
                    }
                }
            }
        }
        if lo <= hi {
            ev.push((lo, 1));
            ev.push((hi + 1, -1));
        }
    }
    ev.sort_by_key(|&(t, d)| (t, std::cmp::Reverse(d)));
    let (mut cur, mut best) = (0i32, 0i32);
    for (_, d) in ev {
        cur += d;
        best = best.max(cur);
    }
    best as usize
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn report_default_vs_harness_policy() {
    let files = 24usize;
    let rows = 250_000usize;
    println!("\n{files} overlapping files x {rows} rows\n");

    // As shipped.
    converge("default", ReclusterPolicy::default(), files, rows).await;

    // As every AWS round actually ran.
    converge(
        "harness-override",
        ReclusterPolicy {
            max_pass_bytes: 4096 * 1024 * 1024,
            max_pass_rows: 200_000_000,
            max_files_per_pass: 64,
            max_bins_per_pass: 8,
            ..ReclusterPolicy::default()
        },
        files,
        rows,
    )
    .await;
    println!();
}
