//! Task #3052: what one auto-promotion sampling pass costs, and what a
//! promoted column costs on the write path.
//!
//! The feature's bounds are a threshold, a column ceiling and a sample bound
//! (`files × rows`). #3052 asks for the two numbers those bounds are supposed
//! to be chosen against: the CPU one pass spends parsing residual JSON, and
//! the per-column write cost that decides whether a 64-column ceiling is a
//! ceiling or a cliff.
//!
//! Run:
//! ```text
//! cargo test -p siglake-storage --test auto_promotion_cost -- --ignored --nocapture
//! ```
//!
//! Knobs: `BENCH_FILES` (8), `BENCH_ROWS_PER_FILE` (10_000).
//!
//! Separate `#[ignore]`d binary because it is a report, not an assertion. The
//! sampling arms use a 100% threshold and a fixture where no key appears in
//! every row, so each arm does the FULL sampling work and then declines to
//! promote — the pass is timed without mutating the table between arms, and
//! without a per-arm warehouse rebuild in the timing.
//!
//! Recorded numbers live in `docs/DESIGN_auto_promotion_qualification.md`.

use std::time::Instant;

use siglake_core::{events_to_record_batch, Event, PromotedColumn, PromotedType};
use siglake_storage::iceberg::IcebergContext;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// An OTLP-shaped residual: 10 scalar keys, two of them nested one level, plus
/// one per-row key so the census sees a long tail. Row `i` drops one of the 10
/// so that no key is present in 100% of the sample.
fn attributes(i: usize) -> String {
    let drop = i % 10;
    let mut parts: Vec<String> = Vec::with_capacity(10);
    if drop != 0 {
        parts.push(format!(r#""k8s.namespace":"ns-{}""#, i % 16));
    }
    if drop != 1 {
        parts.push(format!(r#""k8s.pod":"pod-{}""#, i % 512));
    }
    if drop != 2 {
        parts.push(format!(r#""service.name":"svc-{}""#, i % 8));
    }
    if drop != 3 {
        parts.push(format!(r#""http":{{"status":{}}}"#, 200 + i % 5));
    }
    if drop != 4 {
        parts.push(format!(r#""req":{{"duration":{}.5}}"#, i % 97));
    }
    if drop != 5 {
        parts.push(format!(r#""cache.hit":{}"#, i.is_multiple_of(2)));
    }
    if drop != 6 {
        parts.push(format!(r#""cloud.region":"r-{}""#, i % 4));
    }
    if drop != 7 {
        parts.push(format!(
            r#""severity":"{}""#,
            if i.is_multiple_of(3) { "warn" } else { "info" }
        ));
    }
    if drop != 8 {
        parts.push(format!(r#""trace.sampled":{}"#, i.is_multiple_of(7)));
    }
    if drop != 9 {
        parts.push(format!(r#""msg.len":{}"#, 40 + i % 200));
    }
    // The long tail: a key unique to this row.
    parts.push(format!(r#""req.id.{i}":"x""#));
    format!("{{{}}}", parts.join(","))
}

fn events(offset: usize, n: usize) -> Vec<Event> {
    (offset..offset + n)
        .map(|i| Event::now(format!("row {i}")).with_attributes(Some(attributes(i))))
        .collect()
}

fn promotions(n: usize) -> Vec<PromotedColumn> {
    // Every column must be POPULATED, or the report understates both costs: an
    // absent key is a hash lookup and an `append_null`, and an all-null Parquet
    // column is a few bytes. Past the ten keys the fixture carries, the extra
    // columns re-extract one of them under a distinct name — same work per
    // column, same encoded width as a real 64-key promotion.
    let real = [
        ("k8s.namespace", PromotedType::Utf8),
        ("k8s.pod", PromotedType::Utf8),
        ("service.name", PromotedType::Utf8),
        ("http.status", PromotedType::Int64),
        ("req.duration", PromotedType::Float64),
        ("cache.hit", PromotedType::Boolean),
        ("cloud.region", PromotedType::Utf8),
        ("severity", PromotedType::Utf8),
        ("trace.sampled", PromotedType::Boolean),
        ("msg.len", PromotedType::Int64),
    ];
    (0..n)
        .map(|i| {
            let (key, ty) = real[i % real.len()];
            PromotedColumn {
                attr_key: key.to_string(),
                name: if i < real.len() {
                    key.replace('.', "_")
                } else {
                    format!("{}_{i}", key.replace('.', "_"))
                },
                ty,
            }
        })
        .collect()
}

fn dir_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(entry.path());
            } else if entry.path().extension().is_some_and(|e| e == "parquet") {
                total += meta.len();
            }
        }
    }
    total
}

/// What one sampling pass costs, across the configurable bound. The pass runs
/// once per 300s cadence per table, so the number to weigh is its wall time
/// against that cadence — and its JSON-parse rate, which is what scales.
#[tokio::test]
#[ignore = "cost report; run by hand"]
async fn sampling_pass_cost_by_bound() {
    let files = env_usize("BENCH_FILES", 8);
    let rows_per_file = env_usize("BENCH_ROWS_PER_FILE", 10_000);

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let build = Instant::now();
    for f in 0..files {
        ice.append_events(&events(f * rows_per_file, rows_per_file))
            .await
            .unwrap();
    }
    eprintln!(
        "fixture: {files} files x {rows_per_file} rows, {} parquet bytes, built in {:.1}s",
        dir_bytes(&warehouse),
        build.elapsed().as_secs_f64()
    );

    eprintln!("\n sample_files  rows/file   nominal rows   wall ms   docs/s");
    for (sf, sr) in [
        (1usize, 256usize),
        (4, 4096),
        (8, 16_384),
        (8, 65_536),
        (64, 65_536),
    ] {
        // 100% threshold: the pass does all of the sampling work and then
        // finds nothing promotable, so the table is unchanged for the next arm.
        let start = Instant::now();
        let promoted = ice.auto_promote_hot_keys(1.0, 64, sf, sr).await.unwrap();
        let ms = start.elapsed().as_secs_f64() * 1000.0;
        assert!(
            promoted.is_empty(),
            "a 100% threshold promoted {promoted:?}; the arms are no longer independent"
        );
        // Rows the bound admits, capped by what the fixture holds.
        let rows = sf.min(files) * sr.min(rows_per_file);
        eprintln!(
            "{sf:>13} {sr:>10} {rows:>14} {ms:>9.1} {:>8.0}",
            rows as f64 / (ms / 1000.0)
        );
    }
}

/// What the column ceiling costs on the write path: promotion widens every
/// batch at write time, so 64 columns is 64 extractions per row and 64 more
/// Parquet columns per file, forever.
#[tokio::test]
#[ignore = "cost report; run by hand"]
async fn write_cost_by_column_count() {
    let rows = env_usize("BENCH_ROWS_PER_FILE", 10_000) * env_usize("BENCH_FILES", 8);
    let batch = events_to_record_batch(&events(0, rows)).unwrap();

    eprintln!("\nextraction ({rows} rows):");
    eprintln!(" columns   wall ms   rows/s");
    for n in [0usize, 1, 8, 16, 32, 64] {
        let cols = promotions(n);
        let start = Instant::now();
        let out = siglake_core::promote_attributes(&batch, &cols).unwrap();
        let ms = start.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(out.num_rows(), rows);
        eprintln!("{n:>8} {ms:>9.1} {:>8.0}", rows as f64 / (ms / 1000.0));
    }

    eprintln!("\ncommitted parquet bytes ({rows} rows, one file):");
    eprintln!(" columns       bytes   bytes/row   vs 0 cols");
    let mut base = 0f64;
    for n in [0usize, 8, 16, 64] {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = IcebergContext::open(&warehouse)
            .await
            .unwrap()
            .with_promoted_columns(promotions(n));
        ice.ensure_promoted_columns().await.unwrap();
        let start = Instant::now();
        ice.append_events(&events(0, rows)).await.unwrap();
        let commit_ms = start.elapsed().as_secs_f64() * 1000.0;
        let bytes = dir_bytes(&warehouse) as f64;
        if n == 0 {
            base = bytes;
        }
        eprintln!(
            "{n:>8} {bytes:>11.0} {:>11.2} {:>10.2}x   (append {commit_ms:.0} ms)",
            bytes / rows as f64,
            bytes / base
        );
    }
}
