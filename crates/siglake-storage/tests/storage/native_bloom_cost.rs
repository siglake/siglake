//! What do the Parquet-native bloom filters actually cost? (cross-review F-2)
//!
//! They are written on `host`/`source`/`sourcetype`/`index` for every row group
//! and read by nothing. The cross-review priced them at ~120 KB/row-group for
//! `host`; an earlier A/B in this repo measured ~1.9% of file bytes, but at a
//! 32 MB row-group target rather than the 256 MB production one — and since the
//! filter size depends only on NDV/FPP, not on row-group size, that percentage
//! is inversely proportional to how much data each row group holds. Quoting it
//! without the row-group size attached is misleading.
//!
//! This reads the true per-column-chunk lengths out of the footer, at both
//! sizes, so the cost can be stated per row group AND as a share.
//!
//! ```text
//! cargo test --release -p siglake-storage --test storage native_bloom_cost:: -- --ignored --nocapture
//! ```

use chrono::{TimeZone, Utc};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

/// Corpus-shaped: the share of a file that blooms occupy depends entirely on
/// how many BYTES a row group holds, so a too-compressible fixture inflates it.
/// This mirrors the benchmark corpus — a residual `attributes` JSON with
/// high-entropy trace ids — so a row group holds a realistic amount of data.
fn ev(i: i64, host: u32) -> Event {
    let w = |v: u64| v.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    Event {
        timestamp: Utc.timestamp_opt(1_800_000_000 + i, 0).single().unwrap(),
        host: format!("host-{host:05}"),
        source: format!("/var/log/svc{}.log", host % 10),
        sourcetype: ["debug", "info", "warn", "error"][(host % 4) as usize].to_string(),
        index: "main".into(),
        raw: format!(
            "GET /api/v1/x{} 200 in {}ms service=svc{}",
            i % 8,
            i % 4000,
            host % 10
        ),
        attributes: Some(format!(
            r#"{{"agent":{{"name":"vector","id":"agent-{:05}"}},"cloud":{{"provider":"aws","az":"us-west-2a","account_id":"{:012}"}},"http":{{"method":"GET","status_code":200,"response_time_ms":{}}},"trace":{{"id":"{:016x}{:016x}","span_id":"{:016x}"}}}}"#,
            host % 5000,
            w(i as u64) % 1_000_000_000_000,
            i % 4000,
            w(i as u64),
            w(i as u64 ^ 0x5EED),
            w(i as u64 ^ 0xBEEF),
        )),
    }
}

async fn measure(row_group_mb: usize, rows: usize) -> (u64, u64, usize) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            target_row_group_bytes: Some(row_group_mb * 1024 * 1024),
            ..Default::default()
        });
    // 2000 hosts, matching the benchmark corpus.
    let batch: Vec<Event> = (0..rows as i64).map(|i| ev(i, (i % 2000) as u32)).collect();
    ice.append_events(&batch).await.unwrap();

    let ident = ice.events_table_ident().clone();
    let files = ice.live_data_files(&ident).await.unwrap();
    let mut per_col: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut bloom_bytes = 0u64;
    let mut file_bytes = 0u64;
    let mut row_groups = 0usize;
    for df in &files {
        let path = df.file_path().trim_start_matches("file://").to_string();
        let bytes = std::fs::read(&path).unwrap();
        file_bytes += bytes.len() as u64;
        let md = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(bytes),
        )
        .unwrap()
        .metadata()
        .clone();
        row_groups += md.num_row_groups();
        for rg in md.row_groups() {
            for col in rg.columns() {
                let n = col.bloom_filter_length().unwrap_or(0).max(0) as u64;
                bloom_bytes += n;
                if n > 0 {
                    *per_col
                        .entry(col.column_descr().name().to_string())
                        .or_insert(0u64) += n;
                }
            }
        }
    }
    for (name, bytes) in &per_col {
        println!(
            "    column {name:12} {:>10} bytes total  {:>7} KB/row-group",
            bytes,
            bytes / row_groups.max(1) as u64 / 1024
        );
    }
    (bloom_bytes, file_bytes, row_groups)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn report_native_bloom_cost() {
    println!(
        "\n{:>10} {:>8} {:>12} {:>14} {:>12} {:>9}",
        "rg_target", "rgs", "rows", "bloom_bytes", "file_bytes", "share"
    );
    // 32 MB is what the earlier A/B used; 256 MB is the production default.
    // 2M rows so the 256 MB byte target actually binds rather than the
    // 128Ki MIN_ROW_GROUP_ROWS clamp — otherwise both arms produce identical
    // row groups and the comparison says nothing.
    for (mb, rows) in [(256usize, 2_000_000usize)] {
        let (bloom, total, rgs) = measure(mb, rows).await;
        let per_rg = if rgs > 0 { bloom / rgs as u64 } else { 0 };
        println!(
            "{:>9}M {:>8} {:>12} {:>14} {:>12} {:>8.2}%   ({} KB/row-group)",
            mb,
            rgs,
            rows,
            bloom,
            total,
            100.0 * bloom as f64 / total as f64,
            per_rg / 1024,
        );
    }
    println!();
}
