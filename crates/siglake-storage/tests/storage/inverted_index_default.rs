//! Default-on footer inverted-index coverage and its bounded local cost probe.

use std::path::Path;
use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use datafusion::prelude::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, BLOOM_FILTER_COLUMNS};

fn event(i: usize) -> Event {
    let matches = i.is_multiple_of(5);
    Event {
        timestamp: Utc
            .timestamp_opt(1_800_000_000 + i as i64, 0)
            .single()
            .unwrap(),
        host: format!("host-{:04}", i % 2000),
        source: format!("/var/log/service-{}.log", i % 20),
        sourcetype: ["debug", "info", "warn", "error"][i % 4].into(),
        index: "main".into(),
        raw: if matches {
            format!("database timeout on service {} retry {}", i % 20, i % 7)
        } else {
            format!(
                "request complete on service {} status {}",
                i % 20,
                200 + i % 5
            )
        },
        attributes: Some(format!(r#"{{"region":"r{}","shard":{}}}"#, i % 8, i % 32)),
    }
}

fn file_index_bytes(path: &Path) -> u64 {
    let bytes = std::fs::read(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter(|entry| entry.key.starts_with("siglake.inverted_index.v1"))
        .filter_map(|entry| entry.value.as_ref())
        .map(|hex| (hex.len() / 2) as u64)
        .sum()
}

async fn live_file_cost(ice: &IcebergContext) -> (u64, u64) {
    let files = ice.live_data_files(ice.events_table_ident()).await.unwrap();
    files
        .iter()
        .map(|file| {
            let path = Path::new(file.file_path().trim_start_matches("file://"));
            (file.file_size_in_bytes(), file_index_bytes(path))
        })
        .fold((0, 0), |(file_total, index_total), (file, index)| {
            (file_total + file, index_total + index)
        })
}

async fn exact_match_count(ice: &IcebergContext) -> i64 {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx
        .sql("SELECT count(*) FROM events WHERE raw LIKE '%database timeout%'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn enabled_indexes_keep_flush_and_recluster_results_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_inverted_index(true);

    for chunk in 0..4 {
        let events: Vec<_> = (chunk * 25..(chunk + 1) * 25).map(event).collect();
        ice.append_events(&events).await.unwrap();
    }
    let (_, flush_index_bytes) = live_file_cost(&ice).await;
    assert!(flush_index_bytes > 0, "flush files must carry indexes");
    assert_eq!(exact_match_count(&ice).await, 20);

    let files = ice.live_data_files(ice.events_table_ident()).await.unwrap();
    ice.recluster_files(ice.events_table_ident(), files, BLOOM_FILTER_COLUMNS)
        .await
        .unwrap();
    let (_, recluster_index_bytes) = live_file_cost(&ice).await;
    assert!(
        recluster_index_bytes > 0,
        "reclustered files must carry indexes"
    );
    assert_eq!(exact_match_count(&ice).await, 20);
}

#[tokio::test]
async fn explicit_context_opt_out_skips_flush_and_recluster_indexes() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_inverted_index(false);

    for chunk in 0..4 {
        let events: Vec<_> = (chunk * 25..(chunk + 1) * 25).map(event).collect();
        ice.append_events(&events).await.unwrap();
    }
    assert_eq!(live_file_cost(&ice).await.1, 0);
    assert_eq!(exact_match_count(&ice).await, 20);

    let files = ice.live_data_files(ice.events_table_ident()).await.unwrap();
    ice.recluster_files(ice.events_table_ident(), files, BLOOM_FILTER_COLUMNS)
        .await
        .unwrap();
    assert_eq!(live_file_cost(&ice).await.1, 0);
    assert_eq!(exact_match_count(&ice).await, 20);
}

async fn measure_write(events: &[Event], enabled: bool) -> (Duration, u64, u64) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_inverted_index(enabled);
    let started = Instant::now();
    ice.append_events(events).await.unwrap();
    let elapsed = started.elapsed();
    let (file_bytes, index_bytes) = live_file_cost(&ice).await;
    (elapsed, file_bytes, index_bytes)
}

/// Bounded local A/B used when changing the writer default. It writes the same
/// 20,000 corpus-shaped events five times per arm and reports medians.
#[tokio::test]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn report_footer_inverted_index_write_cost() {
    const ROWS: usize = 20_000;
    const RUNS: usize = 5;
    let events: Vec<_> = (0..ROWS).map(event).collect();
    let mut off = Vec::with_capacity(RUNS);
    let mut on = Vec::with_capacity(RUNS);
    for run in 0..RUNS {
        if run % 2 == 0 {
            off.push(measure_write(&events, false).await);
            on.push(measure_write(&events, true).await);
        } else {
            on.push(measure_write(&events, true).await);
            off.push(measure_write(&events, false).await);
        }
    }
    off.sort_by_key(|sample| sample.0);
    on.sort_by_key(|sample| sample.0);
    let off = off[RUNS / 2];
    let on = on[RUNS / 2];
    assert_eq!(off.2, 0);
    assert!(on.2 > 0);
    println!(
        "footer inverted-index A/B: rows={ROWS} runs={RUNS} off_ms={:.2} on_ms={:.2} time_delta_pct={:.2} off_file_bytes={} on_file_bytes={} index_bytes={} file_delta_bytes={} index_share_pct={:.2}",
        off.0.as_secs_f64() * 1000.0,
        on.0.as_secs_f64() * 1000.0,
        100.0 * (on.0.as_secs_f64() / off.0.as_secs_f64() - 1.0),
        off.1,
        on.1,
        on.2,
        on.1 as i64 - off.1 as i64,
        100.0 * on.2 as f64 / on.1 as f64,
    );
}
