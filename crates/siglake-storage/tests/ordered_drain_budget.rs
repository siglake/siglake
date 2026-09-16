//! Own test binary: both cases overwrite process-global query scan tuning.

use chrono::{Duration, NaiveTime, TimeZone, Utc};
use datafusion::physical_plan::displayable;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

fn collect_timestamps(batches: &[arrow_array::RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let a = siglake_core::column_nanos(b.column(0)).unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

fn nanos(base: chrono::DateTime<Utc>, offs: &[i64]) -> Vec<i64> {
    offs.iter()
        .map(|s| {
            (base + Duration::seconds(*s))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect()
}

async fn run_case(tuning: QueryScanTuning) {
    siglake_storage::configure_query_scan_tuning(tuning);

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let wide = "x".repeat(8192);
    for file_idx in 0..4i64 {
        let evs: Vec<Event> = (0..24)
            .map(|row_idx| {
                let offset = file_idx * 100 + row_idx;
                let mut e = Event::now(format!("row-{offset}-{wide}"));
                e.timestamp = base + Duration::seconds(offset);
                e.host = if file_idx == 0 && row_idx < 5 {
                    format!("hot-{wide}")
                } else {
                    format!("cold-{file_idx}-{row_idx}-{wide}")
                };
                e
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }

    let ctx = siglake_storage::session_context_with_target_partitions(Some(2));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let no_match_sql = "SELECT host FROM events \
        WHERE upper(host) = 'NEVER_MATCH' \
        ORDER BY \"timestamp\" ASC LIMIT 5";
    let df = ctx.sql(no_match_sql).await.unwrap();
    let no_match_plan = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        no_match_plan.contains("FilterExec"),
        "no-match regression must keep a residual filter above the scan:\n{no_match_plan}"
    );
    assert!(
        !no_match_plan.contains("SortExec"),
        "ordered no-match regression must stream the advertised order:\n{no_match_plan}"
    );
    let empty = df.collect().await.unwrap();
    assert_eq!(empty.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

    let match_sql = "SELECT \"timestamp\" FROM events \
        WHERE upper(host) LIKE 'HOT-%' \
        ORDER BY \"timestamp\" ASC LIMIT 5";
    let df = ctx.sql(match_sql).await.unwrap();
    let match_plan = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        match_plan.contains("SortPreservingMergeExec"),
        "ordered LIMIT must still early-stop under the tiny drain budget:\n{match_plan}"
    );
    assert!(
        !match_plan.contains("SortExec"),
        "tiny ordered-drain budget must not force a blocking sort:\n{match_plan}"
    );
    assert_eq!(
        collect_timestamps(&df.collect().await.unwrap()),
        nanos(base, &[0, 1, 2, 3, 4]),
        "ordered LIMIT must still return the head in order under tiny budget"
    );
}

#[tokio::test]
async fn ordered_drain_budget_binds_reader_and_cache_paths() {
    run_case(QueryScanTuning {
        ordered_drain_buffer_bytes: Some(65_536),
        ..Default::default()
    })
    .await;
    run_case(QueryScanTuning {
        file_cache_max_bytes: Some(64 * 1024 * 1024),
        file_cache_max_entries: Some(64),
        ordered_drain_buffer_bytes: Some(65_536),
        ..Default::default()
    })
    .await;
}
