//! Task #5074: local qualification of a process-wide decoded-cache population
//! bound. Isolated because scan tuning and the cache are process-wide.

use datafusion::prelude::SessionContext;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

const FILES: usize = 8;
const FILE_ROWS: usize = 16_384;
const BATCH_ROWS: usize = 4_096;
const SCAN_SQL: &str = "SELECT raw FROM events";

fn events(file: usize) -> Vec<Event> {
    (0..FILE_ROWS)
        .map(|row| {
            let mut event = Event::now(format!("file-{file} row-{row} status=200"));
            event.host = "alpha".into();
            event
        })
        .collect()
}

fn tuning(max_bytes: u64, partitions: usize, bounded: bool) -> QueryScanTuning {
    QueryScanTuning {
        file_cache_max_bytes: Some(max_bytes),
        file_cache_max_entries: Some(1_024),
        file_concurrency_limit: Some(partitions),
        batch_size: Some(BATCH_ROWS),
        file_cache_population_bound_prototype: bounded,
        ..Default::default()
    }
}

async fn row_count(ctx: &SessionContext, sql: &str) -> usize {
    ctx.sql(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

async fn settle() {
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_population_bound_covers_fanout_residents_handoff_and_cancellation() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO);
    for file in 0..FILES {
        ice.append_events(&events(file)).await.unwrap();
    }
    let rows = FILES * FILE_ROWS;

    // Measure this fixture's real conservative price per completed file.
    siglake_storage::configure_query_scan_tuning(tuning(8 * 1024 * 1024 * 1024, 1, false));
    let probe_ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&probe_ctx).await.unwrap();
    assert_eq!(row_count(&probe_ctx, SCAN_SQL).await, rows);
    settle().await;
    let probe = siglake_storage::decoded_file_cache_footprint();
    assert_eq!(probe.entries, FILES, "probe did not populate every file");
    let entry_bytes = probe.priced_bytes / probe.entries as u64;
    assert!(entry_bytes > 0);

    // Four entries fit. Eight partitions and two overlapping queries could
    // retain far more under the old per-stream quarter-budget rule.
    let budget = entry_bytes * 4;
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(budget, FILES, true));
    siglake_storage::reset_decoded_file_cache_population_peaks();
    let ctx = siglake_storage::session_context_with_target_partitions(Some(FILES));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let (left, right) = tokio::join!(row_count(&ctx, SCAN_SQL), row_count(&ctx, SCAN_SQL));
    assert_eq!((left, right), (rows, rows));
    settle().await;

    let first_stats = siglake_storage::decoded_file_cache_population_stats();
    let first_footprint = siglake_storage::decoded_file_cache_footprint();
    assert!(
        first_stats.budget_refusals > 0,
        "fan-out never exercised shared-budget refusal: {first_stats:?}"
    );
    assert!(
        first_stats.peak_accounted_bytes <= budget,
        "completed entries plus live populations exceeded {budget}: {first_stats:?}"
    );
    assert_eq!(
        (
            first_stats.inflight_extent_bytes,
            first_stats.inflight_retained_bytes
        ),
        (0, 0),
        "insertion handoff left a population charge: {first_stats:?}"
    );
    assert!(first_footprint.priced_bytes <= budget);

    // Keep those resident entries and repeat the fan-out. Completed entries
    // consume the same bound, so missing files are refused without disturbing
    // query correctness or charging a ninth cache outcome.
    siglake_storage::reset_decoded_file_cache_population_peaks();
    let refusals_before = first_stats.budget_refusals;
    let (left, right) = tokio::join!(row_count(&ctx, SCAN_SQL), row_count(&ctx, SCAN_SQL));
    assert_eq!((left, right), (rows, rows));
    settle().await;
    let resident_stats = siglake_storage::decoded_file_cache_population_stats();
    assert!(resident_stats.budget_refusals > refusals_before);
    assert!(resident_stats.peak_accounted_bytes <= budget);

    // A clipped stream is cancelled by its LIMIT before EOF. Dropping it must
    // return its admission while preserving the answer.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::reset_decoded_file_cache_population_peaks();
    assert_eq!(
        row_count(
            &ctx,
            "SELECT raw FROM events WHERE lower(host) = 'alpha' LIMIT 1",
        )
        .await,
        1
    );
    settle().await;
    let cancelled = siglake_storage::decoded_file_cache_population_stats();
    assert!(
        cancelled.peak_accounted_bytes > 0,
        "fixture buffered nothing"
    );
    assert_eq!(
        (
            cancelled.inflight_extent_bytes,
            cancelled.inflight_retained_bytes
        ),
        (0, 0),
        "cancelled population leaked its charge: {cancelled:?}"
    );

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
    siglake_storage::clear_decoded_file_cache();
}
