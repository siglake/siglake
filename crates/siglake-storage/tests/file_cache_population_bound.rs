//! Tasks #5074/#5786: qualification of the process-wide decoded-cache
//! population bound and turnover-preserving production admission. Isolated
//! because scan tuning, cache entries and population accounting are process-wide.

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
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

#[derive(Clone, Copy)]
enum Admission {
    Production,
    RejectedPrototype,
    UnboundedControl,
}

fn tuning(max_bytes: u64, partitions: usize, admission: Admission) -> QueryScanTuning {
    QueryScanTuning {
        file_cache_max_bytes: Some(max_bytes),
        file_cache_max_entries: Some(1_024),
        file_concurrency_limit: Some(partitions),
        batch_size: Some(BATCH_ROWS),
        file_cache_population_bound_prototype: matches!(admission, Admission::RejectedPrototype),
        file_cache_unbounded_population_prototype: matches!(admission, Admission::UnboundedControl),
        ..Default::default()
    }
}

fn outcomes(snapshotter: &Snapshotter) -> (u64, u64) {
    let snapshot = snapshotter.snapshot().into_vec();
    let read = |name: &str| {
        snapshot
            .iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "siglake_query_scan_file_cache_requests_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == name)
            })
            .map(|(_, _, _, value)| match value {
                DebugValue::Counter(value) => *value,
                _ => 0,
            })
            .sum()
    };
    (read("insert"), read("evict"))
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
async fn production_population_bound_covers_turnover_fanout_handoff_and_cancellation() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();
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
    siglake_storage::configure_query_scan_tuning(tuning(
        8 * 1024 * 1024 * 1024,
        1,
        Admission::UnboundedControl,
    ));
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
    siglake_storage::configure_query_scan_tuning(tuning(budget, FILES, Admission::Production));
    siglake_storage::reset_decoded_file_cache_population_peaks();
    let ctx = siglake_storage::session_context_with_target_partitions(Some(FILES));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let (left, right) = tokio::join!(row_count(&ctx, SCAN_SQL), row_count(&ctx, SCAN_SQL));
    assert_eq!((left, right), (rows, rows));
    settle().await;

    let first_stats = siglake_storage::decoded_file_cache_population_stats();
    let first_footprint = siglake_storage::decoded_file_cache_footprint();
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

    // Keep those resident entries and change the projection, hence every cache
    // key. Production admission must make room and install replacements rather
    // than freezing the first scheduler-selected residents as #5074 did.
    siglake_storage::reset_decoded_file_cache_population_peaks();
    let _ = outcomes(&snapshotter);
    let changed_sql = "SELECT host FROM events";
    let (left, right) = tokio::join!(row_count(&ctx, changed_sql), row_count(&ctx, changed_sql));
    assert_eq!((left, right), (rows, rows));
    settle().await;
    let resident_stats = siglake_storage::decoded_file_cache_population_stats();
    assert!(resident_stats.peak_accounted_bytes <= budget);
    let (replacement_inserts, replacement_evictions) = outcomes(&snapshotter);
    assert!(
        replacement_inserts > 0,
        "a changed working set installed no replacement entries"
    );
    assert!(
        replacement_evictions > 0,
        "a changed working set evicted no residents"
    );
    assert!(
        siglake_storage::decoded_file_cache_footprint().priced_bytes <= budget,
        "replacement left completed entries beyond the byte bound"
    );

    // Keep the #5074 arm as the matched negative control: once its residents
    // fill the same budget, a changed projection cannot admit a first batch and
    // therefore cannot insert or evict.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(budget, 1, Admission::RejectedPrototype));
    let prototype_ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&prototype_ctx).await.unwrap();
    assert_eq!(row_count(&prototype_ctx, SCAN_SQL).await, rows);
    settle().await;
    let _ = outcomes(&snapshotter);
    let prototype_refusals = siglake_storage::decoded_file_cache_population_stats().budget_refusals;
    assert_eq!(row_count(&prototype_ctx, changed_sql).await, rows);
    settle().await;
    assert_eq!(outcomes(&snapshotter), (0, 0));
    assert!(
        siglake_storage::decoded_file_cache_population_stats().budget_refusals > prototype_refusals
    );

    // A clipped stream is cancelled by its LIMIT before EOF. Dropping it must
    // return its admission while preserving the answer.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(budget, FILES, Admission::Production));
    siglake_storage::reset_decoded_file_cache_population_peaks();
    let cancel_ctx = siglake_storage::session_context_with_target_partitions(Some(FILES));
    ice.register_with_datafusion(&cancel_ctx).await.unwrap();
    assert_eq!(
        row_count(
            &cancel_ctx,
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
