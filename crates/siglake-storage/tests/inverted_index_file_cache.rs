//! WS-5 × file-cache integration: with the scan file cache enabled (the
//! production default since the hot-caches rollout), a `raw LIKE '%substr%'`
//! query must still go through the pruning reader — trigram file-skip +
//! inverted-index row selection — not the cache's whole-file decode path.
//! Regression for the round-73 finding: the cached read path dropped
//! `raw_substring_filter`, silently disabling all substring pruning.
//!
//! Cache-miss-with-filter reads must NOT populate the cache (they decode a
//! subset of the file); cache hits may serve whole-file batches (the engine
//! re-applies the exact `LIKE` above the scan), so results stay exact either
//! way.
//!
//! Isolated in its own test binary: the query-scan tuning, the file batch
//! cache, and the global metrics recorder are all process-wide.

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

// One materialized snapshot, queried repeatedly — `Snapshotter::snapshot()`
// drains the registry, so consecutive snapshots don't re-report a counter.
type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && label.is_none_or(|(lk, lv)| {
                    key.key().labels().any(|l| l.key() == lk && l.value() == lv)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn substring_pruning_fires_with_file_cache_enabled() {
    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    // File cache ON — the production shape that originally bypassed pruning.
    siglake_storage::configure_query_scan_tuning(QueryScanTuning {
        file_cache_max_bytes: Some(64 * 1024 * 1024),
        file_cache_max_entries: Some(64),
        ..Default::default()
    });

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO);

    let raws: Vec<String> = (0..200)
        .map(|i| match i % 4 {
            0 => format!("error connecting database row{i}"),
            1 => format!("user login okay session{i}"),
            2 => format!("database error timeout req{i}"),
            _ => format!("info heartbeat ping{i}"),
        })
        .collect();
    for chunk in raws.chunks(50) {
        let evs: Vec<Event> = chunk
            .iter()
            .enumerate()
            .map(|(i, raw)| {
                let mut event = Event::now(raw.clone());
                if i % 2 != 0 {
                    event.host = "cache-other-host".into();
                }
                event
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();

    // 1. Cold cache + LIKE: exact result, AND the inverted index must have
    //    produced a row selection (the bug served these reads from the cache
    //    path, which never consulted the index).
    let want_db = raws.iter().filter(|r| r.contains("database")).count() as i64;
    let got = count(
        &ctx,
        "SELECT count(*) AS n FROM events WHERE raw LIKE '%database%'",
    )
    .await;
    assert_eq!(got, want_db, "LIKE under file cache dropped or added rows");
    let snapshot = snap.snapshot().into_vec();
    let index_used = counter_sum(&snapshot, "siglake_iceberg_inverted_index_used_total", None);
    assert!(
        index_used > 0,
        "inverted index never fired: the file-cache path bypassed the pruning reader"
    );
    assert!(
        counter_sum(
            &snapshot,
            "siglake_query_scan_file_cache_requests_total",
            Some(("outcome", "bypass")),
        ) > 0,
        "substring-filtered misses must bypass the cache insert"
    );

    // 2. Populate the cache with whole-file batches via a non-LIKE predicate
    //    scan, then re-run the LIKE: hits serve every row of the file and the
    //    engine re-checks the exact LIKE, so the count must not change.
    let got_host = count(
        &ctx,
        "SELECT count(*) AS n FROM events WHERE host = 'localhost'",
    )
    .await;
    assert_eq!(
        got_host,
        (raws.len() / 2) as i64,
        "cache-populating equality scan must remain exact"
    );
    let got_warm = count(
        &ctx,
        "SELECT count(*) AS n FROM events WHERE raw LIKE '%database%'",
    )
    .await;
    assert_eq!(got_warm, want_db, "cache-hit LIKE result diverged");

    // Absent term: still exact (zero) through whichever path serves it.
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%zzznope%'"
        )
        .await,
        0
    );
}

/// WS-3 × file cache: an order-advertising scan must emit batches in task
/// order on the CACHED read path too (`buffered`, not `buffer_unordered` —
/// unordered yielding would interleave files and break the advertised
/// `timestamp ASC`). Multi-file time-disjoint partitions + file cache enabled:
/// the ordered LIMIT plans without a blocking SortExec and returns the exact
/// global head, on both the cold (insert) and warm (hit) cache.
#[tokio::test]
async fn ordered_limit_is_correct_with_file_cache_enabled() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use datafusion::physical_plan::displayable;
    use siglake_core::Event;

    siglake_storage::configure_query_scan_tuning(QueryScanTuning {
        file_cache_max_bytes: Some(64 * 1024 * 1024),
        file_cache_max_entries: Some(64),
        ..Default::default()
    });

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    // Four time-disjoint files.
    for i in 0..4i64 {
        let evs: Vec<Event> = (0..6)
            .map(|j| {
                let mut e = Event::now(format!("row @{}", i * 10 + j));
                e.timestamp = base + Duration::seconds(i * 10 + j);
                e
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }

    let ctx = siglake_storage::session_context_with_target_partitions(Some(2));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let sql = "SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" ASC LIMIT 8";
    let want: Vec<i64> = [0, 1, 2, 3, 4, 5, 10, 11]
        .iter()
        .map(|s| {
            (base + Duration::seconds(*s))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();

    // Twice: cold cache (miss+insert) and warm cache (hits) must both stream
    // each partition's files in time order.
    for round in ["cold", "warm"] {
        let df = ctx.sql(sql).await.unwrap();
        let plan_str = format!(
            "{}",
            displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
        );
        assert!(
            !plan_str.contains("SortExec"),
            "({round}) ordered LIMIT must not need a blocking sort:\n{plan_str}"
        );
        let batches = df.collect().await.unwrap();
        let ts: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let a = siglake_core::column_nanos(b.column(0)).unwrap();
                (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            ts, want,
            "({round}) cached ordered scan must yield the head in order"
        );
    }
}

/// WS-3 k-way merge × file cache: an overlapping-file partition merges its
/// per-file streams correctly when those streams come from the whole-file
/// batch cache (cold and warm).
#[tokio::test]
async fn overlapping_partition_merges_with_file_cache_enabled() {
    use chrono::{Duration, NaiveTime, TimeZone, Utc};
    use datafusion::physical_plan::displayable;
    use siglake_core::Event;

    siglake_storage::configure_query_scan_tuning(QueryScanTuning {
        file_cache_max_bytes: Some(64 * 1024 * 1024),
        file_cache_max_entries: Some(64),
        ..Default::default()
    });

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.from_utc_datetime(
        &Utc::now()
            .date_naive()
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    );
    let mk = |offs: &[i64]| -> Vec<Event> {
        offs.iter()
            .map(|&s| {
                let mut e = Event::now(format!("row @{s}"));
                e.timestamp = base + Duration::seconds(s);
                e
            })
            .collect()
    };
    // Interleaved ranges ⇒ merge required.
    ice.append_events(&mk(&[10, 50, 90])).await.unwrap();
    ice.append_events(&mk(&[20, 60])).await.unwrap();
    ice.append_events(&mk(&[5, 70])).await.unwrap();

    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let want: Vec<i64> = [5, 10, 20, 50, 60, 70, 90]
        .iter()
        .map(|s| {
            (base + Duration::seconds(*s))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect();

    for round in ["cold", "warm"] {
        let df = ctx
            .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" ASC")
            .await
            .unwrap();
        let plan_str = format!(
            "{}",
            displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
        );
        assert!(
            !plan_str.contains("SortExec"),
            "({round}) overlapping partition must merge, not blocking-sort:\n{plan_str}"
        );
        let batches = df.collect().await.unwrap();
        let ts: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let a = siglake_core::column_nanos(b.column(0)).unwrap();
                (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            ts, want,
            "({round}) cache-backed merge must interleave exactly"
        );
    }
}
