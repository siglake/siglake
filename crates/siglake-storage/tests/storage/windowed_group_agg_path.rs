//! A windowed GROUP BY must be served by the 2D time x group rollup, not by
//! summing a footer per live file.
//!
//! Measured 2026-08-25 at 2B rows: `count_by_level_last25` took 1,065.9ms on an
//! otherwise 42-102ms board and reported `served_by: "materialized"` — it NEVER
//! takes the fast path. `windowed_agg_fallback_total{stale_or_capped}` fired
//! once per execution.

use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::index_config::IndexConfig;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, TimeBounds};

/// Which fallback reason fired, if any — the thing that names WHY the rollup
/// was missed. One snapshot only: the debugging recorder drains on each call.
fn fallback_reasons(snap: metrics_util::debugging::Snapshot) -> Vec<(String, u64)> {
    snap.into_vec()
        .into_iter()
        .filter(|(k, _, _, _)| k.key().name() == "siglake_query_windowed_agg_fallback_total")
        .map(|(k, _, _, v)| {
            let reason = k
                .key()
                .labels()
                .find(|l| l.key() == "reason")
                .map(|l| l.value().to_string())
                .unwrap_or_default();
            let n = match v {
                DebugValue::Counter(c) => c,
                _ => 0,
            };
            (reason, n)
        })
        .collect()
}

/// Build an index whose rows carry a low-cardinality `level`, spread over a
/// span wide enough to need many hourly buckets, appended in several commits —
/// the shape the bench corpus has.
async fn seed(dir: &std::path::Path, appends: i64, per_append: i64) -> Arc<IcebergContext> {
    let ice = Arc::new(IcebergContext::open(&dir.join("warehouse")).await.unwrap());
    let config = IndexConfig {
        index_id: "logs-bench".into(),
        ..IndexConfig::builtin_events()
    };
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident("logs-bench");
    let bloom_cols: Vec<String> = config.doc_mapping.tag_fields.to_vec();
    let base = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let levels = ["info", "warn", "debug", "error"];
    for a in 0..appends {
        let mut events = Vec::with_capacity(per_append as usize);
        for i in 0..per_append {
            let k = a * per_append + i;
            let mut e = Event::now(format!("row {k} level={}", levels[(k % 4) as usize]));
            // Spread across hours so the rollup has many buckets.
            e.timestamp = base + Duration::minutes(k * 7);
            e.host = format!("host-{}", k % 3);
            e.sourcetype = levels[(k % 4) as usize].to_string();
            events.push(e);
        }
        let batch = siglake_core::events_to_record_batch(&events).unwrap();
        let mapped = siglake_core::mapping::map_carrier_batch(&batch, &config).unwrap();
        // REAL bloom columns, as the drain passes: `gc_columns` is derived from
        // this argument, so an empty slice would build no rollup at all and the
        // test would reproduce the wrong thing.
        let bloom: Vec<&str> = bloom_cols.iter().map(String::as_str).collect();
        ice.append_to_table(&ident, mapped, &bloom).await.unwrap();
    }
    ice
}

/// THE PROPERTY: a windowed GROUP BY over a low-cardinality column, on a table
/// whose rollup should cover it, must report the rollup path.
#[tokio::test]
async fn a_windowed_group_by_uses_the_rollup() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path(), 6, 400).await;
    let base = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    // Trailing quarter of the span, the `*_last25` shape.
    let span_min = 6 * 400 * 7;
    let window = TimeBounds {
        start: Some(base + Duration::minutes(span_min * 3 / 4)),
        end: Some(base + Duration::minutes(span_min + 60)),
    };

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let got = ice
        .grouped_counts_with_summary("logs-bench", "sourcetype", None, Some(window))
        .await
        .unwrap()
        .expect("windowed group counts");
    let reasons = fallback_reasons(snapshotter.snapshot());
    drop(guard);

    assert_eq!(
        got.source_label(),
        "tier1_windowed_agg",
        "the windowed GROUP BY fell back to summing a footer per live file \
         instead of using the 2D rollup — this is the 1,065ms path measured at \
         2B rows. reasons={reasons:?} rows={:?}",
        got.to_rows()
    );
    assert!(!got.is_empty(), "the rollup answered with nothing");
}

/// THE BENCH CONDITION. `count_by_level_last25` groups by a column that is not
/// one of the index's TAG fields. `gc_columns` — the set the rollup is built
/// over — is derived from `bloom_columns`, which callers populate from
/// `tag_fields`. So a GROUP BY on any other column can never hit the rollup, no
/// matter how low its cardinality.
///
/// The asymmetry that makes this a bug rather than a limitation: the UNWINDOWED
/// `count_by_level` is served from the wide aggregate in 58ms, so the column is
/// clearly cheap to count. Only the windowed form falls off.
#[tokio::test]
async fn a_group_by_on_a_non_tag_column_misses_the_rollup() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path(), 6, 400).await;
    let base = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let span_min = 6 * 400 * 7;
    let window = TimeBounds {
        start: Some(base + Duration::minutes(span_min * 3 / 4)),
        end: Some(base + Duration::minutes(span_min + 60)),
    };

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    // `raw` is a real, mapped, queryable column — and not a tag field.
    let got = ice
        .grouped_counts_with_summary("logs-bench", "raw", None, Some(window))
        .await
        .unwrap();
    let reasons = fallback_reasons(snapshotter.snapshot());
    drop(guard);
    assert_eq!(
        reasons,
        vec![("column_not_covered".to_string(), 1)],
        "a GROUP BY on a non-tag column must report exactly WHY it missed the \
         rollup — these three modes used to be reported as one"
    );
    assert_eq!(
        got.as_ref().map(|g| g.source_label()),
        Some("materialized"),
        "the non-tag column should be served by the per-file footer sum"
    );
}

/// THE ROOT CAUSE CANDIDATE. Does a COMPACTION commit preserve the 2D rollup?
///
/// Measured on the 2026-08-26 round at 2B rows: the rollup covered exactly
/// **1 row of 2,016,590,695** for `level` — a column that IS a tag field, on a
/// table whose bulk appends all carry it, and with no column evicted by either
/// cap. Appends are known to accumulate correctly (the test above does six of
/// them). What happens between appends is compaction.
///
/// If a recluster commit does not carry the rollup forward, it resets to
/// whatever the last commit contributed — which is what `covered=1` looks like,
/// and would make every windowed GROUP BY on a compacted table fall back
/// permanently.
#[tokio::test]
async fn a_recluster_commit_preserves_the_rollup() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = seed(tmp.path(), 6, 400).await;
    let ident = ice.index_table_ident("logs-bench");
    let base = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let span_min = 6 * 400 * 7;
    let window = TimeBounds {
        start: Some(base + Duration::minutes(span_min * 3 / 4)),
        end: Some(base + Duration::minutes(span_min + 60)),
    };
    let bloom: Vec<&str> = vec!["host", "source", "sourcetype", "index"];

    // Sanity: the rollup serves it BEFORE compaction.
    let before = ice
        .grouped_counts_with_summary("logs-bench", "sourcetype", None, Some(window))
        .await
        .unwrap()
        .expect("counts before");
    assert_eq!(
        before.source_label(),
        "tier1_windowed_agg",
        "the rollup must serve this before compaction, or the test proves nothing"
    );

    // Compact every live file into one.
    let files = ice.live_data_files(&ident).await.unwrap();
    assert!(files.len() > 1, "need multiple files to compact");
    ice.recluster_files(&ident, files, &bloom).await.unwrap();

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let after = ice
        .grouped_counts_with_summary("logs-bench", "sourcetype", None, Some(window))
        .await
        .unwrap()
        .expect("counts after");
    let reasons = fallback_reasons(snapshotter.snapshot());
    drop(guard);

    assert_eq!(
        after.to_rows(),
        before.to_rows(),
        "compaction changed the ANSWER, which would be a correctness bug"
    );
    assert_eq!(
        after.source_label(),
        "tier1_windowed_agg",
        "a recluster commit dropped the 2D rollup — every windowed GROUP BY on a \
         compacted table falls back for good. reasons={reasons:?}"
    );
}
