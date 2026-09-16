//! Exact predicate pushdown × decoded-file cache regression coverage.
//!
//! The cache stores whole-file batches under a predicate-independent key. With
//! the cache enabled, exact-capable predicates therefore need to remain as
//! residual DataFusion filters on both cache misses and hits.

use chrono::{Duration, TimeZone, Utc};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

async fn plan_and_strings(ctx: &SessionContext, sql: &str) -> (String, Vec<Option<String>>) {
    let df = ctx.sql(sql).await.unwrap();
    let plan = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    let rows = df
        .collect()
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .collect();
    (plan, rows)
}

#[tokio::test]
async fn exact_filters_match_cache_disabled_results_on_cold_and_warm_cache() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let base = Utc.with_ymd_and_hms(2026, 9, 5, 12, 0, 0).single().unwrap();
    let hosts = [
        "alpha", "beta", "beta", "alpha", "gamma", "beta", "alpha", "gamma",
    ];
    let mut events = Vec::new();
    for (i, host) in hosts.iter().enumerate() {
        let mut event = Event::now(format!("row-{i}"));
        event.timestamp = base + Duration::seconds(i as i64);
        event.host = (*host).into();
        event.source = if i % 2 == 0 { "api" } else { "worker" }.into();
        event.sourcetype = if i % 2 == 0 { "json" } else { "text" }.into();
        event.index = if matches!(i, 1 | 3 | 7) {
            "dev"
        } else {
            "prod"
        }
        .into();
        event.attributes = (i % 2 == 0).then(|| format!(r#"{{"ordinal":{i}}}"#));
        events.push(event);
    }
    struct Case {
        name: &'static str,
        sql: &'static str,
        expected_rows: usize,
        warmup_sql: Option<&'static str>,
    }

    // Each case gets a distinct table/file path so its first cache-enabled
    // execution is a genuine cold miss. The LIMIT case uses an unbounded query
    // with the same projection to finish population before the warm execution.
    let cases = [
        Case {
            name: "equality",
            sql: "SELECT raw FROM events WHERE host = 'alpha'",
            expected_rows: 3,
            warmup_sql: None,
        },
        Case {
            name: "range",
            sql: "SELECT raw FROM events WHERE host >= 'beta' AND host < 'gamma'",
            expected_rows: 3,
            warmup_sql: None,
        },
        Case {
            name: "in",
            sql: "SELECT raw FROM events WHERE source IN ('api', 'edge')",
            expected_rows: 4,
            warmup_sql: None,
        },
        Case {
            name: "null",
            sql: "SELECT raw FROM events WHERE attributes IS NULL",
            expected_rows: 4,
            warmup_sql: None,
        },
        Case {
            name: "and",
            sql: "SELECT raw FROM events WHERE host = 'alpha' AND \"index\" = 'prod'",
            expected_rows: 2,
            warmup_sql: None,
        },
        Case {
            name: "or",
            sql: "SELECT raw FROM events WHERE host = 'gamma' OR source = 'worker'",
            expected_rows: 5,
            warmup_sql: None,
        },
        Case {
            name: "limit",
            sql: "SELECT host FROM events WHERE sourcetype = 'json' LIMIT 2",
            expected_rows: 2,
            warmup_sql: Some("SELECT host FROM events WHERE sourcetype = 'json'"),
        },
    ];

    for case in &cases {
        let case_root = tmp.path().join(case.name);
        let ice = IcebergContext::open(&case_root).await.unwrap();
        ice.append_events(&events).await.unwrap();
        let ctx = SessionContext::new();
        ice.register_with_datafusion(&ctx).await.unwrap();

        siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
        let (plan, rows) = plan_and_strings(&ctx, case.sql).await;
        assert!(
            !plan.contains("FilterExec"),
            "{}: cache-disabled exact pushdown lost its fast path:\n{plan}",
            case.name
        );
        assert_eq!(rows.len(), case.expected_rows, "{} oracle", case.name);

        siglake_storage::configure_query_scan_tuning(QueryScanTuning {
            file_cache_max_bytes: Some(64 * 1024 * 1024),
            file_cache_max_entries: Some(64),
            file_concurrency_limit: Some(1),
            ..Default::default()
        });
        let (cold_plan, cold) = plan_and_strings(&ctx, case.sql).await;
        assert!(
            cold_plan.contains("FilterExec"),
            "{}: cache-enabled scan must retain the residual filter:\n{cold_plan}",
            case.name
        );
        assert_eq!(cold, rows, "{} cold-cache result", case.name);

        if let Some(warmup_sql) = case.warmup_sql {
            let _ = plan_and_strings(&ctx, warmup_sql).await;
        }
        let (_, warm) = plan_and_strings(&ctx, case.sql).await;
        assert_eq!(warm, rows, "{} warm-cache result", case.name);
    }

    let snapshot = snapshotter.snapshot().into_vec();
    let cache_outcome = |outcome: &str| {
        snapshot
            .iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "siglake_query_scan_file_cache_requests_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == outcome)
            })
            .map(|(_, _, _, value)| match value {
                DebugValue::Counter(count) => *count,
                _ => 0,
            })
            .sum::<u64>()
    };
    let misses = cache_outcome("miss");
    let hits = cache_outcome("hit");
    assert!(
        misses >= cases.len() as u64,
        "each predicate shape must exercise a cold cache miss; got {misses}"
    );
    assert!(
        hits >= cases.len() as u64,
        "each predicate shape must exercise a warm cache hit; got {hits}"
    );
}
