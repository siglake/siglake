//! #4905's in-process predicate-keyed decoded-cache prototype.
//!
//! Production policy still bypasses population for a task carrying a converted
//! predicate. This gate enables the test-only prototype and pins four rules:
//! a drained predicate read can populate, another predicate gets another key, a
//! clipped prefix never becomes an entry, and a predicate-free whole-task entry
//! remains a valid fallback. Every answer is compared with the cache-disabled
//! path; planning stays `Inexact`, so the residual `FilterExec` is present on
//! both kinds of hit.

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

const ROWS: usize = 60_000;
const MATCHES: usize = 80;
const CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const CACHE_MAX_ENTRIES: usize = 64;

fn fixture() -> Vec<Event> {
    (0..ROWS)
        .map(|i| {
            let mut event = Event::now(format!("row-{i} checkout latency={} ms", i % 97));
            event.host = if i < MATCHES {
                "alpha"
            } else if i < 2 * MATCHES {
                "beta"
            } else {
                "bulk"
            }
            .into();
            event
        })
        .collect()
}

fn tuning(enabled: bool, prototype: bool) -> QueryScanTuning {
    QueryScanTuning {
        file_cache_max_bytes: enabled.then_some(CACHE_MAX_BYTES),
        file_cache_max_entries: enabled.then_some(CACHE_MAX_ENTRIES),
        file_concurrency_limit: Some(1),
        file_cache_predicate_key_prototype: prototype,
        ..Default::default()
    }
}

async fn raws(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("raw")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| column.value(i).to_string()));
    }
    rows
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Outcomes {
    hit: u64,
    miss: u64,
    insert: u64,
    bypass: u64,
    abandoned: u64,
}

fn outcomes(snapshotter: &Snapshotter) -> Outcomes {
    let snapshot = snapshotter.snapshot().into_vec();
    let sum = |outcome: &str| {
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
    Outcomes {
        hit: sum("hit"),
        miss: sum("miss"),
        insert: sum("insert"),
        bypass: sum("bypass"),
        abandoned: sum("abandoned"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn predicate_keyed_entries_require_exhaustion_and_keep_the_residual() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    ice.append_events(&fixture()).await.unwrap();
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let alpha = "SELECT raw FROM events WHERE host = 'alpha' LIMIT 100";
    let beta = "SELECT raw FROM events WHERE host = 'beta' LIMIT 100";
    let bulk = "SELECT raw FROM events WHERE host = 'bulk' LIMIT 100";
    let drain = "SELECT raw, host FROM events";

    siglake_storage::configure_query_scan_tuning(tuning(false, false));
    let expected_alpha = raws(&ctx, alpha).await;
    let expected_beta = raws(&ctx, beta).await;
    let expected_bulk = raws(&ctx, bulk).await;
    assert_eq!(expected_alpha.len(), MATCHES);
    assert_eq!(expected_beta.len(), MATCHES);
    assert_eq!(expected_bulk.len(), 100);

    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(true, true));
    let _ = outcomes(&snapshotter);

    assert_eq!(raws(&ctx, alpha).await, expected_alpha);
    assert_eq!(
        outcomes(&snapshotter),
        Outcomes {
            miss: 1,
            insert: 1,
            ..Default::default()
        },
        "a fully exhausted predicate read populates its own key"
    );
    assert_eq!(raws(&ctx, alpha).await, expected_alpha);
    assert_eq!(
        outcomes(&snapshotter),
        Outcomes {
            hit: 1,
            ..Default::default()
        }
    );

    assert_eq!(raws(&ctx, beta).await, expected_beta);
    assert_eq!(
        outcomes(&snapshotter),
        Outcomes {
            miss: 1,
            insert: 1,
            ..Default::default()
        },
        "a different predicate must not reuse alpha's entry"
    );

    assert_eq!(raws(&ctx, bulk).await, expected_bulk);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        outcomes(&snapshotter),
        Outcomes {
            miss: 1,
            abandoned: 1,
            ..Default::default()
        },
        "a clipped prefix must not become a complete predicate entry"
    );
    assert_eq!(raws(&ctx, bulk).await, expected_bulk);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        outcomes(&snapshotter),
        Outcomes {
            miss: 1,
            abandoned: 1,
            ..Default::default()
        }
    );

    assert_eq!(raws(&ctx, drain).await.len(), ROWS);
    assert_eq!(
        outcomes(&snapshotter),
        Outcomes {
            miss: 1,
            insert: 1,
            ..Default::default()
        }
    );
    assert_eq!(raws(&ctx, bulk).await, expected_bulk);
    assert_eq!(
        outcomes(&snapshotter),
        Outcomes {
            hit: 1,
            ..Default::default()
        },
        "the predicate-free key remains a safe fallback through the residual filter"
    );

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
    siglake_storage::clear_decoded_file_cache();
}
