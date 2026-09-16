//! Task #4494: why the decoded-file cache served 0 of 1,160 requests on the
//! 2026-09-15 50G bench rounds.
//!
//! The populate stream inserts only at end-of-stream
//! (`CachePopulateStream::poll_next`, the `Poll::Ready(None)` arm), and the
//! `miss` counter is incremented when the task stream is OPENED — which every
//! partition of a scan does before the plan's global LIMIT is satisfied. So a
//! `miss` is not a completed population, and a plan whose LIMIT is satisfied
//! before the file is drained leaves nothing behind for the next execution.
//!
//! These phases pin that shape under the bench's own cache tuning
//! (8 GiB / 16,384 entries):
//!
//! 1. a drained scan inserts, and the identical repeat hits — key reuse and
//!    entry sizing are not the problem;
//! 2. the same scan under a clipping LIMIT populates nothing, twice in a row:
//!    two misses, no `insert`, no `skip_oversized`, no
//!    `insert_skipped_contended`, no `evict` — the exact counter signature of
//!    the rounds' `query-metrics.txt`;
//! 3. one drained pass over the same table is what makes the limited repeat
//!    hit;
//! 4. a limited scan over four one-file partitions counts four misses and no
//!    population, so the rounds' 448 misses measure the tasks partitions
//!    opened, not population attempts.
//!
//! Since #4846 each of those clipped populations also charges
//! `outcome="abandoned"` from `CachePopulateStream`'s `Drop`, so phases 2 and 4
//! read as one abandonment per miss directly instead of by the absence of the
//! four insert-path series. The drained phases must charge none.
//!
//! Isolated in its own test binary: the query-scan tuning, the file batch
//! cache and the global metrics recorder are all process-wide.

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

// The bench rounds' tuning, verbatim:
// SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_BYTES=8589934592,
// SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_ENTRIES=16384.
const BENCH_CACHE_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const BENCH_CACHE_MAX_ENTRIES: usize = 16384;

/// Cache outcomes of one phase. `Snapshotter::snapshot()` drains the registry,
/// so every read is a delta against the previous phase.
#[derive(Debug, Default, PartialEq, Eq)]
struct Outcomes {
    hit: u64,
    miss: u64,
    bypass: u64,
    insert: u64,
    insert_skipped_contended: u64,
    skip_oversized: u64,
    abandoned: u64,
    evict: u64,
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
        bypass: sum("bypass"),
        insert: sum("insert"),
        insert_skipped_contended: sum("insert_skipped_contended"),
        skip_oversized: sum("skip_oversized"),
        abandoned: sum("abandoned"),
        evict: sum("evict"),
    }
}

/// Accumulate outcome deltas until `settled` holds or the deadline passes.
///
/// `abandoned` is charged from `CachePopulateStream::Drop`, and a clipped plan's
/// scan streams are dropped by whatever task last held them — for a multi-
/// partition plan that is a `CoalescePartitionsExec` worker, not the `collect()`
/// this test awaits. So the count can arrive a scheduler tick after the rows do.
/// Every snapshot drains the registry, hence the accumulation.
async fn outcomes_settling(
    snapshotter: &Snapshotter,
    settled: impl Fn(&Outcomes) -> bool,
) -> Outcomes {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut total = Outcomes::default();
    loop {
        let delta = outcomes(snapshotter);
        total.hit += delta.hit;
        total.miss += delta.miss;
        total.bypass += delta.bypass;
        total.insert += delta.insert;
        total.insert_skipped_contended += delta.insert_skipped_contended;
        total.skip_oversized += delta.skip_oversized;
        total.abandoned += delta.abandoned;
        total.evict += delta.evict;
        if settled(&total) || std::time::Instant::now() >= deadline {
            return total;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

fn events(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| Event::now(format!("row-{i} checkout latency={} ms", i % 97)))
        .collect()
}

async fn rows(ctx: &SessionContext, sql: &str) -> usize {
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

#[tokio::test]
async fn limit_clipped_scans_never_populate_the_decoded_file_cache() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    // Predicate-free projection: `raw_prune_spec` is None and `promoted_prune`
    // is empty, so these scans take the `miss`/populate branch rather than the
    // `bypass` branch the rounds' text and promoted-label shapes took.
    let scan_sql = "SELECT raw FROM events";
    // Four batches per file, so a small LIMIT is satisfied from the first one
    // and the file is left undrained — the bench suite's shape, where every
    // scan-served query carries `LIMIT 100`.
    let batch_rows = 256;
    let file_rows = 4 * batch_rows;
    siglake_storage::configure_query_scan_tuning(QueryScanTuning {
        file_cache_max_bytes: Some(BENCH_CACHE_MAX_BYTES),
        file_cache_max_entries: Some(BENCH_CACHE_MAX_ENTRIES),
        file_concurrency_limit: Some(1),
        batch_size: Some(batch_rows),
        ..Default::default()
    });

    // Phase 1: a scan that consumes the whole file inserts, and the identical
    // repeat hits. Key reuse, direction and the 8 GiB budget are all fine.
    let drained_root = tmp.path().join("drained");
    let drained = IcebergContext::open(&drained_root).await.unwrap();
    drained.append_events(&events(file_rows)).await.unwrap();
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    drained.register_with_datafusion(&ctx).await.unwrap();
    let _ = outcomes(&snapshotter);

    assert_eq!(rows(&ctx, scan_sql).await, file_rows);
    let cold = outcomes(&snapshotter);
    assert_eq!(
        cold,
        Outcomes {
            miss: 1,
            insert: 1,
            ..Default::default()
        },
        "a drained scan must miss once, insert once and abandon nothing: {cold:?}"
    );

    assert_eq!(rows(&ctx, scan_sql).await, file_rows);
    let warm = outcomes(&snapshotter);
    assert_eq!(
        warm,
        Outcomes {
            hit: 1,
            ..Default::default()
        },
        "the identical repeat of a drained scan must hit, and builds no populate \
         stream to abandon: {warm:?}"
    );

    // Phase 2: the same scan under a clipping LIMIT, on its own table so the
    // first execution is a genuine cold miss. Twice: nothing is inserted and
    // the repeat misses again. This is the rounds' counter signature — `miss`
    // with no `insert`, no `skip_oversized`, no `insert_skipped_contended` and
    // no `evict`, and no `siglake_query_scan_file_cache_{bytes,entries}` gauge
    // (both are only set from `QueryFileBatchCache::insert`).
    let clipped_root = tmp.path().join("clipped");
    let clipped = IcebergContext::open(&clipped_root).await.unwrap();
    clipped.append_events(&events(file_rows)).await.unwrap();
    let clipped_ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    clipped
        .register_with_datafusion(&clipped_ctx)
        .await
        .unwrap();
    let limited_sql = "SELECT raw FROM events LIMIT 10";

    // Each attempt's one population is now attributed rather than inferred:
    // `abandoned` says the stream decoded batches and was dropped before its
    // insert, which is the reading #4494 had to take from four absent series.
    for attempt in 1..=2 {
        assert_eq!(rows(&clipped_ctx, limited_sql).await, 10);
        let limited = outcomes_settling(&snapshotter, |o| o.abandoned >= 1).await;
        assert_eq!(
            limited,
            Outcomes {
                miss: 1,
                abandoned: 1,
                ..Default::default()
            },
            "attempt {attempt}: a limit-clipped scan must miss, populate nothing \
             and count one abandoned population"
        );
    }

    // Phase 3: the condition under which the limited repeat can hit — one pass
    // that drains the file to end-of-stream.
    assert_eq!(rows(&clipped_ctx, scan_sql).await, file_rows);
    let drain_pass = outcomes(&snapshotter);
    assert_eq!(
        drain_pass,
        Outcomes {
            miss: 1,
            insert: 1,
            ..Default::default()
        },
        "the drained pass must populate and abandon nothing: {drain_pass:?}"
    );
    assert_eq!(rows(&clipped_ctx, limited_sql).await, 10);
    let after_drain = outcomes(&snapshotter);
    assert_eq!(
        after_drain,
        Outcomes {
            hit: 1,
            ..Default::default()
        },
        "after one drained pass the limited repeat hits: {after_drain:?}"
    );

    // Phase 4: one miss per PARTITION, none of them a population attempt —
    // the rounds' per-execution miss count. The label shapes keep a residual
    // `FilterExec` above the scan when the cache is on (see
    // `exact_filter_file_cache.rs`), so DataFusion cannot push their LIMIT into
    // the scan and every partition opens its first task before the global limit
    // is satisfied. Four one-file partitions, a LIMIT filled from the first
    // batch: four misses, nothing read to end-of-stream, nothing inserted.
    // The rounds' 448 is that count — 12 executions x 16 partitions for
    // `label_filter`, 12 x 8 for `label_filter_last25`, 10 x 16 for
    // `multi_label_and` — not 448 populations that failed to stick.
    siglake_storage::configure_query_scan_tuning(QueryScanTuning {
        file_cache_max_bytes: Some(BENCH_CACHE_MAX_BYTES),
        file_cache_max_entries: Some(BENCH_CACHE_MAX_ENTRIES),
        file_concurrency_limit: Some(4),
        batch_size: Some(batch_rows),
        ..Default::default()
    });
    let prefetch_root = tmp.path().join("prefetch");
    let prefetch = IcebergContext::open(&prefetch_root)
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO);
    for _ in 0..4 {
        let mut file = events(file_rows);
        for event in &mut file {
            event.host = "alpha".into();
        }
        prefetch.append_events(&file).await.unwrap();
    }
    let prefetch_ctx = siglake_storage::session_context_with_target_partitions(Some(4));
    prefetch
        .register_with_datafusion(&prefetch_ctx)
        .await
        .unwrap();
    let _ = outcomes(&snapshotter);
    assert_eq!(
        rows(
            &prefetch_ctx,
            "SELECT raw FROM events WHERE host = 'alpha' LIMIT 1"
        )
        .await,
        1
    );
    let partitioned = outcomes_settling(&snapshotter, |o| o.abandoned >= 4).await;
    assert_eq!(
        partitioned,
        Outcomes {
            miss: 4,
            abandoned: 4,
            ..Default::default()
        },
        "one miss per partition that opened its first task, no population, and \
         one abandoned population per miss"
    );

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
}
