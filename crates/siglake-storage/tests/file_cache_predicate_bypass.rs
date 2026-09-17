//! Task #4891: a cache-enabled install must not read MORE than a cache-disabled
//! one because a population wanted a reusable entry.
//!
//! An entry is keyed by file and projection, not by predicate, so the populate
//! read has to strip `task.predicate` — and then the reader's page index has
//! nothing to select on and the whole projection is decoded. On a
//! `host = '<label>' LIMIT 100` browse that was measured at 2.8x the
//! cache-disabled arm's warm latency (#4847), for a clipped browse that then
//! inserts nothing at all (#4494). Since #4891 a task carrying a converted
//! predicate takes the same bypass a raw-text or promoted-column prune takes.
//!
//! The phases:
//!
//! 1. the cache-disabled control, which must PRUNE (otherwise the fixture
//!    proves nothing) and which fixes the exact answer;
//! 2. the same browse with the cache on: `bypass`, no `miss`, no population,
//!    and the same pruning and the same rows as the control. Before the fix
//!    this arm read every page and charged `miss` + `abandoned`;
//! 3. a predicate-free drained scan still populates — the decline is on the
//!    populate path for predicate tasks, not on the cache;
//! 4. the predicate browse then HITS that entry and still answers exactly: the
//!    entry carries rows the predicate rejects and DataFusion's residual
//!    `FilterExec` is what removes them.
//!
//! Isolated in its own test binary: the query-scan tuning, the decoded-file
//! cache and the metrics recorder are all process-wide.

use std::sync::Arc;
use std::time::Duration;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::{QueryScanTuning, SiglakeIcebergTableScan};

const CACHE_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const CACHE_MAX_ENTRIES: usize = 16384;
/// Three parquet data pages per column at the writer's 20,000-row page limit,
/// in one row group (the row-group floor is 131,072 rows). The needle sits in
/// page 0 only, so `host = NEEDLE` selects that page and the page index skips
/// the other two — the pruning this test is about.
const ROWS: usize = 60_000;
const NEEDLES: usize = 120;
const NEEDLE: &str = "needle";

const BROWSE: &str = "SELECT raw FROM events WHERE host = 'needle' LIMIT 100";
/// Same projection as `BROWSE` (the residual filter keeps `host`), no
/// predicate: the shape that is still allowed to populate.
const DRAIN: &str = "SELECT raw, host FROM events";

fn fixture() -> Vec<Event> {
    (0..ROWS)
        .map(|i| {
            let mut event = Event::now(format!("row-{i} checkout latency={} ms", i % 97));
            event.host = if i < NEEDLES { NEEDLE } else { "bulk" }.into();
            event
        })
        .collect()
}

fn cache_tuning(enabled: bool) -> QueryScanTuning {
    QueryScanTuning {
        file_cache_max_bytes: enabled.then_some(CACHE_MAX_BYTES),
        file_cache_max_entries: enabled.then_some(CACHE_MAX_ENTRIES),
        file_concurrency_limit: Some(1),
        ..Default::default()
    }
}

/// Cache outcomes of one phase. `Snapshotter::snapshot()` drains the registry,
/// so every read is a delta against the previous phase.
#[derive(Debug, Default, PartialEq, Eq)]
struct Outcomes {
    hit: u64,
    miss: u64,
    bypass: u64,
    insert: u64,
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
        bypass: sum("bypass"),
        insert: sum("insert"),
        abandoned: sum("abandoned"),
    }
}

/// What the READER did for one execution, from the scan node's own
/// attribution counters.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Reads {
    rows_pruned_selection: usize,
    row_groups_read: usize,
    files_read: usize,
}

/// Run `sql`, returning its `raw` column and the scan node's attribution.
async fn run(ctx: &SessionContext, sql: &str) -> (Vec<String>, Reads) {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
        .await
        .unwrap();
    // A clipped plan closes the root before its partitions have folded their
    // counters into the node (#274); one partition here, but settle anyway so
    // the read is never a partial fold.
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(10));
    assert!(settle.await.complete, "scan partitions did not settle");

    let mut rows = Vec::new();
    for batch in &batches {
        let column = batch
            .column_by_name("raw")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push(column.value(i).to_string());
        }
    }

    let mut scans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    plan.apply(|node| {
        if node
            .as_any()
            .downcast_ref::<SiglakeIcebergTableScan>()
            .is_some()
        {
            scans.push(node.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert_eq!(scans.len(), 1, "one scan leaf expected for {sql}");
    let metrics = scans[0].metrics().unwrap().aggregate_by_name();
    let sum = |name: &str| {
        metrics
            .sum_by_name(name)
            .map(|m| m.as_usize())
            .unwrap_or_default()
    };
    (
        rows,
        Reads {
            rows_pruned_selection: sum("rows_pruned_selection"),
            row_groups_read: sum("row_groups_read"),
            files_read: sum("files_read"),
        },
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_predicate_browse_prunes_with_the_cache_on() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    ice.append_events(&fixture()).await.unwrap();
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();

    // Phase 1: the control. Warm the object/footer caches first so the
    // comparison below is about pages decoded, not first-touch I/O.
    siglake_storage::configure_query_scan_tuning(cache_tuning(false));
    let _ = run(&ctx, BROWSE).await;
    let (expected, control) = run(&ctx, BROWSE).await;
    assert_eq!(expected.len(), 100, "the browse is clipped by its LIMIT");
    assert!(
        control.rows_pruned_selection > 0,
        "the fixture is not page-prunable, so this test cannot tell the two arms \
         apart: {control:?}"
    );
    let _ = outcomes(&snapshotter);

    // Phase 2: the same browse with the cache on. It must read what the control
    // read — before #4891 the populate path stripped the predicate and this arm
    // pruned nothing, decoding all three pages for an entry it never inserted.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(cache_tuning(true));
    for attempt in 1..=2 {
        let (rows, reads) = run(&ctx, BROWSE).await;
        assert_eq!(rows, expected, "attempt {attempt}: answer diverged");
        assert_eq!(
            reads, control,
            "attempt {attempt}: the cache-enabled arm read differently from the \
             cache-disabled control"
        );
        // `abandoned` is charged from the populate stream's Drop, which for a
        // clipped plan can land a tick after the rows do. There is no populate
        // stream on this path at all, so a settled zero is the assertion —
        // hence the delay before the read.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            outcomes(&snapshotter),
            Outcomes {
                bypass: 1,
                ..Default::default()
            },
            "attempt {attempt}: a predicate task must bypass the cache, not open \
             a population of a stripped read"
        );
    }
    eprintln!("BYPASS control={control:?} (cache-enabled arm identical)");

    // Phase 3: a predicate-free drained scan still populates. The decline is
    // narrow: it is about reading a file under no predicate, not about the
    // cache.
    let (drained, _) = run(&ctx, DRAIN).await;
    assert_eq!(drained.len(), ROWS);
    assert_eq!(
        outcomes(&snapshotter),
        Outcomes {
            miss: 1,
            insert: 1,
            ..Default::default()
        },
        "an eligible predicate-free drained scan must still populate"
    );

    // Phase 4: the predicate browse now HITS that entry. The entry holds rows
    // `host = 'needle'` rejects; the residual FilterExec removes them, which is
    // what `filter_pushdown_with_file_cache` exists for.
    let (hit_rows, hit_reads) = run(&ctx, BROWSE).await;
    assert_eq!(
        outcomes(&snapshotter),
        Outcomes {
            hit: 1,
            ..Default::default()
        },
        "the browse must be served from the entry the drained scan left"
    );
    assert_eq!(
        hit_rows, expected,
        "a cross-predicate hit must answer exactly what the cache-disabled \
         control answered"
    );
    assert_eq!(
        hit_reads,
        Reads::default(),
        "a hit builds no reader: {hit_reads:?}"
    );

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
    siglake_storage::clear_decoded_file_cache();
}
