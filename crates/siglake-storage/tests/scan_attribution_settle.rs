//! An early-stopped multi-partition scan must be SETTLED before its counters
//! are read, or the reader sees a partial fold.
//!
//! The mechanism (task #274): `SourceMetricsStream` folds `files_read`,
//! `row_groups_read`, `object_store_reads` and the byte counters into the scan
//! node's metrics only when the partition stream finishes or drops. A `LIMIT`
//! that is satisfied by one partition closes the root stream; DataFusion aborts
//! the other partition pumps, but each abort lands on that pump's next poll.
//! Anyone who reads the node metrics between the root stream ending and those
//! polls sees leaf rows with reads missing — the 2026-09-03 50G `label_filter`
//! artifact (rows_scanned 106,648, files_read 0).
//!
//! On a current-thread runtime the aborted pumps get at most the few polls the
//! scheduler runs before it returns to this task, and each has far more work
//! than that, so the window is reliably open: the test asserts it is
//! (otherwise it proves nothing), then that `settle_scan_partitions` closes it
//! and that nothing lands afterwards.

use std::sync::Arc;
use std::time::Duration;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use futures::StreamExt;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::SiglakeIcebergTableScan;

const PARTITIONS: usize = 8;
const FILES: usize = 64;
const ROWS_PER_FILE: usize = 5_000;
/// The only `host` inside the range predicate below. Every file's `host`
/// statistics span `host-a` .. `host-z`, so the range prunes no file, and a
/// range is invisible to the bloom filters: every needle-free file costs a
/// footer read, a data read and a 5,000-row decode before it is known empty.
const NEEDLE: &str = "host-mm";
const PREDICATE: &str = "host > 'host-ml' AND host < 'host-mn'";

/// `FILES` appends. File 0 carries the needle in its first rows and is the
/// largest, so the unordered-limit re-split (largest first) makes it the first
/// task of partition 0 and the match is produced after one file. Every other
/// partition holds several needle-free files, each two IO hops plus a decode:
/// far more than the polls they can get before the root stream is closed.
async fn needle_table(ice: &IcebergContext) {
    for file in 0..FILES {
        let rows = if file == 0 {
            ROWS_PER_FILE + 1_000
        } else {
            ROWS_PER_FILE
        };
        let events: Vec<Event> = (0..rows)
            .map(|i| {
                let mut e = Event::now(format!("row {file}-{i} payload"));
                e.host = if file == 0 && i < 4 {
                    NEEDLE.to_string()
                } else {
                    format!("host-{}", (b'a' + (i % 26) as u8) as char)
                };
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
}

fn scan_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut out = Vec::new();
    plan.apply(|node| {
        if node
            .as_any()
            .downcast_ref::<SiglakeIcebergTableScan>()
            .is_some()
        {
            out.push(node.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    out
}

fn scan_of(node: &Arc<dyn ExecutionPlan>) -> &SiglakeIcebergTableScan {
    node.as_any()
        .downcast_ref::<SiglakeIcebergTableScan>()
        .unwrap()
}

/// The per-request attribution counters, summed across partitions, as the
/// query server's `summarize_plan_runtime` reads them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Counters {
    files_read: usize,
    row_groups_read: usize,
    object_store_reads: usize,
    bytes_scanned: usize,
}

fn counters(node: &Arc<dyn ExecutionPlan>) -> Counters {
    let metrics = node.metrics().unwrap().aggregate_by_name();
    let sum = |name: &str| {
        metrics
            .sum_by_name(name)
            .map(|m| m.as_usize())
            .unwrap_or_default()
    };
    Counters {
        files_read: sum("files_read"),
        row_groups_read: sum("row_groups_read"),
        object_store_reads: sum("object_store_reads"),
        bytes_scanned: sum("bytes_scanned"),
    }
}

#[tokio::test]
async fn an_early_limit_leaves_partitions_unfolded_until_settled() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    needle_table(&ice).await;

    let ctx = siglake_storage::session_context_with_target_partitions(Some(PARTITIONS));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = ctx
        .sql(&format!(
            "SELECT host, raw FROM events WHERE {PREDICATE} LIMIT 1"
        ))
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let scans = scan_nodes(&plan);
    assert_eq!(scans.len(), 1, "one scan leaf expected in {plan:?}");
    let scan_node = scans[0].clone();
    let scan = scan_of(&scan_node);
    let partitions = scan_node
        .properties()
        .output_partitioning()
        .partition_count();
    assert!(
        partitions > 1,
        "the filtered LIMIT must plan a multi-partition scan (got {partitions}); \
         a single partition is dropped synchronously and cannot show the window"
    );
    assert_eq!(scan.live_partitions(), 0, "nothing executed yet");

    // Drive the root stream to its end: the LIMIT is satisfied by partition 0,
    // the root closes, and DataFusion aborts the other pumps. The stream is
    // dropped inside this block, exactly as the collect path's frame does.
    let hosts = {
        // The shared runtime and nothing else: this asserts on the scan, not on
        // any session option, so the DEFAULT config is what it wants.
        #[allow(clippy::disallowed_methods)]
        let mut stream =
            execute_stream(plan.clone(), siglake_storage::bounded_task_context()).unwrap();
        let mut hosts = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let col = batch
                .column_by_name("host")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            hosts.extend(col.iter().map(|h| h.unwrap().to_string()));
        }
        hosts
    };
    assert_eq!(
        hosts,
        vec![NEEDLE.to_string()],
        "the pushed range filter is exact"
    );

    // THE WINDOW. The aborted pumps have not run since the root closed (this
    // task has not yielded), so their partitions are still live and their
    // counters are not in the node metrics yet.
    let live_before = scan.live_partitions();
    let before = counters(&scan_node);
    assert!(
        live_before > 0,
        "no partition was still unwinding when the root stream ended; the fixture \
         no longer reproduces the window this test exists for (counters {before:?})"
    );

    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(5)).await;
    assert!(
        settle.complete,
        "the partitions must finish within the deadline: {settle:?}"
    );
    assert_eq!(settle.live_partitions, 0);
    assert_eq!(scan.live_partitions(), 0);

    // Settled counters are what a response must carry. They include the
    // aborted partitions' folds (each had at least opened a footer, so the
    // fold moved `files_read`), and nothing lands after the settle returns.
    let after = counters(&scan_node);
    assert!(
        after.files_read > before.files_read,
        "settling must fold the aborted partitions' reads: before {before:?}, after {after:?}"
    );
    assert!(
        after.files_read >= 1 && after.object_store_reads >= 1,
        "{after:?}"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        counters(&scan_node),
        after,
        "request-attributable scan work landed after the plan was settled"
    );
    eprintln!(
        "SETTLE partitions={partitions} live_before={live_before} before={before:?} \
         after={after:?} waited_ms={:.2}",
        settle.waited.as_secs_f64() * 1000.0
    );
}

/// A plan whose partitions all ran to completion has nothing to wait for: the
/// settle is immediate and reports complete. This is the common (no early
/// stop) path, and it must not pay for the fix.
#[tokio::test]
async fn a_fully_drained_scan_settles_immediately() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    needle_table(&ice).await;

    let ctx = siglake_storage::session_context_with_target_partitions(Some(PARTITIONS));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = ctx
        .sql(&format!(
            "SELECT count(*) AS n FROM events WHERE {PREDICATE}"
        ))
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let scans = scan_nodes(&plan);
    assert_eq!(scans.len(), 1);

    {
        // As above: the shared runtime alone, no session option under test.
        #[allow(clippy::disallowed_methods)]
        let mut stream =
            execute_stream(plan.clone(), siglake_storage::bounded_task_context()).unwrap();
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
    }
    assert_eq!(scan_of(&scans[0]).live_partitions(), 0);
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(5)).await;
    assert_eq!(
        settle,
        siglake_storage::ScanSettle {
            complete: true,
            live_partitions: 0,
            waited: Duration::ZERO,
        }
    );
}
