//! #4865: an unordered, row-clipped `LIMIT n` must not start every partition.
//!
//! The shape the card measured on a fully compacted 15-file / 98.5M-row table:
//! one partition per file, no ordering to preserve, and a residual `FilterExec`
//! above the scan that DataFusion cannot push a limit through. Every partition
//! opens at once, every one lands its first batch within 2.0–3.4 ms of the
//! others, and by the time the global limit cancels them the scan has decoded
//! 419,840 rows to return 100. The same query over a layout with a small
//! unmerged tail decoded 597,696 bytes, because the tiny partition answered the
//! limit first and the whole-file partitions were cancelled having decoded
//! nothing.
//!
//! The fix is a staged start (`ScanAdmission`): scheduling only, so a partition
//! that waits still reads every row it would have read and the limit still
//! lives above the residual filter. These tests hold the LAYOUT fixed and vary
//! only the ramp, through the `ClippedAdmissionWave` session extension — `0` is
//! the pre-#4865 behavior — so the comparison is a matched A/B on identical
//! inputs rather than the benchmarks report's cross-build attribution.
//!
//! The controls are the ways a scheduling fix can silently lose rows: a common
//! match (the case the ramp is for), a match that only exists in the last file,
//! fewer matches than the limit asks for, and an `OFFSET`. Unordered SQL has no
//! defined row selection, so they check qualifying membership and cardinality,
//! never a particular set of rows.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::SiglakeIcebergTableScan;

const PARTITIONS: usize = 16;
const FILES: usize = 16;
const ROWS_PER_FILE: usize = 3_000;
/// Invisible to file statistics (`raw` spans the same range everywhere) and
/// present in every file, so no file is pruned and every partition would decode
/// if it were allowed to start.
const NEEDLE: &str = "queen";
const MATCHES_PER_FILE: usize = 64;
/// A late file, so the "too few matches" control cannot be answered by the head
/// of the ramp.
const FILES_LAST_MATCH: usize = FILES - 3;

/// A row wide enough that a decoded batch is worth counting, and a `raw` whose
/// trigrams are the same in every file except for the needle.
fn payload(file: usize, row: usize, needle: bool) -> String {
    let body = if needle { NEEDLE } else { "pawn" };
    format!(
        "2026-09-17 service=checkout file={file:04} row={row:06} outcome={body} \
         detail=lorem ipsum dolor sit amet consectetur adipiscing elit sed do"
    )
}

/// `FILES` appends, each one data file. `needles(file)` says how many of that
/// file's rows carry the needle, placed at the head of the file.
async fn table_with(ice: &IcebergContext, needles: impl Fn(usize) -> usize) {
    for file in 0..FILES {
        let want = needles(file);
        let events: Vec<Event> = (0..ROWS_PER_FILE)
            .map(|row| Event::now(payload(file, row, row < want)))
            .collect();
        ice.append_events(&events).await.unwrap();
    }
}

/// What the query server builds for `SELECT … WHERE <residual> LIMIT n` with no
/// `ORDER BY`: `ClippedScanLimit` and neither ordering extension. `wave` is the
/// admission ramp factor under test; `0` is the pre-#4865 scan.
fn clipped_context(limit: usize, wave: usize) -> SessionContext {
    let base = siglake_storage::session_context_with_target_partitions(Some(PARTITIONS));
    let mut state = base.state();
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::ClippedScanLimit { limit }));
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::ClippedAdmissionWave {
            factor: wave,
        }));
    SessionContext::new_with_state(state)
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

/// The settled per-request scan attribution, summed across partitions, as the
/// query server's `summarize_plan_runtime` reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Work {
    partitions: usize,
    files_read: usize,
    row_groups_read: usize,
    decoded_bytes: usize,
    /// NOT S3 traffic: the vendored reader's fetched-byte counter over a
    /// `file://` store. Reported because the card asks for it, compared only
    /// against the other arm of the same A/B.
    bytes_scanned: usize,
}

struct Run {
    rows: Vec<String>,
    work: Work,
}

/// Plan, drain and settle one arm, then read the scan node's counters. The
/// settle is what makes the numbers comparable: a `LIMIT` that closes the root
/// stream leaves the cancelled partitions folding on their own workers, and a
/// snapshot taken before they land reads a partial fold (#274).
async fn run_arm(ctx: &SessionContext, sql: &str, column: &str) -> Run {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let scans = scan_nodes(&plan);
    assert_eq!(scans.len(), 1, "one scan leaf expected in {plan:?}");
    let partitions = scans[0]
        .properties()
        .output_partitioning()
        .partition_count();

    let rows = {
        // The shared runtime and nothing else: the session options under test
        // are already on `ctx`.
        #[allow(clippy::disallowed_methods)]
        let mut stream = execute_stream(plan.clone(), siglake_storage::bounded_task_context())
            .expect("execute the scan");
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let col = batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            rows.extend(col.iter().map(|v| v.unwrap().to_string()));
        }
        rows
    };

    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(30)).await;
    assert!(
        settle.complete,
        "arm must settle before it is read: {settle:?}"
    );

    let metrics = scans[0].metrics().unwrap().aggregate_by_name();
    let sum = |name: &str| {
        metrics
            .sum_by_name(name)
            .map(|m| m.as_usize())
            .unwrap_or_default()
    };
    Run {
        rows,
        work: Work {
            partitions,
            files_read: sum("files_read"),
            row_groups_read: sum("row_groups_read"),
            decoded_bytes: sum("decoded_bytes"),
            bytes_scanned: sum("bytes_scanned"),
        },
    }
}

fn assert_all_qualify(rows: &[String], sql: &str) {
    for row in rows {
        assert!(
            row.contains(NEEDLE),
            "a row that does not satisfy the residual filter came back from {sql}: {row}"
        );
    }
    let distinct: HashSet<&String> = rows.iter().collect();
    assert_eq!(distinct.len(), rows.len(), "duplicate rows from {sql}");
}

/// THE MEASUREMENT. One layout, two arms, the ramp the only difference.
///
/// Both arms are scheduler-dependent: how far the ungated fan-out gets before
/// the root stream closes is a race, and so is how many credits the ramp
/// collects in the same window. So the arms are INTERLEAVED and summed over
/// several pairs rather than compared once — the same discipline the loopback
/// ingest A/Bs use. The mechanism itself is asserted separately and without a
/// scheduler, in `a_partition_behind_the_ramp_reads_nothing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_admission_ramp_stops_every_partition_decoding_for_a_clipped_limit() {
    const PAIRS: usize = 3;
    const LIMIT: usize = 20;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let gated_ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&gated_ctx).await.unwrap();
    let ungated_ctx = clipped_context(LIMIT, 0);
    ice.register_with_datafusion(&ungated_ctx).await.unwrap();

    let (mut gated_files, mut ungated_files) = (0usize, 0usize);
    let (mut gated_decoded, mut ungated_decoded) = (0usize, 0usize);
    for pair in 0..PAIRS {
        let gated = run_arm(&gated_ctx, &sql, "raw").await;
        let ungated = run_arm(&ungated_ctx, &sql, "raw").await;
        eprintln!(
            "CLIPPED-ADMISSION pair={pair} gated={:?} ungated={:?}",
            gated.work, ungated.work
        );

        assert!(
            gated.work.partitions > 4,
            "the fixture must plan a wide scan for the ramp to hold anything back: {:?}",
            gated.work
        );
        assert_eq!(
            gated.work.partitions, ungated.work.partitions,
            "the arms must share a layout; only the ramp may differ"
        );
        // Both arms answer the query.
        assert_eq!(gated.rows.len(), LIMIT, "gated arm must fill the limit");
        assert_eq!(ungated.rows.len(), LIMIT, "ungated arm must fill the limit");
        assert_all_qualify(&gated.rows, &sql);
        assert_all_qualify(&ungated.rows, &sql);
        // The ungated arm is the shape the card measured: more than one
        // partition decodes speculatively before the limit can cancel them.
        // Asserted, not assumed — without it the comparison below proves
        // nothing. How FAR the fan-out gets is bounded by how long the root
        // stream takes to close rather than by the partition count (at 32
        // partitions this fixture still only reached 6 files), so the floor is
        // "more than one partition", not a fraction of the plan.
        assert!(
            ungated.work.row_groups_read > 1,
            "the ungated arm no longer reproduces the fan-out this test exists for: {:?}",
            ungated.work
        );

        gated_files += gated.work.files_read;
        ungated_files += ungated.work.files_read;
        gated_decoded += gated.work.decoded_bytes;
        ungated_decoded += ungated.work.decoded_bytes;
    }

    eprintln!(
        "CLIPPED-ADMISSION total pairs={PAIRS} files gated={gated_files} ungated={ungated_files} \
         decoded_bytes gated={gated_decoded} ungated={ungated_decoded}"
    );
    // Decoded bytes, not files read: a file counts as read the moment its
    // footer lands, so `files_read` measures how many partitions won a
    // scheduling race with the root stream closing (7/4/5 across three
    // otherwise identical ungated pairs on this box). Decoded bytes count the
    // batches that were actually built, which is the cost the card is about and
    // which the ramp bounds directly — the gated arm decoded exactly two
    // batches in every pair measured.
    assert!(
        gated_decoded * 3 <= ungated_decoded * 2,
        "the ramp must cut what an early-stopped clipped LIMIT decodes by at least a third: \
         gated {gated_decoded} ungated {ungated_decoded} over {PAIRS} pairs \
         (files gated {gated_files} ungated {ungated_files})"
    );
}

/// Sparse matches: the needle exists only in the LAST file, so the ramp has to
/// open every partition before the limit can be filled. This is the case a
/// staged start can break — and the one that says the ramp is scheduling and
/// not a source-row cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_needle_in_the_last_file_alone_still_fills_the_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |file| {
        if file == FILES - 1 {
            MATCHES_PER_FILE
        } else {
            0
        }
    })
    .await;

    const LIMIT: usize = 20;
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let run = run_arm(&ctx, &sql, "raw").await;

    eprintln!("CLIPPED-ADMISSION sparse={:?}", run.work);
    assert_eq!(
        run.rows.len(),
        LIMIT,
        "a match reachable only through the whole ramp must still fill the limit"
    );
    // The rows ARE the proof that the ramp reached the last partition: nothing
    // else in the table qualifies. `files_read` says nothing here — the LIKE
    // trigram bloom prunes the needle-free files, so most partitions correctly
    // never open one.
    assert_all_qualify(&run.rows, &sql);
}

/// Fewer matches than the limit asks for: the scan cannot early-stop, so every
/// partition must run to exhaustion and every qualifying row must come back.
/// This is the liveness control — a ramp that failed to widen would hang here
/// or return short.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn too_few_matches_return_every_qualifying_row() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    // Seven matches in total, spread across three files, none of them the first.
    table_with(&ice, |file| match file {
        3 => 2,
        9 => 4,
        FILES_LAST_MATCH => 1,
        _ => 0,
    })
    .await;

    const LIMIT: usize = 20;
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let run = run_arm(&ctx, &sql, "raw").await;

    eprintln!("CLIPPED-ADMISSION insufficient={:?}", run.work);
    let expected: HashSet<String> = [(3usize, 2usize), (9, 4), (FILES_LAST_MATCH, 1)]
        .into_iter()
        .flat_map(|(file, want)| (0..want).map(move |row| payload(file, row, true)))
        .collect();
    let got: HashSet<String> = run.rows.iter().cloned().collect();
    assert_eq!(got, expected, "every qualifying row must come back");
    assert_eq!(run.rows.len(), 7);
}

/// `OFFSET` rides into the session folded into `ClippedScanLimit` (limit +
/// offset, `sql.rs::clipping_scan_limit`). The scan owes that many rows; the
/// page that comes back must still be `LIMIT` distinct qualifying rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offset_page_is_a_full_page_of_qualifying_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    const LIMIT: usize = 5;
    const OFFSET: usize = 10;
    let sql =
        format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT} OFFSET {OFFSET}");
    let ctx = clipped_context(LIMIT + OFFSET, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let run = run_arm(&ctx, &sql, "raw").await;

    eprintln!("CLIPPED-ADMISSION offset={:?}", run.work);
    assert_eq!(run.rows.len(), LIMIT, "the page must be full");
    assert_all_qualify(&run.rows, &sql);
}

/// THE MECHANISM, with the scheduler taken out of it. The end-to-end arms above
/// race a ramp against the root stream closing, so their margin is a
/// measurement. This one drives the partition streams by hand: a partition
/// behind the ramp is polled, stays `Pending` because nothing can widen it, and
/// — the claim the fix rests on — has fetched nothing at all, because the
/// reader below the gate was never polled.
#[tokio::test]
async fn a_partition_behind_the_ramp_reads_nothing() {
    use futures::task::noop_waker_ref;
    use std::task::Context as TaskContext;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    const LIMIT: usize = 20;
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = ctx
        .sql(&format!(
            "SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}"
        ))
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let scan = scan_nodes(&plan).pop().expect("one scan leaf");
    let partitions = scan.properties().output_partitioning().partition_count();
    assert!(partitions > 4, "need a wide scan, got {partitions}");

    // The session's own context, so the partition streams execute under the
    // same config the plan was built from (#2251).
    let task_ctx = siglake_storage::bounded_task_context_from(
        datafusion::execution::TaskContext::from(&ctx.state()),
    );
    // Executed in order, so tickets follow partition index: partition 0 holds
    // ticket 0 and the rest are queued behind it.
    let mut streams: Vec<_> = (0..partitions)
        .map(|p| scan.execute(p, task_ctx.clone()).unwrap())
        .collect();

    let mut cx = TaskContext::from_waker(noop_waker_ref());
    let gated = partitions - 1;
    for poll in 0..8 {
        assert!(
            streams[gated].poll_next_unpin(&mut cx).is_pending(),
            "partition {gated} became ready on poll {poll} with nothing to widen the ramp"
        );
        tokio::task::yield_now().await;
    }

    // Dropping folds every partition's counters. Only the gated one was ever
    // polled, and it never reached the reader, so the whole plan read nothing.
    drop(streams);
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(30)).await;
    assert!(settle.complete, "{settle:?}");
    let metrics = scan.metrics().unwrap().aggregate_by_name();
    let sum = |name: &str| {
        metrics
            .sum_by_name(name)
            .map(|m| m.as_usize())
            .unwrap_or_default()
    };
    assert_eq!(
        (
            sum("files_read"),
            sum("object_store_reads"),
            sum("bytes_scanned")
        ),
        (0, 0, 0),
        "a partition held by the ramp must not have started its read"
    );
}

/// An ORDER BY scan with no residual filter advertises its ordering, so
/// `preserve_task_order` keeps it out of the ramp entirely. Guard against a
/// future widening of the gate: a `SortPreservingMerge` polls every partition
/// for a first batch before it can emit anything, so gating one would deadlock
/// rather than slow down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordered_scan_is_outside_the_ramp() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    const LIMIT: usize = 20;
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let sql = format!("SELECT raw FROM events ORDER BY timestamp LIMIT {LIMIT}");
    let run = tokio::time::timeout(Duration::from_secs(60), run_arm(&ctx, &sql, "raw"))
        .await
        .expect("an ordered scan must not wait on the unordered ramp");
    eprintln!("CLIPPED-ADMISSION ordered={:?}", run.work);
    assert_eq!(run.rows.len(), LIMIT);
}

/// An aggregate drains every partition by construction. The query server never
/// sets `ClippedScanLimit` for one (`sql.rs::clipping_scan_limit` excludes
/// aggregates), but if it ever did the ramp must widen to completion rather
/// than stall — and the answer must still be exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aggregate_under_the_hint_still_counts_every_row() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |file| if file % 2 == 0 { 3 } else { 0 }).await;

    let ctx = clipped_context(20, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let rows = tokio::time::timeout(
        Duration::from_secs(60),
        ctx.sql(&format!(
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%{NEEDLE}%'"
        ))
        .await
        .unwrap()
        .collect(),
    )
    .await
    .expect("the ramp must widen to a full drain")
    .unwrap();
    let n = rows[0]
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n as usize, FILES.div_ceil(2) * 3);
}
