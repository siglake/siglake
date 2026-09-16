//! The collect path must hand back a plan whose scan attribution is complete.
//!
//! `summarize_plan_runtime` is what `render_records` and the shard route put in
//! `stats.scan`; it reads the scan node's metrics, which each partition folds
//! only when its stream finishes. An early `LIMIT` on a multi-partition scan
//! leaves aborted partitions that fold later, so the collect path settles the
//! plan before returning (task #274). On a current-thread runtime the aborted
//! pumps get only the few polls the scheduler runs before it returns to the
//! collecting task, and each has far more work than that, so without the
//! settle `unsettled_partitions` here reads the aborted partitions — not a
//! race to win.

use std::sync::Arc;
use std::time::Duration;

use siglake_core::Event;
use siglake_query_server::limits::Priority;
use siglake_query_server::midflight::{
    collect_plan_with_rows_scanned_cap_routed, summarize_plan_runtime, CollectOutcome,
};
use siglake_storage::iceberg::IcebergContext;

const PARTITIONS: usize = 8;
const FILES: usize = 64;
const ROWS_PER_FILE: usize = 5_000;
const NEEDLE: &str = "host-mm";
/// A range only `host-mm` satisfies: statistics prune nothing (every file spans
/// `host-a` .. `host-z`) and bloom filters cannot serve a range, so every
/// needle-free file is read and decoded.
const PREDICATE: &str = "host > 'host-ml' AND host < 'host-mn'";

/// See `siglake-storage/tests/scan_attribution_settle.rs` for why file 0 is
/// the largest and the only one carrying the needle.
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

#[tokio::test]
async fn collect_returns_a_settled_plan_after_an_early_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    needle_table(&ice).await;

    let ctx = siglake_storage::session_context_with_target_partitions(Some(PARTITIONS));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx
        .sql(&format!(
            "SELECT host, raw FROM events WHERE {PREDICATE} LIMIT 1"
        ))
        .await
        .unwrap();
    let task_ctx = siglake_query_server::midflight::task_ctx_for(&df);
    let plan = df.create_physical_plan().await.unwrap();

    // `inline = true`: execute on this (current-thread) runtime rather than the
    // exec pool, so the aborted pumps are provably unable to run before the
    // collect returns unless the collect path waits for them.
    let outcome = collect_plan_with_rows_scanned_cap_routed(
        plan.clone(),
        task_ctx,
        10_000_000,
        Priority::Interactive,
        true,
    )
    .await
    .unwrap();
    let CollectOutcome::Ok(batches) = outcome else {
        panic!("unexpected outcome: {outcome:?}");
    };
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let runtime = summarize_plan_runtime(&plan);
    assert!(
        runtime.files_planned as usize == FILES,
        "all files planned: {runtime:?}"
    );
    assert_eq!(
        runtime.unsettled_partitions, 0,
        "the collect path returned while scan partitions were still unwinding; \
         stats.scan would be missing their counters: {runtime:?}"
    );
    assert!(
        runtime.files_read >= 2 && runtime.object_store_reads >= 2,
        "a multi-partition early-stopped scan opened more than the matching file, \
         and those opens must be in the response: {runtime:?}"
    );
    assert!(runtime.leaf_output_rows >= 1, "{runtime:?}");

    // Nothing request-attributable lands after the response is built.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let later = summarize_plan_runtime(&plan);
    assert_eq!(
        (
            later.files_read,
            later.row_groups_read,
            later.object_store_reads,
            later.bytes_scanned,
            later.leaf_output_rows,
        ),
        (
            runtime.files_read,
            runtime.row_groups_read,
            runtime.object_store_reads,
            runtime.bytes_scanned,
            runtime.leaf_output_rows,
        ),
        "scan counters moved after collect returned: {runtime:?} -> {later:?}"
    );
}
