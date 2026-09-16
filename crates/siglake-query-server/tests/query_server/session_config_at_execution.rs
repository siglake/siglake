//! A `datafusion.execution.*` option set on the session that PLANNED a query
//! must reach the operators that execute it (#2251).
//!
//! The mid-flight collector used to execute every plan with
//! `siglake_storage::bounded_task_context()` — `TaskContext::default()` plus
//! the shared runtime — which carries a DEFAULT `SessionConfig`. An option set
//! on the session was therefore dropped at execution with no error and no log
//! line: measured in
//! `tests/jaeger_render_cost_measurement.rs::what_the_producer_hands_the_collector`,
//! where a session `batch_size` of 1,024 produced byte-for-byte the 8,192-row
//! batches of the default.
//!
//! `batch_size` is the probe because it is observable in the RESULT: a
//! `SortExec` sizes its output by it, so the batch row counts the collector
//! hands back say whether the option arrived. The two arms are what makes the
//! assertion non-vacuous — the default arm must produce ONE batch of every
//! row, which is also what the old, config-discarding collector produced for
//! both.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use siglake_core::Event;
use siglake_query_server::limits::Priority;
use siglake_query_server::midflight::{
    collect_plan_with_rows_scanned_cap_routed, task_ctx_for, CollectOutcome,
};
use siglake_storage::iceberg::IcebergContext;

/// Below DataFusion's default `batch_size` (8,192), so the default arm is a
/// single batch and the two arms cannot be confused.
const ROWS: usize = 5_000;
/// The non-default option under test. A divisor of nothing in particular: the
/// arm asserts on the largest batch, not on a count.
const BATCH_SIZE: usize = 1_000;

/// Plan and collect `SELECT raw FROM events ORDER BY raw` through the
/// mid-flight collector, returning the rows of each batch it handed back.
///
/// `ORDER BY` for the `SortExec`: a bare scan is batched by the parquet
/// reader's own tuning, which is process-global and a different knob.
async fn collected_batch_rows(ice: &IcebergContext, batch_size: Option<usize>) -> Vec<usize> {
    // One partition so the sort has a single output stream and its batching is
    // not a repartition artefact.
    let ctx = match batch_size {
        None => siglake_storage::session_context_with_target_partitions(Some(1)),
        Some(rows) => {
            let mut state =
                siglake_storage::session_context_with_target_partitions(Some(1)).state();
            state.config_mut().options_mut().execution.batch_size = rows;
            SessionContext::new_with_state(state)
        }
    };
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx
        .sql("SELECT raw FROM events ORDER BY raw")
        .await
        .unwrap();
    let task_ctx = task_ctx_for(&df);
    assert_eq!(
        task_ctx.session_config().batch_size(),
        batch_size.unwrap_or(8_192),
        "the collector's execution context must carry the planning session's config"
    );
    assert!(
        Arc::ptr_eq(
            &task_ctx.runtime_env(),
            &siglake_storage::shared_query_runtime_env()
        ),
        "the session's config must arrive on the SHARED runtime: the bounded \
         memory pool, the spill configuration and the object-store registry \
         all hang off it"
    );

    let plan = df.create_physical_plan().await.unwrap();
    // `inline = true`: execute on this runtime rather than the exec pool, so
    // the batches come back from a plan this test can reason about alone.
    let outcome = collect_plan_with_rows_scanned_cap_routed(
        plan,
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
    batches.iter().map(|b| b.num_rows()).collect()
}

#[tokio::test]
async fn a_session_execution_option_reaches_the_collector() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let events: Vec<Event> = (0..ROWS)
        .map(|i| Event::now(format!("row {i:06} payload")))
        .collect();
    ice.append_events(&events).await.unwrap();

    let default_arm = collected_batch_rows(&ice, None).await;
    assert_eq!(
        default_arm.iter().sum::<usize>(),
        ROWS,
        "default arm returned {default_arm:?}"
    );
    assert_eq!(
        default_arm,
        vec![ROWS],
        "with the default 8,192-row batch size the whole result is one batch; \
         if this splits, the probe below no longer distinguishes the two arms"
    );

    let tuned_arm = collected_batch_rows(&ice, Some(BATCH_SIZE)).await;
    assert_eq!(
        tuned_arm.iter().sum::<usize>(),
        ROWS,
        "the option must not change the result: {tuned_arm:?}"
    );
    assert_eq!(
        tuned_arm.iter().copied().max(),
        Some(BATCH_SIZE),
        "`datafusion.execution.batch_size` = {BATCH_SIZE} was set on the \
         planning session and did not reach execution: the collector produced \
         {tuned_arm:?}. Before #2251 this arm was byte-for-byte the default \
         one ({default_arm:?})."
    );
}
