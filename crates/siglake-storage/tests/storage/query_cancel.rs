//! End-to-end: cancelling a query must stop its SCAN, through the real provider.
//!
//! The unit tests around `QueryCancel` exercise the take_while in isolation,
//! which proves the primitive but not the wiring -- and the wiring is the part
//! that was broken. This drives a real `IcebergContext`, a real
//! `SessionContext` carrying the extension, and the real
//! `SiglakeIcebergTableScan`, so it fails if the token stops reaching the plan
//! node for any reason (a refactor of `try_new`, a lost extension, a new code
//! path that builds its own session).
//!
//! Why it matters: on 2026-08-19 a cancelled query kept scanning for 35 minutes
//! after the request was gone, and enough of those wedged the host.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use futures::StreamExt;

use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::{CancelOnDrop, QueryCancel};

const FILES: usize = 40;
const ROWS_PER_FILE: usize = 512;
const TOTAL_ROWS: usize = FILES * ROWS_PER_FILE;

async fn fixture() -> (tempfile::TempDir, IcebergContext) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    // Deliberately more data than the read path can have buffered when the
    // first batch arrives. With a handful of tiny files the whole table is
    // already decoded by then, and "cancellation stopped further scanning" is
    // unobservable -- the first version of this test asserted 0 remaining rows
    // and failed for exactly that reason, not because the fix was wrong.
    for chunk in 0..FILES {
        let events: Vec<Event> = (0..ROWS_PER_FILE)
            .map(|i| Event::now(format!("row {chunk}-{i} payload")))
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    (tmp, ice)
}

fn ctx_with(cancel: Option<QueryCancel>) -> SessionContext {
    let ctx = SessionContext::new();
    let Some(cancel) = cancel else {
        return ctx;
    };
    let mut state = ctx.state();
    state.config_mut().set_extension(Arc::new(cancel));
    SessionContext::new_with_state(state)
}

/// Baseline: with a token present but NOT cancelled, the query is unaffected.
/// Without this, a token that broke every scan would pass the cancel test.
#[tokio::test]
async fn an_uncancelled_query_returns_all_rows() {
    let (_tmp, ice) = fixture().await;
    let ctx = ctx_with(Some(QueryCancel::new()));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let batches = ctx
        .sql("SELECT raw FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, TOTAL_ROWS, "an uncancelled query must be untouched");
}

/// A query cancelled BEFORE execution must scan nothing. This is the state a
/// timed-out request's plan is left in.
#[tokio::test]
async fn a_cancelled_query_scans_nothing() {
    let (_tmp, ice) = fixture().await;
    let cancel = QueryCancel::new();
    let ctx = ctx_with(Some(cancel.clone()));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx.sql("SELECT raw FROM events").await.unwrap();
    cancel.cancel(); // the request went away between planning and execution

    let batches = df.collect().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 0,
        "a cancelled query must stop at the source; rows here mean the scan \
         kept running after the request was gone"
    );
}

/// Cancelling MID-STREAM must end the scan rather than let it run to
/// completion -- the actual failure mode, where the consumer is gone and the
/// scan keeps going.
#[tokio::test]
async fn cancelling_mid_stream_ends_the_scan() {
    let (_tmp, ice) = fixture().await;
    let cancel = QueryCancel::new();
    let ctx = ctx_with(Some(cancel.clone()));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let mut stream = ctx
        .sql("SELECT raw FROM events")
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();

    let first = stream.next().await;
    assert!(
        first.is_some(),
        "the scan must start, or this proves nothing"
    );
    let before: usize = first.unwrap().unwrap().num_rows();

    cancel.cancel();

    let mut after = 0usize;
    while let Some(batch) = stream.next().await {
        after += batch.unwrap().num_rows();
    }
    // Rows already decoded when the flag flips are still delivered -- the fix
    // stops FURTHER scanning, it cannot recall work already done. What must not
    // happen is the scan running the table to completion, which is the defect.
    assert!(
        before + after < TOTAL_ROWS,
        "cancelled mid-stream, the scan still delivered the whole table \
         ({before} + {after} of {TOTAL_ROWS}) — it did not stop"
    );
}

/// The guard is the whole point: a request that is DROPPED -- not politely
/// finished -- must cancel its scans. This is the path the 60s timeout and a
/// client disconnect both take.
#[tokio::test]
async fn dropping_the_request_guard_stops_the_scan() {
    let (_tmp, ice) = fixture().await;
    let cancel = QueryCancel::new();
    let ctx = ctx_with(Some(cancel.clone()));
    ice.register_with_datafusion(&ctx).await.unwrap();

    let mut stream = ctx
        .sql("SELECT raw FROM events")
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();
    let before: usize = stream.next().await.unwrap().unwrap().num_rows();

    {
        let _guard = CancelOnDrop(cancel.clone());
    } // the request future is dropped here

    let mut after = 0usize;
    while let Some(batch) = stream.next().await {
        after += batch.unwrap().num_rows();
    }
    // `before + after`, not `after` alone: excluding the first batch makes
    // "< TOTAL_ROWS" trivially true, and an earlier version of this assertion
    // passed with the fix reverted for exactly that reason.
    assert!(
        before + after < TOTAL_ROWS,
        "dropping the request must stop its scan; it delivered {before} + \
         {after} of {TOTAL_ROWS} rows"
    );
}
