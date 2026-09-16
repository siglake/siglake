//! End-to-end test: write Parquet files under a deep partition layout,
//! register the directory as a DataFusion table, and verify rows read back.

use datafusion::prelude::SessionContext;
use object_store::path::Path as ObjectPath;

use siglake_core::{events_schema, events_to_record_batch, Event};
use siglake_storage::{local_store, register_parquet_dir, session_context, write_batch_as_parquet};

#[tokio::test]
async fn write_then_query_recursive() {
    let tmp = tempfile::tempdir().unwrap();
    let store = local_store(tmp.path()).unwrap();

    let events: Vec<Event> = (0..7).map(|i| Event::now(format!("e{i}"))).collect();
    let batch = events_to_record_batch(&events).unwrap();

    let path = ObjectPath::from("events/2026/04/29/15/test.parquet");
    write_batch_as_parquet(store.as_ref(), &path, &batch)
        .await
        .unwrap();

    let ctx = session_context();
    let abs = std::fs::canonicalize(tmp.path()).unwrap();
    let url = format!("file://{}/events/", abs.display());

    register_parquet_dir(&ctx, &url, "events", Some(events_schema()))
        .await
        .unwrap();

    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 7);
}

/// Sanity check: object_store.list (which underlies ListingTable) sees the file.
/// Guards against future regressions in the storage abstraction.
#[tokio::test]
async fn object_store_list_is_recursive() {
    use futures::StreamExt;

    let tmp = tempfile::tempdir().unwrap();
    let store = local_store(tmp.path()).unwrap();
    let events: Vec<Event> = (0..3).map(|i| Event::now(format!("e{i}"))).collect();
    let batch = events_to_record_batch(&events).unwrap();
    let path = ObjectPath::from("events/2026/04/29/15/test.parquet");
    write_batch_as_parquet(store.as_ref(), &path, &batch)
        .await
        .unwrap();

    let prefix = ObjectPath::from("events");
    let mut stream = store.list(Some(&prefix));
    let mut found = Vec::new();
    while let Some(o) = stream.next().await {
        found.push(o.unwrap().location.to_string());
    }
    assert!(
        found.iter().any(|p| p.ends_with("test.parquet")),
        "object_store.list did not recurse; saw: {found:?}"
    );
}

/// Multi-file: two distinct partitions, both should be scanned.
#[tokio::test]
async fn multi_partition_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let store = local_store(tmp.path()).unwrap();

    let batch_a = events_to_record_batch(
        &(0..3)
            .map(|i| Event::now(format!("a{i}")))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let batch_b = events_to_record_batch(
        &(0..5)
            .map(|i| Event::now(format!("b{i}")))
            .collect::<Vec<_>>(),
    )
    .unwrap();

    write_batch_as_parquet(
        store.as_ref(),
        &ObjectPath::from("events/2026/04/29/14/a.parquet"),
        &batch_a,
    )
    .await
    .unwrap();
    write_batch_as_parquet(
        store.as_ref(),
        &ObjectPath::from("events/2026/04/29/15/b.parquet"),
        &batch_b,
    )
    .await
    .unwrap();

    let ctx = session_context();
    let abs = std::fs::canonicalize(tmp.path()).unwrap();
    let url = format!("file://{}/events/", abs.display());
    register_parquet_dir(&ctx, &url, "events", Some(events_schema()))
        .await
        .unwrap();

    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(arr.value(0), 8);
}

/// Default `SessionContext` (without our config tweak) does NOT recurse.
/// This test pins down the upstream default so we notice if it ever flips.
#[tokio::test]
async fn default_context_does_not_recurse() {
    let tmp = tempfile::tempdir().unwrap();
    let store = local_store(tmp.path()).unwrap();
    let events: Vec<Event> = (0..2).map(|i| Event::now(format!("e{i}"))).collect();
    let batch = events_to_record_batch(&events).unwrap();
    let path = ObjectPath::from("events/2026/04/29/15/test.parquet");
    write_batch_as_parquet(store.as_ref(), &path, &batch)
        .await
        .unwrap();

    let ctx = SessionContext::new(); // no listing_table_ignore_subdirectory tweak
    let abs = std::fs::canonicalize(tmp.path()).unwrap();
    let url = format!("file://{}/events/", abs.display());
    register_parquet_dir(&ctx, &url, "events", Some(events_schema()))
        .await
        .unwrap();
    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(
        arr.value(0),
        0,
        "default SessionContext recursed unexpectedly — siglake-storage::session_context() may no longer be needed"
    );
}
