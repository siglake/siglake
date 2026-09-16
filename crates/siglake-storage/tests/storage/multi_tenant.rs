//! Multi-tenant isolation test: two `IcebergContext` instances
//! sharing one catalog + warehouse directory but pinned to different
//! Iceberg namespaces should see disjoint events tables.

use chrono::Utc;
use datafusion::prelude::SessionContext;

use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

fn synth(host: &str) -> Event {
    Event {
        timestamp: Utc::now(),
        host: host.into(),
        source: "test".into(),
        sourcetype: "t".into(),
        index: "main".into(),
        raw: format!("evt from {host}"),
        attributes: None,
    }
}

#[tokio::test]
async fn tenant_namespaces_are_isolated() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    std::fs::create_dir_all(&warehouse).unwrap();
    let abs = std::fs::canonicalize(&warehouse).unwrap();
    let warehouse_url = format!("file://{}", abs.display());
    let catalog_uri = format!("sqlite://{}/_catalog.db?mode=rwc", abs.display());

    // Two tenants, one shared catalog + warehouse.
    let alice = IcebergContext::open_with_namespace(&catalog_uri, &warehouse_url, "tenant_alice")
        .await
        .unwrap();
    let bob = IcebergContext::open_with_namespace(&catalog_uri, &warehouse_url, "tenant_bob")
        .await
        .unwrap();

    // Each tenant writes their own events.
    alice
        .append_events(&[synth("alice-host-1"), synth("alice-host-2")])
        .await
        .unwrap();
    bob.append_events(&[synth("bob-host-1")]).await.unwrap();

    // Each tenant only sees their own data.
    let alice_count = count_events(&alice).await;
    let bob_count = count_events(&bob).await;
    assert_eq!(alice_count, 2, "alice should see 2 rows");
    assert_eq!(bob_count, 1, "bob should see 1 row");
}

async fn count_events(ice: &IcebergContext) -> i64 {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap();
    arr.value(0)
}
