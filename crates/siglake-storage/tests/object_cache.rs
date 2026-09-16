//! The byte-range object cache (the cold-S3 "hot cache" in the vendored FileIO
//! layer) must be transparent: with it enabled, file-reading queries return
//! identical, correct results across cold and warm (cache-backed) rounds. It
//! caches immutable data-file bytes by (path, range), so decode is unchanged.
//!
//! Own test binary (one test) so toggling the process-global cache can't race
//! other tests.

use chrono::{TimeZone, Utc};
use datafusion::prelude::SessionContext;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

fn ev(secs: i64) -> Event {
    Event {
        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
        host: "h1".into(),
        source: "src".into(),
        sourcetype: "app:json".into(),
        index: "main".into(),
        raw: format!("payload number {secs}"),
        attributes: None,
    }
}

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn object_cache_is_transparent_for_repeated_file_reads() {
    // Enable the byte-range object cache for this process.
    siglake_storage::configure_object_cache_max_bytes(64 * 1024 * 1024);

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = 1_700_000_000i64;
    // Several files so the queries actually open data files (cache fill then hit).
    for i in 0..4 {
        let evs: Vec<Event> = (0..50).map(|j| ev(base + i * 100 + j)).collect();
        ice.append_events(&evs).await.unwrap();
    }
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();

    // A FILTERED count + a substring scan both read data files (not manifest
    // stats). Run twice: cold (cache fill) and warm (cache-backed) must agree and
    // be correct.
    let mid = Utc
        .timestamp_opt(base + 100, 0)
        .single()
        .unwrap()
        .to_rfc3339();
    let filtered = format!("SELECT count(*) AS n FROM events WHERE timestamp >= TIMESTAMP '{mid}'");
    let substr = "SELECT count(*) AS n FROM events WHERE raw LIKE '%payload%'";
    for round in 0..2 {
        assert_eq!(
            count(&ctx, &filtered).await,
            150,
            "round {round}: filtered count across (cache-backed) file reads"
        );
        assert_eq!(
            count(&ctx, substr).await,
            200,
            "round {round}: substring scan across (cache-backed) file reads"
        );
    }

    // Restore the default (disabled) so other test binaries are unaffected.
    siglake_storage::configure_object_cache_max_bytes(0);
}
