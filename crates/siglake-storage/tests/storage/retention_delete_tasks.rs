use chrono::{Duration as ChronoDuration, Utc};
use datafusion::prelude::SessionContext;

use siglake_core::index_config::{FieldType, IndexConfig, RetentionPolicy};
use siglake_core::{events_to_record_batch, Event};
use siglake_storage::iceberg::IcebergContext;

fn logs_index(index_id: &str, retention_secs: Option<u64>) -> IndexConfig {
    let mut config = IndexConfig::builtin_events();
    config.index_id = index_id.to_string();
    config.retention = retention_secs.map(|period_secs| RetentionPolicy {
        period_secs,
        schedule: None,
    });
    config
}

fn bloom_columns(config: &IndexConfig) -> Vec<String> {
    config
        .doc_mapping
        .tag_fields
        .iter()
        .filter_map(|name| {
            config
                .doc_mapping
                .field_mappings
                .iter()
                .find(|field| field.name == *name)
                .and_then(|field| match &field.field_type {
                    FieldType::Text { .. } | FieldType::Json => Some(name.clone()),
                    _ => None,
                })
        })
        .collect()
}

async fn append_index_events(ice: &IcebergContext, config: &IndexConfig, events: &[Event]) {
    let batch = events_to_record_batch(events).unwrap();
    let blooms = bloom_columns(config);
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    ice.append_to_table(&ice.index_table_ident(&config.index_id), batch, &bloom_refs)
        .await
        .unwrap();
}

async fn count_index_rows(ice: &IcebergContext, index_id: &str, where_sql: Option<&str>) -> i64 {
    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(index_id), index_id)
        .await
        .unwrap();
    let sql = match where_sql {
        Some(predicate) => format!("SELECT count(*) AS n FROM \"{index_id}\" WHERE {predicate}"),
        None => format!("SELECT count(*) AS n FROM \"{index_id}\""),
    };
    let batches = ctx
        .sql(sql.as_str())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

fn event_at(ts: chrono::DateTime<Utc>, host: &str, raw: &str) -> Event {
    Event {
        timestamp: ts,
        host: host.to_string(),
        source: "/var/log/app.log".to_string(),
        sourcetype: "app:json".to_string(),
        index: "main".to_string(),
        raw: raw.to_string(),
        attributes: None,
    }
}

#[tokio::test]
async fn retention_drops_only_wholly_expired_files_and_keeps_straddlers() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let config = logs_index("logs", Some(2 * 60 * 60));
    ice.create_index(&config).await.unwrap();

    let now = Utc::now();
    append_index_events(
        &ice,
        &config,
        &[
            event_at(now - ChronoDuration::hours(6), "old-a", "old-a"),
            event_at(now - ChronoDuration::hours(5), "old-b", "old-b"),
        ],
    )
    .await;
    append_index_events(
        &ice,
        &config,
        &[
            event_at(now - ChronoDuration::hours(3), "straddle-a", "straddle-a"),
            event_at(
                now - ChronoDuration::minutes(30),
                "straddle-b",
                "straddle-b",
            ),
        ],
    )
    .await;
    append_index_events(
        &ice,
        &config,
        &[
            event_at(now - ChronoDuration::minutes(20), "recent-a", "recent-a"),
            event_at(now - ChronoDuration::minutes(10), "recent-b", "recent-b"),
        ],
    )
    .await;

    let preview = ice.preview_index_retention("logs").await.unwrap();
    assert!(preview.retention_enabled);
    assert!(preview.dry_run);
    // Assert SEMANTICS, not physical file counts. The `day(timestamp)` partition
    // fans an append batch across UTC day boundaries, so in the 00:00-06:00 UTC
    // window EITHER the wholly-old batch (now-6h, now-5h) OR the straddle batch
    // (now-3h, before the cutoff, + now-30m, after it) can land as two single-day
    // files instead of one. Invariant either way: the wholly-old rows are always
    // dropped and the after-cutoff rows always kept. The straddle's OLD row is
    // dropped iff midnight split it into its own wholly-expired file
    // (straddling_files_kept == 0, rows_dropped == 3); otherwise it rides inside an
    // intact straddling file that is kept (straddling_files_kept == 1,
    // rows_dropped == 2). Earlier exact-count assertions failed every night here.
    assert!(
        preview.files_dropped >= 1,
        "the wholly-old batch is always dropped: {preview:?}"
    );
    assert!(
        matches!(
            (preview.straddling_files_kept, preview.rows_dropped),
            (1, 2) | (0, 3)
        ),
        "intact straddle file keeps its old row (2 dropped); a midnight split drops \
         it as a wholly-expired file (3 dropped): {preview:?}"
    );
    assert_eq!(
        count_index_rows(&ice, "logs", None).await,
        6,
        "dry-run must not mutate"
    );

    let applied = ice.enforce_index_retention("logs").await.unwrap();
    assert_eq!(applied.files_dropped, preview.files_dropped);
    assert_eq!(applied.rows_dropped, preview.rows_dropped);
    assert_eq!(applied.straddling_files_kept, preview.straddling_files_kept);
    // Row-identity invariants — independent of how day-partitioning fans the files.
    assert_eq!(
        count_index_rows(&ice, "logs", Some("host IN ('old-a', 'old-b')")).await,
        0,
        "wholly-expired rows are always dropped",
    );
    assert_eq!(
        count_index_rows(
            &ice,
            "logs",
            Some("host IN ('recent-a', 'recent-b', 'straddle-b')")
        )
        .await,
        3,
        "every after-cutoff row is always kept",
    );
    // The straddle's old row survives iff it stayed inside an intact straddling
    // file (the only file that can straddle the cutoff in this fixture).
    let straddle_old_kept = count_index_rows(&ice, "logs", Some("host = 'straddle-a'")).await;
    assert_eq!(
        straddle_old_kept,
        i64::from(applied.straddling_files_kept == 1)
    );
    assert_eq!(
        count_index_rows(&ice, "logs", None).await,
        3 + straddle_old_kept
    );
}

#[tokio::test]
async fn retention_disabled_index_is_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse).await.unwrap();
    let config = logs_index("logs-no-retention", None);
    ice.create_index(&config).await.unwrap();

    append_index_events(
        &ice,
        &config,
        &[event_at(
            Utc::now() - ChronoDuration::days(30),
            "host",
            "row",
        )],
    )
    .await;

    let preview = ice
        .preview_index_retention("logs-no-retention")
        .await
        .unwrap();
    assert!(!preview.retention_enabled);
    assert_eq!(preview.files_dropped, 0);
    let applied = ice
        .enforce_index_retention("logs-no-retention")
        .await
        .unwrap();
    assert!(!applied.retention_enabled);
    assert_eq!(count_index_rows(&ice, "logs-no-retention", None).await, 1);
}
