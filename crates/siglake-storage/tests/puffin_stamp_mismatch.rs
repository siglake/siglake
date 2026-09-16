use datafusion::prelude::SessionContext;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && label.is_none_or(|(lk, lv)| {
                    key.key()
                        .labels()
                        .any(|entry| entry.key() == lk && entry.value() == lv)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            _ => 0,
        })
        .sum()
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
async fn puffin_stamp_mismatch_bails_out_to_scan_and_keeps_results_exact() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_footer_max_bytes: Some(1),
            ..Default::default()
        });
    ice.append_events(&[
        Event::now("database timeout retry"),
        Event::now("healthy startup"),
        Event::now("database migration"),
    ])
    .await
    .unwrap();

    let table = ice
        .catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap();
    let mut statistics = table
        .metadata()
        .statistics_iter()
        .next()
        .cloned()
        .expect("append should register one Puffin statistics file");
    for blob in &mut statistics.blob_metadata {
        blob.properties
            .insert("row_group_size".to_string(), "1".to_string());
    }
    let tx = Transaction::new(&table);
    let tx = tx
        .update_statistics()
        .set_statistics(statistics)
        .apply(tx)
        .unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) FROM events WHERE raw LIKE '%database%'",
        )
        .await,
        2
    );

    let snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter_sum(&snapshot, "siglake_index_stamp_mismatch_total", None) > 0,
        "the mismatch counter should record the stale Puffin stamp"
    );
    assert_eq!(
        counter_sum(
            &snapshot,
            "siglake_iceberg_inverted_index_used_total",
            Some(("storage", "puffin")),
        ),
        0,
        "a mismatched Puffin stamp must not drive row-level pruning"
    );
}
