//! Task #4558, Puffin half: the row-domain check has to hold on the sidecar
//! path too, where the blob arrives from object storage rather than from the
//! Parquet footer.
//!
//! Own test binary: it installs a process-global metrics recorder.

use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use iceberg::puffin::{Blob, CompressionCodec, PuffinWriter};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_index::InvertedIndex;
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

/// A Puffin index whose postings cover one row of a three-row file skips the
/// other two before decode, so the rows it cannot name drop out of the answer.
/// `sidecar_blob_specs_for_data_files` refuses the spillover for the rolled
/// write that produces this shape legitimately, but a corrupt or stale blob
/// gets there anyway; the reader has to match the blob's row domain against
/// the Parquet file before it prunes with it.
#[tokio::test]
async fn a_puffin_index_that_does_not_cover_the_file_falls_back_to_an_exact_scan() {
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
            // Force the index out of the footer and into a Puffin sidecar.
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
    let statistics = table
        .metadata()
        .statistics_iter()
        .next()
        .cloned()
        .expect("append should register one Puffin statistics file");
    assert_eq!(statistics.blob_metadata.len(), 1, "one raw-column blob");
    let registered = statistics.blob_metadata[0].clone();

    // Rewrite the sidecar in place with an index over one row of the three.
    // Everything the reader keys and stamp-checks on is preserved: blob type,
    // fields, snapshot, sequence number and properties (including
    // `row_group_size`), so only the row domain is wrong.
    let under_covering = InvertedIndex::from_rows(["database timeout retry"]);
    assert_eq!(under_covering.n_rows(), 1);
    let output = table
        .file_io()
        .new_output(&statistics.statistics_path)
        .unwrap();
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
        .await
        .unwrap();
    writer
        .add(
            Blob::builder()
                .r#type(registered.r#type.clone())
                .fields(registered.fields.clone())
                .snapshot_id(registered.snapshot_id)
                .sequence_number(registered.sequence_number)
                .data(under_covering.to_bytes())
                .properties(registered.properties.clone())
                .build(),
            CompressionCodec::zstd_default(),
        )
        .await
        .unwrap();
    writer.close().await.unwrap();

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    assert_eq!(
        count(
            &ctx,
            "SELECT count(*) FROM events WHERE raw LIKE '%database%'",
        )
        .await,
        2,
        "both matching rows must survive an index that only covers the first row"
    );

    let snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter_sum(
            &snapshot,
            "siglake_index_row_domain_mismatch_total",
            Some(("storage", "puffin")),
        ) > 0,
        "the blob's row domain should be recorded as not matching the file"
    );
    assert_eq!(
        counter_sum(
            &snapshot,
            "siglake_iceberg_inverted_index_used_total",
            Some(("storage", "puffin")),
        ),
        0,
        "an index that does not cover the file must not drive row-level pruning"
    );
}
