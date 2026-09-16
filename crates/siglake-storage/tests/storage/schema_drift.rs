//! A binary newer than its table must not silently discard the columns the
//! table lacks.
//!
//! `align_batch_to_table_schema` builds its output by walking the TABLE's
//! field list, so a column the batch has and the table lacks is never
//! referenced — no error, no log, no metric. On a read that is how a narrow
//! old file widens to the current schema. On a WRITE it is this project's
//! worst failure class: accepted, durable, permanently gone. The rows are
//! acked, the WAL segment is swept, and the column is absent forever.
//!
//! These tests pin the write direction. A table created at the current schema
//! plus a batch carrying one more column is exactly the shape of "the first
//! post-launch additive bump meets an existing table".

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema};
use siglake_storage::iceberg::IcebergContext;
use std::sync::Arc;

/// A batch matching `events_schema()` plus one extra column, whose values are
/// `extra`. `None` makes the extra column entirely null.
fn batch_with_extra_column(n: usize, extra: Option<&str>) -> RecordBatch {
    let base = siglake_core::events_schema();
    let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut cols: Vec<Arc<dyn Array>> = Vec::new();
    for f in base.fields() {
        cols.push(match f.data_type() {
            DataType::Timestamp(_, _) => Arc::new(
                (0..n)
                    .map(|i| Some(1_700_000_000_000_000i64 + i as i64))
                    .collect::<TimestampMicrosecondArray>()
                    .with_timezone("+00:00"),
            ),
            DataType::Int64 => Arc::new(
                (0..n)
                    .map(|i| Some(1_700_000_000_000_000_000i64 + i as i64 * 1_000))
                    .collect::<Int64Array>(),
            ),
            _ => Arc::new(StringArray::from(vec![Some("x"); n])),
        });
    }
    fields.push(Field::new("from_the_future", DataType::Utf8, true));
    cols.push(Arc::new(StringArray::from(vec![extra; n])));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

/// The refusal. Against the old code this test FAILS: the append returns Ok
/// and `from_the_future` is gone from the committed file.
#[tokio::test]
async fn a_populated_column_the_table_lacks_refuses_the_write() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.ensure_events_table().await.unwrap();

    let err = ice
        .append_batch(batch_with_extra_column(3, Some("data")))
        .await
        .expect_err("a populated column the table lacks must not be silently dropped");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("from_the_future"),
        "the error must name the column that would have been lost, got: {msg}"
    );
    assert!(
        msg.contains("migrate-schema"),
        "the error must name the remedy, got: {msg}"
    );
}

/// The other half, and the reason the discriminator is population rather than
/// presence: an all-null extra column loses nothing, so refusing it would turn
/// a routine upgrade into an ingest outage for no gain.
#[tokio::test]
async fn an_all_null_column_the_table_lacks_is_allowed() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.ensure_events_table().await.unwrap();

    let rows = ice
        .append_batch(batch_with_extra_column(3, None))
        .await
        .expect("an all-null extra column carries no data and must not block the write");
    assert_eq!(rows, 3, "the rows themselves must still land");
}

/// A table records the schema version it is actually at, so a reader can ask
/// instead of assuming its own compiled-in constant describes the table.
#[tokio::test]
async fn a_fresh_table_records_the_schema_version_it_was_created_at() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.ensure_events_table().await.unwrap();

    let observed = ice
        .observed_schema_version(ice.events_table_ident())
        .await
        .unwrap();
    assert_eq!(
        observed,
        siglake_core::EVENTS_SCHEMA_VERSION,
        "a table created by this binary is at this binary's version"
    );
}

/// The recorded version is authoritative over both the binary's constant and
/// the schema's shape. This is the whole point of stamping: a reader that falls
/// back to its own `EVENTS_SCHEMA_VERSION` cannot tell a migrated table from an
/// unmigrated one, which is how the operator's status field came to report a
/// successful migration that never ran.
#[tokio::test]
async fn the_recorded_version_beats_the_binarys_own_constant() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.ensure_events_table().await.unwrap();

    // Claim this table is still at v1 even though its schema is v2-shaped.
    #[allow(clippy::disallowed_methods)]
    ice.stamp_schema_version(ice.events_table_ident(), 1)
        .await
        .unwrap();
    assert_eq!(
        ice.observed_schema_version(ice.events_table_ident())
            .await
            .unwrap(),
        1,
        "the table's own record must win over the running binary's constant"
    );
}
