//! The rollback direction: a writer that declares a NARROWER schema than the
//! table it writes to.
//!
//! `schema_drift.rs` pins the upgrade direction — a batch carrying a column the
//! table lacks, which is refused when populated because accepting it would drop
//! data. This file pins the mirror, which is the shape of a rollback: the
//! additive migration has already run, the table is wide, and the binary put
//! back in front of it declares the columns it knew about and nothing more.
//! `align_batch_to_table_schema` builds its output by walking the TABLE's field
//! list, so the columns the writer does not know about are filled with nulls —
//! the write lands, the rows already in the table keep their values, and the
//! widen is not undone.
//!
//! WHAT THIS QUALIFIES, AND WHAT IT DOES NOT. Every test here runs ONE binary,
//! the current one, against a table widened past what that binary declares.
//! That is exactly the mechanism a rolled-back writer depends on, so it is a
//! real regression test for it. It is NOT evidence about any particular older
//! image: an image can differ in more than its declared column set, and the
//! timestamp and file-format contracts are refusal boundaries rather than
//! additive ones (`assert_events_timestamp_contract`). Qualifying a specific
//! older image against a specific table needs a live two-image run.

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use siglake_core::{Event, PromotedColumn, PromotedType};
use siglake_storage::iceberg::IcebergContext;
use std::sync::Arc;

/// A column the current binary does not declare. Standing in for "the next
/// additive bump", it makes the CURRENT code the older writer — which is the
/// only way to test this direction without a second image.
const FUTURE_COLUMN: &str = "severity_number";

/// `events_schema()` plus one nullable column, i.e. what a future binary would
/// declare and `migrate-schema` would additively add.
fn widened_schema() -> SchemaRef {
    let base = siglake_core::events_schema();
    let mut fields: Vec<Field> = base.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(FUTURE_COLUMN, DataType::Int64, true));
    Arc::new(Schema::new(fields))
}

fn events(n: usize, prefix: &str) -> Vec<Event> {
    (0..n).map(|i| Event::now(format!("{prefix}{i}"))).collect()
}

/// The new column as a writer would declare it: nullable, and carrying the
/// `PARQUET:field_id` every siglake-declared field carries (the id the widen
/// assigns, one past the base schema's last).
fn future_field(after: &Schema) -> Field {
    let next = after
        .fields()
        .iter()
        .filter_map(|f| {
            f.metadata()
                .get(siglake_core::PARQUET_FIELD_ID_KEY)?
                .parse::<i32>()
                .ok()
        })
        .max()
        .expect("the base schema stamps field ids")
        + 1;
    Field::new(FUTURE_COLUMN, DataType::Int64, true).with_metadata(
        [(
            siglake_core::PARQUET_FIELD_ID_KEY.to_string(),
            next.to_string(),
        )]
        .into_iter()
        .collect(),
    )
}

/// What the WIDENED binary writes: the base batch plus a populated
/// `severity_number`.
fn widened_batch(events: &[Event], severity: i64) -> RecordBatch {
    let base = siglake_core::events_to_record_batch(events).unwrap();
    let mut fields: Vec<Field> = base
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(future_field(base.schema().as_ref()));
    let mut columns: Vec<Arc<dyn Array>> = base.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(vec![severity; base.num_rows()])));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

async fn read_raw_and_severity(ice: &IcebergContext) -> Vec<(String, Option<i64>)> {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx
        .sql(&format!(
            "SELECT raw, {FUTURE_COLUMN} FROM events ORDER BY raw"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|b| {
            let raw = b
                .column_by_name("raw")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let sev = b
                .column_by_name(FUTURE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..b.num_rows())
                .map(|i| {
                    (
                        raw.value(i).to_string(),
                        (!sev.is_null(i)).then(|| sev.value(i)),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The regression the release criteria ask for: after an additive widen, a
/// writer that still declares the narrow schema appends successfully, its rows
/// read back null in the new column, and the rows written before the rollback
/// keep the values they had.
#[tokio::test]
async fn an_older_writer_appends_nulls_and_preserves_the_rows_already_there() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.ensure_events_table().await.unwrap();

    // `migrate-schema` has run: the table is one column wider than this binary.
    assert_eq!(
        ice.migrate_table_schema_additive(ice.events_table_ident(), widened_schema().as_ref())
            .await
            .unwrap(),
        1,
        "the widen must add exactly the new column"
    );

    // The newer binary writes two commits with the column populated...
    let before = events(2, "new-a");
    ice.append_batch(widened_batch(&before, 9)).await.unwrap();
    let before_b = events(1, "new-b");
    ice.append_batch(widened_batch(&before_b, 17))
        .await
        .unwrap();

    // ...then it is rolled back. `append_events` builds the batch from
    // `events_schema()` — the narrow, older writer's shape.
    let after = events(3, "old");
    assert_eq!(
        ice.append_events(&after).await.unwrap(),
        3,
        "an older writer's append must land, not be refused: the column it \
         omits carries no data, so nothing is lost by filling it with nulls"
    );

    let rows = read_raw_and_severity(&ice).await;
    assert_eq!(rows.len(), 6, "every row from both writers is readable");
    assert_eq!(
        rows,
        vec![
            ("new-a0".to_string(), Some(9)),
            ("new-a1".to_string(), Some(9)),
            ("new-b0".to_string(), Some(17)),
            ("old0".to_string(), None),
            ("old1".to_string(), None),
            ("old2".to_string(), None),
        ],
        "pre-rollback values survive the rollback; post-rollback rows are null \
         in the column their writer does not know about"
    );

    // And the base columns the older writer DOES declare are intact for its own
    // rows — alignment reorders and null-pads, it never drops or shifts.
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx
        .sql("SELECT count(*) AS n FROM events WHERE host = 'localhost' AND index = 'default'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        6,
        "both writers' rows keep their base-column values"
    );
}

/// The other half of a rollback: the older binary's own `migrate-schema` run
/// (the Helm pre-upgrade Job or the operator's migration Job, now on the old
/// image). It must be a no-op — additive migration diffs BY NAME against the
/// table, so a narrower desired schema adds nothing and drops nothing. A
/// rollback of the binary does not roll back the schema.
#[tokio::test]
async fn an_older_binarys_migration_neither_narrows_nor_re_adds() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.ensure_events_table().await.unwrap();
    let ident = ice.events_table_ident().clone();
    ice.migrate_table_schema_additive(&ident, widened_schema().as_ref())
        .await
        .unwrap();

    let pending = ice
        .pending_schema_additions(&ident, siglake_core::events_schema().as_ref())
        .await
        .unwrap();
    assert!(
        pending.is_empty(),
        "the older binary's --dry-run must report `up to date`, got: {pending:?}"
    );
    assert_eq!(
        ice.migrate_table_schema_additive(&ident, siglake_core::events_schema().as_ref())
            .await
            .unwrap(),
        0,
        "the older binary's migration adds no columns"
    );

    // The widened column is still there afterwards.
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    ctx.sql(&format!("SELECT {FUTURE_COLUMN} FROM events"))
        .await
        .expect("the widened column survives the older binary's migration run");
}

/// A recorded version LOWER than the table's actual shape says nothing about
/// the columns. `migrate-schema` no longer produces one — it stamps through
/// `stamp_schema_version_at_least`, which takes the maximum against the base
/// each commit attempt lands on (`siglake-storage/src/schema_version.rs`, and
/// `migrate_one_namespace`'s tests in `crates/siglake-cli/src/main.rs`) — but
/// an image built before that fix does, and `stamp_schema_version` remains the
/// explicit writer that takes whatever it is given. The
/// stamp here is that older binary, written directly: the columns stay, writes
/// keep working, and rolling forward restamps. Nothing about a rollback
/// reverses a committed schema mutation.
#[tokio::test]
async fn a_lower_recorded_version_does_not_narrow_the_table() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    ice.ensure_events_table().await.unwrap();
    let ident = ice.events_table_ident().clone();
    ice.migrate_table_schema_additive(&ident, widened_schema().as_ref())
        .await
        .unwrap();

    // What an older binary's migrate-schema leaves behind on a table it found
    // already wide: 0 columns added, its own (lower) constant stamped.
    #[allow(clippy::disallowed_methods)]
    ice.stamp_schema_version(&ident, siglake_core::EVENTS_SCHEMA_VERSION - 1)
        .await
        .unwrap();
    assert_eq!(
        ice.observed_schema_version(&ident).await.unwrap(),
        siglake_core::EVENTS_SCHEMA_VERSION - 1
    );

    // The physical schema is unchanged and authoritative: a populated write to
    // the widened column still lands and still reads back.
    ice.append_batch(widened_batch(&events(1, "after-stamp"), 5))
        .await
        .unwrap();
    assert_eq!(
        read_raw_and_severity(&ice).await,
        vec![("after-stamp0".to_string(), Some(5))],
        "the recorded version says nothing about the columns that exist"
    );
}

/// Reverting the WS-7 promotion CONFIG (dropping `--promote-attr`, or rolling
/// back to values that never had it) is not the same as reverting the binary:
/// the promotion list lives on the table, and the write path resolves it from
/// there. So a writer started without the flag keeps extracting into the typed
/// column instead of null-padding it — which matters because both backfill
/// predicates test column PRESENCE, so null-padded files would look repaired
/// and never be.
#[tokio::test]
async fn dropping_the_promote_attr_flag_does_not_null_the_promoted_column() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let declared = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_promoted_columns(vec![PromotedColumn {
            attr_key: "http.status_code".into(),
            name: "status".into(),
            ty: PromotedType::Int64,
        }]);
    assert_eq!(declared.ensure_promoted_columns().await.unwrap(), 1);
    let attrs = Some(r#"{"http.status_code":503}"#.to_string());
    declared
        .append_events(&[Event::now("declared").with_attributes(attrs.clone())])
        .await
        .unwrap();

    // The same code with the flag gone — a values rollback, not an image one.
    let unflagged = IcebergContext::open(&warehouse).await.unwrap();
    unflagged
        .append_events(&[Event::now("unflagged").with_attributes(attrs)])
        .await
        .unwrap();

    let ctx = SessionContext::new();
    unflagged.register_with_datafusion(&ctx).await.unwrap();
    let batches = ctx
        .sql("SELECT count(*) AS n FROM events WHERE status = 503")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2,
        "the table's promotion property drives extraction, not the CLI flag"
    );
}
