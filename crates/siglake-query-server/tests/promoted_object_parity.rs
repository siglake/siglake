//! #6770: an object- or array-valued attribute must read the same whether the
//! query reaches it through `attr_get` on the residual `attributes` JSON or
//! through a promoted Utf8 column.
//!
//! The 0.4 Big5 keyword-terms arm is
//! `attr_get(attr_get(attributes,'aws.cloudwatch'),'log_stream')`. Promoting
//! `aws.cloudwatch:string` only lets the inner call rewrite onto the promoted
//! column if the two paths produce identical text, including key order,
//! whitespace and escaping.
//!
//! Parity here is between the two read paths, not against the stored slice:
//! `attr_get` parses the matched value and serializes it again
//! (`crates/siglake-query-server/src/udfs.rs:87`), and promotion serializes the
//! value it parsed out of the row (`crates/siglake-core/src/promote.rs:196`),
//! so neither side returns the original bytes when the source object has
//! whitespace or unsorted keys. Both go through `serde_json::Value::to_string`
//! over a `serde_json::Map`, which is a `BTreeMap` in this workspace (no
//! `preserve_order` feature), so both canonicalise the same way.

use std::collections::BTreeMap;

use arrow_array::{Array, RecordBatch, StringArray};
use datafusion::prelude::SessionContext;
use siglake_core::{
    events_schema_with, events_to_record_batch, promote_attributes, repromote_attributes, Event,
    PromotedColumn, PromotedType,
};
use siglake_query_server::udfs::register_udfs;

/// One row per interesting shape of a non-scalar attribute value. The index is
/// also the row's `raw` key (`row-0`, …) so SQL results can be matched back to
/// the promoted column without depending on plan output order.
const ROWS: &[Option<&str>] = &[
    // Keys already lexicographic, the Big5 shape.
    Some(
        r#"{"aws.cloudwatch":{"log_group":"/aws/lambda/api","log_stream":"2026/10/11/[$LATEST]9c"},"host.id":"i-1"}"#,
    ),
    // Keys in the reverse of lexicographic order.
    Some(r#"{"aws.cloudwatch":{"log_stream":"stream-b","log_group":"group-b"}}"#),
    // Whitespace around every structural character.
    Some(
        r#"{ "aws.cloudwatch" : { "log_stream" : "stream-c" , "log_group" : "group-c" } , "n" : 1 }"#,
    ),
    // Escapes, non-ASCII, and structural characters inside a string value.
    Some(r#"{"aws.cloudwatch":{"log_stream":"a\"b\\c,}]\tx\ny ünï","log_group":"g"},"after":1}"#),
    // Array value, with a nested object inside it.
    Some(r#"{"aws.cloudwatch":[1,{"log_stream":"stream-e"},"x",null,1.5]}"#),
    // Scalar string: the promoted column takes it verbatim, so the nested
    // `attr_get` has no object to walk into on either path.
    Some(r#"{"aws.cloudwatch":"plain-string"}"#),
    // Numbers that the integer and float parsers must round-trip identically.
    Some(r#"{"aws.cloudwatch":{"big":10000000000000001,"log_stream":"s","retention":1.0}}"#),
    // Deeper nesting under the promoted key.
    Some(r#"{"aws.cloudwatch":{"log_stream":"s-h","tags":{"b":[true,false],"a":null}}}"#),
    // Empty object.
    Some(r#"{"aws.cloudwatch":{}}"#),
    // Key absent.
    Some(r#"{"other":"x"}"#),
    // Explicit JSON null: absent semantics on both paths.
    Some(r#"{"aws.cloudwatch":null}"#),
    // Unparseable attributes.
    Some("not json"),
    // NULL attributes column.
    None,
];

fn promoted() -> Vec<PromotedColumn> {
    vec![PromotedColumn {
        attr_key: "aws.cloudwatch".into(),
        carrier: Default::default(),
        attr_get_path: Vec::new(),
        name: "aws_cloudwatch".into(),
        ty: PromotedType::Utf8,
    }]
}

fn events_batch() -> RecordBatch {
    let events: Vec<Event> = ROWS
        .iter()
        .enumerate()
        .map(|(i, attrs)| Event::now(format!("row-{i}")).with_attributes(attrs.map(str::to_string)))
        .collect();
    events_to_record_batch(&events).unwrap()
}

/// Run `SELECT raw, <expr> FROM t` over `batch` with the query-server UDFs
/// registered, keyed by the row's `raw` marker.
async fn select_by_row(batch: &RecordBatch, expr: &str) -> BTreeMap<String, Option<String>> {
    let ctx = SessionContext::new();
    register_udfs(&ctx);
    ctx.register_batch("t", batch.clone()).unwrap();
    let out = ctx
        .sql(&format!("SELECT raw, {expr} AS v FROM t"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut got = BTreeMap::new();
    for b in out {
        let raw = b
            .column_by_name("raw")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let v = b
            .column_by_name("v")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            let value = (!v.is_null(i)).then(|| v.value(i).to_string());
            assert!(
                got.insert(raw.value(i).to_string(), value).is_none(),
                "duplicate row key {}",
                raw.value(i)
            );
        }
    }
    got
}

fn column_by_row(batch: &RecordBatch, name: &str) -> BTreeMap<String, Option<String>> {
    let raw = batch
        .column_by_name("raw")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| {
            (
                raw.value(i).to_string(),
                (!col.is_null(i)).then(|| col.value(i).to_string()),
            )
        })
        .collect()
}

fn rows(pairs: &[(&str, Option<&str>)]) -> BTreeMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
        .collect()
}

/// A pre-promotion file read back under the widened schema: the JSON still
/// carries the key, the promoted column reads all-NULL.
fn pre_backfill_batch(events: &RecordBatch) -> RecordBatch {
    let mut columns = events.columns().to_vec();
    columns.push(arrow_array::new_null_array(
        &arrow_schema::DataType::Utf8,
        events.num_rows(),
    ));
    RecordBatch::try_new(events_schema_with(&promoted()), columns).unwrap()
}

#[tokio::test]
async fn promoted_column_matches_attr_get_for_objects_and_arrays() {
    let events = events_batch();
    let widened = promote_attributes(&events, &promoted()).unwrap();

    let from_json = select_by_row(&events, "attr_get(attributes,'aws.cloudwatch')").await;
    let from_column = column_by_row(&widened, "aws_cloudwatch");
    assert_eq!(from_column, from_json);

    // Pin the canonical text too, so the two paths cannot drift together:
    // compact, keys sorted, escapes normalised by serde_json.
    assert_eq!(
        from_column,
        rows(&[
            (
                "row-0",
                Some(r#"{"log_group":"/aws/lambda/api","log_stream":"2026/10/11/[$LATEST]9c"}"#),
            ),
            (
                "row-1",
                Some(r#"{"log_group":"group-b","log_stream":"stream-b"}"#),
            ),
            (
                "row-2",
                Some(r#"{"log_group":"group-c","log_stream":"stream-c"}"#),
            ),
            (
                "row-3",
                Some(r#"{"log_group":"g","log_stream":"a\"b\\c,}]\tx\ny ünï"}"#),
            ),
            (
                "row-4",
                Some(r#"[1,{"log_stream":"stream-e"},"x",null,1.5]"#)
            ),
            ("row-5", Some("plain-string")),
            (
                "row-6",
                Some(r#"{"big":10000000000000001,"log_stream":"s","retention":1.0}"#),
            ),
            (
                "row-7",
                Some(r#"{"log_stream":"s-h","tags":{"a":null,"b":[true,false]}}"#),
            ),
            ("row-8", Some("{}")),
            ("row-9", None),
            ("row-10", None),
            ("row-11", None),
            ("row-12", None),
        ])
    );
}

/// The Big5 rewrite target: replacing the inner `attr_get` with the promoted
/// column must not change the outer call's answer, before or after backfill.
#[tokio::test]
async fn nested_log_stream_reads_the_same_through_the_promoted_column() {
    let events = events_batch();
    let residual = select_by_row(
        &events,
        "attr_get(attr_get(attributes,'aws.cloudwatch'),'log_stream')",
    )
    .await;

    // Non-trivial: the object rows resolve, the array and scalar rows do not.
    assert_eq!(residual["row-1"].as_deref(), Some("stream-b"));
    assert_eq!(residual["row-2"].as_deref(), Some("stream-c"));
    assert_eq!(residual["row-4"], None);
    assert_eq!(residual["row-5"], None);

    let widened = promote_attributes(&events, &promoted()).unwrap();
    let rewritten = select_by_row(&widened, "attr_get(aws_cloudwatch,'log_stream')").await;
    assert_eq!(rewritten, residual);

    // Same question against a file written before the promotion was declared
    // and then backfilled by the rewrite path.
    let backfilled = repromote_attributes(&pre_backfill_batch(&events), &promoted()).unwrap();
    let after_backfill = select_by_row(&backfilled, "attr_get(aws_cloudwatch,'log_stream')").await;
    assert_eq!(after_backfill, residual);
}

/// Backfilling a pre-promotion read produces exactly the batch the write path
/// would have produced, and re-running it changes nothing.
#[test]
fn backfill_reproduces_the_write_path_batch() {
    let events = events_batch();
    let widened = promote_attributes(&events, &promoted()).unwrap();

    let backfilled = repromote_attributes(&pre_backfill_batch(&events), &promoted()).unwrap();
    assert_eq!(backfilled.schema(), widened.schema());
    assert_eq!(backfilled, widened);

    let again = repromote_attributes(&backfilled, &promoted()).unwrap();
    assert_eq!(again, widened);
}

/// Only a Utf8 promotion stringifies a non-scalar value. Int64/Float64/Boolean
/// promotions reject it the way the declared-mapping Text builder does, and the
/// value stays in the residual JSON either way.
#[test]
fn non_utf8_promoted_types_reject_object_values() {
    let cols = vec![
        PromotedColumn {
            attr_key: "aws.cloudwatch".into(),
            carrier: Default::default(),
            attr_get_path: Vec::new(),
            name: "as_int".into(),
            ty: PromotedType::Int64,
        },
        PromotedColumn {
            attr_key: "aws.cloudwatch".into(),
            carrier: Default::default(),
            attr_get_path: Vec::new(),
            name: "as_float".into(),
            ty: PromotedType::Float64,
        },
        PromotedColumn {
            attr_key: "aws.cloudwatch".into(),
            carrier: Default::default(),
            attr_get_path: Vec::new(),
            name: "as_bool".into(),
            ty: PromotedType::Boolean,
        },
    ];
    let events = events_batch();
    let widened = promote_attributes(&events, &cols).unwrap();
    for name in ["as_int", "as_float", "as_bool"] {
        let col = widened.column_by_name(name).unwrap();
        assert_eq!(
            col.null_count(),
            col.len(),
            "{name} should be all-null: no non-scalar value coerces"
        );
    }
    let attrs = events
        .column_by_name("attributes")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(attrs.value(0), ROWS[0].unwrap());
}
