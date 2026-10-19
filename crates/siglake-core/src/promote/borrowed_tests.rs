use super::*;
use arrow_array::{BooleanArray, Float64Array, Int64Array};

fn batch(rows: &[Option<&str>], raw: &[Option<&str>]) -> RecordBatch {
    assert_eq!(rows.len(), raw.len());
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("attributes", DataType::Utf8, true),
            Field::new("raw", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(rows.to_vec())),
            Arc::new(StringArray::from(raw.to_vec())),
        ],
    )
    .unwrap()
}

fn all_types(keys: &[&str]) -> Vec<PromotedColumn> {
    let mut columns = Vec::new();
    for key in keys {
        for ty in [
            PromotedType::Utf8,
            PromotedType::Int64,
            PromotedType::Float64,
            PromotedType::Boolean,
        ] {
            columns.push(PromotedColumn {
                attr_key: (*key).into(),
                carrier: PromotionCarrier::Attributes,
                attr_get_path: vec![],
                name: format!("p{}", columns.len()),
                ty,
            });
        }
    }
    columns
}

fn differential(batch: &RecordBatch, columns: &[PromotedColumn]) -> RecordBatch {
    let actual = promote_attributes(batch, columns).unwrap();
    let owned = promote_attributes_impl::<false>(batch, columns).unwrap();
    assert_eq!(actual.schema(), owned.schema());
    for i in 0..batch.num_columns() {
        assert_eq!(actual.column(i).to_data(), batch.column(i).to_data());
    }
    for (i, column) in columns.iter().enumerate() {
        let a = actual.column(batch.num_columns() + i);
        let b = owned.column(batch.num_columns() + i);
        match column.ty {
            PromotedType::Utf8 => assert_eq!(
                a.as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                b.as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                "{column:?}"
            ),
            PromotedType::Int64 => assert_eq!(
                a.as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                b.as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                "{column:?}"
            ),
            // Compare IEEE bits, preserving negative zero and NaN behavior of
            // string coercions rather than relying on NaN != NaN equality.
            PromotedType::Float64 => assert_eq!(
                a.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(f64::to_bits))
                    .collect::<Vec<_>>(),
                b.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(f64::to_bits))
                    .collect::<Vec<_>>(),
                "{column:?}"
            ),
            PromotedType::Boolean => assert_eq!(
                a.as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                b.as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                "{column:?}"
            ),
        }
    }
    actual
}

#[test]
fn borrowed_tree_scalar_types_numeric_extremes_and_binary64_match_owned() {
    let values = [
        "null",
        "true",
        "false",
        "0",
        "-0",
        "-0.0",
        "1",
        "-1",
        "9223372036854775807",
        "-9223372036854775808",
        "18446744073709551615",
        "18446744073709551616",
        "-9223372036854775809",
        "9007199254740993",
        "0.1000000000000000055511151231257827021181583404541015625",
        "1.234567890123456789e-200",
        "1.7976931348623157e308",
        "5e-324",
        "1e-400",
        r#""42""#,
        r#""-0.0""#,
        r#""NaN""#,
        r#""inf""#,
        r#""true""#,
        r#""FALSE""#,
        r#""雪\uD83D\uDE03\n\t\"\\""#,
        "[]",
        "{}",
        r#"{"z":2,"a":1}"#,
    ];
    let mut inputs: Vec<String> = values.iter().map(|v| format!(r#"{{"v":{v}}}"#)).collect();
    // Exercise the locked parser's exponent rounding across normal/subnormal
    // ranges; no independent decimal parser or expected float approximation.
    for exponent in [-324, -308, -200, -20, -1, 0, 1, 20, 200, 307] {
        for mantissa in [
            "1.0000000000000001",
            "2.2250738585072014",
            "9.999999999999999",
        ] {
            inputs.push(format!(r#"{{"v":{mantissa}e{exponent}}}"#));
        }
    }
    let rows: Vec<_> = inputs.iter().map(|s| Some(s.as_str())).collect();
    differential(
        &batch(&rows, &vec![None; rows.len()]),
        &all_types(&["v", "missing"]),
    );
}

#[test]
fn borrowed_tree_duplicates_escaped_keys_dotted_precedence_and_no_backtracking() {
    let rows = [
        Some(r#"{"v":1,"v":2,"a":{"b":{"c":3},"b.c":4}}"#),
        Some(r#"{"v":1,"\u0076":null,"a":{"b.c":null,"b":{"c":5}}}"#),
        Some(r#"{"a":{"x":1},"a.b":{"c":9}}"#),
        Some(r#"{"a.b.c":null,"a":{"b":{"c":7}}}"#),
        Some(r#"{"a":{"b.c":1,"b\u002ec":2},"v":"last"}"#),
        Some(r#"{"a":{"b":{"c":1}},"a":{"b":{"c":8}}}"#),
        Some(r#"{"a":null,"a.b":{"c":9}}"#),
    ];
    let result = differential(
        &batch(&rows, &[None; 7]),
        &all_types(&["a.b.c", "v", "a.b", "a"]),
    );
    let first = result
        .column_by_name("p0")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        first.iter().collect::<Vec<_>>(),
        vec![Some("4"), None, None, None, Some("2"), Some("8"), None]
    );
}

#[test]
fn borrowed_tree_invalid_trailing_nonobject_and_unselected_corruption_match_owned() {
    let deep = format!(
        r#"{{"v":1,"unused":{}0{}}}"#,
        "[".repeat(140),
        "]".repeat(140)
    );
    let rows = [
        None,
        Some(""),
        Some("null"),
        Some("true"),
        Some("42"),
        Some(r#""text""#),
        Some("[]"),
        Some("[1,2]"),
        Some(r#"{"v":1} true"#),
        Some(r#"{"v":1,}"#),
        Some(r#"{"v":1,"bad":???}"#),
        Some(r#"{"v":1,"bad":[0,]}"#),
        Some(r#"{"v":"\uD800"}"#),
        Some(r#"{"v":1,"bad":"\uDC00"}"#),
        Some(r#"{"v":1e999}"#),
        Some(r#"{"v":01}"#),
        Some(r#"{"v":NaN}"#),
        Some(deep.as_str()),
    ];
    let output = differential(&batch(&rows, &vec![None; rows.len()]), &all_types(&["v"]));
    assert_eq!(
        output.column_by_name("p0").unwrap().null_count(),
        rows.len()
    );
}

#[test]
fn borrowed_tree_complex_composed_raw_and_private_keys_use_owned_semantics() {
    let rows = [
        Some(
            r#"{"v":{"z":1,"a":2,"a":3},"arr":[1,{"z":2,"a":1}],"outer":"{\"leaf\":\"{\\\"last\\\":7}\"}"}"#,
        ),
        Some(r#"{"v":[true,null,1.0,-0.0,"x"],"outer":{"leaf":{"last":8}}}"#),
        Some(r#"{"v":2,"$serde_json::private::Number":"42"}"#),
        Some(r#"{"v":3,"nested":{"$serde_json::private::RawValue":"{}"}}"#),
        Some(r#"{"outer":{"leaf":null},"v":"plain"}"#),
    ];
    let raw = [
        Some(r#"{"v":1,"v":2,"outer":"{\"leaf\":9}"}"#),
        Some(r#"{"v":3,"bad":???}"#),
        Some("bad"),
        None,
        Some(r#"{"v":{"z":1,"a":2}}"#),
    ];
    let mut cols = all_types(&["v", "arr", "$serde_json::private::Number"]);
    for spec in [
        r#"["outer","leaf"]:string:composed"#,
        r#"["outer","leaf","last"]:string:twice"#,
    ] {
        cols.push(PromotedColumn::parse_spec(spec).unwrap());
    }
    let mut raw_col = PromotedColumn::parse_spec("v:string:raw_value").unwrap();
    raw_col.carrier = PromotionCarrier::Raw;
    cols.push(raw_col);
    let mut raw_chain = PromotedColumn::parse_spec(r#"["outer","leaf"]:string:raw_chain"#).unwrap();
    raw_chain.carrier = PromotionCarrier::Raw;
    cols.push(raw_chain);
    let result = differential(&batch(&rows, &raw), &cols);
    let text = result
        .column_by_name("p0")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let map: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(rows[0].unwrap()).unwrap();
    assert_eq!(text.value(0), map["v"].to_string());
    let raw_values = result
        .column_by_name("raw_value")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Raw is deliberately a different original parser contract: first match,
    // including a valid match before later malformed bytes. Never tree-parse it.
    assert_eq!(raw_values.value(0), "1");
    assert_eq!(raw_values.value(1), "3");
}

#[test]
fn borrowed_tree_owns_only_fallback_rows_and_keeps_original_rejection() {
    let row = RowAttributes::new::<true>(Some(r#"{"v":"value","obj":{"b":2,"a":1}}"#), true);
    let col = PromotedColumn::parse_spec("v:string:p").unwrap();
    assert!(matches!(row.extract(&col), Some(Extracted::Text("value"))));
    assert!(
        row.owned.get().is_none(),
        "direct scalar must not build owned tree"
    );
    let object = PromotedColumn::parse_spec("obj:string:p").unwrap();
    assert!(matches!(
        row.extract(&object),
        Some(Extracted::Json(Cow::Owned(_)))
    ));
    assert!(
        row.owned.get().unwrap().is_some(),
        "complex output uses original map"
    );
    let private =
        RowAttributes::new::<true>(Some(r#"{"v":1,"$serde_json::private::Number":"3"}"#), true);
    assert!(private.borrowed.is_none());
    assert!(private.extract(&col).is_some());
    let invalid = RowAttributes::new::<true>(Some(r#"{"v":1,"bad":???}"#), true);
    assert!(invalid.borrowed.is_none());
    assert!(invalid.extract(&col).is_none());
    assert!(invalid.owned.get().unwrap().is_none());
}

#[test]
fn borrowed_tree_composed_only_avoids_unused_borrowed_parse() {
    let input = r#"{"outer":{"leaf":"value"}}"#;
    let row = RowAttributes::new::<true>(Some(input), false);
    assert!(row.borrowed.is_none());
    let col = PromotedColumn::parse_spec(r#"["outer","leaf"]:string:p"#).unwrap();
    assert!(
        matches!(row.extract(&col), Some(Extracted::Json(Cow::Owned(serde_json::Value::String(s)))) if s == "value")
    );
    differential(&batch(&[Some(input)], &[None]), &[col]);
}
