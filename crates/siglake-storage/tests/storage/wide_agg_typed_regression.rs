//! Reproduces the 2026-09-02 field regression: after typed columns were admitted
//! to the side aggregate, `top_hosts` went `tier1_wide` -> `materialized` and
//! 140ms -> 3451ms, and `count_distinct_host` went 116ms -> 3194ms. Both are on
//! `host`; every inline-served column was unaffected.
//!
//! The bench index has TWO typed columns and they are not the same kind of
//! thing: `status` is a dimension with 9 values, `size` is a MEASUREMENT with
//! very many. Admitting every Int64 column admits both.

use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::Utc;
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_storage::iceberg::IcebergContext;

fn field(name: &str, field_type: FieldType, required: bool) -> FieldMapping {
    FieldMapping {
        name: name.to_string(),
        field_type,
        required,
    }
}

/// The bench http_logs shape: a low-cardinality text dim, a HIGH-cardinality
/// text dim (the wide column), a low-cardinality typed dim, and a
/// high-cardinality typed measurement.
fn config() -> IndexConfig {
    IndexConfig {
        index_id: "wideagg".to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                field("timestamp", FieldType::Datetime, true),
                field(
                    "method",
                    FieldType::Text {
                        tokenizer: Some("raw".into()),
                    },
                    false,
                ),
                field(
                    "host",
                    FieldType::Text {
                        tokenizer: Some("raw".into()),
                    },
                    false,
                ),
                field("status", FieldType::Long, false),
                field("size", FieldType::Long, false),
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec!["method".to_string(), "host".to_string()],
            default_search_fields: vec![],
        },
        retention: None,
        index_at_flush: None,
    }
}

/// `hosts` distinct hosts and `sizes` distinct sizes per batch — both wide
/// enough to exceed the inline base cap (4096), like the field.
fn batch(
    config: &IndexConfig,
    base_us: i64,
    n: i64,
    off: i64,
    hosts: i64,
    sizes: i64,
) -> RecordBatch {
    let schema = config.to_arrow_schema();
    RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(
                TimestampMicrosecondArray::from(
                    (0..n).map(|i| Some(base_us + i)).collect::<Vec<_>>(),
                )
                .with_timezone("+00:00"),
            ),
            std::sync::Arc::new(StringArray::from(
                (0..n)
                    .map(|i| if i % 2 == 0 { "GET" } else { "POST" })
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(StringArray::from(
                (0..n)
                    .map(|i| format!("host-{:07}", (off + i) % hosts))
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(Int64Array::from(
                (0..n)
                    .map(|i| [200i64, 200, 304, 404, 500][(i % 5) as usize])
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(Int64Array::from(
                (0..n)
                    .map(|i| 128 + ((off + i) % sizes))
                    .collect::<Vec<_>>(),
            )),
            std::sync::Arc::new(StringArray::from(vec![None::<&str>; n as usize])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn a_wide_text_column_keeps_tier1_when_a_typed_measurement_is_present() {
    // The bench harness's cap. Above BASE (4096), so the base+delta path is on
    // and the wide aggregate exists at all.
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            table_group_count_cardinality: Some(4_194_304),
            result_caches: Some(false),
            ..Default::default()
        });
    let config = config();
    ice.create_index(&config).await.unwrap();
    let ident = ice.index_table_ident(&config.index_id);

    // Wide enough that neither `host` nor `size` fits the inline cap, so both
    // can only live in the WIDE aggregate — the competition under test.
    let hosts = 20_000i64;
    let sizes = 20_000i64;
    let rows = 5_000i64;
    let base = Utc::now().timestamp_micros();
    for b in 0..8i64 {
        ice.append_to_table(
            &ident,
            batch(
                &config,
                base + b * 100_000_000,
                rows,
                b * rows,
                hosts,
                sizes,
            ),
            &["method", "host"],
        )
        .await
        .unwrap();
    }

    for column in ["method", "status", "host"] {
        let g = ice
            .grouped_counts_with_summary("wideagg", column, None, None)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("counts for {column}"));
        let mut total = 0u64;
        g.for_each(|_, count| total = total.saturating_add(count));
        println!(
            "column={column:<7} served_by={:<14} groups={:<6} total={total}",
            g.source_label(),
            g.len()
        );
        assert_eq!(total, (rows * 8) as u64, "{column} total must be exact");
    }

    // The regression, as an assertion: `host` must still be served by the
    // table-level aggregate. `materialized` here means it fell to a footer sum,
    // which in the field meant a raw-page decode per file and 3.4 seconds.
    let g = ice
        .grouped_counts_with_summary("wideagg", "host", None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        g.source_label(),
        "tier1_wide",
        "the wide text column lost its Tier-1 aggregate when a typed measurement joined"
    );
}

/// The gate itself, at the unit the field defect actually turned on: a DECLARED
/// dimension keeps the operator's cap however wide it is, and a typed column
/// inferred from its Arrow type is held to the conservative one.
///
/// This is what keeps `size` out of the table-level aggregate while `status`
/// stays in, and `host` — declared, and legitimately 1.1M distinct — unaffected.
#[test]
fn declared_dimensions_keep_the_operator_cap_and_inferred_typed_columns_do_not() {
    let cap = 4_194_304usize;
    let declared = ["method", "host"];
    // Declared: the operator's cap, so `host` at 1.1M distinct still fits.
    assert_eq!(
        siglake_storage::iceberg::group_count_cap_for_test("host", &declared, cap),
        cap
    );
    // Inferred from Int64: held to the typed cap, so a measurement like `size`
    // is dropped from the wide aggregate rather than bloating every delta.
    assert_eq!(
        siglake_storage::iceberg::group_count_cap_for_test("size", &declared, cap),
        1024
    );
    // A low-cardinality typed dimension is unaffected by the cap either way --
    // `status` has 9 values, far under 1024, so it is admitted exactly as before.
    assert!(siglake_storage::iceberg::group_count_cap_for_test("status", &declared, cap) >= 9);
    // And the gate never RAISES a cap: min(), not a replacement.
    assert_eq!(
        siglake_storage::iceberg::group_count_cap_for_test("size", &declared, 16),
        16
    );
}
