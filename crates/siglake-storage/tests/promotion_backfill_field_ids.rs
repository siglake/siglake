//! Task #6745: a promoted column on a narrow managed index keeps the catalog's
//! field id through the persisted backfill rewrite.
//!
//! Promotion extraction uses the canonical events-table ids (the first
//! promoted field is 9). A managed index assigns the next id in its own schema;
//! this four-column fixture assigns 5. The old name-only alignment fast path
//! passed the extracted id 9 to a writer expecting id 5, so every backfill
//! failed with `Field id 5 not found in struct array`.

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use chrono::{TimeZone, Utc};
use datafusion::prelude::SessionContext;
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_core::{events_to_record_batch, mapping::map_carrier_batch, Event};
use siglake_storage::iceberg::{
    IcebergContext, LevelPolicy, LeveledPassOptions, MergePathKind, ReclusterMergeOptions,
    ReclusterPolicy, PROMOTION_BACKFILL_PROP,
};

const INDEX: &str = "promotion-backfill-field-ids";
const CANDIDATE: &str = "promotion_wave_candidate_v1";

fn field(name: &str, field_type: FieldType) -> FieldMapping {
    FieldMapping {
        name: name.to_string(),
        field_type,
        required: true,
    }
}

fn pilot_index() -> IndexConfig {
    IndexConfig {
        index_id: INDEX.to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                field("timestamp", FieldType::Datetime),
                field(
                    "host",
                    FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                ),
                field(
                    "raw",
                    FieldType::Text {
                        tokenizer: Some("default".to_string()),
                    },
                ),
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec!["host".to_string()],
            default_search_fields: vec!["raw".to_string()],
        },
        retention: None,
        index_at_flush: None,
    }
}

fn full_wave_index() -> IndexConfig {
    IndexConfig {
        index_id: INDEX.to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings: vec![
                field("timestamp", FieldType::Datetime),
                field(
                    "host",
                    FieldType::Text {
                        tokenizer: Some("raw".to_string()),
                    },
                ),
                field(
                    "raw",
                    FieldType::Text {
                        tokenizer: Some("default".to_string()),
                    },
                ),
                field("status", FieldType::Long),
                field("ratio", FieldType::Double),
                field("sampled", FieldType::Bool),
            ],
            timestamp_field: "timestamp".to_string(),
            tag_fields: vec!["host".to_string()],
            default_search_fields: vec!["raw".to_string()],
        },
        retention: None,
        index_at_flush: None,
    }
}

fn pilot_events() -> Vec<Event> {
    let timestamp = Utc.with_ymd_and_hms(2026, 10, 11, 5, 0, 0).unwrap();
    (0..64)
        .map(|row| {
            let attributes = if row < 48 {
                format!(r#"{{"{CANDIDATE}":"eligible"}}"#)
            } else {
                "{}".to_string()
            };
            let mut event = Event::now(format!("row {row}")).with_attributes(Some(attributes));
            event.timestamp = timestamp;
            event
        })
        .collect()
}

async fn scalar_i64(ice: &IcebergContext, sql: &str) -> i64 {
    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(INDEX), INDEX)
        .await
        .unwrap();
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn managed_index_backfill_restamps_promoted_field_id_and_commits_exact_values() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let config = pilot_index();
    let ident = ice.create_index(&config).await.unwrap();
    let batch =
        map_carrier_batch(&events_to_record_batch(&pilot_events()).unwrap(), &config).unwrap();
    ice.append_to_table(&ident, batch, &["host"]).await.unwrap();

    assert_eq!(
        scalar_i64(
            &ice,
            &format!("SELECT count(*) FROM \"{INDEX}\" WHERE attributes LIKE '%{CANDIDATE}%'"),
        )
        .await,
        48,
        "the persisted residual JSON is the pre-promotion answer"
    );
    let before = ice.catalog().load_table(&ident).await.unwrap();
    let before_snapshot = before.metadata().current_snapshot().unwrap().snapshot_id();

    let promoted = ice
        .auto_promote_hot_keys_for(&ident, 0.5, 16, 4, 4096)
        .await
        .unwrap();
    assert_eq!(promoted.len(), 1, "{promoted:?}");
    assert_eq!(promoted[0].attr_key, CANDIDATE);
    assert_eq!(promoted[0].name, CANDIDATE);
    assert_eq!(promoted[0].ty, siglake_core::PromotedType::Utf8);

    let declared = ice.catalog().load_table(&ident).await.unwrap();
    let promoted_id = declared
        .metadata()
        .current_schema()
        .field_id_by_name(CANDIDATE)
        .unwrap();
    assert_eq!(promoted_id, 5, "the narrow index assigns its own next id");
    let promotion_json = siglake_core::promoted_property_json(&promoted).unwrap();
    assert_eq!(
        declared
            .metadata()
            .properties()
            .get(siglake_core::PROMOTED_PROPERTY_KEY),
        Some(&promotion_json),
        "the promotion declaration must be durable before backfill"
    );
    assert!(
        !ice.ensure_promotion_backfill_property_for(&ident)
            .await
            .unwrap(),
        "a pre-promotion file keeps the completion property unset"
    );

    let stats = ice
        .recluster_pass_leveled(
            &ident,
            &["host"],
            &LevelPolicy {
                trigger_files: 100,
                max_merge_gen: 0,
                ..Default::default()
            },
            ReclusterPolicy::default(),
            &LeveledPassOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(stats.len(), 1, "the one old file is one backfill bin");
    assert_eq!(stats[0].files_removed, 1);
    assert_eq!(stats[0].files_added, 1);
    assert_eq!(stats[0].rows, 64);

    assert!(
        ice.ensure_promotion_backfill_property_for(&ident)
            .await
            .unwrap(),
        "the committed replacement file must complete the backfill"
    );
    let after = ice.catalog().load_table(&ident).await.unwrap();
    assert_ne!(
        after.metadata().current_snapshot().unwrap().snapshot_id(),
        before_snapshot,
        "the rewrite must commit a replacement snapshot"
    );
    assert_eq!(
        after.metadata().properties().get(PROMOTION_BACKFILL_PROP),
        Some(&promotion_json),
        "the completion property records the exact promoted set"
    );
    let files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(files.len(), 1);
    assert!(
        files[0].column_sizes().contains_key(&promoted_id),
        "the committed file must carry catalog field id {promoted_id}"
    );

    for (predicate, expected) in [
        ("1 = 1".to_string(), 64),
        (format!("{CANDIDATE} = 'eligible'"), 48),
        (format!("{CANDIDATE} IS NULL"), 16),
    ] {
        assert_eq!(
            scalar_i64(
                &ice,
                &format!("SELECT count(*) FROM \"{INDEX}\" WHERE {predicate}"),
            )
            .await,
            expected,
            "post-backfill answer for {predicate}"
        );
    }
}

#[tokio::test]
async fn page_bounded_managed_index_recluster_persists_multiple_promoted_types() {
    const LEGACY_FILES: usize = 4;
    const ROWS_PER_FILE: usize = 8;
    const LATER_ROWS: usize = 8;
    const TOTAL_ROWS: usize = LEGACY_FILES * ROWS_PER_FILE + LATER_ROWS;

    fn wave_events(file: usize, rows: usize, later: bool) -> Vec<Event> {
        let base = Utc.with_ymd_and_hms(2026, 10, 11, 6, 0, 0).unwrap();
        (0..rows)
            .map(|row| {
                let ordinal = file * ROWS_PER_FILE + row;
                let attributes = if ordinal.is_multiple_of(4) {
                    format!(
                        r#"{{"status":200,"ratio":1.25,"sampled":true,"attempts":{},"cached":{},"duration":{}}}"#,
                        ordinal as i64,
                        ordinal.is_multiple_of(2),
                        ordinal as f64 + 0.5,
                    )
                } else {
                    format!(
                        r#"{{"status":200,"ratio":1.25,"sampled":true,"attempts":{},"cached":{},"duration":{},"region":"r{}"}}"#,
                        ordinal as i64,
                        ordinal.is_multiple_of(2),
                        ordinal as f64 + 0.5,
                        ordinal % 3,
                    )
                };
                let phase = if later { "later" } else { "legacy" };
                let mut event =
                    Event::now(format!("{phase}-{file}-{row}")).with_attributes(Some(attributes));
                // Every append overlaps the same short interval, matching the
                // continuous-ingest merge boundary while staying in one day.
                event.timestamp = base + chrono::Duration::seconds(row as i64);
                event
            })
            .collect()
    }

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let config = full_wave_index();
    let ident = ice.create_index(&config).await.unwrap();

    // Persist several narrow files before the promotion wave.
    for file in 0..LEGACY_FILES {
        let batch = map_carrier_batch(
            &events_to_record_batch(&wave_events(file, ROWS_PER_FILE, false)).unwrap(),
            &config,
        )
        .unwrap();
        ice.append_to_table(&ident, batch, &["host"]).await.unwrap();
    }

    let promoted = ice
        .auto_promote_hot_keys_for(&ident, 0.5, 16, LEGACY_FILES, 4096)
        .await
        .unwrap();
    assert_eq!(promoted.len(), 4, "full typed wave: {promoted:?}");
    assert_eq!(
        promoted
            .iter()
            .map(|column| (column.name.as_str(), column.ty))
            .collect::<Vec<_>>(),
        vec![
            ("attempts", siglake_core::PromotedType::Int64),
            ("cached", siglake_core::PromotedType::Boolean),
            ("duration", siglake_core::PromotedType::Float64),
            ("region", siglake_core::PromotedType::Utf8),
        ],
        "equal-frequency candidates use the deterministic name order"
    );

    let declared = ice.catalog().load_table(&ident).await.unwrap();
    let promoted_ids: Vec<i32> = promoted
        .iter()
        .map(|column| {
            declared
                .metadata()
                .current_schema()
                .field_id_by_name(&column.name)
                .unwrap()
        })
        .collect();
    assert_eq!(
        promoted_ids,
        vec![8, 9, 10, 11],
        "six ordinary fields plus residual JSON put the first promotion at the retained failure's field id 8"
    );

    // A later append exercises the post-declaration write path beside the
    // pre-promotion files selected for backfill.
    let later_file = LEGACY_FILES;
    let batch = map_carrier_batch(
        &events_to_record_batch(&wave_events(later_file, LATER_ROWS, true)).unwrap(),
        &config,
    )
    .unwrap();
    ice.append_to_table(&ident, batch, &["host"]).await.unwrap();

    let files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(files.len(), LEGACY_FILES + 1);
    let stats = ice
        .recluster_files_with(
            &ident,
            files,
            &["host", "region"],
            &ReclusterMergeOptions {
                force_streaming: Some(true),
                merge_fanin: Some(2),
                force_tiered: Some(false),
                merge_chunk_rows: Some(1024),
                merge_chunk_prefetch: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(stats.merge_path, Some(MergePathKind::PageBounded));
    assert_eq!(stats.files_removed, LEGACY_FILES + 1);
    assert_eq!(stats.rows, TOTAL_ROWS);
    assert!(stats.files_added >= 1);

    assert!(
        ice.ensure_promotion_backfill_property_for(&ident)
            .await
            .unwrap(),
        "the persisted rewrite must provide full physical coverage"
    );
    let files = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(
        files.iter().map(|file| file.record_count()).sum::<u64>(),
        TOTAL_ROWS as u64
    );
    for file in &files {
        for id in &promoted_ids {
            assert!(
                file.column_sizes().contains_key(id),
                "{} must physically carry promoted field id {id}",
                file.file_path()
            );
        }
    }

    // Read the committed Parquet through Iceberg and compare every physical
    // value with its residual JSON source, including the nullable region.
    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ident, INDEX)
        .await
        .unwrap();
    let batches = ctx
        .sql(&format!(
            "SELECT attributes, attempts, cached, duration, region, status, ratio, sampled FROM \"{INDEX}\""
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut physical_non_null = [0usize; 4];
    let mut source_present = [0usize; 4];
    let mut rows = 0usize;
    for batch in batches {
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let attempts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let cached = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let duration = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let region = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let status = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ratio = batch
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let sampled = batch
            .column(7)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let source: serde_json::Value = serde_json::from_str(attributes.value(row)).unwrap();
            let object = source.as_object().unwrap();
            if let Some(value) = object.get("attempts") {
                source_present[0] += 1;
                assert_eq!(attempts.value(row), value.as_i64().unwrap());
            }
            if let Some(value) = object.get("cached") {
                source_present[1] += 1;
                assert_eq!(cached.value(row), value.as_bool().unwrap());
            }
            if let Some(value) = object.get("duration") {
                source_present[2] += 1;
                assert_eq!(duration.value(row), value.as_f64().unwrap());
            }
            match object.get("region") {
                Some(value) => {
                    source_present[3] += 1;
                    assert!(!region.is_null(row));
                    assert_eq!(region.value(row), value.as_str().unwrap());
                }
                None => assert!(region.is_null(row), "missing source stays null"),
            }
            assert_eq!(status.value(row), 200, "ordinary integer field");
            assert_eq!(ratio.value(row), 1.25, "ordinary float field");
            assert!(sampled.value(row), "ordinary boolean field");
            physical_non_null[0] += usize::from(!attempts.is_null(row));
            physical_non_null[1] += usize::from(!cached.is_null(row));
            physical_non_null[2] += usize::from(!duration.is_null(row));
            physical_non_null[3] += usize::from(!region.is_null(row));
            rows += 1;
        }
    }
    assert_eq!(rows, TOTAL_ROWS);
    assert_eq!(physical_non_null, source_present, "source/physical counts");
    assert_eq!(physical_non_null, [TOTAL_ROWS, TOTAL_ROWS, TOTAL_ROWS, 30]);
}
