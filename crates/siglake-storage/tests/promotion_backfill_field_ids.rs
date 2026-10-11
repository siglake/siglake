//! Task #6745: a promoted column on a narrow managed index keeps the catalog's
//! field id through the persisted backfill rewrite.
//!
//! Promotion extraction uses the canonical events-table ids (the first
//! promoted field is 9). A managed index assigns the next id in its own schema;
//! this four-column fixture assigns 5. The old name-only alignment fast path
//! passed the extracted id 9 to a writer expecting id 5, so every backfill
//! failed with `Field id 5 not found in struct array`.

use arrow_array::Int64Array;
use chrono::{TimeZone, Utc};
use datafusion::prelude::SessionContext;
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_core::{events_to_record_batch, mapping::map_carrier_batch, Event};
use siglake_storage::iceberg::{
    IcebergContext, LevelPolicy, LeveledPassOptions, ReclusterPolicy, PROMOTION_BACKFILL_PROP,
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
