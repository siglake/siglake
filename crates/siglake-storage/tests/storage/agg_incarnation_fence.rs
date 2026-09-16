//! An aggregate belongs to the table INCARNATION that built it, not to the
//! index name.
//!
//! THE DEFECT THIS GUARDS (#2919). `DELETE /api/v1/indexes/logs` followed by a
//! `POST` of the same id is a different Iceberg table at the SAME location: the
//! SQL catalog derives the location from the name. Every aggregate artifact —
//! the inline object the commit path rewrites, the compactor's folded wide
//! base, the per-commit deltas and the rebuild markers — sat at a fixed path
//! under that location, so the replacement read what the dropped table left.
//! The only admission check is `column_total == record_count`, and two
//! incarnations agree on a row count while describing different rows: the
//! dropped table's values then pass the replacement's guard and Tier-1 answers
//! with them. The wide base is worse than stale, because its `rebuilt_through`
//! watermark also SUPPRESSES the replacement's restarted sequence numbers.
//!
//! Deleting the shared prefix at creation would not close it. A publisher
//! stalled between its commit and its PUT lands its object after the delete —
//! which is the second test here.
//!
//! COMPATIBILITY. Artifacts written before the incarnation prefix existed carry
//! no identity, and a name proves nothing, so nothing adopts them: they are
//! read by no one, deleted by no one, and every consumer falls back to the
//! exact per-file tiers until the incarnation has built its own.

use chrono::{TimeZone, Utc};
use datafusion::prelude::SessionContext;
use siglake_core::index_config::{FieldType, IndexConfig};
use siglake_core::{events_to_record_batch, Event};
use siglake_storage::iceberg::{
    aggregate_prefix_rel_path, group_count_delta_rel_path, ColumnGroupCounts, FileGroupCounts,
    GroupCountDelta, IcebergContext, IcebergTuning, SnapshotAggregates, WideGroupCounts,
};

/// Over the inline ceiling (4,096) so `host` can only be served by the wide
/// base plus its deltas — the artifact set the fence has the most to prove
/// about, since it is the one carrying a rebuild watermark.
const DISTINCT_HOSTS: usize = 5_000;
const ROWS: usize = 5_000;

async fn context(warehouse: &std::path::Path) -> IcebergContext {
    IcebergContext::open(warehouse)
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            table_group_count_cardinality: Some(2_000_000),
            ..Default::default()
        })
}

fn logs_index(index_id: &str) -> IndexConfig {
    let mut config = IndexConfig::builtin_events();
    config.index_id = index_id.to_string();
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

/// `ROWS` events whose `host` values all carry `prefix`, so which incarnation
/// an answer came from is visible in the answer itself.
fn events(prefix: &str, base_secs: i64) -> Vec<Event> {
    (0..ROWS)
        .map(|i| Event {
            timestamp: Utc.timestamp_opt(base_secs + i as i64, 0).single().unwrap(),
            host: format!("{prefix}-{:06}", i % DISTINCT_HOSTS),
            source: "/var/log/app.log".to_string(),
            sourcetype: "app:json".to_string(),
            index: "main".to_string(),
            raw: format!("row {i}"),
            attributes: None,
        })
        .collect()
}

async fn append(ice: &IcebergContext, config: &IndexConfig, events: &[Event]) {
    let batch = events_to_record_batch(events).unwrap();
    let blooms = bloom_columns(config);
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    ice.append_to_table(&ice.index_table_ident(&config.index_id), batch, &bloom_refs)
        .await
        .unwrap();
}

/// The ground truth: `GROUP BY` read straight off the committed files, with no
/// aggregate anywhere in the plan.
async fn scanned_counts(ice: &IcebergContext, index_id: &str, column: &str) -> Vec<(String, u64)> {
    use datafusion::arrow::array::{Array, StringArray, UInt64Array};
    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(index_id), index_id)
        .await
        .unwrap();
    let batches = ctx
        .sql(&format!(
            "SELECT \"{column}\" AS k, CAST(count(*) AS BIGINT UNSIGNED) AS n \
             FROM \"{index_id}\" GROUP BY \"{column}\" ORDER BY k"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(keys.is_valid(i), "no NULL groups in this fixture");
            out.push((keys.value(i).to_string(), counts.value(i)));
        }
    }
    out.sort();
    out
}

async fn tier1_counts(
    ice: &IcebergContext,
    index_id: &str,
    column: &str,
) -> Option<Vec<(String, u64)>> {
    let counts = ice.tier1_group_counts(index_id, column).await.unwrap()?;
    let mut rows: Vec<(String, u64)> = counts
        .to_rows()
        .into_iter()
        .map(|(k, n)| (k.expect("no NULL groups in this fixture"), n))
        .collect();
    rows.sort();
    Some(rows)
}

/// The table's location on disk, shared by every incarnation of its name.
async fn table_location(ice: &IcebergContext, index_id: &str) -> std::path::PathBuf {
    let table = ice
        .catalog()
        .load_table(&ice.index_table_ident(index_id))
        .await
        .unwrap();
    table
        .metadata()
        .location()
        .trim_start_matches("file://")
        .into()
}

/// The directory holding one incarnation's aggregate artifacts.
async fn aggregate_dir(ice: &IcebergContext, index_id: &str) -> std::path::PathBuf {
    let table = ice
        .catalog()
        .load_table(&ice.index_table_ident(index_id))
        .await
        .unwrap();
    let uuid = table.metadata().uuid().to_string();
    table_location(ice, index_id)
        .await
        .join(aggregate_prefix_rel_path(&uuid))
}

fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(files_under(&path));
        } else {
            out.push(path);
        }
    }
    out.sort();
    out
}

/// Build and rebuild `logs`'s aggregates, then drop it and recreate the same id
/// with the SAME row count and different group values — the shape that makes
/// `column_total == record_count` admit the wrong incarnation's answer.
#[tokio::test]
async fn a_recreated_index_does_not_inherit_the_dropped_incarnations_aggregates() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = context(&tmp.path().join("warehouse")).await;
    let config = logs_index("logs");

    ice.create_index(&config).await.unwrap();
    append(&ice, &config, &events("a-host", 1_700_000_000)).await;
    ice.fold_group_count_deltas(1).await.unwrap();
    // A rebuild too: it is what stamps `rebuilt_through`, the watermark that
    // would otherwise tell the replacement's fold to skip every delta the
    // replacement wrote.
    ice.rebuild_group_count_aggregate("logs").await.unwrap();

    let dropped_dir = aggregate_dir(&ice, "logs").await;
    let dropped_artifacts = files_under(&dropped_dir);
    assert!(
        !dropped_artifacts.is_empty(),
        "the dropped incarnation must have artifacts to be at risk"
    );
    let dropped_tier1 = tier1_counts(&ice, "logs", "host")
        .await
        .expect("the first incarnation's aggregate answers Tier-1");
    assert_eq!(dropped_tier1, scanned_counts(&ice, "logs", "host").await);
    assert!(dropped_tier1.iter().all(|(host, _)| host.starts_with("a-")));

    assert!(ice.delete_index("logs").await.unwrap());
    ice.create_index(&config).await.unwrap();
    // EQUAL row count, DIFFERENT values: the guard cannot tell these apart.
    append(&ice, &config, &events("b-host", 1_800_000_000)).await;
    ice.fold_group_count_deltas(1).await.unwrap();

    let scanned = scanned_counts(&ice, "logs", "host").await;
    assert_eq!(scanned.len(), DISTINCT_HOSTS);
    assert!(scanned.iter().all(|(host, _)| host.starts_with("b-")));
    let tier1 = tier1_counts(&ice, "logs", "host")
        .await
        .expect("the replacement's own aggregate answers Tier-1");
    assert_eq!(
        tier1, scanned,
        "Tier-1 must agree with a direct file scan, not with the dropped table's aggregate"
    );
    // The low-cardinality columns take the inline object rather than the wide
    // base; that object is shared by exactly the same name and needs the same
    // fence. `sourcetype` is one value here, so a leak would be invisible in
    // the values and visible only in the totals — hence the row count.
    let inline = tier1_counts(&ice, "logs", "sourcetype")
        .await
        .expect("the inline aggregate answers Tier-1");
    assert_eq!(inline, vec![("app:json".to_string(), ROWS as u64)]);

    assert_ne!(
        aggregate_dir(&ice, "logs").await,
        dropped_dir,
        "the replacement addresses its own prefix"
    );
    for path in &dropped_artifacts {
        assert!(
            path.exists(),
            "the dropped incarnation's artifacts are preserved, not collected: {}",
            path.display()
        );
    }
}

/// The case a delete-the-prefix-on-create fix cannot reach: the dropped
/// incarnation's publisher was stalled between its commit and its PUT, and
/// lands its objects AFTER the replacement exists.
#[tokio::test]
async fn a_delayed_publication_from_the_dropped_incarnation_cannot_reach_the_replacement() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = context(&tmp.path().join("warehouse")).await;
    let config = logs_index("logs");

    ice.create_index(&config).await.unwrap();
    append(&ice, &config, &events("a-host", 1_700_000_000)).await;
    ice.fold_group_count_deltas(1).await.unwrap();
    let dropped_dir = aggregate_dir(&ice, "logs").await;

    assert!(ice.delete_index("logs").await.unwrap());
    ice.create_index(&config).await.unwrap();
    append(&ice, &config, &events("b-host", 1_800_000_000)).await;

    // The stalled publisher wakes up. Its sequence number is 1 — the same one
    // the replacement's first commit used, because sequence numbers restart
    // with the table — and its counts are as large as the whole table, so
    // folding them would leave the replacement's total exactly wrong.
    let ghost = GroupCountDelta {
        sequence_number: 1,
        snapshot_id: None,
        coverage_link: None,
        group_counts: Some(FileGroupCounts {
            columns: [(
                "host".to_string(),
                ColumnGroupCounts {
                    values: [("a-host-ghost".to_string(), ROWS as u64)]
                        .into_iter()
                        .collect(),
                    nulls: 0,
                },
            )]
            .into_iter()
            .collect(),
        }),
        sketches: None,
    };
    let ghost_path = dropped_dir.join(group_count_delta_rel_path(1));
    std::fs::create_dir_all(ghost_path.parent().unwrap()).unwrap();
    std::fs::write(&ghost_path, serde_json::to_vec(&ghost).unwrap()).unwrap();
    // And its lost-delta marker, which would ask the maintenance compactor to
    // rebuild the replacement's aggregate from the dropped table's columns.
    // Written by hand, at the path the production writer would have used, so
    // the object lands under the incarnation that lost the delta.
    let ghost_marker = dropped_dir.join("siglake-agg-deltas/00000000000000000001.rebuild.json");
    std::fs::write(
        &ghost_marker,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "sequence_number": 1,
            "columns": {"host": 2_000_000},
            "sketch_columns": [],
        }))
        .unwrap(),
    )
    .unwrap();

    ice.fold_group_count_deltas(1).await.unwrap();
    ice.invalidate_cached_table(&ice.index_table_ident("logs"))
        .await;

    let scanned = scanned_counts(&ice, "logs", "host").await;
    let tier1 = tier1_counts(&ice, "logs", "host")
        .await
        .expect("the replacement's aggregate still answers");
    assert_eq!(tier1, scanned, "a delayed publication changed the answer");
    assert!(tier1.iter().all(|(host, _)| host.starts_with("b-")));
    for path in [&ghost_path, &ghost_marker] {
        assert!(
            path.exists(),
            "the delayed object belongs to the dropped incarnation and is left alone: {}",
            path.display()
        );
    }
}

/// An artifact written before the incarnation prefix existed carries no
/// identity. The current table must not adopt it from the name alone, however
/// exactly its totals happen to match.
#[tokio::test]
async fn a_legacy_artifact_at_the_shared_path_is_never_adopted() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = context(&tmp.path().join("warehouse")).await;
    let config = logs_index("logs");

    ice.create_index(&config).await.unwrap();
    append(&ice, &config, &events("b-host", 1_800_000_000)).await;
    ice.fold_group_count_deltas(1).await.unwrap();

    let metadata_dir = table_location(&ice, "logs").await.join("metadata");
    // A pre-#2919 wide base whose single host accounts for every row — the
    // shape that passes `column_total == record_count`.
    let mut legacy_wide = WideGroupCounts::default();
    legacy_wide.set_group_counts(Some(FileGroupCounts {
        columns: [(
            "host".to_string(),
            ColumnGroupCounts {
                values: [("legacy-host".to_string(), ROWS as u64)]
                    .into_iter()
                    .collect(),
                nulls: 0,
            },
        )]
        .into_iter()
        .collect(),
    }));
    let legacy_wide_path = metadata_dir.join("siglake-agg-wide.json");
    std::fs::write(&legacy_wide_path, serde_json::to_vec(&legacy_wide).unwrap()).unwrap();
    // The inline object of that era says the same thing about a column the
    // inline cap does cover, and is read first when it is read at all.
    let legacy_inline = SnapshotAggregates {
        group_counts: Some(FileGroupCounts {
            columns: [(
                "sourcetype".to_string(),
                ColumnGroupCounts {
                    values: [("legacy:json".to_string(), ROWS as u64)]
                        .into_iter()
                        .collect(),
                    nulls: 0,
                },
            )]
            .into_iter()
            .collect(),
        }),
        ..Default::default()
    };
    let legacy_inline_path = metadata_dir.join("siglake-aggregates.json");
    std::fs::write(
        &legacy_inline_path,
        serde_json::to_vec(&legacy_inline).unwrap(),
    )
    .unwrap();

    ice.invalidate_cached_table(&ice.index_table_ident("logs"))
        .await;
    let tier1 = tier1_counts(&ice, "logs", "host")
        .await
        .expect("the table's own aggregate answers");
    assert_eq!(tier1, scanned_counts(&ice, "logs", "host").await);
    assert!(
        tier1.iter().all(|(host, _)| host.starts_with("b-")),
        "an unowned artifact was adopted from the name"
    );
    assert_eq!(
        tier1_counts(&ice, "logs", "sourcetype").await,
        Some(vec![("app:json".to_string(), ROWS as u64)]),
        "the inline answer came from the legacy object"
    );
    for path in [&legacy_wide_path, &legacy_inline_path] {
        assert!(
            path.exists(),
            "legacy artifacts are preserved, not collected: {}",
            path.display()
        );
    }
}
